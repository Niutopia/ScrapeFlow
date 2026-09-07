"""Replenishment search-term construction (extracted 2026-09-07).

Split from ``_replenishment_local_adapter_impl`` along its natural seam: the
pure term-building grammar for every search source.  The impl module remains
the facade and re-exports every exported name, so external
``adapter._symbol`` consumers and the test suite see no change.
"""

from __future__ import annotations

import hashlib
import json
import re
import time
import unicodedata
from typing import Any, Iterable, Mapping, Sequence

from engine.scrapeflow.media_policy import VIDEO_EXTENSIONS
from engine.scrapeflow.replenishment_matching import (
    coverage_tokens as _coverage_tokens,
    expanded_episode_ids as _expanded_episode_ids,
    normalized_text as _normalized_text,
    season_markers as _season_markers,
)


# DMHY's RSS endpoint rejects long ``keyword`` values (the response is an
# HTTP error rather than an empty result).  Keep this bound local to the
# provider adapter: it is a safety/liveness limit for a read-only query, not
# a user-facing search policy or a title-specific exception.
_DMHY_MAX_QUERY_TERMS = 4
_DMHY_MAX_QUERY_TERM_LENGTH = 64

# Nyaa's RSS endpoint currently returns a fixed broad window (75 rows in the
# live service) and does not honor the usual ``page``/``offset`` parameters.
# Collect enough rows to rank that whole window, then inspect only a bounded
# top window of Torrent metainfo.  A saturated response or an uninspected
# manifest window is deliberately *not* source exhaustion: there is no safe
# pagination receipt with which to prove the rest absent.  Search terms are
# intentionally separate from DMHY's shorter RSS keyword limit.
_NYAA_MAX_QUERY_TERMS = 4
# A cursor advances this deterministic logical set four queries at a time.
# This is intentionally finite: it prevents a malformed Gap ledger from
# creating unbounded discovery work while ensuring later coordinates are not
# silently omitted from no-resource evidence.
_NYAA_MAX_LOGICAL_QUERY_TERMS = 64
_NYAA_MAX_QUERY_TERM_LENGTH = 96
_NYAA_MAX_RSS_ROWS_PER_QUERY = 75
# Keep a modest multiple of the normal RSS window while retaining the
# highest-ranked rows as later terms arrive.  The tighter metainfo cap below
# is enforced only after this relevance sort.
_NYAA_MAX_FEED_ROWS = 256
_NYAA_MAX_MANIFEST_INSPECTIONS = 32

def _optional_episode_title_search_terms(
    request: Mapping[str, Any],
) -> list[str]:
    """Keep a bounded set of exact S00 names in provider query budgets.

    Dynamic providers accept only a few queries per pass.  Prefer one exact
    TMDB title for each release season marker (for example ``3rd season`` and
    ``4th season``) before filling with other episode titles.  The later
    recursive file-manifest check remains authoritative for gap coverage.
    """
    rules = request.get("rules")
    if not (
        isinstance(rules, Mapping)
        and rules.get("optional_discovery_only") is True
    ):
        return []
    media = request.get("media") if isinstance(request.get("media"), Mapping) else {}
    aliases = [
        str(value).strip() for value in media.get("aliases") or []
        if isinstance(value, str) and value.strip()
    ]
    title = str(media.get("title") or "").strip()
    base = next(iter(dict.fromkeys([*aliases, title])), "")
    if not base:
        return []
    exact_titles: list[str] = []
    for group in request.get("query_groups") or []:
        if not isinstance(group, Mapping) or group.get("season") != 0:
            continue
        exact_titles.extend(
            str(value).strip() for value in group.get("episode_titles") or []
            if isinstance(value, str) and value.strip()
        )
    marked: list[str] = []
    remaining: list[str] = []
    seen_markers: set[str] = set()
    for value in dict.fromkeys(exact_titles):
        marker = re.search(
            r"(?i)\b(\d{1,2})(?:st|nd|rd|th)?\s+season\b", value,
        )
        marker_key = marker.group(1) if marker else ""
        if marker_key and marker_key not in seen_markers:
            seen_markers.add(marker_key)
            marked.append(value)
        else:
            remaining.append(value)
    return [f"{base} {value}" for value in [*marked, *remaining][:4]]


def _dynamic_search_terms(request: Mapping[str, Any]) -> list[str]:
    media = request.get("media") if isinstance(request.get("media"), Mapping) else {}
    aliases = [
        str(value).strip() for value in media.get("aliases") or []
        if isinstance(value, str) and value.strip()
    ]
    title = str(media.get("title") or "").strip()
    bases = list(dict.fromkeys([*aliases, title]))
    focused: list[str] = []
    for group in request.get("query_groups") or []:
        if not isinstance(group, Mapping) or not isinstance(group.get("season"), int):
            continue
        season = int(group["season"])
        names = [
            str(value).strip() for value in group.get("season_names") or []
            if isinstance(value, str) and value.strip()
        ]
        for base in bases[:4]:
            focused.append(f"{base} {names[0]}" if names else f"{base} S{season:02d}")
            focused.append(f"{base} S{season:02d}")
    generated = [
        str(value).strip() for value in request.get("search_queries") or []
        if isinstance(value, str)
        and re.search(r"(?i)(?:S\d|\d+x\d|Season\s+\d|第.+季|完结篇|Darkness)", value)
    ]
    episode_focused = _optional_episode_title_search_terms(request)
    terms: list[str] = []
    seen: set[str] = set()
    # Exact S00 episode titles must survive the provider query ceiling; broad
    # season syntax follows them and still finds complete packs.  Focused
    # terms must precede bare aliases.  With many exact TMDB
    # alternative titles, letting bases consume the 12-term budget prevented
    # later official English names from receiving an S00 query at all.
    for value in [*episode_focused, *focused, *generated, *bases]:
        key = re.sub(r"\W+", "", value).casefold()
        if len(key) < 2 or key in seen:
            continue
        seen.add(key)
        terms.append(value)
        if len(terms) >= 12:
            break
    return terms


def _identity_query_bases(
    request: Mapping[str, Any], *, prefer_latin_aliases: bool = False,
) -> list[str]:
    """Return bounded identity evidence only, in a provider-friendly order.

    The replenishment bridge projects ``media.aliases`` exclusively from the
    confirmed C/TMDB identity.  Discovery may rank those aliases differently
    for an index, but it must never manufacture a transliteration from a
    source path or a web result.  In particular, a short pure-Latin official
    alias is usually the release spelling on anime indexes, while a translated
    local display title is not.
    """
    media = request.get("media") if isinstance(request.get("media"), Mapping) else {}
    title = str(media.get("title") or "").strip()
    raw_aliases = media.get("aliases")
    aliases = raw_aliases if isinstance(raw_aliases, (list, tuple)) else []
    if not prefer_latin_aliases:
        # Preserve the established default term order for generic callers.
        output: list[str] = []
        seen_exact: set[str] = set()
        for value in [title, *aliases[:40]]:
            if not isinstance(value, str):
                continue
            text = value.strip()
            if not text or text in seen_exact:
                continue
            seen_exact.add(text)
            output.append(text)
        return output

    # The bridge itself caps aliases, but preserve a local hard limit for a
    # malformed direct adapter request too.
    values = [*aliases, title]
    output: list[tuple[int, str]] = []
    seen: set[str] = set()
    for index, value in enumerate(values[:40]):
        if not isinstance(value, str):
            continue
        text = value.strip()
        key = _normalized_text(text)
        if not text or not key or key in seen:
            continue
        seen.add(key)
        output.append((index, text))
    def rank(row: tuple[int, str]) -> tuple[int, int, int]:
        index, value = row
        normalized = unicodedata.normalize("NFKC", value)
        has_latin = bool(re.search(r"[A-Za-z]", normalized))
        # Keep a pure Latin/romanized alias ahead of a mixed-script alias
        # such as a Japanese title containing an English franchise marker.
        # Both remain confirmed TMDB evidence; this is only a search order.
        non_latin = re.sub(
            r"[A-Za-z0-9\s._,:;!?'\"()\[\]{}&+\-/]", "", normalized,
        )
        latin_rank = 0 if has_latin and not non_latin else 1 if has_latin else 2
        return (latin_rank, len(_normalized_text(value)), index)

    return [value for _index, value in sorted(output, key=rank)]


def _requested_episode_targets(
    request: Mapping[str, Any], *, maximum: int | None = None,
) -> list[tuple[int, int]]:
    """Read bounded exact episode coordinates from groups or ledger gaps.

    Populated ``query_groups`` remain authoritative; they are used by callers
    that intentionally narrow discovery.  The live Gap ledger bridge
    deliberately does not emit that optional legacy shape, so its durable
    ``gaps[].episodes`` rows provide the safe fallback.  A malformed or empty
    legacy group remains compatible with the historical fallback behavior.
    """
    targets: list[tuple[int, int]] = []
    target_limit = maximum if type(maximum) is int and maximum > 0 else None
    seen_targets: set[tuple[int, int]] = set()

    def add_target(season: object, episode: object) -> None:
        if type(season) is not int or season < 0:
            return
        if type(episode) is not int or not 1 <= episode <= 9999:
            return
        coordinate = (season, episode)
        if coordinate in seen_targets:
            return
        seen_targets.add(coordinate)
        targets.append(coordinate)

    for group in request.get("query_groups") or []:
        if not isinstance(group, Mapping) or type(group.get("season")) is not int:
            continue
        season = int(group["season"])
        episodes = group.get("episodes")
        if isinstance(episodes, (list, tuple)):
            for episode in episodes:
                add_target(season, episode)
                if target_limit is not None and len(targets) >= target_limit:
                    return targets
    if targets:
        return targets

    for gap in request.get("gaps") or []:
        if not isinstance(gap, Mapping):
            continue
        season = gap.get("season")
        episodes = gap.get("episodes")
        if isinstance(episodes, (list, tuple)):
            for episode in episodes:
                add_target(season, episode)
                if target_limit is not None and len(targets) >= target_limit:
                    return targets
        add_target(season, gap.get("episode"))
        if target_limit is not None and len(targets) >= target_limit:
            return targets
    return targets


def _positive_requested_seasons(
    request: Mapping[str, Any], *, maximum: int = 8,
) -> list[int]:
    """Return positive seasons from explicit groups or, for bridge rows, gaps."""
    if maximum < 1:
        return []
    output: list[int] = []
    seen: set[int] = set()

    def add(season: object) -> None:
        if type(season) is not int or season <= 0 or season in seen:
            return
        seen.add(season)
        output.append(season)

    declared_group = False
    for group in request.get("query_groups") or []:
        if not isinstance(group, Mapping) or type(group.get("season")) is not int:
            continue
        declared_group = True
        add(group["season"])
        if len(output) >= maximum:
            return output
    if declared_group:
        return output

    for gap in request.get("gaps") or []:
        if not isinstance(gap, Mapping):
            continue
        add(gap.get("season"))
        if len(output) >= maximum:
            break
    return output


def _explicit_episode_search_terms(
    request: Mapping[str, Any], *, maximum: int = 8,
    prefer_latin_aliases: bool = False,
    interleave_aliases: bool = False,
) -> list[str]:
    """Build a small, identity-scoped query set for exact requested episodes.

    A TMDB record can contribute many aliases.  Broad ``S00`` queries for the
    first few aliases used to consume every provider query slot before the
    exact ``S00E##`` form was tried.  Keep the exact token independent of the
    broader generated list so a verified single-episode release remains
    discoverable without trusting a bare series pack.
    """
    if maximum < 1:
        return []
    bases = _identity_query_bases(
        request, prefer_latin_aliases=prefer_latin_aliases,
    )
    # Do not materialize a term cross-product from an arbitrarily large Gap
    # ledger.  A later retry receives the same durable coordinates again, so
    # this only bounds one read-only provider window.
    targets = _requested_episode_targets(
        request, maximum=max(1, min(32, maximum * 4)),
    )
    bases = bases[:max(2, min(8, maximum * 2))]
    output: list[str] = []
    seen: set[str] = set()

    # Provider discovery has a small query ceiling.  When asked, alternate
    # the two strongest authoritative aliases for the first gap coordinates
    # so one translated local title cannot consume the whole window.
    paired: Iterable[tuple[str, tuple[int, int]]]
    if interleave_aliases:
        paired = (
            (base, target)
            for target in targets
            for base in bases[:2]
        )
    else:
        paired = (
            (base, target)
            for base in bases
            for target in targets
        )
    for base, (season, episode) in paired:
        value = f"{base} S{season:02d}E{episode:02d}"
        key = re.sub(r"\W+", "", value).casefold()
        if key in seen:
            continue
        seen.add(key)
        output.append(value)
        if len(output) >= maximum:
            return output
    return output


def _compact_dynamic_search_terms(
    request: Mapping[str, Any], *, maximum: int = 4,
) -> list[str]:
    """Prefer season/episode-specific queries over redundant bare aliases."""
    if maximum < 1:
        return []
    terms = _dynamic_search_terms(request)
    episode_focused = _optional_episode_title_search_terms(request)
    marked_episode_focused = [
        value for value in episode_focused
        if re.search(
            r"(?i)\b\d{1,2}(?:st|nd|rd|th)?\s+season\b", value,
        )
    ]
    # Keep enough authoritative aliases to reach the original/romanized title
    # even when TMDB returns a long multilingual list.  The final query cap is
    # still ``maximum``; this only changes which exact terms get the slots.
    exact_episode_all = _explicit_episode_search_terms(
        request, maximum=max(8, maximum * 3),
    )
    def latin_alias_term(value: str) -> bool:
        base = re.sub(
            r"(?i)\s+S\d{1,3}E\d{1,4}(?:-E?\d{1,4})?\s*$", "", value,
        )
        return bool(re.search(r"[A-Za-z]", base))

    exact_episode = [
        value for value in exact_episode_all if latin_alias_term(value)
    ] + [
        value for value in exact_episode_all if not latin_alias_term(value)
    ]
    exact_season = [
        value for value in terms if re.search(r"(?i)\bS\d{2}\b", value)
    ]
    broad_season = [
        value for value in terms
        if re.search(r"(?i)(?:\bS\d{1,2}\b|\bSeason\s+\d+\b|\b\d+x\d|\u7b2c.+\u5b63)", value)
    ]
    output: list[str] = []
    seen: set[str] = set()
    for value in [
        *marked_episode_focused,
        *exact_episode,
        *episode_focused,
        *exact_season,
        *broad_season,
        *terms,
    ]:
        key = re.sub(r"\W+", "", value).casefold()
        if key in seen:
            continue
        seen.add(key)
        output.append(value)
        if len(output) >= maximum:
            break
    return output


_RELEASE_YEAR_TOKEN = re.compile(r"\b(19\d{2}|20\d{2})\b")


def _release_year_conflict(request: Mapping[str, Any], release_name: str) -> bool:
    """Reject a release whose explicit year predates the work's premiere.

    A same-title different-series pack (``Shameless UK 2004`` against the US
    2011 work) maps cleanly onto SxxEyy coordinates, so coordinate matching
    alone cannot keep it out.  A release year is never earlier than the
    premiere of the work it belongs to, so a year token below the requested
    first-air year (with a one-year festival/cross-year grace) is a hard
    identity disproof.  Season-year pack naming and missing years stay
    accepted.
    """
    media = request.get("media") if isinstance(request.get("media"), Mapping) else {}
    raw_year = str(media.get("year") or "").strip()
    if not re.fullmatch(r"(?:19|20)\d{2}", raw_year):
        return False
    first_air = int(raw_year)
    years = [int(value) for value in _RELEASE_YEAR_TOKEN.findall(release_name)]
    return bool(years) and min(years) < first_air - 1


def _catalog_torrent_candidate_variants(
    candidate: Mapping[str, Any], *, include_local: bool = True,
) -> list[dict[str, Any]]:
    """Return the exact local-Torrent variant for one catalog candidate."""
    local = dict(candidate)
    acquisition = local.get("acquisition")
    if (
        str(local.get("provider") or "").strip().casefold() == PROVIDER_LOCAL_MAGNET
        and isinstance(acquisition, Mapping)
        and str(acquisition.get("kind") or "").strip().casefold() == ACQUISITION_TORRENT
        and candidate_capability_error(local) is None
    ):
        return [local] if include_local else []
    return []


def _nyaa_safe_query_term(value: object) -> str | None:
    """Normalize one TMDB-derived Nyaa keyword and reject unsafe length."""
    if not isinstance(value, str):
        return None
    normalized = unicodedata.normalize("NFKC", value)
    normalized = re.sub(r"\s+", " ", normalized).strip()
    if not normalized or len(normalized) > _NYAA_MAX_QUERY_TERM_LENGTH:
        return None
    if any(
        ord(character) < 0x20 or ord(character) == 0x7F
        for character in normalized
    ):
        return None
    return normalized


def _nyaa_logical_search_terms(
    request: Mapping[str, Any], *, maximum: int = _NYAA_MAX_LOGICAL_QUERY_TERMS,
) -> list[str]:
    """Build a bounded logical Nyaa term set from identity and coordinates.

    Nyaa release labels often use a naked episode ordinal (``Title - 13``)
    even when the same provider returns nothing for ``Title S01E13``.  Query
    one or more exact coordinates first, then use the same confirmed identity
    title plus each requested ordinal as a provider-specific fallback.  Bare
    identity/season queries are last-resort discovery only.  Source paths,
    web results, and provider release titles never enter this list; the
    manifest-to-gap check remains the candidate authorization boundary.
    """
    if maximum < 1:
        return []
    bases: list[str] = []
    seen_bases: set[str] = set()
    for raw in _identity_query_bases(request, prefer_latin_aliases=True):
        value = _nyaa_safe_query_term(raw)
        if value is None:
            continue
        key = _normalized_text(value)
        if not key or key in seen_bases:
            continue
        seen_bases.add(key)
        bases.append(value)
    if not bases:
        return []

    # Bound the intermediate cross-product as well as the final query list.
    # The durable gap ledger is retried in later windows, so this cannot
    # authorize or silently discard a coordinate; it only limits one
    # provider request's read-only work.
    targets = _requested_episode_targets(
        request, maximum=max(1, min(32, maximum * 4)),
    )
    bases = bases[:max(2, min(8, maximum * 2))]
    exact: list[str] = []
    release_style: list[str] = []
    # Use two independent confirmed aliases for exact grammar, but prefer one
    # concise release spelling across several open coordinates below.  That
    # lets a bounded pass find multiple ordinary ``Title - N`` releases
    # without importing a directory label or an external search result.
    for season, episode in targets:
        for base in bases[:2]:
            value = _nyaa_safe_query_term(
                f"{base} S{season:02d}E{episode:02d}"
            )
            if value is not None:
                exact.append(value)
    for base in bases:
        for _season, episode in targets:
            value = _nyaa_safe_query_term(f"{base} {episode}")
            if value is not None:
                release_style.append(value)

    seasons = _positive_requested_seasons(request, maximum=8)
    season_terms: list[str] = []
    for season in seasons:
        for base in bases:
            value = _nyaa_safe_query_term(f"{base} S{season:02d}")
            if value is not None:
                season_terms.append(value)
    # Season 00 releases are commonly bare ``Specials``/``OVA`` rows; a bare
    # confirmed alias is safe for discovery, while the manifest still proves
    # the exact optional coordinate.
    if any(season == 0 for season, _episode in targets):
        season_terms.extend(bases[:2])

    # Preserve two formal token queries as the first lane.  The subsequent
    # release-style coordinates are intentionally contiguous: the cursor can
    # advance through all durable Gap coordinates four at a time rather than
    # repeatedly searching only the first two episodes.
    if exact:
        exact_budget = min(len(exact), 2)
        ordered = [
            *exact[:exact_budget], *release_style,
            *exact[exact_budget:], *bases, *season_terms,
        ]
    else:
        ordered = [*release_style, *bases, *season_terms]

    output: list[str] = []
    seen: set[str] = set()
    for value in ordered:
        key = re.sub(r"\W+", "", value).casefold()
        if not key or key in seen:
            continue
        seen.add(key)
        output.append(value)
        if len(output) >= maximum:
            break
    return output


def _nyaa_search_terms(
    request: Mapping[str, Any], *, maximum: int = _NYAA_MAX_QUERY_TERMS,
) -> list[str]:
    """Return the first bounded Nyaa provider window for direct callers.

    The live searcher uses ``_nyaa_logical_search_terms`` plus a durable term
    cursor.  Keeping this small wrapper preserves the provider helper's
    historical bounded API for tests and isolated callers.
    """
    if maximum < 1:
        return []
    return _nyaa_logical_search_terms(
        request, maximum=_NYAA_MAX_LOGICAL_QUERY_TERMS,
    )[:maximum]


def _nyaa_request_fingerprint(
    request: Mapping[str, Any], terms: Sequence[str],
) -> str:
    """Bind a Nyaa term cursor to C/TMDB identity and exact gap evidence."""
    media = request.get("media")
    media = media if isinstance(media, Mapping) else {}
    gap_payload: list[dict[str, Any]] = []
    for raw in request.get("gaps") or []:
        if not isinstance(raw, Mapping):
            continue
        gap_payload.append({
            "id": raw.get("id"),
            "kind": raw.get("kind"),
            "season": raw.get("season"),
            "episodes": sorted({
                int(value) for value in (raw.get("episodes") or [])
                if type(value) is int and value > 0
            }),
        })
    payload = {
        "provider": "nyaa",
        "media": {
            "media_type": media.get("media_type"),
            "tmdb_id": media.get("tmdb_id"),
            "title": media.get("title"),
            "original_title": media.get("original_title"),
            "aliases": [
                value for value in (media.get("aliases") or [])
                if isinstance(value, str)
            ][:40],
        },
        "gaps": sorted(gap_payload, key=lambda row: (
            str(row.get("id") or ""), str(row.get("kind") or ""),
            int(row.get("season")) if type(row.get("season")) is int else -1,
            row.get("episodes") or [],
        )),
        "terms": list(terms),
    }
    return hashlib.sha256(json.dumps(
        payload, ensure_ascii=True, sort_keys=True, separators=(",", ":"),
    ).encode("utf-8")).hexdigest()


def _nyaa_request_cursor(
    request: Mapping[str, Any], fingerprint: str, term_count: int,
) -> dict[str, Any]:
    """Read a bounded continuation receipt or restart safely at term zero."""
    raw: object = None
    cursors = request.get("search_cursors")
    if isinstance(cursors, Mapping):
        for key, value in cursors.items():
            normalized = re.sub(r"[^a-z0-9]+", "", str(key).casefold())
            if normalized == "nyaa":
                raw = value
                break
    if not isinstance(raw, Mapping):
        return {"fingerprint": fingerprint, "term_index": 0, "page": 1, "exhausted": False}
    term_index = raw.get("term_index")
    page = raw.get("page")
    exhausted = raw.get("exhausted")
    if (
        raw.get("fingerprint") != fingerprint
        or type(term_index) is not int
        or not 0 <= term_index <= term_count
        or type(page) is not int
        or page != 1
        or type(exhausted) is not bool
    ):
        return {"fingerprint": fingerprint, "term_index": 0, "page": 1, "exhausted": False}
    return {
        "fingerprint": fingerprint,
        "term_index": term_index,
        "page": 1,
        "exhausted": exhausted,
    }


def _nyaa_release_priority(
    request: Mapping[str, Any], release_name: str,
) -> tuple[int, int, int, int, int]:
    """Order broad RSS rows by audited identity/episode relevance.

    The score only changes which untrusted metainfo is inspected first.  It
    never admits a candidate: `_torrent_candidate_variants` must still prove
    exact season/episode coverage and all media safety constraints.
    """
    base = _animetosho_release_priority(request, release_name)
    normalized = _normalized_text(release_name)
    media = request.get("media") if isinstance(request.get("media"), Mapping) else {}
    aliases = [
        value for value in [
            media.get("title"), media.get("original_title"),
            *(media.get("aliases") if isinstance(media.get("aliases"), (list, tuple)) else []),
        ]
        if isinstance(value, str) and len(_normalized_text(value)) >= 3
    ]
    alias_strength = max(
        (len(_normalized_text(value)) for value in aliases
         if _normalized_text(value) in normalized),
        default=0,
    )
    # Lower tuple values are inspected first.  Keep exact/alias hits ahead of
    # unrelated rows even when the provider's broad 75-row window is full.
    return (
        0 if alias_strength else 1,
        base[0],
        base[1],
        base[2],
        -alias_strength,
    )

def _animetosho_release_priority(
    request: Mapping[str, Any], release_name: str,
) -> tuple[int, int, int, int]:
    """Order feed rows likely to satisfy an already-audited gap first.

    AnimeTosho's broad alias feed can contain hundreds of rows, while reading
    each ``.torrent`` manifest is comparatively expensive.  Naked episode
    titles (``Show - 24``) therefore need to precede unrelated specials and
    other seasons.  The score is an ordering hint only; all rows still pass
    the exact manifest-to-gap checks before becoming candidates.
    """
    requested = _animetosho_requested_gap_tokens(request)
    media = request.get("media") if isinstance(request.get("media"), Mapping) else {}
    requested_seasons = {
        int(token[1:token.index("E")])
        for token in requested
        if re.fullmatch(r"S\d{2,3}E\d{2,4}", token)
    }
    # A naked trailing number is meaningful for a single requested season;
    # with multiple seasons, refusing the fallback is safer than promoting a
    # cross-season pack merely because its title ends in the right number.
    default_seasons = requested_seasons if len(requested_seasons) == 1 else set()
    release_tokens = _expanded_episode_ids(release_name) | _coverage_tokens(
        [release_name], default_seasons=default_seasons,
    )
    exact = requested & release_tokens
    source_priority = _source_episode_release_priority(request, release_name)
    aliases = [
        value for value in (media.get("aliases") or [])
        if isinstance(value, str) and len(_normalized_text(value)) >= 4
    ]
    normalized_release = _normalized_text(release_name)
    alias_hit = any(
        _normalized_text(value) in normalized_release for value in aliases
    )
    return (
        0 if exact else 1,
        -len(exact),
        source_priority,
        0 if alias_hit else 1,
    )


_S00_TITLE_PREFLIGHT_LIMIT = 4
_S00_TITLE_PREFLIGHT_KEY_LIMIT = 256
_S00_TITLE_PREFLIGHT_GENERIC_KEYS = frozenset({
    "special", "specials", "movie", "ova", "oad", "sp", "extra",
    "bonus", "episode", "episodes", "特别篇", "特別篇", "剧场版",
    "劇場版", "映像特典",
})

def _animetosho_requested_gap_tokens(request: Mapping[str, Any]) -> set[str]:
    """Return exact episode coordinates that a feed row may advertise.

    This helper is used only to order untrusted feed rows.  It does not
    authorize a candidate: ``_gap_file_map`` and the manifest safety checks
    remain the sole source of coverage evidence.  Keeping the requested set
    derived from the durable gap coordinates also means a broad release title
    cannot introduce a new season or episode through this optimization.
    """
    tokens: set[str] = set()
    for raw_gap in request.get("gaps") or []:
        if not isinstance(raw_gap, Mapping):
            continue
        kind = str(raw_gap.get("kind") or "")
        gap_id = str(raw_gap.get("id") or "")
        if kind == "missing_episode" and re.fullmatch(
            r"S\d{2,3}E\d{2,4}", gap_id,
        ):
            tokens.add(gap_id)
            continue
        if kind != "missing_season":
            continue
        season = raw_gap.get("season")
        expected = raw_gap.get("expected_episode_count")
        if type(season) is not int or season < 0:
            continue
        if type(expected) is not int or expected < 1 or expected > 9999:
            continue
        tokens.update(
            f"S{season:02d}E{episode:02d}"
            for episode in range(1, expected + 1)
        )
    if tokens:
        return tokens
    # A few legacy test/integration callers provide only query groups.  They
    # are still confirmed coordinates, but never source-directory evidence.
    return {
        f"S{season:02d}E{episode:02d}"
        for season, episode in _requested_episode_targets(request)
    }

def _source_episode_release_priority(
    request: Mapping[str, Any], release_name: str,
) -> int:
    """Prioritize metadata rows that can contain a verified local alias."""
    release_tokens = _expanded_episode_ids(release_name) | _coverage_tokens(
        [release_name], default_seasons=_season_markers(release_name),
    )
    release_words = set(re.findall(r"[a-z0-9]+", release_name.casefold()))
    normalized_release = _normalized_text(release_name)
    for gap in request.get("gaps") or []:
        if not isinstance(gap, Mapping):
            continue
        for alias in gap.get("source_episode_aliases") or []:
            if not isinstance(alias, Mapping):
                continue
            source_season = alias.get("season")
            source_episode = alias.get("episode")
            if type(source_season) is not int or type(source_episode) is not int:
                continue
            token = f"S{source_season:02d}E{source_episode:02d}"
            if token not in release_tokens:
                continue
            for title in alias.get("series_titles") or []:
                if not isinstance(title, str):
                    continue
                normalized_title = _normalized_text(title)
                title_words = {
                    word for word in re.findall(r"[a-z0-9]+", title.casefold())
                    if len(word) >= 3
                    and word not in {"from", "starting", "season"}
                }
                if (
                    normalized_title and normalized_title in normalized_release
                    or len(title_words) >= 2 and title_words <= release_words
                ):
                    return 0
    return 1
