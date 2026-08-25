"""Provider-neutral post-scrape search, evidence normalization and ranking."""

from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime, timezone
import base64
import json
import re
from pathlib import PurePosixPath
from typing import Any, Mapping, Sequence
import unicodedata
import copy

from engine.scrapeflow.replenishment_acquisition import (
    AcquisitionRouteError, acquisition_lane,
)
from engine.scrapeflow.replenishment_matching import (
    EPISODE_RE,
    SEASON_RE,
    coverage_tokens as _coverage_tokens,
    episode_ranges as _episode_ranges,
    expanded_episode_ids as _expanded_episode_ids,
    season_markers as _season_markers,
)
from engine.scrapeflow.provider_capabilities import (
    ACTIVE_PROVIDERS,
    provider_capability_snapshot,
)
from engine.scrapeflow.media_policy import (
    SUBTITLE_EXTENSIONS,
    VIDEO_EXTENSIONS,
)
from .replenishment_tiers import STRICT_TIER_ORDER


# Movies and episodes share the provider-neutral acquisition path. Subtitle
# gaps use a subtitle-only materializer and never enter the video path.
ACTIONABLE_GAP_KINDS = frozenset({
    "missing_episode", "missing_season", "missing_media", "missing_subtitle",
})
MEDIA_GAP_KINDS = frozenset({
    "missing_episode", "missing_season", "missing_media",
})
SUBTITLE_GAP_KIND = "missing_subtitle"
PROVIDER_ORDER = {
    provider: index
    for index, provider in enumerate(STRICT_TIER_ORDER)
    if provider in ACTIVE_PROVIDERS
}
PROVIDER_DIAGNOSTIC_NAMES = tuple(
    sorted(ACTIVE_PROVIDERS, key=lambda provider: PROVIDER_ORDER[provider])
)
QUALITY_ORDER = {"2160p": 3, "1080p": 2, "720p": 1, "unknown": 0}
_VIDEO_SUFFIXES = VIDEO_EXTENSIONS
BAD_AVAILABILITY_MARKERS = (
    "not available", "dead", "offline", "expired", "invalid", "unavailable",
    "blocked", "banned", "deleted", "失效", "过期", "封禁", "删除", "不可用",
)
_SWARM_MAX_AGE_SECONDS = 6 * 60 * 60
_SWARM_MAX_FUTURE_SKEW_SECONDS = 5 * 60
_SWARM_COUNT_LIMIT = 1_000_000_000
CHINESE_NUMERALS = ("零", "一", "二", "三", "四", "五", "六", "七", "八", "九", "十")

# A successful provider child may be useful on a later run, but its candidate
# must cross a much smaller boundary than the live search result.  Keep this
# allow-list here (rather than copying arbitrary JSON in the coordinator) so
# a remembered row cannot smuggle a helper cookie, bearer token, or a private
# provider action back into a new request.
_REUSABLE_CANDIDATE_TOP_FIELDS = frozenset({
    "provider", "locator", "infohash", "release_name", "title", "titles",
    "year", "files", "file_coverage", "name_coverage", "resolution",
    "quality", "availability", "available", "updated_at", "swarm",
    "swarm_observed_at", "seeders", "leechers", "tmdb_id", "identity_match",
    "media_format", "coverage", "selected_gap_ids", "acquisition",
    "memory_verified_at", "memory_verified_gap_ids",
})
_REUSABLE_CANDIDATE_SECRET_KEY_MARKERS = frozenset({
    "passcode", "password", "passwd", "token", "secret", "cookie",
    "authorization", "auth", "session", "credential", "api_key", "apikey",
})
_REUSABLE_CANDIDATE_MAX_BYTES = 64 * 1024
_REUSABLE_CANDIDATE_MAX_DEPTH = 8
_REUSABLE_CANDIDATE_MAX_LIST_ITEMS = 256
_REUSABLE_CANDIDATE_MAX_STRING = 4096
_REUSABLE_CANDIDATE_SECRET_VALUE_RE = re.compile(
    r"(?i)(?:^|[?&\s])(passcode|password|passwd|token|secret|api[_-]?key)="
)
_REUSABLE_CANDIDATE_USERINFO_RE = re.compile(r"(?i)://[^/\s@]+:[^/\s@]+@")


def _reusable_candidate_key_is_secret(value: object) -> bool:
    key = str(value or "").strip().casefold().replace("-", "_")
    return any(marker in key for marker in _REUSABLE_CANDIDATE_SECRET_KEY_MARKERS)


def _copy_reusable_candidate_value(value: Any, *, depth: int = 0) -> Any:
    """Copy JSON values for the positive-candidate memory boundary.

    Returning ``None`` is ambiguous because ``null`` is a valid JSON value;
    callers therefore use the private sentinel below for malformed values.
    """
    invalid = _REUSABLE_CANDIDATE_INVALID
    if depth > _REUSABLE_CANDIDATE_MAX_DEPTH:
        return invalid
    if value is None or type(value) in {bool, int, float}:
        # JSON's non-finite numbers are rejected by the final encoder.
        return value
    if isinstance(value, str):
        if len(value) > _REUSABLE_CANDIDATE_MAX_STRING:
            return invalid
        # Reject signed/user-info URLs rather than trying to parse and redact
        # them.  A remembered candidate is optional; losing one is safer than
        # persisting a credential-bearing locator.
        if (
            _REUSABLE_CANDIDATE_SECRET_VALUE_RE.search(value)
            or _REUSABLE_CANDIDATE_USERINFO_RE.search(value)
        ):
            return invalid
        return value
    if isinstance(value, Mapping):
        if len(value) > _REUSABLE_CANDIDATE_MAX_LIST_ITEMS:
            return invalid
        output: dict[str, Any] = {}
        for raw_key, raw_value in value.items():
            if not isinstance(raw_key, str) or not raw_key or len(raw_key) > 128:
                return invalid
            if _reusable_candidate_key_is_secret(raw_key):
                return invalid
            copied = _copy_reusable_candidate_value(raw_value, depth=depth + 1)
            if copied is invalid:
                return invalid
            output[raw_key] = copied
        return output
    if isinstance(value, (list, tuple)):
        if len(value) > _REUSABLE_CANDIDATE_MAX_LIST_ITEMS:
            return invalid
        output: list[Any] = []
        for item in value:
            copied = _copy_reusable_candidate_value(item, depth=depth + 1)
            if copied is invalid:
                return invalid
            output.append(copied)
        return output
    return invalid


_REUSABLE_CANDIDATE_INVALID = object()


def normalize_reusable_candidate(candidate: Mapping[str, Any]) -> dict[str, Any] | None:
    """Return one safe, replayable positive candidate or ``None``.

    Only fixed, credential-free acquisition lanes are remembered.  Quark
    share rows are intentionally excluded because their ``passcode`` is a
    secret even when the current search adapter happened to return a public
    share.  Fresh search remains responsible for rediscovering that lane.
    """
    if not isinstance(candidate, Mapping):
        return None
    provider = str(candidate.get("provider") or "").strip().casefold()
    if provider not in PROVIDER_ORDER or provider == "quark_share":
        return None
    copied: dict[str, Any] = {}
    for key in _REUSABLE_CANDIDATE_TOP_FIELDS:
        if key not in candidate:
            continue
        value = _copy_reusable_candidate_value(candidate[key])
        if value is _REUSABLE_CANDIDATE_INVALID:
            return None
        copied[key] = value
    copied["provider"] = provider
    locator = copied.get("locator")
    release_name = copied.get("release_name")
    acquisition = copied.get("acquisition")
    if (
        not isinstance(locator, str) or not locator.strip()
        or not isinstance(release_name, str) or not release_name.strip()
        or not isinstance(acquisition, Mapping)
    ):
        return None
    copied["locator"] = locator.strip()
    copied["release_name"] = release_name.strip()
    # The selector is the authoritative identity/coverage gate.  Running the
    # route validator here catches malformed remembered rows before they are
    # written to disk, without making memory a second selection algorithm.
    try:
        acquisition_lane(copied)
    except (AcquisitionRouteError, TypeError, ValueError):
        return None
    try:
        encoded = json.dumps(copied, ensure_ascii=False, allow_nan=False)
    except (TypeError, ValueError):
        return None
    if len(encoded.encode("utf-8")) > _REUSABLE_CANDIDATE_MAX_BYTES:
        return None
    return copied


def reusable_candidate_scope(
    request: Mapping[str, Any], *, tier: str,
) -> dict[str, Any] | None:
    """Build the non-secret identity coordinate for candidate memory.

    The scope deliberately does not use a filesystem path or a user-provided
    URL.  TMDB identity is preferred; a title/year fallback is accepted only
    when both values are present.  The media namespace is retained too:
    TMDB movie and TV identifiers are separate namespaces, so a numeric id
    alone must never make a film release eligible for a TV gap (or vice
    versa). Gap ids are retained as evidence on each
    entry and are intersected by the runtime, so a verified S01E01 release can
    help a later run for the same work without claiming another episode.
    """
    tier_value = str(tier or "").strip().casefold()
    if tier_value not in PROVIDER_ORDER:
        return None
    media = request.get("media") if isinstance(request.get("media"), Mapping) else {}
    raw_media_type = (
        media.get("media_type")
        or media.get("type")
        or request.get("media_type")
        or request.get("project_key")
    )
    media_type = str(raw_media_type or "").strip().casefold()
    if media_type.startswith("tmdb:"):
        parts = media_type.split(":", 2)
        media_type = parts[1] if len(parts) > 1 else ""
    if media_type in {"movie", "film"}:
        media_namespace = "movie"
    elif media_type in {"tv", "series", "anime", "us_tv", "mixed"}:
        media_namespace = "tv"
    else:
        # Builders always know the namespace. Older/handwritten provider
        # records can still be searched, but cannot read or write positive
        # cross-run memory.
        return None
    tmdb_id = media.get("tmdb_id")
    if type(tmdb_id) is int and tmdb_id > 0:
        identity: dict[str, Any] = {
            "media_type": media_namespace,
            "tmdb_id": tmdb_id,
        }
    else:
        title = _normalized_text(media.get("title"))
        year = str(media.get("year") or "").strip()
        if len(title) < 2 or not re.fullmatch(r"(?:19|20)\d{2}", year):
            return None
        identity = {
            "media_type": media_namespace,
            "title": title,
            "year": year,
        }
    media_format = str(media.get("media_format") or "").strip().casefold()
    if media_format in {"animation", "live_action"}:
        identity["media_format"] = media_format
    raw_gaps = request.get("gaps")
    gap_ids = sorted({
        str(row.get("id"))
        for row in (raw_gaps if isinstance(raw_gaps, list) else [])
        if isinstance(row, Mapping)
        and isinstance(row.get("id"), str)
        and row.get("id")
    })
    if not gap_ids:
        return None
    return {
        "identity": identity,
        "tier": tier_value,
        "gap_ids": gap_ids,
    }


def _gap_identity(gap: Mapping[str, Any]) -> dict[str, Any] | None:
    kind = str(gap.get("kind") or "")
    if kind not in ACTIONABLE_GAP_KINDS:
        return None
    label = str(gap.get("label") or "").strip()
    if not label:
        return None
    if kind == "missing_media":
        gap_id = str(gap.get("id") or "").strip()
        if not gap_id:
            # Produce a deterministic request coordinate when the Gap row
            # does not supply one.
            gap_id = f"missing_media:{_normalized_text(label) or 'movie'}"
        return {
            "id": gap_id,
            "kind": kind,
            "label": label,
            "reason": str(gap.get("reason") or ""),
            "title": label,
            **({"source": str(gap["source"])} if isinstance(gap.get("source"), str) else {}),
        }
    if kind == "missing_subtitle":
        gap_id = str(gap.get("id") or "").strip()
        target_video = gap.get("path")
        if not gap_id or not isinstance(target_video, str) or not target_video.startswith("/"):
            return None
            # The target video path is the pairing coordinate. Provider
        # release names only decide which subtitle payload is fetched; they
        # never decide the final sidecar path.
        return {
            "id": gap_id,
            "kind": kind,
            "label": label,
            "reason": str(gap.get("reason") or ""),
            "title": label,
            "path": target_video,
            **({"source": str(gap["source"])} if isinstance(gap.get("source"), str) else {}),
            **({"subtitle_language": str(gap["subtitle_language"])}
               if isinstance(gap.get("subtitle_language"), str) else {}),
        }
    parsed_episodes = _episode_ranges(label)
    if parsed_episodes:
        season, start, end = parsed_episodes[0]
        if season < 0 or start <= 0 or end < start:
            return None
        episodes = list(range(start, end + 1))
        episode_title = str(gap.get("title") or "").strip()
        if not episode_title:
            # Keep the historical compact title extraction for canonical
            # SxxEyy labels.  Other shared syntaxes (Chinese, ``4x17``, dual
            # ordinals) retain their complete label rather than inventing a
            # second parser merely to trim display text.
            episode_match = EPISODE_RE.search(label)
            episode_title = (
                label[episode_match.end():].strip(" -–—:：")
                if episode_match else label
            )
        raw_title_aliases = gap.get("title_aliases")
        title_aliases = _deduplicated_strings(
            raw_title_aliases if isinstance(raw_title_aliases, list) else [],
            limit=8,
        )
        primary_key = _normalized_text(episode_title)
        title_aliases = [
            value for value in title_aliases
            if _normalized_text(value) != primary_key
        ]
        source_episode_aliases: list[dict[str, Any]] = []
        for alias in gap.get("source_episode_aliases") or []:
            if not isinstance(alias, Mapping):
                continue
            source_season = alias.get("season")
            source_episode = alias.get("episode")
            series_titles = _deduplicated_strings(
                alias.get("series_titles")
                if isinstance(alias.get("series_titles"), list) else [],
                limit=4,
            )
            if (
                type(source_season) is int and source_season > 0
                and type(source_episode) is int and source_episode > 0
                and series_titles
            ):
                source_episode_aliases.append({
                    "season": source_season,
                    "episode": source_episode,
                    "series_titles": series_titles,
                })
        return {
            "id": f"S{season:02d}E{start:02d}" if start == end else f"S{season:02d}E{start:02d}-E{end:02d}",
            "kind": "missing_episode", "season": season, "episodes": episodes,
            "label": label, "reason": str(gap.get("reason") or ""),
            "season_name": str(gap.get("season_name") or "").strip(),
            **({"source": str(gap["source"])} if isinstance(gap.get("source"), str) else {}),
            "title": episode_title,
            **({"title_aliases": title_aliases} if title_aliases else {}),
            **({"source_episode_aliases": source_episode_aliases}
               if source_episode_aliases else {}),
        }
    seasons = _season_markers(label)
    if kind == "missing_season" and len(seasons) == 1:
        season = next(iter(seasons))
        if season < 0:
            return None
        season_match = SEASON_RE.search(label)
        return {
            "id": f"S{season:02d}", "kind": "missing_season", "season": season,
            "episodes": [], "label": label, "reason": str(gap.get("reason") or ""),
            "season_name": str(
                gap.get("season_name")
                or (label[season_match.end():] if season_match else label)
            ).strip(),
            **({"source": str(gap["source"])} if isinstance(gap.get("source"), str) else {}),
            "expected_episode_count": gap.get("expected_episode_count"),
        }
    return None


def _normalized_text(value: Any) -> str:
    text = unicodedata.normalize("NFKC", str(value or "")).casefold()
    return re.sub(r"[^\w\u3400-\u9fff]+", "", text)


_ANIMATION_FORMAT_MARKERS = (
    "animation", "animated", "anime", "动画", "動畫", "动漫", "動漫", "アニメ",
)
_LIVE_ACTION_FORMAT_MARKERS = (
    "live action", "live-action", "live_action", "liveaction", "真人版", "真人剧", "真人劇",
    "真人电视剧", "真人電視劇", "実写", "実寫",
)


def _identity_text_values(candidate: Mapping[str, Any]) -> list[str]:
    """Return independent, human-readable identity evidence from a candidate.

    Provider-populated ``tmdb_id`` and ``identity_match`` values are deliberately
    excluded: dynamic discovery rows can only know the requested identity, not
    the identity of an unverified release.  A release/title/file path must carry
    the requested work's own name before that row can enter selection.
    """
    values: list[str] = []
    for field in ("release_name", "title"):
        value = candidate.get(field)
        if isinstance(value, str) and value.strip():
            values.append(value.strip())
    titles = candidate.get("titles")
    if isinstance(titles, list):
        values.extend(
            value.strip() for value in titles
            if isinstance(value, str) and value.strip()
        )
    files = candidate.get("files")
    if isinstance(files, list):
        for item in files:
            if isinstance(item, str) and item.strip():
                values.append(item.strip())
            elif isinstance(item, Mapping):
                value = item.get("path") or item.get("name")
                if isinstance(value, str) and value.strip():
                    values.append(value.strip())
    return values


def _media_format_evidence(value: Any) -> set[str]:
    """Extract only explicit animation/live-action evidence.

    Unknown is intentionally not guessed.  This lets ordinary releases without
    a format label through while still failing closed on an explicit adaptation
    conflict such as ``真人电视剧版`` for an animation-library request.
    """
    text = unicodedata.normalize("NFKC", str(value or "")).casefold()
    evidence: set[str] = set()
    if any(marker in text for marker in _ANIMATION_FORMAT_MARKERS):
        evidence.add("animation")
    if any(marker in text for marker in _LIVE_ACTION_FORMAT_MARKERS):
        evidence.add("live_action")
    return evidence


def _requested_media_format(metadata: Mapping[str, Any], target_root: Any) -> str:
    for field in ("media_format", "format", "content_format"):
        evidence = _media_format_evidence(metadata.get(field))
        if len(evidence) == 1:
            return next(iter(evidence))
    root = unicodedata.normalize("NFKC", str(target_root or "")).casefold()
    # These are library taxonomy components, not fuzzy title guesses.
    if re.search(r"(?:^|/)(?:番剧|番劇|anime|animation)(?:/|$)", root):
        return "animation"
    if re.search(r"(?:^|/)(?:真人剧|真人劇|电视剧|電視劇|live.action)(?:/|$)", root):
        return "live_action"
    return ""


def _deduplicated_strings(values: Sequence[Any], *, limit: int = 40) -> list[str]:
    output: list[str] = []
    seen: set[str] = set()
    for value in values:
        text = str(value or "").strip()
        key = _normalized_text(text)
        if len(key) < 2 or key in seen:
            continue
        seen.add(key)
        output.append(text)
        if len(output) >= limit:
            break
    return output


_S00_MOVIE_ALIAS_MAX_EPISODES = 3
_S00_GENERIC_MOVIE_TITLES = frozenset({
    "special", "specialepisode", "ova", "oad", "sp", "extra", "bonus",
    "episode", "movie", "特典", "特别篇", "特別篇", "番外", "番外篇",
    "总集篇", "總集篇", "剧场版", "劇場版", "映像特典",
})
_ROMAN_SEQUEL_TOKENS = frozenset({
    "ii", "iii", "iv", "v", "vi", "vii", "viii", "ix", "x",
})
_NONZERO_SEASON_TOKEN_RE = re.compile(
    r"(?:s|season)0*[1-9]\d{0,2}(?:e(?:p)?\d{1,4})?",
    re.I,
)
_ORDINAL_SEASON_TOKEN_RE = re.compile(r"[1-9]\d{0,2}(?:st|nd|rd|th)", re.I)
_CHINESE_NONZERO_SEASON_RE = re.compile(
    r"第\s*(?:[一二三四五六七八九十百两]|[1-9]\d*)\s*季",
)
_COMPACT_NONZERO_SEASON_SUFFIX_RE = re.compile(
    r"(?:s|season)0*[1-9]\d{0,2}(?:e(?:p)?\d{1,4})?"
    r"|第(?:[一二三四五六七八九十百两]|[1-9]\d*)季",
    re.I,
)
_COMPACT_ROMAN_SEQUEL_SUFFIX_RE = re.compile(
    r"(?:viii|vii|iii|vi|iv|ix|ii|v|x)(?=(?:s|season|e|ep|\d|$))",
    re.I,
)


def _identity_tokens(value: Any) -> tuple[str, ...]:
    """Split identity text without joining an adjacent sequel marker.

    ``_normalized_text`` intentionally drops punctuation for ordinary alias
    matching.  This companion representation retains word boundaries so a
    base work such as ``Date A Live`` cannot silently match the ``II`` in a
    similarly named continuation.
    """
    text = unicodedata.normalize("NFKC", str(value or "")).casefold().replace("_", " ")
    return tuple(re.findall(r"[^\W_]+", text))


def _tokens_start_nonzero_season(tokens: Sequence[str], index: int) -> bool:
    """Return whether a token position starts an explicit later-season tag."""
    if index >= len(tokens):
        return False
    token = tokens[index]
    if token in _ROMAN_SEQUEL_TOKENS or _NONZERO_SEASON_TOKEN_RE.fullmatch(token):
        return True
    if _ORDINAL_SEASON_TOKEN_RE.fullmatch(token):
        return True
    if token in {"s", "season"} and index + 1 < len(tokens):
        following = tokens[index + 1]
        return (
            following in _ROMAN_SEQUEL_TOKENS
            or bool(re.fullmatch(r"0*[1-9]\d{0,2}", following))
            or bool(_ORDINAL_SEASON_TOKEN_RE.fullmatch(following))
        )
    return False


def _alias_has_explicit_nonzero_season_marker(alias: Any) -> bool:
    """Recognize a season/continuation marker carried by one media alias."""
    text = unicodedata.normalize("NFKC", str(alias or "")).casefold()
    tokens = _identity_tokens(text)
    return (
        bool(_CHINESE_NONZERO_SEASON_RE.search(text))
        or any(_tokens_start_nonzero_season(tokens, index) for index in range(len(tokens)))
    )


def _alias_has_unseasoned_candidate_match(
    alias: Any,
    alias_key: str,
    candidate_values: Sequence[str],
) -> bool:
    """Return whether an alias matches outside an immediately following sequel tag."""
    alias_tokens = _identity_tokens(alias)
    if not alias_tokens:
        return False
    for value in candidate_values:
        haystack = _normalized_text(value)
        if not haystack or alias_key not in haystack:
            continue
        candidate_tokens = _identity_tokens(value)
        matching_positions = [
            index
            for index in range(0, len(candidate_tokens) - len(alias_tokens) + 1)
            if candidate_tokens[index:index + len(alias_tokens)] == alias_tokens
        ]
        # Preserve the existing punctuation-insensitive identity behavior for
        # aliases which cannot be located at a word boundary, except when the
        # normalized suffix itself is an unmistakable later-season marker
        # (for example ``DateALiveS02`` or ``约会大作战第二季``).
        if not matching_positions:
            offsets = [
                index for index in range(len(haystack))
                if haystack.startswith(alias_key, index)
            ]
            if any(
                not (
                    _COMPACT_NONZERO_SEASON_SUFFIX_RE.match(
                        haystack[index + len(alias_key):],
                    )
                    or _COMPACT_ROMAN_SEQUEL_SUFFIX_RE.match(
                        haystack[index + len(alias_key):],
                    )
                )
                for index in offsets
            ):
                return True
            continue
        if any(
            not _tokens_start_nonzero_season(
                candidate_tokens, index + len(alias_tokens),
            )
            for index in matching_positions
        ):
            return True
    return False


def _is_s00_missing_episode(gap: Mapping[str, Any]) -> bool:
    if gap.get("kind") != "missing_episode":
        return False
    if gap.get("season") == 0:
        return True
    return bool(re.fullmatch(r"S00E\d{2,4}(?:-E\d{2,4})?", str(gap.get("id") or "")))


def _strict_s00_gap_title_key(value: Any, *, media_keys: set[str]) -> str | None:
    """Return a gap-local title only when it is specific enough to override a sequel alias."""
    text = unicodedata.normalize("NFKC", str(value or "")).casefold()
    key = _normalized_text(text)
    if (
        len(key) < 4
        or key in media_keys
        or key in _S00_GENERIC_MOVIE_TITLES
    ):
        return None
    # One unqualified Latin word (for example ``Judgement``) is not a strict
    # special-title proof.  Non-Latin titles do not require whitespace, while
    # a two-word Latin title retains enough specificity for this narrow guard.
    latin_words = re.findall(r"[a-z0-9]+", text)
    if key.isascii() and len(latin_words) < 2:
        return None
    return key


def _has_strict_s00_gap_evidence(
    request: Mapping[str, Any],
    candidate_values: Sequence[str],
    *,
    media_keys: set[str],
) -> bool:
    """Find current-gap evidence without promoting it to a work alias.

    This is deliberately an exception only for an S00 row otherwise matched
    solely by a continuation alias.  A non-generic official special title, or
    a Gap-declared source coordinate plus source series title, can disprove
    that the continuation suffix is the only identity evidence.  The values
    remain gap-local: they are never copied into ``media.aliases``.
    """
    normalized_values = [_normalized_text(value) for value in candidate_values]
    for raw_gap in (
        request.get("gaps") if isinstance(request.get("gaps"), list) else []
    ):
        if not isinstance(raw_gap, Mapping) or not _is_s00_missing_episode(raw_gap):
            continue
        title_keys = {
            key
            for value in [
                raw_gap.get("title"),
                *(
                    raw_gap.get("title_aliases")
                    if isinstance(raw_gap.get("title_aliases"), list) else []
                ),
            ]
            if (key := _strict_s00_gap_title_key(value, media_keys=media_keys))
        }
        if any(
            title_key in candidate_value
            for title_key in title_keys
            for candidate_value in normalized_values
            if candidate_value
        ):
            return True

        for source_alias in raw_gap.get("source_episode_aliases") or []:
            if not isinstance(source_alias, Mapping):
                continue
            season = source_alias.get("season")
            episode = source_alias.get("episode")
            if (
                type(season) is not int or season <= 0
                or type(episode) is not int or episode <= 0
            ):
                continue
            source_titles = {
                key
                for value in source_alias.get("series_titles") or []
                if isinstance(value, str)
                and len(key := _normalized_text(value)) >= 3
            }
            if not source_titles:
                continue
            source_id = f"S{season:02d}E{episode:02d}"
            has_source_coordinate = any(
                source_id in _expanded_episode_ids(value)
                for value in candidate_values
            )
            has_source_title = any(
                source_title in candidate_value
                for source_title in source_titles
                for candidate_value in normalized_values
                if candidate_value
            )
            if has_source_coordinate and has_source_title:
                return True
    return False


def _tmdb_title_values(row: Mapping[str, Any]) -> list[str]:
    """Return only title fields suitable for an identity-bearing TMDB match."""
    return [
        value.strip() for key in (
            "title", "name", "original_title", "original_name",
        )
        if isinstance((value := row.get(key)), str) and value.strip()
    ]


def _specific_s00_movie_title(
    value: Any, *, tv_identity_keys: set[str],
) -> tuple[str, str] | None:
    """Accept a title only when it is useful, non-generic S00 movie evidence."""
    title = str(value or "").strip()
    key = _normalized_text(title)
    if not key or key in tv_identity_keys or key in _S00_GENERIC_MOVIE_TITLES:
        return None
    # A bare ordinal or an S00 coordinate is episode bookkeeping, never a
    # movie identity.  Keeping this narrow prevents a large special-season
    # gap set from turning generic labels into broad movie searches.
    if re.fullmatch(r"(?:s\d{1,3}e)?\d{1,4}(?:集|话|話)?", key):
        return None
    has_han = any("\u3400" <= char <= "\u9fff" for char in key)
    if len(key) < 3 and not (has_han and len(key) >= 2):
        return None
    return title, key


def _s00_movie_aliases(
    getter: object,
    *,
    title: str,
    title_key: str,
    tv_identity_keys: set[str],
) -> list[str]:
    """Resolve one explicitly titled S00 movie, or return no aliases.

    A movie search result is accepted only when exactly one result carries
    both the special's own title and the already-verified TV identity.  Every
    subsequent TMDB request must also succeed before any new alias is used;
    this deliberately favors a missed search over a cross-work expansion.
    """
    if not callable(getter):
        return []
    try:
        search = getter("/search/movie", query=title)
    except Exception:
        return []
    rows = search.get("results") if isinstance(search, Mapping) else None
    if not isinstance(rows, list):
        return []

    matching_ids: set[int] = set()
    for row in rows:
        if not isinstance(row, Mapping):
            continue
        movie_id = row.get("id")
        if type(movie_id) is not int or movie_id <= 0:
            continue
        evidence = [_normalized_text(value) for value in _tmdb_title_values(row)]
        if not any(title_key in value for value in evidence):
            continue
        if not any(
            identity_key in value
            for identity_key in tv_identity_keys
            for value in evidence
        ):
            continue
        matching_ids.add(movie_id)
    if len(matching_ids) != 1:
        return []
    movie_id = next(iter(matching_ids))

    try:
        movie = getter(f"/movie/{movie_id}")
        alternatives = getter(f"/movie/{movie_id}/alternative_titles")
    except Exception:
        return []
    if not isinstance(movie, Mapping) or not isinstance(alternatives, Mapping):
        return []
    returned_id = movie.get("id")
    if returned_id is not None and (
        type(returned_id) is not int or returned_id != movie_id
    ):
        return []
    alternative_rows = alternatives.get("titles")
    if not isinstance(alternative_rows, list):
        alternative_rows = alternatives.get("results")
    if not isinstance(alternative_rows, list):
        return []

    values: list[Any] = list(_tmdb_title_values(movie))
    for row in alternative_rows:
        if isinstance(row, Mapping):
            values.extend(_tmdb_title_values(row))
    return _deduplicated_strings(values, limit=16)


def _enrich_small_s00_movie_aliases(
    output: dict[str, Any],
    getter: object,
    *,
    tmdb_id: int,
    tv_aliases: Sequence[str],
) -> None:
    """Add movie aliases only for one small, unambiguous TV S00 request."""
    scan_report = output.get("scan_report")
    if not isinstance(scan_report, Mapping):
        return
    raw_gaps = scan_report.get("resource_gaps")
    if not isinstance(raw_gaps, list):
        return
    tv_identity_keys = {
        key for value in tv_aliases
        if (key := _normalized_text(value))
    }
    if not tv_identity_keys:
        return

    # One range can encode a whole special season, so bound by the number of
    # requested episodes rather than by only the number of JSON gap rows.
    s00_episode_count = 0
    titles: dict[str, tuple[str, list[int]]] = {}
    for index, raw_gap in enumerate(raw_gaps):
        if not isinstance(raw_gap, Mapping):
            continue
        media = raw_gap.get("media")
        if isinstance(media, Mapping) and "tmdb_id" in media:
            raw_media_id = media.get("tmdb_id")
            if type(raw_media_id) is not int or raw_media_id != tmdb_id:
                continue
        identity = _gap_identity(raw_gap)
        if (
            identity is None
            or identity.get("kind") != "missing_episode"
            or identity.get("season") != 0
        ):
            continue
        episodes = identity.get("episodes")
        if not isinstance(episodes, list) or not episodes:
            return
        s00_episode_count += len(episodes)
        # A missing S00 title is not permission to search aliases or labels.
        # The Gap's explicit episode title is the only allowed query.
        specific = _specific_s00_movie_title(
            raw_gap.get("title"), tv_identity_keys=tv_identity_keys,
        )
        if specific is None:
            continue
        title, key = specific
        stored = titles.get(key)
        if stored is None:
            titles[key] = (title, [index])
        else:
            stored[1].append(index)

    if (
        not titles
        or s00_episode_count > _S00_MOVIE_ALIAS_MAX_EPISODES
        or len(titles) > _S00_MOVIE_ALIAS_MAX_EPISODES
    ):
        return

    resolved: dict[int, list[str]] = {}
    for title, indexes in titles.values():
        title_key = _normalized_text(title)
        aliases = _s00_movie_aliases(
            getter,
            title=title,
            title_key=title_key,
            tv_identity_keys=tv_identity_keys,
        )
        if not aliases:
            continue
        for index in indexes:
            resolved[index] = aliases
    if not resolved:
        return

    changed = False
    updated_gaps: list[Any] = list(raw_gaps)
    for index, aliases in resolved.items():
        raw_gap = raw_gaps[index]
        if not isinstance(raw_gap, Mapping):
            continue
        existing = raw_gap.get("title_aliases")
        combined = _deduplicated_strings(
            [
                *(existing if isinstance(existing, list) else []),
                *aliases,
            ],
            limit=16,
        )
        if combined == (existing if isinstance(existing, list) else []):
            continue
        row = dict(raw_gap)
        row["title_aliases"] = combined
        updated_gaps[index] = row
        changed = True
    if changed:
        report_copy = dict(scan_report)
        report_copy["resource_gaps"] = updated_gaps
        output["scan_report"] = report_copy


def enrich_replenishment_plan_aliases(
    plan: Mapping[str, Any],
    tmdb_client: object | None,
) -> dict[str, Any]:
    """Add authoritative TMDB title aliases to one provider request plan.

    A task's persisted identity can contain only a translated title. Provider
    releases commonly use TMDB's original or alternative title (for example,
    an English release for a Chinese NFO). Fetching aliases for the already
    confirmed TMDB id preserves the strict identity check: aliases come from
    that exact TMDB record, while the candidate still has to contain one of
    them in its release name.

    This helper is best-effort and leaves the input unchanged when the client
    or response is unavailable.  The bounded copy keeps provider query size
    and persisted plan state predictable.
    """
    output = copy.deepcopy(dict(plan))
    metadata = output.get("metadata")
    if not isinstance(metadata, Mapping):
        return output
    raw_id = metadata.get("tmdb_id")
    if isinstance(raw_id, bool):
        return output
    try:
        tmdb_id = int(raw_id)
    except (TypeError, ValueError):
        return output
    if tmdb_id <= 0:
        return output
    getter = getattr(tmdb_client, "get", None)
    if not callable(getter):
        return output
    mode = str(
        metadata.get("media_type")
        or metadata.get("type")
        or output.get("mode")
        or "tv"
    ).casefold()
    endpoint = "movie" if mode == "movie" else "tv"
    values: list[Any] = [metadata.get("title"), metadata.get("original_title")]
    # The primary TMDB record is the authoritative source for the canonical
    # localized and original titles.  Alternative-title responses often omit
    # one or both (notably a canonical English release name), so consult both
    # endpoints independently.  A small fake or a transient upstream failure
    # on either endpoint must not discard useful evidence from the other.
    try:
        primary = getter(f"/{endpoint}/{tmdb_id}")
    except Exception:
        primary = None
    if isinstance(primary, Mapping):
        values.extend(_tmdb_title_values(primary))
    # The configured TMDB locale can be a local display language whose record
    # omits a globally used release title.  Put the authoritative English TV
    # primary/original fields ahead of user-held alternatives so the bounded
    # alias list cannot squeeze out a base-work identity such as Date A Live.
    # This is intentionally TV-only; a movie's primary record already names
    # the actual work being materialized.
    if endpoint == "tv":
        try:
            english_primary = getter(f"/tv/{tmdb_id}", language="en-US")
        except Exception:
            english_primary = None
        if isinstance(english_primary, Mapping):
            values.extend(_tmdb_title_values(english_primary))
    existing = metadata.get("aliases")
    if isinstance(existing, list):
        values.extend(existing)
    try:
        alternatives = getter(f"/{endpoint}/{tmdb_id}/alternative_titles")
    except Exception:
        alternatives = None
    if isinstance(alternatives, Mapping):
        rows = alternatives.get("results")
        if not isinstance(rows, list):
            rows = alternatives.get("titles")
        if isinstance(rows, list):
            for row in rows:
                if not isinstance(row, Mapping):
                    continue
                values.extend(_tmdb_title_values(row))
    aliases = _deduplicated_strings(values, limit=24)
    if not aliases:
        return output
    metadata_copy = dict(metadata)
    metadata_copy["aliases"] = aliases
    output["metadata"] = metadata_copy
    # TMDB represents some TV special-season entries as standalone movies.
    # Keep that bridge intentionally tiny and identity-bound: it enriches
    # query-only gap aliases in memory and never changes the persisted root.
    if endpoint == "tv":
        _enrich_small_s00_movie_aliases(
            output, getter, tmdb_id=tmdb_id, tv_aliases=aliases,
        )
    return output


def _chinese_number(number: int) -> str | None:
    if 0 <= number <= 10:
        return CHINESE_NUMERALS[number]
    if 10 < number < 20:
        return "十" + CHINESE_NUMERALS[number - 10]
    if 20 <= number < 100:
        tens, ones = divmod(number, 10)
        return CHINESE_NUMERALS[tens] + "十" + (CHINESE_NUMERALS[ones] if ones else "")
    return None


def suppress_gaps_satisfied_by_planned_videos(
    plan: Mapping[str, Any],
) -> tuple[dict[str, Any], list[str]]:
    """Drop pre-execution gaps that this exact plan has just committed.

    ``scan_report.resource_gaps`` is produced while the target library is still
    empty.  Post-commit replenishment must not treat those stale observations
    as missing when a verified video ``final_name`` in the same target already
    carries the episode token.  This only removes a gap when every episode in
    that gap is covered; partial/range evidence remains fail-closed.
    """
    output = copy.deepcopy(dict(plan))
    scan_report = output.get("scan_report")
    if not isinstance(scan_report, dict):
        return output, []
    raw_gaps = scan_report.get("resource_gaps")
    if not isinstance(raw_gaps, list):
        return output, []
    planned: list[tuple[str, set[str]]] = []
    for item in output.get("files") or []:
        if not isinstance(item, Mapping) or item.get("media_kind") != "video":
            continue
        target_dir = str(item.get("target_dir") or "").rstrip("/")
        tokens = _expanded_episode_ids(item.get("final_name"))
        if target_dir and tokens:
            planned.append((target_dir, tokens))
    retained: list[Any] = []
    removed: list[str] = []
    for raw in raw_gaps:
        identity = _gap_identity(raw) if isinstance(raw, Mapping) else None
        if identity is None:
            retained.append(raw)
            continue
        gap_ids = _expanded_episode_ids(identity.get("id"))
        if not gap_ids and identity.get("kind") == "missing_season":
            count = identity.get("expected_episode_count")
            season = identity.get("season")
            if type(count) is int and count > 0 and type(season) is int:
                gap_ids = {f"S{season:02d}E{episode:02d}" for episode in range(1, count + 1)}
        media = raw.get("media") if isinstance(raw, Mapping) and isinstance(raw.get("media"), Mapping) else {}
        target_root = str(media.get("target_root") or "").rstrip("/")
        covered = {
            token
            for target_dir, tokens in planned
            if not target_root or target_dir == target_root or target_dir.startswith(target_root + "/")
            for token in tokens
        }
        if gap_ids and gap_ids <= covered:
            removed.extend(sorted(gap_ids))
        else:
            retained.append(raw)
    scan_report["resource_gaps"] = retained
    return output, sorted(set(removed))


def suppress_request_gaps_present_in_names(
    request: Mapping[str, Any], names: Sequence[str],
) -> tuple[dict[str, Any], list[str]]:
    """Revalidate a request against freshly listed target video names."""
    output = copy.deepcopy(dict(request))
    present = set().union(*(_expanded_episode_ids(name) for name in names)) if names else set()
    retained: list[Any] = []
    removed: list[str] = []
    for gap in output.get("gaps") or []:
        if not isinstance(gap, Mapping):
            retained.append(gap)
            continue
        gap_ids = _expanded_episode_ids(gap.get("id"))
        if gap_ids and gap_ids <= present:
            removed.extend(sorted(gap_ids))
        else:
            retained.append(gap)
    output["gaps"] = retained
    output["query_groups"] = _query_groups([
        gap for gap in retained if isinstance(gap, Mapping)
    ])
    media = output.get("media") if isinstance(output.get("media"), Mapping) else {}
    output["search_queries"] = _search_queries(
        media.get("aliases") if isinstance(media.get("aliases"), list) else [],
        str(media.get("year") or ""), output["query_groups"],
    )
    return output, sorted(set(removed))


def _request_gap_ids(request: Mapping[str, Any]) -> tuple[set[str], dict[str, dict[str, Any]]]:
    lookup: dict[str, dict[str, Any]] = {}
    for gap in request.get("gaps") if isinstance(request.get("gaps"), list) else []:
        if not isinstance(gap, Mapping) or not isinstance(gap.get("id"), str):
            continue
        identity = str(gap["id"])
        # A subtitle's ID can legitimately embed ``S01E01`` while still
        # identifying a *sidecar* rather than the episode video itself.  Keep
        # it whole so candidate coverage, durable gap state and the exact
        # formal-library video path all use the same coordinate.  Expanding it
        # to ``S01E01`` here used to make a successful download appear
        # unresolved because the coordinator could no longer find its state
        # file by the original subtitle-gap ID.
        if gap.get("kind") in {"missing_media", "missing_subtitle"}:
            lookup[identity] = dict(gap)
            continue
        expanded = _expanded_episode_ids(identity)
        if expanded:
            for item in expanded:
                lookup[item] = dict(gap)
        else:
            lookup[identity] = dict(gap)
    return set(lookup), lookup


def _query_groups(gaps: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    seasons: dict[int, set[int]] = defaultdict(set)
    season_names: dict[int, list[str]] = defaultdict(list)
    episode_titles: dict[int, list[str]] = defaultdict(list)
    whole_seasons: set[int] = set()
    for gap in gaps:
        season = gap.get("season")
        if not isinstance(season, int) or season < 0:
            continue
        episodes = gap.get("episodes")
        season_name = str(gap.get("season_name") or "").strip()
        if season_name:
            season_names[season].append(season_name)
        episode_titles[season].extend(_deduplicated_strings([
            gap.get("title"),
            *(
                gap.get("title_aliases")
                if isinstance(gap.get("title_aliases"), list) else []
            ),
        ], limit=12))
        if gap.get("kind") == "missing_season" or not isinstance(episodes, list) or not episodes:
            whole_seasons.add(season)
        else:
            seasons[season].update(int(item) for item in episodes if isinstance(item, int) and item > 0)
    groups: list[dict[str, Any]] = []
    for season in sorted(set(seasons) | whole_seasons):
        episodes = sorted(seasons.get(season, set()))
        if season in whole_seasons:
            token = f"S{season:02d}"
        elif episodes and episodes == list(range(episodes[0], episodes[-1] + 1)):
            token = f"S{season:02d}E{episodes[0]:02d}" + (f"-E{episodes[-1]:02d}" if len(episodes) > 1 else "")
        else:
            token = " ".join(f"S{season:02d}E{item:02d}" for item in episodes)
        groups.append({
            "season": season, "episodes": episodes, "token": token,
            "season_names": _deduplicated_strings(season_names.get(season, []), limit=8),
            "episode_titles": _deduplicated_strings(
                episode_titles.get(season, []), limit=120,
            ),
        })
    return groups


def _search_queries(aliases: Sequence[str], year: str, groups: Sequence[Mapping[str, Any]]) -> list[str]:
    values: list[str] = []
    bounded_aliases = list(aliases[:8])
    # Give every bounded official alias a chance before any one alias expands
    # into dozens of syntax variants.  This prevents the 120-query ceiling
    # from dropping an English TMDB alias that appears after the localized
    # primary/original names.
    for alias in bounded_aliases:
        values.extend([alias, f"{alias} {year}".strip()])
    for group in groups:
        for alias in bounded_aliases:
            values.append(f"{alias} {group['token']}")
            values.extend(
                f"{alias} {season_name}"
                for season_name in group.get("season_names") or []
                if isinstance(season_name, str) and season_name.strip()
            )
        # Exact episode-title searches are highest value for S00 resources;
        # combine them with the two authoritative localized identities before
        # adding broad season spelling variants.
        for alias in bounded_aliases[:2]:
            values.extend(
                f"{alias} {episode_title}"
                for episode_title in group.get("episode_titles") or []
                if isinstance(episode_title, str) and episode_title.strip()
            )
    for alias in bounded_aliases:
        for group in groups:
            season = int(group["season"]); token = str(group["token"])
            episodes = [int(item) for item in group.get("episodes") or []]
            chinese = _chinese_number(season)
            values.extend([
                f"{alias} {token}", f"{alias} S{season:02d}",
                f"{alias} S{season}",
                f"{alias} Season {season}", f"{alias} 第{season}季",
                f"{alias} 第{chinese}季" if chinese else "",
                f"{alias} {year} {token}".strip(),
            ])
            if episodes:
                start, end = episodes[0], episodes[-1]
                suffix = str(start) if start == end else f"{start}-{end}"
                values.extend([
                    f"{alias} S{season}E{start}" + (f"-E{end}" if end != start else ""),
                    f"{alias} {season}x{suffix}",
                    f"{alias} 第{season}季 第{suffix}集",
                ])
            else:
                values.append(f"{alias} 第{season}季 全集")
    return _deduplicated_strings(values, limit=120)


def build_replenishment_request(
    plan: Mapping[str, Any], *, job_id: str, round_number: int,
) -> dict[str, Any]:
    """Build a versioned request with reusable aliases and gap-focused queries."""
    metadata = plan.get("metadata") if isinstance(plan.get("metadata"), Mapping) else {}
    scan_report = plan.get("scan_report") if isinstance(plan.get("scan_report"), Mapping) else {}
    raw_gaps = scan_report.get("resource_gaps")
    gaps = [identity for raw in (raw_gaps if isinstance(raw_gaps, list) else [])
            if isinstance(raw, Mapping) and (identity := _gap_identity(raw)) is not None]
    deduplicated = {str(gap["id"]): gap for gap in gaps}
    gaps = list(deduplicated.values())
    source_label = PurePosixPath(str(plan.get("source_root") or "")).name
    metadata_aliases = metadata.get("aliases") if isinstance(metadata.get("aliases"), list) else []
    aliases = _deduplicated_strings([
        metadata.get("title"), metadata.get("original_title"), *metadata_aliases,
        source_label,
    ])
    groups = _query_groups(gaps)
    year = str(metadata.get("year") or "").strip()
    optional_only = bool(gaps) and all(gap.get("season") == 0 for gap in gaps)
    target_root = str(metadata.get("series_root") or plan.get("target_root") or "")
    media_format = _requested_media_format(metadata, target_root)
    request = {
        "version": 2, "job_id": job_id, "round": round_number,
        "media": {
            "title": str(metadata.get("title") or "").strip(), "aliases": aliases,
            "year": year, "tmdb_id": metadata.get("tmdb_id"),
            "target_root": target_root,
            "media_type": (
                "movie"
                if str(plan.get("mode") or "").casefold() == "movie"
                else "tv"
            ),
            **({"media_format": media_format} if media_format else {}),
        },
        "gaps": gaps, "query_groups": groups,
        "search_queries": _search_queries(aliases, year, groups),
        "rules": {
            "require_title_identity": True, "require_name_coverage": True,
            "file_listing_restricts_claimed_coverage": True,
            "provider_order": list(PROVIDER_ORDER),
            "provider_fallback_is_per_gap": True,
            "quality_ladder": ["2160p", "1080p", "720p"],
            "allow_720p_only_without_1080p_or_better": True,
            "newest_before_quality_within_provider": True,
            "allow_multiple_candidates_to_cover_all_gaps": True,
            # Season 00 uses OVA/OAD numbering while remaining a required
            # acquisition lane.
            **({
                "optional_discovery_only": True,
                "season_zero_replenishment_required": True,
            } if optional_only else {}),
        },
    }
    lane = replenishment_request_lane(gaps)
    if lane is not None:
        request["lane"] = lane
        if lane == "subtitle":
            # Keep the sidecar payload intentionally small.  Video tier order,
            # quality ladders and candidate fallback policy are not part of a
            # subtitle repair request and must not be mistaken for executable
            # video-provider instructions by an adapter or a future caller.
            request["rules"] = {
                "subtitle_only": True,
                "require_title_identity": True,
                "require_target_video_path": True,
                "allowed_formats": sorted(
                    extension.lstrip(".") for extension in SUBTITLE_EXTENSIONS
                ),
            }
    return request


def replenishment_request_lane(
    gaps: Sequence[Mapping[str, Any]],
) -> str | None:
    """Return the one legal execution lane for a homogeneous gap set.

    An explicit subtitle gap repairs an already-existing video's sidecar and
    must never inherit video candidates, tiers, attempts, or child planning.
    ``None`` is deliberately fail-closed for empty, unsupported, or mixed
    rows; callers must split before dispatching rather than guessing.
    """
    kinds = {
        str(gap.get("kind") or "")
        for gap in gaps
        if isinstance(gap, Mapping)
    }
    if kinds == {SUBTITLE_GAP_KIND}:
        return "subtitle"
    if kinds and kinds <= MEDIA_GAP_KINDS:
        return "media"
    return None


def _plan_for_replenishment_lane(
    plan: Mapping[str, Any], gaps: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    """Copy one plan while replacing only its provider gap projection."""
    lane_plan = dict(plan)
    scan_report = (
        dict(plan.get("scan_report"))
        if isinstance(plan.get("scan_report"), Mapping)
        else {}
    )
    scan_report["resource_gaps"] = [dict(gap) for gap in gaps]
    lane_plan["scan_report"] = scan_report
    return lane_plan


def build_replenishment_requests(
    plan: Mapping[str, Any], *, job_id: str, round_number: int,
) -> dict[str, Any]:
    """Build one request per media identity and per execution lane."""
    metadata = plan.get("metadata") if isinstance(plan.get("metadata"), Mapping) else {}
    raw_gaps = (
        plan.get("scan_report", {}).get("resource_gaps")
        if isinstance(plan.get("scan_report"), Mapping) else []
    )
    gaps: list[dict[str, Any]] = []
    unresolved: list[dict[str, Any]] = []
    for raw_gap in raw_gaps if isinstance(raw_gaps, list) else []:
        if not isinstance(raw_gap, Mapping):
            unresolved.append({
                "kind": "invalid_gap",
                "reason": "resource gap 不是对象，无法安全补源",
            })
            continue
        gap = dict(raw_gap)
        # Do not silently discard an unsupported or incomplete Gap row. It
        # must remain visible to the root barrier instead of being mistaken
        # for a plan with no work.
        if _gap_identity(gap) is None:
            unresolved.append(gap)
            continue
        gaps.append(gap)
    identities: dict[int, dict[str, Any]] = {}
    raw_tmdb_id = metadata.get("tmdb_id")
    if type(raw_tmdb_id) is int and raw_tmdb_id > 0 and plan.get("mode") in {"tv", "mixed"}:
        identities[raw_tmdb_id] = {
            "tmdb_id": raw_tmdb_id, "title": metadata.get("title"),
            "original_title": metadata.get("original_title"), "year": metadata.get("year"),
            "aliases": metadata.get("aliases"),
            "target_root": metadata.get("series_root") or plan.get("target_root"),
            "season_posters": metadata.get("season_posters"),
            "media_format": metadata.get("media_format") or metadata.get("format")
            or metadata.get("content_format"),
        }
    members = metadata.get("member_tv")
    if isinstance(members, Mapping):
        for target_root, identity in members.items():
            if not isinstance(identity, Mapping):
                continue
            tmdb_id = identity.get("tmdb_id")
            if type(tmdb_id) is int and tmdb_id > 0:
                identities[tmdb_id] = {**dict(identity), "target_root": str(target_root)}
    if not identities:
        requests: list[dict[str, Any]] = []
        for lane, lane_kinds in (
            ("media", MEDIA_GAP_KINDS),
            ("subtitle", frozenset({SUBTITLE_GAP_KIND})),
        ):
            lane_gaps = [
                gap for gap in gaps
                if str(gap.get("kind") or "") in lane_kinds
            ]
            if not lane_gaps:
                continue
            request = build_replenishment_request(
                _plan_for_replenishment_lane(plan, lane_gaps),
                job_id=job_id,
                round_number=round_number,
            )
            if request.get("gaps"):
                request["lane"] = lane
                requests.append(request)
        return {"version": 1, "requests": requests, "unresolved_gaps": unresolved}

    grouped: dict[tuple[int, str], list[dict[str, Any]]] = defaultdict(list)
    for gap in gaps:
        identity_id: int | None = None
        media = gap.get("media") if isinstance(gap.get("media"), Mapping) else {}
        explicit_id = media.get("tmdb_id")
        if type(explicit_id) is int and explicit_id in identities:
            identity_id = explicit_id
        elif len(identities) == 1:
            identity_id = next(iter(identities))
        else:
            parsed = _gap_identity(gap)
            season = parsed.get("season") if parsed else None
            season_matches = [
                tmdb_id for tmdb_id, identity in identities.items()
                if isinstance(identity.get("season_posters"), Mapping)
                and str(season) in identity["season_posters"]
            ] if isinstance(season, int) else []
            if len(season_matches) == 1:
                identity_id = season_matches[0]
            else:
                label_key = _normalized_text(gap.get("label"))
                title_matches = [
                    tmdb_id for tmdb_id, identity in identities.items()
                    if (title_key := _normalized_text(identity.get("title")))
                    and title_key in label_key
                ]
                if len(title_matches) == 1:
                    identity_id = title_matches[0]
        lane = replenishment_request_lane([gap])
        if identity_id is None or lane is None:
            unresolved.append(gap)
        else:
            grouped[(identity_id, lane)].append(gap)

    requests: list[dict[str, Any]] = []
    for (tmdb_id, lane), project_gaps in grouped.items():
        identity = identities[tmdb_id]
        aliases = [
            identity.get("title"), identity.get("original_title"),
            *(identity.get("aliases") if isinstance(identity.get("aliases"), list) else []),
            PurePosixPath(str(identity.get("target_root") or "")).name,
        ]
        project_plan = {
            "mode": "tv",
            "source_root": plan.get("source_root"),
            "target_root": identity.get("target_root"),
            "metadata": {
                "tmdb_id": tmdb_id, "title": identity.get("title"),
                "original_title": identity.get("original_title"),
                "aliases": aliases, "year": identity.get("year"),
                "media_format": identity.get("media_format"),
            },
            "scan_report": {"resource_gaps": project_gaps},
        }
        request = build_replenishment_request(
            project_plan, job_id=job_id, round_number=round_number,
        )
        request["lane"] = lane
        request["project_key"] = (
            f"tmdb:tv:{tmdb_id}:subtitle"
            if lane == "subtitle"
            else f"tmdb:tv:{tmdb_id}"
        )
        requests.append(request)
    return {
        "version": 1, "requests": requests,
        "unresolved_gaps": unresolved,
    }


def _timestamp(value: Any) -> float:
    if not isinstance(value, str) or not value.strip():
        return 0.0
    try:
        parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    except ValueError:
        return 0.0
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.timestamp()


def _swarm_count(value: Any) -> int | None:
    """Parse a bounded non-negative swarm count without trusting coercion."""
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value if 0 <= value <= _SWARM_COUNT_LIMIT else None
    if isinstance(value, str) and re.fullmatch(r"\d{1,10}", value.strip()):
        parsed = int(value.strip())
        return parsed if parsed <= _SWARM_COUNT_LIMIT else None
    return None


def _swarm_timestamp(value: Any) -> float:
    """Accept only an explicit ISO/epoch observation timestamp."""
    if isinstance(value, bool):
        return 0.0
    if isinstance(value, (int, float)):
        if value != value or value < 0:  # NaN and negative epochs are invalid.
            return 0.0
        return float(value)
    return _timestamp(value)


def _swarm_preference(candidate: Mapping[str, Any]) -> tuple[int, int, int]:
    """Return a liveness-only sort key after candidate hard gates.

    ``2`` means a fresh positive seed observation, ``0`` an explicit fresh
    zero-seed observation, and ``1`` neutral (missing, stale or malformed).
    The helper is intentionally observational: callers use it only in sort
    keys after identity and manifest/coverage validation has accepted a row.
    """
    nested = candidate.get("swarm")
    source: Mapping[str, Any] = nested if isinstance(nested, Mapping) else candidate
    seed_value = next(
        (source.get(key) for key in ("seeders", "seeds", "seed") if key in source),
        None,
    )
    seeds = _swarm_count(seed_value)
    if seeds is None:
        return (1, 0, 0)
    leecher_value = next(
        (
            source.get(key)
            for key in ("leechers", "leeches", "peers")
            if key in source
        ),
        None,
    )
    leechers = _swarm_count(leecher_value)
    if leechers is None:
        leechers = 0
    observed_value = next(
        (
            source.get(key)
            for key in ("observed_at", "swarm_observed_at", "tracker_updated")
            if key in source
        ),
        None,
    )
    if observed_value is None and source is not candidate:
        observed_value = next(
            (
                candidate.get(key)
                for key in ("swarm_observed_at", "observed_at", "tracker_updated")
                if key in candidate
            ),
            None,
        )
    observed = _swarm_timestamp(observed_value)
    now = datetime.now(timezone.utc).timestamp()
    if (
        observed <= 0
        or now - observed > _SWARM_MAX_AGE_SECONDS
        or observed - now > _SWARM_MAX_FUTURE_SKEW_SECONDS
    ):
        return (1, 0, 0)
    if seeds == 0:
        return (0, 0, 0)
    return (2, seeds, leechers)


def _normalized_quality(value: Any) -> str:
    text = unicodedata.normalize("NFKC", str(value or "unknown")).casefold()
    if any(marker in text for marker in ("2160", "4k", "uhd", "3840x2160")):
        return "2160p"
    if any(marker in text for marker in ("1080", "fhd", "1920x1080")):
        return "1080p"
    if any(marker in text for marker in ("720", "1280x720")):
        return "720p"
    return "unknown"


def _candidate_file_coverage(candidate: Mapping[str, Any], seasons: set[int]) -> tuple[bool, set[str]]:
    values: list[str] = []
    supplied = False
    for field in ("file_coverage", "files"):
        raw = candidate.get(field)
        if not isinstance(raw, list):
            continue
        supplied = True
        for item in raw:
            if isinstance(item, str):
                values.append(item)
            elif isinstance(item, Mapping):
                name = item.get("name") or item.get("path")
                if isinstance(name, str):
                    values.append(name)
    return supplied, _coverage_tokens(values, default_seasons=seasons)


def _candidate_has_video_file(candidate: Mapping[str, Any]) -> bool:
    """Return true only when candidate evidence names a real video file."""
    raw_files = candidate.get("files")
    if not isinstance(raw_files, list):
        return False
    for item in raw_files:
        value = item if isinstance(item, str) else (
            item.get("path") or item.get("name")
            if isinstance(item, Mapping) else None
        )
        if isinstance(value, str) and PurePosixPath(value).suffix.casefold() in _VIDEO_SUFFIXES:
            return True
    return False


def _candidate_has_subtitle_file(candidate: Mapping[str, Any]) -> bool:
    """Return true only when candidate evidence names a subtitle payload."""
    raw_files = candidate.get("files")
    if not isinstance(raw_files, list):
        return False
    return any(
        isinstance(value, str)
        and PurePosixPath(value).suffix.casefold() in SUBTITLE_EXTENSIONS
        for item in raw_files
        for value in [
            item if isinstance(item, str) else (
                item.get("path") or item.get("name")
                if isinstance(item, Mapping) else None
            )
        ]
    )


def _subtitle_file_matches_language(path: str, requested: object) -> bool:
    """Keep an automatic subtitle request on its configured language lane.

    A candidate without an explicit language marker remains a search miss so
    the automatic retry can choose another release instead of attaching the
    wrong language sidecar.
    """
    language = str(requested or "").strip().casefold()
    if not language:
        return True
    marker = re.sub(r"[^a-z0-9\u3400-\u9fff]+", " ", path.casefold())
    if any(token in language for token in ("zh", "中文", "chinese", "简", "繁")):
        return any(token in marker for token in ("zh", "zho", "chi", "chs", "cht", "中文", "简", "繁"))
    if any(token in language for token in ("en", "english", "英文", "英语")):
        return any(token in marker for token in (" en ", "eng", "english", "英文", "英语"))
    if any(token in language for token in ("ja", "japanese", "日文", "日语")):
        return any(token in marker for token in (" ja ", "jpn", "japanese", "日文", "日语"))
    return True


def _candidate_has_subtitle_for_gap(candidate: Mapping[str, Any], gap: Mapping[str, Any]) -> bool:
    raw_files = candidate.get("files")
    if not isinstance(raw_files, list):
        return False
    return any(
        isinstance(value, str)
        and PurePosixPath(value).suffix.casefold() in SUBTITLE_EXTENSIONS
        and _subtitle_file_matches_language(value, gap.get("subtitle_language"))
        for item in raw_files
        for value in [
            item if isinstance(item, str) else (
                item.get("path") or item.get("name")
                if isinstance(item, Mapping) else None
            )
        ]
    )


def _exact_subtitle_file_coverage(
    candidate: Mapping[str, Any], gap_lookup: Mapping[str, Mapping[str, Any]],
) -> set[str]:
    """Return only subtitle gaps explicitly paired to a candidate file.

    A subtitle-only request may contain many targeted videos. Seeing *a*
    Chinese sidecar in a Torrent is not evidence that it belongs to every one
    of them.  The local adapter has already performed that pairing and emits
    both the exact gap ids in ``file_coverage`` and the selected manifest
    indices in ``file_index_by_gap``.  Keep the selector bound to those
    coordinates so it cannot later ask the materializer for an absent index.
    """
    subtitle_ids = {
        gap_id
        for gap_id, gap in gap_lookup.items()
        if gap.get("kind") == "missing_subtitle"
    }
    if not subtitle_ids:
        return set()

    raw_file_coverage = candidate.get("file_coverage")
    explicit_file_coverage = {
        str(value)
        for value in raw_file_coverage
        if isinstance(value, str) and value in subtitle_ids
    } if isinstance(raw_file_coverage, list) else set()

    acquisition = candidate.get("acquisition")
    raw_index_by_gap = (
        acquisition.get("file_index_by_gap")
        if isinstance(acquisition, Mapping) else None
    )
    if isinstance(raw_index_by_gap, Mapping):
        mapped = {
            gap_id
            for gap_id in subtitle_ids
            if isinstance((indices := raw_index_by_gap.get(gap_id)), list)
            and any(type(index) is int and index > 0 for index in indices)
        }
        # A local Torrent candidate supplies both fields.  If it supplies an
        # inconsistent file_coverage list, fail closed rather than let a
        # malformed provider row broaden the requested work.
        return mapped & explicit_file_coverage if isinstance(raw_file_coverage, list) else mapped

    # Non-Torrent providers have no manifest-index map.  They may still make
    # an explicit direct gap claim, but never receive a language-only
    # fallback that expands one sidecar to every subtitle gap.
    return explicit_file_coverage


def _identity_matches(request: Mapping[str, Any], candidate: Mapping[str, Any]) -> bool:
    media = request.get("media") if isinstance(request.get("media"), Mapping) else {}
    requested_id = media.get("tmdb_id")
    candidate_id = candidate.get("tmdb_id")
    if candidate.get("identity_match") is False:
        return False
    if type(requested_id) is int and type(candidate_id) is int:
        # A differing authoritative ID disproves identity.  An equal ID does
        # not prove it: discovery adapters stamp the requested ID on otherwise
        # unverified rows so that it can be carried through the pipeline.
        if requested_id != candidate_id:
            return False
    requested_year = str(media.get("year") or "").strip()
    candidate_year = str(candidate.get("year") or "").strip()
    if re.fullmatch(r"(?:19|20)\d{2}", requested_year) and re.fullmatch(r"(?:19|20)\d{2}", candidate_year):
        if requested_year != candidate_year:
            return False
    aliases = media.get("aliases") if isinstance(media.get("aliases"), list) else [media.get("title")]
    haystack_values = _identity_text_values(candidate)
    haystacks = [_normalized_text(value) for value in haystack_values]
    matching_aliases = [
        (str(alias), alias_key)
        for alias in aliases
        if (alias_key := _normalized_text(alias))
        and any(alias_key in haystack for haystack in haystacks if haystack)
    ]
    if not matching_aliases:
        # This is also the franchise-sibling guard: a provider cannot turn a
        # Railgun release into Index merely by copying Index's requested ID.
        return False
    # Season 00 belongs to a TV work but frequently maps to a standalone
    # special.  A parent record can legitimately carry aliases for later TV
    # seasons, yet an S00 candidate named only by ``II``/``S02``/``Season 2``
    # is not thereby proven to be the current special.  Do not weaken normal
    # positive-season matching, and do not turn a gap's movie aliases into
    # ordinary media identity.  This narrow check applies only when the
    # continuation alias is the *only* work evidence.
    s00_missing_episode = any(
        isinstance(gap, Mapping) and _is_s00_missing_episode(gap)
        for gap in (
            request.get("gaps") if isinstance(request.get("gaps"), list) else []
        )
    )
    if s00_missing_episode:
        media_keys = {
            alias_key for alias in aliases
            if (alias_key := _normalized_text(alias))
        }
        has_unseasoned_match = any(
            not _alias_has_explicit_nonzero_season_marker(alias)
            and _alias_has_unseasoned_candidate_match(
                alias, alias_key, haystack_values,
            )
            for alias, alias_key in matching_aliases
        )
        if not has_unseasoned_match and not _has_strict_s00_gap_evidence(
            request, haystack_values, media_keys=media_keys,
        ):
            return False

    expected_format = str(media.get("media_format") or "").strip()
    if expected_format not in {"animation", "live_action"}:
        expected_format = _requested_media_format(media, media.get("target_root"))
    candidate_format: set[str] = set()
    for field in ("media_format", "format", "content_format", "media_type"):
        candidate_format.update(_media_format_evidence(candidate.get(field)))
    candidate_format.update(_media_format_evidence(" ".join(haystack_values)))
    if expected_format and candidate_format and expected_format not in candidate_format:
        return False
    # Contradictory candidate metadata is unsafe even if one marker happens to
    # agree with the request.
    if len(candidate_format) > 1:
        return False
    return True


def _name_coverage(
    request: Mapping[str, Any], candidate: Mapping[str, Any], gap_lookup: Mapping[str, Mapping[str, Any]],
) -> set[str]:
    name = str(candidate.get("release_name") or "")
    name_seasons = _season_markers(name)
    request_seasons = {
        int(gap["season"])
        for gap in gap_lookup.values()
        if isinstance(gap.get("season"), int) and int(gap["season"]) > 0
    }
    raw_coverage = _expanded_episode_ids(name) | _coverage_tokens(
        candidate.get("name_coverage"), default_seasons=name_seasons,
    ) | _coverage_tokens([name], default_seasons=request_seasons)
    subtitle_gap_ids = {
        gap_id for gap_id, gap in gap_lookup.items()
        if gap.get("kind") == "missing_subtitle"
    }
    # Release-name tokens, season labels, and episode-title hints are valid
    # media discovery evidence, but are never a subtitle pairing claim.
    coverage = set(raw_coverage) - subtitle_gap_ids
    generic_media_gaps = {
        gap_id for gap_id, gap in gap_lookup.items()
        if gap.get("kind") == "missing_media"
    }
    if generic_media_gaps and _candidate_has_video_file(candidate):
        # A movie gap has no SxxEyy token. The selected payload must contain a
        # video and match the requested identity.
        coverage.update(generic_media_gaps)
    exact_subtitle_gaps = _exact_subtitle_file_coverage(candidate, gap_lookup)
    if exact_subtitle_gaps:
        # Unlike a movie, a subtitle sidecar must be paired with the exact
        # paired video before selection. Do not infer that one sidecar
        # covers siblings in the same subtitle-only request.
        coverage.update(exact_subtitle_gaps)
    explicit_whole_seasons = {
        int(token[1:]) for token in raw_coverage if re.fullmatch(r"S\d{2,3}", token)
    }
    episode_seasons = {
        int(match.group(1))
        for token in raw_coverage
        if (match := re.fullmatch(r"S(\d{2,3})E\d{2,4}", token))
    }
    whole_season_claims = explicit_whole_seasons | (name_seasons - episode_seasons)
    claimed_seasons = name_seasons | explicit_whole_seasons | episode_seasons
    for season in whole_season_claims:
        coverage.update(
            gap_id for gap_id, gap in gap_lookup.items()
            if gap.get("kind") != "missing_subtitle" and gap.get("season") == season
        )
    normalized_name = _normalized_text(name)
    identity_haystacks = [
        key for value in _identity_text_values(candidate)
        if (key := _normalized_text(value))
    ]
    file_haystacks: list[str] = []
    raw_files = candidate.get("files")
    if isinstance(raw_files, list):
        for item in raw_files:
            value = (
                item if isinstance(item, str)
                else item.get("path") or item.get("name")
                if isinstance(item, Mapping) else None
            )
            if isinstance(value, str) and (key := _normalized_text(value)):
                file_haystacks.append(key)
    semantic_file_supported: set[str] = set()
    for gap_id, gap in gap_lookup.items():
        if gap.get("kind") == "missing_subtitle":
            continue
        episode_titles = {
            normalized
            for value in [
                gap.get("title"),
                *(
                    gap.get("title_aliases")
                    if isinstance(gap.get("title_aliases"), list) else []
                ),
            ]
            if len(normalized := _normalized_text(value)) >= 3
        }
        if any(
            episode_title in value
            for episode_title in episode_titles
            for value in identity_haystacks
        ):
            coverage.add(gap_id)
        if any(
            episode_title in value
            for episode_title in episode_titles
            for value in file_haystacks
        ):
            semantic_file_supported.add(gap_id)
    for group in request.get("query_groups") if isinstance(request.get("query_groups"), list) else []:
        if not isinstance(group, Mapping) or not isinstance(group.get("season"), int):
            continue
        semantic_names = group.get("season_names") if isinstance(group.get("season_names"), list) else []
        if any(
            (semantic_key := _normalized_text(season_name))
            and len(semantic_key) >= 2
            and semantic_key in normalized_name
            for season_name in semantic_names
        ):
            claimed_seasons.add(int(group["season"]))
            coverage.update(
                gap_id for gap_id, gap in gap_lookup.items()
                if gap.get("kind") != "missing_subtitle"
                and gap.get("season") == group["season"]
            )
    for gap_id, gap in gap_lookup.items():
        expected = gap.get("expected_episode_count")
        season = gap.get("season")
        if (
            gap.get("kind") == "missing_season"
            and isinstance(season, int)
            and isinstance(expected, int)
            and expected > 0
            and {
                f"S{season:02d}E{episode:02d}"
                for episode in range(1, expected + 1)
            }.issubset(raw_coverage)
        ):
            coverage.add(gap_id)
    coverage &= set(gap_lookup)
    file_listing_supplied, file_coverage = _candidate_file_coverage(candidate, claimed_seasons)
    if file_listing_supplied:
        file_supported = (file_coverage & set(gap_lookup)) - subtitle_gap_ids
        file_supported.update(semantic_file_supported)
        if generic_media_gaps and _candidate_has_video_file(candidate):
            file_supported.update(generic_media_gaps)
        file_supported.update(exact_subtitle_gaps)
        for token in file_coverage:
            if not re.fullmatch(r"S\d{2,3}", token):
                continue
            season = int(token[1:])
            file_supported.update(
                gap_id for gap_id, gap in gap_lookup.items()
                if gap.get("kind") != "missing_subtitle" and gap.get("season") == season
            )
        for gap_id, gap in gap_lookup.items():
            expected = gap.get("expected_episode_count")
            season = gap.get("season")
            if (
                gap.get("kind") == "missing_season"
                and isinstance(season, int)
                and isinstance(expected, int)
                and expected > 0
                and {
                    f"S{season:02d}E{episode:02d}"
                    for episode in range(1, expected + 1)
                }.issubset(file_coverage)
            ):
                file_supported.add(gap_id)
        optional_discovery = (
            isinstance(request.get("rules"), Mapping)
            and request["rules"].get("optional_discovery_only") is True
        )
        # A collection release often has no Season 00 token in its release
        # title.  In the explicitly marked optional lane, the adapter's exact
        # manifest mapping is the whole coverage claim.  Do not union it with
        # a semantic release-name claim such as "特别篇": that previously
        # selected an S00 gap with no file_id and failed before fast-save.
        if optional_discovery:
            coverage = file_supported
        else:
            coverage &= file_supported
    return coverage


def _candidate_available(candidate: Mapping[str, Any]) -> bool:
    if candidate.get("available") is False:
        return False
    status = str(candidate.get("availability") or candidate.get("status") or "").casefold()
    return not any(marker in status for marker in BAD_AVAILABILITY_MARKERS)


def _infohash_aliases(value: Any) -> set[str]:
    """Normalize a v1 torrent hash to equivalent hex and Base32 forms."""
    raw = str(value or "").strip().casefold()
    if not raw:
        return set()
    aliases = {raw}
    if re.fullmatch(r"[0-9a-f]{40}", raw):
        aliases.add(base64.b32encode(bytes.fromhex(raw)).decode("ascii").rstrip("=").casefold())
    elif re.fullmatch(r"[a-z2-7]{32}", raw):
        try:
            aliases.add(base64.b32decode(raw.upper()).hex())
        except ValueError:
            pass
    return aliases


def _candidate_infohash_aliases(candidate: Mapping[str, Any]) -> set[str]:
    aliases = _infohash_aliases(candidate.get("infohash"))
    locator = str(candidate.get("locator") or "")
    match = re.search(r"(?i)\bbtih:([0-9a-f]{40}|[a-z2-7]{32})\b", locator)
    if match:
        aliases.update(_infohash_aliases(match.group(1)))
    return aliases


def _selection_tier(
    request: Mapping[str, Any],
    current_tier: str | None,
) -> str | None:
    """Return the explicit lane the runtime is allowed to select.

    The historical selector was intentionally useful as a standalone ranking
    helper and therefore chose the lowest provider present in a result.  The
    automatic coordinator must not use that behaviour: its durable tier state
    is the authority.  Keep the optional argument for existing read-only
    callers, but make a supplied request/argument tier a strict filter.
    """
    raw = current_tier if current_tier is not None else request.get("tier")
    if raw is None:
        return None
    if not isinstance(raw, str):
        raise ValueError("补源当前 tier 无效")
    tier = raw.strip().casefold()
    if tier not in STRICT_TIER_ORDER or tier not in PROVIDER_ORDER:
        raise ValueError("补源当前 tier 无效")
    return tier


def select_replenishment_candidates(
    request: Mapping[str, Any],
    candidates: Sequence[Mapping[str, Any]],
    *,
    current_tier: str | None = None,
) -> dict[str, Any]:
    """Select a provider-neutral, per-gap bundle.

    Unsupported provider artifacts are untrusted input and are rejected before
    any selection can be persisted or resumed.  A runnable row must be one of
    the fixed acquisition pairs rather than a historical cloud-share or HTTP
    claim.  When ``current_tier`` (or ``request.tier``) is supplied, candidates
    from every other lane remain diagnostic evidence only and cannot enter the
    selected bundle.
    """
    selection_tier = _selection_tier(request, current_tier)
    gap_ids, gap_lookup = _request_gap_ids(request)
    if not gap_ids:
        return {
            "status": "empty", "selections": [],
            "covered_gap_ids": [], "uncovered_gap_ids": [],
            **({"tier": selection_tier} if selection_tier is not None else {}),
        }

    raw_excluded = request.get("excluded_candidates")
    excluded = raw_excluded if isinstance(raw_excluded, list) else []
    excluded_locators = {
        (str(row.get("provider") or "*"), str(row.get("locator") or "").strip())
        for row in excluded
        if isinstance(row, Mapping) and str(row.get("locator") or "").strip()
    }
    excluded_hashes: dict[str, set[str]] = defaultdict(set)
    for row in excluded:
        if isinstance(row, Mapping):
            excluded_hashes[str(row.get("provider") or "*")].update(
                _candidate_infohash_aliases(row)
            )

    rejections: Counter[str] = Counter()
    rejections_by_provider: dict[str, Counter[str]] = defaultdict(Counter)
    candidate_counts: Counter[str] = Counter(
        str(row.get("provider") or "unknown")
        for row in candidates if isinstance(row, Mapping)
    )
    valid_by_identity: dict[tuple[str, str], dict[str, Any]] = {}

    def reject(provider: Any, reason: str) -> None:
        name = str(provider or "unknown")
        rejections[reason] += 1
        rejections_by_provider[name][reason] += 1

    for raw in candidates:
        if not isinstance(raw, Mapping):
            reject("unknown", "candidate_not_object")
            continue
        candidate = dict(raw)
        provider = str(candidate.get("provider") or "").strip().casefold()
        if provider not in PROVIDER_ORDER:
            reject(provider, "unsupported_provider")
            continue
        release_name = candidate.get("release_name")
        locator = candidate.get("locator")
        if not isinstance(release_name, str) or not release_name.strip():
            reject(provider, "missing_release_name")
            continue
        if not isinstance(locator, str) or not locator.strip():
            reject(provider, "missing_locator")
            continue
        # The selector is a durable boundary: every selected candidate must
        # already carry the exact materializer contract.  Accepting a bare
        # magnet locator here would only defer a guaranteed materializer
        # failure until after a task state record had been created.
        try:
            acquisition_lane(candidate)
        except AcquisitionRouteError:
            reject(provider, "provider_acquisition_mismatch")
            continue
        locator_key = locator.strip()
        infohashes = _candidate_infohash_aliases(candidate)
        if (
            (provider, locator_key) in excluded_locators
            or ("*", locator_key) in excluded_locators
            or bool(infohashes & (
                excluded_hashes.get(provider, set())
                | excluded_hashes.get("*", set())
            ))
        ):
            reject(provider, "excluded_candidate")
            continue
        if not _candidate_available(candidate):
            reject(provider, "known_unavailable")
            continue
        if not _identity_matches(request, candidate):
            reject(provider, "title_identity_mismatch")
            continue
        coverage = _name_coverage(request, candidate, gap_lookup)
        if not coverage:
            reject(provider, "name_or_file_coverage_miss")
            continue
        candidate["provider"] = provider
        candidate["release_name"] = release_name.strip()
        candidate["locator"] = locator_key
        candidate["resolution"] = _normalized_quality(
            candidate.get("resolution") or release_name
        )
        candidate["coverage"] = sorted(coverage)
        identity = (provider, locator_key)
        current = valid_by_identity.get(identity)
        if current is None:
            valid_by_identity[identity] = candidate
            continue
        reject(provider, "duplicate_locator")
        combined = sorted(set(current["coverage"]) | set(candidate["coverage"]))
        preferred = max((current, candidate), key=lambda item: (
            _swarm_preference(item),
            len(item.get("coverage") or []),
            1 if item.get("availability") == "verified" else 0,
            _timestamp(item.get("updated_at")),
            QUALITY_ORDER[str(item.get("resolution") or "unknown")],
        ))
        preferred["coverage"] = combined
        valid_by_identity[identity] = preferred

    valid = list(valid_by_identity.values())
    selectable = [
        row for row in valid
        if selection_tier is None or str(row["provider"]) == selection_tier
    ]
    eligible_counts = Counter(str(row["provider"]) for row in valid)
    selected_by_identity: dict[tuple[str, str], dict[str, Any]] = {}
    for gap_id in sorted(gap_ids):
        covering = [row for row in selectable if gap_id in row["coverage"]]
        if not covering:
            continue
        # A tier-constrained selection is intentionally one-provider only.
        # Preserve the historical provider ranking for read-only callers that
        # have not supplied durable tier state.
        if selection_tier is None:
            provider_rank = min(PROVIDER_ORDER[str(row["provider"])] for row in covering)
            pool = [
                row for row in covering
                if PROVIDER_ORDER[str(row["provider"])] == provider_rank
            ]
        else:
            pool = covering
        high = [
            row for row in pool
            if QUALITY_ORDER[str(row["resolution"])] >= QUALITY_ORDER["1080p"]
        ]
        known_720 = [row for row in pool if row["resolution"] == "720p"]
        winner = max(high or known_720 or pool, key=lambda row: (
            _swarm_preference(row),
            _timestamp(row.get("updated_at")),
            QUALITY_ORDER[str(row["resolution"])],
            len(row["coverage"]),
            1 if row.get("availability") == "verified" else 0,
            str(row.get("release_name") or "").casefold(),
        ))
        identity = (str(winner["provider"]), str(winner["locator"]))
        selected = selected_by_identity.setdefault(
            identity, dict(winner, selected_gap_ids=[]),
        )
        selected["selected_gap_ids"].append(gap_id)

    selections = sorted(selected_by_identity.values(), key=lambda row: (
        PROVIDER_ORDER[str(row["provider"])],
        -len(row["selected_gap_ids"]),
        tuple(-value for value in _swarm_preference(row)),
        -_timestamp(row.get("updated_at")),
        -QUALITY_ORDER[str(row["resolution"])],
    ))

    def acquisition_kind(row: Mapping[str, Any]) -> str:
        acquisition = row.get("acquisition")
        return (
            str(acquisition.get("kind") or "unknown")
            if isinstance(acquisition, Mapping)
            else "unknown"
        )

    chain_candidates = selectable if selection_tier is not None else valid
    provider_chain_by_gap = {
        gap_id: [
            {
                "provider": row["provider"],
                "locator": row["locator"],
                "acquisition_kind": acquisition_kind(row),
            }
            for row in sorted(
                (item for item in chain_candidates if gap_id in item["coverage"]),
                key=lambda item: (
                    PROVIDER_ORDER[str(item["provider"])],
                    tuple(-value for value in _swarm_preference(item)),
                    -_timestamp(item.get("updated_at")),
                    -QUALITY_ORDER[str(item.get("resolution") or "unknown")],
                    str(item.get("locator") or ""),
                ),
            )
        ]
        for gap_id in sorted(gap_ids)
    }
    covered = {
        gap_id for selection in selections
        for gap_id in selection["selected_gap_ids"]
    }
    uncovered = gap_ids - covered
    selected_identities = set(selected_by_identity)
    unchecked_current_tier = (
        sum(
            1 for row in selectable
            if (str(row["provider"]), str(row["locator"])) not in selected_identities
        )
        if selection_tier is not None else 0
    )
    current_tier_candidate_count = (
        sum(
            1 for row in candidates
            if isinstance(row, Mapping)
            and str(row.get("provider") or "").strip().casefold() == selection_tier
        )
        if selection_tier is not None else 0
    )
    return {
        "status": "complete" if not uncovered else ("partial" if covered else "no_match"),
        "selections": selections,
        "covered_gap_ids": sorted(covered),
        "uncovered_gap_ids": sorted(uncovered),
        "candidate_count": len(candidates),
        "eligible_candidate_count": len(valid),
        "rejection_reasons": dict(sorted(rejections.items())),
        "provider_chain_by_gap": provider_chain_by_gap,
        "provider_capabilities": provider_capability_snapshot(),
        "provider_diagnostics": {
            provider: {
                "candidate_count": candidate_counts.get(provider, 0),
                "eligible_candidate_count": eligible_counts.get(provider, 0),
                "rejection_reasons": dict(sorted(
                    rejections_by_provider.get(provider, Counter()).items()
                )),
            }
            for provider in PROVIDER_DIAGNOSTIC_NAMES
        },
        **({
            "tier": selection_tier,
            "current_tier_candidate_count": current_tier_candidate_count,
            "eligible_current_tier_candidate_count": len(selectable),
            "unchecked_current_tier_candidate_count": unchecked_current_tier,
        } if selection_tier is not None else {}),
    }
