"""Provider-neutral post-scrape search, evidence normalization and ranking."""

from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime, timezone
import base64
import re
from pathlib import PurePosixPath
from typing import Any, Mapping, Sequence
import unicodedata
import copy

from engine.scrapeflow.replenishment_acquisition import (
    AcquisitionRouteError, acquisition_lane,
)


ACTIONABLE_GAP_KINDS = frozenset({"missing_episode", "missing_season"})
PROVIDER_ORDER = {
    "quark_share": 0, "quark_magnet": 1, "cloud_share": 2, "magnet": 3,
}
QUALITY_ORDER = {"2160p": 3, "1080p": 2, "720p": 1, "unknown": 0}
EPISODE_RE = re.compile(
    r"(?<![A-Z0-9])S0*(\d{1,3})[\s._-]*E0*(\d{1,4})(?!\d)"
    r"(?:\s*[-~–—]\s*E?0*(\d{1,4})(?!\d))?",
    re.I,
)
X_EPISODE_RE = re.compile(
    r"(?<!\d)0*(\d{1,3})\s*[x×]\s*0*(\d{1,4})(?!\d)"
    r"(?:\s*[-~–—]\s*0*(\d{1,4})(?!\d))?",
    re.I,
)
SEASON_DASH_EPISODE_RE = re.compile(
    r"(?<![A-Z0-9])(?:S|Season\s+)0*(\d{1,3})\s*[-–—]\s*"
    r"(?:E(?:P)?\s*)?0*(\d{1,3})"
    r"(?:\s*[-~–—]\s*(?:E(?:P)?\s*)?0*(\d{1,3}))?(?!\d|P\b)",
    re.I,
)
CHINESE_EPISODE_RE = re.compile(
    r"第\s*([\d一二三四五六七八九十百零〇两]{1,5})\s*季"
    r"[^\n]{0,30}?第?\s*([\d一二三四五六七八九十百零〇两]{1,5})\s*[集话]"
    r"(?:\s*[-~–—至到]\s*第?\s*([\d一二三四五六七八九十百零〇两]{1,5})\s*[集话])?",
)
EPISODE_ONLY_RE = re.compile(
    r"(?<![A-Z0-9])E(?:P)?0*(\d{1,4})(?!\d)"
    r"(?:\s*[-~–—]\s*E?(?:P)?0*(\d{1,4})(?!\d))?",
    re.I,
)
CHINESE_EPISODE_ONLY_RE = re.compile(
    r"第\s*([\d一二三四五六七八九十百零〇两]{1,5})\s*[集话]"
    r"(?:\s*[-~–—至到]\s*第?\s*([\d一二三四五六七八九十百零〇两]{1,5})\s*[集话])?",
)
ANIME_EPISODE_RANGE_RE = re.compile(
    r"(?<!\d)\(\s*0*(\d{1,3})\s*[-~–—]\s*0*(\d{1,3})\s*\)(?!\d)",
    re.I,
)
ANIME_BRACKET_RANGE_RE = re.compile(
    r"\[\s*0*(\d{1,3})\s*[-~–—]\s*0*(\d{1,3})\s*(?:全集|全|Fin)?\s*\]",
    re.I,
)
ANIME_BRACKET_EPISODE_RE = re.compile(r"\[\s*0*(\d{1,3})\s*\]", re.I)
ANIME_DASH_EPISODE_RE = re.compile(
    r"\s[-–—]\s*0*(\d{1,3})(?=\s|\(|\[|$)",
    re.I,
)
SEASON_RE = re.compile(
    r"(?<![A-Z0-9])S0*(\d{1,3})(?!\d)(?!\s*E\d)"
    r"|\bSeason\s+0*(\d{1,3})\b",
    re.I,
)
SEASON_RANGE_RE = re.compile(
    r"(?<![A-Z0-9])S0*(\d{1,3})\s*[-~–—]\s*"
    r"S?0*(\d{1,3})(?!\d)(?!\s*E\d)",
    re.I,
)
CHINESE_SEASON_RE = re.compile(r"第\s*([一二三四五六七八九十百零〇两\d]{1,5})\s*季")
BAD_AVAILABILITY_MARKERS = (
    "not available", "dead", "offline", "expired", "invalid", "unavailable",
    "blocked", "banned", "deleted", "失效", "过期", "封禁", "删除", "不可用",
)
CHINESE_DIGITS = {"零": 0, "〇": 0, "一": 1, "二": 2, "两": 2, "三": 3,
                  "四": 4, "五": 5, "六": 6, "七": 7, "八": 8, "九": 9}
CHINESE_NUMERALS = ("零", "一", "二", "三", "四", "五", "六", "七", "八", "九", "十")


def _gap_identity(gap: Mapping[str, Any]) -> dict[str, Any] | None:
    kind = str(gap.get("kind") or "")
    if kind not in ACTIONABLE_GAP_KINDS:
        return None
    label = str(gap.get("label") or "").strip()
    if not label:
        return None
    episode_match = EPISODE_RE.search(label)
    if episode_match:
        season = int(episode_match.group(1))
        start = int(episode_match.group(2))
        end = int(episode_match.group(3) or start)
        if season < 0 or start <= 0 or end < start:
            return None
        episodes = list(range(start, end + 1))
        episode_title = str(gap.get("title") or "").strip()
        if not episode_title:
            episode_title = label[episode_match.end():].strip(" -–—:：")
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
            "title": episode_title,
            **({"title_aliases": title_aliases} if title_aliases else {}),
            **({"source_episode_aliases": source_episode_aliases}
               if source_episode_aliases else {}),
        }
    season_match = SEASON_RE.search(label)
    if kind == "missing_season" and season_match:
        season = int(season_match.group(1) or season_match.group(2))
        if season < 0:
            return None
        return {
            "id": f"S{season:02d}", "kind": "missing_season", "season": season,
            "episodes": [], "label": label, "reason": str(gap.get("reason") or ""),
            "season_name": str(gap.get("season_name") or label[season_match.end():]).strip(),
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


def _chinese_number(number: int) -> str | None:
    if 0 <= number <= 10:
        return CHINESE_NUMERALS[number]
    if 10 < number < 20:
        return "十" + CHINESE_NUMERALS[number - 10]
    if 20 <= number < 100:
        tens, ones = divmod(number, 10)
        return CHINESE_NUMERALS[tens] + "十" + (CHINESE_NUMERALS[ones] if ones else "")
    return None


def _parse_chinese_number(value: str) -> int | None:
    text = value.strip()
    if text.isdecimal():
        return int(text)
    if text in CHINESE_DIGITS:
        return CHINESE_DIGITS[text]
    if "百" in text:
        left, right = text.split("百", 1)
        hundreds = CHINESE_DIGITS.get(left, 1 if not left else -1)
        remainder = _parse_chinese_number(right) if right else 0
        return None if hundreds < 0 or remainder is None else hundreds * 100 + remainder
    if "十" in text:
        left, right = text.split("十", 1)
        tens = CHINESE_DIGITS.get(left, 1 if not left else -1)
        ones = CHINESE_DIGITS.get(right, 0 if not right else -1)
        return None if tens < 0 or ones < 0 else tens * 10 + ones
    return None


def _episode_ranges(value: Any) -> list[tuple[int, int, int]]:
    text = str(value or "")
    output: list[tuple[int, int, int]] = []
    for pattern in (EPISODE_RE, X_EPISODE_RE, SEASON_DASH_EPISODE_RE):
        for match in pattern.finditer(text):
            season = int(match.group(1)); start = int(match.group(2)); end = int(match.group(3) or start)
            output.append((season, start, end))
    for match in CHINESE_EPISODE_RE.finditer(text):
        values = [_parse_chinese_number(item) if item else None for item in match.groups()]
        season, start, end = values[0], values[1], values[2] or values[1]
        if isinstance(season, int) and isinstance(start, int) and isinstance(end, int):
            output.append((season, start, end))
    return output


def _expanded_episode_ids(value: Any) -> set[str]:
    output: set[str] = set()
    for season, start, end in _episode_ranges(value):
        if season < 0 or start <= 0 or end < start or end - start > 5000:
            continue
        output.update(f"S{season:02d}E{episode:02d}" for episode in range(start, end + 1))
    return output


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
    return {
        "version": 2, "job_id": job_id, "round": round_number,
        "media": {
            "title": str(metadata.get("title") or "").strip(), "aliases": aliases,
            "year": year, "tmdb_id": metadata.get("tmdb_id"),
            "target_root": target_root,
            **({"media_format": media_format} if media_format else {}),
        },
        "gaps": gaps, "query_groups": groups,
        "search_queries": _search_queries(aliases, year, groups),
        "rules": {
            "title_identity_is_hard_gate": True, "name_coverage_is_hard_gate": True,
            "file_listing_restricts_claimed_coverage": True,
            "provider_order": ["quark_share", "quark_magnet", "cloud_share", "magnet"],
            "provider_fallback_is_per_gap": True,
            "quality_ladder": ["2160p", "1080p", "720p"],
            "allow_720p_only_without_1080p_or_better": True,
            "newest_before_quality_within_provider": True,
            "allow_multiple_candidates_to_cover_all_gaps": True,
            # Season 00 is a required replenishment lane.  This marker changes
            # only the evidence mapping for OVA/OAD numbering; it is never a
            # waiver and never permits the coordinator to stop retrying.
            **({
                "optional_discovery_only": True,
                "season_zero_replenishment_required": True,
            } if optional_only else {}),
        },
    }


def build_replenishment_requests(
    plan: Mapping[str, Any], *, job_id: str, round_number: int,
) -> dict[str, Any]:
    """Split combined/batch plan gaps into identity-safe project requests."""
    metadata = plan.get("metadata") if isinstance(plan.get("metadata"), Mapping) else {}
    raw_gaps = (
        plan.get("scan_report", {}).get("resource_gaps")
        if isinstance(plan.get("scan_report"), Mapping) else []
    )
    gaps = [dict(gap) for gap in raw_gaps or [] if isinstance(gap, Mapping)]
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
        request = build_replenishment_request(plan, job_id=job_id, round_number=round_number)
        return {"version": 1, "requests": [request], "unresolved_gaps": []}

    grouped: dict[int, list[dict[str, Any]]] = defaultdict(list)
    unresolved: list[dict[str, Any]] = []
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
        if identity_id is None:
            unresolved.append(gap)
        else:
            grouped[identity_id].append(gap)

    requests: list[dict[str, Any]] = []
    for tmdb_id, project_gaps in grouped.items():
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
        request["project_key"] = f"tmdb:tv:{tmdb_id}"
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


def _normalized_quality(value: Any) -> str:
    text = unicodedata.normalize("NFKC", str(value or "unknown")).casefold()
    if any(marker in text for marker in ("2160", "4k", "uhd", "3840x2160")):
        return "2160p"
    if any(marker in text for marker in ("1080", "fhd", "1920x1080")):
        return "1080p"
    if any(marker in text for marker in ("720", "1280x720")):
        return "720p"
    return "unknown"


def _season_markers(value: str) -> set[int]:
    output = {int(match.group(1) or match.group(2)) for match in SEASON_RE.finditer(value)}
    for match in SEASON_RANGE_RE.finditer(value):
        between = value[match.end(1):match.start(2)]
        if (
            re.search(r"\s[-~–—]\s", between)
            and not re.search(r"[-~–—]\s*S", between, re.I)
        ):
            # Current anime release names commonly use ``S4 - 16`` for
            # season 4 episode 16. The episode parser already recognizes that
            # explicit shape; do not reinterpret it as season range 4–16.
            continue
        start, end = int(match.group(1)), int(match.group(2))
        if 0 < start <= end <= 999 and end - start <= 100:
            output.update(range(start, end + 1))
    for match in CHINESE_SEASON_RE.finditer(value):
        number = _parse_chinese_number(match.group(1))
        if isinstance(number, int) and number > 0:
            output.add(number)
    return output


def _coverage_tokens(value: Any, *, default_seasons: set[int] | None = None) -> set[str]:
    if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
        return set()
    output: set[str] = set()
    for item in value:
        output.update(_expanded_episode_ids(item))
        season_match = re.fullmatch(r"\s*(?:S0*(\d{1,3})|Season\s+0*(\d{1,3})|第\s*(\d{1,3})\s*季)\s*", item, re.I)
        if season_match:
            output.add(f"S{int(next(group for group in season_match.groups() if group)):02d}")
        intrinsic_seasons = _season_markers(item)
        effective_seasons = intrinsic_seasons or (default_seasons or set())
        if len(effective_seasons) == 1:
            # A per-file path such as ``Season 2/... - 01.mkv`` carries
            # stronger evidence than the request's default season.  Applying
            # the S01 request default to that naked ordinal used to make a
            # multi-season pack falsely cover S01 with its S02 files.
            season = next(iter(effective_seasons))
            for match in EPISODE_ONLY_RE.finditer(item):
                start, end = int(match.group(1)), int(match.group(2) or match.group(1))
                if 0 < start <= end and end - start <= 5000:
                    output.update(f"S{season:02d}E{episode:02d}" for episode in range(start, end + 1))
            for match in CHINESE_EPISODE_ONLY_RE.finditer(item):
                start = _parse_chinese_number(match.group(1))
                end = _parse_chinese_number(match.group(2)) if match.group(2) else start
                if isinstance(start, int) and isinstance(end, int) and 0 < start <= end and end - start <= 5000:
                    output.update(f"S{season:02d}E{episode:02d}" for episode in range(start, end + 1))
            for pattern in (ANIME_EPISODE_RANGE_RE, ANIME_BRACKET_RANGE_RE):
                for match in pattern.finditer(item):
                    start, end = int(match.group(1)), int(match.group(2))
                    if 0 < start <= end <= 999 and end - start <= 500:
                        output.update(f"S{season:02d}E{episode:02d}" for episode in range(start, end + 1))
            for match in ANIME_BRACKET_EPISODE_RE.finditer(item):
                episode = int(match.group(1))
                if 0 < episode <= 999:
                    output.add(f"S{season:02d}E{episode:02d}")
            for match in ANIME_DASH_EPISODE_RE.finditer(item):
                episode = int(match.group(1))
                if 0 < episode <= 999:
                    output.add(f"S{season:02d}E{episode:02d}")
    return output


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
    title_matches = any(
        alias_key and any(alias_key in haystack for haystack in haystacks if haystack)
        for alias in aliases if (alias_key := _normalized_text(alias))
    )
    if not title_matches:
        # This is also the franchise-sibling guard: a provider cannot turn a
        # Railgun release into Index merely by copying Index's requested ID.
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
    coverage = set(raw_coverage)
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
            if gap.get("season") == season
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
                if gap.get("season") == group["season"]
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
        file_supported = file_coverage & set(gap_lookup)
        file_supported.update(semantic_file_supported)
        for token in file_coverage:
            if not re.fullmatch(r"S\d{2,3}", token):
                continue
            season = int(token[1:])
            file_supported.update(
                gap_id for gap_id, gap in gap_lookup.items()
                if gap.get("season") == season
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


_PERMANENT_EXHAUSTION_KINDS = frozenset({
    "search_complete_no_candidates", "resource_failure_floor_reached",
})


def _verified_exhaustion_proof(
    provider_exhausted: Mapping[str, Any], provider: str, *,
    required_resource_floor: int = 0,
) -> Mapping[str, Any] | None:
    """Accept complete durable proofs, never labels or naked booleans."""
    entry = provider_exhausted.get(provider)
    if not isinstance(entry, Mapping) or entry.get("exhausted") is not True:
        return None
    proof = entry.get("proof")
    if not isinstance(proof, Mapping):
        return None
    kind = str(proof.get("kind") or "")
    if kind not in _PERMANENT_EXHAUSTION_KINDS:
        return None
    if kind == "search_complete_no_candidates":
        required = proof.get("required_sources")
        completed = proof.get("completed_sources")
        if (
            not isinstance(required, list)
            or not required
            or not all(isinstance(item, str) and item for item in required)
            or not isinstance(completed, list)
            or not all(isinstance(item, str) and item for item in completed)
            or set(required) - set(completed)
            or type(proof.get("candidate_count")) is not int
            or proof["candidate_count"] != 0
            or type(proof.get("excluded_candidate_count")) is not int
            or proof["excluded_candidate_count"] < 0
        ):
            return None
    else:
        required_floor = proof.get("required_floor")
        distinct_failures = proof.get("distinct_failure_count")
        if (
            type(required_floor) is not int
            or required_floor <= 0
            or required_floor < required_resource_floor
            or type(distinct_failures) is not int
            or distinct_failures < required_floor
        ):
            return None
    return proof


def _local_torrent_gate(
    request: Mapping[str, Any],
) -> tuple[bool, int, int, Mapping[str, Any] | None]:
    """Resolve the fail-closed tier-3 gate and expose its audit inputs.

    Resource-failure counts alone may advance between cloud candidates, but
    must never authorize local Torrent while a required cloud search source
    still has uninspected candidates.  A complete required-source exhaustion
    proof can cross the cloud/local boundary without an artificial failure
    count: a genuinely empty cloud search space has no resources to fail.
    """
    rules = request.get("rules") if isinstance(request.get("rules"), Mapping) else {}
    attempts = (
        request.get("provider_attempts")
        if isinstance(request.get("provider_attempts"), Mapping) else {}
    )
    exhausted = (
        request.get("provider_exhausted")
        if isinstance(request.get("provider_exhausted"), Mapping) else {}
    )
    try:
        floor = max(0, min(int(rules.get("minimum_attempts_per_cloud_lane") or 0), 1000))
        offline_attempts = max(0, int(attempts.get("quark_magnet") or 0))
    except (TypeError, ValueError):
        return False, 0, 0, None
    proof = _verified_exhaustion_proof(
        exhausted, "quark_magnet", required_resource_floor=floor,
    )
    unlocked = bool(
        isinstance(proof, Mapping)
        and proof.get("kind") == "search_complete_no_candidates"
    )
    return unlocked, floor, offline_attempts, proof


def _has_exact_quark_share_manifest(candidate: Mapping[str, Any]) -> bool:
    """Require the file-id/path/size evidence needed by fast-save."""
    acquisition = candidate.get("acquisition")
    if not isinstance(acquisition, Mapping):
        return False
    gap_map = acquisition.get("file_id_by_gap")
    path_map = acquisition.get("file_path_by_id")
    size_map = acquisition.get("file_size_by_id")
    if not all(isinstance(value, Mapping) and value for value in (
        gap_map, path_map, size_map,
    )):
        return False
    for gap, raw_ids in gap_map.items():
        file_ids = [raw_ids] if isinstance(raw_ids, str) else raw_ids
        if (
            not isinstance(gap, str) or not gap
            or not isinstance(file_ids, list) or not file_ids
            or not all(isinstance(file_id, str) and file_id for file_id in file_ids)
        ):
            return False
        for file_id in file_ids:
            path = path_map.get(file_id)
            size = size_map.get(file_id)
            if not isinstance(path, str) or not path.strip() or type(size) is not int or size <= 0:
                return False
    return True


def select_replenishment_candidates(
    request: Mapping[str, Any], candidates: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    """Select a per-gap provider-first bundle instead of requiring one mega-release."""
    gap_ids, gap_lookup = _request_gap_ids(request)
    if not gap_ids:
        return {"status": "empty", "selections": [], "covered_gap_ids": [], "uncovered_gap_ids": []}

    excluded_rows = request.get("excluded_candidates")
    excluded = excluded_rows if isinstance(excluded_rows, list) else []
    local_torrent_unlocked, _, _, local_torrent_proof = _local_torrent_gate(request)
    infrastructure_suppressed_cloud_locators = {
        str(item.get("locator") or "").strip()
        for item in excluded
        if isinstance(item, Mapping)
        and item.get("provider") in {"quark_share", "quark_magnet"}
        and isinstance(item.get("until_epoch"), (int, float))
        and str(item.get("reason") or "").endswith("_infrastructure_failure")
        and item.get("locator")
    }
    infrastructure_suppressed_cloud_hashes: dict[str, set[str]] = defaultdict(set)
    for item in excluded:
        if (
            isinstance(item, Mapping)
            and item.get("provider") in {"quark_share", "quark_magnet"}
            and isinstance(item.get("until_epoch"), (int, float))
            and str(item.get("reason") or "").endswith("_infrastructure_failure")
        ):
            infrastructure_suppressed_cloud_hashes[str(item["provider"])].update(
                _candidate_infohash_aliases(item)
            )
    excluded_locators = {
        str(item.get("locator") or "").strip()
        for item in excluded if isinstance(item, Mapping) and item.get("locator")
    }
    excluded_hashes_by_provider: dict[str, set[str]] = defaultdict(set)
    for item in excluded:
        if not isinstance(item, Mapping):
            continue
        provider = str(item.get("provider") or "*")
        excluded_hashes_by_provider[provider].update(_candidate_infohash_aliases(item))

    valid_by_locator: dict[str, dict[str, Any]] = {}
    infrastructure_blocked_cloud_by_gap: dict[str, set[str]] = defaultdict(set)
    rejections: Counter[str] = Counter()
    candidate_counts: Counter[str] = Counter(
        str(item.get("provider") or "unknown")
        for item in candidates if isinstance(item, Mapping)
    )
    rejections_by_provider: dict[str, Counter[str]] = defaultdict(Counter)
    durable_rejections: list[dict[str, Any]] = []

    def reject(
        provider_name: Any, reason: str, rejected_candidate: Mapping[str, Any] | None = None,
    ) -> None:
        rejections[reason] += 1
        rejections_by_provider[str(provider_name or "unknown")][reason] += 1
        if (
            reason in {"known_unavailable", "title_identity_mismatch", "name_or_file_coverage_miss"}
            and provider_name in {"quark_share", "quark_magnet"}
            and isinstance(rejected_candidate, Mapping)
            and isinstance(rejected_candidate.get("locator"), str)
            and rejected_candidate.get("locator").strip()
        ):
            durable_rejections.append({
                "provider": provider_name,
                "locator": rejected_candidate["locator"].strip(),
                "infohash": rejected_candidate.get("infohash"),
                "release_name": rejected_candidate.get("release_name"),
                "reason": reason,
            })

    for raw in candidates:
        candidate = dict(raw)
        provider = candidate.get("provider")
        release_name = candidate.get("release_name")
        locator = candidate.get("locator")
        if provider not in PROVIDER_ORDER:
            reject(provider, "unsupported_provider"); continue
        if provider == "magnet" and not local_torrent_unlocked:
            # Older search artifacts may contain a cloud/local pair.  Treat
            # the local row as untrusted input until the current request has
            # both the tier-2 floor and permanent exhaustion proof.
            reject(provider, "local_torrent_locked"); continue
        acquisition = candidate.get("acquisition")
        # The two automated cloud lanes are executable contracts, not merely
        # source labels.  In particular, accepting a legacy ``quark_share``
        # row without its exact file-id manifest lets it win provider ranking
        # and then fail before fast-save can ever be called.  Local ``magnet``
        # rows remain backward compatible because old verified catalogs did
        # not persist an acquisition object.
        if provider in {"quark_share", "quark_magnet"} or (
            provider == "magnet" and isinstance(acquisition, Mapping)
        ):
            try:
                acquisition_lane(candidate)
            except AcquisitionRouteError:
                reject(provider, "provider_acquisition_mismatch"); continue
        if provider == "quark_share" and not _has_exact_quark_share_manifest(candidate):
            reject(provider, "incomplete_quark_share_manifest"); continue
        if not isinstance(release_name, str) or not release_name.strip():
            reject(provider, "missing_release_name"); continue
        if not isinstance(locator, str) or not locator.strip():
            reject(provider, "missing_locator"); continue
        locator_key = locator.strip()
        excluded_hashes = (
            excluded_hashes_by_provider.get(str(provider), set())
            | excluded_hashes_by_provider.get("*", set())
        )
        if not _candidate_available(candidate):
            reject(provider, "known_unavailable", candidate); continue
        if not _identity_matches(request, candidate):
            reject(provider, "title_identity_mismatch", candidate); continue
        coverage = _name_coverage(request, candidate, gap_lookup)
        if not coverage:
            reject(provider, "name_or_file_coverage_miss", candidate); continue
        candidate_hashes = _candidate_infohash_aliases(candidate)
        if locator_key in excluded_locators or bool(
            excluded_hashes and candidate_hashes & excluded_hashes
        ):
            provider_name = str(provider)
            if (
                provider_name in {"quark_share", "quark_magnet"}
                and (
                    locator_key in infrastructure_suppressed_cloud_locators
                    or bool(
                        candidate_hashes
                        & infrastructure_suppressed_cloud_hashes.get(provider_name, set())
                    )
                )
            ):
                for gap_id in coverage:
                    infrastructure_blocked_cloud_by_gap[gap_id].add(provider_name)
                reject(provider, "cloud_infrastructure_suppression")
            else:
                reject(provider, "excluded_candidate")
            continue
        candidate["resolution"] = _normalized_quality(candidate.get("resolution") or release_name)
        candidate["locator"] = locator_key
        candidate["coverage"] = sorted(coverage)
        existing = valid_by_locator.get(locator_key)
        if existing is None:
            valid_by_locator[locator_key] = candidate
            continue
        reject(provider, "duplicate_locator")
        combined_coverage = sorted(set(existing["coverage"]) | set(candidate["coverage"]))
        def evidence_rank(item: Mapping[str, Any]) -> tuple[int, int, float, int]:
            return (
                len(item.get("coverage") or []),
                1 if item.get("availability") == "verified" else 0,
                _timestamp(item.get("updated_at")),
                QUALITY_ORDER[str(item.get("resolution") or "unknown")],
            )
        preferred = candidate if evidence_rank(candidate) > evidence_rank(existing) else existing
        preferred["coverage"] = combined_coverage
        valid_by_locator[locator_key] = preferred

    valid = list(valid_by_locator.values())
    eligible_counts = Counter(str(item.get("provider") or "unknown") for item in valid)

    rules = request.get("rules") if isinstance(request.get("rules"), Mapping) else {}
    raw_attempts = request.get("provider_attempts")
    provider_attempts = raw_attempts if isinstance(raw_attempts, Mapping) else {}
    raw_exhausted = request.get("provider_exhausted")
    provider_exhausted = raw_exhausted if isinstance(raw_exhausted, Mapping) else {}
    try:
        minimum_attempts = int(rules.get("minimum_attempts_per_cloud_lane") or 0)
    except (TypeError, ValueError):
        minimum_attempts = 0
    minimum_attempts = max(0, min(minimum_attempts, 1000))
    share_attempts = int(provider_attempts.get("quark_share") or 0)
    offline_attempts = int(provider_attempts.get("quark_magnet") or 0)
    required_attempt_provider: str | None = None
    selection_pool = valid
    share_exhausted = _verified_exhaustion_proof(
        provider_exhausted, "quark_share",
        required_resource_floor=minimum_attempts,
    ) is not None
    offline_exhausted = _verified_exhaustion_proof(
        provider_exhausted, "quark_magnet",
        required_resource_floor=minimum_attempts,
    ) is not None
    if minimum_attempts and share_attempts < minimum_attempts and not share_exhausted:
        required_attempt_provider = "quark_share"
        selection_pool = [
            item for item in valid if item.get("provider") == "quark_share"
        ]
    elif (
        minimum_attempts or candidate_counts.get("magnet", 0) > 0
    ) and not offline_exhausted:
        required_attempt_provider = "quark_magnet"
        # A newly discovered first-lane candidate always remains preferable;
        # otherwise stay inside the second cloud lane until its own budget is
        # exhausted.  Local Torrent is deliberately absent from this pool.
        selection_pool = [
            item for item in valid
            if item.get("provider") != "magnet"
        ]
    elif not minimum_attempts and any(
        item.get("provider") == "magnet" for item in valid
    ):
        excluded_rows = request.get("excluded_candidates")
        excluded_rows = excluded_rows if isinstance(excluded_rows, list) else []
        first_lane_failed = any(
            isinstance(row, Mapping)
            and (
                row.get("provider") == "quark_share"
                or str(row.get("locator") or "").startswith((
                    "quark_share:", "quark-share:",
                ))
            )
            for row in excluded_rows
        )
        second_lane_failed = any(
            isinstance(row, Mapping)
            and row.get("provider") == "quark_magnet"
            and row.get("failure_scope") != "infrastructure"
            and not isinstance(row.get("until_epoch"), (int, float))
            for row in excluded_rows
        )
        if first_lane_failed and not (
            second_lane_failed or offline_exhausted
        ):
            # Hard fail-closed invariant: an excluded/failed first-lane share
            # can advance only to Quark cloud offline, never directly to a
            # local Torrent, even if the caller forgot the numeric floor.
            required_attempt_provider = "quark_magnet"
            selection_pool = [
                item for item in valid if item.get("provider") == "quark_magnet"
            ]

    selected_by_locator: dict[str, dict[str, Any]] = {}
    infrastructure_blocked_gap_ids: list[str] = []
    for gap_id in sorted(gap_ids):
        covering = [item for item in selection_pool if gap_id in item["coverage"]]
        if not covering:
            continue
        provider_rank = min(PROVIDER_ORDER[str(item["provider"])] for item in covering)
        # A broken helper/network is not evidence that this resource requires
        # local download.  Permit another executable cloud lane, but do not
        # cross the cloud/local boundary until the cloud candidate itself has
        # a durable resource-level failure/exclusion.
        if (
            provider_rank >= PROVIDER_ORDER["magnet"]
            and infrastructure_blocked_cloud_by_gap.get(gap_id)
        ):
            infrastructure_blocked_gap_ids.append(gap_id)
            rejections["cloud_infrastructure_must_retry_before_local"] += 1
            continue
        pool = [item for item in covering if PROVIDER_ORDER[str(item["provider"])] == provider_rank]
        high = [item for item in pool if QUALITY_ORDER[str(item["resolution"])] >= QUALITY_ORDER["1080p"]]
        known_720 = [item for item in pool if item["resolution"] == "720p"]
        pool = high or known_720 or pool
        winner = max(pool, key=lambda item: (
            _timestamp(item.get("updated_at")), QUALITY_ORDER[str(item["resolution"])],
            len(item["coverage"]), 1 if item.get("availability") == "verified" else 0,
            str(item.get("release_name") or "").casefold(),
        ))
        locator = str(winner["locator"])
        selected = selected_by_locator.setdefault(locator, dict(winner, selected_gap_ids=[]))
        selected["selected_gap_ids"].append(gap_id)

    selections = sorted(selected_by_locator.values(), key=lambda item: (
        PROVIDER_ORDER[str(item["provider"])], -len(item["selected_gap_ids"]),
        -_timestamp(item.get("updated_at")), -QUALITY_ORDER[str(item["resolution"])],
    ))
    # Persist the exact per-gap order as audit evidence.  The coordinator
    # excludes or temporarily suppresses only the failed locator and calls the
    # selector again, so the next round advances along this chain instead of
    # jumping directly from a share failure to a large local download.
    def acquisition_kind_label(item: Mapping[str, Any]) -> str:
        acquisition = item.get("acquisition")
        return str(acquisition.get("kind") or "legacy") if isinstance(
            acquisition, Mapping
        ) else "legacy"

    provider_chain_by_gap = {
        gap_id: [
            {
                "provider": item["provider"],
                "locator": item["locator"],
                "acquisition_kind": acquisition_kind_label(item),
            }
            for item in sorted(
                (candidate for candidate in valid if gap_id in candidate["coverage"]),
                key=lambda item: (
                    PROVIDER_ORDER[str(item["provider"])],
                    -_timestamp(item.get("updated_at")),
                    -QUALITY_ORDER[str(item["resolution"])],
                    str(item.get("locator") or ""),
                ),
            )
        ]
        for gap_id in sorted(gap_ids)
    }
    covered = {gap_id for item in selections for gap_id in item["selected_gap_ids"]}
    uncovered = gap_ids - covered
    return {
        "status": "complete" if not uncovered else ("partial" if covered else "no_match"),
        "selections": selections, "covered_gap_ids": sorted(covered),
        "uncovered_gap_ids": sorted(uncovered), "candidate_count": len(candidates),
        "eligible_candidate_count": len(valid), "rejection_reasons": dict(sorted(rejections.items())),
        "durably_rejected_candidates": durable_rejections,
        "provider_chain_by_gap": provider_chain_by_gap,
        "infrastructure_blocked_gap_ids": infrastructure_blocked_gap_ids,
        "required_attempt_provider": (
            required_attempt_provider if not selections else None
        ),
        "required_attempt_blocked_by_infrastructure": bool(
            required_attempt_provider
            and any(
                required_attempt_provider in providers
                for providers in infrastructure_blocked_cloud_by_gap.values()
            )
        ),
        "minimum_attempts_per_cloud_lane": minimum_attempts,
        "provider_attempts": {
            "quark_share": share_attempts,
            "quark_magnet": offline_attempts,
        },
        "provider_exhausted": {
            "quark_share": share_exhausted,
            "quark_magnet": offline_exhausted,
        },
        "local_torrent_unlocked": local_torrent_unlocked,
        "local_torrent_exhaustion_proof": (
            dict(local_torrent_proof) if local_torrent_proof is not None else None
        ),
        "provider_diagnostics": {
            provider: {
                "candidate_count": candidate_counts.get(provider, 0),
                "eligible_candidate_count": eligible_counts.get(provider, 0),
                "rejection_reasons": dict(sorted(
                    rejections_by_provider.get(provider, Counter()).items()
                )),
            }
            for provider in PROVIDER_ORDER
        },
    }


def select_replenishment_candidate(
    request: Mapping[str, Any], candidates: Sequence[Mapping[str, Any]],
) -> dict[str, Any] | None:
    """Backward-compatible single-candidate view for older callers."""
    bundle = select_replenishment_candidates(request, candidates)
    selections = bundle["selections"]
    return selections[0] if bundle["status"] == "complete" and len(selections) == 1 else None


def validate_acquisition_results(value: Mapping[str, Any]) -> list[str]:
    if value.get("status") != "ready":
        raise ValueError("查补适配器未返回 ready 状态")
    sources = value.get("source_paths")
    if sources is None and isinstance(value.get("source_path"), str):
        sources = [value["source_path"]]
    if not isinstance(sources, list) or not sources or not all(
        isinstance(source, str) and source.strip() for source in sources
    ):
        raise ValueError("查补适配器缺少 source_path/source_paths")
    return list(dict.fromkeys(source.strip() for source in sources))


def validate_acquisition_result(value: Mapping[str, Any]) -> str:
    """Backward-compatible first source path."""
    return validate_acquisition_results(value)[0]
