#!/usr/bin/env python3
"""Read-only semantic inventory audit for an AList TV library.

The scanner discovers series from ``tvshow.nfo`` instead of a hard-coded title
list, then compares current files with currently published TMDB episodes.
No AList mutation endpoint is used.
"""

from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from concurrent.futures import Future, ThreadPoolExecutor
from contextlib import contextmanager
from datetime import date, datetime, timezone
import ipaddress
import json
import os
from pathlib import Path, PurePosixPath
import re
import socket
from typing import Any, Callable, Mapping
from urllib.parse import urlsplit
import xml.etree.ElementTree as ET

from engine.scraper import AListClient, TMDBClient, subtitle_language
from engine.scrapeflow.residual_policy import classify_residual


VIDEO_EXTS = {".mkv", ".mp4", ".m4v", ".m2ts", ".ts", ".avi", ".mov", ".webm"}
SUBTITLE_EXTS = {
    ".ass", ".ssa", ".srt", ".vtt", ".idx", ".sub", ".sup", ".mks",
}
DEFAULT_REQUIRED_SUBTITLE_LANGUAGES = ("zh-CN",)
EPISODE_RE = re.compile(r"(?:^|[ ._-])S0*(\d{1,3})E0*(\d{1,4})(?:-E0*(\d{1,4}))?(?:$|[ ._-])", re.I)
SEASON_DIR_RE = re.compile(r"^Season\s+(\d{1,3})$", re.I)
PART_RE = re.compile(r"(?:^|[ ._-])part[ ._-]*0*(\d{1,3})(?:$|[ ._-])", re.I)
MEDIA_CATEGORIES = ("番剧", "美剧", "电影")


@contextmanager
def temporary_tmdb_dns_override(ip_value: str | None):
    """Pin only the official TMDB API hostname for this audit process.

    This exists for networks where the system resolver is polluted but a
    separately verified DNS-over-HTTPS answer is available. TLS still uses and
    verifies ``api.themoviedb.org``; no certificate check is bypassed.
    """
    if not ip_value:
        yield
        return
    address = ipaddress.ip_address(ip_value)
    base_url = os.getenv("TMDB_BASE_URL", "https://api.themoviedb.org/3")
    hostname = urlsplit(base_url).hostname
    if hostname != "api.themoviedb.org":
        raise ValueError("--tmdb-resolve-ip 只允许用于官方 TMDB API 域名")
    original = socket.getaddrinfo

    def resolve(host, port, family=0, type=0, proto=0, flags=0):
        if host == hostname:
            return original(str(address), port, family, type, proto, flags)
        return original(host, port, family, type, proto, flags)

    socket.getaddrinfo = resolve
    try:
        yield
    finally:
        socket.getaddrinfo = original


def formal_media_roots(root: str) -> tuple[str, ...]:
    normalized = root.rstrip("/") or "/"
    if PurePosixPath(normalized).name != "影视":
        return (normalized,)
    return tuple(str(PurePosixPath(normalized) / category) for category in MEDIA_CATEGORIES)


def _inside_roots(path: str, roots: tuple[str, ...]) -> bool:
    return any(path == root or path.startswith(root.rstrip("/") + "/") for root in roots)


def default_excluded_roots(root: str) -> list[str]:
    """Return system/workflow roots excluded from a whole-library audit."""
    normalized_root = root.rstrip("/") or "/"
    if PurePosixPath(normalized_root).name != "影视":
        return []
    return [
        str(PurePosixPath(normalized_root) / "待刮削"),
        str(PurePosixPath(normalized_root) / "已刮削"),
        str(PurePosixPath(normalized_root) / "ScrapeFlow"),
    ]


def _parallel_map_with_serial_retry(
    inspect: Callable[[Any], Any],
    items: list[Any],
    *,
    max_workers: int = 6,
    serial_attempts: int = 2,
) -> list[Any]:
    """Inspect quickly, then retry only failed items without concurrent load.

    A transient TLS/proxy failure in one TMDB request must not immediately
    discard a nearly complete whole-library audit.  At the same time, a title
    that remains unavailable cannot be silently omitted: after bounded serial
    retries its exception is re-raised and the API keeps serving the previous
    known-good snapshot.
    """
    if max_workers <= 0 or serial_attempts <= 0:
        raise ValueError("审计并发数与串行重试次数必须大于 0")
    results: list[Any] = [None] * len(items)
    failed: list[tuple[int, Any, Exception]] = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures: dict[Future[Any], tuple[int, Any]] = {
            executor.submit(inspect, item): (index, item)
            for index, item in enumerate(items)
        }
        for future, (index, item) in futures.items():
            try:
                results[index] = future.result()
            except Exception as exc:  # retry outside the concurrent burst
                failed.append((index, item, exc))

    for index, item, first_error in failed:
        last_error: Exception = first_error
        for _attempt in range(serial_attempts):
            try:
                results[index] = inspect(item)
                break
            except Exception as exc:
                last_error = exc
        else:
            raise last_error from first_error
    return results


def companion_stem(path: str) -> str:
    stem = str(PurePosixPath(path).with_suffix(""))
    return re.sub(
        r"\.(?:(?:zh-CN|zh-TW|zh|en|ja|chs|cht|sc|tc)(?:\.\d+)*|"
        r"subtitle(?:\.\d+|\d*)?)$",
        "",
        stem,
        flags=re.I,
    )


def external_subtitle_gap(
    video_path: str,
    related_subtitles: list[str],
    *,
    required_languages: tuple[str, ...] = DEFAULT_REQUIRED_SUBTITLE_LANGUAGES,
) -> dict[str, Any] | None:
    """Describe a sidecar subtitle gap for one exact video version.

    Episode-only matching is not sufficient when multiple editions of an
    episode coexist: a subtitle for the theatrical cut must not satisfy the
    director's cut.  Exact companion basenames therefore take precedence;
    same-episode subtitles with another basename are retained as mismatch
    evidence for a later replenishment/review step.

    AList inventory cannot inspect embedded streams.  The returned scope and
    remediation action make that limitation explicit instead of claiming the
    media has no subtitles at all.
    """
    normalized_required = tuple(sorted({
        str(language).strip()
        for language in required_languages
        if str(language).strip()
    }))
    video_stem = str(PurePosixPath(video_path).with_suffix(""))
    candidates = sorted(set(related_subtitles))
    companions = [
        path for path in candidates
        if companion_stem(path).casefold() == video_stem.casefold()
    ]
    languages = sorted({
        (
            "und-mks-stream-unverified"
            if PurePosixPath(path).suffix.casefold() == ".mks"
            else subtitle_language(PurePosixPath(path).name) or "und"
        )
        for path in companions
    })
    if companions and (
        not normalized_required
        or any(language in normalized_required for language in languages)
    ):
        return None

    if not companions:
        reason_code = (
            "subtitle_video_stem_mismatch"
            if candidates else "missing_external_subtitle"
        )
        remediation_action = (
            "review_subtitle_pairing"
            if candidates else "acquire_or_verify_embedded_subtitle"
        )
    elif "und" in languages or "und-mks-stream-unverified" in languages:
        reason_code = "subtitle_language_unverified"
        remediation_action = (
            "probe_mks_stream_content"
            if "und-mks-stream-unverified" in languages
            else "verify_or_replace_subtitle_language"
        )
    else:
        reason_code = "required_subtitle_language_missing"
        remediation_action = "acquire_required_language_subtitle"
    return {
        "scope": "external_sidecar",
        "reason_code": reason_code,
        "video_path": video_path,
        "required_languages": list(normalized_required),
        "companion_subtitles": companions,
        "candidate_subtitles": candidates,
        "candidate_languages": languages,
        "remediation_action": remediation_action,
        "embedded_subtitle_status": "not_inspectable_from_alist_inventory",
    }


def parse_tvshow_nfo(payload: bytes) -> dict[str, Any]:
    root = ET.fromstring(payload)
    title = (root.findtext("title") or "").strip()
    ids: set[int] = set()
    for node in root.findall("uniqueid"):
        if str(node.attrib.get("type") or "").casefold() != "tmdb":
            continue
        value = (node.text or "").strip()
        if value.isdigit() and int(value) > 0:
            ids.add(int(value))
    for tag in ("tmdbid", "tmdb_id"):
        value = (root.findtext(tag) or "").strip()
        if value.isdigit() and int(value) > 0:
            ids.add(int(value))
    return {"title": title, "tmdb_ids": sorted(ids)}


def parse_movie_nfo(payload: bytes) -> dict[str, Any]:
    root = ET.fromstring(payload)
    if root.tag.casefold() != "movie":
        raise ValueError("NFO root is not <movie>")
    title = (root.findtext("title") or "").strip()
    year = (root.findtext("year") or "").strip()
    ids: set[int] = set()
    for node in root.findall("uniqueid"):
        if str(node.attrib.get("type") or "").casefold() != "tmdb":
            continue
        value = (node.text or "").strip()
        if value.isdigit() and int(value) > 0:
            ids.add(int(value))
    for tag in ("tmdbid", "tmdb_id"):
        value = (root.findtext(tag) or "").strip()
        if value.isdigit() and int(value) > 0:
            ids.add(int(value))
    return {"title": title, "year": year, "tmdb_ids": sorted(ids)}


def episode_numbers(name: str) -> tuple[int, set[int]] | None:
    match = EPISODE_RE.search(PurePosixPath(name).stem)
    if not match:
        return None
    season = int(match.group(1))
    start = int(match.group(2))
    end = int(match.group(3) or start)
    return season, set(range(min(start, end), max(start, end) + 1))


def empty_library_roots(
    alist: AListClient,
    root: str,
    file_paths: set[str],
    *,
    excluded_roots: list[str] | None = None,
) -> list[str]:
    """Return empty first-level work directories that file-only walks miss.

    AList ``walk`` deliberately returns files, so an abandoned title containing
    only empty ``Season`` directories is otherwise invisible.  Restricting this
    check to the first directory below a known media category avoids confusing
    nested season folders or collection parents with independent works.
    """
    normalized_root = root.rstrip("/") or "/"
    root_name = PurePosixPath(normalized_root).name
    excludes = [value.rstrip("/") for value in (excluded_roots or [])]

    if root_name in MEDIA_CATEGORIES:
        category_roots = [normalized_root]
    elif root_name == "影视":
        entries = alist.list(normalized_root, refresh=True)
        available = {
            str(item.get("name"))
            for item in entries
            if item.get("is_dir") and item.get("name") in MEDIA_CATEGORIES
        }
        category_roots = [
            str(PurePosixPath(normalized_root) / category)
            for category in MEDIA_CATEGORIES
            if category in available
        ]
    else:
        return []

    empty_roots: list[str] = []
    for category_root in category_roots:
        for item in alist.list(category_root, refresh=True):
            name = item.get("name")
            if not item.get("is_dir") or not isinstance(name, str) or not name:
                continue
            candidate = str(PurePosixPath(category_root) / name)
            if any(
                candidate == excluded or candidate.startswith(excluded + "/")
                for excluded in excludes
            ):
                continue
            prefix = candidate.rstrip("/") + "/"
            if not any(path.startswith(prefix) for path in file_paths):
                empty_roots.append(candidate)
    return sorted(set(empty_roots))


def _published_episodes(payload: Mapping[str, Any], today: date) -> dict[int, str]:
    output: dict[int, str] = {}
    for row in payload.get("episodes", []) or []:
        if not isinstance(row, Mapping):
            continue
        number = row.get("episode_number")
        air_date = str(row.get("air_date") or "")
        if not isinstance(number, int) or isinstance(number, bool) or number <= 0:
            continue
        if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", air_date):
            continue
        if date.fromisoformat(air_date) > today:
            continue
        output[number] = str(row.get("name") or f"第 {number} 集").strip()
    return output


def analyze_series(
    *,
    root: str,
    nfo: dict[str, Any],
    files: list[dict[str, Any]],
    tmdb_get: Callable[[str], dict[str, Any]],
    today: date,
    required_subtitle_languages: tuple[str, ...] = DEFAULT_REQUIRED_SUBTITLE_LANGUAGES,
) -> dict[str, Any]:
    issues: list[dict[str, Any]] = []
    tmdb_ids = nfo.get("tmdb_ids") if isinstance(nfo.get("tmdb_ids"), list) else []
    if len(tmdb_ids) != 1:
        return {
            "title": nfo.get("title") or PurePosixPath(root).name,
            "target_root": root,
            "tmdb_ids": tmdb_ids,
            "issues": [{
                "code": "missing_or_ambiguous_tmdb_id",
                "severity": "critical",
                "message": "tvshow.nfo 必须且只能包含一个 TMDB ID。",
            }],
            "regular_missing": [],
            "optional_missing": [],
        }

    tmdb_id = int(tmdb_ids[0])
    show = tmdb_get(f"/tv/{tmdb_id}")
    official_title = str(show.get("name") or show.get("original_name") or "").strip()
    original_title = str(show.get("original_name") or official_title).strip()
    videos_by_episode: dict[tuple[int, int], list[str]] = defaultdict(list)
    subtitle_keys: dict[tuple[int, int], list[str]] = defaultdict(list)
    unnumbered_videos: list[str] = []
    wrong_season_dirs: list[str] = []
    multipart_videos: list[str] = []
    names = {PurePosixPath(str(row.get("full_path") or "")).name.casefold() for row in files}
    expected_leaf = official_title
    actual_leaf = PurePosixPath(root).name
    if expected_leaf and actual_leaf != expected_leaf:
        issues.append({
            "code": "canonical_series_leaf_mismatch",
            "severity": "critical",
            "message": "作品目录名与 TMDB canonical 作品树叶子不一致。",
            "actual_leaf": actual_leaf,
            "expected_leaf": expected_leaf,
        })

    for row in files:
        path = str(row.get("full_path") or "")
        suffix = PurePosixPath(path).suffix.lower()
        identity = episode_numbers(path)
        if suffix in VIDEO_EXTS:
            if PART_RE.search(PurePosixPath(path).stem):
                multipart_videos.append(path)
            if not identity:
                # Extras are valid when they are explicitly outside Season dirs.
                if any(SEASON_DIR_RE.match(part) for part in PurePosixPath(path).parts):
                    unnumbered_videos.append(path)
                continue
            season, episodes = identity
            parent_season = next(
                (int(match.group(1)) for part in reversed(PurePosixPath(path).parts[:-1])
                 if (match := SEASON_DIR_RE.match(part))),
                None,
            )
            if parent_season is not None and parent_season != season:
                wrong_season_dirs.append(path)
            for episode in episodes:
                videos_by_episode[(season, episode)].append(path)
        elif suffix in SUBTITLE_EXTS and identity:
            season, episodes = identity
            for episode in episodes:
                subtitle_keys[(season, episode)].append(path)

    duplicate_versions = {
        key: paths for key, paths in videos_by_episode.items() if len(set(paths)) > 1
    }
    intentional_editions = {
        key: paths for key, paths in duplicate_versions.items()
        if len({path for path in paths if "{edition-" not in path.casefold()}) <= 1
        and any("{edition-" in path.casefold() for path in paths)
    }
    duplicate_versions = {
        key: paths for key, paths in duplicate_versions.items()
        if key not in intentional_editions
    }
    orphan_subtitles = {
        key: paths for key, paths in subtitle_keys.items() if key not in videos_by_episode
    }
    missing_subtitles: list[dict[str, Any]] = []
    for (season, episode), video_paths in sorted(videos_by_episode.items()):
        related_subtitles = subtitle_keys.get((season, episode), [])
        for video_path in sorted(set(video_paths)):
            gap = external_subtitle_gap(
                video_path,
                related_subtitles,
                required_languages=required_subtitle_languages,
            )
            if gap is None:
                continue
            missing_subtitles.append({
                "season": season,
                "episode": episode,
                "label": f"S{season:02d}E{episode:02d}",
                **gap,
            })
    if duplicate_versions:
        issues.append({
            "code": "multiple_videos_for_episode",
            "severity": "high",
            "message": "同一季集存在多个视频；可能是版本、误识别或未合并分段。",
            "count": len(duplicate_versions),
            "examples": [
                {"episode": f"S{s:02d}E{e:02d}", "files": paths[:5]}
                for (s, e), paths in list(sorted(duplicate_versions.items()))[:10]
            ],
        })
    if intentional_editions:
        issues.append({
            "code": "intentional_edition_versions",
            "severity": "info",
            "message": "同集多视频均使用显式 {edition-*} 标记，作为版本保留。",
            "count": len(intentional_editions),
        })
    if orphan_subtitles:
        issues.append({
            "code": "subtitle_without_current_video",
            "severity": "high",
            "message": "当前媒体库存在有集号字幕但没有对应视频。",
            "count": len(orphan_subtitles),
            "examples": [
                {"episode": f"S{s:02d}E{e:02d}", "files": paths[:5]}
                for (s, e), paths in list(sorted(orphan_subtitles.items()))[:10]
            ],
        })
    if missing_subtitles:
        reason_counts = Counter(row["reason_code"] for row in missing_subtitles)
        issues.append({
            "code": "video_subtitle_gap",
            "severity": "high",
            "message": "当前视频缺少精确匹配且满足语言要求的外挂字幕；内封字幕仍需媒体流核验。",
            "count": len(missing_subtitles),
            "reason_counts": dict(reason_counts.most_common()),
            "examples": missing_subtitles[:10],
        })
    if wrong_season_dirs:
        issues.append({
            "code": "season_directory_filename_mismatch",
            "severity": "critical",
            "message": "Season 目录与文件名季号不一致。",
            "count": len(wrong_season_dirs),
            "examples": wrong_season_dirs[:10],
        })
    if unnumbered_videos:
        issues.append({
            "code": "unnumbered_video_in_season_directory",
            "severity": "high",
            "message": "Season 目录中存在没有 SxxExx 的视频，Infuse 可能误识别。",
            "count": len(unnumbered_videos),
            "examples": unnumbered_videos[:10],
        })
    if multipart_videos:
        issues.append({
            "code": "multipart_filename_marker",
            "severity": "high",
            "message": "视频名仍含 partX，可能被 Infuse 显示为分段或版本。",
            "count": len(multipart_videos),
            "examples": multipart_videos[:10],
        })

    present_seasons = sorted({season for season, _episode in videos_by_episode})
    regular_missing: list[dict[str, Any]] = []
    optional_missing: list[dict[str, Any]] = []
    seasons = [
        row for row in show.get("seasons", []) or []
        if isinstance(row, Mapping)
        and isinstance(row.get("season_number"), int)
        and not isinstance(row.get("season_number"), bool)
    ]
    for season_row in seasons:
        season = int(season_row["season_number"])
        season_payload = tmdb_get(f"/tv/{tmdb_id}/season/{season}")
        expected = _published_episodes(season_payload, today)
        season_name = str(season_payload.get("name") or season_row.get("name") or "").strip()
        available = {episode for current_season, episode in videos_by_episode if current_season == season}
        target = optional_missing if season == 0 else regular_missing
        for episode in sorted(set(expected) - available):
            target.append({
                "season": season,
                "episode": episode,
                "label": f"S{season:02d}E{episode:02d}",
                "title": expected[episode],
                "season_name": season_name,
                "expected_episode_count": len(expected),
            })

    if regular_missing:
        issues.append({
            "code": "published_regular_episode_missing",
            "severity": "high",
            "message": "按当前 TMDB 已播日期，正片存在缺集。",
            "count": len(regular_missing),
            "examples": regular_missing[:20],
        })

    poster_present = any(name in names for name in ("poster.jpg", "poster.png", "folder.jpg"))
    seasons_with_official_poster = {
        int(row["season_number"])
        for row in seasons
        if isinstance(row.get("poster_path"), str) and row.get("poster_path")
    }
    missing_season_posters = [
        season for season in present_seasons
        if season in seasons_with_official_poster
        if not any(
            candidate in names
            for candidate in (
                f"season {season}-poster.jpg", f"season {season:02d}-poster.jpg",
                f"season{season}-poster.jpg", f"season{season:02d}-poster.jpg",
            )
        )
    ]
    if isinstance(show.get("poster_path"), str) and show.get("poster_path") and not poster_present:
        issues.append({
            "code": "missing_series_poster",
            "severity": "medium",
            "message": "TMDB 有官方图稿，但作品根目录缺少 poster/folder 海报。",
        })
    if missing_season_posters:
        issues.append({
            "code": "missing_season_poster",
            "severity": "low",
            "message": "有视频的季度缺少季度海报。",
            "seasons": missing_season_posters,
        })

    return {
        "title": nfo.get("title") or official_title or PurePosixPath(root).name,
        "official_title": official_title,
        "original_title": original_title,
        "target_root": root,
        "tmdb_ids": [tmdb_id],
        "video_files": sum(1 for row in files if PurePosixPath(str(row.get("full_path") or "")).suffix.lower() in VIDEO_EXTS),
        "subtitle_files": sum(1 for row in files if PurePosixPath(str(row.get("full_path") or "")).suffix.lower() in SUBTITLE_EXTS),
        "present_episodes": len(videos_by_episode),
        "regular_missing": regular_missing,
        "optional_missing": optional_missing,
        "missing_subtitles": missing_subtitles,
        "issues": issues,
    }


def scan_subtitle_inventory(
    alist: AListClient,
    root: str,
    *,
    excluded_roots: list[str] | None = None,
    required_subtitle_languages: tuple[str, ...] = DEFAULT_REQUIRED_SUBTITLE_LANGUAGES,
) -> dict[str, Any]:
    """Audit current AList video/sidecar pairing without requiring TMDB.

    This is the read-only fallback for subtitle evidence when episode
    metadata services are unavailable.  ``tvshow.nfo`` boundaries are used to
    prevent an S01E01 subtitle from one work satisfying another work nearby.
    """
    normalized_excludes = [value.rstrip("/") for value in (excluded_roots or [])]
    rows = alist.walk(
        root,
        refresh=True,
        ignore_orphan_temp=False,
        include_bonus=True,
        include_title_extras=True,
        excluded_roots=normalized_excludes,
    )
    included_roots = formal_media_roots(root)
    paths = sorted({
        str(row.get("full_path") or "")
        for row in rows
        if isinstance(row, Mapping)
        and str(row.get("full_path") or "")
        and _inside_roots(str(row.get("full_path") or ""), included_roots)
        and not any(
            str(row.get("full_path") or "") == excluded
            or str(row.get("full_path") or "").startswith(excluded + "/")
            for excluded in normalized_excludes
        )
    })
    series_roots = sorted({
        str(PurePosixPath(path).parent)
        for path in paths
        if PurePosixPath(path).name.casefold() == "tvshow.nfo"
    }, key=lambda value: (len(PurePosixPath(value).parts), value))

    def work_root(path: str) -> str:
        candidates = [
            candidate for candidate in series_roots
            if path.startswith(candidate.rstrip("/") + "/")
        ]
        return max(
            candidates,
            key=lambda value: len(PurePosixPath(value).parts),
            default=str(PurePosixPath(path).parent),
        )

    subtitle_paths = [
        path for path in paths
        if PurePosixPath(path).suffix.lower() in SUBTITLE_EXTS
    ]
    nfo_stems = {
        str(PurePosixPath(path).with_suffix("")).casefold()
        for path in paths if PurePosixPath(path).suffix.casefold() == ".nfo"
    }
    subtitles_by_identity: dict[tuple[str, int, int], list[str]] = defaultdict(list)
    for subtitle_path in subtitle_paths:
        identity = episode_numbers(subtitle_path)
        if identity is None:
            continue
        season, episodes = identity
        for episode in episodes:
            subtitles_by_identity[(work_root(subtitle_path), season, episode)].append(
                subtitle_path
            )

    missing_subtitles: list[dict[str, Any]] = []
    subtitle_inventory: list[dict[str, Any]] = []
    video_count = 0
    for video_path in paths:
        if PurePosixPath(video_path).suffix.lower() not in VIDEO_EXTS:
            continue
        video_count += 1
        target_root = work_root(video_path)
        identity = episode_numbers(video_path)
        if identity is not None:
            season, episodes = identity
            episode_rows: list[tuple[int | None, int | None]] = [
                (season, episode) for episode in sorted(episodes)
            ]
        else:
            episode_rows = [(None, None)]
        for season, episode in episode_rows:
            related = (
                subtitles_by_identity.get((target_root, season, episode), [])
                if season is not None and episode is not None
                else [
                    path for path in subtitle_paths
                    if companion_stem(path).casefold()
                    == str(PurePosixPath(video_path).with_suffix("")).casefold()
                ]
            )
            gap = external_subtitle_gap(
                video_path,
                related,
                required_languages=required_subtitle_languages,
            )
            inventory_row = {
                "media_type": (
                    "tv" if identity is not None
                    else "movie"
                    if str(PurePosixPath(video_path).with_suffix("")).casefold() in nfo_stems
                    else "extra"
                ),
                "title": PurePosixPath(target_root).name,
                "target_root": target_root,
                **(
                    {
                        "season": season,
                        "episode": episode,
                        "label": f"S{season:02d}E{episode:02d}",
                    }
                    if season is not None and episode is not None else {}
                ),
                "video_path": video_path,
            }
            if gap is None:
                video_stem = str(PurePosixPath(video_path).with_suffix("")).casefold()
                companions = sorted({
                    path for path in related
                    if companion_stem(path).casefold() == video_stem
                })
                inventory_row.update({
                    "status": "external_required_language_present",
                    "required_languages": list(required_subtitle_languages),
                    "companion_subtitles": companions,
                    "candidate_languages": sorted({
                        subtitle_language(PurePosixPath(path).name) or "und"
                        for path in companions
                    }),
                })
            else:
                inventory_row.update({"status": "gap", **gap})
                missing_subtitles.append(dict(inventory_row))
            subtitle_inventory.append(inventory_row)
    return {
        "schema_version": 1,
        "audited_at": datetime.now(timezone.utc).isoformat(),
        "library_root": root,
        "excluded_roots": normalized_excludes,
        "included_roots": list(included_roots),
        # Complete path inventory is retained so a later approved repair plan
        # cannot introduce an un-audited source by merely supplying a hash.
        "inventory_paths": sorted(paths),
        "methodology": "current AList video inventory + exact sidecar basename + explicit subtitle language",
        "subtitle_policy": {
            "scope": "external_sidecar",
            "required_languages": list(required_subtitle_languages),
            "embedded_subtitle_status": "not_inspectable_from_alist_inventory",
        },
        "summary": {
            "videos": video_count,
            "subtitles": len(subtitle_paths),
            "subtitle_inventory_rows": len(subtitle_inventory),
            "missing_subtitles": len(missing_subtitles),
            "reason_codes": dict(Counter(
                row["reason_code"] for row in missing_subtitles
            ).most_common()),
        },
        "missing_subtitles": missing_subtitles,
        "subtitle_inventory": subtitle_inventory,
    }


def scan_library(
    alist: AListClient,
    tmdb: TMDBClient,
    root: str,
    *,
    today: date | None = None,
    excluded_roots: list[str] | None = None,
    required_subtitle_languages: tuple[str, ...] = DEFAULT_REQUIRED_SUBTITLE_LANGUAGES,
) -> dict[str, Any]:
    normalized_excludes = [value.rstrip("/") for value in (excluded_roots or [])]
    raw_files = alist.walk(
        root,
        refresh=True,
        ignore_orphan_temp=False,
        include_bonus=True,
        include_title_extras=True,
        excluded_roots=normalized_excludes,
    )
    included_roots = formal_media_roots(root)
    all_files = [
        row for row in raw_files
        if _inside_roots(str(row.get("full_path") or ""), included_roots)
        if not any(
            (path := str(row.get("full_path") or "")) == excluded
            or path.startswith(excluded + "/")
            for excluded in normalized_excludes
        )
    ]
    file_paths = {str(row.get("full_path") or "") for row in all_files}
    empty_roots = empty_library_roots(
        alist,
        root,
        file_paths,
        excluded_roots=normalized_excludes,
    )
    video_stems = {
        str(PurePosixPath(path).with_suffix(""))
        for path in file_paths
        if PurePosixPath(path).suffix.lower() in VIDEO_EXTS
    }
    nfo_rows: dict[str, dict[str, Any]] = {}
    movie_rows: dict[str, dict[str, Any]] = {}
    nfo_errors: list[dict[str, str]] = []
    nfo_stems = {
        str(PurePosixPath(str(row.get("full_path") or "")).with_suffix(""))
        for row in all_files
        if PurePosixPath(str(row.get("full_path") or "")).suffix.lower() == ".nfo"
    }
    recognized_artwork_names = re.compile(
        r"^(?:poster|folder|fanart|season[ ._-]*\d+[ ._-]*poster)\.(?:jpe?g|png)$",
        re.IGNORECASE,
    )
    residual_inventory: list[dict[str, Any]] = []
    for path in sorted(file_paths):
        pure = PurePosixPath(path)
        suffix = pure.suffix.casefold()
        managed = (
            suffix in VIDEO_EXTS | SUBTITLE_EXTS | {".nfo"}
            or bool(recognized_artwork_names.fullmatch(pure.name))
            or (
                suffix in {".jpg", ".jpeg", ".png"}
                and str(pure.with_suffix("")) in video_stems
            )
        )
        if managed:
            continue
        decision = classify_residual(path)
        residual_inventory.append({
            "path": path,
            "action": decision.action,
            "kind": decision.kind,
            "evidence": list(decision.evidence),
        })
    for row in all_files:
        path = str(row.get("full_path") or "")
        if (
            PurePosixPath(path).name.casefold() != "tvshow.nfo"
            and (
                PurePosixPath(path).suffix.lower() != ".nfo"
                or str(PurePosixPath(path).with_suffix("")) not in video_stems
            )
        ):
            continue
        try:
            raw_nfo = alist.read_file_prefix(path, max_bytes=256 * 1024)
            if PurePosixPath(path).name.casefold() == "tvshow.nfo":
                series_root = str(PurePosixPath(path).parent)
                nfo_rows[series_root] = parse_tvshow_nfo(raw_nfo)
            elif PurePosixPath(path).suffix.lower() == ".nfo":
                movie_rows[str(PurePosixPath(path).with_suffix(""))] = parse_movie_nfo(raw_nfo)
        except Exception as exc:  # Evidence capture must continue for sibling titles.
            # Non-movie sidecar NFOs are not inventory identities.
            if PurePosixPath(path).name.casefold() == "tvshow.nfo":
                nfo_errors.append({"path": path, "error": str(exc)})

    roots = sorted(nfo_rows, key=lambda value: (len(PurePosixPath(value).parts), value))
    assigned: dict[str, list[dict[str, Any]]] = {series_root: [] for series_root in roots}
    uncovered_media: list[str] = []
    for row in all_files:
        path = str(row.get("full_path") or "")
        suffix = PurePosixPath(path).suffix.lower()
        if suffix not in VIDEO_EXTS | SUBTITLE_EXTS | {".jpg", ".jpeg", ".png", ".nfo"}:
            continue
        candidates = [series_root for series_root in roots if path.startswith(series_root.rstrip("/") + "/")]
        if candidates:
            assigned[max(candidates, key=lambda value: len(PurePosixPath(value).parts))].append(row)
        elif (
            suffix in VIDEO_EXTS | SUBTITLE_EXTS
            and companion_stem(path) not in nfo_stems
        ):
            uncovered_media.append(path)

    effective_today = today or date.today()
    def inspect_series(series_root: str) -> dict[str, Any]:
        return analyze_series(
            root=series_root,
            nfo=nfo_rows[series_root],
            files=assigned[series_root],
            tmdb_get=tmdb.get,
            today=effective_today,
            required_subtitle_languages=required_subtitle_languages,
        )

    projects = _parallel_map_with_serial_retry(inspect_series, roots)

    def inspect_movie(item: tuple[str, dict[str, Any]]) -> dict[str, Any]:
        stem, nfo = item
        matching_videos = sorted(
            path for path in file_paths
            if str(PurePosixPath(path).with_suffix("")) == stem
            and PurePosixPath(path).suffix.lower() in VIDEO_EXTS
        )
        tmdb_ids = nfo.get("tmdb_ids") if isinstance(nfo.get("tmdb_ids"), list) else []
        issues: list[dict[str, Any]] = []
        if len(matching_videos) != 1:
            issues.append({
                "code": "movie_nfo_video_pair_mismatch",
                "severity": "critical",
                "message": "电影 NFO 必须且只能对应一个同名视频。",
                "count": len(matching_videos),
            })
        if len(tmdb_ids) != 1:
            issues.append({
                "code": "missing_or_ambiguous_movie_tmdb_id",
                "severity": "critical",
                "message": "电影 NFO 必须且只能包含一个 TMDB ID。",
            })
        official_title = ""
        official_year = ""
        official_poster_available = False
        if len(tmdb_ids) == 1:
            movie = tmdb.get(f"/movie/{int(tmdb_ids[0])}")
            official_title = str(movie.get("title") or movie.get("original_title") or "").strip()
            official_poster_available = bool(
                isinstance(movie.get("poster_path"), str) and movie.get("poster_path")
            )
            release_date = str(movie.get("release_date") or "")
            official_year = release_date[:4] if re.fullmatch(r"\d{4}-\d{2}-\d{2}", release_date) else ""
            expected_leaf = (
                f"{official_title} ({official_year})"
                if official_title and official_year else official_title
            )
            actual_leaf = PurePosixPath(stem).parent.name
            if expected_leaf and actual_leaf != expected_leaf:
                issues.append({
                    "code": "canonical_movie_leaf_mismatch",
                    "severity": "critical",
                    "message": "电影目录名与 TMDB canonical 作品树叶子不一致。",
                    "actual_leaf": actual_leaf,
                    "expected_leaf": expected_leaf,
                })
            nfo_year = str(nfo.get("year") or "")
            if nfo_year and official_year and nfo_year != official_year:
                issues.append({
                    "code": "movie_year_tmdb_mismatch",
                    "severity": "high",
                    "message": "电影 NFO 年份与当前 TMDB 首映年份不一致。",
                    "nfo_year": nfo_year,
                    "tmdb_year": official_year,
                })
        poster_candidates = {
            f"{stem}.jpg", f"{stem}.jpeg", f"{stem}.png",
            f"{stem}-poster.jpg", f"{stem}-poster.png",
            str(PurePosixPath(stem).parent / "poster.jpg"),
            str(PurePosixPath(stem).parent / "folder.jpg"),
        }
        if official_poster_available and not poster_candidates & file_paths:
            issues.append({
                "code": "missing_movie_poster",
                "severity": "medium",
                "message": "TMDB 有官方图稿，但电影没有同名海报或所在目录通用海报。",
            })
        movie_missing_subtitles: list[dict[str, Any]] = []
        for video_path in matching_videos:
            related_subtitles = sorted(
                path for path in file_paths
                if PurePosixPath(path).suffix.lower() in SUBTITLE_EXTS
                and PurePosixPath(path).parent == PurePosixPath(video_path).parent
            )
            gap = external_subtitle_gap(
                video_path,
                related_subtitles,
                required_languages=required_subtitle_languages,
            )
            if gap is not None:
                movie_missing_subtitles.append(gap)
        if movie_missing_subtitles:
            issues.append({
                "code": "movie_video_subtitle_gap",
                "severity": "high",
                "message": "电影视频缺少精确匹配且满足语言要求的外挂字幕；内封字幕仍需媒体流核验。",
                "count": len(movie_missing_subtitles),
                "examples": movie_missing_subtitles[:10],
            })
        return {
            "title": nfo.get("title") or official_title or PurePosixPath(stem).name,
            "official_title": official_title,
            "target_stem": stem,
            "tmdb_ids": tmdb_ids,
            "video_files": matching_videos,
            "missing_subtitles": movie_missing_subtitles,
            "issues": issues,
        }

    movie_projects = _parallel_map_with_serial_retry(
        inspect_movie, sorted(movie_rows.items())
    )

    multipart_media = sorted(
        path for path in file_paths
        if PurePosixPath(path).suffix.lower() in VIDEO_EXTS
        and PART_RE.search(PurePosixPath(path).stem)
    )
    direct_files_by_dir: dict[str, set[str]] = defaultdict(set)
    for path in file_paths:
        direct_files_by_dir[str(PurePosixPath(path).parent)].add(PurePosixPath(path).name.casefold())
    work_dirs = set(nfo_rows) | {str(PurePosixPath(stem).parent) for stem in movie_rows}
    collection_roots: list[dict[str, Any]] = []
    candidate_dirs = {
        str(PurePosixPath(work_dir).parents[index])
        for work_dir in work_dirs
        for index in range(len(PurePosixPath(work_dir).parents))
        if str(PurePosixPath(work_dir).parents[index]).startswith(root.rstrip("/") + "/")
    }
    for directory in sorted(candidate_dirs):
        if directory in nfo_rows:
            continue
        # `/影视/番剧` and sibling category roots are navigation boundaries,
        # not franchise folders that need their own Infuse artwork.
        if str(PurePosixPath(directory).parent) == root.rstrip("/"):
            continue
        descendants = [
            work_dir for work_dir in work_dirs
            if work_dir.startswith(directory.rstrip("/") + "/")
        ]
        immediate_branches = {
            PurePosixPath(work_dir).parts[len(PurePosixPath(directory).parts)]
            for work_dir in descendants
            if len(PurePosixPath(work_dir).parts) > len(PurePosixPath(directory).parts)
        }
        if len(immediate_branches) < 2:
            continue
        names = direct_files_by_dir.get(directory, set())
        collection_roots.append({
            "target_root": directory,
            "descendant_works": len(descendants),
            "poster_present": any(name in names for name in ("poster.jpg", "poster.png", "folder.jpg")),
        })
    duplicate_tmdb = {
        tmdb_id: [row["target_root"] for row in projects if tmdb_id in row.get("tmdb_ids", [])]
        for tmdb_id in sorted({tmdb_id for row in projects for tmdb_id in row.get("tmdb_ids", [])})
    }
    duplicate_tmdb = {key: value for key, value in duplicate_tmdb.items() if len(value) > 1}
    structured_missing_subtitles = [
        {
            "media_type": "tv",
            "title": str(project.get("title") or ""),
            "target_root": str(project.get("target_root") or ""),
            "tmdb_ids": list(project.get("tmdb_ids") or []),
            **gap,
        }
        for project in projects
        for gap in project.get("missing_subtitles", [])
        if isinstance(gap, dict)
    ] + [
        {
            "media_type": "movie",
            "title": str(movie.get("title") or ""),
            "target_root": str(movie.get("target_stem") or ""),
            "tmdb_ids": list(movie.get("tmdb_ids") or []),
            **gap,
        }
        for movie in movie_projects
        for gap in movie.get("missing_subtitles", [])
        if isinstance(gap, dict)
    ]
    return {
        "schema_version": 1,
        "audited_at": datetime.now(timezone.utc).isoformat(),
        "published_cutoff": effective_today.isoformat(),
        "library_root": root,
        "excluded_roots": normalized_excludes,
        "included_roots": list(included_roots),
        "inventory_paths": sorted(file_paths),
        "methodology": "live AList inventory + tvshow.nfo TMDB identity + currently published TMDB episodes",
        "summary": {
            "series": len(projects),
            "movies": len(movie_projects),
            "regular_missing": sum(len(row.get("regular_missing", [])) for row in projects),
            "optional_missing": sum(len(row.get("optional_missing", [])) for row in projects),
            "missing_subtitles": len(structured_missing_subtitles),
            "issue_codes": dict(Counter(
                issue["code"] for row in projects for issue in row.get("issues", [])
            ).most_common()),
            "uncovered_media": len(uncovered_media),
            "nfo_parse_errors": len(nfo_errors),
            "duplicate_tmdb_ids": len(duplicate_tmdb),
            "movie_issue_codes": dict(Counter(
                issue["code"] for row in movie_projects for issue in row.get("issues", [])
            ).most_common()),
            "multipart_media": len(multipart_media),
            "collection_roots_without_poster": sum(
                1 for row in collection_roots if not row["poster_present"]
            ),
            "empty_library_roots": len(empty_roots),
            "residual_files": len(residual_inventory),
            "residual_actions": dict(Counter(
                row["action"] for row in residual_inventory
            ).most_common()),
            "residual_kinds": dict(Counter(
                row["kind"] for row in residual_inventory
            ).most_common()),
        },
        "nfo_parse_errors": nfo_errors,
        "uncovered_media": uncovered_media,
        "duplicate_tmdb_ids": duplicate_tmdb,
        "projects": projects,
        "movies": movie_projects,
        "multipart_media": multipart_media,
        "collection_roots": collection_roots,
        "empty_library_roots": empty_roots,
        "residual_inventory": residual_inventory,
        "missing_subtitles": structured_missing_subtitles,
        "subtitle_policy": {
            "scope": "external_sidecar",
            "required_languages": list(required_subtitle_languages),
            "embedded_subtitle_status": "not_inspectable_from_alist_inventory",
        },
        "tmdb_cache": tmdb.cache_report(),
    }


def main() -> int:
    parser = argparse.ArgumentParser(
        description="显式启动的一次性/手工只读媒体库审计；不创建任务也不修改 AList",
    )
    parser.add_argument("--root", default="/quark/影视/番剧")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--today", type=date.fromisoformat)
    parser.add_argument("--exclude-root", action="append", default=None)
    parser.add_argument(
        "--subtitle-language",
        action="append",
        dest="subtitle_languages",
        help="要求的外挂字幕语言（可重复；默认 zh-CN）",
    )
    parser.add_argument(
        "--subtitle-only",
        action="store_true",
        help="只用 AList 当前文件清单审计字幕，不请求 TMDB",
    )
    parser.add_argument(
        "--tmdb-resolve-ip",
        help="仅本次进程把官方 api.themoviedb.org 解析到已独立核验的 IP",
    )
    args = parser.parse_args()
    alist = AListClient(
        os.environ.get("ALIST_URL", "http://127.0.0.1:5244"),
        os.environ.get("ALIST_USERNAME", ""),
        os.environ.get("ALIST_PASSWORD", ""),
        allow_insecure_http=True,
    )
    alist.login()
    excluded_roots = args.exclude_root
    if excluded_roots is None:
        excluded_roots = default_excluded_roots(args.root)
    required_subtitle_languages = tuple(
        args.subtitle_languages or DEFAULT_REQUIRED_SUBTITLE_LANGUAGES
    )
    if args.subtitle_only:
        payload = scan_subtitle_inventory(
            alist,
            args.root,
            excluded_roots=excluded_roots,
            required_subtitle_languages=required_subtitle_languages,
        )
    else:
        with temporary_tmdb_dns_override(args.tmdb_resolve_ip):
            tmdb = TMDBClient(os.environ.get("TMDB_API_KEY", ""))
            payload = scan_library(
                alist,
                tmdb,
                args.root,
                today=args.today,
                excluded_roots=excluded_roots,
                required_subtitle_languages=required_subtitle_languages,
            )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, allow_nan=False) + "\n",
        encoding="utf-8",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
