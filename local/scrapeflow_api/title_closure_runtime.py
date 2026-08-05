"""Production I/O adapters for one signed title-closure pass.

The factories in this module are intentionally inert until their returned
callables are invoked.  They never widen a work root to a category/library
root and they own no timer, queue, coordinator, or global task state.
"""

from __future__ import annotations

from datetime import date
import posixpath
from pathlib import Path, PurePosixPath
import re
import threading
from typing import Any, Callable, Mapping, Sequence

from engine.tools.audit_live_library import scan_library
from engine.tools.probe_burned_in_subtitles import _probe_video
from engine.scrapeflow.one_time_tv_exclusion_scope import (
    ExactNestedRootExclusionAListView,
    validate_nested_excluded_roots,
)

from .validation import TARGET_CATEGORY_PARENTS, canonical_digest


LibraryScanner = Callable[..., Mapping[str, Any]]
EpisodeGapScanner = Callable[[Mapping[str, Any]], Sequence[Mapping[str, Any]]]
OCRProbe = Callable[..., Mapping[str, Any]]
OCREngineFactory = Callable[[], Any]


_TV_UNSAFE_ISSUES = frozenset({
    "missing_or_ambiguous_tmdb_id",
    "season_directory_filename_mismatch",
    "unnumbered_video_in_season_directory",
    "multipart_filename_marker",
})
_MOVIE_IDENTITY_ISSUES = frozenset({
    "movie_nfo_video_pair_mismatch",
    "missing_or_ambiguous_movie_tmdb_id",
    "movie_year_tmdb_mismatch",
})
_VIDEO_EXTENSIONS = frozenset({
    ".mkv", ".mp4", ".m4v", ".m2ts", ".ts", ".avi", ".mov", ".webm",
})


def _inside(path: str, root: str) -> bool:
    return path == root or path.startswith(root.rstrip("/") + "/")


def _validated_target(target: Mapping[str, Any]) -> dict[str, Any]:
    if not isinstance(target, Mapping):
        raise ValueError("当前作品目标必须是对象")
    required = {"media_type", "target_root", "category", "tmdb_id", "title"}
    if not required.issubset(target) or set(target) - (required | {"excluded_roots"}):
        raise ValueError("当前作品目标字段不完整或包含未知字段")
    media_type = target.get("media_type")
    if media_type not in {"tv", "movie"}:
        raise ValueError("当前作品 media_type 必须是 tv 或 movie")
    root = target.get("target_root")
    if not isinstance(root, str) or not root or "\\" in root:
        raise ValueError("当前作品 target_root 无效")
    normalized = posixpath.normpath(root)
    if (
        normalized != root
        or not normalized.startswith("/")
        or any(ord(character) < 32 for character in normalized)
    ):
        raise ValueError("当前作品 target_root 必须是规范化绝对路径")
    matches = [
        category
        for category, parent in TARGET_CATEGORY_PARENTS.items()
        if normalized != parent and _inside(normalized, parent)
    ]
    if len(matches) != 1 or target.get("category") != matches[0]:
        raise ValueError("当前作品不在唯一正式媒体分类下")
    tmdb_id = target.get("tmdb_id")
    title = target.get("title")
    if (
        isinstance(tmdb_id, bool) or not isinstance(tmdb_id, int)
        or tmdb_id <= 0 or not isinstance(title, str) or not title.strip()
    ):
        raise ValueError("当前作品缺少唯一 TMDB 身份")
    exclusions = target.get("excluded_roots", [])
    if exclusions:
        if media_type != "tv":
            raise ValueError("只有 TV 作品可以排除嵌套独立作品")
        exclusions = validate_nested_excluded_roots(normalized, exclusions)
    elif exclusions != []:
        raise ValueError("当前作品 excluded_roots 必须是数组")
    return {
        "media_type": media_type,
        "target_root": normalized,
        "category": matches[0],
        "tmdb_id": tmdb_id,
        "title": title.strip(),
        "excluded_roots": exclusions,
    }


def _objects(value: Any, label: str) -> list[dict[str, Any]]:
    if not isinstance(value, list) or not all(isinstance(row, Mapping) for row in value):
        raise ValueError(f"当前作品审计 {label} 无效")
    return [dict(row) for row in value]


def _validate_audit_envelope(
    audit: Any, root: str, *, expected_excluded_roots: list[str] | None = None,
) -> dict[str, Any]:
    if not isinstance(audit, Mapping) or audit.get("schema_version") != 1:
        raise ValueError("当前作品 scan_library 证据无效")
    if (
        audit.get("library_root") != root
        or audit.get("included_roots") != [root]
        or audit.get("excluded_roots") != (expected_excluded_roots or [])
    ):
        raise ValueError("当前作品 scan_library 扩大或隐藏了扫描范围")
    if audit.get("nfo_parse_errors") != []:
        raise ValueError("当前作品 NFO 无法完整解析")
    duplicate_tmdb = audit.get("duplicate_tmdb_ids")
    if not isinstance(duplicate_tmdb, Mapping) or duplicate_tmdb:
        raise ValueError("当前作品存在重复 TMDB 身份")
    uncovered = audit.get("uncovered_media")
    if not isinstance(uncovered, list) or uncovered:
        raise ValueError("当前作品存在未归属媒体，无法安全判定缺口")
    return {
        **dict(audit),
        "projects": _objects(audit.get("projects"), "projects"),
        "movies": _objects(audit.get("movies"), "movies"),
    }


def _issue_codes(row: Mapping[str, Any]) -> set[str]:
    issues = row.get("issues")
    if not isinstance(issues, list) or not all(isinstance(issue, Mapping) for issue in issues):
        raise ValueError("当前作品 issues 证据无效")
    codes: set[str] = set()
    for issue in issues:
        code = issue.get("code")
        if not isinstance(code, str) or not code:
            raise ValueError("当前作品 issue code 无效")
        codes.add(code)
    return codes


def tmdb_tv_aliases(tmdb: Any, tmdb_id: int) -> list[str]:
    """Return bounded official aliases for this exact TMDB TV identity."""
    get = getattr(tmdb, "get", None)
    if not callable(get):
        return []
    try:
        payload = get(f"/tv/{tmdb_id}/alternative_titles")
    except Exception:
        # Alias enrichment must never turn a transient TMDB endpoint failure
        # into guessed identity evidence.  The already verified primary and
        # original names remain available.
        return []
    rows = payload.get("results") if isinstance(payload, Mapping) else None
    if not isinstance(rows, list):
        return []
    aliases: list[str] = []
    seen: set[str] = set()
    preferred_countries = {
        "GB": 0, "US": 1, "CA": 2, "AU": 3, "NZ": 4, "IE": 5,
        "CN": 6, "TW": 7, "HK": 8, "JP": 9, "KR": 10,
    }
    ranked_rows = sorted(
        enumerate(rows[:100]),
        key=lambda item: (
            preferred_countries.get(
                str(item[1].get("iso_3166_1") or "")
                if isinstance(item[1], Mapping) else "",
                100,
            ),
            item[0],
        ),
    )
    for _, row in ranked_rows:
        title = row.get("title") if isinstance(row, Mapping) else None
        if not isinstance(title, str) or not title.strip():
            continue
        value = title.strip()
        key = re.sub(r"[^\w\u3400-\u9fff]+", "", value.casefold())
        if not key or key in seen:
            continue
        seen.add(key)
        aliases.append(value)
    return aliases


def _tmdb_tv_episode_enrichment(
    tmdb: Any,
    tmdb_id: int,
    gaps: Sequence[Mapping[str, Any]],
) -> tuple[
    dict[tuple[int, int], list[str]],
    dict[tuple[int, int], list[dict[str, Any]]],
]:
    """Return bounded multilingual names for exact published S00 episodes.

    Season-zero releases frequently use the original Japanese or an English
    ``Break Time`` title instead of TMDB's localized Chinese name.  Query only
    the already verified TMDB identity and exact episode numbers; aliases never
    create a gap or weaken the numeric/title evidence gates.
    """
    get = getattr(tmdb, "get", None)
    if not callable(get):
        return {}, {}
    wanted = {
        int(gap["episode"])
        for gap in gaps
        if gap.get("season") == 0
        and type(gap.get("episode")) is int
        and int(gap["episode"]) > 0
    }
    if not wanted:
        return {}, {}
    output: dict[tuple[int, int], list[str]] = {}
    seen: dict[tuple[int, int], set[str]] = {}
    all_names: dict[int, list[str]] = {}
    for language in ("ja-JP", "en-US"):
        try:
            payload = get(f"/tv/{tmdb_id}/season/0", language=language)
        except Exception:
            # This is recall enrichment only.  The localized scan has already
            # established the exact TMDB identity and remains authoritative.
            continue
        episodes = payload.get("episodes") if isinstance(payload, Mapping) else None
        if not isinstance(episodes, list):
            continue
        for episode in episodes:
            if not isinstance(episode, Mapping):
                continue
            number = episode.get("episode_number")
            name = episode.get("name")
            if type(number) is not int or number <= 0:
                continue
            if not isinstance(name, str) or not name.strip():
                continue
            value = name.strip()
            if value not in all_names.setdefault(number, []):
                all_names[number].append(value)
            if number not in wanted:
                continue
            key = re.sub(r"[^\w\u3400-\u9fff]+", "", value.casefold())
            identity = (0, number)
            if not key or key in seen.setdefault(identity, set()):
                continue
            seen[identity].add(key)
            output.setdefault(identity, []).append(value)
    marker_re = re.compile(r"(?i)\b(\d{1,2})(?:st|nd|rd|th)\s+season\b")
    marker_groups: dict[tuple[int, str], list[int]] = {}
    for number, names in all_names.items():
        markers = {
            (
                int(match.group(1)),
                re.sub(
                    r"[^\w\u3400-\u9fff]+", "",
                    name[:match.start()].casefold(),
                ),
            )
            for name in names
            for match in marker_re.finditer(name)
            if name[:match.start()].strip(" -:：")
        }
        for marker in markers:
            marker_groups.setdefault(marker, []).append(number)
    source_aliases: dict[tuple[int, int], list[dict[str, Any]]] = {}
    for number in wanted:
        names = all_names.get(number) or []
        markers = {
            (
                int(match.group(1)),
                re.sub(
                    r"[^\w\u3400-\u9fff]+", "",
                    name[:match.start()].casefold(),
                ),
            )
            for name in names
            for match in marker_re.finditer(name)
            if name[:match.start()].strip(" -:：")
        }
        if len(markers) != 1:
            continue
        marker_key = next(iter(markers))
        marker = marker_key[0]
        members = sorted(set(marker_groups.get(marker_key) or []))
        if len(members) < 2 or number not in members:
            continue
        series_titles: list[str] = []
        for name in names:
            match = marker_re.search(name)
            if match:
                prefix = name[:match.start()].strip(" -:：")
            elif ":" in name:
                prefix = name.rsplit(":", 1)[0].strip(" -:：")
            else:
                continue
            if prefix and prefix not in series_titles:
                series_titles.append(prefix)
        if not series_titles:
            continue
        source_aliases[(0, number)] = [{
            "season": marker,
            "episode": members.index(number) + 1,
            "series_titles": series_titles[:4],
        }]
    return output, source_aliases


def _tmdb_tv_episode_title_aliases(
    tmdb: Any,
    tmdb_id: int,
    gaps: Sequence[Mapping[str, Any]],
) -> dict[tuple[int, int], list[str]]:
    aliases, _source_aliases = _tmdb_tv_episode_enrichment(tmdb, tmdb_id, gaps)
    return aliases


def tmdb_tv_episode_enrichment(
    tmdb: Any,
    tmdb_id: int,
    episodes: Sequence[Mapping[str, Any]],
) -> tuple[
    dict[tuple[int, int], list[str]],
    dict[tuple[int, int], list[dict[str, Any]]],
]:
    """Expose bounded episode-title and release-number aliases to callers."""
    return _tmdb_tv_episode_enrichment(tmdb, tmdb_id, episodes)


def _attach_tmdb_episode_title_aliases(
    gaps: Sequence[Mapping[str, Any]], tmdb: Any, tmdb_id: int,
) -> list[dict[str, Any]]:
    aliases, source_aliases = _tmdb_tv_episode_enrichment(tmdb, tmdb_id, gaps)
    output: list[dict[str, Any]] = []
    for gap in gaps:
        row = dict(gap)
        values = aliases.get((int(row.get("season") or 0), int(row.get("episode") or 0)), [])
        primary_key = re.sub(
            r"[^\w\u3400-\u9fff]+", "", str(row.get("title") or "").casefold(),
        )
        bounded = [
            value for value in values
            if re.sub(r"[^\w\u3400-\u9fff]+", "", value.casefold()) != primary_key
        ][:4]
        if bounded:
            row["title_aliases"] = bounded
        release_aliases = source_aliases.get(
            (int(row.get("season") or 0), int(row.get("episode") or 0)),
        )
        if release_aliases:
            row["source_episode_aliases"] = release_aliases
        output.append(row)
    return output


def _tv_gap(
    raw: Mapping[str, Any], *, lane: str, target: Mapping[str, Any],
    project: Mapping[str, Any], aliases: Sequence[str] = (),
) -> dict[str, Any]:
    season = raw.get("season")
    episode = raw.get("episode")
    if (
        isinstance(season, bool) or not isinstance(season, int) or season < 0
        or isinstance(episode, bool) or not isinstance(episode, int) or episode <= 0
    ):
        raise ValueError("当前作品缺集季集号无效")
    if (lane == "regular" and season == 0) or (lane == "s00" and season != 0):
        raise ValueError("当前作品 regular/S00 缺口分类不一致")
    expected_label = f"S{season:02d}E{episode:02d}"
    if raw.get("label") != expected_label:
        raise ValueError("当前作品缺集 label 与季集号不一致")
    episode_title = raw.get("title")
    if not isinstance(episode_title, str) or not episode_title.strip():
        raise ValueError("当前作品缺集缺少 TMDB 集标题")
    expected_count = raw.get("expected_episode_count")
    if (
        isinstance(expected_count, bool)
        or not isinstance(expected_count, int)
        or expected_count <= 0
    ):
        raise ValueError("当前作品缺集 expected_episode_count 无效")
    media = {
        "media_type": "tv",
        "tmdb_id": target["tmdb_id"],
        "title": str(project.get("official_title") or target["title"]),
        "original_title": str(project.get("original_title") or ""),
        "target_root": target["target_root"],
        "category": target["category"],
        **({"aliases": list(aliases)} if aliases else {}),
    }
    return {
        "kind": "missing_episode",
        "lane": lane,
        "season": season,
        "episode": episode,
        "label": expected_label,
        "title": episode_title.strip(),
        "season_name": str(raw.get("season_name") or "").strip(),
        "expected_episode_count": expected_count,
        "target_root": target["target_root"],
        "media_type": "tv",
        "tmdb_id": target["tmdb_id"],
        "media": media,
    }


def _scan_tv_gaps(
    audit: Mapping[str, Any], target: Mapping[str, Any], *,
    aliases: Sequence[str] = (),
) -> list[dict[str, Any]]:
    projects = audit["projects"]
    if len(projects) != 1 or audit["movies"] and any(
        not _inside(str(movie.get("target_stem") or ""), target["target_root"])
        for movie in audit["movies"]
    ):
        raise ValueError("当前 TV 根目录包含额外或超范围作品")
    project = projects[0]
    if (
        project.get("target_root") != target["target_root"]
        or project.get("tmdb_ids") != [target["tmdb_id"]]
        or not isinstance(project.get("official_title"), str)
        or not project.get("official_title").strip()
    ):
        raise ValueError("当前 TV 的 root/NFO/TMDB 身份与签名计划不一致")
    unsafe = _issue_codes(project) & _TV_UNSAFE_ISSUES
    if unsafe:
        raise ValueError("当前 TV 存在会误导缺集判定的结构问题: " + ",".join(sorted(unsafe)))
    regular = _objects(project.get("regular_missing"), "regular_missing")
    specials = _objects(project.get("optional_missing"), "optional_missing")
    gaps = [
        *(_tv_gap(
            row, lane="regular", target=target, project=project, aliases=aliases,
        ) for row in regular),
        *(_tv_gap(
            row, lane="s00", target=target, project=project, aliases=aliases,
        ) for row in specials),
    ]
    identities = {
        (gap["season"], gap["episode"]): canonical_digest(gap)
        for gap in gaps
    }
    if len(identities) != len(gaps):
        raise ValueError("当前 TV 缺集证据包含重复季集身份")
    return sorted(gaps, key=lambda gap: (gap["season"], gap["episode"]))


def _validate_movie_identity(audit: Mapping[str, Any], target: Mapping[str, Any]) -> None:
    if audit["projects"] or len(audit["movies"]) != 1:
        raise ValueError("当前电影根目录必须且只能包含一个电影身份")
    movie = audit["movies"][0]
    stem = movie.get("target_stem")
    videos = movie.get("video_files")
    if (
        not isinstance(stem, str)
        or str(PurePosixPath(stem).parent) != target["target_root"]
        or PurePosixPath(stem).suffix.casefold() in _VIDEO_EXTENSIONS
        or movie.get("tmdb_ids") != [target["tmdb_id"]]
        or not isinstance(movie.get("official_title"), str)
        or not movie.get("official_title").strip()
        or not isinstance(videos, list)
        or len(videos) != 1
        or not isinstance(videos[0], str)
        or str(PurePosixPath(videos[0]).with_suffix("")) != stem
    ):
        raise ValueError("当前电影的 NFO/视频/TMDB 身份与签名计划不一致")
    unsafe = _issue_codes(movie) & _MOVIE_IDENTITY_ISSUES
    if unsafe:
        raise ValueError("当前电影身份证据不一致: " + ",".join(sorted(unsafe)))


def make_current_title_episode_gap_scanner(
    alist: Any,
    tmdb: Any,
    *,
    today: date | None = None,
    library_scanner: LibraryScanner = scan_library,
) -> EpisodeGapScanner:
    """Return a scanner permanently restricted to each supplied title root."""
    if today is not None and type(today) is not date:
        raise ValueError("today 必须是 date 或 None")
    if not callable(library_scanner):
        raise ValueError("library_scanner 必须可调用")

    def scan_target(target: Mapping[str, Any]) -> list[dict[str, Any]]:
        current = _validated_target(target)
        root = current["target_root"]
        exclusions = current["excluded_roots"]
        scoped_alist = (
            ExactNestedRootExclusionAListView(alist, root, exclusions)
            if exclusions else alist
        )
        audit = library_scanner(
            scoped_alist,
            tmdb,
            root,
            today=today,
            excluded_roots=exclusions,
            required_subtitle_languages=("zh-CN",),
        )
        scoped = _validate_audit_envelope(
            audit, root, expected_excluded_roots=exclusions,
        )
        if current["media_type"] == "tv":
            gaps = _scan_tv_gaps(
                scoped, current,
                aliases=tmdb_tv_aliases(tmdb, current["tmdb_id"]),
            )
            return _attach_tmdb_episode_title_aliases(
                gaps, tmdb, current["tmdb_id"],
            )
        _validate_movie_identity(scoped, current)
        return []

    return scan_target


def make_current_tv_exclusion_episode_gap_scanner(
    alist: Any,
    tmdb: Any,
    scope_value: Mapping[str, Any],
    *,
    today: date | None = None,
    library_scanner: LibraryScanner = scan_library,
) -> EpisodeGapScanner:
    """One-time TV scanner with predecessor-sealed nested exclusions."""
    from engine.scrapeflow.one_time_tv_exclusion_scope import (
        ExactTVExclusionAListView,
        validate_exact_tv_exclusion_scope,
    )

    scope = validate_exact_tv_exclusion_scope(scope_value)
    if today is not None and type(today) is not date:
        raise ValueError("today 必须是 date 或 None")
    if not callable(library_scanner):
        raise ValueError("library_scanner 必须可调用")

    def scan_target(target: Mapping[str, Any]) -> list[dict[str, Any]]:
        current = _validated_target(target)
        if (
            current["media_type"] != "tv"
            or current["target_root"] != scope["target_root"]
            or current["category"] != scope["category"]
            or current["tmdb_id"] != scope["tmdb_id"]
            or current["title"] != scope["title"]
        ):
            raise ValueError("TV 排除范围与当前作品身份不一致")
        view = ExactTVExclusionAListView(alist, scope)
        audit = library_scanner(
            view,
            tmdb,
            current["target_root"],
            today=today,
            excluded_roots=scope["excluded_roots"],
            required_subtitle_languages=("zh-CN",),
        )
        scoped = _validate_audit_envelope(
            audit, current["target_root"],
            expected_excluded_roots=scope["excluded_roots"],
        )
        gaps = _scan_tv_gaps(
            scoped, current,
            aliases=tmdb_tv_aliases(tmdb, current["tmdb_id"]),
        )
        return _attach_tmdb_episode_title_aliases(
            gaps, tmdb, current["tmdb_id"],
        )

    return scan_target


def _load_rapidocr() -> Any:
    try:
        from rapidocr import RapidOCR
    except ImportError as exc:  # title_closure records this video as pending
        raise RuntimeError("rapidocr_not_installed") from exc
    return RapidOCR()


def make_burned_in_ocr_adapter(
    *,
    timeout: int = 30,
    evidence_root: Path | None = None,
    engine_factory: OCREngineFactory = _load_rapidocr,
    probe_video: OCRProbe = _probe_video,
) -> Callable[[Any, str], Mapping[str, Any]]:
    """Create a lazy, per-runtime OCR adapter for pending videos only.

    RapidOCR is imported and initialised on the first actual pending video.
    Probe/import failures deliberately propagate so ``title_closure`` converts
    them into pending evidence instead of a false subtitle gap or success.
    """
    if isinstance(timeout, bool) or not isinstance(timeout, int) or not 10 <= timeout <= 120:
        raise ValueError("OCR timeout 必须在 10..120 秒")
    if evidence_root is not None and not isinstance(evidence_root, Path):
        raise ValueError("OCR evidence_root 必须是 Path 或 None")
    if evidence_root is not None and not evidence_root.is_absolute():
        raise ValueError("OCR evidence_root 必须是绝对路径")
    if not callable(engine_factory) or not callable(probe_video):
        raise ValueError("OCR engine_factory/probe_video 必须可调用")
    lock = threading.Lock()
    engine: list[Any] = []

    def adapter(alist: Any, video_path: str) -> Mapping[str, Any]:
        if (
            not isinstance(video_path, str)
            or posixpath.normpath(video_path) != video_path
            or "\\" in video_path
            or any(ord(character) < 32 for character in video_path)
            or PurePosixPath(video_path).suffix.casefold() not in _VIDEO_EXTENSIONS
            or not any(
                _inside(video_path, parent)
                for parent in TARGET_CATEGORY_PARENTS.values()
            )
        ):
            raise ValueError("OCR 视频路径无效")
        with lock:
            if not engine:
                engine.append(engine_factory())
            return probe_video(
                alist,
                engine[0],
                video_path,
                timeout=timeout,
                evidence_root=evidence_root,
            )

    return adapter
