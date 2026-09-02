"""Single-movie planning boundary.

The implementation is separated from the runtime facade so the movie planner
can be tested independently while preserving deterministic plan output.
"""

from __future__ import annotations

import posixpath
from collections import defaultdict
from pathlib import Path
from types import ModuleType
from typing import Any, Mapping, Sequence

from ..errors import PlanError
from ..models import Plan, PlannedCleanup, PlannedProblem


_RUNTIME: ModuleType | None = None
_IMPLEMENTATION_NAMES = (
    "_add_snapshot_warnings",
    "_append_cleanup_warning",
    "_burned_subtitle_cleanup_reason",
    "_collision_key",
    "_compose_filename",
    "_entry_modified_value",
    "_entry_size_value",
    "_extract_year",
    "_filter_media",
    "_lower_resolution_cleanup_reason",
    "_lower_resolution_subtitle_cleanup_reason",
    "_movie_queries_from_item",
    "normalize_exported_srt_entries",
    "_normalize_match_title",
    "_planned_cleanup_files",
    "_planned_file_from_entry",
    "_prefer_highest_resolution_videos",
    "_query_from_source",
    "_resource_gap",
    "_same_resolution_cleanup_reason",
    "_upscaled_4k_problem_reason",
    "bonus_type",
    "build_movie_plan",
    "cleanup_reason",
    "extract_episode_key",
    "is_sample",
    "join_remote",
    "make_unique_media_names",
    "normalize_remote_path",
    "resolve_existing_library_root",
    "safe_name",
    "should_ignore_extra",
    "split_remote",
    "validate_plan",
)
_VALUE_NAMES = (
    "AListClient",
    "Mapping",
    "Path",
    "Plan",
    "PlanError",
    "PlannedCleanup",
    "PlannedProblem",
    "Sequence",
    "SUBTITLE_EXTS",
    "TMDBClient",
    "VIDEO_EXTS",
    "defaultdict",
)


def _make_runtime_dispatch(name: str):
    def dispatch(*args: Any, **kwargs: Any) -> Any:
        runtime = _RUNTIME
        if runtime is None:
            raise RuntimeError("Movie planner has not been bound")
        return getattr(runtime, name)(*args, **kwargs)

    dispatch.__name__ = name
    dispatch.__qualname__ = name
    return dispatch


for _name in _IMPLEMENTATION_NAMES:
    globals()[_name] = _make_runtime_dispatch(_name)


__all__ = ["build_movie_plan", "bind_compat_runtime"]


def bind_compat_runtime(runtime: ModuleType) -> None:
    """Route planner collaborators through the runtime facade."""
    global _RUNTIME
    _RUNTIME = runtime
    for name in _VALUE_NAMES:
        if hasattr(runtime, name):
            globals()[name] = getattr(runtime, name)
    for name in _IMPLEMENTATION_NAMES:
        implementation = globals()[name]
        if getattr(implementation, "__movie_runtime_dispatch__", False):
            continue

        def dispatch(
            *args: Any,
            _name=name,
            _implementation=implementation,
            **kwargs: Any,
        ) -> Any:
            current = getattr(_RUNTIME, _name, _implementation)
            if current is not dispatch:
                return current(*args, **kwargs)
            return _implementation(*args, **kwargs)

        dispatch.__name__ = name
        dispatch.__qualname__ = name
        dispatch.__doc__ = implementation.__doc__
        dispatch.__movie_runtime_dispatch__ = True
        globals()[name] = dispatch


def build_movie_plan(
    alist: AListClient,
    tmdb_client: TMDBClient,
    *,
    src_path: str,
    parent_path: str,
    tmdb_id: int,
    ignore_orphan_temp: bool = False,
    source_files: Sequence[Mapping[str, Any]] | None = None,
    defer_validation: bool = False,
) -> Plan:
    movie = tmdb_client.get(f"/movie/{tmdb_id}")
    title = safe_name(str(movie.get("title") or movie.get("original_title") or tmdb_id))
    year = _extract_year(movie.get("release_date"))
    movie_label = safe_name(f"{title} ({year})")
    desired_movie_dir = join_remote(parent_path, movie_label)
    movie_dir, library_identity_state = resolve_existing_library_root(
        alist,
        parent_path=parent_path,
        desired_root=desired_movie_dir,
        tmdb_id=tmdb_id,
        tv=False,
    )
    scanned_entries = (
        [dict(item) for item in source_files]
        if source_files is not None
        else alist.walk(
            src_path,
            ignore_orphan_temp=ignore_orphan_temp,
            include_bonus=True,
            include_title_extras=True,
        )
    )
    scanned_entries, exported_srt_issues = normalize_exported_srt_entries(
        alist,
        scanned_entries,
        original_language=movie.get("original_language"),
    )
    scanned_files = [
        item
        for item in _filter_media(scanned_entries)
        if not should_ignore_extra(str(item.get("name", "")))
    ]
    if not any(
        Path(str(item.get("name", ""))).suffix.lower() in VIDEO_EXTS
        for item in scanned_files
    ):
        # ``extra`` is normally a release-extra marker, but it can also be an
        # official movie-title word (for example ``未来福音 extra chorus``).
        # In explicit movie mode, recover a filtered primary video only when
        # its cleaned release title agrees with the source directory title.
        # This does not turn a generic Extras folder into a movie.
        source_title_key = _normalize_match_title(_query_from_source(src_path))
        title_matched_videos = [
            item
            for item in scanned_entries
            if Path(str(item.get("name", ""))).suffix.lower() in VIDEO_EXTS
            and cleanup_reason(str(item.get("name", ""))) is None
            and not is_sample(str(item.get("name", "")))
            and any(
                len(query_key) >= 5
                and (
                    query_key in source_title_key
                    or source_title_key in query_key
                )
                for query_key in (
                    _normalize_match_title(query)
                    for query in _movie_queries_from_item(item)
                )
            )
        ]
        if title_matched_videos:
            scanned_files = [
                item
                for item in scanned_entries
                if item in title_matched_videos
                or Path(str(item.get("name", ""))).suffix.lower()
                in SUBTITLE_EXTS
            ]
    cleanup_files = _planned_cleanup_files(scanned_entries)
    samples = [item for item in scanned_files if is_sample(str(item.get("name", "")))]
    bonus_files = [
        item
        for item in scanned_files
        if item not in samples and bonus_type(str(item.get("name", ""))) is not None
    ]
    files = [
        item for item in scanned_files if item not in samples and item not in bonus_files
    ]
    if not any(Path(str(item.get("name", ""))).suffix.lower() in VIDEO_EXTS for item in files):
        raise PlanError("未找到电影媒体文件")

    # Detect numbered movie parts before quality de-duplication.  Otherwise
    # equally encoded part01/part02/... files land in one movie bucket and the
    # smaller parts can be mistaken for inferior copies of part01.
    candidate_video_items = [
        item
        for item in files
        if Path(str(item.get("name", ""))).suffix.lower() in VIDEO_EXTS
    ]
    candidate_video_keys = [
        extract_episode_key(str(item.get("name", "")))
        for item in candidate_video_items
    ]
    candidate_part_numbers = sorted(
        {key.number for key in candidate_video_keys if key is not None}
    )
    release_query_keys = {
        _normalize_match_title(query)
        for item in candidate_video_items
        for query in _movie_queries_from_item(item)[:1]
        if query
    }
    numbered_movie_parts = (
        len(candidate_part_numbers) >= 2
        and all(
            key is not None and key.kind in {"regular", "special"}
            for key in candidate_video_keys
        )
        and len({key.kind for key in candidate_video_keys if key is not None}) == 1
        and len(release_query_keys) == 1
    )
    expected_part_numbers = (
        list(range(1, candidate_part_numbers[-1] + 1))
        if numbered_movie_parts else []
    )
    missing_part_numbers = sorted(
        set(expected_part_numbers) - set(candidate_part_numbers)
    )
    multipart_movie = numbered_movie_parts and not missing_part_numbers
    split_movie_parts = numbered_movie_parts

    # Every entry in this plan has already been confirmed as the same TMDB
    # movie.  Apply the same conservative 4K preference as TV episodes while
    # keeping named cuts/editions in separate comparison buckets. Numbered
    # movie parts are independent content identities, so compare quality only
    # inside each part rather than across the whole movie.
    if split_movie_parts:
        files_by_part: dict[int, list[Mapping[str, Any]]] = defaultdict(list)
        unkeyed_files: list[Mapping[str, Any]] = []
        for item in files:
            key = extract_episode_key(str(item.get("name", "")))
            if key is not None and key.kind in {"regular", "special"}:
                files_by_part[key.number].append(item)
            else:
                unkeyed_files.append(item)
        files = [dict(item) for item in unkeyed_files]
        lower_resolution_videos: list[dict[str, Any]] = []
        for number in candidate_part_numbers:
            kept_part, removed_part = _prefer_highest_resolution_videos(
                files_by_part[number]
            )
            files.extend(kept_part)
            lower_resolution_videos.extend(removed_part)
    else:
        files, lower_resolution_videos = _prefer_highest_resolution_videos(files)
    upscaled_4k_problems: list[PlannedProblem] = []
    for item in lower_resolution_videos:
        source_path = normalize_remote_path(str(item["full_path"]))
        source_dir, original_name = split_remote(source_path)
        preferred_source = str(item["_preferred_resolution_source"])
        cleanup_kind = str(item.get("_duplicate_cleanup_kind", "lower_resolution"))
        if cleanup_kind == "upscaled_4k_duplicate":
            # A self-labelled ``4K Ver.`` upscale lost to the native release:
            # it stays at source as an informational problem row, never as a
            # cleanup candidate with delete authority.
            upscaled_4k_problems.append(
                PlannedProblem(
                    source_path=source_path,
                    reason=_upscaled_4k_problem_reason(preferred_source),
                    stays_at_source=True,
                )
            )
            continue
        if cleanup_kind == "burned_subtitle_duplicate":
            reason = _burned_subtitle_cleanup_reason(preferred_source)
        elif cleanup_kind == "same_resolution_duplicate":
            reason = _same_resolution_cleanup_reason(preferred_source)
        elif cleanup_kind == "lower_resolution_subtitle":
            reason = _lower_resolution_subtitle_cleanup_reason(preferred_source)
        else:
            reason = _lower_resolution_cleanup_reason(preferred_source)
        cleanup_files.append(
            PlannedCleanup(
                source_path=source_path,
                source_dir=source_dir,
                original_name=original_name,
                reason=reason,
                source_size=_entry_size_value(item),
                source_modified=_entry_modified_value(item),
            )
        )

    base_name = movie_label
    files = sorted(files, key=lambda x: _collision_key(str(x["full_path"])))
    video_items = [
        item
        for item in files
        if Path(str(item.get("name", ""))).suffix.lower() in VIDEO_EXTS
    ]
    if split_movie_parts:
        names_by_path: dict[str, str] = {}
        keyed_files: dict[int, list[dict[str, Any]]] = defaultdict(list)
        for item in files:
            key = extract_episode_key(str(item.get("name", "")))
            if key is not None and key.kind in {"regular", "special"}:
                keyed_files[key.number].append(item)
        for number, part_files in keyed_files.items():
            part_names = make_unique_media_names(
                f"{base_name} - part{number}",
                part_files,
                preserve_editions=True,
            )
            for item, name in zip(part_files, part_names):
                names_by_path[str(item["full_path"])] = name
        names = [
            names_by_path.get(str(item["full_path"]))
            or make_unique_media_names(base_name, [item], preserve_editions=True)[0]
            for item in files
        ]
    else:
        names = make_unique_media_names(base_name, files, preserve_editions=True)
    planned = []
    for item, final_name in zip(files, names):
        planned_item = _planned_file_from_entry(
            item,
            final_name=final_name,
            target_dir=movie_dir,
        )
        if isinstance(planned_item.subtitle_validation, Mapping):
            proof = dict(planned_item.subtitle_validation)
            proof["target_coordinate"] = "movie"
            planned_item.subtitle_validation = proof
        planned.append(planned_item)

    bonus_counts: dict[str, int] = defaultdict(int)
    for item in sorted(bonus_files, key=lambda value: _collision_key(str(value["full_path"]))):
        kind = bonus_type(str(item["name"])) or "other"
        bonus_counts[kind] += 1
        serial = "" if bonus_counts[kind] == 1 else str(bonus_counts[kind])
        final_name = _compose_filename(
            base_name,
            f"-{kind}{serial}",
            Path(str(item["name"])).suffix.lower(),
        )
        planned.append(
            _planned_file_from_entry(item, final_name=final_name, target_dir=movie_dir)
        )

    warnings: list[str] = []
    _append_cleanup_warning(warnings, cleanup_files)
    if library_identity_state == "same_tmdb_id" and movie_dir != desired_movie_dir:
        warnings.append(
            f"现有电影 NFO 已确认相同 TMDB ID {tmdb_id}；"
            f"已保留现有目录名 {split_remote(movie_dir)[1]!r}"
        )
    elif library_identity_state == "matching_name_without_nfo":
        warnings.append(
            "目标存在同名目录但没有可验证的电影 NFO；标题/年份相符，"
            "必须人工核对后才能合并"
        )
    if ignore_orphan_temp:
        warnings.append("已显式忽略 .scraper-tmp-* 遗留条目，可能存在未恢复文件")
    if samples:
        warnings.append(
            f"已排除 {len(samples)} 个 sample/样片文件；"
            "保留原位待人工确认"
        )
    if bonus_files:
        warnings.append(f"已按 Infuse 规则整理 {len(bonus_files)} 个预告/花絮文件")
    if exported_srt_issues:
        warnings.append(
            f"{len(exported_srt_issues)} 个导出 .sc/.tc.srt.txt 字幕未通过"
            " UTF-8/SRT 内容校验，已保留原位"
        )
    if multipart_movie:
        warnings.append(
            f"检测到电影被拆为 {len(candidate_part_numbers)} 个连续分段，"
            f"已按 part1-part{candidate_part_numbers[-1]} 命名"
        )

    # Sub-series grouping (operator ruling 2026-08-30): when the layout put
    # this movie inside a directory named after its own TMDB collection, the
    # collection directory is a visible shelf item and carries the official
    # collection artwork.  The metadata is idempotent across the collection's
    # members — the writer's preserve-or-upload keeps an already-present
    # poster authoritative — so any member can supply it.
    collection = movie.get("belongs_to_collection") or {}
    collection_metadata: dict[str, Any] = {}
    if isinstance(collection, Mapping):
        from ..media_naming import collection_directory_label

        collection_label = collection_directory_label(
            str(collection.get("name") or "")
        )
        parent_label = posixpath.basename(
            str(parent_path).rstrip("/")
        )
        if collection_label and collection_label == parent_label:
            collection_metadata = {
                "collection_root": str(parent_path).rstrip("/"),
                "collection_poster_path": collection.get("poster_path"),
                "collection_backdrop_path": collection.get("backdrop_path"),
            }

    plan = Plan(
        mode="movie",
        source_root=normalize_remote_path(src_path),
        target_root=movie_dir,
        files=planned,
        warnings=warnings,
        metadata={
            "tmdb_id": tmdb_id,
            "title": title,
            "year": year,
            "poster_path": movie.get("poster_path"),
            "backdrop_path": movie.get("backdrop_path"),
            **collection_metadata,
        },
        cleanup_files=cleanup_files,
        problem_files=[
            PlannedProblem(
                source_path=str(item["full_path"]),
                reason="sample/样片文件不参与整理；保留原位待人工确认",
            )
            for item in samples
        ] + upscaled_4k_problems,
        scan_report={
            "deferred_subtitles": [
                {
                    "source_path": issue["source_path"],
                    "action": "preserve_at_source",
                    "reason": "invalid_exported_srt",
                    "detail": issue["reason"],
                }
                for issue in exported_srt_issues
            ],
            "resource_gaps": [
                _resource_gap(
                    "missing_multipart_segment",
                    f"{movie_label} - part{number}",
                    "同一电影的源分段编号不连续，该分段缺失",
                    files=[
                        str(item["full_path"])
                        for item in candidate_video_items
                    ],
                )
                for number in missing_part_numbers
            ]
        }
        if missing_part_numbers or exported_srt_issues
        else {},
    )
    _add_snapshot_warnings(plan)
    if not defer_validation:
        validate_plan(alist, plan)
    return plan
