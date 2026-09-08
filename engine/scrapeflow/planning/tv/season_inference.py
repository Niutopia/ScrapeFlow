"""TV season and release inference helpers.

The functions in this module only transform caller-owned planning projections;
they do not perform writes or invent TMDB identity.  Runtime-bound dispatchers
keep runtime overrides visible while making this high-coupling TV boundary
independently testable.
"""

from __future__ import annotations

import re
import shutil
import subprocess
import unicodedata
from collections import defaultdict
from datetime import date, datetime
from pathlib import Path
from types import ModuleType
from typing import Any, Mapping, Sequence

from ...errors import ApiError, PlanError
from ...models import EpisodeKey

# These are bound to the original runtime objects after its constants load.
VIDEO_EXTS: set[str] = set()
SUBTITLE_EXTS: set[str] = set()

_RUNTIME: ModuleType | None = None
_EXTERNAL_NAMES = (
    "_alternative_tmdb_titles",
    "_batch_subtitle_release_stem",
    "_has_movie_context",
    "_has_special_context",
    "_movie_queries_from_item",
    "_normalize_match_title",
    "_query_from_source",
    "_season_from_series_variant",
    "_season_from_source",
    "_search_item_titles",
    "_usable_release_title_query",
    "extract_episode_key",
    "parse_ep_files",
    "split_remote",
    "video_resolution_rank",
)

_IMPLEMENTATION_NAMES = (
    "_attach_unique_numbered_backup_subtitles",
    "_child_work_query_variants",
    "_detach_numbered_subgroups_from_mixed_movie_groups",
    "_extract_runtime_proven_overflow_movies",
    "_merge_broadcast_folders_into_long_tmdb_season",
    "_merge_release_seasons_into_long_tmdb_season_by_major_gaps",
    "_normalize_cumulative_season_episode_numbers",
    "_probe_remote_duration_minutes",
    "_proven_absolute_season_group_endpoint",
    "_proven_missing_root_season_files",
    "_proven_root_first_broadcast_block_files",
    "_remap_complete_reset_absolute_season_groups",
    "_runtime_matched_related_animation_movie",
    "_season_parent_identity_queries",
    "_unique_backup_subtitle_release_owners",
    "_tmdb_long_season_block_counts",
)
# Class names used only in annotations.  They are bound as direct value
# references (the movie planner's pattern), never as call dispatchers.
_VALUE_NAMES = (
    "AListClient",
    "TMDBClient",
)


def _make_runtime_dispatch(name: str):
    def dispatch(*args: Any, **kwargs: Any) -> Any:
        if _RUNTIME is None:
            raise RuntimeError("TV inference runtime has not been bound")
        return getattr(_RUNTIME, name)(*args, **kwargs)

    dispatch.__name__ = name
    dispatch.__qualname__ = name
    return dispatch


for _name in _EXTERNAL_NAMES:
    globals()[_name] = _make_runtime_dispatch(_name)

def _normalize_cumulative_season_episode_numbers(
    season_number: int,
    source_files: Sequence[Mapping[str, Any]],
    official_seasons: Sequence[Mapping[str, Any]],
) -> tuple[list[dict[str, Any]], str | None]:
    """Convert a proven whole-series counter into season-relative numbers.

    Some releases keep counting regular episodes across seasons (25–48 for
    season 2, 49–72 for season 3).  This conversion is intentionally narrow:
    every earlier official season must have a known episode count and the
    source video sequence must start exactly at the resulting season boundary.
    Decimal labels are evidence-bearing source identifiers and are never
    changed here; they still go through the regular-season/S00 online search.
    """
    if season_number <= 1:
        return [dict(item) for item in source_files], None

    counts = {
        int(item["season_number"]): int(item["episode_count"])
        for item in official_seasons
        if isinstance(item.get("season_number"), int)
        and not isinstance(item.get("season_number"), bool)
        and isinstance(item.get("episode_count"), int)
        and not isinstance(item.get("episode_count"), bool)
        and int(item["season_number"]) > 0
        and int(item["episode_count"]) > 0
    }
    required_seasons = range(1, season_number + 1)
    if any(number not in counts for number in required_seasons):
        return [dict(item) for item in source_files], None

    prior_count = sum(counts[number] for number in range(1, season_number))
    current_count = counts[season_number]
    regular_video_keys: list[int] = []
    video_keys_by_resolution: dict[int, set[int]] = defaultdict(set)
    for item in source_files:
        if Path(str(item.get("name", ""))).suffix.lower() not in VIDEO_EXTS:
            continue
        key = extract_episode_key(str(item.get("name", "")))
        if key is None or key.kind != "regular":
            continue
        if key.end_number or item.get("_episode_key_override") is not None:
            return [dict(value) for value in source_files], None
        regular_video_keys.append(key.number)
        video_keys_by_resolution[video_resolution_rank(item)].add(key.number)

    unique_keys = sorted(set(regular_video_keys))
    simple_cumulative = (
        bool(unique_keys)
        and unique_keys[0] == prior_count + 1
        and unique_keys == list(range(unique_keys[0], unique_keys[-1] + 1))
        and unique_keys[-1] <= prior_count + current_count
    )
    # Some releases split a two-cour sequel into separate TMDB seasons while
    # keeping a sequel-local counter (S03=01–12, S04=13–24).  An explicit
    # season group containing exactly the official count as one consecutive
    # run proves the local offset without guessing from a partial batch.
    local_run_offset = (
        unique_keys[0] - 1
        if (
            not simple_cumulative
            and len(unique_keys) == current_count
            and unique_keys[0] > 1
            and unique_keys
            == list(range(unique_keys[0], unique_keys[0] + current_count))
        )
        else 0
    )

    # A library can contain both release-numbering conventions for the same
    # season: a preferred 2160p set numbered cumulatively across the whole
    # show (25–48) and one or more 1080p backups numbered either 25–48 or
    # 01–24.  Treating their union as E01–E48 makes the latter half look
    # unmapped.  Accept the mixed form only when the two legal ranges do not
    # overlap and the highest-resolution cumulative run starts exactly at the
    # official TMDB season boundary.  This keeps the inference mechanical and
    # lets the normal duplicate-quality pass remove every lower-resolution
    # counterpart after both conventions receive the same season-relative key.
    mixed_numbering = False
    if not simple_cumulative and prior_count >= current_count and video_keys_by_resolution:
        best_resolution = max(video_keys_by_resolution)
        best_keys = video_keys_by_resolution[best_resolution]
        best_cumulative = sorted(
            key
            for key in best_keys
            if prior_count < key <= prior_count + current_count
        )
        relative_keys = sorted(key for key in unique_keys if 1 <= key <= current_count)
        cumulative_keys = sorted(
            key
            for key in unique_keys
            if prior_count < key <= prior_count + current_count
        )
        legal_keys = set(relative_keys) | set(cumulative_keys)
        mixed_numbering = (
            bool(relative_keys)
            and bool(best_cumulative)
            and best_cumulative[0] == prior_count + 1
            and best_cumulative
            == list(range(best_cumulative[0], best_cumulative[-1] + 1))
            and relative_keys == list(range(1, relative_keys[-1] + 1))
            and cumulative_keys
            == list(range(prior_count + 1, cumulative_keys[-1] + 1))
            and set(unique_keys) == legal_keys
        )

    # An absolute-numbered partial run that starts mid-window is the same
    # whole-series convention seen from a source that only carries the
    # season's tail (``[79]..[83]`` for Season 4 of a 24+24+24 show whose
    # first six episodes already live in the library): every key sits inside
    # the season's absolute window and none uses the relative convention, so
    # the offset is mechanical.  A non-consecutive or out-of-window run
    # keeps the fail-closed verdict.
    in_window_partial = (
        not simple_cumulative
        and bool(unique_keys)
        and unique_keys[0] > prior_count
        and unique_keys == list(range(unique_keys[0], unique_keys[-1] + 1))
        and unique_keys[-1] <= prior_count + current_count
    )
    if (
        not simple_cumulative
        and not mixed_numbering
        and not local_run_offset
        and not in_window_partial
    ):
        return [dict(item) for item in source_files], None
    if not local_run_offset and (
        not unique_keys
        or max(key for key in unique_keys if key > prior_count)
        > prior_count + current_count
    ):
        return [dict(item) for item in source_files], None

    normalized: list[dict[str, Any]] = []
    for raw_item in source_files:
        item = dict(raw_item)
        key = extract_episode_key(str(item.get("name", "")))
        if (
            key is not None
            and key.kind == "regular"
            and not key.end_number
        ):
            if local_run_offset and key.number in unique_keys:
                item["_episode_kind_override"] = "regular"
                item["_episode_key_override"] = key.number - local_run_offset
            elif prior_count < key.number <= prior_count + current_count:
                item["_episode_kind_override"] = "regular"
                item["_episode_key_override"] = key.number - prior_count
            elif mixed_numbering and 1 <= key.number <= current_count:
                item["_episode_kind_override"] = "regular"
                item["_episode_key_override"] = key.number
        normalized.append(item)

    cumulative_keys = (
        unique_keys
        if local_run_offset
        else [
            key for key in unique_keys
            if prior_count < key <= prior_count + current_count
        ]
    )
    mapped_end = max(cumulative_keys) - (
        local_run_offset if local_run_offset else prior_count
    )
    warning = (
        f"检测到第 {season_number} 季使用"
        + ("续作内累计编号 " if local_run_offset else "全剧累计编号 ")
        + f"{min(cumulative_keys)}–{max(cumulative_keys)}；已依据 "
        + (
            f"TMDB 第 {season_number} 季完整 {current_count} 集边界"
            if local_run_offset
            else f"TMDB 前序季度的 {prior_count} 集边界"
        )
        + "换算为 "
        f"S{season_number:02d}E01–S{season_number:02d}E{mapped_end:02d}。"
        + (
            "同时识别到季度内从 01 重新编号的低清晰度备份，"
            "已合并为同集版本并按清晰度去重。"
            if mixed_numbering
            else ""
        )
        +
        "小数集号未参与换算，仍需分别检索常规季与特别篇后确认"
    )
    return normalized, warning


def _tmdb_long_season_block_counts(
    episodes: Sequence[Mapping[str, Any]],
    *,
    minimum_gap_days: int = 90,
) -> list[int]:
    """Split one TMDB season into provable broadcast blocks.

    Some TMDB records keep every broadcast season in one continuously numbered
    season.  A quarterly-or-longer air-date gap is usable evidence for reset
    points used by release folders.  The caller still requires the source
    folders to cover those exact blocks, so a pause alone cannot force a split.
    Invalid, incomplete or non-contiguous metadata deliberately returns no
    blocks so the caller keeps the files in place instead of guessing.
    """
    rows: list[tuple[int, datetime]] = []
    for item in episodes:
        number = item.get("episode_number")
        air_date = item.get("air_date")
        if (
            not isinstance(number, int)
            or isinstance(number, bool)
            or number <= 0
            or not isinstance(air_date, str)
            or not air_date
        ):
            return []
        try:
            parsed_date = datetime.strptime(air_date, "%Y-%m-%d")
        except ValueError:
            return []
        rows.append((number, parsed_date))
    rows.sort(key=lambda row: row[0])
    if (
        len(rows) < 2
        or [number for number, _date in rows] != list(range(1, len(rows) + 1))
    ):
        return []
    block_counts: list[int] = []
    block_start = 0
    for index in range(1, len(rows)):
        if (rows[index][1] - rows[index - 1][1]).days >= minimum_gap_days:
            block_counts.append(index - block_start)
            block_start = index
    block_counts.append(len(rows) - block_start)
    return block_counts if len(block_counts) >= 2 else []


def _merge_broadcast_folders_into_long_tmdb_season(
    season_groups: dict[int, list[dict[str, Any]]],
    *,
    official_season: int,
    block_counts: Sequence[int],
) -> list[str]:
    """Merge reset/cumulative broadcast folders into one TMDB long season.

    The official-season group may already contain a whole-series absolute
    release (for example E01-E24).  Later source folders can then be either
    local E01-EN or cumulative E(prior+1)-E(prior+N).  Merge a folder only when
    it exactly covers one air-date block and the preceding absolute range is
    already complete.
    """
    base_items = season_groups.get(official_season)
    if not base_items or not block_counts:
        return []

    def video_numbers(items: Sequence[Mapping[str, Any]]) -> set[int]:
        return {
            int(item.get("_episode_key_override", key.number))
            for item in items
            if Path(str(item.get("name", ""))).suffix.lower() in VIDEO_EXTS
            and (key := extract_episode_key(str(item.get("name", "")))) is not None
            and key.kind == "regular"
            and not key.end_number
        }

    absolute_numbers = video_numbers(base_items)
    # The ordinary packer below already handles one local folder per block,
    # including a partially aired latest block.  This helper is specifically
    # for the mixed layout where the official group contains an absolute
    # whole-series release extending beyond the first broadcast block.
    if not absolute_numbers or max(absolute_numbers) <= int(block_counts[0]):
        return []
    warnings: list[str] = []
    for source_season in sorted(set(season_groups) - {official_season}):
        block_index = source_season - 1
        if block_index <= 0 or block_index >= len(block_counts):
            continue
        block_count = int(block_counts[block_index])
        prior_count = sum(int(value) for value in block_counts[:block_index])
        if not set(range(1, prior_count + 1)).issubset(absolute_numbers):
            continue
        members = season_groups[source_season]
        raw_numbers = {
            int(item.get("_episode_key_override", key.number))
            for item in members
            if Path(str(item.get("name", ""))).suffix.lower() in VIDEO_EXTS
            and (key := extract_episode_key(str(item.get("name", "")))) is not None
            and key.kind == "regular"
            and not key.end_number
        }
        local_range = set(range(1, block_count + 1))
        cumulative_range = set(range(prior_count + 1, prior_count + block_count + 1))
        if raw_numbers == cumulative_range:
            numbering = "累计"
            route = {number: number for number in cumulative_range}
        elif raw_numbers == local_range:
            numbering = "本季重置"
            route = {number: prior_count + number for number in local_range}
        else:
            continue
        for item in members:
            key = extract_episode_key(str(item.get("name", "")))
            if key is None or key.kind != "regular" or key.end_number:
                continue
            mapped = route.get(int(item.get("_episode_key_override", key.number)))
            if mapped is not None:
                item["_episode_kind_override"] = "regular"
                item["_episode_key_override"] = mapped
        base_items.extend(members)
        season_groups.pop(source_season)
        absolute_numbers.update(cumulative_range)
        warnings.append(
            f"源第 {source_season} 季完整使用{numbering}编号；根据 TMDB 播出断档块 "
            f"{block_count} 集映射为长季 E{prior_count + 1:02d}–"
            f"E{prior_count + block_count:02d}"
        )
    return warnings


def _merge_release_seasons_into_long_tmdb_season_by_major_gaps(
    season_groups: dict[int, list[dict[str, Any]]],
    *,
    official_season: int,
    official_episodes: Sequence[Mapping[str, Any]],
    today: date | None = None,
    minimum_gap_days: int = 180,
) -> list[str]:
    """Map explicit release seasons into one TMDB long season safely.

    A long TMDB season can contain several real TV seasons and also split
    cours.  The ordinary 90-day block detector intentionally sees both.  This
    fallback uses only major gaps (six months), ignores unaired future rows,
    and requires each source season to cover the exact corresponding local
    range.  It therefore maps Re:Zero S3/S4 without treating a mid-season cour
    break as a new season or importing TMDB's future E78-E85 rows.
    """
    cutoff = today or datetime.now().date()
    rows: list[tuple[int, date]] = []
    for item in official_episodes:
        number = item.get("episode_number")
        raw_date = item.get("air_date")
        if (
            not isinstance(number, int)
            or isinstance(number, bool)
            or not re.fullmatch(r"\d{4}-\d{2}-\d{2}", str(raw_date or ""))
        ):
            continue
        parsed = datetime.fromisoformat(str(raw_date)).date()
        if parsed <= cutoff:
            rows.append((number, parsed))
    rows.sort()
    if not rows or [number for number, _ in rows] != list(range(1, len(rows) + 1)):
        return []
    segments: list[list[int]] = [[]]
    for index, (number, air_date) in enumerate(rows):
        if index and (air_date - rows[index - 1][1]).days >= minimum_gap_days:
            segments.append([])
        segments[-1].append(number)
    if len(segments) < 2:
        return []
    base = season_groups.get(official_season)
    if base is None:
        return []
    source_seasons = sorted(set(season_groups) - {official_season})
    latest_source_season = max(season_groups)
    if source_seasons != list(range(official_season + 1, latest_source_season + 1)):
        return []
    # Validate the whole release layout before mutating any group.  This keeps
    # the fallback atomic and deliberately rejects mixed local+cumulative
    # backup numbering, which is handled by the ordinary edition packer.
    routes: dict[int, tuple[dict[int, int], list[int], bool]] = {}
    for source_season in source_seasons:
        if source_season > len(segments):
            return []
        segment = segments[source_season - 1]
        raw_numbers = {
            key.number
            for item in season_groups[source_season]
            if Path(str(item.get("name", ""))).suffix.lower() in VIDEO_EXTS
            and (key := extract_episode_key(str(item.get("name", "")))) is not None
            and key.kind == "regular"
            and not key.end_number
        }
        local_range = set(range(1, len(segment) + 1))
        absolute_range = set(segment)
        has_complete_local = local_range.issubset(raw_numbers)
        local_with_cumulative_editions = (
            has_complete_local
            and raw_numbers.issubset(local_range | absolute_range)
        )
        absolute_complete = raw_numbers == absolute_range
        local_latest_prefix = (
            source_season == latest_source_season
            and raw_numbers
            and raw_numbers == set(range(1, max(raw_numbers) + 1))
            and max(raw_numbers) <= len(segment)
        )
        absolute_latest_prefix = (
            source_season == latest_source_season
            and raw_numbers
            and raw_numbers == set(segment[: len(raw_numbers)])
        )
        if not (
            local_with_cumulative_editions
            or absolute_complete
            or local_latest_prefix
            or absolute_latest_prefix
        ):
            return []
        if local_with_cumulative_editions:
            raw_to_local = {
                number: number for number in raw_numbers if number in local_range
            }
            raw_to_local.update({
                absolute: index
                for index, absolute in enumerate(segment, start=1)
                if absolute in raw_numbers and absolute not in local_range
            })
            routed_segment = segment
        elif absolute_complete or absolute_latest_prefix:
            raw_to_local = {
                absolute: index
                for index, absolute in enumerate(segment, start=1)
                if absolute in raw_numbers
            }
            routed_segment = segment[: len(raw_numbers)]
        else:
            raw_to_local = {number: number for number in raw_numbers}
            routed_segment = segment[: max(raw_numbers)]
        routes[source_season] = (
            {
                raw: segment[local - 1]
                for raw, local in raw_to_local.items()
            },
            routed_segment,
            len(routed_segment) == len(segment),
        )
    warnings: list[str] = []
    for source_season in source_seasons:
        segment = segments[source_season - 1]
        members = season_groups[source_season]
        route, routed_segment, is_complete = routes[source_season]
        for item in members:
            key = extract_episode_key(str(item.get("name", "")))
            if key is None or key.kind != "regular" or key.end_number:
                continue
            mapped = route.get(key.number)
            if mapped is not None:
                item["_episode_kind_override"] = "regular"
                item["_episode_key_override"] = mapped
        base.extend(members)
        season_groups.pop(source_season)
        warnings.append(
            f"源第 {source_season} 季完整覆盖 TMDB 长季在官方播出日期半年级"
            f"播出断档后的 "
            + (
                f"完整 {len(segment)} 个已播集"
                if is_complete
                else f"当前连续 {len(routed_segment)}/{len(segment)} 集"
            )
            + f"；已映射为 E{routed_segment[0]:02d}–E{routed_segment[-1]:02d}，"
            "未包含未播集"
        )
    return warnings


def _child_work_query_variants(
    release_queries: Sequence[str],
    *,
    parent_titles: Sequence[str],
    parent_aliases: Sequence[str],
) -> list[str]:
    """Build specific child-work queries across the parent's title scripts.

    Release names may romanize only the franchise token while TMDB exposes a
    child under the native-script parent title (``Gintama The Semi-Final`` vs
    ``銀魂 THE SEMI-FINAL``).  A parent alias already proven by the selected
    TMDB record may be replaced with that record's canonical title, but the
    child-specific suffix must remain.  Bare parent aliases and codec payloads
    never become child identities.
    """
    output: list[str] = []
    aliases = sorted(
        {str(value).strip() for value in parent_aliases if str(value).strip()},
        key=len,
        reverse=True,
    )
    canonical = list(dict.fromkeys(
        str(value).strip() for value in parent_titles if str(value).strip()
    ))
    for raw_query in release_queries:
        query = str(raw_query).strip()
        if _usable_release_title_query(query):
            output.append(query)
        for alias in aliases:
            match = re.match(
                rf"^{re.escape(alias)}(?=$|[\W_])",
                query,
                flags=re.IGNORECASE,
            )
            if match is None:
                continue
            suffix = query[match.end():].strip(" \t~～:：_./-[]()（）")
            if len(_normalize_match_title(suffix)) < 4:
                continue
            for title in canonical:
                rewritten = f"{title} {suffix}".strip()
                if _usable_release_title_query(rewritten):
                    output.append(rewritten)
            break
    return list(dict.fromkeys(output))


def _detach_numbered_subgroups_from_mixed_movie_groups(
    movie_groups: dict[int, list[dict[str, Any]]],
) -> list[dict[str, Any]]:
    """Return numbered sibling works that a movie folder must not absorb.

    A release folder named for one movie can also contain a separately
    catalogued two-part TV special (for example ``The Final`` beside
    ``The Semi-Final [01]/[02]``).  If the same provisional movie identity
    contains multiple distinct release-title groups, detach only a complete
    contiguous 01..N subgroup with at least two videos.  The normal
    independent special-work matcher then has to prove its own TMDB identity;
    if it cannot, the files remain in place.  This structural check prevents
    the movie quality pass from deleting the smaller sibling work as a
    supposed duplicate without guessing where it belongs.
    """
    detached: list[dict[str, Any]] = []
    for tmdb_id, members in list(movie_groups.items()):
        video_groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for item in members:
            if Path(str(item.get("name", ""))).suffix.lower() not in VIDEO_EXTS:
                continue
            queries = _movie_queries_from_item(item)
            if not queries:
                continue
            video_groups[_normalize_match_title(queries[0])].append(item)
        if len(video_groups) < 2:
            continue
        detach_paths: set[str] = set()
        detach_stems: set[str] = set()
        for videos in video_groups.values():
            numbers = sorted({
                key.number
                for item in videos
                if (key := extract_episode_key(str(item.get("name", "")))) is not None
                and key.kind == "regular"
                and not key.end_number
            })
            if (
                len(videos) < 2
                or len(numbers) != len(videos)
                or numbers != list(range(1, len(numbers) + 1))
            ):
                continue
            for item in videos:
                detach_paths.add(str(item.get("full_path", "")))
                detach_stems.add(_batch_subtitle_release_stem(str(item.get("name", ""))))
        if not detach_paths:
            continue
        retained: list[dict[str, Any]] = []
        for item in members:
            path = str(item.get("full_path", ""))
            suffix = Path(str(item.get("name", ""))).suffix.lower()
            follows_detached_video = (
                suffix in SUBTITLE_EXTS
                and _batch_subtitle_release_stem(str(item.get("name", "")))
                in detach_stems
            )
            if path in detach_paths or follows_detached_video:
                detached.append(item)
            else:
                retained.append(item)
        movie_groups[tmdb_id] = retained
    return detached


def _proven_missing_root_season_files(
    source_root: str,
    unknown_media: Sequence[Mapping[str, Any]],
    season_groups: Mapping[int, Sequence[Mapping[str, Any]]],
    official_season_counts: Mapping[int, int],
) -> tuple[int | None, list[dict[str, Any]], list[dict[str, Any]]]:
    """Assign a complete root-level run only to the sole missing official season."""
    missing = sorted(set(official_season_counts) - set(season_groups))
    copied = [dict(item) for item in unknown_media]
    if len(missing) != 1:
        return None, [], copied
    season_number = missing[0]
    expected_count = int(official_season_counts[season_number])
    root_videos: list[dict[str, Any]] = []
    for item in copied:
        path = str(item.get("full_path", ""))
        parent, _ = split_remote(path)
        key = extract_episode_key(str(item.get("name", "")))
        if (
            parent == source_root.rstrip("/")
            and Path(str(item.get("name", ""))).suffix.lower() in VIDEO_EXTS
            and not _has_special_context(item)
            and not _has_movie_context(item)
            and key is not None
            and key.kind == "regular"
            and not key.end_number
        ):
            root_videos.append(item)
    video_numbers = {
        extract_episode_key(str(item.get("name", ""))).number
        for item in root_videos
    }
    if video_numbers != set(range(1, expected_count + 1)):
        return None, [], copied
    attached: list[dict[str, Any]] = []
    remaining: list[dict[str, Any]] = []
    for item in copied:
        path = str(item.get("full_path", ""))
        relative = path[len(source_root.rstrip("/")) :].lstrip("/")
        parts = relative.split("/")
        key = extract_episode_key(str(item.get("name", "")))
        root_or_backup = len(parts) == 1 or parts[0] in {
            "备份字幕", "字幕", "Subtitles",
        }
        if (
            root_or_backup
            and key is not None
            and key.kind == "regular"
            and not key.end_number
            and 1 <= key.number <= expected_count
            and not _has_special_context(item)
            and not _has_movie_context(item)
        ):
            attached.append(item)
        else:
            remaining.append(item)
    return season_number, attached, remaining


def _season_parent_identity_queries(
    items: Sequence[Mapping[str, Any]],
    *,
    source_root: str,
    season_number: int,
    show: Mapping[str, Any],
) -> list[str]:
    """Use the season-bearing directory, never an individual episode, as work identity."""
    queries: list[str] = []
    for item in items[:3]:
        full_path = str(item.get("full_path", ""))
        relative = full_path[len(source_root.rstrip("/")) :].lstrip("/")
        for segment in reversed(relative.split("/")[:-1]):
            explicit = _season_from_source("/" + segment)
            variant = _season_from_series_variant(segment, show)
            if explicit == season_number or variant == season_number:
                query = _query_from_source("/" + segment)
                generic_season_label = bool(re.fullmatch(
                    r"(?:第\s*[一二三四五六七八九十\d]{1,3}\s*季|"
                    r"s(?:eason)?\s*0*\d{1,3})",
                    unicodedata.normalize("NFKC", query).strip(),
                    flags=re.I,
                ))
                if generic_season_label:
                    queries.extend(
                        candidate
                        for candidate in _movie_queries_from_item(item)
                        if _usable_release_title_query(candidate)
                    )
                elif _usable_release_title_query(query):
                    queries.append(query)
                break
    return list(dict.fromkeys(queries))


def _proven_root_first_broadcast_block_files(
    source_root: str,
    unknown_media: Sequence[Mapping[str, Any]],
    block_count: int,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Return a complete root-level alternate of the first proven broadcast block."""
    copied = [dict(item) for item in unknown_media]
    root_videos = [
        item for item in copied
        if split_remote(str(item.get("full_path", "")))[0] == source_root.rstrip("/")
        and Path(str(item.get("name", ""))).suffix.lower() in VIDEO_EXTS
        and not _has_special_context(item)
        and not _has_movie_context(item)
        and (key := extract_episode_key(str(item.get("name", "")))) is not None
        and key.kind == "regular"
        and not key.end_number
    ]
    numbers = {
        extract_episode_key(str(item.get("name", ""))).number
        for item in root_videos
    }
    if numbers != set(range(1, block_count + 1)):
        return [], copied
    attached: list[dict[str, Any]] = []
    remaining: list[dict[str, Any]] = []
    for item in copied:
        relative = str(item.get("full_path", ""))[
            len(source_root.rstrip("/")):
        ].lstrip("/")
        parts = relative.split("/")
        key = extract_episode_key(str(item.get("name", "")))
        if (
            (len(parts) == 1 or parts[0] in {"备份字幕", "字幕", "Subtitles"})
            and key is not None
            and key.kind == "regular"
            and not key.end_number
            and 1 <= key.number <= block_count
            and not _has_special_context(item)
            and not _has_movie_context(item)
        ):
            attached.append(item)
        else:
            remaining.append(item)
    return attached, remaining


def _probe_remote_duration_minutes(
    alist: AListClient,
    source_path: str,
) -> float | None:
    """Read only container metadata for an otherwise ambiguous remote video.

    The signed URL has already passed ``AListClient``'s SSRF validation.  The
    probe is deliberately optional: installations without ffprobe retain the
    existing diagnostic row instead of weakening identity checks.
    """
    ffprobe = shutil.which("ffprobe")
    file_link = getattr(alist, "file_link", None)
    if ffprobe is None or not callable(file_link):
        return None
    try:
        raw_url, headers = file_link(source_path, refresh=True)
    except (ApiError, OSError, ValueError):
        return None
    command = [ffprobe, "-v", "error"]
    safe_headers: list[str] = []
    for raw_name, raw_value in headers.items():
        name = str(raw_name).strip()
        value = str(raw_value).strip()
        if (
            not re.fullmatch(r"[A-Za-z0-9-]+", name)
            or "\r" in value
            or "\n" in value
        ):
            return None
        safe_headers.append(f"{name}: {value}\r\n")
    if safe_headers:
        command.extend(["-headers", "".join(safe_headers)])
    command.extend([
        "-show_entries", "format=duration",
        "-of", "default=noprint_wrappers=1:nokey=1",
        raw_url,
    ])
    try:
        completed = subprocess.run(
            command,
            check=False,
            capture_output=True,
            text=True,
            timeout=25,
        )
        if completed.returncode != 0:
            return None
        seconds = float(completed.stdout.strip().splitlines()[0])
    except (OSError, subprocess.TimeoutExpired, ValueError, IndexError):
        return None
    if seconds <= 0 or seconds > 8 * 60 * 60:
        return None
    return seconds / 60.0


def _runtime_matched_related_animation_movie(
    tmdb_client: TMDBClient,
    show: Mapping[str, Any],
    duration_minutes: float,
    official_special_runtimes: Mapping[int, int],
) -> int | None:
    """Return one related animated movie only from unique runtime evidence."""
    tolerance = max(1.5, min(4.0, duration_minutes * 0.08))
    if any(
        abs(float(runtime) - duration_minutes) <= tolerance
        for runtime in official_special_runtimes.values()
    ):
        return None
    show_titles = [
        str(show.get(field) or "").strip()
        for field in ("name", "original_name")
        if str(show.get(field) or "").strip()
    ]
    show_keys = {_normalize_match_title(title) for title in show_titles}
    show_keys.discard("")
    if not show_keys:
        return None
    matches: set[int] = set()
    seen_results: set[int] = set()
    for query in show_titles:
        try:
            payload = tmdb_client.get("/search/movie", query=query)
        except ApiError:
            continue
        for result in payload.get("results") or []:
            if (
                not isinstance(result, Mapping)
                or isinstance(result.get("id"), bool)
                or not isinstance(result.get("id"), int)
            ):
                continue
            movie_id = int(result["id"])
            if movie_id in seen_results:
                continue
            seen_results.add(movie_id)
            try:
                movie = tmdb_client.get(f"/movie/{movie_id}")
            except ApiError:
                continue
            runtime = movie.get("runtime")
            if (
                isinstance(runtime, bool)
                or not isinstance(runtime, int)
                or runtime <= 0
                or abs(float(runtime) - duration_minutes) > tolerance
            ):
                continue
            genre_ids = {
                int(genre["id"])
                for genre in (movie.get("genres") or [])
                if isinstance(genre, Mapping)
                and isinstance(genre.get("id"), int)
                and not isinstance(genre.get("id"), bool)
            }
            if 16 not in genre_ids:
                continue
            candidate_titles = [
                *_search_item_titles(movie, "movie"),
                *_alternative_tmdb_titles(tmdb_client, "movie", movie_id),
            ]
            candidate_keys = {
                _normalize_match_title(title) for title in candidate_titles
            }
            if not any(
                len(show_key) >= 4
                and (
                    show_key in candidate_key
                    or candidate_key in show_key
                )
                for show_key in show_keys
                for candidate_key in candidate_keys
                if candidate_key
            ):
                continue
            matches.add(movie_id)
    return next(iter(matches)) if len(matches) == 1 else None


def _extract_runtime_proven_overflow_movies(
    alist: AListClient,
    tmdb_client: TMDBClient,
    show: Mapping[str, Any],
    season_groups: Mapping[int, list[dict[str, Any]]],
    official_season_counts: Mapping[int, int],
    official_special_runtimes: Mapping[int, int],
    unknown_media: list[dict[str, Any]] | None = None,
) -> tuple[dict[int, list[dict[str, Any]]], list[str]]:
    """Remove uniquely identified standalone movies from TV season groups."""
    resolved: dict[int, list[dict[str, Any]]] = defaultdict(list)
    warnings: list[str] = []
    probe_candidates: list[
        tuple[int, list[dict[str, Any]], int, dict[int, list[dict[str, Any]]]]
    ] = []
    for season_number, members in season_groups.items():
        official_count = official_season_counts.get(season_number)
        if not official_count:
            continue
        overflow_videos: dict[int, list[dict[str, Any]]] = defaultdict(list)
        for item in members:
            key = extract_episode_key(str(item.get("name", "")))
            if (
                Path(str(item.get("name", ""))).suffix.lower() in VIDEO_EXTS
                and key is not None
                and key.kind == "regular"
                and not key.end_number
                and official_count < key.number <= official_count + 3
            ):
                overflow_videos[key.number].append(item)
        # Runtime probing is an expensive last-resort identity check.  Limit
        # it to the common disc layout where a complete season has exactly one
        # trailing file (E{N+1}).  Multiple overflow ordinals are a release
        # run/cumulative-numbering problem and must stay in the normal strict
        # mappers without opening several remote media streams.
        if sorted(overflow_videos) != [official_count + 1]:
            continue
        probe_candidates.append(
            (season_number, members, official_count, dict(overflow_videos))
        )
    # Large franchise roots may contain several seasons each followed by a
    # disc extra.  That is not the isolated one-file ambiguity this expensive
    # fallback is intended to solve, so do not probe any of them here.
    if len(probe_candidates) != 1:
        return {}, []
    for season_number, members, official_count, overflow_videos in probe_candidates:
        moved_paths: set[str] = set()
        for source_number, videos in sorted(overflow_videos.items()):
            durations = [
                duration
                for video in videos
                if (duration := _probe_remote_duration_minutes(
                    alist, str(video.get("full_path", ""))
                )) is not None
            ]
            if not durations:
                continue
            movie_ids = {
                movie_id
                for duration in durations
                if (movie_id := _runtime_matched_related_animation_movie(
                    tmdb_client,
                    show,
                    duration,
                    official_special_runtimes,
                )) is not None
            }
            if len(movie_ids) != 1:
                continue
            movie_id = next(iter(movie_ids))
            video_parents = {
                split_remote(str(video.get("full_path", "")))[0]
                for video in videos
            }
            video_release_keys = {
                _normalize_match_title(query)
                for video in videos
                for query in _movie_queries_from_item(video)
                if _usable_release_title_query(query)
            }
            companions = []
            for item in members:
                key = extract_episode_key(str(item.get("name", "")))
                item_release_keys = {
                    _normalize_match_title(query)
                    for query in _movie_queries_from_item(item)
                    if _usable_release_title_query(query)
                }
                if (
                    key is not None
                    and key.kind == "regular"
                    and key.number == source_number
                    and (
                        split_remote(str(item.get("full_path", "")))[0]
                        in video_parents
                        or bool(video_release_keys & item_release_keys)
                    )
                ):
                    companions.append(item)
            resolved[movie_id].extend(companions)
            moved_paths.update(str(item.get("full_path", "")) for item in companions)
            warnings.append(
                f"E{source_number:02d} 超出第 {season_number} 季官方边界；"
                f"远程媒体时长与唯一同名动画电影 TMDB/{movie_id} 一致，"
                "且不匹配任何官方 Season 00 时长，已作为独立电影"
            )
        if moved_paths:
            members[:] = [
                item for item in members
                if str(item.get("full_path", "")) not in moved_paths
            ]
    if unknown_media is not None and resolved:
        attached_unknown_paths: set[str] = set()
        for movie_id, members in resolved.items():
            source_numbers = {
                key.number
                for member in members
                if (key := extract_episode_key(str(member.get("name", "")))) is not None
                and key.kind == "regular"
            }
            release_keys = {
                _normalize_match_title(query)
                for member in members
                for query in _movie_queries_from_item(member)
                if _usable_release_title_query(query)
            }
            for item in unknown_media:
                key = extract_episode_key(str(item.get("name", "")))
                item_keys = {
                    _normalize_match_title(query)
                    for query in _movie_queries_from_item(item)
                    if _usable_release_title_query(query)
                }
                if (
                    Path(str(item.get("name", ""))).suffix.lower() in SUBTITLE_EXTS
                    and key is not None
                    and key.kind == "regular"
                    and key.number in source_numbers
                    and bool(release_keys & item_keys)
                ):
                    resolved[movie_id].append(item)
                    attached_unknown_paths.add(str(item.get("full_path", "")))
        if attached_unknown_paths:
            unknown_media[:] = [
                item for item in unknown_media
                if str(item.get("full_path", "")) not in attached_unknown_paths
            ]
    return dict(resolved), warnings


def _attach_unique_numbered_backup_subtitles(
    unknown_media: list[dict[str, Any]],
    season_groups: Mapping[int, list[dict[str, Any]]],
    official_season_counts: Mapping[int, int],
) -> tuple[list[dict[str, Any]], int]:
    """Attach a partial backup subtitle only to one exact release/episode video."""
    attached = 0
    remaining: list[dict[str, Any]] = []
    for item in unknown_media:
        key = extract_episode_key(str(item.get("name", "")))
        if (
            Path(str(item.get("name", ""))).suffix.lower() not in SUBTITLE_EXTS
            or key is None
            or key.kind != "regular"
            or key.end_number
        ):
            remaining.append(item)
            continue
        subtitle_keys = {
            _normalize_match_title(query)
            for query in _movie_queries_from_item(item)
            if _usable_release_title_query(query)
        }
        candidates: list[int] = []
        for season_number, members in season_groups.items():
            if not 1 <= key.number <= int(official_season_counts.get(season_number, 0)):
                continue
            matching_video = False
            for video in members:
                video_key = extract_episode_key(str(video.get("name", "")))
                if (
                    Path(str(video.get("name", ""))).suffix.lower() not in VIDEO_EXTS
                    or video_key is None
                    or video_key.kind != "regular"
                    or video_key.number != key.number
                ):
                    continue
                video_keys = {
                    _normalize_match_title(query)
                    for query in _movie_queries_from_item(video)
                    if _usable_release_title_query(query)
                }
                if subtitle_keys and subtitle_keys & video_keys:
                    matching_video = True
                    break
            if matching_video:
                candidates.append(season_number)
        if len(candidates) != 1:
            remaining.append(item)
            continue
        season_groups[candidates[0]].append(item)
        attached += 1
    return remaining, attached


def _unique_backup_subtitle_release_owners(
    top_level_groups: Mapping[str, Sequence[Mapping[str, Any]]],
    unknown_media: Sequence[Mapping[str, Any]],
) -> dict[str, str]:
    """Route a flattened backup subtitle to one top-level release identity.

    Franchise sequels often share a short alias (``Clannad``) while only the
    sequel carries the longer identity (``Clannad After Story``).  The former
    greedy per-directory pass attached a sequel subtitle to whichever folder
    sorted first.  Compare every sibling release before staging: the longest
    exact normalized title key wins, and equal best scores remain unresolved.
    """
    def identity_key(value: str) -> str:
        """Normalize a release title without erasing its disambiguating year.

        ``_normalize_match_title`` intentionally removes years for TMDB title
        matching.  That is unsafe for sibling release ownership: ``Clannad
        2007`` and ``Clannad After Story 2008`` both expose the short alias
        ``Clannad``.  Here an explicit release year is identity evidence, just
        like the sequel subtitle, so preserve it while still folding Unicode,
        punctuation and case.
        """
        normalized = unicodedata.normalize("NFKC", str(value)).casefold()
        return "".join(
            char for char in normalized
            if char.isalnum() or "\u3400" <= char <= "\u9fff"
        )

    release_keys: dict[str, set[str]] = {}
    for segment, members in top_level_groups.items():
        keys = {
            identity_key(query)
            for item in members
            if Path(str(item.get("name", ""))).suffix.lower() in VIDEO_EXTS
            for query in _movie_queries_from_item(item)
            if _usable_release_title_query(query)
        }
        # The top-level folder is itself release-local evidence and often
        # carries the year even when a release parser also emits a short alias.
        keys.add(identity_key(segment))
        keys.discard("")
        if keys:
            release_keys[segment] = keys

    owners: dict[str, str] = {}
    for item in unknown_media:
        if Path(str(item.get("name", ""))).suffix.lower() not in SUBTITLE_EXTS:
            continue
        subtitle_keys = {
            identity_key(query)
            for query in _movie_queries_from_item(item)
            if _usable_release_title_query(query)
        }
        subtitle_keys.discard("")
        scores = {
            segment: max(
                (
                    min(len(subtitle_key), len(release_key))
                    for subtitle_key in subtitle_keys
                    for release_key in keys
                    if (
                        subtitle_key == release_key
                        or subtitle_key in release_key
                        or release_key in subtitle_key
                    )
                ),
                default=0,
            )
            for segment, keys in release_keys.items()
        }
        best_score = max(scores.values(), default=0)
        best = [segment for segment, score in scores.items() if score == best_score > 0]
        if len(best) == 1:
            owners[str(item.get("full_path", ""))] = best[0]
            continue
        # Two sibling folders can be resolution variants of the same child
        # work.  In that case the subtitle's full, specific release identity
        # is present verbatim in every tied group.  Attach it once to the
        # deterministic first group; the caller later merges both groups by
        # the same TMDB child id.  A short shared franchise alias does not pass
        # this exact-match check (for example bare ``Clannad`` beside
        # ``After Story``).
        shared_exact = {
            subtitle_key
            for subtitle_key in subtitle_keys
            if len(subtitle_key) >= 8
            and all(subtitle_key in release_keys[segment] for segment in best)
            and not any(
                subtitle_key != release_key and subtitle_key in release_key
                for segment in best
                for release_key in release_keys[segment]
            )
        }
        if best and shared_exact:
            owners[str(item.get("full_path", ""))] = sorted(best)[0]
    return owners


def _proven_absolute_season_group_endpoint(
    source_season: int,
    source_files: Sequence[Mapping[str, Any]],
    official_seasons: Sequence[Mapping[str, Any]],
) -> int | None:
    """Return an official cumulative endpoint proved by a source video run."""
    if source_season != 1:
        return None
    numbers: set[int] = set()
    for item in source_files:
        if Path(str(item.get("name") or "")).suffix.lower() not in VIDEO_EXTS:
            continue
        key = extract_episode_key(str(item.get("name") or ""))
        if key is None or key.kind != "regular":
            continue
        numbers.update(range(key.number, (key.end_number or key.number) + 1))
    if not numbers or numbers != set(range(1, max(numbers) + 1)):
        return None
    counts = [
        (int(item["season_number"]), int(item["episode_count"]))
        for item in official_seasons
        if isinstance(item.get("season_number"), int)
        and not isinstance(item.get("season_number"), bool)
        and int(item["season_number"]) > 0
        and isinstance(item.get("episode_count"), int)
        and not isinstance(item.get("episode_count"), bool)
        and int(item["episode_count"]) > 0
    ]
    counts.sort()
    cumulative = 0
    first_count = counts[0][1] if counts else 0
    for season_number, count in counts:
        cumulative += count
        if season_number > 1 and max(numbers) == cumulative and cumulative > first_count:
            return cumulative
    return None


def _remap_complete_reset_absolute_season_groups(
    season_groups: Mapping[int, list[dict[str, Any]]],
    official_seasons: Sequence[Mapping[str, Any]],
) -> tuple[dict[int, list[dict[str, Any]]], str | None]:
    """Split complete release-level absolute blocks on official boundaries.

    Long-running shows are sometimes packaged as a few release "seasons"
    whose counters each restart at 01, while TMDB has many broadcast seasons.
    Accept this only when every source group is a complete contiguous 01..N
    run and the ordered group endpoints partition the *entire* ordered TMDB
    season-count vector exactly.
    """
    if len(season_groups) < 2:
        return {number: list(items) for number, items in season_groups.items()}, None
    official = sorted(
        (
            int(item["season_number"]),
            int(item["episode_count"]),
        )
        for item in official_seasons
        if isinstance(item.get("season_number"), int)
        and not isinstance(item.get("season_number"), bool)
        and int(item["season_number"]) > 0
        and isinstance(item.get("episode_count"), int)
        and not isinstance(item.get("episode_count"), bool)
        and int(item["episode_count"]) > 0
    )
    if len(official) <= len(season_groups):
        return {number: list(items) for number, items in season_groups.items()}, None

    def source_key(item: Mapping[str, Any]) -> EpisodeKey | None:
        bracket_range = re.search(
            r"\[\s*0*(\d{1,4})\s*[-–—~～至到]\s*0*(\d{1,4})\s*\]",
            unicodedata.normalize("NFKC", str(item.get("name") or "")),
        )
        if bracket_range is not None:
            start, end = map(int, bracket_range.groups())
            if 0 < start <= end:
                return EpisodeKey("regular", start, end)
        key = extract_episode_key(str(item.get("name") or ""))
        if key is not None:
            return key
        try:
            parsed = parse_ep_files(
                [item],
                prefer_simplified=False,
                defer_unnumbered_specials=True,
            )
        except PlanError:
            return None
        return next(iter(parsed), None) if len(parsed) == 1 else None

    source_blocks: list[tuple[int, int, list[dict[str, Any]]]] = []
    for source_season, items in sorted(season_groups.items()):
        numbers: set[int] = set()
        for item in items:
            if Path(str(item.get("name") or "")).suffix.lower() not in VIDEO_EXTS:
                continue
            key = source_key(item)
            if key is None or key.kind != "regular":
                continue
            numbers.update(range(key.number, (key.end_number or key.number) + 1))
        if not numbers or numbers != set(range(1, max(numbers) + 1)):
            return {number: list(values) for number, values in season_groups.items()}, None
        source_blocks.append((source_season, max(numbers), items))

    partitions: list[list[tuple[int, int]]] = []
    official_index = 0
    for _source_season, endpoint, _items in source_blocks:
        block: list[tuple[int, int]] = []
        total = 0
        while official_index < len(official) and total < endpoint:
            season_number, count = official[official_index]
            block.append((season_number, count))
            total += count
            official_index += 1
        if total != endpoint:
            return {number: list(values) for number, values in season_groups.items()}, None
        partitions.append(block)
    if official_index != len(official) or not any(len(block) > 1 for block in partitions):
        return {number: list(values) for number, values in season_groups.items()}, None

    remapped: dict[int, list[dict[str, Any]]] = defaultdict(list)
    summaries: list[str] = []
    for (source_season, endpoint, items), block in zip(source_blocks, partitions):
        route: dict[int, tuple[int, int]] = {}
        offset = 0
        for target_season, count in block:
            for local in range(1, count + 1):
                route[offset + local] = (target_season, local)
            offset += count
        staged: list[tuple[int, dict[str, Any]]] = []
        for original in items:
            item = dict(original)
            key = source_key(item)
            if key is None or key.kind != "regular":
                # Named/fractional extras inherit only the uniquely proven
                # official block container. Their own strict special mapper
                # still decides Season 00 identity later.
                logical_number = key.number if key is not None else 1
                container = route.get(logical_number, (block[0][0], 1))[0]
                staged.append((container, item))
                continue
            start = route.get(key.number)
            end = route.get(key.end_number or key.number)
            if start is None or end is None or start[0] != end[0]:
                return {number: list(values) for number, values in season_groups.items()}, None
            item["_episode_kind_override"] = "regular"
            item["_episode_key_override"] = start[1]
            if end[1] != start[1]:
                item["_episode_end_override"] = end[1]
            staged.append((start[0], item))
        for target_season, item in staged:
            remapped[target_season].append(item)
        summaries.append(
            f"源第 {source_season} 组 01–{endpoint} → "
            f"S{block[0][0]:02d}–S{block[-1][0]:02d}"
        )
    return dict(remapped), (
        "源发行将长篇剧集分为重置编号的跨季 absolute 块；"
        "已仅在所有视频块完整覆盖 01–N，且与 TMDB 全部季集数"
        "边界唯一分割时自动映射：" + "；".join(summaries)
    )


__all__ = [
    "_normalize_cumulative_season_episode_numbers",
    "_tmdb_long_season_block_counts",
    "_merge_broadcast_folders_into_long_tmdb_season",
    "_merge_release_seasons_into_long_tmdb_season_by_major_gaps",
    "_child_work_query_variants",
    "_detach_numbered_subgroups_from_mixed_movie_groups",
    "_proven_missing_root_season_files",
    "_season_parent_identity_queries",
    "_proven_root_first_broadcast_block_files",
    "_probe_remote_duration_minutes",
    "_runtime_matched_related_animation_movie",
    "_extract_runtime_proven_overflow_movies",
    "_attach_unique_numbered_backup_subtitles",
    "_unique_backup_subtitle_release_owners",
    "_proven_absolute_season_group_endpoint",
    "_remap_complete_reset_absolute_season_groups",
]


def bind_compat_runtime(runtime: ModuleType) -> None:
    """Bind runtime constants and dispatch all planner helper dependencies."""
    global _RUNTIME, VIDEO_EXTS, SUBTITLE_EXTS
    _RUNTIME = runtime
    VIDEO_EXTS = runtime.VIDEO_EXTS
    SUBTITLE_EXTS = runtime.SUBTITLE_EXTS
    for name in _VALUE_NAMES:
        if hasattr(runtime, name):
            globals()[name] = getattr(runtime, name)
    for name in _IMPLEMENTATION_NAMES:
        implementation = globals()[name]
        if getattr(implementation, "__tv_runtime_dispatch__", False):
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
        dispatch.__tv_runtime_dispatch__ = True
        globals()[name] = dispatch
