"""Smart TV planning orchestration.

The high-level season/collection planner lives here so the Engine runtime can
keep transaction, validation, and runtime concerns separate. The binder below
routes each collaborator through ``engine.scraper`` so existing overrides and
deterministic plan output remain unchanged.
"""

from __future__ import annotations

import posixpath
import re
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from types import ModuleType
from typing import Any, Mapping, Sequence

from ...errors import ApiError, PlanError, ScraperError
from ...canonical_work_tree import WorkIdentity
from ...models import AutoMatch, EpisodeKey, Plan, PlannedProblem
from ...residual_policy import classify_residual, is_bonus_directory_path


_RUNTIME: ModuleType | None = None

# Every callable below resolves through the runtime module so callers can
# continue to override a planner collaborator without duplicating the logic.
_IMPLEMENTATION_NAMES = (
    "_alternative_tmdb_titles",
    "_attach_fractional_feature_by_ass_title_to_movie_groups",
    "_attach_unique_movie_subtitles",
    "_attach_unique_numbered_backup_subtitles",
    "_child_work_query_variants",
    "_collision_key",
    "_combine_plans_as_batch",
    "_cross_script_unique_match",
    "_dedupe_cleanup_files",
    "_dedupe_merged_tv_target_variants",
    "_detach_numbered_subgroups_from_mixed_movie_groups",
    "_e00_independent_movie_match",
    "_embedded_movie_tmdb_id",
    "_explicit_release_season_episode",
    "_extract_runtime_proven_overflow_movies",
    "_filter_media",
    "_has_movie_context",
    "_has_numbered_movie_collection_context",
    "_has_special_context",
    "_map_disc_extras_by_official_release_runs",
    "_map_explicit_beta_alternate",
    "_map_explicit_special_release_runs",
    "_map_release_label_editions",
    "_map_split_official_special_folder",
    "_map_unnumbered_special_from_subtitle_title",
    "_matches_named_special_release_context",
    "_media_context_from_source_and_target",
    "_merge_broadcast_folders_into_long_tmdb_season",
    "_merge_release_seasons_into_long_tmdb_season_by_major_gaps",
    "_movie_queries_from_item",
    "_normalize_cumulative_season_episode_numbers",
    "normalize_exported_srt_entries",
    "_normalize_match_title",
    "_ova_volume_ordinal",
    "_partition_movie_groups_with_video",
    "_plan_canonical_batch_tree",
    "_planned_cleanup_files",
    "_propagate_explicit_video_episode_overrides",
    "_proven_missing_root_season_files",
    "_proven_root_first_broadcast_block_files",
    "_query_from_source",
    "_raise_unparsed_media",
    "_remap_complete_reset_absolute_season_groups",
    "_remap_postseason_oav_suffix",
    "_remap_suffix_oav_on_air_versions",
    "_resolve_numbered_movie_collection_groups",
    "_search_item_titles",
    "_season_from_series_variant",
    "_season_from_source",
    "_season_parent_identity_queries",
    "_special_context_overrides_parent_season",
    "_specific_movie_query_agrees_with_match",
    "_title_similarity",
    "_tmdb_long_season_block_counts",
    "_tv_season_resource_gaps",
    "_unique_backup_subtitle_release_owners",
    "_usable_release_title_query",
    "auto_match_tmdb",
    "build_movie_plan",
    "build_tv_plan",
    "build_tv_plan_smart",
    "entry_edition_tag",
    "extract_episode_key",
    "join_remote",
    "normalize_remote_path",
    "parse_ep_files",
    "placement_for",
    "split_remote",
    "validate_plan",
)

_VALUE_NAMES = (
    "ApiError",
    "AutoMatch",
    "EpisodeKey",
    "Mapping",
    "Path",
    "Plan",
    "PlanError",
    "PlannedProblem",
    "ScraperError",
    "SUBTITLE_EXTS",
    "VIDEO_EXTS",
    "WorkIdentity",
    "datetime",
    "defaultdict",
    "posixpath",
    "re",
)

_THEME_MARKER_RE = re.compile(
    r"\[\s*(?:NCOP|NCED|OP|ED|MENU|PV|CM|TRAILER)\s*\d*(?:v\d+)?\s*\]",
    re.IGNORECASE,
)


def _preclassify_theme_residuals(
    files: Sequence[Mapping[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Remove only proven theme/menu videos before unknown-media matching.

    A regular episode coordinate always wins over a residual-looking token.
    A ``Menu`` directory is removed as a unit only when every playable member
    is independently classified as theme residual; mixed directories retain
    their normal episodes and remove only the proven residual members.

    A video inside a dedicated bonus directory (``EXTRA/``, ``PV/``,
    ``特典映像/``, ``NCOP&ED/``) is removed by that directory context alone:
    the context is strong non-story evidence independent of the file's own
    naming, and a bare bracketed ordinal there is release-local numbering
    that must never collide with the real episode run (the same vocabulary
    B/W and D's episode proofs already use).
    """
    videos = [
        dict(item) for item in files
        if Path(str(item.get("name", ""))).suffix.lower() in VIDEO_EXTS
    ]
    by_parent: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for item in videos:
        by_parent[posixpath.dirname(str(item.get("full_path", "")))].append(item)
    proven: list[dict[str, Any]] = []
    for item in videos:
        path = str(item.get("full_path", ""))
        name = str(item.get("name", ""))
        if is_bonus_directory_path(path):
            proven.append(item)
            continue
        if any(
            key.kind == "regular" and not key.end_number
            for key in [extract_episode_key(name)]
            if key is not None
        ):
            continue
        if classify_residual(path).kind != "theme_video":
            continue
        parent = posixpath.dirname(path)
        parent_name = posixpath.basename(parent)
        all_theme = all(
            classify_residual(str(member.get("full_path", ""))).kind == "theme_video"
            and not any(
                key.kind == "regular" and not key.end_number
                for key in [extract_episode_key(str(member.get("name", "")))]
                if key is not None
            )
            for member in by_parent[parent]
        )
        if _THEME_MARKER_RE.search(name) or _THEME_MARKER_RE.search(parent_name) or all_theme:
            proven.append(item)
    removed = {str(item.get("full_path", "")) for item in proven}
    residuals = [
        {
            "source_path": str(item.get("full_path", "")),
            "action": "preserve_at_source",
            "reason": "no_write_source_residual",
            "kind": "theme_menu_video",
        }
        for item in proven
    ]
    return [item for item in files if str(item.get("full_path", "")) not in removed], residuals


def _make_runtime_dispatch(name: str):
    def dispatch(*args: Any, **kwargs: Any) -> Any:
        runtime = _RUNTIME
        if runtime is None:
            raise RuntimeError("TV smart planner has not been bound")
        return getattr(runtime, name)(*args, **kwargs)

    dispatch.__name__ = name
    dispatch.__qualname__ = name
    return dispatch


for _name in _IMPLEMENTATION_NAMES:
    globals()[_name] = _make_runtime_dispatch(_name)


def _release_edition_warning_template() -> str:
    """Return the user-facing release-edition warning template (lexicon data)."""
    from ...data.release_lexicon import RELEASE_EDITION_RULES

    return RELEASE_EDITION_RULES["warning_template"]


__all__ = ["build_tv_plan_smart", "bind_compat_runtime"]


def bind_compat_runtime(runtime: ModuleType) -> None:
    """Bind planner globals to the runtime module."""
    global _RUNTIME
    _RUNTIME = runtime
    for name in _VALUE_NAMES:
        if hasattr(runtime, name):
            globals()[name] = getattr(runtime, name)
    for name in _IMPLEMENTATION_NAMES:
        implementation = globals()[name]
        if getattr(implementation, "__tv_smart_runtime_dispatch__", False):
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
        dispatch.__tv_smart_runtime_dispatch__ = True
        globals()[name] = dispatch


def _plan_with_explicit_episode_map(
    kwargs: Mapping[str, Any],
    smart_kwargs: dict[str, Any],
    files: Sequence[Mapping[str, Any]],
    *,
    preserved_theme_residuals: list[dict[str, Any]],
) -> Plan:
    """Plan one proven absolute-number release around its explicit map.

    The D proof's source-key map only covers the episode files.  A movie
    beside the release (``剧场版 代号：白/`` holding one confirmed film) has
    no map coordinate, so the lower ``build_tv_plan`` would reject it as an
    unparsed episode.  Route movie-context videos through the same movie
    matcher the smart grouping uses, then combine the TV (map-driven) plan
    with each independent movie plan exactly like the ordinary split path.
    """
    prefer_animation = _media_context_from_source_and_target(
        str(kwargs["src_path"]), str(kwargs["parent_path"]),
    )[1]
    movie_groups: dict[int, list[dict[str, Any]]] = defaultdict(list)
    remaining: list[dict[str, Any]] = []
    for item in files:
        name = str(item.get("name", ""))
        if (
            _has_movie_context(item)
            and Path(name).suffix.lower() in VIDEO_EXTS
            and extract_episode_key(name) is None
        ):
            matched = None
            for movie_query in _movie_queries_from_item(item):
                if not _usable_release_title_query(movie_query):
                    continue
                try:
                    candidate, _ = auto_match_tmdb(
                        kwargs["tmdb_client"],
                        movie_query,
                        media_type="movie",
                        min_confidence=0.88,
                        prefer_animation=prefer_animation,
                    )
                except ScraperError:
                    continue
                if candidate.status == "confirmed":
                    matched = candidate
                    break
            if matched is not None:
                movie_groups[matched.tmdb_id].append(item)
                continue
        remaining.append(dict(item))

    map_kwargs = dict(smart_kwargs)
    map_kwargs["source_files"] = remaining
    plan = build_tv_plan(**map_kwargs)
    if not movie_groups:
        if preserved_theme_residuals:
            plan.scan_report.setdefault("preserved_source_residuals", []).extend(
                item
                for item in preserved_theme_residuals
                if item
                not in plan.scan_report.get("preserved_source_residuals", [])
            )
        return plan

    executable_movie_groups, orphan_movie_files = (
        _partition_movie_groups_with_video(movie_groups)
    )
    movie_parent = split_remote(plan.target_root)[0]
    try:
        placement_for(
            str(kwargs["src_path"]),
            movie_parent,
            media_root=kwargs.get("media_root"),
        )
    except ValueError:
        movie_parent = plan.target_root
    movie_plans = [
        build_movie_plan(
            kwargs["alist"],
            kwargs["tmdb_client"],
            src_path=str(kwargs["src_path"]),
            # Independent movies nest under the TV work root as siblings of
            # its episodes, never loose beside its ``tvshow.nfo``.
            parent_path=movie_parent,
            tmdb_id=movie_tmdb_id,
            ignore_orphan_temp=bool(kwargs.get("ignore_orphan_temp")),
            source_files=movie_files,
            defer_validation=True,
        )
        for movie_tmdb_id, movie_files in sorted(executable_movie_groups.items())
    ]
    metadata = dict(plan.metadata)
    metadata["series_root"] = plan.target_root
    metadata["member_posters"] = {
        movie_plan.target_root: movie_plan.metadata["poster_path"]
        for movie_plan in movie_plans
        if isinstance(movie_plan.metadata.get("poster_path"), str)
        and movie_plan.metadata.get("poster_path")
    }
    metadata["member_movies"] = {
        movie_plan.target_root: {
            "tmdb_id": movie_plan.metadata["tmdb_id"],
            "title": movie_plan.metadata["title"],
            "year": movie_plan.metadata["year"],
        }
        for movie_plan in movie_plans
    }
    combined = Plan(
        mode="mixed",
        source_root=plan.source_root,
        target_root=normalize_remote_path(posixpath.commonpath([
            plan.target_root,
            *(movie_plan.target_root for movie_plan in movie_plans),
        ])),
        files=[*plan.files, *(item for p in movie_plans for item in p.files)],
        cleanup_files=_dedupe_cleanup_files([
            *plan.cleanup_files,
            *(item for p in movie_plans for item in p.cleanup_files),
        ]),
        problem_files=[
            *plan.problem_files,
            *(item for p in movie_plans for item in p.problem_files),
            *(
                PlannedProblem(
                    source_path=str(item.get("full_path", "")),
                    reason="已匹配到独立电影，但没有对应视频；保留原位待人工确认",
                )
                for item in orphan_movie_files
            ),
        ],
        warnings=list(dict.fromkeys([
            *plan.warnings,
            *(warning for p in movie_plans for warning in p.warnings),
            f"已识别 {len(movie_plans)} 部独立电影；放入与电视剧作品目录并列的独立电影目录",
        ])),
        metadata=metadata,
        scan_report={
            "resource_gaps": [
                dict(gap)
                for p in [plan, *movie_plans]
                for gap in (p.scan_report.get("resource_gaps") or [])
                if isinstance(gap, Mapping)
            ],
            "preserved_source_residuals": list(preserved_theme_residuals),
        },
    )
    validate_plan(
        kwargs["alist"], combined,
        media_root=kwargs.get("media_root"),
    )
    return combined


def build_tv_plan_smart(*, auto_episode_mode: bool, **kwargs: Any) -> Plan:
    """Split explicit multi-season roots and retry proven absolute-number releases."""
    proven_member_season = bool(kwargs.pop("_proven_member_season", False))
    raw_declared_seasons = kwargs.pop("source_declared_seasons", ())
    if not isinstance(raw_declared_seasons, (tuple, list, set, frozenset)):
        raise PlanError("来源声明季度格式无效")
    source_declared_seasons = {
        value
        for value in raw_declared_seasons
        if isinstance(value, int) and not isinstance(value, bool) and value > 0
    }
    if len(source_declared_seasons) != len(raw_declared_seasons):
        raise PlanError("来源声明季度包含无效或重复值")
    smart_kwargs = dict(kwargs)
    preserved_theme_residuals: list[dict[str, Any]] = []
    # Explicit episode maps intentionally bypass smart season inference, but
    # the common post-plan resource-gap audit still consumes this collection.
    positive_seasons: list[Mapping[str, Any]] = []
    # Provided source files are normalized and preclassified before any path
    # is chosen: the explicit episode-map path (a D proof's source-key map)
    # otherwise skips the smart grouping below entirely, and bonus-directory
    # residuals must never reach the episode parser on either path
    # (``EXTRA/[SP00] Menu - 01`` raised "发现未编号特别篇", 轮回七次 shape).
    provided_files = kwargs.get("source_files")
    if provided_files is not None:
        files = [dict(item) for item in provided_files]
        # Keep the smart wrapper consistent with ``build_tv_plan``: a work
        # whose only playable media lives under an Extras/SP container must
        # get the evidence-based bonus rescan before we freeze
        # ``source_files``.
        if not any(
            Path(str(item.get("name", ""))).suffix.lower() in VIDEO_EXTS
            for item in _filter_media(files)
        ):
            files = [
                dict(item)
                for item in kwargs["alist"].walk(
                    kwargs["src_path"],
                    ignore_orphan_temp=bool(kwargs.get("ignore_orphan_temp")),
                    include_bonus=True,
                )
            ]
        files, _exported_srt_issues = normalize_exported_srt_entries(
            kwargs["alist"], files,
        )
        files, preserved_theme_residuals = _preclassify_theme_residuals(files)
        smart_kwargs["source_files"] = files
        provided_files = files
        if kwargs.get("episode_map_path") is not None:
            return _plan_with_explicit_episode_map(
                kwargs, smart_kwargs, files,
                preserved_theme_residuals=preserved_theme_residuals,
            )
    if auto_episode_mode and kwargs.get("episode_map_path") is None:
        smart_kwargs["auto_special_title_match"] = True
        smart_kwargs["auto_align_subtitles"] = True
        if provided_files is not None:
            files = provided_files
        else:
            files = [
                dict(item)
                for item in kwargs["alist"].walk(
                    kwargs["src_path"],
                    ignore_orphan_temp=bool(kwargs.get("ignore_orphan_temp")),
                )
            ]
            # Keep the smart wrapper consistent with ``build_tv_plan``: a
            # work whose only playable media lives under an Extras/SP
            # container must get the evidence-based bonus rescan before we
            # freeze ``source_files``.  Otherwise the pre-scan masks the
            # lower-level fallback and falsely reports a subtitle-only
            # directory.
            if not any(
                Path(str(item.get("name", ""))).suffix.lower() in VIDEO_EXTS
                for item in _filter_media(files)
            ):
                files = [
                    dict(item)
                    for item in kwargs["alist"].walk(
                        kwargs["src_path"],
                        ignore_orphan_temp=bool(kwargs.get("ignore_orphan_temp")),
                        include_bonus=True,
                    )
                ]
            # Normalize provider-exported SRT sidecars before smart
            # season/movie partitioning.  The helper retains the exact
            # source path and marks failed candidates, so the lower-level
            # normal planner can surface a bounded problem rather than
            # silently treating them as ``.txt``.
            files, _exported_srt_issues = normalize_exported_srt_entries(
                kwargs["alist"], files,
            )
            files, preserved_theme_residuals = _preclassify_theme_residuals(files)
            smart_kwargs["source_files"] = files
        season_groups: dict[int, list[dict[str, Any]]] = defaultdict(list)
        special_files: list[dict[str, Any]] = []
        movie_groups: dict[int, list[dict[str, Any]]] = defaultdict(list)
        unknown_media: list[dict[str, Any]] = []
        child_tv_plans: list[Plan] = []
        retained_future_media: list[dict[str, Any]] = []
        retained_unpublished_season_media: list[tuple[dict[str, Any], int]] = []
        out_of_range_season_by_path: dict[str, int] = {}
        packed_single_season = False
        long_season_pack_diagnostic: str | None = None
        edition_group_warnings: dict[int, list[str]] = defaultdict(list)
        source_root = normalize_remote_path(str(kwargs["src_path"]))
        show_for_season_names: Mapping[str, Any] = {}
        official_special_titles: dict[int, str] = {}
        official_special_runtimes: dict[int, int] = {}
        official_special_air_dates: dict[int, str] = {}
        official_special_title_variants: dict[int, list[str]] = defaultdict(list)
        official_positive_season_numbers: set[int] = set()
        official_long_season_block_counts: list[int] = []
        official_long_season_episodes: list[Mapping[str, Any]] = []
        special_release_warnings: list[str] = []
        independent_e00_warning: str | None = None
        tmdb_id = kwargs.get("tmdb_id")
        tmdb_get = getattr(kwargs.get("tmdb_client"), "get", None)
        if (
            isinstance(tmdb_id, int)
            and not isinstance(tmdb_id, bool)
            and callable(tmdb_get)
        ):
            show_payload = tmdb_get(f"/tv/{tmdb_id}")
            if isinstance(show_payload, Mapping):
                show_for_season_names = show_payload
                positive_seasons = [
                    item
                    for item in (show_payload.get("seasons") or [])
                    if isinstance(item, Mapping)
                    and isinstance(item.get("season_number"), int)
                    and not isinstance(item.get("season_number"), bool)
                    and int(item["season_number"]) > 0
                    and isinstance(item.get("episode_count"), int)
                    and not isinstance(item.get("episode_count"), bool)
                ]
                official_positive_season_numbers = {
                    int(item["season_number"]) for item in positive_seasons
                }
                if len(positive_seasons) == 1:
                    long_season_number = int(positive_seasons[0]["season_number"])
                    try:
                        long_season_payload = tmdb_get(
                            f"/tv/{tmdb_id}/season/{long_season_number}"
                        )
                    except ApiError:
                        long_season_payload = {}
                    if isinstance(long_season_payload, Mapping):
                        official_long_season_episodes = [
                            item
                            for item in (long_season_payload.get("episodes") or [])
                            if isinstance(item, Mapping)
                        ]
                        official_long_season_block_counts = (
                            _tmdb_long_season_block_counts(
                                official_long_season_episodes
                            )
                        )
            try:
                special_payload = tmdb_get(f"/tv/{tmdb_id}/season/0")
            except ApiError:
                special_payload = {}
            if isinstance(special_payload, Mapping):
                official_special_titles = {
                    int(item["episode_number"]): str(item["name"])
                    for item in (special_payload.get("episodes") or [])
                    if isinstance(item, Mapping)
                    and isinstance(item.get("episode_number"), int)
                    and not isinstance(item.get("episode_number"), bool)
                    and isinstance(item.get("name"), str)
                    and item.get("name")
                }
                official_special_runtimes = {
                    int(item["episode_number"]): int(item["runtime"])
                    for item in (special_payload.get("episodes") or [])
                    if isinstance(item, Mapping)
                    and isinstance(item.get("episode_number"), int)
                    and not isinstance(item.get("episode_number"), bool)
                    and isinstance(item.get("runtime"), int)
                    and not isinstance(item.get("runtime"), bool)
                    and int(item["runtime"]) > 0
                }
                official_special_air_dates = {
                    int(item["episode_number"]): str(item["air_date"])
                    for item in (special_payload.get("episodes") or [])
                    if isinstance(item, Mapping)
                    and isinstance(item.get("episode_number"), int)
                    and not isinstance(item.get("episode_number"), bool)
                    and re.fullmatch(
                        r"\d{4}-\d{2}-\d{2}", str(item.get("air_date") or "")
                    )
                }
                for number, title in official_special_titles.items():
                    official_special_title_variants[number].append(title)
                primary_language = str(
                    getattr(kwargs.get("tmdb_client"), "language", "") or ""
                )
                for language in ("zh-CN", "zh-TW", "ja-JP", "en-US"):
                    if language == primary_language:
                        continue
                    try:
                        translated_specials = tmdb_get(
                            f"/tv/{tmdb_id}/season/0",
                            language=language,
                        )
                    except ApiError:
                        continue
                    if not isinstance(translated_specials, Mapping):
                        continue
                    for translated in translated_specials.get("episodes") or []:
                        if (
                            not isinstance(translated, Mapping)
                            or isinstance(translated.get("episode_number"), bool)
                            or not isinstance(translated.get("episode_number"), int)
                        ):
                            continue
                        title = str(translated.get("name") or "").strip()
                        number = int(translated["episode_number"])
                        if (
                            title
                            and title
                            not in official_special_title_variants[number]
                        ):
                            official_special_title_variants[number].append(title)
        beta_alternate_count = _map_explicit_beta_alternate(
            files,
            official_special_title_variants,
        )
        if beta_alternate_count:
            special_release_warnings.append(
                f"{beta_alternate_count} 个明确 23B/23β 版本已根据多语言官方"
                "β/Missing Link 特别篇标题映射到 Season 00"
            )
        release_edition_count = _map_release_label_editions(
            files,
            official_special_title_variants,
        )
        if release_edition_count:
            special_release_warnings.append(
                _release_edition_warning_template().format(
                    count=release_edition_count,
                )
            )
        split_special_count = _map_split_official_special_folder(
            files,
            official_special_title_variants,
        )
        if split_special_count:
            special_release_warnings.append(
                f"{split_special_count} 个分篇文件所在目录与唯一官方特别篇标题一致；"
                "TMDB 仅建一条时已保留同一 S00 集号并按连续 part 命名"
            )
        disc_extra_count = _map_disc_extras_by_official_release_runs(
            files,
            show=show_for_season_names,
            positive_seasons=positive_seasons,
            special_runtimes=official_special_runtimes,
            special_air_dates=official_special_air_dates,
            special_title_variants=official_special_title_variants,
        )
        if disc_extra_count:
            special_release_warnings.append(
                f"{disc_extra_count} 个特典小动画/OVA 已依官方短片时长、"
                "发行断档和源季序映射到全局 Season 00 编号"
            )
        # A suffix such as ``[13 OAV]`` describes an extra released after
        # episode 13, not necessarily OAV number 13.  Resolve this before the
        # smart planner splits regular seasons and special folders; after that
        # split the complete E01-E13 boundary evidence would be unavailable.
        # The helper mutates only the uniquely proven OAV and its exact-name
        # subtitle companions with an explicit Season 00 override.
        pre_split_groups: dict[EpisodeKey, list[dict[str, Any]]] = defaultdict(list)
        for item in _filter_media(files):
            key = extract_episode_key(str(item.get("name", "")))
            if key is not None:
                pre_split_groups[key].append(item)
        special_release_warnings.extend(
            _remap_suffix_oav_on_air_versions(
                pre_split_groups,
                {
                    int(item["season_number"]): int(item["episode_count"])
                    for item in positive_seasons
                },
            )
        )
        special_release_warnings.extend(
            _remap_postseason_oav_suffix(
                pre_split_groups,
                {
                    EpisodeKey("special", number): title
                    for number, title in official_special_titles.items()
                },
            )
        )
        # ``_filter_media`` intentionally returns defensive copies.  Carry
        # only the proven pre-split overrides back to the planner's private
        # file list; otherwise the evidence pass would disappear when the
        # smart planner later rebuilds its season/special groups.
        pre_split_overrides = {
            str(item.get("full_path", "")): item
            for members in pre_split_groups.values()
            for item in members
            if isinstance(item.get("_episode_key_override"), int)
        }
        for item in files:
            proven = pre_split_overrides.get(str(item.get("full_path", "")))
            if proven is None:
                continue
            for field in (
                "_episode_kind_override",
                "_episode_key_override",
                "_episode_end_override",
                "_edition_override",
            ):
                if field in proven:
                    item[field] = proven[field]
        official_season_counts = {
            int(item["season_number"]): int(item["episode_count"])
            for item in positive_seasons
        }
        packed_ova_seasons: dict[int, int] = {}
        ova_volume_videos: dict[int, list[dict[str, Any]]] = defaultdict(list)
        for raw_item in _filter_media(files):
            if Path(str(raw_item.get("name", ""))).suffix.lower() not in VIDEO_EXTS:
                continue
            full_path = str(raw_item.get("full_path", ""))
            relative = full_path[len(source_root.rstrip("/")) :].lstrip("/")
            parent_segments = relative.split("/")[:-1]
            volume = None
            for segment in reversed(parent_segments):
                volume = _ova_volume_ordinal(segment)
                if volume is not None:
                    break
            if volume is not None:
                ova_volume_videos[volume].append(dict(raw_item))
        # An OVA release may call each physical volume a “Season” and pack
        # several short official episodes into one video. Accept that layout
        # only when the parent volume numbers cover the complete TMDB season
        # set, every volume contains exactly one video, and every corresponding
        # official season contains multiple episodes.
        if (
            ova_volume_videos
            and set(ova_volume_videos) == set(official_season_counts)
            and all(len(items) == 1 for items in ova_volume_videos.values())
            and all(count > 1 for count in official_season_counts.values())
        ):
            packed_ova_seasons = dict(official_season_counts)
        e00_media = [
            item
            for item in _filter_media(files)
            if (
                (key := extract_episode_key(str(item.get("name", ""))))
                is not None
                and key.kind == "regular"
                and key.number == 0
                and not key.end_number
            )
        ]
        e00_movie_match = (
            _e00_independent_movie_match(
                kwargs["tmdb_client"],
                e00_media,
                show_for_season_names,
            )
            if e00_media and show_for_season_names
            else None
        )
        if e00_movie_match is not None:
            independent_e00_warning = (
                "源文件 E00 的明确副标题已通过 TMDB 搜索，并由所有发行版本唯一"
                f"确认对应独立电影《{e00_movie_match.title}》"
                f"（{e00_movie_match.year}）；未把它猜作 Season 00 特别篇"
            )
        for raw_item in _filter_media(files):
            item = dict(raw_item)
            full_path = str(item["full_path"])
            relative = full_path[len(source_root.rstrip("/")) :].lstrip("/")
            segments = relative.split("/")
            inferred_season = None
            inferred_season_from_parent = False
            for segment in reversed(segments[:-1]):
                inferred_season = _season_from_source("/" + segment)
                if inferred_season is None:
                    inferred_season = _season_from_series_variant(
                        segment, show_for_season_names
                    )
                if inferred_season is not None:
                    inferred_season_from_parent = True
                    break
            if inferred_season is None:
                inferred_season = _season_from_source("/" + segments[-1])
            if inferred_season is None:
                inferred_season = _season_from_series_variant(
                    segments[-1], show_for_season_names
                )
            if inferred_season is None:
                match = re.search(r"S(?:eason)?\s*0*(\d{1,3})\s*E\s*\d+", segments[-1], re.I)
                if match:
                    inferred_season = int(match.group(1))
            explicit_release_pair = _explicit_release_season_episode(
                segments[-1], official_season_counts,
            )
            if inferred_season is None and explicit_release_pair is not None:
                inferred_season, explicit_episode = explicit_release_pair
                item["_episode_kind_override"] = "regular"
                item["_episode_key_override"] = explicit_episode
            movie_tmdb_id = _embedded_movie_tmdb_id(item)
            source_episode_key = extract_episode_key(str(item.get("name", "")))
            # A regular episode can legitimately contain a movie-like word in
            # its title (for example ``...-大电影``).  When the file carries an
            # explicit SxxEyy coordinate that agrees with an enclosing season
            # directory, that structural evidence wins over the loose movie
            # keyword heuristic below.  Keep the parent-season requirement so
            # a genuinely independent, top-level movie is still fail-closed
            # and reaches the movie matcher.
            explicit_filename_season: int | None = None
            explicit_filename_match = re.search(
                r"S(?:eason)?\s*0*(\d{1,3})\s*E\s*0*\d+",
                str(item.get("name", "")),
                re.IGNORECASE,
            )
            if explicit_filename_match is not None:
                explicit_filename_season = int(explicit_filename_match.group(1))
            explicit_parent_tv_episode = (
                inferred_season_from_parent
                and explicit_filename_season is not None
                and explicit_filename_season == inferred_season
                and source_episode_key is not None
                and source_episode_key.kind == "regular"
                # An explicit embedded movie identity is authoritative.  A
                # parent directory explicitly labelled as a movie/collection
                # is likewise not converted into TV merely because a child
                # filename happens to contain an SxxEyy token.
                and movie_tmdb_id is None
                and not _has_movie_context({
                    "full_path": posixpath.dirname(full_path),
                })
            )
            ova_volume = None
            for segment in reversed(segments[:-1]):
                ova_volume = _ova_volume_ordinal(segment)
                if ova_volume is not None:
                    break
            if ova_volume in packed_ova_seasons:
                item["_episode_kind_override"] = "regular"
                item["_episode_key_override"] = 1
                item["_episode_end_override"] = packed_ova_seasons[ova_volume]
                season_groups[ova_volume].append(item)
                if not edition_group_warnings[ova_volume]:
                    edition_group_warnings[ova_volume].append(
                        f"父目录 OVA {ova_volume:02d} 与 TMDB 第 {ova_volume} 季"
                        f"唯一对应，且该卷只有一个视频；已按官方 "
                        f"{packed_ova_seasons[ova_volume]} 集结构保留为合并集"
                    )
                continue
            if (
                e00_movie_match is not None
                and source_episode_key is not None
                and source_episode_key.kind == "regular"
                and source_episode_key.number == 0
                and not source_episode_key.end_number
            ):
                movie_groups[e00_movie_match.tmdb_id].append(item)
                continue
            # New Edit is an alternate cut of Season 01, not evidence that the
            # enclosing release folder itself is an ordinary season folder.
            # Keep it out of direct season inference so the proven 2N-1 range
            # mapper below can preserve the multi-episode edition correctly.
            if entry_edition_tag(item) == "New Edit":
                unknown_media.append(item)
                continue
            # A surrounding series label can resemble the parent TV title and
            # make ``_season_from_series_variant`` infer Season 01.  Explicit
            # numbered live-action/movie-collection entries must reach the
            # official collection evidence pass before that TV inference.
            if _has_numbered_movie_collection_context(item):
                unknown_media.append(item)
                continue
            # An inner ``SPs``/OVA/mini-anime directory is more specific than
            # an outer folder such as “第二季”.  Let the special mapper handle
            # it before inheriting the parent TV season.
            if (
                source_episode_key is not None
                and source_episode_key.kind == "fractional"
                and inferred_season in official_positive_season_numbers
            ):
                # A decimal release such as Zoku Shou 10.5 belongs to the
                # explicitly inferred source season even when an enclosing
                # pack name also advertises ``SP+Extras``.  Keeping it in a
                # global special bucket would later attach it to the first
                # season and make the correct broadcast interval impossible
                # to evaluate.
                season_groups[int(inferred_season)].append(item)
            elif item.get("_episode_kind_override") == "special":
                special_files.append(item)
            elif (
                inferred_season is not None
                # A numbered top-level release folder such as
                # ``05 剧场版：雪下的誓言`` is a movie identity, not
                # Season 05 or episode 05.  Let the independently evidenced
                # movie matcher below consume it before any season-number
                # inheritance can turn the folder ordinal into an episode.
                and (
                    explicit_parent_tv_episode
                    or not _has_movie_context(item)
                )
                and not _special_context_overrides_parent_season(item)
                and (
                    inferred_season_from_parent
                    or not _has_special_context(item)
                )
                and not _matches_named_special_release_context(
                    item,
                    official_special_title_variants,
                    series_titles=(
                        str(show_for_season_names.get("name") or ""),
                        str(show_for_season_names.get("original_name") or ""),
                    ),
                )
            ):
                season_groups[inferred_season].append(item)
            elif movie_tmdb_id is not None:
                movie_groups[movie_tmdb_id].append(item)
            elif (
                _has_movie_context(item)
                and Path(str(item.get("name", ""))).suffix.lower() in SUBTITLE_EXTS
            ):
                # A generic ``简中.ass`` cannot establish a movie identity by
                # itself. Stage it for the proven same-directory/video-title
                # companion pass below; otherwise a subtitle-only movie group
                # can be created beside the correct video group.
                unknown_media.append(item)
            elif _has_movie_context(item) and Path(str(item.get("name", ""))).suffix.lower() in VIDEO_EXTS:
                movie_match = None
                last_movie_error: PlanError | None = None
                for movie_query in _movie_queries_from_item(item):
                    try:
                        movie_match, _ = auto_match_tmdb(
                            kwargs["tmdb_client"],
                            movie_query,
                            media_type="movie",
                            min_confidence=0.88,
                            prefer_animation=_media_context_from_source_and_target(
                                source_root,
                                str(kwargs["parent_path"]),
                            )[1],
                        )
                        if movie_match.status != "confirmed":
                            continue
                        break
                    except PlanError as exc:
                        last_movie_error = exc
                if movie_match is None:
                    release_years = [
                        int(value)
                        for value in re.findall(r"(?:19|20)\d{2}", full_path)
                    ]
                    if release_years and max(release_years) >= datetime.now().year:
                        retained_future_media.append(item)
                        continue
                    raise PlanError(
                        f"剧场版无法自动识别: {full_path}；{last_movie_error or '没有可用标题'}"
                    ) from last_movie_error
                movie_groups[movie_match.tmdb_id].append(item)
            elif _has_special_context(item) or _matches_named_special_release_context(
                item,
                official_special_title_variants,
                series_titles=(
                    str(show_for_season_names.get("name") or ""),
                    str(show_for_season_names.get("original_name") or ""),
                ),
            ):
                # A named unnumbered OVA can be an independently catalogued
                # movie (for example Prisma Phantasm).  First preserve any
                # official Season 00 override proven by the release-run mapper
                # above; otherwise accept a movie only from a confirmed exact
                # TMDB title match. Generic ``OVA.mkv`` cannot pass this path.
                if (
                    item.get("_episode_kind_override") != "special"
                    and Path(str(item.get("name", ""))).suffix.lower() in VIDEO_EXTS
                    and source_episode_key is None
                ):
                    named_ova_movie: AutoMatch | None = None
                    for movie_query in _movie_queries_from_item(item):
                        if not _usable_release_title_query(movie_query):
                            continue
                        try:
                            candidate, _ = auto_match_tmdb(
                                kwargs["tmdb_client"],
                                movie_query,
                                media_type="movie",
                                min_confidence=0.88,
                                prefer_animation=_media_context_from_source_and_target(
                                    source_root,
                                    str(kwargs["parent_path"]),
                                )[1],
                            )
                        except ScraperError:
                            continue
                        if (
                            candidate.status == "confirmed"
                            and candidate.media_type == "movie"
                            and _specific_movie_query_agrees_with_match(
                                movie_query, candidate
                            )
                        ):
                            named_ova_movie = candidate
                            break
                    if named_ova_movie is not None:
                        movie_groups[named_ova_movie.tmdb_id].append(item)
                        continue
                source_title_key = _normalize_match_title(_query_from_source(source_root))
                series_title_keys = {
                    _normalize_match_title(str(show_for_season_names.get(field) or ""))
                    for field in ("name", "original_name")
                }
                series_title_keys.discard("")
                relative_parents = segments[:-1]
                label_keys: set[str] = set()
                for label in relative_parents:
                    label_key = _normalize_match_title(label)
                    for series_key in {source_title_key, *series_title_keys}:
                        if series_key:
                            label_key = label_key.replace(series_key, "")
                    if len(label_key) >= 4:
                        label_keys.add(label_key)
                file_label_key = _normalize_match_title(
                    Path(str(item.get("name", ""))).stem
                )
                # A numbered named-special run commonly uses filenames such
                # as ``EX Season 01``/``EX Season 02``.  Strip only the final
                # source ordinal so both files can match the shared official
                # TMDB title prefix and then be assigned in source order.
                file_label_key = re.sub(r"\d+$", "", file_label_key)
                if len(file_label_key) >= 4:
                    label_keys.add(file_label_key)
                matching_specials = {
                    number
                    for number, official_title in official_special_titles.items()
                    if any(
                        label_key in _normalize_match_title(official_title)
                        or _normalize_match_title(official_title) in label_key
                        for label_key in label_keys
                    )
                }
                if not matching_specials:
                    # Release directories often wrap a named special arc in
                    # dates, group names and codec tags, so the whole directory
                    # label is no longer a substring of any one episode title.
                    # Recover only a sufficiently long official prefix shared
                    # by a consecutive TMDB special run.  For example, all
                    # three official ``柯里乌斯之梦 …`` titles share the same
                    # arc prefix even inside a noisy ``2023.11 … [WebRip]``
                    # release path.  A lone or non-consecutive title is not
                    # enough evidence for this fallback.
                    official_title_keys = {
                        number: _normalize_match_title(title)
                        for number, title in official_special_titles.items()
                    }
                    prefix_matches: set[tuple[int, ...]] = set()
                    for title_key in official_title_keys.values():
                        for length in range(5, len(title_key) + 1):
                            prefix = title_key[:length]
                            numbers = tuple(sorted(
                                number
                                for number, candidate in official_title_keys.items()
                                if candidate.startswith(prefix)
                            ))
                            if (
                                len(numbers) >= 2
                                and list(numbers)
                                == list(range(numbers[0], numbers[-1] + 1))
                                and any(prefix in label_key for label_key in label_keys)
                            ):
                                prefix_matches.add(numbers)
                    if len(prefix_matches) == 1:
                        matching_specials = set(next(iter(prefix_matches)))
                if len(matching_specials) == 1:
                    item["_episode_kind_override"] = "special"
                    item["_episode_key_override"] = next(iter(matching_specials))
                elif matching_specials:
                    # Named special mini-series are commonly stored as a
                    # numbered three-part folder.  When that folder label
                    # matches an equally sized consecutive run of official
                    # TMDB special titles, preserve source order and map the
                    # numbered files to that official run.  This covers
                    # releases such as “柯里乌斯之梦” without treating them as
                    # Season 01 episodes.
                    source_key = extract_episode_key(str(item.get("name", "")))
                    ordered_specials = sorted(matching_specials)
                    if (
                        source_key is not None
                        and source_key.kind == "regular"
                        and not source_key.end_number
                        and 1 <= source_key.number <= len(ordered_specials)
                    ):
                        item["_episode_kind_override"] = "special"
                        item["_episode_key_override"] = ordered_specials[
                            source_key.number - 1
                        ]
                special_files.append(item)
            else:
                unknown_media.append(item)

        remapped_season_groups, reset_absolute_warning = (
            _remap_complete_reset_absolute_season_groups(
                season_groups,
                positive_seasons,
            )
        )
        if reset_absolute_warning:
            season_groups = defaultdict(list, remapped_season_groups)
            special_release_warnings.append(reset_absolute_warning)

        # A second encode of the same one-season show may use bare bracket
        # numbers (``[01]``) while another encode uses explicit ``S01E01``.
        runtime_movie_groups, runtime_movie_warnings = (
            _extract_runtime_proven_overflow_movies(
                kwargs["alist"],
                kwargs["tmdb_client"],
                show_for_season_names,
                season_groups,
                official_season_counts,
                official_special_runtimes,
                unknown_media,
            )
        )
        for runtime_movie_id, runtime_movie_files in runtime_movie_groups.items():
            movie_groups[runtime_movie_id].extend(runtime_movie_files)
        special_release_warnings.extend(runtime_movie_warnings)

        # Once the TV work has exactly one official positive season, accept the
        # bare form only when its cleaned release title exactly names that same
        # show and every number is within the official season boundary.
        if len(positive_seasons) == 1 and unknown_media:
            sole_season = int(positive_seasons[0]["season_number"])
            sole_count = int(positive_seasons[0]["episode_count"])
            show_title_keys = {
                _normalize_match_title(str(show_for_season_names.get(field) or ""))
                for field in ("name", "original_name")
            }
            show_title_keys.discard("")
            proven_bare_items: list[dict[str, Any]] = []
            remaining_unknown: list[dict[str, Any]] = []
            for item in unknown_media:
                key = extract_episode_key(str(item.get("name", "")))
                query_keys = {
                    _normalize_match_title(query)
                    for query in _movie_queries_from_item(item)
                }
                if (
                    key is not None
                    and key.kind == "regular"
                    and not key.end_number
                    and 1 <= key.number <= sole_count
                    and bool(query_keys & show_title_keys)
                    and entry_edition_tag(item) is None
                ):
                    proven_bare_items.append(item)
                else:
                    remaining_unknown.append(item)
            if proven_bare_items:
                season_groups[sole_season].extend(proven_bare_items)
                edition_group_warnings[sole_season].append(
                    "检测到同一作品的另一发行版本使用裸 [01] 集号；其清理后标题与"
                    "TMDB 正式剧名完全一致，且编号位于唯一官方季度范围内，已合并比较"
                )
                unknown_media = remaining_unknown

        special_release_warnings.extend(
            _map_explicit_special_release_runs(
                special_files,
                official_special_title_variants,
                official_special_air_dates,
            )
        )
        # Resolve metadata-backed generic SP files while the smart planner
        # still has the complete official/used-special context.  Waiting until
        # the split sub-plan loses that context (for example SP02-SP05 may be
        # in a named short-series child while 23β is an explicit override),
        # leaving a provable SP01 as a false orphan.
        if len(positive_seasons) == 1 and special_files:
            pre_split_special_groups = parse_ep_files(
                special_files,
                prefer_simplified=False,
                defer_unnumbered_specials=True,
            )
            metadata_special_warnings = _map_unnumbered_special_from_subtitle_title(
                kwargs["alist"],
                special_files,
                pre_split_special_groups,
                {
                    EpisodeKey("special", number): title
                    for number, title in official_special_titles.items()
                },
                series_titles=[
                    str(show_for_season_names.get("name") or ""),
                    str(show_for_season_names.get("original_name") or ""),
                ],
                regular_episode_count=int(positive_seasons[0]["episode_count"]),
                tmdb_client=kwargs["tmdb_client"],
                tmdb_id=int(tmdb_id),
                season=int(positive_seasons[0]["season_number"]),
            )
            if metadata_special_warnings:
                mapped_special_paths = {
                    str(item.get("full_path", "")): key.number
                    for key, members in pre_split_special_groups.items()
                    if key.kind == "special"
                    for item in members
                }
                for item in special_files:
                    mapped_number = mapped_special_paths.get(
                        str(item.get("full_path", ""))
                    )
                    if mapped_number is None:
                        continue
                    item["_episode_kind_override"] = "special"
                    item["_episode_key_override"] = mapped_number
                special_release_warnings.extend(metadata_special_warnings)
        propagated_special_subtitles = _propagate_explicit_video_episode_overrides(
            [
                *special_files,
                *unknown_media,
                *(item for group in season_groups.values() for item in group),
            ]
        )
        if propagated_special_subtitles:
            special_release_warnings.append(
                f"{propagated_special_subtitles} 个与已确认特别篇视频同名的外挂字幕"
                "已跟随视频的官方季集映射"
            )
            # A backup subtitle directory may have been classified under the
            # parent season before its exact-basename video proved a special
            # mapping.  Keep the proven companions in the same subplan as the
            # video; otherwise split planning would see an orphan subtitle.
            moved_paths: set[str] = set()
            for season_number in list(season_groups):
                retained: list[dict[str, Any]] = []
                for item in season_groups[season_number]:
                    if item.get("_episode_kind_override") == "special":
                        special_files.append(item)
                        moved_paths.add(str(item.get("full_path", "")))
                    else:
                        retained.append(item)
                season_groups[season_number] = retained
            remaining_unknown: list[dict[str, Any]] = []
            for item in unknown_media:
                if item.get("_episode_kind_override") == "special":
                    special_files.append(item)
                    moved_paths.add(str(item.get("full_path", "")))
                else:
                    remaining_unknown.append(item)
            unknown_media = remaining_unknown
            if moved_paths:
                special_files = list({
                    str(item.get("full_path", "")): item
                    for item in special_files
                }.values())

        special_files.extend(
            _detach_numbered_subgroups_from_mixed_movie_groups(movie_groups)
        )

        # A folder labelled OAD/OVA/“特别篇” may be a separately catalogued
        # child work rather than the parent's Season 00.  Resolve the complete
        # top-level folder as its own identity using the cleaned release title,
        # the parent show title plus folder label, and (for TV children) the
        # exact episode count.  Only one confirmed strongest TMDB identity is
        # accepted; otherwise the files remain in the ordinary special
        # diagnostic path below.
        special_top_groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for item in special_files:
            full_path = str(item.get("full_path", ""))
            relative = full_path[len(source_root.rstrip("/")) :].lstrip("/")
            parts = relative.split("/")
            if len(parts) >= 2:
                special_top_groups[parts[0]].append(item)
        independently_routed_paths: set[str] = set()
        for segment, group_items in sorted(special_top_groups.items()):
            videos = [
                item for item in group_items
                if Path(str(item.get("name", ""))).suffix.lower() in VIDEO_EXTS
            ]
            # A complete official Season 00 mapping is stronger than a fuzzy
            # independent-work search for the enclosing folder. Bare part
            # filenames can match an unrelated numeric movie, and a disc
            # extra run can share a title with a separately catalogued OVA.
            # Once every video has an explicit official-special override, do
            # not let this later child-work pass steal it.
            if videos and all(
                item.get("_episode_kind_override") == "special"
                and isinstance(item.get("_episode_key_override"), int)
                for item in videos
            ):
                continue
            source_numbers = sorted({
                key.number
                for item in videos
                if (key := extract_episode_key(str(item.get("name", "")))) is not None
                and key.kind in {"regular", "special"}
                and not key.end_number
            })
            if (
                not videos
                or source_numbers != list(range(1, len(source_numbers) + 1))
                or len(source_numbers) != len(videos)
            ):
                continue
            show_title = str(
                show_for_season_names.get("name")
                or show_for_season_names.get("original_name")
                or ""
            ).strip()
            parent_titles = [
                str(show_for_season_names.get(field) or "").strip()
                for field in ("name", "original_name")
                if str(show_for_season_names.get(field) or "").strip()
            ]
            try:
                parent_aliases = [
                    *parent_titles,
                    *_alternative_tmdb_titles(
                        kwargs["tmdb_client"], "tv", int(kwargs["tmdb_id"])
                    ),
                ]
            except (ApiError, KeyError, TypeError, ValueError):
                parent_aliases = parent_titles
            queries = _child_work_query_variants(
                [
                    query
                    for item in videos[:3]
                    # Only the concrete release title is child identity.
                    # Later fallbacks may be the bare franchise name, codec
                    # payload, or enclosing movie folder and can introduce an
                    # unrelated two-episode title into the ranking.
                    for query in _movie_queries_from_item(item)[:1]
                ],
                parent_titles=parent_titles,
                parent_aliases=parent_aliases,
            )
            if show_title and not queries:
                directory_query = f"{show_title} {segment}"
                if _usable_release_title_query(directory_query):
                    queries.append(directory_query)
            queries = list(dict.fromkeys(queries))
            candidates: dict[tuple[str, int], AutoMatch] = {}
            for query in queries:
                try:
                    candidate, _ = auto_match_tmdb(
                        kwargs["tmdb_client"],
                        query,
                        media_type="tv",
                        min_confidence=0.88,
                        prefer_animation=_media_context_from_source_and_target(
                            source_root,
                            str(kwargs["parent_path"]),
                        )[1],
                        expected_episode_count=len(source_numbers),
                    )
                except ScraperError:
                    continue
                if (
                    candidate.status != "confirmed"
                    or candidate.tmdb_id == kwargs.get("tmdb_id")
                    or candidate.media_type not in {"tv", "movie"}
                ):
                    continue
                identity = (candidate.media_type, candidate.tmdb_id)
                previous = candidates.get(identity)
                if previous is None or candidate.confidence > previous.confidence:
                    candidates[identity] = candidate
            if not candidates:
                continue
            ranked = sorted(
                candidates.values(),
                key=lambda item: item.confidence,
                reverse=True,
            )
            if len(ranked) > 1 and ranked[0].confidence - ranked[1].confidence < 0.08:
                continue
            child_match = ranked[0]
            if child_match.media_type == "movie":
                movie_groups[child_match.tmdb_id].extend(group_items)
                independently_routed_paths.update(
                    str(item["full_path"]) for item in group_items
                )
                special_release_warnings.append(
                    f"子目录《{segment}》已通过 TMDB 标题/别名唯一确认是独立电影"
                    f"《{child_match.title}》（{child_match.year}），未归入母作品 Season 00"
                )
                continue
            child_detail = kwargs["tmdb_client"].get(f"/tv/{child_match.tmdb_id}")
            positive_child_seasons = [
                item
                for item in (child_detail.get("seasons") or [])
                if isinstance(item, Mapping)
                and isinstance(item.get("season_number"), int)
                and int(item["season_number"]) > 0
                and isinstance(item.get("episode_count"), int)
            ]
            if (
                len(positive_child_seasons) != 1
                or int(positive_child_seasons[0]["episode_count"])
                != len(source_numbers)
            ):
                continue
            child_season = int(positive_child_seasons[0]["season_number"])
            child_files: list[dict[str, Any]] = []
            for original in group_items:
                item = dict(original)
                key = extract_episode_key(str(item.get("name", "")))
                if key is not None and key.kind in {"regular", "special"}:
                    item["_episode_kind_override"] = "regular"
                    item["_episode_key_override"] = key.number
                child_files.append(item)
            child_plan = build_tv_plan(
                alist=kwargs["alist"],
                tmdb_client=kwargs["tmdb_client"],
                src_path=source_root,
                parent_path=kwargs["parent_path"],
                tmdb_id=child_match.tmdb_id,
                season=child_season,
                absolute=False,
                prefer_simplified=bool(kwargs.get("prefer_simplified")),
                allow_unmapped=False,
                ignore_orphan_temp=bool(kwargs.get("ignore_orphan_temp")),
                episode_map_path=None,
                episode_group_id=None,
                auto_special_title_match=True,
                auto_align_subtitles=True,
                source_files=child_files,
            )
            child_tv_plans.append(child_plan)
            independently_routed_paths.update(
                str(item["full_path"]) for item in group_items
            )
            special_release_warnings.append(
                f"子目录《{segment}》已通过 TMDB 标题/别名和完整 {len(source_numbers)} 集"
                f"边界唯一确认是独立剧集《{child_match.title}》（{child_match.year}），"
                "未归入母作品 Season 00"
            )
        if independently_routed_paths:
            special_files = [
                item for item in special_files
                if str(item.get("full_path", "")) not in independently_routed_paths
            ]

        # A directly matched season may store ordinary episodes as bare
        # ``01.mkv`` files under quality/language wrappers.  Accept the run
        # either for a one-season work, or when the franchise member matcher
        # has already proven a non-default official season from the exact
            # member title and complete episode boundary.  The internal
            # confidence flag
        # is deliberately unavailable to an ordinary season input, so a
        # multi-season root cannot be guessed into Season 01.
        requested_meta = next(
            (
                item for item in positive_seasons
                if int(item["season_number"]) == int(kwargs.get("season") or 1)
            ),
            None,
        )
        boundary_meta = (
            requested_meta
            if proven_member_season and requested_meta is not None
            else positive_seasons[0]
            if len(positive_seasons) == 1
            else None
        )
        if boundary_meta is not None and not season_groups and unknown_media:
            sole_season = int(boundary_meta["season_number"])
            sole_count = int(boundary_meta["episode_count"])
            regular_video_rows = [
                key.number
                for item in unknown_media
                if Path(str(item.get("name", ""))).suffix.lower() in VIDEO_EXTS
                and not _has_special_context(item)
                and (key := extract_episode_key(str(item.get("name", "")))) is not None
                and key.kind == "regular"
                and not key.end_number
            ]
            if (
                set(regular_video_rows) == set(range(1, sole_count + 1))
            ):
                retained_unknown: list[dict[str, Any]] = []
                for item in unknown_media:
                    key = extract_episode_key(str(item.get("name", "")))
                    if (
                        key is not None
                        and key.kind == "regular"
                        and not key.end_number
                        and 1 <= key.number <= sole_count
                        and not _has_special_context(item)
                    ):
                        season_groups[sole_season].append(item)
                    else:
                        retained_unknown.append(item)
                unknown_media = retained_unknown
                edition_group_warnings[sole_season].append(
                    "源根目录中的裸集号完整覆盖已确认的 TMDB "
                    f"Season {sole_season:02d} 边界；已按完整边界归入"
                )

        # Some release folders flatten several official TMDB seasons into one
        # cumulative E01..EN sequence (often with E00 as the prologue).  Split
        # that sequence only when it exactly covers the sum of the advertised
        # official seasons; this avoids guessing from a partial or irregular
        # release.  The mapping is generic and follows TMDB season counts.
        named_child_video_segments: set[str] = set()
        for item in unknown_media:
            if Path(str(item.get("name", ""))).suffix.lower() not in VIDEO_EXTS:
                continue
            full_path = str(item.get("full_path", ""))
            relative = full_path[len(source_root.rstrip("/")) :].lstrip("/")
            parts = relative.split("/")
            if len(parts) < 2:
                continue
            segment = parts[0]
            if (
                _season_from_source("/" + segment) is None
                and _season_from_series_variant(segment, show_for_season_names) is None
                and _usable_release_title_query(_query_from_source("/" + segment))
            ):
                named_child_video_segments.add(segment)
        if (
            not season_groups
            and len(positive_seasons) >= 2
            and unknown_media
            and not named_child_video_segments
        ):
            official_seasons = sorted(
                (
                    int(item["season_number"]),
                    int(item["episode_count"]),
                )
                for item in positive_seasons
            )
            official_total = sum(count for _, count in official_seasons)
            source_video_keys = sorted({
                key.number
                for item in unknown_media
                if Path(str(item.get("name", ""))).suffix.lower() in VIDEO_EXTS
                and (key := extract_episode_key(str(item.get("name", "")))) is not None
                and key.kind == "regular"
                and not key.end_number
                and key.number > 0
            })
            if source_video_keys == list(range(1, official_total + 1)):
                cumulative_map: dict[int, tuple[int, int]] = {}
                offset = 0
                for official_season, episode_count in official_seasons:
                    for local_episode in range(1, episode_count + 1):
                        cumulative_map[offset + local_episode] = (
                            official_season,
                            local_episode,
                        )
                    offset += episode_count
                remaining_unknown: list[dict[str, Any]] = []
                zero_items: list[dict[str, Any]] = []
                for original in unknown_media:
                    key = extract_episode_key(str(original.get("name", "")))
                    if key is None or key.kind != "regular" or key.end_number:
                        remaining_unknown.append(original)
                        continue
                    if key.number == 0:
                        zero_items.append(original)
                        continue
                    mapped = cumulative_map.get(key.number)
                    if mapped is None:
                        remaining_unknown.append(original)
                        continue
                    official_season, local_episode = mapped
                    item = dict(original)
                    item["_episode_kind_override"] = "regular"
                    item["_episode_key_override"] = local_episode
                    season_groups[official_season].append(item)
                unknown_media = remaining_unknown
                if zero_items and official_special_titles:
                    prologue_candidates = [
                        number
                        for number, titles in official_special_title_variants.items()
                        if any(
                            re.search(
                                r"(?:prologue|序章|プロローグ|前日[譚谭])",
                                title,
                                re.IGNORECASE,
                            )
                            for title in titles
                        )
                    ]
                    prologue_special = (
                        prologue_candidates[0]
                        if len(prologue_candidates) == 1
                        else None
                    )
                else:
                    prologue_special = None
                if zero_items and prologue_special is not None:
                    for original in zero_items:
                        item = dict(original)
                        item["_episode_kind_override"] = "special"
                        item["_episode_key_override"] = prologue_special
                        special_files.append(item)
                    remaining_specials = sorted(
                        set(official_special_titles) - {prologue_special}
                    )
                    unnumbered_specials = [
                        item
                        for item in special_files
                        if (
                            (key := extract_episode_key(str(item.get("name", ""))))
                            is not None
                            and key.kind == "special"
                            and key.number == 0
                            and item.get("_episode_key_override") is None
                        )
                    ]
                    # A sole remaining TMDB candidate is not evidence by
                    # itself.  Require the relative file sizes of E00 and SP
                    # to agree with the official TMDB runtimes; releases from
                    # the same encode family then provide an independent
                    # signal before we assign the unnumbered special.
                    zero_sizes = sorted(
                        int(item.get("size") or 0)
                        for item in zero_items
                        if Path(str(item.get("name", ""))).suffix.lower()
                        in VIDEO_EXTS
                        and int(item.get("size") or 0) > 0
                    )
                    special_sizes = sorted(
                        int(item.get("size") or 0)
                        for item in unnumbered_specials
                        if Path(str(item.get("name", ""))).suffix.lower()
                        in VIDEO_EXTS
                        and int(item.get("size") or 0) > 0
                    )
                    source_ratio = (
                        special_sizes[len(special_sizes) // 2]
                        / zero_sizes[len(zero_sizes) // 2]
                        if zero_sizes and special_sizes
                        else 0.0
                    )
                    runtime_ratio = (
                        official_special_runtimes.get(remaining_specials[0], 0)
                        / official_special_runtimes.get(prologue_special, 0)
                        if len(remaining_specials) == 1
                        and official_special_runtimes.get(prologue_special, 0)
                        else 0.0
                    )
                    ratio_agrees = (
                        source_ratio > 0
                        and runtime_ratio > 0
                        and 0.67 <= source_ratio / runtime_ratio <= 1.5
                    )
                    if (
                        len(remaining_specials) == 1
                        and unnumbered_specials
                        and ratio_agrees
                    ):
                        for item in unnumbered_specials:
                            item["_episode_kind_override"] = "special"
                            item["_episode_key_override"] = remaining_specials[0]
                        edition_group_warnings[official_seasons[0][0]].append(
                            "E00 已由 TMDB 多语言标题确认对应序章；未编号 SP 的"
                            "文件大小比例与 TMDB 官方特别篇时长比例一致，已据此确认映射"
                        )
                packed_single_season = True
                edition_group_warnings[official_seasons[0][0]].append(
                    "源目录使用跨季度连续集号；已按 TMDB 各季度官方集数边界"
                    f"拆分为 {len(official_seasons)} 个 Season"
                )

        # A named "New Edit" is an alternate cut, not a separate season or a
        # generic Director's Cut.  When the ordinary first-season source has
        # exactly 2N-1 episodes and the re-edit has N consecutively numbered
        # files, the one-hour re-edit structure is mechanically provable:
        # file 1 covers E01 and each later file covers the next two episodes.
        # Keep both versions by assigning explicit multi-episode ranges.
        new_edit_items = [
            item
            for item in unknown_media
            if entry_edition_tag(item) == "New Edit"
        ]
        if 1 in season_groups and new_edit_items:
            regular_first_season_keys = sorted({
                key.number
                for item in season_groups[1]
                if Path(str(item.get("name", ""))).suffix.lower() in VIDEO_EXTS
                and entry_edition_tag(item) is None
                and (key := extract_episode_key(str(item.get("name", "")))) is not None
                and key.kind == "regular"
                and not key.end_number
            })
            new_edit_keys = sorted({
                key.number
                for item in new_edit_items
                if Path(str(item.get("name", ""))).suffix.lower() in VIDEO_EXTS
                and (key := extract_episode_key(str(item.get("name", "")))) is not None
                and key.kind == "regular"
                and not key.end_number
            })
            if (
                regular_first_season_keys
                and new_edit_keys
                and regular_first_season_keys
                == list(range(1, len(regular_first_season_keys) + 1))
                and new_edit_keys == list(range(1, len(new_edit_keys) + 1))
                and len(regular_first_season_keys) == 2 * len(new_edit_keys) - 1
            ):
                consumed_new_edit: set[str] = set()
                for original in new_edit_items:
                    item = dict(original)
                    source_key = extract_episode_key(str(item.get("name", "")))
                    if (
                        source_key is None
                        or source_key.kind != "regular"
                        or source_key.end_number
                    ):
                        continue
                    if source_key.number == 1:
                        start_episode = 1
                        end_episode = 0
                    else:
                        start_episode = source_key.number * 2 - 2
                        end_episode = start_episode + 1
                    item["_episode_kind_override"] = "regular"
                    item["_episode_key_override"] = start_episode
                    if end_episode:
                        item["_episode_end_override"] = end_episode
                    season_groups[1].append(item)
                    consumed_new_edit.add(str(item["full_path"]))
                unknown_media = [
                    item
                    for item in unknown_media
                    if str(item["full_path"]) not in consumed_new_edit
                ]
                edition_group_warnings[1].append(
                    f"已识别 {len(new_edit_keys)} 集 New Edit：第 1 集对应 S01E01，"
                    f"其后按双集重编范围对应至 S01E{len(regular_first_season_keys):02d}；"
                    "保留为 {edition-New Edit}，未改称 Director's Cut"
                )

        # Some TMDB records expose every broadcast cour under one long season,
        # while release folders reset numbering per cour (for example White
        # Album and Oshi no Ko).  When the source groups collectively cover the
        # exact official episode count, concatenate them in directory-season
        # order instead of requesting non-existent TMDB seasons.
        #
        # Before packing, verify that a folder labelled “第二季” is not actually
        # an independently catalogued sequel.  White Album 2 is the canonical
        # counterexample: the shelf calls it the second season, but TMDB stores
        # it as a different TV work.  A different id is accepted only when the
        # release title itself matches at high confidence and its exact episode
        # count agrees, so ordinary multi-season shows remain grouped.
        divergent_seasons: list[int] = []
        if len(season_groups) >= 2 and not packed_single_season:
            for source_season, group_items in sorted(season_groups.items()):
                if source_season <= min(season_groups):
                    continue
                videos = [
                    item
                    for item in group_items
                    if Path(str(item.get("name", ""))).suffix.lower() in VIDEO_EXTS
                ]
                episode_count = len({
                    key.number
                    for item in videos
                    if (key := extract_episode_key(str(item.get("name", "")))) is not None
                    and key.kind == "regular"
                }) or None
                if not videos or episode_count is None:
                    continue
                release_queries = _season_parent_identity_queries(
                    videos,
                    source_root=source_root,
                    season_number=source_season,
                    show=show_for_season_names,
                )
                for release_query in release_queries:
                    try:
                        candidate, _ = auto_match_tmdb(
                            kwargs["tmdb_client"],
                            release_query,
                            media_type="tv",
                            min_confidence=0.92,
                            prefer_animation=_media_context_from_source_and_target(
                                source_root,
                                str(kwargs["parent_path"]),
                            )[1],
                            expected_episode_count=episode_count,
                        )
                        if candidate.status != "confirmed":
                            continue
                    except ScraperError:
                        continue
                    if candidate.tmdb_id != kwargs.get("tmdb_id"):
                        divergent_seasons.append(source_season)
                        break
        for source_season in divergent_seasons:
            unknown_media.extend(season_groups.pop(source_season))

        if len(positive_seasons) == 1 and len(season_groups) >= 2:
            official_season = int(positive_seasons[0]["season_number"])
            official_count = int(positive_seasons[0]["episode_count"])
            direct_long_season_warnings = (
                _merge_broadcast_folders_into_long_tmdb_season(
                    season_groups,
                    official_season=official_season,
                    block_counts=official_long_season_block_counts,
                )
            )
            if direct_long_season_warnings:
                edition_group_warnings[official_season].extend(
                    direct_long_season_warnings
                )
                if len(season_groups) == 1:
                    packed_single_season = True
            # A release may place both locally numbered episodes (01..N) and
            # a lower-resolution whole-series numbering (prior+1..prior+N)
            # inside the same explicit broadcast-season folder. Fold only the
            # cumulative copy for which a complete local TMDB block already
            # proves the boundary; partial local runs remain untouched.
            for source_season, group_items in season_groups.items():
                if not (1 < source_season <= len(official_long_season_block_counts)):
                    continue
                block_count = official_long_season_block_counts[source_season - 1]
                prior_count = sum(official_long_season_block_counts[: source_season - 1])
                raw_video_numbers = {
                    key.number
                    for item in group_items
                    if Path(str(item.get("name", ""))).suffix.lower() in VIDEO_EXTS
                    and (key := extract_episode_key(str(item.get("name", "")))) is not None
                    and key.kind == "regular"
                    and not key.end_number
                }
                has_complete_local_run = set(
                    range(1, block_count + 1)
                ).issubset(raw_video_numbers)
                has_complete_cumulative_run = raw_video_numbers == set(
                    range(prior_count + 1, prior_count + block_count + 1)
                )
                if not (has_complete_local_run or has_complete_cumulative_run):
                    continue
                cumulative_numbers = {
                    number
                    for number in raw_video_numbers
                    if prior_count < number <= prior_count + block_count
                }
                if not cumulative_numbers:
                    continue
                for item in group_items:
                    key = extract_episode_key(str(item.get("name", "")))
                    if (
                        key is not None
                        and key.kind == "regular"
                        and not key.end_number
                        and key.number in cumulative_numbers
                    ):
                        item["_episode_key_override"] = key.number - prior_count
                edition_group_warnings[official_season].append(
                    (
                        f"同一播出季度同时包含完整本季编号 01–{block_count:02d} 与"
                        if has_complete_local_run else
                        "该播出季度完整使用"
                    )
                    + f"全剧累计编号 {prior_count + 1:02d}–"
                    f"{max(cumulative_numbers):02d}；已按 TMDB 长期断档边界"
                    + ("合并为同集版本" if has_complete_local_run else "换算为本季集号")
                )
            major_gap_warnings = (
                _merge_release_seasons_into_long_tmdb_season_by_major_gaps(
                    season_groups,
                    official_season=official_season,
                    official_episodes=official_long_season_episodes,
                )
            )
            if major_gap_warnings:
                edition_group_warnings[official_season].extend(major_gap_warnings)
                if len(season_groups) == 1:
                    packed_single_season = True
            group_keys: dict[int, list[int]] = {}
            for source_season, group_items in season_groups.items():
                group_keys[source_season] = sorted({
                    int(item.get("_episode_key_override", key.number))
                    for item in group_items
                    if Path(str(item.get("name", ""))).suffix.lower() in VIDEO_EXTS
                    and (key := extract_episode_key(str(item.get("name", "")))) is not None
                    and key.kind == "regular"
                    and not key.end_number
                })
            source_season_numbers = sorted(group_keys)
            exact_complete_match = (
                sum(len(keys) for keys in group_keys.values()) == official_count
            )
            broadcast_prefix_match = (
                source_season_numbers
                == list(range(1, len(source_season_numbers) + 1))
                and len(source_season_numbers)
                <= len(official_long_season_block_counts)
                and all(
                    len(group_keys[season_number])
                    == official_long_season_block_counts[season_number - 1]
                    for season_number in source_season_numbers[:-1]
                )
                and bool(source_season_numbers)
                and 0 < len(group_keys[source_season_numbers[-1]])
                <= official_long_season_block_counts[source_season_numbers[-1] - 1]
            )
            if (
                all(group_keys.values())
                and (exact_complete_match or broadcast_prefix_match)
                and all(
                    keys == list(range(1, len(keys) + 1))
                    for keys in group_keys.values()
                )
            ):
                packed: list[dict[str, Any]] = []
                next_episode = 1
                for source_season in sorted(season_groups):
                    key_map = {
                        source_key: next_episode + index
                        for index, source_key in enumerate(group_keys[source_season])
                    }
                    next_episode += len(key_map)
                    for original in season_groups[source_season]:
                        item = dict(original)
                        source_key = extract_episode_key(str(item.get("name", "")))
                        if (
                            source_key is not None
                            and source_key.kind == "regular"
                        ):
                            logical_key = int(
                                item.get("_episode_key_override", source_key.number)
                            )
                            mapped_key = key_map.get(logical_key)
                            if mapped_key is not None:
                                item["_episode_key_override"] = mapped_key
                        packed.append(item)
                season_groups = defaultdict(list, {official_season: packed})
                packed_single_season = True
                if broadcast_prefix_match and not exact_complete_match:
                    completed_blocks = ", ".join(
                        str(value)
                        for value in official_long_season_block_counts[
                            : len(source_season_numbers) - 1
                        ]
                    )
                    latest_count = len(group_keys[source_season_numbers[-1]])
                    latest_total = official_long_season_block_counts[
                        source_season_numbers[-1] - 1
                    ]
                    edition_group_warnings[official_season].append(
                        "TMDB 将多个播出季度连续编号在同一 Season；已依据官方播出日期的"
                        f"长期断档边界合并完整前序季度（{completed_blocks or '无'} 集）并"
                        f"保留最新季度当前 {latest_count}/{latest_total} 集，未要求未播内容"
                    )
            else:
                long_season_pack_diagnostic = (
                    "TMDB 长季播出块="
                    f"{official_long_season_block_counts or '无可验证断档'}；"
                    "源季度集号="
                    + ", ".join(
                        f"S{number:02d}:{keys}"
                        for number, keys in sorted(group_keys.items())
                    )
                    + f"；完整总数匹配={exact_complete_match}，"
                    f"播出前缀匹配={broadcast_prefix_match}"
                )
                edition_group_warnings[official_season].append(
                    "TMDB 长季自动合并未通过：" + long_season_pack_diagnostic
                )
        # If the source groups cannot be packed into TMDB's one official
        # season, an out-of-range “第二季” may actually be an independent sequel
        # work.  Move only those impossible season groups through the child
        # title matcher instead of calling a known-nonexistent season endpoint.
        if not packed_single_season and official_positive_season_numbers:
            impossible_seasons = [
                season_number
                for season_number in season_groups
                if season_number not in official_positive_season_numbers
            ]
            for season_number in impossible_seasons:
                impossible_items = season_groups.pop(season_number)
                if season_number > max(official_positive_season_numbers):
                    out_of_range_season_by_path.update({
                        str(item["full_path"]): season_number
                        for item in impossible_items
                    })
                unknown_media.extend(impossible_items)
        if unknown_media:
            numbered_collection_groups, unknown_media, collection_warnings = (
                _resolve_numbered_movie_collection_groups(
                    kwargs["tmdb_client"],
                    unknown_media,
                )
            )
            for movie_id, collection_files in numbered_collection_groups.items():
                movie_groups[movie_id].extend(collection_files)
            special_release_warnings.extend(collection_warnings)
        if unknown_media:
            top_level_groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
            for item in unknown_media:
                full_path = str(item["full_path"])
                relative = full_path[len(source_root.rstrip("/")) :].lstrip("/")
                parts = relative.split("/")
                directory_parts = parts[:-1]
                while directory_parts and (
                    re.fullmatch(
                        r"(?:4k|8k|1080p|2160p)?\s*(?:动漫|动画|备份版|备份|资源)?",
                        directory_parts[0],
                        flags=re.IGNORECASE,
                    )
                    or (
                        re.search(
                            r"(?:4k|8k|2160p|1080p|720p)",
                            directory_parts[0],
                            flags=re.IGNORECASE,
                        )
                        and re.search(
                            r"(?:字幕|内封|内嵌|外挂|硬字|软字|版本|压制)",
                            directory_parts[0],
                            flags=re.IGNORECASE,
                        )
                    )
                ):
                    directory_parts = directory_parts[1:]
                if (
                    directory_parts
                    and directory_parts[0] not in {"备份字幕", "字幕", "Subtitles"}
                ):
                    top_level_groups[directory_parts[0]].append(item)
            consumed: set[str] = set()
            staged_child_paths: set[str] = set()
            child_tv_sources: dict[int, list[dict[str, Any]]] = defaultdict(list)
            backup_subtitle_owners = _unique_backup_subtitle_release_owners(
                top_level_groups,
                unknown_media,
            )

            def companion_subtitles(
                segment: str,
                videos: Sequence[Mapping[str, Any]],
            ) -> list[dict[str, Any]]:
                del videos
                return [
                    item for item in unknown_media
                    if Path(str(item.get("name", ""))).suffix.lower() in SUBTITLE_EXTS
                    and str(item.get("full_path", "")) not in consumed
                    and str(item.get("full_path", "")) not in staged_child_paths
                    and backup_subtitle_owners.get(str(item.get("full_path", "")))
                    == segment
                ]

            for segment, group_items in sorted(top_level_groups.items()):
                videos = [
                    item for item in group_items
                    if Path(str(item.get("name", ""))).suffix.lower() in VIDEO_EXTS
                ]
                if not videos:
                    continue
                try:
                    child_queries = list(dict.fromkeys([
                        _query_from_source(join_remote(source_root, segment)),
                        *(
                            query
                            for item in videos[:3]
                            for query in _movie_queries_from_item(item)
                        ),
                    ]))
                    child_episode_count = len({
                        key.number
                        for item in videos
                        if (key := extract_episode_key(str(item.get("name", "")))) is not None
                        and key.kind == "regular"
                    }) or None
                    child_match = None
                    matched_parent = False
                    last_child_error: ScraperError | None = None
                    for child_query in child_queries:
                        try:
                            candidate, _ = auto_match_tmdb(
                                kwargs["tmdb_client"], child_query,
                                media_type="tv", min_confidence=0.88,
                                prefer_animation=_media_context_from_source_and_target(
                                    source_root,
                                    str(kwargs["parent_path"]),
                                )[1],
                                expected_episode_count=child_episode_count,
                            )
                        except PlanError as exc:
                            last_child_error = exc
                            continue
                        if candidate.status != "confirmed":
                            continue
                        if candidate.tmdb_id == kwargs.get("tmdb_id"):
                            # Do not collapse an explicit future S04 folder
                            # into the parent's sole published Season 01 just
                            # because both happen to contain E01..EN.  The
                            # explicit out-of-range marker is stronger than an
                                # equal episode count and must remain diagnosable.
                            if any(
                                str(video.get("full_path", ""))
                                in out_of_range_season_by_path
                                for video in videos
                            ):
                                continue
                            requested_parent_season = int(
                                kwargs.get("season") or 1
                            )
                            requested_meta = next(
                                (
                                    item for item in positive_seasons
                                    if int(item["season_number"])
                                    == requested_parent_season
                                ),
                                None,
                            )
                            if requested_meta is None:
                                if len(positive_seasons) != 1:
                                    continue
                                requested_meta = positive_seasons[0]
                            parent_season = int(requested_meta["season_number"])
                            parent_count = int(requested_meta["episode_count"])
                            video_numbers = [
                                key.number
                                for video in videos
                                if (key := extract_episode_key(str(video.get("name", "")))) is not None
                                and key.kind == "regular"
                                and not key.end_number
                            ]
                            if (
                                len(video_numbers) != parent_count
                                or sorted(video_numbers) != list(range(1, parent_count + 1))
                            ):
                                continue
                            parent_files = [
                                *group_items,
                                *companion_subtitles(segment, videos),
                            ]
                            parent_files = list({
                                str(item["full_path"]): item for item in parent_files
                            }.values())
                            season_groups[parent_season].extend(parent_files)
                            consumed.update(str(item["full_path"]) for item in parent_files)
                            staged_child_paths.update(str(item["full_path"]) for item in parent_files)
                            matched_parent = True
                            break
                        child_match = candidate
                        break
                    if matched_parent:
                        continue
                    if child_match is None:
                        for child_query in child_queries:
                            try:
                                child_match, _ = auto_match_tmdb(
                                    kwargs["tmdb_client"], child_query,
                                    media_type="movie", min_confidence=0.88,
                                    prefer_animation=_media_context_from_source_and_target(
                                        source_root,
                                        str(kwargs["parent_path"]),
                                    )[1],
                                )
                                if child_match.status != "confirmed":
                                    child_match = None
                                    continue
                                break
                            except PlanError as exc:
                                last_child_error = exc
                    if child_match is None:
                        raise last_child_error or PlanError(
                            f"无法识别子作品目录: {segment}"
                        )
                    if child_match.media_type == "movie":
                        movie_groups[child_match.tmdb_id].extend(group_items)
                        consumed.update(str(item["full_path"]) for item in group_items)
                        continue
                    companions = companion_subtitles(segment, videos)
                    child_files = list({str(item["full_path"]): item for item in [*group_items, *companions]}.values())
                    # Several release roots can represent different
                    # resolutions of the same independently identified
                    # spin-off. Stage them by TMDB id and plan them together so
                    # the normal quality pass can compare 2160p and 1080p
                    # counterparts before target-name validation.
                    child_tv_sources[child_match.tmdb_id].extend(child_files)
                    staged_child_paths.update(
                        str(item["full_path"]) for item in child_files
                    )
                except PlanError:
                    continue
            for child_tmdb_id, staged_files in sorted(child_tv_sources.items()):
                child_files = list({
                    str(item["full_path"]): item for item in staged_files
                }.values())
                try:
                    # The child has already been identified as an independent
                    # work.  Do not re-interpret its enclosing shelf label
                    # (for example “第二季”) as the child's own TMDB season;
                    # White Album 2 starts at Season 01 in its separate record.
                    child_args = dict(
                        alist=kwargs["alist"], tmdb_client=kwargs["tmdb_client"],
                        src_path=source_root, parent_path=kwargs["parent_path"],
                        tmdb_id=child_tmdb_id, season=1, absolute=False,
                        prefer_simplified=bool(kwargs.get("prefer_simplified")),
                        allow_unmapped=False,
                        ignore_orphan_temp=bool(kwargs.get("ignore_orphan_temp")),
                        episode_map_path=None, episode_group_id=None,
                        source_files=child_files,
                        media_root=kwargs.get("media_root"),
                    )
                    try:
                        child_plan = build_tv_plan(
                            **child_args,
                            auto_special_title_match=True,
                            auto_align_subtitles=True,
                        )
                    except PlanError:
                        child_plan = build_tv_plan_smart(
                            auto_episode_mode=True,
                            **child_args,
                        )
                except PlanError:
                    continue
                child_tv_plans.append(child_plan)
                consumed.update(str(item["full_path"]) for item in child_files)
            unknown_media = [item for item in unknown_media if str(item["full_path"]) not in consumed]
        if unknown_media:
            numbered_groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
            for item in unknown_media:
                key = extract_episode_key(str(item.get("name", "")))
                queries = _movie_queries_from_item(item)
                if key and key.kind == "regular" and queries:
                    numbered_groups[_normalize_match_title(queries[0])].append(item)
            consumed_numbered: set[str] = set()
            for group_items in numbered_groups.values():
                videos = [
                    item for item in group_items
                    if Path(str(item.get("name", ""))).suffix.lower() in VIDEO_EXTS
                ]
                video_keys = sorted(
                    {
                        key.number
                        for item in videos
                        if (key := extract_episode_key(str(item.get("name", "")))) is not None
                    }
                )
                if len(videos) < 2 or video_keys != list(range(1, len(video_keys) + 1)):
                    continue
                query = _movie_queries_from_item(videos[0])[0]
                results = [
                    item for item in (
                        kwargs["tmdb_client"].get("/search/movie", query=query).get("results") or []
                    )
                    if isinstance(item, Mapping)
                    and not isinstance(item.get("id"), bool)
                    and str(item.get("release_date") or "")
                ]
                if len(results) != len(video_keys):
                    continue
                query_key = _normalize_match_title(query)
                results_have_title_evidence = True
                for result in results:
                    result_id = int(result["id"])
                    official_titles = _search_item_titles(result, "movie")
                    aliases = _alternative_tmdb_titles(
                        kwargs["tmdb_client"], "movie", result_id
                    )
                    all_titles = [*official_titles, *aliases]
                    best_title_score = max(
                        (_title_similarity(query_key, title) for title in all_titles),
                        default=0.0,
                    )
                    if best_title_score < 0.88 or (
                        _cross_script_unique_match(query, all_titles)
                        and not any(
                            _title_similarity(query_key, alias) >= 0.88
                            for alias in aliases
                        )
                    ):
                        results_have_title_evidence = False
                        break
                if not results_have_title_evidence:
                    continue
                ordered_results = sorted(
                    results,
                    key=lambda item: (str(item.get("release_date") or ""), int(item["id"])),
                )
                for number, result in zip(video_keys, ordered_results):
                    members = [
                        item for item in group_items
                        if (key := extract_episode_key(str(item.get("name", "")))) is not None
                        and key.number == number
                    ]
                    movie_groups[int(result["id"])].extend(members)
                    consumed_numbered.update(str(item["full_path"]) for item in members)
                special_release_warnings.append(
                    f"编号 01–{len(video_keys):02d} 的完整视频序列与 TMDB "
                    f"{len(ordered_results)} 部电影的官方标题/别名逐一一致；"
                    "已仅按官方上映日期顺序建立电影归属"
                )
            unknown_media = [
                item for item in unknown_media
                if str(item["full_path"]) not in consumed_numbered
            ]
        if movie_groups and unknown_media:
            unknown_media = _attach_unique_movie_subtitles(
                movie_groups,
                unknown_media,
                tmdb_client=kwargs["tmdb_client"],
                prefer_animation=_media_context_from_source_and_target(
                    source_root,
                    str(kwargs["parent_path"]),
                )[1],
            )
        if movie_groups and season_groups:
            for season_number, season_items in list(season_groups.items()):
                fractional_items = [
                    item for item in season_items
                    if (
                        (key := extract_episode_key(str(item.get("name", ""))))
                        is not None and key.kind == "fractional"
                    )
                ]
                if not fractional_items:
                    continue
                retained_fractional, fractional_movie_warnings = (
                    _attach_fractional_feature_by_ass_title_to_movie_groups(
                        kwargs["alist"], kwargs["tmdb_client"], movie_groups,
                        fractional_items, files,
                    )
                )
                fractional_paths = {
                    _collision_key(str(item.get("full_path", "")))
                    for item in fractional_items
                }
                season_groups[season_number] = [
                    item for item in season_items
                    if _collision_key(str(item.get("full_path", "")))
                    not in fractional_paths
                ] + retained_fractional
                special_release_warnings.extend(fractional_movie_warnings)
        if movie_groups and special_files:
            special_files, fractional_movie_warnings = (
                _attach_fractional_feature_by_ass_title_to_movie_groups(
                    kwargs["alist"],
                    kwargs["tmdb_client"],
                    movie_groups,
                    special_files,
                    files,
                )
            )
            special_release_warnings.extend(fractional_movie_warnings)
            special_files = _attach_unique_movie_subtitles(
                movie_groups,
                special_files,
                tmdb_client=kwargs["tmdb_client"],
                prefer_animation=_media_context_from_source_and_target(
                    source_root,
                    str(kwargs["parent_path"]),
                )[1],
            )
        # A clearly labelled season beyond TMDB's currently published range is
        # not a parser failure.  First give it the normal child-work matcher
        # above (some shelves call an independently catalogued sequel “S2”).
        # If no independent work can be proven, keep the files in place as
        # diagnostic rows instead of aborting every already verified season.
        if out_of_range_season_by_path and unknown_media:
            still_unknown: list[dict[str, Any]] = []
            for item in unknown_media:
                path = str(item["full_path"])
                season_number = out_of_range_season_by_path.get(path)
                if season_number is None:
                    still_unknown.append(item)
                else:
                    retained_unpublished_season_media.append((item, season_number))
            unknown_media = still_unknown
        if (
            packed_single_season
            and official_long_season_block_counts
            and unknown_media
        ):
            attached_root_block, remaining_unknown = (
                _proven_root_first_broadcast_block_files(
                    source_root,
                    unknown_media,
                    official_long_season_block_counts[0],
                )
            )
            if attached_root_block:
                season_groups[int(positive_seasons[0]["season_number"])].extend(
                    attached_root_block
                )
                unknown_media = remaining_unknown
                edition_group_warnings[int(positive_seasons[0]["season_number"])].append(
                    "源根目录完整覆盖 TMDB 长季的第一播出块；已作为该块的同集发行版本"
                )
        if season_groups and unknown_media:
            missing_season, attached_root_files, remaining_unknown = (
                _proven_missing_root_season_files(
                    source_root,
                    unknown_media,
                    season_groups,
                    official_season_counts,
                )
            )
            if missing_season is not None:
                season_groups[missing_season].extend(attached_root_files)
                unknown_media = remaining_unknown
                edition_group_warnings[missing_season].append(
                    f"源根目录完整覆盖官方第 {missing_season} 季 E01–"
                    f"E{official_season_counts[missing_season]:02d}，且这是唯一尚未"
                    "由明确季度目录覆盖的官方季；已按缺失季度边界归入"
                )
        if season_groups and unknown_media:
            # A release may flatten a complete alternate subtitle track into
            # ``备份字幕`` without repeating the season marker in each
            # filename.  Assign such a track only from an exact, unique
            # episode boundary: the subtitle set and the already identified
            # videos must both cover E01..EN, and exactly one official season
            # may have that N.  Partial sets and equal-length seasons remain
            # unresolved instead of being guessed by title similarity.
            subtitle_directories: dict[str, list[dict[str, Any]]] = defaultdict(list)
            for item in unknown_media:
                if Path(str(item.get("name", ""))).suffix.lower() not in SUBTITLE_EXTS:
                    continue
                full_path = str(item.get("full_path", ""))
                parent_directory, _ = split_remote(full_path)
                subtitle_directories[parent_directory].append(item)
            attached_subtitles: set[str] = set()
            for directory, subtitle_items in subtitle_directories.items():
                subtitle_numbers = {
                    key.number
                    for item in subtitle_items
                    if (key := extract_episode_key(str(item.get("name", "")))) is not None
                    and key.kind == "regular"
                    and not key.end_number
                    and key.number > 0
                }
                if not subtitle_numbers or subtitle_numbers != set(
                    range(1, max(subtitle_numbers) + 1)
                ):
                    continue
                matching_seasons: list[int] = []
                for season_number, season_items in season_groups.items():
                    official_count = official_season_counts.get(season_number)
                    if official_count != max(subtitle_numbers):
                        continue
                    video_numbers = {
                        int(item.get("_episode_key_override") or key.number)
                        for item in season_items
                        if Path(str(item.get("name", ""))).suffix.lower() in VIDEO_EXTS
                        and (key := extract_episode_key(str(item.get("name", "")))) is not None
                        and key.kind == "regular"
                        and not key.end_number
                    }
                    if video_numbers == subtitle_numbers:
                        matching_seasons.append(season_number)
                if len(matching_seasons) != 1:
                    continue
                matched_season = matching_seasons[0]
                season_groups[matched_season].extend(subtitle_items)
                attached_subtitles.update(
                    str(item["full_path"]) for item in subtitle_items
                )
                edition_group_warnings[matched_season].append(
                    f"备份字幕目录 {directory} 完整覆盖 E01-E{max(subtitle_numbers):02d}，"
                    f"且仅与第 {matched_season} 季的官方边界及视频集合唯一一致；"
                    "已作为该季的完整备份字幕轨"
                )
            if attached_subtitles:
                unknown_media = [
                    item for item in unknown_media
                    if str(item["full_path"]) not in attached_subtitles
                ]
        if season_groups and unknown_media:
            unknown_media, attached_backup_subtitle_count = (
                _attach_unique_numbered_backup_subtitles(
                    unknown_media,
                    season_groups,
                    official_season_counts,
                )
            )
            if attached_backup_subtitle_count:
                special_release_warnings.append(
                    f"{attached_backup_subtitle_count} 个分散备份字幕已通过同集号、"
                    "同发行标题和唯一季度视频对应归属"
                )
        if len(season_groups) >= 2 or (season_groups and movie_groups):
            if unknown_media:
                if long_season_pack_diagnostic:
                    preview = ", ".join(
                        str(item["full_path"]) for item in unknown_media[:5]
                    )
                    suffix = (
                        f"，另有 {len(unknown_media) - 5} 个"
                        if len(unknown_media) > 5 else ""
                    )
                    raise PlanError(
                        "发现无法识别多季度目录中的季度编号的媒体文件；"
                        + long_season_pack_diagnostic
                        + f"；问题文件: {preview}{suffix}"
                    )
                _raise_unparsed_media(
                    [str(item["full_path"]) for item in unknown_media],
                    "多季度目录中的季度",
                )
        if (
            season_groups
            and special_files
            and not any(
                item.get("_episode_kind_override") == "special"
                for item in special_files
            )
        ):
            # Specials belong to the identified TV work, but their release
            # labels do not by themselves prove Season 00 placement.  Keep
            # them in the same evidence pass as the ordinary season.  Official
            # TMDB matches can still map them; unresolved SP/OVA/OAD/OAV files
            # become retained problem rows instead of an empty standalone
            # sub-plan that aborts the whole series batch.
            season_groups[min(season_groups)].extend(special_files)
            special_files = []

        # A season directory can legitimately contain only external subtitle
        # sidecars while every video for that published season is absent.  It
        # is not an executable TV sub-plan: handing it to ``build_tv_plan``
        # turns the whole otherwise-valid multi-season work into the generic
        # "no video" failure.  Keep the sidecars in their source directory,
        # surface that decision in the persisted plan, and let the final
        # official-season audit create the corresponding media gap.  This is
        # deliberately narrow: an unplayable group containing anything other
        # than subtitles remains a planning error, and a source with no
        # executable season at all still fails closed below.
        preserved_subtitle_only_seasons: dict[int, list[dict[str, Any]]] = {}
        if season_groups and any(
            any(
                Path(str(item.get("name", ""))).suffix.lower() in VIDEO_EXTS
                for item in group
            )
            for group in season_groups.values()
        ):
            executable_season_groups: dict[int, list[dict[str, Any]]] = {}
            for season_number, group in season_groups.items():
                has_video = any(
                    Path(str(item.get("name", ""))).suffix.lower() in VIDEO_EXTS
                    for item in group
                )
                if has_video:
                    executable_season_groups[season_number] = group
                    continue
                if (
                    season_number in source_declared_seasons
                    and group
                    and all(
                    Path(str(item.get("name", ""))).suffix.lower() in SUBTITLE_EXTS
                    for item in group
                    )
                ):
                    preserved_subtitle_only_seasons[season_number] = list(group)
                    continue
                executable_season_groups[season_number] = group
            if preserved_subtitle_only_seasons:
                season_groups = defaultdict(list, executable_season_groups)
                preserved_numbers = ", ".join(
                    f"{number:02d}" for number in sorted(preserved_subtitle_only_seasons)
                )
                special_release_warnings.append(
                    "以下季度只有外挂字幕、未发现可执行视频；字幕已保留在来源，"
                    "将由官方季集核对登记视频缺口: Season " + preserved_numbers
                )
        # A single-directory whole-series counter spanning every published
        # positive season (for example ``[01]..[48]`` for a 24+24 two-cour
        # show) is split by each season's episode count before the ordinary
        # season-group logic runs.  Non-episode companions stay with season 1.
        if len(season_groups) == 1 and len(positive_seasons) >= 2:
            _only_season, _only_group = next(iter(season_groups.items()))
            _keys = sorted({
                key.number
                for item in _only_group
                if (key := extract_episode_key(str(item.get("name", "")))) is not None
                and key.kind == "regular"
                and not key.end_number
            })
            _ordered = sorted(
                positive_seasons,
                key=lambda item: int(item["season_number"]),
            )
            _counts = [int(item["episode_count"]) for item in _ordered]
            if (
                _keys
                and _keys[0] == 1
                and _keys == list(range(1, _keys[-1] + 1))
                and _keys[-1] == sum(_counts)
            ):
                _split: dict[int, list[dict[str, Any]]] = defaultdict(list)
                _offset = 0
                for _season_item, _count in zip(_ordered, _counts):
                    _season_number = int(_season_item["season_number"])
                    _lo = _offset + 1
                    _hi = _offset + _count
                    _split[_season_number] = [
                        item
                        for item in _only_group
                        if (key := extract_episode_key(str(item.get("name", "")))) is not None
                        and key.kind == "regular"
                        and _lo <= key.number <= _hi
                    ]
                    _offset = _hi
                for item in _only_group:
                    if extract_episode_key(str(item.get("name", ""))) is None:
                        _split[int(_ordered[0]["season_number"])].append(item)
                season_groups = defaultdict(
                    list, {s: g for s, g in _split.items() if g}
                )
        should_split = bool(movie_groups or child_tv_plans) or (
            bool(season_groups)
            and (
                packed_single_season
                or
                len(season_groups) >= 2
                or bool(special_files)
                or bool(edition_group_warnings)
                or bool(preserved_subtitle_only_seasons)
                or bool(retained_future_media)
                or bool(retained_unpublished_season_media)
                or next(iter(season_groups)) != int(kwargs.get("season") or 1)
            )
        )
        if should_split:
            subplans: list[Plan] = []
            if season_groups:
                for season_number, season_files in sorted(season_groups.items()):
                    sub_kwargs = dict(smart_kwargs)
                    sub_kwargs["season"] = season_number
                    normalized_files, normalization_warning = (
                        _normalize_cumulative_season_episode_numbers(
                            season_number,
                            season_files,
                            positive_seasons,
                        )
                    )
                    sub_kwargs["source_files"] = normalized_files
                    subplan = build_tv_plan(**sub_kwargs)
                    for edition_warning in reversed(
                        edition_group_warnings.get(season_number, [])
                    ):
                        subplan.warnings.insert(0, edition_warning)
                    if normalization_warning:
                        subplan.warnings.insert(0, normalization_warning)
                    subplans.append(subplan)
                if special_files:
                    special_kwargs = dict(smart_kwargs)
                    special_kwargs["season"] = min(season_groups)
                    special_kwargs["source_files"] = special_files
                    subplans.append(build_tv_plan(**special_kwargs))
            else:
                consumed_paths = {
                    str(item["full_path"])
                    for group in movie_groups.values()
                    for item in group
                }
                consumed_paths.update(
                    item.source_path for plan in child_tv_plans for item in plan.files
                )
                main_kwargs = dict(smart_kwargs)
                main_kwargs["source_files"] = [
                    item for item in files
                    if str(item.get("full_path", "")) not in consumed_paths
                ]
                subplans.append(build_tv_plan(**main_kwargs))
            first = subplans[0]
            executable_movie_groups, orphan_movie_files = (
                _partition_movie_groups_with_video(movie_groups)
            )
            movie_parent = split_remote(first.target_root)[0]
            try:
                # A nested franchise root (for example ``/Fate``) can safely
                # contain TV and movie siblings under one lock.  If the TV is
                # already directly below a production category root, that
                # category itself is intentionally not lockable; keep the
                # movie in its own child directory below the TV root instead.
                placement_for(
                    source_root,
                    movie_parent,
                    media_root=kwargs.get("media_root"),
                )
            except ValueError:
                movie_parent = first.target_root
            movie_plans = [
                build_movie_plan(
                    kwargs["alist"],
                    kwargs["tmdb_client"],
                    src_path=source_root,
                    # Independent movies are siblings of the TV work, never
                    # children or loose files beside its ``tvshow.nfo``.
                    parent_path=movie_parent,
                    tmdb_id=movie_tmdb_id,
                    ignore_orphan_temp=bool(kwargs.get("ignore_orphan_temp")),
                    source_files=movie_files,
                    defer_validation=True,
                )
                for movie_tmdb_id, movie_files in sorted(executable_movie_groups.items())
            ]
            metadata = dict(first.metadata)
            if movie_plans:
                metadata["series_root"] = first.target_root
                metadata["member_posters"] = {
                    movie_plan.target_root: movie_plan.metadata["poster_path"]
                    for movie_plan in movie_plans
                    if isinstance(movie_plan.metadata.get("poster_path"), str)
                    and movie_plan.metadata.get("poster_path")
                }
                metadata["member_movies"] = {
                    movie_plan.target_root: {
                        "tmdb_id": movie_plan.metadata["tmdb_id"],
                        "title": movie_plan.metadata["title"],
                        "year": movie_plan.metadata["year"],
                    }
                    for movie_plan in movie_plans
                }
            combined_warnings = [
                *(
                    [f"已从目录结构识别并合并 {sum(number > 0 for number in season_groups)} 个季度"]
                    if season_groups else []
                ),
                *(
                    [
                        f"识别到 {len(movie_plans)} 部独立电影；"
                        "已保留独立 TMDB 身份并放入与电视剧作品目录"
                        "并列的独立电影目录"
                    ]
                    if movie_plans else []
                ),
                *([independent_e00_warning] if independent_e00_warning else []),
                *special_release_warnings,
                *(warning for subplan in subplans for warning in subplan.warnings),
                *(warning for movie_plan in movie_plans for warning in movie_plan.warnings),
            ]
            plan = Plan(
                mode="mixed" if movie_plans else "tv",
                source_root=source_root,
                target_root=(
                    normalize_remote_path(posixpath.commonpath([
                        first.target_root,
                        *(movie_plan.target_root for movie_plan in movie_plans),
                    ]))
                    if movie_plans else first.target_root
                ),
                files=[
                    item
                    for subplan in [*subplans, *movie_plans]
                    for item in subplan.files
                ],
                cleanup_files=_dedupe_cleanup_files([
                    *_planned_cleanup_files(files),
                    *(
                        item
                        for subplan in [*subplans, *movie_plans]
                        for item in subplan.cleanup_files
                    ),
                ]),
                problem_files=[
                    item
                    for subplan in [*subplans, *movie_plans]
                    for item in subplan.problem_files
                ] + [
                    PlannedProblem(
                        source_path=str(item["full_path"]),
                        reason="尚未在 TMDB 发布的未来剧场版；保留原位待人工确认",
                    )
                    for item in retained_future_media
                ] + [
                    PlannedProblem(
                        source_path=str(item["full_path"]),
                        reason=(
                            f"明确标记为第 {season_number} 季，但 TMDB 当前尚未发布"
                            "该季；保留原位待人工确认"
                        ),
                    )
                    for item, season_number in retained_unpublished_season_media
                ] + [
                    PlannedProblem(
                        source_path=str(item["full_path"]),
                        reason="已匹配到独立电影，但没有对应视频；保留原位待人工确认",
                    )
                    for item in orphan_movie_files
                ],
                warnings=list(dict.fromkeys(combined_warnings)),
                metadata=metadata,
                scan_report={
                    "resource_gaps": [
                        dict(gap)
                        for subplan in [*subplans, *movie_plans]
                        for gap in (subplan.scan_report.get("resource_gaps") or [])
                        if isinstance(gap, Mapping)
                    ],
                    "deferred_subtitle_only_seasons": [
                        {
                            "kind": "subtitle_only_declared_season",
                            "season": season_number,
                            "source_paths": sorted(
                                str(item["full_path"])
                                for item in season_files
                            ),
                            "reason": (
                                "该季未发现视频；外挂字幕保留在来源，"
                                "不作为无视频正式库写入"
                            ),
                        }
                        for season_number, season_files in sorted(
                            preserved_subtitle_only_seasons.items()
                        )
                    ],
                    "preserved_source_residuals": list(preserved_theme_residuals),
                },
            )
            missing_season_gaps = _tv_season_resource_gaps(
                kwargs["alist"],
                plan,
                series_dir=first.target_root,
                official_seasons=positive_seasons,
            )
            if missing_season_gaps:
                plan.scan_report.setdefault("resource_gaps", []).extend(
                    missing_season_gaps
                )
            if retained_future_media:
                plan.warnings.append(
                    f"{len(retained_future_media)} 个尚未在 TMDB 发布的未来剧场版文件"
                    "将保留原位待人工确认"
                )
            if orphan_movie_files:
                plan.warnings.append(
                    f"{len(orphan_movie_files)} 个独立电影字幕没有对应视频，"
                    "将保留原位待人工确认"
                )
            if retained_unpublished_season_media:
                retained_seasons = sorted({
                    season_number
                    for _, season_number in retained_unpublished_season_media
                })
                plan.warnings.append(
                    f"{len(retained_unpublished_season_media)} 个明确季度文件超出 "
                    f"TMDB 当前已发布范围（第 "
                    f"{', '.join(map(str, retained_seasons))} 季），"
                    "将保留原位待人工确认"
                )
            canonical_warnings: list[str] = []
            if movie_plans:
                # These movies were independently identified from tagged or
                # bounded source groups.  When a movie is nested under the main
                # TV work (``movie_parent == first.target_root``), the TV owns
                # the container root, so it must not be re-nested under a
                # same-named directory-only container; passing the TV identity
                # keeps its root stable while each film stays a child leaf.
                # A franchise-root sibling keeps the old sibling layout.
                root_identity = (
                    WorkIdentity("tmdb.tv", int(first.metadata["tmdb_id"]))
                    if movie_parent == first.target_root
                    else None
                )
                _movie_root, movie_tree_warnings, _movie_tree_posters = (
                    _plan_canonical_batch_tree(
                        [plan],
                        outer_root=movie_parent,
                        root_identity=root_identity,
                        allow_family_boundaries=False,
                    )
                )
                canonical_warnings.extend(movie_tree_warnings)
            if child_tv_plans:
                root_identity = WorkIdentity(
                    "tmdb.tv", int(first.metadata["tmdb_id"])
                )
                _canonical_root, child_tree_warnings, _canonical_posters = (
                    _plan_canonical_batch_tree(
                        [first, *child_tv_plans],
                        outer_root=first.target_root,
                        root_identity=root_identity,
                    )
                )
                canonical_warnings.extend(child_tree_warnings)
                if plan.mode == "mixed":
                    plan.metadata["series_root"] = first.target_root
                plan = _combine_plans_as_batch(
                    source_root, [plan, *child_tv_plans],
                    f"已识别主系列及 {len(child_tv_plans)} 个独立衍生剧集，并保留在主系列目录内",
                )
            if canonical_warnings:
                plan.warnings.extend(
                    warning
                    for warning in canonical_warnings
                    if warning not in plan.warnings
                )
            _dedupe_merged_tv_target_variants(plan)
            if preserved_theme_residuals:
                plan.scan_report.setdefault("preserved_source_residuals", []).extend(
                    item for item in preserved_theme_residuals
                    if item not in plan.scan_report["preserved_source_residuals"]
                )
            validate_plan(
                kwargs["alist"], plan,
                media_root=kwargs.get("media_root"),
            )
            return plan
    try:
        plan = build_tv_plan(**smart_kwargs)
        missing_season_gaps = _tv_season_resource_gaps(
            kwargs["alist"],
            plan,
            series_dir=plan.target_root,
            official_seasons=positive_seasons,
        )
        if missing_season_gaps:
            plan.scan_report.setdefault("resource_gaps", []).extend(
                missing_season_gaps
            )
        if preserved_theme_residuals:
            plan.scan_report.setdefault("preserved_source_residuals", []).extend(
                item for item in preserved_theme_residuals
                if item not in plan.scan_report.get("preserved_source_residuals", [])
            )
        return plan
    except PlanError as seasonal_error:
        can_retry = (
            auto_episode_mode
            and not kwargs.get("absolute")
            and kwargs.get("episode_map_path") is None
            and kwargs.get("episode_group_id") is None
            and "以下集数未在 TMDB 映射中找到" in str(seasonal_error)
        )
        if not can_retry:
            raise
        absolute_kwargs = dict(smart_kwargs)
        absolute_kwargs["absolute"] = True
        try:
            plan = build_tv_plan(**absolute_kwargs)
        except PlanError:
            raise seasonal_error
        plan.warnings.insert(
            0,
            "普通季度映射无法覆盖源集数，系统已验证并自动改用 TMDB 绝对集数映射；请在执行前核对季集结果",
        )
        return plan
