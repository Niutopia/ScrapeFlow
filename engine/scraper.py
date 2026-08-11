"""Public Engine planning API for ScrapeFlow's automatic workflow."""

from __future__ import annotations

from .scrapeflow.core import (
    AListClient,
    ApiError,
    PlanError,
    ScraperError,
    TMDBClient,
    _expected_single_tv_episode_count,
    _media_context_from_source_and_target,
    _query_from_source,
    _season_from_source,
    auto_match_tmdb,
    build_collection_plan,
    build_movie_plan,
    build_tv_plan_smart,
    extract_episode_key,
    join_remote,
    planned_artwork,
    planned_nfos,
    split_remote,
    subtitle_language,
    validate_plan,
)
from .scrapeflow.current_plan import finalize_plan, plan_from_dict, plan_to_dict
from .scrapeflow.models import (
    AutoMatch,
    EpisodeKey,
    Plan,
    PlanNotice,
    PlannedCleanup,
    PlannedFile,
    PlannedProblem,
)

__version__ = "4.0.0"

__all__ = [
    "AListClient",
    "ApiError",
    "AutoMatch",
    "EpisodeKey",
    "Plan",
    "PlanError",
    "PlanNotice",
    "PlannedCleanup",
    "PlannedFile",
    "PlannedProblem",
    "ScraperError",
    "TMDBClient",
    "_expected_single_tv_episode_count",
    "_media_context_from_source_and_target",
    "_query_from_source",
    "_season_from_source",
    "auto_match_tmdb",
    "build_collection_plan",
    "build_movie_plan",
    "build_tv_plan_smart",
    "extract_episode_key",
    "finalize_plan",
    "join_remote",
    "plan_from_dict",
    "plan_to_dict",
    "planned_artwork",
    "planned_nfos",
    "split_remote",
    "subtitle_language",
    "validate_plan",
]
