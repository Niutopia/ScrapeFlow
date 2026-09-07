#!/usr/bin/env python3
"""Local catalog + aria2 + AList replenishment adapter.

Search is read-only and returns only candidates bound to the request TMDB ID.
Acquisition downloads the selected torrent files into an isolated workspace,
verifies exact file indices and sizes, uploads canonical episode names into the
unscraped AList root, and verifies the remote rows.  Successful local staging
is retained until the coordinator proves formal-library convergence.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import time
from typing import Any, Callable, Mapping

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from engine.scrapeflow.provider_capabilities import (
    ACTIVE_PROVIDERS,
    ACQUISITION_TORRENT,
    PROVIDER_LOCAL_MAGNET,
    candidate_capability_error,
    provider_capability_snapshot,
)

# ---- shared infrastructure: extracted module, re-exported ----
from engine.tools.replenishment_common import (  # noqa: F401
    ReplenishmentDeliveryError,
    ReplenishmentCandidateError,
    ReplenishmentInfrastructureError,
    ReplenishmentPauseRequested,
    _pause_checkpoint,
    _AnchorParser,
    _DynamicSearchResult,
    _network_failure_code,
    _fetch_bytes,
    _acg_http_opener,
    MagnetMetadataUnavailable,
    _BDecoder,
    _torrent_manifest,
    _nyaa_torrent_mirror_url,
    _repair_nyaa_land_torrent_comment,
    _magnet_metadata,
    _download_torrent,
    _magnet_metadatas_batch,
    MAX_TORRENT_BYTES,
    _MAGNET_URI_PATTERN,
    _MAGNET_BOOTSTRAP_TRACKERS,
    _SAFE_INFRA_FAILURE_CODES,
    _HTTP_FAILURE_CODE_RE,
    _safe_infrastructure_failure_types,
    _bencode,
    _direct_download_env,
    _bounded_seconds,
    _SUPPLEMENTAL_VIDEO_PATH_RE,
    _is_supplemental_video_path,
    _is_ordinary_primary_video_path,
    _base32_infohash,
    SUBTITLE_EXTENSIONS,
    VIDEO_EXTENSIONS,
)
from engine.scrapeflow.replenishment_matching import (
    coverage_tokens as _coverage_tokens,
    expanded_episode_ids as _expanded_episode_ids,
    normalized_text as _normalized_text,
    season_markers as _season_markers,
)
from engine.scrapeflow.media_quality import (
    minimum_video_bytes,
    video_size_is_admissible,
)

from engine.scrapeflow.serialization import atomic_write_json


from engine.tools.replenishment_search_terms import (  # noqa: F401
    _animetosho_release_priority,
    _source_episode_release_priority,
    _animetosho_requested_gap_tokens,
    _S00_TITLE_PREFLIGHT_LIMIT,
    _S00_TITLE_PREFLIGHT_KEY_LIMIT,
    _S00_TITLE_PREFLIGHT_GENERIC_KEYS,
    _DMHY_MAX_QUERY_TERMS,
    _DMHY_MAX_QUERY_TERM_LENGTH,
    _NYAA_MAX_FEED_ROWS,
    _NYAA_MAX_LOGICAL_QUERY_TERMS,
    _NYAA_MAX_MANIFEST_INSPECTIONS,
    _NYAA_MAX_QUERY_TERMS,
    _NYAA_MAX_QUERY_TERM_LENGTH,
    _NYAA_MAX_RSS_ROWS_PER_QUERY,
    _catalog_torrent_candidate_variants,
    _compact_dynamic_search_terms,
    _dynamic_search_terms,
    _explicit_episode_search_terms,
    _identity_query_bases,
    _nyaa_logical_search_terms,
    _nyaa_release_priority,
    _nyaa_request_cursor,
    _nyaa_request_fingerprint,
    _nyaa_safe_query_term,
    _nyaa_search_terms,
    _optional_episode_title_search_terms,
    _positive_requested_seasons,
    _release_year_conflict,
    _requested_episode_targets,
)


from engine.tools.replenishment_search_sources import (  # noqa: F401
    ANIME_PLAIN_EPISODE_RE,
    OPTIONAL_CONTAINER_RE,
    OPTIONAL_EPISODE_RE,
    OPTIONAL_EXPLICIT_S00_RE,
    OPTIONAL_NEWLYWED_RE,
    OPTIONAL_PAST_ARC_NAME_RE,
    OPTIONAL_PAST_ARC_RE,
    OPTIONAL_RETROSPECTIVE_COLLECTION_RE,
    _ANIMETOSHO_MAX_PAGE,
    _ANIMETOSHO_MAX_PAGES_PER_RUN,
    _ANIMETOSHO_MAX_ROWS_PER_PAGE,
    _BITSEARCH_ENDPOINT,
    _BITSEARCH_MAX_ROWS,
    _BITSEARCH_SITE_TAG,
    _BITSEARCH_USER_AGENT,
    _COMPANION_LANGUAGE_SUFFIX_RE,
    _KNABEN_ENDPOINT,
    _SUBTITLE_LANGUAGE_SUFFIX_RE,
    _animetosho_request_cursor,
    _animetosho_request_fingerprint,
    _animetosho_search_terms,
    _bitsearch_page_rows,
    _broad_identity_alias_terms,
    _companion_member_core,
    _companion_members_share_trusted_identity,
    _dmhy_safe_query_term,
    _dmhy_search_terms,
    _episode_source_alias_ids,
    _explicit_subtitle_seasons,
    _gap_file_map,
    _general_index_row_relevant,
    _general_index_search_terms,
    _infohash_aliases,
    _knaben_page_rows,
    _local_torrent_available,
    _locator_infohash_aliases,
    _magnet_with_trackers,
    _mikan_search_terms,
    _new_media_companion_subtitle_map,
    _now_swarm_observation,
    _optional_bare_alias_terms,
    _optional_semantic_keys,
    _optional_series_title_search_terms,
    _primary_episode_manifest_member_is_safe,
    _prioritize_verified_s00_title_rows,
    _sanitize_episode_gap_mapping,
    _search_acg,
    _search_animetosho,
    _search_bitsearch,
    _search_dmhy,
    _search_index_opener,
    _search_knaben,
    _search_mikan,
    _search_nyaa,
    _search_subsplease,
    _search_tokyotosho,
    _source_episode_search_terms,
    _specific_s00_title_terms,
    _subsplease_magnet_manifest,
    _subtitle_path_has_trusted_request_identity,
    _subtitle_path_matches_audited_video,
    _subtitle_path_matches_language,
    _swarm_count,
    _swarm_epoch_iso,
    _swarm_payload,
    _torrent_candidate,
    _torrent_candidate_variants,
    _verified_s00_title_preflight_keys,
)


from engine.tools.replenishment_acquire import (  # noqa: F401
    _LOCAL_DOWNLOAD_FAILURE_SIGNATURES,
    _TASK_STAGING_PARENT,
    _acquire,
    _acquire_dispatch,
    _alist_client,
    _automatic_upload,
    _ensure_automatic_staging_root,
    _ffprobe_archive_video,
    _find_download,
    _find_retained_member,
    _looks_like_local_download_failure,
    _payload_bytes,
    _payload_is_complete,
    _preflight,
    _preflight_dispatch,
    _remote_upload_matches,
    _require_task_staging_root,
    _safe_name,
    _selected_companion_indices,
    _selected_indices,
    _selection_acquisition_kind,
    _verify_manifest,
    _verify_remote_uploads,
    _verify_video_payload,
    _wrapper_for_selections,
)


def _load(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"JSON 根节点需要是对象: {path}")
    return value


def _atomic_json(path: Path, value: Mapping[str, Any]) -> None:
    atomic_write_json(path, value, sort_keys=True)


def _catalog_path() -> Path | None:
    raw = os.getenv("SCRAPEFLOW_REPLENISHMENT_CATALOG", "").strip()
    if not raw:
        return None
    path = Path(raw)
    if not path.is_file():
        raise ValueError(f"补源候选目录不存在: {path}")
    return path


def _dynamic_search_timeout_seconds(request: Mapping[str, Any]) -> int:
    del request
    return _bounded_seconds(
        "SCRAPEFLOW_REPLENISHMENT_DYNAMIC_SEARCH_TIMEOUT", 45, 10, 300,
    )


def _search(request: Mapping[str, Any]) -> dict[str, Any]:
    """Search exact local Torrent sources only; never register a cloud bridge."""
    media = request.get("media") if isinstance(request.get("media"), Mapping) else {}
    tmdb_id = media.get("tmdb_id")
    if type(tmdb_id) is not int or tmdb_id <= 0:
        return {"version": 1, "candidates": [], "message": "请求缺少 TMDB 身份"}

    existing_rows = request.get("excluded_candidates")
    existing_rows = existing_rows if isinstance(existing_rows, list) else []
    existing_locators = {
        str(row.get("locator"))
        for row in existing_rows
        if isinstance(row, Mapping) and row.get("locator")
    }
    warnings: list[str] = []
    output: list[dict[str, Any]] = []
    try:
        catalog_path = _catalog_path()
        if catalog_path is not None:
            catalog = _load(catalog_path)
            projects = catalog.get("projects") if isinstance(catalog.get("projects"), Mapping) else {}
            project = projects.get(str(tmdb_id))
            raw = project.get("candidates") if isinstance(project, Mapping) else []
            if isinstance(raw, list):
                for row in raw:
                    if not isinstance(row, Mapping):
                        continue
                    candidate = dict(row)
                    if str(candidate.get("provider") or "") in ACTIVE_PROVIDERS:
                        for variant in _catalog_torrent_candidate_variants(candidate):
                            # The operator supplied this resource by hand; it
                            # expresses acquisition intent and outranks equal
                            # index-discovered rows in the selector.
                            variant["operator_supplied"] = True
                            output.append(variant)
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        warnings.append(f"已核验候选目录不可用: {type(exc).__name__}")

    source_specs: list[tuple[str, Callable[..., Any], bool, str, str]] = [
        ("AnimeTosho", _search_animetosho, True,
         "SCRAPEFLOW_REPLENISHMENT_ANIMETOSHO_SEARCH", "0"),
        ("TokyoTosho", _search_tokyotosho, True,
         "SCRAPEFLOW_REPLENISHMENT_TOKYOTOSHO_SEARCH", "0"),
        ("SubsPlease", _search_subsplease, False,
         "SCRAPEFLOW_REPLENISHMENT_SUBSPLEASE_SEARCH", "0"),
        ("Mikan", _search_mikan, False,
         "SCRAPEFLOW_REPLENISHMENT_MIKAN_SEARCH", "0"),
        ("DMHY", _search_dmhy, False,
         "SCRAPEFLOW_REPLENISHMENT_DMHY_SEARCH", "0"),
        # Nyaa remains optional so local tests and installations without that
        # source can keep the search set explicit.
        ("Nyaa", _search_nyaa, False,
         "SCRAPEFLOW_REPLENISHMENT_NYAA_SEARCH", "1"),
        # The general-purpose (non-anime) magnet indexes: without them the
        # magnet tier has no real source for movie/US-TV works at all.  Two
        # independent fetch paths for the same shelf keep one blocked index
        # from silencing the lane.
        ("BitSearch", _search_bitsearch, False,
         "SCRAPEFLOW_REPLENISHMENT_BITSEARCH_SEARCH", "1"),
        ("Knaben", _search_knaben, False,
         "SCRAPEFLOW_REPLENISHMENT_KNABEN_SEARCH", "1"),
        ("ACG", _search_acg, True,
         "SCRAPEFLOW_REPLENISHMENT_ACG_SEARCH", "1"),
    ]
    # Sources are queried serially but receive equal, bounded read-only
    # windows.  Sharing one deadline here meant an early source could consume
    # the whole budget and leave later enabled indexes with zero attempts;
    # that is not evidence that those sources were searched.
    source_timeout_seconds = _dynamic_search_timeout_seconds(request)
    telemetry: dict[str, Any] = {}
    for label, searcher, required, env_name, default_enabled in source_specs:
        enabled = os.getenv(env_name, default_enabled).strip().casefold() not in {
            "0", "false", "no", "off", "",
        }
        if not enabled:
            # Keep disabled sources visible to the evidence boundary.  A
            # dynamic proof must distinguish an actually searched source set
            # from a deployment where every index was disabled.
            telemetry[label] = {
                "configured": False,
                "status": "incomplete",
                "query_attempts": 0,
                "query_responses": 0,
                "source_exhausted": False,
                "resource_failed_locators": [],
                "infrastructure_failures": 0,
                "infrastructure_failure_types": {},
                "required": required,
            }
            continue
        try:
            source_deadline = time.monotonic() + source_timeout_seconds
            result = searcher(
                request,
                existing_locators,
                deadline=source_deadline,
            )
            rows = [dict(row) for row in result if isinstance(row, Mapping)]
            rows = [
                row for row in rows
                if str(row.get("provider") or "") in ACTIVE_PROVIDERS
                and candidate_capability_error(row) is None
            ]
            output.extend(row for row in rows if str(row.get("locator") or "") not in existing_locators)
            query_attempts = int(getattr(result, "query_attempts", 0))
            query_responses = int(getattr(result, "query_responses", 0))
            source_exhausted = bool(getattr(result, "source_exhausted", False))
            infrastructure_failure_types = _safe_infrastructure_failure_types(
                getattr(result, "infrastructure_failure_types", None),
            )
            infrastructure_failures = int(
                getattr(result, "infrastructure_failures", 0),
            )
            infrastructure_failures = max(
                0,
                infrastructure_failures,
                sum(infrastructure_failure_types.values()),
            )
            telemetry[label] = {
                "configured": True,
                "status": (
                    "complete"
                    if source_exhausted
                    and infrastructure_failures == 0
                    and query_attempts > 0
                    and query_responses > 0
                    else "incomplete"
                ),
                "query_attempts": query_attempts,
                "query_responses": query_responses,
                "source_exhausted": source_exhausted,
                "resource_failed_locators": [
                    str(value) for value in getattr(result, "resource_failed_locators", [])
                    if str(value).startswith("torrent:")
                ],
                "reviewed_torrent_miss_locators": [
                    str(value)
                    for value in getattr(result, "reviewed_torrent_miss_locators", [])
                    if str(value).startswith("torrent:")
                ],
                "infrastructure_failures": infrastructure_failures,
                "infrastructure_failure_types": infrastructure_failure_types,
                "required": required,
            }
            cursor = getattr(result, "query_cursor", None)
            if isinstance(cursor, Mapping):
                telemetry[label]["query_cursor"] = dict(cursor)
        except Exception as exc:
            telemetry[label] = {
                "configured": True,
                "status": "incomplete",
                "query_attempts": 0,
                "query_responses": 0,
                "source_exhausted": False, "required": required,
                "infrastructure_failures": 1,
                "infrastructure_failure_types": {"source_error": 1},
                "error_type": type(exc).__name__,
            }
            warnings.append(f"{label} 搜索不可用: {type(exc).__name__}")

    deduplicated: dict[tuple[str, str], dict[str, Any]] = {}
    for row in output:
        provider = str(row.get("provider") or "")
        locator = str(row.get("locator") or "")
        if provider not in ACTIVE_PROVIDERS or not locator:
            continue
        if candidate_capability_error(row) is not None:
            continue
        deduplicated.setdefault((provider, locator), row)
    configured_rows = [
        value for value in telemetry.values()
        if isinstance(value, Mapping) and value.get("configured") is True
    ]
    # A negative result is complete only when at least one source was enabled
    # and every enabled source finished cleanly.  This follows the actual
    # runtime configuration rather than a static required-source list, while
    # retaining fail-closed behavior for an all-disabled or non-responsive
    # deployment.
    search_complete = bool(
        configured_rows
        and all(row.get("source_exhausted") is True for row in configured_rows)
        and all(int(row.get("infrastructure_failures") or 0) == 0 for row in configured_rows)
        and all(int(row.get("query_attempts") or 0) > 0 for row in configured_rows)
        and all(int(row.get("query_responses") or 0) > 0 for row in configured_rows)
    )
    return {
        "version": 1,
        "catalog_verified_at": None,
        "candidates": list(deduplicated.values()),
        "excluded_candidate_count": len(existing_rows),
        "warnings": warnings,
        "lane_status": provider_capability_snapshot(),
        "provider_capabilities": provider_capability_snapshot(),
        "source_telemetry": telemetry,
        "search_complete": search_complete,
        "active_search_lane": "magnet_torrent",
    }


