"""P14: drive the new-path Gap ledger through the exact two-tier chain.

The new architecture records precise gaps in ``gap_ledger_<root_task_id>.json``
(see ``engine.scrapeflow.gap_ledger``).  This module is the orchestrator that
closes those gaps with the current replenishment machinery:

    quark_share  -> QuarkFastSaveAutomaticMaterializer
    magnet       -> LocalTorrentAutomaticMaterializer

``alist_offline`` is retired.  A persisted pre-upgrade AList tier is rejected
before any dispatcher or materializer runs: this version has no AList task
recovery, cleanup, or migration path.  Such an installation must be manually
verified and cleaned before its durable state is upgraded to the two-tier
model.

It never instantiates the legacy ``AutomaticReplenishmentRuntime`` (which
writes forbidden ``EngineJob.summary.replenishment`` projections).  Instead it
reuses the thin, injectable per-tier *materializers* and the bridge/search
boundaries directly, and drives its own durable tier state.

``missing_subtitle`` gaps run through a small, independent subtitle channel
before the two video tiers.  It only retrieves direct, exact sidecars and
installs **one** formal subtitle file per gap: a verified Simplified-Chinese
track, optionally merged with the TMDB-proven original language into that same
file.  It never creates a second original-language sidecar and never falls
back to a video, season pack, or archive download.

Tier progression (contract rule 4) is delegated to the pure
``replenishment_tiers.apply_tier_outcome`` policy:

* ``candidate`` failures accumulate (per locator);
* the tier advances only on a complete no-candidate proof across every open
  request for the tier;
* ``infrastructure`` -> ``waiting == "retry_wait"`` (same tier, never downgrade);
* ``in_doubt`` -> ``waiting == "waiting_reconcile"`` (same tier, and the in-flight
  gap ids are remembered in the durable state so they are never re-submitted).

Parked current-lane ``in_doubt`` coordinates are NOT re-submitted.

The durable state lives at ``state_root / replenishment_<root_task_id>.json``
and is written with ``engine.scrapeflow.serialization.atomic_write_json``.  It
merges the pure tier-policy fields (``tier``, ``candidate_failures_by_provider``,
``exhaustion_proof_by_provider``, ``last_error_scope``) with the orchestration
fields documented below.

Reused vs. wrapped materializers
--------------------------------

The two current per-tier materializer classes are reused **as-is** through the
injectable ``materializer_factory`` (defaulting to the real classes).  They
already enforce the provider/lane contracts and write only into the
``staging_root`` they are given, so no wrapper was required.  The one small
adaptation lives in ``_classify_error``: the materializers signal *why* an
attempt failed through exception attributes (``failure_scope``,
``exclude_candidate``, ``external_task_id``/``task_id``) rather than a typed
return value, so that surface is read here and mapped onto the pure policy
scopes.
"""

from __future__ import annotations

import inspect
import posixpath
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Callable, Mapping

from engine.scrapeflow.gap_ledger import (
    Gap,
    close_gap,
    load_gap_ledger,
    record_attempt,
)
from engine.scrapeflow.replenishment_matching import audit_episode_tokens
from engine.scrapeflow.serialization import atomic_write_json
from engine.scrapeflow.subtitle_content import (
    MAX_MERGED_SUBTITLE_BYTES,
    classify_bilingual_subtitle_content,
    classify_subtitle_content,
    normalize_subtitle_language,
)
from engine.scrapeflow.target_shelf import target_root_for_shelf
from engine.scrapeflow.work_units import load_work_unit_records
from engine.tools.replenishment_adapter import SubtitleMaterializer

from .replenishment_bridge import (
    _nonempty_strings,
    gap_ledger_requests,
    gap_ledger_selection,
)
from .replenishment_tiers import (
    FAILURE_CANDIDATE,
    FAILURE_IN_DOUBT,
    FAILURE_INFRASTRUCTURE,
    STRICT_TIER_ORDER,
    TIER_LOCAL_MAGNET,
    TIER_QUARK_SHARE,
    apply_tier_outcome,
    initial_tier_state,
    required_sources_for_tier,
)
from .simple_engine_runner import EngineRequest

_STATE_FILE_PREFIX = "replenishment_"
_STATE_SUFFIX = ".json"
_IN_FLIGHT_KEY = "in_flight_gap_ids"
_SUBTITLE_INTENTS_KEY = "subtitle_intents"
_MAX_ATTEMPT_LOG = 200
_STAGING_NAMESPACE = "/ScrapeFlow/补源"
_SUBTITLE_TIER = "subtitle"
_SUBTITLE_PROVIDER = "subtitle"
_SUBTITLE_SUPPORTED_ORIGINAL_LANGUAGES = frozenset({
    "japanese", "english", "korean",
})
_SUBTITLE_TEXT_EXTENSIONS = frozenset({".ass", ".ssa", ".srt", ".vtt"})
_SUBTITLE_INTENT_PHASES = frozenset({
    "prepared", "submitting", "staged", "installing", "waiting_reconcile",
})

_KNOWN_FAILURE_SCOPES = frozenset({
    FAILURE_CANDIDATE, FAILURE_INFRASTRUCTURE, FAILURE_IN_DOUBT,
})


class PreUpgradeAListStateError(RuntimeError):
    """A removed AList lane remains in a state file from before this upgrade."""


def _trace(message: str) -> None:
    """Bounded live observability line (mirrors the legacy runner's tracer)."""
    print(f"[root-replenishment] {message}", flush=True)

# The provider name for each current tier equals the tier name, which is also
# what current materializers validate.
_TIER_PROVIDER = {
    TIER_QUARK_SHARE: TIER_QUARK_SHARE,
    TIER_LOCAL_MAGNET: TIER_LOCAL_MAGNET,
}


def _now() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _state_path(state_root: Path, root_task_id: str) -> Path:
    return state_root / f"{_STATE_FILE_PREFIX}{root_task_id}{_STATE_SUFFIX}"


def _fresh_state() -> dict[str, Any]:
    state: dict[str, Any] = initial_tier_state()
    state.update({
        "updated_at": _now(),
        "last_attempt_at": None,
        "waiting": None,
        "attempt_log": [],
        _IN_FLIGHT_KEY: {},
        # One entry exists only while a subtitle acquisition/write requires
        # recovery.  It is intentionally separate from the video-tier
        # in-flight map: subtitle providers have no video-tier reconcile API,
        # so an uncertain subtitle must be held rather than resubmitted.
        _SUBTITLE_INTENTS_KEY: {},
    })
    return state


def _bounded_path(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    path = value.strip()
    if not path or len(path) > 2048 or "\x00" in path:
        return None
    return path


def _normalize_subtitle_intents(value: object) -> dict[str, dict[str, Any]]:
    """Retain only minimal, non-secret recovery evidence for subtitle work.

    Durable intent is written before the provider is called.  A malformed
    state must therefore become a conservative recovery barrier rather than a
    license to call a provider again with an unknown prior attempt.
    """
    if not isinstance(value, Mapping):
        return {}
    normalized: dict[str, dict[str, Any]] = {}
    for raw_gap_id, raw in value.items():
        if (
            not isinstance(raw_gap_id, str)
            or not raw_gap_id
            or len(raw_gap_id) > 512
        ):
            continue
        if not isinstance(raw, Mapping):
            normalized[raw_gap_id] = {"phase": "waiting_reconcile"}
            continue
        phase = str(raw.get("phase") or "").strip().casefold()
        # An unrecognised persisted attempt may be a partially-written record
        # from an older process.  Keep it as a barrier; silently dropping it
        # would permit a duplicate subtitle provider submission.
        if phase not in _SUBTITLE_INTENT_PHASES:
            normalized[raw_gap_id] = {"phase": "waiting_reconcile"}
            continue
        entry: dict[str, Any] = {"phase": phase}
        for key in ("attempt_id", "staging_root", "source", "target", "video_path"):
            parsed = _bounded_path(raw.get(key))
            if parsed is not None:
                entry[key] = parsed
        size = raw.get("size")
        if (
            isinstance(size, int)
            and not isinstance(size, bool)
            and 0 < size <= MAX_MERGED_SUBTITLE_BYTES
        ):
            entry["size"] = size
        language = normalize_subtitle_language(raw.get("subtitle_language"))
        if language == "simplified_chinese":
            entry["subtitle_language"] = language
        original_language = normalize_subtitle_language(raw.get("original_language"))
        if original_language in _SUBTITLE_SUPPORTED_ORIGINAL_LANGUAGES:
            entry["original_language"] = original_language
        if raw.get("bilingual") is True:
            entry["bilingual"] = True
        updated_at = raw.get("updated_at")
        if isinstance(updated_at, str) and len(updated_at) <= 64:
            entry["updated_at"] = updated_at
        normalized[raw_gap_id] = entry
    return normalized


def _normalize_state(raw: Mapping[str, Any]) -> dict[str, Any]:
    state = dict(raw)
    tier = state.get("tier")
    if tier not in STRICT_TIER_ORDER:
        return _fresh_state()
    state.setdefault("candidate_failures_by_provider", {})
    state.setdefault("exhaustion_proof_by_provider", {})
    state.setdefault("last_error_scope", None)
    state.setdefault("updated_at", _now())
    state.setdefault("last_attempt_at", None)
    state.setdefault("waiting", None)
    state.setdefault("attempt_log", [])
    state.setdefault(_IN_FLIGHT_KEY, {})
    state.setdefault(_SUBTITLE_INTENTS_KEY, {})
    in_flight = state.get(_IN_FLIGHT_KEY)
    if not isinstance(in_flight, Mapping):
        state[_IN_FLIGHT_KEY] = {}
    else:
        state[_IN_FLIGHT_KEY] = {
            str(key): (str(value) if value else None)
            for key, value in in_flight.items()
            if isinstance(key, str) and key
        }
    log = state.get("attempt_log")
    if not isinstance(log, list):
        state["attempt_log"] = []
    else:
        state["attempt_log"] = [
            dict(item) for item in log if isinstance(item, Mapping)
        ][- _MAX_ATTEMPT_LOG:]
    state[_SUBTITLE_INTENTS_KEY] = _normalize_subtitle_intents(
        state.get(_SUBTITLE_INTENTS_KEY),
    )
    return state


def load_root_replenishment_state(
    state_root: Path, root_task_id: str,
) -> dict[str, Any]:
    """Load the durable tier/orchestration state for one root task.

    A missing or unreadable file yields a fresh ``quark_share`` state; a file
    with an invalid tier is treated the same (fail-closed, never trust a
    malformed persisted tier).
    """
    state_root = Path(state_root)
    path = _state_path(state_root, root_task_id)
    if not path.exists():
        return _fresh_state()
    try:
        import json
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, ValueError):
        return _fresh_state()
    if not isinstance(raw, Mapping):
        return _fresh_state()
    raw_tier = str(raw.get("tier") or "").strip().casefold()
    if raw_tier in {"alist_offline", "legacy_alist_offline_blocked"}:
        raise PreUpgradeAListStateError(
            "检测到升级前 AList 离线下载状态；当前版本不提供恢复、取消或"
            "迁移。请先人工确认并清理旧外部任务，再升级该状态文件。",
        )
    return _normalize_state(raw)


def save_root_replenishment_state(
    state_root: Path, root_task_id: str, state: Mapping[str, Any],
) -> None:
    """Persist the durable tier/orchestration state atomically."""
    atomic_write_json(
        _state_path(Path(state_root), root_task_id),
        dict(state),
        allow_nan=False,
    )


def _default_materializer_factory(
    tier: str,
    *,
    archive_preprocessor: object | None = None,
) -> Any:
    """Map one tier to its real per-tier materializer class instance."""
    if tier == TIER_QUARK_SHARE:
        from .automatic_replenishment import QuarkFastSaveAutomaticMaterializer
        return QuarkFastSaveAutomaticMaterializer()
    if tier == TIER_LOCAL_MAGNET:
        from .automatic_replenishment import LocalTorrentAutomaticMaterializer
        return LocalTorrentAutomaticMaterializer(
            archive_preprocessor=archive_preprocessor,
        )
    raise ValueError(f"补源 tier 无效: {tier!r}")


def _exception_chain(error: BaseException):
    seen: set[int] = set()
    current: BaseException | None = error
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        yield current
        current = current.__cause__ or current.__context__


def _safe_task_id(value: object) -> str | None:
    if (
        isinstance(value, str)
        and value
        and len(value) <= 256
        and not any(char in value for char in ("/", "\\", "\x00", "\n", "\r"))
    ):
        return value
    return None


def _classify_error(error: Exception) -> tuple[str, str | None]:
    """Map a materializer/search exception onto a pure failure scope.

    The materializers signal their own scope through exception attributes
    (``failure_scope`` / ``exclude_candidate``) and an in-doubt submit through
    ``external_task_id``/``task_id``.  Anything unclassified is conservative
    ``infrastructure`` (same tier, ``retry_wait``) so a surprising failure can
    never silently advance or downgrade the tier.
    """
    for item in _exception_chain(error):
        scope = getattr(item, "failure_scope", None)
        if isinstance(scope, str):
            normalized = scope.strip().casefold()
            if normalized in _KNOWN_FAILURE_SCOPES:
                if normalized == FAILURE_IN_DOUBT:
                    return normalized, _safe_task_id(
                        getattr(item, "external_task_id", None)
                        or getattr(item, "task_id", None)
                    )
                return normalized, None
        if getattr(item, "exclude_candidate", False) is True:
            return FAILURE_CANDIDATE, None
        task_id = _safe_task_id(
            getattr(item, "external_task_id", None)
            or getattr(item, "task_id", None)
        )
        if task_id is not None:
            return FAILURE_IN_DOUBT, task_id
    return FAILURE_INFRASTRUCTURE, None


def _is_pause_error(error: BaseException) -> bool:
    """Recognize the lower provider boundary's resumable stop signal."""
    return any(
        getattr(item, "pause_requested", False) is True
        for item in _exception_chain(error)
    )


def _call_materializer_with_pause(
    method: Callable[..., object],
    *args: object,
    pause_requested: Callable[[], bool] | None,
    **kwargs: object,
) -> object:
    """Never silently call a scoped provider boundary without its fence."""
    if pause_requested is None:
        return method(*args, **kwargs)
    try:
        parameters = inspect.signature(method).parameters.values()
    except (TypeError, ValueError) as exc:
        raise RuntimeError(
            "补源 materializer 无法证明支持 pause_requested，已安全停止",
        ) from exc
    accepts_pause = any(
        parameter.kind is inspect.Parameter.VAR_KEYWORD
        or parameter.name == "pause_requested"
        for parameter in parameters
    )
    if not accepts_pause:
        raise RuntimeError(
            "补源 materializer 不支持 pause_requested，拒绝在 RootJob 试运行范围执行",
        )
    return method(*args, pause_requested=pause_requested, **kwargs)


def _episode_token(season: int, episodes: object) -> str | None:
    ordered = sorted({
        int(item) for item in episodes
        if isinstance(item, int) and not isinstance(item, bool) and item > 0
    })
    if not ordered:
        return None
    if len(ordered) == 1:
        return f"S{season:02d}E{ordered[0]:02d}"
    return f"S{season:02d}E{ordered[0]:02d}-E{ordered[-1]:02d}"


def _bridge_token(gap: Gap) -> str | None:
    """Render the bridge token for one ledger gap (mirrors the bridge)."""
    if gap.kind == "missing_episode":
        if isinstance(gap.season, int) and not isinstance(gap.season, bool):
            return _episode_token(gap.season, gap.episodes)
        return None
    if gap.kind == "missing_season":
        if isinstance(gap.season, int) and not isinstance(gap.season, bool):
            return f"S{gap.season:02d}"
        return None
    if gap.kind in {"missing_media", "missing_subtitle"}:
        return gap.gap_id
    return None


def _open_gaps_by_token(
    state_root: Path,
    root_task_id: str,
    media_type: str,
    tmdb_id: int,
) -> tuple[dict[str, list[Gap]], dict[str, list[Gap]]]:
    """Index open ledger gaps by bridge token and by work unit.

    Returns ``(by_token, by_unit)``.  ``by_token`` maps the bridge's gap token
    (``SxxEyy``, ``Sxx``, or the media/subtitle ``gap_id``) back to the open
    ``Gap`` rows, which carry the authoritative ledger ``gap_id``.
    """
    by_token: dict[str, list[Gap]] = {}
    by_unit: dict[str, list[Gap]] = {}
    for gap in load_gap_ledger(state_root, root_task_id):
        if gap.status != "open":
            continue
        if (
            str(gap.media_type).strip().casefold() != media_type.casefold()
            or gap.tmdb_id != tmdb_id
        ):
            continue
        token = _bridge_token(gap)
        if token is not None:
            by_token.setdefault(token, []).append(gap)
        by_unit.setdefault(gap.work_unit_id, []).append(gap)
    return by_token, by_unit


def _required_episode_coordinates(
    gap: Gap, by_unit: Mapping[str, list[Gap]],
) -> set[tuple[int, int]]:
    if gap.kind == "missing_episode":
        if isinstance(gap.season, int) and not isinstance(gap.season, bool):
            return {
                (gap.season, int(ep)) for ep in gap.episodes
                if isinstance(ep, int) and not isinstance(ep, bool) and ep > 0
            }
        return set()
    if gap.kind == "missing_season":
        if not isinstance(gap.season, int) or isinstance(gap.season, bool):
            return set()
        coords = {
            (gap.season, int(ep)) for ep in gap.episodes
            if isinstance(ep, int) and not isinstance(ep, bool) and ep > 0
        }
        for other in by_unit.get(gap.work_unit_id, ()):
            if (
                other.kind == "missing_episode"
                and other.status == "open"
                and other.season == gap.season
            ):
                coords |= {
                    (other.season, int(ep)) for ep in other.episodes
                    if isinstance(ep, int) and not isinstance(ep, bool) and ep > 0
                }
        return coords
    return set()


def _path_under(path: str, root: str) -> bool:
    if not path or not root:
        return False
    normalized = posixpath.normpath(path)
    root_norm = root.rstrip("/")
    return normalized == root_norm or normalized.startswith(root_norm + "/")


def _prove_gap_coverage(
    gap: Gap,
    by_unit: Mapping[str, list[Gap]],
    executed_plan: Mapping[str, Any],
) -> bool:
    """Prove one gap is covered by the executed child plan's files.

    The executed plan is the single durable proof: episode/season coverage
    comes from ``files[].final_name`` via ``audit_episode_tokens``; media needs
    one video executed under the plan target; subtitle needs a subtitle file
    executed next to a video (same target directory).
    """
    files = executed_plan.get("files") or []
    target_root = str(executed_plan.get("target_root") or "")
    rows = [item for item in files if isinstance(item, Mapping)]

    if gap.kind == "missing_media":
        return any(
            item.get("media_kind") == "video"
            and _path_under(str(item.get("target_dir") or ""), target_root)
            for item in rows
        )

    if gap.kind == "missing_subtitle":
        video_dirs = {
            str(item.get("target_dir") or "")
            for item in rows
            if item.get("media_kind") == "video"
        }
        return any(
            item.get("media_kind") == "subtitle"
            and str(item.get("target_dir") or "") in video_dirs
            for item in rows
        )

    required = _required_episode_coordinates(gap, by_unit)
    if not required:
        return False
    actual: set[tuple[int, int]] = set()
    for item in rows:
        if item.get("media_kind") != "video":
            continue
        actual |= audit_episode_tokens(str(item.get("final_name") or ""))
    return required <= actual


def _attempt_status(scope: str) -> str:
    if scope == FAILURE_IN_DOUBT:
        return "in_doubt"
    if scope == FAILURE_INFRASTRUCTURE:
        return "infrastructure"
    return "candidate_failed"


def _work_parent(
    runner: Any,
    state_root: Path,
    root_task_id: str,
    request: Mapping[str, Any],
) -> str:
    """Derive the child plan's ``parent_path`` from the work unit's library root.

    The planner writes back onto the already-audited work root when given its
    parent, exactly like the merge lane.  Best-effort: an unknown root falls
    back to the confirmed shelf root (the planner then derives the title dir).
    """
    media = request.get("media") or {}
    media_type = str(media.get("media_type") or "").strip().casefold()
    tmdb_id = media.get("tmdb_id")
    for record in load_work_unit_records(state_root, root_task_id):
        identity = record.identity or {}
        if (
            str(identity.get("media_type") or "").strip().casefold() != media_type
            or identity.get("tmdb_id") != tmdb_id
        ):
            continue
        work_root = record.matched_work_root
        if not work_root and record.writer_job_id:
            try:
                job = runner.get_job(record.writer_job_id)
                plan = job.plan if isinstance(job.plan, Mapping) else {}
                work_root = str(plan.get("target_root") or "") or None
            except Exception:
                work_root = None
        if work_root:
            return posixpath.dirname(str(work_root).rstrip("/"))
    try:
        root_job = runner.get_job(root_task_id)
        if root_job.target_shelf is not None:
            return target_root_for_shelf(runner.library_root, root_job.target_shelf)
    except Exception:
        pass
    return str(runner.library_root).rstrip("/") or "/"


def _request_season(request: Mapping[str, Any]) -> int | None:
    for row in request.get("gaps") or []:
        if not isinstance(row, Mapping):
            continue
        if row.get("kind") in {"missing_episode", "missing_season"}:
            season = row.get("season")
            if isinstance(season, int) and not isinstance(season, bool) and season >= 0:
                return season
    return None


def _child_request(
    runner: Any,
    state_root: Path,
    root_task_id: str,
    request: Mapping[str, Any],
    delivery: Mapping[str, Any],
) -> EngineRequest:
    media = request.get("media") or {}
    media_type = str(media.get("media_type") or "movie")
    tmdb_id = media.get("tmdb_id")
    if (
        isinstance(tmdb_id, bool)
        or not isinstance(tmdb_id, int)
        or tmdb_id <= 0
    ):
        raise ValueError("补源请求缺少有效 tmdb_id")
    staging_root = str(delivery.get("staging_root") or "")
    if not staging_root:
        raise ValueError("补源 materializer 未返回 staging_root")
    payload: dict[str, Any] = {
        "source_path": staging_root,
        "parent_path": _work_parent(runner, state_root, root_task_id, request),
        "media_type": media_type,
        "tmdb_id": tmdb_id,
    }
    season = _request_season(request)
    if season is not None:
        payload["season"] = season
    return EngineRequest.from_mapping(payload)


def _strip_video_companion_subtitle_members(
    selection: Mapping[str, Any],
) -> dict[str, Any]:
    """Return a video-only RootJob selection without mutating durable evidence.

    Older Torrent candidate builders could attach a subtitle index as a
    ``companion_subtitle_index_by_media_gap`` entry.  The local adapter turns
    that map directly into aria2 ``--select-file`` arguments.  RootJob owns
    the only automatic subtitle transaction now (including the strict merged
    bilingual proof), so a video materializer must never receive that map.
    Keep the original selection immutable for search/audit evidence and copy
    just the two shallow layers that carry it.
    """
    output = dict(selection)
    acquisition = selection.get("acquisition")
    if not isinstance(acquisition, Mapping):
        return output
    cleaned = dict(acquisition)
    cleaned.pop("companion_subtitle_index_by_media_gap", None)
    output["acquisition"] = cleaned
    return output


def _append_attempt_log(
    state: dict[str, Any],
    entry: Mapping[str, Any],
) -> None:
    log = state.setdefault("attempt_log", [])
    if not isinstance(log, list):
        log = []
        state["attempt_log"] = log
    log.append(dict(entry))
    if len(log) > _MAX_ATTEMPT_LOG:
        del log[:- _MAX_ATTEMPT_LOG]


def _root_target_shelf(runner: Any, root_task_id: str) -> str | None:
    """Return the root's declared shelf without trusting an arbitrary value."""
    try:
        shelf = runner.get_job(root_task_id).target_shelf
    except Exception:
        return None
    return shelf if shelf in {"movie", "anime", "us_tv"} else None


def _search_evidence_completion(
    tier: str,
    evidence: Mapping[str, Any] | None,
    *,
    shelf: str | None,
) -> tuple[bool, list[str]]:
    """Validate one raw-search proof for this tier and return its sources.

    ``gap_ledger_selection`` carries this evidence from the original search
    response.  Do not turn an empty selector result into a proof here: every
    identity request must independently show all required sources for the
    active tier and zero unchecked candidates.  Telemetry outside that
    required-source set is diagnostic only and cannot make this tier fail or
    succeed.
    """
    if not isinstance(evidence, Mapping):
        return False, []
    if evidence.get("scope") != FAILURE_CANDIDATE:
        return False, []
    if evidence.get("search_complete_no_candidates") is not True:
        return False, []
    unchecked = evidence.get("unchecked_secondary_candidates")
    if (
        not isinstance(unchecked, int)
        or isinstance(unchecked, bool)
        or unchecked != 0
    ):
        return False, []
    required = required_sources_for_tier(tier, shelf)
    completed = {
        str(value).strip().casefold()
        for value in (evidence.get("completed_sources") or [])
        if isinstance(value, str) and value.strip()
    } & set(required)
    telemetry = evidence.get("source_telemetry")
    if isinstance(telemetry, Mapping):
        for source in required:
            raw = telemetry.get(source)
            if not isinstance(raw, Mapping):
                continue
            failures = raw.get("infrastructure_failures")
            if (
                isinstance(failures, int)
                and not isinstance(failures, bool)
                and failures > 0
            ):
                return False, []
            if raw.get("source_exhausted") is True:
                completed.add(source)
    if not required.issubset(completed):
        return False, []
    return True, sorted(completed)


def _noop_result(state: dict[str, Any], tier: str) -> dict[str, Any]:
    return {
        "tier": tier,
        "tier_before": tier,
        "requests_built": 0,
        "attempts": [],
        "gaps_closed": [],
        "state": state,
        "waiting": None,
    }


def _enrich_media_titles(runner: Any, request: dict[str, Any]) -> None:
    """Enrich bridge aliases and a trustworthy original language from TMDB.

    An operator-confirmed unit identity may carry only ``media_type +
    tmdb_id`` (no title), which would leave the bridge with empty aliases and
    the selector would reject every candidate.  The TMDB client is the
    canonical title source.  The subtitle path also consumes
    ``original_language`` but only when this exact read-only TMDB detail
    response proves it; request/identity/provider values are deliberately
    discarded first so stale metadata cannot manufacture a bilingual track.
    """
    media = request.get("media")
    if not isinstance(media, dict):
        return
    media.pop("original_language", None)
    media.pop("original_language_verified_by_tmdb", None)
    media_type = str(media.get("media_type") or "").strip().casefold()
    tmdb_id = media.get("tmdb_id")
    if (
        media_type not in {"movie", "tv"}
        or isinstance(tmdb_id, bool)
        or not isinstance(tmdb_id, int)
        or tmdb_id <= 0
    ):
        return
    tmdb = getattr(runner, "tmdb", None)
    getter = getattr(tmdb, "get", None)
    if not callable(getter):
        return
    try:
        details = getter(f"/{media_type}/{tmdb_id}")
    except Exception:
        return
    if not isinstance(details, Mapping):
        return
    title = (
        str(details.get("name") or "").strip()
        if media_type == "tv"
        else str(details.get("title") or "").strip()
    )
    original_title = str(
        details.get("original_name") or details.get("original_title") or ""
    ).strip()
    if title:
        media["title"] = title
        if original_title:
            media["original_title"] = original_title
        media["aliases"] = _nonempty_strings([
            title,
            original_title,
            *(media.get("aliases") if isinstance(media.get("aliases"), list) else []),
        ])

    # A Chinese-original title cannot form the requested 简中 + 原语 pair;
    # unknown / unsupported values are not preserved as a weak hint.  The
    # boolean remains bound to this request's concrete TMDB id and is checked
    # again immediately before any merged file can be installed.
    original_language = normalize_subtitle_language(
        details.get("original_language"),
    )
    if original_language in _SUBTITLE_SUPPORTED_ORIGINAL_LANGUAGES:
        media["original_language"] = original_language
        media["original_language_verified_by_tmdb"] = True


def _subtitle_target_marker(
    language: object,
    *,
    bilingual: bool,
    original_language: object = None,
) -> str | None:
    """Build the one formal marker for a Simplified-Chinese subtitle gap.

    The bilingual marker is deliberately part of the *same filename*.  It is
    durable evidence for later audit/restart code that this one Chinese-named
    sidecar was merged against a particular TMDB-proven original language; it
    never denotes another file to write.
    """
    if normalize_subtitle_language(language) != "simplified_chinese":
        return None
    marker = "zh-CN"
    if not bilingual:
        return marker
    codes = {"japanese": "ja", "english": "en", "korean": "ko"}
    code = codes.get(normalize_subtitle_language(original_language))
    if code is None:
        return None
    return f"{marker}-bilingual-{code}"


def _subtitle_target_path(
    gap: Mapping[str, Any],
    source_path: str,
    *,
    bilingual: bool,
    original_language: object = None,
) -> str | None:
    """Derive a single non-overlapping formal target for one subtitle gap."""
    video_path = gap.get("path")
    if not isinstance(video_path, str) or not video_path.startswith("/"):
        return None
    suffix = posixpath.splitext(source_path)[1].casefold()
    if suffix not in _SUBTITLE_TEXT_EXTENSIONS:
        return None
    marker = _subtitle_target_marker(
        gap.get("subtitle_language"),
        bilingual=bilingual,
        original_language=original_language,
    )
    if marker is None:
        return None
    return f"{posixpath.splitext(video_path)[0]}.{marker}{suffix}"


def _verified_original_language(request: Mapping[str, Any]) -> str | None:
    """Return only a value tied to a fresh TMDB detail response."""
    media = request.get("media")
    if not isinstance(media, Mapping):
        return None
    tmdb_id = media.get("tmdb_id")
    if (
        media.get("original_language_verified_by_tmdb") is not True
        or isinstance(tmdb_id, bool)
        or not isinstance(tmdb_id, int)
        or tmdb_id <= 0
    ):
        return None
    language = normalize_subtitle_language(media.get("original_language"))
    return language if language in _SUBTITLE_SUPPORTED_ORIGINAL_LANGUAGES else None


def _task_subtitle_staging_root(
    runner: Any, root_task_id: str, attempt_id: str,
) -> str:
    return (
        f"{str(runner.library_root).rstrip('/')}"
        f"{_STAGING_NAMESPACE}/{root_task_id}/{attempt_id}"
    )


def _is_task_subtitle_staging_path(
    value: object,
    staging_root: str,
) -> bool:
    if not isinstance(value, str) or not value.startswith("/"):
        return False
    normalized = posixpath.normpath(value)
    root = posixpath.normpath(staging_root)
    return normalized.startswith(root.rstrip("/") + "/")


def _fresh_file_info(alist: Any, path: str) -> Mapping[str, Any] | None:
    """Read one exact remote object, with a refreshed-parent fallback."""
    exact = getattr(alist, "exact_file_info", None)
    if callable(exact):
        try:
            row = exact(path)
        except Exception:
            row = None
        if isinstance(row, Mapping) and row.get("is_dir") is not True:
            return row
    listing = getattr(alist, "list", None)
    if not callable(listing):
        return None
    parent = posixpath.dirname(path) or "/"
    name = posixpath.basename(path)
    try:
        try:
            rows = listing(parent, refresh=True)
        except TypeError:
            rows = listing(parent)
    except Exception:
        return None
    if not isinstance(rows, list):
        return None
    matches = [
        row for row in rows
        if isinstance(row, Mapping)
        and row.get("name") == name
        and row.get("is_dir") is not True
    ]
    return matches[0] if len(matches) == 1 else None


def _file_size(row: Mapping[str, Any] | None) -> int | None:
    if not isinstance(row, Mapping):
        return None
    value = row.get("size")
    if isinstance(value, int) and not isinstance(value, bool) and value > 0:
        return value
    return None


def _read_subtitle_bytes(alist: Any, path: str, *, max_bytes: int) -> bytes | None:
    """Bound a content read used for pre/post writer proof."""
    reader = getattr(alist, "read_file_prefix", None)
    if not callable(reader):
        reader = getattr(alist, "read_file_bytes", None)
    if not callable(reader):
        return None
    try:
        try:
            raw = reader(path, max_bytes=max_bytes)
        except TypeError:
            raw = reader(path, max_bytes)
    except Exception:
        return None
    if not isinstance(raw, (bytes, bytearray)):
        return None
    return bytes(raw)


def _validate_root_subtitle_content(
    runner: Any,
    source_path: str,
    *,
    expected_size: int,
    bilingual: bool,
    original_language: str | None,
) -> dict[str, Any]:
    """Validate the full, freshly observed object that will be written.

    A prefix alone is not acceptance evidence: a provider could append another
    track or payload after the parsed bytes.  The exact remote size must equal
    the persisted delivery size and the bounded read must contain all of it.
    """
    if (
        isinstance(expected_size, bool)
        or not isinstance(expected_size, int)
        or not (0 < expected_size <= MAX_MERGED_SUBTITLE_BYTES)
        or _file_size(_fresh_file_info(runner.alist, source_path)) != expected_size
    ):
        return {"status": "unknown", "reason": "subtitle_size_unproven"}
    raw = _read_subtitle_bytes(
        runner.alist, source_path, max_bytes=expected_size,
    )
    if raw is None or len(raw) != expected_size:
        return {"status": "unknown", "reason": "subtitle_full_read_unproven"}
    if bilingual:
        if original_language not in _SUBTITLE_SUPPORTED_ORIGINAL_LANGUAGES:
            return {"status": "unknown", "reason": "unsupported_original_language"}
        return dict(classify_bilingual_subtitle_content(
            raw, original_language, max_bytes=expected_size,
        ))
    # ``raw`` above is a complete, exact-size read.  Preserve that proof in
    # the classifier instead of silently falling back to its ordinary bounded
    # audit prefix (which could otherwise accept a valid beginning followed by
    # unrelated bytes).
    return dict(classify_subtitle_content(
        raw, "zh", max_bytes=expected_size, require_each_cue=True,
    ))


def _subtitle_content_is_satisfied(result: Mapping[str, Any] | object) -> bool:
    return (
        isinstance(result, Mapping)
        and str(result.get("status") or "").strip().casefold() == "satisfied"
    )


def _call_subtitle_installer(
    installer: Callable[..., object],
    source_path: str,
    target_path: str,
    *,
    expected_size: int,
    video_path: str,
    validator: Callable[..., object],
    pause_requested: Callable[[], bool] | None,
) -> object:
    """Invoke the only writer without silently dropping its pause fence."""
    try:
        parameters = inspect.signature(installer).parameters.values()
    except (TypeError, ValueError) as exc:
        raise RuntimeError("字幕 writer 无法证明支持受控调用") from exc
    names = {parameter.name for parameter in parameters}
    accepts_kwargs = any(
        parameter.kind is inspect.Parameter.VAR_KEYWORD for parameter in parameters
    )
    if pause_requested is not None and not (accepts_kwargs or "pause_requested" in names):
        raise RuntimeError("字幕 writer 不支持 pause_requested，已安全停止")
    # The direct pre/post validation below is necessary but is not a reason to
    # bypass the writer's own validator for a merged file.
    if not (accepts_kwargs or "subtitle_validator" in names):
        raise RuntimeError("字幕 writer 不支持内容验证回调，拒绝写入")
    kwargs: dict[str, object] = {
        "expected_size": expected_size,
        "video_path": video_path,
        "subtitle_language": "zh",
        "subtitle_validator": validator,
    }
    if pause_requested is not None:
        kwargs["pause_requested"] = pause_requested
    return installer(source_path, target_path, **kwargs)


def _empty_subtitle_result() -> dict[str, Any]:
    return {
        "subtitle_requests_built": 0,
        "subtitle_attempts": [],
        "subtitle_gaps_closed": [],
        "subtitle_waiting": None,
        "paused": False,
    }


def _subtitle_request_rows(request: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    """Return only formal Simplified-Chinese subtitle gaps for one request."""
    rows: dict[str, dict[str, Any]] = {}
    for raw in request.get("gaps") or []:
        if not isinstance(raw, Mapping) or raw.get("kind") != "missing_subtitle":
            continue
        gap_id = raw.get("id")
        if (
            not isinstance(gap_id, str)
            or not gap_id
            or normalize_subtitle_language(raw.get("subtitle_language"))
            != "simplified_chinese"
            or not isinstance(raw.get("path"), str)
            or not str(raw.get("path")).startswith("/")
        ):
            continue
        rows[gap_id] = dict(raw)
    return rows


def _extract_subtitle_delivery(
    delivery: Mapping[str, Any],
    request: Mapping[str, Any],
    *,
    staging_root: str,
) -> dict[str, dict[str, Any]]:
    """Validate provider delivery metadata without trusting a second track.

    The provider must emit one row bound to one subtitle gap.  A merged row
    carries an explicit ``bilingual`` flag and the exact TMDB-verified original
    language; Chinese-only is the safe fallback.  ``original_optional`` or a
    second row for the same gap is rejected rather than quietly becoming a
    second formal sidecar.
    """
    rows = delivery.get("files")
    if not isinstance(rows, list):
        raise ValueError("字幕 provider delivery 缺少 files 列表")
    gaps = _subtitle_request_rows(request)
    output: dict[str, dict[str, Any]] = {}
    global_bilingual = delivery.get("bilingual") is True
    global_original = normalize_subtitle_language(delivery.get("original_language"))
    verified_original = _verified_original_language(request)
    for raw in rows:
        if not isinstance(raw, Mapping):
            raise ValueError("字幕 provider files 项无效")
        if raw.get("kind") not in {None, "subtitle"}:
            raise ValueError("字幕 provider 返回了非字幕成员")
        role = str(raw.get("subtitle_role") or "").strip().casefold()
        if role in {"original", "original_optional", "secondary"}:
            raise ValueError("字幕 provider 试图交付独立原语轨")
        gap_ids = raw.get("gap_ids")
        if not isinstance(gap_ids, list) or len(gap_ids) != 1:
            raise ValueError("字幕 provider 未将文件精确绑定唯一 gap")
        gap_id = gap_ids[0]
        if not isinstance(gap_id, str) or gap_id not in gaps or gap_id in output:
            raise ValueError("字幕 provider 交付了未知或重复 gap")
        source = raw.get("path")
        size = raw.get("size")
        if (
            not _is_task_subtitle_staging_path(source, staging_root)
            or isinstance(size, bool)
            or not isinstance(size, int)
            or size <= 0
            or size > MAX_MERGED_SUBTITLE_BYTES
        ):
            raise ValueError("字幕 provider 交付未绑定有效任务 staging 文件")
        suffix = posixpath.splitext(str(source))[1].casefold()
        if suffix not in _SUBTITLE_TEXT_EXTENSIONS:
            raise ValueError("双语字幕仅接受可解析文本格式")
        bilingual = raw.get("bilingual") is True or (
            "bilingual" not in raw and global_bilingual
        )
        original_language = normalize_subtitle_language(
            raw.get("original_language")
            if raw.get("original_language") is not None
            else global_original,
        )
        if bilingual:
            if (
                verified_original is None
                or original_language != verified_original
            ):
                raise ValueError("双语字幕未绑定 TMDB 已验证原语")
            expected_marker = _subtitle_target_marker(
                gaps[gap_id].get("subtitle_language"),
                bilingual=True,
                original_language=original_language,
            )
            marker = raw.get("subtitle_marker")
            if marker is None:
                marker = delivery.get("subtitle_marker")
            if not isinstance(marker, str) or marker != expected_marker:
                raise ValueError("双语字幕缺少可信持久化语言标识")
        else:
            original_language = None
        target = _subtitle_target_path(
            gaps[gap_id], str(source), bilingual=bilingual,
            original_language=original_language,
        )
        if target is None:
            raise ValueError("字幕正式目标无法证明为简中单文件")
        output[gap_id] = {
            "source": str(source),
            "size": size,
            "target": target,
            "video_path": str(gaps[gap_id]["path"]),
            "subtitle_language": "simplified_chinese",
            "bilingual": bilingual,
            **({"original_language": original_language} if original_language else {}),
        }
    return output


def _subtitle_intents(state: dict[str, Any]) -> dict[str, dict[str, Any]]:
    intents = state.get(_SUBTITLE_INTENTS_KEY)
    if not isinstance(intents, dict):
        intents = {}
        state[_SUBTITLE_INTENTS_KEY] = intents
    return intents


def _write_subtitle_intent(
    state_root: Path,
    root_task_id: str,
    state: dict[str, Any],
    gap_id: str,
    value: Mapping[str, Any],
) -> None:
    intents = _subtitle_intents(state)
    intents[gap_id] = dict(value)
    state["updated_at"] = _now()
    save_root_replenishment_state(state_root, root_task_id, state)


def _drop_subtitle_intent(
    state_root: Path,
    root_task_id: str,
    state: dict[str, Any],
    gap_id: str,
) -> None:
    _subtitle_intents(state).pop(gap_id, None)
    state["updated_at"] = _now()
    save_root_replenishment_state(state_root, root_task_id, state)


def _record_subtitle_attempt(
    state_root: Path,
    root_task_id: str,
    gap: Gap,
    *,
    attempt_id: str,
    status: str,
    staged_paths: list[str] | None = None,
    error: str | None = None,
) -> None:
    record_attempt(
        state_root,
        root_task_id,
        gap.gap_id,
        attempt_id=attempt_id,
        provider=_SUBTITLE_PROVIDER,
        tier=_SUBTITLE_TIER,
        locator=None,
        status=status,
        staged_paths=staged_paths or [],
        error=(str(error)[:200] if error else None),
    )


def _cleanup_empty_subtitle_staging(
    runner: Any,
    staging_root: str,
    *,
    pause_requested: Callable[[], bool] | None,
) -> bool:
    """Remove at most one freshly-proven-empty task attempt directory."""
    if pause_requested is not None and pause_requested():
        return False
    listing = getattr(runner.alist, "list", None)
    remove_empty = getattr(runner.alist, "remove_empty_dir", None)
    if not callable(listing) or not callable(remove_empty):
        return False
    try:
        try:
            rows = listing(staging_root, refresh=True)
        except TypeError:
            rows = listing(staging_root)
    except Exception:
        return False
    if not isinstance(rows, list) or rows:
        return False
    if pause_requested is not None and pause_requested():
        return False
    try:
        remove_empty(staging_root)
    except Exception:
        return False
    parent = posixpath.dirname(staging_root) or "/"
    name = posixpath.basename(staging_root)
    try:
        try:
            parent_rows = listing(parent, refresh=True)
        except TypeError:
            parent_rows = listing(parent)
    except Exception:
        return False
    return not any(
        isinstance(row, Mapping)
        and row.get("name") == name
        and row.get("is_dir") is True
        for row in (parent_rows if isinstance(parent_rows, list) else [])
    )


def _subtitle_staging_is_fresh_empty(runner: Any, staging_root: str) -> bool:
    """Prove a task attempt has no staged side effect before re-arming it.

    An empty/invalid provider response is not a no-op proof.  We clear an
    intent only after a fresh listing of its exact task root (or its parent
    when the root no longer exists) shows that no object or directory remains.
    Any read error stays fail-closed.
    """
    listing = getattr(runner.alist, "list", None)
    if not callable(listing):
        return False
    try:
        try:
            rows = listing(staging_root, refresh=True)
        except TypeError:
            rows = listing(staging_root)
    except Exception:
        parent = posixpath.dirname(staging_root) or "/"
        name = posixpath.basename(staging_root)
        try:
            try:
                parent_rows = listing(parent, refresh=True)
            except TypeError:
                parent_rows = listing(parent)
        except Exception:
            return False
        return (
            isinstance(parent_rows, list)
            and not any(
                isinstance(row, Mapping) and row.get("name") == name
                for row in parent_rows
            )
        )
    return isinstance(rows, list) and not rows


def _finish_subtitle_intent(
    runner: Any,
    state_root: Path,
    root_task_id: str,
    state: dict[str, Any],
    *,
    gap: Gap,
    gap_row: Mapping[str, Any],
    request: Mapping[str, Any],
    pause_requested: Callable[[], bool] | None,
) -> dict[str, Any]:
    """Install/recover one persisted subtitle delivery without re-submitting it.

    A ``submitting`` intent with no immutable staged-file evidence is never
    replayed: its provider response may have been lost.  Once a source/target
    pair has been persisted, recovery uses fresh exact observations to either
    prove the formal target and close the gap, or safely continue that one
    writer operation.  Any ambiguous state remains ``waiting_reconcile``.
    """
    intent = _subtitle_intents(state).get(gap.gap_id)
    if not isinstance(intent, Mapping):
        return {"outcome": "none"}
    attempt_id = str(intent.get("attempt_id") or uuid.uuid4().hex)
    source = _bounded_path(intent.get("source"))
    target = _bounded_path(intent.get("target"))
    video_path = _bounded_path(intent.get("video_path"))
    staging_root = _bounded_path(intent.get("staging_root"))
    size = intent.get("size")
    bilingual = intent.get("bilingual") is True
    original_language = normalize_subtitle_language(intent.get("original_language"))
    phase = str(intent.get("phase") or "").casefold()
    expected_staging_root = (
        _task_subtitle_staging_root(runner, root_task_id, attempt_id)
        if _safe_task_id(attempt_id) is not None
        else None
    )
    if (
        phase in {"submitting", "waiting_reconcile"}
        and (not source or not target or not video_path or not staging_root or not size)
    ):
        return {"outcome": "waiting_reconcile"}
    if (
        not source
        or not target
        or not video_path
        or not staging_root
        or not isinstance(size, int)
        or isinstance(size, bool)
        or size <= 0
        or size > MAX_MERGED_SUBTITLE_BYTES
        or expected_staging_root is None
        or posixpath.normpath(staging_root) != posixpath.normpath(expected_staging_root)
        or not _is_task_subtitle_staging_path(source, staging_root)
        or _subtitle_target_path(
            gap_row, source, bilingual=bilingual,
            original_language=original_language,
        ) != target
        or (bilingual and original_language != _verified_original_language(request))
    ):
        _write_subtitle_intent(
            state_root, root_task_id, state, gap.gap_id,
            {"phase": "waiting_reconcile", "attempt_id": attempt_id},
        )
        return {"outcome": "waiting_reconcile"}

    def prove_target() -> bool:
        info = _fresh_file_info(runner.alist, target)
        if _file_size(info) != size:
            return False
        verdict = _validate_root_subtitle_content(
            runner,
            target,
            expected_size=size,
            bilingual=bilingual,
            original_language=original_language if bilingual else None,
        )
        return _subtitle_content_is_satisfied(verdict)

    # This first branch handles a lost writer response and makes restart
    # idempotent without ever creating a second sidecar.
    if prove_target():
        try:
            close_gap(state_root, root_task_id, gap.gap_id)
        except KeyError:
            return {"outcome": "none"}
        _drop_subtitle_intent(state_root, root_task_id, state, gap.gap_id)
        return {
            "outcome": "closed",
            "gap_id": gap.gap_id,
            "attempt_id": attempt_id,
            "target": target,
            "bilingual": bilingual,
        }

    if pause_requested is not None and pause_requested():
        return {"outcome": "paused"}

    source_info = _fresh_file_info(runner.alist, source)
    if _file_size(source_info) != size:
        _write_subtitle_intent(
            state_root, root_task_id, state, gap.gap_id,
            {"phase": "waiting_reconcile", "attempt_id": attempt_id},
        )
        return {"outcome": "waiting_reconcile"}

    verdict = _validate_root_subtitle_content(
        runner,
        source,
        expected_size=size,
        bilingual=bilingual,
        original_language=original_language if bilingual else None,
    )
    if not _subtitle_content_is_satisfied(verdict):
        # The provider has already staged a real remote object.  Even when
        # its bytes fail our strict content proof, treating that as a simple
        # candidate miss would drop the durable intent and permit a second
        # submission beside unexamined task-owned bytes.  Hold the attempt
        # for reconciliation instead; only a separately proven cleanup may
        # make this coordinate retryable.
        pending = dict(intent)
        pending["phase"] = "waiting_reconcile"
        pending["updated_at"] = _now()
        _write_subtitle_intent(
            state_root, root_task_id, state, gap.gap_id, pending,
        )
        _record_subtitle_attempt(
            state_root, root_task_id, gap,
            attempt_id=attempt_id,
            status="in_doubt",
            staged_paths=[source],
            error=str(verdict.get("reason") or "subtitle_content_unproven"),
        )
        return {"outcome": "waiting_reconcile", "gap_id": gap.gap_id}

    installer = getattr(runner, "install_subtitle_sidecar", None)
    if not callable(installer):
        _write_subtitle_intent(
            state_root, root_task_id, state, gap.gap_id,
            {"phase": "waiting_reconcile", "attempt_id": attempt_id},
        )
        return {"outcome": "waiting_reconcile"}

    def validator(candidate_path: str, _required_language: str = "zh") -> Mapping[str, Any]:
        return _validate_root_subtitle_content(
            runner,
            candidate_path,
            expected_size=size,
            bilingual=bilingual,
            original_language=original_language if bilingual else None,
        )

    persisted = dict(intent)
    persisted["phase"] = "installing"
    persisted["updated_at"] = _now()
    _write_subtitle_intent(
        state_root, root_task_id, state, gap.gap_id, persisted,
    )
    try:
        result = _call_subtitle_installer(
            installer,
            source,
            target,
            expected_size=size,
            video_path=video_path,
            validator=validator,
            pause_requested=pause_requested,
        )
    except Exception as exc:
        if _is_pause_error(exc):
            return {"outcome": "paused"}
        # A lost move response is distinguishable only by fresh target/source
        # observations, never by replaying the provider.  Keep the delivery
        # intent intact so the next round can make that proof.
        if prove_target():
            try:
                close_gap(state_root, root_task_id, gap.gap_id)
            except KeyError:
                return {"outcome": "none"}
            _drop_subtitle_intent(state_root, root_task_id, state, gap.gap_id)
            return {
                "outcome": "closed", "gap_id": gap.gap_id,
                "attempt_id": attempt_id, "target": target,
                "bilingual": bilingual,
            }
        pending = dict(persisted)
        pending["phase"] = "waiting_reconcile"
        pending["updated_at"] = _now()
        _write_subtitle_intent(
            state_root, root_task_id, state, gap.gap_id, pending,
        )
        return {
            "outcome": "waiting_reconcile", "gap_id": gap.gap_id,
            "error": str(exc)[:200],
        }

    if (
        not isinstance(result, Mapping)
        or _file_size(result) not in {None, size}
        or not prove_target()
    ):
        pending = dict(persisted)
        pending["phase"] = "waiting_reconcile"
        pending["updated_at"] = _now()
        _write_subtitle_intent(
            state_root, root_task_id, state, gap.gap_id, pending,
        )
        return {"outcome": "waiting_reconcile", "gap_id": gap.gap_id}
    try:
        close_gap(state_root, root_task_id, gap.gap_id)
    except KeyError:
        return {"outcome": "none"}
    _drop_subtitle_intent(state_root, root_task_id, state, gap.gap_id)
    return {
        "outcome": "closed", "gap_id": gap.gap_id,
        "attempt_id": attempt_id, "target": target,
        "bilingual": bilingual,
    }


def _recover_root_subtitle_intents(
    runner: Any,
    state_root: Path,
    root_task_id: str,
    state: dict[str, Any],
    subtitle_requests: list[dict[str, Any]],
    *,
    pause_requested: Callable[[], bool] | None,
) -> dict[str, Any]:
    """Reconcile persisted subtitle writes without calling a provider again."""
    result = _empty_subtitle_result()
    by_gap: dict[str, tuple[dict[str, Any], dict[str, Any]]] = {}
    for request in subtitle_requests:
        for gap_id, row in _subtitle_request_rows(request).items():
            by_gap[gap_id] = (request, row)
    gap_records = {
        gap.gap_id: gap
        for gap in load_gap_ledger(state_root, root_task_id)
        if gap.status == "open" and gap.kind == "missing_subtitle"
    }
    for gap_id in list(_subtitle_intents(state)):
        if pause_requested is not None and pause_requested():
            result["paused"] = True
            break
        pair = by_gap.get(gap_id)
        gap = gap_records.get(gap_id)
        if gap is None:
            # A fresh re-audit may already have proven this target.  The gap is
            # no longer open, so retaining a recovery token would only block
            # unrelated subtitle work; do not delete any remote staging tree.
            _drop_subtitle_intent(state_root, root_task_id, state, gap_id)
            continue
        if pair is None:
            # Missing bridge/title evidence does not prove the old provider
            # call never happened.  Keep the barrier rather than discarding it
            # and allowing a future bridge rebuild to submit again.
            result["subtitle_waiting"] = "waiting_reconcile"
            continue
        request, gap_row = pair
        intent = _subtitle_intents(state).get(gap_id)
        if (
            isinstance(intent, Mapping)
            and str(intent.get("phase") or "").casefold() == "prepared"
        ):
            # ``prepared`` is persisted before local workspace setup only.
            # The provider submission state is written later, immediately
            # before its call, so this state cannot denote a remote effect and
            # can safely be re-armed after a restart or a pre-call pause.
            _drop_subtitle_intent(state_root, root_task_id, state, gap_id)
            continue
        completed = _finish_subtitle_intent(
            runner, state_root, root_task_id, state,
            gap=gap, gap_row=gap_row, request=request,
            pause_requested=pause_requested,
        )
        outcome = completed.get("outcome")
        if outcome == "closed":
            result["subtitle_gaps_closed"].append(gap_id)
            result["subtitle_attempts"].append({
                "gap_id": gap_id, "tier": _SUBTITLE_TIER,
                "outcome": "closed", "recovered": True,
            })
        elif outcome == "paused":
            result["paused"] = True
            break
        elif outcome == FAILURE_CANDIDATE:
            result["subtitle_attempts"].append({
                "gap_id": gap_id, "tier": _SUBTITLE_TIER,
                "outcome": FAILURE_CANDIDATE, "recovered": True,
            })
        elif outcome == "waiting_reconcile":
            result["subtitle_waiting"] = "waiting_reconcile"
    if _subtitle_intents(state):
        result["subtitle_waiting"] = "waiting_reconcile"
    return result


def _run_root_subtitle_channel(
    runner: Any,
    state_root: Path,
    root_task_id: str,
    state: dict[str, Any],
    requests: list[dict[str, Any]],
    *,
    subtitle_materializer_factory: Callable[[], Any] | Any | None,
    pause_requested: Callable[[], bool] | None,
) -> dict[str, Any]:
    """Drive independent subtitle acquisition before any video-tier effect.

    The provider is called at most once per new root request in a round.  A
    durable ``prepared`` intent covers local setup; it becomes ``submitting``
    immediately before the provider call.  If that call loses its response,
    later invocations only inspect the task-owned staging/target state; they
    never call the provider again for that gap.
    """
    result = _empty_subtitle_result()
    subtitle_requests: list[dict[str, Any]] = []
    for raw_request in requests:
        rows = _subtitle_request_rows(raw_request)
        if not rows:
            continue
        request = dict(raw_request)
        media = raw_request.get("media")
        request["media"] = dict(media) if isinstance(media, Mapping) else {}
        request["gaps"] = list(rows.values())
        subtitle_requests.append(request)
    result["subtitle_requests_built"] = len(subtitle_requests)
    if not subtitle_requests and not _subtitle_intents(state):
        return result

    recovery = _recover_root_subtitle_intents(
        runner, state_root, root_task_id, state, subtitle_requests,
        pause_requested=pause_requested,
    )
    result["subtitle_attempts"].extend(recovery["subtitle_attempts"])
    result["subtitle_gaps_closed"].extend(recovery["subtitle_gaps_closed"])
    result["subtitle_waiting"] = recovery["subtitle_waiting"]
    if recovery["paused"]:
        result["paused"] = True
        return result

    gap_records = {
        gap.gap_id: gap
        for gap in load_gap_ledger(state_root, root_task_id)
        if gap.status == "open" and gap.kind == "missing_subtitle"
    }
    candidate_failed = False
    active_staging_roots: set[str] = set()

    for request in subtitle_requests:
        if pause_requested is not None and pause_requested():
            result["paused"] = True
            break
        rows = _subtitle_request_rows(request)
        pending = {
            gap_id: row for gap_id, row in rows.items()
            if gap_id in gap_records and gap_id not in _subtitle_intents(state)
        }
        if not pending:
            continue

        attempt_id = f"subtitle-{uuid.uuid4().hex}"
        staging_root = _task_subtitle_staging_root(
            runner, root_task_id, attempt_id,
        )
        workspace = state_root / "subtitle_replenishment_workspace" / root_task_id / attempt_id
        if pause_requested is not None and pause_requested():
            result["paused"] = True
            break

        # Persist every gap in this provider call before allocating its local
        # workspace or invoking any discovery/download/upload boundary.
        intents = _subtitle_intents(state)
        for gap_id in pending:
            intents[gap_id] = {
                # This reserves the local preparation work only.  It must not
                # be mistaken for a provider submission after a pause or
                # crash before the final pre-call fence.
                "phase": "prepared",
                "attempt_id": attempt_id,
                "staging_root": staging_root,
                "subtitle_language": "simplified_chinese",
                "updated_at": _now(),
            }
        state["last_attempt_at"] = _now()
        state["updated_at"] = _now()
        save_root_replenishment_state(state_root, root_task_id, state)

        if pause_requested is not None and pause_requested():
            # No external provider boundary has been crossed.  Do not leave a
            # misleading in-flight token solely because the pause landed
            # between local intent persistence and workspace preparation.
            for gap_id in pending:
                intents.pop(gap_id, None)
            state["updated_at"] = _now()
            save_root_replenishment_state(state_root, root_task_id, state)
            result["paused"] = True
            break
        try:
            workspace.mkdir(parents=True, exist_ok=True)
        except OSError:
            # The adapter uses the workspace only for bounded temporary bytes;
            # a local failure before the provider call has no remote state and
            # can be classified safely as infrastructure, not in-doubt.
            for gap_id in pending:
                intents.pop(gap_id, None)
                _record_subtitle_attempt(
                    state_root, root_task_id, gap_records[gap_id],
                    attempt_id=attempt_id, status="infrastructure",
                    error="字幕本地 workspace 无法创建",
                )
                result["subtitle_attempts"].append({
                    "gap_id": gap_id, "tier": _SUBTITLE_TIER,
                    "outcome": FAILURE_INFRASTRUCTURE,
                })
            state["updated_at"] = _now()
            save_root_replenishment_state(state_root, root_task_id, state)
            result["subtitle_waiting"] = "retry_wait"
            continue

        if subtitle_materializer_factory is None:
            materializer = SubtitleMaterializer()
        elif callable(subtitle_materializer_factory):
            materializer = subtitle_materializer_factory()
        else:
            materializer = subtitle_materializer_factory
        acquire = getattr(materializer, "acquire_subtitles", None)
        if not callable(acquire):
            # No call occurred; the safe recovery intent may be cleared and
            # the visible failure remains a retryable infrastructure defect.
            for gap_id in pending:
                intents.pop(gap_id, None)
                _record_subtitle_attempt(
                    state_root, root_task_id, gap_records[gap_id],
                    attempt_id=attempt_id, status="infrastructure",
                    error="字幕 materializer 不支持 acquire_subtitles",
                )
                result["subtitle_attempts"].append({
                    "gap_id": gap_id, "tier": _SUBTITLE_TIER,
                    "outcome": FAILURE_INFRASTRUCTURE,
                })
            state["updated_at"] = _now()
            save_root_replenishment_state(state_root, root_task_id, state)
            result["subtitle_waiting"] = "retry_wait"
            continue

        if pause_requested is not None and pause_requested():
            # ``acquire_subtitles`` is the first provider effect.  Clearing a
            # prepared intent here is safe and makes a user pause resumable
            # without requiring a remote reconciliation round.
            for gap_id in pending:
                intents.pop(gap_id, None)
            state["updated_at"] = _now()
            save_root_replenishment_state(state_root, root_task_id, state)
            result["paused"] = True
            break

        # From this exact point onward a crash can race the provider call, so
        # make the durable token conservative before emitting the attempt.
        for gap_id in pending:
            intents[gap_id]["phase"] = "submitting"
            intents[gap_id]["updated_at"] = _now()
        state["updated_at"] = _now()
        save_root_replenishment_state(state_root, root_task_id, state)
        for gap_id in pending:
            _record_subtitle_attempt(
                state_root, root_task_id, gap_records[gap_id],
                attempt_id=attempt_id, status="submitted",
            )

        try:
            delivery = _call_materializer_with_pause(
                acquire,
                request,
                list(pending.values()),
                staging_root=staging_root,
                workspace=workspace,
                alist=runner.alist,
                pause_requested=pause_requested,
            )
        except Exception as exc:
            if _is_pause_error(exc):
                # The adapter might have stopped just before or just after an
                # upload.  Do not clear/replay the persisted intent.
                for gap_id in pending:
                    intents[gap_id]["phase"] = "waiting_reconcile"
                    intents[gap_id]["updated_at"] = _now()
                state["updated_at"] = _now()
                save_root_replenishment_state(state_root, root_task_id, state)
                result["paused"] = True
                result["subtitle_waiting"] = "waiting_reconcile"
                break
            scope, _task_id = _classify_error(exc)
            if (
                scope == FAILURE_CANDIDATE
                and _subtitle_staging_is_fresh_empty(runner, staging_root)
            ):
                # A candidate-labelled exception becomes retryable only after
                # the exact task root freshly proves it contains no staged
                # object.  The exception class alone cannot prove an upload
                # response was not lost.
                for gap_id in pending:
                    intents.pop(gap_id, None)
                    _record_subtitle_attempt(
                        state_root, root_task_id, gap_records[gap_id],
                        attempt_id=attempt_id, status="candidate_failed",
                        error=str(exc),
                    )
                    result["subtitle_attempts"].append({
                        "gap_id": gap_id, "tier": _SUBTITLE_TIER,
                        "outcome": FAILURE_CANDIDATE,
                    })
                candidate_failed = True
                state["updated_at"] = _now()
                save_root_replenishment_state(state_root, root_task_id, state)
                continue
            # A transport/upload *or candidate-labelled* exception can arrive
            # after the remote staging write committed. Preserve the intent as
            # in-doubt; no second provider call for these gap ids is allowed.
            for gap_id in pending:
                intents[gap_id]["phase"] = "waiting_reconcile"
                intents[gap_id]["updated_at"] = _now()
                _record_subtitle_attempt(
                    state_root, root_task_id, gap_records[gap_id],
                    attempt_id=attempt_id, status="in_doubt", error=str(exc),
                )
                result["subtitle_attempts"].append({
                    "gap_id": gap_id, "tier": _SUBTITLE_TIER,
                    "outcome": FAILURE_IN_DOUBT,
                })
            state["updated_at"] = _now()
            save_root_replenishment_state(state_root, root_task_id, state)
            result["subtitle_waiting"] = "waiting_reconcile"
            continue

        if not isinstance(delivery, Mapping):
            delivered: dict[str, dict[str, Any]] = {}
            delivery_error = "字幕 provider 返回无效 delivery"
        else:
            try:
                delivered = _extract_subtitle_delivery(
                    delivery, request, staging_root=staging_root,
                )
                delivery_error = None
            except ValueError as exc:
                delivered = {}
                delivery_error = str(exc)

        valid: dict[str, dict[str, Any]] = {}
        for gap_id, entry in delivered.items():
            info = _fresh_file_info(runner.alist, entry["source"])
            if _file_size(info) == entry["size"]:
                valid[gap_id] = entry
            else:
                delivery_error = "字幕 provider staging 回读不可证明"
        staging_proven_empty = _subtitle_staging_is_fresh_empty(
            runner, staging_root,
        )
        for gap_id in pending:
            if gap_id in valid:
                entry = valid[gap_id]
                intents[gap_id] = {
                    "phase": "staged",
                    "attempt_id": attempt_id,
                    "staging_root": staging_root,
                    **entry,
                    "updated_at": _now(),
                }
                active_staging_roots.add(staging_root)
                continue
            if staging_proven_empty:
                intents.pop(gap_id, None)
                _record_subtitle_attempt(
                    state_root, root_task_id, gap_records[gap_id],
                    attempt_id=attempt_id, status="candidate_failed",
                    error=delivery_error or "字幕 provider 未交付精确单集文件",
                )
                result["subtitle_attempts"].append({
                    "gap_id": gap_id, "tier": _SUBTITLE_TIER,
                    "outcome": FAILURE_CANDIDATE,
                })
                candidate_failed = True
            else:
                # A malformed/partial response can still have staged bytes
                # that are absent from its file list.  Keep its original
                # durable intent and make the ambiguity explicit; a future
                # round is not permitted to submit this gap again.
                intents[gap_id]["phase"] = "waiting_reconcile"
                intents[gap_id]["updated_at"] = _now()
                _record_subtitle_attempt(
                    state_root, root_task_id, gap_records[gap_id],
                    attempt_id=attempt_id, status="in_doubt",
                    error=delivery_error or "字幕 provider 交付不完整",
                )
                result["subtitle_attempts"].append({
                    "gap_id": gap_id, "tier": _SUBTITLE_TIER,
                    "outcome": FAILURE_IN_DOUBT,
                })
                result["subtitle_waiting"] = "waiting_reconcile"
        state["updated_at"] = _now()
        save_root_replenishment_state(state_root, root_task_id, state)

        for gap_id, entry in valid.items():
            if pause_requested is not None and pause_requested():
                result["paused"] = True
                break
            completed = _finish_subtitle_intent(
                runner, state_root, root_task_id, state,
                gap=gap_records[gap_id], gap_row=pending[gap_id],
                request=request, pause_requested=pause_requested,
            )
            outcome = completed.get("outcome")
            if outcome == "closed":
                result["subtitle_gaps_closed"].append(gap_id)
                result["subtitle_attempts"].append({
                    "gap_id": gap_id, "tier": _SUBTITLE_TIER,
                    "outcome": "closed", "bilingual": bool(entry.get("bilingual")),
                })
            elif outcome == "paused":
                result["paused"] = True
                break
            elif outcome == FAILURE_CANDIDATE:
                result["subtitle_attempts"].append({
                    "gap_id": gap_id, "tier": _SUBTITLE_TIER,
                    "outcome": FAILURE_CANDIDATE,
                })
                candidate_failed = True
            elif outcome == "waiting_reconcile":
                result["subtitle_waiting"] = "waiting_reconcile"
        if result["paused"]:
            break

    # Cleanup never walks a non-empty tree and is skipped while any intent
    # still references that attempt.  This leaves uncertain staging untouched
    # for an operator/recovery read rather than deleting evidence.
    still_active = {
        str(entry.get("staging_root"))
        for entry in _subtitle_intents(state).values()
        if isinstance(entry, Mapping) and entry.get("staging_root")
    }
    for staging_root in sorted(active_staging_roots - still_active):
        _cleanup_empty_subtitle_staging(
            runner, staging_root, pause_requested=pause_requested,
        )

    if _subtitle_intents(state):
        result["subtitle_waiting"] = "waiting_reconcile"
    elif result["subtitle_waiting"] is None and candidate_failed:
        result["subtitle_waiting"] = "retry_wait"
    return result


def _exclude_locator(state: dict[str, Any], tier: str, locator: str) -> None:
    """Add one locator to the durable per-tier candidate failure memory."""
    if not locator:
        return
    failures = state.get("candidate_failures_by_provider")
    if not isinstance(failures, dict):
        failures = {}
        state["candidate_failures_by_provider"] = failures
    rows = failures.get(tier)
    if not isinstance(rows, list):
        rows = []
        failures[tier] = rows
    if locator not in rows:
        rows.append(locator)


def run_root_replenishment(
    runner,
    state_root: Path,
    root_task_id: str,
    *,
    search_runner=None,
    materializer_factory=None,
    subtitle_materializer_factory: Callable[[], Any] | Any | None = None,
    pause_requested=None,
) -> dict[str, Any]:
    """Run one replenishment round for the root task's open gaps.

    One invocation processes every open gap for the *current* tier and returns
    a JSON-serializable summary.  Progressing to the next tier happens only
    after a complete no-candidate proof across all open requests; retry/reconcile
    waits pin the tier and never re-submit an in-flight gap.
    """
    state_root = Path(state_root)

    def pause() -> bool:
        """Read the root fence defensively; unknown control means stopped."""
        if pause_requested is None:
            return False
        try:
            return bool(pause_requested())
        except Exception:
            return True

    materializer_pause = pause if pause_requested is not None else None
    if materializer_factory is None:
        archive_preprocessor = getattr(runner, "archive_preprocessor", None)

        def factory(tier: str):
            return _default_materializer_factory(
                tier,
                archive_preprocessor=archive_preprocessor,
            )
    else:
        factory = materializer_factory

    state = load_root_replenishment_state(state_root, root_task_id)
    tier = str(state.get("tier") or TIER_QUARK_SHARE)
    in_flight = {
        str(key)
        for key in (state.get(_IN_FLIGHT_KEY) or {})
        if isinstance(key, str) and key
    }

    # J/N discipline: close gaps the library already proves present before
    # anything reaches the acquisition lane.  Phantom ledger rows (historical
    # registration bugs) must never be searched, submitted or downloaded.
    try:
        from .gap_reaudit import reaudit_open_gaps
        reaudit = reaudit_open_gaps(runner, state_root, root_task_id)
        if reaudit.get("closed"):
            _trace(
                f"reaudit root={root_task_id} closed={len(reaudit['closed'])} "
                f"kept={len(reaudit.get('kept_open') or [])}",
            )
    except Exception:
        reaudit = {}

    requests = gap_ledger_requests(state_root, root_task_id)
    _trace(f"start root={root_task_id} tier={tier} requests={len(requests)}")

    # Operator-confirmed identities may carry no title; fill it from TMDB so
    # the selection boundary has real alias evidence.  The same read-only
    # detail response is the only source trusted for an optional bilingual
    # original language.
    for request in requests:
        _enrich_media_titles(runner, request)

    # Subtitle-only delivery is intentionally completed before any video tier
    # can reach a search/materializer/write boundary.  A pause requested here
    # therefore cannot be followed by an unrelated video acquisition.
    subtitle_result = _run_root_subtitle_channel(
        runner,
        state_root,
        root_task_id,
        state,
        requests,
        subtitle_materializer_factory=subtitle_materializer_factory,
        pause_requested=materializer_pause,
    )

    def attach_subtitle_result(result: dict[str, Any]) -> dict[str, Any]:
        result.update({
            "subtitle_requests_built": subtitle_result["subtitle_requests_built"],
            "subtitle_attempts": subtitle_result["subtitle_attempts"],
            "subtitle_gaps_closed": subtitle_result["subtitle_gaps_closed"],
            "subtitle_waiting": subtitle_result["subtitle_waiting"],
            "paused": subtitle_result["paused"],
        })
        return result

    def merged_waiting(video_waiting: str | None) -> str | None:
        subtitle_waiting = subtitle_result.get("subtitle_waiting")
        if "waiting_reconcile" in {video_waiting, subtitle_waiting}:
            return "waiting_reconcile"
        if "retry_wait" in {video_waiting, subtitle_waiting}:
            return "retry_wait"
        return video_waiting

    if subtitle_result["paused"]:
        state["updated_at"] = _now()
        state["waiting"] = merged_waiting(None)
        save_root_replenishment_state(state_root, root_task_id, state)
        return attach_subtitle_result({
            "tier": tier,
            "tier_before": tier,
            "requests_built": 0,
            "attempts": [],
            "gaps_closed": [],
            "state": state,
            "waiting": state["waiting"],
        })

    if not requests:
        state["updated_at"] = _now()
        state["waiting"] = merged_waiting(None)
        save_root_replenishment_state(state_root, root_task_id, state)
        noop = _noop_result(state, tier)
        noop["waiting"] = state["waiting"]
        return attach_subtitle_result(noop)

    # Drop in-flight (in_doubt) gaps so the same coordinate is never re-submitted.
    # Subtitle rows have already had their independent direct-sidecar pass;
    # they never enter the two video tiers or their full-media search.
    filtered: list[dict[str, Any]] = []
    for request in requests:
        rows = [
            row for row in (request.get("gaps") or [])
            if isinstance(row, Mapping)
            and str(row.get("id") or "") not in in_flight
            and row.get("kind") != "missing_subtitle"
        ]
        if rows:
            copied = dict(request)
            copied["gaps"] = rows
            filtered.append(copied)
    requests = filtered

    # Current materializers do not have a poll-only reconciliation hook.  A
    # durable in-doubt token therefore remains an explicit barrier rather
    # than being re-submitted or converted into another provider's attempt.
    reconcile_attempts: list[dict[str, Any]] = []
    reconcile_closed: list[str] = []
    reconcile_waiting: str | None = (
        "waiting_reconcile" if in_flight else None
    )

    if not requests:
        # Subtitle-only leftovers are the subtitle channel's job and must not
        # loop the video tiers; parked tokens remain a durable barrier.
        still_parked = bool(state.get(_IN_FLIGHT_KEY))
        waiting = (
            reconcile_waiting
            if reconcile_waiting is not None
            else ("waiting_reconcile" if still_parked else None)
        )
        for entry in reconcile_attempts:
            _append_attempt_log(state, {
                "gap_id": entry["gap_id"],
                "tier": entry["tier"],
                "outcome": entry["outcome"],
                "candidate_key": entry.get("candidate_key"),
                "recorded_at": _now(),
            })
        state["updated_at"] = _now()
        waiting = merged_waiting(waiting)
        state["waiting"] = waiting
        save_root_replenishment_state(state_root, root_task_id, state)
        return attach_subtitle_result({
            "tier": tier,
            "tier_before": tier,
            "requests_built": 0,
            "attempts": reconcile_attempts,
            "gaps_closed": reconcile_closed,
            "state": state,
            "waiting": waiting,
        })

    requests_built = len(requests)
    attempts: list[dict[str, Any]] = list(reconcile_attempts)
    gaps_closed: list[str] = list(reconcile_closed)
    waiting: str | None = None

    hit_in_doubt = False
    hit_infrastructure = False
    in_doubt_task_ids: dict[str, str | None] = {}
    failed_locators: set[str] = set()
    newly_in_flight: dict[str, str | None] = {}
    no_candidate_proofs: list[Mapping[str, Any]] = []
    searched_requests = 0
    paused_during_round = False
    shelf = _root_target_shelf(runner, root_task_id)

    for request in requests:
        if pause():
            paused_during_round = True
            break
        media = request.get("media") or {}
        media_type = str(media.get("media_type") or "").strip().casefold()
        tmdb_id = media.get("tmdb_id")

        by_token, by_unit = _open_gaps_by_token(
            state_root, root_task_id, media_type, tmdb_id,
        )

        request["tier"] = tier
        # Feed the durable per-tier candidate failures back as excluded
        # locators so a Quark-rejected candidate is never re-submitted.
        excluded = [
            {"locator": locator}
            for locator in (
                (state.get("candidate_failures_by_provider") or {}).get(tier) or []
            )
            if isinstance(locator, str) and locator
        ]
        if excluded:
            request["excluded_candidates"] = excluded
        _trace(f"select root={root_task_id} tier={tier} tmdb={tmdb_id} gaps={len(request.get('gaps') or [])}")
        try:
            bundle = gap_ledger_selection(
                state_root, root_task_id, request, search_runner=search_runner,
            )
        except Exception as exc:  # search boundary failure
            scope, task_id = _classify_error(exc)
            # A search exception has not inspected or submitted a concrete
            # locator.  Even if a lower adapter labels it ``candidate``, it
            # cannot establish no-candidate exhaustion, so retry this same
            # tier instead of leaving an unprovable silent stop.
            if scope == FAILURE_CANDIDATE:
                scope = FAILURE_INFRASTRUCTURE
            for row in request.get("gaps") or []:
                token = str(row.get("id") or "")
                for gap in by_token.get(token, ()):
                    record_attempt(
                        state_root, root_task_id, gap.gap_id,
                        attempt_id=uuid.uuid4().hex,
                        provider=_TIER_PROVIDER.get(tier, tier),
                        tier=tier,
                        locator=None,
                        status=_attempt_status(scope),
                        external_task_id=task_id,
                        error=str(exc)[:200] or None,
                    )
                    attempts.append({
                        "gap_id": gap.gap_id,
                        "tier": tier,
                        "outcome": scope,
                    })
            if scope == FAILURE_IN_DOUBT:
                hit_in_doubt = True
                in_doubt_task_ids.update({
                    str(row.get("id") or ""): task_id
                    for row in request.get("gaps") or []
                })
                for row in request.get("gaps") or []:
                    token = str(row.get("id") or "")
                    if token:
                        newly_in_flight[token] = task_id
            elif scope == FAILURE_INFRASTRUCTURE:
                hit_infrastructure = True
            continue

        if not isinstance(bundle, Mapping):
            bundle = {}
        searched_requests += 1
        if pause():
            # A completed read-only search is not permission to certify the
            # request or advance the tier after the operator pauses.
            paused_during_round = True
            break
        selections = bundle.get("selections")
        selections = selections if isinstance(selections, list) else []
        _trace(f"selected root={root_task_id} tier={tier} tmdb={tmdb_id} selections={len(selections)}")

        if not selections:
            evidence = bundle.get("search_evidence")
            proof_complete, completed_sources = _search_evidence_completion(
                tier,
                evidence if isinstance(evidence, Mapping) else None,
                shelf=shelf,
            )
            if proof_complete:
                no_candidate_proofs.append({
                    "completed_sources": completed_sources,
                })
            else:
                # Search returned no selectable candidate but did not prove
                # the active tier exhausted.  This is never a candidate
                # exclusion: keep the tier and re-arm the same lane.
                hit_infrastructure = True
                _trace(
                    f"no exhaustion proof root={root_task_id} tier={tier} "
                    f"tmdb={tmdb_id}",
                )
            for row in request.get("gaps") or []:
                token = str(row.get("id") or "")
                for gap in by_token.get(token, ()):
                    attempts.append({
                        "gap_id": gap.gap_id,
                        "tier": tier,
                        "outcome": (
                            FAILURE_CANDIDATE
                            if proof_complete else FAILURE_INFRASTRUCTURE
                        ),
                    })
            continue

        materializer = factory(tier)

        for selection in selections:
            if not isinstance(selection, Mapping):
                continue
            # Video tiers may not piggy-back an old torrent companion
            # subtitle.  RootJob's independent subtitle channel is the only
            # route allowed to stage and formally install the merged file.
            selection = _strip_video_companion_subtitle_members(selection)
            if pause():
                paused_during_round = True
                break
            selected_tokens = [
                str(gid) for gid in (selection.get("selected_gap_ids") or [])
                if isinstance(gid, str) and gid
            ]
            covered_gaps: list[Gap] = []
            for token in selected_tokens:
                covered_gaps.extend(by_token.get(token, ()))
            covered_gaps = list({
                gap.gap_id: gap for gap in covered_gaps
            }.values())
            if not covered_gaps:
                continue

            locator = str(selection.get("locator") or "")
            provider = str(selection.get("provider") or _TIER_PROVIDER.get(tier, tier))
            _trace(f"materialize root={root_task_id} tier={tier} locator={locator[:60]!r}")
            attempt_id = uuid.uuid4().hex
            staging_root = (
                f"{str(runner.library_root).rstrip('/')}"
                f"{_STAGING_NAMESPACE}/{root_task_id}/{attempt_id}"
            )
            workspace = (
                state_root / "replenishment_workspace"
                / root_task_id / attempt_id
            )
            # Creating an attempt workspace is also a provider-owned local
            # mutation.  Do not allocate a new staging directory once the
            # root/pilot fence was withdrawn between selection and submit.
            if pause():
                paused_during_round = True
                break
            try:
                workspace.mkdir(parents=True, exist_ok=True)
            except OSError:
                workspace = state_root

            # record_attempt BEFORE the external submit (contract).
            for gap in covered_gaps:
                record_attempt(
                    state_root, root_task_id, gap.gap_id,
                    attempt_id=attempt_id,
                    provider=provider,
                    tier=tier,
                    locator=locator or None,
                    status="submitted",
                )
            state["last_attempt_at"] = _now()

            try:
                delivery = _call_materializer_with_pause(
                    materializer.acquire,
                    request,
                    [selection],
                    staging_root=staging_root,
                    workspace=workspace,
                    alist=runner.alist,
                    pause_requested=materializer_pause,
                )
            except Exception as exc:
                if _is_pause_error(exc):
                    paused_during_round = True
                    break
                scope, task_id = _classify_error(exc)
                for gap in covered_gaps:
                    record_attempt(
                        state_root, root_task_id, gap.gap_id,
                        attempt_id=attempt_id,
                        provider=provider,
                        tier=tier,
                        locator=locator or None,
                        status=_attempt_status(scope),
                        external_task_id=task_id,
                        error=str(exc)[:200] or None,
                    )
                    attempts.append({
                        "gap_id": gap.gap_id,
                        "tier": tier,
                        "outcome": scope,
                        **({"candidate_key": locator} if locator else {}),
                    })
                if scope == FAILURE_IN_DOUBT:
                    hit_in_doubt = True
                    for gap in covered_gaps:
                        token = _bridge_token(gap) or gap.gap_id
                        newly_in_flight[token] = task_id
                elif scope == FAILURE_INFRASTRUCTURE:
                    hit_infrastructure = True
                else:
                    if locator:
                        failed_locators.add(locator)
                continue

            if not isinstance(delivery, Mapping):
                for gap in covered_gaps:
                    record_attempt(
                        state_root, root_task_id, gap.gap_id,
                        attempt_id=attempt_id,
                        provider=provider,
                        tier=tier,
                        locator=locator or None,
                        status="candidate_failed",
                        error="补源 materializer 返回无效 delivery",
                    )
                    attempts.append({
                        "gap_id": gap.gap_id,
                        "tier": tier,
                        "outcome": FAILURE_CANDIDATE,
                        **({"candidate_key": locator} if locator else {}),
                    })
                if locator:
                    failed_locators.add(locator)
                continue

            # Writer closed loop: plan + execute an internal child, then prove
            # coverage from the executed plan's files before closing any gap.
            try:
                if pause():
                    break
                child_request = _child_request(
                    runner, state_root, root_task_id, request, delivery,
                )
                child = runner.plan_job(
                    child_request,
                    internal_child_of=root_task_id,
                    pause_requested=pause,
                )
                if pause():
                    break
                executed = runner.execute_job(child.id, pause_requested=pause)
                if executed.phase != "executed":
                    # See the reconciliation path above: an active paused
                    # child has no proof of delivered coverage yet.
                    if pause():
                        break
                    raise RuntimeError("补源 internal child 未完成")
                executed_plan = (
                    executed.plan if isinstance(executed.plan, Mapping) else {}
                )
            except Exception as exc:
                if _is_pause_error(exc):
                    # A child plan/write pause has no acceptance evidence and
                    # no failed candidate/infrastructure evidence. Stop this
                    # round without writing a retry attempt or trying another
                    # provider selection.
                    paused_during_round = True
                    break
                scope, task_id = _classify_error(exc)
                for gap in covered_gaps:
                    record_attempt(
                        state_root, root_task_id, gap.gap_id,
                        attempt_id=attempt_id,
                        provider=provider,
                        tier=tier,
                        locator=locator or None,
                        status=_attempt_status(scope),
                        external_task_id=task_id,
                        error=str(exc)[:200] or None,
                    )
                    attempts.append({
                        "gap_id": gap.gap_id,
                        "tier": tier,
                        "outcome": scope,
                        **({"candidate_key": locator} if locator else {}),
                    })
                if scope == FAILURE_IN_DOUBT:
                    hit_in_doubt = True
                    for gap in covered_gaps:
                        token = _bridge_token(gap) or gap.gap_id
                        newly_in_flight[token] = task_id
                elif scope == FAILURE_INFRASTRUCTURE:
                    hit_infrastructure = True
                else:
                    if locator:
                        failed_locators.add(locator)
                continue

            for gap in covered_gaps:
                if _prove_gap_coverage(gap, by_unit, executed_plan):
                    try:
                        close_gap(state_root, root_task_id, gap.gap_id)
                    except KeyError:
                        continue
                    gaps_closed.append(gap.gap_id)
                    attempts.append({
                        "gap_id": gap.gap_id,
                        "tier": tier,
                        "outcome": "closed",
                        **({"candidate_key": locator} if locator else {}),
                    })
                else:
                    record_attempt(
                        state_root, root_task_id, gap.gap_id,
                        attempt_id=attempt_id,
                        provider=provider,
                        tier=tier,
                        locator=locator or None,
                        status="candidate_failed",
                        error="补源执行后未证明缺口被覆盖",
                    )
                    attempts.append({
                        "gap_id": gap.gap_id,
                        "tier": tier,
                        "outcome": FAILURE_CANDIDATE,
                        **({"candidate_key": locator} if locator else {}),
                    })
                    if locator:
                        failed_locators.add(locator)

        if pause():
            paused_during_round = True
            break

    # Tier progression / waiting (contract rule 4).
    # Candidate-locator memory is recorded FIRST so a mixed run (one
    # infrastructure failure plus several rejected candidates) never loses
    # the rejected locators.  A pause, incomplete search proof, or in-doubt
    # task can land after a materializer reports a candidate failure: retain
    # that evidence, but defer the policy update so the 30-locator exhaustion
    # threshold cannot promote a tier while the round is not fully clean.
    if pause():
        paused_during_round = True
    defer_candidate_transition = (
        paused_during_round
        or hit_infrastructure
        or hit_in_doubt
        or reconcile_waiting in {"retry_wait", "waiting_reconcile"}
    )
    if defer_candidate_transition:
        for locator in sorted(failed_locators):
            _exclude_locator(state, tier, locator)
    else:
        for locator in sorted(failed_locators):
            state = apply_tier_outcome(state, {
                "scope": FAILURE_CANDIDATE,
                "locator": locator,
            })
    if hit_in_doubt or reconcile_waiting == "waiting_reconcile":
        waiting = "waiting_reconcile"
        if hit_in_doubt:
            state = apply_tier_outcome(state, {
                "scope": FAILURE_IN_DOUBT,
                "external_task_id": next(iter(in_doubt_task_ids.values()), None),
            })
        state[_IN_FLIGHT_KEY] = {
            **state.get(_IN_FLIGHT_KEY, {}),
            **newly_in_flight,
        }
    elif hit_infrastructure or reconcile_waiting == "retry_wait":
        waiting = "retry_wait"
        state = apply_tier_outcome(state, {"scope": FAILURE_INFRASTRUCTURE})
    elif (
        requests_built > 0
        and not paused_during_round
        and not pause()
        and searched_requests == requests_built
        and len(no_candidate_proofs) == requests_built
    ):
        # No selector result is allowed to manufacture this outcome.  Every
        # request independently supplied a complete raw-search proof above;
        # only then can their already-observed source evidence be aggregated
        # into the policy transition.
        completed_sources = sorted({
            str(source).strip().casefold()
            for proof in no_candidate_proofs
            for source in (proof.get("completed_sources") or [])
            if isinstance(source, str) and source.strip()
        })
        # The preceding check closes the normal loop boundary; this one is
        # deliberately adjacent to the durable state transition so a pause
        # that arrives while the aggregate proof is being assembled cannot
        # promote the next tier.
        if pause():
            paused_during_round = True
        else:
            state = apply_tier_outcome(state, {
                "scope": FAILURE_CANDIDATE,
                "search_complete_no_candidates": True,
                "completed_sources": completed_sources,
                "unchecked_secondary_candidates": 0,
                **({"shelf": shelf} if shelf is not None else {}),
            })

    waiting = merged_waiting(waiting)
    state["updated_at"] = _now()
    state["waiting"] = waiting
    _trace(f"end root={root_task_id} tier={state.get('tier')} waiting={waiting} closed={len(gaps_closed)}")
    for entry in attempts:
        _append_attempt_log(state, {
            "gap_id": entry["gap_id"],
            "tier": entry["tier"],
            "outcome": entry["outcome"],
            "candidate_key": entry.get("candidate_key"),
            "recorded_at": _now(),
        })
    save_root_replenishment_state(state_root, root_task_id, state)

    return attach_subtitle_result({
        "tier": str(state.get("tier") or tier),
        "tier_before": tier,
        "requests_built": requests_built,
        "attempts": attempts,
        "gaps_closed": gaps_closed,
        "state": state,
        "waiting": waiting,
    })


__all__ = [
    "load_root_replenishment_state",
    "run_root_replenishment",
    "save_root_replenishment_state",
]
