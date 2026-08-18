"""P14: drive the new-path Gap ledger through the three-tier replenishment chain.

The new architecture records precise gaps in ``gap_ledger_<root_task_id>.json``
(see ``engine.scrapeflow.gap_ledger``).  This module is the orchestrator that
closes those gaps with the existing, frozen three-tier replenishment machinery:

    quark_share  -> QuarkFastSaveAutomaticMaterializer
    alist_offline -> AlistOfflineAutomaticMaterializer
    magnet       -> LocalTorrentAutomaticMaterializer

It never instantiates the legacy ``AutomaticReplenishmentRuntime`` (which
writes forbidden ``EngineJob.summary.replenishment`` projections).  Instead it
reuses the thin, injectable per-tier *materializers* and the bridge/search
boundaries directly, and drives its own durable tier state.

``missing_subtitle`` gaps are deliberately left open by this module: contract
rule 4 routes subtitles through the independent subtitle channel (subtitle
sites, subtitle-only members, no full-video downloads), so the three video
tiers here never try to acquire them.

Tier progression (contract rule 4) is delegated to the pure
``replenishment_tiers.apply_tier_outcome`` policy:

* ``candidate`` failures accumulate (per locator);
* the tier advances only on a complete no-candidate proof across every open
  request for the tier;
* ``infrastructure`` -> ``waiting == "retry_wait"`` (same tier, never downgrade);
* ``in_doubt`` -> ``waiting == "waiting_reconcile"`` (same tier, and the in-flight
  gap ids are remembered in the durable state so they are never re-submitted).

Parked ``in_doubt`` coordinates are NOT stranded: every round re-enters their
durable attempts through the materializer's reconcile hook (poll-only, never
re-submit) before any fresh search runs, so a finished AList offline task is
finalized and its gaps closed by the normal writer closure.

The durable state lives at ``state_root / replenishment_<root_task_id>.json``
and is written with ``engine.scrapeflow.serialization.atomic_write_json``.  It
merges the pure tier-policy fields (``tier``, ``candidate_failures_by_provider``,
``exhaustion_proof_by_provider``, ``last_error_scope``) with the orchestration
fields documented below.

Reused vs. wrapped materializers
--------------------------------

The three per-tier materializer classes are reused **as-is** through the
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
from engine.scrapeflow.target_shelf import target_root_for_shelf
from engine.scrapeflow.work_units import load_work_unit_records

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
    TIER_ALIST_OFFLINE,
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
_MAX_ATTEMPT_LOG = 200
_STAGING_NAMESPACE = "/ScrapeFlow/补源"

_KNOWN_FAILURE_SCOPES = frozenset({
    FAILURE_CANDIDATE, FAILURE_INFRASTRUCTURE, FAILURE_IN_DOUBT,
})


def _trace(message: str) -> None:
    """Bounded live observability line (mirrors the legacy runner's tracer)."""
    print(f"[root-replenishment] {message}", flush=True)

# The provider name for each tier equals the tier name (quark_share /
# alist_offline / magnet), which is also what the materializers validate.
_TIER_PROVIDER = {
    TIER_QUARK_SHARE: TIER_QUARK_SHARE,
    TIER_ALIST_OFFLINE: TIER_ALIST_OFFLINE,
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
    })
    return state


def _normalize_state(raw: Mapping[str, Any]) -> dict[str, Any]:
    state = dict(raw)
    if state.get("tier") not in STRICT_TIER_ORDER:
        return _fresh_state()
    state.setdefault("candidate_failures_by_provider", {})
    state.setdefault("exhaustion_proof_by_provider", {})
    state.setdefault("last_error_scope", None)
    state.setdefault("updated_at", _now())
    state.setdefault("last_attempt_at", None)
    state.setdefault("waiting", None)
    state.setdefault("attempt_log", [])
    state.setdefault(_IN_FLIGHT_KEY, {})
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
    if tier == TIER_ALIST_OFFLINE:
        from .automatic_replenishment import AlistOfflineAutomaticMaterializer
        return AlistOfflineAutomaticMaterializer()
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


def _external_task_is_final(error: BaseException) -> bool:
    """Whether an infrastructure failure proved its external task stopped.

    Ordinary infrastructure failures retain an in-doubt token because the
    remote task may still be consuming bytes or complete later.  The AList
    transfer-deadline path supplies this marker only after a fresh confirmed
    cancellation, which permits a same-tier retry without re-submitting a
    still-live task.
    """
    return any(
        getattr(item, "external_task_final", False) is True
        for item in _exception_chain(error)
    )


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
    """Fill an empty bridged media title/aliases from TMDB.

    An operator-confirmed unit identity may carry only ``media_type +
    tmdb_id`` (no title), which would leave the bridge with empty aliases and
    the selector would reject every candidate.  The TMDB client is the
    canonical title source; enrichment is best-effort and never overrides a
    non-empty alias list.
    """
    media = request.get("media")
    if not isinstance(media, dict):
        return
    aliases = media.get("aliases")
    if isinstance(aliases, list) and aliases:
        return
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
    if not title:
        return
    media["title"] = title
    if original_title:
        media["original_title"] = original_title
    media["aliases"] = _nonempty_strings([
        title,
        original_title,
        *(media.get("aliases") if isinstance(media.get("aliases"), list) else []),
    ])


def _open_gaps_for_token(
    state_root: Path,
    root_task_id: str,
    token: str,
) -> list[Gap]:
    """Return the open ledger gaps whose bridge token equals ``token``."""
    output: list[Gap] = []
    for gap in load_gap_ledger(state_root, root_task_id):
        if gap.status != "open":
            continue
        if _bridge_token(gap) == token:
            output.append(gap)
    return output


def _latest_parked_attempt(
    gaps: Sequence[Gap],
    recorded_task_id: str | None,
) -> Any | None:
    """Return the newest durable attempt that parked one of these gaps.

    Only submitted/in_doubt rows are considered (those are the rows written
    before/at the external submit); terminal outcome rows describe history.
    When the durable in-flight map carries a task id, a candidate attempt must
    match it — a mismatched row belongs to an older submit.
    """
    candidates: list[Any] = []
    for gap in gaps:
        for attempt in gap.attempts:
            if attempt.status not in {"submitted", "in_doubt"}:
                continue
            if (
                recorded_task_id
                and attempt.external_task_id
                and attempt.external_task_id != recorded_task_id
            ):
                continue
            candidates.append(attempt)
    if not candidates:
        return None
    return max(candidates, key=lambda item: item.recorded_at)


def _read_alist_offline_attempt_state(workspace: Path) -> dict[str, Any] | None:
    """Read the durable alist_offline attempt state; ``None`` when unusable."""
    import json as _json

    path = workspace / "alist_offline_attempt.json"
    if not path.exists():
        return None
    try:
        raw = _json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, ValueError):
        return None
    if not isinstance(raw, Mapping):
        return None
    return dict(raw)


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


def _reconcile_in_flight_tokens(
    runner,
    state_root: Path,
    root_task_id: str,
    state: dict[str, Any],
    factory,
    pause,
    materializer_pause: Callable[[], bool] | None,
) -> tuple[list[dict[str, Any]], list[str], str | None]:
    """Re-enter parked in_doubt attempts through their durable task ids.

    The removed ``quark_magnet`` lane can never reconcile: its parked tokens
    are un-parked so the coordinate re-enters the current tier ladder.
    ``alist_offline`` attempts re-enter the materializer's reconcile hook
    (poll-only, never re-submit) and go through the same writer closure as a
    fresh attempt.  Failures follow the same three-scope classification:
    candidate excludes the locator and un-parks, infrastructure pins the token
    for retry unless it proves the task was cancelled, and in_doubt keeps the
    token parked for the next round.
    """
    in_flight = state.get(_IN_FLIGHT_KEY)
    if not isinstance(in_flight, Mapping):
        in_flight = {}
        state[_IN_FLIGHT_KEY] = {}
    attempts: list[dict[str, Any]] = []
    closed: list[str] = []
    waiting: str | None = None
    parked = dict(in_flight)
    removed_tokens: set[str] = set()

    for token, recorded_task_id in sorted(parked.items()):
        if pause():
            break
        gaps = _open_gaps_for_token(state_root, root_task_id, token)
        if not gaps:
            removed_tokens.add(token)
            continue
        attempt = _latest_parked_attempt(gaps, recorded_task_id)
        if attempt is None:
            # No durable attempt evidence: nothing provable to reconcile, and
            # nothing keeps the coordinate parked.
            removed_tokens.add(token)
            continue
        attempt_tier = str(attempt.tier or "").strip().casefold()
        if attempt_tier == "quark_magnet":
            # The lane that parked this token no longer exists.  Un-park so
            # the coordinate re-enters the current tier ladder.
            removed_tokens.add(token)
            continue
        if attempt_tier != TIER_ALIST_OFFLINE:
            # Other live lanes have no reconcile hook; keep the token parked.
            waiting = waiting or "waiting_reconcile"
            continue
        workspace = (
            state_root / "replenishment_workspace" / root_task_id / attempt.attempt_id
        )
        attempt_state = _read_alist_offline_attempt_state(workspace)
        if attempt_state is None:
            # Fail closed: keep the token parked for a later round.
            waiting = waiting or "waiting_reconcile"
            continue
        staging_root = attempt_state.get("staging_root")
        acquisition = attempt_state.get("acquisition")
        selected = attempt_state.get("selected_gap_ids")
        locator = attempt_state.get("locator")
        if (
            not isinstance(staging_root, str)
            or not staging_root
            or not isinstance(acquisition, Mapping)
            or not isinstance(selected, list)
            or not selected
        ):
            waiting = waiting or "waiting_reconcile"
            continue
        selection: dict[str, Any] = {
            "provider": str(attempt_state.get("provider") or TIER_ALIST_OFFLINE),
            "locator": str(locator or ""),
            "selected_gap_ids": [
                str(gap_id) for gap_id in selected
                if isinstance(gap_id, str) and gap_id
            ],
            "acquisition": dict(acquisition),
        }
        media_type = str(gaps[0].media_type).strip().casefold()
        tmdb_id = gaps[0].tmdb_id
        gap_rows: list[dict[str, Any]] = [
            {
                "id": token,
                "kind": gap.kind,
                "season": gap.season,
                "episodes": list(gap.episodes),
                "title": "",
            }
            for gap in gaps
        ]
        request: dict[str, Any] = {
            "tier": TIER_ALIST_OFFLINE,
            "media": {"media_type": media_type, "tmdb_id": tmdb_id},
            "gaps": gap_rows,
        }
        by_token, by_unit = _open_gaps_by_token(
            state_root, root_task_id, media_type, tmdb_id,
        )
        provider = str(attempt.provider or TIER_ALIST_OFFLINE)

        def record_outcome(scope: str, task_id: str | None, error: str) -> None:
            for gap in gaps:
                record_attempt(
                    state_root, root_task_id, gap.gap_id,
                    attempt_id=attempt.attempt_id,
                    provider=provider,
                    tier=TIER_ALIST_OFFLINE,
                    locator=str(locator or "") or None,
                    status=_attempt_status(scope),
                    external_task_id=task_id,
                    error=error[:200] or None,
                )
                attempts.append({
                    "gap_id": gap.gap_id,
                    "tier": TIER_ALIST_OFFLINE,
                    "outcome": scope,
                    **({"candidate_key": str(locator)} if locator else {}),
                })

        try:
            materializer = factory(TIER_ALIST_OFFLINE)
            method = getattr(materializer, "reconcile_existing_task", None)
            if not callable(method):
                raise ValueError("AList 离线 materializer 缺少 reconcile_existing_task")
            delivery = _call_materializer_with_pause(
                method,
                request, [selection], staging_root=staging_root,
                workspace=workspace, alist=runner.alist,
                external_task_id=recorded_task_id,
                pause_requested=materializer_pause,
            )
        except Exception as exc:
            if _is_pause_error(exc):
                waiting = waiting or "retry_wait"
                break
            scope, task_id = _classify_error(exc)
            record_outcome(scope, task_id, str(exc))
            if scope == FAILURE_IN_DOUBT:
                waiting = waiting or "waiting_reconcile"
            elif scope == FAILURE_INFRASTRUCTURE:
                waiting = waiting or "retry_wait"
                if _external_task_is_final(exc):
                    # AList confirmed cancellation after a bounded transfer.
                    # It is now safe to let the next same-tier retry create a
                    # new task; keeping this token would deadlock recovery.
                    removed_tokens.add(token)
            else:
                removed_tokens.add(token)
                _exclude_locator(state, TIER_ALIST_OFFLINE, str(locator or ""))
            continue

        if not isinstance(delivery, Mapping):
            removed_tokens.add(token)
            _exclude_locator(state, TIER_ALIST_OFFLINE, str(locator or ""))
            record_outcome(FAILURE_CANDIDATE, None, "对账 materializer 返回无效 delivery")
            continue

        # Writer closure (same as a fresh attempt).
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
                # A cooperative pause inside the writer returns its durable
                # active carrier.  Its plan is not acceptance evidence and
                # must never close a gap before fresh recovery completes.
                if pause():
                    break
                raise RuntimeError("补源 internal child 未完成")
            executed_plan = (
                executed.plan if isinstance(executed.plan, Mapping) else {}
            )
        except Exception as exc:
            if _is_pause_error(exc):
                # The child may have reached a cooperative plan/write fence.
                # It is not provider infrastructure and must not create a
                # new attempt or advance the parked token.
                waiting = waiting or "retry_wait"
                break
            scope, task_id = _classify_error(exc)
            record_outcome(scope, task_id, str(exc))
            if scope == FAILURE_IN_DOUBT:
                waiting = waiting or "waiting_reconcile"
            elif scope == FAILURE_INFRASTRUCTURE:
                waiting = waiting or "retry_wait"
            else:
                removed_tokens.add(token)
                _exclude_locator(state, TIER_ALIST_OFFLINE, str(locator or ""))
            continue

        for gap in gaps:
            if _prove_gap_coverage(gap, by_unit, executed_plan):
                try:
                    close_gap(state_root, root_task_id, gap.gap_id)
                except KeyError:
                    continue
                closed.append(gap.gap_id)
                attempts.append({
                    "gap_id": gap.gap_id,
                    "tier": TIER_ALIST_OFFLINE,
                    "outcome": "closed",
                    **({"candidate_key": str(locator)} if locator else {}),
                })
                removed_tokens.add(token)
            else:
                record_outcome(FAILURE_CANDIDATE, None, "对账执行后未证明缺口被覆盖")
                removed_tokens.add(token)
                _exclude_locator(state, TIER_ALIST_OFFLINE, str(locator or ""))

    if removed_tokens:
        state[_IN_FLIGHT_KEY] = {
            key: value for key, value in parked.items()
            if key not in removed_tokens
        }
    return attempts, closed, waiting


def run_root_replenishment(
    runner,
    state_root: Path,
    root_task_id: str,
    *,
    search_runner=None,
    materializer_factory=None,
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
    if not requests:
        return _noop_result(state, tier)

    # Operator-confirmed identities may carry no title; fill it from TMDB so
    # the selection boundary has real alias evidence.
    for request in requests:
        _enrich_media_titles(runner, request)

    # Drop in-flight (in_doubt) gaps so the same coordinate is never re-submitted.
    # missing_subtitle gaps are NOT served by the three video tiers: contract
    # rule 4 routes them through the independent subtitle channel, so they stay
    # open in the ledger for that channel instead of downloading whole videos.
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

    # Reconcile parked in_doubt coordinates BEFORE any fresh search: their
    # durable AList tasks may have finished since the last round.  This runs
    # even when fresh requests exist — otherwise a mixed round would strand
    # parked tokens until every other coordinate drained.
    reconcile_attempts: list[dict[str, Any]] = []
    reconcile_closed: list[str] = []
    reconcile_waiting: str | None = None
    if in_flight:
        try:
            (
                reconcile_attempts,
                reconcile_closed,
                reconcile_waiting,
            ) = _reconcile_in_flight_tokens(
                runner, state_root, root_task_id, state, factory, pause,
                materializer_pause,
            )
        except Exception:
            # Fail closed: keep parked tokens for a later round.
            import traceback
            traceback.print_exc()
            reconcile_waiting = "waiting_reconcile"

    if not requests:
        # Subtitle-only leftovers are the subtitle channel's job and must not
        # loop the video tiers; parked tokens were just reconciled above (the
        # reconcile pass rewrites ``in_flight_gap_ids``, so re-read it here).
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
        state["waiting"] = waiting
        save_root_replenishment_state(state_root, root_task_id, state)
        return {
            "tier": tier,
            "tier_before": tier,
            "requests_built": 0,
            "attempts": reconcile_attempts,
            "gaps_closed": reconcile_closed,
            "state": state,
            "waiting": waiting,
        }

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

    return {
        "tier": str(state.get("tier") or tier),
        "tier_before": tier,
        "requests_built": requests_built,
        "attempts": attempts,
        "gaps_closed": gaps_closed,
        "state": state,
        "waiting": waiting,
    }


__all__ = [
    "load_root_replenishment_state",
    "run_root_replenishment",
    "save_root_replenishment_state",
]
