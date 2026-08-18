"""P11+P12: the authoritative RootJob pipeline (B/W/C/D -> E/F/G/H/J -> R).

Root tasks created through the S-step intake path (``create_root_job``, i.e.
tasks bound to an ``IntakeSource``) run this pipeline instead of the legacy
automatic chain.  The legacy runner remains the carrier and read-back for
imported historical records, but never plans or writes new-path roots.

Pipeline contract:

- B/W  : snapshot + boundary split (only when the unit ledger is missing, so
         retries never wipe confirmed identities or acceptance state);
- C/U  : per-unit TMDB identity; uncertain units park the root without
         blocking anything that is already confirmed;
- D    : three-shelf reconciliation per confirmed unit (with the aggregated
         known-gap coordinates from every gap ledger);
- E    : per-unit lanes for duplicate_complete / existing_gap / merge_existing
         (``unit_e_lanes``);
- F/G/H: ``execute_new_work_units`` drives the single planner/writer and
         records typed acceptance; J registers precise episode gaps;
- R    : the aggregate decides the durable root phase (``completed`` /
         ``reconciliation_uncertain`` / ``failed``).
"""

from __future__ import annotations

import posixpath
from collections.abc import Mapping
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Callable

from engine.scrapeflow.intake_source import load_intake_catalog
from engine.scrapeflow.root_boundaries import analyze_root_boundaries
from engine.scrapeflow.serialization import atomic_write_json
from engine.scrapeflow.unit_identity import resolve_work_unit_identities
from engine.scrapeflow.work_units import load_work_unit_records

from .library_index import reconcile_root_work_units
from .redaction import redact_error, redact_value
from .root_aggregation import aggregate_root_job
from .simple_engine_runner import EngineJob, EnginePauseRequested, SimpleEngineRunner
from .unit_e_lanes import compute_known_gap_tokens, execute_unit_e_lanes
from .unit_execution import execute_new_work_units

# Phases the pipeline may re-enter on dispatch.  ``retry_wait`` appears
# because the scheduler's bounded retry boundary is reused for transient
# B/W/C/D failures (AList/TMDB availability); the pipeline itself is
# idempotent, so re-entry is safe.
RUNNABLE_PHASES = frozenset({"queued", "reconciliation_uncertain", "retry_wait"})

PARK_PHASE = "reconciliation_uncertain"


def _now() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def is_intake_bound_root(state_root: Path, root_task_id: str) -> bool:
    """Whether an IntakeSource catalog entry binds this job as its RootJob.

    The catalog binding is the durable S-step fact (``root_task_id``), so no
    new EngineJob.summary marker is needed to tell new-path roots from
    imported legacy records.  Fully defensive: any unreadable/malformed
    catalog means "not bound" so the legacy lane stays fail-closed.
    """
    try:
        catalog = load_intake_catalog(state_root)
        return any(
            source.root_task_id == root_task_id
            for source in catalog
            if getattr(source, "root_task_id", None) is not None
        )
    except Exception:
        return False


def _persist_root(
    runner: SimpleEngineRunner,
    job: EngineJob,
    phase: str,
    *,
    error: str | None = None,
) -> EngineJob:
    """Persist one bounded root-phase transition without touching the summary."""
    updated = replace(job, phase=phase, error=error, updated_at=_now())
    atomic_write_json(
        runner._job_path(job.id),  # noqa: SLF001 - pipeline composition
        redact_value(updated.as_dict()),
        allow_nan=False,
    )
    return updated


def _refresh_lane_acceptance(
    state_root: Path,
    root_task_id: str,
    records: list,
) -> None:
    """Rewrite acceptance rows for completed E-lane units.

    E lanes record progress on the WorkUnit ledger, not the acceptance file,
    so a stale failed acceptance from an earlier run must be replaced once
    the lane finishes; otherwise R would keep counting the old failure.
    """
    from .unit_execution import (
        WorkAcceptanceResult,
        load_work_acceptance,
        save_work_acceptance,
    )
    fresh = {
        row.work_unit_id: row
        for row in load_work_acceptance(state_root, root_task_id)
    }
    for record in records:
        outcome = record.reconciliation_outcome
        if (
            outcome in {"duplicate_complete", "existing_gap", "merge_existing"}
            and record.lane_status
        ):
            fresh[record.work_unit_id] = WorkAcceptanceResult(
                work_unit_id=record.work_unit_id,
                outcome="accepted",
                writer_job_id=record.writer_job_id,
                phase="executed" if outcome == "merge_existing" else "completed",
                target_root=record.matched_work_root or "",
                planned_files=0,
                error=None,
                recorded_at=_now(),
            )
    save_work_acceptance(
        state_root, root_task_id, list(fresh.values()),
    )


def _cleanup_empty_source_shells(
    runner: SimpleEngineRunner,
    state_root: Path,
    root_task_id: str,
    source: str,
    *,
    pause_requested: Callable[[], bool] | None = None,
) -> list[str]:
    """Remove only verifiably empty dirs in a completed root's own intake tree.

    Ownership is proven by the durable catalog binding (S step), never by path
    naming: the catalog entry must bind this exact root task to this exact
    source path.  Only directories that list as empty are removed; every
    removal is confirmed through a fresh parent listing, and files or
    non-empty directories are never touched.  The source root itself is
    removed when it ends up empty, which lets the intake monitor mark the
    catalog entry missing on its next scan.
    """
    try:
        bound = any(
            entry.root_task_id == root_task_id and entry.canonical_path == source
            for entry in load_intake_catalog(state_root)
        )
    except Exception:
        bound = False
    if not bound:
        return []
    if not runner.source_directory_exists(source):
        # Already consumed (e.g. archived by an E1 lane): nothing to clean.
        return []
    listing = getattr(runner.alist, "list", None)
    remove_empty = getattr(runner.alist, "remove_empty_dir", None)
    if not callable(listing) or not callable(remove_empty):
        return []

    def rows(path: str) -> list[Mapping[str, object]]:
        try:
            raw = listing(path, refresh=True)
        except TypeError:
            raw = listing(path)
        if not isinstance(raw, list) or any(
            not isinstance(item, Mapping) for item in raw
        ):
            raise RuntimeError(f"AList 源目录回读格式无效: {path}")
        return list(raw)

    removed: list[str] = []

    def paused() -> bool:
        """Fail closed if the composition-root pause state is unavailable."""
        if not callable(pause_requested):
            return False
        try:
            return bool(pause_requested())
        except Exception:
            return True

    def visit(directory: str) -> None:
        if paused():
            return
        for item in rows(directory):
            name = item.get("name")
            if (
                not isinstance(name, str)
                or not name
                or name in {".", ".."}
                or "/" in name
                or "\\" in name
            ):
                raise RuntimeError(f"AList 源目录出现不安全条目: {directory}")
            if item.get("is_dir") is True:
                visit(posixpath.join(directory, name))
                if paused():
                    return
        if paused():
            return
        if rows(directory):
            return
        # This is the exact remote-delete boundary.  A completion check at
        # the caller is insufficient because a root-scoped pilot can close
        # while the recursive fresh listing is still in progress.
        if paused():
            return
        deleted = remove_empty(directory)
        parent = posixpath.dirname(directory) or "/"
        name = posixpath.basename(directory)
        try:
            parent_rows = rows(parent)
        except Exception:
            parent_rows = []
        if not any(item.get("name") == name for item in parent_rows):
            removed.append(directory)
            return
        if deleted is not False:
            # Some drivers (Quark via AList) accept remove_empty_directory
            # with HTTP success but never delete.  The directory was just
            # verified empty through a fresh listing, so an explicit remove
            # of that single name is the bounded fallback.
            try:
                remove = getattr(runner.alist, "remove", None)
                if callable(remove):
                    # The fallback is a separate remote delete and needs its
                    # own checkpoint even though remove_empty just ran.
                    if paused():
                        return
                    remove(parent, [name])
                    parent_rows = rows(parent)
                    if not any(item.get("name") == name for item in parent_rows):
                        removed.append(directory)
            except Exception:
                pass

    visit(source)
    return removed


def run_root_pipeline(
    runner: SimpleEngineRunner,
    state_root: Path,
    root_task_id: str,
    *,
    pause_requested: Callable[[], bool] | None = None,
) -> EngineJob:
    """Run one idempotent B/W/C/D -> E/F/G/H/J -> R pass for a root task.

    Raises on transient AList/TMDB failures so the caller's bounded retry
    boundary applies; business-level uncertainty is parked, never raised.
    """
    job = runner.get_job(root_task_id)
    if job.phase not in RUNNABLE_PHASES:
        return job
    source = runner._job_ingress_source(job)  # noqa: SLF001 - pipeline composition

    # B/W: rebuild the ledger only when it does not exist yet.
    if not load_work_unit_records(state_root, root_task_id):
        analyze_root_boundaries(
            runner.alist, source, root_task_id=root_task_id, state_root=state_root,
        )
    records = load_work_unit_records(state_root, root_task_id)
    if not records:
        # An empty source has no work units; the root is complete with
        # nothing to write.
        return _persist_root(runner, job, "completed")

    # C/U: confirmed records (including durable operator overrides) are
    # untouched; only pending units are resolved.
    resolve_work_unit_identities(
        runner.tmdb,
        state_root,
        root_task_id,
        prefer_animation=(job.target_shelf == "anime"),
    )
    records = load_work_unit_records(state_root, root_task_id)
    if any(record.identity_status in {"pending", "uncertain", "failed"} for record in records):
        return _persist_root(
            runner, job, PARK_PHASE,
            error="部分作品单元身份待确认",
        )

    # D: three-shelf reconciliation per confirmed unit, fed by the aggregated
    # known-gap coordinates so ``existing_gap`` can be proven.
    known_gap_tokens = compute_known_gap_tokens(state_root)
    reconcile_root_work_units(
        runner.alist,
        runner.library_root,
        state_root,
        root_task_id,
        known_gap_tokens_by_identity=known_gap_tokens,
    )
    records = load_work_unit_records(state_root, root_task_id)
    if any(
        record.reconciliation_outcome in {None, "uncertain"}
        for record in records
    ):
        return _persist_root(
            runner, job, PARK_PHASE,
            error="部分作品单元对账结果不确定，等待人工确认",
        )

    # Pause/cancel boundary: everything below may move media or write the
    # formal library.
    if callable(pause_requested) and pause_requested():
        return job
    cancelled = runner._consume_cancel_request(job)  # noqa: SLF001
    if cancelled is not None:
        return cancelled

    lane_records = [
        record
        for record in records
        if record.reconciliation_outcome != "new_work"
    ]
    if lane_records:
        # E1/E2/E3 per unit: archive consumption, gap registration/hold,
        # merge into the locked existing work root.
        try:
            execute_unit_e_lanes(
                runner, state_root, root_task_id, pause_requested=pause_requested,
            )
        except EnginePauseRequested:
            # A paused E lane deliberately leaves its durable ledger/carrier
            # for fresh-state recovery.  It is not a business failure.
            return job
        except Exception as exc:
            return _persist_root(
                runner, job, "failed",
                error=f"单元 E 通道执行失败: {redact_error(exc)}",
            )
        _refresh_lane_acceptance(
            state_root,
            root_task_id,
            load_work_unit_records(state_root, root_task_id),
        )

    new_work_records = [
        record
        for record in records
        if record.reconciliation_outcome == "new_work"
    ]
    if new_work_records:
        # F/G/H/J: single planner, single writer, typed acceptance, gap ledger.
        try:
            results = execute_new_work_units(
                runner,
                state_root,
                root_task_id,
                pause_requested=pause_requested,
            )
        except EnginePauseRequested:
            return job
        failed = [result for result in results if result.outcome == "failed"]
        if failed:
            return _persist_root(
                runner, job, "failed",
                error=f"{len(failed)} 个作品单元执行失败，等待重试",
            )

    # R: aggregate the ledger into the durable root phase.
    aggregate = aggregate_root_job(state_root, root_task_id)
    if aggregate.attention:
        return _persist_root(runner, job, PARK_PHASE)
    if aggregate.failed:
        return _persist_root(runner, job, "failed", error="存在失败的作品单元")
    if aggregate.in_progress:
        return _persist_root(
            runner, job, PARK_PHASE,
            error="部分作品单元尚未完成，等待继续处理",
        )
    # Post-completion housekeeping: drop the empty source-dir shells the
    # writer leaves behind in this root's own intake tree.  Strictly
    # emptiness-gated and best-effort — a failure never rolls back the
    # completion, and a paused run skips remote deletes entirely.
    if not (callable(pause_requested) and pause_requested()):
        try:
            _cleanup_empty_source_shells(
                runner,
                state_root,
                root_task_id,
                source,
                pause_requested=pause_requested,
            )
        except Exception:
            pass
    return _persist_root(runner, job, "completed")


__all__ = [
    "PARK_PHASE",
    "RUNNABLE_PHASES",
    "is_intake_bound_root",
    "run_root_pipeline",
]
