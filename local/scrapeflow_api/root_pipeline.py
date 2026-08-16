"""P11: the authoritative RootJob pipeline (B/W/C/D -> F/G/H/J -> R).

Root tasks created through the S-step intake path (``create_root_job``, i.e.
tasks bound to an ``IntakeSource``) run this pipeline instead of the legacy
automatic chain.  The legacy runner remains the carrier and read-back for
imported historical records, but never plans or writes new-path roots.

Pipeline contract:

- B/W  : snapshot + boundary split (only when the unit ledger is missing, so
         retries never wipe confirmed identities or acceptance state);
- C/U  : per-unit TMDB identity; uncertain units park the root without
         blocking anything that is already confirmed;
- D    : three-shelf reconciliation per confirmed unit;
- F/G/H: ``execute_new_work_units`` drives the single planner/writer and
         records typed acceptance; J registers precise episode gaps;
- R    : the aggregate decides the durable root phase (``completed`` /
         ``reconciliation_uncertain`` / ``failed``).

Non-``new_work`` outcomes (duplicate_complete / existing_gap / merge_existing)
are parked read-only with a per-unit attention note until the per-unit E lanes
are wired: the pipeline must never guess a formal write for them.
"""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Callable

from engine.scrapeflow.intake_source import load_intake_catalog
from engine.scrapeflow.root_boundaries import analyze_root_boundaries
from engine.scrapeflow.serialization import atomic_write_json
from engine.scrapeflow.unit_identity import resolve_work_unit_identities
from engine.scrapeflow.work_units import (
    WorkUnitRecord,
    load_work_unit_records,
    save_work_unit_records,
)

from .library_index import reconcile_root_work_units
from .redaction import redact_value
from .root_aggregation import aggregate_root_job
from .simple_engine_runner import EngineJob, SimpleEngineRunner
from .unit_execution import execute_new_work_units

# Phases the pipeline may re-enter on dispatch.  ``retry_wait`` appears
# because the scheduler's bounded retry boundary is reused for transient
# B/W/C/D failures (AList/TMDB availability); the pipeline itself is
# idempotent, so re-entry is safe.
RUNNABLE_PHASES = frozenset({"queued", "reconciliation_uncertain", "retry_wait"})

PARK_PHASE = "reconciliation_uncertain"

_E_LANE_PARK_NOTE = "对账结果 {outcome}：单元级 E 通道尚未接入，来源保持原样（只读）"


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


def _park_non_new_work_units(
    state_root: Path,
    root_task_id: str,
    records: list[WorkUnitRecord],
) -> list[WorkUnitRecord]:
    """Attach a bounded read-only note to units waiting for the E lanes."""
    changed = False
    updated: list[WorkUnitRecord] = []
    for record in records:
        if record.reconciliation_outcome != "new_work" and record.attention is None:
            updated.append(replace(
                record,
                attention=_E_LANE_PARK_NOTE.format(
                    outcome=record.reconciliation_outcome
                ),
                updated_at=_now(),
            ))
            changed = True
        else:
            updated.append(record)
    if changed:
        save_work_unit_records(state_root, root_task_id, updated)
    return updated


def run_root_pipeline(
    runner: SimpleEngineRunner,
    state_root: Path,
    root_task_id: str,
    *,
    pause_requested: Callable[[], bool] | None = None,
) -> EngineJob:
    """Run one idempotent B/W/C/D -> F/G/H/J -> R pass for a root task.

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

    # D: three-shelf reconciliation per confirmed unit.
    reconcile_root_work_units(
        runner.alist, runner.library_root, state_root, root_task_id,
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

    # Pause/cancel boundary: everything below may write the formal library.
    if callable(pause_requested) and pause_requested():
        return job
    cancelled = runner._consume_cancel_request(job)  # noqa: SLF001
    if cancelled is not None:
        return cancelled

    # E-lane parking: never guess a formal write for non-new_work outcomes.
    pending_e_lane = [
        record
        for record in records
        if record.reconciliation_outcome != "new_work"
    ]
    if pending_e_lane:
        _park_non_new_work_units(state_root, root_task_id, records)
        return _persist_root(
            runner, job, PARK_PHASE,
            error="存在重复/缺口/归并单元，单元级 E 通道接入前保持只读",
        )

    # F/G/H/J: single planner, single writer, typed acceptance, gap ledger.
    results = execute_new_work_units(runner, state_root, root_task_id)
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
    return _persist_root(runner, job, "completed")


__all__ = [
    "PARK_PHASE",
    "RUNNABLE_PHASES",
    "is_intake_bound_root",
    "run_root_pipeline",
]
