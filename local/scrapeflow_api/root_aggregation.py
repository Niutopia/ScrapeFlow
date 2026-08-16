"""R-node composition: root-task aggregation over the WorkUnit ledger.

Aggregates one root task's units, acceptance results and gap ledger into the
user-visible state the contract requires: completed / in_progress / attention
/ failed counts plus open/closed gap counts.  No EngineJob.summary fields are
read or written here — the ledger files are the only truth source.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

from engine.scrapeflow.gap_ledger import load_gap_ledger
from engine.scrapeflow.work_units import WorkUnitRecord, load_work_unit_records

from .unit_execution import load_work_acceptance


@dataclass(frozen=True)
class RootJobAggregate:
    root_task_id: str
    unit_count: int
    completed: int
    in_progress: int
    attention: int
    failed: int
    open_gaps: int
    closed_gaps: int

    def as_dict(self) -> dict[str, object]:
        return {
            "root_task_id": self.root_task_id,
            "unit_count": self.unit_count,
            "completed": self.completed,
            "in_progress": self.in_progress,
            "attention": self.attention,
            "failed": self.failed,
            "open_gaps": self.open_gaps,
            "closed_gaps": self.closed_gaps,
        }


def aggregate_root_job(state_root: Path, root_task_id: str) -> RootJobAggregate:
    """Compute the R-node aggregate for one root task."""
    records = load_work_unit_records(state_root, root_task_id)
    acceptance = {
        result.work_unit_id: result
        for result in load_work_acceptance(state_root, root_task_id)
    }
    gaps = load_gap_ledger(state_root, root_task_id)
    completed = 0
    attention = 0
    failed = 0
    for record in records:
        result = acceptance.get(record.work_unit_id)
        if result is not None and result.outcome == "failed":
            failed += 1
            continue
        if record.identity_status in {"uncertain", "failed"}:
            attention += 1
            continue
        if record.reconciliation_outcome == "uncertain":
            attention += 1
            continue
        if result is not None and result.outcome == "accepted":
            completed += 1
            continue
        if record.reconciliation_outcome in {"duplicate_complete", "existing_gap"}:
            completed += 1
            continue
    in_progress = len(records) - completed - attention - failed
    return RootJobAggregate(
        root_task_id=root_task_id,
        unit_count=len(records),
        completed=max(completed, 0),
        in_progress=max(in_progress, 0),
        attention=attention,
        failed=failed,
        open_gaps=sum(1 for gap in gaps if gap.status == "open"),
        closed_gaps=sum(1 for gap in gaps if gap.status == "closed"),
    )


def public_work_unit_row(
    record: WorkUnitRecord,
    state_root: Path,
    root_task_id: str,
) -> dict[str, object]:
    """Bounded, user-visible projection of one work unit.

    Never exposes internal JSON, arbitrary paths beyond the source boundary,
    or anything the user is not meant to confirm.  The only confirmation
    surface is the bounded candidate list plus media_type/tmdb_id.
    """
    acceptance = {
        result.work_unit_id: result
        for result in load_work_acceptance(state_root, root_task_id)
    }
    result = acceptance.get(record.work_unit_id)
    identity = record.identity or {}
    return {
        "work_unit_id": record.work_unit_id,
        "boundary_key": record.boundary_key,
        "display_label": (
            str(record.boundary_key).rstrip("/").rsplit("/", 1)[-1]
            or record.boundary_key
        ),
        "role": record.role,
        "media_context": record.media_context,
        "identity_status": record.identity_status,
        "identity": (
            {
                "media_type": identity.get("media_type"),
                "tmdb_id": identity.get("tmdb_id"),
                "title": identity.get("title"),
                "year": identity.get("year"),
                "confidence": identity.get("confidence"),
                "source": identity.get("source"),
            }
            if identity else None
        ),
        "candidate_identities": list(record.candidate_identities)[:5],
        "reconciliation_outcome": record.reconciliation_outcome,
        "matched_work_root": record.matched_work_root,
        "acceptance": (
            {
                "outcome": result.outcome,
                "phase": result.phase,
                "target_root": result.target_root,
                "planned_files": result.planned_files,
                "error": result.error,
            }
            if result is not None else None
        ),
        "attention": record.attention,
    }


def public_work_unit_rows(
    state_root: Path,
    root_task_id: str,
) -> list[dict[str, object]]:
    records = load_work_unit_records(state_root, root_task_id)
    return [
        public_work_unit_row(record, state_root, root_task_id)
        for record in records
    ]


__all__ = [
    "RootJobAggregate",
    "aggregate_root_job",
    "public_work_unit_row",
    "public_work_unit_rows",
]
