"""F/G/H composition: plan and execute confirmed new_work units.

Each ``new_work`` WorkUnit drives the existing planner through one internal
``EngineJob`` carrier and is written by the single ``SimplePlanExecutor`` with
its exact readback (the legacy G/H machinery is reused unchanged).  The result
is recorded as a typed ``WorkAcceptanceResult`` persisted per root task.

Units whose reconciliation is not ``new_work`` are reported as skipped and are
never written here: merge/duplicate/gap handling belongs to the D/E/J lanes,
not to the new-work writer.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import UTC, datetime
import json
from pathlib import Path
from typing import Any, Mapping, Sequence

from engine.scrapeflow.serialization import atomic_write_json
from engine.scrapeflow.target_shelf import target_root_for_shelf
from engine.scrapeflow.work_units import (
    WorkUnitRecord,
    load_work_unit_records,
    save_work_unit_records,
)

from .redaction import redact_error
from .simple_engine_runner import EngineRequest, SimpleEngineRunner


def _now() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _acceptance_path(state_root: Path, root_task_id: str) -> Path:
    return state_root / f"work_acceptance_{root_task_id}.json"


def _unit_job_id(work_unit_id: str) -> str:
    return f"unit-{work_unit_id}"


@dataclass(frozen=True)
class WorkAcceptanceResult:
    """Typed post-write verification for one work unit (H step)."""

    work_unit_id: str
    outcome: str  # accepted | failed | skipped
    writer_job_id: str | None
    phase: str
    target_root: str
    planned_files: int
    error: str | None
    recorded_at: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "work_unit_id": self.work_unit_id,
            "outcome": self.outcome,
            "writer_job_id": self.writer_job_id,
            "phase": self.phase,
            "target_root": self.target_root,
            "planned_files": self.planned_files,
            "error": self.error,
            "recorded_at": self.recorded_at,
        }

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> "WorkAcceptanceResult":
        return cls(
            work_unit_id=str(raw["work_unit_id"]),
            outcome=str(raw.get("outcome", "failed")),
            writer_job_id=(
                str(raw["writer_job_id"]) if raw.get("writer_job_id") else None
            ),
            phase=str(raw.get("phase", "")),
            target_root=str(raw.get("target_root", "")),
            planned_files=int(raw.get("planned_files", 0)),
            error=str(raw["error"]) if raw.get("error") else None,
            recorded_at=str(raw.get("recorded_at") or _now()),
        )


def save_work_acceptance(
    state_root: Path,
    root_task_id: str,
    results: Sequence[WorkAcceptanceResult],
) -> None:
    atomic_write_json(
        _acceptance_path(state_root, root_task_id),
        [result.as_dict() for result in results],
        allow_nan=False,
    )


def load_work_acceptance(
    state_root: Path,
    root_task_id: str,
) -> list[WorkAcceptanceResult]:
    try:
        raw = json.loads(
            _acceptance_path(state_root, root_task_id).read_text(encoding="utf-8")
        )
    except (FileNotFoundError, OSError, UnicodeDecodeError, json.JSONDecodeError):
        return []
    if not isinstance(raw, list):
        return []
    output: list[WorkAcceptanceResult] = []
    for item in raw:
        if isinstance(item, Mapping):
            try:
                output.append(WorkAcceptanceResult.from_dict(item))
            except (KeyError, TypeError, ValueError):
                continue
    return output


def _request_for_unit(
    runner: SimpleEngineRunner,
    record: WorkUnitRecord,
    root_task_id: str,
) -> EngineRequest:
    root_job = runner._read(root_task_id)  # noqa: SLF001 - ledger composition
    if root_job.target_shelf is None:
        raise ValueError("根任务尚未选择目标货架，无法规划作品单元")
    shelf_root = target_root_for_shelf(runner.library_root, root_job.target_shelf)
    identity = record.identity or {}
    media_type = str(identity.get("media_type") or "tv")
    tmdb_id = identity.get("tmdb_id")
    if not isinstance(tmdb_id, int) or isinstance(tmdb_id, bool) or tmdb_id <= 0:
        raise ValueError("单元身份缺少有效 tmdb_id")
    payload: dict[str, object] = {
        "source_path": record.source_paths[0],
        "parent_path": shelf_root,
        "media_type": media_type,
        "tmdb_id": tmdb_id,
    }
    season = identity.get("season")
    if isinstance(season, int) and not isinstance(season, bool) and season > 0:
        payload["season"] = season
    return EngineRequest.from_mapping(payload)


def execute_new_work_units(
    runner: SimpleEngineRunner,
    state_root: Path,
    root_task_id: str,
) -> list[WorkAcceptanceResult]:
    """Plan + write every confirmed ``new_work`` unit of one root task.

    - F: each unit builds one EngineRequest and goes through the existing
      planner (the internal EngineJob carrier);
    - G: the single writer executes the plan under its worker lock;
    - H: the executed readback is wrapped into a WorkAcceptanceResult.
    Already-executed units are skipped on retry (``writer_job_id`` persists),
    and a failed unit keeps its ``writer_job_id`` unset so the next run
    retries it without creating a second carrier.
    """
    records = load_work_unit_records(state_root, root_task_id)
    results: list[WorkAcceptanceResult] = []
    updated: list[WorkUnitRecord] = []
    changed = False
    for record in records:
        if record.reconciliation_outcome != "new_work":
            results.append(WorkAcceptanceResult(
                work_unit_id=record.work_unit_id,
                outcome="skipped",
                writer_job_id=None,
                phase=record.identity_status,
                target_root="",
                planned_files=0,
                error=None,
                recorded_at=_now(),
            ))
            updated.append(record)
            continue
        if record.writer_job_id is not None:
            # Already planned and executed; re-verify the carrier state.
            carrier = runner.get_job(record.writer_job_id)
            plan_files = len(carrier.plan.get("files") or [])
            results.append(WorkAcceptanceResult(
                work_unit_id=record.work_unit_id,
                outcome="accepted" if carrier.phase == "executed" else "failed",
                writer_job_id=carrier.id,
                phase=carrier.phase,
                target_root=str((carrier.plan.get("target_root")) or ""),
                planned_files=plan_files,
                error=carrier.error,
                recorded_at=_now(),
            ))
            updated.append(record)
            continue
        try:
            request = _request_for_unit(runner, record, root_task_id)
            planned = runner.plan_job(
                request,
                job_id=_unit_job_id(record.work_unit_id),
            )
            record = replace(record, writer_job_id=planned.id)
            changed = True
            executed = runner.execute_job(planned.id)
            plan_files = len(executed.plan.get("files") or [])
            results.append(WorkAcceptanceResult(
                work_unit_id=record.work_unit_id,
                outcome="accepted",
                writer_job_id=executed.id,
                phase=executed.phase,
                target_root=str((executed.plan.get("target_root")) or ""),
                planned_files=plan_files,
                error=None,
                recorded_at=_now(),
            ))
        except Exception as exc:
            # Keep writer_job_id unset so the next run retries the unit.
            results.append(WorkAcceptanceResult(
                work_unit_id=record.work_unit_id,
                outcome="failed",
                writer_job_id=None,
                phase="failed",
                target_root="",
                planned_files=0,
                error=redact_error(exc),
                recorded_at=_now(),
            ))
        updated.append(record)
    if changed:
        save_work_unit_records(state_root, root_task_id, updated)
    save_work_acceptance(state_root, root_task_id, results)
    return results


__all__ = [
    "WorkAcceptanceResult",
    "execute_new_work_units",
    "load_work_acceptance",
    "save_work_acceptance",
]
