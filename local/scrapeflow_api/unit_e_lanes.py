"""P12: per-unit E lanes — E1 duplicate consumption, E2 existing-gap
registration/hold, E3 merge into an existing work root.

Every remote mutation follows the single-writer invariants: task-owned
ingress objects only, no target overwrite, exact parent-listing readback
after every move, and pause/cancel checkpoints before each external side
effect.  Lane progress is durably recorded on the WorkUnit ledger
(``lane_status``), never on EngineJob.summary.
"""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime
import json
from pathlib import Path
import posixpath
import time
from typing import Callable, Mapping

from engine.scrapeflow.gap_ledger import (
    Gap,
    gap_token,
    load_gap_ledger,
    parse_gap_token,
    save_gap_ledger,
)
from engine.scrapeflow.root_boundaries import load_source_snapshot
from engine.scrapeflow.work_units import (
    WorkUnitRecord,
    load_work_unit_records,
    save_work_unit_records,
)

from .simple_engine_runner import (
    EngineExecutionError,
    SimpleEngineRunner,
    _pause_checkpoint,   # noqa: PLC2701 - shared runner primitive
    _safe_job_id,        # noqa: PLC2701
    _safe_remote_path,   # noqa: PLC2701
)
from .unit_execution import (
    _mark_internal_carrier,
    _retire_stale_unit_carrier,
    _unit_job_id,
)


def _now() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def compute_known_gap_tokens(
    state_root: Path,
) -> dict[tuple[str, int], set[str]]:
    """Aggregate open gap coordinates from every ledger, keyed by identity."""
    known: dict[tuple[str, int], set[str]] = {}
    for path in sorted(state_root.glob("gap_ledger_*.json")):
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError):
            continue
        if not isinstance(raw, list):
            continue
        for item in raw:
            if not isinstance(item, Mapping) or item.get("status") != "open":
                continue
            media_type = str(item.get("media_type") or "").strip()
            tmdb_id = item.get("tmdb_id")
            season = item.get("season")
            episodes = item.get("episodes")
            if (
                media_type not in {"movie", "tv"}
                or isinstance(tmdb_id, bool)
                or not isinstance(tmdb_id, int)
                or tmdb_id <= 0
                or isinstance(season, bool)
                or not isinstance(season, int)
                or not isinstance(episodes, list)
            ):
                continue
            for episode in episodes:
                if isinstance(episode, bool) or not isinstance(episode, int) or episode <= 0:
                    continue
                known.setdefault((media_type, tmdb_id), set()).add(
                    gap_token(season, episode)
                )
    return known


def _owned_unit_source(
    runner: SimpleEngineRunner,
    record: WorkUnitRecord,
    root_job,
) -> str:
    """Return the validated task-owned boundary source of one unit."""
    unit_source = (
        str(record.source_paths[0]).rstrip("/")
        if record.source_paths
        else ""
    )
    if not unit_source:
        raise EngineExecutionError("单元缺少来源路径")
    ingress = str(runner._job_ingress_source(root_job)).rstrip("/")  # noqa: SLF001
    if not ingress or not (
        unit_source == ingress or unit_source.startswith(ingress + "/")
    ):
        raise EngineExecutionError("单元来源不属于本任务入站目录")
    return unit_source


def _unit_subtree_is_empty(
    state_root: Path,
    root_task_id: str,
    record: WorkUnitRecord,
) -> bool:
    """Whether the persisted B snapshot holds no file under the unit boundary."""
    snapshot = load_source_snapshot(state_root, root_task_id)
    if snapshot is None:
        return False
    boundary = str(record.source_paths[0]).rstrip("/")
    for row in snapshot["rows"]:
        full_path = str(row.get("full_path") or "")
        if full_path == boundary or full_path.startswith(boundary + "/"):
            if row.get("is_dir") is not True:
                return False
    return True


def _stable_kind(
    runner: SimpleEngineRunner,
    path: str,
    *,
    attempts: int = 4,
    delay_seconds: float = 3.0,
) -> str:
    """Probe the remote kind with bounded retries against eventual consistency.

    The Quark mount's listing cache can briefly return the pre-move snapshot
    even with ``refresh=True``, so verification probes retry for a few
    seconds before giving up.
    """
    last = runner._remote_entry_kind(path)
    for _ in range(max(1, attempts) - 1):
        if last != "unknown":
            return last
        time.sleep(delay_seconds)
        last = runner._remote_entry_kind(path)
    return last


def _move_with_readback(
    runner: SimpleEngineRunner,
    *,
    source: str,
    target_root: str,
    name: str,
    field: str,
    pause_requested: Callable[[], bool] | None,
    root_job,
) -> None:
    """Move one task-owned directory into a bounded lane with exact readback."""
    target = f"{target_root}/{name}"
    ensure = getattr(runner.alist, "ensure_directory", None) or getattr(
        runner.alist, "mkdir", None
    )
    move = getattr(runner.alist, "move", None)
    if not callable(ensure) or not callable(move):
        raise EngineExecutionError(f"AList 客户端缺少 {field} 移动接口")
    source_kind = runner._remote_entry_kind(source)
    if source_kind == "unknown":
        raise EngineExecutionError(f"{field} source 回读不可确认")
    target_kind = runner._remote_entry_kind(target)
    if target_kind == "unknown":
        # On the real AList client, listing a not-yet-created lane root can
        # surface as an error instead of an empty listing.  Create the lane
        # root first, then re-probe the exact target.
        root_kind = runner._remote_entry_kind(target_root)
        if root_kind in {"file", "ambiguous", "unknown"}:
            _pause_checkpoint(pause_requested)
            ensure(target_root)
            if runner._remote_entry_kind(target_root) != "directory":
                raise EngineExecutionError(f"{field} 归档根创建后回读失败")
        target_kind = runner._remote_entry_kind(target)
        if target_kind == "unknown":
            raise EngineExecutionError(f"{field} 目标回读不可确认")
    if source_kind == "missing" and target_kind == "directory":
        return  # The move already committed; idempotent success.
    if source_kind == "missing":
        raise EngineExecutionError(f"{field} source/目标均无法回读")
    if source_kind != "directory":
        raise EngineExecutionError(f"{field} 来源不是唯一目录")
    if target_kind != "missing":
        raise EngineExecutionError(f"{field} 目标已被占用")
    root_kind = runner._remote_entry_kind(target_root)
    if root_kind in {"file", "ambiguous", "unknown"}:
        raise EngineExecutionError(f"{field} 归档根不是可用目录")
    _pause_checkpoint(pause_requested)
    if runner._consume_cancel_request(runner._read(root_job.id)) is not None:  # noqa: SLF001
        raise EngineExecutionError(f"{field} 已取消")
    ensure(target_root)
    if runner._remote_entry_kind(target_root) != "directory":
        raise EngineExecutionError(f"{field} 归档根创建后回读失败")
    if runner._remote_entry_kind(target) != "missing":
        raise EngineExecutionError(f"{field} 目标已被占用")
    _pause_checkpoint(pause_requested)
    parent = posixpath.dirname(source)
    move(parent, target_root, [name])
    verified = False
    for _ in range(4):
        time.sleep(3.0)
        if (
            _stable_kind(runner, source) == "missing"
            and _stable_kind(runner, target) == "directory"
        ):
            verified = True
            break
    if not verified:
        raise EngineExecutionError(f"{field} 移动后回读失败")


def _consume_duplicate_unit(
    runner: SimpleEngineRunner,
    record: WorkUnitRecord,
    root_job,
    *,
    pause_requested: Callable[[], bool] | None,
) -> WorkUnitRecord:
    """E1: move a proven duplicate boundary into the task archive lane."""
    unit_source = _owned_unit_source(runner, record, root_job)
    name = posixpath.basename(unit_source)
    if not name or name in {".", ".."}:
        raise EngineExecutionError("duplicate 单元边界名无效")
    processed_root = _safe_remote_path(
        f"{runner.library_root}/ScrapeFlow/归档/{_safe_job_id(root_job.id)}/processed",
        field="unit duplicate processed root",
        allow_root=False,
    )
    target = f"{processed_root}/{name}"
    source_kind = runner._remote_entry_kind(unit_source)
    target_kind = runner._remote_entry_kind(target)
    if record.lane_status == "duplicate_consumed":
        if source_kind == "missing" and target_kind == "directory":
            return record
        raise EngineExecutionError("duplicate 单元消费后回读失败")
    _move_with_readback(
        runner,
        source=unit_source,
        target_root=processed_root,
        name=name,
        field="duplicate 单元消费",
        pause_requested=pause_requested,
        root_job=root_job,
    )
    return replace(
        record,
        lane_status="duplicate_consumed",
        lane_detail=None,
        attention=None,
        updated_at=_now(),
    )


def _register_and_hold_existing_gap_unit(
    runner: SimpleEngineRunner,
    state_root: Path,
    record: WorkUnitRecord,
    root_job,
    *,
    pause_requested: Callable[[], bool] | None,
) -> WorkUnitRecord:
    """E2: register the known uncovered gaps; hold an empty boundary dir."""
    identity = record.identity or {}
    media_type = str(identity.get("media_type") or "tv")
    tmdb_id = identity.get("tmdb_id")
    if (
        media_type not in {"movie", "tv"}
        or isinstance(tmdb_id, bool)
        or not isinstance(tmdb_id, int)
        or tmdb_id <= 0
    ):
        raise EngineExecutionError("existing_gap 单元身份无效")
    if record.lane_status in {"existing_gap_registered", "existing_gap_held"}:
        return record
    # Register precise open gaps, idempotent by gap_id.
    ledger = load_gap_ledger(state_root, root_job.id)
    existing_ids = {gap.gap_id for gap in ledger if gap.work_unit_id == record.work_unit_id}
    changed = False
    for token in sorted(set(record.uncovered_tokens)):
        coordinate = parse_gap_token(token)
        if coordinate is None:
            continue
        gap_id = f"{record.work_unit_id}::missing_episode::{token}"
        if gap_id in existing_ids:
            continue
        ledger.append(Gap(
            gap_id=gap_id,
            root_task_id=root_job.id,
            work_unit_id=record.work_unit_id,
            kind="missing_episode",
            media_type=media_type,
            tmdb_id=tmdb_id,
            season=coordinate[0],
            episodes=(coordinate[1],),
            subtitle_path=None,
            subtitle_language=None,
            status="open",
        ))
        changed = True
    if changed:
        save_gap_ledger(state_root, root_job.id, ledger)
    # Hold only a provably empty boundary subtree; non-empty sources stay in
    # intake and become operator attention.
    if _unit_subtree_is_empty(state_root, root_job.id, record):
        unit_source = _owned_unit_source(runner, record, root_job)
        name = posixpath.basename(unit_source)
        hold_root = _safe_remote_path(
            f"{runner.library_root}/ScrapeFlow/归档/{_safe_job_id(root_job.id)}/existing-gap-hold",
            field="unit existing-gap hold root",
            allow_root=False,
        )
        _move_with_readback(
            runner,
            source=unit_source,
            target_root=hold_root,
            name=name,
            field="existing-gap 空目录 hold",
            pause_requested=pause_requested,
            root_job=root_job,
        )
        return replace(
            record,
            lane_status="existing_gap_held",
            lane_detail=None,
            attention=None,
            updated_at=_now(),
        )
    return replace(
        record,
        lane_status="existing_gap_registered",
        lane_detail=None,
        attention="既有作品缺口已登记；来源媒体保留在待刮削，等待人工处理",
        updated_at=_now(),
    )


def _merge_unit(
    runner: SimpleEngineRunner,
    state_root: Path,
    record: WorkUnitRecord,
    root_job,
) -> WorkUnitRecord:
    """E3: plan into the locked existing work root and write with no-overwrite."""
    if record.lane_status == "merge_done" and record.writer_job_id:
        carrier = runner.get_job(record.writer_job_id)
        if carrier.phase == "executed":
            return record
        raise EngineExecutionError("归并载体回读失败")
    work_root = record.matched_work_root
    if not work_root:
        raise EngineExecutionError("merge_existing 单元缺少匹配作品根")
    identity = record.identity or {}
    media_type = str(identity.get("media_type") or "tv")
    tmdb_id = identity.get("tmdb_id")
    if (
        media_type not in {"movie", "tv"}
        or isinstance(tmdb_id, bool)
        or not isinstance(tmdb_id, int)
        or tmdb_id <= 0
    ):
        raise EngineExecutionError("merge_existing 单元身份无效")
    unit_source = _owned_unit_source(runner, record, root_job)
    payload: dict[str, object] = {
        "source_path": unit_source,
        # The planner derives the series directory from the TMDB title under
        # ``parent_path``; passing the work root itself would nest a second
        # copy (番剧/刀剑神域/刀剑神域).  Pass its parent so the planner
        # resolves back onto the locked root, exactly like the legacy merge
        # hand-off, and the target lock below enforces the result.
        "parent_path": posixpath.dirname(work_root.rstrip("/")),
        "media_type": media_type,
        "tmdb_id": tmdb_id,
    }
    season = identity.get("season")
    if isinstance(season, int) and not isinstance(season, bool) and season > 0:
        payload["season"] = season
    from .simple_engine_runner import EngineRequest

    request = EngineRequest.from_mapping(payload)
    # A previous attempt may have left a terminal carrier; plan_job refuses
    # existing ids, so retire it first (same rule as new_work units).
    _retire_stale_unit_carrier(runner, _unit_job_id(record.work_unit_id))
    planned = _mark_internal_carrier(
        runner,
        runner.plan_job(request, job_id=_unit_job_id(record.work_unit_id)),
        root_job.id,
    )
    plan_body = dict(planned.plan or {})
    planned_root = plan_body.get("target_root")
    if not isinstance(planned_root, str) or (
        _safe_remote_path(
            planned_root, field="merge target_root", allow_root=False,
        )
        != work_root
    ):
        raise EngineExecutionError("归并计划未锁定既有作品根")
    metadata = plan_body.get("metadata")
    metadata = metadata if isinstance(metadata, Mapping) else {}
    raw_tmdb = metadata.get("tmdb_id")
    if isinstance(raw_tmdb, str) and raw_tmdb.isdecimal():
        raw_tmdb = int(raw_tmdb)
    if raw_tmdb != tmdb_id:
        raise EngineExecutionError("归并计划身份与既有作品不一致")
    if str(plan_body.get("mode") or "").casefold() != media_type:
        raise EngineExecutionError("归并计划媒体类型与既有作品不一致")
    executed = runner.execute_job(planned.id)
    if executed.phase != "executed":
        raise EngineExecutionError("归并写入未完成")
    return replace(
        record,
        writer_job_id=planned.id,
        lane_status="merge_done",
        lane_detail=None,
        attention=None,
        updated_at=_now(),
    )


def execute_unit_e_lanes(
    runner: SimpleEngineRunner,
    state_root: Path,
    root_task_id: str,
    *,
    pause_requested: Callable[[], bool] | None = None,
) -> list[WorkUnitRecord]:
    """Run the matching E lane for every non-``new_work`` unit, idempotently."""
    root_job = runner.get_job(root_task_id)
    records = load_work_unit_records(state_root, root_task_id)
    updated: list[WorkUnitRecord] = []
    changed = False
    for record in records:
        outcome = record.reconciliation_outcome
        if outcome == "duplicate_complete":
            next_record = _consume_duplicate_unit(
                runner, record, root_job, pause_requested=pause_requested,
            )
        elif outcome == "existing_gap":
            next_record = _register_and_hold_existing_gap_unit(
                runner, state_root, record, root_job,
                pause_requested=pause_requested,
            )
        elif outcome == "merge_existing":
            next_record = _merge_unit(runner, state_root, record, root_job)
        else:
            next_record = record
        changed = changed or next_record.as_dict() != record.as_dict()
        updated.append(next_record)
    if changed:
        save_work_unit_records(state_root, root_task_id, updated)
    return updated


__all__ = [
    "compute_known_gap_tokens",
    "execute_unit_e_lanes",
]
