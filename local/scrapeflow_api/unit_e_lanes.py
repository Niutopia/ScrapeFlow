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
from engine.scrapeflow.disc_expansion_bridge import validate_unit_scopes_for_root
from engine.scrapeflow.root_boundaries import load_source_snapshot
from engine.scrapeflow.source_inventory import validate_source_scope
from engine.scrapeflow.work_units import (
    WorkUnitRecord,
    load_work_unit_records,
    save_work_unit_records,
)

from .simple_engine_runner import (
    EngineExecutionError,
    EnginePauseRequested,
    SimpleEngineRunner,
    _pause_checkpoint,   # noqa: PLC2701 - shared runner primitive
    _safe_job_id,        # noqa: PLC2701
    _safe_remote_path,   # noqa: PLC2701
)
from .unit_execution import (
    _complete_unit_episode_gap_registration,
    _mark_internal_carrier,
    _request_for_unit,
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


def _owned_unit_sources(
    runner: SimpleEngineRunner,
    record: WorkUnitRecord,
    root_job,
) -> tuple[str, ...]:
    """Return every validated, non-overlapping task-owned unit source."""
    ingress = str(runner._job_ingress_source(root_job)).rstrip("/")  # noqa: SLF001
    try:
        return validate_unit_scopes_for_root(
            ingress,
            runner.library_root,
            str(root_job.id),
            record,
        )
    except ValueError as exc:
        raise EngineExecutionError(f"单元来源不属于本任务入站目录: {exc}") from exc


def _unit_subtree_is_empty(
    state_root: Path,
    root_task_id: str,
    record: WorkUnitRecord,
) -> bool:
    """Whether the persisted B snapshot holds no file under the unit boundary."""
    snapshot = load_source_snapshot(state_root, root_task_id)
    if snapshot is None:
        return False
    try:
        scopes = validate_source_scope(snapshot["root"], record.source_paths)
    except ValueError:
        return False
    for row in snapshot["rows"]:
        full_path = str(row.get("full_path") or "")
        if any(full_path == scope or full_path.startswith(scope + "/") for scope in scopes):
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
    def _ensure_lane_root(path: str) -> None:
        """Create the lane directory and prove it through a fresh listing.

        Some AList-backed providers (observed on Quark after a burst of
        writes) acknowledge ``fs/mkdir`` with success while the directory
        never appears — the same phantom-directory failure the plan
        executor's _ensure_dir already handles.  Re-announce with a bounded
        cooldown until the provider's own listing proves the directory.
        """
        for attempt in range(3):
            if runner._remote_entry_kind(path) == "directory":
                return
            _pause_checkpoint(pause_requested)
            ensure(path)
            time.sleep(3.0)
        if runner._remote_entry_kind(path) != "directory":
            raise EngineExecutionError(f"{field} 归档根创建后回读失败")

    target_kind = runner._remote_entry_kind(target)
    if target_kind == "unknown":
        # On the real AList client, listing a not-yet-created lane root can
        # surface as an error instead of an empty listing.  Create the lane
        # root first, then re-probe the exact target.
        root_kind = runner._remote_entry_kind(target_root)
        if root_kind != "directory":
            _ensure_lane_root(target_root)
        target_kind = runner._remote_entry_kind(target)
        if target_kind == "unknown":
            raise EngineExecutionError(f"{field} 目标回读不可确认")
    if source_kind == "missing" and target_kind in {"directory", "file"}:
        return  # The move already committed; idempotent success.
    if source_kind == "missing":
        raise EngineExecutionError(f"{field} source/目标均无法回读")
    # A duplicate unit's exact ownership may be one directory (a whole
    # variant folder) or one flat feature file (a movie-package shard).
    # Both are legal consumed sources; anything else fails closed.
    if source_kind not in {"directory", "file"}:
        raise EngineExecutionError(f"{field} 来源不是唯一目录或文件")
    if target_kind != "missing":
        raise EngineExecutionError(f"{field} 目标已被占用")
    root_kind = runner._remote_entry_kind(target_root)
    if root_kind != "directory":
        # missing/unknown both need creation (with the phantom-mkdir
        # re-announce); file/ambiguous are occupied and fail closed inside.
        _ensure_lane_root(target_root)
    if runner._remote_entry_kind(target_root) != "directory":
        raise EngineExecutionError(f"{field} 归档根不是可用目录")
    _pause_checkpoint(pause_requested)
    if runner._consume_cancel_request(runner._read(root_job.id)) is not None:  # noqa: SLF001 - lane composition
        raise EngineExecutionError(f"{field} 已取消")
    if runner._remote_entry_kind(target) != "missing":
        raise EngineExecutionError(f"{field} 目标已被占用")
    _pause_checkpoint(pause_requested)
    parent = posixpath.dirname(source)
    move(parent, target_root, [name])
    expected_kind = "directory" if source_kind == "directory" else "file"
    verified = False
    for _ in range(4):
        time.sleep(3.0)
        if (
            _stable_kind(runner, source) == "missing"
            and _stable_kind(runner, target) == expected_kind
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
    unit_sources = _owned_unit_sources(runner, record, root_job)
    names = [posixpath.basename(source) for source in unit_sources]
    if any(not name or name in {".", ".."} for name in names) or len(set(names)) != len(names):
        raise EngineExecutionError("duplicate 单元边界名无效或冲突")
    processed_root = _safe_remote_path(
        f"{runner.library_root}/ScrapeFlow/归档/{_safe_job_id(root_job.id)}/processed",
        field="unit duplicate processed root",
        allow_root=False,
    )
    lane_root = (
        f"{processed_root}/{record.work_unit_id}"
        if len(unit_sources) > 1 else processed_root
    )
    if record.lane_status == "duplicate_consumed":
        # The idempotent re-entry readback mirrors the move's own contract:
        # a directory source lands as a directory, a flat feature file
        # (movie-package shard) lands as a file.  Hardcoding "directory"
        # here permanently failed any file-scoped duplicate unit on the
        # root's next retry/resume.
        for source, name in zip(unit_sources, names):
            if (
                runner._remote_entry_kind(source) != "missing"
                or runner._remote_entry_kind(f"{lane_root}/{name}")
                not in {"directory", "file"}
            ):
                raise EngineExecutionError("duplicate 单元消费后回读失败")
        return record
    for source, name in zip(unit_sources, names):
        _move_with_readback(
            runner,
            source=source,
            target_root=lane_root,
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
    # Register precise open gaps, idempotent by gap_id.  A coordinate is a
    # root-level fact: when another unit already holds an open row for the
    # same (media_type, tmdb_id, season, episode) the gap is already tracked
    # and this unit must not append a duplicate — the same dedupe
    # discover_episode_gaps applies.
    ledger = load_gap_ledger(state_root, root_job.id)
    existing_ids = {gap.gap_id for gap in ledger if gap.work_unit_id == record.work_unit_id}
    open_coordinates = {
        (gap.media_type, gap.tmdb_id, gap.season, episode)
        for gap in ledger
        if gap.kind == "missing_episode"
        and gap.status == "open"
        and isinstance(gap.episodes, (list, tuple))
        for episode in gap.episodes
        if isinstance(episode, int) and not isinstance(episode, bool)
    }
    changed = False
    for token in sorted(set(record.uncovered_tokens)):
        coordinate = parse_gap_token(token)
        if coordinate is None:
            continue
        gap_id = f"{record.work_unit_id}::missing_episode::{token}"
        if gap_id in existing_ids:
            continue
        if (media_type, tmdb_id, coordinate[0], coordinate[1]) in open_coordinates:
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
        unit_sources = _owned_unit_sources(runner, record, root_job)
        names = [posixpath.basename(source) for source in unit_sources]
        if any(not name or name in {".", ".."} for name in names) or len(set(names)) != len(names):
            raise EngineExecutionError("existing-gap 空目录边界名无效或冲突")
        hold_root = _safe_remote_path(
            f"{runner.library_root}/ScrapeFlow/归档/{_safe_job_id(root_job.id)}/existing-gap-hold",
            field="unit existing-gap hold root",
            allow_root=False,
        )
        lane_root = (
            f"{hold_root}/{record.work_unit_id}"
            if len(unit_sources) > 1 else hold_root
        )
        for source, name in zip(unit_sources, names):
            _move_with_readback(
                runner,
                source=source,
                target_root=lane_root,
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
    *,
    pause_requested: Callable[[], bool] | None = None,
) -> WorkUnitRecord:
    """E3: plan into the locked existing work root and write with no-overwrite."""
    if record.lane_status == "merge_done" and record.writer_job_id:
        carrier = runner.get_job(record.writer_job_id)
        if carrier.phase == "executed":
            # A process can finish G/H and persist the merge carrier before
            # it reaches J.  Resume only the precise gap ledger check here;
            # never re-plan or replay the formal-library writer.
            return _complete_unit_episode_gap_registration(
                runner,
                state_root,
                root_job.id,
                record,
                carrier.plan,
            )
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
    # The planner derives the series directory from the TMDB title under the
    # parent of the locked root; the shared request builder preserves an exact
    # multi-source manifest instead of broadening to a sibling source tree.
    request = _request_for_unit(
        runner,
        record,
        root_job.id,
        state_root,
        parent_override=posixpath.dirname(work_root.rstrip("/")),
        target_scope_override=work_root,
    )
    # A previous attempt may have left a terminal carrier; plan_job refuses
    # existing ids, so retire it first (same rule as new_work units).
    carrier_id = _unit_job_id(record.work_unit_id)
    _pause_checkpoint(pause_requested)
    try:
        existing = runner.get_job(carrier_id)
    except Exception:
        existing = None
    existing_summary = (
        existing.summary if existing is not None and isinstance(existing.summary, Mapping) else {}
    )
    owns_existing = (
        existing is not None
        and existing_summary.get("internal_child") is True
        and existing_summary.get("root_job_id") == root_job.id
    )
    if owns_existing and existing.phase in {"executing", "verifying", "cleaning", "retry_wait"}:
        # A pause can land inside the formal writer after its internal carrier
        # is durable but before the WorkUnit ledger is updated.  Reconcile the
        # same plan on resume; never create a second merge carrier.
        _pause_checkpoint(pause_requested)
        recovered = (
            runner.recover_job(existing.id)
            if existing.phase in {"executing", "verifying", "cleaning"}
            else existing
        )
        if recovered.phase == "retry_wait":
            recovered = runner.execute_job(
                recovered.id,
                pause_requested=pause_requested,
            )
        if recovered.phase in {"executing", "verifying", "cleaning"}:
            raise EnginePauseRequested("归并写入在暂停边界保持可恢复")
        if recovered.phase != "executed":
            raise EngineExecutionError("归并载体恢复未完成")
        planned = recovered
    elif owns_existing and existing.phase == "executed":
        # The formal write completed before the process saw its return; finish
        # only the ledger transition after the same locked plan is validated.
        planned = existing
    else:
        _retire_stale_unit_carrier(runner, carrier_id)
        planned = _mark_internal_carrier(
            runner,
            runner.plan_job(
                request,
                job_id=carrier_id,
                pause_requested=pause_requested,
            ),
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
    # ``plan_job`` may have performed archive preprocessing.  Recheck after
    # validating the plan and pass the same root predicate into the formal
    # writer so a pause or task switch cannot cross this plan->execute gap.
    _pause_checkpoint(pause_requested)
    executed = (
        planned
        if planned.phase == "executed"
        else runner.execute_job(planned.id, pause_requested=pause_requested)
    )
    if executed.phase != "executed":
        if executed.phase in {"executing", "verifying", "cleaning"}:
            raise EnginePauseRequested("归并写入在暂停边界保持可恢复")
        raise EngineExecutionError("归并写入未完成")
    merged = replace(
        record,
        writer_job_id=planned.id,
        lane_status="merge_done",
        lane_detail=None,
        attention=None,
        updated_at=_now(),
    )
    # E3 still passes through H and then J.  The shared completion helper
    # records catalog insufficiency as visible attention and local ledger
    # persistence/readback faults as failed state without replaying the merge.
    return _complete_unit_episode_gap_registration(
        runner,
        state_root,
        root_job.id,
        merged,
        executed.plan,
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

    def persist_completed_lanes() -> None:
        """Keep earlier mutations durable if a later unit observes pause.

        The E lanes can safely stop between units, but an E1/E2 move or an
        E3 write from a preceding unit must not lose its ledger marker merely
        because the next unit reaches a pause checkpoint.
        """
        by_id = {item.work_unit_id: item for item in updated}
        save_work_unit_records(
            state_root,
            root_task_id,
            [by_id.get(record.work_unit_id, record) for record in records],
        )

    try:
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
                next_record = _merge_unit(
                    runner,
                    state_root,
                    record,
                    root_job,
                    pause_requested=pause_requested,
                )
            else:
                next_record = record
            changed = changed or next_record.as_dict() != record.as_dict()
            updated.append(next_record)
    except EnginePauseRequested:
        if changed:
            persist_completed_lanes()
        raise
    if changed:
        persist_completed_lanes()
    return updated


__all__ = [
    "compute_known_gap_tokens",
    "execute_unit_e_lanes",
]
