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
import posixpath
import re
from typing import Any, Mapping, Sequence

from engine.scrapeflow.media_policy import is_video_filename
from engine.scrapeflow.root_boundaries import load_source_snapshot
from engine.scrapeflow.serialization import atomic_write_json
from engine.scrapeflow.gap_ledger import discover_episode_gaps
from engine.scrapeflow.replenishment_matching import audit_episode_tokens
from engine.scrapeflow.target_shelf import target_root_for_shelf
from engine.scrapeflow.work_units import (
    WorkUnitRecord,
    load_work_unit_records,
    save_work_unit_records,
)

from .redaction import redact_error
from .simple_engine_runner import EngineJob, EngineRequest, SimpleEngineRunner
from .simple_library_audit import TmdbEpisodeCatalog


def _now() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _acceptance_path(state_root: Path, root_task_id: str) -> Path:
    return state_root / f"work_acceptance_{root_task_id}.json"


_TERMINAL_CARRIER_PHASES = frozenset({
    "failed", "failed_planning", "failed_write", "failed_verification",
    "failed_cleanup", "failed_archive", "failed_provider", "failed_identity",
    "cancelled", "planned",
})


def _retire_stale_unit_carrier(runner: SimpleEngineRunner, carrier_id: str) -> None:
    """Remove a terminal unit carrier so a retry re-plans from current state.

    Only internal unit carriers (``internal_child``) in terminal/planned
    phases are ever removed; the root ledger, gap ledger and formal library
    are never touched here.
    """
    try:
        carrier = runner.get_job(carrier_id)
    except Exception:
        return
    summary = carrier.summary if isinstance(carrier.summary, Mapping) else {}
    if summary.get("internal_child") is not True:
        return
    if carrier.phase not in _TERMINAL_CARRIER_PHASES:
        return
    path = runner._job_path(carrier_id)  # noqa: SLF001 - carrier composition
    if path.exists():
        path.unlink()


def _unit_job_id(work_unit_id: str) -> str:
    return f"unit-{work_unit_id}"


def _mark_internal_carrier(
    runner: SimpleEngineRunner,
    carrier: EngineJob,
    root_task_id: str,
) -> EngineJob:
    """Tag one unit carrier so it never surfaces as a second public task.

    ``internal_child``/``root_job_id`` are the stock internal-carrier markers
    the provider lane already uses; no new summary field is introduced.
    """
    summary = dict(carrier.summary)
    if summary.get("internal_child") is True and summary.get("root_job_id") == root_task_id:
        return carrier
    summary["internal_child"] = True
    summary["root_job_id"] = root_task_id
    marked = replace(carrier, summary=summary, updated_at=_now())
    atomic_write_json(
        runner._job_path(carrier.id),  # noqa: SLF001 - carrier composition
        marked.as_dict(),
        allow_nan=False,
    )
    return marked


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


_ABSOLUTE_BRACKET_RE = re.compile(r"\[0*(\d{1,4})\]", re.IGNORECASE)
_SE_TOKEN_RE = re.compile(r"S0*\d{1,3}E0*\d{1,4}", re.IGNORECASE)
_LEADING_INDEX_RE = re.compile(r"^\d{1,3}[.、．\-]\s*")
_BRACKET_GROUP_RE = re.compile(
    r"\[[^\]]*\]|【[^】]*】|"
    r"\([^)]*(?:1080p|2160p|4k|x26[45]|hevc|avc|bdrip|web-?dl|bluray|remux)[^)]*\)",
    re.IGNORECASE,
)
_RESOLUTION_TOKEN_RE = re.compile(
    r"\b(1080p|2160p|4k|720p|480p|x26[45]|hevc|avc|bdrip|web-?dl|bluray|remux|dts|aac|flac|ma10p)\b",
    re.IGNORECASE,
)
_JUNK_DIGIT_RUN_RE = re.compile(r"\d{2,}")


def _clean_container_name(value: object) -> str | None:
    """Strip release noise from a container folder name (bounded).

    The operator's own folder name is the primary container name; only
    obvious noise (release groups, resolutions, leading indexes, filler
    symbols, digit runs) is removed.  A name that is still garbage after
    cleaning yields ``None`` so the caller falls back to TMDB evidence.
    """
    name = str(value or "").strip()
    if not name:
        return None
    name = _BRACKET_GROUP_RE.sub(" ", name)
    name = _LEADING_INDEX_RE.sub(" ", name)
    name = _RESOLUTION_TOKEN_RE.sub(" ", name)
    name = re.sub(r"[#@！!]+", " ", name)
    name = re.sub(r"\s+", " ", name).strip(" ._-/\\")
    if len(name) < 2 or _JUNK_DIGIT_RUN_RE.search(name):
        return None
    return name


def _container_plan(
    runner: SimpleEngineRunner,
    root_job: EngineJob,
    records: list[WorkUnitRecord],
) -> tuple[list[WorkUnitRecord], str | None, int | None]:
    """Decide the Fate-style container layout for a multi-unit root.

    Evidence-driven, no name guessing:

    - one distinct TV identity (e.g. 刀剑神域 + its movies): the main series
      owns the container; TV units plan at the shelf root and the main
      unit's planned target root becomes the parent of every movie unit;
    - several distinct TV identities or none (e.g. Fate, 空之境界): the
      container is a pure collection named after the cleaned intake folder
      (falling back to the first unit's TMDB title/boundary), and every
      unit nests under it.

    Single-unit roots are returned unchanged with no container parent.
    Returns ``(ordered_records, container_parent, main_tmdb)``.
    """
    if len(records) <= 1:
        return list(records), None, None
    tv_ids: list[int] = []
    for record in records:
        identity = record.identity or {}
        tmdb_id = identity.get("tmdb_id")
        if (
            str(identity.get("media_type") or "") == "tv"
            and isinstance(tmdb_id, int)
            and not isinstance(tmdb_id, bool)
            and tmdb_id > 0
        ):
            if tmdb_id not in tv_ids:
                tv_ids.append(tmdb_id)
    if len(tv_ids) == 1:
        main_tmdb = tv_ids[0]
        ordered = sorted(
            records,
            key=lambda record: (
                0
                if (record.identity or {}).get("tmdb_id") == main_tmdb
                and str((record.identity or {}).get("media_type") or "") == "tv"
                else 1
            ),
        )
        return ordered, None, main_tmdb
    shelf = str(root_job.target_shelf or "anime")
    shelf_root = target_root_for_shelf(runner.library_root, shelf)
    intake_basename = posixpath.basename(
        str(runner._job_ingress_source(root_job)).rstrip("/")  # noqa: SLF001
    )
    container_name = _clean_container_name(intake_basename)
    if container_name is None:
        tv_records = [
            record for record in records
            if str((record.identity or {}).get("media_type") or "") == "tv"
        ]
        for candidate in tv_records or list(records):
            title = str((candidate.identity or {}).get("title") or "").strip()
            cleaned = (
                title
                if title
                else _clean_container_name(
                    posixpath.basename(str(candidate.source_paths[0]).rstrip("/"))
                )
            )
            if cleaned:
                container_name = cleaned
                break
    container_parent = (
        f"{shelf_root}/{container_name}" if container_name else None
    )
    ordered = sorted(
        records,
        key=lambda record: (
            0
            if str((record.identity or {}).get("media_type") or "") == "tv"
            else 1
        ),
    )
    return ordered, container_parent, None


def _unit_video_rows(
    state_root: Path,
    root_task_id: str,
    record: WorkUnitRecord,
) -> list[dict[str, Any]]:
    """Return the unit's video-file rows from the persisted B snapshot."""
    snapshot = load_source_snapshot(state_root, root_task_id)
    if snapshot is None or not record.source_paths:
        return []
    boundary = str(record.source_paths[0]).rstrip("/")
    rows: list[dict[str, Any]] = []
    for row in snapshot["rows"]:
        full_path = str(row.get("full_path") or "")
        if row.get("is_dir") is True:
            continue
        if full_path != boundary and not full_path.startswith(boundary + "/"):
            continue
        name = str(row.get("name") or "")
        if is_video_filename(name):
            rows.append(row)
    return rows


def _multi_season_absolute_map_path(
    runner: SimpleEngineRunner,
    state_root: Path,
    root_task_id: str,
    record: WorkUnitRecord,
) -> str | None:
    """Derive an explicit episode map for a multi-season absolute-number block.

    ``[01]..[47]`` style releases whose episode count exactly equals one
    contiguous suffix of the official TMDB seasons (e.g. 爱丽丝篇 = S3+S4 of
    the parent series) cannot be expressed by a single ``season`` hint.  The
    Engine's explicit episode map bypasses the smart season inference for
    exactly this shape; the map is written to the local state root and the
    request carries only its path.  Returns ``None`` when the shape does not
    match, leaving the ordinary planner path untouched.
    """
    identity = record.identity or {}
    if str(identity.get("media_type") or "") != "tv":
        return None
    rows = _unit_video_rows(state_root, root_task_id, record)
    if not rows:
        return None
    names = [str(row.get("name") or "") for row in rows]
    if any(_SE_TOKEN_RE.search(name) for name in names):
        return None
    numbers: list[int] = []
    for name in names:
        match = _ABSOLUTE_BRACKET_RE.search(name)
        if match is None:
            # Non-bracketed videos (NCED/NCOP/[18.5]-style specials) stay on
            # the planner's ordinary special path; only regular bracketed
            # episodes participate in the explicit map.
            continue
        numbers.append(int(match.group(1)))
    if not numbers:
        return None
    if sorted(numbers) != list(range(1, len(numbers) + 1)):
        return None
    total = len(numbers)
    tmdb_id = identity.get("tmdb_id")
    if isinstance(tmdb_id, bool) or not isinstance(tmdb_id, int) or tmdb_id <= 0:
        return None
    try:
        expected = TmdbEpisodeCatalog(runner.tmdb)(
            {"tmdb_id": tmdb_id, "media_type": "tv"}
        )
    except Exception:
        return None
    if expected is None:
        return None
    seasons: dict[int, list[int]] = {}
    for season, episode_rows in expected.items():
        if isinstance(season, bool) or not isinstance(season, int) or season <= 0:
            continue
        if not isinstance(episode_rows, list):
            continue
        episodes = [
            int(row.get("episode_number"))
            for row in episode_rows
            if isinstance(row, Mapping)
            and isinstance(row.get("episode_number"), int)
            and not isinstance(row.get("episode_number"), bool)
            and int(row.get("episode_number")) > 0
        ]
        if episodes:
            seasons[season] = sorted(set(episodes))
    if not seasons:
        return None
    ordered = sorted(seasons)
    chosen: list[int] | None = None
    for start in ordered:
        suffix = [season for season in ordered if season >= start]
        if len(suffix) >= 2 and sum(len(seasons[s]) for s in suffix) == total:
            chosen = suffix
            break
    if chosen is None:
        return None
    mapping: dict[str, str] = {}
    absolute = 1
    for season in chosen:
        for episode in seasons[season]:
            if absolute > total:
                break
            mapping[str(absolute)] = f"S{season:02d}E{episode:02d}"
            absolute += 1
    if absolute - 1 != total:
        return None
    path = state_root / f"episode_map_{record.work_unit_id}.json"
    atomic_write_json(path, mapping, allow_nan=False)
    return str(path)


def _request_for_unit(
    runner: SimpleEngineRunner,
    record: WorkUnitRecord,
    root_task_id: str,
    state_root: Path,
    *,
    parent_override: str | None = None,
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
        "parent_path": parent_override or shelf_root,
        "media_type": media_type,
        "tmdb_id": tmdb_id,
    }
    season = identity.get("season")
    if isinstance(season, int) and not isinstance(season, bool) and season > 0:
        payload["season"] = season
    request = EngineRequest.from_mapping(payload)
    # Multi-season absolute-number blocks get the engine's explicit episode
    # map (derived from the B snapshot + official TMDB seasons); anything
    # else keeps the ordinary planner path.
    map_path = _multi_season_absolute_map_path(
        runner, state_root, root_task_id, record,
    )
    if map_path is not None:
        request = replace(request, episode_map_path=map_path)
    return request


def _register_unit_episode_gaps(
    runner: SimpleEngineRunner,
    state_root: Path,
    root_task_id: str,
    record: WorkUnitRecord,
    executed_plan: Mapping[str, Any],
) -> list[Any]:
    """Register precise per-episode gaps after a successful write (J step).

    Expected coordinates come from the official TMDB episode catalog; actual
    coordinates from the executed plan's final names.  Failures are bounded:
    a missing/unavailable catalog simply registers no gaps.
    """
    identity = record.identity or {}
    if str(identity.get("media_type")) != "tv":
        return []
    tmdb_id = identity.get("tmdb_id")
    if not isinstance(tmdb_id, int) or isinstance(tmdb_id, bool) or tmdb_id <= 0:
        return []
    try:
        catalog = TmdbEpisodeCatalog(runner.tmdb)
        expected = catalog({"tmdb_id": tmdb_id, "media_type": "tv"})
    except Exception:
        return []
    if expected is None:
        return []
    expected_by_season: dict[int, list[int]] = {}
    for season, rows in expected.items():
        if not isinstance(season, int) or isinstance(season, bool) or not isinstance(rows, list):
            continue
        episodes = [
            int(row.get("episode_number"))
            for row in rows
            if isinstance(row, Mapping)
            and isinstance(row.get("episode_number"), int)
            and not isinstance(row.get("episode_number"), bool)
            and int(row.get("episode_number")) > 0
        ]
        if episodes:
            expected_by_season[season] = episodes
    if not expected_by_season:
        return []
    actual: list[str] = []
    for item in executed_plan.get("files") or []:
        if not isinstance(item, Mapping) or item.get("media_kind") != "video":
            continue
        name = str(item.get("final_name") or "")
        for season, episode in audit_episode_tokens(name):
            actual.append(f"S{season:02d}E{episode:02d}")
    try:
        return discover_episode_gaps(
            state_root,
            root_task_id,
            record.work_unit_id,
            media_type="tv",
            tmdb_id=tmdb_id,
            expected_by_season=expected_by_season,
            actual_tokens=actual,
        )
    except Exception:
        return []


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
    root_job = runner.get_job(root_task_id)
    ordered, container_parent, main_tmdb = _container_plan(
        runner, root_job, records,
    )
    main_target_root: str | None = None
    if main_tmdb is not None:
        # On re-runs the main unit is already executed; reuse its real
        # target root as the container parent for movie units.
        for record in ordered:
            identity = record.identity or {}
            if (
                record.writer_job_id
                and str(identity.get("media_type") or "") == "tv"
                and identity.get("tmdb_id") == main_tmdb
            ):
                try:
                    carrier = runner.get_job(record.writer_job_id)
                    main_target_root = (
                        str(carrier.plan.get("target_root") or "") or None
                    )
                except Exception:
                    pass
                break
    results: list[WorkAcceptanceResult] = []
    updated: list[WorkUnitRecord] = []
    changed = False
    for record in ordered:
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
            if carrier.phase not in {"executed"}:
                # A terminal carrier is a stale plan from a previous failed
                # attempt.  Retire it and fall through to re-plan from the
                # current source state.
                _retire_stale_unit_carrier(runner, carrier.id)
                record = replace(record, writer_job_id=None)
                changed = True
            else:
                plan_files = len(carrier.plan.get("files") or [])
                results.append(WorkAcceptanceResult(
                    work_unit_id=record.work_unit_id,
                    outcome="accepted",
                    writer_job_id=carrier.id,
                    phase=carrier.phase,
                    target_root=str((carrier.plan.get("target_root")) or ""),
                    planned_files=plan_files,
                    error=carrier.error,
                    recorded_at=_now(),
                ))
                updated.append(record)
                continue
        base_record = record
        carrier_id = _unit_job_id(record.work_unit_id)
        try:
            identity = record.identity or {}
            is_main_tv = (
                main_tmdb is not None
                and str(identity.get("media_type") or "") == "tv"
                and identity.get("tmdb_id") == main_tmdb
            )
            if is_main_tv:
                parent_override = None
            elif container_parent:
                parent_override = container_parent
            elif main_tmdb is not None:
                parent_override = main_target_root
            else:
                parent_override = None
            request = _request_for_unit(
                runner, record, root_task_id, state_root,
                parent_override=parent_override,
            )
            # plan_job refuses an existing carrier id; a stale terminal
            # carrier must be gone before the fresh plan is persisted.
            _retire_stale_unit_carrier(runner, carrier_id)
            planned = _mark_internal_carrier(
                runner,
                runner.plan_job(
                    request,
                    job_id=carrier_id,
                ),
                root_task_id,
            )
            if is_main_tv and main_target_root is None:
                main_target_root = (
                    str(planned.plan.get("target_root") or "") or None
                )
            executed = runner.execute_job(planned.id)
            plan_files = len(executed.plan.get("files") or [])
            record = replace(record, writer_job_id=planned.id)
            changed = True
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
            # J step: register precise episode gaps against the official
            # TMDB catalog.  Advisories: no gap simply means no catalog or
            # no missing coordinates.
            try:
                _register_unit_episode_gaps(
                    runner, state_root, root_task_id, record, executed.plan,
                )
            except Exception:
                pass
        except Exception as exc:
            # Keep writer_job_id unset so the next run re-plans the unit;
            # retire the just-planned carrier so plan_job's existing-id
            # guard cannot pin a stale plan either.
            try:
                _retire_stale_unit_carrier(runner, carrier_id)
            except Exception:
                pass
            record = base_record
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
