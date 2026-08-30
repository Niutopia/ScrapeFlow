"""Scoped gap re-audit: close open gaps the library already proves present.

The J step registers what a freshly written unit is missing; historical bugs
(or two units of one series registering each other's seasons) can leave
phantom rows whose coordinates actually exist in the library.  Contract rule
2.6 requires an audit proof before a gap is closed, and rule 4 forbids the
acquisition lane from chasing coordinates that are already satisfied.

``reaudit_open_gaps`` is that proof: it walks each unit's real library root
with a fresh AList listing and closes only ``missing_episode`` /
``missing_season`` gaps whose coordinates appear in the listed file names.
It never touches media, never writes anything besides gap-ledger status
transitions, and is safe to run repeatedly.
"""

from __future__ import annotations

import posixpath
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from engine.scrapeflow.gap_ledger import close_gap, load_gap_ledger
from engine.scrapeflow.replenishment_matching import audit_episode_tokens
from engine.scrapeflow.work_units import load_work_unit_records

_MAX_DEPTH = 8


def _work_roots(runner: Any, state_root: Path, root_task_id: str) -> dict[str, str]:
    """Map each work unit to its real library root (merge target or plan)."""
    roots: dict[str, str] = {}
    for record in load_work_unit_records(state_root, root_task_id):
        root = record.matched_work_root
        if not root and record.writer_job_id:
            try:
                job = runner.get_job(record.writer_job_id)
                plan = job.plan if isinstance(job.plan, Mapping) else {}
                root = str(plan.get("target_root") or "") or None
            except Exception:
                root = None
        if root:
            roots[record.work_unit_id] = str(root).rstrip("/")
    return roots


def _unit_identity(
    state_root: Path,
    root_task_id: str,
) -> dict[str, tuple[str, int]]:
    """Map each unit id to its confirmed (media_type, tmdb_id) identity."""
    identity: dict[str, tuple[str, int]] = {}
    for record in load_work_unit_records(state_root, root_task_id):
        raw = record.identity if isinstance(record.identity, Mapping) else {}
        media_type = str(raw.get("media_type") or "")
        tmdb_id = raw.get("tmdb_id")
        if (
            media_type in {"movie", "tv"}
            and isinstance(tmdb_id, int)
            and not isinstance(tmdb_id, bool)
            and tmdb_id > 0
        ):
            identity[record.work_unit_id] = (media_type, tmdb_id)
    return identity


def _resolve_stale_roots(
    runner: Any,
    roots: dict[str, str],
    identity: dict[str, tuple[str, int]],
) -> None:
    """Re-point units whose recorded library root no longer lists.

    A manual consolidation (or an earlier mis-placed write) can move the
    work's files to a different library path after D recorded its
    ``matched_work_root``.  The recorded path then lists empty or missing
    and every gap under it stays open forever even though the library holds
    the coordinates.  For exactly those units, fall back to the library
    index: the NFO-confirmed root of the unit's own confirmed identity.
    """
    stale: dict[str, str] = {}
    for unit_id, root in roots.items():
        try:
            rows = runner.alist.list(root, refresh=True)
        except Exception:
            rows = None
        if rows is None or not any(
            isinstance(item, Mapping) for item in rows
        ):
            stale[unit_id] = root
    if not stale:
        return
    wanted = {
        identity[unit_id] for unit_id in stale if unit_id in identity
    }
    if not wanted:
        return
    try:
        from .library_index import build_library_index

        index = build_library_index(runner.alist, runner.library_root)
    except Exception:
        return
    resolved: dict[tuple[str, int], str] = {}
    for work in index.works:
        key = (work.media_type, work.tmdb_id)
        if key in wanted and key not in resolved:
            resolved[key] = work.work_root
    for unit_id in stale:
        unit_identity = identity.get(unit_id)
        if unit_identity is not None and unit_identity in resolved:
            roots[unit_id] = resolved[unit_identity]


def _actual_coordinates(runner: Any, root: str) -> set[tuple[int, int]]:
    """Fresh-listing walk of one library root collecting SxxEyy coordinates."""
    listing = getattr(runner.alist, "list", None)
    if not callable(listing):
        return set()
    actual: set[tuple[int, int]] = set()

    def visit(path: str, depth: int) -> None:
        if depth > _MAX_DEPTH:
            return
        try:
            rows = listing(path, refresh=True)
        except TypeError:
            rows = listing(path)
        if not isinstance(rows, list):
            return
        for item in rows:
            if not isinstance(item, Mapping):
                continue
            name = str(item.get("name") or "")
            if not name or name in {".", ".."} or "/" in name or "\\" in name:
                continue
            if item.get("is_dir") is True:
                visit(posixpath.join(path, name), depth + 1)
                continue
            for season, episode in audit_episode_tokens(name):
                actual.add((season, episode))

    visit(root, 0)
    return actual


def _gap_coordinates(gap: Any) -> set[tuple[int, int]]:
    if not isinstance(gap.season, int) or isinstance(gap.season, bool):
        return set()
    return {
        (gap.season, int(episode))
        for episode in (gap.episodes or ())
        if isinstance(episode, int) and not isinstance(episode, bool) and episode > 0
    }


def reaudit_open_gaps(
    runner: Any,
    state_root: Path,
    root_task_id: str,
) -> dict[str, Any]:
    """Close open episode/season gaps whose coordinates the library holds.

    Returns ``{"closed": [gap_id, ...], "kept_open": [...], "errors": [...]}``.
    Gaps whose unit has no known library root, whose coordinates are empty,
    or whose listing failed stay open (fail closed).
    """
    state_root = Path(state_root)
    roots = _work_roots(runner, state_root, root_task_id)
    _resolve_stale_roots(runner, roots, _unit_identity(state_root, root_task_id))
    actual_by_unit: dict[str, set[tuple[int, int]]] = {}
    errors: list[str] = []
    for unit_id, root in roots.items():
        try:
            actual_by_unit[unit_id] = _actual_coordinates(runner, root)
        except Exception as exc:  # listing failure: keep every gap open
            errors.append(f"{unit_id}: {type(exc).__name__}")

    closed: list[str] = []
    kept: list[str] = []
    for gap in load_gap_ledger(state_root, root_task_id):
        if gap.status != "open" or gap.kind not in {"missing_episode", "missing_season"}:
            continue
        actual = actual_by_unit.get(gap.work_unit_id)
        coordinates = _gap_coordinates(gap)
        if actual is None or not coordinates or not coordinates <= actual:
            kept.append(gap.gap_id)
            continue
        try:
            close_gap(state_root, root_task_id, gap.gap_id, note="reaudit")
        except KeyError:
            kept.append(gap.gap_id)
            continue
        closed.append(gap.gap_id)
    return {"closed": closed, "kept_open": kept, "errors": errors}


__all__ = ["reaudit_open_gaps"]
