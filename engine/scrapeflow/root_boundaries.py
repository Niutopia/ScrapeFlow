"""B/W composition: source snapshot + work-boundary analysis for one root job.

The pure domain modules (``source_inventory``, ``boundary_analysis``,
``work_units``) provide the building blocks; this module owns the small
AList-facing composition that snapshots a source tree, splits it into
``WorkCandidate`` rows, converts them into ``WorkUnitRecord`` entries and
persists the ledger beside the root job.  It performs no TMDB calls, performs
no writes outside the local state root, and never touches the formal library.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping

from .boundary_analysis import analyze_boundaries
from .source_inventory import build_source_inventory
from .work_units import (
    WorkUnitRecord,
    create_work_units_from_candidates,
    save_work_unit_records,
)

MAX_SNAPSHOT_DIRECTORIES = 10_000
MAX_SNAPSHOT_FILES = 200_000


def walk_source_rows(alist: object, source_path: str) -> list[dict[str, Any]]:
    """Fetch a fresh bounded recursive listing of ``source_path``.

    ``AListClient.walk()`` returns files only and applies planner-side extras
    filtering, which is the wrong evidence shape for a boundary snapshot.  This
    walk is read-only and returns directory and file rows with a ``full_path``
    key so ``build_source_inventory`` can reconstruct the real tree.  Unsafe
    names (dot segments, separators, control bytes) are skipped rather than
    failing the whole snapshot.
    """
    listing = getattr(alist, "list", None)
    if not callable(listing):
        raise ValueError("AList client lacks list()")
    root = str(source_path).rstrip("/") or "/"
    rows: list[dict[str, Any]] = []
    stack: list[str] = [root]
    directory_count = 0
    while stack:
        current = stack.pop()
        try:
            items = listing(current, refresh=True)
        except TypeError:
            items = listing(current)
        if not isinstance(items, list):
            raise ValueError(f"AList 目录列表无效: {current}")
        for item in items:
            if not isinstance(item, Mapping):
                continue
            name = item.get("name")
            if (
                not isinstance(name, str)
                or not name
                or name in {".", ".."}
                or "/" in name
                or "\\" in name
                or "\x00" in name
            ):
                continue
            full_path = current.rstrip("/") + "/" + name
            row = dict(item)
            row["full_path"] = full_path
            rows.append(row)
            if len(rows) > MAX_SNAPSHOT_FILES:
                raise ValueError(
                    f"来源快照文件数超过安全上限 {MAX_SNAPSHOT_FILES}: {root}"
                )
            if item.get("is_dir"):
                directory_count += 1
                if directory_count > MAX_SNAPSHOT_DIRECTORIES:
                    raise ValueError(
                        f"来源快照目录数超过安全上限 {MAX_SNAPSHOT_DIRECTORIES}: {root}"
                    )
                stack.append(full_path)
    return rows


def analyze_root_boundaries(
    alist: object,
    source_path: str,
    *,
    root_task_id: str,
    state_root: Path,
) -> list[WorkUnitRecord]:
    """Snapshot one source tree, split it into WorkUnits and persist them.

    This is the runtime B/W step: it must run before any TMDB identity work
    (contract rule 3).  The persisted ledger ``work_units_<root_task_id>.json``
    is the input for the per-unit identity stage (C/U).
    """
    rows = walk_source_rows(alist, source_path)
    node = build_source_inventory(rows, source_path)
    candidates = analyze_boundaries(node, root_task_id=root_task_id)
    records = create_work_units_from_candidates(candidates, root_task_id)
    save_work_unit_records(state_root, root_task_id, records)
    return records


__all__ = [
    "MAX_SNAPSHOT_DIRECTORIES",
    "MAX_SNAPSHOT_FILES",
    "analyze_root_boundaries",
    "walk_source_rows",
]
