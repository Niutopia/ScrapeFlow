"""B/W composition: source snapshot + work-boundary analysis for one root job.

The pure domain modules (``source_inventory``, ``boundary_analysis``,
``work_units``) provide the building blocks; this module owns the small
AList-facing composition that snapshots a source tree, splits it into
``WorkCandidate`` rows, converts them into ``WorkUnitRecord`` entries and
persists the ledger beside the root job.  It performs no TMDB calls, performs
no writes outside the local state root, and never touches the formal library.
"""

from __future__ import annotations

import json
import uuid
from pathlib import Path
from typing import Any, Mapping

from .boundary_analysis import analyze_boundaries
from .serialization import atomic_write_json
from .source_inventory import build_source_inventory
from .source_objects import (
    SourceManifest,
    SourceObjectClaim,
    SourceObjectValidationError,
    validate_unique_source_object_ownership,
)
from .work_units import (
    WorkUnitRecord,
    create_work_units_from_candidates,
    save_work_unit_records,
)

MAX_SNAPSHOT_DIRECTORIES = 10_000
MAX_SNAPSHOT_FILES = 200_000


def _snapshot_path(state_root: Path, root_task_id: str) -> Path:
    return state_root / f"work_snapshot_{root_task_id}.json"


def _source_manifest_path(state_root: Path, root_task_id: str) -> Path:
    """Return the optional exact-object sidecar for one B/W snapshot."""
    return state_root / f"source_manifest_{root_task_id}.json"


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
    is the input for the per-unit identity stage (C/U); the persisted snapshot
    ``work_snapshot_<root_task_id>.json`` lets C rebuild the exact tree that B
    analysed without re-listing the provider.
    """
    snapshot, records = build_root_boundary_analysis(
        alist,
        source_path,
        root_task_id=root_task_id,
    )
    persist_root_boundary_analysis(
        state_root,
        root_task_id,
        snapshot,
        records,
    )
    return records


def build_root_boundary_analysis(
    alist: object,
    source_path: str,
    *,
    root_task_id: str,
    source_revision: int = 1,
) -> tuple[dict[str, Any], list[WorkUnitRecord]]:
    """Build a fresh B/W snapshot and pending ledger entirely in memory."""
    rows = walk_source_rows(alist, source_path)
    root = str(source_path).rstrip("/") or "/"
    snapshot = {"root": root, "rows": rows}
    # Keep the legacy B/W rows intact for old readers, while publishing a
    # stricter exact-object proof whenever provider rows carry the required
    # byte metadata.  A malformed/incomplete provider row is recorded as an
    # explicit error rather than silently manufacturing a size or widening
    # ownership; existing callers still receive the ordinary B/W result and
    # can choose to park when they require the exact proof.
    snapshot_id = f"{root_task_id}:{source_revision}:{uuid.uuid4().hex}"
    exact: SourceManifest | None = None
    try:
        exact = SourceManifest.from_listing_rows(
            rows, root_path=root, snapshot_id=snapshot_id,
        )
        snapshot["source_manifest"] = exact.as_dict()
    except SourceObjectValidationError as exc:
        snapshot["source_manifest_error"] = str(exc)
    node = build_source_inventory(rows, source_path)
    candidates = analyze_boundaries(node, root_task_id=root_task_id)
    records = create_work_units_from_candidates(
        candidates,
        root_task_id,
        source_revision=source_revision,
    )
    if exact is not None:
        claims: list[SourceObjectClaim] = []
        for record in records:
            owned = tuple(
                obj
                for obj in exact.objects
                if any(
                    obj.path == scope or obj.path.startswith(scope + "/")
                    for scope in record.source_paths
                )
                and not obj.is_directory
            )
            if owned:
                claims.append(SourceObjectClaim(
                    owner_kind="work_unit",
                    owner_id=record.work_unit_id,
                    objects=owned,
                ))
        # A source object may be intentionally residual, but it may not be
        # claimed by two WorkUnits.  The exact check is local and read-only;
        # an overlap parks the B/W rebuild instead of widening F's scope.
        validate_unique_source_object_ownership(claims)
    return snapshot, records


def persist_root_boundary_analysis(
    state_root: Path,
    root_task_id: str,
    snapshot: Mapping[str, Any],
    records: list[WorkUnitRecord],
) -> None:
    """Atomically advance B/W state through an empty-ledger safety marker.

    Clearing the ledger first makes any interrupted rebuild fail closed: the
    normal root pipeline treats an empty ledger as a signal to rerun B/W, so no
    old WorkUnit can ever consume a newer snapshot.  Only local JSON state is
    written here; AList is read before this function is entered.
    """
    save_work_unit_records(state_root, root_task_id, [])
    atomic_write_json(
        _snapshot_path(state_root, root_task_id),
        dict(snapshot),
        allow_nan=False,
    )
    exact = snapshot.get("source_manifest")
    if isinstance(exact, Mapping):
        atomic_write_json(
            _source_manifest_path(state_root, root_task_id),
            dict(exact),
            allow_nan=False,
        )
    else:
        # A failed exact proof must not leave an older sidecar that could be
        # mistaken for the current source snapshot after a rebuild.
        try:
            _source_manifest_path(state_root, root_task_id).unlink()
        except FileNotFoundError:
            pass
    save_work_unit_records(state_root, root_task_id, records)


def load_source_snapshot(
    state_root: Path,
    root_task_id: str,
) -> dict[str, Any] | None:
    """Load the persisted B snapshot; ``None`` when absent or malformed."""
    try:
        raw = json.loads(
            _snapshot_path(state_root, root_task_id).read_text(encoding="utf-8")
        )
    except (FileNotFoundError, OSError, UnicodeDecodeError, json.JSONDecodeError):
        return None
    if not isinstance(raw, Mapping):
        return None
    root = raw.get("root")
    rows = raw.get("rows")
    if not isinstance(root, str) or not root or not isinstance(rows, list):
        return None
    result: dict[str, Any] = {
        "root": root,
        "rows": [row for row in rows if isinstance(row, Mapping)],
    }
    manifest = raw.get("source_manifest")
    if isinstance(manifest, Mapping):
        result["source_manifest"] = dict(manifest)
    error = raw.get("source_manifest_error")
    if isinstance(error, str) and error:
        result["source_manifest_error"] = error
    return result


def load_source_manifest(
    state_root: Path,
    root_task_id: str,
) -> SourceManifest | None:
    """Load the exact B/W object proof, if that snapshot produced one."""
    snapshot = load_source_snapshot(state_root, root_task_id)
    if snapshot is None:
        return None
    raw = snapshot.get("source_manifest")
    if not isinstance(raw, Mapping):
        return None
    try:
        return SourceManifest.from_dict(raw)
    except SourceObjectValidationError:
        return None


__all__ = [
    "MAX_SNAPSHOT_DIRECTORIES",
    "MAX_SNAPSHOT_FILES",
    "analyze_root_boundaries",
    "build_root_boundary_analysis",
    "load_source_snapshot",
    "load_source_manifest",
    "persist_root_boundary_analysis",
    "walk_source_rows",
]
