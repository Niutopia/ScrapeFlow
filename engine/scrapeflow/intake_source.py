"""Read-only intake-source discovery records.

An IntakeSource represents a single direct sub-directory of ``/待刮削`` that
was observed by the intake monitor.  It is a **passive observation record**,
not an execution task.  No Engine, no planner, no TMDB call is triggered by
creating or updating an IntakeSource.

A RootJob is only created when the user explicitly selects an IntakeSource and
assigns a target shelf.  The same source path always maps to the same
``source_id`` (UUID5 of the canonical path) and at most one ``root_task_id``.
"""

from __future__ import annotations

import json
import uuid
from dataclasses import asdict, dataclass, replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Sequence


# ---------------------------------------------------------------------------
# Stable identity
# ---------------------------------------------------------------------------

_NAMESPACE = uuid.UUID("a3b4c5d6-e7f8-4001-8002-900000000001")


def intake_source_id(canonical_path: str) -> str:
    """Return the stable UUID5 for a canonical source path.

    The UUID is deterministic so repeated scans of the same path always
    produce the same ``source_id`` without needing to read existing records.
    """
    return str(uuid.uuid5(_NAMESPACE, canonical_path.rstrip("/")))


# ---------------------------------------------------------------------------
# Domain record
# ---------------------------------------------------------------------------

@dataclass(frozen=True, slots=True)
class IntakeSource:
    """Immutable snapshot of one observed ``/待刮削`` sub-directory.

    All fields are plain Python primitives so the record serialises to JSON
    without a custom encoder.  ``root_task_id`` is ``None`` until the user
    creates a RootJob for this source.
    """

    source_id: str
    canonical_path: str
    display_name: str
    first_seen_at: str
    last_seen_at: str
    present: bool
    snapshot_revision: int
    child_count: int | None
    file_count: int | None
    root_task_id: str | None

    def as_dict(self) -> dict[str, object]:
        return asdict(self)

    @classmethod
    def from_dict(cls, raw: dict[str, object]) -> "IntakeSource":
        return cls(
            source_id=str(raw["source_id"]),
            canonical_path=str(raw["canonical_path"]),
            display_name=str(raw["display_name"]),
            first_seen_at=str(raw["first_seen_at"]),
            last_seen_at=str(raw["last_seen_at"]),
            present=bool(raw.get("present", True)),
            snapshot_revision=int(raw.get("snapshot_revision", 0)),
            child_count=int(raw["child_count"]) if raw.get("child_count") is not None else None,
            file_count=int(raw["file_count"]) if raw.get("file_count") is not None else None,
            root_task_id=str(raw["root_task_id"]) if raw.get("root_task_id") is not None else None,
        )


# ---------------------------------------------------------------------------
# Catalog persistence (atomic, single-file)
# ---------------------------------------------------------------------------

def _catalog_path(state_dir: Path) -> Path:
    return state_dir / "intake-sources.json"


def _now() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def load_intake_catalog(state_dir: Path) -> list[IntakeSource]:
    """Load the persisted IntakeSource catalog; return empty list on first run."""
    path = _catalog_path(state_dir)
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return []
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        return []
    if not isinstance(raw, list):
        return []
    result: list[IntakeSource] = []
    for item in raw:
        if not isinstance(item, dict):
            continue
        try:
            result.append(IntakeSource.from_dict(item))
        except (KeyError, TypeError, ValueError):
            continue
    return result


def save_intake_catalog(state_dir: Path, sources: Sequence[IntakeSource]) -> None:
    """Atomically persist the catalog using a write-then-rename strategy."""
    from engine.scrapeflow.serialization import atomic_write_json
    path = _catalog_path(state_dir)
    atomic_write_json(path, [s.as_dict() for s in sources], allow_nan=False)


# ---------------------------------------------------------------------------
# Catalog mutation helpers (pure-functional — return new list)
# ---------------------------------------------------------------------------

def upsert_intake_source(
    catalog: list[IntakeSource],
    canonical_path: str,
    *,
    present: bool = True,
    child_count: int | None = None,
    file_count: int | None = None,
) -> tuple[list[IntakeSource], IntakeSource]:
    """Insert or update one source entry.  Returns (new_catalog, record).

    - First observation: creates a new record with ``snapshot_revision=0``.
    - Repeated observation: updates ``last_seen_at``, ``present``,
      ``child_count``, ``file_count`` and increments ``snapshot_revision``
      only when any of those values changed.
    - Disappearance: call with ``present=False`` to mark missing without
      deleting; preserves ``root_task_id`` and history.
    """
    source_id = intake_source_id(canonical_path)
    display_name = canonical_path.rstrip("/").rsplit("/", 1)[-1] or canonical_path
    now = _now()

    existing: IntakeSource | None = None
    for src in catalog:
        if src.source_id == source_id:
            existing = src
            break

    if existing is None:
        new_record = IntakeSource(
            source_id=source_id,
            canonical_path=canonical_path,
            display_name=display_name,
            first_seen_at=now,
            last_seen_at=now,
            present=present,
            snapshot_revision=0,
            child_count=child_count,
            file_count=file_count,
            root_task_id=None,
        )
        return catalog + [new_record], new_record

    changed = (
        existing.present != present
        or existing.child_count != child_count
        or existing.file_count != file_count
    )
    updated = replace(
        existing,
        last_seen_at=now,
        present=present,
        child_count=child_count,
        file_count=file_count,
        snapshot_revision=existing.snapshot_revision + (1 if changed else 0),
    )
    new_catalog = [updated if s.source_id == source_id else s for s in catalog]
    return new_catalog, updated


def mark_source_missing(
    catalog: list[IntakeSource],
    canonical_path: str,
) -> tuple[list[IntakeSource], IntakeSource | None]:
    """Mark a previously seen source as no longer present.

    Does NOT delete the record so that any associated ``root_task_id`` is
    preserved.  Returns ``None`` if the path was never registered.
    """
    source_id = intake_source_id(canonical_path)
    now = _now()
    found: IntakeSource | None = None
    new_catalog: list[IntakeSource] = []
    for src in catalog:
        if src.source_id == source_id:
            found = replace(src, present=False, last_seen_at=now,
                            snapshot_revision=src.snapshot_revision + 1)
            new_catalog.append(found)
        else:
            new_catalog.append(src)
    return new_catalog, found


def bind_root_task(
    catalog: list[IntakeSource],
    source_id: str,
    root_task_id: str,
) -> tuple[list[IntakeSource], IntakeSource | None]:
    """Attach a RootJob id to an IntakeSource.  Idempotent.

    Returns ``None`` as the second element if the source_id is not found.
    Raises ``ValueError`` if the source already has a *different* root_task_id
    (prevents accidentally overwriting an existing association).
    """
    updated: IntakeSource | None = None
    new_catalog: list[IntakeSource] = []
    for src in catalog:
        if src.source_id == source_id:
            if src.root_task_id is not None and src.root_task_id != root_task_id:
                raise ValueError(
                    f"IntakeSource {source_id!r} already bound to "
                    f"root_task_id={src.root_task_id!r}; "
                    f"cannot rebind to {root_task_id!r}"
                )
            updated = replace(src, root_task_id=root_task_id)
            new_catalog.append(updated)
        else:
            new_catalog.append(src)
    return new_catalog, updated


def retire_root_task_binding(
    catalog: list[IntakeSource],
    source_id: str,
) -> tuple[list[IntakeSource], IntakeSource | None]:
    """Detach a RootJob id from an IntakeSource after terminal cleanup.

    ``bind_root_task`` attaches the lifecycle pointer; this releases it once
    the root's terminal cleanup has archived the binding's identity evidence
    in a tombstone.  The catalog row itself (history, presence, counts) is
    preserved so the source stays observable.  Idempotent; returns ``None``
    as the second element when the source_id is not found or was already
    unbound.
    """
    updated: IntakeSource | None = None
    new_catalog: list[IntakeSource] = []
    for src in catalog:
        if src.source_id == source_id:
            if src.root_task_id is None:
                new_catalog.append(src)
                continue
            updated = replace(src, root_task_id=None)
            new_catalog.append(updated)
        else:
            new_catalog.append(src)
    return new_catalog, updated


def find_by_source_id(
    catalog: list[IntakeSource],
    source_id: str,
) -> IntakeSource | None:
    for src in catalog:
        if src.source_id == source_id:
            return src
    return None


def find_by_path(
    catalog: list[IntakeSource],
    canonical_path: str,
) -> IntakeSource | None:
    target = intake_source_id(canonical_path)
    return find_by_source_id(catalog, target)
