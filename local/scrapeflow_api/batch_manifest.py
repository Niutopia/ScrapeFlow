"""Small, durable batch-order metadata for authorized intake sources.

This module intentionally does *not* schedule work.  In particular it does
not persist a selected RootJob, a pause bit, a writer lock, or any execution
plan.  Those remain the responsibility of the existing single control record
and single writer.  The manifest merely retains the user-authorized order and
the latest bounded result of a fresh source inspection.

The file is fail-closed on corruption: a missing file means that no batch has
been configured yet, while an unreadable or malformed existing file raises a
``BatchManifestValidationError`` rather than silently forgetting an existing
authorization queue.
"""

from __future__ import annotations

import json
import posixpath
import re
from collections.abc import Mapping
from dataclasses import dataclass, replace
from datetime import datetime
from pathlib import Path
from typing import Final

from engine.scrapeflow.intake_source import intake_source_id
from engine.scrapeflow.serialization import atomic_write_json


BATCH_MANIFEST_FILENAME: Final = "batch-manifest.json"
BATCH_MANIFEST_VERSION: Final = 1
MAX_BATCH_ITEMS: Final = 1024

BATCH_ITEM_STATES: Final = frozenset({
    "scheduled",
    "inspecting",
    "ready",
    "active",
    "completed",
    "needs_attention",
    "skipped_currently_nonmedia",
    "technical_failure",
})

TARGET_SHELVES: Final = frozenset({"movie", "anime", "us_tv"})


class BatchManifestError(ValueError):
    """Base error for a batch manifest that cannot safely be used."""


class BatchManifestValidationError(BatchManifestError):
    """The stored or supplied batch metadata violates its narrow schema."""


class BatchManifestTransitionError(BatchManifestError):
    """A requested queue-state transition is not part of the batch lifecycle."""


_SAFE_ID = re.compile(r"\A[a-zA-Z0-9][a-zA-Z0-9._:-]{0,127}\Z")
_UTC_SECOND = "%Y-%m-%dT%H:%M:%SZ"
_UNSET = object()


def _require_string(value: object, field: str, *, maximum: int) -> str:
    if not isinstance(value, str) or not value or len(value) > maximum:
        raise BatchManifestValidationError(f"{field} must be a non-empty string")
    if any(ord(character) < 32 for character in value):
        raise BatchManifestValidationError(f"{field} contains a control character")
    return value


def _validate_source_path(value: object) -> str:
    path = _require_string(value, "source_path_snapshot", maximum=4096)
    if (
        not path.startswith("/")
        or path == "/"
        or path.endswith("/")
        or "\\" in path
        or posixpath.normpath(path) != path
    ):
        raise BatchManifestValidationError(
            "source_path_snapshot must be a canonical absolute POSIX child path"
        )
    parts = path.split("/")[1:]
    if not parts or any(part in {"", ".", ".."} for part in parts):
        raise BatchManifestValidationError(
            "source_path_snapshot contains an invalid path component"
        )
    return path


def _validate_source_id(value: object, source_path_snapshot: str) -> str:
    source_id = _require_string(value, "source_id", maximum=128)
    if _SAFE_ID.fullmatch(source_id) is None:
        raise BatchManifestValidationError("source_id has an invalid format")
    if source_id != intake_source_id(source_path_snapshot):
        raise BatchManifestValidationError(
            "source_id must match the stable IntakeSource identity for its path"
        )
    return source_id


def _validate_shelf(value: object) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or value not in TARGET_SHELVES:
        raise BatchManifestValidationError(
            "shelf must be one of movie, anime, us_tv, or null"
        )
    return value


def _validate_sort(value: object) -> int:
    if (
        isinstance(value, bool)
        or not isinstance(value, int)
        or not 0 <= value <= 2**31 - 1
    ):
        raise BatchManifestValidationError("sort must be a non-negative 32-bit integer")
    return value


def _validate_utc_second(value: object, field: str) -> str:
    timestamp = _require_string(value, field, maximum=20)
    try:
        datetime.strptime(timestamp, _UTC_SECOND)
    except ValueError as exc:
        raise BatchManifestValidationError(
            f"{field} must be a UTC timestamp with whole-second precision"
        ) from exc
    return timestamp


@dataclass(frozen=True, slots=True)
class FreshResult:
    """A bounded reference to one completed recursive fresh inspection.

    ``snapshot_id`` is an opaque source-snapshot reference.  It deliberately
    carries no file inventory, target path, RootJob ID, or plan: a future
    activation must create its own fresh execution evidence rather than reuse
    this queue summary.
    """

    checked_at: str
    source_present: bool
    media_count: int
    snapshot_id: str | None = None

    def __post_init__(self) -> None:
        _validate_utc_second(self.checked_at, "checked_at")
        if type(self.source_present) is not bool:
            raise BatchManifestValidationError("source_present must be boolean")
        if (
            isinstance(self.media_count, bool)
            or not isinstance(self.media_count, int)
            or self.media_count < 0
        ):
            raise BatchManifestValidationError("media_count must be a non-negative integer")
        if not self.source_present and self.media_count != 0:
            raise BatchManifestValidationError(
                "a missing source cannot report executable media"
            )
        if self.snapshot_id is not None:
            _require_string(self.snapshot_id, "snapshot_id", maximum=256)

    def as_dict(self) -> dict[str, object]:
        return {
            "checked_at": self.checked_at,
            "source_present": self.source_present,
            "media_count": self.media_count,
            "snapshot_id": self.snapshot_id,
        }

    @classmethod
    def from_dict(cls, raw: Mapping[str, object]) -> "FreshResult":
        required = {"checked_at", "source_present", "media_count"}
        if not required.issubset(set(raw)) or set(raw) - required - {"snapshot_id"}:
            raise BatchManifestValidationError("invalid fresh result record")
        return cls(
            checked_at=raw["checked_at"],  # type: ignore[arg-type]
            source_present=raw["source_present"],  # type: ignore[arg-type]
            media_count=raw["media_count"],  # type: ignore[arg-type]
            snapshot_id=raw.get("snapshot_id"),  # type: ignore[arg-type]
        )


@dataclass(frozen=True, slots=True)
class BatchManifestItem:
    """One user-authorized intake source in the batch queue.

    The six fields are intentionally the complete persisted payload.  The
    item is not an execution task and does not carry a RootJob identifier.
    """

    source_id: str
    source_path_snapshot: str
    shelf: str | None
    sort: int
    state: str
    recent_fresh_result: FreshResult | None = None

    def __post_init__(self) -> None:
        path = _validate_source_path(self.source_path_snapshot)
        _validate_source_id(self.source_id, path)
        _validate_shelf(self.shelf)
        _validate_sort(self.sort)
        if not isinstance(self.state, str) or self.state not in BATCH_ITEM_STATES:
            raise BatchManifestValidationError(f"unsupported batch state: {self.state!r}")
        if self.recent_fresh_result is not None and not isinstance(
            self.recent_fresh_result, FreshResult
        ):
            raise BatchManifestValidationError(
                "recent_fresh_result must be FreshResult or null"
            )
        self._validate_state_evidence()

    @property
    def source_path(self) -> str:
        """Convenience alias for consumers that do not need snapshot wording."""
        return self.source_path_snapshot

    @property
    def fresh_result(self) -> FreshResult | None:
        """Convenience alias for the latest inspection summary."""
        return self.recent_fresh_result

    def _validate_state_evidence(self) -> None:
        fresh = self.recent_fresh_result
        if self.state in {"scheduled", "inspecting"} and fresh is not None:
            raise BatchManifestValidationError(
                f"{self.state} items cannot carry stale fresh evidence"
            )
        if self.state in {"ready", "active"}:
            if self.shelf is None:
                raise BatchManifestValidationError(
                    f"{self.state} items require an explicit shelf"
                )
            if fresh is None or not fresh.source_present or fresh.media_count <= 0:
                raise BatchManifestValidationError(
                    f"{self.state} items require a present fresh media result"
                )
        if self.state == "skipped_currently_nonmedia":
            if fresh is None or not fresh.source_present or fresh.media_count != 0:
                raise BatchManifestValidationError(
                    "skipped_currently_nonmedia requires a present zero-media fresh result"
                )

    def as_dict(self) -> dict[str, object]:
        return {
            "source_id": self.source_id,
            "source_path_snapshot": self.source_path_snapshot,
            "shelf": self.shelf,
            "sort": self.sort,
            "state": self.state,
            "recent_fresh_result": (
                self.recent_fresh_result.as_dict()
                if self.recent_fresh_result is not None
                else None
            ),
        }

    @classmethod
    def from_dict(cls, raw: Mapping[str, object]) -> "BatchManifestItem":
        required = {
            "source_id",
            "source_path_snapshot",
            "shelf",
            "sort",
            "state",
        }
        if not required.issubset(set(raw)) or set(raw) - required - {"recent_fresh_result"}:
            raise BatchManifestValidationError("invalid batch manifest item")
        raw_fresh = raw.get("recent_fresh_result")
        if raw_fresh is not None and not isinstance(raw_fresh, Mapping):
            raise BatchManifestValidationError(
                "recent_fresh_result must be an object or null"
            )
        return cls(
            source_id=raw["source_id"],  # type: ignore[arg-type]
            source_path_snapshot=raw["source_path_snapshot"],  # type: ignore[arg-type]
            shelf=raw["shelf"],  # type: ignore[arg-type]
            sort=raw["sort"],  # type: ignore[arg-type]
            state=raw["state"],  # type: ignore[arg-type]
            recent_fresh_result=(
                FreshResult.from_dict(raw_fresh) if raw_fresh is not None else None
            ),
        )


_ALLOWED_TRANSITIONS: Final[dict[str, frozenset[str]]] = {
    "scheduled": frozenset({
        "scheduled",
        "inspecting",
        "needs_attention",
        "skipped_currently_nonmedia",
        "technical_failure",
    }),
    "inspecting": frozenset({
        "scheduled",
        "inspecting",
        "ready",
        "needs_attention",
        "skipped_currently_nonmedia",
        "technical_failure",
    }),
    "ready": frozenset({
        "inspecting",
        "ready",
        "active",
        "needs_attention",
        "technical_failure",
    }),
    "active": frozenset({
        "active",
        "completed",
        "needs_attention",
        "technical_failure",
    }),
    "completed": frozenset({"completed"}),
    "needs_attention": frozenset({
        "scheduled",
        "inspecting",
        "needs_attention",
    }),
    "skipped_currently_nonmedia": frozenset({
        "inspecting",
        "skipped_currently_nonmedia",
    }),
    "technical_failure": frozenset({
        "scheduled",
        "inspecting",
        "technical_failure",
    }),
}


def transition_batch_item(
    item: BatchManifestItem,
    state: str,
    *,
    shelf: object = _UNSET,
    recent_fresh_result: object = _UNSET,
) -> BatchManifestItem:
    """Return a validated lifecycle transition for one immutable queue item.

    Re-entering ``scheduled`` or ``inspecting`` intentionally clears any old
    fresh result unless a caller explicitly supplies a replacement; both
    states are pre-fresh and must not promote historic evidence.
    """
    if not isinstance(item, BatchManifestItem):
        raise TypeError("item must be a BatchManifestItem")
    if not isinstance(state, str) or state not in BATCH_ITEM_STATES:
        raise BatchManifestTransitionError(f"unsupported batch state: {state!r}")
    if state not in _ALLOWED_TRANSITIONS[item.state]:
        raise BatchManifestTransitionError(
            f"cannot transition batch item from {item.state!r} to {state!r}"
        )
    next_shelf = item.shelf if shelf is _UNSET else shelf
    next_fresh = (
        item.recent_fresh_result
        if recent_fresh_result is _UNSET
        else recent_fresh_result
    )
    if state in {"scheduled", "inspecting"} and recent_fresh_result is _UNSET:
        next_fresh = None
    return replace(
        item,
        shelf=next_shelf,  # type: ignore[arg-type]
        state=state,
        recent_fresh_result=next_fresh,  # type: ignore[arg-type]
    )


@dataclass(frozen=True, slots=True)
class BatchManifest:
    """The complete lightweight queue, persisted in one atomic JSON file."""

    items: tuple[BatchManifestItem, ...] = ()
    version: int = BATCH_MANIFEST_VERSION

    def __post_init__(self) -> None:
        if self.version != BATCH_MANIFEST_VERSION:
            raise BatchManifestValidationError(
                f"unsupported batch manifest version: {self.version!r}"
            )
        if not isinstance(self.items, tuple) or any(
            not isinstance(item, BatchManifestItem) for item in self.items
        ):
            raise BatchManifestValidationError(
                "items must be a tuple of BatchManifestItem"
            )
        if len(self.items) > MAX_BATCH_ITEMS:
            raise BatchManifestValidationError("batch manifest exceeds the item limit")
        source_ids = [item.source_id for item in self.items]
        paths = [item.source_path_snapshot for item in self.items]
        sorts = [item.sort for item in self.items]
        if len(source_ids) != len(set(source_ids)):
            raise BatchManifestValidationError("batch manifest has duplicate source_id values")
        if len(paths) != len(set(paths)):
            raise BatchManifestValidationError("batch manifest has duplicate source paths")
        if len(sorts) != len(set(sorts)):
            raise BatchManifestValidationError("batch manifest has duplicate sort values")
        if sum(item.state == "active" for item in self.items) > 1:
            raise BatchManifestValidationError("only one batch item may be active")

    @classmethod
    def empty(cls) -> "BatchManifest":
        return cls()

    def ordered_items(self) -> tuple[BatchManifestItem, ...]:
        return tuple(sorted(self.items, key=lambda item: item.sort))

    def find(self, source_id: str) -> BatchManifestItem | None:
        return next((item for item in self.items if item.source_id == source_id), None)

    def active_item(self) -> BatchManifestItem | None:
        """Return the sole active item, if one exists."""
        return next((item for item in self.items if item.state == "active"), None)

    def next_ready(self) -> BatchManifestItem | None:
        """Return the first fresh-ready item in authorization order.

        Inspection and retry policy remain the coordinator's concern; this
        pure selector never promotes a scheduled or attention item merely
        because it appears earlier in the queue.
        """
        return next(
            (item for item in self.ordered_items() if item.state == "ready"),
            None,
        )

    def add(self, item: BatchManifestItem) -> "BatchManifest":
        if not isinstance(item, BatchManifestItem):
            raise TypeError("item must be a BatchManifestItem")
        if self.find(item.source_id) is not None:
            raise BatchManifestValidationError(
                f"batch manifest already contains source_id={item.source_id!r}"
            )
        return BatchManifest(items=self.items + (item,))

    def replace(self, item: BatchManifestItem) -> "BatchManifest":
        if not isinstance(item, BatchManifestItem):
            raise TypeError("item must be a BatchManifestItem")
        previous = self.find(item.source_id)
        if previous is None:
            raise BatchManifestValidationError(
                f"batch manifest does not contain source_id={item.source_id!r}"
            )
        if previous.source_path_snapshot != item.source_path_snapshot:
            raise BatchManifestValidationError("a batch item's source path snapshot is immutable")
        if previous.sort != item.sort:
            raise BatchManifestValidationError("use a new manifest to change batch ordering")
        return BatchManifest(
            items=tuple(
                item if current.source_id == item.source_id else current
                for current in self.items
            )
        )

    def transition(
        self,
        source_id: str,
        state: str,
        *,
        shelf: object = _UNSET,
        recent_fresh_result: object = _UNSET,
    ) -> "BatchManifest":
        item = self.find(source_id)
        if item is None:
            raise BatchManifestValidationError(
                f"batch manifest does not contain source_id={source_id!r}"
            )
        return self.replace(
            transition_batch_item(
                item,
                state,
                shelf=shelf,
                recent_fresh_result=recent_fresh_result,
            )
        )

    def as_dict(self) -> dict[str, object]:
        return {
            "version": self.version,
            "items": [item.as_dict() for item in self.ordered_items()],
        }

    @classmethod
    def from_dict(cls, raw: Mapping[str, object]) -> "BatchManifest":
        if set(raw) != {"version", "items"}:
            raise BatchManifestValidationError("invalid batch manifest record")
        version = raw["version"]
        raw_items = raw["items"]
        if isinstance(version, bool) or not isinstance(version, int):
            raise BatchManifestValidationError("batch manifest version must be an integer")
        if not isinstance(raw_items, list):
            raise BatchManifestValidationError("batch manifest items must be an array")
        items: list[BatchManifestItem] = []
        for raw_item in raw_items:
            if not isinstance(raw_item, Mapping):
                raise BatchManifestValidationError("batch manifest item must be an object")
            items.append(BatchManifestItem.from_dict(raw_item))
        return cls(items=tuple(items), version=version)


def batch_manifest_path(state_dir: Path) -> Path:
    """Return the only local file used by this passive batch queue."""
    return Path(state_dir) / BATCH_MANIFEST_FILENAME


def load_batch_manifest(state_dir: Path) -> BatchManifest:
    """Load one manifest, failing closed if an existing file is malformed."""
    path = batch_manifest_path(state_dir)
    try:
        if path.is_symlink() or (path.exists() and not path.is_file()):
            raise BatchManifestValidationError("batch manifest path is not a regular file")
        raw = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return BatchManifest.empty()
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise BatchManifestValidationError("cannot safely read batch manifest") from exc
    if not isinstance(raw, Mapping):
        raise BatchManifestValidationError("batch manifest root must be an object")
    return BatchManifest.from_dict(raw)


def save_batch_manifest(state_dir: Path, manifest: BatchManifest) -> None:
    """Durably publish a fully validated manifest with write-then-rename."""
    if not isinstance(manifest, BatchManifest):
        raise TypeError("manifest must be a BatchManifest")
    state_path = Path(state_dir)
    if state_path.is_symlink() or (state_path.exists() and not state_path.is_dir()):
        raise BatchManifestValidationError("batch state root is not a regular directory")
    state_path.mkdir(parents=True, exist_ok=True)
    # Reconstruct through the external schema before writing.  This prevents a
    # forged frozen dataclass (for example via object.__setattr__) from being
    # persisted without all cross-item invariants being checked again.
    payload = BatchManifest.from_dict(manifest.as_dict()).as_dict()
    atomic_write_json(batch_manifest_path(state_dir), payload, allow_nan=False)


def add_batch_item(manifest: BatchManifest, item: BatchManifestItem) -> BatchManifest:
    """Functional alias for ``manifest.add(item)`` for simple coordinators."""
    if not isinstance(manifest, BatchManifest):
        raise TypeError("manifest must be a BatchManifest")
    return manifest.add(item)


def transition_batch_manifest_item(
    manifest: BatchManifest,
    source_id: str,
    state: str,
    *,
    shelf: object = _UNSET,
    recent_fresh_result: object = _UNSET,
) -> BatchManifest:
    """Functional alias for the manifest's constrained transition method."""
    if not isinstance(manifest, BatchManifest):
        raise TypeError("manifest must be a BatchManifest")
    return manifest.transition(
        source_id,
        state,
        shelf=shelf,
        recent_fresh_result=recent_fresh_result,
    )


__all__ = [
    "BATCH_ITEM_STATES",
    "BATCH_MANIFEST_FILENAME",
    "BATCH_MANIFEST_VERSION",
    "MAX_BATCH_ITEMS",
    "TARGET_SHELVES",
    "BatchManifest",
    "BatchManifestError",
    "BatchManifestItem",
    "BatchManifestTransitionError",
    "BatchManifestValidationError",
    "FreshResult",
    "add_batch_item",
    "batch_manifest_path",
    "load_batch_manifest",
    "save_batch_manifest",
    "transition_batch_item",
    "transition_batch_manifest_item",
]
