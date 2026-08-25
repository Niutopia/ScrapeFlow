"""Fail-closed domain records for explicitly authorized media replacement.

Replacement is an execution *mode* layered on top of the ordinary planner and
writer; it is not a second writer and it is not a sixth reconciliation result.
This module therefore only records the server-derived proof needed by that
existing chain.  It performs no AList I/O and never accepts an archive path
from an external caller: :func:`build_replacement_manifest` derives the task
archive below the configured library root.

The deliberately narrow model is useful even when a deployment has not yet
enabled the replacement endpoint.  A fresh source/target listing can be
validated and persisted, and an interrupted run can decide whether it is safe
to resume the same manifest or must stop for attention.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
import math
import json
from pathlib import Path, PurePosixPath
import posixpath
import re
import unicodedata
from typing import Final

from .serialization import atomic_write_json


REPLACEMENT_MANIFEST_VERSION: Final = 1
REPLACEMENT_STATES: Final = frozenset({
    "prepared",
    "archiving",
    "archived",
    "writing",
    "completed",
    "needs_attention",
    "technical_failure",
})
REPLACEMENT_OBJECT_KINDS: Final = frozenset({
    "video", "subtitle", "archive", "disc_image", "other", "directory",
})
_REPLACEMENT_CONSUMABLE_KINDS: Final = frozenset({"video", "subtitle"})
_SAFE_ID = re.compile(r"\A[A-Za-z0-9][A-Za-z0-9._:-]{0,127}\Z")
_COORDINATE = re.compile(r"\AS(?P<season>\d{1,3})E(?P<episode>\d{1,4})\Z", re.IGNORECASE)
_UTC_SECOND = "%Y-%m-%dT%H:%M:%SZ"
_ARCHIVE_MARKER = ".scrapeflow-archive"
_UNSET = object()


class ReplacementError(ValueError):
    """Base error for malformed or unsafe replacement evidence."""


class ReplacementValidationError(ReplacementError):
    """A replacement mapping or persisted manifest is not safe to execute."""


class ReplacementTransitionError(ReplacementError):
    """A replacement state transition is not part of the lifecycle."""


def _text(value: object, field_name: str, *, maximum: int = 512) -> str:
    if not isinstance(value, str) or not value or len(value) > maximum:
        raise ReplacementValidationError(f"{field_name} must be a non-empty string")
    if any(ord(character) < 32 for character in value):
        raise ReplacementValidationError(f"{field_name} contains a control character")
    return value


def _safe_id(value: object, field_name: str) -> str:
    text = _text(value, field_name, maximum=128)
    if _SAFE_ID.fullmatch(text) is None:
        raise ReplacementValidationError(f"{field_name} has an unsafe format")
    return text


def _strict_path(value: object, field_name: str, *, allow_root: bool = False) -> str:
    path = _text(value, field_name, maximum=4096)
    if not path.startswith("/") or "\\" in path:
        raise ReplacementValidationError(f"{field_name} must be an absolute POSIX path")
    if any(unicodedata.category(character) in {"Cc", "Cf", "Cs"} for character in path):
        raise ReplacementValidationError(f"{field_name} contains an invisible character")
    if path == "/":
        if allow_root:
            return path
        raise ReplacementValidationError(f"{field_name} cannot be the root")
    if path.endswith("/") or posixpath.normpath(path) != path:
        raise ReplacementValidationError(f"{field_name} is not canonical")
    parts = path.split("/")[1:]
    if any(not part or part in {".", ".."} for part in parts):
        raise ReplacementValidationError(f"{field_name} contains an unsafe segment")
    return path


def _collision_key(path: str) -> str:
    return "/".join(unicodedata.normalize("NFC", part).casefold() for part in path.split("/"))


def _within(path: str, root: str) -> bool:
    path_key = _collision_key(path.rstrip("/") or "/")
    root_key = _collision_key(root.rstrip("/") or "/")
    return root_key == "/" or path_key == root_key or path_key.startswith(root_key.rstrip("/") + "/")


def _overlap(left: str, right: str) -> bool:
    return _within(left, right) or _within(right, left)


def _positive_size(value: object, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ReplacementValidationError(f"{field_name} must be a positive integer")
    return value


def normalize_coordinate(value: object, field_name: str = "coordinate") -> str:
    """Normalize an explicit ``SxxEyy`` coordinate without inventing one."""
    text = _text(value, field_name, maximum=32).upper()
    match = _COORDINATE.fullmatch(text)
    if match is None:
        raise ReplacementValidationError(f"{field_name} must be an SxxEyy coordinate")
    return f"S{int(match.group('season')):02d}E{int(match.group('episode')):02d}"


def _timestamp(value: object, field_name: str, *, default: str | None = None) -> str:
    if value is None and default is not None:
        return default
    text = _text(value, field_name, maximum=20)
    try:
        datetime.strptime(text, _UTC_SECOND)
    except ValueError as exc:
        raise ReplacementValidationError(f"{field_name} must be a UTC whole-second timestamp") from exc
    return text


def _now() -> str:
    return datetime.now(UTC).strftime(_UTC_SECOND)


def _kind(value: object) -> str:
    text = _text(value, "object kind", maximum=32).casefold()
    if text not in REPLACEMENT_OBJECT_KINDS:
        raise ReplacementValidationError("object kind is unsupported")
    return text


def _language(value: object) -> str | None:
    if value is None:
        return None
    text = _text(value, "subtitle language", maximum=64).casefold().replace("_", "-")
    return text


def _opaque_metadata(value: object, field_name: str) -> str | None:
    """Keep optional provider revision metadata losslessly and safely.

    AList and the source-inventory bridge expose revision values in a few
    primitive shapes (usually strings, occasionally numeric timestamps).  We
    normalize those primitives to their textual spelling, while rejecting
    booleans, containers and non-finite floats.  ``None`` means the provider
    did not declare that piece of metadata; it is intentionally distinct from
    a fabricated empty marker.
    """
    if value is None or value == "":
        return None
    if isinstance(value, bool) or not isinstance(value, (str, int, float)):
        raise ReplacementValidationError(f"{field_name} must be a primitive metadata value")
    if isinstance(value, float) and not math.isfinite(value):
        raise ReplacementValidationError(f"{field_name} must be finite")
    return _text(str(value), field_name, maximum=256)


def _is_simplified(language: str | None, path: str) -> bool:
    marker = (language or "").casefold().replace("_", "-")
    if marker in {"sc", "zh", "zh-cn", "zh-hans", "simplified", "simplified-chinese"}:
        return True
    if marker in {"tc", "zh-tw", "zh-hant", "traditional", "traditional-chinese"}:
        return False
    lowered = path.casefold()
    return any(token in lowered for token in (".sc.", ".zh-cn.", ".zh-hans.", "简", "简体"))


@dataclass(frozen=True, slots=True)
class ReplacementObject:
    """One fresh exact source or formal-library object."""

    path: str
    size: int
    kind: str
    # A coordinate is meaningful only for a media object.  Fresh source
    # listings also contain artwork, menus, release notes and archive
    # members; requiring an invented episode coordinate for those residual
    # objects would either discard ownership evidence or encourage callers to
    # fabricate one.  Video/subtitle objects remain strictly coordinate-bound.
    coordinate: str | None = None
    language: str | None = None
    snapshot_id: str | None = None
    # Optional provider revision fields.  They are deliberately opaque: when
    # a manifest declares one, a fresh listing must reproduce it byte-for-byte
    # (after primitive-to-text normalization), but listings that never expose
    # the field remain valid for legacy/source-only callers.
    version: str | int | float | None = None
    modified: str | int | float | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "path", _strict_path(self.path, "object path"))
        kind = _kind(self.kind)
        if isinstance(self.size, bool) or not isinstance(self.size, int) or self.size < 0:
            raise ReplacementValidationError("object size must be a non-negative integer")
        if kind in _REPLACEMENT_CONSUMABLE_KINDS and self.size <= 0:
            raise ReplacementValidationError("media object size must be positive")
        object.__setattr__(self, "size", self.size)
        object.__setattr__(self, "kind", kind)
        if self.coordinate is None:
            if kind in _REPLACEMENT_CONSUMABLE_KINDS:
                raise ReplacementValidationError("media object requires an explicit coordinate")
        else:
            object.__setattr__(self, "coordinate", normalize_coordinate(self.coordinate))
        object.__setattr__(self, "language", _language(self.language))
        if self.snapshot_id is not None:
            object.__setattr__(self, "snapshot_id", _text(self.snapshot_id, "object snapshot_id", maximum=256))
        object.__setattr__(self, "version", _opaque_metadata(self.version, "object version"))
        object.__setattr__(self, "modified", _opaque_metadata(self.modified, "object modified"))

    @property
    def collision_key(self) -> str:
        return _collision_key(self.path)

    @classmethod
    def from_dict(cls, raw: Mapping[str, object]) -> "ReplacementObject":
        if not isinstance(raw, Mapping):
            raise ReplacementValidationError("replacement object must be an object")
        # Link targets are never stable media ownership evidence.  Do not
        # silently flatten a remote listing's link flag into an ordinary file
        # just because its name and byte count look plausible.
        if any(raw.get(flag) is True for flag in ("is_link", "is_symlink", "symlink")):
            raise ReplacementValidationError("replacement object cannot be a link")
        path = raw.get("path", raw.get("full_path"))
        if path is None:
            path = raw.get("source_path", raw.get("target_path"))
        kind = raw.get("kind", raw.get("object_type", raw.get("media_kind")))
        coordinate = raw.get("coordinate", raw.get("source_coordinate", raw.get("target_coordinate")))
        size = raw.get("size")
        if size is None:
            size = raw.get("source_size", raw.get("target_size"))
        return cls(
            path=path,  # type: ignore[arg-type]
            size=size,  # type: ignore[arg-type]
            kind=kind,  # type: ignore[arg-type]
            coordinate=coordinate,  # type: ignore[arg-type]
            language=raw.get("language"),  # type: ignore[arg-type]
            snapshot_id=raw.get("snapshot_id"),  # type: ignore[arg-type]
            version=raw.get("version"),  # type: ignore[arg-type]
            modified=raw.get("modified", raw.get("updated_at", raw.get("mtime", raw.get("last_modified")))),  # type: ignore[arg-type]
        )

    def as_dict(self) -> dict[str, object]:
        return {
            "path": self.path,
            "size": self.size,
            "kind": self.kind,
            "coordinate": self.coordinate,
            "language": self.language,
            "snapshot_id": self.snapshot_id,
            "version": self.version,
            "modified": self.modified,
        }


# Descriptive aliases make the source/target role obvious to callers while
# keeping one exact object schema and one validator.
ReplacementSourceObject = ReplacementObject
ReplacementTargetObject = ReplacementObject


@dataclass(frozen=True, slots=True)
class ReplacementItem:
    """One explicit source-coordinate → target-coordinate replacement."""

    source_coordinate: str
    target_coordinate: str
    source_path: str
    source_size: int
    target_path: str
    target_size: int
    subtitle_source_path: str | None = None
    subtitle_source_size: int | None = None
    subtitle_target_path: str | None = None
    subtitle_target_size: int | None = None
    subtitle_language: str | None = "zh-CN"

    def __post_init__(self) -> None:
        object.__setattr__(self, "source_coordinate", normalize_coordinate(self.source_coordinate, "source_coordinate"))
        object.__setattr__(self, "target_coordinate", normalize_coordinate(self.target_coordinate, "target_coordinate"))
        object.__setattr__(self, "source_path", _strict_path(self.source_path, "source_path"))
        object.__setattr__(self, "target_path", _strict_path(self.target_path, "target_path"))
        object.__setattr__(self, "source_size", _positive_size(self.source_size, "source_size"))
        object.__setattr__(self, "target_size", _positive_size(self.target_size, "target_size"))
        if _collision_key(self.source_path) == _collision_key(self.target_path):
            raise ReplacementValidationError("source and target paths must be distinct")
        if self.subtitle_source_path is None:
            if self.subtitle_source_size is not None:
                raise ReplacementValidationError("subtitle_source_size requires a subtitle source")
            if self.subtitle_target_path is not None:
                object.__setattr__(self, "subtitle_target_path", _strict_path(self.subtitle_target_path, "subtitle_target_path"))
                object.__setattr__(self, "subtitle_target_size", _positive_size(self.subtitle_target_size, "subtitle_target_size"))
            elif self.subtitle_target_size is not None:
                raise ReplacementValidationError("subtitle_target_size requires a subtitle target")
        else:
            object.__setattr__(self, "subtitle_source_path", _strict_path(self.subtitle_source_path, "subtitle_source_path"))
            object.__setattr__(self, "subtitle_source_size", _positive_size(self.subtitle_source_size, "subtitle_source_size"))
            object.__setattr__(self, "subtitle_language", _language(self.subtitle_language) or "zh-cn")
            if not _is_simplified(self.subtitle_language, self.subtitle_source_path):
                raise ReplacementValidationError("replacement subtitles must be the selected simplified track")
            if self.subtitle_target_path is not None:
                object.__setattr__(self, "subtitle_target_path", _strict_path(self.subtitle_target_path, "subtitle_target_path"))
                object.__setattr__(self, "subtitle_target_size", _positive_size(self.subtitle_target_size, "subtitle_target_size"))
            elif self.subtitle_target_size is not None:
                raise ReplacementValidationError("subtitle_target_size requires a subtitle target")
            selected_paths = {_collision_key(self.source_path), _collision_key(self.target_path)}
            if _collision_key(self.subtitle_source_path) in selected_paths:
                raise ReplacementValidationError("subtitle source collides with replacement video")
            if self.subtitle_target_path is not None and _collision_key(self.subtitle_target_path) in selected_paths:
                raise ReplacementValidationError("subtitle target collides with replacement video")
        if self.subtitle_target_path is not None and _collision_key(self.subtitle_target_path) in {
            _collision_key(self.source_path), _collision_key(self.target_path)
        }:
            raise ReplacementValidationError("subtitle target collides with replacement video")

    # Compatibility/readability aliases used by planner-facing callers.
    @property
    def old_target_path(self) -> str:
        return self.target_path

    @property
    def new_source_path(self) -> str:
        return self.source_path

    @property
    def old_target_size(self) -> int:
        return self.target_size

    @property
    def new_source_size(self) -> int:
        return self.source_size

    def as_dict(self) -> dict[str, object]:
        return {
            "source_coordinate": self.source_coordinate,
            "target_coordinate": self.target_coordinate,
            "source_path": self.source_path,
            "source_size": self.source_size,
            "target_path": self.target_path,
            "target_size": self.target_size,
            "subtitle_source_path": self.subtitle_source_path,
            "subtitle_source_size": self.subtitle_source_size,
            "subtitle_target_path": self.subtitle_target_path,
            "subtitle_target_size": self.subtitle_target_size,
            "subtitle_language": self.subtitle_language,
        }

    @classmethod
    def from_dict(cls, raw: Mapping[str, object]) -> "ReplacementItem":
        if not isinstance(raw, Mapping):
            raise ReplacementValidationError("replacement item must be an object")
        required = {
            "source_coordinate", "target_coordinate", "source_path", "source_size",
            "target_path", "target_size", "subtitle_source_path", "subtitle_source_size",
            "subtitle_target_path", "subtitle_target_size", "subtitle_language",
        }
        if set(raw) != required:
            raise ReplacementValidationError("replacement item schema is invalid")
        return cls(**{key: raw[key] for key in required})  # type: ignore[arg-type]


@dataclass(frozen=True, slots=True)
class ReplacementManifest:
    """The immutable server-generated proof for one replacement run."""

    manifest_id: str
    root_job_id: str
    work_unit_id: str
    tmdb_id: int
    media_type: str
    source_root: str
    target_work_root: str
    # Keep the configured formal-library root in the durable proof.  Merely
    # checking for the archive marker is insufficient: a forged sibling path
    # could otherwise pass validation and make the old target disappear into
    # an arbitrary tree.
    library_root: str
    archive_root: str
    source_snapshot_id: str
    items: tuple[ReplacementItem, ...]
    residual_paths: tuple[str, ...] = ()
    # Exact residual ownership evidence.  ``residual_paths`` remains in the
    # public schema for compatibility with early manifests, while new builds
    # persist the full object tuple (kind, size, snapshot and optional
    # provider revision metadata).  An empty tuple with non-empty paths is
    # therefore a loadable legacy projection, but is not execution-ready.
    residual_objects: tuple[ReplacementObject, ...] = ()
    authorized: bool = True
    state: str = "prepared"
    error: str | None = None
    created_at: str = field(default_factory=_now)
    updated_at: str = field(default_factory=_now)
    version: int = REPLACEMENT_MANIFEST_VERSION
    _residual_metadata_declared: bool = field(default=True, init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "manifest_id", _safe_id(self.manifest_id, "manifest_id"))
        object.__setattr__(self, "root_job_id", _safe_id(self.root_job_id, "root_job_id"))
        object.__setattr__(self, "work_unit_id", _safe_id(self.work_unit_id, "work_unit_id"))
        if isinstance(self.tmdb_id, bool) or not isinstance(self.tmdb_id, int) or self.tmdb_id <= 0:
            raise ReplacementValidationError("tmdb_id must be a positive integer")
        media_type = _text(self.media_type, "media_type", maximum=16).casefold()
        if media_type not in {"tv", "movie"}:
            raise ReplacementValidationError("replacement requires a confirmed movie or tv identity")
        object.__setattr__(self, "media_type", media_type)
        source_root = _strict_path(self.source_root, "source_root")
        target_root = _strict_path(self.target_work_root, "target_work_root")
        library_root = _strict_path(self.library_root, "library_root")
        archive_root = _strict_path(self.archive_root, "archive_root")
        if _overlap(source_root, target_root) or _overlap(source_root, archive_root) or _overlap(target_root, archive_root):
            raise ReplacementValidationError("source, target and archive roots must not overlap")
        expected_archive_root = derive_replacement_archive_root(
            library_root,
            target_root,
            self.root_job_id,
        )
        if _collision_key(archive_root) != _collision_key(expected_archive_root):
            raise ReplacementValidationError("archive_root must be the exact derived ScrapeFlow archive path")
        snapshot = _text(self.source_snapshot_id, "source_snapshot_id", maximum=256)
        object.__setattr__(self, "source_root", source_root)
        object.__setattr__(self, "target_work_root", target_root)
        object.__setattr__(self, "library_root", library_root)
        object.__setattr__(self, "archive_root", archive_root)
        object.__setattr__(self, "source_snapshot_id", snapshot)
        if type(self.authorized) is not bool or not self.authorized:
            raise ReplacementValidationError("replacement requires explicit authorization")
        if self.version != REPLACEMENT_MANIFEST_VERSION:
            raise ReplacementValidationError("unsupported replacement manifest version")
        if self.state not in REPLACEMENT_STATES:
            raise ReplacementValidationError(f"unsupported replacement state: {self.state!r}")
        raw_items = tuple(self.items)
        if not raw_items or len(raw_items) > 256 or any(not isinstance(item, ReplacementItem) for item in raw_items):
            raise ReplacementValidationError("replacement manifest requires bounded item records")
        source_coords = [item.source_coordinate for item in raw_items]
        target_coords = [item.target_coordinate for item in raw_items]
        paths = [item.source_path for item in raw_items] + [item.target_path for item in raw_items]
        if len(source_coords) != len(set(source_coords)) or len(target_coords) != len(set(target_coords)):
            raise ReplacementValidationError("replacement coordinates must be unique")
        subtitle_paths = [
            path
            for item in raw_items
            for path in (item.subtitle_source_path, item.subtitle_target_path)
            if path is not None
        ]
        collision_paths = [_collision_key(path) for path in paths + subtitle_paths]
        if len(collision_paths) != len(set(collision_paths)):
            raise ReplacementValidationError("replacement video paths collide")
        for item in raw_items:
            if not _within(item.source_path, source_root) or not _within(item.target_path, target_root):
                raise ReplacementValidationError("replacement object lies outside its declared root")
            if item.subtitle_source_path is not None and not _within(item.subtitle_source_path, source_root):
                raise ReplacementValidationError("selected subtitle lies outside source root")
            if item.subtitle_target_path is not None and not _within(item.subtitle_target_path, target_root):
                raise ReplacementValidationError("selected subtitle target lies outside work root")
        residual = tuple(_strict_path(path, "residual path") for path in self.residual_paths)
        if any(not _within(path, source_root) for path in residual):
            raise ReplacementValidationError("residual path lies outside source root")
        residual_keys = [_collision_key(path) for path in residual]
        if len(residual_keys) != len(set(residual_keys)):
            raise ReplacementValidationError("residual paths collide")
        selected_keys = {
            _collision_key(path)
            for item in raw_items
            for path in (item.source_path, item.subtitle_source_path)
            if path is not None
        }
        if selected_keys.intersection(residual_keys):
            raise ReplacementValidationError("selected replacement object cannot be residual")
        residual_objects = tuple(self.residual_objects)
        if any(not isinstance(obj, ReplacementObject) for obj in residual_objects):
            raise ReplacementValidationError("residual_objects must contain exact ReplacementObject records")
        residual_object_keys = [_collision_key(obj.path) for obj in residual_objects]
        if len(residual_object_keys) != len(set(residual_object_keys)):
            raise ReplacementValidationError("residual objects collide")
        if any(not _within(obj.path, source_root) for obj in residual_objects):
            raise ReplacementValidationError("residual object lies outside source root")
        if any(obj.snapshot_id is not None and obj.snapshot_id != snapshot for obj in residual_objects):
            raise ReplacementValidationError("residual object snapshot does not match the manifest snapshot")
        if selected_keys.intersection(residual_object_keys):
            raise ReplacementValidationError("selected replacement object cannot be residual")
        # New manifests must make the residual object projection exact.  A
        # legacy manifest loaded without ``residual_objects`` is intentionally
        # allowed through with path-only evidence; ``residual_metadata_complete``
        # exposes that it cannot yet be used as execution authority.
        if residual_objects and set(residual_object_keys) != set(residual_keys):
            raise ReplacementValidationError(
                "residual_objects must exactly cover residual_paths"
            )
        object.__setattr__(self, "items", tuple(sorted(raw_items, key=lambda row: row.target_coordinate)))
        object.__setattr__(self, "residual_paths", tuple(sorted(residual)))
        object.__setattr__(
            self,
            "residual_objects",
            tuple(sorted(residual_objects, key=lambda row: row.collision_key)),
        )
        object.__setattr__(self, "error", None if self.error is None else _text(self.error, "error", maximum=1024))
        object.__setattr__(self, "created_at", _timestamp(self.created_at, "created_at"))
        object.__setattr__(self, "updated_at", _timestamp(self.updated_at, "updated_at"))

    @property
    def target_coordinates(self) -> tuple[str, ...]:
        return tuple(item.target_coordinate for item in self.items)

    @property
    def target_root(self) -> str:
        return self.target_work_root

    @property
    def work_root(self) -> str:
        return self.target_work_root

    @property
    def source_snapshot(self) -> str:
        return self.source_snapshot_id

    @property
    def residual_metadata_complete(self) -> bool:
        """Whether every residual path has an exact persisted object record.

        Old manifests can still be read and inspected, but a path-only
        residual projection is not enough to prove that a fresh object kept
        its type/size/revision.  Empty residual sets are complete by
        definition.
        """
        if not self.residual_paths:
            return True
        if not self.residual_objects:
            return False
        return {
            _collision_key(path) for path in self.residual_paths
        } == {
            _collision_key(obj.path) for obj in self.residual_objects
        }

    def transition(self, state: str, *, error: object = _UNSET) -> "ReplacementManifest":
        return transition_replacement_manifest(self, state, error=error)

    @property
    def source_coordinates(self) -> tuple[str, ...]:
        return tuple(item.source_coordinate for item in self.items)

    def item_for_target(self, coordinate: object) -> ReplacementItem | None:
        normalized = normalize_coordinate(coordinate)
        return next((item for item in self.items if item.target_coordinate == normalized), None)

    def as_dict(self) -> dict[str, object]:
        data: dict[str, object] = {
            "version": self.version,
            "manifest_id": self.manifest_id,
            "root_job_id": self.root_job_id,
            "work_unit_id": self.work_unit_id,
            "tmdb_id": self.tmdb_id,
            "media_type": self.media_type,
            "source_root": self.source_root,
            "target_work_root": self.target_work_root,
            "library_root": self.library_root,
            "archive_root": self.archive_root,
            "source_snapshot_id": self.source_snapshot_id,
            "items": [item.as_dict() for item in self.items],
            "residual_paths": list(self.residual_paths),
            "authorized": self.authorized,
            "state": self.state,
            "error": self.error,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        }
        # Keep old, path-only manifests serializable in their original shape;
        # fresh manifests always have this key (including an empty list when
        # there are no residual objects).
        if self._residual_metadata_declared or self.residual_objects:
            data["residual_objects"] = [obj.as_dict() for obj in self.residual_objects]
        return data

    @classmethod
    def from_dict(cls, raw: Mapping[str, object]) -> "ReplacementManifest":
        if not isinstance(raw, Mapping):
            raise ReplacementValidationError("replacement manifest must be an object")
        required = {
            "version", "manifest_id", "root_job_id", "work_unit_id", "tmdb_id", "media_type",
            "source_root", "target_work_root", "library_root", "archive_root", "source_snapshot_id", "items",
            "residual_paths", "authorized", "state", "error", "created_at", "updated_at",
        }
        optional = {"residual_objects"}
        if not required.issubset(raw) or set(raw) - required - optional:
            raise ReplacementValidationError("replacement manifest schema is invalid")
        if not isinstance(raw.get("items"), list) or not isinstance(raw.get("residual_paths"), list):
            raise ReplacementValidationError("replacement manifest schema is invalid")
        if any(not isinstance(row, Mapping) for row in raw["items"]):
            raise ReplacementValidationError("replacement manifest contains an invalid item")
        has_residual_objects = "residual_objects" in raw
        if has_residual_objects and not isinstance(raw.get("residual_objects"), list):
            raise ReplacementValidationError("replacement manifest residual_objects must be a list")
        if has_residual_objects and any(not isinstance(row, Mapping) for row in raw["residual_objects"]):
            raise ReplacementValidationError("replacement manifest contains an invalid residual object")
        manifest = cls(
            version=raw["version"],  # type: ignore[arg-type]
            manifest_id=raw["manifest_id"],  # type: ignore[arg-type]
            root_job_id=raw["root_job_id"],  # type: ignore[arg-type]
            work_unit_id=raw["work_unit_id"],  # type: ignore[arg-type]
            tmdb_id=raw["tmdb_id"],  # type: ignore[arg-type]
            media_type=raw["media_type"],  # type: ignore[arg-type]
            source_root=raw["source_root"],  # type: ignore[arg-type]
            target_work_root=raw["target_work_root"],  # type: ignore[arg-type]
            library_root=raw["library_root"],  # type: ignore[arg-type]
            archive_root=raw["archive_root"],  # type: ignore[arg-type]
            source_snapshot_id=raw["source_snapshot_id"],  # type: ignore[arg-type]
            items=tuple(ReplacementItem.from_dict(row) for row in raw["items"] if isinstance(row, Mapping)),
            residual_paths=tuple(raw["residual_paths"]),  # type: ignore[arg-type]
            residual_objects=(
                tuple(ReplacementObject.from_dict(row) for row in raw["residual_objects"] if isinstance(row, Mapping))
                if has_residual_objects else ()
            ),
            authorized=raw["authorized"],  # type: ignore[arg-type]
            state=raw["state"],  # type: ignore[arg-type]
            error=raw["error"],  # type: ignore[arg-type]
            created_at=raw["created_at"],  # type: ignore[arg-type]
            updated_at=raw["updated_at"],  # type: ignore[arg-type]
        )
        # A present residual_objects key is the new exact schema.  Reject an
        # explicitly empty projection when residual paths exist; an absent key
        # is the supported legacy form and remains path-only by design.
        if has_residual_objects and manifest.residual_paths and not manifest.residual_objects:
            raise ReplacementValidationError(
                "replacement manifest residual_objects must cover residual_paths"
            )
        object.__setattr__(manifest, "_residual_metadata_declared", has_residual_objects)
        return manifest


_ALLOWED_TRANSITIONS: Final[dict[str, frozenset[str]]] = {
    "prepared": frozenset({"prepared", "archiving", "needs_attention", "technical_failure"}),
    "archiving": frozenset({"archiving", "archived", "needs_attention", "technical_failure"}),
    "archived": frozenset({"archived", "writing", "needs_attention", "technical_failure"}),
    "writing": frozenset({"writing", "completed", "needs_attention", "technical_failure"}),
    "completed": frozenset({"completed"}),
    "needs_attention": frozenset({"needs_attention", "archiving", "technical_failure"}),
    "technical_failure": frozenset({"technical_failure", "archiving", "needs_attention"}),
}


def transition_replacement_manifest(
    manifest: ReplacementManifest,
    state: str,
    *,
    error: object = _UNSET,
) -> ReplacementManifest:
    if not isinstance(manifest, ReplacementManifest):
        raise TypeError("manifest must be a ReplacementManifest")
    if state not in REPLACEMENT_STATES or state not in _ALLOWED_TRANSITIONS[manifest.state]:
        raise ReplacementTransitionError(f"cannot transition replacement from {manifest.state!r} to {state!r}")
    next_error = manifest.error if error is _UNSET else error
    if state in {"prepared", "archiving", "archived", "writing", "completed"} and error is _UNSET:
        next_error = None
    return replace(manifest, state=state, error=next_error, updated_at=_now())  # type: ignore[arg-type]


def derive_replacement_archive_root(
    library_root: object,
    work_root: object,
    root_job_id: object,
    *,
    archive_namespace: str = _ARCHIVE_MARKER,
) -> str:
    """Derive a task-owned archive path from the configured library root."""
    library = _strict_path(library_root, "library_root")
    work = _strict_path(work_root, "work_root")
    job_id = _safe_id(root_job_id, "root_job_id")
    namespace = _text(archive_namespace, "archive_namespace", maximum=64)
    if namespace != _ARCHIVE_MARKER or "/" in namespace or "\\" in namespace:
        raise ReplacementValidationError("archive namespace is reserved")
    if not _within(work, library):
        raise ReplacementValidationError("work_root must be within library_root")
    relative = work[len(library):].lstrip("/")
    if not relative:
        raise ReplacementValidationError("work_root cannot equal library_root")
    result = f"{library.rstrip('/')}/{namespace}/{job_id}/{relative}"
    return _strict_path(result, "archive_root")


def archive_path_for(manifest: ReplacementManifest, item: ReplacementItem, *, subtitle: bool = False) -> str:
    """Return the only archive destination allowed for one old target object."""
    if item not in manifest.items:
        raise ReplacementValidationError("item is not part of this manifest")
    old_path = item.subtitle_target_path if subtitle else item.target_path
    if old_path is None:
        raise ReplacementValidationError("replacement item has no old subtitle")
    name = PurePosixPath(old_path).name
    if not name or name in {".", ".."}:
        raise ReplacementValidationError("old target has an invalid basename")
    coordinate = item.target_coordinate
    result = f"{manifest.archive_root.rstrip('/')}/{coordinate}/{name}"
    return _strict_path(result, "derived archive path")


def _coerce_object(value: ReplacementObject | Mapping[str, object]) -> ReplacementObject:
    if isinstance(value, ReplacementObject):
        return value
    return ReplacementObject.from_dict(value)


def _object_index(objects: Sequence[ReplacementObject | Mapping[str, object]], *, kind: str) -> dict[str, ReplacementObject]:
    output: dict[str, ReplacementObject] = {}
    path_keys: set[str] = set()
    for raw in objects:
        item = _coerce_object(raw)
        if item.kind != kind:
            continue
        coordinate = item.coordinate
        # ``ReplacementObject`` already requires this for video/subtitle,
        # but keep the index defensive: it is the point where a forged frozen
        # object would otherwise become a ``None`` ownership key.
        if coordinate is None:
            raise ReplacementValidationError(f"{kind} object requires an explicit coordinate")
        if coordinate in output:
            raise ReplacementValidationError(f"duplicate {kind} coordinate: {coordinate}")
        if item.collision_key in path_keys:
            raise ReplacementValidationError("replacement listing contains a path collision")
        output[coordinate] = item
        path_keys.add(item.collision_key)
    return output


def _subtitle_index(
    subtitles: Mapping[str, ReplacementObject | Mapping[str, object]] | Sequence[ReplacementObject | Mapping[str, object]] | None,
) -> dict[str, ReplacementObject]:
    if subtitles is None:
        return {}
    output: dict[str, ReplacementObject] = {}
    for key, raw in subtitles.items() if isinstance(subtitles, Mapping) else ((None, value) for value in subtitles):
        item = _coerce_object(raw)
        if item.kind != "subtitle":
            raise ReplacementValidationError("selected subtitle map contains a non-subtitle")
        coordinate = item.coordinate
        if coordinate is None:
            raise ReplacementValidationError("selected subtitle requires an explicit coordinate")
        if isinstance(key, str):
            key_coordinate = normalize_coordinate(key, "subtitle coordinate")
            if key_coordinate != coordinate:
                raise ReplacementValidationError("selected subtitle coordinate does not match its object")
        if coordinate in output:
            raise ReplacementValidationError(f"duplicate selected subtitle: {coordinate}")
        if not _is_simplified(item.language, item.path):
            raise ReplacementValidationError("TC subtitle cannot be selected for replacement")
        output[coordinate] = item
    return output


def build_replacement_manifest(
    *,
    manifest_id: str,
    root_job_id: str,
    work_unit_id: str,
    tmdb_id: int,
    media_type: str = "tv",
    source_root: str,
    target_work_root: str,
    library_root: str,
    source_snapshot_id: str,
    coordinate_map: Mapping[str, str],
    source_objects: Sequence[ReplacementObject | Mapping[str, object]],
    target_objects: Sequence[ReplacementObject | Mapping[str, object]],
    selected_subtitles: Mapping[str, ReplacementObject | Mapping[str, object]] | Sequence[ReplacementObject | Mapping[str, object]] | None = None,
    require_subtitles: bool = True,
    allow_same_size: bool = False,
    same_size_proof: str | None = None,
    residual_paths: Sequence[str] | None = None,
    authorized: bool = True,
    expected_source_coordinates: Sequence[str] | None = None,
) -> ReplacementManifest:
    """Build a replacement proof from fresh server-side object listings.

    ``coordinate_map`` is mandatory and explicit.  The function never adds a
    constant offset, trusts a directory name, or accepts a caller-provided
    archive root.  All selected source/target objects must be present with
    positive sizes and one-to-one coordinates.
    """
    if not authorized:
        raise ReplacementValidationError("replacement requires explicit authorization")
    if not isinstance(coordinate_map, Mapping) or not coordinate_map:
        raise ReplacementValidationError("replacement requires an explicit coordinate map")
    source_root = _strict_path(source_root, "source_root")
    target_work_root = _strict_path(target_work_root, "target_work_root")
    library_root = _strict_path(library_root, "library_root")
    source_rows = [_coerce_object(raw) for raw in source_objects]
    snapshot_text = _text(source_snapshot_id, "source_snapshot_id", maximum=256)
    if any(obj.snapshot_id is not None and obj.snapshot_id != snapshot_text for obj in source_rows):
        raise ReplacementValidationError("source object snapshot does not match the manifest snapshot")
    archive_root = derive_replacement_archive_root(library_root, target_work_root, root_job_id)
    source_videos = _object_index(source_rows, kind="video")
    target_videos = _object_index(target_objects, kind="video")
    target_subtitles = _object_index(target_objects, kind="subtitle")
    selected = _subtitle_index(selected_subtitles)
    source_by_path = {
        obj.collision_key: obj
        for obj in source_rows
    }
    normalized_map: dict[str, str] = {}
    for source_coordinate, target_coordinate in coordinate_map.items():
        source_key = normalize_coordinate(source_coordinate, "source coordinate map key")
        target_key = normalize_coordinate(target_coordinate, "target coordinate map value")
        if source_key in normalized_map or target_key in normalized_map.values():
            raise ReplacementValidationError("replacement coordinate map is not one-to-one")
        normalized_map[source_key] = target_key
    if expected_source_coordinates is not None:
        expected = {
            normalize_coordinate(value, "expected source coordinate")
            for value in expected_source_coordinates
        }
        if set(normalized_map) != expected:
            raise ReplacementValidationError("coordinate map does not cover the expected source coordinates")
    if not set(normalized_map).issubset(set(source_videos)):
        raise ReplacementValidationError("coordinate map references a source video that is not present")
    if any(target not in target_videos for target in normalized_map.values()):
        raise ReplacementValidationError("every mapped target coordinate must have an existing video")
    if any(coordinate not in normalized_map for coordinate in selected):
        raise ReplacementValidationError("selected subtitle is not bound to a mapped source coordinate")
    items: list[ReplacementItem] = []
    selected_source_paths: set[str] = set()
    for source_coordinate, target_coordinate in sorted(normalized_map.items()):
        source = source_videos[source_coordinate]
        target = target_videos[target_coordinate]
        if not _within(source.path, source_root) or not _within(target.path, target_work_root):
            raise ReplacementValidationError("replacement object is outside its declared root")
        if source.size == target.size and not allow_same_size:
            raise ReplacementValidationError("same-size replacement is ambiguous")
        if source.size == target.size and (not isinstance(same_size_proof, str) or not same_size_proof.strip()):
            raise ReplacementValidationError("same-size replacement requires explicit content proof")
        subtitle = selected.get(source_coordinate) or selected.get(target_coordinate)
        if require_subtitles and subtitle is None:
            raise ReplacementValidationError(f"missing selected SC subtitle for {target_coordinate}")
        if subtitle is not None:
            observed_subtitle = source_by_path.get(subtitle.collision_key)
            if (
                observed_subtitle is None
                or observed_subtitle.kind != "subtitle"
                or observed_subtitle.size != subtitle.size
            ):
                raise ReplacementValidationError("selected subtitle is not present in the fresh source listing")
        old_subtitle = target_subtitles.get(target_coordinate)
        item = ReplacementItem(
            source_coordinate=source_coordinate,
            target_coordinate=target_coordinate,
            source_path=source.path,
            source_size=source.size,
            target_path=target.path,
            target_size=target.size,
            subtitle_source_path=subtitle.path if subtitle else None,
            subtitle_source_size=subtitle.size if subtitle else None,
            subtitle_target_path=old_subtitle.path if old_subtitle else None,
            subtitle_target_size=old_subtitle.size if old_subtitle else None,
            subtitle_language=subtitle.language if subtitle else None,
        )
        items.append(item)
        selected_source_paths.add(_collision_key(source.path))
        if subtitle is not None:
            selected_source_paths.add(_collision_key(subtitle.path))
    # Residual ownership is not a caller-controlled escape hatch.  The fresh
    # source listing is complete input to this builder, so every unselected
    # object must remain accounted for and no invented residual path may be
    # slipped into the manifest.  Keep the optional argument only as a
    # compatibility assertion for composition code that has already formed
    # the same exact residual projection.
    derived_residual = tuple(
        obj.path for obj in source_rows
        if obj.collision_key not in selected_source_paths
    )
    if residual_paths is None:
        residual = derived_residual
    else:
        supplied_residual = tuple(_strict_path(path, "residual path") for path in residual_paths)
        if {
            _collision_key(path) for path in supplied_residual
        } != {
            _collision_key(path) for path in derived_residual
        }:
            raise ReplacementValidationError(
                "residual paths must exactly cover every unselected fresh source object"
            )
        residual = supplied_residual
    residual_objects = tuple(
        obj
        for obj in source_rows
        if obj.collision_key not in selected_source_paths
    )
    # Keep the object projection deterministic for durable JSON and make the
    # path/list projection relationship explicit before constructing the
    # manifest's cross-field validator.
    if {
        _collision_key(obj.path) for obj in residual_objects
    } != {
        _collision_key(path) for path in residual
    }:
        raise ReplacementValidationError(
            "residual objects must exactly cover every unselected source object"
        )
    return ReplacementManifest(
        manifest_id=manifest_id,
        root_job_id=root_job_id,
        work_unit_id=work_unit_id,
        tmdb_id=tmdb_id,
        media_type=media_type,
        source_root=source_root,
        target_work_root=target_work_root,
        library_root=library_root,
        archive_root=archive_root,
        source_snapshot_id=source_snapshot_id,
        items=tuple(items),
        residual_paths=residual,
        residual_objects=residual_objects,
        authorized=True,
    )


@dataclass(frozen=True, slots=True)
class ReplacementRecoveryDecision:
    """Bounded result of comparing a manifest with fresh remote listings."""

    status: str
    completed_coordinates: tuple[str, ...] = ()
    pending_coordinates: tuple[str, ...] = ()
    unknown_paths: tuple[str, ...] = ()
    reason: str | None = None

    def __post_init__(self) -> None:
        if self.status not in {"not_started", "safe_to_resume", "completed", "attention"}:
            raise ReplacementValidationError("invalid recovery decision status")

    @property
    def can_resume(self) -> bool:
        return self.status in {"not_started", "safe_to_resume"}

    def as_dict(self) -> dict[str, object]:
        return {
            "status": self.status,
            "can_resume": self.can_resume,
            "completed_coordinates": list(self.completed_coordinates),
            "pending_coordinates": list(self.pending_coordinates),
            "unknown_paths": list(self.unknown_paths),
            "reason": self.reason,
        }


def _fresh_index(
    objects: Sequence[ReplacementObject | Mapping[str, object]],
) -> dict[str, ReplacementObject]:
    output: dict[str, ReplacementObject] = {}
    collision: set[str] = set()
    for raw in objects:
        obj = _coerce_object(raw)
        if obj.collision_key in collision:
            raise ReplacementValidationError("fresh replacement listing has a path collision")
        collision.add(obj.collision_key)
        output[obj.collision_key] = obj
    return output


def compare_replacement_fresh(
    manifest: ReplacementManifest,
    *,
    source_objects: Sequence[ReplacementObject | Mapping[str, object]],
    target_objects: Sequence[ReplacementObject | Mapping[str, object]],
    archive_objects: Sequence[ReplacementObject | Mapping[str, object]] = (),
) -> ReplacementRecoveryDecision:
    """Classify restart state without guessing from equal byte sizes."""
    if not isinstance(manifest, ReplacementManifest):
        raise TypeError("manifest must be a ReplacementManifest")
    source = _fresh_index(source_objects)
    target = _fresh_index(target_objects)
    archive = _fresh_index(archive_objects)
    completed: list[str] = []
    pending: list[str] = []
    unknown: list[str] = []
    archived_only = False

    # New manifests carry an exact object record for every residual member.
    # A path-only legacy manifest remains loadable, but it cannot be promoted
    # to a safe execution/recovery decision because a fresh listing could
    # silently change the residual's type or bytes.
    if not manifest.residual_metadata_complete:
        return ReplacementRecoveryDecision(
            "attention",
            pending_coordinates=tuple(manifest.target_coordinates),
            reason="legacy replacement manifest lacks exact residual object metadata",
        )

    # A replacement owns a complete, server-derived source listing.  New or
    # missing source paths therefore are not harmless background noise: they
    # could mean the selected files now belong to a different release tree.
    # The manifest's residual paths intentionally cover non-selected source
    # members (TC, menu, PV, artwork, …) without giving them a fake episode
    # coordinate.
    expected_source = {
        _collision_key(path)
        for item in manifest.items
        for path in (item.source_path, item.subtitle_source_path)
        if path is not None
    }
    expected_source.update(_collision_key(path) for path in manifest.residual_paths)
    observed_source = set(source)
    unexpected_source = [
        obj.path
        for key, obj in source.items()
        if not _within(obj.path, manifest.source_root) or key not in expected_source
    ]
    missing_source_residual = [
        path
        for path in manifest.residual_paths
        if _collision_key(path) not in observed_source
    ]
    if unexpected_source or missing_source_residual:
        return ReplacementRecoveryDecision(
            "attention",
            pending_coordinates=tuple(manifest.target_coordinates),
            unknown_paths=tuple(sorted(unexpected_source + missing_source_residual)),
            reason="source listing no longer exactly matches replacement ownership",
        )

    # Validate every persisted residual object against the fresh source.  The
    # path itself is checked exactly (not merely by the case-folded collision
    # key), followed by kind and size.  Optional provider metadata is strict
    # only when the manifest declared it; this preserves compatibility with
    # providers/listings that never expose version or modified fields while
    # still making a declared revision an execution-time proof.
    for expected_residual in manifest.residual_objects:
        fresh_residual = source.get(expected_residual.collision_key)
        if fresh_residual is None:
            # The path-level check above normally catches this; retain a
            # dedicated branch for defensive callers that forge a frozen
            # manifest instance.
            return ReplacementRecoveryDecision(
                "attention",
                pending_coordinates=tuple(manifest.target_coordinates),
                unknown_paths=(expected_residual.path,),
                reason=f"residual object is missing: {expected_residual.path}",
            )
        if fresh_residual.path != expected_residual.path:
            return ReplacementRecoveryDecision(
                "attention",
                pending_coordinates=tuple(manifest.target_coordinates),
                reason=f"residual path drift: {expected_residual.path}",
            )
        if fresh_residual.kind != expected_residual.kind:
            return ReplacementRecoveryDecision(
                "attention",
                pending_coordinates=tuple(manifest.target_coordinates),
                reason=f"residual kind drift: {expected_residual.path}",
            )
        if fresh_residual.size != expected_residual.size:
            return ReplacementRecoveryDecision(
                "attention",
                pending_coordinates=tuple(manifest.target_coordinates),
                reason=f"residual size drift: {expected_residual.path}",
            )
        for metadata_name in ("snapshot_id", "version", "modified"):
            expected_value = getattr(expected_residual, metadata_name)
            if expected_value is not None and getattr(fresh_residual, metadata_name) != expected_value:
                return ReplacementRecoveryDecision(
                    "attention",
                    pending_coordinates=tuple(manifest.target_coordinates),
                    reason=f"residual {metadata_name} drift: {expected_residual.path}",
                )

    # The old target video coordinates are the only target media that this
    # manifest can replace.  Extra target metadata/directories are normal
    # planner-preserved content, but an undeclared video is an ownership
    # conflict and must stop recovery rather than be silently left behind.
    expected_target_videos = {
        _collision_key(item.target_path)
        for item in manifest.items
    }
    unexpected_target_videos = [
        obj.path
        for key, obj in target.items()
        if not _within(obj.path, manifest.target_work_root)
        or (obj.kind == "video" and key not in expected_target_videos)
    ]
    if unexpected_target_videos:
        return ReplacementRecoveryDecision(
            "attention",
            pending_coordinates=tuple(manifest.target_coordinates),
            unknown_paths=tuple(sorted(unexpected_target_videos)),
            reason="target listing contains an undeclared video object",
        )
    for item in manifest.items:
        source_obj = source.get(_collision_key(item.source_path))
        if source_obj is None or source_obj.size != item.source_size or source_obj.kind != "video":
            return ReplacementRecoveryDecision(
                "attention", pending_coordinates=tuple(manifest.target_coordinates),
                reason=f"source drift or missing object: {item.source_path}",
            )
        source_subtitle = None
        if item.subtitle_source_path is not None:
            source_subtitle = source.get(_collision_key(item.subtitle_source_path))
            if (
                source_subtitle is None
                or source_subtitle.kind != "subtitle"
                or source_subtitle.size != item.subtitle_source_size
            ):
                return ReplacementRecoveryDecision(
                    "attention", pending_coordinates=tuple(manifest.target_coordinates),
                    reason=f"selected subtitle drift or missing object: {item.subtitle_source_path}",
                )
        subtitle_archived_only = False
        if item.subtitle_target_path is not None:
            current_subtitle = target.get(_collision_key(item.subtitle_target_path))
            archived_subtitle = archive.get(_collision_key(archive_path_for(manifest, item, subtitle=True)))
            if archived_subtitle is not None and archived_subtitle.size != item.subtitle_target_size:
                return ReplacementRecoveryDecision("attention", reason="archived old subtitle size mismatch")
            if archived_subtitle is not None and current_subtitle is None:
                subtitle_archived_only = True
            elif archived_subtitle is not None and current_subtitle is not None:
                if current_subtitle.size != item.subtitle_source_size:
                    return ReplacementRecoveryDecision("attention", reason="archive and subtitle target state conflict")
            elif archived_subtitle is None and current_subtitle is not None:
                if current_subtitle.size != item.subtitle_target_size:
                    return ReplacementRecoveryDecision("attention", reason="subtitle target size mismatch")
            else:
                return ReplacementRecoveryDecision("attention", reason="subtitle target disappeared before archive")
        target_obj = target.get(_collision_key(item.target_path))
        archived_obj = archive.get(_collision_key(archive_path_for(manifest, item)))
        if target_obj is not None and target_obj.kind != "video":
            return ReplacementRecoveryDecision("attention", reason="target path is not a video")
        if archived_obj is not None and archived_obj.kind != "video":
            return ReplacementRecoveryDecision("attention", reason="archive path is not an old video")
        if archived_obj is not None and archived_obj.size != item.target_size:
            return ReplacementRecoveryDecision("attention", reason="archived old target size mismatch")
        if target_obj is not None and target_obj.size == item.source_size and archived_obj is not None:
            if subtitle_archived_only:
                archived_only = True
                pending.append(item.target_coordinate)
            else:
                completed.append(item.target_coordinate)
            continue
        if archived_obj is not None and target_obj is None:
            archived_only = True
            pending.append(item.target_coordinate)
            continue
        if archived_obj is None and target_obj is not None and target_obj.size == item.target_size:
            pending.append(item.target_coordinate)
            continue
        if archived_obj is not None and target_obj is not None:
            return ReplacementRecoveryDecision("attention", reason="archive and target state conflict")
        return ReplacementRecoveryDecision("attention", reason="target state is neither old nor archived")
    expected_archive = {
        _collision_key(archive_path_for(manifest, item))
        for item in manifest.items
    }
    expected_archive.update(
        _collision_key(archive_path_for(manifest, item, subtitle=True))
        for item in manifest.items
        if item.subtitle_target_path is not None
    )
    expected_archive_dirs = {
        "/".join(path.split("/")[:index])
        for path in expected_archive
        for index in range(1, len(path.split("/")))
    }
    for key, obj in archive.items():
        if key not in expected_archive and not (
            obj.kind == "directory" and key in expected_archive_dirs
        ):
            unknown.append(obj.path)
    if unknown:
        return ReplacementRecoveryDecision("attention", unknown_paths=tuple(sorted(unknown)), reason="unknown archive object")
    if len(completed) == len(manifest.items):
        return ReplacementRecoveryDecision("completed", completed_coordinates=tuple(sorted(completed)))
    if completed or archived_only:
        return ReplacementRecoveryDecision(
            "safe_to_resume", completed_coordinates=tuple(sorted(completed)),
            pending_coordinates=tuple(sorted(pending)),
        )
    return ReplacementRecoveryDecision(
        "not_started", pending_coordinates=tuple(sorted(pending or manifest.target_coordinates)),
    )


# Friendly aliases for callers that use the plan's wording.
replacement_fresh_recovery = compare_replacement_fresh
validate_replacement_recovery = compare_replacement_fresh
ReplacementMapping = ReplacementItem
ReplacementRecovery = ReplacementRecoveryDecision


def replacement_manifest_path(state_root: Path, manifest_id: str) -> Path:
    safe_id = _safe_id(manifest_id, "manifest_id")
    return Path(state_root) / "replacement-manifests" / f"{safe_id}.json"


def save_replacement_manifest(state_root: Path, manifest: ReplacementManifest) -> None:
    if not isinstance(manifest, ReplacementManifest):
        raise TypeError("manifest must be a ReplacementManifest")
    # Re-parse the public projection before writing to re-check every
    # cross-field invariant even if a caller forged a frozen instance.
    validated = ReplacementManifest.from_dict(manifest.as_dict())
    atomic_write_json(replacement_manifest_path(state_root, validated.manifest_id), validated.as_dict(), allow_nan=False)


def load_replacement_manifest(state_root: Path, manifest_id: str) -> ReplacementManifest:
    path = replacement_manifest_path(state_root, manifest_id)
    try:
        if path.is_symlink() or not path.is_file():
            raise ReplacementValidationError("replacement manifest path is not a regular file")
        raw = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise ReplacementValidationError("replacement manifest does not exist") from exc
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ReplacementValidationError("replacement manifest cannot be read safely") from exc
    if not isinstance(raw, Mapping):
        raise ReplacementValidationError("replacement manifest root is invalid")
    manifest = ReplacementManifest.from_dict(raw)
    if manifest.manifest_id != manifest_id:
        raise ReplacementValidationError("replacement manifest id does not match its filename")
    return manifest


# Persistence spelling aliases used by composition roots that call all local
# records ``*_record`` rather than ``*_manifest``.
save_replacement_manifest_record = save_replacement_manifest
load_replacement_manifest_record = load_replacement_manifest


__all__ = [
    "REPLACEMENT_MANIFEST_VERSION",
    "REPLACEMENT_OBJECT_KINDS",
    "REPLACEMENT_STATES",
    "ReplacementError",
    "ReplacementItem",
    "ReplacementManifest",
    "ReplacementMapping",
    "ReplacementObject",
    "ReplacementRecoveryDecision",
    "ReplacementRecovery",
    "ReplacementSourceObject",
    "ReplacementTargetObject",
    "ReplacementTransitionError",
    "ReplacementValidationError",
    "archive_path_for",
    "build_replacement_manifest",
    "compare_replacement_fresh",
    "derive_replacement_archive_root",
    "load_replacement_manifest",
    "normalize_coordinate",
    "replacement_fresh_recovery",
    "replacement_manifest_path",
    "save_replacement_manifest",
    "save_replacement_manifest_record",
    "load_replacement_manifest_record",
    "transition_replacement_manifest",
    "validate_replacement_recovery",
]
