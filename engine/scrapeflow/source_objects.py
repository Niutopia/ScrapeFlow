"""Exact, immutable source-object manifests.

Directory boundaries are useful evidence during B/W, but they are too broad
to be an execution authority.  This module carries the narrower proof used by
later stages: every source object has one normalized provider path, type,
size, revision metadata, and the identifier of the snapshot that observed it.

The module is deliberately pure.  It does not list AList, persist JSON, choose
media, or make a cleanup decision.  Runtime callers build a manifest from a
fresh listing and use :func:`compare_fresh_manifest` before handing exact
objects to a planner, writer, cleanup operation, or readback routine.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import PurePosixPath
import posixpath
import unicodedata

from .source_inventory import classify_object_type


class SourceObjectValidationError(ValueError):
    """Raised when an exact source-object proof is malformed or unsafe."""


class SourceObjectOwnershipError(SourceObjectValidationError):
    """Raised when two owners claim the same source object."""


class SourceManifestDriftError(SourceObjectValidationError):
    """Raised when a fresh manifest no longer equals its declared snapshot."""


OWNER_KINDS = frozenset({"work_unit", "residual", "attention"})


def _has_unsafe_unicode(value: str) -> bool:
    return any(unicodedata.category(character) in {"Cc", "Cf", "Cs"} for character in value)


def normalize_source_object_path(path: object, *, allow_root: bool = False) -> str:
    """Require one already-normalized absolute provider path.

    Unlike convenience path normalizers, this function never repairs a path.
    A caller that supplies a dot segment, duplicate separator, trailing
    separator, backslash, or invisible-control character must stop and obtain
    a fresh safe listing rather than silently changing the object it claims.
    """
    if not isinstance(path, str) or not path:
        raise SourceObjectValidationError("来源对象路径必须是非空字符串")
    if not path.startswith("/") or "\\" in path:
        raise SourceObjectValidationError("来源对象路径必须是规范的绝对 POSIX 路径")
    if _has_unsafe_unicode(path):
        raise SourceObjectValidationError("来源对象路径不能包含控制或不可见格式字符")
    if path == "/":
        if allow_root:
            return path
        raise SourceObjectValidationError("来源对象路径不能是根目录")
    if path != "/" and path.endswith("/"):
        raise SourceObjectValidationError("来源对象路径不能包含尾随分隔符")
    if posixpath.normpath(path) != path:
        raise SourceObjectValidationError("来源对象路径未规范化")
    parts = path.split("/")[1:]
    if any(not part or part in {".", ".."} for part in parts):
        raise SourceObjectValidationError("来源对象路径包含无效段")
    return path


def _normalized_text(value: object, *, field_name: str, required: bool = False) -> str | None:
    if value is None:
        if required:
            raise SourceObjectValidationError(f"{field_name} 不能为空")
        return None
    if not isinstance(value, str):
        raise SourceObjectValidationError(f"{field_name} 必须是字符串")
    text = value.strip()
    if not text:
        if required:
            raise SourceObjectValidationError(f"{field_name} 不能为空")
        return None
    if _has_unsafe_unicode(text):
        raise SourceObjectValidationError(f"{field_name} 不能包含控制或不可见格式字符")
    return text


def _metadata_text(value: object, *, field_name: str) -> str | None:
    """Validate opaque provider metadata without changing its byte spelling."""
    if value is None:
        return None
    if not isinstance(value, str):
        raise SourceObjectValidationError(f"{field_name} 必须是字符串")
    if not value.strip():
        return None
    if _has_unsafe_unicode(value):
        raise SourceObjectValidationError(f"{field_name} 不能包含控制或不可见格式字符")
    return value


def _listing_metadata(value: object) -> str | None:
    """Losslessly stringify a primitive metadata value emitted by a provider."""
    if value in (None, ""):
        return None
    if isinstance(value, bool) or not isinstance(value, (str, int, float)):
        raise SourceObjectValidationError("来源快照条目版本或修改信息无效")
    return str(value)


def _object_type(value: object) -> str:
    text = _normalized_text(value, field_name="来源对象类型", required=True)
    assert text is not None
    if any(character.isspace() for character in text):
        raise SourceObjectValidationError("来源对象类型不能包含空白")
    return text.casefold()


def _size(value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise SourceObjectValidationError("来源对象大小必须是非负整数")
    return value


def _collision_key(path: str) -> str:
    """Return the provider-ambiguous case/Unicode comparison key for ``path``."""
    return "/".join(unicodedata.normalize("NFC", part).casefold() for part in path.split("/"))


def _is_descendant_or_same(path: str, root_path: str) -> bool:
    return root_path == "/" or path == root_path or path.startswith(root_path + "/")


@dataclass(frozen=True, slots=True)
class SourceObjectRef:
    """One exact object observed in one source snapshot.

    ``object_type`` is a semantic source kind (``video``, ``subtitle``,
    ``directory``, ``other``, ...), not an assertion that the object is safe
    to consume.  ``version`` is an optional provider revision/etag-like value;
    ``modified`` records provider modification metadata when available.  Both
    are compared verbatim so provider-specific formats remain lossless.
    """

    path: str
    object_type: str
    size: int
    snapshot_id: str
    version: str | None = None
    modified: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "path", normalize_source_object_path(self.path))
        object.__setattr__(self, "object_type", _object_type(self.object_type))
        object.__setattr__(self, "size", _size(self.size))
        snapshot_id = _normalized_text(self.snapshot_id, field_name="来源快照标识", required=True)
        assert snapshot_id is not None
        object.__setattr__(self, "snapshot_id", snapshot_id)
        object.__setattr__(self, "version", _metadata_text(self.version, field_name="来源对象版本"))
        object.__setattr__(self, "modified", _metadata_text(self.modified, field_name="来源对象修改信息"))

    @property
    def is_directory(self) -> bool:
        """Whether this reference denotes a directory rather than a file."""
        return self.object_type == "directory"

    @property
    def source_snapshot_id(self) -> str:
        """Compatibility spelling that makes the source provenance explicit."""
        return self.snapshot_id

    @property
    def fingerprint(self) -> tuple[str, str, int, str | None, str | None]:
        """The observation fields that must survive a fresh read unchanged.

        The snapshot id deliberately is *not* part of this fingerprint: a
        newly created fresh snapshot has a new identifier even when the remote
        object did not change.
        """
        return (self.path, self.object_type, self.size, self.version, self.modified)

    @property
    def path_collision_key(self) -> str:
        """Case- and Unicode-normalized key used for fail-closed manifests."""
        return _collision_key(self.path)

    def same_observation(self, other: object) -> bool:
        """Compare the provider-observable object fields, excluding snapshot id."""
        return isinstance(other, SourceObjectRef) and self.fingerprint == other.fingerprint

    def as_dict(self) -> dict[str, object]:
        return {
            "path": self.path,
            "object_type": self.object_type,
            "size": self.size,
            "snapshot_id": self.snapshot_id,
            "version": self.version,
            "modified": self.modified,
        }

    @classmethod
    def from_dict(cls, raw: Mapping[str, object]) -> "SourceObjectRef":
        if not isinstance(raw, Mapping):
            raise SourceObjectValidationError("来源对象必须是映射")
        object_type = raw.get("object_type", raw.get("type"))
        return cls(
            path=raw.get("path"),  # type: ignore[arg-type]
            object_type=object_type,  # type: ignore[arg-type]
            size=raw.get("size"),  # type: ignore[arg-type]
            snapshot_id=raw.get("snapshot_id", raw.get("source_snapshot_id")),  # type: ignore[arg-type]
            version=raw.get("version"),  # type: ignore[arg-type]
            modified=raw.get("modified"),  # type: ignore[arg-type]
        )

    @classmethod
    def from_listing_row(
        cls,
        row: Mapping[str, object],
        *,
        snapshot_id: str,
    ) -> "SourceObjectRef":
        """Convert one already-fetched AList-style row without I/O.

        ``object_type`` takes precedence when an upstream inspector has
        classified a row.  Otherwise directories become ``directory`` and
        files use the shared suffix classifier.  This method intentionally
        does not accept a provider's untrusted generic ``type`` field.
        """
        if not isinstance(row, Mapping):
            raise SourceObjectValidationError("来源快照条目必须是映射")
        raw_path = row.get("full_path")
        if raw_path in (None, ""):
            raw_path = row.get("path")
        path = normalize_source_object_path(raw_path)
        if row.get("is_dir") is True:
            object_type = "directory"
        else:
            raw_type = row.get("object_type")
            object_type = raw_type if isinstance(raw_type, str) and raw_type.strip() else classify_object_type(PurePosixPath(path).name)
        is_directory = row.get("is_dir") is True
        if "size" not in row or row.get("size") is None:
            if not is_directory:
                raise SourceObjectValidationError("来源快照条目缺少大小")
            raw_size: object = 0
        else:
            raw_size = row.get("size")
        if isinstance(raw_size, bool) or isinstance(raw_size, float):
            raise SourceObjectValidationError("来源快照条目大小无效")
        if isinstance(raw_size, int):
            size = raw_size
        elif isinstance(raw_size, str):
            if not raw_size.strip() or raw_size.strip().startswith(("+", "-")):
                raise SourceObjectValidationError("来源快照条目大小无效")
            try:
                size = int(raw_size)
            except ValueError as exc:
                raise SourceObjectValidationError("来源快照条目大小无效") from exc
        else:
            raise SourceObjectValidationError("来源快照条目大小无效")
        modified: object = row.get("modified")
        if modified in (None, ""):
            for key in ("updated_at", "mtime", "last_modified"):
                candidate = row.get(key)
                if candidate not in (None, ""):
                    modified = candidate
                    break
        return cls(
            path=path,
            object_type=object_type,
            size=size,
            snapshot_id=snapshot_id,
            version=_listing_metadata(row.get("version")),
            modified=_listing_metadata(modified),
        )


@dataclass(frozen=True, slots=True)
class SourceObjectChange:
    """A same-path object whose fresh observation no longer matches."""

    path: str
    expected: SourceObjectRef
    fresh: SourceObjectRef
    changed_fields: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class SourceManifestComparison:
    """Structured result of comparing a declared manifest to a fresh listing."""

    expected_snapshot_id: str
    fresh_snapshot_id: str
    root_matches: bool
    missing_paths: tuple[str, ...]
    unexpected_paths: tuple[str, ...]
    changed: tuple[SourceObjectChange, ...]
    unchanged_paths: tuple[str, ...]

    @property
    def matches(self) -> bool:
        """True only when roots and every exact object remain unchanged."""
        return (
            self.root_matches
            and not self.missing_paths
            and not self.unexpected_paths
            and not self.changed
        )


@dataclass(frozen=True, slots=True)
class SourceManifest:
    """All exact objects from one immutable source snapshot."""

    snapshot_id: str
    root_path: str
    objects: tuple[SourceObjectRef, ...] = field(default_factory=tuple)

    def __post_init__(self) -> None:
        snapshot_id = _normalized_text(self.snapshot_id, field_name="来源快照标识", required=True)
        assert snapshot_id is not None
        root_path = normalize_source_object_path(self.root_path, allow_root=True)
        try:
            raw_objects = tuple(self.objects)
        except TypeError as exc:
            raise SourceObjectValidationError("来源清单对象必须是可迭代集合") from exc
        seen_paths: set[str] = set()
        seen_collision_keys: set[str] = set()
        for item in raw_objects:
            if not isinstance(item, SourceObjectRef):
                raise SourceObjectValidationError("来源清单包含无效对象")
            if item.snapshot_id != snapshot_id:
                raise SourceObjectValidationError("来源对象快照标识与清单不一致")
            if not _is_descendant_or_same(item.path, root_path):
                raise SourceObjectValidationError("来源对象不属于来源快照根")
            if item.path in seen_paths:
                raise SourceObjectValidationError("来源清单包含重复对象路径")
            collision_key = item.path_collision_key
            if collision_key in seen_collision_keys:
                raise SourceObjectValidationError("来源清单包含 Unicode 或大小写碰撞路径")
            seen_paths.add(item.path)
            seen_collision_keys.add(collision_key)
        object.__setattr__(self, "snapshot_id", snapshot_id)
        object.__setattr__(self, "root_path", root_path)
        object.__setattr__(self, "objects", tuple(sorted(raw_objects, key=lambda item: item.path)))

    @property
    def object_paths(self) -> tuple[str, ...]:
        return tuple(item.path for item in self.objects)

    def object_at(self, path: object) -> SourceObjectRef | None:
        """Return one exact object by its strict normalized path."""
        normalized = normalize_source_object_path(path)
        for item in self.objects:
            if item.path == normalized:
                return item
        return None

    def as_dict(self) -> dict[str, object]:
        return {
            "snapshot_id": self.snapshot_id,
            "root_path": self.root_path,
            "objects": [item.as_dict() for item in self.objects],
        }

    @classmethod
    def from_dict(cls, raw: Mapping[str, object]) -> "SourceManifest":
        if not isinstance(raw, Mapping):
            raise SourceObjectValidationError("来源清单必须是映射")
        raw_objects = raw.get("objects", ())
        if not isinstance(raw_objects, Sequence) or isinstance(raw_objects, (str, bytes, bytearray)):
            raise SourceObjectValidationError("来源清单对象必须是列表")
        objects: list[SourceObjectRef] = []
        for item in raw_objects:
            if not isinstance(item, Mapping):
                raise SourceObjectValidationError("来源清单包含无效对象")
            objects.append(SourceObjectRef.from_dict(item))
        return cls(
            snapshot_id=raw.get("snapshot_id", raw.get("source_snapshot_id")),  # type: ignore[arg-type]
            root_path=raw.get("root_path"),  # type: ignore[arg-type]
            objects=tuple(objects),
        )

    @classmethod
    def from_listing_rows(
        cls,
        rows: Iterable[Mapping[str, object]],
        *,
        root_path: str,
        snapshot_id: str,
    ) -> "SourceManifest":
        """Build a pure manifest from an already-fetched recursive listing."""
        return cls(
            snapshot_id=snapshot_id,
            root_path=root_path,
            objects=tuple(
                SourceObjectRef.from_listing_row(row, snapshot_id=snapshot_id)
                for row in rows
            ),
        )

    def compare_fresh(self, fresh: "SourceManifest") -> SourceManifestComparison:
        return compare_fresh_manifest(self, fresh)

    def require_fresh_match(self, fresh: "SourceManifest") -> None:
        comparison = self.compare_fresh(fresh)
        if not comparison.matches:
            raise SourceManifestDriftError(_format_drift_message(comparison))


def _changed_fields(expected: SourceObjectRef, fresh: SourceObjectRef) -> tuple[str, ...]:
    fields: list[str] = []
    if expected.object_type != fresh.object_type:
        fields.append("object_type")
    if expected.size != fresh.size:
        fields.append("size")
    if expected.version != fresh.version:
        fields.append("version")
    if expected.modified != fresh.modified:
        fields.append("modified")
    return tuple(fields)


def compare_fresh_manifest(
    expected: SourceManifest,
    fresh: SourceManifest,
) -> SourceManifestComparison:
    """Compare two snapshot manifests without treating a new snapshot id as drift.

    The comparison is intentionally exact.  A newly appeared object is as
    unsafe as a missing or changed one, because no WorkUnit/residual owner has
    declared it yet.
    """
    if not isinstance(expected, SourceManifest) or not isinstance(fresh, SourceManifest):
        raise SourceObjectValidationError("fresh 比对要求两个来源清单")
    expected_by_path = {item.path: item for item in expected.objects}
    fresh_by_path = {item.path: item for item in fresh.objects}
    missing_paths = tuple(sorted(set(expected_by_path) - set(fresh_by_path)))
    unexpected_paths = tuple(sorted(set(fresh_by_path) - set(expected_by_path)))
    changes: list[SourceObjectChange] = []
    unchanged: list[str] = []
    for path in sorted(set(expected_by_path) & set(fresh_by_path)):
        previous = expected_by_path[path]
        current = fresh_by_path[path]
        changed_fields = _changed_fields(previous, current)
        if changed_fields:
            changes.append(SourceObjectChange(path, previous, current, changed_fields))
        else:
            unchanged.append(path)
    return SourceManifestComparison(
        expected_snapshot_id=expected.snapshot_id,
        fresh_snapshot_id=fresh.snapshot_id,
        root_matches=expected.root_path == fresh.root_path,
        missing_paths=missing_paths,
        unexpected_paths=unexpected_paths,
        changed=tuple(changes),
        unchanged_paths=tuple(unchanged),
    )


def _format_drift_message(comparison: SourceManifestComparison) -> str:
    parts: list[str] = []
    if not comparison.root_matches:
        parts.append("来源根不一致")
    if comparison.missing_paths:
        parts.append(f"缺少 {len(comparison.missing_paths)} 个对象")
    if comparison.unexpected_paths:
        parts.append(f"新增 {len(comparison.unexpected_paths)} 个对象")
    if comparison.changed:
        parts.append(f"变更 {len(comparison.changed)} 个对象")
    return "fresh 来源清单与已声明对象不一致（" + "；".join(parts) + "）"


@dataclass(frozen=True, slots=True)
class SourceObjectClaim:
    """One WorkUnit, residual, or attention record's exact object ownership."""

    owner_kind: str
    owner_id: str
    objects: tuple[SourceObjectRef, ...]

    def __post_init__(self) -> None:
        owner_kind = _normalized_text(self.owner_kind, field_name="来源对象归属类型", required=True)
        owner_id = _normalized_text(self.owner_id, field_name="来源对象归属标识", required=True)
        assert owner_kind is not None and owner_id is not None
        owner_kind = owner_kind.casefold()
        if owner_kind not in OWNER_KINDS:
            raise SourceObjectValidationError("来源对象归属类型无效")
        try:
            objects = tuple(self.objects)
        except TypeError as exc:
            raise SourceObjectValidationError("来源对象归属必须是可迭代集合") from exc
        if not objects:
            raise SourceObjectValidationError("来源对象归属不能为空")
        if any(not isinstance(item, SourceObjectRef) for item in objects):
            raise SourceObjectValidationError("来源对象归属包含无效对象")
        object.__setattr__(self, "owner_kind", owner_kind)
        object.__setattr__(self, "owner_id", owner_id)
        object.__setattr__(self, "objects", objects)


@dataclass(frozen=True, slots=True)
class SourceObjectOwner:
    """The sole owner selected for one exact source path."""

    owner_kind: str
    owner_id: str
    snapshot_id: str


def validate_unique_source_object_ownership(
    claims: Iterable[SourceObjectClaim],
) -> dict[str, SourceObjectOwner]:
    """Prove every object belongs to exactly one WorkUnit/residual/attention row.

    All claimed objects must come from one source snapshot.  A path conflict,
    case/Unicode collision, or an attempt to mix stale and current snapshots
    fails closed even when the duplicate owner identifier happens to match.
    """
    ownership: dict[str, SourceObjectOwner] = {}
    collision_keys: dict[str, str] = {}
    snapshot_id: str | None = None
    for claim in claims:
        if not isinstance(claim, SourceObjectClaim):
            raise SourceObjectValidationError("来源对象归属必须是 SourceObjectClaim")
        for item in claim.objects:
            if snapshot_id is None:
                snapshot_id = item.snapshot_id
            elif item.snapshot_id != snapshot_id:
                raise SourceObjectOwnershipError("来源对象归属混用了不同来源快照")
            if item.path in ownership:
                prior = ownership[item.path]
                raise SourceObjectOwnershipError(
                    f"来源对象已由 {prior.owner_kind}:{prior.owner_id} 归属: {item.path}"
                )
            collision_key = item.path_collision_key
            collided_path = collision_keys.get(collision_key)
            if collided_path is not None:
                raise SourceObjectOwnershipError(
                    f"来源对象路径发生 Unicode 或大小写碰撞: {collided_path} / {item.path}"
                )
            ownership[item.path] = SourceObjectOwner(
                owner_kind=claim.owner_kind,
                owner_id=claim.owner_id,
                snapshot_id=item.snapshot_id,
            )
            collision_keys[collision_key] = item.path
    return ownership


def validate_manifest_source_object_ownership(
    manifest: SourceManifest,
    claims: Iterable[SourceObjectClaim],
    *,
    require_complete: bool = True,
) -> dict[str, SourceObjectOwner]:
    """Validate ownership against one declared source manifest.

    In addition to cross-owner uniqueness, this proves that every claimed ref
    is a byte-for-byte observation from ``manifest`` and, by default, that no
    source object was silently omitted.  That latter check is what prevents an
    unknown MV/PV or resource file from disappearing between B/W and F.
    """
    if not isinstance(manifest, SourceManifest):
        raise SourceObjectValidationError("对象归属校验要求来源清单")
    try:
        claim_rows = tuple(claims)
    except TypeError as exc:
        raise SourceObjectValidationError("来源对象归属必须是可迭代集合") from exc
    ownership = validate_unique_source_object_ownership(claim_rows)
    manifest_by_path = {item.path: item for item in manifest.objects}
    for claim in claim_rows:
        for item in claim.objects:
            expected = manifest_by_path.get(item.path)
            if expected is None:
                raise SourceObjectOwnershipError("对象归属包含来源清单外对象")
            if item.snapshot_id != manifest.snapshot_id:
                raise SourceObjectOwnershipError("对象归属不属于当前来源快照")
            if not expected.same_observation(item):
                raise SourceObjectOwnershipError("对象归属与来源快照对象状态不一致")
    if require_complete:
        unclaimed = tuple(sorted(set(manifest_by_path) - set(ownership)))
        if unclaimed:
            preview = "、".join(unclaimed[:3])
            suffix = " …" if len(unclaimed) > 3 else ""
            raise SourceObjectOwnershipError(
                f"来源清单存在 {len(unclaimed)} 个未归属对象: {preview}{suffix}"
            )
    return ownership


def validate_source_object_ownership(
    *,
    manifest: SourceManifest | None = None,
    require_complete: bool | None = None,
    work_units: Mapping[str, Iterable[SourceObjectRef]] | None = None,
    residuals: Mapping[str, Iterable[SourceObjectRef]] | None = None,
    attentions: Mapping[str, Iterable[SourceObjectRef]] | None = None,
) -> dict[str, SourceObjectOwner]:
    """Convenience wrapper for the three first-class source-owner categories.

    Passing ``manifest`` enables exact freshness/state validation and defaults
    to complete coverage.  Without a manifest this function can prove only
    non-overlap, so requesting complete coverage is rejected rather than
    pretending the proof exists.
    """
    claims: list[SourceObjectClaim] = []
    for owner_kind, owners in (
        ("work_unit", {} if work_units is None else work_units),
        ("residual", {} if residuals is None else residuals),
        ("attention", {} if attentions is None else attentions),
    ):
        if not isinstance(owners, Mapping):
            raise SourceObjectValidationError("来源对象归属集合必须是映射")
        for owner_id, objects in owners.items():
            claims.append(SourceObjectClaim(
                owner_kind=owner_kind,
                owner_id=str(owner_id),
                objects=objects,  # type: ignore[arg-type]
            ))
    if manifest is not None:
        return validate_manifest_source_object_ownership(
            manifest,
            claims,
            require_complete=True if require_complete is None else require_complete,
        )
    if require_complete:
        raise SourceObjectValidationError("完整对象归属校验需要来源清单")
    return validate_unique_source_object_ownership(claims)


__all__ = [
    "OWNER_KINDS",
    "SourceManifest",
    "SourceManifestComparison",
    "SourceManifestDriftError",
    "SourceObjectChange",
    "SourceObjectClaim",
    "SourceObjectOwner",
    "SourceObjectOwnershipError",
    "SourceObjectRef",
    "SourceObjectValidationError",
    "compare_fresh_manifest",
    "normalize_source_object_path",
    "validate_manifest_source_object_ownership",
    "validate_source_object_ownership",
    "validate_unique_source_object_ownership",
]
