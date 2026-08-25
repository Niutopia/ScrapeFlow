"""Durable, redaction-safe evidence for opaque source containers.

The ordinary archive helpers (:mod:`engine.scrapeflow.archive`) deliberately
stop at a validated listing/extraction boundary.  This module is the small
domain object that a composition root can persist around that boundary.  It
does *not* inspect AList, mount an image, invoke 7-Zip, choose a WorkUnit, or
write the formal library.  A caller performs those operations through the
existing bounded archive adapter and then records the result here.

Keeping the inspection record separate is useful for recovery: an interrupted
ISO/SFX review can be resumed or parked without treating an old WorkUnit
ledger as fresh source evidence.  All durable projections are intentionally
credential-safe; member names and source paths may contain password markers,
but only their redacted forms are written to JSON.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
import json
import posixpath
import re
from typing import Any, Literal

from .archive import ArchiveListing, ArchiveLimits, ArchiveMember, extract_password_markers, normalize_member_path
from .serialization import atomic_write_json
from .source_objects import SourceObjectRef, normalize_source_object_path


CONTAINER_INSPECTION_VERSION = 1
CONTAINER_INSPECTION_STATUSES = frozenset({
    "inspected",
    "ready",
    "expanded",
    "attention",
    "failed",
})
CONTAINER_MEMBER_KINDS = frozenset({
    "video",
    "subtitle",
    "archive",
    "directory",
    "other",
})
CONTAINER_CANDIDATE_KINDS = frozenset({"video", "subtitle"})


class ContainerInspectionError(ValueError):
    """A malformed or unsafe container inspection record."""


class ContainerInspectionPersistenceError(ContainerInspectionError):
    """A persisted inspection is missing, malformed, or unsafe."""


_SAFE_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
_SAFE_TOKEN_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:+/-]{0,127}$")
_SECRET_ASSIGNMENT_RE = re.compile(
    r"(?i)(\b(?:password|passwd|pass|pwd|token|secret|api[\s_-]?key|cookie)\s*[:=]\s*)"
    r"([\"']?)([^\s,;&}\"']+)(\2)"
)
_SECRET_QUERY_RE = re.compile(
    r"(?i)([?&](?:password|passwd|pass|pwd|token|secret|api[\s_-]?key|cookie)=)"
    r"([^&#\s]+)"
)


def _text(value: object, *, name: str, required: bool = True, limit: int = 512) -> str | None:
    if value is None:
        if required:
            raise ContainerInspectionError(f"{name} is required")
        return None
    if not isinstance(value, str):
        raise ContainerInspectionError(f"{name} must be a string")
    if not value or (required and not value.strip()):
        if required:
            raise ContainerInspectionError(f"{name} is required")
        return None
    if len(value) > limit or "\x00" in value or any(ord(char) < 32 for char in value if char not in "\t\n\r"):
        raise ContainerInspectionError(f"{name} contains invalid text")
    return value


def _safe_id(value: object, *, name: str) -> str:
    text = _text(value, name=name, limit=128)
    assert text is not None
    if not _SAFE_ID_RE.fullmatch(text):
        raise ContainerInspectionError(f"{name} is not a safe identifier")
    return text


def _safe_token(value: object, *, name: str, limit: int = 128) -> str:
    text = _text(value, name=name, limit=limit)
    assert text is not None
    if not _SAFE_TOKEN_RE.fullmatch(text):
        raise ContainerInspectionError(f"{name} contains an unsafe token")
    return text.casefold()


def _nonnegative_int(value: object, *, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ContainerInspectionError(f"{name} must be a non-negative integer")
    return value


def _optional_nonnegative_int(value: object, *, name: str) -> int | None:
    if value is None:
        return None
    return _nonnegative_int(value, name=name)


def _optional_positive_number(value: object, *, name: str) -> int | float | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)) or value <= 0:
        raise ContainerInspectionError(f"{name} must be positive")
    return value


def _bool(value: object, *, name: str) -> bool:
    if not isinstance(value, bool):
        raise ContainerInspectionError(f"{name} must be boolean")
    return value


def _redact_text(value: object) -> str:
    """Return bounded text with marker and key/value secrets removed."""

    text = str(value)
    for secret in extract_password_markers(text):
        if secret:
            text = text.replace(secret, "<redacted>")
    text = _SECRET_ASSIGNMENT_RE.sub(r"\1\2<redacted>\4", text)
    return _SECRET_QUERY_RE.sub(r"\1<redacted>", text)


def _redacted_path(value: str) -> str:
    return _redact_text(value)


def _relative_member_path(value: object) -> str:
    if not isinstance(value, str):
        raise ContainerInspectionError("container member path must be a string")
    try:
        return normalize_member_path(value)
    except Exception as exc:
        raise ContainerInspectionError("container member path is unsafe") from exc


def _absolute_path(value: object, *, name: str) -> str:
    if not isinstance(value, str) or not value.startswith("/") or "\\" in value:
        raise ContainerInspectionError(f"{name} must be an absolute POSIX path")
    try:
        normalized = normalize_source_object_path(value, allow_root=True)
    except Exception as exc:
        raise ContainerInspectionError(f"{name} is not normalized") from exc
    return normalized


def _descendant_or_same(path: str, root: str) -> bool:
    return path == root or root == "/" or path.startswith(root + "/")


@dataclass(frozen=True, slots=True)
class ContainerMember:
    """A redaction-safe projection of one validated container member."""

    path: str = field(repr=False)
    size: int
    kind: str = "other"
    is_dir: bool = False
    is_link: bool = False
    encrypted: bool = False

    def __post_init__(self) -> None:
        object.__setattr__(self, "path", _relative_member_path(self.path))
        object.__setattr__(self, "size", _nonnegative_int(self.size, name="member size"))
        kind = _safe_token(self.kind, name="member kind")
        if kind not in CONTAINER_MEMBER_KINDS:
            raise ContainerInspectionError("member kind is unsupported")
        object.__setattr__(self, "kind", kind)
        for name in ("is_dir", "is_link", "encrypted"):
            _bool(getattr(self, name), name=f"member {name}")

    @property
    def collision_key(self) -> str:
        return "/".join(part.casefold() for part in self.path.split("/"))

    @classmethod
    def from_archive_member(cls, member: ArchiveMember) -> "ContainerMember":
        if not isinstance(member, ArchiveMember):
            raise ContainerInspectionError("archive member is invalid")
        return cls(
            path=member.path,
            size=member.size,
            kind=member.media_kind if not member.is_dir else "directory",
            is_dir=member.is_dir,
            is_link=member.is_link,
            encrypted=member.encrypted,
        )

    @classmethod
    def from_dict(cls, raw: Mapping[str, object]) -> "ContainerMember":
        if not isinstance(raw, Mapping):
            raise ContainerInspectionError("container member must be an object")
        return cls(
            path=raw.get("path"),  # type: ignore[arg-type]
            size=raw.get("size", 0),  # type: ignore[arg-type]
            kind=raw.get("kind", "other"),  # type: ignore[arg-type]
            is_dir=_bool(raw.get("is_dir", False), name="member is_dir"),
            is_link=_bool(raw.get("is_link", False), name="member is_link"),
            encrypted=_bool(raw.get("encrypted", False), name="member encrypted"),
        )

    def as_dict(self) -> dict[str, object]:
        # Member paths are source-controlled names and may contain explicit
        # password markers.  Redact at the projection boundary every time.
        return {
            "path": _redacted_path(self.path),
            "size": self.size,
            "kind": self.kind,
            "is_dir": self.is_dir,
            "is_link": self.is_link,
            "encrypted": self.encrypted,
        }

    to_dict = as_dict


@dataclass(frozen=True, slots=True)
class ContainerMediaCandidate:
    """One media/subtitle member eligible for a later B/W rebuild."""

    path: str = field(repr=False)
    kind: Literal["video", "subtitle"]
    size: int

    def __post_init__(self) -> None:
        object.__setattr__(self, "path", _relative_member_path(self.path))
        if self.kind not in CONTAINER_CANDIDATE_KINDS:
            raise ContainerInspectionError("media candidate kind is invalid")
        object.__setattr__(self, "size", _nonnegative_int(self.size, name="candidate size"))

    @classmethod
    def from_dict(cls, raw: Mapping[str, object]) -> "ContainerMediaCandidate":
        if not isinstance(raw, Mapping):
            raise ContainerInspectionError("media candidate must be an object")
        return cls(
            path=raw.get("path"),  # type: ignore[arg-type]
            kind=raw.get("kind"),  # type: ignore[arg-type]
            size=raw.get("size", 0),  # type: ignore[arg-type]
        )

    def as_dict(self) -> dict[str, object]:
        return {"path": _redacted_path(self.path), "kind": self.kind, "size": self.size}

    to_dict = as_dict


@dataclass(frozen=True, slots=True)
class ContainerLimitResult:
    """The bounded policy outcome captured alongside an inspection."""

    passed: bool
    member_count: int
    total_member_bytes: int
    source_bytes: int = 0
    max_source_bytes: int | None = None
    expansion_ratio: float | None = None
    max_expansion_ratio: float | None = None
    max_members: int | None = None
    max_depth: int | None = None
    max_member_bytes: int | None = None
    max_expanded_bytes: int | None = None
    observed_max_depth: int = 0
    disk_free_bytes: int | None = None
    min_free_bytes: int | None = None
    timeout_seconds: int | float | None = None
    violations: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not isinstance(self.passed, bool):
            raise ContainerInspectionError("limit result passed must be boolean")
        object.__setattr__(self, "member_count", _nonnegative_int(self.member_count, name="member count"))
        object.__setattr__(self, "total_member_bytes", _nonnegative_int(self.total_member_bytes, name="member bytes"))
        object.__setattr__(self, "source_bytes", _nonnegative_int(self.source_bytes, name="source bytes"))
        object.__setattr__(self, "max_source_bytes", _optional_nonnegative_int(self.max_source_bytes, name="max source bytes"))
        if self.expansion_ratio is not None:
            if isinstance(self.expansion_ratio, bool) or not isinstance(self.expansion_ratio, (int, float)) or self.expansion_ratio < 0:
                raise ContainerInspectionError("expansion ratio must be non-negative")
            object.__setattr__(self, "expansion_ratio", float(self.expansion_ratio))
        if self.max_expansion_ratio is not None:
            object.__setattr__(self, "max_expansion_ratio", _optional_positive_number(self.max_expansion_ratio, name="max expansion ratio"))
        object.__setattr__(self, "observed_max_depth", _nonnegative_int(self.observed_max_depth, name="observed member depth"))
        for name in ("max_members", "max_depth", "max_member_bytes", "max_expanded_bytes", "disk_free_bytes", "min_free_bytes"):
            object.__setattr__(self, name, _optional_nonnegative_int(getattr(self, name), name=name))
        object.__setattr__(self, "timeout_seconds", _optional_positive_number(self.timeout_seconds, name="timeout"))
        raw_violations = tuple(self.violations)
        if any(not isinstance(item, str) or not item.strip() for item in raw_violations):
            raise ContainerInspectionError("limit violations must be non-empty text")
        violations = tuple(dict.fromkeys(_redact_text(item) for item in raw_violations))
        if self.passed and violations:
            raise ContainerInspectionError("passed limit result cannot contain violations")
        object.__setattr__(self, "violations", violations)

    @classmethod
    def from_archive_members(
        cls,
        members: Sequence[ContainerMember],
        *,
        limits: ArchiveLimits | None = None,
        archive_format: str | None = None,
        archive_size: int | None = None,
        disk_free_bytes: int | None = None,
        violations: Iterable[str] = (),
        passed: bool = True,
    ) -> "ContainerLimitResult":
        policy = limits or ArchiveLimits()
        total = sum(member.size for member in members if not member.is_dir)
        observed_max_depth = max((member.path.count("/") + 1 for member in members), default=0)
        is_disc = str(archive_format or "").casefold() in {"iso", "udf", "iso9660"}
        source_budget = (
            policy.max_disc_image_bytes if is_disc else policy.max_archive_bytes
        )
        ratio_budget = (
            policy.max_disc_expansion_ratio if is_disc else policy.max_expansion_ratio
        )
        max_member_bytes = policy.max_disc_member_bytes if is_disc else policy.max_member_bytes
        max_expanded_bytes = policy.max_disc_expanded_bytes if is_disc else policy.max_expanded_bytes
        timeout_seconds = (
            policy.disc_command_timeout_seconds if is_disc else policy.command_timeout_seconds
        )
        computed_violations = list(violations)
        if len(members) > policy.max_members:
            computed_violations.append("member_count_exceeds_limit")
        if observed_max_depth > policy.max_depth:
            computed_violations.append("member_depth_exceeds_limit")
        if any(not member.is_dir and member.size > max_member_bytes for member in members):
            computed_violations.append("member_size_exceeds_limit")
        if total > max_expanded_bytes:
            computed_violations.append("expanded_size_exceeds_limit")
        source_value = 0 if archive_size is None else _nonnegative_int(archive_size, name="source bytes")
        if source_value and source_value > source_budget:
            computed_violations.append("source_size_exceeds_limit")
        ratio = (total / source_value) if source_value else None
        if ratio is not None and ratio > ratio_budget:
            computed_violations.append("expansion_ratio_exceeds_limit")
        if disk_free_bytes is not None and disk_free_bytes - total < policy.min_free_bytes:
            computed_violations.append("staging_disk_reserve_insufficient")
        deduped_violations = tuple(dict.fromkeys(computed_violations))
        return cls(
            passed=bool(passed) and not deduped_violations,
            member_count=len(members),
            total_member_bytes=total,
            source_bytes=source_value,
            max_source_bytes=source_budget,
            expansion_ratio=ratio,
            max_expansion_ratio=ratio_budget,
            max_members=policy.max_members,
            max_depth=policy.max_depth,
            max_member_bytes=max_member_bytes,
            max_expanded_bytes=max_expanded_bytes,
            observed_max_depth=observed_max_depth,
            disk_free_bytes=disk_free_bytes,
            min_free_bytes=policy.min_free_bytes,
            timeout_seconds=timeout_seconds,
            violations=deduped_violations,
        )

    @classmethod
    def from_dict(cls, raw: Mapping[str, object]) -> "ContainerLimitResult":
        if not isinstance(raw, Mapping):
            raise ContainerInspectionError("limit result must be an object")
        violations = raw.get("violations", ())
        if not isinstance(violations, Sequence) or isinstance(violations, (str, bytes, bytearray)):
            raise ContainerInspectionError("limit violations must be a list")
        raw_passed = raw.get("passed", False)
        if not isinstance(raw_passed, bool):
            raise ContainerInspectionError("limit result passed must be boolean")
        return cls(
            passed=raw_passed,
            member_count=raw.get("member_count", 0),  # type: ignore[arg-type]
            total_member_bytes=raw.get("total_member_bytes", 0),  # type: ignore[arg-type]
            source_bytes=raw.get("source_bytes", 0),  # type: ignore[arg-type]
            max_source_bytes=raw.get("max_source_bytes"),  # type: ignore[arg-type]
            expansion_ratio=raw.get("expansion_ratio"),  # type: ignore[arg-type]
            max_expansion_ratio=raw.get("max_expansion_ratio"),  # type: ignore[arg-type]
            max_members=raw.get("max_members"),  # type: ignore[arg-type]
            max_depth=raw.get("max_depth"),  # type: ignore[arg-type]
            max_member_bytes=raw.get("max_member_bytes"),  # type: ignore[arg-type]
            max_expanded_bytes=raw.get("max_expanded_bytes"),  # type: ignore[arg-type]
            observed_max_depth=raw.get("observed_max_depth", 0),  # type: ignore[arg-type]
            disk_free_bytes=raw.get("disk_free_bytes"),  # type: ignore[arg-type]
            min_free_bytes=raw.get("min_free_bytes"),  # type: ignore[arg-type]
            timeout_seconds=raw.get("timeout_seconds"),  # type: ignore[arg-type]
            violations=tuple(violations),  # type: ignore[arg-type]
        )

    def as_dict(self) -> dict[str, object]:
        return {
            "passed": self.passed,
            "member_count": self.member_count,
            "total_member_bytes": self.total_member_bytes,
            "source_bytes": self.source_bytes,
            "max_source_bytes": self.max_source_bytes,
            "expansion_ratio": self.expansion_ratio,
            "max_expansion_ratio": self.max_expansion_ratio,
            "max_members": self.max_members,
            "max_depth": self.max_depth,
            "max_member_bytes": self.max_member_bytes,
            "max_expanded_bytes": self.max_expanded_bytes,
            "observed_max_depth": self.observed_max_depth,
            "disk_free_bytes": self.disk_free_bytes,
            "min_free_bytes": self.min_free_bytes,
            "timeout_seconds": self.timeout_seconds,
            "violations": list(self.violations),
        }

    to_dict = as_dict


# A shorter spelling is convenient for callers and keeps compatibility with
# code that names this projection ``LimitResult``.
ContainerLimitsResult = ContainerLimitResult


@dataclass(frozen=True, slots=True, repr=False)
class ContainerInspection:
    """Persisted evidence for one opaque archive/disc/SFX source object."""

    inspection_id: str
    source_object: SourceObjectRef = field(repr=False)
    detected_format: str
    members: tuple[ContainerMember, ...] = field(default_factory=tuple, repr=False)
    media_candidates: tuple[ContainerMediaCandidate, ...] = field(default_factory=tuple, repr=False)
    limits: ContainerLimitResult = field(default_factory=lambda: ContainerLimitResult(False, 0, 0))
    staging_path: str | None = field(default=None, repr=False)
    status: str = "inspected"
    failure_reason: str | None = field(default=None, repr=False)
    created_at: str = "unknown"
    expanded_snapshot_id: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "inspection_id", _safe_id(self.inspection_id, name="inspection_id"))
        if not isinstance(self.source_object, SourceObjectRef):
            raise ContainerInspectionError("source_object must be a SourceObjectRef")
        if self.source_object.is_directory:
            raise ContainerInspectionError("source_object must be a file container")
        object.__setattr__(self, "detected_format", _safe_token(self.detected_format, name="detected_format"))
        members = tuple(self.members)
        if any(not isinstance(item, ContainerMember) for item in members):
            raise ContainerInspectionError("members contain an invalid entry")
        by_path: dict[str, ContainerMember] = {}
        by_collision: dict[str, ContainerMember] = {}
        collisions: set[str] = set()
        for member in members:
            if member.path in by_path or member.collision_key in collisions:
                raise ContainerInspectionError("members contain duplicate or colliding paths")
            by_path[member.path] = member
            by_collision[member.collision_key] = member
            collisions.add(member.collision_key)
        # A file cannot be the ancestor of another member.  Archive validation
        # already enforces this, but retaining the invariant here protects
        # records assembled by future composition layers.
        for member in members:
            parts = member.collision_key.split("/")
            for index in range(1, len(parts)):
                ancestor = by_collision.get("/".join(parts[:index]))
                if ancestor is not None and not ancestor.is_dir:
                    raise ContainerInspectionError("members contain file/directory collision")
        candidates = tuple(self.media_candidates)
        if any(not isinstance(item, ContainerMediaCandidate) for item in candidates):
            raise ContainerInspectionError("media_candidates contain an invalid entry")
        candidate_paths: set[str] = set()
        for candidate in candidates:
            if candidate.path in candidate_paths:
                raise ContainerInspectionError("media_candidates contain duplicates")
            member = by_path.get(candidate.path)
            if member is None or member.is_dir or member.is_link:
                raise ContainerInspectionError("media candidate is not a regular member")
            if member.kind != candidate.kind or member.size != candidate.size:
                raise ContainerInspectionError("media candidate does not match its member")
            candidate_paths.add(candidate.path)
        if not isinstance(self.limits, ContainerLimitResult):
            raise ContainerInspectionError("limits must be a ContainerLimitResult")
        if self.staging_path is not None:
            staging = _absolute_path(self.staging_path, name="staging_path")
            if staging == self.source_object.path or _descendant_or_same(staging, self.source_object.path):
                raise ContainerInspectionError("staging_path overlaps source object")
            source_parent = posixpath.dirname(self.source_object.path)
            if source_parent != "/" and _descendant_or_same(staging, source_parent):
                raise ContainerInspectionError("staging_path must not sit inside the source directory")
            object.__setattr__(self, "staging_path", staging)
        status = _safe_token(self.status, name="status")
        if status not in CONTAINER_INSPECTION_STATUSES:
            raise ContainerInspectionError("container inspection status is invalid")
        if status in {"ready", "expanded"} and not self.limits.passed:
            raise ContainerInspectionError("ready/expanded inspection requires passed limits")
        if status in {"ready", "expanded"} and any(
            member.is_link or member.kind == "archive" for member in members
        ):
            raise ContainerInspectionError("ready/expanded inspection contains a forbidden link or nested archive")
        if status == "expanded" and (self.staging_path is None or not self.expanded_snapshot_id):
            raise ContainerInspectionError("expanded inspection requires staging and a new snapshot id")
        if status in {"attention", "failed"} and not self.failure_reason:
            raise ContainerInspectionError("failed/attention inspection requires a reason")
        object.__setattr__(self, "members", tuple(sorted(members, key=lambda item: item.path)))
        object.__setattr__(self, "media_candidates", tuple(sorted(candidates, key=lambda item: item.path)))
        object.__setattr__(self, "status", status)
        if self.failure_reason is not None:
            object.__setattr__(self, "failure_reason", _redact_text(_text(self.failure_reason, name="failure_reason", limit=2048)))
        object.__setattr__(self, "created_at", _text(self.created_at, name="created_at", limit=128) or "")
        if self.expanded_snapshot_id is not None:
            object.__setattr__(self, "expanded_snapshot_id", _text(self.expanded_snapshot_id, name="expanded_snapshot_id", limit=128))

    @property
    def original_object(self) -> SourceObjectRef:
        """Compatibility alias matching the product-language field name."""
        return self.source_object

    @property
    def is_successful(self) -> bool:
        return self.status in {"inspected", "ready", "expanded"} and self.limits.passed

    @classmethod
    def from_archive_listing(
        cls,
        *,
        inspection_id: str,
        source_object: SourceObjectRef,
        listing: ArchiveListing,
        limits: ArchiveLimits | None = None,
        staging_path: str | None = None,
        status: str = "inspected",
        failure_reason: str | None = None,
        created_at: str,
        expanded_snapshot_id: str | None = None,
        disk_free_bytes: int | None = None,
        limit_violations: Iterable[str] = (),
    ) -> "ContainerInspection":
        if not isinstance(listing, ArchiveListing):
            raise ContainerInspectionError("listing must be an ArchiveListing")
        members = tuple(ContainerMember.from_archive_member(item) for item in listing.members)
        candidates = tuple(
            ContainerMediaCandidate(path=item.path, kind=item.kind, size=item.size)
            for item in members
            if not item.is_dir and not item.is_link and item.kind in CONTAINER_CANDIDATE_KINDS
        )
        raw_violations = tuple(limit_violations)
        limit_result = ContainerLimitResult.from_archive_members(
            members,
            limits=limits,
            archive_format=listing.archive_format,
            archive_size=listing.archive_size,
            disk_free_bytes=disk_free_bytes,
            violations=raw_violations,
            passed=not raw_violations,
        )
        return cls(
            inspection_id=inspection_id,
            source_object=source_object,
            detected_format=listing.archive_format,
            members=members,
            media_candidates=candidates,
            limits=limit_result,
            staging_path=staging_path,
            status=status,
            failure_reason=failure_reason,
            created_at=created_at,
            expanded_snapshot_id=expanded_snapshot_id,
        )

    @classmethod
    def from_dict(cls, raw: Mapping[str, object]) -> "ContainerInspection":
        if not isinstance(raw, Mapping):
            raise ContainerInspectionPersistenceError("container inspection is not an object")
        if raw.get("version", CONTAINER_INSPECTION_VERSION) != CONTAINER_INSPECTION_VERSION:
            raise ContainerInspectionPersistenceError("container inspection version is unsupported")
        source_raw = raw.get("source_object", raw.get("original_object"))
        if not isinstance(source_raw, Mapping):
            raise ContainerInspectionPersistenceError("container inspection source object is invalid")
        raw_members = raw.get("members", ())
        raw_candidates = raw.get("media_candidates", ())
        if not isinstance(raw_members, Sequence) or isinstance(raw_members, (str, bytes, bytearray)):
            raise ContainerInspectionPersistenceError("container inspection members are invalid")
        if not isinstance(raw_candidates, Sequence) or isinstance(raw_candidates, (str, bytes, bytearray)):
            raise ContainerInspectionPersistenceError("container inspection candidates are invalid")
        if any(not isinstance(item, Mapping) for item in raw_members):
            raise ContainerInspectionPersistenceError("container inspection members are invalid")
        if any(not isinstance(item, Mapping) for item in raw_candidates):
            raise ContainerInspectionPersistenceError("container inspection candidates are invalid")
        try:
            return cls(
                inspection_id=raw.get("inspection_id"),  # type: ignore[arg-type]
                source_object=SourceObjectRef.from_dict(source_raw),
                detected_format=raw.get("detected_format"),  # type: ignore[arg-type]
                members=tuple(ContainerMember.from_dict(item) for item in raw_members),  # type: ignore[arg-type]
                media_candidates=tuple(ContainerMediaCandidate.from_dict(item) for item in raw_candidates),  # type: ignore[arg-type]
                limits=ContainerLimitResult.from_dict(raw.get("limits", {})),  # type: ignore[arg-type]
                staging_path=raw.get("staging_path"),  # type: ignore[arg-type]
                status=raw.get("status", "failed"),  # type: ignore[arg-type]
                failure_reason=raw.get("failure_reason"),  # type: ignore[arg-type]
                created_at=raw.get("created_at", ""),  # type: ignore[arg-type]
                expanded_snapshot_id=raw.get("expanded_snapshot_id"),  # type: ignore[arg-type]
            )
        except ContainerInspectionError:
            raise
        except Exception as exc:
            raise ContainerInspectionPersistenceError("container inspection is malformed") from exc

    def as_dict(self) -> dict[str, object]:
        source = self.source_object.as_dict()
        for key in ("path", "version", "modified"):
            if isinstance(source.get(key), str):
                source[key] = _redact_text(source[key])
        return {
            "version": CONTAINER_INSPECTION_VERSION,
            "inspection_id": self.inspection_id,
            "source_object": source,
            "detected_format": self.detected_format,
            "members": [item.as_dict() for item in self.members],
            "media_candidates": [item.as_dict() for item in self.media_candidates],
            "limits": self.limits.as_dict(),
            "staging_path": _redacted_path(self.staging_path) if self.staging_path else None,
            "status": self.status,
            "failure_reason": _redact_text(self.failure_reason) if self.failure_reason else None,
            "created_at": self.created_at,
            "expanded_snapshot_id": self.expanded_snapshot_id,
        }

    to_dict = as_dict

    def __repr__(self) -> str:
        return (
            "ContainerInspection("
            f"inspection_id={self.inspection_id!r}, detected_format={self.detected_format!r}, "
            f"members={len(self.members)}, media_candidates={len(self.media_candidates)}, "
            f"status={self.status!r})"
        )


def container_inspection_path(state_root: Path, inspection_id: str) -> Path:
    """Return the owner-only JSON path for one inspection identifier."""

    if not isinstance(state_root, Path):
        state_root = Path(state_root)
    safe_id = _safe_id(inspection_id, name="inspection_id")
    return state_root / f"container-inspection-{safe_id}.json"


def save_container_inspection(state_root: Path, inspection: ContainerInspection) -> Path:
    """Atomically persist one inspection projection and return its path."""

    if not isinstance(inspection, ContainerInspection):
        raise ContainerInspectionPersistenceError("inspection has an invalid type")
    path = container_inspection_path(state_root, inspection.inspection_id)
    atomic_write_json(path, inspection.as_dict(), allow_nan=False)
    return path


def load_container_inspection(
    state_root: Path,
    inspection_id: str,
    *,
    strict: bool = False,
) -> ContainerInspection | None:
    """Load one persisted inspection; malformed files fail closed.

    ``strict=True`` raises :class:`ContainerInspectionPersistenceError`, which
    is useful at a mutation boundary.  The default returns ``None`` for a
    missing or malformed artifact so a caller can park the current source.
    """

    path = container_inspection_path(state_root, inspection_id)
    try:
        if path.is_symlink() or not path.is_file():
            raise ContainerInspectionPersistenceError("container inspection path is not a regular file")
        raw = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(raw, Mapping) or raw.get("version") != CONTAINER_INSPECTION_VERSION:
            raise ContainerInspectionPersistenceError("container inspection version is unsupported")
        return ContainerInspection.from_dict(raw)
    except ContainerInspectionPersistenceError:
        if strict:
            raise
        return None
    except (OSError, UnicodeDecodeError, json.JSONDecodeError, TypeError, ValueError) as exc:
        if strict:
            raise ContainerInspectionPersistenceError("container inspection cannot be read") from exc
        return None


def list_container_inspections(state_root: Path) -> tuple[ContainerInspection, ...]:
    """Load all well-named inspections, skipping malformed artifacts."""

    root = Path(state_root)
    if not root.exists() or not root.is_dir() or root.is_symlink():
        return ()
    output: list[ContainerInspection] = []
    for path in sorted(root.glob("container-inspection-*.json")):
        if path.is_symlink() or not path.is_file():
            continue
        match = re.fullmatch(r"container-inspection-(?P<id>[A-Za-z0-9][A-Za-z0-9._-]{0,127})\.json", path.name)
        if not match:
            continue
        item = load_container_inspection(root, match.group("id"))
        if item is not None:
            output.append(item)
    return tuple(output)


# Familiar aliases for composition layers that call these records "container
# reviews" or use the shorter persistence names.
save_container_inspection_record = save_container_inspection
load_container_inspection_record = load_container_inspection


__all__ = [
    "CONTAINER_INSPECTION_STATUSES",
    "CONTAINER_INSPECTION_VERSION",
    "ContainerInspection",
    "ContainerInspectionError",
    "ContainerInspectionPersistenceError",
    "ContainerLimitResult",
    "ContainerLimitsResult",
    "ContainerMediaCandidate",
    "ContainerMember",
    "container_inspection_path",
    "list_container_inspections",
    "load_container_inspection",
    "load_container_inspection_record",
    "save_container_inspection",
    "save_container_inspection_record",
]
