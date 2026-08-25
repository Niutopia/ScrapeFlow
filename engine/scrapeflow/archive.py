"""Small, fail-closed archive inspection and extraction domain.

This module is intentionally narrower than the historical archive command.
It owns byte/magic detection, local 7-Zip listing, member/path validation and
bounded selection extraction into a task-owned staging directory.  It does
not own an AList transaction, a receipt/digest/journal, remote decompression,
or a formal-library writer.

The two I/O boundaries are explicit and easy to fake in tests:

``ArchiveRunner``
    Runs a list of argv tokens (normally the local ``7z`` executable).
``ArchiveSource``
    Optionally reads a remote source prefix and streams one source file to a
    local path before the same local inspector is used.  ``AListArchiveSource``
    is a tiny adapter around the already existing AList client methods.

Passwords are accepted only in memory.  Inspection results expose the source
label (``retry``, ``path-marker`` …), never the password value, and all error
messages deliberately omit command output and candidate text.
"""

from __future__ import annotations

from dataclasses import dataclass, field
import json
import math
import os
from pathlib import Path
import posixpath
import re
import shutil
import stat
import subprocess
import unicodedata
from typing import Any, Callable, Iterable, Mapping, Protocol, Sequence

from .media_policy import (
    ARCHIVE_EXTENSIONS,
    SUBTITLE_EXTENSIONS,
    VIDEO_EXTENSIONS,
    extension,
    is_archive_filename,
    is_subtitle_filename,
    is_video_filename,
)
from .media_quality import video_size_is_admissible


# ---------------------------------------------------------------------------
# Errors and bounded policy


class ArchiveError(Exception):
    """Base class for a user-visible, non-secret archive failure."""

    code = "archive_error"

    def __init__(self, message: str = "archive operation failed", *, code: str | None = None):
        super().__init__(message)
        if code is not None:
            self.code = code


class ArchivePauseRequested(ArchiveError):
    """A caller-owned pause fence stopped an archive operation safely.

    The archive domain is intentionally unaware of the application control
    state.  Composition layers may nevertheless pass a checkpoint callback
    before a subprocess, local-staging creation, or remote archive operation.
    Keeping the signal here lets those layers preserve it through archive
    helpers instead of accidentally translating a safe pause into a failed
    archive.
    """

    code = "archive_pause_requested"
    # Keep the cooperative control signal recognizable by the composition
    # layers which cannot import the API runtime without forming a cycle.
    # In particular, root replenishment must not classify this as an
    # infrastructure outage and retry it as a fresh provider attempt.
    pause_requested = True


class ArchiveFormatError(ArchiveError):
    code = "archive_format_invalid"


class ArchiveMagicError(ArchiveFormatError):
    code = "archive_magic_invalid"


class ArchiveListingError(ArchiveError):
    code = "archive_listing_failed"


class ArchiveMemberError(ArchiveError):
    code = "archive_member_invalid"


class ArchivePathError(ArchiveMemberError):
    code = "archive_member_path_invalid"


class ArchiveLinkError(ArchiveMemberError):
    code = "archive_member_link_rejected"


class ArchiveCollisionError(ArchiveMemberError):
    code = "archive_member_collision"


class ArchiveVolumeError(ArchiveError):
    code = "archive_volume_invalid"


class ArchiveBudgetError(ArchiveError):
    code = "archive_budget_exceeded"


class ArchivePasswordError(ArchiveError):
    code = "archive_password_failed"


class ArchivePasswordConflict(ArchivePasswordError):
    code = "archive_password_conflict"


class ArchiveToolError(ArchiveError):
    code = "archive_tool_unavailable"


class ArchiveExtractionError(ArchiveError):
    code = "archive_extraction_failed"


class ArchiveCorruptionError(ArchiveExtractionError):
    """7-Zip reported a structural archive failure, not a password failure."""

    code = "archive_corrupt"


class NestedArchiveUnsupported(ArchiveExtractionError):
    code = "nested_archive_unsupported"


@dataclass(frozen=True, slots=True)
class ArchiveLimits:
    """Hard limits applied before and after invoking 7-Zip.

    The defaults are deliberately finite.  ``min_free_bytes`` may be set to
    zero by an isolated unit test, but production callers should retain a
    reserve so a decompression failure cannot fill the host disk.
    """

    max_members: int = 20_000
    max_depth: int = 32
    max_expanded_bytes: int = 8 * 1024**3
    max_member_bytes: int = 8 * 1024**3
    max_archive_bytes: int = 16 * 1024**3
    max_expansion_ratio: float = 200.0
    min_free_bytes: int = 2 * 1024**3
    max_magic_scan_bytes: int = 1024 * 1024
    max_source_entries: int = 10_000
    max_password_candidates: int = 5
    command_timeout_seconds: float = 15 * 60
    # Optical images are containers rather than direct media.  They need more
    # headroom than ordinary compressed archives (a single Blu-ray image is
    # commonly 45–50 GiB), but keep independently finite limits so widening
    # disc support does not also widen generic ZIP/7z/RAR bomb budgets.
    max_disc_image_bytes: int = 64 * 1024**3
    max_disc_expanded_bytes: int = 64 * 1024**3
    max_disc_member_bytes: int = 64 * 1024**3
    max_disc_expansion_ratio: float = 4.0
    disc_command_timeout_seconds: float = 90 * 60

    def __post_init__(self) -> None:
        integer_fields = (
            "max_members",
            "max_depth",
            "max_expanded_bytes",
            "max_member_bytes",
            "max_archive_bytes",
            "min_free_bytes",
            "max_magic_scan_bytes",
            "max_source_entries",
            "max_password_candidates",
            "max_disc_image_bytes",
            "max_disc_expanded_bytes",
            "max_disc_member_bytes",
        )
        for name in integer_fields:
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ValueError(f"{name} must be a non-negative integer")
        if self.max_members == 0 or self.max_depth == 0 or self.max_source_entries == 0:
            raise ValueError("max_members/max_depth/max_source_entries must be positive")
        finite_positive_fields = (
            "max_expansion_ratio",
            "command_timeout_seconds",
            "max_disc_expansion_ratio",
            "disc_command_timeout_seconds",
        )
        if any(
            not isinstance(getattr(self, name), (int, float))
            or isinstance(getattr(self, name), bool)
            or not math.isfinite(float(getattr(self, name)))
            or float(getattr(self, name)) <= 0
            for name in finite_positive_fields
        ):
            raise ValueError("archive ratio and timeout must be positive")

    @classmethod
    def from_environment(cls, environ: Mapping[str, str] | None = None) -> "ArchiveLimits":
        """Build bounded limits from explicit deployment settings.

        The environment is an optional operational tuning surface, not a
        per-task escape hatch.  Invalid values fail application startup rather
        than silently disabling a guard.  Disc-image limits remain separate
        from ordinary archive limits so a 50 GiB ISO cannot accidentally make
        a 50 GiB ZIP acceptable.
        """

        values = os.environ if environ is None else environ
        integer_settings = {
            "SCRAPEFLOW_ARCHIVE_MAX_MEMBERS": "max_members",
            "SCRAPEFLOW_ARCHIVE_MAX_DEPTH": "max_depth",
            "SCRAPEFLOW_ARCHIVE_MAX_EXPANDED_BYTES": "max_expanded_bytes",
            "SCRAPEFLOW_ARCHIVE_MAX_MEMBER_BYTES": "max_member_bytes",
            "SCRAPEFLOW_ARCHIVE_MAX_SOURCE_BYTES": "max_archive_bytes",
            "SCRAPEFLOW_ARCHIVE_MIN_FREE_BYTES": "min_free_bytes",
            "SCRAPEFLOW_ARCHIVE_MAX_MAGIC_SCAN_BYTES": "max_magic_scan_bytes",
            "SCRAPEFLOW_DISC_IMAGE_MAX_SOURCE_BYTES": "max_disc_image_bytes",
            "SCRAPEFLOW_DISC_IMAGE_MAX_EXPANDED_BYTES": "max_disc_expanded_bytes",
            "SCRAPEFLOW_DISC_IMAGE_MAX_MEMBER_BYTES": "max_disc_member_bytes",
        }
        float_settings = {
            "SCRAPEFLOW_ARCHIVE_MAX_EXPANSION_RATIO": "max_expansion_ratio",
            "SCRAPEFLOW_ARCHIVE_COMMAND_TIMEOUT_SECONDS": "command_timeout_seconds",
            "SCRAPEFLOW_DISC_IMAGE_MAX_EXPANSION_RATIO": "max_disc_expansion_ratio",
            "SCRAPEFLOW_DISC_IMAGE_COMMAND_TIMEOUT_SECONDS": "disc_command_timeout_seconds",
        }
        parsed: dict[str, int | float] = {}
        for environment_name, field_name in integer_settings.items():
            raw = values.get(environment_name)
            if raw is None or not str(raw).strip():
                continue
            try:
                parsed[field_name] = int(str(raw).strip())
            except (TypeError, ValueError) as exc:
                raise ValueError(f"{environment_name} must be an integer") from exc
        for environment_name, field_name in float_settings.items():
            raw = values.get(environment_name)
            if raw is None or not str(raw).strip():
                continue
            try:
                parsed[field_name] = float(str(raw).strip())
            except (TypeError, ValueError) as exc:
                raise ValueError(f"{environment_name} must be a number") from exc
        return cls(**parsed)


# ---------------------------------------------------------------------------
# Pure byte and path helpers


@dataclass(frozen=True, slots=True)
class ArchiveMagic:
    """Result of bounded byte-signature detection."""

    format: str | None
    kind: str
    offset: int = 0
    self_extracting: bool = False

    @property
    def is_archive(self) -> bool:
        """Whether this is a container which the bounded 7-Zip lane may read.

        ``iso`` and ``udf`` deliberately share the established archive lane.
        They are not directly plannable media: 7-Zip only lists or extracts
        explicitly selected members into task-owned staging, and never mounts
        an image or executes an SFX wrapper.
        """

        return self.kind == "archive" and self.format in {
            "zip", "7z", "rar", "iso", "udf",
        }

    @property
    def is_disc_image(self) -> bool:
        """Whether the recognized container is an optical-disc filesystem."""

        return self.kind == "archive" and self.format in {"iso", "udf"}


_MAGIC_SIGNATURES: tuple[tuple[str, bytes], ...] = (
    ("7z", b"7z\xbc\xaf'\x1c"),
    ("rar", b"Rar!\x1a\x07\x01\x00"),
    ("rar", b"Rar!\x1a\x07\x00"),
    ("zip", b"PK\x03\x04"),
    ("zip", b"PK\x05\x06"),
    ("zip", b"PK\x07\x08"),
)
_EBML_MAGIC = b"\x1a\x45\xdf\xa3"
# ISO-9660's volume descriptors begin at logical sector 16.  The identifier
# starts after the one-byte descriptor type.  UDF uses the same Volume
# Recognition Sequence area, where NSR02/NSR03 identifies its namespace.
# These are filesystem signatures, not filename guesses; a ``.iso``/``.img``
# with no corresponding bytes remains an untrusted opaque object.
_DISC_SECTOR_BYTES = 2048
_ISO9660_IDENTIFIER_OFFSET = 16 * _DISC_SECTOR_BYTES + 1
_ISO9660_IDENTIFIER = b"CD001"
_UDF_VRS_FIRST_SECTOR = 16
_UDF_VRS_LAST_SECTOR = 32
_UDF_NAMESPACE_IDENTIFIERS = frozenset({b"NSR02", b"NSR03"})
_UDF_ANCHOR_SECTOR = 256
_PASSWORD_MARKER_RE = re.compile(
    r"(?:解压|压缩包|归档)?\s*(?:密码|password|passwd|pass|pwd)\s*[:：=]\s*"
    r"(?P<password>[^\s,，;；/\\]+)",
    re.IGNORECASE,
)
_MAX_PASSWORD_LENGTH = 128
_DRIVE_PATH_RE = re.compile(r"^[A-Za-z]:")
_CONTROL_RE = re.compile(r"[\x00-\x1f\x7f]")
_PASSWORD_FAILURE_RE = re.compile(
    r"(?:wrong|incorrect)\s+password|password\s+(?:is\s+)?(?:wrong|incorrect|invalid)"
    r"|can(?:not|'t)\s+open\s+(?:the\s+)?encrypted\s+archive",
    re.IGNORECASE,
)
_CORRUPTION_FAILURE_RE = re.compile(
    r"(?:headers?\s+error|unexpected\s+end|data\s+error|crc\s+failed|"
    r"not\s+(?:an|a)\s+archive|cannot\s+open\s+(?:the\s+)?archive)",
    re.IGNORECASE,
)
_RESERVED_WINDOWS_NAMES = {
    "con",
    "prn",
    "aux",
    "nul",
    *(f"com{i}" for i in range(1, 10)),
    *(f"lpt{i}" for i in range(1, 10)),
}


def _find_signature(prefix: bytes, signature: bytes, *, embedded: bool) -> int | None:
    if prefix.startswith(signature):
        return 0
    if not embedded:
        return None
    # An embedded signature is accepted only for a self-extracting MZ file.
    # This keeps an arbitrary media payload containing the bytes ``PK`` from
    # being treated as an archive while still supporting common SFX files.
    if not prefix.startswith(b"MZ"):
        return None
    index = prefix.find(signature, 2)
    return index if 0 <= index <= 1024 * 1024 else None


def _detect_disc_image_magic(data: bytes) -> ArchiveMagic | None:
    """Recognize ISO-9660/UDF without mounting a source image.

    The caller supplies an already bounded prefix.  A true PE/MZ executable
    is never trusted from its filename alone: only a complete embedded
    volume-descriptor proof (or the ordinary ZIP/7z/RAR SFX signatures handled
    by :func:`_find_signature`) can classify it as a read-only container.
    """

    if data.startswith(b"MZ"):
        # A few releases are distributed as a PE-looking wrapper around an
        # appended disc image.  Treat this exactly like an SFX archive: inspect
        # bytes only, require a complete volume-descriptor signature, and pass
        # the resulting source to 7-Zip.  A lone MZ header remains executable
        # and is rejected by ``detect_magic`` below.
        for marker in (b"CD001", b"NSR02", b"NSR03"):
            search_from = 2
            while True:
                marker_offset = data.find(marker, search_from)
                if marker_offset < 0:
                    break
                if marker == b"CD001":
                    image_base = marker_offset - _ISO9660_IDENTIFIER_OFFSET
                    type_offset = image_base + (16 * _DISC_SECTOR_BYTES)
                    version_offset = marker_offset + len(marker)
                    if (
                        image_base >= 2
                        and type_offset < len(data)
                        and data[type_offset] in {1, 2, 255}
                        and version_offset < len(data)
                        and data[version_offset] == 1
                    ):
                        return ArchiveMagic(
                            "iso",
                            "archive",
                            marker_offset,
                            True,
                        )
                else:
                    # NSR identifiers are at byte +1 of a 2048-byte VRS
                    # sector.  Requiring BEA01 in the preceding descriptor
                    # avoids accepting an arbitrary PE payload containing the
                    # five ASCII bytes by coincidence.
                    base = marker_offset - 1
                    bea = base - _DISC_SECTOR_BYTES
                    if (
                        base >= 2
                        and bea >= 2
                        and data[bea + 1:bea + 6] == b"BEA01"
                    ):
                        return ArchiveMagic("udf", "archive", marker_offset, True)
                search_from = marker_offset + 1
        return None

    iso_end = _ISO9660_IDENTIFIER_OFFSET + len(_ISO9660_IDENTIFIER)
    iso_base = 16 * _DISC_SECTOR_BYTES
    if (
        len(data) >= iso_end + 1
        and data[_ISO9660_IDENTIFIER_OFFSET:iso_end] == _ISO9660_IDENTIFIER
        and data[iso_base] in {1, 2, 255}
        and data[_ISO9660_IDENTIFIER_OFFSET + len(_ISO9660_IDENTIFIER)] == 1
    ):
        return ArchiveMagic("iso", "archive", _ISO9660_IDENTIFIER_OFFSET)

    # ECMA-167/UDF volume recognition sequences normally begin with BEA01,
    # followed by NSR02 or NSR03.  NSR itself is the format identifier, so
    # recognize it even when a source starts in the middle of an otherwise
    # valid sequence, but only at the fixed descriptor offset of a sector.
    for sector in range(_UDF_VRS_FIRST_SECTOR + 1, _UDF_VRS_LAST_SECTOR + 1):
        offset = sector * _DISC_SECTOR_BYTES + 1
        end = offset + 5
        previous = (sector - 1) * _DISC_SECTOR_BYTES + 1
        if (
            len(data) >= end
            and data[offset:end] in _UDF_NAMESPACE_IDENTIFIERS
            and len(data) >= previous + 5
            and data[previous:previous + 5] == b"BEA01"
        ):
            return ArchiveMagic("udf", "archive", offset)

    # Some UDF images omit a readable VRS from the available prefix.  The
    # Anchor Volume Descriptor Pointer at sector 256 is a second bounded,
    # structured proof: tag id 2, descriptor version 2/3, and a matching tag
    # location.  Requiring all three fields avoids treating random bytes as an
    # image simply because they start with ``\x02\x00``.
    anchor = _UDF_ANCHOR_SECTOR * _DISC_SECTOR_BYTES
    if len(data) >= anchor + 16:
        tag_id = int.from_bytes(data[anchor:anchor + 2], "little")
        descriptor_version = int.from_bytes(data[anchor + 2:anchor + 4], "little")
        tag_location = int.from_bytes(data[anchor + 12:anchor + 16], "little")
        if tag_id == 2 and descriptor_version in {2, 3} and tag_location == _UDF_ANCHOR_SECTOR:
            return ArchiveMagic("udf", "archive", anchor)
    return None


def detect_magic(prefix: bytes | bytearray | memoryview, *, filename: str = "") -> ArchiveMagic:
    """Detect archive/media/executable magic without ever executing a file.

    ``filename`` is used only for the conservative fallback that distinguishes
    an unknown ``.exe``/``.bin`` from an ordinary unknown file.  Archive
    format is never inferred from a suffix alone.
    """

    if not isinstance(prefix, (bytes, bytearray, memoryview)):
        raise TypeError("prefix must be bytes-like")
    data = bytes(prefix)
    # Prefer a complete embedded ISO/UDF descriptor over a coincidental ``PK``
    # byte sequence in the wrapper's payload.  Ordinary MZ SFX ZIP/7z/RAR
    # remains covered by the signature loop immediately below.
    if data.startswith(b"MZ"):
        embedded_disc = _detect_disc_image_magic(data)
        if embedded_disc is not None:
            return embedded_disc
    for format_name, signature in _MAGIC_SIGNATURES:
        offset = _find_signature(data, signature, embedded=True)
        if offset is not None:
            return ArchiveMagic(
                format_name,
                "archive",
                offset,
                offset > 0,
            )
    disc_image = _detect_disc_image_magic(data)
    if disc_image is not None:
        return disc_image
    if data.startswith(_EBML_MAGIC):
        return ArchiveMagic("mkv", "media")
    if len(data) >= 8 and data[4:8] == b"ftyp":
        return ArchiveMagic("mp4", "media")
    if data.startswith(b"MZ"):
        return ArchiveMagic("exe", "executable")
    # Keep the unknown result explicit.  A caller may report it as a residual,
    # but an archive inspector must stop rather than trust a misleading suffix.
    return ArchiveMagic(None, "unknown")


def detect_archive_format(prefix: bytes | bytearray | memoryview, *, filename: str = "") -> str | None:
    """Compatibility-friendly string projection of :func:`detect_magic`."""

    detection = detect_magic(prefix, filename=filename)
    return detection.format if detection.is_archive else None


def _normalise_segment(segment: str) -> str:
    return unicodedata.normalize("NFC", segment)


def member_collision_key(path: str) -> str:
    """Return a filesystem-equivalence key used for archive collision checks."""

    normalized = str(path).replace("\\", "/")
    segments = []
    for segment in normalized.split("/"):
        segment = unicodedata.normalize("NFKC", segment).casefold()
        # APFS/Windows and common AList backends disagree on trailing dots and
        # spaces.  Treat those names as equivalent before extraction.
        segments.append(segment.rstrip(" ."))
    return "/".join(segments)


def normalize_member_path(value: Any) -> str:
    """Validate and normalize one archive member path.

    Backslashes are treated as separators so a Windows-created archive cannot
    smuggle ``..\\`` through a POSIX extractor.  Absolute paths, drive paths,
    ADS-style colons, control bytes, dot segments and trailing-dot/space names
    are rejected before a filesystem path is ever constructed.
    """

    if not isinstance(value, str) or not value:
        raise ArchivePathError("archive member path is empty")
    if "\x00" in value or _CONTROL_RE.search(value):
        raise ArchivePathError("archive member path contains control characters")
    raw = value.replace("\\", "/")
    if raw.startswith("/") or raw.startswith("//") or _DRIVE_PATH_RE.match(raw):
        raise ArchivePathError("archive member path is absolute")
    if raw.endswith("/"):
        raw = raw.rstrip("/")
    parts = raw.split("/")
    if not parts or any(part == "" for part in parts):
        raise ArchivePathError("archive member path is not normalized")
    normalized: list[str] = []
    for raw_part in parts:
        part = _normalise_segment(raw_part)
        if part in {".", ".."}:
            raise ArchivePathError("archive member path contains dot traversal")
        if part.endswith((".", " ")):
            raise ArchivePathError("archive member path has trailing dot/space")
        if ":" in part:
            raise ArchivePathError("archive member path contains a drive/ADS colon")
        if part.startswith("-"):
            # A selected member beginning with '-' could be parsed as a 7z
            # option when passed on the command line.
            raise ArchivePathError("archive member path begins with an option marker")
        if part.startswith("@") or any(char in part for char in "*?"):
            # 7-Zip treats an ``@``-prefixed argument as a list file and ``*``
            #/``?`` as wildcards.  Selected members are exact paths, so these
            # spellings are rejected rather than delegated to 7-Zip's parser.
            raise ArchivePathError("archive member path contains a 7-Zip pattern marker")
        if part.casefold() in _RESERVED_WINDOWS_NAMES:
            raise ArchivePathError("archive member path uses a reserved device name")
        normalized.append(part)
    result = "/".join(normalized)
    if posixpath.normpath(result) != result:
        raise ArchivePathError("archive member path is not normalized")
    return result


def safe_staging_path(root: Path, relative_path: str) -> Path:
    """Resolve a validated member below ``root`` without following links."""

    normalized = normalize_member_path(relative_path)
    root = Path(root)
    candidate = root.joinpath(*normalized.split("/"))
    try:
        candidate.resolve(strict=False).relative_to(root.resolve(strict=False))
    except ValueError as exc:
        raise ArchivePathError("archive output escapes task staging") from exc
    return candidate


def extract_password_markers(value: Any) -> tuple[str, ...]:
    """Extract explicitly labelled password values from a name/text value."""

    if not isinstance(value, str):
        return ()
    result: list[str] = []
    for match in _PASSWORD_MARKER_RE.finditer(value):
        candidate = match.group("password").strip()
        if candidate and "\x00" not in candidate and len(candidate) <= _MAX_PASSWORD_LENGTH:
            result.append(candidate)
    return tuple(result)


@dataclass(frozen=True, slots=True, repr=False)
class PasswordCandidate:
    """An in-memory password candidate whose repr never reveals its value."""

    value: str
    source: str

    def __post_init__(self) -> None:
        if not isinstance(self.value, str) or len(self.value) > _MAX_PASSWORD_LENGTH or "\x00" in self.value:
            raise ValueError("invalid archive password candidate")
        if not isinstance(self.source, str) or not self.source:
            raise ValueError("invalid archive password source")

    def __repr__(self) -> str:  # pragma: no cover - trivial defensive projection
        return f"PasswordCandidate(source={self.source!r}, value=<redacted>)"


def _unique_nonempty(values: Iterable[str]) -> list[str]:
    output: list[str] = []
    seen: set[str] = set()
    for value in values:
        if not isinstance(value, str):
            continue
        clean = value.strip()
        if not clean or len(clean) > _MAX_PASSWORD_LENGTH or "\x00" in clean:
            continue
        if clean not in seen:
            seen.add(clean)
            output.append(clean)
    return output


def _marker_values(values: Iterable[Any]) -> list[str]:
    result: list[str] = []
    for value in values:
        result.extend(extract_password_markers(value))
    return _unique_nonempty(result)


def password_candidates(
    *,
    retry_password: str | None = None,
    path: str | Path | None = None,
    sibling_names: Iterable[str] = (),
    source_tree_markers: Iterable[str] = (),
    parent_names: Iterable[str] = (),
    max_candidates: int = 5,
) -> tuple[PasswordCandidate, ...]:
    """Build a bounded, conflict-aware candidate sequence.

    Priority is temporary retry value, path marker, sibling marker, unique
    source-tree marker, then at most two explicit parent basenames.  Conflicting
    marker values at the same source level fail closed rather than guessing.
    A no-password attempt is always retained as the final candidate.
    """

    if isinstance(max_candidates, bool) or max_candidates < 1:
        raise ValueError("max_candidates must be positive")
    if retry_password is not None and not isinstance(retry_password, str):
        raise ValueError("retry_password must be a string or None")
    candidates: list[PasswordCandidate] = []
    seen: set[str] = set()

    def add(value: str, source: str) -> None:
        clean = value.strip()
        if not clean or clean in seen:
            return
        if len(clean) > _MAX_PASSWORD_LENGTH or "\x00" in clean:
            return
        candidates.append(PasswordCandidate(clean, source))
        seen.add(clean)

    if retry_password:
        add(retry_password, "retry")

    path_values = _marker_values([path] if path is not None else [])
    sibling_values = _marker_values(sibling_names)
    tree_values = _marker_values(source_tree_markers)
    explicit_values = _unique_nonempty([*path_values, *sibling_values, *tree_values])
    if len(explicit_values) > 1:
        # Explicit markers are assertions, not a password dictionary.  A
        # disagreeing path/sibling/tree hint is ambiguous, so stop instead of
        # trying every secret against the archive.
        raise ArchivePasswordConflict("conflicting explicit password markers")
    for values, source in (
        (path_values, "path-marker"),
        (sibling_values, "sibling-marker"),
        (tree_values, "source-tree-marker"),
    ):
        for value in values:
            add(value, source)

    for name in list(parent_names)[:2]:
        if isinstance(name, str):
            # Parent basenames are candidates only as literal names, never as
            # arbitrary path text.  They are still bounded and redacted.
            add(Path(name.replace("\\", "/")).name, "parent-basename")

    # Keep one no-password attempt.  Truncate before adding it so the bound is
    # strict even when every higher-priority source supplied a candidate.
    if len(candidates) >= max_candidates:
        candidates = candidates[: max_candidates - 1]
    candidates.append(PasswordCandidate("", "none"))
    return tuple(candidates)


def discover_password_candidates(
    path: str | Path,
    *,
    retry_password: str | None = None,
    sibling_names: Iterable[str] = (),
    source_tree_markers: Iterable[str] = (),
    max_candidates: int = 5,
) -> tuple[PasswordCandidate, ...]:
    """Convenience wrapper using the two nearest local parent basenames."""

    path_obj = Path(path)
    parents = [parent.name for parent in list(path_obj.parents) if parent.name][:2]
    return password_candidates(
        retry_password=retry_password,
        path=path_obj,
        sibling_names=sibling_names,
        source_tree_markers=source_tree_markers,
        parent_names=parents,
        max_candidates=max_candidates,
    )


# ---------------------------------------------------------------------------
# Member/volume models and validation


@dataclass(frozen=True, slots=True)
class ArchiveMember:
    path: str
    size: int = 0
    is_dir: bool = False
    encrypted: bool = False
    is_link: bool = False
    link_type: str | None = None
    compressed_size: int | None = None
    attributes: str = ""

    def __post_init__(self) -> None:
        object.__setattr__(self, "path", normalize_member_path(self.path))
        if isinstance(self.size, bool) or not isinstance(self.size, int) or self.size < 0:
            raise ArchiveMemberError("archive member size is invalid")
        if self.compressed_size is not None and (
            isinstance(self.compressed_size, bool)
            or not isinstance(self.compressed_size, int)
            or self.compressed_size < 0
        ):
            raise ArchiveMemberError("archive member packed size is invalid")

    @property
    def depth(self) -> int:
        return self.path.count("/") + 1

    @property
    def collision_key(self) -> str:
        return member_collision_key(self.path)

    @property
    def media_kind(self) -> str:
        if is_video_filename(self.path):
            return "video"
        if is_subtitle_filename(self.path):
            return "subtitle"
        if is_archive_filename(self.path):
            return "archive"
        return "other"

    def to_dict(self) -> dict[str, Any]:
        output: dict[str, Any] = {
            "path": self.path,
            "size": self.size,
            "is_dir": self.is_dir,
        }
        if self.encrypted:
            output["encrypted"] = True
        return output


def _mapping_value(raw: Mapping[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in raw:
            return raw[key]
    folded = {str(key).casefold(): value for key, value in raw.items()}
    for key in keys:
        if key.casefold() in folded:
            return folded[key.casefold()]
    return None


def _coerce_size(value: Any, *, allow_missing: bool = False) -> int | None:
    if value is None or (isinstance(value, str) and value in {"", "-", "?"}):
        return 0 if allow_missing else None
    if isinstance(value, bool):
        return None
    try:
        number = int(value)
    except (TypeError, ValueError):
        return None
    return number if number >= 0 else None


def member_from_mapping(raw: Mapping[str, Any]) -> ArchiveMember:
    """Convert AList/7z-like metadata to a validated :class:`ArchiveMember`."""

    if not isinstance(raw, Mapping):
        raise ArchiveMemberError("archive member is not an object")
    path = _mapping_value(raw, "path", "relative_path", "Path", "name")
    if not isinstance(path, str):
        raise ArchiveMemberError("archive member has no path")
    attributes = str(_mapping_value(raw, "attributes", "Attributes") or "")
    folder_value = _mapping_value(raw, "is_dir", "directory", "Folder", "folder", "type")
    is_dir = bool(
        folder_value is True
        or folder_value == "+"
        or (isinstance(folder_value, str) and folder_value.casefold() in {"d", "dir", "directory"})
        or attributes[:1].casefold() == "d"
        or path.endswith(("/", "\\"))
    )
    link_value = _mapping_value(
        raw,
        "is_link",
        "symlink",
        "symbolic_link",
        "symbolic link",
        "Symbolic Link",
        "hardlink",
        "hard_link",
        "Link",
        "HardLink",
        "Hard Link",
    )
    raw_type = str(_mapping_value(raw, "type", "Type") or "").casefold()
    link_text = str(link_value or "").casefold()
    explicit_link = link_text not in {"", "-", "0", "false", "none", "no"}
    is_link = (
        explicit_link
        or raw_type in {"link", "symlink", "symbolic_link", "hardlink", "hard_link"}
        or attributes[:1].casefold() in {"l", "h"}
    )
    link_type: str | None = None
    if is_link:
        link_type = (
            "hardlink"
            if (
                "hard" in link_text
                or "hard" in raw_type
                or attributes[:1].casefold() == "h"
                or any(str(key).casefold() == "hard link" for key in raw)
            )
            else "symlink"
        )
    size_value = _mapping_value(raw, "size", "Size", "uncompressed_size", "Uncompressed Size")
    size = _coerce_size(size_value, allow_missing=is_dir)
    if size is None:
        raise ArchiveMemberError("archive member size is invalid")
    packed = _mapping_value(raw, "compressed_size", "Packed Size", "packed_size", "Compressed Size")
    compressed_size = _coerce_size(packed, allow_missing=True) if packed is not None else None
    encrypted_value = _mapping_value(raw, "encrypted", "Encrypted")
    encrypted = bool(encrypted_value) and str(encrypted_value).casefold() not in {"-", "false", "0", "none"}
    return ArchiveMember(
        path=path,
        size=size,
        is_dir=is_dir,
        encrypted=encrypted,
        is_link=is_link,
        link_type=link_type,
        compressed_size=compressed_size,
        attributes=attributes,
    )


def _is_disc_format(archive_format: str | None) -> bool:
    return isinstance(archive_format, str) and archive_format.casefold() in {"iso", "udf"}


def _source_budget_bytes(limits: ArchiveLimits, archive_format: str | None) -> int:
    """Return the finite source-input ceiling for the proven container kind."""

    return limits.max_disc_image_bytes if _is_disc_format(archive_format) else limits.max_archive_bytes


def _member_budget_bytes(limits: ArchiveLimits, archive_format: str | None) -> int:
    return limits.max_disc_member_bytes if _is_disc_format(archive_format) else limits.max_member_bytes


def _expanded_budget_bytes(limits: ArchiveLimits, archive_format: str | None) -> int:
    return limits.max_disc_expanded_bytes if _is_disc_format(archive_format) else limits.max_expanded_bytes


def _expansion_ratio_limit(limits: ArchiveLimits, archive_format: str | None) -> float:
    return limits.max_disc_expansion_ratio if _is_disc_format(archive_format) else limits.max_expansion_ratio


def _command_timeout(limits: ArchiveLimits, archive_format: str | None) -> float:
    return limits.disc_command_timeout_seconds if _is_disc_format(archive_format) else limits.command_timeout_seconds


def validate_archive_members(
    members: Iterable[ArchiveMember | Mapping[str, Any]],
    *,
    limits: ArchiveLimits | None = None,
    archive_size: int | None = None,
    archive_format: str | None = None,
) -> tuple[ArchiveMember, ...]:
    """Validate member paths, links, collisions and expansion budgets."""

    policy = limits or ArchiveLimits()
    member_budget = _member_budget_bytes(policy, archive_format)
    expanded_budget = _expanded_budget_bytes(policy, archive_format)
    source_budget = _source_budget_bytes(policy, archive_format)
    ratio_budget = _expansion_ratio_limit(policy, archive_format)
    converted: list[ArchiveMember] = []
    for raw in members:
        member = raw if isinstance(raw, ArchiveMember) else member_from_mapping(raw)
        if member.is_link:
            raise ArchiveLinkError("archive contains a symbolic or hard link")
        if member.depth > policy.max_depth:
            raise ArchiveBudgetError("archive member directory depth exceeds limit")
        if not member.is_dir and member.size > member_budget:
            raise ArchiveBudgetError("archive member size exceeds limit")
        converted.append(member)
        if len(converted) > policy.max_members:
            raise ArchiveBudgetError("archive member count exceeds limit")

    keys: dict[str, ArchiveMember] = {}
    for member in converted:
        key = member.collision_key
        if not key or key in keys:
            raise ArchiveCollisionError("archive contains equivalent member paths")
        keys[key] = member

    # A file cannot also be an ancestor directory.  This catches both
    # ``a``/``a/b`` and case/NFC-equivalent variants.
    for member in converted:
        parts = member.collision_key.split("/")
        for index in range(1, len(parts)):
            ancestor = "/".join(parts[:index])
            prior = keys.get(ancestor)
            if prior is not None and not prior.is_dir:
                raise ArchiveCollisionError("archive file/directory paths collide")

    expanded = sum(member.size for member in converted if not member.is_dir)
    if expanded > expanded_budget:
        raise ArchiveBudgetError("archive expanded size exceeds limit")
    if archive_size is not None:
        if isinstance(archive_size, bool) or not isinstance(archive_size, int) or archive_size <= 0:
            raise ArchiveBudgetError("archive size is invalid")
        if archive_size > source_budget:
            raise ArchiveBudgetError("archive size exceeds limit")
        if expanded and expanded / archive_size > ratio_budget:
            raise ArchiveBudgetError("archive expansion ratio exceeds limit")
    return tuple(sorted(converted, key=lambda item: (item.collision_key, item.is_dir, item.size)))


def select_media_members(
    members: Iterable[ArchiveMember],
    selected: Sequence[str | ArchiveMember] | None = None,
) -> tuple[ArchiveMember, ...]:
    """Select only video/subtitle files; attachments and nested archives stay put."""

    all_members = tuple(members)
    by_path = {member.path: member for member in all_members}
    if selected is None:
        chosen = [
            member
            for member in all_members
            if not member.is_dir and member.media_kind in {"video", "subtitle"}
        ]
    else:
        chosen = []
        seen: set[str] = set()
        for value in selected:
            path = value.path if isinstance(value, ArchiveMember) else value
            if not isinstance(path, str):
                raise ArchiveMemberError("selected archive member path is invalid")
            normalized = normalize_member_path(path)
            if normalized in seen:
                raise ArchiveCollisionError("selected archive member is duplicated")
            seen.add(normalized)
            member = by_path.get(normalized)
            if member is None:
                raise ArchiveMemberError("selected archive member is not in the listing")
            if member.is_dir or member.media_kind not in {"video", "subtitle"}:
                raise ArchiveMemberError("only video and subtitle members may be extracted")
            chosen.append(member)
    nested = [member for member in chosen if is_archive_filename(member.path)]
    if selected is None and not chosen:
        nested = [
            member
            for member in all_members
            if not member.is_dir and is_archive_filename(member.path)
        ]
    if nested:
        raise NestedArchiveUnsupported("nested archive member requires a separate bounded review")
    if not chosen:
        raise ArchiveMemberError("archive contains no selected video or subtitle")
    return tuple(chosen)


# Volume parsing is pure and accepts names from either a local directory or a
# remote AList listing.
_VOLUME_7Z_RE = re.compile(r"^(?P<base>.+)\.(?P<format>7z|zip)\.(?P<index>\d{3,4})$", re.I)
_VOLUME_RAR_PART_RE = re.compile(r"^(?P<base>.+)\.part(?P<index>\d{1,4})\.rar$", re.I)
# The residual/media policy still classifies legacy ``.r00`` files as archive
# remnants so they remain visible to audit.  The active archive inspector does
# not claim support for that old RAR convention: unlike ``.part01.rar`` it
# cannot safely establish the first volume and complete set from the current
# AList/local ports.  Keep the detector only to fail closed explicitly.
_UNSUPPORTED_RAR_OLD_RE = re.compile(r"^(?P<base>.+)\.r(?P<index>\d{2,3})$", re.I)


def _legacy_rar_group(name: str) -> str | None:
    normalized = str(name).replace("\\", "/").rsplit("/", 1)[-1]
    match = _UNSUPPORTED_RAR_OLD_RE.fullmatch(normalized)
    return f"{match.group('base').casefold()}.rar" if match else None


@dataclass(frozen=True, slots=True)
class VolumeName:
    name: str
    group: str
    format: str
    index: int


def volume_name_descriptor(name: str) -> VolumeName | None:
    if not isinstance(name, str) or not name:
        return None
    normalized = name.replace("\\", "/").rsplit("/", 1)[-1]
    match = _VOLUME_7Z_RE.fullmatch(normalized)
    if match:
        fmt = match.group("format").casefold()
        base = match.group("base")
        return VolumeName(normalized, f"{base.casefold()}.{fmt}", fmt, int(match.group("index")))
    match = _VOLUME_RAR_PART_RE.fullmatch(normalized)
    if match:
        base = match.group("base")
        return VolumeName(normalized, f"{base.casefold()}.part.rar", "rar", int(match.group("index")))
    if _UNSUPPORTED_RAR_OLD_RE.fullmatch(normalized):
        return None
    suffix = extension(normalized)
    if suffix == ".rar":
        base = normalized[:-4]
        return VolumeName(normalized, f"{base.casefold()}.rar", "rar", 0)
    return None


def validate_volume_names(names: Iterable[str], first_name: str | None = None) -> tuple[str, ...]:
    """Return a deterministic, contiguous volume sequence or fail closed."""

    materialized_names = tuple(str(name) for name in names)
    first_descriptor = volume_name_descriptor(first_name) if first_name is not None else None
    first_legacy_group = _legacy_rar_group(first_name) if first_name is not None else None
    if first_legacy_group is not None:
        raise ArchiveVolumeError("legacy RAR .r00 volumes are unsupported")
    descriptors = [
        descriptor
        for name in materialized_names
        if (descriptor := volume_name_descriptor(name))
    ]
    if first_descriptor is None and first_name is None and not descriptors:
        # A caller asking to validate an otherwise untyped set gets an explicit
        # failure for legacy RAR rather than a false claim that it is a normal
        # single-file archive.
        if any(_legacy_rar_group(name) is not None for name in materialized_names):
            raise ArchiveVolumeError("legacy RAR .r00 volumes are unsupported")
    if first_name is not None and not descriptors:
        raise ArchiveVolumeError("archive volume set is empty")
    if not descriptors:
        return materialized_names
    if first_name is None:
        first_name = descriptors[0].name
    first = first_descriptor or volume_name_descriptor(first_name)
    if first is None:
        return (first_name,)
    group = [item for item in descriptors if item.group == first.group]
    if first.format == "rar":
        # Ignore unrelated legacy remnants in the same directory, but reject
        # them when they belong to this archive's basename.
        if any(_legacy_rar_group(name) == first.group for name in materialized_names):
            raise ArchiveVolumeError("legacy RAR .r00 volumes are unsupported")
    if not group:
        raise ArchiveVolumeError("archive first volume is missing")
    by_index: dict[int, VolumeName] = {}
    for item in group:
        if item.index in by_index:
            raise ArchiveVolumeError("archive volume names collide")
        by_index[item.index] = item
    lowest = min(by_index)
    if first.format in {"7z", "zip"} and lowest != 1:
        raise ArchiveVolumeError("split archive is missing the first volume")
    if first.format == "rar" and lowest not in {0, 1}:
        raise ArchiveVolumeError("RAR archive is missing the first volume")
    expected = list(range(lowest, max(by_index) + 1))
    if sorted(by_index) != expected:
        raise ArchiveVolumeError("archive volumes are not contiguous")
    return tuple(by_index[index].name for index in expected)


def discover_volume_paths(path: str | Path) -> tuple[Path, ...]:
    """Discover sibling split volumes for one local archive path."""

    archive_path = Path(path)
    if archive_path.is_symlink() or not archive_path.is_file():
        raise ArchiveVolumeError("archive source is not a regular file")
    if _UNSUPPORTED_RAR_OLD_RE.fullmatch(archive_path.name):
        raise ArchiveVolumeError("legacy RAR .r00 volumes are unsupported")
    descriptor = volume_name_descriptor(archive_path.name)
    if descriptor is None:
        return (archive_path,)
    siblings: list[Path] = []
    legacy_group = _legacy_rar_group(archive_path.name)
    for candidate in archive_path.parent.iterdir():
        if candidate.is_symlink() or not candidate.is_file():
            continue
        if legacy_group is not None and _legacy_rar_group(candidate.name) == legacy_group:
            raise ArchiveVolumeError("legacy RAR .r00 volumes are unsupported")
        other = volume_name_descriptor(candidate.name)
        if other is not None and other.group == descriptor.group:
            siblings.append(candidate)
    if any(_legacy_rar_group(candidate.name) == descriptor.group for candidate in archive_path.parent.iterdir() if candidate.is_file() and not candidate.is_symlink()):
        raise ArchiveVolumeError("legacy RAR .r00 volumes are unsupported")
    names = validate_volume_names((item.name for item in siblings), archive_path.name)
    by_name = {item.name.casefold(): item for item in siblings}
    return tuple(by_name[name.casefold()] for name in names)


# ---------------------------------------------------------------------------
# 7-Zip runner and listing parser


@dataclass(frozen=True, slots=True)
class RunnerResult:
    returncode: int
    stdout: str = ""
    stderr: str = ""


class ArchiveRunner(Protocol):
    def run(
        self,
        args: Sequence[str],
        *,
        password: str = "",
        cwd: Path | None = None,
        timeout: float = 900,
    ) -> RunnerResult:
        """Run argv without a shell and return bounded text output."""


class Subprocess7zRunner:
    """Production local runner.  It never invokes a shell or executes members."""

    def __init__(
        self,
        executable: str | None = None,
        *,
        max_output_bytes: int = 4 * 1024 * 1024,
        max_timeout_seconds: float = 2 * 60 * 60,
    ):
        self.executable = executable or shutil.which("7z") or shutil.which("7zz")
        self.max_output_bytes = max_output_bytes
        if (
            isinstance(max_timeout_seconds, bool)
            or not isinstance(max_timeout_seconds, (int, float))
            or not math.isfinite(float(max_timeout_seconds))
            or max_timeout_seconds <= 0
        ):
            raise ValueError("max_timeout_seconds must be positive")
        self.max_timeout_seconds = float(max_timeout_seconds)

    def run(
        self,
        args: Sequence[str],
        *,
        password: str = "",
        cwd: Path | None = None,
        timeout: float = 900,
    ) -> RunnerResult:
        if not self.executable:
            raise ArchiveToolError("7-Zip executable is unavailable")
        if any(not isinstance(arg, str) or "\x00" in arg for arg in args):
            raise ArchiveToolError("7-Zip arguments are invalid")
        argv = [self.executable, *args]
        input_data: bytes | None = None
        if password:
            # ``-p*`` asks 7-Zip for a password on stdin.  Passing the value as
            # ``-pSECRET`` would expose it through ``ps``/process inspection;
            # stdin keeps the argv and all durable diagnostics secret-free.
            argv.insert(1, "-p*")
            input_data = (password + "\n").encode("utf-8")
        try:
            completed = subprocess.run(
                argv,
                cwd=str(cwd) if cwd is not None else None,
                input=input_data,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                # ArchiveLimits supplies the per-container timeout; this
                # process-level ceiling prevents a malformed caller from
                # disabling it.  A 45–50 GiB optical image may legitimately
                # need longer than the old 30-minute ceiling while still
                # remaining bounded.
                timeout=min(float(timeout), self.max_timeout_seconds),
                check=False,
            )
        except FileNotFoundError as exc:
            raise ArchiveToolError("7-Zip executable is unavailable") from exc
        except subprocess.TimeoutExpired as exc:
            raise ArchiveToolError("7-Zip operation timed out") from exc
        stdout = bytes(completed.stdout or b"")[: self.max_output_bytes].decode("utf-8", "replace")
        stderr = bytes(completed.stderr or b"")[: self.max_output_bytes].decode("utf-8", "replace")
        return RunnerResult(int(completed.returncode), stdout, stderr)


def _coerce_runner_result(raw: Any) -> RunnerResult:
    if isinstance(raw, RunnerResult):
        return raw
    if isinstance(raw, tuple) and len(raw) >= 2:
        return RunnerResult(int(raw[0]), str(raw[1] or ""), str(raw[2] if len(raw) > 2 else ""))
    if isinstance(raw, Mapping):
        return RunnerResult(int(raw.get("returncode", raw.get("code", 1))), str(raw.get("stdout", "") or ""), str(raw.get("stderr", "") or ""))
    return RunnerResult(
        int(getattr(raw, "returncode", 1)),
        str(getattr(raw, "stdout", "") or ""),
        str(getattr(raw, "stderr", "") or ""),
    )


def _run_archive_tool(
    runner: ArchiveRunner | Callable[..., Any],
    args: Sequence[str],
    *,
    password: str,
    cwd: Path | None,
    timeout: float,
) -> RunnerResult:
    run = getattr(runner, "run", runner)
    try:
        raw = run(args, password=password, cwd=cwd, timeout=timeout)
    except TypeError:
        # Tiny fake runners often expose only ``run(args)``.  Supporting that
        # shape keeps tests isolated without weakening the production port.
        raw = run(args)
    return _coerce_runner_result(raw)


def parse_7z_slt_listing(
    output: str,
    *,
    limits: ArchiveLimits | None = None,
    archive_format: str | None = None,
) -> tuple[ArchiveMember, ...]:
    """Parse the stable ``7z l -slt`` key/value form and validate it."""

    if not isinstance(output, str):
        raise ArchiveListingError("7-Zip listing output is not text")
    records: list[dict[str, str]] = []
    current: dict[str, str] = {}
    for line in output.splitlines():
        stripped = line.strip("\ufeff\r\n")
        if not stripped:
            if current:
                records.append(current)
                current = {}
            continue
        if stripped.startswith("----------"):
            if current:
                records.append(current)
                current = {}
            continue
        key, separator, value = stripped.partition(" = ")
        if not separator:
            # Localized banners and warnings are ignored; member records must
            # still contain a Path and are validated below.
            continue
        current[key.strip()] = value
    if current:
        records.append(current)

    members: list[ArchiveMember] = []
    for record in records:
        path = record.get("Path")
        if not path:
            continue
        # The first ``-slt`` record describes the archive container itself.
        if "Type" in record and "Attributes" not in record and "Folder" not in record:
            continue
        try:
            members.append(member_from_mapping(record))
        except ArchiveError:
            raise
        except Exception as exc:  # pragma: no cover - defensive parser guard
            raise ArchiveListingError("7-Zip listing contains an invalid member") from exc
    if not members:
        raise ArchiveListingError("7-Zip listing contains no members")
    return validate_archive_members(
        members,
        limits=limits,
        archive_format=archive_format,
    )


# Common aliases useful to callers migrating old naming without importing a
# second archive implementation.
parse_7z_listing = parse_7z_slt_listing
validate_members = validate_archive_members


# ---------------------------------------------------------------------------
# Listing/extraction models and local inspector


@dataclass(frozen=True, slots=True)
class ArchiveListing:
    archive_path: Path
    archive_format: str
    volumes: tuple[Path, ...]
    members: tuple[ArchiveMember, ...]
    archive_size: int
    encrypted: bool = False
    password_source: str = "none"
    _password: str = field(default="", repr=False, compare=False)
    _password_candidates: tuple[PasswordCandidate, ...] = field(default=(), repr=False, compare=False)

    @property
    def format(self) -> str:
        return self.archive_format

    @property
    def selected_media(self) -> tuple[ArchiveMember, ...]:
        return select_media_members(self.members)

    def to_dict(self) -> dict[str, Any]:
        """A safe projection suitable for a job JSON/UI response."""

        secrets = [self._password] if self._password else []
        secrets.extend(
            candidate.value
            for candidate in self._password_candidates
            if isinstance(candidate, PasswordCandidate) and candidate.value
        )

        def safe_path(value: Path) -> str:
            text = str(value)
            # A path can itself contain an explicit marker (for example a
            # directory named ``密码:...``).  Never expose either that marker
            # value or the successfully used in-memory password in a durable
            # projection.
            marker_values = extract_password_markers(text)
            for secret in [*secrets, *marker_values]:
                if secret:
                    text = text.replace(secret, "<redacted>")
            return text

        return {
            "archive_path": safe_path(self.archive_path),
            "archive_format": self.archive_format,
            "volumes": [safe_path(path) for path in self.volumes],
            "archive_size": self.archive_size,
            "encrypted": self.encrypted,
            "password_source": self.password_source,
            "members": [member.to_dict() for member in self.members],
        }


ArchiveInspection = ArchiveListing


def _ensure_archive_file(path: Path) -> None:
    if path.is_symlink() or not path.is_file():
        raise ArchiveFormatError("archive source is not a regular file")
    try:
        if path.stat().st_size <= 0:
            raise ArchiveFormatError("archive source is empty")
    except OSError as exc:
        raise ArchiveFormatError("archive source cannot be read") from exc


def _require_archive_magic(prefix: bytes | bytearray | memoryview, *, filename: str) -> ArchiveMagic:
    """Return archive magic or raise one stable, non-executing failure."""

    detection = detect_magic(prefix, filename=filename)
    if detection.is_archive:
        return detection
    if detection.kind == "executable":
        raise ArchiveMagicError("executable source is not an archive")
    if detection.kind == "media":
        raise ArchiveMagicError("source magic identifies media, not an archive")
    raise ArchiveMagicError("archive magic is unknown")


def _check_disk_budget(staging_root: Path, expected_bytes: int, limits: ArchiveLimits) -> None:
    try:
        usage = shutil.disk_usage(staging_root)
    except OSError as exc:
        raise ArchiveBudgetError("cannot inspect task-staging disk space") from exc
    if usage.free - expected_bytes < limits.min_free_bytes:
        raise ArchiveBudgetError("task-staging disk reserve is insufficient")


class ArchiveInspector:
    """Read-only local archive inspector backed by one 7-Zip runner."""

    def __init__(self, runner: ArchiveRunner | Callable[..., Any] | None = None, *, limits: ArchiveLimits | None = None):
        self.runner = runner or Subprocess7zRunner()
        self.limits = limits or ArchiveLimits()

    def inspect(
        self,
        archive_path: str | Path,
        *,
        password_candidates: Iterable[PasswordCandidate | str] | None = None,
        password: str | None = None,
        pause_checkpoint: Callable[[], None] | None = None,
    ) -> ArchiveListing:
        """List one local archive, retrying only a bounded candidate set."""

        raw_path = Path(archive_path)
        if raw_path.is_symlink():
            raise ArchiveFormatError("archive source is not a regular file")
        path = raw_path.resolve(strict=False)
        _ensure_archive_file(path)
        try:
            with path.open("rb") as source_file:
                prefix = source_file.read(self.limits.max_magic_scan_bytes)
        except OSError as exc:
            raise ArchiveFormatError("archive source cannot be read") from exc
        detection = _require_archive_magic(prefix, filename=path.name)
        volumes = discover_volume_paths(path)
        if volumes[0].name.casefold() != path.name.casefold():
            # Calling the second volume directly is ambiguous and can bypass a
            # missing-first-volume check.
            raise ArchiveVolumeError("archive must be opened from its first volume")
        archive_size = sum(volume.stat().st_size for volume in volumes)
        if archive_size > _source_budget_bytes(self.limits, detection.format):
            raise ArchiveBudgetError("archive size exceeds limit")

        candidates: list[PasswordCandidate] = []
        if password is not None:
            candidates.append(PasswordCandidate(password, "explicit"))
        if password_candidates is None and password is None:
            # Local sources can use the same bounded marker/parent policy as
            # the remote adapter without making the caller remember a second
            # discovery step.  The sibling scan is names-only; file contents
            # are never searched here and no candidate is persisted.
            try:
                sibling_names = [
                    item.name
                    for item in path.parent.iterdir()
                    if not item.is_symlink() and item.is_file()
                ]
            except OSError:
                sibling_names = []
            password_candidates = discover_password_candidates(
                path,
                sibling_names=sibling_names,
                max_candidates=self.limits.max_password_candidates,
            )
        if password_candidates is not None:
            for item in password_candidates:
                candidate = item if isinstance(item, PasswordCandidate) else PasswordCandidate(str(item), "candidate")
                if candidate.value not in {row.value for row in candidates}:
                    candidates.append(candidate)
                if len(candidates) >= self.limits.max_password_candidates:
                    break
        if not candidates:
            candidates = [PasswordCandidate("", "none")]

        args = (
            "l",
            "-slt",
            "-sccUTF-8",
            "-y",
            str(volumes[0]),
        )
        last_result: RunnerResult | None = None
        for candidate_index, candidate in enumerate(candidates[: self.limits.max_password_candidates]):
            if pause_checkpoint is not None:
                pause_checkpoint()
            result = _run_archive_tool(
                self.runner,
                args,
                password=candidate.value,
                cwd=path.parent,
                timeout=_command_timeout(self.limits, detection.format),
            )
            last_result = result
            if result.returncode != 0:
                continue
            members = parse_7z_slt_listing(
                result.stdout,
                limits=self.limits,
                archive_format=detection.format,
            )
            validate_archive_members(
                members,
                limits=self.limits,
                archive_size=archive_size,
                archive_format=detection.format,
            )
            encrypted = any(member.encrypted for member in members)
            return ArchiveListing(
                archive_path=path,
                archive_format=detection.format or "unknown",
                volumes=volumes,
                members=members,
                archive_size=archive_size,
                encrypted=encrypted,
                password_source=candidate.source,
                _password=candidate.value,
                # 7-Zip can list an archive before it has proven the password
                # against every encrypted payload.  Keep only the successful
                # listing candidate and its bounded, untried successors in
                # memory so extraction can safely continue if needed.
                _password_candidates=tuple(candidates[candidate_index:self.limits.max_password_candidates]),
            )
        # Never include stderr/stdout: both may contain a password or a
        # command line echoed by an external 7z wrapper.
        del last_result
        raise ArchivePasswordError("archive listing failed; password candidates exhausted")

    def inspect_remote(
        self,
        source: "ArchiveSource",
        remote_path: str,
        task_staging: str | Path,
        *,
        password_candidates: Iterable[PasswordCandidate | str] | None = None,
        password: str | None = None,
        pause_checkpoint: Callable[[], None] | None = None,
    ) -> ArchiveListing:
        """Download source volumes into task staging, then inspect locally."""

        remote = _normalize_remote_path(remote_path)
        parent, name = posixpath.split(remote)
        if _UNSUPPORTED_RAR_OLD_RE.fullmatch(name):
            raise ArchiveVolumeError("legacy RAR .r00 volumes are unsupported")
        # Probe the source before allocating a task-staging directory or
        # downloading any volume.  Local inspection below repeats this check
        # after download, so a remote adapter cannot substitute bytes between
        # the two boundaries.
        try:
            if pause_checkpoint is not None:
                pause_checkpoint()
            prefix = source.read_prefix(remote, max_bytes=self.limits.max_magic_scan_bytes)
        except ArchivePauseRequested:
            raise
        except Exception as exc:
            raise ArchiveFormatError("archive source prefix cannot be read") from exc
        if not isinstance(prefix, (bytes, bytearray, memoryview)):
            raise ArchiveFormatError("archive source prefix is invalid")
        detection = _require_archive_magic(
            bytes(prefix)[: self.limits.max_magic_scan_bytes],
            filename=name,
        )
        if pause_checkpoint is not None:
            pause_checkpoint()
        entries = list(source.list(parent or "/"))
        if len(entries) > self.limits.max_source_entries:
            raise ArchiveBudgetError("archive source directory listing exceeds limit")
        file_entries = [
            entry for entry in entries
            if isinstance(entry, Mapping)
            and not entry.get("is_dir")
            and isinstance(entry.get("name"), str)
            and entry.get("name")
        ]
        names = [str(entry["name"]) for entry in file_entries]
        volume_names = validate_volume_names(names, name) if volume_name_descriptor(name) else (name,)
        by_name: dict[str, Mapping[str, Any]] = {}
        for entry in file_entries:
            key = str(entry["name"]).casefold()
            if key in by_name:
                raise ArchiveVolumeError("remote archive volume names collide")
            by_name[key] = entry
        downloads: list[tuple[str, int]] = []
        total_size = 0
        for volume_name in volume_names:
            entry = by_name.get(volume_name.casefold())
            if entry is None:
                raise ArchiveVolumeError("remote archive volume is missing")
            size = _coerce_size(entry.get("size"), allow_missing=False)
            if size is None or size <= 0:
                raise ArchiveVolumeError("remote archive volume has no valid size")
            total_size += size
            if total_size > _source_budget_bytes(self.limits, detection.format):
                raise ArchiveBudgetError("archive size exceeds limit")
            downloads.append((volume_name, size))

        # Downloaded inputs themselves consume the same local task disk as
        # later extraction.  Reserve capacity before the first volume rather
        # than discovering a full disk halfway through a split archive.
        if pause_checkpoint is not None:
            pause_checkpoint()
        staging = _prepare_remote_input_root(
            Path(task_staging),
            pause_checkpoint=pause_checkpoint,
        )
        _check_disk_budget(staging, total_size, self.limits)
        local_paths: list[Path] = []
        for volume_name, size in downloads:
            if pause_checkpoint is not None:
                pause_checkpoint()
            local = staging / volume_name
            _download_source(
                source,
                posixpath.join(parent or "/", volume_name),
                local,
                size,
                pause_checkpoint=pause_checkpoint,
            )
            local_paths.append(local)
        if pause_checkpoint is not None:
            pause_checkpoint()
        return self.inspect(
            local_paths[0],
            password_candidates=password_candidates,
            password=password,
            pause_checkpoint=pause_checkpoint,
        )


def _prepare_remote_input_root(
    root: Path,
    *,
    pause_checkpoint: Callable[[], None] | None = None,
) -> Path:
    if root.exists():
        if root.is_symlink() or not root.is_dir():
            raise ArchivePathError("task staging input root is not a directory")
        if any(root.iterdir()):
            raise ArchivePathError("task staging input root must be empty")
    else:
        if pause_checkpoint is not None:
            pause_checkpoint()
        root.mkdir(parents=True, mode=0o700)
    return root


def _normalize_remote_path(value: str) -> str:
    if not isinstance(value, str) or "\x00" in value:
        raise ArchivePathError("remote archive path is invalid")
    raw = value.replace("\\", "/")
    if not raw.startswith("/"):
        raw = "/" + raw
    normalized = posixpath.normpath(raw)
    if normalized != raw or any(part in {".", ".."} for part in raw.split("/")):
        raise ArchivePathError("remote archive path is not normalized")
    return normalized


def _download_source(
    source: "ArchiveSource",
    remote_path: str,
    destination: Path,
    expected_size: int,
    *,
    pause_checkpoint: Callable[[], None] | None = None,
) -> None:
    if pause_checkpoint is not None:
        pause_checkpoint()
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.exists() and destination.is_symlink():
        raise ArchivePathError("task staging destination is a symbolic link")
    method = getattr(source, "download", None)
    if method is None:
        method = getattr(source, "download_file_to_path", None)
    if method is None:
        raise ArchiveToolError("archive source has no bounded download method")
    if pause_checkpoint is not None:
        pause_checkpoint()
    try:
        method(remote_path, destination, expected_size=expected_size)
    except TypeError:
        # Keep the old positional compatibility form, but never replay a
        # download after a RootJob fence has changed while the first call was
        # rejected for its signature.
        if pause_checkpoint is not None:
            pause_checkpoint()
        method(remote_path, destination, expected_size)
    try:
        actual = destination.stat().st_size
    except OSError as exc:
        raise ArchiveExtractionError("downloaded archive volume is unavailable") from exc
    if actual != expected_size:
        raise ArchiveExtractionError("downloaded archive volume size mismatch")


class ArchiveSource(Protocol):
    def list(self, path: str) -> Sequence[Mapping[str, Any]]: ...

    def read_prefix(self, path: str, *, max_bytes: int) -> bytes: ...

    def download(self, path: str, destination: Path, *, expected_size: int) -> None: ...


class AListArchiveSource:
    """Adapter over the existing AList client; no new remote archive API."""

    def __init__(self, client: Any):
        self.client = client

    def list(self, path: str) -> Sequence[Mapping[str, Any]]:
        return self.client.list(path, refresh=True)

    def read_prefix(self, path: str, *, max_bytes: int) -> bytes:
        return self.client.read_file_prefix(path, max_bytes=max_bytes)

    def download(self, path: str, destination: Path, *, expected_size: int) -> None:
        self.client.download_file_to_path(path, destination, expected_size=expected_size)


# ---------------------------------------------------------------------------
# Extraction and content validators


def validate_subtitle_file(path: str | Path, *, max_bytes: int = 64 * 1024 * 1024) -> bool:
    """Perform bounded, format-aware subtitle validation without hashing."""

    file_path = Path(path)
    try:
        size = file_path.stat().st_size
        if size <= 0 or size > max_bytes or file_path.is_symlink():
            return False
        data = file_path.read_bytes()[:max_bytes]
    except OSError:
        return False
    suffix = extension(file_path.name)
    if suffix in {".sup", ".mks"}:
        return bool(data)
    try:
        text = data.decode("utf-8-sig")
    except UnicodeDecodeError:
        try:
            text = data.decode("utf-16")
        except UnicodeDecodeError:
            return False
    if "\x00" in text:
        return False
    if suffix in {".ass", ".ssa"}:
        return bool(re.search(r"(?im)^\s*\[events\]\s*$", text) and re.search(r"(?im)^\s*dialogue\s*:", text))
    if suffix == ".vtt":
        return text.lstrip("\ufeff \t\r\n").startswith("WEBVTT")
    if suffix == ".srt":
        return bool(re.search(r"\d{1,2}:\d{2}:\d{2}[,.]\d{3}\s*-->\s*\d{1,2}:\d{2}:\d{2}[,.]\d{3}", text))
    if suffix in {".sub", ".idx"}:
        return bool(text.strip())
    return bool(text.strip())


def _default_video_validator(path: Path) -> bool:
    ffprobe = shutil.which("ffprobe")
    try:
        size = path.stat().st_size
    except OSError:
        return False
    if not ffprobe or not video_size_is_admissible(size):
        return False
    try:
        completed = subprocess.run(
            [ffprobe, "-v", "error", "-show_entries", "stream=codec_type", "-of", "json", str(path)],
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
            timeout=120,
            check=False,
        )
        payload = json.loads(completed.stdout or "{}") if completed.returncode == 0 else {}
    except (OSError, ValueError, subprocess.TimeoutExpired):
        return False
    streams = payload.get("streams") if isinstance(payload, Mapping) else None
    return isinstance(streams, list) and any(
        isinstance(stream, Mapping) and stream.get("codec_type") == "video" for stream in streams
    )


def _call_validator(validator: Callable[..., Any], path: Path, member: ArchiveMember) -> bool:
    try:
        result = validator(path, member)
    except TypeError:
        result = validator(path)
    return bool(result)


@dataclass(frozen=True, slots=True)
class ArchiveExtractionResult:
    staging_root: Path
    files: tuple[Path, ...]
    members: tuple[ArchiveMember, ...]
    password_source: str = "none"

    def to_dict(self) -> dict[str, Any]:
        def safe_path(value: Path) -> str:
            text = str(value)
            for secret in extract_password_markers(text):
                text = text.replace(secret, "<redacted>")
            return text

        return {
            "staging_root": safe_path(self.staging_root),
            "files": [safe_path(path) for path in self.files],
            "members": [member.to_dict() for member in self.members],
            "password_source": self.password_source,
        }


def _extraction_password_candidates(
    listing: ArchiveListing,
    password: str | None,
    *,
    max_candidates: int,
) -> tuple[PasswordCandidate, ...]:
    """Return the bounded candidate tail that still needs extraction proof.

    An explicit ``extract(..., password=...)`` is deliberately one attempt:
    it overrides the inspector's in-memory discovery sequence.  Otherwise an
    inspected listing carries the candidate that listed successfully plus only
    candidates that have not yet been tried.  This avoids retrying known-bad
    values while allowing a header-visible encrypted archive to fall through
    to the next bounded candidate.
    """

    if password is not None:
        return (PasswordCandidate(password, "explicit"),)
    candidates: list[PasswordCandidate] = []
    seen: set[str] = set()
    for candidate in listing._password_candidates:
        if not isinstance(candidate, PasswordCandidate) or candidate.value in seen:
            continue
        candidates.append(candidate)
        seen.add(candidate.value)
        if len(candidates) >= max_candidates:
            break
    if candidates:
        return tuple(candidates)
    source = listing.password_source if isinstance(listing.password_source, str) and listing.password_source else "none"
    return (PasswordCandidate(listing._password, source),)


def _extraction_failure_kind(result: RunnerResult, *, encrypted: bool) -> str:
    """Classify 7-Zip output without surfacing it in diagnostics or logs."""

    text = "\n".join((result.stdout, result.stderr))
    if _PASSWORD_FAILURE_RE.search(text):
        return "password"
    # Several 7-Zip builds report only ``Data Error in encrypted file`` for a
    # wrong candidate.  That wording is not enough to call the archive corrupt
    # while later bounded candidates remain, so leave it retryable/unknown.
    if (
        encrypted
        and "encrypted" in text.casefold()
        and re.search(r"(?:data\s+error|crc\s+failed)", text, re.I)
    ):
        return "unknown"
    if _CORRUPTION_FAILURE_RE.search(text):
        return "corrupt"
    return "unknown"


def _require_empty_staging_for_password_retry(staging: Path) -> None:
    """Refuse retries when a failed tool invocation left any output behind.

    The extractor never deletes failed output in order to make room for a
    second password.  Leaving task-owned staging intact is both safer for
    investigation and prevents a crafted first attempt from influencing a
    later ``-aos`` extraction through partial files or links.
    """

    try:
        if staging.is_symlink() or not staging.is_dir():
            raise ArchiveExtractionError("failed extraction changed task staging root")
        if any(staging.iterdir()):
            raise ArchiveExtractionError("failed extraction left task staging non-empty")
    except OSError as exc:
        raise ArchiveExtractionError("cannot inspect task staging after failed extraction") from exc


class ArchiveExtractor:
    """Extract selected media into a fresh task staging directory."""

    def __init__(
        self,
        runner: ArchiveRunner | Callable[..., Any] | None = None,
        *,
        limits: ArchiveLimits | None = None,
        video_validator: Callable[..., Any] | None = None,
        subtitle_validator: Callable[..., Any] | None = None,
    ):
        self.runner = runner or Subprocess7zRunner()
        self.limits = limits or ArchiveLimits()
        self.video_validator = video_validator or _default_video_validator
        self.subtitle_validator = subtitle_validator or validate_subtitle_file

    def extract(
        self,
        listing: ArchiveListing,
        staging_root: str | Path,
        *,
        selected: Sequence[str | ArchiveMember] | None = None,
        password: str | None = None,
        pause_checkpoint: Callable[[], None] | None = None,
    ) -> ArchiveExtractionResult:
        if not isinstance(listing, ArchiveListing) or not listing.volumes:
            raise ArchiveExtractionError("archive listing is invalid")
        # ``ArchiveListing`` is a public dataclass, so callers can construct
        # one without going through ``ArchiveInspector``.  Re-check the
        # source files and member budget at this write boundary instead of
        # trusting a stale or hand-built listing to bypass link/size checks.
        archive_path = Path(listing.archive_path)
        volumes = tuple(Path(volume) for volume in listing.volumes)
        if any(_legacy_rar_group(volume.name) is not None for volume in volumes):
            raise ArchiveVolumeError("legacy RAR .r00 volumes are unsupported")
        for volume in volumes:
            _ensure_archive_file(volume)
        try:
            actual_archive_size = sum(volume.stat().st_size for volume in volumes)
        except OSError as exc:
            raise ArchiveVolumeError("archive volume cannot be read") from exc
        if actual_archive_size != listing.archive_size:
            raise ArchiveVolumeError("archive volume size changed since listing")
        try:
            if volumes[0].resolve(strict=False) != archive_path.resolve(strict=False):
                raise ArchiveVolumeError("archive listing first volume does not match source")
        except OSError as exc:
            raise ArchiveVolumeError("archive source cannot be resolved") from exc
        first_descriptor = volume_name_descriptor(volumes[0].name)
        volume_names = validate_volume_names(
            (volume.name for volume in volumes), volumes[0].name
        ) if first_descriptor is not None else tuple(volume.name for volume in volumes)
        if tuple(name.casefold() for name in volume_names) != tuple(volume.name.casefold() for volume in volumes):
            raise ArchiveVolumeError("archive volume order is invalid")
        try:
            with volumes[0].open("rb") as source_file:
                detection = _require_archive_magic(
                    source_file.read(self.limits.max_magic_scan_bytes),
                    filename=volumes[0].name,
                )
        except OSError as exc:
            raise ArchiveExtractionError("archive source cannot be read") from exc
        if detection.format != listing.archive_format:
            raise ArchiveFormatError("archive format changed since listing")
        validated_members = validate_archive_members(
            listing.members,
            limits=self.limits,
            archive_size=actual_archive_size,
            archive_format=listing.archive_format,
        )
        chosen = select_media_members(validated_members, selected)
        if pause_checkpoint is not None:
            pause_checkpoint()
        staging = Path(staging_root).resolve(strict=False)
        _prepare_extraction_root(
            staging,
            archive_path,
            pause_checkpoint=pause_checkpoint,
        )
        expected_bytes = sum(member.size for member in chosen)
        if expected_bytes > _expanded_budget_bytes(self.limits, listing.archive_format):
            raise ArchiveBudgetError("selected archive output exceeds expansion limit")
        _check_disk_budget(staging, expected_bytes, self.limits)

        selected_paths = tuple(member.path for member in chosen)
        args = (
            "x",
            "-sccUTF-8",
            "-y",
            "-aos",
            f"-o{staging}",
            str(volumes[0].resolve(strict=False)),
            *selected_paths,
        )
        candidates = _extraction_password_candidates(
            listing,
            password,
            max_candidates=self.limits.max_password_candidates,
        )
        for candidate_index, candidate in enumerate(candidates):
            if pause_checkpoint is not None:
                pause_checkpoint()
            result = _run_archive_tool(
                self.runner,
                args,
                password=candidate.value,
                cwd=staging,
                timeout=_command_timeout(self.limits, listing.archive_format),
            )
            if result.returncode == 0:
                output_paths = _verify_extraction_outputs(staging, chosen)
                for member, output in zip(chosen, output_paths):
                    if member.media_kind == "video":
                        if not _call_validator(self.video_validator, output, member):
                            raise ArchiveExtractionError("extracted video failed ffprobe validation")
                    elif member.media_kind == "subtitle":
                        if not _call_validator(self.subtitle_validator, output, member):
                            raise ArchiveExtractionError("extracted subtitle failed content validation")
                return ArchiveExtractionResult(
                    staging_root=staging,
                    files=tuple(output_paths),
                    members=chosen,
                    password_source=candidate.source,
                )

            failure_kind = _extraction_failure_kind(result, encrypted=listing.encrypted)
            if failure_kind == "corrupt":
                raise ArchiveCorruptionError("7-Zip reported a corrupted archive")
            if candidate_index + 1 < len(candidates):
                _require_empty_staging_for_password_retry(staging)
                continue
            if failure_kind == "password":
                raise ArchivePasswordError("archive extraction failed; password candidates exhausted")
            raise ArchiveExtractionError("7-Zip extraction failed")

        # ``_extraction_password_candidates`` always returns at least one
        # validated value.  Keep a stable failure in case a malformed public
        # ``ArchiveListing`` is supplied by a third-party caller.
        raise ArchivePasswordError("archive has no usable password candidate")

    def inspect_and_extract(
        self,
        archive_path: str | Path,
        staging_root: str | Path,
        *,
        selected: Sequence[str | ArchiveMember] | None = None,
        password_candidates: Iterable[PasswordCandidate | str] | None = None,
        password: str | None = None,
        pause_checkpoint: Callable[[], None] | None = None,
    ) -> ArchiveExtractionResult:
        inspector = ArchiveInspector(self.runner, limits=self.limits)
        listing = inspector.inspect(
            archive_path,
            password_candidates=password_candidates,
            password=password,
            pause_checkpoint=pause_checkpoint,
        )
        return self.extract(
            listing,
            staging_root,
            selected=selected,
            pause_checkpoint=pause_checkpoint,
        )


def _prepare_extraction_root(
    root: Path,
    archive_path: Path,
    *,
    pause_checkpoint: Callable[[], None] | None = None,
) -> None:
    if root.is_symlink() or (root.exists() and not root.is_dir()):
        raise ArchivePathError("task staging root is not a directory")
    archive_resolved = archive_path.resolve(strict=False)
    root_resolved = root.resolve(strict=False)
    if root_resolved == archive_resolved or archive_resolved.parent == root_resolved:
        raise ArchivePathError("archive and extraction root overlap")
    try:
        archive_resolved.relative_to(root_resolved)
    except ValueError:
        pass
    else:
        raise ArchivePathError("archive and extraction root overlap")
    if root.exists():
        if any(root.iterdir()):
            raise ArchiveExtractionError("task staging root must be empty")
    else:
        if pause_checkpoint is not None:
            pause_checkpoint()
        root.mkdir(parents=True, mode=0o700)


def _verify_extraction_outputs(root: Path, selected: Sequence[ArchiveMember]) -> list[Path]:
    expected = {member.path: member for member in selected}
    expected_keys = {member.collision_key: member.path for member in selected}
    found: dict[str, Path] = {}
    for current, dirs, files in os.walk(root, topdown=True, followlinks=False):
        current_path = Path(current)
        safe_dirs: list[str] = []
        for name in list(dirs):
            path = current_path / name
            if path.is_symlink():
                raise ArchiveLinkError("extracted output contains a symbolic link")
            safe_dirs.append(name)
        dirs[:] = safe_dirs
        for name in files:
            path = current_path / name
            if path.is_symlink():
                raise ArchiveLinkError("extracted output contains a symbolic link")
            try:
                mode = path.stat().st_mode
                size = path.stat().st_size
                if not stat.S_ISREG(mode) or path.stat().st_nlink > 1:
                    raise ArchiveLinkError("extracted output contains a non-regular/link file")
                relative = path.relative_to(root).as_posix()
                normalized = normalize_member_path(relative)
            except (OSError, ValueError) as exc:
                if isinstance(exc, ArchiveError):
                    raise
                raise ArchiveExtractionError("cannot inspect extracted output") from exc
            if normalized not in expected:
                raise ArchiveExtractionError("7-Zip produced an unexpected output file")
            if member_collision_key(normalized) in found:
                raise ArchiveCollisionError("extracted output paths collide")
            member = expected[normalized]
            if size != member.size:
                raise ArchiveExtractionError("extracted output size differs from archive listing")
            found[member.collision_key] = path
    if set(found) != set(expected_keys):
        raise ArchiveExtractionError("7-Zip did not produce every selected output")
    return [found[member.collision_key] for member in selected]


# A lightweight convenience API for stage-4 composition.
def inspect_archive(
    archive_path: str | Path,
    *,
    runner: ArchiveRunner | Callable[..., Any] | None = None,
    limits: ArchiveLimits | None = None,
    password_candidates: Iterable[PasswordCandidate | str] | None = None,
    password: str | None = None,
) -> ArchiveListing:
    return ArchiveInspector(runner, limits=limits).inspect(
        archive_path,
        password_candidates=password_candidates,
        password=password,
    )


def extract_selected(
    listing: ArchiveListing,
    staging_root: str | Path,
    *,
    runner: ArchiveRunner | Callable[..., Any] | None = None,
    limits: ArchiveLimits | None = None,
    selected: Sequence[str | ArchiveMember] | None = None,
    password: str | None = None,
    video_validator: Callable[..., Any] | None = None,
    subtitle_validator: Callable[..., Any] | None = None,
) -> ArchiveExtractionResult:
    return ArchiveExtractor(
        runner,
        limits=limits,
        video_validator=video_validator,
        subtitle_validator=subtitle_validator,
    ).extract(listing, staging_root, selected=selected, password=password)


__all__ = [
    "AListArchiveSource",
    "ArchiveCollisionError",
    "ArchiveCorruptionError",
    "ArchiveError",
    "ArchiveExtractionError",
    "ArchiveExtractionResult",
    "ArchiveExtractor",
    "ArchiveFormatError",
    "ArchiveInspection",
    "ArchiveInspector",
    "ArchiveLimits",
    "ArchiveListing",
    "ArchiveLinkError",
    "ArchiveMagic",
    "ArchiveMagicError",
    "ArchiveMember",
    "ArchiveMemberError",
    "ArchivePasswordConflict",
    "ArchivePasswordError",
    "ArchivePathError",
    "ArchivePauseRequested",
    "ArchiveRunner",
    "ArchiveSource",
    "ArchiveToolError",
    "ArchiveVolumeError",
    "ArchiveVolumes",
    "NestedArchiveUnsupported",
    "PasswordCandidate",
    "RunnerResult",
    "Subprocess7zRunner",
    "detect_archive_format",
    "detect_magic",
    "discover_password_candidates",
    "discover_volume_paths",
    "extract_password_markers",
    "extract_selected",
    "inspect_archive",
    "member_collision_key",
    "member_from_mapping",
    "normalize_member_path",
    "parse_7z_listing",
    "parse_7z_slt_listing",
    "password_candidates",
    "safe_staging_path",
    "select_media_members",
    "validate_archive_members",
    "validate_member_paths",
    "validate_subtitle_file",
    "validate_volume_names",
    "volume_name_descriptor",
]


# Backwards-friendly aliases intentionally kept at the end so the public
# surface above remains easy to scan.  They do not reintroduce old transaction
# semantics.
ArchiveVolumes = tuple[Path, ...]
validate_member_paths = validate_archive_members
