"""Shared, staging-only archive preprocessing for both ingress lanes.

The archive domain in :mod:`engine.scrapeflow.archive` deliberately stops at
safe listing and selective extraction.  This module is the small composition
boundary used by ordinary intake and provider payloads.  It does not know
TMDB, naming, gap ownership or the formal-library writer.  Both callers use
the same ``ArchiveInspector`` and ``ArchiveExtractor`` instance and receive a
descriptor pointing only at task-owned staging.

The remote methods are ports rather than a second AList implementation.  A
client that implements the existing ``list/read_file_prefix/download_file_to_path``
and ``mkdir/upload_file/exact_file_info`` methods can be passed directly (or
wrapped by ``AListArchiveSource``).  Password values are kept on the stack and
never appear in a result or serialized projection.
"""

from __future__ import annotations

from dataclasses import dataclass, field
import os
import posixpath
from pathlib import Path
import re
import shutil
from typing import Any, Callable, Iterable, Literal, Mapping, Protocol, Sequence

from .archive import (
    AListArchiveSource,
    ArchiveError,
    ArchiveExtractor,
    ArchiveInspector,
    ArchiveLimits,
    ArchiveMagicError,
    ArchiveMember,
    ArchivePauseRequested,
    ArchiveSource,
    PasswordCandidate,
    Subprocess7zRunner,
    detect_magic,
    discover_password_candidates,
    extract_password_markers,
    normalize_member_path,
)
from .media_policy import (
    SUBTITLE_EXTENSIONS,
    VIDEO_EXTENSIONS,
    classify_filename,
    extension,
    is_container_candidate_filename,
)
from .remote_paths import _has_unsafe_unicode, join_remote, normalize_remote_path, split_remote


IngressKind = Literal["ordinary", "provider"]


class ArchivePreprocessingError(ArchiveError):
    """A bounded composition or staging failure."""

    code = "archive_preprocessing_failed"


class ArchiveStagingConflict(ArchivePreprocessingError):
    code = "archive_staging_conflict"


class ArchiveMultiplicityError(ArchivePreprocessingError):
    code = "archive_multiple_inputs"


def _pause_checkpoint(checker: Callable[[], bool] | None) -> None:
    """Fail closed before archive staging or subprocess side effects."""
    if checker is None:
        return
    try:
        paused = bool(checker())
    except Exception as exc:
        raise ArchivePauseRequested("暂停状态不可确认，归档预处理已安全停止") from exc
    if paused:
        raise ArchivePauseRequested("暂停已生效，归档预处理保持可恢复")


class ArchiveRemotePort(Protocol):
    """The existing AList-shaped operations needed by this adapter."""

    def list(self, path: str, refresh: bool = False) -> Sequence[Mapping[str, Any]]: ...

    def read_file_prefix(self, path: str, *, max_bytes: int) -> bytes: ...

    def download_file_to_path(
        self, path: str, destination: Path, *, expected_size: int
    ) -> None: ...

    def mkdir(self, path: str) -> None: ...

    def upload_file(self, target_path: str, source: Path, content_type: str = "application/octet-stream") -> None: ...

    def exact_file_info(self, path: str) -> Mapping[str, Any] | None: ...


class ArchiveStagingSink(Protocol):
    """Create-only upload/readback port for a task-owned remote staging root."""

    def mkdir(self, path: str) -> None: ...

    def upload_file(self, target_path: str, source: Path, content_type: str = "application/octet-stream") -> None: ...

    def exact_file_info(self, path: str) -> Mapping[str, Any] | None: ...


@dataclass(frozen=True, slots=True)
class PreparedArchiveFile:
    """One media/sidecar member available to the current planner."""

    path: str = field(repr=False)
    relative_path: str
    size: int
    kind: Literal["video", "subtitle"]
    origin: Literal["archive", "passthrough"] = "archive"
    _password_values: tuple[str, ...] = field(default=(), repr=False, compare=False)

    def __post_init__(self) -> None:
        if not isinstance(self.path, str) or not self.path:
            raise ValueError("prepared file path is required")
        normalized = normalize_member_path(self.relative_path)
        object.__setattr__(self, "relative_path", normalized)
        if isinstance(self.size, bool) or not isinstance(self.size, int) or self.size <= 0:
            raise ValueError("prepared file size must be positive")
        if self.kind not in {"video", "subtitle"}:
            raise ValueError("prepared file kind is invalid")
        if self.origin not in {"archive", "passthrough"}:
            raise ValueError("prepared file origin is invalid")

    def to_dict(self, *, redact: Callable[[str], str] | None = None) -> dict[str, Any]:
        redact_path = redact or _redactor(self._password_values)
        return {
            "path": redact_path(self.path),
            "relative_path": self.relative_path,
            "size": self.size,
            "kind": self.kind,
            "origin": self.origin,
        }


@dataclass(frozen=True, slots=True)
class ArchivePreprocessResult:
    """Safe projection returned to an ingress caller.

    ``source_path`` is either a local task-staging directory/file or a remote
    task-staging path.  It is never a formal-library path.  ``archives`` keeps
    the original source labels for observability only; the source objects are
    intentionally not removed by this module.
    """

    ingress: IngressKind
    source_path: str
    task_staging: str
    files: tuple[PreparedArchiveFile, ...] = ()
    archives: tuple[str, ...] = ()
    changed: bool = False
    password_sources: tuple[str, ...] = ()
    _password_values: tuple[str, ...] = field(default=(), repr=False, compare=False)

    @property
    def media_files(self) -> tuple[PreparedArchiveFile, ...]:
        return tuple(item for item in self.files if item.kind == "video")

    @property
    def subtitle_files(self) -> tuple[PreparedArchiveFile, ...]:
        return tuple(item for item in self.files if item.kind == "subtitle")

    def __repr__(self) -> str:  # pragma: no cover - defensive projection
        return (
            "ArchivePreprocessResult("
            f"ingress={self.ingress!r}, changed={self.changed!r}, "
            f"files={len(self.files)}, archives={len(self.archives)}, "
            f"password_sources={self.password_sources!r})"
        )

    def to_dict(self) -> dict[str, Any]:
        """Return a redaction-safe job/UI projection.

        Deliberately omits ``ArchiveListing`` and all password candidates.
        """

        redact = _redactor(self._password_values)
        return {
            "ingress": self.ingress,
            "source_path": redact(self.source_path),
            "task_staging": redact(self.task_staging),
            "files": [item.to_dict(redact=redact) for item in self.files],
            "archives": [redact(item) for item in self.archives],
            "changed": self.changed,
            "password_sources": list(self.password_sources),
        }


_PASSWORD_NAME_HINT_RE = re.compile(r"(?:密码|password|passwd|pwd|pass)", re.I)
_REMOTE_SAFE_NAME_RE = re.compile(r"[^A-Za-z0-9._-]+")

# Office documents, EPUB/CBZ books, and several other harmless residuals are
# internally ZIP-based.  Byte magic alone must not turn a ``.docx`` notice
# beside otherwise ordinary episodes into a media archive, because that would
# either wrongly extract it or block the source as a mixed archive/media tree.
# Conversely, a ZIP/7z/RAR signature under a video/unknown name is exactly the
# renamed-container case that needs the safe staging lane.  Candidate suffixes
# (archive, disc image, executable) always retain their stricter byte-level
# treatment regardless of any coarse residual category.
_ARCHIVE_BEARING_RESIDUAL_KINDS = frozenset({
    "audio", "document", "font", "image", "manifest", "subtitle", "temporary",
})


def _archive_magic_requires_preprocessing(name: str, detection: object) -> bool:
    """Whether proven archive bytes are an intake container, not a residual.

    The detector has already proved that the object is readable by the bounded
    archive lane.  This routing decision is about *source role*, not trust:
    well-known residual formats such as DOCX retain their ordinary residual
    role even though their payload happens to be a ZIP, and a ``[Fonts].exe``
    self-extracting font installer stays a font residual rather than a media
    container.  Any archive/disc/EXE suffix that is not such a residual, a
    video-like suffix, or an unknown suffix remains a candidate container and
    is never silently consumed as a residual.
    """
    if not bool(getattr(detection, "is_archive", False)):
        return False
    category = classify_filename(name)
    if category in _ARCHIVE_BEARING_RESIDUAL_KINDS:
        return False
    return True


def _redactor(values: Iterable[str]) -> Callable[[str], str]:
    secrets = tuple(value for value in values if isinstance(value, str) and value)

    def redact(value: str) -> str:
        output = str(value)
        for secret in secrets:
            output = output.replace(secret, "<redacted>")
        return output

    return redact


def _read_local_prefix(path: Path, max_bytes: int) -> bytes:
    if max_bytes <= 0:
        return b""
    try:
        with path.open("rb") as handle:
            return handle.read(max_bytes)
    except OSError as exc:
        raise ArchivePreprocessingError("archive source cannot be read") from exc


def _local_tree_regular_files(root: Path) -> list[Path]:
    """Enumerate local source files while rejecting every child symlink.

    ``Path.rglob`` makes it tempting to filter symbolic links out of the
    result.  That is not safe for an intake boundary: a link hidden under a
    source tree is itself an ambiguous source object, whether it targets a file
    or a directory.  Scan with ``scandir`` and fail before any planner or
    archive operation can follow it.
    """

    output: list[Path] = []
    pending = [root]
    while pending:
        current = pending.pop()
        try:
            if current.is_symlink():
                raise ArchivePreprocessingError("archive source tree contains a symbolic link")
            with os.scandir(current) as iterator:
                entries = list(iterator)
        except ArchivePreprocessingError:
            raise
        except OSError as exc:
            raise ArchivePreprocessingError("archive source tree cannot be read") from exc
        for entry in entries:
            path = Path(entry.path)
            try:
                if entry.is_symlink():
                    raise ArchivePreprocessingError("archive source tree contains a symbolic link")
                if entry.is_dir(follow_symlinks=False):
                    pending.append(path)
                elif entry.is_file(follow_symlinks=False):
                    output.append(path)
            except ArchivePreprocessingError:
                raise
            except OSError as exc:
                raise ArchivePreprocessingError("archive source tree entry cannot be inspected") from exc
    return sorted(output, key=lambda item: item.as_posix())


def _safe_slug(value: str, *, fallback: str = "archive") -> str:
    """Build a deterministic, non-secret staging component from a basename."""

    value = _REMOTE_SAFE_NAME_RE.sub("-", str(value)).strip(".-")
    return (value[:96] or fallback).rstrip(".-") or fallback


def _as_ingress(value: str) -> IngressKind:
    if value not in {"ordinary", "provider"}:
        raise ValueError("archive ingress must be ordinary or provider")
    return value  # type: ignore[return-value]


def _entry_name(entry: Mapping[str, Any]) -> str | None:
    value = entry.get("name")
    return value if isinstance(value, str) and value else None


def _entry_size(entry: Mapping[str, Any]) -> int | None:
    """Return a positive declared size for bounded content reads.

    Callers that need to distinguish an explicitly empty remote object from
    an unavailable size use :func:`_declared_entry_size` below.  Keeping this
    legacy projection positive-only preserves the password-marker budget
    semantics.
    """
    parsed = _declared_entry_size(entry)
    return parsed if parsed is not None and parsed > 0 else None


def _declared_entry_size(entry: Mapping[str, Any]) -> int | None:
    """Return an exact non-negative provider size when one was declared."""
    value = entry.get("size")
    if isinstance(value, bool):
        return None
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed >= 0 else None


def _decode_marker_prefix(data: bytes) -> str:
    # Password hints are tiny human-authored text.  Keep the decoder bounded
    # and conservative; an undecodable hint is simply not a candidate.
    for encoding in ("utf-8-sig", "utf-16", "gb18030", "big5"):
        try:
            return data.decode(encoding)
        except UnicodeDecodeError:
            continue
    return ""


def _password_candidates_for_local(
    path: Path,
    *,
    retry_password: str | None,
    max_candidates: int,
    source_tree_markers: Iterable[str] = (),
) -> tuple[PasswordCandidate, ...]:
    try:
        siblings = [
            item.name for item in path.parent.iterdir()
            if not item.is_symlink() and item.is_file()
        ]
    except OSError:
        siblings = []
    # The archive domain already handles path/sibling/parent ordering.  This
    # adapter adds bounded contents of explicitly password-looking text files.
    markers = list(source_tree_markers)
    for name in siblings:
        if not _PASSWORD_NAME_HINT_RE.search(name):
            continue
        try:
            marker = _decode_marker_prefix(_read_local_prefix(path.parent / name, 64 * 1024))
        except ArchivePreprocessingError:
            continue
        if marker:
            markers.append(marker)
    return discover_password_candidates(
        path,
        retry_password=retry_password,
        sibling_names=siblings,
        source_tree_markers=markers,
        max_candidates=max_candidates,
    )


def _password_candidates_for_remote(
    source: ArchiveSource,
    remote_path: str,
    *,
    retry_password: str | None,
    max_candidates: int,
    source_tree_markers: Iterable[str] = (),
    pause_requested: Callable[[], bool] | None = None,
) -> tuple[PasswordCandidate, ...]:
    parent, _name = split_remote(remote_path)
    try:
        _pause_checkpoint(pause_requested)
        rows = list(source.list(parent))
    except ArchivePauseRequested:
        raise
    except Exception as exc:
        raise ArchivePreprocessingError("无法读取归档同目录提示") from exc
    names: list[str] = []
    markers = list(source_tree_markers)
    for raw in rows:
        if not isinstance(raw, Mapping) or raw.get("is_dir"):
            continue
        name = _entry_name(raw)
        if not name:
            continue
        names.append(name)
        if not _PASSWORD_NAME_HINT_RE.search(name):
            continue
        size = _entry_size(raw)
        if size is None or size > 64 * 1024:
            continue
        try:
            _pause_checkpoint(pause_requested)
            prefix = source.read_prefix(
                join_remote(parent, name),
                max_bytes=min(size, 64 * 1024),
            )
        except ArchivePauseRequested:
            raise
        except Exception:
            continue
        marker = _decode_marker_prefix(prefix)
        if marker:
            markers.append(marker)
    parents = [part for part in parent.split("/") if part][-2:][::-1]
    # ``password_candidates`` treats parent names literally and does not
    # inspect their contents, which is the intended bounded fallback order.
    from .archive import password_candidates

    return password_candidates(
        retry_password=retry_password,
        path=remote_path,
        sibling_names=names,
        source_tree_markers=markers,
        parent_names=parents,
        max_candidates=max_candidates,
    )


def _kind_for_path(path: str | Path) -> Literal["video", "subtitle"] | None:
    suffix = extension(Path(path).name)
    if suffix in VIDEO_EXTENSIONS:
        return "video"
    if suffix in SUBTITLE_EXTENSIONS:
        return "subtitle"
    return None


def _password_values(candidates: Iterable[PasswordCandidate]) -> tuple[str, ...]:
    return tuple(
        candidate.value
        for candidate in candidates
        if isinstance(candidate, PasswordCandidate) and candidate.value
    )


def _path_marker_values(value: str | Path) -> tuple[str, ...]:
    return tuple(dict.fromkeys(extract_password_markers(str(value))))


def _ensure_local_staging(
    root: Path,
    *,
    pause_requested: Callable[[], bool] | None = None,
) -> Path:
    if root.is_symlink() or (root.exists() and not root.is_dir()):
        raise ArchivePreprocessingError("task staging root is not a directory")
    # The caller may have inspected archive metadata for a while before it
    # reaches this local write.  Check again at the actual mkdir boundary so
    # a paused or deselected RootJob cannot allocate a new staging tree.
    _pause_checkpoint(pause_requested)
    root.mkdir(mode=0o700, parents=True, exist_ok=True)
    return root


def _fresh_child(
    root: Path,
    name: str,
    *,
    pause_requested: Callable[[], bool] | None = None,
) -> Path:
    """Create a fresh child without deleting an existing task-owned tree."""

    base = root / name
    candidate = base
    for index in range(100):
        if not candidate.exists():
            _pause_checkpoint(pause_requested)
            candidate.mkdir(mode=0o700, parents=True)
            return candidate
        if candidate.is_symlink() or not candidate.is_dir():
            raise ArchivePreprocessingError("task staging child is not a directory")
        if not any(candidate.iterdir()):
            return candidate
        candidate = root / f"{name}-{index + 1:02d}"
    raise ArchivePreprocessingError("task staging contains too many archive attempts")


def _archive_parent_relative(remote: str, source_root: str) -> str:
    """Return the archive's enclosing folder relative to the source root."""
    parent = posixpath.dirname(remote.rstrip("/"))
    root = source_root.rstrip("/")
    if parent == root or parent == "/" or not parent:
        return ""
    if parent.startswith(root + "/"):
        return parent[len(root) + 1:]
    return posixpath.basename(parent)


def _prepared_from_extraction(
    extraction_root: Path,
    result: Any,
    *,
    password_values: Iterable[str] = (),
) -> tuple[PreparedArchiveFile, ...]:
    output: list[PreparedArchiveFile] = []
    for path, member in zip(result.files, result.members):
        local = Path(path)
        try:
            relative = local.resolve(strict=True).relative_to(extraction_root.resolve(strict=True)).as_posix()
        except (OSError, ValueError) as exc:
            raise ArchivePreprocessingError("archive output escapes task staging") from exc
        kind = _kind_for_path(relative)
        if kind is None:
            raise ArchivePreprocessingError("archive output is not media or subtitle")
        size = local.stat().st_size
        if size != int(member.size):
            raise ArchivePreprocessingError("archive output size differs from listing")
        output.append(
            PreparedArchiveFile(
                path=str(local),
                relative_path=relative,
                size=size,
                kind=kind,
                origin="archive",
                _password_values=tuple(password_values),
            )
        )
    return tuple(output)


class ArchivePreprocessingAdapter:
    """One shared inspector/extractor boundary for ordinary and provider input.

    The adapter intentionally has no writer methods.  ``prepare_*`` only
    creates task-owned local staging and, for remote input, uploads into a
    caller-validated task staging prefix.  Identity, naming and formal writes
    remain the caller's responsibility.
    """

    def __init__(
        self,
        runner: Any | None = None,
        *,
        limits: ArchiveLimits | None = None,
        video_validator: Callable[..., Any] | None = None,
        subtitle_validator: Callable[..., Any] | None = None,
        staging_root_validator: Callable[[str], bool] | None = None,
        local_staging_root_validator: Callable[[Path], bool] | None = None,
        max_archives: int = 1,
    ) -> None:
        if isinstance(max_archives, bool) or max_archives < 1 or max_archives > 16:
            raise ValueError("max_archives must be between 1 and 16")
        self.limits = limits or ArchiveLimits()
        # Both lanes must share these exact objects.  This makes it impossible
        # for provider SFX and ordinary intake to silently drift into separate
        # listing/extraction implementations.
        self.runner = runner or Subprocess7zRunner()
        self.inspector = ArchiveInspector(self.runner, limits=self.limits)
        self.extractor = ArchiveExtractor(
            self.runner,
            limits=self.limits,
            video_validator=video_validator,
            subtitle_validator=subtitle_validator,
        )
        self.staging_root_validator = staging_root_validator
        self.local_staging_root_validator = local_staging_root_validator
        self.max_archives = max_archives

    def prepare_ordinary_local(
        self,
        source: str | Path,
        task_staging: str | Path,
        *,
        selected: Sequence[str | ArchiveMember] | None = None,
        retry_password: str | None = None,
        reject_unknown: bool = False,
        pause_requested: Callable[[], bool] | None = None,
    ) -> ArchivePreprocessResult:
        return self._prepare_local_file(
            source,
            task_staging,
            ingress="ordinary",
            selected=selected,
            retry_password=retry_password,
            reject_unknown=reject_unknown,
            pause_requested=pause_requested,
        )

    def prepare_provider_local(
        self,
        source: str | Path,
        task_staging: str | Path,
        *,
        selected: Sequence[str | ArchiveMember] | None = None,
        retry_password: str | None = None,
        pause_requested: Callable[[], bool] | None = None,
    ) -> ArchivePreprocessResult:
        return self._prepare_local_file(
            source,
            task_staging,
            ingress="provider",
            selected=selected,
            retry_password=retry_password,
            reject_unknown=True,
            pause_requested=pause_requested,
        )

    def prepare_ordinary_tree(
        self,
        source_root: str | Path,
        task_staging: str | Path,
        *,
        selected_by_archive: Mapping[str, Sequence[str | ArchiveMember]] | None = None,
        retry_password: str | None = None,
        pause_requested: Callable[[], bool] | None = None,
    ) -> ArchivePreprocessResult:
        return self._prepare_local_tree(
            source_root,
            task_staging,
            ingress="ordinary",
            selected_by_archive=selected_by_archive,
            retry_password=retry_password,
            reject_unknown=False,
            pause_requested=pause_requested,
        )

    def prepare_provider_tree(
        self,
        source_root: str | Path,
        task_staging: str | Path,
        *,
        selected_by_archive: Mapping[str, Sequence[str | ArchiveMember]] | None = None,
        retry_password: str | None = None,
        pause_requested: Callable[[], bool] | None = None,
    ) -> ArchivePreprocessResult:
        return self._prepare_local_tree(
            source_root,
            task_staging,
            ingress="provider",
            selected_by_archive=selected_by_archive,
            retry_password=retry_password,
            reject_unknown=True,
            pause_requested=pause_requested,
        )

    def prepare_ordinary_remote(
        self,
        source: str,
        source_port: ArchiveSource | ArchiveRemotePort,
        task_staging: str | Path,
        *,
        remote_staging_root: str,
        selected: Sequence[str | ArchiveMember] | None = None,
        retry_password: str | None = None,
        source_tree_markers: Iterable[str] = (),
        pause_requested: Callable[[], bool] | None = None,
    ) -> ArchivePreprocessResult:
        return self._prepare_remote_file(
            source,
            source_port,
            task_staging,
            remote_staging_root=remote_staging_root,
            ingress="ordinary",
            selected=selected,
            retry_password=retry_password,
            source_tree_markers=source_tree_markers,
            reject_unknown=False,
            pause_requested=pause_requested,
        )

    def prepare_ordinary_remote_tree(
        self,
        source_root: str,
        source_port: ArchiveSource | ArchiveRemotePort,
        task_staging: str | Path,
        *,
        remote_staging_root: str,
        selected_by_archive: Mapping[str, Sequence[str | ArchiveMember]] | None = None,
        retry_password: str | None = None,
        pause_requested: Callable[[], bool] | None = None,
    ) -> ArchivePreprocessResult:
        """Preprocess one archive-only ordinary source directory.

        A directory containing multiple archives or direct video members is
        ambiguous at the identity boundary and fails closed.  A directory
        without archives is returned unchanged, preserving the existing
        planner/intake behavior.
        """

        _pause_checkpoint(pause_requested)
        root = normalize_remote_path(source_root)
        self._validate_remote_staging_root(remote_staging_root)
        source_adapter = source_port
        if not all(hasattr(source_adapter, name) for name in ("list", "read_prefix", "download")):
            source_adapter = AListArchiveSource(source_port)
        archives: list[str] = []
        direct_media: list[str] = []
        renamed_media: list[tuple[str, str, int]] = []
        marker_texts: list[str] = []
        stack = [root]
        visited: set[str] = set()
        while stack:
            _pause_checkpoint(pause_requested)
            current = stack.pop()
            if current in visited:
                continue
            if len(visited) >= self.limits.max_depth * 256:
                raise ArchivePreprocessingError("来源目录深度/数量超过归档扫描上限")
            visited.add(current)
            try:
                rows = list(source_adapter.list(current))
            except ArchivePauseRequested:
                raise
            except Exception as exc:
                raise ArchivePreprocessingError("无法扫描普通入站目录") from exc
            if len(rows) > self.limits.max_members:
                raise ArchivePreprocessingError("来源目录条目数超过归档扫描上限")
            for raw in rows:
                _pause_checkpoint(pause_requested)
                if not isinstance(raw, Mapping):
                    continue
                name = _entry_name(raw)
                if not name or "/" in name or "\\" in name:
                    raise ArchivePreprocessingError("来源目录包含不安全条目名")
                if _has_unsafe_unicode(name):
                    # A control/format character in the provider name (for
                    # example a zero-width space in ``[Fonts​].7z``) makes the
                    # object unaddressable: no safe remote path can ever be
                    # built for it, so it can only remain at source as a
                    # residual.  Skip it here instead of failing the whole
                    # staging walk.
                    continue
                full = join_remote(current, name)
                if raw.get("is_dir") is True:
                    stack.append(full)
                    continue
                # A provider-declared empty object cannot contain an archive
                # signature or an executable ``MZ`` header.  Some AList
                # storage backends correctly list such placeholder/readme
                # files but reject their file-link Range requests.  Do not
                # turn a harmless zero-byte residual into a pre-planning I/O
                # failure; retain filename-based safety classification before
                # skipping the impossible magic probe.
                declared_size = _declared_entry_size(raw)
                if declared_size == 0:
                    if is_container_candidate_filename(name):
                        raise ArchiveMagicError("零字节容器/伪装文件不能安全预处理")
                    if _kind_for_path(name) is not None:
                        direct_media.append(full)
                    continue
                try:
                    prefix = source_adapter.read_prefix(
                        full, max_bytes=self.limits.max_magic_scan_bytes,
                    )
                except ArchivePauseRequested:
                    raise
                except Exception as exc:
                    raise ArchivePreprocessingError("无法读取来源文件前缀") from exc
                detection = detect_magic(prefix, filename=name)
                if _archive_magic_requires_preprocessing(name, detection):
                    archives.append(full)
                    continue
                if classify_filename(name) == "font":
                    # A ``[Fonts].exe`` self-extracting font installer is a
                    # resource residual: never execute, never expand, keep it
                    # at source while the rest of the intake proceeds.  Its
                    # ``MZ`` header must not trip the executable boundary.
                    continue
                if detection.kind == "executable":
                    raise ArchiveMagicError("来源目录包含可执行文件")
                if (
                    detection.kind == "media"
                    and detection.format in {"mkv", "mp4"}
                    and is_container_candidate_filename(name)
                ):
                    # A masquerade video (.exe/.bin with media magic) is renamed
                    # to its real container extension; the executable-named
                    # object is never executed.
                    renamed_media.append(
                        (full, f"{Path(name).stem}.{detection.format}", _entry_size(raw) or 0)
                    )
                    continue
                if is_container_candidate_filename(name):
                    raise ArchiveMagicError("来源目录容器/伪装文件魔数未知")
                # The canonical media policy classifies names, while magic is
                # a separate archive/executable safety boundary.  Requiring
                # MKV/MP4-only magic here made AVI/TS/M2TS silently disappear
                # from the archive-vs-direct-media ambiguity check.
                if _kind_for_path(name) is not None:
                    direct_media.append(full)
                if _PASSWORD_NAME_HINT_RE.search(name):
                    size = _entry_size(raw)
                    if size is not None and size <= 64 * 1024:
                        marker = _decode_marker_prefix(prefix[:64 * 1024])
                        if marker:
                            marker_texts.append(marker)
        if renamed_media and not archives and not direct_media:
            staging_root = _ensure_local_staging(
                Path(task_staging).resolve(), pause_requested=pause_requested,
            )
            renamed_root = _fresh_child(staging_root, "renamed", pause_requested=pause_requested)
            local_files: list[PreparedArchiveFile] = []
            marker_values: list[str] = []
            for full, new_name, size in renamed_media:
                _pause_checkpoint(pause_requested)
                if size <= 0:
                    raise ArchivePreprocessingError("伪装视频大小为 0")
                dest = renamed_root / new_name
                source_adapter.download(full, dest, expected_size=size)
                if dest.stat().st_size != size:
                    raise ArchivePreprocessingError("伪装视频下载大小不一致")
                marker_values.extend(_path_marker_values(full))
                local_files.append(PreparedArchiveFile(
                    path=str(dest.resolve()),
                    relative_path=new_name,
                    size=size,
                    kind="video",
                    origin="passthrough",
                    _password_values=_path_marker_values(full),
                ))
            remote_root = join_remote(remote_staging_root, "renamed")
            uploaded = self._upload_outputs(
                local_files, renamed_root, remote_root, source_port,
                pause_requested=pause_requested,
            )
            return ArchivePreprocessResult(
                ingress="ordinary",
                source_path=remote_root,
                task_staging=str(Path(task_staging).resolve()),
                files=tuple(uploaded),
                changed=True,
                _password_values=tuple(dict.fromkeys(marker_values)),
            )
        if not archives:
            return ArchivePreprocessResult(
                ingress="ordinary",
                source_path=root,
                task_staging=str(Path(task_staging).resolve()),
            )
        if direct_media:
            raise ArchiveMultiplicityError(
                "普通入站归档不能混入直接媒体"
            )
        if len(archives) > 1:
            # A masquerade batch (one SFX/ISO per episode) is expanded archive
            # by archive into one shared staging directory, preserving each
            # archive's enclosing season folder so B/W still sees the season
            # structure.  The executable-named wrappers are never executed.
            return self._prepare_multiple_remote_archives(
                archives,
                source_adapter,
                source_port,
                task_staging,
                remote_staging_root=remote_staging_root,
                retry_password=retry_password,
                source_tree_markers=marker_texts,
                pause_requested=pause_requested,
                source_root=root,
            )
        selected = (
            selected_by_archive.get(archives[0])
            if isinstance(selected_by_archive, Mapping)
            else None
        )
        return self.prepare_ordinary_remote(
            archives[0],
            # Keep the original combined port here. ``source_adapter`` may be
            # the read-only ``AListArchiveSource`` compatibility wrapper; the
            # remote extraction boundary also needs the caller's create-only
            # staging sink (mkdir/upload/readback).
            source_port,
            task_staging,
            remote_staging_root=remote_staging_root,
            selected=selected,
            retry_password=retry_password,
            source_tree_markers=marker_texts,
            pause_requested=pause_requested,
        )

    def _prepare_multiple_remote_archives(
        self,
        archives: Sequence[str],
        source_adapter: Any,
        source_port: Any,
        task_staging: str | Path,
        *,
        remote_staging_root: str,
        retry_password: str | None,
        source_tree_markers: Iterable[str],
        pause_requested: Callable[[], bool] | None,
        source_root: str,
    ) -> ArchivePreprocessResult:
        """Expand a masquerade batch (one SFX/ISO per episode) archive by archive.

        Each wrapper is 7-Zip read-only listed and bounded-extracted into a
        shared staging tree; the enclosing season folder is preserved as the
        relative path prefix so B/W still sees the season structure.  No wrapper
        is ever executed or mounted.
        """
        staging_root = _ensure_local_staging(
            Path(task_staging).resolve(), pause_requested=pause_requested,
        )
        all_local: list[PreparedArchiveFile] = []
        marker_values: list[str] = []
        for index, remote in enumerate(archives):
            _pause_checkpoint(pause_requested)
            candidates = _password_candidates_for_remote(
                source_adapter,
                remote,
                retry_password=retry_password,
                max_candidates=self.limits.max_password_candidates,
                source_tree_markers=source_tree_markers,
                pause_requested=pause_requested,
            )
            _pause_checkpoint(pause_requested)
            input_root = _fresh_child(
                staging_root, f"input-{index}", pause_requested=pause_requested,
            )
            listing = self.inspector.inspect_remote(
                source_adapter,
                remote,
                input_root,
                password_candidates=candidates,
                pause_checkpoint=lambda: _pause_checkpoint(pause_requested),
            )
            _pause_checkpoint(pause_requested)
            extraction_root = _fresh_child(
                staging_root, f"extract-{index}", pause_requested=pause_requested,
            )
            extracted = self.extractor.extract(
                listing,
                extraction_root,
                selected=None,
                pause_checkpoint=lambda: _pause_checkpoint(pause_requested),
            )
            local_files = _prepared_from_extraction(
                extraction_root,
                extracted,
                password_values=_password_values(candidates),
            )
            parent_rel = _archive_parent_relative(remote, source_root)
            for prepared in local_files:
                rel = prepared.relative_path
                if parent_rel and not rel.startswith(parent_rel + "/"):
                    rel = f"{parent_rel}/{rel}"
                all_local.append(PreparedArchiveFile(
                    path=prepared.path,
                    relative_path=rel,
                    size=prepared.size,
                    kind=prepared.kind,
                    origin=prepared.origin,
                    _password_values=prepared._password_values,
                ))
            marker_values.extend(_password_values(candidates))
        remote_root = join_remote(remote_staging_root, "renamed")
        uploaded = self._upload_outputs(
            all_local,
            staging_root,
            remote_root,
            source_port,
            pause_requested=pause_requested,
        )
        return ArchivePreprocessResult(
            ingress="ordinary",
            source_path=remote_root,
            task_staging=str(staging_root),
            files=tuple(uploaded),
            changed=True,
            archives=tuple(archives),
            _password_values=tuple(dict.fromkeys(marker_values)),
        )

    def prepare_provider_remote(
        self,
        source: str,
        source_port: ArchiveSource | ArchiveRemotePort,
        task_staging: str | Path,
        *,
        remote_staging_root: str,
        selected: Sequence[str | ArchiveMember] | None = None,
        retry_password: str | None = None,
        source_tree_markers: Iterable[str] = (),
        pause_requested: Callable[[], bool] | None = None,
    ) -> ArchivePreprocessResult:
        return self._prepare_remote_file(
            source,
            source_port,
            task_staging,
            remote_staging_root=remote_staging_root,
            ingress="provider",
            selected=selected,
            retry_password=retry_password,
            source_tree_markers=source_tree_markers,
            reject_unknown=True,
            pause_requested=pause_requested,
        )

    def prepare_ordinary_request(
        self,
        request: Mapping[str, Any],
        *,
        alist: Any | None = None,
        task_staging: str | Path | None = None,
        remote_staging_root: str | None = None,
        retry_password: str | None = None,
        pause_requested: Callable[[], bool] | None = None,
    ) -> Mapping[str, Any]:
        """Optional runner hook used before ordinary Engine planning.

        A composition root may provide a concrete task staging location.  If
        it does not (the current legacy queue stores only a remote source
        directory), this hook is a deliberate no-op so ordinary non-archive
        behavior remains byte-for-byte unchanged.
        """

        _pause_checkpoint(pause_requested)
        if not isinstance(request, Mapping) or task_staging is None:
            return request
        source = request.get("source_path")
        if not isinstance(source, str) or not source:
            return request
        local = Path(source)
        if local.is_file():
            prepared = self.prepare_ordinary_local(
                local,
                task_staging,
                retry_password=retry_password,
                pause_requested=pause_requested,
            )
        elif local.is_dir():
            prepared = self.prepare_ordinary_tree(
                local,
                task_staging,
                retry_password=retry_password,
                pause_requested=pause_requested,
            )
        elif alist is not None and remote_staging_root:
            # A user can submit either an inbound directory or a single file.
            # Prefer the existing exact-info port when it can distinguish a
            # file; otherwise use the bounded directory scanner.  No path is
            # inferred from a basename and a failed probe remains fail-closed.
            exact = getattr(alist, "exact_file_info", None)
            info: Mapping[str, object] | None = None
            if callable(exact):
                try:
                    candidate = exact(source)
                    if isinstance(candidate, Mapping):
                        info = candidate
                except ArchivePauseRequested:
                    raise
                except Exception:
                    info = None
            if info is not None and info.get("is_dir") is not True:
                prepared = self.prepare_ordinary_remote(
                    source,
                    alist,
                    task_staging,
                    remote_staging_root=remote_staging_root,
                    retry_password=retry_password,
                    pause_requested=pause_requested,
                )
            else:
                # The ordinary intake contract is normally a source
                # directory. Scan it before identity/planning; a directory
                # with no archive returns a no-op descriptor and retains the
                # old path unchanged.
                prepared = self.prepare_ordinary_remote_tree(
                    source,
                    alist,
                    task_staging,
                    remote_staging_root=remote_staging_root,
                    retry_password=retry_password,
                    pause_requested=pause_requested,
                )
        else:
            return request
        updated = dict(request)
        if prepared.changed:
            updated["source_path"] = prepared.source_path
            updated["archive_preprocessed"] = prepared.to_dict()
        return updated

    def prepare_provider_delivery(
        self,
        acquisition: Mapping[str, Any],
        *,
        request: Mapping[str, Any],
        staging_root: str,
        workspace: str | Path,
        alist: Any,
        retry_password: str | None = None,
        pause_requested: Callable[[], bool] | None = None,
    ) -> Mapping[str, Any]:
        """Preprocess explicitly marked provider archive rows in place.

        The existing Torrent materializer normally returns media-only rows, so
        this is a no-op for today's lane.  If a future SFX-capable materializer
        returns a row with ``archive_source=true``, the row must carry explicit
        ``selected_members`` and ``gap_ids``; no identity or episode mapping is
        inferred from an archive filename.
        """

        rows = acquisition.get("files") if isinstance(acquisition, Mapping) else None
        if not isinstance(rows, list):
            return acquisition
        archive_rows = [
            row for row in rows
            if isinstance(row, Mapping) and row.get("archive_source") is True
        ]
        if not archive_rows:
            return acquisition
        output = dict(acquisition)
        output_rows: list[dict[str, Any]] = []
        for raw in rows:
            _pause_checkpoint(pause_requested)
            if not isinstance(raw, Mapping) or raw.get("archive_source") is not True:
                output_rows.append(dict(raw) if isinstance(raw, Mapping) else raw)
                continue
            source = raw.get("path")
            gap_ids = raw.get("gap_ids")
            selected = raw.get("selected_members")
            if not isinstance(source, str) or not isinstance(gap_ids, list) or not gap_ids:
                raise ArchivePreprocessingError("provider archive row lacks explicit gap binding")
            if selected is not None and (
                not isinstance(selected, list) or not all(isinstance(value, str) for value in selected)
            ):
                raise ArchivePreprocessingError("provider archive selected_members is invalid")
            prepared = self.prepare_provider_remote(
                source,
                alist,
                workspace,
                remote_staging_root=staging_root,
                selected=selected,
                retry_password=retry_password,
                pause_requested=pause_requested,
            )
            for file in prepared.files:
                output_rows.append({
                    "path": file.path,
                    "size": file.size,
                    "kind": file.kind,
                    "gap_ids": list(gap_ids),
                    "source_name": file.relative_path,
                    "provider_path": file.relative_path,
                })
        output["files"] = output_rows
        output["archive_preprocessed"] = True
        return output

    def _prepare_local_file(
        self,
        source: str | Path,
        task_staging: str | Path,
        *,
        ingress: IngressKind,
        selected: Sequence[str | ArchiveMember] | None,
        retry_password: str | None,
        source_tree_markers: Iterable[str] = (),
        reject_unknown: bool,
        pause_requested: Callable[[], bool] | None = None,
    ) -> ArchivePreprocessResult:
        _pause_checkpoint(pause_requested)
        ingress = _as_ingress(ingress)
        source_path = Path(source)
        if source_path.is_symlink() or not source_path.is_file():
            raise ArchivePreprocessingError("archive source is not a regular file")
        _pause_checkpoint(pause_requested)
        staging = _ensure_local_staging(
            Path(task_staging).resolve(),
            pause_requested=pause_requested,
        )
        self._validate_local_staging_root(staging, source_path.resolve())
        prefix = _read_local_prefix(source_path, self.limits.max_magic_scan_bytes)
        detection = detect_magic(prefix, filename=source_path.name)
        suffix_requires_container_proof = is_container_candidate_filename(source_path.name)
        if not detection.is_archive:
            if detection.kind == "media":
                kind = _kind_for_path(source_path)
                if (
                    kind is None
                    and detection.format in {"mkv", "mp4"}
                    and is_container_candidate_filename(source_path.name)
                ):
                    # A masquerade video (.exe/.bin with media magic) is renamed
                    # to its real container extension in task staging; the
                    # original executable-named object is never executed.
                    new_name = f"{Path(source_path.name).stem}.{detection.format}"
                    dest = staging / new_name
                    shutil.copy2(source_path, dest)
                    path_markers = _path_marker_values(source_path)
                    return ArchivePreprocessResult(
                        ingress=ingress,
                        source_path=str(dest.resolve()),
                        task_staging=str(staging),
                        files=(PreparedArchiveFile(
                            path=str(dest.resolve()),
                            relative_path=new_name,
                            size=dest.stat().st_size,
                            kind="video",
                            origin="passthrough",
                            _password_values=path_markers,
                        ),),
                        changed=True,
                        _password_values=path_markers,
                    )
                if kind is None:
                    raise ArchiveMagicError("source magic identifies media with unsupported suffix")
                path_markers = _path_marker_values(source_path)
                return ArchivePreprocessResult(
                    ingress=ingress,
                    source_path=str(source_path.resolve()),
                    task_staging=str(staging),
                    files=(PreparedArchiveFile(
                        path=str(source_path.resolve()),
                        relative_path=source_path.name,
                        size=source_path.stat().st_size,
                        kind=kind,
                        origin="passthrough",
                        _password_values=path_markers,
                    ),),
                    _password_values=path_markers,
                )
            if detection.kind == "executable":
                raise ArchiveMagicError("executable source is not an archive")
            if suffix_requires_container_proof or reject_unknown:
                raise ArchiveMagicError("archive magic is unknown")
            # Ordinary residuals are left for the residual policy/audit; this
            # adapter has no deletion authority.
            return ArchivePreprocessResult(
                ingress=ingress,
                source_path=str(source_path.resolve()),
                task_staging=str(staging),
                _password_values=_path_marker_values(source_path),
            )

        candidates = _password_candidates_for_local(
            source_path,
            retry_password=retry_password,
            max_candidates=self.limits.max_password_candidates,
            source_tree_markers=source_tree_markers,
        )
        # ``inspect`` and ``extract`` invoke the archive subprocess and may
        # create task-owned staging.  Fence each independently so a pause
        # that arrives during password discovery cannot start either one.
        _pause_checkpoint(pause_requested)
        listing = self.inspector.inspect(
            source_path,
            password_candidates=candidates,
            pause_checkpoint=lambda: _pause_checkpoint(pause_requested),
        )
        chosen = selected
        _pause_checkpoint(pause_requested)
        extraction_root = _fresh_child(
            staging,
            f"archive/{_safe_slug(source_path.name)}",
            pause_requested=pause_requested,
        )
        _pause_checkpoint(pause_requested)
        extracted = self.extractor.extract(
            listing,
            extraction_root,
            selected=chosen,
            pause_checkpoint=lambda: _pause_checkpoint(pause_requested),
        )
        candidate_values = _password_values(candidates)
        files = _prepared_from_extraction(
            extraction_root,
            extracted,
            password_values=candidate_values,
        )
        return ArchivePreprocessResult(
            ingress=ingress,
            source_path=str(extraction_root),
            task_staging=str(staging),
            files=files,
            archives=(str(source_path.resolve()),),
            changed=True,
            password_sources=(extracted.password_source,),
            _password_values=candidate_values,
        )

    def _prepare_local_tree(
        self,
        source_root: str | Path,
        task_staging: str | Path,
        *,
        ingress: IngressKind,
        selected_by_archive: Mapping[str, Sequence[str | ArchiveMember]] | None,
        retry_password: str | None,
        reject_unknown: bool,
        pause_requested: Callable[[], bool] | None = None,
    ) -> ArchivePreprocessResult:
        _pause_checkpoint(pause_requested)
        ingress = _as_ingress(ingress)
        root = Path(source_root)
        if root.is_symlink() or not root.is_dir():
            raise ArchivePreprocessingError("archive source root is not a directory")
        _pause_checkpoint(pause_requested)
        staging = _ensure_local_staging(
            Path(task_staging).resolve(),
            pause_requested=pause_requested,
        )
        self._validate_local_staging_root(staging, root.resolve())
        files = _local_tree_regular_files(root)
        if len(files) > self.limits.max_members:
            raise ArchivePreprocessingError("source tree file count exceeds archive limit")
        archive_candidates: list[Path] = []
        direct: list[PreparedArchiveFile] = []
        for item in files:
            if item.is_symlink():
                # Re-check at the use boundary in case the source tree changed
                # after the directory scan.  Never silently skip an injected
                # link and continue with a partial view of the source tree.
                raise ArchivePreprocessingError("archive source tree contains a symbolic link")
            try:
                prefix = _read_local_prefix(item, self.limits.max_magic_scan_bytes)
            except ArchivePreprocessingError:
                raise
            detection = detect_magic(prefix, filename=item.name)
            if _archive_magic_requires_preprocessing(item.name, detection):
                archive_candidates.append(item)
                continue
            if classify_filename(item.name) == "font":
                # A ``[Fonts].exe`` font installer is a residual resource;
                # its ``MZ`` header must not trip the executable boundary.
                continue
            if detection.kind == "executable":
                raise ArchiveMagicError("source tree contains an executable payload")
            if is_container_candidate_filename(item.name):
                raise ArchiveMagicError("source tree container/masquerade magic is unknown")
            if (
                reject_unknown and detection.kind == "unknown"
                and extension(item.name) in {".exe", ".bin", ".dat"}
            ):
                raise ArchiveMagicError("source tree contains an executable or unknown payload")
            kind = _kind_for_path(item)
            if kind is not None:
                direct.append(PreparedArchiveFile(
                    path=str(item.resolve()),
                    relative_path=item.relative_to(root).as_posix(),
                    size=item.stat().st_size,
                    kind=kind,
                    origin="passthrough",
                    _password_values=_path_marker_values(item),
                ))
        if len(archive_candidates) > self.max_archives:
            raise ArchiveMultiplicityError("source tree contains multiple archive inputs")
        if archive_candidates and direct:
            # A planner must not silently choose between direct media and an
            # archive member; callers can submit them as separate tasks.
            raise ArchiveMultiplicityError("source tree mixes direct media and archive inputs")
        extracted_files: list[PreparedArchiveFile] = list(direct)
        labels: list[str] = []
        sources: list[str] = []
        secrets: list[str] = []
        for item in direct:
            secrets.extend(item._password_values)
        for archive_path in archive_candidates:
            _pause_checkpoint(pause_requested)
            selected = (
                selected_by_archive.get(str(archive_path))
                if isinstance(selected_by_archive, Mapping)
                else None
            )
            result = self._prepare_local_file(
                archive_path,
                staging,
                ingress=ingress,
                selected=selected,
                retry_password=retry_password,
                reject_unknown=reject_unknown,
                pause_requested=pause_requested,
            )
            extracted_files.extend(result.files)
            labels.extend(result.archives)
            sources.extend(result.password_sources)
            secrets.extend(result._password_values)
        if not archive_candidates and not direct:
            return ArchivePreprocessResult(
                ingress=ingress,
                source_path=str(root.resolve()),
                task_staging=str(staging),
                _password_values=tuple(dict.fromkeys(secrets)),
            )
        source_path = str(staging / "archive") if archive_candidates else str(root.resolve())
        return ArchivePreprocessResult(
            ingress=ingress,
            source_path=source_path,
            task_staging=str(staging),
            files=tuple(extracted_files),
            archives=tuple(labels),
            changed=bool(archive_candidates),
            password_sources=tuple(sources),
            _password_values=tuple(dict.fromkeys(secrets)),
        )

    def _prepare_remote_file(
        self,
        source: str,
        source_port: ArchiveSource | ArchiveRemotePort,
        task_staging: str | Path,
        *,
        remote_staging_root: str,
        ingress: IngressKind,
        selected: Sequence[str | ArchiveMember] | None,
        retry_password: str | None,
        source_tree_markers: Iterable[str] = (),
        reject_unknown: bool,
        pause_requested: Callable[[], bool] | None = None,
    ) -> ArchivePreprocessResult:
        _pause_checkpoint(pause_requested)
        ingress = _as_ingress(ingress)
        remote = normalize_remote_path(source)
        self._validate_remote_staging_root(remote_staging_root)
        _pause_checkpoint(pause_requested)
        staging = _ensure_local_staging(
            Path(task_staging).resolve(),
            pause_requested=pause_requested,
        )
        self._validate_local_staging_root(staging, None)
        source_adapter = source_port
        if not all(hasattr(source_adapter, name) for name in ("list", "read_prefix", "download")):
            source_adapter = AListArchiveSource(source_port)
        try:
            _pause_checkpoint(pause_requested)
            prefix = source_adapter.read_prefix(
                remote, max_bytes=self.limits.max_magic_scan_bytes,
            )
        except ArchivePauseRequested:
            raise
        except Exception as exc:
            raise ArchivePreprocessingError("无法读取远端归档前缀") from exc
        detection = detect_magic(prefix, filename=Path(remote).name)
        suffix_requires_container_proof = is_container_candidate_filename(Path(remote).name)
        if not detection.is_archive:
            if detection.kind == "media":
                kind = _kind_for_path(remote)
                if kind is None:
                    raise ArchiveMagicError("远端来源媒体后缀不受支持")
                _pause_checkpoint(pause_requested)
                info = self._remote_exact(source_port, remote)
                size = int(info.get("size") or 0) if info else 0
                if size <= 0:
                    raise ArchivePreprocessingError("远端媒体大小无效")
                path_markers = _path_marker_values(remote)
                return ArchivePreprocessResult(
                    ingress=ingress,
                    source_path=remote,
                    task_staging=str(staging),
                    files=(PreparedArchiveFile(
                        path=remote,
                        relative_path=Path(remote).name,
                        size=size,
                        kind=kind,
                        origin="passthrough",
                        _password_values=path_markers,
                    ),),
                    _password_values=path_markers,
                )
            if detection.kind == "executable":
                raise ArchiveMagicError("远端 executable 不是归档")
            if suffix_requires_container_proof or reject_unknown:
                raise ArchiveMagicError("远端归档魔数未知")
            return ArchivePreprocessResult(
                ingress=ingress,
                source_path=remote,
                task_staging=str(staging),
                _password_values=_path_marker_values(remote),
            )

        candidates = _password_candidates_for_remote(
            source_adapter,
            remote,
            retry_password=retry_password,
            max_candidates=self.limits.max_password_candidates,
            source_tree_markers=source_tree_markers,
            pause_requested=pause_requested,
        )
        _pause_checkpoint(pause_requested)
        input_root = _fresh_child(
            staging,
            f"archive-input/{_safe_slug(Path(remote).name)}",
            pause_requested=pause_requested,
        )
        _pause_checkpoint(pause_requested)
        listing = self.inspector.inspect_remote(
            source_adapter,
            remote,
            input_root,
            password_candidates=candidates,
            pause_checkpoint=lambda: _pause_checkpoint(pause_requested),
        )
        _pause_checkpoint(pause_requested)
        extraction_root = _fresh_child(
            staging,
            f"archive/{_safe_slug(Path(remote).name)}",
            pause_requested=pause_requested,
        )
        _pause_checkpoint(pause_requested)
        extracted = self.extractor.extract(
            listing,
            extraction_root,
            selected=selected,
            pause_checkpoint=lambda: _pause_checkpoint(pause_requested),
        )
        candidate_values = _password_values(candidates)
        local_files = _prepared_from_extraction(
            extraction_root,
            extracted,
            password_values=candidate_values,
        )
        remote_root = join_remote(
            join_remote(remote_staging_root, "archive"),
            _safe_slug(Path(remote).name),
        )
        uploaded = self._upload_outputs(
            local_files,
            extraction_root,
            remote_root,
            source_port,
            pause_requested=pause_requested,
        )
        return ArchivePreprocessResult(
            ingress=ingress,
            source_path=remote_root,
            task_staging=str(staging),
            files=tuple(uploaded),
            archives=(remote,),
            changed=True,
            password_sources=(extracted.password_source,),
            _password_values=candidate_values,
        )

    def _validate_local_staging_root(self, staging: Path, source: Path | None) -> None:
        validator = self.local_staging_root_validator
        if validator is not None:
            try:
                accepted = bool(validator(staging))
            except Exception as exc:
                raise ArchivePreprocessingError("local task staging 归属校验失败") from exc
            if not accepted:
                raise ArchivePreprocessingError("归档输出必须位于 task staging")
        # Never unpack beside an inbound archive or underneath an inbound
        # source tree.  Provider callers should pass a sibling task workspace;
        # a nested payload directory remains safe and is allowed.
        if source is not None:
            source_parent = source.parent if source.is_file() else source
            if staging == source_parent:
                raise ArchivePreprocessingError("归档输入与 task staging 目录重叠")
            try:
                staging.relative_to(source)
            except ValueError:
                pass
            else:
                raise ArchivePreprocessingError("task staging 位于归档来源树内")

    def _validate_remote_staging_root(self, root: str) -> str:
        normalized = normalize_remote_path(root)
        validator = self.staging_root_validator
        if validator is None:
            raise ArchivePreprocessingError("缺少 task staging 归属校验器")
        try:
            accepted = bool(validator(normalized))
        except Exception as exc:
            raise ArchivePreprocessingError("task staging 归属校验失败") from exc
        if not accepted:
            raise ArchivePreprocessingError("远端归档输出必须位于 task staging")
        return normalized

    @staticmethod
    def _remote_exact(port: Any, path: str) -> Mapping[str, Any] | None:
        method = getattr(port, "exact_file_info", None)
        if not callable(method):
            raise ArchivePreprocessingError("远端 archive port 缺少 exact_file_info")
        value = method(path)
        return value if isinstance(value, Mapping) else None

    def _upload_outputs(
        self,
        files: Sequence[PreparedArchiveFile],
        extraction_root: Path,
        remote_root: str,
        sink: Any,
        *,
        pause_requested: Callable[[], bool] | None = None,
    ) -> tuple[PreparedArchiveFile, ...]:
        root = normalize_remote_path(remote_root)
        validator = self.staging_root_validator
        if validator is None or not validator(root):
            raise ArchivePreprocessingError("archive output root is not task-owned staging")
        mkdir = getattr(sink, "mkdir", None)
        upload = getattr(sink, "upload_file", None)
        exact = getattr(sink, "exact_file_info", None)
        if not all(callable(item) for item in (mkdir, upload, exact)):
            raise ArchivePreprocessingError("archive staging sink lacks create/readback methods")
        _pause_checkpoint(pause_requested)
        mkdir(root)
        output: list[PreparedArchiveFile] = []
        for item in files:
            _pause_checkpoint(pause_requested)
            local = Path(item.path)
            relative = normalize_member_path(item.relative_path)
            target = join_remote(root, relative)
            if not bool(validator(target)):
                raise ArchivePreprocessingError("archive member target escapes task staging")
            parent = posixpath.dirname(target)
            if parent != root:
                # The sink's mkdir is idempotent in AList; creating each path
                # is bounded by the validated member count and avoids a
                # provider-specific recursive directory API.
                parts = root.strip("/").split("/")
                target_parts = parent.strip("/").split("/")
                for end in range(len(parts) + 1, len(target_parts) + 1):
                    _pause_checkpoint(pause_requested)
                    mkdir("/" + "/".join(target_parts[:end]))
            _pause_checkpoint(pause_requested)
            existing = exact(target)
            if existing is not None:
                existing_size = existing.get("size")
                try:
                    existing_size = int(existing_size)
                except (TypeError, ValueError):
                    existing_size = -1
                if existing_size != item.size:
                    raise ArchiveStagingConflict("task staging target exists with a different size")
            else:
                _pause_checkpoint(pause_requested)
                upload(target, local, _content_type(local.name))
            _pause_checkpoint(pause_requested)
            observed = exact(target)
            if observed is None:
                raise ArchivePreprocessingError("archive staging upload is not visible")
            try:
                observed_size = int(observed.get("size") or 0)
            except (TypeError, ValueError):
                observed_size = 0
            if observed_size != item.size:
                raise ArchivePreprocessingError("archive staging readback size mismatch")
            output.append(
                PreparedArchiveFile(
                    path=target,
                    relative_path=relative,
                    size=item.size,
                    kind=item.kind,
                    origin="archive",
                    _password_values=item._password_values,
                )
            )
        return tuple(output)


def _content_type(name: str) -> str:
    return {
        ".srt": "application/x-subrip",
        ".ass": "text/x-ass",
        ".ssa": "text/x-ssa",
        ".vtt": "text/vtt",
    }.get(extension(name), "application/octet-stream")


def prepare_ordinary_archive(
    adapter: ArchivePreprocessingAdapter,
    source: str | Path,
    task_staging: str | Path,
    **kwargs: Any,
) -> ArchivePreprocessResult:
    """Named ordinary-intake facade; delegates to the shared adapter."""

    return adapter.prepare_ordinary_local(source, task_staging, **kwargs)


def prepare_provider_archive(
    adapter: ArchivePreprocessingAdapter,
    source: str | Path,
    task_staging: str | Path,
    **kwargs: Any,
) -> ArchivePreprocessResult:
    """Named Provider/SFX facade; delegates to the shared adapter."""

    return adapter.prepare_provider_local(source, task_staging, **kwargs)


__all__ = [
    "ArchiveMultiplicityError",
    "ArchivePauseRequested",
    "ArchivePreprocessResult",
    "ArchivePreprocessingAdapter",
    "ArchivePreprocessingError",
    "ArchiveRemotePort",
    "ArchiveStagingConflict",
    "ArchiveStagingSink",
    "PreparedArchiveFile",
    "prepare_ordinary_archive",
    "prepare_provider_archive",
]
