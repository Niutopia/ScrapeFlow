"""Crash-safe remote file copy/delete transactions.

The transaction deliberately does not depend on :mod:`engine.scraper`.  A
caller supplies a small exact-path client adapter, which keeps the safety
rules usable by the scraper, residual routing, subtitle execution, and
replenishment without creating an import cycle.

There is one intentionally conservative rule: after an upload call may have
started, this module never calls upload for that transaction again.  An
ambiguous response is reconciled through an exact-path stat and a complete
read-back hash.  If the target is still not visible, the durable local stage
is retained and the transaction remains uncertain.
"""

from __future__ import annotations

import contextlib
import hashlib
import json
import os
import re
import shutil
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, BinaryIO, Callable, ContextManager, Mapping, Protocol

from .serialization import atomic_write_json


_TRANSACTION_ID_RE = re.compile(r"\A[A-Za-z0-9][A-Za-z0-9._-]{0,127}\Z")
_SHA256_RE = re.compile(r"\A[0-9a-f]{64}\Z")
_STATES = {
    "staged",
    "upload_intent",
    "upload_uncertain",
    "target_verified",
    "source_delete_intent",
    "complete",
}
_CHUNK_SIZE = 1024 * 1024


class RemoteFileTransactionError(RuntimeError):
    """Base class for safe, caller-visible transaction failures."""


class TransactionConflict(RemoteFileTransactionError):
    """A remote path or local transaction belongs to different content."""


class TransactionUncertain(RemoteFileTransactionError):
    """Remote state cannot be proved; the local stage has been retained."""


class TransactionCorrupt(RemoteFileTransactionError):
    """A durable local transaction artifact is malformed or inconsistent."""


@dataclass(frozen=True, slots=True)
class RemoteFileInfo:
    """Identity returned by an exact-path remote stat operation."""

    size: int
    sha256: str | None = None
    version: str | None = None

    def __post_init__(self) -> None:
        if isinstance(self.size, bool) or self.size < 0:
            raise ValueError("remote file size must be a non-negative integer")
        if self.sha256 is not None and not _SHA256_RE.fullmatch(self.sha256):
            raise ValueError("remote sha256 must be lowercase hexadecimal")


class ExactRemoteFileClient(Protocol):
    """Minimal adapter required by :func:`run_remote_file_transaction`.

    ``stat_exact`` must query the exact path, not infer presence from a parent
    directory listing.  ``upload_file_once`` must issue one non-retrying
    upload request and should fail rather than overwrite an existing path.
    """

    def stat_exact(self, path: str) -> RemoteFileInfo | None: ...

    def open_reader(self, path: str) -> ContextManager[BinaryIO]: ...

    def upload_file_once(
        self,
        target_path: str,
        source: Path,
        content_type: str,
    ) -> None: ...

    def remove_file(self, path: str) -> None: ...


@dataclass(frozen=True, slots=True)
class RemoteFileTransferSpec:
    transaction_id: str
    source_path: str
    target_path: str
    expected_size: int
    expected_sha256: str | None = None
    content_type: str = "application/octet-stream"

    def __post_init__(self) -> None:
        if not _TRANSACTION_ID_RE.fullmatch(self.transaction_id):
            raise ValueError("invalid transaction_id")
        if not self.source_path.startswith("/") or not self.target_path.startswith("/"):
            raise ValueError("source_path and target_path must be absolute remote paths")
        if self.source_path == self.target_path:
            raise ValueError("source_path and target_path must differ")
        if isinstance(self.expected_size, bool) or self.expected_size < 0:
            raise ValueError("expected_size must be a non-negative integer")
        if self.expected_sha256 is not None and not _SHA256_RE.fullmatch(
            self.expected_sha256
        ):
            raise ValueError("expected_sha256 must be lowercase hexadecimal")
        if not self.content_type:
            raise ValueError("content_type must not be empty")


@dataclass(frozen=True, slots=True)
class RemoteFileTransferResult:
    transaction_id: str
    state: str
    size: int
    sha256: str
    stage_path: Path
    journal_path: Path
    upload_calls_recorded: int
    source_deleted: bool


CheckpointHook = Callable[[str, Mapping[str, Any]], None]


@dataclass(slots=True)
class _Paths:
    directory: Path
    payload: Path
    partial: Path
    journal: Path
    lock: Path


def _utc_now() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def _fsync_directory(path: Path) -> None:
    if os.name == "nt":
        return
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _paths(stage_root: Path, transaction_id: str) -> _Paths:
    directory = stage_root / transaction_id
    return _Paths(
        directory=directory,
        payload=directory / "payload.bin",
        partial=directory / "payload.part",
        journal=directory / "journal.json",
        lock=directory / "transaction.lock",
    )


@contextlib.contextmanager
def _transaction_lock(path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor = os.open(path, os.O_CREAT | os.O_RDWR, 0o600)
    try:
        try:
            import fcntl
        except ImportError:  # pragma: no cover - Windows fallback
            fcntl = None  # type: ignore[assignment]
        if fcntl is not None:
            fcntl.flock(descriptor, fcntl.LOCK_EX)
        yield
    finally:
        if "fcntl" in locals() and fcntl is not None:
            fcntl.flock(descriptor, fcntl.LOCK_UN)
        os.close(descriptor)


def _hash_local(path: Path) -> tuple[int, str]:
    digest = hashlib.sha256()
    size = 0
    with path.open("rb") as handle:
        while chunk := handle.read(_CHUNK_SIZE):
            size += len(chunk)
            digest.update(chunk)
    return size, digest.hexdigest()


def _load_journal(path: Path) -> dict[str, Any] | None:
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return None
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise TransactionCorrupt(f"cannot read transaction journal: {path}") from exc
    if not isinstance(raw, dict):
        raise TransactionCorrupt(f"transaction journal is not an object: {path}")
    return raw


def _validate_loaded_journal(
    raw: Mapping[str, Any],
    spec: RemoteFileTransferSpec,
    paths: _Paths,
) -> dict[str, Any]:
    expected = {
        "schema_version": 1,
        "transaction_id": spec.transaction_id,
        "source_path": spec.source_path,
        "target_path": spec.target_path,
        "content_type": spec.content_type,
    }
    for key, value in expected.items():
        if raw.get(key) != value:
            raise TransactionConflict(
                f"transaction journal {key} does not match requested transfer"
            )
    state = raw.get("state")
    if state not in _STATES:
        raise TransactionCorrupt(f"invalid transaction state: {state!r}")
    size = raw.get("size")
    sha256 = raw.get("sha256")
    upload_calls = raw.get("upload_calls")
    upload_started = raw.get("upload_started")
    history = raw.get("history")
    if (
        isinstance(size, bool)
        or not isinstance(size, int)
        or size != spec.expected_size
        or not isinstance(sha256, str)
        or not _SHA256_RE.fullmatch(sha256)
        or (spec.expected_sha256 is not None and sha256 != spec.expected_sha256)
        or isinstance(upload_calls, bool)
        or not isinstance(upload_calls, int)
        or upload_calls not in {0, 1}
        or not isinstance(upload_started, bool)
        or not isinstance(history, list)
    ):
        raise TransactionCorrupt("transaction journal identity fields are invalid")
    if not paths.payload.is_file():
        if state == "complete":
            return dict(raw)
        raise TransactionCorrupt("transaction journal exists but staged payload is missing")
    local_size, local_sha256 = _hash_local(paths.payload)
    if local_size != size or local_sha256 != sha256:
        raise TransactionCorrupt("staged payload no longer matches transaction journal")
    return dict(raw)


def _checkpoint(
    paths: _Paths,
    journal: dict[str, Any],
    state: str,
    hook: CheckpointHook | None,
    *,
    event: str | None = None,
    **updates: Any,
) -> None:
    if state not in _STATES:
        raise AssertionError(f"invalid internal state: {state}")
    now = _utc_now()
    journal.update(updates)
    journal["state"] = state
    journal["updated_at"] = now
    history = journal.setdefault("history", [])
    history.append({"state": state, "event": event or state, "at": now})
    atomic_write_json(paths.journal, journal, allow_nan=False, sort_keys=True)
    if hook is not None:
        hook(event or state, dict(journal))


def _assert_info_matches_expected(
    info: RemoteFileInfo,
    spec: RemoteFileTransferSpec,
    *,
    label: str,
) -> None:
    if info.size != spec.expected_size:
        raise TransactionConflict(
            f"{label} size changed: expected={spec.expected_size}, actual={info.size}"
        )
    if spec.expected_sha256 is not None and info.sha256 is not None:
        if info.sha256 != spec.expected_sha256:
            raise TransactionConflict(f"{label} provider digest changed")


def _stream_remote_hash(
    client: ExactRemoteFileClient,
    path: str,
    *,
    expected_size: int,
) -> str:
    before = client.stat_exact(path)
    if before is None:
        raise TransactionUncertain(f"remote file is not visible at exact path: {path}")
    if before.size != expected_size:
        raise TransactionConflict(
            f"remote file size mismatch at {path}: expected={expected_size}, "
            f"actual={before.size}"
        )
    digest = hashlib.sha256()
    size = 0
    with client.open_reader(path) as reader:
        while chunk := reader.read(_CHUNK_SIZE):
            size += len(chunk)
            if size > expected_size:
                raise TransactionConflict(f"remote file grew while reading: {path}")
            digest.update(chunk)
    if size != expected_size:
        raise TransactionUncertain(
            f"remote read ended early at {path}: expected={expected_size}, actual={size}"
        )
    after = client.stat_exact(path)
    if after is None:
        raise TransactionUncertain(f"remote file vanished after read-back: {path}")
    if after.size != before.size:
        raise TransactionConflict(f"remote file size changed during read-back: {path}")
    if before.version is not None and after.version is not None:
        if before.version != after.version:
            raise TransactionConflict(f"remote file version changed during read-back: {path}")
    sha256 = digest.hexdigest()
    for info in (before, after):
        if info.sha256 is not None and info.sha256 != sha256:
            raise TransactionConflict(f"provider digest disagrees with read-back: {path}")
    return sha256


def _stage_source(
    client: ExactRemoteFileClient,
    spec: RemoteFileTransferSpec,
    paths: _Paths,
) -> tuple[int, str]:
    source_before = client.stat_exact(spec.source_path)
    if source_before is None:
        raise TransactionUncertain(
            f"source is not visible and no durable stage exists: {spec.source_path}"
        )
    _assert_info_matches_expected(source_before, spec, label="source")

    paths.directory.mkdir(parents=True, exist_ok=True)
    try:
        paths.partial.unlink()
    except FileNotFoundError:
        pass
    free = shutil.disk_usage(paths.directory).free
    if free < spec.expected_size:
        raise RemoteFileTransactionError(
            f"insufficient local staging space: need={spec.expected_size}, free={free}"
        )

    digest = hashlib.sha256()
    written = 0
    descriptor = os.open(
        paths.partial,
        os.O_CREAT | os.O_EXCL | os.O_WRONLY,
        0o600,
    )
    try:
        try:
            output = os.fdopen(descriptor, "wb")
        except BaseException:
            os.close(descriptor)
            raise
        with output, client.open_reader(spec.source_path) as reader:
            while chunk := reader.read(_CHUNK_SIZE):
                written += len(chunk)
                if written > spec.expected_size:
                    raise TransactionConflict("source exceeded its planned size while staging")
                digest.update(chunk)
                output.write(chunk)
            if written != spec.expected_size:
                raise TransactionUncertain(
                    "source read ended before its planned size while staging"
                )
            output.flush()
            os.fsync(output.fileno())
        sha256 = digest.hexdigest()
        if spec.expected_sha256 is not None and sha256 != spec.expected_sha256:
            raise TransactionConflict("staged source digest differs from planned digest")
        if source_before.sha256 is not None and source_before.sha256 != sha256:
            raise TransactionConflict("staged source digest differs from provider digest")
        source_after = client.stat_exact(spec.source_path)
        if source_after is not None:
            _assert_info_matches_expected(source_after, spec, label="source")
            if (
                source_before.version is not None
                and source_after.version is not None
                and source_before.version != source_after.version
            ):
                raise TransactionConflict("source version changed while staging")
        os.replace(paths.partial, paths.payload)
        _fsync_directory(paths.directory)
        return written, sha256
    finally:
        try:
            paths.partial.unlink()
        except FileNotFoundError:
            pass


def _verify_target(
    client: ExactRemoteFileClient,
    spec: RemoteFileTransferSpec,
    journal: Mapping[str, Any],
) -> bool:
    info = client.stat_exact(spec.target_path)
    if info is None:
        return False
    expected_size = int(journal["size"])
    expected_sha256 = str(journal["sha256"])
    if info.size != expected_size:
        raise TransactionConflict(
            f"target exists with a different size: {spec.target_path}"
        )
    actual_sha256 = _stream_remote_hash(
        client,
        spec.target_path,
        expected_size=expected_size,
    )
    if actual_sha256 != expected_sha256:
        raise TransactionConflict(
            f"target exists with different content: {spec.target_path}"
        )
    return True


def _source_state(
    client: ExactRemoteFileClient,
    spec: RemoteFileTransferSpec,
    journal: Mapping[str, Any],
) -> str:
    info = client.stat_exact(spec.source_path)
    if info is None:
        return "missing"
    if info.size != journal["size"]:
        raise TransactionConflict("source path now contains a different-sized file")
    source_sha256 = _stream_remote_hash(
        client,
        spec.source_path,
        expected_size=int(journal["size"]),
    )
    if source_sha256 != journal["sha256"]:
        raise TransactionConflict("source path now contains different content")
    return "matching"


def _result(paths: _Paths, journal: Mapping[str, Any]) -> RemoteFileTransferResult:
    return RemoteFileTransferResult(
        transaction_id=str(journal["transaction_id"]),
        state=str(journal["state"]),
        size=int(journal["size"]),
        sha256=str(journal["sha256"]),
        stage_path=paths.payload,
        journal_path=paths.journal,
        upload_calls_recorded=int(journal["upload_calls"]),
        source_deleted=bool(journal.get("source_deleted", False)),
    )


def prepare_remote_file_transaction(
    client: ExactRemoteFileClient,
    *,
    stage_root: Path,
    spec: RemoteFileTransferSpec,
    checkpoint_hook: CheckpointHook | None = None,
) -> RemoteFileTransferResult:
    """Durably stage and hash a source without starting any remote mutation."""
    paths = _paths(stage_root, spec.transaction_id)
    with _transaction_lock(paths.lock):
        raw = _load_journal(paths.journal)
        if raw is not None:
            journal = _validate_loaded_journal(raw, spec, paths)
            return _result(paths, journal)
        if paths.payload.exists():
            # A payload without its atomic identity journal cannot be trusted.
            paths.payload.unlink()
            _fsync_directory(paths.directory)
        size, sha256 = _stage_source(client, spec, paths)
        journal: dict[str, Any] = {
            "schema_version": 1,
            "transaction_id": spec.transaction_id,
            "source_path": spec.source_path,
            "target_path": spec.target_path,
            "content_type": spec.content_type,
            "size": size,
            "sha256": sha256,
            "upload_started": False,
            "upload_calls": 0,
            "source_deleted": False,
            "history": [],
        }
        _checkpoint(paths, journal, "staged", checkpoint_hook)
        return _result(paths, journal)


def discard_completed_remote_file_transaction(
    *,
    stage_root: Path,
    spec: RemoteFileTransferSpec,
) -> None:
    """Release payload bytes only after the durable journal says complete.

    Uncertain and interrupted transactions are deliberately retained.  The
    small journal and lock files remain as an audit receipt while the large
    payload is removed to keep one-file-at-a-time staging bounded.
    """
    paths = _paths(stage_root, spec.transaction_id)
    with _transaction_lock(paths.lock):
        raw = _load_journal(paths.journal)
        if raw is None:
            return
        journal = _validate_loaded_journal(raw, spec, paths)
        if journal.get("state") != "complete":
            raise TransactionUncertain(
                "refusing to discard payload for an incomplete transaction"
            )
        try:
            paths.payload.unlink()
        except FileNotFoundError:
            return
        _fsync_directory(paths.directory)


def run_remote_file_transaction(
    client: ExactRemoteFileClient,
    *,
    stage_root: Path,
    spec: RemoteFileTransferSpec,
    checkpoint_hook: CheckpointHook | None = None,
) -> RemoteFileTransferResult:
    """Copy one remote file through a durable local stage, then delete source.

    Reinvoking this function with the same ``stage_root`` and ``spec`` resumes
    the journal.  Once ``upload_started`` is durable, the function only
    reconciles the target and will never call ``upload_file_once`` again.
    """

    paths = _paths(stage_root, spec.transaction_id)
    with _transaction_lock(paths.lock):
        raw = _load_journal(paths.journal)
        if raw is None:
            if paths.payload.exists():
                # Without its digest checkpoint a payload cannot be trusted as
                # belonging to this transaction, even if its size happens to
                # match.  The source has not been uploaded at this point.
                paths.payload.unlink()
                _fsync_directory(paths.directory)
            size, sha256 = _stage_source(client, spec, paths)
            journal: dict[str, Any] = {
                "schema_version": 1,
                "transaction_id": spec.transaction_id,
                "source_path": spec.source_path,
                "target_path": spec.target_path,
                "content_type": spec.content_type,
                "size": size,
                "sha256": sha256,
                "upload_started": False,
                "upload_calls": 0,
                "source_deleted": False,
                "history": [],
            }
            _checkpoint(paths, journal, "staged", checkpoint_hook)
        else:
            journal = _validate_loaded_journal(raw, spec, paths)

        while True:
            state = str(journal["state"])
            if state == "complete":
                return _result(paths, journal)

            if state == "staged":
                try:
                    already_present = _verify_target(client, spec, journal)
                except TransactionUncertain as exc:
                    raise TransactionUncertain(
                        f"cannot preflight target; local stage retained: {exc}"
                    ) from exc
                if already_present:
                    _checkpoint(
                        paths,
                        journal,
                        "target_verified",
                        checkpoint_hook,
                        event="target_verified_existing",
                    )
                    continue
                _checkpoint(paths, journal, "upload_intent", checkpoint_hook)
                continue

            if state == "upload_intent" and not journal["upload_started"]:
                # This second durable intent records that entering the upload
                # call is now possible.  A crash after it must reconcile only.
                journal["upload_started"] = True
                journal["upload_calls"] = 1
                _checkpoint(
                    paths,
                    journal,
                    "upload_intent",
                    checkpoint_hook,
                    event="upload_started",
                )
                try:
                    client.upload_file_once(
                        spec.target_path,
                        paths.payload,
                        spec.content_type,
                    )
                except Exception as exc:
                    _checkpoint(
                        paths,
                        journal,
                        "upload_uncertain",
                        checkpoint_hook,
                        error=f"{type(exc).__name__}: {exc}",
                    )
                else:
                    try:
                        visible = _verify_target(client, spec, journal)
                    except (TransactionUncertain, TransactionConflict) as exc:
                        _checkpoint(
                            paths,
                            journal,
                            "upload_uncertain",
                            checkpoint_hook,
                            error=f"{type(exc).__name__}: {exc}",
                        )
                    else:
                        if visible:
                            _checkpoint(
                                paths,
                                journal,
                                "target_verified",
                                checkpoint_hook,
                            )
                        else:
                            _checkpoint(
                                paths,
                                journal,
                                "upload_uncertain",
                                checkpoint_hook,
                                error="target is not visible after upload response",
                            )
                continue

            if state in {"upload_intent", "upload_uncertain"}:
                # upload_started is true here.  Never invoke upload again.
                try:
                    visible = _verify_target(client, spec, journal)
                except TransactionConflict:
                    raise
                except Exception as exc:
                    raise TransactionUncertain(
                        f"target reconciliation failed; local stage retained: {exc}"
                    ) from exc
                if not visible:
                    raise TransactionUncertain(
                        "target is not visible after an upload may have started; "
                        f"local stage retained at {paths.payload}"
                    )
                _checkpoint(
                    paths,
                    journal,
                    "target_verified",
                    checkpoint_hook,
                    event="target_verified_after_reconciliation",
                )
                continue

            if state == "target_verified":
                if not _verify_target(client, spec, journal):
                    raise TransactionUncertain(
                        "verified target is no longer visible; source was retained"
                    )
                source_state = _source_state(client, spec, journal)
                _checkpoint(
                    paths,
                    journal,
                    "source_delete_intent",
                    checkpoint_hook,
                    source_was_visible=source_state == "matching",
                )
                continue

            if state == "source_delete_intent":
                if not _verify_target(client, spec, journal):
                    raise TransactionUncertain(
                        "target cannot be reverified before source deletion"
                    )
                source_state = _source_state(client, spec, journal)
                if source_state == "matching":
                    try:
                        client.remove_file(spec.source_path)
                    except Exception as exc:
                        # Delete requests are safe to reconcile/retry because
                        # the byte-identical target and local stage are durable.
                        if client.stat_exact(spec.source_path) is not None:
                            raise TransactionUncertain(
                                f"source deletion is uncertain: {exc}"
                            ) from exc
                    if client.stat_exact(spec.source_path) is not None:
                        raise TransactionUncertain(
                            "source remains visible after deletion request"
                        )
                _checkpoint(
                    paths,
                    journal,
                    "complete",
                    checkpoint_hook,
                    source_deleted=True,
                )
                continue

            raise TransactionCorrupt(f"unhandled transaction state: {state}")


__all__ = [
    "ExactRemoteFileClient",
    "RemoteFileInfo",
    "RemoteFileTransactionError",
    "RemoteFileTransferResult",
    "RemoteFileTransferSpec",
    "TransactionConflict",
    "TransactionCorrupt",
    "TransactionUncertain",
    "discard_completed_remote_file_transaction",
    "prepare_remote_file_transaction",
    "run_remote_file_transaction",
]
