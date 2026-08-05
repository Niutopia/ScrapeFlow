"""Recoverable exact-path deletion for remote residual files.

Known non-feature residuals must leave the media library, but deleting them
directly makes a later title-level rollback impossible.  This module stages
the complete remote payload locally, binds it to a SHA-256 journal, removes
only the exact matching source, and retains the payload until the caller
explicitly commits the title acceptance.

An unknown or differently identified object is never removed.  A quarantined
payload can be restored to its original path through a separate exactly-once
local upload transaction.
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

from .local_upload_transaction import (
    LocalUploadSpec,
    run_local_upload_transaction,
)
from .serialization import atomic_write_json


_TRANSACTION_ID_RE = re.compile(r"\A[A-Za-z0-9][A-Za-z0-9._-]{0,127}\Z")
_SHA256_RE = re.compile(r"\A[0-9a-f]{64}\Z")
_STATES = {"staged", "delete_intent", "quarantined", "committed", "restored"}
_CHUNK_SIZE = 1024 * 1024


class RemoteDeleteTransactionError(RuntimeError):
    """Base class for recoverable deletion failures."""


class RemoteDeleteConflict(RemoteDeleteTransactionError):
    """The source, local stage, or journal belongs to different content."""


class RemoteDeleteUncertain(RemoteDeleteTransactionError):
    """The exact state cannot be proved; the local payload is retained."""


class RemoteDeleteCorrupt(RemoteDeleteTransactionError):
    """A persisted deletion transaction is malformed or incomplete."""


@dataclass(frozen=True, slots=True)
class RemoteDeleteInfo:
    size: int
    sha256: str | None = None
    version: str | None = None

    def __post_init__(self) -> None:
        if isinstance(self.size, bool) or self.size < 0:
            raise ValueError("remote file size must be a non-negative integer")
        if self.sha256 is not None and not _SHA256_RE.fullmatch(self.sha256):
            raise ValueError("remote sha256 must be lowercase hexadecimal")


class ExactRemoteDeleteClient(Protocol):
    def stat_exact(self, path: str) -> RemoteDeleteInfo | None: ...

    def open_reader(self, path: str) -> ContextManager[BinaryIO]: ...

    def upload_file_once(
        self, target_path: str, source: Path, content_type: str,
    ) -> None: ...

    def remove_file(self, path: str) -> None: ...


@dataclass(frozen=True, slots=True)
class RemoteDeleteSpec:
    transaction_id: str
    source_path: str
    expected_size: int
    expected_sha256: str | None = None
    content_type: str = "application/octet-stream"

    def __post_init__(self) -> None:
        if not _TRANSACTION_ID_RE.fullmatch(self.transaction_id):
            raise ValueError("invalid transaction_id")
        if not self.source_path.startswith("/"):
            raise ValueError("source_path must be an absolute remote path")
        if isinstance(self.expected_size, bool) or self.expected_size < 0:
            raise ValueError("expected_size must be a non-negative integer")
        if self.expected_sha256 is not None and not _SHA256_RE.fullmatch(
            self.expected_sha256
        ):
            raise ValueError("expected_sha256 must be lowercase hexadecimal")
        if not self.content_type:
            raise ValueError("content_type must not be empty")


@dataclass(frozen=True, slots=True)
class RemoteDeleteResult:
    transaction_id: str
    state: str
    size: int
    sha256: str
    stage_path: Path
    journal_path: Path
    remove_calls_recorded: int


CheckpointHook = Callable[[str, Mapping[str, Any]], None]


@dataclass(frozen=True, slots=True)
class _Paths:
    directory: Path
    payload: Path
    partial: Path
    journal: Path
    lock: Path
    restore_root: Path


def _paths(stage_root: Path, transaction_id: str) -> _Paths:
    directory = stage_root / transaction_id
    return _Paths(
        directory=directory,
        payload=directory / "payload.bin",
        partial=directory / "payload.part",
        journal=directory / "journal.json",
        lock=directory / "transaction.lock",
        restore_root=directory / "restore-upload",
    )


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


@contextlib.contextmanager
def _transaction_lock(path: Path):
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
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
    try:
        with path.open("rb") as handle:
            while chunk := handle.read(_CHUNK_SIZE):
                size += len(chunk)
                digest.update(chunk)
    except OSError as exc:
        raise RemoteDeleteCorrupt(f"cannot read staged payload: {path}") from exc
    return size, digest.hexdigest()


def _load_journal(path: Path) -> dict[str, Any] | None:
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return None
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RemoteDeleteCorrupt(f"cannot read deletion journal: {path}") from exc
    if not isinstance(raw, dict):
        raise RemoteDeleteCorrupt("deletion journal is not an object")
    return raw


def _validate_journal(
    raw: Mapping[str, Any], spec: RemoteDeleteSpec, paths: _Paths,
) -> dict[str, Any]:
    expected = {
        "schema_version": 1,
        "kind": "recoverable_remote_delete",
        "transaction_id": spec.transaction_id,
        "source_path": spec.source_path,
        "content_type": spec.content_type,
        "size": spec.expected_size,
    }
    for key, value in expected.items():
        if raw.get(key) != value:
            raise RemoteDeleteConflict(
                f"deletion journal {key} does not match requested file"
            )
    state = raw.get("state")
    sha256 = raw.get("sha256")
    remove_calls = raw.get("remove_calls")
    history = raw.get("history")
    if (
        state not in _STATES
        or not isinstance(sha256, str)
        or not _SHA256_RE.fullmatch(sha256)
        or (spec.expected_sha256 is not None and sha256 != spec.expected_sha256)
        or isinstance(remove_calls, bool)
        or not isinstance(remove_calls, int)
        or remove_calls < 0
        or not isinstance(history, list)
    ):
        raise RemoteDeleteCorrupt("deletion journal fields are invalid")
    if not paths.payload.is_file():
        if state == "committed":
            return dict(raw)
        raise RemoteDeleteCorrupt("deletion journal exists but payload is missing")
    size, actual_sha256 = _hash_local(paths.payload)
    if size != spec.expected_size or actual_sha256 != sha256:
        raise RemoteDeleteCorrupt("staged deletion payload does not match journal")
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
        raise AssertionError(f"invalid deletion state: {state}")
    now = _utc_now()
    journal.update(updates)
    journal["state"] = state
    journal["updated_at"] = now
    journal.setdefault("history", []).append({
        "state": state,
        "event": event or state,
        "at": now,
    })
    atomic_write_json(paths.journal, journal, allow_nan=False, sort_keys=True)
    if hook is not None:
        hook(event or state, dict(journal))


def _stream_remote_hash(
    client: ExactRemoteDeleteClient, path: str, *, expected_size: int,
) -> str:
    before = client.stat_exact(path)
    if before is None:
        raise RemoteDeleteUncertain(f"remote file is not visible: {path}")
    if before.size != expected_size:
        raise RemoteDeleteConflict(
            f"remote file size differs: expected={expected_size}, actual={before.size}"
        )
    digest = hashlib.sha256()
    size = 0
    try:
        with client.open_reader(path) as reader:
            while chunk := reader.read(_CHUNK_SIZE):
                size += len(chunk)
                if size > expected_size:
                    raise RemoteDeleteConflict("remote file grew during read-back")
                digest.update(chunk)
    except RemoteDeleteTransactionError:
        raise
    except Exception as exc:
        raise RemoteDeleteUncertain(f"cannot read exact remote file: {path}") from exc
    if size != expected_size:
        raise RemoteDeleteUncertain(
            f"remote read ended early: expected={expected_size}, actual={size}"
        )
    after = client.stat_exact(path)
    if after is None:
        raise RemoteDeleteUncertain(f"remote file vanished after read-back: {path}")
    if after.size != before.size:
        raise RemoteDeleteConflict("remote file size changed during read-back")
    if before.version is not None and after.version is not None:
        if before.version != after.version:
            raise RemoteDeleteConflict("remote file version changed during read-back")
    value = digest.hexdigest()
    for info in (before, after):
        if info.sha256 is not None and info.sha256 != value:
            raise RemoteDeleteConflict("provider digest disagrees with read-back")
    return value


def _source_state(
    client: ExactRemoteDeleteClient,
    spec: RemoteDeleteSpec,
    journal: Mapping[str, Any],
) -> str:
    info = client.stat_exact(spec.source_path)
    if info is None:
        return "missing"
    if info.size != spec.expected_size:
        raise RemoteDeleteConflict("source path contains a different-sized file")
    actual_sha256 = _stream_remote_hash(
        client, spec.source_path, expected_size=spec.expected_size,
    )
    if actual_sha256 != journal["sha256"]:
        raise RemoteDeleteConflict("source path contains different content")
    return "matching"


def _stage_source(
    client: ExactRemoteDeleteClient, spec: RemoteDeleteSpec, paths: _Paths,
) -> tuple[int, str]:
    before = client.stat_exact(spec.source_path)
    if before is None:
        raise RemoteDeleteUncertain(
            f"source is absent and no recoverable deletion stage exists: {spec.source_path}"
        )
    if before.size != spec.expected_size:
        raise RemoteDeleteConflict("source size differs from deletion plan")
    paths.directory.mkdir(mode=0o700, parents=True, exist_ok=True)
    try:
        paths.partial.unlink()
    except FileNotFoundError:
        pass
    free = shutil.disk_usage(paths.directory).free
    if free < spec.expected_size:
        raise RemoteDeleteTransactionError(
            f"insufficient local quarantine space: need={spec.expected_size}, free={free}"
        )
    descriptor = os.open(paths.partial, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    digest = hashlib.sha256()
    written = 0
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
                    raise RemoteDeleteConflict("source grew while staging for deletion")
                digest.update(chunk)
                output.write(chunk)
            if written != spec.expected_size:
                raise RemoteDeleteUncertain("source read ended early while staging")
            output.flush()
            os.fsync(output.fileno())
        sha256 = digest.hexdigest()
        if spec.expected_sha256 is not None and sha256 != spec.expected_sha256:
            raise RemoteDeleteConflict("staged digest differs from deletion plan")
        if before.sha256 is not None and before.sha256 != sha256:
            raise RemoteDeleteConflict("provider digest differs from staged payload")
        after = client.stat_exact(spec.source_path)
        if after is None or after.size != before.size:
            raise RemoteDeleteUncertain("source changed while staging for deletion")
        if before.version is not None and after.version is not None:
            if before.version != after.version:
                raise RemoteDeleteConflict("source version changed while staging")
        os.replace(paths.partial, paths.payload)
        _fsync_directory(paths.directory)
        return written, sha256
    finally:
        try:
            paths.partial.unlink()
        except FileNotFoundError:
            pass


def _result(paths: _Paths, journal: Mapping[str, Any]) -> RemoteDeleteResult:
    return RemoteDeleteResult(
        transaction_id=str(journal["transaction_id"]),
        state=str(journal["state"]),
        size=int(journal["size"]),
        sha256=str(journal["sha256"]),
        stage_path=paths.payload,
        journal_path=paths.journal,
        remove_calls_recorded=int(journal["remove_calls"]),
    )


def prepare_remote_delete_transaction(
    client: ExactRemoteDeleteClient,
    *,
    stage_root: Path,
    spec: RemoteDeleteSpec,
    checkpoint_hook: CheckpointHook | None = None,
) -> RemoteDeleteResult:
    """Create the durable local SHA-256 stage without deleting anything."""
    paths = _paths(stage_root, spec.transaction_id)
    with _transaction_lock(paths.lock):
        raw = _load_journal(paths.journal)
        if raw is not None:
            return _result(paths, _validate_journal(raw, spec, paths))
        if paths.payload.exists():
            paths.payload.unlink()
            _fsync_directory(paths.directory)
        size, sha256 = _stage_source(client, spec, paths)
        journal: dict[str, Any] = {
            "schema_version": 1,
            "kind": "recoverable_remote_delete",
            "transaction_id": spec.transaction_id,
            "source_path": spec.source_path,
            "content_type": spec.content_type,
            "size": size,
            "sha256": sha256,
            "remove_calls": 0,
            "history": [],
        }
        _checkpoint(paths, journal, "staged", checkpoint_hook)
        return _result(paths, journal)


def run_remote_delete_transaction(
    client: ExactRemoteDeleteClient,
    *,
    stage_root: Path,
    spec: RemoteDeleteSpec,
    checkpoint_hook: CheckpointHook | None = None,
) -> RemoteDeleteResult:
    """Quarantine one exact source locally, then remove only matching bytes."""
    paths = _paths(stage_root, spec.transaction_id)
    prepare_remote_delete_transaction(
        client, stage_root=stage_root, spec=spec, checkpoint_hook=checkpoint_hook,
    )
    with _transaction_lock(paths.lock):
        raw = _load_journal(paths.journal)
        if raw is None:  # pragma: no cover - lock-protected invariant
            raise RemoteDeleteCorrupt("prepared deletion journal disappeared")
        journal = _validate_journal(raw, spec, paths)
        state = str(journal["state"])
        if state in {"quarantined", "committed", "restored"}:
            if state == "quarantined" and _source_state(client, spec, journal) != "missing":
                raise RemoteDeleteConflict(
                    "quarantined source path became occupied; refusing further mutation"
                )
            return _result(paths, journal)
        if state == "staged":
            if _source_state(client, spec, journal) != "matching":
                raise RemoteDeleteUncertain("staged source unexpectedly disappeared")
            _checkpoint(paths, journal, "delete_intent", checkpoint_hook)
        if _source_state(client, spec, journal) == "missing":
            _checkpoint(
                paths, journal, "quarantined", checkpoint_hook,
                event="source_absent_after_delete",
            )
            return _result(paths, journal)
        journal["remove_calls"] = int(journal["remove_calls"]) + 1
        _checkpoint(
            paths, journal, "delete_intent", checkpoint_hook,
            event="remove_started",
        )
        try:
            client.remove_file(spec.source_path)
        except Exception as exc:
            if _source_state(client, spec, journal) != "missing":
                raise RemoteDeleteUncertain(
                    f"source deletion did not converge; local payload retained: {exc}"
                ) from exc
        if _source_state(client, spec, journal) != "missing":
            raise RemoteDeleteUncertain(
                "source remains visible after deletion; local payload retained"
            )
        _checkpoint(paths, journal, "quarantined", checkpoint_hook)
        return _result(paths, journal)


def commit_remote_delete_transaction(
    client: ExactRemoteDeleteClient,
    *,
    stage_root: Path,
    spec: RemoteDeleteSpec,
    checkpoint_hook: CheckpointHook | None = None,
) -> RemoteDeleteResult:
    """Release payload bytes only after the enclosing title was accepted."""
    paths = _paths(stage_root, spec.transaction_id)
    with _transaction_lock(paths.lock):
        raw = _load_journal(paths.journal)
        if raw is None:
            raise RemoteDeleteCorrupt("deletion transaction does not exist")
        journal = _validate_journal(raw, spec, paths)
        if journal["state"] == "committed":
            return _result(paths, journal)
        if journal["state"] != "quarantined":
            raise RemoteDeleteUncertain(
                "deletion cannot be committed before quarantine succeeds"
            )
        if _source_state(client, spec, journal) != "missing":
            raise RemoteDeleteConflict(
                "source path is occupied; refusing to discard recovery payload"
            )
        _checkpoint(paths, journal, "committed", checkpoint_hook)
        try:
            paths.payload.unlink()
        except FileNotFoundError:  # pragma: no cover - validated above
            pass
        _fsync_directory(paths.directory)
        return _result(paths, journal)


def restore_remote_delete_transaction(
    client: ExactRemoteDeleteClient,
    *,
    stage_root: Path,
    spec: RemoteDeleteSpec,
    checkpoint_hook: CheckpointHook | None = None,
) -> RemoteDeleteResult:
    """Restore quarantined bytes to the exact original path without overwrite."""
    paths = _paths(stage_root, spec.transaction_id)
    with _transaction_lock(paths.lock):
        raw = _load_journal(paths.journal)
        if raw is None:
            raise RemoteDeleteCorrupt("deletion transaction does not exist")
        journal = _validate_journal(raw, spec, paths)
        if journal["state"] == "committed":
            raise RemoteDeleteUncertain("committed deletion no longer has recovery bytes")
        source_state = _source_state(client, spec, journal)
        if source_state == "missing":
            restore_id = "restore-" + hashlib.sha256(
                f"{spec.transaction_id}\0{spec.source_path}".encode(
                    "utf-8", errors="surrogatepass",
                )
            ).hexdigest()[:48]
            result = run_local_upload_transaction(
                client,
                transaction_root=paths.restore_root,
                spec=LocalUploadSpec(
                    transaction_id=restore_id,
                    source_path=paths.payload.resolve(),
                    target_path=spec.source_path,
                    expected_size=spec.expected_size,
                    expected_sha256=str(journal["sha256"]),
                    content_type=spec.content_type,
                ),
            )
            if _source_state(client, spec, journal) != "matching":
                raise RemoteDeleteUncertain("restored source cannot be reverified")
            receipt = str(result.receipt_sha256 or "")
        else:
            receipt = "already-present"
        _checkpoint(
            paths, journal, "restored", checkpoint_hook,
            restore_receipt_sha256=receipt,
        )
        return _result(paths, journal)


__all__ = [
    "ExactRemoteDeleteClient",
    "RemoteDeleteConflict",
    "RemoteDeleteCorrupt",
    "RemoteDeleteInfo",
    "RemoteDeleteResult",
    "RemoteDeleteSpec",
    "RemoteDeleteTransactionError",
    "RemoteDeleteUncertain",
    "commit_remote_delete_transaction",
    "prepare_remote_delete_transaction",
    "restore_remote_delete_transaction",
    "run_remote_delete_transaction",
]
