"""Crash-safe, exactly-once local-to-remote upload transactions.

The local source is already the durable payload.  Before the first remote
request this module hashes it, persists an ``upload_started`` checkpoint, and
then permits exactly one upload call for that transaction.  Any ambiguous
response is reconciled only by exact-path stat plus a complete read-back hash;
an invisible target remains uncertain and the caller must retain the source.
"""

from __future__ import annotations

import contextlib
import hashlib
import json
import os
import re
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, BinaryIO, Callable, ContextManager, Mapping, Protocol, Sequence

from .serialization import atomic_write_json, canonical_json_bytes


_TRANSACTION_ID_RE = re.compile(r"\A[A-Za-z0-9][A-Za-z0-9._-]{0,127}\Z")
_SHA256_RE = re.compile(r"\A[0-9a-f]{64}\Z")
_CHUNK_SIZE = 1024 * 1024
_STATES = {"prepared", "upload_started", "upload_uncertain", "complete"}


class LocalUploadTransactionError(RuntimeError):
    """Base class for durable local upload failures."""


class LocalUploadConflict(LocalUploadTransactionError):
    """The source, target, or journal belongs to different content."""


class LocalUploadUncertain(LocalUploadTransactionError):
    """The target cannot be proved and the local source must be retained."""


class LocalUploadCorrupt(LocalUploadTransactionError):
    """A persisted local upload transaction is malformed."""


@dataclass(frozen=True, slots=True)
class LocalUploadRemoteInfo:
    size: int
    sha256: str | None = None
    version: str | None = None

    def __post_init__(self) -> None:
        if isinstance(self.size, bool) or self.size < 0:
            raise ValueError("remote size must be a non-negative integer")
        if self.sha256 is not None and not _SHA256_RE.fullmatch(self.sha256):
            raise ValueError("remote sha256 must be lowercase hexadecimal")


class ExactLocalUploadClient(Protocol):
    """Exact-path remote operations required by the upload transaction."""

    def stat_exact(self, path: str) -> LocalUploadRemoteInfo | None: ...

    def open_reader(self, path: str) -> ContextManager[BinaryIO]: ...

    def upload_file_once(
        self, target_path: str, source: Path, content_type: str,
    ) -> None: ...


@dataclass(frozen=True, slots=True)
class LocalUploadSpec:
    transaction_id: str
    source_path: Path
    target_path: str
    expected_size: int
    expected_sha256: str | None = None
    content_type: str = "application/octet-stream"

    def __post_init__(self) -> None:
        if not _TRANSACTION_ID_RE.fullmatch(self.transaction_id):
            raise ValueError("invalid transaction_id")
        if not self.source_path.is_absolute():
            raise ValueError("source_path must be absolute")
        if not self.target_path.startswith("/"):
            raise ValueError("target_path must be absolute")
        if isinstance(self.expected_size, bool) or self.expected_size < 0:
            raise ValueError("expected_size must be a non-negative integer")
        if self.expected_sha256 is not None and not _SHA256_RE.fullmatch(
            self.expected_sha256
        ):
            raise ValueError("expected_sha256 must be lowercase hexadecimal")
        if not self.content_type:
            raise ValueError("content_type must not be empty")


@dataclass(frozen=True, slots=True)
class LocalUploadResult:
    transaction_id: str
    state: str
    size: int
    sha256: str
    journal_path: Path
    receipt_sha256: str | None
    upload_calls_recorded: int


CheckpointHook = Callable[[str, Mapping[str, Any]], None]


def deterministic_local_upload_id(source_path: Path, target_path: str) -> str:
    """Bind one stable transaction to one exact local source and target path."""
    identity = (
        f"local-upload\0{source_path.resolve()}\0{target_path}"
        .encode("utf-8", errors="surrogatepass")
    )
    return "local-upload-" + hashlib.sha256(identity).hexdigest()[:48]


def _utc_now() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


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


def _hash_file(path: Path, *, expected_size: int) -> str:
    try:
        before = path.stat()
    except OSError as exc:
        raise LocalUploadUncertain(f"local upload source is unavailable: {path}") from exc
    if before.st_size != expected_size:
        raise LocalUploadConflict(
            f"local source size changed: expected={expected_size}, actual={before.st_size}"
        )
    digest = hashlib.sha256()
    size = 0
    try:
        with path.open("rb") as handle:
            while chunk := handle.read(_CHUNK_SIZE):
                size += len(chunk)
                if size > expected_size:
                    raise LocalUploadConflict("local source grew while hashing")
                digest.update(chunk)
        after = path.stat()
    except OSError as exc:
        raise LocalUploadUncertain(f"cannot verify local upload source: {path}") from exc
    if size != expected_size:
        raise LocalUploadConflict(
            f"local source read size changed: expected={expected_size}, actual={size}"
        )
    if (
        after.st_size != before.st_size
        or after.st_mtime_ns != before.st_mtime_ns
        or getattr(after, "st_ino", None) != getattr(before, "st_ino", None)
    ):
        raise LocalUploadConflict("local source changed while hashing")
    return digest.hexdigest()


def _load_journal(path: Path) -> dict[str, Any] | None:
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return None
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise LocalUploadCorrupt(f"cannot read local upload journal: {path}") from exc
    if not isinstance(raw, dict):
        raise LocalUploadCorrupt("local upload journal is not an object")
    return raw


def _receipt_core(journal: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "transaction_id": journal["transaction_id"],
        "target_path": journal["target_path"],
        "size": journal["size"],
        "sha256": journal["sha256"],
        "verified_at": journal["verified_at"],
    }


def _validate_journal(
    raw: Mapping[str, Any], spec: LocalUploadSpec, *, source_sha256: str,
) -> dict[str, Any]:
    expected = {
        "schema_version": 1,
        "kind": "durable_local_upload",
        "transaction_id": spec.transaction_id,
        "source_path": str(spec.source_path),
        "target_path": spec.target_path,
        "content_type": spec.content_type,
        "size": spec.expected_size,
        "sha256": source_sha256,
    }
    for key, value in expected.items():
        if raw.get(key) != value:
            raise LocalUploadConflict(
                f"local upload journal {key} does not match requested content"
            )
    state = raw.get("state")
    upload_started = raw.get("upload_started")
    upload_calls = raw.get("upload_calls")
    history = raw.get("history")
    if (
        state not in _STATES
        or not isinstance(upload_started, bool)
        or isinstance(upload_calls, bool)
        or not isinstance(upload_calls, int)
        or upload_calls not in {0, 1}
        or not isinstance(history, list)
    ):
        raise LocalUploadCorrupt("local upload journal state fields are invalid")
    if upload_calls == 1 and not upload_started:
        raise LocalUploadCorrupt("upload call count exists without upload_started")
    if state == "prepared" and (upload_started or upload_calls != 0):
        raise LocalUploadCorrupt("prepared journal crossed the upload boundary")
    if state in {"upload_started", "upload_uncertain"} and (
        not upload_started or upload_calls != 1
    ):
        raise LocalUploadCorrupt("local upload journal has an invalid upload boundary")
    if state == "complete" and (upload_started, upload_calls) not in {
        (False, 0), (True, 1),
    }:
        raise LocalUploadCorrupt("completed journal has an invalid upload boundary")
    if state == "complete":
        verified_at = raw.get("verified_at")
        receipt_sha256 = raw.get("receipt_sha256")
        if (
            not isinstance(verified_at, str)
            or not verified_at
            or not isinstance(receipt_sha256, str)
            or not _SHA256_RE.fullmatch(receipt_sha256)
            or hashlib.sha256(canonical_json_bytes(_receipt_core(raw))).hexdigest()
            != receipt_sha256
        ):
            raise LocalUploadCorrupt("completed local upload receipt is invalid")
    return dict(raw)


def _checkpoint(
    journal_path: Path,
    journal: dict[str, Any],
    state: str,
    hook: CheckpointHook | None,
    *,
    event: str | None = None,
    **updates: Any,
) -> None:
    if state not in _STATES:
        raise AssertionError(f"invalid local upload state: {state}")
    now = _utc_now()
    journal.update(updates)
    journal["state"] = state
    journal["updated_at"] = now
    journal.setdefault("history", []).append({
        "state": state, "event": event or state, "at": now,
    })
    atomic_write_json(journal_path, journal, allow_nan=False, sort_keys=True)
    if hook is not None:
        hook(event or state, dict(journal))


def _stream_remote_hash(
    client: ExactLocalUploadClient, path: str, *, expected_size: int,
) -> str:
    before = client.stat_exact(path)
    if before is None:
        raise LocalUploadUncertain(f"remote target is not visible: {path}")
    if before.size != expected_size:
        raise LocalUploadConflict(
            f"remote target size differs: expected={expected_size}, actual={before.size}"
        )
    digest = hashlib.sha256()
    size = 0
    try:
        with client.open_reader(path) as reader:
            while chunk := reader.read(_CHUNK_SIZE):
                size += len(chunk)
                if size > expected_size:
                    raise LocalUploadConflict("remote target grew during read-back")
                digest.update(chunk)
    except LocalUploadTransactionError:
        raise
    except Exception as exc:
        raise LocalUploadUncertain(f"cannot read back exact remote target: {path}") from exc
    if size != expected_size:
        raise LocalUploadUncertain(
            f"remote target read ended early: expected={expected_size}, actual={size}"
        )
    after = client.stat_exact(path)
    if after is None:
        raise LocalUploadUncertain(f"remote target vanished after read-back: {path}")
    if after.size != before.size:
        raise LocalUploadConflict("remote target size changed during read-back")
    if before.version is not None and after.version is not None:
        if before.version != after.version:
            raise LocalUploadConflict("remote target version changed during read-back")
    value = digest.hexdigest()
    for info in (before, after):
        if info.sha256 is not None and info.sha256 != value:
            raise LocalUploadConflict("provider digest disagrees with target read-back")
    return value


def _verify_target_once(
    client: ExactLocalUploadClient, spec: LocalUploadSpec, *, sha256: str,
) -> bool:
    info = client.stat_exact(spec.target_path)
    if info is None:
        return False
    if info.size != spec.expected_size:
        raise LocalUploadConflict(
            f"target exists with different size: {spec.target_path}"
        )
    actual_sha256 = _stream_remote_hash(
        client, spec.target_path, expected_size=spec.expected_size,
    )
    if actual_sha256 != sha256:
        raise LocalUploadConflict(
            f"target exists with different content: {spec.target_path}"
        )
    return True


def _reconcile_target(
    client: ExactLocalUploadClient,
    spec: LocalUploadSpec,
    *,
    sha256: str,
    delays: Sequence[float],
) -> tuple[bool, str | None]:
    last_error: str | None = None
    for delay in delays:
        if delay > 0:
            time.sleep(delay)
        try:
            if _verify_target_once(client, spec, sha256=sha256):
                return True, None
        except LocalUploadConflict:
            raise
        except Exception as exc:
            last_error = f"{type(exc).__name__}: {exc}"
    return False, last_error


def _complete(
    journal_path: Path,
    journal: dict[str, Any],
    hook: CheckpointHook | None,
) -> None:
    verified_at = _utc_now()
    journal["verified_at"] = verified_at
    journal["receipt_sha256"] = hashlib.sha256(
        canonical_json_bytes(_receipt_core(journal))
    ).hexdigest()
    _checkpoint(journal_path, journal, "complete", hook, event="target_verified")


def run_local_upload_transaction(
    client: ExactLocalUploadClient,
    *,
    transaction_root: Path,
    spec: LocalUploadSpec,
    reconciliation_delays: Sequence[float] = (0.0, 0.25, 0.75, 1.5, 3.0),
    checkpoint_hook: CheckpointHook | None = None,
) -> LocalUploadResult:
    """Upload exactly once and reconcile every later entry by full hash only."""
    if not reconciliation_delays:
        raise ValueError("reconciliation_delays must not be empty")
    if any(delay < 0 for delay in reconciliation_delays):
        raise ValueError("reconciliation delays must be non-negative")
    directory = transaction_root / spec.transaction_id
    journal_path = directory / "journal.json"
    lock_path = directory / "transaction.lock"
    with _transaction_lock(lock_path):
        source_sha256 = _hash_file(spec.source_path, expected_size=spec.expected_size)
        if spec.expected_sha256 is not None and source_sha256 != spec.expected_sha256:
            raise LocalUploadConflict("local source sha256 differs from expected content")
        raw = _load_journal(journal_path)
        if raw is None:
            journal: dict[str, Any] = {
                "schema_version": 1,
                "kind": "durable_local_upload",
                "transaction_id": spec.transaction_id,
                "source_path": str(spec.source_path),
                "target_path": spec.target_path,
                "content_type": spec.content_type,
                "size": spec.expected_size,
                "sha256": source_sha256,
                "upload_started": False,
                "upload_calls": 0,
                "history": [],
            }
            _checkpoint(journal_path, journal, "prepared", checkpoint_hook)
        else:
            journal = _validate_journal(raw, spec, source_sha256=source_sha256)

        if journal["state"] == "complete":
            visible, error = _reconcile_target(
                client,
                spec,
                sha256=source_sha256,
                delays=reconciliation_delays,
            )
            if not visible:
                raise LocalUploadUncertain(
                    "completed receipt exists but the exact target can no longer be "
                    f"proved; retain local source: {error or spec.target_path}"
                )
            return LocalUploadResult(
                spec.transaction_id, "complete", spec.expected_size, source_sha256,
                journal_path, str(journal["receipt_sha256"]),
                int(journal["upload_calls"]),
            )

        if journal["state"] == "prepared":
            visible, error = _reconcile_target(
                client, spec, sha256=source_sha256, delays=(0.0,),
            )
            if visible:
                _complete(journal_path, journal, checkpoint_hook)
            else:
                if error is not None:
                    raise LocalUploadUncertain(
                        "cannot prove the exact target is absent before upload: "
                        f"{error}"
                    )
                journal["upload_started"] = True
                journal["upload_calls"] = 1
                _checkpoint(
                    journal_path, journal, "upload_started", checkpoint_hook,
                    event="upload_started", reconciliation_error=error,
                )
                try:
                    client.upload_file_once(
                        spec.target_path, spec.source_path, spec.content_type,
                    )
                except Exception as exc:
                    _checkpoint(
                        journal_path, journal, "upload_uncertain", checkpoint_hook,
                        upload_error=f"{type(exc).__name__}: {exc}",
                    )

        if journal["state"] in {"upload_started", "upload_uncertain"}:
            visible, error = _reconcile_target(
                client,
                spec,
                sha256=source_sha256,
                delays=reconciliation_delays,
            )
            if not visible:
                if journal["state"] != "upload_uncertain" or error is not None:
                    _checkpoint(
                        journal_path, journal, "upload_uncertain", checkpoint_hook,
                        reconciliation_error=error or "target_not_visible",
                    )
                raise LocalUploadUncertain(
                    "upload outcome remains uncertain; exact target was not proven; "
                    f"retain source and journal: {journal_path}"
                )
            _complete(journal_path, journal, checkpoint_hook)

        return LocalUploadResult(
            spec.transaction_id, "complete", spec.expected_size, source_sha256,
            journal_path, str(journal["receipt_sha256"]),
            int(journal["upload_calls"]),
        )


__all__ = [
    "ExactLocalUploadClient",
    "LocalUploadConflict",
    "LocalUploadCorrupt",
    "LocalUploadRemoteInfo",
    "LocalUploadResult",
    "LocalUploadSpec",
    "LocalUploadTransactionError",
    "LocalUploadUncertain",
    "deterministic_local_upload_id",
    "run_local_upload_transaction",
]
