"""Hybrid remote rollback transactions for irreplaceable media files.

The rollback payload lives on the remote AList/Quark storage while the host
only holds one file at a time for hashing and transport.  Remote writes are
never trusted from their response alone: every accepted object is read back
in full and compared by SHA-256.  All paths are exact and uploads are
single-attempt/no-overwrite operations.

This module contains no AList implementation and cannot launch Quark.  The
caller supplies an exact-path client, which also makes the state machine
testable without touching a real account.
"""

from __future__ import annotations

import contextlib
import hashlib
import json
import os
import re
import secrets
import unicodedata
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, BinaryIO, Callable, ContextManager, Mapping, Protocol

from .remote_file_transaction import (
    RemoteFileInfo,
    RemoteFileTransactionError,
    TransactionConflict,
    TransactionCorrupt,
    TransactionUncertain,
)
from .serialization import atomic_write_json


DEFAULT_ROLLBACK_ROOT = "/quark/影视/ScrapeFlow/事务回滚"
REMOTE_ROLLBACK_ROOT_ENV = "SCRAPEFLOW_REMOTE_ROLLBACK_ROOT"
_ID_RE = re.compile(r"\A[A-Za-z0-9][A-Za-z0-9._-]{0,127}\Z")
_SHA_RE = re.compile(r"\A[0-9a-f]{64}\Z")
_CHUNK = 1024 * 1024
_ALLOWED_COMPAT_PUNCTUATION = frozenset("：，。！？；（）【】［］《》「」『』、·…—～")
_STATES = {
    "new", "rollback_upload_intent", "rollback_verified", "target_upload_intent",
    "target_verified", "source_delete_intent", "complete", "restoring",
    "restored", "committing", "committed",
    "aborted_unmodified", "aborting", "aborted",
}


def _validate_remote_path(path: str, *, label: str) -> None:
    if not isinstance(path, str) or not path.startswith("/") or path == "/":
        raise ValueError(f"{label} must be a non-root absolute path")
    if path.endswith("/") or "\\" in path or "%" in path:
        raise ValueError(f"{label} contains an unsafe path alias")
    # Formal media names legitimately contain compatibility punctuation such
    # as the full-width Chinese colon.  Preserve that exact provider path, but
    # compare every protected path through NFKC below so a visually-equivalent
    # alias can never enter the same transaction batch.
    if unicodedata.normalize("NFC", path) != path:
        raise ValueError(f"{label} must use NFC Unicode")
    for character in path:
        compatibility = unicodedata.normalize("NFKC", character)
        if compatibility != character and character not in _ALLOWED_COMPAT_PUNCTUATION:
            raise ValueError(f"{label} contains an unsafe compatibility alias")
    segments = path.split("/")[1:]
    if any(
        segment in {"", ".", ".."}
        or any(unicodedata.category(character).startswith("C") for character in segment)
        for segment in segments
    ):
        raise ValueError(f"{label} is not a canonical remote path")


def _paths_overlap(left: str, right: str) -> bool:
    left_alias = unicodedata.normalize("NFKC", left).casefold()
    right_alias = unicodedata.normalize("NFKC", right).casefold()
    return (
        left_alias == right_alias
        or left_alias.startswith(right_alias + "/")
        or right_alias.startswith(left_alias + "/")
    )


def _path_is_within(path: str, root: str) -> bool:
    path_alias = unicodedata.normalize("NFKC", path).casefold()
    root_alias = unicodedata.normalize("NFKC", root).casefold()
    return path_alias == root_alias or path_alias.startswith(root_alias + "/")


def _assert_disjoint_paths(paths: list[tuple[str, str]]) -> None:
    for index, (left_label, left) in enumerate(paths):
        for right_label, right in paths[index + 1:]:
            if _paths_overlap(left, right):
                raise ValueError(
                    f"remote transaction paths overlap: {left_label}={left!r}, "
                    f"{right_label}={right!r}"
                )


class HybridRemoteClient(Protocol):
    """Exact-path transport with single-attempt, fail-if-exists uploads.

    ``upload_file_once`` must not retry and must never select an alternate
    name such as ``(1)``.  A race that creates the exact target is reconciled
    by full read-back; an implementation must not overwrite it.
    """

    def stat_exact(self, path: str) -> RemoteFileInfo | None: ...
    def open_reader(self, path: str) -> ContextManager[BinaryIO]: ...
    def upload_file_once(self, target_path: str, source: Path, content_type: str) -> None: ...
    def remove_file(self, path: str) -> None: ...
    def ensure_directory(self, path: str) -> None: ...


@dataclass(frozen=True, slots=True)
class HybridTransferSpec:
    batch_id: str
    item_id: str
    source_path: str
    target_path: str | None
    expected_size: int
    expected_sha256: str | None = None
    content_type: str = "application/octet-stream"
    rollback_root: str = DEFAULT_ROLLBACK_ROOT
    operation: str = "transfer"

    def __post_init__(self) -> None:
        if not _ID_RE.fullmatch(self.batch_id) or not _ID_RE.fullmatch(self.item_id):
            raise ValueError("batch_id and item_id must be safe stable identifiers")
        if self.operation not in {"transfer", "delete"}:
            raise ValueError("operation must be 'transfer' or 'delete'")
        if self.operation == "transfer" and self.target_path is None:
            raise ValueError("transfer operation requires target_path")
        if self.operation == "delete" and self.target_path is not None:
            raise ValueError("delete operation must not have target_path")
        path_values = [("source_path", self.source_path), ("rollback_root", self.rollback_root)]
        if self.target_path is not None:
            path_values.append(("target_path", self.target_path))
        for label, value in path_values:
            _validate_remote_path(value, label=label)
        if self.target_path is not None and self.source_path == self.target_path:
            raise ValueError("source_path and target_path must differ")
        if _path_is_within(self.source_path, self.rollback_root) or (
            self.target_path is not None
            and _path_is_within(self.target_path, self.rollback_root)
        ):
            raise ValueError("source_path/target_path must be outside rollback_root")
        if isinstance(self.expected_size, bool) or self.expected_size < 0:
            raise ValueError("expected_size must be a non-negative integer")
        if self.expected_sha256 is not None and not _SHA_RE.fullmatch(self.expected_sha256):
            raise ValueError("expected_sha256 must be lowercase hexadecimal")
        protected = [
            ("source_path", self.source_path),
            ("rollback_path", self.rollback_path),
            ("remote_manifest_path", self.remote_manifest_path),
            ("batch_manifest_path", f"{self.batch_root}/batch-manifest.json"),
        ]
        if self.target_path is not None:
            protected.append(("target_path", self.target_path))
        _assert_disjoint_paths(protected)

    @property
    def batch_root(self) -> str:
        return f"{self.rollback_root.rstrip('/')}/{self.batch_id}"

    @property
    def item_root(self) -> str:
        return f"{self.batch_root}/items/{self.item_id}"

    @property
    def rollback_path(self) -> str:
        return f"{self.item_root}/payload.bin"

    @property
    def remote_manifest_path(self) -> str:
        return f"{self.item_root}/manifest.json"

    def to_dict(self) -> dict[str, Any]:
        """Return the complete, versioned durable representation."""
        return {
            "schema_version": 1,
            "batch_id": self.batch_id,
            "item_id": self.item_id,
            "operation": self.operation,
            "source_path": self.source_path,
            "target_path": self.target_path,
            "expected_size": self.expected_size,
            "expected_sha256": self.expected_sha256,
            "content_type": self.content_type,
            "rollback_root": self.rollback_root,
        }

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> "HybridTransferSpec":
        """Strictly reconstruct a spec; missing/unknown fields fail closed."""
        expected_keys = {
            "schema_version", "batch_id", "item_id", "operation", "source_path",
            "target_path", "expected_size", "expected_sha256", "content_type",
            "rollback_root",
        }
        if not isinstance(raw, Mapping) or set(raw) != expected_keys:
            raise ValueError("hybrid transfer spec fields do not match schema version 1")
        if raw.get("schema_version") != 1:
            raise ValueError("unsupported hybrid transfer spec schema")
        string_fields = (
            "batch_id", "item_id", "operation", "source_path", "content_type",
            "rollback_root",
        )
        if any(not isinstance(raw.get(key), str) for key in string_fields):
            raise ValueError("hybrid transfer spec contains a non-string identity field")
        if raw.get("target_path") is not None and not isinstance(raw.get("target_path"), str):
            raise ValueError("hybrid transfer target_path must be a string or null")
        if raw.get("expected_sha256") is not None and not isinstance(raw.get("expected_sha256"), str):
            raise ValueError("hybrid transfer expected_sha256 must be a string or null")
        size = raw.get("expected_size")
        if isinstance(size, bool) or not isinstance(size, int):
            raise ValueError("hybrid transfer expected_size must be an integer")
        return cls(
            batch_id=str(raw["batch_id"]), item_id=str(raw["item_id"]),
            operation=str(raw["operation"]), source_path=str(raw["source_path"]),
            target_path=raw["target_path"], expected_size=size,
            expected_sha256=raw["expected_sha256"],
            content_type=str(raw["content_type"]),
            rollback_root=str(raw["rollback_root"]),
        )


@dataclass(frozen=True, slots=True)
class HybridTransferResult:
    batch_id: str
    item_id: str
    state: str
    size: int
    sha256: str
    rollback_path: str
    local_payload_retained: bool
    journal_path: Path


CheckpointHook = Callable[[str, Mapping[str, Any]], None]


def _now() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def _paths(state_root: Path, spec: HybridTransferSpec) -> tuple[Path, Path, Path, Path]:
    root = state_root / spec.batch_id / spec.item_id
    return root, root / "payload.bin", root / "journal.json", root / "lock"


@contextlib.contextmanager
def _lock(path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor = os.open(path, os.O_CREAT | os.O_RDWR, 0o600)
    try:
        try:
            import fcntl
        except ImportError:  # pragma: no cover
            fcntl = None  # type: ignore[assignment]
        if fcntl is not None:
            fcntl.flock(descriptor, fcntl.LOCK_EX)
        yield
    finally:
        if "fcntl" in locals() and fcntl is not None:
            fcntl.flock(descriptor, fcntl.LOCK_UN)
        os.close(descriptor)


def _identity(spec: HybridTransferSpec) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "batch_id": spec.batch_id,
        "item_id": spec.item_id,
        "source_path": spec.source_path,
        "target_path": spec.target_path,
        "operation": spec.operation,
        "rollback_path": spec.rollback_path,
        "remote_manifest_path": spec.remote_manifest_path,
        "content_type": spec.content_type,
        "expected_size": spec.expected_size,
        "expected_sha256": spec.expected_sha256,
    }


def _load(path: Path, spec: HybridTransferSpec) -> dict[str, Any] | None:
    try:
        raw = json.loads(path.read_text("utf-8"))
    except FileNotFoundError:
        return None
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise TransactionCorrupt(f"cannot read hybrid transaction journal: {path}") from exc
    if not isinstance(raw, dict):
        raise TransactionCorrupt("hybrid transaction journal is not an object")
    for key, value in _identity(spec).items():
        if raw.get(key) != value:
            raise TransactionConflict(f"hybrid transaction identity mismatch: {key}")
    if raw.get("state") not in _STATES:
        raise TransactionCorrupt("invalid hybrid transaction state")
    if not isinstance(raw.get("history"), list):
        raise TransactionCorrupt("invalid hybrid transaction history")
    digest = raw.get("sha256")
    size = raw.get("size")
    if digest is not None and (not isinstance(digest, str) or not _SHA_RE.fullmatch(digest)):
        raise TransactionCorrupt("invalid hybrid transaction digest")
    if size is not None and (isinstance(size, bool) or not isinstance(size, int) or size < 0):
        raise TransactionCorrupt("invalid hybrid transaction size")
    return raw


def _save(path: Path, journal: dict[str, Any], state: str, hook: CheckpointHook | None,
          *, event: str | None = None, **updates: Any) -> None:
    if state not in _STATES:
        raise AssertionError(state)
    journal.update(updates)
    journal["state"] = state
    journal["updated_at"] = _now()
    journal.setdefault("history", []).append({"at": journal["updated_at"], "event": event or state})
    atomic_write_json(path, journal, allow_nan=False, sort_keys=True)
    if hook is not None:
        hook(event or state, dict(journal))


def _remote_hash(client: HybridRemoteClient, path: str, expected_size: int) -> str:
    before = client.stat_exact(path)
    if before is None:
        raise TransactionUncertain(f"exact remote object is not visible: {path}")
    if before.size != expected_size:
        raise TransactionConflict(f"remote object has wrong size: {path}")
    digest, size = hashlib.sha256(), 0
    with client.open_reader(path) as reader:
        while chunk := reader.read(_CHUNK):
            size += len(chunk)
            if size > expected_size:
                raise TransactionConflict(f"remote object grew while reading: {path}")
            digest.update(chunk)
    if size != expected_size:
        raise TransactionUncertain(f"short remote read: {path}")
    after = client.stat_exact(path)
    if after is None:
        raise TransactionUncertain(f"remote object vanished after read-back: {path}")
    if after.size != before.size or (
        before.version is not None and after.version is not None and before.version != after.version
    ):
        raise TransactionConflict(f"remote object changed during read-back: {path}")
    actual = digest.hexdigest()
    for info in (before, after):
        if info.sha256 is not None and info.sha256 != actual:
            raise TransactionConflict(f"provider digest disagrees with read-back: {path}")
    return actual


def _verify(client: HybridRemoteClient, path: str, size: int, sha256: str) -> bool:
    if client.stat_exact(path) is None:
        return False
    if _remote_hash(client, path, size) != sha256:
        raise TransactionConflict(f"exact remote path contains different content: {path}")
    return True


def _read_verified_bytes(
    client: HybridRemoteClient,
    path: str,
    *,
    expected_size: int,
    expected_sha256: str,
) -> bytes:
    """Return the exact bytes whose size/version/hash were verified."""
    before = client.stat_exact(path)
    if before is None:
        raise TransactionUncertain(f"exact remote object is not visible: {path}")
    if before.size != expected_size:
        raise TransactionConflict(f"remote object has wrong size: {path}")
    chunks: list[bytes] = []
    digest, size = hashlib.sha256(), 0
    with client.open_reader(path) as reader:
        while chunk := reader.read(_CHUNK):
            size += len(chunk)
            if size > expected_size:
                raise TransactionConflict(f"remote object grew while reading: {path}")
            digest.update(chunk)
            chunks.append(chunk)
    if size != expected_size:
        raise TransactionUncertain(f"short remote read: {path}")
    after = client.stat_exact(path)
    if after is None:
        raise TransactionUncertain(f"remote object vanished after read-back: {path}")
    if after.size != before.size or (
        before.version is not None and after.version is not None
        and before.version != after.version
    ):
        raise TransactionConflict(f"remote object changed during read-back: {path}")
    actual = digest.hexdigest()
    if actual != expected_sha256:
        raise TransactionConflict(f"remote object digest differs from receipt: {path}")
    for info in (before, after):
        if info.sha256 is not None and info.sha256 != actual:
            raise TransactionConflict(f"provider digest disagrees with read-back: {path}")
    return b"".join(chunks)


def _stage_remote(client: HybridRemoteClient, source: str, payload: Path,
                  expected_size: int, expected_sha256: str | None) -> tuple[int, str]:
    info = client.stat_exact(source)
    if info is None:
        raise TransactionUncertain(f"source is not visible: {source}")
    if info.size != expected_size:
        raise TransactionConflict(f"source size differs from plan: {source}")
    partial = payload.with_suffix(".part")
    payload.parent.mkdir(parents=True, exist_ok=True)
    partial.unlink(missing_ok=True)
    digest, size = hashlib.sha256(), 0
    try:
        with partial.open("xb") as output, client.open_reader(source) as reader:
            while chunk := reader.read(_CHUNK):
                size += len(chunk)
                if size > expected_size:
                    raise TransactionConflict("source grew while staging")
                digest.update(chunk)
                output.write(chunk)
            if size != expected_size:
                raise TransactionUncertain("source read ended early")
            output.flush()
            os.fsync(output.fileno())
        actual = digest.hexdigest()
        if expected_sha256 is not None and actual != expected_sha256:
            raise TransactionConflict("source digest differs from plan")
        os.replace(partial, payload)
        return size, actual
    finally:
        partial.unlink(missing_ok=True)


def _upload_reconcile(client: HybridRemoteClient, *, path: str, payload: Path,
                      content_type: str, size: int, sha256: str) -> None:
    if client.stat_exact(path) is not None:
        if not _verify(client, path, size, sha256):  # pragma: no cover
            raise AssertionError
        return
    try:
        client.upload_file_once(path, payload, content_type)
    except Exception as exc:
        try:
            visible = _verify(client, path, size, sha256)
        except TransactionConflict:
            raise
        except Exception as verify_exc:
            raise TransactionUncertain(
                f"upload response and read-back are uncertain for {path}: {verify_exc}"
            ) from exc
        if not visible:
            raise TransactionUncertain(f"upload may have started but target is absent: {path}") from exc
    if not _verify(client, path, size, sha256):
        raise TransactionUncertain(f"uploaded object is not visible: {path}")


def _durable_upload(
    client: HybridRemoteClient,
    *,
    path: str,
    payload: Path,
    content_type: str,
    size: int,
    sha256: str,
    attempted_key: str,
    state: str,
    journal_path: Path,
    journal: dict[str, Any],
    hook: CheckpointHook | None,
) -> None:
    """Issue at most one upload request across crashes and restarts."""
    if client.stat_exact(path) is not None:
        _verify(client, path, size, sha256)
        return
    if journal.get(attempted_key):
        raise TransactionUncertain(
            f"an upload may already have started and exact target is absent: {path}"
        )
    _save(
        journal_path,
        journal,
        state,
        hook,
        event=f"{attempted_key}_checkpoint",
        **{attempted_key: True},
    )
    _upload_reconcile(
        client,
        path=path,
        payload=payload,
        content_type=content_type,
        size=size,
        sha256=sha256,
    )


def _manifest_bytes(
    spec: HybridTransferSpec,
    size: int,
    sha256: str,
    *,
    created_at: str,
) -> bytes:
    return (json.dumps({
        **_identity(spec), "size": size, "sha256": sha256,
        "retention": "until_batch_commit", "created_at": created_at,
    }, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n").encode()


def _publish_manifest(client: HybridRemoteClient, spec: HybridTransferSpec,
                      local_root: Path, size: int, sha256: str, *,
                      journal_path: Path, journal: dict[str, Any],
                      hook: CheckpointHook | None) -> None:
    manifest = _manifest_bytes(
        spec, size, sha256, created_at=str(journal["created_at"]),
    )
    manifest_sha256 = hashlib.sha256(manifest).hexdigest()
    manifest_path = local_root / "remote-manifest.json"
    manifest_path.write_bytes(manifest)
    try:
        _durable_upload(
            client, path=spec.remote_manifest_path, payload=manifest_path,
            content_type="application/json", size=len(manifest),
            sha256=manifest_sha256,
            attempted_key="manifest_upload_started",
            state="rollback_upload_intent",
            journal_path=journal_path,
            journal=journal,
            hook=hook,
        )
        _save(
            journal_path, journal, "rollback_upload_intent", hook,
            event="remote_manifest_verified", remote_manifest_size=len(manifest),
            remote_manifest_sha256=manifest_sha256,
        )
    finally:
        manifest_path.unlink(missing_ok=True)


def _result(spec: HybridTransferSpec, payload: Path, journal_path: Path,
            journal: Mapping[str, Any]) -> HybridTransferResult:
    return HybridTransferResult(
        spec.batch_id, spec.item_id, str(journal["state"]), int(journal["size"]),
        str(journal["sha256"]), spec.rollback_path, payload.is_file(), journal_path,
    )


def _verify_item_manifest(
    client: HybridRemoteClient,
    spec: HybridTransferSpec,
    journal: Mapping[str, Any],
) -> None:
    size = journal.get("remote_manifest_size")
    digest = journal.get("remote_manifest_sha256")
    if (
        isinstance(size, bool) or not isinstance(size, int) or size <= 0
        or not isinstance(digest, str) or not _SHA_RE.fullmatch(digest)
    ):
        raise TransactionCorrupt("item manifest verification receipt is missing")
    try:
        manifest = json.loads(_read_verified_bytes(
            client, spec.remote_manifest_path, expected_size=size,
            expected_sha256=digest,
        ).decode("utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise TransactionCorrupt("item manifest is not valid JSON") from exc
    expected = {
        **_identity(spec), "size": int(journal["size"]),
        "sha256": str(journal["sha256"]), "retention": "until_batch_commit",
        "created_at": str(journal["created_at"]),
    }
    if manifest != expected:
        raise TransactionConflict("item manifest content differs from transaction receipt")


def _validate_terminal_semantics(
    client: HybridRemoteClient,
    spec: HybridTransferSpec,
    journal: Mapping[str, Any],
) -> None:
    size, sha256 = int(journal["size"]), str(journal["sha256"])
    if not _verify(client, spec.rollback_path, size, sha256):
        raise TransactionUncertain("rollback payload is missing before commit")
    _verify_item_manifest(client, spec, journal)
    state = journal.get("state")
    if state == "complete":
        if not journal.get("forward_started"):
            raise TransactionCorrupt("complete transaction has no forward receipt")
        if client.stat_exact(spec.source_path) is not None:
            raise TransactionConflict("complete transaction source unexpectedly exists")
        if spec.operation == "transfer":
            if not journal.get("target_owned") or spec.target_path is None:
                raise TransactionCorrupt("complete transfer has no target ownership receipt")
            if not _verify(client, spec.target_path, size, sha256):
                raise TransactionUncertain("complete transaction target disappeared")
    elif state == "restored":
        if not _verify(client, spec.source_path, size, sha256):
            raise TransactionUncertain("restored transaction source disappeared")
        if spec.target_path is not None and client.stat_exact(spec.target_path) is not None:
            raise TransactionConflict("restored transaction target unexpectedly exists")
    else:
        raise TransactionUncertain("transaction has no committable terminal semantics")


def prepare_hybrid_transfer(
    client: HybridRemoteClient,
    *,
    state_root: Path,
    spec: HybridTransferSpec,
    checkpoint_hook: CheckpointHook | None = None,
) -> HybridTransferResult:
    """Create and prove one remote rollback copy without forward mutation."""
    root, payload, journal_path, lock = _paths(state_root, spec)
    with _lock(lock):
        journal = _load(journal_path, spec)
        if journal is None:
            size, sha256 = _stage_remote(
                client, spec.source_path, payload, spec.expected_size,
                spec.expected_sha256,
            )
            journal = {
                **_identity(spec), "size": size, "sha256": sha256,
                "created_at": _now(), "rollback_upload_started": False,
                "manifest_upload_started": False, "target_upload_started": False,
                "restore_upload_started": False, "forward_started": False,
                "target_owned": False, "history": [],
            }
            _save(journal_path, journal, "new", checkpoint_hook)
        size, sha256 = int(journal["size"]), str(journal["sha256"])
        state = str(journal["state"])
        if state == "new":
            if not payload.is_file():
                raise TransactionCorrupt("local stage missing before rollback upload")
            _save(journal_path, journal, "rollback_upload_intent", checkpoint_hook)
            state = "rollback_upload_intent"
        if state == "rollback_upload_intent":
            if not payload.is_file():
                if not _verify(client, spec.rollback_path, size, sha256):
                    raise TransactionUncertain(
                        "rollback upload intent has neither local nor remote payload"
                    )
            else:
                client.ensure_directory(spec.item_root)
                _durable_upload(
                    client, path=spec.rollback_path, payload=payload,
                    content_type=spec.content_type, size=size, sha256=sha256,
                    attempted_key="rollback_upload_started",
                    state="rollback_upload_intent", journal_path=journal_path,
                    journal=journal, hook=checkpoint_hook,
                )
            if not _verify(client, spec.rollback_path, size, sha256):
                raise TransactionUncertain("rollback copy cannot be proven")
            _publish_manifest(
                client, spec, root, size, sha256, journal_path=journal_path,
                journal=journal, hook=checkpoint_hook,
            )
            _save(journal_path, journal, "rollback_verified", checkpoint_hook)
            payload.unlink(missing_ok=True)
            state = "rollback_verified"
        if state not in {
            "rollback_verified", "target_upload_intent", "target_verified",
            "source_delete_intent", "complete", "restoring", "restored",
            "committing", "committed",
        }:
            raise TransactionCorrupt(f"cannot prepare from state: {state}")
        if state not in {"committing", "committed"} and not _verify(
            client, spec.rollback_path, size, sha256
        ):
            raise TransactionUncertain("prepared rollback copy is no longer intact")
        payload.unlink(missing_ok=True)
        return _result(spec, payload, journal_path, journal)


def _batch_paths(state_root: Path, spec: HybridTransferSpec) -> tuple[Path, Path, Path]:
    root = state_root / spec.batch_id
    return root / "batch.json", root / "batch-manifest.json", root / "batch.lock"


def _batch_manifest_path(spec: HybridTransferSpec) -> str:
    return f"{spec.batch_root}/batch-manifest.json"


def _desired_specs_payload(specs: list[HybridTransferSpec]) -> bytes:
    return (json.dumps(
        [item.to_dict() for item in sorted(specs, key=lambda value: value.item_id)],
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ) + "\n").encode("utf-8")


def _validated_durable_specs(
    batch: Mapping[str, Any],
    *,
    batch_id: str,
) -> list[HybridTransferSpec]:
    raw_specs = batch.get("specs")
    if not isinstance(raw_specs, list) or not raw_specs:
        raise TransactionCorrupt("hybrid batch has no durable desired specs")
    try:
        specs = [HybridTransferSpec.from_dict(item) for item in raw_specs]
    except (TypeError, ValueError) as exc:
        raise TransactionCorrupt("hybrid batch contains an invalid durable spec") from exc
    ordered = sorted(specs, key=lambda value: value.item_id)
    canonical = [item.to_dict() for item in ordered]
    if raw_specs != canonical:
        raise TransactionConflict("durable batch specs are not canonical and sorted")
    item_ids = [item.item_id for item in ordered]
    if (
        len(set(item_ids)) != len(item_ids)
        or any(item.batch_id != batch_id for item in ordered)
        or batch.get("item_ids") != item_ids
    ):
        raise TransactionConflict("durable batch spec membership differs")
    _validate_batch_path_isolation(ordered)
    actual = hashlib.sha256(_desired_specs_payload(ordered)).hexdigest()
    recorded = batch.get("specs_sha256")
    if not isinstance(recorded, str) or not _SHA_RE.fullmatch(recorded):
        raise TransactionCorrupt("durable desired specs digest is invalid")
    if recorded != actual:
        raise TransactionConflict("durable desired specs digest differs from specs")
    return ordered


def _assert_requested_specs(
    durable: list[HybridTransferSpec],
    requested: list[HybridTransferSpec],
) -> None:
    if [item.to_dict() for item in durable] != [
        item.to_dict() for item in sorted(requested, key=lambda value: value.item_id)
    ]:
        raise TransactionConflict("requested specs differ from durable desired batch")


def _batch_manifest_payload(specs: list[HybridTransferSpec], results: list[HybridTransferResult],
                            created_at: str) -> bytes:
    by_id = {result.item_id: result for result in results}
    items = []
    for spec in sorted(specs, key=lambda item: item.item_id):
        result = by_id[spec.item_id]
        items.append({
            "item_id": spec.item_id, "source_path": spec.source_path,
            "target_path": spec.target_path, "rollback_path": spec.rollback_path,
            "operation": spec.operation,
            "content_type": spec.content_type,
            "expected_size": spec.expected_size,
            "expected_sha256": spec.expected_sha256,
            "size": result.size, "sha256": result.sha256,
        })
    return (json.dumps({
        "schema_version": 1, "batch_id": specs[0].batch_id,
        "rollback_root": specs[0].rollback_root, "created_at": created_at,
        "retention": "until_explicit_batch_commit", "items": items,
    }, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n").encode()


def _batch_members(
    specs: list[HybridTransferSpec],
    results: list[HybridTransferResult],
) -> list[dict[str, Any]]:
    by_id = {result.item_id: result for result in results}
    return [
        {
            "item_id": item.item_id,
            "source_path": item.source_path,
            "target_path": item.target_path,
            "rollback_path": item.rollback_path,
            "operation": item.operation,
            "content_type": item.content_type,
            "expected_size": item.expected_size,
            "expected_sha256": item.expected_sha256,
            "size": by_id[item.item_id].size,
            "sha256": by_id[item.item_id].sha256,
        }
        for item in sorted(specs, key=lambda value: value.item_id)
    ]


def _validate_batch_path_isolation(specs: list[HybridTransferSpec]) -> None:
    protected: list[tuple[str, str]] = []
    for item in specs:
        protected.extend([
            (f"{item.item_id}.source_path", item.source_path),
            (f"{item.item_id}.rollback_path", item.rollback_path),
            (f"{item.item_id}.remote_manifest_path", item.remote_manifest_path),
        ])
        if item.target_path is not None:
            protected.append((f"{item.item_id}.target_path", item.target_path))
    protected.append(("batch_manifest_path", _batch_manifest_path(specs[0])))
    _assert_disjoint_paths(protected)


def prepare_hybrid_batch(
    client: HybridRemoteClient,
    *,
    state_root: Path,
    specs: list[HybridTransferSpec],
    checkpoint_hook: CheckpointHook | None = None,
) -> list[HybridTransferResult]:
    """Prepare every item, then atomically seal one immutable batch manifest."""
    if not specs:
        raise ValueError("hybrid batch must contain at least one item")
    first = specs[0]
    if any(
        item.batch_id != first.batch_id or item.rollback_root != first.rollback_root
        for item in specs
    ) or len({item.item_id for item in specs}) != len(specs):
        raise ValueError("batch specs must share identity/root and have unique item ids")
    _validate_batch_path_isolation(specs)
    journal_path, manifest_local, lock = _batch_paths(state_root, first)
    desired_specs = [
        item.to_dict() for item in sorted(specs, key=lambda value: value.item_id)
    ]
    desired_digest = hashlib.sha256(_desired_specs_payload(specs)).hexdigest()
    with _lock(lock):
        try:
            batch = json.loads(journal_path.read_text("utf-8"))
        except FileNotFoundError:
            batch = {
                "schema_version": 1,
                "batch_id": first.batch_id,
                "rollback_root": first.rollback_root,
                "created_at": _now(),
                "upload_started": False,
                "state": "preparing",
                "item_ids": sorted(item.item_id for item in specs),
                "specs": desired_specs,
                "specs_sha256": desired_digest,
            }
            # This immutable desired set is durable before even the first item
            # is staged/uploaded, preventing a changed retry from orphaning a
            # rollback object outside the caller's new batch membership.
            atomic_write_json(journal_path, batch, allow_nan=False, sort_keys=True)
            if checkpoint_hook is not None:
                checkpoint_hook("batch_desired_specs_persisted", dict(batch))
        except (OSError, UnicodeError, json.JSONDecodeError) as exc:
            raise TransactionCorrupt("cannot preflight existing hybrid batch") from exc
        if (
            not isinstance(batch, dict)
            or batch.get("batch_id") != first.batch_id
            or batch.get("rollback_root") != first.rollback_root
        ):
            raise TransactionConflict("hybrid batch journal identity mismatch")
        if batch.get("state") not in {
            "preparing", "sealed", "accepted", "committing", "committed",
        }:
            raise TransactionUncertain("hybrid batch is not eligible for preparation")
        if (
            batch.get("specs_sha256") != desired_digest
            or batch.get("specs") != desired_specs
            or batch.get("item_ids") != sorted(item.item_id for item in specs)
        ):
            raise TransactionConflict(
                "requested specs differ from durable desired batch"
            )
    results = [
        prepare_hybrid_transfer(
            client, state_root=state_root, spec=spec,
            checkpoint_hook=checkpoint_hook,
        )
        for spec in specs
    ]
    with _lock(lock):
        try:
            batch = json.loads(journal_path.read_text("utf-8"))
        except (FileNotFoundError, OSError, UnicodeError, json.JSONDecodeError) as exc:
            raise TransactionCorrupt("cannot read hybrid batch journal") from exc
        if batch.get("batch_id") != first.batch_id or batch.get("rollback_root") != first.rollback_root:
            raise TransactionConflict("hybrid batch journal identity mismatch")
        manifest = _batch_manifest_payload(specs, results, str(batch["created_at"]))
        digest = hashlib.sha256(manifest).hexdigest()
        previous = batch.get("manifest_sha256")
        if previous is not None and previous != digest:
            raise TransactionConflict("sealed batch item set differs from requested batch")
        manifest_local.write_bytes(manifest)
        remote_path = _batch_manifest_path(first)
        try:
            client.ensure_directory(first.batch_root)
            if client.stat_exact(remote_path) is None:
                if batch.get("upload_started"):
                    raise TransactionUncertain(
                        "batch manifest upload may have started but is absent"
                    )
                batch.update({"upload_started": True, "manifest_sha256": digest})
                atomic_write_json(journal_path, batch, allow_nan=False, sort_keys=True)
                if checkpoint_hook is not None:
                    checkpoint_hook("batch_manifest_upload_started", dict(batch))
                _upload_reconcile(
                    client, path=remote_path, payload=manifest_local,
                    content_type="application/json", size=len(manifest), sha256=digest,
                )
            if not _verify(client, remote_path, len(manifest), digest):
                raise TransactionUncertain("batch manifest is not visible")
            batch.update({
                "state": "sealed", "manifest_sha256": digest,
                "manifest_size": len(manifest), "item_ids": sorted(item.item_id for item in specs),
                "members": _batch_members(specs, results),
                "sealed_at": _now(),
            })
            atomic_write_json(journal_path, batch, allow_nan=False, sort_keys=True)
        finally:
            manifest_local.unlink(missing_ok=True)
    return results


def load_sealed_batch_specs(*, state_root: Path, batch_id: str) -> list[HybridTransferSpec]:
    """Rebuild exact specs for restart-time restore/commit coordination."""
    if not _ID_RE.fullmatch(batch_id):
        raise ValueError("invalid batch_id")
    journal_path = state_root / batch_id / "batch.json"
    try:
        raw = json.loads(journal_path.read_text("utf-8"))
    except (FileNotFoundError, OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise TransactionCorrupt("cannot read sealed hybrid batch specs") from exc
    if not isinstance(raw, dict) or raw.get("state") not in {
        "sealed", "accepted", "committing", "committed",
        "aborting", "aborted",
    } or raw.get("batch_id") != batch_id or not isinstance(raw.get("specs"), list):
        raise TransactionCorrupt("hybrid batch is not sealed or has no durable specs")
    return _validated_durable_specs(raw, batch_id=batch_id)


def _assert_batch_sealed(
    client: HybridRemoteClient,
    state_root: Path,
    spec: HybridTransferSpec,
    *,
    allowed_states: tuple[str, ...] = ("sealed",),
) -> None:
    journal_path, _manifest_local, _lock_path = _batch_paths(state_root, spec)
    try:
        batch = json.loads(journal_path.read_text("utf-8"))
    except (FileNotFoundError, OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise TransactionUncertain("hybrid batch has not been sealed") from exc
    durable_specs = _validated_durable_specs(batch, batch_id=spec.batch_id)
    matching_specs = [item for item in durable_specs if item.item_id == spec.item_id]
    if len(matching_specs) != 1 or matching_specs[0].to_dict() != spec.to_dict():
        raise TransactionConflict("current spec differs from durable desired batch")
    if batch.get("state") not in allowed_states or spec.item_id not in batch.get("item_ids", []):
        raise TransactionUncertain("item does not belong to a sealed hybrid batch")
    size, digest = batch.get("manifest_size"), batch.get("manifest_sha256")
    if isinstance(size, bool) or not isinstance(size, int) or not isinstance(digest, str):
        raise TransactionCorrupt("sealed batch receipt is invalid")
    try:
        manifest = json.loads(_read_verified_bytes(
            client, _batch_manifest_path(spec), expected_size=size,
            expected_sha256=digest,
        ).decode("utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise TransactionCorrupt("sealed remote batch manifest is not valid JSON") from exc
    members = batch.get("members")
    if (
        not isinstance(manifest, dict)
        or manifest.get("schema_version") != 1
        or manifest.get("batch_id") != spec.batch_id
        or manifest.get("rollback_root") != spec.rollback_root
        or not isinstance(members, list)
        or manifest.get("items") != members
        or sorted(item.get("item_id") for item in members if isinstance(item, dict))
        != batch.get("item_ids")
    ):
        raise TransactionConflict("remote batch manifest membership differs from sealed receipt")
    expected_member = {
        "item_id": spec.item_id,
        "source_path": spec.source_path,
        "target_path": spec.target_path,
        "rollback_path": spec.rollback_path,
        "operation": spec.operation,
        "content_type": spec.content_type,
        "expected_size": spec.expected_size,
        "expected_sha256": spec.expected_sha256,
        "size": spec.expected_size,
        "sha256": spec.expected_sha256,
    }
    matching = [item for item in members if isinstance(item, dict) and item.get("item_id") == spec.item_id]
    if len(matching) != 1:
        raise TransactionConflict("sealed batch does not contain exactly one current item")
    actual = matching[0]
    for key, expected in expected_member.items():
        if key == "sha256" and expected is None:
            if not isinstance(actual.get(key), str) or not _SHA_RE.fullmatch(actual[key]):
                raise TransactionConflict("sealed batch item has invalid resolved sha256")
            continue
        if actual.get(key) != expected:
            raise TransactionConflict(f"sealed batch item differs from current spec: {key}")


def run_hybrid_transfer(client: HybridRemoteClient, *, state_root: Path,
                        spec: HybridTransferSpec,
                        checkpoint_hook: CheckpointHook | None = None) -> HybridTransferResult:
    """Execute/resume one move while retaining a verified remote rollback copy."""
    root, payload, journal_path, lock = _paths(state_root, spec)
    with _lock(lock):
        journal = _load(journal_path, spec)
        if journal is None:
            raise TransactionUncertain(
                "batch must be fully prepared and sealed before forward mutation"
            )
        _assert_batch_sealed(client, state_root, spec)

        size, sha256 = int(journal["size"]), str(journal["sha256"])
        while True:
            state = str(journal["state"])
            if state in {"complete", "restored", "committed"}:
                return _result(spec, payload, journal_path, journal)
            if state in {"new", "rollback_upload_intent"}:
                raise TransactionUncertain("item was not fully prepared before batch seal")
            if state == "rollback_verified":
                if not _verify(client, spec.rollback_path, size, sha256):
                    raise TransactionUncertain("verified rollback copy disappeared")
                if spec.operation == "delete":
                    _save(
                        journal_path, journal, "source_delete_intent", checkpoint_hook,
                        event="delete_forward_started", forward_started=True,
                        target_owned=False,
                    )
                    continue
                if spec.target_path is None:  # pragma: no cover - dataclass invariant
                    raise TransactionCorrupt("transfer target is missing")
                if client.stat_exact(spec.target_path) is not None:
                    raise TransactionConflict(
                        "forward target already exists; transaction cannot prove ownership"
                    )
                if not payload.is_file():
                    _stage_remote(client, spec.rollback_path, payload, size, sha256)
                _save(
                    journal_path, journal, "target_upload_intent", checkpoint_hook,
                    forward_started=True, target_owned=False,
                )
                continue
            if state == "target_upload_intent":
                if spec.target_path is None:
                    raise TransactionCorrupt("delete transaction entered target upload state")
                if (
                    client.stat_exact(spec.target_path) is not None
                    and not journal.get("target_upload_started")
                ):
                    raise TransactionConflict(
                        "target appeared before this transaction started its upload"
                    )
                if not payload.is_file():
                    if _verify(client, spec.target_path, size, sha256):
                        _save(
                            journal_path, journal, "target_verified", checkpoint_hook,
                            target_owned=True,
                        )
                        continue
                    _stage_remote(client, spec.rollback_path, payload, size, sha256)
                _durable_upload(
                    client, path=spec.target_path, payload=payload,
                    content_type=spec.content_type, size=size, sha256=sha256,
                    attempted_key="target_upload_started",
                    state="target_upload_intent",
                    journal_path=journal_path,
                    journal=journal,
                    hook=checkpoint_hook,
                )
                _save(
                    journal_path, journal, "target_verified", checkpoint_hook,
                    target_owned=True,
                )
                continue
            if state == "target_verified":
                if spec.target_path is None:
                    raise TransactionCorrupt("delete transaction entered target verification state")
                if not _verify(client, spec.rollback_path, size, sha256) or not _verify(
                    client, spec.target_path, size, sha256
                ):
                    raise TransactionUncertain("target or rollback copy cannot be reverified")
                _save(journal_path, journal, "source_delete_intent", checkpoint_hook)
                continue
            if state == "source_delete_intent":
                if not _verify(client, spec.rollback_path, size, sha256):
                    raise TransactionUncertain("refusing source deletion without rollback copy")
                _verify_item_manifest(client, spec, journal)
                if spec.operation == "transfer":
                    if spec.target_path is None or not _verify(
                        client, spec.target_path, size, sha256
                    ):
                        raise TransactionUncertain(
                            "refusing source deletion without two verified copies"
                        )
                source = client.stat_exact(spec.source_path)
                if source is not None:
                    if not _verify(client, spec.source_path, size, sha256):  # conflict raised above
                        raise AssertionError
                    try:
                        client.remove_file(spec.source_path)
                    except Exception as exc:
                        if client.stat_exact(spec.source_path) is not None:
                            raise TransactionUncertain("source deletion uncertain") from exc
                if client.stat_exact(spec.source_path) is not None:
                    raise TransactionUncertain("source remains after deletion")
                payload.unlink(missing_ok=True)
                _save(journal_path, journal, "complete", checkpoint_hook)
                continue
            raise TransactionCorrupt(f"state {state!r} cannot run forward")


def restore_hybrid_transfer(client: HybridRemoteClient, *, state_root: Path,
                            spec: HybridTransferSpec,
                            checkpoint_hook: CheckpointHook | None = None) -> HybridTransferResult:
    """Restore the exact original source and remove only a matching target.

    The rollback copy is retained after restore.  Only an explicit commit may
    remove it.  Existing paths are never overwritten; different content is a
    hard conflict.
    """
    root, payload, journal_path, lock = _paths(state_root, spec)
    with _lock(lock):
        _assert_batch_sealed(client, state_root, spec)
        journal = _load(journal_path, spec)
        if journal is None or journal.get("sha256") is None:
            raise TransactionUncertain("no durable hybrid transaction to restore")
        if journal["state"] == "committed":
            raise TransactionUncertain("committed rollback payload cannot be restored")
        if not journal.get("forward_started") or (
            spec.operation == "transfer" and not journal.get("target_owned")
        ):
            raise TransactionUncertain(
                "restore requires an owned forward mutation"
            )
        if journal.get("state") not in {
            "target_verified", "source_delete_intent", "complete", "restoring", "restored",
        }:
            raise TransactionUncertain("transaction has not reached a restorable forward state")
        size, sha256 = int(journal["size"]), str(journal["sha256"])
        if not _verify(client, spec.rollback_path, size, sha256):
            raise TransactionUncertain("rollback payload is not intact")
        source = client.stat_exact(spec.source_path)
        if source is not None:
            _verify(client, spec.source_path, size, sha256)
        else:
            _save(journal_path, journal, "restoring", checkpoint_hook)
            if not payload.is_file():
                _stage_remote(client, spec.rollback_path, payload, size, sha256)
            _durable_upload(
                client, path=spec.source_path, payload=payload,
                content_type=spec.content_type, size=size, sha256=sha256,
                attempted_key="restore_upload_started",
                state="restoring",
                journal_path=journal_path,
                journal=journal,
                hook=checkpoint_hook,
            )
        if not _verify(client, spec.source_path, size, sha256):
            raise TransactionUncertain("restored source cannot be proven")
        if spec.target_path is not None:
            target = client.stat_exact(spec.target_path)
            if target is not None:
                _verify(client, spec.target_path, size, sha256)
                try:
                    client.remove_file(spec.target_path)
                except Exception as exc:
                    if client.stat_exact(spec.target_path) is not None:
                        raise TransactionUncertain("target cleanup after restore is uncertain") from exc
            if client.stat_exact(spec.target_path) is not None:
                raise TransactionUncertain("target remains after restore")
        payload.unlink(missing_ok=True)
        _save(journal_path, journal, "restored", checkpoint_hook)
        return _result(spec, payload, journal_path, journal)


def _cleanup_lease_path(spec: HybridTransferSpec) -> str:
    return f"{spec.batch_root}/cleanup-lease.json"


def _cleanup_lease_payload(
    batch_id: str,
    specs_sha256: str,
    token: str,
    purpose: str,
) -> bytes:
    return (json.dumps({
        "schema_version": 1,
        "batch_id": batch_id,
        "specs_sha256": specs_sha256,
        "owner_token": token,
        "purpose": purpose,
        "scope": "scrapeflow_internal_cleanup_exclusion_only",
        "external_writer_cas_guarantee": False,
    }, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")


def _read_stable_small_remote(
    client: HybridRemoteClient,
    path: str,
    *,
    maximum_size: int = 16 * 1024,
) -> bytes:
    before = client.stat_exact(path)
    if before is None:
        raise TransactionUncertain(f"remote object is not visible: {path}")
    if before.size <= 0 or before.size > maximum_size:
        raise TransactionConflict(f"remote control object has invalid size: {path}")
    chunks: list[bytes] = []
    size, digest = 0, hashlib.sha256()
    with client.open_reader(path) as reader:
        while chunk := reader.read(_CHUNK):
            size += len(chunk)
            if size > before.size or size > maximum_size:
                raise TransactionConflict(f"remote control object grew: {path}")
            digest.update(chunk)
            chunks.append(chunk)
    if size != before.size:
        raise TransactionUncertain(f"remote control object read ended early: {path}")
    after = client.stat_exact(path)
    if after is None:
        raise TransactionUncertain(f"remote control object vanished: {path}")
    if after.size != before.size or (
        before.version is not None
        and after.version is not None
        and before.version != after.version
    ):
        raise TransactionConflict(f"remote control object changed: {path}")
    actual = digest.hexdigest()
    for info in (before, after):
        if info.sha256 is not None and info.sha256 != actual:
            raise TransactionConflict(
                f"provider digest disagrees with control object: {path}"
            )
    return b"".join(chunks)


def _parse_cleanup_lease(
    payload: bytes,
    *,
    batch_id: str,
    specs_sha256: str,
    purpose: str,
) -> str:
    try:
        raw = json.loads(payload.decode("utf-8"))
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise TransactionConflict("cleanup lease is not valid JSON") from exc
    expected_keys = {
        "schema_version", "batch_id", "specs_sha256", "owner_token",
        "purpose", "scope", "external_writer_cas_guarantee",
    }
    if not isinstance(raw, dict) or set(raw) != expected_keys:
        raise TransactionConflict("cleanup lease schema differs")
    token = raw.get("owner_token")
    if (
        raw.get("schema_version") != 1
        or raw.get("batch_id") != batch_id
        or raw.get("specs_sha256") != specs_sha256
        or raw.get("purpose") != purpose
        or raw.get("scope") != "scrapeflow_internal_cleanup_exclusion_only"
        or raw.get("external_writer_cas_guarantee") is not False
        or not isinstance(token, str)
        or not _SHA_RE.fullmatch(token)
    ):
        raise TransactionConflict("cleanup lease identity differs")
    canonical = _cleanup_lease_payload(batch_id, specs_sha256, token, purpose)
    if payload != canonical:
        raise TransactionConflict("cleanup lease encoding is not canonical")
    return token


def _atomic_write_bytes(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    temporary.unlink(missing_ok=True)
    try:
        with temporary.open("xb") as output:
            output.write(payload)
            output.flush()
            os.fsync(output.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _ensure_cleanup_lease(
    client: HybridRemoteClient,
    *,
    state_root: Path,
    spec: HybridTransferSpec,
    batch: dict[str, Any],
    purpose: str,
    checkpoint_hook: CheckpointHook | None = None,
) -> tuple[str, int, str]:
    """Acquire/reconcile one create-only batch cleanup lease.

    This excludes independent ScrapeFlow cleanup owners that use different
    durable state roots. AList exposes no compare-and-swap primitive, so an
    arbitrary external writer remains an explicitly unclosed storage risk.
    """
    specs_sha256 = batch.get("specs_sha256")
    if not isinstance(specs_sha256, str) or not _SHA_RE.fullmatch(specs_sha256):
        raise TransactionCorrupt("durable desired specs digest is missing")
    lease_path = _cleanup_lease_path(spec)
    token = batch.get("cleanup_lease_token")
    recorded_purpose = batch.get("cleanup_lease_purpose")
    if token is None:
        remote = client.stat_exact(lease_path)
        local = state_root / spec.batch_id / "cleanup-lease.payload"
        if remote is not None:
            token = _parse_cleanup_lease(
                _read_stable_small_remote(client, lease_path),
                batch_id=spec.batch_id,
                specs_sha256=specs_sha256,
                purpose=purpose,
            )
        elif local.is_file():
            token = _parse_cleanup_lease(
                local.read_bytes(),
                batch_id=spec.batch_id,
                specs_sha256=specs_sha256,
                purpose=purpose,
            )
        else:
            token = secrets.token_hex(32)
        batch["cleanup_lease_token"] = token
        batch["cleanup_lease_purpose"] = purpose
    elif not isinstance(token, str) or not re.fullmatch(r"[0-9a-f]{64}", token):
        raise TransactionCorrupt("cleanup lease token is invalid")
    elif recorded_purpose != purpose:
        raise TransactionConflict("cleanup lease belongs to another terminal action")
    payload = _cleanup_lease_payload(spec.batch_id, specs_sha256, token, purpose)
    digest = hashlib.sha256(payload).hexdigest()
    local = state_root / spec.batch_id / "cleanup-lease.payload"
    _atomic_write_bytes(local, payload)
    client.ensure_directory(spec.batch_root)
    _upload_reconcile(
        client,
        path=lease_path,
        payload=local,
        content_type="application/json",
        size=len(payload),
        sha256=digest,
    )
    if not _verify(client, lease_path, len(payload), digest):
        raise TransactionUncertain("cleanup lease is not visible")
    if checkpoint_hook is not None:
        checkpoint_hook("cleanup_lease_remote_verified", dict(batch))
    # Persist the owner receipt inside the batch state before permitting any
    # destructive cleanup.  A crash at the checkpoint above is recovered by
    # strict adoption of the immutable remote lease.
    journal_path, _manifest, _lock_path = _batch_paths(state_root, spec)
    atomic_write_json(journal_path, batch, allow_nan=False, sort_keys=True)
    return token, len(payload), digest


def _verify_cleanup_lease(
    client: HybridRemoteClient,
    spec: HybridTransferSpec,
    batch: Mapping[str, Any],
    purpose: str,
) -> None:
    token = batch.get("cleanup_lease_token")
    if not isinstance(token, str) or batch.get("cleanup_lease_purpose") != purpose:
        raise TransactionCorrupt("cleanup lease receipt is missing")
    specs_sha256 = batch.get("specs_sha256")
    if not isinstance(specs_sha256, str) or not _SHA_RE.fullmatch(specs_sha256):
        raise TransactionCorrupt("durable desired specs digest is missing")
    payload = _cleanup_lease_payload(
        spec.batch_id, specs_sha256, token, purpose
    )
    if not _verify(
        client,
        _cleanup_lease_path(spec),
        len(payload),
        hashlib.sha256(payload).hexdigest(),
    ):
        raise TransactionUncertain("exclusive cleanup lease disappeared")


def _compact_terminal_cleanup_receipt(
    *,
    state_root: Path,
    spec: HybridTransferSpec,
    batch: dict[str, Any],
    purpose: str,
) -> None:
    """Replace local owner material with one non-secret terminal receipt."""
    journal_path, _manifest, _lock_path = _batch_paths(state_root, spec)
    specs_sha256 = batch.get("specs_sha256")
    if not isinstance(specs_sha256, str) or not _SHA_RE.fullmatch(specs_sha256):
        raise TransactionCorrupt("durable desired specs digest is missing")
    receipt = batch.get("cleanup_lease_receipt")
    token = batch.get("cleanup_lease_token")
    if token is not None:
        if (
            not isinstance(token, str)
            or not _SHA_RE.fullmatch(token)
            or batch.get("cleanup_lease_purpose") != purpose
        ):
            raise TransactionCorrupt("cleanup lease owner receipt is invalid")
        payload = _cleanup_lease_payload(
            spec.batch_id, specs_sha256, token, purpose
        )
        receipt = {
            "remote_path": _cleanup_lease_path(spec),
            "sha256": hashlib.sha256(payload).hexdigest(),
            "purpose": purpose,
            "specs_sha256": specs_sha256,
            "scope": "scrapeflow_internal_cleanup_exclusion_only",
        }
        batch["cleanup_lease_receipt"] = receipt
        batch.pop("cleanup_lease_token", None)
        batch.pop("cleanup_lease_purpose", None)
    expected = {
        "remote_path": _cleanup_lease_path(spec),
        "purpose": purpose,
        "specs_sha256": specs_sha256,
        "scope": "scrapeflow_internal_cleanup_exclusion_only",
    }
    if (
        not isinstance(receipt, dict)
        or any(receipt.get(key) != value for key, value in expected.items())
        or not isinstance(receipt.get("sha256"), str)
        or not _SHA_RE.fullmatch(receipt["sha256"])
    ):
        raise TransactionCorrupt("compact cleanup lease receipt is invalid")
    atomic_write_json(journal_path, batch, allow_nan=False, sort_keys=True)
    (state_root / spec.batch_id / "cleanup-lease.payload").unlink(missing_ok=True)


def _validate_cleanup_media_semantics(
    client: HybridRemoteClient,
    spec: HybridTransferSpec,
    journal: Mapping[str, Any],
    terminal_state: str,
) -> None:
    size, sha256 = int(journal["size"]), str(journal["sha256"])
    if terminal_state == "complete":
        if client.stat_exact(spec.source_path) is not None:
            raise TransactionConflict("complete cleanup source unexpectedly exists")
        if spec.operation == "transfer":
            if spec.target_path is None or not _verify(
                client, spec.target_path, size, sha256
            ):
                raise TransactionUncertain("complete cleanup target disappeared")
    elif terminal_state == "restored":
        if not _verify(client, spec.source_path, size, sha256):
            raise TransactionUncertain("restored cleanup source disappeared")
        if spec.target_path is not None and client.stat_exact(spec.target_path) is not None:
            raise TransactionConflict("restored cleanup target unexpectedly exists")
    elif terminal_state == "aborted_unmodified":
        if not _verify(client, spec.source_path, size, sha256):
            raise TransactionUncertain("unmodified cleanup source disappeared")
    else:
        raise TransactionCorrupt("cleanup terminal state is invalid")


def _revalidate_before_cleanup_remove(
    client: HybridRemoteClient,
    *,
    state_root: Path,
    spec: HybridTransferSpec,
    journal: Mapping[str, Any],
    batch: Mapping[str, Any],
    purpose: str,
    terminal_state: str,
    object_path: str,
    object_size: int,
    object_sha256: str,
) -> None:
    _verify_cleanup_lease(client, spec, batch, purpose)
    _assert_batch_sealed(
        client,
        state_root,
        spec,
        allowed_states=("accepted", "committing", "aborting"),
    )
    _validate_cleanup_media_semantics(client, spec, journal, terminal_state)
    if object_path == spec.rollback_path:
        # The rollback payload is the first destructive cleanup.  Preserve
        # both immutable witnesses until its item manifest and the enclosing
        # batch manifest have just been re-read and hash-verified.
        _verify_item_manifest(client, spec, journal)
    if not _verify(client, object_path, object_size, object_sha256):
        raise TransactionUncertain(f"cleanup object disappeared before removal: {object_path}")


def commit_hybrid_transfer(client: HybridRemoteClient, *, state_root: Path,
                           spec: HybridTransferSpec,
                           checkpoint_hook: CheckpointHook | None = None) -> HybridTransferResult:
    """Release the remote recovery copy after the caller's batch acceptance."""
    root, payload, journal_path, lock = _paths(state_root, spec)
    with _lock(lock):
        batch_journal_path, _batch_manifest_local, _batch_lock = _batch_paths(
            state_root, spec
        )
        try:
            batch = json.loads(batch_journal_path.read_text("utf-8"))
        except (FileNotFoundError, OSError, UnicodeError, json.JSONDecodeError) as exc:
            raise TransactionUncertain("batch acceptance receipt is missing") from exc
        durable_specs = _validated_durable_specs(batch, batch_id=spec.batch_id)
        matching_specs = [item for item in durable_specs if item.item_id == spec.item_id]
        if len(matching_specs) != 1 or matching_specs[0].to_dict() != spec.to_dict():
            raise TransactionConflict("current spec differs from durable desired batch")
        if batch.get("state") not in {"accepted", "committing", "committed"}:
            raise TransactionUncertain(
                "individual rollback payload cannot be committed before batch acceptance"
            )
        journal = _load(journal_path, spec)
        if journal is None or journal.get("state") not in {"complete", "restored", "committing", "committed"}:
            raise TransactionUncertain("only a complete/restored transaction may be committed")
        if journal["state"] == "committed":
            _validate_cleanup_media_semantics(
                client,
                spec,
                journal,
                str(journal.get("commit_validated_state")),
            )
            return _result(spec, payload, journal_path, journal)
        if journal["state"] in {"complete", "restored"}:
            terminal_state = str(journal["state"])
            _validate_terminal_semantics(client, spec, journal)
            _save(
                journal_path, journal, "committing", checkpoint_hook,
                commit_validated_state=terminal_state,
                commit_validated_at=_now(),
                commit_validated_sha256=str(journal["sha256"]),
            )
        if journal["state"] != "committed":
            if (
                journal.get("commit_validated_state") not in {"complete", "restored"}
                or journal.get("commit_validated_sha256") != journal.get("sha256")
            ):
                raise TransactionCorrupt("commit has no durable validation receipt")
            terminal_state = str(journal["commit_validated_state"])
            cleanup_objects = (
                (spec.rollback_path, int(journal["size"]), str(journal["sha256"])),
                (
                    spec.remote_manifest_path,
                    int(journal["remote_manifest_size"]),
                    str(journal["remote_manifest_sha256"]),
                ),
            )
            for remote_path, object_size, object_sha256 in cleanup_objects:
                batch = json.loads(batch_journal_path.read_text("utf-8"))
                _revalidate_before_cleanup_remove(
                    client,
                    state_root=state_root,
                    spec=spec,
                    journal=journal,
                    batch=batch,
                    purpose="commit",
                    terminal_state=terminal_state,
                    object_path=remote_path,
                    object_size=object_size,
                    object_sha256=object_sha256,
                )
                try:
                    client.remove_file(remote_path)
                except Exception as exc:
                    if client.stat_exact(remote_path) is not None:
                        raise TransactionUncertain(f"commit deletion uncertain: {remote_path}") from exc
                if client.stat_exact(remote_path) is not None:
                    raise TransactionUncertain(f"commit object remains: {remote_path}")
            payload.unlink(missing_ok=True)
            _save(journal_path, journal, "committed", checkpoint_hook)
        return _result(spec, payload, journal_path, journal)


def _validate_unmodified_abort(
    client: HybridRemoteClient,
    spec: HybridTransferSpec,
    journal: Mapping[str, Any],
) -> None:
    if journal.get("forward_started") or journal.get("state") not in {
        "rollback_verified", "aborted_unmodified",
    }:
        raise TransactionUncertain("item is not an unmodified prepared transaction")
    size, sha256 = int(journal["size"]), str(journal["sha256"])
    if not _verify(client, spec.source_path, size, sha256):
        raise TransactionUncertain("unmodified source cannot be proven during abort")
    if not _verify(client, spec.rollback_path, size, sha256):
        raise TransactionUncertain("unmodified rollback cannot be proven during abort")
    _verify_item_manifest(client, spec, journal)


def _remove_exact_reconciled(client: HybridRemoteClient, path: str) -> None:
    if client.stat_exact(path) is None:
        return
    try:
        client.remove_file(path)
    except Exception as exc:
        if client.stat_exact(path) is not None:
            raise TransactionUncertain(f"abort cleanup is uncertain: {path}") from exc
    if client.stat_exact(path) is not None:
        raise TransactionUncertain(f"abort cleanup object remains: {path}")


def abort_hybrid_batch(
    client: HybridRemoteClient,
    *,
    state_root: Path,
    specs: list[HybridTransferSpec],
    checkpoint_hook: CheckpointHook | None = None,
) -> list[HybridTransferResult]:
    """Safely close a failed/cancelled batch and release proven rollback data.

    Forward items are restored first.  Items that never crossed the forward
    boundary are proven byte-identical at their source and marked
    ``aborted_unmodified``.  No rollback object is removed until every member
    reaches one of those two safe states.
    """
    if not specs:
        raise ValueError("hybrid batch must contain at least one item")
    first = specs[0]
    if any(item.batch_id != first.batch_id for item in specs):
        raise ValueError("all aborted items must share one batch_id")
    journal_path, _manifest_local, batch_lock = _batch_paths(state_root, first)
    try:
        batch = json.loads(journal_path.read_text("utf-8"))
    except (FileNotFoundError, OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise TransactionUncertain("sealed batch receipt is missing") from exc
    durable_specs = _validated_durable_specs(batch, batch_id=first.batch_id)
    _assert_requested_specs(durable_specs, specs)
    if batch.get("item_ids") != sorted(item.item_id for item in specs):
        raise TransactionConflict("abort item set differs from sealed batch")
    if batch.get("state") == "aborted":
        with _lock(batch_lock):
            batch = json.loads(journal_path.read_text("utf-8"))
            _compact_terminal_cleanup_receipt(
                state_root=state_root,
                spec=first,
                batch=batch,
                purpose="abort",
            )
        completed: list[HybridTransferResult] = []
        for item in specs:
            _root, payload, item_journal_path, _item_lock = _paths(state_root, item)
            journal = _load(item_journal_path, item)
            if journal is None or journal.get("state") != "aborted":
                raise TransactionCorrupt("aborted batch has a non-aborted item receipt")
            _validate_cleanup_media_semantics(
                client,
                item,
                journal,
                str(journal.get("abort_validated_state")),
            )
            payload.unlink(missing_ok=True)
            completed.append(_result(item, payload, item_journal_path, journal))
        return completed
    if batch.get("state") == "sealed":
        # Reconcile every member while the immutable batch witness is intact.
        for item in specs:
            _assert_batch_sealed(client, state_root, item)
            _root, _payload, item_journal_path, item_lock = _paths(state_root, item)
            with _lock(item_lock):
                journal = _load(item_journal_path, item)
                if journal is None:
                    raise TransactionCorrupt("abort item journal is missing")
                forward_started = bool(journal.get("forward_started"))
            if forward_started:
                # Complete reconciliation establishes target ownership even if
                # the process crashed immediately after the upload call.
                if journal.get("state") not in {"complete", "restored"}:
                    run_hybrid_transfer(
                        client, state_root=state_root, spec=item,
                        checkpoint_hook=checkpoint_hook,
                    )
                restored = restore_hybrid_transfer(
                    client, state_root=state_root, spec=item,
                    checkpoint_hook=checkpoint_hook,
                )
                if restored.state != "restored":
                    raise TransactionUncertain("forward item did not restore during abort")
            else:
                with _lock(item_lock):
                    journal = _load(item_journal_path, item)
                    if journal is None:
                        raise TransactionCorrupt("abort item journal disappeared")
                    _validate_unmodified_abort(client, item, journal)
                    _save(
                        item_journal_path, journal, "aborted_unmodified",
                        checkpoint_hook,
                    )

        # Revalidate all safe terminal states before the first cleanup.
        for item in specs:
            _root, _payload, item_journal_path, item_lock = _paths(state_root, item)
            with _lock(item_lock):
                journal = _load(item_journal_path, item)
                if journal is None:
                    raise TransactionCorrupt("abort item journal disappeared")
                if journal.get("state") == "restored":
                    _validate_terminal_semantics(client, item, journal)
                elif journal.get("state") == "aborted_unmodified":
                    _validate_unmodified_abort(client, item, journal)
                else:
                    raise TransactionUncertain("abort member is not safely closed")
        with _lock(batch_lock):
            batch = json.loads(journal_path.read_text("utf-8"))
            if batch.get("state") != "sealed":
                raise TransactionConflict("batch state changed during abort validation")
            batch.update({"state": "aborting", "abort_validated_at": _now()})
            _ensure_cleanup_lease(
                client,
                state_root=state_root,
                spec=first,
                batch=batch,
                purpose="abort",
                checkpoint_hook=checkpoint_hook,
            )
            atomic_write_json(journal_path, batch, allow_nan=False, sort_keys=True)
            if checkpoint_hook is not None:
                checkpoint_hook("batch_abort_validated", dict(batch))
    elif batch.get("state") != "aborting":
        raise TransactionUncertain("batch is not eligible for abort")
    else:
        with _lock(batch_lock):
            batch = json.loads(journal_path.read_text("utf-8"))
            _ensure_cleanup_lease(
                client,
                state_root=state_root,
                spec=first,
                batch=batch,
                purpose="abort",
                checkpoint_hook=checkpoint_hook,
            )
            atomic_write_json(journal_path, batch, allow_nan=False, sort_keys=True)

    results: list[HybridTransferResult] = []
    for item in specs:
        root, payload, item_journal_path, item_lock = _paths(state_root, item)
        with _lock(item_lock):
            journal = _load(item_journal_path, item)
            if journal is None:
                raise TransactionCorrupt("abort item journal disappeared")
            if journal.get("state") != "aborted":
                if journal.get("state") in {"restored", "aborted_unmodified"}:
                    if journal.get("state") == "restored":
                        _validate_terminal_semantics(client, item, journal)
                    else:
                        _validate_unmodified_abort(client, item, journal)
                    _save(
                        item_journal_path, journal, "aborting", checkpoint_hook,
                        abort_validated_state=str(journal["state"]),
                        abort_validated_sha256=str(journal["sha256"]),
                    )
                elif journal.get("state") != "aborting":
                    raise TransactionCorrupt("abort cleanup has no validation receipt")
                if journal.get("abort_validated_sha256") != journal.get("sha256"):
                    raise TransactionCorrupt("abort cleanup digest receipt is invalid")
                terminal_state = str(journal.get("abort_validated_state"))
                cleanup_objects = (
                    (item.rollback_path, int(journal["size"]), str(journal["sha256"])),
                    (
                        item.remote_manifest_path,
                        int(journal["remote_manifest_size"]),
                        str(journal["remote_manifest_sha256"]),
                    ),
                )
                for remote_path, object_size, object_sha256 in cleanup_objects:
                    batch = json.loads(journal_path.read_text("utf-8"))
                    _revalidate_before_cleanup_remove(
                        client,
                        state_root=state_root,
                        spec=item,
                        journal=journal,
                        batch=batch,
                        purpose="abort",
                        terminal_state=terminal_state,
                        object_path=remote_path,
                        object_size=object_size,
                        object_sha256=object_sha256,
                    )
                    try:
                        client.remove_file(remote_path)
                    except Exception as exc:
                        if client.stat_exact(remote_path) is not None:
                            raise TransactionUncertain(
                                f"abort cleanup is uncertain: {remote_path}"
                            ) from exc
                    if client.stat_exact(remote_path) is not None:
                        raise TransactionUncertain(
                            f"abort cleanup object remains: {remote_path}"
                        )
                payload.unlink(missing_ok=True)
                _save(item_journal_path, journal, "aborted", checkpoint_hook)
            results.append(_result(item, payload, item_journal_path, journal))

    with _lock(batch_lock):
        batch = json.loads(journal_path.read_text("utf-8"))
        _verify_cleanup_lease(client, first, batch, "abort")
        for item in specs:
            _root, _payload, item_path, _item_lock = _paths(state_root, item)
            item_journal = _load(item_path, item)
            if item_journal is None or item_journal.get("state") != "aborted":
                raise TransactionUncertain("batch abort item terminal receipt is missing")
            _validate_cleanup_media_semantics(
                client,
                item,
                item_journal,
                str(item_journal.get("abort_validated_state")),
            )
        batch_manifest = _batch_manifest_path(first)
        if not _verify(
            client,
            batch_manifest,
            int(batch["manifest_size"]),
            str(batch["manifest_sha256"]),
        ):
            raise TransactionUncertain("batch manifest disappeared before abort removal")
        try:
            client.remove_file(batch_manifest)
        except Exception as exc:
            if client.stat_exact(batch_manifest) is not None:
                raise TransactionUncertain(
                    "batch manifest abort deletion uncertain"
                ) from exc
        if client.stat_exact(batch_manifest) is not None:
            raise TransactionUncertain("batch manifest remains after abort")
        batch.update({"state": "aborted", "aborted_at": _now()})
        _compact_terminal_cleanup_receipt(
            state_root=state_root,
            spec=first,
            batch=batch,
            purpose="abort",
        )
        if checkpoint_hook is not None:
            checkpoint_hook("batch_aborted", dict(batch))
    return results


def commit_hybrid_batch(
    client: HybridRemoteClient,
    *,
    state_root: Path,
    specs: list[HybridTransferSpec],
    checkpoint_hook: CheckpointHook | None = None,
) -> list[HybridTransferResult]:
    """Accept a complete batch, then release every remote rollback payload."""
    if not specs:
        raise ValueError("hybrid batch must contain at least one item")
    first = specs[0]
    if any(spec.batch_id != first.batch_id for spec in specs):
        raise ValueError("all committed items must share one batch_id")
    journal_path, _manifest_local, lock = _batch_paths(state_root, first)
    with _lock(lock):
        try:
            batch = json.loads(journal_path.read_text("utf-8"))
        except (FileNotFoundError, OSError, UnicodeError, json.JSONDecodeError) as exc:
            raise TransactionUncertain("sealed batch receipt is missing") from exc
        durable_specs = _validated_durable_specs(batch, batch_id=first.batch_id)
        _assert_requested_specs(durable_specs, specs)
        expected_ids = sorted(spec.item_id for spec in specs)
        if batch.get("item_ids") != expected_ids:
            raise TransactionConflict("commit item set differs from sealed batch")
        if batch.get("state") == "committed":
            _compact_terminal_cleanup_receipt(
                state_root=state_root,
                spec=first,
                batch=batch,
                purpose="commit",
            )
            completed: list[HybridTransferResult] = []
            for spec in specs:
                _root, payload, item_path, _item_lock = _paths(state_root, spec)
                item = _load(item_path, spec)
                if item is None or item.get("state") != "committed":
                    raise TransactionCorrupt(
                        "committed batch has a non-committed item receipt"
                    )
                _validate_cleanup_media_semantics(
                    client,
                    spec,
                    item,
                    str(item.get("commit_validated_state")),
                )
                completed.append(_result(spec, payload, item_path, item))
            return completed
        if batch.get("state") == "sealed":
            # Validate every item before the first recovery payload is removed.
            for spec in specs:
                _assert_batch_sealed(client, state_root, spec)
                _root, _payload, item_journal_path, _item_lock = _paths(state_root, spec)
                item = _load(item_journal_path, spec)
                if item is None or item.get("state") not in {"complete", "restored"}:
                    raise TransactionUncertain(
                        f"batch item has not reached an accepted terminal state: {spec.item_id}"
                    )
                _validate_terminal_semantics(client, spec, item)
            batch.update({"state": "accepted", "accepted_at": _now()})
            atomic_write_json(journal_path, batch, allow_nan=False, sort_keys=True)
            if checkpoint_hook is not None:
                checkpoint_hook("batch_accepted", dict(batch))
        if batch.get("state") not in {"accepted", "committing", "committed"}:
            raise TransactionUncertain("batch is not eligible for commit")
        if batch.get("state") != "committed":
            batch["state"] = "committing"
            _ensure_cleanup_lease(
                client,
                state_root=state_root,
                spec=first,
                batch=batch,
                purpose="commit",
                checkpoint_hook=checkpoint_hook,
            )
            atomic_write_json(journal_path, batch, allow_nan=False, sort_keys=True)

    results = [
        commit_hybrid_transfer(
            client, state_root=state_root, spec=spec,
            checkpoint_hook=checkpoint_hook,
        )
        for spec in specs
    ]

    with _lock(lock):
        batch = json.loads(journal_path.read_text("utf-8"))
        remote_manifest = _batch_manifest_path(first)
        _verify_cleanup_lease(client, first, batch, "commit")
        for spec in specs:
            _root, _payload, item_path, _item_lock = _paths(state_root, spec)
            item = _load(item_path, spec)
            if item is None or item.get("state") != "committed":
                raise TransactionUncertain("batch commit item terminal receipt is missing")
            _validate_cleanup_media_semantics(
                client,
                spec,
                item,
                str(item.get("commit_validated_state")),
            )
        if not _verify(
            client,
            remote_manifest,
            int(batch["manifest_size"]),
            str(batch["manifest_sha256"]),
        ):
            raise TransactionUncertain("batch manifest disappeared before commit removal")
        try:
            client.remove_file(remote_manifest)
        except Exception as exc:
            if client.stat_exact(remote_manifest) is not None:
                raise TransactionUncertain("batch manifest commit deletion uncertain") from exc
        if client.stat_exact(remote_manifest) is not None:
            raise TransactionUncertain("batch manifest remains after commit")
        batch.update({"state": "committed", "committed_at": _now()})
        _compact_terminal_cleanup_receipt(
            state_root=state_root,
            spec=first,
            batch=batch,
            purpose="commit",
        )
    return results


__all__ = [
    "DEFAULT_ROLLBACK_ROOT", "REMOTE_ROLLBACK_ROOT_ENV", "HybridRemoteClient", "HybridTransferResult",
    "HybridTransferSpec", "abort_hybrid_batch", "commit_hybrid_batch", "commit_hybrid_transfer", "prepare_hybrid_batch",
    "load_sealed_batch_specs", "prepare_hybrid_transfer", "restore_hybrid_transfer",
    "run_hybrid_transfer",
]
