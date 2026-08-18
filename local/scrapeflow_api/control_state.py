"""Small, fail-closed persistent control state for the local scheduler."""

from __future__ import annotations

import json
import os
import threading
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Iterator, Mapping

from engine.scrapeflow.serialization import atomic_write_json

from .root_job_pilot import (
    RootJobPilotError,
    disabled_scope,
    normalize_scope,
)


_DOCUMENT_VERSION = 3
_LEGACY_DOCUMENT_VERSION = 1
_SCOPED_DOCUMENT_VERSION = 2
_LEGACY_DOCUMENT_FIELDS = frozenset({
    "version", "paused", "scheduler_paused", "persistent", "updated_at", "reason",
})
_SCOPED_DOCUMENT_FIELDS = _LEGACY_DOCUMENT_FIELDS | {"automatic_scope"}
_DOCUMENT_FIELDS = _SCOPED_DOCUMENT_FIELDS | {"revision"}


def _now() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


class PersistentControlState:
    """Read one operator-controlled pause bit without ever bootstrapping it.

    A missing, partial, or malformed document is intentionally treated as a
    pause.  Reading state never repairs the file: only an explicit operator
    pause/resume is allowed to publish a new document.
    """

    def __init__(self, path: Path) -> None:
        self.path = Path(path)
        self._lock = threading.RLock()

    @contextmanager
    def _process_lock(self) -> Iterator[None]:
        """Serialize a control transition across API processes.

        ``atomic_write_json`` prevents a torn document, but it does not make
        a read/compare/write sequence atomic across two processes.  Keep a
        stable sidecar inode in the state directory: locking the JSON file
        itself would be defeated by its atomic ``os.replace`` publication.
        """
        try:
            import fcntl
        except ImportError as exc:  # pragma: no cover - deployment is Unix
            raise RuntimeError("控制状态跨进程锁需要 fcntl") from exc
        locks_root = self.path.parent / "locks"
        locks_root.mkdir(mode=0o700, parents=True, exist_ok=True)
        descriptor = os.open(
            locks_root / "control-state.lock",
            os.O_CREAT | os.O_RDWR,
            0o600,
        )
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX)
            yield
        finally:
            try:
                fcntl.flock(descriptor, fcntl.LOCK_UN)
            finally:
                os.close(descriptor)

    @staticmethod
    def _blocked(reason: str) -> dict[str, object]:
        return {
            "version": _DOCUMENT_VERSION,
            "paused": True,
            "scheduler_paused": True,
            "persistent": True,
            "updated_at": None,
            "reason": reason,
            # A missing/corrupt control record must not inherit an implicit
            # all-root scheduler grant.  A public resume writes an explicit
            # single_root scope before it can open the pause fence.
            "automatic_scope": disabled_scope(),
            # This is a local compare-and-set generation.  It lets a resume
            # reject a pause/scope change that happened while its no-write
            # preflight was running, instead of overwriting that decision.
            "revision": 0,
        }

    @staticmethod
    def _validate(payload: object) -> dict[str, object]:
        if not isinstance(payload, Mapping):
            raise ValueError("invalid control document fields")
        version = payload.get("version")
        fields = set(payload)
        revision = 0
        if version == _LEGACY_DOCUMENT_VERSION:
            if fields != _LEGACY_DOCUMENT_FIELDS:
                raise ValueError("invalid control document fields")
            # Version-1 records predate a durable RootJob selector.  They may
            # describe a historical unpaused process, but they cannot safely
            # authorize a new scheduler after this migration.  The next
            # explicit pilot arm/resume writes a version-2 scope.
            automatic_scope = disabled_scope()
        elif version == _SCOPED_DOCUMENT_VERSION:
            if fields != _SCOPED_DOCUMENT_FIELDS:
                raise ValueError("invalid control document fields")
            try:
                automatic_scope = normalize_scope(payload.get("automatic_scope"))
            except RootJobPilotError as exc:
                raise ValueError("invalid automatic scope") from exc
        elif version == _DOCUMENT_VERSION:
            if fields != _DOCUMENT_FIELDS:
                raise ValueError("invalid control document fields")
            try:
                automatic_scope = normalize_scope(payload.get("automatic_scope"))
            except RootJobPilotError as exc:
                raise ValueError("invalid automatic scope") from exc
            revision = payload.get("revision")
        else:
            raise ValueError("invalid control document version")
        paused = payload.get("paused")
        scheduler_paused = payload.get("scheduler_paused")
        persistent = payload.get("persistent")
        updated_at = payload.get("updated_at")
        reason = payload.get("reason")
        if (
            type(version) is not int
            or type(paused) is not bool
            or type(scheduler_paused) is not bool
            or scheduler_paused is not paused
            or persistent is not True
            or not isinstance(updated_at, str)
            or not updated_at
            or (reason is not None and not isinstance(reason, str))
            or type(revision) is not int
            or revision < 0
        ):
            raise ValueError("invalid control document values")
        return {
            "version": version,
            "paused": paused,
            "scheduler_paused": scheduler_paused,
            "persistent": persistent,
            "updated_at": updated_at,
            "reason": reason,
            "automatic_scope": automatic_scope,
            "revision": revision,
        }

    def _read_locked(self) -> dict[str, object]:
        """Read under ``_lock`` without recursively opening a write window."""
        try:
            raw = self.path.read_text(encoding="utf-8")
            return self._validate(json.loads(raw))
        except FileNotFoundError:
            return self._blocked("control_state_missing")
        except (OSError, UnicodeDecodeError, json.JSONDecodeError, TypeError, ValueError):
            return self._blocked("control_state_invalid")

    @staticmethod
    def _validated_scope(
        automatic_scope: Mapping[str, object] | None,
        current: Mapping[str, object],
    ) -> dict[str, object]:
        if automatic_scope is None:
            candidate = current.get("automatic_scope")
        else:
            candidate = automatic_scope
        try:
            return normalize_scope(candidate)
        except RootJobPilotError as exc:
            if automatic_scope is None:
                return disabled_scope()
            raise ValueError("invalid automatic scope") from exc

    @staticmethod
    def _validated_transition(
        paused: bool,
        reason: str | None,
    ) -> tuple[bool, str]:
        if type(paused) is not bool:
            raise TypeError("paused must be boolean")
        if reason is not None and not isinstance(reason, str):
            raise TypeError("reason must be a string")
        return paused, reason.strip()[:500] if isinstance(reason, str) else ""

    def _write_locked(
        self,
        current: Mapping[str, object],
        *,
        paused: bool,
        normalized_reason: str,
        automatic_scope: Mapping[str, object],
    ) -> dict[str, object]:
        revision = current.get("revision")
        # ``_read_locked`` always gives a checked integer, including a
        # synthetic blocked state for missing/corrupt documents.
        if type(revision) is not int or revision < 0:
            raise ValueError("invalid control revision")
        payload: dict[str, object] = {
            "version": _DOCUMENT_VERSION,
            "paused": paused,
            "scheduler_paused": paused,
            "persistent": True,
            "updated_at": _now(),
            "reason": normalized_reason or "operator pause" if paused else None,
            "automatic_scope": dict(automatic_scope),
            "revision": revision + 1,
        }
        atomic_write_json(self.path, payload, allow_nan=False)
        return dict(payload)

    def read(self) -> dict[str, object]:
        """Return a fresh snapshot, pausing on every failed read or validation."""
        with self._lock:
            return self._read_locked()

    def set_paused(
        self,
        paused: bool,
        reason: str | None = None,
        *,
        automatic_scope: Mapping[str, object] | None = None,
    ) -> dict[str, object]:
        """Persist one explicit operator state transition and scope.

        ``automatic_scope`` is normally supplied by the public single-RootJob
        arm/resume operation.  Omitting it never widens a disabled scope: an
        internal caller that needs all-root behavior must state that test-only
        exception explicitly with ``{"mode": "all", "root_job_id": None}``.
        """
        paused, normalized_reason = self._validated_transition(paused, reason)
        with self._lock:
            with self._process_lock():
                current = self._read_locked()
                normalized_scope = self._validated_scope(automatic_scope, current)
                return self._write_locked(
                    current,
                    paused=paused,
                    normalized_reason=normalized_reason,
                    automatic_scope=normalized_scope,
                )

    def compare_and_set_paused(
        self,
        *,
        expected_revision: object,
        expected_paused: bool,
        expected_scope: Mapping[str, object],
        paused: bool,
        reason: str | None = None,
        automatic_scope: Mapping[str, object] | None = None,
    ) -> dict[str, object] | None:
        """Write one transition only if the durable control snapshot matches.

        The API uses this after a potentially slow no-write preflight.  A
        ``None`` return is a conflict, not a reason to retry or reopen the
        scheduler: the caller must make the operator inspect the new state.
        """
        if type(expected_revision) is not int or expected_revision < 0:
            raise TypeError("expected_revision must be a non-negative integer")
        if type(expected_paused) is not bool:
            raise TypeError("expected_paused must be boolean")
        try:
            normalized_expected_scope = normalize_scope(expected_scope)
        except RootJobPilotError as exc:
            raise ValueError("invalid expected automatic scope") from exc
        paused, normalized_reason = self._validated_transition(paused, reason)
        with self._lock:
            with self._process_lock():
                current = self._read_locked()
                if (
                    current.get("revision") != expected_revision
                    or current.get("paused") is not expected_paused
                    or current.get("automatic_scope") != normalized_expected_scope
                ):
                    return None
                normalized_scope = self._validated_scope(automatic_scope, current)
                return self._write_locked(
                    current,
                    paused=paused,
                    normalized_reason=normalized_reason,
                    automatic_scope=normalized_scope,
                )


__all__ = ["PersistentControlState"]
