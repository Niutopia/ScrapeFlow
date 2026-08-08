"""Small, fail-closed persistent control state for the local scheduler."""

from __future__ import annotations

import json
import threading
from datetime import UTC, datetime
from pathlib import Path
from typing import Mapping

from engine.scrapeflow.serialization import atomic_write_json


_DOCUMENT_VERSION = 1
_DOCUMENT_FIELDS = frozenset({
    "version", "paused", "scheduler_paused", "persistent", "updated_at", "reason",
})


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

    @staticmethod
    def _blocked(reason: str) -> dict[str, object]:
        return {
            "version": _DOCUMENT_VERSION,
            "paused": True,
            "scheduler_paused": True,
            "persistent": True,
            "updated_at": None,
            "reason": reason,
        }

    @staticmethod
    def _validate(payload: object) -> dict[str, object]:
        if not isinstance(payload, Mapping) or set(payload) != _DOCUMENT_FIELDS:
            raise ValueError("invalid control document fields")
        version = payload.get("version")
        paused = payload.get("paused")
        scheduler_paused = payload.get("scheduler_paused")
        persistent = payload.get("persistent")
        updated_at = payload.get("updated_at")
        reason = payload.get("reason")
        if (
            type(version) is not int
            or version != _DOCUMENT_VERSION
            or type(paused) is not bool
            or type(scheduler_paused) is not bool
            or scheduler_paused is not paused
            or persistent is not True
            or not isinstance(updated_at, str)
            or not updated_at
            or (reason is not None and not isinstance(reason, str))
        ):
            raise ValueError("invalid control document values")
        return {
            "version": version,
            "paused": paused,
            "scheduler_paused": scheduler_paused,
            "persistent": persistent,
            "updated_at": updated_at,
            "reason": reason,
        }

    def read(self) -> dict[str, object]:
        """Return a fresh snapshot, pausing on every failed read or validation."""
        with self._lock:
            try:
                raw = self.path.read_text(encoding="utf-8")
                return self._validate(json.loads(raw))
            except FileNotFoundError:
                return self._blocked("control_state_missing")
            except (OSError, UnicodeDecodeError, json.JSONDecodeError, TypeError, ValueError):
                return self._blocked("control_state_invalid")

    def set_paused(self, paused: bool, reason: str | None = None) -> dict[str, object]:
        """Persist the one explicit operator state transition."""
        if type(paused) is not bool:
            raise TypeError("paused must be boolean")
        if reason is not None and not isinstance(reason, str):
            raise TypeError("reason must be a string")
        normalized_reason = reason.strip()[:500] if isinstance(reason, str) else ""
        payload: dict[str, object] = {
            "version": _DOCUMENT_VERSION,
            "paused": paused,
            "scheduler_paused": paused,
            "persistent": True,
            "updated_at": _now(),
            "reason": normalized_reason or "operator pause" if paused else None,
        }
        with self._lock:
            atomic_write_json(self.path, payload, allow_nan=False)
        return dict(payload)


__all__ = ["PersistentControlState"]
