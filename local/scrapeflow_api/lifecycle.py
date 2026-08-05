"""Durable process-wide scheduling control.

The pause bit is deliberately independent from individual job records.  It
gates new scheduler dispatch without changing a job phase, setting its cancel
flag, or touching a remote process.  This makes a pause restart-safe and keeps
the original task audit trail intact.
"""

from __future__ import annotations

import json
import os
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


class PersistentGlobalControl:
    """Thread-safe, fail-closed persistent global pause state."""

    def __init__(self, path: Path, *, default_paused: bool = False) -> None:
        self.path = path
        self._lock = threading.RLock()
        self._paused = False
        self._updated_at: str | None = None
        self._reason: str | None = None
        existed = self.path.exists()
        self.reload()
        if not existed and default_paused:
            self.set_paused(True, reason="bootstrap_pause")

    def reload(self) -> dict[str, Any]:
        with self._lock:
            if not self.path.exists():
                self._paused = False
                self._updated_at = None
                self._reason = None
                return self.snapshot()
            try:
                payload = json.loads(self.path.read_text(encoding="utf-8"))
                if not isinstance(payload, dict) or payload.get("version") != 1:
                    raise ValueError("unsupported global control document")
                paused = payload.get("paused")
                updated_at = payload.get("updated_at")
                reason = payload.get("reason")
                if type(paused) is not bool or not isinstance(updated_at, str):
                    raise ValueError("invalid global control document")
                if reason is not None and not isinstance(reason, str):
                    raise ValueError("invalid global control reason")
            except (OSError, ValueError, json.JSONDecodeError):
                # A damaged control file must never accidentally resume remote
                # work.  Surface the state as paused until an explicit resume
                # rewrites a valid document.
                self._paused = True
                self._updated_at = _utc_now()
                self._reason = "global_control_state_invalid"
                return self.snapshot()
            self._paused = paused
            self._updated_at = updated_at
            self._reason = reason
            return self.snapshot()

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            return {
                "paused": self._paused,
                "updated_at": self._updated_at,
                "reason": self._reason,
                "persistent": True,
            }

    @property
    def paused(self) -> bool:
        with self._lock:
            return self._paused

    def set_paused(self, paused: bool, *, reason: str | None = None) -> dict[str, Any]:
        if type(paused) is not bool:
            raise ValueError("paused must be a boolean")
        if reason is not None and (not isinstance(reason, str) or len(reason) > 500):
            raise ValueError("invalid global pause reason")
        now = _utc_now()
        normalized_reason = reason.strip() if isinstance(reason, str) and reason.strip() else None
        payload = {
            "version": 1,
            "paused": paused,
            "updated_at": now,
            "reason": normalized_reason if paused else None,
        }
        with self._lock:
            self.path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
            temporary = self.path.with_suffix(self.path.suffix + ".tmp")
            temporary.write_text(
                json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2) + "\n",
                encoding="utf-8",
            )
            os.chmod(temporary, 0o600)
            temporary.replace(self.path)
            self._paused = paused
            self._updated_at = now
            self._reason = payload["reason"]
            return self.snapshot()
