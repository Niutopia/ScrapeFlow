"""One small, local control record for the single-user scheduler."""

from __future__ import annotations

import json
import re
from pathlib import Path

from engine.scrapeflow.serialization import atomic_write_json


_ROOT_JOB_ID = re.compile(r"\A[a-zA-Z0-9][a-zA-Z0-9._-]{0,127}\Z")
_UNSET = object()


class PersistentControlState:
    """Persist exactly the pause bit and the selected public RootJob.

    ScrapeFlow has one local API process, so this is deliberately not a
    multi-process coordination protocol: no revision, CAS, scope modes, or
    sidecar file lock. A malformed or missing record simply starts paused.
    """

    def __init__(self, path: Path) -> None:
        self.path = Path(path)

    @staticmethod
    def _default() -> dict[str, object]:
        return {"paused": True, "root_job_id": None}

    @staticmethod
    def _validate(payload: object) -> dict[str, object]:
        if not isinstance(payload, dict) or set(payload) != {"paused", "root_job_id"}:
            raise ValueError("invalid control record")
        paused = payload.get("paused")
        root_job_id = payload.get("root_job_id")
        if type(paused) is not bool:
            raise ValueError("invalid pause value")
        if root_job_id is not None and (
            not isinstance(root_job_id, str)
            or _ROOT_JOB_ID.fullmatch(root_job_id) is None
        ):
            raise ValueError("invalid root job id")
        return {"paused": paused, "root_job_id": root_job_id}

    def read(self) -> dict[str, object]:
        try:
            return self._validate(json.loads(self.path.read_text(encoding="utf-8")))
        except (FileNotFoundError, OSError, UnicodeDecodeError, json.JSONDecodeError, ValueError, TypeError):
            return self._default()

    def set(
        self,
        *,
        paused: bool,
        root_job_id: object = _UNSET,
    ) -> dict[str, object]:
        if type(paused) is not bool:
            raise TypeError("paused must be boolean")
        current = self.read()
        selected = current["root_job_id"] if root_job_id is _UNSET else root_job_id
        payload = self._validate({"paused": paused, "root_job_id": selected})
        atomic_write_json(self.path, payload, allow_nan=False)
        return payload


__all__ = ["PersistentControlState"]
