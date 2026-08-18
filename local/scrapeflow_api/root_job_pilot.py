"""Fail-closed RootJob scope helpers for a bounded automatic pilot.

The scheduler has several independent entry points (startup recovery, retry
timers, the formal writer and both Provider lanes).  A single durable scope is
therefore deliberately represented as plain data and checked at every entry
point rather than being a UI-only filter.
"""

from __future__ import annotations

import os
import re
from collections.abc import Mapping


ROOT_JOB_PILOT_ENV = "SCRAPEFLOW_ROOT_JOB_PILOT"
_ROOT_JOB_ID_RE = re.compile(r"\A[a-zA-Z0-9][a-zA-Z0-9._-]{0,127}\Z")

SCOPE_NONE = "none"
SCOPE_SINGLE_ROOT = "single_root"
SCOPE_ALL = "all"
_SCOPE_MODES = frozenset({SCOPE_NONE, SCOPE_SINGLE_ROOT, SCOPE_ALL})


class RootJobPilotError(ValueError):
    """A persisted or configured automatic RootJob scope is unsafe."""


def normalize_root_job_id(value: object) -> str:
    """Return one safe Engine/RootJob id or fail closed.

    This intentionally uses the same portable identifier grammar as the
    engine job store.  It keeps a control document from becoming a path or
    selector injection surface before the application looks up the job.
    """
    if not isinstance(value, str) or _ROOT_JOB_ID_RE.fullmatch(value) is None:
        raise RootJobPilotError("RootJob 试运行 id 无效")
    return value


def disabled_scope() -> dict[str, object]:
    """Return a scope that authorizes no automatic RootJob."""
    return {"mode": SCOPE_NONE, "root_job_id": None}


def unrestricted_scope() -> dict[str, object]:
    """Return the legacy all-root scope for internal compatibility only."""
    return {"mode": SCOPE_ALL, "root_job_id": None}


def single_root_scope(root_job_id: object) -> dict[str, object]:
    """Return the exact-one-root scope used by the public resume operation."""
    return {"mode": SCOPE_SINGLE_ROOT, "root_job_id": normalize_root_job_id(root_job_id)}


def normalize_scope(value: object) -> dict[str, object]:
    """Validate and copy a persisted automatic scope.

    No omitted/unknown shape is interpreted as broad permission.  Older
    control documents are handled explicitly by ``PersistentControlState``;
    this parser itself always treats malformed data as an error.
    """
    if not isinstance(value, Mapping) or set(value) != {"mode", "root_job_id"}:
        raise RootJobPilotError("RootJob 试运行范围格式无效")
    mode = value.get("mode")
    root_job_id = value.get("root_job_id")
    if not isinstance(mode, str) or mode not in _SCOPE_MODES:
        raise RootJobPilotError("RootJob 试运行范围模式无效")
    if mode == SCOPE_SINGLE_ROOT:
        return single_root_scope(root_job_id)
    if root_job_id is not None:
        raise RootJobPilotError("非单 RootJob 范围不得携带 root_job_id")
    return disabled_scope() if mode == SCOPE_NONE else unrestricted_scope()


def environment_root_job_pilot(
    environ: Mapping[str, str] | None = None,
) -> str | None:
    """Read the optional deployment-level RootJob ceiling.

    The environment selector intersects the durable control scope.  It is
    useful when an operator wants a Compose-level guard in addition to the
    API's persisted one.  A malformed non-empty value is never treated as an
    absent selector.
    """
    source = os.environ if environ is None else environ
    raw = source.get(ROOT_JOB_PILOT_ENV, "")
    if not isinstance(raw, str):
        raise RootJobPilotError(f"{ROOT_JOB_PILOT_ENV} 必须是 RootJob id")
    value = raw.strip()
    return normalize_root_job_id(value) if value else None


def root_job_allowed(
    scope: object,
    root_job_id: object,
    *,
    environment_root_job_id: str | None = None,
) -> bool:
    """Whether one RootJob is allowed through the automatic scheduler.

    The caller should regard parser/configuration failures as ``False``.  A
    deployment selector, when present, is a hard intersection rather than an
    alternate way to widen a durable scope.
    """
    normalized_scope = normalize_scope(scope)
    candidate = normalize_root_job_id(root_job_id)
    if environment_root_job_id is not None:
        configured = normalize_root_job_id(environment_root_job_id)
        if candidate != configured:
            return False
    mode = normalized_scope["mode"]
    if mode == SCOPE_ALL:
        return True
    return mode == SCOPE_SINGLE_ROOT and normalized_scope["root_job_id"] == candidate


__all__ = [
    "ROOT_JOB_PILOT_ENV",
    "RootJobPilotError",
    "SCOPE_ALL",
    "SCOPE_NONE",
    "SCOPE_SINGLE_ROOT",
    "disabled_scope",
    "environment_root_job_pilot",
    "normalize_root_job_id",
    "normalize_scope",
    "root_job_allowed",
    "single_root_scope",
    "unrestricted_scope",
]
