"""Shared job lifecycle contract for persistence, HTTP and orchestration."""

from __future__ import annotations

from collections.abc import Mapping


TERMINAL_PHASES = frozenset({"completed", "failed", "cancelled", "recovered"})
EXECUTION_PHASES = frozenset({
    "starting_archive_execution", "extracting_archives",
    "starting_media_execution", "executing_media",
    "starting_recovery_execution", "executing_recovery",
})
VALID_PHASES = TERMINAL_PHASES | frozenset({
    "queued", "planning_archives",
    "starting_archive_execution", "extracting_archives", "planning_media",
    "awaiting_media_approval", "starting_media_execution", "executing_media",
    "replenishing",
    "planning_recovery", "awaiting_recovery_approval",
    "starting_recovery_execution", "executing_recovery",
    "cancelling", "recovery_required",
})

PHASE_TRANSITIONS: Mapping[str, frozenset[str]] = {
    "queued": frozenset({
        "planning_archives", "planning_media", "cancelling", "cancelled",
        "failed",
    }),
    "planning_archives": frozenset({"starting_archive_execution", "planning_media", "cancelling", "cancelled", "failed"}),
    "starting_archive_execution": frozenset({"extracting_archives", "cancelling", "cancelled", "failed"}),
    "extracting_archives": frozenset({"planning_media", "cancelling", "cancelled", "failed"}),
    "planning_media": frozenset({
        "awaiting_media_approval", "starting_media_execution",
        "queued", "completed", "cancelling", "cancelled", "failed",
    }),
    "awaiting_media_approval": frozenset({"starting_media_execution", "cancelled", "failed"}),
    "starting_media_execution": frozenset({"awaiting_media_approval", "executing_media", "cancelling", "cancelled", "failed", "recovery_required"}),
    "executing_media": frozenset({"replenishing", "completed", "cancelling", "cancelled", "failed", "recovery_required"}),
    "replenishing": frozenset({"completed", "cancelling", "cancelled", "failed"}),
    "planning_recovery": frozenset({"awaiting_recovery_approval", "starting_recovery_execution", "cancelling", "failed", "recovery_required"}),
    "awaiting_recovery_approval": frozenset({"starting_recovery_execution", "recovery_required", "failed"}),
    "starting_recovery_execution": frozenset({"awaiting_recovery_approval", "executing_recovery", "cancelling", "failed", "recovery_required"}),
    "executing_recovery": frozenset({"recovered", "cancelling", "failed", "recovery_required"}),
    "cancelling": frozenset({"completed", "cancelled", "failed", "recovery_required"}),
    "recovery_required": frozenset({"planning_recovery"}),
    # A deterministic preflight failure may be explicitly dismissed by the
    # user (for example, keeping an already-populated target version).  This
    # is a cancellation, not a successful scrape.
    "failed": frozenset({
        "queued", "starting_archive_execution", "planning_recovery", "cancelled",
    }),
    "completed": frozenset(),
    "recovered": frozenset({"queued"}),
    "cancelled": frozenset(),
}


def require_transition(current: str, target: str) -> None:
    """Reject lifecycle changes that are not part of the public workflow."""
    if current == target:
        return
    if current not in VALID_PHASES or target not in VALID_PHASES:
        raise ValueError(f"未知任务阶段转换: {current} → {target}")
    if target not in PHASE_TRANSITIONS[current]:
        raise ValueError(f"不允许的任务阶段转换: {current} → {target}")
