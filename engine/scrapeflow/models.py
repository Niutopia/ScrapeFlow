"""Domain models shared by planning, execution and recovery."""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Mapping

from .serialization import write_json_reserved


@dataclass(frozen=True, order=True)
class EpisodeKey:
    """Episode number recognized from a source filename."""

    kind: str
    number: int
    end_number: int = 0
    fractional_digits: str = ""

    @property
    def display(self) -> str:
        if self.kind == "fractional":
            suffix = f".{self.fractional_digits}" if self.fractional_digits else ""
            return f"E{self.number:02d}{suffix}"
        prefix = "SP" if self.kind == "special" else "E"
        start = f"{prefix}{self.number:02d}"
        if self.end_number and self.end_number != self.number:
            return f"{start}-{prefix}{self.end_number:02d}"
        return start


@dataclass
class PlannedFile:
    source_path: str
    source_dir: str
    original_name: str
    final_name: str
    target_dir: str
    media_kind: str
    episode_key: str | None = None
    source_size: int | None = None
    source_modified: str | None = None
    source_hash: str | None = None

    @property
    def requires_rename(self) -> bool:
        return self.original_name != self.final_name


@dataclass
class PlannedCleanup:
    source_path: str
    source_dir: str
    original_name: str
    reason: str
    source_size: int | None = None
    source_modified: str | None = None
    source_hash: str | None = None


@dataclass
class PlannedProblem:
    """A source file that needs explicit attention during plan review."""

    source_path: str
    reason: str
    target_path: str | None = None


@dataclass(frozen=True)
class PlanNotice:
    """Machine-readable planning notice used by gates and the UI."""

    code: str
    severity: str
    requires_review: bool
    message: str
    evidence: dict[str, Any] = field(default_factory=dict)


@dataclass
class Plan:
    mode: str
    source_root: str
    target_root: str
    files: list[PlannedFile]
    warnings: list[str]
    metadata: dict[str, Any]
    cleanup_files: list[PlannedCleanup] = field(default_factory=list)
    problem_files: list[PlannedProblem] = field(default_factory=list)
    notices: list[PlanNotice] = field(default_factory=list)
    decision_trace: dict[str, Any] = field(default_factory=dict)
    scan_report: dict[str, Any] = field(default_factory=dict)


@dataclass
class ExecutionRecord:
    action: str
    source: str
    target: str
    status: str
    message: str = ""


def _plan_sha256(plan: Mapping[str, Any]) -> str:
    payload = json.dumps(
        plan, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


@dataclass
class ExecutionJournal:
    created_at: str
    plan: dict[str, Any]
    records: list[ExecutionRecord]
    success: bool = False

    def save(self, path: Path) -> None:
        payload = {
            "created_at": self.created_at,
            "plan_sha256": _plan_sha256(self.plan),
            "plan": self.plan,
            "records": [asdict(item) for item in self.records],
            "success": self.success,
        }
        write_json_reserved(path, payload)


@dataclass
class RecoveryState:
    item: PlannedFile
    current_dir: str
    current_name: str
    entry: Mapping[str, Any]


@dataclass(frozen=True)
class AutoMatch:
    media_type: str
    tmdb_id: int
    title: str
    year: str
    confidence: float
    status: str = "confirmed"
    score_components: dict[str, float] = field(default_factory=dict)
    decision_trace: dict[str, Any] = field(default_factory=dict)
