"""Domain models for the automatic planning workflow."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


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


@dataclass
class PlannedProblem:
    """A source file that the automatic planner could not place."""

    source_path: str
    reason: str
    target_path: str | None = None


@dataclass(frozen=True)
class PlanNotice:
    """Machine-readable planning notice."""

    code: str
    severity: str
    message: str
    details: dict[str, Any] = field(default_factory=dict)


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
