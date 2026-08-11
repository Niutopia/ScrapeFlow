"""Capture release-check output as a local acceptance artifact."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
import json
from pathlib import Path
import subprocess
from typing import Any

from .release_checks import project_root


@dataclass(frozen=True, slots=True)
class ReleaseEvidenceResult:
    returncode: int
    output: str


ReleaseEvidenceRunner = Callable[[tuple[str, ...], Path], ReleaseEvidenceResult]
Clock = Callable[[], datetime]


def _run_command(args: tuple[str, ...], cwd: Path) -> ReleaseEvidenceResult:
    completed = subprocess.run(
        args,
        cwd=cwd,
        check=False,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    return ReleaseEvidenceResult(completed.returncode, completed.stdout or "")


def _clock() -> datetime:
    return datetime.now(UTC)


def release_evidence_command(*, include_docker: bool = True) -> tuple[str, ...]:
    """Return the wrapped release-check command."""
    command = ["python3", "scripts/scrapeflow_release_check.py"]
    if not include_docker:
        command.append("--skip-docker")
    return tuple(command)


def capture_release_evidence(
    output_dir: Path,
    *,
    include_docker: bool = True,
    root: Path | None = None,
    runner: ReleaseEvidenceRunner = _run_command,
    clock: Clock = _clock,
) -> dict[str, Any]:
    """Run the release gate once and write a JSON report plus raw log."""
    base = project_root() if root is None else Path(root)
    target = Path(output_dir).expanduser().resolve()
    log_path = target / "scrapeflow-release-check.log"
    report_path = target / "scrapeflow-release-evidence.json"

    command = release_evidence_command(include_docker=include_docker)
    started_at = clock()
    # Do not create the requested artifact directory until after the gate has
    # checked ``git status --porcelain``.  The documented artifacts path is
    # ignored, but this ordering also keeps a caller's custom in-repository
    # output path from making an otherwise clean commit look dirty.
    result = runner(command, base)
    finished_at = clock()

    target.mkdir(parents=True, exist_ok=True)
    log_path.write_text(result.output, encoding="utf-8")
    report: dict[str, Any] = {
        "status": "通过" if result.returncode == 0 else "失败",
        "command": list(command),
        "returncode": result.returncode,
        "include_docker": include_docker,
        "started_at": started_at.isoformat(),
        "finished_at": finished_at.isoformat(),
        "log_path": str(log_path),
        "report_path": str(report_path),
    }
    report_path.write_text(
        json.dumps(report, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    return report


__all__ = [
    "ReleaseEvidenceResult",
    "capture_release_evidence",
    "release_evidence_command",
]
