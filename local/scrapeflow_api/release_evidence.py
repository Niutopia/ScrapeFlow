"""Capture release-check output as a local acceptance artifact."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
import hashlib
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


def _pre_gate_git_binding(
    root: Path,
    runner: ReleaseEvidenceRunner,
) -> tuple[str, bool]:
    """Capture the exact Git object and cleanliness before opening the gate.

    This deliberately happens before either the wrapped gate or the artifact
    directory is created.  The latter matters for callers that choose an
    unignored in-repository output directory: the evidence operation itself
    must not turn an otherwise clean candidate into a dirty one.
    """
    commit_result = runner(("git", "rev-parse", "--verify", "HEAD"), root)
    status_result = runner(("git", "status", "--porcelain"), root)
    commit = commit_result.output.strip() if commit_result.returncode == 0 else ""
    worktree_clean = (
        status_result.returncode == 0 and not status_result.output.strip()
    )
    return commit, worktree_clean


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
    git_runner: ReleaseEvidenceRunner = _run_command,
    clock: Clock = _clock,
) -> dict[str, Any]:
    """Run the release gate once and write a JSON report plus raw log."""
    base = project_root() if root is None else Path(root)
    target = Path(output_dir).expanduser().resolve()
    log_path = target / "scrapeflow-release-check.log"
    report_path = target / "scrapeflow-release-evidence.json"

    command = release_evidence_command(include_docker=include_docker)
    started_at = clock()
    # Do not create the requested artifact directory until after the pre-gate
    # Git binding and the gate's own ``git status --porcelain`` check.  The
    # documented artifacts path is ignored, but this ordering also keeps a
    # caller's custom in-repository output path from making an otherwise clean
    # candidate look dirty.
    commit, worktree_clean_before_gate = _pre_gate_git_binding(base, git_runner)
    result = runner(command, base)
    finished_at = clock()

    target.mkdir(parents=True, exist_ok=True)
    log_path.write_text(result.output, encoding="utf-8")
    report: dict[str, Any] = {
        "status": "通过" if result.returncode == 0 else "失败",
        "command": list(command),
        "returncode": result.returncode,
        "include_docker": include_docker,
        # ``rev-parse --verify HEAD`` emits the complete object ID, rather
        # than the display-oriented short hash used in package headings.
        "commit": commit,
        "worktree_clean_before_gate": worktree_clean_before_gate,
        "started_at": started_at.isoformat(),
        "finished_at": finished_at.isoformat(),
        "log_path": str(log_path),
        # Bind the JSON record to the exact raw output without adding metadata
        # to that raw output or exposing any environment values it may contain.
        "raw_log_sha512": hashlib.sha512(result.output.encode("utf-8")).hexdigest(),
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
