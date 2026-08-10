"""One local release-check command list for the converged backend."""

from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path
import subprocess
import sys
from typing import TextIO


@dataclass(frozen=True, slots=True)
class ReleaseCommand:
    name: str
    args: tuple[str, ...]
    env: dict[str, str] | None = None
    isolated_env: bool = False

    def subprocess_env(self) -> dict[str, str] | None:
        if self.env is None:
            return None
        if self.isolated_env:
            return dict(self.env)
        return {**os.environ, **self.env}


def project_root() -> Path:
    return Path(__file__).resolve().parents[2]


def release_commands(*, include_docker: bool = True) -> list[ReleaseCommand]:
    """Return the exact local commands used for the backend release gate."""
    commands = [
        ReleaseCommand(
            "python-unittest",
            (
                "python3",
                "-m",
                "unittest",
                "discover",
                "-s",
                "local/tests",
                "-p",
                "test_*.py",
            ),
            env={
                "SCRAPEFLOW_IGNORE_LOCAL_ENV": "1",
                "PYTHONDONTWRITEBYTECODE": "1",
            },
        ),
        ReleaseCommand("git-diff-check", ("git", "diff", "--check")),
    ]
    if include_docker:
        commands.extend([
            ReleaseCommand(
                "docker-compose-config",
                ("docker", "compose", "config"),
                env={
                    "PATH": os.environ.get("PATH", ""),
                    "HOME": os.environ.get("HOME", ""),
                    "SCRAPEFLOW_HOST_STATE_ROOT": "/tmp/scrapeflow-state",
                },
                isolated_env=True,
            ),
            ReleaseCommand(
                "docker-build-api",
                ("docker", "build", "-f", "Dockerfile.api", "."),
            ),
        ])
    return commands


def active_python_paths(root: Path | None = None) -> list[Path]:
    """Return activity-code Python files, excluding tests and caches."""
    base = project_root() if root is None else Path(root)
    paths: list[Path] = []
    for directory in (base / "engine", base / "local"):
        if not directory.exists():
            continue
        for path in directory.rglob("*.py"):
            parts = set(path.relative_to(base).parts)
            if "__pycache__" in parts or "tests" in parts:
                continue
            paths.append(path)
    return sorted(paths)


def active_media_fingerprint_call_hits(root: Path | None = None) -> list[str]:
    """Find application-code calls to the banned media-content primitive."""
    needle = "hashlib." + "sha" + "256"
    hits: list[str] = []
    for path in active_python_paths(root):
        try:
            text = path.read_text(encoding="utf-8")
        except (OSError, UnicodeDecodeError):
            continue
        for number, line in enumerate(text.splitlines(), start=1):
            if needle in line:
                hits.append(f"{path}:{number}")
    return hits


def run_release_checks(
    *,
    include_docker: bool = True,
    stream: TextIO | None = None,
) -> int:
    """Run release commands serially and stop at the first failure."""
    output = stream or sys.stderr
    root = project_root()
    hits = active_media_fingerprint_call_hits(root)
    if hits:
        print("active code contains banned media fingerprint calls:", file=output)
        for hit in hits:
            print(f"  {hit}", file=output)
        return 1
    for command in release_commands(include_docker=include_docker):
        print("$ " + " ".join(command.args), file=output)
        completed = subprocess.run(
            command.args,
            cwd=root,
            env=command.subprocess_env(),
            check=False,
        )
        if completed.returncode != 0:
            return completed.returncode
    return 0


__all__ = [
    "ReleaseCommand",
    "active_media_fingerprint_call_hits",
    "active_python_paths",
    "project_root",
    "release_commands",
    "run_release_checks",
]
