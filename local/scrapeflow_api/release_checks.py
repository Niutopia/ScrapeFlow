"""One local release-check command list for the converged backend."""

from __future__ import annotations

import ast
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


def _literal_string(node: ast.AST) -> str | None:
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
        left = _literal_string(node.left)
        right = _literal_string(node.right)
        if left is not None and right is not None:
            return left + right
    return None


def _is_banned_algorithm_name(value: object) -> bool:
    if not isinstance(value, str):
        return False
    normalized = value.strip().casefold().replace("-", "").replace("_", "")
    return normalized == "sha" + "256"


class _MediaFingerprintVisitor(ast.NodeVisitor):
    def __init__(self, path: Path) -> None:
        self.path = path
        self.hashlib_aliases: set[str] = set()
        self.banned_call_aliases: set[str] = set()
        self.hashlib_new_aliases: set[str] = set()
        self.hits: list[str] = []

    def visit_Import(self, node: ast.Import) -> None:  # noqa: N802
        for alias in node.names:
            if alias.name == "hashlib":
                self.hashlib_aliases.add(alias.asname or alias.name)
        self.generic_visit(node)

    def visit_ImportFrom(self, node: ast.ImportFrom) -> None:  # noqa: N802
        if node.module == "hashlib":
            for alias in node.names:
                imported = alias.name
                local_name = alias.asname or alias.name
                if _is_banned_algorithm_name(imported):
                    self.banned_call_aliases.add(local_name)
                elif imported == "new":
                    self.hashlib_new_aliases.add(local_name)
        self.generic_visit(node)

    def visit_Call(self, node: ast.Call) -> None:  # noqa: N802
        if self._is_banned_call(node):
            self.hits.append(f"{self.path}:{node.lineno}")
        self.generic_visit(node)

    def _call_uses_banned_algorithm(self, node: ast.Call) -> bool:
        if node.args and _is_banned_algorithm_name(_literal_string(node.args[0])):
            return True
        for keyword in node.keywords:
            if keyword.arg == "name" and _is_banned_algorithm_name(_literal_string(keyword.value)):
                return True
        return False

    def _getattr_returns_banned_call(self, node: ast.Call) -> bool:
        if not isinstance(node.func, ast.Name) or node.func.id != "getattr":
            return False
        if len(node.args) < 2:
            return False
        base, attr = node.args[0], node.args[1]
        return (
            isinstance(base, ast.Name)
            and base.id in self.hashlib_aliases
            and _is_banned_algorithm_name(_literal_string(attr))
        )

    def _is_banned_call(self, node: ast.Call) -> bool:
        func = node.func
        if isinstance(func, ast.Attribute) and isinstance(func.value, ast.Name):
            if func.value.id in self.hashlib_aliases:
                if _is_banned_algorithm_name(func.attr):
                    return True
                if func.attr == "new" and self._call_uses_banned_algorithm(node):
                    return True
        if isinstance(func, ast.Name):
            if func.id in self.banned_call_aliases:
                return True
            if func.id in self.hashlib_new_aliases and self._call_uses_banned_algorithm(node):
                return True
        if isinstance(func, ast.Call) and self._getattr_returns_banned_call(func):
            return True
        return False


def active_media_fingerprint_call_hits(root: Path | None = None) -> list[str]:
    """Find application-code calls to the banned media-content primitive."""
    hits: list[str] = []
    for path in active_python_paths(root):
        try:
            text = path.read_text(encoding="utf-8")
        except (OSError, UnicodeDecodeError):
            continue
        try:
            tree = ast.parse(text, filename=str(path))
        except SyntaxError as exc:
            hits.append(f"{path}:{exc.lineno or 1}: syntax error")
            continue
        visitor = _MediaFingerprintVisitor(path)
        visitor.visit(tree)
        hits.extend(visitor.hits)
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
