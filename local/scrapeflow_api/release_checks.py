"""One local release-check command list for the converged backend."""

from __future__ import annotations

import ast
from dataclasses import dataclass
import os
from pathlib import Path
import subprocess
import sys
from typing import TextIO


REQUIRED_ENV_TEMPLATE_VALUES = {
    "SCRAPEFLOW_START_PAUSED": "1",
    "SCRAPEFLOW_INTAKE_MONITOR": "0",
    "SCRAPEFLOW_AUTOMATIC_AUDIT": "0",
    "SCRAPEFLOW_AUDIT_AUTO_REPAIR_ENABLED": "0",
    "SCRAPEFLOW_PROVIDER_AUTO_REPAIR_ENABLED": "0",
    "SCRAPEFLOW_PROVIDER_WORKERS": "1",
    # Keep the historical host-side endpoint in the copied template.  The
    # blank token, rather than an invented URL, keeps Compose fail-closed when
    # a user has not supplied `.env.local`.
    "SCRAPEFLOW_QUARK_HELPER_URL": "http://host.docker.internal:18765",
    "SCRAPEFLOW_QUARK_HELPER_TOKEN": (
        "replace-with-a-random-helper-token-at-least-24-characters"
    ),
    # A first-tier share source must be explicitly enabled and configured;
    # the copied template never reaches a random network endpoint by default.
    "SCRAPEFLOW_PANSOU_ENABLED": "0",
    "SCRAPEFLOW_PANSOU_URL": "",
    "SCRAPEFLOW_PANSOU_TOKEN": "",
    "SCRAPEFLOW_PANSOU_TIMEOUT": "12",
    "SCRAPEFLOW_PANSOU_MAX_QUERIES": "4",
    "SCRAPEFLOW_PANSOU_MAX_LINKS": "64",
    # A pilot has no implicit target.  Both selectors must nevertheless be
    # wired so a real acceptance run does not rely on undocumented host env.
    "SCRAPEFLOW_PROVIDER_PILOT_TMDB": "",
    "SCRAPEFLOW_PROVIDER_PILOT_GAP": "",
}
REQUIRED_COMPOSE_DEFAULTS = {
    **REQUIRED_ENV_TEMPLATE_VALUES,
    "SCRAPEFLOW_REPLENISHMENT_ANIMETOSHO_SEARCH": "0",
    "SCRAPEFLOW_REPLENISHMENT_TOKYOTOSHO_SEARCH": "0",
    "SCRAPEFLOW_REPLENISHMENT_SUBSPLEASE_SEARCH": "0",
    "SCRAPEFLOW_REPLENISHMENT_MIKAN_SEARCH": "0",
    "SCRAPEFLOW_REPLENISHMENT_DMHY_SEARCH": "0",
    "SCRAPEFLOW_REPLENISHMENT_NYAA_SEARCH": "0",
    "SCRAPEFLOW_REPLENISHMENT_ACG_SEARCH": "0",
    "SCRAPEFLOW_QUARK_HELPER_URL": "http://host.docker.internal:18765",
    "SCRAPEFLOW_QUARK_HELPER_TOKEN": "",
    "SCRAPEFLOW_PANSOU_ENABLED": "0",
    "SCRAPEFLOW_PANSOU_URL": "",
    "SCRAPEFLOW_PANSOU_TOKEN": "",
    "SCRAPEFLOW_PANSOU_TIMEOUT": "12",
    "SCRAPEFLOW_PANSOU_MAX_QUERIES": "4",
    "SCRAPEFLOW_PANSOU_MAX_LINKS": "64",
    "SCRAPEFLOW_PROVIDER_PILOT_TMDB": "",
    "SCRAPEFLOW_PROVIDER_PILOT_GAP": "",
}
REQUIRED_LOOPBACK_PORTS = {
    "alist": ["127.0.0.1:5244:5244"],
    "api": ["127.0.0.1:${SCRAPEFLOW_API_PORT:-8765}:8765"],
}


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


def _read_project_text(root: Path, relative: str, issues: list[str]) -> str:
    path = root / relative
    try:
        return path.read_text(encoding="utf-8")
    except OSError as exc:
        issues.append(f"{relative}: cannot read: {exc}")
        return ""


def _env_values(text: str) -> dict[str, str]:
    values: dict[str, str] = {}
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, value = stripped.split("=", 1)
        values[key.strip()] = value.strip()
    return values


def _service_block(compose_text: str, service: str) -> list[str]:
    marker = f"  {service}:"
    lines = compose_text.splitlines()
    start: int | None = None
    for index, line in enumerate(lines):
        if line.rstrip() == marker:
            start = index + 1
            break
    if start is None:
        return []
    output: list[str] = []
    for line in lines[start:]:
        if line.startswith("  ") and not line.startswith("    ") and line.strip().endswith(":"):
            break
        output.append(line)
    return output


def _strip_scalar(value: str) -> str:
    stripped = value.strip()
    if len(stripped) >= 2 and stripped[0] == stripped[-1] and stripped[0] in {"'", '"'}:
        return stripped[1:-1]
    return stripped


def _mapping_block_values(block: list[str], header: str) -> dict[str, str]:
    values: dict[str, str] = {}
    start: int | None = None
    header_indent = 0
    for index, line in enumerate(block):
        if line.strip() == f"{header}:":
            start = index + 1
            header_indent = len(line) - len(line.lstrip())
            break
    if start is None:
        return values
    for line in block[start:]:
        if not line.strip():
            continue
        indent = len(line) - len(line.lstrip())
        if indent <= header_indent:
            break
        stripped = line.strip()
        if stripped.startswith("#") or ":" not in stripped:
            continue
        key, value = stripped.split(":", 1)
        values[key.strip()] = _strip_scalar(value)
    return values


def _list_block_values(block: list[str], header: str) -> list[str]:
    values: list[str] = []
    start: int | None = None
    header_indent = 0
    for index, line in enumerate(block):
        if line.strip() == f"{header}:":
            start = index + 1
            header_indent = len(line) - len(line.lstrip())
            break
    if start is None:
        return values
    for line in block[start:]:
        if not line.strip():
            continue
        indent = len(line) - len(line.lstrip())
        if indent <= header_indent:
            break
        stripped = line.strip()
        if stripped.startswith("- "):
            values.append(_strip_scalar(stripped[2:]))
    return values


def local_deployment_contract_issues(root: Path | None = None) -> list[str]:
    """Return static release/deployment default drift from the frozen contract."""
    base = project_root() if root is None else Path(root)
    issues: list[str] = []
    env_text = _read_project_text(base, ".env.local.example", issues)
    compose_text = _read_project_text(base, "docker-compose.yml", issues)

    env_values = _env_values(env_text)
    for key, expected in REQUIRED_ENV_TEMPLATE_VALUES.items():
        actual = env_values.get(key)
        if actual != expected:
            issues.append(
                f".env.local.example: {key} must default to {expected!r}, got {actual!r}"
            )

    api_block = _service_block(compose_text, "api")
    if not api_block:
        issues.append("docker-compose.yml: missing api service")
    api_env = _mapping_block_values(api_block, "environment")
    for key, expected in REQUIRED_COMPOSE_DEFAULTS.items():
        actual = api_env.get(key)
        required = f"${{{key}:-{expected}}}"
        if actual != required:
            issues.append(
                f"docker-compose.yml api.environment: {key} must default to {required!r}, got {actual!r}"
            )

    for service, expected_ports in REQUIRED_LOOPBACK_PORTS.items():
        block = _service_block(compose_text, service)
        if not block:
            issues.append(f"docker-compose.yml: missing {service} service")
            continue
        ports = _list_block_values(block, "ports")
        if ports != expected_ports:
            issues.append(
                f"docker-compose.yml {service}.ports must be {expected_ports!r}, got {ports!r}"
            )
    return issues


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
        ReleaseCommand("git-worktree-clean", ("git", "status", "--porcelain")),
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
    deployment_issues = local_deployment_contract_issues(root)
    if deployment_issues:
        print("local deployment defaults violate the frozen contract:", file=output)
        for issue in deployment_issues:
            print(f"  {issue}", file=output)
        return 1
    hits = active_media_fingerprint_call_hits(root)
    if hits:
        print("active code contains banned media fingerprint calls:", file=output)
        for hit in hits:
            print(f"  {hit}", file=output)
        return 1
    for command in release_commands(include_docker=include_docker):
        print("$ " + " ".join(command.args), file=output)
        status_kwargs: dict[str, object] = {}
        if command.name == "git-worktree-clean":
            status_kwargs = {
                "stdout": subprocess.PIPE,
                "stderr": subprocess.STDOUT,
                "text": True,
            }
        completed = subprocess.run(
            command.args,
            cwd=root,
            env=command.subprocess_env(),
            check=False,
            **status_kwargs,
        )
        if completed.returncode != 0:
            return completed.returncode
        if command.name == "git-worktree-clean":
            status = str(completed.stdout or "").strip()
            if status:
                print("working tree is not clean:", file=output)
                print(status, file=output)
                return 1
    return 0


__all__ = [
    "ReleaseCommand",
    "active_media_fingerprint_call_hits",
    "active_python_paths",
    "local_deployment_contract_issues",
    "project_root",
    "release_commands",
    "run_release_checks",
]
