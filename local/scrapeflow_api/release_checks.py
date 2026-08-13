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
    # The sidecar resolves a fresh Quark Cookie/root from local AList storage
    # for every typed action.
    "ALIST_URL": "http://127.0.0.1:5244",
    "ALIST_USERNAME": "admin",
    "ALIST_PASSWORD": "replace-with-your-alist-password",
    "SCRAPEFLOW_START_PAUSED": "1",
    "SCRAPEFLOW_INTAKE_MONITOR": "0",
    "SCRAPEFLOW_AUTOMATIC_AUDIT": "0",
    "SCRAPEFLOW_AUDIT_AUTO_REPAIR_ENABLED": "0",
    "SCRAPEFLOW_PROVIDER_AUTO_REPAIR_ENABLED": "0",
    "SCRAPEFLOW_PROVIDER_WORKERS": "1",
    # The typed sidecar shares the API network namespace and listens only on
    # that namespace's loopback.  The blank token, rather than an invented
    # credential, keeps Compose fail-closed without `.env.local`.
    "SCRAPEFLOW_QUARK_HELPER_URL": "http://127.0.0.1:18765",
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
    # AList credentials are intentionally not interpolated into the API
    # service's internal network URL; the helper block below checks their
    # dedicated sidecar wiring separately.
    **{
        key: value
        for key, value in REQUIRED_ENV_TEMPLATE_VALUES.items()
        if not key.startswith("ALIST_")
    },
    "SCRAPEFLOW_REPLENISHMENT_ANIMETOSHO_SEARCH": "0",
    "SCRAPEFLOW_REPLENISHMENT_TOKYOTOSHO_SEARCH": "0",
    "SCRAPEFLOW_REPLENISHMENT_SUBSPLEASE_SEARCH": "0",
    "SCRAPEFLOW_REPLENISHMENT_MIKAN_SEARCH": "0",
    "SCRAPEFLOW_REPLENISHMENT_DMHY_SEARCH": "0",
    "SCRAPEFLOW_REPLENISHMENT_NYAA_SEARCH": "0",
    "SCRAPEFLOW_REPLENISHMENT_ACG_SEARCH": "0",
    "SCRAPEFLOW_QUARK_HELPER_URL": "http://127.0.0.1:18765",
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
    "alist": ["127.0.0.1:${SCRAPEFLOW_ALIST_PORT:-5244}:5244"],
    "api": ["127.0.0.1:${SCRAPEFLOW_API_PORT:-8765}:8765"],
    # The Helper is reachable only through the API network namespace.
    "quark-helper": [],
}
REQUIRED_SERVICE_IMAGES = {
    "api": "${SCRAPEFLOW_API_IMAGE:-scrapeflow-api:local}",
    "quark-helper": "${SCRAPEFLOW_API_IMAGE:-scrapeflow-api:local}",
}
REQUIRED_VOLUME_BINDINGS = {
    "alist": [
        "${SCRAPEFLOW_HOST_STATE_ROOT:?set SCRAPEFLOW_HOST_STATE_ROOT}/alist-data:/opt/alist/data",
        "${SCRAPEFLOW_HOST_STATE_ROOT:?set SCRAPEFLOW_HOST_STATE_ROOT}/alist-temp:/opt/alist/data/temp",
    ],
    "api": [
        "${SCRAPEFLOW_HOST_STATE_ROOT:?set SCRAPEFLOW_HOST_STATE_ROOT}/scrapeflow-data:/data",
        "${SCRAPEFLOW_HOST_STATE_ROOT:?set SCRAPEFLOW_HOST_STATE_ROOT}/api-temp:/var/tmp/scrapeflow",
    ],
}
REQUIRED_HELPER_ENVIRONMENT = {
    "ALIST_URL": "http://alist:5244",
    "ALIST_USERNAME": "${ALIST_USERNAME:-}",
    "ALIST_PASSWORD": "${ALIST_PASSWORD:-}",
    "SCRAPEFLOW_MEDIA_ROOT": "${SCRAPEFLOW_MEDIA_ROOT:-/quark/影视}",
    "NO_PROXY": "alist,localhost,127.0.0.1${NO_PROXY:+,}${NO_PROXY:-}",
    "SCRAPEFLOW_QUARK_HELPER_TOKEN": "${SCRAPEFLOW_QUARK_HELPER_TOKEN:-}",
    "SCRAPEFLOW_QUARK_HELPER_CDP_URL": (
        "http://host.docker.internal:19222/json/list"
    ),
}
REQUIRED_HELPER_COMMAND = (
    '["python3", "scripts/scrapeflow_quark_helper.py", "--docker-sidecar"]'
)
FORBIDDEN_HELPER_DEPLOYMENT_MARKERS = (
    "scrapeflow_quark_helper.py --install-launch-agent",
    "com.scrapeflow.quark-native-helper",
)


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


def _service_scalar(block: list[str], key: str) -> str | None:
    """Return one scalar declared directly below a Compose service."""

    marker = f"{key}:"
    for line in block:
        if len(line) - len(line.lstrip()) != 4:
            continue
        stripped = line.strip()
        if stripped == marker:
            return ""
        if stripped.startswith(marker):
            return _strip_scalar(stripped[len(marker):])
    return None


def _dependency_conditions(block: list[str]) -> dict[str, str]:
    """Return long-form Compose dependency conditions for one service."""

    output: dict[str, str] = {}
    start: int | None = None
    for index, line in enumerate(block):
        if len(line) - len(line.lstrip()) == 4 and line.strip() == "depends_on:":
            start = index + 1
            break
    if start is None:
        return output
    dependency: str | None = None
    for line in block[start:]:
        if not line.strip():
            continue
        indent = len(line) - len(line.lstrip())
        if indent <= 4:
            break
        stripped = line.strip()
        if indent == 6 and stripped.endswith(":"):
            dependency = stripped[:-1]
            output[dependency] = ""
        elif indent == 8 and dependency is not None and stripped.startswith("condition:"):
            output[dependency] = _strip_scalar(stripped.split(":", 1)[1])
    return output


def _dependency_restart_flags(block: list[str]) -> dict[str, str]:
    """Return explicit Compose restart coupling for service dependencies."""

    output: dict[str, str] = {}
    start: int | None = None
    for index, line in enumerate(block):
        if len(line) - len(line.lstrip()) == 4 and line.strip() == "depends_on:":
            start = index + 1
            break
    if start is None:
        return output
    dependency: str | None = None
    for line in block[start:]:
        if not line.strip():
            continue
        indent = len(line) - len(line.lstrip())
        if indent <= 4:
            break
        stripped = line.strip()
        if indent == 6 and stripped.endswith(":"):
            dependency = stripped[:-1]
        elif (
            indent == 8
            and dependency is not None
            and stripped.startswith("restart:")
        ):
            output[dependency] = _strip_scalar(stripped.split(":", 1)[1])
    return output


def local_deployment_contract_issues(root: Path | None = None) -> list[str]:
    """Return static release/deployment default drift from the frozen contract."""
    base = project_root() if root is None else Path(root)
    issues: list[str] = []
    env_text = _read_project_text(base, ".env.local.example", issues)
    compose_text = _read_project_text(base, "docker-compose.yml", issues)
    dockerfile_text = _read_project_text(base, "Dockerfile.api", issues)
    helper_requirements = _read_project_text(
        base, "requirements.quark-helper.txt", issues,
    )
    readme_text = _read_project_text(base, "README.md", issues)
    deployment_text = _read_project_text(
        base, "docs/scrapeflow-deployment-open-order.md", issues,
    )
    helper_cli_text = _read_project_text(
        base, "scripts/scrapeflow_quark_helper.py", issues,
    )
    lifecycle_cli_text = _read_project_text(
        base, "scripts/scrapeflow_quark_lifecycle.py", issues,
    )

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
    for service, expected in REQUIRED_SERVICE_IMAGES.items():
        block = _service_block(compose_text, service)
        if not block:
            issues.append(f"docker-compose.yml: missing {service} service")
            continue
        actual = _service_scalar(block, "image")
        if actual != expected:
            issues.append(
                f"docker-compose.yml {service}.image must be {expected!r}, got {actual!r}"
            )

    for service, expected_volumes in REQUIRED_VOLUME_BINDINGS.items():
        block = _service_block(compose_text, service)
        if not block:
            issues.append(f"docker-compose.yml: missing {service} service")
            continue
        volumes = _list_block_values(block, "volumes")
        if volumes != expected_volumes:
            issues.append(
                f"docker-compose.yml {service}.volumes must be {expected_volumes!r}, got {volumes!r}"
            )

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

    helper_block = _service_block(compose_text, "quark-helper")
    if helper_block:
        helper_scalars = {
            "image": "${SCRAPEFLOW_API_IMAGE:-scrapeflow-api:local}",
            "restart": "unless-stopped",
            "network_mode": "service:api",
            "command": REQUIRED_HELPER_COMMAND,
        }
        for key, expected in helper_scalars.items():
            actual = _service_scalar(helper_block, key)
            if actual != expected:
                issues.append(
                    f"docker-compose.yml quark-helper.{key} must be "
                    f"{expected!r}, got {actual!r}"
                )
        helper_env = _mapping_block_values(helper_block, "environment")
        for key, expected in REQUIRED_HELPER_ENVIRONMENT.items():
            actual = helper_env.get(key)
            if actual != expected:
                issues.append(
                    f"docker-compose.yml quark-helper.environment: {key} must be "
                    f"{expected!r}, got {actual!r}"
                )
        dependencies = _dependency_conditions(helper_block)
        if dependencies != {"api": "service_started"}:
            issues.append(
                "docker-compose.yml quark-helper.depends_on must be "
                "{'api': 'service_started'}"
            )
        restart_flags = _dependency_restart_flags(helper_block)
        if restart_flags != {"api": "true"}:
            issues.append(
                "docker-compose.yml quark-helper.depends_on.api.restart must be true"
            )
        extra_hosts = _list_block_values(helper_block, "extra_hosts")
        if extra_hosts:
            issues.append(
                "docker-compose.yml quark-helper must not declare extra_hosts "
                "while sharing the API network namespace"
            )

    if "quark-helper" in _dependency_conditions(api_block):
        issues.append(
            "docker-compose.yml api must not wait for Quark Helper readiness"
        )

    required_image_snippets = (
        "COPY requirements.quark-helper.txt ./requirements.quark-helper.txt",
        "python3 -m pip install --requirement requirements.quark-helper.txt",
        (
            "COPY scripts/scrapeflow_quark_helper.py "
            "./scripts/scrapeflow_quark_helper.py"
        ),
    )
    for snippet in required_image_snippets:
        if snippet not in dockerfile_text:
            issues.append(f"Dockerfile.api must include {snippet!r}")
    allowed_scripts_copy = (
        "COPY scripts/scrapeflow_quark_helper.py "
        "./scripts/scrapeflow_quark_helper.py"
    )
    for line in dockerfile_text.splitlines():
        stripped = line.strip()
        if stripped.startswith("COPY scripts") and stripped != allowed_scripts_copy:
            issues.append(
                "Dockerfile.api must copy only the passive Quark Helper CLI, "
                f"got {stripped!r}"
            )
    if "scrapeflow_quark_lifecycle.py" in dockerfile_text:
        issues.append("Dockerfile.api must not contain the macOS Quark lifecycle CLI")
    if not any(
        line.strip().startswith("aiohttp")
        for line in helper_requirements.splitlines()
    ):
        issues.append("requirements.quark-helper.txt must install aiohttp")

    lifecycle_markers = (
        'LAUNCH_AGENT_LABEL = "com.scrapeflow.quark-cdp"',
        '"--remote-debugging-address=127.0.0.1"',
        '"--remote-debugging-port=19222"',
        '"LimitLoadToSessionType": "Aqua"',
        '"ProcessType": "Interactive"',
        '"KeepAlive": {"SuccessfulExit": False}',
        'actions.add_argument("--start"',
        'actions.add_argument("--restart"',
        '"--force-restart"',
    )
    for marker in lifecycle_markers:
        if marker not in lifecycle_cli_text:
            issues.append(
                "scripts/scrapeflow_quark_lifecycle.py must retain "
                f"{marker!r}"
            )
    if "forceTerminate" in lifecycle_cli_text:
        issues.append(
            "scripts/scrapeflow_quark_lifecycle.py must not force-terminate "
            "Quark from its normal lifecycle path"
        )
    for marker in ("launchctl", "osascript", "QuarkCloudDrive"):
        if marker in helper_cli_text:
            issues.append(
                "scripts/scrapeflow_quark_helper.py must remain passive and "
                f"must not contain {marker!r}"
            )
    lifecycle_command = (
        "python3 scripts/scrapeflow_quark_lifecycle.py "
        "--install-launch-agent --replace-running"
    )
    for relative, text in (
        ("README.md", readme_text),
        ("docs/scrapeflow-deployment-open-order.md", deployment_text),
    ):
        if lifecycle_command not in text:
            issues.append(
                f"{relative} must document the fixed Quark lifecycle install command"
            )

    for command in FORBIDDEN_HELPER_DEPLOYMENT_MARKERS:
        for relative, text in (
            ("docker-compose.yml", compose_text),
            ("README.md", readme_text),
            ("docs/scrapeflow-deployment-open-order.md", deployment_text),
        ):
            if command in text:
                issues.append(
                    f"{relative} must not retain host Helper lifecycle {command!r}"
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
        if command.name in {"git-worktree-clean", "docker-compose-config"}:
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
            if command.name == "docker-compose-config":
                # ``docker compose config`` resolves every interpolation,
                # including operator credentials.  The surrounding release
                # evidence runner captures this program's stdout/stderr, so
                # never relay Compose's diagnostic or resolved configuration
                # into the durable raw evidence log.
                print(
                    "docker compose config failed; resolved configuration output withheld",
                    file=output,
                )
            return completed.returncode
        if command.name == "docker-compose-config":
            print(
                "docker compose config passed; resolved configuration output withheld",
                file=output,
            )
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
