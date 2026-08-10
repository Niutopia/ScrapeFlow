"""Tests for the local acceptance-package draft generator."""

from __future__ import annotations

from datetime import UTC, datetime
import json
import tempfile
import unittest
from pathlib import Path

from local.scrapeflow_api.acceptance_package import (
    CommandResult,
    build_acceptance_package,
    compose_evidence,
    git_evidence,
)


ENV_TEMPLATE_DEFAULTS = {
    "SCRAPEFLOW_START_PAUSED": "1",
    "SCRAPEFLOW_INTAKE_MONITOR": "0",
    "SCRAPEFLOW_AUTOMATIC_AUDIT": "0",
    "SCRAPEFLOW_AUDIT_AUTO_REPAIR_ENABLED": "0",
    "SCRAPEFLOW_PROVIDER_AUTO_REPAIR_ENABLED": "0",
    "SCRAPEFLOW_PROVIDER_WORKERS": "1",
}
COMPOSE_DEFAULTS = {
    **ENV_TEMPLATE_DEFAULTS,
    "SCRAPEFLOW_REPLENISHMENT_ANIMETOSHO_SEARCH": "0",
    "SCRAPEFLOW_REPLENISHMENT_TOKYOTOSHO_SEARCH": "0",
    "SCRAPEFLOW_REPLENISHMENT_SUBSPLEASE_SEARCH": "0",
    "SCRAPEFLOW_REPLENISHMENT_MIKAN_SEARCH": "0",
    "SCRAPEFLOW_REPLENISHMENT_DMHY_SEARCH": "0",
    "SCRAPEFLOW_REPLENISHMENT_NYAA_SEARCH": "0",
    "SCRAPEFLOW_REPLENISHMENT_ACG_SEARCH": "0",
}


def write_contract_files(root: Path, *, provider_gate: str = "0") -> None:
    (root / ".env.local.example").write_text(
        "\n".join(f"{key}={value}" for key, value in ENV_TEMPLATE_DEFAULTS.items()) + "\n",
        encoding="utf-8",
    )
    compose_values = dict(COMPOSE_DEFAULTS)
    compose_values["SCRAPEFLOW_PROVIDER_AUTO_REPAIR_ENABLED"] = provider_gate
    api_env = "\n".join(
        f"      {key}: ${{{key}:-{value}}}"
        for key, value in compose_values.items()
    )
    (root / "docker-compose.yml").write_text(
        "name: scrapeflow\n"
        "services:\n"
        "  alist:\n"
        "    ports:\n"
        '      - "127.0.0.1:5244:5244"\n'
        "  api:\n"
        "    environment:\n"
        f"{api_env}\n"
        "    ports:\n"
        '      - "127.0.0.1:${SCRAPEFLOW_API_PORT:-8765}:8765"\n',
        encoding="utf-8",
    )


def fake_runner(args: tuple[str, ...], cwd: Path, env: dict[str, str] | None) -> CommandResult:
    if args == ("git", "rev-parse", "--abbrev-ref", "HEAD"):
        return CommandResult(0, "codex/test\n")
    if args == ("git", "rev-parse", "--short", "HEAD"):
        return CommandResult(0, "abc1234\n")
    if args == ("git", "status", "--short"):
        return CommandResult(0, "")
    if args == ("docker", "compose", "config", "--format", "json"):
        payload = {
            "services": {
                "api": {
                    "image": "scrapeflow-api:local",
                    "environment": {
                        "SCRAPEFLOW_START_PAUSED": "1",
                        "SCRAPEFLOW_INTAKE_MONITOR": "0",
                        "SCRAPEFLOW_AUTOMATIC_AUDIT": "0",
                        "SCRAPEFLOW_PROVIDER_AUTO_REPAIR_ENABLED": "0",
                        "SCRAPEFLOW_PROVIDER_WORKERS": "1",
                    },
                    "ports": [{
                        "host_ip": "127.0.0.1",
                        "published": "8765",
                        "target": 8765,
                        "protocol": "tcp",
                    }],
                },
                "alist": {
                    "image": "xhofe/alist:test",
                    "ports": [{
                        "host_ip": "127.0.0.1",
                        "published": "5244",
                        "target": 5244,
                        "protocol": "tcp",
                    }],
                },
            },
        }
        return CommandResult(0, json.dumps(payload))
    return CommandResult(127, "", "unexpected command")


class AcceptancePackageTests(unittest.TestCase):
    def test_git_evidence_reports_clean_worktree(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            evidence = git_evidence(Path(temporary), fake_runner)

        self.assertEqual(evidence["branch"], "codex/test")
        self.assertEqual(evidence["commit"], "abc1234")
        self.assertTrue(evidence["worktree_clean"])

    def test_compose_evidence_summarizes_loopback_services(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            evidence = compose_evidence(Path(temporary), fake_runner)

        self.assertEqual(evidence["status"], "可读")
        api = next(service for service in evidence["services"] if service["name"] == "api")
        self.assertEqual(api["ports"], ["127.0.0.1:8765->8765/tcp"])
        self.assertEqual(api["start_paused"], "1")
        self.assertEqual(api["provider_workers"], "1")

    def test_package_draft_keeps_real_samples_unexecuted(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            write_contract_files(root)

            package = build_acceptance_package(
                root=root,
                generated_at=datetime(2026, 8, 10, 6, 0, tzinfo=UTC),
                release_check_status="通过",
                runner=fake_runner,
            )

        self.assertIn("状态: 未完成，等待真实隔离验收和用户授权。", package)
        self.assertIn("- Git commit: abc1234", package)
        self.assertIn("- 静态部署合同: 通过", package)
        self.assertIn("| 电影 |  | 选择 movie 后入库，回读正确 | 未执行 |  |", package)
        self.assertIn("| 有效夸克分享 |  | 第一阶完成，后二阶未调用 | 未执行 |  |", package)
        self.assertIn("- [ ] 三条获取线路全部真实可执行。", package)

    def test_package_draft_reports_static_deployment_drift(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            write_contract_files(root, provider_gate="1")

            package = build_acceptance_package(
                root=root,
                generated_at=datetime(2026, 8, 10, 6, 0, tzinfo=UTC),
                runner=fake_runner,
            )

        self.assertIn("- 静态部署合同: 失败", package)
        self.assertIn("SCRAPEFLOW_PROVIDER_AUTO_REPAIR_ENABLED", package)


if __name__ == "__main__":
    unittest.main()
