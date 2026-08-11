"""Tests for the local acceptance-package draft generator."""

from __future__ import annotations

from contextlib import redirect_stderr, redirect_stdout
from datetime import UTC, datetime
from io import StringIO
import json
import tempfile
import unittest
from pathlib import Path

from engine.scrapeflow.serialization import atomic_write_json
from engine.scrapeflow.provider_capabilities import QUARK_HELPER_REQUIRED_ACTIONS
from local.scrapeflow_api.acceptance_package import (
    CommandResult,
    build_acceptance_package,
    compose_evidence,
    git_evidence,
    isolated_preflight_evidence,
    release_evidence_summary,
    runtime_readiness_evidence,
)
from local.scrapeflow_api.offline_backup import MANIFEST_NAME, create_offline_backup
from local.scrapeflow_api.isolated_preflight import capture_isolated_preflight_report
from scripts.scrapeflow_acceptance_package import main as acceptance_package_main


ENV_TEMPLATE_DEFAULTS = {
    "SCRAPEFLOW_START_PAUSED": "1",
    "SCRAPEFLOW_INTAKE_MONITOR": "0",
    "SCRAPEFLOW_AUTOMATIC_AUDIT": "0",
    "SCRAPEFLOW_AUDIT_AUTO_REPAIR_ENABLED": "0",
    "SCRAPEFLOW_PROVIDER_AUTO_REPAIR_ENABLED": "0",
    "SCRAPEFLOW_PROVIDER_WORKERS": "1",
    "SCRAPEFLOW_QUARK_HELPER_URL": "http://host.docker.internal:18765",
    "SCRAPEFLOW_QUARK_HELPER_TOKEN": (
        "replace-with-a-random-helper-token-at-least-24-characters"
    ),
    "SCRAPEFLOW_PANSOU_ENABLED": "0",
    "SCRAPEFLOW_PANSOU_URL": "",
    "SCRAPEFLOW_PANSOU_TOKEN": "",
    "SCRAPEFLOW_PANSOU_TIMEOUT": "12",
    "SCRAPEFLOW_PANSOU_MAX_QUERIES": "4",
    "SCRAPEFLOW_PANSOU_MAX_LINKS": "64",
    "SCRAPEFLOW_PROVIDER_PILOT_TMDB": "",
    "SCRAPEFLOW_PROVIDER_PILOT_GAP": "",
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


def valid_isolated_declaration(root: Path) -> dict[str, object]:
    state_dir = root / "isolated" / "scrapeflow-data"
    alist_dir = root / "isolated" / "alist-data"
    state_dir.mkdir(parents=True)
    alist_dir.mkdir(parents=True)
    source_alist = root / "backup-source" / "alist-data"
    source_scrapeflow = root / "backup-source" / "scrapeflow-data"
    source_alist.mkdir(parents=True)
    source_scrapeflow.mkdir(parents=True)
    atomic_write_json(source_alist / "config.json", {"version": 1}, allow_nan=False)
    atomic_write_json(
        source_scrapeflow / "global-control.json",
        {
            "version": 1,
            "paused": True,
            "scheduler_paused": True,
            "persistent": True,
            "updated_at": "2026-08-10T00:00:00Z",
            "reason": "test pause",
        },
        allow_nan=False,
    )
    backup_output = root / "backup"
    backup_output.mkdir()
    create_offline_backup(
        alist_data=source_alist,
        scrapeflow_data=source_scrapeflow,
        output_dir=backup_output,
        media_snapshot_note="test media recovery point",
        label="backup-one",
    )
    return {
        "api_url": "http://127.0.0.1:8765",
        "alist_url": "http://127.0.0.1:5244",
        "scrapeflow_state_dir": str(state_dir),
        "alist_data_dir": str(alist_dir),
        "media_root": "/quark/影视/ScrapeFlow/验收/run-20260810",
        "storage_label": "isolated-quark-storage-20260810",
        "offline_backup_manifest": str(backup_output / "backup-one" / MANIFEST_NAME),
        "media_recovery_point": "snapshot:isolated-media-before-acceptance",
        "provider_workers": 1,
        "start_paused": True,
        "intake_monitor": False,
        "automatic_audit": False,
        "audit_repair": False,
        "provider_auto_repair": False,
        "one_task_at_a_time": True,
        "old_backlog_restored": False,
        "bulk_retry": False,
        "bulk_cleanup": False,
    }


def valid_runtime_readiness_report() -> dict[str, object]:
    return {
        "status": "通过",
        "api_url": "http://127.0.0.1:8765",
        "expected_commit": "abc1234",
        "allow_existing_jobs": False,
        "issues": [],
        "health": {
            "build_commit": "abc1234",
            "connected": True,
            "tmdb_configured": True,
            "engine_configured": True,
            "provider_capabilities": {
                "quark_share": {"status": "ready"},
                "quark_magnet": {"status": "ready"},
                "magnet": {"status": "ready"},
            },
            "helper_readiness": {
                "quark": {
                    "configured": True,
                    "reachable": True,
                    "authenticated": True,
                    "status": "ready",
                    "required_actions": list(QUARK_HELPER_REQUIRED_ACTIONS),
                    "actions": list(QUARK_HELPER_REQUIRED_ACTIONS),
                },
            },
            "lane_gates": {
                "provider_auto_repair_enabled": False,
                "audit_auto_repair_enabled": False,
            },
            "intake": {"enabled": False},
            "operations": {
                "jobs_total": 0,
                "jobs_active": 0,
                "formal_write_workers": 0,
                "provider_workers": 0,
                "provider_active": 0,
                "audit_running": False,
            },
        },
        "control": {
            "paused": True,
            "scheduler_paused": True,
            "persistent": True,
        },
    }


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
    def test_release_evidence_requires_full_gate_and_bound_raw_log(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            evidence_path = root / "scrapeflow-release-evidence.json"
            log_path = root / "scrapeflow-release-check.log"
            log_path.write_text("$ python3 -m unittest\nOK\n", encoding="utf-8")
            report = {
                "status": "通过",
                "command": ["python3", "scripts/scrapeflow_release_check.py"],
                "returncode": 0,
                "include_docker": True,
                "started_at": "2026-08-10T01:00:00+00:00",
                "finished_at": "2026-08-10T01:01:00+00:00",
                "log_path": str(log_path),
                "report_path": str(evidence_path),
            }
            evidence_path.write_text(json.dumps(report), encoding="utf-8")

            summary = release_evidence_summary(report, evidence_path=evidence_path)

        self.assertEqual(summary["status"], "通过")
        self.assertEqual(summary["issues"], [])

    def test_release_evidence_rejects_skipped_or_unbound_success_claim(self) -> None:
        report = {
            "returncode": 0,
            "command": ["python3", "scripts/scrapeflow_release_check.py", "--skip-docker"],
            "include_docker": False,
            "started_at": "2026-08-10T01:01:00+00:00",
            "finished_at": "2026-08-10T01:00:00+00:00",
            "log_path": "",
            "report_path": "",
        }

        summary = release_evidence_summary(report)

        self.assertEqual(summary["status"], "无效")
        self.assertIn("command must run the full release gate without --skip-docker", summary["issues"])
        self.assertIn("include_docker must be true", summary["issues"])
        self.assertIn("finished_at must not precede started_at", summary["issues"])

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
        self.assertIn("- 隔离 preflight: 未提供", package)
        self.assertIn("- Runtime readiness: 未提供", package)
        self.assertIn("- [ ] 隔离 preflight 通过", package)
        self.assertIn("- [ ] Runtime readiness 通过", package)
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

    def test_preflight_evidence_reports_valid_declaration(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            evidence = isolated_preflight_evidence(valid_isolated_declaration(root), root=root / "repo")

        self.assertEqual(evidence["status"], "通过")
        self.assertEqual(evidence["issues"], [])
        self.assertEqual(evidence["summary"]["provider_workers"], 1)

    def test_package_draft_includes_preflight_declaration(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            repo = root / "repo"
            repo.mkdir()
            write_contract_files(repo)

            package = build_acceptance_package(
                root=repo,
                generated_at=datetime(2026, 8, 10, 6, 0, tzinfo=UTC),
                release_check_status="通过",
                isolated_declaration=valid_isolated_declaration(root),
                runner=fake_runner,
            )

        self.assertIn("- 隔离 preflight: 通过", package)
        self.assertIn("- 离线备份 manifest: ", package)
        self.assertIn("scrapeflow-offline-backup.json", package)
        self.assertIn("- 正式媒体库外部恢复点: snapshot:isolated-media-before-acceptance", package)
        self.assertIn("## 隔离环境声明", package)
        self.assertIn("| media_root | /quark/影视/ScrapeFlow/验收/run-20260810 |", package)
        self.assertIn("当前记录: 通过", package)

    def test_package_draft_records_failed_preflight_without_passing_samples(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            repo = root / "repo"
            repo.mkdir()
            write_contract_files(repo)
            declaration = valid_isolated_declaration(root)
            declaration["provider_workers"] = 2
            declaration["media_root"] = "/quark/影视/电影"

            package = build_acceptance_package(
                root=repo,
                generated_at=datetime(2026, 8, 10, 6, 0, tzinfo=UTC),
                isolated_declaration=declaration,
                runner=fake_runner,
            )

        self.assertIn("- 隔离 preflight: 失败", package)
        self.assertIn("preflight 问题:", package)
        self.assertIn("provider_workers must be 1", package)
        self.assertIn("formal library shelf", package)
        self.assertIn("| 电影 |  | 选择 movie 后入库，回读正确 | 未执行 |  |", package)

    def test_package_uses_captured_pass_after_runtime_directories_fill(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            repo = root / "repo"
            repo.mkdir()
            write_contract_files(repo)
            declaration = valid_isolated_declaration(root)
            report = capture_isolated_preflight_report(
                declaration,
                root=repo,
                checked_at=datetime(2026, 8, 11, 2, 3, tzinfo=UTC),
            )
            Path(str(declaration["scrapeflow_state_dir"])).joinpath(
                "runtime-state.json"
            ).write_text("{}", encoding="utf-8")
            Path(str(declaration["alist_data_dir"])).joinpath(
                "data.db"
            ).write_bytes(b"runtime")

            evidence = isolated_preflight_evidence(
                declaration,
                report=report,
                root=repo,
            )
            package = build_acceptance_package(
                root=repo,
                generated_at=datetime(2026, 8, 11, 3, 0, tzinfo=UTC),
                isolated_declaration=declaration,
                isolated_preflight_report=report,
                runner=fake_runner,
            )

        self.assertEqual(evidence["status"], "通过")
        self.assertEqual(evidence["mode"], "captured_report")
        self.assertEqual(evidence["issues"], [])
        self.assertIn("- 隔离 preflight: 通过", package)
        self.assertIn("- Preflight 证据模式: captured_report", package)
        self.assertIn("- Preflight 固化时间: 2026-08-11T02:03:00+00:00", package)
        self.assertIn("| audit_repair | False |", package)

    def test_package_rejects_failed_malformed_and_mismatched_reports(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            repo = root / "repo"
            repo.mkdir()
            declaration = valid_isolated_declaration(root)
            passed = capture_isolated_preflight_report(declaration, root=repo)

            failed_declaration = dict(declaration)
            failed_declaration["provider_workers"] = 2
            failed = capture_isolated_preflight_report(failed_declaration, root=repo)
            malformed = dict(passed)
            malformed["status"] = "trusted"
            mismatched = dict(declaration)
            mismatched["storage_label"] = "different-storage"

            failed_evidence = isolated_preflight_evidence(
                None,
                report=failed,
                root=repo,
            )
            malformed_evidence = isolated_preflight_evidence(
                None,
                report=malformed,
                root=repo,
            )
            mismatch_evidence = isolated_preflight_evidence(
                mismatched,
                report=passed,
                root=repo,
            )

        for evidence in (failed_evidence, malformed_evidence, mismatch_evidence):
            self.assertEqual(evidence["status"], "拒绝")
            self.assertTrue(evidence["issues"])

    def test_cli_reads_captured_report_and_rejects_declaration_drift(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            declaration = valid_isolated_declaration(root)
            report = capture_isolated_preflight_report(declaration)
            report_path = root / "preflight-report.json"
            failed_report_path = root / "failed-preflight-report.json"
            malformed_report_path = root / "malformed-preflight-report.json"
            declaration_path = root / "declaration.json"
            drifted_path = root / "drifted-declaration.json"
            output_path = root / "acceptance.md"
            report_path.write_text(json.dumps(report), encoding="utf-8")
            failed_declaration = dict(declaration)
            failed_declaration["provider_workers"] = 2
            failed_report_path.write_text(
                json.dumps(capture_isolated_preflight_report(failed_declaration)),
                encoding="utf-8",
            )
            malformed_report = dict(report)
            malformed_report["status"] = "trusted"
            malformed_report_path.write_text(
                json.dumps(malformed_report),
                encoding="utf-8",
            )
            declaration_path.write_text(json.dumps(declaration), encoding="utf-8")
            drifted = dict(declaration)
            drifted["storage_label"] = "different-storage"
            drifted_path.write_text(json.dumps(drifted), encoding="utf-8")
            Path(str(declaration["scrapeflow_state_dir"])).joinpath(
                "runtime-state.json"
            ).write_text("{}", encoding="utf-8")
            Path(str(declaration["alist_data_dir"])).joinpath(
                "data.db"
            ).write_bytes(b"runtime")

            with redirect_stdout(StringIO()), redirect_stderr(StringIO()):
                accepted = acceptance_package_main([
                    "--preflight-report",
                    str(report_path),
                    "--preflight-declaration",
                    str(declaration_path),
                    "--output",
                    str(output_path),
                ])
            output = output_path.read_text(encoding="utf-8")
            with redirect_stdout(StringIO()), redirect_stderr(StringIO()):
                rejected = acceptance_package_main([
                    "--preflight-report",
                    str(report_path),
                    "--preflight-declaration",
                    str(drifted_path),
                    "--output",
                    str(root / "rejected.md"),
                ])
            rejected_reports: list[int] = []
            for invalid_path in (failed_report_path, malformed_report_path):
                with redirect_stdout(StringIO()), redirect_stderr(StringIO()):
                    rejected_reports.append(acceptance_package_main([
                        "--preflight-report",
                        str(invalid_path),
                        "--output",
                        str(root / f"{invalid_path.stem}.md"),
                    ]))

        self.assertEqual(accepted, 0)
        self.assertIn("- 隔离 preflight: 通过", output)
        self.assertIn("- Preflight 证据模式: captured_report", output)
        self.assertIn(f"- Preflight 报告路径: {report_path.resolve()}", output)
        self.assertRegex(output, r"- Preflight 报告 SHA-512: [0-9a-f]{128}")
        self.assertEqual(rejected, 2)
        self.assertEqual(rejected_reports, [2, 2])

    def test_cli_refuses_output_collision_and_captured_recovery_drift(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            declaration = valid_isolated_declaration(root)
            report = capture_isolated_preflight_report(declaration)
            report_path = root / "preflight-report.json"
            report_path.write_text(json.dumps(report), encoding="utf-8")
            manifest = Path(str(declaration["offline_backup_manifest"]))
            report_before = report_path.read_bytes()
            manifest_before = manifest.read_bytes()

            with redirect_stdout(StringIO()), redirect_stderr(StringIO()):
                report_collision = acceptance_package_main([
                    "--preflight-report", str(report_path),
                    "--output", str(report_path),
                ])
                manifest_collision = acceptance_package_main([
                    "--preflight-report", str(report_path),
                    "--output", str(manifest),
                ])
                backup_drift = acceptance_package_main([
                    "--preflight-report", str(report_path),
                    "--backup-manifest", str(root / "different-manifest.json"),
                    "--output", str(root / "drift.md"),
                ])
                media_drift = acceptance_package_main([
                    "--preflight-report", str(report_path),
                    "--media-recovery-point", "different:recovery",
                    "--output", str(root / "media-drift.md"),
                ])
            report_after = report_path.read_bytes()
            manifest_after = manifest.read_bytes()

        self.assertEqual(
            (report_collision, manifest_collision, backup_drift, media_drift),
            (2, 2, 2, 2),
        )
        self.assertEqual(report_after, report_before)
        self.assertEqual(manifest_after, manifest_before)

    def test_runtime_readiness_evidence_reports_valid_summary(self) -> None:
        evidence = runtime_readiness_evidence(valid_runtime_readiness_report())

        self.assertEqual(evidence["status"], "通过")
        self.assertEqual(evidence["issues"], [])
        self.assertEqual(evidence["summary"]["api_url"], "http://127.0.0.1:8765")
        self.assertEqual(evidence["summary"]["control_paused"], True)
        self.assertEqual(
            evidence["summary"]["provider_lanes"],
            "magnet, quark_magnet, quark_share",
        )
        self.assertEqual(evidence["summary"]["quark_helper_status"], "ready")
        self.assertEqual(
            evidence["summary"]["quark_helper_actions"],
            "health, share-save, magnet-submit, magnet-status",
        )

    def test_package_draft_includes_runtime_readiness_report(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            write_contract_files(root)

            package = build_acceptance_package(
                root=root,
                generated_at=datetime(2026, 8, 10, 6, 0, tzinfo=UTC),
                runtime_readiness=valid_runtime_readiness_report(),
                runner=fake_runner,
            )

        self.assertIn("- Runtime readiness: 通过", package)
        self.assertIn("## Runtime Readiness", package)
        self.assertIn("| api_url | http://127.0.0.1:8765 |", package)
        self.assertIn("| build_commit | abc1234 |", package)
        self.assertIn("| control_paused | True |", package)
        self.assertIn("| quark_helper_status | ready |", package)
        self.assertIn("当前记录: 通过", package)

    def test_package_draft_records_failed_runtime_readiness_without_passing_samples(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            write_contract_files(root)
            readiness = valid_runtime_readiness_report()
            readiness["status"] = "失败"
            readiness["issues"] = ["operations.jobs_total must be 0 before opening acceptance"]

            package = build_acceptance_package(
                root=root,
                generated_at=datetime(2026, 8, 10, 6, 0, tzinfo=UTC),
                runtime_readiness=readiness,
                runner=fake_runner,
            )

        self.assertIn("- Runtime readiness: 失败", package)
        self.assertIn("readiness 问题:", package)
        self.assertIn("operations.jobs_total must be 0 before opening acceptance", package)
        self.assertIn("| 电影 |  | 选择 movie 后入库，回读正确 | 未执行 |  |", package)


if __name__ == "__main__":
    unittest.main()
