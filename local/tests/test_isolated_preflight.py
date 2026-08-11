"""Tests for isolated acceptance declaration preflight checks."""

from __future__ import annotations

from contextlib import redirect_stderr, redirect_stdout
from datetime import UTC, datetime
from io import StringIO
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from engine.scrapeflow.serialization import atomic_write_json
from local.scrapeflow_api.isolated_preflight import (
    PREFLIGHT_REPORT_FAILED,
    PREFLIGHT_REPORT_PASSED,
    capture_isolated_preflight_report,
    isolated_preflight_issues,
    isolated_preflight_template,
    load_declaration,
    validate_isolated_preflight_report,
)
from local.scrapeflow_api.offline_backup import MANIFEST_NAME, create_offline_backup
from scripts.scrapeflow_isolated_preflight import main as isolated_preflight_main


def _verified_backup_manifest(root: Path) -> Path:
    source = root / "backup-source"
    alist_source = source / "alist-data"
    scrapeflow_source = source / "scrapeflow-data"
    alist_source.mkdir(parents=True)
    scrapeflow_source.mkdir(parents=True)
    atomic_write_json(alist_source / "config.json", {"version": 1}, allow_nan=False)
    atomic_write_json(
        scrapeflow_source / "global-control.json",
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
    output = root / "backup"
    output.mkdir()
    create_offline_backup(
        alist_data=alist_source,
        scrapeflow_data=scrapeflow_source,
        output_dir=output,
        media_snapshot_note="test media recovery point",
        label="backup-one",
    )
    return output / "backup-one" / MANIFEST_NAME


def valid_declaration(root: Path) -> dict[str, object]:
    state_dir = root / "isolated-runtime" / "scrapeflow-data"
    alist_dir = root / "isolated-runtime" / "alist-data"
    state_dir.mkdir(parents=True)
    alist_dir.mkdir(parents=True)
    payload = isolated_preflight_template()
    payload.update({
        "scrapeflow_state_dir": str(state_dir),
        "alist_data_dir": str(alist_dir),
        "media_root": "/quark/影视/ScrapeFlow/验收/run-20260810",
        "storage_label": "isolated-quark-storage-20260810",
        "offline_backup_manifest": str(_verified_backup_manifest(root)),
        "media_recovery_point": "snapshot:isolated-media-before-acceptance",
    })
    return payload


class IsolatedPreflightTests(unittest.TestCase):
    def test_valid_declaration_passes(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            declaration = valid_declaration(root)

            issues = isolated_preflight_issues(declaration, root=root / "repo")

        self.assertEqual(issues, [])

    def test_unmodified_template_is_not_ready(self) -> None:
        declaration = isolated_preflight_template()

        issues = isolated_preflight_issues(declaration, root=Path("/tmp/repo"))

        self.assertTrue(any("scrapeflow_state_dir" in issue for issue in issues))
        self.assertTrue(any("alist_data_dir" in issue for issue in issues))
        self.assertTrue(any("media_root" in issue for issue in issues))
        self.assertTrue(any("storage_label" in issue for issue in issues))
        self.assertTrue(any("offline_backup_manifest" in issue for issue in issues))
        self.assertTrue(any("media_recovery_point" in issue for issue in issues))

    def test_rejects_non_loopback_urls_and_runtime_inside_repo(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            repo = root / "repo"
            repo.mkdir()
            declaration = valid_declaration(root)
            declaration.update({
                "api_url": "http://192.0.2.10:8765",
                "alist_url": "https://example.invalid:5244",
                "scrapeflow_state_dir": str(repo / "state"),
            })

            issues = isolated_preflight_issues(declaration, root=repo)

        self.assertTrue(any("api_url" in issue for issue in issues))
        self.assertTrue(any("alist_url" in issue for issue in issues))
        self.assertTrue(any("scrapeflow_state_dir" in issue for issue in issues))

    def test_rejects_nested_state_paths_and_formal_media_root(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            declaration = valid_declaration(root)
            declaration.update({
                "scrapeflow_state_dir": str(root / "isolated"),
                "alist_data_dir": str(root / "isolated" / "alist-data"),
                "media_root": "/quark/影视/电影",
            })

            issues = isolated_preflight_issues(declaration, root=root / "repo")

        self.assertTrue(any("must not be nested" in issue for issue in issues))
        self.assertTrue(any("formal library shelf" in issue for issue in issues))

    def test_rejects_gate_worker_and_recovery_drift(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            declaration = valid_declaration(root)
            declaration.update({
                "provider_workers": 2,
                "start_paused": False,
                "provider_auto_repair": True,
                "one_task_at_a_time": False,
                "offline_backup_manifest": "",
                "media_recovery_point": "",
                "bulk_cleanup": True,
            })

            issues = isolated_preflight_issues(declaration, root=root / "repo")

        self.assertTrue(any("provider_workers" in issue for issue in issues))
        self.assertTrue(any("start_paused" in issue for issue in issues))
        self.assertTrue(any("provider auto repair" in issue for issue in issues))
        self.assertTrue(any("one_task_at_a_time" in issue for issue in issues))
        self.assertTrue(any("offline_backup_manifest" in issue for issue in issues))
        self.assertTrue(any("media_recovery_point" in issue for issue in issues))
        self.assertTrue(any("bulk cleanup" in issue for issue in issues))

    def test_requires_existing_empty_writable_runtime_directories(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            declaration = valid_declaration(root)
            state_dir = Path(str(declaration["scrapeflow_state_dir"]))
            state_dir.joinpath("old-state.json").write_text("{}", encoding="utf-8")
            declaration["alist_data_dir"] = str(root / "missing-alist-data")

            issues = isolated_preflight_issues(declaration, root=root / "repo")

        self.assertTrue(any("scrapeflow_state_dir must be empty" in issue for issue in issues))
        self.assertTrue(any("alist_data_dir must exist" in issue for issue in issues))

    def test_reports_non_writable_isolated_runtime_directory(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            declaration = valid_declaration(root)
            with patch(
                "local.scrapeflow_api.isolated_preflight._directory_write_issue",
                return_value="alist_data_dir must be writable: test denial",
            ):
                issues = isolated_preflight_issues(declaration, root=root / "repo")

        self.assertTrue(any("must be writable" in issue for issue in issues))

    def test_rejects_formal_media_descendant_and_tampered_backup_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            declaration = valid_declaration(root)
            declaration["media_root"] = "/quark/影视/电影/验收/run-20260810"
            manifest_path = Path(str(declaration["offline_backup_manifest"]))
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            manifest["checks"]["copied_stats"]["alist_data"]["files"] += 1
            manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

            issues = isolated_preflight_issues(declaration, root=root / "repo")

        self.assertTrue(any("formal library shelf" in issue for issue in issues))
        self.assertTrue(any("valid verified backup" in issue for issue in issues))

    def test_load_declaration_requires_json_object(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "declaration.json"
            path.write_text(json.dumps(["not-object"]), encoding="utf-8")

            with self.assertRaises(ValueError):
                load_declaration(path)

    def test_captured_pass_survives_runtime_directories_becoming_non_empty(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            declaration = valid_declaration(root)
            report = capture_isolated_preflight_report(
                declaration,
                root=root / "repo",
                checked_at=datetime(2026, 8, 11, 1, 2, 3, tzinfo=UTC),
            )
            Path(str(declaration["scrapeflow_state_dir"])).joinpath(
                "started.json"
            ).write_text("{}", encoding="utf-8")
            Path(str(declaration["alist_data_dir"])).joinpath(
                "data.db"
            ).write_bytes(b"started")

            validated = validate_isolated_preflight_report(
                report,
                expected_declaration=declaration,
                root=root / "repo",
                require_passed=True,
            )

        self.assertEqual(validated["version"], 1)
        self.assertEqual(validated["status"], PREFLIGHT_REPORT_PASSED)
        self.assertEqual(validated["checked_at"], "2026-08-11T01:02:03+00:00")
        self.assertEqual(validated["issues"], [])

    def test_report_rejects_arbitrary_status_and_declaration_mismatch(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            declaration = valid_declaration(root)
            report = capture_isolated_preflight_report(
                declaration,
                root=root / "repo",
            )
            forged_status = dict(report)
            forged_status["status"] = "通过"
            non_string_status = dict(report)
            non_string_status["status"] = ["passed"]
            non_integer_version = dict(report)
            non_integer_version["version"] = 1.0
            drifted_declaration = dict(declaration)
            drifted_declaration["storage_label"] = "different-storage"

            with self.assertRaisesRegex(ValueError, "status must be passed or failed"):
                validate_isolated_preflight_report(forged_status, root=root / "repo")
            with self.assertRaisesRegex(ValueError, "status must be passed or failed"):
                validate_isolated_preflight_report(non_string_status, root=root / "repo")
            with self.assertRaisesRegex(ValueError, "version must be 1"):
                validate_isolated_preflight_report(non_integer_version, root=root / "repo")
            with self.assertRaisesRegex(ValueError, "does not match the expected declaration"):
                validate_isolated_preflight_report(
                    report,
                    expected_declaration=drifted_declaration,
                    root=root / "repo",
                )

    def test_captured_pass_still_requires_runtime_directories_to_exist(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            declaration = valid_declaration(root)
            report = capture_isolated_preflight_report(
                declaration,
                root=root / "repo",
            )
            state_dir = Path(str(declaration["scrapeflow_state_dir"]))
            state_dir.rmdir()

            with self.assertRaisesRegex(ValueError, "must still exist"):
                validate_isolated_preflight_report(
                    report,
                    root=root / "repo",
                    require_passed=True,
                )

    def test_report_rejects_malformed_schema_and_failed_capture(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            declaration = valid_declaration(root)
            declaration["provider_workers"] = 2
            failed = capture_isolated_preflight_report(
                declaration,
                root=root / "repo",
            )
            malformed = dict(failed)
            malformed.pop("checked_at")

            with self.assertRaisesRegex(ValueError, "report is missing keys: checked_at"):
                validate_isolated_preflight_report(malformed, root=root / "repo")
            with self.assertRaisesRegex(ValueError, "did not pass"):
                validate_isolated_preflight_report(
                    failed,
                    root=root / "repo",
                    require_passed=True,
                )

        self.assertEqual(failed["status"], PREFLIGHT_REPORT_FAILED)
        self.assertTrue(failed["issues"])

    def test_cli_atomically_writes_structured_report(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            declaration = valid_declaration(root)
            declaration_path = root / "declaration.json"
            report_path = root / "preflight-report.json"
            declaration_path.write_text(
                json.dumps(declaration),
                encoding="utf-8",
            )

            with redirect_stdout(StringIO()), redirect_stderr(StringIO()):
                returncode = isolated_preflight_main([
                    str(declaration_path),
                    "--report",
                    str(report_path),
                ])
            report = json.loads(report_path.read_text(encoding="utf-8"))

        self.assertEqual(returncode, 0)
        self.assertEqual(set(report), {
            "version",
            "status",
            "checked_at",
            "declaration",
            "issues",
            "checks",
        })
        self.assertEqual(report["status"], PREFLIGHT_REPORT_PASSED)
        self.assertEqual(
            set(report["checks"]["runtime_directories"]),
            {"scrapeflow_state_dir", "alist_data_dir"},
        )
        self.assertEqual(report["checks"]["offline_backup_manifest"]["verify"], "passed")

    def test_report_rejects_missing_or_forged_captured_checks(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            declaration = valid_declaration(root)
            report = capture_isolated_preflight_report(declaration, root=root / "repo")
            missing = dict(report)
            missing.pop("checks")
            forged = dict(report)
            forged["checks"] = {}

            with self.assertRaisesRegex(ValueError, "report is missing keys: checks"):
                validate_isolated_preflight_report(missing, root=root / "repo")
            with self.assertRaisesRegex(ValueError, "checks must contain"):
                validate_isolated_preflight_report(forged, root=root / "repo")

    def test_cli_rejects_report_paths_that_can_corrupt_runtime_or_backup(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            declaration = valid_declaration(root)
            declaration_path = root / "declaration.json"
            declaration_path.write_text(json.dumps(declaration), encoding="utf-8")
            state_report = Path(str(declaration["scrapeflow_state_dir"])) / "report.json"
            manifest = Path(str(declaration["offline_backup_manifest"]))
            manifest_before = manifest.read_bytes()

            with redirect_stdout(StringIO()), redirect_stderr(StringIO()):
                state_result = isolated_preflight_main([
                    str(declaration_path), "--report", str(state_report),
                ])
                manifest_result = isolated_preflight_main([
                    str(declaration_path), "--report", str(manifest),
                ])
                template_result = isolated_preflight_main([
                    "--template", "--report", str(root / "ignored.json"),
                ])
            state_report_exists = state_report.exists()
            manifest_after = manifest.read_bytes()

        self.assertEqual((state_result, manifest_result, template_result), (2, 2, 2))
        self.assertFalse(state_report_exists)
        self.assertEqual(manifest_after, manifest_before)


if __name__ == "__main__":
    unittest.main()
