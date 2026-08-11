"""Tests for release-check artifact capture."""

from __future__ import annotations

from datetime import UTC, datetime
import json
import tempfile
import unittest
from pathlib import Path

from local.scrapeflow_api.release_evidence import (
    ReleaseEvidenceResult,
    capture_release_evidence,
    release_evidence_command,
)


class ReleaseEvidenceTests(unittest.TestCase):
    def test_release_evidence_command_wraps_full_gate_by_default(self) -> None:
        self.assertEqual(
            release_evidence_command(),
            ("python3", "scripts/scrapeflow_release_check.py"),
        )

    def test_release_evidence_command_can_skip_docker(self) -> None:
        self.assertEqual(
            release_evidence_command(include_docker=False),
            ("python3", "scripts/scrapeflow_release_check.py", "--skip-docker"),
        )

    def test_capture_writes_report_and_raw_log(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "repo"
            root.mkdir()
            output_dir = Path(temporary) / "artifacts"
            calls: list[tuple[tuple[str, ...], Path]] = []
            moments = iter([
                datetime(2026, 8, 10, 1, 0, tzinfo=UTC),
                datetime(2026, 8, 10, 1, 1, tzinfo=UTC),
            ])

            def runner(args: tuple[str, ...], cwd: Path) -> ReleaseEvidenceResult:
                # The release gate's clean-worktree assertion runs before any
                # evidence path is created, even when the caller chooses a
                # non-ignored directory inside its repository.
                self.assertFalse(output_dir.exists())
                calls.append((args, cwd))
                return ReleaseEvidenceResult(0, "tests ok\nbuild ok\n")

            report = capture_release_evidence(
                output_dir,
                root=root,
                runner=runner,
                clock=lambda: next(moments),
            )

            report_path = Path(str(report["report_path"]))
            log_path = Path(str(report["log_path"]))
            saved = json.loads(report_path.read_text(encoding="utf-8"))
            log_text = log_path.read_text(encoding="utf-8")

            self.assertEqual(calls, [(
                ("python3", "scripts/scrapeflow_release_check.py"),
                root,
            )])
            self.assertEqual(report["status"], "通过")
            self.assertEqual(saved["status"], "通过")
            self.assertEqual(saved["returncode"], 0)
            self.assertTrue(saved["include_docker"])
            self.assertEqual(saved["started_at"], "2026-08-10T01:00:00+00:00")
            self.assertEqual(saved["finished_at"], "2026-08-10T01:01:00+00:00")
            self.assertEqual(log_text, "tests ok\nbuild ok\n")

    def test_capture_marks_failure_without_discarding_log(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "repo"
            root.mkdir()
            output_dir = Path(temporary) / "artifacts"

            def runner(args: tuple[str, ...], cwd: Path) -> ReleaseEvidenceResult:
                return ReleaseEvidenceResult(3, "docker build failed\n")

            report = capture_release_evidence(
                output_dir,
                include_docker=False,
                root=root,
                runner=runner,
                clock=lambda: datetime(2026, 8, 10, 1, 0, tzinfo=UTC),
            )

            saved = json.loads(Path(str(report["report_path"])).read_text(encoding="utf-8"))
            log_text = Path(str(report["log_path"])).read_text(encoding="utf-8")

        self.assertEqual(saved["status"], "失败")
        self.assertEqual(saved["returncode"], 3)
        self.assertFalse(saved["include_docker"])
        self.assertEqual(saved["command"], [
            "python3",
            "scripts/scrapeflow_release_check.py",
            "--skip-docker",
        ])
        self.assertEqual(log_text, "docker build failed\n")


if __name__ == "__main__":
    unittest.main()
