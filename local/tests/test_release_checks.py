"""Tests for the single backend release-check gate."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from local.scrapeflow_api.release_checks import (
    active_media_fingerprint_call_hits,
    active_python_paths,
    release_commands,
)


class ReleaseCheckTests(unittest.TestCase):
    def test_release_commands_match_backend_gate_contract(self) -> None:
        commands = release_commands()

        self.assertEqual(
            [command.name for command in commands],
            [
                "python-unittest",
                "git-diff-check",
                "docker-compose-config",
                "docker-build-api",
            ],
        )
        self.assertEqual(
            commands[0].args,
            (
                "python3", "-m", "unittest", "discover",
                "-s", "local/tests", "-p", "test_*.py",
            ),
        )
        self.assertEqual(commands[0].env["SCRAPEFLOW_IGNORE_LOCAL_ENV"], "1")
        self.assertEqual(commands[0].env["PYTHONDONTWRITEBYTECODE"], "1")
        self.assertEqual(commands[1].args, ("git", "diff", "--check"))
        self.assertEqual(commands[2].args, ("docker", "compose", "config"))
        self.assertTrue(commands[2].isolated_env)
        self.assertEqual(
            commands[2].env["SCRAPEFLOW_HOST_STATE_ROOT"],
            "/tmp/scrapeflow-state",
        )
        self.assertEqual(
            commands[3].args,
            ("docker", "build", "-f", "Dockerfile.api", "."),
        )

    def test_skip_docker_returns_only_fast_local_commands(self) -> None:
        commands = release_commands(include_docker=False)

        self.assertEqual(
            [command.name for command in commands],
            ["python-unittest", "git-diff-check"],
        )

    def test_active_path_scan_excludes_tests_and_caches(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "engine" / "__pycache__").mkdir(parents=True)
            (root / "engine" / "__pycache__" / "ignored.py").write_text("")
            (root / "local" / "tests").mkdir(parents=True)
            (root / "local" / "tests" / "ignored.py").write_text("")
            (root / "local" / "scrapeflow_api").mkdir(parents=True)
            active = root / "local" / "scrapeflow_api" / "active.py"
            active.write_text("VALUE = 1\n", encoding="utf-8")

            paths = active_python_paths(root)

        self.assertEqual(paths, [active])

    def test_active_code_has_no_banned_media_fingerprint_calls(self) -> None:
        self.assertEqual(active_media_fingerprint_call_hits(), [])

    def test_active_code_scan_reports_banned_call_in_fake_tree(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            active_dir = root / "engine"
            active_dir.mkdir()
            banned_call = "hashlib." + "sha" + "256"
            (active_dir / "bad.py").write_text(
                f"import hashlib\nvalue = {banned_call}(b'data')\n",
                encoding="utf-8",
            )

            hits = active_media_fingerprint_call_hits(root)

        self.assertEqual(len(hits), 1)
        self.assertIn("bad.py:2", hits[0])


if __name__ == "__main__":
    unittest.main()
