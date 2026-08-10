"""Tests for the single backend release-check gate."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from local.scrapeflow_api.release_checks import (
    active_media_fingerprint_call_hits,
    active_python_paths,
    local_deployment_contract_issues,
    release_commands,
)


class ReleaseCheckTests(unittest.TestCase):
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

    def _write_deployment_contract_files(
        self,
        root: Path,
        *,
        env_overrides: dict[str, str] | None = None,
        compose_overrides: dict[str, str] | None = None,
        api_ports: list[str] | None = None,
    ) -> None:
        env_values = dict(self.ENV_TEMPLATE_DEFAULTS)
        env_values.update(env_overrides or {})
        (root / ".env.local.example").write_text(
            "\n".join(f"{key}={value}" for key, value in env_values.items()) + "\n",
            encoding="utf-8",
        )
        compose_values = dict(self.COMPOSE_DEFAULTS)
        compose_values.update(compose_overrides or {})
        api_port_values = api_ports or ["127.0.0.1:${SCRAPEFLOW_API_PORT:-8765}:8765"]
        api_env = "\n".join(
            f"      {key}: ${{{key}:-{value}}}"
            for key, value in compose_values.items()
        )
        api_ports_yaml = "\n".join(f'      - "{port}"' for port in api_port_values)
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
            f"{api_ports_yaml}\n",
            encoding="utf-8",
        )

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

    def test_deployment_contract_accepts_current_defaults(self) -> None:
        self.assertEqual(local_deployment_contract_issues(), [])

    def test_deployment_contract_rejects_unpaused_env_template(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            self._write_deployment_contract_files(
                root,
                env_overrides={"SCRAPEFLOW_START_PAUSED": "0"},
            )

            issues = local_deployment_contract_issues(root)

        self.assertTrue(any("SCRAPEFLOW_START_PAUSED" in issue for issue in issues))

    def test_deployment_contract_rejects_compose_gate_or_port_drift(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            self._write_deployment_contract_files(
                root,
                compose_overrides={"SCRAPEFLOW_PROVIDER_AUTO_REPAIR_ENABLED": "1"},
                api_ports=["0.0.0.0:${SCRAPEFLOW_API_PORT:-8765}:8765"],
            )

            issues = local_deployment_contract_issues(root)

        self.assertTrue(any("SCRAPEFLOW_PROVIDER_AUTO_REPAIR_ENABLED" in issue for issue in issues))
        self.assertTrue(any("api.ports" in issue for issue in issues))

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

    def test_active_code_scan_reports_imported_alias_call(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            active_dir = root / "engine"
            active_dir.mkdir()
            banned_name = "sha" + "256"
            (active_dir / "bad_alias.py").write_text(
                f"from hashlib import {banned_name} as media_fingerprint\n"
                "value = media_fingerprint(b'data')\n",
                encoding="utf-8",
            )

            hits = active_media_fingerprint_call_hits(root)

        self.assertEqual(len(hits), 1)
        self.assertIn("bad_alias.py:2", hits[0])

    def test_active_code_scan_reports_hashlib_new_variants(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            active_dir = root / "engine"
            active_dir.mkdir()
            (active_dir / "bad_new.py").write_text(
                "import hashlib as h\n"
                "value = h.new('sha' + '256', b'data')\n"
                "other = h.new(name='SHA' + '-256', data=b'data')\n",
                encoding="utf-8",
            )

            hits = active_media_fingerprint_call_hits(root)

        self.assertEqual(len(hits), 2)
        self.assertIn("bad_new.py:2", hits[0])
        self.assertIn("bad_new.py:3", hits[1])

    def test_active_code_scan_reports_getattr_call(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            active_dir = root / "engine"
            active_dir.mkdir()
            (active_dir / "bad_getattr.py").write_text(
                "import hashlib\n"
                "value = getattr(hashlib, 'sha' + '256')(b'data')\n",
                encoding="utf-8",
            )

            hits = active_media_fingerprint_call_hits(root)

        self.assertEqual(len(hits), 1)
        self.assertIn("bad_getattr.py:2", hits[0])

    def test_active_code_scan_allows_torrent_infohash_primitive(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            active_dir = root / "engine"
            active_dir.mkdir()
            (active_dir / "torrent_infohash.py").write_text(
                "import hashlib\n"
                "value = hashlib.sha1(b'torrent-metainfo').hexdigest()\n",
                encoding="utf-8",
            )

            hits = active_media_fingerprint_call_hits(root)

        self.assertEqual(hits, [])


if __name__ == "__main__":
    unittest.main()
