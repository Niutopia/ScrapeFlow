"""Tests for the single backend release-check gate."""

from __future__ import annotations

from io import StringIO
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from local.scrapeflow_api.release_checks import (
    active_media_fingerprint_call_hits,
    active_python_paths,
    local_deployment_contract_issues,
    release_commands,
    run_release_checks,
)


class ReleaseCheckTests(unittest.TestCase):
    ENV_TEMPLATE_DEFAULTS = {
        "ALIST_URL": "http://127.0.0.1:5244",
        "ALIST_USERNAME": "admin",
        "ALIST_PASSWORD": "replace-with-your-alist-password",
        "SCRAPEFLOW_START_PAUSED": "1",
        "SCRAPEFLOW_INTAKE_MONITOR": "0",
        "SCRAPEFLOW_AUTOMATIC_AUDIT": "0",
        "SCRAPEFLOW_AUDIT_AUTO_REPAIR_ENABLED": "0",
        "SCRAPEFLOW_PROVIDER_AUTO_REPAIR_ENABLED": "0",
        "SCRAPEFLOW_PROVIDER_WORKERS": "1",
        "SCRAPEFLOW_ROOT_JOB_PILOT": "",
        "SCRAPEFLOW_QUARK_HELPER_URL": "http://127.0.0.1:18765",
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
        api_port_values = api_ports or ["127.0.0.1:${SCRAPEFLOW_API_PORT:-3010}:8765"]
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
            '      - "127.0.0.1:${SCRAPEFLOW_ALIST_PORT:-5244}:5244"\n'
            "    volumes:\n"
            "      - ${SCRAPEFLOW_HOST_STATE_ROOT:?set SCRAPEFLOW_HOST_STATE_ROOT}/alist-data:/opt/alist/data\n"
            "  api:\n"
            "    image: ${SCRAPEFLOW_API_IMAGE:-scrapeflow-api:local}\n"
            "    environment:\n"
            f"{api_env}\n"
            "    depends_on:\n"
            "      alist:\n"
            "        condition: service_healthy\n"
            "    ports:\n"
            f"{api_ports_yaml}\n"
            "    volumes:\n"
            "      - ${SCRAPEFLOW_HOST_STATE_ROOT:?set SCRAPEFLOW_HOST_STATE_ROOT}/scrapeflow-data:/data\n"
            "      - ${SCRAPEFLOW_HOST_STATE_ROOT:?set SCRAPEFLOW_HOST_STATE_ROOT}/api-temp:/var/tmp/scrapeflow\n"
            "  quark-helper:\n"
            "    image: ${SCRAPEFLOW_API_IMAGE:-scrapeflow-api:local}\n"
            "    restart: unless-stopped\n"
            "    command: [\"python3\", \"scripts/scrapeflow_quark_helper.py\", \"--docker-sidecar\"]\n"
            "    environment:\n"
            "      ALIST_URL: http://alist:5244\n"
            "      ALIST_USERNAME: ${ALIST_USERNAME:-}\n"
            "      ALIST_PASSWORD: ${ALIST_PASSWORD:-}\n"
            "      SCRAPEFLOW_MEDIA_ROOT: ${SCRAPEFLOW_MEDIA_ROOT:-/quark/影视}\n"
            "      NO_PROXY: \"alist,localhost,127.0.0.1${NO_PROXY:+,}${NO_PROXY:-}\"\n"
            "      SCRAPEFLOW_QUARK_HELPER_TOKEN: ${SCRAPEFLOW_QUARK_HELPER_TOKEN:-}\n"
            "      SCRAPEFLOW_QUARK_HELPER_CDP_URL: http://host.docker.internal:19222/json/list\n"
            "    depends_on:\n"
            "      api:\n"
            "        condition: service_started\n"
            "        restart: true\n"
            "    network_mode: service:api\n",
            encoding="utf-8",
        )
        (root / "Dockerfile.api").write_text(
            "FROM python:3.12-slim-bookworm\n"
            "COPY requirements.quark-helper.txt ./requirements.quark-helper.txt\n"
            "RUN python3 -m pip install --requirement requirements.quark-helper.txt\n"
            "COPY scripts/scrapeflow_quark_helper.py "
            "./scripts/scrapeflow_quark_helper.py\n",
            encoding="utf-8",
        )
        (root / "requirements.quark-helper.txt").write_text(
            "aiohttp>=3.9,<4\n", encoding="utf-8",
        )
        lifecycle_command = (
            "python3 scripts/scrapeflow_quark_lifecycle.py "
            "--install-launch-agent --replace-running\n"
        )
        (root / "README.md").write_text(
            "Compose sidecar deployment.\n" + lifecycle_command,
            encoding="utf-8",
        )
        (root / "docs").mkdir()
        (root / "docs" / "scrapeflow-deployment-open-order.md").write_text(
            "Compose sidecar startup order.\n" + lifecycle_command,
            encoding="utf-8",
        )
        (root / "scripts").mkdir()
        (root / "scripts" / "scrapeflow_quark_helper.py").write_text(
            "# passive typed helper sidecar\n", encoding="utf-8",
        )
        (root / "scripts" / "scrapeflow_quark_lifecycle.py").write_text(
            'LAUNCH_AGENT_LABEL = "com.scrapeflow.quark-cdp"\n'
            'CDP_ARGUMENTS = ("--remote-debugging-address=127.0.0.1", '
            '"--remote-debugging-port=19222")\n'
            'PAYLOAD = {"LimitLoadToSessionType": "Aqua", '
            '"ProcessType": "Interactive", '
            '"KeepAlive": {"SuccessfulExit": False}}\n'
            'actions.add_argument("--start", action="store_true")\n'
            'actions.add_argument("--restart", action="store_true")\n'
            'actions.add_argument("--force-restart", action="store_true")\n',
            encoding="utf-8",
        )

    def test_release_commands_match_backend_gate_contract(self) -> None:
        commands = release_commands()

        self.assertEqual(
            [command.name for command in commands],
            [
                "python-unittest",
                "git-worktree-clean",
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
        self.assertEqual(commands[1].args, ("git", "status", "--porcelain"))
        self.assertEqual(commands[2].args, ("git", "diff", "--check"))
        self.assertEqual(commands[3].args, ("docker", "compose", "config"))
        self.assertTrue(commands[3].isolated_env)
        self.assertEqual(
            commands[3].env["SCRAPEFLOW_HOST_STATE_ROOT"],
            "/tmp/scrapeflow-state",
        )
        self.assertEqual(
            commands[4].args,
            ("docker", "build", "-f", "Dockerfile.api", "."),
        )

    def test_skip_docker_returns_only_fast_local_commands(self) -> None:
        commands = release_commands(include_docker=False)

        self.assertEqual(
            [command.name for command in commands],
            ["python-unittest", "git-worktree-clean", "git-diff-check"],
        )

    def test_compose_config_output_is_withheld_from_release_stream(self) -> None:
        secret = "compose-resolved-secret-must-not-reach-evidence"
        for compose_returncode, expected_status in ((0, "passed"), (17, "failed")):
            with self.subTest(compose_returncode=compose_returncode):
                output = StringIO()
                compose_calls: list[dict[str, object]] = []

                def fake_run(
                    args: tuple[str, ...],
                    *,
                    cwd: Path,
                    env: dict[str, str] | None = None,
                    check: bool,
                    **kwargs: object,
                ) -> subprocess.CompletedProcess[str]:
                    del cwd, env, check
                    if args == ("docker", "compose", "config"):
                        compose_calls.append(kwargs)
                        return subprocess.CompletedProcess(
                            args,
                            compose_returncode,
                            stdout=f"environment:\n  TOKEN: {secret}\n",
                        )
                    if args == ("git", "status", "--porcelain"):
                        return subprocess.CompletedProcess(args, 0, stdout="")
                    return subprocess.CompletedProcess(args, 0)

                with patch(
                    "local.scrapeflow_api.release_checks.local_deployment_contract_issues",
                    return_value=[],
                ), patch(
                    "local.scrapeflow_api.release_checks.active_media_fingerprint_call_hits",
                    return_value=[],
                ), patch(
                    "local.scrapeflow_api.release_checks.subprocess.run",
                    side_effect=fake_run,
                ):
                    returncode = run_release_checks(stream=output)

                log_text = output.getvalue()
                self.assertEqual(returncode, compose_returncode)
                self.assertEqual(compose_calls, [{
                    "stdout": subprocess.PIPE,
                    "stderr": subprocess.STDOUT,
                    "text": True,
                }])
                self.assertIn("$ docker compose config", log_text)
                self.assertIn(
                    f"docker compose config {expected_status}; "
                    "resolved configuration output withheld",
                    log_text,
                )
                self.assertNotIn(secret, log_text)

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

    def test_current_operator_docs_describe_two_lane_contract(self) -> None:
        root = Path(__file__).resolve().parents[2]
        readme = (root / "README.md").read_text(encoding="utf-8")
        deployment = (root / "docs" / "scrapeflow-deployment-open-order.md").read_text(
            encoding="utf-8"
        )
        acceptance = (root / "docs" / "scrapeflow-isolated-acceptance-record.md").read_text(
            encoding="utf-8"
        )
        environment_template = (root / ".env.local.example").read_text(encoding="utf-8")
        engine_readme = (root / "engine" / "README.md").read_text(encoding="utf-8")

        self.assertIn("POST /api/root-jobs", readme)
        self.assertIn("quark_share → magnet", readme)
        self.assertIn("http://127.0.0.1:3010", readme)
        self.assertIn("<isolated-api-port>", readme)
        self.assertIn("两动作", readme)
        self.assertIn("--select-file", readme)
        self.assertIn("AList 离线下载功能已撤除", readme)
        self.assertNotIn("172.20.0.0/16", readme)
        self.assertNotIn("尚未接入运行时", readme)
        self.assertNotIn("四动作", readme)
        self.assertIn("http://127.0.0.1:3010", deployment)
        self.assertIn("-p scrapeflow-acceptance-<run-id>", deployment)
        self.assertIn("POST /api/control/resume", deployment)
        self.assertIn("quark_share → magnet", deployment)
        self.assertIn("AList 离线下载", deployment)
        self.assertNotIn("四动作", deployment)
        self.assertIn("有效 quark_share", acceptance)
        self.assertIn("quark_share → magnet", acceptance)
        self.assertIn("仅选择已映射成员", acceptance)
        self.assertNotIn("SCRAPEFLOW_API_PORT=8765", environment_template)
        self.assertIn("Setting it to 0 does not bypass", environment_template)
        self.assertNotIn("SCRAPEFLOW_ALIST_OFFLINE", environment_template)
        self.assertIn("创建 RootJob", engine_readme)
        self.assertNotIn("策略冲突", engine_readme)

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
                api_ports=["0.0.0.0:${SCRAPEFLOW_API_PORT:-3010}:8765"],
            )

            issues = local_deployment_contract_issues(root)

        self.assertTrue(any("SCRAPEFLOW_PROVIDER_AUTO_REPAIR_ENABLED" in issue for issue in issues))
        self.assertTrue(any("api.ports" in issue for issue in issues))

    def test_deployment_contract_rejects_isolation_boundary_drift(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            self._write_deployment_contract_files(root)
            compose_path = root / "docker-compose.yml"
            compose_path.write_text(
                compose_path.read_text(encoding="utf-8")
                .replace(
                    "SCRAPEFLOW_ALIST_PORT:-5244",
                    "SCRAPEFLOW_ALIST_PORT:-15244",
                )
                .replace(
                    "image: ${SCRAPEFLOW_API_IMAGE:-scrapeflow-api:local}",
                    "image: scrapeflow-api:legacy",
                )
                .replace(
                    "${SCRAPEFLOW_HOST_STATE_ROOT:?set SCRAPEFLOW_HOST_STATE_ROOT}/api-temp:/var/tmp/scrapeflow",
                    "./.runtime/api-temp:/var/tmp/scrapeflow",
                ),
                encoding="utf-8",
            )

            issues = local_deployment_contract_issues(root)

        self.assertTrue(any("alist.ports" in issue for issue in issues))
        self.assertTrue(any("api.image" in issue for issue in issues))
        self.assertTrue(any("quark-helper.image" in issue for issue in issues))
        self.assertTrue(any("api.volumes" in issue for issue in issues))

    def test_deployment_contract_rejects_retired_offline_runtime_markers(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            self._write_deployment_contract_files(root)
            compose_path = root / "docker-compose.yml"
            compose_path.write_text(
                compose_path.read_text(encoding="utf-8")
                + "  offline-aria2:\n"
                + "    image: scrapeflow-api:local\n",
                encoding="utf-8",
            )

            issues = local_deployment_contract_issues(root)

        self.assertTrue(any("offline-aria2" in issue for issue in issues))

    def test_deployment_contract_requires_helper_pansou_and_pilot_wiring(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            self._write_deployment_contract_files(root)
            env_path = root / ".env.local.example"
            env_path.write_text(
                "\n".join(
                    line for line in env_path.read_text(encoding="utf-8").splitlines()
                    if not line.startswith("SCRAPEFLOW_QUARK_HELPER_TOKEN=")
                ) + "\n",
                encoding="utf-8",
            )
            compose_path = root / "docker-compose.yml"
            compose_path.write_text(
                "\n".join(
                    line for line in compose_path.read_text(encoding="utf-8").splitlines()
                    if "SCRAPEFLOW_PROVIDER_PILOT_GAP" not in line
                    and "SCRAPEFLOW_ROOT_JOB_PILOT" not in line
                    and "SCRAPEFLOW_PANSOU_MAX_LINKS" not in line
                ) + "\n",
                encoding="utf-8",
            )

            issues = local_deployment_contract_issues(root)

        self.assertTrue(any("SCRAPEFLOW_QUARK_HELPER_TOKEN" in issue for issue in issues))
        self.assertTrue(any("SCRAPEFLOW_PROVIDER_PILOT_GAP" in issue for issue in issues))
        self.assertTrue(any("SCRAPEFLOW_ROOT_JOB_PILOT" in issue for issue in issues))
        self.assertTrue(any("SCRAPEFLOW_PANSOU_MAX_LINKS" in issue for issue in issues))

    def test_deployment_contract_rejects_sidecar_topology_drift(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            self._write_deployment_contract_files(root)
            compose_path = root / "docker-compose.yml"
            compose_path.write_text(
                compose_path.read_text(encoding="utf-8")
                .replace("network_mode: service:api", "network_mode: bridge")
                .replace("restart: true", "restart: false")
                .replace("--docker-sidecar", "--legacy-helper")
                .replace(
                    "http://host.docker.internal:19222/json/list",
                    "http://127.0.0.1:19222/json/list",
                ),
                encoding="utf-8",
            )

            issues = local_deployment_contract_issues(root)

        self.assertTrue(any("network_mode" in issue for issue in issues))
        self.assertTrue(any("depends_on.api.restart" in issue for issue in issues))
        self.assertTrue(any("command" in issue for issue in issues))
        self.assertTrue(any("SCRAPEFLOW_QUARK_HELPER_CDP_URL" in issue for issue in issues))

    def test_deployment_contract_rejects_host_helper_install_commands(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            self._write_deployment_contract_files(root)
            (root / "README.md").write_text(
                "python3 scripts/scrapeflow_quark_helper.py "
                "--install-launch-agent\n",
                encoding="utf-8",
            )

            issues = local_deployment_contract_issues(root)

        self.assertTrue(any("--install-launch-agent" in issue for issue in issues))

    def test_deployment_contract_rejects_lifecycle_code_in_helper_image(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            self._write_deployment_contract_files(root)
            dockerfile = root / "Dockerfile.api"
            dockerfile.write_text(
                dockerfile.read_text(encoding="utf-8")
                + "COPY scripts ./scripts\n"
                + "COPY scripts/scrapeflow_quark_lifecycle.py ./scripts/\n",
                encoding="utf-8",
            )

            issues = local_deployment_contract_issues(root)

        self.assertTrue(any("copy only the passive" in issue for issue in issues))
        self.assertTrue(any("must not contain the macOS" in issue for issue in issues))

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
