"""CLI tests for the direct and Docker-sidecar typed Quark Helper."""

from __future__ import annotations

from contextlib import redirect_stderr
import io
import os
from pathlib import Path
import tempfile
import unittest
from unittest import mock

from scripts import scrapeflow_quark_helper as cli


TOKEN = "t" * 32


class QuarkHelperCliTest(unittest.TestCase):
    def _run(self, arguments: list[str], *, environment: dict[str, str] | None = None):
        with tempfile.TemporaryDirectory() as directory, mock.patch.dict(
            os.environ,
            environment or {},
            clear=True,
        ), mock.patch.object(
            cli.Path,
            "home",
            return_value=Path(directory),
        ), mock.patch.object(
            cli,
            "load_helper_token",
            return_value=TOKEN,
        ) as load_token, mock.patch.object(
            cli,
            "serve_quark_helper",
        ) as serve, redirect_stderr(io.StringIO()):
            result = cli.main(arguments)
        return result, load_token, serve

    def test_direct_defaults_keep_historical_loopback_cdp_and_bind(self) -> None:
        result, load_token, serve = self._run([])

        self.assertEqual(result, 0)
        config = serve.call_args.args[0]
        self.assertEqual(config.host, "127.0.0.1")
        self.assertEqual(config.port, 18765)
        self.assertEqual(config.cdp_url, "http://127.0.0.1:19222/json/list")
        self.assertFalse(config.docker_sidecar)
        token_file = load_token.call_args.kwargs["token_file"]
        self.assertEqual(token_file.name, "token")
        self.assertEqual(token_file.parent.name, ".scrapeflow-quark-helper")

    def test_explicit_docker_sidecar_uses_only_fixed_bridge_and_loopback_bind(self) -> None:
        result, _load_token, serve = self._run(["--docker-sidecar"])

        self.assertEqual(result, 0)
        config = serve.call_args.args[0]
        self.assertEqual(config.host, "127.0.0.1")
        self.assertEqual(config.port, 18765)
        self.assertEqual(config.cdp_url, cli.DOCKER_SIDECAR_CDP_URL)
        self.assertTrue(config.docker_sidecar)

    def test_docker_sidecar_rejects_every_nonfixed_cdp_override(self) -> None:
        for url in (
            cli.DEFAULT_CDP_URL,
            "http://host.docker.internal:19223/json/list",
            "http://host.docker.internal:19222/json",
        ):
            with self.subTest(url=url), self.assertRaises(SystemExit) as raised:
                self._run(["--docker-sidecar", "--cdp-url", url])
            self.assertEqual(raised.exception.code, 2)

    def test_docker_sidecar_rejects_ambient_cdp_override(self) -> None:
        with self.assertRaises(SystemExit) as raised:
            self._run(
                ["--docker-sidecar"],
                environment={"SCRAPEFLOW_QUARK_HELPER_CDP_URL": cli.DEFAULT_CDP_URL},
            )
        self.assertEqual(raised.exception.code, 2)

    def test_helper_bind_remains_loopback_in_every_mode(self) -> None:
        for arguments in (
            ["--host", "0.0.0.0"],
            ["--docker-sidecar", "--host", "0.0.0.0"],
        ):
            with self.subTest(arguments=arguments), self.assertRaises(SystemExit) as raised:
                self._run(arguments)
            self.assertEqual(raised.exception.code, 2)

    def test_explicit_token_file_is_forwarded_without_creation(self) -> None:
        token_path = "/run/secrets/scrapeflow-quark-helper-token"
        result, load_token, serve = self._run(
            ["--docker-sidecar"],
            environment={"SCRAPEFLOW_QUARK_HELPER_TOKEN_FILE": token_path},
        )

        self.assertEqual(result, 0)
        load_token.assert_called_once_with(token_file=Path(token_path))
        self.assertEqual(serve.call_args.args[0].token, TOKEN)

    def test_cli_contains_no_desktop_background_lifecycle(self) -> None:
        source = Path(cli.__file__).read_text(encoding="utf-8")
        for forbidden in (
            "launchctl",
            "plistlib",
            "LaunchAgent",
            "install-launch-agent",
            "uninstall-launch-agent",
            "subprocess",
        ):
            with self.subTest(forbidden=forbidden):
                self.assertNotIn(forbidden, source)


if __name__ == "__main__":
    unittest.main()
