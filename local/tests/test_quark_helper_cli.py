"""Operational compatibility tests for the typed Quark Helper CLI."""

from __future__ import annotations

import argparse
import os
from pathlib import Path
import plistlib
import stat
import subprocess
import tempfile
import unittest
from unittest import mock

from scripts import scrapeflow_quark_helper as cli


class QuarkHelperCliTest(unittest.TestCase):
    def test_historical_defaults_and_default_token_path(self) -> None:
        with tempfile.TemporaryDirectory() as directory, mock.patch.dict(
            os.environ,
            {},
            clear=True,
        ), mock.patch.object(cli.Path, "home", return_value=Path(directory)), mock.patch.object(
            cli,
            "_install_launch_agent",
        ) as install:
            result = cli.main(["--install-launch-agent"])

        self.assertEqual(result, 0)
        args = install.call_args.args[0]
        self.assertEqual(args.port, 18765)
        self.assertEqual(args.cdp_url, "http://127.0.0.1:19222/json/list")
        self.assertEqual(args.state_dir, Path(directory) / ".scrapeflow-quark-helper")
        self.assertEqual(args.token_file, args.state_dir / "token")

    def test_token_creation_is_private_and_does_not_replace_existing_secret(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            token_file = Path(directory) / "state" / "token"
            cli._ensure_token_file(token_file)
            original = token_file.read_text(encoding="utf-8")

            self.assertGreaterEqual(len(original.strip()), 24)
            self.assertEqual(stat.S_IMODE(token_file.stat().st_mode), 0o600)

            cli._ensure_token_file(token_file)
            self.assertEqual(token_file.read_text(encoding="utf-8"), original)

    def test_existing_state_directory_is_owned_real_and_tightened_to_0700(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            state_dir = Path(directory) / "state"
            state_dir.mkdir(mode=0o755)

            cli._ensure_private_state_dir(state_dir)

            state_stat = state_dir.lstat()
            self.assertTrue(stat.S_ISDIR(state_stat.st_mode))
            self.assertEqual(state_stat.st_uid, os.getuid())
            self.assertEqual(stat.S_IMODE(state_stat.st_mode), 0o700)

    def test_state_directory_rejects_symlink(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            real_state = root / "real-state"
            real_state.mkdir()
            linked_state = root / "linked-state"
            linked_state.symlink_to(real_state, target_is_directory=True)

            with self.assertRaisesRegex(cli.QuarkHelperCliError, "real directory"):
                cli._ensure_private_state_dir(linked_state)

    def test_token_file_rejects_group_or_other_access(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            token_file = Path(directory) / "token"
            token_file.write_text("a" * 32, encoding="utf-8")
            token_file.chmod(0o640)

            with self.assertRaisesRegex(cli.QuarkHelperCliError, "inaccessible"):
                cli._ensure_token_file(token_file)

    def test_token_file_rejects_wrong_owner_or_multiple_links(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            token_file = Path(directory) / "token"
            token_file.write_text("a" * 32, encoding="utf-8")
            token_file.chmod(0o600)
            with mock.patch.object(cli.os, "getuid", return_value=os.getuid() + 1):
                with self.assertRaisesRegex(cli.QuarkHelperCliError, "current user"):
                    cli._ensure_token_file(token_file)

            second_link = Path(directory) / "token-link"
            os.link(token_file, second_link)
            with self.assertRaisesRegex(cli.QuarkHelperCliError, "exactly one link"):
                cli._ensure_token_file(token_file)

    def test_launch_agent_runs_current_typed_cli_in_background(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            target = root / "Library" / "LaunchAgents" / "helper.plist"
            state_dir = root / ".scrapeflow-quark-helper"
            token_file = state_dir / "token"
            args = argparse.Namespace(
                port=18765,
                cdp_url="http://127.0.0.1:19222/json/list",
                staging_root=cli.DEFAULT_STAGING_ROOT,
                mount_path=cli.DEFAULT_MOUNT_PATH,
                root_fid="0",
                state_dir=state_dir,
                token_file=token_file,
            )
            with mock.patch.object(
                cli,
                "_launch_agent_path",
                return_value=target,
            ), mock.patch.object(cli, "_run_launchctl") as launchctl, mock.patch.object(
                cli,
                "_wait_helper_alive",
            ) as wait:
                installed = cli._install_launch_agent(args)

            self.assertEqual(installed, target)
            self.assertEqual(stat.S_IMODE(state_dir.stat().st_mode), 0o700)
            self.assertEqual(stat.S_IMODE(target.stat().st_mode), 0o600)
            with target.open("rb") as source:
                payload = plistlib.load(source)
            self.assertEqual(payload["Label"], "com.scrapeflow.quark-native-helper")
            self.assertIs(payload["RunAtLoad"], True)
            self.assertEqual(payload["KeepAlive"], {"Crashed": True})
            self.assertEqual(payload["ProcessType"], "Background")
            self.assertEqual(payload["StandardOutPath"], "/dev/null")
            self.assertEqual(payload["StandardErrorPath"], "/dev/null")
            program = payload["ProgramArguments"]
            self.assertEqual(program[1], str(Path(cli.__file__).resolve()))
            self.assertEqual(program[program.index("--port") + 1], "18765")
            self.assertEqual(
                program[program.index("--cdp-url") + 1],
                "http://127.0.0.1:19222/json/list",
            )
            self.assertEqual(
                program[program.index("--token-file") + 1],
                str(token_file.resolve()),
            )
            self.assertNotIn("--launch-quark", program)
            self.assertNotIn("--restart-quark", program)
            self.assertNotIn("--activate-quark", program)
            self.assertEqual(
                launchctl.call_args_list,
                [
                    mock.call("bootout", cli._launchctl_service(), check=False),
                    mock.call("bootstrap", cli._launchctl_domain(), str(target)),
                    mock.call("kickstart", "-k", cli._launchctl_service()),
                ],
            )
            wait.assert_called_once()
            self.assertEqual(wait.call_args.args[0], 18765)

    def test_launch_agent_running_check_uses_exact_launchctl_service(self) -> None:
        result = subprocess.CompletedProcess(
            ["launchctl"],
            0,
            stdout="path = /tmp/helper.plist\nstate = running\npid = 123\n",
            stderr="",
        )
        with mock.patch.object(cli, "_run_launchctl", return_value=result) as launchctl:
            self.assertTrue(cli._launch_agent_is_running())

        launchctl.assert_called_once_with("print", cli._launchctl_service())

    def test_launch_agent_wait_uses_only_launchd_state_and_tcp_bind(self) -> None:
        with mock.patch.object(
            cli,
            "_launch_agent_is_running",
            side_effect=[False, True],
        ) as running, mock.patch.object(
            cli,
            "_helper_port_is_bound",
            return_value=True,
        ) as bound, mock.patch.object(cli.time, "sleep"):
            result = cli._wait_helper_alive(18765, timeout=1.0)

        self.assertIsNone(result)
        self.assertEqual(running.call_count, 2)
        bound.assert_called_once_with(18765, timeout=mock.ANY)
        self.assertFalse(hasattr(cli, "_fetch_helper_health"))

    def test_plist_write_ignores_predictable_temp_symlink(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            target = root / "helper.plist"
            victim = root / "victim"
            victim.write_text("do-not-touch", encoding="utf-8")
            predictable = target.with_suffix(".plist.tmp")
            predictable.symlink_to(victim)

            cli._write_launch_agent_plist(target, {"Label": "test-helper"})

            self.assertEqual(victim.read_text(encoding="utf-8"), "do-not-touch")
            self.assertTrue(predictable.is_symlink())
            self.assertEqual(stat.S_IMODE(target.stat().st_mode), 0o600)

    def test_install_rejects_symlink_plist_target_before_bootout(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            state_dir = root / "state"
            token_file = state_dir / "token"
            victim = root / "victim.plist"
            victim.write_text("do-not-touch", encoding="utf-8")
            target = root / "helper.plist"
            target.symlink_to(victim)
            args = argparse.Namespace(
                port=18765,
                cdp_url=cli.DEFAULT_CDP_URL,
                staging_root=cli.DEFAULT_STAGING_ROOT,
                mount_path=cli.DEFAULT_MOUNT_PATH,
                root_fid="0",
                state_dir=state_dir,
                token_file=token_file,
            )
            with mock.patch.object(
                cli,
                "_launch_agent_path",
                return_value=target,
            ), mock.patch.object(cli, "_run_launchctl") as launchctl:
                with self.assertRaisesRegex(cli.QuarkHelperCliError, "unsafe"):
                    cli._install_launch_agent(args)

            launchctl.assert_not_called()
            self.assertEqual(victim.read_text(encoding="utf-8"), "do-not-touch")

    def test_install_prevalidates_full_typed_runtime_before_plist_or_launchctl(self) -> None:
        invalid_overrides = {
            "cdp_url": "http://127.0.0.1:19222/json/version",
            "staging_root": "/quark/other-root",
            "mount_path": "/somewhere-else",
            "root_fid": "invalid root fid",
        }
        for field, invalid_value in invalid_overrides.items():
            with self.subTest(field=field), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                state_dir = root / "state"
                token_file = state_dir / "token"
                target = root / "Library" / "LaunchAgents" / "helper.plist"
                values = {
                    "port": 18765,
                    "cdp_url": cli.DEFAULT_CDP_URL,
                    "staging_root": cli.DEFAULT_STAGING_ROOT,
                    "mount_path": cli.DEFAULT_MOUNT_PATH,
                    "root_fid": "0",
                    "state_dir": state_dir,
                    "token_file": token_file,
                }
                values[field] = invalid_value
                args = argparse.Namespace(**values)
                with mock.patch.object(
                    cli,
                    "_launch_agent_path",
                    return_value=target,
                ), mock.patch.object(cli, "_run_launchctl") as launchctl, mock.patch.object(
                    cli,
                    "_write_launch_agent_plist",
                ) as write_plist:
                    with self.assertRaises(cli.QuarkHelperValidationError):
                        cli._install_launch_agent(args)

                launchctl.assert_not_called()
                write_plist.assert_not_called()
                self.assertFalse(target.parent.exists())
                self.assertFalse(target.exists())

    def test_install_prevalidation_uses_final_private_token(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            state_dir = Path(directory) / "state"
            token_file = state_dir / "token"
            cli._ensure_private_state_dir(state_dir)
            cli._ensure_token_file(token_file)
            args = argparse.Namespace(
                port=18765,
                cdp_url=cli.DEFAULT_CDP_URL,
                staging_root=cli.DEFAULT_STAGING_ROOT,
                mount_path=cli.DEFAULT_MOUNT_PATH,
                root_fid="0",
                state_dir=state_dir,
                token_file=token_file,
            )
            with mock.patch.dict(
                os.environ,
                {"SCRAPEFLOW_QUARK_HELPER_TOKEN": "z" * 32},
            ):
                config = cli._validate_install_config(args)

            self.assertEqual(config.token, token_file.read_text(encoding="utf-8").strip())
            self.assertNotEqual(config.token, "z" * 32)

    def test_uninstall_boots_out_and_removes_only_launch_agent_plist(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            target = Path(directory) / "helper.plist"
            target.write_text("placeholder", encoding="utf-8")
            with mock.patch.object(
                cli,
                "_launch_agent_path",
                return_value=target,
            ), mock.patch.object(cli, "_bootout_launch_agent") as bootout:
                cli._uninstall_launch_agent()

            bootout.assert_called_once_with()
            self.assertFalse(target.exists())


if __name__ == "__main__":
    unittest.main()
