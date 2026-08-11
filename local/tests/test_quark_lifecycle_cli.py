"""Tests for the direct QuarkCloudDrive macOS lifecycle CLI."""

from __future__ import annotations

from contextlib import redirect_stderr, redirect_stdout
import io
import json
import os
from pathlib import Path
import plistlib
import stat
import subprocess
import tempfile
import unittest
from unittest import mock

from scripts import scrapeflow_quark_lifecycle as cli


def completed(
    returncode: int = 0, *, stdout: str = "", stderr: str = ""
) -> subprocess.CompletedProcess[str]:
    return subprocess.CompletedProcess(
        ["command"], returncode, stdout=stdout, stderr=stderr
    )


def service_snapshot(path: Path) -> subprocess.CompletedProcess[str]:
    return completed(stdout=f"state = running\npath = {path}\n")


class QuarkLifecyclePayloadTest(unittest.TestCase):
    def test_payload_runs_quark_directly_with_only_fixed_cdp_arguments(self) -> None:
        payload = cli._launch_agent_payload()

        self.assertEqual(payload["Label"], "com.scrapeflow.quark-cdp")
        self.assertEqual(
            payload["ProgramArguments"],
            [
                "/Applications/QuarkCloudDrive.app/Contents/MacOS/QuarkCloudDrive",
                "--remote-debugging-address=127.0.0.1",
                "--remote-debugging-port=19222",
            ],
        )
        self.assertIs(payload["RunAtLoad"], True)
        self.assertEqual(payload["KeepAlive"], {"SuccessfulExit": False})
        self.assertEqual(payload["LimitLoadToSessionType"], "Aqua")
        self.assertEqual(payload["ProcessType"], "Interactive")
        self.assertEqual(payload["ThrottleInterval"], 30)
        self.assertNotIn("UserName", payload)
        self.assertNotIn("AbandonProcessGroup", payload)
        rendered = json.dumps(payload)
        self.assertNotIn("python", rendered.casefold())
        self.assertNotIn("scrapeflow_quark_helper", rendered)

    def test_normal_quit_uses_appkit_and_never_force_terminates_or_activates(self) -> None:
        self.assertIn("NSRunningApplication", cli.NORMAL_TERMINATE_JXA)
        self.assertIn("target.terminate", cli.NORMAL_TERMINATE_JXA)
        for forbidden in ("forceTerminate", ".activate", "click", "SIGKILL"):
            with self.subTest(forbidden=forbidden):
                self.assertNotIn(forbidden, cli.NORMAL_TERMINATE_JXA)


class QuarkLifecycleFilesystemTest(unittest.TestCase):
    def test_atomic_write_creates_user_only_regular_plist(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            target = Path(directory) / "agent.plist"
            cli._write_launch_agent(target, cli._launch_agent_payload())

            metadata = target.lstat()
            self.assertTrue(stat.S_ISREG(metadata.st_mode))
            self.assertEqual(stat.S_IMODE(metadata.st_mode), 0o600)
            with target.open("rb") as source:
                self.assertEqual(plistlib.load(source), cli._launch_agent_payload())
            self.assertEqual(list(target.parent.glob(f".{target.name}.*.tmp")), [])

    def test_atomic_write_refuses_existing_symlink(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            victim = root / "victim"
            victim.write_text("preserve", encoding="utf-8")
            target = root / "agent.plist"
            target.symlink_to(victim)

            with self.assertRaisesRegex(cli.QuarkLifecycleError, "symbolic link"):
                cli._write_launch_agent(target, cli._launch_agent_payload())

            self.assertEqual(victim.read_text(encoding="utf-8"), "preserve")

    def test_target_validation_refuses_hard_link(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            first = root / "first.plist"
            target = root / "agent.plist"
            first.write_text("plist", encoding="utf-8")
            os.link(first, target)

            with self.assertRaisesRegex(cli.QuarkLifecycleError, "one hard link"):
                cli._inspect_launch_agent_target(target)

    def test_previous_plist_must_keep_the_fixed_label_and_argv(self) -> None:
        valid = plistlib.dumps(cli._launch_agent_payload())
        cli._validate_previous_launch_agent(valid)
        invalid = plistlib.dumps({"Label": "other", "ProgramArguments": ["bad"]})
        with self.assertRaisesRegex(cli.QuarkLifecycleError, "fixed lifecycle"):
            cli._validate_previous_launch_agent(invalid)


class QuarkLifecycleProcessTest(unittest.TestCase):
    def test_process_scan_revalidates_every_pgrep_candidate(self) -> None:
        with mock.patch.object(
            cli,
            "_run_command",
            return_value=completed(stdout="100\n101\n"),
        ), mock.patch.object(
            cli, "_pid_is_exact_quark", side_effect=lambda pid: pid == 100
        ):
            self.assertEqual(cli._find_quark_pids(), (100,))

    def test_pid_identity_requires_current_uid_and_strict_managed_argv(self) -> None:
        bare = (os.getuid(), (str(cli.QUARK_EXECUTABLE),))
        managed = (os.getuid(), (str(cli.QUARK_EXECUTABLE), *cli.CDP_ARGUMENTS))
        polluted = (
            os.getuid(),
            (str(cli.QUARK_EXECUTABLE), *cli.CDP_ARGUMENTS, "--other"),
        )
        with mock.patch.object(cli, "_pid_identity", return_value=bare):
            self.assertTrue(cli._pid_is_exact_quark(1))
            self.assertFalse(cli._pid_is_managed_quark(1))
            self.assertTrue(cli._pid_has_replaceable_argv(1))
        with mock.patch.object(cli, "_pid_identity", return_value=managed):
            self.assertTrue(cli._pid_is_managed_quark(1))
            self.assertTrue(cli._pid_has_replaceable_argv(1))
        with mock.patch.object(cli, "_pid_identity", return_value=polluted):
            self.assertFalse(cli._pid_is_managed_quark(1))
            self.assertFalse(cli._pid_has_replaceable_argv(1))
        with mock.patch.object(
            cli, "_pid_identity", return_value=(os.getuid() + 1, bare[1])
        ):
            self.assertFalse(cli._pid_is_exact_quark(1))

    def test_normal_termination_uses_exact_bundle_pid_without_signals(self) -> None:
        evidence = completed(stdout="terminate-requested:100\n")
        with mock.patch.object(cli, "_pid_is_exact_quark", return_value=True), \
             mock.patch.object(cli, "_run_command", return_value=evidence) as run:
            cli._request_normal_quark_termination(100)

        arguments = run.call_args.args[0]
        self.assertEqual(arguments[:3], ("/usr/bin/osascript", "-l", "JavaScript"))
        self.assertEqual(arguments[-1], "100")
        self.assertNotIn("kill", " ".join(arguments).casefold())

    def test_normal_termination_timeout_never_escalates(self) -> None:
        with mock.patch.object(cli, "_pid_is_exact_quark", return_value=True), \
             mock.patch.object(
                 cli.time, "monotonic", side_effect=(0.0, 0.0, 2.0)
             ), mock.patch.object(cli.time, "sleep"):
            with self.assertRaisesRegex(cli.QuarkLifecycleError, "refusing force"):
                cli._wait_for_quark_pid_exit(100, timeout=1)


class QuarkLifecycleServiceTest(unittest.TestCase):
    def test_missing_service_is_the_only_ignored_launchctl_print_error(self) -> None:
        missing = completed(
            113, stderr="Could not find service com.scrapeflow.quark-cdp"
        )
        with mock.patch.object(cli, "_run_launchctl", return_value=missing):
            self.assertIsNone(cli._service_snapshot())
        other = completed(5, stderr="permission denied")
        with mock.patch.object(cli, "_run_launchctl", return_value=other):
            with self.assertRaisesRegex(cli.QuarkLifecycleError, "permission denied"):
                cli._service_snapshot()

    def test_loaded_label_must_belong_to_exact_plist(self) -> None:
        expected = Path("/tmp/expected.plist")
        cli._assert_service_origin(service_snapshot(expected), expected)
        with self.assertRaisesRegex(cli.QuarkLifecycleError, "belongs to"):
            cli._assert_service_origin(
                service_snapshot(Path("/tmp/other.plist")), expected
            )

    def test_kickstart_uses_documented_pid_output_and_force_is_explicit(self) -> None:
        with mock.patch.object(
            cli, "_run_launchctl", return_value=completed(stdout="321\n")
        ) as launchctl:
            self.assertEqual(cli._kickstart(force=False), 321)
            launchctl.assert_called_once_with(
                "kickstart", "-p", cli._launchctl_service()
            )
        with mock.patch.object(
            cli, "_run_launchctl", return_value=completed(stdout="322\n")
        ) as launchctl:
            self.assertEqual(cli._kickstart(force=True), 322)
            launchctl.assert_called_once_with(
                "kickstart", "-kp", cli._launchctl_service()
            )
        with mock.patch.object(
            cli, "_run_launchctl", return_value=completed(stdout="not-a-pid")
        ):
            with self.assertRaisesRegex(cli.QuarkLifecycleError, "valid PID"):
                cli._kickstart(force=False)


class FakeResponse:
    status = 200

    def __init__(self, value: object) -> None:
        self.raw = json.dumps(value).encode("utf-8")

    def __enter__(self) -> "FakeResponse":
        return self

    def __exit__(self, *args: object) -> None:
        return None

    def read(self, limit: int) -> bytes:
        return self.raw[:limit]


class FakeOpener:
    def __init__(self, value: object) -> None:
        self.value = value

    def open(self, request: object, timeout: float) -> FakeResponse:
        return FakeResponse(self.value)


class QuarkLifecycleCdpTest(unittest.TestCase):
    def _ready(self, entry: dict[str, object]) -> bool:
        with mock.patch.object(cli, "_pid_is_managed_quark", return_value=True), \
             mock.patch.object(cli, "_cdp_listener_owned_by", return_value=True), \
             mock.patch.object(
                 cli.urllib.request,
                 "build_opener",
                 return_value=FakeOpener([entry]),
             ):
            return cli._cdp_ready(100)

    def test_cdp_ready_requires_quark_page_and_exact_page_socket(self) -> None:
        entry = {
            "type": "page",
            "title": "QuarkCloudDrive",
            "url": "uccd://cloud.quark/clouddrive/renderer/index.html?name=main",
            "webSocketDebuggerUrl": (
                "ws://127.0.0.1:19222/devtools/page/renderer-1"
            ),
        }
        self.assertTrue(self._ready(entry))

        mutations = (
            {**entry, "type": "worker"},
            {**entry, "title": "other", "url": "https://example.test"},
            {**entry, "webSocketDebuggerUrl": "ws://127.0.0.1:19223/devtools/page/x"},
            {**entry, "webSocketDebuggerUrl": "ws://127.0.0.1:19222/devtools/browser/x"},
            {**entry, "webSocketDebuggerUrl": "ws://127.0.0.1:19222/devtools/page/x?q=1"},
        )
        for value in mutations:
            with self.subTest(value=value):
                self.assertFalse(self._ready(value))

    def test_cdp_ready_first_requires_listener_owned_by_managed_pid(self) -> None:
        with mock.patch.object(cli, "_pid_is_managed_quark", return_value=False), \
             mock.patch.object(cli, "_cdp_listener_owned_by") as listener:
            self.assertFalse(cli._cdp_ready(100))
            listener.assert_not_called()
        with mock.patch.object(cli, "_pid_is_managed_quark", return_value=True), \
             mock.patch.object(cli, "_cdp_listener_owned_by", return_value=False), \
             mock.patch.object(cli.urllib.request, "build_opener") as opener:
            self.assertFalse(cli._cdp_ready(100))
            opener.assert_not_called()

    def test_lsof_evidence_must_bind_exact_loopback_port_to_pid(self) -> None:
        evidence = completed(stdout="p100\nf12\nn127.0.0.1:19222\n")
        with mock.patch.object(cli, "_run_command", return_value=evidence):
            self.assertTrue(cli._cdp_listener_owned_by(100))
        wrong = completed(stdout="p101\nn*:19222\n")
        with mock.patch.object(cli, "_run_command", return_value=wrong):
            self.assertFalse(cli._cdp_listener_owned_by(100))


class QuarkLifecycleInstallTest(unittest.TestCase):
    def test_install_refuses_running_quark_without_explicit_replace(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            target = Path(directory) / "agent.plist"
            with mock.patch.object(cli, "_validate_quark_executable"), \
                 mock.patch.object(cli, "_launch_agent_path", return_value=target), \
                 mock.patch.object(cli, "_find_quark_pids", return_value=(100,)), \
                 mock.patch.object(cli, "_write_launch_agent") as write:
                with self.assertRaisesRegex(cli.QuarkLifecycleError, "already running"):
                    cli._install_launch_agent(replace_running=False)
            write.assert_not_called()

    def test_install_refuses_multiple_or_polluted_processes_before_mutation(self) -> None:
        for pids, replaceable, message in (
            ((100, 101), True, "multiple exact"),
            ((100,), False, "unexpected arguments"),
        ):
            with self.subTest(pids=pids), tempfile.TemporaryDirectory() as directory:
                target = Path(directory) / "agent.plist"
                with mock.patch.object(cli, "_validate_quark_executable"), \
                     mock.patch.object(cli, "_launch_agent_path", return_value=target), \
                     mock.patch.object(cli, "_find_quark_pids", return_value=pids), \
                     mock.patch.object(
                         cli, "_pid_has_replaceable_argv", return_value=replaceable
                     ), mock.patch.object(cli, "_write_launch_agent") as write:
                    with self.assertRaisesRegex(cli.QuarkLifecycleError, message):
                        cli._install_launch_agent(replace_running=True)
                write.assert_not_called()

    def test_install_writes_and_lints_before_normal_quit_then_bootstraps(self) -> None:
        events: list[str] = []
        with tempfile.TemporaryDirectory() as directory:
            target = Path(directory) / "agent.plist"
            with mock.patch.object(cli, "_validate_quark_executable"), \
                 mock.patch.object(cli, "_launch_agent_path", return_value=target), \
                 mock.patch.object(cli, "_find_quark_pids", return_value=(100,)), \
                 mock.patch.object(cli, "_pid_has_replaceable_argv", return_value=True), \
                 mock.patch.object(cli, "_service_snapshot", return_value=None), \
                 mock.patch.object(cli, "_prepare_launch_agent_parent"), \
                 mock.patch.object(
                     cli, "_write_launch_agent", side_effect=lambda *_: events.append("write")
                 ), mock.patch.object(
                     cli, "_lint_launch_agent", side_effect=lambda *_: events.append("lint")
                 ), mock.patch.object(
                     cli,
                     "_terminate_exact_quark_pid",
                     side_effect=lambda *_: events.append("normal-quit"),
                 ), mock.patch.object(
                     cli,
                     "_run_launchctl",
                     side_effect=lambda *args: events.append(":".join(args)) or completed(),
                 ), mock.patch.object(
                     cli, "_kickstart", side_effect=lambda **_: events.append("kickstart") or 200
                 ), mock.patch.object(
                     cli,
                     "_wait_for_managed_quark_ready",
                     side_effect=lambda *_: events.append("ready") or 200,
                 ):
                self.assertEqual(
                    cli._install_launch_agent(replace_running=True), target
                )

        self.assertLess(events.index("write"), events.index("normal-quit"))
        self.assertLess(events.index("lint"), events.index("normal-quit"))
        self.assertLess(events.index("normal-quit"), events.index("enable:" + cli._launchctl_service()))
        self.assertIn("bootstrap:" + cli._launchctl_domain() + ":" + str(target), events)
        self.assertEqual(events[-2:], ["kickstart", "ready"])

    def test_failed_first_install_restores_unmanaged_quark_and_plist(self) -> None:
        events: list[str] = []
        with tempfile.TemporaryDirectory() as directory:
            target = Path(directory) / "agent.plist"
            with mock.patch.object(cli, "_validate_quark_executable"), \
                 mock.patch.object(cli, "_launch_agent_path", return_value=target), \
                 mock.patch.object(cli, "_find_quark_pids", return_value=(100,)), \
                 mock.patch.object(cli, "_pid_has_replaceable_argv", return_value=True), \
                 mock.patch.object(
                     cli,
                     "_service_snapshot",
                     side_effect=(None, service_snapshot(target)),
                 ), \
                 mock.patch.object(cli, "_prepare_launch_agent_parent"), \
                 mock.patch.object(cli, "_write_launch_agent"), \
                 mock.patch.object(cli, "_lint_launch_agent"), \
                 mock.patch.object(cli, "_terminate_exact_quark_pid"), \
                 mock.patch.object(cli, "_run_launchctl", return_value=completed()), \
                 mock.patch.object(cli, "_kickstart", return_value=200), \
                 mock.patch.object(
                     cli,
                     "_wait_for_managed_quark_ready",
                     side_effect=cli.QuarkLifecycleError("not ready"),
                 ), mock.patch.object(
                     cli,
                     "_bootout_launch_agent",
                     side_effect=lambda: events.append("bootout") or True,
                 ), mock.patch.object(
                     cli,
                     "_restore_plist",
                     side_effect=lambda *_: events.append("restore-plist"),
                 ), mock.patch.object(
                     cli,
                     "_restore_unmanaged_quark",
                     side_effect=lambda: events.append("restore-app") or 300,
                 ):
                with self.assertRaisesRegex(
                    cli.QuarkLifecycleError, "previous lifecycle state restored"
                ):
                    cli._install_launch_agent(replace_running=True)

        self.assertEqual(events, ["bootout", "restore-plist", "restore-app"])

    def test_loaded_label_collision_is_rejected_before_write_or_quit(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            target = Path(directory) / "agent.plist"
            target.write_bytes(plistlib.dumps(cli._launch_agent_payload()))
            wrong = service_snapshot(Path(directory) / "other.plist")
            with mock.patch.object(cli, "_validate_quark_executable"), \
                 mock.patch.object(cli, "_launch_agent_path", return_value=target), \
                 mock.patch.object(cli, "_find_quark_pids", return_value=(100,)), \
                 mock.patch.object(cli, "_pid_has_replaceable_argv", return_value=True), \
                 mock.patch.object(cli, "_service_snapshot", return_value=wrong), \
                 mock.patch.object(cli, "_write_launch_agent") as write, \
                 mock.patch.object(cli, "_terminate_exact_quark_pid") as terminate:
                with self.assertRaisesRegex(cli.QuarkLifecycleError, "belongs to"):
                    cli._install_launch_agent(replace_running=True)
            write.assert_not_called()
            terminate.assert_not_called()


class QuarkLifecycleOperationTest(unittest.TestCase):
    def test_start_is_idempotent_and_restart_is_normal_by_default(self) -> None:
        target = cli._launch_agent_path()
        snapshot = service_snapshot(target)
        with mock.patch.object(cli, "_service_snapshot", return_value=snapshot), \
             mock.patch.object(cli, "_managed_quark_pid", return_value=100), \
             mock.patch.object(cli, "_wait_for_managed_quark_ready") as ready, \
             mock.patch.object(cli, "_kickstart") as kickstart:
            cli._start_launch_agent()
        kickstart.assert_not_called()
        ready.assert_called_once_with(100)

        events: list[str] = []
        with mock.patch.object(cli, "_service_snapshot", return_value=snapshot), \
             mock.patch.object(cli, "_managed_quark_pid", return_value=100), \
             mock.patch.object(
                 cli, "_terminate_exact_quark_pid", side_effect=lambda *_: events.append("quit")
             ), mock.patch.object(
                 cli, "_kickstart", side_effect=lambda **kwargs: events.append(str(kwargs)) or 200
             ), mock.patch.object(
                 cli, "_wait_for_managed_quark_ready", side_effect=lambda *_: events.append("ready")
             ):
            cli._restart_launch_agent()
        self.assertEqual(events, ["quit", "{'force': False}", "ready"])

    def test_force_restart_is_the_only_forceful_path(self) -> None:
        snapshot = service_snapshot(cli._launch_agent_path())
        with mock.patch.object(cli, "_service_snapshot", return_value=snapshot), \
             mock.patch.object(cli, "_kickstart", return_value=200) as kickstart, \
             mock.patch.object(cli, "_wait_for_managed_quark_ready") as ready, \
             mock.patch.object(cli, "_terminate_exact_quark_pid") as terminate:
            cli._force_restart_launch_agent()
        kickstart.assert_called_once_with(force=True)
        ready.assert_called_once_with(200)
        terminate.assert_not_called()

    def test_uninstall_preserves_plist_when_bootout_fails(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            target = Path(directory) / "agent.plist"
            target.write_bytes(plistlib.dumps(cli._launch_agent_payload()))
            snapshot = service_snapshot(target)
            with mock.patch.object(cli, "_launch_agent_path", return_value=target), \
                 mock.patch.object(cli, "_service_snapshot", return_value=snapshot), \
                 mock.patch.object(
                     cli,
                     "_bootout_launch_agent",
                     side_effect=cli.QuarkLifecycleError("bootout failed"),
                 ):
                with self.assertRaisesRegex(cli.QuarkLifecycleError, "bootout failed"):
                    cli._uninstall_launch_agent()
            self.assertTrue(target.exists())

    def test_status_prints_loaded_service_without_mutation(self) -> None:
        target = cli._launch_agent_path()
        snapshot = service_snapshot(target)
        with mock.patch.object(cli, "_service_snapshot", return_value=snapshot), \
             redirect_stdout(io.StringIO()) as output:
            self.assertEqual(cli._status_launch_agent(), 0)
        self.assertIn("state = running", output.getvalue())

    def test_replace_running_flag_is_install_only(self) -> None:
        with redirect_stderr(io.StringIO()), self.assertRaises(SystemExit) as raised:
            cli.main(["--restart", "--replace-running"])
        self.assertEqual(raised.exception.code, 2)


if __name__ == "__main__":
    unittest.main()
