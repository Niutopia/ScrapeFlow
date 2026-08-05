from __future__ import annotations

import io
import json
import argparse
import base64
import hashlib
from pathlib import Path
import tempfile
from unittest import mock
import unittest
import urllib.error

from engine.scrapeflow.quark_fast_save_bridge import (
    QUARK_DRIVE_API, QUARK_SHARE_API, QuarkMagnetInDoubtError,
    QuarkMagnetOfflineBridge, QuarkNativeHelperTransport, QuarkSession,
)
from engine.scrapeflow.quark_native_helper import (
    NativeHelperError, NativeHelperService, NativeRequestInDoubt,
    NativeRuntimeUnavailable, QuarkCdpRuntime, QuarkNativeRequestDriver,
    SubmitJournal,
    _WebSocket, canonical_request_id, validate_native_request,
)
from engine.tools import quark_native_helper as helper_cli


class JsonResponse(io.BytesIO):
    def __enter__(self):
        return self

    def __exit__(self, *_args):
        self.close()


def native_payload(path="/offline/download/submit"):
    endpoint = QUARK_SHARE_API + path
    params = {"pr": "ucpro"}
    body = {"token": "fixture-token", "selected_files": [7]}
    return {
        "version": 1, "method": "POST", "endpoint": endpoint,
        "params": params, "body": body, "cookie": "SECRET_COOKIE",
        "request_id": canonical_request_id("POST", endpoint, params, body),
    }


class NativeHelperTests(unittest.TestCase):
    def test_websocket_handshake_accepts_case_insensitive_bytes_header(self):
        class FakeSocket:
            def __init__(self):
                self.response = b""

            def settimeout(self, _timeout):
                return None

            def sendall(self, request):
                key = request.split(b"Sec-WebSocket-Key: ", 1)[1].split(b"\r\n", 1)[0]
                accept = base64.b64encode(hashlib.sha1(
                    key + b"258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
                ).digest())
                self.response = (
                    b"HTTP/1.1 101 Switching Protocols\r\n"
                    b"Sec-WebSocket-Accept: " + accept + b"\r\n\r\n"
                )

            def recv(self, _length):
                response, self.response = self.response, b""
                return response

            def close(self):
                return None

        with mock.patch("socket.create_connection", return_value=FakeSocket()):
            websocket = _WebSocket("ws://127.0.0.1:19222/devtools/page/test")
        websocket.close()

    def test_health_probe_checks_native_encrypt_and_decrypt_primitives(self):
        runtime = QuarkCdpRuntime("http://127.0.0.1:19222/json/list")
        with mock.patch.object(runtime, "quark_pids", return_value=[77]), \
                mock.patch.object(runtime, "_evaluate", return_value="ready") as evaluate:
            runtime.probe()
        expression = evaluate.call_args.args[0]
        self.assertIn("wsg.encrypt", expression)
        self.assertIn("encryptOrDecrypt", expression)
        with mock.patch.object(runtime, "quark_pids", return_value=[77]), \
                mock.patch.object(runtime, "_evaluate", return_value="missing"):
            with self.assertRaises(NativeRuntimeUnavailable):
                runtime.probe()

    def test_runtime_exposes_no_active_launch_or_restart_capability(self):
        self.assertFalse(hasattr(QuarkCdpRuntime, "_launch_quark"))
        self.assertFalse(hasattr(QuarkCdpRuntime, "_graceful_quit_quark"))
        self.assertFalse(hasattr(QuarkCdpRuntime, "_recover_cdp"))

    def test_health_endpoint_reports_idle_helper_when_quark_is_absent(self):
        runtime = mock.Mock(spec=QuarkCdpRuntime)
        runtime.passive_probe.side_effect = NativeRuntimeUnavailable(
            "quark_not_running"
        )
        runtime.probe.side_effect = AssertionError("active probe is forbidden")
        driver = mock.Mock()
        driver.runtime = runtime
        service = NativeHelperService(driver, mock.Mock())
        handler_type = helper_cli._handler(service, "x" * 24)
        handler = object.__new__(handler_type)
        handler.path = "/health"
        handler._json = mock.Mock()

        handler.do_GET()

        runtime.passive_probe.assert_called_once_with()
        runtime.probe.assert_not_called()
        handler._json.assert_called_once()
        status, payload = handler._json.call_args.args
        self.assertEqual(status, 200)
        self.assertTrue(payload["helper_alive"])
        self.assertFalse(payload["existing_quark_connected"])
        self.assertEqual(payload["runtime"], "idle")
        self.assertEqual(payload["readiness"], "waiting-for-existing-quark")
        self.assertNotIn("runtime_error", payload)

    def test_health_endpoint_does_not_disguise_helper_fault_as_idle(self):
        runtime = mock.Mock(spec=QuarkCdpRuntime)
        runtime.passive_probe.side_effect = NativeHelperError(
            "internal_driver_invariant_failed"
        )
        driver = mock.Mock()
        driver.runtime = runtime
        service = NativeHelperService(driver, mock.Mock())
        handler_type = helper_cli._handler(service, "x" * 24)
        handler = object.__new__(handler_type)
        handler.path = "/health/passive"
        handler._json = mock.Mock()

        handler.do_GET()

        status, payload = handler._json.call_args.args
        self.assertEqual(status, 503)
        self.assertEqual(payload["status"], "error")
        self.assertEqual(payload["runtime"], "error")
        self.assertEqual(payload["readiness"], "helper-error")
        self.assertNotIn("runtime_error", payload)
        self.assertNotIn("internal_driver_invariant_failed", json.dumps(payload))

    def test_health_endpoint_distinguishes_existing_quark_connection(self):
        runtime = mock.Mock(spec=QuarkCdpRuntime)
        runtime.passive_probe.return_value = {"quark_pids": [77], "cdp_port": 19222}
        driver = mock.Mock()
        driver.runtime = runtime
        journal = mock.Mock()
        journal.status_counts.return_value = {
            "complete": 0, "in_doubt": 0, "failed": 0,
        }
        service = NativeHelperService(driver, journal)
        handler_type = helper_cli._handler(service, "x" * 24)
        handler = object.__new__(handler_type)
        handler.path = "/health/passive"
        handler._json = mock.Mock()

        handler.do_GET()

        status, payload = handler._json.call_args.args
        self.assertEqual(status, 200)
        self.assertTrue(payload["helper_alive"])
        self.assertTrue(payload["existing_quark_connected"])
        self.assertEqual(payload["runtime"], "connected")
        self.assertEqual(payload["quark_pids"], [77])

    def test_request_fails_closed_without_starting_or_activating_quark(self):
        runtime = mock.Mock(spec=QuarkCdpRuntime)
        runtime.passive_probe.side_effect = NativeRuntimeUnavailable(
            "quark_not_running"
        )
        runtime.probe.side_effect = AssertionError("active probe is forbidden")
        driver = QuarkNativeRequestDriver(runtime)
        with tempfile.TemporaryDirectory() as directory:
            service = NativeHelperService(
                driver, SubmitJournal(Path(directory) / "journal.json"),
            )
            with self.assertRaisesRegex(
                NativeRuntimeUnavailable, "quark_not_running",
            ):
                service.execute(native_payload("/offline/download/parse"))

        runtime.passive_probe.assert_called_once_with()
        runtime.probe.assert_not_called()

    def test_probe_fails_closed_when_quark_is_absent(self):
        runtime = QuarkCdpRuntime("http://127.0.0.1:19222/json/list")
        with mock.patch.object(runtime, "quark_pids", return_value=[]), \
                mock.patch.object(runtime, "_probe_native_once") as native_probe:
            with self.assertRaisesRegex(
                NativeRuntimeUnavailable, "quark_not_running",
            ):
                runtime.probe()
        native_probe.assert_not_called()

    def test_cdp_prefers_main_renderer_over_vip_window(self):
        runtime = QuarkCdpRuntime("http://127.0.0.1:19222/json/list")
        rows = [
            {"type": "page", "title": "收银台",
             "url": "uccd://cloud.quark/clouddrive/renderer/vip.html",
             "webSocketDebuggerUrl": "ws://127.0.0.1/vip"},
            {"type": "page", "title": "首页",
             "url": "uccd://cloud.quark/clouddrive/renderer/index.html?name=main#/list/all",
             "webSocketDebuggerUrl": "ws://127.0.0.1/main"},
        ]
        with mock.patch.object(runtime._http, "open", return_value=JsonResponse(
            json.dumps(rows).encode("utf-8")
        )):
            self.assertEqual(runtime._target(), "ws://127.0.0.1/main")

    def test_launch_agent_generator_uses_absolute_paths_and_separate_0600_token(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            args = argparse.Namespace(
                token_file=root / "state/token", port=18765,
                cdp_url="http://127.0.0.1:19222/json/list",
                state_dir=root / "state",
                quark_binary=Path("/Applications/QuarkCloudDrive.app/Contents/MacOS/QuarkCloudDrive"),
            )
            target = root / "com.scrapeflow.quark-native-helper.plist"
            launchctl = mock.Mock(return_value=mock.Mock(returncode=0))
            with mock.patch.object(
                helper_cli, "_launch_agent_path", return_value=target,
            ), mock.patch.object(
                helper_cli.subprocess, "run", launchctl,
            ), mock.patch.object(helper_cli, "_wait_helper_alive") as ready:
                self.assertEqual(helper_cli._install_launch_agent(args), target)
            payload = __import__("plistlib").loads(target.read_bytes())
            arguments = payload["ProgramArguments"]
            self.assertNotIn("--launch-quark", arguments)
            self.assertNotIn("--restart-quark-without-cdp", arguments)
            self.assertNotIn("--activate-quark", arguments)
            self.assertNotIn("--allow-ui-activation", arguments)
            self.assertIn("--token-file", arguments)
            self.assertTrue(Path(arguments[0]).is_absolute())
            self.assertTrue(Path(arguments[1]).is_absolute())
            self.assertEqual(args.token_file.stat().st_mode & 0o777, 0o600)
            self.assertEqual(target.stat().st_mode & 0o777, 0o600)
            self.assertNotIn(args.token_file.read_text().strip(), target.read_text())
            self.assertEqual(payload["KeepAlive"], {"Crashed": True})
            self.assertNotIn("SuccessfulExit", payload["KeepAlive"])
            domain = f"gui/{helper_cli.os.getuid()}"
            service = f"{domain}/{helper_cli.LAUNCH_AGENT_LABEL}"
            self.assertEqual(launchctl.call_args_list, [
                mock.call(
                    ["launchctl", "bootout", service], check=False, text=True,
                    stdout=helper_cli.subprocess.PIPE,
                    stderr=helper_cli.subprocess.PIPE,
                ),
                mock.call(
                    ["launchctl", "bootstrap", domain, str(target)],
                    check=True, text=True, stdout=helper_cli.subprocess.PIPE,
                    stderr=helper_cli.subprocess.PIPE,
                ),
                mock.call(
                    ["launchctl", "kickstart", "-k", service],
                    check=True, text=True, stdout=helper_cli.subprocess.PIPE,
                    stderr=helper_cli.subprocess.PIPE,
                ),
            ])
            ready.assert_called_once_with(18765)

    def test_launch_agent_is_unconditionally_passive(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            args = argparse.Namespace(
                token_file=root / "state/token", port=18765,
                cdp_url="http://127.0.0.1:19222/json/list",
                state_dir=root / "state",
            )
            target = root / "com.scrapeflow.quark-native-helper.plist"
            with mock.patch.object(
                helper_cli, "_launch_agent_path", return_value=target,
            ), mock.patch.object(
                helper_cli, "_run_launchctl", return_value=mock.Mock(returncode=0),
            ), mock.patch.object(helper_cli, "_wait_helper_alive") as ready:
                helper_cli._install_launch_agent(args)

            payload = __import__("plistlib").loads(target.read_bytes())
            self.assertNotIn("--passive-startup", payload["ProgramArguments"])
            self.assertNotIn("--launch-quark", payload["ProgramArguments"])
            self.assertNotIn(
                "--restart-quark-without-cdp", payload["ProgramArguments"],
            )
            self.assertEqual(payload["KeepAlive"], {"Crashed": True})
            ready.assert_called_once_with(18765)

    def test_cli_rejects_all_legacy_active_quark_options_before_action(self):
        for option in (
            "--launch-quark", "--restart-quark-without-cdp",
            "--activate-quark", "--allow-ui-activation",
        ):
            with self.subTest(option=option), mock.patch.object(
                helper_cli.sys, "argv",
                ["quark-native-helper", "--install-launch-agent", option],
            ), mock.patch.object(helper_cli, "_install_launch_agent") as install:
                with self.assertRaises(SystemExit) as raised:
                    helper_cli.main()
                self.assertEqual(raised.exception.code, 2)
                install.assert_not_called()

    def test_install_migrates_legacy_active_plist_to_idle_helper(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            args = argparse.Namespace(
                token_file=root / "state/token", port=18765,
                cdp_url="http://127.0.0.1:19222/json/list",
                state_dir=root / "state",
            )
            target = root / "com.scrapeflow.quark-native-helper.plist"
            target.write_bytes(__import__("plistlib").dumps({
                "Label": helper_cli.LAUNCH_AGENT_LABEL,
                "ProgramArguments": [
                    "/legacy/python", "/legacy/helper.py", "--launch-quark",
                    "--restart-quark-without-cdp", "--allow-ui-activation",
                ],
                "KeepAlive": {"SuccessfulExit": False},
            }))
            events = []

            def launchctl(*arguments, **kwargs):
                events.append(arguments)
                return mock.Mock(returncode=0)

            idle = {
                "status": "ok", "service": "quark-native-helper",
                "helper_alive": True, "existing_quark_connected": False,
                "runtime": "idle", "readiness": "waiting-for-existing-quark",
                "build_id": "fixture", "journal": {},
            }
            with mock.patch.object(
                helper_cli, "_launch_agent_path", return_value=target,
            ), mock.patch.object(
                helper_cli, "_run_launchctl", side_effect=launchctl,
            ), mock.patch.object(
                helper_cli, "_wait_helper_alive", return_value=idle,
            ) as ready:
                helper_cli._install_launch_agent(args)

            payload = __import__("plistlib").loads(target.read_bytes())
            arguments = payload["ProgramArguments"]
            self.assertEqual(events[0], (
                "bootout", helper_cli._launchctl_service(),
            ))
            self.assertEqual(payload["KeepAlive"], {"Crashed": True})
            self.assertNotIn("SuccessfulExit", payload["KeepAlive"])
            self.assertFalse(any(
                marker in arguments for marker in (
                    "--launch-quark", "--restart-quark-without-cdp",
                    "--activate-quark", "--allow-ui-activation",
                )
            ))
            ready.assert_called_once_with(18765)

    def test_launch_agent_uninstall_boots_out_before_unlink(self):
        with tempfile.TemporaryDirectory() as directory:
            target = Path(directory) / "helper.plist"
            target.write_text("fixture", encoding="utf-8")
            events = []
            original_unlink = Path.unlink

            def launchctl(*arguments, **kwargs):
                events.append(("launchctl", arguments, kwargs))
                return mock.Mock(returncode=0)

            def unlink(path, *, missing_ok=False):
                events.append(("unlink", path, missing_ok))
                return original_unlink(path, missing_ok=missing_ok)

            with mock.patch.object(
                helper_cli, "_launch_agent_path", return_value=target,
            ), mock.patch.object(
                helper_cli, "_run_launchctl", side_effect=launchctl,
            ), mock.patch.object(Path, "unlink", autospec=True, side_effect=unlink):
                helper_cli._uninstall_launch_agent()
            self.assertEqual(events[0][0], "launchctl")
            self.assertEqual(events[0][1], (
                "bootout", helper_cli._launchctl_service(),
            ))
            self.assertEqual(events[0][2], {"check": False})
            self.assertEqual(events[1], ("unlink", target, True))
            self.assertFalse(target.exists())

    def test_launch_agent_wait_accepts_idle_current_helper_build(self):
        build_id = "current-build"
        health = mock.Mock(side_effect=[
            urllib.error.URLError("starting"),
            {
                "status": "ok", "service": "quark-native-helper",
                "helper_alive": True, "existing_quark_connected": False,
                "runtime": "idle", "readiness": "waiting-for-existing-quark",
                "build_id": "old-build", "journal": {},
            },
            {
                "status": "ok", "service": "quark-native-helper",
                "helper_alive": True, "existing_quark_connected": False,
                "runtime": "idle", "readiness": "waiting-for-existing-quark",
                "build_id": build_id, "journal": {"complete": 1, "in_doubt": 0},
            },
        ])
        with mock.patch.object(
            helper_cli, "_fetch_helper_health", health,
        ), mock.patch.object(
            helper_cli, "_helper_build_id", return_value=build_id,
        ), mock.patch.object(helper_cli.time, "sleep") as sleep:
            result = helper_cli._wait_helper_alive(18765, timeout=2)
        self.assertEqual(result["build_id"], build_id)
        self.assertEqual(health.call_count, 3)
        self.assertEqual(sleep.call_count, 2)

    def test_launch_agent_helper_alive_timeout_is_bounded(self):
        with mock.patch.object(
            helper_cli.time, "monotonic", side_effect=[0.0, 0.1, 0.9, 1.1],
        ), mock.patch.object(
            helper_cli, "_fetch_helper_health",
            side_effect=urllib.error.URLError("not listening"),
        ), mock.patch.object(
            helper_cli, "_helper_build_id", return_value="current-build",
        ), mock.patch.object(helper_cli.time, "sleep"):
            with self.assertRaisesRegex(NativeHelperError, "within 1s"):
                helper_cli._wait_helper_alive(18765, timeout=1)

    def test_request_validation_rejects_non_quark_and_tampered_identity(self):
        payload = native_payload()
        payload["endpoint"] = "https://evil.invalid/offline/download/submit"
        with self.assertRaisesRegex(NativeHelperError, "allowlist"):
            validate_native_request(payload)
        payload = native_payload(); payload["body"]["selected_files"] = [8]
        with self.assertRaisesRegex(NativeHelperError, "request id"):
            validate_native_request(payload)

    def test_request_validation_rejects_active_process_or_ui_control_fields(self):
        for field in (
            "launch_quark", "restart_quark", "restart_quark_without_cdp",
            "activate_quark", "activate_ui", "allow_ui_activation",
        ):
            with self.subTest(field=field):
                payload = native_payload("/offline/download/parse")
                payload[field] = True
                with self.assertRaisesRegex(NativeHelperError, "permanently forbidden"):
                    validate_native_request(payload)

    def test_submit_journal_replays_result_without_repeating_mutation(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "journal.json"
            journal = SubmitJournal(path)
            operation = mock.Mock(return_value={"code": 0, "data": {"task_id": "t1"}})
            first = journal.run("a" * 64, operation)
            second = SubmitJournal(path).run("a" * 64, operation)
            self.assertEqual(first, second)
            operation.assert_called_once()
            self.assertEqual(path.stat().st_mode & 0o777, 0o600)
            self.assertNotIn("SECRET_COOKIE", path.read_text())
            self.assertEqual(SubmitJournal(path).status_counts(), {
                "complete": 1, "in_doubt": 0, "failed": 0,
            })

    def test_submit_identity_survives_ephemeral_parse_token_refresh(self):
        first = native_payload(); second = native_payload()
        second["body"]["token"] = "refreshed-token"
        second["request_id"] = canonical_request_id(
            second["method"], second["endpoint"], second["params"], second["body"],
        )
        self.assertEqual(first["request_id"], second["request_id"])
        self.assertEqual(validate_native_request(second)["body"]["token"], "refreshed-token")

    def test_submit_journal_never_repeats_in_doubt_mutation(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "journal.json"
            journal = SubmitJournal(path)
            with self.assertRaisesRegex(RuntimeError, "lost reply"):
                journal.run("b" * 64, lambda: (_ for _ in ()).throw(RuntimeError("lost reply")))
            with self.assertRaises(NativeRequestInDoubt):
                SubmitJournal(path).run("b" * 64, mock.Mock())
            self.assertEqual(SubmitJournal(path).status_counts(), {
                "complete": 0, "in_doubt": 1, "failed": 0,
            })

    def test_explicit_failed_reconciliation_is_audited_and_allows_retry(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "journal.json"
            journal = SubmitJournal(path)
            request_id = "b" * 64
            with self.assertRaisesRegex(RuntimeError, "lost reply"):
                journal.run(
                    request_id,
                    lambda: (_ for _ in ()).throw(RuntimeError("lost reply")),
                )
            evidence = journal.reconcile_failed(request_id, {
                "resolution": "confirmed_failed",
                "task_id": "c" * 32,
                "task_status": -1,
                "task_name_sha256": "d" * 64,
                "destination": "/quark/影视/ScrapeFlow/补源/fixture",
                "destination_empty": True,
                "source_job_id": "e" * 12,
                "observed_at": "2026-07-31T10:00:00+00:00",
            })
            self.assertTrue(Path(evidence["evidence_path"]).exists())
            self.assertEqual(SubmitJournal(path).status_counts(), {
                "complete": 0, "in_doubt": 0, "failed": 1,
            })
            result = SubmitJournal(path).run(
                request_id, lambda: {"code": 0, "data": {"task_id": "retry"}},
            )
            self.assertEqual(result["data"]["task_id"], "retry")
            self.assertEqual(SubmitJournal(path).status_counts(), {
                "complete": 1, "in_doubt": 0, "failed": 0,
            })

    def test_service_journals_submit_but_not_progress(self):
        with tempfile.TemporaryDirectory() as directory:
            driver = mock.Mock()
            driver.request.return_value = {"code": 0, "data": {"task_id": "t1"}}
            service = NativeHelperService(driver, SubmitJournal(Path(directory) / "j.json"))
            submit = native_payload()
            service.execute(submit); service.execute(submit)
            self.assertEqual(driver.request.call_count, 1)
            progress = native_payload("/offline/save_to/progress")
            service.execute(progress); service.execute(progress)
            self.assertEqual(driver.request.call_count, 3)

    def test_service_proves_readiness_before_submit_journal_boundary(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "journal.json"
            driver = mock.Mock(spec=QuarkNativeRequestDriver)
            driver.passive_ensure_ready.side_effect = NativeRuntimeUnavailable(
                "quark_running_without_cdp"
            )
            service = NativeHelperService(driver, SubmitJournal(path))

            with self.assertRaisesRegex(
                NativeRuntimeUnavailable, "quark_running_without_cdp",
            ):
                service.execute(native_payload())

            driver.request.assert_not_called()
            self.assertFalse(path.exists())
            self.assertEqual(service.journal.status_counts(), {
                "complete": 0, "in_doubt": 0, "failed": 0,
            })

    def test_passive_service_request_never_calls_recovering_readiness(self):
        with tempfile.TemporaryDirectory() as directory:
            driver = mock.Mock(spec=QuarkNativeRequestDriver)
            driver.passive_ensure_ready.return_value = {"quark_pids": [77]}
            driver.request.return_value = {"status": 200, "code": 0, "data": {}}
            service = NativeHelperService(
                driver, SubmitJournal(Path(directory) / "journal.json"),
            )
            payload = native_payload("/offline/download/parse")
            service.execute(payload)
            driver.passive_ensure_ready.assert_called_once_with()

    def test_passive_runtime_probe_never_launches_or_restarts_quark(self):
        runtime = QuarkCdpRuntime("http://127.0.0.1:19222/json/list")
        with mock.patch.object(runtime, "quark_pids", return_value=[77]), \
                mock.patch.object(runtime, "_probe_native_once"):
            evidence = runtime.passive_probe()
        self.assertEqual(evidence["quark_pids"], [77])
        self.assertFalse(hasattr(runtime, "_launch_quark"))
        self.assertFalse(hasattr(runtime, "_graceful_quit_quark"))

    def test_readiness_failure_never_masks_existing_in_doubt_submit(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "journal.json"
            journal = SubmitJournal(path)
            with self.assertRaisesRegex(RuntimeError, "lost reply"):
                journal.run(
                    native_payload()["request_id"],
                    lambda: (_ for _ in ()).throw(RuntimeError("lost reply")),
                )
            driver = mock.Mock(spec=QuarkNativeRequestDriver)
            driver.passive_ensure_ready.side_effect = NativeRuntimeUnavailable(
                "quark_running_without_cdp"
            )
            service = NativeHelperService(driver, SubmitJournal(path))

            with self.assertRaises(NativeRequestInDoubt):
                service.execute(native_payload())

            driver.passive_ensure_ready.assert_not_called()
            driver.request.assert_not_called()

    def test_bridge_end_to_end_allows_destination_parse_submit_and_progress(self):
        with tempfile.TemporaryDirectory() as directory:
            driver = mock.Mock()
            driver.request.side_effect = [
                {"status": 200, "code": 0, "data": {"list": [{
                    "fid": "inbox-fid", "file_name": "inbox", "file": False,
                }]}},
                {"status": 200, "code": 0, "data": {"list": [{
                    "fid": "target-fid", "file_name": "fixture", "file": False,
                }]}},
                {"status": 200, "code": 0, "data": {
                    "token": "parse-token", "files": [{
                        "file_no": 107, "path": "Example/Example.S01E01.mkv", "size": 123,
                    }],
                }},
                {"status": 200, "code": 0, "data": {"task_id": "offline-task"}},
                {"status": 200, "code": 0, "data": [{
                    "task_id": "offline-task", "status": 2,
                }]},
            ]
            service = NativeHelperService(driver, SubmitJournal(Path(directory) / "j.json"))

            class InProcessTransport:
                supports_wsg = True

                def request(self, method, endpoint, *, params, body, cookie):
                    return service.execute({
                        "method": method, "endpoint": endpoint, "params": dict(params),
                        "body": body, "cookie": cookie,
                        "request_id": canonical_request_id(method, endpoint, params, body),
                    })

            selection = {
                "provider": "quark_magnet", "release_name": "Example S01E01",
                "locator": "quark_magnet:0123456789abcdef0123456789abcdef01234567",
                "selected_gap_ids": ["S01E01"], "acquisition": {
                    "kind": "quark_magnet_offline",
                    "magnet_url": "magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567",
                    "expected_files": [{
                        "torrent_index": 7, "path": "Example/Example.S01E01.mkv",
                        "size": 123, "gap_ids": ["S01E01"],
                    }],
                },
            }
            result = QuarkMagnetOfflineBridge(
                InProcessTransport(), sleep=mock.Mock(),
            ).execute(
                selection, "/quark/inbox/fixture",
                QuarkSession("/quark", "root", "SECRET_COOKIE"),
            )
        self.assertEqual(result["task_id"], "offline-task")
        endpoints = [call.args[0]["endpoint"] for call in driver.request.call_args_list]
        self.assertEqual(endpoints, [
            QUARK_DRIVE_API + "/file/sort", QUARK_DRIVE_API + "/file/sort",
            QUARK_SHARE_API + "/offline/download/parse",
            QUARK_SHARE_API + "/offline/download/submit",
            QUARK_SHARE_API + "/offline/save_to/progress",
        ])
        parse_body = driver.request.call_args_list[2].args[0]["body"]
        self.assertEqual(parse_body, {
            "url": selection["acquisition"]["magnet_url"],
            "parse_mode": 0, "cookie": "", "entry": "download",
            "req_info": {"method": "", "body": "", "is_multipart": False},
            "conflict_mode": 4, "auto_download": False,
            "support_v2_play": True,
        })
        submit_body = driver.request.call_args_list[3].args[0]["body"]
        self.assertEqual(submit_body["selected_files"], [107])

    def test_transport_uses_bearer_and_never_returns_cookie(self):
        calls = []

        def opener(request, **kwargs):
            calls.append((request, kwargs))
            return JsonResponse(json.dumps({
                "status": "ok", "result": {"code": 0, "data": {"task_id": "t1"}},
            }).encode())

        transport = QuarkNativeHelperTransport(
            "http://127.0.0.1:18765", "x" * 24, opener=opener,
        )
        result = transport.request(
            "POST", QUARK_SHARE_API + "/offline/download/submit",
            params={"pr": "ucpro"}, body={"token": "fixture"},
            cookie="SECRET_COOKIE",
        )
        self.assertEqual(result["data"]["task_id"], "t1")
        request = calls[0][0]
        self.assertEqual(request.get_header("Authorization"), "Bearer " + "x" * 24)
        self.assertNotIn("SECRET_COOKIE", json.dumps(result))

    def test_transport_maps_helper_in_doubt_to_non_fallback_error(self):
        error = urllib.error.HTTPError(
            "http://127.0.0.1:18765/v1/quark/request", 409, "Conflict", {},
            io.BytesIO(json.dumps({
                "status": "error", "in_doubt": True, "error": "in doubt",
            }).encode()),
        )
        transport = QuarkNativeHelperTransport(
            "http://127.0.0.1:18765", "x" * 24,
            opener=mock.Mock(side_effect=error),
        )
        with self.assertRaises(QuarkMagnetInDoubtError) as raised:
            transport.request(
                "POST", QUARK_SHARE_API + "/offline/download/submit",
                params={}, body={}, cookie="SECRET_COOKIE",
            )
        self.assertEqual(raised.exception.failure_stage, "quark_magnet_submit_in_doubt")


if __name__ == "__main__":
    unittest.main()
