"""No-write contract tests for the AList/aria2 offline preflight."""

from __future__ import annotations

import json
import unittest

from local.scrapeflow_api.alist_offline_readiness import (
    ARIA2_RPC_EXPECTED_DIR,
    alist_offline_readiness,
    unverified_alist_offline_readiness,
    validate_aria2_rpc_url,
)


class ReadOnlyAList:
    """A deliberately small double that exposes no mutation methods."""

    def __init__(
        self,
        *,
        tools: list[str] | None = None,
        aria2_dir: str = ARIA2_RPC_EXPECTED_DIR,
        storages: list[dict[str, object]] | None = None,
        settings_error: Exception | None = None,
    ) -> None:
        self.token: str | None = None
        self.tools = list(["aria2"] if tools is None else tools)
        self.aria2_dir = aria2_dir
        self.storages = list(storages or [
            {
                "mount_path": "/quark",
                "driver": "Quark",
                "disabled": False,
                "status": "work",
            },
        ])
        self.settings_error = settings_error
        self.calls: list[str] = []
        self.secret = "offline-rpc-secret-not-public"

    def login(self) -> None:
        self.calls.append("login")
        self.token = "in-memory-alist-token"

    def offline_download_tools(self) -> list[str]:
        self.calls.append("offline_download_tools")
        return list(self.tools)

    def admin_storages(self) -> list[dict[str, object]]:
        self.calls.append("admin_storages")
        return [dict(row) for row in self.storages]

    def offline_download_undone(self) -> list[dict[str, object]]:
        self.calls.append("offline_download_undone")
        return []

    def offline_download_done(self) -> list[dict[str, object]]:
        self.calls.append("offline_download_done")
        return []

    def offline_download_aria2_settings(self) -> dict[str, str]:
        self.calls.append("offline_download_aria2_settings")
        if self.settings_error is not None:
            raise self.settings_error
        return {
            "aria2_uri": "http://offline-aria2:6800/jsonrpc",
            "aria2_secret": self.secret,
        }

    def list(self, path: str, refresh: bool = False) -> list[dict[str, object]]:
        self.calls.append(f"list:{path}:{refresh}")
        if path != "/quark/影视/ScrapeFlow":
            raise AssertionError(f"unexpected list path: {path}")
        return [{"name": "补源", "is_dir": True}]


class AlistOfflineReadinessTests(unittest.TestCase):
    def test_ready_report_verifies_every_read_only_link_without_submission(self) -> None:
        alist = ReadOnlyAList()
        rpc_calls: list[tuple[str, str, str]] = []

        def rpc(url: str, secret: str, method: str) -> dict[str, str]:
            rpc_calls.append((url, secret, method))
            if method == "aria2.getVersion":
                return {"version": "1.36.0"}
            if method == "aria2.getGlobalOption":
                return {"dir": ARIA2_RPC_EXPECTED_DIR}
            raise AssertionError(f"unexpected RPC method: {method}")

        report = alist_offline_readiness(
            alist,
            media_root="/quark/影视",
            environ={"SCRAPEFLOW_ALIST_OFFLINE_ARIA2_HOST": "offline-aria2"},
            rpc_call=rpc,
        )

        self.assertEqual(report["status"], "ready")
        self.assertTrue(report["configured"])
        self.assertTrue(report["verified"])
        self.assertTrue(report["read_only"])
        self.assertEqual(report["checks"]["tool"]["name"], "aria2")
        self.assertTrue(report["checks"]["transfer"]["route_verified"])
        self.assertFalse(report["checks"]["transfer"]["end_to_end_transfer_proven"])
        self.assertEqual(report["checks"]["transfer"]["staging_root"], "/quark/影视/ScrapeFlow/补源")
        self.assertEqual(
            report["checks"]["transfer"]["offline_sibling"],
            "/quark/影视/ScrapeFlow/补源__offline__",
        )
        self.assertEqual(report["checks"]["transfer"]["storage_mount"], "/quark")
        self.assertEqual(report["checks"]["transfer"]["upload_capability"], "reviewed_driver")
        self.assertEqual(
            [method for _url, _secret, method in rpc_calls],
            ["aria2.getVersion", "aria2.getGlobalOption"],
        )
        self.assertEqual(
            set(alist.calls),
            {
                "login",
                "offline_download_tools",
                "admin_storages",
                "offline_download_undone",
                "offline_download_done",
                "offline_download_aria2_settings",
                "list:/quark/影视/ScrapeFlow:True",
            },
        )
        # No task/file mutation method exists on the fake, and credentials
        # fetched for the in-memory RPC call cannot leak into its report.
        serialized = json.dumps(report, ensure_ascii=False)
        self.assertNotIn(alist.secret, serialized)
        self.assertNotIn("in-memory-alist-token", serialized)

    def test_missing_tool_fails_closed_before_a_real_transfer(self) -> None:
        alist = ReadOnlyAList(tools=[])

        report = alist_offline_readiness(
            alist,
            media_root="/quark/影视",
            rpc_call=lambda _url, _secret, method: (
                {"version": "1.36.0"}
                if method == "aria2.getVersion"
                else {"dir": ARIA2_RPC_EXPECTED_DIR}
            ),
        )

        self.assertEqual(report["status"], "not_ready")
        self.assertFalse(report["verified"])
        self.assertFalse(report["checks"]["tool"]["verified"])

    def test_blank_or_whitespace_aria2_secret_fails_before_rpc_and_never_leaks(self) -> None:
        for secret in ("", " \t "):
            with self.subTest(secret=repr(secret)):
                alist = ReadOnlyAList()
                alist.secret = secret
                rpc_calls: list[tuple[str, str, str]] = []

                def rpc(url: str, supplied_secret: str, method: str) -> dict[str, str]:
                    rpc_calls.append((url, supplied_secret, method))
                    raise AssertionError("blank AList secret must not reach aria2 RPC")

                report = alist_offline_readiness(
                    alist,
                    media_root="/quark/影视",
                    rpc_call=rpc,
                )

                serialized = json.dumps(report, ensure_ascii=False)
                self.assertEqual(report["status"], "not_ready")
                self.assertFalse(report["verified"])
                self.assertFalse(report["checks"]["aria2"]["verified"])
                self.assertEqual(rpc_calls, [])
                self.assertNotIn("aria2_secret", serialized)

    def test_wrong_aria2_temp_directory_fails_closed_and_redacts_secret(self) -> None:
        alist = ReadOnlyAList(aria2_dir="/wrong")

        def rpc(_url: str, secret: str, method: str) -> dict[str, str]:
            if method == "aria2.getVersion":
                return {"version": "1.36.0"}
            raise RuntimeError(f"secret={secret}; wrong aria2 directory")

        report = alist_offline_readiness(
            alist,
            media_root="/quark/影视",
            rpc_call=rpc,
        )

        self.assertEqual(report["status"], "not_ready")
        self.assertFalse(report["checks"]["aria2"]["verified"])
        self.assertNotIn(alist.secret, json.dumps(report, ensure_ascii=False))

    def test_remote_aria2_setting_error_never_reflects_a_secret(self) -> None:
        leaked = "readiness-secret-must-not-appear"
        alist = ReadOnlyAList(
            settings_error=RuntimeError(f"aria2_secret={leaked}; remote rejected request"),
        )

        report = alist_offline_readiness(alist, media_root="/quark/影视")

        serialized = json.dumps(report, ensure_ascii=False)
        self.assertFalse(report["verified"])
        self.assertFalse(report["checks"]["aria2"]["verified"])
        self.assertNotIn(leaked, serialized)
        self.assertIn("<redacted>", serialized)

    def test_unverified_startup_shape_and_rpc_url_boundary_are_fail_closed(self) -> None:
        report = unverified_alist_offline_readiness()
        self.assertEqual(report["status"], "unverified")
        self.assertFalse(report["verified"])
        self.assertTrue(report["read_only"])

        for invalid in (
            "http://example.invalid:6800/jsonrpc",
            "http://offline-aria2:6801/jsonrpc",
            "http://user:password@offline-aria2:6800/jsonrpc",
            "http://offline-aria2:6800/jsonrpc?token=leak",
            "http://localhost:6800/jsonrpc",
            "http://127.0.0.1:6800/jsonrpc",
            "http://[::1]:6800/jsonrpc",
        ):
            with self.subTest(invalid=invalid):
                with self.assertRaisesRegex(Exception, "aria2 RPC"):
                    validate_aria2_rpc_url(invalid)

        with self.assertRaisesRegex(Exception, "Compose 服务 offline-aria2"):
            validate_aria2_rpc_url(
                "http://offline-aria2:6800/jsonrpc",
                environ={"SCRAPEFLOW_ALIST_OFFLINE_ARIA2_HOST": "localhost"},
            )

    def test_transfer_mount_uses_derived_staging_path_not_broad_media_root(self) -> None:
        alist = ReadOnlyAList(storages=[
            {"mount_path": "/quark", "driver": "Quark", "disabled": False},
            {
                "mount_path": "/quark/影视/ScrapeFlow",
                "driver": "Local",
                "disabled": True,
                "status": "work",
            },
        ])

        report = alist_offline_readiness(
            alist,
            media_root="/quark/影视",
            rpc_call=lambda _url, _secret, method: (
                {"version": "1.36.0"}
                if method == "aria2.getVersion"
                else {"dir": ARIA2_RPC_EXPECTED_DIR}
            ),
        )

        self.assertFalse(report["verified"])
        self.assertFalse(report["checks"]["transfer"]["verified"])
        self.assertIn("destination", str(report["checks"]["transfer"]["reason"]).casefold())
        self.assertFalse(any(call.startswith("list:") for call in alist.calls))

    def test_unknown_or_upload_disabled_destination_never_reports_ready(self) -> None:
        for storage in (
            {
                "mount_path": "/quark",
                "driver": "Local",
                "disabled": False,
                "status": "work",
            },
            {
                "mount_path": "/quark",
                "driver": "Quark",
                "disabled": False,
                "status": "work",
                "NoUpload": True,
            },
        ):
            with self.subTest(storage=storage):
                alist = ReadOnlyAList(storages=[storage])
                report = alist_offline_readiness(
                    alist,
                    media_root="/quark/影视",
                    rpc_call=lambda _url, _secret, method: (
                        {"version": "1.36.0"}
                        if method == "aria2.getVersion"
                        else {"dir": ARIA2_RPC_EXPECTED_DIR}
                    ),
                )
                self.assertFalse(report["verified"])
                self.assertFalse(report["checks"]["transfer"]["verified"])


if __name__ == "__main__":
    unittest.main()
