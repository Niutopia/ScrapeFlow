"""Tests for the fixed Quark share fast-save bridge."""

from __future__ import annotations

import io
import json
import os
import unittest
from unittest import mock
import urllib.error
import urllib.request

from engine.scrapeflow.quark_fast_save_bridge import (
    QUARK_DRIVE_API,
    QUARK_SHARE_API,
    QuarkFastSaveBridge,
    QuarkBridgeError,
    QuarkSession,
    QuarkShareInDoubtError,
    QuarkShareExpiredError,
    UrlLibQuarkTransport,
    _build_quark_opener,
    delegated_quark_session,
)


class FixtureTransport:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def request(self, method, endpoint, *, params, body, cookie):
        self.calls.append({
            "method": method,
            "endpoint": endpoint,
            "params": dict(params),
            "body": dict(body) if body is not None else None,
            "cookie_seen": bool(cookie),
        })
        return self.responses.pop(0)


def _selection() -> dict[str, object]:
    return {
        "provider": "quark_share",
        "locator": "quark_share:fixture-share",
        "release_name": "Example Show S01E01",
        "selected_gap_ids": ["S01E01"],
        "acquisition": {
            "kind": "quark_fast_save",
            "share_id": "fixture-share",
            "passcode": "1234",
            "file_id_by_gap": {"S01E01": ["share-fid"]},
            "file_path_by_id": {"share-fid": "Season 01/Example.Show.S01E01.mkv"},
            "file_size_by_id": {"share-fid": 123},
        },
    }


class QuarkFastSaveBridgeTests(unittest.TestCase):
    def test_dry_run_never_returns_passcode(self) -> None:
        plan = QuarkFastSaveBridge.dry_run(
            _selection(),
            "/quark/影视/ScrapeFlow/补源/root/attempt",
        )
        self.assertEqual(plan["status"], "dry_run")
        self.assertEqual(plan["file_names"], ["Example.Show.S01E01.mkv"])
        self.assertNotIn("passcode", json.dumps(plan))

    def test_delegates_legacy_alist_quark_root_id_without_returning_cookie(self) -> None:
        client = mock.Mock()
        client.admin_storages.return_value = [{
            "driver": "Quark",
            "mount_path": "/quark",
            "disabled": False,
            "addition": json.dumps({"cookie": "SECRET_COOKIE", "root_id": "root"}),
        }]
        session = delegated_quark_session(
            client,
            "/quark/影视/ScrapeFlow/补源/root/attempt",
        )
        self.assertEqual(session.cookie, "SECRET_COOKIE")
        self.assertEqual(session.root_id, "root")

    def test_delegates_current_alist_root_folder_id(self) -> None:
        client = mock.Mock()
        client.admin_storages.return_value = [{
            "driver": "Quark",
            "mount_path": "/quark",
            "disabled": False,
            "addition": json.dumps({
                "cookie": "SECRET_COOKIE",
                "root_folder_id": "isolated-root",
            }),
        }]

        session = delegated_quark_session(
            client,
            "/quark/影视/ScrapeFlow/补源/root/attempt",
        )

        self.assertEqual(session.root_id, "isolated-root")

    def test_rejects_missing_invalid_or_conflicting_alist_root_folder(self) -> None:
        for addition in (
            {"cookie": "SECRET_COOKIE"},
            {"cookie": "SECRET_COOKIE", "root_folder_id": ""},
            {
                "cookie": "SECRET_COOKIE",
                "root_folder_id": "isolated-root",
                "root_id": "other-root",
            },
        ):
            with self.subTest(addition=addition):
                client = mock.Mock()
                client.admin_storages.return_value = [{
                    "driver": "Quark",
                    "mount_path": "/quark",
                    "disabled": False,
                    "addition": json.dumps(addition),
                }]
                with self.assertRaisesRegex(QuarkBridgeError, "root folder"):
                    delegated_quark_session(
                        client,
                        "/quark/影视/ScrapeFlow/补源/root/attempt",
                    )

    def test_executes_token_detail_destination_save_and_task(self) -> None:
        transport = FixtureTransport([
            {"status": 200, "code": 0, "data": {"stoken": "fixture-stoken"}},
            {"status": 200, "code": 0, "data": {"list": [{
                "fid": "season", "file_name": "Season 01", "file": False,
            }]}},
            {"status": 200, "code": 0, "data": {"list": [{
                "fid": "share-fid",
                "share_fid_token": "fixture-fid-token",
                "file_name": "Example.Show.S01E01.mkv",
                "file": True,
                "size": 123,
            }]}},
            {"status": 200, "code": 0, "data": {"list": [{
                "fid": "movies", "file_name": "影视", "file": False,
            }]}},
            {"status": 200, "code": 0, "data": {"list": [{
                "fid": "sf", "file_name": "ScrapeFlow", "file": False,
            }]}},
            {"status": 200, "code": 0, "data": {"list": [{
                "fid": "repl", "file_name": "补源", "file": False,
            }]}},
            {"status": 200, "code": 0, "data": {"list": [{
                "fid": "root", "file_name": "root", "file": False,
            }]}},
            {"status": 200, "code": 0, "data": {"list": [{
                "fid": "attempt", "file_name": "attempt", "file": False,
            }]}},
            {"status": 200, "code": 0, "data": {"task_id": "task-fixture"}},
            {"status": 200, "code": 0, "data": {"status": 2}},
        ])
        bridge = QuarkFastSaveBridge(transport, sleep=mock.Mock())
        result = bridge.execute(
            _selection(),
            "/quark/影视/ScrapeFlow/补源/root/attempt",
            QuarkSession("/quark", "mount-root", "SECRET_COOKIE"),
        )

        self.assertEqual(result["task_id"], "task-fixture")
        self.assertEqual(result["expected_files"], [{
            "name": "Example.Show.S01E01.mkv",
            "size": 123,
            "gap_ids": ["S01E01"],
        }])
        self.assertEqual([call["endpoint"] for call in transport.calls], [
            QUARK_SHARE_API + "/share/sharepage/token",
            QUARK_SHARE_API + "/share/sharepage/detail",
            QUARK_SHARE_API + "/share/sharepage/detail",
            QUARK_DRIVE_API + "/file/sort",
            QUARK_DRIVE_API + "/file/sort",
            QUARK_DRIVE_API + "/file/sort",
            QUARK_DRIVE_API + "/file/sort",
            QUARK_DRIVE_API + "/file/sort",
            QUARK_DRIVE_API + "/share/sharepage/save",
            QUARK_SHARE_API + "/task",
        ])
        self.assertEqual(transport.calls[-2]["body"]["fid_list"], ["share-fid"])
        self.assertEqual(transport.calls[-2]["body"]["to_pdir_fid"], "attempt")
        self.assertNotIn("SECRET_COOKIE", json.dumps(result))

    def test_existing_share_task_only_queries_status_without_saving_again(self) -> None:
        transport = FixtureTransport([
            {"status": 200, "code": 0, "data": {"status": 2}},
        ])
        bridge = QuarkFastSaveBridge(transport, sleep=mock.Mock())

        result = bridge.execute(
            _selection(),
            "/quark/影视/ScrapeFlow/补源/root/attempt",
            QuarkSession("/quark", "mount-root", "SECRET_COOKIE"),
            task_id="task-fixture",
        )

        self.assertEqual(result["task_id"], "task-fixture")
        self.assertEqual(
            [call["endpoint"] for call in transport.calls],
            [QUARK_SHARE_API + "/task"],
        )

    def test_unsaved_share_task_id_is_in_doubt(self) -> None:
        transport = FixtureTransport([
            {"status": 200, "code": 0, "data": {"stoken": "fixture-stoken"}},
            {"status": 200, "code": 0, "data": {"list": [{
                "fid": "season", "file_name": "Season 01", "file": False,
            }]}},
            {"status": 200, "code": 0, "data": {"list": [{
                "fid": "share-fid", "share_fid_token": "fixture-fid-token",
                "file_name": "Example.Show.S01E01.mkv", "file": True, "size": 123,
            }]}},
            {"status": 200, "code": 0, "data": {"list": [{
                "fid": "movies", "file_name": "影视", "file": False,
            }]}},
            {"status": 200, "code": 0, "data": {"list": [{
                "fid": "sf", "file_name": "ScrapeFlow", "file": False,
            }]}},
            {"status": 200, "code": 0, "data": {"list": [{
                "fid": "repl", "file_name": "补源", "file": False,
            }]}},
            {"status": 200, "code": 0, "data": {"list": [{
                "fid": "root", "file_name": "root", "file": False,
            }]}},
            {"status": 200, "code": 0, "data": {"list": [{
                "fid": "attempt", "file_name": "attempt", "file": False,
            }]}},
            {"status": 200, "code": 0, "data": {"task_id": "task-fixture"}},
        ])
        bridge = QuarkFastSaveBridge(transport, sleep=mock.Mock())

        with self.assertRaises(QuarkShareInDoubtError) as raised:
            bridge.execute(
                _selection(),
                "/quark/影视/ScrapeFlow/补源/root/attempt",
                QuarkSession("/quark", "mount-root", "SECRET_COOKIE"),
                on_task_id=lambda _task_id: (_ for _ in ()).throw(OSError("state disk full")),
            )

        self.assertEqual(raised.exception.failure_scope, "in_doubt")

    def test_cancelled_share_http_error_is_candidate_failure(self) -> None:
        response = urllib.error.HTTPError(
            QUARK_SHARE_API + "/share/sharepage/token",
            404,
            "Not Found",
            {},
            io.BytesIO(json.dumps({
                "code": 41011,
                "message": "分享地址已失效",
            }).encode("utf-8")),
        )
        with self.assertRaises(QuarkShareExpiredError) as raised:
            UrlLibQuarkTransport(opener=mock.Mock(side_effect=response)).request(
                "POST",
                QUARK_SHARE_API + "/share/sharepage/token",
                params={},
                body={"pwd_id": "cancelled"},
                cookie="SECRET_COOKIE",
            )
        self.assertEqual(raised.exception.failure_scope, "candidate")

    def test_transport_ignores_environment_proxies(self) -> None:
        with mock.patch.dict(
            os.environ,
            {
                "HTTPS_PROXY": "http://198.51.100.7:8080",
                "https_proxy": "http://198.51.100.8:8080",
            },
            clear=False,
        ), mock.patch(
            "engine.scrapeflow.quark_fast_save_bridge.urllib.request.getproxies",
            side_effect=AssertionError("environment proxies must not be read"),
        ):
            opener = _build_quark_opener()

        proxy_handlers = [
            handler for handler in opener.handlers
            if isinstance(handler, urllib.request.ProxyHandler)
        ]
        # CPython omits an empty ProxyHandler from the final opener; if a
        # version retains it, it must still carry no proxy entries.
        self.assertLessEqual(len(proxy_handlers), 1)
        self.assertTrue(all(not handler.proxies for handler in proxy_handlers))

    def test_transport_refuses_redirect_without_replaying_cookie(self) -> None:
        calls = []

        def opener(request, *, timeout):
            calls.append((request.full_url, request.get_header("Cookie"), timeout))
            raise urllib.error.HTTPError(
                request.full_url,
                302,
                "Found",
                {"Location": "https://example.com/steal"},
                io.BytesIO(b""),
            )

        with self.assertRaisesRegex(QuarkBridgeError, "redirect refused"):
            UrlLibQuarkTransport(opener=opener).request(
                "POST",
                QUARK_SHARE_API + "/share/sharepage/token",
                params={},
                body={"pwd_id": "fixture-share"},
                cookie="SECRET_COOKIE",
            )

        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0][1], "SECRET_COOKIE")

    def test_transport_rejects_response_from_any_other_final_url(self) -> None:
        class Response(io.BytesIO):
            def __enter__(self):
                return self

            def __exit__(self, *_args):
                self.close()

            def geturl(self):
                return "https://example.com/stolen"

        with self.assertRaisesRegex(QuarkBridgeError, "response URL escaped"):
            UrlLibQuarkTransport(opener=lambda _request, *, timeout: Response(b"{}"))\
                .request(
                    "GET",
                    QUARK_SHARE_API + "/task",
                    params={"task_id": "fixture"},
                    body=None,
                    cookie="SECRET_COOKIE",
                )

    def test_business_error_does_not_echo_sensitive_server_text(self) -> None:
        class Response(io.BytesIO):
            def __enter__(self):
                return self

            def __exit__(self, *_args):
                self.close()

            def geturl(self):
                return QUARK_SHARE_API + "/share/sharepage/token"

        transport = UrlLibQuarkTransport(
            opener=lambda _request, _timeout: Response(json.dumps({
                "code": 99999,
                "message": "cookie=SECRET_COOKIE token=SECRET_TOKEN",
            }).encode())
        )
        with self.assertRaises(QuarkBridgeError) as raised:
            transport.request(
                "POST",
                QUARK_SHARE_API + "/share/sharepage/token",
                params={},
                body={"pwd_id": "fixture"},
                cookie="SECRET_COOKIE",
            )
        self.assertNotIn("SECRET_COOKIE", str(raised.exception))
        self.assertNotIn("SECRET_TOKEN", str(raised.exception))

    def test_transport_rejects_external_request_endpoint_before_network(self) -> None:
        opener = mock.Mock()
        with self.assertRaisesRegex(QuarkBridgeError, "escaped the fixed API origin"):
            UrlLibQuarkTransport(opener=opener).request(
                "POST",
                "https://example.com/1/clouddrive/share/sharepage/token",
                params={},
                body={"pwd_id": "fixture-share"},
                cookie="SECRET_COOKIE",
            )
        opener.assert_not_called()


if __name__ == "__main__":
    unittest.main()
