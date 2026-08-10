"""Tests for the fixed Quark share fast-save bridge."""

from __future__ import annotations

import io
import json
import unittest
from unittest import mock
import urllib.error

from engine.scrapeflow.quark_fast_save_bridge import (
    QUARK_DRIVE_API,
    QUARK_SHARE_API,
    QuarkFastSaveBridge,
    QuarkSession,
    QuarkShareExpiredError,
    UrlLibQuarkTransport,
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

    def test_delegates_matching_alist_quark_cookie_without_returning_it(self) -> None:
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
        with mock.patch(
            "engine.scrapeflow.quark_fast_save_bridge.urllib.request.urlopen",
            side_effect=response,
        ):
            with self.assertRaises(QuarkShareExpiredError) as raised:
                UrlLibQuarkTransport().request(
                    "POST",
                    QUARK_SHARE_API + "/share/sharepage/token",
                    params={},
                    body={"pwd_id": "cancelled"},
                    cookie="SECRET_COOKIE",
                )
        self.assertEqual(raised.exception.failure_scope, "candidate")


if __name__ == "__main__":
    unittest.main()
