import io
import json
import unittest
from unittest import mock
import urllib.error

from engine.scrapeflow.quark_fast_save_bridge import (
    QUARK_DRIVE_API, QUARK_SHARE_API,
    QuarkBridgeError, QuarkFastSaveBridge, QuarkMagnetCandidateError,
    QuarkMagnetInfrastructureError, QuarkMagnetOfflineBridge,
    QuarkMagnetParseInfrastructureError,
    QuarkMagnetWsgUnavailableError, QuarkShareExpiredError,
    QuarkSession,
    UrlLibQuarkTransport, delegated_quark_session,
)
from engine.tools import replenishment_local_adapter as adapter


class FixtureTransport:
    supports_wsg = True

    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def request(self, method, endpoint, *, params, body, cookie):
        self.calls.append({
            "method": method, "endpoint": endpoint, "params": dict(params),
            "body": dict(body) if body is not None else None,
            "cookie_seen": bool(cookie),
        })
        return self.responses.pop(0)


class QuarkFastSaveBridgeTests(unittest.TestCase):
    def selection(self):
        return {
            "provider": "quark_share", "release_name": "Example S01E01",
            "selected_gap_ids": ["S01E01"],
            "acquisition": {
                "kind": "quark_fast_save", "pwd_id": "fixture-share",
                "file_id_by_gap": {"S01E01": ["share-fid"]},
                "file_path_by_id": {"share-fid": "Season 01/Example.S01E01.mkv"},
                "file_size_by_id": {"share-fid": 123},
            },
        }

    def test_dry_run_has_endpoint_plan_and_never_reads_alist_or_http(self):
        result = adapter._quark_fast_save_port(
            self.selection(), "/quark/inbox/fixture", dry_run=True,
        )
        self.assertEqual(result["status"], "dry_run")
        self.assertEqual(result["file_names"], ["Example.S01E01.mkv"])
        self.assertNotIn("passcode", result)

    def test_delegates_matching_alist_cookie_without_returning_it(self):
        client = mock.Mock()
        client.admin_storages.return_value = [{
            "driver": "Quark", "mount_path": "/quark", "disabled": False,
            "addition": json.dumps({"cookie": "SECRET_COOKIE", "root_id": "root"}),
        }]
        session = delegated_quark_session(client, "/quark/inbox/fixture")
        self.assertEqual(session.cookie, "SECRET_COOKIE")
        self.assertNotIn("SECRET_COOKIE", repr({
            "mount_path": session.mount_path, "root_id": session.root_id,
        }))

    def test_fixture_executes_token_detail_destination_save_and_task(self):
        selection = self.selection()
        selection["acquisition"]["file_path_by_id"]["share-fid"] = "Example.S01E01.mkv"
        transport = FixtureTransport([
            {"status": 200, "code": 0, "data": {"stoken": "fixture-stoken"}},
            {"status": 200, "code": 0, "data": {"list": [{
                "fid": "share-fid", "share_fid_token": "fixture-fid-token",
                "file_name": "Example.S01E01.mkv", "size": 123,
            }]}},
            {"status": 200, "code": 0, "data": {"list": [{
                "fid": "inbox-fid", "file_name": "inbox", "file": False,
            }]}},
            {"status": 200, "code": 0, "data": {"list": [{
                "fid": "target-fid", "file_name": "fixture", "file": False,
            }]}},
            {"status": 200, "code": 0, "data": {"task_id": "task-fixture"}},
            {"status": 200, "code": 0, "data": {"status": 2}},
        ])
        bridge = QuarkFastSaveBridge(transport, sleep=mock.Mock())
        result = bridge.execute(
            selection, "/quark/inbox/fixture",
            QuarkSession("/quark", "root", "SECRET_COOKIE"),
        )
        self.assertEqual(result["status"], "submitted")
        self.assertEqual(result["expected_files"][0]["size"], 123)
        self.assertEqual([call["endpoint"] for call in transport.calls], [
            QUARK_SHARE_API + "/share/sharepage/token",
            QUARK_SHARE_API + "/share/sharepage/detail",
            QUARK_DRIVE_API + "/file/sort", QUARK_DRIVE_API + "/file/sort",
            QUARK_DRIVE_API + "/share/sharepage/save", QUARK_SHARE_API + "/task",
        ])
        save = transport.calls[-2]
        self.assertEqual(save["body"]["to_pdir_fid"], "target-fid")
        self.assertEqual(save["body"]["fid_list"], ["share-fid"])
        self.assertTrue(all(call["cookie_seen"] for call in transport.calls))
        self.assertNotIn("SECRET_COOKIE", json.dumps(result))

    def test_inspect_share_recursively_lists_files_without_save_mutation(self):
        transport = FixtureTransport([
            {"status": 200, "code": 0, "data": {"stoken": "fixture-stoken"}},
            {"status": 200, "code": 0, "data": {"list": [{
                "fid": "season", "file_name": "Season 00", "file": False,
            }]}},
            {"status": 200, "code": 0, "data": {"list": [{
                "fid": "episode", "file_name": "Example.S00E01.mkv",
                "file": True, "size": 456,
            }]}},
        ])
        rows = QuarkFastSaveBridge(transport).inspect_share(
            QuarkSession("/quark", "root", "SECRET_COOKIE"),
            pwd_id="fixture-share",
        )
        self.assertEqual(rows, [{
            "file_id": "episode", "path": "Season 00/Example.S00E01.mkv", "size": 456,
        }])
        endpoints = [call["endpoint"] for call in transport.calls]
        self.assertEqual(endpoints, [
            QUARK_SHARE_API + "/share/sharepage/token",
            QUARK_SHARE_API + "/share/sharepage/detail",
            QUARK_SHARE_API + "/share/sharepage/detail",
        ])
        self.assertFalse(any(endpoint.endswith("/save") for endpoint in endpoints))

    def test_cancelled_share_http_error_is_candidate_failure_not_infrastructure(self):
        for code, message in (
            (41010, "文件涉及违规内容"),
            (41011, "分享地址已失效"),
            (41012, "好友已取消了分享"),
            (41019, "分享地址已过期"),
            (41031, "分享者用户封禁链接查看受限"),
        ):
            with self.subTest(code=code):
                response = urllib.error.HTTPError(
                    QUARK_SHARE_API + "/share/sharepage/token", 404,
                    "Not Found", {}, io.BytesIO(json.dumps({
                        "code": code, "message": message,
                    }).encode("utf-8")),
                )
                with mock.patch(
                    "engine.scrapeflow.quark_fast_save_bridge.urllib.request.urlopen",
                    side_effect=response,
                ):
                    with self.assertRaises(QuarkShareExpiredError) as raised:
                        UrlLibQuarkTransport().request(
                            "POST", QUARK_SHARE_API + "/share/sharepage/token",
                            params={}, body={"pwd_id": "cancelled"},
                            cookie="SECRET_COOKIE",
                        )
                self.assertIsInstance(raised.exception, QuarkBridgeError)
                self.assertEqual(raised.exception.failure_scope, "candidate")

    def test_resume_task_polls_without_revalidating_or_saving_share(self):
        transport = FixtureTransport([
            {"status": 200, "code": 0, "data": {
                "status": 2, "created_at": 1_700_000_000_000,
                "finished_at": 1_700_000_001_000,
            }},
        ])
        prepared = mock.Mock()
        submitted = mock.Mock()
        result = QuarkFastSaveBridge(transport, sleep=mock.Mock()).execute(
            self.selection(), "/quark/inbox/fixture",
            QuarkSession("/quark", "root", "SECRET_COOKIE"),
            resume_task_id="persisted-task", on_prepared=prepared,
            on_submitted=submitted,
        )
        self.assertEqual(result["task_id"], "persisted-task")
        self.assertEqual(result["expected_files"], [{
            "name": "Example.S01E01.mkv", "size": 123,
            "gap_ids": ["S01E01"],
        }])
        self.assertEqual(result["task_status"], 2)
        self.assertEqual(result["task_created_at"], 1_700_000_000_000)
        self.assertEqual(result["task_finished_at"], 1_700_000_001_000)
        self.assertEqual(
            [call["endpoint"] for call in transport.calls],
            [QUARK_SHARE_API + "/task"],
        )
        prepared.assert_not_called()
        submitted.assert_not_called()

    def test_submit_callbacks_bracket_the_share_save_mutation(self):
        selection = self.selection()
        selection["acquisition"]["file_path_by_id"]["share-fid"] = "Example.S01E01.mkv"
        events = []

        class EventTransport(FixtureTransport):
            def request(self, method, endpoint, **kwargs):
                if endpoint.endswith("/share/sharepage/save"):
                    events.append("save")
                return super().request(method, endpoint, **kwargs)

        transport = EventTransport([
            {"status": 200, "code": 0, "data": {"stoken": "fixture-stoken"}},
            {"status": 200, "code": 0, "data": {"list": [{
                "fid": "share-fid", "share_fid_token": "fixture-token",
                "file_name": "Example.S01E01.mkv", "size": 123,
            }]}},
            {"status": 200, "code": 0, "data": {"list": [{
                "fid": "target-fid", "file_name": "fixture", "file": False,
            }]}},
            {"status": 200, "code": 0, "data": {"task_id": "task-fixture"}},
            {"status": 200, "code": 0, "data": {"status": 2}},
        ])
        QuarkFastSaveBridge(transport, sleep=mock.Mock()).execute(
            selection, "/quark/fixture",
            QuarkSession("/quark", "root", "SECRET_COOKIE"),
            on_prepared=lambda: events.append("prepared"),
            on_submitted=lambda task_id: events.append(f"submitted:{task_id}"),
        )
        self.assertEqual(events, ["prepared", "save", "submitted:task-fixture"])

    def test_fixture_traverses_reviewed_nested_path_before_save(self):
        selection = self.selection()
        selection["acquisition"]["file_path_by_id"]["share-fid"] = (
            "Series/Season 00/Example.S00E01.mkv"
        )
        transport = FixtureTransport([
            {"status": 200, "code": 0, "data": {"stoken": "fixture-stoken"}},
            {"status": 200, "code": 0, "data": {"list": [{
                "fid": "wrong-root-fid", "share_fid_token": "wrong-token",
                "file_name": "Example.S00E01.mkv", "file": True, "size": 123,
            }, {
                "fid": "series-fid", "file_name": "Series", "file": False,
            }]}},
            {"status": 200, "code": 0, "data": {"list": [{
                "fid": "season-fid", "file_name": "Season 00", "file": False,
            }]}},
            {"status": 200, "code": 0, "data": {"list": [{
                "fid": "share-fid", "share_fid_token": "leaf-token",
                "file_name": "Example.S00E01.mkv", "file": True, "size": 123,
            }]}},
            {"status": 200, "code": 0, "data": {"list": [{
                "fid": "target-fid", "file_name": "fixture", "file": False,
            }]}},
            {"status": 200, "code": 0, "data": {"task_id": "task-fixture"}},
            {"status": 200, "code": 0, "data": {"status": 2}},
        ])
        bridge = QuarkFastSaveBridge(transport, sleep=mock.Mock())
        receipt = bridge.execute(
            selection, "/quark/fixture", QuarkSession("/quark", "root", "SECRET_COOKIE"),
        )
        self.assertEqual(receipt["expected_files"][0]["name"], "Example.S00E01.mkv")
        details = [call for call in transport.calls if call["endpoint"].endswith("/share/sharepage/detail")]
        self.assertEqual([call["params"]["pdir_fid"] for call in details], ["0", "series-fid", "season-fid"])
        self.assertEqual(transport.calls[-2]["body"]["fid_list"], ["share-fid"])

    def magnet_selection(self):
        return {
            "provider": "quark_magnet", "release_name": "Example S01E01",
            "locator": "magnet:fixture", "selected_gap_ids": ["S01E01"],
            "acquisition": {
                "kind": "quark_magnet_offline",
                "magnet_url": "magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567",
                "expected_files": [{
                    "torrent_index": 7,
                    "path": "Example/Example.S01E01.mkv", "size": 123,
                    "gap_ids": ["S01E01"],
                }],
            },
        }

    def test_magnet_dry_run_validates_btih_and_exact_arrival_manifest(self):
        plan = QuarkMagnetOfflineBridge.dry_run(
            self.magnet_selection(), "/quark/inbox/fixture",
        )
        self.assertEqual(plan["infohash"], "0123456789abcdef0123456789abcdef01234567")
        self.assertEqual(plan["expected_files"][0]["size"], 123)
        broken = self.magnet_selection()
        broken["acquisition"]["magnet_url"] = "magnet:?xt=urn:btih:bad"
        with self.assertRaises(QuarkMagnetCandidateError):
            QuarkMagnetOfflineBridge.dry_run(broken, "/quark/inbox/fixture")

    def test_magnet_dry_run_projects_release_manifest_to_current_gap_subset(self):
        selection = self.magnet_selection()
        selection["acquisition"]["expected_files"] = [
            {
                "torrent_index": 6,
                "path": "Example/Example.S01E00.mkv", "size": 122,
                "gap_ids": ["S01E00"],
            },
            *selection["acquisition"]["expected_files"],
            {
                "torrent_index": 8,
                "path": "Example/Example.S01E02.mkv", "size": 124,
                "gap_ids": ["S01E02"],
            },
        ]

        plan = QuarkMagnetOfflineBridge.dry_run(
            selection, "/quark/inbox/fixture",
        )

        self.assertEqual(plan["selected_gap_ids"], ["S01E01"])
        self.assertEqual(plan["expected_files"], [{
            "torrent_index": 7,
            "path": "Example/Example.S01E01.mkv", "size": 123,
            "gap_ids": ["S01E01"],
        }])

    def test_fixture_resolves_destination_submits_magnet_and_polls_task(self):
        transport = FixtureTransport([
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
            {"status": 200, "code": 0, "data": [{"task_id": "offline-task", "status": 1}]},
            {"status": 200, "code": 0, "data": [{"task_id": "offline-task", "status": 2}]},
        ])
        bridge = QuarkMagnetOfflineBridge(transport, sleep=mock.Mock())
        result = bridge.execute(
            self.magnet_selection(), "/quark/inbox/fixture",
            QuarkSession("/quark", "root", "SECRET_COOKIE"),
        )
        self.assertEqual(result["task_id"], "offline-task")
        parse = transport.calls[2]
        self.assertEqual(parse["endpoint"], QUARK_SHARE_API + "/offline/download/parse")
        self.assertTrue(parse["body"]["url"].startswith("magnet:?"))
        self.assertFalse(parse["body"]["auto_download"])
        submit = transport.calls[3]
        self.assertEqual(submit["endpoint"], QUARK_SHARE_API + "/offline/download/submit")
        self.assertEqual(submit["body"]["pdir_fid"], "target-fid")
        self.assertEqual(submit["body"]["selected_files"], [107])
        self.assertEqual(transport.calls[-1]["body"]["query_times"], 1)

    def test_empty_cloud_parse_is_infrastructure_not_candidate_failure(self):
        transport = FixtureTransport([
            {"status": 200, "code": 0, "data": {"list": [{
                "fid": "inbox-fid", "file_name": "inbox", "file": False,
            }]}},
            {"status": 200, "code": 0, "data": {"list": [{
                "fid": "target-fid", "file_name": "fixture", "file": False,
            }]}},
            {"status": 200, "code": 0, "data": {}},
        ])
        bridge = QuarkMagnetOfflineBridge(transport, sleep=mock.Mock())
        with self.assertRaises(QuarkMagnetParseInfrastructureError) as raised:
            bridge.execute(
                self.magnet_selection(), "/quark/inbox/fixture",
                QuarkSession("/quark", "root", "SECRET_COOKIE"),
            )
        self.assertEqual(raised.exception.failure_scope, "infrastructure")
        self.assertFalse(raised.exception.exclude_candidate)

    def test_failed_offline_task_is_candidate_failure(self):
        transport = FixtureTransport([
            {"status": 200, "code": 0, "data": {"list": []}},
        ])
        # Root destination needs no file/sort calls.
        bridge = QuarkMagnetOfflineBridge(transport, sleep=mock.Mock())
        transport.responses = [
            {"status": 200, "code": 0, "data": {
                "token": "parse-token", "files": [{
                    "index": 7, "path": "Example/Example.S01E01.mkv", "size": 123,
                }],
            }},
            {"status": 200, "code": 0, "data": {"task_id": "offline-task"}},
            {"status": 200, "code": 0, "data": [{"task_id": "offline-task", "status": 3}]},
        ]
        with self.assertRaises(QuarkMagnetCandidateError):
            bridge.execute(
                self.magnet_selection(), "/quark",
                QuarkSession("/quark", "root", "SECRET_COOKIE"),
            )

    def test_offline_plain_transport_is_blocked_before_http(self):
        transport = FixtureTransport([])
        transport.supports_wsg = False
        bridge = QuarkMagnetOfflineBridge(transport, sleep=mock.Mock())
        with self.assertRaises(QuarkMagnetWsgUnavailableError):
            bridge.execute(
                self.magnet_selection(), "/quark/inbox/fixture",
                QuarkSession("/quark", "root", "SECRET_COOKIE"),
            )
        self.assertEqual(transport.calls, [])

    def test_offline_resume_checkpoint_polls_without_parse_or_submit(self):
        transport = FixtureTransport([
            {"status": 200, "code": 0, "data": [{
                "task_id": "checkpoint-task", "status": 2,
            }]},
        ])
        bridge = QuarkMagnetOfflineBridge(transport, sleep=mock.Mock())
        result = bridge.execute(
            self.magnet_selection(), "/quark",
            QuarkSession("/quark", "root", "SECRET_COOKIE"),
            resume_task_id="checkpoint-task",
        )
        self.assertEqual(result["task_id"], "checkpoint-task")
        self.assertEqual(len(transport.calls), 1)
        self.assertTrue(transport.calls[0]["endpoint"].endswith("/offline/save_to/progress"))

    def test_offline_missing_task_id_has_magnet_infrastructure_stage(self):
        transport = FixtureTransport([
            {"status": 200, "code": 0, "data": {
                "token": "parse-token", "files": [{
                    "index": 7, "path": "Example/Example.S01E01.mkv", "size": 123,
                }],
            }},
            {"status": 200, "code": 0, "data": {}},
        ])
        bridge = QuarkMagnetOfflineBridge(transport, sleep=mock.Mock())
        with self.assertRaises(QuarkMagnetInfrastructureError) as raised:
            bridge.execute(
                self.magnet_selection(), "/quark",
                QuarkSession("/quark", "root", "SECRET_COOKIE"),
            )
        self.assertEqual(raised.exception.failure_stage, "quark_magnet_submit")


if __name__ == "__main__":
    unittest.main()
