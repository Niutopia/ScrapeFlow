"""Contract tests for the passive, loopback-only host Quark Helper."""

from __future__ import annotations

import json
from pathlib import Path
import unittest
from unittest import mock

from aiohttp import ClientSession, web

from local.scrapeflow_api.quark_host_helper import (
    DEFAULT_STAGING_ROOT,
    HELPER_ACTIONS,
    PassiveQuarkCdp,
    QUARK_DRIVE_API,
    QuarkHelperConfig,
    QuarkHelperInDoubt,
    QuarkHelperLostResponse,
    QuarkHelperNotReady,
    QuarkHelperRemoteRejected,
    QuarkHelperValidationError,
    QuarkHostHelperService,
    create_quark_helper_app,
    validate_magnet_submit_payload,
    validate_share_save_payload,
    validate_staging_root,
)


TOKEN = "t" * 32
DESTINATION = "/quark/影视/ScrapeFlow/补源/root-1/attempt-1"
INFOHASH = "a" * 40


def share_payload(**changes: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "attempt_id": "attempt-1",
        "destination": DESTINATION,
        "share_id": "share-123",
        "passcode": "",
        "selected_gap_ids": ["gap-1"],
        "expected_files": [{
            "file_id": "file-1",
            "path": "Season 1/episode-01.mkv",
            "name": "episode-01.mkv",
            "size": 1_024,
            "gap_ids": ["gap-1"],
        }],
        "title": "fixture",
    }
    payload.update(changes)
    return payload


def magnet_payload(**changes: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "destination": DESTINATION,
        "magnet_url": f"magnet:?xt=urn:btih:{INFOHASH}",
        "infohash": INFOHASH,
        "selected_gap_ids": ["gap-1"],
        "expected_files": [{
            "path": "Season 1/episode-01.mkv",
            "size": 1_024,
            "gap_ids": ["gap-1"],
            "torrent_index": 1,
        }],
        "title": "fixture",
    }
    payload.update(changes)
    return payload


class FakeSession:
    def __init__(self) -> None:
        self.ready_error: Exception | None = None
        self.share_result: object = {"status": "submitted", "task_id": "share-task-1"}
        self.magnet_result: object = {"status": "submitted", "task_id": "magnet-task-1"}
        self.magnet_status_result: object = {"status": "running", "task_id": "magnet-task-1"}
        self.calls: list[tuple[str, object]] = []

    async def assert_authenticated(self) -> None:
        self.calls.append(("health", None))
        if self.ready_error is not None:
            raise self.ready_error

    async def share_save(self, payload: object) -> object:
        self.calls.append(("share-save", payload))
        if isinstance(self.share_result, Exception):
            raise self.share_result
        return self.share_result

    async def magnet_submit(self, payload: object) -> object:
        self.calls.append(("magnet-submit", payload))
        if isinstance(self.magnet_result, Exception):
            raise self.magnet_result
        return self.magnet_result

    async def magnet_status(self, task_id: str) -> object:
        self.calls.append(("magnet-status", task_id))
        if isinstance(self.magnet_status_result, Exception):
            raise self.magnet_status_result
        return self.magnet_status_result


class HelperValidationTests(unittest.TestCase):
    def test_share_contract_rejects_cookie_endpoint_and_formal_library_paths(self) -> None:
        for changes in (
            {"cookie": "must-not-be-accepted"},
            {"endpoint": "https://drive.quark.cn/anything"},
            {"destination": "/quark/影视/电影/root-1/attempt-1"},
            {"attempt_id": "other-attempt"},
        ):
            with self.subTest(changes=changes), self.assertRaises(QuarkHelperValidationError):
                validate_share_save_payload(share_payload(**changes), staging_root=DEFAULT_STAGING_ROOT)

    def test_share_contract_keeps_reentry_task_id_and_exact_manifest(self) -> None:
        normalized = validate_share_save_payload(
            share_payload(task_id="share-task-1"), staging_root=DEFAULT_STAGING_ROOT,
        )
        self.assertEqual(normalized["task_id"], "share-task-1")
        self.assertEqual(normalized["expected_files"], [{
            "path": "Season 1/episode-01.mkv",
            "size": 1_024,
            "gap_ids": ["gap-1"],
            "file_id": "file-1",
            "name": "episode-01.mkv",
        }])

    def test_magnet_contract_rejects_generic_urls_and_unselected_indexes(self) -> None:
        for changes in (
            {"url": "https://example.invalid"},
            {"magnet_url": "https://example.invalid"},
            {"infohash": "b" * 40},
            {"expected_files": [{
                "path": "episode-01.mkv", "size": 1_024,
                "gap_ids": ["gap-1"], "torrent_index": 0,
            }]},
        ):
            with self.subTest(changes=changes), self.assertRaises(QuarkHelperValidationError):
                validate_magnet_submit_payload(magnet_payload(**changes), staging_root=DEFAULT_STAGING_ROOT)

    def test_staging_root_refuses_a_formal_library_subtree(self) -> None:
        for root in (
            "/quark/影视/电影/ScrapeFlow/补源",
            "/quark/私人/ScrapeFlow/补源",
        ):
            with self.subTest(root=root), self.assertRaises(QuarkHelperValidationError):
                validate_staging_root(root)

    def test_config_requires_explicit_safe_cdp_and_loopback_bind(self) -> None:
        with self.assertRaises(QuarkHelperValidationError):
            QuarkHelperConfig(
                host="0.0.0.0", port=8766, token=TOKEN,
                cdp_url="http://127.0.0.1:9222/json/list",
            )
        with self.assertRaises(QuarkHelperValidationError):
            QuarkHelperConfig(
                host="127.0.0.1", port=8766, token=TOKEN,
                cdp_url="http://127.0.0.1:9125/json/list",
            )
        with self.assertRaises(QuarkHelperValidationError):
            QuarkHelperConfig(host="127.0.0.1", port=8766, token=TOKEN, cdp_url="")

    def test_new_helper_does_not_import_legacy_control_or_generic_proxy_tools(self) -> None:
        source = Path("local/scrapeflow_api/quark_host_helper.py").read_text(encoding="utf-8")
        for forbidden in (
            "subprocess", "launchctl", "webbrowser", "/v1/quark/request",
            "SubmitJournal", "hashlib", "urllib.request",
        ):
            with self.subTest(forbidden=forbidden):
                self.assertNotIn(forbidden, source)
        self.assertNotIn("/v1/", source.replace("/v1/share-save", "").replace("/v1/magnet-submit", "").replace("/v1/magnet-status", ""))


class HelperHttpTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.session = FakeSession()
        self.service = QuarkHostHelperService(self.session, staging_root=DEFAULT_STAGING_ROOT)
        self.runner = web.AppRunner(create_quark_helper_app(self.service, token=TOKEN))
        await self.runner.setup()
        self.site = web.TCPSite(self.runner, "127.0.0.1", 0)
        await self.site.start()
        server = self.site._server
        assert server is not None
        port = server.sockets[0].getsockname()[1]
        self.url = f"http://127.0.0.1:{port}"
        self.client = ClientSession()

    async def asyncTearDown(self) -> None:
        await self.client.close()
        await self.runner.cleanup()

    async def request(self, method: str, path: str, *, body: object | None = None, token: str = TOKEN):
        headers = {"Authorization": "Bearer " + token}
        kwargs = {"headers": headers}
        if body is not None:
            kwargs["json"] = body
        response = await self.client.request(method, self.url + path, **kwargs)
        return response.status, await response.json()

    async def test_health_requires_bearer_and_proves_authenticated_existing_session(self) -> None:
        status, body = await self.request("GET", "/health", token="wrong")
        self.assertEqual((status, body["error"]), (401, "unauthorized"))
        status, body = await self.request("GET", "/health")
        self.assertEqual(status, 200)
        self.assertEqual(body["status"], "ready")
        self.assertTrue(body["authenticated"])
        self.assertEqual(body["actions"], list(HELPER_ACTIONS))
        self.assertEqual(self.session.calls, [("health", None)])

    async def test_idle_or_unauthenticated_renderer_is_not_ready(self) -> None:
        self.session.ready_error = QuarkHelperNotReady("no existing renderer")
        status, body = await self.request("GET", "/health")
        self.assertEqual((status, body["error"]), (503, "quark_not_ready"))
        self.assertNotIn("renderer", json.dumps(body))

    async def test_only_fixed_routes_are_available(self) -> None:
        status, body = await self.request("POST", "/v1/quark/request", body={})
        self.assertEqual((status, body["error"]), (404, "not_found"))
        status, body = await self.request("GET", "/debug")
        self.assertEqual((status, body["error"]), (404, "not_found"))

    async def test_share_save_forwards_only_normalized_fixed_contract(self) -> None:
        status, body = await self.request("POST", "/v1/share-save", body=share_payload(task_id="old-task"))
        self.assertEqual(status, 200)
        self.assertEqual(body, {"status": "submitted", "task_id": "share-task-1"})
        action, payload = self.session.calls[-1]
        self.assertEqual(action, "share-save")
        self.assertIsInstance(payload, dict)
        assert isinstance(payload, dict)
        self.assertEqual(set(payload), {
            "attempt_id", "destination", "share_id", "passcode", "selected_gap_ids",
            "expected_files", "title", "task_id",
        })
        self.assertNotIn("cookie", payload)
        self.assertNotIn("endpoint", payload)

    async def test_submit_unknown_result_returns_contractual_in_doubt(self) -> None:
        self.session.magnet_result = QuarkHelperInDoubt("lost response")
        status, body = await self.request("POST", "/v1/magnet-submit", body=magnet_payload())
        self.assertEqual((status, body), (409, {
            "status": "error", "error": "submit_in_doubt", "in_doubt": True,
        }))

    async def test_magnet_status_accepts_only_task_id(self) -> None:
        status, body = await self.request("POST", "/v1/magnet-status", body={"task_id": "magnet-task-1"})
        self.assertEqual((status, body), (200, {"status": "running", "task_id": "magnet-task-1"}))
        status, body = await self.request("POST", "/v1/magnet-status", body={
            "task_id": "magnet-task-1", "destination": DESTINATION,
        })
        self.assertEqual((status, body["error"]), (400, "invalid_request"))


class HelperReentryTests(unittest.IsolatedAsyncioTestCase):
    async def test_cdp_discovery_redirect_is_refused_without_following(self) -> None:
        async def redirect(_request):
            return web.Response(
                status=302,
                headers={"Location": "https://example.invalid/json/list"},
            )

        app = web.Application()
        app.router.add_get("/json/list", redirect)
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, "127.0.0.1", 0)
        await site.start()
        server = site._server
        assert server is not None
        port = server.sockets[0].getsockname()[1]
        session = PassiveQuarkCdp(
            cdp_url=f"http://127.0.0.1:{port}/json/list",
            staging_root=DEFAULT_STAGING_ROOT,
            mount_path="/quark",
            root_fid="0",
        )
        try:
            with self.assertRaisesRegex(QuarkHelperNotReady, "redirect refused"):
                await session._select_renderer_socket()
        finally:
            await runner.cleanup()

    async def test_share_reentry_requires_task_and_staging_reconciliation(self) -> None:
        session = PassiveQuarkCdp(
            cdp_url="http://127.0.0.1:9222/json/list",
            staging_root=DEFAULT_STAGING_ROOT,
            mount_path="/quark",
            root_fid="0",
        )
        payload = validate_share_save_payload(
            share_payload(task_id="share-task-1"), staging_root=DEFAULT_STAGING_ROOT,
        )
        with mock.patch.object(session, "_reconcile_existing_share_task", new=mock.AsyncMock()) as reconcile:
            result = await session.share_save(payload)
        self.assertEqual(result, {"status": "finished", "task_id": "share-task-1"})
        reconcile.assert_awaited_once_with(
            task_id="share-task-1",
            destination=DESTINATION,
            expected_files=payload["expected_files"],
        )

    async def test_share_reentry_returns_in_doubt_when_reconciliation_cannot_prove_staging(self) -> None:
        session = PassiveQuarkCdp(
            cdp_url="http://127.0.0.1:9222/json/list",
            staging_root=DEFAULT_STAGING_ROOT,
            mount_path="/quark",
            root_fid="0",
        )
        payload = validate_share_save_payload(
            share_payload(task_id="share-task-1"), staging_root=DEFAULT_STAGING_ROOT,
        )
        with mock.patch.object(
            session,
            "_reconcile_existing_share_task",
            new=mock.AsyncMock(side_effect=QuarkHelperNotReady("not proven")),
        ):
            with self.assertRaises(QuarkHelperInDoubt):
                await session.share_save(payload)

    async def test_health_requires_passive_wsg_capabilities_before_session_readback(self) -> None:
        session = PassiveQuarkCdp(
            cdp_url="http://127.0.0.1:9222/json/list",
            staging_root=DEFAULT_STAGING_ROOT,
            mount_path="/quark",
            root_fid="0",
        )
        with mock.patch.object(
            session,
            "_evaluate_json",
            side_effect=[
                {"kind": "wsg-capabilities", "encrypt": True, "decrypt": True},
                {"kind": "response", "status": 200, "text": '{"code":0,"data":{"list":[]}}'},
            ],
        ) as evaluate:
            await session.assert_authenticated()
        first_expression = evaluate.await_args_list[0].args[0]
        self.assertIn("wsg-capabilities", first_expression)
        self.assertNotIn("fetch(", first_expression)

    async def test_missing_wsg_is_not_ready_without_attempting_session_network_readback(self) -> None:
        session = PassiveQuarkCdp(
            cdp_url="http://127.0.0.1:9222/json/list",
            staging_root=DEFAULT_STAGING_ROOT,
            mount_path="/quark",
            root_fid="0",
        )
        with mock.patch.object(
            session,
            "_evaluate_json",
            return_value={"kind": "wsg-capabilities", "encrypt": True, "decrypt": False},
        ) as evaluate:
            with self.assertRaises(QuarkHelperNotReady):
                await session.assert_authenticated()
        evaluate.assert_awaited_once()

    async def test_upstream_rate_limit_and_5xx_are_not_candidate_rejections(self) -> None:
        session = PassiveQuarkCdp(
            cdp_url="http://127.0.0.1:9222/json/list",
            staging_root=DEFAULT_STAGING_ROOT,
            mount_path="/quark",
            root_fid="0",
        )
        for response in (
            {"kind": "response", "status": 429, "text": "{}"},
            {"kind": "response", "status": 503, "text": "{}"},
            {"kind": "response", "status": 200, "text": '{"code":429}'},
        ):
            with self.subTest(response=response), mock.patch.object(
                session, "_evaluate_json", return_value=response,
            ):
                with self.assertRaises(QuarkHelperNotReady):
                    await session._call_fixed(
                        origin=QUARK_DRIVE_API, path="/file/sort", method="GET",
                    )

    async def test_definite_quark_business_rejection_stays_candidate_scoped(self) -> None:
        session = PassiveQuarkCdp(
            cdp_url="http://127.0.0.1:9222/json/list",
            staging_root=DEFAULT_STAGING_ROOT,
            mount_path="/quark",
            root_fid="0",
        )
        with mock.patch.object(
            session,
            "_evaluate_json",
            return_value={"kind": "response", "status": 200, "text": '{"code":41004}'},
        ):
            with self.assertRaises(QuarkHelperRemoteRejected):
                await session._call_fixed(
                    origin=QUARK_DRIVE_API, path="/file/sort", method="GET",
                )

    async def test_pre_submit_loss_is_not_ready_for_both_lanes(self) -> None:
        session = PassiveQuarkCdp(
            cdp_url="http://127.0.0.1:9222/json/list",
            staging_root=DEFAULT_STAGING_ROOT,
            mount_path="/quark",
            root_fid="0",
        )
        for method, payload in ((session.share_save, share_payload()), (session.magnet_submit, magnet_payload())):
            with self.subTest(method=method.__name__), mock.patch.object(
                session,
                "_destination_fid",
                new=mock.AsyncMock(side_effect=QuarkHelperLostResponse("pre-submit")),
            ):
                with self.assertRaises(QuarkHelperNotReady):
                    await method(payload)

    async def test_post_submit_loss_is_in_doubt_for_both_lanes(self) -> None:
        session = PassiveQuarkCdp(
            cdp_url="http://127.0.0.1:9222/json/list",
            staging_root=DEFAULT_STAGING_ROOT,
            mount_path="/quark",
            root_fid="0",
        )
        with mock.patch.object(session, "_destination_fid", new=mock.AsyncMock(return_value="stage-fid")), \
                mock.patch.object(session, "_share_file_tokens", new=mock.AsyncMock(return_value=["fid-token"])), \
                mock.patch.object(
                    session,
                    "_call_fixed",
                    new=mock.AsyncMock(side_effect=[
                        {"data": {"stoken": "share-token"}},
                        QuarkHelperLostResponse("post-share-submit"),
                    ]),
                ):
            with self.assertRaises(QuarkHelperInDoubt):
                await session.share_save(share_payload())
        with mock.patch.object(session, "_destination_fid", new=mock.AsyncMock(return_value="stage-fid")), \
                mock.patch.object(
                    session,
                    "_call_fixed",
                    new=mock.AsyncMock(side_effect=[
                        {"data": {"token": "parse-token"}},
                        QuarkHelperLostResponse("post-magnet-submit"),
                    ]),
                ):
            with self.assertRaises(QuarkHelperInDoubt):
                await session.magnet_submit(magnet_payload())

    async def test_nested_share_source_reentry_reads_flattened_staging_name(self) -> None:
        session = PassiveQuarkCdp(
            cdp_url="http://127.0.0.1:9222/json/list",
            staging_root=DEFAULT_STAGING_ROOT,
            mount_path="/quark",
            root_fid="0",
        )
        expected = validate_share_save_payload(
            share_payload(task_id="share-task-1"), staging_root=DEFAULT_STAGING_ROOT,
        )["expected_files"]
        with mock.patch.object(session, "_destination_fid", new=mock.AsyncMock(return_value="stage-fid")), \
                mock.patch.object(
                    session,
                    "_call_fixed",
                    new=mock.AsyncMock(side_effect=[
                        {"data": {"status": 2}},
                        {"data": {"list": [{
                            "fid": "saved-file", "file": True,
                            "file_name": "episode-01.mkv", "size": 1_024,
                        }]}},
                    ]),
                ) as call_fixed:
            await session._reconcile_existing_share_task(
                task_id="share-task-1",
                destination=DESTINATION,
                expected_files=expected,
            )
        listing_query = call_fixed.await_args_list[1].kwargs["query"]
        self.assertEqual(listing_query["pdir_fid"], "stage-fid")


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
