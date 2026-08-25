"""Contract tests for the passive, loopback-only host Quark Helper."""

from __future__ import annotations

import json
from pathlib import Path
import unittest
from unittest import mock

from aiohttp import ClientSession, web

from local.scrapeflow_api.quark_host_helper import (
    AListQuarkSessionResolver,
    DEFAULT_STAGING_ROOT,
    DelegatedQuarkSession,
    DOCKER_SIDECAR_CDP_HOST,
    DOCKER_SIDECAR_CDP_PORT,
    DOCKER_SIDECAR_CDP_URL,
    PassiveQuarkCdp,
    QUARK_DRIVE_API,
    QuarkHelperConfig,
    QuarkHelperInDoubt,
    QuarkHelperLostResponse,
    QuarkHelperNotReady,
    QuarkHelperRemoteRejected,
    QuarkHelperValidationError,
    QuarkHostHelperService,
    _cdp_websocket_url,
    create_quark_helper_app,
    validate_share_save_payload,
    validate_staging_root,
)


TOKEN = "t" * 32
DESTINATION = "/quark/影视/ScrapeFlow/补源/root-1/attempt-1"
ALIST_CONFIG = {
    "alist_url": "http://127.0.0.1:5244",
    "alist_username": "admin",
    "alist_password": "fixture-alist-password",
}


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


class FakeSession:
    def __init__(self) -> None:
        self.ready_error: Exception | None = None
        self.share_result: object = {"status": "submitted", "task_id": "share-task-1"}
        self.calls: list[tuple[str, object]] = []

    async def share_save(self, payload: object) -> object:
        self.calls.append(("share-save", payload))
        if isinstance(self.share_result, Exception):
            raise self.share_result
        return self.share_result


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
                **ALIST_CONFIG,
            )
        with self.assertRaises(QuarkHelperValidationError):
            QuarkHelperConfig(
                host="127.0.0.1", port=8766, token=TOKEN,
                cdp_url="http://127.0.0.1:9125/json/list",
                **ALIST_CONFIG,
            )
        with self.assertRaises(QuarkHelperValidationError):
            QuarkHelperConfig(
                host="127.0.0.1", port=8766, token=TOKEN, cdp_url="",
                **ALIST_CONFIG,
            )

    def test_docker_sidecar_requires_the_one_fixed_discovery_endpoint(self) -> None:
        config = QuarkHelperConfig(
            host="127.0.0.1",
            port=18765,
            token=TOKEN,
            cdp_url=DOCKER_SIDECAR_CDP_URL,
            docker_sidecar=True,
            **ALIST_CONFIG,
        )
        self.assertTrue(config.docker_sidecar)
        session = PassiveQuarkCdp(
            cdp_url=config.cdp_url,
            staging_root=config.staging_root,
            mount_path=config.mount_path,
            root_fid=config.root_fid,
            docker_sidecar=config.docker_sidecar,
        )
        self.assertEqual(session.cdp_url, DOCKER_SIDECAR_CDP_URL)
        self.assertTrue(session.docker_sidecar)

        invalid_urls = (
            "http://127.0.0.1:19222/json/list",
            "http://localhost:19222/json/list",
            "http://host.docker.internal:19223/json/list",
            "http://host.docker.internal:19222/json",
            "http://host.docker.internal:19222/json/list/",
            "http://user@host.docker.internal:19222/json/list",
            "http://host.docker.internal:19222/json/list?target=quark",
            "http://host.docker.internal:19222/json/list#fragment",
        )
        for cdp_url in invalid_urls:
            with self.subTest(cdp_url=cdp_url), self.assertRaises(
                QuarkHelperValidationError
            ):
                QuarkHelperConfig(
                    host="127.0.0.1",
                    port=18765,
                    token=TOKEN,
                    cdp_url=cdp_url,
                    docker_sidecar=True,
                    **ALIST_CONFIG,
                )

    def test_default_mode_cannot_use_docker_host_bridge(self) -> None:
        with self.assertRaises(QuarkHelperValidationError):
            QuarkHelperConfig(
                host="127.0.0.1",
                port=18765,
                token=TOKEN,
                cdp_url=DOCKER_SIDECAR_CDP_URL,
                **ALIST_CONFIG,
            )

    def test_docker_sidecar_rewrites_only_fixed_loopback_renderer_sockets(self) -> None:
        for source_host in ("127.0.0.1", "localhost"):
            with self.subTest(source_host=source_host):
                source = (
                    f"ws://{source_host}:{DOCKER_SIDECAR_CDP_PORT}"
                    "/devtools/page/quark-main"
                )
                self.assertEqual(
                    _cdp_websocket_url(
                        source,
                        discovery_host=DOCKER_SIDECAR_CDP_HOST,
                        discovery_port=DOCKER_SIDECAR_CDP_PORT,
                        docker_sidecar=True,
                    ),
                    (
                        f"ws://{DOCKER_SIDECAR_CDP_HOST}:{DOCKER_SIDECAR_CDP_PORT}"
                        "/devtools/page/quark-main"
                    ),
                )

    def test_docker_sidecar_renderer_socket_rejects_every_target_drift(self) -> None:
        invalid_urls = (
            "ws://host.docker.internal:19222/devtools/page/quark-main",
            "ws://127.0.0.2:19222/devtools/page/quark-main",
            "ws://127.0.0.1:19223/devtools/page/quark-main",
            "ws://127.0.0.1:19222/other/page/quark-main",
            "ws://127.0.0.1:19222/devtools/page/../quark-main",
            "ws://user@127.0.0.1:19222/devtools/page/quark-main",
            "ws://127.0.0.1:19222/devtools/page/quark-main?target=other",
            "ws://127.0.0.1:19222/devtools/page/quark-main#fragment",
            "wss://127.0.0.1:19222/devtools/page/quark-main",
        )
        for socket_url in invalid_urls:
            with self.subTest(socket_url=socket_url), self.assertRaises(
                QuarkHelperNotReady
            ):
                _cdp_websocket_url(
                    socket_url,
                    discovery_host=DOCKER_SIDECAR_CDP_HOST,
                    discovery_port=DOCKER_SIDECAR_CDP_PORT,
                    docker_sidecar=True,
                )

    def test_new_helper_does_not_import_legacy_control_or_generic_proxy_tools(self) -> None:
        source = Path("local/scrapeflow_api/quark_host_helper.py").read_text(encoding="utf-8")
        for forbidden in (
            "subprocess", "launchctl", "webbrowser", "/v1/quark/request",
            "SubmitJournal", "hashlib", "urllib.request",
        ):
            with self.subTest(forbidden=forbidden):
                self.assertNotIn(forbidden, source)
        self.assertNotIn("/v1/", source.replace("/v1/share-save", ""))


class AListDelegationTests(unittest.IsolatedAsyncioTestCase):
    async def test_resolver_selects_the_longest_enabled_quark_storage_per_request(self) -> None:
        class Client:
            token: str | None = None

            def __init__(self) -> None:
                self.logins = 0
                self.storage_reads = 0

            def login(self) -> None:
                self.logins += 1
                self.token = "alist-token"

            def admin_storages(self) -> list[dict[str, object]]:
                self.storage_reads += 1
                return [
                    {
                        "driver": "Quark",
                        "mount_path": "/quark",
                        "addition": json.dumps({"cookie": "base-cookie", "root_id": "0"}),
                    },
                    {
                        "driver": "Quark",
                        "mount_path": "/quark/影视",
                        "addition": json.dumps({"cookie": "nested-cookie", "root_id": "nested-root"}),
                    },
                    {
                        "driver": "Quark",
                        "mount_path": "/quark/影视/ScrapeFlow",
                        "disabled": True,
                        "addition": json.dumps({"cookie": "disabled-cookie", "root_id": "wrong"}),
                    },
                ]

        client = Client()
        resolver = AListQuarkSessionResolver(
            "http://127.0.0.1:5244",
            "admin",
            "fixture-alist-password",
            client=client,
        )

        delegated = await resolver.resolve(DESTINATION)

        self.assertEqual(delegated.mount_path, "/quark/影视")
        self.assertEqual(delegated.root_fid, "nested-root")
        self.assertEqual(client.logins, 1)
        self.assertEqual(client.storage_reads, 1)
        self.assertNotIn("nested-cookie", repr(delegated))

    async def test_delegated_https_read_keeps_cookie_in_memory_and_fixed_origin(self) -> None:
        captured: dict[str, object] = {}

        class Content:
            async def read(self, _limit: int) -> bytes:
                return b'{"code":0,"data":{}}'

        class Response:
            status = 200
            headers: dict[str, str] = {}
            content = Content()

            class Url:
                scheme = "https"
                host = "drive.quark.cn"
                path = "/1/clouddrive/file/sort"

            url = Url()

            async def __aenter__(self):
                return self

            async def __aexit__(self, *_args):
                return None

        class Session:
            def __init__(self, **kwargs: object) -> None:
                captured["session"] = kwargs

            async def __aenter__(self):
                return self

            async def __aexit__(self, *_args):
                return None

            def request(self, method: str, endpoint: str, **kwargs: object) -> Response:
                captured["method"] = method
                captured["endpoint"] = endpoint
                captured["request"] = kwargs
                return Response()

        session = PassiveQuarkCdp(
            cdp_url="http://127.0.0.1:19222/json/list",
            staging_root=DEFAULT_STAGING_ROOT,
            mount_path="/quark",
            root_fid="0",
        )
        delegated = DelegatedQuarkSession("/quark", "0", "fixture-cookie")
        with mock.patch("local.scrapeflow_api.quark_host_helper.ClientSession", Session):
            result = await session._delegated_fixed_request(
                delegated,
                origin=QUARK_DRIVE_API,
                path="/file/sort",
                method="GET",
                query={"pdir_fid": "0", "_page": 1},
                body=None,
            )

        self.assertEqual(result, {"kind": "response", "status": 200, "text": '{"code":0,"data":{}}'})
        self.assertEqual(captured["method"], "GET")
        self.assertEqual(
            captured["endpoint"], "https://drive.quark.cn/1/clouddrive/file/sort"
        )
        request = captured["request"]
        assert isinstance(request, dict)
        self.assertEqual(request["headers"], {
            "Accept": "application/json, text/plain, */*",
            "Cookie": "fixture-cookie",
            "Origin": "https://pan.quark.cn",
            "Referer": "https://pan.quark.cn/",
            "User-Agent": mock.ANY,
        })
        self.assertEqual(request["params"], {"pr": "ucpro", "fr": "pc", "pdir_fid": "0", "_page": 1})
        self.assertFalse(request["allow_redirects"])
        client_options = captured["session"]
        assert isinstance(client_options, dict)
        # The delegated Quark HTTPS honors the ambient proxy (the live route
        # for drive.quark.cn); only CDP discovery keeps an explicit direct
        # session, so this session must NOT pin trust_env=False.
        self.assertNotIn("trust_env", client_options)


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

    async def test_health_requires_bearer_and_reports_liveness_only(self) -> None:
        status, body = await self.request("GET", "/health", token="wrong")
        self.assertEqual((status, body["error"]), (401, "unauthorized"))
        status, body = await self.request("GET", "/health")
        self.assertEqual(status, 200)
        self.assertEqual(body, {"status": "ok"})
        self.assertEqual(self.session.calls, [])

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

class HelperReentryTests(unittest.IsolatedAsyncioTestCase):
    async def _select_from_discovery_rows(self, rows_factory, observed_headers=None):
        async def targets(request):
            if observed_headers is not None:
                observed_headers.append(dict(request.headers))
            return web.json_response(rows_factory(port))

        app = web.Application()
        app.router.add_get("/json/list", targets)
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
        return runner, session, port

    async def test_cdp_renderer_socket_matches_discovery_host_and_port(self) -> None:
        observed_headers: list[dict[str, str]] = []
        runner, session, port = await self._select_from_discovery_rows(
            lambda discovery_port: [{
                "type": "page",
                "title": "Quark Cloud Drive",
                "url": "https://pan.quark.cn/clouddrive/renderer/index.html?name=main",
                "webSocketDebuggerUrl": (
                    f"ws://127.0.0.1:{discovery_port}/devtools/page/quark-main"
                ),
            }],
            observed_headers,
        )
        try:
            self.assertEqual(
                await session._select_renderer_socket(),
                f"ws://127.0.0.1:{port}/devtools/page/quark-main",
            )
        finally:
            await runner.cleanup()
        self.assertEqual(observed_headers[0]["Host"], f"127.0.0.1:{port}")
        self.assertNotIn("Origin", observed_headers[0])

    async def test_docker_sidecar_discovery_uses_bridge_with_fixed_loopback_host(self) -> None:
        captured: dict[str, object] = {}
        rows = [{
            "type": "page",
            "title": "Quark Cloud Drive",
            # This is the actual main-renderer URL shape shipped in the Quark
            # 7.0.6.771 app.asar, rather than a browser-page approximation.
            "url": "uccd://cloud.quark/clouddrive/renderer/index.html?name=main",
            "webSocketDebuggerUrl": (
                f"ws://127.0.0.1:{DOCKER_SIDECAR_CDP_PORT}"
                "/devtools/page/quark-main"
            ),
        }]

        class Response:
            status = 200
            url = DOCKER_SIDECAR_CDP_URL

            async def __aenter__(self):
                return self

            async def __aexit__(self, *_args):
                return None

            async def json(self, *, content_type=None):
                self.content_type = content_type
                return rows

        class Session:
            def __init__(self, **kwargs):
                captured["session"] = kwargs

            async def __aenter__(self):
                return self

            async def __aexit__(self, *_args):
                return None

            def get(self, url, **kwargs):
                captured["get_url"] = url
                captured["get"] = kwargs
                return Response()

        session = PassiveQuarkCdp(
            cdp_url=DOCKER_SIDECAR_CDP_URL,
            staging_root=DEFAULT_STAGING_ROOT,
            mount_path="/quark",
            root_fid="0",
            docker_sidecar=True,
        )
        with mock.patch(
            "local.scrapeflow_api.quark_host_helper.ClientSession", Session,
        ):
            socket_url = await session._select_renderer_socket()

        self.assertEqual(captured["get_url"], DOCKER_SIDECAR_CDP_URL)
        self.assertEqual(captured["session"]["trust_env"], False)
        self.assertEqual(captured["get"]["headers"], {
            "Accept": "application/json",
            "Host": f"127.0.0.1:{DOCKER_SIDECAR_CDP_PORT}",
        })
        self.assertNotIn("Origin", captured["get"]["headers"])
        self.assertIs(captured["get"]["allow_redirects"], False)
        self.assertEqual(
            socket_url,
            f"ws://{DOCKER_SIDECAR_CDP_HOST}:{DOCKER_SIDECAR_CDP_PORT}"
            "/devtools/page/quark-main",
        )

    async def test_docker_sidecar_websocket_uses_bridge_with_fixed_loopback_host(self) -> None:
        captured: dict[str, object] = {}
        socket_url = (
            f"ws://{DOCKER_SIDECAR_CDP_HOST}:{DOCKER_SIDECAR_CDP_PORT}"
            "/devtools/page/quark-main"
        )

        class Session:
            def __init__(self, **kwargs):
                captured["session"] = kwargs

            async def __aenter__(self):
                return self

            async def __aexit__(self, *_args):
                return None

            def ws_connect(self, url, **kwargs):
                captured["ws_url"] = url
                captured["ws"] = kwargs
                raise RuntimeError("stop after transport capture")

        session = PassiveQuarkCdp(
            cdp_url=DOCKER_SIDECAR_CDP_URL,
            staging_root=DEFAULT_STAGING_ROOT,
            mount_path="/quark",
            root_fid="0",
            docker_sidecar=True,
        )
        with mock.patch.object(
            session,
            "_select_renderer_socket",
            new=mock.AsyncMock(return_value=socket_url),
        ), mock.patch(
            "local.scrapeflow_api.quark_host_helper.ClientSession", Session,
        ):
            with self.assertRaisesRegex(QuarkHelperNotReady, "renderer is unavailable"):
                await session._evaluate_json("JSON.stringify({status: 'probe'})")

        self.assertEqual(captured["session"]["trust_env"], False)
        self.assertEqual(captured["ws_url"], socket_url)
        self.assertEqual(captured["ws"]["headers"], {
            "Host": f"127.0.0.1:{DOCKER_SIDECAR_CDP_PORT}",
        })
        self.assertIsNone(captured["ws"]["origin"])
        self.assertIs(captured["ws"]["autoping"], True)

    async def test_cdp_renderer_socket_cannot_switch_to_another_loopback_port(self) -> None:
        runner, session, _port = await self._select_from_discovery_rows(
            lambda discovery_port: [{
                "type": "page",
                "title": "Quark Cloud Drive",
                "url": "https://pan.quark.cn/clouddrive/renderer/index.html?name=main",
                "webSocketDebuggerUrl": (
                    f"ws://127.0.0.1:{discovery_port + 1}/devtools/page/quark-main"
                ),
            }],
        )
        try:
            with self.assertRaisesRegex(QuarkHelperNotReady, "no Quark renderer"):
                await session._select_renderer_socket()
        finally:
            await runner.cleanup()

    async def test_cdp_renderer_socket_cannot_switch_loopback_host_aliases(self) -> None:
        runner, session, _port = await self._select_from_discovery_rows(
            lambda discovery_port: [{
                "type": "page",
                "title": "Quark Cloud Drive",
                "url": "https://pan.quark.cn/clouddrive/renderer/index.html?name=main",
                "webSocketDebuggerUrl": (
                    f"ws://localhost:{discovery_port}/devtools/page/quark-main"
                ),
            }],
        )
        try:
            with self.assertRaisesRegex(QuarkHelperNotReady, "no Quark renderer"):
                await session._select_renderer_socket()
        finally:
            await runner.cleanup()

    async def test_cdp_discovery_redirect_is_refused_without_following(self) -> None:
        redirected_endpoint_called = False

        async def redirect(_request):
            return web.Response(
                status=302,
                headers={"Location": "/redirected-json/list"},
            )

        async def redirected_endpoint(_request):
            nonlocal redirected_endpoint_called
            redirected_endpoint_called = True
            return web.json_response([])

        app = web.Application()
        app.router.add_get("/json/list", redirect)
        app.router.add_get("/redirected-json/list", redirected_endpoint)
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
        self.assertFalse(redirected_endpoint_called)

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

    async def test_pre_submit_loss_is_not_ready_for_share_save(self) -> None:
        session = PassiveQuarkCdp(
            cdp_url="http://127.0.0.1:9222/json/list",
            staging_root=DEFAULT_STAGING_ROOT,
            mount_path="/quark",
            root_fid="0",
        )
        with mock.patch.object(
            session,
            "_destination_fid",
            new=mock.AsyncMock(side_effect=QuarkHelperLostResponse("pre-submit")),
        ):
            with self.assertRaises(QuarkHelperNotReady):
                await session.share_save(share_payload())

    async def test_post_submit_loss_is_in_doubt_for_share_save(self) -> None:
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
