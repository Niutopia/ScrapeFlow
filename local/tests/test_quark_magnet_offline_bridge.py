from __future__ import annotations

import io
import json
import os
import unittest
import urllib.error
from unittest.mock import patch

from engine.scrapeflow.quark_fast_save_bridge import (
    QuarkShareExpiredError,
    QuarkShareInDoubtError,
)
from engine.scrapeflow.provider_capabilities import QUARK_HELPER_REQUIRED_ACTIONS
from engine.scrapeflow.quark_magnet_offline_bridge import (
    DEFAULT_QUARK_HELPER_URL,
    HttpQuarkHelperClient,
    QuarkMagnetBridgeError,
    QuarkMagnetCandidateError,
    QuarkMagnetInDoubtError,
    QuarkMagnetOfflineBridge,
    normalize_quark_magnet_selection,
)


INFOHASH = "0123456789012345678901234567890123456789"


def _selection() -> dict[str, object]:
    return {
        "provider": "quark_magnet",
        "locator": f"quark_magnet:{INFOHASH}",
        "release_name": "Example Show S01E01-S01E02 1080p",
        "files": [
            "Example.Show.S01E01.mkv",
            "Example.Show.S01E02.mkv",
        ],
        "selected_gap_ids": ["S01E01"],
        "acquisition": {
            "kind": "quark_magnet_offline",
            "magnet_url": f"magnet:?xt=urn:btih:{INFOHASH}",
            "expected_files": [{
                "torrent_index": 1,
                "path": "Example.Show.S01E01.mkv",
                "size": 1024 * 1024,
                "gap_ids": ["S01E01"],
            }, {
                "torrent_index": 2,
                "path": "Example.Show.S01E02.mkv",
                "size": 1024 * 1024,
                "gap_ids": ["S01E02"],
            }],
        },
    }


class QuarkMagnetOfflineBridgeTests(unittest.TestCase):
    def test_http_client_from_env_uses_historical_default_with_token(self) -> None:
        with patch.dict(
            os.environ,
            {"SCRAPEFLOW_QUARK_HELPER_TOKEN": "abcdefghijklmnopqrstuvwxyz"},
            clear=True,
        ):
            client = HttpQuarkHelperClient.from_env()

        self.assertEqual(client.base_url, DEFAULT_QUARK_HELPER_URL)
        self.assertEqual(client.token, "abcdefghijklmnopqrstuvwxyz")

    def test_http_client_from_env_still_requires_token(self) -> None:
        with patch.dict(os.environ, {}, clear=True), self.assertRaisesRegex(
            QuarkMagnetBridgeError, "token is missing or too short",
        ):
            HttpQuarkHelperClient.from_env()

    def test_http_client_rejects_external_helper_hosts_for_every_scheme(self) -> None:
        token = "abcdefghijklmnopqrstuvwxyz"

        for url in (
            "http://example.com:8765",
            "https://example.com:8765",
            "https://198.51.100.9:8765",
            "https://[2001:db8::9]:8765",
        ):
            with self.subTest(url=url), self.assertRaises(QuarkMagnetBridgeError):
                HttpQuarkHelperClient(url, token)

        # TLS does not relax the local-only boundary; numeric loopback and
        # Docker's explicit host bridge remain the only valid target shapes.
        HttpQuarkHelperClient("https://127.0.0.1:8765", token)
        HttpQuarkHelperClient("http://host.docker.internal:8765", token)

    def test_http_client_refuses_redirect_without_a_second_request(self) -> None:
        calls = []

        def opener(request, timeout):
            del timeout
            calls.append(request.full_url)
            raise urllib.error.HTTPError(
                request.full_url,
                302,
                "Found",
                {"Location": "https://example.com/steal"},
                io.BytesIO(b""),
            )

        client = HttpQuarkHelperClient(
            "http://127.0.0.1:8765",
            "abcdefghijklmnopqrstuvwxyz",
            opener=opener,
        )

        with self.assertRaisesRegex(QuarkMagnetBridgeError, "redirect refused"):
            client.health()

        self.assertEqual(calls, ["http://127.0.0.1:8765/health"])

    def test_http_share_save_uses_fixed_typed_endpoint(self) -> None:
        calls = []

        class Response(io.BytesIO):
            def __enter__(self):
                return self

            def __exit__(self, *_args):
                self.close()

        def opener(request, timeout):
            calls.append((request, timeout))
            return Response(b'{"status":"finished","task_id":"share-task-1"}')

        client = HttpQuarkHelperClient(
            "http://127.0.0.1:8765",
            "abcdefghijklmnopqrstuvwxyz",
            opener=opener,
        )
        plan = {
            "attempt_id": "attempt-one",
            "destination": "/quark/ScrapeFlow/attempt-one",
            "share_id": "fixture-share",
            "passcode": "1234",
            "selected_gap_ids": ["S01E01"],
            "expected_files": [{
                "file_id": "share-fid",
                "path": "Season 01/Example.Show.S01E01.mkv",
                "name": "Example.Show.S01E01.mkv",
                "size": 123,
                "gap_ids": ["S01E01"],
            }],
        }

        result = client.share_save(plan)

        self.assertEqual(result, {"status": "finished", "task_id": "share-task-1"})
        self.assertEqual(len(calls), 1)
        request, _timeout = calls[0]
        self.assertEqual(request.full_url, "http://127.0.0.1:8765/v1/share-save")
        self.assertEqual(request.get_method(), "POST")
        self.assertEqual(json.loads(request.data.decode("utf-8")), plan)
        self.assertEqual(request.get_header("Authorization"), "Bearer abcdefghijklmnopqrstuvwxyz")

    def test_share_save_timeout_is_in_doubt(self) -> None:
        def opener(_request, timeout):
            del timeout
            raise TimeoutError("response lost after share-save")

        client = HttpQuarkHelperClient(
            "http://127.0.0.1:8765",
            "abcdefghijklmnopqrstuvwxyz",
            opener=opener,
        )

        with self.assertRaises(QuarkShareInDoubtError) as raised:
            client.share_save({"destination": "/quark/attempt"})

        self.assertEqual(raised.exception.failure_scope, "in_doubt")

    def test_share_save_candidate_rejection_is_not_in_doubt(self) -> None:
        def opener(_request, timeout):
            del timeout
            raise urllib.error.HTTPError(
                "http://127.0.0.1:8765/v1/share-save",
                422,
                "Unprocessable Entity",
                {},
                io.BytesIO(b'{"failure_scope":"candidate"}'),
            )

        client = HttpQuarkHelperClient(
            "http://127.0.0.1:8765",
            "abcdefghijklmnopqrstuvwxyz",
            opener=opener,
        )

        with self.assertRaises(QuarkShareExpiredError) as raised:
            client.share_save({"destination": "/quark/attempt"})

        self.assertEqual(raised.exception.failure_scope, "candidate")

    def test_missing_share_save_route_is_infrastructure_not_candidate(self) -> None:
        def opener(_request, timeout):
            del timeout
            raise urllib.error.HTTPError(
                "http://127.0.0.1:8765/v1/share-save",
                404,
                "Not Found",
                {},
                io.BytesIO(b'{"error":"not_found"}'),
            )

        client = HttpQuarkHelperClient(
            "http://127.0.0.1:8765",
            "abcdefghijklmnopqrstuvwxyz",
            opener=opener,
        )

        with self.assertRaises(QuarkMagnetBridgeError) as raised:
            client.share_save({"destination": "/quark/attempt"})

        self.assertEqual(raised.exception.failure_scope, "infrastructure")

    def test_dry_run_keeps_only_selected_exact_files(self) -> None:
        plan = normalize_quark_magnet_selection(_selection(), "/task/staging")

        self.assertEqual(plan["status"], "dry_run")
        self.assertEqual(plan["provider"], "quark_magnet")
        self.assertEqual(plan["infohash"], INFOHASH)
        self.assertEqual(
            plan["helper_actions"],
            list(QUARK_HELPER_REQUIRED_ACTIONS),
        )
        self.assertEqual(plan["expected_files"], [{
            "torrent_index": 1,
            "path": "Example.Show.S01E01.mkv",
            "size": 1024 * 1024,
            "gap_ids": ["S01E01"],
        }])

    def test_execute_uses_only_the_fixed_helper_actions(self) -> None:
        calls: list[tuple[str, object]] = []

        class Helper:
            def health(self):
                calls.append(("health", None))
                return {
                    "status": "ready",
                    "authenticated": True,
                    "actions": list(QUARK_HELPER_REQUIRED_ACTIONS),
                }

            def magnet_submit(self, plan):
                calls.append(("magnet-submit", dict(plan)))
                return {"task_id": "quark-offline-task-1"}

            def magnet_status(self, task_id):
                calls.append(("magnet-status", task_id))
                return {"status": "done"}

        bridge = QuarkMagnetOfflineBridge(Helper(), sleep=lambda _seconds: None)
        result = bridge.execute(_selection(), "/task/staging")

        self.assertEqual(result["status"], "submitted")
        self.assertEqual(result["task_id"], "quark-offline-task-1")
        self.assertEqual(
            [name for name, _payload in calls],
            ["health", "magnet-submit", "magnet-status"],
        )
        submitted = calls[1][1]
        self.assertEqual(set(submitted), {
            "destination",
            "expected_files",
            "infohash",
            "magnet_url",
            "selected_gap_ids",
            "title",
        })

    def test_execute_with_existing_task_id_only_queries_status(self) -> None:
        calls: list[tuple[str, object]] = []

        class Helper:
            def health(self):
                calls.append(("health", None))
                return {
                    "status": "ok",
                    "authenticated": True,
                    "actions": list(QUARK_HELPER_REQUIRED_ACTIONS),
                }

            def magnet_submit(self, _plan):
                raise AssertionError("existing task must not be submitted again")

            def magnet_status(self, task_id):
                calls.append(("magnet-status", task_id))
                return {"status": "success"}

        bridge = QuarkMagnetOfflineBridge(Helper(), sleep=lambda _seconds: None)
        result = bridge.execute(
            _selection(),
            "/task/staging",
            task_id="quark-offline-task-1",
        )

        self.assertEqual(result["task_id"], "quark-offline-task-1")
        self.assertEqual(
            calls,
            [("health", None), ("magnet-status", "quark-offline-task-1")],
        )

    def test_execute_rejects_partial_or_unauthenticated_helper_health(self) -> None:
        for health in (
            {"status": "ready", "authenticated": False, "actions": list(QUARK_HELPER_REQUIRED_ACTIONS)},
            {"status": "ready", "authenticated": True, "actions": ["health", "magnet-submit", "magnet-status"]},
        ):
            class Helper:
                def health(self):
                    return health

                def magnet_submit(self, _plan):
                    raise AssertionError("invalid helper health must stop before submit")

                def magnet_status(self, _task_id):
                    raise AssertionError("invalid helper health must stop before status")

            with self.subTest(health=health), self.assertRaisesRegex(
                QuarkMagnetBridgeError, "helper (is not authenticated|actions do not match)",
            ):
                QuarkMagnetOfflineBridge(Helper(), sleep=lambda _seconds: None).execute(
                    _selection(), "/task/staging",
                )

    def test_http_409_in_doubt_is_not_a_candidate_failure(self) -> None:
        def opener(_request, timeout):
            del timeout
            raise urllib.error.HTTPError(
                "http://127.0.0.1:8765/v1/magnet-submit",
                409,
                "Conflict",
                {},
                io.BytesIO(b'{"in_doubt": true, "message": "submitted maybe"}'),
            )

        client = HttpQuarkHelperClient(
            "http://127.0.0.1:8765",
            "abcdefghijklmnopqrstuvwxyz",
            opener=opener,
        )

        with self.assertRaises(QuarkMagnetInDoubtError) as raised:
            client.magnet_submit({"destination": "/task/staging"})
        self.assertEqual(raised.exception.failure_scope, "in_doubt")

    def test_http_magnet_submit_explicit_candidate_rejection_is_candidate_failure(self) -> None:
        for status, body in (
            (422, b'{"error":"invalid_candidate"}'),
            (400, b'{"failure_scope":"candidate"}'),
        ):
            with self.subTest(status=status, body=body):
                def opener(_request, timeout):
                    del timeout
                    raise urllib.error.HTTPError(
                        "http://127.0.0.1:8765/v1/magnet-submit",
                        status,
                        "Rejected",
                        {},
                        io.BytesIO(body),
                    )

                client = HttpQuarkHelperClient(
                    "http://127.0.0.1:8765",
                    "abcdefghijklmnopqrstuvwxyz",
                    opener=opener,
                )

                with self.assertRaises(QuarkMagnetCandidateError) as raised:
                    client.magnet_submit({"destination": "/task/staging"})
                self.assertEqual(raised.exception.failure_scope, "candidate")
                self.assertTrue(raised.exception.exclude_candidate)

    def test_http_share_save_503_is_known_helper_outage(self) -> None:
        def opener(request, timeout):
            del timeout
            raise urllib.error.HTTPError(
                request.full_url,
                503,
                "Service Unavailable",
                {},
                io.BytesIO(b'{"status":"not_ready"}'),
            )

        client = HttpQuarkHelperClient(
            "http://127.0.0.1:8765",
            "abcdefghijklmnopqrstuvwxyz",
            opener=opener,
        )

        with self.assertRaises(QuarkMagnetBridgeError) as raised:
            client.share_save({"destination": "/task/staging"})

        self.assertEqual(raised.exception.failure_scope, "infrastructure")
        self.assertNotIsInstance(raised.exception, QuarkShareInDoubtError)

    def test_http_magnet_submit_503_is_known_helper_outage(self) -> None:
        def opener(request, timeout):
            del timeout
            raise urllib.error.HTTPError(
                request.full_url,
                503,
                "Service Unavailable",
                {},
                io.BytesIO(b'{"status":"not_ready"}'),
            )

        client = HttpQuarkHelperClient(
            "http://127.0.0.1:8765",
            "abcdefghijklmnopqrstuvwxyz",
            opener=opener,
        )

        with self.assertRaises(QuarkMagnetBridgeError) as raised:
            client.magnet_submit({"destination": "/task/staging"})

        self.assertEqual(raised.exception.failure_scope, "infrastructure")
        self.assertNotIsInstance(raised.exception, QuarkMagnetInDoubtError)

    def test_submit_timeout_is_in_doubt_and_not_safe_to_retry(self) -> None:
        def opener(_request, timeout):
            del timeout
            raise TimeoutError("response lost after submit")

        client = HttpQuarkHelperClient(
            "http://127.0.0.1:8765",
            "abcdefghijklmnopqrstuvwxyz",
            opener=opener,
        )

        with self.assertRaises(QuarkMagnetInDoubtError) as raised:
            client.magnet_submit({"destination": "/task/staging"})

        self.assertEqual(raised.exception.failure_scope, "in_doubt")

    def test_submit_without_task_id_is_in_doubt(self) -> None:
        class Helper:
            def health(self):
                return {
                    "status": "ready",
                    "authenticated": True,
                    "actions": list(QUARK_HELPER_REQUIRED_ACTIONS),
                }

            def magnet_submit(self, _plan):
                return {"accepted": True}

            def magnet_status(self, _task_id):
                raise AssertionError("task status is unavailable without task_id")

        bridge = QuarkMagnetOfflineBridge(Helper(), sleep=lambda _seconds: None)

        with self.assertRaises(QuarkMagnetInDoubtError) as raised:
            bridge.execute(_selection(), "/task/staging")

        self.assertEqual(raised.exception.failure_scope, "in_doubt")


if __name__ == "__main__":
    unittest.main()
