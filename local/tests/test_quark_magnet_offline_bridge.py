from __future__ import annotations

import io
import unittest
import urllib.error

from engine.scrapeflow.quark_magnet_offline_bridge import (
    HttpQuarkHelperClient,
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
    def test_dry_run_keeps_only_selected_exact_files(self) -> None:
        plan = normalize_quark_magnet_selection(_selection(), "/task/staging")

        self.assertEqual(plan["status"], "dry_run")
        self.assertEqual(plan["provider"], "quark_magnet")
        self.assertEqual(plan["infohash"], INFOHASH)
        self.assertEqual(
            plan["helper_actions"],
            ["health", "magnet-submit", "magnet-status"],
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
                return {"status": "ready"}

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
                return {"status": "ok"}

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


if __name__ == "__main__":
    unittest.main()
