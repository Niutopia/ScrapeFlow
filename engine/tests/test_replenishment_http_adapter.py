from __future__ import annotations

import os
from unittest import mock
import unittest

from engine.tools import replenishment_http_adapter as adapter


class ReplenishmentHttpAdapterTests(unittest.TestCase):
    def test_ready_acquisition_returns_immediately(self):
        result = {"status": "ready", "source_paths": ["/quark/影视/待刮削/Example"]}
        self.assertIs(adapter._wait_for_acquisition(result), result)

    def test_queued_acquisition_polls_until_ready(self):
        queued = {"status": "queued", "operation_id": "op-42"}
        ready = {"status": "ready", "source_paths": ["/quark/影视/待刮削/Example"]}
        with mock.patch.dict(os.environ, {
            "SCRAPEFLOW_REPLENISHMENT_STATUS_URL": "https://adapter.example/status",
            "SCRAPEFLOW_REPLENISHMENT_ACQUIRE_TIMEOUT": "60",
            "SCRAPEFLOW_REPLENISHMENT_POLL_INTERVAL": "1",
        }), mock.patch.object(adapter.time, "monotonic", side_effect=[0, 0, 1]), mock.patch.object(
            adapter.time, "sleep",
        ) as sleep_mock, mock.patch.object(
            adapter, "_post_json", return_value=ready,
        ) as post_mock:
            self.assertEqual(adapter._wait_for_acquisition(queued), ready)
        sleep_mock.assert_called_once_with(1.0)
        post_mock.assert_called_once_with(
            "https://adapter.example/status", {"operation_id": "op-42"},
        )

    def test_known_failed_acquisition_preserves_provider_message(self):
        with self.assertRaisesRegex(RuntimeError, "storage quota"):
            adapter._wait_for_acquisition({"status": "failed", "message": "storage quota"})

    def test_queued_acquisition_requires_operation_id(self):
        with self.assertRaisesRegex(ValueError, "operation_id"):
            adapter._wait_for_acquisition({"status": "queued"})


if __name__ == "__main__":
    unittest.main()
