import json
import unittest
from unittest import mock

from engine.tools.plan_one_time_library_completion import read_live_pause_control


class _Response:
    def __init__(self, payload):
        self.payload = json.dumps(payload).encode("utf-8")

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def read(self, _limit):
        return self.payload


class OneTimeLibraryControlTests(unittest.TestCase):
    def test_accepts_live_persistent_pause_without_duplicate_scheduler_state(self):
        payload = {
            "paused": True,
            "persistent": True,
            "updated_at": "2026-08-03T00:00:00+00:00",
        }
        with mock.patch(
            "engine.tools.plan_one_time_library_completion.urlopen",
            return_value=_Response(payload),
        ):
            self.assertEqual(
                read_live_pause_control("http://127.0.0.1:3010/api/control"),
                payload,
            )

    def test_rejects_nonpersistent_live_pause(self):
        payload = {
            "paused": True,
            "persistent": False,
        }
        with mock.patch(
            "engine.tools.plan_one_time_library_completion.urlopen",
            return_value=_Response(payload),
        ):
            with self.assertRaisesRegex(ValueError, "持久暂停"):
                read_live_pause_control("http://localhost:3010/api/control")

    def test_rejects_non_loopback_or_credentialed_control_url(self):
        for url in (
            "https://127.0.0.1/api/control",
            "http://example.com/api/control",
            "http://user:password@127.0.0.1/api/control",
        ):
            with self.subTest(url=url), self.assertRaisesRegex(ValueError, "本机 HTTP"):
                read_live_pause_control(url)

    def test_explicit_compose_host_allowlist_is_narrow(self):
        payload = {
            "paused": True, "persistent": True,
        }
        with mock.patch(
            "engine.tools.plan_one_time_library_completion.urlopen",
            return_value=_Response(payload),
        ):
            self.assertEqual(
                read_live_pause_control(
                    "http://api:8765/api/control", allowed_hosts=frozenset({"api"}),
                    host_header="localhost",
                ),
                payload,
            )
        with self.assertRaisesRegex(ValueError, "明确允许"):
            read_live_pause_control(
                "http://evil:3010/api/control", allowed_hosts=frozenset({"api"}),
            )
        with self.assertRaisesRegex(ValueError, "Host"):
            read_live_pause_control(
                "http://api:8765/api/control", allowed_hosts=frozenset({"api"}),
                host_header="evil.example",
            )


if __name__ == "__main__":
    unittest.main()
