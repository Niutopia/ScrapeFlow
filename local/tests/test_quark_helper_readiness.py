"""Tests for the redacted loopback-sidecar Quark Helper projection."""

from __future__ import annotations

import threading
import time
import unittest

from engine.scrapeflow.provider_capabilities import QUARK_HELPER_REQUIRED_ACTIONS
from local.scrapeflow_api.quark_helper_readiness import (
    QuarkHelperReadinessCache,
    quark_helper_readiness_from_env,
)


ENV = {
    "SCRAPEFLOW_QUARK_HELPER_URL": "http://host.docker.internal:18765",
    "SCRAPEFLOW_QUARK_HELPER_TOKEN": "abcdefghijklmnopqrstuvwxyz012345",
}


def helper_factory(payload: object):
    class Client:
        def health(self):
            return payload

    return lambda _url, _token, _timeout: Client()


class QuarkHelperReadinessTests(unittest.TestCase):
    def test_missing_configuration_is_explicit_and_redacted(self) -> None:
        factory_called = False

        def forbidden_factory(_url: str, _token: str, _timeout: float):
            nonlocal factory_called
            factory_called = True
            raise AssertionError("missing token must stop before health")

        report = quark_helper_readiness_from_env(
            {}, client_factory=forbidden_factory,
        )

        self.assertEqual(report["status"], "not_configured")
        self.assertFalse(report["configured"])
        self.assertFalse(report["reachable"])
        self.assertTrue(report["url_configured"])
        self.assertFalse(report["token_configured"])
        self.assertFalse(factory_called)
        self.assertNotIn("abcdefghijklmnopqrstuvwxyz012345", str(report))
        self.assertNotIn("host.docker.internal:18765", str(report))

    def test_token_only_configuration_uses_loopback_sidecar_default(self) -> None:
        calls: list[tuple[str, str, float]] = []

        def factory(url: str, token: str, timeout: float):
            calls.append((url, token, timeout))

            class Client:
                def health(self):
                    return {
                        "status": "ready",
                        "authenticated": True,
                        "actions": list(QUARK_HELPER_REQUIRED_ACTIONS),
                    }

            return Client()

        report = quark_helper_readiness_from_env(
            {"SCRAPEFLOW_QUARK_HELPER_TOKEN": "abcdefghijklmnopqrstuvwxyz012345"},
            client_factory=factory,
        )

        self.assertEqual(report["status"], "ready")
        self.assertTrue(report["configured"])
        self.assertTrue(report["url_configured"])
        self.assertEqual(
            calls,
            [(
                "http://127.0.0.1:18765",
                "abcdefghijklmnopqrstuvwxyz012345",
                20.0,
            )],
        )
        self.assertNotIn("127.0.0.1:18765", str(report))

    def test_short_token_is_invalid_and_never_builds_a_client(self) -> None:
        factory_called = False

        def forbidden_factory(_url: str, _token: str, _timeout: float):
            nonlocal factory_called
            factory_called = True
            raise AssertionError("short token must stop before health")

        report = quark_helper_readiness_from_env(
            {"SCRAPEFLOW_QUARK_HELPER_TOKEN": "too-short"},
            client_factory=forbidden_factory,
        )

        self.assertEqual(report["status"], "invalid_configuration")
        self.assertTrue(report["configured"])
        self.assertTrue(report["token_configured"])
        self.assertFalse(report["reachable"])
        self.assertFalse(factory_called)
        self.assertNotIn("too-short", str(report))

    def test_explicit_external_url_is_rejected_and_redacted(self) -> None:
        report = quark_helper_readiness_from_env({
            "SCRAPEFLOW_QUARK_HELPER_URL": "http://example.com:18765",
            "SCRAPEFLOW_QUARK_HELPER_TOKEN": "abcdefghijklmnopqrstuvwxyz012345",
        })

        self.assertEqual(report["status"], "invalid_configuration")
        self.assertFalse(report["reachable"])
        self.assertNotIn("example.com", str(report))
        self.assertNotIn("abcdefghijklmnopqrstuvwxyz012345", str(report))

    def test_ready_requires_authenticated_exact_fixed_actions(self) -> None:
        report = quark_helper_readiness_from_env(
            ENV,
            client_factory=helper_factory({
                "status": "ready",
                "authenticated": True,
                "actions": list(QUARK_HELPER_REQUIRED_ACTIONS),
            }),
        )

        self.assertEqual(report["status"], "ready")
        self.assertTrue(report["reachable"])
        self.assertTrue(report["authenticated"])
        self.assertEqual(report["actions"], list(QUARK_HELPER_REQUIRED_ACTIONS))

    def test_incomplete_health_payload_is_not_ready(self) -> None:
        report = quark_helper_readiness_from_env(
            ENV,
            client_factory=helper_factory({
                "status": "ok",
                "authenticated": True,
                "actions": ["health", "magnet-submit"],
            }),
        )

        self.assertEqual(report["status"], "not_ready")
        self.assertTrue(report["reachable"])
        self.assertIn("fixed contract", report["reason"])

    def test_health_transport_failure_is_not_misreported_as_candidate_absence(self) -> None:
        def unavailable(_url: str, _token: str, _timeout: float):
            class Client:
                def health(self):
                    raise TimeoutError("helper did not answer")

            return Client()

        report = quark_helper_readiness_from_env(ENV, client_factory=unavailable)

        self.assertEqual(report["status"], "unreachable")
        self.assertFalse(report["reachable"])
        self.assertNotIn("helper did not answer", str(report))


class QuarkHelperReadinessCacheTests(unittest.TestCase):
    def test_ordinary_snapshot_returns_immediately_while_slow_probe_runs(self) -> None:
        started = threading.Event()
        release = threading.Event()
        calls: list[float] = []

        def factory(_url: str, _token: str, timeout: float):
            calls.append(timeout)

            class Client:
                def health(self):
                    started.set()
                    if not release.wait(2):
                        raise AssertionError("test probe was not released")
                    return {
                        "status": "ready",
                        "authenticated": True,
                        "actions": list(QUARK_HELPER_REQUIRED_ACTIONS),
                    }

            return Client()

        cache = QuarkHelperReadinessCache(
            client_factory=factory,
            probe_timeout=15,
        )
        before = time.monotonic()
        pending = cache.snapshot(ENV)
        elapsed = time.monotonic() - before

        self.assertLess(elapsed, 0.2)
        self.assertEqual(pending["status"], "not_verified")
        self.assertIsNone(pending["reachable"])
        self.assertFalse(pending["verified"])
        self.assertTrue(pending["refreshing"])
        self.assertTrue(started.wait(1))
        self.assertEqual(calls, [15.0])

        release.set()
        ready = self._wait_for_status(cache, "ready")
        self.assertTrue(ready["fresh"])
        self.assertTrue(ready["verified"])

    def test_concurrent_health_snapshots_share_one_probe(self) -> None:
        started = threading.Event()
        release = threading.Event()
        factory_calls = 0
        factory_lock = threading.Lock()

        def factory(_url: str, _token: str, _timeout: float):
            nonlocal factory_calls
            with factory_lock:
                factory_calls += 1

            class Client:
                def health(self):
                    started.set()
                    if not release.wait(2):
                        raise AssertionError("test probe was not released")
                    return {
                        "status": "ready",
                        "authenticated": True,
                        "actions": list(QUARK_HELPER_REQUIRED_ACTIONS),
                    }

            return Client()

        cache = QuarkHelperReadinessCache(client_factory=factory)
        barrier = threading.Barrier(9)
        reports: list[dict[str, object]] = []
        reports_lock = threading.Lock()

        def reader() -> None:
            barrier.wait()
            report = cache.snapshot(ENV)
            with reports_lock:
                reports.append(report)

        threads = [threading.Thread(target=reader) for _ in range(8)]
        for thread in threads:
            thread.start()
        barrier.wait()
        for thread in threads:
            thread.join(1)

        self.assertTrue(started.wait(1))
        self.assertEqual(factory_calls, 1)
        self.assertEqual(len(reports), 8)
        self.assertTrue(all(report["status"] == "not_verified" for report in reports))
        release.set()
        self.assertEqual(self._wait_for_status(cache, "ready")["status"], "ready")

    def test_expired_ready_result_is_stale_then_failed_refresh_is_not_green(self) -> None:
        now = [10.0]
        failed = threading.Event()
        attempts = 0

        def factory(_url: str, _token: str, _timeout: float):
            nonlocal attempts
            attempts += 1
            attempt = attempts

            class Client:
                def health(self):
                    if attempt == 1:
                        return {
                            "status": "ready",
                            "authenticated": True,
                            "actions": list(QUARK_HELPER_REQUIRED_ACTIONS),
                        }
                    failed.set()
                    raise TimeoutError("private renderer detail must not leak")

            return Client()

        cache = QuarkHelperReadinessCache(
            client_factory=factory,
            ttl_seconds=5,
            monotonic=lambda: now[0],
        )
        first = cache.probe_now(ENV, wait_timeout=1)
        self.assertEqual(first["status"], "ready")
        self.assertTrue(first["fresh"])
        self.assertTrue(first["verified"])

        now[0] = 16.0
        stale = cache.snapshot(ENV)
        self.assertEqual(stale["status"], "stale")
        self.assertTrue(stale["stale"])
        self.assertFalse(stale["verified"])
        self.assertIsNone(stale["reachable"])
        self.assertEqual(stale["last_status"], "ready")
        self.assertTrue(stale["refreshing"])

        self.assertTrue(failed.wait(1))
        failed_view = self._wait_for_status(cache, "unreachable")
        self.assertTrue(failed_view["fresh"])
        self.assertFalse(failed_view["verified"])
        self.assertEqual(failed_view["last_error"], "helper health request failed")
        self.assertNotIn("private renderer detail", str(failed_view))

    def test_explicit_probe_joins_existing_background_probe(self) -> None:
        started = threading.Event()
        release = threading.Event()
        calls = 0
        calls_lock = threading.Lock()

        def factory(_url: str, _token: str, _timeout: float):
            nonlocal calls
            with calls_lock:
                calls += 1

            class Client:
                def health(self):
                    started.set()
                    if not release.wait(2):
                        raise AssertionError("test probe was not released")
                    return {
                        "status": "ready",
                        "authenticated": True,
                        "actions": list(QUARK_HELPER_REQUIRED_ACTIONS),
                    }

            return Client()

        cache = QuarkHelperReadinessCache(client_factory=factory, probe_timeout=5)
        self.assertEqual(cache.snapshot(ENV)["status"], "not_verified")
        self.assertTrue(started.wait(1))
        result: dict[str, object] = {}

        def explicit() -> None:
            result.update(cache.probe_now(ENV, wait_timeout=1))

        thread = threading.Thread(target=explicit)
        thread.start()
        time.sleep(0.02)
        self.assertEqual(calls, 1)
        release.set()
        thread.join(1)

        self.assertEqual(calls, 1)
        self.assertEqual(result["status"], "ready")
        self.assertEqual(result["probe_mode"], "explicit")
        self.assertTrue(result["fresh"])

    @staticmethod
    def _wait_for_status(
        cache: QuarkHelperReadinessCache,
        status: str,
    ) -> dict[str, object]:
        deadline = time.monotonic() + 1
        latest: dict[str, object] = {}
        while time.monotonic() < deadline:
            latest = cache.snapshot(ENV)
            if latest.get("status") == status:
                return latest
            time.sleep(0.005)
        raise AssertionError(f"did not observe {status}: {latest}")


if __name__ == "__main__":
    unittest.main()
