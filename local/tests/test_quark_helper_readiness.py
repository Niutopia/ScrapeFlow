"""Tests for the redacted loopback-sidecar Quark Helper projection."""

from __future__ import annotations

import unittest

from engine.scrapeflow.provider_capabilities import QUARK_HELPER_REQUIRED_ACTIONS
from local.scrapeflow_api.quark_helper_readiness import (
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
                3.0,
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


if __name__ == "__main__":
    unittest.main()
