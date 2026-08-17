"""Tests for read-only runtime readiness checks."""

from __future__ import annotations

import unittest

from engine.scrapeflow.provider_capabilities import QUARK_HELPER_REQUIRED_ACTIONS
from local.scrapeflow_api.runtime_readiness import runtime_readiness_report


def healthy_payload(*, commit: str = "abc1234", jobs_total: int = 0) -> dict[str, object]:
    return {
        "ok": True,
        "mode": "automatic",
        "connected": True,
        "tmdb_configured": True,
        "engine_configured": True,
        "build_commit": commit,
        "provider_capabilities": {
            "quark_share": {"status": "ready"},
            "alist_offline": {"status": "ready"},
            "magnet": {"status": "ready"},
        },
        "helper_readiness": {
            "quark": {
                "configured": True,
                "reachable": True,
                "authenticated": True,
                "status": "ready",
                "required_actions": list(QUARK_HELPER_REQUIRED_ACTIONS),
                "actions": list(QUARK_HELPER_REQUIRED_ACTIONS),
            },
        },
        "lane_gates": {
            "provider_auto_repair_enabled": False,
            "audit_auto_repair_enabled": False,
        },
        "intake_monitoring": False,
        "intake": {"enabled": False},
        "operations": {
            "jobs_total": jobs_total,
            "jobs_active": 0,
            "formal_write_workers": 0,
            "provider_workers": 0,
            "provider_active": 0,
            "audit_running": False,
        },
    }


def paused_control() -> dict[str, object]:
    return {
        "paused": True,
        "scheduler_paused": True,
        "persistent": True,
    }


def fake_fetcher(
    health: dict[str, object] | None = None,
    control: dict[str, object] | None = None,
):
    health_payload = healthy_payload() if health is None else health
    control_payload = paused_control() if control is None else control

    def fetch(url: str, timeout: float) -> tuple[int, object]:
        if url.endswith("/api/health"):
            return 200, health_payload
        if url.endswith("/api/control"):
            return 200, control_payload
        return 404, {}

    return fetch


class RuntimeReadinessTests(unittest.TestCase):
    def test_ready_runtime_passes(self) -> None:
        report = runtime_readiness_report(
            api_url="http://127.0.0.1:8765",
            expected_commit="abc1234",
            fetch_json=fake_fetcher(),
        )

        self.assertEqual(report["status"], "通过")
        self.assertEqual(report["issues"], [])

    def test_non_loopback_api_url_fails_without_fetching(self) -> None:
        calls: list[str] = []

        def fetch(url: str, timeout: float) -> tuple[int, object]:
            calls.append(url)
            return 200, {}

        report = runtime_readiness_report(
            api_url="http://192.0.2.10:8765",
            fetch_json=fetch,
        )

        self.assertEqual(report["status"], "失败")
        self.assertEqual(calls, [])
        self.assertTrue(any("loopback" in issue for issue in report["issues"]))

    def test_gate_or_intake_drift_fails(self) -> None:
        health = healthy_payload()
        health["lane_gates"] = {
            "provider_auto_repair_enabled": True,
            "audit_auto_repair_enabled": False,
        }
        health["intake_monitoring"] = True
        health["intake"] = {"enabled": True}

        report = runtime_readiness_report(
            api_url="http://localhost:8765",
            fetch_json=fake_fetcher(health=health),
        )

        self.assertEqual(report["status"], "失败")
        self.assertTrue(any("provider auto repair gate" in issue for issue in report["issues"]))
        self.assertTrue(any("intake monitoring" in issue for issue in report["issues"]))
        self.assertTrue(any("intake.enabled" in issue for issue in report["issues"]))

    def test_unpaused_control_fails(self) -> None:
        control = paused_control()
        control["paused"] = False
        control["scheduler_paused"] = False

        report = runtime_readiness_report(
            api_url="http://127.0.0.1:8765",
            fetch_json=fake_fetcher(control=control),
        )

        self.assertEqual(report["status"], "失败")
        self.assertTrue(any("paused must be true" in issue for issue in report["issues"]))
        self.assertTrue(any("scheduler_paused must be true" in issue for issue in report["issues"]))

    def test_commit_mismatch_fails(self) -> None:
        report = runtime_readiness_report(
            api_url="http://127.0.0.1:8765",
            expected_commit="def4567",
            fetch_json=fake_fetcher(),
        )

        self.assertEqual(report["status"], "失败")
        self.assertTrue(any("build_commit" in issue for issue in report["issues"]))

    def test_existing_jobs_fail_unless_explicitly_allowed(self) -> None:
        fetch = fake_fetcher(health=healthy_payload(jobs_total=2))

        blocked = runtime_readiness_report(
            api_url="http://127.0.0.1:8765",
            fetch_json=fetch,
        )
        allowed = runtime_readiness_report(
            api_url="http://127.0.0.1:8765",
            allow_existing_jobs=True,
            fetch_json=fetch,
        )

        self.assertEqual(blocked["status"], "失败")
        self.assertTrue(any("jobs_total" in issue for issue in blocked["issues"]))
        self.assertEqual(allowed["status"], "通过")

    def test_static_lane_status_cannot_substitute_for_helper_readiness(self) -> None:
        health = healthy_payload()
        del health["helper_readiness"]

        report = runtime_readiness_report(
            api_url="http://127.0.0.1:8765",
            fetch_json=fake_fetcher(health=health),
        )

        self.assertEqual(report["status"], "失败")
        self.assertIn(
            "health.helper_readiness must be an object",
            report["issues"],
        )

    def test_helper_requires_authenticated_fixed_action_contract(self) -> None:
        health = healthy_payload()
        helper = health["helper_readiness"]["quark"]
        helper["authenticated"] = False
        helper["actions"] = ["health"]

        report = runtime_readiness_report(
            api_url="http://127.0.0.1:8765",
            fetch_json=fake_fetcher(health=health),
        )

        self.assertEqual(report["status"], "失败")
        self.assertTrue(any("authenticated" in issue for issue in report["issues"]))
        self.assertTrue(any("actions" in issue for issue in report["issues"]))

    def test_lane_status_is_only_a_capability_declaration(self) -> None:
        health = healthy_payload()
        health["provider_capabilities"]["alist_offline"]["status"] = "unavailable"

        report = runtime_readiness_report(
            api_url="http://127.0.0.1:8765",
            fetch_json=fake_fetcher(health=health),
        )

        self.assertEqual(report["status"], "通过")


if __name__ == "__main__":
    unittest.main()
