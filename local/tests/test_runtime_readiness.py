"""Tests for read-only runtime readiness checks."""

from __future__ import annotations

import unittest
from unittest.mock import patch

from engine.scrapeflow.provider_capabilities import QUARK_HELPER_REQUIRED_ACTIONS
from local.scrapeflow_api.runtime_readiness import runtime_readiness_report
from scripts.scrapeflow_runtime_readiness import main as runtime_readiness_main


def healthy_payload(
    *,
    commit: str = "abc1234",
    jobs_total: int = 0,
    jobs_active: int = 0,
) -> dict[str, object]:
    return {
        "ok": True,
        "mode": "automatic",
        "connected": True,
        "tmdb_configured": True,
        "engine_configured": True,
        "build_commit": commit,
        "build_time": "2026-08-18T00:00:00Z",
        "provider_capabilities": {
            "quark_share": {"status": "ready"},
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
            "jobs_active": jobs_active,
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
            expected_commit="abc1234",
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
            expected_commit="abc1234",
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
        # A reused but paused deployment can retain queued RootJobs.  It must
        # still be rejected by default, while the explicit flag retains the
        # zero-worker / paused invariants and allows that known-safe state.
        fetch = fake_fetcher(health=healthy_payload(jobs_total=2, jobs_active=2))

        blocked = runtime_readiness_report(
            api_url="http://127.0.0.1:8765",
            expected_commit="abc1234",
            fetch_json=fetch,
        )
        allowed = runtime_readiness_report(
            api_url="http://127.0.0.1:8765",
            expected_commit="abc1234",
            allow_existing_jobs=True,
            fetch_json=fetch,
        )

        self.assertEqual(blocked["status"], "失败")
        self.assertTrue(any("jobs_total" in issue for issue in blocked["issues"]))
        self.assertTrue(any("jobs_active" in issue for issue in blocked["issues"]))
        self.assertEqual(allowed["status"], "通过")

    def test_static_lane_status_cannot_substitute_for_helper_readiness(self) -> None:
        health = healthy_payload()
        del health["helper_readiness"]

        report = runtime_readiness_report(
            api_url="http://127.0.0.1:8765",
            expected_commit="abc1234",
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
            expected_commit="abc1234",
            fetch_json=fake_fetcher(health=health),
        )

        self.assertEqual(report["status"], "失败")
        self.assertTrue(any("authenticated" in issue for issue in report["issues"]))
        self.assertTrue(any("actions" in issue for issue in report["issues"]))

    def test_lane_status_is_only_a_capability_declaration(self) -> None:
        health = healthy_payload()
        health["provider_capabilities"]["magnet"]["status"] = "unavailable"

        report = runtime_readiness_report(
            api_url="http://127.0.0.1:8765",
            expected_commit="abc1234",
            fetch_json=fake_fetcher(health=health),
        )

        self.assertEqual(report["status"], "通过")

    def test_build_identity_requires_expected_id_and_utc_time(self) -> None:
        no_expected = runtime_readiness_report(
            api_url="http://127.0.0.1:8765",
            fetch_json=fake_fetcher(),
        )
        unrecorded = healthy_payload(commit="unrecorded")
        unrecorded["build_time"] = "unrecorded"
        invalid_metadata = runtime_readiness_report(
            api_url="http://127.0.0.1:8765",
            expected_commit="abc1234",
            fetch_json=fake_fetcher(health=unrecorded),
        )

        self.assertEqual(no_expected["status"], "失败")
        self.assertIn("expected_commit", " ".join(no_expected["issues"]))
        self.assertEqual(invalid_metadata["status"], "失败")
        self.assertIn("health.build_commit", " ".join(invalid_metadata["issues"]))
        self.assertIn("health.build_time", " ".join(invalid_metadata["issues"]))

    def test_build_id_prefix_only_matches_from_expected_to_actual(self) -> None:
        full_actual = healthy_payload(commit="abc1234deadbeef")
        accepted = runtime_readiness_report(
            api_url="http://127.0.0.1:8765",
            expected_commit="abc1234",
            fetch_json=fake_fetcher(health=full_actual),
        )
        truncated_actual = healthy_payload(commit="abc1234")
        rejected = runtime_readiness_report(
            api_url="http://127.0.0.1:8765",
            expected_commit="abc1234deadbeef",
            fetch_json=fake_fetcher(health=truncated_actual),
        )
        dirty_actual = healthy_payload(commit="0b53bcd-dirty")
        dirty = runtime_readiness_report(
            api_url="http://127.0.0.1:8765",
            expected_commit="0b53bcd-dirty",
            fetch_json=fake_fetcher(health=dirty_actual),
        )

        self.assertEqual(accepted["status"], "通过")
        self.assertEqual(rejected["status"], "失败")
        self.assertEqual(dirty["status"], "通过")

    def test_cli_requires_explicit_expected_build_id(self) -> None:
        with self.assertRaises(SystemExit) as missing:
            runtime_readiness_main([])
        self.assertEqual(missing.exception.code, 2)

        with patch(
            "scripts.scrapeflow_runtime_readiness.runtime_readiness_report",
            return_value={"status": "通过", "issues": []},
        ) as readiness:
            result = runtime_readiness_main(["--expected-commit", "0b53bcd-dirty"])

        self.assertEqual(result, 0)
        self.assertEqual(readiness.call_args.kwargs["expected_commit"], "0b53bcd-dirty")


if __name__ == "__main__":
    unittest.main()
