from __future__ import annotations

import unittest

from local.scrapeflow_api.replenishment_tiers import (
    EXHAUSTION_MIN_DISTINCT_LOCATORS,
    FAILURE_CANDIDATE,
    FAILURE_IN_DOUBT,
    FAILURE_INFRASTRUCTURE,
    MAGNET_REQUIRED_SOURCES,
    TIER_LOCAL_MAGNET,
    TIER_QUARK_MAGNET,
    TIER_QUARK_SHARE,
    apply_tier_outcome,
    initial_tier_state,
)


def _candidate(locator: str, **extra: object) -> dict[str, object]:
    return {"scope": FAILURE_CANDIDATE, "locator": locator, **extra}


class ReplenishmentTierPolicyTests(unittest.TestCase):
    def test_infrastructure_failure_does_not_count_or_degrade(self) -> None:
        state = initial_tier_state()
        result = apply_tier_outcome(
            state,
            {"scope": FAILURE_INFRASTRUCTURE, "locator": "share://dead"},
        )

        self.assertEqual(result["tier"], TIER_QUARK_SHARE)
        self.assertEqual(result["candidate_failures_by_provider"], {})
        self.assertEqual(result["status"], "retry_wait")

    def test_first_tier_exhaustion_advances_only_to_second_tier(self) -> None:
        state = initial_tier_state()
        for index in range(EXHAUSTION_MIN_DISTINCT_LOCATORS):
            state = apply_tier_outcome(
                state,
                _candidate(f"quark-share://{index}"),
            )

        self.assertEqual(state["tier"], TIER_QUARK_MAGNET)
        self.assertNotEqual(state["tier"], TIER_LOCAL_MAGNET)

    def test_no_candidate_proof_requires_completed_required_source(self) -> None:
        incomplete = apply_tier_outcome(
            initial_tier_state(),
            {
                "scope": FAILURE_CANDIDATE,
                "search_complete_no_candidates": True,
                "completed_sources": [],
            },
        )
        complete = apply_tier_outcome(
            initial_tier_state(),
            {
                "scope": FAILURE_CANDIDATE,
                "search_complete_no_candidates": True,
                "completed_sources": ["pansou"],
            },
        )

        self.assertEqual(incomplete["tier"], TIER_QUARK_SHARE)
        self.assertEqual(incomplete["exhaustion_proof_by_provider"], {})
        self.assertEqual(complete["tier"], TIER_QUARK_MAGNET)

    def test_second_tier_count_alone_cannot_enter_local_torrent(self) -> None:
        state = {**initial_tier_state(), "tier": TIER_QUARK_MAGNET}
        for index in range(EXHAUSTION_MIN_DISTINCT_LOCATORS):
            state = apply_tier_outcome(
                state,
                _candidate(f"magnet:?xt=urn:btih:{index:040d}"),
            )

        self.assertEqual(state["tier"], TIER_QUARK_MAGNET)
        self.assertEqual(state["status"], "candidate_failed")

    def test_second_tier_requires_all_sources_and_no_unchecked_candidates(self) -> None:
        state = {**initial_tier_state(), "tier": TIER_QUARK_MAGNET}
        blocked = apply_tier_outcome(
            state,
            {
                "scope": FAILURE_CANDIDATE,
                "search_complete_no_candidates": True,
                "completed_sources": sorted(MAGNET_REQUIRED_SOURCES),
                "unchecked_secondary_candidates": 1,
            },
        )
        advanced = apply_tier_outcome(
            state,
            {
                "scope": FAILURE_CANDIDATE,
                "search_complete_no_candidates": True,
                "completed_sources": sorted(MAGNET_REQUIRED_SOURCES),
                "unchecked_secondary_candidates": 0,
            },
        )

        self.assertEqual(blocked["tier"], TIER_QUARK_MAGNET)
        self.assertEqual(advanced["tier"], TIER_LOCAL_MAGNET)

    def test_cloud_and_local_failures_keep_same_infohash_separate(self) -> None:
        locator = "magnet:?xt=urn:btih:" + "a" * 40
        state = {**initial_tier_state(), "tier": TIER_QUARK_MAGNET}
        state = apply_tier_outcome(state, _candidate(locator))
        state["tier"] = TIER_LOCAL_MAGNET
        state = apply_tier_outcome(state, _candidate(locator))

        failures = state["candidate_failures_by_provider"]
        self.assertEqual(failures[TIER_QUARK_MAGNET], [locator])
        self.assertEqual(failures[TIER_LOCAL_MAGNET], [locator])

    def test_in_doubt_waits_for_reconcile_without_excluding_or_degrading(self) -> None:
        state = apply_tier_outcome(
            {**initial_tier_state(), "tier": TIER_QUARK_MAGNET},
            {
                "scope": FAILURE_IN_DOUBT,
                "locator": "magnet:?xt=urn:btih:" + "b" * 40,
                "external_task_id": "quark-task-1",
            },
        )

        self.assertEqual(state["tier"], TIER_QUARK_MAGNET)
        self.assertEqual(state["status"], "waiting_reconcile")
        self.assertEqual(state["candidate_failures_by_provider"], {})
        self.assertEqual(state["external_task_id"], "quark-task-1")


if __name__ == "__main__":
    unittest.main()
