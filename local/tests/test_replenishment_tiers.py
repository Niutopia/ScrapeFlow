from __future__ import annotations

import unittest

from local.scrapeflow_api.replenishment_tiers import (
    EXHAUSTION_MIN_DISTINCT_LOCATORS,
    FAILURE_CANDIDATE,
    FAILURE_IN_DOUBT,
    FAILURE_INFRASTRUCTURE,
    MAGNET_REQUIRED_SOURCES,
    MAGNET_REQUIRED_SOURCES_BY_SHELF,
    STRICT_TIER_ORDER,
    TIER_LOCAL_MAGNET,
    TIER_QUARK_SHARE,
    ReplenishmentTierError,
    apply_tier_outcome,
    initial_tier_state,
    required_sources_for_tier,
)


def _candidate(locator: str, **extra: object) -> dict[str, object]:
    return {"scope": FAILURE_CANDIDATE, "locator": locator, **extra}


class ReplenishmentTierPolicyTests(unittest.TestCase):
    def test_current_order_is_quark_share_then_exact_local_magnet(self) -> None:
        self.assertEqual(
            STRICT_TIER_ORDER,
            (TIER_QUARK_SHARE, TIER_LOCAL_MAGNET),
        )

    def test_infrastructure_failure_does_not_count_or_degrade(self) -> None:
        result = apply_tier_outcome(
            initial_tier_state(),
            {"scope": FAILURE_INFRASTRUCTURE, "locator": "share://dead"},
        )

        self.assertEqual(result["tier"], TIER_QUARK_SHARE)
        self.assertEqual(result["candidate_failures_by_provider"], {})
        self.assertEqual(result["status"], "retry_wait")

    def test_quark_exhaustion_advances_directly_to_magnet(self) -> None:
        state = initial_tier_state()
        for index in range(EXHAUSTION_MIN_DISTINCT_LOCATORS):
            state = apply_tier_outcome(state, _candidate(f"quark-share://{index}"))

        self.assertEqual(state["tier"], TIER_LOCAL_MAGNET)
        self.assertEqual(state["status"], "advanced")

    def test_quark_no_candidate_proof_advances_directly_to_magnet(self) -> None:
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
        self.assertEqual(complete["tier"], TIER_LOCAL_MAGNET)

    def test_magnet_exhaustion_requires_all_sources_and_no_unchecked_candidates(self) -> None:
        state = {**initial_tier_state(), "tier": TIER_LOCAL_MAGNET}
        blocked = apply_tier_outcome(
            state,
            {
                "scope": FAILURE_CANDIDATE,
                "search_complete_no_candidates": True,
                "completed_sources": sorted(MAGNET_REQUIRED_SOURCES),
                "configured_sources": sorted(MAGNET_REQUIRED_SOURCES),
                "unchecked_secondary_candidates": 1,
            },
        )
        exhausted = apply_tier_outcome(
            state,
            {
                "scope": FAILURE_CANDIDATE,
                "search_complete_no_candidates": True,
                "completed_sources": sorted(MAGNET_REQUIRED_SOURCES),
                "configured_sources": sorted(MAGNET_REQUIRED_SOURCES),
                "unchecked_secondary_candidates": 0,
            },
        )

        self.assertEqual(blocked["tier"], TIER_LOCAL_MAGNET)
        self.assertEqual(blocked["status"], "candidate_failed")
        self.assertEqual(exhausted["tier"], TIER_LOCAL_MAGNET)
        self.assertEqual(exhausted["status"], "exhausted")

    def test_magnet_source_requirements_are_shelf_aware(self) -> None:
        self.assertEqual(
            required_sources_for_tier(TIER_LOCAL_MAGNET, "anime"),
            MAGNET_REQUIRED_SOURCES,
        )
        for shelf in ("movie", "us_tv"):
            required = required_sources_for_tier(TIER_LOCAL_MAGNET, shelf)
            self.assertEqual(required, MAGNET_REQUIRED_SOURCES_BY_SHELF[shelf])
            # The general-purpose index is deliberately outside the anime
            # index set: movie/US-TV shelves owe it, anime does not.
            self.assertEqual(required, frozenset({"bitsearch"}))
            self.assertFalse(required.issubset(MAGNET_REQUIRED_SOURCES))
        self.assertEqual(
            required_sources_for_tier(TIER_LOCAL_MAGNET, None),
            MAGNET_REQUIRED_SOURCES,
        )

    def test_configured_subset_is_valid_against_the_canonical_source_set(self) -> None:
        state = {**initial_tier_state(), "tier": TIER_LOCAL_MAGNET}
        general_only = {
            "scope": FAILURE_CANDIDATE,
            "search_complete_no_candidates": True,
            "completed_sources": sorted(MAGNET_REQUIRED_SOURCES_BY_SHELF["movie"]),
            "configured_sources": sorted(MAGNET_REQUIRED_SOURCES_BY_SHELF["movie"]),
            "unchecked_secondary_candidates": 0,
        }

        # Without a shelf the canonical set stays the full anime index list,
        # so a general-index-only configuration proves nothing there.
        conservative = apply_tier_outcome(state, dict(general_only))
        movie = apply_tier_outcome(state, {**general_only, "shelf": "movie"})
        anime_subset = apply_tier_outcome(state, {
            **general_only,
            "completed_sources": ["acg", "nyaa"],
            "configured_sources": ["acg", "nyaa"],
        })

        self.assertEqual(conservative["status"], "candidate_failed")
        self.assertEqual(movie["status"], "exhausted")
        self.assertEqual(anime_subset["status"], "exhausted")

    def test_magnet_dynamic_proof_requires_a_nonempty_canonical_configured_set(self) -> None:
        state = {**initial_tier_state(), "tier": TIER_LOCAL_MAGNET}
        base = {
            "scope": FAILURE_CANDIDATE,
            "search_complete_no_candidates": True,
            "completed_sources": ["acg"],
            "unchecked_secondary_candidates": 0,
            "shelf": "anime",
        }

        disabled = apply_tier_outcome(state, dict(base))
        unknown = apply_tier_outcome(
            state, {**base, "configured_sources": ["untrusted-index"]},
        )
        one_enabled = apply_tier_outcome(
            state, {**base, "configured_sources": ["acg"]},
        )

        self.assertEqual(disabled["status"], "candidate_failed")
        self.assertEqual(unknown["status"], "candidate_failed")
        self.assertEqual(one_enabled["status"], "exhausted")

    def test_in_doubt_keeps_current_exact_lane(self) -> None:
        result = apply_tier_outcome(
            {**initial_tier_state(), "tier": TIER_LOCAL_MAGNET},
            {
                "scope": FAILURE_IN_DOUBT,
                "locator": "magnet:?xt=urn:btih:" + "b" * 40,
                "external_task_id": "aria2-task-1",
            },
        )

        self.assertEqual(result["tier"], TIER_LOCAL_MAGNET)
        self.assertEqual(result["status"], "waiting_reconcile")
        self.assertEqual(result["candidate_failures_by_provider"], {})
        self.assertEqual(result["external_task_id"], "aria2-task-1")

    def test_retired_alist_tier_is_not_accepted_by_policy(self) -> None:
        with self.assertRaises(ReplenishmentTierError):
            apply_tier_outcome(
                {**initial_tier_state(), "tier": "alist_offline"},
                _candidate("obsolete"),
            )


if __name__ == "__main__":
    unittest.main()
