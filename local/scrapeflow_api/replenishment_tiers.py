"""Strict three-tier replenishment policy.

This module is intentionally pure JSON-shaped state logic.  It does not
search, download, submit Quark tasks, call aria2, or touch AList.  The runtime
uses this policy to decide whether an acquisition lane may advance; each lane
still has to prove its own staging delivery before Engine is involved.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any


TIER_QUARK_SHARE = "quark_share"
TIER_QUARK_MAGNET = "quark_magnet"
TIER_LOCAL_MAGNET = "magnet"
STRICT_TIER_ORDER = (TIER_QUARK_SHARE, TIER_QUARK_MAGNET, TIER_LOCAL_MAGNET)

FAILURE_CANDIDATE = "candidate"
FAILURE_INFRASTRUCTURE = "infrastructure"
FAILURE_IN_DOUBT = "in_doubt"

EXHAUSTION_MIN_DISTINCT_LOCATORS = 30
SHARE_REQUIRED_SOURCES = frozenset({"pansou"})
MAGNET_REQUIRED_SOURCES = frozenset({
    "animetosho",
    "tokyotosho",
    "mikan",
    "subsplease",
    "dmhy",
    "nyaa",
    "acg",
})


class ReplenishmentTierError(ValueError):
    """The persisted tier state or outcome is malformed."""


def initial_tier_state() -> dict[str, object]:
    return {
        "tier": TIER_QUARK_SHARE,
        "candidate_failures_by_provider": {},
        "exhaustion_proof_by_provider": {},
        "last_error_scope": None,
    }


def _tier(value: object) -> str:
    if value in STRICT_TIER_ORDER:
        return str(value)
    raise ReplenishmentTierError("补源 tier 无效")


def _provider_failures(state: dict[str, object]) -> dict[str, list[str]]:
    raw = state.get("candidate_failures_by_provider")
    output: dict[str, list[str]] = {}
    if isinstance(raw, Mapping):
        for key, value in raw.items():
            if not isinstance(key, str) or key not in STRICT_TIER_ORDER:
                continue
            if isinstance(value, list):
                output[key] = sorted({
                    item for item in value
                    if isinstance(item, str) and item
                })
    state["candidate_failures_by_provider"] = output
    return output


def _exhaustion_proofs(state: dict[str, object]) -> dict[str, dict[str, object]]:
    raw = state.get("exhaustion_proof_by_provider")
    output: dict[str, dict[str, object]] = {}
    if isinstance(raw, Mapping):
        for key, value in raw.items():
            if isinstance(key, str) and key in STRICT_TIER_ORDER and isinstance(value, Mapping):
                output[key] = dict(value)
    state["exhaustion_proof_by_provider"] = output
    return output


def _copy_state(state: Mapping[str, object] | None) -> dict[str, object]:
    copied = dict(state or initial_tier_state())
    copied["tier"] = _tier(copied.get("tier", TIER_QUARK_SHARE))
    _provider_failures(copied)
    _exhaustion_proofs(copied)
    return copied


def _strings(values: object) -> set[str]:
    if not isinstance(values, Iterable) or isinstance(values, (str, bytes, bytearray)):
        return set()
    return {
        str(value).strip().casefold()
        for value in values
        if isinstance(value, str) and value.strip()
    }


def required_sources_for_tier(tier: str) -> frozenset[str]:
    tier = _tier(tier)
    if tier == TIER_QUARK_SHARE:
        return SHARE_REQUIRED_SOURCES
    if tier == TIER_QUARK_MAGNET:
        return MAGNET_REQUIRED_SOURCES
    return frozenset()


def _has_complete_no_candidate_proof(tier: str, outcome: Mapping[str, object]) -> bool:
    if outcome.get("search_complete_no_candidates") is not True:
        return False
    completed = _strings(outcome.get("completed_sources"))
    required = required_sources_for_tier(tier)
    if required and not required.issubset(completed):
        return False
    unchecked = outcome.get("unchecked_secondary_candidates")
    if unchecked is not None and unchecked != 0:
        return False
    return True


def _tier_exhausted(state: Mapping[str, object], tier: str, outcome: Mapping[str, object]) -> bool:
    tier = _tier(tier)
    failures = state.get("candidate_failures_by_provider")
    if isinstance(failures, Mapping):
        locators = failures.get(tier)
        if isinstance(locators, list) and len({
            value for value in locators if isinstance(value, str) and value
        }) >= EXHAUSTION_MIN_DISTINCT_LOCATORS:
            if tier != TIER_QUARK_MAGNET:
                return True
            completed = _strings(outcome.get("completed_sources"))
            required = required_sources_for_tier(tier)
            if required.issubset(completed) and outcome.get("unchecked_secondary_candidates", 0) == 0:
                return True
    proofs = state.get("exhaustion_proof_by_provider")
    if isinstance(proofs, Mapping) and tier in proofs:
        return True
    return False


def _advance(tier: str) -> str:
    index = STRICT_TIER_ORDER.index(_tier(tier))
    if index >= len(STRICT_TIER_ORDER) - 1:
        return tier
    return STRICT_TIER_ORDER[index + 1]


def apply_tier_outcome(
    state: Mapping[str, object] | None,
    outcome: Mapping[str, object],
) -> dict[str, object]:
    """Return updated tier state after one acquisition/search outcome.

    ``outcome.scope`` accepts:
    - ``candidate``: a resource was checked and proven unusable.
    - ``infrastructure``: network/auth/quota/helper/AList/etc. failed.
    - ``in_doubt``: an external task may already have been submitted.

    Infrastructure and in-doubt outcomes never exclude locators and never
    advance the tier.
    """
    current = _copy_state(state)
    tier = _tier(current["tier"])
    scope = str(outcome.get("scope") or "").strip().casefold()
    if scope not in {FAILURE_CANDIDATE, FAILURE_INFRASTRUCTURE, FAILURE_IN_DOUBT}:
        raise ReplenishmentTierError("补源失败 scope 无效")
    current["last_error_scope"] = scope

    if scope == FAILURE_INFRASTRUCTURE:
        current["status"] = "retry_wait"
        return current
    if scope == FAILURE_IN_DOUBT:
        current["status"] = "waiting_reconcile"
        external = outcome.get("external_task_id")
        if isinstance(external, str) and external:
            current["external_task_id"] = external
        return current

    failures = _provider_failures(current)
    locator = outcome.get("locator")
    if isinstance(locator, str) and locator:
        failures[tier] = sorted({*failures.get(tier, []), locator})

    if _has_complete_no_candidate_proof(tier, outcome):
        proofs = _exhaustion_proofs(current)
        proofs[tier] = {
            "type": "search_complete_no_candidates",
            "completed_sources": sorted(_strings(outcome.get("completed_sources"))),
        }

    if _tier_exhausted(current, tier, outcome):
        current["tier"] = _advance(tier)
        current["status"] = (
            "exhausted" if current["tier"] == tier else "advanced"
        )
    else:
        current["status"] = "candidate_failed"
    return current


__all__ = [
    "EXHAUSTION_MIN_DISTINCT_LOCATORS",
    "FAILURE_CANDIDATE",
    "FAILURE_IN_DOUBT",
    "FAILURE_INFRASTRUCTURE",
    "MAGNET_REQUIRED_SOURCES",
    "SHARE_REQUIRED_SOURCES",
    "STRICT_TIER_ORDER",
    "TIER_LOCAL_MAGNET",
    "TIER_QUARK_MAGNET",
    "TIER_QUARK_SHARE",
    "ReplenishmentTierError",
    "apply_tier_outcome",
    "initial_tier_state",
    "required_sources_for_tier",
]
