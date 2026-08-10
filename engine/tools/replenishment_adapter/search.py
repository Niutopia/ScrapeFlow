"""Read-only provider candidate discovery for replenishment."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Any

import engine.tools._replenishment_local_adapter_impl as _impl
from engine.scrapeflow.provider_capabilities import (
    ACTIVE_PROVIDERS,
    candidate_capability_error,
    provider_capability_snapshot,
)


def _provider_neutral(result: Mapping[str, Any]) -> dict[str, Any]:
    """Return only candidates accepted by the fixed materializer chain.

    Search is an evidence boundary, not a promise that every historical
    source can be delivered.  Cloud-share/HTTP rows are intentionally dropped
    here; only fixed Quark share, Quark magnet, and local Torrent shapes remain.
    """
    output = dict(result)
    candidates = []
    rejected: dict[str, int] = {}
    for row in result.get("candidates") or []:
        if not isinstance(row, Mapping):
            rejected["candidate_not_object"] = rejected.get("candidate_not_object", 0) + 1
            continue
        locator = str(row.get("locator") or "")
        error = candidate_capability_error(row)
        if error is not None or not locator:
            reason = error or "missing_locator"
            rejected[reason] = rejected.get(reason, 0) + 1
            continue
        candidate = dict(row)
        # Search results are data; materialization is a separate step.
        candidate.pop("action", None)
        candidate.pop("request", None)
        for key in tuple(candidate):
            if "offline" in str(key).casefold():
                candidate.pop(key, None)
        candidates.append(candidate)
    output["candidates"] = candidates
    output["active_search_lane"] = "strict_replenishment_chain"
    output["active_provider"] = "fixed_chain"
    output["lane_status"] = provider_capability_snapshot()
    output["provider_capabilities"] = provider_capability_snapshot()
    output["candidate_rejections"] = rejected
    output["provider_rejections"] = dict(rejected)
    return output


def search(request: Mapping[str, Any]) -> dict[str, Any]:
    """Run the read-only provider-neutral search boundary."""
    result = _impl._search(request)
    if not isinstance(result, Mapping):
        raise TypeError("补源搜索结果必须是对象")
    return _provider_neutral(result)


class ReplenishmentSearchService:
    """Injectable search service used by the automatic workflow."""

    def __init__(
        self,
        runner: Callable[[Mapping[str, Any]], Mapping[str, Any]] | None = None,
    ) -> None:
        self._runner = runner or search

    def run(self, request: Mapping[str, Any]) -> dict[str, Any]:
        result = self._runner(request)
        if not isinstance(result, Mapping):
            raise TypeError("补源搜索结果必须是对象")
        return _provider_neutral(result)


def dynamic_terms(request: Mapping[str, Any], *, maximum: int = 12) -> list[str]:
    """Expose deterministic query generation without exposing search I/O."""
    return list(_impl._compact_dynamic_search_terms(request, maximum=maximum))


def candidate_variants(
    request: Mapping[str, Any], release_name: str, locator: str,
    manifest: Mapping[str, Any], *, include_local: bool = False,
) -> list[dict[str, Any]]:
    """Build verified candidate evidence for a source snapshot."""
    rows = _impl._torrent_candidate_variants(
        request, release_name, locator, manifest, include_local=include_local,
    )
    return [
        dict(row) for row in rows
        if isinstance(row, Mapping)
        and str(row.get("provider") or "") in ACTIVE_PROVIDERS
        and candidate_capability_error(row) is None
    ]


__all__ = [
    "ACTIVE_PROVIDERS",
    "ReplenishmentSearchService",
    "candidate_variants",
    "dynamic_terms",
    "search",
]
