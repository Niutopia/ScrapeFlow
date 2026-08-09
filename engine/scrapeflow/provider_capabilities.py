"""The small, truthful provider capability contract.

ScrapeFlow currently has one executable acquisition implementation:
``LocalTorrentMaterializer``.  Provider discovery and the public status API
must derive their claims from that fact instead of advertising historical
cloud-share/HTTP lanes.  This module intentionally contains data and pure
validation only; it does not import a materializer or perform I/O.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any


EXECUTABLE_PROVIDER = "magnet"
EXECUTABLE_ACQUISITION_KIND = "torrent"
ACTIVE_PROVIDERS = frozenset({EXECUTABLE_PROVIDER})

# Names which may still occur in old catalog/state documents.  They are kept
# here only so callers can report an explicit unavailable status; they are not
# part of ``ACTIVE_PROVIDERS`` and can never be selected or materialized.
UNAVAILABLE_PROVIDERS = frozenset({"cloud_share"})


def provider_capability_snapshot() -> dict[str, dict[str, Any]]:
    """Return a fresh JSON-safe snapshot for health/search projections.

    A new mapping is returned on every call so an API consumer cannot mutate
    the process-wide capability declaration.  ``ready`` is reserved for the
    one lane whose candidate kind the current materializer accepts.
    """
    return {
        EXECUTABLE_PROVIDER: {
            "status": "ready",
            "acquisition_kinds": [EXECUTABLE_ACQUISITION_KIND],
            "materializer": "LocalTorrentMaterializer",
            # The current real materializer returns media-only deliveries;
            # no production archive_source fixture has proved provider SFX.
            "sfx": {"status": "deferred", "reason": "no_real_archive_source_materializer_input"},
        },
        "cloud_share": {
            "status": "unavailable",
            "reason": "no_executable_materializer",
            "acquisition_kinds": [],
            "materializer": None,
        },
    }


def candidate_capability_error(candidate: Mapping[str, Any]) -> str | None:
    """Return a stable rejection reason, or ``None`` for a runnable row.

    This is deliberately stricter than checking the provider name alone.  A
    forged ``magnet`` row carrying an HTTP acquisition must fail before it can
    enter a durable selection or reach the local materializer.
    """
    provider = str(candidate.get("provider") or "").strip().casefold()
    if provider != EXECUTABLE_PROVIDER:
        return "unsupported_provider"
    acquisition = candidate.get("acquisition")
    if not isinstance(acquisition, Mapping):
        return "provider_acquisition_mismatch"
    if str(acquisition.get("kind") or "").strip().casefold() != EXECUTABLE_ACQUISITION_KIND:
        return "provider_acquisition_mismatch"
    return None


def is_executable_candidate(candidate: object) -> bool:
    """Return whether a candidate is accepted by the current execution lane."""
    return isinstance(candidate, Mapping) and candidate_capability_error(candidate) is None


__all__ = [
    "ACTIVE_PROVIDERS",
    "EXECUTABLE_ACQUISITION_KIND",
    "EXECUTABLE_PROVIDER",
    "UNAVAILABLE_PROVIDERS",
    "candidate_capability_error",
    "is_executable_candidate",
    "provider_capability_snapshot",
]
