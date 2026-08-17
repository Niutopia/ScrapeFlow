"""The small, truthful provider capability contract.

ScrapeFlow exposes only fixed replenishment lanes that have a materializer
boundary.  Provider discovery and the public status API derive their claims
from these exact pairs instead of advertising historical HTTP/cloud
placeholders.  This module intentionally contains data and pure validation
only; it does not import a materializer or perform I/O.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any


PROVIDER_QUARK_SHARE = "quark_share"
PROVIDER_ALIST_OFFLINE = "alist_offline"
PROVIDER_LOCAL_MAGNET = "magnet"
ACQUISITION_QUARK_FAST_SAVE = "quark_fast_save"
ACQUISITION_ALIST_OFFLINE = "alist_offline"
ACQUISITION_TORRENT = "torrent"

# ``provider_capability_snapshot`` is a declaration of the fixed materializer
# contract, not a liveness probe.  Keep the helper dependency names and action
# set here so the API health projection and the independent readiness command
# cannot silently drift apart.
#
# The helper contract shrank to the share-save path only (2026-08-17): the
# CDP/WSG magnet impersonation tier was removed after Quark account-level
# rate-limiting proved it unusable, and offline acquisition now rides the
# AList offline-download tool lane instead.
QUARK_HELPER_NAME = "quark"
QUARK_HELPER_REQUIRED_ACTIONS = (
    "health",
    "share-save",
)

# The AList offline-download lane depends on the AList tool framework bound to
# the dedicated aria2 RPC service (download direct, local relay to Quark).
ALIST_OFFLINE_TOOL_NAME = "aria2"
ALIST_OFFLINE_REQUIRED_ACTIONS = (
    "offline_download_add",
    "offline_download_status",
)

# Backwards-compatible names for the one local Torrent executor.
EXECUTABLE_PROVIDER = PROVIDER_LOCAL_MAGNET
EXECUTABLE_ACQUISITION_KIND = ACQUISITION_TORRENT

ACTIVE_PROVIDER_ACQUISITION_KINDS = {
    PROVIDER_QUARK_SHARE: ACQUISITION_QUARK_FAST_SAVE,
    PROVIDER_ALIST_OFFLINE: ACQUISITION_ALIST_OFFLINE,
    PROVIDER_LOCAL_MAGNET: ACQUISITION_TORRENT,
}
ACTIVE_PROVIDERS = frozenset(ACTIVE_PROVIDER_ACQUISITION_KINDS)


def _quark_helper_runtime_dependency() -> dict[str, Any]:
    """Return a fresh declaration of the complete typed Helper contract."""
    return {
        "helper": QUARK_HELPER_NAME,
        "required_actions": list(QUARK_HELPER_REQUIRED_ACTIONS),
    }


def _alist_offline_runtime_dependency() -> dict[str, Any]:
    """Return the AList offline-tool dependency declaration for the lane."""
    return {
        "alist_offline_tool": ALIST_OFFLINE_TOOL_NAME,
        "required_actions": list(ALIST_OFFLINE_REQUIRED_ACTIONS),
    }


def provider_capability_snapshot() -> dict[str, dict[str, Any]]:
    """Return a fresh JSON-safe snapshot for health/search projections.

    A new mapping is returned on every call so an API consumer cannot mutate
    the process-wide capability declaration.  ``ready`` is reserved for fixed
    lanes whose candidate kind the current materializer chain accepts.  It is
    deliberately *not* evidence that an external helper is reachable or
    authenticated: consumers that need runtime truth must inspect the
    structured ``health.helper_readiness`` projection instead.
    """
    return {
        PROVIDER_QUARK_SHARE: {
            "status": "ready",
            "status_scope": "declared_materializer",
            "acquisition_kinds": [ACQUISITION_QUARK_FAST_SAVE],
            "materializer": "QuarkFastSaveMaterializer",
            "runtime_dependency": _quark_helper_runtime_dependency(),
            "sfx": {"status": "deferred", "reason": "provider_v1_media_and_subtitles_only"},
        },
        PROVIDER_ALIST_OFFLINE: {
            "status": "ready",
            "status_scope": "declared_materializer",
            "acquisition_kinds": [ACQUISITION_ALIST_OFFLINE],
            "materializer": "AlistOfflineAutomaticMaterializer",
            "runtime_dependency": _alist_offline_runtime_dependency(),
            "sfx": {"status": "deferred", "reason": "provider_v1_media_and_subtitles_only"},
        },
        PROVIDER_LOCAL_MAGNET: {
            "status": "ready",
            "status_scope": "declared_materializer",
            "acquisition_kinds": [ACQUISITION_TORRENT],
            "materializer": "LocalTorrentMaterializer",
            # The current real materializer returns media-only deliveries;
            # no production archive_source fixture has proved provider SFX.
            "sfx": {"status": "deferred", "reason": "no_real_archive_source_materializer_input"},
        },
    }


def candidate_capability_error(candidate: Mapping[str, Any]) -> str | None:
    """Return a stable rejection reason, or ``None`` for a runnable row.

    This is deliberately stricter than checking the provider name alone.  A
    forged ``magnet`` row carrying an HTTP acquisition must fail before it can
    enter a durable selection or reach the local materializer.
    """
    provider = str(candidate.get("provider") or "").strip().casefold()
    expected_kind = ACTIVE_PROVIDER_ACQUISITION_KINDS.get(provider)
    if expected_kind is None:
        return "unsupported_provider"
    acquisition = candidate.get("acquisition")
    if not isinstance(acquisition, Mapping):
        return "provider_acquisition_mismatch"
    if str(acquisition.get("kind") or "").strip().casefold() != expected_kind:
        return "provider_acquisition_mismatch"
    return None


def is_executable_candidate(candidate: object) -> bool:
    """Return whether a candidate is accepted by the current execution lane."""
    return isinstance(candidate, Mapping) and candidate_capability_error(candidate) is None


__all__ = [
    "ACTIVE_PROVIDERS",
    "ACTIVE_PROVIDER_ACQUISITION_KINDS",
    "ACQUISITION_ALIST_OFFLINE",
    "ACQUISITION_QUARK_FAST_SAVE",
    "ACQUISITION_TORRENT",
    "ALIST_OFFLINE_REQUIRED_ACTIONS",
    "ALIST_OFFLINE_TOOL_NAME",
    "EXECUTABLE_ACQUISITION_KIND",
    "EXECUTABLE_PROVIDER",
    "PROVIDER_ALIST_OFFLINE",
    "PROVIDER_LOCAL_MAGNET",
    "PROVIDER_QUARK_SHARE",
    "QUARK_HELPER_NAME",
    "QUARK_HELPER_REQUIRED_ACTIONS",
    "candidate_capability_error",
    "is_executable_candidate",
    "provider_capability_snapshot",
]
