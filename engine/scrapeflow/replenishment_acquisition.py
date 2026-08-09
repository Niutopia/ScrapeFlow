"""Acquisition routing for the currently executable local provider lane.

The process ships one materializer: exact magnet/Torrent acquisition.  The
HTTP protocol remains injectable as a future extension point, but it is not a
current capability and must never make a cloud-share candidate runnable.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from typing import Any, Protocol

from .provider_capabilities import (
    EXECUTABLE_ACQUISITION_KIND,
    candidate_capability_error,
)


class AcquisitionRouteError(RuntimeError):
    failure_scope = "infrastructure"
    failure_stage = "acquisition_route"
    reusable_candidate = False
    exclude_candidate = False


class CandidateIdentityError(AcquisitionRouteError):
    failure_scope = "candidate"
    failure_stage = "candidate_identity"
    exclude_candidate = True


class DeliveryError(AcquisitionRouteError):
    failure_scope = "delivery"
    failure_stage = "delivery_visibility"
    reusable_candidate = True


class ArrivalVerifierPort(Protocol):
    def __call__(
        self, destination: str, expected_files: Sequence[Mapping[str, Any]],
    ) -> None: ...


def acquisition_lane(selection: Mapping[str, Any]) -> str:
    """Validate the real materializer contract and return its execution lane."""
    error = candidate_capability_error(selection)
    if error is None:
        return EXECUTABLE_ACQUISITION_KIND
    provider = str(selection.get("provider") or "")
    acquisition = selection.get("acquisition")
    kind = str(acquisition.get("kind") or "") if isinstance(acquisition, Mapping) else ""
    raise AcquisitionRouteError(
        "no executable materializer for "
        f"provider={provider!r}, kind={kind!r} ({error})"
    )


def acquire_selection(
    selection: Mapping[str, Any],
    destination: str,
    *,
    verify_arrival: ArrivalVerifierPort | None = None,
    acquire_torrent: Callable[[Mapping[str, Any], str], Mapping[str, Any]],
) -> dict[str, Any]:
    """Dispatch one selected candidate to its task-owned staging destination.

    The caller owns scheduling and staging isolation.  This boundary only
    selects the compatible injected lane, normalizes its ready result, and
    verifies the arrival when a verifier is supplied.
    """
    lane = acquisition_lane(selection)
    handler = acquire_torrent
    try:
        result = handler(selection, destination)
    except AcquisitionRouteError:
        raise
    except Exception as exc:
        raise AcquisitionRouteError(str(exc)) from exc
    if not isinstance(result, Mapping) or result.get("status") != "ready":
        raise AcquisitionRouteError(f"{lane} lane did not return ready")
    normalized = dict(result)
    if verify_arrival is not None:
        expected = normalized.get("expected_files")
        if not isinstance(expected, list):
            expected = []
        try:
            verify_arrival(destination, expected)
        except Exception as exc:
            raise DeliveryError(str(exc)) from exc
    return normalized
