"""Provider-neutral acquisition routing.

Only generic HTTP and exact Torrent lanes are executable. Unsupported provider
artifacts are rejected before they can reach a materializer.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from typing import Any, Protocol


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


class HttpAcquirePort(Protocol):
    def __call__(
        self, selection: Mapping[str, Any], destination: str,
    ) -> Mapping[str, Any]: ...


class ArrivalVerifierPort(Protocol):
    def __call__(
        self, destination: str, expected_files: Sequence[Mapping[str, Any]],
    ) -> None: ...


def acquisition_lane(selection: Mapping[str, Any]) -> str:
    """Validate provider/kind alignment and return a generic execution lane."""
    provider = str(selection.get("provider") or "")
    acquisition = selection.get("acquisition")
    kind = str(acquisition.get("kind") or "") if isinstance(acquisition, Mapping) else ""
    if provider == "magnet" and kind == "torrent":
        return "torrent"
    if provider == "cloud_share" and kind in {"http", "http_download", "external_http"}:
        return "http"
    raise AcquisitionRouteError(
        f"provider/acquisition kind mismatch: provider={provider!r}, kind={kind!r}"
    )


def acquire_selection(
    selection: Mapping[str, Any],
    destination: str,
    *,
    acquire_http: HttpAcquirePort | None = None,
    verify_arrival: ArrivalVerifierPort | None = None,
    acquire_torrent: Callable[[Mapping[str, Any], str], Mapping[str, Any]],
) -> dict[str, Any]:
    """Dispatch one selected candidate to its task-owned staging destination.

    The caller owns scheduling and staging isolation.  This boundary only
    selects the compatible injected lane, normalizes its ready result, and
    verifies the arrival when a verifier is supplied.
    """
    lane = acquisition_lane(selection)
    handler = acquire_torrent if lane == "torrent" else acquire_http
    if handler is None:
        raise AcquisitionRouteError(f"{lane} lane has no injected executor")
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
