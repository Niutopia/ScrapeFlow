"""Provider-neutral acquisition routing for replenishment candidates.

This module deliberately contains no Quark credentials or HTTP endpoints.
The Quark share-save operation and the AList arrival check are injected ports,
so the adapter can prove routing and failure semantics without performing a
real cloud mutation in tests.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from typing import Any, Protocol


class AcquisitionRouteError(RuntimeError):
    failure_scope = "infrastructure"
    failure_stage = "acquisition_route"
    reusable_candidate = False
    exclude_candidate = False


class QuarkShareCandidateError(AcquisitionRouteError):
    """The share/file identity is expired, missing, or does not match the request."""

    failure_scope = "candidate"
    failure_stage = "quark_share_candidate"
    exclude_candidate = True

    def __init__(self, message: str, candidate: Mapping[str, Any] | None = None) -> None:
        super().__init__(message)
        self.candidate = {
            key: candidate.get(key)
            for key in ("provider", "release_name", "locator")
            if candidate is not None and candidate.get(key) is not None
        }


class QuarkFastSaveInfrastructureError(AcquisitionRouteError):
    """Credentials, quota, rate limiting, or the save bridge failed."""

    failure_stage = "quark_fast_save_submit"


class QuarkFastSaveDeliveryError(AcquisitionRouteError):
    """The save was accepted but the destination is not yet provably visible."""

    failure_scope = "delivery"
    failure_stage = "delivery_visibility"
    reusable_candidate = True


class FastSavePort(Protocol):
    def __call__(
        self, selection: Mapping[str, Any], destination: str,
    ) -> Mapping[str, Any]: ...


class ArrivalVerifierPort(Protocol):
    def __call__(
        self, destination: str, expected_files: Sequence[Mapping[str, Any]],
    ) -> None: ...


def acquisition_lane(selection: Mapping[str, Any]) -> str:
    """Validate provider/kind alignment and return the execution lane."""
    provider = str(selection.get("provider") or "")
    acquisition = selection.get("acquisition")
    kind = str(acquisition.get("kind") or "") if isinstance(acquisition, Mapping) else ""
    if provider == "quark_share" and kind == "quark_fast_save":
        payload_kind = str(
            acquisition.get("payload_kind") or selection.get("payload_kind") or "video_payload"
        )
        if (
            payload_kind != "video_payload"
            or acquisition.get("requires_extraction") is True
            or selection.get("requires_extraction") is True
        ):
            raise AcquisitionRouteError(
                "archive payload cannot use the direct quark_fast_save lane"
            )
        return "quark_fast_save"
    if provider == "quark_share" and kind == "quark_sfx_archive":
        payload_kind = str(
            acquisition.get("payload_kind") or selection.get("payload_kind") or ""
        )
        archive_format = str(
            acquisition.get("archive_format") or selection.get("archive_format") or ""
        )
        if (
            payload_kind != "archive_payload" or archive_format != "sfx"
            or not (
                acquisition.get("requires_extraction") is True
                or selection.get("requires_extraction") is True
            )
        ):
            raise AcquisitionRouteError(
                "quark_sfx_archive lane requires archive_payload/sfx/extraction contract"
            )
        return "quark_sfx_archive"
    if provider == "quark_magnet" and kind == "quark_magnet_offline":
        return "quark_magnet_offline"
    if provider == "magnet" and kind == "torrent":
        return "torrent"
    raise AcquisitionRouteError(
        f"provider/acquisition kind mismatch: provider={provider!r}, kind={kind!r}"
    )


def _validated_fast_save_receipt(
    selection: Mapping[str, Any], destination: str, receipt: Mapping[str, Any],
) -> list[dict[str, Any]]:
    if receipt.get("status") not in {"submitted", "ready"}:
        raise QuarkFastSaveInfrastructureError("fast-save port returned an invalid status")
    if receipt.get("destination") != destination:
        raise QuarkFastSaveInfrastructureError("fast-save destination differs from the request")
    raw_files = receipt.get("expected_files")
    if not isinstance(raw_files, list) or not raw_files:
        raise QuarkFastSaveInfrastructureError("fast-save receipt lacks expected_files")
    expected: list[dict[str, Any]] = []
    covered: set[str] = set()
    for row in raw_files:
        if not isinstance(row, Mapping):
            raise QuarkFastSaveInfrastructureError("fast-save expected file is not an object")
        name = row.get("name")
        size = row.get("size")
        gap_ids = row.get("gap_ids")
        if (
            not isinstance(name, str) or not name.strip()
            or type(size) is not int or size <= 0
            or not isinstance(gap_ids, list) or not gap_ids
            or not all(isinstance(item, str) and item for item in gap_ids)
        ):
            raise QuarkFastSaveInfrastructureError("fast-save expected file is incomplete")
        covered.update(gap_ids)
        expected.append({"name": name, "size": size, "gap_ids": list(gap_ids)})
    selected = {
        str(item) for item in selection.get("selected_gap_ids") or []
        if isinstance(item, str) and item
    }
    if not selected or not selected <= covered:
        raise QuarkShareCandidateError(
            "fast-save receipt does not cover every selected gap", selection,
        )
    return expected


def acquire_selection(
    selection: Mapping[str, Any],
    destination: str,
    *,
    fast_save: FastSavePort,
    verify_arrival: ArrivalVerifierPort,
    acquire_torrent: Callable[[Mapping[str, Any], str], Mapping[str, Any]],
    acquire_sfx: Callable[[Mapping[str, Any], str], Mapping[str, Any]] | None = None,
    acquire_offline: Callable[[Mapping[str, Any], str], Mapping[str, Any]] | None = None,
) -> dict[str, Any]:
    """Dispatch one selected candidate and normalize the adapter result."""
    lane = acquisition_lane(selection)
    if lane == "torrent":
        result = acquire_torrent(selection, destination)
        if result.get("status") != "ready":
            raise AcquisitionRouteError("torrent lane did not return ready")
        return dict(result)
    if lane in {"quark_sfx_archive", "quark_magnet_offline"}:
        handler = acquire_sfx if lane == "quark_sfx_archive" else acquire_offline
        if handler is None:
            raise AcquisitionRouteError(f"{lane} lane has no injected executor")
        result = handler(selection, destination)
        if result.get("status") != "ready":
            raise AcquisitionRouteError(f"{lane} lane did not return ready")
        return dict(result)

    try:
        receipt = fast_save(selection, destination)
    except AcquisitionRouteError:
        raise
    except Exception as exc:
        raise QuarkFastSaveInfrastructureError(str(exc)) from exc
    if not isinstance(receipt, Mapping):
        raise QuarkFastSaveInfrastructureError("fast-save port returned a non-object receipt")
    expected = _validated_fast_save_receipt(selection, destination, receipt)
    try:
        verify_arrival(destination, expected)
    except Exception as exc:
        raise QuarkFastSaveDeliveryError(str(exc)) from exc
    return {
        "status": "ready",
        "source_paths": [destination],
        "provider": "quark_share",
        "materialization": "fast_save",
        "saved_files": len(expected),
        "saved_bytes": sum(int(row["size"]) for row in expected),
    }
