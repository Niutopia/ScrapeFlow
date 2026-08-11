"""Typed client and bridge for the fixed host-side Quark Helper actions.

The Helper is deliberately the only component that may operate the host's
logged-in Quark session.  The API/container sends it a reviewed, task-scoped
manifest over loopback; it never drives a GUI or forwards an AList cookie.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
import ipaddress
import json
import os
from pathlib import PurePosixPath
import re
import time
from typing import Any, Protocol
import urllib.error
import urllib.parse
import urllib.request

from .provider_capabilities import QUARK_HELPER_REQUIRED_ACTIONS


DEFAULT_QUARK_HELPER_URL = "http://host.docker.internal:18765"


class QuarkMagnetBridgeError(RuntimeError):
    failure_scope = "infrastructure"
    failure_stage = "quark_magnet_submit"
    reusable_candidate = False
    exclude_candidate = False


class QuarkMagnetCandidateError(QuarkMagnetBridgeError):
    failure_scope = "candidate"
    failure_stage = "quark_magnet_candidate"
    exclude_candidate = True


class QuarkMagnetDeliveryError(QuarkMagnetBridgeError):
    failure_scope = "delivery"
    failure_stage = "quark_magnet_progress"
    reusable_candidate = True


class QuarkMagnetInDoubtError(QuarkMagnetBridgeError):
    failure_scope = "in_doubt"
    failure_stage = "quark_magnet_submit_in_doubt"
    reusable_candidate = True


class _QuarkHelperRedirectError(QuarkMagnetBridgeError):
    """The fixed Helper client never follows an HTTP redirect."""


class _RejectRedirectHandler(urllib.request.HTTPRedirectHandler):
    """Turn every 3xx response into an error before a second request exists."""

    def redirect_request(self, _req, _fp, _code, _msg, _headers, _newurl):
        return None


# The helper bearer token and reviewed manifests must never traverse an
# environment proxy, and urllib's normal redirect behavior is unsafe for a
# fixed local control boundary.  This opener is deliberately process-local and
# has no cookie jar.
_HELPER_OPENER = urllib.request.build_opener(
    urllib.request.ProxyHandler({}),
    _RejectRedirectHandler(),
)


def _open_helper_request(request: Any, timeout: float) -> Any:
    return _HELPER_OPENER.open(request, timeout=timeout)


def _approved_helper_host(value: str) -> bool:
    """Allow only numeric loopback addresses or the Docker host bridge name."""
    try:
        return ipaddress.ip_address(value).is_loopback
    except ValueError:
        return value.casefold() in {"localhost", "host.docker.internal"}


def _require_helper_health(value: object) -> Mapping[str, Any]:
    """Validate the complete fixed Helper contract before any mutation."""
    if not isinstance(value, Mapping):
        raise QuarkMagnetBridgeError("Quark helper health returned an invalid object")
    status = str(value.get("status") or "").strip().casefold()
    if status not in {"ok", "ready"}:
        raise QuarkMagnetBridgeError("Quark helper is not ready")
    if value.get("authenticated") is not True:
        raise QuarkMagnetBridgeError("Quark helper is not authenticated")
    actions = value.get("actions")
    if (
        not isinstance(actions, list)
        or any(not isinstance(action, str) for action in actions)
        or len(actions) != len(set(actions))
        or set(actions) != set(QUARK_HELPER_REQUIRED_ACTIONS)
    ):
        raise QuarkMagnetBridgeError("Quark helper actions do not match the fixed contract")
    return value


class QuarkHelper(Protocol):
    def health(self) -> Mapping[str, Any]: ...

    def share_save(self, plan: Mapping[str, Any]) -> Mapping[str, Any]: ...

    def magnet_submit(self, plan: Mapping[str, Any]) -> Mapping[str, Any]: ...

    def magnet_status(self, task_id: str) -> Mapping[str, Any]: ...


# Keep the old protocol name for source compatibility with the magnet bridge.
# It is intentionally an alias of the complete fixed-action contract: a real
# Helper cannot implement a magnet-only subset and still claim readiness.
QuarkMagnetHelper = QuarkHelper


class HttpQuarkHelperClient:
    """HTTP client for the fixed host-side Quark helper actions."""

    def __init__(
        self,
        base_url: str,
        token: str,
        *,
        timeout: float = 90.0,
        opener: Callable[..., Any] = _open_helper_request,
    ) -> None:
        parsed = urllib.parse.urlsplit(base_url.rstrip("/"))
        if parsed.scheme not in {"http", "https"} or not parsed.hostname:
            raise QuarkMagnetBridgeError("Quark helper URL is invalid")
        if (
            parsed.username
            or parsed.password
            or parsed.query
            or parsed.fragment
            or parsed.path not in {"", "/"}
        ):
            raise QuarkMagnetBridgeError("Quark helper URL must not contain credentials")
        try:
            port = parsed.port
        except ValueError as exc:
            raise QuarkMagnetBridgeError("Quark helper URL port is invalid") from exc
        if port is not None and not 1 <= port <= 65535:
            raise QuarkMagnetBridgeError("Quark helper URL port is invalid")
        if not _approved_helper_host(parsed.hostname):
            raise QuarkMagnetBridgeError(
                "Quark helper host must be loopback or Docker host bridge"
            )
        if not token or len(token) < 24:
            raise QuarkMagnetBridgeError("Quark helper token is missing or too short")
        self.base_url = base_url.rstrip("/")
        self.token = token
        self.timeout = timeout
        self.opener = opener

    @classmethod
    def from_env(cls) -> "HttpQuarkHelperClient":
        return cls(
            os.getenv("SCRAPEFLOW_QUARK_HELPER_URL", "").strip()
            or DEFAULT_QUARK_HELPER_URL,
            os.getenv("SCRAPEFLOW_QUARK_HELPER_TOKEN", "").strip(),
        )

    def _request(self, path: str, body: Mapping[str, Any] | None = None) -> Mapping[str, Any]:
        if path not in {
            "/health",
            "/v1/share-save",
            "/v1/magnet-submit",
            "/v1/magnet-status",
        }:
            raise QuarkMagnetBridgeError("unsupported Quark helper action")
        payload = None if body is None else json.dumps(body, ensure_ascii=False).encode()
        request = urllib.request.Request(
            self.base_url + path,
            data=payload,
            method="GET" if body is None else "POST",
            headers={
                "Accept": "application/json",
                "Content-Type": "application/json",
                "Authorization": "Bearer " + self.token,
            },
        )
        try:
            with self.opener(request, timeout=self.timeout) as response:
                response_status = getattr(response, "status", None)
                if response_status is None:
                    getcode = getattr(response, "getcode", None)
                    response_status = getcode() if callable(getcode) else None
                if (
                    isinstance(response_status, int)
                    and 300 <= response_status < 400
                ):
                    raise _QuarkHelperRedirectError("Quark helper redirect refused")
                geturl = getattr(response, "geturl", None)
                response_url = geturl() if callable(geturl) else request.full_url
                if response_url != request.full_url:
                    raise _QuarkHelperRedirectError("Quark helper redirect refused")
                value = json.load(response)
        except _QuarkHelperRedirectError:
            raise
        except urllib.error.HTTPError as exc:
            if 300 <= exc.code < 400:
                raise QuarkMagnetBridgeError("Quark helper redirect refused") from exc
            raw = exc.read(16_384)
            in_doubt = False
            failure_scope = ""
            try:
                error_value = json.loads(raw.decode("utf-8", "replace"))
                if isinstance(error_value, Mapping):
                    in_doubt = error_value.get("in_doubt") is True
                    raw_scope = error_value.get("failure_scope")
                    failure_scope = (
                        raw_scope.strip().casefold()
                        if isinstance(raw_scope, str) else ""
                    )
            except (UnicodeError, json.JSONDecodeError):
                pass
            # A received HTTP error proves that the Helper answered.  In
            # particular, ``503 not_ready`` is a retryable infrastructure
            # outage, not a possibly-created Quark task.  Only the Helper's
            # explicit reconciliation response (409 + in_doubt) is ambiguous
            # at this boundary; lost transport responses are handled below.
            if path in {"/v1/share-save", "/v1/magnet-submit"} and (
                exc.code == 409 and in_doubt
            ):
                if path == "/v1/share-save":
                    from engine.scrapeflow.quark_fast_save_bridge import QuarkShareInDoubtError

                    raise QuarkShareInDoubtError(
                        "Quark share-save is in doubt; reconcile the task destination"
                    ) from exc
                raise QuarkMagnetInDoubtError(
                    "Quark magnet submit is in doubt; reconcile the task destination"
                ) from exc
            if path == "/v1/share-save" and (
                failure_scope == "candidate" or exc.code == 422
            ):
                from engine.scrapeflow.quark_fast_save_bridge import QuarkShareExpiredError

                raise QuarkShareExpiredError(
                    "Quark share-save rejected the reviewed share candidate"
                ) from exc
            if path == "/v1/magnet-submit" and (
                failure_scope == "candidate" or exc.code == 422
            ):
                # This is an explicit, received rejection of the reviewed
                # magnet payload.  It is neither a Helper outage nor an
                # ambiguous post-submit result, so normal candidate exclusion
                # rules may safely take over.
                raise QuarkMagnetCandidateError(
                    "Quark helper rejected the reviewed magnet candidate"
                ) from exc
            raise QuarkMagnetBridgeError(
                f"Quark helper HTTP error: status={exc.code}"
            ) from exc
        except Exception as exc:
            if path in {"/v1/share-save", "/v1/magnet-submit"}:
                if path == "/v1/share-save":
                    from engine.scrapeflow.quark_fast_save_bridge import QuarkShareInDoubtError

                    raise QuarkShareInDoubtError(
                        "Quark share-save response is unknown; reconcile before retrying"
                    ) from exc
                raise QuarkMagnetInDoubtError(
                    "Quark magnet submit response is unknown; reconcile before retrying"
                ) from exc
            raise QuarkMagnetBridgeError(
                f"Quark helper unavailable: {type(exc).__name__}"
            ) from exc
        if not isinstance(value, Mapping):
            if path in {"/v1/share-save", "/v1/magnet-submit"}:
                if path == "/v1/share-save":
                    from engine.scrapeflow.quark_fast_save_bridge import QuarkShareInDoubtError

                    raise QuarkShareInDoubtError(
                        "Quark share-save returned an invalid response; reconcile before retrying"
                    )
                raise QuarkMagnetInDoubtError(
                    "Quark magnet submit returned an invalid response; reconcile before retrying"
                )
            raise QuarkMagnetBridgeError("Quark helper returned a non-object response")
        return value

    def health(self) -> Mapping[str, Any]:
        return self._request("/health")

    def share_save(self, plan: Mapping[str, Any]) -> Mapping[str, Any]:
        """Save/reconcile one reviewed share into its exact task staging root.

        ``plan`` is intentionally opaque to this transport.  It is constructed
        and validated by the materializer; the Helper must treat it as the
        complete authority boundary and return only after the task is
        reconcilable.  A lost submit response is classified as ``in_doubt``.
        """
        return self._request("/v1/share-save", plan)

    def magnet_submit(self, plan: Mapping[str, Any]) -> Mapping[str, Any]:
        return self._request("/v1/magnet-submit", plan)

    def magnet_status(self, task_id: str) -> Mapping[str, Any]:
        return self._request("/v1/magnet-status", {"task_id": task_id})


def _safe_relative_path(value: object) -> str:
    if not isinstance(value, str) or not value or value.startswith("/") or "\\" in value:
        raise QuarkMagnetCandidateError("offline expected file path is invalid")
    normalized = str(PurePosixPath(value))
    if normalized != value or any(part in {"", ".", ".."} for part in normalized.split("/")):
        raise QuarkMagnetCandidateError("offline expected file path is unsafe")
    return normalized


def normalize_quark_magnet_selection(
    selection: Mapping[str, Any],
    destination: str,
) -> dict[str, Any]:
    if str(selection.get("provider") or "").strip().casefold() != "quark_magnet":
        raise QuarkMagnetCandidateError("candidate is not quark_magnet")
    acquisition = selection.get("acquisition")
    selected = selection.get("selected_gap_ids")
    if (
        not isinstance(acquisition, Mapping)
        or acquisition.get("kind") != "quark_magnet_offline"
        or not isinstance(selected, list)
        or not selected
    ):
        raise QuarkMagnetCandidateError("candidate lacks quark_magnet_offline acquisition")
    magnet = acquisition.get("magnet_url")
    if not isinstance(magnet, str) or not magnet.startswith("magnet:?"):
        raise QuarkMagnetCandidateError("offline candidate lacks a magnet URL")
    query = urllib.parse.parse_qs(urllib.parse.urlsplit(magnet).query)
    xt = query.get("xt", [""])[0]
    match = re.fullmatch(r"urn:btih:([0-9a-fA-F]{40}|[A-Z2-7a-z2-7]{32})", xt)
    if match is None:
        raise QuarkMagnetCandidateError("offline candidate has an invalid BTIH")
    rows = acquisition.get("expected_files")
    if not isinstance(rows, list) or not rows:
        raise QuarkMagnetCandidateError("offline candidate lacks exact expected files")
    selected_set = {str(gap) for gap in selected if isinstance(gap, str) and gap}
    if len(selected_set) != len(selected):
        raise QuarkMagnetCandidateError("offline selected gaps are invalid")
    expected: list[dict[str, Any]] = []
    covered: set[str] = set()
    for row in rows:
        if not isinstance(row, Mapping):
            raise QuarkMagnetCandidateError("offline expected file is invalid")
        path = _safe_relative_path(row.get("path"))
        size = row.get("size")
        gaps = row.get("gap_ids")
        if type(size) is not int or size <= 0 or not isinstance(gaps, list) or not gaps:
            raise QuarkMagnetCandidateError("offline expected file manifest is invalid")
        selected_gaps = [
            str(gap) for gap in gaps
            if isinstance(gap, str) and gap in selected_set
        ]
        if not selected_gaps:
            continue
        covered.update(selected_gaps)
        item = {"path": path, "size": size, "gap_ids": selected_gaps}
        torrent_index = row.get("torrent_index")
        if torrent_index is not None:
            if type(torrent_index) is not int or torrent_index <= 0:
                raise QuarkMagnetCandidateError("offline torrent index is invalid")
            item["torrent_index"] = torrent_index
        expected.append(item)
    if covered != selected_set:
        raise QuarkMagnetCandidateError("offline expected files do not cover selected gaps")
    return {
        "status": "dry_run",
        "provider": "quark_magnet",
        "destination": destination,
        "magnet_url": magnet,
        "infohash": match.group(1).lower(),
        "selected_gap_ids": list(selected),
        "expected_files": expected,
        "helper_actions": list(QUARK_HELPER_REQUIRED_ACTIONS),
    }


class QuarkMagnetOfflineBridge:
    """Submit a reviewed magnet to a fixed host-side Quark helper."""

    def __init__(
        self,
        helper: QuarkMagnetHelper,
        *,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self.helper = helper
        self.sleep = sleep

    @staticmethod
    def dry_run(selection: Mapping[str, Any], destination: str) -> dict[str, Any]:
        return normalize_quark_magnet_selection(selection, destination)

    def execute(
        self,
        selection: Mapping[str, Any],
        destination: str,
        *,
        task_id: str | None = None,
        on_task_id: Callable[[str], None] | None = None,
    ) -> dict[str, Any]:
        plan = normalize_quark_magnet_selection(selection, destination)
        _require_helper_health(self.helper.health())
        if not isinstance(task_id, str) or not task_id:
            submitted = self.helper.magnet_submit({
                "destination": destination,
                "magnet_url": plan["magnet_url"],
                "infohash": plan["infohash"],
                "selected_gap_ids": plan["selected_gap_ids"],
                "expected_files": plan["expected_files"],
                "title": selection.get("release_name"),
            })
            task_id = submitted.get("task_id")
            if not isinstance(task_id, str) or not task_id:
                raise QuarkMagnetInDoubtError(
                    "Quark helper did not return task_id; reconcile before retrying"
                )
            if on_task_id is not None:
                try:
                    on_task_id(task_id)
                except Exception as exc:
                    raise QuarkMagnetInDoubtError(
                        "Quark magnet task was submitted but local attempt state was not saved"
                    ) from exc
        try:
            polls = int(os.getenv("SCRAPEFLOW_QUARK_MAGNET_STATUS_POLLS", "7200"))
        except ValueError:
            polls = 7200
        polls = max(1, min(86400, polls))
        for _index in range(polls):
            status = self.helper.magnet_status(task_id)
            state = str(status.get("status") or "").strip().casefold()
            if state in {"finished", "success", "done", "ready"}:
                return {
                    "status": "submitted",
                    "destination": destination,
                    "expected_files": plan["expected_files"],
                    "task_id": task_id,
                    "infohash": plan["infohash"],
                }
            if state in {"candidate_failed", "failed", "error", "rejected"}:
                raise QuarkMagnetCandidateError("Quark offline task rejected the magnet payload")
            self.sleep(1.0)
        raise QuarkMagnetDeliveryError("Quark offline task did not finish before timeout")


__all__ = [
    "DEFAULT_QUARK_HELPER_URL",
    "HttpQuarkHelperClient",
    "QuarkHelper",
    "QuarkMagnetBridgeError",
    "QuarkMagnetCandidateError",
    "QuarkMagnetDeliveryError",
    "QuarkMagnetInDoubtError",
    "QuarkMagnetOfflineBridge",
    "normalize_quark_magnet_selection",
]
