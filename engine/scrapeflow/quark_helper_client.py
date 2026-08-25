"""Typed client for the fixed loopback Quark Helper actions.

The Helper sidecar shares the API container's network namespace and is the
only component that may operate the typed Quark action.  The API sends it a
reviewed, task-scoped manifest over loopback; it never drives a GUI or
forwards an AList cookie.  The sidecar resolves that short-lived Cookie
internally and uses the desktop renderer only for passive WSG transforms.

The fixed action contract is a lightweight ``health`` + ``share-save`` (2026-08-17): the
magnet submit/status impersonation path was removed with the ``quark_magnet``
tier, which Quark account-level rate-limiting made unusable in production.
Video fallback now uses the local Torrent lane with exact selected members.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
import ipaddress
import json
import os
from typing import Any, Protocol
import urllib.error
import urllib.parse
import urllib.request

DEFAULT_QUARK_HELPER_URL = "http://127.0.0.1:18765"


class QuarkHelperClientError(RuntimeError):
    failure_scope = "infrastructure"
    failure_stage = "quark_helper_request"
    reusable_candidate = False
    exclude_candidate = False


class _QuarkHelperRedirectError(QuarkHelperClientError):
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


class QuarkHelper(Protocol):
    def health(self) -> Mapping[str, Any]: ...

    def share_save(self, plan: Mapping[str, Any]) -> Mapping[str, Any]: ...


class HttpQuarkHelperClient:
    """HTTP client for the fixed loopback sidecar Helper actions."""

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
            raise QuarkHelperClientError("Quark helper URL is invalid")
        if (
            parsed.username
            or parsed.password
            or parsed.query
            or parsed.fragment
            or parsed.path not in {"", "/"}
        ):
            raise QuarkHelperClientError("Quark helper URL must not contain credentials")
        try:
            port = parsed.port
        except ValueError as exc:
            raise QuarkHelperClientError("Quark helper URL port is invalid") from exc
        if port is not None and not 1 <= port <= 65535:
            raise QuarkHelperClientError("Quark helper URL port is invalid")
        if not _approved_helper_host(parsed.hostname):
            raise QuarkHelperClientError(
                "Quark helper host must be loopback or Docker host bridge"
            )
        if not token or len(token) < 24:
            raise QuarkHelperClientError("Quark helper token is missing or too short")
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
        if path not in {"/health", "/v1/share-save"}:
            raise QuarkHelperClientError("unsupported Quark helper action")
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
                raise QuarkHelperClientError("Quark helper redirect refused") from exc
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
            if path == "/v1/share-save" and exc.code == 409 and in_doubt:
                from engine.scrapeflow.quark_fast_save_bridge import QuarkShareInDoubtError

                raise QuarkShareInDoubtError(
                    "Quark share-save is in doubt; reconcile the task destination"
                ) from exc
            if path == "/v1/share-save" and (
                failure_scope == "candidate" or exc.code == 422
            ):
                from engine.scrapeflow.quark_fast_save_bridge import QuarkShareExpiredError

                raise QuarkShareExpiredError(
                    "Quark share-save rejected the reviewed share candidate"
                ) from exc
            raise QuarkHelperClientError(
                f"Quark helper HTTP error: status={exc.code}"
            ) from exc
        except Exception as exc:
            if path == "/v1/share-save":
                from engine.scrapeflow.quark_fast_save_bridge import QuarkShareInDoubtError

                raise QuarkShareInDoubtError(
                    "Quark share-save response is unknown; reconcile before retrying"
                ) from exc
            raise QuarkHelperClientError(
                f"Quark helper unavailable: {type(exc).__name__}"
            ) from exc
        if not isinstance(value, Mapping):
            if path == "/v1/share-save":
                from engine.scrapeflow.quark_fast_save_bridge import QuarkShareInDoubtError

                raise QuarkShareInDoubtError(
                    "Quark share-save returned an invalid response; reconcile before retrying"
                )
            raise QuarkHelperClientError("Quark helper returned a non-object response")
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


__all__ = [
    "DEFAULT_QUARK_HELPER_URL",
    "HttpQuarkHelperClient",
    "QuarkHelper",
    "QuarkHelperClientError",
]
