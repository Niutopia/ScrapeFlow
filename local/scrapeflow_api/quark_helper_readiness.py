"""Small, redacted Quark Helper sidecar projection for ``/api/health``.

The fixed provider-capability table describes which materializer shapes the
runtime understands.  It cannot prove that the loopback sidecar is reachable,
authenticated, or implements the narrow action contract.  This module makes
that distinction explicit without exposing the helper token or invoking any
provider write action.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from datetime import UTC, datetime
import os
import threading
import time
from typing import Any, Protocol

from engine.scrapeflow.provider_capabilities import (
    QUARK_HELPER_NAME,
    QUARK_HELPER_REQUIRED_ACTIONS,
)
from engine.scrapeflow.quark_helper_client import (
    DEFAULT_QUARK_HELPER_URL,
    HttpQuarkHelperClient,
)


# A Helper health response authenticates against the live Quark renderer.  It
# is intentionally more expensive than an API liveness read, and the desktop
# renderer can legitimately take longer than a short HTTP dashboard poll.
# Keep the *deep* probe bounded, but do not turn a three-second UI deadline
# into a false ``unreachable`` verdict.
DEFAULT_HELPER_HEALTH_TIMEOUT_SECONDS = 20.0
DEFAULT_HELPER_HEALTH_CACHE_TTL_SECONDS = 30.0
DEFAULT_HELPER_HEALTH_WAIT_SLACK_SECONDS = 1.0


class QuarkHelperHealthClient(Protocol):
    """The only helper action needed by the read-only health projection."""

    def health(self) -> Mapping[str, Any]: ...


HelperClientFactory = Callable[[str, str, float], QuarkHelperHealthClient]


class QuarkHelperReadinessCache:
    """A bounded, single-flight cache for the expensive Helper proof.

    ``/api/health`` is an API-process liveness endpoint.  Calling the
    Helper's authenticated health action synchronously for every browser poll
    made that lightweight endpoint depend on CDP/renderer latency, and a
    fixed short timeout could make a healthy-but-slow Helper look offline.

    The cache deliberately distinguishes three facts:

    * a fresh deep proof (``fresh=True``) can be used as readiness evidence;
    * an expired result is ``stale`` and is *not* reported as ready, even if
      the previous proof was successful;
    * no completed proof is ``not_verified``, not ``unreachable``.

    A regular snapshot only schedules one daemon probe and returns
    immediately.  ``probe_now`` is the explicit, still read-only acceptance
    path: it waits for that same single in-flight probe rather than creating a
    second authentication request.
    """

    def __init__(
        self,
        *,
        ttl_seconds: float = DEFAULT_HELPER_HEALTH_CACHE_TTL_SECONDS,
        probe_timeout: float = DEFAULT_HELPER_HEALTH_TIMEOUT_SECONDS,
        client_factory: HelperClientFactory | None = None,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        if ttl_seconds <= 0:
            raise ValueError("Helper readiness cache TTL must be positive")
        if probe_timeout <= 0:
            raise ValueError("Helper readiness probe timeout must be positive")
        self._ttl_seconds = float(ttl_seconds)
        self._probe_timeout = float(probe_timeout)
        self._client_factory = _helper_client if client_factory is None else client_factory
        self._monotonic = monotonic
        self._lock = threading.Lock()
        self._configuration_key: tuple[str, str] | None = None
        self._report: dict[str, object] | None = None
        self._completed_at: float | None = None
        self._checked_at: str | None = None
        self._inflight = False
        self._inflight_event: threading.Event | None = None

    @staticmethod
    def _configuration(
        environ: Mapping[str, object] | None,
    ) -> tuple[tuple[str, str] | None, dict[str, object] | None]:
        source: Mapping[str, object] = os.environ if environ is None else environ
        url = (
            _environment_text(source, "SCRAPEFLOW_QUARK_HELPER_URL")
            or DEFAULT_QUARK_HELPER_URL
        )
        token = _environment_text(source, "SCRAPEFLOW_QUARK_HELPER_TOKEN")
        if not token:
            return None, _snapshot(
                configured=False,
                url_configured=bool(url),
                token_configured=False,
                status="not_configured",
                reachable=False,
                authenticated=False,
                reason="helper token must be configured",
            )
        if len(token) < 24:
            return None, _snapshot(
                configured=True,
                url_configured=bool(url),
                token_configured=True,
                status="invalid_configuration",
                reachable=False,
                authenticated=False,
                reason="helper token configuration is invalid",
            )
        # This value is intentionally retained only in process memory as the
        # cache identity.  Neither the URL nor token is placed in a report.
        return (url, token), None

    @staticmethod
    def _timestamp() -> str:
        return datetime.now(UTC).replace(microsecond=0).isoformat().replace(
            "+00:00", "Z",
        )

    def _reset_for_configuration_locked(self, key: tuple[str, str]) -> None:
        if self._configuration_key == key:
            return
        self._configuration_key = key
        self._report = None
        self._completed_at = None
        self._checked_at = None
        # An older probe may still finish after a configuration change.  Its
        # worker compares the key before publishing, so it cannot leak the
        # former token/endpoint's result into the new view.
        self._inflight = False
        self._inflight_event = None

    def _start_probe_locked(self, key: tuple[str, str]) -> threading.Event:
        if self._inflight and self._inflight_event is not None:
            return self._inflight_event
        event = threading.Event()
        self._inflight = True
        self._inflight_event = event
        thread = threading.Thread(
            target=self._run_probe,
            args=(key, event),
            daemon=True,
            name="scrapeflow-quark-helper-readiness",
        )
        thread.start()
        return event

    def _run_probe(self, key: tuple[str, str], event: threading.Event) -> None:
        url, token = key
        report = _probe_quark_helper_readiness(
            url=url,
            token=token,
            timeout=self._probe_timeout,
            client_factory=self._client_factory,
        )
        completed_at = self._monotonic()
        checked_at = self._timestamp()
        with self._lock:
            if self._configuration_key == key:
                self._report = report
                self._completed_at = completed_at
                self._checked_at = checked_at
                self._inflight = False
                self._inflight_event = None
        # Always release explicit waiters, including those which were waiting
        # for a probe invalidated by a configuration change.
        event.set()

    @staticmethod
    def _not_verified_view(
        base: Mapping[str, object],
        *,
        refreshing: bool,
        reason: str,
    ) -> dict[str, object]:
        output = dict(base)
        output.update({
            "status": "not_verified",
            # ``None`` means unknown: a deep probe has not proved either
            # reachability or authentication yet.  Returning false here would
            # incorrectly claim an outage merely because a background check
            # has not finished.
            "reachable": None,
            "authenticated": None,
            "actions": [],
            "verified": False,
            "fresh": False,
            "stale": False,
            "refreshing": refreshing,
            "last_checked_at": None,
            "last_error": None,
            "reason": reason,
        })
        return output

    @staticmethod
    def _configuration_view(base: Mapping[str, object]) -> dict[str, object]:
        output = dict(base)
        output.update({
            "verified": False,
            "fresh": False,
            "stale": False,
            "refreshing": False,
            "last_checked_at": None,
            "last_error": output.get("reason") or None,
        })
        return output

    def _cached_view_locked(
        self,
        key: tuple[str, str],
        *,
        start_refresh: bool,
    ) -> dict[str, object]:
        # The cache is keyed by a configured endpoint/token pair.  Resetting
        # first is also what makes an environment change fail closed.
        self._reset_for_configuration_locked(key)
        now = self._monotonic()
        report = self._report
        completed_at = self._completed_at
        if report is None or completed_at is None:
            refreshing = self._inflight
            if start_refresh and not refreshing:
                self._start_probe_locked(key)
                refreshing = True
            base = _snapshot(
                configured=True,
                url_configured=True,
                token_configured=True,
                status="not_verified",
                reachable=False,
                authenticated=False,
            )
            return self._not_verified_view(
                base,
                refreshing=refreshing,
                reason="Helper 深度认证尚未完成",
            )

        age_seconds = max(0.0, now - completed_at)
        fresh = age_seconds <= self._ttl_seconds
        if fresh:
            output = dict(report)
            output.update({
                "verified": output.get("status") == "ready",
                "fresh": True,
                "stale": False,
                "refreshing": self._inflight,
                "last_checked_at": self._checked_at,
                "last_error": (
                    output.get("reason") if output.get("status") != "ready" else None
                ),
                "age_seconds": round(age_seconds, 3),
            })
            return output

        refreshing = self._inflight
        if start_refresh and not refreshing:
            self._start_probe_locked(key)
            refreshing = True
        # A past ready result must never silently keep a green readiness
        # indicator after it expires.  Preserve only redacted diagnostic
        # evidence about the prior probe; availability is deliberately
        # unknown until the fresh probe completes.
        output = _snapshot(
            configured=True,
            url_configured=True,
            token_configured=True,
            status="stale",
            reachable=False,
            authenticated=False,
            actions=[],
            reason="Helper 深度认证结果已过期，等待重新验证",
        )
        output.update({
            "reachable": None,
            "authenticated": None,
            "verified": False,
            "fresh": False,
            "stale": True,
            "refreshing": refreshing,
            "last_checked_at": self._checked_at,
            "last_status": report.get("status"),
            "last_error": (
                report.get("reason") if report.get("status") != "ready" else None
            ),
            "age_seconds": round(age_seconds, 3),
        })
        return output

    def snapshot(
        self,
        environ: Mapping[str, object] | None = None,
    ) -> dict[str, object]:
        """Return immediately and, when necessary, schedule one deep probe."""
        key, invalid = self._configuration(environ)
        if key is None:
            assert invalid is not None
            return self._configuration_view(invalid)
        with self._lock:
            return self._cached_view_locked(key, start_refresh=True)

    def probe_now(
        self,
        environ: Mapping[str, object] | None = None,
        *,
        wait_timeout: float | None = None,
    ) -> dict[str, object]:
        """Force one fresh, read-only proof and wait only a bounded period.

        This is deliberately the endpoint used by isolated acceptance.  It
        joins an already-running cache probe instead of issuing a duplicate
        authentication request, preserving the single-flight guarantee.
        """
        key, invalid = self._configuration(environ)
        if key is None:
            assert invalid is not None
            output = self._configuration_view(invalid)
            output["probe_mode"] = "explicit"
            return output
        with self._lock:
            self._reset_for_configuration_locked(key)
            event = self._start_probe_locked(key)
        bounded_wait = (
            self._probe_timeout + DEFAULT_HELPER_HEALTH_WAIT_SLACK_SECONDS
            if wait_timeout is None else max(0.0, float(wait_timeout))
        )
        event.wait(bounded_wait)
        with self._lock:
            output = self._cached_view_locked(key, start_refresh=False)
        output["probe_mode"] = "explicit"
        if event.is_set():
            return output
        # A badly behaved transport/test double can ignore its supplied
        # timeout.  The acceptance caller still receives a bounded, honest
        # result and does not mistake an unfinished probe for an outage.
        output["reason"] = "Helper 深度认证仍在进行，尚未得到可用结论"
        output["last_error"] = "Helper 深度认证等待超时"
        return output


def _environment_text(environ: Mapping[str, object], key: str) -> str:
    value = environ.get(key, "")
    return value.strip() if isinstance(value, str) else ""


def _helper_client(url: str, token: str, timeout: float) -> QuarkHelperHealthClient:
    return HttpQuarkHelperClient(url, token, timeout=timeout)


def _actions(value: object) -> list[str] | None:
    if not isinstance(value, list):
        return None
    output: list[str] = []
    for item in value:
        if not isinstance(item, str):
            return None
        action = item.strip()
        if not action or action in output:
            return None
        output.append(action)
    return output


def _snapshot(
    *,
    configured: bool,
    url_configured: bool,
    token_configured: bool,
    status: str,
    reachable: bool,
    authenticated: bool,
    actions: list[str] | None = None,
    helper_status: str = "",
    reason: str = "",
) -> dict[str, object]:
    """Return a JSON-safe helper record with no credential-bearing values."""
    output: dict[str, object] = {
        "name": QUARK_HELPER_NAME,
        "configured": configured,
        "url_configured": url_configured,
        "token_configured": token_configured,
        "reachable": reachable,
        "authenticated": authenticated,
        "status": status,
        "required_actions": list(QUARK_HELPER_REQUIRED_ACTIONS),
        "actions": list(actions or []),
    }
    if helper_status:
        output["helper_status"] = helper_status
    if reason:
        output["reason"] = reason
    return output


def _probe_quark_helper_readiness(
    *,
    url: str,
    token: str,
    timeout: float,
    client_factory: HelperClientFactory,
) -> dict[str, object]:
    """Perform one authenticated read-only Helper health request.

    The caller is responsible for choosing whether this is an explicit
    acceptance probe or the background work of :class:`QuarkHelperReadinessCache`.
    This function intentionally never returns exception text, URLs, or tokens.
    """
    url_configured = bool(url)
    token_configured = bool(token)
    configured = token_configured
    if not configured:
        return _snapshot(
            configured=False,
            url_configured=url_configured,
            token_configured=token_configured,
            status="not_configured",
            reachable=False,
            authenticated=False,
            reason="helper token must be configured",
        )
    if len(token) < 24:
        return _snapshot(
            configured=True,
            url_configured=url_configured,
            token_configured=True,
            status="invalid_configuration",
            reachable=False,
            authenticated=False,
            reason="helper token configuration is invalid",
        )

    try:
        client = client_factory(url, token, timeout)
    except Exception:
        return _snapshot(
            configured=True,
            url_configured=True,
            token_configured=True,
            status="invalid_configuration",
            reachable=False,
            authenticated=False,
            reason="helper URL or token configuration is invalid",
        )

    try:
        payload = client.health()
    except Exception:
        return _snapshot(
            configured=True,
            url_configured=True,
            token_configured=True,
            status="unreachable",
            reachable=False,
            authenticated=False,
            reason="helper health request failed",
        )
    if not isinstance(payload, Mapping):
        return _snapshot(
            configured=True,
            url_configured=True,
            token_configured=True,
            status="invalid_response",
            reachable=True,
            authenticated=False,
            reason="helper health response must be an object",
        )

    raw_status = payload.get("status")
    helper_status = raw_status.strip().casefold() if isinstance(raw_status, str) else ""
    authenticated = payload.get("authenticated") is True
    actions = _actions(payload.get("actions"))
    if helper_status not in {"ok", "ready"}:
        return _snapshot(
            configured=True,
            url_configured=True,
            token_configured=True,
            status="not_ready",
            reachable=True,
            authenticated=authenticated,
            actions=actions,
            helper_status=helper_status,
            reason="helper health response is not ready",
        )
    if not authenticated:
        return _snapshot(
            configured=True,
            url_configured=True,
            token_configured=True,
            status="not_ready",
            reachable=True,
            authenticated=False,
            actions=actions,
            helper_status=helper_status,
            reason="helper health response is not authenticated",
        )
    if actions is None or set(actions) != set(QUARK_HELPER_REQUIRED_ACTIONS):
        return _snapshot(
            configured=True,
            url_configured=True,
            token_configured=True,
            status="not_ready",
            reachable=True,
            authenticated=True,
            actions=actions,
            helper_status=helper_status,
            reason="helper actions do not match the fixed contract",
        )
    return _snapshot(
        configured=True,
        url_configured=True,
        token_configured=True,
        status="ready",
        reachable=True,
        authenticated=True,
        actions=actions,
        helper_status=helper_status,
    )


def quark_helper_readiness_from_env(
    environ: Mapping[str, object] | None = None,
    *,
    timeout: float = DEFAULT_HELPER_HEALTH_TIMEOUT_SECONDS,
    client_factory: HelperClientFactory = _helper_client,
) -> dict[str, object]:
    """Run one explicit, authenticated, read-only Helper proof.

    This backward-compatible helper remains intentionally synchronous for
    callers that *explicitly* requested a deep proof (for example focused
    tests).  Ordinary API liveness uses :class:`QuarkHelperReadinessCache`
    instead and never invokes this function on its request thread.
    """
    source: Mapping[str, object] = os.environ if environ is None else environ
    url = (
        _environment_text(source, "SCRAPEFLOW_QUARK_HELPER_URL")
        or DEFAULT_QUARK_HELPER_URL
    )
    token = _environment_text(source, "SCRAPEFLOW_QUARK_HELPER_TOKEN")
    return _probe_quark_helper_readiness(
        url=url,
        token=token,
        timeout=timeout,
        client_factory=client_factory,
    )


__all__ = [
    "DEFAULT_HELPER_HEALTH_CACHE_TTL_SECONDS",
    "DEFAULT_HELPER_HEALTH_TIMEOUT_SECONDS",
    "DEFAULT_HELPER_HEALTH_WAIT_SLACK_SECONDS",
    "QuarkHelperReadinessCache",
    "quark_helper_readiness_from_env",
]
