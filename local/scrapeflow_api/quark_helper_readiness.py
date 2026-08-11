"""Small, redacted Quark Helper liveness projection for ``/api/health``.

The fixed provider-capability table describes which materializer shapes the
runtime understands.  It cannot prove that the host-side helper is reachable,
authenticated, or implements the narrow action contract.  This module makes
that distinction explicit without exposing the helper token or invoking any
provider write action.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
import os
from typing import Any, Protocol

from engine.scrapeflow.provider_capabilities import (
    QUARK_HELPER_NAME,
    QUARK_HELPER_REQUIRED_ACTIONS,
)
from engine.scrapeflow.quark_magnet_offline_bridge import (
    DEFAULT_QUARK_HELPER_URL,
    HttpQuarkHelperClient,
)


DEFAULT_HELPER_HEALTH_TIMEOUT_SECONDS = 3.0


class QuarkHelperHealthClient(Protocol):
    """The only helper action needed by the read-only health projection."""

    def health(self) -> Mapping[str, Any]: ...


HelperClientFactory = Callable[[str, str, float], QuarkHelperHealthClient]


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


def quark_helper_readiness_from_env(
    environ: Mapping[str, object] | None = None,
    *,
    timeout: float = DEFAULT_HELPER_HEALTH_TIMEOUT_SECONDS,
    client_factory: HelperClientFactory = _helper_client,
) -> dict[str, object]:
    """Probe only ``health`` and expose a redacted, structural readiness row.

    A healthy HTTP response is insufficient: the helper must explicitly
    attest authentication and its exact fixed action set.  Any configuration,
    transport, authentication, or schema problem fails closed.  The return
    value intentionally contains no helper URL, token, or raw exception text.
    """
    source: Mapping[str, object] = os.environ if environ is None else environ
    url = (
        _environment_text(source, "SCRAPEFLOW_QUARK_HELPER_URL")
        or DEFAULT_QUARK_HELPER_URL
    )
    token = _environment_text(source, "SCRAPEFLOW_QUARK_HELPER_TOKEN")
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


__all__ = [
    "DEFAULT_HELPER_HEALTH_TIMEOUT_SECONDS",
    "quark_helper_readiness_from_env",
]
