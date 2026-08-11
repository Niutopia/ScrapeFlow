"""Read-only runtime readiness checks for stage-11 startup verification."""

from __future__ import annotations

from collections.abc import Callable, Mapping
import json
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urljoin, urlparse
from urllib.request import Request, urlopen

from engine.scrapeflow.provider_capabilities import (
    QUARK_HELPER_NAME,
    QUARK_HELPER_REQUIRED_ACTIONS,
)


LOOPBACK_HOSTS = frozenset({"127.0.0.1", "localhost", "::1"})
EXPECTED_PROVIDER_LANES = frozenset({"quark_share", "quark_magnet", "magnet"})


class RuntimeReadinessError(RuntimeError):
    """The runtime readiness probe could not complete."""


FetchJson = Callable[[str, float], tuple[int, object]]


def _is_loopback_api_url(value: str) -> bool:
    parsed = urlparse(value)
    return (
        parsed.scheme in {"http", "https"}
        and parsed.hostname in LOOPBACK_HOSTS
        and bool(parsed.netloc)
    )


def _endpoint(api_url: str, path: str) -> str:
    return urljoin(api_url.rstrip("/") + "/", path.lstrip("/"))


def _fetch_json(url: str, timeout: float) -> tuple[int, object]:
    request = Request(url, headers={"Accept": "application/json"})
    try:
        with urlopen(request, timeout=timeout) as response:  # noqa: S310
            status = int(response.status)
            body = response.read()
    except HTTPError as exc:
        status = int(exc.code)
        body = exc.read()
    except URLError as exc:
        raise RuntimeReadinessError(str(exc.reason)) from exc
    try:
        return status, json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RuntimeReadinessError(f"{url} did not return JSON") from exc


def _expect_bool(payload: Mapping[str, object], key: str, expected: bool, issues: list[str]) -> None:
    if payload.get(key) is not expected:
        issues.append(f"{key} must be {str(expected).lower()}")


def _int_value(value: object) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    return None


def _commit_matches(actual: object, expected: str | None) -> bool:
    if not expected:
        return True
    if not isinstance(actual, str) or not actual.strip():
        return False
    left = actual.strip()
    right = expected.strip()
    return left == right or left.startswith(right) or right.startswith(left)


def _check_quark_helper_readiness(
    health: Mapping[str, object],
    issues: list[str],
) -> None:
    """Require a live, authenticated helper proof rather than lane metadata.

    ``provider_capabilities`` intentionally remains a backwards-compatible
    declaration of the materializer contract.  Its static ``status=ready``
    must never be interpreted as evidence that the host-side Quark Helper is
    available for a real pilot.
    """
    helpers = health.get("helper_readiness")
    if not isinstance(helpers, Mapping):
        issues.append("health.helper_readiness must be an object")
        return
    helper = helpers.get(QUARK_HELPER_NAME)
    if not isinstance(helper, Mapping):
        issues.append("health.helper_readiness.quark must be an object")
        return
    for key in ("configured", "reachable", "authenticated"):
        if helper.get(key) is not True:
            issues.append(f"helper quark.{key} must be true")
    if helper.get("status") != "ready":
        issues.append("helper quark.status must be ready")
    def fixed_action_list(value: object, *, field: str) -> list[str] | None:
        if not isinstance(value, list) or any(
            not isinstance(action, str) or not action.strip()
            for action in value
        ):
            issues.append(f"helper quark.{field} must be a string list")
            return None
        normalized = [action.strip() for action in value]
        if len(normalized) != len(set(normalized)):
            issues.append(f"helper quark.{field} must not contain duplicates")
            return None
        if set(normalized) != set(QUARK_HELPER_REQUIRED_ACTIONS):
            issues.append(f"helper quark.{field} must match the fixed helper contract")
            return None
        return normalized

    required_actions = fixed_action_list(
        helper.get("required_actions"), field="required_actions",
    )
    actions = fixed_action_list(helper.get("actions"), field="actions")
    if required_actions is None or actions is None:
        return
    if set(required_actions) != set(actions):
        # This is intentionally redundant with the fixed-contract checks: it
        # leaves a clear diagnostic if a future schema relaxes one side.
        issues.append("helper quark.required_actions must equal actions")


def _check_health(
    health: Mapping[str, object],
    *,
    expected_commit: str | None,
    allow_existing_jobs: bool,
    issues: list[str],
) -> None:
    _expect_bool(health, "ok", True, issues)
    if health.get("mode") != "automatic":
        issues.append("health.mode must be automatic")
    _expect_bool(health, "connected", True, issues)
    _expect_bool(health, "tmdb_configured", True, issues)
    _expect_bool(health, "engine_configured", True, issues)
    if not _commit_matches(health.get("build_commit"), expected_commit):
        issues.append("health.build_commit does not match expected commit")

    lanes = health.get("provider_capabilities")
    if not isinstance(lanes, Mapping):
        issues.append("health.provider_capabilities must be an object")
    else:
        actual_lanes = {key for key in lanes if isinstance(key, str)}
        if actual_lanes != EXPECTED_PROVIDER_LANES:
            issues.append("health.provider_capabilities must expose exactly the fixed three lanes")
        for lane in EXPECTED_PROVIDER_LANES:
            row = lanes.get(lane)
            if not isinstance(row, Mapping):
                issues.append(f"provider lane {lane} must be an object")

    _check_quark_helper_readiness(health, issues)

    gates = health.get("lane_gates")
    if not isinstance(gates, Mapping):
        issues.append("health.lane_gates must be an object")
    else:
        if gates.get("provider_auto_repair_enabled") is not False:
            issues.append("provider auto repair gate must be off")
        if gates.get("audit_auto_repair_enabled") is not False:
            issues.append("audit repair gate must be off")

    if health.get("intake_monitoring") is not False:
        issues.append("intake monitoring must be off")
    intake = health.get("intake")
    if not isinstance(intake, Mapping):
        issues.append("health.intake must be an object")
    elif intake.get("enabled") is not False:
        issues.append("intake.enabled must be false")

    operations = health.get("operations")
    if not isinstance(operations, Mapping):
        issues.append("health.operations must be an object")
        return
    for key in ("formal_write_workers", "provider_workers", "provider_active"):
        value = _int_value(operations.get(key))
        if value != 0:
            issues.append(f"operations.{key} must be 0")
    if operations.get("audit_running") is not False:
        issues.append("operations.audit_running must be false")
    active_jobs = _int_value(operations.get("jobs_active"))
    if active_jobs != 0:
        issues.append("operations.jobs_active must be 0")
    if not allow_existing_jobs:
        total_jobs = _int_value(operations.get("jobs_total"))
        if total_jobs != 0:
            issues.append("operations.jobs_total must be 0 before opening acceptance")


def _check_control(control: Mapping[str, object], issues: list[str]) -> None:
    _expect_bool(control, "paused", True, issues)
    _expect_bool(control, "scheduler_paused", True, issues)
    _expect_bool(control, "persistent", True, issues)


def runtime_readiness_report(
    *,
    api_url: str,
    expected_commit: str | None = None,
    timeout: float = 5.0,
    allow_existing_jobs: bool = False,
    fetch_json: FetchJson = _fetch_json,
) -> dict[str, Any]:
    """Fetch health/control and return a read-only readiness report."""
    issues: list[str] = []
    if not _is_loopback_api_url(api_url):
        return {
            "status": "失败",
            "api_url": api_url,
            "issues": ["api_url must be an HTTP(S) loopback URL"],
            "health": None,
            "control": None,
        }

    endpoints = {
        "health": _endpoint(api_url, "/api/health"),
        "control": _endpoint(api_url, "/api/control"),
    }
    payloads: dict[str, object] = {}
    for name, url in endpoints.items():
        try:
            status, payload = fetch_json(url, timeout)
        except RuntimeReadinessError as exc:
            issues.append(f"{name} fetch failed: {exc}")
            continue
        if status != 200:
            issues.append(f"{name} returned HTTP {status}")
            continue
        if not isinstance(payload, Mapping):
            issues.append(f"{name} response must be a JSON object")
            continue
        payloads[name] = dict(payload)

    health = payloads.get("health")
    if isinstance(health, Mapping):
        _check_health(
            health,
            expected_commit=expected_commit,
            allow_existing_jobs=allow_existing_jobs,
            issues=issues,
        )
    control = payloads.get("control")
    if isinstance(control, Mapping):
        _check_control(control, issues)

    return {
        "status": "通过" if not issues else "失败",
        "api_url": api_url,
        "expected_commit": expected_commit or "",
        "allow_existing_jobs": allow_existing_jobs,
        "issues": issues,
        "health": health,
        "control": control,
    }


__all__ = [
    "RuntimeReadinessError",
    "runtime_readiness_report",
]
