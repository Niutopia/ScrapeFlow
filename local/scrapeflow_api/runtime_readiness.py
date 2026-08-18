"""Read-only runtime readiness checks for stage-11 startup verification."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from datetime import datetime, timedelta
import json
import re
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urljoin, urlparse
from urllib.request import ProxyHandler, Request, build_opener

from engine.scrapeflow.provider_capabilities import (
    QUARK_HELPER_NAME,
    QUARK_HELPER_REQUIRED_ACTIONS,
)
from local.scrapeflow_api.quark_helper_readiness import (
    DEFAULT_HELPER_HEALTH_TIMEOUT_SECONDS,
    DEFAULT_HELPER_HEALTH_WAIT_SLACK_SECONDS,
)


LOOPBACK_HOSTS = frozenset({"127.0.0.1", "localhost", "::1"})
EXPECTED_PROVIDER_LANES = frozenset({"quark_share", "magnet"})
_BUILD_ID_RE = re.compile(r"(?P<sha>[0-9a-f]{7,64})(?P<dirty>-dirty)?\Z")


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
    # Loopback probes must never leak through an ambient host proxy: macOS
    # forwards them to the proxy port, which dials its own loopback instead
    # of this machine and makes the readiness check hang or report garbage.
    opener = build_opener(ProxyHandler({}))
    try:
        with opener.open(request, timeout=timeout) as response:
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


def _parse_build_id(value: object) -> tuple[str, bool] | None:
    """Return a reviewed build identifier without accepting placeholders.

    A production image carries either a lowercase Git SHA (full or a
    human-recorded prefix of at least seven hexadecimal characters), or that
    identifier with the explicit ``-dirty`` suffix.  In particular, empty
    strings and the Dockerfile's safe ``unrecorded`` fallback are evidence of
    an unknown image, not a version identifier.
    """
    if not isinstance(value, str):
        return None
    match = _BUILD_ID_RE.fullmatch(value.strip())
    if match is None:
        return None
    return match.group("sha"), match.group("dirty") == "-dirty"


def _build_id_matches(actual: object, expected: object) -> bool:
    """Match only in the safe direction: actual begins with expected SHA.

    A short expected SHA is a deliberate operator assertion, so an image
    carrying the full SHA may satisfy it.  The reverse is never accepted: a
    truncated or unrelated image must not satisfy a longer expected ID.  A
    ``-dirty`` marker is part of the identity and must agree on both sides.
    """
    parsed_actual = _parse_build_id(actual)
    parsed_expected = _parse_build_id(expected)
    if parsed_actual is None or parsed_expected is None:
        return False
    actual_sha, actual_dirty = parsed_actual
    expected_sha, expected_dirty = parsed_expected
    return actual_dirty == expected_dirty and actual_sha.startswith(expected_sha)


def _is_utc_build_time(value: object) -> bool:
    """Accept an explicit ISO-8601 UTC timestamp, never a placeholder."""
    if not isinstance(value, str):
        return False
    text = value.strip()
    if "T" not in text or not (text.endswith("Z") or text.endswith("+00:00")):
        return False
    normalized = f"{text[:-1]}+00:00" if text.endswith("Z") else text
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return False
    return parsed.tzinfo is not None and parsed.utcoffset() == timedelta(0)


def _check_quark_helper_record(
    helper: Mapping[str, object],
    issues: list[str],
) -> None:
    """Require a live, authenticated helper proof rather than lane metadata.

    ``provider_capabilities`` intentionally remains a backwards-compatible
    declaration of the materializer contract.  Its static ``status=ready``
    must never be interpreted as evidence that the host-side Quark Helper is
    available for a real pilot.
    """
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


def _check_quark_helper_readiness(
    health: Mapping[str, object],
    issues: list[str],
) -> None:
    helpers = health.get("helper_readiness")
    if not isinstance(helpers, Mapping):
        issues.append("health.helper_readiness must be an object")
        return
    helper = helpers.get(QUARK_HELPER_NAME)
    if not isinstance(helper, Mapping):
        issues.append("health.helper_readiness.quark must be an object")
        return
    _check_quark_helper_record(helper, issues)


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
    if _parse_build_id(health.get("build_commit")) is None:
        issues.append("health.build_commit must be a recorded lowercase Git SHA build ID")
    elif not _build_id_matches(health.get("build_commit"), expected_commit):
        issues.append("health.build_commit does not match expected commit")
    if not _is_utc_build_time(health.get("build_time")):
        issues.append("health.build_time must be a recorded ISO-8601 UTC timestamp")

    lanes = health.get("provider_capabilities")
    if not isinstance(lanes, Mapping):
        issues.append("health.provider_capabilities must be an object")
    else:
        actual_lanes = {key for key in lanes if isinstance(key, str)}
        if actual_lanes != EXPECTED_PROVIDER_LANES:
            issues.append("health.provider_capabilities must expose exactly the fixed lanes")
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
    # ``jobs_active`` is the durable open-job count, not a live worker count:
    # a paused, already-used installation can legitimately retain queued
    # RootJobs.  The three worker counters above, the audit flag, and the
    # durable control pause still prove that no effectful operation is in
    # flight.  Keep the strict empty-environment rule unless the operator has
    # explicitly opted into that already-used, paused deployment mode.
    if active_jobs != 0 and not allow_existing_jobs:
        issues.append("operations.jobs_active must be 0")
    if not allow_existing_jobs:
        total_jobs = _int_value(operations.get("jobs_total"))
        if total_jobs != 0:
            issues.append("operations.jobs_total must be 0 before opening acceptance")


def _check_control(control: Mapping[str, object], issues: list[str]) -> None:
    _expect_bool(control, "paused", True, issues)
    _expect_bool(control, "scheduler_paused", True, issues)
    _expect_bool(control, "persistent", True, issues)


def runtime_readiness_evidence_issues(report: Mapping[str, object]) -> list[str]:
    """Validate the irreducible proof fields of a saved readiness report.

    The acceptance-package generator consumes a JSON artifact, which may have
    been created by an older checker or edited manually.  Re-check the image
    identity here so a textual ``通过`` claim cannot turn into acceptance
    evidence without that proof.
    """
    issues: list[str] = []
    expected = report.get("expected_commit")
    if _parse_build_id(expected) is None:
        issues.append("expected_commit must be an explicit lowercase Git SHA build ID")
    health = report.get("health")
    if not isinstance(health, Mapping):
        issues.append("health must be an object")
    else:
        if _parse_build_id(health.get("build_commit")) is None:
            issues.append("health.build_commit must be a recorded lowercase Git SHA build ID")
        elif _parse_build_id(expected) is not None and not _build_id_matches(
            health.get("build_commit"), expected,
        ):
            issues.append("health.build_commit does not match expected commit")
        if not _is_utc_build_time(health.get("build_time")):
            issues.append("health.build_time must be a recorded ISO-8601 UTC timestamp")
        helpers = health.get("helper_readiness")
        helper = helpers.get(QUARK_HELPER_NAME) if isinstance(helpers, Mapping) else None
        if not isinstance(helper, Mapping):
            issues.append("health.helper_readiness.quark must be an explicit readiness object")
        else:
            if helper.get("probe_mode") != "explicit":
                issues.append("helper quark.probe_mode must be explicit")
            if helper.get("fresh") is not True:
                issues.append("helper quark.fresh must be true")
            if helper.get("verified") is not True:
                issues.append("helper quark.verified must be true")
            _check_quark_helper_record(helper, issues)
    return issues


def runtime_readiness_report(
    *,
    api_url: str,
    expected_commit: str | None = None,
    timeout: float = 5.0,
    allow_existing_jobs: bool = False,
    fetch_json: FetchJson = _fetch_json,
) -> dict[str, Any]:
    """Fetch health/control plus an explicit Helper proof for acceptance."""
    issues: list[str] = []
    expected = expected_commit.strip() if isinstance(expected_commit, str) else ""
    if _parse_build_id(expected) is None:
        issues.append("expected_commit must be an explicit lowercase Git SHA build ID")
    if not _is_loopback_api_url(api_url):
        return {
            "status": "失败",
            "api_url": api_url,
            "expected_commit": expected,
            "issues": ["api_url must be an HTTP(S) loopback URL"],
            "health": None,
            "control": None,
            "quark_helper_readiness": None,
        }

    endpoints = {
        "health": _endpoint(api_url, "/api/health"),
        "control": _endpoint(api_url, "/api/control"),
        # Ordinary health intentionally uses a non-blocking cached Helper
        # projection.  Acceptance needs a fresh read-only authentication
        # proof, so it requests the dedicated endpoint exactly once.
        "quark_helper_readiness": _endpoint(
            api_url, "/api/dependencies/quark-helper/readiness",
        ),
    }
    payloads: dict[str, object] = {}
    for name, url in endpoints.items():
        try:
            # The explicit endpoint can wait for one bounded deep Helper
            # authentication probe (20s by default).  The generic CLI's 5s
            # liveness timeout must not race that intentional acceptance
            # action and turn a slow renderer into a transport failure.
            request_timeout = (
                max(
                    timeout,
                    DEFAULT_HELPER_HEALTH_TIMEOUT_SECONDS
                    + DEFAULT_HELPER_HEALTH_WAIT_SLACK_SECONDS
                    + 2.0,
                )
                if name == "quark_helper_readiness" else timeout
            )
            status, payload = fetch_json(url, request_timeout)
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
    helper_probe = payloads.get("quark_helper_readiness")
    if isinstance(health, Mapping) and isinstance(helper_probe, Mapping):
        # The ordinary endpoint must still expose a Helper projection for the
        # console.  It may be not_verified/stale by design, so do not demand
        # a green result here; require only that the explicit proof is not
        # masking a missing public dependency record.
        cached_helpers = health.get("helper_readiness")
        if not isinstance(cached_helpers, Mapping):
            issues.append("health.helper_readiness must be an object")
        elif not isinstance(cached_helpers.get(QUARK_HELPER_NAME), Mapping):
            issues.append("health.helper_readiness.quark must be an object")
        helper = helper_probe.get(QUARK_HELPER_NAME)
        if not isinstance(helper, Mapping):
            issues.append("quark_helper_readiness.quark must be an object")
        else:
            if helper.get("probe_mode") != "explicit":
                issues.append("quark helper readiness must be an explicit probe")
            if helper.get("fresh") is not True:
                issues.append("quark helper readiness must be fresh")
            if helper.get("verified") is not True:
                issues.append("quark helper readiness must be verified")
            _check_quark_helper_record(helper, issues)
            # Persist the acceptance proof under the conventional health
            # shape so downstream evidence consumers do not treat a static
            # lane declaration or an expired dashboard cache as proof.
            helpers = health.get("helper_readiness")
            copied_helpers = dict(helpers) if isinstance(helpers, Mapping) else {}
            copied_helpers[QUARK_HELPER_NAME] = dict(helper)
            health = {**health, "helper_readiness": copied_helpers}
    elif isinstance(health, Mapping):
        # Missing helper proof is a hard acceptance failure.  Continue with
        # ordinary health validation to report all independently actionable
        # issues in one invocation.
        issues.append("quark_helper_readiness response must be a JSON object")

    if isinstance(health, Mapping):
        _check_health(
            health,
            expected_commit=expected,
            allow_existing_jobs=allow_existing_jobs,
            issues=issues,
        )
    control = payloads.get("control")
    if isinstance(control, Mapping):
        _check_control(control, issues)
    return {
        "status": "通过" if not issues else "失败",
        "api_url": api_url,
        "expected_commit": expected,
        "allow_existing_jobs": allow_existing_jobs,
        "issues": issues,
        "health": health,
        "control": control,
        "quark_helper_readiness": helper_probe,
    }


__all__ = [
    "RuntimeReadinessError",
    "runtime_readiness_evidence_issues",
    "runtime_readiness_report",
]
