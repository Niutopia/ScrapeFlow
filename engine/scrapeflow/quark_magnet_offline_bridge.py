"""Minimal Quark magnet-offline bridge using fixed helper actions."""

from __future__ import annotations

from collections.abc import Callable, Mapping
import json
import os
from pathlib import PurePosixPath
import re
import time
from typing import Any, Protocol
import urllib.error
import urllib.parse
import urllib.request


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


class QuarkMagnetHelper(Protocol):
    def health(self) -> Mapping[str, Any]: ...

    def magnet_submit(self, plan: Mapping[str, Any]) -> Mapping[str, Any]: ...

    def magnet_status(self, task_id: str) -> Mapping[str, Any]: ...


class HttpQuarkHelperClient:
    """HTTP client for the fixed host-side Quark helper actions."""

    def __init__(
        self,
        base_url: str,
        token: str,
        *,
        timeout: float = 90.0,
        opener: Callable[..., Any] = urllib.request.urlopen,
    ) -> None:
        parsed = urllib.parse.urlsplit(base_url.rstrip("/"))
        if parsed.scheme not in {"http", "https"} or not parsed.hostname:
            raise QuarkMagnetBridgeError("Quark helper URL is invalid")
        if parsed.username or parsed.password or parsed.query or parsed.fragment:
            raise QuarkMagnetBridgeError("Quark helper URL must not contain credentials")
        if parsed.scheme == "http" and parsed.hostname not in {
            "127.0.0.1", "localhost", "::1", "host.docker.internal",
        }:
            raise QuarkMagnetBridgeError("plain HTTP Quark helper must be local")
        if not token or len(token) < 24:
            raise QuarkMagnetBridgeError("Quark helper token is missing or too short")
        self.base_url = base_url.rstrip("/")
        self.token = token
        self.timeout = timeout
        self.opener = opener

    @classmethod
    def from_env(cls) -> "HttpQuarkHelperClient":
        return cls(
            os.getenv("SCRAPEFLOW_QUARK_HELPER_URL", "").strip(),
            os.getenv("SCRAPEFLOW_QUARK_HELPER_TOKEN", "").strip(),
        )

    def _request(self, path: str, body: Mapping[str, Any] | None = None) -> Mapping[str, Any]:
        if path not in {"/health", "/v1/magnet-submit", "/v1/magnet-status"}:
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
                value = json.load(response)
        except urllib.error.HTTPError as exc:
            raw = exc.read(16_384)
            in_doubt = False
            message = ""
            try:
                error_value = json.loads(raw.decode("utf-8", "replace"))
                if isinstance(error_value, Mapping):
                    in_doubt = error_value.get("in_doubt") is True
                    message = str(error_value.get("error") or error_value.get("message") or "")
            except (UnicodeError, json.JSONDecodeError):
                pass
            if path == "/v1/magnet-submit" and (
                (exc.code == 409 and in_doubt) or exc.code >= 500
            ):
                raise QuarkMagnetInDoubtError(
                    "Quark magnet submit is in doubt; reconcile the task destination"
                ) from exc
            raise QuarkMagnetBridgeError(
                f"Quark helper HTTP error: status={exc.code}, message={message[:200]!r}"
            ) from exc
        except Exception as exc:
            if path == "/v1/magnet-submit":
                raise QuarkMagnetInDoubtError(
                    "Quark magnet submit response is unknown; reconcile before retrying"
                ) from exc
            raise QuarkMagnetBridgeError(
                f"Quark helper unavailable: {type(exc).__name__}"
            ) from exc
        if not isinstance(value, Mapping):
            if path == "/v1/magnet-submit":
                raise QuarkMagnetInDoubtError(
                    "Quark magnet submit returned an invalid response; reconcile before retrying"
                )
            raise QuarkMagnetBridgeError("Quark helper returned a non-object response")
        return value

    def health(self) -> Mapping[str, Any]:
        return self._request("/health")

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
        "helper_actions": ["health", "magnet-submit", "magnet-status"],
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
        health = self.helper.health()
        if health.get("status") not in {"ok", "ready"}:
            raise QuarkMagnetBridgeError("Quark helper is not ready")
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
    "HttpQuarkHelperClient",
    "QuarkMagnetBridgeError",
    "QuarkMagnetCandidateError",
    "QuarkMagnetDeliveryError",
    "QuarkMagnetInDoubtError",
    "QuarkMagnetOfflineBridge",
    "normalize_quark_magnet_selection",
]
