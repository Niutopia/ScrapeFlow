#!/usr/bin/env python3
"""HTTP bridge for a deployment's media search/acquisition service.

The remote service keeps provider-specific sessions and implements two JSON
endpoints.  ScrapeFlow owns candidate validation/ranking and sends the selected
opaque locator back only to the acquisition endpoint.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import sys
import time
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit
from urllib.request import Request, urlopen

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from engine.scrapeflow.serialization import atomic_write_json


MAX_RESPONSE_BYTES = 8 * 1024 * 1024


def _endpoint(action: str) -> str:
    names = {
        "search": "SCRAPEFLOW_REPLENISHMENT_SEARCH_URL",
        "acquire": "SCRAPEFLOW_REPLENISHMENT_ACQUIRE_URL",
        "status": "SCRAPEFLOW_REPLENISHMENT_STATUS_URL",
    }
    try:
        name = names[action]
    except KeyError as exc:
        raise ValueError(f"未知适配器动作: {action}") from exc
    value = os.getenv(name, "").strip()
    parsed = urlsplit(value)
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        raise ValueError(f"{name} 需要填写 HTTP(S) JSON 接口")
    if parsed.scheme == "http" and parsed.hostname not in {"127.0.0.1", "localhost"}:
        raise ValueError(f"{name} 的远程接口需要使用 HTTPS")
    return value


def _read_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"输入不是 JSON 对象: {path}")
    return value


def _post_json(url: str, payload: dict[str, Any]) -> dict[str, Any]:
    headers = {"Content-Type": "application/json", "Accept": "application/json"}
    token = os.getenv("SCRAPEFLOW_REPLENISHMENT_TOKEN", "").strip()
    if token:
        headers["Authorization"] = f"Bearer {token}"
    request = Request(
        url,
        data=json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8"),
        headers=headers,
        method="POST",
    )
    try:
        with urlopen(request, timeout=120) as response:
            body = response.read(MAX_RESPONSE_BYTES + 1)
    except HTTPError as exc:
        raise RuntimeError(f"查补服务返回 HTTP {exc.code}") from exc
    except URLError as exc:
        raise RuntimeError(f"查补服务连接失败: {exc.reason}") from exc
    if len(body) > MAX_RESPONSE_BYTES:
        raise ValueError("查补服务响应超过 8 MiB")
    value = json.loads(body.decode("utf-8"))
    if not isinstance(value, dict):
        raise ValueError("查补服务响应不是 JSON 对象")
    return value


def _positive_number(name: str, default: str, minimum: float, maximum: float) -> float:
    raw = os.getenv(name, default).strip()
    try:
        value = float(raw)
    except ValueError as exc:
        raise ValueError(f"{name} 需要是数字") from exc
    if not minimum <= value <= maximum:
        raise ValueError(f"{name} 需要在 {minimum:g}–{maximum:g} 之间")
    return value


def _wait_for_acquisition(result: dict[str, Any]) -> dict[str, Any]:
    """Poll a queued acquisition until the provider confirms local materialization."""
    pending = {"accepted", "pending", "queued", "running", "transferring"}
    failed = {"cancelled", "error", "failed", "rejected"}
    status = str(result.get("status") or "").strip().casefold()
    if status == "ready":
        return result
    if status in failed:
        message = str(result.get("message") or result.get("error") or status).strip()
        raise RuntimeError(f"查补获取任务失败: {message}")
    if status not in pending:
        raise ValueError("获取接口响应的 status 不受支持")

    operation_id = result.get("operation_id") or result.get("task_id")
    if not isinstance(operation_id, str) or not operation_id.strip():
        raise ValueError("排队中的获取任务缺少 operation_id")
    status_url = _endpoint("status")
    timeout = _positive_number(
        "SCRAPEFLOW_REPLENISHMENT_ACQUIRE_TIMEOUT", "21600", 60, 86400,
    )
    interval = _positive_number(
        "SCRAPEFLOW_REPLENISHMENT_POLL_INTERVAL", "10", 1, 300,
    )
    deadline = time.monotonic() + timeout
    poll_payload = {"operation_id": operation_id.strip()}
    while True:
        if time.monotonic() >= deadline:
            raise TimeoutError(f"查补获取任务等待超时: {operation_id.strip()}")
        time.sleep(min(interval, max(0.0, deadline - time.monotonic())))
        result = _post_json(status_url, poll_payload)
        status = str(result.get("status") or "").strip().casefold()
        if status == "ready":
            return result
        if status in failed:
            message = str(result.get("message") or result.get("error") or status).strip()
            raise RuntimeError(f"查补获取任务失败: {message}")
        if status not in pending:
            raise ValueError("状态接口响应的 status 不受支持")


def _atomic_json(path: Path, value: dict[str, Any]) -> None:
    atomic_write_json(path, value, sort_keys=True)


def main() -> int:
    parser = argparse.ArgumentParser(description="ScrapeFlow 查补 HTTP 适配器")
    subparsers = parser.add_subparsers(dest="action", required=True)
    search = subparsers.add_parser("search")
    search.add_argument("--request", type=Path, required=True)
    search.add_argument("--output", type=Path, required=True)
    acquire = subparsers.add_parser("acquire")
    acquire.add_argument("--selection", type=Path, required=True)
    acquire.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()

    input_path = args.request if args.action == "search" else args.selection
    result = _post_json(_endpoint(args.action), _read_json(input_path))
    if args.action == "search" and not isinstance(result.get("candidates"), list):
        raise ValueError("搜索接口响应缺少 candidates 数组")
    if args.action == "acquire":
        result = _wait_for_acquisition(result)
    _atomic_json(args.output, result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
