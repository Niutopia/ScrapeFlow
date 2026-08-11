"""Passive, loopback-only host helper for the existing Quark desktop session.

This is deliberately not a Quark proxy.  Its HTTP surface is the four fixed
actions in ``HELPER_ACTIONS`` and every cloud operation is constructed here
from a reviewed task payload.  The helper never accepts cookies, arbitrary
URLs, arbitrary Quark paths, or DevTools commands from its HTTP caller.

The helper only attaches to an *explicitly configured* existing loopback CDP
endpoint.  It has no desktop-process or window-management capability and no
DOM-click capability.  A missing renderer or unauthenticated Quark session
therefore makes the helper not ready rather than attempting recovery.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
import asyncio
import hmac
import ipaddress
import json
import os
from pathlib import Path, PurePosixPath
import posixpath
import re
from typing import Any, Protocol
import urllib.parse

try:  # Host-only dependency; see requirements.quark-helper.txt.
    from aiohttp import ClientSession, ClientTimeout, WSMsgType, web
except ImportError as exc:  # pragma: no cover - exercised by the CLI install path.
    raise RuntimeError(
        "ScrapeFlow Quark Helper requires aiohttp; install "
        "requirements.quark-helper.txt in the host Python environment"
    ) from exc


HELPER_ACTIONS = ("health", "share-save", "magnet-submit", "magnet-status")
DEFAULT_STAGING_ROOT = "/quark/影视/ScrapeFlow/补源"
DEFAULT_MOUNT_PATH = "/quark"
MAX_BODY_BYTES = 256 * 1024
MAX_EXPECTED_FILES = 128
MAX_TEXT = 512
MAX_MAGNET_BYTES = 32 * 1024
MAX_RENDERER_RESPONSE_BYTES = 1024 * 1024
QUARK_DRIVE_API = "https://drive.quark.cn/1/clouddrive"
QUARK_SHARE_API = "https://drive-pc.quark.cn/1/clouddrive"
QUARK_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "KHTML, like Gecko) Chrome/120.0 Safari/537.36"
)
_FORBIDDEN_CDP_PORT = 9125
_SEGMENT_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
_IDENTIFIER_RE = re.compile(r"^[A-Za-z0-9._:-]{1,256}$")
_INFOHASH_RE = re.compile(r"^(?:[0-9a-f]{40}|[a-z2-7]{32})$")
_FORMAL_LIBRARY_COMPONENTS = frozenset({
    "电影", "番剧", "美剧", "待刮削", "movie", "movies", "anime", "tv",
    "library", "media",
})


class QuarkHelperError(RuntimeError):
    """A deliberately non-sensitive helper failure."""


class QuarkHelperNotReady(QuarkHelperError):
    """There is no already-running, authenticated Quark renderer to attach."""


class QuarkHelperValidationError(QuarkHelperError):
    """The fixed helper contract was not satisfied."""


class QuarkHelperInDoubt(QuarkHelperError):
    """A mutating renderer request crossed an ambiguous response boundary."""


class QuarkHelperLostResponse(QuarkHelperError):
    """The renderer transport ended after a request may have been sent."""


class QuarkHelperRemoteRejected(QuarkHelperError):
    """Quark answered a fixed operation but rejected its reviewed payload."""


class QuarkSessionPort(Protocol):
    """The only private capability the HTTP service needs from a renderer."""

    async def assert_authenticated(self) -> None: ...

    async def share_save(self, payload: Mapping[str, object]) -> Mapping[str, object]: ...

    async def magnet_submit(self, payload: Mapping[str, object]) -> Mapping[str, object]: ...

    async def magnet_status(self, task_id: str) -> Mapping[str, object]: ...


def _is_loopback_host(value: object) -> bool:
    if not isinstance(value, str) or not value:
        return False
    if value.casefold() == "localhost":
        return True
    try:
        return ipaddress.ip_address(value).is_loopback
    except ValueError:
        return False


def _require_loopback_bind(host: object) -> str:
    if not isinstance(host, str) or host not in {"127.0.0.1", "::1"}:
        raise QuarkHelperValidationError("helper may bind only to 127.0.0.1 or ::1")
    return host


def _safe_absolute_cloud_path(value: object, *, label: str) -> str:
    if not isinstance(value, str) or not value or "\\" in value or not value.startswith("/"):
        raise QuarkHelperValidationError(f"{label} must be an absolute POSIX path")
    normalized = posixpath.normpath(value)
    if normalized != value or normalized == "/" or any(
        part in {"", ".", ".."} for part in value[1:].split("/")
    ):
        raise QuarkHelperValidationError(f"{label} is unsafe")
    return normalized


def _safe_relative_path(value: object, *, label: str) -> str:
    if not isinstance(value, str) or not value or value.startswith("/") or "\\" in value:
        raise QuarkHelperValidationError(f"{label} must be a safe relative path")
    normalized = str(PurePosixPath(value))
    if normalized != value or any(part in {"", ".", ".."} for part in value.split("/")):
        raise QuarkHelperValidationError(f"{label} is unsafe")
    return normalized


def _safe_identifier(value: object, *, label: str) -> str:
    if not isinstance(value, str) or not _IDENTIFIER_RE.fullmatch(value):
        raise QuarkHelperValidationError(f"{label} is invalid")
    return value


def _safe_text(value: object, *, label: str, maximum: int = MAX_TEXT) -> str:
    if not isinstance(value, str) or not value or len(value) > maximum or "\x00" in value:
        raise QuarkHelperValidationError(f"{label} is invalid")
    return value


def validate_staging_root(value: object) -> str:
    """Accept only the frozen ScrapeFlow provider staging root.

    The CLI/environment cannot turn this Helper into a writer for another
    cloud root.  A task request may only select a root-job/attempt child below
    this canonical provider staging prefix.
    """

    root = _safe_absolute_cloud_path(value, label="staging root")
    if root != DEFAULT_STAGING_ROOT:
        raise QuarkHelperValidationError("staging root must equal the frozen provider staging root")
    return root


def _validate_attempt_destination(
    value: object,
    *,
    staging_root: str,
    attempt_id: object | None = None,
) -> tuple[str, str]:
    destination = _safe_absolute_cloud_path(value, label="destination")
    prefix = staging_root + "/"
    if not destination.startswith(prefix):
        raise QuarkHelperValidationError("destination is outside the task staging root")
    tail = destination[len(prefix):].split("/")
    if len(tail) != 2 or any(_SEGMENT_RE.fullmatch(part) is None for part in tail):
        raise QuarkHelperValidationError(
            "destination must be one root-job and one attempt below staging root"
        )
    actual_attempt_id = tail[1]
    if attempt_id is not None and _safe_identifier(attempt_id, label="attempt_id") != actual_attempt_id:
        raise QuarkHelperValidationError("attempt_id does not match destination")
    return destination, actual_attempt_id


def _reject_unknown_keys(value: Mapping[str, object], *, allowed: frozenset[str]) -> None:
    if set(value) - allowed:
        raise QuarkHelperValidationError("request contains unsupported fields")


def _selected_gaps(value: object) -> list[str]:
    if not isinstance(value, list) or not value or len(value) > MAX_EXPECTED_FILES:
        raise QuarkHelperValidationError("selected_gap_ids is invalid")
    gaps = [_safe_identifier(item, label="selected gap") for item in value]
    if len(gaps) != len(set(gaps)):
        raise QuarkHelperValidationError("selected_gap_ids contains duplicates")
    return gaps


def _validate_expected_files(
    value: object,
    *,
    selected_gap_ids: Sequence[str],
    share: bool,
) -> list[dict[str, object]]:
    if not isinstance(value, list) or not value or len(value) > MAX_EXPECTED_FILES:
        raise QuarkHelperValidationError("expected_files is invalid")
    selected = set(selected_gap_ids)
    covered: set[str] = set()
    paths: set[str] = set()
    share_names: set[str] = set()
    output: list[dict[str, object]] = []
    for raw in value:
        if not isinstance(raw, Mapping):
            raise QuarkHelperValidationError("expected file is invalid")
        allowed = (
            frozenset({"file_id", "path", "name", "size", "gap_ids"})
            if share else frozenset({"path", "size", "gap_ids", "torrent_index"})
        )
        _reject_unknown_keys(raw, allowed=allowed)
        path = _safe_relative_path(raw.get("path"), label="expected file path")
        if path in paths:
            raise QuarkHelperValidationError("expected_files contains duplicate paths")
        paths.add(path)
        size = raw.get("size")
        if type(size) is not int or size <= 0:
            raise QuarkHelperValidationError("expected file size is invalid")
        raw_gaps = raw.get("gap_ids")
        if not isinstance(raw_gaps, list) or not raw_gaps:
            raise QuarkHelperValidationError("expected file gap_ids is invalid")
        gaps = [_safe_identifier(item, label="expected file gap") for item in raw_gaps]
        if len(gaps) != len(set(gaps)) or not set(gaps) <= selected:
            raise QuarkHelperValidationError("expected file gaps are invalid")
        covered.update(gaps)
        normalized: dict[str, object] = {"path": path, "size": size, "gap_ids": gaps}
        if share:
            file_id = _safe_identifier(raw.get("file_id"), label="share file_id")
            name = _safe_text(raw.get("name"), label="share file name", maximum=255)
            if name != posixpath.basename(path) or "/" in name or "\\" in name:
                raise QuarkHelperValidationError("share file name does not match path")
            if name in share_names:
                raise QuarkHelperValidationError("share files collide at the task staging root")
            share_names.add(name)
            normalized.update({"file_id": file_id, "name": name})
        elif raw.get("torrent_index") is not None:
            index = raw.get("torrent_index")
            if type(index) is not int or index <= 0:
                raise QuarkHelperValidationError("torrent index is invalid")
            normalized["torrent_index"] = index
        output.append(normalized)
    if covered != selected:
        raise QuarkHelperValidationError("expected_files does not cover selected gaps")
    return output


def validate_share_save_payload(
    value: Mapping[str, object], *, staging_root: str,
) -> dict[str, object]:
    """Validate the fixed share-save request; no session material is accepted."""

    _reject_unknown_keys(value, allowed=frozenset({
        "attempt_id", "destination", "share_id", "passcode", "selected_gap_ids",
        "expected_files", "title", "task_id",
    }))
    destination, attempt_id = _validate_attempt_destination(
        value.get("destination"), staging_root=staging_root, attempt_id=value.get("attempt_id"),
    )
    share_id = _safe_identifier(value.get("share_id"), label="share_id")
    passcode = value.get("passcode")
    if not isinstance(passcode, str) or len(passcode) > 32 or "\x00" in passcode:
        raise QuarkHelperValidationError("passcode is invalid")
    selected = _selected_gaps(value.get("selected_gap_ids"))
    files = _validate_expected_files(value.get("expected_files"), selected_gap_ids=selected, share=True)
    output: dict[str, object] = {
        "attempt_id": attempt_id,
        "destination": destination,
        "share_id": share_id,
        "passcode": passcode,
        "selected_gap_ids": selected,
        "expected_files": files,
    }
    if value.get("title") is not None:
        output["title"] = _safe_text(value.get("title"), label="title")
    if value.get("task_id") is not None:
        output["task_id"] = _safe_identifier(value.get("task_id"), label="task_id")
    return output


def validate_magnet_submit_payload(
    value: Mapping[str, object], *, staging_root: str,
) -> dict[str, object]:
    """Validate one reviewed magnet submission, keeping the URL non-generic."""

    _reject_unknown_keys(value, allowed=frozenset({
        "destination", "magnet_url", "infohash", "selected_gap_ids", "expected_files", "title",
    }))
    destination, _attempt_id = _validate_attempt_destination(
        value.get("destination"), staging_root=staging_root,
    )
    magnet_url = value.get("magnet_url")
    if not isinstance(magnet_url, str) or not magnet_url.startswith("magnet:?") or len(magnet_url.encode()) > MAX_MAGNET_BYTES:
        raise QuarkHelperValidationError("magnet_url is invalid")
    query = urllib.parse.parse_qs(urllib.parse.urlsplit(magnet_url).query, keep_blank_values=False)
    xt = query.get("xt", [""])[0]
    if not isinstance(xt, str) or not xt.casefold().startswith("urn:btih:"):
        raise QuarkHelperValidationError("magnet_url lacks a BTIH")
    infohash = _safe_text(value.get("infohash"), label="infohash", maximum=64).casefold()
    if not _INFOHASH_RE.fullmatch(infohash) or xt.split(":", 2)[-1].casefold() != infohash:
        raise QuarkHelperValidationError("infohash does not match magnet_url")
    selected = _selected_gaps(value.get("selected_gap_ids"))
    files = _validate_expected_files(value.get("expected_files"), selected_gap_ids=selected, share=False)
    output: dict[str, object] = {
        "destination": destination,
        "magnet_url": magnet_url,
        "infohash": infohash,
        "selected_gap_ids": selected,
        "expected_files": files,
    }
    if value.get("title") is not None:
        output["title"] = _safe_text(value.get("title"), label="title")
    return output


def _safe_task_id(value: object) -> str:
    return _safe_identifier(value, label="task_id")


def _cdp_discovery_url(value: object) -> str:
    if not isinstance(value, str) or not value:
        raise QuarkHelperValidationError("an explicit CDP discovery URL is required")
    parsed = urllib.parse.urlsplit(value)
    if (
        parsed.scheme != "http"
        or not _is_loopback_host(parsed.hostname)
        or parsed.port is None
        or parsed.port == _FORBIDDEN_CDP_PORT
        or parsed.username
        or parsed.password
        or parsed.query
        or parsed.fragment
        or parsed.path.rstrip("/") not in {"/json", "/json/list"}
    ):
        raise QuarkHelperValidationError(
            "CDP discovery URL must be an explicit loopback /json/list endpoint"
        )
    return value.rstrip("/")


def _cdp_websocket_url(value: object) -> str:
    if not isinstance(value, str) or not value:
        raise QuarkHelperNotReady("renderer did not expose a DevTools socket")
    parsed = urllib.parse.urlsplit(value)
    if (
        parsed.scheme != "ws"
        or not _is_loopback_host(parsed.hostname)
        or parsed.port is None
        or parsed.port == _FORBIDDEN_CDP_PORT
        or parsed.username
        or parsed.password
        or not parsed.path.startswith("/devtools/")
    ):
        raise QuarkHelperNotReady("renderer DevTools socket is not an approved loopback target")
    return value


@dataclass(frozen=True)
class QuarkHelperConfig:
    host: str
    port: int
    token: str
    cdp_url: str
    staging_root: str = DEFAULT_STAGING_ROOT
    mount_path: str = DEFAULT_MOUNT_PATH
    root_fid: str = "0"
    timeout_seconds: float = 20.0

    def __post_init__(self) -> None:
        _require_loopback_bind(self.host)
        if type(self.port) is not int or not (1 <= self.port <= 65535):
            raise QuarkHelperValidationError("helper port is invalid")
        if not isinstance(self.token, str) or len(self.token) < 24:
            raise QuarkHelperValidationError("helper bearer token is too short")
        _cdp_discovery_url(self.cdp_url)
        validate_staging_root(self.staging_root)
        _safe_absolute_cloud_path(self.mount_path, label="Quark mount path")
        _safe_identifier(self.root_fid, label="Quark root_fid")
        if type(self.timeout_seconds) not in {float, int} or not (1 <= float(self.timeout_seconds) <= 120):
            raise QuarkHelperValidationError("helper timeout is invalid")


class PassiveQuarkCdp:
    """Attach to an existing Quark renderer and make only fixed cloud calls."""

    def __init__(
        self,
        *,
        cdp_url: str,
        staging_root: str,
        mount_path: str,
        root_fid: str,
        timeout_seconds: float = 20.0,
    ) -> None:
        self.cdp_url = _cdp_discovery_url(cdp_url)
        self.staging_root = validate_staging_root(staging_root)
        self.mount_path = _safe_absolute_cloud_path(mount_path, label="Quark mount path")
        self.root_fid = _safe_identifier(root_fid, label="Quark root_fid")
        self.timeout_seconds = float(timeout_seconds)
        if not (
            self.staging_root == self.mount_path
            or self.staging_root.startswith(self.mount_path.rstrip("/") + "/")
        ):
            raise QuarkHelperValidationError("staging root is outside the configured Quark mount")
        self._sequence = 0
        self._lock = asyncio.Lock()

    async def _select_renderer_socket(self) -> str:
        timeout = ClientTimeout(total=self.timeout_seconds)
        try:
            async with ClientSession(timeout=timeout, trust_env=False) as session:
                async with session.get(
                    self.cdp_url,
                    headers={"Accept": "application/json"},
                    allow_redirects=False,
                ) as response:
                    if response.status in {301, 302, 303, 307, 308}:
                        raise QuarkHelperNotReady("CDP discovery redirect refused")
                    if str(response.url).rstrip("/") != self.cdp_url:
                        raise QuarkHelperNotReady("CDP discovery response URL changed")
                    if response.status != 200:
                        raise QuarkHelperNotReady("explicit CDP discovery did not answer")
                    rows = await response.json(content_type=None)
        except QuarkHelperError:
            raise
        except Exception as exc:
            raise QuarkHelperNotReady("explicit CDP discovery is unavailable") from exc
        if not isinstance(rows, list):
            raise QuarkHelperNotReady("CDP target list is invalid")
        candidates: list[tuple[tuple[int, int, int], str]] = []
        for row in rows:
            if not isinstance(row, Mapping) or row.get("type") != "page":
                continue
            target_url = str(row.get("url") or "")
            title = str(row.get("title") or "")
            marker = (target_url + " " + title).casefold()
            if "quark" not in marker and "ucpro" not in marker:
                continue
            try:
                socket_url = _cdp_websocket_url(row.get("webSocketDebuggerUrl"))
            except QuarkHelperNotReady:
                continue
            score = (
                0 if "clouddrive/renderer" in marker else 1,
                0 if "name=main" in marker or "main" in title.casefold() else 1,
                1 if "vip" in marker else 0,
            )
            candidates.append((score, socket_url))
        if not candidates:
            raise QuarkHelperNotReady("explicit CDP endpoint has no Quark renderer")
        candidates.sort(key=lambda item: item[0])
        return candidates[0][1]

    async def _evaluate_json(self, expression: str) -> Mapping[str, object]:
        socket_url = await self._select_renderer_socket()
        timeout = ClientTimeout(total=self.timeout_seconds)
        async with self._lock:
            self._sequence += 1
            request_id = self._sequence
            request_sent = False
            try:
                async with ClientSession(timeout=timeout, trust_env=False) as session:
                    async with session.ws_connect(socket_url, autoping=True) as socket:
                        await socket.send_json({
                            "id": request_id,
                            "method": "Runtime.evaluate",
                            "params": {
                                "expression": expression,
                                "returnByValue": True,
                                "awaitPromise": True,
                                "userGesture": False,
                            },
                        })
                        request_sent = True
                        while True:
                            message = await socket.receive(timeout=self.timeout_seconds)
                            if message.type is WSMsgType.TEXT:
                                raw = json.loads(message.data)
                                if not isinstance(raw, Mapping) or raw.get("id") != request_id:
                                    continue
                                if raw.get("error"):
                                    raise QuarkHelperNotReady("Quark renderer rejected the fixed request")
                                result = raw.get("result")
                                remote = result.get("result") if isinstance(result, Mapping) else None
                                if not isinstance(remote, Mapping) or remote.get("subtype") == "error":
                                    raise QuarkHelperNotReady("Quark renderer did not return a value")
                                encoded = remote.get("value")
                                if not isinstance(encoded, str) or len(encoded.encode("utf-8")) > MAX_RENDERER_RESPONSE_BYTES:
                                    raise QuarkHelperNotReady("Quark renderer response is invalid")
                                parsed = json.loads(encoded)
                                if not isinstance(parsed, Mapping):
                                    raise QuarkHelperNotReady("Quark renderer response is invalid")
                                return dict(parsed)
                            if message.type in {WSMsgType.CLOSE, WSMsgType.CLOSED, WSMsgType.ERROR}:
                                raise QuarkHelperLostResponse("Quark renderer response was lost")
            except QuarkHelperError:
                raise
            except Exception as exc:
                if request_sent:
                    raise QuarkHelperLostResponse("Quark renderer response was lost") from exc
                raise QuarkHelperNotReady("Quark renderer is unavailable") from exc

    @staticmethod
    def _wsg_capability_expression() -> str:
        """Read renderer capability flags without network or desktop effects."""

        return """(() => JSON.stringify({
  kind: "wsg-capabilities",
  encrypt: !!(globalThis.quantum && globalThis.quantum.wsg && typeof globalThis.quantum.wsg.encrypt === "function"),
  decrypt: !!((globalThis.quantum && globalThis.quantum.wsg && typeof globalThis.quantum.wsg.decrypt === "function") || (globalThis.chrome && globalThis.chrome.quarkBizPrivate && typeof globalThis.chrome.quarkBizPrivate.encryptOrDecrypt === "function") )
}))()"""

    async def _probe_wsg_capabilities(self) -> None:
        result = await self._evaluate_json(self._wsg_capability_expression())
        if (
            result.get("kind") != "wsg-capabilities"
            or result.get("encrypt") is not True
            or result.get("decrypt") is not True
        ):
            raise QuarkHelperNotReady("Quark renderer lacks the required WSG capabilities")

    @staticmethod
    def _fixed_fetch_expression(
        *,
        origin: str,
        path: str,
        method: str,
        query: Mapping[str, object] | None = None,
        body: Mapping[str, object] | None = None,
    ) -> str:
        """Build a renderer expression for one internal, preselected API call.

        ``origin`` and ``path`` are module constants at every call site.  No
        request caller can cause an arbitrary origin/path/cookie to reach this
        expression.  The renderer uses its existing authenticated session;
        neither cookies nor raw Quark answers leave this narrow boundary.
        """

        if origin not in {QUARK_DRIVE_API, QUARK_SHARE_API}:
            raise QuarkHelperValidationError("internal Quark origin is invalid")
        allowed_paths = {
            "/file/sort", "/share/sharepage/token", "/share/sharepage/save",
            "/share/sharepage/detail",
            "/task", "/offline/download/parse", "/offline/download/submit",
            "/offline/save_to/progress",
        }
        if path not in allowed_paths or method not in {"GET", "POST"}:
            raise QuarkHelperValidationError("internal Quark operation is invalid")
        payload = json.dumps({
            "url": origin + path,
            "method": method,
            "query": {"pr": "ucpro", "fr": "pc", **dict(query or {})},
            "body": dict(body) if body is not None else None,
        }, ensure_ascii=False, separators=(",", ":"))
        return """(() => {
const input = %s;
const query = new URLSearchParams(Object.entries(input.query).map(([key, value]) => [key, String(value)])).toString();
const url = input.url + (query ? "?" + query : "");
const headers = {"Accept": "application/json, text/plain, */*", "User-Agent": %s};
const decode = (cipher) => new Promise((resolve, reject) => {
  const bridge = globalThis.chrome && globalThis.chrome.quarkBizPrivate;
  if (bridge && typeof bridge.encryptOrDecrypt === "function") {
    bridge.encryptOrDecrypt({encrypt: false, data: cipher, wsgNum: 13801}, value => resolve(typeof value === "string" ? value : (value && value.data)));
    return;
  }
  const wsg = globalThis.quantum && globalThis.quantum.wsg;
  try { const result = wsg && wsg.decrypt({number: 13801, cipher_b64: cipher}); resolve(result && (result.plain || result.plain_text || result.text)); } catch (error) { reject(error); }
});
const run = async () => {
  let requestBody = undefined;
  if (input.body !== null) {
    const wsg = globalThis.quantum && globalThis.quantum.wsg;
    if (!wsg || typeof wsg.encrypt !== "function") return JSON.stringify({kind: "renderer_missing_wsg"});
    const encrypted = wsg.encrypt({number: 13801, plain: JSON.stringify(input.body)});
    const cipher = encrypted && (encrypted.cipher_b64 || encrypted.cipherB64 || encrypted.cipher);
    if (typeof cipher !== "string" || !cipher) return JSON.stringify({kind: "renderer_missing_wsg"});
    requestBody = cipher;
    headers["Content-Type"] = "application/json";
    headers["X-U-Content-Encoding"] = "wg";
  }
  const response = await fetch(url, {method: input.method, headers, body: requestBody, credentials: "include", cache: "no-store", redirect: "error"});
  let text = await response.text();
  if (text.length > %d) return JSON.stringify({kind: "response_too_large"});
  if ((response.headers.get("X-U-Content-Encoding") || "").toLowerCase() === "wg") text = await decode(text);
  return JSON.stringify({kind: "response", status: response.status, text: typeof text === "string" ? text : ""});
};
return run().catch(() => JSON.stringify({kind: "transport_error"}));
})()""" % (payload, json.dumps(QUARK_UA), MAX_RENDERER_RESPONSE_BYTES)

    async def _call_fixed(
        self,
        *,
        origin: str,
        path: str,
        method: str,
        query: Mapping[str, object] | None = None,
        body: Mapping[str, object] | None = None,
    ) -> Mapping[str, object]:
        result = await self._evaluate_json(self._fixed_fetch_expression(
            origin=origin, path=path, method=method, query=query, body=body,
        ))
        if result.get("kind") == "transport_error":
            raise QuarkHelperLostResponse("Quark renderer response was lost")
        if result.get("kind") != "response" or type(result.get("status")) is not int:
            raise QuarkHelperNotReady("Quark session cannot complete a fixed request")
        status = int(result["status"])
        text = result.get("text")
        if status < 200 or status >= 300 or not isinstance(text, str):
            if status in {401, 403, 408, 425, 429} or status >= 500:
                raise QuarkHelperNotReady("Quark session or service is unavailable")
            raise QuarkHelperRemoteRejected("Quark rejected the fixed operation")
        try:
            payload = json.loads(text)
        except (TypeError, json.JSONDecodeError) as exc:
            raise QuarkHelperNotReady("Quark returned an invalid fixed response") from exc
        if not isinstance(payload, Mapping):
            raise QuarkHelperNotReady("Quark returned an invalid fixed response")
        raw_code = payload.get("code")
        raw_status = payload.get("status")
        try:
            numeric_code = int(raw_code) if raw_code is not None else 0
        except (TypeError, ValueError):
            numeric_code = 0
        try:
            numeric_status = int(raw_status) if raw_status is not None else 200
        except (TypeError, ValueError):
            numeric_status = 200
        # Quark business codes such as 41004 are not HTTP status codes.  Do
        # not treat every large numeric code as an outage: only a real 5xx
        # code, auth, timeout, or rate-limit response is infrastructure.
        if numeric_code in {401, 403, 408, 425, 429} or 500 <= numeric_code <= 599:
            raise QuarkHelperNotReady("Quark session or service is unavailable")
        if numeric_status in {401, 403, 408, 425, 429} or numeric_status >= 500:
            raise QuarkHelperNotReady("Quark session or service is unavailable")
        if raw_code not in {None, 0, "0"} or raw_status not in {None, 200, "200"}:
            raise QuarkHelperRemoteRejected("Quark rejected the fixed operation")
        return dict(payload)

    async def _destination_fid(self, destination: str) -> str:
        relative = destination[len(self.mount_path):].strip("/")
        parent = self.root_fid
        for component in relative.split("/"):
            listing = await self._call_fixed(
                origin=QUARK_DRIVE_API,
                path="/file/sort",
                method="GET",
                query={
                    "pdir_fid": parent, "_page": 1, "_size": 100,
                    "_fetch_total": 1, "fetch_all_file": 1,
                },
            )
            data = listing.get("data")
            rows = data.get("list") if isinstance(data, Mapping) else None
            matches = [
                row for row in rows if isinstance(row, Mapping)
                and row.get("file_name") == component and row.get("file") is False
                and isinstance(row.get("fid"), str) and row.get("fid")
            ] if isinstance(rows, list) else []
            if len(matches) != 1:
                raise QuarkHelperNotReady("task staging folder is not uniquely available in Quark")
            parent = str(matches[0]["fid"])
        return parent

    async def assert_authenticated(self) -> None:
        # A read-only root listing proves both a renderer and its current Quark
        # session.  No credentials are read from the renderer or returned.
        await self._probe_wsg_capabilities()
        await self._call_fixed(
            origin=QUARK_DRIVE_API,
            path="/file/sort",
            method="GET",
            query={"pdir_fid": self.root_fid, "_page": 1, "_size": 1, "_fetch_total": 0},
        )

    async def share_save(self, payload: Mapping[str, object]) -> Mapping[str, object]:
        existing = payload.get("task_id")
        if isinstance(existing, str) and existing:
            try:
                await self._reconcile_existing_share_task(
                    task_id=existing,
                    destination=str(payload["destination"]),
                    expected_files=payload["expected_files"],
                )
            except QuarkHelperRemoteRejected:
                raise
            except QuarkHelperError as exc:
                raise QuarkHelperInDoubt("prior share task is not reconcilable") from exc
            return {"status": "finished", "task_id": existing}
        try:
            destination = str(payload["destination"])
            target_fid = await self._destination_fid(destination)
            token = await self._call_fixed(
                origin=QUARK_SHARE_API,
                path="/share/sharepage/token",
                method="POST",
                body={"pwd_id": payload["share_id"], "passcode": payload["passcode"]},
            )
            token_data = token.get("data")
            stoken = token_data.get("stoken") if isinstance(token_data, Mapping) else None
            if not isinstance(stoken, str) or not stoken:
                raise QuarkHelperRemoteRejected("Quark share token was rejected")
            # PanSou's reviewed manifest gives file IDs, but Quark's save API also
            # needs per-file ephemeral tokens.  Resolve those server-side via the
            # current share detail before the mutation rather than accepting any
            # token/cookie from the HTTP caller.
            files = payload["expected_files"]
            if not isinstance(files, list):  # impossible after service validation
                raise QuarkHelperValidationError("share expected files are invalid")
            tokens = await self._share_file_tokens(
                share_id=str(payload["share_id"]), stoken=stoken, expected_files=files,
            )
        except QuarkHelperLostResponse as exc:
            raise QuarkHelperNotReady("Quark became unavailable before share save") from exc
        try:
            result = await self._call_fixed(
                origin=QUARK_DRIVE_API,
                path="/share/sharepage/save",
                method="POST",
                body={
                    "fid_list": [str(row["file_id"]) for row in files if isinstance(row, Mapping)],
                    "fid_token_list": tokens,
                    "to_pdir_fid": target_fid,
                    "pwd_id": payload["share_id"],
                    "stoken": stoken,
                    "pdir_fid": "0",
                    "scene": "link",
                },
            )
        except QuarkHelperRemoteRejected:
            raise
        except QuarkHelperNotReady:
            raise
        except QuarkHelperLostResponse as exc:
            raise QuarkHelperInDoubt("share save outcome is unknown") from exc
        except QuarkHelperError as exc:
            raise QuarkHelperInDoubt("share save outcome is unknown") from exc
        data = result.get("data")
        task_id = data.get("task_id") if isinstance(data, Mapping) else None
        if not isinstance(task_id, str) or not task_id:
            raise QuarkHelperInDoubt("share save response did not contain a task id")
        return {"status": "submitted", "task_id": _safe_task_id(task_id)}

    async def _reconcile_existing_share_task(
        self,
        *,
        task_id: str,
        destination: str,
        expected_files: object,
    ) -> None:
        """Accept a reentry only after task completion and exact staging readback."""

        task = await self._call_fixed(
            origin=QUARK_SHARE_API,
            path="/task",
            method="GET",
            query={"task_id": task_id, "retry_index": 0},
        )
        data = task.get("data")
        state = str(data.get("status") or "") if isinstance(data, Mapping) else ""
        if state not in {"2", "finished", "success", "done", "completed"}:
            raise QuarkHelperInDoubt("prior share task is not complete")
        if not isinstance(expected_files, list):
            raise QuarkHelperValidationError("share expected files are invalid")
        root_fid = await self._destination_fid(destination)
        rows: list[Mapping[str, object]] = []
        for page in range(1, 42):
            listing = await self._call_fixed(
                origin=QUARK_DRIVE_API,
                path="/file/sort",
                method="GET",
                query={
                    "pdir_fid": root_fid, "_page": page, "_size": 100,
                    "_fetch_total": 1, "fetch_all_file": 1,
                },
            )
            data = listing.get("data")
            page_rows = data.get("list") if isinstance(data, Mapping) else None
            if not isinstance(page_rows, list):
                raise QuarkHelperNotReady("Quark staging listing is invalid")
            rows.extend(row for row in page_rows if isinstance(row, Mapping))
            if len(rows) > 4096:
                raise QuarkHelperNotReady("Quark staging listing is too large")
            if len(page_rows) < 100:
                break
        else:
            raise QuarkHelperNotReady("Quark staging listing exceeded the fixed limit")
        for expected in expected_files:
            if not isinstance(expected, Mapping):
                raise QuarkHelperValidationError("share expected file is invalid")
            matches = [
                row for row in rows
                if row.get("file_name") == expected["name"]
                and row.get("file") is not False
                and row.get("size") == expected["size"]
            ]
            if len(matches) != 1:
                raise QuarkHelperInDoubt("prior share task staging differs from its manifest")

    async def _share_file_tokens(
        self,
        *,
        share_id: str,
        stoken: str,
        expected_files: Sequence[object],
    ) -> list[str]:
        # Resolve only the reviewed path/ID/size rows.  The server-generated
        # ``share_fid_token`` never crosses the Helper HTTP boundary.
        directory_cache: dict[str, list[Mapping[str, object]]] = {}
        traversed_directories = 0
        traversed_entries = 0

        async def list_directory(parent: str) -> list[Mapping[str, object]]:
            nonlocal traversed_directories, traversed_entries
            cached = directory_cache.get(parent)
            if cached is not None:
                return cached
            traversed_directories += 1
            if traversed_directories > 256:
                raise QuarkHelperRemoteRejected("Quark share directory traversal is too large")
            rows: list[Mapping[str, object]] = []
            for page in range(1, 42):
                detail = await self._call_fixed(
                    origin=QUARK_SHARE_API,
                    path="/share/sharepage/detail",
                    method="GET",
                    query={
                        "pwd_id": share_id, "stoken": stoken, "pdir_fid": parent,
                        "force": 0, "_page": page, "_size": 100, "_fetch_total": 1,
                    },
                )
                data = detail.get("data")
                page_rows = data.get("list") if isinstance(data, Mapping) else None
                if not isinstance(page_rows, list):
                    raise QuarkHelperRemoteRejected("Quark share detail is invalid")
                rows.extend(row for row in page_rows if isinstance(row, Mapping))
                traversed_entries += len(page_rows)
                if traversed_entries > 4096:
                    raise QuarkHelperRemoteRejected("Quark share traversal is too large")
                if len(page_rows) < 100:
                    break
            else:
                raise QuarkHelperRemoteRejected("Quark share pagination exceeded the fixed limit")
            directory_cache[parent] = rows
            return rows

        tokens: list[str] = []
        for raw_expected in expected_files:
            if not isinstance(raw_expected, Mapping):  # impossible after validation
                raise QuarkHelperValidationError("share expected file is invalid")
            path = str(raw_expected["path"])
            parent = "0"
            leaf: Mapping[str, object] | None = None
            parts = path.split("/")
            for index, component in enumerate(parts):
                matches = [
                    row for row in await list_directory(parent)
                    if row.get("file_name") == component
                ]
                if len(matches) != 1:
                    raise QuarkHelperRemoteRejected("reviewed Quark share path is unavailable")
                row = matches[0]
                fid = row.get("fid")
                if not isinstance(fid, str) or not fid:
                    raise QuarkHelperRemoteRejected("Quark share file id is invalid")
                if index < len(parts) - 1:
                    if row.get("file") is not False:
                        raise QuarkHelperRemoteRejected("Quark share path crosses a file")
                    parent = fid
                    continue
                leaf = row
            if leaf is None or leaf.get("file") is False:
                raise QuarkHelperRemoteRejected("reviewed Quark share file is unavailable")
            if (
                leaf.get("fid") != raw_expected["file_id"]
                or leaf.get("file_name") != raw_expected["name"]
                or leaf.get("size") != raw_expected["size"]
            ):
                raise QuarkHelperRemoteRejected("Quark share differs from the reviewed manifest")
            token = leaf.get("share_fid_token") or leaf.get("fid_token")
            if not isinstance(token, str) or not token:
                raise QuarkHelperRemoteRejected("Quark share file token is unavailable")
            tokens.append(token)
        return tokens

    async def magnet_submit(self, payload: Mapping[str, object]) -> Mapping[str, object]:
        try:
            destination = str(payload["destination"])
            target_fid = await self._destination_fid(destination)
            parsed = await self._call_fixed(
                origin=QUARK_DRIVE_API,
                path="/offline/download/parse",
                method="POST",
                body={"url": payload["magnet_url"]},
            )
            parsed_data = parsed.get("data")
            if not isinstance(parsed_data, Mapping):
                raise QuarkHelperRemoteRejected("Quark rejected the magnet")
            selected_files = payload.get("expected_files")
            if not isinstance(selected_files, list):  # impossible after validation
                raise QuarkHelperValidationError("magnet expected files are invalid")
            indexes = []
            for row in selected_files:
                index = row.get("torrent_index") if isinstance(row, Mapping) else None
                if type(index) is not int or index <= 0:
                    raise QuarkHelperValidationError("magnet expected file lacks a precise torrent index")
                indexes.append(index)
            # Quark's offline endpoint uses the parse token and task-facing fields
            # returned by the already-authenticated renderer.  The caller supplies
            # only a reviewed magnet and exact indexes; no API URL/cookie/body is
            # accepted from it.
            submit_body: dict[str, object] = {
                "url": payload["magnet_url"],
                "to_pdir_fid": target_fid,
                "selected_file_index": indexes,
            }
            parse_token = parsed_data.get("token")
            if not isinstance(parse_token, str) or not parse_token:
                raise QuarkHelperNotReady("Quark magnet parse response lacks a submit token")
            submit_body["token"] = parse_token
        except QuarkHelperLostResponse as exc:
            raise QuarkHelperNotReady("Quark became unavailable before magnet submit") from exc
        try:
            result = await self._call_fixed(
                origin=QUARK_DRIVE_API,
                path="/offline/download/submit",
                method="POST",
                body=submit_body,
            )
        except QuarkHelperRemoteRejected:
            raise
        except QuarkHelperNotReady:
            raise
        except QuarkHelperLostResponse as exc:
            raise QuarkHelperInDoubt("magnet submit outcome is unknown") from exc
        except QuarkHelperError as exc:
            raise QuarkHelperInDoubt("magnet submit outcome is unknown") from exc
        data = result.get("data")
        task_id = data.get("task_id") if isinstance(data, Mapping) else None
        if not isinstance(task_id, str) or not task_id:
            raise QuarkHelperInDoubt("magnet submit response did not contain a task id")
        return {"status": "submitted", "task_id": _safe_task_id(task_id)}

    async def magnet_status(self, task_id: str) -> Mapping[str, object]:
        response = await self._call_fixed(
            origin=QUARK_DRIVE_API,
            path="/offline/save_to/progress",
            method="GET",
            query={"task_id": task_id},
        )
        data = response.get("data")
        raw_state = ""
        if isinstance(data, Mapping):
            raw_state = str(data.get("status") or data.get("state") or "").casefold()
        if raw_state in {"2", "success", "finished", "done", "completed"}:
            status = "finished"
        elif raw_state in {"3", "4", "failed", "error", "rejected"}:
            status = "failed"
        else:
            status = "running"
        return {"status": status, "task_id": task_id}


class QuarkHostHelperService:
    """Narrow service facade which owns all request validation."""

    def __init__(self, session: QuarkSessionPort, *, staging_root: str) -> None:
        self.session = session
        self.staging_root = validate_staging_root(staging_root)

    async def health(self) -> dict[str, object]:
        try:
            await self.session.assert_authenticated()
        except (QuarkHelperRemoteRejected, QuarkHelperLostResponse) as exc:
            raise QuarkHelperNotReady("Quark session is not authenticated") from exc
        return {
            "status": "ready",
            "authenticated": True,
            "actions": list(HELPER_ACTIONS),
            "required_actions": list(HELPER_ACTIONS),
            "passive": True,
        }

    async def share_save(self, payload: Mapping[str, object]) -> dict[str, object]:
        normalized = validate_share_save_payload(payload, staging_root=self.staging_root)
        try:
            result = await self.session.share_save(normalized)
        except QuarkHelperInDoubt:
            raise
        except (QuarkHelperNotReady, QuarkHelperRemoteRejected):
            raise
        except Exception as exc:
            raise QuarkHelperInDoubt("share save outcome is unknown") from exc
        return self._mutation_result(result)

    async def magnet_submit(self, payload: Mapping[str, object]) -> dict[str, object]:
        normalized = validate_magnet_submit_payload(payload, staging_root=self.staging_root)
        try:
            result = await self.session.magnet_submit(normalized)
        except QuarkHelperInDoubt:
            raise
        except (QuarkHelperNotReady, QuarkHelperRemoteRejected):
            raise
        except Exception as exc:
            raise QuarkHelperInDoubt("magnet submit outcome is unknown") from exc
        return self._mutation_result(result)

    async def magnet_status(self, task_id: object) -> dict[str, object]:
        result = await self.session.magnet_status(_safe_task_id(task_id))
        if not isinstance(result, Mapping):
            raise QuarkHelperNotReady("Quark task status is invalid")
        status = str(result.get("status") or "").casefold()
        if status not in {"running", "finished", "failed"}:
            raise QuarkHelperNotReady("Quark task status is invalid")
        return {"status": status, "task_id": _safe_task_id(result.get("task_id"))}

    @staticmethod
    def _mutation_result(value: Mapping[str, object]) -> dict[str, object]:
        status = str(value.get("status") or "").casefold()
        task_id = value.get("task_id")
        if status not in {"finished", "submitted", "success", "done", "ready", "completed"}:
            raise QuarkHelperNotReady("Quark mutation result is invalid")
        return {"status": status, "task_id": _safe_task_id(task_id)}


async def _read_json_body(request: web.Request) -> Mapping[str, object]:
    if request.content_length is not None and request.content_length > MAX_BODY_BYTES:
        raise QuarkHelperValidationError("request body is too large")
    try:
        value = await request.json(loads=json.loads)
    except (json.JSONDecodeError, UnicodeDecodeError, ValueError) as exc:
        raise QuarkHelperValidationError("request body must be JSON") from exc
    if not isinstance(value, Mapping):
        raise QuarkHelperValidationError("request body must be an object")
    return dict(value)


def _authorized(request: web.Request, token: str) -> bool:
    supplied = request.headers.get("Authorization", "")
    return hmac.compare_digest(supplied, "Bearer " + token)


def _error(status: int, code: str, *, in_doubt: bool = False) -> web.Response:
    payload: dict[str, object] = {"status": "error", "error": code}
    if in_doubt:
        payload["in_doubt"] = True
    return web.json_response(payload, status=status, headers={"Cache-Control": "no-store"})


def create_quark_helper_app(service: QuarkHostHelperService, *, token: str) -> web.Application:
    """Create the only allowed helper HTTP routes.

    Bearer authentication is required even for health: loopback is a boundary,
    not authorization.  The host process must still bind through
    :class:`QuarkHelperConfig`, which only accepts loopback addresses.
    """

    if not isinstance(token, str) or len(token) < 24:
        raise QuarkHelperValidationError("helper bearer token is too short")
    app = web.Application(client_max_size=MAX_BODY_BYTES)

    async def guarded(request: web.Request, action: str) -> web.Response:
        if not _authorized(request, token):
            return _error(401, "unauthorized")
        try:
            if action == "health":
                return web.json_response(await service.health(), headers={"Cache-Control": "no-store"})
            body = await _read_json_body(request)
            if action == "share-save":
                return web.json_response(await service.share_save(body), headers={"Cache-Control": "no-store"})
            if action == "magnet-submit":
                return web.json_response(await service.magnet_submit(body), headers={"Cache-Control": "no-store"})
            if action == "magnet-status":
                _reject_unknown_keys(body, allowed=frozenset({"task_id"}))
                return web.json_response(
                    await service.magnet_status(body.get("task_id")),
                    headers={"Cache-Control": "no-store"},
                )
        except QuarkHelperValidationError:
            return _error(400, "invalid_request")
        except QuarkHelperInDoubt:
            return _error(409, "submit_in_doubt", in_doubt=True)
        except QuarkHelperRemoteRejected:
            return _error(422, "quark_rejected")
        except QuarkHelperNotReady:
            return _error(503, "quark_not_ready")
        return _error(404, "not_found")

    async def health_handler(request: web.Request) -> web.Response:
        return await guarded(request, "health")

    async def share_save_handler(request: web.Request) -> web.Response:
        return await guarded(request, "share-save")

    async def magnet_submit_handler(request: web.Request) -> web.Response:
        return await guarded(request, "magnet-submit")

    async def magnet_status_handler(request: web.Request) -> web.Response:
        return await guarded(request, "magnet-status")

    async def not_found_handler(_request: web.Request) -> web.Response:
        return _error(404, "not_found")

    app.router.add_get("/health", health_handler, allow_head=False)
    app.router.add_post("/v1/share-save", share_save_handler)
    app.router.add_post("/v1/magnet-submit", magnet_submit_handler)
    app.router.add_post("/v1/magnet-status", magnet_status_handler)
    app.router.add_route("*", "/{tail:.*}", not_found_handler)
    return app


def serve_quark_helper(config: QuarkHelperConfig) -> None:
    """Run the host-only service without process/UI management side effects."""

    session = PassiveQuarkCdp(
        cdp_url=config.cdp_url,
        staging_root=config.staging_root,
        mount_path=config.mount_path,
        root_fid=config.root_fid,
        timeout_seconds=config.timeout_seconds,
    )
    service = QuarkHostHelperService(session, staging_root=config.staging_root)
    web.run_app(
        create_quark_helper_app(service, token=config.token),
        host=config.host,
        port=config.port,
        access_log=None,
        print=None,
    )


def load_helper_token(*, token_file: Path | None = None, environ: Mapping[str, str] | None = None) -> str:
    """Read a Bearer secret from the host environment or a regular 0600 file."""

    values = os.environ if environ is None else environ
    token = str(values.get("SCRAPEFLOW_QUARK_HELPER_TOKEN") or "").strip()
    if not token and token_file is not None:
        try:
            mode = token_file.stat().st_mode & 0o777
            if token_file.is_symlink() or not token_file.is_file() or mode & 0o077:
                raise QuarkHelperValidationError("helper token file must be a regular 0600 file")
            token = token_file.read_text(encoding="utf-8").strip()
        except OSError as exc:
            raise QuarkHelperValidationError("helper token file is unavailable") from exc
    if len(token) < 24:
        raise QuarkHelperValidationError("helper bearer token is too short")
    return token


__all__ = [
    "DEFAULT_MOUNT_PATH",
    "DEFAULT_STAGING_ROOT",
    "HELPER_ACTIONS",
    "PassiveQuarkCdp",
    "QuarkHelperConfig",
    "QuarkHelperError",
    "QuarkHelperInDoubt",
    "QuarkHelperNotReady",
    "QuarkHelperRemoteRejected",
    "QuarkHelperValidationError",
    "QuarkHostHelperService",
    "create_quark_helper_app",
    "load_helper_token",
    "serve_quark_helper",
    "validate_magnet_submit_payload",
    "validate_share_save_payload",
    "validate_staging_root",
]
