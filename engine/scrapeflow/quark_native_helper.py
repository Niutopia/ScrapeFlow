"""Passive host-side Quark WSG helper core.

The Quark desktop runtime owns the native WSG implementation.  This module
connects to a renderer exposed on a loopback Chrome DevTools endpoint, uses
only the WSG encrypt/decrypt primitives, and performs the HTTP request itself.
It never starts, restarts, quits, or activates Quark, never clicks UI, and
never persists Quark/AList credentials.  If the existing Quark/CDP runtime is
not ready, every readiness boundary fails closed.
"""

from __future__ import annotations

from dataclasses import dataclass
import base64
import hashlib
import json
import os
from pathlib import Path
import re
import secrets
import socket
import struct
import subprocess
import threading
from typing import Any, Mapping
import urllib.error
import urllib.parse
import urllib.request

from engine.scrapeflow.quark_fast_save_bridge import (
    QUARK_DRIVE_API, QUARK_SHARE_API, QUARK_UA,
)
from engine.scrapeflow.serialization import atomic_write_json


ALLOWED_PATHS = frozenset({
    "/file/sort",
    "/offline/download/parse",
    "/offline/download/submit",
    "/offline/save_to/progress",
})
MUTATING_PATHS = frozenset({"/offline/download/submit"})
WSG_SECURE_NO = 13801
MAX_REQUEST_BYTES = 2 * 1024 * 1024
DEFAULT_QUARK_PROCESS_NAME = "QuarkCloudDrive"
FORBIDDEN_ACTIVE_CONTROL_FIELDS = frozenset({
    "launch_quark",
    "restart_quark",
    "restart_quark_without_cdp",
    "activate_quark",
    "activate_ui",
    "allow_ui_activation",
    "ui_activation",
})


class NativeHelperError(RuntimeError):
    pass


class NativeRuntimeUnavailable(NativeHelperError):
    pass


class NativeRequestInDoubt(NativeHelperError):
    """A prior submit crossed the mutation boundary without a stored reply."""


def canonical_request_id(
    method: str, endpoint: str, params: Mapping[str, Any],
    body: Mapping[str, Any] | None,
) -> str:
    canonical_body = body
    if endpoint.endswith("/offline/download/submit") and isinstance(body, Mapping):
        canonical_body = {key: value for key, value in body.items() if key != "token"}
    payload = json.dumps({
        "method": method, "endpoint": endpoint,
        "params": dict(params), "body": canonical_body,
    }, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def validate_native_request(value: Mapping[str, Any]) -> dict[str, Any]:
    forbidden = sorted(FORBIDDEN_ACTIVE_CONTROL_FIELDS.intersection(value))
    if forbidden:
        raise NativeHelperError(
            "active Quark process/UI control is permanently forbidden"
        )
    method = value.get("method")
    endpoint = value.get("endpoint")
    params = value.get("params")
    body = value.get("body")
    cookie = value.get("cookie")
    request_id = value.get("request_id")
    if method not in {"GET", "POST"} or not isinstance(endpoint, str):
        raise NativeHelperError("invalid method or endpoint")
    parsed = urllib.parse.urlsplit(endpoint)
    base = next(
        (candidate for candidate in (QUARK_SHARE_API, QUARK_DRIVE_API)
         if endpoint.startswith(candidate + "/")),
        None,
    )
    relative_path = endpoint[len(base):] if base is not None else ""
    if (
        parsed.scheme != "https" or parsed.query or parsed.fragment
        or base is None or relative_path not in ALLOWED_PATHS
    ):
        raise NativeHelperError("endpoint is not in the Quark helper allowlist")
    if not isinstance(params, Mapping) or any(
        not isinstance(key, str) or isinstance(item, (dict, list))
        for key, item in params.items()
    ):
        raise NativeHelperError("invalid request params")
    if body is not None and not isinstance(body, Mapping):
        raise NativeHelperError("invalid request body")
    if not isinstance(cookie, str) or not cookie or len(cookie) > 32_768:
        raise NativeHelperError("invalid delegated Cookie")
    expected_id = canonical_request_id(method, endpoint, params, body)
    if not isinstance(request_id, str) or not secrets.compare_digest(request_id, expected_id):
        raise NativeHelperError("request id does not match canonical payload")
    return {
        "method": method, "endpoint": endpoint, "params": dict(params),
        "body": dict(body) if body is not None else None,
        "cookie": cookie, "request_id": request_id,
    }


class _WebSocket:
    """Small RFC6455 client sufficient for the loopback CDP JSON protocol."""

    def __init__(self, url: str, *, timeout: float = 15.0) -> None:
        parsed = urllib.parse.urlsplit(url)
        if parsed.scheme != "ws" or parsed.hostname not in {"127.0.0.1", "localhost", "::1"}:
            raise NativeRuntimeUnavailable("CDP WebSocket must be loopback ws://")
        self.sock = socket.create_connection((parsed.hostname, parsed.port or 80), timeout)
        self.sock.settimeout(timeout)
        key = base64.b64encode(os.urandom(16)).decode("ascii")
        path = urllib.parse.urlunsplit(("", "", parsed.path or "/", parsed.query, ""))
        request = (
            f"GET {path} HTTP/1.1\r\nHost: {parsed.hostname}:{parsed.port or 80}\r\n"
            f"Upgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: {key}\r\n"
            "Sec-WebSocket-Version: 13\r\n\r\n"
        ).encode("ascii")
        self.sock.sendall(request)
        raw = self._read_headers()
        if not raw.startswith(b"HTTP/1.1 101 "):
            self.close()
            raise NativeRuntimeUnavailable("CDP WebSocket upgrade failed")
        expected = base64.b64encode(hashlib.sha1(
            (key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11").encode("ascii")
        ).digest()).decode("ascii")
        expected_header = f"sec-websocket-accept: {expected}".lower().encode("ascii")
        if expected_header not in raw.lower():
            self.close()
            raise NativeRuntimeUnavailable("CDP WebSocket accept key is invalid")

    def _read_headers(self) -> bytes:
        data = bytearray()
        while b"\r\n\r\n" not in data and len(data) < 64 * 1024:
            chunk = self.sock.recv(4096)
            if not chunk:
                break
            data.extend(chunk)
        return bytes(data)

    def _read_exact(self, length: int) -> bytes:
        output = bytearray()
        while len(output) < length:
            chunk = self.sock.recv(length - len(output))
            if not chunk:
                raise NativeRuntimeUnavailable("CDP WebSocket closed")
            output.extend(chunk)
        return bytes(output)

    def send_json(self, value: Mapping[str, Any]) -> None:
        payload = json.dumps(value, separators=(",", ":")).encode("utf-8")
        mask = os.urandom(4)
        length = len(payload)
        header = bytearray([0x81])
        if length < 126:
            header.append(0x80 | length)
        elif length < 65536:
            header.append(0x80 | 126); header.extend(struct.pack("!H", length))
        else:
            header.append(0x80 | 127); header.extend(struct.pack("!Q", length))
        header.extend(mask)
        masked = bytes(byte ^ mask[index % 4] for index, byte in enumerate(payload))
        self.sock.sendall(bytes(header) + masked)

    def receive_json(self) -> Mapping[str, Any]:
        while True:
            first, second = self._read_exact(2)
            opcode, length = first & 0x0F, second & 0x7F
            if length == 126:
                length = struct.unpack("!H", self._read_exact(2))[0]
            elif length == 127:
                length = struct.unpack("!Q", self._read_exact(8))[0]
            mask = self._read_exact(4) if second & 0x80 else b""
            payload = self._read_exact(length)
            if mask:
                payload = bytes(byte ^ mask[index % 4] for index, byte in enumerate(payload))
            if opcode == 0x8:
                raise NativeRuntimeUnavailable("CDP WebSocket closed")
            if opcode == 0x9:
                self._send_control(0xA, payload)
                continue
            if opcode != 0x1:
                continue
            value = json.loads(payload)
            if isinstance(value, Mapping):
                return value

    def _send_control(self, opcode: int, payload: bytes) -> None:
        mask = os.urandom(4)
        masked = bytes(byte ^ mask[index % 4] for index, byte in enumerate(payload))
        self.sock.sendall(bytes([0x80 | opcode, 0x80 | len(payload)]) + mask + masked)

    def close(self) -> None:
        try:
            self.sock.close()
        except OSError:
            pass


class QuarkCdpRuntime:
    def __init__(
        self,
        discovery_url: str,
        *,
        timeout: float = 20.0,
        quark_process_name: str = DEFAULT_QUARK_PROCESS_NAME,
    ) -> None:
        parsed = urllib.parse.urlsplit(discovery_url)
        if (
            parsed.scheme != "http" or parsed.hostname not in {"127.0.0.1", "localhost", "::1"}
            or parsed.path.rstrip("/") not in {"", "/json", "/json/list"}
        ):
            raise NativeHelperError("CDP discovery URL must be loopback /json/list")
        if parsed.port is None:
            raise NativeHelperError("CDP discovery URL must contain an explicit port")
        self.discovery_url = discovery_url.rstrip("/")
        self.timeout = timeout
        self.cdp_port = parsed.port
        self.quark_process_name = quark_process_name
        # Loopback CDP must never be routed through a host HTTP proxy.  Besides
        # leaking a local URL, proxy errors would be misclassified as renderer
        # failures.
        self._http = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        self._lock = threading.Lock()
        self._sequence = 0

    def _target(self) -> str:
        request = urllib.request.Request(self.discovery_url, headers={"Accept": "application/json"})
        try:
            with self._http.open(request, timeout=self.timeout) as response:
                rows = json.load(response)
        except Exception as exc:
            raise NativeRuntimeUnavailable(f"Quark CDP unavailable: {type(exc).__name__}") from exc
        if not isinstance(rows, list):
            raise NativeRuntimeUnavailable("Quark CDP target list is invalid")
        candidates = [row for row in rows if isinstance(row, Mapping) and row.get("type") == "page"
                      and isinstance(row.get("webSocketDebuggerUrl"), str)]
        candidates.sort(key=lambda row: (
            "clouddrive/renderer" not in str(row.get("url") or ""),
            "index.html?name=main" not in str(row.get("url") or ""),
            "modal-parse-resource.html" not in str(row.get("url") or ""),
            "vip.html" in str(row.get("url") or ""),
            "quark" not in (str(row.get("title") or "") + str(row.get("url") or "")).casefold(),
        ))
        if not candidates:
            raise NativeRuntimeUnavailable("Quark CDP has no renderer target")
        return str(candidates[0]["webSocketDebuggerUrl"])

    @staticmethod
    def _native_probe_expression() -> str:
        return (
            "(() => { const q=globalThis.quantum; const c=globalThis.chrome; "
            "const encrypt=!!(q && q.wsg && typeof q.wsg.encrypt === 'function'); "
            "const decrypt=!!((q && q.wsg && typeof q.wsg.decrypt === 'function') || "
            "(c && c.quarkBizPrivate && typeof c.quarkBizPrivate.encryptOrDecrypt === 'function')); "
            "return encrypt && decrypt ? 'ready' : 'missing'; })()"
        )

    def _probe_native_once(self) -> None:
        value = self._evaluate(
            self._native_probe_expression()
        )
        if value != "ready":
            raise NativeRuntimeUnavailable("selected Quark renderer lacks native WSG primitives")

    def quark_pids(self) -> list[int]:
        """Return current desktop PIDs without launching or activating it."""
        completed = subprocess.run(
            ["pgrep", "-x", self.quark_process_name], check=False,
            stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True,
        )
        if completed.returncode not in {0, 1}:
            raise NativeRuntimeUnavailable("quark_process_probe_failed")
        return sorted({
            int(value) for value in completed.stdout.split()
            if value.isascii() and value.isdecimal() and int(value) > 0
        })

    def passive_probe(self) -> dict[str, Any]:
        """Prove existing CDP/WSG readiness without any process side effect."""
        pids = self.quark_pids()
        if not pids:
            raise NativeRuntimeUnavailable("quark_not_running")
        self._probe_native_once()
        return {"quark_pids": pids, "cdp_port": self.cdp_port}

    def probe(self) -> None:
        """Compatibility readiness probe; permanently passive and fail-closed."""
        self.passive_probe()

    def _evaluate(self, expression: str, *, await_promise: bool = False) -> str:
        with self._lock:
            socket_client = _WebSocket(self._target(), timeout=self.timeout)
            try:
                self._sequence += 1
                call_id = self._sequence
                socket_client.send_json({
                    "id": call_id, "method": "Runtime.evaluate", "params": {
                        "expression": expression, "returnByValue": True,
                        "awaitPromise": await_promise,
                    },
                })
                while True:
                    message = socket_client.receive_json()
                    if message.get("id") != call_id:
                        continue
                    if message.get("error"):
                        raise NativeRuntimeUnavailable("Quark CDP evaluation failed")
                    result = message.get("result")
                    remote = result.get("result") if isinstance(result, Mapping) else None
                    if not isinstance(remote, Mapping) or remote.get("subtype") == "error":
                        raise NativeRuntimeUnavailable("Quark native WSG evaluation failed")
                    value = remote.get("value")
                    if not isinstance(value, str) or not value:
                        raise NativeRuntimeUnavailable("Quark native WSG returned no text")
                    return value
            finally:
                socket_client.close()

    def encrypt(self, plain: str) -> str:
        expression = (
            "(() => { const q = globalThis.quantum; if (!q || !q.wsg) "
            "throw new Error('WSG_UNAVAILABLE'); const r = q.wsg.encrypt({number:"
            f"{WSG_SECURE_NO},plain:{json.dumps(plain)}}}); return r && "
            "(r.cipher_b64 || r.cipherB64 || r.cipher); })()"
        )
        return self._evaluate(expression)

    def decrypt(self, cipher: str) -> str:
        # The Chromium private bridge is the production response-decryption
        # path confirmed in Quark 7.0.0.764.  Keep quantum.wsg.decrypt only as
        # a compatibility fallback for renderer builds that expose it.
        expression = (
            "new Promise((resolve,reject) => { try { const c=globalThis.chrome; "
            "if (c && c.quarkBizPrivate && c.quarkBizPrivate.encryptOrDecrypt) { "
            "c.quarkBizPrivate.encryptOrDecrypt({encrypt:false,data:"
            f"{json.dumps(cipher)},wsgNum:{WSG_SECURE_NO}}}, r => "
            "resolve(typeof r === 'string' ? r : (r && r.data))); return; } "
            "const q=globalThis.quantum; const r=q.wsg.decrypt({number:"
            f"{WSG_SECURE_NO},cipher_b64:{json.dumps(cipher)}}}); "
            "resolve(r && (r.plain || r.plain_text || r.text)); } catch(e) { reject(e); } })"
        )
        return self._evaluate(expression, await_promise=True)


class QuarkNativeRequestDriver:
    def __init__(self, runtime: QuarkCdpRuntime, *, timeout: float = 90.0) -> None:
        self.runtime = runtime
        self.timeout = timeout

    def passive_ensure_ready(self) -> Mapping[str, Any]:
        return self.runtime.passive_probe()

    def request(self, value: Mapping[str, Any]) -> Mapping[str, Any]:
        body = value.get("body")
        plain = json.dumps(body, ensure_ascii=False, separators=(",", ":")) if body is not None else ""
        encrypted = self.runtime.encrypt(plain) if body is not None else None
        query = urllib.parse.urlencode({key: str(item) for key, item in value["params"].items()})
        url = str(value["endpoint"]) + ("?" + query if query else "")
        request = urllib.request.Request(url, data=(encrypted.encode("utf-8") if encrypted is not None else None),
            method=str(value["method"]), headers={
                "Accept": "application/json, text/plain, */*",
                "Content-Type": "application/json",
                "Cookie": str(value["cookie"]),
                "Origin": "https://pan.quark.cn", "Referer": "https://pan.quark.cn/",
                "User-Agent": QUARK_UA,
                **({"X-U-Content-Encoding": "wg"} if encrypted is not None else {}),
            })
        try:
            with urllib.request.urlopen(request, timeout=self.timeout) as response:
                raw = response.read(MAX_REQUEST_BYTES + 1)
                encoding = response.headers.get("X-U-Content-Encoding", "")
        except urllib.error.HTTPError as exc:
            # Quark error bodies may also be WSG; preserve a bounded generic
            # diagnostic without leaking request credentials or payloads.
            raise NativeHelperError(f"Quark HTTP status {exc.code}") from exc
        except Exception as exc:
            raise NativeHelperError(f"Quark request failed: {type(exc).__name__}") from exc
        if len(raw) > MAX_REQUEST_BYTES:
            raise NativeHelperError("Quark response exceeds helper limit")
        text = raw.decode("utf-8", "strict")
        if encoding.casefold() == "wg":
            text = self.runtime.decrypt(text)
        result = json.loads(text)
        if not isinstance(result, Mapping):
            raise NativeHelperError("Quark response is not an object")
        return result


@dataclass
class _JournalRow:
    status: str
    result: Mapping[str, Any] | None = None


class SubmitJournal:
    """0600 persistent replay journal for the submit mutation boundary."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self._lock = threading.Lock()
        self._rows: dict[str, _JournalRow] = {}
        if path.exists():
            value = json.loads(path.read_text(encoding="utf-8"))
            raw_rows = value.get("requests") if isinstance(value, Mapping) else None
            if isinstance(raw_rows, Mapping):
                for key, row in raw_rows.items():
                    if isinstance(key, str) and isinstance(row, Mapping):
                        result = row.get("result")
                        self._rows[key] = _JournalRow(
                            str(row.get("status") or "in_doubt"),
                            dict(result) if isinstance(result, Mapping) else None,
                        )

    def _save(self) -> None:
        atomic_write_json(
            self.path,
            {"version": 1, "requests": {
                key: {"status": row.status, **({"result": row.result} if row.result else {})}
                for key, row in self._rows.items()
            }},
            indent=None,
            separators=(",", ":"),
            trailing_newline=False,
        )

    def status_counts(self) -> dict[str, int]:
        """Return non-sensitive journal health counters."""
        with self._lock:
            counts = {"complete": 0, "in_doubt": 0, "failed": 0}
            for row in self._rows.values():
                key = row.status if row.status in counts else "in_doubt"
                counts[key] += 1
            return counts

    def reconcile_failed(
        self, request_id: str, evidence: Mapping[str, Any],
    ) -> Mapping[str, Any]:
        """Resolve one indeterminate submit only from explicit failure proof.

        The immutable sidecar survives a later retry, while the journal row's
        ``failed`` state permits that exact canonical request to be attempted
        again.  This is intentionally narrower than a generic delete/reset.
        """
        if not re.fullmatch(r"[0-9a-f]{64}", request_id):
            raise NativeHelperError("invalid reconciliation request id")
        required = {
            "resolution": "confirmed_failed",
            "task_status": -1,
            "destination_empty": True,
        }
        if any(evidence.get(key) != value for key, value in required.items()):
            raise NativeHelperError("reconciliation lacks confirmed failure proof")
        if not re.fullmatch(r"[0-9a-f]{32}", str(evidence.get("task_id") or "")):
            raise NativeHelperError("reconciliation task id is invalid")
        if not re.fullmatch(r"[0-9a-f]{64}", str(evidence.get("task_name_sha256") or "")):
            raise NativeHelperError("reconciliation task-name digest is invalid")
        destination = evidence.get("destination")
        observed_at = evidence.get("observed_at")
        if (
            not isinstance(destination, str)
            or not destination.startswith("/quark/影视/ScrapeFlow/补源/")
            or not isinstance(observed_at, str) or not observed_at
        ):
            raise NativeHelperError("reconciliation destination/timestamp is invalid")
        safe_evidence = {
            "schema_version": 1,
            "kind": "quark_submit_failed_reconciliation",
            "request_id": request_id,
            "resolution": "confirmed_failed",
            "task_id": str(evidence["task_id"]),
            "task_status": -1,
            "task_name_sha256": str(evidence["task_name_sha256"]),
            "destination": destination,
            "destination_empty": True,
            "source_job_id": str(evidence.get("source_job_id") or ""),
            "observed_at": observed_at,
        }
        with self._lock:
            prior = self._rows.get(request_id)
            if prior is None or prior.status != "in_doubt":
                raise NativeHelperError("request is not currently in doubt")
            evidence_root = self.path.parent / "submit-reconciliations"
            evidence_root.mkdir(mode=0o700, parents=True, exist_ok=True)
            evidence_path = evidence_root / f"{request_id}.json"
            atomic_write_json(
                evidence_path,
                safe_evidence,
                indent=None,
                sort_keys=True,
                separators=(",", ":"),
            )
            self._rows[request_id] = _JournalRow("failed", safe_evidence)
            self._save()
            return {**safe_evidence, "evidence_path": str(evidence_path)}

    def replay(self, request_id: str) -> Mapping[str, Any] | None:
        """Replay a commit or preserve in-doubt state before readiness checks."""
        with self._lock:
            prior = self._rows.get(request_id)
            if prior is None:
                return None
            if prior.status == "failed":
                return None
            if prior.status == "complete" and prior.result is not None:
                return dict(prior.result)
            raise NativeRequestInDoubt(
                "prior Quark submit is in doubt; verify destination before retry"
            )

    def run(self, request_id: str, operation: Any) -> Mapping[str, Any]:
        with self._lock:
            prior = self._rows.get(request_id)
            if prior is not None and prior.status == "complete" and prior.result is not None:
                return prior.result
            if prior is not None and prior.status != "failed":
                raise NativeRequestInDoubt(
                    "prior Quark submit is in doubt; verify destination before retry"
                )
            self._rows[request_id] = _JournalRow("in_doubt")
            self._save()
            result = operation()
            if not isinstance(result, Mapping):
                raise NativeHelperError("native driver returned a non-object response")
            self._rows[request_id] = _JournalRow("complete", dict(result))
            self._save()
            return result


class NativeHelperService:
    def __init__(self, driver: QuarkNativeRequestDriver, journal: SubmitJournal) -> None:
        self.driver = driver
        self.journal = journal

    def execute(self, raw: Mapping[str, Any]) -> Mapping[str, Any]:
        value = validate_native_request(raw)
        endpoint = str(value["endpoint"])
        base = next(candidate for candidate in (QUARK_SHARE_API, QUARK_DRIVE_API)
                    if endpoint.startswith(candidate + "/"))
        path = endpoint[len(base):]
        request_id = str(value["request_id"])
        if path in MUTATING_PATHS:
            replay = self.journal.replay(request_id)
            if replay is not None:
                return replay
        # Every request is permanently passive.  Client flags cannot opt into a
        # launch/restart/UI path, and the driver exposes no active readiness
        # method.  Readiness must be proven before the submit journal crosses
        # its in-doubt boundary; a missing existing renderer is infrastructure
        # state, not evidence that a cloud mutation may have happened.
        self.driver.passive_ensure_ready()
        operation = lambda: self.driver.request(value)
        if path in MUTATING_PATHS:
            return self.journal.run(request_id, operation)
        return operation()
