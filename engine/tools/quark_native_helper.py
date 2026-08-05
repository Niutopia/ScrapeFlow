#!/usr/bin/env python3
"""Run the authenticated host-side ScrapeFlow Quark native helper."""

from __future__ import annotations

import argparse
import hashlib
import hmac
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import plistlib
import secrets
import subprocess
import sys
import time
from typing import Any, Mapping
import urllib.error
import urllib.request

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from engine.scrapeflow.quark_native_helper import (
    MAX_REQUEST_BYTES, NativeHelperError, NativeHelperService,
    NativeRequestInDoubt, NativeRuntimeUnavailable, QuarkCdpRuntime,
    QuarkNativeRequestDriver,
    SubmitJournal,
)


LAUNCH_AGENT_LABEL = "com.scrapeflow.quark-native-helper"
LAUNCH_AGENT_READY_TIMEOUT = 45.0


def _helper_build_id() -> str:
    return hashlib.sha256(
        Path(__file__).read_bytes()
        + (PROJECT_ROOT / "engine/scrapeflow/quark_native_helper.py").read_bytes()
    ).hexdigest()


def _handler(service: NativeHelperService, token: str):
    build_id = _helper_build_id()

    class Handler(BaseHTTPRequestHandler):
        server_version = "ScrapeFlowQuarkHelper/1"

        def log_message(self, _format: str, *_args: Any) -> None:
            # Never let the standard HTTP logger print an Authorization header,
            # Cookie-bearing body, Quark token, or magnet URL.
            return

        def _json(self, status: int, value: Mapping[str, Any]) -> None:
            payload = json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers(); self.wfile.write(payload)

        def do_GET(self) -> None:
            if self.path not in {"/health", "/health/passive"}:
                self._json(404, {"status": "error", "error": "not found"}); return
            try:
                runtime_evidence = service.driver.runtime.passive_probe()
            except NativeRuntimeUnavailable:
                # The helper is intentionally useful as an idle loopback service.
                # Quark being closed (or lacking an existing CDP renderer) is
                # runtime availability, not helper process failure.  Returning a
                # healthy helper envelope lets LaunchAgent installation finish
                # without opening, activating, or waiting for Quark.
                self._json(200, {
                    "status": "ok", "service": "quark-native-helper",
                    "helper_alive": True,
                    "existing_quark_connected": False,
                    "runtime": "idle",
                    "readiness": "waiting-for-existing-quark",
                    "build_id": build_id,
                    "journal": service.journal.status_counts(),
                }); return
            except NativeHelperError:
                # A helper/driver invariant failure is not an idle Quark state.
                # Keep its details out of the unauthenticated health response and
                # report a non-healthy envelope instead.
                self._json(503, {
                    "status": "error", "service": "quark-native-helper",
                    "helper_alive": True,
                    "existing_quark_connected": False,
                    "runtime": "error",
                    "readiness": "helper-error",
                    "build_id": build_id,
                }); return
            self._json(200, {
                "status": "ok", "service": "quark-native-helper",
                "helper_alive": True,
                "existing_quark_connected": True,
                "runtime": "connected", "readiness": "native-ready",
                "build_id": build_id,
                "journal": service.journal.status_counts(),
                **dict(runtime_evidence),
            })

        def do_POST(self) -> None:
            if self.path not in {"/v1/quark/request", "/v1/journal/reconcile-failed"}:
                self._json(404, {"status": "error", "error": "not found"}); return
            supplied = self.headers.get("Authorization", "")
            if not hmac.compare_digest(supplied, "Bearer " + token):
                self._json(401, {"status": "error", "error": "unauthorized"}); return
            try:
                length = int(self.headers.get("Content-Length", "0"))
            except ValueError:
                length = -1
            if length <= 0 or length > MAX_REQUEST_BYTES:
                self._json(413, {"status": "error", "error": "invalid body size"}); return
            try:
                value = json.loads(self.rfile.read(length))
                if not isinstance(value, Mapping):
                    raise NativeHelperError("request body is not an object")
                if self.path == "/v1/journal/reconcile-failed":
                    request_id = value.get("request_id")
                    evidence = value.get("evidence")
                    if not isinstance(request_id, str) or not isinstance(evidence, Mapping):
                        raise NativeHelperError("invalid reconciliation body")
                    result = service.journal.reconcile_failed(request_id, evidence)
                else:
                    result = service.execute(value)
            except NativeRequestInDoubt as exc:
                self._json(409, {"status": "error", "error": str(exc), "in_doubt": True}); return
            except (NativeHelperError, UnicodeError, json.JSONDecodeError) as exc:
                self._json(502, {"status": "error", "error": str(exc)[:240]}); return
            self._json(200, {"status": "ok", "result": result})

    return Handler


def _load_token(token_file: Path) -> str:
    token = os.getenv("SCRAPEFLOW_QUARK_HELPER_TOKEN", "")
    if token:
        return token
    if not token_file.exists():
        raise NativeHelperError(f"helper token file does not exist: {token_file}")
    if token_file.is_symlink() or not token_file.is_file() or token_file.stat().st_mode & 0o077:
        raise NativeHelperError("helper token file permissions must be 0600")
    return token_file.read_text(encoding="utf-8").strip()


def _ensure_token_file(token_file: Path) -> None:
    if token_file.exists():
        if token_file.is_symlink() or token_file.stat().st_mode & 0o077:
            raise NativeHelperError("helper token file must be a regular 0600 file")
        if len(token_file.read_text(encoding="utf-8").strip()) < 24:
            raise NativeHelperError("helper token file must contain at least 24 characters")
        return
    token_file.parent.mkdir(parents=True, exist_ok=True)
    descriptor = os.open(token_file, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        os.write(descriptor, (secrets.token_urlsafe(36) + "\n").encode("ascii"))
    finally:
        os.close(descriptor)


def _launch_agent_path() -> Path:
    return Path.home() / f"Library/LaunchAgents/{LAUNCH_AGENT_LABEL}.plist"


def _launchctl_domain() -> str:
    return f"gui/{os.getuid()}"


def _launchctl_service() -> str:
    return f"{_launchctl_domain()}/{LAUNCH_AGENT_LABEL}"


def _run_launchctl(*arguments: str, check: bool = True) -> subprocess.CompletedProcess[str]:
    try:
        return subprocess.run(
            ["launchctl", *arguments], check=check, text=True,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        )
    except (OSError, subprocess.CalledProcessError) as exc:
        detail = ""
        if isinstance(exc, subprocess.CalledProcessError):
            detail = str(exc.stderr or exc.stdout or "").strip()[:240]
        raise NativeHelperError(
            f"launchctl {' '.join(arguments)} failed"
            + (f": {detail}" if detail else "")
        ) from exc


def _bootout_launch_agent() -> None:
    # An absent legacy service is the normal first-install/uninstalled state.
    _run_launchctl("bootout", _launchctl_service(), check=False)


def _fetch_helper_health(port: int, *, timeout: float) -> Mapping[str, Any]:
    request = urllib.request.Request(
        f"http://127.0.0.1:{port}/health/passive",
        method="GET",
        headers={"Accept": "application/json"},
    )
    with urllib.request.urlopen(request, timeout=timeout) as response:
        value = json.loads(response.read())
    if not isinstance(value, Mapping):
        raise NativeHelperError("helper health response is not an object")
    return value


def _wait_helper_alive(
    port: int, *, timeout: float = LAUNCH_AGENT_READY_TIMEOUT,
) -> Mapping[str, Any]:
    """Wait only for this helper build, never for Quark/CDP readiness."""

    deadline = time.monotonic() + timeout
    expected_build_id = _helper_build_id()
    last_error = "helper health endpoint did not respond"
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise NativeHelperError(
                f"launch agent helper did not become alive within {timeout:g}s: {last_error}"
            )
        try:
            value = _fetch_helper_health(port, timeout=min(2.0, remaining))
            if (
                value.get("status") == "ok"
                and value.get("service") == "quark-native-helper"
                and value.get("helper_alive") is True
                and value.get("build_id") == expected_build_id
                and isinstance(value.get("journal"), Mapping)
            ):
                return value
            last_error = "health response is not helper-alive or belongs to an older build"
        except (
            OSError, urllib.error.URLError, UnicodeError,
            json.JSONDecodeError, NativeHelperError,
        ) as exc:
            last_error = f"{type(exc).__name__}: {str(exc)[:160]}"
        time.sleep(min(0.25, max(0.0, deadline - time.monotonic())))


def _install_launch_agent(args: argparse.Namespace) -> Path:
    _ensure_token_file(args.token_file)
    target = _launch_agent_path()
    target.parent.mkdir(parents=True, exist_ok=True)
    program_arguments = [
        str(Path(sys.executable).resolve()), str(Path(__file__).resolve()),
        "--host", "127.0.0.1", "--port", str(args.port),
        "--cdp-url", args.cdp_url, "--state-dir", str(args.state_dir.resolve()),
        "--token-file", str(args.token_file.resolve()),
    ]
    payload = {
        "Label": LAUNCH_AGENT_LABEL,
        "ProgramArguments": program_arguments,
        "RunAtLoad": True,
        # Historical plists used SuccessfulExit=False.  A rejected legacy active
        # argument then made launchd retry a clean argparse failure forever.
        # Restart only signal-crashed helpers; a normal/nonzero CLI exit does not
        # become a KeepAlive failure loop.
        "KeepAlive": {"Crashed": True},
        "ProcessType": "Background",
        "StandardOutPath": "/dev/null",
        "StandardErrorPath": "/dev/null",
    }
    # Stop a loaded historical job before replacing its plist.  This is the
    # migration boundary that removes any legacy launch/restart/UI arguments
    # from launchd before the new passive-only service is bootstrapped.
    _bootout_launch_agent()
    temporary = target.with_suffix(".plist.tmp")
    with temporary.open("wb") as output:
        plistlib.dump(payload, output, sort_keys=True)
    os.chmod(temporary, 0o600)
    os.replace(temporary, target)
    _run_launchctl("bootstrap", _launchctl_domain(), str(target))
    _run_launchctl("kickstart", "-k", _launchctl_service())
    _wait_helper_alive(args.port)
    return target


def _uninstall_launch_agent() -> None:
    _bootout_launch_agent()
    _launch_agent_path().unlink(missing_ok=True)


def main() -> int:
    parser = argparse.ArgumentParser(description="ScrapeFlow Quark Native Helper")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=18765)
    parser.add_argument("--cdp-url", default="http://127.0.0.1:19222/json/list")
    parser.add_argument("--state-dir", type=Path, default=Path.home() / ".scrapeflow-quark-helper")
    parser.add_argument("--token-file", type=Path)
    actions = parser.add_mutually_exclusive_group()
    actions.add_argument("--install-launch-agent", action="store_true")
    actions.add_argument("--uninstall-launch-agent", action="store_true")
    args = parser.parse_args()
    if args.token_file is None:
        args.token_file = args.state_dir / "token"
    if args.host not in {"127.0.0.1", "::1"}:
        parser.error("helper may bind only to loopback")
    if args.install_launch_agent:
        try:
            _install_launch_agent(args)
        except NativeHelperError as exc:
            parser.error(str(exc))
        return 0
    if args.uninstall_launch_agent:
        try:
            _uninstall_launch_agent()
        except NativeHelperError as exc:
            parser.error(str(exc))
        return 0
    try:
        token = _load_token(args.token_file)
    except NativeHelperError as exc:
        parser.error(str(exc))
    if len(token) < 24:
        parser.error("SCRAPEFLOW_QUARK_HELPER_TOKEN must contain at least 24 characters")
    runtime = QuarkCdpRuntime(args.cdp_url)
    service = NativeHelperService(
        QuarkNativeRequestDriver(runtime),
        SubmitJournal(args.state_dir / "submit-journal.json"),
    )
    server = ThreadingHTTPServer((args.host, args.port), _handler(service, token))
    server.daemon_threads = True
    server.serve_forever()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
