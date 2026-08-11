#!/usr/bin/env python3
"""Install and operate the macOS LaunchAgent for Quark's fixed local CDP.

The generated job runs QuarkCloudDrive itself.  It does not run a ScrapeFlow
helper and it does not interact with Quark's windows or desktop UI.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import plistlib
import re
import stat
import subprocess
import tempfile
import time
from typing import Sequence
import urllib.error
import urllib.request
from urllib.parse import urlsplit


LAUNCH_AGENT_LABEL = "com.scrapeflow.quark-cdp"
QUARK_BUNDLE_IDENTIFIER = "com.quark.clouddrive.desktop"
QUARK_EXECUTABLE = Path(
    "/Applications/QuarkCloudDrive.app/Contents/MacOS/QuarkCloudDrive"
)
QUARK_INFO_PLIST = Path("/Applications/QuarkCloudDrive.app/Contents/Info.plist")
CDP_ARGUMENTS = (
    "--remote-debugging-address=127.0.0.1",
    "--remote-debugging-port=19222",
)
REPLACE_TIMEOUT_SECONDS = 20.0
REPLACE_POLL_SECONDS = 0.1
LAUNCH_READY_TIMEOUT_SECONDS = 30.0
CDP_LIST_URL = "http://127.0.0.1:19222/json/list"
CDP_PAGE_PATH_RE = re.compile(r"^/devtools/page/[A-Za-z0-9._:-]+$")
CDP_QUARK_MARKERS = ("quark", "clouddrive", "uccd://")

NORMAL_TERMINATE_JXA = r'''ObjC.import("AppKit");
function run(argv) {
    if (argv.length !== 1 || !/^[1-9][0-9]*$/.test(argv[0])) {
        throw new Error("one positive PID is required");
    }
    const pid = Number(argv[0]);
    const apps = $.NSRunningApplication.runningApplicationsWithBundleIdentifier(
        "com.quark.clouddrive.desktop"
    );
    let target = null;
    for (let index = 0; index < apps.count; index++) {
        const candidate = apps.objectAtIndex(index);
        if (Number(candidate.processIdentifier) === pid) {
            target = candidate;
            break;
        }
    }
    if (target === null) {
        throw new Error("exact Quark bundle/PID is not running");
    }
    // In JXA, reading this zero-argument ObjC selector invokes the regular
    // NSRunningApplication termination request without a window action.
    if (!Boolean(target.terminate)) {
        throw new Error("Quark declined normal termination");
    }
    return "terminate-requested:" + String(pid);
}'''


class QuarkLifecycleError(RuntimeError):
    """Raised when a lifecycle operation cannot be completed safely."""


def _launch_agent_path() -> Path:
    return Path.home() / "Library" / "LaunchAgents" / f"{LAUNCH_AGENT_LABEL}.plist"


def _launchctl_domain() -> str:
    return f"gui/{os.getuid()}"


def _launchctl_service() -> str:
    return f"{_launchctl_domain()}/{LAUNCH_AGENT_LABEL}"


def _run_command(
    arguments: Sequence[str], *, check: bool = True
) -> subprocess.CompletedProcess[str]:
    try:
        return subprocess.run(
            list(arguments),
            check=check,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
    except OSError as exc:
        raise QuarkLifecycleError(f"cannot run {arguments[0]}: {exc}") from exc
    except subprocess.CalledProcessError as exc:
        detail = str(exc.stderr or exc.stdout or "").strip()[:240]
        operation = " ".join(arguments)
        raise QuarkLifecycleError(
            f"{operation} failed" + (f": {detail}" if detail else "")
        ) from exc


def _run_launchctl(
    *arguments: str, check: bool = True
) -> subprocess.CompletedProcess[str]:
    return _run_command(("/bin/launchctl", *arguments), check=check)


def _validate_quark_executable() -> None:
    try:
        metadata = QUARK_EXECUTABLE.lstat()
    except FileNotFoundError as exc:
        raise QuarkLifecycleError(
            f"Quark executable does not exist: {QUARK_EXECUTABLE}"
        ) from exc
    except OSError as exc:
        raise QuarkLifecycleError(
            f"cannot inspect Quark executable {QUARK_EXECUTABLE}: {exc}"
        ) from exc
    if stat.S_ISLNK(metadata.st_mode):
        raise QuarkLifecycleError("Quark executable must not be a symbolic link")
    if not stat.S_ISREG(metadata.st_mode):
        raise QuarkLifecycleError("Quark executable must be a regular file")
    if not os.access(QUARK_EXECUTABLE, os.X_OK):
        raise QuarkLifecycleError("Quark executable is not executable")
    try:
        info_metadata = QUARK_INFO_PLIST.lstat()
        if stat.S_ISLNK(info_metadata.st_mode) or not stat.S_ISREG(info_metadata.st_mode):
            raise QuarkLifecycleError("Quark Info.plist must be a regular non-link file")
        with QUARK_INFO_PLIST.open("rb") as source:
            bundle = plistlib.load(source)
    except QuarkLifecycleError:
        raise
    except (OSError, plistlib.InvalidFileException) as exc:
        raise QuarkLifecycleError(f"cannot validate Quark application bundle: {exc}") from exc
    if bundle.get("CFBundleIdentifier") != QUARK_BUNDLE_IDENTIFIER:
        raise QuarkLifecycleError("Quark application bundle identifier is unexpected")
    if bundle.get("CFBundleExecutable") != QUARK_EXECUTABLE.name:
        raise QuarkLifecycleError("Quark application bundle executable is unexpected")


def _inspect_launch_agent_target(target: Path) -> None:
    """Reject unsafe existing targets and unsafe LaunchAgents directories."""

    parent = target.parent
    if parent.exists() or parent.is_symlink():
        try:
            parent_metadata = parent.lstat()
        except OSError as exc:
            raise QuarkLifecycleError(
                f"cannot inspect LaunchAgents directory {parent}: {exc}"
            ) from exc
        if stat.S_ISLNK(parent_metadata.st_mode):
            raise QuarkLifecycleError(
                "LaunchAgents directory must not be a symbolic link"
            )
        if not stat.S_ISDIR(parent_metadata.st_mode):
            raise QuarkLifecycleError("LaunchAgents parent must be a directory")
        if parent_metadata.st_uid != os.getuid():
            raise QuarkLifecycleError(
                "LaunchAgents directory must be owned by the current user"
            )

    if not (target.exists() or target.is_symlink()):
        return
    try:
        metadata = target.lstat()
    except OSError as exc:
        raise QuarkLifecycleError(
            f"cannot inspect LaunchAgent target {target}: {exc}"
        ) from exc
    if stat.S_ISLNK(metadata.st_mode):
        raise QuarkLifecycleError("LaunchAgent target must not be a symbolic link")
    if not stat.S_ISREG(metadata.st_mode):
        raise QuarkLifecycleError("LaunchAgent target must be a regular file")
    if metadata.st_uid != os.getuid():
        raise QuarkLifecycleError("LaunchAgent target must be owned by the current user")
    if metadata.st_nlink != 1:
        raise QuarkLifecycleError("LaunchAgent target must have exactly one hard link")


def _prepare_launch_agent_parent(target: Path) -> None:
    parent = target.parent
    if not parent.exists():
        try:
            parent.mkdir(mode=0o700, parents=True)
        except OSError as exc:
            raise QuarkLifecycleError(
                f"cannot create LaunchAgents directory {parent}: {exc}"
            ) from exc
    _inspect_launch_agent_target(target)


def _launch_agent_payload() -> dict[str, object]:
    return {
        "Label": LAUNCH_AGENT_LABEL,
        "ProgramArguments": [str(QUARK_EXECUTABLE), *CDP_ARGUMENTS],
        "RunAtLoad": True,
        # launchd restarts an abnormal exit but respects an intentional clean exit.
        "KeepAlive": {"SuccessfulExit": False},
        "LimitLoadToSessionType": "Aqua",
        "ProcessType": "Interactive",
        "ThrottleInterval": 30,
        "StandardOutPath": "/dev/null",
        "StandardErrorPath": "/dev/null",
    }


def _write_launch_agent(target: Path, payload: dict[str, object]) -> None:
    """Write a user-only plist atomically without following an existing link."""

    _inspect_launch_agent_target(target)
    try:
        serialized = plistlib.dumps(payload, fmt=plistlib.FMT_XML, sort_keys=True)
    except (TypeError, ValueError) as exc:
        raise QuarkLifecycleError(f"cannot serialize LaunchAgent plist: {exc}") from exc

    _atomic_replace_bytes(target, serialized)


def _atomic_replace_bytes(target: Path, content: bytes) -> None:
    """Atomically replace a validated target with user-only bytes."""

    descriptor = -1
    temporary_name: str | None = None
    try:
        descriptor, temporary_name = tempfile.mkstemp(
            prefix=f".{target.name}.", suffix=".tmp", dir=target.parent
        )
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "wb") as output:
            descriptor = -1
            output.write(content)
            output.flush()
            os.fsync(output.fileno())
        # Refuse a link introduced between preflight and replacement.  Replacing
        # it would be filesystem-safe, but silently accepting it is surprising.
        _inspect_launch_agent_target(target)
        os.replace(temporary_name, target)
        temporary_name = None
    except QuarkLifecycleError:
        raise
    except OSError as exc:
        raise QuarkLifecycleError(f"cannot write LaunchAgent plist {target}: {exc}") from exc
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        if temporary_name is not None:
            try:
                Path(temporary_name).unlink(missing_ok=True)
            except OSError:
                pass


def _find_quark_pids() -> tuple[int, ...]:
    # macOS may truncate a bulk `ps ... comm=` column.  pgrep only supplies
    # candidates; every PID is independently revalidated by uid and full argv.
    result = _run_command(
        ("/usr/bin/pgrep", "-x", QUARK_EXECUTABLE.name), check=False
    )
    if result.returncode == 1:
        return ()
    if result.returncode != 0:
        detail = (result.stderr or "").strip()[:240]
        raise QuarkLifecycleError(
            "pgrep failed while enumerating QuarkCloudDrive"
            + (f": {detail}" if detail else "")
        )
    candidates: list[int] = []
    for raw_line in result.stdout.splitlines():
        try:
            pid = int(raw_line.strip())
        except ValueError as exc:
            raise QuarkLifecycleError("pgrep returned a malformed PID") from exc
        if pid > 0:
            candidates.append(pid)
    return tuple(
        pid for pid in sorted(set(candidates)) if _pid_is_exact_quark(pid)
    )


def _pid_identity(pid: int) -> tuple[int, tuple[str, ...]] | None:
    result = _run_command(
        ("/bin/ps", "-ww", "-p", str(pid), "-o", "uid=,command="), check=False
    )
    if result.returncode != 0:
        return None
    fields = result.stdout.strip().split()
    if len(fields) < 2:
        return None
    try:
        uid = int(fields[0])
    except ValueError:
        return None
    return uid, tuple(fields[1:])


def _pid_is_exact_quark(pid: int) -> bool:
    identity = _pid_identity(pid)
    return bool(
        identity
        and identity[0] == os.getuid()
        and identity[1]
        and identity[1][0] == str(QUARK_EXECUTABLE)
    )


def _pid_is_managed_quark(pid: int) -> bool:
    identity = _pid_identity(pid)
    return bool(
        identity
        and identity[0] == os.getuid()
        and identity[1] == (str(QUARK_EXECUTABLE), *CDP_ARGUMENTS)
    )


def _pid_has_replaceable_argv(pid: int) -> bool:
    identity = _pid_identity(pid)
    return bool(
        identity
        and identity[0] == os.getuid()
        and identity[1]
        in {
            (str(QUARK_EXECUTABLE),),
            (str(QUARK_EXECUTABLE), *CDP_ARGUMENTS),
        }
    )


def _request_normal_quark_termination(pid: int) -> None:
    if not _pid_is_exact_quark(pid):
        raise QuarkLifecycleError(
            f"PID {pid} is not the exact current-user QuarkCloudDrive executable"
        )
    result = _run_command(
        (
            "/usr/bin/osascript",
            "-l",
            "JavaScript",
            "-e",
            NORMAL_TERMINATE_JXA,
            str(pid),
        )
    )
    if result.stdout.strip() != f"terminate-requested:{pid}":
        raise QuarkLifecycleError("normal Quark termination returned unexpected evidence")


def _wait_for_quark_pid_exit(
    pid: int, *, timeout: float = REPLACE_TIMEOUT_SECONDS
) -> None:
    deadline = time.monotonic() + timeout
    while True:
        if not _pid_is_exact_quark(pid):
            return
        if time.monotonic() >= deadline:
            raise QuarkLifecycleError(
                f"QuarkCloudDrive did not complete normal termination within {timeout:g}s "
                f"(PID {pid}); refusing force termination"
            )
        time.sleep(REPLACE_POLL_SECONDS)


def _terminate_exact_quark_pid(
    pid: int, *, timeout: float = REPLACE_TIMEOUT_SECONDS
) -> None:
    _request_normal_quark_termination(pid)
    _wait_for_quark_pid_exit(pid, timeout=timeout)


def _service_snapshot() -> subprocess.CompletedProcess[str] | None:
    result = _run_launchctl("print", _launchctl_service(), check=False)
    if result.returncode == 0:
        return result
    missing_detail = str(result.stderr or result.stdout or "")
    if result.returncode == 113 and (
        "Could not find service" in missing_detail
        or "service not found" in missing_detail.casefold()
    ):
        return None
    detail = missing_detail.strip()[:240]
    raise QuarkLifecycleError(
        "cannot inspect LaunchAgent service" + (f": {detail}" if detail else "")
    )


def _service_origin_path(
    snapshot: subprocess.CompletedProcess[str],
) -> Path:
    """Fail closed unless launchd reports this exact user plist as the origin."""

    matches = re.findall(r"(?m)^\s*path\s*=\s*(.+?)\s*$", snapshot.stdout)
    if len(matches) != 1:
        raise QuarkLifecycleError(
            "cannot prove the loaded Quark LaunchAgent plist origin"
        )
    raw = matches[0].strip().strip('"')
    if not raw.startswith("/"):
        raise QuarkLifecycleError("loaded Quark LaunchAgent origin is not absolute")
    return Path(raw)


def _assert_service_origin(
    snapshot: subprocess.CompletedProcess[str], target: Path,
) -> None:
    try:
        actual = _service_origin_path(snapshot).resolve(strict=False)
        expected = target.resolve(strict=False)
    except OSError as exc:
        raise QuarkLifecycleError(
            f"cannot resolve Quark LaunchAgent origin: {exc}"
        ) from exc
    if actual != expected:
        raise QuarkLifecycleError(
            f"loaded label {LAUNCH_AGENT_LABEL} belongs to {actual}, not {expected}"
        )


def _bootout_launch_agent() -> bool:
    if _service_snapshot() is None:
        return False
    _run_launchctl("bootout", _launchctl_service())
    return True


def _lint_launch_agent(target: Path) -> None:
    _run_command(("/usr/bin/plutil", "-lint", str(target)))


class _RefuseRedirects(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):  # type: ignore[no-untyped-def]
        raise urllib.error.HTTPError(req.full_url, code, "redirect refused", headers, fp)


def _cdp_listener_owned_by(pid: int) -> bool:
    result = _run_command(
        (
            "/usr/sbin/lsof",
            "-nP",
            "-a",
            "-p",
            str(pid),
            "-iTCP@127.0.0.1:19222",
            "-sTCP:LISTEN",
            "-Fpn",
        ),
        check=False,
    )
    if result.returncode != 0:
        return False
    lines = {line.strip() for line in result.stdout.splitlines() if line.strip()}
    return f"p{pid}" in lines and any(
        line.startswith("n127.0.0.1:19222") for line in lines
    )


def _cdp_ready(pid: int) -> bool:
    if not _pid_is_managed_quark(pid) or not _cdp_listener_owned_by(pid):
        return False
    try:
        opener = urllib.request.build_opener(
            urllib.request.ProxyHandler({}),
            _RefuseRedirects(),
        )
        request = urllib.request.Request(
            CDP_LIST_URL,
            headers={"Accept": "application/json", "Host": "127.0.0.1:19222"},
            method="GET",
        )
        with opener.open(request, timeout=0.75) as response:
            if response.status != 200:
                return False
            raw = response.read(1_048_577)
            if len(raw) > 1_048_576:
                return False
            value = json.loads(raw)
    except (OSError, urllib.error.URLError, UnicodeError, json.JSONDecodeError):
        return False
    if not isinstance(value, list) or not value:
        return False
    for entry in value:
        if not isinstance(entry, dict):
            continue
        if entry.get("type") != "page":
            continue
        marker_text = " ".join(
            str(entry.get(key) or "") for key in ("url", "title", "description")
        ).casefold()
        if not any(marker in marker_text for marker in CDP_QUARK_MARKERS):
            continue
        socket_url = entry.get("webSocketDebuggerUrl")
        if not isinstance(socket_url, str):
            continue
        try:
            parsed = urlsplit(socket_url)
            port = parsed.port
        except ValueError:
            continue
        if (
            parsed.scheme == "ws"
            and parsed.hostname in {"127.0.0.1", "localhost"}
            and port == 19222
            and parsed.username is None
            and parsed.password is None
            and not parsed.query
            and not parsed.fragment
            and CDP_PAGE_PATH_RE.fullmatch(parsed.path) is not None
        ):
            return True
    return False


def _managed_quark_pid() -> int | None:
    pids = _find_quark_pids()
    if len(pids) > 1:
        rendered = ", ".join(str(pid) for pid in pids)
        raise QuarkLifecycleError(
            f"multiple exact QuarkCloudDrive processes are running (PID {rendered})"
        )
    if not pids:
        return None
    if not _pid_is_managed_quark(pids[0]):
        raise QuarkLifecycleError(
            f"QuarkCloudDrive PID {pids[0]} does not have the fixed managed argv"
        )
    return pids[0]


def _kickstart(*, force: bool) -> int:
    option = "-kp" if force else "-p"
    result = _run_launchctl("kickstart", option, _launchctl_service())
    output = result.stdout.strip()
    if not output.isdigit() or int(output) <= 0:
        raise QuarkLifecycleError("launchctl kickstart did not return a valid PID")
    return int(output)


def _wait_for_managed_quark_ready(
    expected_pid: int | None = None,
    *, timeout: float = LAUNCH_READY_TIMEOUT_SECONDS
) -> int:
    deadline = time.monotonic() + timeout
    last = "LaunchAgent has no running PID"
    while True:
        try:
            pid = _managed_quark_pid()
        except QuarkLifecycleError as exc:
            pid = None
            last = str(exc)
        if pid is not None:
            if expected_pid is not None and pid != expected_pid:
                last = (
                    f"managed Quark PID changed from {expected_pid} to {pid} "
                    "during readiness"
                )
            elif _cdp_ready(pid):
                return pid
            else:
                last = "fixed Quark CDP endpoint is not ready"
        if time.monotonic() >= deadline:
            raise QuarkLifecycleError(
                f"Quark LaunchAgent did not become ready within {timeout:g}s: {last}"
            )
        time.sleep(REPLACE_POLL_SECONDS)


def _wait_for_unmanaged_quark_ready(
    *, timeout: float = LAUNCH_READY_TIMEOUT_SECONDS,
) -> int:
    deadline = time.monotonic() + timeout
    while True:
        pids = _find_quark_pids()
        if len(pids) == 1:
            identity = _pid_identity(pids[0])
            if identity == (os.getuid(), (str(QUARK_EXECUTABLE),)):
                return pids[0]
        elif len(pids) > 1:
            raise QuarkLifecycleError(
                "multiple QuarkCloudDrive processes appeared during rollback"
            )
        if time.monotonic() >= deadline:
            raise QuarkLifecycleError(
                "unmanaged QuarkCloudDrive did not return after rollback"
            )
        time.sleep(REPLACE_POLL_SECONDS)


def _restore_unmanaged_quark() -> int:
    pids = _find_quark_pids()
    if len(pids) == 1:
        identity = _pid_identity(pids[0])
        if identity == (os.getuid(), (str(QUARK_EXECUTABLE),)):
            return pids[0]
        raise QuarkLifecycleError(
            "cannot restore unmanaged Quark while a different Quark argv is running"
        )
    if len(pids) > 1:
        raise QuarkLifecycleError(
            "cannot restore unmanaged Quark while multiple instances are running"
        )
    _run_command(("/usr/bin/open", "-g", "-b", QUARK_BUNDLE_IDENTIFIER))
    return _wait_for_unmanaged_quark_ready()


def _restore_plist(target: Path, previous: bytes | None) -> None:
    _inspect_launch_agent_target(target)
    try:
        if previous is None:
            target.unlink(missing_ok=True)
        else:
            _atomic_replace_bytes(target, previous)
    except QuarkLifecycleError:
        raise
    except OSError as exc:
        raise QuarkLifecycleError(
            f"cannot restore LaunchAgent plist {target}: {exc}"
        ) from exc


def _validate_previous_launch_agent(content: bytes) -> None:
    try:
        payload = plistlib.loads(content)
    except (ValueError, plistlib.InvalidFileException) as exc:
        raise QuarkLifecycleError(
            "existing Quark LaunchAgent plist is invalid"
        ) from exc
    if not isinstance(payload, dict):
        raise QuarkLifecycleError("existing Quark LaunchAgent plist is not a mapping")
    expected = _launch_agent_payload()
    if set(payload) - set(expected):
        raise QuarkLifecycleError(
            "existing Quark LaunchAgent contains unapproved keys"
        )
    required = (
        "Label",
        "ProgramArguments",
        "RunAtLoad",
        "KeepAlive",
        "StandardOutPath",
        "StandardErrorPath",
    )
    if any(payload.get(key) != expected[key] for key in required):
        raise QuarkLifecycleError(
            "existing Quark LaunchAgent does not match the fixed lifecycle contract"
        )
    for key in ("LimitLoadToSessionType", "ProcessType", "ThrottleInterval"):
        if key in payload and payload[key] != expected[key]:
            raise QuarkLifecycleError(
                "existing Quark LaunchAgent contains a conflicting lifecycle option"
            )


def _install_launch_agent(*, replace_running: bool) -> Path:
    # Complete both preflight checks before changing launchd or any plist.
    _validate_quark_executable()
    target = _launch_agent_path()
    _inspect_launch_agent_target(target)
    payload = _launch_agent_payload()
    try:
        plistlib.dumps(payload, fmt=plistlib.FMT_XML, sort_keys=True)
    except (TypeError, ValueError) as exc:
        raise QuarkLifecycleError(f"cannot serialize LaunchAgent plist: {exc}") from exc

    running = _find_quark_pids()
    if len(running) > 1:
        rendered = ", ".join(str(pid) for pid in running)
        raise QuarkLifecycleError(
            f"multiple exact QuarkCloudDrive processes are running (PID {rendered}); "
            "refusing ambiguous replacement"
        )
    if running and not replace_running:
        rendered = ", ".join(str(pid) for pid in running)
        raise QuarkLifecycleError(
            "QuarkCloudDrive is already running "
            f"(PID {rendered}); retry with --replace-running for a normal app quit"
        )
    if running and not _pid_has_replaceable_argv(running[0]):
        raise QuarkLifecycleError(
            f"QuarkCloudDrive PID {running[0]} has unexpected arguments; "
            "refusing replacement"
        )
    previous = target.read_bytes() if target.exists() else None
    if previous is not None:
        _validate_previous_launch_agent(previous)
    previous_snapshot = _service_snapshot()
    was_loaded = previous_snapshot is not None
    if previous_snapshot is not None:
        if previous is None:
            raise QuarkLifecycleError(
                "the Quark LaunchAgent label is loaded without its expected plist"
            )
        _assert_service_origin(previous_snapshot, target)
    _prepare_launch_agent_parent(target)
    wrote = False
    booted_out = False
    bootstrap_attempted = False
    try:
        _write_launch_agent(target, payload)
        wrote = True
        _lint_launch_agent(target)
        if running:
            _terminate_exact_quark_pid(running[0])
        if was_loaded:
            booted_out = _bootout_launch_agent()
            if not booted_out:
                raise QuarkLifecycleError(
                    "loaded Quark LaunchAgent disappeared before controlled bootout"
                )
        _run_launchctl("enable", _launchctl_service())
        bootstrap_attempted = True
        _run_launchctl("bootstrap", _launchctl_domain(), str(target))
        started_pid = _kickstart(force=False)
        _wait_for_managed_quark_ready(started_pid)
    except QuarkLifecycleError as exc:
        rollback_errors: list[str] = []
        new_service_stopped = not bootstrap_attempted
        if bootstrap_attempted:
            try:
                current_snapshot = _service_snapshot()
                if current_snapshot is not None:
                    _assert_service_origin(current_snapshot, target)
                    _bootout_launch_agent()
                new_service_stopped = True
            except QuarkLifecycleError as rollback_exc:
                rollback_errors.append(f"new service bootout failed: {rollback_exc}")
        if wrote:
            try:
                _restore_plist(target, previous)
            except QuarkLifecycleError as rollback_exc:
                rollback_errors.append(f"plist rollback failed: {rollback_exc}")
        if was_loaded and previous is not None and new_service_stopped:
            try:
                restored_snapshot = _service_snapshot()
                if restored_snapshot is None:
                    _run_launchctl("bootstrap", _launchctl_domain(), str(target))
                else:
                    _assert_service_origin(restored_snapshot, target)
                if running:
                    previous_pid = _kickstart(force=False)
                    _wait_for_managed_quark_ready(previous_pid)
            except QuarkLifecycleError as rollback_exc:
                rollback_errors.append(f"previous service restore failed: {rollback_exc}")
        elif running and not was_loaded and new_service_stopped:
            try:
                _restore_unmanaged_quark()
            except QuarkLifecycleError as rollback_exc:
                rollback_errors.append(f"unmanaged Quark restore failed: {rollback_exc}")
        detail = "; ".join(rollback_errors)
        raise QuarkLifecycleError(
            str(exc)
            + (
                f"; rollback warning: {detail}"
                if detail
                else "; previous lifecycle state restored"
            )
        ) from exc
    return target


def _restart_launch_agent() -> None:
    snapshot = _service_snapshot()
    if snapshot is None:
        raise QuarkLifecycleError("Quark LaunchAgent is not loaded")
    _assert_service_origin(snapshot, _launch_agent_path())
    pid = _managed_quark_pid()
    if pid is not None:
        _terminate_exact_quark_pid(pid)
    started_pid = _kickstart(force=False)
    _wait_for_managed_quark_ready(started_pid)


def _start_launch_agent() -> None:
    snapshot = _service_snapshot()
    if snapshot is None:
        raise QuarkLifecycleError("Quark LaunchAgent is not loaded")
    _assert_service_origin(snapshot, _launch_agent_path())
    current = _managed_quark_pid()
    if current is not None:
        _wait_for_managed_quark_ready(current)
        return
    started_pid = _kickstart(force=False)
    _wait_for_managed_quark_ready(started_pid)


def _force_restart_launch_agent() -> None:
    snapshot = _service_snapshot()
    if snapshot is None:
        raise QuarkLifecycleError("Quark LaunchAgent is not loaded")
    _assert_service_origin(snapshot, _launch_agent_path())
    started_pid = _kickstart(force=True)
    _wait_for_managed_quark_ready(started_pid)


def _uninstall_launch_agent() -> None:
    target = _launch_agent_path()
    _inspect_launch_agent_target(target)
    snapshot = _service_snapshot()
    if snapshot is not None:
        _assert_service_origin(snapshot, target)
    _bootout_launch_agent()
    if target.exists():
        try:
            target.unlink()
        except OSError as exc:
            raise QuarkLifecycleError(
                f"cannot remove LaunchAgent plist {target}: {exc}"
            ) from exc


def _status_launch_agent() -> int:
    result = _service_snapshot()
    if result is None:
        print(f"not loaded: {_launchctl_service()}")
        return 1
    _assert_service_origin(result, _launch_agent_path())
    output = result.stdout.strip()
    print(output or f"loaded: {_launchctl_service()}")
    return 0


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Manage QuarkCloudDrive with fixed loopback CDP arguments"
    )
    actions = parser.add_mutually_exclusive_group(required=True)
    actions.add_argument("--install-launch-agent", action="store_true")
    actions.add_argument("--start", action="store_true")
    actions.add_argument("--restart", action="store_true")
    actions.add_argument(
        "--force-restart",
        action="store_true",
        help="explicitly let launchd kill and restart the exact loaded job",
    )
    actions.add_argument("--uninstall-launch-agent", action="store_true")
    actions.add_argument("--status", action="store_true")
    parser.add_argument(
        "--replace-running",
        action="store_true",
        help="terminate only an exact running QuarkCloudDrive process before install",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _parser()
    args = parser.parse_args(argv)
    if args.replace_running and not args.install_launch_agent:
        parser.error("--replace-running is valid only with --install-launch-agent")
    try:
        if args.install_launch_agent:
            target = _install_launch_agent(replace_running=args.replace_running)
            print(f"installed: {target}")
            return 0
        if args.start:
            _start_launch_agent()
            return 0
        if args.restart:
            _restart_launch_agent()
            return 0
        if args.force_restart:
            _force_restart_launch_agent()
            return 0
        if args.uninstall_launch_agent:
            _uninstall_launch_agent()
            return 0
        return _status_launch_agent()
    except QuarkLifecycleError as exc:
        parser.error(str(exc))
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
