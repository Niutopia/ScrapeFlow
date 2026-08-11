#!/usr/bin/env python3
"""Run or install the passive host-side ScrapeFlow Quark Helper.

The LaunchAgent managed here starts only this narrow four-action Helper.  The
Helper still attaches passively to the historical fixed Quark CDP endpoint and
has no capability to launch, restart, activate, or click the desktop app.
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path
import plistlib
import secrets
import socket
import stat
import subprocess
import sys
import tempfile
import time


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from local.scrapeflow_api.quark_host_helper import (
    DEFAULT_MOUNT_PATH,
    DEFAULT_STAGING_ROOT,
    PassiveQuarkCdp,
    QuarkHelperConfig,
    QuarkHelperValidationError,
    load_helper_token,
    serve_quark_helper,
)


DEFAULT_HELPER_PORT = 18765
DEFAULT_CDP_URL = "http://127.0.0.1:19222/json/list"
DEFAULT_STATE_DIR_NAME = ".scrapeflow-quark-helper"
LAUNCH_AGENT_LABEL = "com.scrapeflow.quark-native-helper"
LAUNCH_AGENT_READY_TIMEOUT = 45.0


class QuarkHelperCliError(RuntimeError):
    """A non-sensitive Helper lifecycle failure suitable for CLI output."""


def _integer_port(value: str) -> int:
    try:
        port = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("port must be an integer") from exc
    if not (1 <= port <= 65535):
        raise argparse.ArgumentTypeError("port must be in 1..65535")
    return port


def _default_state_dir() -> Path:
    configured = os.getenv("SCRAPEFLOW_QUARK_HELPER_STATE_DIR", "").strip()
    return Path(configured).expanduser() if configured else Path.home() / DEFAULT_STATE_DIR_NAME


def _launch_agent_path() -> Path:
    return Path.home() / "Library" / "LaunchAgents" / f"{LAUNCH_AGENT_LABEL}.plist"


def _launchctl_domain() -> str:
    return f"gui/{os.getuid()}"


def _launchctl_service() -> str:
    return f"{_launchctl_domain()}/{LAUNCH_AGENT_LABEL}"


def _run_launchctl(
    *arguments: str,
    check: bool = True,
) -> subprocess.CompletedProcess[str]:
    try:
        return subprocess.run(
            ["launchctl", *arguments],
            check=check,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
    except (OSError, subprocess.CalledProcessError) as exc:
        detail = ""
        if isinstance(exc, subprocess.CalledProcessError):
            detail = str(exc.stderr or exc.stdout or "").strip()[:240]
        raise QuarkHelperCliError(
            f"launchctl {' '.join(arguments)} failed"
            + (f": {detail}" if detail else "")
        ) from exc


def _bootout_launch_agent() -> None:
    # Missing jobs are the normal first-install and already-uninstalled states.
    _run_launchctl("bootout", _launchctl_service(), check=False)


def _ensure_private_state_dir(state_dir: Path) -> None:
    """Create or tighten the Helper state directory to current-user 0700."""

    try:
        if not state_dir.exists() and not state_dir.is_symlink():
            state_dir.mkdir(parents=True, mode=0o700)
        state_stat = state_dir.lstat()
        if stat.S_ISLNK(state_stat.st_mode) or not stat.S_ISDIR(state_stat.st_mode):
            raise QuarkHelperCliError("helper state directory must be a real directory")
        if state_stat.st_uid != os.getuid():
            raise QuarkHelperCliError(
                "helper state directory must be owned by the current user"
            )
        if stat.S_IMODE(state_stat.st_mode) != 0o700:
            os.chmod(state_dir, 0o700)
    except QuarkHelperCliError:
        raise
    except OSError as exc:
        raise QuarkHelperCliError("helper state directory is unavailable") from exc


def _ensure_token_file(token_file: Path) -> None:
    """Create the LaunchAgent secret once, with mode 0600 and no overwrite."""

    try:
        if token_file.exists() or token_file.is_symlink():
            token_stat = token_file.lstat()
            mode = stat.S_IMODE(token_stat.st_mode)
            if stat.S_ISLNK(token_stat.st_mode) or not stat.S_ISREG(token_stat.st_mode):
                raise QuarkHelperCliError(
                    "helper token file must be a regular file"
                )
            if token_stat.st_uid != os.getuid():
                raise QuarkHelperCliError("helper token file must be owned by the current user")
            if token_stat.st_nlink != 1:
                raise QuarkHelperCliError("helper token file must have exactly one link")
            if mode & 0o077:
                raise QuarkHelperCliError(
                    "helper token file must be inaccessible to group/other"
                )
            if len(token_file.read_text(encoding="utf-8").strip()) < 24:
                raise QuarkHelperCliError(
                    "helper token file must contain at least 24 characters"
                )
            return
        token_file.parent.mkdir(parents=True, exist_ok=True)
        descriptor = os.open(
            token_file,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL,
            0o600,
        )
        try:
            os.write(descriptor, (secrets.token_urlsafe(36) + "\n").encode("ascii"))
        finally:
            os.close(descriptor)
    except QuarkHelperCliError:
        raise
    except OSError as exc:
        raise QuarkHelperCliError("helper token file is unavailable") from exc


def _launch_agent_is_running() -> bool:
    """Check the exact launchd job without invoking the Helper HTTP surface."""

    try:
        result = _run_launchctl("print", _launchctl_service())
    except QuarkHelperCliError:
        return False
    return any(line.strip() == "state = running" for line in result.stdout.splitlines())


def _helper_port_is_bound(port: int, *, timeout: float) -> bool:
    """Open and close one loopback TCP connection without sending HTTP bytes."""

    try:
        with socket.create_connection(("127.0.0.1", port), timeout=timeout):
            return True
    except OSError:
        return False


def _wait_helper_alive(
    port: int,
    *,
    timeout: float = LAUNCH_AGENT_READY_TIMEOUT,
) -> None:
    """Wait only for launchd state and a TCP bind, never for Quark readiness."""

    deadline = time.monotonic() + timeout
    last_error = "launch agent is not running"
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise QuarkHelperCliError(
                f"launch agent helper did not become alive within {timeout:g}s: {last_error}"
            )
        if _launch_agent_is_running():
            if _helper_port_is_bound(port, timeout=min(1.0, remaining)):
                return
            last_error = f"launch agent is running but 127.0.0.1:{port} is not bound"
        time.sleep(min(0.25, max(0.0, deadline - time.monotonic())))


def _reject_unsafe_launch_agent_target(target: Path) -> None:
    """Refuse to replace a symlink or other non-regular plist target."""

    try:
        target_stat = target.lstat()
    except FileNotFoundError:
        return
    except OSError as exc:
        raise QuarkHelperCliError("could not inspect Helper LaunchAgent plist") from exc
    if stat.S_ISLNK(target_stat.st_mode) or not stat.S_ISREG(target_stat.st_mode):
        raise QuarkHelperCliError("Helper LaunchAgent plist target is unsafe")


def _write_launch_agent_plist(target: Path, payload: dict[str, object]) -> None:
    """Write a private, exclusive same-directory temporary and atomically replace."""

    temporary: Path | None = None
    descriptor: int | None = None
    try:
        descriptor, temporary_name = tempfile.mkstemp(
            dir=target.parent,
            prefix=f".{LAUNCH_AGENT_LABEL}.",
            suffix=".plist.tmp",
        )
        temporary = Path(temporary_name)
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "wb") as output:
            descriptor = None
            plistlib.dump(payload, output, sort_keys=True)
            output.flush()
            os.fsync(output.fileno())
        _reject_unsafe_launch_agent_target(target)
        os.replace(temporary, target)
        temporary = None
    except OSError as exc:
        raise QuarkHelperCliError("could not write Helper LaunchAgent plist") from exc
    finally:
        if descriptor is not None:
            os.close(descriptor)
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def _validate_install_config(args: argparse.Namespace) -> QuarkHelperConfig:
    """Validate the exact installed service configuration without network I/O."""

    # The LaunchAgent receives a token-file argument and does not embed a
    # secret environment variable.  Ignore the installer's shell environment
    # here so validation matches the durable configuration without leaking the
    # token into the plist or launchctl arguments.
    token = load_helper_token(token_file=args.token_file, environ={})
    config = QuarkHelperConfig(
        host="127.0.0.1",
        port=args.port,
        token=token,
        cdp_url=args.cdp_url,
        staging_root=args.staging_root,
        mount_path=args.mount_path,
        root_fid=args.root_fid,
    )
    # QuarkHelperConfig owns the public configuration validation; the passive
    # runtime constructor additionally enforces that staging_root is inside
    # mount_path.  Construction is pure and does not contact CDP or Quark.
    PassiveQuarkCdp(
        cdp_url=config.cdp_url,
        staging_root=config.staging_root,
        mount_path=config.mount_path,
        root_fid=config.root_fid,
        timeout_seconds=config.timeout_seconds,
    )
    return config


def _install_launch_agent(args: argparse.Namespace) -> Path:
    _ensure_private_state_dir(args.state_dir)
    _ensure_token_file(args.token_file)
    _validate_install_config(args)
    target = _launch_agent_path()
    target.parent.mkdir(parents=True, exist_ok=True)
    _reject_unsafe_launch_agent_target(target)
    program_arguments = [
        str(Path(sys.executable).resolve()),
        str(Path(__file__).resolve()),
        "--host",
        "127.0.0.1",
        "--port",
        str(args.port),
        "--cdp-url",
        args.cdp_url,
        "--staging-root",
        args.staging_root,
        "--mount-path",
        args.mount_path,
        "--root-fid",
        args.root_fid,
        "--state-dir",
        str(args.state_dir.resolve()),
        "--token-file",
        str(args.token_file.resolve()),
    ]
    payload = {
        "Label": LAUNCH_AGENT_LABEL,
        "ProgramArguments": program_arguments,
        "RunAtLoad": True,
        "KeepAlive": {"Crashed": True},
        "ProcessType": "Background",
        "StandardOutPath": "/dev/null",
        "StandardErrorPath": "/dev/null",
    }

    # Stop a loaded historical job before replacing its plist.  This also
    # removes any legacy active Quark arguments before the typed service starts.
    _bootout_launch_agent()
    _write_launch_agent_plist(target, payload)
    _run_launchctl("bootstrap", _launchctl_domain(), str(target))
    _run_launchctl("kickstart", "-k", _launchctl_service())
    _wait_helper_alive(args.port)
    return target


def _uninstall_launch_agent() -> None:
    _bootout_launch_agent()
    try:
        _launch_agent_path().unlink(missing_ok=True)
    except OSError as exc:
        raise QuarkHelperCliError("could not remove Helper LaunchAgent plist") from exc


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Passive loopback helper for an already-running Quark CDP renderer",
    )
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument(
        "--port",
        default=os.getenv("SCRAPEFLOW_QUARK_HELPER_PORT", str(DEFAULT_HELPER_PORT)),
        type=_integer_port,
    )
    parser.add_argument(
        "--cdp-url",
        default=(
            os.getenv("SCRAPEFLOW_QUARK_HELPER_CDP_URL", "").strip()
            or DEFAULT_CDP_URL
        ),
        help="fixed existing loopback CDP /json/list URL; no port discovery occurs",
    )
    parser.add_argument(
        "--staging-root",
        default=os.getenv("SCRAPEFLOW_QUARK_HELPER_STAGING_ROOT", DEFAULT_STAGING_ROOT),
    )
    parser.add_argument(
        "--mount-path",
        default=os.getenv("SCRAPEFLOW_QUARK_HELPER_MOUNT_PATH", DEFAULT_MOUNT_PATH),
    )
    parser.add_argument(
        "--root-fid",
        default=os.getenv("SCRAPEFLOW_QUARK_HELPER_ROOT_FID", "0"),
    )
    parser.add_argument("--state-dir", type=Path, default=_default_state_dir())
    parser.add_argument(
        "--token-file",
        type=Path,
        default=(
            Path(os.environ["SCRAPEFLOW_QUARK_HELPER_TOKEN_FILE"])
            if os.getenv("SCRAPEFLOW_QUARK_HELPER_TOKEN_FILE")
            else None
        ),
    )
    actions = parser.add_mutually_exclusive_group()
    actions.add_argument("--install-launch-agent", action="store_true")
    actions.add_argument("--uninstall-launch-agent", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _parser()
    args = parser.parse_args(argv)
    args.state_dir = args.state_dir.expanduser()
    if args.token_file is None:
        args.token_file = args.state_dir / "token"
    else:
        args.token_file = args.token_file.expanduser()
    if args.host not in {"127.0.0.1", "::1"}:
        parser.error("helper may bind only to 127.0.0.1 or ::1")
    try:
        if args.install_launch_agent:
            _install_launch_agent(args)
            return 0
        if args.uninstall_launch_agent:
            _uninstall_launch_agent()
            return 0
    except (QuarkHelperCliError, QuarkHelperValidationError) as exc:
        parser.error(str(exc))
    try:
        token = load_helper_token(token_file=args.token_file)
        config = QuarkHelperConfig(
            host=args.host,
            port=args.port,
            token=token,
            cdp_url=args.cdp_url,
            staging_root=args.staging_root,
            mount_path=args.mount_path,
            root_fid=args.root_fid,
        )
    except QuarkHelperValidationError as exc:
        parser.error(str(exc))
    serve_quark_helper(config)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
