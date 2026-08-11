#!/usr/bin/env python3
"""Run the passive typed ScrapeFlow Quark Helper.

The process may run directly against an explicit loopback CDP endpoint or as
a Docker Compose sidecar against the one fixed Docker Desktop host bridge.
Both modes require the local AList credentials because typed actions resolve a
short-lived Quark Cookie/root per request.  It only starts the narrow
four-action HTTP service: it never manages the Quark process, its windows, or
desktop UI.
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path
import sys


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from local.scrapeflow_api.quark_host_helper import (
    DEFAULT_MOUNT_PATH,
    DEFAULT_STAGING_ROOT,
    DOCKER_SIDECAR_CDP_URL,
    QuarkHelperConfig,
    QuarkHelperValidationError,
    load_helper_token,
    serve_quark_helper,
)


DEFAULT_HELPER_PORT = 18765
DEFAULT_CDP_URL = "http://127.0.0.1:19222/json/list"
DEFAULT_STATE_DIR_NAME = ".scrapeflow-quark-helper"


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


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Passive typed helper for an already-running Quark CDP renderer",
    )
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument(
        "--port",
        default=os.getenv("SCRAPEFLOW_QUARK_HELPER_PORT", str(DEFAULT_HELPER_PORT)),
        type=_integer_port,
    )
    parser.add_argument(
        "--cdp-url",
        default=None,
        help=(
            "explicit existing CDP /json/list URL; defaults to loopback, or to "
            "the fixed Docker Desktop bridge with --docker-sidecar"
        ),
    )
    parser.add_argument(
        "--docker-sidecar",
        action="store_true",
        help="use only the fixed host.docker.internal:19222 Docker Desktop CDP bridge",
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
    return parser


def _configured_cdp_url(args: argparse.Namespace) -> str:
    explicit = args.cdp_url
    if explicit is None:
        explicit = os.getenv("SCRAPEFLOW_QUARK_HELPER_CDP_URL", "").strip() or None
    if explicit is not None:
        return explicit
    return DOCKER_SIDECAR_CDP_URL if args.docker_sidecar else DEFAULT_CDP_URL


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
        token = load_helper_token(token_file=args.token_file)
        config = QuarkHelperConfig(
            host=args.host,
            port=args.port,
            token=token,
            cdp_url=_configured_cdp_url(args),
            alist_url=os.getenv("ALIST_URL", "").strip(),
            alist_username=os.getenv("ALIST_USERNAME", "").strip(),
            # Preserve the password byte-for-byte and leave all validation and
            # redaction inside the typed config/session resolver.  In
            # particular, never place it in argparse values or diagnostics.
            alist_password=os.getenv("ALIST_PASSWORD", ""),
            staging_root=args.staging_root,
            mount_path=args.mount_path,
            root_fid=args.root_fid,
            docker_sidecar=args.docker_sidecar,
        )
    except QuarkHelperValidationError as exc:
        parser.error(str(exc))
    serve_quark_helper(config)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
