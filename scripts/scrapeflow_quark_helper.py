#!/usr/bin/env python3
"""Run the passive host-side ScrapeFlow Quark Helper.

This CLI deliberately has no daemon installer and no Quark process/UI options.
It requires an explicitly supplied existing DevTools endpoint; when Quark does
not expose one, stop here and leave the desktop application untouched.
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
    QuarkHelperConfig,
    QuarkHelperValidationError,
    load_helper_token,
    serve_quark_helper,
)


def _integer_port(value: str) -> int:
    try:
        port = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("port must be an integer") from exc
    if not (1 <= port <= 65535):
        raise argparse.ArgumentTypeError("port must be in 1..65535")
    return port


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Passive loopback helper for an already-running Quark CDP renderer",
    )
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument(
        "--port",
        default=os.getenv("SCRAPEFLOW_QUARK_HELPER_PORT", "8766"),
        type=_integer_port,
    )
    parser.add_argument(
        "--cdp-url",
        default=os.getenv("SCRAPEFLOW_QUARK_HELPER_CDP_URL", "").strip(),
        help="explicit existing loopback CDP /json/list URL; no port discovery occurs",
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
    parser.add_argument(
        "--token-file",
        type=Path,
        default=(
            Path(os.environ["SCRAPEFLOW_QUARK_HELPER_TOKEN_FILE"])
            if os.getenv("SCRAPEFLOW_QUARK_HELPER_TOKEN_FILE") else None
        ),
    )
    args = parser.parse_args(argv)
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
