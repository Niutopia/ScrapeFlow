#!/usr/bin/env python3
"""Run the local backend release checks."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from local.scrapeflow_api.release_checks import run_release_checks  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--skip-docker",
        action="store_true",
        help="skip compose config and Docker build; keep local static checks",
    )
    args = parser.parse_args(argv)
    return run_release_checks(include_docker=not args.skip_docker)


if __name__ == "__main__":
    raise SystemExit(main())
