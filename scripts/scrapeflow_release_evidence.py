#!/usr/bin/env python3
"""Capture ScrapeFlow release-check output for the acceptance package."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from local.scrapeflow_api.release_evidence import capture_release_evidence  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output-dir",
        type=Path,
        required=True,
        help="directory for scrapeflow-release-evidence.json and the raw release log",
    )
    parser.add_argument(
        "--skip-docker",
        action="store_true",
        help="skip compose config and Docker build in the wrapped release check",
    )
    args = parser.parse_args(argv)
    report = capture_release_evidence(
        args.output_dir,
        include_docker=not args.skip_docker,
    )
    print(report["report_path"])
    return int(report["returncode"])


if __name__ == "__main__":
    raise SystemExit(main())
