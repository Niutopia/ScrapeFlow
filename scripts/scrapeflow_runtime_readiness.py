#!/usr/bin/env python3
"""Check a started ScrapeFlow API before opening isolated acceptance."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from local.scrapeflow_api.runtime_readiness import runtime_readiness_report  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--api-url",
        default="http://127.0.0.1:8765",
        help="loopback ScrapeFlow API URL",
    )
    parser.add_argument(
        "--expected-commit",
        default="",
        help="expected build commit from /api/health; prefix matches are accepted",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=5.0,
        help="HTTP timeout in seconds",
    )
    parser.add_argument(
        "--allow-existing-jobs",
        action="store_true",
        help="allow completed or queued public jobs when checking an already-used isolated run",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="print the readiness report as JSON",
    )
    args = parser.parse_args(argv)
    report = runtime_readiness_report(
        api_url=args.api_url,
        expected_commit=args.expected_commit or None,
        timeout=args.timeout,
        allow_existing_jobs=args.allow_existing_jobs,
    )
    if args.json:
        print(json.dumps(report, ensure_ascii=False, indent=2))
    elif report["status"] == "通过":
        print("runtime readiness passed")
    else:
        print("runtime readiness failed:", file=sys.stderr)
        for issue in report["issues"]:
            print(f"  - {issue}", file=sys.stderr)
    return 0 if report["status"] == "通过" else 1


if __name__ == "__main__":
    raise SystemExit(main())
