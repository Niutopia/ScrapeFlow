#!/usr/bin/env python3
"""Generate a local ScrapeFlow acceptance-package draft."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from local.scrapeflow_api.acceptance_package import build_acceptance_package  # noqa: E402
from local.scrapeflow_api.isolated_preflight import load_declaration  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output",
        type=Path,
        help="write the package draft to this Markdown file; stdout is used when omitted",
    )
    parser.add_argument(
        "--release-check-status",
        default="未执行",
        help="record the release-check evidence status, for example: 通过",
    )
    parser.add_argument(
        "--backup-manifest",
        default="",
        help="record the offline backup manifest path after a real backup verify",
    )
    parser.add_argument(
        "--media-recovery-point",
        default="",
        help="record the external formal media-library recovery point",
    )
    parser.add_argument(
        "--preflight-declaration",
        type=Path,
        help="include stage-10 isolated preflight declaration status in the package",
    )
    parser.add_argument(
        "--runtime-readiness-report",
        type=Path,
        help="include JSON from scripts/scrapeflow_runtime_readiness.py --json",
    )
    args = parser.parse_args(argv)
    declaration = None
    if args.preflight_declaration is not None:
        try:
            declaration = load_declaration(args.preflight_declaration)
        except (OSError, ValueError) as exc:
            print(f"preflight declaration error: {exc}", file=sys.stderr)
            return 2
    readiness = None
    if args.runtime_readiness_report is not None:
        try:
            readiness = json.loads(args.runtime_readiness_report.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            print(f"runtime readiness report error: {exc}", file=sys.stderr)
            return 2
        if not isinstance(readiness, dict):
            print("runtime readiness report error: report must be a JSON object", file=sys.stderr)
            return 2
    package = build_acceptance_package(
        release_check_status=args.release_check_status,
        backup_manifest=args.backup_manifest,
        media_recovery_point=args.media_recovery_point,
        isolated_declaration=declaration,
        runtime_readiness=readiness,
    )
    if args.output is None:
        print(package)
        return 0
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(package, encoding="utf-8")
    print(args.output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
