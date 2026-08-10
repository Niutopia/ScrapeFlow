#!/usr/bin/env python3
"""Generate a local ScrapeFlow acceptance-package draft."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from local.scrapeflow_api.acceptance_package import build_acceptance_package  # noqa: E402


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
    args = parser.parse_args(argv)
    package = build_acceptance_package(
        release_check_status=args.release_check_status,
        backup_manifest=args.backup_manifest,
        media_recovery_point=args.media_recovery_point,
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
