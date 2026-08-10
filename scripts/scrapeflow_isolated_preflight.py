#!/usr/bin/env python3
"""Validate a ScrapeFlow isolated-acceptance declaration."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from local.scrapeflow_api.isolated_preflight import (  # noqa: E402
    isolated_preflight_issues,
    isolated_preflight_template,
    load_declaration,
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "declaration",
        nargs="?",
        type=Path,
        help="JSON declaration to validate",
    )
    parser.add_argument(
        "--template",
        action="store_true",
        help="print a JSON declaration template and exit",
    )
    args = parser.parse_args(argv)
    if args.template:
        print(json.dumps(isolated_preflight_template(), ensure_ascii=False, indent=2))
        return 0
    if args.declaration is None:
        parser.error("declaration is required unless --template is used")
    try:
        declaration = load_declaration(args.declaration)
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        print(f"preflight declaration error: {exc}", file=sys.stderr)
        return 2
    issues = isolated_preflight_issues(declaration)
    if issues:
        print("isolated acceptance preflight failed:", file=sys.stderr)
        for issue in issues:
            print(f"  - {issue}", file=sys.stderr)
        return 1
    print("isolated acceptance preflight passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
