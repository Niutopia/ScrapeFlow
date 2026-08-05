#!/usr/bin/env python3
"""Run unittest discovery and emit one machine-readable result object."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start", required=True)
    parser.add_argument("--pattern", default="test*.py")
    args = parser.parse_args()
    started_at = datetime.now(timezone.utc).isoformat()
    suite = unittest.defaultTestLoader.discover(args.start, pattern=args.pattern)
    result = unittest.TextTestRunner(stream=sys.stderr, verbosity=1).run(suite)
    failed = len(result.failures) + len(result.errors) + len(result.unexpectedSuccesses)
    payload = {
        "schema_version": 1,
        "kind": "unittest_result",
        "started_at": started_at,
        "finished_at": datetime.now(timezone.utc).isoformat(),
        "tests": result.testsRun,
        "passed": result.testsRun - failed - len(result.skipped),
        "failed": failed,
        "skipped": len(result.skipped),
        "successful": result.wasSuccessful(),
    }
    print(json.dumps(payload, separators=(",", ":")))
    return 0 if result.wasSuccessful() and result.testsRun > 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
