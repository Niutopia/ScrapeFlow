#!/usr/bin/env python3
"""Manual ScrapeFlow offline backup helper."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from local.scrapeflow_api.offline_backup import (  # noqa: E402
    OfflineBackupError,
    create_offline_backup,
    restore_offline_backup,
    verify_offline_backup,
)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Create or verify a stopped ScrapeFlow local-state backup.",
    )
    subcommands = parser.add_subparsers(dest="command", required=True)

    create = subcommands.add_parser("create", help="copy stopped local state")
    create.add_argument("--alist-data", required=True)
    create.add_argument("--scrapeflow-data", required=True)
    create.add_argument("--output-dir", required=True)
    create.add_argument("--media-snapshot-note", required=True)
    create.add_argument("--label")

    verify = subcommands.add_parser("verify", help="re-run backup checks")
    verify.add_argument("backup_dir")

    restore = subcommands.add_parser("restore", help="copy into an isolated directory")
    restore.add_argument("backup_dir")
    restore.add_argument("--restore-dir", required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "create":
            result = create_offline_backup(
                alist_data=args.alist_data,
                scrapeflow_data=args.scrapeflow_data,
                output_dir=args.output_dir,
                media_snapshot_note=args.media_snapshot_note,
                label=args.label,
            )
        elif args.command == "verify":
            result = verify_offline_backup(args.backup_dir)
        else:
            result = restore_offline_backup(
                backup_dir=args.backup_dir,
                restore_dir=args.restore_dir,
            )
    except OfflineBackupError as exc:
        print(str(exc), file=sys.stderr)
        return 2
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
