#!/usr/bin/env python3
"""Plan, apply or restore retirement of legacy resident one-time owners."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
from typing import Sequence


PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from local.scrapeflow_api.legacy_one_time_migration import (  # noqa: E402
    LegacyOneTimeMigrationError,
    apply_retirement_plan,
    build_retirement_plan,
    read_retirement_plan,
    restore_retirement_plan,
    write_retirement_plan,
)


def parser() -> argparse.ArgumentParser:
    value = argparse.ArgumentParser(description=__doc__)
    value.add_argument(
        "--state-root", type=Path, required=True,
        help="live scrapeflow-data directory containing global-control.json and jobs/",
    )
    mode = value.add_mutually_exclusive_group()
    mode.add_argument(
        "--apply", action="store_true",
        help="apply a previously sealed plan (planning is the default)",
    )
    mode.add_argument(
        "--restore", action="store_true",
        help="restore jobs from a previously applied sealed plan",
    )
    value.add_argument(
        "--backup-manifest", type=Path,
        help="latest verified host-state backup manifest (planning only)",
    )
    value.add_argument(
        "--expected-active-count", type=int,
        help="operator-supplied exact non-terminal owner count (planning only)",
    )
    value.add_argument(
        "--expected-terminal-count", type=int,
        help="operator-supplied exact terminal owner count (planning only)",
    )
    value.add_argument(
        "--plan-output", type=Path,
        help="write the sealed plan outside live state (planning only)",
    )
    value.add_argument(
        "--plan", type=Path,
        help="sealed plan to apply or restore",
    )
    value.add_argument(
        "--approve-plan-sha256",
        help="explicitly approve the plan's full SHA-256 for apply/restore",
    )
    value.add_argument(
        "--confirm-api-stopped", action="store_true",
        help="assert the Local API and all workers are stopped",
    )
    return value


def _same_root(requested: Path, plan_root: str) -> bool:
    return requested.expanduser().resolve(strict=True) == Path(plan_root).resolve(strict=True)


def _plan_summary(plan: dict, *, destination: Path | None = None) -> dict:
    """Never expose sealed job JSON/base64 on stdout."""
    return {
        "mode": "planned" if destination is not None else "dry-run",
        "mutation_performed": False,
        "active_owner_count": plan["active_owner_count"],
        "terminal_owner_count": plan["terminal_owner_count"],
        "total_owner_count": plan["total_owner_count"],
        "descendant_count": plan["descendant_count"],
        "total_entry_count": plan["total_entry_count"],
        "migration_id": plan["migration_id"],
        "plan": str(destination) if destination is not None else None,
        "plan_sha256": plan["plan_sha256"],
    }


def run(argv: Sequence[str] | None = None) -> dict:
    arguments = parser().parse_args(argv)
    changing = arguments.apply or arguments.restore
    if changing:
        if arguments.plan is None:
            raise LegacyOneTimeMigrationError("--apply/--restore requires --plan")
        if (
            arguments.backup_manifest is not None
            or arguments.expected_active_count is not None
            or arguments.expected_terminal_count is not None
        ):
            raise LegacyOneTimeMigrationError(
                "backup/count inputs belong to planning; apply the sealed plan unchanged"
            )
        if arguments.plan_output is not None:
            raise LegacyOneTimeMigrationError("--plan-output cannot be combined with apply/restore")
        plan = read_retirement_plan(arguments.plan)
        if not _same_root(arguments.state_root, str(plan["state_root"])):
            raise LegacyOneTimeMigrationError("--state-root differs from the sealed plan")
        if arguments.apply:
            return apply_retirement_plan(
                plan,
                approved_sha256=str(arguments.approve_plan_sha256 or ""),
                confirm_api_stopped=arguments.confirm_api_stopped,
            )
        return restore_retirement_plan(
            plan,
            approved_sha256=str(arguments.approve_plan_sha256 or ""),
            confirm_api_stopped=arguments.confirm_api_stopped,
        )

    if arguments.plan is not None or arguments.approve_plan_sha256 is not None:
        raise LegacyOneTimeMigrationError(
            "--plan/--approve-plan-sha256 are only valid for apply/restore"
        )
    if arguments.confirm_api_stopped:
        raise LegacyOneTimeMigrationError(
            "planning is read-only and does not accept --confirm-api-stopped"
        )
    if (
        arguments.backup_manifest is None
        or arguments.expected_active_count is None
        or arguments.expected_terminal_count is None
    ):
        raise LegacyOneTimeMigrationError(
            "planning requires --backup-manifest, --expected-active-count "
            "and --expected-terminal-count"
        )
    plan = build_retirement_plan(
        arguments.state_root,
        arguments.backup_manifest,
        expected_active_count=arguments.expected_active_count,
        expected_terminal_count=arguments.expected_terminal_count,
    )
    if arguments.plan_output is None:
        return _plan_summary(plan)
    destination = write_retirement_plan(arguments.plan_output, plan)
    return _plan_summary(plan, destination=destination)


def main(argv: Sequence[str] | None = None) -> int:
    try:
        result = run(argv)
    except (LegacyOneTimeMigrationError, OSError) as exc:
        print(f"legacy one-time retirement refused: {exc}", file=sys.stderr)
        return 2
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
