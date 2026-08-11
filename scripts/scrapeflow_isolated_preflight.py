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

from engine.scrapeflow.serialization import atomic_write_json  # noqa: E402
from local.scrapeflow_api.isolated_preflight import (  # noqa: E402
    capture_isolated_preflight_report,
    isolated_preflight_issues,
    isolated_preflight_template,
    load_declaration,
    validate_isolated_preflight_report,
)


def _path_is_within(path: Path, parent: Path) -> bool:
    try:
        path.relative_to(parent)
        return True
    except ValueError:
        return False


def _report_path_issue(
    report_path: Path,
    declaration_path: Path,
    declaration: dict[str, object],
) -> str | None:
    """Keep the evidence output away from every input and runtime root."""
    if report_path.is_symlink():
        return "report path must not be a symlink"
    if report_path.exists():
        return "report path must not already exist"
    parent = report_path.expanduser().parent.resolve(strict=True)
    if not parent.is_dir():
        return "report parent must be an existing directory"
    resolved = report_path.expanduser().resolve(strict=False)
    protected_files = [declaration_path.expanduser().resolve(strict=True)]
    for key in ("offline_backup_manifest", "media_recovery_point"):
        value = declaration.get(key)
        if not isinstance(value, str) or not value.strip():
            continue
        candidate = Path(value).expanduser()
        if not candidate.is_absolute():
            continue
        protected_files.append(candidate.resolve(strict=False))
    if resolved in protected_files:
        return "report path must differ from every declaration evidence input"

    protected_roots: list[Path] = []
    for key in ("scrapeflow_state_dir", "alist_data_dir"):
        value = declaration.get(key)
        if isinstance(value, str) and value.strip():
            protected_roots.append(Path(value).expanduser().resolve(strict=False))
    manifest = declaration.get("offline_backup_manifest")
    if isinstance(manifest, str) and manifest.strip():
        protected_roots.append(Path(manifest).expanduser().resolve(strict=False).parent)
    if any(_path_is_within(resolved, protected) for protected in protected_roots):
        return "report path must be outside runtime directories and the backup root"
    return None


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
    parser.add_argument(
        "--report",
        type=Path,
        help=(
            "atomically write the structured preflight report here; the report "
            "captures the one-time empty-directory check"
        ),
    )
    args = parser.parse_args(argv)
    if args.template and (args.declaration is not None or args.report is not None):
        print("template mode cannot be combined with a declaration or --report", file=sys.stderr)
        return 2
    if args.template:
        print(json.dumps(isolated_preflight_template(), ensure_ascii=False, indent=2))
        return 0
    if args.declaration is None:
        parser.error("declaration is required unless --template is used")
    declaration_path = args.declaration.expanduser().resolve(strict=False)
    report_path = (
        None
        if args.report is None
        else args.report.expanduser().resolve(strict=False)
    )
    try:
        declaration = load_declaration(declaration_path)
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        print(f"preflight declaration error: {exc}", file=sys.stderr)
        return 2
    if report_path is not None:
        try:
            path_issue = _report_path_issue(
                report_path,
                declaration_path,
                declaration,
            )
            if path_issue is not None:
                raise ValueError(path_issue)
            report_parent = report_path.parent.resolve(strict=True)
            parent_stat = report_parent.stat()
            report = capture_isolated_preflight_report(declaration)
            atomic_write_json(report_path, report, allow_nan=False)
            current_parent = report_parent.stat()
            if (
                current_parent.st_dev != parent_stat.st_dev
                or current_parent.st_ino != parent_stat.st_ino
            ):
                raise ValueError("report parent changed while evidence was written")
            if report["status"] == "passed":
                try:
                    validate_isolated_preflight_report(
                        report,
                        require_passed=True,
                        require_transient_checks=True,
                    )
                except ValueError as exc:
                    failed_report = dict(report)
                    failed_report["status"] = "failed"
                    failed_report["issues"] = [f"post-write transient check failed: {exc}"]
                    failed_report["checks"] = {}
                    atomic_write_json(report_path, failed_report, allow_nan=False)
                    raise
        except (OSError, TypeError, ValueError) as exc:
            print(f"preflight report error: {exc}", file=sys.stderr)
            return 2
        issues = report["issues"]
    else:
        issues = isolated_preflight_issues(declaration)
    if issues:
        print("isolated acceptance preflight failed:", file=sys.stderr)
        for issue in issues:
            print(f"  - {issue}", file=sys.stderr)
        if report_path is not None:
            print(f"captured report: {report_path}", file=sys.stderr)
        return 1
    if report_path is not None:
        print(f"isolated acceptance preflight passed; captured report: {report_path}")
    else:
        print("isolated acceptance preflight passed (live check; no report written)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
