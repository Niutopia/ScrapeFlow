#!/usr/bin/env python3
"""Generate a local ScrapeFlow acceptance-package draft."""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from local.scrapeflow_api.acceptance_package import build_acceptance_package  # noqa: E402
from engine.scrapeflow.serialization import atomic_write_bytes  # noqa: E402
from local.scrapeflow_api.isolated_preflight import (  # noqa: E402
    load_declaration,
    load_isolated_preflight_report,
)


def _paths_alias(output: Path, candidate: Path) -> bool:
    output_resolved = output.expanduser().resolve(strict=False)
    candidate_resolved = candidate.expanduser().resolve(strict=False)
    if output_resolved == candidate_resolved:
        return True
    if output.exists() and candidate.exists():
        try:
            return os.path.samefile(output, candidate)
        except OSError:
            return False
    return False


def _path_is_within(path: Path, parent: Path) -> bool:
    try:
        path.relative_to(parent)
        return True
    except ValueError:
        return False


def _output_path_issue(
    args: argparse.Namespace,
    captured_declaration: dict[str, object] | None = None,
) -> str | None:
    output = args.output
    if output is None:
        return None
    if output.is_symlink():
        return "output path must not be a symlink"
    inputs = [
        value
        for value in (
            args.release_evidence,
            args.preflight_declaration,
            args.preflight_report,
            args.runtime_readiness_report,
        )
        if isinstance(value, Path)
    ]
    for raw in (args.backup_manifest, args.media_recovery_point):
        if isinstance(raw, str) and raw.strip():
            candidate = Path(raw).expanduser()
            if candidate.is_absolute() or candidate.exists():
                inputs.append(candidate)
    protected_roots: list[Path] = []
    if isinstance(captured_declaration, dict):
        for key in ("scrapeflow_state_dir", "alist_data_dir"):
            value = captured_declaration.get(key)
            if isinstance(value, str) and value.strip():
                protected_roots.append(Path(value).expanduser().resolve(strict=False))
        for key in ("offline_backup_manifest", "media_recovery_point"):
            value = captured_declaration.get(key)
            if not isinstance(value, str) or not value.strip():
                continue
            candidate = Path(value).expanduser()
            if candidate.is_absolute() or candidate.exists():
                inputs.append(candidate)
                if key == "offline_backup_manifest":
                    protected_roots.append(candidate.resolve(strict=False).parent)
    if any(_paths_alias(output, candidate) for candidate in inputs):
        return "output path must differ from every evidence input"
    resolved_output = output.expanduser().resolve(strict=False)
    if any(_path_is_within(resolved_output, root) for root in protected_roots):
        return "output path must be outside captured runtime directories and the backup root"
    return None


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
        "--release-evidence",
        type=Path,
        help="include JSON from scripts/scrapeflow_release_evidence.py --output-dir ...",
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
        help=(
            "legacy compatibility: live-recheck a stage-10 declaration; when used "
            "with --preflight-report it must exactly match the captured declaration"
        ),
    )
    parser.add_argument(
        "--preflight-report",
        type=Path,
        help=(
            "include a passing report captured before runtime start without "
            "rerunning the one-time empty-directory checks"
        ),
    )
    parser.add_argument(
        "--runtime-readiness-report",
        type=Path,
        help="include JSON from scripts/scrapeflow_runtime_readiness.py --json",
    )
    args = parser.parse_args(argv)
    for attribute in (
        "output",
        "release_evidence",
        "preflight_declaration",
        "preflight_report",
        "runtime_readiness_report",
    ):
        value = getattr(args, attribute)
        if isinstance(value, Path):
            setattr(args, attribute, value.expanduser().resolve(strict=False))
    output_issue = _output_path_issue(args)
    if output_issue is not None:
        print(f"output error: {output_issue}", file=sys.stderr)
        return 2
    declaration = None
    if args.preflight_declaration is not None:
        try:
            declaration = load_declaration(args.preflight_declaration)
        except (OSError, ValueError) as exc:
            print(f"preflight declaration error: {exc}", file=sys.stderr)
            return 2
        output_issue = _output_path_issue(args, declaration)
        if output_issue is not None:
            print(f"output error: {output_issue}", file=sys.stderr)
            return 2
    preflight_report = None
    if args.preflight_report is not None:
        try:
            preflight_report = load_isolated_preflight_report(
                args.preflight_report,
                expected_declaration=declaration,
                root=PROJECT_ROOT,
                require_passed=True,
            )
        except (OSError, ValueError) as exc:
            print(f"preflight report error: {exc}", file=sys.stderr)
            return 2
        captured_declaration = preflight_report.get("declaration")
        output_issue = _output_path_issue(
            args,
            captured_declaration if isinstance(captured_declaration, dict) else None,
        )
        if output_issue is not None:
            print(f"output error: {output_issue}", file=sys.stderr)
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
    release_evidence = None
    if args.release_evidence is not None:
        try:
            release_evidence = json.loads(args.release_evidence.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            print(f"release evidence error: {exc}", file=sys.stderr)
            return 2
        if not isinstance(release_evidence, dict):
            print("release evidence error: report must be a JSON object", file=sys.stderr)
            return 2
    try:
        package = build_acceptance_package(
            release_check_status=args.release_check_status,
            backup_manifest=args.backup_manifest,
            media_recovery_point=args.media_recovery_point,
            release_evidence=release_evidence,
            release_evidence_path=args.release_evidence,
            isolated_declaration=declaration,
            isolated_preflight_report=preflight_report,
            isolated_preflight_report_path=args.preflight_report,
            runtime_readiness=readiness,
        )
    except ValueError as exc:
        print(f"acceptance package input error: {exc}", file=sys.stderr)
        return 2
    if args.output is None:
        print(package)
        return 0
    atomic_write_bytes(args.output, package.encode("utf-8"), mode=0o600)
    print(args.output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
