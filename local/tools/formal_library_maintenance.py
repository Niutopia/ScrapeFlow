#!/usr/bin/env python3
"""Explicit offline/maintenance entry point; planning is the safe default.

Only ``execute`` and ``commit`` can mutate AList and both require the literal
``--confirm-remote-mutations`` switch plus an approved immutable plan SHA.
No credentials are accepted on the command line or written to output.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from pathlib import Path
from typing import Any, Mapping

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from engine.scraper import AListClient
from engine.scrapeflow.alist_exact_file_adapter import AListExactFileAdapter
from engine.scrapeflow.formal_library_remediation import build_formal_remediation_plan
from engine.scrapeflow.formal_library_remediation import canonical_digest
from local.scrapeflow_api.formal_library_maintenance import (
    accept_and_commit_formal_remediation,
    build_formal_maintenance_acceptance,
    build_formal_post_audit_evidence,
    execute_formal_remediation,
)


def _read(path: Path) -> Mapping[str, Any]:
    value = json.loads(path.read_text("utf-8"))
    if not isinstance(value, Mapping):
        raise ValueError(f"JSON object required: {path}")
    return value


def _write(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True, allow_nan=False) + "\n",
        "utf-8",
    )


def _client() -> AListExactFileAdapter:
    backend = AListClient(
        os.environ.get("ALIST_URL", "http://127.0.0.1:5244"),
        os.environ.get("ALIST_USERNAME", ""),
        os.environ.get("ALIST_PASSWORD", ""),
        allow_insecure_http=True,
    )
    backend.login()
    return AListExactFileAdapter(backend)


def _full_read_witness(client: AListExactFileAdapter, path: str) -> dict[str, Any]:
    before = client.stat_exact(path)
    if before is None:
        raise RuntimeError(f"planned target is absent: {path}")
    size, digest = 0, hashlib.sha256()
    with client.open_reader(path) as reader:
        while chunk := reader.read(1024 * 1024):
            size += len(chunk)
            digest.update(chunk)
    after = client.stat_exact(path)
    actual = digest.hexdigest()
    if (
        after is None or size != before.size or after.size != before.size
        or (before.version is not None and after.version is not None
            and before.version != after.version)
        or (before.sha256 is not None and before.sha256 != actual)
        or (after.sha256 is not None and after.sha256 != actual)
    ):
        raise RuntimeError(f"planned target changed during full read: {path}")
    return {"size": size, "sha256": actual}


def _blocking_issue_codes(audit: Mapping[str, Any]) -> list[str]:
    codes: set[str] = set()
    for group in (audit.get("projects", []), audit.get("movies", [])):
        if not isinstance(group, list):
            continue
        for work in group:
            if not isinstance(work, Mapping):
                continue
            for issue in work.get("issues", []):
                if (
                    isinstance(issue, Mapping)
                    and issue.get("severity") in {"critical", "high"}
                    and isinstance(issue.get("code"), str)
                ):
                    codes.add(str(issue["code"]))
    for key in ("nfo_parse_errors", "uncovered_media", "duplicate_tmdb_ids"):
        value = audit.get(key)
        if value:
            codes.add(key)
    return sorted(codes)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Formal library remediation; plan-only unless explicitly applied")
    commands = parser.add_subparsers(dest="command", required=True)
    plan = commands.add_parser("plan", help="pure plan-only mode; never connects to AList")
    plan.add_argument("--audit", type=Path, required=True)
    plan.add_argument("--work-order", type=Path, required=True)
    plan.add_argument("--output", type=Path, required=True)

    execute = commands.add_parser("execute", help="seal and run, retaining rollback data")
    execute.add_argument("--plan", type=Path, required=True)
    execute.add_argument("--approved-plan-sha256", required=True)
    execute.add_argument("--state-root", type=Path, required=True)
    execute.add_argument("--global-control", type=Path, required=True)
    execute.add_argument("--confirm-remote-mutations", action="store_true", required=True)

    verify = commands.add_parser("verify", help="read-only full target hashing and fresh audit binding")
    verify.add_argument("--plan", type=Path, required=True)
    verify.add_argument("--fresh-audit", type=Path, required=True)
    verify.add_argument("--output", type=Path, required=True)

    accept = commands.add_parser("accept", help="pure Local acceptance construction")
    accept.add_argument("--plan", type=Path, required=True)
    accept.add_argument("--post-audit-evidence", type=Path, required=True)
    accept.add_argument("--output", type=Path, required=True)

    commit = commands.add_parser("commit", help="release rollback only after Local acceptance")
    commit.add_argument("--plan", type=Path, required=True)
    commit.add_argument("--acceptance", type=Path, required=True)
    commit.add_argument("--fresh-audit", type=Path, required=True,
                        help="fresh read-only audit JSON; no mutation is performed")
    commit.add_argument("--approved-plan-sha256", required=True)
    commit.add_argument("--state-root", type=Path, required=True)
    commit.add_argument("--global-control", type=Path, required=True)
    commit.add_argument("--confirm-remote-mutations", action="store_true", required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    if args.command == "plan":
        value = build_formal_remediation_plan(_read(args.audit), _read(args.work_order))
        _write(args.output, value)
        print(json.dumps({"mode": "plan-only", "plan_sha256": value["plan_sha256"]}))
        return 0
    plan = _read(args.plan)
    if args.command == "verify":
        client = _client()
        witnesses: dict[str, Mapping[str, Any]] = {}
        absent: list[str] = []
        for operation in plan["operations"]:
            source = str(operation["source_path"])
            if client.stat_exact(source) is not None:
                raise RuntimeError(f"planned source still exists after execution: {source}")
            absent.append(source)
            target = operation.get("target_path")
            if isinstance(target, str):
                witnesses[target] = _full_read_witness(client, target)
            retained = operation.get("retained_path")
            if isinstance(retained, str):
                witnesses[retained] = _full_read_witness(client, retained)
        audit = _read(args.fresh_audit)
        if audit.get("canonical_tree_sha256") != canonical_digest(plan["canonical_tree"]):
            raise RuntimeError("fresh audit canonical tree does not match approved plan")
        evidence = build_formal_post_audit_evidence(
            audit, target_witnesses=witnesses, absent_paths=absent,
            blocking_issue_codes=_blocking_issue_codes(audit),
        )
        _write(args.output, evidence)
        print(json.dumps({"mode": "read-only-verify", "evidence_sha256": evidence["evidence_sha256"]}))
        return 0
    if args.command == "accept":
        acceptance = build_formal_maintenance_acceptance(
            plan, post_audit_evidence=_read(args.post_audit_evidence),
        )
        _write(args.output, acceptance)
        print(json.dumps({"mode": "acceptance-only", "acceptance_sha256": acceptance["acceptance_sha256"]}))
        return 0
    if not args.confirm_remote_mutations:  # argparse also requires it; defense in depth
        raise RuntimeError("remote mutation confirmation is required")
    client = _client()
    if args.command == "execute":
        result = execute_formal_remediation(
            client, state_root=args.state_root, pause_receipt_path=args.global_control,
            plan=plan, approved_plan_sha256=args.approved_plan_sha256,
        )
    else:
        result = accept_and_commit_formal_remediation(
            client, state_root=args.state_root, pause_receipt_path=args.global_control,
            plan=plan, approved_plan_sha256=args.approved_plan_sha256,
            acceptance=_read(args.acceptance),
            post_audit_reader=lambda: _read(args.fresh_audit),
        )
    print(json.dumps({"mode": args.command, "receipt_sha256": result["receipt_sha256"]}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
