#!/usr/bin/env python3
"""Produce a read-only worklist for one explicitly launched library completion."""

from __future__ import annotations

import argparse
from collections import Counter
from copy import deepcopy
import json
import os
from pathlib import Path, PurePosixPath
import sys
from typing import Any, Mapping
from urllib.parse import urlsplit
from urllib.request import Request, urlopen

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from engine.scraper import AListClient
from engine.tools.build_library_audit_report import build as build_library_audit_report
from engine.scrapeflow.one_time_library_completion import (
    INBOX_ROOT,
    build_cleanup_plan,
    build_one_time_worklist,
    build_inbox_discovery_plan,
    build_one_time_library_plan,
    build_one_time_title_batches,
    canonical_digest,
    seal_one_time_worklist,
)


def load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def load_connection_env(path: Path | None = None) -> None:
    """Load only this read-only tool's AList connection values.

    Importing the API runtime config creates its state directories, which is an
    unwanted side effect for a standalone one-time planner.
    """
    source = path or PROJECT_ROOT / ".env.local"
    if not source.exists():
        return
    allowed = {"ALIST_URL", "ALIST_USERNAME", "ALIST_PASSWORD"}
    for raw_line in source.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        if key not in allowed:
            continue
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
            value = value[1:-1]
        os.environ.setdefault(key, value)


def atomic_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    temporary.write_text(
        json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8",
    )
    temporary.replace(path)


def read_live_pause_control(
    url: str, *, allowed_hosts: frozenset[str] = frozenset(),
    host_header: str | None = None,
) -> dict[str, Any]:
    """Require the running loopback API to confirm the durable pause."""
    parsed = urlsplit(url)
    if (
        parsed.scheme != "http"
        or parsed.hostname not in {"127.0.0.1", "localhost", "::1", *allowed_hosts}
        or parsed.username is not None
        or parsed.password is not None
    ):
        raise ValueError("control URL 必须是无凭据的本机 HTTP 或明确允许的 HTTP 地址")
    if host_header not in {None, "localhost", "127.0.0.1"}:
        raise ValueError("control Host 只允许本机值")
    headers = {"Accept": "application/json"}
    if host_header is not None:
        headers["Host"] = host_header
    request = Request(url, headers=headers)
    with urlopen(request, timeout=3) as response:  # nosec B310 - loopback checked above
        payload_bytes = response.read(65_537)
    if len(payload_bytes) > 65_536:
        raise ValueError("实时暂停响应过大")
    payload = json.loads(payload_bytes.decode("utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("实时暂停响应格式无效")
    required = {"paused": True, "persistent": True}
    if any(payload.get(key) is not value for key, value in required.items()):
        raise ValueError("一次性只读审计要求 API 持久暂停")
    return payload


def read_only_inventory(client: AListClient) -> list[dict[str, Any]]:
    rows = client.list(INBOX_ROOT, refresh=True)
    inventory: list[dict[str, Any]] = []
    for row in sorted(rows, key=lambda item: str(item.get("name") or "").casefold()):
        name = row.get("name")
        if not row.get("is_dir") or not isinstance(name, str) or not name or "/" in name:
            continue
        source = f"{INBOX_ROOT}/{name}"
        walked = client.walk(
            source, refresh=True, max_directories=10_000, max_files=200_000,
            include_bonus=True, include_title_extras=True,
        )
        files = []
        extensions: Counter[str] = Counter()
        for item in walked:
            if item.get("is_dir"):
                continue
            path = str(item.get("full_path") or "")
            if not path:
                continue
            extensions[PurePosixPath(path).suffix.casefold() or "<none>"] += 1
            files.append({
                "path": path,
                "size": int(item.get("size") or 0),
                "hash": item.get("hash_info") or item.get("hash"),
                "modified": item.get("modified"),
            })
        inventory.append({
            "path": source,
            "file_count": len(files),
            "bytes": sum(row["size"] for row in files),
            "extensions": dict(sorted(extensions.items())),
            "files": files,
            "pending_delete": bool(row.get("pending_delete") or row.get("marked_for_deletion")),
        })
    return inventory


def load_historical_tasks(jobs_root: Path) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    tasks: list[dict[str, Any]] = []
    cleanup_evidence: list[dict[str, Any]] = []
    for job_path in sorted(jobs_root.glob("*/job.json")):
        try:
            job = load_json(job_path)
        except (OSError, ValueError, json.JSONDecodeError):
            continue
        if not isinstance(job, dict):
            continue
        tasks.append(job)
        journal_path = job_path.parent / "media-journal.json"
        plan_path = job_path.parent / "media-plan.json"
        if not journal_path.is_file() or not plan_path.is_file():
            continue
        try:
            journal = load_json(journal_path)
            wrapped_plan = load_json(plan_path)
        except (OSError, ValueError, json.JSONDecodeError):
            continue
        plan = wrapped_plan.get("plan") if isinstance(wrapped_plan, Mapping) else None
        if not isinstance(plan, Mapping):
            continue
        target = plan.get("target_root")
        metadata = plan.get("metadata")
        if isinstance(metadata, Mapping):
            target = metadata.get("series_root") or target
        cleanup_evidence.append({
            "id": job.get("id"), "source": job.get("source"),
            "phase": job.get("phase"), "target_root": target, "journal": journal,
        })
    return tasks, cleanup_evidence


def normalized_projects(
    raw_audit: Mapping[str, Any], boundaries: Mapping[str, Any],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    audit_report = build_library_audit_report(
        dict(raw_audit), {"rows": []}, dict(boundaries),
        audited_at="dry-run",
    )
    allowed: dict[tuple[str, str], set[tuple[int, int]]] = {}
    metadata_by_target: dict[str, list[dict[str, Any]]] = {}
    for show in audit_report.get("shows", []):
        if not isinstance(show, Mapping):
            continue
        category = str(show.get("category") or "")
        target = str(show.get("target_root") or "")
        if category in {"core", "optional"}:
            allowed.setdefault((target, category), set()).update(
                (int(row.get("season") or 0), int(row.get("episode") or 0))
                for row in show.get("missing", []) if isinstance(row, Mapping)
            )
        elif category == "metadata":
            metadata_by_target.setdefault(target, []).extend(
                dict(row) for row in show.get("missing", []) if isinstance(row, Mapping)
            )
    projects = []
    for raw in raw_audit.get("projects", []):
        if not isinstance(raw, Mapping):
            continue
        row = deepcopy(dict(raw))
        target = str(row.get("target_root") or "")
        for field, category in (("regular_missing", "core"), ("optional_missing", "optional")):
            accepted = allowed.get((target, category), set())
            row[field] = [
                item for item in row.get(field, [])
                if isinstance(item, Mapping)
                and (item.get("season"), item.get("episode")) in accepted
            ]
        row["metadata_issues"] = metadata_by_target.get(target, [])
        projects.append(row)
    for movie in raw_audit.get("movies", []):
        if not isinstance(movie, Mapping):
            continue
        target = movie.get("target_root") or movie.get("target_stem")
        if not isinstance(target, str):
            continue
        projects.append({
            **dict(movie), "target_root": target, "media_type": "movie",
            "regular_missing": [], "optional_missing": [],
            "metadata_issues": metadata_by_target.get(target, []),
        })
    regular_count = sum(len(row.get("regular_missing", [])) for row in projects)
    optional_count = sum(len(row.get("optional_missing", [])) for row in projects)
    if regular_count != audit_report.get("total_missing") or optional_count != audit_report.get("optional_missing"):
        raise ValueError(
            "一次性工作清单与审计报告语义不一致: "
            f"plan={regular_count}/{optional_count} "
            f"report={audit_report.get('total_missing')}/{audit_report.get('optional_missing')}"
        )
    return projects, audit_report


def clean_targets(
    projects: list[Mapping[str, Any]],
    refined: Mapping[str, Any],
) -> list[str]:
    dirty_subtitle_targets = {
        str(row.get("target_root"))
        for field in ("confirmed_missing_chinese", "pending_review_or_probe")
        for row in refined.get(field, [])
        if isinstance(row, Mapping) and isinstance(row.get("target_root"), str)
    }
    output = []
    for row in projects:
        target = row.get("target_root")
        if not isinstance(target, str):
            continue
        if row.get("regular_missing") or row.get("optional_missing") or row.get("metadata_issues"):
            continue
        if target in dirty_subtitle_targets:
            continue
        output.append(target)
    return output


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--live-audit", type=Path, required=True)
    parser.add_argument("--refined-subtitle-audit", type=Path, required=True)
    parser.add_argument("--boundaries", type=Path, required=True)
    parser.add_argument("--jobs-root", type=Path, required=True)
    parser.add_argument("--global-control", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--alist-url", default=os.getenv("ALIST_URL", "http://127.0.0.1:5244"))
    parser.add_argument("--username", default=os.getenv("ALIST_USERNAME", "admin"))
    parser.add_argument(
        "--control-url", default="http://127.0.0.1:3010/api/control",
        help="运行中本机 API 的暂停状态端点",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    load_connection_env()
    args = build_parser().parse_args(argv)
    password = os.getenv("ALIST_PASSWORD")
    if not password:
        raise ValueError("缺少 ALIST_PASSWORD")
    live = load_json(args.live_audit)
    refined = load_json(args.refined_subtitle_audit)
    boundaries = load_json(args.boundaries)
    durable_control = load_json(args.global_control)
    if not isinstance(live, Mapping) or not isinstance(refined, Mapping) or not isinstance(boundaries, Mapping):
        raise ValueError("审计报告结构无效")
    if not isinstance(durable_control, Mapping) or durable_control.get("paused") is not True:
        raise ValueError("本 dry-run 要求持久化全局暂停")
    live_control = read_live_pause_control(args.control_url)

    client = AListClient(
        args.alist_url, args.username, password, timeout=20, retries=1,
        allow_insecure_http=args.alist_url.startswith("http://127.0.0.1")
        or args.alist_url.startswith("http://localhost"),
    )
    client.login()
    inventory = read_only_inventory(client)
    tasks, cleanup_evidence = load_historical_tasks(args.jobs_root)
    projects, audit_report = normalized_projects(live, boundaries)
    inbox = build_inbox_discovery_plan(
        inventory, projects, historical_tasks=tasks,
        # The current inbox is the anime intake.  This supplies only the
        # destination category for unmatched folders; type remains ``auto``
        # and the normal TMDB planning stage must still prove one identity.
        unmatched_media_category="番剧",
    )
    confirmed = refined.get("confirmed_missing_chinese", [])
    if not isinstance(confirmed, list):
        raise ValueError("refined subtitle confirmed_missing_chinese 必须是数组")
    pending_verification = refined.get("pending_review_or_probe", [])
    if not isinstance(pending_verification, list):
        raise ValueError("refined subtitle pending_review_or_probe 必须是数组")
    library = build_one_time_library_plan(
        {"projects": projects}, confirmed_subtitle_gaps=confirmed,
        subtitle_verification_items=pending_verification,
    )
    title_batches = build_one_time_title_batches(library)
    cleanup = build_cleanup_plan(
        inbox, task_evidence=cleanup_evidence,
        clean_audit_targets=clean_targets(projects, refined),
    )
    runtime_control = {
        "paused": True, "persistent": True,
        "basis": "durable_global_pause_plus_live_scheduler_pause",
        "durable_digest": canonical_digest(durable_control),
        "live_control_digest": canonical_digest(live_control),
    }
    report = build_one_time_worklist(
        global_control=runtime_control, inbox_plan=inbox,
        library_plan=library, cleanup_plan=cleanup,
    )
    report["observations"]["title_batches"] = title_batches
    report["inputs"] = {
        "live_audit": str(args.live_audit),
        "live_audit_digest": canonical_digest(live),
        "refined_subtitle_audit": str(args.refined_subtitle_audit),
        "refined_subtitle_audit_digest": canonical_digest(refined),
        "boundaries": str(args.boundaries),
        "boundaries_digest": canonical_digest(boundaries),
        "audit_report_semantics": {
            "regular_missing": audit_report.get("total_missing"),
            "optional_missing": audit_report.get("optional_missing"),
            "metadata_issues": audit_report.get("metadata_issues"),
        },
        "jobs_root": str(args.jobs_root),
        "historical_task_count": len(tasks),
        "global_control": str(args.global_control),
        "live_control": {
            "url": args.control_url,
            "updated_at": live_control.get("updated_at"),
            "digest": canonical_digest(live_control),
        },
    }
    report["safety_assertions"] = {
        "alist_mutation_endpoints_used": 0,
        "production_tasks_created": 0,
        "remote_files_deleted": 0,
        "subtitle_pending_rows_promoted_to_confirmed": 0,
        "subtitle_verification_actions": library["verification_action_count"],
        "subtitle_video_replacement_allowed": False,
        "synthetic_media_journal_allowed": False,
        "cleanup_file_deletion_allowed": False,
    }
    report = seal_one_time_worklist(report)
    atomic_json(args.output, report)
    print(json.dumps({
        "output": str(args.output),
        "inbox_sources": inbox["source_count"],
        "inbox_schedulable": inbox["schedulable_count"],
        "inbox_blocked": inbox["blocked_count"],
        "cleanup_actions": cleanup["action_count"],
        "library_lanes": library["lane_count"],
        "library_gaps": library["gap_count"],
        "subtitle_verification_actions": library["verification_action_count"],
        "dispatch_allowed": report["dispatch_gate"]["allowed"],
    }, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    sys.exit(main())
