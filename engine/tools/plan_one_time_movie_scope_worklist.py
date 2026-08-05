#!/usr/bin/env python3
"""Seal an independent phase-2 worklist for exact movie-member reads only.

The predecessor worklist is immutable input.  This tool never talks to AList,
creates a Job/journal, or enables replenishment.  Runtime resolution of each
sealed candidate is performed later by ``audit_one_time_title_batch.py``.
"""

from __future__ import annotations

import argparse
from copy import deepcopy
import json
import os
from pathlib import Path
import re
import sys
from typing import Any, Iterable, Mapping

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from engine.scrapeflow.one_time_library_completion import (
    build_one_time_library_plan,
    build_one_time_title_batches,
    build_one_time_worklist,
    canonical_digest,
    one_time_worklist_is_valid,
    seal_one_time_worklist,
)
from engine.tools.plan_one_time_library_completion import (
    normalized_projects,
    read_live_pause_control,
)


PHASE_KIND = "one_time_exact_movie_member_read_only"


def load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _phase_one_movie_batches(
    worklist: Mapping[str, Any],
) -> dict[str, dict[str, Any]]:
    observations = worklist.get("observations")
    title_batches = observations.get("title_batches") if isinstance(observations, Mapping) else None
    batches = title_batches.get("batches") if isinstance(title_batches, Mapping) else None
    if not isinstance(batches, list):
        raise ValueError("phase-1 工作清单缺少封存作品批次")
    output: dict[str, dict[str, Any]] = {}
    for row in batches:
        identity = row.get("identity") if isinstance(row, Mapping) else None
        target = row.get("target_root") if isinstance(row, Mapping) else None
        if not isinstance(identity, Mapping) or identity.get("media_type") != "movie":
            continue
        if not isinstance(target, str) or target in output:
            raise ValueError("phase-1 电影目标缺失或重复")
        output[target] = deepcopy(dict(row))
    if not output:
        raise ValueError("phase-1 工作清单没有电影批次")
    return output


def _rows_for_movie_targets(
    rows: Iterable[Any], targets: set[str],
) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    for raw in rows:
        if not isinstance(raw, Mapping):
            continue
        target = raw.get("target_root")
        if target in targets:
            output.append(deepcopy(dict(raw)))
            continue
        video_path = raw.get("video_path")
        matches = [
            stem for stem in targets
            if isinstance(video_path, str)
            and (
                video_path.startswith(stem + ".")
                or video_path.startswith(stem.rstrip("/") + "/")
            )
        ]
        if len(matches) == 1:
            output.append({**deepcopy(dict(raw)), "target_root": matches[0]})
    return output


def phase_two_movie_worklist_is_valid(value: Mapping[str, Any]) -> bool:
    """Validate the extra phase boundary on top of the generic worklist seal."""
    if not isinstance(value, Mapping) or not one_time_worklist_is_valid(value):
        return False
    phase = value.get("phase")
    safety = value.get("safety_assertions")
    observations = value.get("observations")
    title_batches = observations.get("title_batches") if isinstance(observations, Mapping) else None
    batches = title_batches.get("batches") if isinstance(title_batches, Mapping) else None
    if (
        not isinstance(phase, Mapping)
        or set(phase) != {
            "number", "kind", "predecessor_worklist",
            "predecessor_worklist_sha256", "predecessor_immutable",
        }
        or phase.get("number") != 2
        or phase.get("kind") != PHASE_KIND
        or phase.get("predecessor_immutable") is not True
        or not isinstance(phase.get("predecessor_worklist"), str)
        or re.fullmatch(
            r"[0-9a-f]{64}", str(phase.get("predecessor_worklist_sha256") or ""),
        ) is None
        or value.get("mutation") is not False
        or value.get("dispatchable_count") != 0
        or value.get("dispatchable") != []
        or (value.get("dispatch_gate") or {}).get("reason") != "global_pause_active"
        or not isinstance(safety, Mapping)
        or not isinstance(batches, list)
    ):
        return False
    required_safety = {
        "alist_mutation_endpoints_used": 0,
        "production_tasks_created": 0,
        "media_journals_created": 0,
        "replenishment_requests_created": 0,
        "remote_files_deleted": 0,
        "remote_files_moved": 0,
        "phase1_worklist_resealed": False,
    }
    if any(safety.get(key) != expected for key, expected in required_safety.items()):
        return False
    for batch in batches:
        identity = batch.get("identity") if isinstance(batch, Mapping) else None
        if not isinstance(identity, Mapping) or identity.get("media_type") != "movie":
            return False
        if batch.get("read_only_audit_allowed") is True and not isinstance(
            batch.get("movie_member_candidate"), Mapping,
        ):
            return False
    return True


def build_phase_two_movie_worklist(
    *,
    phase_one: Mapping[str, Any],
    phase_one_path: str,
    projects: Iterable[Mapping[str, Any]],
    confirmed_subtitle_gaps: Iterable[Any],
    pending_subtitle_verification: Iterable[Any],
    runtime_control: Mapping[str, Any],
    input_evidence: Mapping[str, Any],
) -> dict[str, Any]:
    """Build phase 2 without mutating or widening the phase-1 movie set."""
    if not one_time_worklist_is_valid(phase_one):
        raise ValueError("phase-1 工作清单 digest 无效或已被改动")
    phase_one_digest = str(phase_one["worklist_sha256"])
    predecessor_batches = _phase_one_movie_batches(phase_one)
    movie_targets = set(predecessor_batches)
    movie_projects = [
        deepcopy(dict(row)) for row in projects
        if isinstance(row, Mapping)
        and row.get("media_type") == "movie"
        and row.get("target_root") in movie_targets
    ]
    project_targets = {str(row.get("target_root")) for row in movie_projects}
    missing = sorted(movie_targets - project_targets, key=str.casefold)
    if missing:
        raise ValueError("phase-2 当前审计缺少 phase-1 电影目标: " + ", ".join(missing[:5]))

    confirmed = _rows_for_movie_targets(confirmed_subtitle_gaps, movie_targets)
    pending = _rows_for_movie_targets(pending_subtitle_verification, movie_targets)
    library = build_one_time_library_plan(
        {"projects": movie_projects},
        confirmed_subtitle_gaps=confirmed,
        subtitle_verification_items=pending,
    )
    title_batches = build_one_time_title_batches(library)
    # Phase 2 removes only the old implementation blocker.  Known identity,
    # nesting, or duplicate-TMDB blockers from phase 1 remain authoritative;
    # rebuilding a movie-only catalog must never make them disappear.
    for batch in title_batches["batches"]:
        target = str(batch.get("target_root") or "")
        predecessor = predecessor_batches.get(target)
        if predecessor is None:
            raise ValueError("phase-2 电影批次不在 phase-1 授权集合中")
        raw_blockers = predecessor.get("scope_blockers")
        if not isinstance(raw_blockers, list):
            raise ValueError("phase-1 电影批次缺少 scope blockers")
        preserved = [
            deepcopy(dict(row)) for row in raw_blockers
            if isinstance(row, Mapping)
            and row.get("reason") != "exact_movie_member_scope_required"
        ]
        if preserved:
            existing = batch.get("scope_blockers")
            if not isinstance(existing, list):
                raise ValueError("phase-2 电影批次 scope blockers 无效")
            combined = {
                canonical_digest(row): row
                for row in [*existing, *preserved]
            }
            batch["scope_blockers"] = [combined[key] for key in sorted(combined)]
            batch["read_only_audit_allowed"] = False
    title_batches["blocked_title_scope_count"] = sum(
        batch.get("read_only_audit_allowed") is False
        for batch in title_batches["batches"]
    )
    built_targets = {
        str(row.get("target_root"))
        for row in title_batches["batches"] if isinstance(row, Mapping)
    }
    if built_targets != movie_targets:
        missing_batches = sorted(movie_targets - built_targets, key=str.casefold)
        extra_batches = sorted(built_targets - movie_targets, key=str.casefold)
        raise ValueError(
            "phase-2 电影批次没有精确继承 phase-1 范围: "
            f"missing={missing_batches[:5]} extra={extra_batches[:5]}"
        )

    report = build_one_time_worklist(
        global_control=runtime_control,
        inbox_plan={
            "schema_version": 1, "kind": "phase2_inert_inbox",
            "mutation": False, "sources": [],
        },
        library_plan=library,
        cleanup_plan={
            "schema_version": 1, "kind": "phase2_inert_cleanup",
            "mutation": False, "actions": [],
        },
    )
    report["observations"]["title_batches"] = title_batches
    report["phase"] = {
        "number": 2,
        "kind": PHASE_KIND,
        "predecessor_worklist": phase_one_path,
        "predecessor_worklist_sha256": phase_one_digest,
        "predecessor_immutable": True,
    }
    report["inputs"] = {
        **deepcopy(dict(input_evidence)),
        "phase1_worklist": phase_one_path,
        "phase1_worklist_sha256": phase_one_digest,
    }
    report["safety_assertions"] = {
        "alist_mutation_endpoints_used": 0,
        "production_tasks_created": 0,
        "media_journals_created": 0,
        "replenishment_requests_created": 0,
        "remote_files_deleted": 0,
        "remote_files_moved": 0,
        "phase1_worklist_resealed": False,
        "movie_scope_resolution_deferred_to_fresh_alist_read": True,
        "shared_parent_directory_as_title_scope_allowed": False,
    }
    sealed = seal_one_time_worklist(report)
    if phase_one.get("worklist_sha256") != phase_one_digest:
        raise RuntimeError("phase-1 工作清单在构建期间发生变化")
    if not phase_two_movie_worklist_is_valid(sealed):
        raise RuntimeError("phase-2 电影工作清单封存校验失败")
    return sealed


def _write_new_json(path: Path, value: Any) -> None:
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    payload = (json.dumps(value, ensure_ascii=False, indent=2) + "\n").encode("utf-8")
    temporary = path.with_name(path.name + f".tmp-{os.getpid()}")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        # A hard-link is an atomic create-if-absent operation on the same
        # filesystem.  It cannot overwrite an existing phase worklist.
        os.link(temporary, path)
    finally:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--phase1-worklist", type=Path, required=True)
    parser.add_argument("--live-audit", type=Path, required=True)
    parser.add_argument("--refined-subtitle-audit", type=Path, required=True)
    parser.add_argument("--boundaries", type=Path, required=True)
    parser.add_argument("--global-control", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--control-url", default="http://127.0.0.1:3010/api/control")
    parser.add_argument("--docker-compose-network", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.output.resolve() == args.phase1_worklist.resolve():
        raise ValueError("phase-2 输出禁止指向 phase-1 工作清单")
    phase_one_bytes = args.phase1_worklist.read_bytes()
    phase_one = json.loads(phase_one_bytes.decode("utf-8"))
    live = load_json(args.live_audit)
    refined = load_json(args.refined_subtitle_audit)
    boundaries = load_json(args.boundaries)
    durable = load_json(args.global_control)
    if not all(isinstance(value, Mapping) for value in (live, refined, boundaries, durable)):
        raise ValueError("phase-2 输入证据结构无效")
    if durable.get("paused") is not True:
        raise ValueError("phase-2 只读工具要求持久全局暂停")
    if not one_time_worklist_is_valid(phase_one):
        raise ValueError("phase-1 工作清单 digest 无效或已被改动")
    predecessor_inputs = phase_one.get("inputs")
    if not isinstance(predecessor_inputs, Mapping):
        raise ValueError("phase-1 工作清单缺少输入 digest")
    current_inputs = {
        "live_audit_digest": canonical_digest(live),
        "refined_subtitle_audit_digest": canonical_digest(refined),
        "boundaries_digest": canonical_digest(boundaries),
    }
    for key, digest in current_inputs.items():
        if predecessor_inputs.get(key) != digest:
            raise ValueError(f"phase-2 {key} 与 phase-1 封存输入不一致")

    allowed_hosts = frozenset({"api"}) if args.docker_compose_network else frozenset()
    live_control = read_live_pause_control(
        args.control_url, allowed_hosts=allowed_hosts,
        host_header="localhost" if args.docker_compose_network else None,
    )
    if any(live_control.get(key) is not True for key in ("paused", "persistent")):
        raise ValueError("phase-2 生成前必须保持全局持久暂停")
    projects, _audit_report = normalized_projects(live, boundaries)
    confirmed = refined.get("confirmed_missing_chinese", [])
    pending = refined.get("pending_review_or_probe", [])
    if not isinstance(confirmed, list) or not isinstance(pending, list):
        raise ValueError("phase-2 字幕精炼证据必须是数组")
    runtime_control = {
        "paused": True, "persistent": True,
        "basis": "durable_global_pause_plus_live_scheduler_pause",
        "durable_digest": canonical_digest(durable),
        "live_control_digest": canonical_digest(live_control),
    }
    report = build_phase_two_movie_worklist(
        phase_one=phase_one,
        phase_one_path=str(args.phase1_worklist),
        projects=projects,
        confirmed_subtitle_gaps=confirmed,
        pending_subtitle_verification=pending,
        runtime_control=runtime_control,
        input_evidence={
            **current_inputs,
            "live_audit": str(args.live_audit),
            "refined_subtitle_audit": str(args.refined_subtitle_audit),
            "boundaries": str(args.boundaries),
            "global_control": str(args.global_control),
            "live_control_digest": canonical_digest(live_control),
        },
    )
    if args.phase1_worklist.read_bytes() != phase_one_bytes:
        raise RuntimeError("phase-1 文件在 phase-2 构建期间发生变化，拒绝写出")
    _write_new_json(args.output, report)
    if args.phase1_worklist.read_bytes() != phase_one_bytes:
        raise RuntimeError("phase-1 文件在 phase-2 写出期间发生变化")
    batches = report["observations"]["title_batches"]
    print(json.dumps({
        "output": str(args.output),
        "phase": 2,
        "predecessor_worklist_sha256": phase_one["worklist_sha256"],
        "movie_batches": batches["title_count"],
        "read_only_allowed": sum(
            row.get("read_only_audit_allowed") is True for row in batches["batches"]
        ),
        "blocked": batches["blocked_title_scope_count"],
        "remote_mutations": False,
    }, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
