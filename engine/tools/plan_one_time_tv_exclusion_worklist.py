#!/usr/bin/env python3
"""Seal the independent phase-3 TV worklist with nested-title exclusions.

Phase 1 and phase 2 are immutable inputs.  This planner performs no AList
request and creates no dispatch, Job, journal, or replenishment action.
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
    canonical_digest,
    one_time_worklist_is_valid,
    seal_one_time_worklist,
)
from engine.scrapeflow.one_time_movie_member_scope import (
    movie_scope_matches_candidate,
    validate_exact_movie_member_scope,
    validate_movie_member_candidate,
)
from engine.scrapeflow.one_time_tv_exclusion_scope import (
    seal_exact_tv_exclusion_scope_from_predecessor_batch,
    tv_exclusion_scope_matches_batch,
    validate_exact_tv_exclusion_scope,
)
from engine.tools.plan_one_time_library_completion import read_live_pause_control
from engine.tools.plan_one_time_movie_scope_worklist import phase_two_movie_worklist_is_valid


PHASE_KIND = "one_time_exact_tv_root_with_nested_exclusions"


def load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _eligible_tv_batches(worklist: Mapping[str, Any]) -> list[dict[str, Any]]:
    observations = worklist.get("observations")
    title_batches = observations.get("title_batches") if isinstance(observations, Mapping) else None
    raw = title_batches.get("batches") if isinstance(title_batches, Mapping) else None
    if not isinstance(raw, list):
        raise ValueError("phase-1 工作清单缺少封存作品批次")
    output: list[dict[str, Any]] = []
    for row in raw:
        if not isinstance(row, Mapping):
            continue
        identity = row.get("identity")
        blockers = row.get("scope_blockers")
        if (
            isinstance(identity, Mapping)
            and identity.get("status") == "exact"
            and identity.get("media_type") == "tv"
            and row.get("read_only_audit_allowed") is False
            and isinstance(blockers, list)
            and len(blockers) == 1
            and isinstance(blockers[0], Mapping)
            and blockers[0].get("reason") == "nested_title_identity"
        ):
            output.append(deepcopy(dict(row)))
    if not output:
        raise ValueError("phase-1 没有仅因 nested_title_identity 封锁的 TV 批次")
    keys = [row.get("title_work_key") for row in output]
    if any(not isinstance(key, str) or re.fullmatch(r"[0-9a-f]{64}", key) is None for key in keys):
        raise ValueError("phase-1 TV 批次 work key 无效")
    if len(keys) != len(set(keys)):
        raise ValueError("phase-1 TV 批次 work key 重复")
    return sorted(output, key=lambda row: str(row["title_work_key"]))


def _phase_two_batches(worklist: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    observations = worklist.get("observations")
    title_batches = observations.get("title_batches") if isinstance(observations, Mapping) else None
    raw = title_batches.get("batches") if isinstance(title_batches, Mapping) else None
    if not isinstance(raw, list):
        raise ValueError("phase-2 工作清单缺少电影批次")
    output: dict[str, dict[str, Any]] = {}
    for row in raw:
        identity = row.get("identity") if isinstance(row, Mapping) else None
        key = row.get("title_work_key") if isinstance(row, Mapping) else None
        if not isinstance(identity, Mapping) or identity.get("media_type") != "movie":
            raise ValueError("phase-2 工作清单混入非电影批次")
        if not isinstance(key, str) or key in output:
            raise ValueError("phase-2 电影 work key 缺失或重复")
        output[key] = deepcopy(dict(row))
    return output


def _verified_phase_two_scopes(
    phase_two: Mapping[str, Any], audit_scopes: Iterable[Mapping[str, Any]],
) -> dict[str, dict[str, Any]]:
    """Bind resolved movie members to their sealed phase-2 candidates."""
    batches = _phase_two_batches(phase_two)
    output: dict[str, dict[str, Any]] = {}
    for raw in audit_scopes:
        if not isinstance(raw, Mapping):
            raise ValueError("phase-2 电影复核 scope 必须是对象")
        key = raw.get("title_work_key")
        batch = batches.get(str(key))
        if batch is None or batch.get("read_only_audit_allowed") is not True:
            raise ValueError("phase-2 电影复核 scope 不属于允许批次")
        if raw.get("worklist_sha256") != phase_two.get("worklist_sha256"):
            raise ValueError("phase-2 电影复核 scope 来自过期 worklist")
        scopes = raw.get("movie_member_scopes")
        if not isinstance(scopes, list) or len(scopes) != 1:
            raise ValueError("phase-2 电影复核缺少唯一成员范围")
        scope = validate_exact_movie_member_scope(scopes[0])
        candidate = validate_movie_member_candidate(batch.get("movie_member_candidate"))
        before = raw.get("before_inventory")
        after = raw.get("after_inventory")
        def inventory_is_valid(value: Any) -> bool:
            if not isinstance(value, Mapping):
                return False
            member = value.get("member_inventory")
            parent = value.get("parent_inventory")
            if not isinstance(member, Mapping) or not isinstance(parent, Mapping):
                return False
            member_digest = member.get("inventory_sha256")
            parent_digest = parent.get("inventory_sha256")
            member_core = {key: item for key, item in member.items() if key != "inventory_sha256"}
            parent_core = {key: item for key, item in parent.items() if key != "inventory_sha256"}
            envelope = {"member_inventory": member, "parent_inventory": parent}
            return bool(
                isinstance(member_digest, str)
                and isinstance(parent_digest, str)
                and canonical_digest(member_core) == member_digest
                and canonical_digest(parent_core) == parent_digest
                and value.get("inventory_sha256") == canonical_digest(envelope)
            )
        if (
            not movie_scope_matches_candidate(scope, candidate)
            or raw.get("source_scope_kind") != "one_time_exact_movie_member_scope"
            or raw.get("scope_sha256") != canonical_digest([scope])
            or not isinstance(before, Mapping) or not isinstance(after, Mapping)
            or not inventory_is_valid(before) or not inventory_is_valid(after)
            or before != after
            or raw.get("remote_mutations") is not False
            or raw.get("scheduler_dispatch") is not False
        ):
            raise ValueError("phase-2 电影成员范围未通过只读指纹绑定")
        stem = scope["target_stem"]
        if stem in output:
            raise ValueError("phase-2 电影 target stem 重复")
        output[stem] = scope
    return output


def phase_three_tv_exclusion_worklist_is_valid(value: Mapping[str, Any]) -> bool:
    if not isinstance(value, Mapping) or not one_time_worklist_is_valid(value):
        return False
    phase = value.get("phase")
    safety = value.get("safety_assertions")
    observations = value.get("observations")
    title_batches = observations.get("title_batches") if isinstance(observations, Mapping) else None
    batches = title_batches.get("batches") if isinstance(title_batches, Mapping) else None
    if (
        not isinstance(phase, Mapping)
        or phase.get("number") != 3
        or phase.get("kind") != PHASE_KIND
        or phase.get("phase1_immutable") is not True
        or phase.get("phase2_immutable") is not True
        or re.fullmatch(r"[0-9a-f]{64}", str(phase.get("phase1_worklist_sha256") or "")) is None
        or re.fullmatch(r"[0-9a-f]{64}", str(phase.get("phase2_worklist_sha256") or "")) is None
        or value.get("mutation") is not False
        or value.get("dispatchable_count") != 0
        or value.get("dispatchable") != []
        or (value.get("dispatch_gate") or {}).get("reason") != "global_pause_active"
        or not isinstance(safety, Mapping)
        or not isinstance(batches, list)
    ):
        return False
    required = {
        "alist_mutation_endpoints_used": 0,
        "production_tasks_created": 0,
        "media_journals_created": 0,
        "replenishment_requests_created": 0,
        "remote_files_deleted": 0,
        "remote_files_moved": 0,
        "phase1_worklist_resealed": False,
        "phase2_worklist_resealed": False,
    }
    if any(safety.get(key) != expected for key, expected in required.items()):
        return False
    blocked = 0
    for batch in batches:
        identity = batch.get("identity") if isinstance(batch, Mapping) else None
        if not isinstance(identity, Mapping) or identity.get("media_type") != "tv":
            return False
        if batch.get("read_only_audit_allowed") is True:
            try:
                scope = validate_exact_tv_exclusion_scope(batch.get("tv_exclusion_scope"))
            except (TypeError, ValueError):
                return False
            if not tv_exclusion_scope_matches_batch(scope, batch):
                return False
        else:
            blocked += 1
            if "tv_exclusion_scope" in batch:
                return False
    return (
        title_batches.get("title_count") == len(batches)
        and title_batches.get("blocked_title_scope_count") == blocked
    )


def build_phase_three_tv_exclusion_worklist(
    *, phase_one: Mapping[str, Any], phase_one_path: str,
    phase_two: Mapping[str, Any], phase_two_path: str,
    phase_two_audit_scopes: Iterable[Mapping[str, Any]],
    runtime_control: Mapping[str, Any], input_evidence: Mapping[str, Any],
) -> dict[str, Any]:
    """Build an independent phase 3; an unsealable parent remains blocked."""
    if not one_time_worklist_is_valid(phase_one):
        raise ValueError("phase-1 工作清单 digest 无效或已被改动")
    if not phase_two_movie_worklist_is_valid(phase_two):
        raise ValueError("phase-2 电影工作清单无效或已被改动")
    if any(runtime_control.get(key) is not True for key in ("paused", "persistent")):
        raise ValueError("phase-3 生成必须保持全局持久暂停")
    phase_one_digest = str(phase_one["worklist_sha256"])
    phase_two_digest = str(phase_two["worklist_sha256"])
    predecessors = _eligible_tv_batches(phase_one)
    member_scopes = _verified_phase_two_scopes(phase_two, phase_two_audit_scopes)
    batches: list[dict[str, Any]] = []
    for predecessor in predecessors:
        blocker = predecessor["scope_blockers"][0]
        nested_roots = blocker.get("target_roots")
        if not isinstance(nested_roots, list):
            raise ValueError("phase-1 nested_title_identity target_roots 无效")
        matching = [member_scopes[root] for root in nested_roots if root in member_scopes]
        batch = deepcopy(predecessor)
        try:
            scope = seal_exact_tv_exclusion_scope_from_predecessor_batch(
                predecessor, movie_member_scopes=matching,
            )
        except ValueError:
            # A contained/overlapping exclusion cannot be widened safely.  It
            # remains an explicit blocked batch without discarding other TVs.
            batch["phase3_scope_resolution"] = {
                "status": "blocked",
                "reason": "conservative_tv_exclusion_scope_rejected",
            }
            batch.pop("tv_exclusion_scope", None)
            batch["read_only_audit_allowed"] = False
        else:
            batch["tv_exclusion_scope"] = scope
            batch["scope_blockers"] = []
            batch["read_only_audit_allowed"] = True
            batch["first_action"] = "fresh_exact_tv_read_only_audit_with_nested_exclusions"
            batch["phase3_scope_resolution"] = {
                "status": "sealed",
                "phase2_movie_scope_count": len(matching),
                "conservative_stem_exclusion_count": len(scope["excluded_roots"]) - len(matching),
            }
        batch["mutation_allowed"] = False
        batches.append(batch)

    blocked = sum(row.get("read_only_audit_allowed") is not True for row in batches)
    report = {
        "schema_version": 1,
        "kind": "one_time_library_completion_worklist",
        "planned_at": phase_one.get("planned_at"),
        "mutation": False,
        "dispatch_gate": deepcopy(phase_one.get("dispatch_gate")),
        "dispatchable_count": 0,
        "dispatchable": [],
        "observations": {
            "title_batches": {
                "schema_version": 1,
                "kind": "one_time_title_batches",
                "title_count": len(batches),
                "blocked_title_scope_count": blocked,
                "batches": batches,
            },
        },
        "inputs": {
            **deepcopy(dict(input_evidence)),
            "runtime_control_digest": canonical_digest(runtime_control),
            "phase1_worklist": phase_one_path,
            "phase1_worklist_sha256": phase_one_digest,
            "phase2_worklist": phase_two_path,
            "phase2_worklist_sha256": phase_two_digest,
            "phase2_movie_scope_set_sha256": canonical_digest([
                member_scopes[key] for key in sorted(member_scopes, key=str.casefold)
            ]),
        },
        "phase": {
            "number": 3,
            "kind": PHASE_KIND,
            "phase1_worklist": phase_one_path,
            "phase1_worklist_sha256": phase_one_digest,
            "phase1_immutable": True,
            "phase2_worklist": phase_two_path,
            "phase2_worklist_sha256": phase_two_digest,
            "phase2_immutable": True,
        },
        "safety_assertions": {
            "alist_mutation_endpoints_used": 0,
            "production_tasks_created": 0,
            "media_journals_created": 0,
            "replenishment_requests_created": 0,
            "remote_files_deleted": 0,
            "remote_files_moved": 0,
            "phase1_worklist_resealed": False,
            "phase2_worklist_resealed": False,
            "unresolved_movie_stems_excluded_conservatively": True,
            "overlapping_exclusion_batches_remain_blocked": True,
        },
    }
    sealed = seal_one_time_worklist(report)
    if (
        phase_one.get("worklist_sha256") != phase_one_digest
        or phase_two.get("worklist_sha256") != phase_two_digest
    ):
        raise RuntimeError("phase-1/phase-2 工作清单在构建期间发生变化")
    if not phase_three_tv_exclusion_worklist_is_valid(sealed):
        raise RuntimeError("phase-3 TV 排除工作清单封存校验失败")
    return sealed


def _write_new_json(path: Path, value: Any) -> None:
    """Atomically create a new phase output; never replace an old seal."""
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    payload = (json.dumps(value, ensure_ascii=False, indent=2) + "\n").encode("utf-8")
    temporary = path.with_name(path.name + f".tmp-{os.getpid()}")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.link(temporary, path)
    finally:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--phase1-worklist", type=Path, required=True)
    parser.add_argument("--phase2-worklist", type=Path, required=True)
    parser.add_argument("--phase2-audits-root", type=Path, required=True)
    parser.add_argument("--global-control", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--control-url", default="http://127.0.0.1:3010/api/control")
    parser.add_argument("--docker-compose-network", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    resolved = {args.phase1_worklist.resolve(), args.phase2_worklist.resolve()}
    if args.output.resolve() in resolved:
        raise ValueError("phase-3 输出禁止指向 phase-1/phase-2 工作清单")
    phase_one_bytes = args.phase1_worklist.read_bytes()
    phase_two_bytes = args.phase2_worklist.read_bytes()
    phase_one = json.loads(phase_one_bytes.decode("utf-8"))
    phase_two = json.loads(phase_two_bytes.decode("utf-8"))
    durable = load_json(args.global_control)
    if not isinstance(durable, Mapping) or durable.get("paused") is not True:
        raise ValueError("phase-3 只读工具要求持久全局暂停")
    allowed_hosts = frozenset({"api"}) if args.docker_compose_network else frozenset()
    live_control = read_live_pause_control(
        args.control_url, allowed_hosts=allowed_hosts,
        host_header="localhost" if args.docker_compose_network else None,
    )
    if any(live_control.get(key) is not True for key in ("paused", "persistent")):
        raise ValueError("phase-3 生成前必须保持全局持久暂停")
    scope_files = sorted(
        args.phase2_audits_root.glob("*/title-scope.json"),
        key=lambda path: str(path).casefold(),
    )
    scopes = [load_json(path) for path in scope_files]
    report = build_phase_three_tv_exclusion_worklist(
        phase_one=phase_one, phase_one_path=str(args.phase1_worklist),
        phase_two=phase_two, phase_two_path=str(args.phase2_worklist),
        phase_two_audit_scopes=scopes,
        runtime_control={
            "paused": True, "persistent": True,
            "basis": "durable_global_pause_plus_live_scheduler_pause",
            "durable_digest": canonical_digest(durable),
            "live_control_digest": canonical_digest(live_control),
        },
        input_evidence={
            "global_control": str(args.global_control),
            "phase2_audits_root": str(args.phase2_audits_root),
            "phase2_title_scope_files": [str(path) for path in scope_files],
        },
    )
    if (
        args.phase1_worklist.read_bytes() != phase_one_bytes
        or args.phase2_worklist.read_bytes() != phase_two_bytes
    ):
        raise RuntimeError("phase-1/phase-2 文件在 phase-3 构建期间发生变化")
    _write_new_json(args.output, report)
    if (
        args.phase1_worklist.read_bytes() != phase_one_bytes
        or args.phase2_worklist.read_bytes() != phase_two_bytes
    ):
        raise RuntimeError("phase-1/phase-2 文件在 phase-3 写出期间发生变化")
    batches = report["observations"]["title_batches"]
    print(json.dumps({
        "output": str(args.output), "phase": 3,
        "tv_batches": batches["title_count"],
        "read_only_allowed": batches["title_count"] - batches["blocked_title_scope_count"],
        "blocked": batches["blocked_title_scope_count"],
        "dispatchable_count": 0, "remote_mutations": False,
    }, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
