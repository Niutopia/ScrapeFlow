#!/usr/bin/env python3
"""Prepare, but never dispatch, one exact-title remediation import."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import sys
from typing import Any, Mapping

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from engine.scrapeflow.one_time_library_completion import (
    canonical_digest,
    one_time_worklist_is_valid,
)
from engine.scrapeflow.one_time_movie_member_scope import (
    movie_scope_matches_candidate,
    validate_exact_movie_member_scope,
)
from engine.scrapeflow.one_time_tv_exclusion_scope import (
    tv_exclusion_scope_matches_batch,
    validate_exact_tv_exclusion_scope,
)
from engine.tools.audit_one_time_title_batch import (
    _find_batch,
    _movie_member_candidate_from_batch,
    _target_from_batch,
)
from engine.tools.plan_one_time_library_completion import read_live_pause_control
from local.scrapeflow_api.replenishment import build_replenishment_requests
from local.scrapeflow_api.title_closure import title_closure_evidence_is_valid


def load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def atomic_json(path: Path, value: Any) -> None:
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    temporary.write_text(
        json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8",
    )
    os.chmod(temporary, 0o600)
    temporary.replace(path)


def validate_scope_closure_binding(
    worklist: Mapping[str, Any], scope: Mapping[str, Any], closure: Mapping[str, Any],
    *, title_work_key: str,
) -> dict[str, Any]:
    """Return the sealed target only when title or movie-member evidence binds."""
    batch = _find_batch(worklist, title_work_key)
    target = _target_from_batch(batch)
    targets = closure.get("title_targets")
    if scope.get("scope") != [target] or targets != [target]:
        raise ValueError("作品范围与封存批次身份不一致")
    kind = closure.get("source_scope_kind")
    if target["media_type"] == "movie":
        if any(
            container.get("tv_exclusion_scopes") is not None
            for container in (scope, closure)
        ) or batch.get("tv_exclusion_scope") is not None:
            raise ValueError("电影成员导入不得携带 TV exclusion 范围")
        if kind != "one_time_exact_movie_member_scope":
            raise ValueError("电影导入必须来自精确成员范围")
        raw_scopes = scope.get("movie_member_scopes")
        if not isinstance(raw_scopes, list) or len(raw_scopes) != 1:
            raise ValueError("电影导入缺少唯一成员范围")
        member_scope = validate_exact_movie_member_scope(raw_scopes[0])
        candidate = _movie_member_candidate_from_batch(batch)
        if not movie_scope_matches_candidate(member_scope, candidate):
            raise ValueError("电影成员范围与封存候选不一致")
        member_scopes = [member_scope]
        if closure.get("movie_member_scopes") != member_scopes:
            raise ValueError("电影成员范围与复核证据不一致")
        expected_sha = canonical_digest(member_scopes)
    else:
        if kind == "one_time_exact_tv_root_with_nested_exclusions":
            if any(
                container.get("movie_member_scopes") is not None
                for container in (scope, closure)
            ) or batch.get("movie_member_candidate") is not None:
                raise ValueError("TV exclusion 导入不得携带电影成员范围")
            if batch.get("read_only_audit_allowed") is not True:
                raise ValueError("TV 排除范围批次未获只读复核许可")
            batch_scope = validate_exact_tv_exclusion_scope(
                batch.get("tv_exclusion_scope"),
            )
            if not tv_exclusion_scope_matches_batch(batch_scope, batch):
                raise ValueError("TV 排除范围与封存批次身份不一致")
            raw_scopes = scope.get("tv_exclusion_scopes")
            if not isinstance(raw_scopes, list) or len(raw_scopes) != 1:
                raise ValueError("TV 导入缺少唯一嵌套排除范围")
            tv_scope = validate_exact_tv_exclusion_scope(raw_scopes[0])
            tv_scopes = [tv_scope]
            if tv_scope != batch_scope:
                raise ValueError("TV 排除范围与 worklist 封存批次不一致")
            if closure.get("tv_exclusion_scopes") != tv_scopes:
                raise ValueError("TV 排除范围与复核证据不一致")
            if scope.get("source_scope_kind") != kind:
                raise ValueError("TV scope envelope 未绑定嵌套排除类型")
            expected_exclusions_sha = canonical_digest(tv_scope["excluded_roots"])
            expected_members_sha = canonical_digest(tv_scope["excluded_member_paths"])
            for location, candidate_scope in (
                ("worklist batch", batch_scope),
                ("title scope", tv_scope),
                ("title closure", closure["tv_exclusion_scopes"][0]),
            ):
                if (
                    candidate_scope.get("excluded_roots") != tv_scope["excluded_roots"]
                    or candidate_scope.get("excluded_roots_sha256")
                    != expected_exclusions_sha
                    or candidate_scope.get("excluded_member_paths")
                    != tv_scope["excluded_member_paths"]
                    or candidate_scope.get("excluded_member_paths_sha256")
                    != expected_members_sha
                ):
                    raise ValueError(f"{location} 的 TV exclusion 列表或 digest 不一致")
            expected_sha = canonical_digest(tv_scopes)
        elif kind == "one_time_exact_title_scope":
            if (
                isinstance(batch.get("tv_exclusion_scope"), Mapping)
                or scope.get("tv_exclusion_scopes") is not None
                or closure.get("tv_exclusion_scopes") is not None
                or scope.get("movie_member_scopes") is not None
                or closure.get("movie_member_scopes") is not None
            ):
                raise ValueError("含嵌套排除的 TV 批次不得降级为普通作品范围")
            expected_sha = canonical_digest([target])
        else:
            raise ValueError("剧集导入必须来自精确作品范围")
    if (
        scope.get("scope_sha256") != expected_sha
        or closure.get("source_plan_sha256") != expected_sha
    ):
        raise ValueError("作品范围与复核证据 digest 不一致")
    return target


def prepare_import(
    worklist: Mapping[str, Any],
    scope: Mapping[str, Any],
    closure: Mapping[str, Any],
    *,
    title_work_key: str,
    live_control: Mapping[str, Any],
) -> dict[str, Any]:
    if not one_time_worklist_is_valid(worklist):
        raise ValueError("一次性工作清单 digest 无效")
    if not title_closure_evidence_is_valid(closure):
        raise ValueError("作品复核证据 digest 无效")
    if scope.get("title_work_key") != title_work_key:
        raise ValueError("作品范围 work key 不一致")
    if scope.get("worklist_sha256") != worklist.get("worklist_sha256"):
        raise ValueError("作品复核来自过期 worklist")
    before = scope.get("before_inventory")
    after = scope.get("after_inventory")
    if (
        not isinstance(before, Mapping) or not isinstance(after, Mapping)
        or before.get("inventory_sha256") != after.get("inventory_sha256")
    ):
        raise ValueError("作品复核前后 AList 指纹不一致")
    target = validate_scope_closure_binding(
        worklist, scope, closure, title_work_key=title_work_key,
    )
    if any(live_control.get(key) is not True for key in ("paused", "persistent")):
        raise ValueError("导入准备必须在全局持久暂停下进行")

    summary = closure.get("summary")
    targets = closure.get("title_targets")
    if not isinstance(summary, Mapping) or not isinstance(targets, list) or len(targets) != 1:
        raise ValueError("导入准备只允许一个精确作品")
    if targets[0] != target:
        raise ValueError("导入作品身份无效")
    episode_gaps = closure.get("episode_gaps")
    refinement = closure.get("subtitle_refinement")
    if not isinstance(episode_gaps, list) or not isinstance(refinement, Mapping):
        raise ValueError("导入缺口证据无效")
    subtitle_rows = [
        *list(refinement.get("confirmed_missing_chinese") or []),
        *list(refinement.get("pending_review_or_probe") or []),
    ]
    actions: list[dict[str, Any]] = []
    if episode_gaps:
        original_title = ""
        media = episode_gaps[0].get("media") if isinstance(episode_gaps[0], Mapping) else None
        if isinstance(media, Mapping):
            original_title = str(media.get("original_title") or "")
        request_bundle = build_replenishment_requests({
            "mode": "tv",
            "source_root": target["target_root"],
            "target_root": target["target_root"],
            "metadata": {
                "tmdb_id": target["tmdb_id"], "title": target["title"],
                "original_title": original_title,
                "series_root": target["target_root"],
            },
            "scan_report": {"resource_gaps": episode_gaps},
        }, job_id=f"one-time-{title_work_key[:16]}", round_number=1)
        if request_bundle.get("unresolved_gaps") or len(request_bundle.get("requests") or []) != 1:
            raise ValueError("作品缺集无法生成唯一补源请求")
        actions.append({
            "kind": "episode_replenishment",
            "request": request_bundle["requests"][0],
            "fresh_audit_required_before_dispatch": True,
        })
    if subtitle_rows:
        actions.append({
            "kind": "subtitle_resolution",
            "confirmed_gap_count": len(refinement.get("confirmed_missing_chinese") or []),
            "pending_verification_count": len(refinement.get("pending_review_or_probe") or []),
            "refinement_sha256": canonical_digest(refinement),
            "fresh_candidate_selection_required_before_dispatch": True,
            "video_replacement_allowed": False,
        })
    if not actions or summary.get("complete") is True:
        raise ValueError("作品没有可导入的未闭合缺口")
    core = {
        "schema_version": 1,
        "kind": "one_time_title_import_preparation",
        "status": "prepared_not_dispatched",
        "dispatch_allowed": False,
        "dispatch_blocker": "global_pause_active",
        "title_work_key": title_work_key,
        "worklist_sha256": worklist["worklist_sha256"],
        "scope_sha256": scope["scope_sha256"],
        "inventory_sha256": after["inventory_sha256"],
        "closure_evidence_sha256": closure["evidence_sha256"],
        "target": dict(target),
        "actions": actions,
        "safety": {
            "jobs_created": 0, "alist_mutations": 0,
            "synthetic_media_journal": False,
            "delivery_must_return_to_normal_inbox_scrape": True,
        },
    }
    return {**core, "import_sha256": canonical_digest(core)}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--worklist", type=Path, required=True)
    parser.add_argument("--title-work-key", required=True)
    parser.add_argument("--title-scope", type=Path, required=True)
    parser.add_argument("--title-closure", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--control-url", default="http://127.0.0.1:3010/api/control")
    args = parser.parse_args(argv)
    live_control = read_live_pause_control(args.control_url)
    result = prepare_import(
        load_json(args.worklist), load_json(args.title_scope),
        load_json(args.title_closure), title_work_key=args.title_work_key,
        live_control=live_control,
    )
    atomic_json(args.output, result)
    print(json.dumps({
        "output": str(args.output), "status": result["status"],
        "action_kinds": [row["kind"] for row in result["actions"]],
        "dispatch_allowed": False,
    }, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
