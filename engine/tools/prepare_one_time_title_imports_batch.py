#!/usr/bin/env python3
"""Prepare imports for every incomplete exact-title audit, without dispatching.

This is a local evidence transformer.  It does not create ScrapeFlow jobs,
call a replenishment adapter, mutate AList, write media, or resume scheduling.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import sys
from typing import Any, Callable, Mapping, Sequence

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from engine.scrapeflow.one_time_library_completion import (
    canonical_digest,
    one_time_worklist_is_valid,
)
from engine.tools.audit_one_time_title_batch import _target_from_batch
from engine.tools.plan_one_time_library_completion import read_live_pause_control
from engine.tools.prepare_one_time_title_import import (
    prepare_import,
    validate_scope_closure_binding,
)
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


def _allowed_batches(worklist: Mapping[str, Any]) -> list[dict[str, Any]]:
    observations = worklist.get("observations")
    title_batches = observations.get("title_batches") if isinstance(observations, Mapping) else None
    raw = title_batches.get("batches") if isinstance(title_batches, Mapping) else None
    if not isinstance(raw, list):
        raise ValueError("工作清单缺少一次性作品批次")
    batches = [dict(row) for row in raw if isinstance(row, Mapping)
               and row.get("read_only_audit_allowed") is True]
    keys = [row.get("title_work_key") for row in batches]
    if any(not isinstance(key, str) or len(key) != 64 for key in keys):
        raise ValueError("一次性作品 work key 无效")
    if len(keys) != len(set(keys)):
        raise ValueError("一次性作品 work key 重复")
    return sorted(batches, key=lambda row: str(row["title_work_key"]))


def _pause_is_active(control: Mapping[str, Any]) -> bool:
    return all(control.get(key) is True for key in ("paused", "persistent"))


def _validate_bound_evidence(
    worklist: Mapping[str, Any], batch: Mapping[str, Any],
    scope: Mapping[str, Any], closure: Mapping[str, Any],
) -> None:
    key = str(batch["title_work_key"])
    if not title_closure_evidence_is_valid(closure):
        raise ValueError("作品复核证据 digest 无效")
    target = _target_from_batch(batch)
    before = scope.get("before_inventory")
    after = scope.get("after_inventory")
    if scope.get("title_work_key") != key:
        raise ValueError("作品范围 work key 不一致")
    if scope.get("worklist_sha256") != worklist.get("worklist_sha256"):
        raise ValueError("作品复核来自过期 worklist")
    if (
        not isinstance(before, Mapping) or not isinstance(after, Mapping)
        or before.get("inventory_sha256") != after.get("inventory_sha256")
    ):
        raise ValueError("作品复核前后 AList 指纹不一致")
    bound_target = validate_scope_closure_binding(
        worklist, scope, closure, title_work_key=key,
    )
    if bound_target != target:
        raise ValueError("作品范围与封存批次或复核证据不一致")
    if scope.get("remote_mutations") is not False or scope.get("scheduler_dispatch") is not False:
        raise ValueError("作品复核不是纯只读证据")


def prepare_batch(
    worklist: Mapping[str, Any], audits_root: Path, output_dir: Path, *,
    read_pause_control: Callable[[], Mapping[str, Any]],
    progress_path: Path | None = None,
    summary_path: Path | None = None,
) -> dict[str, Any]:
    if not one_time_worklist_is_valid(worklist):
        raise ValueError("一次性工作清单 digest 无效或已被改动")
    if (worklist.get("dispatch_gate") or {}).get("reason") != "global_pause_active":
        raise ValueError("批量导入准备要求封存时已全局持久暂停")
    batches = _allowed_batches(worklist)
    prepared: list[str] = []
    complete_skipped: list[str] = []
    failures: dict[str, str] = {}
    stopped_for_pause_change = False
    progress = progress_path or output_dir / "batch-progress.json"
    summary_output = summary_path or output_dir / "batch-summary.json"

    def snapshot(kind: str) -> dict[str, Any]:
        core = {
            "schema_version": 1,
            "kind": kind,
            "worklist_sha256": worklist["worklist_sha256"],
            "allowed_count": len(batches),
            "processed_count": len(prepared) + len(complete_skipped) + len(failures),
            "prepared": sorted(prepared),
            "complete_skipped": sorted(complete_skipped),
            "failures": dict(sorted(failures.items())),
            "stopped_for_pause_change": stopped_for_pause_change,
            "remaining_count": len(batches) - len(prepared) - len(complete_skipped) - len(failures),
            "dispatch_allowed": False,
            "jobs_created": 0,
            "remote_mutations": False,
            "scheduler_dispatch": False,
        }
        return {**core, "batch_sha256": canonical_digest(core)}

    atomic_json(progress, snapshot("one_time_title_import_batch_progress"))
    for index, batch in enumerate(batches):
        key = str(batch["title_work_key"])
        control = read_pause_control()
        if not _pause_is_active(control):
            stopped_for_pause_change = True
            for remaining in batches[index:]:
                failures[str(remaining["title_work_key"])] = "global_persistent_pause_changed"
            atomic_json(progress, snapshot("one_time_title_import_batch_progress"))
            break
        audit_dir = audits_root / key
        try:
            scope = load_json(audit_dir / "title-scope.json")
            closure = load_json(audit_dir / "title-closure.json")
            if not isinstance(scope, Mapping) or not isinstance(closure, Mapping):
                raise ValueError("作品复核证据必须是 JSON 对象")
            _validate_bound_evidence(worklist, batch, scope, closure)
            summary = closure.get("summary")
            if not isinstance(summary, Mapping):
                raise ValueError("作品复核 summary 无效")
            if summary.get("complete") is True:
                complete_skipped.append(key)
            else:
                result = prepare_import(
                    worklist, scope, closure, title_work_key=key,
                    live_control=control,
                )
                if (
                    result.get("dispatch_allowed") is not False
                    or result.get("status") != "prepared_not_dispatched"
                    or (result.get("safety") or {}).get("jobs_created") != 0
                    or (result.get("safety") or {}).get("alist_mutations") != 0
                ):
                    raise ValueError("导入准备结果违反只读安全约束")
                atomic_json(output_dir / f"{key}.json", result)
                prepared.append(key)
        except (FileNotFoundError, KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
            failures[key] = f"{type(exc).__name__}: {exc}"
        atomic_json(progress, snapshot("one_time_title_import_batch_progress"))

    final = snapshot("one_time_title_import_batch_summary")
    atomic_json(summary_output, final)
    atomic_json(progress, snapshot("one_time_title_import_batch_progress"))
    return final


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--worklist", type=Path, required=True)
    parser.add_argument("--audits-root", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--global-control", type=Path, required=True)
    parser.add_argument("--control-url", default="http://127.0.0.1:3010/api/control")
    parser.add_argument("--docker-compose-network", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    worklist = load_json(args.worklist)
    initial_durable = load_json(args.global_control)
    durable_sha = canonical_digest(initial_durable)
    allowed_hosts = frozenset({"api"}) if args.docker_compose_network else frozenset()

    def read_pause() -> Mapping[str, Any]:
        try:
            durable = load_json(args.global_control)
            live = read_live_pause_control(
                args.control_url, allowed_hosts=allowed_hosts,
                host_header="localhost" if args.docker_compose_network else None,
            )
            unchanged = (
                isinstance(durable, Mapping)
                and durable.get("paused") is True
                and canonical_digest(durable) == durable_sha
            )
            return {
                "paused": unchanged and live.get("paused") is True,
                "persistent": unchanged and live.get("persistent") is True,
            }
        except Exception:
            return {"paused": False, "persistent": False}

    result = prepare_batch(
        worklist, args.audits_root.resolve(), args.output_dir.resolve(),
        read_pause_control=read_pause,
    )
    print(json.dumps({
        "summary": str(args.output_dir.resolve() / "batch-summary.json"),
        "prepared_count": len(result["prepared"]),
        "complete_skipped_count": len(result["complete_skipped"]),
        "failure_count": len(result["failures"]),
        "dispatch_allowed": False,
    }, ensure_ascii=False))
    return 1 if result["failures"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
