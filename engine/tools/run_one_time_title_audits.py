#!/usr/bin/env python3
"""Resume the sealed one-time library's exact-title read-only audits.

The runner only launches ``audit_one_time_title_batch.py``.  It never creates
jobs, dispatches replenishment, or writes to AList.  Every child gets its own
lock directory so at most three independent title roots can be read in
parallel without weakening the child's pause and inventory checks.
"""

from __future__ import annotations

import argparse
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import dataclass
import json
import os
from pathlib import Path
import subprocess
import sys
from typing import Any, Callable, Mapping, Sequence

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
    _movie_member_candidate_from_batch,
    _target_from_batch,
)
from engine.tools.plan_one_time_library_completion import read_live_pause_control
from local.scrapeflow_api.title_closure import title_closure_evidence_is_valid


@dataclass(frozen=True)
class AuditResult:
    title_work_key: str
    returncode: int
    stdout: str = ""
    stderr: str = ""


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


def _batches(worklist: Mapping[str, Any]) -> list[dict[str, Any]]:
    observations = worklist.get("observations")
    title_batches = observations.get("title_batches") if isinstance(observations, Mapping) else None
    raw = title_batches.get("batches") if isinstance(title_batches, Mapping) else None
    if not isinstance(raw, list):
        raise ValueError("工作清单缺少一次性作品批次")
    allowed = [dict(row) for row in raw if isinstance(row, Mapping)
               and row.get("read_only_audit_allowed") is True]
    keys = [row.get("title_work_key") for row in allowed]
    if any(not isinstance(key, str) or len(key) != 64 for key in keys):
        raise ValueError("一次性作品 work key 无效")
    if len(set(keys)) != len(keys):
        raise ValueError("一次性作品 work key 重复")
    return sorted(allowed, key=lambda row: str(row["title_work_key"]))


def _self_digested_inventory(value: Any) -> bool:
    if not isinstance(value, Mapping):
        return False
    digest = value.get("inventory_sha256")
    if not isinstance(digest, str):
        return False
    core = {key: item for key, item in value.items() if key != "inventory_sha256"}
    return canonical_digest(core) == digest


def _movie_inventory_is_valid(value: Any, movie_scope: Mapping[str, Any]) -> bool:
    if not isinstance(value, Mapping):
        return False
    member = value.get("member_inventory")
    parent = value.get("parent_inventory")
    core = {"member_inventory": member, "parent_inventory": parent}
    return bool(
        _self_digested_inventory(member)
        and _self_digested_inventory(parent)
        and member.get("target_stem") == movie_scope.get("target_stem")
        and parent.get("parent_root") == movie_scope.get("parent_root")
        and value.get("inventory_sha256") == canonical_digest(core)
    )


def evidence_is_verified(
    directory: Path, worklist: Mapping[str, Any], batch: Mapping[str, Any],
) -> bool:
    """Accept only fully bound, self-digested, unchanged-inventory evidence."""
    try:
        scope = load_json(directory / "title-scope.json")
        closure = load_json(directory / "title-closure.json")
        status = load_json(directory / "status.json")
        target = _target_from_batch(batch)
        expected_scope = [target]
        is_movie = target["media_type"] == "movie"
        is_tv_exclusion = (
            target["media_type"] == "tv"
            and isinstance(batch.get("tv_exclusion_scope"), Mapping)
        )
        expected_kind = "one_time_exact_title_scope"
        movie_scopes: list[dict[str, Any]] = []
        if is_movie:
            candidate = _movie_member_candidate_from_batch(batch)
            raw_scopes = scope.get("movie_member_scopes")
            if not isinstance(raw_scopes, list) or len(raw_scopes) != 1:
                return False
            movie_scope = validate_exact_movie_member_scope(raw_scopes[0])
            if not movie_scope_matches_candidate(movie_scope, candidate):
                return False
            movie_scopes = [movie_scope]
            scope_sha = canonical_digest(movie_scopes)
            expected_kind = "one_time_exact_movie_member_scope"
        elif is_tv_exclusion:
            raw_scopes = scope.get("tv_exclusion_scopes")
            if not isinstance(raw_scopes, list) or len(raw_scopes) != 1:
                return False
            tv_scope = validate_exact_tv_exclusion_scope(raw_scopes[0])
            if (
                batch.get("tv_exclusion_scope") != tv_scope
                or not tv_exclusion_scope_matches_batch(tv_scope, batch)
            ):
                return False
            tv_scopes = [tv_scope]
            scope_sha = canonical_digest(tv_scopes)
            expected_kind = "one_time_exact_tv_root_with_nested_exclusions"
        else:
            scope_sha = canonical_digest(expected_scope)
        before_sha = scope["before_inventory"]["inventory_sha256"]
        after_sha = scope["after_inventory"]["inventory_sha256"]
        status_digest = status["status_sha256"]
        status_core = {key: value for key, value in status.items() if key != "status_sha256"}
        expected_status = "complete" if closure["summary"]["complete"] else "incomplete"
        return bool(
            title_closure_evidence_is_valid(closure)
            and scope.get("title_work_key") == batch.get("title_work_key")
            and scope.get("worklist_sha256") == worklist.get("worklist_sha256")
            and scope.get("scope") == expected_scope
            and scope.get("scope_sha256") == scope_sha
            and (
                scope.get("source_scope_kind") == expected_kind
                if is_movie or is_tv_exclusion
                else scope.get("source_scope_kind") in {None, expected_kind}
            )
            and scope.get("remote_mutations") is False
            and scope.get("scheduler_dispatch") is False
            and before_sha == after_sha
            and (
                not is_movie
                or (
                    _movie_inventory_is_valid(scope.get("before_inventory"), movie_scopes[0])
                    and _movie_inventory_is_valid(scope.get("after_inventory"), movie_scopes[0])
                    and scope["before_inventory"] == scope["after_inventory"]
                    and closure.get("movie_member_scopes") == movie_scopes
                )
            )
            and (
                not is_tv_exclusion
                or (
                    _self_digested_inventory(scope.get("before_inventory"))
                    and _self_digested_inventory(scope.get("after_inventory"))
                    and scope["before_inventory"] == scope["after_inventory"]
                    and scope["before_inventory"].get("excluded_roots")
                    == tv_scopes[0]["excluded_roots"]
                    and scope["before_inventory"].get("excluded_member_paths_sha256")
                    == tv_scopes[0]["excluded_member_paths_sha256"]
                    and closure.get("tv_exclusion_scopes") == tv_scopes
                )
            )
            and closure.get("source_scope_kind") == expected_kind
            and closure.get("source_plan_sha256") == scope_sha
            and closure.get("title_targets") == expected_scope
            and status.get("title_work_key") == batch.get("title_work_key")
            and status.get("scope_sha256") == scope_sha
            and status.get("inventory_sha256") == after_sha
            and status.get("evidence_sha256") == closure.get("evidence_sha256")
            and status.get("status") == expected_status
            and status.get("remote_mutations") is False
            and isinstance(status_digest, str)
            and canonical_digest(status_core) == status_digest
        )
    except (FileNotFoundError, KeyError, TypeError, ValueError, json.JSONDecodeError):
        return False


def evidence_has_pending_subtitles(directory: Path) -> bool:
    """Return true only for validly shaped evidence that still requires OCR."""
    try:
        closure = load_json(directory / "title-closure.json")
        summary = closure.get("summary")
        return bool(
            isinstance(summary, Mapping)
            and isinstance(summary.get("pending_subtitle_verification_count"), int)
            and not isinstance(summary.get("pending_subtitle_verification_count"), bool)
            and summary["pending_subtitle_verification_count"] > 0
        )
    except (FileNotFoundError, TypeError, ValueError, json.JSONDecodeError):
        return False


def _evidence_directory(output_root: Path, worklist: Mapping[str, Any], batch: Mapping[str, Any]) -> tuple[Path, bool]:
    title_root = output_root / str(batch["title_work_key"])
    return title_root, evidence_is_verified(title_root, worklist, batch)


def run_audits(
    worklist: Mapping[str, Any], output_root: Path, *, max_workers: int,
    pause_unchanged: Callable[[], bool],
    invoke: Callable[[str, Path], AuditResult],
    progress_path: Path | None = None,
    rerun_pending_subtitles: bool = False,
) -> dict[str, Any]:
    """Run or resume safe title reads; dependencies are injectable for tests."""
    if not one_time_worklist_is_valid(worklist):
        raise ValueError("一次性工作清单 digest 无效或已被改动")
    if (worklist.get("dispatch_gate") or {}).get("reason") != "global_pause_active":
        raise ValueError("批量只读复核要求封存时已全局持久暂停")
    if not 1 <= max_workers <= 3:
        raise ValueError("一次性复核并发数只能为 1..3")
    if not pause_unchanged():
        raise ValueError("开始批量复核前持久暂停证据已变化")

    batches = _batches(worklist)
    skipped: list[str] = []
    pending: list[tuple[dict[str, Any], Path]] = []
    for batch in batches:
        directory, verified = _evidence_directory(output_root, worklist, batch)
        if verified and not (
            rerun_pending_subtitles and evidence_has_pending_subtitles(directory)
        ):
            skipped.append(str(batch["title_work_key"]))
        else:
            pending.append((batch, directory))

    completed: list[str] = []
    failed: dict[str, str] = {}
    halted = False

    def persist() -> None:
        if progress_path is None:
            return
        core = {
            "schema_version": 1,
            "kind": "one_time_title_audit_run",
            "worklist_sha256": worklist["worklist_sha256"],
            "total_allowed": len(batches),
            "verified_skipped": sorted(skipped),
            "completed_this_run": sorted(completed),
            "failed": dict(sorted(failed.items())),
            "pending_not_started": len(pending),
            "remaining_unverified": len(batches) - len(skipped) - len(completed),
            "halted_for_pause_change": halted,
            "remote_mutations": False,
            "scheduler_dispatch": False,
            "max_workers": max_workers,
            "rerun_pending_subtitles": rerun_pending_subtitles,
        }
        atomic_json(progress_path, {**core, "run_sha256": canonical_digest(core)})

    persist()
    running: dict[Future[AuditResult], tuple[dict[str, Any], Path]] = {}
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        while pending or running:
            while pending and len(running) < max_workers:
                if not pause_unchanged():
                    halted = True
                    break
                batch, directory = pending.pop(0)
                key = str(batch["title_work_key"])
                running[pool.submit(invoke, key, directory)] = (batch, directory)
            if not running:
                break
            done, _ = wait(running, return_when=FIRST_COMPLETED)
            for future in done:
                batch, directory = running.pop(future)
                key = str(batch["title_work_key"])
                try:
                    result = future.result()
                    if result.returncode == 0 and evidence_is_verified(directory, worklist, batch):
                        completed.append(key)
                    else:
                        detail = result.stderr.strip() or result.stdout.strip()
                        failed[key] = detail[-2000:] or f"exit={result.returncode}"
                except Exception as exc:  # keep other independent read-only titles resumable
                    failed[key] = f"{type(exc).__name__}: {exc}"
                if not pause_unchanged():
                    halted = True
                persist()
            if halted:
                # Already-running children enforce the same pause gate and are
                # allowed to finish; no additional title is started.
                while running:
                    done, _ = wait(running, return_when=FIRST_COMPLETED)
                    for future in done:
                        batch, directory = running.pop(future)
                        key = str(batch["title_work_key"])
                        try:
                            result = future.result()
                            if result.returncode == 0 and evidence_is_verified(directory, worklist, batch):
                                completed.append(key)
                            else:
                                failed[key] = (result.stderr.strip() or result.stdout.strip() or f"exit={result.returncode}")[-2000:]
                        except Exception as exc:
                            failed[key] = f"{type(exc).__name__}: {exc}"
                        persist()
                break

    persist()
    return {
        "total_allowed": len(batches), "verified_skipped": len(skipped),
        "completed_this_run": len(completed), "failed": len(failed),
        "remaining_unverified": len(batches) - len(skipped) - len(completed),
        "halted_for_pause_change": halted,
        "remote_mutations": False, "scheduler_dispatch": False,
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--worklist", type=Path, required=True)
    parser.add_argument("--global-control", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--workers", type=int, default=3, choices=(1, 2, 3))
    parser.add_argument("--control-url", default="http://127.0.0.1:3010/api/control")
    parser.add_argument("--alist-url")
    parser.add_argument("--tmdb-resolve-ip")
    parser.add_argument("--tmdb-direct", action="store_true")
    parser.add_argument("--tmdb-snapshot-root", type=Path)
    parser.add_argument("--docker-compose-network", action="store_true")
    parser.add_argument(
        "--reuse-episode-root", type=Path,
        help="复用同一 worklist 下一小时内的缺集证据，仅重做容器字幕复核",
    )
    parser.add_argument(
        "--rerun-pending-subtitles", action="store_true",
        help="仅重做已验证证据中仍需 OCR 的作品，其余继续安全跳过",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    worklist = load_json(args.worklist)
    durable = load_json(args.global_control)
    durable_digest = canonical_digest(durable)
    allowed_hosts = frozenset({"api"}) if args.docker_compose_network else frozenset()

    def pause_unchanged() -> bool:
        try:
            current = load_json(args.global_control)
            live = read_live_pause_control(
                args.control_url, allowed_hosts=allowed_hosts,
                host_header="localhost" if args.docker_compose_network else None,
            )
            return bool(
                isinstance(current, Mapping)
                and current.get("paused") is True
                and canonical_digest(current) == durable_digest
                and live.get("paused") is True
                and live.get("persistent") is True
            )
        except Exception:
            return False

    script = PROJECT_ROOT / "engine/tools/audit_one_time_title_batch.py"

    def invoke(key: str, directory: Path) -> AuditResult:
        command = [
            sys.executable, str(script), "--worklist", str(args.worklist),
            "--title-work-key", key, "--global-control", str(args.global_control),
            "--output-dir", str(directory), "--control-url", args.control_url,
        ]
        if args.alist_url:
            command.extend(("--alist-url", args.alist_url))
        if args.tmdb_resolve_ip:
            command.extend(("--tmdb-resolve-ip", args.tmdb_resolve_ip))
        if args.tmdb_direct:
            command.append("--tmdb-direct")
        if args.tmdb_snapshot_root:
            command.extend(("--tmdb-snapshot-root", str(args.tmdb_snapshot_root)))
        if args.docker_compose_network:
            command.append("--docker-compose-network")
        if args.reuse_episode_root:
            source = args.reuse_episode_root / key
            command.extend((
                "--reuse-episode-evidence", str(source / "title-closure.json"),
                "--reuse-episode-scope", str(source / "title-scope.json"),
            ))
        completed = subprocess.run(command, text=True, capture_output=True, check=False)
        return AuditResult(key, completed.returncode, completed.stdout, completed.stderr)

    result = run_audits(
        worklist, args.output_root.resolve(), max_workers=args.workers,
        pause_unchanged=pause_unchanged, invoke=invoke,
        progress_path=args.output_root.resolve() / "batch-progress.json",
        rerun_pending_subtitles=args.rerun_pending_subtitles,
    )
    print(json.dumps(result, ensure_ascii=False))
    return 0 if not result["failed"] and not result["halted_for_pause_change"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
