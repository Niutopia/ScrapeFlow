#!/usr/bin/env python3
"""Explicitly audit one exact title from a sealed one-time worklist.

This command is read-only with respect to AList.  It creates no ScrapeFlow
job, media plan, journal, scheduler entry, or replenishment request.
"""

from __future__ import annotations

import argparse
from contextlib import contextmanager
from datetime import datetime, timezone
import fcntl
import json
import os
from pathlib import Path
import subprocess
import sys
from typing import Any, Iterator, Mapping
import urllib.parse

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from engine.scraper import AListClient, TMDBClient
from engine.scrapeflow.one_time_library_completion import (
    canonical_digest,
    one_time_worklist_is_valid,
)
from engine.scrapeflow.one_time_movie_member_scope import (
    resolve_exact_movie_member_scope,
    validate_movie_member_candidate,
)
from engine.scrapeflow.one_time_tv_exclusion_scope import (
    ExactTVExclusionAListView,
    tv_exclusion_scope_matches_batch,
    validate_exact_tv_exclusion_scope,
)
from engine.scrapeflow.one_time_tmdb_snapshot import SealedMovieSnapshotClient
from engine.tools.audit_live_library import temporary_tmdb_dns_override
from engine.tools.plan_one_time_library_completion import read_live_pause_control
from local.scrapeflow_api.title_closure import (
    TitleClosureAdapters,
    build_exact_movie_member_closure_evidence,
    build_exact_title_closure_evidence,
    build_exact_tv_exclusion_closure_evidence,
    title_closure_evidence_is_valid,
)
from local.scrapeflow_api.title_closure_runtime import (
    make_burned_in_ocr_adapter,
    make_current_title_episode_gap_scanner,
    make_current_tv_exclusion_episode_gap_scanner,
)


class CurlPinnedTMDBClient:
    """Small read-only TMDB transport for networks that reset urllib TLS."""

    def __init__(self, api_key: str, resolve_ip: str, *, language: str = "zh-CN"):
        self.api_key = api_key
        self.resolve_ip = resolve_ip
        self.language = language
        self._cache: dict[str, dict[str, Any]] = {}
        self._requests = 0

    def get(self, path: str, **params: Any) -> dict[str, Any]:
        if not isinstance(path, str) or not path.startswith("/"):
            raise ValueError("TMDB path 必须是绝对 API 路径")
        query = {"api_key": self.api_key, "language": self.language}
        query.update({key: value for key, value in params.items() if value is not None})
        url = "https://api.themoviedb.org/3" + path + "?" + urllib.parse.urlencode(
            sorted(query.items()), doseq=True,
        )
        cache_key = path + "?" + urllib.parse.urlencode(
            sorted((key, value) for key, value in query.items() if key != "api_key"),
            doseq=True,
        )
        if cache_key in self._cache:
            return self._cache[cache_key]
        config = (
            f'url = "{url}"\n'
            f'resolve = "api.themoviedb.org:443:{self.resolve_ip}"\n'
            'max-time = 25\nretry = 2\nretry-delay = 1\nretry-all-errors\n'
            'silent\nshow-error\nwrite-out = "\\n%{http_code}"\n'
        )
        environment = dict(os.environ)
        for key in (
            "HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY",
            "http_proxy", "https_proxy", "all_proxy",
        ):
            environment.pop(key, None)
        completed = subprocess.run(
            ["curl", "--config", "-"], input=config, text=True,
            capture_output=True, timeout=90, env=environment, check=False,
        )
        body, separator, status = completed.stdout.rpartition("\n")
        if (
            not separator or completed.returncode != 0 or status != "200"
            or len(body.encode("utf-8")) > 8 * 1024 * 1024
        ):
            raise RuntimeError(
                f"TMDB curl 读取失败: exit={completed.returncode} status={status or 'missing'}"
            )
        payload = json.loads(body)
        if not isinstance(payload, dict):
            raise ValueError("TMDB curl 返回格式无效")
        self._requests += 1
        self._cache[cache_key] = payload
        return payload

    def cache_report(self) -> dict[str, Any]:
        return {
            "transport": "curl_pinned_official_tls",
            "request_count": self._requests,
            "entry_count": len(self._cache),
        }


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


def load_connection_env(path: Path | None = None) -> None:
    source = path or PROJECT_ROOT / ".env.local"
    if not source.exists():
        return
    allowed = {
        "ALIST_URL", "ALIST_USERNAME", "ALIST_PASSWORD", "TMDB_API_KEY",
        "TMDB_BASE_URL", "TMDB_IMAGE_BASE_URL", "TMDB_PROXY_URL", "TMDB_LANGUAGE",
    }
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


def exact_title_inventory(
    client: AListClient, root: str,
    *, tv_exclusion_scope: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    scope = (
        validate_exact_tv_exclusion_scope(tv_exclusion_scope)
        if tv_exclusion_scope is not None else None
    )
    if scope is not None and scope["target_root"] != root:
        raise ValueError("TV 指纹范围与作品根不一致")
    reader: Any = ExactTVExclusionAListView(client, scope) if scope is not None else client
    rows = reader.walk(
        root, refresh=True, max_directories=10_000, max_files=200_000,
        include_bonus=True, include_title_extras=True,
        excluded_roots=scope["excluded_roots"] if scope is not None else [],
    )
    stable = []
    for raw in rows:
        path = raw.get("full_path")
        if not isinstance(path, str) or not (path == root or path.startswith(root + "/")):
            raise ValueError("作品指纹扫描返回了超出目标根的路径")
        stable.append({
            "path": path,
            "is_dir": raw.get("is_dir") is True,
            "size": raw.get("size"),
            "modified": raw.get("modified"),
            "hash": raw.get("hash_info") or raw.get("hash"),
        })
    stable.sort(key=lambda row: row["path"].casefold())
    core = {
        "target_root": root,
        "excluded_roots": scope["excluded_roots"] if scope is not None else [],
        "excluded_member_paths_sha256": (
            scope["excluded_member_paths_sha256"] if scope is not None else None
        ),
        "entries": stable,
    }
    return {**core, "inventory_sha256": canonical_digest(core)}


@contextmanager
def exclusive_run_lock(path: Path) -> Iterator[None]:
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    with path.open("a+", encoding="utf-8") as handle:
        os.chmod(path, 0o600)
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise RuntimeError("已有一个一次性作品复核在运行") from exc
        try:
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


@contextmanager
def direct_tmdb_network(enabled: bool) -> Iterator[None]:
    if not enabled:
        yield
        return
    keys = (
        "TMDB_PROXY_URL", "HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY",
        "http_proxy", "https_proxy", "all_proxy",
    )
    previous = {key: os.environ.get(key) for key in keys}
    for key in keys:
        os.environ.pop(key, None)
    try:
        yield
    finally:
        for key, value in previous.items():
            if value is not None:
                os.environ[key] = value


def _find_batch(worklist: Mapping[str, Any], work_key: str) -> dict[str, Any]:
    observations = worklist.get("observations")
    title_batches = observations.get("title_batches") if isinstance(observations, Mapping) else None
    batches = title_batches.get("batches") if isinstance(title_batches, Mapping) else None
    if not isinstance(batches, list):
        raise ValueError("工作清单缺少一次性作品批次")
    matches = [row for row in batches if isinstance(row, Mapping) and row.get("title_work_key") == work_key]
    if len(matches) != 1:
        raise ValueError("一次性作品 work key 不唯一或不存在")
    return dict(matches[0])


def _target_from_batch(batch: Mapping[str, Any]) -> dict[str, Any]:
    identity = batch.get("identity")
    if not isinstance(identity, Mapping) or identity.get("status") != "exact":
        raise ValueError("一次性作品批次缺少精确身份")
    return {
        "media_type": identity.get("media_type"),
        "target_root": batch.get("target_root"),
        "category": batch.get("category"),
        "tmdb_id": identity.get("tmdb_id"),
        "title": identity.get("title"),
    }


def _movie_member_candidate_from_batch(batch: Mapping[str, Any]) -> dict[str, Any]:
    identity = batch.get("identity")
    if not isinstance(identity, Mapping) or identity.get("media_type") != "movie":
        raise ValueError("一次性电影成员批次身份无效")
    candidate = validate_movie_member_candidate(batch.get("movie_member_candidate"))
    target = _target_from_batch(batch)
    expected = {
        "target_stem": target["target_root"],
        "category": target["category"],
        "tmdb_id": target["tmdb_id"],
        "title": target["title"],
    }
    if any(candidate.get(key) != value for key, value in expected.items()):
        raise ValueError("电影成员候选与封存批次身份不一致")
    return candidate


def _movie_inventory_envelope(resolved: Mapping[str, Any]) -> dict[str, Any]:
    member = resolved.get("member_inventory")
    parent = resolved.get("parent_inventory")
    if not isinstance(member, Mapping) or not isinstance(parent, Mapping):
        raise ValueError("电影成员解析缺少成员/父目录指纹")
    core = {
        "member_inventory": dict(member),
        "parent_inventory": dict(parent),
    }
    return {**core, "inventory_sha256": canonical_digest(core)}


def _tv_exclusion_scope_from_batch(batch: Mapping[str, Any]) -> dict[str, Any]:
    raw = batch.get("tv_exclusion_scope")
    scope = validate_exact_tv_exclusion_scope(raw)
    if not tv_exclusion_scope_matches_batch(scope, batch):
        raise ValueError("TV 嵌套排除范围与 phase-3 批次身份不一致")
    return scope


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--worklist", type=Path, required=True)
    parser.add_argument("--title-work-key", required=True)
    parser.add_argument("--global-control", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--control-url", default="http://127.0.0.1:3010/api/control")
    parser.add_argument("--alist-url", default=None)
    parser.add_argument("--tmdb-resolve-ip")
    parser.add_argument("--tmdb-direct", action="store_true")
    parser.add_argument("--tmdb-snapshot-root", type=Path)
    parser.add_argument("--reuse-episode-evidence", type=Path)
    parser.add_argument("--reuse-episode-scope", type=Path)
    parser.add_argument(
        "--docker-compose-network", action="store_true",
        help="仅允许 Compose 内网中的 api/alist 服务名",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    load_connection_env()
    args = build_parser().parse_args(argv)
    worklist = load_json(args.worklist)
    if not isinstance(worklist, Mapping) or not one_time_worklist_is_valid(worklist):
        raise ValueError("一次性工作清单 digest 无效或已被改动")
    if (worklist.get("dispatch_gate") or {}).get("reason") != "global_pause_active":
        raise ValueError("一次性只读复核要求工作清单生成时已持久暂停")
    batch = _find_batch(worklist, args.title_work_key)
    if batch.get("read_only_audit_allowed") is not True:
        raise ValueError("一次性作品目标包含嵌套的其他作品身份，拒绝扩大复核范围")
    target = _target_from_batch(batch)
    is_movie_member = target["media_type"] == "movie"
    is_tv_exclusion = (
        target["media_type"] == "tv"
        and isinstance(batch.get("tv_exclusion_scope"), Mapping)
    )
    initial_control_file = load_json(args.global_control)
    initial_control_digest = canonical_digest(initial_control_file)

    def stop_requested() -> bool:
        try:
            durable = load_json(args.global_control)
            live = read_live_pause_control(
                args.control_url,
                allowed_hosts=frozenset({"api"}) if args.docker_compose_network else frozenset(),
                host_header="localhost" if args.docker_compose_network else None,
            )
        except Exception:
            return True
        return not (
            isinstance(durable, Mapping)
            and durable.get("paused") is True
            and canonical_digest(durable) == initial_control_digest
            and live.get("paused") is True
            and live.get("persistent") is True
        )

    if stop_requested():
        raise ValueError("开始一次性复核前持久暂停证据已变化")
    password = os.getenv("ALIST_PASSWORD")
    tmdb_key = os.getenv("TMDB_API_KEY")
    if not password or not tmdb_key:
        raise ValueError("一次性作品复核需要 ALIST_PASSWORD 和 TMDB_API_KEY")
    alist_url = args.alist_url or os.getenv("ALIST_URL", "http://127.0.0.1:5244")
    alist_host = urllib.parse.urlsplit(alist_url).hostname
    trusted_compose_alist = args.docker_compose_network and alist_host == "alist"
    client = AListClient(
        alist_url, os.getenv("ALIST_USERNAME", "admin"), password,
        timeout=20, retries=1,
        allow_insecure_http=(
            alist_url.startswith(("http://127.0.0.1", "http://localhost"))
            or trusted_compose_alist
        ),
    )
    client.login()
    if args.tmdb_snapshot_root is not None:
        if not is_movie_member:
            raise ValueError("TMDB 电影快照禁止用于非电影作品")
        tmdb = SealedMovieSnapshotClient(args.tmdb_snapshot_root.resolve())
    else:
        tmdb = (
            CurlPinnedTMDBClient(tmdb_key, args.tmdb_resolve_ip)
            if args.tmdb_direct and args.tmdb_resolve_ip
            else TMDBClient(tmdb_key)
        )

    output = args.output_dir.resolve()
    # Worklist validation already rejects overlapping runnable title roots.
    # A per-title lock keeps retries idempotent while allowing a small bounded
    # pool to audit independent titles concurrently.
    lock_path = output / ".audit.lock"
    with exclusive_run_lock(lock_path), direct_tmdb_network(args.tmdb_direct), temporary_tmdb_dns_override(args.tmdb_resolve_ip):
        movie_scopes: list[dict[str, Any]] = []
        tv_exclusion_scopes: list[dict[str, Any]] = []
        if is_movie_member:
            candidate = _movie_member_candidate_from_batch(batch)
            if stop_requested():
                raise ValueError("解析电影精确成员前持久暂停证据已变化")
            before_resolution = resolve_exact_movie_member_scope(
                client, candidate, tmdb=tmdb,
            )
            if stop_requested():
                raise ValueError("解析电影精确成员后持久暂停证据已变化")
            movie_scopes = [dict(before_resolution["scope"])]
            scope_sha256 = canonical_digest(movie_scopes)
            before = _movie_inventory_envelope(before_resolution)
        elif is_tv_exclusion:
            tv_scope = _tv_exclusion_scope_from_batch(batch)
            tv_exclusion_scopes = [tv_scope]
            scope_sha256 = canonical_digest(tv_exclusion_scopes)
            before = exact_title_inventory(
                client, target["target_root"], tv_exclusion_scope=tv_scope,
            )
        else:
            scope_sha256 = canonical_digest([target])
            before = exact_title_inventory(client, target["target_root"])
        reused_episode_digest: str | None = None
        if args.reuse_episode_evidence or args.reuse_episode_scope:
            if is_movie_member:
                raise ValueError("电影成员复核不允许复用目录级缺集证据")
            if not args.reuse_episode_evidence or not args.reuse_episode_scope:
                raise ValueError("复用缺集证据必须同时提供 closure 和 scope")
            prior = load_json(args.reuse_episode_evidence)
            prior_scope = load_json(args.reuse_episode_scope)
            expected_reuse_kind = (
                "one_time_exact_tv_root_with_nested_exclusions"
                if is_tv_exclusion else "one_time_exact_title_scope"
            )
            tv_scope_reuse_matches = (
                not is_tv_exclusion
                or (
                    isinstance(prior, Mapping)
                    and isinstance(prior_scope, Mapping)
                    and prior.get("tv_exclusion_scopes") == tv_exclusion_scopes
                    and prior_scope.get("tv_exclusion_scopes") == tv_exclusion_scopes
                )
            )
            if (
                not isinstance(prior, Mapping)
                or not title_closure_evidence_is_valid(prior)
                or prior.get("source_scope_kind") != expected_reuse_kind
                or prior.get("source_plan_sha256") != scope_sha256
                or prior.get("title_targets") != [target]
                or not isinstance(prior_scope, Mapping)
                or prior_scope.get("scope_sha256") != scope_sha256
                or not tv_scope_reuse_matches
                or (prior_scope.get("after_inventory") or {}).get("inventory_sha256")
                != before["inventory_sha256"]
            ):
                raise ValueError("复用缺集证据与当前作品范围或 AList 指纹不一致")
            audited_at = prior.get("audited_at")
            try:
                age = datetime.now(timezone.utc) - datetime.fromisoformat(str(audited_at))
            except ValueError as exc:
                raise ValueError("复用缺集证据时间无效") from exc
            if age.total_seconds() < 0 or age.total_seconds() > 3600:
                raise ValueError("复用缺集证据已超过一小时")
            prior_gaps = prior.get("episode_gaps")
            if not isinstance(prior_gaps, list) or not all(
                isinstance(row, Mapping) for row in prior_gaps
            ):
                raise ValueError("复用缺集证据格式无效")
            reused_episode_digest = str(prior.get("evidence_sha256"))
            episode_scanner = lambda _target: [dict(row) for row in prior_gaps]
        else:
            episode_scanner = (
                (lambda _target: [])
                if is_movie_member
                else make_current_tv_exclusion_episode_gap_scanner(
                    client, tmdb, tv_scope,
                )
                if is_tv_exclusion
                else make_current_title_episode_gap_scanner(client, tmdb)
            )
        adapters = TitleClosureAdapters(
            scan_episode_gaps=episode_scanner,
            probe_burned_in_ocr=make_burned_in_ocr_adapter(
                timeout=30, evidence_root=output / "ocr-evidence",
            ),
        )
        if is_movie_member:
            evidence = build_exact_movie_member_closure_evidence(
                movie_scopes, scope_sha256, alist=client,
                pause_active=stop_requested, adapters=adapters,
            )
            if stop_requested():
                raise ValueError("复核电影精确成员后持久暂停证据已变化")
            after_resolution = resolve_exact_movie_member_scope(
                client, candidate, tmdb=tmdb,
            )
            if after_resolution["scope"] != movie_scopes[0]:
                raise RuntimeError("电影精确成员集合在复核期间发生变化")
            after = _movie_inventory_envelope(after_resolution)
        elif is_tv_exclusion:
            evidence = build_exact_tv_exclusion_closure_evidence(
                tv_exclusion_scopes, scope_sha256, alist=client,
                pause_active=stop_requested, adapters=adapters,
            )
            after = exact_title_inventory(
                client, target["target_root"], tv_exclusion_scope=tv_scope,
            )
        else:
            evidence = build_exact_title_closure_evidence(
                [target], scope_sha256, alist=client,
                pause_active=stop_requested, adapters=adapters,
            )
            after = exact_title_inventory(client, target["target_root"])
        if before["inventory_sha256"] != after["inventory_sha256"]:
            atomic_json(output / "status.json", {
                "status": "changed_during_audit", "title_work_key": args.title_work_key,
                "before_sha256": before["inventory_sha256"],
                "after_sha256": after["inventory_sha256"],
            })
            raise RuntimeError("作品文件在复核期间发生变化，证据已作废")
        if stop_requested() or not title_closure_evidence_is_valid(evidence):
            raise RuntimeError("一次性作品复核的暂停门或证据 digest 无效")
        scope_record = {
            "schema_version": 1, "title_work_key": args.title_work_key,
            "worklist_sha256": worklist["worklist_sha256"],
            "scope": [target], "scope_sha256": scope_sha256,
            "source_scope_kind": evidence["source_scope_kind"],
            "reused_episode_evidence_sha256": reused_episode_digest,
            "before_inventory": before, "after_inventory": after,
            "remote_mutations": False, "scheduler_dispatch": False,
        }
        if is_movie_member:
            scope_record["movie_member_scopes"] = movie_scopes
        if is_tv_exclusion:
            scope_record["tv_exclusion_scopes"] = tv_exclusion_scopes
        atomic_json(output / "title-scope.json", scope_record)
        atomic_json(output / "title-closure.json", evidence)
        status = "complete" if evidence["summary"]["complete"] else "incomplete"
        status_core = {
            "schema_version": 1, "status": status,
            "title_work_key": args.title_work_key,
            "scope_sha256": scope_sha256,
            "inventory_sha256": after["inventory_sha256"],
            "evidence_sha256": evidence["evidence_sha256"],
            "remote_mutations": False,
        }
        atomic_json(output / "status.json", {
            **status_core, "status_sha256": canonical_digest(status_core),
        })
    print(json.dumps({
        "output": str(output), "status": status,
        "summary": evidence["summary"], "remote_mutations": False,
    }, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
