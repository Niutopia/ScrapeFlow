#!/usr/bin/env python3
"""Fetch short-lived official TMDB identity snapshots for phase-2 movies.

This host-side helper performs only official TMDB GET requests and exclusive
local JSON creates.  It does not connect to AList or create ScrapeFlow work.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import sys
from typing import Any, Mapping

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from engine.scrapeflow.one_time_library_completion import canonical_digest
from engine.scrapeflow.one_time_tmdb_snapshot import seal_movie_snapshot
from engine.tools.audit_one_time_title_batch import CurlPinnedTMDBClient, load_connection_env
from engine.tools.plan_one_time_library_completion import read_live_pause_control
from engine.tools.plan_one_time_movie_scope_worklist import (
    _write_new_json,
    phase_two_movie_worklist_is_valid,
)


def load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--worklist", type=Path, required=True)
    parser.add_argument("--global-control", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--tmdb-resolve-ip", required=True)
    parser.add_argument("--control-url", default="http://127.0.0.1:3010/api/control")
    return parser


def main(argv: list[str] | None = None) -> int:
    load_connection_env()
    args = build_parser().parse_args(argv)
    worklist = load_json(args.worklist)
    durable = load_json(args.global_control)
    if not isinstance(worklist, Mapping) or not phase_two_movie_worklist_is_valid(worklist):
        raise ValueError("phase-2 电影工作清单无效或已被改动")
    if not isinstance(durable, Mapping) or durable.get("paused") is not True:
        raise ValueError("TMDB 快照准备要求持久暂停")
    durable_digest = canonical_digest(durable)
    live = read_live_pause_control(args.control_url)
    if any(live.get(key) is not True for key in ("paused", "persistent")):
        raise ValueError("TMDB 快照准备要求实时调度器保持持久暂停")
    key = os.getenv("TMDB_API_KEY")
    if not key:
        raise ValueError("TMDB 快照准备缺少 API Key")
    client = CurlPinnedTMDBClient(key, args.tmdb_resolve_ip)
    batches = worklist["observations"]["title_batches"]["batches"]
    ids = sorted({
        int(batch["identity"]["tmdb_id"])
        for batch in batches
        if batch.get("read_only_audit_allowed") is True
    })
    fetched_at = datetime.now(timezone.utc).isoformat()
    created = 0
    reused = 0
    for tmdb_id in ids:
        output = args.output_root / f"movie-{tmdb_id}.json"
        if output.exists():
            reused += 1
            continue
        payload = client.get(f"/movie/{tmdb_id}")
        _write_new_json(output, seal_movie_snapshot(payload, fetched_at=fetched_at))
        created += 1
    if canonical_digest(load_json(args.global_control)) != durable_digest:
        raise RuntimeError("准备 TMDB 快照期间持久暂停文件发生变化")
    current_live = read_live_pause_control(args.control_url)
    if any(current_live.get(key) is not True for key in ("paused", "persistent")):
        raise RuntimeError("准备 TMDB 快照期间实时暂停发生变化")
    print(json.dumps({
        "output_root": str(args.output_root),
        "movie_ids": len(ids),
        "created": created,
        "reused_existing": reused,
        "tmdb_transport": client.cache_report(),
        "alist_requests": 0,
        "remote_mutations": False,
    }, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
