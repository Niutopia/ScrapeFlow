#!/usr/bin/env python3
"""按 JSON 清单生成计划，或执行已审核的计划；任何任务失败即停止。"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from pathlib import Path
from typing import Any


def reject_duplicate_object_pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"JSON 对象包含重复字段: {key!r}")
        result[key] = value
    return result


def require_bool(task: dict[str, Any], key: str, index: int) -> bool | None:
    value = task.get(key)
    if value is None:
        return None
    if type(value) is not bool:
        raise SystemExit(f"第 {index} 个任务 {key} 必须是 JSON 布尔值")
    return value


def resolve_path(base: Path, value: str, field: str, index: int) -> Path:
    if not isinstance(value, str) or not value.strip():
        raise SystemExit(f"第 {index} 个任务 {field} 必须是非空字符串")
    path = Path(value)
    return path if path.is_absolute() else base / path


def run(cmd: list[str], index: int) -> None:
    print("运行:", subprocess.list2cmdline(cmd))
    try:
        subprocess.run(cmd, check=True)
    except subprocess.CalledProcessError as exc:
        raise SystemExit(f"第 {index} 个任务失败，退出码 {exc.returncode}") from exc


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("manifest", type=Path)
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args()

    try:
        raw = json.loads(
            args.manifest.read_text(encoding="utf-8"),
            parse_constant=lambda value: (_ for _ in ()).throw(
                ValueError(f"JSON 不允许非有限数值: {value}")
            ),
            object_pairs_hook=reject_duplicate_object_pairs,
        )
    except (OSError, ValueError) as exc:
        raise SystemExit(f"无法读取 manifest: {exc}") from exc
    if not isinstance(raw, list):
        raise SystemExit("manifest 必须是任务数组")

    scraper_path = Path(__file__).resolve().parents[1] / "scraper.py"
    base = args.manifest.resolve().parent
    for index, task in enumerate(raw, 1):
        if not isinstance(task, dict):
            raise SystemExit(f"第 {index} 个任务不是对象")

        execute_fields = {
            "execute_plan", "approve_plan_sha256", "journal", "skip_poster",
            "overwrite_poster", "cleanup_empty_source", "allow_insecure_http",
        }
        generation_fields = {
            "src", "parent", "type", "id", "plan_json", "season", "absolute",
            "allow_unmapped", "prefer_simplified", "collection_map", "episode_map", "episode_group",
            "allow_index_mapping", "ignore_orphan_temp", "allow_insecure_http",
            "auto_match", "query", "min_confidence",
        }
        allowed_fields = execute_fields if args.execute else generation_fields
        unknown_fields = sorted(set(task) - allowed_fields)
        if unknown_fields:
            raise SystemExit(
                f"第 {index} 个任务包含当前模式不支持的字段: "
                + ", ".join(unknown_fields)
            )

        cmd = [sys.executable, str(scraper_path)]
        if args.execute:
            plan_value = task.get("execute_plan")
            digest = task.get("approve_plan_sha256")
            plan_path = resolve_path(base, plan_value, "execute_plan", index)
            if not isinstance(digest, str) or not re.fullmatch(r"[0-9a-fA-F]{64}", digest):
                raise SystemExit(
                    f"第 {index} 个执行任务 approve_plan_sha256 必须是完整 64 位十六进制值"
                )
            cmd.extend(
                [
                    "--execute-plan",
                    str(plan_path),
                    "--approve-plan-sha256",
                    digest,
                    "--execute",
                ]
            )
            journal = task.get("journal")
            if journal is not None:
                cmd.extend(["--journal", str(resolve_path(base, journal, "journal", index))])
            skip_poster = require_bool(task, "skip_poster", index)
            overwrite_poster = require_bool(task, "overwrite_poster", index)
            if skip_poster and overwrite_poster:
                raise SystemExit(
                    f"第 {index} 个任务不能同时设置 skip_poster 与 overwrite_poster"
                )
            for key, flag in (
                ("skip_poster", "--skip-poster"),
                ("overwrite_poster", "--overwrite-poster"),
                ("cleanup_empty_source", "--cleanup-empty-source"),
                ("allow_insecure_http", "--allow-insecure-http"),
            ):
                if require_bool(task, key, index) is True:
                    cmd.append(flag)
            run(cmd, index)
            continue

        src = task.get("src")
        parent = task.get("parent")
        tmdb_id = task.get("id")
        media_type = task.get("type")
        if not isinstance(src, str) or not src.strip():
            raise SystemExit(f"第 {index} 个任务 src 必须是非空字符串")
        if not isinstance(parent, str) or not parent.strip():
            raise SystemExit(f"第 {index} 个任务 parent 必须是非空字符串")
        if not isinstance(media_type, str) or media_type not in {"tv", "movie", "collection", "auto"}:
            raise SystemExit(f"第 {index} 个任务 type 无效: {media_type!r}")
        for boolean_key in (
            "absolute", "allow_unmapped", "prefer_simplified",
            "allow_index_mapping", "ignore_orphan_temp", "allow_insecure_http", "auto_match",
        ):
            require_bool(task, boolean_key, index)
        auto_match = task.get("auto_match") is True or media_type == "auto"
        if tmdb_id is not None and auto_match:
            raise SystemExit(f"第 {index} 个任务不能同时提供 id 和自动匹配")
        if tmdb_id is None and not auto_match:
            raise SystemExit(f"第 {index} 个任务必须提供 id 或启用 auto_match")
        if tmdb_id is not None and (
            isinstance(tmdb_id, bool) or not isinstance(tmdb_id, int) or tmdb_id <= 0
        ):
            raise SystemExit(f"第 {index} 个任务 id 必须是正整数")
        query = task.get("query")
        if query is not None and (not isinstance(query, str) or not query.strip()):
            raise SystemExit(f"第 {index} 个任务 query 必须是非空字符串")
        if query is not None and not auto_match:
            raise SystemExit(f"第 {index} 个任务 query 只能用于自动匹配")
        min_confidence = task.get("min_confidence")
        if min_confidence is not None:
            if isinstance(min_confidence, bool) or not isinstance(min_confidence, (int, float)):
                raise SystemExit(f"第 {index} 个任务 min_confidence 必须是 0 到 1 的数字")
            if not 0 <= float(min_confidence) <= 1:
                raise SystemExit(f"第 {index} 个任务 min_confidence 必须在 0 到 1 之间")
        if media_type == "tv":
            if "collection_map" in task or task.get("allow_index_mapping") is True:
                raise SystemExit(f"第 {index} 个电视剧任务包含合集参数")
            if task.get("absolute") is True and task.get("allow_unmapped") is True:
                raise SystemExit(
                    f"第 {index} 个电视剧任务不能同时启用 absolute 与 allow_unmapped"
                )
            episode_group = task.get("episode_group")
            if episode_group is not None:
                if not isinstance(episode_group, str) or not re.fullmatch(r"[A-Za-z0-9_-]{1,100}", episode_group):
                    raise SystemExit(f"第 {index} 个任务 episode_group 格式无效")
                if task.get("absolute") is not True:
                    raise SystemExit(f"第 {index} 个任务 episode_group 必须配合 absolute=true")
        elif media_type == "movie":
            irrelevant = {
                "season", "absolute", "allow_unmapped", "prefer_simplified",
                "collection_map", "episode_map", "episode_group", "allow_index_mapping",
            } & set(task)
            if irrelevant:
                raise SystemExit(
                    f"第 {index} 个电影任务包含不适用字段: "
                    + ", ".join(sorted(irrelevant))
                )
        elif media_type == "collection":
            irrelevant = {
                "season", "absolute", "allow_unmapped", "prefer_simplified", "episode_map", "episode_group"
            } & set(task)
            if irrelevant:
                raise SystemExit(
                    f"第 {index} 个合集任务包含不适用字段: "
                    + ", ".join(sorted(irrelevant))
                )
            if "collection_map" in task and task.get("allow_index_mapping") is True:
                raise SystemExit(
                    f"第 {index} 个合集任务只能选择 collection_map 或 allow_index_mapping"
                )
            if auto_match:
                raise SystemExit(f"第 {index} 个合集任务不支持自动匹配")

        plan_value = task.get("plan_json")
        plan_path = resolve_path(base, plan_value, "plan_json", index)

        if tmdb_id is not None:
            cmd.extend(["--id", str(tmdb_id)])
        cmd.extend(
            ["--parent", parent, "--type", media_type, "--plan-json", str(plan_path)]
        )
        if task.get("auto_match") is True:
            cmd.append("--auto-match")
        if query is not None:
            cmd.extend(["--query", query])
        if min_confidence is not None:
            cmd.extend(["--min-confidence", str(float(min_confidence))])
        season = task.get("season")
        if season is not None:
            if isinstance(season, bool) or not isinstance(season, int) or season < 0:
                raise SystemExit(f"第 {index} 个任务 season 必须是非负整数")
            cmd.extend(["--season", str(season)])
        for key, flag in (
            ("absolute", "--absolute"),
            ("allow_unmapped", "--allow-unmapped"),
            ("prefer_simplified", "--prefer-simplified"),
            ("allow_index_mapping", "--allow-index-mapping"),
            ("ignore_orphan_temp", "--ignore-orphan-temp"),
            ("allow_insecure_http", "--allow-insecure-http"),
        ):
            if require_bool(task, key, index) is True:
                cmd.append(flag)

        collection_map = task.get("collection_map")
        if collection_map is not None:
            mapping = resolve_path(base, collection_map, "collection_map", index)
            cmd.extend(["--collection-map", str(mapping)])
        episode_map = task.get("episode_map")
        if episode_map is not None:
            mapping = resolve_path(base, episode_map, "episode_map", index)
            cmd.extend(["--episode-map", str(mapping)])
        episode_group = task.get("episode_group")
        if episode_group is not None:
            cmd.extend(["--episode-group", episode_group])
        if media_type == "collection" and collection_map is None and not task.get(
            "allow_index_mapping", False
        ):
            raise SystemExit(
                f"第 {index} 个合集任务必须提供 collection_map，"
                "或显式设置 allow_index_mapping=true"
            )
        cmd.extend(["--", src])
        run(cmd, index)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
