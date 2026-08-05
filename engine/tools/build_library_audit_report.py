#!/usr/bin/env python3
"""Convert a one-time live inventory into a conservative read-only audit report."""

from __future__ import annotations

import argparse
from datetime import date
import json
from pathlib import Path
from typing import Any


def _episode(value: dict[str, Any]) -> dict[str, Any]:
    return {
        "season": int(value.get("season") or 0),
        "episode": int(value.get("episode") or 0),
        "label": str(value.get("label") or "待核验"),
        "title": str(value.get("title") or "未命名问题"),
    }


def _uncovered_work_root(path: str) -> str:
    parts = Path(path).parts
    for category in ("番剧", "美剧", "电影"):
        if category not in parts:
            continue
        index = parts.index(category)
        if index + 1 < len(parts):
            return "/" + "/".join(parts[1:index + 2])
    return str(Path(path).parent)


def build(
    live: dict[str, Any],
    source_gaps: dict[str, Any],
    boundaries: dict[str, Any],
    *,
    audited_at: str | None = None,
) -> dict[str, Any]:
    remediations = [
        row for row in boundaries.get("semantic_remediations", [])
        if isinstance(row, dict)
    ]
    remediation_by_target = {
        str(row.get("target_root") or ""): row for row in remediations
    }
    suppressed_roots = {
        str(root)
        for row in remediations
        for root in row.get("suppress_series_roots", [])
        if isinstance(root, str)
    }
    optional_equivalences = {
        str(row.get("target_root") or ""): {
            (int(item.get("season") or 0), int(item.get("episode") or 0))
            for item in row.get("episodes", [])
            if isinstance(item, dict)
        }
        for row in boundaries.get("optional_equivalences", [])
        if isinstance(row, dict)
    }
    shows: list[dict[str, Any]] = []
    completed: list[str] = []
    missing_subtitles = [
        dict(row) for row in live.get("missing_subtitles", [])
        if isinstance(row, dict)
    ]

    for project in live.get("projects", []):
        if not isinstance(project, dict):
            continue
        target = str(project.get("target_root") or "")
        title = str(project.get("title") or target.rsplit("/", 1)[-1])
        tmdb_ids = project.get("tmdb_ids") if isinstance(project.get("tmdb_ids"), list) else []
        common: dict[str, Any] = {"title": title, "target_root": target}
        if len(tmdb_ids) == 1 and isinstance(tmdb_ids[0], int):
            common["tmdb_id"] = tmdb_ids[0]
        regular = [_episode(row) for row in project.get("regular_missing", []) if isinstance(row, dict)]
        optional = [
            episode
            for row in project.get("optional_missing", [])
            if isinstance(row, dict)
            and (episode := _episode(row))
            and (episode["season"], episode["episode"]) not in optional_equivalences.get(target, set())
        ]
        remediation = remediation_by_target.get(target)
        if target not in suppressed_roots and not (remediation and remediation.get("suppress_core_missing")):
            if regular:
                shows.append({
                    **common,
                    "regular_status": f"当前 TMDB 已播正片仍缺 {len(regular)} 集。",
                    "category": "core",
                    "missing": regular,
                })
            else:
                blocking = [
                    issue for issue in project.get("issues", [])
                    if isinstance(issue, dict) and issue.get("severity") in {"critical", "high"}
                    and issue.get("code") != "published_regular_episode_missing"
                ]
                if not blocking:
                    completed.append(f"《{title}》：当前已播正片与文件结构通过实时核对")
        if target not in suppressed_roots and optional:
            shows.append({
                **common,
                "regular_status": "正片状态单独统计；以下为 Season 00 可选内容，不计入正片缺失。",
                "category": "optional",
                "missing": optional,
            })

    for row in remediations:
        target = str(row.get("target_root") or "")
        project = next(
            (item for item in live.get("projects", []) if item.get("target_root") == target),
            None,
        )
        missing = []
        if row.get("suppress_core_missing") and isinstance(project, dict):
            missing = [_episode(item) for item in project.get("regular_missing", []) if isinstance(item, dict)]
        if not missing:
            missing = [{
                "season": 0,
                "episode": 0,
                "label": str(row.get("label") or "结构"),
                "title": str(row.get("detail") or "需要修复媒体库结构"),
            }]
        shows.append({
            "title": str(row.get("title") or target.rsplit("/", 1)[-1]),
            "target_root": target,
            "regular_status": str(row.get("detail") or "存在已确认的识别或结构问题。"),
            "category": "metadata",
            "missing": missing,
        })

    for row in live.get("collection_roots", []):
        if not isinstance(row, dict) or row.get("poster_present") is not False:
            continue
        target = str(row.get("target_root") or "")
        shows.append({
            "title": target.rsplit("/", 1)[-1],
            "target_root": target,
            "regular_status": "合集父目录没有海报，Infuse 文件视图会显示空白文件夹卡片。",
            "category": "metadata",
            "missing": [{"season": 0, "episode": 0, "label": "父目录海报", "title": "缺少 poster.jpg 或 folder.jpg"}],
        })

    uncovered_groups: dict[str, list[str]] = {}
    for raw_path in live.get("uncovered_media", []):
        if not isinstance(raw_path, str) or not raw_path:
            continue
        uncovered_groups.setdefault(_uncovered_work_root(raw_path), []).append(raw_path)
    for target, paths in sorted(uncovered_groups.items()):
        examples = "；".join(Path(path).name for path in paths[:3])
        shows.append({
            "title": Path(target).name,
            "target_root": target,
            "regular_status": f"该作品有 {len(paths)} 个媒体/字幕文件未被 tvshow.nfo 或电影 NFO 覆盖，无法可靠核对季集。",
            "category": "metadata",
            "missing": [{
                "season": 0,
                "episode": 0,
                "label": "未纳入元数据",
                "title": f"共 {len(paths)} 个文件；示例：{examples}",
            }],
        })

    for target in sorted(set(live.get("empty_library_roots", []))):
        if not isinstance(target, str) or not target:
            continue
        shows.append({
            "title": Path(target).name,
            "target_root": target,
            "regular_status": "正式媒体库存在作品目录，但目录树中没有任何文件；可能是中断后留下的空壳。",
            "category": "metadata",
            "missing": [{
                "season": 0,
                "episode": 0,
                "label": "空壳目录",
                "title": "没有视频、字幕、NFO 或海报，需要重新整理或清理残留目录。",
            }],
        })

    for movie in live.get("movies", []):
        if not isinstance(movie, dict):
            continue
        for issue in movie.get("issues", []):
            if not isinstance(issue, dict):
                continue
            # Movie subtitle evidence is already published through
            # ``missing_subtitles`` and refined into the dedicated confirmed /
            # verification subtitle lanes.  Counting it again as metadata
            # creates duplicate remediation work and, more importantly, makes
            # a subtitle-only gap look like a broken NFO or poster.
            if issue.get("code") == "movie_video_subtitle_gap":
                continue
            shows.append({
                "title": str(movie.get("title") or "电影"),
                "target_root": str(movie.get("target_stem") or ""),
                "regular_status": str(issue.get("message") or "电影元数据需要修复。"),
                "category": "metadata",
                "missing": [{
                    "season": 0,
                    "episode": 0,
                    "label": str(issue.get("code") or "电影元数据"),
                    "title": str(issue.get("message") or "需要修复"),
                }],
            })

    for row in source_gaps.get("rows", []):
        if not isinstance(row, dict):
            continue
        gaps = [item for item in row.get("gaps", []) if isinstance(item, dict)]
        if not gaps:
            continue
        shows.append({
            "title": str(row.get("title") or "未命名项目"),
            "target_root": str(row.get("target") or ""),
            "regular_status": "源备份中有字幕但没有对应视频；不会写入 Infuse 目录。",
            "category": "orphan_subtitle",
            "missing": [
                {
                    "season": 0,
                    "episode": index,
                    "label": f"孤立字幕 {index}",
                    "title": str(item.get("label") or "字幕无视频"),
                    **(
                        {"source_path": str(item["source_path"])}
                        if isinstance(item.get("source_path"), str) and item.get("source_path")
                        else {}
                    ),
                }
                for index, item in enumerate(gaps, 1)
            ],
        })

    counts = {
        category: sum(len(row["missing"]) for row in shows if row["category"] == category)
        for category in ("core", "optional", "orphan_subtitle", "metadata")
    }
    return {
        "version": 3,
        "audited_at": audited_at or date.today().isoformat(),
        **(
            {"snapshot_at": live["audited_at"]}
            if isinstance(live.get("audited_at"), str) and live["audited_at"].strip()
            else {}
        ),
        "total_missing": counts["core"],
        "optional_missing": counts["optional"],
        "orphan_subtitles": counts["orphan_subtitle"],
        "missing_subtitles": missing_subtitles,
        "subtitle_policy": dict(live.get("subtitle_policy") or {}),
        "metadata_issues": counts["metadata"],
        "audited_projects": int(live.get("summary", {}).get("series") or 0) + int(live.get("summary", {}).get("movies") or 0),
        "methodology": "当前 AList 实际文件、NFO 中 TMDB 身份、TMDB 已播日期和有证据的系列边界交叉核对；事务完成状态不作为语义通过证据。",
        "shows": shows,
        "boundary_notes": [
            "正片缺失：当前确实没有对应视频资源。",
            "识别与结构：资源通常存在，但季集、NFO、海报或系列边界需要修复。",
            "可选特别篇：单独列出，不宣称正片不完整。",
            "孤立字幕：只作为缺资源证据保留，不写入播放器目录。",
            "字幕缺口：逐视频版本核对外挂字幕和语言；内封字幕需要媒体流复核后才能关闭缺口。",
        ],
        "fixed_issues": [
            f"《{row.get('title') or '未命名项目'}》：{row.get('label') or '结构问题'}已修复并通过实时复核"
            for row in boundaries.get("resolved_remediations", [])
            if isinstance(row, dict)
        ],
        "completed_checks": sorted(set(completed)),
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--live", type=Path, required=True)
    parser.add_argument("--source-gaps", type=Path, required=True)
    parser.add_argument("--boundaries", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    payload = build(
        json.loads(args.live.read_text(encoding="utf-8")),
        json.loads(args.source_gaps.read_text(encoding="utf-8")),
        json.loads(args.boundaries.read_text(encoding="utf-8")),
    )
    args.output.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
