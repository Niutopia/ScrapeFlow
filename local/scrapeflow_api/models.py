"""Persistent local job model and its public API representation."""

from __future__ import annotations

import subprocess
import json
import re
from copy import deepcopy
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, ClassVar

from .config import JOBS_ROOT


def _latest_adapter_reason(logs: list[str]) -> str | None:
    """Extract one bounded adapter error without exposing the whole job log."""
    for line in reversed(logs):
        match = re.search(r"补源适配器失败\s*[:：]\s*(.+)$", line.strip())
        if match:
            return match.group(1).strip()[:500] or None
    return None


def _replenishment_failure_detail(
    plan: dict[str, Any], logs: list[str]
) -> dict[str, str] | None:
    """Turn a failed replenishment trace into a small user-facing diagnosis."""
    replenishment = plan.get("replenishment")
    if not isinstance(replenishment, dict):
        return None
    status = str(replenishment.get("status") or "failed")
    joined = "\n".join(logs)
    adapter_reason = _latest_adapter_reason(logs)
    project_messages = [
        str(project.get("message")).strip()
        for project in replenishment.get("projects", [])
        if isinstance(project, dict) and project.get("message")
    ]
    technical_reason = adapter_reason or next(
        (message for message in reversed(project_messages)
         if message not in {"自动查补获取失败", "自动查补搜索失败"}),
        project_messages[-1] if project_messages else status,
    )

    failed_projects = [
        project for project in replenishment.get("projects", [])
        if isinstance(project, dict) and project.get("status") == "acquire_failed"
    ]
    structured = failed_projects[-1] if failed_projects else {}
    failure_scope = structured.get("failure_scope")
    failure_stage = str(structured.get("failure_stage") or "")
    if failure_scope == "delivery":
        delivery_labels = {
            "delivery_connect": "AList 连接与认证",
            "delivery_prepare": "AList 远程目录准备",
            "delivery_upload": "AList 文件上传",
            "delivery_visibility": "AList 到盘可见性核验",
        }
        return {
            "stage": delivery_labels.get(failure_stage, "AList 交付"),
            "summary": "候选内容已完成本地验证，失败发生在云端交付阶段。",
            "evidence": technical_reason,
            "technical_reason": technical_reason,
            "upload_status": "保留已验证下载和已到盘同尺寸对象",
            "next_action": "原地重试交付并复用现有文件；不隔离该候选。",
        }
    if failure_scope == "infrastructure":
        return {
            "stage": "本地环境与编排",
            "summary": "本轮没有形成候选失效证据，失败来自容量、依赖或任务编排。",
            "evidence": technical_reason,
            "technical_reason": technical_reason,
            "upload_status": "未将候选写入永久隔离账本",
            "next_action": "修复环境约束后重试同一候选。",
        }

    if re.search(r"(?:^|\W)0\s*B/s", joined, re.IGNORECASE) or re.search(
        r"download speed was too slow", joined, re.IGNORECASE
    ):
        return {
            "stage": "磁力候选获取",
            "summary": "候选种子下载速度持续为 0 B/s，零速保护终止了获取任务。",
            "evidence": "aria2c 获取失败 · 末次速度 0 B/s · 未产生到盘文件",
            "technical_reason": technical_reason,
            "upload_status": "未生成 AList 待刮削目录",
            "next_action": "隔离当前零速候选，按原排序规则搜索下一条夸克分享；仍未命中再换磁力候选。",
        }
    if "下载文件定位结果异常" in joined or "matches=0" in joined:
        return {
            "stage": "下载后文件定位",
            "summary": "候选已经下载，但文件定位没有识别发布名中的方括号路径，匹配结果为 0。",
            "evidence": "下载完成 · 文件匹配 matches=0 · 入库步骤未开始",
            "technical_reason": technical_reason,
            "upload_status": "未生成 AList 待刮削目录",
            "next_action": "使用已修复的文件定位逻辑重新获取同一候选，并核对缺失季集后再上传。",
        }

    labels = {
        "search_failed": ("候选搜索", "补源候选搜索执行失败。", "修复搜索源后重新检索候选。"),
        "invalid_candidates": ("候选校验", "候选返回格式或字段校验失败。", "排除无效候选并重新搜索。"),
        "no_match": ("覆盖筛选", "现有候选均未通过作品身份与缺失季集覆盖门槛。", "扩大搜索范围，保持季集覆盖硬门槛并按清晰度降级。"),
        "invalid_acquisition": ("到盘校验", "候选获取结果未通过目录或文件到盘校验。", "清理无效结果并切换候选重新获取。"),
        "acquire_failed": ("候选获取", "已选候选在获取阶段失败。", "隔离失败候选并选择下一条合格候选。"),
    }
    stage, summary, next_action = labels.get(
        status, ("补源执行", "本轮补源没有形成可入库文件。", "根据技术原因修复后重新查补。")
    )
    return {
        "stage": stage,
        "summary": summary,
        "evidence": technical_reason,
        "technical_reason": technical_reason,
        "upload_status": "未生成 AList 待刮削目录",
        "next_action": next_action,
    }


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass
class Job:
    id: str
    source: str
    parent: str
    media_type: str
    absolute: bool
    prefer_simplified: bool
    tmdb_id: int | None = None
    query: str | None = None
    season: int | None = None
    episode_group: str | None = None
    episode_map: dict[str, str] | None = None
    collection_map: dict[str, int] | None = None
    visibility: str = "user"
    created_at: str = field(default_factory=utc_now)
    updated_at: str = field(default_factory=utc_now)
    phase: str = "queued"
    logs: list[str] = field(default_factory=list)
    error: str | None = None
    digest: str | None = None
    approval_source: str | None = None
    plan_summary: dict[str, Any] | None = None
    progress: dict[str, Any] | None = None
    root_job_id: str | None = None
    replenishment_round: int = 0
    process: subprocess.Popen[str] | None = field(default=None, repr=False)
    cancel_requested: bool = False
    # Process-local signal used only while the API is shutting down under a
    # durable global pause.  Unlike ``cancel_requested`` this must never turn
    # an interrupted replenishment adapter into a user cancellation or a
    # terminal job.  The durable phase/checkpoint is repaired before exit.
    maintenance_stop_requested: bool = field(default=False, repr=False)
    force_killed: bool = False
    queue_position: int | None = field(default=None, repr=False)
    queue_kind: str | None = field(default=None, repr=False)

    root_provider: ClassVar[Callable[[], Path]] = staticmethod(lambda: JOBS_ROOT)

    @property
    def directory(self) -> Path:
        return type(self).root_provider() / self.id

    @property
    def log_path(self) -> Path:
        return self.directory / "job.log"

    @property
    def state_path(self) -> Path:
        return self.directory / "job.json"

    def record(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "source": self.source,
            "parent": self.parent,
            "media_type": self.media_type,
            "absolute": self.absolute,
            "prefer_simplified": self.prefer_simplified,
            "tmdb_id": self.tmdb_id,
            "query": self.query,
            "season": self.season,
            "episode_group": self.episode_group,
            "episode_map": self.episode_map,
            "collection_map": self.collection_map,
            "visibility": self.visibility,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "phase": self.phase,
            "error": self.error,
            "digest": self.digest,
            "approval_source": self.approval_source,
            "plan": self.plan_summary,
            "progress": self.progress,
            "root_job_id": self.root_job_id,
            "replenishment_round": self.replenishment_round,
        }

    def public(self, *, include_plan: bool = True) -> dict[str, Any]:
        progress = self.progress
        if self.phase == "extracting_archives":
            # Archive execution happens inside AList tasks, so the subprocess
            # may be quiet for many minutes.  Derive bounded aggregate progress
            # from the persisted plan/journal instead of leaving the Web UI at
            # a fixed phase percentage.
            try:
                plan_wrapper = json.loads((self.directory / "archive-plan.json").read_text(encoding="utf-8"))
                plan = plan_wrapper.get("plan", plan_wrapper)
                archives = plan.get("archives", []) if isinstance(plan, dict) else []
                journal = json.loads((self.directory / "archive-journal.json").read_text(encoding="utf-8"))
                retained = journal.get("retained_archives", []) if isinstance(journal, dict) else []
                blocked = journal.get("blocked_archives", []) if isinstance(journal, dict) else []
                tasks = journal.get("tasks", []) if isinstance(journal, dict) else []
                total = len(archives) if isinstance(archives, list) else 0
                retained_count = len(retained) if isinstance(retained, list) else 0
                blocked_count = (
                    sum(1 for row in blocked if isinstance(row, dict))
                    if isinstance(blocked, list)
                    else 0
                )
                completed = min(retained_count + blocked_count, total)
                active_fraction = 0.0
                if isinstance(tasks, list):
                    running = [row for row in tasks if isinstance(row, dict) and row.get("state") != 2]
                    for row in running:
                        raw = row.get("progress")
                        if isinstance(raw, (int, float)) and not isinstance(raw, bool):
                            active_fraction = max(active_fraction, min(max(float(raw), 0.0), 100.0) / 100.0)
                if total > 0:
                    overall = min(34.0 + 24.0 * (completed + active_fraction) / total, 58.0)
                    current = f" · 当前归档 {active_fraction * 100:.0f}%" if active_fraction else ""
                    progress = {
                        "stage": "archive_extract",
                        "completed": completed,
                        "total": total,
                        "percent": round(overall, 1),
                        "message": f"正在逐个安全解压 {completed}/{total}{current}",
                    }
            except (OSError, ValueError, TypeError, AttributeError):
                pass
        public_plan = self.plan_summary
        if include_plan and isinstance(self.plan_summary, dict):
            detail = _replenishment_failure_detail(self.plan_summary, self.logs)
            if detail:
                public_plan = deepcopy(self.plan_summary)
                replenishment = public_plan.get("replenishment")
                if isinstance(replenishment, dict):
                    replenishment["failure_detail"] = detail
        value = {
            "id": self.id,
            "source": self.source,
            "parent": self.parent,
            "updated_at": self.updated_at,
            "phase": self.phase,
            "error": self.error,
            "digest": self.digest,
            "plan": public_plan,
            "progress": progress,
            "queue_position": self.queue_position,
            "queue_kind": self.queue_kind,
            "visibility": self.visibility,
            "settings": {
                "media_type": self.media_type,
                "tmdb_id": self.tmdb_id,
                "query": self.query,
                "season": self.season,
                "absolute": self.absolute,
            },
        }
        if not include_plan:
            value.pop("plan", None)
            value.pop("digest", None)
        value["recovery_available"] = (self.directory / "media-journal.json").exists()
        return value
