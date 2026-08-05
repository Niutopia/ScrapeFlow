#!/usr/bin/env python3
"""Local-only HTTP bridge between the ScrapeFlow UI and the Python engine."""

from __future__ import annotations

from collections import Counter, defaultdict
import base64
from contextlib import contextmanager
from copy import deepcopy
import hashlib
import json
import os
import posixpath
import re
import secrets
import shutil
import signal
import subprocess
import sys
import threading
import time
import unicodedata
import uuid
from datetime import date, datetime, timedelta, timezone
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path, PurePosixPath
from typing import Any, Callable, Mapping
from urllib.parse import parse_qs, urlsplit
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

# Script mode puts ``/app/local`` (not the repository root) on ``sys.path``.
# The API modules now import shared ``engine.*`` contracts during startup, so
# make the root importable before either package-import branch runs.
_LOCAL_PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(_LOCAL_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_LOCAL_PROJECT_ROOT))

from engine.scrapeflow.errors import ApiError
from engine.scrapeflow.alist_exact_file_adapter import AListExactFileAdapter
from engine.scrapeflow.hybrid_remote_transaction import (
    DEFAULT_ROLLBACK_ROOT,
    REMOTE_ROLLBACK_ROOT_ENV,
    HybridTransferSpec,
    abort_hybrid_batch,
    commit_hybrid_batch,
    load_sealed_batch_specs,
)
from engine.scrapeflow.remote_delete_transaction import (
    RemoteDeleteSpec,
    commit_remote_delete_transaction,
    restore_remote_delete_transaction,
)

try:  # Package import in tests/builds; script import in ``python local/server.py``.
    from local.scrapeflow_api.config import (
        ARCHIVE_TOOL, ENGINE_ROOT, JOBS_ROOT, PROJECT_ROOT, SCRAPER, STATE_ROOT,
        alist_url, analysis_worker_count, auto_execute_media_enabled,
        auto_replenish_missing_enabled,
        command_env, common_connection_args,
        docker_loopback_bridge, execution_worker_count,
        replenishment_adapter_command, replenishment_max_rounds,
        replenishment_min_cloud_attempts,
        replenishment_retry_delay,
    )
    from local.scrapeflow_api.contracts import (
        EXECUTION_PHASES, TERMINAL_PHASES, VALID_PHASES, require_transition,
    )
    from local.scrapeflow_api.models import Job, utc_now
    from local.scrapeflow_api.lifecycle import PersistentGlobalControl
    from local.scrapeflow_api.legacy_one_time_migration import (
        LEGACY_ONE_TIME_APPROVAL_SOURCE,
        retired_legacy_lineage_ids,
    )
    from local.scrapeflow_api.ordinary_completion import (
        build_ordinary_title_completion,
        ordinary_completion_evidence_is_valid,
    )
    from local.scrapeflow_api.subtitle_source_discovery import (
        SubtitleSourceDiscoveryRuntime,
    )
    from local.scrapeflow_api.replenishment import (
        build_replenishment_requests, select_replenishment_candidates,
        suppress_gaps_satisfied_by_planned_videos,
        suppress_request_gaps_present_in_names,
        validate_acquisition_results,
    )
    from local.scrapeflow_api.scheduler import FifoScheduler
    from local.scrapeflow_api.title_closure import (
        TitleClosureAdapters, TitleClosureBlocked, build_title_closure_evidence,
        title_closure_evidence_is_valid,
    )
    from local.scrapeflow_api.title_closure_runtime import (
        make_burned_in_ocr_adapter, make_current_title_episode_gap_scanner,
        tmdb_tv_aliases, tmdb_tv_episode_enrichment,
    )
    from local.scrapeflow_api.validation import (
        MEDIA_LIBRARY_ROOT, UNSCRAPED_MEDIA_ROOT,
        canonical_digest,
        default_parent,
        media_library_path,
        normalize_remote_input,
        paths_overlap,
        redact,
        strict_json_loads,
        target_parent_for_category,
        unscraped_pending_delete,
        unscraped_reserved_reason,
        unscraped_media_path,
    )
except ModuleNotFoundError:
    from scrapeflow_api.config import (
        ARCHIVE_TOOL, ENGINE_ROOT, JOBS_ROOT, PROJECT_ROOT, SCRAPER, STATE_ROOT,
        alist_url, analysis_worker_count, auto_execute_media_enabled,
        auto_replenish_missing_enabled,
        command_env, common_connection_args,
        docker_loopback_bridge, execution_worker_count,
        replenishment_adapter_command, replenishment_max_rounds,
        replenishment_min_cloud_attempts,
        replenishment_retry_delay,
    )
    from scrapeflow_api.contracts import (
        EXECUTION_PHASES, TERMINAL_PHASES, VALID_PHASES, require_transition,
    )
    from scrapeflow_api.models import Job, utc_now
    from scrapeflow_api.lifecycle import PersistentGlobalControl
    from scrapeflow_api.legacy_one_time_migration import (
        LEGACY_ONE_TIME_APPROVAL_SOURCE,
        retired_legacy_lineage_ids,
    )
    from scrapeflow_api.ordinary_completion import (
        build_ordinary_title_completion,
        ordinary_completion_evidence_is_valid,
    )
    from scrapeflow_api.subtitle_source_discovery import (
        SubtitleSourceDiscoveryRuntime,
    )
    from scrapeflow_api.replenishment import (
        build_replenishment_requests, select_replenishment_candidates,
        suppress_gaps_satisfied_by_planned_videos,
        suppress_request_gaps_present_in_names,
        validate_acquisition_results,
    )
    from scrapeflow_api.scheduler import FifoScheduler
    from scrapeflow_api.title_closure import (
        TitleClosureAdapters, TitleClosureBlocked, build_title_closure_evidence,
        title_closure_evidence_is_valid,
    )
    from scrapeflow_api.title_closure_runtime import (
        make_burned_in_ocr_adapter, make_current_title_episode_gap_scanner,
        tmdb_tv_aliases, tmdb_tv_episode_enrichment,
    )
    from scrapeflow_api.validation import (
        MEDIA_LIBRARY_ROOT, UNSCRAPED_MEDIA_ROOT,
        canonical_digest,
        default_parent,
        media_library_path,
        normalize_remote_input,
        paths_overlap,
        redact,
        strict_json_loads,
        target_parent_for_category,
        unscraped_pending_delete,
        unscraped_reserved_reason,
        unscraped_media_path,
    )

NO_ARCHIVES_EXIT_CODE = 3
MAX_BODY_BYTES = 64 * 1024
MAX_LOG_LINES = 600


def audit_local_date(now: datetime | None = None) -> date:
    """Return the dashboard date in the configured user timezone.

    Containers default to UTC, which made an evening scan in Asia/Shanghai
    appear as yesterday even though all files had just been refreshed.
    """
    timezone_name = os.getenv("SCRAPEFLOW_TIMEZONE") or os.getenv("TZ") or "Asia/Shanghai"
    instant = now or datetime.now(timezone.utc)
    if instant.tzinfo is None:
        instant = instant.replace(tzinfo=timezone.utc)
    try:
        return instant.astimezone(ZoneInfo(timezone_name)).date()
    except ZoneInfoNotFoundError:
        return instant.astimezone().date()
MAX_LOG_BYTES = 2 * 1024 * 1024
MAX_SUMMARY_ISSUES_PER_KIND = 50
MAX_SUMMARY_WARNINGS = 20
MAX_SUMMARY_CLEANUP_GROUPS = 20
PROGRESS_PREFIX = "SCRAPEFLOW_PROGRESS "
RECOVERY_DIGEST_PREFIX = "SCRAPEFLOW_RECOVERY_DIGEST "
RECOVERY_ITEM_PREFIX = "SCRAPEFLOW_RECOVERY_ITEM "
TRANSIENT_TMDB_FAILURE_MARKERS = (
    "TMDB HTTPS 证书校验失败",
    "连接 TMDB 超时",
    "TMDB 域名解析失败",
    "TMDB 服务暂时不可用",
    "TMDB 请求过于频繁",
)
ALLOWED_HOST_RE = re.compile(r"^(?:127\.0\.0\.1|localhost)(?::\d+)?$", re.I)
RECOVERY_RETRY_LOCK = threading.Lock()
RECOVERY_RETRY_PENDING: set[str] = set()
REPLENISHMENT_MONITOR_LOCK = threading.Lock()
REPLENISHMENT_MONITOR_PENDING: set[str] = set()
DELAYED_REPLENISHMENT_RETRY_LOCK = threading.Lock()
DELAYED_REPLENISHMENT_RETRY_PENDING: set[str] = set()
DELAYED_REPLENISHMENT_RETRY_TOKENS: dict[str, object] = {}
SUBTITLE_MEMBER_ACQUISITION_ROOT = STATE_ROOT / "subtitle-member-acquisition"
SUBTITLE_SOURCE_MANIFEST_ROOT = SUBTITLE_MEMBER_ACQUISITION_ROOT / "source-manifests"
SUBTITLE_MEMBER_CACHE_ROOT = SUBTITLE_MEMBER_ACQUISITION_ROOT / "verified-cache"
SUBTITLE_MEMBER_STAGING_ROOT = f"{MEDIA_LIBRARY_ROOT}/ScrapeFlow/验证/字幕获取"
SUBTITLE_SOURCE_DISCOVERY_RUNTIME = SubtitleSourceDiscoveryRuntime(
    SUBTITLE_MEMBER_ACQUISITION_ROOT / "discovery-queue",
    SUBTITLE_SOURCE_MANIFEST_ROOT,
    worker_count=max(1, min(
        int(os.getenv("SCRAPEFLOW_SUBTITLE_DISCOVERY_WORKERS", "4")), 16,
    )),
    allow_legacy_tasks=False,
)
TITLE_CLOSURE_OCR = make_burned_in_ocr_adapter(timeout=30)


def load_json(path: Path) -> dict[str, Any]:
    value = strict_json_loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"计划文件不是 JSON 对象: {path}")
    return value


def is_legacy_one_time_owner(job: "Job") -> bool:
    """Classify retired resident owners without reviving their runtime."""
    return job.approval_source == LEGACY_ONE_TIME_APPROVAL_SOURCE


def is_legacy_one_time_lineage(job: "Job") -> bool:
    """Keep both a retired owner and its historical children unscheduled."""
    root = JOBS.get(job.root_job_id or job.id)
    if is_legacy_one_time_owner(job) or (
        root is not None and is_legacy_one_time_owner(root)
    ):
        return True
    try:
        retired_roots, retired_members = retired_legacy_lineage_ids(STATE_ROOT)
    except Exception:
        # A damaged durable deny-list must not accidentally reactivate an
        # internal child whose historical root can no longer be inspected.
        return bool(job.visibility == "internal" and job.root_job_id)
    return bool(
        job.id in retired_members
        or (job.root_job_id or job.id) in retired_roots | retired_members
    )


def require_nonlegacy_job_mutation(job: "Job") -> None:
    if is_legacy_one_time_lineage(job):
        raise ValueError(
            "历史 one-time owner 已永久停用；请使用离线迁移工具生成并批准封存计划"
        )


def archive_journal_succeeded(job: "Job") -> bool:
    """Return whether archive preparation already completed safely for a job."""
    journal_path = job.directory / "archive-journal.json"
    if not journal_path.exists():
        return False
    try:
        return load_json(journal_path).get("status") == "success"
    except (OSError, ValueError, json.JSONDecodeError):
        return False


def media_journal_succeeded(job: "Job") -> bool:
    """Return whether media mutation fully committed before post-check failed."""
    journal_path = job.directory / "media-journal.json"
    if not journal_path.exists():
        return False
    try:
        if load_json(journal_path).get("success") is not True:
            return False
        lifecycle_path = job.directory / "hybrid-transaction-lifecycle.json"
        if lifecycle_path.exists():
            lifecycle = _load_terminal_transaction_lifecycle(job)
            if lifecycle is None or _transaction_lifecycle_cleanup_blockers(job):
                return False
            if lifecycle["outcome"] == "restored":
                return False
        return True
    except (OSError, ValueError, TypeError, json.JSONDecodeError):
        return False


def _media_journal_success_key(job: "Job") -> str | None:
    if not media_journal_succeeded(job):
        return None
    try:
        return canonical_digest(load_json(job.directory / "media-journal.json"))
    except (OSError, ValueError, json.JSONDecodeError):
        return None


def _append_post_commit_resume_log_once(job: "Job", message: str) -> bool:
    """Persist one restart observation per immutable successful journal."""
    journal_key = _media_journal_success_key(job)
    if journal_key is None:
        return False
    with LOCK:
        summary = dict(job.plan_summary or {})
        lifecycle = dict(summary.get("lifecycle") or {})
        if lifecycle.get("post_commit_resume_journal_sha256") == journal_key:
            return False
        lifecycle["post_commit_resume_journal_sha256"] = journal_key
        lifecycle["post_commit_resume_recorded_at"] = utc_now()
        summary["lifecycle"] = lifecycle
        job.plan_summary = summary
        persist_job(job)
    append_log(job, message)
    return True


def is_internal_replenishment_followup(job: "Job") -> bool:
    """Return whether this job only materializes a parent's acquisition.

    The exact episode map is an additional routing proof, not the ownership
    marker.  A cloud provider may reject the original container and force a
    lossless remux before upload; the uploaded path/size then differs from the
    selection receipt and an exact map cannot always be inherited.  The job is
    still a system-created internal child and must stop after its successful
    media journal so that only the parent performs the next gap audit.
    """
    return bool(
        job.visibility == "internal"
        and job.root_job_id
        and is_replenishment_system_source(job.source)
    )


def is_replenishment_system_source(source: str) -> bool:
    """Recognize legacy and canonical materialized replenishment inputs."""
    normalized = str(source or "").rstrip("/")
    basename = posixpath.basename(normalized)
    return bool(
        normalized.startswith(f"{MEDIA_LIBRARY_ROOT}/ScrapeFlow/补源/")
        or re.match(r"^_?ScrapeFlow补源-", basename)
    )


def complete_internal_replenishment_followup(job: "Job", *, restored: bool = False) -> bool:
    """Close a committed internal child without recursively searching extras."""
    if not is_internal_replenishment_followup(job) or not media_journal_succeeded(job):
        return False
    if job.phase != "completed":
        if not restored:
            require_transition(job.phase, "completed")
        job.phase = "completed"
        job.error = None
        job.progress = {
            "stage": "replenishment_followup_complete",
            "completed": 1,
            "total": 1,
            "percent": 100.0,
            "message": "补源内部整理已完成，正在由主任务再次审计",
        }
        job.updated_at = utc_now()
        persist_job(job)
        append_log(
            job,
            "补源内部整理 journal 已成功提交；不在内部任务递归搜索可选内容，"
            "交由主任务重新审计原缺口。",
        )
        remember_completed_job(job)
    return True


def reconcile_completed_replenishment_summary(job: "Job") -> bool:
    """Remove stale failure diagnostics once the same follow-up is verified."""
    if job.phase != "completed" or not isinstance(job.plan_summary, dict):
        return False
    replenishment = job.plan_summary.get("replenishment")
    if (
        not isinstance(replenishment, dict)
        or replenishment.get("status") != "acquired"
        or replenishment.get("followup_verified") is not True
        or "failed_followup_job_ids" not in replenishment
    ):
        return False
    summary = dict(job.plan_summary)
    clean = dict(replenishment)
    clean.pop("failed_followup_job_ids", None)
    summary["replenishment"] = clean
    job.plan_summary = summary
    job.updated_at = utc_now()
    persist_job(job)
    return True


def finalize_replenishment_acquisition_artifacts(job: "Job") -> bool:
    """Close ready acquisition receipts after verified follow-up consumption.

    The materialized source directory is intentionally emptied/removed by the
    organizer. Leaving a ``ready`` receipt pointing at that vanished staging
    path makes later audits look corrupt and can tempt a retry to reuse a
    non-existent source. Preserve the paths as history and mark the immutable
    delivery lifecycle complete instead.
    """
    if job.phase != "completed" or not isinstance(job.plan_summary, dict):
        return False
    replenishment = job.plan_summary.get("replenishment")
    if (
        not isinstance(replenishment, dict)
        or replenishment.get("status") != "acquired"
        or replenishment.get("followup_verified") is not True
    ):
        return False
    changed = False
    followup_ids = list(replenishment.get("followup_job_ids") or [])
    for path in sorted(job.directory.glob("replenishment-acquisition*.json")):
        try:
            payload = load_json(path)
        except (OSError, ValueError, json.JSONDecodeError):
            continue
        if payload.get("status") != "ready":
            continue
        sources = payload.get("source_paths")
        if sources is None and isinstance(payload.get("source_path"), str):
            sources = [payload["source_path"]]
        if not isinstance(sources, list) or not all(
            isinstance(source, str) and source for source in sources
        ):
            continue
        finalized = dict(payload)
        finalized.update({
            "status": "consumed",
            "source_paths": [],
            "consumed_source_paths": list(dict.fromkeys(sources)),
            "followup_verified": True,
            "followup_job_ids": followup_ids,
            "consumed_at": utc_now(),
        })
        finalized.pop("source_path", None)
        _atomic_json(path, finalized)
        changed = True
    return changed


def archive_journal_can_retry(job: "Job") -> bool:
    """Allow checkpoint resume only for an intact plan and restored source."""
    journal_path = job.directory / "archive-journal.json"
    plan_path = job.directory / "archive-plan.json"
    if not journal_path.exists() or not plan_path.exists():
        return False
    try:
        journal = load_json(journal_path)
        plan_digest = canonical_digest(load_json(plan_path))
    except (OSError, ValueError, json.JSONDecodeError):
        return False
    return (
        journal.get("status") == "failed"
        and not journal.get("temporary_restore_error")
        and isinstance(journal.get("retained_archives", []), list)
        and all(isinstance(path, str) for path in journal.get("retained_archives", []))
        and isinstance(journal.get("plan_sha256"), str)
        and secrets.compare_digest(journal["plan_sha256"], plan_digest)
    )


def media_journal_can_retry_without_recovery(job: "Job") -> bool:
    """Return true only when execution stopped before any media-file mutation.

    Lock acquisition/release records and the terminal abort are forensic state,
    but they do not require moving files back. Empty directories created and
    removed during a failed preflight are also safe to leave or recreate. Any
    rename, move, cleanup, poster or metadata record fails closed into the
    normal recovery workflow.
    """
    journal_path = job.directory / "media-journal.json"
    if not journal_path.exists():
        return False
    try:
        journal = load_json(journal_path)
    except (OSError, ValueError, json.JSONDecodeError):
        return False
    if journal.get("success") is True:
        try:
            lifecycle = _load_terminal_transaction_lifecycle(job)
            return bool(
                lifecycle is not None
                and lifecycle["outcome"] == "restored"
                and not _transaction_lifecycle_cleanup_blockers(job)
            )
        except (OSError, ValueError, TypeError, json.JSONDecodeError):
            return False
    records = journal.get("records")
    if journal.get("success") is not False or not isinstance(records, list) or not records:
        return False
    allowed = {
        "acquire-lock", "release-lock", "mkdir", "rollback-rmdir", "abort",
    }
    return all(
        isinstance(record, dict) and record.get("action") in allowed
        for record in records
    )


def next_attempt_artifact(path: Path) -> Path:
    """Preserve previous forensic output and return a fresh attempt path."""
    if not path.exists():
        return path
    for attempt in range(2, 10_000):
        candidate = path.with_name(f"{path.stem}-{attempt}{path.suffix}")
        if not candidate.exists():
            return candidate
    raise ValueError(f"恢复尝试记录过多，无法创建新的本地 journal: {path.parent}")


def unwrap_media_plan(value: dict[str, Any]) -> tuple[dict[str, Any], str]:
    plan = value.get("plan")
    digest = value.get("plan_sha256")
    if not isinstance(plan, dict):
        raise ValueError("媒体计划缺少 plan 对象")
    if not isinstance(digest, str) or not re.fullmatch(r"[0-9a-f]{64}", digest):
        raise ValueError("媒体计划缺少有效的 plan_sha256")
    actual = canonical_digest(plan)
    if not secrets.compare_digest(actual, digest):
        raise ValueError("媒体计划正文与 SHA-256 不一致")
    return plan, digest


def _resource_gap_for_user(
    item: dict[str, Any],
    problem_by_source: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    """Turn a machine gap into a concrete, user-facing missing resource."""
    result = {
        "kind": item.get("kind"),
        "label": item.get("label"),
        "reason": item.get("reason"),
        "files": [
            str(path) for path in (item.get("files") or [])
            if isinstance(path, str)
        ],
    }
    if result["kind"] != "subtitle_without_video" or not result["files"]:
        return result

    source_path = result["files"][0]
    source_name = posixpath.basename(source_path)
    problem = problem_by_source.get(source_path) or {}
    target_path = problem.get("target_path")
    target_label = ""
    if isinstance(target_path, str) and target_path:
        target_stem = Path(posixpath.basename(target_path)).stem
        target_stem = re.sub(
            r"\.(?:zh-CN|zh-TW|en|ja)(?:\.\d+)*$|\.subtitle\d*$",
            "",
            target_stem,
            flags=re.I,
        )
        match = re.match(
            r"^(.+?) - (S\d{2}E\d{2}(?:-E\d{2})?) - (.+)$",
            target_stem,
            re.I,
        )
        target_label = (
            f"{match.group(1)} {match.group(2).upper()}《{match.group(3)}》"
            if match
            else target_stem
        )
    if not target_label:
        target_label = f"字幕「{source_name}」对应的内容"

    marker_match = re.search(
        r"\b(?:MMR\s*(?:[IVXLCDM]+|\d+)|Making\s*0*\d+)\b",
        source_name,
        re.I,
    )
    marker = marker_match.group(0) if marker_match else ""
    marker_note = (
        f"（源字幕标记：{marker}）"
        if marker and marker.casefold() not in target_label.casefold()
        else ""
    )
    result["label"] = f"{target_label}{marker_note}：缺少对应视频"
    result["reason"] = (
        f"已有字幕文件「{source_name}」，但源目录和现有目标库都没有与"
        f"「{target_label}」对应的视频。系统不会移动或删除该字幕；请补齐对应视频"
        "或修正映射后重新生成计划，并由逐视频闭环证据确认字幕状态。"
    )
    return result


_AUTO_SAFE_CLEANUP_NAMES = {".ds_store", "thumbs.db", "desktop.ini"}
_AUTO_SAFE_CLEANUP_REASONS = frozenset({
    "macOS AppleDouble 隐藏文件",
    "无字幕片头/片尾/光盘菜单视频",
    "发布组广告图片",
    "字体资源包",
    "经特典目录与同集正片交叉确认的片头/片尾视频",
    "特典动画广告/Animated Magia Report Commercial",
})
_AUTO_SAFE_DEDUPE_REASON_PATTERNS = tuple(re.compile(pattern) for pattern in (
    r"^同一 TMDB 集号已有更高清晰度版本 .+，删除较低清晰度重复视频$",
    r"^同一 TMDB 电影 movie/\d+ 已有更高清晰度版本 .+，删除较低清晰度重复视频$",
    r"^同一 TMDB 集号已有同清晰度的内封/软字幕版本 .+，删除同内容重复视频$",
    r"^同一 TMDB 集号已有同清晰度但文件更完整的版本 .+，删除较小的重复视频$",
    r"^同一 TMDB 集号已有同清晰度同字幕形态的简体中文字幕版本 .+，删除繁体中文字幕重复视频$",
    r"^同一 TMDB 集号的更高清晰度版本已有对应字幕 .+，删除低清发布版附带的重复字幕$",
))


def _cleanup_is_housekeeping(item: Any) -> bool:
    """Return whether an engine-proven cleanup follows the user's auto rules.

    The engine revalidates these exact reasons against filenames, TMDB identity,
    resolution and file size immediately before execution.  Free-form or novel
    deletion reasons remain outside this allowlist and retain the review gate.
    """
    if not isinstance(item, dict):
        return False
    source = item.get("source_path")
    if not isinstance(source, str) or not source:
        return False
    name = posixpath.basename(source).casefold()
    if name.startswith("._") or name in _AUTO_SAFE_CLEANUP_NAMES:
        return True
    reason = item.get("reason")
    return isinstance(reason, str) and (
        reason in _AUTO_SAFE_CLEANUP_REASONS
        or any(pattern.fullmatch(reason) for pattern in _AUTO_SAFE_DEDUPE_REASON_PATTERNS)
    )


_COMPLETE_OFFICIAL_SEASON_BOUNDARY_RE = re.compile(
    r"^源根目录中的裸集号完整覆盖已确认的 TMDB "
    r"Season (\d{2,3}) 边界；已按完整边界归入$"
)


def _complete_official_boundary_notice_is_safe(notice: dict[str, Any]) -> bool:
    message = notice.get("message")
    match = (
        _COMPLETE_OFFICIAL_SEASON_BOUNDARY_RE.fullmatch(message)
        if isinstance(message, str) else None
    )
    if match is None:
        return False
    if notice.get("code") != "complete_official_season_boundary":
        evidence = notice.get("evidence")
        return (
            isinstance(evidence, dict)
            and evidence.get("classification") == "engine_generated"
        )
    evidence = notice.get("evidence")
    season = int(match.group(1))
    episode_numbers = (
        evidence.get("source_episode_numbers")
        if isinstance(evidence, dict) else None
    )
    return (
        isinstance(evidence, dict)
        and evidence.get("classification") == "engine_generated"
        and evidence.get("evidence_kind") == "official_episode_boundary"
        and evidence.get("season") == season
        and isinstance(episode_numbers, list)
        and episode_numbers == list(range(1, len(episode_numbers) + 1))
        and evidence.get("official_episode_count") == len(episode_numbers)
    )


def media_plan_review_reasons(plan: dict[str, Any]) -> list[str]:
    """Return stable, user-facing reasons that block unattended execution."""
    reasons: list[str] = []
    problem_files = plan.get("problem_files") or []
    cleanup_files = plan.get("cleanup_files") or []
    resource_gaps = (plan.get("scan_report") or {}).get("resource_gaps") or []
    if not isinstance(problem_files, list):
        reasons.append("计划中的异常文件结构无效")
    elif problem_files:
        reasons.append(
            f"计划包含 {len(problem_files)} 个无法安全处理的问题文件；"
            "必须修正识别并重新生成计划"
        )
    if not isinstance(resource_gaps, list):
        reasons.append("资源缺口结构无效")
    # A well-formed resource gap describes absent input, not an unsafe media
    # move.  It is handled only after the normal scrape has committed.
    if not isinstance(cleanup_files, list):
        reasons.append("计划中的清理文件结构无效")
    else:
        destructive = [item for item in cleanup_files if not _cleanup_is_housekeeping(item)]
        if destructive:
            reasons.append(f"执行后将永久删除 {len(destructive)} 个用户文件")

    notices = plan.get("notices")
    if notices is not None:
        if not isinstance(notices, list):
            reasons.append("计划审核通知结构无效")
        elif any(
            not isinstance(notice, dict)
            or (
                notice.get("requires_review") is not False
                and not (
                    notice.get("code") == "destructive_cleanup_requires_review"
                    and bool(cleanup_files)
                    and all(_cleanup_is_housekeeping(item) for item in cleanup_files)
                )
                and not _complete_official_boundary_notice_is_safe(notice)
                and not (
                    isinstance(notice.get("message"), str)
                    and any(
                        pattern.search(str(notice["message"]))
                        for pattern in _AUTO_SAFE_WARNING_PATTERNS
                    )
                )
            )
            for notice in notices
        ):
            reasons.append("识别证据存在冲突或需要人工确认")
    else:
        warnings = plan.get("warnings") or []
        if not isinstance(warnings, list) or any(
            not isinstance(warning, str)
            or not any(
                pattern.search(warning) for pattern in _AUTO_SAFE_WARNING_PATTERNS
            )
            for warning in warnings
        ):
            reasons.append("旧版计划包含无法自动证明安全的警告")

    return list(dict.fromkeys(reasons))


def _cleanup_groups(cleanup_files: list[Any]) -> list[dict[str, Any]]:
    grouped: dict[str, list[str]] = {}
    for item in cleanup_files:
        if not isinstance(item, dict):
            continue
        reason = str(item.get("reason") or "未分类清理项")
        source = item.get("source_path")
        if isinstance(source, str) and source:
            grouped.setdefault(reason, []).append(source)
    return [
        {
            "reason": reason,
            "count": len(paths),
            "examples": paths[:3],
            "truncated": len(paths) > 3,
        }
        for reason, paths in sorted(grouped.items(), key=lambda pair: (-len(pair[1]), pair[0]))
    ]


def summarize_media_plan(plan: dict[str, Any]) -> dict[str, Any]:
    files = plan.get("files") or []
    cleanup_files = plan.get("cleanup_files") or []
    problem_files = plan.get("problem_files") or []
    notices = plan.get("notices") or []
    warnings = [item for item in (plan.get("warnings") or []) if isinstance(item, str)]
    metadata = plan.get("metadata") or {}
    scan_report = dict(plan.get("scan_report") or {})
    resource_gaps = [
        item for item in (scan_report.get("resource_gaps") or [])
        if isinstance(item, dict)
    ]
    problem_sources = {
        item.get("source_path")
        for item in problem_files
        if isinstance(item, dict) and item.get("source_path")
    }
    problem_by_source = {
        str(item.get("source_path")): item
        for item in problem_files
        if isinstance(item, dict) and item.get("source_path")
    }
    normal_file_count = sum(
        1 for item in files if item.get("source_path") not in problem_sources
    )
    review_reasons = media_plan_review_reasons(plan)
    auto_match = (plan.get("decision_trace") or {}).get("auto_match") or {}
    destructive_cleanup_count = sum(
        1 for item in cleanup_files if not _cleanup_is_housekeeping(item)
    )
    cleanup_groups = _cleanup_groups(cleanup_files)
    return {
        "kind": "media",
        "source_root": plan.get("source_root"),
        "target_root": metadata.get("series_root") or plan.get("target_root"),
        "title": metadata.get("title"),
        "year": metadata.get("year"),
        "tmdb_id": metadata.get("tmdb_id"),
        "file_count": len(files),
        "normal_file_count": normal_file_count,
        "warnings": warnings[:MAX_SUMMARY_WARNINGS],
        "warning_count": len(warnings),
        "notices": [dict(item) for item in notices if isinstance(item, dict)],
        "decision_trace": dict(plan.get("decision_trace") or {}),
        "review": {
            "automation_eligible": not review_reasons,
            "risk_level": "high" if destructive_cleanup_count else ("medium" if review_reasons else "low"),
            "reasons": review_reasons,
            "match": dict(auto_match) if isinstance(auto_match, dict) else {},
            "destructive_cleanup_count": destructive_cleanup_count,
        },
        "scan_report": scan_report,
        "resource_gaps": [
            _resource_gap_for_user(item, problem_by_source)
            for item in resource_gaps[:MAX_SUMMARY_ISSUES_PER_KIND]
        ],
        "resource_gap_count": len(resource_gaps),
        "problem_files": [
            {
                "source": item.get("source_path"),
                "target": item.get("target_path"),
                "reason": item.get("reason"),
            }
            for item in problem_files[:MAX_SUMMARY_ISSUES_PER_KIND]
        ],
        "problem_file_count": len(problem_files),
        "cleanup_files": [
            {
                "source": item.get("source_path"),
                "reason": item.get("reason"),
            }
            for item in cleanup_files[:MAX_SUMMARY_ISSUES_PER_KIND]
        ],
        "cleanup_file_count": len(cleanup_files),
        "cleanup_groups": cleanup_groups[:MAX_SUMMARY_CLEANUP_GROUPS],
        "cleanup_group_count": len(cleanup_groups),
        "truncated": (
            len(problem_files) > MAX_SUMMARY_ISSUES_PER_KIND
            or len(cleanup_files) > MAX_SUMMARY_ISSUES_PER_KIND
            or len(resource_gaps) > MAX_SUMMARY_ISSUES_PER_KIND
            or len(warnings) > MAX_SUMMARY_WARNINGS
            or len(cleanup_groups) > MAX_SUMMARY_CLEANUP_GROUPS
        ),
    }


_AUTO_SAFE_WARNING_PATTERNS = tuple(
    re.compile(pattern)
    for pattern in (
        r"^确认执行后将删除明确无用的",
        r"^TMDB 标题与现有作品目录仅大小写或 Unicode 拼写不同",
        r"^\d+ 个文件只有单一弱快照字段",
        r"^根据 TMDB 官方特别篇标题将 .+ 自动映射为 SP\d+$",
        r"^根据文件中的官方特别篇标签将 SP\d+ 映射为 SP\d+$",
        r"^根据文件中的子系列标题将 SP\d+ 映射为 SP\d+$",
        r"^根据第 \d+ 季官方时间线将 (?:OVBSP|OVA|OAV|OAD) 自动映射为 SP\d+$",
        r"^E\d+ 超出第 \d+ 季官方正片集数；根据 TMDB 标题映射为 SP\d+$",
        r"^源文件的 E00 已对应 TMDB 第 0 季第 1 集",
        r"^已检索TMDB 官方开播时间与完整时长并唯一确认源文件 E00 对应 "
        r"(?:E|SP)\d+(?:-(?:E|SP)\d+)?（序章/第 0 话）$",
        r"^(?:E|SP)\d+(?:-(?:E|SP)\d+)? 使用显式覆盖映射$",
        r"^已从目录结构识别并合并 \d+ 个季度$",
        r"^源根目录中的裸集号完整覆盖 TMDB 唯一官方季度；已按完整边界归入 Season 01$",
        r"^源根目录中的裸集号完整覆盖已确认的 TMDB Season \d{2,3} 边界；已按完整边界归入$",
        r"^源发行将长篇剧集分为重置编号的跨季 absolute 块；"
        r"已仅在所有视频块完整覆盖 01–N，且与 TMDB 全部季集数"
        r"边界唯一分割时自动映射：.+$",
        r"^已识别主系列及 \d+ 个独立衍生剧集",
        r"^子目录《.+》已通过 TMDB 标题/别名和完整 \d+ 集边界"
        r"唯一确认是独立剧集《.+》（(?:19|20)\d{2}），未归入母作品 Season 00$",
        r"^已按 Infuse 规则整理 \d+ 个预告/花絮文件$",
        r"^\d+ 个明确位于特典目录的幕后/访谈/花絮视频"
        r"已按 Infuse Extras 命名保留，不作为正片集号$",
        r"^已按文件名中的 TMDB 编号识别 \d+ 部合集电影$",
        r"^确认执行后将删除 \d+ 个明确无用的",
        r"^(?:E|SP)\d+(?:-(?:E|SP)\d+)? 存在同一 TMDB 集号的多个清晰度版本；"
        r"已优先保留 4K，并计划清理 \d+ 个重复版本$",
        r"^已识别 \d+ 个独立作品目录",
        r"^编号 01–\d{2,4} 的完整视频序列与 TMDB \d+ 部电影的"
        r"官方标题/别名逐一一致；已仅按官方上映日期顺序建立电影归属$",
        r"^\d+ 个独立字幕目录文件已通过唯一同发行 basename "
        r"跟随已确认视频$",
        r"^(?:E\d+(?:\.\d+)?|SP\d+)(?:-(?:E|SP)\d+)? 的 ASS 文本伴侣 title 样式"
        r"唯一标记为已确认的 TMDB movie/\d+；已按同一电影版本参与清晰度去重$",
        r"^同一播出季度同时包含完整本季编号 .+；已按 TMDB 长期断档边界合并为同集版本$",
        r"^源第 \d+ 季完整覆盖 TMDB 长季在官方播出日期.+；已映射为 E\d+–E\d+，未包含未播集$",
        r"^源根目录完整覆盖 TMDB 长季的第一播出块；已作为该块的同集发行版本$",
        r"^识别到 \d+ 部独立电影；已保留独立 TMDB 身份并放入与电视剧作品目录并列的独立电影目录$",
        r"^识别到 \d+ 部剧场版，已保留独立 TMDB 电影身份并以文件、同名 NFO/海报扁平归入本系列根目录$",
        r"^\d+ 个分篇文件所在目录与唯一官方特别篇标题一致；TMDB 仅建一条时已保留同一 S00 集号并按连续 part 命名$",
        r"^\d+ 部系列电影已保留独立 TMDB 身份和各自独立电影目录$",
        r"^\d+ 部系列电影已保留独立 TMDB 身份，并以影片、同名 NFO/海报扁平归入各自系列根目录$",
        r"^E\d+ 存在同一 TMDB 集号的多个清晰度版本；已优先保留最高可确认清晰度，并计划清理 \d+ 个低清晰度重复版本$",
        r"^E\d+ 存在同清晰度的重复发布版；已保留文件更完整的版本，并计划清理 \d+ 个较小重复视频$",
        r"^检测到第 \d+ 季使用全剧累计编号 \d+–\d+；已依据 TMDB 前序季度的 \d+ 集边界换算为 S\d+E\d+–S\d+E\d+。小数集号未参与换算，仍需分别检索常规季与特别篇后确认$",
        r"^源目录使用跨季度连续集号；已按 TMDB 各季度官方集数边界拆分为 \d+ 个 Season$",
        r"^检测到电影被拆为 \d+ 个连续分段，已按 part\d+(?:-part\d+)+ 命名$",
        r"^\d+ 个特典小动画/OVA 已依官方短片时长、发行断档和源季序映射到全局 Season 00 编号$",
        r"^\d+ 个与已确认特别篇视频同名的外挂字幕已跟随视频的官方季集映射$",
    )
)


def media_plan_requires_review(plan: dict[str, Any]) -> bool:
    """Keep destructive or ambiguous plans out of unattended execution."""
    return bool(media_plan_review_reasons(plan))


def _require_problem_free_media_plan(plan: Mapping[str, Any]) -> None:
    """Reject unresolved planner output even after an explicit approval.

    Problem files have no safe implicit destination.  In particular, subtitle
    ambiguity is accepted only through the later per-video closure contract,
    never by moving an unmatched file into a system folder.
    """
    problems = plan.get("problem_files", [])
    if problems is None:
        problems = []
    if not isinstance(problems, list):
        raise ValueError("媒体计划的问题文件结构无效，已拒绝执行")
    if problems:
        raise ValueError(
            f"媒体计划仍有 {len(problems)} 个问题文件；"
            "请修正识别并重新生成计划，不能用批准绕过"
        )


def unattended_media_plan(plan: dict[str, Any]) -> dict[str, Any]:
    """Return an isolated copy without weakening any planner safety gate.

    Earlier versions rewrote unproven cleanup into auto-routed problem files.
    That created an implicit remote destination and made unattended execution
    less strict than the signed planner output.  Unknown cleanup and every
    problem file now remain explicit blockers until a new plan proves them.
    """
    return json.loads(json.dumps(plan, ensure_ascii=False))


JOBS: dict[str, Job] = {}
LOCK = threading.RLock()
GLOBAL_CONTROL = PersistentGlobalControl(
    STATE_ROOT / "global-control.json",
    default_paused=os.getenv("SCRAPEFLOW_START_PAUSED", "0") == "1",
)
GLOBAL_CONTROL_TRANSITION_LOCK = threading.RLock()
# Serializes an ordinary-job insertion with the exact write-capable boundary
# of subtitle/replenishment work.  The lock is deliberately not held while a
# provider search runs; it is held only while a bounded mutation is allowed to
# cross the freshly revalidated scrape-first gate.
SCRAPE_FIRST_TRANSITION_LOCK = threading.RLock()
SHUTDOWN_EVENT = threading.Event()
SCHEDULER = FifoScheduler(
    analysis_workers=analysis_worker_count(),
    execution_workers=execution_worker_count(),
    pause_reader=lambda: GLOBAL_CONTROL.paused,
)
Job.root_provider = staticmethod(lambda: JOBS_ROOT)


def global_control_status() -> dict[str, Any]:
    return GLOBAL_CONTROL.snapshot()


def _remote_dispatch_closed() -> bool:
    """Close remote-write boundaries for operator pause or process exit.

    Only ``GLOBAL_CONTROL`` is persistent and user-visible.  The shutdown
    event is process-local and is never projected as a pause state.
    """
    return GLOBAL_CONTROL.paused or SHUTDOWN_EVENT.is_set()


def set_global_pause(paused: bool, *, reason: str | None = None) -> dict[str, Any]:
    """Persist and apply the process-wide dispatch gate.

    Existing task phases, cancel flags, subprocesses and queue entries are not
    changed.  Pausing prevents the next queued analysis/mutation from being
    dispatched; it is therefore safe to survive an API process restart.
    """
    if paused:
        # Publish the sole gate before waiting for an already-started bounded
        # mutation.  This prevents a fresh FIFO execution from starting while
        # the pause request waits on the transition lock.
        GLOBAL_CONTROL.set_paused(True, reason=reason)
        SCHEDULER.wake()
        with GLOBAL_CONTROL_TRANSITION_LOCK:
            pass
    else:
        with GLOBAL_CONTROL_TRANSITION_LOCK:
            if GLOBAL_CONTROL.paused:
                GLOBAL_CONTROL.set_paused(False)
            SCHEDULER.wake()
    return global_control_status()


def _wait_for_global_resume(job: Job | None = None) -> bool:
    """Hold auxiliary coordinators while globally paused.

    Main analysis and mutation work is gated inside ``FifoScheduler``.  A few
    lightweight retry/child-monitor threads live outside that pool; they must
    observe the same gate before changing durable task state.
    """
    while GLOBAL_CONTROL.paused:
        if job is not None and job.cancel_requested:
            return False
        if SHUTDOWN_EVENT.is_set():
            return False
        time.sleep(0.25)
    return not SHUTDOWN_EVENT.is_set()


def _ordinary_scrape_source(value: Any) -> str | None:
    """Return one user-owned inbox path, excluding every system/residual lane."""
    try:
        path = normalize_remote_input(value)
    except ValueError:
        return None
    prefix = UNSCRAPED_MEDIA_ROOT.rstrip("/") + "/"
    if not path.startswith(prefix):
        return None
    if unscraped_pending_delete(path) or unscraped_reserved_reason(path):
        return None
    if is_replenishment_system_source(path):
        return None
    return path


_ORDINARY_COMPLETION_RESIDUAL_KINDS = frozenset({
    "novel", "manga", "docx", "ncop", "detached_audio",
    "other_non_feature",
})


def _ordinary_nonnegative_count(value: Any) -> int | None:
    return value if type(value) is int and value >= 0 else None


def _ordinary_scoped_path(value: Any, root: Any) -> bool:
    return bool(
        isinstance(value, str) and value
        and isinstance(root, str) and root
        and (value == root or value.startswith(root.rstrip("/") + "/"))
    )


def _ordinary_completion_subtitle_states(
    work: Mapping[str, Any], target_root: Any,
) -> dict[str, str] | None:
    """Validate the literal per-video subtitle policy and return its states."""
    media = work.get("media")
    subtitles = work.get("subtitles")
    if not isinstance(media, Mapping) or not isinstance(subtitles, Mapping):
        return None
    main_paths = media.get("main_video_paths")
    rows = subtitles.get("videos")
    external_paths = subtitles.get("external_sidecars")
    if (
        not isinstance(main_paths, list)
        or not main_paths
        or not all(_ordinary_scoped_path(path, target_root) for path in main_paths)
        or len(set(main_paths)) != len(main_paths)
        or not isinstance(rows, list)
        or not all(isinstance(row, Mapping) for row in rows)
        or not isinstance(external_paths, list)
        or not all(_ordinary_scoped_path(path, target_root) for path in external_paths)
    ):
        return None
    states: dict[str, str] = {}
    collected_external: list[str] = []
    for row in rows:
        video_path = row.get("video_path")
        internal = row.get("internal_chinese_status")
        probe = row.get("embedded_probe")
        sidecars = row.get("external_sidecars")
        evidence = row.get("external_evidence")
        external_count = _ordinary_nonnegative_count(
            row.get("external_sidecar_count"),
        )
        chinese_status = row.get("chinese_status")
        if (
            not isinstance(video_path, str)
            or video_path in states
            or video_path not in main_paths
            or internal not in {
                "embedded_chinese", "burned_in_chinese", "absent", "undetermined",
            }
            or not isinstance(probe, Mapping)
            or not isinstance(sidecars, list)
            or not all(_ordinary_scoped_path(path, target_root) for path in sidecars)
            or len(set(sidecars)) != len(sidecars)
            or external_count != len(sidecars)
            or len(sidecars) > 1
            or not isinstance(evidence, list)
            or len(evidence) != len(sidecars)
            or chinese_status not in {
                "satisfied_internal", "satisfied_external", "missing", "pending",
            }
        ):
            return None
        evidence_by_path: dict[str, Mapping[str, Any]] = {}
        for item in evidence:
            if not isinstance(item, Mapping):
                return None
            path = item.get("path")
            if not isinstance(path, str) or path in evidence_by_path:
                return None
            method = item.get("method")
            status = item.get("status")
            if status not in {"chinese", "non_chinese", "undetermined"}:
                return None
            if PurePosixPath(path).suffix.casefold() == ".mks":
                container_probe = item.get("probe")
                if (
                    method != "ffprobe_container"
                    or not isinstance(container_probe, Mapping)
                    or (
                        status == "chinese"
                        and container_probe.get("status") != "embedded_chinese"
                    )
                ):
                    return None
            elif method not in {"text_content", "unsupported_binary"}:
                return None
            evidence_by_path[path] = item
        if set(evidence_by_path) != set(sidecars):
            return None
        external_statuses = [evidence_by_path[path].get("status") for path in sidecars]
        if internal in {"embedded_chinese", "burned_in_chinese"}:
            expected = "satisfied_internal"
            if sidecars:
                return None
        elif internal == "absent":
            if len(sidecars) == 1 and external_statuses == ["chinese"]:
                expected = "satisfied_external"
            elif any(status == "undetermined" for status in external_statuses):
                expected = "pending"
            else:
                expected = "missing"
        else:
            expected = "pending"
        if chinese_status != expected:
            return None
        collected_external.extend(sidecars)
        states[video_path] = str(chinese_status)
    gap_count = sum(state in {"missing", "pending"} for state in states.values())
    if (
        set(states) != set(main_paths)
        or subtitles.get("scope_root") != target_root
        or _ordinary_nonnegative_count(subtitles.get("video_count")) != len(main_paths)
        or _ordinary_nonnegative_count(
            subtitles.get("chinese_subtitle_gap_count"),
        ) != gap_count
        or _ordinary_nonnegative_count(
            subtitles.get("external_sidecar_count"),
        ) != len(collected_external)
        or _ordinary_nonnegative_count(
            subtitles.get("duplicate_external_sidecar_count"),
        ) != len(collected_external) - len(set(collected_external))
        or external_paths != sorted(collected_external, key=str.casefold)
        or len(collected_external) != len(set(collected_external))
    ):
        return None
    return states


def _ordinary_completion_work_checks(
    work: Mapping[str, Any], target: Mapping[str, Any],
) -> tuple[dict[str, bool], dict[str, int]]:
    """Validate one movie/TV work without inferring missing producer fields."""
    media_type = target.get("media_type")
    target_root = target.get("target_root")
    identity_valid = bool(
        media_type in {"movie", "tv"}
        and work.get("media_type") == media_type
        and work.get("target_root") == target_root
        and work.get("tmdb_id") == target.get("tmdb_id")
        and work.get("title") == target.get("title")
        and work.get("excluded_roots") == target.get("excluded_roots", [])
    )

    inventory = work.get("inventory")
    inventory_valid = bool(
        isinstance(inventory, Mapping)
        and inventory.get("refresh") is True
        and _ordinary_nonnegative_count(inventory.get("directory_count")) not in {None, 0}
        and _ordinary_nonnegative_count(inventory.get("file_count")) is not None
        and isinstance(inventory.get("inventory_sha256"), str)
        and re.fullmatch(r"[0-9a-f]{64}", inventory.get("inventory_sha256"))
    )

    hierarchy = work.get("hierarchy")
    hierarchy_valid = bool(
        isinstance(hierarchy, Mapping)
        and hierarchy.get("status") == "canonical"
        and hierarchy.get("canonical_root") == target_root
        and _ordinary_nonnegative_count(hierarchy.get("work_tree_count")) == 1
        and _ordinary_nonnegative_count(
            hierarchy.get("unexpected_outer_directory_count"),
        ) == 0
        and _ordinary_nonnegative_count(
            hierarchy.get("split_same_work_root_count"),
        ) == 0
        and _ordinary_nonnegative_count(
            hierarchy.get("noncanonical_path_count"),
        ) == 0
        and (
            media_type == "movie"
            and hierarchy.get("hierarchy_kind") == "movie_directory"
            and _ordinary_nonnegative_count(
                hierarchy.get("movie_directory_count"),
            ) == 1
            and _ordinary_nonnegative_count(
                hierarchy.get("nested_title_directory_count"),
            ) == 0
            or media_type == "tv"
            and hierarchy.get("hierarchy_kind") == "tv_series_season"
            and _ordinary_nonnegative_count(
                hierarchy.get("season_directory_count"),
            ) not in {None, 0}
            and _ordinary_nonnegative_count(
                hierarchy.get("episode_outside_season_count"),
            ) == 0
        )
    )

    media = work.get("media")
    main_video_count = (
        _ordinary_nonnegative_count(media.get("main_video_count"))
        if isinstance(media, Mapping) else None
    )
    duplicate_video_count = (
        _ordinary_nonnegative_count(media.get("duplicate_main_video_count"))
        if isinstance(media, Mapping) else None
    )
    main_video_paths = media.get("main_video_paths") if isinstance(media, Mapping) else None
    media_valid = bool(
        main_video_count is not None
        and main_video_count >= 1
        and isinstance(main_video_paths, list)
        and len(main_video_paths) == main_video_count
        and len(set(main_video_paths)) == len(main_video_paths)
        and all(_ordinary_scoped_path(path, target_root) for path in main_video_paths)
        and duplicate_video_count == 0
        and isinstance(media.get("duplicate_groups"), list)
        and not media.get("duplicate_groups")
    ) if isinstance(media, Mapping) else False

    metadata = work.get("metadata")
    nfo_valid = False
    artwork_valid = False
    if isinstance(metadata, Mapping) and main_video_count is not None:
        missing_nfo = metadata.get("missing_nfo_paths")
        missing_artwork = metadata.get("missing_artwork_paths")
        empty_missing_lists = bool(
            isinstance(missing_nfo, list) and not missing_nfo
            and isinstance(missing_artwork, list) and not missing_artwork
        )
        if media_type == "movie" and metadata.get("contract") == "movie":
            required_nfo = _ordinary_nonnegative_count(
                metadata.get("required_movie_nfo_count"),
            )
            present_nfo = _ordinary_nonnegative_count(
                metadata.get("present_movie_nfo_count"),
            )
            required_artwork = _ordinary_nonnegative_count(
                metadata.get("required_artwork_count"),
            )
            present_artwork = _ordinary_nonnegative_count(
                metadata.get("present_artwork_count"),
            )
            nfo_valid = bool(
                metadata.get("movie_nfo_present") is True
                and required_nfo is not None and required_nfo >= 1
                and present_nfo == required_nfo
                and isinstance(missing_nfo, list) and not missing_nfo
            )
            artwork_valid = bool(
                metadata.get("movie_poster_present") is True
                and required_artwork is not None and required_artwork >= 1
                and present_artwork == required_artwork
                and isinstance(missing_artwork, list) and not missing_artwork
            )
        elif media_type == "tv" and metadata.get("contract") == "tv":
            required_nfo = _ordinary_nonnegative_count(
                metadata.get("required_episode_nfo_count"),
            )
            present_nfo = _ordinary_nonnegative_count(
                metadata.get("present_episode_nfo_count"),
            )
            required_posters = _ordinary_nonnegative_count(
                metadata.get("required_season_poster_count"),
            )
            present_posters = _ordinary_nonnegative_count(
                metadata.get("present_season_poster_count"),
            )
            nfo_valid = bool(
                metadata.get("series_nfo_present") is True
                and required_nfo == main_video_count
                and present_nfo == required_nfo
                and isinstance(missing_nfo, list) and not missing_nfo
            )
            artwork_valid = bool(
                metadata.get("series_poster_present") is True
                and required_posters is not None and required_posters >= 1
                and present_posters == required_posters
                and isinstance(missing_artwork, list) and not missing_artwork
            )
        if not empty_missing_lists:
            nfo_valid = False
            artwork_valid = False

    residuals = work.get("residuals")
    residual_counts: list[int] = []
    residuals_valid = isinstance(residuals, Mapping)
    if isinstance(residuals, Mapping):
        residuals_valid = residuals_valid and set(residuals) == {
            *_ORDINARY_COMPLETION_RESIDUAL_KINDS, "items",
        }
        for key, value in residuals.items():
            if key == "items":
                continue
            count = _ordinary_nonnegative_count(value)
            if count is None:
                residuals_valid = False
            else:
                residual_counts.append(count)
        residuals_valid = bool(
            residuals_valid
            and isinstance(residuals.get("items"), list)
            and not residuals.get("items")
            and all(count == 0 for count in residual_counts)
        )

    subtitles = work.get("subtitles")
    chinese_gap_count = (
        _ordinary_nonnegative_count(subtitles.get("chinese_subtitle_gap_count"))
        if isinstance(subtitles, Mapping) else None
    )
    external_count = (
        _ordinary_nonnegative_count(subtitles.get("external_sidecar_count"))
        if isinstance(subtitles, Mapping) else None
    )
    external_duplicates = (
        _ordinary_nonnegative_count(
            subtitles.get("duplicate_external_sidecar_count"),
        )
        if isinstance(subtitles, Mapping) else None
    )
    external_paths = (
        subtitles.get("external_sidecars")
        if isinstance(subtitles, Mapping) else None
    )
    subtitle_states = _ordinary_completion_subtitle_states(work, target_root)
    subtitle_valid = bool(
        subtitle_states is not None
        and chinese_gap_count is not None
        and external_count is not None
        and external_duplicates == 0
        and isinstance(external_paths, list)
    )

    checks = {
        "identity": identity_valid,
        "hierarchy": hierarchy_valid,
        "media": media_valid and inventory_valid,
        "nfo": nfo_valid,
        "artwork": artwork_valid,
        "residuals": residuals_valid,
        "subtitle_policy": subtitle_valid,
    }
    counts = {
        "main_video_count": main_video_count or 0,
        "duplicate_main_video_count": duplicate_video_count or 0,
        "missing_nfo_count": (
            len(metadata.get("missing_nfo_paths", []))
            if isinstance(metadata, Mapping)
            and isinstance(metadata.get("missing_nfo_paths"), list) else 1
        ),
        "missing_artwork_count": (
            len(metadata.get("missing_artwork_paths", []))
            if isinstance(metadata, Mapping)
            and isinstance(metadata.get("missing_artwork_paths"), list) else 1
        ),
        "non_feature_residual_count": sum(residual_counts),
        "chinese_subtitle_gap_count": chinese_gap_count or 0,
        "external_sidecar_count": external_count or 0,
    }
    return checks, counts


def _ordinary_scrape_extended_acceptance_checks(
    job: Job, *, _plan: Mapping[str, Any], _plan_sha256: str,
    _title_closure: Mapping[str, Any],
) -> dict[str, bool]:
    """Consume the strict Engine-produced ordinary-title completion contract.

    Local code does not infer hierarchy, metadata, artwork, duplicates, or
    residual classification from an old plan.  Missing producer fields fail
    closed until a current audit emits this self-digested, plan-bound artifact.
    """
    del _plan
    checks = {
        "completion_evidence_present": False,
        "completion_evidence_self_digest_valid": False,
        "completion_bound_to_current_plan": False,
        "completion_bound_to_current_closure": False,
        "completion_targets_match_closure": False,
        "completion_source_departure_bound": False,
        "completion_hierarchy_valid": False,
        "completion_media_valid": False,
        "completion_nfo_valid": False,
        "completion_artwork_valid": False,
        "completion_residuals_valid": False,
        "completion_subtitle_policy_valid": False,
        "completion_summary_valid": False,
        "completion_contract_valid": False,
    }
    path = job.directory / "ordinary-title-completion.json"
    checks["completion_evidence_present"] = path.is_file()
    if not checks["completion_evidence_present"]:
        return checks
    try:
        evidence = load_json(path)
        evidence_sha256 = evidence.get("evidence_sha256")
        core = {
            key: value for key, value in evidence.items()
            if key != "evidence_sha256"
        }
        checks["completion_evidence_self_digest_valid"] = bool(
            evidence.get("schema_version") == 2
            and evidence.get("kind") == "ordinary_title_completion"
            and isinstance(evidence.get("audited_at"), str)
            and bool(evidence.get("audited_at"))
            and isinstance(evidence_sha256, str)
            and re.fullmatch(r"[0-9a-f]{64}", evidence_sha256)
            and secrets.compare_digest(
                canonical_digest(core), evidence_sha256,
            )
            and ordinary_completion_evidence_is_valid(evidence)
            and isinstance(evidence.get("policy"), Mapping)
            and evidence["policy"].get("remote_mutations") is False
            and evidence["policy"].get("mks_subtitle")
            == "ffprobe_container_not_text_parser"
        )
        completion_plan_sha256 = evidence.get("source_plan_sha256")
        checks["completion_bound_to_current_plan"] = bool(
            isinstance(completion_plan_sha256, str)
            and secrets.compare_digest(completion_plan_sha256, _plan_sha256)
        )
        closure_sha256 = _title_closure.get("evidence_sha256")
        completion_closure_sha256 = evidence.get("title_closure_sha256")
        checks["completion_bound_to_current_closure"] = bool(
            isinstance(closure_sha256, str)
            and isinstance(completion_closure_sha256, str)
            and secrets.compare_digest(
                closure_sha256, completion_closure_sha256,
            )
        )
        title_targets = _title_closure.get("title_targets")
        title_targets_sha256 = _title_closure.get("title_targets_sha256")
        completion_targets_sha256 = evidence.get("title_targets_sha256")
        works = evidence.get("works")
        checks["completion_targets_match_closure"] = bool(
            isinstance(title_targets, list)
            and isinstance(works, list)
            and len(title_targets) == len(works) > 0
            and isinstance(title_targets_sha256, str)
            and isinstance(completion_targets_sha256, str)
            and secrets.compare_digest(
                title_targets_sha256, completion_targets_sha256,
            )
        )
        departure = evidence.get("source_departure")
        checks["completion_source_departure_bound"] = bool(
            isinstance(departure, Mapping)
            and departure.get("source_path") == job.source
            and departure.get("source_parent")
            == str(PurePosixPath(job.source).parent)
            and departure.get("source_name") == PurePosixPath(job.source).name
            and departure.get("refresh") is True
            and departure.get("absent_from_unscraped_root") is True
            and isinstance(departure.get("parent_inventory_sha256"), str)
            and re.fullmatch(
                r"[0-9a-f]{64}", departure.get("parent_inventory_sha256"),
            )
        )

        aggregate = {
            "work_count": len(works) if isinstance(works, list) else 0,
            "main_video_count": 0,
            "duplicate_main_video_count": 0,
            "missing_nfo_count": 0,
            "missing_artwork_count": 0,
            "non_feature_residual_count": 0,
            "chinese_subtitle_gap_count": 0,
            "external_sidecar_count": 0,
        }
        work_checks: list[dict[str, bool]] = []
        completion_subtitle_states: dict[str, str] = {}
        if checks["completion_targets_match_closure"]:
            target_by_root = {
                target.get("target_root"): target
                for target in title_targets if isinstance(target, Mapping)
            }
            work_by_root = {
                work.get("target_root"): work
                for work in works if isinstance(work, Mapping)
            }
            if (
                len(target_by_root) == len(title_targets)
                and len(work_by_root) == len(works)
                and set(target_by_root) == set(work_by_root)
            ):
                for root in sorted(target_by_root, key=str.casefold):
                    current_checks, counts = _ordinary_completion_work_checks(
                        work_by_root[root], target_by_root[root],
                    )
                    work_checks.append(current_checks)
                    for key, count in counts.items():
                        aggregate[key] += count
                    states = _ordinary_completion_subtitle_states(
                        work_by_root[root], root,
                    )
                    if states is not None:
                        completion_subtitle_states.update(states)
        checks["completion_hierarchy_valid"] = bool(
            work_checks and all(item["hierarchy"] for item in work_checks)
        )
        checks["completion_media_valid"] = bool(
            work_checks and all(item["identity"] and item["media"] for item in work_checks)
        )
        checks["completion_nfo_valid"] = bool(
            work_checks and all(item["nfo"] for item in work_checks)
        )
        checks["completion_artwork_valid"] = bool(
            work_checks and all(item["artwork"] for item in work_checks)
        )
        checks["completion_residuals_valid"] = bool(
            work_checks and all(item["residuals"] for item in work_checks)
        )
        closure_states: dict[str, str] = {}
        closure_states_valid = True
        refinement = _title_closure.get("subtitle_refinement")
        bucket_states = {
            "resolved_with_chinese": "resolved",
            "confirmed_missing_chinese": "missing",
            "pending_review_or_probe": "pending",
        }
        if not isinstance(refinement, Mapping):
            closure_states_valid = False
        else:
            for bucket, expected_state in bucket_states.items():
                rows = refinement.get(bucket)
                if not isinstance(rows, list) or not all(
                    isinstance(row, Mapping) for row in rows
                ):
                    closure_states_valid = False
                    continue
                for row in rows:
                    path = row.get("video_path")
                    if path not in completion_subtitle_states:
                        continue
                    previous = closure_states.setdefault(str(path), expected_state)
                    if previous != expected_state:
                        closure_states_valid = False
        for path, completion_state in completion_subtitle_states.items():
            expected = closure_states.get(path)
            if (
                expected is None
                or expected == "resolved"
                and completion_state not in {"satisfied_internal", "satisfied_external"}
                or expected == "missing" and completion_state != "missing"
                or expected == "pending" and completion_state != "pending"
            ):
                closure_states_valid = False
        checks["completion_subtitle_policy_valid"] = bool(
            work_checks
            and all(item["subtitle_policy"] for item in work_checks)
            and completion_subtitle_states
            and closure_states_valid
            and set(closure_states) == set(completion_subtitle_states)
        )
        summary = evidence.get("summary")
        checks["completion_summary_valid"] = bool(
            isinstance(summary, Mapping)
            and set(summary) == set(aggregate)
            and all(type(summary.get(key)) is int for key in aggregate)
            and all(summary.get(key) == value for key, value in aggregate.items())
            and sum(
                state in {"missing", "pending"}
                for state in closure_states.values()
            ) == aggregate["chinese_subtitle_gap_count"]
        )
    except (OSError, ValueError, TypeError, json.JSONDecodeError):
        return checks
    checks["completion_contract_valid"] = all(
        passed for name, passed in checks.items()
        if name != "completion_contract_valid"
    )
    return checks


def _ordinary_scrape_acceptance_contract(job: Job) -> dict[str, Any]:
    """Verify immutable local evidence before releasing the global gate."""
    checks: dict[str, bool] = {
        "successful_media_journal": media_journal_succeeded(job),
        "current_signed_media_plan": False,
        "plan_problem_files_empty": False,
        "valid_title_closure_evidence": False,
        "title_closure_bound_to_current_plan": False,
        "summary_projection_matches_title_closure": False,
    }
    extension_checks: dict[str, bool] = {}
    try:
        plan, plan_sha256 = unwrap_media_plan(
            load_json(job.directory / "media-plan.json"),
        )
        checks["current_signed_media_plan"] = True
        problems = plan.get("problem_files", [])
        checks["plan_problem_files_empty"] = bool(
            (problems is None or isinstance(problems, list))
            and not problems
        )
        title_closure = load_json(job.directory / "title-closure.json")
        checks["valid_title_closure_evidence"] = (
            title_closure_evidence_is_valid(title_closure)
        )
        source_plan_sha256 = title_closure.get("source_plan_sha256")
        checks["title_closure_bound_to_current_plan"] = bool(
            isinstance(source_plan_sha256, str)
            and secrets.compare_digest(source_plan_sha256, plan_sha256)
        )
        closure_summary = title_closure.get("summary")
        summary = job.plan_summary if isinstance(job.plan_summary, Mapping) else {}
        projection = summary.get("title_closure")
        evidence_sha256 = title_closure.get("evidence_sha256")
        projected_evidence_sha256 = (
            projection.get("evidence_sha256")
            if isinstance(projection, Mapping) else None
        )
        projected_plan_sha256 = (
            projection.get("source_plan_sha256")
            if isinstance(projection, Mapping) else None
        )
        checks["summary_projection_matches_title_closure"] = bool(
            isinstance(projection, Mapping)
            and projection.get("status") == "audited"
            and isinstance(projection.get("summary"), Mapping)
            and projection.get("summary") == closure_summary
            and isinstance(evidence_sha256, str)
            and isinstance(projected_evidence_sha256, str)
            and secrets.compare_digest(
                projected_evidence_sha256, evidence_sha256,
            )
            and isinstance(projected_plan_sha256, str)
            and secrets.compare_digest(projected_plan_sha256, plan_sha256)
        )
        extension_checks = _ordinary_scrape_extended_acceptance_checks(
            job, _plan=plan, _plan_sha256=plan_sha256,
            _title_closure=title_closure,
        )
        if not isinstance(extension_checks, dict) or any(
            not isinstance(name, str) or type(value) is not bool
            for name, value in extension_checks.items()
        ):
            extension_checks = {"invalid_extension_contract": False}
    except (OSError, ValueError, TypeError, json.JSONDecodeError):
        pass
    accepted = all(checks.values()) and all(extension_checks.values())
    return {
        "accepted": accepted,
        "checks": checks,
        "extension_checks": extension_checks,
    }


def _ordinary_scrape_job_accepted(job: Job) -> bool:
    """Release scrape-first after the safe base tree is accepted.

    Episode/subtitle gaps remain valid work at this layer.  Requiring them to
    be zero here would deadlock the global scrape-first policy that deliberately
    waits for every ordinary source to leave the inbox before supplementing.
    """
    return _ordinary_scrape_acceptance_contract(job)["accepted"] is True


def _ordinary_final_completion_contract(job: Job) -> dict[str, Any]:
    """Require the base scrape contract plus a fully closed current title."""
    scrape_acceptance = _ordinary_scrape_acceptance_contract(job)
    final_checks = {
        "title_closure_complete": False,
        "title_closure_zero_episode_gaps": False,
        "title_closure_zero_confirmed_subtitle_gaps": False,
        "title_closure_zero_pending_subtitle_verification": False,
    }
    try:
        title_closure = load_json(job.directory / "title-closure.json")
        summary = title_closure.get("summary")
        if isinstance(summary, Mapping):
            final_checks.update({
                "title_closure_complete": summary.get("complete") is True,
                "title_closure_zero_episode_gaps": (
                    summary.get("episode_gap_count") == 0
                ),
                "title_closure_zero_confirmed_subtitle_gaps": (
                    summary.get("confirmed_subtitle_gap_count") == 0
                ),
                "title_closure_zero_pending_subtitle_verification": (
                    summary.get("pending_subtitle_verification_count") == 0
                ),
            })
    except (OSError, ValueError, TypeError, json.JSONDecodeError):
        pass
    return {
        "accepted": bool(
            scrape_acceptance.get("accepted") is True
            and all(final_checks.values())
        ),
        "scrape_acceptance": scrape_acceptance,
        "final_checks": final_checks,
    }


def scrape_first_gate_evidence(current: Job) -> dict[str, Any]:
    """Prove the ordinary inbox and every other ordinary scrape are accepted.

    This is deliberately a read-only gate.  A malformed pause document, an
    unreadable inbox, or an invalid listing fails closed and becomes durable
    wait evidence; none of those conditions may fall through to source search,
    subtitle acquisition, or a candidate rotation.
    """
    checked_at = utc_now()
    control = global_control_status()
    if control.get("paused") is not False:
        blockers = [{
            "kind": "global_pause",
            "path": UNSCRAPED_MEDIA_ROOT,
            "reason": "persistent_global_pause",
        }]
        return {
            "ready": False, "status": "blocked", "checked_at": checked_at,
            "blocker_count": len(blockers), "blockers": blockers,
            "message": "全局暂停期间无法证明普通刮削已全部验收",
        }

    blockers_by_path: dict[str, dict[str, Any]] = {}
    inbox_snapshot: list[dict[str, Any]] = []
    try:
        rows = _execution_alist_client().list(UNSCRAPED_MEDIA_ROOT, refresh=True)
        if not isinstance(rows, list):
            raise ValueError("待刮削目录列表不是数组")
        for row in rows:
            if not isinstance(row, Mapping):
                raise ValueError("待刮削目录包含无效条目")
            name = row.get("name")
            if (
                not isinstance(name, str) or not name or "/" in name
                or name in {".", ".."}
            ):
                raise ValueError("待刮削目录包含无效名称")
            path = posixpath.join(UNSCRAPED_MEDIA_ROOT, name)
            if _ordinary_scrape_source(path) is None:
                continue
            inbox_snapshot.append({
                "path": path,
                "is_dir": row.get("is_dir") is True,
                "size": row.get("size") if type(row.get("size")) is int else None,
                "modified": (
                    str(row.get("modified")) if row.get("modified") is not None
                    else None
                ),
            })
            blockers_by_path[path] = {
                "kind": "inbox_directory" if row.get("is_dir") else "inbox_file",
                "path": path,
            }
    except Exception as exc:  # fail closed; the caller persists bounded evidence
        blockers = [{
            "kind": "inbox_read_failed", "path": UNSCRAPED_MEDIA_ROOT,
            "reason": f"{type(exc).__name__}: {redact(str(exc))}",
        }]
        return {
            "ready": False, "status": "blocked", "checked_at": checked_at,
            "blocker_count": len(blockers), "blockers": blockers,
            "message": "无法读取待刮削实况，已阻止补源",
        }

    with LOCK:
        local_jobs = list(JOBS.values())
    local_snapshot: list[dict[str, Any]] = []
    for job in local_jobs:
        if job.id == current.id:
            continue
        path = _ordinary_scrape_source(job.source)
        if path is None:
            continue
        acceptance = _ordinary_scrape_acceptance_contract(job)
        acceptance_snapshot = {
            "accepted": acceptance["accepted"] is True,
            "checks": acceptance["checks"],
            "extension_checks": acceptance["extension_checks"],
        }
        local_snapshot.append({
            "job_id": job.id,
            "source": path,
            "phase": job.phase,
            "updated_at": job.updated_at,
            "acceptance_sha256": canonical_digest(acceptance_snapshot),
        })
        if acceptance["accepted"] is True:
            continue
        blockers_by_path[path] = {
            "kind": "local_job", "path": path,
            "job_id": job.id, "phase": job.phase,
            "failed_acceptance_checks": sorted(
                name for name, passed in {
                    **acceptance["checks"], **acceptance["extension_checks"],
                }.items() if passed is not True
            ),
        }

    blockers = sorted(
        blockers_by_path.values(), key=lambda row: str(row["path"]).casefold(),
    )
    snapshot_core = {
        "schema_version": 1,
        "control": {
            "paused": control.get("paused"),
            "updated_at": control.get("updated_at"),
        },
        "inbox": sorted(inbox_snapshot, key=lambda row: str(row["path"]).casefold()),
        "ordinary_jobs": sorted(
            local_snapshot, key=lambda row: (str(row["source"]).casefold(), row["job_id"]),
        ),
    }
    return {
        "ready": not blockers,
        "status": "ready" if not blockers else "blocked",
        "checked_at": checked_at,
        "blocker_count": len(blockers),
        "blockers": blockers,
        "snapshot_sha256": canonical_digest(snapshot_core),
        "message": (
            "普通待刮削已清空且其他普通任务均已验收"
            if not blockers else f"待刮削仍有 {len(blockers)} 项未完成"
        ),
    }


class ScrapeFirstGateClosed(RuntimeError):
    """A fresh gate read no longer matches the previously accepted snapshot."""

    def __init__(self, evidence: Mapping[str, Any]):
        super().__init__("scrape_first_gate_changed")
        self.evidence = dict(evidence)


def _revalidate_scrape_first_snapshot(
    current: Job, expected_sha256: str,
) -> dict[str, Any]:
    """Require a second ready read of the exact same ordinary-work snapshot."""
    if re.fullmatch(r"[0-9a-f]{64}", expected_sha256) is None:
        raise ScrapeFirstGateClosed({
            "ready": False,
            "status": "blocked",
            "checked_at": utc_now(),
            "blocker_count": 1,
            "blockers": [{
                "kind": "gate_witness_missing",
                "path": UNSCRAPED_MEDIA_ROOT,
            }],
            "message": "补源边界缺少可复核的普通刮削快照",
        })
    current_evidence = scrape_first_gate_evidence(current)
    current_sha256 = current_evidence.get("snapshot_sha256")
    if (
        current_evidence.get("ready") is not True
        or not isinstance(current_sha256, str)
        or not secrets.compare_digest(current_sha256, expected_sha256)
    ):
        changed = dict(current_evidence)
        blockers = list(changed.get("blockers") or [])
        if current_evidence.get("ready") is True:
            blockers.append({
                "kind": "gate_snapshot_changed",
                "path": UNSCRAPED_MEDIA_ROOT,
                "expected_sha256": expected_sha256,
                "actual_sha256": current_sha256,
            })
        changed.update({
            "ready": False,
            "status": "blocked",
            "blockers": blockers,
            "blocker_count": len(blockers),
            "message": "普通刮削快照在补源边界前发生变化，已重新等待",
        })
        raise ScrapeFirstGateClosed(changed)
    return current_evidence


def _scrape_first_recheck_delay(summary: Mapping[str, Any]) -> int:
    replenishment = summary.get("replenishment")
    next_check_at = (
        replenishment.get("next_check_at")
        if isinstance(replenishment, Mapping) else None
    )
    if isinstance(next_check_at, str):
        try:
            due = datetime.fromisoformat(next_check_at.replace("Z", "+00:00"))
            if due.tzinfo is not None:
                remaining = (
                    due.astimezone(timezone.utc) - datetime.now(timezone.utc)
                ).total_seconds()
                return max(1, int(remaining) + 1)
        except ValueError:
            pass
    return max(1, replenishment_retry_delay())


def _schedule_scrape_first_recheck(
    job: Job, *, summary: dict[str, Any], restored: bool = False,
) -> bool:
    """Arm one restart-safe gate recheck without consuming a source round."""
    replenishment = summary.get("replenishment")
    if not (
        isinstance(replenishment, Mapping)
        and replenishment.get("status") == "scrape_first_wait"
    ):
        return False
    wait = _scrape_first_recheck_delay(summary)
    scheduled = _launch_delayed_replenishment_retry(job, wait)
    if not restored:
        append_log(job, f"普通刮削尚未全部验收；{wait} 秒后只复查门禁，不轮换补源候选。")
    return scheduled


def _enter_scrape_first_wait(
    job: Job, *, summary: dict[str, Any], gate: Mapping[str, Any],
    restored: bool = False,
) -> bool:
    """Persist the global barrier while preserving the current source round."""
    public = dict(summary)
    previous = public.get("replenishment")
    previous = dict(previous) if isinstance(previous, Mapping) else {}
    now = datetime.now(timezone.utc)
    wait = max(1, replenishment_retry_delay())
    next_check_at = (now + timedelta(seconds=wait)).isoformat()
    existing_next = previous.get("next_check_at")
    if previous.get("status") == "scrape_first_wait" and isinstance(existing_next, str):
        try:
            existing_due = datetime.fromisoformat(existing_next.replace("Z", "+00:00"))
            if existing_due.tzinfo is not None and existing_due > now:
                next_check_at = existing_due.astimezone(timezone.utc).isoformat()
        except ValueError:
            pass
    closure = public.get("title_closure")
    closure_summary = (
        closure.get("summary") if isinstance(closure, Mapping) else None
    )
    gap_count = previous.get("gap_count")
    if isinstance(closure_summary, Mapping):
        gap_count = sum(int(closure_summary.get(key) or 0) for key in (
            "episode_gap_count", "confirmed_subtitle_gap_count",
            "pending_subtitle_verification_count",
        ))
    wait_state = {
        "status": "scrape_first_wait",
        "round": job.replenishment_round,
        "gap_count": gap_count,
        "message": str(gate.get("message") or "普通刮削尚未全部验收"),
        "checked_at": gate.get("checked_at") or utc_now(),
        "next_check_at": next_check_at,
        "blocker_count": int(gate.get("blocker_count") or 0),
        "blockers": list(gate.get("blockers") or [])[:250],
    }
    prior_status = previous.get("status")
    if prior_status and prior_status != "scrape_first_wait":
        wait_state["deferred_status"] = str(prior_status)
    public["replenishment"] = wait_state
    with LOCK:
        if job.cancel_requested or job.phase not in {
            "replenishing", "failed", "completed", "cancelled",
            "recovery_required", "planning_recovery",
            "awaiting_recovery_approval", "starting_recovery_execution",
            "executing_recovery",
        }:
            return False
        job.phase = "replenishing"
        job.error = None
        job.digest = None
        job.process = None
        job.plan_summary = public
        job.progress = {
            "stage": "scrape_first_wait", "completed": 0, "total": 1,
            "percent": 94.0,
            "message": wait_state["message"],
        }
        job.updated_at = utc_now()
        persist_job(job)
    return _schedule_scrape_first_recheck(
        job, summary=dict(job.plan_summary), restored=restored,
    )


def _resume_scrape_first_wait(job: Job, *, restored: bool = False) -> bool:
    """Restore a persisted barrier from any post-commit legacy phase."""
    summary = dict(job.plan_summary or {})
    replenishment = summary.get("replenishment")
    if not (
        isinstance(replenishment, Mapping)
        and replenishment.get("status") == "scrape_first_wait"
        and (job.phase == "replenishing" or media_journal_succeeded(job))
    ):
        return False
    with LOCK:
        job.phase = "replenishing"
        job.error = None
        job.digest = None
        job.process = None
        job.cancel_requested = False
        job.force_killed = False
        job.progress = {
            "stage": "scrape_first_wait", "completed": 0, "total": 1,
            "percent": 94.0,
            "message": str(replenishment.get("message") or "等待普通刮削全部验收"),
        }
        job.updated_at = utc_now()
        persist_job(job)
    return _schedule_scrape_first_recheck(
        job, summary=dict(job.plan_summary), restored=restored,
    )


def _atomic_json(path: Path, value: dict[str, Any]) -> None:
    temp = path.with_suffix(path.suffix + ".tmp")
    payload = json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2) + "\n"
    temp.write_text(payload, encoding="utf-8")
    os.chmod(temp, 0o600)
    temp.replace(path)


def _hybrid_state_root_for_job(job: Job) -> Path:
    configured = os.getenv("SCRAPEFLOW_HYBRID_TRANSACTION_STATE_ROOT", "").strip()
    if configured:
        return Path(configured).expanduser().resolve()
    return (job.directory / ".hybrid-remote-transactions").resolve()


def _expected_hybrid_batch_id(plan_sha256: str) -> str:
    transaction_scope = f"forward-{plan_sha256}"
    return "scraper-" + hashlib.sha256(
        f"{transaction_scope}\0{plan_sha256}".encode(
            "utf-8", errors="surrogatepass",
        ),
    ).hexdigest()[:48]


def _remote_delete_state_root_for_job(job: Job) -> Path:
    configured = os.getenv("SCRAPEFLOW_REMOTE_DELETE_ROOT", "").strip()
    if configured:
        owner = hashlib.sha256(
            str(job.directory).encode("utf-8", errors="surrogatepass"),
        ).hexdigest()[:24]
        return Path(configured).expanduser().resolve() / owner
    return (job.directory / ".remote-delete-transactions").resolve()


def _allowed_remote_rollback_roots() -> set[str]:
    configured = os.getenv(REMOTE_ROLLBACK_ROOT_ENV, DEFAULT_ROLLBACK_ROOT).strip()
    return {
        media_library_path(value, allow_root=False)
        for value in {configured or DEFAULT_ROLLBACK_ROOT, DEFAULT_ROLLBACK_ROOT}
    }


def _hybrid_specs_from_media_journal(
    job: Job,
) -> tuple[Path, list[HybridTransferSpec]] | None:
    """Load only Engine's pre-mutation sealed receipt, bound to this plan."""
    path = job.directory / "media-journal.json"
    if not path.is_file():
        return None
    journal = load_json(path)
    plan = journal.get("plan")
    plan_sha256 = journal.get("plan_sha256")
    records = journal.get("records")
    if not isinstance(plan, Mapping) or not isinstance(records, list):
        # A configured root can contain other jobs, so only a job-local root
        # can prove an unbound artifact here.  Old/no-op journals remain
        # compatible when they have no attributable hybrid state.
        state_root = _hybrid_state_root_for_job(job)
        configured = os.getenv(
            "SCRAPEFLOW_HYBRID_TRANSACTION_STATE_ROOT", "",
        ).strip()
        if not configured and state_root.exists() and any(state_root.iterdir()):
            raise ValueError("媒体 journal 缺少可绑定的 hybrid sealed receipt")
        return None
    if (
        not isinstance(plan_sha256, str)
        or re.fullmatch(r"[0-9a-f]{64}", plan_sha256) is None
        or not secrets.compare_digest(canonical_digest(plan), plan_sha256)
    ):
        raise ValueError("媒体 journal 的计划摘要无效")
    plan_path = job.directory / "media-plan.json"
    if plan_path.is_file():
        _signed_plan, signed_sha256 = unwrap_media_plan(load_json(plan_path))
        if not secrets.compare_digest(signed_sha256, plan_sha256):
            raise ValueError("hybrid sealed receipt 未绑定当前签名计划")

    sealed_rows = [
        row for row in records
        if isinstance(row, Mapping) and row.get("action") == "hybrid-batch-sealed"
    ]
    if not sealed_rows:
        expected_batch = (
            _hybrid_state_root_for_job(job)
            / _expected_hybrid_batch_id(plan_sha256)
        )
        if expected_batch.exists():
            raise ValueError("本机存在未绑定 sealed receipt 的 hybrid 批次")
        return None
    if len(sealed_rows) != 1:
        raise ValueError("媒体 journal 包含重复 hybrid sealed receipt")
    row = sealed_rows[0]
    try:
        receipt = strict_json_loads(str(row.get("message") or ""))
    except (ValueError, json.JSONDecodeError) as exc:
        raise ValueError("hybrid sealed receipt 不是严格 JSON") from exc
    if not isinstance(receipt, Mapping):
        raise ValueError("hybrid sealed receipt 不是对象")
    if set(receipt) != {
        "schema_version", "kind", "state_root", "batch_id",
        "rollback_root", "plan_sha256", "commit_authority", "items",
    }:
        raise ValueError("hybrid sealed receipt 字段集合无效")
    expected_state_root = _hybrid_state_root_for_job(job)
    try:
        raw_state_root = receipt.get("state_root")
        if not isinstance(raw_state_root, str):
            raise ValueError("state_root 不是字符串")
        recorded_path = Path(raw_state_root)
        if not recorded_path.is_absolute():
            raise ValueError("state_root 不是绝对路径")
        recorded_state_root = recorded_path.resolve()
    except (OSError, ValueError) as exc:
        raise ValueError("hybrid sealed receipt 的 state_root 无效") from exc
    if (
        receipt.get("schema_version") != 1
        or receipt.get("kind") != "hybrid_batch_sealed_receipt"
        or receipt.get("commit_authority") != "local_ordinary_acceptance_only"
        or receipt.get("plan_sha256") != plan_sha256
        or recorded_state_root != expected_state_root
        or row.get("status") != "retained"
        or row.get("source") != plan.get("source_root")
    ):
        raise ValueError("hybrid sealed receipt 的身份或授权字段无效")
    rollback_root = receipt.get("rollback_root")
    batch_id = receipt.get("batch_id")
    items = receipt.get("items")
    if (
        not isinstance(rollback_root, str)
        or rollback_root not in _allowed_remote_rollback_roots()
        or not isinstance(batch_id, str)
        or not isinstance(items, list)
        or not items
        or row.get("target") != f"{rollback_root}/{batch_id}"
    ):
        raise ValueError("hybrid sealed receipt 的批次字段无效")

    planned_files = {
        item.get("source_path"): item
        for item in (plan.get("files") or [])
        if isinstance(item, Mapping) and isinstance(item.get("source_path"), str)
    }
    planned_cleanup = {
        item.get("source_path"): item
        for item in (plan.get("cleanup_files") or [])
        if isinstance(item, Mapping) and isinstance(item.get("source_path"), str)
    }
    specs: list[HybridTransferSpec] = []
    verified_by_id: dict[str, tuple[int, str]] = {}
    seen_ids: set[str] = set()
    seen_sources: set[str] = set()
    for item in items:
        if (
            not isinstance(item, Mapping)
            or set(item) != {
                "operation", "spec", "verified_size", "verified_sha256",
                "rollback_path",
            }
            or not isinstance(item.get("spec"), Mapping)
        ):
            raise ValueError("hybrid sealed receipt 包含无效项目")
        try:
            spec = HybridTransferSpec.from_dict(item["spec"])
        except (TypeError, ValueError) as exc:
            raise ValueError("hybrid sealed receipt 的 spec 无效") from exc
        verified_size = item.get("verified_size")
        verified_sha256 = item.get("verified_sha256")
        if (
            spec.batch_id != batch_id
            or spec.rollback_root != rollback_root
            or item.get("operation") != spec.operation
            or spec.item_id in seen_ids
            or spec.source_path in seen_sources
            or verified_size != spec.expected_size
            or not isinstance(verified_sha256, str)
            or re.fullmatch(r"[0-9a-f]{64}", verified_sha256) is None
            or item.get("rollback_path") != spec.rollback_path
        ):
            raise ValueError("hybrid sealed receipt 的项目绑定无效")
        seen_ids.add(spec.item_id)
        seen_sources.add(spec.source_path)
        verified_by_id[spec.item_id] = (verified_size, verified_sha256)
        if spec.operation == "delete":
            if spec.target_path is not None or spec.source_path not in planned_cleanup:
                raise ValueError("hybrid delete spec 未绑定计划 cleanup 文件")
        else:
            planned = planned_files.get(spec.source_path)
            if planned is None or not isinstance(spec.target_path, str):
                raise ValueError("hybrid transfer spec 未绑定计划文件")
            source_dir = planned.get("source_dir")
            target_dir = planned.get("target_dir")
            final_name = planned.get("final_name")
            if source_dir != target_dir:
                expected_target = posixpath.join(str(target_dir), str(final_name))
                if spec.target_path != expected_target:
                    raise ValueError("hybrid transfer 目标与计划不一致")
            elif (
                posixpath.dirname(spec.target_path) != source_dir
                or re.fullmatch(
                    r"\.scraper-tmp-[0-9a-f]{16}-[0-9a-f]{12}(?:\.[^/]+)?",
                    posixpath.basename(spec.target_path),
                ) is None
            ):
                raise ValueError("hybrid 同目录临时目标无效")
        specs.append(spec)
    specs.sort(key=lambda value: value.item_id)
    expected_item_ids = sorted(seen_ids)
    batch_path = expected_state_root / batch_id / "batch.json"
    batch = load_json(batch_path)
    if (
        batch.get("batch_id") != batch_id
        or batch.get("rollback_root") != rollback_root
        or batch.get("item_ids") != expected_item_ids
        or batch.get("state") not in {
            "sealed", "accepted", "committing", "committed",
            "aborting", "aborted",
        }
    ):
        raise ValueError("本机 hybrid batch journal 与 sealed receipt 不一致")
    sealed_specs = load_sealed_batch_specs(
        state_root=expected_state_root, batch_id=batch_id,
    )
    if sealed_specs != specs:
        raise ValueError("本机 sealed batch specs 与媒体 journal 回执不一致")
    members = batch.get("members")
    if not isinstance(members, list) or len(members) != len(specs):
        raise ValueError("本机 hybrid batch 缺少完整成员回执")
    member_by_id = {
        member.get("item_id"): member
        for member in members if isinstance(member, Mapping)
        and isinstance(member.get("item_id"), str)
    }
    if set(member_by_id) != set(expected_item_ids):
        raise ValueError("本机 hybrid batch 成员身份不一致")
    for spec in specs:
        member = member_by_id[spec.item_id]
        verified_size, verified_sha256 = verified_by_id[spec.item_id]
        if (
            member.get("source_path") != spec.source_path
            or member.get("target_path") != spec.target_path
            or member.get("rollback_path") != spec.rollback_path
            or member.get("operation") != spec.operation
            or member.get("content_type") != spec.content_type
            or member.get("expected_size") != spec.expected_size
            or member.get("expected_sha256") != spec.expected_sha256
            or member.get("size") != verified_size
            or member.get("sha256") != verified_sha256
        ):
            raise ValueError("本机 hybrid batch 成员内容与 sealed receipt 不一致")
    return expected_state_root, specs


def _remote_delete_specs_from_media_journal(
    job: Job,
) -> tuple[Path, list[RemoteDeleteSpec]] | None:
    """Load legacy local cleanup quarantine journals for lifecycle closure."""
    stage_root = _remote_delete_state_root_for_job(job)
    if not stage_root.exists():
        return None
    journal = load_json(job.directory / "media-journal.json")
    plan = journal.get("plan")
    plan_sha256 = journal.get("plan_sha256")
    if (
        not isinstance(plan, Mapping)
        or not isinstance(plan_sha256, str)
        or not secrets.compare_digest(canonical_digest(plan), plan_sha256)
    ):
        raise ValueError("删除隔离缺少绑定的媒体计划")
    cleanup_sources = {
        item.get("source_path")
        for item in (plan.get("cleanup_files") or [])
        if isinstance(item, Mapping) and isinstance(item.get("source_path"), str)
    }
    specs: list[RemoteDeleteSpec] = []
    for transaction_dir in sorted(stage_root.iterdir()):
        if not transaction_dir.is_dir():
            raise ValueError("删除隔离根包含非目录工件")
        raw = load_json(transaction_dir / "journal.json")
        source_path = raw.get("source_path")
        size = raw.get("size")
        sha256 = raw.get("sha256")
        expected_id = "cleanup-" + hashlib.sha256(
            f"{plan_sha256}\0{source_path}".encode(
                "utf-8", errors="surrogatepass",
            ),
        ).hexdigest()[:48]
        if (
            raw.get("schema_version") != 1
            or raw.get("kind") != "recoverable_remote_delete"
            or raw.get("transaction_id") != transaction_dir.name
            or transaction_dir.name != expected_id
            or source_path not in cleanup_sources
            or isinstance(size, bool)
            or not isinstance(size, int)
            or size < 0
            or not isinstance(sha256, str)
            or re.fullmatch(r"[0-9a-f]{64}", sha256) is None
            or raw.get("state") not in {
                "staged", "delete_intent", "quarantined", "committed", "restored",
            }
        ):
            raise ValueError("删除隔离 journal 身份或状态无效")
        specs.append(RemoteDeleteSpec(
            transaction_id=transaction_dir.name,
            source_path=str(source_path),
            expected_size=size,
            expected_sha256=sha256,
            content_type=str(raw.get("content_type") or "application/octet-stream"),
        ))
    return (stage_root, specs) if specs else None


def _transaction_lifecycle_receipt(
    job: Job, *, outcome: str, action: str,
    hybrid_specs: list[HybridTransferSpec],
    remote_delete_specs: list[RemoteDeleteSpec],
) -> dict[str, Any]:
    receipt = {
        "schema_version": 1,
        "job_id": job.id,
        "outcome": outcome,
        "action": action,
        "updated_at": utc_now(),
        "hybrid_batch_ids": sorted({spec.batch_id for spec in hybrid_specs}),
        "hybrid_item_ids": sorted(spec.item_id for spec in hybrid_specs),
        "remote_delete_transaction_ids": sorted(
            spec.transaction_id for spec in remote_delete_specs
        ),
    }
    _atomic_json(job.directory / "hybrid-transaction-lifecycle.json", receipt)
    return receipt


def _load_terminal_transaction_lifecycle(job: Job) -> dict[str, Any] | None:
    path = job.directory / "hybrid-transaction-lifecycle.json"
    if not path.is_file():
        return None
    lifecycle = load_json(path)
    expected_keys = {
        "schema_version", "job_id", "outcome", "action", "updated_at",
        "hybrid_batch_ids", "hybrid_item_ids",
        "remote_delete_transaction_ids",
    }
    outcome = lifecycle.get("outcome")
    action = lifecycle.get("action")
    member_fields = (
        "hybrid_batch_ids", "hybrid_item_ids",
        "remote_delete_transaction_ids",
    )
    if (
        set(lifecycle) != expected_keys
        or lifecycle.get("schema_version") != 1
        or lifecycle.get("job_id") != job.id
        or outcome not in {"accepted", "restored"}
        or not isinstance(action, str)
        or not isinstance(lifecycle.get("updated_at"), str)
        or not lifecycle.get("updated_at")
        or outcome == "accepted"
        and action != "commit_after_strict_title_acceptance"
        or outcome == "restored" and not action.startswith("restore:")
    ):
        raise ValueError("事务生命周期回执身份或动作无效")
    for field in member_fields:
        values = lifecycle.get(field)
        if (
            not isinstance(values, list)
            or not all(
                isinstance(value, str)
                and re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}", value)
                for value in values
            )
            or values != sorted(values)
            or len(values) != len(set(values))
        ):
            raise ValueError(f"事务生命周期回执的 {field} 无效")
    return lifecycle


def _job_transaction_bindings(
    job: Job,
) -> tuple[
    tuple[Path, list[HybridTransferSpec]] | None,
    tuple[Path, list[RemoteDeleteSpec]] | None,
]:
    return (
        _hybrid_specs_from_media_journal(job),
        _remote_delete_specs_from_media_journal(job),
    )


def _commit_job_transaction_quarantines(job: Job) -> dict[str, Any]:
    """Release rollback payloads only after the caller proves title acceptance."""
    lifecycle = _load_terminal_transaction_lifecycle(job)
    if lifecycle is not None:
        blockers = _transaction_lifecycle_cleanup_blockers(job)
        if blockers:
            raise ValueError("既有事务生命周期回执未闭环：" + "；".join(blockers[:4]))
        return {
            "status": (
                "already_committed"
                if lifecycle["outcome"] == "accepted" else "already_restored"
            ),
            "hybrid_item_count": len(lifecycle["hybrid_item_ids"]),
            "remote_delete_count": len(
                lifecycle["remote_delete_transaction_ids"],
            ),
        }
    hybrid, remote_delete = _job_transaction_bindings(job)
    hybrid_specs = list(hybrid[1]) if hybrid is not None else []
    delete_specs = list(remote_delete[1]) if remote_delete is not None else []
    if not hybrid_specs and not delete_specs:
        return {
            "status": "not_applicable", "hybrid_item_count": 0,
            "remote_delete_count": 0,
        }
    # Validate every legacy cleanup state before releasing the first payload.
    if remote_delete is not None:
        stage_root, _ = remote_delete
        for spec in delete_specs:
            state = load_json(
                stage_root / spec.transaction_id / "journal.json",
            ).get("state")
            if state not in {"quarantined", "committed"}:
                raise ValueError(
                    f"删除隔离事务尚未达到可提交状态: {spec.transaction_id}={state}"
                )
    adapter = AListExactFileAdapter(_execution_alist_client())
    if hybrid is not None:
        state_root, specs = hybrid
        commit_hybrid_batch(adapter, state_root=state_root, specs=list(specs))
    if remote_delete is not None:
        stage_root, specs = remote_delete
        for spec in specs:
            commit_remote_delete_transaction(
                adapter, stage_root=stage_root, spec=spec,
            )
    receipt = _transaction_lifecycle_receipt(
        job,
        outcome="accepted",
        action="commit_after_strict_title_acceptance",
        hybrid_specs=hybrid_specs,
        remote_delete_specs=delete_specs,
    )
    return {
        "status": "committed",
        "hybrid_item_count": len(hybrid_specs),
        "remote_delete_count": len(delete_specs),
        "receipt_sha256": canonical_digest(receipt),
    }


def _restore_job_transaction_quarantines(
    job: Job, *, reason: str,
) -> dict[str, Any]:
    """Restore exact original sources on failure/cancellation, then close batch."""
    lifecycle = _load_terminal_transaction_lifecycle(job)
    if lifecycle is not None:
        blockers = _transaction_lifecycle_cleanup_blockers(job)
        if blockers:
            raise ValueError("既有事务生命周期回执未闭环：" + "；".join(blockers[:4]))
        if lifecycle["outcome"] == "restored":
            return {
                "status": "already_restored", "hybrid_item_count": len(
                    lifecycle["hybrid_item_ids"]
                ),
                "remote_delete_count": len(
                    lifecycle["remote_delete_transaction_ids"]
                ),
            }
        raise ValueError("已严格验收并提交的远端回滚事务不能恢复")
    hybrid, remote_delete = _job_transaction_bindings(job)
    hybrid_specs = list(hybrid[1]) if hybrid is not None else []
    delete_specs = list(remote_delete[1]) if remote_delete is not None else []
    if not hybrid_specs and not delete_specs:
        return {
            "status": "not_applicable", "hybrid_item_count": 0,
            "remote_delete_count": 0,
        }
    adapter = AListExactFileAdapter(_execution_alist_client())
    if remote_delete is not None:
        stage_root, specs = remote_delete
        for spec in reversed(specs):
            state = load_json(
                stage_root / spec.transaction_id / "journal.json",
            ).get("state")
            if state in {"quarantined", "delete_intent"}:
                restore_remote_delete_transaction(
                    adapter, stage_root=stage_root, spec=spec,
                )
            elif state not in {"staged", "restored"}:
                raise ValueError(
                    f"删除隔离事务无法恢复: {spec.transaction_id}={state}"
                )
    if hybrid is not None:
        state_root, specs = hybrid
        sealed_specs = load_sealed_batch_specs(
            state_root=state_root, batch_id=specs[0].batch_id,
        )
        if sealed_specs != list(specs):
            raise ValueError("本机 sealed batch specs 与媒体 journal 回执不一致")
        abort_hybrid_batch(
            adapter, state_root=state_root, specs=sealed_specs,
        )
    receipt = _transaction_lifecycle_receipt(
        job,
        outcome="restored",
        action=f"restore:{reason}",
        hybrid_specs=hybrid_specs,
        remote_delete_specs=delete_specs,
    )
    return {
        "status": "restored",
        "hybrid_item_count": len(hybrid_specs),
        "remote_delete_count": len(delete_specs),
        "receipt_sha256": canonical_digest(receipt),
    }


def _transaction_lineage(job: Job) -> list[Job]:
    lineage_id = job.root_job_id or job.id
    with LOCK:
        members = [
            item for item in JOBS.values()
            if item.id == lineage_id or item.root_job_id == lineage_id
        ]
    if all(item.id != job.id for item in members):
        members.append(job)
    return sorted(members, key=lambda item: (item.id != lineage_id, item.id))


def _commit_transaction_lineage(job: Job) -> dict[str, Any]:
    results = [
        {"job_id": member.id, **_commit_job_transaction_quarantines(member)}
        for member in _transaction_lineage(job)
    ]
    return {
        "status": "committed",
        "job_count": len(results),
        "jobs": results,
    }


def _restore_transaction_lineage(job: Job, *, reason: str) -> dict[str, Any]:
    results = [
        {
            "job_id": member.id,
            **_restore_job_transaction_quarantines(member, reason=reason),
        }
        for member in reversed(_transaction_lineage(job))
    ]
    return {
        "status": "restored",
        "job_count": len(results),
        "jobs": results,
    }


def _restore_transaction_failure_scope(
    job: Job, *, reason: str,
) -> dict[str, Any]:
    """A failed internal child owns only its batch; a root owns its lineage."""
    if job.root_job_id:
        result = _restore_job_transaction_quarantines(job, reason=reason)
        return {"status": "restored", "job_count": 1, "jobs": [
            {"job_id": job.id, **result},
        ]}
    return _restore_transaction_lineage(job, reason=reason)


def _persist_executable_media_plan(
    path: Path, wrapper: Mapping[str, Any], plan: dict[str, Any], digest: str,
) -> None:
    """Write the complete engine plan envelope after unattended filtering."""
    schema_version = wrapper.get("schema_version")
    if type(schema_version) is not int:
        schema_version = 4
    created_at = wrapper.get("created_at")
    if not isinstance(created_at, str) or not created_at:
        created_at = utc_now()
    _atomic_json(path, {
        "schema_version": schema_version,
        "created_at": created_at,
        "plan_sha256": digest,
        "plan": plan,
    })


def _resume_failed_unattended_plan(job: Job) -> bool:
    """Repair the pre-mutation envelope regression from the first auto rollout."""
    if (
        job.phase != "failed"
        or job.approval_source != "auto"
        or job.error != "计划字段 created_at 必须是字符串"
        or (job.directory / "media-journal.json").exists()
    ):
        return False
    path = job.directory / "media-plan.json"
    try:
        wrapper = load_json(path)
        plan, _digest = unwrap_media_plan(wrapper)
        plan = unattended_media_plan(plan)
        digest = canonical_digest(plan)
        _persist_executable_media_plan(path, wrapper, plan, digest)
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        append_log(job, f"无人值守计划信封自动修复失败：{redact(str(exc))}")
        return False
    needs_review = media_plan_requires_review(plan)
    with LOCK:
        job.phase = "awaiting_media_approval" if needs_review else "starting_media_execution"
        job.error = None
        job.digest = digest
        job.approval_source = None if needs_review else "auto"
        job.plan_summary = summarize_media_plan(plan)
        job.updated_at = utc_now()
        persist_job(job)
    if needs_review:
        append_log(job, "自动修复计划信封后仍有真实风险，已恢复到审核队列。")
    else:
        append_log(job, "已自动修复部署时的计划信封缺失；未发生媒体写入，继续原无人值守执行。")
        start_execution(execute_approved_media, job, digest)
    return True


def processed_index_path() -> Path:
    return JOBS_ROOT.parent / "processed.json"


def load_processed_paths() -> dict[str, dict[str, str]]:
    path = processed_index_path()
    if not path.exists():
        return {}
    try:
        value = load_json(path)
        rows = value.get("paths")
        if not isinstance(rows, dict):
            return {}
        return {
            key: row
            for key, row in rows.items()
            if isinstance(key, str) and isinstance(row, dict)
        }
    except (OSError, ValueError, json.JSONDecodeError):
        return {}


def remember_completed_job(job: Job) -> None:
    """Keep a minimal processed marker even if the queue record is removed."""
    if job.phase != "completed":
        return
    candidates = [job.source]
    if isinstance(job.plan_summary, dict):
        candidates.append(job.plan_summary.get("target_root"))
    valid_paths: list[str] = []
    for candidate in candidates:
        if not isinstance(candidate, str):
            continue
        try:
            valid_paths.append(media_library_path(candidate, allow_root=False))
        except ValueError:
            continue
    if not valid_paths:
        return
    with LOCK:
        rows = load_processed_paths()
        changed = False
        for path in valid_paths:
            existing = rows.get(path)
            if isinstance(existing, dict) and existing.get("phase") == "completed":
                existing_updated_at = existing.get("updated_at")
                try:
                    existing_instant = datetime.fromisoformat(
                        str(existing_updated_at).replace("Z", "+00:00")
                    )
                    job_instant = datetime.fromisoformat(job.updated_at.replace("Z", "+00:00"))
                except (TypeError, ValueError):
                    existing_instant = None
                    job_instant = None
                if (
                    existing_updated_at == job.updated_at
                    or (
                        existing_instant is not None
                        and job_instant is not None
                        and existing_instant >= job_instant
                    )
                ):
                    continue
            rows[path] = {"phase": "completed", "updated_at": job.updated_at}
            changed = True
        if not changed:
            return
        index = processed_index_path()
        index.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        _atomic_json(index, {"version": 1, "paths": rows})


def forget_completed_job(job: Job) -> None:
    """Forget directory badges when the user explicitly deletes task state."""
    candidates = [job.source]
    if isinstance(job.plan_summary, dict):
        candidates.append(job.plan_summary.get("target_root"))
    valid_paths: list[str] = []
    for candidate in candidates:
        if not isinstance(candidate, str):
            continue
        try:
            valid_paths.append(media_library_path(candidate, allow_root=False))
        except ValueError:
            continue
    if not valid_paths:
        return
    with LOCK:
        rows = load_processed_paths()
        changed = False
        for path in valid_paths:
            changed = rows.pop(path, None) is not None or changed
        if changed:
            index = processed_index_path()
            index.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
            _atomic_json(index, {"version": 1, "paths": rows})


def _remote_transaction_cleanup_blockers(job_directory: Path) -> list[str]:
    """Return actionable reasons why local transaction evidence must survive."""
    stage_root = job_directory / ".remote-file-transactions"
    if not stage_root.exists() and not stage_root.is_symlink():
        return []
    if stage_root.is_symlink() or not stage_root.is_dir():
        return [
            ".remote-file-transactions 不是可验证的真实目录；"
            "请保留任务并人工核对本机事务证据",
        ]

    blockers: list[str] = []
    transaction_entries: dict[str, dict[str, Path]] = defaultdict(dict)
    try:
        descendants = sorted(
            stage_root.rglob("*"), key=lambda path: str(path.relative_to(stage_root)),
        )
    except OSError as exc:
        return [f"无法遍历本机事务目录：{redact(str(exc))}"]

    for path in descendants:
        relative = path.relative_to(stage_root)
        parts = relative.parts
        label = relative.as_posix()
        if path.is_symlink():
            blockers.append(f"{label}: 事务工件是符号链接，拒绝删除")
            continue
        if len(parts) == 1:
            if not path.is_dir():
                blockers.append(f"{label}: 事务根目录包含未知工件")
            continue
        transaction_id = parts[0]
        if len(parts) != 2 or path.is_dir():
            blockers.append(f"{label}: 事务目录包含未知或嵌套工件")
            continue
        name = parts[1]
        if name not in {
            "journal.json", "transaction.lock", "payload.bin", "payload.part",
        }:
            blockers.append(f"{label}: 事务目录包含未知工件")
            continue
        transaction_entries[transaction_id][name] = path

    direct_directories = [
        path for path in stage_root.iterdir()
        if path.is_dir() and not path.is_symlink()
    ]
    for directory in direct_directories:
        transaction_entries.setdefault(directory.name, {})

    for transaction_id, artifacts in sorted(transaction_entries.items()):
        prefix = f"{transaction_id}/"
        journal_path = artifacts.get("journal.json")
        payload_path = artifacts.get("payload.bin")
        partial_path = artifacts.get("payload.part")
        lock_path = artifacts.get("transaction.lock")
        if journal_path is None:
            present = ", ".join(sorted(artifacts)) or "空目录"
            blockers.append(
                f"{transaction_id}: 缺少 journal.json，{present} 是孤立事务工件；"
                "请保留并恢复 journal",
            )
            continue
        if payload_path is not None:
            blockers.append(
                f"{prefix}payload.bin: 仍保留可恢复本机 payload；"
                "请先恢复/核对远端，不得删除任务",
            )
        if partial_path is not None:
            blockers.append(
                f"{prefix}payload.part: 存在未完成 staging；"
                "请保留任务并重新核对事务",
            )
        if lock_path is not None and not lock_path.is_file():
            blockers.append(f"{prefix}transaction.lock: lock 工件类型无效")
        try:
            journal = load_json(journal_path)
        except (OSError, ValueError, json.JSONDecodeError) as exc:
            blockers.append(
                f"{prefix}journal.json: journal 损坏或无法读取；"
                f"请保留取证文件（{redact(str(exc))}）",
            )
            continue
        identity_valid = bool(
            journal.get("schema_version") == 1
            and journal.get("transaction_id") == transaction_id
            and isinstance(journal.get("source_path"), str)
            and str(journal.get("source_path")).startswith("/")
            and isinstance(journal.get("target_path"), str)
            and str(journal.get("target_path")).startswith("/")
            and journal.get("source_path") != journal.get("target_path")
            and isinstance(journal.get("content_type"), str)
            and bool(journal.get("content_type"))
            and _ordinary_nonnegative_count(journal.get("size")) is not None
            and isinstance(journal.get("sha256"), str)
            and re.fullmatch(r"[0-9a-f]{64}", str(journal.get("sha256")))
            and type(journal.get("upload_started")) is bool
            and _ordinary_nonnegative_count(journal.get("upload_calls")) in {0, 1}
            and isinstance(journal.get("history"), list)
        )
        if not identity_valid:
            blockers.append(
                f"{prefix}journal.json: journal 身份或完整性字段损坏；"
                "请保留并人工核对",
            )
            continue
        state = journal.get("state")
        if state != "complete":
            blockers.append(
                f"{prefix}journal.json: 事务 state={state}尚未完成；"
                "请先恢复事务并验证目标后再删除本机任务",
            )
        elif journal.get("source_deleted") is not True:
            blockers.append(
                f"{prefix}journal.json: complete 事务未证明源删除闭环；"
                "请保留并核对远端",
            )
    return blockers


def _transaction_lifecycle_cleanup_blockers(job: Job) -> list[str]:
    """Protect sealed hybrid and cleanup quarantine evidence from deletion."""
    hybrid_root = _hybrid_state_root_for_job(job)
    delete_root = _remote_delete_state_root_for_job(job)

    def has_entries(path: Path) -> bool:
        try:
            return path.exists() and any(path.iterdir())
        except OSError:
            return True

    try:
        lifecycle = _load_terminal_transaction_lifecycle(job)
    except (OSError, ValueError, TypeError, json.JSONDecodeError) as exc:
        return ["事务生命周期回执损坏：" + redact(str(exc))]
    if lifecycle is None:
        blockers = []
        try:
            hybrid_binding = _hybrid_specs_from_media_journal(job)
        except Exception as exc:
            blockers.append("hybrid 批次无法验证：" + redact(str(exc)))
        else:
            if hybrid_binding is not None or (
                not os.getenv(
                    "SCRAPEFLOW_HYBRID_TRANSACTION_STATE_ROOT", "",
                ).strip()
                and has_entries(hybrid_root)
            ):
                blockers.append("hybrid 批次尚无 Local commit/abort 生命周期回执")
        try:
            delete_binding = _remote_delete_specs_from_media_journal(job)
        except Exception as exc:
            blockers.append("remote-delete 隔离无法验证：" + redact(str(exc)))
        else:
            if delete_binding is not None or (
                not os.getenv("SCRAPEFLOW_REMOTE_DELETE_ROOT", "").strip()
                and has_entries(delete_root)
            ):
                blockers.append("remote-delete 隔离尚无 Local commit/restore 生命周期回执")
        return blockers

    outcome = str(lifecycle["outcome"])
    batch_ids = list(lifecycle["hybrid_batch_ids"])
    item_ids_from_receipt = list(lifecycle["hybrid_item_ids"])
    transaction_ids = list(lifecycle["remote_delete_transaction_ids"])
    blockers: list[str] = []
    try:
        hybrid_binding = _hybrid_specs_from_media_journal(job)
    except Exception as exc:
        blockers.append("hybrid 批次无法验证：" + redact(str(exc)))
        hybrid_binding = None
    try:
        delete_binding = _remote_delete_specs_from_media_journal(job)
    except Exception as exc:
        blockers.append("remote-delete 隔离无法验证：" + redact(str(exc)))
        delete_binding = None

    bound_batch_ids = sorted({
        spec.batch_id for spec in hybrid_binding[1]
    }) if hybrid_binding is not None else []
    bound_item_ids = sorted(
        spec.item_id for spec in hybrid_binding[1]
    ) if hybrid_binding is not None else []
    bound_transaction_ids = sorted(
        spec.transaction_id for spec in delete_binding[1]
    ) if delete_binding is not None else []
    if batch_ids != bound_batch_ids or item_ids_from_receipt != bound_item_ids:
        blockers.append("生命周期回执的 hybrid 成员与媒体 journal 不一致")
    if transaction_ids != bound_transaction_ids:
        blockers.append("生命周期回执的 remote-delete 成员与媒体 journal 不一致")

    expected_batch_state = "committed" if outcome == "accepted" else "aborted"
    expected_item_state = "committed" if outcome == "accepted" else "aborted"
    for batch_id in batch_ids:
        batch_path = hybrid_root / batch_id / "batch.json"
        try:
            batch = load_json(batch_path)
        except (OSError, ValueError, json.JSONDecodeError) as exc:
            blockers.append(f"{batch_id}: batch journal 无法读取（{redact(str(exc))}）")
            continue
        item_ids = batch.get("item_ids")
        if (
            batch.get("batch_id") != batch_id
            or batch.get("state") != expected_batch_state
            or not isinstance(item_ids, list)
            or not item_ids
            or sorted(item_ids) != sorted(
                spec.item_id for spec in (
                    hybrid_binding[1] if hybrid_binding is not None else []
                ) if spec.batch_id == batch_id
            )
        ):
            blockers.append(
                f"{batch_id}: batch state={batch.get('state')}，"
                f"期望 {expected_batch_state}"
            )
            continue
        for item_id in item_ids:
            if not isinstance(item_id, str):
                blockers.append(f"{batch_id}: item_ids 无效")
                continue
            try:
                item = load_json(hybrid_root / batch_id / item_id / "journal.json")
            except (OSError, ValueError, json.JSONDecodeError) as exc:
                blockers.append(
                    f"{batch_id}/{item_id}: item journal 无法读取（{redact(str(exc))}）"
                )
                continue
            if item.get("state") != expected_item_state:
                blockers.append(
                    f"{batch_id}/{item_id}: state={item.get('state')}，"
                    f"期望 {expected_item_state}"
                )
    allowed_delete_states = {"committed"} if outcome == "accepted" else {"restored", "staged"}
    for transaction_id in transaction_ids:
        journal_path = delete_root / transaction_id / "journal.json"
        try:
            transaction = load_json(journal_path)
        except (OSError, ValueError, json.JSONDecodeError) as exc:
            blockers.append(
                f"{transaction_id}: remote-delete journal 无法读取（{redact(str(exc))}）"
            )
            continue
        if (
            transaction.get("transaction_id") != transaction_id
            or transaction.get("state") not in allowed_delete_states
        ):
            blockers.append(
                f"{transaction_id}: remote-delete state={transaction.get('state')} 未闭环"
            )
    return blockers


def _disposable_job_directory(job: Job) -> Path:
    """Resolve one job directory only after every local transaction is safe."""
    require_nonlegacy_job_mutation(job)
    directory = job.directory.resolve()
    jobs_root = JOBS_ROOT.resolve()
    if directory.parent != jobs_root or not re.fullmatch(
        r"[0-9a-f]{12}", directory.name,
    ):
        raise ValueError("任务目录越界，拒绝删除")
    blockers = [
        *_remote_transaction_cleanup_blockers(directory),
        *_transaction_lifecycle_cleanup_blockers(job),
    ]
    if blockers:
        raise ValueError(
            f"任务 {job.id} 仍有未闭环或不确定的本机文件事务："
            + "；".join(blockers[:8])
        )
    return directory


def clear_local_task_data() -> int:
    """Clear queue records and the independent processed-path index together.

    This never touches AList media.  Active writes and recovery-required jobs
    block the operation so their journals cannot be orphaned accidentally.
    """
    with LOCK:
        blocked = [
            job
            for job in JOBS.values()
            if job.phase not in {
                "awaiting_media_approval", "completed", "recovered", "failed",
                "cancelled",
            }
            or job.phase == "recovery_required"
            or (
                job.phase == "failed"
                and (job.directory / "media-journal.json").exists()
            )
        ]
        if blocked:
            raise ValueError(
                f"仍有 {len(blocked)} 个运行中或待恢复任务，不能清空本地数据"
            )
        # Preflight every directory before deleting even one.  Otherwise a
        # later uncertain payload could block after earlier forensic state had
        # already been irreversibly removed.
        directories = [
            _disposable_job_directory(job) for job in JOBS.values()
        ]
        removed = len(JOBS)
        for directory in directories:
            if directory.exists():
                shutil.rmtree(directory)
        JOBS.clear()
        processed_index_path().unlink(missing_ok=True)
        return removed


def directory_task_phase(path: str) -> str | None:
    """Return the latest live task phase, falling back to completed history."""
    with LOCK:
        matching = []
        for job in JOBS.values():
            target = job.plan_summary.get("target_root") if isinstance(job.plan_summary, dict) else None
            if job.source == path or target == path:
                matching.append(job)
        if matching:
            return max(matching, key=lambda job: job.updated_at).phase
        if path in load_processed_paths():
            return "completed"
    return None


def persist_job(job: Job) -> None:
    if job.directory.exists():
        _atomic_json(job.state_path, job.record())




def reconcile_legacy_completed_source_wait(job: Job) -> bool:
    """Reactivate old jobs that treated an unfilled title as completed."""
    summary = job.plan_summary if isinstance(job.plan_summary, dict) else {}
    replenishment = summary.get("replenishment")
    if not (
        job.phase == "completed"
        and isinstance(replenishment, Mapping)
        and replenishment.get("status") in {"awaiting_sources", "sources_exhausted"}
        and media_journal_succeeded(job)
    ):
        return False
    if replenishment.get("status") == "sources_exhausted":
        summary["replenishment"] = _awaiting_sources_business_state(replenishment)
    job.phase = "replenishing"
    job.error = None
    job.progress = {
        "stage": "current_title_source_wait", "completed": 0, "total": 1,
        "percent": 94.0,
        "message": "旧版本曾把来源穷尽记为完成；已恢复当前作品补源闭环",
    }
    job.updated_at = utc_now()
    persist_job(job)
    with job.log_path.open("a", encoding="utf-8") as handle:
        handle.write("启动纠正旧状态：作品仍有缺口，不再以来源穷尽作为完成。\n")
    job.logs.append("启动纠正旧状态：作品仍有缺口，不再以来源穷尽作为完成。")
    return True


def reconcile_post_commit_cancel(job: Job) -> bool:
    """Keep legacy/in-flight title-loop cancellation terminal across restart."""
    summary = job.plan_summary if isinstance(job.plan_summary, dict) else {}
    replenishment = summary.get("replenishment")
    status = replenishment.get("status") if isinstance(replenishment, Mapping) else None
    if not (
        job.phase in {"completed", "cancelling"}
        and status in {
            "cancelled_after_media_commit",
            "cancellation_requested_after_media_commit",
        }
    ):
        return False
    normalized = dict(replenishment)
    normalized["status"] = "cancelled_after_media_commit"
    summary["replenishment"] = normalized
    job.phase = "cancelled"
    job.error = None
    job.digest = None
    job.process = None
    job.cancel_requested = True
    job.plan_summary = summary
    job.progress = {
        "stage": "replenishment_cancelled", "completed": 0, "total": 1,
        "percent": 94.0, "message": "当前作品缺项闭环已取消，未记为补齐",
    }
    job.updated_at = utc_now()
    persist_job(job)
    return True


def reconcile_pending_delete_job(job: Job) -> bool:
    """Close a stale planning task whose source is already marked for deletion."""
    if (
        job.phase not in {"queued", "failed", "awaiting_media_approval"}
        or not unscraped_pending_delete(job.source)
    ):
        return False
    require_transition(job.phase, "cancelled")
    job.phase = "cancelled"
    job.error = None
    job.digest = None
    job.updated_at = utc_now()
    job.progress = {
        "stage": "source_excluded", "completed": 1, "total": 1,
        "percent": 100.0, "message": "源目录已标记待删除，已自动排除",
    }
    persist_job(job)
    with job.log_path.open("a", encoding="utf-8") as handle:
        handle.write("启动复审：源目录已标记待删除，不再查询 TMDB。\n")
    job.logs.append("启动复审：源目录已标记待删除，不再查询 TMDB。")
    return True


def _automatic_planning_retry_allowed(job: Job) -> bool:
    """Retry a historical TMDB miss only when the new engine adds evidence."""
    if (
        job.phase != "failed"
        or str((job.progress or {}).get("stage") or "") != "planning_start"
        or not str(job.error or "").startswith("TMDB 未找到自动匹配候选:")
        or (job.directory / "media-journal.json").exists()
        or unscraped_pending_delete(job.source)
    ):
        return False
    if any(
        other.id != job.id
        and other.source == job.source
        and (other.updated_at, other.id) > (job.updated_at, job.id)
        for other in JOBS.values()
    ):
        return False
    try:
        from engine import scraper as scraper_engine  # pylint: disable=import-outside-toplevel

        query = job.query or scraper_engine._query_from_source(job.source)
        failed_query = str(job.error).split(":", 1)[1].strip()
        variants = scraper_engine._search_query_variants(query)
        return (
            scraper_engine._source_suggests_batch(job.source)
            or any(variant.strip() != failed_query for variant in variants)
        )
    except (ImportError, AttributeError, ValueError):
        return False


def _resume_failed_planning(job: Job) -> bool:
    if not _automatic_planning_retry_allowed(job):
        return False
    with LOCK:
        job.phase = "queued"
        job.error = None
        job.digest = None
        job.cancel_requested = False
        job.force_killed = False
        job.plan_summary = None
        job.progress = {
            "stage": "planning_retry", "completed": 0, "total": 1,
            "percent": 1.0, "message": "新版标题规范化提供了新证据，正在自动重试",
        }
        job.updated_at = utc_now()
        persist_job(job)
    append_log(job, "新版引擎已生成不同的标题查询或多作品证据；自动重试，仍有歧义时会继续阻止。")
    if archive_journal_succeeded(job):
        start_thread(plan_media, job)
    else:
        start_thread(prepare_job, job)
    return True


def _legacy_lineage_ids_from_disk() -> set[str]:
    """Precompute the retired closure before restore can rewrite any record."""
    payloads: dict[str, dict[str, Any]] = {}
    for state_path in sorted(JOBS_ROOT.glob("*/job.json")):
        try:
            value = load_json(state_path)
            job_id = str(value.get("id") or "")
            if job_id == state_path.parent.name and re.fullmatch(r"[0-9a-f]{12}", job_id):
                payloads[job_id] = value
        except (OSError, ValueError, json.JSONDecodeError):
            continue
    index_invalid = False
    try:
        _retired_roots, retired_members = retired_legacy_lineage_ids(STATE_ROOT)
    except Exception:
        retired_members = set()
        index_invalid = True
    lineage_ids = set(retired_members)
    lineage_ids.update(
        job_id for job_id, value in payloads.items()
        if value.get("approval_source") == LEGACY_ONE_TIME_APPROVAL_SOURCE
    )
    changed = True
    while changed:
        changed = False
        for job_id, value in payloads.items():
            if job_id in lineage_ids:
                continue
            parent_id = value.get("root_job_id")
            if isinstance(parent_id, str) and parent_id in lineage_ids:
                lineage_ids.add(job_id)
                changed = True
    if index_invalid:
        # The deny-list cannot identify an already archived root.  Refuse all
        # linked internal records rather than accidentally resuming one.
        lineage_ids.update(
            job_id for job_id, value in payloads.items()
            if value.get("visibility") == "internal" and value.get("root_job_id")
        )
    return lineage_ids


def restore_jobs() -> None:
    SCHEDULER.clear()
    legacy_lineage_ids = _legacy_lineage_ids_from_disk()
    with LOCK:
        JOBS.clear()
        for state_path in sorted(JOBS_ROOT.glob("*/job.json")):
            try:
                value = load_json(state_path)
                job_id = str(value["id"])
                if not re.fullmatch(r"[0-9a-f]{12}", job_id) or job_id != state_path.parent.name:
                    raise ValueError("任务 ID 与状态目录不一致")
                root_job_id = value.get("root_job_id")
                if root_job_id is not None and (
                    not isinstance(root_job_id, str)
                    or not re.fullmatch(r"[0-9a-f]{12}", root_job_id)
                ):
                    raise ValueError("查补根任务 ID 无效")
                replenishment_round = value.get("replenishment_round", 0)
                if (
                    type(replenishment_round) is not int
                    or not 0 <= replenishment_round <= 1_000_000
                ):
                    raise ValueError("查补轮次无效")
                job = Job(
                    id=job_id,
                    source=str(value["source"]),
                    parent=str(value["parent"]),
                    media_type=str(value.get("media_type", "auto")),
                    absolute=bool(value.get("absolute", False)),
                    prefer_simplified=bool(value.get("prefer_simplified", True)),
                    tmdb_id=value.get("tmdb_id"),
                    query=value.get("query"),
                    season=value.get("season"),
                    episode_group=value.get("episode_group"),
                    episode_map=value.get("episode_map"),
                    collection_map=value.get("collection_map"),
                    visibility=str(value.get("visibility") or "user"),
                    created_at=str(value.get("created_at") or utc_now()),
                    updated_at=str(value.get("updated_at") or utc_now()),
                    phase=str(value.get("phase") or "failed"),
                    error=value.get("error"),
                    digest=value.get("digest"),
                    approval_source=value.get("approval_source"),
                    plan_summary=value.get("plan"),
                    progress=value.get("progress"),
                    root_job_id=root_job_id,
                    replenishment_round=replenishment_round,
                )
                if job.phase not in VALID_PHASES:
                    raise ValueError(f"未知任务阶段: {job.phase}")
                if job.approval_source not in {
                    None, "auto", "manual", LEGACY_ONE_TIME_APPROVAL_SOURCE,
                }:
                    raise ValueError("未知计划批准来源")
                if job.visibility not in {"user", "internal"}:
                    raise ValueError("未知任务可见性")
                if job.id in legacy_lineage_ids:
                    if is_legacy_one_time_owner(job):
                        if job.visibility != "internal" or job.root_job_id is not None:
                            raise ValueError("历史 one-time owner 身份无效")
                    elif job.visibility != "internal" or not job.root_job_id:
                        raise ValueError("历史 one-time descendant 身份无效")
                    if job.log_path.exists():
                        job.logs = job.log_path.read_text(encoding="utf-8").splitlines()[-MAX_LOG_LINES:]
                    # Inventory only.  Do not reconcile, rewrite, remember or
                    # schedule this retired control-plane record on startup.
                    JOBS[job.id] = job
                    continue
                if (
                    job.visibility != "internal"
                    and is_replenishment_system_source(job.source)
                ):
                    job.visibility = "internal"
                    persist_job(job)
                if job.log_path.exists():
                    job.logs = job.log_path.read_text(encoding="utf-8").splitlines()[-MAX_LOG_LINES:]
                reconcile_post_commit_cancel(job)
                reconcile_legacy_completed_source_wait(job)
                reconcile_pending_delete_job(job)
                if job.phase == "failed" and job.error and "查看实时日志" in job.error:
                    job.error = command_failure_reason("\n".join(job.logs), job.error)
                    persist_job(job)
                if job.phase in {"extracting_archives", "executing_media", "executing_recovery", "cancelling"}:
                    if (job.directory / "media-journal.json").exists():
                        job.phase = "recovery_required"
                        job.error = "上次运行在写入阶段中断，请先检查并执行恢复。"
                    else:
                        job.phase = "failed"
                        job.error = "上次运行意外中断，请检查 AList 任务与远端锁。"
                    job.digest = None
                    persist_job(job)
                elif job.phase in {"planning_archives", "planning_media", "planning_recovery"}:
                    if job.phase == "planning_recovery" and (job.directory / "media-journal.json").exists():
                        job.phase = "recovery_required"
                        job.error = "恢复检查被服务重启中断，请重新检查恢复计划。"
                    else:
                        if job.phase == "planning_archives":
                            (job.directory / "archive-plan.json").unlink(missing_ok=True)
                        if job.phase == "planning_media":
                            (job.directory / "media-plan.json").unlink(missing_ok=True)
                        job.phase = "queued"
                        job.error = None
                    job.digest = None
                    persist_job(job)
                elif job.phase == "awaiting_media_approval":
                    plan_path = job.directory / "media-plan.json"
                    if plan_path.exists():
                        plan, digest = unwrap_media_plan(load_json(plan_path))
                        if job.digest and not secrets.compare_digest(job.digest, digest):
                            raise ValueError("任务摘要与媒体计划 SHA-256 不一致")
                        refreshed_summary = summarize_media_plan(plan)
                        if job.plan_summary != refreshed_summary or job.digest != digest:
                            job.plan_summary = refreshed_summary
                            job.digest = digest
                            persist_job(job)
                complete_internal_replenishment_followup(job, restored=True)
                reconcile_completed_replenishment_summary(job)
                finalize_replenishment_acquisition_artifacts(job)
                JOBS[job.id] = job
                remember_completed_job(job)
            except (KeyError, OSError, ValueError, json.JSONDecodeError) as exc:
                print(f"[local-api] 忽略损坏的任务状态 {state_path}: {exc}", file=sys.stderr)


def reconcile_transaction_lifecycles_on_startup() -> None:
    """Resume retained rollback batches before any normal work is scheduled."""
    with LOCK:
        jobs = sorted(JOBS.values(), key=lambda item: (item.created_at, item.id))
    handled_lineages: set[str] = set()
    for job in jobs:
        if is_legacy_one_time_lineage(job):
            continue
        try:
            lifecycle = _load_terminal_transaction_lifecycle(job)
            lifecycle_blockers = (
                _transaction_lifecycle_cleanup_blockers(job)
                if lifecycle is not None else []
            )
        except Exception as exc:
            lifecycle = None
            lifecycle_blockers = [redact(str(exc))]
        if lifecycle_blockers:
            job.phase = "recovery_required"
            job.error = (
                "启动时事务生命周期回执未闭环："
                + "；".join(lifecycle_blockers[:4])
            )
            job.digest = None
            persist_job(job)
            append_log(job, job.error)
            continue
        if lifecycle is not None:
            # A valid terminal receipt is idempotent.  Other pending members
            # of the same lineage are encountered and reconciled separately.
            if lifecycle["outcome"] in {"accepted", "restored"}:
                continue
        try:
            hybrid, remote_delete = _job_transaction_bindings(job)
        except Exception as exc:
            job.phase = "recovery_required"
            job.error = "启动时无法验证远端回滚事务：" + redact(str(exc))
            job.digest = None
            persist_job(job)
            append_log(job, job.error)
            continue
        if hybrid is None and remote_delete is None:
            continue

        lineage_id = job.root_job_id or job.id
        root = JOBS.get(lineage_id)
        if root is not None and lineage_id not in handled_lineages:
            handled_lineages.add(lineage_id)
            lineage_reconciled = False
            try:
                if (
                    root.phase == "completed"
                    and _ordinary_scrape_source(root.source) is not None
                ):
                    final_acceptance = _ordinary_final_completion_contract(root)
                    if final_acceptance["accepted"] is True:
                        result = _commit_transaction_lineage(root)
                        message = "启动核对严格完成证据后，已提交远端回滚批次。"
                    else:
                        result = _restore_transaction_lineage(
                            root, reason="startup_invalid_completed_state",
                        )
                        root.phase = "recovery_required"
                        root.error = "历史 completed 任务缺少严格作品闭环，已恢复原始文件。"
                        root.digest = None
                        message = root.error
                    summary = dict(root.plan_summary or {})
                    summary["transaction_lifecycle"] = result
                    summary["ordinary_final_completion"] = final_acceptance
                    root.plan_summary = summary
                    persist_job(root)
                    append_log(root, message)
                    lineage_reconciled = True
                elif root.phase in {
                    "failed", "cancelled", "recovered", "recovery_required",
                    "planning_recovery", "awaiting_recovery_approval",
                    "starting_recovery_execution", "executing_recovery",
                }:
                    result = _restore_transaction_lineage(
                        root, reason="startup_reconcile",
                    )
                    summary = dict(root.plan_summary or {})
                    summary["transaction_lifecycle"] = result
                    root.plan_summary = summary
                    persist_job(root)
                    append_log(root, "启动时已恢复并关闭未提交的远端回滚批次。")
                    lineage_reconciled = True
            except Exception as exc:
                root.phase = "recovery_required"
                root.error = "启动事务 reconcile 未闭环：" + redact(str(exc))
                root.digest = None
                persist_job(root)
                append_log(root, root.error)
                continue
            if lineage_reconciled:
                continue

        if job.root_job_id and root is None:
            try:
                result = _restore_job_transaction_quarantines(
                    job, reason="startup_orphan_child",
                )
            except Exception as exc:
                job.error = "孤立内部任务事务 reconcile 未闭环：" + redact(str(exc))
            else:
                summary = dict(job.plan_summary or {})
                summary["transaction_lifecycle"] = result
                job.plan_summary = summary
                job.error = "孤立内部任务已恢复原始文件，等待人工核对。"
            job.phase = "recovery_required"
            job.digest = None
            persist_job(job)
            append_log(job, str(job.error))
            continue

        # An orphaned/failed internal child must not bypass restore merely
        # because its source lives under the system replenishment tree.
        if job.phase in {"failed", "cancelled", "recovered", "recovery_required"}:
            try:
                result = _restore_job_transaction_quarantines(
                    job, reason="startup_orphan_child",
                )
            except Exception as exc:
                job.phase = "recovery_required"
                job.error = "内部任务事务 reconcile 未闭环：" + redact(str(exc))
                job.digest = None
            else:
                summary = dict(job.plan_summary or {})
                summary["transaction_lifecycle"] = result
                job.plan_summary = summary
            persist_job(job)


def resume_jobs() -> None:
    """Rebuild both FIFO queues from safely resumable persisted states."""
    with LOCK:
        resumable = sorted(JOBS.values(), key=lambda row: (row.created_at, row.id))
    # Repair exact replenishment children before scheduling any coordinator.
    # Otherwise an older root can restart a new search before its already
    # materialized internal child has inherited the acquisition evidence.
    for job in resumable:
        if is_legacy_one_time_lineage(job):
            continue
        close_consumed_internal_replenishment_followup(job)
    for job in resumable:
        if is_legacy_one_time_lineage(job):
            continue
        _repair_failed_replenishment_followup(job)
    for job in resumable:
        if is_legacy_one_time_lineage(job):
            continue
        if _resume_scrape_first_wait(job, restored=True):
            continue
        if job.phase == "queued":
            if archive_journal_succeeded(job):
                start_thread(plan_media, job)
            else:
                start_thread(prepare_job, job)
        elif job.phase == "starting_archive_execution":
            try:
                digest = canonical_digest(load_json(job.directory / "archive-plan.json"))
            except (OSError, ValueError, json.JSONDecodeError) as exc:
                fail_job(job, f"无法恢复待执行的解压计划: {exc}")
                continue
            job.digest = digest
            persist_job(job)
            start_execution(execute_archive, job, digest)
        elif job.phase in {
            "recovery_required", "planning_recovery",
            "awaiting_recovery_approval", "starting_recovery_execution",
            "executing_recovery",
        } and _resume_post_commit_replenishment(job, restored=True):
            continue
        elif job.phase == "starting_media_execution" and job.digest:
            if job.approval_source != "manual":
                try:
                    wrapper = load_json(job.directory / "media-plan.json")
                    plan, digest = unwrap_media_plan(wrapper)
                except (OSError, ValueError, json.JSONDecodeError) as exc:
                    fail_job(job, f"自动执行恢复失败：无法验证媒体计划：{exc}")
                    continue
                plan = unattended_media_plan(plan)
                digest = canonical_digest(plan)
                _persist_executable_media_plan(
                    job.directory / "media-plan.json", wrapper, plan, digest,
                )
                if media_plan_requires_review(plan):
                    update_job(
                        job,
                        phase="awaiting_media_approval",
                        digest=digest,
                        approval_source=None,
                        plan_summary=summarize_media_plan(plan),
                    )
                    append_log(job, "服务恢复时发现计划包含风险，已重新转入人工审核。")
                    continue
                job.approval_source = "auto"
                job.digest = digest
                persist_job(job)
            start_execution(execute_approved_media, job, job.digest)
        elif job.phase == "starting_recovery_execution" and job.digest:
            start_execution(execute_approved_recovery, job, job.digest)
        elif job.phase == "awaiting_recovery_approval" and auto_execute_media_enabled():
            if not job.digest:
                update_job(
                    job, phase="recovery_required",
                    error="自动恢复缺少 journal 摘要，正在重新检查。",
                )
                request_recovery(job)
                continue
            update_job(job, phase="starting_recovery_execution", error=None)
            append_log(job, "服务恢复后自动继续回滚，无需人工批准。")
            start_execution(execute_approved_recovery, job, job.digest)
        elif job.phase == "awaiting_media_approval":
            if not auto_execute_media_enabled():
                continue
            if not job.digest:
                fail_job(job, "自动执行失败：媒体计划缺少 SHA-256 摘要")
                continue
            try:
                wrapper = load_json(job.directory / "media-plan.json")
                plan, digest = unwrap_media_plan(wrapper)
            except (OSError, ValueError, json.JSONDecodeError) as exc:
                fail_job(job, f"自动执行恢复失败：无法验证媒体计划：{exc}")
                continue
            plan = unattended_media_plan(plan)
            digest = canonical_digest(plan)
            _persist_executable_media_plan(
                job.directory / "media-plan.json", wrapper, plan, digest,
            )
            if media_plan_requires_review(plan):
                job.digest = digest
                job.plan_summary = summarize_media_plan(plan)
                persist_job(job)
                append_log(job, "计划仍包含需要人工确认的风险，已保留在审核队列。")
                continue
            update_job(job, phase="starting_media_execution", digest=digest)
            job.approval_source = "auto"
            persist_job(job)
            append_log(job, "自动流水线已恢复，计划将重新校验后进入执行队列。")
            start_execution(execute_approved_media, job, job.digest)
        elif job.phase == "replenishing":
            replenishment = (
                job.plan_summary.get("replenishment")
                if isinstance(job.plan_summary, dict) else None
            )
            if (
                isinstance(replenishment, Mapping)
                and replenishment.get("status") in {
                    "subtitle_retryable", "post_check_failed", "title_reaudit_paused",
                }
            ):
                _schedule_current_title_reaudit(
                    job, summary=dict(job.plan_summary), delay=1, restored=True,
                )
                continue
            if (
                isinstance(replenishment, Mapping)
                and replenishment.get("status") == "awaiting_sources"
            ):
                _schedule_current_title_source_review(
                    job, summary=dict(job.plan_summary), restored=True,
                )
                continue
            followup_ids = (
                replenishment.get("followup_job_ids")
                if isinstance(replenishment, dict) else None
            )
            if isinstance(followup_ids, list) and followup_ids:
                if media_journal_succeeded(job):
                    _append_post_commit_resume_log_once(
                        job, "服务恢复后继续等待查补后续任务完成并再次审计。",
                    )
                else:
                    append_log(job, "服务恢复后继续等待查补后续任务完成并再次审计。")
                _launch_replenishment_followup_monitor(job, list(followup_ids))
            else:
                if media_journal_succeeded(job):
                    _append_post_commit_resume_log_once(
                        job, "服务恢复后继续完成缺项搜索、到盘核验与后续任务创建。",
                    )
                else:
                    append_log(job, "服务恢复后继续完成缺项搜索、到盘核验与后续任务创建。")
                start_thread(finalize_media_replenishment, job)
        elif job.phase == "failed" and _resume_failed_current_title_source_wait(job):
            continue
        elif job.phase == "failed" and _resume_failed_planning(job):
            continue
        elif job.phase == "failed" and _resume_post_commit_replenishment(job, restored=True):
            continue
        elif job.phase == "failed" and _resume_failed_unattended_plan(job):
            continue
        elif job.phase == "failed" and _automatic_replenishment_retry_allowed(job):
            append_log(job, "服务恢复后自动续接尚未落地的查补任务，无需人工点击重试。")
            _schedule_replenishment_retry(job, restored=True)
        elif job.phase == "recovery_required" and auto_execute_media_enabled():
            append_log(job, "服务恢复后自动校验并回滚未完成的媒体写入。")
            request_recovery(job)


def update_job(job: Job, **changes: Any) -> None:
    with LOCK:
        if "phase" in changes:
            require_transition(job.phase, str(changes["phase"]))
        for key, value in changes.items():
            setattr(job, key, value)
        job.updated_at = utc_now()
        persist_job(job)


def append_log(job: Job, line: str) -> None:
    safe = redact(line)
    if not safe:
        return
    with LOCK:
        job.logs.append(safe)
        if len(job.logs) > MAX_LOG_LINES:
            del job.logs[: len(job.logs) - MAX_LOG_LINES]
        job.updated_at = utc_now()
        with job.log_path.open("a", encoding="utf-8") as handle:
            handle.write(safe + "\n")
        os.chmod(job.log_path, 0o600)
        if job.log_path.stat().st_size > MAX_LOG_BYTES:
            job.log_path.write_text("\n".join(job.logs) + "\n", encoding="utf-8")
            os.chmod(job.log_path, 0o600)


def run_command(job: Job, command: list[str]) -> tuple[int, str]:
    append_log(job, f"$ {' '.join(command[:2])} …")
    if job.cancel_requested:
        return 130, ""
    process = subprocess.Popen(
        command,
        cwd=PROJECT_ROOT,
        env=command_env(),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )
    update_job(job, process=process)
    captured: list[str] = []
    assert process.stdout is not None
    for line in process.stdout:
        if line.startswith(PROGRESS_PREFIX):
            try:
                progress = json.loads(line[len(PROGRESS_PREFIX):])
                if (
                    not isinstance(progress, dict)
                    or not isinstance(progress.get("stage"), str)
                    or isinstance(progress.get("percent"), bool)
                    or not isinstance(progress.get("percent"), (int, float))
                    or not 0 <= float(progress["percent"]) <= 100
                ):
                    raise ValueError("invalid progress payload")
            except (json.JSONDecodeError, ValueError):
                append_log(job, "警告: 引擎返回了无效的进度事件")
            else:
                update_job(job, progress=progress)
            continue
        if line.startswith((RECOVERY_DIGEST_PREFIX, RECOVERY_ITEM_PREFIX)):
            # Machine-readable recovery evidence is parsed by prepare_recovery
            # and must not be mixed into user-facing logs.
            captured.append(line)
            continue
        captured.append(line)
        append_log(job, line)
    process.stdout.close()
    code = process.wait()
    update_job(job, process=None)
    return code, "".join(captured)


class ReplenishmentMaintenanceStop(Exception):
    """Abort post-commit coordination without converting it to cancellation."""


class ReplenishmentShutdownStop(Exception):
    """Leave a process-exit boundary without manufacturing a task failure."""


class ReplenishmentCancelledStop(Exception):
    """Project a cooperative post-commit stop as cancelled, never complete."""


class SubtitleDispatchBlocked(RuntimeError):
    """The durable pause gate closed during a current-title subtitle cycle."""


def _raise_if_replenishment_maintenance_stopped(job: Job) -> None:
    """Stop at a replenishment stage boundary while global dispatch is closed.

    A search subprocess that was already running may finish after the operator
    pauses the service.  It must not then advance into selection/acquisition.
    Keep the worker parked at the boundary; an ordinary resume continues it,
    while API shutdown marks ``maintenance_stop_requested`` and converts the
    durable coordinator into a restart-safe parked job.
    """
    while _remote_dispatch_closed():
        if job.cancel_requested:
            raise ReplenishmentCancelledStop
        if job.maintenance_stop_requested:
            raise ReplenishmentMaintenanceStop
        if SHUTDOWN_EVENT.is_set() and job.cancel_requested:
            raise ReplenishmentShutdownStop
        time.sleep(0.1)
    if job.cancel_requested:
        raise ReplenishmentCancelledStop
    if job.maintenance_stop_requested:
        raise ReplenishmentMaintenanceStop
    if SHUTDOWN_EVENT.is_set():
        raise ReplenishmentShutdownStop


def _run_replenishment_mutation_stage(
    job: Job, action: Callable[[], Any], *, scrape_gate_sha256: str | None = None,
) -> Any:
    """Atomically cross an open dispatch gate into one bounded mutation."""
    while True:
        _raise_if_replenishment_maintenance_stopped(job)
        with GLOBAL_CONTROL_TRANSITION_LOCK, SCRAPE_FIRST_TRANSITION_LOCK:
            # Pause may have won the race between the wait above and this
            # lock.  Release the lock before waiting so resume cannot deadlock.
            if _remote_dispatch_closed():
                continue
            if job.cancel_requested:
                raise ReplenishmentCancelledStop
            if job.maintenance_stop_requested:
                raise ReplenishmentMaintenanceStop
            if scrape_gate_sha256 is not None:
                _revalidate_scrape_first_snapshot(job, scrape_gate_sha256)
            return action()


def fail_job(job: Job, message: str) -> None:
    append_log(job, f"错误: {message}")
    update_job(job, phase="failed", error=message, digest=None)


def command_failure_reason(output: str, fallback: str) -> str:
    """Return the actionable engine error instead of a generic log pointer."""
    lines = [redact(line).strip() for line in output.splitlines() if line.strip()]
    for line in reversed(lines):
        adapter_match = re.match(r"^补源适配器失败\s*[:：]\s*(.+)$", line)
        if adapter_match:
            return adapter_match.group(1).strip()[:800] or fallback
        match = re.match(r"^(?:❌|错误\s*[:：])\s*(.+)$", line)
        if match:
            reason = match.group(1).strip()
            if "查看实时日志" in reason:
                continue
            return reason[:800] or fallback
    for line in reversed(lines):
        if re.match(
            r"^(?:[A-Za-z_][\w.]*)(?:Error|Exception|IncompleteRead):\s*.+$",
            line,
        ):
            return line[:800]
    return fallback


def _residue_title_key(value: str) -> str:
    """Return a conservative key for an empty source and an existing target."""
    normalized = unicodedata.normalize("NFKC", value).casefold()
    normalized = re.sub(r"\s*[（(](?:19|20)\d{2}[)）]\s*$", "", normalized)
    return re.sub(r"[\s._\-—:：·]+", "", normalized)


def _residue_explicit_year(value: str) -> str | None:
    match = re.search(r"[（(]((?:19|20)\d{2})[)）]\s*$", unicodedata.normalize("NFKC", value))
    return match.group(1) if match else None


def _directory_has_file(
    client: Any,
    root: str,
    *,
    video_only: bool = False,
    ignore_disposable_menu: bool = False,
    max_directories: int = 10_000,
) -> bool:
    """Inspect physical AList rows, failing closed on malformed evidence."""
    video_extensions = {
        ".mkv", ".mp4", ".avi", ".mov", ".wmv", ".m4v", ".ts", ".m2ts", ".webm",
    }
    stack = [media_library_path(root, allow_root=False)]
    visited: set[str] = set()
    while stack:
        current = stack.pop()
        if current in visited:
            continue
        if len(visited) >= max_directories:
            raise ValueError(f"空目录核对超过 {max_directories} 个子目录")
        visited.add(current)
        for item in client.list(current, refresh=True):
            name = item.get("name")
            if not isinstance(name, str) or not name or "/" in name or name in {".", ".."}:
                raise ValueError(f"AList 返回无法验证的目录条目: {current}")
            if item.get("is_dir"):
                stack.append(posixpath.join(current, name))
            else:
                is_video = posixpath.splitext(name)[1].casefold() in video_extensions
                directory_name = posixpath.basename(current).casefold()
                disposable_menu = (
                    is_video
                    and ignore_disposable_menu
                    and (
                        directory_name in {"menu", "menus", "菜单"}
                        or re.search(r"(?i)(?:^|[\s._\-\[\]()])menu\s*\d*(?=$|[\s._\-\[\]()])", name)
                    )
                )
                if disposable_menu:
                    continue
                if not video_only or is_video:
                    return True
    return False


def complete_empty_source_residue(job: Job, reason: str) -> bool:
    """Turn proven non-actionable, already-organized residue into a no-op.

    Explicit disc-menu clips are ignored because the engine intentionally
    excludes them from episode planning. Subtitle-only, archive-only, arbitrary
    video and unreadable sources deliberately return ``False`` so the original
    actionable failure remains visible to the user.
    """
    no_media_markers = (
        "未找到剧集视频文件",
        "未找到可识别的剧集媒体文件",
        "未找到电影媒体文件",
    )
    if not any(marker in reason for marker in no_media_markers):
        return False
    try:
        client = _execution_alist_client()
        if _directory_has_file(client, job.source, ignore_disposable_menu=True):
            return False
        source_name = posixpath.basename(job.source)
        source_key = _residue_title_key(source_name)
        source_year = _residue_explicit_year(source_name)
        target_candidates: list[str] = []
        for item in client.list(job.parent, refresh=True):
            name = item.get("name")
            if (
                item.get("is_dir")
                and isinstance(name, str)
                and _residue_title_key(name) == source_key
            ):
                candidate = posixpath.join(job.parent, name)
                candidate_year = _residue_explicit_year(name)
                if source_year and candidate_year != source_year:
                    continue
                if _directory_has_file(client, candidate, video_only=True):
                    target_candidates.append(candidate)
        if len(target_candidates) != 1:
            return False
        target_root = target_candidates[0]
    except Exception as exc:  # read-only proof failure must not hide the real error
        append_log(job, f"空目录残留核对未通过：{exc}")
        return False

    with LOCK:
        if job.phase != "planning_media" or job.cancel_requested:
            return False
        update_job(
            job,
            phase="completed",
            error=None,
            digest=None,
            plan_summary={
                "kind": "noop",
                "source_root": job.source,
                "target_root": target_root,
                "title": source_name,
                "file_count": 0,
                "normal_file_count": 0,
                "warnings": ["源目录已无主要媒体（或仅剩可忽略的光盘菜单片段），且目标库已存在对应视频；本次为幂等跳过。"],
                "problem_files": [],
                "problem_file_count": 0,
                "cleanup_files": [],
                "cleanup_file_count": 0,
                "resource_gaps": [],
                "resource_gap_count": 0,
            },
        )
    remember_completed_job(job)
    append_log(job, "源目录已无主要媒体（或仅剩可忽略的菜单片段），且目标库已有对应媒体；已按完成处理。")
    return True


def finish_cancel(job: Job, *, media_execution: bool = False) -> None:
    if job.force_killed and media_execution and (job.directory / "media-journal.json").exists():
        update_job(
            job,
            phase="recovery_required",
            error="任务被强制停止，远端状态可能不完整；请先执行恢复检查。",
            digest=None,
        )
    else:
        update_job(job, phase="cancelled", error=None, digest=None)


def legacy_archive_candidates(source: str) -> list[str]:
    """Read-only fallback for old AList versions that lack safe archive APIs."""
    sys.path.insert(0, str(ENGINE_ROOT))
    from scraper import AListClient  # pylint: disable=import-outside-toplevel

    password = os.getenv("ALIST_PASSWORD")
    if not password:
        raise ValueError("缺少 ALIST_PASSWORD")
    client = AListClient(
        alist_url(),
        os.getenv("ALIST_USERNAME", "admin"),
        password,
        timeout=20,
        retries=1,
        allow_insecure_http=docker_loopback_bridge(),
    )
    client.login()
    candidates: list[str] = []
    for item in client.walk(source):
        name = str(item.get("name") or "")
        if re.search(r"\.(?:7z|zip)\.001$", name, re.I) or re.search(r"\.part0*1\.rar$", name, re.I):
            candidates.append(str(item.get("full_path") or name))
    return candidates


def browse_remote(path: str, *, refresh: bool = False) -> dict[str, Any]:
    normalized = media_library_path(path, allow_root=True)
    password = os.getenv("ALIST_PASSWORD")
    if not password:
        raise ValueError("缺少 ALIST_PASSWORD")
    sys.path.insert(0, str(ENGINE_ROOT))
    from scraper import AListClient  # pylint: disable=import-outside-toplevel

    client = AListClient(
        alist_url(),
        os.getenv("ALIST_USERNAME", "admin"),
        password,
        timeout=10,
        retries=1,
        allow_insecure_http=docker_loopback_bridge(),
    )
    client.login()
    items = client.list(normalized, refresh=refresh)
    directories: list[dict[str, str]] = []
    for item in items:
        name = item.get("name")
        if not isinstance(name, str) or not name or "/" in name or name in {".", ".."}:
            continue
        if item.get("is_dir"):
            child = posixpath.join(normalized.rstrip("/") or "/", name)
            directory = {"name": name, "path": child}
            reserved_reason = unscraped_reserved_reason(child)
            if reserved_reason:
                directory["selectable"] = False
                directory["disabled_reason"] = reserved_reason
            task_phase = directory_task_phase(child)
            if task_phase:
                directory["task_phase"] = task_phase
            pending_delete = bool(
                re.search(r"[（(]\s*待删\d*\s*[）)]\s*$", name)
                or item.get("pending_delete") is True
                or item.get("marked_for_deletion") is True
                or str(item.get("status") or "").casefold() in {
                    "pending_delete", "marked_for_deletion",
                }
            )
            if pending_delete:
                # This is an AList object lifecycle state, not a ScrapeFlow job
                # phase.  Keep it separate so it can never fall back to the
                # UI's generic "unprocessed" bucket.
                directory["directory_state"] = "pending_delete"
            directories.append(directory)
    directories.sort(key=lambda item: item["name"].casefold())
    return {
        "path": normalized,
        "parent": None if normalized == MEDIA_LIBRARY_ROOT else posixpath.dirname(normalized),
        "directories": directories,
    }


def _execution_alist_client() -> Any:
    password = os.getenv("ALIST_PASSWORD")
    if not password:
        raise ValueError("缺少 ALIST_PASSWORD")
    sys.path.insert(0, str(ENGINE_ROOT))
    from scraper import AListClient  # pylint: disable=import-outside-toplevel

    client = AListClient(
        alist_url(),
        os.getenv("ALIST_USERNAME", "admin"),
        password,
        timeout=15,
        retries=1,
        allow_insecure_http=docker_loopback_bridge(),
    )
    client.login()
    return client


def audit_current_job_titles(job: Job) -> dict[str, Any]:
    """Build fresh, exact-title completion evidence after media commit.

    The executable plan supplies the only allowed roots.  The episode adapter
    asks TMDB and AList about those roots again, subtitle inspection stays
    read-only, and OCR is reached only for videos whose Chinese-subtitle state
    remains unknown after external and embedded-stream checks.
    """
    if not media_journal_succeeded(job):
        raise ValueError("当前作品复核要求已成功提交的媒体 journal")
    plan, digest = unwrap_media_plan(load_json(job.directory / "media-plan.json"))
    tmdb_key = os.getenv("TMDB_API_KEY")
    if not tmdb_key:
        raise ValueError("当前作品复核需要 TMDB_API_KEY")
    from engine.scraper import TMDBClient  # pylint: disable=import-outside-toplevel

    client = _execution_alist_client()
    episode_scanner = make_current_title_episode_gap_scanner(
        client, TMDBClient(tmdb_key), today=audit_local_date(),
    )
    evidence = build_title_closure_evidence(
        plan,
        digest,
        alist=client,
        pause_active=_remote_dispatch_closed,
        adapters=TitleClosureAdapters(
            scan_episode_gaps=episode_scanner,
            probe_burned_in_ocr=TITLE_CLOSURE_OCR,
        ),
    )
    if not title_closure_evidence_is_valid(evidence):
        raise ValueError("当前作品复核证据摘要或完成计数无效")
    _atomic_json(job.directory / "title-closure.json", evidence)
    ordinary_source = _ordinary_scrape_source(job.source)
    if ordinary_source is not None:
        completion = build_ordinary_title_completion(
            plan,
            digest,
            evidence,
            source_path=ordinary_source,
            alist=client,
        )
        if not ordinary_completion_evidence_is_valid(completion):
            raise ValueError("普通刮削完成证据 digest 无效")
        _atomic_json(
            job.directory / "ordinary-title-completion.json", completion,
        )
    return evidence


def _persistent_pause_active() -> bool:
    control = global_control_status()
    return control.get("paused") is True and control.get("persistent") is True




def validate_plan_target_boundary(job: Job, plan: Mapping[str, Any]) -> None:
    """Keep resolved media identity inside the operator-selected destination."""
    parent = media_library_path(job.parent, allow_root=True)

    def inside_selected_parent(value: Any, *, label: str) -> str:
        path = media_library_path(value, allow_root=True)
        if path != parent and not path.startswith(parent.rstrip("/") + "/"):
            raise ValueError(f"{label}越出原始刮削目标目录: {path}")
        return path

    inside_selected_parent(plan.get("target_root"), label="计划作品根")
    files = plan.get("files")
    if not isinstance(files, list):
        raise ValueError("计划文件列表无效")
    for item in files:
        if not isinstance(item, Mapping):
            raise ValueError("计划文件项无效")
        inside_selected_parent(item.get("target_dir"), label="计划文件目标目录")


def validate_approved_execution(job: Job, digest: str) -> None:
    """Refresh AList and reject stale sources or target-name collisions."""
    client = _execution_alist_client()
    listings: dict[str, dict[str, dict[str, Any]]] = {}

    def entries(path: str) -> dict[str, dict[str, Any]]:
        normalized = media_library_path(path, allow_root=True)
        if normalized not in listings:
            rows = client.list(normalized, refresh=True)
            listings[normalized] = {
                str(row.get("name") or "").casefold(): row
                for row in rows
                if isinstance(row.get("name"), str) and row.get("name")
            }
        return listings[normalized]

    def directory_exists(path: str) -> bool:
        normalized = media_library_path(path, allow_root=True)
        if normalized == MEDIA_LIBRARY_ROOT:
            return True
        parent, name = posixpath.split(normalized)
        if not directory_exists(parent):
            return False
        row = entries(parent).get(name.casefold())
        return bool(row and row.get("is_dir"))

    if job.phase == "starting_media_execution":
        try:
            entries(job.source)
        except Exception as exc:
            raise ValueError(
                f"执行前检查失败：源目录不存在或无法刷新：{job.source}"
            ) from exc
        plan, actual_digest = unwrap_media_plan(load_json(job.directory / "media-plan.json"))
        if not secrets.compare_digest(actual_digest, digest):
            raise ValueError("执行前检查失败：审核计划已经变化，请重新审核")
        _require_problem_free_media_plan(plan)
        validate_plan_target_boundary(job, plan)
        source_root = media_library_path(plan.get("source_root"), allow_root=False)
        if source_root != job.source:
            raise ValueError("执行前检查失败：计划源目录与当前任务不一致")
        files = plan.get("files")
        if not isinstance(files, list):
            raise ValueError("执行前检查失败：计划文件列表无效")
        for item in files:
            if not isinstance(item, dict):
                raise ValueError("执行前检查失败：计划文件项无效")
            source_path = normalize_remote_input(item.get("source_path"))
            source_dir, source_name = posixpath.split(source_path)
            target_dir = media_library_path(item.get("target_dir"), allow_root=True)
            final_name = item.get("final_name")
            if not isinstance(final_name, str) or not final_name or "/" in final_name:
                raise ValueError("执行前检查失败：目标文件名无效")
            source_rows = entries(source_dir)
            if source_name.casefold() not in source_rows:
                raise ValueError(f"执行前检查失败：源文件已不存在：{source_path}")
            target_path = posixpath.join(target_dir, final_name)
            if target_path == source_path:
                continue
            if directory_exists(target_dir):
                target_rows = entries(target_dir)
                if final_name.casefold() in target_rows:
                    raise ValueError(f"执行前检查失败：目标文件已存在：{target_path}")
    elif job.phase == "starting_recovery_execution":
        plan = job.plan_summary or {}
        for item in plan.get("files") or []:
            if not isinstance(item, dict):
                continue
            source_path = normalize_remote_input(item.get("source"))
            target_path = normalize_remote_input(item.get("target"))
            source_dir, source_name = posixpath.split(source_path)
            target_dir, target_name = posixpath.split(target_path)
            if source_name.casefold() not in entries(source_dir):
                raise ValueError(f"执行前检查失败：恢复源文件已不存在：{source_path}")
            if (
                target_path != source_path
                and directory_exists(target_dir)
                and target_name.casefold() in entries(target_dir)
            ):
                raise ValueError(f"执行前检查失败：恢复目标已存在：{target_path}")


def _schedule_recovery_retry(job: Job, *, delay: int = 5) -> bool:
    """Retry unattended journal recovery without a manual approval dead-end."""
    if (
        not auto_execute_media_enabled()
        or job.cancel_requested
        or job.phase != "recovery_required"
        or not (job.directory / "media-journal.json").exists()
    ):
        return False
    with RECOVERY_RETRY_LOCK:
        if job.id in RECOVERY_RETRY_PENDING:
            return True
        RECOVERY_RETRY_PENDING.add(job.id)

    def retry() -> None:
        try:
            deadline = time.monotonic() + max(0, delay)
            while time.monotonic() < deadline:
                if job.cancel_requested or job.phase != "recovery_required":
                    return
                time.sleep(min(1.0, deadline - time.monotonic()))
            if job.cancel_requested or job.phase != "recovery_required":
                return
            if not _wait_for_global_resume(job):
                return
            try:
                request_recovery(job)
            except ValueError as exc:
                append_log(job, f"自动恢复重试未启动：{redact(str(exc))}")
        finally:
            with RECOVERY_RETRY_LOCK:
                RECOVERY_RETRY_PENDING.discard(job.id)

    threading.Thread(
        target=retry,
        name=f"scrapeflow-recovery-retry-{job.id}",
        daemon=True,
    ).start()
    return True


def execute_approved_media(job: Job, digest: str) -> None:
    try:
        validate_approved_execution(job, digest)
    except Exception as exc:
        message = redact(str(exc)) or "执行前刷新 AList 失败，请稍后重新审核"
        append_log(job, message)
        if auto_execute_media_enabled() and job.approval_source != "manual":
            fail_job(job, message)
        else:
            update_job(job, phase="awaiting_media_approval", error=message)
        return
    append_log(job, "AList 已刷新，源路径和目标冲突检查通过。")
    execute_media(job, digest)


def execute_approved_recovery(job: Job, digest: str) -> None:
    try:
        validate_approved_execution(job, digest)
    except Exception as exc:
        message = redact(str(exc)) or "执行前刷新 AList 失败，请稍后重新审核"
        append_log(job, message)
        if auto_execute_media_enabled():
            update_job(job, phase="recovery_required", error=message)
            append_log(job, "恢复前校验未通过；稍后自动重新读取 journal 与远端状态。")
            _schedule_recovery_retry(job)
        else:
            update_job(job, phase="awaiting_recovery_approval", error=message)
        return
    append_log(job, "AList 已刷新，恢复源路径和目标冲突检查通过。")
    execute_recovery(job, digest)


def plan_media(job: Job) -> None:
    if job.cancel_requested:
        update_job(job, phase="cancelled")
        return
    update_job(job, phase="planning_media", error=None, digest=None, plan_summary=None, progress=None)
    plan_path = job.directory / "media-plan.json"
    command = [
        sys.executable,
        str(SCRAPER),
        *common_connection_args(),
        "--parent",
        job.parent,
        "--type",
        job.media_type,
        "--plan-json",
        str(plan_path),
    ]
    if job.tmdb_id is not None:
        command.extend(["--id", str(job.tmdb_id)])
    elif job.media_type != "auto":
        command.append("--auto-match")
    if job.media_type in {"auto", "tv"}:
        command.append("--auto-episode-mode")
    if job.query:
        command.extend(["--query", job.query])
    if job.season is not None:
        command.extend(["--season", str(job.season)])
    if job.absolute:
        command.append("--absolute")
    if job.episode_group:
        command.extend(["--episode-group", job.episode_group])
    if job.episode_map:
        episode_map_path = job.directory / "episode-map.json"
        _atomic_json(episode_map_path, job.episode_map)
        command.extend(["--episode-map", str(episode_map_path)])
    if job.collection_map:
        collection_map_path = job.directory / "collection-map.json"
        _atomic_json(collection_map_path, job.collection_map)
        command.extend(["--collection-map", str(collection_map_path)])
    if job.prefer_simplified and job.media_type in {"auto", "tv"}:
        command.append("--prefer-simplified")
    command.extend(["--", job.source])
    code, output = 1, ""
    for transient_attempt in range(3):
        code, output = run_command(job, command)
        if code == 0 or job.cancel_requested:
            break
        if (
            transient_attempt >= 2
            or not any(marker in output for marker in TRANSIENT_TMDB_FAILURE_MARKERS)
        ):
            break
        delay = 2 ** (transient_attempt + 1)
        append_log(
            job,
            "TMDB 瞬时网络故障，已保留本地审计输出；"
            f"{delay} 秒后自动重试整理计划 "
            f"({transient_attempt + 2}/3)。",
        )
        update_job(
            job,
            progress={
                "stage": "planning_retry",
                "completed": transient_attempt + 1,
                "total": 3,
                "percent": 5.0,
                "message": "TMDB 瞬时网络故障，正在自动重试",
            },
        )
        time.sleep(delay)
    if job.cancel_requested:
        update_job(job, phase="cancelled")
        return
    if code != 0:
        reason = command_failure_reason(output, "媒体识别计划生成失败")
        transient_planning_failure = any(
            marker in output for marker in TRANSIENT_TMDB_FAILURE_MARKERS
        )
        if transient_planning_failure and auto_execute_media_enabled():
            # A failed planner invocation has no approved output.  Remove any
            # stale/partial local artifact and persist a restart-safe queue
            # state before arming the background retry.  The user never needs
            # to visit the failed-task page for a temporary TMDB/TLS outage.
            plan_path.unlink(missing_ok=True)
            try:
                retry_delay = int(os.getenv("SCRAPEFLOW_PLANNING_RETRY_DELAY", "30"))
            except ValueError:
                retry_delay = 30
            retry_delay = max(1, min(300, retry_delay))
            update_job(
                job,
                phase="queued",
                error=None,
                progress={
                    "stage": "planning_retry_wait",
                    "completed": 0,
                    "total": 1,
                    "percent": 5.0,
                    "message": f"TMDB 瞬时网络故障，{retry_delay} 秒后自动重试",
                },
            )
            append_log(
                job,
                f"TMDB 瞬时网络故障连续重试未恢复；{retry_delay} 秒后自动续跑，"
                "无需人工操作。",
            )

            def delayed_retry() -> None:
                deadline = time.monotonic() + retry_delay
                while time.monotonic() < deadline:
                    if job.cancel_requested or job.phase != "queued":
                        return
                    time.sleep(min(1.0, deadline - time.monotonic()))
                if job.phase == "queued" and not job.cancel_requested:
                    start_thread(plan_media, job)

            threading.Thread(
                target=delayed_retry,
                name=f"scrapeflow-planning-retry-{job.id}",
                daemon=True,
            ).start()
            return
        if complete_empty_source_residue(job, reason):
            return
        if job.cancel_requested:
            finish_cancel(job)
            return
        fail_job(job, reason)
        return
    try:
        wrapper = load_json(plan_path)
        plan, digest = unwrap_media_plan(wrapper)
        validate_plan_target_boundary(job, plan)
        if auto_execute_media_enabled():
            plan = unattended_media_plan(plan)
            digest = canonical_digest(plan)
            _persist_executable_media_plan(
                plan_path, wrapper, plan, digest,
            )
        summary = summarize_media_plan(plan)
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        fail_job(job, f"无法读取媒体计划: {exc}")
        return
    review_reasons = media_plan_review_reasons(plan)
    if review_reasons:
        update_job(
            job,
            phase="awaiting_media_approval",
            digest=digest,
            approval_source=None,
            plan_summary=summary,
        )
        append_log(
            job,
            "媒体计划已停止自动执行：" + "；".join(review_reasons)
            + "。请只核对页面归纳出的风险项，再决定是否批准。",
        )
    elif auto_execute_media_enabled():
        update_job(
            job,
            phase="starting_media_execution",
            digest=digest,
            approval_source="auto",
            plan_summary=summary,
        )
        append_log(
            job,
            "媒体计划已生成并通过结构校验；自动流水线将刷新源文件和目标冲突后执行。",
        )
        start_execution(execute_approved_media, job, digest)
    else:
        update_job(
            job,
            phase="awaiting_media_approval",
            digest=digest,
            approval_source=None,
            plan_summary=summary,
        )
        append_log(job, "媒体计划已生成，等待人工审核和批准。")


def prepare_job(job: Job) -> None:
    if job.cancel_requested:
        finish_cancel(job)
        return
    if close_consumed_internal_replenishment_followup(job):
        return
    update_job(job, phase="planning_archives", error=None)
    archive_path = job.directory / "archive-plan.json"
    command = [
        sys.executable,
        str(ARCHIVE_TOOL),
        *common_connection_args(),
        "--plan-json",
        str(archive_path),
    ]
    archive_password_path = job.directory / ".archive-password"
    if archive_password_path.exists():
        command.extend(["--archive-password-file", str(archive_password_path)])
    command.append(job.source)
    code, output = run_command(job, command)
    if job.cancel_requested:
        update_job(job, phase="cancelled")
        return
    if code == 0:
        try:
            plan = load_json(archive_path)
            digest = canonical_digest(plan)
        except (OSError, ValueError, json.JSONDecodeError) as exc:
            fail_job(job, f"无法读取解压计划: {exc}")
            return
        # Archive extraction is a non-destructive preparation step: the source
        # archive is retained and collision checks already passed.  Continue in
        # the same task so the user only confirms once, after media recognition.
        update_job(job, phase="starting_archive_execution", digest=digest, plan_summary=None)
        append_log(job, "压缩包安全检查通过，自动解压并继续识别；原压缩包保留。")
        start_execution(execute_archive, job, digest)
        return
    if code == NO_ARCHIVES_EXIT_CODE:
        plan_media(job)
        return
    if "未找到可支持的首卷" in output:
        append_log(job, "未发现需要解压的分卷，继续识别媒体。")
        plan_media(job)
        return
    if "不满足安全解压要求" in output:
        try:
            candidates = legacy_archive_candidates(job.source)
        except Exception as exc:  # read-only compatibility scan
            fail_job(job, f"旧版 AList 兼容扫描失败: {exc}")
            return
        if not candidates:
            append_log(job, "AList 版本较旧，但目录未发现分卷；继续识别媒体。")
            plan_media(job)
            return
        fail_job(
            job,
            f"发现 {len(candidates)} 个分卷首卷，但当前 AList 不支持安全的服务器端解压；"
            "请先升级到 v3.57.0 或更高版本。",
        )
        return
    fail_job(job, command_failure_reason(output, "压缩包检查失败"))


def execute_archive(job: Job, digest: str) -> None:
    if job.cancel_requested:
        finish_cancel(job)
        return
    update_job(job, phase="extracting_archives", error=None)
    command = [
        sys.executable,
        str(ARCHIVE_TOOL),
        *common_connection_args(),
        "--execute-plan",
        str(job.directory / "archive-plan.json"),
        "--approve-plan-sha256",
        digest,
        "--journal",
        str(job.directory / "archive-journal.json"),
        "--execute",
    ]
    archive_password_path = job.directory / ".archive-password"
    if archive_password_path.exists():
        command.extend(["--archive-password-file", str(archive_password_path)])
    code, output = run_command(job, command)
    if job.cancel_requested:
        finish_cancel(job)
    elif code != 0:
        fail_job(job, command_failure_reason(output, "解压执行失败"))
    else:
        archive_password_path.unlink(missing_ok=True)
        append_log(job, "解压完成，继续生成媒体整理计划。")
        start_thread(plan_media, job)


def _replenishment_summary(
    status: str,
    request: dict[str, Any],
    **details: Any,
) -> dict[str, Any]:
    value = {
        "status": status,
        "round": request.get("round"),
        "gap_count": len(request.get("gaps") or []),
        "gaps": [
            {"id": gap.get("id"), "label": gap.get("label")}
            for gap in request.get("gaps") or []
            if isinstance(gap, dict)
        ],
    }
    value.update(details)
    return value


def _complete_lane_exhaustion_proof(
    lane_status: Mapping[str, Any], provider: str,
) -> dict[str, Any] | None:
    """Return a fail-closed proof that one configured cloud lane is empty."""
    lane = lane_status.get(provider)
    if not isinstance(lane, Mapping) or lane.get("status") != "exhausted":
        return None
    proof = lane.get("proof")
    if not isinstance(proof, Mapping) or proof.get("kind") != "search_complete_no_candidates":
        return None
    required = proof.get("required_sources")
    completed = proof.get("completed_sources")
    if (
        not isinstance(required, list)
        or not required
        or not all(isinstance(item, str) and item for item in required)
        or not isinstance(completed, list)
        or not all(isinstance(item, str) and item for item in completed)
        or set(required) - set(completed)
        or type(proof.get("candidate_count")) is not int
        or proof["candidate_count"] != 0
        or type(proof.get("excluded_candidate_count")) is not int
        or proof["excluded_candidate_count"] < 0
    ):
        return None
    return dict(proof)


def _all_required_sources_exhausted(
    lane_status: Mapping[str, Any], *, remaining_candidate_count: int,
    local_torrent_unlocked: bool,
) -> dict[str, Any] | None:
    """Prove cloud lanes are empty and tier 3 has no remaining candidate."""
    if remaining_candidate_count != 0 or local_torrent_unlocked is not True:
        return None
    proofs: dict[str, Any] = {}
    for provider in ("quark_share", "quark_magnet"):
        proof = _complete_lane_exhaustion_proof(lane_status, provider)
        if proof is None:
            return None
        proofs[provider] = proof
    incomplete_optional_sources = sorted({
        str(source)
        for proof in proofs.values()
        for source in proof.get("incomplete_optional_sources", [])
        if isinstance(source, str) and source
    })
    return {
        "kind": "all_required_sources_exhausted",
        "providers": proofs,
        "local_torrent_unlocked": True,
        "remaining_candidate_count": 0,
        "incomplete_optional_sources": incomplete_optional_sources,
    }


def _replenishment_failure_path(job: Job) -> Path:
    return job.directory / "replenishment-failures.json"


def _replenishment_lane_suppression_path(job: Job) -> Path:
    return job.directory / "replenishment-lane-suppressions.json"


def _replenishment_provider_attempt_path(job: Job) -> Path:
    return job.directory / "replenishment-provider-attempts.json"


def _replenishment_attempt_key(request: Mapping[str, Any]) -> str:
    media = request.get("media") if isinstance(request.get("media"), Mapping) else {}
    gaps = request.get("gaps") if isinstance(request.get("gaps"), list) else []
    return canonical_digest({
        # Attempt/exhaustion evidence is valid only for the exact discovery
        # vocabulary that produced it.  Keeping merely the gap ids here made
        # an older S00-only search suppress a later, stronger search after
        # TMDB episode titles were added to the same gaps.
        "search_semantics_version": 3,
        "media": {
            "tmdb_id": media.get("tmdb_id"),
            "title": str(media.get("title") or "").strip().casefold(),
            "target_root": str(media.get("target_root") or "").strip(),
        },
        "gaps": sorted((
            {
                "id": str(gap.get("id")),
                "title": str(gap.get("title") or "").strip().casefold(),
                "title_aliases": sorted({
                    str(value).strip().casefold()
                    for value in (
                        gap.get("title_aliases")
                        if isinstance(gap.get("title_aliases"), list) else []
                    )
                    if isinstance(value, str) and value.strip()
                }),
                "season_name": str(
                    gap.get("season_name") or ""
                ).strip().casefold(),
            }
            for gap in gaps
            if isinstance(gap, Mapping) and gap.get("id")
        ), key=lambda item: (item["id"], item["title"], item["season_name"])),
        "search_queries": sorted({
            str(query).strip().casefold()
            for query in request.get("search_queries") or []
            if isinstance(query, str) and query.strip()
        }),
    })


def _load_replenishment_provider_attempts(
    job: Job, request: Mapping[str, Any],
) -> dict[str, int]:
    path = _replenishment_provider_attempt_path(job)
    if not path.exists():
        return {"quark_share": 0, "quark_magnet": 0}
    try:
        payload = load_json(path)
    except (OSError, ValueError, json.JSONDecodeError):
        return {"quark_share": 0, "quark_magnet": 0}
    entries = payload.get("entries")
    entry = entries.get(_replenishment_attempt_key(request)) if isinstance(
        entries, Mapping
    ) else None
    attempts = entry.get("attempts") if isinstance(entry, Mapping) else None
    history = entry.get("history") if isinstance(entry, Mapping) else None
    if isinstance(entry, Mapping) and isinstance(history, list):
        evidence_locators = {
            provider: {
                str(locator)
                for row in history if isinstance(row, Mapping)
                and row.get("provider") == provider
                for locator in row.get("locators") or [] if locator
            }
            for provider in ("quark_share", "quark_magnet")
        }
        normalized = {
            provider: len(evidence_locators[provider])
            for provider in ("quark_share", "quark_magnet")
        }
        stored = {
            provider: max(0, int(attempts.get(provider) or 0))
            if isinstance(attempts, Mapping) else 0
            for provider in ("quark_share", "quark_magnet")
        }
        if normalized != stored:
            migrated_entry = dict(entry)
            migrated_entry["attempts"] = normalized
            migrated_entry["migrated_empty_poll_attempts"] = {
                provider: max(0, stored[provider] - normalized[provider])
                for provider in normalized
            }
            migrated_entry["updated_at"] = utc_now()
            migrated_entries = dict(entries)
            migrated_entries[_replenishment_attempt_key(request)] = migrated_entry
            migrated_payload = dict(payload)
            migrated_payload["version"] = max(int(payload.get("version") or 1), 2)
            migrated_payload["entries"] = migrated_entries
            _atomic_json(path, migrated_payload)
            removed = sum(max(0, stored[p] - normalized[p]) for p in normalized)
            if removed:
                append_log(
                    job,
                    f"已移除 {removed} 次无唯一 locator 证据的历史空轮询计数。",
                )
        return normalized
    return {
        provider: max(0, int(attempts.get(provider) or 0))
        if isinstance(attempts, Mapping) else 0
        for provider in ("quark_share", "quark_magnet")
    }


_PERMANENT_REPLENISHMENT_EXHAUSTION_KINDS = frozenset({
    "search_complete_no_candidates",
    "resource_failure_floor_reached",
})


def _normalize_replenishment_exhaustion_entry(
    value: Any,
) -> dict[str, Any] | None:
    """Validate durable lane exhaustion evidence; naked legacy booleans fail closed."""
    if not isinstance(value, Mapping) or value.get("exhausted") is not True:
        return None
    raw_proof = value.get("proof")
    if not isinstance(raw_proof, Mapping):
        return None
    proof = dict(raw_proof)
    kind = str(proof.get("kind") or "")
    if kind not in _PERMANENT_REPLENISHMENT_EXHAUSTION_KINDS:
        return None
    if kind == "search_complete_no_candidates":
        required = proof.get("required_sources")
        completed = proof.get("completed_sources")
        if (
            not isinstance(required, list)
            or not required
            or not all(isinstance(item, str) and item for item in required)
            or not isinstance(completed, list)
            or not all(isinstance(item, str) and item for item in completed)
            or set(required) - set(completed)
            or type(proof.get("candidate_count")) is not int
            or int(proof["candidate_count"]) != 0
            or type(proof.get("excluded_candidate_count")) is not int
            or int(proof["excluded_candidate_count"]) < 0
        ):
            return None
        proof["required_sources"] = sorted(set(required))
        proof["completed_sources"] = sorted(set(completed))
    else:
        required_floor = proof.get("required_floor")
        distinct_failures = proof.get("distinct_failure_count")
        if (
            type(required_floor) is not int
            or required_floor <= 0
            or type(distinct_failures) is not int
            or distinct_failures < required_floor
        ):
            return None
    return {"exhausted": True, "proof": proof}


def _load_replenishment_provider_exhausted(
    job: Job, request: Mapping[str, Any],
) -> dict[str, dict[str, Any]]:
    path = _replenishment_provider_attempt_path(job)
    if not path.exists():
        return {}
    try:
        payload = load_json(path)
    except (OSError, ValueError, json.JSONDecodeError):
        return {}
    entries = payload.get("entries")
    entry = entries.get(_replenishment_attempt_key(request)) if isinstance(
        entries, Mapping
    ) else None
    exhausted = entry.get("exhausted") if isinstance(entry, Mapping) else None
    result: dict[str, dict[str, Any]] = {}
    for provider in ("quark_share", "quark_magnet"):
        normalized = _normalize_replenishment_exhaustion_entry(
            exhausted.get(provider) if isinstance(exhausted, Mapping) else None
        )
        if normalized is not None:
            result[provider] = normalized
    return result


def _load_replenishment_attempted_locators(
    job: Job, request: Mapping[str, Any], provider: str,
) -> set[str]:
    path = _replenishment_provider_attempt_path(job)
    if not path.exists():
        return set()
    try:
        payload = load_json(path)
    except (OSError, ValueError, json.JSONDecodeError):
        return set()
    entries = payload.get("entries")
    entry = entries.get(_replenishment_attempt_key(request)) if isinstance(
        entries, Mapping
    ) else None
    history = entry.get("history") if isinstance(entry, Mapping) else None
    return {
        str(locator)
        for row in history or [] if isinstance(row, Mapping)
        and row.get("provider") == provider
        for locator in row.get("locators") or [] if locator
    }


def _load_replenishment_resource_mismatch_locators(
    job: Job, request: Mapping[str, Any], provider: str,
) -> set[str]:
    """Return locators whose inspected bytes cannot satisfy this gap set."""
    path = _replenishment_provider_attempt_path(job)
    if not path.exists():
        return set()
    try:
        payload = load_json(path)
    except (OSError, ValueError, json.JSONDecodeError):
        return set()
    entries = payload.get("entries")
    entry = entries.get(_replenishment_attempt_key(request)) if isinstance(
        entries, Mapping
    ) else None
    history = entry.get("history") if isinstance(entry, Mapping) else None
    mismatch_reasons = {
        "distinct_cloud_offline_resource_mismatch",
        "distinct_share_resource_failure",
    }
    return {
        str(locator)
        for row in history or [] if isinstance(row, Mapping)
        and row.get("provider") == provider
        and row.get("reason") in mismatch_reasons
        for locator in row.get("locators") or [] if locator
    }


def _record_replenishment_provider_attempts(
    job: Job,
    request: Mapping[str, Any],
    provider: str,
    *,
    count: int = 1,
    reason: str,
    locators: list[str] | None = None,
) -> dict[str, int]:
    """Persist only completed search rounds or proven candidate failures.

    Callers must never invoke this for infrastructure or delivery failures.
    The bounded evidence trail makes it possible to audit why a lane advanced
    without turning transient CDP/network failures into fallback permission.
    """
    if provider not in {"quark_share", "quark_magnet"}:
        raise ValueError("只允许记录一、二级云候选尝试")
    if type(count) is not int or count < 1:
        raise ValueError("补源尝试增量必须是正整数")
    path = _replenishment_provider_attempt_path(job)
    try:
        payload = load_json(path) if path.exists() else {}
    except (OSError, ValueError, json.JSONDecodeError):
        payload = {}
    entries = dict(payload.get("entries") or {}) if isinstance(
        payload.get("entries"), Mapping
    ) else {}
    key = _replenishment_attempt_key(request)
    media = request.get("media") if isinstance(request.get("media"), Mapping) else {}
    gaps = request.get("gaps") if isinstance(request.get("gaps"), list) else []
    entry = dict(entries.get(key) or {}) if isinstance(entries.get(key), Mapping) else {}
    attempts = dict(entry.get("attempts") or {}) if isinstance(
        entry.get("attempts"), Mapping
    ) else {}
    history = [
        dict(row) for row in entry.get("history") or [] if isinstance(row, Mapping)
    ]
    recorded_locators = {
        str(locator)
        for row in history if row.get("provider") == provider
        for locator in row.get("locators") or [] if locator
    }
    normalized_locators = sorted({
        str(value) for value in locators or [] if value
    })
    if locators is not None:
        normalized_locators = [
            locator for locator in normalized_locators
            if locator not in recorded_locators
        ]
        count = len(normalized_locators)
        if count == 0:
            return {
                lane: max(0, int(attempts.get(lane) or 0))
                for lane in ("quark_share", "quark_magnet")
            }
    attempts[provider] = max(0, int(attempts.get(provider) or 0)) + count
    history.append({
        "provider": provider,
        "count": count,
        "reason": str(reason),
        "locators": normalized_locators,
        "recorded_at": utc_now(),
    })
    exhausted = dict(entry.get("exhausted") or {}) if isinstance(
        entry.get("exhausted"), Mapping
    ) else {}
    rules = request.get("rules") if isinstance(request.get("rules"), Mapping) else {}
    try:
        required_floor = max(0, int(rules.get("minimum_attempts_per_cloud_lane") or 0))
    except (TypeError, ValueError):
        required_floor = 0
    existing_exhaustion = _normalize_replenishment_exhaustion_entry(
        exhausted.get(provider)
    )
    existing_proof = (
        existing_exhaustion.get("proof")
        if isinstance(existing_exhaustion, Mapping) else {}
    )
    if (
        required_floor
        and attempts[provider] >= required_floor
        and existing_proof.get("kind") != "search_complete_no_candidates"
    ):
        exhausted[provider] = {
            "exhausted": True,
            "proof": {
                "kind": "resource_failure_floor_reached",
                "required_floor": required_floor,
                "distinct_failure_count": attempts[provider],
            },
        }
    entries[key] = {
        "media": {
            "tmdb_id": media.get("tmdb_id"),
            "title": media.get("title"),
            "target_root": media.get("target_root"),
        },
        "gap_ids": sorted(
            str(gap.get("id")) for gap in gaps
            if isinstance(gap, Mapping) and gap.get("id")
        ),
        "attempts": attempts,
        "exhausted": exhausted,
        "history": history[-200:],
        "updated_at": utc_now(),
    }
    _atomic_json(path, {
        "version": 1,
        "job_id": job.id,
        "entries": entries,
    })
    return {
        lane: max(0, int(attempts.get(lane) or 0))
        for lane in ("quark_share", "quark_magnet")
    }


def _record_replenishment_provider_exhausted(
    job: Job,
    request: Mapping[str, Any],
    provider: str,
    *,
    reason: str,
    proof: Mapping[str, Any],
) -> dict[str, dict[str, Any]]:
    if provider not in {"quark_share", "quark_magnet"}:
        raise ValueError("只允许标记一、二级云候选穷尽")
    path = _replenishment_provider_attempt_path(job)
    try:
        payload = load_json(path) if path.exists() else {}
    except (OSError, ValueError, json.JSONDecodeError):
        payload = {}
    entries = dict(payload.get("entries") or {}) if isinstance(
        payload.get("entries"), Mapping
    ) else {}
    key = _replenishment_attempt_key(request)
    entry = dict(entries.get(key) or {}) if isinstance(entries.get(key), Mapping) else {}
    exhausted = dict(entry.get("exhausted") or {}) if isinstance(
        entry.get("exhausted"), Mapping
    ) else {}
    normalized = _normalize_replenishment_exhaustion_entry({
        "exhausted": True,
        "proof": proof,
    })
    if normalized is None:
        raise ValueError("候选来源穷尽证明不完整")
    existing = _normalize_replenishment_exhaustion_entry(exhausted.get(provider))
    if existing == normalized:
        # Source exhaustion belongs to this exact media/gap set and is a
        # durable state, not another attempt.  Replaying the same completed
        # discovery must not fill the evidence history with identical rows.
        return {
            lane: entry
            for lane in ("quark_share", "quark_magnet")
            if (entry := _normalize_replenishment_exhaustion_entry(exhausted.get(lane)))
            is not None
        }
    exhausted[provider] = normalized
    history = [
        dict(row) for row in entry.get("history") or [] if isinstance(row, Mapping)
    ]
    history.append({
        "provider": provider, "count": 0, "reason": str(reason),
        "proof_kind": normalized["proof"]["kind"],
        "locators": [], "recorded_at": utc_now(),
    })
    media = request.get("media") if isinstance(request.get("media"), Mapping) else {}
    gaps = request.get("gaps") if isinstance(request.get("gaps"), list) else []
    entries[key] = {
        "media": {
            "tmdb_id": media.get("tmdb_id"), "title": media.get("title"),
            "target_root": media.get("target_root"),
        },
        "gap_ids": sorted(
            str(gap.get("id")) for gap in gaps
            if isinstance(gap, Mapping) and gap.get("id")
        ),
        "attempts": dict(entry.get("attempts") or {}),
        "exhausted": exhausted,
        "history": history[-200:],
        "updated_at": utc_now(),
    }
    _atomic_json(path, {"version": 2, "job_id": job.id, "entries": entries})
    return {
        lane: entry
        for lane in ("quark_share", "quark_magnet")
        if (entry := _normalize_replenishment_exhaustion_entry(exhausted.get(lane)))
        is not None
    }


def _load_replenishment_lane_suppressions(job: Job) -> list[dict[str, Any]]:
    path = _replenishment_lane_suppression_path(job)
    if not path.exists():
        return []
    try:
        payload = load_json(path)
    except (OSError, ValueError, json.JSONDecodeError):
        return []
    now = time.time()
    rows = payload.get("suppressions")
    active = [
        dict(row) for row in rows or [] if isinstance(row, dict)
        and isinstance(row.get("until_epoch"), (int, float))
        and float(row["until_epoch"]) > now and row.get("locator")
    ]
    if isinstance(rows, list) and len(active) != len(rows):
        _atomic_json(path, {"version": 1, "suppressions": active})
    return active


def _record_replenishment_lane_suppressions(
    job: Job, rows: Any,
) -> None:
    if not isinstance(rows, list):
        return
    active = _load_replenishment_lane_suppressions(job)
    by_locator = {str(row.get("locator")): row for row in active if row.get("locator")}
    for raw in rows:
        if (
            not isinstance(raw, dict)
            or raw.get("provider") not in {"quark_share", "quark_magnet"}
        ):
            continue
        locator = str(raw.get("locator") or "")
        until = raw.get("until_epoch")
        if not locator or not isinstance(until, (int, float)) or until <= time.time():
            continue
        by_locator[locator] = dict(raw)
    if by_locator:
        _atomic_json(_replenishment_lane_suppression_path(job), {
            "version": 1, "suppressions": list(by_locator.values()),
        })


def _infohash_aliases(value: Any) -> set[str]:
    """Normalize torrent hashes so hex and base32 identify one candidate."""
    raw = str(value or "").strip().casefold()
    if not raw:
        return set()
    aliases = {raw}
    if re.fullmatch(r"[0-9a-f]{40}", raw):
        aliases.add(base64.b32encode(bytes.fromhex(raw)).decode("ascii").rstrip("=").casefold())
    elif re.fullmatch(r"[a-z2-7]{32}", raw):
        try:
            aliases.add(base64.b32decode(raw.upper()).hex())
        except ValueError:
            pass
    return aliases


def _load_replenishment_failures(job: Job) -> list[dict[str, Any]]:
    path = _replenishment_failure_path(job)
    if not path.exists():
        return []
    try:
        value = load_json(path)
    except (OSError, ValueError, json.JSONDecodeError):
        return []
    rows = value.get("failures")
    failures = [dict(row) for row in rows if isinstance(row, dict)] if isinstance(rows, list) else []
    # Older adapters reported every non-zero exit as a candidate failure.  A
    # Quark multipart upload failure occurs after torrent verification and must
    # be migrated out of the permanent locator/infohash exclusion ledger.
    delivery_migrations = sum(
        1 for row in failures if _legacy_reusable_delivery_failure(row)
    )
    cross_lane_migrations = sum(
        1 for row in failures if _legacy_cross_lane_local_failure(row)
    )
    retained = [
        row for row in failures
        if not _legacy_reusable_delivery_failure(row)
        and not _legacy_cross_lane_local_failure(row)
    ]
    if len(retained) != len(failures):
        migrated = dict(value)
        migrated["version"] = max(int(value.get("version") or 1), 2)
        migrated["failures"] = retained
        migrated["migrated_delivery_failures"] = delivery_migrations
        migrated["migrated_cross_lane_local_failures"] = cross_lane_migrations
        _atomic_json(path, migrated)
        if delivery_migrations:
            append_log(
                job,
                f"已从候选隔离账本移除 {delivery_migrations} 条历史上传交付失败；保留候选可重试。",
            )
        if cross_lane_migrations:
            append_log(
                job,
                f"已修复 {cross_lane_migrations} 条被本地 Torrent/aria2 失败污染的"
                "二级云离线隔离记录；恢复原 quark_magnet 候选资格。",
            )
    return retained


def _legacy_reusable_delivery_failure(row: Mapping[str, Any]) -> bool:
    if row.get("failure_scope") == "delivery":
        return True
    reason = str(row.get("reason") or "").casefold()
    provider_markers = (
        "entitytoosmall", "invalidpart", "nosuchupload", "multipart",
        "proposedsize", "partnumber",
    )
    return (
        ("alist" in reason or "上传" in reason or "upload" in reason)
        and any(marker in reason for marker in provider_markers)
    )


def _legacy_cross_lane_local_failure(row: Mapping[str, Any]) -> bool:
    """Drop qmag exclusions whose evidence belongs to the local Torrent lane."""
    if str(row.get("provider") or "") != "quark_magnet":
        return False
    reason = str(row.get("reason") or "").casefold()
    return (
        "aria2" in reason
        or "torrent/download" in reason
        or "下载失败" in reason and "0b/s" in reason
    )


def _failed_replenishment_selections(
    selections: list[dict[str, Any]], output: str,
    *, failure_scope: str | None = None,
    failed_candidate: Mapping[str, Any] | None = None,
) -> list[dict[str, Any]]:
    """Identify the candidate active when the adapter stopped."""
    if (failure_scope is not None and failure_scope != "candidate") or re.search(
        r"\[replenishment\]\s*failure_scope=(?:delivery|infrastructure)\b", output, re.I,
    ):
        # The torrent was downloaded and verified.  A provider-side upload
        # failure must not poison the candidate locator/infohash ledger.
        return []
    if failed_candidate:
        candidate = dict(failed_candidate)
        provider = str(candidate.get("provider") or "").strip()
        locator = str(failed_candidate.get("locator") or "").strip()
        hashes = _infohash_aliases(failed_candidate.get("infohash"))
        release = str(failed_candidate.get("release_name") or "").strip()
        if provider and (locator or hashes or release):
            # The adapter may have switched acquisition lanes internally.  Its
            # structured candidate is the authoritative identity in that case;
            # never coerce a local Torrent failure back onto the selected cloud
            # row merely because both transports share the same BTIH.
            return [candidate]
        matched = [
            selection for selection in selections
            if str(selection.get("provider") or "").strip() == provider
            and (
                (locator and str(selection.get("locator") or "").strip() == locator)
                or (hashes and hashes & _infohash_aliases(selection.get("infohash")))
                or (release and str(selection.get("release_name") or "").strip() == release)
            )
        ]
        if matched:
            return matched
    release_name = None
    for line in reversed(output.splitlines()):
        match = re.search(r"\[replenishment\]\s*下载候选\s+\d+/\d+\s*:\s*(.+)$", line)
        if match:
            release_name = match.group(1).strip()
            break
    if release_name:
        matched = [
            selection for selection in selections
            if str(selection.get("release_name") or "").strip() == release_name
        ]
        if matched:
            return matched
    return selections if len(selections) == 1 else []


def _record_replenishment_failures(
    job: Job, selections: list[dict[str, Any]], output: str, reason: str,
    *, failure_scope: str | None = None,
    failed_candidate: Mapping[str, Any] | None = None,
) -> list[dict[str, Any]]:
    failures = _load_replenishment_failures(job)
    failed = _failed_replenishment_selections(
        selections, output, failure_scope=failure_scope,
        failed_candidate=failed_candidate,
    )
    for selection in failed:
        provider = str(selection.get("provider") or "").strip()
        locator = str(selection.get("locator") or "").strip()
        infohash = str(selection.get("infohash") or "").strip().casefold()
        infohashes = _infohash_aliases(infohash)
        existing = next((
            row for row in failures
            if str(row.get("provider") or "").strip() == provider
            and (
                (locator and row.get("locator") == locator)
                or (infohashes & _infohash_aliases(row.get("infohash")))
            )
        ), None)
        if existing:
            existing["failures"] = int(existing.get("failures") or 1) + 1
            existing["last_failed_at"] = utc_now()
            existing["reason"] = reason
            continue
        failures.append({
            "provider": provider or None,
            "release_name": selection.get("release_name"),
            "locator": locator or None,
            "infohash": infohash or None,
            "reason": reason,
            "failure_scope": "candidate",
            "failures": 1,
            "first_failed_at": utc_now(),
            "last_failed_at": utc_now(),
        })
    if failed:
        _atomic_json(_replenishment_failure_path(job), {
            "version": 1, "job_id": job.id, "failures": failures,
        })
    return failed


def _seed_replenishment_failures_from_last_attempt(job: Job) -> list[dict[str, Any]]:
    """Migrate failures recorded before the durable exclusion ledger existed."""
    if _load_replenishment_failures(job):
        return []
    seeded: list[dict[str, Any]] = []
    for path in sorted(job.directory.glob("replenishment-selection*.json")):
        try:
            wrapper = load_json(path)
        except (OSError, ValueError, json.JSONDecodeError):
            continue
        bundle = wrapper.get("selection")
        selections = bundle.get("selections") if isinstance(bundle, dict) else None
        if not isinstance(selections, list):
            continue
        typed = [dict(item) for item in selections if isinstance(item, dict)]
        seeded.extend(_record_replenishment_failures(
            job, typed, "\n".join(job.logs),
            command_failure_reason("\n".join(job.logs), job.error or "候选获取失败"),
        ))
    return seeded


def _stable_live_target_video_names(client: Any, target_root: str) -> list[str]:
    """Collect a bounded union of forced-refresh target listings.

    AList/cloud storage can briefly return a coherent but stale empty tree
    immediately after a successful move.  Four forced refresh rounds with
    backoff make any newly visible episode a hard stop for acquisition.  The
    union is intentional: once an exact target filename has been observed,
    a later stale response must not make it missing again.
    """
    root = media_library_path(target_root, allow_root=False)
    observed: set[str] = set()
    video_suffixes = {".mkv", ".mp4", ".avi", ".mov", ".wmv", ".m4v", ".ts", ".m2ts", ".webm"}
    for delay in (0.0, 0.25, 0.75, 1.5):
        if delay:
            time.sleep(delay)
        stack = [root]
        visited: set[str] = set()
        while stack:
            current = stack.pop()
            if current in visited:
                continue
            if len(visited) >= 10_000:
                raise ValueError("补源前目标复核超过 10000 个目录")
            visited.add(current)
            rows = client.list(current, refresh=True)
            if not isinstance(rows, list):
                raise ValueError(f"补源前目标复核返回无效目录: {current}")
            # A library root may contain another, independently identified TV
            # work below it (for example a parent Fate directory containing
            # Unlimited Blade Works).  Files below that descendant tvshow.nfo
            # belong to the deepest series root and must never satisfy the
            # parent's gaps.  The live library auditor applies the same
            # deepest-root rule; keep this last-moment acquisition guard in
            # semantic lockstep with it.
            if current != root and any(
                isinstance(item, dict)
                and not item.get("is_dir")
                and str(item.get("name") or "").casefold() == "tvshow.nfo"
                for item in rows
            ):
                continue
            for item in rows:
                name = item.get("name") if isinstance(item, dict) else None
                if not isinstance(name, str) or not name or "/" in name:
                    continue
                if item.get("is_dir"):
                    stack.append(posixpath.join(current, name))
                elif posixpath.splitext(name)[1].casefold() in video_suffixes:
                    observed.add(name)
    return sorted(observed)


def _aggregate_replenishment_project_status(
    project_summaries: list[dict[str, Any]], *, unresolved_gap_count: int,
) -> str:
    """Aggregate only still-open title members; closed members do not distort it."""
    open_projects = [
        item for item in project_summaries
        if str(item.get("status") or "") != "no_regular_gaps"
    ]
    if not open_projects:
        return "no_regular_gaps" if unresolved_gap_count == 0 else "unresolved_gaps"
    statuses = Counter(str(item.get("status") or "") for item in open_projects)
    acquired = statuses.get("acquired", 0)
    if acquired == len(open_projects) and unresolved_gap_count == 0:
        return "acquired"
    if acquired:
        return "partial"
    if unresolved_gap_count:
        return "unresolved_gaps"
    if statuses.get("sources_exhausted", 0) == len(open_projects):
        return "sources_exhausted"
    if (
        statuses.get("no_match", 0) + statuses.get("sources_exhausted", 0)
        == len(open_projects)
    ):
        return "no_match"
    if len(statuses) == 1:
        return next(iter(statuses))
    return "failed"


def _load_interrupted_replenishment_selection(
    job: Job, *, suffix: str, request: Mapping[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]] | None:
    """Resume a sealed selection when shutdown preceded its acquisition receipt.

    Search evidence and provider counters may legitimately change while the API
    is down.  The immutable work identity is therefore the request owner,
    round, gaps, media target, and selection rules.  The prior selection must
    also reproduce exactly from its own candidate artifact and must not have a
    durable candidate-failure entry.  A missing acquisition artifact is the
    commit marker: once any adapter result exists, normal retry policy owns the
    next round instead of this restart-only path.
    """
    selection_path = job.directory / f"replenishment-selection{suffix}.json"
    candidates_path = job.directory / f"replenishment-candidates{suffix}.json"
    acquisition_path = job.directory / f"replenishment-acquisition{suffix}.json"
    if (
        acquisition_path.exists()
        or not selection_path.exists()
        or not candidates_path.exists()
    ):
        return None
    try:
        wrapper = load_json(selection_path)
        prior_request = wrapper.get("request")
        prior_selection = wrapper.get("selection")
        payload = load_json(candidates_path)
        candidates = payload.get("candidates")
        if (
            wrapper.get("version") != 2
            or not isinstance(prior_request, Mapping)
            or not isinstance(prior_selection, Mapping)
            or not isinstance(candidates, list)
            or not all(isinstance(item, Mapping) for item in candidates)
        ):
            return None
        identity_fields = ("job_id", "round", "gaps", "media", "rules")
        prior_identity = {
            field: prior_request.get(field) for field in identity_fields
        }
        current_identity = {field: request.get(field) for field in identity_fields}
        if canonical_digest(prior_identity) != canonical_digest(current_identity):
            return None
        rebuilt = select_replenishment_candidates(prior_request, candidates)
        if canonical_digest(rebuilt) != canonical_digest(prior_selection):
            return None
        selections = prior_selection.get("selections")
        if not isinstance(selections, list) or not selections or not all(
            isinstance(item, Mapping) and item.get("locator") for item in selections
        ):
            return None
        failed_locators = {
            (str(item.get("provider") or ""), str(item.get("locator") or ""))
            for item in _load_replenishment_failures(job)
            if isinstance(item, Mapping) and item.get("locator")
        }
        if any(
            (str(item.get("provider") or ""), str(item.get("locator") or ""))
            in failed_locators
            for item in selections
        ):
            return None
        return payload, dict(prior_selection)
    except (OSError, ValueError, json.JSONDecodeError):
        return None


def prepare_post_scrape_replenishment(
    job: Job,
    *,
    current_episode_gaps: list[dict[str, Any]] | None = None,
    scrape_gate_sha256: str | None = None,
) -> tuple[dict[str, Any], list[dict[str, Any]], dict[str, Any]]:
    """Load the signed media plan and run the shared replenishment core."""
    plan, _digest = unwrap_media_plan(load_json(job.directory / "media-plan.json"))
    return prepare_replenishment_from_plan(
        job, plan, current_episode_gaps=current_episode_gaps,
        scrape_gate_sha256=scrape_gate_sha256,
    )


def prepare_replenishment_from_plan(
    job: Job,
    plan: dict[str, Any],
    *,
    current_episode_gaps: list[dict[str, Any]] | None = None,
    request_job_id: str | None = None,
    round_number: int | None = None,
    scrape_gate_sha256: str | None = None,
) -> tuple[dict[str, Any], list[dict[str, Any]], dict[str, Any]]:
    """Search and materialize gaps from an explicit, already-validated plan.

    ``job`` remains the durable owner for replenishment requests, candidate
    evidence, acquisition receipts, logs and pause gates.  This core never
    reads or writes ``media-plan.json``; callers that do not originate from a
    committed scrape can therefore reuse replenishment without manufacturing a
    synthetic media plan or media journal.

    The adapter protocol is intentionally two-step:

    ``ADAPTER search --request REQUEST --output CANDIDATES``
        writes ``{"candidates": [...]}`` with provider, release name,
        optional file coverage, resolution, updated time and an opaque locator.

    ``ADAPTER acquire --selection SELECTION --output ACQUISITION``
        waits for the selected bundle to be present in the unscraped root and
        writes ``{"status": "ready", "source_paths": ["..."]}``.
    """
    effective_request_job_id = job.id if request_job_id is None else request_job_id
    if not isinstance(effective_request_job_id, str) or not effective_request_job_id.strip():
        raise ValueError("补源请求归属 ID 无效")
    effective_round_number = (
        job.replenishment_round + 1 if round_number is None else round_number
    )
    if type(effective_round_number) is not int or effective_round_number < 1:
        raise ValueError("补源轮次必须是正整数")
    if current_episode_gaps is None:
        # Compatibility for a committed job that predates title-closure
        # evidence.  The signed plan was produced before placement, so remove
        # only gaps proven covered by its own successfully committed files.
        plan, suppressed_planned_gaps = suppress_gaps_satisfied_by_planned_videos(plan)
        if suppressed_planned_gaps:
            append_log(
                job,
                f"已用本轮执行成功的视频计划扣除 "
                f"{len(suppressed_planned_gaps)} 个执行前过期缺口。",
            )
    else:
        if not all(
            isinstance(gap, dict)
            and gap.get("kind") in {"missing_episode", "missing_season"}
            for gap in current_episode_gaps
        ):
            raise ValueError("当前作品缺集证据格式无效")
        # The fresh, exact-title audit is authoritative after placement.  Do
        # not merge the plan's pre-execution observations back in or a closed
        # gap can be searched forever.  Work on a copy; the signed executable
        # plan remains immutable on disk.
        plan = deepcopy(plan)
        scan_report = dict(plan.get("scan_report") or {})
        scan_report["resource_gaps"] = deepcopy(current_episode_gaps)
        plan["scan_report"] = scan_report
    batch = build_replenishment_requests(
        plan,
        job_id=effective_request_job_id,
        round_number=effective_round_number,
    )
    requests = batch["requests"]
    unresolved_gaps = batch["unresolved_gaps"]
    _atomic_json(job.directory / "replenishment-requests.json", batch)
    if len(requests) == 1:
        _atomic_json(job.directory / "replenishment-request.json", requests[0])
    total_gaps = sum(len(request.get("gaps") or []) for request in requests)
    if not total_gaps and not unresolved_gaps:
        empty_request = {"round": effective_round_number, "gaps": []}
        return _replenishment_summary("no_regular_gaps", empty_request), [], plan
    max_rounds = replenishment_max_rounds()
    if max_rounds and effective_round_number > max_rounds:
        return {
            "status": "round_limit_reached", "round": effective_round_number,
            "gap_count": total_gaps + len(unresolved_gaps),
        }, [], plan
    if not auto_replenish_missing_enabled():
        return {
            "status": "detected", "round": effective_round_number,
            "gap_count": total_gaps + len(unresolved_gaps),
            "project_count": len(requests), "unresolved_gap_count": len(unresolved_gaps),
        }, [], plan

    adapter = replenishment_adapter_command()
    if not adapter:
        return {
            "status": "adapter_not_configured", "round": effective_round_number,
            "gap_count": total_gaps + len(unresolved_gaps),
            "project_count": len(requests), "unresolved_gap_count": len(unresolved_gaps),
        }, [], plan

    project_summaries: list[dict[str, Any]] = []
    followup_specs: list[dict[str, Any]] = []
    for index, request in enumerate(requests, start=1):
        _raise_if_replenishment_maintenance_stopped(job)
        suffix = "" if len(requests) == 1 else f"-{index:02d}"
        media = request.get("media") if isinstance(request.get("media"), dict) else {}
        target_root = media.get("target_root")
        if isinstance(target_root, str) and target_root:
            live_names = _stable_live_target_video_names(
                _execution_alist_client(), target_root,
            )
            request, suppressed_live_gaps = suppress_request_gaps_present_in_names(
                request, live_names,
            )
            if suppressed_live_gaps:
                append_log(
                    job,
                    f"补源搜索前强制刷新复核已发现 "
                    f"{len(suppressed_live_gaps)} 个目标集，不再搜索这些缺口。",
                )
            if not request.get("gaps"):
                requests[index - 1] = request
                project_summaries.append(_replenishment_summary(
                    "no_regular_gaps", request, media=media,
                ))
                continue
        rules = request.get("rules")
        if not isinstance(rules, dict):
            rules = {}
            request["rules"] = rules
        if all(
            isinstance(gap, dict) and gap.get("season") == 0
            for gap in request.get("gaps") or []
        ):
            rules["optional_discovery_only"] = True
            rules["season_zero_replenishment_required"] = True
        else:
            rules.pop("optional_discovery_only", None)
            rules.pop("season_zero_replenishment_required", None)
        rules["minimum_attempts_per_cloud_lane"] = replenishment_min_cloud_attempts()
        request["provider_attempts"] = _load_replenishment_provider_attempts(
            job, request,
        )
        request["provider_exhausted"] = _load_replenishment_provider_exhausted(
            job, request,
        )
        requests[index - 1] = request
        attempted_by_provider = {
            provider: _load_replenishment_attempted_locators(
                job, request, provider,
            )
            for provider in ("quark_share", "quark_magnet")
        }
        mismatch_by_provider = {
            provider: _load_replenishment_resource_mismatch_locators(
                job, request, provider,
            )
            for provider in ("quark_share", "quark_magnet")
        }
        request["excluded_candidates"] = (
            _load_replenishment_failures(job)
            + _load_replenishment_lane_suppressions(job)
            + [
                {
                    "provider": provider,
                    "locator": locator,
                    "failure_scope": "candidate",
                    "reason": (
                        "resource_inspected_without_requested_gap"
                        if locator in mismatch_by_provider[provider]
                        else "provider_scoped_candidate_rejection"
                    ),
                }
                for provider in ("quark_share", "quark_magnet")
                for locator in sorted(attempted_by_provider[provider])
            ]
        )
        request_path = job.directory / f"replenishment-request{suffix}.json"
        _atomic_json(request_path, request)
        candidates_path = job.directory / f"replenishment-candidates{suffix}.json"
        selection_path = job.directory / f"replenishment-selection{suffix}.json"
        acquisition_path = job.directory / f"replenishment-acquisition{suffix}.json"
        interrupted = _load_interrupted_replenishment_selection(
            job, suffix=suffix, request=request,
        )
        if interrupted is None:
            _raise_if_replenishment_maintenance_stopped(job)
            if scrape_gate_sha256 is not None:
                _revalidate_scrape_first_snapshot(job, scrape_gate_sha256)
            code, output = run_command(job, [
                *adapter, "search", "--request", str(request_path),
                "--output", str(candidates_path),
            ])
            _raise_if_replenishment_maintenance_stopped(job)
            if scrape_gate_sha256 is not None:
                _revalidate_scrape_first_snapshot(job, scrape_gate_sha256)
            if code != 0:
                append_log(
                    job,
                    "补源搜索基础设施/适配器未完成，本轮不计入一、二级有效尝试，"
                    "也不解锁本地 Torrent。",
                )
                project_summaries.append(_replenishment_summary(
                    "search_failed", request,
                    message=command_failure_reason(output, "自动查补搜索失败"),
                    media=request.get("media"),
                ))
                continue
            payload: dict[str, Any] | None = None
            resumed_selection: dict[str, Any] | None = None
        else:
            payload, resumed_selection = interrupted
            if scrape_gate_sha256 is not None:
                _revalidate_scrape_first_snapshot(job, scrape_gate_sha256)
            append_log(
                job,
                "恢复重启前已封存但尚未回执的补源 selection；"
                "跳过重复搜索并继续原 acquisition checkpoint。",
            )
        try:
            if payload is None:
                payload = load_json(candidates_path)
            candidates = payload.get("candidates")
            if not isinstance(candidates, list) or not all(isinstance(item, dict) for item in candidates):
                raise ValueError("候选输出必须包含 candidates 对象数组")
            lane_status = payload.get("lane_status")
            if not isinstance(lane_status, Mapping):
                lane_status = {}
            for provider in ("quark_share", "quark_magnet"):
                status = lane_status.get(provider)
                if isinstance(status, Mapping) and status.get("status") == "exhausted":
                    proof = status.get("proof")
                    if isinstance(proof, Mapping):
                        request["provider_exhausted"] = (
                            _record_replenishment_provider_exhausted(
                                job,
                                request,
                                provider,
                                reason=str(
                                    status.get("reason")
                                    or "configured_sources_exhausted"
                                ),
                                proof=proof,
                            )
                        )
                    else:
                        append_log(
                            job,
                            f"{provider} 声称穷尽但未提供结构化证明；"
                            "忽略该标记并继续锁定后续层级。",
                        )
            selection_bundle = (
                resumed_selection
                if resumed_selection is not None
                else select_replenishment_candidates(request, candidates)
            )
        except (OSError, ValueError, json.JSONDecodeError) as exc:
            project_summaries.append(_replenishment_summary(
                "invalid_candidates", request, message=redact(str(exc)),
                media=request.get("media"),
            ))
            continue
        selections = selection_bundle["selections"]
        stats = {
            "candidate_count": selection_bundle.get("candidate_count", 0),
            "eligible_candidate_count": selection_bundle.get("eligible_candidate_count", 0),
            "rejection_reasons": selection_bundle.get("rejection_reasons", {}),
            "provider_diagnostics": selection_bundle.get("provider_diagnostics", {}),
            "provider_attempts": selection_bundle.get("provider_attempts", {}),
            "lane_status": dict(lane_status),
            "minimum_attempts_per_cloud_lane": selection_bundle.get(
                "minimum_attempts_per_cloud_lane", 0,
            ),
            "covered_gap_count": len(selection_bundle.get("covered_gap_ids") or []),
            "uncovered_gap_count": len(selection_bundle.get("uncovered_gap_ids") or []),
        }
        share_audit = stats["provider_diagnostics"].get("quark_share", {})
        append_log(
            job,
            "一级分享候选审计: "
            f"发现 {int(share_audit.get('candidate_count') or 0)} 个，"
            f"可执行 {int(share_audit.get('eligible_candidate_count') or 0)} 个，"
            f"拒绝原因 {share_audit.get('rejection_reasons') or {}}。",
        )
        offline_audit = stats["provider_diagnostics"].get("quark_magnet", {})
        append_log(
            job,
            "二级云离线候选审计: "
            f"发现 {int(offline_audit.get('candidate_count') or 0)} 个，"
            f"可执行 {int(offline_audit.get('eligible_candidate_count') or 0)} 个，"
            f"拒绝原因 {offline_audit.get('rejection_reasons') or {}}。",
        )
        if not selections:
            newly_recorded_by_provider: dict[str, list[str]] = {}
            deterministic_rejections = selection_bundle.get(
                "durably_rejected_candidates",
            )
            rejected_by_provider: dict[str, list[str]] = defaultdict(list)
            for row in deterministic_rejections or []:
                if not isinstance(row, Mapping):
                    continue
                provider = str(row.get("provider") or "")
                locator = str(row.get("locator") or "")
                if provider in {"quark_share", "quark_magnet"} and locator:
                    rejected_by_provider[provider].append(locator)
            for provider, locators in rejected_by_provider.items():
                attempted = _load_replenishment_attempted_locators(
                    job, request, provider,
                )
                novel = sorted(set(locators) - attempted)
                if not novel:
                    continue
                attempts = _record_replenishment_provider_attempts(
                    job,
                    request,
                    provider,
                    count=len(novel),
                    reason="selector_deterministic_candidate_rejection",
                    locators=novel,
                )
                newly_recorded_by_provider[provider] = novel
                request["provider_attempts"] = attempts
                request["provider_exhausted"] = (
                    _load_replenishment_provider_exhausted(job, request)
                )
                stats["provider_attempts"] = attempts
                append_log(
                    job,
                    f"{provider} 已持久隔离 {len(novel)} 个确定性拒绝候选；"
                    "下一轮将在搜索结果上限前按 locator/infoHash 排除。",
                )
            for provider, discovery_key, locator_prefix, reason in (
                (
                    "quark_share", "share_discovery", "quark_share:",
                    "distinct_share_resource_failure",
                ),
                (
                    "quark_magnet", "magnet_discovery", "quark_magnet:",
                    "distinct_cloud_offline_resource_mismatch",
                ),
            ):
                discovery = payload.get(discovery_key)
                discovered_failures = (
                    discovery.get("resource_failed_locators")
                    if isinstance(discovery, Mapping) else []
                )
                attempted = _load_replenishment_attempted_locators(
                    job, request, provider,
                )
                locators = sorted({
                    str(value) for value in discovered_failures or []
                    if value
                    and str(value).startswith(locator_prefix)
                    and str(value) not in attempted
                })
                if not locators:
                    continue
                attempts = _record_replenishment_provider_attempts(
                    job,
                    request,
                    provider,
                    count=len(locators),
                    reason=reason,
                    locators=locators,
                )
                newly_recorded_by_provider[provider] = locators
                request["provider_attempts"] = attempts
                request["provider_exhausted"] = (
                    _load_replenishment_provider_exhausted(job, request)
                )
                stats["provider_attempts"] = attempts
                append_log(
                    job,
                    f"{provider} 已入账 {len(locators)} 个不同的"
                    "资源级失败证据；"
                    f"当前进度 {attempts[provider]}/"
                    f"{stats['minimum_attempts_per_cloud_lane']}。",
                )
            required_provider = selection_bundle.get("required_attempt_provider")
            infrastructure_blocked = bool(
                selection_bundle.get("required_attempt_blocked_by_infrastructure")
            )
            required_lane_status = (
                lane_status.get(str(required_provider))
                if isinstance(lane_status, Mapping) else None
            )
            if (
                isinstance(required_lane_status, Mapping)
                and required_lane_status.get("status") == "infrastructure_failure"
            ):
                infrastructure_blocked = True
            if required_provider in {"quark_share", "quark_magnet"}:
                if infrastructure_blocked:
                    append_log(
                        job,
                        f"{required_provider} 本轮同时存在基础设施故障；"
                        "已获得的不同 locator 证据保留，"
                        "未完成的搜索部分不计次，三级仍按门禁判定。",
                    )
                elif required_provider not in newly_recorded_by_provider:
                    # An empty scheduler round is not a resource attempt.
                    # Complete-search exhaustion is persisted separately.
                    append_log(
                        job,
                        f"{required_provider} 本轮无新的候选资源证据；"
                        "不增加有效尝试数，不提前推进后续层级。",
                    )
            source_exhaustion = _all_required_sources_exhausted(
                lane_status,
                remaining_candidate_count=int(
                    selection_bundle.get("candidate_count") or 0
                ),
                local_torrent_unlocked=(
                    selection_bundle.get("local_torrent_unlocked") is True
                ),
            )
            project_status = (
                "sources_exhausted" if source_exhaustion is not None else "no_match"
            )
            project_summaries.append(_replenishment_summary(
                project_status,
                request,
                media=request.get("media"),
                **({
                    "source_exhaustion": source_exhaustion,
                    "message": (
                        "已接入的必需来源完整搜索均无候选；附加来源存在不可用或"
                        "未完整响应，三级目录也无剩余候选"
                        if source_exhaustion.get("incomplete_optional_sources")
                        else "已接入的必需来源完整搜索均无候选，三级目录也无剩余候选"
                    ),
                } if source_exhaustion is not None else {}),
                **stats,
            ))
            continue
        selection_wrapper = {
            "version": 2, "request": request, "selection": selection_bundle,
        }
        _atomic_json(selection_path, selection_wrapper)
        selection_sha256 = canonical_digest(selection_wrapper)
        covered_count = len(selection_bundle.get("covered_gap_ids") or [])
        update_job(job, progress={
            "stage": "replenishment_acquire",
            "completed": 0,
            "total": max(covered_count, 1),
            "percent": 96.0,
            "message": f"已选定 {len(selections)} 个来源，正在获取 {covered_count} 个缺项并核验到盘",
        })
        reuse_acquisition = False
        if acquisition_path.exists():
            try:
                # A service restart can occur after the adapter committed its
                # ready artifact but before the follow-up job was created.
                # Reuse only a receipt cryptographically bound to the selection
                # rebuilt above.  Visibility alone cannot prove that a stale
                # fixed-path receipt belongs to the current provider bundle.
                acquisition_checkpoint = load_json(acquisition_path)
                reuse_acquisition = (
                    acquisition_checkpoint.get("selection_sha256") == selection_sha256
                    and bool(validate_acquisition_results(acquisition_checkpoint))
                )
            except (OSError, ValueError, json.JSONDecodeError):
                reuse_acquisition = False
        if reuse_acquisition:
            append_log(job, "复用重启前已提交的补源到盘工件；继续可见性核验。")
            code, output = 0, ""
        else:
            acquisition_path.unlink(missing_ok=True)
            _raise_if_replenishment_maintenance_stopped(job)
            # Serialize the acquisition start with the global pause
            # transition.  If acquisition is already in flight, pause waits
            # for that bounded stage to finish; once pause returns, no new
            # write-capable adapter stage can have started behind it.
            code, output = _run_replenishment_mutation_stage(
                job, lambda: run_command(job, [
                    *adapter, "acquire", "--selection", str(selection_path),
                    "--output", str(acquisition_path),
                ]), scrape_gate_sha256=scrape_gate_sha256,
            )
        _raise_if_replenishment_maintenance_stopped(job)
        public_selections = [{
            "provider": selection.get("provider"), "release_name": selection.get("release_name"),
            "resolution": selection.get("resolution"), "updated_at": selection.get("updated_at"),
            "selected_gap_ids": list(selection.get("selected_gap_ids") or []),
        } for selection in selections]
        if code != 0:
            reason = command_failure_reason(output, "自动查补获取失败")
            failure: dict[str, Any] = {}
            try:
                failed_payload = load_json(acquisition_path)
                _record_replenishment_lane_suppressions(
                    job, failed_payload.get("lane_suppressions"),
                )
                raw_failure = failed_payload.get("failure")
                if failed_payload.get("status") == "failed" and isinstance(raw_failure, dict):
                    failure = dict(raw_failure)
            except (OSError, ValueError, json.JSONDecodeError):
                pass
            structured_scope = failure.get("scope")
            logged_scope_match = re.search(
                r"\[replenishment\]\s*failure_scope="
                r"(candidate|infrastructure|delivery)\b",
                output,
                re.I,
            )
            failure_scope = (
                structured_scope if structured_scope in {
                    "candidate", "infrastructure", "delivery",
                }
                else logged_scope_match.group(1).casefold()
                if logged_scope_match else "candidate"
            )
            reusable_candidate = bool(
                failure.get("reusable_candidate", failure_scope == "delivery")
            )
            exclude_candidate = bool(
                failure.get("exclude_candidate", failure_scope == "candidate")
            )
            failed_selections = _record_replenishment_failures(
                job, [dict(item) for item in selections], output, reason,
                failure_scope=failure_scope if exclude_candidate else "infrastructure",
                failed_candidate=(
                    failure.get("candidate")
                    if isinstance(failure.get("candidate"), dict) else None
                ),
            )
            if failed_selections:
                append_log(job, "失败候选已写入隔离名单；下一轮搜索将跳过该 locator/infohash。")
                failed_by_provider: dict[str, list[str]] = defaultdict(list)
                for failed_selection in failed_selections:
                    provider = str(failed_selection.get("provider") or "")
                    if provider in {"quark_share", "quark_magnet"}:
                        failed_by_provider[provider].append(
                            str(failed_selection.get("locator") or "")
                        )
                for provider, locators in failed_by_provider.items():
                    unique_locators = sorted({value for value in locators if value})
                    attempts = _record_replenishment_provider_attempts(
                        job,
                        request,
                        provider,
                        count=max(1, len(unique_locators)),
                        reason="candidate_resource_failure",
                        locators=unique_locators,
                    )
                    request["provider_attempts"] = attempts
                    stats["provider_attempts"] = attempts
                    append_log(
                        job,
                        f"{provider} 资源级失败已计入 "
                        f"{max(1, len(unique_locators))} 次；当前进度 "
                        f"{attempts[provider]}/{stats['minimum_attempts_per_cloud_lane']}。",
                    )
            elif failure_scope == "delivery":
                append_log(job, "下载候选已验证；仅上传交付失败，保留候选和暂存文件供原地重试。")
            elif failure_scope == "infrastructure":
                append_log(job, "本轮为环境或编排失败；没有直接候选失效证据，不写入 locator/infohash 隔离。")
            project_summaries.append(_replenishment_summary(
                "acquire_failed", request, selections=public_selections,
                selection=public_selections[0],
                message=reason,
                failure_scope=failure_scope,
                failure_stage=failure.get("stage"),
                reusable_candidate=reusable_candidate,
                exclude_candidate=exclude_candidate,
                retained_workspace=(
                    failure.get("workspace")
                    if reusable_candidate or failure.get("retained_workspace") is True
                    else None
                ),
                workspace_key=failure.get("workspace_key") if reusable_candidate else None,
                media=request.get("media"), **stats,
            ))
            continue
        try:
            acquisition_payload = load_json(acquisition_path)
            receipt_selection_sha256 = acquisition_payload.get("selection_sha256")
            if (
                receipt_selection_sha256 is not None
                and receipt_selection_sha256 != selection_sha256
            ):
                raise ValueError("查补到盘工件与当前 selection 摘要不一致")
            if acquisition_payload.get("status") == "ready" and receipt_selection_sha256 is None:
                acquisition_payload = dict(
                    acquisition_payload, selection_sha256=selection_sha256,
                )
                _atomic_json(acquisition_path, acquisition_payload)
            _record_replenishment_lane_suppressions(
                job, acquisition_payload.get("lane_suppressions"),
            )
            sources = [
                replenishment_source_path(source)
                for source in validate_acquisition_results(acquisition_payload)
            ]
            client = _execution_alist_client()
            for source in sources:
                rows = client.list(source, refresh=True)
                if not isinstance(rows, list) or not rows:
                    raise ValueError(f"查补目录为空: {source}")
        except (OSError, ValueError, json.JSONDecodeError) as exc:
            project_summaries.append(_replenishment_summary(
                "invalid_acquisition", request, selections=public_selections,
                selection=public_selections[0], message=redact(str(exc)),
                media=request.get("media"), **stats,
            ))
            continue
        followup_specs.extend({"source": source, "media": request.get("media")} for source in sources)
        project_summaries.append(_replenishment_summary(
            "acquired", request, selections=public_selections,
            selection=public_selections[0], media=request.get("media"), **stats,
        ))

    statuses = Counter(str(item.get("status")) for item in project_summaries)
    public_selections = [
        selection for item in project_summaries for selection in item.get("selections", [])
    ]
    aggregate_status = _aggregate_replenishment_project_status(
        project_summaries, unresolved_gap_count=len(unresolved_gaps),
    )
    rejection_reasons: Counter[str] = Counter()
    for item in project_summaries:
        raw_reasons = item.get("rejection_reasons")
        if isinstance(raw_reasons, dict):
            rejection_reasons.update({str(key): int(value) for key, value in raw_reasons.items()})
    remaining_gap_count = sum(len(request.get("gaps") or []) for request in requests)
    return {
        "status": aggregate_status,
        "round": effective_round_number,
        "gap_count": remaining_gap_count + len(unresolved_gaps),
        "project_count": len(requests), "projects": project_summaries,
        "unresolved_gap_count": len(unresolved_gaps),
        "project_status_counts": dict(statuses),
        "selections": public_selections,
        "selection": public_selections[0] if public_selections else None,
        "candidate_count": sum(int(item.get("candidate_count") or 0) for item in project_summaries),
        "eligible_candidate_count": sum(int(item.get("eligible_candidate_count") or 0) for item in project_summaries),
        "rejection_reasons": dict(rejection_reasons),
        "covered_gap_count": sum(int(item.get("covered_gap_count") or 0) for item in project_summaries),
        "uncovered_gap_count": len(unresolved_gaps) + sum(
            int(item.get("uncovered_gap_count") or 0) for item in project_summaries
        ),
    }, followup_specs, plan


def _replenishment_followup_closed(job: Job) -> bool:
    """Return whether a child has evidence for scrape + post-scrape audit."""
    summary = job.plan_summary if isinstance(job.plan_summary, dict) else {}
    consumed = summary.get("replenishment_followup")
    if (
        job.phase == "cancelled"
        and job.visibility == "internal"
        and is_replenishment_system_source(job.source)
        and isinstance(consumed, dict)
        and consumed.get("status") == "source_already_consumed"
        and consumed.get("root_job_id") == job.root_job_id
    ):
        return True
    if job.phase != "completed":
        return False
    # Exact internal children are bounded to the parent's proven gap set.  A
    # successful media journal is their delivery audit; recursively searching
    # unrelated optional specials would keep the parent open forever.
    if is_internal_replenishment_followup(job) and media_journal_succeeded(job):
        return True
    if not isinstance(job.plan_summary, dict):
        return False
    replenishment = job.plan_summary.get("replenishment")
    if isinstance(replenishment, dict):
        status = str(replenishment.get("status") or "")
        if status == "no_regular_gaps":
            return True
        if status == "acquired":
            return replenishment.get("followup_verified") is True
        return False
    # A proven empty-source residue is an idempotent completion: another task
    # already organized the files and the no-op proof records zero gaps.
    return (
        job.plan_summary.get("kind") == "noop"
        and job.plan_summary.get("resource_gap_count") == 0
    )


_REPLENISHMENT_VIDEO_SUFFIXES = frozenset({
    ".mkv", ".mp4", ".m4v", ".m2ts", ".ts", ".avi", ".wmv", ".mov",
    ".webm", ".flv", ".mpeg", ".mpg", ".rmvb", ".strm",
})
_SYSTEM_REPLENISHMENT_ROOT = f"{MEDIA_LIBRARY_ROOT}/ScrapeFlow/补源"


def replenishment_source_path(value: Any) -> str:
    """Accept a concrete legacy inbox or the dedicated system source tree."""
    normalized = media_library_path(value, allow_root=False)
    if is_replenishment_system_source(normalized):
        return normalized
    return unscraped_media_path(normalized)


def _delivered_replenishment_owner(job: Job, owner: Job | None = None) -> Job | None:
    """Return the linked root only after it records completion or delivery."""
    if (
        job.visibility != "internal"
        or not job.root_job_id
        or not is_replenishment_system_source(job.source)
    ):
        return None
    candidate = owner or JOBS.get(job.root_job_id)
    if candidate is None or candidate.id != job.root_job_id:
        return None
    if candidate.phase == "completed":
        return candidate
    summary = candidate.plan_summary if isinstance(candidate.plan_summary, dict) else {}
    replenishment = summary.get("replenishment")
    if not isinstance(replenishment, dict):
        return None
    status = str(replenishment.get("status") or "")
    followup_ids = replenishment.get("followup_job_ids")
    source_paths = replenishment.get("source_paths")
    if (
        status in {"acquired", "partial", "followup_partial"}
        and isinstance(followup_ids, list)
        and job.id in followup_ids
        and isinstance(source_paths, list)
        and job.source in source_paths
    ):
        return candidate
    return None


def _replenishment_source_is_missing(source: str) -> bool:
    """Prove absence with a refreshed listing; connectivity errors fail closed."""
    try:
        client = _execution_alist_client()
        try_list = getattr(client, "try_list", None)
        if callable(try_list):
            return try_list(source, refresh=True) is None
        client.list(source, refresh=True)
        return False
    except ApiError as exc:
        message = str(exc).casefold()
        return any(marker in message for marker in (
            "not found", "no such file", "not exist", "object does not exist",
            "不存在", "未找到",
        ))
    except (OSError, ValueError):
        return False


def close_consumed_internal_replenishment_followup(
    job: Job, *, owner: Job | None = None,
) -> bool:
    """Close a hidden child whose delivered staging source was already consumed.

    This is deliberately a cancellation/no-op rather than claiming that this
    child executed media writes.  The exact root linkage plus its durable
    delivery state and a refreshed missing-source proof prevent a vanished
    staging directory from triggering unbounded acquisition rounds.
    """
    if (
        job.phase not in {"queued", "planning_archives", "planning_media", "failed"}
        or (job.directory / "media-journal.json").exists()
    ):
        return False
    delivered_by = _delivered_replenishment_owner(job, owner)
    if delivered_by is None or not _replenishment_source_is_missing(job.source):
        return False
    with LOCK:
        summary = dict(job.plan_summary or {})
        summary["replenishment_followup"] = {
            "status": "source_already_consumed",
            "root_job_id": delivered_by.id,
            "source": job.source,
        }
        summary.setdefault("kind", "internal_replenishment_noop")
        summary.setdefault("resource_gap_count", 0)
        job.phase = "cancelled"
        job.error = None
        job.digest = None
        job.plan_summary = summary
        job.progress = {
            "stage": "replenishment_source_consumed",
            "completed": 1,
            "total": 1,
            "percent": 100.0,
            "message": "补源暂存目录已被已交付根任务消费，内部任务已自动收口",
        }
        job.updated_at = utc_now()
        persist_job(job)
    append_log(
        job,
        f"补源暂存目录已不存在；根任务 {delivered_by.id} 已完成或记录到盘交付，"
        "本内部任务按已消费终态收口，不再启动新一轮补源。",
    )
    return True


def _replenishment_artifact_pairs(job: Job) -> list[tuple[Path, Path]]:
    pairs: list[tuple[Path, Path]] = []
    for selection_path in sorted(job.directory.glob("replenishment-selection*.json")):
        suffix = selection_path.stem.removeprefix("replenishment-selection")
        acquisition_path = job.directory / f"replenishment-acquisition{suffix}.json"
        if acquisition_path.is_file():
            pairs.append((selection_path, acquisition_path))
    return pairs


def _replenishment_source_video_snapshot(source: str) -> list[dict[str, Any]]:
    """Read one follow-up source without accepting basename-only evidence."""
    client = _execution_alist_client()
    stack = [replenishment_source_path(source)]
    visited: set[str] = set()
    videos: list[dict[str, Any]] = []
    while stack:
        current = stack.pop()
        if current in visited or len(visited) >= 10_000:
            raise ValueError("补源精确归因超过 10000 个目录")
        visited.add(current)
        rows = client.list(current, refresh=True)
        if not isinstance(rows, list):
            raise ValueError(f"补源精确归因返回无效目录: {current}")
        for item in rows:
            name = item.get("name") if isinstance(item, dict) else None
            if not isinstance(name, str) or not name or "/" in name or name in {".", ".."}:
                raise ValueError("补源精确归因包含无效路径项")
            path = posixpath.join(current, name)
            if item.get("is_dir"):
                stack.append(path)
                continue
            if posixpath.splitext(name)[1].casefold() not in _REPLENISHMENT_VIDEO_SUFFIXES:
                continue
            size = item.get("size")
            if type(size) is not int or size <= 0:
                raise ValueError(f"补源视频缺少精确大小: {path}")
            videos.append({"path": path, "size": size})
    if not videos:
        raise ValueError("补源目录中没有可归因的视频")
    return sorted(videos, key=lambda item: str(item["path"]).casefold())


def _replenishment_expected_path_matches(actual: str, expected: str) -> bool:
    """Match exact paths plus Quark's narrow long-basename truncation form."""
    if actual == expected or actual.endswith("/" + expected):
        return True
    actual_name = posixpath.basename(actual)
    expected_name = posixpath.basename(expected)
    actual_stem, actual_ext = posixpath.splitext(actual_name)
    expected_stem, expected_ext = posixpath.splitext(expected_name)
    if (
        actual_ext.casefold() != expected_ext.casefold()
        or not actual_stem.endswith("...")
    ):
        return False
    preserved = actual_stem[:-3]
    if (
        len(preserved.encode("utf-8")) < 96
        or not expected_stem.startswith(preserved)
        or len(expected_stem) <= len(preserved)
    ):
        return False
    expected_parent = posixpath.dirname(expected)
    if not expected_parent:
        return True
    actual_parent = posixpath.dirname(actual)
    return actual_parent == expected_parent or actual_parent.endswith(
        "/" + expected_parent,
    )


def _selection_episode_evidence(
    owner: Job,
    source: str,
    selection_path: Path,
    acquisition_path: Path,
    actual_videos: list[dict[str, Any]],
    tmdb_id: int,
) -> dict[str, Any] | None:
    """Prove a path+size+gap one-to-one map for one acquisition receipt."""
    try:
        acquisition = load_json(acquisition_path)
        source_paths = [
            replenishment_source_path(path)
            for path in validate_acquisition_results(acquisition)
        ]
        normalized_source = replenishment_source_path(source)
        if normalized_source not in source_paths:
            return None
        wrapper = load_json(selection_path)
    except (OSError, ValueError, json.JSONDecodeError):
        return None
    request = wrapper.get("request")
    selection = wrapper.get("selection")
    if not isinstance(request, dict) or not isinstance(selection, dict):
        return None
    media = request.get("media")
    if (
        not isinstance(media, dict)
        or type(media.get("tmdb_id")) is not int
        or int(media["tmdb_id"]) != tmdb_id
    ):
        return None
    gap_targets: dict[str, tuple[int, int]] = {}
    for gap in request.get("gaps") or []:
        if not isinstance(gap, dict) or gap.get("kind") != "missing_episode":
            continue
        raw_gap_id = gap.get("id")
        gap_id = str(raw_gap_id or "")
        match = re.fullmatch(r"S0*(\d{1,3})E0*(\d{1,4})", gap_id, re.I)
        episodes = gap.get("episodes")
        season = gap.get("season")
        if (
            match is None
            or type(season) is not int
            or not isinstance(episodes, list)
            or len(episodes) != 1
            or type(episodes[0]) is not int
            or season != int(match.group(1))
            or episodes[0] != int(match.group(2))
            or gap_id in gap_targets
        ):
            return None
        gap_targets[gap_id] = (season, episodes[0])
    if not gap_targets:
        return None
    expected: list[dict[str, Any]] = []
    selections = selection.get("selections")
    if not isinstance(selections, list):
        return None
    for selected in selections:
        acquisition_spec = selected.get("acquisition") if isinstance(selected, dict) else None
        rows = acquisition_spec.get("expected_files") if isinstance(acquisition_spec, dict) else None
        if not isinstance(rows, list):
            return None
        for row in rows:
            if not isinstance(row, dict):
                return None
            raw_path = row.get("path")
            if (
                not isinstance(raw_path, str)
                or raw_path.startswith(("/", "\\"))
            ):
                continue
            path = raw_path.replace("\\", "/").strip("/")
            size = row.get("size")
            gaps = row.get("gap_ids")
            if (
                not path
                or posixpath.normpath(path) != path
                or path.startswith("../")
                or type(size) is not int
                or size <= 0
                or not isinstance(gaps, list)
                or len(gaps) != 1
                or gaps[0] not in gap_targets
            ):
                continue
            expected.append({"path": path, "size": size, "gap_id": str(gaps[0])})
    if not expected:
        return None
    matched_rows: list[dict[str, Any]] = []
    used_expected: set[int] = set()
    used_gaps: set[str] = set()
    episode_map: dict[str, str] = {}
    try:
        from engine.scraper import extract_episode_key  # pylint: disable=import-outside-toplevel
    except ImportError:
        return None
    prefix = normalized_source.rstrip("/") + "/"
    for actual in actual_videos:
        actual_path = str(actual["path"])
        if not actual_path.startswith(prefix):
            return None
        relative = actual_path[len(prefix):]
        candidates = [
            (index, row) for index, row in enumerate(expected)
            if index not in used_expected
            and int(actual["size"]) == int(row["size"])
            and _replenishment_expected_path_matches(relative, str(row["path"]))
        ]
        if len(candidates) != 1:
            return None
        index, expected_row = candidates[0]
        key = extract_episode_key(posixpath.basename(actual_path))
        if (
            key is None or key.kind not in {"regular", "special"} or key.end_number
            or key.fractional_digits or key.number <= 0
        ):
            return None
        source_key = f"{'SP' if key.kind == 'special' else 'E'}{key.number}"
        gap_id = str(expected_row["gap_id"])
        if source_key in episode_map or gap_id in used_gaps:
            return None
        used_expected.add(index)
        used_gaps.add(gap_id)
        episode_map[source_key] = gap_id
        matched_rows.append({
            "source_path": actual_path,
            "expected_path": expected_row["path"],
            "size": int(actual["size"]),
            "source_episode": source_key,
            "target_episode": gap_id,
        })
    if len(matched_rows) != len(actual_videos):
        return None
    target_seasons = {
        gap_targets[row["target_episode"]][0] for row in matched_rows
    }
    return {
        "version": 1,
        "kind": "replenishment_exact_episode_map",
        "owner_job_id": owner.id,
        "source": normalized_source,
        "tmdb_id": tmdb_id,
        "season": next(iter(target_seasons)) if len(target_seasons) == 1 else None,
        "episode_map": dict(sorted(episode_map.items())),
        "files": sorted(matched_rows, key=lambda row: str(row["source_path"]).casefold()),
        "selection_artifact": selection_path.name,
        "selection_sha256": canonical_digest(wrapper),
        "acquisition_artifact": acquisition_path.name,
        "acquisition_sha256": canonical_digest(acquisition),
    }


def _replenishment_followup_episode_evidence(
    preferred_owner: Job,
    source: str,
    tmdb_id: int | None,
) -> tuple[Job, dict[str, Any]] | None:
    if type(tmdb_id) is not int or tmdb_id <= 0:
        return None
    lineage = preferred_owner.root_job_id or preferred_owner.id
    owners = [preferred_owner, *(
        job for job in JOBS.values()
        if job.id != preferred_owner.id
        and (job.root_job_id or job.id) == lineage
    )]
    artifact_pairs = [
        (owner, selection_path, acquisition_path)
        for owner in owners
        for selection_path, acquisition_path in _replenishment_artifact_pairs(owner)
    ]
    # Exact inheritance is an optional optimization.  A normal follow-up may
    # have no acquisition receipt (older adapters and hand-created fixtures),
    # so do not touch AList unless there is a complete artifact pair to prove.
    if not artifact_pairs:
        return None
    try:
        actual_videos = _replenishment_source_video_snapshot(source)
    except (OSError, ValueError, ApiError):
        # A missing/temporarily invisible source cannot establish exact
        # evidence, but it must not prevent the ordinary planner from reporting
        # the source through its existing error/retry path.
        return None
    proven: list[tuple[Job, dict[str, Any]]] = []
    for owner, selection_path, acquisition_path in artifact_pairs:
        evidence = _selection_episode_evidence(
            owner, source, selection_path, acquisition_path,
            actual_videos, tmdb_id,
        )
        if evidence is not None:
            proven.append((owner, evidence))
    return proven[0] if len(proven) == 1 else None


def _persist_replenishment_episode_evidence(
    followup: Job, evidence: dict[str, Any],
) -> None:
    followup.episode_map = dict(evidence["episode_map"])
    followup.season = evidence.get("season") if type(evidence.get("season")) is int else None
    _atomic_json(followup.directory / "replenishment-episode-evidence.json", evidence)
    persist_job(followup)


def _link_replenishment_followup_owner(
    owner: Job, followup: Job, evidence: dict[str, Any],
) -> None:
    """Restore the durable parent/child monitor edge from exact artifacts."""
    if owner.id == followup.id:
        return
    with LOCK:
        summary = dict(owner.plan_summary or {})
        replenishment = dict(summary.get("replenishment") or {})
        followup_ids = [
            value for value in (replenishment.get("followup_job_ids") or [])
            if isinstance(value, str) and re.fullmatch(r"[0-9a-f]{12}", value)
        ]
        if followup.id not in followup_ids:
            followup_ids.append(followup.id)
        source_paths = [
            value for value in (replenishment.get("source_paths") or [])
            if isinstance(value, str)
        ]
        if followup.source not in source_paths:
            source_paths.append(followup.source)
        replenishment.update({
            "status": "acquired",
            "source_paths": source_paths,
            "followup_job_ids": followup_ids,
            "followup_job_id": followup_ids[0],
            "followup_verified": False,
            "episode_evidence_owner_job_id": evidence["owner_job_id"],
        })
        summary["replenishment"] = replenishment
        if owner.phase in {"failed", "replenishing"}:
            owner.phase = "replenishing"
            owner.error = None
        owner.plan_summary = summary
        owner.progress = {
            "stage": "replenishment_followup",
            "completed": 0,
            "total": len(followup_ids),
            "percent": 97.0,
            "message": "补源文件已精确归因，正在等待内部整理与再次审计",
        }
        owner.updated_at = utc_now()
        persist_job(owner)


def _inherit_replenishment_followup_evidence(
    followup: Job, *, launch_monitor: bool = False,
) -> tuple[Job, dict[str, Any]] | None:
    if (
        followup.media_type != "tv"
        or not followup.root_job_id
        or type(followup.tmdb_id) is not int
        or followup.tmdb_id <= 0
    ):
        return None
    proven = _replenishment_followup_episode_evidence(
        followup, followup.source, followup.tmdb_id,
    )
    if proven is None:
        return None
    owner, evidence = proven
    if not followup.episode_map:
        _persist_replenishment_episode_evidence(followup, evidence)
    followup.visibility = "internal"
    persist_job(followup)
    _link_replenishment_followup_owner(owner, followup, evidence)
    if launch_monitor:
        replenishment = (
            owner.plan_summary.get("replenishment")
            if isinstance(owner.plan_summary, dict) else None
        )
        followup_ids = (
            replenishment.get("followup_job_ids")
            if isinstance(replenishment, dict) else None
        )
        if isinstance(followup_ids, list) and followup_ids:
            _launch_replenishment_followup_monitor(owner, list(followup_ids))
    return owner, evidence


def _repair_failed_replenishment_followup(followup: Job) -> bool:
    """Requeue a failed internal child only after exact evidence is inherited."""
    if (
        followup.phase != "failed"
        or str((followup.progress or {}).get("stage") or "") != "planning_start"
        or (followup.directory / "media-journal.json").exists()
    ):
        return False
    inherited = _inherit_replenishment_followup_evidence(followup)
    if inherited is None:
        return False
    with LOCK:
        followup.phase = "queued"
        followup.error = None
        followup.digest = None
        followup.plan_summary = None
        followup.progress = {
            "stage": "planning_retry", "completed": 0, "total": 1,
            "percent": 1.0,
            "message": "已恢复补源精确集号证据，正在重新规划内部任务",
        }
        followup.updated_at = utc_now()
        persist_job(followup)
    append_log(
        followup,
        "已从父任务 selection/acquisition 与实盘路径、大小恢复"
        "一一集号映射；未重新搜索整个多季目录。",
    )
    return True


def create_replenishment_followup(
    previous: Job,
    source: str,
    plan: dict[str, Any],
    media: dict[str, Any] | None = None,
) -> Job:
    """Create a normal scrape job for files materialized by the adapter."""
    require_nonlegacy_job_mutation(previous)
    metadata = media or (plan.get("metadata") if isinstance(plan.get("metadata"), dict) else {})
    mode = "tv" if media else str(plan.get("mode") or previous.media_type)
    media_type = mode if mode in {"tv", "movie", "collection"} else previous.media_type
    tmdb_id = metadata.get("tmdb_id")
    if type(tmdb_id) is not int or tmdb_id <= 0 or media_type == "auto":
        tmdb_id = None
    proven_episode = _replenishment_followup_episode_evidence(
        previous, source, tmdb_id,
    ) if media_type == "tv" else None
    episode_evidence = proven_episode[1] if proven_episode is not None else None
    followup_parent = previous.parent
    target_root = metadata.get("target_root")
    if isinstance(target_root, str) and target_root.strip():
        normalized_target = normalize_remote_input(target_root)
        candidate_parent = posixpath.dirname(normalized_target)
        normalized_previous_parent = normalize_remote_input(previous.parent)
        if (
            candidate_parent == normalized_previous_parent
            or candidate_parent.startswith(normalized_previous_parent.rstrip("/") + "/")
        ):
            followup_parent = candidate_parent
    followup = Job(
        id=uuid.uuid4().hex[:12],
        source=replenishment_source_path(source),
        parent=followup_parent,
        media_type=media_type,
        absolute=previous.absolute,
        prefer_simplified=previous.prefer_simplified,
        tmdb_id=tmdb_id,
        query=None if tmdb_id else (str(metadata.get("title") or "").strip() or previous.query),
        season=(
            episode_evidence.get("season")
            if isinstance(episode_evidence, dict)
            and type(episode_evidence.get("season")) is int
            else None
        ),
        episode_map=(
            dict(episode_evidence["episode_map"])
            if isinstance(episode_evidence, dict) else None
        ),
        visibility="internal",
        root_job_id=previous.root_job_id or previous.id,
        replenishment_round=previous.replenishment_round + 1,
    )
    with LOCK:
        lineage = followup.root_job_id
        linked = next((
            existing for existing in JOBS.values()
            if existing.source == followup.source
            and existing.root_job_id == lineage
            and (
                existing.phase not in TERMINAL_PHASES
                or _replenishment_followup_closed(existing)
            )
        ), None)
        if linked is not None:
            if episode_evidence is not None and not linked.episode_map:
                _persist_replenishment_episode_evidence(linked, episode_evidence)
                append_log(
                    linked,
                    "已从原补源 acquisition 的路径、大小和缺口一一对应"
                    "恢复精确集号映射。",
                )
            return linked
        if any(
            existing.source == followup.source
            and existing.phase not in TERMINAL_PHASES
            for existing in JOBS.values()
        ):
            raise ValueError("查补目录已有进行中的整理任务")
        followup.directory.mkdir(mode=0o700, parents=True, exist_ok=False)
        if episode_evidence is not None:
            _atomic_json(
                followup.directory / "replenishment-episode-evidence.json",
                episode_evidence,
            )
        inherited_failures = _load_replenishment_failures(previous)
        if inherited_failures:
            _atomic_json(_replenishment_failure_path(followup), {
                "version": 1,
                "job_id": followup.id,
                "inherited_from_job_id": previous.id,
                "failures": inherited_failures,
            })
        JOBS[followup.id] = followup
        persist_job(followup)
    append_log(followup, f"由任务 {previous.id} 的完成后缺项检测自动创建。")
    if episode_evidence is not None:
        append_log(
            followup,
            f"已按 {len(followup.episode_map or {})} 个到盘文件的完整路径后缀、"
            "精确大小和唯一 resource gap 传递集号映射。",
        )
    start_thread(prepare_job, followup)
    return followup


def _cancel_unlinked_replenishment_followup(followup: Any) -> None:
    """Stop a child created concurrently with cancellation of its owner."""
    if not isinstance(followup, Job):
        return
    with LOCK:
        followup.cancel_requested = True
        removed = SCHEDULER.cancel_pending(followup.id)
        if removed or followup.phase == "queued":
            if followup.phase != "cancelled":
                require_transition(followup.phase, "cancelled")
                followup.phase = "cancelled"
            followup.error = None
            followup.progress = {
                "stage": "owner_cancelled_before_link", "completed": 0,
                "total": 1, "percent": 0.0,
                "message": "父作品在子任务关联前已取消",
            }
            followup.updated_at = utc_now()
            persist_job(followup)


def execute_media(job: Job, digest: str) -> None:
    if job.cancel_requested:
        finish_cancel(job, media_execution=True)
        return
    update_job(job, phase="executing_media", error=None, progress=None)
    command = [
        sys.executable,
        str(SCRAPER),
        *common_connection_args(),
        "--execute-plan",
        str(job.directory / "media-plan.json"),
        "--approve-plan-sha256",
        digest,
        "--journal",
        str(job.directory / "media-journal.json"),
        "--cleanup-empty-source",
        "--execute",
    ]
    code, output = run_command(job, command)
    if job.cancel_requested:
        try:
            transaction_restore = _restore_transaction_failure_scope(
                job, reason="media_execution_cancelled",
            )
        except Exception as exc:
            append_log(job, "取消后的远端回滚恢复未闭环：" + redact(str(exc)))
            update_job(
                job,
                phase="recovery_required",
                error="取消后的远端回滚恢复未闭环，请保留任务并重试恢复。",
                digest=None,
            )
        else:
            summary = dict(job.plan_summary or {})
            summary["transaction_lifecycle"] = transaction_restore
            job.plan_summary = summary
            finish_cancel(job, media_execution=True)
    elif code != 0:
        if (job.directory / "media-journal.json").exists():
            append_log(job, "媒体执行未成功，已保留 journal 供恢复检查。")
            try:
                transaction_restore = _restore_transaction_failure_scope(
                    job, reason="media_execution_failed",
                )
            except Exception as exc:
                append_log(job, "执行失败后的远端回滚恢复未闭环：" + redact(str(exc)))
            else:
                summary = dict(job.plan_summary or {})
                summary["transaction_lifecycle"] = transaction_restore
                job.plan_summary = summary
            update_job(
                job,
                phase="recovery_required",
                error="媒体执行未成功，请先检查恢复计划。",
                digest=None,
            )
            if auto_execute_media_enabled():
                append_log(job, "自动流水线将校验恢复摘要、回滚到执行前状态并重新规划。")
                request_recovery(job)
        else:
            fail_job(job, command_failure_reason(output, "媒体整理执行失败"))
    else:
        if is_internal_replenishment_followup(job):
            complete_internal_replenishment_followup(job)
            return
        update_job(
            job,
            phase="replenishing",
            error=None,
            progress={
                "stage": "replenishment_search",
                "completed": 0,
                "total": 1,
                "percent": 94.0,
                "message": "正常刮削已提交，正在执行完成后缺项搜索",
            },
        )
        append_log(job, "媒体整理已提交并通过校验；执行槽已释放，继续完成缺项搜索与到盘核验。")
        start_thread(finalize_media_replenishment, job)


_AUTOMATIC_REPLENISHMENT_RETRY_STATUSES = frozenset({
    "adapter_not_configured", "search_failed", "invalid_candidates",
    "no_match", "acquire_failed", "invalid_acquisition", "partial",
    "failed", "post_check_failed", "followup_failed", "followup_partial",
    "subtitle_retryable",
    # Persisted jobs may have been stopped by a formerly finite setting.  If
    # the operator changes the setting to zero, they become resumable again.
    "round_limit_reached", "detected",
})


def _automatic_replenishment_retry_allowed(
    job: Job, summary: dict[str, Any] | None = None,
) -> bool:
    """Return whether a persisted incomplete acquisition must self-resume."""
    if not auto_replenish_missing_enabled() or job.cancel_requested:
        return False
    public = summary if isinstance(summary, dict) else job.plan_summary
    replenishment = public.get("replenishment") if isinstance(public, dict) else None
    status = str(replenishment.get("status") or "") if isinstance(replenishment, dict) else ""
    if status in {
        "sources_exhausted", "awaiting_sources",
        "scrape_first_wait",
        "cancelled_after_media_commit", "cancellation_requested_after_media_commit",
    }:
        return False
    historical_incomplete = (
        job.phase == "failed"
        and (job.directory / "media-plan.json").is_file()
        and (
            str((job.progress or {}).get("stage") or "").startswith("replenishment_")
            or "查补" in str(job.error or "")
        )
    )
    if status not in _AUTOMATIC_REPLENISHMENT_RETRY_STATUSES and not historical_incomplete:
        return False
    maximum = replenishment_max_rounds()
    return maximum == 0 or job.replenishment_round < maximum


def _delayed_replenishment_retry(
    job: Job, delay: int, *, before_dispatch: Callable[[], None] | None = None,
) -> None:
    if SHUTDOWN_EVENT.is_set():
        return
    deadline = time.monotonic() + delay
    while time.monotonic() < deadline:
        if SHUTDOWN_EVENT.is_set():
            return
        if job.cancel_requested or job.phase != "replenishing":
            if job.cancel_requested and job.phase in {"replenishing", "cancelling"}:
                finalize_media_replenishment(job)
            return
        time.sleep(min(1.0, deadline - time.monotonic()))
    if job.phase == "replenishing" and not job.cancel_requested:
        if SHUTDOWN_EVENT.is_set():
            return
        if not _wait_for_global_resume(job):
            return
        if job.phase != "replenishing" or job.cancel_requested:
            if job.cancel_requested and job.phase in {"replenishing", "cancelling"}:
                finalize_media_replenishment(job)
            return
        # This timer is deliberately outside the FIFO worker.  Queue the next
        # analysis only after the previous finalize call has returned; trying
        # to enqueue it from the still-active worker is rejected as a duplicate
        # job and used to overwrite the durable retry state with a false local
        # service failure.
        # Relinquish this timer's ownership before the callback is queued.
        # The callback may run immediately and arm its next retry; retaining
        # the old pending marker until this stack unwinds would make that arm
        # look like a duplicate and strand the durable replenishment loop.
        if before_dispatch is not None:
            before_dispatch()
        start_thread(finalize_media_replenishment, job)


def _launch_delayed_replenishment_retry(job: Job, delay: int) -> bool:
    """Arm at most one process-local delayed replenishment callback per job."""
    if SHUTDOWN_EVENT.is_set():
        return False
    with DELAYED_REPLENISHMENT_RETRY_LOCK:
        if job.id in DELAYED_REPLENISHMENT_RETRY_PENDING:
            return True
        token = object()
        DELAYED_REPLENISHMENT_RETRY_PENDING.add(job.id)
        DELAYED_REPLENISHMENT_RETRY_TOKENS[job.id] = token

    def release_ownership() -> None:
        with DELAYED_REPLENISHMENT_RETRY_LOCK:
            if DELAYED_REPLENISHMENT_RETRY_TOKENS.get(job.id) is not token:
                return
            DELAYED_REPLENISHMENT_RETRY_TOKENS.pop(job.id, None)
            DELAYED_REPLENISHMENT_RETRY_PENDING.discard(job.id)

    def delayed() -> None:
        try:
            _delayed_replenishment_retry(
                job, delay, before_dispatch=release_ownership,
            )
        except ValueError as exc:
            if "任务已经在队列中" not in str(exc):
                append_log(job, f"自动复查未入队：{redact(str(exc))}")
        finally:
            release_ownership()

    thread = threading.Thread(
        target=delayed,
        name=f"scrapeflow-replenishment-retry-{job.id}",
        daemon=True,
    )
    try:
        thread.start()
    except Exception:
        release_ownership()
        raise
    return True


def _schedule_replenishment_retry(
    job: Job, *, summary: dict[str, Any] | None = None, restored: bool = False,
) -> bool:
    """Persist and launch the next unattended candidate/search attempt."""
    if not _automatic_replenishment_retry_allowed(job, summary):
        return False
    gate = scrape_first_gate_evidence(job)
    if gate.get("ready") is not True:
        return _enter_scrape_first_wait(
            job,
            summary=dict(summary) if isinstance(summary, Mapping) else dict(job.plan_summary or {}),
            gate=gate,
        )
    with LOCK:
        if job.cancel_requested or job.phase not in {"replenishing", "failed"}:
            return False
        public = summary if isinstance(summary, Mapping) else job.plan_summary
        replenishment = (
            public.get("replenishment") if isinstance(public, Mapping) else None
        )
        status = str(replenishment.get("status") or "") if isinstance(
            replenishment, Mapping,
        ) else ""
        # Only a candidate/acquisition failure may quarantine the last
        # selection.  Audit, subtitle, child creation and source-wait failures
        # do not prove that a successfully delivered source was bad.
        seeded = (
            _seed_replenishment_failures_from_last_attempt(job)
            if status in {"", "failed", "acquire_failed", "invalid_acquisition"}
            else []
        )
        # A failed persisted coordinator is resumed directly because the public
        # lifecycle intentionally has no user-visible "manual retry" hop.
        job.phase = "replenishing"
        job.error = None
        job.digest = None
        job.process = None
        job.cancel_requested = False
        job.force_killed = False
        job.replenishment_round += 1
        if summary is not None:
            job.plan_summary = summary
        base = replenishment_retry_delay()
        projects = replenishment.get("projects") if isinstance(
            replenishment, Mapping,
        ) else None
        infrastructure_blocked = status in {
            "search_failed", "invalid_candidates", "acquire_failed",
        }
        if isinstance(projects, list):
            infrastructure_blocked = infrastructure_blocked or any(
                isinstance(lane, Mapping)
                and lane.get("status") == "infrastructure_failure"
                for project in projects if isinstance(project, Mapping)
                for lane in (
                    project.get("lane_status") or {}
                ).values() if isinstance(project.get("lane_status"), Mapping)
            )
        # A clean no-match round means the current distinct batch was audited
        # and the next batch is immediately useful work.  Do not exponential-
        # backoff candidate rotation.  Genuine infrastructure failures retain
        # the configured backoff so an outage cannot hot-loop.
        fast_candidate_rotation = status == "no_match" and not infrastructure_blocked
        delay = (
            1 if restored or fast_candidate_rotation
            else min(base * (2 ** min(max(job.replenishment_round - 1, 0), 3)), 300)
        )
        job.progress = {
            "stage": "replenishment_retry_wait",
            "completed": 0,
            "total": 1,
            "percent": 94.0,
            "message": f"本轮未落地，{delay} 秒后自动更换来源继续查补",
        }
        job.updated_at = utc_now()
        persist_job(job)
    append_log(
        job,
        f"查补未落地；已隔离 {len(seeded)} 个候选失败记录，"
        f"{delay} 秒后自动进入第 {job.replenishment_round + 1} 轮，无需人工操作。",
    )
    _launch_delayed_replenishment_retry(job, delay)
    return True


def _monitor_replenishment_followups(job: Job, followup_ids: list[str]) -> None:
    """Keep the root active until every acquired source is scraped and audited."""
    while True:
        if not _wait_for_global_resume(job):
            return
        if job.cancel_requested:
            finalize_media_replenishment(job)
            return
        with LOCK:
            followups = [JOBS.get(job_id) for job_id in followup_ids]
        if any(item is None for item in followups):
            summary = dict(job.plan_summary or {})
            replenishment = dict(summary.get("replenishment") or {})
            replenishment.update({
                "status": "followup_failed",
                "message": "查补后续任务状态缺失，自动重新核验到盘文件",
            })
            summary["replenishment"] = replenishment
            if not _schedule_replenishment_retry(job, summary=summary):
                update_job(job, phase="failed", error=replenishment["message"], plan_summary=summary)
            return
        for item in followups:
            if item is not None and item.phase == "failed":
                close_consumed_internal_replenishment_followup(item, owner=job)
        recovery_waiting = [
            item for item in followups
            if item is not None and item.phase == "recovery_required"
        ]
        recovery_stalled: set[str] = set()
        for item in recovery_waiting:
            if auto_execute_media_enabled() and not _schedule_recovery_retry(item):
                recovery_stalled.add(item.id)
        terminal_failures = [
            item for item in followups
            if item is not None and (
                (
                    item.phase in {"failed", "cancelled"}
                    and not _replenishment_followup_closed(item)
                )
                or (
                    item.phase == "recovery_required"
                    and (
                        not auto_execute_media_enabled()
                        or item.id in recovery_stalled
                    )
                )
            )
        ]
        if terminal_failures:
            summary = dict(job.plan_summary or {})
            replenishment = dict(summary.get("replenishment") or {})
            replenishment.update({
                "status": "followup_failed",
                "failed_followup_job_ids": [item.id for item in terminal_failures],
                "message": "查补文件已到盘但后续整理未闭环，正在自动重试",
            })
            summary["replenishment"] = replenishment
            if not _schedule_replenishment_retry(job, summary=summary):
                update_job(job, phase="failed", error=replenishment["message"], plan_summary=summary)
            return
        unverified_completed = [
            item for item in followups
            if item is not None
            and item.phase == "completed"
            and not _replenishment_followup_closed(item)
        ]
        if unverified_completed:
            summary = dict(job.plan_summary or {})
            replenishment = dict(summary.get("replenishment") or {})
            replenishment.update({
                "status": "followup_failed",
                "failed_followup_job_ids": [item.id for item in unverified_completed],
                "message": "查补后续任务缺少再次审计通过证据，正在自动重新核验",
            })
            summary["replenishment"] = replenishment
            if not _schedule_replenishment_retry(job, summary=summary):
                update_job(job, phase="failed", error=replenishment["message"], plan_summary=summary)
            return
        if all(item is not None and _replenishment_followup_closed(item) for item in followups):
            summary = dict(job.plan_summary or {})
            replenishment = dict(summary.get("replenishment") or {})
            previous_status = str(replenishment.get("status") or "")
            replenishment.update({
                "followup_verified": True,
            })
            if previous_status in {"partial", "followup_partial"}:
                replenishment["message"] = "已到盘部分完成整理与审计，继续查补剩余缺口"
                summary["replenishment"] = replenishment
                if not _schedule_replenishment_retry(job, summary=summary):
                    update_job(
                        job, phase="failed", plan_summary=summary,
                        error=replenishment["message"],
                    )
                return
            # A committed child proves only that the acquired files were
            # organized safely.  It does not prove that the original title is
            # now complete: another episode may still be absent and subtitle
            # verification has not run yet.  Clear the active monitor edge and
            # send the root through the ordinary post-commit audit again.  The
            # root may become completed only when that fresh audit reports no
            # episode or subtitle work.
            replenishment.update({
                "status": "post_followup_reaudit",
                "message": "补源文件已完成整理，正在重新审计当前作品",
                "verified_followup_job_ids": list(followup_ids),
            })
            replenishment.pop("followup_job_ids", None)
            replenishment.pop("followup_job_id", None)
            replenishment.pop("failed_followup_job_ids", None)
            summary["replenishment"] = replenishment
            update_job(
                job, error=None, plan_summary=summary,
                progress={
                    "stage": "title_reaudit", "completed": 0,
                    "total": 1, "percent": 98.0,
                    "message": "补源文件已整理，正在重新核验当前作品缺集与字幕",
                },
            )
            append_log(job, "所有查补后续任务均已完成；重新审计当前作品后再决定是否闭环。")
            start_thread(finalize_media_replenishment, job)
            return
        time.sleep(5)


def _launch_replenishment_followup_monitor(job: Job, followup_ids: list[str]) -> bool:
    """Run one durable child monitor outside the bounded analysis scheduler.

    ``finalize_media_replenishment`` itself runs as scheduled analysis.  Trying
    to submit the same root job again before it returns is correctly rejected
    by the scheduler as a duplicate and used to turn a successful acquisition
    into a false root failure.  The monitor is a coordinator, not analysis or
    mutation work, so keep one daemon per root and let child jobs use the FIFO
    pools normally.
    """
    with REPLENISHMENT_MONITOR_LOCK:
        if job.id in REPLENISHMENT_MONITOR_PENDING:
            return False
        REPLENISHMENT_MONITOR_PENDING.add(job.id)

    def monitor() -> None:
        try:
            _monitor_replenishment_followups(job, followup_ids)
        finally:
            with REPLENISHMENT_MONITOR_LOCK:
                REPLENISHMENT_MONITOR_PENDING.discard(job.id)

    threading.Thread(
        target=monitor,
        name=f"scrapeflow-replenishment-followups-{job.id}",
        daemon=True,
    ).start()
    return True


def _cancel_post_commit_replenishment(job: Job) -> None:
    """Restore retained transactions before closing an incomplete title loop."""
    if job.phase == "cancelled":
        return
    summary = dict(job.plan_summary or {})
    try:
        summary["transaction_lifecycle"] = _restore_transaction_lineage(
            job, reason="post_commit_cancelled",
        )
    except Exception as exc:
        message = "取消后的远端回滚恢复未闭环：" + redact(str(exc))
        update_job(
            job, phase="recovery_required", error=message,
            digest=None, plan_summary=summary,
            progress={
                "stage": "transaction_restore_required", "completed": 0,
                "total": 1, "percent": 94.0,
                "message": "正在保留远端回滚副本，等待安全恢复",
            },
        )
        append_log(job, message)
        return
    previous = summary.get("replenishment")
    replenishment = dict(previous) if isinstance(previous, Mapping) else {}
    replenishment.update({
        "status": "cancelled_after_media_commit",
        "round": job.replenishment_round + 1,
        "gap_count": replenishment.get("gap_count"),
    })
    summary["replenishment"] = replenishment
    update_job(
        job, phase="cancelled", error=None, plan_summary=summary,
        progress={
            "stage": "replenishment_cancelled", "completed": 0, "total": 1,
            "percent": 94.0,
            "message": "已停止当前作品的缺项闭环；不记为补齐",
        },
    )
    append_log(
        job,
        "远端回滚事务已恢复；当前作品缺项闭环已取消，"
        "任务记为取消而不是补齐，未伪报完成。",
    )




def _subtitle_discovery_manifest_checkpoint(
    task: Mapping[str, Any],
) -> dict[str, str] | None:
    """Digest one batch's immutable manifest set without copying its payloads."""
    batch_id = task.get("batch_id")
    manifest_sha256s = task.get("manifest_sha256s")
    if (
        not isinstance(batch_id, str)
        or re.fullmatch(r"[0-9a-f]{24}", batch_id) is None
        or not isinstance(manifest_sha256s, list)
        or len(manifest_sha256s) != len(set(manifest_sha256s))
        or not all(
            isinstance(value, str)
            and re.fullmatch(r"[0-9a-f]{64}", value) is not None
            for value in manifest_sha256s
        )
    ):
        return None
    core = {
        "batch_id": batch_id,
        "manifest_sha256s": sorted(manifest_sha256s),
    }
    return {
        "batch_id": batch_id,
        "manifest_set_sha256": canonical_digest(core),
    }


def _subtitle_discovery_manifest_checkpoints(
    owner_job_id: str,
) -> list[dict[str, str]]:
    """Capture discovery evidence visible before one subtitle execution."""
    checkpoints: list[dict[str, str]] = []
    queue_root = SUBTITLE_SOURCE_DISCOVERY_RUNTIME.queue_root
    for path in sorted(queue_root.glob("*.json")) if queue_root.exists() else []:
        try:
            task = load_json(path)
        except (OSError, ValueError, json.JSONDecodeError):
            continue
        if task.get("owner_job_id") != owner_job_id:
            continue
        checkpoint = _subtitle_discovery_manifest_checkpoint(task)
        if checkpoint is not None:
            checkpoints.append(checkpoint)
    return sorted(checkpoints, key=lambda row: row["batch_id"])




def _title_closure_projection(closure: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "status": "audited",
        "evidence_sha256": closure.get("evidence_sha256"),
        "source_plan_sha256": closure.get("source_plan_sha256"),
        "audited_at": closure.get("audited_at"),
        "summary": closure.get("summary"),
    }


def finalize_media_replenishment(job: Job) -> None:
    """Close only the committed job's title loop; never scan the whole library."""
    if job.maintenance_stop_requested:
        _park_replenishment_for_maintenance_restart(job)
        return
    if job.cancel_requested:
        _cancel_post_commit_replenishment(job)
        return

    closure: dict[str, Any] | None = None
    subtitle_execution: dict[str, Any] | None = None
    followup_specs: list[dict[str, Any]] = []
    plan: dict[str, Any] = {}
    gate_snapshot_sha256: str | None = None
    failure_stage = "scrape_first_gate"
    failure_message: str | None = None
    try:
        _raise_if_replenishment_maintenance_stopped(job)
        gate = scrape_first_gate_evidence(job)
        if gate.get("ready") is not True:
            waiting_summary = dict(job.plan_summary or {})
            waiting_summary["title_subtitle_execution"] = {
                "status": "deferred_until_scrape_first_gate",
                "reason": "ordinary_scrapes_not_yet_accepted",
            }
            _enter_scrape_first_wait(
                job, summary=waiting_summary, gate=gate,
            )
            return
        raw_gate_snapshot = gate.get("snapshot_sha256")
        if (
            not isinstance(raw_gate_snapshot, str)
            or re.fullmatch(r"[0-9a-f]{64}", raw_gate_snapshot) is None
        ):
            raise ScrapeFirstGateClosed({
                "ready": False,
                "status": "blocked",
                "checked_at": utc_now(),
                "blocker_count": 1,
                "blockers": [{
                    "kind": "gate_witness_missing",
                    "path": UNSCRAPED_MEDIA_ROOT,
                }],
                "message": "普通刮削门禁缺少稳定快照，已阻止补源",
            })
        gate_snapshot_sha256 = raw_gate_snapshot
        failure_stage = "title_audit"
        _raise_if_replenishment_maintenance_stopped(job)
        closure = audit_current_job_titles(job)
        closure_summary = closure["summary"]
        current_episode_gaps = list(closure["episode_gaps"])
        subtitle_actions = (
            int(closure_summary["confirmed_subtitle_gap_count"])
            + int(closure_summary["pending_subtitle_verification_count"])
        )
        if not current_episode_gaps and subtitle_actions:
            failure_stage = "title_subtitle_resolution"
            update_job(job, progress={
                "stage": "title_subtitle_resolution", "completed": 0,
                "total": subtitle_actions, "percent": 94.0,
                "message": "缺集已核验，正在确认或补齐当前作品的中文字幕",
            })
            _raise_if_replenishment_maintenance_stopped(job)
            subtitle_execution = execute_current_title_subtitles(
                job, closure, scrape_gate_sha256=gate_snapshot_sha256,
            )
            failure_stage = "title_reaudit"
            _raise_if_replenishment_maintenance_stopped(job)
            closure = audit_current_job_titles(job)
            closure_summary = closure["summary"]
            current_episode_gaps = list(closure["episode_gaps"])

        remaining_subtitle_actions = (
            int(closure_summary["confirmed_subtitle_gap_count"])
            + int(closure_summary["pending_subtitle_verification_count"])
        )
        if not current_episode_gaps and remaining_subtitle_actions:
            replenishment = {
                "status": "subtitle_retryable",
                "round": job.replenishment_round + 1,
                "gap_count": remaining_subtitle_actions,
                "episode_gap_count": 0,
                "confirmed_subtitle_gap_count": int(
                    closure_summary["confirmed_subtitle_gap_count"]
                ),
                "pending_subtitle_verification_count": int(
                    closure_summary["pending_subtitle_verification_count"]
                ),
                "message": "当前作品的中文字幕仍未形成完整证据",
            }
        else:
            failure_stage = "replenishment_search"
            _raise_if_replenishment_maintenance_stopped(job)
            replenishment, followup_specs, plan = prepare_post_scrape_replenishment(
                job, current_episode_gaps=current_episode_gaps,
                scrape_gate_sha256=gate_snapshot_sha256,
            )
            _raise_if_replenishment_maintenance_stopped(job)
    except ReplenishmentCancelledStop:
        _cancel_post_commit_replenishment(job)
        return
    except ReplenishmentMaintenanceStop:
        _park_replenishment_for_maintenance_restart(job)
        return
    except ReplenishmentShutdownStop:
        return
    except ScrapeFirstGateClosed as exc:
        waiting_summary = dict(job.plan_summary or {})
        if closure is not None:
            waiting_summary["title_closure"] = _title_closure_projection(closure)
        waiting_summary["title_subtitle_execution"] = {
            "status": "deferred_until_scrape_first_gate",
            "reason": "ordinary_scrape_snapshot_changed",
        }
        _enter_scrape_first_wait(
            job, summary=waiting_summary, gate=exc.evidence,
        )
        return
    except (TitleClosureBlocked, SubtitleDispatchBlocked) as exc:
        summary = dict(job.plan_summary or {})
        summary["title_closure"] = {
            "status": "paused", "stage": failure_stage,
            "message": redact(str(exc)),
        }
        summary["title_subtitle_execution"] = {
            "status": "not_run", "reason": "title_reaudit_paused",
        }
        summary["replenishment"] = {
            "status": "title_reaudit_paused", "round": job.replenishment_round + 1,
            "gap_count": None, "message": "持久暂停已阻止当前作品复核",
        }
        _schedule_current_title_reaudit(job, summary=summary, delay=1)
        return
    except Exception as exc:  # committed media stays safe, but title is incomplete
        failure_message = redact(str(exc))
        replenishment = {
            "status": "post_check_failed",
            "round": job.replenishment_round + 1,
            "gap_count": None,
            "stage": failure_stage,
            "message": failure_message,
        }

    summary = dict(job.plan_summary or {})
    if closure is not None:
        summary["title_closure"] = _title_closure_projection(closure)
    else:
        summary["title_closure"] = {
            "status": "unavailable", "stage": failure_stage,
            "message": failure_message or "当前作品复核未返回证据",
        }
    if subtitle_execution is not None:
        summary["title_subtitle_execution"] = subtitle_execution
    elif closure is None:
        summary["title_subtitle_execution"] = {
            "status": "not_run", "reason": "title_closure_unavailable",
        }
    else:
        closure_summary = closure.get("summary") or {}
        episode_count = int(closure_summary.get("episode_gap_count") or 0)
        subtitle_count = (
            int(closure_summary.get("confirmed_subtitle_gap_count") or 0)
            + int(closure_summary.get("pending_subtitle_verification_count") or 0)
        )
        summary["title_subtitle_execution"] = {
            "status": (
                "deferred_until_episode_gaps_close" if episode_count
                else "not_needed" if not subtitle_count
                else "not_run"
            ),
        }

    if followup_specs:
        followup_ids: list[str] = []
        followup_errors: list[str] = []
        try:
            for followup_spec in followup_specs:
                _raise_if_replenishment_maintenance_stopped(job)
                followup_source = str(followup_spec["source"])
                try:
                    followup = create_replenishment_followup(
                        job, followup_source, plan,
                        followup_spec.get("media")
                        if isinstance(followup_spec.get("media"), dict) else None,
                    )
                except Exception as exc:
                    followup_errors.append(redact(str(exc)))
                    append_log(job, f"查补文件已到盘，后续整理任务创建失败：{redact(str(exc))}")
                else:
                    with LOCK:
                        if job.cancel_requested or job.phase == "cancelling":
                            _cancel_unlinked_replenishment_followup(followup)
                            raise ReplenishmentCancelledStop
                        if job.maintenance_stop_requested or _remote_dispatch_closed():
                            followup.maintenance_stop_requested = True
                            raise ReplenishmentMaintenanceStop
                        followup_ids.append(followup.id)
                        replenishment["followup_job_ids"] = list(followup_ids)
                        replenishment["followup_job_id"] = followup_ids[0]
                        summary["replenishment"] = dict(replenishment)
                        update_job(
                            job, phase="replenishing", error=None,
                            plan_summary=summary,
                        )
                    append_log(job, f"缺项候选已到盘，自动创建后续整理任务 {followup.id}。")
                _raise_if_replenishment_maintenance_stopped(job)
        except ReplenishmentCancelledStop:
            _cancel_post_commit_replenishment(job)
            return
        except ReplenishmentMaintenanceStop:
            _park_replenishment_for_maintenance_restart(job)
            return
        except ReplenishmentShutdownStop:
            return
        if followup_errors:
            replenishment["status"] = "followup_partial" if followup_ids else "followup_failed"
            replenishment["message"] = "；".join(followup_errors[:3])
    if replenishment.get("status") == "sources_exhausted":
        replenishment = _awaiting_sources_business_state(replenishment)
    summary["replenishment"] = replenishment
    status = str(replenishment.get("status") or "unknown")
    if status == "awaiting_sources":
        _schedule_current_title_source_review(job, summary=summary)
        return
    if status in {"subtitle_retryable", "post_check_failed", "title_reaudit_paused"}:
        if _schedule_current_title_reaudit(job, summary=summary):
            return
    if job.cancel_requested:
        _cancel_post_commit_replenishment(job)
        return
    closure_public = closure.get("summary") if isinstance(closure, Mapping) else None
    closure_complete = bool(
        isinstance(closure_public, Mapping)
        and closure_public.get("complete") is True
    )
    ordinary_source = _ordinary_scrape_source(job.source)
    if status == "no_regular_gaps" and closure_complete and ordinary_source is not None:
        # The acceptance contract intentionally binds to the persisted public
        # closure projection.  Persist that projection while the task remains
        # non-terminal, then run the complete contract before any completed
        # transition or processed-index update is possible.
        update_job(
            job, phase="replenishing", error=None, plan_summary=summary,
        )
        ordinary_acceptance = _ordinary_final_completion_contract(job)
        summary["ordinary_final_completion"] = ordinary_acceptance
        if ordinary_acceptance.get("accepted") is not True:
            scrape_contract = ordinary_acceptance.get("scrape_acceptance")
            failed_checks = sorted(
                name for name, passed in {
                    **dict(
                        scrape_contract.get("checks")
                        if isinstance(scrape_contract, Mapping) else {}
                    ),
                    **dict(
                        scrape_contract.get("extension_checks")
                        if isinstance(scrape_contract, Mapping) else {}
                    ),
                    **dict(ordinary_acceptance.get("final_checks") or {}),
                }.items()
                if passed is not True
            )
            replenishment = dict(replenishment)
            replenishment.update({
                "status": "post_check_failed",
                "stage": "ordinary_acceptance",
                "message": (
                    "普通任务完整验收未通过："
                    + "、".join(failed_checks[:8])
                ),
            })
            summary["replenishment"] = replenishment
            if _schedule_current_title_reaudit(job, summary=summary):
                return
            status = "post_check_failed"
        else:
            try:
                transaction_lifecycle = _run_replenishment_mutation_stage(
                    job,
                    lambda: _commit_transaction_lineage(job),
                    scrape_gate_sha256=gate_snapshot_sha256,
                )
            except ReplenishmentCancelledStop:
                _cancel_post_commit_replenishment(job)
                return
            except ReplenishmentMaintenanceStop:
                _park_replenishment_for_maintenance_restart(job)
                return
            except ReplenishmentShutdownStop:
                return
            except ScrapeFirstGateClosed as exc:
                _enter_scrape_first_wait(
                    job, summary=summary, gate=exc.evidence,
                )
                return
            except Exception as exc:
                replenishment = dict(replenishment)
                replenishment.update({
                    "status": "post_check_failed",
                    "stage": "hybrid_transaction_commit",
                    "message": "远端回滚隔离提交未闭环：" + redact(str(exc)),
                })
                summary["replenishment"] = replenishment
                if _schedule_current_title_reaudit(job, summary=summary):
                    return
                status = "post_check_failed"
            else:
                summary["transaction_lifecycle"] = transaction_lifecycle
    succeeded = status == "no_regular_gaps" and closure_complete
    cancelled = status == "cancelled_after_media_commit"
    terminal = succeeded
    active_followup_ids = replenishment.get("followup_job_ids")
    if (
        status in {"acquired", "partial", "followup_partial"}
        and isinstance(active_followup_ids, list)
        and active_followup_ids
    ):
        update_job(
            job, phase="replenishing", error=None, plan_summary=summary,
            progress={
                "stage": "replenishment_followup", "completed": 0,
                "total": len(active_followup_ids), "percent": 97.0,
                "message": "补源文件已到盘，正在等待自动整理与再次审计",
            },
        )
        _launch_replenishment_followup_monitor(job, list(active_followup_ids))
        return
    if status == "acquired" and not active_followup_ids:
        replenishment = dict(replenishment)
        replenishment.update({
            "status": "followup_failed",
            "message": "补源已报告到盘，但没有可跟踪的后续刮削任务",
        })
        summary["replenishment"] = replenishment
        status = "followup_failed"
    elif status == "no_regular_gaps" and not closure_complete:
        replenishment = dict(replenishment)
        replenishment.update({
            "status": "post_check_failed",
            "message": "补源请求为空，但当前作品复核证据未闭合",
        })
        summary["replenishment"] = replenishment
        if _schedule_current_title_reaudit(job, summary=summary):
            return
        status = "post_check_failed"
    if not terminal and not cancelled:
        summary["replenishment"] = replenishment
        if _schedule_replenishment_retry(job, summary=summary):
            return
    replenishment_failed = not terminal and not cancelled
    failure_reason = str(
        replenishment.get("message")
        or f"历史查补未落地: {replenishment.get('status')}"
    )
    maximum = replenishment_max_rounds()
    retry_exhausted = bool(
        replenishment_failed
        and maximum > 0
        and job.replenishment_round >= maximum
    )
    failure_progress = (
        f"查补未落地：{failure_reason}；已达到配置的自动重试上限"
        if retry_exhausted
        else f"查补未落地：{failure_reason}；当前配置不允许自动续跑"
    )
    update_job(
        job,
        phase=("failed" if replenishment_failed else "cancelled" if cancelled else "completed"),
        error=failure_reason if replenishment_failed else None,
        plan_summary=summary,
        progress={
            "stage": "replenishment_complete",
            "completed": 1,
            "total": 1,
            "percent": 100.0,
            "message": (
                failure_progress
                if replenishment_failed
                else (
                    "已停止当前作品的补源闭环；已落盘媒体与日志保留"
                    if cancelled
                    else "正常刮削与当前作品缺项处理均已结束"
                )
            ),
        },
    )
    if succeeded:
        remember_completed_job(job)
    append_log(job, (
        failure_progress + "，已保留完整记录。"
        if replenishment_failed
        else (
            "已停止当前作品的补源闭环；已落盘媒体与安全日志保留。"
            if cancelled
            else "整理任务与当前作品缺项处理均已结束。"
        )
    ))




def replenishment_source_review_interval() -> int:
    """Bound the business review cadence independently from adapter retries."""
    try:
        value = int(os.getenv("SCRAPEFLOW_SOURCE_REVIEW_INTERVAL", "21600"))
    except ValueError:
        value = 21600
    return max(300, min(value, 30 * 86400))




def _awaiting_sources_business_state(
    replenishment: Mapping[str, Any], *, reviewed_at: datetime | None = None,
) -> dict[str, Any]:
    """Keep an exhausted current-title search open for a later source review."""
    reviewed = (reviewed_at or datetime.now(timezone.utc)).astimezone(timezone.utc)
    interval = replenishment_source_review_interval()
    return {
        **dict(replenishment),
        "status": "awaiting_sources",
        "evidence_status": "sources_exhausted",
        "business_state": "current_title_source_review",
        "reviewed_at": reviewed.isoformat(),
        "next_review_at": (reviewed + timedelta(seconds=interval)).isoformat(),
        "review_interval_seconds": interval,
    }


def _current_title_source_review_delay(replenishment: Mapping[str, Any]) -> int:
    """Return a restart-safe delay for one current title, never a global sweep."""
    next_review_at = replenishment.get("next_review_at")
    if not isinstance(next_review_at, str):
        return 1
    try:
        due = datetime.fromisoformat(next_review_at.replace("Z", "+00:00"))
    except ValueError:
        return 1
    if due.tzinfo is None:
        return 1
    remaining = (
        due.astimezone(timezone.utc) - datetime.now(timezone.utc)
    ).total_seconds()
    return max(1, int(remaining) + 1)


def _schedule_current_title_source_review(
    job: Job, *, summary: dict[str, Any], restored: bool = False,
) -> None:
    """Persist one title's source wait and resume it after the due time."""
    replenishment = summary.get("replenishment")
    if not (
        isinstance(replenishment, Mapping)
        and replenishment.get("status") == "awaiting_sources"
    ):
        raise ValueError("当前作品来源复查缺少 awaiting_sources 状态")
    delay = _current_title_source_review_delay(replenishment)
    with LOCK:
        cancelled = job.cancel_requested or job.phase in {"cancelling", "cancelled"}
        if cancelled:
            pass
        elif job.phase not in {"replenishing", "failed"}:
            raise ValueError("当前任务状态不允许等待作品来源复查")
        else:
            job.phase = "replenishing"
            job.error = None
            job.digest = None
            job.process = None
            job.plan_summary = summary
            job.progress = {
                "stage": "current_title_source_wait",
                "completed": 0,
                "total": 1,
                "percent": 94.0,
                "message": f"当前来源已搜索完毕；{delay} 秒后只重试这个作品",
            }
            job.updated_at = utc_now()
            persist_job(job)
    if cancelled:
        _cancel_post_commit_replenishment(job)
        return
    if not restored:
        append_log(
            job,
            "当前作品仍有缺口；现有来源证明已穷尽，"
            f"将在 {delay} 秒后重新查找，任务不记为完成。",
        )
    _launch_delayed_replenishment_retry(job, delay)


def _resume_failed_current_title_source_wait(job: Job) -> bool:
    """Recover an old failed source wait without rotating candidates or rounds."""
    summary = dict(job.plan_summary or {})
    replenishment = summary.get("replenishment")
    if not (
        isinstance(replenishment, Mapping)
        and replenishment.get("status") in {"awaiting_sources", "sources_exhausted"}
        and media_journal_succeeded(job)
    ):
        return False
    if replenishment.get("status") == "sources_exhausted":
        summary["replenishment"] = _awaiting_sources_business_state(replenishment)
    _schedule_current_title_source_review(job, summary=summary, restored=True)
    return True


def _schedule_current_title_reaudit(
    job: Job, *, summary: dict[str, Any], delay: int | None = None,
    restored: bool = False,
) -> bool:
    """Retry title evidence/OCR without rotating or poisoning media sources."""
    wait = max(1, delay if delay is not None else replenishment_retry_delay())
    with LOCK:
        if job.cancel_requested or job.phase not in {"replenishing", "failed"}:
            return False
        job.phase = "replenishing"
        job.error = None
        job.digest = None
        job.process = None
        job.plan_summary = summary
        job.progress = {
            "stage": "current_title_reaudit_wait",
            "completed": 0,
            "total": 1,
            "percent": 94.0,
            "message": f"当前作品证据尚未闭合；{wait} 秒后再次复核",
        }
        job.updated_at = utc_now()
        persist_job(job)
    if not restored:
        append_log(job, f"当前作品复核未闭合；{wait} 秒后原作品内重试。")
    _launch_delayed_replenishment_retry(job, wait)
    return True






def _require_subtitle_dispatch_open() -> None:
    if _remote_dispatch_closed():
        raise SubtitleDispatchBlocked("global_pause_active_during_subtitle_cycle")


@contextmanager
def _subtitle_execution_guard() -> Any:
    """Hold the pause transition lock across one create-only subtitle item."""
    with GLOBAL_CONTROL_TRANSITION_LOCK:
        _require_subtitle_dispatch_open()
        yield


def _scrape_first_subtitle_guard(
    job: Job, scrape_gate_sha256: str,
) -> Callable[[], Any]:
    """Return an item guard bound to one fresh ordinary-scrape snapshot."""

    @contextmanager
    def guard() -> Any:
        # Use the same lock as ordinary job insertion.  Once the second read
        # succeeds, no new local ordinary job can appear until this one
        # bounded subtitle item has finished its remote mutation.
        with GLOBAL_CONTROL_TRANSITION_LOCK, SCRAPE_FIRST_TRANSITION_LOCK:
            _require_subtitle_dispatch_open()
            _revalidate_scrape_first_snapshot(job, scrape_gate_sha256)
            yield

    return guard


def prepare_title_subtitle_execution(
    refined: Mapping[str, Any],
) -> tuple[Any, dict[str, Any]]:
    """Read and validate fresh candidates; never write to AList."""
    from engine.tools.subtitle_executor import (  # pylint: disable=import-outside-toplevel
        prepare_selection,
    )

    _require_subtitle_dispatch_open()
    client = _execution_alist_client()
    prepared = prepare_selection(
        client, refined, scan_gate=_require_subtitle_dispatch_open,
    )
    return client, prepared


def _subtitle_source_manifests() -> list[dict[str, Any]]:
    """Load background-search manifests; malformed rows remain rejection evidence."""
    if not SUBTITLE_SOURCE_MANIFEST_ROOT.exists():
        return []
    output: list[dict[str, Any]] = []
    for path in sorted(SUBTITLE_SOURCE_MANIFEST_ROOT.glob("*.json")):
        try:
            output.append(load_json(path))
        except (OSError, ValueError, json.JSONDecodeError) as exc:
            output.append({
                "kind": "invalid_subtitle_source_manifest",
                "locator": str(path),
                "load_error": f"{type(exc).__name__}: {redact(str(exc))}",
            })
    return output


def _discover_subtitle_source_batch(batch: Mapping[str, Any]) -> dict[str, Any]:
    """Discover complete source manifests with read-only background APIs."""
    from engine.scrapeflow.quark_fast_save_bridge import (  # pylint: disable=import-outside-toplevel
        QuarkFastSaveBridge,
        UrlLibQuarkTransport,
        delegated_quark_session,
    )
    from engine.scrapeflow.subtitle_source_discovery import (  # pylint: disable=import-outside-toplevel
        discover_quark_manifests,
        discover_torrent_manifests,
    )
    from engine.tools import replenishment_local_adapter as adapter  # pylint: disable=import-outside-toplevel

    _require_subtitle_dispatch_open()
    existing_rows = _subtitle_source_manifests()
    existing_locators = {
        str(row.get("locator")) for row in existing_rows
        if isinstance(row, Mapping) and isinstance(row.get("locator"), str)
    }
    # Exclude both URL and metainfo identity before the provider result cap.
    # This prevents the old first 32 hashes from occupying every later pass.
    for row in existing_rows:
        acquisition = row.get("acquisition") if isinstance(row, Mapping) else None
        if isinstance(acquisition, Mapping):
            infohash = str(acquisition.get("infohash") or "").casefold()
            if re.fullmatch(r"[0-9a-f]{40}", infohash):
                existing_locators.add(f"torrent_infohash:{infohash}")
    discovery_queue = SUBTITLE_MEMBER_ACQUISITION_ROOT / "discovery-queue"
    for path in sorted(discovery_queue.glob("*.json")) if discovery_queue.exists() else []:
        try:
            task = load_json(path)
        except (OSError, ValueError, json.JSONDecodeError):
            continue
        telemetry = task.get("provider_telemetry")
        if not isinstance(telemetry, Mapping):
            continue
        for provider in ("quark_share", "torrent"):
            row = telemetry.get(provider)
            if not isinstance(row, Mapping):
                continue
            existing_locators.update(
                str(locator) for locator in row.get("resource_failed_locators", [])
                if isinstance(locator, str) and locator
            )
    try:
        seconds = int(os.getenv("SCRAPEFLOW_SUBTITLE_DISCOVERY_DEADLINE", "180"))
    except ValueError:
        seconds = 180
    deadline = time.monotonic() + max(30, min(900, seconds))

    def search_links(request: Mapping[str, Any]) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        return adapter._pansou_quark_links(  # pylint: disable=protected-access
            request, deadline=deadline, excluded_locators=existing_locators,
        )

    session_holder: dict[str, Any] = {}

    def inspect_share(row: Mapping[str, Any]) -> list[dict[str, Any]]:
        _require_subtitle_dispatch_open()
        if "session" not in session_holder:
            # Delegated AList cookie + direct HTTPS only.  No Native Helper,
            # browser activation, share save, or offline task is used here.
            session_holder["session"] = delegated_quark_session(
                _execution_alist_client(), MEDIA_LIBRARY_ROOT,
            )
        bridge = QuarkFastSaveBridge(UrlLibQuarkTransport(timeout=max(
            2.0, min(20.0, deadline - time.monotonic()),
        )))
        return bridge.inspect_share(
            session_holder["session"], pwd_id=str(row.get("share_id") or ""),
            passcode=str(row.get("passcode") or ""),
        )

    quark_manifests, quark_telemetry = discover_quark_manifests(
        batch, search_links=search_links, inspect_share=inspect_share,
        existing_locators=existing_locators,
    )
    _require_subtitle_dispatch_open()
    torrent_manifests, torrent_telemetry = discover_torrent_manifests(
        batch,
        fetch_bytes=adapter._fetch_bytes,  # pylint: disable=protected-access
        download_torrent=adapter._download_torrent,  # pylint: disable=protected-access
        existing_locators=existing_locators,
        deadline=deadline,
    )
    return {
        "manifests": [*quark_manifests, *torrent_manifests],
        "provider_telemetry": {
            "quark_share": quark_telemetry,
            "torrent": torrent_telemetry,
        },
        "search_complete": bool(
            quark_telemetry.get("search_complete")
            and torrent_telemetry.get("search_complete")
        ),
        "video_members_selected": 0,
        "quark_ui_operations": 0,
    }


def prepare_subtitle_member_acquisition(
    prepared: Mapping[str, Any],
) -> dict[str, Any]:
    """Build an immutable unmatched-member plan from durable source manifests."""
    from engine.scrapeflow.subtitle_member_acquisition import (  # pylint: disable=import-outside-toplevel
        build_search_batches,
        plan_subtitle_member_acquisition,
    )

    _require_subtitle_dispatch_open()
    requests = prepared.get("requests")
    selection = prepared.get("selection")
    if not isinstance(requests, Mapping) or not isinstance(selection, Mapping):
        raise ValueError("字幕 member acquisition 缺少 request/selection")
    manifests = _subtitle_source_manifests()
    search = build_search_batches(requests, selection)
    plan = plan_subtitle_member_acquisition(requests, selection, manifests)
    return {
        "search": search,
        "plan": plan,
        "source_manifest_files": len(manifests),
    }


def _passive_quark_helper_health() -> dict[str, Any]:
    """Probe an already-running Quark/CDP runtime without self-healing it."""
    helper_url = os.getenv("SCRAPEFLOW_QUARK_HELPER_URL", "").strip().rstrip("/")
    token = os.getenv("SCRAPEFLOW_QUARK_HELPER_TOKEN", "")
    if not helper_url or not token:
        return {"native_ready": False, "configured": False}
    try:
        request = Request(
            helper_url + "/health/passive",
            headers={"Authorization": "Bearer " + token, "Accept": "application/json"},
        )
        with urlopen(request, timeout=4) as response:
            value = json.load(response)
        if not isinstance(value, Mapping):
            raise ValueError("passive helper health is not an object")
        pids = value.get("quark_pids")
        return {
            "configured": True,
            "native_ready": bool(
                response.status == 200 and value.get("status") == "ok"
                and value.get("runtime") == "connected"
                and isinstance(pids, list) and pids
                and all(type(pid) is int and pid > 0 for pid in pids)
            ),
            "quark_pids": list(pids) if isinstance(pids, list) else [],
            "cdp_port": value.get("cdp_port"),
            "build_id": value.get("build_id"),
        }
    except Exception as exc:
        return {
            "configured": True, "native_ready": False,
            "error_type": type(exc).__name__,
        }


def _await_background_subtitle_arrival(
    client: Any, destination: str, *, expected_path: str, expected_size: int,
) -> str:
    """Poll refreshed AList state until exactly one expected subtitle arrives."""
    normalized = expected_path.replace("\\", "/")
    name = PurePosixPath(normalized).name
    if (
        not name or PurePosixPath(name).suffix.casefold() not in {".ass", ".srt"}
        or expected_size <= 0
    ):
        raise ValueError("background subtitle arrival identity is invalid")
    try:
        timeout = int(os.getenv("SCRAPEFLOW_SUBTITLE_ARRIVAL_TIMEOUT", "180"))
    except ValueError:
        timeout = 180
    deadline = time.monotonic() + max(5, min(900, timeout))
    while True:
        _require_subtitle_dispatch_open()
        rows: list[Mapping[str, Any]] = []
        try:
            walked = client.walk(
                destination, refresh=True, ignore_orphan_temp=False,
                include_bonus=True, include_title_extras=True,
            )
            if isinstance(walked, list):
                rows = [row for row in walked if isinstance(row, Mapping)]
        except (AttributeError, TypeError):
            listed = client.try_list(destination, refresh=True) or []
            rows = [row for row in listed if isinstance(row, Mapping)]
        matches = []
        for row in rows:
            if row.get("is_dir"):
                continue
            row_path = str(row.get("path") or row.get("name") or "").replace("\\", "/")
            if PurePosixPath(row_path).name == name and row.get("size") == expected_size:
                matches.append(
                    row_path if row_path.startswith("/") else f"{destination}/{row_path.lstrip('/')}"
                )
        if len(matches) == 1:
            return matches[0]
        if len(matches) > 1:
            raise ValueError("background subtitle arrival is ambiguous")
        if time.monotonic() >= deadline:
            raise TimeoutError("background subtitle did not become visible in AList")
        time.sleep(1.0)


def _local_subtitle_cloud_exhaustion_proof(
    item: Mapping[str, Any], current_journal: Mapping[str, Any],
    member_plan: Mapping[str, Any],
) -> dict[str, Any] | None:
    """Build tier-3 authority only from local, digest-bound durable evidence."""
    plan_sha256 = str(current_journal.get("plan_sha256") or "")
    request_id = str(item.get("request_id") or "")
    minimum_attempts = 30
    if member_plan.get("plan_sha256") != plan_sha256:
        return None
    acquisitions = [
        row for row in member_plan.get("acquisitions", [])
        if isinstance(row, Mapping) and row.get("request_id") == request_id
    ]
    expected_share_digests = {
        str(row.get("source_manifest_sha256")) for row in acquisitions
        if row.get("provider") == "quark_share"
    }
    expected_torrent_digests = {
        str(row.get("source_manifest_sha256")) for row in acquisitions
        if row.get("provider") == "torrent"
    }
    journal_root = (
        SUBTITLE_MEMBER_ACQUISITION_ROOT / "cloud-provider-journals"
        / plan_sha256 / request_id
    )
    journals = []
    for path in sorted(journal_root.glob("*.json")) if journal_root.exists() else []:
        try:
            journals.append(load_json(path))
        except (OSError, ValueError, json.JSONDecodeError):
            return None
    share_journals = [row for row in journals if row.get("provider") == "quark_share"]
    magnet_journals = [row for row in journals if row.get("provider") == "quark_magnet"]

    def has_recent_resource_failure_floor(row: Mapping[str, Any]) -> bool:
        """Ignore old infrastructure outages, never count them as exhaustion.

        A recovered provider may eventually produce the required candidate-level
        failures.  Requiring the most recent bounded evidence window to contain
        only resource failures keeps the tier-3 gate fail-closed without making
        one historical network/CDP outage an irreversible lock.

        Older journals did not persist per-attempt records.  They remain usable
        only when they contain no infrastructure failures at all.
        """
        records = row.get("records")
        if not isinstance(records, list):
            return bool(
                int(row.get("resource_failures") or 0) >= minimum_attempts
                and int(row.get("infrastructure_failures") or 0) == 0
            )
        terminal = [
            str(record.get("status") or "")
            for record in records if isinstance(record, Mapping)
            and record.get("status") in {
                "resource_failed", "infrastructure_failed", "complete",
            }
        ]
        return bool(
            len(terminal) >= minimum_attempts
            and terminal[-minimum_attempts:] == ["resource_failed"] * minimum_attempts
            and int(row.get("resource_failures") or 0) >= minimum_attempts
        )

    if not expected_torrent_digests:
        return None
    if (
        {str(row.get("source_manifest_sha256")) for row in share_journals}
        != expected_share_digests
        or {str(row.get("source_manifest_sha256")) for row in magnet_journals}
        != expected_torrent_digests
        or any(not has_recent_resource_failure_floor(row)
               for row in [*share_journals, *magnet_journals])
    ):
        return None
    share_search_complete = False
    queue_root = SUBTITLE_MEMBER_ACQUISITION_ROOT / "discovery-queue"
    for path in sorted(queue_root.glob("*.json")) if queue_root.exists() else []:
        try:
            task = load_json(path)
        except (OSError, ValueError, json.JSONDecodeError):
            continue
        batch = task.get("batch")
        telemetry = task.get("provider_telemetry")
        if (
            task.get("status") == "completed" and isinstance(batch, Mapping)
            and request_id in (batch.get("request_ids") or [])
            and isinstance(telemetry, Mapping)
            and isinstance(telemetry.get("quark_share"), Mapping)
            and telemetry["quark_share"].get("search_complete") is True
        ):
            share_search_complete = True
            break
    if not share_search_complete:
        return None
    return {
        "provider": "quark_magnet", "permanent": True,
        "search_complete": True,
        "attempts": min(int(row.get("resource_failures") or 0) for row in magnet_journals),
        "minimum_attempts": minimum_attempts,
        "all_candidates_resource_failed_or_absent": True,
        "quark_share_search_complete": True,
        "quark_share_candidates_resource_failed": len(share_journals),
        "quark_magnet_candidates_resource_failed": len(magnet_journals),
        "evidence_source": "local_durable_provider_journals",
        "plan_sha256": plan_sha256,
    }


def _fetch_background_subtitle_member(
    client: Any, item: Mapping[str, Any], *, plan_sha256: str,
    member_plan: Mapping[str, Any],
) -> bytes:
    """Fetch one exact subtitle member without launching or activating Quark."""
    from engine.scrapeflow.quark_fast_save_bridge import (  # pylint: disable=import-outside-toplevel
        QuarkMagnetCandidateError,
        QuarkMagnetOfflineBridge,
        QuarkNativeHelperTransport,
        QuarkFastSaveBridge,
        QuarkShareExpiredError,
        UrlLibQuarkTransport,
        delegated_quark_session,
    )
    from engine.scrapeflow.subtitle_member_acquisition import (  # pylint: disable=import-outside-toplevel
        fetch_torrent_subtitle_member_after_cloud_exhaustion,
        quark_bridge_selection,
        quark_magnet_bridge_selection,
    )

    transport = item.get("transport")
    if (
        item.get("include_video") is not False
        or not isinstance(transport, Mapping)
        or transport.get("may_launch_or_restart_quark") is not False
        or transport.get("allow_ui_activation") is not False
    ):
        raise ValueError("字幕获取计划未禁止视频/Quark UI")
    if item.get("provider") == "torrent":
        request_id = str(item.get("request_id") or "")
        source_digest = str(item.get("source_manifest_sha256") or "")
        if (
            not re.fullmatch(r"[0-9a-f]{64}", plan_sha256)
            or not re.fullmatch(r"[0-9a-f]{64}", source_digest)
            or not re.fullmatch(r"[0-9a-z]{3,64}", request_id)
        ):
            raise ValueError("Torrent 字幕获取身份无效")
        journal_path = (
            SUBTITLE_MEMBER_ACQUISITION_ROOT / "cloud-provider-journals"
            / plan_sha256 / request_id / f"{source_digest}.json"
        )
        journal_path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        journal = (
            load_json(journal_path) if journal_path.exists() else {
                "schema_version": 1,
                "kind": "subtitle_cloud_provider_journal",
                "plan_sha256": plan_sha256,
                "request_id": request_id,
                "source_manifest_sha256": source_digest,
                "provider": "quark_magnet",
                "attempts": 0,
                "resource_failures": 0,
                "infrastructure_failures": 0,
                "records": [],
            }
        )
        if any(
            journal.get(key) != value for key, value in {
                "plan_sha256": plan_sha256,
                "request_id": request_id,
                "source_manifest_sha256": source_digest,
                "provider": "quark_magnet",
            }.items()
        ):
            raise ValueError("Torrent 字幕云获取 journal 身份不一致")
        journal["attempts"] = int(journal.get("attempts") or 0) + 1
        attempt = {
            "attempt": journal["attempts"], "status": "running",
            "started_at": utc_now(),
            "may_launch_or_restart_quark": False, "allow_ui_activation": False,
        }
        journal["records"].append(attempt)
        _atomic_json(journal_path, journal)
        helper_before = _passive_quark_helper_health()
        if helper_before.get("native_ready") is not True:
            attempt.update({
                "status": "infrastructure_failed", "finished_at": utc_now(),
                "error": "existing_native_runtime_unavailable",
            })
            journal["infrastructure_failures"] = int(journal.get("infrastructure_failures") or 0) + 1
            journal["status"] = "retryable"
            _atomic_json(journal_path, journal)
            raise RuntimeError("quark_magnet_existing_native_runtime_unavailable_retryable")
        pids_before = helper_before.get("quark_pids")
        if not isinstance(pids_before, list) or not pids_before:
            attempt.update({
                "status": "infrastructure_failed", "finished_at": utc_now(),
                "error": "passive_health_lacks_process_identity",
            })
            journal["infrastructure_failures"] = int(journal.get("infrastructure_failures") or 0) + 1
            journal["status"] = "retryable"
            _atomic_json(journal_path, journal)
            raise RuntimeError("quark_magnet_passive_health_lacks_process_identity")
        attempt["quark_pids_before"] = pids_before
        _atomic_json(journal_path, journal)
        helper_url = os.getenv("SCRAPEFLOW_QUARK_HELPER_URL", "").strip()
        helper_token = os.getenv("SCRAPEFLOW_QUARK_HELPER_TOKEN", "")
        if not helper_url or not helper_token:
            attempt.update({
                "status": "infrastructure_failed", "finished_at": utc_now(),
                "error": "helper_not_configured",
            })
            journal["infrastructure_failures"] = int(journal.get("infrastructure_failures") or 0) + 1
            journal["status"] = "retryable"
            _atomic_json(journal_path, journal)
            raise RuntimeError("quark_magnet_helper_not_configured_retryable")
        selection = quark_magnet_bridge_selection(item)
        destination = (
            f"{SUBTITLE_MEMBER_STAGING_ROOT}/{plan_sha256[:16]}/"
            f"{request_id}/{source_digest[:16]}-magnet"
        )
        resume_task_id = next((
            str(row.get("task_id")) for row in reversed(journal.get("records", []))
            if isinstance(row, Mapping) and row.get("status") == "submitted"
            and isinstance(row.get("task_id"), str) and row.get("task_id")
        ), None)
        def on_submitted(task_id: str) -> None:
            _require_subtitle_dispatch_open()
            attempt["status"] = "submitted"
            attempt["task_id"] = task_id
            attempt["submitted_at"] = utc_now()
            _atomic_json(journal_path, journal)

        try:
            client.mkdir(destination)
            transport = QuarkNativeHelperTransport(
                helper_url, helper_token,
                timeout=float(os.getenv("SCRAPEFLOW_QUARK_HELPER_TIMEOUT", "120")),
                passive_only=True,
            )
            bridge = QuarkMagnetOfflineBridge(transport)
            session = delegated_quark_session(client, destination)
            receipt = bridge.execute(
                selection, destination, session,
                resume_task_id=resume_task_id, on_submitted=on_submitted,
            )
            helper_after = _passive_quark_helper_health()
            if (
                helper_after.get("native_ready") is not True
                or helper_after.get("quark_pids") != pids_before
            ):
                raise RuntimeError("quark_process_identity_changed_during_background_acquisition")
            member = item.get("subtitle_member")
            if not isinstance(member, Mapping):
                raise ValueError("Torrent 字幕成员缺失")
            target = _await_background_subtitle_arrival(
                client, destination,
                expected_path=str(member.get("path") or ""),
                expected_size=int(member.get("size") or 0),
            )
            payload = client.read_file_bytes(target, max_bytes=16 * 1024 * 1024)
            if len(payload) != member.get("size"):
                raise ValueError("Quark magnet 字幕到盘大小不一致")
            attempt.update({
                "status": "complete", "finished_at": utc_now(),
                "task_id": receipt.get("task_id"), "quark_pids_after": pids_before,
            })
            journal["status"] = "complete"
            _atomic_json(journal_path, journal)
            return payload
        except QuarkMagnetCandidateError as exc:
            attempt.update({
                "status": "resource_failed", "finished_at": utc_now(),
                "error": f"{type(exc).__name__}: {redact(str(exc))}",
            })
            journal["resource_failures"] = int(journal.get("resource_failures") or 0) + 1
            journal["status"] = "retryable"
            _atomic_json(journal_path, journal)
            SUBTITLE_SOURCE_DISCOVERY_RUNTIME.reopen_for_request(
                request_id, reason="quark_magnet_candidate_resource_failed",
            )
            proof = _local_subtitle_cloud_exhaustion_proof(
                item, journal, member_plan,
            )
            if proof is None:
                raise
            return fetch_torrent_subtitle_member_after_cloud_exhaustion(
                item, cloud_exhaustion_proof=proof,
                workspace_root=SUBTITLE_MEMBER_ACQUISITION_ROOT / "torrent-workspaces",
            )
        except Exception as exc:
            attempt.update({
                "status": "infrastructure_failed", "finished_at": utc_now(),
                "error": f"{type(exc).__name__}: {redact(str(exc))}",
            })
            journal["infrastructure_failures"] = int(journal.get("infrastructure_failures") or 0) + 1
            journal["status"] = "retryable"
            _atomic_json(journal_path, journal)
            raise
    if item.get("provider") != "quark_share":
        raise ValueError("字幕获取 provider 不受支持")
    selection = quark_bridge_selection(item)
    request_id = str(item.get("request_id") or "")
    source_digest = str(item.get("source_manifest_sha256") or "")
    if (
        not re.fullmatch(r"[0-9a-f]{64}", plan_sha256)
        or not re.fullmatch(r"[0-9a-f]{64}", source_digest)
        or not re.fullmatch(r"[0-9a-z]{3,64}", request_id)
    ):
        raise ValueError("字幕获取身份摘要无效")
    destination = (
        f"{SUBTITLE_MEMBER_STAGING_ROOT}/{plan_sha256[:16]}/"
        f"{request_id}/{source_digest[:16]}"
    )
    bridge = QuarkFastSaveBridge(UrlLibQuarkTransport())
    dry = bridge.dry_run(selection, destination)
    expected = dry.get("expected_files")
    if not isinstance(expected, list) or len(expected) != 1:
        raise ValueError("Quark 字幕 fast-save 必须只包含一个成员")
    name, size = expected[0].get("name"), expected[0].get("size")
    if (
        not isinstance(name, str) or not name
        or PurePosixPath(name).suffix.casefold() not in {".ass", ".srt"}
        or type(size) is not int or size <= 0
    ):
        raise ValueError("Quark 字幕 fast-save 成员无效")
    journal_path = (
        SUBTITLE_MEMBER_ACQUISITION_ROOT / "cloud-provider-journals"
        / plan_sha256 / request_id / f"{source_digest}.json"
    )
    journal_path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    journal = (
        load_json(journal_path) if journal_path.exists() else {
            "schema_version": 1, "kind": "subtitle_cloud_provider_journal",
            "plan_sha256": plan_sha256, "request_id": request_id,
            "source_manifest_sha256": source_digest, "provider": "quark_share",
            "attempts": 0, "resource_failures": 0,
            "infrastructure_failures": 0, "records": [],
        }
    )
    if any(
        journal.get(key) != value for key, value in {
            "plan_sha256": plan_sha256, "request_id": request_id,
            "source_manifest_sha256": source_digest, "provider": "quark_share",
        }.items()
    ):
        raise ValueError("Quark share 字幕 journal 身份不一致")
    existing = [
        row for row in (client.try_list(destination, refresh=True) or [])
        if not row.get("is_dir") and row.get("name") == name and row.get("size") == size
    ]
    journal["attempts"] = int(journal.get("attempts") or 0) + 1
    attempt = {
        "attempt": journal["attempts"], "status": "running",
        "started_at": utc_now(), "may_launch_or_restart_quark": False,
        "allow_ui_activation": False,
    }
    journal["records"].append(attempt)
    _atomic_json(journal_path, journal)
    try:
        client.mkdir(destination)
        if not existing:
            # Direct HTTPS share APIs only; Native Helper/UI are not involved.
            session = delegated_quark_session(client, destination)
            resume_task_id = next((
                str(row.get("task_id")) for row in reversed(journal.get("records", []))
                if isinstance(row, Mapping) and row is not attempt
                and row.get("status") == "submitted"
                and isinstance(row.get("task_id"), str) and row.get("task_id")
            ), None)

            def on_submitted(task_id: str) -> None:
                _require_subtitle_dispatch_open()
                attempt.update({
                    "status": "submitted", "task_id": task_id,
                    "submitted_at": utc_now(),
                })
                _atomic_json(journal_path, journal)

            bridge.execute(
                selection, destination, session,
                resume_task_id=resume_task_id, on_submitted=on_submitted,
            )
        target = _await_background_subtitle_arrival(
            client, destination,
            expected_path=str(expected[0].get("name") or name),
            expected_size=size,
        )
        payload = client.read_file_bytes(target, max_bytes=16 * 1024 * 1024)
        if len(payload) != size:
            raise ValueError("Quark 字幕 fast-save 到盘大小不一致")
        attempt.update({"status": "complete", "finished_at": utc_now()})
        journal["status"] = "complete"
        _atomic_json(journal_path, journal)
        return payload
    except QuarkShareExpiredError as exc:
        attempt.update({
            "status": "resource_failed", "finished_at": utc_now(),
            "error": f"{type(exc).__name__}: {redact(str(exc))}",
        })
        journal["resource_failures"] = int(journal.get("resource_failures") or 0) + 1
        journal["status"] = "retryable"
        _atomic_json(journal_path, journal)
        SUBTITLE_SOURCE_DISCOVERY_RUNTIME.reopen_for_request(
            request_id, reason="quark_share_candidate_resource_failed",
        )
        raise
    except Exception as exc:
        attempt.update({
            "status": "infrastructure_failed", "finished_at": utc_now(),
            "error": f"{type(exc).__name__}: {redact(str(exc))}",
        })
        journal["infrastructure_failures"] = int(journal.get("infrastructure_failures") or 0) + 1
        journal["status"] = "retryable"
        _atomic_json(journal_path, journal)
        raise


def execute_prepared_subtitle_member_acquisition(
    client: Any, prepared: Mapping[str, Any], member_prepared: Mapping[str, Any],
    *, run_root: Path, owner_job_id: str = "standalone",
    item_guard: Callable[[], Any] | None = None,
) -> dict[str, Any]:
    """Persist, acquire, verify and promote unmatched subtitle members."""
    from engine.scrapeflow.subtitle_member_acquisition import (  # pylint: disable=import-outside-toplevel
        build_verified_cache_selection,
        materialize_verified_members,
    )
    from engine.tools.subtitle_executor import execute_selection  # pylint: disable=import-outside-toplevel

    search = member_prepared.get("search")
    plan = member_prepared.get("plan")
    requests = prepared.get("requests")
    if not all(isinstance(value, Mapping) for value in (search, plan, requests)):
        raise ValueError("字幕 member acquisition 计划格式无效")
    plan_sha256 = str(plan.get("plan_sha256") or "")
    if not re.fullmatch(r"[0-9a-f]{64}", plan_sha256):
        raise ValueError("字幕 member acquisition 计划摘要无效")
    member_root = run_root / "member-acquisition" / plan_sha256
    effective_guard = item_guard or _subtitle_execution_guard
    with effective_guard():
        member_root.mkdir(mode=0o700, parents=True, exist_ok=True)
        _atomic_json(member_root / "subtitle-search-batches.json", dict(search))
        _atomic_json(member_root / "subtitle-member-plan.json", dict(plan))
        queue_result = SUBTITLE_SOURCE_DISCOVERY_RUNTIME.enqueue(
            search, owner_job_id=owner_job_id,
        )
    acquisition_journal = materialize_verified_members(
        plan,
        approved_plan_sha256=plan_sha256,
        fetch_member=lambda item: _fetch_background_subtitle_member(
            client, item, plan_sha256=plan_sha256, member_plan=plan,
        ),
        cache_root=SUBTITLE_MEMBER_CACHE_ROOT,
        journal_path=member_root / "subtitle-member-journal.json",
        item_guard=effective_guard,
    )
    cached = build_verified_cache_selection(
        requests, acquisition_journal, cache_root=SUBTITLE_MEMBER_CACHE_ROOT,
    )
    cached_selection = cached["selection"]
    promotion_journal: Mapping[str, Any] = {
        "status": "no_verified_members", "records": [],
    }
    if cached_selection.get("selections"):
        promotion_digest = str(cached_selection.get("selection_sha256") or "")
        promotion_root = member_root / "promotions" / promotion_digest
        with effective_guard():
            promotion_root.mkdir(mode=0o700, parents=True, exist_ok=True)
            _atomic_json(promotion_root / "verified-cache-requests.json", cached["requests"])
            _atomic_json(promotion_root / "verified-cache-selection.json", cached_selection)
        promotion_journal = execute_selection(
            client, cached_selection,
            approved_selection_sha256=promotion_digest,
            journal_path=promotion_root / "verified-cache-promotion-journal.json",
            item_guard=effective_guard,
            local_cache_root=SUBTITLE_MEMBER_CACHE_ROOT,
        )
    latest = _subtitle_latest_records(promotion_journal)
    promotion_counts = Counter(
        str(row.get("status") or "unknown") for row in latest.values()
    )
    resolved_ids = sorted(
        request_id for request_id, row in latest.items()
        if row.get("status") in {"created", "already_satisfied"}
    )
    result = {
        "schema_version": 1,
        "kind": "subtitle_member_acquisition_execution",
        "status": (
            "promoted" if resolved_ids else
            "retryable_search_required" if not plan.get("acquisitions") else
            "retryable_acquisition_incomplete"
        ),
        "plan_sha256": plan_sha256,
        "search_batches_sha256": search.get("search_batches_sha256"),
        "search_summary": search.get("summary"),
        "plan_summary": plan.get("summary"),
        "resolved_request_ids": resolved_ids,
        "resolved_count": len(resolved_ids),
        "created_count": promotion_counts.get("created", 0),
        "already_satisfied_count": promotion_counts.get("already_satisfied", 0),
        "acquisition_journal_status": acquisition_journal.get("status"),
        "promotion_journal_status": promotion_journal.get("status"),
        "discovery_queue": {
            **queue_result,
            **SUBTITLE_SOURCE_DISCOVERY_RUNTIME.snapshot(),
        },
        "video_members_selected": 0,
        "quark_ui_operations": 0,
        "updated_at": utc_now(),
    }
    with effective_guard():
        _atomic_json(member_root / "execution-result.json", result)
    return result


def _subtitle_latest_records(journal: Mapping[str, Any]) -> dict[str, Mapping[str, Any]]:
    latest: dict[str, Mapping[str, Any]] = {}
    for row in journal.get("records", []) or []:
        if isinstance(row, Mapping) and isinstance(row.get("request_id"), str):
            latest[str(row["request_id"])] = row
    return latest




def execute_prepared_title_subtitles(
    client: Any, prepared: Mapping[str, Any],
    *,
    execution_root: Path,
    owner_job_id: str = "standalone",
    item_guard: Callable[[], Any] | None = None,
) -> dict[str, Any]:
    """Digest-gate one title's subtitle plan with one pause gate per item."""
    from engine.tools.subtitle_executor import (  # pylint: disable=import-outside-toplevel
        execute_selection,
    )

    selection = prepared.get("selection")
    requests = prepared.get("requests")
    if not isinstance(selection, Mapping) or not isinstance(requests, Mapping):
        raise ValueError("字幕执行计划格式无效")
    request_core = {
        key: requests.get(key) for key in ("schema_version", "kind", "requests")
    }
    request_digest = str(requests.get("request_sha256") or "")
    if (
        not re.fullmatch(r"[0-9a-f]{64}", request_digest)
        or canonical_digest(request_core) != request_digest
        or selection.get("request_sha256") != request_digest
    ):
        raise ValueError("字幕 request 摘要无效或与 selection 不匹配")
    scan_failures = prepared.get("inventory_scan_failures")
    digest = str(selection.get("selection_sha256") or "")
    if not re.fullmatch(r"[0-9a-f]{64}", digest):
        raise ValueError("字幕 selection 摘要无效")
    run_root = execution_root
    result_kind = "title_subtitle_execution"
    effective_guard = item_guard or _subtitle_execution_guard
    with effective_guard():
        run_root.mkdir(mode=0o700, parents=True, exist_ok=True)
        _atomic_json(run_root / "subtitle-requests.json", dict(requests))
        _atomic_json(run_root / "subtitle-selection.json", dict(selection))
        if scan_failures:
            blocked = {
                "schema_version": 1,
                "kind": result_kind,
                "status": "inventory_scan_blocked",
                "selection_sha256": digest,
                "inventory_scan_failures": scan_failures,
                "inventory_missing_optional_roots": prepared.get(
                    "inventory_missing_optional_roots", []
                ),
                "updated_at": utc_now(),
                "executor_configured": True,
                "video_mutations": 0,
            }
            _atomic_json(run_root / "execution-result.json", blocked)
            return blocked

    journal = execute_selection(
        client, selection, approved_selection_sha256=digest,
        journal_path=run_root / "subtitle-execution-journal.json",
        item_guard=effective_guard,
    )
    latest = _subtitle_latest_records(journal)
    counts = Counter(str(row.get("status") or "unknown") for row in latest.values())
    selected_ids = {
        str(row.get("request_id")) for row in selection.get("selections", [])
        if isinstance(row, Mapping)
    }
    terminal_success = {
        request_id for request_id, row in latest.items()
        if row.get("status") in {"created", "already_satisfied"}
    }
    member_execution: dict[str, Any]
    try:
        member_prepared = prepare_subtitle_member_acquisition(prepared)
        member_execution = execute_prepared_subtitle_member_acquisition(
            client, prepared, member_prepared, run_root=run_root,
            owner_job_id=owner_job_id,
            item_guard=effective_guard,
        )
    except Exception as exc:
        if _remote_dispatch_closed():
            raise RuntimeError("global_pause_activated_during_subtitle_member_acquisition") from exc
        member_execution = {
            "schema_version": 1,
            "kind": "subtitle_member_acquisition_execution",
            "status": "planning_or_acquisition_failed_retryable",
            "error": f"{type(exc).__name__}: {redact(str(exc))}",
            "resolved_request_ids": [],
            "resolved_count": 0,
            "video_members_selected": 0,
            "quark_ui_operations": 0,
            "updated_at": utc_now(),
        }
    member_success = {
        str(request_id) for request_id in member_execution.get("resolved_request_ids", [])
        if isinstance(request_id, str)
    }
    unmatched_ids = {
        str(row.get("request_id"))
        for row in selection.get("failures", []) or []
        if isinstance(row, Mapping) and isinstance(row.get("request_id"), str)
    }
    unmatched = len(unmatched_ids - member_success)
    selected_unresolved = len(selected_ids - terminal_success)
    unresolved = unmatched + selected_unresolved
    result = {
        "schema_version": 1,
        "kind": result_kind,
        "status": "converged" if unresolved == 0 else "retryable_unresolved",
        "executor_configured": True,
        "selection_sha256": digest,
        "request_sha256": requests.get("request_sha256"),
        "request_count": len(requests.get("requests", []) or []),
        "selected_count": len(selected_ids),
        "created_count": counts.get("created", 0) + int(member_execution.get("created_count") or 0),
        "already_satisfied_count": (
            counts.get("already_satisfied", 0)
            + int(member_execution.get("already_satisfied_count") or 0)
        ),
        "isolated_failure_count": counts.get("failed", 0),
        "unmatched_retryable_count": unmatched,
        "unresolved_action_count": unresolved,
        "journal_status": journal.get("status"),
        "journal_path": str(run_root / "subtitle-execution-journal.json"),
        "inventory_missing_optional_roots": prepared.get(
            "inventory_missing_optional_roots", []
        ),
        "member_acquisition": member_execution,
        "member_acquisition_resolved_count": len(member_success),
        "video_mutations": 0,
        "quark_ui_operations": 0,
        "updated_at": utc_now(),
    }
    with effective_guard():
        _atomic_json(run_root / "execution-result.json", result)
    return result


def _subtitle_refinement_with_official_aliases(
    closure: Mapping[str, Any], refined: Mapping[str, Any],
) -> dict[str, Any]:
    """Attach bounded TMDB aliases as search hints for the exact title only."""
    from engine.scraper import TMDBClient  # pylint: disable=import-outside-toplevel

    tmdb_key = os.getenv("TMDB_API_KEY")
    targets = closure.get("title_targets")
    if not tmdb_key or not isinstance(targets, list):
        return deepcopy(dict(refined))
    client = TMDBClient(tmdb_key)
    aliases_by_root: dict[str, list[str]] = {}
    episode_enrichment_by_root: dict[
        str, tuple[
            dict[tuple[int, int], list[str]],
            dict[tuple[int, int], list[dict[str, Any]]],
        ]
    ] = {}
    for target in targets:
        if not isinstance(target, Mapping) or target.get("media_type") != "tv":
            continue
        root = str(target.get("target_root") or "")
        title = str(target.get("title") or "").strip()
        tmdb_id = target.get("tmdb_id")
        if not root or not title or type(tmdb_id) is not int or tmdb_id <= 0:
            continue
        values = [title, *tmdb_tv_aliases(client, tmdb_id)]
        aliases: list[str] = []
        seen: set[str] = set()
        for raw in values:
            value = str(raw).strip()
            key = re.sub(r"[^\w\u3400-\u9fff]+", "", value.casefold())
            if (
                not key or key in seen or len(value) > 120
                or any(char in value for char in "\r\n\x00")
            ):
                continue
            seen.add(key)
            aliases.append(value)
            if len(aliases) >= 16:
                break
        if aliases:
            aliases_by_root[root] = aliases
        episode_rows = []
        for bucket in ("confirmed_missing_chinese", "pending_review_or_probe"):
            for row in refined.get(bucket, []) or []:
                if (
                    isinstance(row, Mapping)
                    and str(row.get("target_root") or "") == root
                    and type(row.get("season")) is int
                    and type(row.get("episode")) is int
                ):
                    episode_rows.append({
                        "season": row["season"], "episode": row["episode"],
                    })
        if episode_rows:
            episode_enrichment_by_root[root] = tmdb_tv_episode_enrichment(
                client, tmdb_id, episode_rows,
            )
    enriched = deepcopy(dict(refined))
    for bucket in ("confirmed_missing_chinese", "pending_review_or_probe"):
        rows = enriched.get(bucket)
        if not isinstance(rows, list):
            continue
        for row in rows:
            if not isinstance(row, dict):
                continue
            aliases = aliases_by_root.get(str(row.get("target_root") or ""))
            if aliases:
                row["aliases"] = list(aliases)
            enrichment = episode_enrichment_by_root.get(
                str(row.get("target_root") or ""),
            )
            identity = (row.get("season"), row.get("episode"))
            if enrichment and type(identity[0]) is int and type(identity[1]) is int:
                title_aliases = enrichment[0].get(identity)
                source_aliases = enrichment[1].get(identity)
                if title_aliases:
                    row["title_aliases"] = list(title_aliases[:4])
                if source_aliases:
                    row["source_episode_aliases"] = deepcopy(source_aliases)
    return enriched


def execute_current_title_subtitles(
    job: Job, closure: Mapping[str, Any], *, scrape_gate_sha256: str | None = None,
) -> dict[str, Any]:
    """Resolve only this job's subtitle actions and keep evidence in its tree."""
    if not title_closure_evidence_is_valid(closure):
        raise ValueError("当前作品字幕执行拒绝无效的复核证据")
    summary = closure.get("summary")
    refined = closure.get("subtitle_refinement")
    if not isinstance(summary, Mapping) or not isinstance(refined, Mapping):
        raise ValueError("当前作品字幕执行证据缺少 refinement")
    if int(summary.get("episode_gap_count") or 0) != 0:
        raise ValueError("当前作品仍有缺集，先补集再执行字幕闭环")
    action_count = (
        int(summary.get("confirmed_subtitle_gap_count") or 0)
        + int(summary.get("pending_subtitle_verification_count") or 0)
    )
    if action_count == 0:
        return {
            "schema_version": 1,
            "kind": "title_subtitle_execution",
            "status": "not_needed",
            "unresolved_action_count": 0,
            "updated_at": utc_now(),
        }
    # Capture exactly which discovery manifests are visible before the
    # selection layer reads the immutable source-manifest store.  A task may
    # finish concurrently; a later digest mismatch deliberately causes one
    # prompt retry so newly published evidence cannot be delayed.
    discovery_manifest_checkpoints = _subtitle_discovery_manifest_checkpoints(
        job.id,
    )
    enriched_refined = _subtitle_refinement_with_official_aliases(
        closure, refined,
    )
    client, prepared = prepare_title_subtitle_execution(enriched_refined)
    selection = prepared.get("selection")
    digest = selection.get("selection_sha256") if isinstance(selection, Mapping) else None
    if not isinstance(digest, str) or re.fullmatch(r"[0-9a-f]{64}", digest) is None:
        raise ValueError("当前作品字幕 selection 摘要无效")
    run_root = job.directory / "title-subtitles" / digest
    item_guard = (
        _scrape_first_subtitle_guard(job, scrape_gate_sha256)
        if scrape_gate_sha256 is not None else _subtitle_execution_guard
    )
    execution_result = execute_prepared_title_subtitles(
        client,
        prepared,
        execution_root=run_root,
        owner_job_id=job.id,
        item_guard=item_guard,
    )
    result = {
        **execution_result,
        "discovery_manifest_checkpoints": discovery_manifest_checkpoints,
    }
    _atomic_json(job.directory / "title-subtitle-result.json", result)
    return result




def prepare_recovery(job: Job) -> None:
    journal_path = job.directory / "media-journal.json"
    if not journal_path.exists():
        fail_job(job, "没有找到可恢复的媒体执行 journal")
        return
    if _resume_post_commit_replenishment(job):
        # Recovery is meaningful only for an uncommitted media mutation.  A
        # successful journal is stronger evidence than a stale persisted
        # recovery phase (for example when the service restarted between the
        # commit and post-commit gap audit).  Never call the rollback command
        # for an already committed tree; resume only the read-only audit/search.
        return
    try:
        transaction_restore = _restore_transaction_failure_scope(
            job, reason="recovery_requested",
        )
    except Exception as exc:
        message = "远端回滚隔离尚未安全恢复：" + redact(str(exc))
        append_log(job, message)
        update_job(job, phase="recovery_required", error=message, digest=None)
        _schedule_recovery_retry(job)
        return
    else:
        summary = dict(job.plan_summary or {})
        summary["transaction_lifecycle"] = transaction_restore
        job.plan_summary = summary
        persist_job(job)
    update_job(job, phase="planning_recovery", error=None, digest=None, plan_summary=None)
    command = [
        sys.executable,
        str(SCRAPER),
        *common_connection_args(),
        "--recover-journal",
        str(journal_path),
    ]
    code, output = run_command(job, command)
    if job.cancel_requested:
        update_job(
            job,
            phase="recovery_required",
            error="恢复检查已取消，原任务仍需要恢复。",
            digest=None,
        )
        return
    if code != 0:
        message = "恢复状态检查失败，将自动保留 journal 并重试"
        append_log(job, f"错误: {message}")
        update_job(job, phase="recovery_required", error=message, digest=None)
        _schedule_recovery_retry(job)
        return
    digest_rows = [
        line[len(RECOVERY_DIGEST_PREFIX):].strip()
        for line in output.splitlines()
        if line.startswith(RECOVERY_DIGEST_PREFIX)
    ]
    if len(digest_rows) != 1 or not re.fullmatch(r"[0-9a-f]{64}", digest_rows[0]):
        message = "恢复检查没有返回可批准的 journal 摘要"
        append_log(job, f"错误: {message}")
        update_job(job, phase="recovery_required", error=message, digest=None)
        _schedule_recovery_retry(job)
        return
    moves: list[dict[str, str]] = []
    for raw_line in output.splitlines():
        if not raw_line.startswith(RECOVERY_ITEM_PREFIX):
            continue
        try:
            item = strict_json_loads(raw_line[len(RECOVERY_ITEM_PREFIX):])
            if not isinstance(item, dict):
                raise ValueError("恢复项目不是 JSON 对象")
            source = media_library_path(item.get("source"), allow_root=False)
            target = media_library_path(item.get("target"), allow_root=False)
        except (ValueError, json.JSONDecodeError) as exc:
            message = f"恢复检查返回了无效的文件项目: {exc}"
            append_log(job, f"错误: {message}")
            update_job(job, phase="recovery_required", error=message, digest=None)
            _schedule_recovery_retry(job)
            return
        moves.append({"source": source, "target": target, "name": posixpath.basename(target)})
    if len({(item["source"], item["target"]) for item in moves}) != len(moves):
        message = "恢复检查返回了重复的文件项目"
        append_log(job, f"错误: {message}")
        update_job(job, phase="recovery_required", error=message, digest=None)
        _schedule_recovery_retry(job)
        return
    recovery_summary = {
            "kind": "recovery",
            "title": "恢复到执行前路径",
            "file_count": len(moves),
            "files": moves[:250],
            "truncated": len(moves) > 250,
            "warnings": ["恢复会根据失败 journal 将已移动或改名的文件还原。"],
        }
    if auto_execute_media_enabled():
        update_job(
            job, phase="starting_recovery_execution",
            digest=digest_rows[0], plan_summary=recovery_summary,
        )
        append_log(job, "恢复计划已通过结构校验；自动回滚进入执行队列。")
        start_execution(execute_approved_recovery, job, digest_rows[0])
    else:
        update_job(
            job, phase="awaiting_recovery_approval",
            digest=digest_rows[0], plan_summary=recovery_summary,
        )


def execute_recovery(job: Job, digest: str) -> None:
    if job.cancel_requested:
        finish_cancel(job, media_execution=True)
        return
    update_job(job, phase="executing_recovery", error=None)
    recovery_journal = next_attempt_artifact(job.directory / "recovery-journal.json")
    if recovery_journal.name != "recovery-journal.json":
        append_log(job, f"保留上次恢复记录，本次写入新 journal: {recovery_journal.name}")
    command = [
        sys.executable,
        str(SCRAPER),
        *common_connection_args(),
        "--recover-journal",
        str(job.directory / "media-journal.json"),
        "--approve-recovery-sha256",
        digest,
        "--journal",
        str(recovery_journal),
        "--execute",
    ]
    code, _ = run_command(job, command)
    if job.cancel_requested:
        update_job(
            job,
            phase="recovery_required",
            error="恢复过程被取消，请重新检查恢复状态。",
            digest=None,
        )
    elif code != 0:
        update_job(
            job,
            phase="recovery_required",
            error="自动恢复没有完成，请查看日志并再次检查。",
            digest=None,
        )
        _schedule_recovery_retry(job)
    else:
        update_job(job, phase="recovered", error=None, digest=None)
        append_log(job, "任务已恢复到执行前状态。")
        if auto_execute_media_enabled():
            journal = job.directory / "media-journal.json"
            if journal.exists():
                archived = next_attempt_artifact(job.directory / "media-journal-recovered.json")
                journal.replace(archived)
                append_log(job, f"失败写入 journal 已归档为 {archived.name}。")
            plan = job.directory / "media-plan.json"
            if plan.exists():
                archived_plan = next_attempt_artifact(
                    job.directory / "media-plan-recovered.json",
                )
                plan.replace(archived_plan)
                append_log(job, f"旧媒体计划已归档为 {archived_plan.name}。")
            update_job(
                job, phase="queued", error=None, digest=None,
                approval_source=None, plan_summary=None, progress=None,
            )
            append_log(job, "安全回滚完成，自动从原目录重新规划，无需人工重试。")
            start_thread(prepare_job, job)


def request_recovery(job: Job) -> None:
    require_nonlegacy_job_mutation(job)
    with LOCK:
        if job.phase not in {"recovery_required", "failed"}:
            raise ValueError("当前任务不需要恢复")
        if not (job.directory / "media-journal.json").exists():
            raise ValueError("当前任务没有可用的媒体 journal")
        job.cancel_requested = False
        job.force_killed = False
        job.phase = "planning_recovery"
        job.updated_at = utc_now()
        persist_job(job)
    start_thread(prepare_recovery, job)


def request_cancel(job: Job) -> None:
    """Request a cooperative stop while preserving recovery evidence."""
    require_nonlegacy_job_mutation(job)
    cancelled_pending_replenishment = False
    with LOCK:
        if job.phase in TERMINAL_PHASES:
            raise ValueError("任务已经结束")
        if job.phase == "recovery_required":
            raise ValueError("任务存在待恢复记录，请先完成安全恢复")
        if job.phase == "awaiting_recovery_approval":
            update_job(
                job,
                phase="recovery_required",
                error="已取消当前恢复审核；媒体 journal 仍需处理。",
                digest=None,
            )
            return
        if job.phase == "awaiting_media_approval":
            update_job(job, phase="cancelled", error=None, digest=None)
            append_log(job, "已取消待审核计划，未执行任何媒体写入。")
            return

        job.cancel_requested = True
        post_commit_replenishment = job.phase == "replenishing"
        removed_from_queue = SCHEDULER.cancel_pending(job.id)
        process = job.process if job.process and job.process.poll() is None else None
        if removed_from_queue:
            if post_commit_replenishment:
                summary = dict(job.plan_summary or {})
                summary["replenishment"] = {
                    "status": "cancellation_requested_after_media_commit",
                    "round": job.replenishment_round + 1,
                    "gap_count": None,
                }
                update_job(
                    job, phase="cancelling",
                    error="正在恢复远端回滚事务并关闭当前作品缺项闭环。",
                    plan_summary=summary,
                )
                cancelled_pending_replenishment = True
            else:
                update_job(job, phase="cancelled", error=None, digest=None)
                append_log(job, "任务已从等待队列移除，未启动媒体操作。")
                return
        elif post_commit_replenishment:
            summary = dict(job.plan_summary or {})
            replenishment = summary.get("replenishment")
            replenishment = dict(replenishment) if isinstance(replenishment, Mapping) else {}
            replenishment.update({
                "status": "cancellation_requested_after_media_commit",
                "round": job.replenishment_round + 1,
            })
            summary["replenishment"] = replenishment
            update_job(
                job, phase="cancelling", plan_summary=summary,
                error="正在等待当前作品处理安全退出。",
            )
        elif not removed_from_queue:
            update_job(
                job,
                phase="cancelling",
                error="正在等待当前操作安全退出。",
            )
    if cancelled_pending_replenishment:
        _cancel_post_commit_replenishment(job)
        return
    if process is not None:
        process.send_signal(signal.SIGINT)
    append_log(job, "收到安全停止请求，正在保留 journal 并退出。")


def resolve_failed_job(job: Job, payload: dict[str, Any]) -> None:
    """Resolve a deterministic failure without touching remote media.

    ``keep_existing`` intentionally leaves both the current library and the
    newly uploaded source untouched.  It only closes the failed task after an
    explicit confirmation, so the action cannot masquerade as a completed
    scrape or silently discard an alternate encode.
    """
    require_nonlegacy_job_mutation(job)
    if payload.get("confirm") is not True:
        raise ValueError("保留现有版本需要明确确认")
    if payload.get("action") != "keep_existing":
        raise ValueError("未知的失败处置方式")
    with LOCK:
        if job.phase != "failed":
            raise ValueError("只有失败任务可以执行此处置")
        if (job.directory / "media-journal.json").exists():
            raise ValueError("任务存在待恢复记录，请先完成安全恢复")
        error = str(job.error or "")
        if not re.search(r"目标目录已存在同名文件|目标冲突|拒绝覆盖", error):
            raise ValueError("当前失败不是可保留现有版本的目标冲突")
        update_job(job, phase="cancelled", error=None, digest=None)
        append_log(job, "用户选择保留现有目标版本；未改动目标库或新资源目录。")


def _park_replenishment_for_maintenance_restart(job: Job) -> None:
    """Persist an interrupted adapter as resumable work, never cancellation.

    Adapter search/acquire artifacts are checkpoints.  A maintenance restart
    under the durable global pause may interrupt their subprocess, but it must
    not erase those artifacts, consume an attempt, or close the coordinator as
    though the operator cancelled it.
    """
    with LOCK:
        summary = dict(job.plan_summary or {})
        now = utc_now()
        maintenance = dict(summary.get("maintenance_restart") or {})
        already_parked = maintenance.get("status") == "parked"
        maintenance.update({
            "status": "parked",
            "reason": "persistent_global_pause_api_shutdown",
        })
        maintenance.setdefault("interrupted_at", now)
        summary["maintenance_restart"] = maintenance
        progress = {
            "stage": "replenishment_maintenance_parked", "completed": 0,
            "total": 1, "percent": 94.0,
            "message": "API 维护重启已保留补源检查点，恢复调度后自动续跑",
        }
        job.phase = "replenishing"
        job.error = None
        job.digest = None
        job.process = None
        job.cancel_requested = False
        job.force_killed = False
        job.plan_summary = summary
        job.progress = progress
        job.updated_at = now
        persist_job(job)
    if not already_parked:
        append_log(job, "API 维护停机已中断补源适配器；保留现有检查点并等待恢复调度。")


def shutdown_running_jobs(timeout: float = 20.0) -> None:
    control = GLOBAL_CONTROL.snapshot()
    maintenance_pause = bool(
        control.get("paused") is True and control.get("persistent") is True
    )
    with LOCK:
        active = [(job, job.process) for job in JOBS.values() if job.process and job.process.poll() is None]
        scheduled_active = SCHEDULER.active_jobs()
        maintenance_jobs = {
            job.id: job
            for job in [*(job for job, _ in active), *scheduled_active]
            if maintenance_pause and job.phase == "replenishing"
        }
        maintenance_job_ids = {
            job.id for job in maintenance_jobs.values()
        }
        cancellation_jobs = {
            job.id: job for job, _ in active
            if job.id not in maintenance_job_ids
        }
        # A replenishment worker can be between adapter subprocesses while it
        # still owns the FIFO slot.  Once the process-exit gate is closed it
        # waits here, so project the same ordinary-shutdown cancellation used
        # for an active subprocess and let the worker leave without a write.
        for job in scheduled_active:
            if job.phase == "replenishing" and job.id not in maintenance_job_ids:
                cancellation_jobs[job.id] = job
        for job in maintenance_jobs.values():
            job.maintenance_stop_requested = True
            job.error = None
            job.updated_at = utc_now()
            persist_job(job)
        for job in cancellation_jobs.values():
            job.cancel_requested = True
            job.phase = "cancelling"
            job.error = "服务正在停止，等待引擎安全退出。"
            job.updated_at = utc_now()
            persist_job(job)
    for _, process in active:
        process.send_signal(signal.SIGINT)
    deadline = time.monotonic() + timeout
    while active and time.monotonic() < deadline:
        active = [(job, process) for job, process in active if process.poll() is None]
        if active:
            time.sleep(0.1)
    for job, process in active:
        job.force_killed = True
        process.terminate()
        if job.id in maintenance_job_ids:
            try:
                process.wait(timeout=2.0)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=2.0)
            continue
        if (job.directory / "media-journal.json").exists():
            update_job(
                job,
                phase="recovery_required",
                error="服务停止时安全退出超时，请检查恢复计划。",
                digest=None,
            )
        else:
            update_job(job, phase="failed", error="服务停止时任务被强制终止，请检查远端状态。")
    for job in maintenance_jobs.values():
        _park_replenishment_for_maintenance_restart(job)


def run_scheduled(target: Callable[..., None], job: Job, *args: Any) -> None:
    try:
        target(job, *args)
    except ReplenishmentShutdownStop:
        # ``shutdown_running_jobs`` owns the durable cancellation/maintenance
        # projection.  A process-local exit gate must not invent a failure.
        return
    except BaseException as exc:
        append_log(job, f"本地服务异常: {redact(str(exc))}")
        if job.phase not in TERMINAL_PHASES and job.phase != "recovery_required":
            if job.phase in EXECUTION_PHASES and (job.directory / "media-journal.json").exists():
                update_job(
                    job,
                    phase="recovery_required",
                    error="本地服务发生异常，请先检查恢复计划。",
                    digest=None,
                )
            else:
                fail_job(job, "本地服务发生异常，请查看日志")


def _job_mutation_resources(job: Job) -> list[str]:
    """Resolve the minimal path lease shared by all write-capable pools."""
    resources = {job.source}
    plan_path = job.directory / "media-plan.json"
    if plan_path.exists():
        try:
            plan, _ = unwrap_media_plan(load_json(plan_path))
            for value in (plan.get("source_root"), plan.get("target_root")):
                if isinstance(value, str) and value:
                    resources.add(value)
            for item in plan.get("files") or []:
                if isinstance(item, dict):
                    for field in ("source_path", "target_dir", "target_path"):
                        value = item.get(field)
                        if isinstance(value, str) and value:
                            resources.add(
                                posixpath.dirname(value) if field != "target_dir" else value
                            )
        except (OSError, ValueError, json.JSONDecodeError):
            # Execution performs the authoritative plan validation. Keeping the
            # source key here fails closed by serializing same-source retries.
            pass
    for item in (job.plan_summary or {}).get("files") or []:
        if not isinstance(item, dict):
            continue
        for field in ("source", "target"):
            value = item.get(field)
            if isinstance(value, str) and value:
                resources.add(posixpath.dirname(value))

    normalized = sorted(
        {normalize_remote_input(path) for path in resources},
        key=lambda path: (path.count("/"), path.casefold()),
    )
    minimal = [
        path for path in normalized
        if not any(paths_overlap(path, existing) for existing in normalized[:normalized.index(path)])
    ]
    return minimal


def start_thread(target: Callable[..., None], job: Job, *args: Any) -> None:
    """Queue analysis; current-title finalization owns a cross-pool path lease."""
    require_nonlegacy_job_mutation(job)
    resources = (
        _job_mutation_resources(job)
        if target is finalize_media_replenishment else ()
    )
    SCHEDULER.submit("analysis", job, target, *args, resources=resources)


def start_execution(target: Callable[..., None], job: Job, *args: Any) -> None:
    """Queue a mutation; non-overlapping jobs may execute concurrently."""
    require_nonlegacy_job_mutation(job)
    minimal = _job_mutation_resources(job)
    SCHEDULER.submit("execution", job, target, *args, resources=minimal)




class ExistingJobConflict(ValueError):
    """A normalized source already has an active or recoverable job."""

    def __init__(self, job: Job):
        super().__init__("这个目录已有未完成或待恢复任务，已返回原任务")
        self.job = job


def create_job(payload: dict[str, Any]) -> Job:
    source = unscraped_media_path(payload.get("path"))
    category_parent = target_parent_for_category(payload.get("category"))
    parent_value = payload.get("parent")
    if parent_value is not None and not isinstance(parent_value, str):
        raise ValueError("目标父目录格式无效")
    parent = media_library_path(parent_value, allow_root=True) if parent_value else category_parent
    if parent != category_parent and not parent.startswith(category_parent + "/"):
        raise ValueError(f"目标父目录必须位于所选分类 {category_parent} 下")
    if paths_overlap(source, parent):
        raise ValueError("源目录与目标目录不能相同，也不能互为父子目录")
    media_type = payload.get("type", "auto")
    if not isinstance(media_type, str) or media_type not in {"auto", "tv", "movie", "collection"}:
        raise ValueError("媒体类型必须是自动、电视剧、电影或电影合集")
    absolute = payload.get("absolute", False)
    simplified = payload.get("prefer_simplified", True)
    if type(absolute) is not bool or type(simplified) is not bool:
        raise ValueError("任务选项格式无效")
    tmdb_id = payload.get("tmdb_id")
    if tmdb_id is None or tmdb_id == "":
        tmdb_id = None
    if tmdb_id is not None and (type(tmdb_id) is not int or tmdb_id <= 0):
        raise ValueError("TMDB ID 必须是正整数")
    query_value = payload.get("query")
    if query_value is not None and not isinstance(query_value, str):
        raise ValueError("搜索标题格式无效")
    query = query_value.strip() if isinstance(query_value, str) and query_value.strip() else None
    if query is not None and len(query) > 200:
        raise ValueError("搜索标题格式无效")
    season = payload.get("season")
    if season is None or season == "":
        season = None
    if season is not None and (type(season) is not int or season < 0 or season > 100):
        raise ValueError("季度必须是 0 到 100 的整数")
    episode_group_value = payload.get("episode_group")
    if episode_group_value is not None and not isinstance(episode_group_value, str):
        raise ValueError("Episode Group ID 格式无效")
    episode_group = episode_group_value.strip() if isinstance(episode_group_value, str) and episode_group_value.strip() else None
    if episode_group is not None and (
        not isinstance(episode_group, str)
        or not re.fullmatch(r"[A-Za-z0-9_-]{1,100}", episode_group)
    ):
        raise ValueError("Episode Group ID 格式无效")
    episode_map_value = payload.get("episode_map")
    if episode_map_value is not None and not isinstance(episode_map_value, dict):
        raise ValueError("集数映射必须是 JSON 对象")
    episode_map = episode_map_value or None
    if episode_map is not None:
        if not isinstance(episode_map, dict) or len(episode_map) > 5000:
            raise ValueError("集数映射必须是 JSON 对象且不超过 5000 项")
        if not all(isinstance(key, str) and isinstance(value, str) for key, value in episode_map.items()):
            raise ValueError("集数映射的键和值必须是字符串")
    collection_map_value = payload.get("collection_map")
    if collection_map_value is not None and not isinstance(collection_map_value, dict):
        raise ValueError("合集映射必须是 JSON 对象")
    collection_map = collection_map_value or None
    if collection_map is not None:
        if not isinstance(collection_map, dict) or len(collection_map) > 1000:
            raise ValueError("合集映射必须是 JSON 对象且不超过 1000 项")
        if not all(isinstance(key, str) and type(value) is int and value > 0 for key, value in collection_map.items()):
            raise ValueError("合集映射必须使用字符串源编号和正整数 TMDB ID")
    if media_type == "collection" and (tmdb_id is None or not collection_map):
        raise ValueError("电影合集需要填写合集 TMDB ID 和合集映射")
    if tmdb_id is not None and media_type == "auto":
        raise ValueError("填写 TMDB ID 时需要明确选择电视剧、电影或电影合集")
    if tmdb_id is not None and query:
        raise ValueError("TMDB ID 与搜索标题只能填写一个")
    if media_type in {"movie", "collection"} and (season is not None or absolute or episode_group or episode_map):
        raise ValueError("季度和集数映射只适用于电视剧 / 番剧")
    if episode_group and not absolute:
        raise ValueError("使用 Episode Group 时需要开启绝对集数转换")
    job = Job(
        id=uuid.uuid4().hex[:12],
        source=source,
        parent=parent,
        media_type=media_type,
        absolute=absolute,
        prefer_simplified=simplified,
        tmdb_id=tmdb_id,
        query=query,
        season=season,
        episode_group=episode_group,
        episode_map=episode_map,
        collection_map=collection_map,
    )
    with SCRAPE_FIRST_TRANSITION_LOCK:
        with LOCK:
            same_source = [
                existing for existing in JOBS.values() if existing.source == source
            ]
            existing = (
                max(same_source, key=lambda item: item.updated_at)
                if same_source else None
            )
            if existing is not None:
                raise ExistingJobConflict(existing)
            job.directory.mkdir(mode=0o700, parents=True, exist_ok=False)
            JOBS[job.id] = job
            persist_job(job)
    # A queued job is already a durable task artifact.  Create its audit log
    # before handing it to the asynchronous planner so a busy/paused queue is
    # never left with an incomplete job tree.
    append_log(job, "任务已创建并进入规划队列")
    start_thread(prepare_job, job)
    return job


def validated_retry_settings(previous: Job, payload: dict[str, Any] | None) -> dict[str, Any]:
    """Validate the small set of recognition settings that can be repaired in Web."""
    payload = payload or {}
    allowed = {"tmdb_id", "media_type", "query", "season", "archive_password"}
    unknown = set(payload) - allowed
    if unknown:
        raise ValueError("重试参数包含不支持的字段")

    tmdb_id = previous.tmdb_id
    if "tmdb_id" in payload:
        tmdb_id = payload["tmdb_id"]
        if tmdb_id is not None and (type(tmdb_id) is not int or tmdb_id <= 0):
            raise ValueError("TMDB ID 必须是正整数")

    query = previous.query
    if "query" in payload:
        value = payload["query"]
        if value is not None and not isinstance(value, str):
            raise ValueError("搜索标题格式无效")
        query = value.strip() if isinstance(value, str) and value.strip() else None
        if query is not None and len(query) > 200:
            raise ValueError("搜索标题格式无效")

    season = previous.season
    if "season" in payload:
        season = payload["season"]
        if season is not None and (type(season) is not int or season < 0 or season > 100):
            raise ValueError("季度必须是 0 到 100 的整数")

    if tmdb_id is not None and query:
        raise ValueError("TMDB ID 与搜索标题只能填写一个")
    media_type = previous.media_type
    if "media_type" in payload:
        requested_type = payload["media_type"]
        if requested_type not in {"tv", "movie"}:
            raise ValueError("TMDB 类型必须明确选择剧集或电影")
        media_type = requested_type
    if tmdb_id is not None and media_type == "auto":
        raise ValueError("填写 TMDB ID 时必须明确选择剧集或电影；目标文件夹不决定媒体类型")
    if media_type in {"movie", "collection"} and season is not None:
        raise ValueError("季度只适用于电视剧 / 番剧")
    archive_password_provided = "archive_password" in payload
    archive_password = payload.get("archive_password")
    if archive_password_provided:
        if archive_password is not None and not isinstance(archive_password, str):
            raise ValueError("压缩包密码格式无效")
        if isinstance(archive_password, str) and len(archive_password) > 512:
            raise ValueError("压缩包密码过长")
        archive_password = archive_password if archive_password else None
    return {
        "tmdb_id": tmdb_id,
        "query": query,
        "season": season,
        "media_type": media_type,
        "archive_password_provided": archive_password_provided,
        "archive_password": archive_password,
    }


def _resume_ready_replenishment_acquisition(previous: Job) -> Job | None:
    """Create follow-up work from already uploaded files without downloading again."""
    summary = previous.plan_summary if isinstance(previous.plan_summary, dict) else {}
    replenishment = summary.get("replenishment")
    if not isinstance(replenishment, dict) or replenishment.get("status") != "invalid_acquisition":
        return None
    acquisition_paths = sorted(previous.directory.glob("replenishment-acquisition*.json"))
    if not acquisition_paths:
        return None
    sources: list[str] = []
    for path in acquisition_paths:
        sources.extend(
            replenishment_source_path(source)
            for source in validate_acquisition_results(load_json(path))
        )
    if not sources:
        return None
    client = _execution_alist_client()
    for source in sources:
        rows = client.list(source, refresh=True)
        if not isinstance(rows, list) or not rows:
            return None
    plan, _digest = unwrap_media_plan(load_json(previous.directory / "media-plan.json"))
    selection_paths = sorted(previous.directory.glob("replenishment-selection*.json"))
    media: dict[str, Any] | None = None
    if selection_paths:
        wrapper = load_json(selection_paths[-1])
        request = wrapper.get("request")
        if isinstance(request, dict) and isinstance(request.get("media"), dict):
            media = dict(request["media"])
    followups = [
        create_replenishment_followup(previous, source, plan, media)
        for source in sources
    ]
    repaired = dict(replenishment)
    repaired.update({
        "status": "acquired",
        "message": "已复用到盘核验通过的查补目录，未重复下载",
        "source_paths": sources,
        "followup_job_ids": [job.id for job in followups],
        "followup_job_id": followups[0].id,
    })
    updated_summary = dict(summary)
    updated_summary["replenishment"] = repaired
    with LOCK:
        previous.phase = "replenishing"
        previous.error = None
        previous.plan_summary = updated_summary
        previous.progress = {
            "stage": "replenishment_followup", "completed": 0,
            "total": len(followups), "percent": 97.0,
            "message": "查补文件已到盘，正在等待自动整理与再次审计",
        }
        previous.updated_at = utc_now()
        persist_job(previous)
    append_log(previous, "复用已到盘查补文件并创建后续整理任务；根任务继续等待闭环。")
    _launch_replenishment_followup_monitor(previous, [item.id for item in followups])
    return previous


def _post_commit_replenishment_retryable(previous: Job) -> bool:
    """Prove retry can resume after commit without replanning media writes."""
    if previous.phase not in {
        "failed", "completed", "cancelled", "recovery_required", "planning_recovery",
        "awaiting_recovery_approval", "starting_recovery_execution",
        "executing_recovery",
    }:
        return False
    if not (previous.directory / "media-plan.json").is_file():
        return False
    if not media_journal_succeeded(previous):
        return False
    summary = previous.plan_summary if isinstance(previous.plan_summary, dict) else {}
    replenishment = summary.get("replenishment")
    status = str(replenishment.get("status") or "") if isinstance(replenishment, dict) else ""
    if status in {
        "acquired", "no_regular_gaps", "sources_exhausted", "awaiting_sources",
        "scrape_first_wait",
    }:
        return False
    # ``success=true`` is the authoritative mutation boundary.  An older
    # image could crash before persisting any replenishment summary/progress;
    # requiring those weaker fields sent the already-committed journal into a
    # recovery loop on every restart.
    return True


def _resume_post_commit_replenishment(
    previous: Job, *, restored: bool = False,
) -> bool:
    """Resume only post-commit audit/search from a successful media journal."""
    if not _post_commit_replenishment_retryable(previous):
        return False
    gate = scrape_first_gate_evidence(previous)
    if gate.get("ready") is not True:
        return _enter_scrape_first_wait(
            previous, summary=dict(previous.plan_summary or {}),
            gate=gate, restored=restored,
        )
    with LOCK:
        previous.phase = "replenishing"
        previous.error = None
        previous.digest = None
        previous.process = None
        previous.cancel_requested = False
        previous.force_killed = False
        previous.progress = {
            "stage": "replenishment_search", "completed": 0, "total": 1,
            "percent": 94.0,
            "message": "媒体已成功提交；仅重跑强制刷新后的缺项复核",
        }
        previous.updated_at = utc_now()
        persist_job(previous)
    _append_post_commit_resume_log_once(
        previous,
        ("服务恢复发现" if restored else "重试发现")
        + "媒体 journal 已完整成功；不重新规划或移动源，仅续跑完成后缺项复核。",
    )
    start_thread(finalize_media_replenishment, previous)
    return True


def retry_job(previous: Job, payload: dict[str, Any] | None = None) -> Job:
    require_nonlegacy_job_mutation(previous)
    if _resume_scrape_first_wait(previous):
        return previous
    if _resume_post_commit_replenishment(previous):
        return previous
    if previous.phase == "failed" and not previous.episode_map:
        _inherit_replenishment_followup_evidence(previous, launch_monitor=True)
    retry_settings = validated_retry_settings(previous, payload)
    original_source = previous.source
    original_parent = previous.parent
    resolved_source = original_source
    try:
        parent_result = browse_remote(posixpath.dirname(original_source), refresh=True)
        original_name = original_source.rsplit("/", 1)[-1]

        def retry_name_key(value: str) -> str:
            # Quark appends ``(1)``/``（1）`` when a same-name folder is recreated.
            return re.sub(r"\s*[（(]\d+[)）]\s*$", "", value).casefold()

        aliases = [
            item["path"]
            for item in parent_result["directories"]
            if retry_name_key(item["name"]) == retry_name_key(original_name)
        ]
        if original_source not in aliases and len(aliases) == 1:
            resolved_source = aliases[0]
    except Exception:
        # The planning command will retain and report the exact AList error.
        pass
    archives_prepared = archive_journal_succeeded(previous)
    retry_failed_archive = (
        (previous.directory / "archive-journal.json").exists()
        and not archives_prepared
        and archive_journal_can_retry(previous)
    )
    archive_resume_digest = (
        canonical_digest(load_json(previous.directory / "archive-plan.json"))
        if retry_failed_archive
        else None
    )
    retry_failed_media = media_journal_can_retry_without_recovery(previous)
    with SCRAPE_FIRST_TRANSITION_LOCK, LOCK:
        if previous.phase not in {"failed", "awaiting_media_approval", "cancelled"} and not (
            previous.phase == "recovery_required" and retry_failed_media
        ):
            raise ValueError("只有失败、已取消任务或待审核计划可以重新整理")
        if (previous.directory / "media-journal.json").exists() and not retry_failed_media:
            raise ValueError("任务存在待恢复记录，请先完成安全恢复")
        if (
            (previous.directory / "archive-journal.json").exists()
            and not archives_prepared
            and not retry_failed_archive
        ):
            raise ValueError("任务存在未完成的解压记录，不能直接重新整理")
        cleanup_files = ["media-plan.json", "episode-map.json", "collection-map.json"]
        if not archives_prepared and not retry_failed_archive:
            cleanup_files.append("archive-plan.json")
        for filename in cleanup_files:
            (previous.directory / filename).unlink(missing_ok=True)
        if retry_failed_media:
            failed_media_journal = previous.directory / "media-journal.json"
            media_history_path = next_attempt_artifact(
                previous.directory / "media-journal-failed.json"
            )
            failed_media_journal.replace(media_history_path)
            lifecycle_path = (
                previous.directory / "hybrid-transaction-lifecycle.json"
            )
            if lifecycle_path.exists():
                lifecycle_history = next_attempt_artifact(
                    previous.directory
                    / "hybrid-transaction-lifecycle-restored.json"
                )
                lifecycle_path.replace(lifecycle_history)
        previous.phase = (
            "starting_archive_execution" if retry_failed_archive else "queued"
        )
        previous.error = None
        previous.digest = archive_resume_digest
        previous.approval_source = None
        previous.plan_summary = None
        previous.process = None
        previous.cancel_requested = False
        previous.force_killed = False
        previous.source = resolved_source
        previous.parent = original_parent
        previous.tmdb_id = retry_settings["tmdb_id"]
        previous.query = retry_settings["query"]
        previous.season = retry_settings["season"]
        previous.media_type = retry_settings["media_type"]
        if retry_settings["archive_password_provided"]:
            password_path = previous.directory / ".archive-password"
            if retry_settings["archive_password"] is None:
                password_path.unlink(missing_ok=True)
            else:
                password_path.write_text(retry_settings["archive_password"], encoding="utf-8")
                os.chmod(password_path, 0o600)
        if not retry_failed_archive:
            previous.logs.clear()
            previous.log_path.unlink(missing_ok=True)
        previous.updated_at = utc_now()
        persist_job(previous)
    append_log(previous, "重新整理已启动，沿用原任务记录。")
    if retry_failed_media:
        append_log(previous, "上次仅在整理锁阶段停止，未移动媒体；已保留失败 journal 并安全重试。")
    if resolved_source != original_source:
        append_log(previous, f"检测到目录已改名，已继续使用当前路径: {resolved_source}")
    if archives_prepared:
        append_log(previous, "上次解压已安全完成，本次直接重新识别媒体，不会重复解压。")
        start_thread(plan_media, previous)
    elif retry_failed_archive and archive_resume_digest:
        append_log(previous, "已验证解压计划和断点记录，将跳过已完成归档后续跑。")
        start_execution(execute_archive, previous, archive_resume_digest)
    else:
        start_thread(prepare_job, previous)
    return previous


def approve_job(job: Job, payload: dict[str, Any]) -> None:
    require_nonlegacy_job_mutation(job)
    digest = payload.get("digest")
    if not isinstance(digest, str) or not re.fullmatch(r"[0-9a-f]{64}", digest):
        raise ValueError("批准摘要必须是完整的 64 位 SHA-256")
    with LOCK:
        if job.digest != digest:
            raise ValueError("批准摘要与当前计划不一致，请刷新后重试")
        if job.phase == "awaiting_media_approval":
            plan, plan_sha256 = unwrap_media_plan(
                load_json(job.directory / "media-plan.json"),
            )
            if not secrets.compare_digest(plan_sha256, digest):
                raise ValueError("批准摘要与磁盘计划不一致，请刷新后重试")
            _require_problem_free_media_plan(plan)
            job.phase = "starting_media_execution"
            job.approval_source = "manual"
            target = execute_approved_media
        elif job.phase == "awaiting_recovery_approval":
            job.phase = "starting_recovery_execution"
            target = execute_approved_recovery
        else:
            raise ValueError("当前任务没有等待批准的计划")
        job.updated_at = utc_now()
        persist_job(job)
    start_execution(target, job, digest)


def delete_job(job: Job) -> None:
    """Delete only disposable local task state; never touch remote media."""
    require_nonlegacy_job_mutation(job)
    with LOCK:
        if job.process is not None and job.process.poll() is None:
            raise ValueError("任务仍在执行，请先安全停止后再删除")
        if (
            (job.directory / "media-journal.json").exists()
            and job.phase not in {"completed", "recovered"}
        ):
            try:
                lifecycle = _load_terminal_transaction_lifecycle(job)
                safely_restored = bool(
                    lifecycle is not None
                    and lifecycle["outcome"] == "restored"
                    and not _transaction_lifecycle_cleanup_blockers(job)
                )
            except (OSError, ValueError, TypeError, json.JSONDecodeError):
                safely_restored = False
            if not safely_restored:
                raise ValueError("任务仍有待恢复记录，完成安全恢复前不能删除")
        if job.phase not in TERMINAL_PHASES | {"awaiting_media_approval"}:
            raise ValueError("当前任务不能直接删除，请先停止或完成恢复")
        directory = _disposable_job_directory(job)
        forget_completed_job(job)
        if directory.exists():
            shutil.rmtree(directory)
        JOBS.pop(job.id, None)


def health_payload() -> dict[str, Any]:
    alist_password = os.getenv("ALIST_PASSWORD")
    tmdb_key = os.getenv("TMDB_API_KEY")
    result: dict[str, Any] = {
        "tmdb_configured": bool(tmdb_key),
        "connected": False,
        "quark_helper": quark_helper_health_payload(),
        "global_control": global_control_status(),
    }
    if not alist_password:
        result["message"] = "缺少 ALIST_PASSWORD"
        return result
    try:
        sys.path.insert(0, str(ENGINE_ROOT))
        from scraper import AListClient  # pylint: disable=import-outside-toplevel

        client = AListClient(
            alist_url(),
            os.getenv("ALIST_USERNAME", "admin"),
            alist_password,
            timeout=5,
            retries=0,
            allow_insecure_http=docker_loopback_bridge(),
        )
        client.login()
        version = client.server_version()
        match = re.search(r"(\d+)\.(\d+)\.(\d+)", version)
        archive_supported = bool(match and tuple(map(int, match.groups())) >= (3, 57, 0))
        result["connected"] = True
        result["message"] = (
            "本地引擎与 AList 已连接"
            if archive_supported
            else "AList 已连接，但版本过旧，不能安全地服务器端解压"
        )
    except Exception as exc:  # health endpoint must return diagnostic JSON
        result["message"] = f"AList 连接失败: {redact(str(exc))}"
    return result


def quark_helper_health_payload() -> dict[str, Any]:
    helper_url = os.getenv("SCRAPEFLOW_QUARK_HELPER_URL", "").strip().rstrip("/")
    token = os.getenv("SCRAPEFLOW_QUARK_HELPER_TOKEN", "")
    result: dict[str, Any] = {
        "configured": bool(helper_url and token),
        "reachable": False,
        "native_ready": False,
    }
    if not result["configured"]:
        return result
    try:
        request = Request(
            helper_url + "/health/passive",
            headers={"Authorization": "Bearer " + token},
        )
        with urlopen(request, timeout=4) as response:
            value = json.load(response)
        if not isinstance(value, Mapping):
            raise ValueError("helper health is not an object")
        result.update({
            "reachable": True,
            "native_ready": (
                response.status == 200
                and value.get("status") == "ok"
                and value.get("runtime") == "connected"
            ),
            "service": value.get("service"),
            "build_id": value.get("build_id"),
            "journal": value.get("journal"),
        })
    except Exception as exc:
        result["error_type"] = type(exc).__name__
    return result


class Handler(BaseHTTPRequestHandler):
    server_version = "ScrapeFlowLocal/1.0"

    def log_message(self, format_string: str, *args: Any) -> None:
        print(f"[local-api] {format_string % args}")

    def _origin(self) -> str | None:
        origin = self.headers.get("Origin")
        if not origin:
            return None
        try:
            parsed = urlsplit(origin)
        except ValueError:
            return None
        host = self.headers.get("Host", "")
        if (
            parsed.scheme not in {"http", "https"}
            or parsed.username is not None
            or parsed.password is not None
            or parsed.path not in {"", "/"}
            or parsed.query
            or parsed.fragment
            or parsed.netloc.casefold() != host.casefold()
        ):
            return None
        return origin.rstrip("/")

    def _host_allowed(self) -> bool:
        return bool(ALLOWED_HOST_RE.fullmatch(self.headers.get("Host", "")))

    def _local_same_origin_allowed(self) -> bool:
        if not self._host_allowed():
            return False
        origin = self.headers.get("Origin")
        if origin and not self._origin():
            return False
        fetch_site = self.headers.get("Sec-Fetch-Site", "").strip().casefold()
        return fetch_site != "cross-site"

    def _reject_nonlocal_request(self) -> bool:
        if not self._local_same_origin_allowed():
            self._send(HTTPStatus.FORBIDDEN, {"error": "仅允许本机 ScrapeFlow 页面访问"})
            return True
        return False

    def _send(self, status: int, payload: dict[str, Any]) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _json_body(self) -> dict[str, Any]:
        content_type = self.headers.get("Content-Type", "").split(";", 1)[0].strip().lower()
        if content_type != "application/json":
            raise ValueError("请求 Content-Type 必须是 application/json")
        try:
            length = int(self.headers.get("Content-Length", "0"))
        except ValueError as exc:
            raise ValueError("请求长度无效") from exc
        if length <= 0 or length > MAX_BODY_BYTES:
            raise ValueError("请求内容为空或过大")
        value = strict_json_loads(self.rfile.read(length).decode("utf-8"))
        if not isinstance(value, dict):
            raise ValueError("请求必须是 JSON 对象")
        return value

    def do_GET(self) -> None:  # noqa: N802
        if self._reject_nonlocal_request():
            return
        path = urlsplit(self.path).path
        if path == "/api/health":
            self._send(HTTPStatus.OK, health_payload())
            return
        if path == "/api/control":
            self._send(HTTPStatus.OK, global_control_status())
            return
        if path == "/api/jobs":
            with LOCK:
                jobs = [
                    job.public(include_plan=False)
                    for job in sorted(JOBS.values(), key=lambda row: row.created_at, reverse=True)
                    if job.visibility != "internal"
                ]
            self._send(HTTPStatus.OK, {"jobs": jobs})
            return
        if path == "/api/browse":
            query = parse_qs(urlsplit(self.path).query, keep_blank_values=True)
            browse_path = (query.get("path") or [MEDIA_LIBRARY_ROOT])[0]
            force_refresh = (query.get("refresh") or ["0"])[0] == "1"
            try:
                self._send(HTTPStatus.OK, browse_remote(browse_path, refresh=force_refresh))
            except (ValueError, OSError) as exc:
                self._send(HTTPStatus.BAD_REQUEST, {"error": redact(str(exc))})
            except Exception as exc:
                print(f"[local-api] AList 浏览失败: {redact(str(exc))}", file=sys.stderr)
                self._send(HTTPStatus.BAD_GATEWAY, {"error": "AList 目录读取失败，请检查连接"})
            return
        match = re.fullmatch(r"/api/jobs/([0-9a-f]{12})", path)
        if match:
            with LOCK:
                job = JOBS.get(match.group(1))
                payload = job.public() if job else None
            if payload is None:
                self._send(HTTPStatus.NOT_FOUND, {"error": "任务不存在"})
            else:
                self._send(HTTPStatus.OK, {"job": payload})
            return
        self._send(HTTPStatus.NOT_FOUND, {"error": "接口不存在"})

    def do_POST(self) -> None:  # noqa: N802
        if self._reject_nonlocal_request():
            return
        path = urlsplit(self.path).path
        try:
            payload = self._json_body()
            if path == "/api/jobs":
                job = create_job(payload)
                self._send(HTTPStatus.ACCEPTED, {"job": job.public()})
                return
            if path in {"/api/control/pause", "/api/control/resume"}:
                if payload.get("confirm") is not True:
                    raise ValueError("全局暂停或恢复需要明确确认")
                paused = path.endswith("/pause")
                reason = payload.get("reason")
                if reason is not None and not isinstance(reason, str):
                    raise ValueError("全局暂停原因格式无效")
                self._send(
                    HTTPStatus.OK,
                    set_global_pause(paused, reason=reason if paused else None),
                )
                return
            match = re.fullmatch(
                r"/api/jobs/([0-9a-f]{12})/"
                r"(approve|recover|retry|cancel|resolve)",
                path,
            )
            if not match:
                self._send(HTTPStatus.NOT_FOUND, {"error": "接口不存在"})
                return
            with LOCK:
                job = JOBS.get(match.group(1))
            if job is None:
                self._send(HTTPStatus.NOT_FOUND, {"error": "任务不存在"})
                return
            if match.group(2) == "approve":
                approve_job(job, payload)
            elif match.group(2) == "recover":
                if payload.get("confirm") is not True:
                    raise ValueError("恢复检查需要明确确认")
                request_recovery(job)
            elif match.group(2) == "cancel":
                if payload.get("confirm") is not True:
                    raise ValueError("安全停止需要明确确认")
                request_cancel(job)
            elif match.group(2) == "resolve":
                resolve_failed_job(job, payload)
            else:
                job = retry_job(job, payload)
            self._send(HTTPStatus.ACCEPTED, {"job": job.public()})
        except ExistingJobConflict as exc:
            self._send(
                HTTPStatus.CONFLICT,
                {"error": redact(str(exc)), "job": exc.job.public()},
            )
        except (ValueError, json.JSONDecodeError) as exc:
            self._send(HTTPStatus.BAD_REQUEST, {"error": redact(str(exc))})
        except Exception as exc:
            print(f"[local-api] 写请求异常: {redact(str(exc))}", file=sys.stderr)
            self._send(HTTPStatus.INTERNAL_SERVER_ERROR, {"error": "本地服务发生异常，请查看服务日志"})

    def do_DELETE(self) -> None:  # noqa: N802
        if self._reject_nonlocal_request():
            return
        path = urlsplit(self.path).path
        try:
            if path == "/api/jobs":
                removed = clear_local_task_data()
                self._send(HTTPStatus.OK, {"cleared": removed})
                return
            match = re.fullmatch(r"/api/jobs/([0-9a-f]{12})", path)
            if not match:
                self._send(HTTPStatus.NOT_FOUND, {"error": "接口不存在"})
                return
            with LOCK:
                job = JOBS.get(match.group(1))
            if job is None:
                self._send(HTTPStatus.NOT_FOUND, {"error": "任务不存在"})
                return
            delete_job(job)
            self._send(HTTPStatus.OK, {"deleted": job.id})
        except ValueError as exc:
            self._send(HTTPStatus.CONFLICT, {"error": redact(str(exc))})
        except Exception as exc:
            print(f"[local-api] 删除任务异常: {redact(str(exc))}", file=sys.stderr)
            self._send(HTTPStatus.INTERNAL_SERVER_ERROR, {"error": "任务记录删除失败，请查看服务日志"})


def main() -> int:
    SHUTDOWN_EVENT.clear()
    GLOBAL_CONTROL.reload()
    restore_jobs()
    reconcile_transaction_lifecycles_on_startup()
    resume_jobs()
    SCHEDULER.start(run_scheduled)
    SUBTITLE_SOURCE_DISCOVERY_RUNTIME.start(
        _discover_subtitle_source_batch,
        global_control_status,
        item_guard=_subtitle_execution_guard,
    )
    host = os.getenv("SCRAPEFLOW_API_HOST", "127.0.0.1")
    port = int(os.getenv("SCRAPEFLOW_API_PORT", "8765"))
    server = ThreadingHTTPServer((host, port), Handler)
    previous_sigterm = signal.getsignal(signal.SIGTERM)

    def stop_on_sigterm(_signum: int, _frame: Any) -> None:
        raise KeyboardInterrupt

    signal.signal(signal.SIGTERM, stop_on_sigterm)
    print(f"ScrapeFlow 本地 API: http://{host}:{port}", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
        # Process exit has its own non-persistent gate.  Close it while holding
        # the same transition lock used at remote-write boundaries; the
        # durable operator pause remains byte-for-byte unchanged.
        with GLOBAL_CONTROL_TRANSITION_LOCK:
            SHUTDOWN_EVENT.set()
        SCHEDULER.stop(timeout=0)
        SUBTITLE_SOURCE_DISCOVERY_RUNTIME.stop(timeout=0)
        shutdown_running_jobs()
        SUBTITLE_SOURCE_DISCOVERY_RUNTIME.stop(timeout=2)
        SCHEDULER.stop(timeout=2)
        signal.signal(signal.SIGTERM, previous_sigterm)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
