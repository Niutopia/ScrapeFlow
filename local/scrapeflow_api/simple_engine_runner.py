"""Bridge Engine planning to the automatic single-user workflow.

A source is identified and planned, written to AList, read back by exact path
and size, then cleaned according to the plan.  Interrupted writes are
reconciled from AList state before they are retried.
"""

from __future__ import annotations

import contextlib
import contextvars
import errno
import json
import os
import posixpath
import re
import shutil
import time
import uuid
from dataclasses import asdict, dataclass, replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Callable, Iterator, Mapping, Protocol

from engine.scrapeflow.archive import ArchivePasswordError
from engine.scrapeflow.errors import FormalTargetConflictError
from engine.scrapeflow.media_quality import (
    is_production_test_media_path,
    is_video_filename,
    minimum_video_bytes,
    video_size_is_admissible,
)
from engine.scrapeflow.residual_policy import (
    cleanup_allowlist_reason,
    is_task_owned_staging_root,
)
from engine.scrapeflow.serialization import atomic_write_json
from engine.scrapeflow.subtitle_content import (
    DEFAULT_MAX_PREFIX_BYTES,
    classify_subtitle_content,
)
from engine.scrapeflow.target_shelf import (
    TargetShelf,
    parse_target_shelf,
    target_root_for_shelf,
    target_shelf_allows_media_type,
    target_shelf_for_root,
)
from local.scrapeflow_api.replenishment import ACTIONABLE_GAP_KINDS
from local.scrapeflow_api.redaction import redact_error


class SimpleEngineError(RuntimeError):
    """Base error for the small Engine bridge."""


class EngineRequestError(SimpleEngineError, ValueError):
    """The plan request is malformed or incomplete."""


class EngineExecutionError(SimpleEngineError):
    """A simple plan operation failed or could not be read back."""


class EngineCancellationRequested(EngineExecutionError):
    """A durable operator cancellation reached a safe Engine boundary."""


class EnginePauseRequested(EngineExecutionError):
    """The global pause fence reached a safe Engine boundary.

    Pause is deliberately distinct from cancellation: the current durable
    operation remains resumable and is not converted to a terminal state.
    """


class EngineJobConflictError(EngineExecutionError):
    """A valid request conflicts with durable job/source state."""


class TargetShelfPolicyConflictError(EngineJobConflictError):
    """TMDB media type conflicts with the user's selected target shelf."""

    def __init__(
        self,
        *,
        target_shelf: TargetShelf | str,
        media_type: str,
        identity: object | None = None,
    ) -> None:
        selected = parse_target_shelf(target_shelf)
        super().__init__(
            "TMDB 识别结果与用户选择的目标货架冲突: "
            f"{media_type or 'unknown'} 不能进入 {selected.value}"
        )
        self.target_shelf = selected.value
        self.media_type = media_type
        self.identity = identity


class EngineRecoveryMatrixError(EngineExecutionError):
    """A restart readback found a durable, non-retryable state conflict.

    Recovery deliberately distinguishes a provider visibility/transport error
    (which may be retried) from facts that cannot be repaired by replaying the
    same plan.  The latter are persisted as a terminal verification failure so
    a scheduler cannot keep submitting an operation that might overwrite a
    different object or silently recreate a lost source.
    """

    def __init__(
        self,
        reason: str,
        message: str,
        *,
        source: str | None = None,
        target: str | None = None,
        expected_size: int | None = None,
        actual_size: int | None = None,
    ) -> None:
        super().__init__(message)
        self.reason = reason
        self.source = source
        self.target = target
        self.expected_size = expected_size
        self.actual_size = actual_size


class EngineRecoveryRetryableError(EngineExecutionError):
    """A restart readback fact is known but safe to retry later."""

    def __init__(
        self,
        reason: str,
        message: str,
        *,
        source: str | None = None,
        target: str | None = None,
    ) -> None:
        super().__init__(message)
        self.reason = reason
        self.source = source
        self.target = target


class EngineJobNotFoundError(SimpleEngineError):
    """The requested persisted Engine job does not exist."""


class EngineWorkerBusyError(SimpleEngineError):
    """Another process is currently executing or reconciling an Engine job."""


def _require_admissible_video_size(
    item: object,
    size: object,
    *,
    path: str,
    stage: str,
) -> None:
    """Reject an undersized planned video before formal-library acceptance.

    AList exact-size readback proves transport consistency, not that a tiny
    test fixture or error page is a media payload.  This guard is deliberately
    repeated at execution and recovery, because persisted plans may predate
    plan-time validation or have been interrupted after a move.
    """
    if not (
        getattr(item, "media_kind", None) == "video"
        or is_video_filename(getattr(item, "original_name", None))
        or is_video_filename(path)
    ):
        return
    if not video_size_is_admissible(size):
        raise EngineExecutionError(
            f"{stage}拒绝小于正式库准入下限 {minimum_video_bytes()} bytes 的视频: {path}"
        )


def _require_non_test_media_path(path: str, *, stage: str) -> None:
    """Keep legacy production E2E source trees outside the formal writer."""
    if is_production_test_media_path(path):
        raise EngineExecutionError(
            f"{stage}拒绝保留的生产 E2E 测试来源路径: {path}"
        )


@dataclass(frozen=True, slots=True)
class AutomaticIdentity:
    """Machine-selected identity used by an automatic job."""

    media_type: str
    tmdb_id: int
    title: str
    year: str
    confidence: float
    # Read-only reconciliation deliberately has no destination yet.  The
    # post-selection planning adapter fills this field after /start.
    target_parent: str | None
    season: int | None
    trace: Mapping[str, object]
    target_shelf: str | None = None
    # ``target_root`` historically means the concrete planned work directory
    # in identity/audit consumers.  Keep the selected first-level shelf under
    # an unambiguous name so it can never widen a scoped audit to a whole
    # library category.
    target_shelf_root: str | None = None

    def as_dict(self) -> dict[str, object]:
        return {
            "media_type": self.media_type,
            "tmdb_id": self.tmdb_id,
            "title": self.title,
            "year": self.year,
            "confidence": self.confidence,
            "target_parent": self.target_parent,
            "season": self.season,
            "trace": dict(self.trace),
            "target_shelf": self.target_shelf,
            "target_shelf_root": self.target_shelf_root,
        }


_JOB_ID_RE = re.compile(r"\A[a-zA-Z0-9][a-zA-Z0-9._-]{0,127}\Z")
_MEDIA_TYPES = frozenset({"auto", "movie", "tv", "collection"})
_AUDIT_ROOT_GAP_KINDS = frozenset({
    "missing_media", "missing_episode", "missing_season",
})
# A subtitle-only audit root is a local provider parent for an already
# visible, identity-verified video.  It must never be accepted by the media
# child planner as an episode/movie acquisition request.
_AUDIT_SUBTITLE_GAP_KINDS = frozenset({"missing_subtitle"})

# Automatic roots keep their ingress/archive cleanup evidence until the
# audit/provider lifecycle has reached a durable terminal decision.  A
# ContextVar carries that one-shot execution policy through injected executor
# wrappers without changing the public Plan model or forcing every test/
# provider executor to accept a new keyword argument.
_DEFER_TASK_CLEANUP: contextvars.ContextVar[bool] = contextvars.ContextVar(
    "scrapeflow_defer_task_cleanup", default=False,
)
# The executor deliberately receives no new public argument.  A runner-owned
# ContextVar lets the concrete executor stop between remote operations while
# preserving the existing injected-executor protocol.
_CANCEL_REQUEST_CHECK: contextvars.ContextVar[Callable[[], bool] | None] = (
    contextvars.ContextVar("scrapeflow_cancel_request_check", default=None)
)
_PAUSE_REQUEST_CHECK: contextvars.ContextVar[Callable[[], bool] | None] = (
    contextvars.ContextVar("scrapeflow_pause_request_check", default=None)
)


def _cancellation_checkpoint() -> None:
    """Stop only at an operation boundary when pause or cancel is requested."""
    checker = _CANCEL_REQUEST_CHECK.get()
    if callable(checker) and checker():
        raise EngineCancellationRequested("操作员已请求取消；当前远端操作完成后停止任务")
    pause_checker = _PAUSE_REQUEST_CHECK.get()
    if callable(pause_checker):
        try:
            paused = bool(pause_checker())
        except Exception as exc:
            raise EnginePauseRequested("暂停状态不可确认，已安全停止") from exc
        if paused:
            raise EnginePauseRequested("全局暂停已生效；当前远端操作完成后停止任务")


def _pause_checkpoint(checker: Callable[[], bool] | None) -> None:
    """Apply an optional composition-root pause fence at a phase boundary."""
    if checker is None:
        return
    try:
        paused = bool(checker())
    except Exception as exc:
        raise EnginePauseRequested("暂停状态不可确认，已安全停止") from exc
    if paused:
        raise EnginePauseRequested("全局暂停已生效；当前阶段保持可恢复")


_ENGINE_PHASES = frozenset({
    "reconciling", "reconciled", "reconciliation_uncertain",
    "awaiting_target_shelf", "queued", "analyzing", "archive_preprocessing", "identity_matching",
    "target_policy_conflict", "planning", "planned",
    "executing", "verifying", "cleaning", "executed", "completed",
    "retry_wait", "failed", "failed_archive", "failed_identity", "failed_planning", "failed_provider",
    "failed_write", "failed_verification", "failed_cleanup", "cancelled",
})

# A cleanup request is intentionally narrower than a general state migration.
# ``executed`` is the durable Engine write fact, while the application may keep
# a provider/audit projection beside it; ``cleanup_terminal_job`` checks both
# before removing any task-owned local state.
_CLEANUP_TERMINAL_PHASES = frozenset({
    "executed", "completed", "failed", "failed_archive", "failed_identity", "failed_planning", "failed_provider",
    "failed_write", "failed_verification", "failed_cleanup", "cancelled",
})
_CLEANUP_ACTIVE_PROVIDER_STATUSES = frozenset({
    "gap_discovering", "provider_searching", "acquiring", "staging_verifying",
    "subtitle_installing", "child_planning", "child_executing", "final_verifying",
    "cleaning", "child_failed", "retry_wait",
})
_CLEANUP_RETRYABLE_AUDIT_STATUSES = frozenset({
    "pending", "repairing", "retry_wait", "unknown", "blocked", "failed",
    "failed_provider",
})
# A duplicate-complete intake has no formal Engine write and therefore has
# its own, deliberately small source-consumption gate.  Keep this list local
# to the runner rather than importing the Provider runtime (which imports the
# runner in a few deployment compositions).
_DUPLICATE_TERMINAL_PROVIDER_STATUSES = frozenset({
    "completed", "resolved", "ready", "skipped", "deferred", "no_gap", "terminal",
})
_DUPLICATE_ACTIVE_PROVIDER_STATUSES = frozenset({
    "gap_discovering", "provider_searching", "acquiring", "staging_verifying",
    "subtitle_installing", "child_planning", "child_executing", "final_verifying",
    "cleaning", "child_failed", "retry_wait", "waiting_reconcile", "in_doubt",
    "pending", "repairing",
})
_DUPLICATE_IN_DOUBT_SCOPES = frozenset({"in_doubt", "failure_in_doubt"})
# Provider attempt staging is held in one gap JSON marker after a successful
# child until a later scoped audit proves the selected gap disappeared.  Keep
# this literal local to the runner to avoid importing the provider runtime
# (which intentionally imports this module) and creating a circular cleanup
# dependency.
_POST_ACQUISITION_REAUDIT_KEY = "post_acquisition_reaudit"

# Provider replenishment children are deliberately narrower than ordinary
# Engine roots: they only carry the missing media member into the formal
# library.  Their existing NFO/artwork belongs to the already-audited work
# root and must never be generated or replaced as a side effect of a child
# retry.  Keep this as a plan-metadata marker so the persisted child retains
# the same execution/recovery semantics after a process restart.
_PROVIDER_MEDIA_ONLY_METADATA_KEY = "provider_media_only"
_PROVIDER_EPISODE_TOKEN_RE = re.compile(
    r"(?i)(?<![a-z0-9])s0*(\d{1,3})[ ._-]*e(?:p)?0*(\d{1,4})(?!\d)"
)
_PROVIDER_SUPPLEMENTAL_MEMBER_RE = re.compile(
    r"(?i)(?:^|[/\\\s._\-\[\](){}])"
    r"(?:bonus(?:es)?|extra(?:s)?|sample(?:s)?|scan(?:s)?|"
    r"menu|preview(?:s)?|trailer(?:s)?|teaser(?:s)?|featurette(?:s)?|"
    r"behind[ ._\-]*the[ ._\-]*scenes|"
    r"ncop|nced|pv|cm|creditless|op|ed)"
    r"(?=$|[/\\\s._\-\[\](){}])"
)


def _provider_episode_coordinates(value: object) -> set[str]:
    """Return explicit SxxEyy coordinates carried by one child filename."""
    return {
        f"S{int(match.group(1)):02d}E{int(match.group(2)):02d}"
        for match in _PROVIDER_EPISODE_TOKEN_RE.finditer(str(value or ""))
        if 0 <= int(match.group(1)) <= 999 and 0 < int(match.group(2)) <= 9999
    }


def _provider_member_is_supplemental(*values: object) -> bool:
    return any(_PROVIDER_SUPPLEMENTAL_MEMBER_RE.search(str(value or "")) for value in values)


def _provider_tv_child_primary_video_errors(plan: object) -> list[str]:
    """Return serialization-stable violations for a provider TV child.

    Adapter validation proves why a manifest member maps to a gap.  Once that
    choice has been serialized into an Engine child, this runner still has to
    prove the durable plan contains one ordinary video for one episode and no
    duplicate coordinate.  The check deliberately reads source/original and
    final names, so changing one JSON field cannot relabel a bonus or route a
    source episode into another target episode.
    """
    mode = str(getattr(plan, "mode", "") or "").casefold()
    metadata = getattr(plan, "metadata", None)
    media_type = (
        str(metadata.get("media_type") or metadata.get("type") or "").casefold()
        if isinstance(metadata, Mapping) else ""
    )
    if mode != "tv" and media_type != "tv":
        return []
    files = [
        item for item in list(getattr(plan, "files", ()) or ())
        if getattr(item, "media_kind", None) == "video"
    ]
    if not files:
        return ["provider TV child 没有视频"]
    owners: dict[str, list[str]] = {}
    errors: list[str] = []
    for item in files:
        source_path = str(getattr(item, "source_path", "") or "")
        original_name = str(getattr(item, "original_name", "") or "")
        final_name = str(getattr(item, "final_name", "") or "")
        label = original_name or source_path or final_name or "<unknown>"
        if not all(is_video_filename(value) for value in (source_path, original_name, final_name)):
            errors.append(f"provider TV child 包含非视频主文件: {label}")
            continue
        if _provider_member_is_supplemental(source_path, original_name, final_name):
            errors.append(f"provider TV child 包含附加内容: {label}")
            continue
        source_ids = _provider_episode_coordinates(Path(source_path).name)
        original_ids = _provider_episode_coordinates(original_name)
        final_ids = _provider_episode_coordinates(final_name)
        if (
            len(source_ids) != 1
            or original_ids != source_ids
            or final_ids != source_ids
        ):
            errors.append(f"provider TV child 缺少一致的唯一 episode 映射: {label}")
            continue
        episode_id = next(iter(source_ids))
        owners.setdefault(episode_id, []).append(label)
    for episode_id, labels in owners.items():
        if len(labels) > 1:
            errors.append(
                f"provider TV child 将多个视频映射到 {episode_id}: "
                + ", ".join(labels[:3])
            )
    return errors


def _require_provider_tv_child_primary_videos(
    plan: object, *, stage: str,
) -> None:
    errors = _provider_tv_child_primary_video_errors(plan)
    if errors:
        raise ValueError(f"{stage}拒绝不唯一/非正片 provider TV child: {errors[0]}")


def _is_provider_media_only_plan(plan: object) -> bool:
    """Return whether a persisted plan is an internal provider media child."""
    metadata = getattr(plan, "metadata", None)
    return (
        isinstance(metadata, Mapping)
        and metadata.get(_PROVIDER_MEDIA_ONLY_METADATA_KEY) is True
    )


def _require_problem_free_plan(plan: object, *, stage: str) -> None:
    """Refuse every formal-write path while a plan still has open problems.

    Planner validation is useful but not a write boundary: persisted plans can
    predate a validation change and tests/providers may inject their own
    executor.  Keep this guard in the runner as well as the concrete executor
    so changing ``executor=`` cannot turn a problem-bearing plan into a move,
    upload, or cleanup operation.
    """
    problems = list(getattr(plan, "problem_files", ()) or ())
    if not problems:
        return
    first = problems[0]
    path = str(getattr(first, "source_path", "") or "<unknown>")
    reason = str(getattr(first, "reason", "") or "")
    detail = f": {path}" + (f"（{reason}）" if reason else "")
    raise EngineExecutionError(
        f"{stage}拒绝含有 {len(problems)} 个未闭合问题文件的计划{detail}"
    )


def _require_cleanup_allowlist(plan: object, *, stage: str) -> None:
    """Verify cleanup rows before any executor can issue an AList delete.

    This is intentionally independent from plan-time validation. A persisted
    pre-convergence plan, or an injected executor, must not regain authority
    to delete a font/PDF/theme video merely because its old cleanup reason
    looks familiar.
    """
    raw_root = getattr(plan, "source_root", None)
    source_root = _safe_remote_path(
        raw_root,
        field="cleanup source_root",
        allow_root=False,
    )
    source_prefix = source_root.rstrip("/") + "/"
    for item in list(getattr(plan, "cleanup_files", ()) or ()):
        raw_path = getattr(item, "source_path", None)
        raw_dir = getattr(item, "source_dir", None)
        raw_name = getattr(item, "original_name", None)
        raw_reason = getattr(item, "reason", None)
        source_path = _safe_remote_path(
            raw_path,
            field="cleanup source_path",
            allow_root=False,
        )
        source_dir = _safe_remote_path(
            raw_dir,
            field="cleanup source_dir",
            allow_root=False,
        )
        if (
            not isinstance(raw_name, str)
            or not raw_name
            or "/" in raw_name
            or "\\" in raw_name
            or raw_name in {".", ".."}
            or posixpath.join(source_dir, raw_name) != source_path
        ):
            raise EngineExecutionError(f"{stage}拒绝不一致的清理路径: {source_path}")
        if source_path != source_root and not source_path.startswith(source_prefix):
            raise EngineExecutionError(f"{stage}拒绝任务来源外的清理项: {source_path}")
        expected_reason = cleanup_allowlist_reason(
            source_path,
            task_root=source_root,
        )
        if expected_reason is None or raw_reason != expected_reason:
            raise EngineExecutionError(
                f"{stage}拒绝非白名单或非任务自有清理项: {source_path}"
            )


def _mark_provider_media_only_body(body: Mapping[str, object]) -> dict[str, object]:
    """Copy a serialized plan and mark it as media-only for provider children."""
    marked = dict(body)
    metadata = body.get("metadata")
    if not isinstance(metadata, Mapping):
        raise SimpleEngineError("Engine child 计划缺少 metadata")
    child_metadata = dict(metadata)
    child_metadata[_PROVIDER_MEDIA_ONLY_METADATA_KEY] = True
    marked["metadata"] = child_metadata
    return marked


@contextlib.contextmanager
def _engine_worker_lock(state_root: Path) -> Iterator[None]:
    """Acquire the one cross-process lock for formal-library writes."""
    try:
        import fcntl
    except ImportError as exc:  # pragma: no cover - the deployment is Unix
        raise SimpleEngineError("Engine 单 worker 锁需要 fcntl") from exc
    locks_root = Path(state_root) / "locks"
    locks_root.mkdir(mode=0o700, parents=True, exist_ok=True)
    descriptor = os.open(locks_root / "worker.lock", os.O_CREAT | os.O_RDWR, 0o600)
    try:
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            if exc.errno in {errno.EACCES, errno.EAGAIN}:
                raise EngineWorkerBusyError("另一个 ScrapeFlow Engine 任务正在执行") from exc
            raise
        yield
    finally:
        try:
            fcntl.flock(descriptor, fcntl.LOCK_UN)
        finally:
            os.close(descriptor)


def _now() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def _safe_remote_path(value: object, *, field: str, allow_root: bool = True) -> str:
    if not isinstance(value, str) or not value.startswith("/") or "\x00" in value:
        raise EngineRequestError(f"{field} 必须是绝对远端路径")
    if "\\" in value:
        raise EngineRequestError(f"{field} 不得包含反斜杠")
    if not allow_root and value == "/":
        raise EngineRequestError(f"{field} 不能是根目录")
    parts = value.split("/")
    if any(part in {"", ".", ".."} for part in parts[1:]):
        raise EngineRequestError(f"{field} 含有不安全的路径段")
    normalized = posixpath.normpath(value)
    if normalized != value or not normalized.startswith("/"):
        raise EngineRequestError(f"{field} 不是规范化绝对路径")
    return normalized


def _safe_job_id(value: object) -> str:
    if not isinstance(value, str) or not _JOB_ID_RE.fullmatch(value):
        raise EngineRequestError("无效的 Engine job id")
    return value


def _bool_option(payload: Mapping[str, object], name: str, default: bool = False) -> bool:
    value = payload.get(name, default)
    if type(value) is not bool:
        raise EngineRequestError(f"{name} 必须是布尔值")
    return value


@dataclass(frozen=True, slots=True)
class EngineRequest:
    """The small request surface needed by the existing Engine planners."""

    source_path: str
    parent_path: str
    media_type: str
    target_shelf: str | None = None
    tmdb_id: int | None = None
    query: str | None = None
    season: int = 1
    absolute: bool = False
    auto_episode_mode: bool = True
    prefer_simplified: bool = True
    allow_unmapped: bool = False
    allow_index_mapping: bool = False
    collection_map: Mapping[str, object] | None = None
    episode_map: Mapping[str, object] | None = None
    episode_group_id: str | None = None
    ignore_orphan_temp: bool = False

    @classmethod
    def from_mapping(cls, payload: Mapping[str, object]) -> "EngineRequest":
        if not isinstance(payload, Mapping):
            raise EngineRequestError("Engine 计划请求必须是 JSON 对象")
        source = _safe_remote_path(payload.get("source_path"), field="source_path", allow_root=False)
        parent = _safe_remote_path(payload.get("parent_path", "/"), field="parent_path")
        media_type = payload.get("media_type", payload.get("type", "auto"))
        if media_type not in _MEDIA_TYPES:
            raise EngineRequestError("media_type 必须是 auto、movie、tv 或 collection")
        raw_target_shelf = payload.get("target_shelf")
        if raw_target_shelf is None:
            target_shelf = None
        else:
            try:
                target_shelf = parse_target_shelf(raw_target_shelf).value
            except ValueError as exc:
                raise EngineRequestError(str(exc)) from exc
        raw_id = payload.get("tmdb_id", payload.get("id"))
        tmdb_id: int | None
        if raw_id is None or raw_id == "":
            tmdb_id = None
        elif isinstance(raw_id, bool) or not isinstance(raw_id, int) or raw_id <= 0:
            # Accept the common JSON form where an ID is sent as a string, but
            # never coerce arbitrary values or booleans.
            if isinstance(raw_id, str) and raw_id.isdigit() and int(raw_id) > 0:
                tmdb_id = int(raw_id)
            else:
                raise EngineRequestError("tmdb_id 必须是正整数")
        else:
            tmdb_id = raw_id
        query = payload.get("query")
        if query is not None and (not isinstance(query, str) or not query.strip()):
            raise EngineRequestError("query 必须是非空字符串")
        raw_season = payload.get("season", 1)
        if isinstance(raw_season, bool) or not isinstance(raw_season, int) or raw_season < 0:
            raise EngineRequestError("season 必须是非负整数")
        collection_map = payload.get("collection_map")
        if collection_map is not None and not isinstance(collection_map, Mapping):
            raise EngineRequestError("collection_map 必须是对象")
        episode_map = payload.get("episode_map")
        if episode_map is not None and not isinstance(episode_map, Mapping):
            raise EngineRequestError("episode_map 必须是对象")
        if episode_map is not None:
            raise EngineRequestError(
                "自动流程不接受内嵌 episode_map；请让 Engine 根据来源和 TMDB 自动推断"
            )
        episode_group = payload.get("episode_group_id", payload.get("episode_group"))
        if episode_group is not None and not isinstance(episode_group, str):
            raise EngineRequestError("episode_group_id 必须是字符串")
        return cls(
            source_path=source,
            parent_path=parent,
            media_type=str(media_type),
            target_shelf=target_shelf,
            tmdb_id=tmdb_id,
            query=query.strip() if isinstance(query, str) else None,
            season=raw_season,
            absolute=_bool_option(payload, "absolute"),
            auto_episode_mode=_bool_option(payload, "auto_episode_mode", True),
            prefer_simplified=_bool_option(payload, "prefer_simplified", True),
            allow_unmapped=_bool_option(payload, "allow_unmapped"),
            allow_index_mapping=_bool_option(payload, "allow_index_mapping"),
            collection_map=dict(collection_map) if isinstance(collection_map, Mapping) else None,
            episode_map=dict(episode_map) if isinstance(episode_map, Mapping) else None,
            episode_group_id=episode_group,
            ignore_orphan_temp=_bool_option(payload, "ignore_orphan_temp"),
        )


@dataclass(frozen=True, slots=True)
class EngineJob:
    """Persisted projection of one planned or executed Engine job."""

    id: str
    phase: str
    created_at: str
    updated_at: str
    request: Mapping[str, object]
    plan: Mapping[str, object]
    summary: Mapping[str, object]
    target_shelf: str | None = None
    target_root: str | None = None
    selected_at: str | None = None
    execution: Mapping[str, object] | None = None
    error: str | None = None

    def as_dict(self) -> dict[str, object]:
        result: dict[str, object] = {
            "id": self.id,
            "phase": self.phase,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "request": dict(self.request),
            "plan": dict(self.plan),
            "summary": dict(self.summary),
            "target_shelf": self.target_shelf,
            "target_root": self.target_root,
            "selected_at": self.selected_at,
        }
        if self.execution is not None:
            result["execution"] = dict(self.execution)
        if self.error is not None:
            # ``EngineJob`` is the durable job-JSON boundary.  Individual
            # failure paths also redact their returned job objects, while
            # this guard keeps a newly added writer from persisting an error
            # string verbatim by accident.
            result["error"] = redact_error(self.error)
        return result

    @classmethod
    def from_dict(cls, raw: Mapping[str, object]) -> "EngineJob":
        job_id = _safe_job_id(raw.get("id"))
        phase = raw.get("phase")
        if phase not in _ENGINE_PHASES:
            raise SimpleEngineError(f"Engine job {job_id} 的 phase 无效")
        for key in ("created_at", "updated_at"):
            if not isinstance(raw.get(key), str):
                raise SimpleEngineError(f"Engine job {job_id} 缺少 {key}")
        request = raw.get("request")
        plan = raw.get("plan")
        summary = raw.get("summary")
        if not isinstance(request, Mapping) or not isinstance(plan, Mapping) or not isinstance(summary, Mapping):
            raise SimpleEngineError(f"Engine job {job_id} 的计划记录格式无效")
        execution = raw.get("execution")
        if execution is not None and not isinstance(execution, Mapping):
            raise SimpleEngineError(f"Engine job {job_id} 的执行记录格式无效")
        error = raw.get("error")
        if error is not None and not isinstance(error, str):
            raise SimpleEngineError(f"Engine job {job_id} 的错误记录格式无效")
        raw_target_shelf = raw.get("target_shelf")
        if raw_target_shelf is None:
            target_shelf = None
        else:
            try:
                target_shelf = parse_target_shelf(raw_target_shelf).value
            except ValueError as exc:
                raise SimpleEngineError(f"Engine job {job_id} 的 target_shelf 无效") from exc
        raw_target_root = raw.get("target_root")
        if raw_target_root is None:
            target_root = None
        else:
            try:
                target_root = _safe_remote_path(
                    raw_target_root,
                    field="target_root",
                    allow_root=False,
                )
            except EngineRequestError as exc:
                raise SimpleEngineError(f"Engine job {job_id} 的 target_root 无效") from exc
        selected_at = raw.get("selected_at")
        if selected_at is not None and not isinstance(selected_at, str):
            raise SimpleEngineError(f"Engine job {job_id} 的 selected_at 记录格式无效")
        if target_shelf is None and (target_root is not None or selected_at is not None):
            raise SimpleEngineError(f"Engine job {job_id} 的目标货架记录不完整")
        if target_shelf is not None and (target_root is None or selected_at is None):
            raise SimpleEngineError(f"Engine job {job_id} 的目标货架记录不完整")
        return cls(
            id=job_id,
            phase=str(phase),
            created_at=str(raw["created_at"]),
            updated_at=str(raw["updated_at"]),
            request=dict(request),
            plan=dict(plan),
            summary=dict(summary),
            target_shelf=target_shelf,
            target_root=target_root,
            selected_at=selected_at,
            execution=dict(execution) if isinstance(execution, Mapping) else None,
            error=error,
        )


def recover_persisted_engine_jobs(state_root: Path) -> list[EngineJob]:
    """Queue interrupted writes for automatic AList reconciliation.

    A process restart does not prove that a remote move failed.  Preserve the
    plan and let the active scheduler first run exact readback, then replay
    only the still-missing operations.  This function intentionally performs
    no network I/O because it is also used before clients are configured.
    """
    root = Path(state_root).resolve()
    jobs_root = root / "jobs"
    if not jobs_root.exists():
        return []
    recovered: list[EngineJob] = []
    cancel_root = root / "cancel-requests"
    cancel_root.mkdir(mode=0o700, parents=True, exist_ok=True)

    def read_cancel_marker(path: Path) -> Mapping[str, object] | None:
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
        except FileNotFoundError:
            return None
        except (OSError, UnicodeDecodeError, json.JSONDecodeError):
            return None
        return dict(raw) if isinstance(raw, Mapping) else None

    with _engine_worker_lock(root):
        for path in sorted(jobs_root.glob("*.json")):
            try:
                raw = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise SimpleEngineError(f"Engine job 无法读取: {path.stem}") from exc
            if not isinstance(raw, Mapping):
                raise SimpleEngineError(f"Engine job 记录不是对象: {path.stem}")
            job = EngineJob.from_dict(raw)
            active = job.summary.get("active_operation")
            operation_id = active.get("id") if isinstance(active, Mapping) else None
            marker_path = cancel_root / f"{job.id}.json"
            active_phases = {
                "archive_preprocessing", "identity_matching", "planning",
                "executing", "verifying", "cleaning",
            }
            marker = read_cancel_marker(marker_path)
            matches_marker = (
                job.phase in active_phases
                and isinstance(operation_id, str)
                and isinstance(marker, Mapping)
                and marker.get("operation_id") == operation_id
            )
            # A cancel request is allowed to arrive while restart recovery has
            # the worker lock. Before converting an interrupted execution to
            # retry_wait and dropping its operation id, read the marker once
            # more under that same lock.
            if not matches_marker and job.phase == "executing":
                marker = read_cancel_marker(marker_path)
                matches_marker = (
                    isinstance(operation_id, str)
                    and isinstance(marker, Mapping)
                    and marker.get("operation_id") == operation_id
                )
            if matches_marker:
                summary = dict(job.summary)
                summary.pop("active_operation", None)
                lifecycle_raw = summary.get("lifecycle")
                if isinstance(lifecycle_raw, Mapping):
                    lifecycle = dict(lifecycle_raw)
                    cleanup_raw = lifecycle.get("cleanup")
                    if isinstance(cleanup_raw, Mapping) and cleanup_raw.get("status") == "running":
                        cleanup = dict(cleanup_raw)
                        cleanup.update({"status": "cancelled", "updated_at": _now()})
                        lifecycle["cleanup"] = cleanup
                        summary["lifecycle"] = lifecycle
                summary["cancellation"] = {
                    "status": "cancelled",
                    "cancelled_at": _now(),
                    "recovered_after_restart": True,
                }
                reason = marker.get("reason")
                cancelled = replace(
                    job,
                    phase="cancelled",
                    updated_at=_now(),
                    summary=summary,
                    error=redact_error(
                        reason if isinstance(reason, str) else "cancelled by operator"
                    ),
                )
                atomic_write_json(path, cancelled.as_dict(), allow_nan=False)
                try:
                    marker_path.unlink()
                except FileNotFoundError:
                    pass
                recovered.append(cancelled)
                continue
            if job.phase != "executing":
                continue
            summary = dict(job.summary)
            summary.pop("active_operation", None)
            if isinstance(operation_id, str):
                summary["recovered_operation_id"] = operation_id
            recovered_job = replace(
                job,
                phase="retry_wait",
                updated_at=_now(),
                summary=summary,
                error=(
                    "Engine 在远端结果写入前重启；已排入自动 AList 回读和重试。"
                ),
            )
            atomic_write_json(path, recovered_job.as_dict(), allow_nan=False)
            recovered.append(recovered_job)
    return recovered


class PlanBuilder(Protocol):
    def __call__(self, request: EngineRequest, alist: object, tmdb: object) -> object: ...


class PlanExecutor(Protocol):
    def execute(self, plan: object) -> Mapping[str, object]: ...


def _jsonable(value: object) -> object:
    """Return a defensive JSON-compatible copy for injected test doubles."""
    try:
        json.dumps(value, ensure_ascii=False, allow_nan=False)
    except (TypeError, ValueError):
        return str(value)
    return value


class SimplePlanExecutor:
    """Minimal AList move/rename/upload executor with size readback only."""

    def __init__(self, alist: object, tmdb: object | None = None) -> None:
        self.alist = alist
        self.tmdb = tmdb

    @staticmethod
    def _entry_size(raw: object) -> int | None:
        if raw is None:
            return None
        value = raw.get("size") if isinstance(raw, Mapping) else getattr(raw, "size", None)
        if isinstance(value, bool):
            return None
        if isinstance(value, int) and value >= 0:
            return value
        if isinstance(value, str) and value.isdigit():
            return int(value)
        return None

    def _exact(self, path: str) -> Mapping[str, object] | None:
        method = getattr(self.alist, "exact_file_info", None)
        if not callable(method):
            method = getattr(self.alist, "stat_exact", None)
        if not callable(method):
            raise EngineExecutionError("AList 客户端缺少 exact_file_info/stat_exact")
        raw = method(path)
        if raw is None:
            return None
        size = self._entry_size(raw)
        if size is None:
            raise EngineExecutionError(f"远端对象没有可读的大小: {path}")
        return {"size": size}

    def _exact_with_visibility_retry(
        self,
        path: str,
        *,
        expected_size: int | None = None,
    ) -> Mapping[str, object] | None:
        """Read one exact object through a short provider visibility window.

        A few AList storage drivers expose newly-written small sidecars in a
        refreshed parent listing before ``fs/get`` starts answering for that
        exact path.  The listing fallback still matches the full parent/name
        pair and byte size; it is not a broad recursive search.
        """
        # Quark-backed AList can acknowledge a tiny metadata PUT before the
        # object is visible to ``fs/get`` for a couple of minutes.  The bound
        # is still finite, and a genuine absence becomes a failed/recoverable
        # job rather than an unbounded request.
        last_observed: Mapping[str, object] | None = None
        for delay in (0.0, 1.0, 2.0, 4.0, 8.0, 16.0, 32.0, 64.0, 64.0):
            if delay:
                time.sleep(delay)
            observed = self._exact(path)
            if observed is not None:
                if expected_size is None or int(observed["size"]) == expected_size:
                    return observed
                last_observed = observed
            observed = self._listed_exact(path)
            if observed is not None:
                if expected_size is None or int(observed["size"]) == expected_size:
                    return observed
                last_observed = observed
        return last_observed

    def _listed_exact(self, path: str) -> Mapping[str, object] | None:
        listing = getattr(self.alist, "list", None)
        if not callable(listing):
            return None
        parent = posixpath.dirname(path) or "/"
        name = posixpath.basename(path)
        try:
            rows = listing(parent, refresh=True)
        except TypeError:
            # A minimal list implementation can still provide an exact
            # parent/name observation when it has no refresh keyword.
            rows = listing(parent)
        except Exception:
            return None
        if not isinstance(rows, list):
            return None
        matches = [
            row for row in rows
            if isinstance(row, Mapping)
            and row.get("name") == name
            and row.get("is_dir") is not True
        ]
        if len(matches) != 1:
            return None
        size = self._entry_size(matches[0])
        return {"size": size} if size is not None else None

    def _visible_exact(self, path: str) -> Mapping[str, object] | None:
        """Check an object first by exact lookup, then by one refreshed parent listing.

        AList/Quark can acknowledge a move before its exact-file endpoint sees
        the renamed object.  Refreshing the *one parent* is read-only and is
        enough to distinguish that short visibility delay from a missing
        object.  Callers still use bounded retries; this is not a background
        cache invalidation loop.
        """
        return self._exact(path) or self._listed_exact(path)

    def _ensure_dir(self, path: str) -> None:
        if path == "/":
            return
        ensure = getattr(self.alist, "ensure_directory", None)
        if callable(ensure):
            ensure(path)
            return
        mkdir = getattr(self.alist, "mkdir", None)
        if not callable(mkdir):
            raise EngineExecutionError("AList 客户端缺少目录创建接口")
        current = "/"
        for segment in path.strip("/").split("/"):
            current = posixpath.join(current, segment)
            mkdir(current)

    def _check_size(self, path: str, expected: int | None) -> Mapping[str, object]:
        observed = self._exact_with_visibility_retry(path, expected_size=expected)
        if observed is None:
            raise EngineExecutionError(f"远端对象在执行后不可见: {path}")
        if expected is not None and int(observed["size"]) != expected:
            raise EngineExecutionError(
                f"远端大小不匹配: {path}; expected={expected}; actual={observed['size']}"
            )
        return observed

    def _move_file(self, source_dir: str, target_dir: str, original: str, final: str) -> None:
        def rename_with_visibility_retry(full_path: str, new_name: str) -> None:
            """Rename after an AList move without treating a delayed listing as loss.

            Some AList-backed storage returns from ``move`` before the object
            is visible to the immediately following ``rename`` call.  Retrying
            the *exact same* rename is safe only after first checking whether
            the final path already appeared; that also handles a successful
            rename whose response was lost.
            """
            rename = getattr(self.alist, "rename", None)
            if not callable(rename):
                raise EngineExecutionError("AList 客户端缺少 rename 接口")
            parent = posixpath.dirname(full_path) or "/"
            final_path = posixpath.join(parent, new_name)
            last_error: Exception | None = None
            rename_submitted = False
            for delay in (0.0, 0.15, 0.4, 0.8, 1.2):
                if delay:
                    time.sleep(delay)
                # Do not force a remote refresh before the first rename.  It
                # is only useful after an attempt may already have succeeded
                # but its exact-file response is still stale.
                observed = (
                    self._visible_exact(final_path)
                    if rename_submitted
                    else self._exact(final_path)
                )
                if observed is not None:
                    return
                if rename_submitted and self._exact(full_path) is None:
                    # The rename was accepted and the old name disappeared,
                    # while the final name is still in AList's visibility
                    # window.  Replaying rename here is not useful (and some
                    # drivers return a misleading "source not found").
                    # Return to the caller: its bounded final-file readback
                    # will keep refreshing this one destination until it is
                    # visible or becomes a recoverable failure.
                    return
                try:
                    rename(full_path, new_name)
                except Exception as exc:
                    last_error = exc
                    rename_submitted = True
                    continue
                rename_submitted = True
                if self._visible_exact(final_path) is not None:
                    return
                # A success response is followed by another visibility check
                # on the next iteration; do not assume the provider's ACK is
                # the final readback.
            if self._visible_exact(final_path) is not None:
                return
            if last_error is not None:
                raise last_error
            raise EngineExecutionError(f"AList rename 后目标不可见: {final_path}")

        if source_dir == target_dir:
            if original != final:
                rename_with_visibility_retry(posixpath.join(source_dir, original), final)
            return
        self._ensure_dir(target_dir)
        move = getattr(self.alist, "move", None)
        if not callable(move):
            raise EngineExecutionError("AList 客户端缺少 move 接口")
        source_path = posixpath.join(source_dir, original)
        intermediate_path = posixpath.join(target_dir, original)
        last_move_error: Exception | None = None
        move_submitted = False
        # Quark may return a transient provider error while the source listing
        # is settling, or lose the response after it has already moved the
        # object.  Check both exact paths before replaying the move.
        for delay in (0.0, 0.5, 1.0, 2.0, 4.0, 8.0, 16.0):
            if delay:
                time.sleep(delay)
            try:
                observed = (
                    self._visible_exact(intermediate_path)
                    if move_submitted
                    else self._exact(intermediate_path)
                )
                if observed is not None:
                    break
                if self._exact(source_path) is None:
                    # An absent source can mean the previous move committed;
                    # make one refreshed destination check before retrying.
                    move_submitted = True
                    continue
                move(source_dir, target_dir, [original])
                move_submitted = True
                if self._visible_exact(intermediate_path) is not None:
                    break
            except Exception as exc:
                last_move_error = exc
                move_submitted = True
        else:
            if self._visible_exact(intermediate_path) is None:
                if last_move_error is not None:
                    raise last_move_error
                raise EngineExecutionError(f"AList move 后目标不可见: {intermediate_path}")
        if original != final:
            rename_with_visibility_retry(posixpath.join(target_dir, original), final)

    def _upload_bytes(self, target: str, data: bytes, content_type: str) -> Mapping[str, object]:
        uploader = getattr(self.alist, "upload_bytes", None)
        if not callable(uploader):
            raise EngineExecutionError("AList 客户端缺少 upload_bytes 接口，无法写入生成元数据")
        # A pre-upload absence is normal; only the post-upload check needs the
        # short provider visibility retry in ``_check_size``.
        existing = self._exact(target)
        if existing is None:
            uploader(target, data, content_type, overwrite=False)
        elif int(existing["size"]) != len(data):
            raise EngineExecutionError(f"目标元数据已存在但大小不同，拒绝覆盖: {target}")
        return self._check_size(target, len(data))

    def _preserve_or_upload_bytes(
        self,
        target: str,
        data: bytes,
        content_type: str,
    ) -> Mapping[str, object]:
        """Write one child artifact only when its target is absent.

        Existing NFO/artwork is authoritative even when its byte size differs
        from the current plan.  A second exact read after an upload error
        handles the narrow race where another writer created the target after
        our initial absence check; it still never enables overwrite.
        """
        existing = self._exact(target)
        if existing is not None:
            return {"size": int(existing["size"]), "status": "already_present"}
        self._ensure_dir(posixpath.dirname(target) or "/")
        try:
            return self._upload_bytes(target, data, content_type)
        except Exception:
            raced = self._exact(target)
            if raced is not None:
                return {"size": int(raced["size"]), "status": "already_present"}
            raise

    def _verify_source_absent(self, path: str) -> None:
        """Prove a moved source disappeared before the job becomes complete."""
        for delay in (0.0, 0.15, 0.4, 0.8, 1.5, 3.0):
            if delay:
                time.sleep(delay)
            if self._exact(path) is None and self._listed_exact(path) is None:
                return
        raise EngineExecutionError(f"AList move 后源文件仍可见: {path}")

    def install_subtitle_sidecar(
        self,
        source_path: str,
        target_path: str,
        *,
        expected_size: int,
        video_path: str | None = None,
        subtitle_language: str | None = None,
        subtitle_validator: Callable[..., object] | None = None,
    ) -> Mapping[str, object]:
        """Move one verified subtitle member beside an existing final video.

        Subtitle replenishment is not a media child plan: the video already
        exists in the formal library, so planning it again would make a
        subtitle-only staging root look like a broken media source. This
        small writer still uses the same formal-library lock (the caller is
        the runner) and the same move/source/target readback helpers.
        """
        source = _safe_remote_path(source_path, field="subtitle source")
        target = _safe_remote_path(target_path, field="subtitle target")
        suffix = posixpath.splitext(source)[1].casefold()
        if suffix not in {".ass", ".idx", ".srt", ".ssa", ".sub", ".sup", ".vtt"}:
            raise EngineExecutionError(f"字幕格式不支持: {source}")
        if isinstance(expected_size, bool) or not isinstance(expected_size, int) or expected_size <= 0:
            raise EngineExecutionError("字幕大小无效")
        if subtitle_language is not None:
            validator = subtitle_validator or self.validate_subtitle_source_content
            try:
                verdict = validator(source, subtitle_language)
            except TypeError:
                verdict = validator(source_path=source, required_language=subtitle_language)
            if not isinstance(verdict, Mapping) or str(verdict.get("status") or "").casefold() != "satisfied":
                reason = str(verdict.get("reason") or "subtitle_content_unknown") if isinstance(verdict, Mapping) else "subtitle_content_unknown"
                raise EngineExecutionError(f"字幕内容未通过语言校验: {reason}")
        if not posixpath.dirname(target):
            raise EngineExecutionError("字幕目标路径无效")
        if video_path is not None:
            video = _safe_remote_path(video_path, field="subtitle video")
            # The video was just found by the fresh audit.  A pre-write
            # existence check must stay cheap; the long visibility retry is
            # reserved for the object written by this operation.
            if self._visible_exact(video) is None:
                raise EngineExecutionError(f"字幕对应的正式视频不可见: {video}")
        existing = self._visible_exact(target)
        if existing is not None:
            raise EngineExecutionError(f"字幕目标已存在，拒绝覆盖: {target}")
        source_info = self._visible_exact(source)
        if source_info is not None and int(source_info["size"]) != expected_size:
            source_info = None
        if source_info is None:
            raise EngineExecutionError(f"字幕 staging 源不可见或大小不符: {source}")
        source_dir = posixpath.dirname(source) or "/"
        target_dir = posixpath.dirname(target) or "/"
        self._move_file(source_dir, target_dir, posixpath.basename(source), posixpath.basename(target))
        observed = self._check_size(target, expected_size)
        self._verify_source_absent(source)
        return {
            "source": source,
            "target": target,
            "size": int(observed["size"]),
            "status": "moved",
        }

    def validate_subtitle_source_content(
        self,
        source_path: str,
        required_language: str,
    ) -> Mapping[str, object]:
        """Read a bounded staging prefix and require an explicit language match."""
        reader = getattr(self.alist, "read_file_prefix", None)
        if not callable(reader):
            reader = getattr(self.alist, "read_file_bytes", None)
        if not callable(reader):
            return {
                "status": "unknown",
                "reason": "subtitle_content_reader_unavailable",
            }
        try:
            try:
                prefix = reader(source_path, max_bytes=DEFAULT_MAX_PREFIX_BYTES)
            except TypeError:
                prefix = reader(source_path, DEFAULT_MAX_PREFIX_BYTES)
        except Exception:
            return {"status": "unknown", "reason": "subtitle_content_read_error"}
        result = classify_subtitle_content(
            prefix, required_language, max_bytes=DEFAULT_MAX_PREFIX_BYTES,
        )
        return dict(result) if isinstance(result, Mapping) else {
            "status": "unknown", "reason": "subtitle_decode_or_format_unknown",
        }

    @staticmethod
    def _owned_source_root(path: str) -> bool:
        return is_task_owned_staging_root(path)

    def _cleanup_empty_source_tree(self, root: str) -> list[str]:
        """Remove only empty directories inside one task-owned source tree."""
        if not self._owned_source_root(root):
            return []
        listing = getattr(self.alist, "list", None)
        remove_empty = getattr(self.alist, "remove_empty_dir", None)
        if not callable(listing) or not callable(remove_empty):
            return []
        removed: list[str] = []

        def rows(path: str) -> list[Mapping[str, object]]:
            try:
                raw = listing(path, refresh=True)
            except TypeError:
                raw = listing(path)
            if not isinstance(raw, list) or any(not isinstance(item, Mapping) for item in raw):
                raise EngineExecutionError(f"AList 源目录回读格式无效: {path}")
            return list(raw)

        def visit(directory: str) -> None:
            for item in rows(directory):
                name = item.get("name")
                if not isinstance(name, str) or not name or name in {".", ".."} or "/" in name or "\\" in name:
                    raise EngineExecutionError(f"AList 源目录出现不安全条目: {directory}")
                if item.get("is_dir") is True:
                    visit(posixpath.join(directory, name))
            if rows(directory):
                return
            try:
                deleted = remove_empty(directory)
            except Exception as exc:
                raise EngineExecutionError(f"无法清理空源目录: {directory}: {exc}") from exc
            if deleted is False:
                return
            parent = posixpath.dirname(directory) or "/"
            name = posixpath.basename(directory)
            try:
                parent_rows = rows(parent)
            except Exception:
                parent_rows = []
            if not any(item.get("name") == name for item in parent_rows):
                removed.append(directory)

        visit(root)
        return removed

    def finalize_cleanup(self, plan: object) -> Mapping[str, object]:
        """Apply only the plan-owned residual cleanup after lifecycle gates.

        Formal media moves and artifact writes are intentionally absent from
        this method.  It is the re-entrant final step used after a trusted
        scoped audit and any provider decision; every row is revalidated from
        the persisted plan before the first remote delete.
        """
        _cancellation_checkpoint()
        _require_cleanup_allowlist(plan, stage="最终清理")
        cleanup_items = list(getattr(plan, "cleanup_files", ()) or ())
        cleaned: list[str] = []
        if cleanup_items:
            remove = getattr(self.alist, "remove", None)
            if not callable(remove):
                raise EngineExecutionError("AList 客户端缺少 remove 接口，无法执行最终清理")
            for item in cleanup_items:
                _cancellation_checkpoint()
                source_path = str(getattr(item, "source_path"))
                source_dir = str(getattr(item, "source_dir"))
                original = str(getattr(item, "original_name"))
                if self._exact(source_path) is None:
                    continue
                remove(source_dir, [original])
                if self._exact(source_path) is not None:
                    raise EngineExecutionError(f"最终清理后源文件仍存在: {source_path}")
                cleaned.append(source_path)
                _cancellation_checkpoint()
        _cancellation_checkpoint()
        removed_source_directories = self._cleanup_empty_source_tree(
            str(getattr(plan, "source_root", ""))
        )
        return {
            "cleanup": cleaned,
            "cleanup_count": len(cleaned),
            "removed_source_directories": removed_source_directories,
        }

    def execute(self, plan: object) -> Mapping[str, object]:
        _cancellation_checkpoint()
        all_files = list(getattr(plan, "files", ()) or ())
        media_only = _is_provider_media_only_plan(plan)
        # Provider children are deliberately video-only transactions.  A
        # torrent may carry subtitle companions, but this executor never
        # moves them implicitly.  The coordinator may install one later only
        # through the audited ``missing_subtitle`` sidecar lane after exact
        # language/identity pairing.
        files = [
            item for item in all_files
            if not media_only or getattr(item, "media_kind", None) == "video"
        ]
        _require_problem_free_plan(plan, stage="计划执行")
        # Validate every cleanup row before the first media move. This avoids
        # a partial formal write followed by discovery that an old plan wanted
        # to delete a user-owned attachment.
        _require_cleanup_allowlist(plan, stage="计划执行")
        if media_only and not files:
            raise EngineExecutionError("provider media-only child 没有可执行视频")
        if media_only:
            try:
                _require_provider_tv_child_primary_videos(plan, stage="计划执行")
            except ValueError as exc:
                raise EngineExecutionError(str(exc)) from exc
        # Reject every known undersized video before the first move, so a
        # multi-file plan cannot partially write formal media and only then
        # discover a test fragment later in the same plan.
        for item in files:
            source_path = str(getattr(item, "source_path"))
            _require_non_test_media_path(source_path, stage="计划执行")
            expected = getattr(item, "source_size", None)
            if expected is not None and (
                isinstance(expected, bool)
                or not isinstance(expected, int)
                or expected < 0
            ):
                raise EngineExecutionError(f"计划文件大小无效: {source_path}")
            if expected is not None:
                _require_admissible_video_size(
                    item, expected, path=source_path, stage="计划",
                )
        moved: list[dict[str, object]] = []
        for item in files:
            # One file is one coherent remote move/readback unit.  Never
            # interrupt an AList call, but do not begin another unit after an
            # operator has cancelled the running job.
            _cancellation_checkpoint()
            source_path = str(getattr(item, "source_path"))
            source_dir = str(getattr(item, "source_dir"))
            original = str(getattr(item, "original_name"))
            final = str(getattr(item, "final_name"))
            target_dir = str(getattr(item, "target_dir"))
            target_path = posixpath.join(target_dir, final)
            expected = getattr(item, "source_size", None)
            source = self._exact(source_path)
            target = self._exact(target_path)
            if target is not None:
                if source is not None:
                    # An occupied destination with a still-visible source is a
                    # real conflict, even when the sizes happen to match.
                    raise EngineExecutionError(f"目标已存在且源文件仍在: {target_path}")
                observed = self._check_size(target_path, expected)
                _require_admissible_video_size(
                    item, observed["size"], path=target_path, stage="正式库回读",
                )
                moved.append({"source": source_path, "target": target_path, "status": "already_present", **observed})
                continue
            if source is None:
                raise EngineExecutionError(f"源文件不可见: {source_path}")
            source_size = int(source["size"])
            if expected is not None and source_size != expected:
                raise EngineExecutionError(f"源文件大小不匹配: {source_path}")
            _require_admissible_video_size(
                item, source_size, path=source_path, stage="来源回读",
            )
            if original != final:
                intermediate_path = posixpath.join(target_dir, original)
                if self._exact(intermediate_path) is not None:
                    raise EngineExecutionError(
                        f"目标目录已有同名原文件，拒绝跨目录改名覆盖: {intermediate_path}"
                    )
            self._move_file(source_dir, target_dir, original, final)
            observed = self._check_size(target_path, expected if expected is not None else source_size)
            _require_admissible_video_size(
                item, observed["size"], path=target_path, stage="正式库回读",
            )
            moved.append({"source": source_path, "target": target_path, "status": "moved", **observed})
            _cancellation_checkpoint()

        for item in moved:
            _cancellation_checkpoint()
            self._verify_source_absent(str(item["source"]))

        artifacts: list[dict[str, object]] = []
        if not media_only:
            # NFO generation is deterministic and does not need the TMDB
            # network.  Provider children deliberately skip this entire
            # artifact lane: they are media-only writes, and an episode NFO
            # (or a newly synthesized poster) would change the established
            # library metadata convention as a side effect of replenishment.
            planned_nfos = getattr(__import__("engine.scraper", fromlist=["planned_nfos"]), "planned_nfos", None)
            if callable(planned_nfos):
                for target, data in planned_nfos(plan):
                    _cancellation_checkpoint()
                    if not isinstance(target, str) or not isinstance(data, (bytes, bytearray)):
                        raise EngineExecutionError("Engine 生成的 NFO 结构无效")
                    self._ensure_dir(posixpath.dirname(target) or "/")
                    observed = self._upload_bytes(target, bytes(data), "application/xml")
                    artifacts.append({"target": target, "kind": "nfo", **observed})

            planned_artwork = getattr(__import__("engine.scraper", fromlist=["planned_artwork"]), "planned_artwork", None)
            downloader = getattr(self.tmdb, "download_poster", None) if self.tmdb is not None else None
            if callable(planned_artwork):
                for target, image_path, role in planned_artwork(plan):
                    _cancellation_checkpoint()
                    if not callable(downloader):
                        raise EngineExecutionError("计划包含海报，但 TMDB 客户端没有 download_poster")
                    data = downloader(image_path)
                    if not isinstance(data, (bytes, bytearray)):
                        raise EngineExecutionError(f"TMDB 海报响应无效: {image_path}")
                    self._ensure_dir(posixpath.dirname(target) or "/")
                    observed = self._upload_bytes(target, bytes(data), "image/jpeg")
                    artifacts.append({"target": target, "kind": role, **observed})

        if _DEFER_TASK_CLEANUP.get():
            pending_cleanup = [
                str(getattr(item, "source_path"))
                for item in list(getattr(plan, "cleanup_files", ()) or ())
            ]
            cleanup_result: Mapping[str, object] = {
                "cleanup": [],
                "cleanup_count": 0,
                "removed_source_directories": [],
                "cleanup_deferred": True,
                "cleanup_pending": pending_cleanup,
            }
        else:
            _cancellation_checkpoint()
            cleanup_result = self.finalize_cleanup(plan)
        return {
            "files": moved,
            "file_count": len(moved),
            "artifacts": artifacts,
            "artifact_count": len(artifacts),
            "media_only": media_only,
            **dict(cleanup_result),
        }


class SimpleEngineRunner:
    """Plan, execute and reconcile automatic Engine jobs."""

    def __init__(
        self,
        state_root: Path,
        *,
        alist: object,
        tmdb: object,
        planner: PlanBuilder | None = None,
        executor: PlanExecutor | Callable[..., Mapping[str, object]] | None = None,
        validate: bool = True,
        library_root: str = "/quark/影视",
        archive_preprocessor: object | None = None,
    ) -> None:
        self.state_root = Path(state_root).resolve()
        self.jobs_root = self.state_root / "jobs"
        self.locks_root = self.state_root / "locks"
        # A cancellation request may be written while another process owns
        # the single formal-write lock.  Keep it outside ``jobs`` so listing
        # durable jobs can never mistake a request marker for a job record.
        self.cancel_requests_root = self.state_root / "cancel-requests"
        self.jobs_root.mkdir(mode=0o700, parents=True, exist_ok=True)
        self.cancel_requests_root.mkdir(mode=0o700, parents=True, exist_ok=True)
        self.alist = alist
        self.tmdb = tmdb
        self.planner = planner
        self.executor = executor or SimplePlanExecutor(alist, tmdb)
        self.validate = bool(validate)
        # The optional adapter is intentionally a narrow ingress hook.  It
        # owns only archive-to-task-staging preparation; the current Engine
        # still owns identity, naming, problem gates and the one formal writer.
        self.archive_preprocessor = archive_preprocessor
        # The composition root binds the process-wide pause fence here.  It
        # is intentionally an optional runner-owned callback so injected
        # executors and provider child protocols remain backward compatible.
        self._pause_requested: Callable[[], bool] | None = None
        self.library_root = _safe_remote_path(
            library_root.rstrip("/") or "/",
            field="library_root",
        )
        try:
            self.recover_active_jobs()
        except EngineWorkerBusyError:
            # A second read-only API process may start while the original
            # worker is still finishing a remote operation.  Listing remains
            # useful; a later execute/cancel call will return 409 explicitly.
            pass

    def _job_path(self, job_id: str) -> Path:
        return self.jobs_root / f"{_safe_job_id(job_id)}.json"

    def _cancel_request_path(self, job_id: str) -> Path:
        return self.cancel_requests_root / f"{_safe_job_id(job_id)}.json"

    @staticmethod
    def _active_operation_id(job: EngineJob) -> str | None:
        active = job.summary.get("active_operation")
        if not isinstance(active, Mapping):
            return None
        identifier = active.get("id")
        return identifier if isinstance(identifier, str) and identifier else None

    @staticmethod
    def _with_active_operation(
        summary: Mapping[str, object],
        *,
        kind: str,
    ) -> dict[str, object]:
        updated = dict(summary)
        updated.pop("recovered_operation_id", None)
        updated["active_operation"] = {
            "id": uuid.uuid4().hex,
            "kind": kind,
            "started_at": _now(),
        }
        return updated

    @staticmethod
    def _without_active_operation(summary: Mapping[str, object]) -> dict[str, object]:
        updated = dict(summary)
        updated.pop("active_operation", None)
        return updated

    def _read_cancel_request(self, job_id: str) -> Mapping[str, object] | None:
        try:
            raw = json.loads(self._cancel_request_path(job_id).read_text(encoding="utf-8"))
        except FileNotFoundError:
            return None
        except (OSError, UnicodeDecodeError, json.JSONDecodeError):
            # A malformed marker must never block or broaden formal work.
            # It is safe to discard because it has no authenticated operation
            # identity to apply to.
            self._clear_cancel_request(job_id)
            return None
        return dict(raw) if isinstance(raw, Mapping) else None

    def _clear_cancel_request(self, job_id: str) -> None:
        try:
            self._cancel_request_path(job_id).unlink()
        except FileNotFoundError:
            pass

    def _cancel_requested(self, job: EngineJob) -> bool:
        operation_id = self._active_operation_id(job)
        if operation_id is None:
            return False
        request = self._read_cancel_request(job.id)
        return (
            isinstance(request, Mapping)
            and request.get("operation_id") == operation_id
        )

    def set_pause_requested(self, checker: Callable[[], bool] | None) -> None:
        """Bind the application pause fence without adding a second state store."""
        if checker is not None and not callable(checker):
            raise TypeError("pause checker must be callable or None")
        self._pause_requested = checker

    def _cancel_request_matches(
        self,
        job: EngineJob,
        request: Mapping[str, object],
    ) -> bool:
        """Match a cancellation to one durable job operation or idle revision."""
        operation_id = self._active_operation_id(job)
        if operation_id is not None:
            return request.get("operation_id") == operation_id
        recovered_operation_id = job.summary.get("recovered_operation_id")
        if (
            job.phase == "retry_wait"
            and isinstance(recovered_operation_id, str)
            and request.get("kind") == "running"
        ):
            return request.get("operation_id") == recovered_operation_id
        return (
            request.get("kind") == "inactive"
            and request.get("phase") == job.phase
            and request.get("updated_at") == job.updated_at
        )

    def _request_running_cancellation(self, job: EngineJob, *, reason: str) -> None:
        operation_id = self._active_operation_id(job)
        if operation_id is None:
            raise EngineWorkerBusyError(
                "Engine 任务正在切换状态；请稍后再次取消"
            )
        atomic_write_json(
            self._cancel_request_path(job.id),
            {
                "kind": "running",
                "operation_id": operation_id,
                "requested_at": _now(),
                "reason": redact_error(reason),
            },
            allow_nan=False,
        )

    def _request_inactive_cancellation(self, job: EngineJob, *, reason: str) -> None:
        """Fence an idle revision before cancelling it without the global lock.

        This path is used only when another job owns the global formal-write
        lock.  No worker can concurrently begin this queued/planned revision
        without first acquiring that lock; the marker makes a just-released
        scheduler consume cancellation before it can advance the job.
        """
        atomic_write_json(
            self._cancel_request_path(job.id),
            {
                "kind": "inactive",
                "phase": job.phase,
                "updated_at": job.updated_at,
                "requested_at": _now(),
                "reason": redact_error(reason),
            },
            allow_nan=False,
        )

    def _cancelled_job(self, job: EngineJob, *, reason: str) -> EngineJob:
        summary = self._without_active_operation(job.summary)
        lifecycle_raw = summary.get("lifecycle")
        if isinstance(lifecycle_raw, Mapping):
            lifecycle = dict(lifecycle_raw)
            cleanup_raw = lifecycle.get("cleanup")
            if isinstance(cleanup_raw, Mapping) and cleanup_raw.get("status") == "running":
                cleanup = dict(cleanup_raw)
                cleanup.update({"status": "cancelled", "updated_at": _now()})
                lifecycle["cleanup"] = cleanup
                summary["lifecycle"] = lifecycle
        summary["cancellation"] = {
            "status": "cancelled",
            "cancelled_at": _now(),
        }
        cancelled = replace(
            job,
            phase="cancelled",
            updated_at=_now(),
            summary=summary,
            error=redact_error(reason.strip() or "cancelled by operator"),
        )
        atomic_write_json(self._job_path(job.id), cancelled.as_dict(), allow_nan=False)
        self._clear_cancel_request(job.id)
        return cancelled

    def _consume_cancel_request(self, job: EngineJob) -> EngineJob | None:
        request = self._read_cancel_request(job.id)
        if not isinstance(request, Mapping) or not self._cancel_request_matches(job, request):
            return None
        reason = request.get("reason")
        return self._cancelled_job(
            job,
            reason=reason if isinstance(reason, str) else "cancelled by operator",
        )

    def _read(self, job_id: str) -> EngineJob:
        path = self._job_path(job_id)
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
        except FileNotFoundError as exc:
            raise EngineJobNotFoundError(f"Engine job 不存在: {job_id}") from exc
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise SimpleEngineError(f"Engine job 无法读取: {job_id}") from exc
        if not isinstance(raw, Mapping):
            raise SimpleEngineError(f"Engine job 记录不是对象: {job_id}")
        return EngineJob.from_dict(raw)

    def get_job(self, job_id: str) -> EngineJob:
        return self._read(job_id)

    def list_jobs(self) -> list[EngineJob]:
        return [self._read(path.stem) for path in sorted(self.jobs_root.glob("*.json"))]

    @contextlib.contextmanager
    def worker_lock(self) -> Iterator[None]:
        with _engine_worker_lock(self.state_root):
            yield

    def recover_active_jobs(self) -> list[EngineJob]:
        """Expose restart reconciliation to the HTTP composition root."""
        return recover_persisted_engine_jobs(self.state_root)

    def install_subtitle_sidecar(
        self,
        source_path: str,
        target_path: str,
        *,
        expected_size: int,
        video_path: str | None = None,
        subtitle_language: str | None = None,
        subtitle_validator: Callable[..., object] | None = None,
    ) -> Mapping[str, object]:
        """Install one subtitle member under the single formal write lock."""
        with self.worker_lock():
            installer = getattr(self.executor, "install_subtitle_sidecar", None)
            if not callable(installer):
                raise EngineExecutionError("当前 Engine executor 不支持字幕侧挂写入")
            kwargs: dict[str, object] = {
                "expected_size": expected_size,
                "video_path": video_path,
            }
            if subtitle_language is not None:
                kwargs["subtitle_language"] = subtitle_language
            if subtitle_validator is not None:
                kwargs["subtitle_validator"] = subtitle_validator
            try:
                return dict(installer(source_path, target_path, **kwargs))
            except TypeError as exc:
                # Focused legacy executors may not yet accept the optional
                # language keyword.  Do not hide a real write TypeError; only
                # retry when the signature itself rejected that keyword.
                if subtitle_language is None or "subtitle_language" not in str(exc):
                    raise
                kwargs.pop("subtitle_language", None)
                kwargs.pop("subtitle_validator", None)
                return dict(installer(source_path, target_path, **kwargs))

    def validate_subtitle_source_content(
        self,
        source_path: str,
        required_language: str,
    ) -> Mapping[str, object]:
        """Expose the default executor's bounded subtitle validator."""
        validator = getattr(self.executor, "validate_subtitle_source_content", None)
        if not callable(validator):
            raise EngineExecutionError("当前 Engine executor 不支持字幕内容校验")
        result = validator(source_path, required_language)
        if not isinstance(result, Mapping):
            raise EngineExecutionError("字幕内容校验返回无效")
        return dict(result)

    def find_by_source(self, source_path: str) -> EngineJob | None:
        """Return the durable public owner of an ingress path, if any.

        Source consumption now occurs after the root workflow settles, rather
        than immediately after the formal move.  Keep every persisted public
        root as the source owner until an explicit terminal cleanup removes
        its record; otherwise an intake rescan could recreate a second job for
        a source that still belongs to an audit/provider lifecycle.
        """
        normalized = _safe_remote_path(source_path, field="source_path", allow_root=False)
        for job in self.list_jobs():
            if job.summary.get("internal_child") is True:
                continue
            raw_candidate = job.summary.get("ingress_source_path") or job.request.get("source_path")
            candidate_value = (
                raw_candidate.rstrip("/")
                if isinstance(raw_candidate, str) and raw_candidate != "/"
                else raw_candidate
            )
            try:
                candidate = _safe_remote_path(
                    candidate_value,
                    field="persisted ingress_source_path",
                    allow_root=False,
                )
            except EngineRequestError:
                # Old malformed JSON remains read-only; it must not become a
                # new source of ownership decisions or a reason to widen the
                # current intake boundary.
                continue
            if candidate == normalized:
                return job
        return None

    def _confirmed_target_selection(self, job: EngineJob) -> tuple[TargetShelf, str]:
        """Rebuild and verify a persisted ordinary-job shelf selection."""
        if job.target_shelf is None or job.target_root is None or job.selected_at is None:
            reconciliation = job.summary.get("reconciliation")
            if (
                isinstance(reconciliation, Mapping)
                and reconciliation.get("outcome") == "merge_existing"
            ):
                _identity, shelf, shelf_root, _work_root = self._merge_existing_context(
                    job.summary,
                )
                return shelf, shelf_root
            raise EngineRequestError("自动任务尚未选择目标货架，不能启动正式处理")
        try:
            shelf = parse_target_shelf(job.target_shelf)
            expected_root = target_root_for_shelf(self.library_root, shelf)
        except ValueError as exc:
            raise EngineRequestError("自动任务的目标货架记录无效") from exc
        if job.target_root != expected_root:
            raise EngineRequestError("自动任务的目标货架根目录与服务策略不一致")
        return shelf, expected_root

    @staticmethod
    def _job_ingress_source(job: EngineJob) -> str:
        source = job.summary.get("ingress_source_path") or job.request.get("source_path")
        return _safe_remote_path(source, field="ingress_source_path", allow_root=False)

    def source_directory_exists(self, source_path: str) -> bool:
        """Prove one source is a direct remote directory through its parent.

        AList returns an empty list both for an empty directory and, on some
        backends, a missing path.  Inspecting the exact parent/name pair keeps
        the start gate narrow and makes an empty source directory observable
        without treating a broad recursive read as evidence.
        """
        source = _safe_remote_path(source_path, field="source_path", allow_root=False)
        parent, name = posixpath.split(source)
        if not parent or not name:
            return False
        self._ensure_authenticated(self.alist)
        listing = getattr(self.alist, "list", None)
        if not callable(listing):
            return False
        try:
            rows = listing(parent, refresh=True)
        except TypeError:
            # Focused/test AList clients may not expose ``refresh``.  Treat a
            # failure in the compatibility call exactly like a failed narrow
            # probe (the start gate must remain fail-closed), rather than
            # letting an arbitrary provider exception escape as HTTP 500.
            try:
                rows = listing(parent)
            except Exception:
                return False
        except Exception:
            return False
        if not isinstance(rows, list):
            return False
        matches = [
            row for row in rows
            if isinstance(row, Mapping) and row.get("name") == name
        ]
        return len(matches) == 1 and matches[0].get("is_dir") is True

    def mark_waiting_source_missing(self, job_id: str) -> EngineJob:
        """Record a passive intake observation without deleting/retrying a job."""
        with self.worker_lock():
            job = self._read(job_id)
            if job.phase not in {"awaiting_target_shelf", "reconciling"}:
                return job
            summary = dict(job.summary)
            if summary.get("waiting_source_state") == "missing" and job.error:
                return job
            summary["waiting_source_state"] = "missing"
            updated = replace(
                job,
                summary=summary,
                updated_at=_now(),
                error="待选择来源目录已不存在；已保留任务记录",
            )
            atomic_write_json(self._job_path(job.id), updated.as_dict(), allow_nan=False)
            return updated

    def create_pending_job(
        self,
        source_path: str,
        *,
        job_id: str | None = None,
    ) -> EngineJob:
        """Persist inbound ownership without starting formal processing."""
        with self.worker_lock():
            return self._create_pending_job_locked(source_path, job_id=job_id)

    def _create_pending_job_locked(
        self,
        source_path: str,
        *,
        job_id: str | None = None,
    ) -> EngineJob:
        """Worker-lock-held helper for ``create_pending_job``.

        The formal-write lock is a non-reentrant ``flock``: composition roots
        that already hold it (``create_root_job``) must use this helper instead
        of nesting a second lock acquisition.
        """
        source = _safe_remote_path(source_path, field="source_path", allow_root=False)
        if is_production_test_media_path(source):
            raise EngineRequestError("生产 E2E 测试目录不能创建自动任务")
        # Intake polling and an explicit POST can arrive together.  Re-check
        # durable ownership under the existing process lock so neither caller
        # creates a second pending root for the same source.
        existing = self.find_by_source(source)
        if existing is not None:
            return existing
        identifier = job_id or f"engine-{uuid.uuid4().hex}"
        _safe_job_id(identifier)
        if self._job_path(identifier).exists():
            raise SimpleEngineError(f"Engine job 已存在: {identifier}")
        now = _now()
        job = EngineJob(
            id=identifier,
            phase="awaiting_target_shelf",
            created_at=now,
            updated_at=now,
            request={"source_path": source},
            plan={},
            summary={
                "automatic": True,
                "automatic_stage": "awaiting_target_shelf",
                "source_root": source,
                "ingress_source_path": source,
                "mode": "auto",
                "automatic_attempts": 0,
                "automatic_terminal": False,
                "next_retry_seconds": None,
                "reconciliation": {
                    "status": "blocked_by_target_shelf",
                    "outcome": None,
                },
            },
            target_shelf=None,
            target_root=None,
            selected_at=None,
        )
        atomic_write_json(self._job_path(identifier), job.as_dict(), allow_nan=False)
        return job

    def create_automatic_job(
        self,
        source_path: str,
        *,
        job_id: str | None = None,
    ) -> EngineJob:
        """Compatibility alias for the no-side-effect inbound registration."""
        return self.create_pending_job(source_path, job_id=job_id)

    # ------------------------------------------------------------------
    # Phase 1: IntakeSource → RootJob binding
    # ------------------------------------------------------------------

    def find_root_job_by_intake(self, source_id: str) -> EngineJob | None:
        """Return the unique RootJob associated with an IntakeSource, or None.

        Searches the persisted intake catalog for the given ``source_id``, then
        looks up the linked ``root_task_id`` in the jobs store.  Returns
        ``None`` if no catalog entry exists, the entry has no ``root_task_id``,
        or the linked job record has been deleted.
        """
        from engine.scrapeflow.intake_source import (
            find_by_source_id,
            load_intake_catalog,
        )
        catalog = load_intake_catalog(self.state_root)
        intake = find_by_source_id(catalog, source_id)
        if intake is None or intake.root_task_id is None:
            return None
        try:
            return self._read(intake.root_task_id)
        except (SimpleEngineError, FileNotFoundError):
            return None

    def create_root_job(
        self,
        source_id: str,
        *,
        source_path: str,
        target_shelf: object | None = None,
    ) -> EngineJob:
        """Create or return the unique RootJob for an IntakeSource.

        This is the S-step entry point called when the user explicitly creates
        a task from the intake catalog (source + shelf).  It guarantees that
        the same ``source_id`` always maps to at most one ``EngineJob``:

        - If the intake catalog already records a ``root_task_id`` whose job
          record still exists, that job is returned without creating a new one.
        - Otherwise a new ``EngineJob`` is created under the worker lock and
          the catalog is updated to record the association.

        ``target_shelf`` is accepted for observability only; the shelf itself
        is persisted by the caller through ``start_automatic_job()`` so the
        create-and-authorize transition stays atomic to the user action.
        """
        from engine.scrapeflow.intake_source import (
            bind_root_task,
            find_by_source_id,
            load_intake_catalog,
            save_intake_catalog,
            upsert_intake_source,
        )
        with self.worker_lock():
            catalog = load_intake_catalog(self.state_root)
            intake = find_by_source_id(catalog, source_id)
            if intake is not None and intake.root_task_id is not None:
                try:
                    existing_job = self._read(intake.root_task_id)
                    return existing_job
                except (SimpleEngineError, FileNotFoundError):
                    pass  # Orphaned reference — create a fresh job below.

            # The worker lock is already held; create_pending_job would nest a
            # second non-reentrant flock, so use the locked helper directly.
            job = self._create_pending_job_locked(source_path)

            # Persist the intake → root_task binding in the catalog.  A direct
            # POST may arrive before the intake monitor ever scanned the
            # source, so register the entry first when it is missing.
            if intake is None:
                catalog, intake = upsert_intake_source(
                    catalog, source_path, present=True,
                )
            try:
                new_catalog, _ = bind_root_task(catalog, source_id, job.id)
                save_intake_catalog(self.state_root, new_catalog)
            except (ValueError, OSError):
                # Binding failed — the job is still valid; the catalog update
                # is best-effort and will be retried on the next scan cycle.
                pass

            return job

    def start_automatic_job(
        self,
        job_id: str,
        *,
        target_shelf: object,
    ) -> EngineJob:
        """Atomically persist one user shelf selection and make a root queueable.

        This is the only ordinary-root transition that may enter ``queued``.
        It holds the existing cross-process worker lock only while it reads
        and replaces the JSON record; no archive, TMDB, planner, writer or
        provider operation occurs within this method.
        """
        try:
            selected = parse_target_shelf(target_shelf)
            selected_root = target_root_for_shelf(self.library_root, selected)
        except ValueError as exc:
            raise EngineRequestError(str(exc)) from exc
        with self.worker_lock():
            job = self._read(job_id)
            if job.summary.get("internal_child") is True or job.summary.get("audit_owned") is True:
                raise EngineJobConflictError("内部任务不能通过用户目标货架启动")
            has_reconciliation = "reconciliation" in job.summary
            reconciliation = (
                job.summary.get("reconciliation")
                if isinstance(job.summary.get("reconciliation"), Mapping)
                else {}
            )
            reconciliation_outcome = str(reconciliation.get("outcome") or "")
            if job.phase == "reconciling":
                raise EngineJobConflictError("任务已开始只读对账，不能更改目标货架")
            if job.phase == "reconciliation_uncertain":
                raise EngineJobConflictError("身份/正式库对账不确定，请先处理 needs_attention")
            if job.phase == "reconciled":
                raise EngineJobConflictError("已匹配现有作品，不需要重新选择目标货架")
            # Every newly registered ordinary root reaches this gate before
            # reconciliation. Legacy records that already completed the old
            # reconciliation-first flow may still start only when that result
            # is a validated ``new_work`` outcome.
            if has_reconciliation and reconciliation_outcome:
                if reconciliation_outcome != "new_work":
                    raise EngineJobConflictError("已匹配现有作品的任务不能重新选择目标货架")
                try:
                    self._reconciled_identity(reconciliation.get("identity"))
                except EngineRequestError as exc:
                    raise EngineJobConflictError(
                        "new_work 对账身份记录无效，请先处理 needs_attention"
                    ) from exc
            if job.phase not in {"awaiting_target_shelf", "target_policy_conflict"}:
                # A duplicate start is idempotent only while the selected
                # root is still in the pre-terminal workflow.  Once the job
                # is completed/cancelled or has a terminal bounded failure,
                # reopening it belongs to the explicit retry/reopen contract;
                # /start must not silently revive a terminal record.
                if job.phase in _CLEANUP_TERMINAL_PHASES:
                    raise EngineJobConflictError(
                        "任务已经进入终态，请通过 retry 或明确的重新打开流程处理"
                    )
                if job.target_shelf == selected.value and job.target_root == selected_root:
                    # Keep selected_at unchanged: a duplicate start is a read
                    # of the durable transition, not a new selection event.
                    return job
                raise EngineJobConflictError("任务已经开始，不能更改目标货架")
            source = self._job_ingress_source(job)
            if not self.source_directory_exists(source):
                raise EngineJobConflictError("待选择来源目录已不存在；已保留任务记录")
            # A waiting root should not have a child.  Check it anyway so a
            # malformed/imported record cannot re-open a root while an
            # internal provider child still owns state under the same id.
            children = [
                candidate
                for candidate in self.list_jobs()
                if candidate.summary.get("root_job_id") == job.id
                and candidate.phase not in {"executed", "completed", "failed", "cancelled"}
            ]
            if children:
                raise EngineJobConflictError("任务仍有活动内部子任务，不能重新选择目标货架")
            summary = dict(job.summary)
            pre_reconciliation = not reconciliation_outcome
            summary.update({
                "automatic": True,
                "automatic_stage": "queued",
                "source_root": source,
                "ingress_source_path": source,
                "target_shelf": selected.value,
                "selected_target_root": selected_root,
                "target_work_path": None,
                "automatic_terminal": False,
                "next_retry_seconds": None,
            })
            # A target-policy conflict never has a formal plan.  Drop only
            # stale result projections; an explicit manual identity correction
            # remains a user-requested retry input and still goes through the
            # same compatibility matrix after the new shelf is selected.
            summary.pop("identity", None)
            summary.pop("resource_gaps", None)
            summary.pop("waiting_source_state", None)
            selected_at = _now()
            updated = replace(
                job,
                phase="queued",
                updated_at=selected_at,
                request={"source_path": source},
                plan={},
                summary=summary,
                target_shelf=selected.value,
                target_root=selected_root,
                selected_at=selected_at,
                execution=None,
                error=None,
            )
            atomic_write_json(self._job_path(job.id), updated.as_dict(), allow_nan=False)
            return updated

    def mark_reconciling(self, job_id: str) -> EngineJob:
        """Open read-only reconciliation after the user shelf is durable."""
        with self.worker_lock():
            job = self._read(job_id)
            reconciliation = (
                job.summary.get("reconciliation")
                if isinstance(job.summary.get("reconciliation"), Mapping)
                else {}
            )
            if job.phase == "reconciling":
                return job
            if (
                job.phase != "queued"
                or not job.target_shelf
                or not job.target_root
                or not job.selected_at
                or reconciliation.get("outcome")
            ):
                raise EngineJobConflictError("任务当前不能进入只读对账")
            summary = dict(job.summary)
            summary["automatic_stage"] = "reconciling"
            updated = replace(job, phase="reconciling", summary=summary, updated_at=_now())
            atomic_write_json(self._job_path(job.id), updated.as_dict(), allow_nan=False)
            return updated

    @staticmethod
    def _reconciliation_formal_roots(library_root: str) -> tuple[str, str, str]:
        """Return the three closed formal shelves for a configured library root."""
        return tuple(
            target_root_for_shelf(library_root, shelf)
            for shelf in (TargetShelf.MOVIE, TargetShelf.ANIME, TargetShelf.US_TV)
        )

    @staticmethod
    def _reconciliation_identity_key(identity: AutomaticIdentity) -> tuple[int, str] | None:
        if identity.media_type not in {"movie", "tv"} or identity.tmdb_id <= 0:
            return None
        return identity.tmdb_id, identity.media_type

    @staticmethod
    def _reconciliation_work_metadata(work: Mapping[str, object]) -> Mapping[str, object]:
        metadata = work.get("metadata")
        return metadata if isinstance(metadata, Mapping) else work

    @classmethod
    def _reconciliation_work_key(cls, work: Mapping[str, object]) -> tuple[int, str] | None:
        metadata = cls._reconciliation_work_metadata(work)
        raw_tmdb_id = metadata.get("tmdb_id")
        if isinstance(raw_tmdb_id, str) and raw_tmdb_id.isdecimal():
            raw_tmdb_id = int(raw_tmdb_id)
        media_type = str(metadata.get("media_type") or metadata.get("type") or "").casefold()
        if type(raw_tmdb_id) is not int or raw_tmdb_id <= 0 or media_type not in {"movie", "tv"}:
            return None
        return raw_tmdb_id, media_type

    @classmethod
    def _reconciliation_target_root(cls, work: Mapping[str, object]) -> str | None:
        metadata = cls._reconciliation_work_metadata(work)
        value = metadata.get("target_root") or metadata.get("series_root")
        if not isinstance(value, str) or not value.startswith("/"):
            return None
        try:
            return _safe_remote_path(value, field="formal target_root", allow_root=False)
        except EngineRequestError:
            return None

    @staticmethod
    def _reconciliation_scope_is_ambiguous(work: Mapping[str, object]) -> bool:
        metadata = SimpleEngineRunner._reconciliation_work_metadata(work)
        scope = metadata.get("identity_scope")
        if not isinstance(scope, Mapping):
            # Completed legacy job records are a weaker but still durable
            # identity projection. They can only classify a work when no
            # competing formal identity exists; we do not invent a scope.
            return False
        return str(scope.get("kind") or "").casefold().startswith("ambiguous_")

    def _reconciliation_shelf_for_work(self, target_root: str) -> str | None:
        parent = posixpath.dirname(target_root)
        shelf = target_shelf_for_root(self.library_root, parent)
        return shelf.value if shelf is not None else None

    @staticmethod
    def _reconciliation_public_work(work: Mapping[str, object]) -> dict[str, object]:
        metadata = SimpleEngineRunner._reconciliation_work_metadata(work)
        return {
            "tmdb_id": metadata.get("tmdb_id"),
            "title": metadata.get("title"),
            "year": metadata.get("year"),
            "media_type": metadata.get("media_type") or metadata.get("type"),
            "target_root": metadata.get("target_root") or metadata.get("series_root"),
            "identity_source": metadata.get("identity_source"),
            "identity_scope": dict(metadata.get("identity_scope"))
            if isinstance(metadata.get("identity_scope"), Mapping)
            else None,
        }

    @staticmethod
    def _reconciliation_reason(exc: Exception) -> str:
        message = redact_error(exc).strip()
        return message[:320] if message else type(exc).__name__

    @staticmethod
    def _reconciled_identity(value: object) -> AutomaticIdentity:
        """Validate the compact identity persisted by read-only reconciliation."""
        if not isinstance(value, Mapping):
            raise EngineRequestError("new_work 对账缺少已确认身份")
        media_type = value.get("media_type")
        raw_tmdb_id = value.get("tmdb_id")
        if (
            not isinstance(media_type, str)
            or media_type not in {"movie", "tv"}
            or isinstance(raw_tmdb_id, bool)
            or not isinstance(raw_tmdb_id, int)
            or raw_tmdb_id <= 0
        ):
            raise EngineRequestError("new_work 对账身份记录无效")
        title = value.get("title")
        year = value.get("year")
        season = value.get("season")
        trace = value.get("trace")
        if not isinstance(title, str) or not title.strip():
            raise EngineRequestError("new_work 对账身份缺少标题")
        if not isinstance(year, str) or not year.strip():
            raise EngineRequestError("new_work 对账身份缺少年份")
        if media_type == "tv":
            if isinstance(season, bool) or not isinstance(season, int) or season < 0:
                raise EngineRequestError("new_work 对账身份季号无效")
        elif season is not None:
            raise EngineRequestError("电影 new_work 对账身份不应包含季号")
        if not isinstance(trace, Mapping):
            trace = {}
        raw_confidence = value.get("confidence", 1.0)
        if (
            isinstance(raw_confidence, bool)
            or not isinstance(raw_confidence, (int, float))
            or not 0.0 <= float(raw_confidence) <= 1.0
        ):
            raise EngineRequestError("new_work 对账身份置信度无效")
        return AutomaticIdentity(
            media_type=media_type,
            tmdb_id=raw_tmdb_id,
            title=title.strip(),
            year=year.strip(),
            # Preserve the existing Engine's bounded evidence value.  It is
            # not re-scored here, but malformed persisted evidence must fail
            # closed instead of silently becoming a perfect match.
            confidence=float(raw_confidence),
            target_parent=None,
            season=season if media_type == "tv" else None,
            trace=dict(trace),
        )

    def _merge_existing_context(
        self,
        summary: Mapping[str, object],
    ) -> tuple[AutomaticIdentity, TargetShelf, str, str]:
        """Validate the durable identity and destination selected by reconciliation.

        ``EngineJob.target_root`` is intentionally the first-level shelf root;
        the concrete matched work root remains in the reconciliation projection.
        Keeping those meanings separate lets the existing planner/executor
        retain their ordinary containment and readback gates.
        """
        return self._reconciled_existing_context(
            summary,
            expected_outcome="merge_existing",
        )

    def _reconciled_existing_context(
        self,
        summary: Mapping[str, object],
        *,
        expected_outcome: str,
    ) -> tuple[AutomaticIdentity, TargetShelf, str, str]:
        """Validate one matched-work reconciliation without doing remote I/O.

        ``merge_existing`` and ``existing_gap`` share the same durable
        identity/shelf/work-root proof.  Keeping this validation in one
        helper prevents the Provider hand-off from trusting a second copy of
        the path/identity rules.
        """
        reconciliation = summary.get("reconciliation")
        if not isinstance(reconciliation, Mapping):
            raise EngineRequestError("正式库对账记录缺失")
        if reconciliation.get("outcome") != expected_outcome:
            raise EngineRequestError(f"当前任务不是 {expected_outcome}")
        identity = self._reconciled_identity(reconciliation.get("identity"))
        matched = reconciliation.get("matched_formal_work")
        if not isinstance(matched, Mapping):
            raise EngineRequestError("merge_existing 缺少匹配的正式作品")
        matched_key = self._reconciliation_work_key(matched)
        if matched_key != self._reconciliation_identity_key(identity):
            raise EngineRequestError("正式作品身份与对账身份不一致")
        work_root = self._reconciliation_target_root(matched)
        shelf_value = reconciliation.get("matched_shelf")
        if work_root is None or not isinstance(shelf_value, str):
            raise EngineRequestError("既有作品的货架或作品根无效")
        try:
            shelf = parse_target_shelf(shelf_value)
            shelf_root = target_root_for_shelf(self.library_root, shelf)
        except ValueError as exc:
            raise EngineRequestError("既有作品的货架无效") from exc
        # Reconciliation only accepts a concrete work directly below one of
        # the three closed shelves. Re-check that invariant at the write gate.
        if posixpath.dirname(work_root) != shelf_root:
            raise EngineRequestError("既有作品根不在匹配一级货架下")
        if self._reconciliation_shelf_for_work(work_root) != shelf.value:
            raise EngineRequestError("既有作品根与货架映射不一致")
        if not target_shelf_allows_media_type(shelf, identity.media_type):
            raise EngineRequestError("既有作品媒体类型与货架不兼容")
        if self._reconciliation_scope_is_ambiguous(matched):
            raise EngineRequestError("正式作品身份范围不明确")
        return identity, shelf, shelf_root, work_root

    def reconciled_existing_work_root(
        self,
        job_id: str,
        *,
        outcome: str = "existing_gap",
    ) -> str:
        """Return a validated matched work root for a post-reconciliation lane.

        This is a local-state read gate for the composition root.  It does
        not scan AList, call TMDB, or create a Provider owner; the scoped
        audit that follows remains the existing Provider/audit composition
        path.
        """
        if outcome not in {"merge_existing", "existing_gap"}:
            raise EngineRequestError("只允许读取既有作品对账结果")
        with self.worker_lock():
            job = self._read(job_id)
            if job.phase not in {"reconciled", "queued", "planned", "executing", "executed"}:
                raise EngineJobConflictError(
                    f"任务当前没有可用的既有作品对账结果: {job.phase}"
                )
            _identity, _shelf, _shelf_root, work_root = self._reconciled_existing_context(
                job.summary,
                expected_outcome=outcome,
            )
            return work_root

    def _request_from_reconciled_identity(
        self,
        source: str,
        *,
        identity: AutomaticIdentity,
        shelf: TargetShelf,
        shelf_root: str,
    ) -> EngineRequest:
        """Adapt persisted reconciliation evidence to the existing planner API."""
        return EngineRequest.from_mapping({
            "source_path": source,
            "parent_path": shelf_root,
            "media_type": identity.media_type,
            "target_shelf": shelf.value,
            "tmdb_id": identity.tmdb_id,
            "query": identity.title,
            "season": identity.season if isinstance(identity.season, int) else 1,
        })

    def _require_merge_existing_plan_target(
        self,
        plan: object,
        *,
        identity: AutomaticIdentity,
        work_root: str,
        stage: str,
    ) -> None:
        """Fail closed if the existing planner proposes a second work root."""
        planned_root = getattr(plan, "target_root", None)
        if not isinstance(planned_root, str):
            raise FormalTargetConflictError(
                f"{stage}缺少既有作品目标根"
            )
        try:
            planned_root = _safe_remote_path(
                planned_root, field=f"{stage} target_root", allow_root=False,
            )
        except EngineRequestError as exc:
            raise FormalTargetConflictError(f"{stage}目标根无效") from exc
        if planned_root != work_root:
            raise FormalTargetConflictError(
                f"{stage}不得为既有作品创建第二个正式作品根: "
                f"expected={work_root}; planned={planned_root}"
            )
        self._require_reconciled_plan_identity(
            plan,
            identity=identity,
            stage=stage,
        )

    @staticmethod
    def _require_reconciled_plan_identity(
        plan: object,
        *,
        identity: AutomaticIdentity,
        stage: str,
    ) -> None:
        """Keep a persisted reconciliation identity stable through recovery."""
        metadata = getattr(plan, "metadata", {})
        if not isinstance(metadata, Mapping):
            raise FormalTargetConflictError(f"{stage}缺少作品身份元数据")
        raw_tmdb_id = metadata.get("tmdb_id")
        if isinstance(raw_tmdb_id, str) and raw_tmdb_id.isdecimal():
            raw_tmdb_id = int(raw_tmdb_id)
        if raw_tmdb_id != identity.tmdb_id:
            raise FormalTargetConflictError(
                f"{stage}计划身份与既有作品不一致"
            )
        mode = str(getattr(plan, "mode", "")).casefold()
        if mode != identity.media_type:
            raise FormalTargetConflictError(
                f"{stage}计划媒体类型与既有作品不一致"
            )

    def _reconciliation_source_video_paths(self, source: str) -> set[str] | None:
        """Return only real intake videos from an existing read-only walk."""
        walker = getattr(self.alist, "walk", None)
        if not callable(walker):
            return None
        try:
            rows = walker(source, ignore_orphan_temp=True)
        except TypeError:
            try:
                rows = walker(source)
            except Exception:
                return None
        except Exception:
            return None
        if not isinstance(rows, list):
            return None
        paths: set[str] = set()
        source_prefix = source.rstrip("/") + "/"
        for row in rows:
            if not isinstance(row, Mapping):
                continue
            path = row.get("full_path") or row.get("path") or row.get("name")
            size = row.get("size")
            # A zero-byte listing is not usable media evidence.  Keep the
            # broader quality/readability gate in the existing planner, but
            # do not let an empty placeholder authorize merge_existing.
            if not isinstance(path, str) or not path.startswith(source_prefix):
                continue
            try:
                normalized = _safe_remote_path(
                    path, field="reconciliation source media path", allow_root=False,
                )
            except EngineRequestError:
                continue
            if (
                normalized.startswith(source_prefix)
                and is_video_filename(normalized)
                and video_size_is_admissible(size)
            ):
                paths.add(normalized)
        return paths

    def _reconciliation_source_episode_tokens(
        self,
        source: str,
        *,
        default_season: int | None,
    ) -> set[tuple[int, int]] | None:
        """Reuse the shared episode parser over real intake videos only."""
        paths = self._reconciliation_source_video_paths(source)
        if paths is None:
            return None
        from engine.scrapeflow.replenishment_matching import audit_episode_tokens

        tokens: set[tuple[int, int]] = set()
        for path in paths:
            tokens.update(audit_episode_tokens(path, default_season=default_season))
        return tokens

    @staticmethod
    def _reconciliation_formal_video_paths(
        report: Mapping[str, object],
        target_root: str,
    ) -> set[str]:
        """Return formally admissible movie videos inside one matched root.

        The semantic audit projection is intentionally reused for identity and
        gap evidence, but its inventory may still contain zero-byte or tiny
        video entries as diagnostics.  Those entries are not sufficient proof
        of a complete formal movie; the same admission floor used by the
        writer must apply at this reconciliation boundary too.
        """
        inventory = report.get("inventory")
        if not isinstance(inventory, list):
            return set()
        prefix = target_root.rstrip("/") + "/"
        paths: set[str] = set()
        for row in inventory:
            if not isinstance(row, Mapping) or row.get("type") != "file":
                continue
            path = row.get("path") or row.get("full_path")
            if not isinstance(path, str):
                continue
            try:
                normalized = _safe_remote_path(
                    path, field="reconciliation formal media path", allow_root=False,
                )
            except EngineRequestError:
                continue
            if (
                (normalized == target_root or normalized.startswith(prefix))
                and is_video_filename(normalized)
                and video_size_is_admissible(row.get("size"))
            ):
                paths.add(normalized)
        return paths

    def _reconciliation_outcome(
        self,
        *,
        source: str,
        identity: AutomaticIdentity,
        report: Mapping[str, object],
        works: list[dict[str, object]],
        semantic: Mapping[str, object],
    ) -> tuple[str, str, Mapping[str, object] | None, str | None]:
        """Classify one known identity without authorising any side effect."""
        identity_key = self._reconciliation_identity_key(identity)
        if identity_key is None:
            return "uncertain", "Engine identity lacks a supported media type or TMDB id", None, None
        if report.get("complete") is not True or report.get("status") != "completed":
            return "uncertain", "正式库只读清单不完整", None, None
        matches = [
            work for work in works
            if self._reconciliation_work_key(work) == identity_key
        ]
        if not matches:
            # Any formal media without a projected identity makes absence
            # inconclusive. The audit exposes these directly as unknowns;
            # do not infer a new work merely from a missing NFO match.
            semantic_unknowns = semantic.get("unknowns")
            if isinstance(semantic_unknowns, list) and semantic_unknowns:
                return "uncertain", "正式库存在未能由 NFO/完成任务确认身份的媒体", None, None
            return "new_work", "三库中没有已确认的同一 TMDB 作品", None, None
        unique_roots = {
            target for work in matches
            if (target := self._reconciliation_target_root(work)) is not None
        }
        if len(matches) != 1 or len(unique_roots) != 1:
            return "uncertain", "多个正式作品身份匹配，不能安全选择工作根", None, None
        work = matches[0]
        target_root = next(iter(unique_roots))
        shelf = self._reconciliation_shelf_for_work(target_root)
        if shelf is None or self._reconciliation_scope_is_ambiguous(work):
            return "uncertain", "正式作品身份范围或一级货架不明确", work, shelf
        work_gaps = semantic.get("gaps") if isinstance(semantic.get("gaps"), list) else []
        work_unknowns = semantic.get("unknowns") if isinstance(semantic.get("unknowns"), list) else []
        matching_gaps = [
            row for row in work_gaps
            if isinstance(row, Mapping)
            and isinstance(row.get("media"), Mapping)
            and row["media"].get("tmdb_id") == identity.tmdb_id
            and row["media"].get("target_root") == target_root
        ]
        matching_unknowns = [
            row for row in work_unknowns
            if isinstance(row, Mapping)
            and (
                row.get("target_root") == target_root
                or row.get("work") == f"tmdb:{identity.tmdb_id}"
                or (
                    isinstance(row.get("path"), str)
                    and (
                        row["path"] == target_root
                        or row["path"].startswith(target_root.rstrip("/") + "/")
                    )
                )
                or any(
                    isinstance(path, str)
                    and (
                        path == target_root
                        or path.startswith(target_root.rstrip("/") + "/")
                    )
                    for path in (
                        row.get("uncovered_video_paths")
                        if isinstance(row.get("uncovered_video_paths"), list)
                        else []
                    )
                )
            )
        ]
        media_gap_kinds = {"missing_media", "missing_episode", "missing_season"}
        matching_media_gaps = [
            row for row in matching_gaps
            if str(row.get("kind") or "") in media_gap_kinds
        ]
        if identity.media_type == "movie" and matching_media_gaps:
            source_videos = self._reconciliation_source_video_paths(source)
            if source_videos is None:
                return "uncertain", "无法只读确认输入电影媒体", work, shelf
            if source_videos:
                return "merge_existing", "输入包含可补入既有正式电影的视频", work, shelf
        if identity.media_type == "tv":
            source_tokens = self._reconciliation_source_episode_tokens(
                source,
                default_season=identity.season,
            )
            if source_tokens is None:
                return "uncertain", "无法只读确认输入剧集范围", work, shelf
            inventory = report.get("inventory") if isinstance(report.get("inventory"), list) else []
            formal_tokens: set[tuple[int, int]] = set()
            from engine.scrapeflow.replenishment_matching import audit_episode_tokens

            for row in inventory:
                if not isinstance(row, Mapping) or row.get("type") != "file":
                    continue
                path = row.get("path")
                if not isinstance(path, str):
                    continue
                try:
                    normalized = _safe_remote_path(
                        path, field="reconciliation formal episode path", allow_root=False,
                    )
                except EngineRequestError:
                    continue
                if (
                    is_video_filename(normalized)
                    and video_size_is_admissible(row.get("size"))
                    and (normalized == target_root or normalized.startswith(target_root.rstrip("/") + "/"))
                ):
                    formal_tokens.update(
                        audit_episode_tokens(normalized, default_season=identity.season),
                    )
            if not source_tokens:
                return "uncertain", "输入剧集没有可验证的季集坐标", work, shelf
            if source_tokens - formal_tokens:
                return "merge_existing", "输入包含正式作品尚未覆盖的明确剧集", work, shelf
        if matching_unknowns:
            return "uncertain", "正式作品完整性证据不足", work, shelf
        formal_movie_videos = (
            self._reconciliation_formal_video_paths(report, target_root)
            if identity.media_type == "movie"
            else None
        )
        if identity.media_type == "movie" and not formal_movie_videos:
            source_videos = self._reconciliation_source_video_paths(source)
            if source_videos is None:
                return "uncertain", "无法只读确认输入电影媒体", work, shelf
            if source_videos:
                return "merge_existing", "输入包含可补入既有正式电影的视频", work, shelf
            return "existing_gap", "正式电影缺少可接受的视频媒体", work, shelf
        if matching_media_gaps:
            return "existing_gap", "已确认正式作品存在媒体缺口", work, shelf
        return "duplicate_complete", "正式作品身份与媒体范围已充分匹配", work, shelf

    @staticmethod
    def _reconciliation_identity_candidates(
        identity: AutomaticIdentity | None,
        error_candidates: object,
    ) -> list[dict[str, object]]:
        """Project bounded, confirmable identity candidates for uncertain.

        Rows come either from a safe matcher rejection (which refused to
        choose but scored candidates) or from the resolved identity's own
        decision trace.  Only movie/tv rows with a usable TMDB id survive,
        because the U-node confirmation tuple accepts exactly those two
        media types.  This list is read-only evidence; it never authorises
        a shelf, path, link or Provider operation.
        """
        rows: object = error_candidates
        if rows is None and identity is not None:
            trace = identity.trace if isinstance(identity.trace, Mapping) else {}
            rows = trace.get("top_candidates")
        if not isinstance(rows, list):
            return []
        projected: list[dict[str, object]] = []
        for row in rows[:5]:
            if not isinstance(row, Mapping):
                continue
            tmdb_id = row.get("tmdb_id")
            media_type = str(row.get("media_type") or "")
            if isinstance(tmdb_id, bool) or not isinstance(tmdb_id, int) or tmdb_id <= 0:
                continue
            if media_type not in {"movie", "tv"}:
                continue
            confidence = row.get("confidence")
            projected.append({
                "media_type": media_type,
                "tmdb_id": tmdb_id,
                "title": str(row.get("title") or ""),
                "year": str(row.get("year") or ""),
                "confidence": (
                    float(confidence)
                    if isinstance(confidence, (int, float))
                    and not isinstance(confidence, bool)
                    else None
                ),
                "status": str(row.get("status") or ""),
            })
        return projected

    def reconcile_automatic_job(self, job_id: str) -> EngineJob:
        """Persist a bounded, read-only intake reconciliation result.

        This method deliberately calls only existing Engine identity code and
        the existing library audit projections. It never invokes archive
        preprocessing, a planner, executor, Provider, cleanup, or any formal
        writer. The sole mutation is the task's local JSON record.
        """
        with self.worker_lock():
            job = self._read(job_id)
            if job.phase != "reconciling":
                return job
            cancelled = self._consume_cancel_request(job)
            if cancelled is not None:
                return cancelled
            identity: AutomaticIdentity | None = None
            matched_work: Mapping[str, object] | None = None
            matched_shelf: str | None = None
            try:
                source = self._job_ingress_source(job)
                if not self.source_directory_exists(source):
                    raise EngineRequestError("待刮削来源目录不存在或不可读取")
                confirmation = job.summary.get("reconciliation_identity_confirmation")
                if isinstance(confirmation, Mapping):
                    # The operator confirms only the bounded identity tuple;
                    # title/year remain Engine-derived source evidence and the
                    # ordinary library comparison below still decides all
                    # five outcomes.  In particular this is not a second
                    # planner, destination, or Provider input path.
                    original = self.resolve_automatic_identity(source)
                    confirmed_id = confirmation.get("tmdb_id")
                    confirmed_type = str(confirmation.get("media_type") or "").casefold()
                    confirmed_season = confirmation.get("season")
                    if (
                        isinstance(confirmed_id, bool)
                        or not isinstance(confirmed_id, int)
                        or confirmed_id <= 0
                        or confirmed_type not in {"movie", "tv"}
                        or (
                            confirmed_type == "tv"
                            and (
                                isinstance(confirmed_season, bool)
                                or not isinstance(confirmed_season, int)
                                or not 0 <= confirmed_season <= 999
                            )
                        )
                        or (confirmed_type == "movie" and confirmed_season is not None)
                    ):
                        raise EngineRequestError("手工对账身份确认记录无效")
                    trace = dict(original.trace)
                    trace["reconciliation_identity_confirmation"] = {
                        "tmdb_id": confirmed_id,
                        "media_type": confirmed_type,
                    }
                    identity = replace(
                        original,
                        media_type=confirmed_type,
                        tmdb_id=confirmed_id,
                        confidence=1.0,
                        season=confirmed_season if confirmed_type == "tv" else None,
                        trace=trace,
                    )
                else:
                    identity = self.resolve_automatic_identity(source)
                from local.scrapeflow_api.simple_library_audit import (
                    SimpleLibraryAuditor,
                    TmdbEpisodeCatalog,
                    _merge_library_and_job_works,
                    automatic_works_from_engine_jobs,
                    bootstrap_automatic_works_from_library,
                    build_automatic_library_gaps,
                )

                formal_roots = self._reconciliation_formal_roots(self.library_root)
                report = SimpleLibraryAuditor(
                    self.alist,
                    formal_roots=formal_roots,
                ).scan()
                library_works = bootstrap_automatic_works_from_library(
                    report,
                    self.alist,
                    formal_roots=formal_roots,
                )
                job_works = automatic_works_from_engine_jobs(self.list_jobs())
                works, _unowned = _merge_library_and_job_works(library_works, job_works)
                # Reuse the audit's bounded, read-only published-episode
                # evidence for TV work. A TMDB failure stays unknown and is
                # classified fail-closed rather than treating a partial TV
                # directory as complete.
                catalog = TmdbEpisodeCatalog(self.tmdb)
                try:
                    catalog.prefetch(works)
                except Exception:
                    pass
                semantic = build_automatic_library_gaps(
                    report,
                    works,
                    episode_catalog=catalog,
                )
                outcome, reason, matched_work, matched_shelf = self._reconciliation_outcome(
                    source=source,
                    identity=identity,
                    report=report,
                    works=works,
                    semantic=semantic,
                )
                if matched_shelf is not None and matched_shelf != job.target_shelf:
                    outcome = "uncertain"
                    reason = (
                        f"用户选择的一级货架 {job.target_shelf} 与正式库匹配货架 "
                        f"{matched_shelf} 冲突"
                    )
            except Exception as exc:
                outcome = "uncertain"
                reason = self._reconciliation_reason(exc)
                # A safe matcher rejection may still carry bounded candidate
                # evidence (title/year/TMDB id/confidence).  Keep it for the
                # U-node so the operator confirms from system candidates
                # instead of researching identities by hand.
                error_candidates = getattr(exc, "candidates", None)
            else:
                error_candidates = None
            summary = dict(job.summary)
            reconciliation: dict[str, object] = {
                "status": "completed" if outcome != "uncertain" else "needs_attention",
                "outcome": outcome,
                "reason": reason,
            }
            if identity is not None:
                reconciliation["identity"] = identity.as_dict()
            if outcome == "uncertain":
                candidate_rows = self._reconciliation_identity_candidates(
                    identity, error_candidates,
                )
                if candidate_rows:
                    reconciliation["identity_candidates"] = candidate_rows
            if matched_work is not None:
                reconciliation["matched_formal_work"] = self._reconciliation_public_work(matched_work)
            if matched_shelf is not None:
                reconciliation["matched_shelf"] = matched_shelf
            summary.update({
                "automatic": True,
                "automatic_stage": (
                    "queued" if outcome == "new_work"
                    else "reconciliation_needs_attention" if outcome == "uncertain"
                    else "reconciled"
                ),
                "reconciliation": reconciliation,
                "reconciliation_outcome": outcome,
                "automatic_terminal": outcome not in {"new_work", "merge_existing"},
                "next_retry_seconds": None,
            })
            if isinstance(summary.get("reconciliation_identity_confirmation"), Mapping):
                confirmation = dict(summary["reconciliation_identity_confirmation"])
                confirmation["reconciled_at"] = _now()
                confirmation["result"] = outcome
                summary["reconciliation_identity_confirmation"] = confirmation
            phase = (
                "queued" if outcome == "new_work"
                else "reconciliation_uncertain" if outcome == "uncertain"
                else "reconciled"
            )
            updated = replace(
                job,
                phase=phase,
                updated_at=_now(),
                summary=summary,
                error=(reason if outcome == "uncertain" else None),
            )
            latest = self._read(job_id)
            cancelled = self._consume_cancel_request(latest)
            if cancelled is not None:
                return cancelled
            if latest.phase != "reconciling" or latest.updated_at != job.updated_at:
                return latest
            atomic_write_json(self._job_path(job.id), updated.as_dict(), allow_nan=False)
            return self._read(job_id)

    def reopen_reconciliation_uncertain(
        self,
        job_id: str,
        correction: Mapping[str, object],
    ) -> EngineJob:
        """Accept one bounded identity confirmation and repeat only B/C.

        ``uncertain`` must never be reopened into a shelf, planner, writer or
        Provider lane.  The caller can confirm only the same compact identity
        tuple used by the ordinary retry boundary; this method records it as
        audit evidence and returns the root to ``reconciling``.  The next
        invocation of :meth:`reconcile_automatic_job` performs the ordinary
        read-only three-library comparison again.
        """
        if not isinstance(correction, Mapping):
            raise EngineRequestError("对账身份确认必须是 JSON 对象")
        forbidden = set(correction) - {"tmdb_id", "media_type", "season"}
        if forbidden:
            raise EngineRequestError("对账身份确认包含不支持的字段")
        raw_id = correction.get("tmdb_id")
        if isinstance(raw_id, str) and raw_id.isascii() and raw_id.isdecimal():
            raw_id = int(raw_id)
        if isinstance(raw_id, bool) or not isinstance(raw_id, int) or raw_id <= 0:
            raise EngineRequestError("对账身份确认需要正整数 tmdb_id")
        media_type = str(correction.get("media_type") or "").strip().casefold()
        if media_type not in {"movie", "tv"}:
            raise EngineRequestError("对账身份确认需要 media_type=movie 或 tv")
        raw_season = correction.get("season", 1)
        if isinstance(raw_season, str) and raw_season.isascii() and raw_season.isdecimal():
            raw_season = int(raw_season)
        if isinstance(raw_season, bool) or not isinstance(raw_season, int) or not 0 <= raw_season <= 999:
            raise EngineRequestError("对账身份确认 season 必须是 0–999 的整数")

        normalized: dict[str, object] = {
            "tmdb_id": raw_id,
            "media_type": media_type,
            # Keep the existing public correction shape backward compatible:
            # movie retries historically carry the default ``season=1`` even
            # though it is ignored.  It is discarded below and never becomes
            # a movie identity fact.
            "season": raw_season if media_type == "tv" else None,
        }
        with self.worker_lock():
            job = self._read(job_id)
            if job.phase != "reconciliation_uncertain":
                raise EngineJobConflictError("只有 needs_attention 对账任务可以确认身份")
            summary = job.summary if isinstance(job.summary, Mapping) else {}
            reconciliation = summary.get("reconciliation")
            if not isinstance(reconciliation, Mapping) or reconciliation.get("outcome") != "uncertain":
                raise EngineJobConflictError("当前任务不是不确定的只读对账结果")
            if self._owned_children(job) or self._existing_gap_registration_blockers(job):
                raise EngineJobConflictError("任务仍有活动 child 或 Provider 状态，不能重新对账")
            source = self._job_ingress_source(job)
            if not self.source_directory_exists(source):
                raise EngineJobConflictError("待刮削来源目录不存在或不可读取")
            cancelled = self._consume_cancel_request(job)
            if cancelled is not None:
                return cancelled
            updated_summary = dict(summary)
            updated_summary["reconciliation_identity_confirmation"] = {
                **normalized,
                "confirmed_at": _now(),
            }
            updated_summary["automatic"] = True
            updated_summary["automatic_stage"] = "reconciling"
            updated_summary["automatic_terminal"] = False
            updated_summary["next_retry_seconds"] = None
            updated_summary.pop("existing_gap_registration", None)
            updated_summary.pop("source_fate", None)
            updated = replace(
                job,
                phase="reconciling",
                request={"source_path": source},
                plan={},
                summary=updated_summary,
                updated_at=_now(),
                error=None,
                execution=None,
            )
            atomic_write_json(self._job_path(job.id), updated.as_dict(), allow_nan=False)
            return self._read(job.id)

    def prepare_reconciled_merge_job(self, job_id: str) -> EngineJob:
        """Open a read-only ``merge_existing`` result for the existing Engine.

        Reconciliation intentionally leaves the result in ``reconciled`` so
        the HTTP composition root can expose the five-way decision before any
        formal planning starts.  This narrow local transition is the explicit
        hand-off for the one result that is allowed to continue automatically:
        it records no user shelf choice, copies the already validated matched
        work coordinates into the summary, and changes only the local job
        phase to ``queued``.  The planner branch below consumes the persisted
        identity and performs the ordinary Engine/write path.

        ``duplicate_complete`` and ``existing_gap`` deliberately do not use
        this method.  They require their own terminal/Provider decisions and
        must never be silently turned into a media write.
        """
        with self.worker_lock():
            job = self._read(job_id)
            if job.phase == "queued":
                reconciliation = job.summary.get("reconciliation")
                if (
                    isinstance(reconciliation, Mapping)
                    and reconciliation.get("outcome") == "merge_existing"
                ):
                    return job
            if job.phase != "reconciled":
                raise EngineJobConflictError(
                    f"任务当前不能打开 merge_existing: {job.phase}"
                )
            reconciliation = job.summary.get("reconciliation")
            if not isinstance(reconciliation, Mapping) or reconciliation.get("outcome") != "merge_existing":
                raise EngineJobConflictError("只有 merge_existing 对账结果才能进入现有 Engine")
            # Validate every durable coordinate at the hand-off boundary.  No
            # network call or planner is made here.
            _identity, shelf, shelf_root, work_root = self._merge_existing_context(
                job.summary,
            )
            source = self._job_ingress_source(job)
            summary = dict(job.summary)
            summary.update({
                "automatic": True,
                "automatic_stage": "queued",
                "selected_target_root": shelf_root,
                "target_work_path": work_root,
                "merge_existing_ready": True,
                "automatic_terminal": False,
                "next_retry_seconds": None,
            })
            # Keep the classification and matched evidence intact.  The
            # target shelf remains in reconciliation evidence, not in the
            # user-selection fields used by ``new_work`` /start.
            reconciliation_copy = dict(reconciliation)
            reconciliation_copy.update({
                "status": "completed",
                "execution_ready": True,
                "matched_shelf": shelf.value,
                "matched_work_root": work_root,
            })
            summary["reconciliation"] = reconciliation_copy
            now = _now()
            updated = replace(
                job,
                phase="queued",
                updated_at=now,
                request={"source_path": source},
                plan={},
                summary=summary,
                target_shelf=None,
                target_root=None,
                selected_at=None,
                execution=None,
                error=None,
            )
            atomic_write_json(self._job_path(job.id), updated.as_dict(), allow_nan=False)
            return updated

    @staticmethod
    def _audit_text(value: object, *, field: str, required: bool = False) -> str:
        """Normalize bounded audit text without allowing path/control data through."""
        if not isinstance(value, str):
            if required:
                raise EngineRequestError(f"审计项目缺少 {field}")
            return ""
        text = value.strip()
        if required and not text:
            raise EngineRequestError(f"审计项目缺少 {field}")
        if "\x00" in text or len(text) > 512:
            raise EngineRequestError(f"审计项目 {field} 无效")
        return text

    def _audit_target_root(self, value: object) -> str:
        """Accept only a formal-library work root, never intake or staging."""
        target = _safe_remote_path(value, field="audit target_root", allow_root=False)
        library = self.library_root.rstrip("/") or "/"
        if library == "/" or not target.startswith(library + "/"):
            raise EngineRequestError("审计目标必须位于配置的正式媒体库根目录")
        relative = target[len(library) + 1:]
        if "/" not in relative:
            raise EngineRequestError("审计目标必须是正式媒体库中的作品目录")
        first = relative.split("/", 1)[0]
        if first == "待刮削" or first.casefold() == "scrapeflow":
            raise EngineRequestError("审计目标不得位于待刮削或 ScrapeFlow 运行时目录")
        return target

    @staticmethod
    def _audit_positive_int(value: object, *, field: str, allow_zero: bool = False) -> int:
        if isinstance(value, bool) or not isinstance(value, int):
            raise EngineRequestError(f"审计项目 {field} 必须是整数")
        if value < 0 or (value == 0 and not allow_zero):
            raise EngineRequestError(f"审计项目 {field} 超出范围")
        return value

    @staticmethod
    def _audit_media_mapping(
        raw_gap: Mapping[str, object],
        *,
        tmdb_id: int,
        target_root: str,
        media_type: str,
        title: str,
        original_title: str,
        year: str,
        media_format: str,
    ) -> dict[str, object] | None:
        """Project one semantic gap into the narrow provider input contract.

        The audit report is untrusted observation data.  Rebuild the row from
        scalar fields instead of copying arbitrary nested values; rows whose
        identity disagrees with the owning project are ignored.  In
        particular, subtitle/metadata/unknown findings never become video
        acquisition work here.
        """
        kind = str(raw_gap.get("kind") or "").strip().casefold()
        if kind not in _AUDIT_ROOT_GAP_KINDS:
            return None
        raw_media = raw_gap.get("media")
        media = raw_media if isinstance(raw_media, Mapping) else raw_gap
        raw_tmdb = media.get("tmdb_id")
        if isinstance(raw_tmdb, str) and raw_tmdb.isdigit():
            raw_tmdb = int(raw_tmdb)
        if raw_tmdb != tmdb_id:
            return None
        raw_target = media.get("target_root")
        if not isinstance(raw_target, str):
            raw_target = raw_gap.get("target_root")
        try:
            if _safe_remote_path(raw_target, field="audit gap target_root", allow_root=False) != target_root:
                return None
        except EngineRequestError:
            return None
        expected_type = "movie" if kind == "missing_media" else "tv"
        if media_type != expected_type:
            return None
        row_type = str(media.get("media_type") or media.get("type") or "").strip().casefold()
        if row_type not in {expected_type, "mixed" if expected_type == "tv" else expected_type}:
            return None

        label = SimpleEngineRunner._audit_text(raw_gap.get("label"), field="label", required=True)
        reason = SimpleEngineRunner._audit_text(raw_gap.get("reason"), field="reason")
        row: dict[str, object] = {
            "kind": kind,
            "source": "automatic_library_audit",
            "reason": reason,
            "media": {
                "tmdb_id": tmdb_id,
                "title": title,
                "original_title": original_title,
                "year": year,
                "target_root": target_root,
                "media_type": expected_type,
                **({"media_format": media_format} if media_format else {}),
            },
        }
        # Keep a deterministic coordinate.  The provider adapter derives its
        # own request id from the explicit season/episode fields, so an
        # untrusted human label cannot create a path outside the gap state.
        if kind == "missing_media":
            row["id"] = f"missing_media:{tmdb_id}"
            row["label"] = label
            return row

        season = raw_gap.get("season")
        try:
            season_number = SimpleEngineRunner._audit_positive_int(
                season, field="season", allow_zero=True,
            )
        except EngineRequestError:
            return None
        if season_number > 999:
            return None
        row["season"] = season_number
        if kind == "missing_episode":
            try:
                episode_number = SimpleEngineRunner._audit_positive_int(
                    raw_gap.get("episode"), field="episode",
                )
            except EngineRequestError:
                return None
            if episode_number > 9999:
                return None
            row["episode"] = episode_number
            row["id"] = f"missing_episode:{tmdb_id}:S{season_number:02d}E{episode_number:02d}"
            row["label"] = f"{title} S{season_number:02d}E{episode_number:02d}"
            episode_title = SimpleEngineRunner._audit_text(
                raw_gap.get("title"), field="title",
            )
            if episode_title:
                row["title"] = episode_title
            raw_title_aliases = raw_gap.get("title_aliases")
            if isinstance(raw_title_aliases, list):
                title_aliases: list[str] = []
                seen_titles = {episode_title.casefold()} if episode_title else set()
                for raw_alias in raw_title_aliases:
                    if not isinstance(raw_alias, str):
                        continue
                    alias = raw_alias.strip()
                    if (
                        not alias or "\x00" in alias or len(alias) > 512
                        or alias.casefold() in seen_titles
                    ):
                        continue
                    seen_titles.add(alias.casefold())
                    title_aliases.append(alias)
                    if len(title_aliases) >= 7:
                        break
                if title_aliases:
                    row["title_aliases"] = title_aliases
            raw_source_aliases = raw_gap.get("source_episode_aliases")
            if isinstance(raw_source_aliases, list):
                source_aliases: list[dict[str, object]] = []
                for raw_alias in raw_source_aliases:
                    if not isinstance(raw_alias, Mapping):
                        continue
                    source_season = raw_alias.get("season")
                    source_episode = raw_alias.get("episode")
                    if (
                        isinstance(source_season, bool)
                        or not isinstance(source_season, int)
                        or source_season <= 0
                        or source_season > 999
                        or isinstance(source_episode, bool)
                        or not isinstance(source_episode, int)
                        or source_episode <= 0
                        or source_episode > 9999
                    ):
                        continue
                    raw_series_titles = raw_alias.get("series_titles")
                    if not isinstance(raw_series_titles, list):
                        continue
                    series_titles: list[str] = []
                    seen_series_titles: set[str] = set()
                    for raw_series_title in raw_series_titles:
                        if not isinstance(raw_series_title, str):
                            continue
                        series_title = raw_series_title.strip()
                        if (
                            not series_title or "\x00" in series_title
                            or len(series_title) > 512
                            or series_title.casefold() in seen_series_titles
                        ):
                            continue
                        seen_series_titles.add(series_title.casefold())
                        series_titles.append(series_title)
                        if len(series_titles) >= 4:
                            break
                    if series_titles:
                        source_aliases.append({
                            "season": source_season,
                            "episode": source_episode,
                            "series_titles": series_titles,
                        })
                    if len(source_aliases) >= 8:
                        break
                if source_aliases:
                    row["source_episode_aliases"] = source_aliases
            return row

        row["id"] = f"missing_season:{tmdb_id}:S{season_number:02d}"
        row["label"] = f"{title} S{season_number:02d}"
        expected_count = raw_gap.get("expected_episode_count")
        if expected_count is not None:
            try:
                count = SimpleEngineRunner._audit_positive_int(
                    expected_count, field="expected_episode_count",
                )
            except EngineRequestError:
                return None
            if count > 5000:
                return None
            row["expected_episode_count"] = count
        season_name = SimpleEngineRunner._audit_text(raw_gap.get("season_name"), field="season_name")
        if season_name:
            row["season_name"] = season_name
        return row

    @staticmethod
    def _audit_subtitle_mapping(
        raw_gap: Mapping[str, object],
        *,
        tmdb_id: int,
        target_root: str,
        media_type: str,
        title: str,
        original_title: str,
        year: str,
        media_format: str,
    ) -> dict[str, object] | None:
        """Project one exact subtitle gap into the pure sidecar contract.

        Subtitle-only roots are deliberately stricter than normal media
        projects.  The row must identify one existing video below the already
        validated work root and one non-empty language.  No season/episode
        inference is performed here: the audited video path is the only
        pairing coordinate consumed by the provider lane.
        """
        kind = str(raw_gap.get("kind") or "").strip().casefold()
        if kind not in _AUDIT_SUBTITLE_GAP_KINDS:
            return None
        raw_media = raw_gap.get("media")
        media = raw_media if isinstance(raw_media, Mapping) else raw_gap
        raw_tmdb = media.get("tmdb_id")
        if isinstance(raw_tmdb, str) and raw_tmdb.isdigit():
            raw_tmdb = int(raw_tmdb)
        if raw_tmdb != tmdb_id:
            return None
        raw_target = media.get("target_root")
        if not isinstance(raw_target, str):
            raw_target = raw_gap.get("target_root")
        try:
            if _safe_remote_path(raw_target, field="audit subtitle target_root", allow_root=False) != target_root:
                return None
        except EngineRequestError:
            return None
        expected_type = "tv" if media_type == "mixed" else media_type
        if expected_type not in {"movie", "tv"}:
            return None
        row_type = str(media.get("media_type") or media.get("type") or "").strip().casefold()
        if row_type not in {expected_type, "mixed" if expected_type == "tv" else expected_type}:
            return None

        label = SimpleEngineRunner._audit_text(
            raw_gap.get("label"), field="subtitle label", required=True,
        )
        reason = SimpleEngineRunner._audit_text(raw_gap.get("reason"), field="reason")
        gap_id = SimpleEngineRunner._audit_text(
            raw_gap.get("id"), field="subtitle gap id", required=True,
        )
        # Gap ids become local filenames.  Reject path/control characters
        # rather than allowing a crafted report to escape the gap directory.
        if any(char in gap_id for char in ("/", "\\", "\x00", "\n", "\r")):
            return None
        raw_path = raw_gap.get("path")
        if not isinstance(raw_path, str):
            return None
        try:
            video_path = _safe_remote_path(
                raw_path, field="audit subtitle video path", allow_root=False,
            )
        except EngineRequestError:
            return None
        if (
            not video_path.startswith(target_root.rstrip("/") + "/")
            or not is_video_filename(video_path)
            or is_production_test_media_path(video_path)
        ):
            return None
        raw_language = raw_gap.get("subtitle_language")
        if not isinstance(raw_language, str):
            return None
        language = raw_language.strip()
        if (
            not language or len(language) > 64
            or any(char in language for char in ("/", "\\", "\x00", "\n", "\r"))
        ):
            return None
        return {
            "id": gap_id,
            "kind": "missing_subtitle",
            "label": label,
            "reason": reason,
            "source": "automatic_library_audit",
            "media": {
                "tmdb_id": tmdb_id,
                "title": title,
                "original_title": original_title,
                "year": year,
                "target_root": target_root,
                "media_type": expected_type,
                **({"media_format": media_format} if media_format else {}),
            },
            "path": video_path,
            "subtitle_language": language,
        }

    def _audit_project_payload(
        self, project: Mapping[str, object],
    ) -> tuple[str, str, int, str, dict[str, object], list[dict[str, object]]]:
        """Validate and reduce one audit acquisition project.

        Returns ``(work_key, target_root, tmdb_id, media_type, plan, gaps)``.
        No planner/network call is made here; the resulting plan is consumed
        only by ``AutomaticReplenishmentRuntime``.
        """
        if not isinstance(project, Mapping):
            raise EngineRequestError("审计补源项目必须是对象")
        raw_plan = project.get("plan")
        if not isinstance(raw_plan, Mapping):
            raise EngineRequestError("审计补源项目缺少 plan")
        mode = str(raw_plan.get("mode") or project.get("media_type") or "").strip().casefold()
        if mode not in {"movie", "tv"}:
            raise EngineRequestError("审计补源项目媒体类型不受支持")
        raw_project_id = project.get("tmdb_id")
        raw_metadata = raw_plan.get("metadata")
        metadata = raw_metadata if isinstance(raw_metadata, Mapping) else {}
        metadata_type = str(metadata.get("media_type") or metadata.get("type") or "").strip().casefold()
        if metadata_type and metadata_type not in {mode, "mixed" if mode == "tv" else mode}:
            raise EngineRequestError("审计项目 metadata 的媒体类型不一致")
        raw_metadata_id = metadata.get("tmdb_id")
        if raw_project_id != raw_metadata_id:
            # Accept the common string JSON form only after an exact positive
            # conversion; never guess an identity from a title.
            if not (
                isinstance(raw_project_id, str) and raw_project_id.isdigit()
                and isinstance(raw_metadata_id, str) and raw_metadata_id.isdigit()
                and int(raw_project_id) == int(raw_metadata_id)
            ):
                raise EngineRequestError("审计项目的 TMDB 身份字段不一致")
        try:
            tmdb_id = self._audit_positive_int(
                int(raw_project_id) if isinstance(raw_project_id, str) else raw_project_id,
                field="tmdb_id",
            )
        except (TypeError, ValueError) as exc:
            raise EngineRequestError("审计项目 tmdb_id 必须是正整数") from exc
        project_target = project.get("target_root")
        plan_target = raw_plan.get("target_root")
        if not isinstance(project_target, str) or not isinstance(plan_target, str):
            raise EngineRequestError("审计项目缺少 target_root")
        target_root = self._audit_target_root(project_target)
        if self._audit_target_root(plan_target) != target_root:
            raise EngineRequestError("审计项目的 target_root 字段不一致")
        metadata_target = metadata.get("target_root") or metadata.get("series_root")
        if metadata_target is not None and self._audit_target_root(metadata_target) != target_root:
            raise EngineRequestError("审计项目 metadata 的 target_root 字段不一致")
        title = self._audit_text(
            metadata.get("title") or metadata.get("name") or metadata.get("original_title"),
            field="title", required=True,
        )
        original_title = self._audit_text(metadata.get("original_title"), field="original_title")
        year = self._audit_text(metadata.get("year"), field="year")
        media_format = self._audit_text(
            metadata.get("media_format") or metadata.get("format"),
            field="media_format",
        )
        raw_gaps = (
            raw_plan.get("scan_report", {}).get("resource_gaps")
            if isinstance(raw_plan.get("scan_report"), Mapping) else None
        )
        if not isinstance(raw_gaps, list):
            raw_gaps = project.get("gaps")
        if not isinstance(raw_gaps, list):
            raise EngineRequestError("审计项目缺少 resource_gaps")
        gaps: list[dict[str, object]] = []
        seen: set[str] = set()
        for raw_gap in raw_gaps:
            if not isinstance(raw_gap, Mapping):
                continue
            projected = self._audit_media_mapping(
                raw_gap,
                tmdb_id=tmdb_id,
                target_root=target_root,
                media_type=mode,
                title=title,
                original_title=original_title,
                year=year,
                media_format=media_format,
            )
            if projected is None:
                continue
            identity = str(projected["id"])
            if identity in seen:
                continue
            seen.add(identity)
            gaps.append(projected)
        if not gaps:
            raise EngineRequestError("审计项目没有可安全补源的媒体缺口")
        metadata_body: dict[str, object] = {
            "tmdb_id": tmdb_id,
            "title": title,
            "original_title": original_title or title,
            "year": year,
            "media_type": mode,
            "target_root": target_root,
            "series_root": target_root,
            **({"media_format": media_format} if media_format else {}),
        }
        aliases = metadata.get("aliases")
        if isinstance(aliases, list):
            safe_aliases = [
                value.strip() for value in aliases
                if isinstance(value, str) and value.strip() and len(value.strip()) <= 256
            ][:16]
            if safe_aliases:
                metadata_body["aliases"] = safe_aliases
        plan = {
            "mode": mode,
            "target_root": target_root,
            "metadata": metadata_body,
            "scan_report": {"resource_gaps": gaps},
        }
        work_key = f"tmdb:{mode}:{tmdb_id}:{target_root}"
        return work_key, target_root, tmdb_id, mode, plan, gaps

    def _audit_subtitle_project_payload(
        self, project: Mapping[str, object],
    ) -> tuple[str, str, int, str, dict[str, object], list[dict[str, object]]]:
        """Validate a project that contains *only* subtitle sidecar gaps.

        This contract is intentionally separate from ``_audit_project_payload``
        so a subtitle observation can never silently become a missing-media
        project when a report is malformed or a caller supplies a broad
        target.  The returned plan is consumed by the pure subtitle lane and
        has no media child inputs.
        """
        if not isinstance(project, Mapping) or project.get("subtitle_only") is not True:
            raise EngineRequestError("字幕-only 审计项目必须显式声明 subtitle_only")
        raw_plan = project.get("plan")
        if not isinstance(raw_plan, Mapping):
            raise EngineRequestError("字幕-only 审计项目缺少 plan")
        mode = str(raw_plan.get("mode") or project.get("media_type") or "").strip().casefold()
        if mode not in {"movie", "tv"}:
            raise EngineRequestError("字幕-only 审计项目媒体类型不受支持")
        raw_project_id = project.get("tmdb_id")
        metadata = raw_plan.get("metadata") if isinstance(raw_plan.get("metadata"), Mapping) else {}
        metadata_type = str(metadata.get("media_type") or metadata.get("type") or "").strip().casefold()
        if metadata_type and metadata_type not in {mode, "mixed" if mode == "tv" else mode}:
            raise EngineRequestError("字幕-only 项目 metadata 的媒体类型不一致")
        raw_metadata_id = metadata.get("tmdb_id")
        if raw_project_id != raw_metadata_id:
            if not (
                isinstance(raw_project_id, str) and raw_project_id.isdigit()
                and isinstance(raw_metadata_id, str) and raw_metadata_id.isdigit()
                and int(raw_project_id) == int(raw_metadata_id)
            ):
                raise EngineRequestError("字幕-only 项目的 TMDB 身份字段不一致")
        try:
            tmdb_id = self._audit_positive_int(
                int(raw_project_id) if isinstance(raw_project_id, str) else raw_project_id,
                field="tmdb_id",
            )
        except (TypeError, ValueError) as exc:
            raise EngineRequestError("字幕-only 项目 tmdb_id 必须是正整数") from exc
        project_target = project.get("target_root")
        plan_target = raw_plan.get("target_root")
        if not isinstance(project_target, str) or not isinstance(plan_target, str):
            raise EngineRequestError("字幕-only 项目缺少 target_root")
        target_root = self._audit_target_root(project_target)
        if self._audit_target_root(plan_target) != target_root:
            raise EngineRequestError("字幕-only 项目的 target_root 字段不一致")
        metadata_target = metadata.get("target_root") or metadata.get("series_root")
        if metadata_target is not None and self._audit_target_root(metadata_target) != target_root:
            raise EngineRequestError("字幕-only 项目 metadata 的 target_root 字段不一致")
        title = self._audit_text(
            metadata.get("title") or metadata.get("name") or metadata.get("original_title"),
            field="title", required=True,
        )
        original_title = self._audit_text(metadata.get("original_title"), field="original_title")
        year = self._audit_text(metadata.get("year"), field="year")
        media_format = self._audit_text(
            metadata.get("media_format") or metadata.get("format"),
            field="media_format",
        )
        raw_gaps = (
            raw_plan.get("scan_report", {}).get("resource_gaps")
            if isinstance(raw_plan.get("scan_report"), Mapping) else None
        )
        if not isinstance(raw_gaps, list):
            raw_gaps = project.get("gaps")
        if not isinstance(raw_gaps, list):
            raise EngineRequestError("字幕-only 项目缺少 resource_gaps")
        gaps: list[dict[str, object]] = []
        seen_ids: set[str] = set()
        seen_coordinates: set[tuple[str, str]] = set()
        for raw_gap in raw_gaps:
            if not isinstance(raw_gap, Mapping):
                continue
            projected = self._audit_subtitle_mapping(
                raw_gap,
                tmdb_id=tmdb_id,
                target_root=target_root,
                media_type=mode,
                title=title,
                original_title=original_title,
                year=year,
                media_format=media_format,
            )
            if projected is None:
                continue
            gap_id = str(projected["id"])
            coordinate = (str(projected["path"]), str(projected["subtitle_language"]).casefold())
            # A duplicate id with a different exact video/language coordinate
            # is ambiguous; fail closed rather than letting one state file
            # claim two sidecars.
            if gap_id in seen_ids:
                if coordinate not in seen_coordinates:
                    raise EngineRequestError("字幕-only 项目包含重复 gap id")
                continue
            if coordinate in seen_coordinates:
                continue
            seen_ids.add(gap_id)
            seen_coordinates.add(coordinate)
            gaps.append(projected)
        if not gaps:
            raise EngineRequestError("字幕-only 审计项目没有可安全补源的字幕缺口")
        metadata_body: dict[str, object] = {
            "tmdb_id": tmdb_id,
            "title": title,
            "original_title": original_title or title,
            "year": year,
            "media_type": mode,
            "target_root": target_root,
            "series_root": target_root,
            "subtitle_only": True,
            **({"media_format": media_format} if media_format else {}),
        }
        aliases = metadata.get("aliases")
        if isinstance(aliases, list):
            safe_aliases = [
                value.strip() for value in aliases
                if isinstance(value, str) and value.strip() and len(value.strip()) <= 256
            ][:16]
            if safe_aliases:
                metadata_body["aliases"] = safe_aliases
        plan = {
            "mode": mode,
            "target_root": target_root,
            "source_root": target_root,
            "metadata": metadata_body,
            "scan_report": {"resource_gaps": gaps},
        }
        work_key = f"tmdb:{mode}:{tmdb_id}:{target_root}:subtitle"
        return work_key, target_root, tmdb_id, mode, plan, gaps

    @staticmethod
    def _job_work_coordinates(job: EngineJob) -> tuple[int | None, str | None, str | None]:
        summary = job.summary if isinstance(job.summary, Mapping) else {}
        identity = summary.get("identity") if isinstance(summary.get("identity"), Mapping) else {}
        metadata = job.plan.get("metadata") if isinstance(job.plan.get("metadata"), Mapping) else {}
        raw_id = identity.get("tmdb_id") or metadata.get("tmdb_id") or summary.get("tmdb_id")
        if isinstance(raw_id, str) and raw_id.isdigit():
            raw_id = int(raw_id)
        tmdb_id = raw_id if type(raw_id) is int and raw_id > 0 else None
        raw_target = (
            identity.get("target_root") or metadata.get("series_root")
            or metadata.get("target_root") or job.plan.get("target_root")
            or summary.get("target_root")
        )
        target = raw_target if isinstance(raw_target, str) and raw_target.startswith("/") else None
        media_type = str(identity.get("media_type") or metadata.get("media_type")
                         or job.plan.get("mode") or summary.get("mode") or "").casefold()
        return tmdb_id, target, media_type

    def create_audit_owned_root(self, project: Mapping[str, object]) -> EngineJob:
        """Ensure one hidden implementation root for an audited media gap.

        The returned root is intentionally persisted as ``executed`` without
        an execution record: the audit observed the existing target and no
        formal media move occurred.  Only the provider worker may act on its
        validated ``resource_gaps`` by creating a separate internal child.
        Calling this method is local-state-only and never invokes AList, TMDB,
        a planner, or an executor.
        """
        work_key, target_root, tmdb_id, media_type, plan, gaps = self._audit_project_payload(project)
        parent_path = posixpath.dirname(target_root) or "/"
        season_values = {
            int(row["season"])
            for row in gaps
            if isinstance(row.get("season"), int)
        }
        season = next(iter(season_values)) if len(season_values) == 1 else 1
        request = asdict(EngineRequest.from_mapping({
            # This is an observed formal target used only as a placeholder;
            # the provider worker replaces it with task-owned staging before
            # any Engine child is planned.
            "source_path": target_root,
            "parent_path": parent_path,
            "media_type": media_type,
            "tmdb_id": tmdb_id,
            "query": plan["metadata"]["title"],
            "season": season,
            "auto_episode_mode": True,
            "prefer_simplified": True,
        }))
        now = _now()
        identity = {
            "media_type": media_type,
            "tmdb_id": tmdb_id,
            "title": plan["metadata"]["title"],
            "year": plan["metadata"].get("year") or "",
            "target_parent": parent_path,
            "target_root": target_root,
            "season": season if media_type == "tv" else None,
            "trace": {"source": "library_audit", "work_key": work_key},
        }
        base_summary: dict[str, object] = {
            "automatic": True,
            "audit_owned": True,
            "audit_work_key": work_key,
            "audit_origin": "library_audit",
            "automatic_stage": "gap_discovering",
            "source_root": "全库审计",
            "target_root": target_root,
            "mode": media_type,
            "title": plan["metadata"]["title"],
            "tmdb_id": tmdb_id,
            "file_count": 0,
            "cleanup_count": 0,
            "problem_count": 0,
            "warning_count": 0,
            "identity": identity,
            "resource_gaps": gaps,
            "last_audit_at": now,
            "automatic_attempts": 0,
            "automatic_terminal": False,
            "next_retry_seconds": None,
        }
        with self.worker_lock():
            existing_audit: EngineJob | None = None
            existing_work: EngineJob | None = None
            for candidate in self.list_jobs():
                if self._is_internal_job(candidate):
                    continue
                candidate_id, candidate_target, candidate_type = self._job_work_coordinates(candidate)
                if candidate_id != tmdb_id or candidate_target != target_root:
                    continue
                if candidate_type not in {media_type, "mixed"}:
                    continue
                if candidate.summary.get("audit_owned") is True:
                    existing_audit = candidate
                    break
                if candidate.phase in {"executed", "completed"}:
                    existing_work = candidate
            if existing_work is not None and existing_audit is None:
                # A historical completed root already owns this work.  Do not
                # manufacture a second root merely because its old plan was
                # not present in the current process snapshot.
                return existing_work
            if existing_audit is not None:
                summary = dict(existing_audit.summary)
                summary.update(base_summary)
                # Preserve provider attempt/child lineage while replacing the
                # audit's fresh gap observation.
                for key in ("replenishment", "replenishment_attempts"):
                    if key in existing_audit.summary:
                        summary[key] = existing_audit.summary[key]
                updated = replace(
                    existing_audit,
                    phase="executed",
                    updated_at=now,
                    request=request,
                    plan=plan,
                    summary=summary,
                    error=None,
                )
                if updated.as_dict() != existing_audit.as_dict():
                    atomic_write_json(
                        self._job_path(existing_audit.id), updated.as_dict(), allow_nan=False,
                    )
                return updated
            identifier = f"audit-{uuid.uuid4().hex}"
            job = EngineJob(
                id=identifier,
                phase="executed",
                created_at=now,
                updated_at=now,
                request=request,
                plan=plan,
                summary=base_summary,
                execution=None,
                error=None,
            )
            atomic_write_json(self._job_path(identifier), job.as_dict(), allow_nan=False)
            return job

    def create_audit_owned_subtitle_root(self, project: Mapping[str, object]) -> EngineJob:
        """Persist one local, subtitle-only audit owner.

        This method never invokes the planner, Engine executor, AList, or a
        metadata/artwork writer.  Its plan contains only exact subtitle gaps;
        ``AutomaticReplenishmentRuntime`` therefore enters its pure sidecar
        lane and cannot create a media child from this owner.
        """
        work_key, target_root, tmdb_id, media_type, plan, gaps = (
            self._audit_subtitle_project_payload(project)
        )
        parent_path = posixpath.dirname(target_root) or "/"
        season_values = {
            int(row.get("season")) for row in gaps
            if isinstance(row.get("season"), int) and 0 <= int(row.get("season")) <= 999
        }
        season = next(iter(season_values)) if len(season_values) == 1 else 1
        request = asdict(EngineRequest.from_mapping({
            # This is a validated formal target placeholder.  Pure subtitle
            # acquisition never executes this request as a media plan.
            "source_path": target_root,
            "parent_path": parent_path,
            "media_type": media_type,
            "tmdb_id": tmdb_id,
            "query": plan["metadata"]["title"],
            "season": season,
            "auto_episode_mode": True,
            "prefer_simplified": True,
        }))
        languages = {str(row.get("subtitle_language") or "").casefold() for row in gaps}
        now = _now()
        identity = {
            "media_type": media_type,
            "tmdb_id": tmdb_id,
            "title": plan["metadata"]["title"],
            "year": plan["metadata"].get("year") or "",
            "target_parent": parent_path,
            "target_root": target_root,
            "season": season if media_type == "tv" else None,
            "trace": {"source": "library_audit", "work_key": work_key},
        }
        base_summary: dict[str, object] = {
            "automatic": True,
            "audit_owned": True,
            "audit_subtitle_only": True,
            "subtitle_only": True,
            "audit_work_key": work_key,
            "audit_origin": "library_audit",
            "automatic_stage": "gap_discovering",
            "source_root": "全库审计",
            "target_root": target_root,
            "mode": media_type,
            "title": plan["metadata"]["title"],
            "tmdb_id": tmdb_id,
            "file_count": 0,
            "cleanup_count": 0,
            "problem_count": 0,
            "warning_count": 0,
            "identity": identity,
            "resource_gaps": gaps,
            "last_audit_at": now,
            "automatic_attempts": 0,
            "automatic_terminal": False,
            "next_retry_seconds": None,
            **({"audit_subtitle_language": next(iter(languages))} if len(languages) == 1 else {}),
        }
        with self.worker_lock():
            existing: EngineJob | None = None
            for candidate in self.list_jobs():
                if self._is_internal_job(candidate):
                    continue
                summary = candidate.summary if isinstance(candidate.summary, Mapping) else {}
                if (
                    summary.get("audit_subtitle_only") is True
                    and summary.get("audit_work_key") == work_key
                ):
                    existing = candidate
                    break
            if existing is not None:
                summary = dict(existing.summary)
                summary.update(base_summary)
                # Preserve provider attempt/child lineage while replacing the
                # fresh audit observation.  Never resurrect a media child.
                for key in ("replenishment", "replenishment_attempts"):
                    if key in existing.summary:
                        summary[key] = existing.summary[key]
                candidate = replace(
                    existing,
                    phase="executed",
                    request=request,
                    plan=plan,
                    summary=summary,
                    error=None,
                    # Keep identical scans idempotent at the durable-record
                    # level; a changed gap set receives a fresh timestamp.
                    updated_at=existing.updated_at,
                )
                if candidate.as_dict() != existing.as_dict():
                    candidate = replace(candidate, updated_at=now)
                    atomic_write_json(
                        self._job_path(existing.id), candidate.as_dict(), allow_nan=False,
                    )
                return candidate
            identifier = f"audit-{uuid.uuid4().hex}"
            job = EngineJob(
                id=identifier,
                phase="executed",
                created_at=now,
                updated_at=now,
                request=request,
                plan=plan,
                summary=base_summary,
                execution=None,
                error=None,
            )
            atomic_write_json(self._job_path(identifier), job.as_dict(), allow_nan=False)
            return job

    @staticmethod
    def _is_internal_job(job: EngineJob) -> bool:
        return isinstance(job.summary, Mapping) and job.summary.get("internal_child") is True

    def _owned_children(self, root: EngineJob) -> list[EngineJob]:
        return [
            candidate
            for candidate in self.list_jobs()
            if self._is_internal_job(candidate)
            and isinstance(candidate.summary, Mapping)
            and candidate.summary.get("root_job_id") == root.id
        ]

    def _pending_replenishment_reaudit_states(self, job_id: str) -> list[str]:
        """Return task-state markers that still own provider staging.

        The finalizer cannot remove a provider attempt itself: it lacks the
        selected-gap/audit evidence and must not turn into a second Provider
        coordinator.  It can, however, refuse to consume the ordinary root's
        source or erase local JSON while that evidence is still pending.
        """
        safe_id = _safe_job_id(job_id)
        state_id = re.sub(r"[^a-zA-Z0-9._-]+", "-", safe_id).strip(".-")[:96] or "job"
        directory = self.state_root / "gaps" / state_id
        if not directory.exists():
            return []
        if directory.is_symlink() or not directory.is_dir():
            return ["gap-state-directory-invalid"]
        pending: list[str] = []
        try:
            paths = sorted(path for path in directory.glob("*.json") if path.is_file())
        except OSError:
            return ["gap-state-directory-unreadable"]
        for path in paths:
            if path.is_symlink():
                pending.append(path.name)
                continue
            try:
                raw = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, UnicodeDecodeError, json.JSONDecodeError):
                # A damaged task-state record could be the only durable
                # indication of a held staging attempt.  Failing closed is
                # preferable to erasing it along with the root record.
                pending.append(path.name)
                continue
            if not isinstance(raw, Mapping):
                pending.append(path.name)
                continue
            if _POST_ACQUISITION_REAUDIT_KEY not in raw:
                continue
            marker = raw.get(_POST_ACQUISITION_REAUDIT_KEY)
            if (
                not isinstance(marker, Mapping)
                or str(marker.get("status") or "").casefold() != "cleaned"
            ):
                pending.append(path.name)
        return pending

    def has_pending_replenishment_reaudit(self, job_id: str) -> bool:
        """Publicly expose the finalizer's narrow provider-staging fence."""
        return bool(self._pending_replenishment_reaudit_states(job_id))

    def _validate_cleanup_root(self, root: EngineJob) -> list[EngineJob]:
        """Return terminal children after proving the root is safe to forget."""
        if self._is_internal_job(root):
            raise EngineExecutionError("只允许清理根任务，内部 child 不能单独清理")
        if root.phase not in _CLEANUP_TERMINAL_PHASES:
            raise EngineWorkerBusyError(f"任务仍在运行，不能清理记录: {root.phase}")
        summary = root.summary if isinstance(root.summary, Mapping) else {}
        if summary.get("automatic") is True and root.phase in {
            "executed", "completed", "failed_cleanup",
        }:
            lifecycle = summary.get("lifecycle")
            cleanup = lifecycle.get("cleanup") if isinstance(lifecycle, Mapping) else None
            if lifecycle is not None and (
                not isinstance(cleanup, Mapping)
                or cleanup.get("status") != "completed"
            ):
                raise EngineWorkerBusyError("任务最终 source/staging 清理尚未完成，不能删除记录")
        if (
            root.phase in {"failed", "failed_provider"}
            and summary.get("automatic") is True
            and summary.get("automatic_terminal") is not True
        ):
            raise EngineWorkerBusyError("任务仍会自动重试，不能清理记录")
        if self._pending_replenishment_reaudit_states(root.id):
            raise EngineWorkerBusyError(
                "任务仍有待定向重审/清理的补源 staging，不能删除记录"
            )
        replenishment = summary.get("replenishment")
        if isinstance(replenishment, Mapping):
            status = str(replenishment.get("status") or "").casefold()
            if status in _CLEANUP_ACTIVE_PROVIDER_STATUSES or (
                status
                and replenishment.get("terminal") is not True
                and status not in {"completed", "resolved", "ready"}
            ):
                raise EngineWorkerBusyError("任务仍有未终结的补源工作，不能清理记录")
        audit = summary.get("audit")
        if isinstance(audit, Mapping):
            audit_status = str(audit.get("status") or "").casefold()
            if (
                audit_status in _CLEANUP_RETRYABLE_AUDIT_STATUSES
                and audit.get("retryable") is not False
            ):
                raise EngineWorkerBusyError("任务仍有可重试审计工作，不能清理记录")
        children = self._owned_children(root)
        for child in children:
            if child.phase not in _CLEANUP_TERMINAL_PHASES:
                raise EngineWorkerBusyError(
                    f"根任务仍有活动 child，不能清理记录: {child.id}/{child.phase}"
                )
            child_summary = child.summary if isinstance(child.summary, Mapping) else {}
            child_replenishment = child_summary.get("replenishment")
            if isinstance(child_replenishment, Mapping):
                child_status = str(child_replenishment.get("status") or "").casefold()
                if child_status in _CLEANUP_ACTIVE_PROVIDER_STATUSES or (
                    child_status
                    and child_replenishment.get("terminal") is not True
                    and child_status not in {"completed", "resolved", "ready"}
                ):
                    raise EngineWorkerBusyError(
                        f"根任务仍有未终结 child 状态，不能清理记录: {child.id}"
                    )
        return children

    @staticmethod
    def _remove_owned_local_tree(parent: Path, owner_id: str, *, label: str) -> bool:
        """Remove exactly ``parent/<owner_id>`` and reject symlink escapes."""
        if parent.is_symlink():
            raise EngineExecutionError(f"{label} 根目录不允许符号链接")
        parent = parent.resolve()
        path = parent / _safe_job_id(owner_id)
        if path.parent != parent:
            raise EngineExecutionError(f"{label} 路径归属无效")
        if path.is_symlink():
            raise EngineExecutionError(f"{label} 不允许符号链接")
        if not path.exists():
            return False
        if path.is_dir():
            shutil.rmtree(path)
        else:
            # Gap state is normally a directory and local staging is always a
            # directory.  Unlinking an unexpected regular file is still
            # bounded to the exact task-owned name; never recurse its parent.
            path.unlink()
        return True

    def cleanup_terminal_job(self, job_id: str) -> dict[str, object]:
        """Safely discard one terminal root's local state.

        The method intentionally does not accept paths and never calls AList:
            only the root/owned-child JSON files, ``gaps/<root>``,
            ``staging/<root>`` and archive preprocessing staging under this
            runner's state root are in scope.  A
        formal media-library path cannot enter this operation.
        """
        safe_id = _safe_job_id(job_id)
        with self.worker_lock():
            root = self._read(safe_id)
            # Perform every ownership check while the same inter-process lock
            # is held, before the first destructive operation.
            children = self._validate_cleanup_root(root)

            if self.jobs_root.is_symlink():
                raise EngineExecutionError("Engine jobs 根目录不允许符号链接")
            root_path = self._job_path(safe_id)
            if root_path.is_symlink() or not root_path.is_file():
                raise EngineExecutionError(f"根任务 JSON 路径无效: {safe_id}")
            child_paths: list[tuple[str, Path]] = []
            for child in children:
                path = self._job_path(child.id)
                if path.is_symlink() or not path.is_file():
                    raise EngineExecutionError(f"child 任务 JSON 路径无效: {child.id}")
                child_paths.append((child.id, path))

            gaps_root = self.state_root / "gaps"
            staging_root = self.state_root / "staging"
            archive_staging_root = self.state_root / "archive-staging"
            removed_gap = self._remove_owned_local_tree(
                gaps_root, safe_id, label="gap state"
            )
            removed_staging = self._remove_owned_local_tree(
                staging_root, safe_id, label="local staging"
            )
            removed_archive_staging = self._remove_owned_local_tree(
                archive_staging_root, safe_id, label="archive local staging"
            )
            removed_children: list[str] = []
            for child_id, path in child_paths:
                path.unlink()
                removed_children.append(child_id)
            root_path.unlink()
            return {
                "job_id": safe_id,
                "removed": True,
                "removed_job_ids": [*removed_children, safe_id],
                "removed_child_job_ids": removed_children,
                "removed_gap": removed_gap,
                "removed_staging": removed_staging,
                "removed_archive_staging": removed_archive_staging,
                "formal_library_touched": False,
            }

    def clear_jobs(self) -> int:
        """Bulk state deletion is intentionally retired.

        Callers must name one terminal root through ``cleanup_terminal_job``;
        silently deleting every job cannot prove ownership or child quiescence.
        """
        raise EngineExecutionError("已禁用批量 Engine 任务清理，请按 terminal 根任务逐项清理")

    def delete_job(self, job_id: str) -> bool:
        """Compatibility alias for the safe terminal-root cleanup boundary."""
        try:
            result = self.cleanup_terminal_job(job_id)
        except EngineJobNotFoundError:
            return False
        return bool(result.get("removed"))

    @staticmethod
    def _ensure_authenticated(client: object) -> None:
        login = getattr(client, "login", None)
        if callable(login) and not getattr(client, "token", None):
            login()

    @staticmethod
    def _automatic_confidence() -> float:
        raw = os.getenv("SCRAPEFLOW_AUTO_MATCH_MIN_CONFIDENCE", "0.88").strip()
        try:
            value = float(raw)
        except ValueError:
            value = 0.88
        return max(0.5, min(0.99, value))

    def _target_root_for_confirmed_shelf(self, value: object) -> tuple[TargetShelf, str]:
        """Resolve a user-confirmed shelf without consulting TMDB heuristics."""
        try:
            shelf = parse_target_shelf(value)
            return shelf, target_root_for_shelf(self.library_root, shelf)
        except ValueError as exc:
            raise EngineRequestError(str(exc)) from exc

    @staticmethod
    def _require_plan_target_shelf_containment(
        plan: object,
        *,
        target_root: str,
        stage: str,
    ) -> None:
        """Keep every formal target inside one confirmed first-level shelf.

        The planner normally receives the selected root as its parent, but a
        persisted plan or an injected planner is still untrusted at the
        single-writer boundary. Validate the concrete work root and every
        target file before the plan can be stored or replayed.
        """
        shelf_root = _safe_remote_path(
            target_root,
            field=f"{stage} target_shelf_root",
            allow_root=False,
        )
        prefix = shelf_root + "/"

        def require_within(value: object, *, field: str) -> str:
            path = _safe_remote_path(value, field=f"{stage} {field}", allow_root=False)
            if not path.startswith(prefix):
                raise EngineRequestError(
                    f"{stage}拒绝目标货架外的路径: {path}"
                )
            return path

        def require_basename(value: object, *, field: str) -> str:
            if (
                not isinstance(value, str)
                or not value
                or value in {".", ".."}
                or "/" in value
                or "\\" in value
                or "\x00" in value
            ):
                raise EngineRequestError(f"{stage} {field} 必须是安全文件名")
            return value

        require_within(getattr(plan, "target_root", None), field="target_work_path")
        metadata = getattr(plan, "metadata", None)
        if isinstance(metadata, Mapping):
            series_root = metadata.get("series_root")
            if series_root is not None:
                require_within(series_root, field="metadata.series_root")
        for index, item in enumerate(list(getattr(plan, "files", ()) or ())):
            source_path = _safe_remote_path(
                getattr(item, "source_path", None),
                field=f"{stage} files[{index}].source_path",
                allow_root=False,
            )
            source_dir = _safe_remote_path(
                getattr(item, "source_dir", None),
                field=f"{stage} files[{index}].source_dir",
                allow_root=False,
            )
            original_name = require_basename(
                getattr(item, "original_name", None),
                field=f"files[{index}].original_name",
            )
            if source_path != posixpath.join(source_dir, original_name):
                raise EngineRequestError(
                    f"{stage} files[{index}] 的来源路径与文件名不一致"
                )
            target_dir = require_within(
                getattr(item, "target_dir", None),
                field=f"files[{index}].target_dir",
            )
            final_name = require_basename(
                getattr(item, "final_name", None),
                field=f"files[{index}].final_name",
            )
            # The executor checks this intermediate path before a rename, so
            # it is a formal-library target too (not just the final name).
            require_within(
                posixpath.join(target_dir, original_name),
                field=f"files[{index}].intermediate_target_path",
            )
            require_within(
                posixpath.join(target_dir, final_name),
                field=f"files[{index}].target_path",
            )

        # NFO and artwork targets are derived from metadata (collection/batch
        # member roots in particular), not necessarily from ``files``.  Run
        # the same pure projections used by the writer and gate every target
        # before a plan can be persisted, replayed, or repaired.
        try:
            engine = __import__("engine.scraper", fromlist=["planned_nfos", "planned_artwork"])
            artifact_functions = (
                ("nfo", getattr(engine, "planned_nfos", None)),
                ("artwork", getattr(engine, "planned_artwork", None)),
            )
            for kind, function in artifact_functions:
                if not callable(function):
                    raise EngineRequestError(f"{stage}缺少 {kind} 目标投影")
                outputs = function(plan)
                if not isinstance(outputs, (list, tuple)):
                    raise EngineRequestError(f"{stage}{kind} 目标投影格式无效")
                for index, output in enumerate(outputs):
                    if not isinstance(output, (list, tuple)) or not output:
                        raise EngineRequestError(f"{stage}{kind}[{index}] 目标投影格式无效")
                    require_within(output[0], field=f"{kind}[{index}].target_path")
        except EngineRequestError:
            raise
        except Exception as exc:
            raise EngineRequestError(f"{stage}无法验证元数据/艺术图目标: {exc}") from exc

    def _require_persisted_target_shelf_containment(
        self,
        job: EngineJob,
        plan: object,
        *,
        stage: str,
    ) -> None:
        """Revalidate a selected job before any execution or recovery.

        Plain direct planner tests/tools predate the public automatic intake
        and are not ordinary roots.  A durable automatic root, however, can
        never use that compatibility lane to write or recover without a
        complete user selection.
        """
        if job.target_shelf is None and job.target_root is None and job.selected_at is None:
            reconciliation = job.summary.get("reconciliation")
            if (
                isinstance(reconciliation, Mapping)
                and reconciliation.get("outcome") == "merge_existing"
            ):
                identity, _selected_shelf, expected_root, work_root = (
                    self._merge_existing_context(job.summary)
                )
                self._require_plan_target_shelf_containment(
                    plan,
                    target_root=expected_root,
                    stage=stage,
                )
                self._require_merge_existing_plan_target(
                    plan,
                    identity=identity,
                    work_root=work_root,
                    stage=stage,
                )
                return
            if (
                isinstance(reconciliation, Mapping)
                and reconciliation.get("outcome") == "new_work"
                and not isinstance(job.summary.get("manual_identity"), Mapping)
            ):
                identity = self._reconciled_identity(reconciliation.get("identity"))
                self._require_reconciled_plan_identity(
                    plan,
                    identity=identity,
                    stage=stage,
                )
            if (
                job.summary.get("automatic") is True
                and job.summary.get("audit_owned") is not True
                and job.summary.get("internal_child") is not True
            ):
                raise EngineRequestError("自动任务尚未选择目标货架，不能进入正式处理")
            return
        _selected, expected_root = self._confirmed_target_selection(job)
        self._require_plan_target_shelf_containment(
            plan,
            target_root=expected_root,
            stage=stage,
        )
        reconciliation = job.summary.get("reconciliation")
        if (
            isinstance(reconciliation, Mapping)
            and reconciliation.get("outcome") == "new_work"
            and not isinstance(job.summary.get("manual_identity"), Mapping)
        ):
            self._require_reconciled_plan_identity(
                plan,
                identity=self._reconciled_identity(reconciliation.get("identity")),
                stage=stage,
            )

    def _archive_task_roots(self, job_id: str) -> tuple[Path, str]:
        """Return deterministic local/remote task-owned archive roots.

        The roots are derived from the durable job id, never from an inbound
        basename.  This gives restart/cleanup code an exact ownership key and
        keeps an archive output outside both the source tree and formal shelf.
        """
        safe_id = _safe_job_id(job_id)
        local_root = self.state_root / "archive-staging" / safe_id
        remote_root = _safe_remote_path(
            f"{self.library_root}/ScrapeFlow/归档/{safe_id}",
            field="archive remote staging root",
            allow_root=False,
        )
        return local_root, remote_root

    def _preprocess_ordinary_request_details(
        self,
        request: EngineRequest,
        *,
        job_id: str,
        retry_password: str | None = None,
    ) -> tuple[EngineRequest, Mapping[str, object] | None]:
        """Let an injected archive adapter replace only a source with staging.

        The composition root supplies both sides of the ownership boundary.
        Small test adapters from the migration era may expose the old narrow
        signature, so the fallback is intentionally one-way and never passes a
        password or a formal-library target.
        """

        adapter = self.archive_preprocessor
        method = getattr(adapter, "prepare_ordinary_request", None)
        if not callable(method):
            return request, None
        # Archive inspection may perform the first remote listing/download;
        # authenticate at this post-start boundary before invoking it.
        self._ensure_authenticated(self.alist)
        local_staging, remote_staging = self._archive_task_roots(job_id)
        kwargs = {
            "alist": self.alist,
            "task_staging": local_staging,
            "remote_staging_root": remote_staging,
        }
        if retry_password is not None:
            kwargs["retry_password"] = retry_password
        try:
            prepared = method(asdict(request), **kwargs)
        except TypeError:
            try:
                # Small migration/test adapters may accept ``alist`` but not
                # concrete staging kwargs.
                prepared = method(asdict(request), alist=self.alist)
            except TypeError:
                prepared = method(asdict(request))
        if not isinstance(prepared, Mapping):
            raise EngineRequestError("归档预处理返回无效请求")
        source = prepared.get("source_path", request.source_path)
        if not isinstance(source, str):
            raise EngineRequestError("归档预处理来源路径无效")
        normalized = _safe_remote_path(source, field="archive source_path", allow_root=False)
        projection = prepared.get("archive_preprocessed")
        if not isinstance(projection, Mapping):
            projection = None
        return replace(request, source_path=normalized), projection

    def _reusable_archive_projection(
        self,
        job: EngineJob,
        request: EngineRequest,
    ) -> tuple[EngineRequest, Mapping[str, object] | None]:
        """Reuse one durable archive staging projection after a conflict.

        A target-shelf conflict happens after archive extraction has already
        been verified.  Re-running the adapter on reselection would create a
        second task staging tree (and can consume a one-shot archive source).
        Only accept a projection that still points inside this job's exact
        archive staging root; malformed or foreign records fail closed.
        """
        raw = job.summary.get("archive_preprocessed")
        if not isinstance(raw, Mapping) or raw.get("changed") is not True:
            return request, None
        source = raw.get("source_path")
        task_staging = raw.get("task_staging")
        if not isinstance(source, str) or not isinstance(task_staging, str):
            raise EngineRequestError("已验证的归档 staging 记录不完整")
        normalized_source = _safe_remote_path(
            source,
            field="已验证归档 source_path",
            allow_root=False,
        )
        expected_local, expected_remote = self._archive_task_roots(job.id)
        local_staging = Path(task_staging)
        if not local_staging.is_absolute():
            raise EngineRequestError("已验证的归档 task_staging 必须是绝对本地路径")
        if local_staging.is_symlink():
            raise EngineRequestError("已验证的归档 task_staging 不能是符号链接")
        try:
            normalized_local = local_staging.resolve(strict=False)
            normalized_expected_local = expected_local.resolve(strict=False)
        except OSError as exc:
            raise EngineRequestError("已验证的归档 task_staging 无法解析") from exc
        if normalized_local != normalized_expected_local:
            raise EngineRequestError("已验证的归档 staging 不属于当前任务")
        archive_prefix = expected_remote + "/archive/"
        if not normalized_source.startswith(archive_prefix):
            raise EngineRequestError("已验证的归档 source 不属于当前任务 staging")
        return replace(request, source_path=normalized_source), dict(raw)

    def _preprocess_ordinary_request(
        self, request: EngineRequest, *, job_id: str | None = None,
    ) -> EngineRequest:
        """Compatibility wrapper for focused callers of the old private hook."""
        if job_id is None:
            job_id = f"preprocess-{uuid.uuid4().hex}"
        result, _projection = self._preprocess_ordinary_request_details(
            request, job_id=job_id,
        )
        return result

    def _resolve_automatic_identity(
        self,
        source_path: str,
        *,
        target_parent: str | None = None,
    ) -> tuple[str, AutomaticIdentity]:
        """Use the existing Engine matcher without requiring a destination.

        A confirmed shelf remains a planning/write policy.  It can provide a
        helpful source-context fallback *after* selection, but no destination
        is needed for the harmless query, TMDB match, or season inspection
        used by read-only intake reconciliation.
        """
        source = _safe_remote_path(source_path, field="source_path", allow_root=False)
        parent = (
            _safe_remote_path(target_parent, field="target_parent", allow_root=False)
            if target_parent is not None
            else None
        )
        self._ensure_authenticated(self.alist)
        engine = __import__("engine.scraper", fromlist=["auto_match_tmdb"])
        query_fn = getattr(engine, "_query_from_source", None)
        query = query_fn(source) if callable(query_fn) else posixpath.basename(source)
        query = str(query).strip()
        requested_type: str | None = None
        prefer_animation = False
        if parent is not None:
            context_fn = getattr(engine, "_media_context_from_source_and_target", None)
            if callable(context_fn):
                requested_type, prefer_animation = context_fn(source, parent)
        else:
            source_type = getattr(engine, "_media_type_from_source_context", None)
            source_animation = getattr(engine, "_source_is_animation_library", None)
            if callable(source_type):
                candidate_type = source_type(source)
                requested_type = candidate_type if candidate_type in {"movie", "tv"} else None
            if callable(source_animation):
                prefer_animation = bool(source_animation(source))
        expected_episode_count: int | None = None
        if requested_type == "tv":
            try:
                rows = self.alist.walk(source, ignore_orphan_temp=True)
                expected_fn = getattr(engine, "_expected_single_tv_episode_count", None)
                if callable(expected_fn):
                    expected_episode_count = expected_fn(rows)
            except Exception:
                expected_episode_count = None
        matcher = getattr(engine, "auto_match_tmdb", None)
        if not callable(matcher):
            raise EngineRequestError("当前 Engine 没有自动匹配能力")
        match, candidates = matcher(
            self.tmdb,
            query,
            media_type=requested_type,
            min_confidence=self._automatic_confidence(),
            prefer_animation=prefer_animation,
            expected_episode_count=expected_episode_count,
        )
        trace = dict(getattr(match, "decision_trace", {}) or {})
        trace["top_candidates"] = [
            {
                "media_type": candidate.media_type,
                "tmdb_id": candidate.tmdb_id,
                "title": candidate.title,
                "year": candidate.year,
                "confidence": candidate.confidence,
                "status": candidate.status,
            }
            for candidate in list(candidates)[:5]
        ]
        season_fn = getattr(engine, "_season_from_source", None)
        season = season_fn(source) if callable(season_fn) else None
        media_type = str(getattr(match, "media_type", ""))
        if media_type == "tv" and not isinstance(season, int):
            season = 1
        return query, AutomaticIdentity(
            media_type=media_type,
            tmdb_id=int(getattr(match, "tmdb_id")),
            title=str(getattr(match, "title", "")),
            year=str(getattr(match, "year", "未知年份")),
            confidence=float(getattr(match, "confidence", 0.0)),
            target_parent=parent,
            season=season if isinstance(season, int) else None,
            trace=trace,
        )

    def resolve_automatic_identity(self, source_path: str) -> AutomaticIdentity:
        """Resolve intake identity for reconciliation without selecting a shelf."""
        _query, identity = self._resolve_automatic_identity(source_path)
        return identity

    def resolve_automatic_request(
        self,
        source_path: str,
        payload: Mapping[str, object] | None = None,
        *,
        target_shelf: object | None = None,
    ) -> tuple[EngineRequest, AutomaticIdentity]:
        """Adapt the shared identity result to a post-selection plan request."""
        del payload
        if target_shelf is None:
            raise EngineRequestError("自动任务尚未选择目标货架")
        selected_shelf, selected_root = self._target_root_for_confirmed_shelf(target_shelf)
        source = _safe_remote_path(source_path, field="source_path", allow_root=False)
        query, resolved = self._resolve_automatic_identity(
            source,
            target_parent=selected_root,
        )
        identity = replace(
            resolved,
            target_parent=selected_root,
            target_shelf=selected_shelf.value,
            target_shelf_root=selected_root,
        )
        if not target_shelf_allows_media_type(selected_shelf, identity.media_type):
            raise TargetShelfPolicyConflictError(
                target_shelf=selected_shelf,
                media_type=identity.media_type,
                identity=identity,
            )
        request = EngineRequest.from_mapping({
            "source_path": source,
            "parent_path": selected_root,
            "media_type": identity.media_type,
            "target_shelf": selected_shelf.value,
            "tmdb_id": identity.tmdb_id,
            "query": query,
            "season": identity.season if isinstance(identity.season, int) else 1,
        })
        return request, identity

    def _request_from_manual_identity(
        self,
        source: str,
        correction: Mapping[str, object],
        *,
        target_shelf: object,
    ) -> tuple[EngineRequest, AutomaticIdentity]:
        """Build a bounded explicit retry request after identity failure.

        This is deliberately a retry-only escape hatch.  It accepts one TMDB
        id, one media type and (for TV) one season; it never accepts a target
        path outside the configured library root or an arbitrary planner map.
        """
        forbidden = set(correction) - {"tmdb_id", "media_type", "season"}
        if forbidden:
            raise EngineRequestError("手工修正包含不支持的字段")
        raw_id = correction.get("tmdb_id")
        if isinstance(raw_id, str) and raw_id.isascii() and raw_id.isdecimal():
            raw_id = int(raw_id)
        if isinstance(raw_id, bool) or not isinstance(raw_id, int) or raw_id <= 0:
            raise EngineRequestError("手工修正 tmdb_id 必须是正整数")
        media_type = str(correction.get("media_type") or "").strip().casefold()
        if media_type not in {"movie", "tv"}:
            raise EngineRequestError("手工修正 media_type 必须是 movie 或 tv")
        raw_season = correction.get("season", 1)
        if isinstance(raw_season, str) and raw_season.isascii() and raw_season.isdecimal():
            raw_season = int(raw_season)
        if isinstance(raw_season, bool) or not isinstance(raw_season, int) or raw_season < 0 or raw_season > 999:
            raise EngineRequestError("手工修正 season 必须是 0–999 的整数")
        # Keep title/year and target parent policy-derived.  The correction
        # request must never become a second, client-controlled planner.
        title = posixpath.basename(source.rstrip("/")) or str(raw_id)
        selected_shelf, parent = self._target_root_for_confirmed_shelf(target_shelf)
        year_text = "未知年份"
        identity = AutomaticIdentity(
            media_type=media_type,
            tmdb_id=raw_id,
            title=title,
            year=year_text,
            confidence=1.0,
            target_parent=parent,
            season=raw_season if media_type == "tv" else None,
            trace={"source": "manual_retry"},
            target_shelf=selected_shelf.value,
            target_shelf_root=parent,
        )
        if not target_shelf_allows_media_type(selected_shelf, media_type):
            raise TargetShelfPolicyConflictError(
                target_shelf=selected_shelf,
                media_type=media_type,
                identity=identity,
            )
        request = EngineRequest.from_mapping({
            "source_path": source,
            "parent_path": parent,
            "media_type": media_type,
            "target_shelf": selected_shelf.value,
            "tmdb_id": raw_id,
            "query": title,
            "season": raw_season if media_type == "tv" else 1,
        })
        return request, identity

    def _build_plan(self, request: EngineRequest) -> object:
        self._ensure_authenticated(self.alist)
        if self.planner is not None:
            plan = self.planner(request, self.alist, self.tmdb)
            if isinstance(plan, Mapping):
                engine = __import__("engine.scraper", fromlist=["plan_from_dict"])
                plan = engine.plan_from_dict(plan)
        else:
            engine = __import__("engine.scraper", fromlist=["build_movie_plan"])
            current = request
            if current.tmdb_id is None:
                matcher = getattr(engine, "auto_match_tmdb", None)
                if not callable(matcher):
                    raise SimpleEngineError("当前 Engine 没有 auto_match_tmdb")
                query = current.query or posixpath.basename(current.source_path)
                requested_type = None if current.media_type == "auto" else current.media_type
                match, _candidates = matcher(
                    self.tmdb,
                    query,
                    media_type=requested_type,
                )
                current = replace(
                    current,
                    media_type=str(match.media_type),
                    tmdb_id=int(match.tmdb_id),
                )
            if current.media_type == "movie":
                plan = engine.build_movie_plan(
                    self.alist,
                    self.tmdb,
                    src_path=current.source_path,
                    parent_path=current.parent_path,
                    tmdb_id=int(current.tmdb_id),
                    ignore_orphan_temp=current.ignore_orphan_temp,
                )
            elif current.media_type == "tv":
                plan = engine.build_tv_plan_smart(
                    auto_episode_mode=current.auto_episode_mode,
                    alist=self.alist,
                    tmdb_client=self.tmdb,
                    src_path=current.source_path,
                    parent_path=current.parent_path,
                    tmdb_id=int(current.tmdb_id),
                    season=current.season,
                    absolute=current.absolute,
                    prefer_simplified=current.prefer_simplified,
                    allow_unmapped=current.allow_unmapped,
                    ignore_orphan_temp=current.ignore_orphan_temp,
                    episode_map_path=None,
                    episode_group_id=current.episode_group_id,
                    media_root=self.library_root,
                )
            elif current.media_type == "collection":
                if current.collection_map:
                    raise EngineRequestError(
                        "简化入口暂不把内嵌 collection_map 写成临时文件；请使用 allow_index_mapping 或注入 planner"
                    )
                plan = engine.build_collection_plan(
                    self.alist,
                    self.tmdb,
                    src_path=current.source_path,
                    parent_path=current.parent_path,
                    tmdb_id=int(current.tmdb_id),
                    mapping_path=None,
                    allow_index_mapping=current.allow_index_mapping,
                    ignore_orphan_temp=current.ignore_orphan_temp,
                )
            else:
                raise EngineRequestError("无法确定媒体类型；请提供 movie、tv 或 collection")

        # ``finalize_plan_evidence`` belonged to the retired transaction-era
        # planner.  The current model's finalizer is still required for every
        # runner plan, including injected planners used by provider children
        # and tests, because it builds the persisted scan-report projection.
        engine = __import__("engine.scraper", fromlist=["finalize_plan"])
        finalize = getattr(engine, "finalize_plan", None)
        if not callable(finalize):
            raise SimpleEngineError("当前 Engine 缺少 finalize_plan")
        finalized = finalize(plan)
        if finalized is not None:
            plan = finalized
        if self.validate:
            validate = getattr(engine, "validate_plan", None)
            if callable(validate):
                try:
                    validate(self.alist, plan, media_root=self.library_root)
                except TypeError as exc:
                    # Injected/legacy Engine validators may still expose the
                    # old two-argument contract.  Only signature-level
                    # incompatibility gets the compatibility call; an
                    # internal TypeError must not replay validation blindly.
                    if "media_root" not in str(exc):
                        raise
                    validate(self.alist, plan)
        return plan

    @staticmethod
    def _summary(plan: object) -> dict[str, object]:
        files = list(getattr(plan, "files", ()) or ())
        cleanup = list(getattr(plan, "cleanup_files", ()) or ())
        problems = list(getattr(plan, "problem_files", ()) or ())
        metadata = getattr(plan, "metadata", {}) or {}
        return {
            "mode": str(getattr(plan, "mode", "")),
            "source_root": str(getattr(plan, "source_root", "")),
            "target_root": str(getattr(plan, "target_root", "")),
            "title": _jsonable(metadata.get("title")) if isinstance(metadata, Mapping) else None,
            "tmdb_id": _jsonable(metadata.get("tmdb_id")) if isinstance(metadata, Mapping) else None,
            "file_count": len(files),
            "cleanup_count": len(cleanup),
            "problem_count": len(problems),
            "warning_count": len(list(getattr(plan, "warnings", ()) or ())),
        }

    def plan_job(
        self,
        request: EngineRequest | Mapping[str, object],
        *,
        job_id: str | None = None,
        internal_child_of: str | None = None,
        skip_archive_preprocessing: bool = False,
    ) -> EngineJob:
        """Build and persist one plan.

        ``internal_child_of`` is written into the first durable JSON record
        when a provider creates a child.  That removes the small crash window
        in which a planned child existed but had not yet been marked hidden
        from the public root queue.
        """
        request = request if isinstance(request, EngineRequest) else EngineRequest.from_mapping(request)
        selected_shelf: TargetShelf | None = None
        selected_root: str | None = None
        if request.target_shelf is not None:
            selected_shelf, selected_root = self._target_root_for_confirmed_shelf(
                request.target_shelf,
            )
            if request.parent_path != selected_root:
                raise EngineRequestError("Engine 请求 parent_path 必须等于已确认目标货架根目录")
        if job_id is None:
            job_id = f"engine-{uuid.uuid4().hex}"
        _safe_job_id(job_id)
        if internal_child_of is not None:
            _safe_job_id(internal_child_of)
        if self._job_path(job_id).exists():
            raise SimpleEngineError(f"Engine job 已存在: {job_id}")
        original_source = request.source_path
        archive_projection: Mapping[str, object] | None = None
        if internal_child_of is None and not skip_archive_preprocessing:
            request, archive_projection = self._preprocess_ordinary_request_details(
                request, job_id=job_id,
            )
        plan = self._build_plan(request)
        if selected_root is not None:
            self._require_plan_target_shelf_containment(
                plan,
                target_root=selected_root,
                stage="计划生成",
            )
        if internal_child_of is not None:
            try:
                _require_provider_tv_child_primary_videos(plan, stage="child 计划")
            except ValueError as exc:
                raise EngineRequestError(str(exc)) from exc
        engine = __import__("engine.scraper", fromlist=["plan_to_dict"])
        serializer = getattr(engine, "plan_to_dict", None)
        if not callable(serializer):
            raise SimpleEngineError("Engine 缺少 plan_to_dict")
        body = serializer(plan)
        if not isinstance(body, Mapping):
            raise SimpleEngineError("Engine 计划序列化结果无效")
        if internal_child_of is not None:
            # Keep the child mode in the persisted plan itself.  The summary
            # is only a queue/UI projection and is not available to the
            # executor during restart recovery.
            body = _mark_provider_media_only_body(body)
        now = _now()
        summary = self._summary(plan)
        if request.source_path != original_source:
            summary["ingress_source_path"] = original_source
        if archive_projection is not None:
            summary["archive_preprocessed"] = dict(archive_projection)
        if internal_child_of is not None:
            summary.update({
                "internal_child": True,
                "root_job_id": internal_child_of,
                "provider_media_only": True,
            })
        job = EngineJob(
            id=job_id,
            phase="planned",
            created_at=now,
            updated_at=now,
            request=asdict(request),
            plan=dict(body),
            summary=summary,
            target_shelf=selected_shelf.value if selected_shelf is not None else None,
            target_root=selected_root,
            selected_at=now if selected_shelf is not None else None,
        )
        atomic_write_json(self._job_path(job_id), job.as_dict(), allow_nan=False)
        return job

    def mark_internal_child(self, job_id: str, *, root_job_id: str) -> EngineJob:
        """Associate one provider-created child with its visible root job.

        A replenishment child is an implementation detail: it may have its
        own persisted Engine plan so restart recovery can finish it safely,
        but it must never turn into a second user-facing task or a new source
        of provider work.  Store the relationship in the durable summary so
        the HTTP composition root and library audit can filter it without
        guessing from its staging path.
        """
        root_id = _safe_job_id(root_job_id)
        with self.worker_lock():
            job = self._read(job_id)
            summary = dict(job.summary)
            summary["internal_child"] = True
            summary["root_job_id"] = root_id
            try:
                _require_provider_tv_child_primary_videos(
                    self._plan_from_job(job), stage="child 标记",
                )
            except ValueError as exc:
                raise EngineRequestError(str(exc)) from exc
            plan = _mark_provider_media_only_body(job.plan)
            summary["provider_media_only"] = True
            updated = replace(job, plan=plan, summary=summary, updated_at=_now())
            atomic_write_json(self._job_path(job_id), updated.as_dict(), allow_nan=False)
            return updated

    def plan_automatic(
        self,
        source_path: str,
        payload: Mapping[str, object] | None = None,
        *,
        job_id: str | None = None,
        target_shelf: object | None = None,
    ) -> EngineJob:
        """Resolve and persist one already-shelf-selected automatic plan.

        This direct helper is used by focused tests and internal tooling; it
        deliberately has the same selection gate as the durable HTTP path.
        """
        if job_id is None:
            job_id = f"engine-{uuid.uuid4().hex}"
        original_source = _safe_remote_path(source_path, field="source_path", allow_root=False)
        if target_shelf is None:
            raise EngineRequestError("自动任务尚未选择目标货架")
        selected_shelf, selected_root = self._target_root_for_confirmed_shelf(target_shelf)
        intake = EngineRequest.from_mapping({
            "source_path": original_source,
            "parent_path": selected_root,
            "media_type": "auto",
            "target_shelf": selected_shelf.value,
        })
        intake, archive_projection = self._preprocess_ordinary_request_details(
            intake, job_id=job_id,
        )
        request, identity = self.resolve_automatic_request(
            intake.source_path,
            payload,
            target_shelf=selected_shelf,
        )
        job = self.plan_job(request, job_id=job_id, skip_archive_preprocessing=True)
        summary = dict(job.summary)
        summary.update({
            "identity": identity.as_dict(),
            "automatic": True,
            "target_shelf": selected_shelf.value,
            "selected_target_root": selected_root,
            "target_work_path": summary.get("target_root"),
        })
        if original_source != request.source_path:
            summary["ingress_source_path"] = original_source
        if archive_projection is not None:
            summary["archive_preprocessed"] = dict(archive_projection)
        summary["resource_gaps"] = list(
            (job.plan.get("scan_report") or {}).get("resource_gaps") or []
        ) if isinstance(job.plan.get("scan_report"), Mapping) else []
        updated = replace(job, summary=summary, updated_at=_now())
        atomic_write_json(self._job_path(job.id), updated.as_dict(), allow_nan=False)
        return updated

    def plan_automatic_job(
        self,
        job_id: str,
        *,
        retry_password: str | None = None,
        pause_requested: Callable[[], bool] | None = None,
    ) -> EngineJob:
        """Resolve and persist the plan for an already queued source job."""
        with self.worker_lock():
            job = self._read(job_id)
            effective_pause = pause_requested if pause_requested is not None else self._pause_requested
            cancelled = self._consume_cancel_request(job)
            if cancelled is not None:
                return cancelled
            _pause_checkpoint(effective_pause)
            if job.phase == "planned":
                return job
            if job.phase == "executed":
                return job
            if job.phase == "awaiting_target_shelf":
                raise EngineRequestError("自动任务尚未选择目标货架，不能规划")
            if job.phase == "target_policy_conflict":
                return job
            if job.phase not in {
                "queued", "archive_preprocessing", "identity_matching",
                "planning", "retry_wait", "failed_identity",
            }:
                raise EngineJobConflictError(
                    f"Engine job {job_id} 当前不能规划: {job.phase}"
                )
            reconciliation = (
                job.summary.get("reconciliation")
                if isinstance(job.summary.get("reconciliation"), Mapping)
                else {}
            )
            reconciliation_outcome = str(reconciliation.get("outcome") or "")
            merge_existing = reconciliation_outcome == "merge_existing"
            reconciled_new_work = reconciliation_outcome == "new_work"
            merge_identity: AutomaticIdentity | None = None
            merge_work_root: str | None = None
            reconciled_identity: AutomaticIdentity | None = None
            selected_shelf, selected_root = self._confirmed_target_selection(job)
            if merge_existing:
                # This is a post-reconciliation planning boundary.  The
                # identity and concrete work root are already authoritative;
                # do not call TMDB/matcher again or ask for a new shelf.
                merge_identity, selected_shelf, selected_root, merge_work_root = (
                    self._merge_existing_context(job.summary)
                )
            elif reconciled_new_work and not isinstance(job.summary.get("manual_identity"), Mapping):
                # Once read-only reconciliation has established a genuine
                # new work, shelf selection is only a policy confirmation.
                # Reuse that durable Engine identity instead of allowing a
                # second TMDB/matcher pass to reinterpret the same intake.
                reconciled_identity = self._reconciled_identity(
                    reconciliation.get("identity"),
                )
            original_source = self._job_ingress_source(job)
            intake = EngineRequest.from_mapping({
                "source_path": original_source,
                "parent_path": selected_root,
                "media_type": "auto",
                "target_shelf": selected_shelf.value,
            })
            archiving_summary = self._with_active_operation(
                {**job.summary, "automatic_stage": "archive_preprocessing"},
                kind="planning",
            )
            archiving = replace(
                job,
                phase="archive_preprocessing",
                updated_at=_now(),
                error=None,
                summary=archiving_summary,
            )
            atomic_write_json(self._job_path(job_id), archiving.as_dict(), allow_nan=False)
            pause_token = _PAUSE_REQUEST_CHECK.set(effective_pause)
            try:
                _cancellation_checkpoint()
                archive_request, archive_projection = self._reusable_archive_projection(job, intake)
                if archive_projection is None:
                    archive_request, archive_projection = self._preprocess_ordinary_request_details(
                        intake, job_id=job_id, retry_password=retry_password,
                    )
                cancelled = self._consume_cancel_request(archiving)
                if cancelled is not None:
                    return cancelled
            except EnginePauseRequested:
                # Keep the active archive operation durable.  A later resume
                # re-enters this same phase and can reuse its projection;
                # pause never becomes a terminal cancellation.
                return self._read(job_id)
            except (EngineRequestError, ArchivePasswordError) as exc:
                summary = dict(archiving.summary)
                summary.update({
                    "automatic_terminal": True,
                    "automatic_stage": "failed_archive",
                    "archive_projection_status": "invalid",
                })
                summary = self._without_active_operation(summary)
                failed = replace(
                    archiving,
                    phase="failed_archive",
                    updated_at=_now(),
                    summary=summary,
                    error=redact_error(exc),
                )
                atomic_write_json(self._job_path(job_id), failed.as_dict(), allow_nan=False)
                raise
            finally:
                _PAUSE_REQUEST_CHECK.reset(pause_token)
            archive_summary = dict(archiving.summary)
            if archive_projection is not None:
                archive_summary["archive_preprocessed"] = dict(archive_projection)
            if archive_request.source_path != original_source:
                archive_summary["ingress_source_path"] = original_source
                archive_summary["archive_source_path"] = archive_request.source_path
            matching = replace(
                archiving,
                phase="identity_matching",
                updated_at=_now(),
                request={"source_path": archive_request.source_path},
                summary=archive_summary,
            )
            atomic_write_json(self._job_path(job_id), matching.as_dict(), allow_nan=False)
            # B/W step: understand the directory structure before any TMDB
            # identity work (contract rule 3).  The WorkUnit ledger is
            # persisted beside the job for the per-unit identity stage (C/U);
            # it stays advisory here so the legacy chain keeps its current
            # behavior until C consumes it.
            try:
                from engine.scrapeflow.root_boundaries import analyze_root_boundaries
                analyze_root_boundaries(
                    self.alist,
                    archive_request.source_path,
                    root_task_id=job_id,
                    state_root=self.state_root,
                )
            except Exception:
                pass  # Advisory in P3; the C/U stage will make it authoritative.
            try:
                _pause_checkpoint(effective_pause)
            except EnginePauseRequested:
                return self._read(job_id)
            cancelled = self._consume_cancel_request(matching)
            if cancelled is not None:
                return cancelled
            correction = job.summary.get("manual_identity")
            try:
                if merge_existing and merge_identity is not None:
                    request = self._request_from_reconciled_identity(
                        archive_request.source_path,
                        identity=replace(
                            merge_identity,
                            target_parent=selected_root,
                            target_shelf=selected_shelf.value,
                            target_shelf_root=selected_root,
                        ),
                        shelf=selected_shelf,
                        shelf_root=selected_root,
                    )
                    identity = replace(
                        merge_identity,
                        target_parent=selected_root,
                        target_shelf=selected_shelf.value,
                        target_shelf_root=selected_root,
                    )
                elif (
                    reconciled_new_work
                    and reconciled_identity is not None
                    and not isinstance(correction, Mapping)
                ):
                    if not target_shelf_allows_media_type(
                        selected_shelf, reconciled_identity.media_type,
                    ):
                        raise TargetShelfPolicyConflictError(
                            target_shelf=selected_shelf,
                            media_type=reconciled_identity.media_type,
                            identity=reconciled_identity,
                        )
                    identity = replace(
                        reconciled_identity,
                        target_parent=selected_root,
                        target_shelf=selected_shelf.value,
                        target_shelf_root=selected_root,
                    )
                    request = self._request_from_reconciled_identity(
                        archive_request.source_path,
                        identity=identity,
                        shelf=selected_shelf,
                        shelf_root=selected_root,
                    )
                elif isinstance(correction, Mapping):
                    request, identity = self._request_from_manual_identity(
                        archive_request.source_path,
                        correction,
                        target_shelf=selected_shelf,
                    )
                else:
                    request, identity = self.resolve_automatic_request(
                        archive_request.source_path,
                        target_shelf=selected_shelf,
                    )
                cancelled = self._consume_cancel_request(matching)
                if cancelled is not None:
                    return cancelled
            except TargetShelfPolicyConflictError as exc:
                summary = dict(matching.summary)
                summary.update({
                    "automatic": True,
                    "automatic_stage": "target_policy_conflict",
                    "target_shelf": selected_shelf.value,
                    "selected_target_root": selected_root,
                    "target_work_path": None,
                })
                if archive_projection is not None:
                    # Keep the verified staging coordinates across the
                    # conflict/reselection boundary; the next /start must
                    # consume this projection without re-extracting.
                    summary["archive_preprocessed"] = dict(archive_projection)
                if archive_request.source_path != original_source:
                    summary["ingress_source_path"] = original_source
                if isinstance(exc.identity, AutomaticIdentity):
                    summary["identity"] = exc.identity.as_dict()
                summary = self._without_active_operation(summary)
                conflicted = replace(
                    matching,
                    phase="target_policy_conflict",
                    updated_at=_now(),
                    request={"source_path": original_source},
                    plan={},
                    summary=summary,
                    error=redact_error(exc),
                    execution=None,
                )
                atomic_write_json(self._job_path(job_id), conflicted.as_dict(), allow_nan=False)
                return conflicted
            planning = replace(matching, phase="planning", updated_at=_now())
            atomic_write_json(self._job_path(job_id), planning.as_dict(), allow_nan=False)
            try:
                _pause_checkpoint(effective_pause)
            except EnginePauseRequested:
                return self._read(job_id)
            cancelled = self._consume_cancel_request(planning)
            if cancelled is not None:
                return cancelled
            try:
                plan = self._build_plan(request)
                if merge_existing and merge_identity is not None and merge_work_root is not None:
                    self._require_merge_existing_plan_target(
                        plan,
                        identity=merge_identity,
                        work_root=merge_work_root,
                        stage="既有作品合并计划",
                    )
            except FormalTargetConflictError as exc:
                summary = dict(planning.summary)
                summary.update({
                    "identity": identity.as_dict(),
                    "automatic": True,
                    "automatic_stage": "failed_planning",
                    "target_shelf": selected_shelf.value,
                    "selected_target_root": selected_root,
                    "target_work_path": None,
                    "automatic_terminal": True,
                    "next_retry_seconds": None,
                })
                if isinstance(correction, Mapping):
                    summary["manual_identity"] = dict(correction)
                if request.source_path != original_source:
                    summary["ingress_source_path"] = original_source
                if archive_projection is not None:
                    summary["archive_preprocessed"] = dict(archive_projection)
                summary = self._without_active_operation(summary)
                failed = replace(
                    planning,
                    phase="failed_planning",
                    updated_at=_now(),
                    request=asdict(request),
                    plan={},
                    summary=summary,
                    error=redact_error(exc),
                    execution=None,
                )
                atomic_write_json(
                    self._job_path(job_id), failed.as_dict(), allow_nan=False,
                )
                return failed
            cancelled = self._consume_cancel_request(planning)
            if cancelled is not None:
                return cancelled
            _pause_checkpoint(effective_pause)
            self._require_plan_target_shelf_containment(
                plan,
                target_root=selected_root,
                stage="自动计划生成",
            )
            if merge_existing and merge_identity is not None and merge_work_root is not None:
                self._require_merge_existing_plan_target(
                    plan,
                    identity=merge_identity,
                    work_root=merge_work_root,
                    stage="自动计划生成",
                )
            engine = __import__("engine.scraper", fromlist=["plan_to_dict"])
            serializer = getattr(engine, "plan_to_dict", None)
            if not callable(serializer):
                raise SimpleEngineError("Engine 缺少 plan_to_dict")
            body = serializer(plan)
            if not isinstance(body, Mapping):
                raise SimpleEngineError("Engine 计划序列化结果无效")
            # Preserve the reconciliation evidence through the hand-off.  It
            # is the durable explanation for why this root may write into an
            # existing work and must remain available on restart/UI reads.
            preserve_reconciliation = merge_existing or reconciled_new_work
            summary = (
                {**dict(planning.summary), **self._summary(plan)}
                if preserve_reconciliation
                else self._summary(plan)
            )
            summary.update({
                "identity": identity.as_dict(),
                "automatic": True,
                "automatic_stage": "formal_write",
                "target_shelf": selected_shelf.value,
                "selected_target_root": selected_root,
                # Keep the historical summary/plan ``target_root`` as the
                # concrete work path; audit and Provider rely on that scope.
                "target_work_path": summary.get("target_root"),
                "resource_gaps": list(
                    (body.get("scan_report") or {}).get("resource_gaps") or []
                ) if isinstance(body.get("scan_report"), Mapping) else [],
                "automatic_attempts": int(job.summary.get("automatic_attempts") or 0),
                "next_retry_seconds": None,
            })
            if isinstance(correction, Mapping):
                summary["manual_identity"] = dict(correction)
            if request.source_path != original_source:
                summary["ingress_source_path"] = original_source
            if archive_projection is not None:
                summary["archive_preprocessed"] = dict(archive_projection)
            summary = self._without_active_operation(summary)
            planned = replace(
                planning,
                phase="planned",
                updated_at=_now(),
                request=asdict(request),
                plan=dict(body),
                summary=summary,
                error=None,
            )
            atomic_write_json(self._job_path(job_id), planned.as_dict(), allow_nan=False)
            return planned

    def _invoke_executor(
        self,
        plan: object,
        *,
        defer_cleanup: bool = False,
        cancel_requested: Callable[[], bool] | None = None,
        pause_requested: Callable[[], bool] | None = None,
    ) -> Mapping[str, object]:
        # Keep the gate here too. ``repair_automatic_artifacts`` and injected
        # executors both use this path, so neither can bypass the runner's
        # formal-write boundary by calling a different execution entrypoint.
        _require_problem_free_plan(plan, stage="执行器")
        _require_cleanup_allowlist(plan, stage="执行器")
        executor = self.executor
        method = getattr(executor, "execute", None)
        target = method if callable(method) else executor if callable(executor) else None
        if target is None:
            raise SimpleEngineError("无可调用的 Engine executor")
        token = _DEFER_TASK_CLEANUP.set(bool(defer_cleanup))
        effective_pause = pause_requested if pause_requested is not None else self._pause_requested
        pause_token = _PAUSE_REQUEST_CHECK.set(effective_pause)
        cancel_token = _CANCEL_REQUEST_CHECK.set(cancel_requested)
        try:
            result = target(plan)
        finally:
            _CANCEL_REQUEST_CHECK.reset(cancel_token)
            _PAUSE_REQUEST_CHECK.reset(pause_token)
            _DEFER_TASK_CLEANUP.reset(token)
        if not isinstance(result, Mapping):
            return {"result": _jsonable(result)}
        return dict(result)

    @staticmethod
    def _with_verified_automatic_formal_write(
        summary: Mapping[str, object],
        *,
        reset_cleanup: bool,
    ) -> dict[str, object]:
        """Persist the formal-write fact without erasing cleanup recovery.

        The ordinary root has two durable boundaries: formal media/artifact
        write, then the later source/staging cleanup.  A restart readback or
        an artifact repair may prove the first boundary again while the second
        is pending or has already failed.  Keep the latter state (and its
        per-step evidence) intact unless a fresh formal execution explicitly
        replaces it.
        """
        updated = dict(summary)
        lifecycle_raw = updated.get("lifecycle")
        lifecycle = dict(lifecycle_raw) if isinstance(lifecycle_raw, Mapping) else {}
        now = _now()

        formal_raw = lifecycle.get("formal_write")
        formal = dict(formal_raw) if isinstance(formal_raw, Mapping) else {}
        formal.update({"status": "verified", "updated_at": now})
        lifecycle["formal_write"] = formal

        cleanup_raw = lifecycle.get("cleanup")
        cleanup = dict(cleanup_raw) if isinstance(cleanup_raw, Mapping) else {}
        known_cleanup_states = {"pending", "running", "failed", "completed"}
        if reset_cleanup or cleanup.get("status") not in known_cleanup_states:
            lifecycle["cleanup"] = {
                "status": "pending",
                "updated_at": now,
            }
        else:
            # Preserve failed/running/completed state and any durable step
            # facts.  This is what lets a restarted cleanup retry without a
            # second writer invocation.
            lifecycle["cleanup"] = cleanup

        # Do not let a stale prior audit/provider decision authorize cleanup
        # after a newly executed formal write.  Recovery instead preserves an
        # existing true decision because it only read back the same write.
        if reset_cleanup:
            lifecycle["cleanup_ready"] = False
        elif type(lifecycle.get("cleanup_ready")) is not bool:
            lifecycle["cleanup_ready"] = False

        updated["lifecycle"] = lifecycle
        return updated

    def execute_job(
        self,
        job_id: str,
        *,
        pause_requested: Callable[[], bool] | None = None,
    ) -> EngineJob:
        with self.worker_lock():
            job = self._read(job_id)
            effective_pause = pause_requested if pause_requested is not None else self._pause_requested
            cancelled = self._consume_cancel_request(job)
            if cancelled is not None:
                return cancelled
            _pause_checkpoint(effective_pause)
            if job.phase == "executed":
                return job
            if job.summary.get("audit_owned") is True:
                raise SimpleEngineError(
                    "审计创建的根任务只能由补源 child 执行，不能执行正式媒体计划"
                )
            if job.phase not in {"planned", "retry_wait", "failed"}:
                raise SimpleEngineError(f"Engine job {job_id} 当前不能执行: {job.phase}")
            engine = __import__("engine.scraper", fromlist=["plan_from_dict"])
            parser = getattr(engine, "plan_from_dict", None)
            if not callable(parser):
                raise SimpleEngineError("Engine 缺少 plan_from_dict")
            plan = parser(job.plan)
            self._require_persisted_target_shelf_containment(
                job,
                plan,
                stage="计划执行",
            )
            defer_cleanup = (
                job.summary.get("automatic") is True
                and job.summary.get("internal_child") is not True
            )
            try:
                # Do this before persisting ``executing``. A problem-bearing
                # plan must never look like it began a formal write, even for
                # an injected executor that would otherwise accept it.
                _require_problem_free_plan(plan, stage="计划执行")
                _require_cleanup_allowlist(plan, stage="计划执行")
            except EngineExecutionError as exc:
                blocked = replace(
                    job,
                    phase="failed",
                    updated_at=_now(),
                    error=redact_error(exc),
                )
                atomic_write_json(self._job_path(job_id), blocked.as_dict(), allow_nan=False)
                raise
            if _is_provider_media_only_plan(plan):
                try:
                    _require_provider_tv_child_primary_videos(plan, stage="child 执行")
                except ValueError as exc:
                    raise EngineExecutionError(str(exc)) from exc
            executing = replace(
                job,
                phase="executing",
                updated_at=_now(),
                summary=self._with_active_operation(
                    job.summary,
                    kind="formal_write",
                ),
                error=None,
            )
            atomic_write_json(self._job_path(job_id), executing.as_dict(), allow_nan=False)
            try:
                result = self._invoke_executor(
                    plan,
                    defer_cleanup=defer_cleanup,
                    cancel_requested=lambda: self._cancel_requested(executing),
                    pause_requested=effective_pause,
                )
                result = dict(result)
                if defer_cleanup:
                    result.setdefault("cleanup_deferred", True)
                    result.setdefault(
                        "cleanup_pending",
                        [
                            str(getattr(item, "source_path"))
                            for item in list(getattr(plan, "cleanup_files", ()) or ())
                        ],
                    )
                cancelled = self._consume_cancel_request(executing)
                if cancelled is not None:
                    return cancelled
            except EnginePauseRequested:
                # Preserve ``executing`` and its active operation. Restart
                # recovery/readback can safely decide whether the previous
                # unit completed; resume must never replay blindly.
                return self._read(job_id)
            except EngineCancellationRequested:
                cancelled = self._consume_cancel_request(executing)
                return cancelled or self._cancelled_job(
                    executing,
                    reason="cancelled by operator",
                )
            except Exception as exc:
                cancelled = self._consume_cancel_request(executing)
                if cancelled is not None:
                    return cancelled
                failed = replace(
                    executing,
                    phase="failed",
                    updated_at=_now(),
                    summary=self._without_active_operation(executing.summary),
                    error=redact_error(exc),
                )
                atomic_write_json(self._job_path(job_id), failed.as_dict(), allow_nan=False)
                raise
            _pause_checkpoint(effective_pause)
            summary = self._without_active_operation(executing.summary)
            if defer_cleanup:
                summary = self._with_verified_automatic_formal_write(
                    summary,
                    reset_cleanup=True,
                )
            done = replace(
                executing,
                phase="executed",
                updated_at=_now(),
                summary=summary,
                execution=result,
                error=None,
            )
            atomic_write_json(self._job_path(job_id), done.as_dict(), allow_nan=False)
            self._clear_cancel_request(job_id)
            return done

    def _consume_archive_source(self, job: EngineJob) -> Mapping[str, object] | None:
        """Move a successfully processed automatic source out of intake.

        Failures/cancellation never call this method.  Archive input and the
        now-empty ordinary source directory both move to a task-owned
        processed area outside the formal library, so cleanup followed by an
        intake scan cannot recreate the same successful job.
        """
        projection = job.summary.get("archive_preprocessed")
        if job.summary.get("automatic") is not True and not isinstance(projection, Mapping):
            return None
        source = job.summary.get("ingress_source_path") or job.request.get("source_path")
        if not isinstance(source, str):
            return None
        source = _safe_remote_path(source, field="archive ingress source", allow_root=False)
        parent, name = posixpath.split(source.rstrip("/"))
        if not parent or not name:
            raise EngineExecutionError("归档原始来源路径无效，无法隔离")
        # Every automatic ingress owns the same task-scoped processed lane;
        # ordinary videos and extracted archives must not split ownership
        # between ``入站`` and ``归档``.
        lane = "归档"
        processed_root = _safe_remote_path(
            f"{self.library_root}/ScrapeFlow/{lane}/{_safe_job_id(job.id)}/processed",
            field="archive processed root",
            allow_root=False,
        )
        exact = getattr(self.alist, "exact_file_info", None)
        listing = getattr(self.alist, "list", None)
        exists = False
        if callable(exact):
            try:
                exists = exact(source) is not None
            except Exception:
                exists = False
        if not exists and callable(listing):
            try:
                exists = bool(listing(source, refresh=True))
            except TypeError:
                exists = bool(listing(source))
            except Exception:
                exists = False
        if not exists and callable(listing):
            try:
                parent_rows = listing(parent, refresh=True)
            except TypeError:
                parent_rows = listing(parent)
            except Exception:
                parent_rows = []
            # A parent listing can contain a regular file with the same name
            # as an intake directory (or stale metadata from a previous
            # move).  The lifecycle boundary owns a directory, so only an
            # explicit directory row is admissible here.  Failing closed is
            # important: moving a same-named file would consume an object
            # outside the persisted ingress ownership proof.
            exists = isinstance(parent_rows, list) and any(
                isinstance(row, Mapping)
                and row.get("name") == name
                and row.get("is_dir") is True
                for row in parent_rows
            )
        if not exists:
            return {"status": "already_consumed", "source": source}
        ensure = getattr(self.alist, "ensure_directory", None) or getattr(self.alist, "mkdir", None)
        if callable(ensure):
            ensure(processed_root)
        move = getattr(self.alist, "move", None)
        if not callable(move):
            raise EngineExecutionError("AList 客户端缺少 move 接口，无法隔离原始归档")
        move(parent, processed_root, [name])
        return {
            "status": "moved_to_processed",
            "source": source,
            "target": f"{processed_root}/{name}",
        }

    def _remote_directory_exists(self, path: str) -> bool:
        """Prove one exact remote path is a directory through its parent."""
        return self._remote_entry_kind(path) == "directory"

    def _remote_entry_kind(self, path: str) -> str:
        """Return the exact parent-listing fact for one remote object.

        AList can return an empty listing for both a missing path and an empty
        directory.  The parent/name listing is therefore the only admissible
        existence proof here.  ``ambiguous`` is deliberately distinct from a
        regular file so a same-named stale object can never be overwritten by
        a duplicate-consume move.
        """
        normalized = _safe_remote_path(path, field="remote directory", allow_root=False)
        parent, name = posixpath.split(normalized)
        listing = getattr(self.alist, "list", None)
        if not parent or not name or not callable(listing):
            return "missing"
        try:
            rows = listing(parent, refresh=True)
        except TypeError:
            try:
                rows = listing(parent)
            except Exception:
                return "unknown"
        except Exception:
            return "unknown"
        if not isinstance(rows, list):
            return "unknown"
        matches = [
            row for row in rows
            if isinstance(row, Mapping) and row.get("name") == name
        ]
        if len(matches) != 1:
            return "ambiguous" if matches else "missing"
        return "directory" if matches[0].get("is_dir") is True else "file"

    def _duplicate_provider_blockers(self, job_id: str) -> list[str]:
        """Return active/in-doubt Provider evidence that blocks duplicate consume."""
        safe_id = _safe_job_id(job_id)
        directory = self.state_root / "gaps" / re.sub(
            r"[^a-zA-Z0-9._-]+", "-", safe_id,
        ).strip(".-")[:96]
        if not directory.exists():
            return []
        if directory.is_symlink() or not directory.is_dir():
            return ["gap_state_directory_invalid"]
        try:
            paths = sorted(directory.glob("*.json"))
        except OSError:
            return ["gap_state_directory_unreadable"]
        blockers: list[str] = []
        for path in paths:
            if path.is_symlink() or not path.is_file():
                blockers.append(path.name)
                continue
            try:
                raw = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, UnicodeDecodeError, json.JSONDecodeError):
                blockers.append(path.name)
                continue
            if not isinstance(raw, Mapping):
                blockers.append(path.name)
                continue
            phase = str(raw.get("phase") or "").casefold()
            tier_status = str(raw.get("tier_status") or "").casefold()
            scope = str(raw.get("last_error_scope") or "").casefold()
            active_attempt = raw.get("active_attempt")
            post_audit = raw.get(_POST_ACQUISITION_REAUDIT_KEY)
            post_audit_pending = post_audit is not None
            if isinstance(post_audit, Mapping):
                post_audit_pending = str(post_audit.get("status") or "").casefold() not in {
                    "cleaned", "completed", "resolved", "closed",
                }
            active_attempt_pending = isinstance(active_attempt, Mapping)
            # A terminal phase is not proof that a serialized attempt was
            # cleaned: a crash can persist the phase before releasing the
            # attempt marker.  Keep the source-consume gate conservative and
            # allow that marker only when the post-acquisition audit records
            # an explicit cleanup fact.
            if active_attempt_pending and isinstance(post_audit, Mapping):
                active_attempt_pending = (
                    str(post_audit.get("status") or "").casefold() != "cleaned"
                )
            if (
                phase in _DUPLICATE_ACTIVE_PROVIDER_STATUSES
                or tier_status in _DUPLICATE_ACTIVE_PROVIDER_STATUSES
                or scope in _DUPLICATE_IN_DOUBT_SCOPES
                or active_attempt_pending
                or post_audit_pending
            ):
                blockers.append(path.name)
        return blockers

    def _existing_gap_hold_paths(
        self,
        job: EngineJob,
    ) -> tuple[str, str, str, str, str]:
        """Build the only source/hold paths admitted for an ``existing_gap``.

        Reconciliation stores the original intake path as task ownership.  A
        later hand-off may consume *only* a direct child of ``待刮削`` and may
        target the task's own archive lane.  Rebuilding both paths here keeps a
        hand-edited ingress path from widening the move scope.
        """
        try:
            source = self._job_ingress_source(job)
            # Every persisted source coordinate is an ownership proof.  A
            # hand-edited ``ingress_source_path`` that merely points at a
            # different direct child of intake must not be accepted as this
            # task's source.
            for field, raw_value in (
                ("request.source_path", job.request.get("source_path")),
                ("summary.source_root", job.summary.get("source_root")),
            ):
                if raw_value is None:
                    continue
                persisted = _safe_remote_path(
                    raw_value,
                    field=f"existing_gap {field}",
                    allow_root=False,
                )
                if persisted != source:
                    raise EngineJobConflictError(
                        "existing_gap 来源所有权记录不一致；拒绝执行来源交接"
                    )
            parent, name = posixpath.split(source.rstrip("/"))
            expected_parent = f"{self.library_root.rstrip('/')}/待刮削"
            if parent != expected_parent or not name or name in {".", ".."}:
                raise EngineJobConflictError(
                    "existing_gap ingress source 不属于任务待刮削直接子目录"
                )
            hold_root = _safe_remote_path(
                f"{self.library_root}/ScrapeFlow/归档/{_safe_job_id(job.id)}/existing-gap-hold",
                field="existing_gap hold root",
                allow_root=False,
            )
            target = _safe_remote_path(
                f"{hold_root}/{name}",
                field="existing_gap hold target",
                allow_root=False,
            )
        except EngineJobConflictError:
            raise
        except (EngineRequestError, ValueError) as exc:
            raise EngineJobConflictError(
                "existing_gap ingress/hold 路径无效；拒绝执行来源交接"
            ) from exc
        return source, parent, name, hold_root, target

    @staticmethod
    def _existing_gap_marker(summary: Mapping[str, object]) -> Mapping[str, object] | None:
        marker = summary.get("existing_gap_registration")
        return dict(marker) if isinstance(marker, Mapping) else None

    def _block_existing_gap_registration(
        self,
        job: EngineJob,
        *,
        source: str,
        target: str,
        status: str,
        reason: str,
        observed_count: int | None = None,
    ) -> EngineJob:
        """Persist a visible, source-retaining registration block.

        This path deliberately does not move, delete, or otherwise mutate the
        intake object.  ``reconciliation_uncertain`` keeps the global L gate
        closed while exposing a stable retry/needs-attention projection.
        """
        marker: dict[str, object] = {
            "status": status,
            "source": source,
            "target": target,
            "reason": redact_error(reason),
            "updated_at": _now(),
        }
        if observed_count is not None:
            marker["observed_count"] = observed_count
        summary = dict(job.summary)
        reconciliation_raw = summary.get("reconciliation")
        if isinstance(reconciliation_raw, Mapping):
            reconciliation = dict(reconciliation_raw)
            reconciliation["status"] = "needs_attention"
            reconciliation["registration_status"] = "blocked"
            summary["reconciliation"] = reconciliation
        summary["existing_gap_registration"] = marker
        summary["source_fate"] = "retained_needs_attention"
        summary["automatic_stage"] = "existing_gap_registration_blocked"
        summary["automatic_terminal"] = True
        summary["next_retry_seconds"] = None
        updated = replace(
            job,
            phase="reconciliation_uncertain",
            summary=summary,
            updated_at=_now(),
            error=redact_error(reason),
        )
        atomic_write_json(self._job_path(job.id), updated.as_dict(), allow_nan=False)
        return self._read(job.id)

    def _existing_gap_registration_blockers(self, job: EngineJob) -> list[str]:
        """Return any Provider/child state that makes an E hand-off unsafe."""
        blockers = self._duplicate_provider_blockers(job.id)
        replenishment = job.summary.get("replenishment")
        if isinstance(replenishment, Mapping):
            status = str(replenishment.get("status") or "").casefold()
            scope = str(
                replenishment.get("failure_scope")
                or replenishment.get("last_error_scope")
                or ""
            ).casefold()
            if status in _DUPLICATE_ACTIVE_PROVIDER_STATUSES or (
                status
                and replenishment.get("terminal") is not True
                and status not in _DUPLICATE_TERMINAL_PROVIDER_STATUSES
            ):
                blockers.append("root_provider_active")
            if scope in _DUPLICATE_IN_DOUBT_SCOPES:
                blockers.append("root_provider_in_doubt")
        for child in self._owned_children(job):
            child_summary = child.summary if isinstance(child.summary, Mapping) else {}
            child_provider = child_summary.get("replenishment")
            child_status = ""
            child_scope = ""
            if isinstance(child_provider, Mapping):
                child_status = str(child_provider.get("status") or "").casefold()
                child_scope = str(
                    child_provider.get("failure_scope")
                    or child_provider.get("last_error_scope")
                    or ""
                ).casefold()
            if child.phase not in _CLEANUP_TERMINAL_PHASES:
                blockers.append(f"child_active:{child.id}")
            if (
                child_status in _DUPLICATE_ACTIVE_PROVIDER_STATUSES
                or child_scope in _DUPLICATE_IN_DOUBT_SCOPES
            ):
                blockers.append(f"child_provider:{child.id}")
        post_audit = job.summary.get(_POST_ACQUISITION_REAUDIT_KEY)
        if post_audit and (
            not isinstance(post_audit, Mapping)
            or str(post_audit.get("status") or "").casefold()
            not in {"cleaned", "completed", "resolved", "closed"}
        ):
            blockers.append("post_acquisition_reaudit_pending")
        return sorted(set(blockers))

    def _existing_gap_hold_verified_locked(self, job: EngineJob) -> bool:
        """Verify a durable existing-gap hold without performing mutations."""
        summary = job.summary if isinstance(job.summary, Mapping) else {}
        reconciliation = summary.get("reconciliation")
        marker = self._existing_gap_marker(summary)
        if (
            job.phase != "completed"
            or not isinstance(reconciliation, Mapping)
            or reconciliation.get("outcome") != "existing_gap"
            or reconciliation.get("status") != "completed"
            or not isinstance(marker, Mapping)
            or marker.get("status") not in {"moved_to_hold", "already_held"}
            or summary.get("source_fate") not in {"moved_to_hold", "already_held"}
        ):
            return False
        if marker.get("evidence") != "reconciliation.existing_gap.empty_ingress":
            return False
        completed_at = marker.get("completed_at")
        if not isinstance(completed_at, str) or not completed_at.strip():
            return False
        try:
            datetime.fromisoformat(completed_at.replace("Z", "+00:00"))
        except ValueError:
            return False
        try:
            source, _parent, _name, _hold_root, expected_target = self._existing_gap_hold_paths(job)
            self._reconciled_existing_context(summary, expected_outcome="existing_gap")
        except (EngineRequestError, EngineJobConflictError, ValueError):
            return False
        if (
            marker.get("source") == source
            and marker.get("target") == expected_target
            and self._remote_entry_kind(source) == "missing"
            and self._remote_entry_kind(expected_target) == "directory"
        ) is not True:
            return False
        listing = getattr(self.alist, "list", None)
        if not callable(listing):
            return False
        try:
            try:
                rows = listing(expected_target, refresh=True)
            except TypeError:
                rows = listing(expected_target)
        except Exception:
            return False
        return isinstance(rows, list) and not rows

    def existing_gap_source_hold_verified(self, job_id: str) -> bool:
        """Read-only proof used by the global intake-empty barrier."""
        with self.worker_lock():
            try:
                return self._existing_gap_hold_verified_locked(self._read(job_id))
            except (EngineJobNotFoundError, SimpleEngineError):
                return False

    @staticmethod
    def _existing_gap_hold_intent_matches(
        marker: Mapping[str, object] | None,
        *,
        source: str,
        target: str,
    ) -> bool:
        """Return whether a durable pre-move receipt authorizes recovery.

        The receipt is intentionally tiny: it records only that this exact
        task proved its direct-child ingress empty immediately before its one
        AList move.  It is *not* a completion fact; the target must still be
        read back as an empty directory before a restarted process can turn
        it into a completed hold.
        """
        if not isinstance(marker, Mapping):
            return False
        if (
            marker.get("status") != "hold_prepared"
            or marker.get("source") != source
            or marker.get("target") != target
            or marker.get("evidence") != "reconciliation.existing_gap.empty_ingress"
        ):
            return False
        prepared_at = marker.get("prepared_at")
        if not isinstance(prepared_at, str) or not prepared_at.strip():
            return False
        try:
            datetime.fromisoformat(prepared_at.replace("Z", "+00:00"))
        except ValueError:
            return False
        return True

    def _complete_existing_gap_hold(
        self,
        job: EngineJob,
        *,
        source: str,
        target: str,
        cancellation_observed_after_commit: bool = False,
    ) -> EngineJob:
        """Persist the local completion half after an exact empty hold readback."""
        completed_marker: dict[str, object] = {
            "status": "moved_to_hold",
            "source": source,
            "target": target,
            "completed_at": _now(),
            "evidence": "reconciliation.existing_gap.empty_ingress",
        }
        if cancellation_observed_after_commit:
            completed_marker["cancellation_observed_after_commit"] = True
        updated_summary = dict(job.summary)
        updated_summary["existing_gap_registration"] = completed_marker
        updated_summary["source_fate"] = "moved_to_hold"
        updated_summary["automatic_stage"] = "existing_gap_registered"
        updated_summary["automatic_terminal"] = True
        updated_summary["next_retry_seconds"] = None
        rec = updated_summary.get("reconciliation")
        if isinstance(rec, Mapping):
            rec_copy = dict(rec)
            rec_copy["status"] = "completed"
            rec_copy["registration_status"] = "held"
            updated_summary["reconciliation"] = rec_copy
        updated = replace(
            job,
            phase="completed",
            summary=updated_summary,
            updated_at=_now(),
            error=None,
        )
        atomic_write_json(self._job_path(job.id), updated.as_dict(), allow_nan=False)
        return self._read(job.id)

    def hold_existing_gap_source(
        self,
        job_id: str,
        *,
        pause_requested: Callable[[], bool] | None = None,
    ) -> EngineJob:
        """Register an ``existing_gap`` source only when it is proven empty.

        Existing-gap reconciliation is intentionally report-only before the
        global L audit.  The sole E-side hand-off is moving an *empty*,
        task-owned intake directory into that task's archive hold lane. Any
        member (video, subtitle, archive, or unknown row) remains in intake
        and becomes visible ``needs_attention``; no Provider or formal writer
        is started here.
        """
        with self.worker_lock():
            job = self._read(job_id)
            effective_pause = (
                pause_requested if callable(pause_requested) else self._pause_requested
            )
            summary = job.summary if isinstance(job.summary, Mapping) else {}
            reconciliation = summary.get("reconciliation")
            marker = self._existing_gap_marker(summary)

            if job.phase == "completed":
                if self._existing_gap_hold_verified_locked(job):
                    return job
                raise EngineExecutionError("existing_gap hold 目标回读失败；拒绝重复交接")
            if job.phase not in {"reconciled", "reconciliation_uncertain", "failed_cleanup"}:
                raise EngineJobConflictError(
                    f"任务当前不能登记 existing_gap 来源: {job.phase}"
                )
            if not isinstance(reconciliation, Mapping) or reconciliation.get("outcome") != "existing_gap":
                raise EngineJobConflictError("只有 existing_gap 才能登记空来源 hold")
            if reconciliation.get("status") not in {"completed", "needs_attention"}:
                raise EngineJobConflictError("existing_gap 对账证据尚未完成")
            reason = reconciliation.get("reason")
            if not isinstance(reason, str) or not reason.strip():
                raise EngineJobConflictError("existing_gap 缺少完整对账理由")
            if job.phase == "reconciliation_uncertain" and not (
                isinstance(marker, Mapping)
                and marker.get("status") in {
                    "blocked_nonempty_source",
                    "blocked_source_not_directory",
                    "failed",
                    "hold_prepared",
                }
            ):
                raise EngineJobConflictError("needs_attention 任务缺少 existing_gap 登记阻断证据")
            if job.phase == "failed_cleanup" and not (
                isinstance(marker, Mapping)
                and marker.get("status") in {"failed", "hold_prepared"}
            ):
                raise EngineJobConflictError("existing_gap 清理重试证据缺失")

            # Revalidate identity, shelf, and exact matched work root before
            # touching the task-owned source.
            self._reconciled_existing_context(summary, expected_outcome="existing_gap")
            source, parent, name, hold_root, target = self._existing_gap_hold_paths(job)
            blockers = self._existing_gap_registration_blockers(job)
            if blockers:
                raise EngineWorkerBusyError(
                    "existing_gap 仍有活动或待核对状态: " + ",".join(blockers)
                )
            cancelled = self._consume_cancel_request(job)
            if cancelled is not None:
                return cancelled

            source_kind = self._remote_entry_kind(source)
            target_kind = self._remote_entry_kind(target)
            if source_kind == "unknown" or target_kind == "unknown":
                raise EngineExecutionError("existing_gap source/hold 回读不可确认")
            if source_kind in {"file", "ambiguous"}:
                return self._block_existing_gap_registration(
                    job,
                    source=source,
                    target=target,
                    status="blocked_source_not_directory",
                    reason="existing_gap 来源不是唯一目录；已保留入站对象",
                )
            if target_kind in {"file", "ambiguous"}:
                raise EngineJobConflictError("existing_gap hold 目标已被占用")
            if source_kind == "missing" and target_kind == "directory":
                # A completed move may crash between the remote AList commit
                # and the local completion receipt.  Recover only when this
                # very task persisted its pre-move empty-ingress intent *and*
                # the derived target is still exactly empty.  A bare matching
                # directory remains ambiguous and must not turn the global
                # barrier green.
                if self._existing_gap_hold_intent_matches(
                    marker,
                    source=source,
                    target=target,
                ):
                    listing = getattr(self.alist, "list", None)
                    if callable(listing):
                        try:
                            try:
                                target_rows = listing(target, refresh=True)
                            except TypeError:
                                target_rows = listing(target)
                        except Exception:
                            target_rows = None
                        if isinstance(target_rows, list) and not target_rows:
                            committed_job = self._read(job_id)
                            cancellation_observed_after_commit = False
                            cancel_request = self._read_cancel_request(job_id)
                            if (
                                isinstance(cancel_request, Mapping)
                                and self._cancel_request_matches(committed_job, cancel_request)
                            ):
                                self._clear_cancel_request(job_id)
                                cancellation_observed_after_commit = True
                            return self._complete_existing_gap_hold(
                                committed_job,
                                source=source,
                                target=target,
                                cancellation_observed_after_commit=cancellation_observed_after_commit,
                            )
                return self._block_existing_gap_registration(
                    job,
                    source=source,
                    target=target,
                    status="failed",
                    reason="existing_gap 来源已缺失但 hold 没有可验证空目录登记；已保留目标等待人工确认",
                )
            if source_kind == "missing":
                raise EngineExecutionError("existing_gap source 与 hold 目标均无法回读")
            if target_kind != "missing":
                raise EngineJobConflictError("existing_gap source/hold 目标状态冲突")

            listing = getattr(self.alist, "list", None)
            if not callable(listing):
                raise EngineExecutionError("AList 客户端缺少 existing_gap 来源读取接口")

            def source_rows() -> list[object]:
                try:
                    rows = listing(source, refresh=True)
                except TypeError:
                    rows = listing(source)
                except Exception as exc:
                    raise EngineExecutionError("existing_gap 来源内容无法确认") from exc
                if not isinstance(rows, list):
                    raise EngineExecutionError("existing_gap 来源目录返回格式不可确认")
                return rows

            rows = source_rows()
            if rows:
                return self._block_existing_gap_registration(
                    job,
                    source=source,
                    target=target,
                    status="blocked_nonempty_source",
                    reason="existing_gap 来源目录非空；已保留入站对象，等待人工处理",
                    observed_count=len(rows),
                )

            ensure = getattr(self.alist, "ensure_directory", None) or getattr(self.alist, "mkdir", None)
            move = getattr(self.alist, "move", None)
            if not callable(ensure) or not callable(move):
                raise EngineExecutionError("AList 客户端缺少 existing_gap hold 接口")
            hold_kind = self._remote_entry_kind(hold_root)
            if hold_kind in {"file", "ambiguous", "unknown"}:
                raise EngineExecutionError("existing_gap hold 根不是可用目录")
            _pause_checkpoint(effective_pause)
            if self._consume_cancel_request(self._read(job_id)) is not None:
                return self._read(job_id)
            if hold_kind == "missing":
                ensure(hold_root)
                if self._remote_entry_kind(hold_root) != "directory":
                    raise EngineExecutionError("existing_gap hold 根创建后回读失败")
            if self._remote_entry_kind(target) != "missing":
                raise EngineJobConflictError("existing_gap hold 目标已被占用")
            _pause_checkpoint(effective_pause)
            latest = self._read(job_id)
            if self._consume_cancel_request(latest) is not None:
                return self._read(job_id)
            # Recheck emptiness immediately before the only remote move.
            if source_rows():
                return self._block_existing_gap_registration(
                    latest,
                    source=source,
                    target=target,
                    status="blocked_nonempty_source",
                    reason="existing_gap 来源在交接前变为非空；已保留入站对象",
                )
            # Persist a narrow intent before the only remote mutation.  It
            # makes the otherwise unobservable move→local-write crash
            # recoverable without accepting a hand-created hold directory as
            # evidence.  The barrier still rejects this non-terminal marker.
            prepared_summary = dict(latest.summary)
            prepared_summary["existing_gap_registration"] = {
                "status": "hold_prepared",
                "source": source,
                "target": target,
                "prepared_at": _now(),
                "evidence": "reconciliation.existing_gap.empty_ingress",
            }
            prepared_summary["source_fate"] = "hold_prepared"
            prepared_summary["automatic_stage"] = "existing_gap_hold_prepared"
            prepared = replace(
                latest,
                summary=prepared_summary,
                updated_at=_now(),
                error=None,
            )
            atomic_write_json(self._job_path(job.id), prepared.as_dict(), allow_nan=False)
            latest = self._read(job_id)
            move(parent, hold_root, [name])
            target_rows: list[object] | None = None
            listing_after_move = getattr(self.alist, "list", None)
            if callable(listing_after_move):
                try:
                    try:
                        target_rows = listing_after_move(target, refresh=True)
                    except TypeError:
                        target_rows = listing_after_move(target)
                except Exception:
                    target_rows = None
            if (
                self._remote_entry_kind(source) != "missing"
                or self._remote_entry_kind(target) != "directory"
                or not isinstance(target_rows, list)
                or target_rows
            ):
                raise EngineExecutionError("existing_gap 来源交接后回读失败")
            committed_job = self._read(job_id)
            cancellation_observed_after_commit = False
            # The move is the commit point.  An inactive cancellation request
            # can arrive after the last pre-move checkpoint, so match and
            # consume it explicitly rather than converting a committed hold
            # back into ``cancelled`` (which would leave the source absent but
            # falsely block the next barrier forever).
            cancel_request = self._read_cancel_request(job_id)
            if (
                isinstance(cancel_request, Mapping)
                and self._cancel_request_matches(committed_job, cancel_request)
            ):
                self._clear_cancel_request(job_id)
                cancellation_observed_after_commit = True

            return self._complete_existing_gap_hold(
                committed_job,
                source=source,
                target=target,
                cancellation_observed_after_commit=cancellation_observed_after_commit,
            )

    def consume_duplicate_complete_source(
        self,
        job_id: str,
        *,
        pause_requested: Callable[[], bool] | None = None,
    ) -> EngineJob:
        """Idempotently consume a proven duplicate-complete ingress directory.

        This is intentionally separate from ``finalize_automatic_lifecycle``:
        duplicate-complete has no formal write fact or cleanup transaction.  A
        source is moved only after the read-only reconciliation proof and all
        Provider/child state are rechecked under the existing worker lock.
        Every remote mutation is followed by exact parent-listing readback;
        an ambiguous result raises and leaves the job non-terminal.
        """
        with self.worker_lock():
            job = self._read(job_id)
            effective_pause = (
                pause_requested if callable(pause_requested) else self._pause_requested
            )
            summary = job.summary if isinstance(job.summary, Mapping) else {}
            reconciliation = summary.get("reconciliation")
            marker = summary.get("duplicate_complete_consumption")
            if not isinstance(marker, Mapping):
                # Older WIP records used this name.  It is accepted only as
                # an equivalent marker; paths are still rebuilt and checked
                # against the current job below.
                marker = summary.get("duplicate_cleanup")
            marker = marker if isinstance(marker, Mapping) else None
            expected_source: str | None = None
            expected_target: str | None = None
            try:
                expected_source = self._job_ingress_source(job)
                source_parent, source_name = posixpath.split(expected_source)
                if (
                    source_parent == f"{self.library_root.rstrip('/')}/待刮削"
                    and source_name not in {"", ".", ".."}
                ):
                    expected_target = _safe_remote_path(
                        f"{self.library_root}/ScrapeFlow/归档/{_safe_job_id(job.id)}/processed/{source_name}",
                        field="duplicate processed target",
                        allow_root=False,
                    )
            except (EngineRequestError, ValueError):
                expected_source = expected_target = None
            if (
                isinstance(reconciliation, Mapping)
                and reconciliation.get("outcome") == "duplicate_complete"
                and isinstance(marker, Mapping)
                and job.phase == "completed"
            ):
                # Repeated scheduler calls are reads, but retain the exact
                # processed target proof; a missing target is corruption, not
                # permission to move the source again.
                target = marker.get("target")
                if (
                    expected_source is not None
                    and expected_target is not None
                    and marker.get("source") == expected_source
                    and target == expected_target
                    and isinstance(target, str)
                    and marker.get("status") in {"moved_to_processed", "already_consumed"}
                    and self._remote_entry_kind(target) == "directory"
                ):
                    return job
                raise EngineExecutionError("duplicate_complete processed 目标回读失败")
            if job.phase not in {"reconciled", "failed_cleanup"}:
                raise EngineJobConflictError(
                    f"任务当前不能消费 duplicate_complete 来源: {job.phase}"
                )
            if not isinstance(reconciliation, Mapping):
                raise EngineJobConflictError("duplicate_complete 对账证据缺失")
            if reconciliation.get("outcome") != "duplicate_complete":
                raise EngineJobConflictError("只有 duplicate_complete 才能消费来源")
            if reconciliation.get("status") != "completed":
                raise EngineJobConflictError("duplicate_complete 对账证据尚未完成")
            reason = reconciliation.get("reason")
            if not isinstance(reason, str) or not reason.strip():
                raise EngineJobConflictError("duplicate_complete 缺少完整对账理由")
            # Reuse the same identity/shelf/work-root proof used by the
            # existing-work hand-off, but require the duplicate outcome.
            self._reconciled_existing_context(
                summary,
                expected_outcome="duplicate_complete",
            )
            if job.phase == "failed_cleanup" and not (
                isinstance(marker, Mapping)
                and marker.get("status") in {"failed", "moved_to_processed", "already_consumed"}
            ):
                raise EngineJobConflictError("duplicate_complete 清理重试证据缺失")
            blockers = self._duplicate_provider_blockers(job.id)
            replenishment = summary.get("replenishment")
            if isinstance(replenishment, Mapping):
                status = str(replenishment.get("status") or "").casefold()
                scope = str(
                    replenishment.get("failure_scope")
                    or replenishment.get("last_error_scope")
                    or ""
                ).casefold()
                if status in _DUPLICATE_ACTIVE_PROVIDER_STATUSES or (
                    status
                    and replenishment.get("terminal") is not True
                    and status not in _DUPLICATE_TERMINAL_PROVIDER_STATUSES
                ):
                    blockers.append("root_provider_active")
                if scope in _DUPLICATE_IN_DOUBT_SCOPES:
                    blockers.append("root_provider_in_doubt")
            for child in self._owned_children(job):
                child_summary = child.summary if isinstance(child.summary, Mapping) else {}
                child_provider = child_summary.get("replenishment")
                child_scope = ""
                child_status = ""
                if isinstance(child_provider, Mapping):
                    child_status = str(child_provider.get("status") or "").casefold()
                    child_scope = str(
                        child_provider.get("failure_scope")
                        or child_provider.get("last_error_scope")
                        or ""
                    ).casefold()
                if child.phase not in _CLEANUP_TERMINAL_PHASES:
                    blockers.append(f"child_active:{child.id}")
                if child_status in _DUPLICATE_ACTIVE_PROVIDER_STATUSES or child_scope in _DUPLICATE_IN_DOUBT_SCOPES:
                    blockers.append(f"child_provider:{child.id}")
            if blockers:
                raise EngineWorkerBusyError(
                    "duplicate_complete 仍有活动或待核对的补源状态: "
                    + ",".join(sorted(set(blockers)))
                )

            try:
                cancellation_observed_after_commit = False
                source = self._job_ingress_source(job)
                parent, name = posixpath.split(source.rstrip("/"))
                expected_parent = f"{self.library_root.rstrip('/')}/待刮削"
                if parent != expected_parent or not name or name in {".", ".."}:
                    raise EngineExecutionError("duplicate_complete ingress source 不属于任务待刮削直接子目录")
                processed_root = _safe_remote_path(
                    f"{self.library_root}/ScrapeFlow/归档/{_safe_job_id(job.id)}/processed",
                    field="duplicate processed root",
                    allow_root=False,
                )
                target = f"{processed_root}/{name}"
                source_kind = self._remote_entry_kind(source)
                target_kind = self._remote_entry_kind(target)
                if source_kind == "unknown" or target_kind == "unknown":
                    raise EngineExecutionError("duplicate_complete source/processed 回读不可确认")
                if source_kind == "directory" and target_kind != "missing":
                    raise EngineExecutionError("duplicate_complete source 与 processed 目标冲突")
                if source_kind in {"file", "ambiguous"}:
                    raise EngineExecutionError("duplicate_complete ingress source 不是唯一目录")
                if source_kind == "missing" and target_kind == "directory":
                    consumed = {
                        "status": "already_consumed",
                        "source": source,
                        "target": target,
                    }
                elif source_kind == "missing":
                    raise EngineExecutionError("duplicate_complete source/processed 均无法回读")
                else:
                    ensure = getattr(self.alist, "ensure_directory", None) or getattr(self.alist, "mkdir", None)
                    move = getattr(self.alist, "move", None)
                    if not callable(ensure) or not callable(move):
                        raise EngineExecutionError("AList 客户端缺少 duplicate source 消费接口")
                    processed_kind = self._remote_entry_kind(processed_root)
                    if processed_kind in {"file", "ambiguous", "unknown"}:
                        raise EngineExecutionError("duplicate_complete processed 根不是可用目录")
                    _pause_checkpoint(effective_pause)
                    if self._consume_cancel_request(self._read(job_id)) is not None:
                        return self._read(job_id)
                    ensure(processed_root)
                    if self._remote_entry_kind(processed_root) != "directory":
                        raise EngineExecutionError("processed 目录创建后回读失败")
                    # A same-name object may have appeared while creating the
                    # parent. Never let AList's move semantics overwrite it.
                    if self._remote_entry_kind(target) != "missing":
                        raise EngineExecutionError("duplicate_complete processed 目标已被占用")
                    _pause_checkpoint(effective_pause)
                    latest = self._read(job_id)
                    if self._consume_cancel_request(latest) is not None:
                        return self._read(job_id)
                    move(parent, processed_root, [name])
                    if (
                        self._remote_entry_kind(source) != "missing"
                        or self._remote_entry_kind(target) != "directory"
                    ):
                        raise EngineExecutionError("duplicate_complete source 消费后回读失败")
                    # Move is the remote commit point.  If cancellation was
                    # requested during that operation, consume the stale
                    # marker and record the observation instead of exposing a
                    # cancelled job whose source has already been consumed.
                    committed_job = self._read(job_id)
                    if self._cancel_requested(committed_job):
                        self._clear_cancel_request(job_id)
                        cancellation_observed_after_commit = True
                    consumed = {
                        "status": "moved_to_processed",
                        "source": source,
                        "target": target,
                    }
            except (EnginePauseRequested, EngineCancellationRequested):
                raise
            except Exception as exc:
                # Persist a narrow retry lane, without fabricating a formal
                # write/readback fact. The source remains owned and untouched
                # whenever the remote result is ambiguous.
                failed_marker = dict(marker) if isinstance(marker, Mapping) else {}
                failed_marker.update({
                    "status": "failed",
                    "error": redact_error(exc),
                    "updated_at": _now(),
                })
                failed_summary = dict(summary)
                failed_summary["duplicate_complete_consumption"] = failed_marker
                failed_summary["duplicate_cleanup"] = failed_marker
                failed_summary["cleanup_only_retry"] = True
                failed_summary["automatic_terminal"] = True
                failed = replace(
                    job,
                    phase="failed_cleanup",
                    summary=failed_summary,
                    updated_at=_now(),
                    error=redact_error(exc),
                )
                atomic_write_json(self._job_path(job.id), failed.as_dict(), allow_nan=False)
                raise
            marker = {
                **consumed,
                "completed_at": _now(),
                "evidence": "reconciliation.duplicate_complete",
            }
            if cancellation_observed_after_commit:
                marker["cancellation_observed_after_commit"] = True
            updated_summary = dict(summary)
            updated_summary["duplicate_complete_consumption"] = marker
            updated_summary["duplicate_cleanup"] = marker
            updated_summary["source_fate"] = str(consumed["status"])
            updated_summary["automatic_stage"] = "duplicate_complete"
            updated_summary["automatic_terminal"] = True
            updated_summary["next_retry_seconds"] = None
            updated = replace(
                job,
                phase="completed",
                summary=updated_summary,
                updated_at=_now(),
                error=None,
            )
            atomic_write_json(self._job_path(job.id), updated.as_dict(), allow_nan=False)
            return self._read(job.id)

    def duplicate_complete_consumption_verified(self, job_id: str) -> bool:
        """Verify the durable duplicate-consumption marker and remote facts.

        The intake-empty barrier uses this read-only predicate so a stale or
        hand-edited ``completed`` job cannot make the global audit look ready.
        """
        with self.worker_lock():
            job = self._read(job_id)
            reconciliation = job.summary.get("reconciliation")
            marker = job.summary.get("duplicate_complete_consumption")
            if not isinstance(marker, Mapping):
                marker = job.summary.get("duplicate_cleanup")
            if (
                not isinstance(reconciliation, Mapping)
                or reconciliation.get("outcome") != "duplicate_complete"
                or job.phase != "completed"
                or not isinstance(marker, Mapping)
                or marker.get("status") not in {"moved_to_processed", "already_consumed"}
            ):
                return False
            source = marker.get("source")
            target = marker.get("target")
            if not isinstance(source, str) or not isinstance(target, str):
                return False
            try:
                self._reconciled_existing_context(
                    job.summary,
                    expected_outcome="duplicate_complete",
                )
                expected_source = self._job_ingress_source(job)
                parent, name = posixpath.split(expected_source)
                if parent != f"{self.library_root.rstrip('/')}/待刮削" or not name:
                    return False
                expected_target = _safe_remote_path(
                    f"{self.library_root}/ScrapeFlow/归档/{_safe_job_id(job.id)}/processed/{name}",
                    field="duplicate processed target",
                    allow_root=False,
                )
            except (EngineRequestError, ValueError):
                return False
            if source != expected_source or target != expected_target:
                return False
            return (
                self._remote_entry_kind(source) == "missing"
                and self._remote_entry_kind(target) == "directory"
            )

    def execute_automatic(
        self,
        job_id: str,
        *,
        pause_requested: Callable[[], bool] | None = None,
    ) -> EngineJob:
        """Execute or retry an automatic job."""
        return self.execute_job(job_id, pause_requested=pause_requested)

    def repair_automatic_artifacts(self, job_id: str) -> EngineJob:
        """Re-run only a completed plan's deterministic metadata/artwork.

        A full-library audit may discover that a poster or NFO disappeared
        after the original media move.  Replanning from the now-empty inbound
        directory would be wrong; the persisted plan is the authoritative
        target map, and ``SimplePlanExecutor`` already treats an existing
        target with an absent source as idempotent.
        """
        with self.worker_lock():
            job = self._read(job_id)
            if job.phase != "executed":
                raise SimpleEngineError(f"Engine job {job_id} 当前不能修复元数据: {job.phase}")
            if job.summary.get("audit_owned") is True:
                raise SimpleEngineError("审计创建的根任务没有可直接重放的元数据计划")
            plan = self._plan_from_job(job)
            self._require_persisted_target_shelf_containment(
                job,
                plan,
                stage="元数据修复",
            )
            result = self._invoke_executor(
                plan,
                defer_cleanup=(
                    job.summary.get("automatic") is True
                    and job.summary.get("internal_child") is not True
                ),
            )
            summary = dict(job.summary)
            summary["last_artifact_repair_at"] = _now()
            if (
                job.summary.get("automatic") is True
                and job.summary.get("internal_child") is not True
            ):
                summary = self._with_verified_automatic_formal_write(
                    summary,
                    reset_cleanup=False,
                )
            repaired = replace(
                job,
                updated_at=_now(),
                summary=summary,
                execution=result,
                error=None,
            )
            atomic_write_json(self._job_path(job_id), repaired.as_dict(), allow_nan=False)
            return repaired

    def recover_job(self, job_id: str) -> EngineJob:
        """Read back a failed/interrupted Engine plan without mutating AList.

        Restart recovery must not infer success merely because a target name is
        visible.  Every planned media file must have its expected size, the
        source must no longer be visible, and deterministic NFO/artwork
        targets must be present before the job can become completed.
        """
        with self.worker_lock():
            job = self._read(job_id)
            cancelled = self._consume_cancel_request(job)
            if cancelled is not None:
                return cancelled
            # Recovery is a readback boundary for an already-persisted plan;
            # it must never manufacture a retry record for an intake gate or
            # an identity/planning phase that has no executable plan yet.
            # In particular, a direct recovery call (or a stale timer) must
            # leave ``awaiting_target_shelf`` untouched.
            recoverable_phases = {
                "executing", "verifying", "cleaning", "retry_wait", "failed",
                "failed_write", "failed_verification", "failed_cleanup",
            }
            if job.phase not in recoverable_phases:
                return job
            if not job.plan:
                return job
            try:
                plan = self._plan_from_job(job)
                self._require_persisted_target_shelf_containment(
                    job,
                    plan,
                    stage="恢复检查",
                )
                execution = self._readback_plan(
                    plan,
                    allow_pending_cleanup=(
                        job.summary.get("automatic") is True
                        and job.summary.get("internal_child") is not True
                    ),
                )
            except EngineRecoveryMatrixError as exc:
                # A matrix conflict is a durable fact, not a transient
                # provider error.  Keep the plan and paths for operator
                # correction, but make the phase terminal so automatic retry
                # cannot replay a potentially destructive write.
                recovery: dict[str, object] = {
                    "status": "terminal",
                    "reason": exc.reason,
                }
                if exc.source is not None:
                    recovery["source"] = exc.source
                if exc.target is not None:
                    recovery["target"] = exc.target
                if exc.expected_size is not None:
                    recovery["expected_size"] = exc.expected_size
                if exc.actual_size is not None:
                    recovery["actual_size"] = exc.actual_size
                summary = dict(job.summary)
                summary["recovery"] = recovery
                summary["automatic_terminal"] = True
                failed = replace(
                    job,
                    phase="failed_verification",
                    updated_at=_now(),
                    summary=summary,
                    execution=None,
                    error=(
                        "恢复检查发现不可自动修复的状态，已停止: "
                        f"{redact_error(exc)}"
                    ),
                )
                atomic_write_json(self._job_path(job_id), failed.as_dict(), allow_nan=False)
                return failed
            except EngineRecoveryRetryableError as exc:
                recovery = {
                    "status": "retryable",
                    "reason": exc.reason,
                }
                if exc.source is not None:
                    recovery["source"] = exc.source
                if exc.target is not None:
                    recovery["target"] = exc.target
                summary = dict(job.summary)
                summary["recovery"] = recovery
                failed = replace(
                    job,
                    phase="retry_wait",
                    updated_at=_now(),
                    summary=summary,
                    error=(
                        "恢复检查确认结果尚不完整，将自动重试: "
                        f"{redact_error(exc)}"
                    ),
                )
                atomic_write_json(self._job_path(job_id), failed.as_dict(), allow_nan=False)
                return failed
            except EngineRequestError as exc:
                # A persisted-path or target-shelf policy violation is not a
                # remote visibility transient.  Retrying it would only keep
                # an invalid plan on the scheduler and risk a later bypass.
                summary = dict(job.summary)
                summary["recovery"] = {
                    "status": "terminal",
                    "reason": "target_shelf_policy_violation",
                }
                summary["automatic_terminal"] = True
                failed = replace(
                    job,
                    phase="failed_verification",
                    updated_at=_now(),
                    summary=summary,
                    execution=None,
                    error=(
                        "恢复检查发现计划路径违反目标货架策略，已停止: "
                        f"{redact_error(exc)}"
                    ),
                )
                atomic_write_json(self._job_path(job_id), failed.as_dict(), allow_nan=False)
                return failed
            except Exception as exc:
                summary = dict(job.summary)
                summary["recovery"] = {
                    "status": "unknown",
                    "reason": "readback_unavailable",
                }
                failed = replace(
                    job,
                    phase="retry_wait",
                    updated_at=_now(),
                    summary=summary,
                    error=(
                        "恢复检查暂时无法确认远端结果，将自动重试: "
                        f"{redact_error(exc)}"
                    ),
                )
                atomic_write_json(self._job_path(job_id), failed.as_dict(), allow_nan=False)
                return failed
            summary = dict(job.summary)
            if (
                job.summary.get("automatic") is True
                and job.summary.get("internal_child") is not True
            ):
                # A successful exact-path readback is the same formal-write
                # proof as a synchronous executor return.  Older automatic
                # JSON records predate this lifecycle field, while a failed
                # cleanup record may already carry step evidence; normalize
                # both without re-running the writer or downgrading a failed
                # cleanup to a fresh pending state.
                summary = self._with_verified_automatic_formal_write(
                    summary,
                    reset_cleanup=False,
                )
            recovered = replace(
                job,
                phase="executed",
                updated_at=_now(),
                summary=summary,
                execution=execution,
                error=None,
            )
            atomic_write_json(self._job_path(job_id), recovered.as_dict(), allow_nan=False)
            return recovered

    def _plan_from_job(self, job: EngineJob) -> object:
        engine = __import__("engine.scraper", fromlist=["plan_from_dict"])
        parser = getattr(engine, "plan_from_dict", None)
        if not callable(parser):
            raise SimpleEngineError("Engine 缺少 plan_from_dict")
        return parser(job.plan)

    def _exact_info(
        self,
        path: str,
        *,
        wait_for_visibility: bool = True,
        expected_size: int | None = None,
    ) -> Mapping[str, object] | None:
        self._ensure_authenticated(self.alist)
        executor = SimplePlanExecutor(self.alist, self.tmdb)
        raw = (
            executor._exact_with_visibility_retry(path, expected_size=expected_size)
            if wait_for_visibility
            else executor._visible_exact(path)
        )
        if raw is None:
            return None
        size = raw.get("size") if isinstance(raw, Mapping) else None
        if isinstance(size, bool) or not isinstance(size, int) or size < 0:
            raise SimpleEngineError(f"AList readback 没有有效大小: {path}")
        return {"size": size}

    def _readback_plan(
        self,
        plan: object,
        *,
        allow_pending_cleanup: bool = False,
    ) -> dict[str, object]:
        _require_problem_free_plan(plan, stage="恢复检查")
        _require_cleanup_allowlist(plan, stage="恢复检查")
        media_only = _is_provider_media_only_plan(plan)
        files = [
            item for item in list(getattr(plan, "files", ()) or ())
            if not media_only or getattr(item, "media_kind", None) == "video"
        ]
        if not files:
            raise EngineExecutionError("恢复检查的计划没有媒体文件")
        if media_only:
            try:
                _require_provider_tv_child_primary_videos(plan, stage="child 恢复检查")
            except ValueError as exc:
                raise EngineExecutionError(str(exc)) from exc
        verified_files: list[dict[str, object]] = []
        for item in files:
            source = _safe_remote_path(str(getattr(item, "source_path")), field="source_path", allow_root=False)
            _require_non_test_media_path(source, stage="恢复检查")
            target = _safe_remote_path(
                posixpath.join(str(getattr(item, "target_dir")), str(getattr(item, "final_name"))),
                field="target_path",
                allow_root=False,
            )
            expected = getattr(item, "source_size", None)
            if isinstance(expected, bool) or not isinstance(expected, int) or expected < 0:
                raise EngineExecutionError(f"恢复检查没有有效源大小: {source}")
            _require_admissible_video_size(
                item, expected, path=source, stage="恢复检查",
            )
            # Read the target without an expected-size filter first.  The
            # provider helper may return its last observed object when the
            # expected size never appeared; passing ``expected`` here would
            # collapse that durable mismatch into an indistinguishable
            # absence and could incorrectly trigger a retry.
            observed_target = self._exact_info(target, expected_size=None)
            if observed_target is not None:
                actual_target_size = int(observed_target["size"])
                if actual_target_size != expected:
                    raise EngineRecoveryMatrixError(
                        "target_size_mismatch",
                        (
                            "恢复检查目标大小不匹配: "
                            f"{target} expected={expected}, actual={actual_target_size}"
                        ),
                        source=source,
                        target=target,
                        expected_size=expected,
                        actual_size=actual_target_size,
                    )
                _require_admissible_video_size(
                    item, actual_target_size, path=target, stage="恢复检查",
                )
                observed_source = self._exact_info(source, wait_for_visibility=False)
                if observed_source is not None:
                    source_size = int(observed_source["size"])
                    if source_size != expected:
                        raise EngineRecoveryMatrixError(
                            "source_size_mismatch",
                            (
                                "恢复检查源文件大小不匹配: "
                                f"{source} expected={expected}, actual={source_size}"
                            ),
                            source=source,
                            target=target,
                            expected_size=expected,
                            actual_size=source_size,
                        )
                    raise EngineRecoveryMatrixError(
                        "target_source_conflict",
                        f"恢复检查发现目标和源文件同时存在，状态冲突: {target} / {source}",
                        source=source,
                        target=target,
                        expected_size=expected,
                        actual_size=actual_target_size,
                    )
            else:
                observed_source = self._exact_info(source, wait_for_visibility=False)
                if observed_source is None:
                    raise EngineRecoveryMatrixError(
                        "source_lost",
                        f"恢复检查发现目标和源文件均不存在，来源已丢失: {source} / {target}",
                        source=source,
                        target=target,
                        expected_size=expected,
                    )
                source_size = int(observed_source["size"])
                if source_size != expected:
                    raise EngineRecoveryMatrixError(
                        "source_size_mismatch",
                        (
                            "恢复检查源文件大小不匹配: "
                            f"{source} expected={expected}, actual={source_size}"
                        ),
                        source=source,
                        target=target,
                        expected_size=expected,
                        actual_size=source_size,
                    )
                raise EngineRecoveryRetryableError(
                    "target_missing_source_present",
                    f"恢复检查发现源文件仍在但目标不存在，可安全重试: {source} -> {target}",
                    source=source,
                    target=target,
                )
            verified_files.append({"source": source, "target": target, "size": expected})

        verified_artifacts: list[dict[str, object]] = []
        if not media_only:
            # A provider child is a media-only transaction.  Recovery checks
            # the moved video and cleanup source only; it neither requires nor
            # synthesizes the ordinary root/season/episode artifact set.
            engine = __import__("engine.scraper", fromlist=["planned_nfos", "planned_artwork"])
            planned_nfos = getattr(engine, "planned_nfos", None)
            if callable(planned_nfos):
                for target, content in planned_nfos(plan):
                    if not isinstance(target, str) or not isinstance(content, (bytes, bytearray)):
                        raise EngineExecutionError("Engine NFO 计划格式无效")
                    # As with media targets, inspect the actual observed size
                    # before deciding whether the artifact is merely missing
                    # (safe to replay) or is a durable conflicting object
                    # (terminal, never overwrite during recovery).
                    observed = self._exact_info(
                        target,
                        wait_for_visibility=True,
                        expected_size=None,
                    )
                    if observed is not None and int(observed["size"]) != len(content):
                        raise EngineRecoveryMatrixError(
                            "artifact_size_mismatch",
                            (
                                "恢复检查 NFO 大小不匹配: "
                                f"{target} expected={len(content)}, actual={observed['size']}"
                            ),
                            target=target,
                            expected_size=len(content),
                            actual_size=int(observed["size"]),
                        )
                    if observed is None:
                        raise EngineExecutionError(f"恢复检查找不到或无法核对 NFO: {target}")
                    verified_artifacts.append({"target": target, "kind": "nfo", "size": len(content)})
            planned_artwork = getattr(engine, "planned_artwork", None)
            if callable(planned_artwork):
                for target, _image_path, role in planned_artwork(plan):
                    observed = self._exact_info(
                        target,
                        wait_for_visibility=True,
                        expected_size=None,
                    )
                    if observed is not None and int(observed["size"]) <= 0:
                        raise EngineRecoveryMatrixError(
                            "artifact_size_mismatch",
                            f"恢复检查海报大小无效: {target} actual={observed['size']}",
                            target=target,
                            actual_size=int(observed["size"]),
                        )
                    if observed is None:
                        raise EngineExecutionError(f"恢复检查找不到海报: {target}")
                    verified_artifacts.append({"target": target, "kind": str(role), "size": int(observed["size"])})
        cleaned: list[str] = []
        pending_cleanup: list[str] = []
        for item in list(getattr(plan, "cleanup_files", ()) or ()):
            source = _safe_remote_path(
                str(getattr(item, "source_path")),
                field="cleanup_source_path",
                allow_root=False,
            )
            if allow_pending_cleanup:
                pending_cleanup.append(source)
                continue
            if self._exact_info(source, wait_for_visibility=False) is not None:
                raise EngineExecutionError(f"恢复检查发现清理项仍存在: {source}")
            cleaned.append(source)
        return {
            "recovered": True,
            "files": verified_files,
            "file_count": len(verified_files),
            "artifacts": verified_artifacts,
            "artifact_count": len(verified_artifacts),
            "media_only": media_only,
            "cleanup": cleaned,
            "cleanup_count": len(cleaned),
            "cleanup_deferred": allow_pending_cleanup,
            "cleanup_pending": pending_cleanup,
        }

    def _remove_empty_archive_staging(self, job: EngineJob | str) -> list[str]:
        """Remove and read back only a task's ``archive`` staging subtree.

        AList may report an empty list for both an empty directory and a
        missing path.  Probe the exact parent/name pair before each deletion
        and again afterwards, so a failed remote cleanup cannot be recorded as
        completed merely because an exception was swallowed.  This method
        deliberately knows only descendants of ``<task>/archive`` plus that
        exact directory.  It must never delete ``<task>`` itself: successful
        source consumption moves the original input to its sibling
        ``<task>/processed`` directory, which is not staging and remains owned
        by the user.
        """
        owner = job if isinstance(job, EngineJob) else self._read(job)
        projection = owner.summary.get("archive_preprocessed")
        if not isinstance(projection, Mapping) or projection.get("changed") is not True:
            return []

        _local_root, remote_root = self._archive_task_roots(owner.id)
        listing = getattr(self.alist, "list", None)
        remove = getattr(self.alist, "remove", None)
        remove_empty = getattr(self.alist, "remove_empty_dir", None)
        if not callable(listing):
            raise EngineExecutionError("AList 客户端缺少 list 接口，无法核对归档 staging 清理")
        if not callable(remove):
            raise EngineExecutionError("AList 客户端缺少 remove 接口，无法清理归档 staging 文件")
        if not callable(remove_empty):
            raise EngineExecutionError("AList 客户端缺少 remove_empty_dir 接口，无法清理归档 staging")

        def rows(path: str) -> list[Mapping[str, object]]:
            try:
                raw = listing(path, refresh=True)
            except TypeError:
                raw = listing(path)
            except Exception as exc:
                raise EngineExecutionError(f"无法读取归档 staging 目录: {path}: {exc}") from exc
            if not isinstance(raw, list) or any(not isinstance(item, Mapping) for item in raw):
                raise EngineExecutionError(f"AList 归档 staging 目录回读格式无效: {path}")
            return list(raw)

        def safe_name(directory: str, item: Mapping[str, object]) -> str:
            name = item.get("name")
            if (
                not isinstance(name, str)
                or not name
                or name in {".", ".."}
                or "/" in name
                or "\\" in name
            ):
                raise EngineExecutionError(f"AList 归档 staging 目录出现不安全条目: {directory}")
            return name

        def directory_exists(path: str) -> bool:
            parent, name = posixpath.split(path.rstrip("/"))
            if not parent or not name:
                raise EngineExecutionError(f"归档 staging 路径无效: {path}")
            matches = [
                item for item in rows(parent)
                if item.get("name") == name
            ]
            if len(matches) > 1:
                raise EngineExecutionError(f"归档 staging 父目录出现重名条目: {path}")
            if not matches:
                return False
            if matches[0].get("is_dir") is not True:
                raise EngineExecutionError(f"归档 staging 路径不是目录: {path}")
            return True

        def directory_missing(path: str) -> bool:
            for delay in (0.0, 0.25, 0.5, 1.0):
                if delay:
                    time.sleep(delay)
                if not directory_exists(path):
                    return True
            return False

        def remove_empty_checked(path: str) -> None:
            parent, name = posixpath.split(path.rstrip("/"))
            if not parent or not name:
                raise EngineExecutionError(f"归档 staging 路径无效: {path}")
            try:
                deleted = remove_empty(path)
            except Exception as exc:
                raise EngineExecutionError(f"无法清理空归档 staging 目录: {path}: {exc}") from exc
            if deleted is False:
                raise EngineExecutionError(f"归档 staging 目录仍非空，拒绝删除: {path}")
            if directory_missing(path):
                return
            # AList's remove_empty_directory cleans empty descendants for some
            # drivers but can leave the requested directory itself visible.
            # The directory was just proven empty, so delete that exact
            # basename through the ordinary remove endpoint and read it back.
            try:
                remove(parent, [name])
            except Exception as exc:
                raise EngineExecutionError(f"无法删除空归档 staging 目录: {path}: {exc}") from exc
            if not directory_missing(path):
                raise EngineExecutionError(f"归档 staging 清理后目录仍存在: {path}")

        removed: list[str] = []
        archive_root = f"{remote_root}/archive"

        def purge_children(directory: str) -> None:
            for item in rows(directory):
                _cancellation_checkpoint()
                name = safe_name(directory, item)
                child = posixpath.join(directory, name)
                if item.get("is_dir") is True:
                    purge_children(child)
                    if rows(child):
                        raise EngineExecutionError(f"归档 staging 目录仍非空，拒绝删除: {child}")
                    remove_empty_checked(child)
                    removed.append(child)
                    continue
                try:
                    remove(directory, [name])
                except Exception as exc:
                    raise EngineExecutionError(f"无法清理归档 staging 文件: {child}: {exc}") from exc
                if any(row.get("name") == name for row in rows(directory)):
                    raise EngineExecutionError(f"归档 staging 文件清理后仍存在: {child}")
                removed.append(child)

        if directory_exists(archive_root):
            purge_children(archive_root)
            if rows(archive_root):
                raise EngineExecutionError(f"归档 staging 目录仍非空，拒绝删除: {archive_root}")
            remove_empty_checked(archive_root)
            removed.append(archive_root)
        return removed

    def record_automatic_lifecycle_decision(
        self,
        job_id: str,
        *,
        audit_status: str,
        provider_status: str,
        cleanup_ready: bool,
        reason: str | None = None,
    ) -> EngineJob:
        """Persist the audit/provider decision that gates final cleanup."""
        if not isinstance(audit_status, str) or not audit_status:
            raise EngineRequestError("lifecycle audit status 无效")
        if not isinstance(provider_status, str) or not provider_status:
            raise EngineRequestError("lifecycle provider status 无效")
        if type(cleanup_ready) is not bool:
            raise EngineRequestError("lifecycle cleanup_ready 必须是布尔值")
        with self.worker_lock():
            job = self._read(job_id)
            if job.summary.get("automatic") is not True:
                return job
            summary = dict(job.summary)
            lifecycle_raw = summary.get("lifecycle")
            lifecycle = dict(lifecycle_raw) if isinstance(lifecycle_raw, Mapping) else {}
            cleanup_raw = lifecycle.get("cleanup")
            cleanup = dict(cleanup_raw) if isinstance(cleanup_raw, Mapping) else {}
            # A delayed audit/provider callback must not re-open a root whose
            # source/staging evidence was already consumed.  Similarly, do
            # not let a new decision overwrite the in-progress record that a
            # finalizer will use for crash recovery.
            if cleanup.get("status") == "completed":
                return job
            if cleanup.get("status") == "running":
                raise EngineWorkerBusyError("任务最终清理正在执行，不能覆盖生命周期决定")
            now = _now()
            lifecycle["audit"] = {
                "status": audit_status,
                "updated_at": now,
            }
            provider: dict[str, object] = {
                "status": provider_status,
                "updated_at": now,
            }
            if reason:
                provider["reason"] = redact_error(reason)
            lifecycle["provider"] = provider
            lifecycle["cleanup_ready"] = cleanup_ready
            summary["lifecycle"] = lifecycle
            updated = replace(job, summary=summary, updated_at=now)
            atomic_write_json(self._job_path(job.id), updated.as_dict(), allow_nan=False)
            return updated

    @staticmethod
    def _actionable_root_resource_gaps(job: EngineJob) -> list[dict[str, object]]:
        """Return every durable resource gap that still needs a Provider lane.

        The plan is the durable root contract, while ``summary.resource_gaps``
        is the latest audit projection.  Either can be newer after a restart
        or a delayed coordinator callback, so final cleanup must treat them as
        one fail-closed set.  Only the shared Provider-actionable kinds fence
        cleanup; informational planner notices such as an unpaired subtitle do
        not turn into an unrelated source-retention deadlock.
        """
        sources: list[object] = []
        summary_rows = job.summary.get("resource_gaps")
        if isinstance(summary_rows, list):
            sources.extend(summary_rows)
        scan = job.plan.get("scan_report")
        if isinstance(scan, Mapping):
            plan_rows = scan.get("resource_gaps")
            if isinstance(plan_rows, list):
                sources.extend(plan_rows)

        gaps: list[dict[str, object]] = []
        seen: set[tuple[str, str, str, str]] = set()
        for raw in sources:
            if not isinstance(raw, Mapping):
                continue
            kind = str(raw.get("kind") or "").strip().casefold()
            if kind not in ACTIONABLE_GAP_KINDS:
                continue
            # The same audit row is usually persisted in both the plan and
            # summary.  Deduplicate only for stable diagnostics; either copy
            # remains sufficient to fence final cleanup.
            key = (
                kind,
                str(raw.get("id") or ""),
                str(raw.get("label") or ""),
                str(raw.get("path") or ""),
            )
            if key in seen:
                continue
            seen.add(key)
            gaps.append(dict(raw))
        return gaps

    def finalize_automatic_lifecycle(
        self,
        job_id: str,
        *,
        pause_requested: Callable[[], bool] | None = None,
    ) -> EngineJob:
        """Finish task-owned source/staging cleanup after audit/provider gates.

        The method is deliberately separate from ``execute_job``.  It is
        idempotent and only accepts a durable ``lifecycle.cleanup_ready``
        decision written by the audit/provider coordinator, so a writer or a
        stale provider callback cannot consume ingress early.
        """
        with self.worker_lock():
            job = self._read(job_id)
            effective_pause = pause_requested if pause_requested is not None else self._pause_requested
            cancelled = self._consume_cancel_request(job)
            if cancelled is not None:
                return cancelled
            _pause_checkpoint(effective_pause)
            if job.summary.get("internal_child") is True:
                raise EngineJobConflictError("内部 child 不能执行根任务最终清理")
            if job.summary.get("automatic") is not True:
                raise EngineRequestError("只有 automatic 根任务使用最终生命周期清理")
            lifecycle_raw = job.summary.get("lifecycle")
            lifecycle = dict(lifecycle_raw) if isinstance(lifecycle_raw, Mapping) else {}
            cleanup_raw = lifecycle.get("cleanup")
            cleanup = dict(cleanup_raw) if isinstance(cleanup_raw, Mapping) else {}
            actionable_gaps = self._actionable_root_resource_gaps(job)
            if actionable_gaps:
                # ``cleanup_ready`` is coordinator input, not permission to
                # override an observed missing resource.  In particular,
                # historical JSON or a late external callback may still carry
                # ``true`` after a fresh audit has recorded a gap.  Correct
                # that stale bit before returning so a subsequent caller
                # cannot make source/staging cleanup appear authorized.
                state_changed = lifecycle.get("cleanup_ready") is not False
                lifecycle["cleanup_ready"] = False
                # A historical coordinator might also have persisted a
                # completed cleanup before the gap observation reached this
                # root.  Never let that idempotent marker bypass the fence on
                # the next call.  Keep any completed-step evidence so, once a
                # later targeted audit proves the gap gone, retry remains
                # idempotent and does not replay a remote deletion.
                if cleanup.get("status") == "completed":
                    cleanup["status"] = "pending"
                    cleanup["invalidated_by_resource_gaps_at"] = _now()
                    lifecycle["cleanup"] = cleanup
                    state_changed = True
                if state_changed:
                    summary = dict(job.summary)
                    summary["lifecycle"] = lifecycle
                    job = replace(job, summary=summary, updated_at=_now())
                    atomic_write_json(
                        self._job_path(job.id), job.as_dict(), allow_nan=False,
                    )
                kinds = ", ".join(sorted({
                    str(gap.get("kind") or "").strip().casefold()
                    for gap in actionable_gaps
                }))
                raise EngineWorkerBusyError(
                    "资源缺口尚未消失，拒绝最终清理"
                    + (f": {kinds}" if kinds else "")
                )
            pending_reaudit = self._pending_replenishment_reaudit_states(job.id)
            if pending_reaudit:
                # The formal child may have completed, but its provider
                # staging still belongs to the selected-gap re-audit.  A
                # coordinator callback cannot use ``cleanup_ready`` to skip
                # that independently durable state boundary.
                state_changed = lifecycle.get("cleanup_ready") is not False
                lifecycle["cleanup_ready"] = False
                if cleanup.get("status") == "completed":
                    cleanup["status"] = "pending"
                    cleanup["invalidated_by_post_acquisition_reaudit_at"] = _now()
                    lifecycle["cleanup"] = cleanup
                    state_changed = True
                if state_changed:
                    summary = dict(job.summary)
                    summary["lifecycle"] = lifecycle
                    job = replace(job, summary=summary, updated_at=_now())
                    atomic_write_json(
                        self._job_path(job.id), job.as_dict(), allow_nan=False,
                    )
                raise EngineWorkerBusyError(
                    "补源 staging 尚待定向重审/清理，拒绝最终清理"
                )
            if cleanup.get("status") == "completed":
                # Earlier drafts could leave a successfully retried cleanup
                # in ``failed_cleanup``.  Normalize that public projection on
                # the idempotent path without running any remote operation.
                if job.phase == "failed_cleanup":
                    normalized = replace(
                        job,
                        phase="executed",
                        updated_at=_now(),
                        error=None,
                    )
                    atomic_write_json(
                        self._job_path(job.id), normalized.as_dict(), allow_nan=False,
                    )
                    return normalized
                return job
            formal_write = lifecycle.get("formal_write")
            if not isinstance(formal_write, Mapping) or formal_write.get("status") != "verified":
                raise EngineWorkerBusyError("正式写入/回读尚未形成可清理事实")
            if lifecycle.get("cleanup_ready") is not True:
                raise EngineWorkerBusyError("审计或补源尚未形成最终清理决定")
            audit = lifecycle.get("audit")
            provider = lifecycle.get("provider")
            if not (
                isinstance(audit, Mapping)
                and isinstance(audit.get("status"), str)
                and audit.get("status")
                and isinstance(provider, Mapping)
                and isinstance(provider.get("status"), str)
                and provider.get("status")
            ):
                raise EngineWorkerBusyError("审计/补源决定记录不完整，不能最终清理")
            if job.phase not in {"executed", "completed", "cleaning", "failed_cleanup"}:
                raise EngineWorkerBusyError(f"任务当前不能最终清理: {job.phase}")
            children = self._owned_children(job)
            if any(
                child.phase not in _CLEANUP_TERMINAL_PHASES
                for child in children
            ):
                raise EngineWorkerBusyError("任务仍有活动内部子任务，不能最终清理")

            steps_raw = cleanup.get("steps")
            steps = dict(steps_raw) if isinstance(steps_raw, Mapping) else {}
            attempts_raw = cleanup.get("attempts")
            attempts = attempts_raw if isinstance(attempts_raw, int) and not isinstance(attempts_raw, bool) else 0
            cleanup["status"] = "running"
            cleanup["attempts"] = attempts + 1
            cleanup["steps"] = steps
            cleanup["updated_at"] = _now()
            cleanup.pop("error", None)
            lifecycle["cleanup"] = cleanup
            running_summary = self._with_active_operation(
                job.summary,
                kind="final_cleanup",
            )
            running_summary["lifecycle"] = lifecycle
            running = replace(
                job,
                phase="cleaning",
                summary=running_summary,
                updated_at=_now(),
                error=None,
            )
            atomic_write_json(self._job_path(job.id), running.as_dict(), allow_nan=False)

            current = running
            missing = object()
            pause_token = _PAUSE_REQUEST_CHECK.set(effective_pause)
            cancel_token = _CANCEL_REQUEST_CHECK.set(
                lambda: self._cancel_requested(current)
            )

            def prior_result(step: str) -> object:
                raw = steps.get(step)
                if isinstance(raw, Mapping) and raw.get("status") == "completed":
                    return raw.get("result", missing)
                return missing

            def update_projection(
                summary: dict[str, object],
                step: str,
                result: object,
            ) -> None:
                if step == "source_consumption":
                    if isinstance(result, Mapping):
                        summary["source_fate"] = str(result.get("status") or "already_consumed")
                    else:
                        summary["source_fate"] = "already_consumed"
                    return
                staging_raw = summary.get("staging_fate")
                staging = dict(staging_raw) if isinstance(staging_raw, Mapping) else {}
                if step == "plan_cleanup":
                    staging["plan_cleanup"] = result
                elif step == "archive_remote_staging":
                    staging["archive_remote_removed"] = result
                elif step == "archive_local_staging":
                    staging["archive_local_removed"] = bool(result)
                summary["staging_fate"] = staging

            def persist_completed_step(step: str, result: object) -> None:
                nonlocal current
                now = _now()
                steps[step] = {
                    "status": "completed",
                    "updated_at": now,
                    "result": _jsonable(result),
                }
                cleanup["status"] = "running"
                cleanup["steps"] = steps
                cleanup["updated_at"] = now
                cleanup.pop("error", None)
                lifecycle["cleanup"] = cleanup
                summary = dict(current.summary)
                summary["lifecycle"] = dict(lifecycle)
                update_projection(summary, step, _jsonable(result))
                current = replace(
                    current,
                    phase="cleaning",
                    summary=summary,
                    updated_at=now,
                    error=None,
                )
                atomic_write_json(
                    self._job_path(current.id), current.as_dict(), allow_nan=False,
                )

            current_step = "plan_cleanup"
            try:
                _cancellation_checkpoint()
                stored_cleanup = prior_result("plan_cleanup")
                if isinstance(stored_cleanup, Mapping):
                    cleanup_result: Mapping[str, object] = dict(stored_cleanup)
                else:
                    plan = self._plan_from_job(current)
                    cleanup_result = SimplePlanExecutor(self.alist, self.tmdb).finalize_cleanup(plan)
                    if not isinstance(cleanup_result, Mapping):
                        raise EngineExecutionError("最终清理返回无效结果")
                    cleanup_result = dict(cleanup_result)
                    persist_completed_step("plan_cleanup", cleanup_result)

                current_step = "source_consumption"
                _cancellation_checkpoint()
                stored_consumed = prior_result("source_consumption")
                if isinstance(stored_consumed, Mapping):
                    consumed: Mapping[str, object] | None = dict(stored_consumed)
                else:
                    consumed = self._consume_archive_source(current)
                    consumed_result: Mapping[str, object] = (
                        dict(consumed)
                        if isinstance(consumed, Mapping)
                        else {"status": "already_consumed"}
                    )
                    persist_completed_step("source_consumption", consumed_result)

                current_step = "archive_remote_staging"
                _cancellation_checkpoint()
                stored_staging = prior_result("archive_remote_staging")
                if isinstance(stored_staging, list) and all(isinstance(item, str) for item in stored_staging):
                    staging_removed = list(stored_staging)
                else:
                    staging_removed = self._remove_empty_archive_staging(current)
                    persist_completed_step("archive_remote_staging", staging_removed)

                current_step = "archive_local_staging"
                _cancellation_checkpoint()
                stored_local = prior_result("archive_local_staging")
                if type(stored_local) is bool:
                    local_archive_removed = stored_local
                else:
                    local_archive_removed = self._remove_owned_local_tree(
                        self.state_root / "archive-staging",
                        current.id,
                        label="archive local staging",
                    )
                    persist_completed_step("archive_local_staging", local_archive_removed)
            except EnginePauseRequested:
                # Keep the cleaning operation and completed-step evidence;
                # resume can continue the next cleanup boundary idempotently.
                return self._read(job_id)
            except EngineCancellationRequested:
                cancelled = self._consume_cancel_request(current)
                return cancelled or self._cancelled_job(
                    current,
                    reason="cancelled by operator",
                )
            except Exception as exc:
                failed_lifecycle = dict(lifecycle)
                now = _now()
                steps[current_step] = {
                    "status": "failed",
                    "error": redact_error(exc),
                    "updated_at": now,
                }
                failed_cleanup = dict(cleanup)
                failed_cleanup.update({
                    "status": "failed",
                    "error": redact_error(exc),
                    "updated_at": now,
                    "steps": steps,
                })
                failed_lifecycle["cleanup"] = failed_cleanup
                failed_summary = self._without_active_operation(current.summary)
                failed_summary["cleanup_only_retry"] = True
                failed_summary["lifecycle"] = failed_lifecycle
                failed = replace(
                    current,
                    phase="failed_cleanup",
                    summary=failed_summary,
                    updated_at=now,
                    error=redact_error(exc),
                )
                atomic_write_json(self._job_path(current.id), failed.as_dict(), allow_nan=False)
                return failed
            finally:
                _CANCEL_REQUEST_CHECK.reset(cancel_token)
                _PAUSE_REQUEST_CHECK.reset(pause_token)

            final_lifecycle = dict(lifecycle)
            final_cleanup = dict(cleanup)
            final_cleanup.update({
                "status": "completed",
                "updated_at": _now(),
                "steps": steps,
            })
            final_cleanup.pop("error", None)
            final_lifecycle["cleanup"] = final_cleanup
            final_summary = self._without_active_operation(current.summary)
            final_summary.pop("cleanup_only_retry", None)
            final_summary["lifecycle"] = final_lifecycle
            final_summary["source_fate"] = str(
                consumed.get("status") if isinstance(consumed, Mapping) else "already_consumed"
            )
            staging_raw = final_summary.get("staging_fate")
            final_summary["staging_fate"] = {
                **(dict(staging_raw) if isinstance(staging_raw, Mapping) else {}),
                "archive_remote_removed": staging_removed,
                "archive_local_removed": bool(local_archive_removed),
                "plan_cleanup": dict(cleanup_result),
            }
            execution = dict(current.execution) if isinstance(current.execution, Mapping) else {}
            execution["final_cleanup"] = dict(cleanup_result)
            if isinstance(consumed, Mapping):
                execution["archive_source_consumption"] = dict(consumed)
            completed = replace(
                current,
                phase="completed" if job.phase == "completed" else "executed",
                summary=final_summary,
                execution=execution,
                updated_at=_now(),
                error=None,
            )
            atomic_write_json(self._job_path(completed.id), completed.as_dict(), allow_nan=False)
            self._clear_cancel_request(job_id)
            return completed

    def cancel_job(self, job_id: str, *, reason: str = "cancelled by operator") -> EngineJob:
        """Cancel a queued job immediately or a running one at a safe boundary.

        A queued/inactive job changes only its durable projection and never
        touches AList.  When another process owns the formal-write lock,
        archive/identity/planning/executing/cleanup phases receive a
        task-scoped request marker;
        the current remote operation completes, then the owning worker marks
        the job cancelled before another move, upload, or cleanup action.
        """
        normalized_reason = reason.strip() or "cancelled by operator"
        immediate_phases = {
            "reconciling", "reconciled", "reconciliation_uncertain",
            "awaiting_target_shelf", "target_policy_conflict", "queued",
            "archive_preprocessing", "identity_matching", "planning", "planned",
            "retry_wait", "failed", "failed_archive", "failed_identity",
            "failed_planning", "failed_provider", "failed_write",
            "failed_verification", "failed_cleanup", "executing", "verifying",
            "cleaning", "cancelled",
        }
        try:
            with self.worker_lock():
                job = self._read(job_id)
                if job.phase in {"executed", "completed"}:
                    self._clear_cancel_request(job_id)
                    return job
                if job.phase not in immediate_phases:
                    raise SimpleEngineError(
                        f"Engine job {job_id} 当前不能取消: {job.phase}"
                    )
                if job.phase == "cancelled":
                    self._clear_cancel_request(job_id)
                    return job
                return self._cancelled_job(job, reason=normalized_reason)
        except EngineWorkerBusyError:
            # Fence an idle revision before atomically closing it. Another
            # task owns the global worker lock, so this job cannot start until
            # that lock is released; its marker protects the tiny release
            # race. Active operations instead consume a marker at their next
            # safe AList/planning boundary.
            job = self._read(job_id)
            inactive_phases = {
                "reconciling",
                "reconciled", "reconciliation_uncertain",
                "awaiting_target_shelf", "target_policy_conflict", "queued",
                "planned", "retry_wait", "failed", "failed_archive",
                "failed_identity", "failed_planning", "failed_provider",
                "failed_write", "failed_verification", "failed_cleanup",
            }
            if job.phase in inactive_phases:
                self._request_inactive_cancellation(job, reason=normalized_reason)
                try:
                    with self.worker_lock():
                        latest = self._read(job_id)
                        if (
                            latest.phase != job.phase
                            or latest.updated_at != job.updated_at
                        ):
                            self._clear_cancel_request(job_id)
                            return latest
                        return self._consume_cancel_request(latest) or latest
                except EngineWorkerBusyError:
                    # The marker is the durable cancellation intent. Do not
                    # overwrite the stale queued snapshot while the other
                    # worker still owns the lock; its next planning boundary
                    # will consume the marker before any formal operation.
                    return self._read(job_id)
            if job.phase not in {
                "archive_preprocessing", "identity_matching", "planning",
                "executing", "verifying", "cleaning",
            }:
                raise
            operation_id = self._active_operation_id(job)
            self._request_running_cancellation(job, reason=normalized_reason)
            # Close the small restart/finish race around the marker write. If
            # the worker released its lock before the marker landed, consume
            # it under the lock now; if the operation already advanced, clear
            # the stale marker rather than applying it to a later retry.
            try:
                with self.worker_lock():
                    latest = self._read(job_id)
                    if (
                        latest.phase == "retry_wait"
                        and latest.summary.get("recovered_operation_id") == operation_id
                    ):
                        # Restart recovery saw an interrupted write before
                        # this request could acquire its lock.  The explicit
                        # operator intent still owns that recovered operation;
                        # cancel it rather than allowing a later readback or
                        # replay to revive the job.
                        self._clear_cancel_request(job_id)
                        return self._cancelled_job(latest, reason=normalized_reason)
                    if (
                        operation_id is None
                        or self._active_operation_id(latest) != operation_id
                        or latest.phase not in {
                            "archive_preprocessing", "identity_matching", "planning",
                            "executing", "verifying", "cleaning",
                        }
                    ):
                        self._clear_cancel_request(job_id)
                        return latest
                    return self._consume_cancel_request(latest) or latest
            except EngineWorkerBusyError:
                latest = self._read(job_id)
                if self._active_operation_id(latest) != operation_id:
                    self._clear_cancel_request(job_id)
                return latest


__all__ = [
    "AutomaticIdentity",
    "EngineCancellationRequested",
    "EnginePauseRequested",
    "EngineExecutionError",
    "EngineJob",
    "EngineJobConflictError",
    "EngineJobNotFoundError",
    "EngineRequest",
    "EngineRequestError",
    "EngineWorkerBusyError",
    "TargetShelfPolicyConflictError",
    "recover_persisted_engine_jobs",
    "SimpleEngineError",
    "SimpleEngineRunner",
    "SimplePlanExecutor",
]
