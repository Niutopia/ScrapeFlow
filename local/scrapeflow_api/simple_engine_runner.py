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
import unicodedata
import uuid
from dataclasses import asdict, dataclass, replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Callable, Iterator, Mapping, Protocol

from engine.scrapeflow.archive import ArchivePasswordError
from engine.scrapeflow.archive_preprocessing import ArchivePauseRequested
from engine.scrapeflow.errors import FormalTargetConflictError
from engine.scrapeflow.gap_ledger import load_gap_ledger
from engine.scrapeflow.media_quality import (
    is_video_filename,
    minimum_video_bytes,
    video_size_is_admissible,
)
from engine.scrapeflow.media_policy import is_subtitle_filename
from engine.scrapeflow.replacement import (
    ReplacementManifest,
    ReplacementValidationError,
    archive_path_for,
)
from engine.scrapeflow.residual_policy import (
    cleanup_allowlist_reason,
    is_task_owned_staging_root,
)
from engine.scrapeflow.remote_paths import (
    is_provider_safe_basename,
    provider_safe_basename,
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

    # Composition layers recognize this marker without importing the engine
    # module. A root-scoped pause during child plan/write is not an
    # infrastructure outage and must not schedule a retry as one.
    pause_requested = True


class EngineJobConflictError(EngineExecutionError):
    """A valid request conflicts with durable job/source state."""


class EngineRecoveryMatrixError(EngineExecutionError):
    """A restart readback found a durable, non-retryable state conflict.

    Recovery deliberately distinguishes a provider visibility/transport error
    (which may be retried) from facts that cannot be repaired by replaying the
    same plan. The latter are persisted as a terminal verification failure so
    a later retry cannot submit an operation that might overwrite a different
    object or silently recreate a lost source.
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
    """The one local formal writer is currently busy."""


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
    # in legacy identity callers.  Keep the selected first-level shelf under
    # an unambiguous name so a plan can never widen from its chosen category.
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
# A legacy direct automatic job may defer its own source cleanup until its
# formal write returns.  The current RootJob pipeline uses its task-scoped
# cleanup path instead.  Keep this narrow compatibility flag out of the
# public Plan protocol.
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

# A replenishment child is still an ordinary Engine plan: its media, NFO and
# artwork must use the same plan projection and writer as every other work.
# Keep this small validation separate from artifact selection.  It proves that
# a TV child handed over by the provider contains exactly one ordinary video
# for every explicit SxxEyy coordinate and cannot smuggle an NCOP/bonus/PV
# file into the formal library merely by being marked internal.
_INTERNAL_CHILD_EPISODE_TOKEN_RE = re.compile(
    r"(?i)(?<![a-z0-9])s0*(\d{1,3})[ ._-]*e(?:p)?0*(\d{1,4})(?!\d)"
)
_INTERNAL_CHILD_SUPPLEMENTAL_MEMBER_RE = re.compile(
    r"(?i)(?:^|[/\\\s._\-\[\](){}])"
    r"(?:bonus(?:es)?|extra(?:s)?|sample(?:s)?|scan(?:s)?|"
    r"menu|preview(?:s)?|trailer(?:s)?|teaser(?:s)?|featurette(?:s)?|"
    r"behind[ ._\-]*the[ ._\-]*scenes|"
    r"ncop|nced|pv|cm|creditless|op|ed)"
    r"(?=$|[/\\\s._\-\[\](){}])"
)


def _internal_child_episode_coordinates(value: object) -> set[str]:
    """Return explicit SxxEyy coordinates carried by one child filename."""
    return {
        f"S{int(match.group(1)):02d}E{int(match.group(2)):02d}"
        for match in _INTERNAL_CHILD_EPISODE_TOKEN_RE.finditer(str(value or ""))
        if 0 <= int(match.group(1)) <= 999 and 0 < int(match.group(2)) <= 9999
    }


def _internal_child_member_is_supplemental(*values: object) -> bool:
    return any(
        _INTERNAL_CHILD_SUPPLEMENTAL_MEMBER_RE.search(str(value or ""))
        for value in values
    )


def _internal_child_tv_primary_video_errors(plan: object) -> list[str]:
    """Return durable-plan violations for one internal replenishment TV child.

    This is an ownership/recovery guard, not a second provider planning path.
    It only applies to TV plans; container-artifact and layout carriers use
    their own modes and therefore retain their normal writer semantics.
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
        return ["内部 TV child 没有视频"]
    owners: dict[str, list[str]] = {}
    errors: list[str] = []
    for item in files:
        source_path = str(getattr(item, "source_path", "") or "")
        original_name = str(getattr(item, "original_name", "") or "")
        final_name = str(getattr(item, "final_name", "") or "")
        label = original_name or source_path or final_name or "<unknown>"
        if not all(
            is_video_filename(value)
            for value in (source_path, original_name, final_name)
        ):
            errors.append(f"内部 TV child 包含非视频主文件: {label}")
            continue
        if _internal_child_member_is_supplemental(
            source_path, original_name, final_name,
        ):
            errors.append(f"内部 TV child 包含附加内容: {label}")
            continue
        source_ids = _internal_child_episode_coordinates(Path(source_path).name)
        original_ids = _internal_child_episode_coordinates(original_name)
        final_ids = _internal_child_episode_coordinates(final_name)
        # The canonical final name is the authoritative coordinate.  A
        # bracketed/bare source (``[01]``) carries no SxxEyy token, so its
        # source/original sets are empty and must not invalidate the single
        # final coordinate the normal planner already derived from the
        # single-season proof.  When the source does carry an explicit
        # token, it still has to agree with the final coordinate.
        if len(final_ids) != 1:
            errors.append(f"内部 TV child 缺少一致的唯一 episode 映射: {label}")
            continue
        if source_ids and source_ids != final_ids:
            errors.append(f"内部 TV child 来源集号与最终集号不一致: {label}")
            continue
        if original_ids and original_ids != final_ids:
            errors.append(f"内部 TV child 原文件名集号与最终集号不一致: {label}")
            continue
        episode_id = next(iter(final_ids))
        owners.setdefault(episode_id, []).append(label)
    for episode_id, labels in owners.items():
        if len(labels) > 1:
            errors.append(
                f"内部 TV child 将多个视频映射到 {episode_id}: "
                + ", ".join(labels[:3])
            )
    return errors


def _require_internal_child_tv_primary_videos(plan: object, *, stage: str) -> None:
    errors = _internal_child_tv_primary_video_errors(plan)
    if errors:
        raise ValueError(f"{stage}拒绝不唯一/非正片内部 TV child: {errors[0]}")


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


class _PauseCheckedArchivePort:
    """Proxy every archive-adapter AList operation through the root fence.

    Archive adapters intentionally receive a small AList-shaped port rather
    than the runner itself.  Guarding that port preserves old adapter
    signatures while closing the check->download/mkdir/upload race that a
    single preprocessor-entry checkpoint cannot cover.
    """

    def __init__(
        self,
        target: object,
        pause_requested: Callable[[], bool] | None,
    ) -> None:
        self._target = target
        self._pause_requested = pause_requested

    def __getattr__(self, name: str) -> object:
        value = getattr(self._target, name)
        if not callable(value):
            return value

        def guarded(*args: object, **kwargs: object) -> object:
            try:
                if self._pause_requested is not None and self._pause_requested():
                    raise ArchivePauseRequested(
                        "暂停已生效，归档远端操作已安全停止",
                    )
            except ArchivePauseRequested:
                raise
            except Exception as exc:
                raise ArchivePauseRequested(
                    "暂停状态不可确认，归档远端操作已安全停止",
                ) from exc
            return value(*args, **kwargs)

        return guarded


_ENGINE_PHASES = frozenset({
    "reconciling", "reconciled", "reconciliation_uncertain",
    "awaiting_target_shelf", "queued", "analyzing", "archive_preprocessing", "identity_matching",
    "target_policy_conflict", "planning", "planned",
    "executing", "verifying", "cleaning", "executed", "gaps_pending", "completed",
    "retry_wait", "failed", "failed_archive", "failed_identity", "failed_planning", "failed_provider",
    "failed_write", "failed_verification", "failed_cleanup", "cancelled",
})

# A cleanup request is intentionally narrower than a general state migration.
# ``executed`` is the durable Engine write fact; cleanup only removes local
# state after the root and its own children are terminal.
_CLEANUP_TERMINAL_PHASES = frozenset({
    "executed", "completed", "failed", "failed_archive", "failed_identity", "failed_planning", "failed_provider",
    "failed_write", "failed_verification", "failed_cleanup", "cancelled",
})
_CLEANUP_ACTIVE_PROVIDER_STATUSES = frozenset({
    "gap_discovering", "provider_searching", "acquiring", "staging_verifying",
    "subtitle_installing", "child_planning", "child_executing", "final_verifying",
    "cleaning", "child_failed", "retry_wait",
})
# A problem row whose reason says the file genuinely stays at the source
# (an unidentifiable special with no official TMDB match) is informational,
# not a safety issue: the plan's media writes are all correctly mapped and
# the residual simply remains untouched.  These rows must not block the
# formal write of every correctly mapped file.
_PRESERVE_AT_SOURCE_PROBLEM_RE = re.compile(
    r"保留原位|保留于源目录|保留在来源|待人工确认|未闭合",
    re.IGNORECASE,
)


def _require_problem_free_plan(plan: object, *, stage: str) -> None:
    """Refuse every formal-write path while a plan still has open problems.

    Planner validation is useful but not a write boundary: persisted plans can
    predate a validation change and tests/providers may inject their own
    executor.  Keep this guard in the runner as well as the concrete executor
    so changing ``executor=`` cannot turn a problem-bearing plan into a move,
    upload, or cleanup operation.

    A problem row whose reason explicitly says the file stays at the source
    (an unidentifiable OVA/SP with no unique official TMDB match) does not
    block the write: the plan's actual media files are all correctly mapped,
    and the residual simply remains untouched at source.
    """
    problems = list(getattr(plan, "problem_files", ()) or ())
    if not problems:
        return
    blocking = [
        problem for problem in problems
        if not _PRESERVE_AT_SOURCE_PROBLEM_RE.search(
            str(getattr(problem, "reason", "") or "")
        )
    ]
    if not blocking:
        return
    first = blocking[0]
    path = str(getattr(first, "source_path", "") or "<unknown>")
    reason = str(getattr(first, "reason", "") or "")
    detail = f": {path}" + (f"（{reason}）" if reason else "")
    raise EngineExecutionError(
        f"{stage}拒绝含有 {len(blocking)} 个未闭合问题文件的计划{detail}"
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
    # Local state-root path of an explicit episode-map file the pipeline may
    # derive for multi-season absolute-number releases.  Deliberately NOT
    # accepted from the HTTP payload: only the internal unit pipeline sets it.
    episode_map_path: str | None = None
    # D/F-only gate for the narrow ``Title - 01`` grammar.  It is never an
    # external request option: F sets it only alongside a freshly revalidated
    # release-dash proof and its explicit source-key map.
    allow_release_dash_ordinal: bool = False
    # D/F-only gate for the narrow ``Title 01`` grammar.  It is never an
    # external request option: F sets it only beside a freshly revalidated
    # homogeneous title-ordinal proof and its explicit source-key map.
    allow_release_title_ordinal: bool = False
    # The following fields are internal-only WorkUnit ownership/placement
    # evidence.  They are persisted with the internal carrier so
    # execute/recovery can re-check the same boundary, but no HTTP request may
    # provide them.
    source_files: tuple[Mapping[str, object], ...] | None = None
    source_scope_paths: tuple[str, ...] = ()
    # Positive seasons B/W proved inside a single owned work root.  This is
    # not a TMDB or operator override: F uses it only to retain a subtitle-only
    # declared season at source while J later checks the official gap.
    source_declared_seasons: tuple[int, ...] = ()
    # Exact formal-library subtree a WorkUnit planner may target.  For a new
    # work this is its selected shelf (or a container below it); for a merged
    # work it can be the D-locked existing work root.  It is deliberately
    # separate from ``target_shelf`` because nested work units must not pretend
    # that their parent is a first-level shelf.
    target_scope_root: str | None = None

    @classmethod
    def from_mapping(cls, payload: Mapping[str, object]) -> "EngineRequest":
        if not isinstance(payload, Mapping):
            raise EngineRequestError("Engine 计划请求必须是 JSON 对象")
        if (
            "source_files" in payload
            or "source_scope_paths" in payload
            or "source_declared_seasons" in payload
            or "target_scope_root" in payload
            or "allow_release_dash_ordinal" in payload
            or "allow_release_title_ordinal" in payload
            or "episode_map_path" in payload
        ):
            raise EngineRequestError("WorkUnit 内部范围仅允许由服务端流程生成")
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

    @classmethod
    def from_persisted_mapping(cls, payload: Mapping[str, object]) -> "EngineRequest":
        """Restore a request persisted by the runner, including internal scope.

        This is intentionally separate from ``from_mapping``: browser/API
        callers never gain a way to nominate an arbitrary subset of a source,
        while execute/recovery can still verify the exact scope originally
        approved by B/W.
        """
        if not isinstance(payload, Mapping):
            raise EngineRequestError("持久化 Engine 请求必须是对象")
        raw = dict(payload)
        raw_files = raw.pop("source_files", None)
        raw_scopes = raw.pop("source_scope_paths", None)
        raw_declared_seasons = raw.pop("source_declared_seasons", None)
        raw_target_scope = raw.pop("target_scope_root", None)
        raw_release_dash_ordinal = raw.pop("allow_release_dash_ordinal", False)
        raw_release_title_ordinal = raw.pop("allow_release_title_ordinal", False)
        raw_episode_map_path = raw.pop("episode_map_path", None)
        request = cls.from_mapping(raw)
        if type(raw_release_dash_ordinal) is not bool:
            raise EngineRequestError("持久化 release-dash 集号开关必须是布尔值")
        if type(raw_release_title_ordinal) is not bool:
            raise EngineRequestError("持久化 title-ordinal 集号开关必须是布尔值")
        episode_map_path: str | None = None
        if raw_episode_map_path is not None:
            if (
                not isinstance(raw_episode_map_path, str)
                or not raw_episode_map_path.startswith("/")
                or "\x00" in raw_episode_map_path
            ):
                raise EngineRequestError("持久化 episode_map_path 必须是绝对路径")
            episode_map_path = raw_episode_map_path
        declared_seasons: tuple[int, ...] = ()
        if raw_declared_seasons is not None:
            if not isinstance(raw_declared_seasons, (list, tuple)):
                raise EngineRequestError("持久化来源声明季度必须是整数列表")
            normalized_declared: list[int] = []
            for value in raw_declared_seasons:
                if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
                    raise EngineRequestError("持久化来源声明季度包含无效值")
                normalized_declared.append(value)
            if len(set(normalized_declared)) != len(normalized_declared):
                raise EngineRequestError("持久化来源声明季度包含重复值")
            declared_seasons = tuple(sorted(normalized_declared))
        target_scope_root: str | None = None
        if raw_target_scope is not None:
            target_scope_root = _safe_remote_path(
                raw_target_scope,
                field="持久化 WorkUnit 目标范围",
                allow_root=False,
            )
            if not (
                target_scope_root == request.parent_path
                or target_scope_root.startswith(request.parent_path + "/")
            ):
                raise EngineRequestError("持久化 WorkUnit 目标范围不属于 Planner 父目录")
        if raw_files is None and (raw_scopes is None or raw_scopes == () or raw_scopes == []):
            return replace(
                request,
                target_scope_root=target_scope_root,
                source_declared_seasons=declared_seasons,
                allow_release_dash_ordinal=raw_release_dash_ordinal,
                allow_release_title_ordinal=raw_release_title_ordinal,
                episode_map_path=episode_map_path,
            )
        if not isinstance(raw_scopes, (list, tuple)):
            raise EngineRequestError("持久化来源范围必须是路径列表")
        if not isinstance(raw_files, (list, tuple)):
            raise EngineRequestError("持久化来源清单必须是文件列表")
        scopes = tuple(
            _safe_remote_path(value, field="持久化来源范围", allow_root=False)
            for value in raw_scopes
        )
        files: list[Mapping[str, object]] = []
        for value in raw_files:
            if not isinstance(value, Mapping):
                raise EngineRequestError("持久化来源清单包含无效条目")
            files.append(dict(value))
        return replace(
            request,
            source_files=tuple(files),
            source_scope_paths=scopes,
            source_declared_seasons=declared_seasons,
            target_scope_root=target_scope_root,
            allow_release_dash_ordinal=raw_release_dash_ordinal,
            allow_release_title_ordinal=raw_release_title_ordinal,
            episode_map_path=episode_map_path,
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
    plan for a later user-triggered exact readback, then replay only the
    still-missing operations. This function intentionally performs
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
            ) or (
                isinstance(marker, Mapping)
                and marker.get("kind") == "inactive"
                and marker.get("phase") == job.phase
                and marker.get("updated_at") == job.updated_at
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
                    "Engine 在远端结果写入前重启；等待用户恢复或重试后进行 AList 回读。"
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

    def _remote_entry_kind(self, path: str) -> str:
        """Freshly classify one exact remote path for layout recovery.

        ``list(path)`` cannot distinguish a missing path from an empty
        directory, so use the parent/name row just like the runner's
        read-only probe.  This helper lives on the executor so writer-side
        checks do not reach into the runner's private methods.
        """
        normalized = _safe_remote_path(path, field="远端探测路径", allow_root=False)
        parent, name = posixpath.split(normalized)
        listing = getattr(self.alist, "list", None)
        if not parent or not name or not callable(listing):
            return "missing"
        try:
            rows = listing(parent, refresh=True)
        except TypeError:
            rows = listing(parent)
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
        if (
            matches[0].get("is_link") is True
            or matches[0].get("is_symlink") is True
            or matches[0].get("symlink") is True
        ):
            return "ambiguous"
        return "directory" if matches[0].get("is_dir") is True else "file"

    def _fresh_tree_files(self, root: str) -> list[dict[str, object]]:
        """Return a bounded, exact recursive file inventory for one directory.

        This is intentionally separate from ``AListClient.walk``: an archive
        transaction must account for metadata and artwork as well as media,
        while the ordinary planner deliberately filters release extras.  The
        inventory is read-only and rejects malformed names, links and unknown
        sizes.
        """
        normalized = _safe_remote_path(root, field="递归清单根", allow_root=False)
        listing = getattr(self.alist, "list", None)
        if not callable(listing):
            raise EngineExecutionError("AList 客户端缺少 list 接口，无法核对布局来源")
        stack = [normalized]
        visited: set[str] = set()
        files: list[dict[str, object]] = []
        while stack:
            current = stack.pop()
            if current in visited:
                continue
            visited.add(current)
            if len(visited) > 10_000:
                raise EngineExecutionError("递归清单目录过多，已停止")
            try:
                rows = listing(current, refresh=True)
            except TypeError:
                rows = listing(current)
            if not isinstance(rows, list):
                raise EngineExecutionError(f"递归清单目录响应无效: {current}")
            for raw in rows:
                if not isinstance(raw, Mapping):
                    raise EngineExecutionError(f"递归清单包含无效条目: {current}")
                name = raw.get("name")
                if (
                    not isinstance(name, str)
                    or not name
                    or name in {".", ".."}
                    or "/" in name
                    or "\\" in name
                    or "\x00" in name
                ):
                    raise EngineExecutionError(f"递归清单包含不安全名称: {current}")
                if (
                    raw.get("is_link") is True
                    or raw.get("is_symlink") is True
                    or raw.get("symlink") is True
                ):
                    raise EngineExecutionError(f"递归清单拒绝链接条目: {current}/{name}")
                full_path = posixpath.join(current, name)
                if raw.get("is_dir") is True:
                    stack.append(full_path)
                    continue
                size = self._entry_size(raw)
                if size is None:
                    raise EngineExecutionError(f"递归清单文件缺少有效大小: {full_path}")
                files.append({
                    "full_path": full_path,
                    "name": name,
                    "size": size,
                    "modified": raw.get("modified") or raw.get("updated_at"),
                })
                if len(files) > 200_000:
                    raise EngineExecutionError("递归清单文件过多，已停止")
        files.sort(key=lambda row: str(row["full_path"]).casefold())
        return files

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
            _cancellation_checkpoint()
            ensure(path)
            return
        mkdir = getattr(self.alist, "mkdir", None)
        if not callable(mkdir):
            raise EngineExecutionError("AList 客户端缺少目录创建接口")
        current = "/"
        for segment in path.strip("/").split("/"):
            current = posixpath.join(current, segment)
            _cancellation_checkpoint()
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
                    _cancellation_checkpoint()
                    rename(full_path, new_name)
                except (EnginePauseRequested, EngineCancellationRequested):
                    raise
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
                _cancellation_checkpoint()
                rename_with_visibility_retry(posixpath.join(source_dir, original), final)
            return
        _cancellation_checkpoint()
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
                _cancellation_checkpoint()
                move(source_dir, target_dir, [original])
                move_submitted = True
                if self._visible_exact(intermediate_path) is not None:
                    break
            except (EnginePauseRequested, EngineCancellationRequested):
                raise
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
            _cancellation_checkpoint()
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
        except (EnginePauseRequested, EngineCancellationRequested):
            raise
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

    def _require_executable_final_basenames(
        self,
        files: list[object],
    ) -> None:
        """Fence stale provider-incompatible names before the first move.

        Fresh plans are rejected by the Engine validator, but persisted plans
        may predate a provider basename policy.  A legacy final name is
        tolerated only when it is already the exact completed target and its
        source is absent; this permits readback/artifact recovery without
        renaming a previously accepted file.  Every other unsafe final name
        is rejected before this executor issues *any* move or rename.
        """
        unsafe_targets: list[str] = []
        for item in files:
            final = str(getattr(item, "final_name", "") or "")
            if is_provider_safe_basename(final):
                continue
            target_dir = str(getattr(item, "target_dir", "") or "")
            source_path = str(getattr(item, "source_path", "") or "")
            target_path = posixpath.join(target_dir, final)
            target = self._exact(target_path)
            source = self._exact(source_path)
            expected = getattr(item, "source_size", None)
            if target is not None and source is None:
                observed_size = int(target["size"])
                if (
                    expected is not None
                    and (
                        isinstance(expected, bool)
                        or not isinstance(expected, int)
                        or observed_size != expected
                    )
                ):
                    raise EngineExecutionError(
                        "已存在的旧目标文件大小无法证明与计划一致: "
                        f"{target_path}"
                    )
                continue
            unsafe_targets.append(target_path)
        if unsafe_targets:
            raise EngineExecutionError(
                "计划包含 AList 不兼容的目标文件名；已在写入前停止: "
                f"{unsafe_targets[0]}"
            )

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
            # The video was just found by an exact fresh listing.  A pre-write
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
        _cancellation_checkpoint()
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
                _cancellation_checkpoint()
                deleted = remove_empty(directory)
            except (EnginePauseRequested, EngineCancellationRequested):
                raise
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
        """Apply only the plan-owned residual cleanup after plan validation.

        Formal media moves and artifact writes are intentionally absent from
        this method.  It is the re-entrant final step; every row is
        revalidated from the persisted plan before the first remote delete.
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
                _cancellation_checkpoint()
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
        files = list(getattr(plan, "files", ()) or ())
        _require_problem_free_plan(plan, stage="计划执行")
        # Validate every cleanup row before the first media move. This avoids
        # a partial formal write followed by discovery that an old plan wanted
        # to delete a user-owned attachment.
        _require_cleanup_allowlist(plan, stage="计划执行")
        # Reject every known undersized video before the first move, so a
        # multi-file plan cannot partially write formal media and only then
        # discover a test fragment later in the same plan.
        for item in files:
            source_path = str(getattr(item, "source_path"))
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
        # This must precede the first move: an old persisted plan can contain
        # a bad name late in its list, and discovering it after earlier media
        # writes would recreate the move/rename split that recovery is trying
        # to close.
        self._require_executable_final_basenames(files)
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
                if source_dir != target_dir and original != final:
                    intermediate_path = posixpath.join(target_dir, original)
                    if self._exact(intermediate_path) is not None:
                        raise EngineExecutionError(
                            "目标和中间原文件同时存在，拒绝覆盖或静默清理: "
                            f"{target_path} / {intermediate_path}"
                        )
                moved.append({"source": source_path, "target": target_path, "status": "already_present", **observed})
                continue
            if source is None:
                intermediate_path = posixpath.join(target_dir, original)
                intermediate = (
                    self._exact(intermediate_path)
                    if source_dir != target_dir and original != final
                    else None
                )
                if intermediate is not None:
                    intermediate_size = int(intermediate["size"])
                    if expected is not None and intermediate_size != expected:
                        raise EngineExecutionError(
                            "中间原文件大小不匹配，拒绝自动 rename: "
                            f"{intermediate_path}"
                        )
                    _require_admissible_video_size(
                        item,
                        intermediate_size,
                        path=intermediate_path,
                        stage="中断 rename 回读",
                    )
                    self._move_file(target_dir, target_dir, original, final)
                    observed = self._check_size(
                        target_path,
                        expected if expected is not None else intermediate_size,
                    )
                    self._verify_source_absent(intermediate_path)
                    _require_admissible_video_size(
                        item, observed["size"], path=target_path, stage="正式库回读",
                    )
                    moved.append({
                        "source": source_path,
                        "target": target_path,
                        "status": "renamed_after_interrupted_move",
                        **observed,
                    })
                    continue
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
        # Every plan, including an internally owned replenishment child,
        # follows the normal artifact projection and the same no-overwrite
        # writer primitive.  The child request itself is constrained to its
        # fresh selected staging manifest; artifacts are not a parallel
        # provider lane.
        planned_nfos = getattr(
            __import__("engine.scraper", fromlist=["planned_nfos"]),
            "planned_nfos",
            None,
        )
        if not callable(planned_nfos):
            raise EngineExecutionError("Engine 缺少 NFO 目标投影")
        nfo_rows = planned_nfos(plan)
        for target, data in nfo_rows:
            _cancellation_checkpoint()
            if not isinstance(target, str) or not isinstance(data, (bytes, bytearray)):
                raise EngineExecutionError("Engine 生成的 NFO 结构无效")
            self._ensure_dir(posixpath.dirname(target) or "/")
            observed = self._preserve_or_upload_bytes(
                target, bytes(data), "application/xml",
            )
            artifacts.append({"target": target, "kind": "nfo", **observed})

        planned_artwork = getattr(
            __import__("engine.scraper", fromlist=["planned_artwork"]),
            "planned_artwork",
            None,
        )
        downloader = getattr(self.tmdb, "download_poster", None) if self.tmdb is not None else None
        if callable(planned_artwork):
            for target, image_path, role in planned_artwork(plan):
                _cancellation_checkpoint()
                # Existing library artwork is authoritative.  In particular,
                # an artifact-only repair must not require a live TMDB image
                # downloader just to prove that it will preserve the file.
                existing = self._exact(target)
                if existing is not None:
                    artifacts.append({
                        "target": target,
                        "kind": role,
                        "size": int(existing["size"]),
                        "status": "already_present",
                    })
                    continue
                if not callable(downloader):
                    raise EngineExecutionError("计划包含海报，但 TMDB 客户端没有 download_poster")
                _cancellation_checkpoint()
                data = downloader(image_path)
                if not isinstance(data, (bytes, bytearray)):
                    raise EngineExecutionError(f"TMDB 海报响应无效: {image_path}")
                self._ensure_dir(posixpath.dirname(target) or "/")
                observed = self._preserve_or_upload_bytes(
                    target, bytes(data), "image/jpeg",
                )
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

        This path is used only while the one local writer owns the formal-write
        lock.  No worker can concurrently begin this queued/planned revision
        without first acquiring that lock; the marker makes a just-released
        next worker consumes cancellation before it can advance the job.
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

    def cancellation_pending(self, job_id: str) -> bool:
        """Whether one durable cancellation now fences this job's next effect.

        This is deliberately a read-only predicate so the root worker can use
        it as its existing pause callback while a child writer or provider
        operation is still active.  The worker consumes the marker only after
        it reaches that safe boundary.
        """
        job = self._read(job_id)
        if job.phase == "cancelled":
            return True
        request = self._read_cancel_request(job.id)
        return (
            isinstance(request, Mapping)
            and self._cancel_request_matches(job, request)
        )

    def consume_cancellation(self, job_id: str) -> EngineJob | None:
        """Commit a matching cancellation after a caller reaches a boundary."""
        job = self._read(job_id)
        if job.phase == "cancelled":
            return job
        return self._consume_cancel_request(job)

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
        pause_requested: Callable[[], bool] | None = None,
    ) -> Mapping[str, object]:
        """Install one subtitle member under the single formal write lock."""
        with self.worker_lock():
            effective_pause = (
                pause_requested
                if pause_requested is not None
                else self._pause_requested
            )
            _pause_checkpoint(effective_pause)
            pause_token = _PAUSE_REQUEST_CHECK.set(effective_pause)
            installer = getattr(self.executor, "install_subtitle_sidecar", None)
            try:
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
                    # language keyword.  Do not hide a real write TypeError;
                    # only retry when the signature itself rejected that
                    # keyword.  A scoped invocation never retries unguarded.
                    if (
                        effective_pause is not None
                        or subtitle_language is None
                        or "subtitle_language" not in str(exc)
                    ):
                        raise
                    kwargs.pop("subtitle_language", None)
                    kwargs.pop("subtitle_validator", None)
                    return dict(installer(source_path, target_path, **kwargs))
            finally:
                _PAUSE_REQUEST_CHECK.reset(pause_token)

    @staticmethod
    def _validated_replacement_manifest(manifest: object) -> ReplacementManifest:
        """Re-parse replacement evidence at the formal-write boundary.

        The HTTP composition layer persists a server-derived manifest before
        it asks the runner to archive anything.  The runner must nevertheless
        treat that object as untrusted: a forged frozen dataclass must not be
        able to turn an arbitrary formal-library path into an archive source.
        Re-parsing the public projection also keeps this narrow primitive
        independent from a second replacement-specific state store.
        """
        if not isinstance(manifest, ReplacementManifest):
            raise EngineRequestError("replacement archiving requires a ReplacementManifest")
        try:
            return ReplacementManifest.from_dict(manifest.as_dict())
        except ReplacementValidationError as exc:
            raise EngineRequestError("replacement manifest validation failed") from exc

    @staticmethod
    def _replacement_records(
        manifest: ReplacementManifest,
    ) -> list[dict[str, object]]:
        """Project exactly the old formal objects allowed to be archived.

        A replacement is never allowed to archive a work directory, NFO,
        artwork, an unselected subtitle, or a broadly matched media file.  A
        record exists only for the old video and the one old managed subtitle
        explicitly carried by each manifest item.
        """
        records: list[dict[str, object]] = []
        path_keys: set[str] = set()
        for item in manifest.items:
            rows: list[tuple[str, str, int, str, int]] = [
                (
                    "video",
                    item.target_path,
                    item.target_size,
                    item.source_path,
                    item.source_size,
                ),
            ]
            if item.subtitle_target_path is not None:
                if item.subtitle_source_path is None:
                    raise EngineRequestError(
                        "replacement subtitle target has no selected source"
                    )
                if (
                    isinstance(item.subtitle_target_size, bool)
                    or not isinstance(item.subtitle_target_size, int)
                    or item.subtitle_target_size <= 0
                    or isinstance(item.subtitle_source_size, bool)
                    or not isinstance(item.subtitle_source_size, int)
                    or item.subtitle_source_size <= 0
                ):
                    raise EngineRequestError("replacement subtitle size is invalid")
                rows.append((
                    "subtitle",
                    item.subtitle_target_path,
                    item.subtitle_target_size,
                    item.subtitle_source_path,
                    item.subtitle_source_size,
                ))
            for kind, old_target, old_size, new_source, new_size in rows:
                archive_path = archive_path_for(
                    manifest,
                    item,
                    subtitle=kind == "subtitle",
                )
                for path, label in (
                    (old_target, "old target"),
                    (new_source, "replacement source"),
                    (archive_path, "replacement archive"),
                ):
                    normalized = _safe_remote_path(
                        path,
                        field=label,
                        allow_root=False,
                    )
                    key = unicodedata.normalize("NFC", normalized).casefold()
                    if label != "replacement source" and key in path_keys:
                        raise EngineRequestError(
                            "replacement archive contains colliding old targets"
                        )
                    if label != "replacement source":
                        path_keys.add(key)
                if kind == "video":
                    if not is_video_filename(old_target) or not is_video_filename(new_source):
                        raise EngineRequestError("replacement video path has an invalid media type")
                elif not (
                    is_subtitle_filename(old_target)
                    and is_subtitle_filename(new_source)
                ):
                    raise EngineRequestError("replacement subtitle path has an invalid media type")
                records.append({
                    "coordinate": item.target_coordinate,
                    "kind": kind,
                    "old_target_path": old_target,
                    "old_target_size": old_size,
                    "new_source_path": new_source,
                    "new_source_size": new_size,
                    "archive_path": archive_path,
                })
        if not records:
            raise EngineRequestError("replacement manifest has no archiveable target")
        return records

    @staticmethod
    def _replacement_regular_file_state(
        executor: "SimplePlanExecutor",
        path: str,
        expected_size: int,
        *,
        label: str,
    ) -> Mapping[str, object] | None:
        """Return one fresh, exact regular-file observation or ``None``.

        ``exact_file_info`` alone does not prove that a path is not a
        directory or link on every AList backend.  Pair its byte observation
        with the writer's fresh parent/name classification, then reject every
        non-file state rather than allowing a broad directory move.
        """
        observed_kind = executor._remote_entry_kind(path)
        if observed_kind == "missing":
            return None
        if observed_kind != "file":
            raise EngineJobConflictError(
                f"replacement {label} is not one exact regular file: {path}"
            )
        observed = executor._visible_exact(path)
        if observed is None:
            raise EngineExecutionError(
                f"replacement {label} is not visible after a fresh listing: {path}"
            )
        raw_size = observed.get("size") if isinstance(observed, Mapping) else None
        if isinstance(raw_size, bool) or not isinstance(raw_size, int) or raw_size < 0:
            raise EngineExecutionError(
                f"replacement {label} has no valid byte size: {path}"
            )
        if raw_size != expected_size:
            raise EngineJobConflictError(
                "replacement exact size changed: "
                f"{path}; expected={expected_size}; actual={raw_size}"
            )
        return {"size": raw_size}

    @staticmethod
    def _replacement_existing_path_kind(
        executor: "SimplePlanExecutor",
        path: str,
        *,
        label: str,
    ) -> str:
        """Require one residual source path to remain a safe owned object."""
        observed_kind = executor._remote_entry_kind(path)
        if observed_kind not in {"file", "directory"}:
            raise EngineJobConflictError(
                f"replacement residual {label} changed or is unsafe: {path}"
            )
        if observed_kind == "file" and executor._visible_exact(path) is None:
            raise EngineExecutionError(
                f"replacement residual {label} is not visible after a fresh listing: {path}"
            )
        return observed_kind

    def archive_replacement_targets(
        self,
        manifest: ReplacementManifest,
        *,
        pause_requested: Callable[[], bool] | None = None,
    ) -> Mapping[str, object]:
        """Archive only the exact old targets declared by a replacement proof.

        This is intentionally not a new writer.  It takes the existing
        process-wide formal-writer lock and delegates every move, size
        readback and source-absence proof to ``SimplePlanExecutor``.  The
        operation is idempotent after an interruption: for each declared
        object the only admissible preflight states are ``old target present,
        archive absent`` or ``old target absent, archive present``.  Every
        other matrix is a conflict and no move is issued.

        The caller remains responsible for persisting manifest lifecycle
        transitions and for handing the selected source back through the
        ordinary Planner/Writer.  This narrow boundary deliberately performs
        neither a replacement plan nor a direct server-side AList write.
        """
        checked = self._validated_replacement_manifest(manifest)
        if checked.state in {"writing", "completed"}:
            raise EngineJobConflictError(
                "replacement archive cannot run after formal replacement writing began"
            )
        if checked.library_root != self.library_root:
            raise EngineJobConflictError(
                "replacement manifest library_root differs from the configured writer root"
            )
        executor = self.executor
        if not isinstance(executor, SimplePlanExecutor) or executor.alist is not self.alist:
            raise EngineExecutionError(
                "replacement archiving requires the configured SimplePlanExecutor writer"
            )
        records = self._replacement_records(checked)
        effective_pause = (
            pause_requested
            if pause_requested is not None
            else self._pause_requested
        )

        with self.worker_lock():
            _pause_checkpoint(effective_pause)
            self._ensure_authenticated(self.alist)
            pause_token = _PAUSE_REQUEST_CHECK.set(effective_pause)
            try:
                root_job = self._read(checked.root_job_id)
                if root_job.summary.get("internal_child") is True:
                    raise EngineJobConflictError(
                        "replacement manifest cannot be attached to an internal child"
                    )
                if self._job_ingress_source(root_job) != checked.source_root:
                    raise EngineJobConflictError(
                        "replacement source root differs from the owning RootJob ingress"
                    )
                source_root_kind = executor._remote_entry_kind(checked.source_root)
                if source_root_kind != "directory":
                    raise EngineJobConflictError(
                        "replacement source root is missing or not a directory"
                    )
                source_files = executor._fresh_tree_files(checked.source_root)
                declared_source_paths = {
                    str(record["new_source_path"])
                    for record in records
                }
                declared_source_paths.update(checked.residual_paths)
                unexpected_source_paths = sorted(
                    str(row["full_path"])
                    for row in source_files
                    if str(row["full_path"]) not in declared_source_paths
                )
                if unexpected_source_paths:
                    raise EngineJobConflictError(
                        "replacement source listing contains an undeclared object: "
                        f"{unexpected_source_paths[0]}"
                    )
                # A residual has no execution authority, but it is part of
                # the durable source ownership snapshot.  Its disappearance
                # is source drift, not permission to archive an old episode.
                for residual_path in checked.residual_paths:
                    self._replacement_existing_path_kind(
                        executor,
                        residual_path,
                        label="source object",
                    )
                for record in records:
                    self._replacement_regular_file_state(
                        executor,
                        str(record["new_source_path"]),
                        int(record["new_source_size"]),
                        label="new source",
                    )

                archive_root_kind = executor._remote_entry_kind(checked.archive_root)
                if archive_root_kind not in {"missing", "directory"}:
                    raise EngineJobConflictError(
                        "replacement archive root is already occupied by a non-directory"
                    )
                expected_archive = {
                    str(record["archive_path"]): int(record["old_target_size"])
                    for record in records
                }
                if archive_root_kind == "directory":
                    archive_files = executor._fresh_tree_files(checked.archive_root)
                    unexpected_archive_paths = sorted(
                        str(row["full_path"])
                        for row in archive_files
                        if str(row["full_path"]) not in expected_archive
                    )
                    if unexpected_archive_paths:
                        raise EngineJobConflictError(
                            "replacement archive contains an undeclared object: "
                            f"{unexpected_archive_paths[0]}"
                        )

                pending: list[dict[str, object]] = []
                result_rows: list[dict[str, object]] = []
                # Preflight every item before the first write.  A pre-existing
                # collision therefore cannot leave earlier episodes partially
                # archived merely because it was discovered late in the run.
                for record in records:
                    old_target = str(record["old_target_path"])
                    archive_path = str(record["archive_path"])
                    expected_size = int(record["old_target_size"])
                    old_state = self._replacement_regular_file_state(
                        executor,
                        old_target,
                        expected_size,
                        label="old target",
                    )
                    archive_state = self._replacement_regular_file_state(
                        executor,
                        archive_path,
                        expected_size,
                        label="archive target",
                    )
                    if old_state is not None and archive_state is not None:
                        raise EngineJobConflictError(
                            "replacement old target and archive are both present: "
                            f"{old_target}"
                        )
                    if old_state is None and archive_state is None:
                        raise EngineJobConflictError(
                            "replacement old target disappeared before archival: "
                            f"{old_target}"
                        )
                    row = {
                        "coordinate": str(record["coordinate"]),
                        "kind": str(record["kind"]),
                        "old_target_path": old_target,
                        "archive_path": archive_path,
                        "size": expected_size,
                    }
                    if old_state is None:
                        result_rows.append({**row, "status": "already_archived"})
                    else:
                        pending.append(record)
                        result_rows.append({**row, "status": "moved"})

                for record in pending:
                    _pause_checkpoint(effective_pause)
                    old_target = str(record["old_target_path"])
                    archive_path = str(record["archive_path"])
                    expected_size = int(record["old_target_size"])
                    # Re-check immediately before the external move.  The
                    # single writer lock prevents ScrapeFlow races; this also
                    # fences manual/provider drift between the broad
                    # all-items preflight and the individual operation.
                    if self._replacement_regular_file_state(
                        executor,
                        old_target,
                        expected_size,
                        label="old target",
                    ) is None:
                        raise EngineJobConflictError(
                            f"replacement old target disappeared before move: {old_target}"
                        )
                    if self._replacement_regular_file_state(
                        executor,
                        archive_path,
                        expected_size,
                        label="archive target",
                    ) is not None:
                        raise EngineJobConflictError(
                            f"replacement archive target appeared before move: {archive_path}"
                        )
                    executor._move_file(
                        posixpath.dirname(old_target) or "/",
                        posixpath.dirname(archive_path) or "/",
                        posixpath.basename(old_target),
                        posixpath.basename(archive_path),
                    )
                    if executor._remote_entry_kind(archive_path) != "file":
                        raise EngineExecutionError(
                            f"replacement archive target is not a regular file: {archive_path}"
                        )
                    executor._check_size(archive_path, expected_size)
                    executor._verify_source_absent(old_target)
                    if executor._remote_entry_kind(old_target) != "missing":
                        raise EngineExecutionError(
                            f"replacement old target remains visible after archive: {old_target}"
                        )

                # The task-specific archive is allowed to contain only these
                # exact old objects.  This final fresh recursive listing is
                # the durable hand-off proof for the ordinary replacement
                # Planner/Writer stage.
                final_archive_files = executor._fresh_tree_files(checked.archive_root)
                final_archive = {
                    str(row["full_path"]): int(row["size"])
                    for row in final_archive_files
                }
                if final_archive != expected_archive:
                    raise EngineExecutionError(
                        "replacement archive fresh readback does not match the manifest"
                    )
                for record in records:
                    old_target = str(record["old_target_path"])
                    archive_path = str(record["archive_path"])
                    expected_size = int(record["old_target_size"])
                    if self._replacement_regular_file_state(
                        executor,
                        old_target,
                        expected_size,
                        label="old target",
                    ) is not None:
                        raise EngineExecutionError(
                            f"replacement old target remains visible after archive: {old_target}"
                        )
                    if self._replacement_regular_file_state(
                        executor,
                        archive_path,
                        expected_size,
                        label="archive target",
                    ) is None:
                        raise EngineExecutionError(
                            f"replacement archive target disappeared after readback: {archive_path}"
                        )
                return {
                    "status": "archived",
                    "manifest_id": checked.manifest_id,
                    "root_job_id": checked.root_job_id,
                    "work_unit_id": checked.work_unit_id,
                    "archive_root": checked.archive_root,
                    "objects": result_rows,
                }
            finally:
                _PAUSE_REQUEST_CHECK.reset(pause_token)

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
        a source that still belongs to the task lifecycle.
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
                "source_root": source,
                "ingress_source_path": source,
                "mode": "auto",
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
            intake_source_id,
            load_intake_catalog,
            save_intake_catalog,
            upsert_intake_source,
        )
        # The S-step request must name the same canonical direct child that
        # produced ``source_id``.  Accepting a mismatched pair would let a
        # caller attach a RootJob to the wrong IntakeSource and is especially
        # dangerous when a stale binding is present.
        canonical_source_id = intake_source_id(source_path)
        if source_id != canonical_source_id:
            raise EngineJobConflictError(
                "IntakeSource source_id 与来源路径不一致，拒绝创建根任务"
            )
        with self.worker_lock():
            catalog = load_intake_catalog(self.state_root)
            intake = find_by_source_id(catalog, source_id)
            if intake is not None and intake.root_task_id is not None:
                try:
                    existing_job = self._read(intake.root_task_id)
                except (SimpleEngineError, FileNotFoundError):
                    # A missing job record is an orphaned lifecycle, not an
                    # invitation to create a replacement.  Replacing it here
                    # used to leave an unbound JSON job when bind_root_task()
                    # rejected the old catalog association, and could also
                    # revive deleted formal-library ownership.  Stop
                    # fail-closed; a separate audited repair operation must
                    # reconcile or retire the orphan before S can proceed.
                    raise EngineJobConflictError(
                        "IntakeSource 仍绑定已不存在的 RootJob；需先完成孤儿任务核对"
                    )
                # A durable binding is also an ownership claim.  Even if the
                # caller supplied the right source_id, never return a job
                # whose ingress path points at a different source.
                existing_source = self._job_ingress_source(existing_job)
                if existing_source != source_path:
                    raise EngineJobConflictError(
                        "IntakeSource 已绑定到不同来源路径，拒绝复用根任务"
                    )
                return existing_job

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
            except (ValueError, OSError) as exc:
                # Do not leave a freshly-created unbound job behind if the
                # durable source claim changed or could not be persisted.
                # The lock still protects this local cleanup; no media or
                # formal-library path is touched.
                try:
                    self._job_path(job.id).unlink()
                except FileNotFoundError:
                    pass
                raise EngineJobConflictError(
                    "IntakeSource 根任务绑定未能原子保存，已拒绝创建"
                ) from exc

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
            if job.summary.get("internal_child") is True:
                raise EngineJobConflictError("内部任务不能通过用户目标货架启动")
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
            # 存量清退：``automatic_stage`` 是 legacy 状态镜像，phase 是唯一权威。
            # 执行链路接管记录时丢弃旧记录残留的镜像值，且不再续写。
            summary.pop("automatic_stage", None)
            summary.update({
                "automatic": True,
                "source_root": source,
                "ingress_source_path": source,
                "target_shelf": selected.value,
                "selected_target_root": selected_root,
            })
            # Drop stale projections from retired compatibility flows. The
            # RootJob pipeline owns identity and D-step reconciliation in its
            # own ledgers, never in EngineJob.summary.
            summary.pop("identity", None)
            summary.pop("resource_gaps", None)
            summary.pop("waiting_source_state", None)
            summary.pop("reconciliation", None)
            summary.pop("reconciliation_outcome", None)
            summary.pop("reconciliation_identity_confirmation", None)
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

    def _validate_cleanup_root(self, root: EngineJob) -> list[EngineJob]:
        """Return terminal children after proving the root is safe to forget."""
        if self._is_internal_job(root):
            raise EngineExecutionError("只允许清理根任务，内部 child 不能单独清理")
        if root.phase not in _CLEANUP_TERMINAL_PHASES:
            raise EngineWorkerBusyError(f"任务仍在运行，不能清理记录: {root.phase}")
        # A historical Engine record may still say ``completed`` even though
        # its RootJob J ledger contains open coordinates.  Never let local
        # cleanup erase that durable replenishment evidence before N closes
        # every Gap; public projections also expose this as gaps_pending.
        if root.phase == "completed":
            open_gaps = [
                gap for gap in load_gap_ledger(self.state_root, root.id)
                if gap.status == "open"
            ]
            if open_gaps:
                raise EngineWorkerBusyError(
                    f"根任务仍有 {len(open_gaps)} 个开放缺口，不能清理记录"
                )
        summary = root.summary if isinstance(root.summary, Mapping) else {}
        replenishment = summary.get("replenishment")
        if isinstance(replenishment, Mapping):
            status = str(replenishment.get("status") or "").casefold()
            if status in _CLEANUP_ACTIVE_PROVIDER_STATUSES or (
                status
                and replenishment.get("terminal") is not True
                and status not in {"completed", "resolved", "ready"}
            ):
                raise EngineWorkerBusyError("任务仍有未终结的补源工作，不能清理记录")
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

            # An IntakeSource binding is the durable identity of a RootJob.
            # Keep a small, local tombstone before removing its JSON record so
            # cleanup can never leave an unauditable dangling binding again.
            # The tombstone is identity evidence only: reopening still has to
            # prove paused/quiescent state and the absence of every F+ side
            # effect before rebuilding a fresh B/W generation.
            tombstone_path: Path | None = None
            try:
                from engine.scrapeflow.intake_source import load_intake_catalog

                bindings = [
                    item for item in load_intake_catalog(self.state_root)
                    if item.root_task_id == safe_id
                ]
            except Exception as exc:
                raise EngineExecutionError(
                    "IntakeSource 绑定无法核验，拒绝清理根任务"
                ) from exc
            if len(bindings) > 1:
                raise EngineExecutionError("RootJob 被多个 IntakeSource 绑定，拒绝清理")
            if bindings:
                binding = bindings[0]
                source = self._job_ingress_source(root)
                if binding.canonical_path != source:
                    raise EngineExecutionError("RootJob 来源与 IntakeSource 绑定不一致")
                tombstone_root = self.state_root / "root-job-tombstones"
                if tombstone_root.is_symlink():
                    raise EngineExecutionError("RootJob tombstone 根目录不允许符号链接")
                tombstone_path = tombstone_root / f"{safe_id}.json"
                atomic_write_json(
                    tombstone_path,
                    {
                        "schema_version": 1,
                        "job_id": safe_id,
                        "source_id": binding.source_id,
                        "source_path": source,
                        "target_shelf": root.target_shelf,
                        "target_root": root.target_root,
                        "created_at": root.created_at,
                        "selected_at": root.selected_at,
                        "cleaned_phase": root.phase,
                        "cleaned_at": _now(),
                    },
                    allow_nan=False,
                )

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
                "identity_tombstone": str(tombstone_path) if tombstone_path else None,
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
        allow_target_root: bool = False,
    ) -> None:
        """Keep every formal target inside one approved target subtree.

        ``target_root`` is normally the selected first-level shelf, but the
        same primitive also gates a nested WorkUnit or a D-locked existing
        work root.  A persisted plan or an injected planner is still
        untrusted at the single-writer boundary, so validate the concrete work
        root and every target file before the plan can be stored or replayed.
        """
        allowed_root = _safe_remote_path(
            target_root,
            field=f"{stage} target_scope_root",
            allow_root=False,
        )
        prefix = allowed_root + "/"

        def require_within(
            value: object,
            *,
            field: str,
            allow_exact_root: bool = False,
        ) -> str:
            path = _safe_remote_path(value, field=f"{stage} {field}", allow_root=False)
            if path == allowed_root and allow_exact_root:
                return path
            if not path.startswith(prefix):
                raise EngineRequestError(
                    f"{stage}拒绝目标货架外或允许目标范围外的路径: {path}"
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

        require_within(
            getattr(plan, "target_root", None),
            field="target_work_path",
            allow_exact_root=allow_target_root,
        )
        metadata = getattr(plan, "metadata", None)
        if isinstance(metadata, Mapping):
            series_root = metadata.get("series_root")
            if series_root is not None:
                require_within(
                    series_root,
                    field="metadata.series_root",
                    allow_exact_root=allow_target_root,
                )
            container_root = metadata.get("container_root")
            if container_root is not None:
                container_path = require_within(
                    container_root,
                    field="metadata.container_root",
                    allow_exact_root=allow_target_root,
                )
                plan_root = _safe_remote_path(
                    getattr(plan, "target_root", None),
                    field=f"{stage} target_work_path",
                    allow_root=False,
                )
                if not (
                    container_path == plan_root
                    or container_path.startswith(plan_root + "/")
                ):
                    raise EngineRequestError(
                        f"{stage}容器元数据根目录必须属于计划目标根: {container_path}"
                    )
                container_title = metadata.get("container_title")
                if not isinstance(container_title, str) or not container_title.strip():
                    raise EngineRequestError(
                        f"{stage}容器元数据缺少有效 container_title"
                    )
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
                # A merge plan writes new season files directly into its
                # D-locked work root.  The filenames themselves remain strict
                # descendants and are checked below.
                allow_exact_root=allow_target_root,
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

    def _require_plan_work_unit_target_scope(
        self,
        plan: object,
        request: EngineRequest,
        *,
        stage: str,
    ) -> None:
        """Gate a WorkUnit plan to its durable, internal target subtree.

        RootJob shelf selection is an authorization for *new* work, while a
        D result may instead lock a sibling under an existing work root on a
        different shelf.  ``target_scope_root`` records that exact authorized
        subtree in the internal carrier so plan, execute, and restart recovery
        share one containment rule.
        """
        raw_scope = request.target_scope_root
        if raw_scope is None:
            return
        scope = _safe_remote_path(
            raw_scope,
            field=f"{stage} WorkUnit target_scope_root",
            allow_root=False,
        )
        parent = _safe_remote_path(
            request.parent_path,
            field=f"{stage} WorkUnit parent_path",
            allow_root=False,
        )
        if not (scope == parent or scope.startswith(parent + "/")):
            raise EngineRequestError(
                f"{stage} WorkUnit 目标范围不属于 Planner 父目录"
            )
        formal_shelves = tuple(
            target_root_for_shelf(self.library_root, shelf)
            for shelf in ("movie", "anime", "us_tv")
        )
        scope_shelf = next(
            (
                shelf
                for shelf in formal_shelves
                if scope == shelf or scope.startswith(shelf + "/")
            ),
            None,
        )
        if scope_shelf is None:
            raise EngineRequestError(
                f"{stage} WorkUnit 目标范围不在正式库货架内"
            )
        self._require_plan_target_shelf_containment(
            plan,
            target_root=scope,
            stage=stage,
            # Only E3 supplies a narrower D-locked work root below both its
            # planner parent and its formal shelf and may therefore plan *at*
            # that root. A malformed D record must never turn a whole shelf
            # into an exact-target allowance. A normal new WorkUnit has
            # scope == parent (shelf/container/main root) and must still
            # create a distinct work child. File/artifact paths remain strict
            # descendants in both cases.
            allow_target_root=(scope != parent and scope != scope_shelf),
        )

    def _require_persisted_plan_work_unit_target_scope(
        self,
        job: EngineJob,
        plan: object,
        *,
        stage: str,
    ) -> None:
        """Reapply a persisted WorkUnit target boundary before any replay."""
        request = EngineRequest.from_persisted_mapping(job.request)
        self._require_plan_work_unit_target_scope(plan, request, stage=stage)

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
            if (
                job.summary.get("automatic") is True
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
        pause_requested: Callable[[], bool] | None = None,
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
        effective_pause = (
            pause_requested
            if pause_requested is not None
            else self._pause_requested
        )
        _pause_checkpoint(effective_pause)
        # Archive inspection may perform the first remote listing/download;
        # authenticate through the same guarded port before invoking it.  A
        # raw ``self.alist.login`` here would leave a root-scope race between
        # the checkpoint and the authentication request.
        guarded_alist = _PauseCheckedArchivePort(self.alist, effective_pause)
        self._ensure_authenticated(guarded_alist)
        _pause_checkpoint(effective_pause)
        local_staging, remote_staging = self._archive_task_roots(job_id)
        kwargs = {
            "alist": guarded_alist,
            "task_staging": local_staging,
            "remote_staging_root": remote_staging,
            "pause_requested": effective_pause,
        }
        if retry_password is not None:
            kwargs["retry_password"] = retry_password
        try:
            try:
                prepared = method(asdict(request), **kwargs)
            except TypeError:
                try:
                    # Small migration/test adapters may accept ``alist`` but
                    # not concrete staging/pause kwargs.  The guarded port
                    # still fences every remote call they make.
                    _pause_checkpoint(effective_pause)
                    prepared = method(asdict(request), alist=guarded_alist)
                except TypeError:
                    # A legacy adapter with no port contract cannot expose
                    # per-operation hooks.  A scoped automatic root cannot
                    # safely invoke it because its remote operations would be
                    # invisible to the RootJob fence.
                    if pause_requested is not None:
                        raise EngineRequestError(
                            "归档预处理器不支持受控 AList port，拒绝在 RootJob 试运行范围执行"
                        )
                    _pause_checkpoint(effective_pause)
                    prepared = method(asdict(request))
        except ArchivePauseRequested as exc:
            raise EnginePauseRequested("归档预处理已响应暂停请求") from exc
        _pause_checkpoint(effective_pause)
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
        self,
        request: EngineRequest,
        *,
        job_id: str | None = None,
        pause_requested: Callable[[], bool] | None = None,
    ) -> EngineRequest:
        """Compatibility wrapper for focused callers of the old private hook."""
        if job_id is None:
            job_id = f"preprocess-{uuid.uuid4().hex}"
        result, _projection = self._preprocess_ordinary_request_details(
            request, job_id=job_id, pause_requested=pause_requested,
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

    def _validated_source_scope_paths(self, request: EngineRequest) -> tuple[str, ...]:
        """Validate the internal-only exact source ownership boundary."""
        if not request.source_scope_paths:
            if request.source_files is not None:
                raise EngineRequestError("来源清单缺少对应的来源范围")
            return ()
        if request.source_files is None:
            raise EngineRequestError("来源范围缺少 fresh 文件清单")
        source_root = _safe_remote_path(
            request.source_path, field="source_path", allow_root=False,
        )
        scopes: list[str] = []
        for raw in request.source_scope_paths:
            scope = _safe_remote_path(raw, field="source_scope_path", allow_root=False)
            if not (scope == source_root or scope.startswith(source_root + "/")):
                raise EngineRequestError("来源范围不属于 Engine 来源根")
            if scope in scopes:
                raise EngineRequestError("来源范围包含重复路径")
            scopes.append(scope)
        for index, left in enumerate(scopes):
            for right in scopes[index + 1:]:
                if left.startswith(right + "/") or right.startswith(left + "/"):
                    raise EngineRequestError("来源范围不得互相嵌套")
        return tuple(scopes)

    @staticmethod
    def _path_in_exactly_one_scope(path: str, scopes: tuple[str, ...]) -> bool:
        matches = [
            scope for scope in scopes
            if path == scope or path.startswith(scope + "/")
        ]
        return len(matches) == 1

    def _validated_scoped_source_files(
        self,
        request: EngineRequest,
    ) -> tuple[tuple[str, ...], list[dict[str, object]]] | None:
        """Validate the fresh manifest that a multi-path WorkUnit hands over."""
        scopes = self._validated_source_scope_paths(request)
        if not scopes:
            return None
        if request.media_type not in {"tv", "movie", "collection"}:
            raise EngineRequestError(
                "多来源范围仅支持已确认的 TV、电影或合集 WorkUnit"
            )
        files: list[dict[str, object]] = []
        seen: set[str] = set()
        for raw in request.source_files or ():
            if not isinstance(raw, Mapping):
                raise EngineRequestError("来源范围清单包含无效条目")
            item = dict(raw)
            if item.get("is_dir") is True:
                raise EngineRequestError("来源范围清单不得包含目录条目")
            path = _safe_remote_path(
                item.get("full_path"), field="来源范围文件路径", allow_root=False,
            )
            if not self._path_in_exactly_one_scope(path, scopes):
                raise EngineRequestError("来源范围清单包含范围外文件")
            if path in seen:
                raise EngineRequestError("来源范围清单包含重复文件")
            seen.add(path)
            item["full_path"] = path
            files.append(item)
        return scopes, files

    def _require_plan_source_scope_containment(
        self,
        plan: object,
        request: EngineRequest,
        *,
        stage: str,
    ) -> None:
        """Require every plan source reference to stay inside its WorkUnit."""
        scoped = self._validated_scoped_source_files(request)
        if scoped is None:
            return
        scopes, _files = scoped
        plan_root = _safe_remote_path(
            str(getattr(plan, "source_root", "")),
            field=f"{stage} source_root",
            allow_root=False,
        )
        if plan_root != request.source_path:
            raise EngineRequestError(f"{stage} 的 source_root 未保持 WorkUnit 公共来源根")
        # A flat movie WorkUnit pins one exact file while the legacy planner
        # still carries its parent directory as ``source_root``/``source_dir``.
        # Permit that single, verifiable parent relation only for the exact
        # file scope; never let a sibling file or directory inherit the scope.
        file_scopes = {
            scope
            for scope in scopes
            if any(
                isinstance(raw, Mapping)
                and raw.get("is_dir") is not True
                and str(raw.get("full_path") or "").rstrip("/") == scope
                for raw in (_files or ())
            )
        }
        for collection_name in ("files", "cleanup_files", "problem_files"):
            for item in list(getattr(plan, collection_name, ()) or ()):
                source = _safe_remote_path(
                    str(getattr(item, "source_path", "")),
                    field=f"{stage} {collection_name} source_path",
                    allow_root=False,
                )
                if not self._path_in_exactly_one_scope(source, scopes):
                    raise EngineRequestError(f"{stage} 计划包含范围外来源: {source}")
                source_dir = getattr(item, "source_dir", None)
                if source_dir is not None:
                    directory = _safe_remote_path(
                        str(source_dir),
                        field=f"{stage} {collection_name} source_dir",
                        allow_root=False,
                    )
                    directory_in_scope = self._path_in_exactly_one_scope(
                        directory, scopes,
                    )
                    exact_file_parent = any(
                        scope in file_scopes
                        and source == scope
                        and directory == (posixpath.dirname(scope) or "/")
                        for scope in file_scopes
                    )
                    if not directory_in_scope and not exact_file_parent:
                        raise EngineRequestError(f"{stage} 计划包含范围外来源目录: {directory}")

    def _require_persisted_plan_source_scope(
        self,
        job: EngineJob,
        plan: object,
        *,
        stage: str,
    ) -> None:
        request = EngineRequest.from_persisted_mapping(job.request)
        self._require_episode_map_path(request, stage=stage)
        self._require_plan_source_scope_containment(plan, request, stage=stage)

    def _require_episode_map_path(
        self,
        request: EngineRequest,
        *,
        stage: str,
    ) -> None:
        """Keep an internal episode-map replay file inside this state root."""
        raw = request.episode_map_path
        if raw is None:
            return
        try:
            state_root = self.state_root.resolve()
            candidate = Path(raw).resolve(strict=False)
            candidate.relative_to(state_root)
        except (OSError, ValueError) as exc:
            raise EngineRequestError(
                f"{stage} episode_map_path 不属于当前 state_root"
            ) from exc

    def _build_plan(self, request: EngineRequest) -> object:
        self._ensure_authenticated(self.alist)
        scoped = self._validated_scoped_source_files(request)
        source_files = scoped[1] if scoped is not None else None
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
                    source_files=source_files,
                )
            elif current.media_type == "tv":
                tv_kwargs: dict[str, object] = {
                    "auto_episode_mode": current.auto_episode_mode,
                    "alist": self.alist,
                    "tmdb_client": self.tmdb,
                    "src_path": current.source_path,
                    "parent_path": current.parent_path,
                    "tmdb_id": int(current.tmdb_id),
                    "season": current.season,
                    "absolute": current.absolute,
                    "prefer_simplified": current.prefer_simplified,
                    "allow_unmapped": current.allow_unmapped,
                    "ignore_orphan_temp": current.ignore_orphan_temp,
                    "episode_map_path": (
                        Path(current.episode_map_path)
                        if current.episode_map_path
                        else None
                    ),
                    "episode_group_id": current.episode_group_id,
                    "allow_release_dash_ordinal": current.allow_release_dash_ordinal,
                    "allow_release_title_ordinal": current.allow_release_title_ordinal,
                    "media_root": self.library_root,
                }
                if source_files is not None:
                    tv_kwargs["source_files"] = source_files
                if current.source_declared_seasons:
                    tv_kwargs["source_declared_seasons"] = current.source_declared_seasons
                plan = engine.build_tv_plan_smart(
                    **tv_kwargs,
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
                    kwargs: dict[str, object] = {"media_root": self.library_root}
                    validate(self.alist, plan, **kwargs)
                except TypeError as exc:
                    # Injected/legacy Engine validators may still expose the
                    # old two-argument contract.  Only signature-level
                    # incompatibility gets the compatibility call; an
                    # internal TypeError must not replay validation blindly.
                    if "media_root" not in str(exc):
                        raise
                    validate(self.alist, plan)
        self._require_plan_source_scope_containment(
            plan, request, stage="计划生成",
        )
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
        pause_requested: Callable[[], bool] | None = None,
    ) -> EngineJob:
        """Build and persist one plan.

        ``internal_child_of`` is written into the first durable JSON record
        when a provider creates a child.  That removes the small crash window
        in which a planned child existed but had not yet been marked hidden
        from the public root queue.
        """
        effective_pause = (
            pause_requested
            if pause_requested is not None
            else self._pause_requested
        )
        _pause_checkpoint(effective_pause)
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
        # A grouped WorkUnit uses the RootJob ingress merely as a common
        # planner root.  Letting archive preprocessing recurse that shared
        # root could inspect or stage an unclaimed sibling, so scoped requests
        # intentionally bypass that broad preprocessor.
        if (
            internal_child_of is None
            and not skip_archive_preprocessing
            and not request.source_scope_paths
        ):
            request, archive_projection = self._preprocess_ordinary_request_details(
                request,
                job_id=job_id,
                pause_requested=effective_pause,
            )
        _pause_checkpoint(effective_pause)
        plan = self._build_plan(request)
        _pause_checkpoint(effective_pause)
        if internal_child_of is not None:
            try:
                _require_internal_child_tv_primary_videos(plan, stage="计划生成")
            except ValueError as exc:
                raise EngineRequestError(str(exc)) from exc
        self._require_plan_work_unit_target_scope(
            plan,
            request,
            stage="计划生成",
        )
        if selected_root is not None:
            self._require_plan_target_shelf_containment(
                plan,
                target_root=selected_root,
                stage="计划生成",
            )
        engine = __import__("engine.scraper", fromlist=["plan_to_dict"])
        serializer = getattr(engine, "plan_to_dict", None)
        if not callable(serializer):
            raise SimpleEngineError("Engine 缺少 plan_to_dict")
        body = serializer(plan)
        if not isinstance(body, Mapping):
            raise SimpleEngineError("Engine 计划序列化结果无效")
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
        _pause_checkpoint(effective_pause)
        atomic_write_json(self._job_path(job_id), job.as_dict(), allow_nan=False)
        return job

    def plan_container_artifacts(
        self,
        *,
        root_job_id: str,
        source_path: str,
        target_root: str,
        target_shelf: object,
        container_title: str,
        poster_path: str | None = None,
        backdrop_path: str | None = None,
        representative_tmdb_id: int | None = None,
        job_id: str,
        pause_requested: Callable[[], bool] | None = None,
    ) -> EngineJob:
        """Persist one metadata-only carrier for a directory container.

        A series container is a real library item even though it has no TMDB
        identity of its own.  The carrier deliberately has no media or
        cleanup rows; ``execute_job`` therefore runs the same single writer
        and exact artifact readback while never moving a source file.  It is
        hidden as an internal child of the selected RootJob and uses a
        deterministic id so a retry cannot create a second artifact writer.
        """
        root_id = _safe_job_id(root_job_id)
        identifier = _safe_job_id(job_id)
        source = _safe_remote_path(
            source_path, field="container artifact source_path", allow_root=False,
        )
        target = _safe_remote_path(
            target_root, field="container artifact target_root", allow_root=False,
        )
        if not isinstance(container_title, str) or not container_title.strip():
            raise EngineRequestError("container_title 必须是非空字符串")
        if poster_path is not None and (
            not isinstance(poster_path, str) or not poster_path.strip()
        ):
            raise EngineRequestError("poster_path 必须是非空字符串或省略")
        if backdrop_path is not None and (
            not isinstance(backdrop_path, str) or not backdrop_path.strip()
        ):
            raise EngineRequestError("backdrop_path 必须是非空字符串或省略")
        if (
            representative_tmdb_id is not None
            and (
                isinstance(representative_tmdb_id, bool)
                or not isinstance(representative_tmdb_id, int)
                or representative_tmdb_id <= 0
            )
        ):
            raise EngineRequestError("representative_tmdb_id 必须是正整数或省略")
        effective_pause = (
            pause_requested
            if pause_requested is not None
            else self._pause_requested
        )
        with self.worker_lock():
            _pause_checkpoint(effective_pause)
            root_job = self._read(root_id)
            if root_job.summary.get("internal_child") is True:
                raise EngineJobConflictError("容器元数据 carrier 不能挂在内部任务下")
            if root_job.target_shelf is None:
                raise EngineRequestError("根任务尚未选择目标货架")
            selected, shelf_root = self._target_root_for_confirmed_shelf(
                target_shelf,
            )
            if root_job.target_shelf != selected.value or root_job.target_root != shelf_root:
                raise EngineJobConflictError("容器元数据货架与 RootJob 授权不一致")
            ingress = self._job_ingress_source(root_job)
            if source != ingress:
                raise EngineJobConflictError("容器元数据来源不是 RootJob 的入站来源")
            if not (target == shelf_root or target.startswith(shelf_root + "/")):
                raise EngineRequestError("容器元数据目标不属于 RootJob 货架")
            if self._job_path(identifier).exists():
                existing = self._read(identifier)
                summary = existing.summary if isinstance(existing.summary, Mapping) else {}
                if (
                    summary.get("container_artifacts") is not True
                    or summary.get("root_job_id") != root_id
                    or existing.plan.get("target_root") != target
                ):
                    raise EngineJobConflictError(
                        "同名容器元数据 carrier 的来源、目标或所有权不一致"
                    )
                return existing

            from engine.scrapeflow.models import Plan

            metadata: dict[str, object] = {
                "container_root": target,
                "container_title": container_title.strip(),
                "container_nfo_kind": "tvshow",
                "container_artifacts": True,
            }
            if poster_path is not None:
                metadata["container_poster_path"] = poster_path.strip()
            if backdrop_path is not None:
                metadata["container_backdrop_path"] = backdrop_path.strip()
            if representative_tmdb_id is not None:
                # Observability only; the root NFO intentionally does not
                # serialize this id because the container is not a work.
                metadata["representative_tmdb_id"] = representative_tmdb_id
            plan = Plan(
                mode="container",
                source_root=source,
                target_root=target,
                files=[],
                cleanup_files=[],
                problem_files=[],
                warnings=[],
                metadata=metadata,
                decision_trace={"source": "root_container_metadata"},
                scan_report={"container_artifacts": True},
            )
            engine = __import__("engine.scraper", fromlist=[
                "finalize_plan", "plan_to_dict", "validate_plan",
            ])
            finalize = getattr(engine, "finalize_plan", None)
            serializer = getattr(engine, "plan_to_dict", None)
            validate = getattr(engine, "validate_plan", None)
            if not callable(finalize) or not callable(serializer) or not callable(validate):
                raise SimpleEngineError("当前 Engine 缺少容器元数据计划接口")
            plan = finalize(plan)
            validate(self.alist, plan, media_root=self.library_root)
            self._require_plan_target_shelf_containment(
                plan,
                target_root=shelf_root,
                stage="容器元数据计划",
            )
            body = serializer(plan)
            if not isinstance(body, Mapping):
                raise SimpleEngineError("容器元数据计划序列化结果无效")
            request = EngineRequest.from_mapping({
                "source_path": source,
                "parent_path": shelf_root,
                "media_type": "tv",
                "target_shelf": selected.value,
                "tmdb_id": representative_tmdb_id or 1,
                "query": container_title.strip(),
                "season": 1,
            })
            now = _now()
            summary = self._summary(plan)
            summary.update({
                "internal_child": True,
                "root_job_id": root_id,
                "container_artifacts": True,
                "container_root": target,
                "container_title": container_title.strip(),
            })
            job = EngineJob(
                id=identifier,
                phase="planned",
                created_at=now,
                updated_at=now,
                request=asdict(request),
                plan=dict(body),
                summary=summary,
                target_shelf=selected.value,
                target_root=shelf_root,
                selected_at=now,
            )
            _pause_checkpoint(effective_pause)
            atomic_write_json(self._job_path(identifier), job.as_dict(), allow_nan=False)
            return job

    def mark_internal_child(self, job_id: str, *, root_job_id: str) -> EngineJob:
        """Associate one provider-created child with its visible root job.

        A replenishment child is an implementation detail: it may have its
        own persisted Engine plan so restart recovery can finish it safely,
        but it must never turn into a second user-facing task or a new source
        of provider work.  Store the relationship in the durable summary so
        the HTTP composition root can filter it without
        guessing from its staging path.
        """
        root_id = _safe_job_id(root_job_id)
        with self.worker_lock():
            job = self._read(job_id)
            try:
                _require_internal_child_tv_primary_videos(
                    self._plan_from_job(job), stage="内部 child 标记",
                )
            except ValueError as exc:
                raise EngineRequestError(str(exc)) from exc
            summary = dict(job.summary)
            summary["internal_child"] = True
            summary["root_job_id"] = root_id
            updated = replace(job, summary=summary, updated_at=_now())
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
            selected_shelf, selected_root = self._confirmed_target_selection(job)
            original_source = self._job_ingress_source(job)
            intake = EngineRequest.from_mapping({
                "source_path": original_source,
                "parent_path": selected_root,
                "media_type": "auto",
                "target_shelf": selected_shelf.value,
            })
            archiving_summary = self._with_active_operation(
                dict(job.summary),
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
                        intake,
                        job_id=job_id,
                        retry_password=retry_password,
                        pause_requested=effective_pause,
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
            # C/U step: resolve each pending WorkUnit independently.  An
            # ambiguous unit is parked in the ledger without blocking its
            # siblings; durable operator overrides are never re-asked.
            try:
                from engine.scrapeflow.unit_identity import (
                    resolve_work_unit_identities,
                )
                resolve_work_unit_identities(
                    self.tmdb,
                    self.state_root,
                    job_id,
                    prefer_animation=(selected_shelf.value == "anime"),
                )
            except Exception:
                pass  # Advisory in P4; P6 planning will consume the ledger.
            # D step: three-shelf reconciliation per confirmed WorkUnit.  The
            # index spans 电影/番剧/欧美剧, so an existing work in another shelf
            # is inherited instead of duplicated (contract rule D).
            try:
                from local.scrapeflow_api.library_index import (
                    reconcile_root_work_units,
                )
                from local.scrapeflow_api.tmdb_episode_catalog import (
                    TmdbEpisodeCatalog,
                )
                reconcile_root_work_units(
                    self.alist, self.library_root, self.state_root, job_id,
                    episode_catalog=TmdbEpisodeCatalog(self.tmdb),
                    tmdb_client=self.tmdb,
                )
            except Exception:
                pass  # Advisory in P5; P6 planning will consume the ledger.
            try:
                _pause_checkpoint(effective_pause)
            except EnginePauseRequested:
                return self._read(job_id)
            cancelled = self._consume_cancel_request(matching)
            if cancelled is not None:
                return cancelled
            correction = job.summary.get("manual_identity")
            if isinstance(correction, Mapping):
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
            except FormalTargetConflictError as exc:
                summary = dict(planning.summary)
                summary.update({
                    "identity": identity.as_dict(),
                    "automatic": True,
                    "target_shelf": selected_shelf.value,
                    "selected_target_root": selected_root,
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
            engine = __import__("engine.scraper", fromlist=["plan_to_dict"])
            serializer = getattr(engine, "plan_to_dict", None)
            if not callable(serializer):
                raise SimpleEngineError("Engine 缺少 plan_to_dict")
            body = serializer(plan)
            if not isinstance(body, Mapping):
                raise SimpleEngineError("Engine 计划序列化结果无效")
            summary = self._summary(plan)
            summary.update({
                "identity": identity.as_dict(),
                "automatic": True,
                "target_shelf": selected_shelf.value,
                "selected_target_root": selected_root,
                "resource_gaps": list(
                    (body.get("scan_report") or {}).get("resource_gaps") or []
                ) if isinstance(body.get("scan_report"), Mapping) else [],
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

        # A new formal write must not inherit a stale cleanup-ready flag.
        # Recovery preserves the existing state because it only read back the
        # same write.
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
            self._require_persisted_plan_source_scope(
                job, plan, stage="计划执行",
            )
            self._require_persisted_plan_work_unit_target_scope(
                job, plan, stage="计划执行",
            )
            # Plans persisted before the shared AList basename policy may
            # contain an unsafe final name.  Prove the three-way remote state
            # and atomically migrate only incomplete rows before this run can
            # mark itself executing or issue a writer operation.
            prepared_job = self._prepare_legacy_provider_basename_plan(job, plan)
            if prepared_job is not job:
                job = prepared_job
                plan = parser(job.plan)
                self._require_persisted_target_shelf_containment(
                    job, plan, stage="计划执行",
                )
                self._require_persisted_plan_source_scope(
                    job, plan, stage="计划执行",
                )
                self._require_persisted_plan_work_unit_target_scope(
                    job, plan, stage="计划执行",
                )
            defer_cleanup = (
                job.summary.get("automatic") is True
                and job.summary.get("internal_child") is not True
            )
            try:
                # Do this before persisting ``executing``. A problem-bearing
                # plan must never look like it began a formal write, even for
                # an injected executor that would otherwise accept it.
                if job.summary.get("internal_child") is True:
                    try:
                        _require_internal_child_tv_primary_videos(
                            plan, stage="计划执行",
                        )
                    except ValueError as exc:
                        raise EngineExecutionError(str(exc)) from exc
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
            _cancellation_checkpoint()
            ensure(processed_root)
        move = getattr(self.alist, "move", None)
        if not callable(move):
            raise EngineExecutionError("AList 客户端缺少 move 接口，无法隔离原始归档")
        _cancellation_checkpoint()
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
        # This probe is also used by paused recovery gates before any normal
        # execution start gate has authenticated the AList client.  An
        # unauthenticated parent listing is not evidence that task staging is
        # absent, so authenticate here and preserve the fail-closed
        # ``unknown`` result if that cannot be done.
        try:
            self._ensure_authenticated(self.alist)
        except Exception:
            return "unknown"
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
        if (
            matches[0].get("is_link") is True
            or matches[0].get("is_symlink") is True
            or matches[0].get("symlink") is True
        ):
            return "ambiguous"
        return "directory" if matches[0].get("is_dir") is True else "file"

    def execute_automatic(
        self,
        job_id: str,
        *,
        pause_requested: Callable[[], bool] | None = None,
    ) -> EngineJob:
        """Execute or retry an automatic job."""
        return self.execute_job(job_id, pause_requested=pause_requested)

    def _require_artifact_repair_media_present(self, plan: object) -> None:
        """Prove an artifact repair cannot replay a media move.

        ``repair_automatic_artifacts`` deliberately reuses the ordinary
        executor so its NFO/artwork projection and preserve-or-upload policy
        remain identical to normal ingestion.  Before that executor is
        entered, every media destination must already exist at its persisted
        size while the staged source and any interrupted intermediate name are
        absent.  Anything else is a regular execute/recovery case, never a
        metadata repair case.
        """
        for item in list(getattr(plan, "files", ()) or ()):
            source = _safe_remote_path(
                str(getattr(item, "source_path", "") or ""),
                field="artifact repair source_path",
                allow_root=False,
            )
            source_dir = _safe_remote_path(
                str(getattr(item, "source_dir", "") or ""),
                field="artifact repair source_dir",
                allow_root=False,
            )
            target_dir = _safe_remote_path(
                str(getattr(item, "target_dir", "") or ""),
                field="artifact repair target_dir",
                allow_root=False,
            )
            original = str(getattr(item, "original_name", "") or "")
            final = str(getattr(item, "final_name", "") or "")
            if not original or not final or "/" in original or "/" in final:
                raise EngineExecutionError("元数据修复计划含有无效媒体文件名")
            target = _safe_remote_path(
                posixpath.join(target_dir, final),
                field="artifact repair target_path",
                allow_root=False,
            )
            expected = getattr(item, "source_size", None)
            if isinstance(expected, bool) or not isinstance(expected, int) or expected < 0:
                raise EngineExecutionError(f"元数据修复没有有效媒体大小: {source}")
            observed_target = self._exact_info(
                target, wait_for_visibility=False,
            )
            if observed_target is None:
                raise EngineExecutionError(
                    f"元数据修复拒绝缺失媒体目标: {target}"
                )
            actual_size = int(observed_target["size"])
            if actual_size != expected:
                raise EngineExecutionError(
                    "元数据修复拒绝大小不一致的媒体目标: "
                    f"{target} expected={expected}, actual={actual_size}"
                )
            _require_admissible_video_size(
                item, actual_size, path=target, stage="元数据修复",
            )
            if self._exact_info(source, wait_for_visibility=False) is not None:
                raise EngineExecutionError(
                    f"元数据修复拒绝仍可见的媒体来源: {source}"
                )
            if source_dir != target_dir and original != final:
                intermediate = _safe_remote_path(
                    posixpath.join(target_dir, original),
                    field="artifact repair intermediate_path",
                    allow_root=False,
                )
                if self._exact_info(intermediate, wait_for_visibility=False) is not None:
                    raise EngineExecutionError(
                        f"元数据修复拒绝仍可见的中间媒体文件: {intermediate}"
                    )

    def _is_replenishment_artifact_child(
        self,
        job: EngineJob,
        *,
        root_job_id: str,
    ) -> bool:
        """Whether one direct child is an executed video replenishment plan.

        Internal children also represent container metadata and layout
        carriers.  Historical NFO backfill must never sweep those unrelated
        plans, so the exact task-owned media staging shape is part of this
        predicate rather than inferred from a title or provider marker.
        """
        summary = job.summary if isinstance(job.summary, Mapping) else {}
        if (
            job.phase != "executed"
            or summary.get("internal_child") is not True
            or summary.get("root_job_id") != root_job_id
            or summary.get("container_artifacts") is True
        ):
            return False
        request = job.request if isinstance(job.request, Mapping) else {}
        source = request.get("source_path")
        if not isinstance(source, str):
            return False
        try:
            source = _safe_remote_path(
                source, field="replenishment child source_path", allow_root=False,
            )
        except EngineRequestError:
            return False
        prefix = (
            f"{self.library_root.rstrip('/')}/ScrapeFlow/补源/"
            f"{root_job_id}/"
        )
        if not source.startswith(prefix):
            return False
        remainder = source[len(prefix):].split("/")
        if (
            len(remainder) != 2
            or not _JOB_ID_RE.fullmatch(remainder[0])
            or remainder[1] != "__scrapeflow_media__"
        ):
            return False
        files = job.plan.get("files") if isinstance(job.plan, Mapping) else None
        metadata = job.plan.get("metadata") if isinstance(job.plan, Mapping) else None
        media_type = (
            str(metadata.get("media_type") or metadata.get("type") or "").casefold()
            if isinstance(metadata, Mapping) else ""
        )
        if media_type not in {"", "tv", "movie"}:
            return False
        return (
            isinstance(files, list)
            and bool(files)
            and all(
                isinstance(item, Mapping)
                and str(item.get("media_kind") or "video") == "video"
                and is_video_filename(str(item.get("source_path") or ""))
                and is_video_filename(str(item.get("final_name") or ""))
                for item in files
            )
        )

    def repair_root_artifacts(
        self,
        root_job_id: str,
        *,
        pause_requested: Callable[[], bool] | None = None,
    ) -> tuple[EngineJob, list[EngineJob]]:
        """Repair one visible root and its direct executed video children.

        This is intentionally a narrow historic-backfill orchestration.  It
        does not discover jobs, regenerate media plans, or manufacture NFOs;
        each selected child re-enters ``repair_automatic_artifacts`` and thus
        the ordinary executor's normal NFO/artwork projection.  The repair
        preflight below ensures every media target already exists exactly, so
        no selected child can turn this endpoint into a media move.
        """
        root = self._read(root_job_id)
        if self._is_internal_job(root):
            raise EngineRequestError("内部 child 不能作为根元数据修复目标")
        # Root aggregation can advance an otherwise valid Engine carrier to
        # ``completed``/``gaps_pending`` after its formal writer finished.
        # Do not let that aggregate-only phase hide its executed children.
        # A root with no persisted executable plan simply has nothing of its
        # own to repair; direct replenishment children are still considered.
        if root.phase in {"executed", "completed", "gaps_pending"} and root.plan:
            repaired_root = self.repair_automatic_artifacts(
                root_job_id, pause_requested=pause_requested,
            )
        else:
            repaired_root = root
        candidates = [
            child.id
            for child in self.list_jobs()
            if self._is_replenishment_artifact_child(
                child, root_job_id=root_job_id,
            )
        ]
        repaired_children: list[EngineJob] = []
        for child_id in candidates:
            _pause_checkpoint(
                pause_requested if pause_requested is not None else self._pause_requested,
            )
            repaired_children.append(self.repair_automatic_artifacts(
                child_id, pause_requested=pause_requested,
            ))
        return repaired_root, repaired_children

    def repair_automatic_artifacts(
        self,
        job_id: str,
        *,
        pause_requested: Callable[[], bool] | None = None,
    ) -> EngineJob:
        """Re-run only a completed plan's deterministic metadata/artwork.

        The persisted plan is the authoritative target map, so this narrow
        operator action can restore its NFO/poster artifacts without creating
        a new task or touching source media.
        """
        with self.worker_lock():
            job = self._read(job_id)
            effective_pause = (
                pause_requested
                if pause_requested is not None
                else self._pause_requested
            )
            _pause_checkpoint(effective_pause)
            if job.phase not in {"executed", "completed", "gaps_pending"}:
                raise SimpleEngineError(f"Engine job {job_id} 当前不能修复元数据: {job.phase}")
            plan = self._plan_from_job(job)
            if job.summary.get("internal_child") is True:
                try:
                    _require_internal_child_tv_primary_videos(
                        plan, stage="元数据修复",
                    )
                except ValueError as exc:
                    raise EngineExecutionError(str(exc)) from exc
            self._require_persisted_target_shelf_containment(
                job,
                plan,
                stage="元数据修复",
            )
            self._require_persisted_plan_source_scope(
                job, plan, stage="元数据修复",
            )
            self._require_persisted_plan_work_unit_target_scope(
                job, plan, stage="元数据修复",
            )
            self._require_artifact_repair_media_present(plan)
            result = self._invoke_executor(
                plan,
                # This endpoint is an artifact backfill, never a delayed
                # source-cleanup path.  The preflight proved all media targets
                # already exist and sources are absent; defer cleanup also
                # prevents a historical child repair from deleting any
                # residual staging object while writing sidecars.
                defer_cleanup=True,
                pause_requested=effective_pause,
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
                if job.summary.get("internal_child") is True:
                    try:
                        _require_internal_child_tv_primary_videos(
                            plan, stage="恢复检查",
                        )
                    except ValueError as exc:
                        raise EngineRecoveryMatrixError(
                            "internal_child_manifest_invalid",
                            str(exc),
                        ) from exc
                self._require_persisted_target_shelf_containment(
                    job,
                    plan,
                    stage="恢复检查",
                )
                self._require_persisted_plan_source_scope(
                    job, plan, stage="恢复检查",
                )
                self._require_persisted_plan_work_unit_target_scope(
                    job, plan, stage="恢复检查",
                )
                # Do not classify an interrupted old-name move as source
                # loss.  First turn any still-unwritten unsafe final names
                # into their deterministic current equivalents and persist
                # that continuation before performing the ordinary readback
                # matrix below.
                prepared_job = self._prepare_legacy_provider_basename_plan(job, plan)
                if prepared_job is not job:
                    job = prepared_job
                    plan = self._plan_from_job(job)
                    self._require_persisted_target_shelf_containment(
                        job, plan, stage="恢复检查",
                    )
                    self._require_persisted_plan_source_scope(
                        job, plan, stage="恢复检查",
                    )
                    self._require_persisted_plan_work_unit_target_scope(
                        job, plan, stage="恢复检查",
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
                        "恢复检查确认结果尚不完整，等待用户重试: "
                        f"{redact_error(exc)}"
                    ),
                )
                atomic_write_json(self._job_path(job_id), failed.as_dict(), allow_nan=False)
                return failed
            except EngineRequestError as exc:
                # A persisted-path or target-shelf policy violation is not a
                # remote visibility transient. Retrying it would only keep
                # an invalid plan and risk a later bypass.
                summary = dict(job.summary)
                summary["recovery"] = {
                    "status": "terminal",
                    "reason": "target_shelf_policy_violation",
                }
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
                        "恢复检查暂时无法确认远端结果，等待用户重试: "
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

    @staticmethod
    def has_legacy_provider_basename_plan(job: EngineJob) -> bool:
        """Whether a persisted job predates the current AList name policy.

        This deliberately inspects only the durable plan.  It does not make
        a remote-state claim and is used by the WorkUnit coordinator solely
        to choose exact recovery over unsafe retirement/replanning.
        """
        files = job.plan.get("files") if isinstance(job.plan, Mapping) else None
        if not isinstance(files, list):
            return False
        return any(
            isinstance(item, Mapping)
            and not is_provider_safe_basename(item.get("final_name"))
            for item in files
        )

    @classmethod
    def has_provider_basename_recovery_intent(cls, job: EngineJob) -> bool:
        """Whether this carrier must retain its persisted move/rename plan."""
        if cls.has_legacy_provider_basename_plan(job):
            return True
        summary = job.summary if isinstance(job.summary, Mapping) else {}
        migration = summary.get("remote_basename_migration")
        return (
            isinstance(migration, Mapping)
            and migration.get("status") == "prepared"
        )

    @staticmethod
    def _basename_migration_key(path: str) -> str:
        """Match the Engine's NFC/case-insensitive formal-name collision key."""
        return unicodedata.normalize("NFC", path).casefold().rstrip(" .")

    def _prepare_legacy_provider_basename_plan(
        self,
        job: EngineJob,
        plan: object,
    ) -> EngineJob:
        """Persist a safe continuation for a pre-policy filename plan.

        An AList move and the following rename are separate provider effects.
        Old plans may therefore have an exact, task-owned original basename
        in the target directory while their source is already absent.  Before
        any replay, inspect the strict source/final/intermediate matrix and
        atomically replace only still-unwritten unsafe final names.  Existing
        legacy finals remain authoritative and are never renamed or
        overwritten.
        """
        if not self.has_legacy_provider_basename_plan(job):
            return job
        raw_files = job.plan.get("files") if isinstance(job.plan, Mapping) else None
        if not isinstance(raw_files, list):
            raise EngineRequestError("持久化计划的 files 必须是数组")

        updated_files: list[dict[str, object]] = []
        migration_rows: list[dict[str, object]] = []
        changed = False

        for index, raw in enumerate(raw_files):
            if not isinstance(raw, Mapping):
                raise EngineRequestError(f"持久化计划 files[{index}] 格式无效")
            row = dict(raw)
            final_name = row.get("final_name")
            if not isinstance(final_name, str) or not final_name:
                raise EngineRequestError(f"持久化计划 files[{index}].final_name 无效")
            if is_provider_safe_basename(final_name):
                updated_files.append(row)
                continue

            source_path = _safe_remote_path(
                row.get("source_path"),
                field=f"持久化计划 files[{index}].source_path",
                allow_root=False,
            )
            source_dir = _safe_remote_path(
                row.get("source_dir"),
                field=f"持久化计划 files[{index}].source_dir",
                allow_root=False,
            )
            target_dir = _safe_remote_path(
                row.get("target_dir"),
                field=f"持久化计划 files[{index}].target_dir",
                allow_root=False,
            )
            original_name = row.get("original_name")
            if (
                not isinstance(original_name, str)
                or not original_name
                or "/" in original_name
                or "\\" in original_name
                or posixpath.join(source_dir, original_name) != source_path
            ):
                raise EngineRequestError(
                    f"持久化计划 files[{index}] 的来源路径与原文件名不一致"
                )
            expected = row.get("source_size")
            if isinstance(expected, bool) or not isinstance(expected, int) or expected < 0:
                raise EngineRequestError(
                    f"持久化计划 files[{index}] 没有有效来源大小"
                )

            canonical_name = provider_safe_basename(final_name)
            if not is_provider_safe_basename(canonical_name):
                raise EngineRequestError(
                    f"无法生成 AList 兼容的目标文件名: {final_name!r}"
                )
            old_target = _safe_remote_path(
                posixpath.join(target_dir, final_name),
                field=f"持久化计划 files[{index}].legacy_target",
                allow_root=False,
            )
            canonical_target = _safe_remote_path(
                posixpath.join(target_dir, canonical_name),
                field=f"持久化计划 files[{index}].canonical_target",
                allow_root=False,
            )
            intermediate = _safe_remote_path(
                posixpath.join(target_dir, original_name),
                field=f"持久化计划 files[{index}].intermediate_target",
                allow_root=False,
            )

            observed_old = self._exact_info(old_target, wait_for_visibility=False)
            observed_source = self._exact_info(source_path, wait_for_visibility=False)
            observed_intermediate = (
                self._exact_info(intermediate, wait_for_visibility=False)
                if intermediate != old_target
                else observed_old
            )
            observed_canonical = (
                self._exact_info(canonical_target, wait_for_visibility=False)
                if canonical_target != old_target
                else observed_old
            )

            def require_size(
                observed: Mapping[str, object] | None,
                *,
                path: str,
                reason: str,
            ) -> None:
                if observed is None:
                    return
                actual = int(observed["size"])
                if actual != expected:
                    raise EngineRecoveryMatrixError(
                        reason,
                        (
                            "旧计划文件名迁移发现大小不匹配: "
                            f"{path} expected={expected}, actual={actual}"
                        ),
                        source=source_path,
                        target=path,
                        expected_size=expected,
                        actual_size=actual,
                    )

            require_size(observed_old, path=old_target, reason="legacy_target_size_mismatch")
            require_size(observed_source, path=source_path, reason="source_size_mismatch")
            require_size(
                observed_intermediate,
                path=intermediate,
                reason="intermediate_size_mismatch",
            )
            require_size(
                observed_canonical,
                path=canonical_target,
                reason="canonical_target_size_mismatch",
            )

            if observed_old is not None:
                if observed_source is not None:
                    raise EngineRecoveryMatrixError(
                        "legacy_target_source_conflict",
                        "旧计划文件名迁移发现来源和既有目标同时存在",
                        source=source_path,
                        target=old_target,
                        expected_size=expected,
                    )
                if intermediate != old_target and observed_intermediate is not None:
                    raise EngineRecoveryMatrixError(
                        "legacy_target_intermediate_conflict",
                        "旧计划文件名迁移发现既有目标和中间原文件同时存在",
                        source=source_path,
                        target=old_target,
                        expected_size=expected,
                    )
                if canonical_target != old_target and observed_canonical is not None:
                    raise EngineRecoveryMatrixError(
                        "legacy_target_canonical_conflict",
                        "旧计划文件名迁移发现既有目标和规范目标同时存在",
                        source=source_path,
                        target=canonical_target,
                        expected_size=expected,
                    )
                # A completed legacy target is an observed library fact.  Do
                # not rename it merely because today's policy would emit a
                # cleaner name; retain it in this hybrid plan for readback.
                migration_rows.append({
                    "source_path": source_path,
                    "legacy_final_name": final_name,
                    "final_name": final_name,
                    "state": "legacy_final_preserved",
                })
                updated_files.append(row)
                continue

            if observed_canonical is not None:
                raise EngineRecoveryMatrixError(
                    "canonical_target_collision",
                    "旧计划文件名迁移发现规范目标已存在，拒绝覆盖",
                    source=source_path,
                    target=canonical_target,
                    expected_size=expected,
                )
            if observed_source is not None:
                if observed_intermediate is not None:
                    raise EngineRecoveryMatrixError(
                        "source_intermediate_conflict",
                        "旧计划文件名迁移发现来源和中间原文件同时存在",
                        source=source_path,
                        target=intermediate,
                        expected_size=expected,
                    )
                state = "source_pending"
            elif observed_intermediate is not None:
                state = "rename_pending"
            else:
                raise EngineRecoveryMatrixError(
                    "source_lost",
                    "旧计划文件名迁移发现来源、既有目标和中间原文件均不存在",
                    source=source_path,
                    target=canonical_target,
                    expected_size=expected,
                )

            row["final_name"] = canonical_name
            changed = True
            migration_rows.append({
                "source_path": source_path,
                "legacy_final_name": final_name,
                "final_name": canonical_name,
                "state": state,
            })
            updated_files.append(row)

        destination_owners: dict[str, str] = {}
        for index, row in enumerate(updated_files):
            target_dir = _safe_remote_path(
                row.get("target_dir"),
                field=f"持久化计划 files[{index}].target_dir",
                allow_root=False,
            )
            final_name = row.get("final_name")
            if not isinstance(final_name, str) or not final_name:
                raise EngineRequestError(f"持久化计划 files[{index}].final_name 无效")
            target = _safe_remote_path(
                posixpath.join(target_dir, final_name),
                field=f"持久化计划 files[{index}].target_path",
                allow_root=False,
            )
            key = self._basename_migration_key(target)
            previous = destination_owners.get(key)
            if previous is not None and previous != target:
                raise EngineRecoveryMatrixError(
                    "canonical_target_collision",
                    "旧计划文件名迁移会让多个计划文件落到同一目标",
                    target=target,
                )
            destination_owners[key] = target

        if not changed:
            return job
        body = dict(job.plan)
        body["files"] = updated_files
        summary = dict(job.summary)
        summary["remote_basename_migration"] = {
            "status": "prepared",
            "prepared_at": _now(),
            "files": migration_rows,
        }
        prepared = replace(
            job,
            updated_at=_now(),
            plan=body,
            summary=summary,
        )
        # Persist before an executor can see the canonical target.  A restart
        # after this point has one durable, scope-checked continuation plan;
        # no caller needs to infer it from a mutated intake tree.
        atomic_write_json(self._job_path(job.id), prepared.as_dict(), allow_nan=False)
        return prepared

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
        files = list(getattr(plan, "files", ()) or ())
        metadata = getattr(plan, "metadata", None)
        artifact_only = (
            not files
            and isinstance(metadata, Mapping)
            and metadata.get("container_artifacts") is True
        )
        if not files and not artifact_only:
            raise EngineExecutionError("恢复检查的计划没有媒体文件")
        verified_files: list[dict[str, object]] = []
        for item in files:
            source = _safe_remote_path(str(getattr(item, "source_path")), field="source_path", allow_root=False)
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
            # Read the three exact paths once without a visibility backoff.
            # A missing final plus an intact source, or an exact intermediate
            # original basename, is already enough evidence for a safe retry;
            # waiting through the entire provider visibility schedule first
            # would merely stall recovery and cannot make a move/rename replay
            # more correct.  Only the otherwise-unexplained absence below
            # gets the bounded delayed-final probe.
            immediate_target = self._exact_info(target, wait_for_visibility=False)
            if immediate_target is None:
                immediate_source = self._exact_info(source, wait_for_visibility=False)
                if immediate_source is not None:
                    immediate_source_size = int(immediate_source["size"])
                    if immediate_source_size != expected:
                        raise EngineRecoveryMatrixError(
                            "source_size_mismatch",
                            (
                                "恢复检查源文件大小不匹配: "
                                f"{source} expected={expected}, "
                                f"actual={immediate_source_size}"
                            ),
                            source=source,
                            target=target,
                            expected_size=expected,
                            actual_size=immediate_source_size,
                        )
                    raise EngineRecoveryRetryableError(
                        "target_missing_source_present",
                        f"恢复检查发现源文件仍在但目标不存在，可安全重试: {source} -> {target}",
                        source=source,
                        target=target,
                    )
                original_name = str(getattr(item, "original_name", "") or "")
                target_dir = _safe_remote_path(
                    str(getattr(item, "target_dir", "") or ""),
                    field="immediate_intermediate_target_dir",
                    allow_root=False,
                )
                source_dir = _safe_remote_path(
                    str(getattr(item, "source_dir", "") or ""),
                    field="immediate_intermediate_source_dir",
                    allow_root=False,
                )
                if source_dir != target_dir and original_name != str(getattr(item, "final_name", "") or ""):
                    intermediate = _safe_remote_path(
                        posixpath.join(target_dir, original_name),
                        field="immediate_intermediate_target_path",
                        allow_root=False,
                    )
                    observed_intermediate = self._exact_info(
                        intermediate,
                        wait_for_visibility=False,
                    )
                    if observed_intermediate is not None:
                        actual_intermediate_size = int(observed_intermediate["size"])
                        if actual_intermediate_size != expected:
                            raise EngineRecoveryMatrixError(
                                "intermediate_size_mismatch",
                                (
                                    "恢复检查中间原文件大小不匹配: "
                                    f"{intermediate} expected={expected}, "
                                    f"actual={actual_intermediate_size}"
                                ),
                                source=source,
                                target=target,
                                expected_size=expected,
                                actual_size=actual_intermediate_size,
                            )
                        _require_admissible_video_size(
                            item,
                            actual_intermediate_size,
                            path=intermediate,
                            stage="恢复检查中间原文件",
                        )
                        raise EngineRecoveryRetryableError(
                            "rename_pending_intermediate",
                            (
                                "恢复检查发现跨目录 move 已完成、rename 尚未完成；"
                                f"可安全继续: {intermediate} -> {target}"
                            ),
                            source=source,
                            target=target,
                        )
            # Read the target without an expected-size filter first.  The
            # provider helper may return its last observed object when the
            # expected size never appeared; passing ``expected`` here would
            # collapse that durable mismatch into an indistinguishable
            # absence and could incorrectly trigger a retry.  The long
            # visibility retry is reserved for an otherwise unexplained
            # source/final/intermediate absence.
            observed_target = (
                immediate_target
                if immediate_target is not None
                else self._exact_info(target, expected_size=None)
            )
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
                original_name = str(getattr(item, "original_name", "") or "")
                target_dir = _safe_remote_path(
                    str(getattr(item, "target_dir", "") or ""),
                    field="completed_target_dir",
                    allow_root=False,
                )
                source_dir = _safe_remote_path(
                    str(getattr(item, "source_dir", "") or ""),
                    field="completed_source_dir",
                    allow_root=False,
                )
                if source_dir != target_dir and original_name != str(getattr(item, "final_name", "") or ""):
                    intermediate = _safe_remote_path(
                        posixpath.join(target_dir, original_name),
                        field="completed_intermediate_target_path",
                        allow_root=False,
                    )
                    observed_intermediate = self._exact_info(
                        intermediate,
                        wait_for_visibility=False,
                    )
                    if observed_intermediate is not None:
                        raise EngineRecoveryMatrixError(
                            "target_intermediate_conflict",
                            (
                                "恢复检查发现最终目标和中间原文件同时存在，"
                                "拒绝静默覆盖或清理: "
                                f"{target} / {intermediate}"
                            ),
                            source=source,
                            target=target,
                            expected_size=expected,
                            actual_size=int(observed_intermediate["size"]),
                        )
            else:
                observed_source = self._exact_info(source, wait_for_visibility=False)
                if observed_source is None:
                    original_name = str(getattr(item, "original_name", "") or "")
                    target_dir = _safe_remote_path(
                        str(getattr(item, "target_dir", "") or ""),
                        field="intermediate_target_dir",
                        allow_root=False,
                    )
                    source_dir = _safe_remote_path(
                        str(getattr(item, "source_dir", "") or ""),
                        field="intermediate_source_dir",
                        allow_root=False,
                    )
                    intermediate = (
                        _safe_remote_path(
                            posixpath.join(target_dir, original_name),
                            field="intermediate_target_path",
                            allow_root=False,
                        )
                        if source_dir != target_dir and original_name != str(getattr(item, "final_name", "") or "")
                        else None
                    )
                    if intermediate is not None:
                        observed_intermediate = self._exact_info(
                            intermediate,
                            wait_for_visibility=False,
                        )
                        if observed_intermediate is not None:
                            actual_intermediate_size = int(observed_intermediate["size"])
                            if actual_intermediate_size != expected:
                                raise EngineRecoveryMatrixError(
                                    "intermediate_size_mismatch",
                                    (
                                        "恢复检查中间原文件大小不匹配: "
                                        f"{intermediate} expected={expected}, "
                                        f"actual={actual_intermediate_size}"
                                    ),
                                    source=source,
                                    target=target,
                                    expected_size=expected,
                                    actual_size=actual_intermediate_size,
                                )
                            _require_admissible_video_size(
                                item,
                                actual_intermediate_size,
                                path=intermediate,
                                stage="恢复检查中间原文件",
                            )
                            raise EngineRecoveryRetryableError(
                                "rename_pending_intermediate",
                                (
                                    "恢复检查发现跨目录 move 已完成、rename 尚未完成；"
                                    f"可安全继续: {intermediate} -> {target}"
                                ),
                                source=source,
                                target=target,
                            )
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
        # Recovery uses the same full ordinary artifact projection as the
        # writer. Existing library artifacts are authoritative: the writer's
        # preserve-or-upload primitive deliberately never overwrites a
        # different, already-present NFO/poster, so readback must not turn
        # that safe preservation into a terminal mismatch.
        engine = __import__("engine.scraper", fromlist=["planned_nfos", "planned_artwork"])
        planned_nfos = getattr(engine, "planned_nfos", None)
        if not callable(planned_nfos):
            raise EngineExecutionError("Engine 缺少 NFO 目标投影")
        nfo_rows = planned_nfos(plan)
        for target, content in nfo_rows:
            if not isinstance(target, str) or not isinstance(content, (bytes, bytearray)):
                raise EngineExecutionError("Engine NFO 计划格式无效")
            # As with media targets, inspect the actual observed size before
            # deciding whether the artifact is merely missing (safe to replay)
            # or is a durable conflicting object (terminal, never overwrite
            # during recovery).
            observed = self._exact_info(
                target,
                wait_for_visibility=True,
                expected_size=None,
            )
            if observed is None:
                raise EngineExecutionError(f"恢复检查找不到或无法核对 NFO: {target}")
            verified_artifacts.append({
                "target": target,
                "kind": "nfo",
                "size": int(observed["size"]),
            })
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
                _cancellation_checkpoint()
                deleted = remove_empty(path)
            except (EnginePauseRequested, EngineCancellationRequested):
                raise
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
                _cancellation_checkpoint()
                remove(parent, [name])
            except (EnginePauseRequested, EngineCancellationRequested):
                raise
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
                    _cancellation_checkpoint()
                    remove(directory, [name])
                except (EnginePauseRequested, EngineCancellationRequested):
                    raise
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

    def cancel_job(self, job_id: str, *, reason: str = "cancelled by operator") -> EngineJob:
        """Cancel a queued job immediately or a running one at a safe boundary.

        A queued/inactive job changes only its durable projection and never
        touches AList.  When the current local writer owns the formal-write
        lock, archive/identity/planning/executing/cleanup phases receive a
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
            "cleaning", "executed", "completed", "cancelled",
        }
        try:
            with self.worker_lock():
                job = self._read(job_id)
                if job.phase not in immediate_phases:
                    raise SimpleEngineError(
                        f"Engine job {job_id} 当前不能取消: {job.phase}"
                    )
                if job.phase == "cancelled":
                    self._clear_cancel_request(job_id)
                    return job
                return self._cancelled_job(job, reason=normalized_reason)
        except EngineWorkerBusyError:
            # Fence an idle revision before atomically closing it. The local
            # writer owns the one worker lock, so this job cannot start until
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
                "executed", "completed",
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
    "recover_persisted_engine_jobs",
    "SimpleEngineError",
    "SimpleEngineRunner",
    "SimplePlanExecutor",
]
