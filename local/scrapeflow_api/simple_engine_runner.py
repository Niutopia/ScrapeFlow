"""Bridge Engine planning to the automatic single-user workflow.

A source is identified and planned, written to AList, read back by exact path
and size, then cleaned according to the plan.  Interrupted writes are
reconciled from AList state before they are retried.
"""

from __future__ import annotations

import contextlib
import errno
import json
import os
import posixpath
import re
import time
import uuid
from dataclasses import asdict, dataclass, replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Callable, Iterator, Mapping, Protocol

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
from local.scrapeflow_api.redaction import redact_error


class SimpleEngineError(RuntimeError):
    """Base error for the small Engine bridge."""


class EngineRequestError(SimpleEngineError, ValueError):
    """The plan request is malformed or incomplete."""


class EngineExecutionError(SimpleEngineError):
    """A simple plan operation failed or could not be read back."""


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
    target_parent: str
    season: int | None
    trace: Mapping[str, object]

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
_ENGINE_PHASES = frozenset({
    "queued", "analyzing", "identity_matching", "planning", "planned",
    "executing", "verifying", "cleaning", "executed", "completed",
    "retry_wait", "failed", "failed_identity", "failed_provider",
    "failed_write", "failed_verification", "failed_cleanup", "cancelled",
})

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
        return cls(
            id=job_id,
            phase=str(phase),
            created_at=str(raw["created_at"]),
            updated_at=str(raw["updated_at"]),
            request=dict(request),
            plan=dict(plan),
            summary=dict(summary),
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
    with _engine_worker_lock(root):
        for path in sorted(jobs_root.glob("*.json")):
            try:
                raw = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise SimpleEngineError(f"Engine job 无法读取: {path.stem}") from exc
            if not isinstance(raw, Mapping):
                raise SimpleEngineError(f"Engine job 记录不是对象: {path.stem}")
            job = EngineJob.from_dict(raw)
            if job.phase != "executing":
                continue
            recovered_job = replace(
                job,
                phase="retry_wait",
                updated_at=_now(),
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

    def execute(self, plan: object) -> Mapping[str, object]:
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

        for item in moved:
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
                    if not isinstance(target, str) or not isinstance(data, (bytes, bytearray)):
                        raise EngineExecutionError("Engine 生成的 NFO 结构无效")
                    self._ensure_dir(posixpath.dirname(target) or "/")
                    observed = self._upload_bytes(target, bytes(data), "application/xml")
                    artifacts.append({"target": target, "kind": "nfo", **observed})

            planned_artwork = getattr(__import__("engine.scraper", fromlist=["planned_artwork"]), "planned_artwork", None)
            downloader = getattr(self.tmdb, "download_poster", None) if self.tmdb is not None else None
            if callable(planned_artwork):
                for target, image_path, role in planned_artwork(plan):
                    if not callable(downloader):
                        raise EngineExecutionError("计划包含海报，但 TMDB 客户端没有 download_poster")
                    data = downloader(image_path)
                    if not isinstance(data, (bytes, bytearray)):
                        raise EngineExecutionError(f"TMDB 海报响应无效: {image_path}")
                    self._ensure_dir(posixpath.dirname(target) or "/")
                    observed = self._upload_bytes(target, bytes(data), "image/jpeg")
                    artifacts.append({"target": target, "kind": role, **observed})

        cleaned: list[str] = []
        remove = getattr(self.alist, "remove", None)
        if not callable(remove):
            raise EngineExecutionError("AList 客户端缺少 remove 接口，无法执行计划清理项")
        for item in list(getattr(plan, "cleanup_files", ()) or ()):
            source_path = str(getattr(item, "source_path"))
            source_dir = str(getattr(item, "source_dir"))
            original = str(getattr(item, "original_name"))
            if self._exact(source_path) is None:
                continue
            remove(source_dir, [original])
            if self._exact(source_path) is not None:
                raise EngineExecutionError(f"清理后源文件仍存在: {source_path}")
            cleaned.append(source_path)
        removed_source_directories = self._cleanup_empty_source_tree(
            str(getattr(plan, "source_root", ""))
        )
        return {
            "files": moved,
            "file_count": len(moved),
            "artifacts": artifacts,
            "artifact_count": len(artifacts),
            "media_only": media_only,
            "cleanup": cleaned,
            "cleanup_count": len(cleaned),
            "removed_source_directories": removed_source_directories,
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
    ) -> None:
        self.state_root = Path(state_root).resolve()
        self.jobs_root = self.state_root / "jobs"
        self.locks_root = self.state_root / "locks"
        self.jobs_root.mkdir(mode=0o700, parents=True, exist_ok=True)
        self.alist = alist
        self.tmdb = tmdb
        self.planner = planner
        self.executor = executor or SimplePlanExecutor(alist, tmdb)
        self.validate = bool(validate)
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
    ) -> Mapping[str, object]:
        """Install one subtitle member under the single formal write lock."""
        with self.worker_lock():
            installer = getattr(self.executor, "install_subtitle_sidecar", None)
            if not callable(installer):
                raise EngineExecutionError("当前 Engine executor 不支持字幕侧挂写入")
            return dict(installer(
                source_path, target_path, expected_size=expected_size,
                video_path=video_path,
            ))

    def find_by_source(self, source_path: str) -> EngineJob | None:
        """Return an existing non-terminal job for duplicate-submit checks."""
        normalized = _safe_remote_path(source_path, field="source_path", allow_root=False)
        for job in self.list_jobs():
            if job.summary.get("internal_child") is True:
                continue
            candidate = job.request.get("source_path")
            if candidate == normalized and job.phase not in {"executed", "failed", "cancelled"}:
                return job
        return None

    def create_automatic_job(
        self,
        source_path: str,
        *,
        job_id: str | None = None,
    ) -> EngineJob:
        """Persist a queued source before doing any network/TMDB work."""
        source = _safe_remote_path(source_path, field="source_path", allow_root=False)
        if is_production_test_media_path(source):
            raise EngineRequestError("生产 E2E 测试目录不能创建自动任务")
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
            phase="queued",
            created_at=now,
            updated_at=now,
            request={"source_path": source},
            plan={},
            summary={
                "automatic": True,
                "automatic_stage": "identity_matching",
                "source_root": source,
                "mode": "auto",
                "automatic_attempts": 0,
                "automatic_terminal": False,
                "next_retry_seconds": None,
            },
        )
        atomic_write_json(self._job_path(identifier), job.as_dict(), allow_nan=False)
        return job

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

    def clear_jobs(self) -> int:
        removed = 0
        for path in self.jobs_root.glob("*.json"):
            path.unlink(missing_ok=True)
            removed += 1
        return removed

    def delete_job(self, job_id: str) -> bool:
        path = self._job_path(job_id)
        if not path.exists():
            return False
        path.unlink()
        return True

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

    def _automatic_target_parent(
        self,
        match: object,
        source_path: str,
    ) -> str:
        """Choose the library shelf from machine evidence, never a UI choice."""
        media_type = str(getattr(match, "media_type", ""))
        if media_type == "movie":
            return f"{self.library_root}/电影"
        if media_type != "tv":
            raise EngineRequestError(f"自动匹配返回了不支持的媒体类型: {media_type}")
        lowered = source_path.casefold()
        anime_markers = ("/番剧/", "/番組/", "/anime/", "/animation/", "/动漫/")
        is_animation = any(marker in lowered for marker in anime_markers)
        # TMDB details are the authoritative fallback when the inbound folder
        # is the neutral /待刮削 root.  A failed enrichment must not discard a
        # confident TV match or leave its shelf undecided.
        details: Mapping[str, object] = {}
        getter = getattr(self.tmdb, "get", None)
        if callable(getter):
            try:
                raw = getter(f"/tv/{int(getattr(match, 'tmdb_id'))}")
                if isinstance(raw, Mapping):
                    details = raw
            except Exception:
                details = {}
        genres = details.get("genres")
        if isinstance(genres, list):
            is_animation = is_animation or any(
                isinstance(row, Mapping) and row.get("id") == 16 for row in genres
            )
        if details.get("original_language") in {"ja", "zh", "ko"}:
            is_animation = is_animation or details.get("original_language") == "ja"
        countries = details.get("origin_country")
        if isinstance(countries, list) and "JP" in countries:
            is_animation = True
        return (
            f"{self.library_root}/番剧"
            if is_animation
            else f"{self.library_root}/美剧"
        )

    def resolve_automatic_request(
        self,
        source_path: str,
        payload: Mapping[str, object] | None = None,
    ) -> tuple[EngineRequest, AutomaticIdentity]:
        """Infer identity, shelf and season from one source directory."""
        # The automatic contract has one input: the source directory.
        del payload
        body: dict[str, object] = {}
        source = _safe_remote_path(source_path, field="source_path", allow_root=False)
        self._ensure_authenticated(self.alist)
        engine = __import__("engine.scraper", fromlist=["auto_match_tmdb"])
        raw_type = "auto"
        query_fn = getattr(engine, "_query_from_source", None)
        query = query_fn(source) if callable(query_fn) else posixpath.basename(source)
        query = str(query).strip()
        requested_type = None if raw_type == "auto" else str(raw_type)
        context_fn = getattr(engine, "_media_context_from_source_and_target", None)
        prefer_animation = False
        if callable(context_fn):
            contextual_type, prefer_animation = context_fn(
                source,
                str(body.get("parent_path") or self.library_root),
            )
            if requested_type is None:
                requested_type = contextual_type
        expected_episode_count: int | None = None
        if requested_type == "tv":
            try:
                rows = self.alist.walk(source, ignore_orphan_temp=True)
                expected_fn = getattr(engine, "_expected_single_tv_episode_count", None)
                if callable(expected_fn):
                    expected_episode_count = expected_fn(rows)
            except Exception:
                expected_episode_count = None
        match = None
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
        trace = getattr(match, "decision_trace", None)
        if isinstance(trace, dict):
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
        parent = self._automatic_target_parent(
            match,
            source,
        )
        request = EngineRequest.from_mapping({
            **body,
            "source_path": source,
            "parent_path": parent,
            "media_type": media_type,
            "tmdb_id": int(getattr(match, "tmdb_id")),
            "query": query,
            "season": season if isinstance(season, int) else 1,
        })
        identity = AutomaticIdentity(
            media_type=media_type,
            tmdb_id=int(getattr(match, "tmdb_id")),
            title=str(getattr(match, "title", "")),
            year=str(getattr(match, "year", "未知年份")),
            confidence=float(getattr(match, "confidence", 0.0)),
            target_parent=parent,
            season=season if isinstance(season, int) else None,
            trace=dict(getattr(match, "decision_trace", {}) or {}),
        )
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
    ) -> EngineJob:
        """Build and persist one plan.

        ``internal_child_of`` is written into the first durable JSON record
        when a provider creates a child.  That removes the small crash window
        in which a planned child existed but had not yet been marked hidden
        from the public root queue.
        """
        request = request if isinstance(request, EngineRequest) else EngineRequest.from_mapping(request)
        if job_id is None:
            job_id = f"engine-{uuid.uuid4().hex}"
        _safe_job_id(job_id)
        if internal_child_of is not None:
            _safe_job_id(internal_child_of)
        if self._job_path(job_id).exists():
            raise SimpleEngineError(f"Engine job 已存在: {job_id}")
        plan = self._build_plan(request)
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
    ) -> EngineJob:
        """Resolve and persist one plan without exposing identity decisions."""
        request, identity = self.resolve_automatic_request(source_path, payload)
        job = self.plan_job(request, job_id=job_id)
        summary = dict(job.summary)
        summary["identity"] = identity.as_dict()
        summary["automatic"] = True
        summary["resource_gaps"] = list(
            (job.plan.get("scan_report") or {}).get("resource_gaps") or []
        ) if isinstance(job.plan.get("scan_report"), Mapping) else []
        updated = replace(job, summary=summary, updated_at=_now())
        atomic_write_json(self._job_path(job.id), updated.as_dict(), allow_nan=False)
        return updated

    def plan_automatic_job(self, job_id: str) -> EngineJob:
        """Resolve and persist the plan for an already queued source job."""
        with self.worker_lock():
            job = self._read(job_id)
            if job.phase == "planned":
                return job
            if job.phase == "executed":
                return job
            source = job.request.get("source_path")
            if not isinstance(source, str):
                raise EngineRequestError("自动任务缺少 source_path")
            matching = replace(job, phase="identity_matching", updated_at=_now(), error=None)
            atomic_write_json(self._job_path(job_id), matching.as_dict(), allow_nan=False)
            request, identity = self.resolve_automatic_request(source)
            planning = replace(matching, phase="planning", updated_at=_now())
            atomic_write_json(self._job_path(job_id), planning.as_dict(), allow_nan=False)
            plan = self._build_plan(request)
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
                "automatic_stage": "formal_write",
                "resource_gaps": list(
                    (body.get("scan_report") or {}).get("resource_gaps") or []
                ) if isinstance(body.get("scan_report"), Mapping) else [],
                "automatic_attempts": int(job.summary.get("automatic_attempts") or 0),
                "next_retry_seconds": None,
            })
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

    def _invoke_executor(self, plan: object) -> Mapping[str, object]:
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
        result = target(plan)
        if not isinstance(result, Mapping):
            return {"result": _jsonable(result)}
        return dict(result)

    def execute_job(self, job_id: str) -> EngineJob:
        with self.worker_lock():
            job = self._read(job_id)
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
                error=None,
            )
            atomic_write_json(self._job_path(job_id), executing.as_dict(), allow_nan=False)
            try:
                result = self._invoke_executor(plan)
            except Exception as exc:
                failed = replace(
                    executing,
                    phase="failed",
                    updated_at=_now(),
                    error=redact_error(exc),
                )
                atomic_write_json(self._job_path(job_id), failed.as_dict(), allow_nan=False)
                raise
            done = replace(
                executing,
                phase="executed",
                updated_at=_now(),
                execution=result,
                error=None,
            )
            atomic_write_json(self._job_path(job_id), done.as_dict(), allow_nan=False)
            return done

    def execute_automatic(self, job_id: str) -> EngineJob:
        """Execute or retry an automatic job."""
        return self.execute_job(job_id)

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
            result = self._invoke_executor(plan)
            summary = dict(job.summary)
            summary["last_artifact_repair_at"] = _now()
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
            if job.phase in {"planned", "executed", "cancelled"}:
                return job
            try:
                plan = self._plan_from_job(job)
                execution = self._readback_plan(plan)
            except Exception as exc:
                failed = replace(
                    job,
                    phase="retry_wait",
                    updated_at=_now(),
                    error=(
                        "恢复检查暂时无法确认远端结果，将自动重试: "
                        f"{redact_error(exc)}"
                    ),
                )
                atomic_write_json(self._job_path(job_id), failed.as_dict(), allow_nan=False)
                return failed
            recovered = replace(
                job,
                phase="executed",
                updated_at=_now(),
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

    def _readback_plan(self, plan: object) -> dict[str, object]:
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
            observed = self._exact_info(target, expected_size=expected)
            if observed is None:
                raise EngineExecutionError(f"恢复检查找不到目标: {target}")
            if int(observed["size"]) != expected:
                raise EngineExecutionError(
                    f"恢复检查目标大小不匹配: {target} expected={expected}, actual={observed['size']}"
                )
            _require_admissible_video_size(
                item, observed["size"], path=target, stage="恢复检查",
            )
            if self._exact_info(source, wait_for_visibility=False) is not None:
                raise EngineExecutionError(f"恢复检查发现源文件仍存在，状态不确定: {source}")
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
                    observed = self._exact_info(
                        target,
                        wait_for_visibility=True,
                        expected_size=len(content),
                    )
                    if observed is None or int(observed["size"]) != len(content):
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
                    if observed is None or int(observed["size"]) <= 0:
                        raise EngineExecutionError(f"恢复检查找不到海报: {target}")
                    verified_artifacts.append({"target": target, "kind": str(role), "size": int(observed["size"])})
        cleaned: list[str] = []
        for item in list(getattr(plan, "cleanup_files", ()) or ()):
            source = _safe_remote_path(
                str(getattr(item, "source_path")),
                field="cleanup_source_path",
                allow_root=False,
            )
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
        }

    def cancel_job(self, job_id: str, *, reason: str = "cancelled by operator") -> EngineJob:
        """Mark a not-yet-executed plan cancelled without touching AList.

        ``failed_identity`` is intentionally included: a terminal identity
        failure can retain its inbound source forever, and an operator needs
        a local, reversible projection to close that task without deleting or
        moving the source remotely.
        """
        with self.worker_lock():
            job = self._read(job_id)
            if job.phase == "executed":
                return job
            if job.phase == "executing":
                raise EngineWorkerBusyError("Engine 任务正在执行；请等待当前远端操作结束")
            if job.phase not in {"planned", "failed", "failed_identity", "cancelled"}:
                raise SimpleEngineError(f"Engine job {job_id} 当前不能取消: {job.phase}")
            if job.phase == "cancelled":
                return job
            cancelled = replace(
                job,
                phase="cancelled",
                updated_at=_now(),
                error=redact_error(reason.strip() or "cancelled by operator"),
            )
            atomic_write_json(self._job_path(job_id), cancelled.as_dict(), allow_nan=False)
            return cancelled


__all__ = [
    "AutomaticIdentity",
    "EngineExecutionError",
    "EngineJob",
    "EngineJobNotFoundError",
    "EngineRequest",
    "EngineRequestError",
    "EngineWorkerBusyError",
    "recover_persisted_engine_jobs",
    "SimpleEngineError",
    "SimpleEngineRunner",
    "SimplePlanExecutor",
]
