"""Automatic, task-owned provider acquisition and replenishment."""

from __future__ import annotations

import functools
import inspect
import json
import os
import posixpath
import re
import shutil
import time
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Callable, Mapping, Protocol, Sequence

from engine.scrapeflow.serialization import atomic_write_json
from engine.scrapeflow.media_policy import (
    SUBTITLE_EXTENSIONS,
    VIDEO_EXTENSIONS,
)
from engine.scrapeflow.target_shelf import target_shelf_for_shelf_segment
from engine.scrapeflow.video_admission import (
    VideoAdmissionError,
    probe_remote_video_stream,
)
from .replenishment import (
    build_replenishment_requests,
    enrich_replenishment_plan_aliases,
    normalize_reusable_candidate,
    replenishment_request_lane,
    reusable_candidate_scope,
    select_replenishment_candidates,
)
from .provider_delivery import ProviderDeliveryError, validate_provider_delivery
from .provider_staging import (
    CANONICAL_REPLENISHMENT_STAGING_ROOT,
    ProviderStagingPathError,
    validate_provider_staging_root,
)
from .redaction import redact_error, redact_value
from .replenishment_tiers import (
    EXHAUSTION_MIN_DISTINCT_LOCATORS,
    FAILURE_CANDIDATE,
    FAILURE_INFRASTRUCTURE,
    FAILURE_IN_DOUBT,
    ReplenishmentTierError,
    STRICT_TIER_ORDER,
    TIER_LOCAL_MAGNET,
    TIER_QUARK_SHARE,
    apply_tier_outcome,
    initial_tier_state,
    required_sources_for_tier,
)
from .simple_engine_runner import EngineJob, SimpleEngineRunner


_VIDEO_EXTENSIONS = VIDEO_EXTENSIONS
_SUBTITLE_EXTENSIONS = SUBTITLE_EXTENSIONS
_GAP_SLUG = re.compile(r"[^a-zA-Z0-9._-]+")
_EPISODE_TOKEN = re.compile(r"(?<![A-Z0-9])S0*(\d{1,3})[ ._-]*E0*(\d{1,4})(?!\d)", re.I)
_SEASON_TOKEN = re.compile(r"(?<![A-Z0-9])S0*(\d{1,3})(?!\d)", re.I)
_INTERRUPTED_GAP_PHASES = frozenset({
    "provider_searching", "acquiring", "staging_verifying",
    "subtitle_installing", "child_planning", "child_executing",
    "final_verifying", "cleaning", "child_failed",
})
_DURABLE_CANDIDATE_EXCLUSION_LIMIT = EXHAUSTION_MIN_DISTINCT_LOCATORS
_DURABLE_CANDIDATE_LOCATOR_LIMIT = 4096
_DURABLE_CANDIDATE_PROVIDER_LIMIT = 64
_DURABLE_CANDIDATE_RELEASE_NAME_LIMIT = 512
_BTIH_TOKEN = re.compile(r"(?i)\bbtih:([0-9a-f]{40}|[a-z2-7]{32})\b")
_INFOHASH_TOKEN = re.compile(r"(?i)^(?:[0-9a-f]{40}|[a-z2-7]{32})$")
_ATTEMPT_ID_TOKEN = re.compile(r"^attempt-[a-zA-Z0-9._-]{1,96}$")
_JOB_ID_TOKEN = re.compile(r"^[a-zA-Z0-9][a-zA-Z0-9._-]{0,127}$")
_SELECTION_SNAPSHOT_FILE = "selected_candidate.json"
_SELECTION_SNAPSHOT_MAX_BYTES = 256 * 1024
_CANDIDATE_MEMORY_FILE = "replenishment-candidate-memory.json"
_CANDIDATE_MEMORY_VERSION = 1
_CANDIDATE_MEMORY_MAX_BYTES = 2 * 1024 * 1024
_CANDIDATE_MEMORY_MAX_ENTRIES = 128
_CANDIDATE_MEMORY_MAX_GAPS_PER_ENTRY = 64
_FAILURE_DELIVERY = "delivery"
_FAILURE_CANCELLED = "cancelled"
_POST_ACQUISITION_REAUDIT_KEY = "post_acquisition_reaudit"
_LEGACY_COMPANION_SUBTITLE_MIGRATION_KEY = "legacy_companion_subtitle_migration"
_LEGACY_COMPANION_SUBTITLE_MIGRATION_MESSAGE = (
    "旧媒体补源携带的字幕成员已停用；系统没有把它作为字幕通道交付或写入正式库，"
    "请通过 RootJob 字幕通道重新审计"
)
_LEGACY_SUBTITLE_MIGRATION_MESSAGE = (
    "旧字幕自动通道已停用；请通过 RootJob 字幕通道重新触发，"
    "系统没有提交、下载、暂存或写入任何字幕"
)
_POST_ACQUISITION_REAUDIT_PENDING_STATUSES = frozenset({
    "pending",
    "audit_uncertain",
    "gap_still_actionable",
    "cleanup_failed",
})
_POST_ACQUISITION_REAUDIT_MAX_GAPS = 256
_KNOWN_FAILURE_SCOPES = frozenset({
    FAILURE_CANDIDATE,
    FAILURE_INFRASTRUCTURE,
    FAILURE_IN_DOUBT,
    _FAILURE_DELIVERY,
})
_DURABLE_GAP_STATE_DEFAULTS: dict[str, object] = {
    **initial_tier_state(),
    "active_attempt": None,
    "external_task_id": None,
    "next_retry_at": None,
    "tier_status": None,
}

class AutomaticReplenishmentError(RuntimeError):
    """An automatic replenishment attempt could not be completed."""


class AutomaticReplenishmentCancelled(AutomaticReplenishmentError):
    """A cooperative control boundary stopped an in-flight provider run."""

    # Lower-level archive/Torrent adapters use this marker to preserve the
    # control stop instead of wrapping it as a candidate/provider failure.
    pause_requested = True


class AutomaticReplenishmentPaused(AutomaticReplenishmentCancelled):
    """Pause stopped a child at a resumable boundary; do not cancel the child."""

    # Shared lower-level adapters cannot import this API module without
    # creating a dependency cycle.  This marker lets them preserve a pause
    # rather than reclassifying it as a failed provider delivery.
    pause_requested = True


def _provider_pause_checkpoint(
    pause_requested: Callable[[], bool] | None,
) -> None:
    """Fence a provider-owned side effect on the current RootJob scope.

    A failing control callback is indistinguishable from an unreadable pause
    state, so it intentionally stops the attempt.  The callback is evaluated
    immediately before the port call rather than only once at materializer
    entry; a pilot scope can be withdrawn while a downloader is validating a
    manifest or while AList is polling a completed transfer.
    """
    if pause_requested is None:
        return
    try:
        paused = bool(pause_requested())
    except AutomaticReplenishmentPaused:
        raise
    except Exception as exc:
        raise AutomaticReplenishmentPaused(
            "补源暂停状态不可确认，已在外部操作前安全停止",
        ) from exc
    if paused:
        raise AutomaticReplenishmentPaused(
            "补源已暂停或不在当前 RootJob 试运行范围",
        )


class _PauseCheckedProviderPort:
    """Proxy every AList call through one cooperative RootJob checkpoint."""

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

        @functools.wraps(value)
        def guarded(*args: object, **kwargs: object) -> object:
            _provider_pause_checkpoint(self._pause_requested)
            return value(*args, **kwargs)

        return guarded


def _accepts_keyword(method: object, keyword: str) -> bool:
    """Return whether a callable can safely receive one named callback.

    This deliberately avoids the old ``except TypeError: retry without
    callback`` pattern: an implementation's real TypeError must never cause a
    second unguarded provider call.
    """
    if not callable(method):
        return False
    try:
        parameters = inspect.signature(method).parameters.values()
    except (TypeError, ValueError):
        return False
    return any(
        parameter.kind is inspect.Parameter.VAR_KEYWORD
        or parameter.name == keyword
        for parameter in parameters
    )


def _call_with_pause(
    method: Callable[..., object],
    *args: object,
    pause_requested: Callable[[], bool] | None,
    **kwargs: object,
) -> object:
    """Call a provider boundary without silently dropping its scope fence."""
    if pause_requested is None:
        return method(*args, **kwargs)
    if not _accepts_keyword(method, "pause_requested"):
        raise AutomaticReplenishmentError(
            "补源边界不支持 pause_requested；拒绝在 RootJob 试运行范围执行",
        )
    return method(*args, pause_requested=pause_requested, **kwargs)


class _CandidateRoundLimitError(AutomaticReplenishmentError):
    """The current invocation exhausted its bounded candidate work slice."""

    failure_scope = FAILURE_CANDIDATE


class _ProviderVideoAdmissionError(AutomaticReplenishmentError):
    """Preserve whether a failed probe is candidate, delivery, or runtime."""

    def __init__(
        self,
        error: VideoAdmissionError,
        *,
        candidate: Mapping[str, object] | None = None,
    ) -> None:
        super().__init__(f"视频流准入失败: {error.reason}")
        self.failure_scope = (
            FAILURE_CANDIDATE
            if error.candidate_invalid
            else FAILURE_INFRASTRUCTURE
            if error.infrastructure
            else _FAILURE_DELIVERY
        )
        self.exclude_candidate = error.candidate_invalid
        self.reusable_candidate = not error.candidate_invalid
        if candidate is not None:
            self.candidate = {
                key: candidate.get(key)
                for key in ("provider", "release_name", "locator", "infohash")
                if candidate.get(key) is not None
            }


def _contract_delivery_shape(
    delivery: Mapping[str, object],
    *,
    lane: str,
    staging_root: str,
) -> dict[str, object]:
    """Return only the public provider Delivery fields.

    Provider-specific manifests and convenience roots are intentionally
    consumed before this boundary.  Runtime code therefore has one shape for
    every lane and cannot accidentally regain a private provider contract.
    """

    delivered_lane = delivery.get("lane", lane)
    if delivered_lane != lane:
        raise AutomaticReplenishmentError("materializer 返回了错误 lane")
    delivered_root = delivery.get("staging_root", staging_root)
    if delivered_root != staging_root:
        raise AutomaticReplenishmentError("materializer 返回了错误 staging_root")
    attempt_id = posixpath.basename(staging_root.rstrip("/"))
    delivered_attempt = delivery.get("attempt_id", attempt_id)
    if delivered_attempt != attempt_id:
        raise AutomaticReplenishmentError("materializer 返回了错误 attempt_id")
    raw_files = delivery.get("files")
    if not isinstance(raw_files, list):
        raise AutomaticReplenishmentError("materializer 返回的 files 无效")
    files: list[dict[str, object]] = []
    for raw in raw_files:
        if not isinstance(raw, Mapping):
            raise AutomaticReplenishmentError("materializer 返回的 files 项无效")
        files.append({
            "path": raw.get("path"),
            "size": raw.get("size"),
            "kind": raw.get("kind"),
            "gap_ids": raw.get("gap_ids"),
        })
    result: dict[str, object] = {
        "lane": lane,
        "attempt_id": attempt_id,
        "staging_root": staging_root,
        "files": files,
    }
    if "external_task_id" in delivery:
        result["external_task_id"] = delivery.get("external_task_id")
    return result


def _remote_file_size(alist: object, path: str) -> int | None:
    exact = getattr(alist, "exact_file_info", None)
    if callable(exact):
        try:
            row = exact(path)
        except AutomaticReplenishmentPaused:
            raise
        except Exception:
            row = None
        size = row.get("size") if isinstance(row, Mapping) else None
        if isinstance(size, int) and not isinstance(size, bool) and size > 0:
            return size
    listing = getattr(alist, "list", None)
    if not callable(listing):
        return None
    parent = posixpath.dirname(path) or "/"
    name = posixpath.basename(path)
    try:
        try:
            rows = listing(parent, refresh=True)
        except TypeError:
            rows = listing(parent)
    except AutomaticReplenishmentPaused:
        raise
    except Exception:
        return None
    matches = [
        row for row in rows
        if isinstance(row, Mapping) and row.get("name") == name
        and row.get("is_dir") is not True
    ] if isinstance(rows, list) else []
    if len(matches) != 1:
        return None
    size = matches[0].get("size")
    return size if isinstance(size, int) and not isinstance(size, bool) and size > 0 else None


def _isolate_cloud_delivery_videos(
    files: Sequence[Mapping[str, object]],
    *,
    staging_root: str,
    alist: object,
) -> list[dict[str, object]]:
    """Move only mixed cloud videos to a task-owned planner subroot.

    Subtitle movement is deliberately unnecessary.  If subtitle pairing or
    isolation cannot later be proven from the normalized file rows, runtime
    ignores those sidecars while the isolated video still proceeds.
    """

    output = [dict(row) for row in files]
    videos = [row for row in output if row.get("kind") == "video"]
    subtitles = [row for row in output if row.get("kind") == "subtitle"]
    if not videos or not subtitles:
        return output
    mkdir = getattr(alist, "mkdir", None)
    move = getattr(alist, "move", None)
    if not callable(mkdir) or not callable(move):
        raise AutomaticReplenishmentError(
            "AList 客户端缺少混合交付视频隔离能力"
        )
    media_root = f"{staging_root}/__scrapeflow_media__"
    if any(
        isinstance(row.get("path"), str)
        and str(row["path"]).startswith(media_root + "/")
        for row in subtitles
    ):
        raise AutomaticReplenishmentError("云端混合交付占用了保留的媒体隔离根")
    basenames = [posixpath.basename(str(row.get("path") or "")) for row in videos]
    if any(not name for name in basenames) or len(basenames) != len(set(basenames)):
        raise AutomaticReplenishmentError("云端混合交付视频文件名冲突")
    mkdir(media_root)
    for row, name in zip(videos, basenames, strict=True):
        source = row.get("path")
        size = row.get("size")
        if not isinstance(source, str) or type(size) is not int or size <= 0:
            raise AutomaticReplenishmentError("云端混合交付视频映射无效")
        destination = f"{media_root}/{name}"
        if source != destination:
            source_size = _remote_file_size(alist, source)
            destination_size = _remote_file_size(alist, destination)
            if destination_size == size and source_size is None:
                row["path"] = destination
                continue
            if source_size != size or destination_size is not None:
                raise AutomaticReplenishmentError("云端混合交付视频隔离前回读不一致")
            move(posixpath.dirname(source), media_root, [name])
            if _remote_file_size(alist, destination) != size:
                raise AutomaticReplenishmentError("云端混合交付视频隔离后回读不一致")
            row["path"] = destination
    return output


class AutomaticProviderSearch(Protocol):
    def run(self, request: Mapping[str, object]) -> Mapping[str, object]: ...


class AutomaticMaterializer(Protocol):
    def acquire(
        self,
        request: Mapping[str, object],
        selections: Sequence[Mapping[str, object]],
        *,
        staging_root: str,
        workspace: Path,
        alist: object,
        pause_requested: Callable[[], bool] | None = None,
    ) -> Mapping[str, object]: ...

    # Optional recovery hook.  Implementations query a persisted external
    # task id when one is known.  This hook must never submit a new task.
    def reconcile_existing_task(
        self,
        request: Mapping[str, object],
        selections: Sequence[Mapping[str, object]],
        *,
        staging_root: str,
        workspace: Path,
        alist: object,
        external_task_id: str | None,
        pause_requested: Callable[[], bool] | None = None,
    ) -> Mapping[str, object]: ...


class LocalTorrentAutomaticMaterializer:
    """Use the bundled Torrent downloader with a task-owned staging root."""

    def __init__(
        self,
        delegate: object | None = None,
        *,
        archive_preprocessor: object | None = None,
    ) -> None:
        self.pre_admits_local_video = delegate is None
        if delegate is None:
            from engine.tools.replenishment_adapter.materialize import LocalTorrentMaterializer
            delegate = LocalTorrentMaterializer()
        self.delegate = delegate
        # Optional shared ingress adapter.  It receives only a completed
        # task-owned delivery and can replace an explicitly marked archive/SFX
        # row with extracted media in the same staging tree; it never writes a
        # formal-library target.
        self.archive_preprocessor = archive_preprocessor

    @staticmethod
    def _with_delivery_contract_defaults(
        delivery: Mapping[str, object],
        *,
        staging_root: str,
    ) -> dict[str, object]:
        return _contract_delivery_shape(
            delivery, lane=TIER_LOCAL_MAGNET, staging_root=staging_root,
        )

    def acquire(
        self,
        request: Mapping[str, object],
        selections: Sequence[Mapping[str, object]],
        *,
        staging_root: str,
        workspace: Path,
        alist: object,
        pause_requested: Callable[[], bool] | None = None,
    ) -> Mapping[str, object]:
        _provider_pause_checkpoint(pause_requested)
        for selection in selections:
            acquisition = selection.get("acquisition")
            if (
                str(selection.get("provider") or "").strip().casefold()
                != TIER_LOCAL_MAGNET
                or not isinstance(acquisition, Mapping)
                or str(acquisition.get("kind") or "").strip().casefold() != "torrent"
            ):
                raise AutomaticReplenishmentError(
                    "本地 Torrent materializer 只接受 magnet/torrent 候选",
                )
        method = getattr(self.delegate, "acquire", None)
        if not callable(method):
            raise AutomaticReplenishmentError("Torrent materializer 不支持 acquire")
        wrapper = {
            "request": dict(request),
            "selection": {"selections": [dict(row) for row in selections]},
            "automatic_staging_root": staging_root,
            # The coordinator, rather than an HTTP caller, derives this
            # parent from its configured library root.  Passing it explicitly
            # keeps a non-default `/quark/...` mount usable without reviving
            # a user-selectable staging path.
            "automatic_staging_parent": posixpath.dirname(posixpath.dirname(staging_root)),
        }
        guarded_alist = _PauseCheckedProviderPort(alist, pause_requested)
        result = _call_with_pause(
            method,
            wrapper,
            workspace,
            automatic=True,
            client=guarded_alist,
            pause_requested=pause_requested,
        )
        if not isinstance(result, Mapping):
            raise AutomaticReplenishmentError("Torrent materializer 返回无效")
        delivery = dict(result)
        preprocessor = self.archive_preprocessor
        preprocess = getattr(preprocessor, "prepare_provider_delivery", None)
        if not callable(preprocess):
            return self._with_delivery_contract_defaults(
                delivery, staging_root=staging_root,
            )
        _provider_pause_checkpoint(pause_requested)
        if pause_requested is None and not _accepts_keyword(preprocess, "request"):
            # Retain a tiny positional compatibility shape for focused legacy
            # test doubles only when there is no active RootJob fence.  A
            # scoped call must never retry without its callback.
            prepared = preprocess(delivery)
        else:
            prepared = _call_with_pause(
                preprocess,
                delivery,
                request=dict(request),
                staging_root=staging_root,
                workspace=workspace,
                alist=guarded_alist,
                pause_requested=pause_requested,
            )
        if not isinstance(prepared, Mapping):
            raise AutomaticReplenishmentError("归档预处理返回无效 delivery")
        return self._with_delivery_contract_defaults(
            prepared, staging_root=staging_root,
        )

    def reconcile_existing_task(
        self, request, selections, *, staging_root, workspace, alist, external_task_id,
        pause_requested: Callable[[], bool] | None = None,
    ) -> Mapping[str, object]:
        del request, selections, staging_root, workspace, alist, external_task_id, pause_requested
        raise AutomaticReplenishmentError(
            "本地 Torrent 没有可查询的 external_task_id；必须重新进入严格 tier"
        )


class QuarkFastSaveAutomaticMaterializer:
    """Use Quark share fast-save to place reviewed files in task staging."""

    _STATE_FILE = "quark_share_attempt.json"
    _STATE_FIELDS = frozenset({
        "provider",
        "attempt_id",
        "staging_root",
        "task_id",
        "locator",
        "selected_gap_ids",
        "updated_at",
    })

    def __init__(
        self,
        helper: object | None = None,
    ) -> None:
        # The loopback sidecar Helper owns the typed Quark operation.  It
        # resolves the AList Cookie internally and uses the logged-in renderer
        # only for passive WSG transforms.  Keeping this dependency injectable
        # makes the materializer deterministic in unit tests while the default
        # remains a lazy HTTP client in the API process.
        self.helper = helper

    def _helper(self) -> object:
        if self.helper is None:
            from engine.scrapeflow.quark_helper_client import (
                HttpQuarkHelperClient,
            )

            self.helper = HttpQuarkHelperClient.from_env()
        return self.helper

    @staticmethod
    def _require_helper_ready(helper: object) -> None:
        health = getattr(helper, "health", None)
        if not callable(health):
            raise AutomaticReplenishmentError("夸克 Helper 缺少 health")
        try:
            payload = health()
        except Exception as exc:
            raise AutomaticReplenishmentError("夸克 Helper health 不可用") from exc
        if not isinstance(payload, Mapping):
            raise AutomaticReplenishmentError("夸克 Helper health 返回无效")
        status = str(payload.get("status") or "").strip().casefold()
        if status not in {"ok", "ready"}:
            raise AutomaticReplenishmentError("夸克 Helper 未就绪")
        # Readiness already validates the exact action set before a pilot.  A
        # materializer also fails closed when a host helper advertises a
        # partial contract, so a share-only endpoint cannot silently become a
        # wider provider surface.
        actions = payload.get("actions")
        if actions is not None:
            if (
                not isinstance(actions, list)
                or any(not isinstance(action, str) for action in actions)
                or len(actions) != len(set(actions))
                or set(actions) != {"health", "share-save"}
            ):
                raise AutomaticReplenishmentError("夸克 Helper actions 不符合固定合同")
        else:
            raise AutomaticReplenishmentError("夸克 Helper health 缺少 actions")
        if payload.get("authenticated") is not True:
            raise AutomaticReplenishmentError("夸克 Helper 未认证")

    @staticmethod
    def _safe_share_path(value: object) -> str:
        if (
            not isinstance(value, str)
            or not value
            or value.startswith("/")
            or "\\" in value
            or posixpath.normpath(value) != value
            or any(part in {"", ".", ".."} for part in value.split("/"))
            or any(ord(char) < 32 for char in value)
        ):
            raise AutomaticReplenishmentError("夸克分享文件路径不安全")
        return value

    @classmethod
    def _share_save_plan(
        cls,
        selection: Mapping[str, object],
        *,
        destination: str,
        task_id: str | None,
    ) -> dict[str, object]:
        """Build the complete typed ``/v1/share-save`` request.

        Only reviewed file IDs, their exact source paths/sizes, selected gap
        IDs and the task staging root cross the process boundary.  AList
        cookies, delegated sessions, arbitrary destinations and the old
        Quark API transport are intentionally absent.
        """
        from engine.scrapeflow.quark_fast_save_bridge import (
            QuarkBridgeError,
            normalize_quark_fast_save_selection,
        )

        try:
            normalized = normalize_quark_fast_save_selection(selection)
        except QuarkBridgeError as exc:
            raise AutomaticReplenishmentError("夸克分享候选 manifest 无效") from exc
        acquisition = normalized.get("acquisition")
        if not isinstance(acquisition, Mapping):
            raise AutomaticReplenishmentError("夸克分享候选 acquisition 无效")
        selected = normalized.get("selected_gap_ids")
        if not isinstance(selected, list) or not selected:
            raise AutomaticReplenishmentError("夸克分享候选缺少 selected_gap_ids")
        share_id = acquisition.get("pwd_id") or acquisition.get("share_id")
        if (
            not isinstance(share_id, str)
            or not share_id
            or len(share_id) > 512
            or any(ord(char) < 32 or char in {"/", "\\"} for char in share_id)
        ):
            raise AutomaticReplenishmentError("夸克分享候选 share_id 无效")
        passcode = acquisition.get("passcode") or ""
        if (
            not isinstance(passcode, str)
            or len(passcode) > 128
            or any(ord(char) < 32 for char in passcode)
        ):
            raise AutomaticReplenishmentError("夸克分享候选 passcode 无效")
        path_map = acquisition.get("file_path_by_id")
        if not isinstance(path_map, Mapping):
            raise AutomaticReplenishmentError("夸克分享候选缺少 file_path_by_id")
        expected_raw = acquisition.get("expected_files")
        if not isinstance(expected_raw, list) or not expected_raw:
            raise AutomaticReplenishmentError("夸克分享候选缺少 expected_files")
        expected: list[dict[str, object]] = []
        for row in expected_raw:
            if not isinstance(row, Mapping):
                raise AutomaticReplenishmentError("夸克分享 expected_files 项无效")
            file_id = row.get("file_id")
            path = path_map.get(file_id) if isinstance(file_id, str) else None
            name = row.get("name")
            size = row.get("size")
            gap_ids = row.get("gap_ids")
            if (
                not isinstance(file_id, str)
                or not file_id
                or len(file_id) > 512
                or any(ord(char) < 32 or char in {"/", "\\"} for char in file_id)
                or not isinstance(name, str)
                or not name
                or name != posixpath.basename(str(path).replace("\\", "/"))
                or not isinstance(path, str)
                or cls._safe_share_path(path) != path
                or type(size) is not int
                or size <= 0
                or not isinstance(gap_ids, list)
                or not gap_ids
                or any(not isinstance(gap, str) or not gap for gap in gap_ids)
                or len(set(gap_ids)) != len(gap_ids)
            ):
                raise AutomaticReplenishmentError("夸克分享 expected_files manifest 无效")
            expected.append({
                "file_id": file_id,
                "path": path,
                "name": name,
                "size": size,
                "gap_ids": list(gap_ids),
            })
        selected_set = set(selected)
        covered = {
            gap
            for row in expected
            for gap in row["gap_ids"]
            if isinstance(gap, str)
        }
        if covered != selected_set:
            raise AutomaticReplenishmentError(
                "夸克分享 expected_files 未精确覆盖 selected_gap_ids"
            )
        output: dict[str, object] = {
            "attempt_id": cls._attempt_id(destination),
            "destination": destination,
            "share_id": share_id,
            "passcode": passcode,
            "selected_gap_ids": list(selected),
            "expected_files": expected,
        }
        title = selection.get("release_name")
        if isinstance(title, str) and title and len(title) <= 512:
            output["title"] = title
        if task_id is not None:
            output["task_id"] = task_id
        return output

    @staticmethod
    def _delivery_kind(name: str) -> str:
        suffix = Path(name).suffix.casefold()
        if suffix in _VIDEO_EXTENSIONS:
            return "video"
        if suffix in _SUBTITLE_EXTENSIONS:
            return "subtitle"
        raise AutomaticReplenishmentError("夸克分享快转返回了不支持的文件类型")

    @classmethod
    def _state_path(cls, workspace: Path) -> Path:
        return workspace / cls._STATE_FILE

    @staticmethod
    def _safe_task_id(value: object) -> str | None:
        if (
            isinstance(value, str)
            and value
            and len(value) <= 256
            and not any(char in value for char in ("/", "\\", "\x00", "\n", "\r"))
        ):
            return value
        return None

    @staticmethod
    def _selection_locator(selection: Mapping[str, object]) -> str:
        value = selection.get("locator")
        if (
            not isinstance(value, str)
            or not value
            or len(value) > _DURABLE_CANDIDATE_LOCATOR_LIMIT
            or any(char in value for char in ("\x00", "\n", "\r"))
        ):
            raise AutomaticReplenishmentError("夸克分享候选 locator 无效")
        return value

    @staticmethod
    def _selection_gap_ids(selection: Mapping[str, object]) -> list[str]:
        raw = selection.get("selected_gap_ids")
        if not isinstance(raw, list) or not raw:
            raise AutomaticReplenishmentError("夸克分享候选缺少 selected_gap_ids")
        gap_ids = [
            value for value in raw
            if isinstance(value, str) and value and len(value) <= 256
        ]
        if len(gap_ids) != len(raw) or len(set(gap_ids)) != len(gap_ids):
            raise AutomaticReplenishmentError("夸克分享候选 selected_gap_ids 无效")
        return gap_ids

    @staticmethod
    def _attempt_id(staging_root: str) -> str:
        value = posixpath.basename(staging_root.rstrip("/"))
        if not _ATTEMPT_ID_TOKEN.fullmatch(value):
            raise AutomaticReplenishmentError("夸克分享 staging attempt_id 无效")
        return value

    @staticmethod
    def _valid_updated_at(value: object) -> bool:
        if not isinstance(value, str) or not value.endswith("Z"):
            return False
        try:
            parsed = datetime.fromisoformat(value[:-1] + "+00:00")
        except ValueError:
            return False
        return parsed.tzinfo is not None

    @classmethod
    def _read_attempt_state(
        cls,
        workspace: Path,
        *,
        staging_root: str,
        selection: Mapping[str, object],
    ) -> dict[str, object]:
        path = cls._state_path(workspace)
        if path.is_symlink():
            raise AutomaticReplenishmentError("夸克分享 attempt 状态不得为软链接")
        if not path.exists():
            return {}
        try:
            stat = path.stat()
            if not path.is_file() or stat.st_size <= 0 or stat.st_size > 16_384:
                raise AutomaticReplenishmentError("夸克分享 attempt 状态大小无效")
            raw = json.loads(path.read_text(encoding="utf-8"))
        except AutomaticReplenishmentError:
            raise
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise AutomaticReplenishmentError("夸克分享 attempt 状态不可读") from exc
        if not isinstance(raw, Mapping) or set(raw) != cls._STATE_FIELDS:
            raise AutomaticReplenishmentError("夸克分享 attempt 状态结构无效")
        state = dict(raw)
        expected_attempt_id = cls._attempt_id(staging_root)
        expected_locator = cls._selection_locator(selection)
        expected_gap_ids = cls._selection_gap_ids(selection)
        task_id = cls._safe_task_id(state.get("task_id"))
        if (
            state.get("provider") != TIER_QUARK_SHARE
            or state.get("attempt_id") != expected_attempt_id
            or state.get("staging_root") != staging_root
            or state.get("locator") != expected_locator
            or state.get("selected_gap_ids") != expected_gap_ids
            or task_id is None
            or not cls._valid_updated_at(state.get("updated_at"))
        ):
            raise AutomaticReplenishmentError(
                "夸克分享 attempt 状态不属于当前候选与 staging"
            )
        state["task_id"] = task_id
        return state

    @classmethod
    def _write_attempt_state(
        cls,
        workspace: Path,
        *,
        staging_root: str,
        selection: Mapping[str, object],
        task_id: str,
    ) -> None:
        safe_task_id = cls._safe_task_id(task_id)
        if safe_task_id is None:
            raise AutomaticReplenishmentError("夸克分享 task_id 无效")
        payload = {
            "provider": TIER_QUARK_SHARE,
            "attempt_id": cls._attempt_id(staging_root),
            "staging_root": staging_root,
            "task_id": safe_task_id,
            "locator": cls._selection_locator(selection),
            "selected_gap_ids": cls._selection_gap_ids(selection),
            "updated_at": _now(),
        }
        workspace.mkdir(mode=0o700, parents=True, exist_ok=True)
        atomic_write_json(
            cls._state_path(workspace),
            payload,
            allow_nan=False,
        )

    @staticmethod
    def _in_doubt_error(message: str, *, task_id: str | None = None) -> Exception:
        from engine.scrapeflow.quark_fast_save_bridge import QuarkShareInDoubtError

        error = QuarkShareInDoubtError(message)
        if task_id is not None:
            error.task_id = task_id
        return error

    def acquire(
        self,
        request: Mapping[str, object],
        selections: Sequence[Mapping[str, object]],
        *,
        staging_root: str,
        workspace: Path,
        alist: object,
        pause_requested: Callable[[], bool] | None = None,
    ) -> Mapping[str, object]:
        _provider_pause_checkpoint(pause_requested)
        alist = _PauseCheckedProviderPort(alist, pause_requested)
        del request
        if len(selections) != 1:
            raise AutomaticReplenishmentError("夸克分享快转一次只接受一个候选")
        selection = selections[0]
        acquisition = selection.get("acquisition")
        if (
            str(selection.get("provider") or "").strip().casefold()
            != TIER_QUARK_SHARE
            or not isinstance(acquisition, Mapping)
            or str(acquisition.get("kind") or "").strip().casefold()
            != "quark_fast_save"
        ):
            raise AutomaticReplenishmentError(
                "夸克分享 materializer 只接受 quark_share/quark_fast_save 候选"
            )
        state = self._read_attempt_state(
            workspace,
            staging_root=staging_root,
            selection=selection,
        )
        existing_task_id = self._safe_task_id(state.get("task_id"))
        mkdir = getattr(alist, "mkdir", None)
        if not callable(mkdir):
            raise AutomaticReplenishmentError("AList 客户端缺少 mkdir，无法创建夸克 staging")
        mkdir(posixpath.dirname(staging_root))
        mkdir(staging_root)
        # Settle window: Quark rate-limits the same account when the AList
        # mkdir (direct egress) and the Helper's follow-up fixed requests
        # burst in the same seconds; a short pause decorrelates them.
        time.sleep(4.0)
        _provider_pause_checkpoint(pause_requested)
        helper = self._helper()
        self._require_helper_ready(helper)
        persisted_task_id = existing_task_id
        share_save = getattr(helper, "share_save", None)
        if not callable(share_save):
            raise AutomaticReplenishmentError("夸克 Helper 缺少 typed share-save")
        plan = self._share_save_plan(
            selection,
            destination=staging_root,
            task_id=existing_task_id,
        )
        _provider_pause_checkpoint(pause_requested)
        save_result = share_save(plan)
        if not isinstance(save_result, Mapping):
            raise AutomaticReplenishmentError("夸克分享快转返回无效")
        state = str(save_result.get("status") or "").strip().casefold()
        if state in {"candidate_failed", "rejected", "invalid", "expired"}:
            from engine.scrapeflow.quark_fast_save_bridge import QuarkShareExpiredError

            raise QuarkShareExpiredError("Quark Helper rejected the reviewed share candidate")
        result_task_id = self._safe_task_id(save_result.get("task_id"))
        if result_task_id is None:
            raise self._in_doubt_error(
                "夸克分享结果缺少 task_id，必须先核对再重试",
                task_id=persisted_task_id,
            )
        if persisted_task_id is not None and persisted_task_id != result_task_id:
            raise self._in_doubt_error(
                "夸克分享结果 task_id 与 attempt 状态不一致",
                task_id=persisted_task_id,
            )
        try:
            self._write_attempt_state(
                workspace,
                staging_root=staging_root,
                selection=selection,
                task_id=result_task_id,
            )
        except Exception as exc:
            error = self._in_doubt_error(
                "夸克分享任务已提交但 attempt 状态未保存",
                task_id=result_task_id,
            )
            raise error from exc
        persisted_task_id = result_task_id
        if state not in {
            "submitted", "finished", "success", "done", "ready", "completed",
        }:
            error = self._in_doubt_error(
                "Quark Helper share-save returned a non-terminal task state",
                task_id=persisted_task_id,
            )
            raise error
        rows = plan["expected_files"]
        if not isinstance(rows, list) or not rows:
            raise AutomaticReplenishmentError("夸克分享 typed manifest 为空")
        files: list[dict[str, object]] = []
        for raw in rows:
            if not isinstance(raw, Mapping):
                raise AutomaticReplenishmentError("夸克分享 expected_files 项无效")
            name = _safe_name(raw.get("name"), label="夸克分享文件")
            size = raw.get("size")
            if isinstance(size, bool) or not isinstance(size, int) or size <= 0:
                raise AutomaticReplenishmentError("夸克分享文件大小无效")
            gap_ids = raw.get("gap_ids")
            if not isinstance(gap_ids, list) or not gap_ids:
                raise AutomaticReplenishmentError("夸克分享文件缺少 gap_ids")
            files.append({
                "path": f"{staging_root}/{name}",
                "size": size,
                "kind": self._delivery_kind(name),
                "gap_ids": [
                    str(gap_id) for gap_id in gap_ids
                    if isinstance(gap_id, str) and gap_id
                ],
            })
        if any(not row["gap_ids"] for row in files):
            raise AutomaticReplenishmentError("夸克分享文件 gap_ids 无效")
        files = _isolate_cloud_delivery_videos(
            files, staging_root=staging_root, alist=alist,
        )
        result: dict[str, object] = {
            "lane": TIER_QUARK_SHARE,
            "attempt_id": posixpath.basename(staging_root.rstrip("/")),
            "staging_root": staging_root,
            "files": files,
        }
        result["external_task_id"] = result_task_id
        return result

    def reconcile_existing_task(
        self, request, selections, *, staging_root, workspace, alist, external_task_id,
        pause_requested: Callable[[], bool] | None = None,
    ) -> Mapping[str, object]:
        """Query the persisted Quark share task through the existing bridge."""
        task_id = self._safe_task_id(external_task_id)
        if task_id is None:
            raise AutomaticReplenishmentError("夸克分享 external_task_id 无效")
        if len(selections) != 1 or not isinstance(selections[0], Mapping):
            raise AutomaticReplenishmentError("夸克分享已有任务恢复候选无效")
        selection = selections[0]
        # ``acquire`` already passes the persisted id into the typed
        # share-save bridge when this state file exists.  Seed it first for
        # the crash window where the external submit returned a task id but
        # the local workspace write did not complete; this is a resume/query,
        # never a fresh submission.
        state_path = self._state_path(workspace)
        if state_path.exists():
            state = self._read_attempt_state(
                workspace, staging_root=staging_root, selection=selection,
            )
            if state.get("task_id") != task_id:
                raise AutomaticReplenishmentError(
                    "夸克分享已有 task_id 与本地 attempt 不一致"
                )
        else:
            self._write_attempt_state(
                workspace,
                staging_root=staging_root,
                selection=selection,
                task_id=task_id,
            )
        return self.acquire(
            request, selections, staging_root=staging_root, workspace=workspace,
            alist=alist, pause_requested=pause_requested,
        )


class FixedTierAutomaticMaterializer:
    """Dispatch one attempt to exactly one fixed replenishment lane."""

    def __init__(
        self,
        *,
        quark_share: AutomaticMaterializer | None = None,
        local_torrent: AutomaticMaterializer | None = None,
    ) -> None:
        self.quark_share = quark_share or QuarkFastSaveAutomaticMaterializer()
        self.local_torrent = local_torrent or LocalTorrentAutomaticMaterializer()

    def acquire(
        self,
        request: Mapping[str, object],
        selections: Sequence[Mapping[str, object]],
        *,
        staging_root: str,
        workspace: Path,
        alist: object,
        pause_requested: Callable[[], bool] | None = None,
    ) -> Mapping[str, object]:
        providers = {
            str(row.get("provider") or "").strip().casefold()
            for row in selections if isinstance(row, Mapping)
        }
        requested_tier = request.get("tier")
        if isinstance(requested_tier, str):
            tier = requested_tier.strip().casefold()
            if tier in STRICT_TIER_ORDER and providers != {tier}:
                raise AutomaticReplenishmentError(
                    "补源 bundle 必须只包含当前 tier 的候选"
                )
        if providers == {TIER_QUARK_SHARE}:
            return _call_with_pause(
                self.quark_share.acquire,
                request, selections, staging_root=staging_root,
                workspace=workspace, alist=alist, pause_requested=pause_requested,
            )
        if providers == {TIER_LOCAL_MAGNET}:
            return _call_with_pause(
                self.local_torrent.acquire,
                request, selections, staging_root=staging_root,
                workspace=workspace, alist=alist, pause_requested=pause_requested,
            )
        raise AutomaticReplenishmentError("单次补源 attempt 必须只使用一个固定 lane")

    def reconcile_existing_task(
        self, request, selections, *, staging_root, workspace, alist, external_task_id,
        pause_requested: Callable[[], bool] | None = None,
    ) -> Mapping[str, object]:
        providers = {
            str(row.get("provider") or "").strip().casefold()
            for row in selections if isinstance(row, Mapping)
        }
        delegate = (
            self.quark_share if providers == {TIER_QUARK_SHARE}
            else self.local_torrent if providers == {TIER_LOCAL_MAGNET}
            else None
        )
        method = getattr(delegate, "reconcile_existing_task", None)
        if not callable(method):
            raise AutomaticReplenishmentError("当前补源 materializer 不支持 external_task_id 恢复")
        return _call_with_pause(
            method,
            request, selections, staging_root=staging_root, workspace=workspace,
            alist=alist, external_task_id=external_task_id,
            pause_requested=pause_requested,
        )


def _now() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def _safe_path(value: object, *, label: str, allow_root: bool = False) -> str:
    if not isinstance(value, str) or not value.startswith("/") or "\x00" in value or "\\" in value:
        raise AutomaticReplenishmentError(f"{label} 不是安全的绝对路径")
    normalized = posixpath.normpath(value)
    if normalized != value or (not allow_root and normalized == "/"):
        raise AutomaticReplenishmentError(f"{label} 不是规范化路径")
    if any(part in {"", ".", ".."} for part in normalized.split("/")[1:]):
        raise AutomaticReplenishmentError(f"{label} 含有不安全路径段")
    return normalized


def _safe_name(value: object, *, label: str) -> str:
    if not isinstance(value, str) or not value or value in {".", ".."}:
        raise AutomaticReplenishmentError(f"{label} 文件名无效")
    if "/" in value or "\\" in value or "\x00" in value:
        raise AutomaticReplenishmentError(f"{label} 文件名不安全")
    return value


def _gap_file_name(gap_id: str) -> str:
    slug = _GAP_SLUG.sub("-", gap_id).strip(".-")[:96]
    # Gap state is durable task state, not an event log.  A stable name lets a
    # retry update the same record instead of creating one file every 30
    # seconds and eventually making the state directory look like a second
    # provider queue.
    return f"{slug or 'gap'}.json"


def reconcile_interrupted_gap_states(
    state_root: str | Path,
    job_id: str,
    *,
    error: str,
) -> int:
    """Turn only orphaned in-flight local gap records into ``retry_wait``.

    This is deliberately local-state-only.  A forced API recreation has no
    live Python future to own a persisted ``acquiring`` record; reporting it
    as still active would be misleading, while touching AList to clean it up
    would violate a global pause.
    """
    safe_job = _GAP_SLUG.sub("-", job_id).strip(".-")[:96] or "job"
    directory = Path(state_root).resolve() / "gaps" / safe_job
    try:
        paths = sorted(path for path in directory.glob("*.json") if path.is_file())
    except OSError:
        return 0
    updated = 0
    for path in paths:
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError):
            continue
        if not isinstance(raw, Mapping) or str(raw.get("phase") or "") not in _INTERRUPTED_GAP_PHASES:
            continue
        state = dict(raw)
        gap = state.get("gap") if isinstance(state.get("gap"), Mapping) else {}
        if str(gap.get("kind") or "") == "missing_subtitle":
            # Reconciliation after a process restart must not resurrect video
            # tier/candidate evidence on a sidecar row.
            for key in (
                "tier", "tier_status", "candidate_failures_by_provider",
                "exhaustion_proof_by_provider", "excluded_candidates",
                "resolved",
            ):
                state.pop(key, None)
            state.update({
                "lane": "subtitle",
                "phase": "retry_wait",
                "updated_at": _now(),
                "error": redact_error(error),
                "last_error_scope": FAILURE_INFRASTRUCTURE,
                "active_attempt": None,
                "external_task_id": None,
                "next_retry_at": None,
            })
        else:
            state.update({
                "phase": "retry_wait",
                "updated_at": _now(),
                "error": redact_error(error),
                "last_error_scope": FAILURE_INFRASTRUCTURE,
                "next_retry_at": None,
            })
        try:
            redacted = redact_value(state)
            atomic_write_json(
                path,
                dict(redacted) if isinstance(redacted, Mapping) else state,
                allow_nan=False,
            )
        except OSError:
            continue
        updated += 1
    return updated


@dataclass(frozen=True, slots=True)
class StagingFile:
    path: str
    size: int
    kind: str

    def as_dict(self) -> dict[str, object]:
        return {"path": self.path, "size": self.size, "kind": self.kind}


class AutomaticReplenishmentRuntime:
    """Run one provider attempt end-to-end for Engine resource gaps."""

    def __init__(
        self,
        state_root: str | Path,
        *,
        engine_runner: SimpleEngineRunner,
        alist: object,
        search: AutomaticProviderSearch,
        materializer: AutomaticMaterializer,
        subtitle_materializer: object | None = None,
        staging_root: str = CANONICAL_REPLENISHMENT_STAGING_ROOT,
        max_candidate_rounds: int = 3,
        progress: Callable[[EngineJob, str, Mapping[str, object]], None] | None = None,
        cancel_requested: Callable[[EngineJob], bool] | None = None,
        pause_requested: Callable[[EngineJob], bool] | None = None,
        remote_video_probe: Callable[[object, str], Mapping[str, object]] | None = None,
    ) -> None:
        self.state_root = Path(state_root).resolve()
        self.engine_runner = engine_runner
        self.alist = alist
        self.search = search
        self.materializer = materializer
        # Kept only as a constructor compatibility slot for callers that still
        # inject the retired EngineJob subtitle provider.  This runtime never
        # invokes it: RootJob owns the sole automatic subtitle transaction.
        self.subtitle_materializer = subtitle_materializer
        try:
            self.staging_root = validate_provider_staging_root(staging_root)
        except ProviderStagingPathError as exc:
            raise AutomaticReplenishmentError(
                "自动补源 staging_root 必须是生产根或受限验收根派生的补源目录"
            ) from exc
        # The strict policy advances only after thirty distinct
        # candidate-local failures.  A caller may choose a smaller execution
        # slice, but the runtime must not silently cap a configured thirty
        # candidate proof at the historical twelve-round limit.
        self.max_candidate_rounds = max(
            1, min(EXHAUSTION_MIN_DISTINCT_LOCATORS, int(max_candidate_rounds)),
        )
        self.progress = progress
        self.remote_video_probe = remote_video_probe or probe_remote_video_stream
        # This intentionally remains a cooperative boundary.  It cannot
        # safely interrupt an already-running downloader, but it prevents a
        # stopped pilot from starting another provider round or formal write.
        self.cancel_requested = cancel_requested
        # Pause is kept separate from the provider pilot/cancel predicate so
        # a child Engine operation remains resumable rather than becoming a
        # terminal cancellation when the operator pauses the process.
        self.pause_requested = pause_requested
        self.gaps_root = self.state_root / "gaps"
        self.workspace_root = self.state_root / "staging"
        for root in (self.gaps_root, self.workspace_root):
            root.mkdir(mode=0o700, parents=True, exist_ok=True)

    def _progress(self, job: EngineJob, phase: str, **details: object) -> None:
        """Publish best-effort progress for the root job."""
        callback = self.progress
        if callback is None:
            return
        try:
            redacted = redact_value(dict(details))
            callback(
                job,
                phase,
                dict(redacted) if isinstance(redacted, Mapping) else dict(details),
            )
        except Exception:
            return

    def _raise_if_cancelled(
        self,
        job: EngineJob,
        *,
        round_number: int,
        boundary: str,
    ) -> None:
        """Stop before a new external/provider write boundary when requested.

        The application supplies the predicate from its persisted pause
        control and current pilot allowlist.  A predicate failure is also
        fail-closed: continuing from a malformed/unknown control state could
        otherwise widen a deliberately bounded provider run.
        """
        # Keep pause separate from cancellation.  Every existing provider
        # operation boundary already funnels through this helper; checking the
        # dedicated predicate first gives a paused child the resumable
        # ``AutomaticReplenishmentPaused`` path instead of cancelling it.
        self._raise_if_paused(
            job, round_number=round_number, boundary=boundary,
        )
        predicate = self.cancel_requested
        if predicate is None:
            return
        try:
            requested = bool(predicate(job))
        except Exception as exc:
            message = "自动补源取消条件不可用，已停止当前尝试"
            self._progress(
                job, "retry_wait", round=round_number, error=message,
                cancellation_boundary=boundary,
            )
            raise AutomaticReplenishmentCancelled(message) from exc
        if requested:
            message = "自动补源已暂停或不在当前试点范围"
            self._progress(
                job, "retry_wait", round=round_number, error=message,
                cancellation_boundary=boundary,
            )
            raise AutomaticReplenishmentCancelled(message)

    def _raise_if_paused(
        self,
        job: EngineJob,
        *,
        round_number: int,
        boundary: str,
    ) -> None:
        predicate = self.pause_requested
        if predicate is None:
            return
        try:
            paused = bool(predicate(job))
        except Exception as exc:
            message = "自动补源暂停状态不可确认，已停止当前尝试"
            self._progress(job, "retry_wait", round=round_number, error=message,
                           cancellation_boundary=boundary)
            raise AutomaticReplenishmentPaused(message) from exc
        if paused:
            message = "自动补源已暂停；保留 child 供恢复检查"
            self._progress(job, "retry_wait", round=round_number, error=message,
                           cancellation_boundary=boundary)
            raise AutomaticReplenishmentPaused(message)

    def _provider_pause_callback(
        self,
        job: EngineJob,
        *,
        round_number: int,
        boundary: str,
    ) -> Callable[[], bool] | None:
        """Adapt runtime control to provider/archive callback shape.

        The provider port asks this callback immediately before its own AList,
        local-staging, downloader, or archive effect.  It combines the global
        pause fence and the root/pilot cancellation fence; supplying only the
        former would let a withdrawn single-root pilot continue mutating its
        task staging.
        """
        if self.pause_requested is None and self.cancel_requested is None:
            return None

        def checkpoint() -> bool:
            try:
                self._raise_if_cancelled(
                    job,
                    round_number=round_number,
                    boundary=boundary,
                )
            except AutomaticReplenishmentCancelled:
                # The lower provider/engine protocols use a boolean result.
                # The durable helper above already recorded why it stopped;
                # returning True preserves that control signal at every
                # downstream boundary instead of reclassifying it as a
                # provider failure.
                return True
            return False

        return checkpoint

    def _plan_internal_child(
        self,
        request: Mapping[str, object],
        *,
        root_job_id: str,
        pause_requested: Callable[[], bool] | None = None,
    ) -> EngineJob:
        """Persist a provider attempt as an internal child of the root job."""
        planner = getattr(self.engine_runner, "plan_job", None)
        if not callable(planner):
            raise AutomaticReplenishmentError("Engine runner 不支持 child plan")
        if pause_requested is None:
            child = planner(request, internal_child_of=root_job_id)
        elif _accepts_keyword(planner, "pause_requested"):
            child = planner(
                request,
                internal_child_of=root_job_id,
                pause_requested=pause_requested,
            )
        else:
            raise AutomaticReplenishmentError(
                "Engine child planner 不支持 pause_requested；拒绝在 RootJob 试运行范围执行",
            )
        if not isinstance(child, EngineJob):
            raise AutomaticReplenishmentError("Engine child plan 返回无效")
        return child

    @staticmethod
    def _audit_child_target_matches(root: EngineJob, child: EngineJob) -> bool:
        """Fail closed if an audit-owned child would write another work root."""
        root_summary = root.summary if isinstance(root.summary, Mapping) else {}
        if root_summary.get("audit_owned") is not True:
            return True
        root_plan = root.plan if isinstance(root.plan, Mapping) else {}
        root_metadata = root_plan.get("metadata") if isinstance(root_plan.get("metadata"), Mapping) else {}
        expected = (
            root_metadata.get("series_root")
            or root_metadata.get("target_root")
            or root_plan.get("target_root")
            or root_summary.get("target_root")
        )
        child_plan = child.plan if isinstance(child.plan, Mapping) else {}
        child_metadata = child_plan.get("metadata") if isinstance(child_plan.get("metadata"), Mapping) else {}
        actual = (
            child_metadata.get("series_root")
            or child_metadata.get("target_root")
            or child_plan.get("target_root")
        )
        return isinstance(expected, str) and isinstance(actual, str) and expected == actual

    @staticmethod
    def _child_inherits_target_shelf(root: EngineJob, child: EngineJob) -> bool:
        """Keep a provider child inside its root's user-confirmed shelf.

        Audit-owned legacy roots predate the intake selection gate and retain
        their existing work-root validation above. Ordinary selected roots
        must carry exactly the same semantic shelf and fixed first-level root
        into every child request/record; copying only a derived work path is
        not enough to prove the user choice survived the provider boundary.
        """
        if root.target_shelf is None and root.target_root is None:
            # Roots written before the start-gate schema have no durable
            # shelf fields.  Keep the pre-gate work-root check above as the
            # compatibility boundary; newly registered public roots always
            # carry both fields before a provider child can be created.
            return True
        return (
            isinstance(root.target_shelf, str)
            and isinstance(root.target_root, str)
            and child.target_shelf == root.target_shelf
            and child.target_root == root.target_root
        )

    @staticmethod
    def _request_inherits_target_shelf(
        root: EngineJob,
        request: Mapping[str, object],
    ) -> bool:
        """Check shelf coordinates before asking Engine to persist a child.

        The post-plan ``EngineJob`` check remains the durable fail-closed
        boundary.  This earlier request check closes the smaller window in
        which a custom/legacy runner could persist a mismatched child and only
        then report the mismatch to the replenishment runtime.
        """
        if root.target_shelf is None and root.target_root is None:
            return True
        return (
            isinstance(root.target_shelf, str)
            and isinstance(root.target_root, str)
            and request.get("target_shelf") == root.target_shelf
            and request.get("parent_path") == root.target_root
        )

    def _write_gap(self, state: Mapping[str, object], existing_path: Path | None = None) -> Path:
        path = existing_path
        if path is None:
            gap_id = str(state.get("id") or "gap")
            job_id = str(state.get("job_id") or "job")
            path = self.gaps_root / _GAP_SLUG.sub("-", job_id).strip(".-") / _gap_file_name(gap_id)
        path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        redacted = redact_value(dict(state))
        atomic_write_json(
            path,
            dict(redacted) if isinstance(redacted, Mapping) else dict(state),
            allow_nan=False,
        )
        return path

    def _gap_path(self, *, job_id: str, gap_id: str) -> Path:
        """Return the one durable state file for a job/gap pair."""
        safe_job = _GAP_SLUG.sub("-", job_id).strip(".-")[:96] or "job"
        return self.gaps_root / safe_job / _gap_file_name(gap_id)

    @staticmethod
    def _safe_attempt_id(value: object) -> str | None:
        if isinstance(value, str) and _ATTEMPT_ID_TOKEN.fullmatch(value):
            return value
        return None

    @staticmethod
    def _safe_external_task_id(value: object) -> str | None:
        if (
            isinstance(value, str)
            and value
            and len(value) <= 256
            and not any(char in value for char in ("/", "\\", "\x00", "\n", "\r"))
        ):
            return value
        return None

    @staticmethod
    def _safe_replenishment_job_id(value: object) -> str | None:
        """Return a job id safe to bind to one provider staging subtree."""
        if isinstance(value, str) and _JOB_ID_TOKEN.fullmatch(value):
            return value
        return None

    @staticmethod
    def _parse_utc_timestamp(value: object) -> datetime | None:
        """Parse the existing audit/state UTC timestamp representation."""
        if not isinstance(value, str) or not value.endswith("Z"):
            return None
        try:
            parsed = datetime.fromisoformat(value[:-1] + "+00:00")
        except ValueError:
            return None
        if parsed.tzinfo is None:
            return None
        return parsed.astimezone(UTC)

    @classmethod
    def _post_acquisition_reaudit_is_pending(cls, state: Mapping[str, object]) -> bool:
        """Whether one gap still owns staging pending a post-write audit.

        A malformed marker is deliberately pending as well.  Treating an
        unreadable/unknown lifecycle marker as permission to delete the
        attempt would make a damaged JSON record a cleanup bypass.
        """
        if _POST_ACQUISITION_REAUDIT_KEY not in state:
            return False
        raw = state.get(_POST_ACQUISITION_REAUDIT_KEY)
        if not isinstance(raw, Mapping):
            return True
        return str(raw.get("status") or "").casefold() != "cleaned"

    @classmethod
    def _post_acquisition_reaudit_blocks_provider(
        cls, state: Mapping[str, object],
    ) -> bool:
        """Whether a re-audit marker must prevent a fresh provider attempt.

        ``gap_still_actionable`` is different from an in-flight/uncertain
        marker: the scoped audit has completed and explicitly returned the gap
        to node M of the flow.  Keep its old staging for evidence, but allow
        the independent lane to make a bounded next attempt.
        """
        if _POST_ACQUISITION_REAUDIT_KEY not in state:
            return False
        raw = state.get(_POST_ACQUISITION_REAUDIT_KEY)
        if not isinstance(raw, Mapping):
            return True
        status = str(raw.get("status") or "").casefold()
        if status == "gap_still_actionable":
            return False
        return status != "cleaned"

    def _coerce_post_acquisition_reaudit(
        self,
        value: object,
        *,
        job_id: str,
        gap_id: str,
    ) -> dict[str, object] | None:
        """Validate one durable post-acquisition re-audit marker.

        The marker is the only authority that permits removal of a provider
        attempt after a successful child.  Keep its parser deliberately
        narrow: a corrupted marker remains pending, but can never choose an
        arbitrary AList path for cleanup.
        """
        if not isinstance(value, Mapping):
            return None
        status = str(value.get("status") or "").casefold()
        if status not in _POST_ACQUISITION_REAUDIT_PENDING_STATUSES | {"cleaned"}:
            return None
        attempt_id = self._safe_attempt_id(value.get("attempt_id"))
        if attempt_id is None:
            return None
        try:
            staging_root = _safe_path(
                value.get("staging_root"), label="post-acquisition staging",
            )
        except AutomaticReplenishmentError:
            return None
        expected_prefix = f"{self.staging_root}/{job_id}/"
        if (
            not staging_root.startswith(expected_prefix)
            or posixpath.basename(staging_root) != attempt_id
        ):
            return None
        raw_gap_ids = value.get("selected_gap_ids")
        if (
            not isinstance(raw_gap_ids, list)
            or not raw_gap_ids
            or len(raw_gap_ids) > _POST_ACQUISITION_REAUDIT_MAX_GAPS
        ):
            return None
        selected_gap_ids: list[str] = []
        for raw_gap_id in raw_gap_ids:
            if (
                not isinstance(raw_gap_id, str)
                or not raw_gap_id
                or len(raw_gap_id) > 256
                or any(char in raw_gap_id for char in ("/", "\\", "\x00", "\n", "\r"))
            ):
                return None
            selected_gap_ids.append(raw_gap_id)
        if len(selected_gap_ids) != len(set(selected_gap_ids)) or gap_id not in selected_gap_ids:
            return None
        requested_at = self._parse_utc_timestamp(value.get("requested_at"))
        if requested_at is None:
            return None
        record: dict[str, object] = {
            "status": status,
            "attempt_id": attempt_id,
            "staging_root": staging_root,
            "selected_gap_ids": sorted(selected_gap_ids),
            "requested_at": str(value["requested_at"]),
        }
        child_job_id = self._safe_replenishment_job_id(value.get("child_job_id"))
        if child_job_id is not None:
            record["child_job_id"] = child_job_id
        cleanup_attempts = value.get("cleanup_attempts")
        if (
            isinstance(cleanup_attempts, int)
            and not isinstance(cleanup_attempts, bool)
            and 0 <= cleanup_attempts <= 5
        ):
            record["cleanup_attempts"] = cleanup_attempts
        for key in ("last_audit_started_at", "cleaned_at"):
            if self._parse_utc_timestamp(value.get(key)) is not None:
                record[key] = str(value[key])
        error = value.get("error")
        if isinstance(error, str) and error:
            record["error"] = redact_error(error)
        return record

    @staticmethod
    def _copy_durable_defaults() -> dict[str, object]:
        defaults: dict[str, object] = {}
        for key, value in _DURABLE_GAP_STATE_DEFAULTS.items():
            defaults[key] = dict(value) if isinstance(value, dict) else value
        return defaults

    @staticmethod
    def _provider_failure_map(value: object) -> dict[str, list[str]]:
        output: dict[str, list[str]] = {}
        if not isinstance(value, Mapping):
            return output
        for provider, rows in value.items():
            if provider not in STRICT_TIER_ORDER or not isinstance(rows, list):
                continue
            locators = [
                item for item in rows
                if isinstance(item, str)
                and item
                and len(item) <= _DURABLE_CANDIDATE_LOCATOR_LIMIT
            ]
            if locators:
                output[str(provider)] = sorted(set(locators))[-_DURABLE_CANDIDATE_EXCLUSION_LIMIT:]
        return output

    @staticmethod
    def _exhaustion_proof_map(value: object) -> dict[str, dict[str, object]]:
        output: dict[str, dict[str, object]] = {}
        if not isinstance(value, Mapping):
            return output
        for provider, proof in value.items():
            if provider in STRICT_TIER_ORDER and isinstance(proof, Mapping):
                output[str(provider)] = dict(proof)
        return output

    @staticmethod
    def _tier_from_state(state: Mapping[str, object]) -> str:
        """Read one durable tier without deriving it from a candidate.

        A damaged/legacy gap record falls back to the policy's initial state,
        never to the lowest provider returned by search.  This keeps recovery
        conservative while the pure policy remains the only transition rule.
        """
        raw = state.get("tier")
        if isinstance(raw, str):
            tier = raw.strip().casefold()
            if tier in STRICT_TIER_ORDER:
                return tier
        return str(initial_tier_state()["tier"])

    def _current_tier_for_gap_states(
        self,
        gap_state_paths: Mapping[str, Path],
    ) -> str:
        """Return the earliest durable tier shared by this root attempt.

        A request may contain several gaps.  They must never be materialized
        by a mixed tier bundle.  New states are aligned by every policy update;
        if an interrupted legacy run left them divergent, choose the earliest
        lane so no gap can skip a required upstream tier.
        """
        tiers: list[str] = []
        for path in dict.fromkeys(gap_state_paths.values()):
            try:
                raw = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise AutomaticReplenishmentError("补源 gap 状态不可读") from exc
            if isinstance(raw, Mapping):
                tiers.append(self._tier_from_state(raw))
        if not tiers:
            return str(initial_tier_state()["tier"])
        return min(tiers, key=STRICT_TIER_ORDER.index)

    @staticmethod
    def _waiting_reconcile_gap_states(
        gap_state_paths: Mapping[str, Path],
    ) -> bool:
        """Do not resubmit an external task whose outcome is ambiguous."""
        for path in dict.fromkeys(gap_state_paths.values()):
            try:
                raw = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, UnicodeDecodeError, json.JSONDecodeError):
                # The ordinary state loader will fail closed with a useful
                # message below; a damaged file cannot prove a safe resubmit.
                return True
            if not isinstance(raw, Mapping):
                return True
            if (
                raw.get("tier_status") == "waiting_reconcile"
                or raw.get("last_error_scope") == FAILURE_IN_DOUBT
            ):
                return True
        return False

    @staticmethod
    def _copy_tier_policy_fields(
        state: dict[str, object],
        policy_state: Mapping[str, object],
    ) -> None:
        """Project the pure tier result into one durable gap record."""
        for key in (
            "tier",
            "candidate_failures_by_provider",
            "exhaustion_proof_by_provider",
            "last_error_scope",
            "external_task_id",
        ):
            if key in policy_state:
                state[key] = policy_state[key]
        status = policy_state.get("status")
        if isinstance(status, str) and status:
            state["tier_status"] = status

    def _apply_tier_outcome_to_gap_states(
        self,
        gap_state_paths: Mapping[str, Path],
        *,
        outcome: Mapping[str, object],
        updates: Mapping[str, object] | None = None,
    ) -> list[dict[str, object]]:
        """Apply one pure transition to every gap in the current bundle.

        The caller has already constrained selection to one current tier.  A
        shared outcome therefore updates every participating gap identically,
        preserving one tier for the whole materialization attempt.
        """
        results: list[dict[str, object]] = []
        for path in dict.fromkeys(gap_state_paths.values()):
            try:
                raw = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise AutomaticReplenishmentError("补源 gap 状态不可读") from exc
            if not isinstance(raw, Mapping):
                raise AutomaticReplenishmentError("补源 gap 状态格式无效")
            state = dict(raw)
            try:
                policy_state = apply_tier_outcome(state, outcome)
            except ReplenishmentTierError as exc:
                raise AutomaticReplenishmentError("补源 tier 状态无效") from exc
            self._copy_tier_policy_fields(state, policy_state)
            if updates:
                state.update(dict(updates))
            if "updated_at" not in (updates or {}):
                state["updated_at"] = _now()
            self._write_gap(state, path)
            results.append(policy_state)
        return results

    @staticmethod
    def _source_name(value: object) -> str:
        if not isinstance(value, str):
            return ""
        return re.sub(r"[^a-z0-9]+", "", value.casefold())

    @staticmethod
    def _shelf_for_request(request: Mapping[str, object]) -> str | None:
        """Derive the owning first-level shelf from the request work root.

        The formal-library work root always sits below exactly one of the
        three shelf directories.  Ambiguous or unrecognizable paths return
        None so the pure tier policy keeps its conservative full
        required-source behavior.
        """
        media = request.get("media")
        if not isinstance(media, Mapping):
            return None
        target_root = media.get("target_root")
        if not isinstance(target_root, str) or not target_root.startswith("/"):
            return None
        shelves = {
            shelf.value
            for segment in target_root.split("/")
            if (shelf := target_shelf_for_shelf_segment(segment)) is not None
        }
        if len(shelves) == 1:
            return next(iter(shelves))
        return None

    def _search_tier_outcome(
        self,
        result: Mapping[str, object],
        selection_bundle: Mapping[str, object],
        *,
        tier: str,
        shelf: str | None = None,
    ) -> dict[str, object]:
        """Translate read-only search evidence into the pure policy schema."""
        required = required_sources_for_tier(tier, shelf)
        completed = {
            self._source_name(value)
            for value in result.get("completed_sources", [])
            if self._source_name(value)
        } if isinstance(result.get("completed_sources"), list) else set()
        infrastructure_failure = (
            str(result.get("failure_scope") or "").strip().casefold()
            == FAILURE_INFRASTRUCTURE
        )
        telemetry = result.get("source_telemetry")
        if isinstance(telemetry, Mapping):
            for source, evidence in telemetry.items():
                name = self._source_name(source)
                if name not in required:
                    continue
                if not isinstance(evidence, Mapping):
                    continue
                raw_failures = evidence.get("infrastructure_failures", 0)
                failures = (
                    raw_failures if isinstance(raw_failures, int)
                    and not isinstance(raw_failures, bool) else 0
                )
                if failures > 0:
                    infrastructure_failure = True
                    continue
                if evidence.get("source_exhausted") is True:
                    completed.add(name)

        raw_unchecked = selection_bundle.get(
            "unchecked_current_tier_candidate_count", 0,
        )
        unchecked = (
            raw_unchecked if isinstance(raw_unchecked, int)
            and not isinstance(raw_unchecked, bool) and raw_unchecked >= 0 else 1
        )
        explicit_unchecked = result.get("unchecked_secondary_candidates")
        if (
            isinstance(explicit_unchecked, int)
            and not isinstance(explicit_unchecked, bool)
            and explicit_unchecked >= 0
        ):
            unchecked = max(unchecked, explicit_unchecked)
        raw_eligible = selection_bundle.get(
            "eligible_current_tier_candidate_count", 0,
        )
        eligible = (
            raw_eligible if isinstance(raw_eligible, int)
            and not isinstance(raw_eligible, bool) and raw_eligible >= 0 else 1
        )
        search_complete = (
            result.get("search_complete_no_candidates") is True
            or result.get("search_complete") is True
        )
        no_candidates = eligible == 0 and unchecked == 0
        outcome: dict[str, object] = {
            "scope": (
                FAILURE_INFRASTRUCTURE
                if infrastructure_failure else FAILURE_CANDIDATE
            ),
            "search_complete_no_candidates": bool(
                search_complete and no_candidates and not infrastructure_failure
            ),
            "completed_sources": sorted(completed),
            "unchecked_secondary_candidates": unchecked,
        }
        if shelf is not None:
            outcome["shelf"] = shelf
        return outcome

    def _active_attempt_record(
        self,
        *,
        job_id: str,
        attempt_id: str,
        staging_root: str,
        workspace: Path,
        selections: Sequence[Mapping[str, object]],
        external_task_id: object | None = None,
    ) -> dict[str, object]:
        providers = sorted({
            str(row.get("provider") or "").strip().casefold()
            for row in selections
            if isinstance(row, Mapping)
            and 0 < len(str(row.get("provider") or "").strip()) <= _DURABLE_CANDIDATE_PROVIDER_LIMIT
        })
        markers: list[str] = []
        seen: set[str] = set()
        for selection in selections:
            normalized = self._normalized_excluded_selection(selection)
            if normalized is None:
                continue
            value = normalized.get("infohash") or normalized.get("locator")
            if isinstance(value, str) and value and value not in seen:
                seen.add(value)
                markers.append(value)
        record: dict[str, object] = {
            "attempt_id": attempt_id,
            "staging_root": staging_root,
            "workspace": str(workspace.resolve()),
            "providers": providers,
            "locators": markers[:8],
        }
        snapshot = self._selection_snapshot(selections)
        if snapshot:
            record["selections"] = snapshot
        task_id = self._safe_external_task_id(external_task_id)
        if task_id is not None:
            record["external_task_id"] = task_id
        return record

    @staticmethod
    def _selection_snapshot(
        selections: Sequence[Mapping[str, object]],
    ) -> list[dict[str, object]]:
        """Keep a bounded, JSON-only copy for external-task recovery.

        This is deliberately a reviewed-candidate snapshot, not a general
        catalog.  It is capped per row and in total, and only fields consumed
        by the existing materializers cross the restart boundary.
        """
        allowed = {
            "provider", "locator", "infohash", "release_name", "title", "year",
            "files", "file_coverage", "resolution", "quality", "coverage",
            "selected_gap_ids", "acquisition",
        }
        output: list[dict[str, object]] = []
        total = 0
        for raw in selections:
            if not isinstance(raw, Mapping):
                continue
            candidate = {key: raw[key] for key in allowed if key in raw}
            try:
                encoded = json.dumps(candidate, ensure_ascii=False, allow_nan=False)
            except (TypeError, ValueError):
                continue
            if not encoded or len(encoded.encode("utf-8")) > 64 * 1024:
                continue
            total += len(encoded.encode("utf-8"))
            if total > _SELECTION_SNAPSHOT_MAX_BYTES:
                break
            output.append(json.loads(encoded))
            if len(output) >= 8:
                break
        return output

    @property
    def _candidate_memory_path(self) -> Path:
        """One task-state-owned positive candidate ledger.

        It intentionally lives beside (not inside) a gap record: the same
        verified release can cover several later audit projections, while the
        scope coordinate below prevents it crossing work identity or tier.
        """
        return self.state_root / _CANDIDATE_MEMORY_FILE

    @staticmethod
    def _candidate_memory_entry_key(entry: Mapping[str, object]) -> tuple[str, ...] | None:
        scope = entry.get("scope")
        candidate = entry.get("candidate")
        if not isinstance(scope, Mapping) or not isinstance(candidate, Mapping):
            return None
        identity = scope.get("identity")
        tier = str(scope.get("tier") or "").strip().casefold()
        provider = str(candidate.get("provider") or "").strip().casefold()
        locator = str(candidate.get("locator") or "").strip()
        if not isinstance(identity, Mapping) or not tier or not provider or not locator:
            return None
        try:
            identity_key = json.dumps(
                dict(identity), ensure_ascii=False, sort_keys=True,
                separators=(",", ":"), allow_nan=False,
            )
        except (TypeError, ValueError):
            return None
        return (identity_key, tier, provider, locator)

    @classmethod
    def _candidate_memory_file_entries(cls, raw: object) -> list[dict[str, object]]:
        """Parse the bounded memory envelope without replaying arbitrary JSON."""
        if not isinstance(raw, Mapping) or raw.get("version") != _CANDIDATE_MEMORY_VERSION:
            return []
        entries = raw.get("entries")
        if not isinstance(entries, list):
            return []
        output: list[dict[str, object]] = []
        for value in entries[:_CANDIDATE_MEMORY_MAX_ENTRIES]:
            if not isinstance(value, Mapping):
                continue
            scope = value.get("scope")
            candidate = value.get("candidate")
            verified_at = value.get("verified_at")
            verified_gap_ids = value.get("verified_gap_ids")
            if (
                not isinstance(scope, Mapping)
                or not isinstance(candidate, Mapping)
                or not isinstance(verified_at, str)
                or not isinstance(verified_gap_ids, list)
                or not verified_gap_ids
                or len(verified_gap_ids) > _CANDIDATE_MEMORY_MAX_GAPS_PER_ENTRY
                or any(
                    not isinstance(item, str) or not item or len(item) > 256
                    or any(char in item for char in ("/", "\\", "\x00", "\n", "\r"))
                    for item in verified_gap_ids
                )
            ):
                continue
            normalized = normalize_reusable_candidate(candidate)
            if normalized is None:
                continue
            # Candidate memory is never an execution result by itself.  The
            # selector still recomputes identity/name/file coverage below.
            normalized["_memory_reused"] = True
            normalized["memory_verified_at"] = verified_at
            normalized["memory_verified_gap_ids"] = sorted(set(verified_gap_ids))
            output.append({
                "scope": dict(scope),
                "candidate": normalized,
                "verified_at": verified_at,
                "verified_gap_ids": sorted(set(verified_gap_ids)),
            })
        return output

    def _load_candidate_memory(
        self,
        request: Mapping[str, object],
        *,
        tier: str,
    ) -> list[dict[str, object]]:
        """Load only positive rows matching this work, tier and live gaps."""
        scope = reusable_candidate_scope(request, tier=tier)
        if scope is None:
            return []
        path = self._candidate_memory_path
        try:
            if path.is_symlink() or not path.is_file() or path.stat().st_size > _CANDIDATE_MEMORY_MAX_BYTES:
                return []
            raw = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError):
            return []
        current_identity = scope.get("identity")
        current_gap_ids = set(scope.get("gap_ids") or [])
        output: list[dict[str, object]] = []
        for entry in self._candidate_memory_file_entries(raw):
            entry_scope = entry.get("scope")
            candidate = entry.get("candidate")
            if not isinstance(entry_scope, Mapping) or not isinstance(candidate, Mapping):
                continue
            if (
                str(entry_scope.get("tier") or "").strip().casefold() != tier
                or entry_scope.get("identity") != current_identity
            ):
                continue
            remembered_scope_gaps = {
                item for item in entry_scope.get("gap_ids", [])
                if isinstance(item, str)
            } if isinstance(entry_scope.get("gap_ids"), list) else set()
            verified_gaps = {
                item for item in entry.get("verified_gap_ids", [])
                if isinstance(item, str)
            }
            if not (current_gap_ids & remembered_scope_gaps & verified_gaps):
                continue
            # Keep only the currently requested coordinates.  This is useful
            # when one remembered pack covered multiple seasons but today’s
            # audit asks for just one episode.
            candidate = dict(candidate)
            candidate["memory_verified_gap_ids"] = sorted(
                current_gap_ids & remembered_scope_gaps & verified_gaps
            )
            output.append(candidate)
        return output

    def _remember_verified_candidates(
        self,
        request: Mapping[str, object],
        *,
        tier: str,
        selections: Sequence[Mapping[str, object]],
        resolved_gap_ids: set[str],
    ) -> None:
        """Best-effort persist candidates proven by an executed child.

        This ledger is an optimization.  A failed write must never turn a
        completed formal child into a provider failure; fresh search remains
        the correctness path.  Only normalized, credential-free rows enter
        the file and every later load revalidates the same schema.
        """
        scope = reusable_candidate_scope(request, tier=tier)
        if scope is None or not resolved_gap_ids:
            return
        new_entries: list[dict[str, object]] = []
        for raw in selections:
            if not isinstance(raw, Mapping):
                continue
            candidate = normalize_reusable_candidate(raw)
            if candidate is None:
                continue
            selected = {
                item for item in raw.get("selected_gap_ids", [])
                if isinstance(item, str)
            } if isinstance(raw.get("selected_gap_ids"), list) else set()
            verified = sorted(selected & resolved_gap_ids)
            if not verified:
                continue
            candidate["memory_verified_at"] = _now()
            candidate["memory_verified_gap_ids"] = verified
            new_entries.append({
                "scope": dict(scope),
                "candidate": candidate,
                "verified_at": candidate["memory_verified_at"],
                "verified_gap_ids": verified,
            })
        if not new_entries:
            return
        path = self._candidate_memory_path
        try:
            raw_existing: object = {}
            if path.is_file() and not path.is_symlink() and path.stat().st_size <= _CANDIDATE_MEMORY_MAX_BYTES:
                raw_existing = json.loads(path.read_text(encoding="utf-8"))
            entries = self._candidate_memory_file_entries(raw_existing)
            by_key: dict[tuple[str, ...], dict[str, object]] = {}
            for entry in entries:
                key = self._candidate_memory_entry_key(entry)
                if key is not None:
                    by_key[key] = entry
            for entry in new_entries:
                key = self._candidate_memory_entry_key(entry)
                if key is not None:
                    by_key[key] = entry
            # Both entry count and serialized envelope are bounded. A row is
            # individually capped, but 128 maximal rows would otherwise
            # exceed the loader's 2 MiB safety limit and make the whole
            # positive-memory file unreadable on the next invocation.
            bounded: list[dict[str, object]] = []
            for entry in reversed(list(by_key.values())):
                candidate_entries = [entry, *bounded]
                try:
                    candidate_size = len(json.dumps(
                        {
                            "version": _CANDIDATE_MEMORY_VERSION,
                            "updated_at": _now(),
                            "entries": candidate_entries,
                        },
                        ensure_ascii=False,
                        allow_nan=False,
                    ).encode("utf-8"))
                except (TypeError, ValueError):
                    continue
                if candidate_size > _CANDIDATE_MEMORY_MAX_BYTES:
                    continue
                bounded = candidate_entries
                if len(bounded) >= _CANDIDATE_MEMORY_MAX_ENTRIES:
                    break
            atomic_write_json(
                path,
                {
                    "version": _CANDIDATE_MEMORY_VERSION,
                    "updated_at": _now(),
                    "entries": bounded,
                },
                allow_nan=False,
            )
        except (OSError, UnicodeDecodeError, json.JSONDecodeError, TypeError, ValueError):
            # Candidate memory is deliberately non-authoritative.
            return

    def _coerce_active_attempt(
        self,
        value: object,
        *,
        job_id: str,
    ) -> dict[str, object] | None:
        if not isinstance(value, Mapping):
            return None
        attempt_id = self._safe_attempt_id(value.get("attempt_id"))
        if attempt_id is None:
            return None
        try:
            staging = _safe_path(value.get("staging_root"), label="active attempt staging")
        except AutomaticReplenishmentError:
            return None
        expected_prefix = f"{self.staging_root}/{job_id}/"
        if not staging.startswith(expected_prefix) or posixpath.basename(staging) != attempt_id:
            return None
        providers = sorted({
            item.strip().casefold()
            for item in value.get("providers", [])
            if isinstance(item, str)
            and 0 < len(item.strip()) <= _DURABLE_CANDIDATE_PROVIDER_LIMIT
        }) if isinstance(value.get("providers"), list) else []
        locators = []
        seen: set[str] = set()
        raw_locators = value.get("locators")
        if isinstance(raw_locators, list):
            for item in raw_locators:
                if (
                    isinstance(item, str)
                    and item
                    and len(item) <= _DURABLE_CANDIDATE_LOCATOR_LIMIT
                    and item not in seen
                ):
                    seen.add(item)
                    locators.append(item)
        record: dict[str, object] = {
            "attempt_id": attempt_id,
            "staging_root": staging,
            "workspace": str((self.workspace_root / job_id / attempt_id).resolve()),
            "providers": providers,
            "locators": locators[:8],
        }
        snapshot = self._selection_snapshot(
            value.get("selections") if isinstance(value.get("selections"), list) else []
        )
        if snapshot:
            record["selections"] = snapshot
        task_id = self._safe_external_task_id(value.get("external_task_id"))
        if task_id is not None:
            record["external_task_id"] = task_id
        return record

    def _load_active_attempt(
        self,
        job_id: str,
        gap_state_paths: Mapping[str, Path],
    ) -> tuple[str, str, Path, dict[str, object]] | None:
        records: dict[tuple[str, str], dict[str, object]] = {}
        task_ids: set[str] = set()
        for path in dict.fromkeys(gap_state_paths.values()):
            try:
                state = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, UnicodeDecodeError, json.JSONDecodeError):
                continue
            if not isinstance(state, Mapping):
                continue
            record = self._coerce_active_attempt(
                state.get("active_attempt"), job_id=job_id,
            )
            if record is None:
                continue
            top_level_task_id = self._safe_external_task_id(
                state.get("external_task_id"),
            )
            if top_level_task_id is not None:
                task_ids.add(top_level_task_id)
                record.setdefault("external_task_id", top_level_task_id)
            records[(str(record["attempt_id"]), str(record["staging_root"]))] = record
        # A split task id is itself an in-doubt fact.  Do not choose one copy
        # merely because it happens to be encountered first on disk.
        if len(records) != 1 or len(task_ids) > 1:
            return None
        record = next(iter(records.values()))
        attempt_id = str(record["attempt_id"])
        staging = str(record["staging_root"])
        return attempt_id, staging, self.workspace_root / job_id / attempt_id, record

    @staticmethod
    def _waiting_reconcile_result(
        request: Mapping[str, object],
        gaps: Sequence[Mapping[str, object]],
        *,
        tier: str,
        message: str,
        needs_attention: bool = False,
        external_task_id: str | None = None,
    ) -> dict[str, object]:
        """Build the non-submitting result for an external-task barrier."""
        result: dict[str, object] = {
            "request": dict(request),
            "resolved_gap_ids": [],
            "unresolved_gap_ids": [
                str(gap.get("id") or "") for gap in gaps
                if isinstance(gap.get("id"), str) and gap.get("id")
            ],
            "tier": tier,
            "tier_status": "needs_attention" if needs_attention else "waiting_reconcile",
            "error": message,
        }
        if needs_attention:
            result["needs_attention"] = True
        if external_task_id is not None:
            result["external_task_id"] = external_task_id
        return result

    @classmethod
    def _selection_markers(
        cls,
        selections: Sequence[Mapping[str, object]],
    ) -> tuple[set[str], set[str]]:
        providers: set[str] = set()
        markers: set[str] = set()
        for selection in selections:
            if not isinstance(selection, Mapping):
                continue
            provider = str(selection.get("provider") or "").strip().casefold()
            if provider:
                providers.add(provider)
            normalized = cls._normalized_excluded_selection(selection)
            if normalized is None:
                continue
            value = normalized.get("infohash") or normalized.get("locator")
            if isinstance(value, str) and value:
                markers.add(value)
        return providers, markers

    @classmethod
    def _active_attempt_matches_selections(
        cls,
        active_attempt: Mapping[str, object],
        selections: Sequence[Mapping[str, object]],
    ) -> bool:
        providers, markers = cls._selection_markers(selections)
        active_providers = {
            item for item in active_attempt.get("providers", [])
            if isinstance(item, str) and item
        } if isinstance(active_attempt.get("providers"), list) else set()
        active_markers = {
            item for item in active_attempt.get("locators", [])
            if isinstance(item, str) and item
        } if isinstance(active_attempt.get("locators"), list) else set()
        if active_providers and active_providers != providers:
            return False
        if active_markers and not markers:
            return False
        if active_markers and not active_markers <= markers:
            return False
        return True

    def _update_gap_states(
        self,
        gap_state_paths: Mapping[str, Path],
        *,
        gap_ids: set[str] | None = None,
        updates: Mapping[str, object],
    ) -> None:
        seen_paths: set[Path] = set()
        for current_gap_id, path in gap_state_paths.items():
            if gap_ids is not None and current_gap_id not in gap_ids:
                continue
            if path in seen_paths:
                continue
            seen_paths.add(path)
            try:
                raw = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, UnicodeDecodeError, json.JSONDecodeError):
                continue
            state = dict(raw) if isinstance(raw, Mapping) else {}
            state.update(dict(updates))
            if "updated_at" not in updates:
                state["updated_at"] = _now()
            self._write_gap(state, path)

    @staticmethod
    def _candidate_failure_locator(candidate: Mapping[str, object]) -> str | None:
        locator = candidate.get("locator")
        if isinstance(locator, str) and locator:
            return locator
        infohash = candidate.get("infohash")
        if isinstance(infohash, str) and infohash:
            return infohash
        return None

    def _durable_gap_fields(
        self,
        prior_state: Mapping[str, object] | None,
        *,
        job_id: str,
    ) -> dict[str, object]:
        fields = self._copy_durable_defaults()
        if not isinstance(prior_state, Mapping):
            return fields
        tier = prior_state.get("tier")
        if tier in STRICT_TIER_ORDER:
            fields["tier"] = str(tier)
        active_attempt = self._coerce_active_attempt(
            prior_state.get("active_attempt"), job_id=job_id,
        )
        task_id = self._safe_external_task_id(prior_state.get("external_task_id"))
        if active_attempt is not None and task_id is not None:
            active_attempt.setdefault("external_task_id", task_id)
        fields["active_attempt"] = active_attempt
        fields["candidate_failures_by_provider"] = self._provider_failure_map(
            prior_state.get("candidate_failures_by_provider"),
        )
        fields["exhaustion_proof_by_provider"] = self._exhaustion_proof_map(
            prior_state.get("exhaustion_proof_by_provider"),
        )
        fields["external_task_id"] = task_id
        next_retry = prior_state.get("next_retry_at")
        if isinstance(next_retry, str) and next_retry:
            fields["next_retry_at"] = next_retry
        last_scope = prior_state.get("last_error_scope")
        if last_scope in {*_KNOWN_FAILURE_SCOPES, _FAILURE_CANCELLED}:
            fields["last_error_scope"] = str(last_scope)
        tier_status = prior_state.get("tier_status")
        if isinstance(tier_status, str) and 0 < len(tier_status) <= 64:
            fields["tier_status"] = tier_status
        # A successful provider child is not reusable provider input.  It is
        # nevertheless still task-owned staging until a *later* scoped audit
        # proves the selected gap disappeared.  Preserve even a malformed
        # marker so a damaged gap JSON cannot turn into a cleanup bypass when
        # the next audit re-projects the same row.
        if self._post_acquisition_reaudit_is_pending(prior_state):
            raw_reaudit = prior_state.get(_POST_ACQUISITION_REAUDIT_KEY)
            fields[_POST_ACQUISITION_REAUDIT_KEY] = (
                dict(raw_reaudit) if isinstance(raw_reaudit, Mapping) else raw_reaudit
            )
        # This is a RootJob migration notice, not a provider-tier state and
        # not an EngineJob summary mirror.  Preserve only its fixed,
        # operator-facing shape when the next audit re-projects the media gap;
        # arbitrary historic JSON must not become provider input.
        raw_companion_migration = prior_state.get(
            _LEGACY_COMPANION_SUBTITLE_MIGRATION_KEY,
        )
        if (
            isinstance(raw_companion_migration, Mapping)
            and raw_companion_migration.get("status") == "required"
        ):
            migration: dict[str, object] = {
                "status": "required",
                "manual_action": "migrate_rootjob_subtitle",
                "message": _LEGACY_COMPANION_SUBTITLE_MIGRATION_MESSAGE,
            }
            observed_at = raw_companion_migration.get("observed_at")
            if isinstance(observed_at, str) and 0 < len(observed_at) <= 64:
                migration["observed_at"] = observed_at
            fields[_LEGACY_COMPANION_SUBTITLE_MIGRATION_KEY] = migration
        return fields

    @staticmethod
    def _request_gaps(request: Mapping[str, object]) -> list[dict[str, object]]:
        rows = request.get("gaps")
        if not isinstance(rows, list):
            return []
        return [dict(row) for row in rows if isinstance(row, Mapping)]

    @classmethod
    def _validated_request_lane(cls, request: Mapping[str, object]) -> str:
        """Validate that declared and actual gap lanes are homogeneous."""
        raw_gaps = request.get("gaps")
        if not isinstance(raw_gaps, list) or not raw_gaps:
            raise AutomaticReplenishmentError("补源请求缺少 gap 列表")
        if any(not isinstance(gap, Mapping) for gap in raw_gaps):
            raise AutomaticReplenishmentError("补源请求包含无效 gap 行")
        lane = replenishment_request_lane(
            [dict(gap) for gap in raw_gaps if isinstance(gap, Mapping)]
        )
        if lane is None:
            raise AutomaticReplenishmentError(
                "补源请求混合了字幕与视频缺口，拒绝执行"
            )
        declared = request.get("lane")
        if declared is not None and declared != lane:
            raise AutomaticReplenishmentError("补源请求 lane 与 gap 类型不一致")
        return lane

    @staticmethod
    def _strip_legacy_companion_subtitle_members(
        selections: Sequence[Mapping[str, object]],
    ) -> tuple[list[dict[str, object]], set[str]]:
        """Drop legacy companion members before a media materializer sees them.

        A previous EngineJob compatibility path allowed one ``.chs`` member in
        a media torrent to cross into staging and then wrote it beside the
        child video.  That is incompatible with the RootJob-owned, verified
        merged-bilingual subtitle transaction.  Strip the selection map before
        the downloader's ``--select-file`` list is derived, while keeping the
        selected video members untouched.
        """
        sanitized: list[dict[str, object]] = []
        affected_gap_ids: set[str] = set()
        for raw_selection in selections:
            selection = dict(raw_selection)
            acquisition = selection.get("acquisition")
            if not isinstance(acquisition, Mapping):
                sanitized.append(selection)
                continue
            cleaned_acquisition = dict(acquisition)
            raw_companions = cleaned_acquisition.pop(
                "companion_subtitle_index_by_media_gap", None,
            )
            if raw_companions is not None:
                selected_gap_ids = {
                    gap_id
                    for gap_id in selection.get("selected_gap_ids", [])
                    if isinstance(gap_id, str) and gap_id
                } if isinstance(selection.get("selected_gap_ids"), list) else set()
                selected_companion_gap_ids: set[str] = set()
                if isinstance(raw_companions, Mapping):
                    selected_companion_gap_ids.update(
                        str(gap_id)
                        for gap_id in raw_companions
                        if isinstance(gap_id, str)
                        and gap_id
                        and gap_id in selected_gap_ids
                    )
                # A malformed map cannot authorize a subtitle either.  Keep
                # the notice scoped to the already selected media coordinates.
                if not selected_companion_gap_ids:
                    selected_companion_gap_ids.update(selected_gap_ids)
                affected_gap_ids.update(selected_companion_gap_ids)
            selection["acquisition"] = cleaned_acquisition
            sanitized.append(selection)
        return sanitized, affected_gap_ids

    @staticmethod
    def _delivered_legacy_companion_subtitle_gap_ids(
        acquisition: Mapping[str, object],
        *,
        known_gap_ids: set[str],
    ) -> tuple[set[str], int]:
        """Return media gaps with an already-delivered subtitle member.

        A third-party or old materializer can ignore the sanitized selection.
        The normalized delivery record is the only evidence used for the
        resulting manual-migration marker; the member is never installed.
        """
        rows = acquisition.get("files")
        if not isinstance(rows, list):
            raise AutomaticReplenishmentError("媒体获取结果缺少 files 映射")
        delivered: set[str] = set()
        count = 0
        for raw in rows:
            if not isinstance(raw, Mapping) or raw.get("kind") != "subtitle":
                continue
            count += 1
            raw_gap_ids = raw.get("gap_ids")
            if not isinstance(raw_gap_ids, list):
                continue
            delivered.update(
                gap_id
                for gap_id in raw_gap_ids
                if isinstance(gap_id, str) and gap_id in known_gap_ids
            )
        return delivered, count

    def _mark_legacy_companion_subtitle_migration(
        self,
        gap_state_paths: Mapping[str, Path],
        *,
        gap_ids: set[str],
        delivered_member_count: int = 0,
    ) -> list[str]:
        """Persist a manual RootJob notice without blocking media delivery."""
        affected = sorted(set(gap_state_paths) & set(gap_ids))
        if not affected:
            return []
        marker: dict[str, object] = {
            "status": "required",
            "manual_action": "migrate_rootjob_subtitle",
            "message": _LEGACY_COMPANION_SUBTITLE_MIGRATION_MESSAGE,
            "observed_at": _now(),
        }
        if delivered_member_count > 0:
            marker["delivered_member_count"] = min(delivered_member_count, 256)
        self._update_gap_states(
            gap_state_paths,
            gap_ids=set(affected),
            updates={
                _LEGACY_COMPANION_SUBTITLE_MIGRATION_KEY: marker,
                "updated_at": _now(),
            },
        )
        return affected

    @staticmethod
    def _media_child_staging_root(
        staging_root: str,
        staging_files: Sequence[StagingFile],
    ) -> str:
        """Derive, rather than accept, the exact video-only planner root."""
        root = _safe_path(staging_root, label="staging path")
        videos = [item.path for item in staging_files if item.kind == "video"]
        subtitles = [item.path for item in staging_files if item.kind == "subtitle"]
        if not videos:
            raise AutomaticReplenishmentError("媒体 child staging 没有视频文件")
        relative = [path[len(root) + 1:] for path in videos]
        first_parts = {
            value.split("/", 1)[0]
            for value in relative if "/" in value
        }
        isolated = len(first_parts) == 1 and all("/" in value for value in relative)
        if not subtitles:
            return f"{root}/{next(iter(first_parts))}" if isolated else root
        if not isolated:
            raise AutomaticReplenishmentError("混合补源无法从 files 证明视频隔离根")
        media_root = f"{root}/{next(iter(first_parts))}"
        prefix = media_root + "/"
        if any(path.startswith(prefix) for path in subtitles):
            raise AutomaticReplenishmentError("字幕不得进入媒体 child staging 根")
        return media_root

    def _gap_state(
        self,
        gap: Mapping[str, object],
        *,
        job_id: str,
        prior_state: Mapping[str, object] | None = None,
    ) -> dict[str, object]:
        gap_id = gap.get("id")
        if not isinstance(gap_id, str) or not gap_id:
            raise AutomaticReplenishmentError("Engine gap 缺少 id")
        subtitle_lane = str(gap.get("kind") or "") == "missing_subtitle"
        # Re-projecting an existing gap normally starts a new provider-search
        # round.  An ambiguous external submit is the exception: preserve its
        # reconciliation barrier so this write cannot make a later run look
        # safe to resubmit.
        waiting_reconcile = bool(
            not subtitle_lane
            and isinstance(prior_state, Mapping)
            and (
                prior_state.get("tier_status") == "waiting_reconcile"
                or prior_state.get("last_error_scope") == FAILURE_IN_DOUBT
            )
        )
        waiting_reaudit = bool(
            isinstance(prior_state, Mapping)
            and self._post_acquisition_reaudit_is_pending(prior_state)
        )
        prior_error = (
            prior_state.get("error")
            if isinstance(prior_state, Mapping)
            and isinstance(prior_state.get("error"), str)
            else None
        )
        if subtitle_lane:
            # A sidecar repair is not a provider-tier attempt.  In particular,
            # do not carry a stale video tier, candidate exclusion ledger,
            # external task id, or in-doubt marker from an older mixed run.
            # The only durable video-adjacent evidence a subtitle row may
            # retain is a pending post-acquisition audit marker, because that
            # marker owns task staging until a fresh scoped audit completes.
            durable_fields: dict[str, object] = {
                "lane": "subtitle",
                "active_attempt": None,
                "external_task_id": None,
                "next_retry_at": None,
                "last_error_scope": None,
            }
            if waiting_reaudit and isinstance(prior_state, Mapping):
                marker = prior_state.get(_POST_ACQUISITION_REAUDIT_KEY)
                durable_fields[_POST_ACQUISITION_REAUDIT_KEY] = (
                    dict(marker) if isinstance(marker, Mapping) else marker
                )
        else:
            durable_fields = self._durable_gap_fields(prior_state, job_id=job_id)
            durable_fields["lane"] = "media"
        state: dict[str, object] = {
            "id": gap_id,
            "job_id": job_id,
            "gap": dict(gap),
            "phase": (
                "waiting_reconcile" if waiting_reconcile
                else "waiting_reaudit" if waiting_reaudit
                else "provider_searching"
            ),
            "attempts": 0,
            "created_at": _now(),
            "updated_at": _now(),
            "error": prior_error if waiting_reconcile else None,
            **durable_fields,
        }
        # Candidate failures are local, task-owned evidence.  Carry only the
        # bounded, normalized identity list forward when a fresh audit
        # projection recreates this gap state; never copy arbitrary persisted
        # JSON into a provider request.
        if not subtitle_lane:
            prior_exclusions = (
                prior_state.get("excluded_candidates")
                if isinstance(prior_state, Mapping) else None
            )
            exclusions = self._merge_excluded_candidates(prior_exclusions)
            if exclusions:
                state["excluded_candidates"] = exclusions
        return state

    @staticmethod
    def _project_subtitle_lane_state(
        state: dict[str, object],
        *,
        phase: str,
        error: str | None = None,
        failure_scope: str | None = None,
        preserve_reaudit: bool = True,
    ) -> None:
        """Project one gap onto the independent subtitle state schema.

        Subtitle rows deliberately have no video tier/candidate evidence.
        Keeping this projection in one small helper also makes exception paths
        (including a materializer failure after a partial write) clear stale
        video attempts instead of inheriting them on the next audit.
        """
        marker = state.get(_POST_ACQUISITION_REAUDIT_KEY)
        for key in (
            "tier", "tier_status", "candidate_failures_by_provider",
            "exhaustion_proof_by_provider", "excluded_candidates",
            "resolved",
        ):
            state.pop(key, None)
        state.update({
            "lane": "subtitle",
            "phase": phase,
            "error": error,
            "active_attempt": None,
            "external_task_id": None,
            "next_retry_at": None,
            "last_error_scope": failure_scope,
            "updated_at": _now(),
        })
        if preserve_reaudit and marker is not None:
            state[_POST_ACQUISITION_REAUDIT_KEY] = marker
        else:
            state.pop(_POST_ACQUISITION_REAUDIT_KEY, None)

    def _mark_legacy_subtitle_migration(
        self,
        *,
        gap: Mapping[str, object],
        job_id: str,
        state_path: Path,
        prior_state: Mapping[str, object],
    ) -> None:
        """Durably retire one legacy subtitle row before any provider boundary.

        Some pre-retirement deployments persisted only
        ``phase=completed_with_gaps``.  That is historical exhaustion evidence,
        not proof that the RootJob subtitle transaction ran, so normalize the
        row into the explicit manual migration state rather than skipping it.
        """
        gap_id = gap.get("id")
        if not isinstance(gap_id, str) or not gap_id:
            raise AutomaticReplenishmentError("旧字幕迁移缺少 gap id")
        state = dict(prior_state)
        state.setdefault("id", gap_id)
        state.setdefault("job_id", job_id)
        state.setdefault("gap", dict(gap))
        state.setdefault("created_at", _now())
        self._project_subtitle_lane_state(
            state,
            phase="needs_attention",
            error=_LEGACY_SUBTITLE_MIGRATION_MESSAGE,
            failure_scope=None,
            # A pre-retirement attempt may own task-scoped staging.  Preserve
            # its audit evidence for a human; the retired lane never resumes
            # or cleans it automatically.
            preserve_reaudit=True,
        )
        state["subtitle_migration_required"] = True
        state["subtitle_migration_message"] = _LEGACY_SUBTITLE_MIGRATION_MESSAGE
        self._write_gap(state, state_path)

    def _fresh_list(
        self,
        path: str,
        *,
        alist: object | None = None,
    ) -> list[Mapping[str, object]]:
        client = self.alist if alist is None else alist
        listing = getattr(client, "list", None)
        if not callable(listing):
            raise AutomaticReplenishmentError("AList 客户端缺少 list")
        login = getattr(client, "login", None)
        if callable(login) and not getattr(client, "token", None):
            login()
        try:
            rows = listing(path, refresh=True)
        except TypeError:
            # Small test doubles and older AList wrappers expose ``list(path)``
            # only.  Losing the refresh keyword must not turn a usable client
            # into a permanent provider failure.
            rows = listing(path)
        if not isinstance(rows, list) or any(not isinstance(row, Mapping) for row in rows):
            raise AutomaticReplenishmentError(f"AList 目录响应无效: {path}")
        return list(rows)

    def verify_staging(self, staging_root: str) -> list[StagingFile]:
        """Recursively inspect staging for usable media or subtitles.

        The caller decides whether the request is media or subtitle-only. This
        keeps the shared staging verifier strict about ownership, paths, types
        and sizes without forcing a subtitle-only download through a media
        child plan.
        """
        root = _safe_path(staging_root, label="staging path")
        if root == self.staging_root or not root.startswith(self.staging_root + "/"):
            raise AutomaticReplenishmentError("拒绝核对任务 staging 根以外的目录")
        files: list[StagingFile] = []
        seen: set[str] = set()

        def visit(directory: str) -> None:
            for row in self._fresh_list(directory):
                name = _safe_name(row.get("name"), label=f"{directory} entry")
                full = posixpath.join(directory, name)
                if full in seen:
                    raise AutomaticReplenishmentError(f"staging 出现重复条目: {full}")
                seen.add(full)
                if row.get("is_dir") is True:
                    visit(full)
                    continue
                suffix = Path(name).suffix.casefold()
                if suffix in _VIDEO_EXTENSIONS:
                    kind = "video"
                elif suffix in _SUBTITLE_EXTENSIONS:
                    kind = "subtitle"
                else:
                    raise AutomaticReplenishmentError(f"staging 包含不支持文件: {full}")
                raw_size = row.get("size")
                if isinstance(raw_size, bool) or not isinstance(raw_size, int) or raw_size <= 0:
                    raise AutomaticReplenishmentError(f"staging 文件大小无效: {full}")
                files.append(StagingFile(path=full, size=raw_size, kind=kind))

        visit(root)
        if not files:
            raise AutomaticReplenishmentError("staging 没有可回投文件")
        return files

    def _verify_delivery_contract(
        self,
        acquisition: Mapping[str, object],
        *,
        job: EngineJob,
        attempt_id: str,
        staging_root: str,
        staging_files: Sequence[StagingFile],
    ) -> dict[str, object]:
        """Admit a materializer delivery only if its declaration matches AList."""
        try:
            normalized = validate_provider_delivery(
                acquisition,
                root_job_id=job.id,
                attempt_id=attempt_id,
                staging_parent=self.staging_root,
            )
        except ProviderDeliveryError as exc:
            raise AutomaticReplenishmentError(
                f"provider delivery 合同无效: {exc}"
            ) from exc
        if normalized.get("staging_root") != staging_root:
            raise AutomaticReplenishmentError("delivery staging_root 与当前任务不一致")
        raw_files = normalized.get("files")
        if not isinstance(raw_files, list):
            raise AutomaticReplenishmentError("delivery files 无效")
        declared: dict[str, tuple[str, int, str]] = {}
        for raw in raw_files:
            if not isinstance(raw, Mapping):
                raise AutomaticReplenishmentError("delivery files 项无效")
            path = str(raw.get("path"))
            if path in declared:
                raise AutomaticReplenishmentError("delivery 重复声明 staging 文件")
            declared[path] = (
                path,
                int(raw["size"]),
                str(raw["kind"]),
            )
        observed = {
            item.path: (item.path, item.size, item.kind)
            for item in staging_files
        }
        if declared != observed:
            raise AutomaticReplenishmentError("delivery 声明与 AList 回读不一致")
        return normalized

    def _local_video_is_pre_admitted(self) -> bool:
        materializer = self.materializer
        if isinstance(materializer, LocalTorrentAutomaticMaterializer):
            return materializer.pre_admits_local_video
        if isinstance(materializer, FixedTierAutomaticMaterializer):
            local = materializer.local_torrent
            return bool(
                isinstance(local, LocalTorrentAutomaticMaterializer)
                and local.pre_admits_local_video
            )
        # A deployment may provide a local-only materializer through the
        # narrow ``AutomaticMaterializer`` protocol rather than the bundled
        # class.  It can opt into the same once-only admission only by making
        # this explicit on the trusted in-process adapter; provider payloads
        # cannot set it because Delivery has no such field.  Unknown adapters
        # remain subject to remote probing below.
        return bool(getattr(materializer, "pre_admits_local_video", False))

    def _admit_delivery_videos(
        self,
        delivery: Mapping[str, object],
        staging_files: Sequence[StagingFile],
        selections: Sequence[Mapping[str, object]],
    ) -> None:
        """Probe each delivered video exactly once before child planning.

        The bundled local Torrent materializer already uses the same shared
        probe before upload, so its trusted lane is not probed a second time.
        Cloud lanes (and injected/unknown local materializers) are admitted
        from their exact remote staging objects here.
        """

        videos = [item for item in staging_files if item.kind == "video"]
        if not videos:
            return
        if (
            delivery.get("lane") == TIER_LOCAL_MAGNET
            and self._local_video_is_pre_admitted()
        ):
            return
        candidate = selections[0] if len(selections) == 1 else None
        for video in videos:
            try:
                verdict = self.remote_video_probe(self.alist, video.path)
            except VideoAdmissionError as exc:
                raise _ProviderVideoAdmissionError(
                    exc, candidate=candidate,
                ) from exc
            except Exception as exc:
                wrapped = VideoAdmissionError(
                    "remote_video_probe_error", infrastructure=True,
                )
                raise _ProviderVideoAdmissionError(
                    wrapped, candidate=candidate,
                ) from exc
            if (
                not isinstance(verdict, Mapping)
                or str(verdict.get("status") or "").casefold() != "satisfied"
            ):
                raise _ProviderVideoAdmissionError(
                    VideoAdmissionError("video_stream_unproven"),
                    candidate=candidate,
                )

    def _install_subtitle_members(
        self,
        *,
        job: EngineJob,
        request: Mapping[str, object],
        acquisition: Mapping[str, object],
        staging_root: str,
        round_number: int,
        required_gap_ids: set[str] | None = None,
    ) -> list[dict[str, object]]:
        """Fail closed for callers retained from the retired EngineJob lane.

        Automatic subtitle acquisition, staging, recovery and the single
        merged-sidecar write are now RootJob responsibilities.  This guard is
        intentionally kept only to make a stale direct call fail before it can
        inspect a provider payload or invoke the formal writer.
        """
        del job, request, acquisition, staging_root, round_number, required_gap_ids
        raise AutomaticReplenishmentError(
            "旧字幕正式写入器已移除；请通过 RootJob 字幕通道重新触发"
        )

    def _remove_staging(
        self,
        staging_root: str,
        *,
        pause_requested: Callable[[], bool] | None = None,
    ) -> None:
        """Delete only one verified task-owned staging tree and read it back."""
        _provider_pause_checkpoint(pause_requested)
        root = _safe_path(staging_root, label="staging cleanup path")
        if root == self.staging_root or not root.startswith(self.staging_root + "/"):
            raise AutomaticReplenishmentError("拒绝清理任务 staging 根以外的目录")
        alist = _PauseCheckedProviderPort(self.alist, pause_requested)
        remove = getattr(alist, "remove", None)
        remove_empty = getattr(alist, "remove_empty_dir", None)
        if not callable(remove):
            raise AutomaticReplenishmentError("AList 客户端缺少 remove")

        def fresh_cleanup_list(path: str) -> list[Mapping[str, object]]:
            """Treat an already-removed task path as a successful cleanup.

            The child engine may remove the attempt (or one of its parents)
            before the coordinator's final readback.  AList reports that
            state as ``object not found``; it is not a network or permission
            failure and must not turn an otherwise failed download into a
            misleading ``staging cleanup failed`` outcome.
            """
            try:
                return self._fresh_list(path, alist=alist)
            except Exception as exc:
                message = str(exc).casefold()
                if "object not found" in message:
                    return []
                raise

        # The child Engine may already have removed the now-empty source root.
        # That is a successful cleanup outcome, not a reason to leave the
        # parent task in retry forever.
        parent = posixpath.dirname(root) or "/"
        name = posixpath.basename(root)
        parent_rows = fresh_cleanup_list(parent)
        if not any(row.get("name") == name and row.get("is_dir") is True for row in parent_rows):
            return

        def remove_verified_empty(parent: str, name: str) -> bool:
            """Remove one exact empty task directory, with a stale-list fallback.

            Some AList storage backends acknowledge ``remove_empty_directory``
            before a forced parent listing stops showing the directory.  The
            normal endpoint remains the first choice.  The generic remove
            fallback is deliberately available only after two fresh checks
            prove that the exact, task-owned directory is still an empty
            directory; it must never turn a stale cleanup observation into a
            recursive delete of a non-empty or shared parent.
            """
            directory = posixpath.join(parent, name)
            rows = fresh_cleanup_list(parent)
            matching = [row for row in rows if row.get("name") == name]
            if not matching:
                return True
            if len(matching) != 1 or matching[0].get("is_dir") is not True:
                return False
            if fresh_cleanup_list(directory):
                return False

            if callable(remove_empty):
                removed = remove_empty(directory)
                # An explicit non-empty result is authoritative.  Do not
                # fall back to a potentially recursive generic remove.
                if removed is False:
                    return False
            else:
                remove(parent, [name])

            rows = fresh_cleanup_list(parent)
            matching = [row for row in rows if row.get("name") == name]
            if not matching:
                return True
            if not callable(remove_empty):
                return False

            # ``remove_empty_dir`` reported success but this fresh parent
            # readback still sees the exact directory.  Re-check that it is
            # empty before using ``remove(parent, [name])`` as a narrowly
            # scoped fallback for this backend inconsistency.
            if (
                len(matching) != 1
                or matching[0].get("is_dir") is not True
                or fresh_cleanup_list(directory)
            ):
                return False
            remove(parent, [name])
            rows = fresh_cleanup_list(parent)
            return not any(row.get("name") == name for row in rows)

        def visit(directory: str) -> None:
            rows = fresh_cleanup_list(directory)
            files: list[str] = []
            directories: list[str] = []
            for row in rows:
                name = _safe_name(row.get("name"), label=f"{directory} cleanup entry")
                if row.get("is_dir") is True:
                    directories.append(name)
                else:
                    files.append(name)
            if files:
                remove(directory, files)
            for name in directories:
                child = posixpath.join(directory, name)
                visit(child)
                remove_verified_empty(directory, name)

        visit(root)
        current = root
        while current != self.staging_root:
            current_parent = posixpath.dirname(current) or "/"
            current_name = posixpath.basename(current)
            if not remove_verified_empty(current_parent, current_name):
                # A shared parent may contain another attempt.  Stop rather
                # than deleting something this task did not create.
                if current == root:
                    raise AutomaticReplenishmentError(f"staging 清理后仍可见: {root}")
                break
            current = current_parent

    def _remove_local_attempt_workspace(
        self,
        *,
        job_id: str,
        attempt_id: str,
        pause_requested: Callable[[], bool] | None = None,
    ) -> None:
        """Delete exactly one post-audit local provider workspace.

        The workspace can contain a persisted Quark task id needed for
        restart/reconciliation, so it follows the same post-audit boundary as
        its remote attempt.  Do not fold this into root terminal cleanup:
        a successful gap can be re-audited while another root gap remains
        active, and only this exact attempt is eligible here.
        """
        safe_job_id = self._safe_replenishment_job_id(job_id)
        safe_attempt_id = self._safe_attempt_id(attempt_id)
        if safe_job_id is None or safe_attempt_id is None:
            raise AutomaticReplenishmentError("本地补源 staging 标识无效")
        if self.workspace_root.is_symlink():
            raise AutomaticReplenishmentError("本地补源 staging 根不允许符号链接")
        job_root = self.workspace_root / safe_job_id
        if job_root.is_symlink():
            raise AutomaticReplenishmentError("本地补源任务根不允许符号链接")
        attempt_root = job_root / safe_attempt_id
        if attempt_root.parent != job_root or attempt_root.is_symlink():
            raise AutomaticReplenishmentError("本地补源 attempt 路径无效")
        if not attempt_root.exists():
            return
        if not attempt_root.is_dir():
            raise AutomaticReplenishmentError("本地补源 attempt 不是目录")
        _provider_pause_checkpoint(pause_requested)
        shutil.rmtree(attempt_root)
        if attempt_root.exists():
            raise AutomaticReplenishmentError("本地补源 attempt 清理后仍存在")
        try:
            _provider_pause_checkpoint(pause_requested)
            job_root.rmdir()
        except FileNotFoundError:
            return
        except OSError:
            # A sibling attempt remains.  It has separate durable ownership
            # and must never be removed as a convenience cleanup.
            return

    def _mark_post_acquisition_reaudit(
        self,
        gap_state_paths: Mapping[str, Path],
        *,
        job_id: str,
        attempt_id: str,
        staging_root: str,
        selected_gap_ids: set[str],
        child_job_id: str | None = None,
    ) -> dict[str, object]:
        """Hold a successful attempt until its selected gaps are re-audited."""
        safe_job_id = self._safe_replenishment_job_id(job_id)
        safe_attempt_id = self._safe_attempt_id(attempt_id)
        if safe_job_id is None or safe_attempt_id is None:
            raise AutomaticReplenishmentError("补源重审状态缺少安全任务标识")
        try:
            safe_staging_root = _safe_path(
                staging_root, label="post-acquisition staging",
            )
        except AutomaticReplenishmentError:
            raise
        if (
            not safe_staging_root.startswith(f"{self.staging_root}/{safe_job_id}/")
            or posixpath.basename(safe_staging_root) != safe_attempt_id
        ):
            raise AutomaticReplenishmentError("补源重审状态不属于当前 attempt")
        selected = sorted({
            gap_id for gap_id in selected_gap_ids
            if isinstance(gap_id, str)
            and gap_id
            and len(gap_id) <= 256
            and not any(char in gap_id for char in ("/", "\\", "\x00", "\n", "\r"))
        })
        if not selected or len(selected) != len(selected_gap_ids):
            raise AutomaticReplenishmentError("补源重审状态缺少有效 selected gap")
        if len(selected) > _POST_ACQUISITION_REAUDIT_MAX_GAPS:
            raise AutomaticReplenishmentError("补源重审 selected gap 数量超限")
        marker: dict[str, object] = {
            "status": "pending",
            "attempt_id": safe_attempt_id,
            "staging_root": safe_staging_root,
            "selected_gap_ids": selected,
            "requested_at": _now(),
        }
        safe_child_id = self._safe_replenishment_job_id(child_job_id)
        if safe_child_id is not None:
            marker["child_job_id"] = safe_child_id
        for gap_id in selected:
            path = gap_state_paths.get(gap_id)
            if path is None:
                raise AutomaticReplenishmentError("补源重审状态找不到 selected gap")
            try:
                raw = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise AutomaticReplenishmentError("补源 gap 状态不可读") from exc
            if not isinstance(raw, Mapping) or raw.get("id") != gap_id:
                raise AutomaticReplenishmentError("补源 gap 状态与 selected gap 不一致")
            state = dict(raw)
            state.update({
                "phase": "waiting_reaudit",
                "updated_at": _now(),
                "error": None,
                "active_attempt": None,
                "last_error_scope": None,
                "next_retry_at": None,
                _POST_ACQUISITION_REAUDIT_KEY: dict(marker),
            })
            self._write_gap(state, path)
        return marker

    def _post_acquisition_reaudit_entries(
        self,
        job_id: str,
    ) -> tuple[list[dict[str, object]], list[str]]:
        """Load pending re-audit records without trusting arbitrary JSON paths."""
        safe_job_id = self._safe_replenishment_job_id(job_id)
        if safe_job_id is None:
            raise AutomaticReplenishmentError("补源重审 job id 无效")
        state_directory = self.gaps_root / _GAP_SLUG.sub("-", safe_job_id).strip(".-")[:96]
        if not state_directory.exists():
            return [], []
        if state_directory.is_symlink() or not state_directory.is_dir():
            return [], ["gap_state_directory_invalid"]
        entries: list[dict[str, object]] = []
        invalid: list[str] = []
        try:
            paths = sorted(path for path in state_directory.glob("*.json") if path.is_file())
        except OSError:
            return [], ["gap_state_directory_unreadable"]
        for path in paths:
            if path.is_symlink():
                invalid.append(path.name)
                continue
            try:
                raw = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, UnicodeDecodeError, json.JSONDecodeError):
                # A malformed unrelated historical row is not a new cleanup
                # authority.  It is only relevant when it declares this
                # lifecycle marker, which cannot be known after a parse
                # failure; fail closed for the root's own task-state folder.
                invalid.append(path.name)
                continue
            if not isinstance(raw, Mapping):
                invalid.append(path.name)
                continue
            state = dict(raw)
            if not self._post_acquisition_reaudit_is_pending(state):
                continue
            gap_id = state.get("id")
            if not isinstance(gap_id, str) or not gap_id:
                invalid.append(path.name)
                continue
            marker = self._coerce_post_acquisition_reaudit(
                state.get(_POST_ACQUISITION_REAUDIT_KEY),
                job_id=safe_job_id,
                gap_id=gap_id,
            )
            if marker is None:
                invalid.append(path.name)
                continue
            entries.append({
                "path": path,
                "state": state,
                "gap_id": gap_id,
                "marker": marker,
            })
        return entries, invalid

    def _update_post_acquisition_reaudit_group(
        self,
        entries: Sequence[Mapping[str, object]],
        *,
        marker_updates: Mapping[str, object],
        phase: str,
    ) -> None:
        """Atomically replace only the current attempt marker on its gaps."""
        for entry in entries:
            path = entry.get("path")
            raw_state = entry.get("state")
            raw_marker = entry.get("marker")
            if not isinstance(path, Path) or not isinstance(raw_state, Mapping) or not isinstance(raw_marker, Mapping):
                raise AutomaticReplenishmentError("补源重审状态条目无效")
            state = dict(raw_state)
            marker = dict(raw_marker)
            marker.update(dict(marker_updates))
            state.update({
                "phase": phase,
                "updated_at": _now(),
                "active_attempt": None,
                "next_retry_at": None,
                _POST_ACQUISITION_REAUDIT_KEY: marker,
            })
            self._write_gap(state, path)

    def reconcile_post_acquisition_reaudit(
        self,
        job_id: str,
        *,
        audit_started_at: object,
        audit_complete: bool,
        actionable_gap_ids: Sequence[str] | set[str],
        audit_uncertain: bool,
        unidentified_actionable_gap: bool = False,
        pause_requested: Callable[[], bool] | None = None,
    ) -> dict[str, object]:
        """Clean successful attempt staging only after a later scoped audit.

        This is intentionally a narrow state transition, not a second
        provider pass.  An incomplete/unknown audit, a selected gap still in
        the report, or a cleanup error leaves both local and remote attempt
        state intact for inspection or a later bounded retry.
        """
        safe_job_id = self._safe_replenishment_job_id(job_id)
        if safe_job_id is None:
            raise AutomaticReplenishmentError("补源重审 job id 无效")
        entries, invalid_entries = self._post_acquisition_reaudit_entries(safe_job_id)
        cleaned: list[str] = []
        pending: set[str] = set()
        blocked: list[dict[str, object]] = []
        cleanup_errors: list[dict[str, object]] = []
        if invalid_entries:
            pending.update(invalid_entries)
            blocked.extend({"attempt_id": item, "reason": "state_invalid"} for item in invalid_entries)
        if not entries:
            return {
                "job_id": safe_job_id,
                "cleaned_attempt_ids": cleaned,
                "pending_attempt_ids": sorted(pending),
                "blocked_attempts": blocked,
                "cleanup_errors": cleanup_errors,
                "retryable": False,
            }

        groups: dict[tuple[str, str], list[dict[str, object]]] = {}
        for entry in entries:
            marker = entry["marker"]
            if not isinstance(marker, Mapping):  # guarded above; keep mypy honest.
                continue
            key = (str(marker["attempt_id"]), str(marker["staging_root"]))
            groups.setdefault(key, []).append(entry)
        audit_started = self._parse_utc_timestamp(audit_started_at)
        observed = {
            value for value in actionable_gap_ids
            if isinstance(value, str) and value
        }
        for (attempt_id, staging_root), group in groups.items():
            pending.add(attempt_id)
            markers = [entry["marker"] for entry in group if isinstance(entry.get("marker"), Mapping)]
            selected_sets = {
                tuple(marker.get("selected_gap_ids") or [])
                for marker in markers
            }
            requested_times = {
                str(marker.get("requested_at") or "")
                for marker in markers
            }
            if len(selected_sets) != 1 or len(requested_times) != 1:
                blocked.append({"attempt_id": attempt_id, "reason": "state_conflict"})
                continue
            marker = dict(markers[0])
            requested_at = self._parse_utc_timestamp(marker.get("requested_at"))
            if (
                audit_complete is not True
                or audit_uncertain
                or unidentified_actionable_gap
                or audit_started is None
                or requested_at is None
                # The scoped audit has to begin strictly after the durable
                # marker.  Equal timestamps are not evidence of ordering and
                # must not permit cleanup (for example after a coarse clock
                # or a forged report fixture).
                or audit_started <= requested_at
            ):
                reason = (
                    "audit_uncertain" if audit_uncertain or audit_complete is not True
                    else "audit_scope_or_time_unproven"
                )
                blocked.append({"attempt_id": attempt_id, "reason": reason})
                continue
            selected_gap_ids = set(marker.get("selected_gap_ids") or [])
            if selected_gap_ids & observed:
                try:
                    self._update_post_acquisition_reaudit_group(
                        group,
                        marker_updates={
                            "status": "gap_still_actionable",
                            "last_audit_started_at": str(audit_started_at),
                        },
                        phase="waiting_reaudit",
                    )
                except Exception as exc:
                    cleanup_errors.append({
                        "attempt_id": attempt_id,
                        "error": redact_error(exc),
                    })
                blocked.append({"attempt_id": attempt_id, "reason": "selected_gap_still_actionable"})
                continue
            try:
                self._remove_staging(
                    staging_root,
                    pause_requested=pause_requested,
                )
                self._remove_local_attempt_workspace(
                    job_id=safe_job_id, attempt_id=attempt_id,
                    pause_requested=pause_requested,
                )
                self._update_post_acquisition_reaudit_group(
                    group,
                    marker_updates={
                        "status": "cleaned",
                        "last_audit_started_at": str(audit_started_at),
                        "cleaned_at": _now(),
                        "error": None,
                    },
                    phase="resolved",
                )
            except AutomaticReplenishmentCancelled:
                raise
            except Exception as exc:
                cleanup_attempts = marker.get("cleanup_attempts")
                attempts = (
                    cleanup_attempts if isinstance(cleanup_attempts, int)
                    and not isinstance(cleanup_attempts, bool) else 0
                ) + 1
                attempts = min(5, attempts)
                try:
                    self._update_post_acquisition_reaudit_group(
                        group,
                        marker_updates={
                            "status": "cleanup_failed",
                            "cleanup_attempts": attempts,
                            "last_audit_started_at": str(audit_started_at),
                            "error": redact_error(exc),
                        },
                        phase="waiting_reaudit",
                    )
                except Exception:
                    pass
                cleanup_errors.append({
                    "attempt_id": attempt_id,
                    "error": redact_error(exc),
                    "attempts": attempts,
                })
                continue
            pending.discard(attempt_id)
            cleaned.append(attempt_id)
        retryable = any(
            isinstance(row.get("attempts"), int) and row["attempts"] < 5
            for row in cleanup_errors
        )
        return {
            "job_id": safe_job_id,
            "cleaned_attempt_ids": sorted(cleaned),
            "pending_attempt_ids": sorted(pending),
            "blocked_attempts": blocked,
            "cleanup_errors": cleanup_errors,
            "retryable": retryable,
        }

    @classmethod
    def cleanup_post_acquisition_reaudit(
        cls,
        state_root: str | Path,
        *,
        job_id: str,
        alist: object,
        staging_root: str,
        audit_started_at: object,
        audit_complete: bool,
        actionable_gap_ids: Sequence[str] | set[str],
        audit_uncertain: bool,
        unidentified_actionable_gap: bool = False,
        pause_requested: Callable[[], bool] | None = None,
    ) -> dict[str, object]:
        """Run the state-only post-audit cleanup without provider setup.

        An audit callback after API restart must not instantiate a search
        adapter or a Quark helper merely to remove a previously successful
        task-owned attempt.  The runtime constructor has no external side
        effects, so a tiny cleanup-only instance keeps this path shared with
        the normal in-process provider runtime.
        """
        runtime = cls(
            state_root,
            engine_runner=object(),
            alist=alist,
            search=object(),
            materializer=object(),
            staging_root=staging_root,
        )
        return runtime.reconcile_post_acquisition_reaudit(
            job_id,
            audit_started_at=audit_started_at,
            audit_complete=audit_complete,
            actionable_gap_ids=actionable_gap_ids,
            audit_uncertain=audit_uncertain,
            unidentified_actionable_gap=unidentified_actionable_gap,
            pause_requested=pause_requested,
        )

    @staticmethod
    def _normalized_excluded_selection(
        selection: Mapping[str, object],
    ) -> dict[str, object] | None:
        """Return one bounded, provider-neutral candidate identity.

        Gap JSON survives API recreation, so it must never be replayed as
        arbitrary provider input.  The selector only needs a provider plus a
        locator and/or a torrent infohash; keep that small evidence set and
        discard malformed or unbounded historic rows.
        """
        raw_provider = selection.get("provider")
        if not isinstance(raw_provider, str):
            return None
        provider = raw_provider.strip().casefold()
        if not provider or len(provider) > _DURABLE_CANDIDATE_PROVIDER_LIMIT:
            return None

        raw_locator = selection.get("locator")
        locator = raw_locator.strip() if isinstance(raw_locator, str) else ""
        if len(locator) > _DURABLE_CANDIDATE_LOCATOR_LIMIT:
            return None

        raw_infohash = selection.get("infohash")
        infohash = (
            raw_infohash.strip().casefold()
            if isinstance(raw_infohash, str) else ""
        )
        if not _INFOHASH_TOKEN.fullmatch(infohash):
            infohash = ""
        if not infohash and locator:
            match = _BTIH_TOKEN.search(locator)
            if match:
                infohash = match.group(1).casefold()
        if not locator and not infohash:
            return None

        normalized: dict[str, object] = {"provider": provider}
        if locator:
            normalized["locator"] = locator
        if infohash:
            normalized["infohash"] = infohash
        raw_release_name = selection.get("release_name")
        if isinstance(raw_release_name, str):
            release_name = raw_release_name.strip()
            if release_name and len(release_name) <= _DURABLE_CANDIDATE_RELEASE_NAME_LIMIT:
                normalized["release_name"] = release_name
        return normalized

    @classmethod
    def _excluded_selection_key(cls, selection: Mapping[str, object]) -> tuple[str, ...]:
        provider = str(selection.get("provider") or "").casefold()
        infohash = str(selection.get("infohash") or "").casefold()
        if infohash:
            return ("infohash", provider, infohash)
        return ("locator", provider, str(selection.get("locator") or ""))

    @classmethod
    def _merge_excluded_candidates(
        cls,
        *groups: object,
    ) -> list[dict[str, object]]:
        """Normalize, deduplicate and cap persisted candidate identities."""
        merged: list[dict[str, object]] = []
        seen: set[tuple[str, ...]] = set()
        for rows in groups:
            if not isinstance(rows, (list, tuple)):
                continue
            for raw in rows:
                if not isinstance(raw, Mapping):
                    continue
                normalized = cls._normalized_excluded_selection(raw)
                if normalized is None:
                    continue
                key = cls._excluded_selection_key(normalized)
                if key in seen:
                    continue
                seen.add(key)
                merged.append(normalized)
        # Preserve the newest records when a legacy or damaged state file
        # carries more history than this runtime is willing to replay.
        return merged[-_DURABLE_CANDIDATE_EXCLUSION_LIMIT:]

    @classmethod
    def _same_excluded_candidate(
        cls,
        left: Mapping[str, object],
        right: Mapping[str, object],
    ) -> bool:
        if str(left.get("provider") or "").casefold() != str(
            right.get("provider") or ""
        ).casefold():
            return False
        left_hash = str(left.get("infohash") or "").casefold()
        right_hash = str(right.get("infohash") or "").casefold()
        if left_hash and right_hash and left_hash == right_hash:
            return True
        left_locator = str(left.get("locator") or "")
        right_locator = str(right.get("locator") or "")
        return bool(left_locator and right_locator and left_locator == right_locator)

    @staticmethod
    def _exception_chain(error: BaseException):
        seen: set[int] = set()
        current: BaseException | None = error
        while current is not None and id(current) not in seen:
            seen.add(id(current))
            yield current
            cause = current.__cause__ or current.__context__
            current = cause if isinstance(cause, BaseException) else None

    @classmethod
    def _nonfinal_reconcile_failure_scope(cls, error: Exception) -> str | None:
        """Return a durable-hold failure that must outrank candidate context.

        Exception context is useful for diagnostics, but it must never turn a
        later ``in_doubt`` or non-final infrastructure error into a candidate
        exclusion merely because an earlier candidate validation failed.  The
        former means an external task or its cleanup may still consume disk;
        keeping the attempt is more important than advancing a tier.
        """
        for item in cls._exception_chain(error):
            scope = getattr(item, "failure_scope", None)
            if not isinstance(scope, str):
                continue
            normalized = scope.strip().casefold()
            if (
                normalized in {FAILURE_IN_DOUBT, FAILURE_INFRASTRUCTURE}
                and not getattr(item, "external_task_final", False)
            ):
                return normalized
        return None

    @classmethod
    def _failure_scope(
        cls,
        error: Exception,
        candidate_exclusions: Sequence[Mapping[str, object]] | None = None,
    ) -> str:
        if isinstance(error, AutomaticReplenishmentCancelled):
            return _FAILURE_CANCELLED
        held_scope = cls._nonfinal_reconcile_failure_scope(error)
        if held_scope is not None:
            return held_scope
        if candidate_exclusions:
            return FAILURE_CANDIDATE
        for item in cls._exception_chain(error):
            scope = getattr(item, "failure_scope", None)
            if isinstance(scope, str):
                normalized = scope.strip().casefold()
                if normalized in _KNOWN_FAILURE_SCOPES:
                    return normalized
            if getattr(item, "exclude_candidate", False) is True:
                return FAILURE_CANDIDATE
        return FAILURE_INFRASTRUCTURE

    @classmethod
    def _failure_external_task_id(cls, error: Exception) -> str | None:
        for item in cls._exception_chain(error):
            for attribute in ("external_task_id", "task_id"):
                value = getattr(item, attribute, None)
                task_id = cls._safe_external_task_id(value)
                if task_id is not None:
                    return task_id
        return None

    @classmethod
    def _failure_external_task_is_final(cls, error: Exception) -> bool:
        """Whether this failure durably proved an external task stopped."""
        return any(
            getattr(item, "external_task_final", False) is True
            for item in cls._exception_chain(error)
        )

    @classmethod
    def _cleanup_attempt_after_error(
        cls,
        error: Exception | None,
        candidate_exclusions: Sequence[Mapping[str, object]],
    ) -> bool:
        # A successful child has only proved its direct Engine readback.  Its
        # task-owned staging must survive until the coordinator's later,
        # scoped audit confirms the selected gap is gone.  Candidate-local
        # invalid payloads are the sole early-delete exception.
        return (
            error is not None
            and cls._failure_scope(error, candidate_exclusions) == FAILURE_CANDIDATE
        )

    @classmethod
    def _failure_candidate_exclusions(
        cls,
        error: Exception,
        selections: Sequence[Mapping[str, object]],
    ) -> list[dict[str, object]]:
        """Return only identities that the materializer blamed on a release.

        Network, AList, capacity and control failures must remain retryable;
        treating them as a bad release would hide a usable source.  The local
        Torrent materializer explicitly marks candidate-local failures with
        ``exclude_candidate=True`` and normally includes the exact selection.
        """
        if cls._nonfinal_reconcile_failure_scope(error) is not None:
            return []
        candidate_error = None
        for item in cls._exception_chain(error):
            if getattr(item, "exclude_candidate", False) is True:
                candidate_error = item
                break
        if candidate_error is None:
            return []
        normalized_selections = cls._merge_excluded_candidates(list(selections))
        if not normalized_selections:
            return []
        raw_candidate = getattr(candidate_error, "candidate", None)
        if isinstance(raw_candidate, Mapping):
            normalized_failure = cls._normalized_excluded_selection(raw_candidate)
            if normalized_failure is not None:
                matching = [
                    selection for selection in normalized_selections
                    if cls._same_excluded_candidate(selection, normalized_failure)
                ]
                if matching:
                    return matching
                # The error did identify a release, but not one in this
                # selected bundle.  Do not trust it as durable state.
                return []
        # Older/materializer-agnostic candidate errors may not carry a row.
        # One selected release is still unambiguous; a multi-selection bundle
        # is not, so leave it retryable rather than poisoning every source.
        return normalized_selections if len(normalized_selections) == 1 else []

    def _load_excluded_candidates(
        self,
        gap_state_paths: Mapping[str, Path],
    ) -> list[dict[str, object]]:
        persisted: list[object] = []
        for path in dict.fromkeys(gap_state_paths.values()):
            try:
                state = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, UnicodeDecodeError, json.JSONDecodeError):
                continue
            if isinstance(state, Mapping):
                persisted.append(state.get("excluded_candidates"))
        return self._merge_excluded_candidates(*persisted)

    def _persist_excluded_candidates(
        self,
        gap_state_paths: Mapping[str, Path],
        candidates: Sequence[Mapping[str, object]],
    ) -> None:
        """Save explicit candidate failures into each active task-gap record.

        A materializer acquisition is shared by the request's selected gaps:
        a torrent that cannot deliver one selected member cannot deliver the
        same selected release for its sibling gap either.  Persisting the
        narrow provider/locator/hash identity on the active local records
        therefore prevents a process restart from spending another full idle
        timeout on that exact release, while still permitting every other
        identity to be searched and selected.
        """
        additions = self._merge_excluded_candidates(list(candidates))
        if not additions:
            return
        for path in dict.fromkeys(gap_state_paths.values()):
            try:
                raw = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise AutomaticReplenishmentError("补源 gap 状态不可读") from exc
            if not isinstance(raw, Mapping):
                raise AutomaticReplenishmentError("补源 gap 状态格式无效")
            state = dict(raw)
            merged = self._merge_excluded_candidates(
                state.get("excluded_candidates"), additions,
            )
            if not merged:
                continue
            policy_state: Mapping[str, object] | None = None
            for candidate in additions:
                provider = str(candidate.get("provider") or "").strip().casefold()
                locator = self._candidate_failure_locator(candidate)
                if provider != self._tier_from_state(state) or locator is None:
                    raise AutomaticReplenishmentError(
                        "候选失败与当前补源 tier 不一致"
                    )
                try:
                    policy_state = apply_tier_outcome(
                        state,
                        {"scope": FAILURE_CANDIDATE, "locator": locator},
                    )
                except ReplenishmentTierError as exc:
                    raise AutomaticReplenishmentError("补源 tier 状态无效") from exc
                self._copy_tier_policy_fields(state, policy_state)
            state["excluded_candidates"] = merged
            state["active_attempt"] = None
            state["next_retry_at"] = None
            state["phase"] = (
                "provider_searching"
                if policy_state is not None and policy_state.get("status") == "advanced"
                else "retry_wait"
            )
            state["updated_at"] = _now()
            self._write_gap(state, path)

    @classmethod
    def _excluded_selection(
        cls,
        selection: Mapping[str, object],
    ) -> dict[str, object]:
        """Return a selector-compatible identity, or an empty safe row."""
        return cls._normalized_excluded_selection(selection) or {}

    @staticmethod
    def _child_resolved_gap_ids(
        child: EngineJob,
        gaps: Sequence[Mapping[str, object]],
    ) -> set[str]:
        """Return gaps proven by the child execution's actual video targets.

        Provider manifests are only candidate evidence.  A child can complete
        successfully after receiving a partial pack, so a provider-selected
        gap must not become durable ``resolved`` state until the child reports
        a corresponding, executed video target.
        """
        execution = child.execution if isinstance(child.execution, Mapping) else {}
        rows = execution.get("files")
        if not isinstance(rows, list):
            raise AutomaticReplenishmentError("补源 child 缺少已执行文件回读")
        video_targets: list[str] = []
        for row in rows:
            if not isinstance(row, Mapping):
                raise AutomaticReplenishmentError("补源 child 文件回读格式无效")
            target = row.get("target") or row.get("path")
            if not isinstance(target, str) or not target.startswith("/"):
                continue
            if Path(target).suffix.casefold() in _VIDEO_EXTENSIONS:
                video_targets.append(target)
        if not video_targets:
            raise AutomaticReplenishmentError("补源 child 没有已执行视频")

        episode_ids: set[str] = set()
        seasons: set[int] = set()
        for target in video_targets:
            for match in _EPISODE_TOKEN.finditer(target):
                season, episode = int(match.group(1)), int(match.group(2))
                if season > 999 or episode <= 0:
                    continue
                episode_ids.add(f"S{season:02d}E{episode:02d}")
                seasons.add(season)
            for match in _SEASON_TOKEN.finditer(target):
                seasons.add(int(match.group(1)))

        resolved: set[str] = set()
        for raw_gap in gaps:
            gap_id = raw_gap.get("id")
            if not isinstance(gap_id, str) or not gap_id:
                continue
            kind = str(raw_gap.get("kind") or "")
            if kind == "missing_media":
                resolved.add(gap_id)
                continue
            if kind == "missing_episode":
                season = raw_gap.get("season")
                episodes = raw_gap.get("episodes")
                if isinstance(season, int) and isinstance(episodes, list) and episodes:
                    expected = {
                        f"S{season:02d}E{int(episode):02d}"
                        for episode in episodes
                        if isinstance(episode, int) and episode > 0
                    }
                    if expected and expected.issubset(episode_ids):
                        resolved.add(gap_id)
                elif gap_id in episode_ids:
                    resolved.add(gap_id)
                continue
            if kind == "missing_season":
                season = raw_gap.get("season")
                if isinstance(season, int) and season in seasons:
                    expected_count = raw_gap.get("expected_episode_count")
                    if (
                        isinstance(expected_count, int)
                        and expected_count > 0
                        and sum(1 for item in episode_ids if item.startswith(f"S{season:02d}E")) < expected_count
                    ):
                        continue
                    resolved.add(gap_id)
        return resolved

    def _run_request(
        self,
        *,
        job: EngineJob,
        request: Mapping[str, object],
        gap_state_paths: Mapping[str, Path],
    ) -> dict[str, object]:
        # These records are loaded from task-owned gap JSON, so a recreated
        # API does not reselect a release that already failed at the candidate
        # acquisition boundary.  The loader normalizes and caps the list.
        excluded = self._load_excluded_candidates(gap_state_paths)
        request_body = dict(request)
        request_gaps = self._request_gaps(request_body)
        if self._validated_request_lane(request_body) != "media":
            raise AutomaticReplenishmentError(
                "字幕缺口不得进入视频三阶补源链"
            )
        resolved_total: set[str] = set()
        for round_number in range(1, self.max_candidate_rounds + 1):
            # A failed downloader may return after an operator has paused the
            # pilot.  Check *before* the next provider search so a single
            # in-flight attempt cannot fan out into another candidate round.
            self._raise_if_cancelled(
                job, round_number=round_number, boundary="candidate_round",
            )
            current_tier = self._current_tier_for_gap_states(gap_state_paths)
            # Recovery ordering is intentional: load the durable attempt and
            # task id *before* applying the waiting barrier.  A known id is
            # an instruction to query/re-enter the existing provider
            # operation; it is never permission to submit a new one.
            restored_attempt = self._load_active_attempt(job.id, gap_state_paths)
            restored_task_id = (
                self._safe_external_task_id(restored_attempt[3].get("external_task_id"))
                if restored_attempt is not None else None
            )
            waiting_reconcile = self._waiting_reconcile_gap_states(gap_state_paths)
            if waiting_reconcile and restored_task_id is None:
                message = "外部补源结果不确定且没有 task_id，必须人工核对"
                self._progress(
                    job,
                    "needs_attention",
                    round=round_number,
                    tier=current_tier,
                    error=message,
                )
                result = self._waiting_reconcile_result(
                    request_body,
                    request_gaps,
                    tier=current_tier,
                    message=message,
                    needs_attention=True,
                )
                result["failure_scope"] = FAILURE_IN_DOUBT
                return result
            restored_selections = (
                restored_attempt[3].get("selections")
                if waiting_reconcile and restored_attempt is not None
                else None
            )
            reuse_existing_task = bool(
                waiting_reconcile and restored_task_id is not None
            )
            if reuse_existing_task:
                if (
                    not isinstance(restored_selections, list)
                    or not restored_selections
                    or any(not isinstance(row, Mapping) for row in restored_selections)
                    or not self._active_attempt_matches_selections(
                        restored_attempt[3],
                        [row for row in restored_selections if isinstance(row, Mapping)],
                    )
                ):
                    message = "已有外部补源任务缺少可验证候选快照，必须人工核对"
                    self._progress(
                        job, "waiting_reconcile", round=round_number,
                        tier=current_tier, error=message,
                        external_task_id=restored_task_id,
                    )
                    result = self._waiting_reconcile_result(
                        request_body, request_gaps, tier=current_tier,
                        message=message, external_task_id=restored_task_id,
                    )
                    result["failure_scope"] = FAILURE_IN_DOUBT
                    result["needs_attention"] = True
                    return result
                if not callable(getattr(self.materializer, "reconcile_existing_task", None)):
                    # An injected/legacy materializer cannot prove the
                    # external task status.  Do not call acquire (which may
                    # submit again) and leave the durable attempt untouched.
                    message = "当前 materializer 不支持已有 task_id 查询，必须人工核对"
                    self._progress(
                        job, "waiting_reconcile", round=round_number,
                        tier=current_tier, error=message,
                        external_task_id=restored_task_id,
                    )
                    result = self._waiting_reconcile_result(
                        request_body, request_gaps, tier=current_tier,
                        message=message, external_task_id=restored_task_id,
                    )
                    result["failure_scope"] = FAILURE_IN_DOUBT
                    result["needs_attention"] = True
                    return result
            self._progress(
                job,
                "waiting_reconcile" if reuse_existing_task else "provider_searching",
                round=round_number, tier=current_tier,
            )
            request_body["excluded_candidates"] = list(excluded)
            request_body["gaps"] = [dict(gap) for gap in request_gaps]
            # The policy state, not the first provider visible in this search
            # result, is the sole authority for selection.
            request_body["tier"] = current_tier
            for gap in request_gaps:
                gap_id = str(gap.get("id") or "")
                state_path = gap_state_paths.get(gap_id)
                if state_path is None:
                    continue
                state = json.loads(state_path.read_text(encoding="utf-8"))
                state.update({
                    "phase": "provider_searching",
                    "attempts": round_number,
                    "tier": current_tier,
                    "updated_at": _now(),
                    "error": None,
                })
                self._write_gap(state, state_path)
            # A durable external task is resumed from its reviewed candidate
            # snapshot.  The search adapter is deliberately not called: a
            # fresh search could select a different locator or accidentally
            # submit a second provider task.
            if reuse_existing_task:
                # A durable external task is resumed from its reviewed
                # snapshot only.  Positive memory is intentionally not mixed
                # into this path: changing the selected manifest could make
                # the task id point at a different provider operation.
                result: Mapping[str, object] = {
                    "candidates": list(restored_selections or []),
                }
                candidates = result.get("candidates")
            else:
                search_result = self.search.run(request_body)
                if not isinstance(search_result, Mapping):
                    raise AutomaticReplenishmentError("provider 搜索结果不是对象")
                fresh_candidates = search_result.get("candidates")
                if not isinstance(fresh_candidates, list):
                    raise AutomaticReplenishmentError("provider 搜索没有返回 candidates 数组")
                remembered_candidates = self._load_candidate_memory(
                    request_body, tier=current_tier,
                )
                # Search remains mandatory on every ordinary invocation.  A
                # remembered row is merely additional evidence; the selector
                # below applies the same identity, availability, coverage,
                # exclusion and strict-tier gates to both sources.
                candidates = [*remembered_candidates, *fresh_candidates]
                result = dict(search_result)
                result["candidates"] = candidates
                result["reused_candidate_count"] = len(remembered_candidates)
            if not isinstance(candidates, list):
                raise AutomaticReplenishmentError("provider 搜索没有返回 candidates 数组")
            try:
                selection_bundle = select_replenishment_candidates(
                    request_body, candidates, current_tier=current_tier,
                )
            except ValueError as exc:
                raise AutomaticReplenishmentError("补源当前 tier 无效") from exc
            selections = selection_bundle.get("selections")
            if not isinstance(selections, list) or not selections:
                # A durable external attempt cannot be advanced to another
                # tier merely because a fresh provider search is empty.  Keep
                # the existing task (including a delayed-visible AList task
                # with no id yet) in reconcile and never create a new submit.
                if reuse_existing_task:
                    message = "已有外部补源任务待核对，当前搜索无候选，拒绝新提交"
                    self._progress(
                        job,
                        "waiting_reconcile",
                        round=round_number,
                        tier=current_tier,
                        error=message,
                        external_task_id=restored_task_id,
                    )
                    result = self._waiting_reconcile_result(
                        request_body,
                        request_gaps,
                        tier=current_tier,
                        message=message,
                        external_task_id=restored_task_id,
                    )
                    result["failure_scope"] = FAILURE_IN_DOUBT
                    return result
                search_outcome = self._search_tier_outcome(
                    result if isinstance(result, Mapping) else {},
                    selection_bundle,
                    tier=current_tier,
                    shelf=self._shelf_for_request(request_body),
                )
                tier_results = self._apply_tier_outcome_to_gap_states(
                    gap_state_paths,
                    outcome=search_outcome,
                    updates={
                        "phase": "provider_searching",
                        "attempts": round_number,
                        "error": None,
                        "next_retry_at": None,
                        "updated_at": _now(),
                    },
                )
                statuses = {
                    str(item.get("status") or "") for item in tier_results
                }
                if statuses == {"advanced"}:
                    self._progress(
                        job,
                        "provider_searching",
                        round=round_number,
                        tier=current_tier,
                        tier_status="advanced",
                    )
                    continue
                tier_status = next(iter(statuses), "candidate_failed")
                phase = (
                    "waiting_reconcile"
                    if tier_status == "waiting_reconcile" else "retry_wait"
                )
                message = "当前补源 tier 没有可执行候选或完整穷尽证明"
                self._update_gap_states(
                    gap_state_paths,
                    updates={
                        "phase": phase,
                        "error": message,
                        "updated_at": _now(),
                    },
                )
                self._progress(
                    job, phase, round=round_number, tier=current_tier,
                    tier_status=tier_status, error=message,
                )
                return {
                    "request": request_body,
                    "resolved_gap_ids": sorted(resolved_total),
                    "unresolved_gap_ids": [
                        str(gap.get("id") or "") for gap in request_gaps
                        if isinstance(gap.get("id"), str) and gap.get("id")
                    ],
                    "tier": current_tier,
                    "tier_status": tier_status,
                    "error": message,
                }
            # ``_run_request`` is the legacy media lane.  It is intentionally
            # never allowed to ask a materializer for an optional sidecar:
            # RootJob owns the only automatic subtitle acquisition/write path.
            selections, selected_companion_gap_ids = (
                self._strip_legacy_companion_subtitle_members(
                    [row for row in selections if isinstance(row, Mapping)],
                )
            )
            if not selections:
                raise AutomaticReplenishmentError("补源选择缺少有效候选")
            # Selection/search is read-only. Do not turn it into a staging
            # write once a live control change has stopped this root.
            self._raise_if_cancelled(
                job, round_number=round_number, boundary="materialization",
            )
            if restored_attempt is None:
                attempt_id = f"attempt-{uuid.uuid4().hex}"
                staging = f"{self.staging_root}/{job.id}/{attempt_id}"
                workspace = self.workspace_root / job.id / attempt_id
                restored_record: dict[str, object] | None = None
            else:
                attempt_id, staging, workspace, restored_record = restored_attempt
                if not self._active_attempt_matches_selections(restored_record, selections):
                    raise AutomaticReplenishmentError(
                        "保留的补源 attempt 与当前候选不一致，等待重试核对",
                    )
            restored_task_id = (
                self._safe_external_task_id(restored_record.get("external_task_id"))
                if isinstance(restored_record, Mapping) else None
            )
            active_attempt = self._active_attempt_record(
                job_id=job.id,
                attempt_id=attempt_id,
                staging_root=staging,
                workspace=workspace,
                selections=[row for row in selections if isinstance(row, Mapping)],
                external_task_id=restored_task_id,
            )
            selected_providers, _selected_markers = self._selection_markers(
                [row for row in selections if isinstance(row, Mapping)],
            )
            if selected_providers != {current_tier}:
                raise AutomaticReplenishmentError(
                    "补源选择包含当前 tier 以外的 provider"
                )
            for gap in request_gaps:
                gap_id = str(gap.get("id") or "")
                state_path = gap_state_paths.get(gap_id)
                if state_path is None:
                    continue
                state = json.loads(state_path.read_text(encoding="utf-8"))
                state.update({
                    "phase": "acquiring",
                    "attempts": round_number,
                    "updated_at": _now(),
                    "error": None,
                    "staging_root": staging,
                    "active_attempt": active_attempt,
                    "next_retry_at": None,
                })
                # Never overwrite durable tier state from the chosen provider:
                # the selection was already constrained by ``current_tier``.
                state["tier"] = current_tier
                if isinstance(active_attempt.get("external_task_id"), str):
                    state["external_task_id"] = active_attempt["external_task_id"]
                self._write_gap(state, state_path)
            companion_migration_gap_ids = self._mark_legacy_companion_subtitle_migration(
                gap_state_paths,
                gap_ids=selected_companion_gap_ids,
            )
            staging_files: list[StagingFile] = []
            child: EngineJob | None = None
            completed_child: EngineJob | None = None
            attempt_error: Exception | None = None
            candidate_exclusions: list[dict[str, object]] = []
            try:
                self._progress(job, "acquiring", round=round_number, staging_root=staging)
                # Keep this immediately adjacent to the materializer: a pause
                # can arrive while local gap state is being updated above.
                self._raise_if_cancelled(
                    job, round_number=round_number, boundary="materialization",
                )
                selected_rows = [
                    dict(row) for row in selections if isinstance(row, Mapping)
                ]
                provider_pause = self._provider_pause_callback(
                    job,
                    round_number=round_number,
                    boundary="provider_materializer",
                )
                if reuse_existing_task:
                    acquisition = _call_with_pause(
                        self.materializer.reconcile_existing_task,
                        request_body,
                        selected_rows,
                        staging_root=staging,
                        workspace=workspace,
                        alist=self.alist,
                        external_task_id=restored_task_id,
                        pause_requested=provider_pause,
                    )
                else:
                    acquisition = _call_with_pause(
                        self.materializer.acquire,
                        request_body,
                        selected_rows,
                        staging_root=staging,
                        workspace=workspace,
                        alist=self.alist,
                        pause_requested=provider_pause,
                    )
                if not isinstance(acquisition, Mapping):
                    raise AutomaticReplenishmentError("provider delivery 不是对象")
                external_task_id = self._safe_external_task_id(
                    acquisition.get("external_task_id")
                    if isinstance(acquisition, Mapping) else None
                )
                if external_task_id is not None:
                    active_attempt = self._active_attempt_record(
                        job_id=job.id,
                        attempt_id=attempt_id,
                        staging_root=staging,
                        workspace=workspace,
                        selections=[
                            row for row in selections if isinstance(row, Mapping)
                        ],
                        external_task_id=external_task_id,
                    )
                    self._update_gap_states(
                        gap_state_paths,
                        updates={
                            "active_attempt": active_attempt,
                            "external_task_id": external_task_id,
                            "updated_at": _now(),
                        },
                    )
                # A downloader can finish just as the operator pauses. Stop
                # before even verifying/claiming its result; no child or
                # subtitle write may follow that boundary.
                self._raise_if_cancelled(
                    job, round_number=round_number, boundary="post_materialization",
                )
                self._progress(job, "staging_verifying", round=round_number, staging_root=staging)
                staging_files = self.verify_staging(staging)
                acquisition = self._verify_delivery_contract(
                    acquisition,
                    job=job,
                    attempt_id=attempt_id,
                    staging_root=staging,
                    staging_files=staging_files,
                )
                self._admit_delivery_videos(
                    acquisition,
                    staging_files,
                    [row for row in selections if isinstance(row, Mapping)],
                )
                # A media delivery may still contain a sidecar if an old or
                # third-party materializer ignored the stripped select-file
                # map.  Keep it outside the child source tree and record only
                # a durable RootJob migration notice; this compatibility lane
                # must never validate, stage-as-subtitle, or write that file.
                delivered_companion_gap_ids, delivered_companion_count = (
                    self._delivered_legacy_companion_subtitle_gap_ids(
                        acquisition,
                        known_gap_ids=set(gap_state_paths),
                    )
                )
                companion_migration_gap_ids = sorted(set(
                    companion_migration_gap_ids
                ) | set(self._mark_legacy_companion_subtitle_migration(
                    gap_state_paths,
                    gap_ids=delivered_companion_gap_ids,
                    delivered_member_count=delivered_companion_count,
                )))
                has_video = any(item.kind == "video" for item in staging_files)
                if not has_video:
                    raise AutomaticReplenishmentError("视频补源 staging 没有可回投的视频")
                # Planning is durable local state; check before creating it as
                # well as immediately before the formal child write.
                self._raise_if_cancelled(
                    job, round_number=round_number, boundary="child_plan",
                )
                child_request = dict(job.request)
                child_request["source_path"] = self._media_child_staging_root(
                    staging, staging_files,
                )
                if not self._request_inherits_target_shelf(job, child_request):
                    raise AutomaticReplenishmentError(
                        "补源 child 请求未继承根任务的目标货架，拒绝规划"
                    )
                child = self._plan_internal_child(
                    child_request,
                    root_job_id=job.id,
                    pause_requested=self._provider_pause_callback(
                        job,
                        round_number=round_number,
                        boundary="child_plan",
                    ),
                )
                if not self._child_inherits_target_shelf(job, child):
                    raise AutomaticReplenishmentError(
                        "补源 child 未继承根任务的目标货架，拒绝写入"
                    )
                if not self._audit_child_target_matches(job, child):
                    raise AutomaticReplenishmentError(
                        "审计补源 child 目标与已审计正式目录不一致，拒绝写入"
                    )
                self._progress(
                    job, "child_planning", round=round_number,
                    staging_root=staging, child_job_id=child.id,
                    child_phase=child.phase,
                )
                self._progress(
                    job, "child_executing", round=round_number,
                    child_job_id=child.id, child_phase="executing",
                )
                self._raise_if_cancelled(
                    job, round_number=round_number, boundary="child_write",
                )
                # The shared adapter combines global pause and the root/pilot
                # cancellation fence. It is ``None`` only for unscoped
                # library/test callers; a scoped runtime must prove that the
                # child writer accepts it.
                child_pause = self._provider_pause_callback(
                    job,
                    round_number=round_number,
                    boundary="child_write",
                )
                completed_child = _call_with_pause(
                    self.engine_runner.execute_automatic,
                    child.id,
                    pause_requested=child_pause,
                )
                if completed_child.phase != "executed":
                    self._raise_if_paused(
                        job, round_number=round_number,
                        boundary="child_resume",
                    )
                    raise AutomaticReplenishmentError("补源 child 未完成")
                self._progress(
                    job, "final_verifying", round=round_number,
                    child_job_id=completed_child.id,
                    child_phase=completed_child.phase,
                )
            except Exception as exc:
                attempt_error = exc
                # Capture the materializer's explicit candidate attribution
                # before cleanup can wrap the error as an infrastructure
                # failure.  A generic exception is intentionally not enough
                # evidence to poison a provider identity across restarts.
                candidate_exclusions = self._failure_candidate_exclusions(
                    exc,
                    [row for row in selections if isinstance(row, Mapping)],
                )
                if child is not None:
                    if isinstance(exc, AutomaticReplenishmentCancelled):
                        if isinstance(exc, AutomaticReplenishmentPaused):
                            # A child that is still only ``planned`` has not
                            # crossed the Engine's formal-write boundary, so
                            # remove that local implementation record instead
                            # of leaving an orphan that a later retry cannot
                            # safely associate with this attempt.  Once the
                            # child is executing/verifying/cleaning, preserve
                            # it: its remote operation may already have
                            # started and restart readback owns the decision.
                            child_at_boundary = child
                            get_child = getattr(self.engine_runner, "get_job", None)
                            if callable(get_child):
                                try:
                                    observed_child = get_child(child.id)
                                    if isinstance(observed_child, EngineJob):
                                        child_at_boundary = observed_child
                                except Exception:
                                    pass
                            if child_at_boundary.phase == "planned":
                                cancel = getattr(self.engine_runner, "cancel_job", None)
                                if callable(cancel):
                                    try:
                                        cancel(
                                            child_at_boundary.id,
                                            reason="provider paused before child write",
                                        )
                                    except TypeError:
                                        try:
                                            cancel(child_at_boundary.id)
                                        except Exception:
                                            pass
                                    except Exception:
                                        pass
                                    else:
                                        child = None
                            else:
                                child = child_at_boundary
                            continue_cleanup = False
                        else:
                            continue_cleanup = True
                        # A plan may have been persisted in the narrow window
                        # between the pre-plan check and the pre-execute
                        # check.  Cancel that local child record without
                        # touching AList, rather than leaving a misleading
                        # planned implementation task behind.
                        cancel = getattr(self.engine_runner, "cancel_job", None) if continue_cleanup else None
                        if callable(cancel):
                            try:
                                cancel(child.id, reason="provider paused before child write")
                            except TypeError:
                                try:
                                    cancel(child.id)
                                except Exception:
                                    pass
                            except Exception:
                                pass
                    else:
                        self._progress(
                            job, "child_failed", round=round_number,
                            child_job_id=child.id, child_phase="failed",
                            error=redact_error(exc),
                        )
            finally:
                # A successful child now retains its task-owned attempt for
                # the post-acquisition scoped audit below. Candidate-local
                # invalid payloads alone may be removed here; infrastructure,
                # delivery, in-doubt and cancellation remain restartable.
                if self._cleanup_attempt_after_error(attempt_error, candidate_exclusions):
                    try:
                        self._progress(job, "cleaning", round=round_number, staging_root=staging)
                        cleanup_pause = self._provider_pause_callback(
                            job,
                            round_number=round_number,
                            boundary="candidate_cleanup",
                        )
                        self._remove_staging(
                            staging,
                            pause_requested=cleanup_pause,
                        )
                        self._remove_local_attempt_workspace(
                            job_id=job.id, attempt_id=attempt_id,
                            pause_requested=cleanup_pause,
                        )
                    except AutomaticReplenishmentCancelled:
                        raise
                    except Exception as cleanup_exc:
                        if attempt_error is None:
                            attempt_error = cleanup_exc
                        else:
                            attempt_error = AutomaticReplenishmentError(
                                f"补源尝试失败且 staging 清理失败: {attempt_error}; {cleanup_exc}"
                            )
            if attempt_error is not None:
                scope = self._failure_scope(attempt_error, candidate_exclusions)
                # Capture this fact before a concurrent pause replaces the
                # surfaced exception.  A pause must not resurrect an already
                # confirmed-dead external task as an in-flight barrier.
                external_task_final = self._failure_external_task_is_final(
                    attempt_error,
                )
                if (
                    scope == FAILURE_INFRASTRUCTURE
                    and not isinstance(attempt_error, AutomaticReplenishmentCancelled)
                ):
                    try:
                        self._raise_if_cancelled(
                            job,
                            round_number=round_number,
                            boundary="attempt_failure",
                        )
                    except AutomaticReplenishmentCancelled as cancel_exc:
                        attempt_error = cancel_exc
                        candidate_exclusions = []
                        scope = self._failure_scope(
                            attempt_error, candidate_exclusions,
                        )
                final_external_task = external_task_final
                failure_task_id = (
                    None
                    if final_external_task else (
                        self._safe_external_task_id(
                            active_attempt.get("external_task_id")
                        )
                        or self._failure_external_task_id(attempt_error)
                    )
                )
                if failure_task_id is not None:
                    active_attempt = self._active_attempt_record(
                        job_id=job.id,
                        attempt_id=attempt_id,
                        staging_root=staging,
                        workspace=workspace,
                        selections=[
                            row for row in selections if isinstance(row, Mapping)
                        ],
                        external_task_id=failure_task_id,
                    )
                failure_phase = (
                    "waiting_reconcile"
                    if scope == FAILURE_IN_DOUBT else "retry_wait"
                )
                failure_updates: dict[str, object] = {
                    "phase": failure_phase,
                    "updated_at": _now(),
                    "error": redact_error(attempt_error),
                    "last_error_scope": scope,
                    "next_retry_at": None,
                    "active_attempt": (
                        None if (
                            scope == FAILURE_CANDIDATE or final_external_task
                        ) else active_attempt
                    ),
                }
                if final_external_task:
                    # AList state 4 was made durable before its row was
                    # deleted.  The old task cannot consume bytes or complete
                    # later, so leaving its id/attempt behind would make the
                    # next same-tier retry reconcile a dead row forever.
                    failure_updates["external_task_id"] = None
                elif failure_task_id is not None:
                    failure_updates["external_task_id"] = failure_task_id
                if scope in {FAILURE_INFRASTRUCTURE, FAILURE_IN_DOUBT}:
                    # Network/auth/helper failures and ambiguous external
                    # submissions are policy outcomes, not candidate evidence.
                    # The pure state machine keeps their tier unchanged and
                    # gives in-doubt attempts the durable reconcile state.
                    self._apply_tier_outcome_to_gap_states(
                        gap_state_paths,
                        outcome={
                            "scope": scope,
                            **({"external_task_id": failure_task_id}
                               if failure_task_id is not None else {}),
                        },
                        updates=failure_updates,
                    )
                else:
                    self._update_gap_states(
                        gap_state_paths,
                        gap_ids={
                            str(gap.get("id") or "") for gap in request_gaps
                            if isinstance(gap.get("id"), str) and gap.get("id")
                        },
                        updates=failure_updates,
                    )
                # Keep the last bounded candidate failure visible while a
                # later round is running. Without this projection a fresh
                # search masks whether the failure was manifest, payload,
                # delivery, cleanup, or child planning.
                self._progress(
                    job,
                    failure_phase,
                    round=round_number,
                    error=redact_error(attempt_error),
                    failure_scope=scope,
                    attempt_failure_stage=getattr(
                        attempt_error, "failure_stage", None,
                    ),
                )
                # Do not exclude the candidate or continue into a second
                # round after a pause/allowlist stop. ``run_for_job`` records
                # the durable per-gap retry_wait state for this exception.
                if isinstance(attempt_error, AutomaticReplenishmentCancelled):
                    raise attempt_error
                if scope != FAILURE_CANDIDATE:
                    raise attempt_error
                if candidate_exclusions:
                    self._persist_excluded_candidates(
                        gap_state_paths, candidate_exclusions,
                    )
                else:
                    raise attempt_error
                excluded = self._merge_excluded_candidates(
                    excluded, candidate_exclusions,
                )
                if round_number >= self.max_candidate_rounds:
                    self._progress(
                        job, "retry_wait", round=round_number,
                        error=redact_error(attempt_error),
                        failure_scope=scope,
                    )
                    raise _CandidateRoundLimitError(
                        f"补源已尝试 {round_number} 轮仍失败: {attempt_error}"
                    ) from attempt_error
                continue
            resolved_now = set()
            if completed_child is not None:
                resolved_now.update(
                    self._child_resolved_gap_ids(completed_child, request_gaps)
                )
            resolved_now &= {
                str(gap.get("id")) for gap in request_gaps
                if isinstance(gap.get("id"), str) and gap.get("id")
            }
            if not resolved_now:
                self._update_gap_states(
                    gap_state_paths,
                    gap_ids={
                        str(gap.get("id") or "") for gap in request_gaps
                        if isinstance(gap.get("id"), str) and gap.get("id")
                    },
                    updates={
                        "active_attempt": None,
                        "next_retry_at": None,
                        "updated_at": _now(),
                    },
                )
                raise AutomaticReplenishmentError("补源 child 实际文件未覆盖当前 gap")
            post_acquisition_reaudit = self._mark_post_acquisition_reaudit(
                gap_state_paths,
                job_id=job.id,
                attempt_id=attempt_id,
                staging_root=staging,
                selected_gap_ids=resolved_now,
                child_job_id=(
                    completed_child.id if completed_child is not None else None
                ),
            )
            # The child/readback path has now proven the selected coordinates.
            # Keep only those positive rows whose selected gaps were actually
            # resolved; candidate metadata alone is never promoted to memory.
            self._remember_verified_candidates(
                request_body,
                tier=current_tier,
                selections=[row for row in selections if isinstance(row, Mapping)],
                resolved_gap_ids=resolved_now,
            )
            resolved_total.update(resolved_now)
            pending = [
                gap for gap in request_gaps
                if str(gap.get("id") or "") not in resolved_now
            ]
            if pending:
                pending_ids = {
                    str(gap.get("id") or "") for gap in pending
                    if isinstance(gap.get("id"), str) and gap.get("id")
                }
                excluded = self._merge_excluded_candidates(
                    excluded,
                    [
                        self._excluded_selection(row)
                        for row in selections if isinstance(row, Mapping)
                    ],
                )
                self._update_gap_states(
                    gap_state_paths,
                    gap_ids=pending_ids,
                    updates={
                        "active_attempt": None,
                        "last_error_scope": FAILURE_CANDIDATE,
                        "next_retry_at": None,
                        "updated_at": _now(),
                    },
                )
                if round_number >= self.max_candidate_rounds:
                    self._progress(
                        job, "retry_wait", round=round_number,
                        error="补源 child 只覆盖了部分 gap",
                    )
                    raise AutomaticReplenishmentError("补源 child 只覆盖了部分 gap")
                # The successfully written subset now waits for its own
                # post-acquisition audit.  Do not let later candidate rounds
                # overwrite those durable re-audit markers.
                gap_state_paths = {
                    gap_id: path
                    for gap_id, path in gap_state_paths.items()
                    if gap_id in pending_ids
                }
                request_gaps = pending
                continue
            return {
                "request": request_body,
                "resolved_gap_ids": sorted(resolved_total),
                "staging_files": [item.as_dict() for item in staging_files],
                "post_acquisition_reaudit": post_acquisition_reaudit,
                **({"child_job_id": completed_child.id} if completed_child is not None else {}),
                **({
                    "legacy_companion_subtitle_migration": {
                        "status": "required",
                        "manual_action": "migrate_rootjob_subtitle",
                        "gap_ids": companion_migration_gap_ids,
                    },
                } if companion_migration_gap_ids else {}),
            }
        raise AutomaticReplenishmentError("补源没有可执行候选")

    def _retire_legacy_subtitle_request(
        self,
        *,
        job: EngineJob,
        request: Mapping[str, object],
        gap_state_paths: Mapping[str, Path],
    ) -> dict[str, object]:
        """Fail closed instead of using the retired legacy subtitle writer.

        New RootJob work owns the only automatic subtitle transaction: it
        persists a submit intent, reconciles task-scoped staging, and performs
        the one merged bilingual-sidecar write.  The legacy EngineJob lane
        cannot provide those same restart guarantees, so it must never search,
        download, stage, or write a subtitle merely because an old record is
        still scheduled.  Keep an explicit durable operator-visible marker
        rather than converting it into a retry or a misleading completion.
        """
        request_body = dict(request)
        request_gaps = self._request_gaps(request_body)
        if self._validated_request_lane(request_body) != "subtitle":
            raise AutomaticReplenishmentError(
                "视频缺口不得进入已停用的字幕自动通道"
            )
        message = _LEGACY_SUBTITLE_MIGRATION_MESSAGE
        affected_ids: list[str] = []
        for gap in request_gaps:
            gap_id = gap.get("id")
            if not isinstance(gap_id, str) or not gap_id:
                continue
            state_path = gap_state_paths.get(gap_id)
            if state_path is None:
                continue
            try:
                raw_state = json.loads(state_path.read_text(encoding="utf-8"))
            except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise AutomaticReplenishmentError(
                    "旧字幕任务状态不可读取，拒绝自动重试"
                ) from exc
            if not isinstance(raw_state, Mapping):
                raise AutomaticReplenishmentError(
                    "旧字幕任务状态无效，拒绝自动重试"
                )
            self._mark_legacy_subtitle_migration(
                gap=gap,
                job_id=job.id,
                state_path=state_path,
                prior_state=raw_state,
            )
            affected_ids.append(gap_id)
        self._progress(
            job,
            "needs_attention",
            lane="subtitle",
            message=message,
            migration_required=True,
        )
        return {
            "request": request_body,
            "resolved_gap_ids": [],
            "unresolved_gap_ids": affected_ids,
            "terminal": True,
            "status": "needs_attention",
            "manual_action": "migrate_rootjob_subtitle",
            "migration_required": True,
            "error": message,
        }

    def _run_subtitle_request(
        self,
        *,
        job: EngineJob,
        request: Mapping[str, object],
        gap_state_paths: Mapping[str, Path],
    ) -> dict[str, object]:
        """Compatibility stop for code that still references the retired lane."""
        return self._retire_legacy_subtitle_request(
            job=job,
            request=request,
            gap_state_paths=gap_state_paths,
        )

    def run_for_job(self, job: EngineJob) -> dict[str, object]:
        """Automatically resolve all engine-discovered, provider-compatible gaps."""
        if not isinstance(job.plan, Mapping):
            raise AutomaticReplenishmentError("根任务缺少可用 Engine 计划")
        self._progress(job, "gap_discovering")
        # NFO-bootstrapped audit roots often have only a local-language title.
        # Enrich the in-memory request with aliases from the authoritative
        # TMDB record before querying providers; this does not alter the root
        # job or weaken candidate identity checks.
        request_plan = enrich_replenishment_plan_aliases(
            job.plan, getattr(self.engine_runner, "tmdb", None),
        )
        request_bundle = build_replenishment_requests(
            request_plan, job_id=job.id, round_number=1,
        )
        requests = request_bundle.get("requests") if isinstance(request_bundle, Mapping) else None
        if not isinstance(requests, list):
            raise AutomaticReplenishmentError("Engine gap 请求格式无效")
        # A subtitle-audit root is a sidecar-only transaction.  Validate the
        # complete split before creating any gap state or invoking a provider;
        # a malformed/mixed plan must not leak a media request first and only
        # then fail at the per-request dispatch boundary.
        if job.summary.get("audit_subtitle_only") is True:
            for candidate_request in requests:
                if not isinstance(candidate_request, Mapping):
                    raise AutomaticReplenishmentError("字幕审计请求格式无效")
                raw_candidate_gaps = candidate_request.get("gaps")
                if isinstance(raw_candidate_gaps, list) and raw_candidate_gaps:
                    if self._validated_request_lane(candidate_request) != "subtitle":
                        raise AutomaticReplenishmentError(
                            "字幕专属任务包含视频或混合缺口，拒绝执行"
                        )
                elif raw_candidate_gaps not in (None, []):
                    raise AutomaticReplenishmentError("字幕审计请求 gap 列表无效")
        outcomes: list[dict[str, object]] = []
        already_resolved: list[str] = []
        already_exhausted: list[str] = []
        legacy_subtitle_migration: list[str] = []
        legacy_companion_subtitle_migration: list[str] = []
        pending_reaudit: list[str] = []
        for request in requests:
            if not isinstance(request, Mapping):
                raise AutomaticReplenishmentError("补源请求不是对象")
            raw_request_gaps = request.get("gaps")
            if not isinstance(raw_request_gaps, list):
                raise AutomaticReplenishmentError("补源请求缺少 gap 列表")
            if any(not isinstance(gap, Mapping) for gap in raw_request_gaps):
                raise AutomaticReplenishmentError("补源请求包含无效 gap 行")
            if not raw_request_gaps:
                continue
            # Validate the complete row set before filtering durable rows or
            # invoking either provider lane.  A valid subtitle row paired
            # with a malformed sibling must not be executed in isolation.
            self._validated_request_lane(request)
            active_gaps: list[dict[str, object]] = []
            states: dict[str, Path] = {}
            for gap in self._request_gaps(request):
                gap_id = gap.get("id")
                if not isinstance(gap_id, str) or not gap_id:
                    continue
                path = self._gap_path(job_id=job.id, gap_id=gap_id)
                state: Mapping[str, object] | None = None
                if path.exists():
                    try:
                        state = json.loads(path.read_text(encoding="utf-8"))
                    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
                        state = None
                    if (
                        isinstance(state, Mapping)
                        and str(gap.get("kind") or "") != "missing_subtitle"
                        and isinstance(
                            state.get(_LEGACY_COMPANION_SUBTITLE_MIGRATION_KEY),
                            Mapping,
                        )
                        and state[_LEGACY_COMPANION_SUBTITLE_MIGRATION_KEY].get(
                            "status"
                        ) == "required"
                    ):
                        legacy_companion_subtitle_migration.append(gap_id)
                    if (
                        str(gap.get("kind") or "") == "missing_subtitle"
                        and isinstance(state, Mapping)
                        and (
                            state.get("subtitle_migration_required") is True
                            or state.get("phase") == "completed_with_gaps"
                        )
                    ):
                        # Markerless ``completed_with_gaps`` is the durable
                        # shape written by the pre-retirement subtitle lane.
                        # Normalize it before any re-audit/provider branch so
                        # a historic row can never be mistaken for success.
                        self._mark_legacy_subtitle_migration(
                            gap=gap,
                            job_id=job.id,
                            state_path=path,
                            prior_state=state,
                        )
                        legacy_subtitle_migration.append(gap_id)
                        continue
                    if (
                        isinstance(state, Mapping)
                        and self._post_acquisition_reaudit_blocks_provider(state)
                    ):
                        # A completed child must not become a fresh provider
                        # request while the targeted audit is in flight or
                        # uncertain.  A completed audit that still reports
                        # the gap as actionable is deliberately allowed back
                        # into M; its old staging remains task-owned evidence.
                        pending_reaudit.append(gap_id)
                        continue
                    if (
                        isinstance(state, Mapping)
                        and state.get("phase") == "completed_with_gaps"
                    ):
                        # Exhaustion is lane-local durable evidence. A retry
                        # for a sibling media gap must not restart subtitle
                        # discovery (and vice versa). Retired subtitle rows
                        # are handled above so they never look complete.
                        already_exhausted.append(gap_id)
                        continue
                    if (
                        isinstance(state, Mapping)
                        and state.get("phase") == "resolved"
                        and not (
                            isinstance(state.get("gap"), Mapping)
                            and state["gap"].get("source") == "automatic_library_audit"
                        )
                    ):
                        already_resolved.append(gap_id)
                        continue
                states[gap_id] = self._write_gap(
                    self._gap_state(
                        gap,
                        job_id=job.id,
                        prior_state=state if isinstance(state, Mapping) else None,
                    ),
                    path,
                )
                active_gaps.append(gap)
            if not active_gaps:
                continue
            active_request = dict(request)
            active_request["gaps"] = active_gaps
            try:
                lane = self._validated_request_lane(active_request)
                if (
                    job.summary.get("audit_subtitle_only") is True
                    and lane != "subtitle"
                ):
                    raise AutomaticReplenishmentError(
                        "字幕专属任务包含视频缺口，拒绝执行"
                    )
                if lane == "subtitle":
                    retired = self._retire_legacy_subtitle_request(
                        job=job,
                        request=active_request,
                        gap_state_paths=states,
                    )
                    outcomes.append(retired)
                    if retired.get("migration_required") is True:
                        legacy_subtitle_migration.extend(
                            gap_id
                            for gap_id in retired.get("unresolved_gap_ids", [])
                            if isinstance(gap_id, str) and gap_id
                        )
                elif lane == "media":
                    media_outcome = self._run_request(
                        job=job,
                        request=active_request,
                        gap_state_paths=states,
                    )
                    outcomes.append(media_outcome)
                    raw_companion_migration = media_outcome.get(
                        "legacy_companion_subtitle_migration"
                    )
                    if (
                        isinstance(raw_companion_migration, Mapping)
                        and raw_companion_migration.get("status") == "required"
                        and isinstance(raw_companion_migration.get("gap_ids"), list)
                    ):
                        legacy_companion_subtitle_migration.extend(
                            gap_id
                            for gap_id in raw_companion_migration["gap_ids"]
                            if isinstance(gap_id, str)
                            and gap_id
                            and gap_id in states
                        )
            except AutomaticReplenishmentCancelled as exc:
                scope = self._failure_scope(exc, [])
                subtitle_lane = request.get("lane") == "subtitle"
                for gap_id, path in states.items():
                    state = json.loads(path.read_text(encoding="utf-8"))
                    raw_companion_migration = state.get(
                        _LEGACY_COMPANION_SUBTITLE_MIGRATION_KEY
                    )
                    if (
                        not subtitle_lane
                        and isinstance(raw_companion_migration, Mapping)
                        and raw_companion_migration.get("status") == "required"
                    ):
                        legacy_companion_subtitle_migration.append(gap_id)
                    if (
                        state.get("phase") != "resolved"
                        and not self._post_acquisition_reaudit_blocks_provider(state)
                    ):
                        if subtitle_lane:
                            self._project_subtitle_lane_state(
                                state,
                                phase="retry_wait",
                                error=redact_error(exc),
                                failure_scope=scope,
                            )
                        else:
                            state.update({
                                "phase": "retry_wait",
                                "updated_at": _now(),
                                "error": redact_error(exc),
                                "last_error_scope": scope,
                                "next_retry_at": None,
                            })
                        self._write_gap(state, path)
                outcomes.append({
                    "request": active_request,
                    "resolved_gap_ids": [],
                    "error": redact_error(exc),
                    "failure_scope": scope,
                    "cancelled": True,
                })
                # Do not create state or invoke a provider for another request
                # group once the operator has paused this root.
                return {
                    "job_id": job.id,
                    "outcomes": outcomes,
                    "already_resolved_gap_ids": sorted(set(already_resolved)),
                    "already_exhausted_gap_ids": sorted(set(already_exhausted)),
                    "legacy_subtitle_migration_gap_ids": sorted(
                        set(legacy_subtitle_migration)
                        | set(legacy_companion_subtitle_migration)
                    ),
                    "legacy_companion_subtitle_migration_gap_ids": sorted(
                        set(legacy_companion_subtitle_migration)
                    ),
                    "pending_reaudit_gap_ids": sorted(set(pending_reaudit)),
                    "unresolved_gaps": list(request_bundle.get("unresolved_gaps") or []),
                    "cancelled": True,
                }
            except Exception as exc:
                scope = self._failure_scope(exc, [])
                final_external_task = bool(
                    request.get("lane") != "subtitle"
                    and self._failure_external_task_is_final(exc)
                )
                task_id = (
                    None if final_external_task
                    else self._failure_external_task_id(exc)
                )
                failure_phase = (
                    "waiting_reconcile"
                    if scope == FAILURE_IN_DOUBT else "retry_wait"
                )
                subtitle_lane = request.get("lane") == "subtitle"
                for gap_id, path in states.items():
                    state = json.loads(path.read_text(encoding="utf-8"))
                    raw_companion_migration = state.get(
                        _LEGACY_COMPANION_SUBTITLE_MIGRATION_KEY
                    )
                    if (
                        not subtitle_lane
                        and isinstance(raw_companion_migration, Mapping)
                        and raw_companion_migration.get("status") == "required"
                    ):
                        legacy_companion_subtitle_migration.append(gap_id)
                    if (
                        state.get("phase") != "resolved"
                        and not self._post_acquisition_reaudit_blocks_provider(state)
                    ):
                        if subtitle_lane:
                            self._project_subtitle_lane_state(
                                state,
                                phase=failure_phase,
                                error=redact_error(exc),
                                failure_scope=scope,
                            )
                        else:
                            state.update({
                                "phase": failure_phase,
                                "updated_at": _now(),
                                "error": redact_error(exc),
                                "last_error_scope": scope,
                                "next_retry_at": None,
                            })
                        if final_external_task and not subtitle_lane:
                            # ``_run_request`` normally consumed this marker
                            # already.  Keep the outer exception boundary
                            # idempotent for an error raised before that inner
                            # state update: no dead task may be reattached.
                            state["active_attempt"] = None
                            state["external_task_id"] = None
                        elif task_id is not None and not subtitle_lane:
                            state["external_task_id"] = task_id
                        self._write_gap(state, path)
                outcomes.append({
                    "request": active_request,
                    "resolved_gap_ids": [],
                    "error": redact_error(exc),
                    "failure_scope": scope,
                })
        return {
            "job_id": job.id,
            "outcomes": outcomes,
            "already_resolved_gap_ids": sorted(set(already_resolved)),
            "already_exhausted_gap_ids": sorted(set(already_exhausted)),
            "legacy_subtitle_migration_gap_ids": sorted(
                set(legacy_subtitle_migration)
                | set(legacy_companion_subtitle_migration)
            ),
            "legacy_companion_subtitle_migration_gap_ids": sorted(
                set(legacy_companion_subtitle_migration)
            ),
            "pending_reaudit_gap_ids": sorted(set(pending_reaudit)),
            "unresolved_gaps": list(request_bundle.get("unresolved_gaps") or []),
        }


__all__ = [
    "AutomaticMaterializer",
    "AutomaticProviderSearch",
    "AutomaticReplenishmentCancelled",
    "AutomaticReplenishmentPaused",
    "AutomaticReplenishmentError",
    "AutomaticReplenishmentRuntime",
    "CANONICAL_REPLENISHMENT_STAGING_ROOT",
    "FixedTierAutomaticMaterializer",
    "LocalTorrentAutomaticMaterializer",
    "QuarkFastSaveAutomaticMaterializer",
    "StagingFile",
    "reconcile_interrupted_gap_states",
]
