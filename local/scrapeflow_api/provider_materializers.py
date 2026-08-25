"""The two task-owned replenishment delivery boundaries.

This module intentionally contains no scheduler, global audit, pilot, or
second deployment workflow.  A selected RootJob owns the orchestration in
``root_replenishment``; these classes only materialize one already-reviewed
candidate into that RootJob's staging attempt.
"""

from __future__ import annotations

import functools
import inspect
import json
import posixpath
import re
from datetime import UTC, datetime
from pathlib import Path
from typing import Callable, Mapping, Sequence

from engine.scrapeflow.media_policy import SUBTITLE_EXTENSIONS, VIDEO_EXTENSIONS
from engine.scrapeflow.serialization import atomic_write_json

from .provider_delivery import ProviderDeliveryError, validate_provider_delivery
from .provider_staging import (
    CANONICAL_REPLENISHMENT_STAGING_ROOT,
    ProviderStagingPathError,
    validate_provider_staging_root,
)
from .replenishment_tiers import TIER_LOCAL_MAGNET, TIER_QUARK_SHARE


_VIDEO_EXTENSIONS = VIDEO_EXTENSIONS
_SUBTITLE_EXTENSIONS = SUBTITLE_EXTENSIONS
_TASK_STAGING_SEGMENT = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
_SAFE_EXTERNAL_TASK_ID = re.compile(r"^[A-Za-z0-9._:-]{1,256}$")


class ProviderMaterializerError(RuntimeError):
    """A provider could not materialize the reviewed candidate safely."""


class ProviderMaterializerPaused(ProviderMaterializerError):
    """Stop before the next provider side effect and let the RootJob resume."""

    pause_requested = True


def _pause_checkpoint(pause_requested: Callable[[], bool] | None) -> None:
    """Fence each external operation on the selected RootJob pause switch."""
    if pause_requested is None:
        return
    try:
        paused = bool(pause_requested())
    except ProviderMaterializerPaused:
        raise
    except Exception as exc:
        raise ProviderMaterializerPaused("补源暂停状态不可确认，已安全停止") from exc
    if paused:
        raise ProviderMaterializerPaused("补源已暂停")


class _PauseCheckedPort:
    """Apply the same pause boundary to every AList operation."""

    def __init__(self, target: object, pause_requested: Callable[[], bool] | None) -> None:
        self._target = target
        self._pause_requested = pause_requested

    def __getattr__(self, name: str) -> object:
        value = getattr(self._target, name)
        if not callable(value):
            return value

        @functools.wraps(value)
        def guarded(*args: object, **kwargs: object) -> object:
            _pause_checkpoint(self._pause_requested)
            return value(*args, **kwargs)

        return guarded


def _accepts_keyword(method: object, keyword: str) -> bool:
    if not callable(method):
        return False
    try:
        parameters = inspect.signature(method).parameters.values()
    except (TypeError, ValueError):
        return False
    return any(
        parameter.kind is inspect.Parameter.VAR_KEYWORD or parameter.name == keyword
        for parameter in parameters
    )


def _call_with_pause(
    method: Callable[..., object],
    *args: object,
    pause_requested: Callable[[], bool] | None,
    **kwargs: object,
) -> object:
    """Do not retry an unguarded external call after a signature mismatch."""
    if pause_requested is None:
        return method(*args, **kwargs)
    if not _accepts_keyword(method, "pause_requested"):
        raise ProviderMaterializerError("补源边界不支持暂停检查")
    return method(*args, pause_requested=pause_requested, **kwargs)


def _task_staging_coordinates(staging_root: object) -> tuple[str, str]:
    """Return the RootJob and attempt names for the one allowed staging tree."""
    if not isinstance(staging_root, str):
        raise ProviderMaterializerError("补源 staging_root 无效")
    parent = posixpath.dirname(posixpath.dirname(staging_root))
    root_job_id = posixpath.basename(posixpath.dirname(staging_root))
    attempt_id = posixpath.basename(staging_root)
    if (
        staging_root != f"{CANONICAL_REPLENISHMENT_STAGING_ROOT}/{root_job_id}/{attempt_id}"
        or _TASK_STAGING_SEGMENT.fullmatch(root_job_id) is None
        or _TASK_STAGING_SEGMENT.fullmatch(attempt_id) is None
    ):
        raise ProviderMaterializerError(
            "补源只能写入 /quark/影视/ScrapeFlow/补源/<root>/<attempt>"
        )
    try:
        validate_provider_staging_root(parent)
    except ProviderStagingPathError as exc:
        raise ProviderMaterializerError(
            "补源 staging 不属于正式任务 staging 根"
        ) from exc
    return root_job_id, attempt_id


def _delivery_contract(
    delivery: Mapping[str, object],
    *,
    lane: str,
    staging_root: str,
) -> dict[str, object]:
    """Keep a materializer result bound to exactly one task staging attempt."""
    root_job_id, attempt_id = _task_staging_coordinates(staging_root)
    if delivery.get("lane", lane) != lane:
        raise ProviderMaterializerError("materializer 返回了错误 lane")
    if delivery.get("staging_root", staging_root) != staging_root:
        raise ProviderMaterializerError("materializer 返回了错误 staging_root")
    if delivery.get("attempt_id", attempt_id) != attempt_id:
        raise ProviderMaterializerError("materializer 返回了错误 attempt_id")
    raw_files = delivery.get("files")
    if not isinstance(raw_files, list):
        raise ProviderMaterializerError("materializer 返回的 files 无效")
    files: list[dict[str, object]] = []
    for row in raw_files:
        if not isinstance(row, Mapping):
            raise ProviderMaterializerError("materializer 返回的 files 项无效")
        files.append({
            "path": row.get("path"),
            "size": row.get("size"),
            "kind": row.get("kind"),
            "gap_ids": row.get("gap_ids"),
        })
    result: dict[str, object] = {
        "lane": lane,
        "attempt_id": attempt_id,
        "staging_root": staging_root,
        "files": files,
    }
    if delivery.get("external_task_id") is not None:
        result["external_task_id"] = delivery.get("external_task_id")
    try:
        return validate_provider_delivery(
            result,
            root_job_id=root_job_id,
            attempt_id=attempt_id,
        )
    except ProviderDeliveryError as exc:
        raise ProviderMaterializerError("materializer 返回的 staging 交付无效") from exc


def _remote_file_size(alist: object, path: str) -> int | None:
    exact = getattr(alist, "exact_file_info", None)
    if callable(exact):
        try:
            row = exact(path)
        except ProviderMaterializerPaused:
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
    except ProviderMaterializerPaused:
        raise
    except Exception:
        return None
    matches = [
        row for row in rows
        if isinstance(row, Mapping)
        and row.get("name") == name
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
    """Keep mixed cloud delivery video members in the reserved staging subroot."""
    output = [dict(row) for row in files]
    videos = [row for row in output if row.get("kind") == "video"]
    subtitles = [row for row in output if row.get("kind") == "subtitle"]
    if not videos or not subtitles:
        return output
    mkdir = getattr(alist, "mkdir", None)
    move = getattr(alist, "move", None)
    if not callable(mkdir) or not callable(move):
        raise ProviderMaterializerError("AList 客户端缺少混合交付视频隔离能力")
    media_root = f"{staging_root}/__scrapeflow_media__"
    if any(
        isinstance(row.get("path"), str)
        and str(row["path"]).startswith(media_root + "/")
        for row in subtitles
    ):
        raise ProviderMaterializerError("云端混合交付占用了保留的媒体隔离根")
    names = [posixpath.basename(str(row.get("path") or "")) for row in videos]
    if any(not name for name in names) or len(names) != len(set(names)):
        raise ProviderMaterializerError("云端混合交付视频文件名冲突")
    mkdir(media_root)
    for row, name in zip(videos, names, strict=True):
        source, size = row.get("path"), row.get("size")
        if not isinstance(source, str) or type(size) is not int or size <= 0:
            raise ProviderMaterializerError("云端混合交付视频映射无效")
        destination = f"{media_root}/{name}"
        if source == destination:
            continue
        source_size = _remote_file_size(alist, source)
        destination_size = _remote_file_size(alist, destination)
        if destination_size == size and source_size is None:
            row["path"] = destination
            continue
        if source_size != size or destination_size is not None:
            raise ProviderMaterializerError("云端混合交付视频隔离前回读不一致")
        move(posixpath.dirname(source), media_root, [name])
        if _remote_file_size(alist, destination) != size:
            raise ProviderMaterializerError("云端混合交付视频隔离后回读不一致")
        row["path"] = destination
    return output


class LocalTorrentMaterializer:
    """Download only the preselected Torrent members into task staging."""

    def __init__(
        self,
        delegate: object | None = None,
        *,
        archive_preprocessor: object | None = None,
    ) -> None:
        if delegate is None:
            from engine.tools.replenishment_adapter.materialize import LocalTorrentMaterializer as Delegate

            delegate = Delegate()
        self.delegate = delegate
        self.archive_preprocessor = archive_preprocessor

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
        _pause_checkpoint(pause_requested)
        _task_staging_coordinates(staging_root)
        for selection in selections:
            acquisition = selection.get("acquisition")
            if (
                str(selection.get("provider") or "").strip().casefold() != TIER_LOCAL_MAGNET
                or not isinstance(acquisition, Mapping)
                or str(acquisition.get("kind") or "").strip().casefold() != "torrent"
            ):
                raise ProviderMaterializerError("本地 Torrent 只接受 magnet/torrent 候选")
        method = getattr(self.delegate, "acquire", None)
        if not callable(method):
            raise ProviderMaterializerError("Torrent materializer 不支持 acquire")
        wrapper = {
            "request": dict(request),
            "selection": {"selections": [dict(row) for row in selections]},
            "automatic_staging_root": staging_root,
            "automatic_staging_parent": posixpath.dirname(posixpath.dirname(staging_root)),
        }
        guarded_alist = _PauseCheckedPort(alist, pause_requested)
        result = _call_with_pause(
            method,
            wrapper,
            workspace,
            automatic=True,
            client=guarded_alist,
            pause_requested=pause_requested,
        )
        if not isinstance(result, Mapping):
            raise ProviderMaterializerError("Torrent materializer 返回无效")
        delivery: Mapping[str, object] = result
        preprocess = getattr(self.archive_preprocessor, "prepare_provider_delivery", None)
        if callable(preprocess):
            _pause_checkpoint(pause_requested)
            if pause_requested is None and not _accepts_keyword(preprocess, "request"):
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
                raise ProviderMaterializerError("归档预处理返回无效 delivery")
            delivery = prepared
        return _delivery_contract(delivery, lane=TIER_LOCAL_MAGNET, staging_root=staging_root)

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
    ) -> Mapping[str, object]:
        del request, selections, staging_root, workspace, alist, external_task_id, pause_requested
        raise ProviderMaterializerError("本地 Torrent 没有可查询的外部任务；保持当前 tier 等待处理")


class QuarkFastSaveMaterializer:
    """Submit or resume one reviewed Quark share save for one task attempt."""

    _STATE_FILE = "quark_share_attempt.json"
    _STATE_FIELDS = frozenset({
        "provider", "attempt_id", "staging_root", "task_id", "selected_gap_ids", "updated_at",
    })

    def __init__(self, helper: object | None = None) -> None:
        self.helper = helper

    def _helper(self) -> object:
        if self.helper is None:
            from engine.scrapeflow.quark_helper_client import HttpQuarkHelperClient

            self.helper = HttpQuarkHelperClient.from_env()
        return self.helper

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
            raise ProviderMaterializerError("夸克分享文件路径不安全")
        return value

    @staticmethod
    def _safe_name(value: object) -> str:
        if (
            not isinstance(value, str)
            or not value
            or value in {".", ".."}
            or "/" in value
            or "\\" in value
            or "\x00" in value
        ):
            raise ProviderMaterializerError("夸克分享文件名不安全")
        return value

    @staticmethod
    def _safe_task_id(value: object) -> str | None:
        return value if isinstance(value, str) and _SAFE_EXTERNAL_TASK_ID.fullmatch(value) else None

    @staticmethod
    def _attempt_id(staging_root: str) -> str:
        _root_job_id, attempt_id = _task_staging_coordinates(staging_root)
        return attempt_id

    @staticmethod
    def _selection_gap_ids(selection: Mapping[str, object]) -> list[str]:
        raw = selection.get("selected_gap_ids")
        if not isinstance(raw, list) or not raw:
            raise ProviderMaterializerError("夸克分享候选缺少 selected_gap_ids")
        values = [value for value in raw if isinstance(value, str) and value and len(value) <= 256]
        if len(values) != len(raw) or len(values) != len(set(values)):
            raise ProviderMaterializerError("夸克分享候选 selected_gap_ids 无效")
        return values

    @classmethod
    def _share_save_plan(
        cls,
        selection: Mapping[str, object],
        *,
        destination: str,
        task_id: str | None,
    ) -> dict[str, object]:
        """Build the typed helper request from exact reviewed share members only."""
        from engine.scrapeflow.quark_fast_save_bridge import (
            QuarkBridgeError,
            normalize_quark_fast_save_selection,
        )

        try:
            normalized = normalize_quark_fast_save_selection(selection)
        except QuarkBridgeError as exc:
            raise ProviderMaterializerError("夸克分享候选 manifest 无效") from exc
        acquisition = normalized.get("acquisition")
        selected = normalized.get("selected_gap_ids")
        if not isinstance(acquisition, Mapping) or not isinstance(selected, list) or not selected:
            raise ProviderMaterializerError("夸克分享候选缺少精确成员清单")
        share_id = acquisition.get("pwd_id") or acquisition.get("share_id")
        passcode = acquisition.get("passcode") or ""
        path_map = acquisition.get("file_path_by_id")
        expected_raw = acquisition.get("expected_files")
        if (
            not isinstance(share_id, str)
            or not share_id
            or len(share_id) > 512
            or any(ord(char) < 32 or char in {"/", "\\"} for char in share_id)
            or not isinstance(passcode, str)
            or len(passcode) > 128
            or any(ord(char) < 32 for char in passcode)
            or not isinstance(path_map, Mapping)
            or not isinstance(expected_raw, list)
            or not expected_raw
        ):
            raise ProviderMaterializerError("夸克分享候选 manifest 无效")
        expected: list[dict[str, object]] = []
        for row in expected_raw:
            if not isinstance(row, Mapping):
                raise ProviderMaterializerError("夸克分享 expected_files 项无效")
            file_id = row.get("file_id")
            path = path_map.get(file_id) if isinstance(file_id, str) else None
            name, size, gap_ids = row.get("name"), row.get("size"), row.get("gap_ids")
            if (
                not isinstance(file_id, str)
                or not file_id
                or len(file_id) > 512
                or any(ord(char) < 32 or char in {"/", "\\"} for char in file_id)
                or not isinstance(name, str)
                or not name
                or not isinstance(path, str)
                or cls._safe_share_path(path) != path
                or name != posixpath.basename(path)
                or type(size) is not int
                or size <= 0
                or not isinstance(gap_ids, list)
                or not gap_ids
                or any(not isinstance(gap, str) or not gap for gap in gap_ids)
                or len(gap_ids) != len(set(gap_ids))
            ):
                raise ProviderMaterializerError("夸克分享 expected_files manifest 无效")
            expected.append({
                "file_id": file_id,
                "path": path,
                "name": name,
                "size": size,
                "gap_ids": list(gap_ids),
            })
        covered = {gap for row in expected for gap in row["gap_ids"] if isinstance(gap, str)}
        if covered != set(selected):
            raise ProviderMaterializerError("夸克分享 expected_files 未精确覆盖 selected_gap_ids")
        plan: dict[str, object] = {
            "attempt_id": cls._attempt_id(destination),
            "destination": destination,
            "share_id": share_id,
            "passcode": passcode,
            "selected_gap_ids": list(selected),
            "expected_files": expected,
        }
        title = selection.get("release_name")
        if isinstance(title, str) and title and len(title) <= 512:
            plan["title"] = title
        if task_id is not None:
            plan["task_id"] = task_id
        return plan

    @staticmethod
    def _delivery_kind(name: str) -> str:
        suffix = Path(name).suffix.casefold()
        if suffix in _VIDEO_EXTENSIONS:
            return "video"
        if suffix in _SUBTITLE_EXTENSIONS:
            return "subtitle"
        raise ProviderMaterializerError("夸克分享快转返回了不支持的文件类型")

    @classmethod
    def _state_path(cls, workspace: Path) -> Path:
        return workspace / cls._STATE_FILE

    @staticmethod
    def _valid_updated_at(value: object) -> bool:
        if not isinstance(value, str) or not value.endswith("Z"):
            return False
        try:
            return datetime.fromisoformat(value[:-1] + "+00:00").tzinfo is not None
        except ValueError:
            return False

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
            raise ProviderMaterializerError("夸克分享 attempt 状态不得为软链接")
        if not path.exists():
            return {}
        try:
            stat = path.stat()
            if not path.is_file() or stat.st_size <= 0 or stat.st_size > 16_384:
                raise ProviderMaterializerError("夸克分享 attempt 状态大小无效")
            raw = json.loads(path.read_text(encoding="utf-8"))
        except ProviderMaterializerError:
            raise
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ProviderMaterializerError("夸克分享 attempt 状态不可读") from exc
        if not isinstance(raw, Mapping) or set(raw) != cls._STATE_FIELDS:
            raise ProviderMaterializerError("夸克分享 attempt 状态结构无效")
        state = dict(raw)
        task_id = cls._safe_task_id(state.get("task_id"))
        if (
            state.get("provider") != TIER_QUARK_SHARE
            or state.get("attempt_id") != cls._attempt_id(staging_root)
            or state.get("staging_root") != staging_root
            or state.get("selected_gap_ids") != cls._selection_gap_ids(selection)
            or task_id is None
            or not cls._valid_updated_at(state.get("updated_at"))
        ):
            raise ProviderMaterializerError("夸克分享 attempt 状态不属于当前候选与 staging")
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
            raise ProviderMaterializerError("夸克分享 task_id 无效")
        workspace.mkdir(mode=0o700, parents=True, exist_ok=True)
        atomic_write_json(cls._state_path(workspace), {
            "provider": TIER_QUARK_SHARE,
            "attempt_id": cls._attempt_id(staging_root),
            "staging_root": staging_root,
            "task_id": safe_task_id,
            "selected_gap_ids": cls._selection_gap_ids(selection),
            "updated_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        }, allow_nan=False)

    @staticmethod
    def _in_doubt(message: str, *, task_id: str | None = None) -> Exception:
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
        del request
        _pause_checkpoint(pause_requested)
        _task_staging_coordinates(staging_root)
        if len(selections) != 1:
            raise ProviderMaterializerError("夸克分享快转一次只接受一个候选")
        selection = selections[0]
        acquisition = selection.get("acquisition")
        if (
            str(selection.get("provider") or "").strip().casefold() != TIER_QUARK_SHARE
            or not isinstance(acquisition, Mapping)
            or str(acquisition.get("kind") or "").strip().casefold() != "quark_fast_save"
        ):
            raise ProviderMaterializerError("夸克分享只接受 quark_share/quark_fast_save 候选")
        state = self._read_attempt_state(workspace, staging_root=staging_root, selection=selection)
        persisted_task_id = self._safe_task_id(state.get("task_id"))
        guarded_alist = _PauseCheckedPort(alist, pause_requested)
        mkdir = getattr(guarded_alist, "mkdir", None)
        if not callable(mkdir):
            raise ProviderMaterializerError("AList 客户端缺少 mkdir，无法创建夸克 staging")
        mkdir(posixpath.dirname(staging_root))
        mkdir(staging_root)
        _pause_checkpoint(pause_requested)
        share_save = getattr(self._helper(), "share_save", None)
        if not callable(share_save):
            raise ProviderMaterializerError("夸克 Helper 缺少 typed share-save")
        plan = self._share_save_plan(selection, destination=staging_root, task_id=persisted_task_id)
        _pause_checkpoint(pause_requested)
        result = share_save(plan)
        if not isinstance(result, Mapping):
            raise ProviderMaterializerError("夸克分享快转返回无效")
        status = str(result.get("status") or "").strip().casefold()
        if status in {"candidate_failed", "rejected", "invalid", "expired"}:
            from engine.scrapeflow.quark_fast_save_bridge import QuarkShareExpiredError

            raise QuarkShareExpiredError("Quark Helper rejected the reviewed share candidate")
        task_id = self._safe_task_id(result.get("task_id"))
        if task_id is None:
            raise self._in_doubt("夸克分享结果缺少 task_id，必须先核对再重试", task_id=persisted_task_id)
        if persisted_task_id is not None and task_id != persisted_task_id:
            raise self._in_doubt("夸克分享结果 task_id 与 attempt 状态不一致", task_id=persisted_task_id)
        try:
            self._write_attempt_state(workspace, staging_root=staging_root, selection=selection, task_id=task_id)
        except Exception as exc:
            raise self._in_doubt("夸克分享任务已提交但 attempt 状态未保存", task_id=task_id) from exc
        if status not in {"submitted", "finished", "success", "done", "ready", "completed"}:
            raise self._in_doubt("Quark Helper share-save returned a non-terminal task state", task_id=task_id)
        rows = plan["expected_files"]
        assert isinstance(rows, list)
        files: list[dict[str, object]] = []
        for row in rows:
            if not isinstance(row, Mapping):
                raise ProviderMaterializerError("夸克分享 expected_files 项无效")
            name = self._safe_name(row.get("name"))
            size, gap_ids = row.get("size"), row.get("gap_ids")
            if type(size) is not int or size <= 0 or not isinstance(gap_ids, list) or not gap_ids:
                raise ProviderMaterializerError("夸克分享文件映射无效")
            exact_gaps = [str(gap) for gap in gap_ids if isinstance(gap, str) and gap]
            if len(exact_gaps) != len(gap_ids):
                raise ProviderMaterializerError("夸克分享文件 gap_ids 无效")
            files.append({
                "path": f"{staging_root}/{name}",
                "size": size,
                "kind": self._delivery_kind(name),
                "gap_ids": exact_gaps,
            })
        return _delivery_contract({
            "lane": TIER_QUARK_SHARE,
            "attempt_id": self._attempt_id(staging_root),
            "staging_root": staging_root,
            "files": _isolate_cloud_delivery_videos(files, staging_root=staging_root, alist=guarded_alist),
            "external_task_id": task_id,
        }, lane=TIER_QUARK_SHARE, staging_root=staging_root)

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
    ) -> Mapping[str, object]:
        _task_staging_coordinates(staging_root)
        task_id = self._safe_task_id(external_task_id)
        if task_id is None or len(selections) != 1:
            raise ProviderMaterializerError("夸克分享已有任务恢复候选无效")
        selection = selections[0]
        state_path = self._state_path(workspace)
        if state_path.exists():
            state = self._read_attempt_state(workspace, staging_root=staging_root, selection=selection)
            if state.get("task_id") != task_id:
                raise ProviderMaterializerError("夸克分享已有 task_id 与本地 attempt 不一致")
        else:
            self._write_attempt_state(workspace, staging_root=staging_root, selection=selection, task_id=task_id)
        return self.acquire(
            request,
            selections,
            staging_root=staging_root,
            workspace=workspace,
            alist=alist,
            pause_requested=pause_requested,
        )


__all__ = [
    "LocalTorrentMaterializer",
    "ProviderMaterializerError",
    "ProviderMaterializerPaused",
    "QuarkFastSaveMaterializer",
]
