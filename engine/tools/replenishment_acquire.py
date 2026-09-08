#!/usr/bin/env python3
"""Replenishment acquisition pipeline (extracted 2026-09-07).

Split from ``_replenishment_local_adapter_impl`` along its natural seam:
the download/upload pipeline — selection verification, manifest proof,
preflight (torrent metadata fetch + identity), the aria2 group download
loop with cross-group adoption, exact-size completion proof, canonical
upload with second-opinion reconciliation, and task staging-root
management.  The impl module remains the facade and re-exports every
exported name, so external ``adapter._symbol`` consumers and the test
suite see no change.  Import direction is strictly downward
(common ← terms ← search_sources ← acquire ← impl facade).
"""

from __future__ import annotations

import os
import posixpath
import re
import shutil
import subprocess
import threading
import time
from pathlib import Path
from typing import Any, Callable, Mapping
import urllib.error
import urllib.request

from engine.scraper import AListClient, join_remote
from engine.scrapeflow.provider_capabilities import candidate_capability_error
from engine.scrapeflow.video_admission import (
    VideoAdmissionError,
    probe_local_video_stream,
)
from engine.scrapeflow.media_quality import (
    minimum_video_bytes,
    video_size_is_admissible,
)
from engine.tools.replenishment_common import (
    MagnetMetadataUnavailable,
    ReplenishmentCandidateError,
    ReplenishmentDeliveryError,
    ReplenishmentInfrastructureError,
    ReplenishmentPauseRequested,
    SUBTITLE_EXTENSIONS,
    VIDEO_EXTENSIONS,
    _base32_infohash,
    _bounded_seconds,
    _direct_download_env,
    _download_torrent,
    _is_ordinary_primary_video_path,
    _is_supplemental_video_path,
    _pause_checkpoint,
)

_TASK_STAGING_PARENT = "/quark/影视/ScrapeFlow/补源"
def _selected_companion_indices(
    selection: Mapping[str, Any], *, selected_gap_ids: set[str],
) -> dict[int, list[str]]:
    """Read narrowly-scoped new-media subtitle companion bindings.

    Companion indices are additional download members, not gap fulfilments:
    they must not be folded into ``by_index`` or a subtitle could masquerade
    as a missing-episode payload.  This parser is repeated during manifest
    verification so a hand-edited persisted selection cannot turn an
    arbitrary subtitle into a child sidecar.
    """
    acquisition = selection.get("acquisition")
    if not isinstance(acquisition, Mapping):
        raise ValueError("选中候选缺少 torrent 获取说明")
    raw_map = acquisition.get("companion_subtitle_index_by_media_gap")
    if raw_map is None:
        return {}
    if not isinstance(raw_map, Mapping):
        raise ValueError("伴随字幕索引映射无效")
    gap_map = acquisition.get("file_index_by_gap")
    if not isinstance(gap_map, Mapping):
        raise ValueError("选中候选缺少集号到 torrent 文件索引映射")
    path_map = acquisition.get("file_path_by_index")
    if not isinstance(path_map, Mapping):
        raise ValueError("伴随字幕缺少 torrent 文件路径映射")
    primary_indices = {
        index for values in gap_map.values() if isinstance(values, list)
        for index in values if type(index) is int and index > 0
    }
    result: dict[int, list[str]] = {}
    used_indices: set[int] = set()
    for raw_gap_id, values in raw_map.items():
        gap_id = str(raw_gap_id)
        primary = gap_map.get(gap_id)
        if (
            not gap_id
            or gap_id.startswith("missing_subtitle:")
            or not isinstance(primary, list)
            or len(primary) != 1
            or type(primary[0]) is not int
            or primary[0] <= 0
            or not isinstance(values, list)
            or len(values) != 1
            or type(values[0]) is not int
            or values[0] <= 0
        ):
            raise ValueError("伴随字幕没有唯一媒体 gap/manifest 绑定")
        index = values[0]
        path = path_map.get(str(index), path_map.get(index))
        if (
            index in primary_indices
            or index in used_indices
            or not isinstance(path, str)
            or Path(path).suffix.casefold() not in SUBTITLE_EXTENSIONS
            or _is_supplemental_video_path(path)
        ):
            raise ValueError("伴随字幕 manifest 成员无效或重复")
        # A candidate can advertise a companion for a gap which the selector
        # did not choose this round.  Do not download it, but validate its
        # shape above so a persisted unknown pseudo-gap cannot hide here.
        if gap_id in selected_gap_ids:
            result[index] = [gap_id]
            used_indices.add(index)
    return result
def _selected_indices(selection: Mapping[str, Any]) -> tuple[set[int], dict[int, list[str]]]:
    acquisition = selection.get("acquisition")
    if not isinstance(acquisition, Mapping) or acquisition.get("kind") != "torrent":
        raise ValueError("选中候选缺少 torrent 获取说明")
    gap_map = acquisition.get("file_index_by_gap")
    if not isinstance(gap_map, Mapping):
        raise ValueError("选中候选缺少集号到 torrent 文件索引映射")
    # Companion sidecars belonged to the retired legacy media flow.  The
    # current RootJob subtitle channel obtains and proves one merged bilingual
    # file independently, so a direct Torrent invocation must fail closed if
    # a serialized old companion map slips past its caller.  Current callers
    # strip it before reaching this lower boundary; rejecting here protects
    # manual/recovery callers too, before aria2 sees an extra index.
    if acquisition.get("companion_subtitle_index_by_media_gap") is not None:
        raise ValueError("媒体磁力补源不接受伴随字幕成员")
    by_index: dict[int, list[str]] = {}
    for gap_id in selection.get("selected_gap_ids") or []:
        values = gap_map.get(gap_id)
        if not isinstance(values, list) or not values:
            raise ValueError(f"缺口没有 torrent 文件索引: {gap_id}")
        for value in values:
            if type(value) is not int or value <= 0:
                raise ValueError(f"torrent 文件索引无效: {gap_id}")
            by_index.setdefault(value, []).append(str(gap_id))
    if not by_index:
        raise ValueError("选中候选没有需要获取的文件")
    path_map = acquisition.get("file_path_by_index")
    if not isinstance(path_map, Mapping):
        raise ValueError("选中候选缺少 torrent 文件路径映射")

    def is_subtitle_gap_id(gap_id: str) -> bool:
        """Recognize the durable subtitle coordinates accepted by this adapter.

        The compact selector keeps only ids at this boundary.  Do not infer a
        media kind from an extension after aria2 has already started: a
        non-subtitle coordinate must prove an ordinary video member *before*
        it is serialized into ``--select-file``.  Both legacy and RootJob
        ledger ids are accepted for the standalone subtitle lane.
        """
        return (
            gap_id.startswith("missing_subtitle:")
            or "::missing_subtitle::" in gap_id
        )

    # Every selected primary member is checked here, before preflight or
    # aria2.  The prior episode-only guard left movie/season/manual rows able
    # to map a subtitle (or another non-media member) as a primary payload.
    # A mixed media/subtitle binding is equally unsafe: it would let a video
    # delivery masquerade as a sidecar or vice versa.
    for index, gap_ids in by_index.items():
        path = path_map.get(str(index), path_map.get(index))
        if not isinstance(path, str):
            raise ValueError(f"torrent 文件缺少路径映射: {index}")
        subtitle_gaps = [gap_id for gap_id in gap_ids if is_subtitle_gap_id(gap_id)]
        media_gaps = [gap_id for gap_id in gap_ids if not is_subtitle_gap_id(gap_id)]
        if subtitle_gaps and media_gaps:
            raise ValueError("torrent 文件不能同时绑定媒体与字幕缺口")
        if media_gaps:
            if (
                not _is_ordinary_primary_video_path(path)
                or len(media_gaps) != 1
            ):
                raise ValueError(
                    f"媒体缺口的 torrent 文件不唯一或非正片: {media_gaps[0]}"
                )
        elif Path(path).suffix.casefold() not in SUBTITLE_EXTENSIONS:
            raise ValueError(f"字幕缺口的 torrent 文件不是字幕: {subtitle_gaps[0]}")
    # Exact episode gaps are never batch members.  A selected candidate must
    # serialize one ordinary video for each one, and no video may be replayed
    # against several gaps after a restart/manual edit.
    episode_gap_ids = {
        gap_id for gap_id in (str(value) for value in selection.get("selected_gap_ids") or [])
        if re.fullmatch(r"S\d{2,3}E\d{2,4}", gap_id)
    }
    for gap_id in episode_gap_ids:
        values = gap_map.get(gap_id)
        if not isinstance(values, list) or len(values) != 1 or type(values[0]) is not int:
            raise ValueError(f"episode gap 没有唯一 torrent 视频: {gap_id}")
        index = values[0]
        path = path_map.get(str(index), path_map.get(index))
        if (
            not isinstance(path, str)
            or not _is_ordinary_primary_video_path(path)
            or len(by_index.get(index, [])) != 1
        ):
            raise ValueError(f"episode gap 的 torrent 视频不唯一或非正片: {gap_id}")
    return set(by_index), by_index
def _verify_manifest(selection: Mapping[str, Any], manifest: Mapping[str, Any]) -> tuple[set[int], dict[int, list[str]]]:
    indices, by_index = _selected_indices(selection)
    acquisition = selection["acquisition"]
    expected_hash = str(selection.get("infohash") or "").casefold()
    actual_hash = str(manifest.get("infohash") or "").casefold()
    if expected_hash and expected_hash not in {actual_hash, _base32_infohash(actual_hash)}:
        raise ValueError("torrent infohash 与候选目录不一致")
    files = manifest.get("files") if isinstance(manifest.get("files"), Mapping) else {}
    size_map = acquisition.get("file_size_by_index") if isinstance(acquisition.get("file_size_by_index"), Mapping) else {}
    path_map = acquisition.get("file_path_by_index") if isinstance(acquisition.get("file_path_by_index"), Mapping) else {}
    for index in indices:
        row = files.get(index)
        if not isinstance(row, Mapping):
            raise ValueError(f"torrent 不含目录声明的文件索引: {index}")
        expected_size = size_map.get(str(index), size_map.get(index))
        if type(expected_size) is not int or expected_size <= 0 or row.get("size") != expected_size:
            raise ValueError(f"torrent 文件大小与目录不一致: {index}")
        expected_path = path_map.get(str(index), path_map.get(index))
        if not isinstance(expected_path, str) or expected_path != row.get("path"):
            raise ValueError(f"torrent 文件路径与目录不一致: {index}")
    return indices, by_index
_LOCAL_DOWNLOAD_FAILURE_SIGNATURES = (
    # aria2 surfaces local errno text on host-side failures; a zero-byte
    # download whose tail matches one of these never proved the resource
    # bad — the host could not even open the file it was writing.
    "no space left",
    "cannot open",
    "permission denied",
    "read-only",
    "input/output error",
    "disk quota",
)
def _looks_like_local_download_failure(tail: str) -> bool:
    """Whether an aria2 failure tail carries host-side errno evidence."""
    folded = str(tail).casefold()
    return any(
        signature in folded
        for signature in _LOCAL_DOWNLOAD_FAILURE_SIGNATURES
    )
def _payload_bytes(payload_dir: Path) -> int:
    """Sum retained media bytes, ignoring aria2 control files."""
    if not payload_dir.is_dir():
        return 0
    total = 0
    for path in payload_dir.rglob("*"):
        if path.is_file() and path.suffix != ".aria2":
            try:
                total += path.stat().st_size
            except OSError:
                continue
    return total
def _payload_is_complete(
    payload_dir: Path, acquisition: Mapping[str, Any], indices: set[int],
) -> bool:
    """Trust a retained payload only after every selected file exactly matches.

    A retained ``.aria2`` control file is *not* evidence of an incomplete
    member: aria2 deliberately keeps it for a partially-selected torrent (the
    selection covers some files, not the whole swarm), so per-member exact
    sizes remain the only completion proof.
    """
    if not payload_dir.is_dir():
        return False
    size_map = acquisition.get("file_size_by_index")
    path_map = acquisition.get("file_path_by_index")
    if not isinstance(size_map, Mapping) or not isinstance(path_map, Mapping):
        return False
    try:
        for index in indices:
            _find_download(
                payload_dir,
                str(path_map[str(index)]),
                int(size_map[str(index)]),
            )
    except (KeyError, TypeError, ValueError):
        return False
    return True
def _preflight(
    selection_wrapper: Mapping[str, Any], workspace: Path,
    *, resume_workspace: Path | None = None,
    pause_requested: Callable[[], bool] | None = None,
) -> dict[str, Any]:
    bundle = selection_wrapper.get("selection")
    selections = bundle.get("selections") if isinstance(bundle, Mapping) else None
    if not isinstance(selections, list) or not selections:
        raise ReplenishmentInfrastructureError(
            "选择文件缺少 selections", stage="artifact_validation",
        )
    acquisition_kinds = {
        str(row.get("acquisition", {}).get("kind") or "")
        for row in selections if isinstance(row, Mapping)
        and isinstance(row.get("acquisition"), Mapping)
    }
    if acquisition_kinds != {"torrent"}:
        raise ReplenishmentInfrastructureError(
            "选择包含非 Torrent acquisition；本地适配器失败关闭",
            stage="unsupported_provider_lane",
        )
    if shutil.which("aria2c") is None:
        raise ReplenishmentInfrastructureError(
            "运行环境缺少 aria2c", stage="local_dependency",
        )
    _pause_checkpoint(pause_requested)
    workspace.mkdir(parents=True, exist_ok=True)
    selected_bytes = 0
    selected_files = 0
    reusable_bytes = 0
    verified: list[dict[str, Any]] = []
    for offset, selection in enumerate(selections, start=1):
        _pause_checkpoint(pause_requested)
        if not isinstance(selection, Mapping):
            raise ValueError("selection 项格式无效")
        acquisition = selection.get("acquisition")
        url = acquisition.get("url") if isinstance(acquisition, Mapping) else None
        if not isinstance(url, str):
            raise ValueError("选中候选缺少 torrent URL")
        torrent_path = workspace / f"candidate-{offset:02d}.torrent"
        try:
            if pause_requested is None:
                manifest = _download_torrent(url, torrent_path)
            else:
                manifest = _download_torrent(
                    url,
                    torrent_path,
                    pause_requested=pause_requested,
                )
            indices, _by_index = _verify_manifest(selection, manifest)
        except ReplenishmentPauseRequested:
            raise
        except MagnetMetadataUnavailable as exc:
            # The DHT window did not reach the swarm; the resource itself is
            # unproven either way, so retry later without excluding it.
            raise ReplenishmentInfrastructureError(
                f"magnet 元数据本窗口不可得，等待重试: {exc}",
                stage="candidate_preflight",
            ) from exc
        except (OSError, ValueError, RuntimeError, urllib.error.URLError) as exc:
            raise ReplenishmentCandidateError(
                str(exc), stage="candidate_preflight", candidate=selection,
            ) from exc
        files = manifest["files"]
        byte_count = sum(int(files[index]["size"]) for index in indices)
        selected_bytes += byte_count
        selected_files += len(indices)
        member_pending: list[int] = []
        for index in sorted(indices):
            member_size = int(files[index]["size"])
            # Batch groups are named by group ordinal, not member index;
            # a resumed attempt rebuilds the same deterministic grouping, so
            # a retained group payload is reusable for its whole batch.
            member_pending.append(member_size)
        if resume_workspace is not None:
            candidate_dir = resume_workspace / f"download-{offset:02d}"
            for group_dir in sorted(candidate_dir.glob("group-*")):
                payload_dir = group_dir / "payload"
                for index in indices:
                    if _payload_is_complete(payload_dir, acquisition, {index}):
                        reusable_bytes += int(files[index]["size"])
                        try:
                            member_pending.remove(int(files[index]["size"]))
                        except ValueError:
                            pass
        verified.append({
            "release_name": selection.get("release_name"),
            "torrent": url,
            "infohash": manifest["infohash"],
            "selected_indices": sorted(indices),
            "selected_files": len(indices),
            "selected_bytes": byte_count,
            "member_pending_bytes": sorted(member_pending, reverse=True),
            "torrent_path": str(torrent_path),
            "manifest": manifest,
        })
    free = shutil.disk_usage(workspace).free
    remaining_bytes = selected_bytes - reusable_bytes
    # The batched pipeline holds at most one group locally at a time
    # (download → per-member upload/verify → delete), so the capacity floor
    # is the batch size times the largest single member, not the whole pack.
    batch_size = _bounded_seconds(
        "SCRAPEFLOW_REPLENISHMENT_MEMBER_BATCH", 4, 1, 64,
    )
    pending_member_sizes: list[int] = []
    for row in verified:
        pending_member_sizes.extend(row.get("member_pending_bytes") or [])
    pending_member_sizes.sort(reverse=True)
    group_peak = sum(pending_member_sizes[:batch_size])
    required = int(group_peak * 1.15) + 1024 ** 3
    if free < required:
        raise ReplenishmentInfrastructureError(
            f"补源暂存空间不足: required={required}, free={free}",
            stage="local_capacity",
        )
    return {
        "status": "verified",
        "selected_files": selected_files,
        "selected_bytes": selected_bytes,
        "reusable_bytes": reusable_bytes,
        "remaining_bytes": remaining_bytes,
        "free_bytes": free,
        "required_bytes": required,
        "candidates": verified,
    }
def _alist_client() -> AListClient:
    base_url = os.getenv("ALIST_URL", "http://127.0.0.1:5244")
    username = os.getenv("ALIST_USERNAME", "")
    password = os.getenv("ALIST_PASSWORD", "")
    if not username or not password:
        raise ValueError("AList 上传凭据未配置")
    client = AListClient(
        base_url, username, password, timeout=60, retries=4,
        allow_insecure_http=base_url.startswith("http://alist:") or base_url.startswith("http://127.0.0.1"),
    )
    client.login()
    return client
def _selection_acquisition_kind(selection: Mapping[str, Any]) -> str:
    acquisition = selection.get("acquisition")
    return str(acquisition.get("kind") or "") if isinstance(acquisition, Mapping) else ""
def _wrapper_for_selections(
    wrapper: Mapping[str, Any], selections: list[dict[str, Any]],
) -> dict[str, Any]:
    output = dict(wrapper)
    bundle = dict(wrapper.get("selection") or {})
    bundle["selections"] = selections
    output["selection"] = bundle
    return output
def _preflight_dispatch(
    wrapper: Mapping[str, Any], workspace: Path, *, resume_workspace: Path | None = None,
    pause_requested: Callable[[], bool] | None = None,
) -> dict[str, Any]:
    bundle = wrapper.get("selection")
    rows = bundle.get("selections") if isinstance(bundle, Mapping) else None
    if not isinstance(rows, list) or not rows or not all(isinstance(row, dict) for row in rows):
        raise ReplenishmentInfrastructureError(
            "选择文件缺少 selections", stage="artifact_validation",
        )
    if any(
        _selection_acquisition_kind(row) != "torrent"
        or candidate_capability_error(row) is not None
        for row in rows
    ):
        raise ReplenishmentInfrastructureError(
            "本地适配器只接受可执行的 magnet/torrent selection",
            stage="artifact_validation",
        )
    _pause_checkpoint(pause_requested)
    arguments = (
        _wrapper_for_selections(wrapper, [dict(row) for row in rows]),
        workspace / "torrent",
    )
    keyword_arguments = {
        "resume_workspace": resume_workspace / "torrent" if resume_workspace else None,
    }
    if pause_requested is not None:
        keyword_arguments["pause_requested"] = pause_requested
    return _preflight(*arguments, **keyword_arguments)
def _ffprobe_archive_video(path: Path) -> dict[str, Any]:
    """Verify one retained payload through the shared bounded admission."""
    try:
        return probe_local_video_stream(path)
    except VideoAdmissionError as exc:
        if exc.infrastructure:
            raise ReplenishmentInfrastructureError(
                f"视频准入环境不可用: {exc.reason}",
                stage="local_dependency",
            ) from exc
        raise ReplenishmentCandidateError(
            f"视频流准入失败: {path.name}: {exc.reason}",
            stage="candidate_payload",
        ) from exc
def _verify_video_payload(
    path: Path,
    expected_size: int,
    selection: Mapping[str, Any],
) -> None:
    """Apply local-video admission before any AList staging upload.

    Torrent manifest size checks only prove that aria2 received the bytes the
    torrent advertised.  They do not prove that a tiny fixture, HTML error
    page, or other non-video payload renamed to ``.mkv`` is safe to expose to
    the Engine.  The formal writer repeats the byte guard as a final boundary;
    this earlier local check keeps rejected payloads out of remote staging.
    """
    if not video_size_is_admissible(expected_size):
        raise ReplenishmentCandidateError(
            "候选视频小于正式库准入下限 "
            f"{minimum_video_bytes()} bytes: {path.name}",
            stage="candidate_payload_validation",
            candidate=selection,
        )
    _ffprobe_archive_video(path)
def _acquire_dispatch(
    wrapper: Mapping[str, Any],
    workspace: Path,
    *,
    automatic: bool = True,
    client: AListClient | None = None,
    pause_requested: Callable[[], bool] | None = None,
) -> dict[str, Any]:
    """Dispatch only exact Torrent selections to the local materializer."""
    bundle = wrapper.get("selection")
    rows = bundle.get("selections") if isinstance(bundle, Mapping) else None
    if not isinstance(rows, list) or not rows or not all(isinstance(row, dict) for row in rows):
        raise ReplenishmentInfrastructureError(
            "选择文件缺少 selections", stage="artifact_validation",
        )
    if any(
        _selection_acquisition_kind(row) != "torrent"
        or candidate_capability_error(row) is not None
        for row in rows
    ):
        raise ReplenishmentInfrastructureError(
            "本地适配器拒绝不可执行 provider/acquisition；没有云端 fallback",
            stage="artifact_validation",
        )
    if not automatic:
        raise ReplenishmentInfrastructureError(
            "补源必须由自动调度器创建任务 staging",
            stage="automatic_route_required",
        )
    _pause_checkpoint(pause_requested)
    return _acquire(
        wrapper,
        workspace,
        client=client,
        pause_requested=pause_requested,
    )
def _safe_name(value: str, *, limit: int = 180) -> str:
    value = re.sub(r"[\\/:*?\"<>|\x00-\x1f]+", " ", value)
    value = re.sub(r"\s+", " ", value).strip(" .")
    return (value or "补源文件")[:limit].rstrip(" .")
def _verify_remote_uploads(
    client: AListClient,
    remote_root: str,
    uploaded: list[dict[str, Any]],
    *,
    pause_requested: Callable[[], bool] | None = None,
) -> None:
    """Wait for cloud-backed AList listings to expose committed uploads.

    AList's upload endpoint can return before a provider refresh exposes the
    new row and its final size.  Treating that short visibility window as a
    failed torrent discards a fully downloaded candidate and causes an
    unnecessary retry, so poll the refreshed directory for a bounded period
    before declaring the acquisition failed.
    """
    timeout = _bounded_seconds(
        "SCRAPEFLOW_REPLENISHMENT_ARRIVAL_TIMEOUT", 120, 10, 600,
    )
    deadline = time.monotonic() + timeout
    last_files: dict[str, int] = {}
    while True:
        _pause_checkpoint(pause_requested)
        rows = client.list(remote_root, refresh=True)
        last_files = {
            str(row.get("name")): int(row.get("size") or 0)
            for row in rows if not row.get("is_dir")
        }
        if all(last_files.get(row["remote_name"]) == row["size"] for row in uploaded):
            return
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            missing = [
                row["remote_name"] for row in uploaded
                if last_files.get(row["remote_name"]) != row["size"]
            ]
            raise ValueError(f"AList 到盘核验失败: {', '.join(missing[:3])}")
        time.sleep(min(5.0, remaining))
def _remote_upload_matches(
    client: AListClient, remote_root: str, remote_name: str, size: int,
) -> bool:
    rows = client.try_list(remote_root, refresh=True) or []
    return any(
        not row.get("is_dir")
        and str(row.get("name") or "") == remote_name
        and int(row.get("size") or 0) == size
        for row in rows
    )
def _automatic_upload(
    client: AListClient,
    remote_root: str,
    row: dict[str, Any],
    *,
    pause_requested: Callable[[], bool] | None = None,
) -> None:
    """Upload one file from task-owned staging and verify its exact size."""
    remote_name = str(row["remote_name"])
    source = Path(row["source"]).resolve()
    size = int(row["size"])
    if not source.is_file() or source.stat().st_size != size:
        raise ReplenishmentCandidateError(
            f"自动补源本地文件大小无效: {source}", stage="candidate_payload_validation",
        )
    target = join_remote(remote_root, remote_name)
    _pause_checkpoint(pause_requested)
    existing = client.exact_file_info(target)
    if existing is not None:
        existing_size = int(existing.get("size") or 0)
        if existing_size == size:
            return
        raise ReplenishmentDeliveryError(
            f"自动补源 staging 已有不同大小文件: {target}", stage="delivery_upload",
        )
    content_type = {
        ".srt": "application/x-subrip",
        ".ass": "text/x-ass",
        ".ssa": "text/x-ssa",
        ".vtt": "text/vtt",
    }.get(source.suffix.casefold(), "application/octet-stream")
    _pause_checkpoint(pause_requested)
    client.upload_file(target, source, content_type)
def _find_retained_member(
    candidate_dir: Path, relative_path: str, size: int,
) -> Path | None:
    """Locate one completed member in any retained group of the candidate.

    A resumed attempt regroups the pending members (remote-committed ones
    drop out), so a member completed inside an OLD group-NNN directory is
    invisible to the new group's payload check.  The retained bytes are
    keyed by the manifest's exact relative path and size — never by the
    group ordinal, which drifts on every regroup.
    """
    if not candidate_dir.is_dir():
        return None
    suffix = relative_path.replace("\\", "/")
    try:
        for payload in sorted(candidate_dir.glob("group-*/payload")):
            for path in payload.rglob("*"):
                if (
                    path.is_file()
                    and path.as_posix().endswith(suffix)
                    and path.stat().st_size == size
                ):
                    return path
    except OSError:
        return None
    return None
def _find_download(payload: Path, relative_path: str, size: int) -> Path:
    suffix = relative_path.replace("\\", "/")
    basename = Path(relative_path).name
    # The exact relative path from the manifest is the primary key: two
    # files in different subdirectories of one pack may share a name and a
    # size (season packs do this), and a name+size scan cannot tell them
    # apart.  Only when the exact suffix is absent (a pack flattened by a
    # prior tool) does the basename+size scan stand in.
    exact = [
        path for path in payload.rglob("*")
        if path.is_file() and path.as_posix().endswith(suffix) and path.stat().st_size == size
    ]
    if len(exact) == 1:
        return exact[0]
    if exact:
        raise ValueError(f"下载文件定位结果异常: {relative_path}; matches={len(exact)}")
    candidates = [
        path for path in payload.rglob("*")
        if path.is_file() and path.name == basename and path.stat().st_size == size
    ]
    if len(candidates) != 1:
        raise ValueError(f"下载文件定位结果异常: {relative_path}; matches={len(candidates)}")
    return candidates[0]
def _require_task_staging_root(remote_parent: object, remote_root: object) -> tuple[str, str]:
    """Accept only the single RootJob/attempt staging layout."""
    if remote_parent != _TASK_STAGING_PARENT or not isinstance(remote_root, str):
        raise ReplenishmentInfrastructureError(
            "补源只能使用 /quark/影视/ScrapeFlow/补源/<root>/<attempt>",
            stage="staging_root",
        )
    prefix = _TASK_STAGING_PARENT + "/"
    if not remote_root.startswith(prefix) or posixpath.normpath(remote_root) != remote_root:
        raise ReplenishmentInfrastructureError(
            "自动补源 staging_root 无效", stage="staging_root",
        )
    segments = remote_root[len(prefix):].split("/")
    if len(segments) != 2 or any(
        re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}", part) is None
        for part in segments
    ):
        raise ReplenishmentInfrastructureError(
            "自动补源 staging_root 必须是一个 RootJob 和一个 attempt",
            stage="staging_root",
        )
    return segments[0], segments[1]
def _ensure_automatic_staging_root(
    client: AListClient,
    remote_parent: str,
    remote_root: str,
    *,
    pause_requested: Callable[[], bool] | None = None,
) -> None:
    """Create the known task-owned staging path one level at a time."""
    root_job_id, _attempt_id = _require_task_staging_root(remote_parent, remote_root)
    parent = _TASK_STAGING_PARENT
    root = str(remote_root)
    job_root = posixpath.dirname(root)
    if job_root != f"{parent}/{root_job_id}":
        raise ReplenishmentInfrastructureError(
            "自动补源任务 staging 父目录无效", stage="staging_root",
        )
    for directory in dict.fromkeys(
        path for path in (parent, job_root, root) if path
    ):
        _pause_checkpoint(pause_requested)
        client.mkdir(directory)
def _run_download_command(
    command: list[str], timeout: float, *,
    pause_requested: Callable[[], bool],
    env: dict[str, str],
) -> subprocess.CompletedProcess[str]:
    """Run one aria2c download with pause- and deadline-aware waiting.

    ``subprocess.run`` blocks until aria2c exits on its own — up to the full
    torrent budget — so a pause that lands inside that window could only
    take effect after the whole window (2026-09-08: the wind-down crawl
    held the worker slot for an hour despite a pause).  Poll instead: a
    pause request terminates aria2c immediately; SIGTERM lets it save the
    ``.aria2`` checkpoint so the next attempt resumes byte-exactly, and the
    standard pause checkpoint then aborts the round.  The deadline keeps the
    original ``TimeoutExpired`` contract for its caller.
    """
    process = subprocess.Popen(
        command, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
        env=env,
    )
    output: list[str] = []

    def _drain() -> None:
        if process.stdout is None:
            return
        for line in process.stdout:
            output.append(line)

    reader = threading.Thread(target=_drain, daemon=True)
    reader.start()
    deadline_ts = time.monotonic() + timeout
    while True:
        try:
            process.wait(timeout=0.5)
            break
        except subprocess.TimeoutExpired:
            pass
        if pause_requested():
            process.terminate()
            try:
                process.wait(timeout=30)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
            reader.join(timeout=5)
            # 暂停走标准断点语义:控制文件已由 SIGTERM 保全,下一次尝试
            # 字节精确续传。若此刻 pause 又被撤回,断点部分进度交给下方
            # returncode 分支按"已有部分进度"的窗口语义处理。
            _pause_checkpoint(pause_requested)
            raise ReplenishmentPauseRequested("下载中被暂停,aria2c 断点已保存")
        if time.monotonic() >= deadline_ts:
            process.terminate()
            try:
                process.wait(timeout=30)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
            reader.join(timeout=5)
            raise subprocess.TimeoutExpired(command, timeout)
    reader.join(timeout=5)
    return subprocess.CompletedProcess(
        command, process.returncode, stdout="".join(output),
    )


def _acquire(
    selection_wrapper: Mapping[str, Any],
    workspace: Path,
    *,
    client: AListClient | None = None,
    pause_requested: Callable[[], bool] | None = None,
) -> dict[str, Any]:
    request = selection_wrapper.get("request") if isinstance(selection_wrapper.get("request"), Mapping) else {}
    remote_parent = _TASK_STAGING_PARENT
    automatic_parent = selection_wrapper.get("automatic_staging_parent")
    automatic_staging = selection_wrapper.get("automatic_staging_root")
    if automatic_parent != remote_parent or not isinstance(automatic_staging, str):
        raise ReplenishmentInfrastructureError(
            "自动补源缺少固定任务 staging_root", stage="staging_root",
        )
    remote_root = automatic_staging
    _require_task_staging_root(remote_parent, remote_root)
    uploaded: list[dict[str, Any]] = []
    raw_request_rows = request.get("gaps")
    if not isinstance(raw_request_rows, list) or any(
        not isinstance(gap, Mapping) for gap in raw_request_rows
    ):
        raise ReplenishmentCandidateError(
            "补源请求 gap 证据无效，拒绝下载",
            stage="lane_validation",
        )
    request_rows = [dict(gap) for gap in raw_request_rows]
    if not request_rows:
        raise ReplenishmentCandidateError(
            "补源请求没有可执行 gap",
            stage="lane_validation",
        )
    request_kinds = {str(gap.get("kind") or "") for gap in request_rows}
    if request_kinds - {
        "missing_episode", "missing_season", "missing_media", "missing_subtitle",
    }:
        raise ReplenishmentCandidateError(
            "补源请求包含不支持的 gap 类型",
            stage="lane_validation",
        )
    if "missing_subtitle" in request_kinds and request_kinds - {"missing_subtitle"}:
        raise ReplenishmentCandidateError(
            "字幕缺口必须使用独立 sidecar 补源请求",
            stage="lane_validation",
        )
    # ``client`` may be injected by the automatic coordinator so the staging
    # upload and its later Engine readback use the same AList session.  Do not
    # overwrite it below with a second client.
    payload_verified = False
    delivery_stage = "candidate_preflight"
    try:
        request_gap_kinds = {
            str(gap.get("id")): str(gap.get("kind") or "")
            for gap in request.get("gaps") or []
            if isinstance(gap, Mapping) and isinstance(gap.get("id"), str)
            and gap.get("id")
        }
        _pause_checkpoint(pause_requested)
        if pause_requested is None:
            preflight = _preflight(
                selection_wrapper, workspace / "preflight", resume_workspace=workspace,
            )
        else:
            preflight = _preflight(
                selection_wrapper,
                workspace / "preflight",
                resume_workspace=workspace,
                pause_requested=pause_requested,
            )
        bundle = selection_wrapper["selection"]
        # X-phase shape: acquire one member at a time — download it, upload
        # it, prove the remote copy is visible, then drop the local bytes.
        # The local peak footprint is one member (one episode), never the
        # whole pack, so a multi-season remux cannot exhaust the host disk.
        # Each member's aria2 run gets the full torrent-timeout budget; the
        # per-member fail-fast is the BT idle timeout, not a shared deadline.
        # Every member's binding is validated for every selection before any
        # byte moves, so a malformed selection still fails as a clean
        # candidate error with zero partial remote commits.
        plans: list[dict[str, Any]] = []
        for offset, selection in enumerate(bundle["selections"], start=1):
            if not isinstance(selection, Mapping):
                raise ReplenishmentCandidateError(
                    "补源选择项格式无效", stage="artifact_validation",
                )
            candidate_dir = workspace / f"download-{offset:02d}"
            _pause_checkpoint(pause_requested)
            candidate_dir.mkdir(parents=True, exist_ok=True)
            # A pre-pipeline attempt may have left a bulk-layout payload
            # directory behind.  Its single aria2 control file cannot serve
            # per-member resume, so reclaim the space instead of keeping it.
            legacy_payload = candidate_dir / "payload"
            if legacy_payload.exists():
                print(
                    f"[replenishment] 清理旧批量下载布局 "
                    f"{offset}/{len(bundle['selections'])}",
                    flush=True,
                )
                shutil.rmtree(legacy_payload, ignore_errors=True)
            verified = preflight["candidates"][offset - 1]
            manifest = verified["manifest"]
            torrent_path = Path(verified["torrent_path"])
            try:
                indices, by_index = _verify_manifest(selection, manifest)
                companion_by_index = _selected_companion_indices(
                    selection,
                    selected_gap_ids={
                        str(value) for value in selection.get("selected_gap_ids") or []
                    },
                )
            except ValueError as exc:
                raise ReplenishmentCandidateError(
                    str(exc), stage="candidate_manifest", candidate=selection,
                ) from exc
            acquisition = selection["acquisition"]
            size_map = acquisition["file_size_by_index"]
            path_map = acquisition["file_path_by_index"]
            for index in sorted(indices):
                expected_size = int(size_map[str(index)])
                relative_path = str(path_map[str(index)])
                gaps = sorted(set(by_index.get(index, [])))
                companion_for = sorted(set(companion_by_index.get(index, [])))
                if companion_for and gaps:
                    raise ReplenishmentCandidateError(
                        "伴随字幕与直接 gap 绑定冲突",
                        stage="candidate_payload_validation", candidate=selection,
                    )
                extension = Path(relative_path).suffix.casefold()
                if companion_for:
                    if len(companion_for) != 1:
                        raise ReplenishmentCandidateError(
                            "伴随字幕没有唯一媒体 gap 绑定",
                            stage="candidate_payload_validation", candidate=selection,
                        )
                    paired_gap_id = companion_for[0]
                    paired_indices = acquisition.get("file_index_by_gap", {}).get(paired_gap_id)
                    if (
                        not isinstance(paired_indices, list) or len(paired_indices) != 1
                        or type(paired_indices[0]) is not int or paired_indices[0] <= 0
                    ):
                        raise ReplenishmentCandidateError(
                            "伴随字幕缺少唯一正片 manifest 索引",
                            stage="candidate_payload_validation", candidate=selection,
                        )
                    paired_video_index = paired_indices[0]
                    paired_video_path = path_map.get(str(paired_video_index))
                    if (
                        extension not in SUBTITLE_EXTENSIONS
                        or _is_supplemental_video_path(relative_path)
                        or not isinstance(paired_video_path, str)
                    ):
                        raise ReplenishmentCandidateError(
                            f"伴随字幕不是可配对的字幕格式: {relative_path}",
                            stage="candidate_payload_validation", candidate=selection,
                        )
                    remote_file = _safe_name(
                        f"{paired_gap_id} - {Path(relative_path).stem}", limit=170,
                    ) + extension
                    plan = {
                        "kind": "subtitle", "gaps": [paired_gap_id],
                        "companion_for_gap_ids": [paired_gap_id],
                        "paired_video_index": paired_video_index,
                        "paired_video_source_name": paired_video_path,
                        "subtitle_language": "zh",
                    }
                else:
                    gap_prefix = "+".join(gaps)
                    kinds = {request_gap_kinds.get(gap_id) for gap_id in gaps}
                    if not gaps or None in kinds or "" in kinds:
                        raise ReplenishmentCandidateError(
                            "选中文件缺少受审计缺口绑定",
                            stage="candidate_payload_validation", candidate=selection,
                        )
                    if kinds == {"missing_subtitle"}:
                        allowed_extensions = SUBTITLE_EXTENSIONS
                    elif "missing_subtitle" not in kinds:
                        allowed_extensions = VIDEO_EXTENSIONS
                    else:
                        # A manifest member must be either a video move or one
                        # exact subtitle sidecar.  Sharing it across both lanes
                        # would defeat the formal-library pairing proof.
                        raise ReplenishmentCandidateError(
                            "选中文件同时绑定媒体和字幕缺口",
                            stage="candidate_payload_validation", candidate=selection,
                        )
                    if extension not in allowed_extensions:
                        raise ReplenishmentCandidateError(
                            f"选中文件不是本次缺口支持的媒体格式: {relative_path}",
                            stage="candidate_payload_validation", candidate=selection,
                        )
                    remote_file = _safe_name(
                        f"{gap_prefix} - {Path(relative_path).stem}", limit=170,
                    ) + extension
                    plan = {
                        "kind": "subtitle" if extension in SUBTITLE_EXTENSIONS else "video",
                        "gaps": gaps,
                    }
                plan.update({
                    "offset": offset, "index": index, "size": expected_size,
                    "path": relative_path, "remote_name": remote_file,
                    "candidate_dir": candidate_dir, "torrent_path": torrent_path,
                    "acquisition": acquisition, "selection": selection,
                    "is_video": extension in VIDEO_EXTENSIONS,
                })
                plans.append(plan)
        if not plans:
            raise ReplenishmentCandidateError(
                "没有可上传的补源媒体或字幕",
                stage="candidate_payload_validation",
            )
        delivery_stage = "delivery_connect"
        _pause_checkpoint(pause_requested)
        client = client or _alist_client()
        login = getattr(client, "login", None)
        if callable(login) and not getattr(client, "token", None):
            _pause_checkpoint(pause_requested)
            login()
        delivery_stage = "delivery_prepare"
        if pause_requested is None:
            _ensure_automatic_staging_root(client, remote_parent, remote_root)
        else:
            _ensure_automatic_staging_root(
                client,
                remote_parent,
                remote_root,
                pause_requested=pause_requested,
            )
        has_video = any(plan["kind"] == "video" for plan in plans)
        has_subtitle = any(plan["kind"] == "subtitle" for plan in plans)
        # A mixed provider candidate must never expose its subtitle members
        # to the child Engine planner.  The planner is deliberately strict
        # about subtitle companions and could otherwise turn a sidecar for an
        # already-existing episode into a child-plan problem.  Keep both
        # subroots inside the one task-owned attempt so final cleanup remains
        # bounded, but hand only ``media`` to the child.
        mixed_delivery = has_video and has_subtitle
        media_staging_root = join_remote(remote_root, "media") if mixed_delivery else remote_root
        subtitle_staging_root = join_remote(remote_root, "subtitles") if mixed_delivery else remote_root
        if mixed_delivery:
            _pause_checkpoint(pause_requested)
            client.mkdir(media_staging_root)
            _pause_checkpoint(pause_requested)
            client.mkdir(subtitle_staging_root)
        for plan in plans:
            plan["delivery_root"] = (
                subtitle_staging_root if plan["kind"] == "subtitle"
                else media_staging_root
            )
        delivery_stage = "delivery_upload"
        # Batch members into groups: requesting a wider piece range lets the
        # swarm's partial peers serve their fragments (a single-member range
        # starves: the peers hold pieces across the whole pack).  The batch
        # size bounds the local peak footprint (batch x largest member).
        batch_size = _bounded_seconds(
            "SCRAPEFLOW_REPLENISHMENT_MEMBER_BATCH", 4, 1, 64,
        )
        pending: list[dict[str, Any]] = []
        for number, plan in enumerate(plans, start=1):
            delivery_root = str(plan["delivery_root"])
            plan["delivery_root_str"] = delivery_root
            target = join_remote(delivery_root, str(plan["remote_name"]))
            committed = client.exact_file_info(target)
            if (
                committed is not None
                and int(committed.get("size") or 0) == int(plan["size"])
            ):
                # A previously interrupted attempt already proved this member
                # remotely; skip both its download and its upload.
                print(
                    f"[replenishment] 复用远端已提交成员 "
                    f"{number}/{len(plans)}: {plan['remote_name']}",
                    flush=True,
                )
                uploaded.append({
                    "gap_ids": list(plan["gaps"]),
                    **({
                        "companion_for_gap_ids": plan["companion_for_gap_ids"],
                        "paired_video_index": plan["paired_video_index"],
                        "paired_video_source_name": plan["paired_video_source_name"],
                        "subtitle_language": plan["subtitle_language"],
                    } if plan["kind"] == "subtitle" and plan.get("companion_for_gap_ids") else {}),
                    "source": None, "remote_name": plan["remote_name"],
                    "size": int(plan["size"]), "manifest_index": plan["index"],
                    "source_name": plan["path"], "provider_path": plan["path"],
                    "kind": plan["kind"], "delivery_root": delivery_root,
                    "remote_committed": True,
                })
            else:
                pending.append(plan)
        for group_start in range(0, len(pending), batch_size):
            group = pending[group_start:group_start + batch_size]
            group_number = group_start // batch_size + 1
            group_total = (len(pending) + batch_size - 1) // batch_size
            group_dir = group[0]["candidate_dir"] / f"group-{group_number:03d}"
            group_payload = group_dir / "payload"
            group_payload.mkdir(parents=True, exist_ok=True)
            # A resumed attempt regrouped the pending members, so a member
            # completed inside an OLD group is invisible to this group's
            # payload check.  Before declaring anything pending for aria2,
            # adopt any retained completed member from any group of this
            # candidate — matched by exact manifest path and size, never by
            # group ordinal.  Adopted members flow straight into the upload
            # loop below.
            adopted: list[dict[str, Any]] = []
            still_pending: list[dict[str, Any]] = []
            for plan in group:
                candidate_dir = Path(plan["candidate_dir"])
                source = _find_retained_member(
                    candidate_dir, str(plan["path"]), int(plan["size"]),
                )
                if source is not None:
                    plan["adopted_source"] = source
                    adopted.append(plan)
                else:
                    still_pending.append(plan)
            group_indices = {int(plan["index"]) for plan in still_pending}
            if (
                not still_pending
                or _payload_is_complete(
                    group_payload, group[0]["acquisition"], group_indices,
                )
            ):
                print(
                    f"[replenishment] 复用已验证下载组 {group_number}/{group_total}",
                    flush=True,
                )
                group = still_pending
            else:
                group = still_pending
                command = [
                    "aria2c", "--seed-time=0", "--file-allocation=none",
                    "--allow-overwrite=true", "--auto-file-renaming=false",
                    "--summary-interval=60", "--console-log-level=notice",
                    # In mainland deployments the HTTP proxy exists for the
                    # blocked search indexes only; tracker announces and peer
                    # traffic must stay direct.  DHT/PEX/LPD give the swarm a
                    # chance even when every tracker is unreachable.
                    "--enable-dht=true", "--enable-peer-exchange=true",
                    "--bt-enable-lpd=true",
                    f"--bt-stop-timeout={_bounded_seconds('SCRAPEFLOW_REPLENISHMENT_BT_IDLE_TIMEOUT', 600, 60, 3600)}",
                    f"--dir={group_payload}",
                    "--select-file=" + ",".join(
                        str(int(plan["index"])) for plan in group
                    ),
                    str(group[0]["torrent_path"]),
                ]
                budget = _bounded_seconds(
                    "SCRAPEFLOW_REPLENISHMENT_TORRENT_TIMEOUT", 21600, 300, 86400,
                )
                print(
                    f"[replenishment] 下载组 {group_number}/{group_total} "
                    f"({len(group)} 个成员)",
                    flush=True,
                )
                try:
                    _pause_checkpoint(pause_requested)
                    if pause_requested is None:
                        completed = subprocess.run(
                            command, text=True, stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT,
                            timeout=budget,
                            env=_direct_download_env(os.environ),
                        )
                    else:
                        # 带 pause 回调的生产路径走可中断执行:暂停即刻
                        # SIGTERM aria2c(断点保全),不再等满整个预算窗。
                        completed = _run_download_command(
                            command, budget,
                            pause_requested=pause_requested,
                            env=_direct_download_env(os.environ),
                        )
                except subprocess.TimeoutExpired as exc:
                    if _payload_bytes(group_payload) > 0:
                        # Bytes already flowed in this attempt: the swarm is
                        # intermittent, not dead.  Keep the workspace (the
                        # group control file resumes) and retry in a later
                        # window instead of excluding a good resource.
                        raise ReplenishmentInfrastructureError(
                            "aria2c 下载超时但已有部分进度；保留断点等待下个 seeder 窗口",
                            stage="candidate_download",
                        ) from exc
                    raise ReplenishmentCandidateError(
                        "aria2c 下载超过总时限", stage="candidate_download",
                        candidate=group[0]["selection"],
                    ) from exc
                if completed.returncode != 0:
                    tail = " ".join(completed.stdout.splitlines()[-8:])[:1200]
                    if _payload_bytes(group_payload) > 0:
                        # A partial download (typically the BT idle-stop after
                        # seeders left mid-window) proves the resource serves
                        # bytes; classify the window, not the candidate.
                        raise ReplenishmentInfrastructureError(
                            f"aria2c 下载未完成但已有部分进度；保留断点等待下个 seeder 窗口: {tail}",
                            stage="candidate_download",
                        )
                    if _looks_like_local_download_failure(tail):
                        # Zero bytes plus host-side errno evidence: the
                        # download never proved anything about the resource.
                        # Excluding the locator here would burn a good
                        # candidate on a full disk.
                        raise ReplenishmentInfrastructureError(
                            f"aria2c 下载失败且呈本地故障特征；保留断点等待重试: {tail}",
                            stage="candidate_download",
                        )
                    raise ReplenishmentCandidateError(
                        f"aria2c 下载失败: {tail}", stage="candidate_download",
                        candidate=group[0]["selection"],
                    )
            for plan in adopted + list(group):
                _pause_checkpoint(pause_requested)
                delivery_root = str(plan["delivery_root"])
                adopted_source = plan.get("adopted_source")
                if adopted_source is not None:
                    source = Path(adopted_source)
                else:
                    source = _find_download(
                        group_payload, str(plan["path"]), int(plan["size"]),
                    )
                if plan["is_video"]:
                    _verify_video_payload(source, int(plan["size"]), plan["selection"])
                row: dict[str, Any] = {
                    "gap_ids": list(plan["gaps"]),
                    **({
                        "companion_for_gap_ids": plan["companion_for_gap_ids"],
                        "paired_video_index": plan["paired_video_index"],
                        "paired_video_source_name": plan["paired_video_source_name"],
                        "subtitle_language": plan["subtitle_language"],
                    } if plan["kind"] == "subtitle" and plan.get("companion_for_gap_ids") else {}),
                    "source": source, "remote_name": plan["remote_name"],
                    "size": int(plan["size"]), "manifest_index": plan["index"],
                    "source_name": plan["path"], "provider_path": plan["path"],
                    "kind": plan["kind"], "delivery_root": delivery_root,
                }
                print(
                    f"[replenishment] 上传成员 {plan['remote_name']}",
                    flush=True,
                )
                # 2026-09-08 教训:11GB 单集上传流式约 35 分钟 + 响应等待
                # 最长约 50 分钟,全程零日志,监控只能靠字节探测猜死活。
                # 每 5 分钟一行时间心跳,证明成员仍在处理中。
                heartbeat_stop = threading.Event()

                def _heartbeat(member_name: str) -> None:
                    elapsed = 0
                    while not heartbeat_stop.wait(300):
                        elapsed += 300
                        print(
                            f"[replenishment] 上传进行中 {member_name}"
                            f" 已 {elapsed // 60} 分钟",
                            flush=True,
                        )

                heartbeat = threading.Thread(
                    target=_heartbeat, args=(plan["remote_name"],),
                    daemon=True,
                )
                heartbeat.start()
                try:
                    # From the first upload attempt on, an ambiguous failure must
                    # be reconciled rather than retried from scratch: a partial
                    # remote commit can no longer be distinguished from a miss.
                    payload_verified = True
                    if pause_requested is None:
                        _automatic_upload(client, delivery_root, row)
                    else:
                        _automatic_upload(
                            client, delivery_root, row,
                            pause_requested=pause_requested,
                        )
                    # The remote copy must be visible before the local bytes are
                    # dropped, or an interrupted run could lose both at once.
                    if pause_requested is None:
                        _verify_remote_uploads(client, delivery_root, [row])
                    else:
                        _verify_remote_uploads(
                            client, delivery_root, [row],
                            pause_requested=pause_requested,
                        )
                finally:
                    heartbeat_stop.set()
                uploaded.append(row)
            # The whole group's bytes are proven remotely; free them locally.
            shutil.rmtree(group_dir, ignore_errors=True)
            print(
                f"[replenishment] 组完成并释放本地 {group_number}/{group_total}",
                flush=True,
            )
        delivery_stage = "delivery_visibility"
        grouped_uploads: dict[str, list[dict[str, Any]]] = {}
        for row in uploaded:
            delivery_root = str(row["delivery_root"])
            grouped_uploads.setdefault(delivery_root, []).append(row)
        for delivery_root, rows in grouped_uploads.items():
            if pause_requested is None:
                _verify_remote_uploads(client, delivery_root, rows)
            else:
                _verify_remote_uploads(
                    client,
                    delivery_root,
                    rows,
                    pause_requested=pause_requested,
                )
        delivery_files: list[dict[str, Any]] = []
        for row in uploaded:
            delivery_files.append({
                "path": join_remote(str(row["delivery_root"]), str(row["remote_name"])),
                "size": int(row["size"]),
                "gap_ids": list(row["gap_ids"]),
                "kind": str(row["kind"]),
            })
        # Both local bytes and the remote task tree remain retry/reconcile
        # evidence.  The coordinator owns their eventual cleanup after a
        # formal readback and targeted audit prove the gap disappeared.
        return {
            "lane": "magnet",
            "attempt_id": posixpath.basename(remote_root.rstrip("/")),
            "staging_root": remote_root,
            "files": delivery_files,
        }
    except BaseException as exc:
        if getattr(exc, "pause_requested", False) is True:
            raise
        delivery_failure = payload_verified and isinstance(exc, Exception)
        # Never recursively remove the deterministic remote delivery root on
        # an ambiguous failure. It may already contain a verified object from
        # this or an earlier attempt. A later retry inspects the exact objects
        # in place; cleanup remains limited to this attempt's staging prefix.
        if delivery_failure:
            # Keep both the verified local payload and any exact-size remote
            # objects.  The deterministic workspace/remote names make the next
            # retry skip the torrent and already committed uploads.
            if isinstance(exc, ReplenishmentDeliveryError):
                raise
            raise ReplenishmentDeliveryError(str(exc), stage=delivery_stage) from exc
        # Capacity/dependency/orchestration failures do not invalidate bytes
        # retained by a previous attempt.  Candidate failures do.  An
        # *unclassified* failure (an AList ApiError, an OSError, a config
        # ValueError) may be this host's fault just as much as the typed
        # infrastructure errors: keep the workspace so a resumed attempt
        # continues from the retained bytes — the orchestrator classifies
        # anything untyped as infrastructure and retries without excluding
        # the locator.  Wiping here would replay the 4e55e59 incident family
        # through the AList channel.
        if isinstance(exc, ReplenishmentCandidateError):
            _pause_checkpoint(pause_requested)
            shutil.rmtree(workspace, ignore_errors=True)
        raise
