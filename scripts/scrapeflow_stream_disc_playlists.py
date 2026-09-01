#!/usr/bin/env python3
"""Stream proven single-clip Blu-ray playlists to AList staging without disk.

The command is generic: it reads a remote UDF/ISO image through strict HTTP
Range, calculates Quark's MD5/SHA1 in a first pass, then streams the same image
extents to AList with those hashes in a second pass.  No media file is created
on local storage.
"""

from __future__ import annotations

import argparse
import contextlib
import json
import os
import posixpath
import re
from datetime import datetime, timezone
from pathlib import Path

from engine.scrapeflow.core import AListClient
from engine.scrapeflow.disc_image import (
    DiscImageError,
    InnerFile,
    probe_disc_image_via_alist,
    stream_inner_file_via_alist,
)

_PROGRESS_STEP = 512 * 1024 * 1024
_PLAYLIST_RE = re.compile(r"^[0-9]{5}\.mpls$", re.IGNORECASE)
_ROOT_JOB_RE = re.compile(r"^engine-[0-9a-f]{32}$")


def _now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _atomic_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix(path.suffix + ".tmp")
    temp.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    os.replace(temp, path)


class ProgressAList(AListClient):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._tracked_file: InnerFile | None = None
        self._tracked_state: dict[str, object] | None = None
        self._tracked_state_path: Path | None = None
        self._transfer_pass = 0

    def begin_transfer(
        self,
        inner_file: InnerFile,
        state: dict[str, object],
        state_path: Path,
    ) -> None:
        self._tracked_file = inner_file
        self._tracked_state = state
        self._tracked_state_path = state_path
        self._transfer_pass = 0

    def end_transfer(self) -> None:
        self._tracked_file = None
        self._tracked_state = None
        self._tracked_state_path = None
        self._transfer_pass = 0

    @contextlib.contextmanager
    def open_file_range_reader(
        self,
        path: str,
        *,
        expected_size: int,
        refresh: bool = True,
    ):
        with super().open_file_range_reader(
            path,
            expected_size=expected_size,
            refresh=refresh,
        ) as base:
            tracked_file = self._tracked_file
            state = self._tracked_state
            state_path = self._tracked_state_path
            if tracked_file is None or state is None or state_path is None:
                yield base
                return
            self._transfer_pass += 1
            pass_no = self._transfer_pass
            phase = "hashing" if pass_no == 1 else "uploading"
            read_total = 0
            next_report = _PROGRESS_STEP
            _atomic_json(
                state_path,
                {
                    **state,
                    "status": phase,
                    "pass": pass_no,
                    "pass_bytes": 0,
                    "updated_at": _now(),
                },
            )
            print(f"PHASE={phase} PASS={pass_no}", flush=True)

            def tracked(offset: int, length: int) -> bytes:
                nonlocal read_total, next_report
                data = base(offset, length)
                read_total += len(data)
                if read_total >= next_report or read_total == tracked_file.size:
                    percent = 100.0 * read_total / tracked_file.size
                    print(
                        f"PROGRESS pass={pass_no} bytes={read_total} "
                        f"total={tracked_file.size} pct={percent:.2f}",
                        flush=True,
                    )
                    _atomic_json(
                        state_path,
                        {
                            **state,
                            "status": phase,
                            "pass": pass_no,
                            "pass_bytes": read_total,
                            "total_bytes": tracked_file.size,
                            "progress_percent": round(percent, 3),
                            "updated_at": _now(),
                        },
                    )
                    while next_report <= read_total:
                        next_report += _PROGRESS_STEP
                return data

            yield tracked


def _arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--image", required=True)
    parser.add_argument("--root-job-id", required=True)
    parser.add_argument("--season", required=True, type=int)
    parser.add_argument("--disc", required=True, type=int)
    parser.add_argument("--playlist", action="append", required=True)
    parser.add_argument(
        "--staging-base",
        default="/quark/影视/ScrapeFlow/展开",
    )
    parser.add_argument(
        "--state-root",
        default="/data/disc-expansion",
    )
    parser.add_argument(
        "--answerbook-plan",
        help="可选的 disc_expansion_answerbook_plan JSON，用于绑定坐标和最终目标",
    )
    parser.add_argument("--chunk-mib", type=int, default=32)
    args = parser.parse_args()
    if _ROOT_JOB_RE.fullmatch(args.root_job_id) is None:
        parser.error("--root-job-id 格式无效")
    if args.season <= 0 or args.disc <= 0:
        parser.error("--season/--disc 必须大于 0")
    if args.chunk_mib <= 0 or args.chunk_mib > 128:
        parser.error("--chunk-mib 必须位于 1..128")
    for item in args.playlist:
        if _PLAYLIST_RE.fullmatch(item) is None:
            parser.error(f"无效 playlist: {item}")
    return args


def _answerbook_rows(args: argparse.Namespace) -> dict[str, dict[str, object]]:
    if not args.answerbook_plan:
        return {}
    payload = json.loads(Path(args.answerbook_plan).read_text(encoding="utf-8"))
    if payload.get("artifact_kind") != "disc_expansion_answerbook_plan":
        raise DiscImageError("answerbook plan 类型无效")
    if payload.get("root_job_id") != args.root_job_id:
        raise DiscImageError("answerbook plan RootJob 不匹配")
    images = payload.get("images")
    if not isinstance(images, list):
        raise DiscImageError("answerbook plan images 格式无效")
    matched = [item for item in images if isinstance(item, dict) and item.get("image_path") == args.image]
    if len(matched) != 1:
        raise DiscImageError("answerbook plan 无法唯一匹配镜像")
    rows: dict[str, dict[str, object]] = {}
    for mapping in matched[0].get("mappings") or []:
        if not isinstance(mapping, dict):
            raise DiscImageError("answerbook plan mapping 格式无效")
        member_ref = mapping.get("member_ref")
        if not isinstance(member_ref, str) or not member_ref:
            raise DiscImageError("answerbook plan mapping 缺少 member_ref")
        key = posixpath.basename(member_ref).casefold()
        if key in rows:
            raise DiscImageError("answerbook plan playlist 重复")
        rows[key] = {
            **mapping,
            "source_observation_id": matched[0].get("source_observation_id"),
            "work_unit_id": matched[0].get("work_unit_id"),
            "identity_review_id": matched[0].get("identity_review_id"),
        }
    return rows


def _main_playlist(inventory, name: str):
    candidates = [
        item
        for item in inventory.playlists
        if posixpath.basename(item.inner_path).casefold() == name.casefold()
    ]
    primary = [
        item
        for item in candidates
        if "/bdmv/playlist/" in item.inner_path.casefold()
        and "/backup/" not in item.inner_path.casefold()
    ]
    if len(primary) == 1:
        return primary[0]
    if not primary and len(candidates) == 1:
        return candidates[0]
    raise DiscImageError(f"playlist 无法唯一确定: {name}; candidates={len(candidates)}")


def main() -> int:
    args = _arguments()
    client = ProgressAList(
        os.environ["ALIST_URL"],
        os.environ["ALIST_USERNAME"],
        os.environ["ALIST_PASSWORD"],
        timeout=120,
        retries=2,
        allow_insecure_http=True,
    )
    client.login()
    print("PROBE_START image=" + args.image, flush=True)
    inventory = probe_disc_image_via_alist(client, args.image)
    answerbook_rows = _answerbook_rows(args)
    source_info = client.exact_file_info(args.image)
    if source_info is None:
        raise DiscImageError("镜像在 probe 后不存在")

    season_dir = f"S{args.season:02d}"
    disc_dir = f"DISC{args.disc}"
    staging_dir = (
        args.staging_base.rstrip("/")
        + "/"
        + args.root_job_id
        + "/"
        + season_dir
        + "/"
        + disc_dir
    )
    directories = (
        args.staging_base.rstrip("/"),
        args.staging_base.rstrip("/") + "/" + args.root_job_id,
        args.staging_base.rstrip("/") + "/" + args.root_job_id + "/" + season_dir,
        staging_dir,
    )
    for directory in directories:
        if client.try_list(directory, refresh=True) is None:
            client.mkdir(directory)

    state_root = Path(args.state_root) / args.root_job_id
    for playlist_name in args.playlist:
        playlist = _main_playlist(inventory, playlist_name)
        answerbook = answerbook_rows.get(playlist_name.casefold())
        if args.answerbook_plan and answerbook is None:
            raise DiscImageError(f"answerbook plan 未授权 playlist: {playlist_name}")
        if answerbook is not None:
            expected_member = answerbook.get("member_ref")
            if expected_member != playlist.inner_path:
                raise DiscImageError(
                    f"answerbook member_ref 不匹配: expected={expected_member}, "
                    f"actual={playlist.inner_path}"
                )
            expected_duration = answerbook.get("duration_seconds")
            if isinstance(expected_duration, (int, float)) and abs(
                float(expected_duration) - playlist.duration_seconds
            ) > 0.05:
                raise DiscImageError(
                    f"answerbook playlist 时长不匹配: expected={expected_duration}, "
                    f"actual={playlist.duration_seconds}"
                )
        if len(playlist.play_items) != 1:
            raise DiscImageError(
                f"playlist 不是单 clip 正片: {playlist.inner_path}"
            )
        play_item = playlist.play_items[0]
        inner_file = next(
            (
                item
                for item in inventory.inner_files
                if "/bdmv/stream/" in item.inner_path.casefold()
                and posixpath.splitext(posixpath.basename(item.inner_path))[0]
                == play_item.clip_id
            ),
            None,
        )
        if inner_file is None:
            raise DiscImageError(f"playlist clip 不存在: {play_item.clip_id}")
        target_name = (
            f"{season_dir}-{disc_dir}-{posixpath.splitext(playlist_name)[0]}"
            f"__clip-{play_item.clip_id}.m2ts"
        )
        target_path = staging_dir + "/" + target_name
        state_path = state_root / (
            f"{season_dir}-{disc_dir}-{posixpath.splitext(playlist_name)[0]}.json"
        )
        state: dict[str, object] = {
            "schema_version": 1,
            "root_job_id": args.root_job_id,
            "image_path": args.image,
            "source_size": source_info.get("size"),
            "source_version": source_info.get("version"),
            "playlist": playlist.inner_path,
            "clip_id": play_item.clip_id,
            "inner_path": inner_file.inner_path,
            "target_path": target_path,
            "total_bytes": inner_file.size,
            "duration_seconds": playlist.duration_seconds,
            "status": "preparing",
            "materialization_stage": "extracted_stream",
            "started_at": _now(),
            "updated_at": _now(),
        }
        if answerbook is not None:
            state.update(
                {
                    "source_observation_id": answerbook.get("source_observation_id"),
                    "work_unit_id": answerbook.get("work_unit_id"),
                    "identity_review_id": answerbook.get("identity_review_id"),
                    "coordinate": answerbook.get("coordinate"),
                    "expected_target_path": answerbook.get("target_path"),
                    "answerbook_member_ref": answerbook.get("member_ref"),
                    "answerbook_duration_seconds": answerbook.get("duration_seconds"),
                    "requires_container_remux": str(answerbook.get("target_path") or "")
                    .casefold()
                    .endswith(".mkv"),
                }
            )
        _atomic_json(state_path, state)
        print(
            "MAPPING="
            + json.dumps(
                {
                    "playlist": playlist.inner_path,
                    "clip": play_item.clip_id,
                    "duration_seconds": round(playlist.duration_seconds, 3),
                    "inner": inner_file.inner_path,
                    "bytes": inner_file.size,
                    "target": target_path,
                },
                ensure_ascii=False,
            ),
            flush=True,
        )
        existing = client.exact_file_info(target_path)
        if existing is not None:
            if existing.get("size") != inner_file.size:
                raise DiscImageError(f"staging 目标冲突: {target_path}")
            state.update(
                {
                    "status": "already_present",
                    "target_version": existing.get("version"),
                    "updated_at": _now(),
                }
            )
            _atomic_json(state_path, state)
            print("ALREADY_PRESENT=" + target_path, flush=True)
            continue

        client.begin_transfer(inner_file, state, state_path)
        try:
            result = stream_inner_file_via_alist(
                client,
                image_path=args.image,
                inner_file=inner_file,
                target_path=target_path,
                content_type="video/mp2t",
                chunk_bytes=args.chunk_mib * 1024 * 1024,
            )
        except Exception as exc:
            state.update(
                {
                    "status": "failed",
                    "error": f"{type(exc).__name__}: {exc}",
                    "updated_at": _now(),
                }
            )
            _atomic_json(state_path, state)
            print("FAILED=" + str(state["error"]), flush=True)
            raise
        finally:
            client.end_transfer()
        state.update(
            {
                "status": "completed",
                "md5": result.md5,
                "sha1": result.sha1,
                "target_version": result.target_version,
                "completed_at": _now(),
                "updated_at": _now(),
            }
        )
        _atomic_json(state_path, state)
        print("COMPLETED=" + json.dumps(state, ensure_ascii=False), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
