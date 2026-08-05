#!/usr/bin/env python3
"""安全地规划并通过 AList 解压分卷归档。

生成阶段只读取归档目录并写入不含密码的 JSON 计划。执行阶段
必须提交同一计划的完整 SHA-256，再次核对分卷快照、归档成员和
目标冲突。密码可从所选目录树和路径中的“密码：...”标记自动识别，但从不写入
计划、journal 或终端输出。
"""

from __future__ import annotations

import argparse
import getpass
import hashlib
import json
import os
import re
import shutil
import signal
import subprocess
import sys
import tempfile
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scraper import (  # noqa: E402
    AListClient,
    ApiError,
    DEFAULT_ALIST_URL,
    ScraperError,
    SUBTITLE_EXTS,
    VIDEO_EXTS,
    _canonical_json_bytes,
    _collision_key,
    _entry_hash_value,
    _load_json_text,
    _reserve_output_path,
    _validate_remote_basename,
    _write_json_reserved,
    cleanup_reason,
    is_scraper_lock,
    join_remote,
    normalize_remote_path,
    split_remote,
)
from scrapeflow.alist_exact_file_adapter import AListExactFileAdapter  # noqa: E402
from scrapeflow.local_upload_transaction import (  # noqa: E402
    LocalUploadSpec,
    LocalUploadTransactionError,
    deterministic_local_upload_id,
    run_local_upload_transaction,
)
from scrapeflow.remote_file_transaction import (  # noqa: E402
    RemoteFileTransactionError,
    RemoteFileTransferSpec,
    discard_completed_remote_file_transaction,
    prepare_remote_file_transaction,
    run_remote_file_transaction,
)


ARCHIVE_PLAN_SCHEMA = 2
MIN_SAFE_ALIST_VERSION = (3, 57, 0)
NO_ARCHIVES_EXIT_CODE = 3
MULTIPART_RE = re.compile(r"^(?P<prefix>.+\.(?:7z|zip)\.)(?P<index>\d{3})$", re.I)
RAR_PART_RE = re.compile(r"^(?P<prefix>.+\.part)(?P<index>\d+)\.rar$", re.I)
SINGLE_ARCHIVE_EXTS = {".zip", ".7z", ".rar"}
DISGUISED_CONTAINER_EXTS = {".exe", ".bin", ".dat"}
SUBTITLE_ARCHIVE_NAME_RE = re.compile(r"(?:字幕|subtitles?)", re.I)
DIRECT_SUBTITLE_MEMBER_LIMIT = 64 * 1024 * 1024
DIRECT_SUBTITLE_ARCHIVE_LIMIT = 512 * 1024 * 1024
MAX_LOCAL_ARCHIVE_MEMBERS = 20_000
MAX_LOCAL_ARCHIVE_DEPTH = 32
MAX_LOCAL_EXPANSION_RATIO = 200
LOCAL_DISK_RESERVE_BYTES = 2 * 1024 * 1024 * 1024
EXTRACT_VERIFY_ATTEMPTS = 5
EXTRACT_VERIFY_DELAY_SECONDS = 1.0
SUBTITLE_MIME_TYPES = {
    ".ass": "text/x-ssa",
    ".ssa": "text/x-ssa",
    ".srt": "application/x-subrip",
    ".vtt": "text/vtt",
    ".sub": "text/plain",
    ".idx": "application/octet-stream",
    ".sup": "application/octet-stream",
}
PASSWORD_MARKER_RE = re.compile(
    r"(?:解压|压缩包|归档)?\s*密码\s*[:：=]\s*"
    r"(?P<password>[^\s,，;；/\\]+)",
    re.I,
)
NON_MEDIA_ARCHIVE_CONTEXT_RE = re.compile(
    r"(?:^|/)(?:小说|同人|漫画|书籍|电子书|文库(?:版)?|短篇集|"
    r"txt|texts?|novels?|ebooks?|comics?)(?:/|$)",
    re.IGNORECASE,
)
FONT_FILE_EXTS = {".ttf", ".otf", ".ttc", ".woff", ".woff2"}


class NoArchivesFound(ScraperError):
    """The source is valid but contains no supported multipart archive."""


class ArchiveOutputRejected(ScraperError):
    """The storage provider deterministically refused one extracted output."""


def _disguised_file_format(prefix: bytes) -> str | None:
    """Identify renamed archives/media by bytes only; the file is never run."""
    archive_signatures = (
        ("7z", b"7z\xbc\xaf'\x1c"),
        ("rar", b"Rar!\x1a\x07\x00"),
        ("rar", b"Rar!\x1a\x07\x01\x00"),
        ("zip", b"PK\x03\x04"),
        ("zip", b"PK\x05\x06"),
        ("zip", b"PK\x07\x08"),
    )
    for format_name, signature in archive_signatures:
        if prefix.find(signature) >= 0:
            return format_name
    if prefix.startswith(b"\x1aE\xdf\xa3"):
        return "mkv"
    if len(prefix) >= 12 and prefix[4:8] == b"ftyp":
        return "mp4"
    if prefix.startswith(b"MZ"):
        return "exe"
    return None


def _version_tuple(raw: str) -> tuple[int, int, int]:
    match = re.search(r"(?:^|\D)(\d+)\.(\d+)\.(\d+)(?:\D|$)", raw)
    if not match:
        raise ScraperError(f"无法解析 AList 版本号: {raw!r}")
    return tuple(int(value) for value in match.groups())  # type: ignore[return-value]


def require_safe_archive_server(alist: AListClient) -> str:
    version = alist.server_version()
    if _version_tuple(version) < MIN_SAFE_ALIST_VERSION:
        raise ScraperError(
            f"AList {version} 不满足安全解压要求；至少需要 v3.57.0。"
            "旧版本缺少归档 API 或存在已知路径穿越风险。"
        )
    return version


def resolve_password(path: Path | None, env_name: str, prompt: str) -> str:
    if path:
        try:
            value = path.read_text(encoding="utf-8").strip()
        except OSError as exc:
            raise ScraperError(f"无法读取密码文件 {path}: {exc}") from exc
        if not value:
            raise ScraperError(f"密码文件为空: {path}")
        return value
    value = os.getenv(env_name)
    if value:
        return value
    if not sys.stdin.isatty():
        raise ScraperError(f"缺少 {env_name}")
    return getpass.getpass(prompt)


def discover_archive_password(alist: AListClient, archive_dir: str) -> tuple[str, str]:
    """返回 (密码, 来源类型)；不返回含密码的标记名。"""
    path_matches: list[str] = []
    for segment in normalize_remote_path(archive_dir).split("/"):
        match = PASSWORD_MARKER_RE.search(segment)
        if match:
            path_matches.append(match.group("password").strip())
    path_unique = sorted(set(filter(None, path_matches)))
    if len(path_unique) > 1:
        raise ScraperError(f"归档路径存在多个不同密码标记，拒绝猜测: {archive_dir}")
    if path_unique:
        return path_unique[0], "path-marker"

    matches: list[str] = []
    for entry in alist.list(archive_dir, refresh=True):
        name = entry.get("name")
        if not isinstance(name, str):
            continue
        match = PASSWORD_MARKER_RE.search(name)
        if match:
            value = match.group("password").strip()
            if value:
                matches.append(value)
    unique = sorted(set(matches))
    if len(unique) > 1:
        raise ScraperError(f"归档目录存在多个不同密码标记，拒绝猜测: {archive_dir}")
    if unique:
        return unique[0], "sibling-marker"
    return "", "none"


def discover_tree_passwords(alist: AListClient, source_root: str) -> list[str]:
    """查找所选目录树内的密码标记；仅返回去重后的值，不写入计划。"""
    root = normalize_remote_path(source_root)
    stack = [root]
    visited: set[str] = set()
    matches: set[str] = set()
    while stack:
        current = stack.pop()
        if current in visited:
            continue
        if len(visited) >= 10_000:
            raise ScraperError(f"密码标记扫描目录数超过安全上限: {root}")
        visited.add(current)
        for entry in alist.list(current, refresh=True):
            name = entry.get("name")
            if not isinstance(name, str) or not name:
                continue
            match = PASSWORD_MARKER_RE.search(name)
            if match:
                value = match.group("password").strip()
                if value:
                    matches.add(value)
            if entry.get("is_dir"):
                stack.append(join_remote(current, name))
    return sorted(matches)


def _archive_candidate(name: str) -> bool:
    multipart = MULTIPART_RE.match(name)
    if multipart:
        return int(multipart.group("index")) == 1
    rar_part = RAR_PART_RE.match(name)
    if rar_part:
        return int(rar_part.group("index")) == 1
    return Path(name).suffix.lower() in SINGLE_ARCHIVE_EXTS


def _reject_missing_first_parts(files: list[dict[str, Any]]) -> None:
    groups: dict[tuple[str, str, str], set[int]] = {}
    for item in files:
        name = str(item.get("name") or "")
        full_path = normalize_remote_path(str(item.get("full_path") or "/"))
        directory, _ = split_remote(full_path)
        multipart = MULTIPART_RE.match(name)
        if multipart:
            key = (directory, multipart.group("prefix"), "001")
            groups.setdefault(key, set()).add(int(multipart.group("index")))
            continue
        rar_part = RAR_PART_RE.match(name)
        if rar_part:
            key = (directory, rar_part.group("prefix"), "01.rar")
            groups.setdefault(key, set()).add(int(rar_part.group("index")))
    for (directory, prefix, first_suffix), indexes in sorted(groups.items()):
        if 1 not in indexes:
            raise ScraperError(
                f"分卷缺少第 001 卷: {join_remote(directory, prefix + first_suffix)}"
            )


def _part_entries(alist: AListClient, archive_path: str) -> list[dict[str, Any]]:
    archive_dir, archive_name = split_remote(archive_path)
    content = alist.list(archive_dir, refresh=True)
    match = MULTIPART_RE.match(archive_name)
    if match:
        prefix = match.group("prefix")
        indexed: dict[int, dict[str, Any]] = {}
        for entry in content:
            name = entry.get("name")
            if not isinstance(name, str) or entry.get("is_dir"):
                continue
            candidate = MULTIPART_RE.match(name)
            if candidate and _collision_key(candidate.group("prefix")) == _collision_key(prefix):
                indexed[int(candidate.group("index"))] = dict(entry)
        if not indexed or min(indexed) != 1:
            raise ScraperError(f"分卷缺少第 001 卷: {archive_path}")
        expected = list(range(1, max(indexed) + 1))
        if sorted(indexed) != expected:
            missing = sorted(set(expected) - set(indexed))
            raise ScraperError(f"分卷不连续，缺少: {missing}; {archive_path}")
        return [indexed[index] for index in expected]
    # part01.rar 及普通 rar 分卷的完整性由 AList archive/meta 再校验。
    return [entry for entry in content if entry.get("name") == archive_name]


def _snapshot(entry: Mapping[str, Any], parent: str) -> dict[str, Any]:
    name = str(entry.get("name", ""))
    return {
        "path": join_remote(parent, name),
        "name": name,
        "size": int(entry.get("size") or 0),
        "modified": entry.get("modified"),
        "hash": _entry_hash_value(entry),
    }


def _flatten_members(content: Any) -> list[dict[str, Any]]:
    if not isinstance(content, list):
        raise ScraperError("AList 归档目录格式异常")
    output: list[dict[str, Any]] = []

    def visit(node: Any, parent: str) -> None:
        if not isinstance(node, Mapping):
            raise ScraperError("AList 归档成员格式异常")
        raw_name = node.get("name")
        if not isinstance(raw_name, str):
            raise ScraperError("AList 归档成员缺少名称")
        try:
            name = _validate_remote_basename(raw_name)
        except ValueError as exc:
            raise ScraperError(f"归档含不安全路径成员: {raw_name!r}; {exc}") from exc
        relative = f"{parent}/{name}" if parent else name
        is_dir = bool(node.get("is_dir"))
        output.append(
            {
                "path": relative,
                "is_dir": is_dir,
                "size": int(node.get("size") or 0),
            }
        )
        children = node.get("children") or []
        if children and not is_dir:
            raise ScraperError(f"归档文件成员不应包含子项: {relative}")
        if not isinstance(children, list):
            raise ScraperError(f"归档目录子项格式异常: {relative}")
        for child in children:
            visit(child, relative)

    for root in content:
        visit(root, "")
    keys: set[str] = set()
    for member in output:
        key = _collision_key(str(member["path"]))
        if key in keys:
            raise ScraperError(f"归档内存在大小写/Unicode 等价重复路径: {member['path']}")
        keys.add(key)
    # AList/provider archive listings are not contractually ordered.  Hashing
    # the raw traversal order made an unchanged archive fail preflight when a
    # later metadata request returned siblings in another order.  Canonicalize
    # by the same collision key used for safety checks; real path/type/size
    # changes remain part of the digest and are still rejected.
    output.sort(
        key=lambda member: (
            _collision_key(str(member["path"])),
            not bool(member["is_dir"]),
            int(member["size"]),
        )
    )
    return output


def _password_for_archive(
    alist: AListClient,
    archive_dir: str,
    explicit_password: str | None,
    tree_passwords: list[str] | None = None,
) -> tuple[str, str]:
    if explicit_password is not None:
        return explicit_password, "password-file-or-env"
    value, source = discover_archive_password(alist, archive_dir)
    if value:
        return value, source
    unique_tree_passwords = sorted(set(tree_passwords or []))
    if len(unique_tree_passwords) == 1:
        return unique_tree_passwords[0], "source-tree-marker"
    # 普通无密码压缩包（尤其字幕包）应允许直接解析；若实际加密，
    # AList archive/meta 会明确报错，而不是在这里误判为缺少密码。
    return "", "none"


def _check_destination_collisions(
    alist: AListClient,
    destination: str,
    members: list[dict[str, Any]],
    *,
    allow_matching_files: bool = False,
) -> None:
    if allow_matching_files:
        for member in members:
            current = destination
            parts = str(member["path"]).split("/")
            for index, name in enumerate(parts):
                matches = [
                    entry for entry in alist.list(current, refresh=True)
                    if _collision_key(str(entry.get("name", ""))) == _collision_key(name)
                ]
                if not matches:
                    break
                is_final = index == len(parts) - 1
                valid = (
                    len(matches) == 1
                    and (
                        (not is_final and bool(matches[0].get("is_dir")))
                        or (
                            is_final
                            and bool(matches[0].get("is_dir")) == bool(member["is_dir"])
                            and (
                                bool(member["is_dir"])
                                or int(matches[0].get("size") or -1)
                                == int(member.get("size") or 0)
                            )
                        )
                    )
                )
                if not valid:
                    raise ScraperError(
                        f"解压目标与已有文件冲突: "
                        f"{join_remote(current, name)}"
                    )
                current = join_remote(current, name)
        return
    existing = alist.list(destination, refresh=True)
    existing_keys = {_collision_key(str(entry.get("name", ""))) for entry in existing}
    top_level = {str(member["path"]).split("/", 1)[0] for member in members}
    collisions = sorted(name for name in top_level if _collision_key(name) in existing_keys)
    if collisions:
        raise ScraperError(
            f"解压目标已存在归档顶层同名项: {destination}; "
            + ", ".join(collisions)
        )


def build_archive_plan(
    alist: AListClient,
    source_root: str,
    *,
    explicit_archive_password: str | None,
) -> tuple[dict[str, Any], dict[str, str]]:
    source_root = normalize_remote_path(source_root)
    server_version = require_safe_archive_server(alist)
    files = alist.walk(source_root)
    _reject_missing_first_parts(files)
    disguised_formats: dict[str, str] = {}
    media_renames: list[dict[str, Any]] = []
    unknown_disguised: list[str] = []
    for item in files:
        name = str(item.get("name", ""))
        if (
            item.get("is_dir")
            or cleanup_reason(name) is not None
            or Path(name).suffix.lower() not in DISGUISED_CONTAINER_EXTS
        ):
            continue
        full_path = normalize_remote_path(str(item.get("full_path") or "/"))
        detected = _disguised_file_format(alist.read_file_prefix(full_path))
        if detected in {"zip", "7z", "rar"}:
            disguised_formats[full_path] = detected
        elif detected in {"mkv", "mp4"}:
            source_dir, original_name = split_remote(full_path)
            new_name = str(Path(original_name).with_suffix(f".{detected}"))
            if any(
                _collision_key(str(entry.get("name", ""))) == _collision_key(new_name)
                for entry in alist.list(source_dir, refresh=True)
            ):
                raise ScraperError(f"伪装媒体的目标文件已存在: {join_remote(source_dir, new_name)}")
            media_renames.append(
                {
                    "source_path": full_path,
                    "src_dir": source_dir,
                    "name": original_name,
                    "new_name": new_name,
                    "detected_format": detected,
                    "snapshot": _snapshot(item, source_dir),
                }
            )
        elif detected is None:
            unknown_disguised.append(full_path)
    if unknown_disguised:
        raise ScraperError(
            "发现 exe/bin/dat 文件但魔数无法确认，未自动处理；请人工审核: "
            + ", ".join(unknown_disguised[:20])
        )
    candidates = sorted(
        [
            item for item in files
            if cleanup_reason(str(item.get("name", ""))) is None
            and not NON_MEDIA_ARCHIVE_CONTEXT_RE.search(
                normalize_remote_path(str(item.get("full_path") or "/"))
            )
            and (
                _archive_candidate(str(item.get("name", "")))
                or normalize_remote_path(str(item.get("full_path") or "/")) in disguised_formats
            )
        ],
        key=lambda item: _collision_key(str(item.get("full_path", ""))),
    )
    if not candidates and not media_renames:
        raise NoArchivesFound("未发现需要解压的视频或字幕压缩包")
    tree_passwords = discover_tree_passwords(alist, source_root)
    archives: list[dict[str, Any]] = []
    passwords: dict[str, str] = {}
    planned_destinations: set[tuple[str, str]] = set()
    for item in candidates:
        archive_path = normalize_remote_path(str(item["full_path"]))
        archive_dir, archive_name = split_remote(archive_path)
        password, password_source = _password_for_archive(
            alist, archive_dir, explicit_archive_password, tree_passwords
        )
        disguised_format = disguised_formats.get(archive_path)
        deferred_inspection = disguised_format is not None
        members: list[dict[str, Any]] = []
        if not deferred_inspection:
            meta = alist.archive_meta(archive_path, archive_password=password, refresh=True)
            members = _flatten_members(meta.get("content") or [])
        videos = [
            member
            for member in members
            if not member["is_dir"] and Path(str(member["path"])).suffix.lower() in VIDEO_EXTS
        ]
        subtitles = [
            member
            for member in members
            if not member["is_dir"]
            and Path(str(member["path"])).suffix.lower() in SUBTITLE_EXTS
        ]
        if (
            not deferred_inspection
            and not videos
            and not subtitles
            and not SUBTITLE_ARCHIVE_NAME_RE.search(archive_name)
        ):
            nested = [
                member
                for member in members
                if not member["is_dir"]
                and (
                    Path(str(member["path"])).suffix.lower() in {".exe", ".zip", ".rar", ".7z"}
                    or re.search(r"\.(?:zip|7z)\.\d{3}$|\.part\d+\.rar$", str(member["path"]), re.I)
                )
            ]
            if nested and sum(int(member.get("size") or 0) for member in nested) >= 1024 * 1024:
                raise ScraperError(
                    f"压缩包内只有无法直接整理的内层压缩文件: {archive_path}。"
                    "请先在网盘中解开内层 EXE/压缩包，再重新检查当前任务。"
                )
            print(f"忽略不含视频或字幕的说明性压缩包: {archive_path}", flush=True)
            continue
        direct_subtitle_members = _is_direct_subtitle_archive({"members": members})
        if not deferred_inspection:
            _check_destination_collisions(
                alist,
                archive_dir,
                members,
                allow_matching_files=direct_subtitle_members,
            )
        for member in members:
            if "/" in str(member["path"]):
                continue
            key = (_collision_key(archive_dir), _collision_key(str(member["path"])))
            if key in planned_destinations:
                raise ScraperError(f"多个归档会生成同一目标: {archive_dir}/{member['path']}")
            planned_destinations.add(key)
        parts = _part_entries(alist, archive_path)
        archives.append(
            {
                "archive_path": archive_path,
                "src_dir": archive_dir,
                "name": archive_name,
                "dst_dir": archive_dir,
                "parts": [_snapshot(part, archive_dir) for part in parts],
                "password_source": password_source,
                "members": members,
                "members_sha256": _members_digest(members),
                "video_count": len(videos),
                "video_bytes": sum(int(member["size"]) for member in videos),
                "cache_full": True,
                "put_into_new_dir": False,
                "detected_format": disguised_format,
                "deferred_inspection": deferred_inspection,
            }
        )
        passwords[archive_path] = password
    if not archives and not media_renames:
        raise NoArchivesFound("未发现需要解压的视频或字幕压缩包")
    plan = {
        "archive_plan_schema": ARCHIVE_PLAN_SCHEMA,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "source_root": source_root,
        "alist_version": server_version,
        "archives": archives,
        "media_renames": media_renames,
    }
    return plan, passwords


def _validate_loaded_plan(raw: Any) -> dict[str, Any]:
    if not isinstance(raw, dict):
        raise ScraperError("解压计划必须是 JSON 对象")
    allowed = {
        "archive_plan_schema", "created_at", "source_root", "alist_version", "archives",
        "media_renames",
    }
    unknown = sorted(set(raw) - allowed)
    if unknown:
        raise ScraperError("解压计划含未知字段: " + ", ".join(unknown))
    if raw.get("archive_plan_schema") != ARCHIVE_PLAN_SCHEMA:
        raise ScraperError("不支持的解压计划 schema")
    source_root = raw.get("source_root")
    if not isinstance(source_root, str) or normalize_remote_path(source_root) != source_root:
        raise ScraperError("解压计划 source_root 无效")
    archives = raw.get("archives")
    media_renames = raw.get("media_renames")
    if not isinstance(archives, list) or not isinstance(media_renames, list):
        raise ScraperError("解压计划 archives/media_renames 必须是数组")
    if not archives and not media_renames:
        raise ScraperError("解压计划不能为空")
    required = {
        "archive_path", "src_dir", "name", "dst_dir", "parts", "password_source",
        "members", "members_sha256", "video_count", "video_bytes", "cache_full",
        "put_into_new_dir", "detected_format", "deferred_inspection",
    }
    for index, archive in enumerate(archives, 1):
        if not isinstance(archive, dict) or set(archive) != required:
            raise ScraperError(f"第 {index} 个归档计划字段不完整或含未知字段")
        for key in ("archive_path", "src_dir", "dst_dir"):
            value = archive.get(key)
            if not isinstance(value, str) or normalize_remote_path(value) != value:
                raise ScraperError(f"第 {index} 个归档的 {key} 无效")
        try:
            _validate_remote_basename(archive.get("name"))
        except (TypeError, ValueError) as exc:
            raise ScraperError(f"第 {index} 个归档 name 无效") from exc
        members = archive.get("members")
        deferred = archive.get("deferred_inspection") is True
        detected_format = archive.get("detected_format")
        if deferred and detected_format not in {"zip", "7z", "rar"}:
            raise ScraperError(f"第 {index} 个伪装归档的检测格式无效")
        if not deferred and detected_format is not None:
            raise ScraperError(f"第 {index} 个普通归档不应含检测格式")
        if not isinstance(members, list) or (not members and not deferred):
            raise ScraperError(f"第 {index} 个归档 members 无效")
        digest = _members_digest(members)
        if archive.get("members_sha256") != digest:
            raise ScraperError(f"第 {index} 个归档成员摘要不匹配")
        if archive.get("cache_full") is not True or archive.get("put_into_new_dir") is not False:
            raise ScraperError("当前 schema 要求 cache_full=true 且 put_into_new_dir=false")
    rename_required = {
        "source_path", "src_dir", "name", "new_name", "detected_format", "snapshot",
    }
    for index, rename in enumerate(media_renames, 1):
        if not isinstance(rename, dict) or set(rename) != rename_required:
            raise ScraperError(f"第 {index} 个伪装媒体计划字段不完整或含未知字段")
        if rename.get("detected_format") not in {"mkv", "mp4"}:
            raise ScraperError(f"第 {index} 个伪装媒体格式无效")
        for key in ("source_path", "src_dir"):
            value = rename.get(key)
            if not isinstance(value, str) or normalize_remote_path(value) != value:
                raise ScraperError(f"第 {index} 个伪装媒体的 {key} 无效")
        try:
            _validate_remote_basename(rename.get("name"))
            _validate_remote_basename(rename.get("new_name"))
        except (TypeError, ValueError) as exc:
            raise ScraperError(f"第 {index} 个伪装媒体文件名无效") from exc
        if not isinstance(rename.get("snapshot"), Mapping):
            raise ScraperError(f"第 {index} 个伪装媒体快照无效")
    return raw


def _task_rows(alist: AListClient, kind: str) -> list[dict[str, Any]]:
    return alist.archive_tasks(kind, done=False) + alist.archive_tasks(kind, done=True)


def _task_id(task: Mapping[str, Any]) -> str:
    value = task.get("id")
    return str(value) if value is not None else ""


def wait_for_archive_tasks(
    alist: AListClient,
    baseline: Mapping[str, set[str]],
    initial_ids: set[str],
    *,
    timeout: float,
    journal: dict[str, Any],
    journal_path: Path,
    archive: Mapping[str, Any] | None = None,
) -> None:
    deadline = time.monotonic() + timeout
    seen: dict[str, dict[str, Any]] = {}
    quiet_success_polls = 0
    last_status = ""
    while time.monotonic() < deadline:
        active = False
        for kind in ("decompress", "decompress_upload"):
            for task in _task_rows(alist, kind):
                task_id = _task_id(task)
                if not task_id or task_id in baseline[kind]:
                    continue
                row = dict(task)
                row["kind"] = kind
                seen[task_id] = row
                if int(task.get("state", -1)) != 2:
                    active = True
        failed = [
            row for row in seen.values()
            if int(row.get("state", -1)) in {4, 7} or str(row.get("error") or "")
        ]
        if failed:
            journal["tasks"] = list(seen.values())
            journal["status"] = "failed"
            if archive is not None:
                _record_archive_member_checkpoint(
                    alist, archive, journal=journal, journal_path=journal_path,
                )
            _write_json_reserved(journal_path, journal)
            details = "; ".join(
                f"{row.get('kind')}:{row.get('id')} {row.get('error') or row.get('status')}"
                for row in failed
            )
            raise ScraperError(f"AList 解压任务失败: {details}")
        download_done = initial_ids and initial_ids.issubset(
            {task_id for task_id, row in seen.items() if int(row.get("state", -1)) == 2}
        )
        upload_rows = [row for row in seen.values() if row.get("kind") == "decompress_upload"]
        all_seen_succeeded = seen and all(int(row.get("state", -1)) == 2 for row in seen.values())
        summary = " | ".join(
            f"{row.get('kind')} {float(row.get('progress') or 0):.1f}% {row.get('status') or ''}"
            for row in seen.values()
            if int(row.get("state", -1)) != 2
        )
        if summary and summary != last_status:
            print(summary, flush=True)
            last_status = summary
        if download_done and upload_rows and all_seen_succeeded and not active:
            quiet_success_polls += 1
            if quiet_success_polls >= 2:
                journal["tasks"] = list(seen.values())
                journal["status"] = "tasks-succeeded"
                _write_json_reserved(journal_path, journal)
                return
        else:
            quiet_success_polls = 0
        journal["tasks"] = list(seen.values())
        journal["status"] = "running"
        if archive is not None:
            _record_archive_member_checkpoint(
                alist, archive, journal=journal, journal_path=journal_path,
            )
        _write_json_reserved(journal_path, journal)
        time.sleep(5)
    raise ScraperError(f"等待 AList 解压超时（{timeout:.0f} 秒）")


def _verify_part_snapshots(
    alist: AListClient,
    archive: Mapping[str, Any],
    *,
    allow_restored_modified_drift: bool = False,
) -> None:
    current = {
        str(entry.get("name")): entry
        for entry in alist.list(str(archive["src_dir"]), refresh=True)
        if not entry.get("is_dir")
    }
    for expected in archive["parts"]:
        name = str(expected["name"])
        actual = current.get(name)
        if actual is None:
            raise ScraperError(f"执行前分卷已消失: {expected['path']}")
        actual_snapshot = _snapshot(actual, str(archive["src_dir"]))
        for key in ("size", "modified", "hash"):
            if key == "modified" and allow_restored_modified_drift:
                continue
            if expected.get(key) is not None and actual_snapshot.get(key) != expected.get(key):
                raise ScraperError(f"执行前分卷快照已变化: {expected['path']}; {key}")


def _verify_single_snapshot(
    alist: AListClient,
    *,
    src_dir: str,
    name: str,
    expected: Mapping[str, Any],
) -> None:
    actual = next(
        (
            entry for entry in alist.list(src_dir, refresh=True)
            if not entry.get("is_dir")
            and _collision_key(str(entry.get("name", ""))) == _collision_key(name)
        ),
        None,
    )
    if actual is None:
        raise ScraperError(f"执行前文件已消失: {join_remote(src_dir, name)}")
    current = _snapshot(actual, src_dir)
    for key in ("size", "modified", "hash"):
        if expected.get(key) is not None and current.get(key) != expected.get(key):
            raise ScraperError(f"执行前文件快照已变化: {expected.get('path')}; {key}")


def _verify_extracted_members(alist: AListClient, archive: Mapping[str, Any]) -> None:
    expected = list(archive["members"])
    parent_paths = {
        split_remote(join_remote(str(archive["dst_dir"]), str(member["path"])))[0]
        for member in expected
    }
    last_error = ""
    for attempt in range(EXTRACT_VERIFY_ATTEMPTS):
        # Cloud providers can acknowledge an upload before their directory listing
        # is updated. Force-refresh every parent and allow that listing to settle.
        by_parent = {
            parent: alist.list(parent, refresh=True)
            for parent in parent_paths
        }
        last_error = ""
        for member in expected:
            full_path = join_remote(str(archive["dst_dir"]), str(member["path"]))
            parent, name = split_remote(full_path)
            matches = [
                entry for entry in by_parent[parent]
                if bool(entry.get("is_dir")) == bool(member["is_dir"])
                and _collision_key(str(entry.get("name", ""))) == _collision_key(name)
            ]
            if len(matches) != 1:
                last_error = f"解压后归档成员缺失或冲突: {full_path}"
                break
            if (
                not member["is_dir"]
                and int(matches[0].get("size") or 0) != int(member["size"])
            ):
                last_error = f"解压后归档成员大小不匹配: {full_path}"
                break
        if not last_error:
            return
        if attempt + 1 < EXTRACT_VERIFY_ATTEMPTS:
            time.sleep(EXTRACT_VERIFY_DELAY_SECONDS)
    raise ScraperError(last_error)


def _exact_remote_file_sha256(
    alist: AListClient,
    path: str,
    *,
    expected_size: int,
) -> str:
    adapter = AListExactFileAdapter(alist)
    before = adapter.stat_exact(path)
    if before is None:
        raise ScraperError(f"原生解压输出在精确路径不可见: {path}")
    if before.size != expected_size:
        raise ScraperError(
            f"原生解压输出大小不匹配: {path}; "
            f"expected={expected_size}, actual={before.size}"
        )
    digest = hashlib.sha256()
    size = 0
    try:
        with adapter.open_reader(path) as reader:
            while chunk := reader.read(1024 * 1024):
                size += len(chunk)
                if size > expected_size:
                    raise ScraperError(f"原生解压输出读取越界: {path}")
                digest.update(chunk)
    except ScraperError:
        raise
    except Exception as exc:
        raise ScraperError(f"无法完整回读原生解压输出: {path}") from exc
    if size != expected_size:
        raise ScraperError(
            f"原生解压输出回读不完整: {path}; "
            f"expected={expected_size}, actual={size}"
        )
    after = adapter.stat_exact(path)
    if after is None or after.size != before.size:
        raise ScraperError(f"原生解压输出在回读期间变化: {path}")
    if (
        before.version is not None
        and after.version is not None
        and before.version != after.version
    ):
        raise ScraperError(f"原生解压输出版本在回读期间变化: {path}")
    value = digest.hexdigest()
    if any(
        info.sha256 is not None and info.sha256 != value
        for info in (before, after)
    ):
        raise ScraperError(f"原生解压输出 provider 摘要与回读不一致: {path}")
    return value


def _native_archive_receipt_core(receipt: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": receipt.get("schema_version"),
        "plan_sha256": receipt.get("plan_sha256"),
        "archive_path": receipt.get("archive_path"),
        "dst_dir": receipt.get("dst_dir"),
        "members_sha256": receipt.get("members_sha256"),
        "outputs": receipt.get("outputs"),
        "task_ids": receipt.get("task_ids"),
        "verified_at": receipt.get("verified_at"),
    }


def _capture_native_archive_receipt(
    alist: AListClient,
    runtime_archive: Mapping[str, Any],
    *,
    archive_identity_path: str,
    plan_sha256: str,
    task_ids: set[str],
    journal: dict[str, Any],
    journal_path: Path,
) -> Mapping[str, Any]:
    _verify_extracted_members(alist, runtime_archive)
    expected = _expected_archive_file_members(runtime_archive)
    outputs = []
    for relative in sorted(expected):
        target = join_remote(str(runtime_archive["dst_dir"]), relative)
        size = int(expected[relative]["size"])
        outputs.append(
            {
                "path": target,
                "relative_path": relative,
                "size": size,
                "sha256": _exact_remote_file_sha256(
                    alist, target, expected_size=size
                ),
            }
        )
    receipt: dict[str, Any] = {
        "schema_version": 1,
        "plan_sha256": plan_sha256,
        "archive_path": normalize_remote_path(archive_identity_path),
        "dst_dir": normalize_remote_path(str(runtime_archive["dst_dir"])),
        "members_sha256": str(runtime_archive["members_sha256"]),
        "outputs": outputs,
        "task_ids": sorted(task_ids),
        "verified_at": datetime.now(timezone.utc).isoformat(),
    }
    receipt["receipt_sha256"] = hashlib.sha256(
        _canonical_json_bytes(_native_archive_receipt_core(receipt))
    ).hexdigest()
    receipts = journal.setdefault("native_archive_receipts", {})
    if not isinstance(receipts, dict):
        raise ScraperError("原生解压 receipt 容器格式无效")
    receipts[normalize_remote_path(archive_identity_path)] = receipt
    _write_json_reserved(journal_path, journal)
    return receipt


def _verify_native_archive_receipt(
    alist: AListClient,
    runtime_archive: Mapping[str, Any],
    *,
    archive_identity_path: str,
    plan_sha256: str,
    journal: Mapping[str, Any],
) -> bool:
    receipts = journal.get("native_archive_receipts")
    if receipts is None:
        return False
    if not isinstance(receipts, Mapping):
        raise ScraperError("原生解压 receipt 容器格式无效")
    key = normalize_remote_path(archive_identity_path)
    receipt = receipts.get(key)
    if receipt is None:
        return False
    if not isinstance(receipt, Mapping):
        raise ScraperError(f"原生解压 receipt 格式无效: {key}")
    receipt_sha256 = receipt.get("receipt_sha256")
    task_ids = receipt.get("task_ids")
    if (
        receipt.get("schema_version") != 1
        or receipt.get("plan_sha256") != plan_sha256
        or receipt.get("archive_path") != key
        or receipt.get("dst_dir") != runtime_archive.get("dst_dir")
        or receipt.get("members_sha256") != runtime_archive.get("members_sha256")
        or not isinstance(receipt_sha256, str)
        or not isinstance(receipt.get("verified_at"), str)
        or not isinstance(task_ids, list)
        or not task_ids
        or len(set(task_ids)) != len(task_ids)
        or not all(isinstance(task_id, str) and task_id for task_id in task_ids)
        or hashlib.sha256(
            _canonical_json_bytes(_native_archive_receipt_core(receipt))
        ).hexdigest()
        != receipt_sha256
    ):
        raise ScraperError(f"原生解压 receipt 身份不一致: {key}")
    expected = _expected_archive_file_members(runtime_archive)
    outputs = receipt.get("outputs")
    if not isinstance(outputs, list) or len(outputs) != len(expected):
        raise ScraperError(f"原生解压 receipt 输出清单不完整: {key}")
    seen: set[str] = set()
    for row in outputs:
        if not isinstance(row, Mapping):
            raise ScraperError(f"原生解压 receipt 输出格式无效: {key}")
        relative = str(row.get("relative_path") or "")
        expected_row = expected.get(relative)
        target = join_remote(str(runtime_archive["dst_dir"]), relative)
        if (
            expected_row is None
            or relative in seen
            or row.get("path") != target
            or row.get("size") != expected_row["size"]
            or not isinstance(row.get("sha256"), str)
            or re.fullmatch(r"[0-9a-f]{64}", str(row.get("sha256"))) is None
        ):
            raise ScraperError(f"原生解压 receipt 输出身份不一致: {key}")
        actual_sha256 = _exact_remote_file_sha256(
            alist, target, expected_size=int(expected_row["size"])
        )
        if actual_sha256 != row["sha256"]:
            raise ScraperError(f"原生解压 receipt 输出内容已变化: {target}")
        seen.add(relative)
    if seen != set(expected):
        raise ScraperError(f"原生解压 receipt 输出清单不完整: {key}")
    return True


def _native_task_success_evidence(
    journal: Mapping[str, Any],
    archive_identity_path: str,
) -> set[str] | None:
    if (
        journal.get("resumed_from_status") != "tasks-succeeded"
        or journal.get("active_archive")
        != normalize_remote_path(archive_identity_path)
    ):
        return None
    history = journal.get("task_history")
    if not isinstance(history, list) or not history:
        return None
    rows = history[-1]
    if not isinstance(rows, list) or not rows:
        return None
    task_ids: set[str] = set()
    for row in rows:
        if (
            not isinstance(row, Mapping)
            or int(row.get("state", -1)) != 2
            or not _task_id(row)
        ):
            return None
        task_ids.add(_task_id(row))
    return task_ids or None


def _reject_native_archive_reentry_conflicts(
    alist: AListClient,
    runtime_archive: Mapping[str, Any],
) -> None:
    conflicts = _live_verified_archive_members(alist, runtime_archive)
    if conflicts:
        first = sorted(conflicts)[0]
        raise ScraperError(
            "原生解压提交前目标已存在，但没有可验证的精确 receipt；"
            f"拒绝仅凭存在重入: {first}"
        )


def _archive_member_checkpoint_key(archive: Mapping[str, Any]) -> str:
    return normalize_remote_path(str(archive["archive_path"]))


def _expected_archive_file_members(
    archive: Mapping[str, Any],
) -> dict[str, dict[str, Any]]:
    expected: dict[str, dict[str, Any]] = {}
    for member in archive.get("members") or []:
        if not isinstance(member, Mapping) or member.get("is_dir"):
            continue
        relative = str(member.get("path") or "").replace("\\", "/").strip("/")
        if not relative:
            raise ScraperError(
                f"归档成员 checkpoint 路径无效: {archive.get('archive_path')}"
            )
        size = member.get("size")
        if isinstance(size, bool):
            raise ScraperError(f"归档成员 checkpoint 大小无效: {relative}")
        try:
            parsed_size = int(size)
        except (TypeError, ValueError) as exc:
            raise ScraperError(f"归档成员 checkpoint 大小无效: {relative}") from exc
        if parsed_size < 0 or relative in expected:
            raise ScraperError(f"归档成员 checkpoint 身份无效: {relative}")
        expected[relative] = {"path": relative, "size": parsed_size}
    if not expected:
        raise ScraperError(f"归档没有可 checkpoint 的文件: {archive.get('archive_path')}")
    return expected


def _live_verified_archive_members(
    alist: AListClient,
    archive: Mapping[str, Any],
) -> set[str]:
    """Return exact path+size members currently visible at the destination."""
    expected = _expected_archive_file_members(archive)
    by_parent: dict[str, list[dict[str, Any]]] = {}
    verified: set[str] = set()
    for relative, member in expected.items():
        target = join_remote(str(archive["dst_dir"]), relative)
        parent, name = split_remote(target)
        if parent not in by_parent:
            try_list = getattr(alist, "try_list", None)
            by_parent[parent] = (
                list(try_list(parent, refresh=True) or [])
                if callable(try_list)
                else alist.list(parent, refresh=True)
            )
        matches = [
            row for row in by_parent[parent]
            if not row.get("is_dir")
            and _collision_key(str(row.get("name") or "")) == _collision_key(name)
        ]
        if not matches:
            continue
        if len(matches) != 1 or int(matches[0].get("size") or -1) != member["size"]:
            raise ScraperError(f"归档成员 checkpoint 与到盘对象冲突: {target}")
        verified.add(relative)
    return verified


def _record_archive_member_checkpoint(
    alist: AListClient,
    archive: Mapping[str, Any],
    *,
    journal: dict[str, Any],
    journal_path: Path,
) -> set[str]:
    """Atomically persist member-level arrival evidence for one archive."""
    expected = _expected_archive_file_members(archive)
    verified = _live_verified_archive_members(alist, archive)
    checkpoints = journal.setdefault("archive_member_checkpoints", {})
    if not isinstance(checkpoints, dict):
        raise ScraperError("解压 journal 的成员 checkpoint 格式无效")
    key = _archive_member_checkpoint_key(archive)
    previous = checkpoints.get(key)
    if previous is not None and (
        not isinstance(previous, Mapping)
        or previous.get("members_sha256") != archive.get("members_sha256")
        or previous.get("dst_dir") != archive.get("dst_dir")
    ):
        raise ScraperError(f"解压 journal 的成员 checkpoint 身份不一致: {key}")
    checkpoints[key] = {
        "archive_path": key,
        "dst_dir": str(archive["dst_dir"]),
        "members_sha256": archive.get("members_sha256"),
        "expected": [expected[path] for path in sorted(expected)],
        "verified": [
            {**expected[path], "verified_at": datetime.now(timezone.utc).isoformat()}
            for path in sorted(verified)
        ],
        "remaining": sorted(set(expected) - verified),
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }
    _write_json_reserved(journal_path, journal)
    return verified


def _resume_archive_member_checkpoint(
    alist: AListClient,
    archive: Mapping[str, Any],
    *,
    journal: dict[str, Any],
    journal_path: Path,
) -> set[str] | None:
    """Validate a durable checkpoint; unrecorded existing members fail closed."""
    checkpoints = journal.get("archive_member_checkpoints")
    if not isinstance(checkpoints, Mapping):
        return None
    key = _archive_member_checkpoint_key(archive)
    checkpoint = checkpoints.get(key)
    if checkpoint is None:
        return None
    if (
        not isinstance(checkpoint, Mapping)
        or checkpoint.get("members_sha256") != archive.get("members_sha256")
        or checkpoint.get("dst_dir") != archive.get("dst_dir")
    ):
        raise ScraperError(f"解压 journal 的成员 checkpoint 身份不一致: {key}")
    verified_rows = checkpoint.get("verified")
    if not isinstance(verified_rows, list):
        raise ScraperError(f"解压 journal 的成员 checkpoint 格式无效: {key}")
    expected = _expected_archive_file_members(archive)
    recorded: set[str] = set()
    for row in verified_rows:
        if not isinstance(row, Mapping):
            raise ScraperError(f"解压 journal 的成员 checkpoint 格式无效: {key}")
        path = str(row.get("path") or "")
        expected_row = expected.get(path)
        if expected_row is None or path in recorded:
            raise ScraperError(f"解压 journal 的成员 checkpoint 身份不一致: {key}")
        size = row.get("size")
        if isinstance(size, bool):
            raise ScraperError(f"解压 journal 的成员 checkpoint 身份不一致: {key}")
        try:
            recorded_size = int(size)
        except (TypeError, ValueError) as exc:
            raise ScraperError(
                f"解压 journal 的成员 checkpoint 身份不一致: {key}"
            ) from exc
        if recorded_size != expected_row["size"]:
            raise ScraperError(f"解压 journal 的成员 checkpoint 身份不一致: {key}")
        recorded.add(path)
    live = _live_verified_archive_members(alist, archive)
    unexpected = live - recorded
    if unexpected:
        raise ScraperError(
            f"解压目标存在未登记的归档成员，拒绝续跑: {sorted(unexpected)[0]}"
        )
    # Rewrite the checkpoint after live revalidation so stale members are no
    # longer trusted if a provider removed them between processes.
    return _record_archive_member_checkpoint(
        alist, archive, journal=journal, journal_path=journal_path,
    )


def _is_direct_subtitle_archive(archive: Mapping[str, Any]) -> bool:
    files = [member for member in archive["members"] if not member["is_dir"]]
    subtitles = [
        member
        for member in files
        if Path(str(member["path"])).suffix.lower() in SUBTITLE_EXTS
    ]
    return bool(subtitles) and all(
        (
            Path(str(member["path"])).suffix.lower() in SUBTITLE_EXTS
            and int(member.get("size") or 0) <= DIRECT_SUBTITLE_MEMBER_LIMIT
        )
        or Path(str(member["path"])).suffix.lower() in FONT_FILE_EXTS
        for member in files
    )


def _members_digest(members: list[dict[str, Any]]) -> str:
    """Hash archive semantics while tolerating provider-only font mojibake."""
    digest_rows: list[dict[str, Any]] = members
    if _is_direct_subtitle_archive({"members": members}):
        digest_rows = []
        for member in members:
            path = str(member.get("path") or "")
            suffix = Path(path).suffix.lower()
            # Directory rows are implicit in the exact subtitle paths below.
            # Legacy filename decoding can invent or collapse font-directory
            # boundaries, so their standalone count is not stable evidence.
            if member.get("is_dir"):
                continue
            if suffix in SUBTITLE_EXTS:
                stable_path = _collision_key(path)
            else:
                stable_path = f"<discarded-font>{suffix}"
            digest_rows.append({
                "path": stable_path,
                "is_dir": False,
                "size": int(member.get("size") or 0),
            })
        digest_rows.sort(
            key=lambda row: (str(row["path"]), bool(row["is_dir"]), int(row["size"]))
        )
    return hashlib.sha256(_canonical_json_bytes(digest_rows)).hexdigest()


def _retain_only_subtitle_members(archive: Mapping[str, Any]) -> None:
    """Drop extracted font resources from post-upload verification.

    Font files bundled with ASS subtitles are renderer resources, not subtitle
    tracks.  The archive is still extracted and compared in full locally, but
    only subtitle files (and their ancestor directories) are uploaded.
    """
    raw_members = archive.get("members")
    if not isinstance(raw_members, list):
        return
    subtitle_paths = {
        str(member["path"]).replace("\\", "/")
        for member in raw_members
        if not member.get("is_dir")
        and Path(str(member.get("path") or "")).suffix.lower() in SUBTITLE_EXTS
    }
    needed_directories = {
        "/".join(path.split("/")[:index])
        for path in subtitle_paths
        for index in range(1, len(path.split("/")))
    }
    retained = [
        member
        for member in raw_members
        if (
            not member.get("is_dir")
            and str(member.get("path") or "").replace("\\", "/") in subtitle_paths
        )
        or (
            member.get("is_dir")
            and str(member.get("path") or "").replace("\\", "/") in needed_directories
        )
    ]
    if isinstance(archive, dict):
        archive["members"] = retained


def _ensure_remote_directory(alist: AListClient, path: str) -> None:
    path = normalize_remote_path(path)
    if path == "/":
        return
    parent, name = split_remote(path)
    _ensure_remote_directory(alist, parent)
    existing = alist.list(parent, refresh=True)
    matches = [
        entry for entry in existing
        if _collision_key(str(entry.get("name", ""))) == _collision_key(name)
    ]
    if matches:
        if len(matches) != 1 or not matches[0].get("is_dir"):
            raise ScraperError(f"解压目录与已有文件冲突: {path}")
        return
    alist.mkdir(path)


class ArchiveLocalUploadContext:
    __slots__ = ("transaction_root", "plan_sha256", "journal", "journal_path")

    def __init__(
        self,
        transaction_root: Path,
        plan_sha256: str,
        journal: dict[str, Any],
        journal_path: Path,
    ) -> None:
        self.transaction_root = transaction_root
        self.plan_sha256 = plan_sha256
        self.journal = journal
        self.journal_path = journal_path


def _require_archive_upload_context(
    context: ArchiveLocalUploadContext | None,
) -> ArchiveLocalUploadContext:
    if context is None:
        raise ScraperError("本机解压上传缺少持久事务上下文")
    return context


def _hash_local_upload_source(source: Path | bytes) -> tuple[int, str]:
    digest = hashlib.sha256()
    size = 0
    if isinstance(source, bytes):
        digest.update(source)
        return len(source), digest.hexdigest()
    try:
        before = source.stat()
        with source.open("rb") as handle:
            while chunk := handle.read(1024 * 1024):
                size += len(chunk)
                digest.update(chunk)
        after = source.stat()
    except OSError as exc:
        raise ScraperError(f"本机解压上传源不可读: {source}") from exc
    if (
        size != before.st_size
        or after.st_size != before.st_size
        or after.st_mtime_ns != before.st_mtime_ns
        or getattr(after, "st_ino", None) != getattr(before, "st_ino", None)
    ):
        raise ScraperError(f"本机解压上传源在读取期间变化: {source}")
    return size, digest.hexdigest()


def _materialize_archive_upload_payload(
    source: Path | bytes,
    destination: Path,
    *,
    expected_size: int,
    expected_sha256: str,
) -> None:
    destination.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    if destination.exists():
        actual_size, actual_sha256 = _hash_local_upload_source(destination)
        if actual_size != expected_size or actual_sha256 != expected_sha256:
            raise ScraperError(f"持久上传 payload 与当前解压结果冲突: {destination}")
        return
    partial = destination.with_name(
        f"{destination.name}.{uuid.uuid4().hex}.part"
    )
    try:
        descriptor = os.open(partial, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        try:
            with os.fdopen(descriptor, "wb") as output:
                if isinstance(source, bytes):
                    output.write(source)
                else:
                    with source.open("rb") as handle:
                        shutil.copyfileobj(handle, output, length=1024 * 1024)
                output.flush()
                os.fsync(output.fileno())
        except BaseException:
            try:
                os.close(descriptor)
            except OSError:
                pass
            raise
        actual_size, actual_sha256 = _hash_local_upload_source(partial)
        if actual_size != expected_size or actual_sha256 != expected_sha256:
            raise ScraperError("持久上传 payload 写入后身份不一致")
        os.replace(partial, destination)
        if os.name != "nt":
            directory_fd = os.open(
                destination.parent,
                os.O_RDONLY | getattr(os, "O_DIRECTORY", 0),
            )
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
    finally:
        try:
            partial.unlink()
        except FileNotFoundError:
            pass


def _execute_archive_local_upload(
    alist: AListClient,
    source: Path | bytes,
    target_path: str,
    content_type: str,
    *,
    context: ArchiveLocalUploadContext,
) -> Mapping[str, Any]:
    target_path = normalize_remote_path(target_path)
    expected_size, expected_sha256 = _hash_local_upload_source(source)
    payload_key = hashlib.sha256(
        f"{context.plan_sha256}\0{target_path}".encode(
            "utf-8", errors="surrogatepass"
        )
    ).hexdigest()
    suffix = Path(target_path).suffix.lower() or ".bin"
    payload_path = (
        context.transaction_root / "payloads" / f"{payload_key}{suffix}"
    ).resolve()
    _materialize_archive_upload_payload(
        source,
        payload_path,
        expected_size=expected_size,
        expected_sha256=expected_sha256,
    )
    transaction_id = deterministic_local_upload_id(payload_path, target_path)
    spec = LocalUploadSpec(
        transaction_id=transaction_id,
        source_path=payload_path,
        target_path=target_path,
        expected_size=expected_size,
        expected_sha256=expected_sha256,
        content_type=content_type,
    )
    receipts = context.journal.setdefault("local_upload_receipts", {})
    if not isinstance(receipts, dict):
        raise ScraperError("归档父 journal 的本机上传 receipt 格式无效")
    receipt_key = hashlib.sha256(
        f"{context.plan_sha256}\0{target_path}".encode(
            "utf-8", errors="surrogatepass"
        )
    ).hexdigest()
    existing_receipt = receipts.get(receipt_key)
    if existing_receipt is not None and (
        not isinstance(existing_receipt, Mapping)
        or existing_receipt.get("transaction_id") != transaction_id
        or existing_receipt.get("target_path") != target_path
        or existing_receipt.get("source_path") != str(payload_path)
    ):
        raise ScraperError(f"归档父 journal 的本机上传 receipt 身份冲突: {target_path}")

    def checkpoint(event: str, state: Mapping[str, Any]) -> None:
        receipts[receipt_key] = {
            "transaction_id": transaction_id,
            "source_path": str(payload_path),
            "target_path": target_path,
            "state": state.get("state"),
            "event": event,
            "size": state.get("size"),
            "sha256": state.get("sha256"),
            "upload_calls": state.get("upload_calls"),
            "upload_error": state.get("upload_error"),
            "transaction_journal": str(
                context.transaction_root / transaction_id / "journal.json"
            ),
            "updated_at": state.get("updated_at"),
        }
        _write_json_reserved(context.journal_path, context.journal)

    try:
        result = run_local_upload_transaction(
            AListExactFileAdapter(alist),
            transaction_root=context.transaction_root,
            spec=spec,
            checkpoint_hook=checkpoint,
        )
    except LocalUploadTransactionError as exc:
        row = receipts.get(receipt_key) or {}
        upload_error = str(row.get("upload_error") or "")
        if re.search(r"(?:invalid file|非法文件不能上传)", upload_error, re.I):
            raise ArchiveOutputRejected(
                f"存储提供方拒绝解压输出: {target_path}"
            ) from exc
        raise ScraperError(
            "本机解压上传事务未能证明安全完成，已保留 payload："
            f"{payload_path}; {exc}"
        ) from exc

    receipts[receipt_key] = {
        "transaction_id": transaction_id,
        "source_path": str(payload_path),
        "target_path": target_path,
        "state": result.state,
        "size": result.size,
        "sha256": result.sha256,
        "upload_calls": result.upload_calls_recorded,
        "transaction_journal": str(result.journal_path),
        "receipt_sha256": result.receipt_sha256,
        "bound_at": datetime.now(timezone.utc).isoformat(),
    }
    _write_json_reserved(context.journal_path, context.journal)
    try:
        payload_path.unlink()
    except FileNotFoundError:
        pass
    return receipts[receipt_key]


def _reject_archive_upload_aliases(
    alist: AListClient,
    target_path: str,
) -> None:
    parent, target_name = split_remote(target_path)
    collisions = [
        entry
        for entry in alist.list(parent, refresh=True)
        if _collision_key(str(entry.get("name", "")))
        == _collision_key(target_name)
    ]
    if len(collisions) > 1 or any(
        entry.get("is_dir")
        or str(entry.get("name", "")) != target_name
        for entry in collisions
    ):
        raise ScraperError(f"本机解压上传目标路径冲突: {target_path}")


def _looks_like_subtitle_payload(data: bytes, suffix: str) -> bool:
    """Reject empty/error responses before trusting AList's archive byte stream."""
    if not data:
        return False
    if suffix == ".sup":
        return data.startswith(b"PG")
    if suffix == ".sub" and b"\x00" in data[:4096]:
        return True
    try:
        text = data.decode("utf-8-sig")
    except UnicodeDecodeError:
        try:
            text = data.decode("utf-16")
        except UnicodeDecodeError:
            return suffix == ".sub"
    sample = text[:128 * 1024]
    lowered = sample.lower()
    if suffix in {".ass", ".ssa"}:
        return "[script info]" in lowered and "[events]" in lowered
    if suffix == ".srt":
        return re.search(r"\d{2}:\d{2}:\d{2}[,.]\d{3}\s*-->", sample) is not None
    if suffix == ".vtt":
        return sample.lstrip().startswith("WEBVTT")
    if suffix == ".idx":
        return "vobsub index file" in lowered
    if suffix == ".sub":
        return bool(sample.strip())
    return False


def _extract_subtitles_with_explicit_types(
    alist: AListClient,
    archive: Mapping[str, Any],
    *,
    archive_password: str,
    upload_context: ArchiveLocalUploadContext | None = None,
) -> None:
    """Extract subtitle members through AList's read endpoint, then upload typed bytes."""
    archive_metadata = alist.archive_meta(
        str(archive["archive_path"]),
        archive_password=archive_password,
        refresh=True,
    )
    for member in archive["members"]:
        if member["is_dir"]:
            _ensure_remote_directory(
                alist,
                join_remote(str(archive["dst_dir"]), str(member["path"])),
            )
            continue
        suffix = Path(str(member["path"])).suffix.lower()
        if suffix in FONT_FILE_EXTS:
            continue
        target = join_remote(str(archive["dst_dir"]), str(member["path"]))
        parent, _ = split_remote(target)
        _ensure_remote_directory(alist, parent)
        data = alist.archive_member_bytes(
            str(archive["archive_path"]),
            str(member["path"]),
            archive_password=archive_password,
            archive_metadata=archive_metadata,
            max_bytes=DIRECT_SUBTITLE_MEMBER_LIMIT,
        )
        if len(data) != int(member["size"]):
            candidates = [data]
            verified = None
            for _ in range(3):
                repeated = alist.archive_member_bytes(
                    str(archive["archive_path"]),
                    str(member["path"]),
                    archive_password=archive_password,
                    archive_metadata=archive_metadata,
                    max_bytes=DIRECT_SUBTITLE_MEMBER_LIMIT,
                )
                if any(repeated == candidate for candidate in candidates) and _looks_like_subtitle_payload(
                    repeated, suffix
                ):
                    verified = repeated
                    break
                candidates.append(repeated)
            if verified is None:
                raise ScraperError(f"归档字幕读取大小不匹配: {member['path']}")
            data = verified
            # AList v3 occasionally reports a stale uncompressed size for an
            # individual archive member. Two matching reads plus a format
            # check are stronger evidence than that advisory size, and the
            # runtime value is used by the post-upload verification below.
            member["size"] = len(data)
        _reject_archive_upload_aliases(alist, target)
        _execute_archive_local_upload(
            alist,
            data,
            target,
            SUBTITLE_MIME_TYPES[suffix],
            context=_require_archive_upload_context(upload_context),
        )
    _retain_only_subtitle_members(archive)


def _validate_local_extraction_budget(
    archive: Mapping[str, Any],
    temporary_root: Path,
    *,
    compressed_bytes: int,
) -> None:
    """Reject archive bombs before downloading or invoking 7-Zip.

    AList's refreshed member listing is treated as an advisory upper-bound
    input.  Exact output is still verified after extraction, but these checks
    prevent ordinary high-ratio/count/depth bombs from consuming the host
    workspace that backs ``TMPDIR``.
    """
    files = [member for member in archive.get("members") or [] if not member.get("is_dir")]
    if not files:
        raise ScraperError(f"归档没有可解压文件: {archive['archive_path']}")
    if len(files) > MAX_LOCAL_ARCHIVE_MEMBERS:
        raise ScraperError(
            f"归档文件数超过本地解压上限 {MAX_LOCAL_ARCHIVE_MEMBERS}: "
            f"{archive['archive_path']}"
        )
    total_output = 0
    for member in files:
        relative = str(member.get("path") or "").replace("\\", "/")
        depth = len([part for part in relative.split("/") if part])
        if depth > MAX_LOCAL_ARCHIVE_DEPTH:
            raise ScraperError(
                f"归档目录深度超过上限 {MAX_LOCAL_ARCHIVE_DEPTH}: {relative}"
            )
        size = member.get("size")
        if isinstance(size, bool):
            raise ScraperError(f"归档成员大小无效: {relative}")
        try:
            parsed_size = int(size)
        except (TypeError, ValueError) as exc:
            raise ScraperError(f"归档成员大小无效: {relative}") from exc
        if parsed_size < 0:
            raise ScraperError(f"归档成员大小无效: {relative}")
        total_output += parsed_size
    if compressed_bytes <= 0:
        raise ScraperError(f"归档压缩大小无效: {archive['archive_path']}")
    if total_output > compressed_bytes * MAX_LOCAL_EXPANSION_RATIO:
        raise ScraperError(
            f"归档展开比超过 {MAX_LOCAL_EXPANSION_RATIO}:1 安全上限: "
            f"{archive['archive_path']}"
        )
    free_bytes = shutil.disk_usage(temporary_root).free
    required = compressed_bytes + total_output + LOCAL_DISK_RESERVE_BYTES
    if required > free_bytes:
        raise ScraperError(
            f"本地解压空间不足: {archive['archive_path']}; "
            f"需要至少 {required} 字节，当前可用 {free_bytes} 字节"
        )


def _seven_zip_password_input(password: str) -> bytes | None:
    """Feed a prompted password over stdin so it never appears in argv/ps."""
    return f"{password}\n".encode("utf-8") if password else None


def _local_archive_listing(
    seven_zip: str,
    archive_path: Path,
    *,
    archive_password: str,
) -> tuple[str, list[dict[str, Any]]]:
    """Inspect an archive with local 7-Zip and return validated flat members."""
    password_input = _seven_zip_password_input(archive_password)
    run_input: dict[str, Any] = (
        {"input": password_input}
        if password_input is not None
        else {"stdin": subprocess.DEVNULL}
    )
    try:
        completed = subprocess.run(
            [
                seven_zip,
                "l",
                "-slt",
                "-sccUTF-8",
                str(archive_path),
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            timeout=120,
            check=False,
            **run_input,
        )
    except subprocess.TimeoutExpired as exc:
        raise ScraperError(f"归档链接安全检查超时: {archive_path.name}") from exc
    if completed.returncode != 0:
        raise ScraperError(f"无法读取本地归档目录: {archive_path.name}")
    listing = completed.stdout.decode("utf-8", errors="replace")
    _, separator, member_text = listing.partition("----------")
    if not separator:
        raise ScraperError(f"本地归档目录格式异常: {archive_path.name}")
    members: list[dict[str, Any]] = []
    keys: set[str] = set()
    for raw_block in re.split(r"(?:\r?\n){2,}", member_text.strip()):
        fields: dict[str, str] = {}
        for line in raw_block.splitlines():
            key, field_separator, raw_value = line.partition("=")
            if field_separator:
                fields[key.strip().casefold()] = raw_value.strip()
        raw_path = fields.get("path")
        if not raw_path:
            continue
        if fields.get("symbolic link") or fields.get("hard link"):
            raise ScraperError(f"归档中包含不允许的链接: {archive_path.name}")
        attributes = fields.get("attributes", "")
        unix_modes = [token for token in attributes.split() if len(token) >= 10]
        if any(mode[0].casefold() == "l" for mode in unix_modes):
            raise ScraperError(f"归档中包含不允许的符号链接: {archive_path.name}")
        path_parts = raw_path.replace("\\", "/").split("/")
        try:
            safe_parts = [_validate_remote_basename(part) for part in path_parts]
        except ValueError as exc:
            raise ScraperError(f"归档含不安全路径成员: {raw_path!r}; {exc}") from exc
        relative = "/".join(safe_parts)
        is_dir = fields.get("folder") == "+" or attributes.startswith("D")
        try:
            size = int(fields.get("size", "0"))
        except ValueError as exc:
            raise ScraperError(f"归档成员大小无效: {relative}") from exc
        if size < 0:
            raise ScraperError(f"归档成员大小无效: {relative}")
        collision_key = _collision_key(relative)
        if collision_key in keys:
            raise ScraperError(f"归档内存在大小写/Unicode 等价重复路径: {relative}")
        keys.add(collision_key)
        members.append({"path": relative, "is_dir": is_dir, "size": size})
    if not members:
        raise ScraperError(f"归档没有可解压文件: {archive_path.name}")
    members.sort(
        key=lambda member: (
            _collision_key(str(member["path"])),
            not bool(member["is_dir"]),
            int(member["size"]),
        )
    )
    return listing, members


def _reject_local_archive_links(
    seven_zip: str,
    archive_path: Path,
    *,
    archive_password: str = "",
) -> None:
    """Inspect 7-Zip metadata and reject link-like members before extraction."""
    _local_archive_listing(
        seven_zip,
        archive_path,
        archive_password=archive_password,
    )


def _extract_subtitles_locally(
    alist: AListClient,
    archive: Mapping[str, Any],
    *,
    archive_password: str,
    upload_context: ArchiveLocalUploadContext | None = None,
) -> bool:
    """Download and extract a subtitle archive once, then upload typed files.

    AList's member endpoint may decompress the whole archive for every member.
    Besides being slow, that can exhaust AList and trigger provider rate limits.
    Local extraction is used for unencrypted, fully inspected subtitle archives;
    encrypted or unsupported cases retain the existing safe AList fallback.
    """
    seven_zip = shutil.which("7z") or shutil.which("7zz")
    if seven_zip is None or archive_password or archive.get("deferred_inspection") is True:
        return False
    parts = list(archive.get("parts") or [])
    if not parts:
        raise ScraperError(f"归档缺少可下载的分卷: {archive['archive_path']}")
    total_size = sum(int(part.get("size") or 0) for part in parts)
    if total_size <= 0 or total_size > DIRECT_SUBTITLE_ARCHIVE_LIMIT:
        return False

    with tempfile.TemporaryDirectory(prefix="scrapeflow-subtitles-") as temporary:
        temporary_root = Path(temporary)
        _validate_local_extraction_budget(
            archive,
            temporary_root,
            compressed_bytes=total_size,
        )
        archive_root = temporary_root / "archive"
        output_root = temporary_root / "output"
        archive_root.mkdir()
        output_root.mkdir()
        local_parts: dict[str, Path] = {}
        for part in parts:
            name = _validate_remote_basename(str(part["name"]))
            expected_size = int(part.get("size") or 0)
            if expected_size <= 0:
                raise ScraperError(f"归档分卷大小无效: {part['path']}")
            data = alist.read_file_bytes(
                str(part["path"]), max_bytes=expected_size + 1
            )
            if len(data) != expected_size:
                raise ScraperError(f"归档分卷下载大小不匹配: {part['path']}")
            local_path = archive_root / name
            local_path.write_bytes(data)
            local_parts[name] = local_path

        first_name = _validate_remote_basename(str(archive["name"]))
        first_part = local_parts.get(first_name)
        if first_part is None:
            raise ScraperError(f"归档首卷未在下载列表中: {archive['archive_path']}")
        _reject_local_archive_links(seven_zip, first_part)
        try:
            completed = subprocess.run(
                [
                    seven_zip,
                    "x",
                    "-y",
                    "-bd",
                    "-bso0",
                    "-bsp0",
                    f"-o{output_root}",
                    str(first_part),
                ],
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                timeout=180,
                check=False,
            )
        except subprocess.TimeoutExpired as exc:
            raise ScraperError(f"本地解压超时: {archive['archive_path']}") from exc
        if completed.returncode != 0:
            raise ScraperError(f"本地解压失败: {archive['archive_path']}")

        expected_files = {
            str(member["path"]).replace("\\", "/")
            for member in archive["members"]
            if not member["is_dir"]
            and Path(str(member["path"])).suffix.lower() in SUBTITLE_EXTS
        }
        actual_files: set[str] = set()
        unexpected_files: set[str] = set()
        for local_path in output_root.rglob("*"):
            if local_path.is_symlink():
                raise ScraperError(f"归档中包含不允许的链接: {local_path.name}")
            if local_path.is_file():
                relative = local_path.relative_to(output_root).as_posix()
                suffix = local_path.suffix.lower()
                if suffix in SUBTITLE_EXTS:
                    actual_files.add(relative)
                elif suffix not in FONT_FILE_EXTS:
                    unexpected_files.add(relative)
        if actual_files != expected_files or unexpected_files:
            raise ScraperError(f"本地解压结果与已审核归档目录不一致: {archive['archive_path']}")

        for member in archive["members"]:
            if member["is_dir"]:
                continue
            relative = str(member["path"]).replace("\\", "/")
            if Path(relative).suffix.lower() in FONT_FILE_EXTS:
                continue
            target = join_remote(str(archive["dst_dir"]), relative)
            parent, target_name = split_remote(target)
            _ensure_remote_directory(alist, parent)
            suffix = Path(relative).suffix.lower()
            data = (output_root / relative).read_bytes()
            if len(data) > DIRECT_SUBTITLE_MEMBER_LIMIT or not _looks_like_subtitle_payload(
                data, suffix
            ):
                raise ScraperError(f"本地解出的字幕格式无效: {relative}")
            # The local extractor is authoritative when AList's advisory member
            # size is stale; post-upload verification uses this actual size.
            member["size"] = len(data)
            _reject_archive_upload_aliases(alist, target)
            _execute_archive_local_upload(
                alist,
                output_root / relative,
                target,
                SUBTITLE_MIME_TYPES[suffix],
                context=_require_archive_upload_context(upload_context),
            )
        _retain_only_subtitle_members(archive)
        return True


def _extract_deferred_media_locally(
    alist: AListClient,
    archive: dict[str, Any],
    *,
    archive_password: str,
    upload_context: ArchiveLocalUploadContext | None = None,
    resume_checkpoint: bool = False,
) -> bool:
    """Use 7-Zip for disguised media or member-checkpoint recovery.

    A resumed native AList extraction cannot safely resubmit the whole archive:
    its task IDs disappear across an AList restart while already uploaded
    members remain.  The recovery path extracts once locally and uploads only
    the members that are still absent, preserving verified remote members.
    """
    seven_zip = shutil.which("7z") or shutil.which("7zz")
    if (
        seven_zip is None
        or (
            not resume_checkpoint
            and (
                archive.get("deferred_inspection") is not True
                or (
                    archive_password
                    and archive.get("_local_fallback_required") is not True
                )
            )
        )
    ):
        return False
    parts = list(archive.get("parts") or [])
    if not parts:
        raise ScraperError(f"归档缺少可下载的分卷: {archive['archive_path']}")
    total_size = sum(int(part.get("size") or 0) for part in parts)
    if total_size <= 0:
        raise ScraperError(f"归档分卷总大小无效: {archive['archive_path']}")

    with tempfile.TemporaryDirectory(prefix="scrapeflow-media-") as temporary:
        temporary_root = Path(temporary)
        if total_size + LOCAL_DISK_RESERVE_BYTES > shutil.disk_usage(temporary_root).free:
            raise ScraperError(f"本地解压空间不足: {archive['archive_path']}")
        archive_root = temporary_root / "archive"
        output_root = temporary_root / "output"
        archive_root.mkdir()
        output_root.mkdir()
        local_parts: dict[str, Path] = {}
        for part in parts:
            name = _validate_remote_basename(str(part["name"]))
            expected_size = int(part.get("size") or 0)
            if expected_size <= 0:
                raise ScraperError(f"归档分卷大小无效: {part['path']}")
            local_path = archive_root / name
            remote_part_path = (
                str(archive["archive_path"])
                if len(parts) == 1
                else str(part["path"])
            )
            alist.download_file_to_path(
                remote_part_path,
                local_path,
                expected_size=expected_size,
            )
            local_parts[name] = local_path

        first_name = _validate_remote_basename(str(archive["name"]))
        first_part = local_parts.get(first_name)
        if first_part is None:
            # Deferred archives are temporarily renamed to a real extension,
            # while their immutable part snapshot still carries the original
            # disguised filename.
            if len(local_parts) == 1:
                first_part = next(iter(local_parts.values()))
            else:
                raise ScraperError(f"归档首卷未在下载列表中: {archive['archive_path']}")
        _, inspected_members = _local_archive_listing(
            seven_zip,
            first_part,
            archive_password=archive_password,
        )
        if archive.get("members"):
            if _members_digest(inspected_members) != _members_digest(archive["members"]):
                raise ScraperError(
                    f"本地归档目录与云端审核结果不一致: {archive['archive_path']}"
                )
        else:
            archive["members"] = inspected_members
            archive["members_sha256"] = _members_digest(inspected_members)
        if not any(
            not member["is_dir"]
            and Path(str(member["path"])).suffix.lower() in VIDEO_EXTS | SUBTITLE_EXTS
            for member in archive["members"]
        ):
            raise ScraperError(f"伪装压缩包中没有可整理的视频或字幕: {archive['archive_path']}")
        _validate_local_extraction_budget(
            archive,
            temporary_root,
            compressed_bytes=total_size,
        )
        _check_destination_collisions(
            alist,
            str(archive["dst_dir"]),
            archive["members"],
            allow_matching_files=True,
        )
        password_input = _seven_zip_password_input(archive_password)
        run_input: dict[str, Any] = (
            {"input": password_input}
            if password_input is not None
            else {"stdin": subprocess.DEVNULL}
        )
        try:
            completed = subprocess.run(
                [
                    seven_zip,
                    "x",
                    "-y",
                    "-bd",
                    "-bso0",
                    "-bsp0",
                    f"-o{output_root}",
                    str(first_part),
                ],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                timeout=3600,
                check=False,
                **run_input,
            )
        except subprocess.TimeoutExpired as exc:
            raise ScraperError(f"本地媒体解压超时: {archive['archive_path']}") from exc
        if completed.returncode != 0:
            raise ScraperError(f"本地媒体解压失败: {archive['archive_path']}")

        expected_files = {
            str(member["path"]).replace("\\", "/")
            for member in archive["members"]
            if not member["is_dir"]
        }
        actual_files: set[str] = set()
        for local_path in output_root.rglob("*"):
            if local_path.is_symlink():
                raise ScraperError(f"归档中包含不允许的链接: {local_path.name}")
            if local_path.is_file():
                actual_files.add(local_path.relative_to(output_root).as_posix())
        if actual_files != expected_files:
            raise ScraperError(
                f"本地媒体解压结果与已审核归档目录不一致: {archive['archive_path']}"
            )

        for member in archive["members"]:
            relative = str(member["path"]).replace("\\", "/")
            target = join_remote(str(archive["dst_dir"]), relative)
            if member["is_dir"]:
                _ensure_remote_directory(alist, target)
                continue
            local_path = output_root / relative
            actual_size = local_path.stat().st_size
            expected_size = int(member.get("size") or 0)
            if expected_size > 0 and actual_size != expected_size:
                raise ScraperError(f"本地媒体解压大小不匹配: {relative}")
            member["size"] = actual_size
            parent, target_name = split_remote(target)
            _ensure_remote_directory(alist, parent)
            _reject_archive_upload_aliases(alist, target)
            suffix = Path(relative).suffix.lower()
            _execute_archive_local_upload(
                alist,
                local_path,
                target,
                SUBTITLE_MIME_TYPES.get(suffix, "application/octet-stream"),
                context=_require_archive_upload_context(upload_context),
            )
        return True


def _retained_archive_paths(archive: Mapping[str, Any]) -> list[str]:
    """Record retained source parts without mutating the remote library."""
    names = [str(part["name"]) for part in archive["parts"]]
    if not names:
        raise ScraperError(f"归档缺少源分卷: {archive['archive_path']}")
    return [join_remote(str(archive["src_dir"]), name) for name in names]


def _archive_transaction_stage_root(journal_path: Path) -> Path:
    """Keep every archive file transaction beside its owning execution journal."""
    return journal_path.parent / ".remote-file-transactions"


def _archive_local_upload_transaction_root(journal_path: Path) -> Path:
    return journal_path.parent / ".local-upload-transactions"


def _archive_transaction_id(
    plan_sha256: str,
    source_path: str,
    target_path: str,
) -> str:
    identity = f"{plan_sha256}\0{source_path}\0{target_path}".encode(
        "utf-8", errors="surrogatepass"
    )
    return "archive-" + hashlib.sha256(identity).hexdigest()[:48]


def _archive_transaction_journal_path(
    journal_path: Path,
    plan_sha256: str,
    source_path: str,
    target_path: str,
) -> Path:
    return (
        _archive_transaction_stage_root(journal_path)
        / _archive_transaction_id(plan_sha256, source_path, target_path)
        / "journal.json"
    )


def _archive_temporary_name(
    plan_sha256: str,
    source_path: str,
    detected_format: str,
) -> str:
    identity = f"{plan_sha256}\0archive-temporary\0{source_path}".encode(
        "utf-8", errors="surrogatepass"
    )
    token = hashlib.sha256(identity).hexdigest()[:32]
    return f".scraper-tmp-{token}.{detected_format}"


def _execute_archive_file_transaction(
    alist: AListClient,
    *,
    plan_sha256: str,
    source_path: str,
    target_path: str,
    expected_size: int,
    journal: dict[str, Any],
    journal_path: Path,
) -> None:
    """Transfer one remote file without MOVE, retry uploads, or blind deletion."""
    stage_root = _archive_transaction_stage_root(journal_path)
    transaction_id = _archive_transaction_id(
        plan_sha256, source_path, target_path
    )
    spec = RemoteFileTransferSpec(
        transaction_id=transaction_id,
        source_path=normalize_remote_path(source_path),
        target_path=normalize_remote_path(target_path),
        expected_size=expected_size,
    )
    adapter = AListExactFileAdapter(alist)

    def checkpoint(event: str, state: Mapping[str, Any]) -> None:
        transactions = journal.setdefault("remote_file_transactions", {})
        transactions[transaction_id] = {
            "transaction_id": transaction_id,
            "source": spec.source_path,
            "target": spec.target_path,
            "state": state.get("state"),
            "event": event,
            "size": state.get("size"),
            "sha256": state.get("sha256"),
            "upload_calls": state.get("upload_calls"),
            "source_deleted": state.get("source_deleted"),
            "stage_directory": str(stage_root / transaction_id),
            "updated_at": state.get("updated_at"),
        }
        _write_json_reserved(journal_path, journal)

    try:
        # The prepare checkpoint guarantees that the complete source and its
        # SHA-256 are durable locally before the first remote write is possible.
        prepare_remote_file_transaction(
            adapter,
            stage_root=stage_root,
            spec=spec,
            checkpoint_hook=checkpoint,
        )
        run_remote_file_transaction(
            adapter,
            stage_root=stage_root,
            spec=spec,
            checkpoint_hook=checkpoint,
        )
        discard_completed_remote_file_transaction(stage_root=stage_root, spec=spec)
    except RemoteFileTransactionError as exc:
        raise ScraperError(
            "归档文件事务未能证明安全完成，已保留本机 payload："
            f"{source_path} -> {target_path}; "
            f"stage={stage_root / transaction_id}; {exc}"
        ) from exc

    transaction = journal.setdefault("remote_file_transactions", {}).setdefault(
        transaction_id, {}
    )
    transaction["payload_retained"] = False
    _write_json_reserved(journal_path, journal)


def _archive_source_size(archive: Mapping[str, Any], source_path: str) -> int:
    normalized = normalize_remote_path(source_path)
    for part in archive.get("parts") or []:
        if normalize_remote_path(str(part.get("path") or "")) == normalized:
            size = part.get("size")
            if isinstance(size, bool) or not isinstance(size, int) or size < 0:
                break
            return size
    raise ScraperError(f"归档计划缺少有效源文件大小: {normalized}")


def execute_archive_plan(
    alist: AListClient,
    plan: dict[str, Any],
    passwords: Mapping[str, str],
    *,
    timeout: float,
    journal_path: Path,
) -> None:
    require_safe_archive_server(alist)
    plan_sha256 = hashlib.sha256(_canonical_json_bytes(plan)).hexdigest()
    if journal_path.exists():
        try:
            loaded_journal = _load_json_text(journal_path.read_text(encoding="utf-8"))
        except (OSError, ValueError) as exc:
            raise ScraperError(f"无法读取解压断点记录: {exc}") from exc
        interrupted_status = (
            loaded_journal.get("status") if isinstance(loaded_journal, dict) else None
        )
        if (
            not isinstance(loaded_journal, dict)
            or interrupted_status not in {"failed", "running", "tasks-succeeded"}
            or loaded_journal.get("plan_sha256") != plan_sha256
        ):
            raise ScraperError("现有解压 journal 不满足安全断点续跑条件")
        if interrupted_status in {"running", "tasks-succeeded"}:
            prior_ids = {
                _task_id(row)
                for row in loaded_journal.get("tasks") or []
                if isinstance(row, Mapping) and _task_id(row)
            }
            current_ids = {
                _task_id(row)
                for kind in ("decompress", "decompress_upload")
                for row in _task_rows(alist, kind)
                if _task_id(row)
            }
            if prior_ids & current_ids:
                raise ScraperError("原 AList 解压任务仍可跟踪，拒绝并发续跑")
        journal = loaded_journal
        previous_tasks = journal.get("tasks") or []
        if previous_tasks:
            journal.setdefault("task_history", []).append(previous_tasks)
        journal["tasks"] = []
        journal["locks"] = []
        journal["status"] = "preflight"
        journal["resumed_at"] = datetime.now(timezone.utc).isoformat()
        journal["resumed_from_status"] = interrupted_status
        journal.pop("error", None)
        journal.pop("temporary_restore_error", None)
    else:
        _reserve_output_path(journal_path)
        journal = {
            "created_at": datetime.now(timezone.utc).isoformat(),
            "plan_sha256": plan_sha256,
            "status": "preflight",
            "locks": [],
            "tasks": [],
            "renames": [],
            # Kept for journal compatibility with earlier attempts.  New runs do
            # not remove source archives automatically.
            "archive_removals": [],
            "retained_archives": [],
            "archive_member_checkpoints": {},
            "remote_file_transactions": {},
            "local_upload_receipts": {},
            "native_archive_receipts": {},
        }
    retained_paths = {
        normalize_remote_path(path)
        for path in journal.get("retained_archives") or []
        if isinstance(path, str)
    }
    completed_renames = {
        str(row.get("source"))
        for row in journal.get("renames") or []
        if isinstance(row, Mapping) and row.get("status") == "success"
    }
    restored_temporary_sources = {
        str(row.get("source"))
        for row in journal.get("temporary_renames") or []
        if isinstance(row, Mapping) and row.get("status") == "restored"
    }
    journal.setdefault("remote_file_transactions", {})
    journal.setdefault("local_upload_receipts", {})
    journal.setdefault("native_archive_receipts", {})
    upload_context = ArchiveLocalUploadContext(
        transaction_root=_archive_local_upload_transaction_root(journal_path),
        plan_sha256=plan_sha256,
        journal=journal,
        journal_path=journal_path,
    )

    def pending_temporary_record(source_path: str) -> dict[str, Any] | None:
        matches = [
            row
            for row in journal.get("temporary_renames") or []
            if isinstance(row, dict)
            and row.get("source") == source_path
            and row.get("status") == "pending"
        ]
        if len(matches) > 1:
            raise ScraperError(f"归档存在多个未完成临时文件事务: {source_path}")
        return matches[0] if matches else None

    blocked_archive_sources = {
        str(row.get("archive_path"))
        for row in journal.get("blocked_archives") or []
        if isinstance(row, Mapping) and row.get("reason") == "provider-rejected-output"
    }
    local_fallback_sources = {
        str(row.get("source"))
        for row in journal.get("temporary_renames") or []
        if isinstance(row, Mapping)
        and row.get("status") == "restored"
        and row.get("metadata_status") == "failed"
    }
    fallback_directory_counts: dict[str, int] = {}
    for source in local_fallback_sources:
        directory, _ = split_remote(source)
        fallback_directory_counts[directory] = fallback_directory_counts.get(directory, 0) + 1
    local_fallback_directories = {
        directory
        for directory, count in fallback_directory_counts.items()
        if count >= 2
    }

    def mark_temporary_restored(record: dict[str, Any]) -> None:
        record["status"] = "restored"
        record["restored_at"] = datetime.now(timezone.utc).isoformat()
        restored_temporary_sources.add(str(record["source"]))
        _write_json_reserved(journal_path, journal)

    def restore_temporary_archive(
        record: dict[str, Any],
        temporary_path: str,
        original_path: str,
        expected_size: int,
    ) -> None:
        _execute_archive_file_transaction(
            alist,
            plan_sha256=plan_sha256,
            source_path=temporary_path,
            target_path=original_path,
            expected_size=expected_size,
            journal=journal,
            journal_path=journal_path,
        )
        mark_temporary_restored(record)

    def archive_completed(archive: Mapping[str, Any]) -> bool:
        expected = {
            normalize_remote_path(path) for path in _retained_archive_paths(archive)
        }
        return bool(expected) and expected.issubset(retained_paths)

    _write_json_reserved(journal_path, journal)
    lock_nonce = uuid.uuid4().hex
    lock_name = f".scraper-lock-archive-{lock_nonce}.json"
    lock_path = join_remote(str(plan["source_root"]), lock_name)
    lock_held = False
    active_temporary: tuple[str, str, int, dict[str, Any]] | None = None
    try:
        for rename in plan.get("media_renames") or []:
            if str(rename["source_path"]) in completed_renames:
                continue
            target_path = join_remote(
                str(rename["src_dir"]), str(rename["new_name"])
            )
            transaction_started = _archive_transaction_journal_path(
                journal_path,
                plan_sha256,
                str(rename["source_path"]),
                target_path,
            ).is_file()
            if not transaction_started:
                _verify_single_snapshot(
                    alist,
                    src_dir=str(rename["src_dir"]),
                    name=str(rename["name"]),
                    expected=rename["snapshot"],
                )
            existing = alist.list(str(rename["src_dir"]), refresh=True)
            collisions = [
                entry
                for entry in existing
                if _collision_key(str(entry.get("name", "")))
                == _collision_key(str(rename["new_name"]))
            ]
            # An exact target can be the result of a lost upload response.  The
            # transaction will adopt it only after a complete SHA-256 read-back.
            # Case/Unicode aliases and directories remain ambiguous.
            if len(collisions) > 1 or any(
                entry.get("is_dir")
                or str(entry.get("name", "")) != str(rename["new_name"])
                for entry in collisions
            ):
                raise ScraperError(
                    f"执行前伪装媒体的目标路径存在冲突: {target_path}"
                )
        for archive in plan["archives"]:
            if (
                archive_completed(archive)
                or str(archive["archive_path"]) in blocked_archive_sources
            ):
                continue
            archive_source = str(archive["archive_path"])
            if (
                archive_source in restored_temporary_sources
                and archive.get("deferred_inspection") is True
            ):
                prior_temporary_path = join_remote(
                    str(archive["src_dir"]),
                    _archive_temporary_name(
                        plan_sha256,
                        archive_source,
                        str(archive["detected_format"]),
                    ),
                )
                if _archive_transaction_journal_path(
                    journal_path,
                    plan_sha256,
                    archive_source,
                    prior_temporary_path,
                ).is_file():
                    raise ScraperError(
                        "伪装归档已通过文件事务恢复原名；"
                        f"拒绝在同一计划内重放已完成的转移: {archive_source}。"
                        "请重新生成并审核解压计划。"
                    )
            pending_record = pending_temporary_record(archive_source)
            pending_transaction_started = False
            if pending_record is not None:
                expected_temporary = join_remote(
                    str(archive["src_dir"]),
                    _archive_temporary_name(
                        plan_sha256,
                        archive_source,
                        str(archive["detected_format"]),
                    ),
                )
                if pending_record.get("temporary") != expected_temporary:
                    raise ScraperError(
                        f"归档临时文件断点路径不匹配: {archive_source}"
                    )
                pending_transaction_started = _archive_transaction_journal_path(
                    journal_path,
                    plan_sha256,
                    archive_source,
                    expected_temporary,
                ).is_file()
            if not pending_transaction_started:
                _verify_part_snapshots(
                    alist,
                    archive,
                    allow_restored_modified_drift=(
                        archive_source in restored_temporary_sources
                    ),
                )
            if archive.get("deferred_inspection") is True:
                continue
            password = passwords[str(archive["archive_path"])]
            meta = alist.archive_meta(
                str(archive["archive_path"]), archive_password=password, refresh=True
            )
            members = _flatten_members(meta.get("content") or [])
            digest = _members_digest(members)
            if digest != archive["members_sha256"]:
                raise ScraperError(f"执行前归档成员已变化: {archive['archive_path']}")
            native_receipt_verified = _verify_native_archive_receipt(
                alist,
                archive,
                archive_identity_path=str(archive["archive_path"]),
                plan_sha256=plan_sha256,
                journal=journal,
            )
            checkpoint_verified = (
                _record_archive_member_checkpoint(
                    alist,
                    archive,
                    journal=journal,
                    journal_path=journal_path,
                )
                if native_receipt_verified
                else _resume_archive_member_checkpoint(
                    alist,
                    archive,
                    journal=journal,
                    journal_path=journal_path,
                )
            )
            _check_destination_collisions(
                alist,
                str(archive["dst_dir"]),
                members,
                allow_matching_files=(
                    _is_direct_subtitle_archive(archive)
                    or checkpoint_verified is not None
                    or native_receipt_verified
                ),
            )
        lock_payload = json.dumps(
            {
                "kind": "archive-extraction",
                "created_at": datetime.now(timezone.utc).isoformat(),
                "plan_sha256": journal["plan_sha256"],
                "nonce": lock_nonce,
            },
            ensure_ascii=False,
        ).encode("utf-8")
        alist.upload_bytes(lock_path, lock_payload, "application/json")
        lock_held = True
        locks = [
            entry for entry in alist.list(str(plan["source_root"]), refresh=True)
            if isinstance(entry.get("name"), str) and is_scraper_lock(str(entry["name"]))
        ]
        if len(locks) != 1 or _collision_key(str(locks[0]["name"])) != _collision_key(lock_name):
            raise ScraperError("归档解压锁竞争：源目录存在其他整理任务")
        lock_readback = alist.read_file_bytes(
            lock_path, max_bytes=len(lock_payload) + 1
        )
        if lock_readback != lock_payload:
            raise ScraperError("归档解压锁 nonce 或内容回读不一致")
        journal["locks"] = [lock_path]
        _write_json_reserved(journal_path, journal)

        for rename in plan.get("media_renames") or []:
            if str(rename["source_path"]) in completed_renames:
                continue
            target_path = join_remote(
                str(rename["src_dir"]), str(rename["new_name"])
            )
            _execute_archive_file_transaction(
                alist,
                plan_sha256=plan_sha256,
                source_path=str(rename["source_path"]),
                target_path=target_path,
                expected_size=int(rename["snapshot"]["size"]),
                journal=journal,
                journal_path=journal_path,
            )
            journal["renames"].append(
                {
                    "source": rename["source_path"],
                    "target": target_path,
                    "status": "success",
                }
            )
            _write_json_reserved(journal_path, journal)

        for archive in plan["archives"]:
            if archive_completed(archive):
                print(f"断点续跑跳过已验证归档：{archive['name']}", flush=True)
                continue
            if str(archive["archive_path"]) in blocked_archive_sources:
                print(
                    f"断点续跑跳过已记录的 provider 拒绝项：{archive['name']}",
                    flush=True,
                )
                continue
            runtime_archive = dict(archive)
            original_archive_path = str(archive["archive_path"])
            temporary_name: str | None = None
            temporary_record: dict[str, Any] | None = None
            if archive.get("deferred_inspection") is True:
                detected_format = str(archive["detected_format"])
                temporary_name = _archive_temporary_name(
                    plan_sha256, original_archive_path, detected_format
                )
                temporary_path = join_remote(
                    str(archive["src_dir"]), temporary_name
                )
                temporary_record = pending_temporary_record(original_archive_path)
                if temporary_record is None:
                    temporary_record = {
                        "source": original_archive_path,
                        "temporary": temporary_path,
                        "status": "pending",
                        "started_at": datetime.now(timezone.utc).isoformat(),
                    }
                    journal.setdefault("temporary_renames", []).append(
                        temporary_record
                    )
                    _write_json_reserved(journal_path, journal)
                elif temporary_record.get("temporary") != temporary_path:
                    raise ScraperError(
                        f"归档临时文件断点路径不匹配: {original_archive_path}"
                    )
                archive_source_size = _archive_source_size(
                    archive, original_archive_path
                )
                if _archive_transaction_journal_path(
                    journal_path,
                    plan_sha256,
                    temporary_path,
                    original_archive_path,
                ).is_file():
                    restore_temporary_archive(
                        temporary_record,
                        temporary_path,
                        original_archive_path,
                        archive_source_size,
                    )
                    raise ScraperError(
                        "已安全续跑并完成上次的归档原名恢复；"
                        f"为防止重放已完成的转移，请重新生成计划: "
                        f"{original_archive_path}"
                    )
                _execute_archive_file_transaction(
                    alist,
                    plan_sha256=plan_sha256,
                    source_path=original_archive_path,
                    target_path=temporary_path,
                    expected_size=archive_source_size,
                    journal=journal,
                    journal_path=journal_path,
                )
                runtime_archive["name"] = temporary_name
                runtime_archive["archive_path"] = temporary_path
                active_temporary = (
                    temporary_path,
                    original_archive_path,
                    archive_source_size,
                    temporary_record,
                )
                metadata_error: BaseException | None = None
                if (
                    original_archive_path in local_fallback_sources
                    or str(archive["src_dir"]) in local_fallback_directories
                ):
                    metadata_error = ScraperError(
                        "断点记录已确认云端归档元数据不兼容"
                    )
                    runtime_archive["members"] = []
                    runtime_archive["_local_fallback_required"] = True
                try:
                    if metadata_error is not None:
                        raise metadata_error
                    meta = alist.archive_meta(
                        str(runtime_archive["archive_path"]),
                        archive_password=passwords[original_archive_path],
                        refresh=True,
                    )
                    members = _flatten_members(meta.get("content") or [])
                    if not any(
                        not member["is_dir"]
                        and Path(str(member["path"])).suffix.lower()
                        in VIDEO_EXTS | SUBTITLE_EXTS
                        for member in members
                    ):
                        raise ScraperError(
                            f"伪装压缩包中没有可整理的视频或字幕: {original_archive_path}"
                        )
                    runtime_archive["members"] = members
                    runtime_video_count = sum(
                        1
                        for member in members
                        if not member["is_dir"]
                        and Path(str(member["path"])).suffix.lower() in VIDEO_EXTS
                    )
                    runtime_subtitle_count = sum(
                        1
                        for member in members
                        if not member["is_dir"]
                        and Path(str(member["path"])).suffix.lower() in SUBTITLE_EXTS
                    )
                    print(
                        "伪装归档运行时成员分类："
                        f"{archive['name']} | 视频 {runtime_video_count} | "
                        f"字幕 {runtime_subtitle_count} | 总成员 "
                        f"{sum(1 for member in members if not member['is_dir'])}",
                        flush=True,
                    )
                    _check_destination_collisions(
                        alist,
                        str(archive["dst_dir"]),
                        members,
                        allow_matching_files=_is_direct_subtitle_archive(runtime_archive),
                    )
                    runtime_archive["members_sha256"] = _members_digest(members)
                except BaseException as exc:
                    metadata_error = exc
                    runtime_archive["members"] = []
                    runtime_archive["_local_fallback_required"] = True
                    if temporary_record is not None:
                        temporary_record["metadata_status"] = "failed"
                        _write_json_reserved(journal_path, journal)
                    if original_archive_path not in local_fallback_sources:
                        local_fallback_sources.add(original_archive_path)
                        fallback_dir = str(archive["src_dir"])
                        fallback_directory_counts[fallback_dir] = (
                            fallback_directory_counts.get(fallback_dir, 0) + 1
                        )
                        if fallback_directory_counts[fallback_dir] >= 2:
                            local_fallback_directories.add(fallback_dir)
                if metadata_error is not None and not (
                    (shutil.which("7z") or shutil.which("7zz"))
                    and str(archive.get("detected_format")) in {"zip", "7z", "rar"}
                ):
                    if temporary_record is None:
                        raise AssertionError("临时归档缺少事务记录")
                    restore_temporary_archive(
                        temporary_record,
                        str(runtime_archive["archive_path"]),
                        original_archive_path,
                        _archive_source_size(archive, original_archive_path),
                    )
                    temporary_name = None
                    active_temporary = None
                    raise metadata_error
            checkpoint_archive = dict(runtime_archive)
            checkpoint_archive["archive_path"] = original_archive_path
            native_receipt_verified = _verify_native_archive_receipt(
                alist,
                runtime_archive,
                archive_identity_path=original_archive_path,
                plan_sha256=plan_sha256,
                journal=journal,
            )
            checkpoint_verified = (
                _record_archive_member_checkpoint(
                    alist,
                    checkpoint_archive,
                    journal=journal,
                    journal_path=journal_path,
                )
                if native_receipt_verified
                else _resume_archive_member_checkpoint(
                    alist,
                    checkpoint_archive,
                    journal=journal,
                    journal_path=journal_path,
                )
            )
            if native_receipt_verified:
                _verify_extracted_members(alist, runtime_archive)
                _record_archive_member_checkpoint(
                    alist,
                    checkpoint_archive,
                    journal=journal,
                    journal_path=journal_path,
                )
                if temporary_name is not None:
                    if temporary_record is None:
                        raise AssertionError("临时归档缺少事务记录")
                    restore_temporary_archive(
                        temporary_record,
                        join_remote(str(archive["src_dir"]), temporary_name),
                        original_archive_path,
                        _archive_source_size(archive, original_archive_path),
                    )
                    active_temporary = None
                journal["retained_archives"].extend(
                    _retained_archive_paths(archive)
                )
                journal.pop("active_archive", None)
                _write_json_reserved(journal_path, journal)
                continue
            if checkpoint_verified is not None and not _is_direct_subtitle_archive(
                runtime_archive
            ):
                expected_files = set(_expected_archive_file_members(runtime_archive))
                if checkpoint_verified == expected_files:
                    task_evidence = _native_task_success_evidence(
                        journal, original_archive_path
                    )
                    if task_evidence is None:
                        raise ScraperError(
                            "归档输出已全部存在，但缺少原生解压的精确哈希 "
                            f"receipt；拒绝仅凭存在判定成功: {original_archive_path}"
                        )
                    _capture_native_archive_receipt(
                        alist,
                        runtime_archive,
                        archive_identity_path=original_archive_path,
                        plan_sha256=plan_sha256,
                        task_ids=task_evidence,
                        journal=journal,
                        journal_path=journal_path,
                    )
                if checkpoint_verified != expected_files:
                    resumed_locally = _extract_deferred_media_locally(
                        alist,
                        runtime_archive,
                        archive_password=passwords[original_archive_path],
                        upload_context=upload_context,
                        resume_checkpoint=True,
                    )
                    if not resumed_locally:
                        raise ScraperError(
                            "成员级 checkpoint 续跑需要可用的 7-Zip: "
                            f"{original_archive_path}"
                        )
                _verify_extracted_members(alist, runtime_archive)
                _record_archive_member_checkpoint(
                    alist,
                    checkpoint_archive,
                    journal=journal,
                    journal_path=journal_path,
                )
                if temporary_name is not None:
                    if temporary_record is None:
                        raise AssertionError("临时归档缺少事务记录")
                    restore_temporary_archive(
                        temporary_record,
                        join_remote(str(archive["src_dir"]), temporary_name),
                        original_archive_path,
                        _archive_source_size(archive, original_archive_path),
                    )
                    active_temporary = None
                journal["retained_archives"].extend(_retained_archive_paths(archive))
                journal.pop("active_archive", None)
                _write_json_reserved(journal_path, journal)
                continue
            if _is_direct_subtitle_archive(runtime_archive):
                print(
                    f"正在安全解出字幕包：{archive['name']} | "
                    f"{sum(1 for member in runtime_archive['members'] if not member['is_dir'])} 个字幕",
                    flush=True,
                )
                extracted_locally = _extract_subtitles_locally(
                    alist,
                    runtime_archive,
                    archive_password=passwords[original_archive_path],
                    upload_context=upload_context,
                )
                if not extracted_locally:
                    _extract_subtitles_with_explicit_types(
                        alist,
                        runtime_archive,
                        archive_password=passwords[original_archive_path],
                        upload_context=upload_context,
                    )
                _verify_extracted_members(alist, runtime_archive)
                if temporary_name is not None:
                    if temporary_record is None:
                        raise AssertionError("临时归档缺少事务记录")
                    restore_temporary_archive(
                        temporary_record,
                        join_remote(str(archive["src_dir"]), temporary_name),
                        original_archive_path,
                        _archive_source_size(archive, original_archive_path),
                    )
                    active_temporary = None
                journal["retained_archives"].extend(_retained_archive_paths(archive))
                _write_json_reserved(journal_path, journal)
                continue
            try:
                extracted_deferred_locally = _extract_deferred_media_locally(
                    alist,
                    runtime_archive,
                    archive_password=passwords[original_archive_path],
                    upload_context=upload_context,
                )
            except ArchiveOutputRejected as exc:
                if temporary_name is not None:
                    if temporary_record is None:
                        raise AssertionError("临时归档缺少事务记录")
                    restore_temporary_archive(
                        temporary_record,
                        join_remote(str(archive["src_dir"]), temporary_name),
                        original_archive_path,
                        _archive_source_size(archive, original_archive_path),
                    )
                    active_temporary = None
                blocked_archive_sources.add(original_archive_path)
                journal.setdefault("blocked_archives", []).append(
                    {
                        "archive_path": original_archive_path,
                        "reason": "provider-rejected-output",
                        "error": str(exc),
                        "recorded_at": datetime.now(timezone.utc).isoformat(),
                    }
                )
                _write_json_reserved(journal_path, journal)
                print(
                    f"警告：{archive['name']} 的解压输出被存储提供方拒绝；"
                    "原归档已保留，继续处理其他归档。",
                    file=sys.stderr,
                    flush=True,
                )
                continue
            if extracted_deferred_locally:
                _verify_extracted_members(alist, runtime_archive)
                if temporary_name is not None:
                    if temporary_record is None:
                        raise AssertionError("临时归档缺少事务记录")
                    restore_temporary_archive(
                        temporary_record,
                        join_remote(str(archive["src_dir"]), temporary_name),
                        original_archive_path,
                        _archive_source_size(archive, original_archive_path),
                    )
                    active_temporary = None
                journal["retained_archives"].extend(_retained_archive_paths(archive))
                _write_json_reserved(journal_path, journal)
                continue
            _reject_native_archive_reentry_conflicts(alist, runtime_archive)
            baseline = {
                kind: {_task_id(row) for row in _task_rows(alist, kind)}
                for kind in ("decompress", "decompress_upload")
            }
            journal["active_archive"] = original_archive_path
            _record_archive_member_checkpoint(
                alist,
                checkpoint_archive,
                journal=journal,
                journal_path=journal_path,
            )
            tasks = alist.archive_decompress(
                src_dir=str(runtime_archive["src_dir"]),
                dst_dir=str(runtime_archive["dst_dir"]),
                name=str(runtime_archive["name"]),
                archive_password=passwords[original_archive_path],
                cache_full=True,
                put_into_new_dir=False,
            )
            initial_ids = {_task_id(task) for task in tasks if _task_id(task)}
            if not initial_ids:
                raise ScraperError("AList 未返回可跟踪的解压任务 ID")
            print(
                f"已提交解压任务：{archive['name']} | "
                f"预计媒体文件 "
                f"{sum(1 for member in runtime_archive['members'] if not member['is_dir'] and Path(str(member['path'])).suffix.lower() in VIDEO_EXTS | SUBTITLE_EXTS)} 个",
                flush=True,
            )
            wait_for_archive_tasks(
                alist,
                baseline,
                initial_ids,
                timeout=timeout,
                journal=journal,
                journal_path=journal_path,
                archive=checkpoint_archive,
            )
            receipt_task_ids = {
                _task_id(row)
                for row in journal.get("tasks") or []
                if isinstance(row, Mapping)
                and int(row.get("state", -1)) == 2
                and _task_id(row)
            }
            if not receipt_task_ids or not initial_ids.issubset(receipt_task_ids):
                raise ScraperError("原生解压任务证据清单不完整")
            _capture_native_archive_receipt(
                alist,
                runtime_archive,
                archive_identity_path=original_archive_path,
                plan_sha256=plan_sha256,
                task_ids=receipt_task_ids,
                journal=journal,
                journal_path=journal_path,
            )
            _record_archive_member_checkpoint(
                alist,
                checkpoint_archive,
                journal=journal,
                journal_path=journal_path,
            )
            if temporary_name is not None:
                if temporary_record is None:
                    raise AssertionError("临时归档缺少事务记录")
                restore_temporary_archive(
                    temporary_record,
                    join_remote(str(archive["src_dir"]), temporary_name),
                    original_archive_path,
                    _archive_source_size(archive, original_archive_path),
                )
                active_temporary = None
            journal["retained_archives"].extend(_retained_archive_paths(archive))
            journal.pop("active_archive", None)
            _write_json_reserved(journal_path, journal)
        journal["status"] = "success"
        journal["completed_at"] = datetime.now(timezone.utc).isoformat()
        _write_json_reserved(journal_path, journal)
    except BaseException as exc:
        if active_temporary is not None:
            (
                temporary_path,
                original_path,
                expected_size,
                temporary_record,
            ) = active_temporary
            try:
                restore_temporary_archive(
                    temporary_record,
                    temporary_path,
                    original_path,
                    expected_size,
                )
                active_temporary = None
            except Exception as restore_exc:
                journal["temporary_restore_error"] = str(restore_exc)
        journal["status"] = "failed"
        journal["error"] = str(exc)
        _write_json_reserved(journal_path, journal)
        raise
    finally:
        if lock_held:
            try:
                alist.remove(str(plan["source_root"]), [lock_name])
            except Exception as exc:
                print(f"警告：无法删除解压锁 {lock_path}: {exc}", file=sys.stderr)


def _load_plan(path: Path) -> dict[str, Any]:
    try:
        raw = _load_json_text(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise ScraperError(f"无法读取解压计划 {path}: {exc}") from exc
    return _validate_loaded_plan(raw)


def main() -> int:
    # Python preserves an inherited SIG_IGN across exec. Container parents
    # commonly ignore SIGINT, which would make the Web safe-stop action a
    # no-op during a blocking AList request. Restore KeyboardInterrupt only
    # in that case so transaction cleanup can restore names and release locks.
    if signal.getsignal(signal.SIGINT) == signal.SIG_IGN:
        signal.signal(signal.SIGINT, signal.default_int_handler)
    parser = argparse.ArgumentParser()
    parser.add_argument("source", nargs="?")
    parser.add_argument("--plan-json", type=Path)
    parser.add_argument("--execute-plan", type=Path)
    parser.add_argument("--approve-plan-sha256")
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--journal", type=Path)
    parser.add_argument("--task-timeout", type=float, default=6 * 60 * 60)
    parser.add_argument("--alist-url", default=os.getenv("ALIST_URL", DEFAULT_ALIST_URL))
    parser.add_argument("--username", default=os.getenv("ALIST_USERNAME", "admin"))
    parser.add_argument("--password-file", type=Path)
    parser.add_argument("--archive-password-file", type=Path)
    parser.add_argument("--allow-insecure-http", action="store_true")
    args = parser.parse_args()

    try:
        if args.task_timeout <= 0:
            raise ScraperError("--task-timeout 必须大于 0")
        execute_mode = args.execute_plan is not None or args.execute
        if execute_mode:
            if args.source is not None or args.plan_json is not None:
                raise ScraperError("执行已审核计划时不能同时生成新计划")
            if args.execute_plan is None or not args.execute:
                raise ScraperError("执行需要 --execute-plan 和 --execute")
            if not isinstance(args.approve_plan_sha256, str) or not re.fullmatch(
                r"[0-9a-fA-F]{64}", args.approve_plan_sha256
            ):
                raise ScraperError("执行需要完整的 --approve-plan-sha256")
            plan = _load_plan(args.execute_plan)
            digest = hashlib.sha256(_canonical_json_bytes(plan)).hexdigest()
            if digest.lower() != args.approve_plan_sha256.lower():
                raise ScraperError("解压计划 SHA-256 与批准值不匹配")
            if args.journal is None:
                raise ScraperError("执行解压必须提供 --journal")
        else:
            if args.source is None or args.plan_json is None:
                raise ScraperError("生成解压计划需要 source 和 --plan-json")
            if args.approve_plan_sha256 or args.journal:
                raise ScraperError("批准摘要和 journal 只能用于执行")

        password = resolve_password(args.password_file, "ALIST_PASSWORD", "AList 密码: ")
        alist = AListClient(
            args.alist_url,
            args.username,
            password,
            timeout=600,
            retries=1,
            allow_insecure_http=args.allow_insecure_http,
        )
        alist.login()
        explicit_archive_password = None
        if args.archive_password_file is not None or os.getenv("ARCHIVE_PASSWORD"):
            explicit_archive_password = resolve_password(
                args.archive_password_file, "ARCHIVE_PASSWORD", "归档密码: "
            )

        if not execute_mode:
            plan, _ = build_archive_plan(
                alist,
                args.source,
                explicit_archive_password=explicit_archive_password,
            )
            _reserve_output_path(args.plan_json)
            _write_json_reserved(args.plan_json, plan)
            digest = hashlib.sha256(_canonical_json_bytes(plan)).hexdigest()
            print(f"解压计划: {args.plan_json}")
            print(f"归档数: {len(plan['archives'])}")
            for archive in plan["archives"]:
                inspection_summary = (
                    "待运行时按文件签名与成员类型复核"
                    if archive.get("deferred_inspection") is True
                    else f"{archive['video_count']} 视频"
                )
                print(
                    f"  {archive['archive_path']} -> {archive['dst_dir']} | "
                    f"{len(archive['parts'])} 分卷 | {inspection_summary} | "
                    f"密码来源: {archive['password_source']}"
                )
            print(f"计划 SHA-256: {digest}")
            print("DRY RUN：未解压任何文件。")
            return 0

        passwords: dict[str, str] = {}
        tree_passwords = discover_tree_passwords(alist, str(plan["source_root"]))
        for archive in plan["archives"]:
            archive_dir = str(archive["src_dir"])
            archive_password, _ = _password_for_archive(
                alist,
                archive_dir,
                explicit_archive_password,
                tree_passwords,
            )
            passwords[str(archive["archive_path"])] = archive_password
        execute_archive_plan(
            alist,
            plan,
            passwords,
            timeout=args.task_timeout,
            journal_path=args.journal,
        )
        print("解压完成并通过视频数量与大小校验。")
        return 0
    except NoArchivesFound as exc:
        print(f"{exc}，继续识别媒体。")
        return NO_ARCHIVES_EXIT_CODE
    except (ScraperError, ApiError, OSError, ValueError) as exc:
        print(f"错误: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
