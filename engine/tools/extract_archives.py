#!/usr/bin/env python3
"""安全地规划并通过 AList 解压分卷归档。

生成阶段只读取归档目录并写入不含密码的 JSON 计划。执行阶段
必须提交同一计划的完整 SHA-256，再次核对分卷快照、归档成员和
目标冲突。密码可从同目录的“密码：...”标记自动识别，但从不写入
计划、journal 或终端输出。
"""

from __future__ import annotations

import argparse
import getpass
import hashlib
import json
import os
import re
import sys
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
    VIDEO_EXTS,
    _canonical_json_bytes,
    _collision_key,
    _entry_hash_value,
    _load_json_text,
    _reserve_output_path,
    _validate_remote_basename,
    _write_json_reserved,
    is_scraper_lock,
    join_remote,
    normalize_remote_path,
    split_remote,
)


ARCHIVE_PLAN_SCHEMA = 1
MIN_SAFE_ALIST_VERSION = (3, 57, 0)
MULTIPART_RE = re.compile(r"^(?P<prefix>.+\.(?:7z|zip)\.)(?P<index>\d{3})$", re.I)
RAR_FIRST_RE = re.compile(r"^(?P<prefix>.+\.part)(?P<index>0*1)(?P<suffix>\.rar)$", re.I)
PASSWORD_MARKER_RE = re.compile(
    r"^\s*(?:解压|压缩包|归档)?\s*密码\s*[:：]\s*(?P<password>.+?)\s*$",
    re.I,
)


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
    matches: list[str] = []
    for entry in alist.list(archive_dir, refresh=True):
        name = entry.get("name")
        if not isinstance(name, str):
            continue
        match = PASSWORD_MARKER_RE.match(name)
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


def _archive_candidate(name: str) -> bool:
    return bool(MULTIPART_RE.match(name) and name.lower().endswith(".001")) or bool(
        RAR_FIRST_RE.match(name)
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
    return output


def _password_for_archive(
    alist: AListClient, archive_dir: str, explicit_password: str | None
) -> tuple[str, str]:
    if explicit_password is not None:
        return explicit_password, "password-file-or-env"
    value, source = discover_archive_password(alist, archive_dir)
    if not value:
        if not sys.stdin.isatty():
            raise ScraperError(f"未找到归档密码标记: {archive_dir}")
        value = getpass.getpass(f"归档密码 ({archive_dir})：")
        if not value:
            raise ScraperError("归档密码不能为空")
        source = "interactive"
    return value, source


def _check_destination_collisions(
    alist: AListClient, destination: str, members: list[dict[str, Any]]
) -> None:
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
    candidates = sorted(
        [item for item in files if _archive_candidate(str(item.get("name", "")))],
        key=lambda item: _collision_key(str(item.get("full_path", ""))),
    )
    if not candidates:
        raise ScraperError("未找到可支持的首卷（.7z.001/.zip.001/.part1.rar）")
    archives: list[dict[str, Any]] = []
    passwords: dict[str, str] = {}
    planned_destinations: set[tuple[str, str]] = set()
    for item in candidates:
        archive_path = normalize_remote_path(str(item["full_path"]))
        archive_dir, archive_name = split_remote(archive_path)
        password, password_source = _password_for_archive(
            alist, archive_dir, explicit_archive_password
        )
        meta = alist.archive_meta(archive_path, archive_password=password, refresh=True)
        members = _flatten_members(meta.get("content") or [])
        videos = [
            member
            for member in members
            if not member["is_dir"] and Path(str(member["path"])).suffix.lower() in VIDEO_EXTS
        ]
        if not videos:
            raise ScraperError(f"归档内未找到视频文件: {archive_path}")
        _check_destination_collisions(alist, archive_dir, members)
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
                "members_sha256": hashlib.sha256(_canonical_json_bytes(members)).hexdigest(),
                "video_count": len(videos),
                "video_bytes": sum(int(member["size"]) for member in videos),
                "cache_full": True,
                "put_into_new_dir": False,
            }
        )
        passwords[archive_path] = password
    plan = {
        "archive_plan_schema": ARCHIVE_PLAN_SCHEMA,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "source_root": source_root,
        "alist_version": server_version,
        "archives": archives,
    }
    return plan, passwords


def _validate_loaded_plan(raw: Any) -> dict[str, Any]:
    if not isinstance(raw, dict):
        raise ScraperError("解压计划必须是 JSON 对象")
    allowed = {"archive_plan_schema", "created_at", "source_root", "alist_version", "archives"}
    unknown = sorted(set(raw) - allowed)
    if unknown:
        raise ScraperError("解压计划含未知字段: " + ", ".join(unknown))
    if raw.get("archive_plan_schema") != ARCHIVE_PLAN_SCHEMA:
        raise ScraperError("不支持的解压计划 schema")
    source_root = raw.get("source_root")
    if not isinstance(source_root, str) or normalize_remote_path(source_root) != source_root:
        raise ScraperError("解压计划 source_root 无效")
    archives = raw.get("archives")
    if not isinstance(archives, list) or not archives:
        raise ScraperError("解压计划 archives 必须是非空数组")
    required = {
        "archive_path", "src_dir", "name", "dst_dir", "parts", "password_source",
        "members", "members_sha256", "video_count", "video_bytes", "cache_full",
        "put_into_new_dir",
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
        if not isinstance(members, list) or not members:
            raise ScraperError(f"第 {index} 个归档 members 无效")
        digest = hashlib.sha256(_canonical_json_bytes(members)).hexdigest()
        if archive.get("members_sha256") != digest:
            raise ScraperError(f"第 {index} 个归档成员摘要不匹配")
        if archive.get("cache_full") is not True or archive.get("put_into_new_dir") is not False:
            raise ScraperError("当前 schema 要求 cache_full=true 且 put_into_new_dir=false")
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
        _write_json_reserved(journal_path, journal)
        time.sleep(5)
    raise ScraperError(f"等待 AList 解压超时（{timeout:.0f} 秒）")


def _verify_part_snapshots(alist: AListClient, archive: Mapping[str, Any]) -> None:
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
            if expected.get(key) is not None and actual_snapshot.get(key) != expected.get(key):
                raise ScraperError(f"执行前分卷快照已变化: {expected['path']}; {key}")


def _verify_extracted_videos(alist: AListClient, archive: Mapping[str, Any]) -> None:
    expected = [
        member for member in archive["members"]
        if not member["is_dir"] and Path(str(member["path"])).suffix.lower() in VIDEO_EXTS
    ]
    by_parent: dict[str, list[dict[str, Any]]] = {}
    for member in expected:
        full_path = join_remote(str(archive["dst_dir"]), str(member["path"]))
        parent, name = split_remote(full_path)
        if parent not in by_parent:
            by_parent[parent] = alist.list(parent, refresh=True)
        matches = [
            entry for entry in by_parent[parent]
            if not entry.get("is_dir")
            and _collision_key(str(entry.get("name", ""))) == _collision_key(name)
        ]
        if len(matches) != 1:
            raise ScraperError(f"解压后视频缺失或冲突: {full_path}")
        if int(matches[0].get("size") or 0) != int(member["size"]):
            raise ScraperError(f"解压后视频大小不匹配: {full_path}")


def execute_archive_plan(
    alist: AListClient,
    plan: dict[str, Any],
    passwords: Mapping[str, str],
    *,
    timeout: float,
    journal_path: Path,
) -> None:
    require_safe_archive_server(alist)
    _reserve_output_path(journal_path)
    journal: dict[str, Any] = {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "plan_sha256": hashlib.sha256(_canonical_json_bytes(plan)).hexdigest(),
        "status": "preflight",
        "locks": [],
        "tasks": [],
    }
    _write_json_reserved(journal_path, journal)
    lock_name = f".scraper-lock-archive-{uuid.uuid4().hex}.json"
    lock_path = join_remote(str(plan["source_root"]), lock_name)
    lock_held = False
    try:
        for archive in plan["archives"]:
            _verify_part_snapshots(alist, archive)
            password = passwords[str(archive["archive_path"])]
            meta = alist.archive_meta(
                str(archive["archive_path"]), archive_password=password, refresh=True
            )
            members = _flatten_members(meta.get("content") or [])
            digest = hashlib.sha256(_canonical_json_bytes(members)).hexdigest()
            if digest != archive["members_sha256"]:
                raise ScraperError(f"执行前归档成员已变化: {archive['archive_path']}")
            _check_destination_collisions(alist, str(archive["dst_dir"]), members)
        lock_payload = json.dumps(
            {
                "kind": "archive-extraction",
                "created_at": datetime.now(timezone.utc).isoformat(),
                "plan_sha256": journal["plan_sha256"],
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
        journal["locks"] = [lock_path]
        _write_json_reserved(journal_path, journal)

        for archive in plan["archives"]:
            baseline = {
                kind: {_task_id(row) for row in _task_rows(alist, kind)}
                for kind in ("decompress", "decompress_upload")
            }
            tasks = alist.archive_decompress(
                src_dir=str(archive["src_dir"]),
                dst_dir=str(archive["dst_dir"]),
                name=str(archive["name"]),
                archive_password=passwords[str(archive["archive_path"])],
                cache_full=True,
                put_into_new_dir=False,
            )
            initial_ids = {_task_id(task) for task in tasks if _task_id(task)}
            if not initial_ids:
                raise ScraperError("AList 未返回可跟踪的解压任务 ID")
            print(
                f"已提交解压任务：{archive['name']} | "
                f"预计视频 {archive['video_count']} 个",
                flush=True,
            )
            wait_for_archive_tasks(
                alist,
                baseline,
                initial_ids,
                timeout=timeout,
                journal=journal,
                journal_path=journal_path,
            )
            _verify_extracted_videos(alist, archive)
        journal["status"] = "success"
        journal["completed_at"] = datetime.now(timezone.utc).isoformat()
        _write_json_reserved(journal_path, journal)
    except BaseException as exc:
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
                print(
                    f"  {archive['archive_path']} -> {archive['dst_dir']} | "
                    f"{len(archive['parts'])} 分卷 | {archive['video_count']} 视频 | "
                    f"密码来源: {archive['password_source']}"
                )
            print(f"计划 SHA-256: {digest}")
            print("DRY RUN：未解压任何文件。")
            return 0

        passwords: dict[str, str] = {}
        for archive in plan["archives"]:
            archive_dir = str(archive["src_dir"])
            archive_password, _ = _password_for_archive(
                alist, archive_dir, explicit_archive_password
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
    except (ScraperError, ApiError, OSError, ValueError) as exc:
        print(f"错误: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
