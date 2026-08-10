"""Manual offline backup and restore checks for local ScrapeFlow state."""

from __future__ import annotations

import json
import shutil
import sqlite3
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping

from engine.scrapeflow.serialization import atomic_write_json

from .control_state import PersistentControlState


MANIFEST_NAME = "scrapeflow-offline-backup.json"
BACKUP_VERSION = 1


class OfflineBackupError(RuntimeError):
    """The requested offline backup or restore check is unsafe."""


def _now() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def _directory(value: str | Path, *, label: str, must_exist: bool = True) -> Path:
    path = Path(value).expanduser()
    if must_exist:
        try:
            resolved = path.resolve(strict=True)
        except OSError as exc:
            raise OfflineBackupError(f"{label} 不存在或不可访问: {path}") from exc
        if not resolved.is_dir():
            raise OfflineBackupError(f"{label} 不是目录: {resolved}")
        return resolved
    return path.resolve()


def _reject_nested_output(output: Path, inputs: Mapping[str, Path]) -> None:
    for label, source in inputs.items():
        if output == source or output.is_relative_to(source):
            raise OfflineBackupError(f"备份输出不能位于 {label} 内部: {output}")


def _copy_tree(source: Path, destination: Path) -> None:
    if destination.exists():
        raise OfflineBackupError(f"备份目标已存在，拒绝覆盖: {destination}")
    shutil.copytree(source, destination, symlinks=True)


def _tree_stats(root: Path) -> dict[str, object]:
    files = 0
    directories = 0
    total_bytes = 0
    for path in root.rglob("*"):
        try:
            stat = path.lstat()
        except OSError as exc:
            raise OfflineBackupError(f"无法读取备份树条目: {path}") from exc
        if path.is_dir():
            directories += 1
            continue
        if path.is_file():
            files += 1
            total_bytes += stat.st_size
    return {
        "files": files,
        "directories": directories,
        "total_bytes": total_bytes,
    }


def _parse_json_tree(root: Path) -> dict[str, object]:
    checked = 0
    for path in sorted(root.rglob("*.json")):
        if not path.is_file():
            continue
        try:
            with path.open("r", encoding="utf-8") as handle:
                json.load(handle)
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise OfflineBackupError(f"JSON 校验失败: {path}") from exc
        checked += 1
    return {"checked": checked}


def _sqlite_quick_check(root: Path) -> list[dict[str, object]]:
    results: list[dict[str, object]] = []
    suffixes = {".db", ".sqlite", ".sqlite3"}
    for path in sorted(root.rglob("*")):
        if not path.is_file() or path.suffix.casefold() not in suffixes:
            continue
        try:
            with sqlite3.connect(f"file:{path}?mode=ro", uri=True) as connection:
                rows = [row[0] for row in connection.execute("PRAGMA quick_check")]
        except sqlite3.Error as exc:
            raise OfflineBackupError(f"SQLite quick_check 无法执行: {path}") from exc
        if rows != ["ok"]:
            raise OfflineBackupError(f"SQLite quick_check 未通过: {path}: {rows}")
        results.append({
            "path": str(path.relative_to(root)),
            "result": "ok",
        })
    return results


def _control_snapshot(scrapeflow_data: Path) -> dict[str, object]:
    state = PersistentControlState(scrapeflow_data / "global-control.json").read()
    if state.get("paused") is not True:
        raise OfflineBackupError("备份前必须保持全局 pause")
    return state


def _validate_media_snapshot_note(note: str) -> str:
    normalized = str(note or "").strip()
    if not normalized:
        raise OfflineBackupError("必须记录正式媒体库的外部快照或可恢复副本说明")
    if len(normalized) > 1000:
        raise OfflineBackupError("正式媒体库快照说明过长")
    return normalized


def _backup_label(label: str | None) -> str:
    if label is None or not label.strip():
        return "scrapeflow-offline-" + datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    normalized = label.strip()
    if any(char in normalized for char in ("/", "\\", "\x00")):
        raise OfflineBackupError("备份标签不能包含路径分隔符")
    if normalized in {".", ".."}:
        raise OfflineBackupError("备份标签无效")
    return normalized


def _build_manifest(
    *,
    backup_dir: Path,
    alist_data: Path,
    scrapeflow_data: Path,
    copied_alist: Path,
    copied_scrapeflow: Path,
    media_snapshot_note: str,
) -> dict[str, object]:
    copied_stats = {
        "alist_data": _tree_stats(copied_alist),
        "scrapeflow_data": _tree_stats(copied_scrapeflow),
    }
    source_stats = {
        "alist_data": _tree_stats(alist_data),
        "scrapeflow_data": _tree_stats(scrapeflow_data),
    }
    if copied_stats != source_stats:
        raise OfflineBackupError("复制后的文件数或总字节数与源目录不一致")
    return {
        "version": BACKUP_VERSION,
        "created_at": _now(),
        "backup_root": str(backup_dir),
        "sources": {
            "alist_data": str(alist_data),
            "scrapeflow_data": str(scrapeflow_data),
        },
        "copies": {
            "alist_data": "alist-data",
            "scrapeflow_data": "scrapeflow-data",
        },
        "control": _control_snapshot(scrapeflow_data),
        "media_library": {
            "status": "external_snapshot_required",
            "note": _validate_media_snapshot_note(media_snapshot_note),
        },
        "checks": {
            "source_stats": source_stats,
            "copied_stats": copied_stats,
            "json": {
                "alist_data": _parse_json_tree(copied_alist),
                "scrapeflow_data": _parse_json_tree(copied_scrapeflow),
            },
            "sqlite_quick_check": {
                "alist_data": _sqlite_quick_check(copied_alist),
                "scrapeflow_data": _sqlite_quick_check(copied_scrapeflow),
            },
        },
    }


def create_offline_backup(
    *,
    alist_data: str | Path,
    scrapeflow_data: str | Path,
    output_dir: str | Path,
    media_snapshot_note: str,
    label: str | None = None,
) -> dict[str, object]:
    """Copy stopped local state and write a small verification manifest."""
    alist = _directory(alist_data, label="AList data")
    scrapeflow = _directory(scrapeflow_data, label="ScrapeFlow data")
    output_root = _directory(output_dir, label="备份输出父目录", must_exist=False)
    _reject_nested_output(output_root, {"AList data": alist, "ScrapeFlow data": scrapeflow})
    output_root.mkdir(mode=0o700, parents=True, exist_ok=True)
    backup_dir = output_root / _backup_label(label)
    if backup_dir.exists():
        raise OfflineBackupError(f"备份目录已存在，拒绝覆盖: {backup_dir}")
    control = _control_snapshot(scrapeflow)
    if control.get("paused") is not True:
        raise OfflineBackupError("备份前必须保持全局 pause")
    backup_dir.mkdir(mode=0o700)
    copied_alist = backup_dir / "alist-data"
    copied_scrapeflow = backup_dir / "scrapeflow-data"
    try:
        _copy_tree(alist, copied_alist)
        _copy_tree(scrapeflow, copied_scrapeflow)
        manifest = _build_manifest(
            backup_dir=backup_dir,
            alist_data=alist,
            scrapeflow_data=scrapeflow,
            copied_alist=copied_alist,
            copied_scrapeflow=copied_scrapeflow,
            media_snapshot_note=media_snapshot_note,
        )
        atomic_write_json(backup_dir / MANIFEST_NAME, manifest, allow_nan=False)
        return manifest
    except Exception:
        shutil.rmtree(backup_dir, ignore_errors=True)
        raise


def _load_manifest(backup_dir: Path) -> dict[str, object]:
    path = backup_dir / MANIFEST_NAME
    try:
        with path.open("r", encoding="utf-8") as handle:
            payload = json.load(handle)
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise OfflineBackupError(f"备份 manifest 不可读: {path}") from exc
    if not isinstance(payload, dict) or payload.get("version") != BACKUP_VERSION:
        raise OfflineBackupError("备份 manifest 版本无效")
    return payload


def verify_offline_backup(backup_dir: str | Path) -> dict[str, object]:
    """Re-run local JSON, SQLite and file count checks for one backup."""
    root = _directory(backup_dir, label="备份目录")
    manifest = _load_manifest(root)
    copies = manifest.get("copies")
    if not isinstance(copies, Mapping):
        raise OfflineBackupError("备份 manifest 缺少 copies")
    copied_alist = root / str(copies.get("alist_data") or "")
    copied_scrapeflow = root / str(copies.get("scrapeflow_data") or "")
    if not copied_alist.is_dir() or not copied_scrapeflow.is_dir():
        raise OfflineBackupError("备份目录缺少本地状态副本")
    result = {
        "backup_root": str(root),
        "control": _control_snapshot(copied_scrapeflow),
        "checks": {
            "copied_stats": {
                "alist_data": _tree_stats(copied_alist),
                "scrapeflow_data": _tree_stats(copied_scrapeflow),
            },
            "json": {
                "alist_data": _parse_json_tree(copied_alist),
                "scrapeflow_data": _parse_json_tree(copied_scrapeflow),
            },
            "sqlite_quick_check": {
                "alist_data": _sqlite_quick_check(copied_alist),
                "scrapeflow_data": _sqlite_quick_check(copied_scrapeflow),
            },
        },
    }
    return result


def restore_offline_backup(
    *,
    backup_dir: str | Path,
    restore_dir: str | Path,
) -> dict[str, object]:
    """Copy one backup into an isolated restore directory and verify pause."""
    root = _directory(backup_dir, label="备份目录")
    restore_root = _directory(restore_dir, label="恢复输出父目录", must_exist=False)
    if restore_root.exists() and any(restore_root.iterdir()):
        raise OfflineBackupError(f"恢复目录必须为空: {restore_root}")
    manifest = _load_manifest(root)
    copies = manifest.get("copies")
    if not isinstance(copies, Mapping):
        raise OfflineBackupError("备份 manifest 缺少 copies")
    source_alist = root / str(copies.get("alist_data") or "")
    source_scrapeflow = root / str(copies.get("scrapeflow_data") or "")
    restore_root.mkdir(mode=0o700, parents=True, exist_ok=True)
    restored_alist = restore_root / "alist-data"
    restored_scrapeflow = restore_root / "scrapeflow-data"
    _copy_tree(source_alist, restored_alist)
    _copy_tree(source_scrapeflow, restored_scrapeflow)
    control = _control_snapshot(restored_scrapeflow)
    return {
        "restore_root": str(restore_root),
        "alist_data": str(restored_alist),
        "scrapeflow_data": str(restored_scrapeflow),
        "control": control,
        "checks": {
            "json": {
                "alist_data": _parse_json_tree(restored_alist),
                "scrapeflow_data": _parse_json_tree(restored_scrapeflow),
            },
            "sqlite_quick_check": {
                "alist_data": _sqlite_quick_check(restored_alist),
                "scrapeflow_data": _sqlite_quick_check(restored_scrapeflow),
            },
            "copied_stats": {
                "alist_data": _tree_stats(restored_alist),
                "scrapeflow_data": _tree_stats(restored_scrapeflow),
            },
        },
    }


__all__ = [
    "BACKUP_VERSION",
    "MANIFEST_NAME",
    "OfflineBackupError",
    "create_offline_backup",
    "restore_offline_backup",
    "verify_offline_backup",
]
