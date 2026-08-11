"""Manual offline backup and restore checks for local ScrapeFlow state."""

from __future__ import annotations

import json
import shutil
import sqlite3
import tempfile
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping

from engine.scrapeflow.serialization import atomic_write_json

from .control_state import PersistentControlState


MANIFEST_NAME = "scrapeflow-offline-backup.json"
BACKUP_VERSION = 1
_COPY_LAYOUT = {
    "alist_data": "alist-data",
    "scrapeflow_data": "scrapeflow-data",
}
_TREE_STAT_FIELDS = frozenset({"files", "directories", "total_bytes"})


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


def _path_is_within(path: Path, parent: Path) -> bool:
    try:
        path.relative_to(parent)
        return True
    except ValueError:
        return False


def _validate_tree_containment(root: Path, *, label: str) -> None:
    """Reject state trees whose symlinks escape the state root.

    AList and ScrapeFlow state normally do not need external symlinks.  Keeping
    an internal symlink is safe, but following an external one while parsing
    JSON or SQLite would make a backup/restore operation reach outside its
    declared state boundary.
    """
    resolved_root = root.resolve(strict=True)
    for path in root.rglob("*"):
        if not path.is_symlink():
            continue
        try:
            target = path.resolve(strict=True)
        except (OSError, RuntimeError) as exc:
            raise OfflineBackupError(f"{label} 包含不可解析的符号链接: {path}") from exc
        if not _path_is_within(target, resolved_root):
            raise OfflineBackupError(
                f"{label} 的符号链接越出状态根目录: {path}",
            )


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
            # Opening a WAL-mode database with SQLite's regular read-only URI
            # can still create ``-wal``/``-shm`` companions beside the file.
            # Verification must never mutate the backup it is measuring, so
            # run quick_check on a disposable copy.  Preserve a durable WAL or
            # rollback journal when one exists; ``-shm`` is process-local and
            # is deliberately regenerated only inside the scratch directory.
            with tempfile.TemporaryDirectory(
                prefix="scrapeflow-sqlite-check-",
            ) as temporary:
                scratch = Path(temporary) / path.name
                shutil.copy2(path, scratch)
                for suffix in ("-wal", "-journal"):
                    companion = Path(str(path) + suffix)
                    if companion.is_file():
                        shutil.copy2(companion, Path(str(scratch) + suffix))
                with sqlite3.connect(scratch) as connection:
                    rows = [row[0] for row in connection.execute("PRAGMA quick_check")]
        except (OSError, sqlite3.Error) as exc:
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
            **_COPY_LAYOUT,
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
    _validate_tree_containment(alist, label="AList data")
    _validate_tree_containment(scrapeflow, label="ScrapeFlow data")
    backup_dir.mkdir(mode=0o700)
    copied_alist = backup_dir / "alist-data"
    copied_scrapeflow = backup_dir / "scrapeflow-data"
    try:
        _copy_tree(alist, copied_alist)
        _copy_tree(scrapeflow, copied_scrapeflow)
        _validate_tree_containment(copied_alist, label="备份 AList data")
        _validate_tree_containment(copied_scrapeflow, label="备份 ScrapeFlow data")
        manifest = _build_manifest(
            backup_dir=backup_dir,
            alist_data=alist,
            scrapeflow_data=scrapeflow,
            copied_alist=copied_alist,
            copied_scrapeflow=copied_scrapeflow,
            media_snapshot_note=media_snapshot_note,
        )
        atomic_write_json(backup_dir / MANIFEST_NAME, manifest, allow_nan=False)
        # Re-open the written manifest and prove the final on-disk copy is the
        # exact state described by it before reporting a backup as successful.
        verify_offline_backup(backup_dir)
        return manifest
    except Exception:
        shutil.rmtree(backup_dir, ignore_errors=True)
        raise


def _load_manifest(backup_dir: Path) -> dict[str, object]:
    path = backup_dir / MANIFEST_NAME
    if path.is_symlink() or not path.is_file():
        raise OfflineBackupError(f"备份 manifest 必须是目录内的普通文件: {path}")
    try:
        with path.open("r", encoding="utf-8") as handle:
            payload = json.load(handle)
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise OfflineBackupError(f"备份 manifest 不可读: {path}") from exc
    if not isinstance(payload, dict) or payload.get("version") != BACKUP_VERSION:
        raise OfflineBackupError("备份 manifest 版本无效")
    return payload


def _manifest_mapping(value: object, *, label: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise OfflineBackupError(f"备份 manifest 的 {label} 无效")
    return value


def _manifest_absolute_path(value: object, *, label: str) -> Path:
    if not isinstance(value, str):
        raise OfflineBackupError(f"备份 manifest 的 {label} 无效")
    try:
        path = Path(value)
    except ValueError as exc:
        raise OfflineBackupError(f"备份 manifest 的 {label} 无效") from exc
    if not path.is_absolute():
        raise OfflineBackupError(f"备份 manifest 的 {label} 无效")
    return path


def _manifest_tree_stats(value: object, *, label: str) -> dict[str, int]:
    payload = _manifest_mapping(value, label=label)
    if set(payload) != _TREE_STAT_FIELDS:
        raise OfflineBackupError(f"备份 manifest 的 {label} 字段无效")
    result: dict[str, int] = {}
    for key in sorted(_TREE_STAT_FIELDS):
        item = payload.get(key)
        if type(item) is not int or item < 0:
            raise OfflineBackupError(f"备份 manifest 的 {label}.{key} 无效")
        result[key] = item
    return result


def _manifest_json_check(value: object, *, label: str) -> dict[str, int]:
    payload = _manifest_mapping(value, label=label)
    if set(payload) != {"checked"}:
        raise OfflineBackupError(f"备份 manifest 的 {label} 字段无效")
    checked = payload.get("checked")
    if type(checked) is not int or checked < 0:
        raise OfflineBackupError(f"备份 manifest 的 {label}.checked 无效")
    return {"checked": checked}


def _manifest_sqlite_check(value: object, *, label: str) -> list[dict[str, str]]:
    if not isinstance(value, list):
        raise OfflineBackupError(f"备份 manifest 的 {label} 无效")
    result: list[dict[str, str]] = []
    previous_path: str | None = None
    for index, row in enumerate(value):
        payload = _manifest_mapping(row, label=f"{label}[{index}]")
        if set(payload) != {"path", "result"}:
            raise OfflineBackupError(f"备份 manifest 的 {label}[{index}] 字段无效")
        relative_path = payload.get("path")
        result_value = payload.get("result")
        try:
            candidate = Path(relative_path) if isinstance(relative_path, str) else None
        except ValueError as exc:
            raise OfflineBackupError(f"备份 manifest 的 {label}[{index}] 无效") from exc
        if (
            not isinstance(relative_path, str)
            or not relative_path
            or candidate is None
            or candidate.is_absolute()
            or any(part in {"", ".", ".."} for part in candidate.parts)
            or result_value != "ok"
        ):
            raise OfflineBackupError(f"备份 manifest 的 {label}[{index}] 无效")
        normalized = candidate.as_posix()
        if previous_path is not None and normalized <= previous_path:
            raise OfflineBackupError(f"备份 manifest 的 {label} 未按路径严格排序")
        previous_path = normalized
        result.append({"path": normalized, "result": "ok"})
    return result


def _manifest_copy_root(
    backup_root: Path,
    copies: Mapping[str, object],
    *,
    key: str,
) -> Path:
    expected_name = _COPY_LAYOUT[key]
    raw_name = copies.get(key)
    if raw_name != expected_name:
        raise OfflineBackupError(f"备份 manifest 的 copies.{key} 路径无效")
    candidate = backup_root / expected_name
    if candidate.is_symlink():
        raise OfflineBackupError(f"备份 manifest 的 copies.{key} 不能是符号链接")
    try:
        resolved = candidate.resolve(strict=True)
    except (OSError, RuntimeError) as exc:
        raise OfflineBackupError(f"备份目录缺少本地状态副本: {candidate}") from exc
    if (
        not resolved.is_dir()
        or resolved.parent != backup_root
        or not _path_is_within(resolved, backup_root)
    ):
        raise OfflineBackupError(f"备份 manifest 的 copies.{key} 越出备份目录")
    return resolved


def _validated_manifest(
    backup_root: Path,
    manifest: Mapping[str, object],
) -> tuple[dict[str, Path], dict[str, object]]:
    """Validate every manifest-controlled path before accessing copied data."""
    if set(manifest) != {
        "version",
        "created_at",
        "backup_root",
        "sources",
        "copies",
        "control",
        "media_library",
        "checks",
    }:
        raise OfflineBackupError("备份 manifest 字段无效")
    if manifest.get("version") != BACKUP_VERSION:
        raise OfflineBackupError("备份 manifest 版本无效")
    if not isinstance(manifest.get("created_at"), str) or not manifest["created_at"]:
        raise OfflineBackupError("备份 manifest 缺少创建时间")
    _manifest_absolute_path(manifest.get("backup_root"), label="backup_root")

    sources = _manifest_mapping(manifest.get("sources"), label="sources")
    if set(sources) != set(_COPY_LAYOUT):
        raise OfflineBackupError("备份 manifest 的 sources 字段无效")
    for key in _COPY_LAYOUT:
        _manifest_absolute_path(sources.get(key), label=f"sources.{key}")

    copies = _manifest_mapping(manifest.get("copies"), label="copies")
    if set(copies) != set(_COPY_LAYOUT):
        raise OfflineBackupError("备份 manifest 的 copies 字段无效")
    copy_roots = {
        key: _manifest_copy_root(backup_root, copies, key=key)
        for key in _COPY_LAYOUT
    }
    for key, path in copy_roots.items():
        _validate_tree_containment(path, label=f"备份 {key}")

    control = _manifest_mapping(manifest.get("control"), label="control")
    if control.get("paused") is not True or control.get("scheduler_paused") is not True:
        raise OfflineBackupError("备份 manifest 未记录 paused 控制状态")
    media_library = _manifest_mapping(manifest.get("media_library"), label="media_library")
    if (
        set(media_library) != {"status", "note"}
        or media_library.get("status") != "external_snapshot_required"
        or not isinstance(media_library.get("note"), str)
        or not media_library["note"].strip()
    ):
        raise OfflineBackupError("备份 manifest 的正式媒体库恢复点无效")

    checks = _manifest_mapping(manifest.get("checks"), label="checks")
    if set(checks) != {"source_stats", "copied_stats", "json", "sqlite_quick_check"}:
        raise OfflineBackupError("备份 manifest 的 checks 字段无效")
    source_stats_raw = _manifest_mapping(checks.get("source_stats"), label="checks.source_stats")
    copied_stats_raw = _manifest_mapping(checks.get("copied_stats"), label="checks.copied_stats")
    json_raw = _manifest_mapping(checks.get("json"), label="checks.json")
    sqlite_raw = _manifest_mapping(
        checks.get("sqlite_quick_check"), label="checks.sqlite_quick_check",
    )
    if (
        set(source_stats_raw) != set(_COPY_LAYOUT)
        or set(copied_stats_raw) != set(_COPY_LAYOUT)
        or set(json_raw) != set(_COPY_LAYOUT)
        or set(sqlite_raw) != set(_COPY_LAYOUT)
    ):
        raise OfflineBackupError("备份 manifest 的 checks 子字段无效")
    source_stats = {
        key: _manifest_tree_stats(source_stats_raw.get(key), label=f"checks.source_stats.{key}")
        for key in _COPY_LAYOUT
    }
    copied_stats = {
        key: _manifest_tree_stats(copied_stats_raw.get(key), label=f"checks.copied_stats.{key}")
        for key in _COPY_LAYOUT
    }
    if source_stats != copied_stats:
        raise OfflineBackupError("备份 manifest 的源目录与副本统计不一致")
    expected = {
        "copied_stats": copied_stats,
        "json": {
            key: _manifest_json_check(json_raw.get(key), label=f"checks.json.{key}")
            for key in _COPY_LAYOUT
        },
        "sqlite_quick_check": {
            key: _manifest_sqlite_check(
                sqlite_raw.get(key), label=f"checks.sqlite_quick_check.{key}",
            )
            for key in _COPY_LAYOUT
        },
        "control": dict(control),
    }
    return copy_roots, expected


def _verified_copy_checks(
    copy_roots: Mapping[str, Path],
    expected: Mapping[str, object],
) -> dict[str, object]:
    copied_stats = {
        key: _tree_stats(copy_roots[key])
        for key in _COPY_LAYOUT
    }
    if copied_stats != expected["copied_stats"]:
        raise OfflineBackupError("备份副本统计与 manifest 不一致")
    json_checks = {
        key: _parse_json_tree(copy_roots[key])
        for key in _COPY_LAYOUT
    }
    if json_checks != expected["json"]:
        raise OfflineBackupError("备份 JSON 校验结果与 manifest 不一致")
    sqlite_checks = {
        key: _sqlite_quick_check(copy_roots[key])
        for key in _COPY_LAYOUT
    }
    if sqlite_checks != expected["sqlite_quick_check"]:
        raise OfflineBackupError("备份 SQLite 校验结果与 manifest 不一致")
    control = _control_snapshot(copy_roots["scrapeflow_data"])
    if control != expected["control"]:
        raise OfflineBackupError("备份控制状态与 manifest 不一致")
    return {
        "control": control,
        "checks": {
            "copied_stats": copied_stats,
            "json": json_checks,
            "sqlite_quick_check": sqlite_checks,
        },
    }


def verify_offline_backup(backup_dir: str | Path) -> dict[str, object]:
    """Re-run local JSON, SQLite and file count checks for one backup."""
    root = _directory(backup_dir, label="备份目录")
    manifest = _load_manifest(root)
    copy_roots, expected = _validated_manifest(root, manifest)
    verified = _verified_copy_checks(copy_roots, expected)
    return {
        "backup_root": str(root),
        **verified,
    }


def restore_offline_backup(
    *,
    backup_dir: str | Path,
    restore_dir: str | Path,
) -> dict[str, object]:
    """Copy one backup into an isolated restore directory and verify pause."""
    root = _directory(backup_dir, label="备份目录")
    manifest = _load_manifest(root)
    source_roots, expected = _validated_manifest(root, manifest)
    # A failed backup must never be copied into a new isolated runtime.
    _verified_copy_checks(source_roots, expected)
    restore_root = _directory(restore_dir, label="恢复输出父目录", must_exist=False)
    if restore_root == root or _path_is_within(restore_root, root):
        raise OfflineBackupError("恢复目录不能位于备份目录内部")
    if restore_root.exists() and any(restore_root.iterdir()):
        raise OfflineBackupError(f"恢复目录必须为空: {restore_root}")
    restore_root.mkdir(mode=0o700, parents=True, exist_ok=True)
    restored_alist = restore_root / "alist-data"
    restored_scrapeflow = restore_root / "scrapeflow-data"
    try:
        _copy_tree(source_roots["alist_data"], restored_alist)
        _copy_tree(source_roots["scrapeflow_data"], restored_scrapeflow)
        _validate_tree_containment(restored_alist, label="恢复 AList data")
        _validate_tree_containment(restored_scrapeflow, label="恢复 ScrapeFlow data")
        restored = _verified_copy_checks(
            {
                "alist_data": restored_alist,
                "scrapeflow_data": restored_scrapeflow,
            },
            expected,
        )
        return {
            "restore_root": str(restore_root),
            "alist_data": str(restored_alist),
            "scrapeflow_data": str(restored_scrapeflow),
            **restored,
        }
    except Exception:
        shutil.rmtree(restored_alist, ignore_errors=True)
        shutil.rmtree(restored_scrapeflow, ignore_errors=True)
        raise


__all__ = [
    "BACKUP_VERSION",
    "MANIFEST_NAME",
    "OfflineBackupError",
    "create_offline_backup",
    "restore_offline_backup",
    "verify_offline_backup",
]
