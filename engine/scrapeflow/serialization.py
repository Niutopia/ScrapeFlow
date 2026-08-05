"""Canonical JSON and crash-safe local artifact writes."""

from __future__ import annotations

import errno
import json
import os
import sys
import uuid
from pathlib import Path
from typing import Any, Mapping

from .errors import ScraperError


def canonical_json_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


_UNSUPPORTED_DIRECTORY_FSYNC_ERRORS = {
    value
    for value in (
        errno.EINVAL,
        getattr(errno, "ENOTSUP", None),
        getattr(errno, "EOPNOTSUPP", None),
    )
    if value is not None
}


def _fsync_directory(path: Path) -> None:
    """Persist one directory entry update when the platform supports it.

    Windows does not expose a portable directory ``fsync`` through ``os``.
    Other non-Linux/non-macOS platforms may report the operation itself as
    unsupported; only those explicit errors are ignored.  Linux and macOS
    always attempt the real directory sync and propagate every failure so a
    caller cannot cross a mutation boundary after an uncertain checkpoint.
    """
    if os.name == "nt":
        return
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    try:
        descriptor = os.open(path, flags)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
    except OSError as exc:
        strictly_supported = sys.platform == "darwin" or sys.platform.startswith("linux")
        if not strictly_supported and exc.errno in _UNSUPPORTED_DIRECTORY_FSYNC_ERRORS:
            return
        raise


def atomic_write_bytes(path: Path, payload: bytes, *, mode: int = 0o600) -> None:
    """Durably replace ``path`` without ever exposing a partial payload."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    descriptor = os.open(temporary, os.O_CREAT | os.O_EXCL | os.O_WRONLY, mode)
    try:
        try:
            handle = os.fdopen(descriptor, "wb")
        except BaseException:
            os.close(descriptor)
            raise
        with handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        _fsync_directory(path.parent)
    finally:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass


def atomic_write_json(
    path: Path,
    payload: Any,
    *,
    ensure_ascii: bool = False,
    indent: int | None = 2,
    sort_keys: bool = False,
    separators: tuple[str, str] | None = None,
    allow_nan: bool = True,
    trailing_newline: bool = True,
    mode: int = 0o600,
) -> None:
    """Serialize JSON, then durably publish it with owner-only permissions."""
    encoded = json.dumps(
        payload,
        ensure_ascii=ensure_ascii,
        indent=indent,
        sort_keys=sort_keys,
        separators=separators,
        allow_nan=allow_nan,
    )
    if trailing_newline:
        encoded += "\n"
    atomic_write_bytes(path, encoded.encode("utf-8"), mode=mode)


def reserve_output_path(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        descriptor = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    except FileExistsError as exc:
        raise ScraperError(f"本地输出文件已存在，拒绝覆盖: {path}") from exc
    try:
        try:
            # ``os.open`` is unbuffered, so there is no userspace flush to do
            # for the zero-byte reservation before syncing its inode.
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        _fsync_directory(path.parent)
    except BaseException:
        # A failed reservation must not leave a collision marker that looks
        # successfully committed to a later process.
        try:
            path.unlink()
        except OSError:
            pass
        raise


def write_json_reserved(path: Path, payload: Mapping[str, Any]) -> None:
    atomic_write_json(path, payload, allow_nan=False)
