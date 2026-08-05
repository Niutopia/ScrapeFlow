"""Exact-path adapter from the scraper's AList client to safe file transactions.

The adapter intentionally exposes no remote ``move`` operation.  Moving a file
is implemented by :mod:`remote_file_transaction` as a locally staged,
content-verified upload followed by deletion of the proven source object.
"""

from __future__ import annotations

from pathlib import Path
from typing import BinaryIO, ContextManager, Mapping, Protocol

from .remote_file_transaction import RemoteFileInfo


class AListExactFileBackend(Protocol):
    """Small structural interface implemented by ``engine.scraper.AListClient``."""

    def exact_file_info(self, path: str) -> Mapping[str, object] | None: ...

    def open_file_reader(self, path: str) -> ContextManager[BinaryIO]: ...

    def upload_file(
        self,
        target_path: str,
        source: Path,
        content_type: str = "application/octet-stream",
    ) -> None: ...

    def mkdir(self, path: str) -> None: ...

    def remove(self, parent: str, names: list[str]) -> None: ...


def _split_remote(path: str) -> tuple[str, str]:
    normalized = "/" + path.strip("/")
    parent, separator, name = normalized.rpartition("/")
    if not separator or not name:
        raise ValueError(f"remote file path must include a basename: {path!r}")
    return parent or "/", name


class AListExactFileAdapter:
    """Implement the transaction protocol without retrying an upload call."""

    def __init__(self, backend: AListExactFileBackend) -> None:
        self._backend = backend

    def stat_exact(self, path: str) -> RemoteFileInfo | None:
        raw = self._backend.exact_file_info(path)
        if raw is None:
            return None
        size = raw.get("size")
        if isinstance(size, bool) or not isinstance(size, int) or size < 0:
            raise ValueError(f"AList exact stat returned an invalid size: {path}")
        sha256 = raw.get("sha256")
        if sha256 is not None and not isinstance(sha256, str):
            raise ValueError(f"AList exact stat returned an invalid sha256: {path}")
        version = raw.get("version")
        if version is not None and not isinstance(version, str):
            version = str(version)
        return RemoteFileInfo(size=size, sha256=sha256, version=version)

    def open_reader(self, path: str) -> ContextManager[BinaryIO]:
        return self._backend.open_file_reader(path)

    def upload_file_once(
        self,
        target_path: str,
        source: Path,
        content_type: str,
    ) -> None:
        # ``AListClient.upload_file`` issues exactly one PUT.  It may reconcile
        # a lost response by observing the exact target, but it never sends a
        # second request.  The transaction layer performs the authoritative
        # full read-back hash before it permits source deletion.
        self._backend.upload_file(target_path, source, content_type)

    def ensure_directory(self, path: str) -> None:
        """Ensure the exact rollback directory exists through AList.

        Directory creation is idempotent in the backend.  The adapter does
        not infer, rename, or select an alternate path.
        """
        if not path.startswith("/") or "//" in path or "/../" in path + "/":
            raise ValueError(f"remote directory must be a normalized absolute path: {path!r}")
        self._backend.mkdir(path)

    def remove_file(self, path: str) -> None:
        parent, name = _split_remote(path)
        self._backend.remove(parent, [name])


__all__ = ["AListExactFileAdapter", "AListExactFileBackend"]
