"""Read-only AList inventory and automatic follow-up task discovery."""

from __future__ import annotations

import copy
import inspect
import json
import math
import os
import posixpath
import re
import shutil
import subprocess
import threading
import time
import xml.etree.ElementTree as ET
from collections import defaultdict
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from datetime import UTC, date, datetime
from pathlib import Path, PurePosixPath
from typing import Any, Callable, Mapping, Protocol, Sequence

from engine.scrapeflow.serialization import atomic_write_json
from local.scrapeflow_api.content_identity_overrides import (
    apply_content_identity_overrides,
)


DEFAULT_FORMAL_LIBRARY_ROOTS = (
    "/quark/影视/电影",
    "/quark/影视/番剧",
    "/quark/影视/美剧",
)
VIDEO_SUFFIXES = frozenset({
    ".mkv", ".mp4", ".m4v", ".m2ts", ".ts", ".avi", ".mov", ".webm", ".wmv", ".iso",
})
SUBTITLE_SUFFIXES = frozenset({".ass", ".ssa", ".srt", ".vtt", ".idx", ".sub", ".sup", ".mks"})
POSTER_SUFFIXES = frozenset({".jpg", ".jpeg", ".png", ".webp", ".gif"})
TEMPORARY_SUFFIXES = frozenset({
    ".part", ".partial", ".tmp", ".temp", ".crdownload", ".aria2", ".download", ".!qb",
})

# These expressions intentionally cover only explicit episode notation.  A
# library audit must never guess that an arbitrary number in a filename is an
# episode (that would create a false acquisition request).  ``Season N`` is
# accepted as context for the common ``01.mkv``/``E01.mkv`` form.
_SEASON_EPISODE_RE = re.compile(
    r"(?<![A-Z0-9])S0*(?P<season>\d{1,3})[ ._-]*E0*(?P<episode>\d{1,4})(?!\d)",
    re.IGNORECASE,
)
_SEASON_EPISODE_RANGE_RE = re.compile(
    r"(?<![A-Z0-9])S0*(?P<season>\d{1,3})[ ._-]*E0*(?P<start>\d{1,4})"
    r"\s*(?:-|–|—|~|～)\s*"
    r"(?:S0*(?P<end_season>\d{1,3})[ ._-]*)?E0*(?P<end>\d{1,4})(?!\d)",
    re.IGNORECASE,
)
_EPISODE_ONLY_RE = re.compile(
    r"(?<![A-Z0-9])E(?:P)?0*(?P<episode>\d{1,4})(?!\d)",
    re.IGNORECASE,
)
_EPISODE_RANGE_RE = re.compile(
    r"(?<![A-Z0-9])E(?:P)?0*(?P<start>\d{1,4})"
    r"\s*(?:-|–|—|~|～)\s*E(?:P)?0*(?P<end>\d{1,4})(?!\d)",
    re.IGNORECASE,
)
# A range must be explicit at both ends and remain small.  This handles the
# common double-episode release while refusing a malformed/batch filename
# such as ``S01E01-E9999`` from claiming an entire season.
_MAX_EXPLICIT_EPISODE_RANGE = 24
_SEASON_DIR_RE = re.compile(r"^(?:season|s)\s*0*(\d{1,3})$", re.IGNORECASE)
_BARE_EPISODE_RE = re.compile(r"(?:^|[ ._\[(?-])0*(\d{1,4})(?:\]|$)")
_DATE_RE = re.compile(r"^(\d{4})-(\d{2})-(\d{2})$")
_MOVIE_DIRECTORY_RE = re.compile(r"^(?P<title>.+?)\s*\((?P<year>[^()]{1,32})\)$")
_ANCILLARY_VIDEO_RE = re.compile(
    r"(?:^|[ ._\-\[\]()])(?:"
    r"trailer\d*|behind(?:[ ._\-]?the)?[ ._\-]?scenes\d*"
    r")(?:$|[ ._\-\[\]()])",
    re.IGNORECASE,
)
_NFO_MAX_BYTES = 1024 * 1024
_SUBTITLE_PROBE_DEFAULT_BYTES = 8 * 1024 * 1024
_SUBTITLE_PROBE_MAX_BYTES = 64 * 1024 * 1024
_SUBTITLE_PROBE_DEFAULT_TIMEOUT = 15
_SUBTITLE_PROBE_DEFAULT_WORKERS = 4
_SUBTITLE_PROBE_DEFAULT_MAX_FILES = 2048
# A full scan should make visible progress through a large subtitle evidence
# backlog without turning every audit into a provider-wide ffprobe storm.  The
# persistent checkpoint below advances this bounded window on each audit.
_SUBTITLE_PROBE_DEFAULT_BATCH_SIZE = 75
_SUBTITLE_PROBE_MAX_BATCH_SIZE = 100
# A full inventory can contain thousands of videos.  Signed-link/ffprobe
# evidence is deliberately a best-effort lane: the audit must return a
# deterministic report instead of waiting for every provider timeout.  The
# budget is per checker (normally one checker per full-library audit), not per
# file.  A zero value is useful for operators who want a sidecar-only pass.
_SUBTITLE_PROBE_DEFAULT_BUDGET_SECONDS = 120.0
_SUBTITLE_PROBE_MAX_BUDGET_SECONDS = 900.0

# Subtitle evidence is intentionally a tiny, local-only ledger.  It never
# retains a signed URL, provider header, ffprobe command line, or stream title:
# the file identity and a compact verdict are enough to make later audits both
# safe and useful.
_SUBTITLE_EVIDENCE_LEDGER_SCHEMA_VERSION = 1
_SUBTITLE_EVIDENCE_LEDGER_KIND = "subtitle_evidence_ledger"
_SUBTITLE_EVIDENCE_LEDGER_FILENAME = "subtitle-evidence-ledger.json"
_SUBTITLE_EVIDENCE_STATUSES = frozenset({"satisfied", "missing", "unknown"})
_SUBTITLE_EVIDENCE_DEFINITIVE_STATUSES = frozenset({"satisfied", "missing"})
_SUBTITLE_EVIDENCE_SOURCES = frozenset({
    "embedded",
    "embedded_complete_mkv_prefix",
})
_SUBTITLE_EVIDENCE_REASONS = frozenset({
    "alist_file_link_unavailable",
    "ffprobe_nonzero_exit",
    "ffprobe_not_installed",
    "ffprobe_output_too_large",
    "ffprobe_timeout",
    "invalid_alist_file_link",
    "invalid_ffprobe_output",
    "subtitle_evidence_unavailable",
    "subtitle_probe_batch_deferred",
    "subtitle_probe_budget_exhausted",
    "subtitle_probe_error",
    "subtitle_probe_untrusted_evidence",
    "unsafe_provider_headers",
})


class AListDirectoryLister(Protocol):
    def list(self, path: str, refresh: bool = False) -> list[Mapping[str, object]]: ...


class SimpleLibraryAuditError(RuntimeError):
    """The visible AList tree cannot be represented as a complete inventory."""


class SimpleLibraryAuditUnavailable(SimpleLibraryAuditError):
    """The client does not currently provide a usable directory listing."""


def _utc_now() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def _tmdb_audit_workers() -> int:
    """Return a conservative per-audit TMDB prefetch limit."""
    raw = os.getenv("SCRAPEFLOW_TMDB_AUDIT_WORKERS", "4").strip()
    try:
        requested = int(raw)
    except ValueError:
        requested = 4
    return max(1, min(4, requested))


_TMDB_AUDIT_DEFAULT_BUDGET_SECONDS = 120.0
_TMDB_AUDIT_MAX_BUDGET_SECONDS = 900.0


def _tmdb_audit_budget_seconds() -> float:
    """Return the bounded wall-clock budget for one TMDB audit prefetch.

    TMDB is advisory evidence for an audit, so a provider that stalls must not
    hold the whole-library scan open indefinitely.  A floating-point value is
    accepted to make short budgets useful in tests; non-finite values fall
    back to the safe default.  ``0`` deliberately disables remote warm-up and
    makes every unresolved identity fail closed as an unknown catalog.
    """
    raw = os.getenv("SCRAPEFLOW_TMDB_AUDIT_BUDGET_SECONDS", "").strip()
    try:
        value = float(raw) if raw else _TMDB_AUDIT_DEFAULT_BUDGET_SECONDS
    except (TypeError, ValueError):
        value = _TMDB_AUDIT_DEFAULT_BUDGET_SECONDS
    if not math.isfinite(value):
        value = _TMDB_AUDIT_DEFAULT_BUDGET_SECONDS
    return max(0.0, min(_TMDB_AUDIT_MAX_BUDGET_SECONDS, value))


def _root(value: object) -> str:
    if not isinstance(value, str) or not value.startswith("/") or "\\" in value or "\x00" in value:
        raise ValueError("formal roots must be absolute slash-separated paths")
    if any(part in {".", ".."} for part in value.split("/")[1:]):
        raise ValueError("formal roots must not contain dot segments")
    normalized = posixpath.normpath(value)
    if not normalized.startswith("/"):
        raise ValueError("formal roots must be absolute paths")
    return normalized.rstrip("/") or "/"


def _inside(path: str, root: str) -> bool:
    return path == root or path.startswith(root.rstrip("/") + "/")


def _child(parent: str, name: object) -> str:
    if (
        not isinstance(name, str) or not name or name in {".", ".."}
        or "/" in name or "\\" in name or "\x00" in name
    ):
        raise SimpleLibraryAuditError(f"unsafe AList entry name under {parent}")
    path = posixpath.normpath(posixpath.join(parent, name))
    if not path.startswith(parent.rstrip("/") + "/"):
        raise SimpleLibraryAuditError(f"AList entry escaped {parent}")
    return path


def _suffix(path: str) -> str:
    return PurePosixPath(path).suffix.casefold()


def _safe_file_version_token(value: object) -> str | None:
    """Return one bounded, non-secret provider version token."""
    if value in (None, "") or isinstance(value, bool):
        return None
    if isinstance(value, (str, int, float)):
        text = str(value)
        if (
            text
            and len(text) <= 512
            and "\x00" not in text
            and "\r" not in text
            and "\n" not in text
        ):
            return text
    return None


def _inventory_file_version(entry: Mapping[str, object]) -> str | None:
    """Return a bounded, non-secret AList file version token when supplied.

    AList providers normally expose one of these timestamp-like fields in a
    directory entry.  It is deliberately kept exact rather than parsed: a
    ledger may only reuse evidence for the very same provider version.  An
    absent or malformed value is not guessed; such a file is still probed,
    but receives no durable cache reuse.
    """
    for key in ("modified", "updated_at", "mtime", "last_modified"):
        version = _safe_file_version_token(entry.get(key))
        if version is not None:
            return version
    return None


def _kind(path: str) -> str | None:
    suffix = _suffix(path)
    if suffix in VIDEO_SUFFIXES:
        return "video"
    if suffix in SUBTITLE_SUFFIXES:
        return "subtitle"
    if suffix == ".nfo":
        return "nfo"
    pure = PurePosixPath(path)
    if suffix in POSTER_SUFFIXES and (
        pure.stem.casefold() in {"poster", "folder", "cover"}
        or pure.stem.casefold().endswith("-poster")
    ):
        return "poster"
    return None


def _is_episode_nfo(path: str) -> bool:
    """Return whether an NFO basename carries an explicit episode token.

    Episode sidecars are useful evidence for a particular file, but they are
    not a work-level TV identity.  Treating one as the directory's primary
    NFO would hide an inherited ``tvshow.nfo`` and make an otherwise scoped
    season look unknown.  Only explicit SxxEyy/Eyy notation is accepted here;
    a bare number remains an explicit movie-sidecar boundary.
    """
    stem = PurePosixPath(path).stem
    return any(pattern.search(stem) for pattern in (
        _SEASON_EPISODE_RANGE_RE,
        _SEASON_EPISODE_RE,
        _EPISODE_RANGE_RE,
        _EPISODE_ONLY_RE,
    ))


def _accepts_refresh(listing: Callable[..., object]) -> bool:
    """Support a small ``list(path)`` fake without retrying client failures."""
    try:
        parameters = inspect.signature(listing).parameters.values()
    except (TypeError, ValueError):
        return True
    return any(
        parameter.name == "refresh" or parameter.kind is inspect.Parameter.VAR_KEYWORD
        for parameter in parameters
    )


def latest_audit_path(state_root: str | Path) -> Path:
    return Path(state_root) / "library-audit" / "latest.json"


class SimpleLibraryAuditor:
    """Build an inspectable recursive inventory using only ``client.list``."""

    def __init__(
        self,
        client: AListDirectoryLister | object | None,
        *,
        formal_roots: Sequence[str] = DEFAULT_FORMAL_LIBRARY_ROOTS,
        max_directories: int = 20_000,
        max_files: int = 250_000,
        clock: Callable[[], str] | None = None,
    ) -> None:
        roots = tuple(_root(value) for value in formal_roots)
        if not roots or len(set(roots)) != len(roots):
            raise ValueError("formal roots must be non-empty and unique")
        if any(left != right and _inside(right, left) for left in roots for right in roots):
            raise ValueError("formal roots must not overlap")
        if type(max_directories) is not int or max_directories < 1:
            raise ValueError("max_directories must be a positive integer")
        if type(max_files) is not int or max_files < 1:
            raise ValueError("max_files must be a positive integer")
        self.client, self.roots = client, roots
        self.max_directories, self.max_files = max_directories, max_files
        self.clock = clock or _utc_now

    def run(self, output_path: str | Path) -> dict[str, object]:
        """Scan and atomically replace the current report file."""
        report = self.scan()
        atomic_write_json(Path(output_path), report, allow_nan=False)
        return report

    def scan(self) -> dict[str, object]:
        report = self._new_report(self.clock())
        listing = getattr(self.client, "list", None) if self.client is not None else None
        if not callable(listing):
            self._client_failure(report, "unavailable", "directory_listing_unavailable")
            return self._finish(report)
        try:
            login = getattr(self.client, "login", None)
            if callable(login) and not getattr(self.client, "token", None):
                login()
        except Exception as exc:
            self._client_failure(report, "error", "authentication_failed", type(exc).__name__)
            return self._finish(report)

        for root_state in report["roots"]:
            root = root_state["path"]
            try:
                self._scan_root(root, root_state, report, listing)
                root_state["status"] = "completed"
            except SimpleLibraryAuditUnavailable as exc:
                root_state.update(status="unavailable", error=type(exc).__name__)
                self._error(report, "root", "directory_listing_unavailable", root, type(exc).__name__)
            except Exception as exc:
                root_state.update(status="error", error=type(exc).__name__)
                self._error(report, "root", "inventory_failed", root, type(exc).__name__)

        statuses = [row["status"] for row in report["roots"]]
        if all(status == "completed" for status in statuses):
            report.update(status="completed", available=True, complete=True)
            self._findings(report)
            report["clean"] = not report["automatic_tasks"]
        elif "unavailable" in statuses:
            report["status"] = "unavailable"
        else:
            report["status"] = "error"
        return self._finish(report)

    def _new_report(self, started_at: str) -> dict[str, Any]:
        return {
            "schema_version": 1,
            "kind": "library_audit",
            "started_at": started_at,
            "finished_at": None,
            "status": "unavailable",
            "available": False,
            "complete": False,
            "clean": None,
            "roots": [
                {"path": root, "status": "pending", "file_count": 0, "directory_count": 0}
                for root in self.roots
            ],
            "counts": {"files": 0, "directories": 0, "videos": 0, "subtitles": 0, "nfo": 0, "posters": 0},
            "inventory": [],
            "zero_byte_files": [],
            "temporary_entries": [],
            "duplicates": [],
            "empty_directories": [],
            "observations": {
                "video_files": [], "subtitle_files": [], "nfo_files": [], "poster_files": [],
                "media_directories": [], "ancillary_media": [],
            },
            "automatic_tasks": [],
            "errors": [],
        }

    def _finish(self, report: dict[str, Any]) -> dict[str, object]:
        report["finished_at"] = self.clock()
        return report

    @staticmethod
    def _error(
        report: dict[str, Any],
        scope: str,
        code: str,
        path: str | None = None,
        error_type: str | None = None,
    ) -> None:
        row: dict[str, object] = {"scope": scope, "code": code}
        if path is not None:
            row["path"] = path
        if error_type is not None:
            row["error_type"] = error_type
        report["errors"].append(row)

    def _client_failure(
        self, report: dict[str, Any], status: str, code: str, error_type: str | None = None
    ) -> None:
        report["status"] = status
        self._error(report, "client", code, error_type=error_type)
        for root in report["roots"]:
            root["status"] = status

    def _list(self, listing: Callable[..., object], path: str) -> list[Mapping[str, object]]:
        rows = listing(path, refresh=True) if _accepts_refresh(listing) else listing(path)
        if rows is None:
            raise SimpleLibraryAuditUnavailable(f"AList listing unavailable: {path}")
        if not isinstance(rows, list) or not all(isinstance(row, Mapping) for row in rows):
            raise SimpleLibraryAuditError(f"invalid AList listing: {path}")
        return rows

    def _scan_root(
        self,
        root: str,
        root_state: dict[str, Any],
        report: dict[str, Any],
        listing: Callable[..., object],
    ) -> None:
        stack, visited = [root], set()
        while stack:
            current = stack.pop()
            if current in visited:
                raise SimpleLibraryAuditError(f"repeated AList directory: {current}")
            if report["counts"]["directories"] >= self.max_directories:
                raise SimpleLibraryAuditError("directory limit reached")
            visited.add(current)
            report["inventory"].append({"path": current, "type": "directory"})
            report["counts"]["directories"] += 1
            root_state["directory_count"] += 1
            names: set[str] = set()
            for raw in self._list(listing, current):
                name = raw.get("name")
                path = _child(current, name)
                if not _inside(path, root):
                    raise SimpleLibraryAuditError(f"AList entry escaped formal root: {path}")
                if str(name).casefold() in names:
                    raise SimpleLibraryAuditError(f"AList directory has a name collision: {current}")
                names.add(str(name).casefold())
                if raw.get("is_symlink") is True or raw.get("type") in {"symlink", "link"}:
                    raise SimpleLibraryAuditError(f"AList symlink is not auditable: {path}")
                if raw.get("is_dir") is True:
                    stack.append(path)
                    continue
                size = raw.get("size")
                if type(size) is not int or size < 0:
                    raise SimpleLibraryAuditError(f"invalid AList file size: {path}")
                if report["counts"]["files"] >= self.max_files:
                    raise SimpleLibraryAuditError("file limit reached")
                row = {"path": path, "type": "file", "size": size}
                version = _inventory_file_version(raw)
                if version is not None:
                    row["version"] = version
                report["inventory"].append(row)
                report["counts"]["files"] += 1
                root_state["file_count"] += 1
                kind = _kind(path)
                if kind:
                    key = "nfo" if kind == "nfo" else f"{kind}s"
                    report["counts"][key] += 1
                    report["observations"][f"{kind}_files"].append(dict(row))

    def _findings(self, report: dict[str, Any]) -> None:
        inventory = report["inventory"]
        directories = [row["path"] for row in inventory if row["type"] == "directory"]
        files = [row for row in inventory if row["type"] == "file"]
        children = {path: 0 for path in directories}
        files_by_parent: dict[str, list[dict[str, Any]]] = defaultdict(list)
        duplicates: dict[tuple[str, int], list[str]] = defaultdict(list)
        for row in inventory:
            path = row["path"]
            parent = posixpath.dirname(path) or "/"
            if parent in children:
                children[parent] += 1
            if row["type"] == "file":
                files_by_parent[parent].append(row)
                duplicates[(PurePosixPath(path).name, row["size"])].append(path)

        report["inventory"] = sorted(inventory, key=lambda row: row["path"].casefold())
        report["zero_byte_files"] = sorted(
            (dict(row) for row in files if row["size"] == 0), key=lambda row: row["path"].casefold()
        )
        report["temporary_entries"] = sorted((
            {"path": row["path"], "type": row["type"], "suffix": _suffix(row["path"])}
            for row in inventory if _suffix(row["path"]) in TEMPORARY_SUFFIXES
        ), key=lambda row: row["path"].casefold())
        report["duplicates"] = sorted((
            {"basename": name, "size": size, "paths": sorted(paths, key=str.casefold)}
            for (name, size), paths in duplicates.items() if len(paths) > 1
        ), key=lambda row: (row["basename"].casefold(), row["size"]))
        report["empty_directories"] = sorted(
            (path for path, count in children.items() if count == 0), key=str.casefold
        )

        media_dirs: list[dict[str, object]] = []
        tv_roots = tuple(
            root for root in self.roots
            if PurePosixPath(root).name.casefold() in {"番剧", "美剧", "tv", "anime", "series"}
        )
        for path, rows in files_by_parent.items():
            kinds = [_kind(row["path"]) for row in rows]
            video_count = sum(kind == "video" for kind in kinds)
            if video_count:
                subtitles = sum(kind == "subtitle" for kind in kinds)
                posters = sum(kind == "poster" for kind in kinds)
                # Episode NFOs describe one media member, not the work-level
                # identity of this directory.  Exclude them from the local
                # metadata count so a season can inherit its nearest
                # ``tvshow.nfo``.  Explicit movie NFOs remain boundaries.
                work_nfo_rows = [
                    row for row in rows
                    if _kind(row["path"]) == "nfo"
                    and not _is_episode_nfo(str(row["path"]))
                ]
                nfo_path = next(
                    (
                        row["path"] for row in work_nfo_rows
                        if PurePosixPath(row["path"]).name.casefold() == "tvshow.nfo"
                    ),
                    None,
                )
                if nfo_path is None and work_nfo_rows:
                    nfo_path = work_nfo_rows[0]["path"]
                nfo = len(work_nfo_rows)
                poster_path = next(
                    (row["path"] for row in rows if _kind(row["path"]) == "poster"), None
                )
                inherited_nfo, inherited_poster, inherited_from = (
                    self._tv_ancestor_observations(path, files_by_parent, tv_roots)
                    if nfo_path is None or poster_path is None
                    else (None, None, None)
                )
                if nfo_path is None and inherited_nfo is not None:
                    nfo_path, nfo_inherited = inherited_nfo, True
                    nfo += 1
                else:
                    nfo_inherited = False
                if poster_path is None and inherited_poster is not None:
                    poster_path, poster_inherited = inherited_poster, True
                    posters += 1
                else:
                    poster_inherited = False
                media_dirs.append({
                    "path": path, "video_count": video_count, "subtitle_count": subtitles,
                    "nfo_count": nfo, "poster_count": posters, "has_subtitle": bool(subtitles),
                    "has_nfo": bool(nfo), "has_poster": bool(posters),
                    "nfo_path": nfo_path, "poster_path": poster_path,
                    "nfo_inherited": nfo_inherited, "poster_inherited": poster_inherited,
                    # A poster may be inherited even when this directory has
                    # its own (possibly movie) NFO.  Only an inherited NFO is
                    # evidence that a TV ancestor owns the media directory;
                    # otherwise a parent show could claim a sibling movie.
                    "metadata_source": inherited_from if nfo_inherited else None,
                })
        report["observations"]["media_directories"] = sorted(media_dirs, key=lambda row: str(row["path"]).casefold())

        # Keep the inventory evidence separate from machine-actionable work.
        # A same-name/same-size collision is not an identity proof (it is very
        # commonly a repeated ``tvshow.nfo``/poster filename across works),
        # and an empty directory can be a deliberate Season 00 placeholder or
        # an unfinished import.  Neither observation authorizes a remote
        # delete/move or should make an otherwise complete scan ``clean``.
        # They remain available in ``duplicates`` and ``empty_directories``
        # for an operator to inspect explicitly.
        tasks: list[dict[str, object]] = []
        for row in report["zero_byte_files"]:
            tasks.append({"kind": "zero_byte_file", "path": row["path"], "task": "重新传输或删除空文件"})
        for row in report["temporary_entries"]:
            tasks.append({"kind": "temporary_entry", "path": row["path"], "task": "在相关任务结束后自动清理临时条目"})
        for row in report["observations"]["media_directories"]:
            if not row["has_nfo"]:
                tasks.append({"kind": "missing_nfo", "path": row["path"], "task": "自动补齐该视频目录的 NFO 元数据"})
            if not row["has_poster"]:
                tasks.append({"kind": "missing_poster", "path": row["path"], "task": "自动补齐该视频目录的海报"})
        report["automatic_tasks"] = sorted(
            tasks,
            key=lambda row: (str(row["kind"]).casefold(), str(row.get("path", row.get("basename", ""))).casefold()),
        )

    @staticmethod
    def _tv_ancestor_observations(
        path: str,
        files_by_parent: Mapping[str, list[dict[str, Any]]],
        tv_roots: Sequence[str],
    ) -> tuple[str | None, str | None, str | None]:
        """Return the nearest ancestor work tree's tvshow/NFO and poster."""
        matching_root = next((root for root in tv_roots if _inside(path, root)), None)
        if matching_root is None:
            return None, None, None
        current = posixpath.dirname(path)
        while current != matching_root and _inside(current, matching_root):
            rows = files_by_parent.get(current, [])
            tvshow = next(
                (
                    row["path"] for row in rows
                    if PurePosixPath(row["path"]).name.casefold() == "tvshow.nfo"
                ),
                None,
            )
            if tvshow is not None:
                poster = next(
                    (row["path"] for row in rows if _kind(row["path"]) == "poster"),
                    None,
                )
                return tvshow, poster, current
            current = posixpath.dirname(current)
        return None, None, None


# ---------------------------------------------------------------------------
# Automatic semantic gap discovery
# ---------------------------------------------------------------------------

def _canonical_optional_path(value: object) -> str | None:
    """Return a safe absolute path, or None for an absent value."""
    if not isinstance(value, str) or not value.startswith("/") or "\\" in value or "\x00" in value:
        return None
    normalized = posixpath.normpath(value)
    if normalized != value or any(part in {"", ".", ".."} for part in normalized.split("/")[1:]):
        return None
    return normalized


def _as_positive_int(value: object) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int) and value > 0:
        return value
    if isinstance(value, str) and value.isdecimal() and int(value) > 0:
        return int(value)
    return None


def _work_metadata(raw: Mapping[str, object]) -> dict[str, object]:
    """Flatten common Engine identity/plan metadata shapes."""
    metadata: dict[str, object] = {}
    nested = raw.get("metadata")
    if isinstance(nested, Mapping):
        metadata.update(nested)
    identity = raw.get("identity")
    if isinstance(identity, Mapping):
        metadata.update(identity)
    metadata.update({key: value for key, value in raw.items() if key not in {"metadata", "identity"}})
    return metadata


def _job_mapping(raw_job: object) -> Mapping[str, object] | None:
    if isinstance(raw_job, Mapping):
        return raw_job
    as_dict = getattr(raw_job, "as_dict", None)
    candidate = as_dict() if callable(as_dict) else None
    return candidate if isinstance(candidate, Mapping) else None


def _engine_work(
    *,
    target_root: object,
    media_type: object,
    identity: Mapping[str, object],
    metadata: Mapping[str, object],
    job_id: object,
    audit_owned: bool = False,
) -> dict[str, object] | None:
    """Project one persisted Engine identity into an audit work."""
    target = _canonical_optional_path(target_root)
    normalized_type = str(media_type or "").casefold()
    if normalized_type == "mixed":
        normalized_type = "tv"
    tmdb_id = _as_positive_int(identity.get("tmdb_id") or metadata.get("tmdb_id"))
    if tmdb_id is None or target is None or normalized_type not in {"movie", "tv"}:
        return None
    season = identity.get("season")
    if season is None:
        season = metadata.get("season")
    source: dict[str, object] = {
        "tmdb_id": tmdb_id,
        "title": identity.get("title") or metadata.get("title"),
        "original_title": identity.get("original_title") or metadata.get("original_title"),
        "year": identity.get("year") or metadata.get("year"),
        "target_root": target,
        "media_type": normalized_type,
        "season": season,
        "media_format": metadata.get("media_format") or metadata.get("format"),
        "aliases": metadata.get("aliases"),
        "identity_source": "engine_job",
    }
    if isinstance(job_id, str) and job_id.strip():
        source["owner_job_id"] = job_id.strip()
    if audit_owned:
        # Audit-owned roots are a local projection of current semantic gaps,
        # not an historical target-tree claim.  Preserve that distinction so
        # the legacy-scope guard below cannot alter their lifecycle.
        source["audit_owned"] = True
    # A plan can carry usable TMDB episode data from a prior automatic lookup.
    # Keep it, but do not require it; TmdbEpisodeCatalog can fill the gap on a
    # later full-library scan.
    for name in (
        "expected_episodes", "season_episodes", "official_episodes",
        "seasons", "official_seasons",
    ):
        if name in identity:
            source[name] = identity[name]
        elif name in metadata:
            source[name] = metadata[name]
    return source


def automatic_works_from_engine_jobs(jobs: Sequence[object]) -> list[dict[str, object]]:
    """Extract known formal-library works from completed automatic Engine jobs.

    This intentionally accepts either EngineJob objects or their JSON-shaped
    dictionaries. It only reads persisted job facts. A missing identity or
    target is omitted so an incomplete task cannot invent a semantic gap. Batch
    plans are expanded from their explicit member identities rather than being
    treated as one synthetic work.
    """
    output: list[dict[str, object]] = []
    seen: set[tuple[int, str, str]] = set()

    def append(work: dict[str, object] | None) -> None:
        if work is None:
            return
        key = (
            int(work["tmdb_id"]),
            str(work["target_root"]),
            str(work["media_type"]),
        )
        if key not in seen:
            seen.add(key)
            output.append(work)

    for raw_job in jobs:
        job = _job_mapping(raw_job)
        if job is None or str(job.get("phase") or "") not in {"executed", "completed"}:
            continue
        plan = job.get("plan") if isinstance(job.get("plan"), Mapping) else {}
        summary = job.get("summary") if isinstance(job.get("summary"), Mapping) else {}
        # Provider child records are implementation details of a root job.
        # They must not register a second formal-library work identity.
        if summary.get("internal_child") is True:
            continue
        identity = summary.get("identity") if isinstance(summary.get("identity"), Mapping) else {}
        metadata = plan.get("metadata") if isinstance(plan.get("metadata"), Mapping) else {}
        job_id = job.get("id")
        append(_engine_work(
            target_root=(metadata.get("series_root") or plan.get("target_root") or identity.get("target_root")),
            media_type=identity.get("media_type") or plan.get("mode"),
            identity=identity,
            metadata=metadata,
            job_id=job_id,
            audit_owned=summary.get("audit_owned") is True,
        ))
        for member_field, member_type in (("member_tv", "tv"), ("member_movies", "movie")):
            members = metadata.get(member_field)
            if not isinstance(members, Mapping):
                continue
            for member_root, raw_identity in members.items():
                if not isinstance(raw_identity, Mapping):
                    continue
                append(_engine_work(
                    target_root=member_root,
                    media_type=member_type,
                    identity=raw_identity,
                    metadata=raw_identity,
                    job_id=job_id,
                    audit_owned=summary.get("audit_owned") is True,
                ))
    return output


def _xml_tag_name(element: ET.Element) -> str:
    return str(element.tag).rsplit("}", 1)[-1].casefold()


def _compact_nfo_text(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    text = re.sub(r"\s+", " ", value).strip()
    if not text or any(ord(char) < 32 for char in text):
        return None
    return text[:240]


def _nfo_child_text(root: ET.Element, *names: str) -> str | None:
    wanted = {name.casefold() for name in names}
    for element in root:
        if _xml_tag_name(element) in wanted:
            text = _compact_nfo_text(element.text)
            if text is not None:
                return text
    return None


def _read_nfo_identity(client: object | None, path: str) -> dict[str, object] | None:
    """Read one bounded metadata sidecar without treating it as trusted input."""
    reader = getattr(client, "read_file_bytes", None) if client is not None else None
    if not callable(reader):
        return None
    try:
        payload = reader(path, max_bytes=_NFO_MAX_BYTES)
    except Exception:
        return None
    if not isinstance(payload, (bytes, bytearray)) or len(payload) > _NFO_MAX_BYTES:
        return None
    raw = bytes(payload)
    lowered = raw.lower()
    # NFO is an untrusted remote artifact. Refuse XML declarations that can
    # trigger entity expansion before handing it to the XML parser.
    if b"<!doctype" in lowered or b"<!entity" in lowered:
        return None
    try:
        root = ET.fromstring(raw)
    except (ET.ParseError, ValueError, UnicodeError):
        return None
    root_tag = _xml_tag_name(root)
    media_type = {"movie": "movie", "tvshow": "tv"}.get(root_tag)
    if media_type is None:
        return None
    tmdb_ids: set[int] = set()
    for element in root.iter():
        tag = _xml_tag_name(element)
        is_tmdb_unique = (
            tag == "uniqueid"
            and str(element.attrib.get("type", "")).casefold() == "tmdb"
        )
        if tag != "tmdbid" and not is_tmdb_unique:
            continue
        tmdb_id = _as_positive_int((element.text or "").strip())
        if tmdb_id is not None:
            tmdb_ids.add(tmdb_id)
    if len(tmdb_ids) != 1:
        return None
    year_text = _nfo_child_text(root, "year", "premiered", "releasedate")
    year_match = re.search(r"(?:18|19|20)\d{2}", year_text or "")
    return {
        "tmdb_id": next(iter(tmdb_ids)),
        "media_type": media_type,
        "title": _nfo_child_text(root, "title", "name"),
        "original_title": _nfo_child_text(root, "originaltitle", "original_title"),
        "year": year_match.group(0) if year_match else None,
    }


def _formal_library_types(formal_roots: Sequence[str]) -> dict[str, str]:
    """Map the supported movie/TV shelves without inferring from file names."""
    roots = tuple(_root(value) for value in formal_roots)
    output: dict[str, str] = {}
    for index, root in enumerate(roots):
        leaf = PurePosixPath(root).name.casefold()
        if leaf in {"电影", "movie", "movies", "film", "films"}:
            output[root] = "movie"
        elif leaf in {"番剧", "美剧", "剧集", "电视", "tv", "anime", "series", "shows"}:
            output[root] = "tv"
        else:
            # The public default is ordered movie, anime, US-TV. Preserve that
            # convention for custom roots whose labels are not English/Chinese.
            output[root] = "movie" if index == 0 else "tv"
    return output


def _directory_identity_fields(target_root: str, media_type: str) -> dict[str, object]:
    """Use the normal work-leaf spelling only as display metadata, never as an ID."""
    leaf = PurePosixPath(target_root).name.strip()
    if not leaf:
        return {}
    if media_type == "movie":
        match = _MOVIE_DIRECTORY_RE.fullmatch(leaf)
        if match:
            return {
                "title": match.group("title").strip(),
                "year": match.group("year").strip(),
            }
    return {"title": leaf}


def bootstrap_automatic_works_from_library(
    report: Mapping[str, object],
    client: AListDirectoryLister | object | None,
    *,
    formal_roots: Sequence[str] = DEFAULT_FORMAL_LIBRARY_ROOTS,
) -> list[dict[str, object]]:
    """Project library NFO identities into fail-closed read-only audit works.

    A path is not, by itself, a work boundary.  In particular, a TV shelf may
    contain movie extras beside a show's ``tvshow.nfo``, and a single bundle
    directory may contain several films.  The bootstrap therefore records an
    explicit ``identity_scope`` for every accepted NFO:

    * a movie NFO owns exactly one same-stem sibling video;
    * a TV ``tvshow.nfo`` owns only media directories whose structural
      ``metadata_source`` is that exact show directory (the nearest ancestor
      rule already stops at nested ``tvshow.nfo`` files);
    * a movie NFO with no sibling video can describe an empty, single-movie
      directory, which is retained solely so the semantic pass can report a
      safe ``missing_media`` gap.

    An NFO identity that cannot be assigned safely is preserved with an
    explicit ambiguous scope, rather than silently dropped.  That keeps the
    audit fail-closed: an empty or mixed directory must never make the formal
    library appear complete merely because it contains no video rows to feed
    the ordinary unknown-work aggregation.  This function never creates an
    Engine task or writes to AList.
    """
    if report.get("complete") is not True or report.get("status") != "completed":
        return []
    inventory = report.get("inventory")
    if not isinstance(inventory, list):
        return []
    root_types = _formal_library_types(formal_roots)
    candidates: list[dict[str, object]] = []
    media_dirs_raw = (
        report.get("observations", {}).get("media_directories", [])
        if isinstance(report.get("observations"), Mapping)
        else []
    )
    media_dirs = (
        [row for row in media_dirs_raw if isinstance(row, Mapping)]
        if isinstance(media_dirs_raw, list)
        else []
    )
    videos_by_parent: dict[str, list[str]] = defaultdict(list)
    for row in inventory:
        if not isinstance(row, Mapping) or row.get("type") != "file":
            continue
        path = _canonical_optional_path(row.get("path"))
        if path is not None and _kind(path) == "video":
            videos_by_parent[posixpath.dirname(path)].append(path)
    for paths in videos_by_parent.values():
        paths.sort(key=str.casefold)
    nfo_stems_by_parent: dict[str, set[str]] = defaultdict(set)
    for row in inventory:
        if not isinstance(row, Mapping) or row.get("type") != "file":
            continue
        path = _canonical_optional_path(row.get("path"))
        if (
            path is not None
            and _suffix(path) == ".nfo"
            and not _is_episode_nfo(path)
        ):
            nfo_stems_by_parent[posixpath.dirname(path)].add(
                PurePosixPath(path).stem.casefold()
            )
    tvshow_nfo_paths = {
        path
        for row in inventory
        if isinstance(row, Mapping)
        and row.get("type") == "file"
        and (path := _canonical_optional_path(row.get("path"))) is not None
        and PurePosixPath(path).name.casefold() == "tvshow.nfo"
    }

    def tv_scope_paths(target_root: str) -> list[str]:
        # ``metadata_source`` is the nearest ancestor tvshow directory found
        # by the structural scan.  Comparing it to this exact target makes a
        # parent show unable to absorb a nested child show, while still
        # supporting custom season directory names (for example, a folder
        # named with a Chinese season label rather than ``Season 01``).
        paths = {
            _canonical_optional_path(row.get("path"))
            for row in media_dirs
            if row.get("metadata_source") == target_root
        }
        return sorted((path for path in paths if path is not None), key=str.casefold)

    def tv_direct_episode_paths(target_root: str) -> list[str]:
        """Return only explicit episode files directly beside a TV NFO.

        A TV folder may also contain movie/special files.  Such a file is
        eligible here only when its basename carries an explicit episode
        token and it has no same-stem NFO that could define a movie scope.
        This is intentionally bounded; unmarked trailers remain unknown.
        """
        direct_media = next(
            (
                row for row in media_dirs
                if _canonical_optional_path(row.get("path")) == target_root
            ),
            None,
        )
        if not isinstance(direct_media, Mapping):
            return []
        # The structural row itself must be backed by the show-level sidecar.
        # If an unrelated direct NFO won the row's basename selection, do not
        # infer TV ownership from episode-looking filenames.
        expected_tvshow = f"{target_root}/tvshow.nfo"
        if _canonical_optional_path(direct_media.get("nfo_path")) != expected_tvshow:
            return []
        paths: list[str] = []
        movie_nfo_stems = nfo_stems_by_parent.get(target_root, set())
        for path in videos_by_parent.get(target_root, []):
            stem = PurePosixPath(path).stem.casefold()
            if stem in movie_nfo_stems:
                continue
            if _episode_tokens(path):
                paths.append(path)
        return sorted(paths, key=str.casefold)

    def descendant_same_stem_videos(target_root: str, stem: str) -> list[str]:
        """Find bounded duplicate copies without crossing the formal root."""
        matches: list[str] = []
        wanted = stem.casefold()
        for parent, paths in videos_by_parent.items():
            if parent == target_root or not _inside(parent, target_root):
                continue
            # A nested TV sidecar is a hard identity boundary.  Even an
            # exact-stem video below it may be a special/extra of that show,
            # not a duplicate of an ancestor movie NFO.
            boundary = parent
            crosses_tv_boundary = False
            while boundary != target_root and _inside(boundary, target_root):
                if f"{boundary}/tvshow.nfo" in tvshow_nfo_paths:
                    crosses_tv_boundary = True
                    break
                boundary = posixpath.dirname(boundary)
            if crosses_tv_boundary:
                continue
            # A same-stem NFO in the child directory is an explicit identity
            # boundary; that child will be handled as its own movie work.
            if wanted in nfo_stems_by_parent.get(parent, set()):
                continue
            matches.extend(
                path for path in paths
                if PurePosixPath(path).stem.casefold() == wanted
            )
        return sorted(set(matches), key=str.casefold)

    for row in inventory:
        if not isinstance(row, Mapping) or row.get("type") != "file":
            continue
        nfo_path = _canonical_optional_path(row.get("path"))
        if nfo_path is None or _suffix(nfo_path) != ".nfo":
            continue
        formal_root = next((root for root in root_types if _inside(nfo_path, root)), None)
        if formal_root is None:
            continue
        identity = _read_nfo_identity(client, nfo_path)
        if identity is None:
            continue
        target_root = posixpath.dirname(nfo_path)
        if target_root == formal_root:
            continue
        media_type = str(identity.get("media_type") or "").casefold()
        scope: dict[str, object] | None = None
        if media_type == "tv":
            # Only a show-level sidecar on a TV shelf identifies a TV tree.
            # A movie-shaped NFO is intentionally accepted on either shelf;
            # real libraries commonly keep specials/films under an anime
            # bundle directory.
            if (
                root_types[formal_root] != "tv"
                or PurePosixPath(nfo_path).name.casefold() != "tvshow.nfo"
            ):
                continue
            media_paths = tv_scope_paths(target_root)
            direct_episode_paths = tv_direct_episode_paths(target_root)
            if not media_paths and not direct_episode_paths:
                # A bare TV NFO is still identity evidence.  A standalone
                # show can be entirely empty and must stay non-green (TMDB
                # will then yield missing episodes, or an unavailable catalog
                # will yield an explicit unknown).  A parent with a nested
                # tvshow.nfo cannot safely be acquired as a separate empty
                # show, but it must likewise remain visible as unknown rather
                # than being silently omitted.
                has_nested_tvshow = any(
                    nested != nfo_path and _inside(nested, target_root)
                    for nested in tvshow_nfo_paths
                )
                scope = {
                    "kind": (
                        "ambiguous_tv_container"
                        if has_nested_tvshow
                        else "empty_tv_directory"
                    ),
                }
            else:
                scope = {
                    "kind": "tv_metadata_source",
                    "metadata_source": target_root,
                    "media_paths": media_paths,
                    **({"video_paths": direct_episode_paths} if direct_episode_paths else {}),
                }
        elif media_type == "movie":
            # Match the sidecar to one direct video by basename.  Do not use
            # the containing directory as a blanket scope: that would make a
            # movie NFO in a mixed bundle claim its neighbours.
            sibling_videos = videos_by_parent.get(target_root, [])
            nfo_stem = PurePosixPath(nfo_path).stem.casefold()
            matching_videos = [
                path for path in sibling_videos
                if PurePosixPath(path).stem.casefold() == nfo_stem
            ]
            if len(matching_videos) == 1:
                duplicate_paths = descendant_same_stem_videos(target_root, nfo_stem)
                # One exact child copy is bounded evidence.  Multiple
                # unpaired descendants are left for the unknown aggregation;
                # folding them all into one movie would be too broad.
                bounded_duplicates = duplicate_paths if len(duplicate_paths) == 1 else []
                all_paths = [matching_videos[0], *bounded_duplicates]
                if len(all_paths) == 1:
                    scope = {"kind": "video_file", "video_path": all_paths[0]}
                else:
                    scope = {"kind": "video_files", "video_paths": all_paths}
            elif sibling_videos:
                # There is media in this directory, but no unambiguous
                # sidecar-to-file pairing.  Leave it unknown.
                continue
            else:
                descendant_paths = descendant_same_stem_videos(target_root, nfo_stem)
                if len(descendant_paths) == 1:
                    scope = {"kind": "video_file", "video_path": descendant_paths[0]}
                elif len(descendant_paths) > 1:
                    # More than one unpaired descendant has no stable file
                    # coordinate; leave all copies fail-closed.
                    continue
                else:
                    # Preserve the safe, explicit empty-directory expectation
                    # for one movie NFO.  A later target-level ambiguity check
                    # marks it unknown when several movie identities share the
                    # folder.
                    scope = {"kind": "empty_movie_directory"}
        else:
            continue
        directory = _directory_identity_fields(target_root, str(identity["media_type"]))
        candidate: dict[str, object] = {
            "tmdb_id": identity["tmdb_id"],
            "title": identity.get("title") or directory.get("title"),
            "original_title": identity.get("original_title"),
            "year": identity.get("year") or directory.get("year"),
            "target_root": target_root,
            "media_type": identity["media_type"],
            "identity_source": "library_nfo",
            "nfo_path": nfo_path,
            "identity_scope": scope,
        }
        candidates.append(candidate)

    # Several empty movie NFOs in one directory have no file coordinate and
    # therefore cannot be assigned safely.  File-scoped movie NFOs in the same
    # directory are fine: each one has its own exact sibling video.  Preserve
    # the former as explicit ambiguous works: dropping them would make an
    # otherwise empty directory invisible to the completion decision.
    by_target: dict[str, list[dict[str, object]]] = defaultdict(list)
    for candidate in candidates:
        by_target[str(candidate["target_root"])].append(candidate)
    for rows in by_target.values():
        empty_movies = [
            candidate
            for candidate in rows
            if candidate["media_type"] == "movie"
            and isinstance(candidate.get("identity_scope"), Mapping)
            and candidate["identity_scope"].get("kind") == "empty_movie_directory"
        ]
        if len(empty_movies) > 1:
            for candidate in empty_movies:
                candidate["identity_scope"] = {
                    "kind": "ambiguous_empty_movie_directory",
                }

    # The same TMDB identity claimed by multiple independent scopes is still
    # ambiguous.  Keep all claims explicit as unknown instead of arbitrarily
    # selecting one (or silently losing empty directories).
    by_identity: dict[tuple[str, int], list[dict[str, object]]] = defaultdict(list)
    for candidate in candidates:
        by_identity[(str(candidate["media_type"]), int(candidate["tmdb_id"]))].append(candidate)
    output: list[dict[str, object]] = []
    for rows in by_identity.values():
        if len(rows) == 1:
            output.append(rows[0])
            continue
        # Duplicate copies of one movie are safe when every NFO is paired to
        # a distinct, exact-stem video file.  Treat each file scope as an
        # independent observed copy; do not collapse the target roots or let
        # the TV parent scope absorb either copy.  Any empty, unpaired, or TV
        # scope remains ambiguous and stays fail-closed below.
        movie_file_scopes = [
            row.get("identity_scope")
            for row in rows
            if str(row.get("media_type") or "").casefold() == "movie"
        ]
        video_paths: list[str] = []
        all_file_scopes = len(movie_file_scopes) == len(rows)
        for scope in movie_file_scopes:
            if not isinstance(scope, Mapping):
                all_file_scopes = False
                continue
            kind = str(scope.get("kind") or "").casefold()
            if kind == "video_file":
                path = _canonical_optional_path(scope.get("video_path"))
                if path is None:
                    all_file_scopes = False
                else:
                    video_paths.append(path)
            elif kind == "video_files":
                raw_paths = scope.get("video_paths")
                if not isinstance(raw_paths, (list, tuple, set, frozenset)):
                    all_file_scopes = False
                    continue
                paths = [
                    path
                    for value in raw_paths
                    if (path := _canonical_optional_path(value)) is not None
                ]
                if not paths:
                    all_file_scopes = False
                video_paths.extend(paths)
            else:
                all_file_scopes = False
        if (
            all_file_scopes
            and video_paths
            and len(set(video_paths)) == len(video_paths)
        ):
            output.extend(rows)
            continue
        for candidate in rows:
            ambiguous = dict(candidate)
            ambiguous["identity_scope"] = {
                "kind": "ambiguous_duplicate_nfo_identity",
            }
            output.append(ambiguous)
    return sorted(
        output,
        key=lambda row: str(row["target_root"]).casefold(),
    )


def _work_identity_key(work: Mapping[str, object]) -> tuple[int, str, str] | None:
    metadata = _work_metadata(work)
    tmdb_id = _as_positive_int(metadata.get("tmdb_id"))
    target_root = _canonical_optional_path(metadata.get("target_root") or metadata.get("series_root"))
    media_type = str(metadata.get("media_type") or metadata.get("type") or "").casefold()
    if tmdb_id is None or target_root is None or media_type not in {"movie", "tv"}:
        return None
    return tmdb_id, target_root, media_type


def _merge_library_and_job_works(
    library_works: Sequence[Mapping[str, object]],
    job_works: Sequence[Mapping[str, object]],
) -> tuple[list[dict[str, object]], set[tuple[int, str, str]]]:
    """Prefer direct NFO evidence while retaining matching job episode facts.

    The returned set contains NFO works with no matching persisted root job.
    It is report-only: callers must not interpret it as authorization to create
    a task or mutate a media library.
    """
    indexed_jobs = {
        key: dict(work)
        for work in job_works
        if isinstance(work, Mapping) and (key := _work_identity_key(work)) is not None
    }
    output: list[dict[str, object]] = []
    occupied_keys: set[tuple[int, str, str]] = set()
    unowned: set[tuple[int, str, str]] = set()
    library_keys_by_target: dict[str, set[tuple[int, str, str]]] = defaultdict(set)
    for raw_work in library_works:
        if not isinstance(raw_work, Mapping):
            continue
        key = _work_identity_key(raw_work)
        if key is None:
            continue
        library_keys_by_target[key[1]].add(key)
        library = dict(raw_work)
        matching_job = indexed_jobs.pop(key, None)
        if matching_job is not None:
            # NFO is the direct library identity; completed jobs contribute
            # only fields NFO cannot carry, such as published episode data.
            for field, value in matching_job.items():
                existing = library.get(field)
                if field not in library or existing is None or existing == "" or existing == [] or existing == {}:
                    library[field] = value
            library["identity_sources"] = ["library_nfo", "engine_job"]
        else:
            library["identity_sources"] = ["library_nfo"]
            unowned.add(key)
        output.append(library)
        occupied_keys.add(key)
    for key, work in indexed_jobs.items():
        # A conflicting NFO identity is deliberately preferred above.  Do not
        # discard a different movie/file scope merely because a bundle shares
        # its containing directory with an older job.  However, an old job
        # without an explicit scope cannot safely claim that entire bundle:
        # it could otherwise hide an unpaired neighbour and turn the audit
        # green.  Preserve the job as an explicit unknown instead.
        if key in occupied_keys:
            continue
        job = dict(work)
        metadata = _work_metadata(job)
        has_explicit_scope = isinstance(metadata.get("identity_scope"), Mapping)
        is_audit_owned = metadata.get("audit_owned") is True
        conflicting_library_scope = any(
            library_key != key
            for library_key in library_keys_by_target.get(key[1], set())
        )
        if not has_explicit_scope and not is_audit_owned and conflicting_library_scope:
            job["identity_scope"] = {
                "kind": "ambiguous_legacy_target_tree",
            }
        output.append(job)
    return output, unowned


def automatic_job_gaps(jobs: Sequence[object]) -> list[dict[str, object]]:
    """Report unfinished automatic jobs without turning them into provider gaps."""
    rows: list[dict[str, object]] = []
    terminal = {"executed", "completed", "cancelled"}
    for raw_job in jobs:
        if isinstance(raw_job, Mapping):
            job = raw_job
        else:
            as_dict = getattr(raw_job, "as_dict", None)
            job = as_dict() if callable(as_dict) else {}
        if not isinstance(job, Mapping):
            continue
        summary = job.get("summary") if isinstance(job.get("summary"), Mapping) else {}
        if summary.get("internal_child") is True:
            continue
        phase = str(job.get("phase") or "")
        if not phase or phase in terminal:
            continue
        job_id = str(job.get("id") or "").strip()
        request = job.get("request") if isinstance(job.get("request"), Mapping) else {}
        source_path = request.get("source_path")
        row: dict[str, object] = {
            "id": f"incomplete_job:{job_id or 'unknown'}",
            "kind": "incomplete_job",
            "job_id": job_id or None,
            "phase": phase,
            "retryable": phase not in {"failed_identity", "failed_provider", "failed_write", "failed_verification"},
            "reason": str(job.get("error") or f"自动任务尚未完成：{phase}"),
        }
        if isinstance(source_path, str) and source_path.startswith("/"):
            row["source_path"] = source_path
        rows.append(row)
    return rows


class TmdbEpisodeCatalog:
    """Read-only adapter for already-published TV episodes.

    One instance belongs to one audit run.  Its bounded cache deliberately
    stores failures as ``None`` as well as successful catalogs, so a provider
    outage cannot multiply requests for duplicate works or turn a partial
    snapshot into a green audit.
    """

    def __init__(
        self,
        client: object | None,
        *,
        today: Callable[[], date] | None = None,
    ) -> None:
        self.client = client
        self.today = today or date.today
        self._cache: dict[tuple[int, date], dict[int, list[dict[str, object]]] | None] = {}
        # Keys placed here reached the bounded prefetch deadline (or could
        # not be submitted).  A late worker may still finish its network call,
        # but it must not replace the fail-closed ``None`` with evidence from
        # outside this audit's time budget.
        self._unresolved: set[tuple[int, date]] = set()
        self._cache_lock = threading.Lock()

    @staticmethod
    def _published(value: object, today: date) -> bool:
        if not isinstance(value, str) or not _DATE_RE.fullmatch(value):
            return False
        try:
            return date.fromisoformat(value) <= today
        except ValueError:
            return False

    @staticmethod
    def _merge_season_zero_localized_aliases(
        getter: Callable[..., object],
        path: str,
        rows: Sequence[dict[str, object]],
    ) -> None:
        """Add bounded English/Japanese title evidence to published S00 rows.

        The default TMDB client language remains the source of truth for the
        catalog rows themselves.  Localized responses are deliberately used
        only for names, and only after a primary S00 row has already passed
        coordinate and air-date validation.  A localized lookup is optional:
        older custom clients may not accept ``language=`` and a transient
        translation failure must not make an otherwise valid catalog unknown.
        """
        rows_by_episode: dict[int, dict[str, object]] = {}
        aliases_by_episode: dict[int, list[str]] = {}
        seen_by_episode: dict[int, set[str]] = {}
        limits_by_episode: dict[int, int] = {}
        for row in rows:
            episode_number = row.get("episode_number")
            if (
                isinstance(episode_number, bool)
                or not isinstance(episode_number, int)
                or episode_number <= 0
            ):
                continue
            primary_name = str(row.get("name") or "").strip()
            aliases = [
                value
                for value in _episode_title_values(row)
                if value.casefold() != primary_name.casefold()
            ]
            rows_by_episode[episode_number] = row
            aliases_by_episode[episode_number] = aliases
            seen_by_episode[episode_number] = {
                primary_name.casefold(), *(value.casefold() for value in aliases),
            }
            # ``_episode_title_values`` carries at most eight values.  Leave
            # space for the primary name where one exists, preserving that
            # same cap when the localized payload adds evidence.
            limits_by_episode[episode_number] = 7 if primary_name else 8

        if not rows_by_episode:
            return
        for language in ("en-US", "ja-JP"):
            try:
                localized = getter(path, language=language)
            except Exception:
                # This includes TypeError from legacy/fake ``get(path)``
                # clients.  Never retry without the language argument: that
                # would duplicate the primary payload rather than add a
                # localized alias.
                continue
            if not isinstance(localized, Mapping):
                continue
            episodes = localized.get("episodes")
            if not isinstance(episodes, list):
                continue
            for episode in episodes:
                if not isinstance(episode, Mapping):
                    continue
                episode_number = episode.get("episode_number")
                if (
                    isinstance(episode_number, bool)
                    or not isinstance(episode_number, int)
                    or episode_number not in rows_by_episode
                ):
                    continue
                # The endpoint itself scopes the response to S00.  Reject an
                # explicitly contradictory row rather than borrowing a title
                # from another season that happens to share an episode number.
                localized_season = episode.get("season_number")
                if localized_season is not None and (
                    isinstance(localized_season, bool)
                    or not isinstance(localized_season, int)
                    or localized_season != 0
                ):
                    continue
                aliases = aliases_by_episode[episode_number]
                seen = seen_by_episode[episode_number]
                limit = limits_by_episode[episode_number]
                for value in _episode_title_values(episode):
                    key = value.casefold()
                    if key in seen:
                        continue
                    seen.add(key)
                    aliases.append(value)
                    if len(aliases) >= limit:
                        break
                if aliases:
                    rows_by_episode[episode_number]["title_aliases"] = aliases

    def _fetch_uncached(
        self,
        tmdb_id: int,
        current_day: date,
    ) -> dict[int, list[dict[str, object]]] | None:
        getter = getattr(self.client, "get", None)
        if not callable(getter):
            return None
        try:
            show = getter(f"/tv/{tmdb_id}")
        except Exception:
            return None
        if not isinstance(show, Mapping):
            return None
        seasons = show.get("seasons")
        if not isinstance(seasons, list):
            return None
        output: dict[int, list[dict[str, object]]] = {}
        for raw_season in seasons:
            if not isinstance(raw_season, Mapping):
                continue
            number = raw_season.get("season_number")
            if isinstance(number, bool) or not isinstance(number, int) or number < 0:
                continue
            # Season metadata may lack an air date. Query it instead of
            # guessing; individual episodes decide whether they are released.
            try:
                payload = getter(f"/tv/{tmdb_id}/season/{number}")
            except Exception:
                return None
            if not isinstance(payload, Mapping) or not isinstance(payload.get("episodes"), list):
                return None
            rows: list[dict[str, object]] = []
            for episode in payload["episodes"]:
                if not isinstance(episode, Mapping):
                    continue
                episode_number = episode.get("episode_number")
                if (
                    isinstance(episode_number, bool)
                    or not isinstance(episode_number, int)
                    or episode_number <= 0
                    or not self._published(episode.get("air_date"), current_day)
                ):
                    continue
                primary_name = str(episode.get("name") or "").strip()
                row: dict[str, object] = {
                    "season_number": number,
                    "episode_number": episode_number,
                    "name": primary_name,
                    "air_date": str(episode.get("air_date") or ""),
                }
                # TMDB's standard payload has only ``name``.  Preserve
                # optional adapter-provided localized names when present,
                # without changing the established row shape for ordinary
                # responses.
                aliases = [
                    value for value in _episode_title_values(episode)
                    if value.casefold() != primary_name.casefold()
                ]
                if aliases:
                    row["title_aliases"] = aliases
                rows.append(row)
            if rows:
                if number == 0:
                    try:
                        self._merge_season_zero_localized_aliases(
                            getter,
                            f"/tv/{tmdb_id}/season/{number}",
                            rows,
                        )
                    except Exception:
                        # Localized aliases are supplementary evidence.  The
                        # primary catalog above remains valid even if a
                        # custom client or malformed optional response breaks
                        # this best-effort enrichment.
                        pass
                output[number] = rows
        return output

    def _catalog_for_id(
        self,
        tmdb_id: int,
        current_day: date,
    ) -> dict[int, list[dict[str, object]]] | None:
        key = (tmdb_id, current_day)
        with self._cache_lock:
            if key in self._cache:
                return copy.deepcopy(self._cache[key])
        # ``prefetch`` submits one future per unique id, so this path does not
        # duplicate work under normal audit execution. The lock protects the
        # result map for callers that invoke the catalog concurrently.
        try:
            result = self._fetch_uncached(tmdb_id, current_day)
        except Exception:
            result = None
        with self._cache_lock:
            if key in self._unresolved:
                self._cache[key] = None
                return None
            self._cache[key] = copy.deepcopy(result)
        return copy.deepcopy(result)

    def _cache_unresolved(
        self,
        tmdb_ids: Sequence[int],
        current_day: date,
    ) -> None:
        """Cache identities that a bounded prefetch could not resolve.

        This is intentionally separate from ``_catalog_for_id``: timeout
        cleanup can mark a running future before that future returns, and the
        marker prevents its late result from leaking into the current audit.
        """
        with self._cache_lock:
            for tmdb_id in tmdb_ids:
                key = (tmdb_id, current_day)
                self._unresolved.add(key)
                self._cache[key] = None

    def __call__(self, work: Mapping[str, object]) -> dict[int, list[dict[str, object]]] | None:
        metadata = _work_metadata(work)
        if str(metadata.get("media_type") or metadata.get("type") or "").casefold() == "movie":
            return {}
        tmdb_id = _as_positive_int(metadata.get("tmdb_id"))
        if tmdb_id is None:
            return None
        return self._catalog_for_id(tmdb_id, self.today())

    def prefetch(
        self,
        works: Sequence[Mapping[str, object]],
        *,
        max_workers: int = 4,
    ) -> None:
        """Warm unique TV identities with a tightly bounded worker pool.

        The method is an optimization only. Every fetch catches provider
        failures and stores ``None``; the later semantic pass still calls this
        same cache and therefore reports unknown evidence instead of assuming
        an empty or complete episode catalog.
        """
        current_day = self.today()
        ids: set[int] = set()
        for work in works:
            if not isinstance(work, Mapping):
                continue
            metadata = _work_metadata(work)
            if str(metadata.get("media_type") or metadata.get("type") or "").casefold() == "movie":
                continue
            tmdb_id = _as_positive_int(metadata.get("tmdb_id"))
            if tmdb_id is not None:
                ids.add(tmdb_id)
        if not ids:
            return
        with self._cache_lock:
            pending = [
                tmdb_id for tmdb_id in sorted(ids)
                if (tmdb_id, current_day) not in self._cache
            ]
        if not pending:
            return
        try:
            requested_workers = int(max_workers)
        except (TypeError, ValueError):
            requested_workers = 4
        workers = max(1, min(4, requested_workers, len(pending)))
        budget_seconds = _tmdb_audit_budget_seconds()
        deadline = time.monotonic() + budget_seconds

        # Keep at most ``workers`` futures submitted at any point.  In
        # particular, do not enqueue every library identity in an executor's
        # unbounded work queue: a large queue makes cancellation expensive and
        # lets stale requests continue long after the audit has moved on.
        if budget_seconds <= 0:
            self._cache_unresolved(pending, current_day)
            return

        futures: dict[object, int] = {}
        next_index = 0
        pool: ThreadPoolExecutor | None = None

        def submit_available() -> None:
            nonlocal next_index
            if pool is None:
                return
            while (
                next_index < len(pending)
                and len(futures) < workers
                and time.monotonic() < deadline
            ):
                tmdb_id = pending[next_index]
                next_index += 1
                try:
                    future = pool.submit(
                        self._catalog_for_id,
                        tmdb_id,
                        current_day,
                    )
                except Exception:
                    self._cache_unresolved([tmdb_id], current_day)
                    continue
                futures[future] = tmdb_id

        try:
            pool = ThreadPoolExecutor(
                max_workers=workers,
                thread_name_prefix="scrapeflow-tmdb-audit",
            )
            submit_available()
            while futures:
                remaining_seconds = deadline - time.monotonic()
                if remaining_seconds <= 0:
                    break
                done, _ = wait(
                    tuple(futures),
                    timeout=remaining_seconds,
                    return_when=FIRST_COMPLETED,
                )
                if not done:
                    # The budget elapsed while one or more provider calls were
                    # still running.  They are handled uniformly by the
                    # fail-closed cleanup below.
                    break
                for future in done:
                    tmdb_id = futures.pop(future)
                    try:
                        # ``_catalog_for_id`` catches provider failures and
                        # writes ``None`` itself; this guard also handles an
                        # executor/future implementation that raises here.
                        future.result()
                    except Exception:
                        self._cache_unresolved([tmdb_id], current_day)
                submit_available()
        except Exception:
            # Prefetch is advisory evidence.  A broken executor or an
            # unexpected future implementation must degrade to unknown IDs,
            # never fail the structural/semantic audit itself.
            pass
        finally:
            # Futures that completed right at the deadline still provide
            # in-budget evidence.  Consume those results before marking the
            # remainder unknown.  Crucially, never use a context manager or
            # ``shutdown(wait=True)``: running provider calls are allowed to
            # finish in the background while this audit returns promptly.
            for future, tmdb_id in list(futures.items()):
                if not getattr(future, "done", lambda: False)():
                    continue
                futures.pop(future, None)
                try:
                    future.result()
                except Exception:
                    self._cache_unresolved([tmdb_id], current_day)

            unresolved = list(futures.values()) + pending[next_index:]
            self._cache_unresolved(unresolved, current_day)
            if pool is not None:
                for future in tuple(futures):
                    try:
                        future.cancel()
                    except Exception:
                        pass
                try:
                    pool.shutdown(wait=False, cancel_futures=True)
                except TypeError:  # pragma: no cover - Python < 3.9 fallback
                    try:
                        pool.shutdown(wait=False)
                    except Exception:
                        pass
                except Exception:
                    # A broken executor must not turn a bounded evidence pass
                    # into a hard audit failure; unresolved identities are
                    # already cached above.
                    pass


def _episode_title_values(value: Mapping[str, object]) -> list[str]:
    """Return bounded title evidence carried by one catalog episode row.

    TMDB's regular episode response calls the primary title ``name``.  A
    caller may also provide an optional ``title`` or a small list of aliases
    (useful for a localized/catalog adapter).  These values are search
    evidence only: they never become episode numbers or alter the normalized
    catalog shape.
    """
    candidates: list[object] = [
        value.get("name"), value.get("title"), value.get("original_name"),
    ]
    for key in ("title_aliases", "aliases", "names", "alternate_titles"):
        raw = value.get(key)
        if isinstance(raw, str):
            candidates.append(raw)
        elif isinstance(raw, (list, tuple, set, frozenset)):
            candidates.extend(raw)
    output: list[str] = []
    seen: set[str] = set()
    for raw in candidates:
        if not isinstance(raw, str):
            continue
        text = raw.strip()
        if not text or "\x00" in text or len(text) > 512:
            continue
        key = text.casefold()
        if key in seen:
            continue
        seen.add(key)
        output.append(text)
        if len(output) >= 8:
            break
    return output


def _normalise_expected_episodes(
    value: object, *, default_season: int | None = None,
    title_evidence: dict[tuple[int, int], list[str]] | None = None,
) -> dict[int, set[int]]:
    """Normalize flexible catalog shapes to {season: {episode, ...}}.

    ``title_evidence`` is an optional side channel for callers that need the
    human-readable title of a catalog row.  It is deliberately opt-in so the
    long-standing return shape (and all episode-set semantics) remain exactly
    unchanged.
    """
    output: dict[int, set[int]] = defaultdict(set)

    def record_title(season: int, episode: int, row: Mapping[str, object]) -> None:
        if title_evidence is None:
            return
        values = _episode_title_values(row)
        if not values:
            return
        target = title_evidence.setdefault((season, episode), [])
        seen = {item.casefold() for item in target}
        for item in values:
            if item.casefold() in seen:
                continue
            seen.add(item.casefold())
            target.append(item)
            if len(target) >= 8:
                break

    def add(season_value: object, episodes_value: object) -> None:
        season = _as_positive_int(season_value)
        if season is None:
            # Season 00 is valid for TV specials.  Keep it explicit, but do
            # not treat an absent season as season zero.
            if isinstance(season_value, int) and not isinstance(season_value, bool) and season_value == 0:
                season = 0
            elif isinstance(season_value, str) and season_value.strip() == "0":
                season = 0
        if season is None:
            return
        if isinstance(episodes_value, Mapping):
            for key in ("episodes", "episode_numbers", "numbers", "items"):
                if key in episodes_value:
                    add(season, episodes_value[key])
                    return
            count = _as_positive_int(episodes_value.get("episode_count"))
            if count is not None:
                output[season].update(range(1, count + 1))
                return
        count = _as_positive_int(episodes_value)
        if count is not None:
            output[season].update(range(1, count + 1))
            return
        if isinstance(episodes_value, (list, tuple, set, frozenset)):
            for item in episodes_value:
                number = _as_positive_int(item)
                if number is not None:
                    output[season].add(number)
                elif isinstance(item, Mapping):
                    air_date = item.get("air_date")
                    if isinstance(air_date, str) and _DATE_RE.fullmatch(air_date):
                        try:
                            if date.fromisoformat(air_date) > date.today():
                                continue
                        except ValueError:
                            continue
                    number = _as_positive_int(item.get("episode_number", item.get("number")))
                    if number is not None:
                        output[season].add(number)
                        record_title(season, number, item)

    if isinstance(value, Mapping):
        # {1: [1, 2]}, {"1": {"episode_count": 12}}, and a single
        # {season_number, episode_count} row are all emitted by existing
        # Engine/TMDB adapters.
        if "season_number" in value or "season" in value:
            add(value.get("season_number", value.get("season")), value)
        else:
            for season, episodes in value.items():
                add(season, episodes)
    elif isinstance(value, (list, tuple)):
        for item in value:
            if isinstance(item, Mapping):
                air_date = item.get("air_date")
                if isinstance(air_date, str) and _DATE_RE.fullmatch(air_date):
                    try:
                        if date.fromisoformat(air_date) > date.today():
                            continue
                    except ValueError:
                        continue
                season = item.get("season_number", item.get("season", default_season))
                episode = item.get("episode_number", item.get("episode", item.get("number")))
                if episode is not None:
                    add(season, [episode])
                    season_number = _as_positive_int(season)
                    if season_number is None and isinstance(season, int) and season == 0:
                        season_number = 0
                    episode_number = _as_positive_int(episode)
                    if season_number is not None and episode_number is not None:
                        record_title(season_number, episode_number, item)
                elif "episode_count" in item or "episodes" in item or "episode_numbers" in item:
                    add(season, item)
            else:
                number = _as_positive_int(item)
                if number is not None and default_season is not None:
                    output[default_season].add(number)
    return {season: set(sorted(numbers)) for season, numbers in output.items() if numbers}


def _catalog_for_work(
    work: Mapping[str, object],
    episode_catalog: Mapping[object, object] | Callable[[Mapping[str, object]], object] | None,
) -> dict[int, set[int]]:
    expected, _titles = _catalog_episode_evidence_for_work(work, episode_catalog)
    return expected


def _catalog_episode_evidence_for_work(
    work: Mapping[str, object],
    episode_catalog: Mapping[object, object] | Callable[[Mapping[str, object]], object] | None,
) -> tuple[dict[int, set[int]], dict[tuple[int, int], list[str]]]:
    """Return expected episodes plus optional official title evidence.

    Only the externally supplied catalog value is collected into the title
    side channel.  Local episode-count metadata remains the authoritative
    set source but cannot accidentally turn arbitrary plan text into a
    provider search term.
    """
    metadata = _work_metadata(work)
    default_season = _as_positive_int(metadata.get("season"))
    if isinstance(metadata.get("season"), int) and metadata.get("season") == 0:
        default_season = 0
    values: list[tuple[object, bool]] = []
    for key in ("expected_episodes", "season_episodes", "official_episodes", "episodes"):
        if key in metadata:
            values.append((metadata[key], False))
    if "expected_episode_count" in metadata and default_season is not None:
        values.append(({default_season: metadata["expected_episode_count"]}, False))
    for key in ("seasons", "official_seasons"):
        raw_seasons = metadata.get(key)
        if isinstance(raw_seasons, (list, tuple, Mapping)):
            values.append((raw_seasons, False))
    if episode_catalog is not None:
        try:
            extra = episode_catalog(work) if callable(episode_catalog) else None
            if extra is None and isinstance(episode_catalog, Mapping):
                tmdb_id = metadata.get("tmdb_id")
                extra = episode_catalog.get(tmdb_id, episode_catalog.get(str(tmdb_id)))
            if extra is not None:
                values.append((extra, True))
        except Exception:
            # A catalog outage should not discard explicit episode evidence
            # already persisted on the work. If there is no local evidence,
            # the caller will correctly report unknown below.
            pass
    output: dict[int, set[int]] = defaultdict(set)
    title_evidence: dict[tuple[int, int], list[str]] = {}
    for value, collect_titles in values:
        for season, episodes in _normalise_expected_episodes(
            value,
            default_season=default_season,
            title_evidence=title_evidence if collect_titles else None,
        ).items():
            output[season].update(episodes)
    return dict(output), title_evidence


def _identity_scope_members(
    metadata: Mapping[str, object],
    *,
    target_root: str,
    video_paths: Sequence[str],
    media_dirs: Sequence[Mapping[str, object]],
) -> tuple[list[str], set[str]]:
    """Return the videos and media directories owned by one identity scope.

    Legacy Engine jobs have no ``identity_scope`` and retain their historical
    target-tree semantics.  Library NFO works always carry an explicit scope;
    a file scope can therefore never claim a neighbouring movie in the same
    directory, and a TV metadata-source scope can never cross a nested show
    boundary.
    """
    all_videos = set(video_paths)
    raw_scope = metadata.get("identity_scope")
    if not isinstance(raw_scope, Mapping):
        scoped_videos = sorted(
            (path for path in all_videos if _inside(path, target_root)), key=str.casefold
        )
        scoped_media = {
            path
            for row in media_dirs
            if (path := _canonical_optional_path(row.get("path"))) is not None
            and _inside(path, target_root)
        }
        return scoped_videos, scoped_media

    kind = str(raw_scope.get("kind") or "").casefold()
    if kind == "video_file":
        path = _canonical_optional_path(raw_scope.get("video_path"))
        if path is None or path not in all_videos or not _inside(path, target_root):
            return [], set()
        return [path], {posixpath.dirname(path)}
    if kind == "video_files":
        raw_paths = raw_scope.get("video_paths")
        if not isinstance(raw_paths, (list, tuple, set, frozenset)):
            return [], set()
        scoped_videos = {
            path
            for value in raw_paths
            if (path := _canonical_optional_path(value)) is not None
            and path in all_videos
            and _inside(path, target_root)
        }
        if not scoped_videos:
            return [], set()
        return sorted(scoped_videos, key=str.casefold), {
            posixpath.dirname(path) for path in scoped_videos
        }
    if kind == "tv_metadata_source":
        raw_paths = raw_scope.get("media_paths")
        if not isinstance(raw_paths, (list, tuple, set, frozenset)):
            raw_paths = []
        scoped_media = {
            path
            for value in raw_paths
            if (path := _canonical_optional_path(value)) is not None
            and _inside(path, target_root)
        }
        scoped_videos = {
            path for path in all_videos if posixpath.dirname(path) in scoped_media
        }
        # Direct episode files beside ``tvshow.nfo`` are carried explicitly;
        # this avoids making the whole mixed parent directory TV-owned.
        raw_direct_videos = raw_scope.get("video_paths")
        if isinstance(raw_direct_videos, (list, tuple, set, frozenset)):
            scoped_videos.update(
                path
                for value in raw_direct_videos
                if (path := _canonical_optional_path(value)) is not None
                and path in all_videos
                and _inside(path, target_root)
            )
        scoped_media.update(posixpath.dirname(path) for path in scoped_videos)
        return sorted(scoped_videos, key=str.casefold), scoped_media
    if kind in {
        "empty_movie_directory",
        "empty_tv_directory",
        "ambiguous_empty_movie_directory",
        "ambiguous_duplicate_nfo_identity",
        "ambiguous_legacy_target_tree",
        "ambiguous_tv_container",
    }:
        return [], set()
    # Unknown scope shapes are not trusted.  This branch is intentionally
    # conservative for library identities; only jobs without a scope use the
    # legacy fallback above.
    return [], set()


def _ancillary_marker(path: str) -> str | None:
    """Classify only explicit non-episode ancillary filename markers."""
    match = _ANCILLARY_VIDEO_RE.search(PurePosixPath(path).stem)
    if match is None:
        return None
    return "behind_the_scenes" if "behind" in match.group(0).casefold() else "trailer"


def _observed_ancillary_media(
    report: Mapping[str, object],
    works: Sequence[Mapping[str, object]],
) -> list[dict[str, object]]:
    """Return bounded, non-actionable TV ancillary observations.

    This deliberately has a narrower evidence bar than a work scope: an
    ancillary video must sit directly beside the one valid ``tvshow.nfo`` for
    its TV identity, carry an explicit trailer/behind-the-scenes marker, and
    have no same-stem NFO.  It is only an inventory observation; callers must
    never turn it into an episode, a semantic gap, or an acquisition project.
    """
    inventory = report.get("inventory")
    if not isinstance(inventory, list):
        return []
    videos_by_parent: dict[str, list[str]] = defaultdict(list)
    nfo_stems_by_parent: dict[str, set[str]] = defaultdict(set)
    direct_tvshow_counts: dict[str, int] = defaultdict(int)
    tvshow_dirs: set[str] = set()
    for row in inventory:
        if not isinstance(row, Mapping) or row.get("type") != "file":
            continue
        path = _canonical_optional_path(row.get("path"))
        if path is None:
            continue
        if _kind(path) == "video":
            videos_by_parent[posixpath.dirname(path)].append(path)
            continue
        if _suffix(path) != ".nfo":
            continue
        parent = posixpath.dirname(path)
        nfo_stems_by_parent[parent].add(PurePosixPath(path).stem.casefold())
        if PurePosixPath(path).name.casefold() == "tvshow.nfo":
            direct_tvshow_counts[parent] += 1
            tvshow_dirs.add(parent)

    # A target must have exactly one direct, parsed library TV identity.  A
    # merged same-identity Engine job is fine; multiple different TV IDs at
    # one target are deliberately not ancillary-safe.
    tv_ids_by_target: dict[str, set[int]] = defaultdict(set)
    for raw_work in works:
        if not isinstance(raw_work, Mapping):
            continue
        metadata = _work_metadata(raw_work)
        if str(metadata.get("media_type") or metadata.get("type") or "").casefold() != "tv":
            continue
        target = _canonical_optional_path(metadata.get("target_root") or metadata.get("series_root"))
        tmdb_id = _as_positive_int(metadata.get("tmdb_id"))
        raw_scope = metadata.get("identity_scope")
        nfo_path = _canonical_optional_path(metadata.get("nfo_path"))
        sources = metadata.get("identity_sources")
        has_library_nfo = (
            metadata.get("identity_source") == "library_nfo"
            or isinstance(sources, (list, tuple, set, frozenset))
            and "library_nfo" in sources
        )
        if (
            target is None
            or tmdb_id is None
            or not has_library_nfo
            or not isinstance(raw_scope, Mapping)
            or raw_scope.get("kind") != "tv_metadata_source"
            or nfo_path != f"{target}/tvshow.nfo"
        ):
            continue
        tv_ids_by_target[target].add(tmdb_id)

    observations: list[dict[str, object]] = []
    for target, tmdb_ids in tv_ids_by_target.items():
        if len(tmdb_ids) != 1 or direct_tvshow_counts.get(target) != 1:
            continue
        for path in videos_by_parent.get(target, []):
            marker = _ancillary_marker(path)
            if marker is None:
                continue
            stem = PurePosixPath(path).stem.casefold()
            if stem in nfo_stems_by_parent.get(target, set()):
                continue
            # The closest TV sidecar must be this target itself. This rejects
            # a video below a nested child TV identity even if an ancestor
            # happens to have a matching title.
            boundaries = [root for root in tvshow_dirs if _inside(path, root)]
            if not boundaries or max(boundaries, key=len) != target:
                continue
            observations.append({
                "path": path,
                "target_root": target,
                "tmdb_id": next(iter(tmdb_ids)),
                "kind": marker,
                "source": "explicit_ancillary_filename",
            })
    return sorted(observations, key=lambda row: str(row["path"]).casefold())


def _bounded_episode_range_tokens(
    season: int,
    start: int,
    end: int,
    *,
    end_season: int | None = None,
) -> set[tuple[int, int]]:
    """Return a small, same-season explicit episode range or no evidence."""
    if (
        season < 0
        or end_season not in {None, season}
        or start <= 0
        or end < start
        or end - start + 1 > _MAX_EXPLICIT_EPISODE_RANGE
    ):
        return set()
    return {(season, episode) for episode in range(start, end + 1)}


def _episode_tokens(path: str, *, default_season: int | None = None) -> set[tuple[int, int]]:
    """Extract only explicit episode tokens from one audited video path."""
    tokens: set[tuple[int, int]] = set()
    for match in _SEASON_EPISODE_RANGE_RE.finditer(path):
        season = int(match.group("season"))
        end_season = match.group("end_season")
        tokens.update(_bounded_episode_range_tokens(
            season,
            int(match.group("start")),
            int(match.group("end")),
            end_season=int(end_season) if end_season is not None else None,
        ))
    for match in _SEASON_EPISODE_RE.finditer(path):
        season, episode = int(match.group("season")), int(match.group("episode"))
        if season >= 0 and episode > 0:
            tokens.add((season, episode))
    if tokens:
        return tokens
    if default_season is None:
        return tokens
    for match in _EPISODE_RANGE_RE.finditer(path):
        tokens.update(_bounded_episode_range_tokens(
            default_season,
            int(match.group("start")),
            int(match.group("end")),
        ))
    for match in _EPISODE_ONLY_RE.finditer(path):
        episode = int(match.group("episode"))
        if episode > 0:
            tokens.add((default_season, episode))
    if tokens:
        return tokens
    # Season 2/01.mkv and [01].mkv are common anime layouts.  Infer from a
    # clearly named season directory, never from an arbitrary movie number.
    parts = PurePosixPath(path).parts
    season = next(
        (int(match.group(1)) for part in reversed(parts) if (match := _SEASON_DIR_RE.match(part))),
        default_season,
    )
    if season is None:
        return tokens
    stem = PurePosixPath(path).stem
    match = re.search(r"(?:^|[ ._\[(?-])0*(\d{1,4})(?:\]|$)", stem)
    if match:
        episode = int(match.group(1))
        if episode > 0:
            tokens.add((season, episode))
    return tokens


def _language_keys(value: object) -> set[str]:
    text = str(value or "").casefold()
    if any(marker in text for marker in (
        "zh", "chi", "chs", "cht", "中文", "简中", "簡中", "简体", "簡體", "chinese",
    )):
        return {"zh"}
    if any(marker in text for marker in ("en", "eng", "英文", "英语", "英語", "english")):
        return {"en"}
    if any(marker in text for marker in ("ja", "jpn", "日文", "日语", "日語", "japanese")):
        return {"ja"}
    return set()


def _safe_ffprobe_headers(headers: Mapping[str, object]) -> str | None:
    """Serialize provider headers without allowing option/header injection."""
    output: list[str] = []
    for raw_name, raw_value in headers.items():
        name = str(raw_name).strip()
        value = str(raw_value).strip()
        if (
            not re.fullmatch(r"[A-Za-z0-9-]+", name)
            or "\r" in value
            or "\n" in value
        ):
            return None
        output.append(f"{name}: {value}\r\n")
    return "".join(output)


def _subtitle_probe_int(name: str, default: int, *, minimum: int, maximum: int) -> int:
    raw = os.getenv(name, "").strip()
    try:
        value = int(raw) if raw else default
    except ValueError:
        value = default
    return max(minimum, min(maximum, value))


def _subtitle_probe_seconds(
    name: str,
    default: float,
    *,
    minimum: float,
    maximum: float,
) -> float:
    """Parse a bounded monotonic wall-clock budget from the environment.

    ``float`` is intentional here: a small value makes the fail-closed path
    testable without making a production audit wait for a whole second.  NaN
    and infinity are rejected because they would defeat the deadline check.
    """
    raw = os.getenv(name, "").strip()
    try:
        value = float(raw) if raw else default
    except (TypeError, ValueError):
        value = default
    if not math.isfinite(value):
        value = default
    return max(minimum, min(maximum, value))


def subtitle_evidence_ledger_path(state_root: str | Path) -> Path:
    """Return the local-only checkpoint path for subtitle probe evidence."""
    return Path(state_root) / "library-audit" / _SUBTITLE_EVIDENCE_LEDGER_FILENAME


def _subtitle_evidence_language(required_language: str | Sequence[str]) -> str | None:
    """Canonicalise the configured lane without preserving arbitrary input."""
    languages: set[str] = set()
    if isinstance(required_language, str):
        languages.update(_language_keys(required_language))
    elif isinstance(required_language, (list, tuple, set, frozenset)):
        for value in required_language:
            languages.update(_language_keys(value))
    if not languages:
        return None
    return ",".join(sorted(languages))


def _subtitle_evidence_identity(
    raw: object,
    required_language: str | Sequence[str],
) -> dict[str, object] | None:
    """Normalise one inventory row for safe evidence lookup.

    The cached identity deliberately requires both a non-negative exact size
    and a provider version token.  A path lacking either can still be probed
    during this audit, but cannot inherit a prior verdict after a restart.
    That conservative rule is what makes same-path replacement fail closed.
    """
    language = _subtitle_evidence_language(required_language)
    if language is None:
        return None
    if isinstance(raw, Mapping):
        candidate_path = raw.get("path")
        candidate_size = raw.get("size")
        candidate_version = (
            raw.get("version")
            if "version" in raw else _inventory_file_version(raw)
        )
    else:
        candidate_path = raw
        candidate_size = None
        candidate_version = None
    path = _canonical_optional_path(candidate_path)
    if path is None or _kind(path) != "video":
        return None
    size = (
        candidate_size
        if isinstance(candidate_size, int) and not isinstance(candidate_size, bool)
        and candidate_size >= 0
        else None
    )
    # The scanner has already selected an AList version field by name.  A
    # direct checker may provide no version at all, which deliberately blocks
    # durable reuse rather than guessing a same-path object is unchanged.
    version = _safe_file_version_token(candidate_version)
    return {
        "path": path,
        "size": size,
        "version": version,
        "language": language,
    }


def _subtitle_evidence_key(identity: Mapping[str, object]) -> str | None:
    """Return the durable key only for a fully versioned inventory object."""
    path = _canonical_optional_path(identity.get("path"))
    size = identity.get("size")
    version = identity.get("version")
    language = identity.get("language")
    if (
        path is None
        or not isinstance(size, int)
        or isinstance(size, bool)
        or size < 0
        or not isinstance(version, str)
        or not version
        or not isinstance(language, str)
        or not re.fullmatch(r"[a-z]{2,3}(?:,[a-z]{2,3})*", language)
    ):
        return None
    # A compact JSON key is inspectable and carries exactly the required
    # coordinates: schema, canonical path, byte size, provider version and
    # configured language.  It contains no signed link/header material.
    return json.dumps(
        [
            _SUBTITLE_EVIDENCE_LEDGER_SCHEMA_VERSION,
            path,
            size,
            version,
            language,
        ],
        ensure_ascii=False,
        separators=(",", ":"),
    )


def _safe_subtitle_evidence_source(value: object) -> str | None:
    return value if isinstance(value, str) and value in _SUBTITLE_EVIDENCE_SOURCES else None


def _safe_subtitle_evidence_reason(value: object) -> str:
    return value if isinstance(value, str) and value in _SUBTITLE_EVIDENCE_REASONS else "subtitle_probe_error"


def _normalise_subtitle_evidence_result(value: object) -> dict[str, object]:
    """Strip a probe result down to the only values safe to checkpoint.

    A definitive result without an allow-listed proof source is downgraded to
    unknown.  This prevents a mocked/provider-specific payload from becoming
    a durable missing-subtitle assertion merely because it spelled a status
    string correctly.
    """
    if not isinstance(value, Mapping):
        return {"status": "unknown", "reason": "subtitle_probe_error"}
    status = str(value.get("status") or "").casefold()
    if status not in _SUBTITLE_EVIDENCE_STATUSES:
        return {"status": "unknown", "reason": "subtitle_probe_error"}
    source = _safe_subtitle_evidence_source(value.get("source"))
    if status in _SUBTITLE_EVIDENCE_DEFINITIVE_STATUSES and source is None:
        return {"status": "unknown", "reason": "subtitle_probe_untrusted_evidence"}
    result: dict[str, object] = {"status": status}
    if source is not None:
        result["source"] = source
    if status == "unknown":
        result["reason"] = _safe_subtitle_evidence_reason(value.get("reason"))
    return result


def _decode_subtitle_evidence_key(value: object) -> dict[str, object] | None:
    """Validate a persisted ledger key before it is eligible for reuse."""
    if not isinstance(value, str) or len(value) > 4096:
        return None
    try:
        raw = json.loads(value)
    except (TypeError, ValueError, json.JSONDecodeError):
        return None
    if not isinstance(raw, list) or len(raw) != 5:
        return None
    schema, path, size, version, language = raw
    if schema != _SUBTITLE_EVIDENCE_LEDGER_SCHEMA_VERSION:
        return None
    identity = {
        "path": path,
        "size": size,
        "version": version,
        "language": language,
    }
    return identity if _subtitle_evidence_key(identity) == value else None


class SubtitleEvidenceLedger:
    """Small durable checkpoint for versioned subtitle probe evidence.

    Unknown entries are retained as safe diagnostic checkpoints but never
    reused as a verdict.  Every later audit selects them again through the
    cursor, while satisfied/missing entries suppress only an exact identity
    match.  A corrupt or hand-edited ledger simply starts empty.
    """

    def __init__(self, state_root: str | Path | None) -> None:
        self.path = subtitle_evidence_ledger_path(state_root) if state_root is not None else None
        self.entries: dict[str, dict[str, object]] = {}
        self.cursors: dict[str, str] = {}
        self._dirty = False
        self._load()

    def _load(self) -> None:
        if self.path is None:
            return
        try:
            raw = json.loads(self.path.read_text(encoding="utf-8"))
        except (FileNotFoundError, OSError, UnicodeDecodeError, json.JSONDecodeError):
            return
        if not isinstance(raw, Mapping):
            return
        if (
            raw.get("schema_version") != _SUBTITLE_EVIDENCE_LEDGER_SCHEMA_VERSION
            or raw.get("kind") != _SUBTITLE_EVIDENCE_LEDGER_KIND
        ):
            return
        raw_entries = raw.get("entries")
        if isinstance(raw_entries, Mapping):
            for raw_key, raw_result in raw_entries.items():
                key = raw_key if isinstance(raw_key, str) else None
                identity = _decode_subtitle_evidence_key(key)
                if key is None or identity is None or not isinstance(raw_result, Mapping):
                    continue
                result = _normalise_subtitle_evidence_result(raw_result)
                # A malformed definitive entry is normalised to unknown; do
                # not let it appear as cached proof after a manual edit.
                if (
                    str(raw_result.get("status") or "").casefold()
                    in _SUBTITLE_EVIDENCE_DEFINITIVE_STATUSES
                    and result.get("status") != raw_result.get("status")
                ):
                    continue
                self.entries[key] = result
        raw_cursors = raw.get("cursors")
        if isinstance(raw_cursors, Mapping):
            for language, path in raw_cursors.items():
                if (
                    isinstance(language, str)
                    and re.fullmatch(r"[a-z]{2,3}(?:,[a-z]{2,3})*", language)
                    and _canonical_optional_path(path) is not None
                ):
                    self.cursors[language] = str(path)

    @staticmethod
    def _result_for_storage(value: Mapping[str, object]) -> dict[str, object]:
        result = _normalise_subtitle_evidence_result(value)
        # Only the status/reason/source whitelist ever reaches disk.
        return {
            key: result[key]
            for key in ("status", "reason", "source")
            if key in result
        }

    def lookup(self, identity: Mapping[str, object]) -> dict[str, object] | None:
        key = _subtitle_evidence_key(identity)
        if key is None:
            return None
        result = self.entries.get(key)
        return dict(result) if result is not None else None

    def record(self, identity: Mapping[str, object], value: Mapping[str, object]) -> None:
        key = _subtitle_evidence_key(identity)
        if key is None:
            return
        result = self._result_for_storage(value)
        if self.entries.get(key) != result:
            self.entries[key] = result
            self._dirty = True

    def invalidate_mutated(self, identities: Sequence[Mapping[str, object]]) -> None:
        """Drop prior versions for paths whose current identity is explicit.

        Ignoring a mismatched key is already fail-closed.  Removing it as
        well keeps the checkpoint bounded and makes the invalidation property
        inspectable: a changed provider object cannot leave a competing old
        version that might be reused if the path later changes again.
        """
        current: dict[tuple[str, str], str] = {}
        for identity in identities:
            key = _subtitle_evidence_key(identity)
            path = _canonical_optional_path(identity.get("path"))
            language = identity.get("language")
            if key is not None and path is not None and isinstance(language, str):
                current[(path, language)] = key
        if not current:
            return
        for key in tuple(self.entries):
            identity = _decode_subtitle_evidence_key(key)
            if identity is None:
                continue
            path = str(identity["path"])
            language = str(identity["language"])
            replacement = current.get((path, language))
            if replacement is not None and replacement != key:
                self.entries.pop(key, None)
                self._dirty = True

    def cursor(self, language: str) -> str | None:
        return self.cursors.get(language)

    def set_cursor(self, language: str, path: str) -> None:
        if (
            not re.fullmatch(r"[a-z]{2,3}(?:,[a-z]{2,3})*", language)
            or _canonical_optional_path(path) is None
        ):
            return
        if self.cursors.get(language) != path:
            self.cursors[language] = path
            self._dirty = True

    def persist(self) -> None:
        if self.path is None or not self._dirty:
            return
        payload = {
            "schema_version": _SUBTITLE_EVIDENCE_LEDGER_SCHEMA_VERSION,
            "kind": _SUBTITLE_EVIDENCE_LEDGER_KIND,
            "cursors": dict(sorted(self.cursors.items())),
            "entries": dict(sorted(self.entries.items())),
        }
        atomic_write_json(self.path, payload, allow_nan=False)
        self._dirty = False


def _subtitle_track_language_keys(value: object) -> set[str]:
    """Map ffprobe language/title evidence to the small configured lanes."""
    text = str(value or "").casefold().replace("_", "-")
    compact = re.sub(r"[^a-z0-9\u3400-\u9fff]+", "", text)
    keys: set[str] = set()
    if (
        compact in {"zh", "zho", "chi", "chs", "cht", "cmn", "zhcn", "zhtw", "zhhans", "zhhant"}
        or any(marker in text for marker in (
            "中文", "简中", "繁中", "简体", "繁體", "繁体", "chinese", "hans", "hant",
        ))
    ):
        keys.add("zh")
    if compact in {"en", "eng", "enus", "english"} or any(
        marker in text for marker in ("english", "英文", "英语", "英語")
    ):
        keys.add("en")
    if compact in {"ja", "jpn", "jp", "japanese"} or any(
        marker in text for marker in ("japanese", "日文", "日语", "日語")
    ):
        keys.add("ja")
    return keys


def _has_explicit_subtitle_language_code(value: object) -> bool:
    """Return whether ffprobe supplied a concrete ISO-639-ish code."""
    text = str(value or "").strip().casefold().replace("_", "-")
    if text in {"", "und", "unknown", "unk", "mul", "mis", "zxx"}:
        return False
    return re.fullmatch(r"[a-z]{2,3}(?:-[a-z]{2,4})?", text) is not None


def classify_embedded_subtitle_streams(
    streams: Sequence[Mapping[str, object]],
    required_language: str | Sequence[str],
) -> dict[str, object]:
    """Classify ffprobe subtitle tracks for one explicit language lane.

    An explicitly tagged matching track is sufficient evidence of presence.
    A complete stream table with only other tagged languages is sufficient
    evidence of absence.  Any untagged/ambiguous track remains ``unknown`` so
    the audit cannot download a duplicate or the wrong language.
    """
    required: set[str] = set()
    if isinstance(required_language, str):
        required.update(_language_keys(required_language))
    elif isinstance(required_language, (list, tuple, set, frozenset)):
        for value in required_language:
            required.update(_language_keys(value))
    if not required:
        return {"status": "unknown", "reason": "unsupported_required_language"}
    classified: list[dict[str, object]] = []
    has_unknown = False
    has_match = False
    for raw in streams:
        if not isinstance(raw, Mapping):
            return {"status": "unknown", "reason": "invalid_stream_row"}
        tags = raw.get("tags") if isinstance(raw.get("tags"), Mapping) else {}
        language = str(tags.get("language") or "").strip().casefold()
        title = str(tags.get("title") or "").strip()
        evidence = f"{language} {title}".strip()
        keys = _subtitle_track_language_keys(evidence)
        if keys & required:
            classification = "matching"
            has_match = True
        elif not language and not title:
            classification = "unknown"
            has_unknown = True
        elif not keys:
            # A concrete ISO-639 code (for example ``eng`` or ``ita``) is
            # conclusive non-target evidence.  An unrecognised title-only or
            # ``und`` track remains ambiguous and must not trigger a download.
            if _has_explicit_subtitle_language_code(language):
                classification = "non_matching"
            else:
                classification = "unknown"
                has_unknown = True
        else:
            classification = "non_matching"
        classified.append({
            "index": raw.get("index"),
            "codec_name": raw.get("codec_name"),
            "language": language,
            "title": title,
            "classification": classification,
        })
    if has_match:
        status = "satisfied"
    elif has_unknown:
        status = "unknown"
    else:
        status = "missing"
    return {"status": status, "source": "embedded", "tracks": classified}


# Matroska places its complete ``Tracks`` element in the Segment header in
# ordinary files.  We do not assume that convention blindly: the tiny EBML
# reader below returns a verdict only when the prefix contains a finite,
# fully-decoded Tracks element.  Any unknown-size/truncated/unrecognised
# structural element remains ``None`` and preserves the signed-link fallback.
_EBML_HEADER_ID = 0x1A45DFA3
_EBML_SEGMENT_ID = 0x18538067
_EBML_TRACKS_ID = 0x1654AE6B
_EBML_TRACK_ENTRY_ID = 0xAE
_EBML_TRACK_TYPE_ID = 0xD7
_EBML_TRACK_LANGUAGE_ID = 0x22B59C
_EBML_TRACK_LANGUAGE_IETF_ID = 0x22B59D
_EBML_TRACK_NAME_ID = 0x536E
_EBML_VOID_ID = 0xEC
_EBML_CRC32_ID = 0xBF


def _ebml_vint(
    data: bytes, offset: int, *, size: bool,
) -> tuple[int | None, int] | None:
    """Read one bounded EBML element ID/size VINT without recovery guesses."""
    if offset < 0 or offset >= len(data):
        return None
    first = data[offset]
    marker = 0x80
    length = 1
    while length <= 8 and not first & marker:
        marker >>= 1
        length += 1
    # Element IDs allow at most four bytes; a zero leading byte is invalid.
    if marker == 0 or (not size and length > 4) or offset + length > len(data):
        return None
    if size:
        value = first & (marker - 1)
        for byte in data[offset + 1:offset + length]:
            value = (value << 8) | byte
        # All value bits set is EBML's unknown-length sentinel.  It cannot
        # prove that this prefix contains the complete child element.
        if value == (1 << (7 * length)) - 1:
            return None, length
        return value, length
    return int.from_bytes(data[offset:offset + length], "big"), length


def _ebml_element(
    data: bytes, offset: int, limit: int, *, allow_unknown_size: bool = False,
) -> tuple[int, int, int | None] | None:
    """Return ``(id, payload_start, payload_end)`` for one finite element."""
    identifier = _ebml_vint(data, offset, size=False)
    if identifier is None:
        return None
    element_id, id_length = identifier
    length = _ebml_vint(data, offset + id_length, size=True)
    if length is None:
        return None
    payload_size, size_length = length
    if payload_size is None:
        return (element_id, offset + id_length + size_length, None) if allow_unknown_size else None
    payload_start = offset + id_length + size_length
    payload_end = payload_start + payload_size
    if payload_start > limit or payload_end > limit:
        return None
    return element_id, payload_start, payload_end


def _ebml_track_entry_stream(
    data: bytes, start: int, end: int,
) -> Mapping[str, object] | None:
    """Decode only the fields required to classify one complete TrackEntry."""
    position = start
    track_type: int | None = None
    language: str | None = None
    language_ietf: str | None = None
    title: str | None = None
    while position < end:
        element = _ebml_element(data, position, end)
        if element is None:
            return None
        element_id, payload_start, payload_end = element
        payload = data[payload_start:payload_end]
        if element_id == _EBML_TRACK_TYPE_ID:
            if not payload or len(payload) > 8 or track_type is not None:
                return None
            track_type = int.from_bytes(payload, "big")
        elif element_id in {
            _EBML_TRACK_LANGUAGE_ID,
            _EBML_TRACK_LANGUAGE_IETF_ID,
            _EBML_TRACK_NAME_ID,
        }:
            if len(payload) > 4096:
                return None
            try:
                text = payload.decode("utf-8").strip()
            except UnicodeDecodeError:
                return None
            if element_id == _EBML_TRACK_LANGUAGE_ID:
                if language is not None:
                    return None
                language = text
            elif element_id == _EBML_TRACK_LANGUAGE_IETF_ID:
                if language_ietf is not None:
                    return None
                language_ietf = text
            else:
                if title is not None:
                    return None
                title = text
        position = payload_end
    if position != end or track_type is None:
        return None
    if track_type != 17:  # Matroska TrackType Subtitle.
        return {}
    tags: dict[str, object] = {}
    if language_ietf:
        tags["language"] = language_ietf
    elif language:
        tags["language"] = language
    if title:
        tags["title"] = title
    return {"codec_name": "matroska", "tags": tags}


def _matroska_prefix_subtitle_streams(prefix: bytes) -> list[Mapping[str, object]] | None:
    """Return subtitle tracks only when a complete Matroska Tracks header fits.

    ``None`` means no proof: the caller must continue with the regular remote
    probe.  An empty list is meaningful only when a finite entire Segment was
    available and contained no Tracks element.
    """
    if not isinstance(prefix, bytes) or not prefix:
        return None
    header = _ebml_element(prefix, 0, len(prefix))
    if header is None or header[0] != _EBML_HEADER_ID:
        return None
    position = header[2]
    segment: tuple[int, int, int | None] | None = None
    # A finite Void may appear between the EBML header and Segment.  No other
    # top-level item is accepted before Segment because guessing container
    # boundaries would turn an incomplete prefix into a false absence proof.
    while position < len(prefix):
        element = _ebml_element(
            prefix, position, len(prefix), allow_unknown_size=True,
        )
        if element is None:
            return None
        if element[0] == _EBML_SEGMENT_ID:
            segment = element
            break
        if element[0] != _EBML_VOID_ID or element[2] is None:
            return None
        position = int(element[2])
    if segment is None:
        return None
    _segment_id, segment_start, declared_segment_end = segment
    segment_complete = declared_segment_end is not None
    segment_end = int(declared_segment_end) if declared_segment_end is not None else len(prefix)
    position = segment_start
    streams: list[Mapping[str, object]] = []
    while position < segment_end:
        element = _ebml_element(prefix, position, segment_end)
        if element is None:
            return None
        element_id, payload_start, payload_end = element
        if element_id == _EBML_TRACKS_ID:
            track_position = payload_start
            while track_position < payload_end:
                track = _ebml_element(prefix, track_position, payload_end)
                if track is None:
                    return None
                track_id, track_start, track_end = track
                if track_id == _EBML_TRACK_ENTRY_ID:
                    stream = _ebml_track_entry_stream(prefix, track_start, track_end)
                    if stream is None:
                        return None
                    if stream:
                        streams.append(stream)
                elif track_id not in {_EBML_VOID_ID, _EBML_CRC32_ID}:
                    # The Matroska Tracks schema does not permit arbitrary
                    # siblings that could encode another subtitle identity.
                    return None
                track_position = track_end
            return streams if track_position == payload_end else None
        position = payload_end
    # Reaching a finite Segment end is conclusive: there was no Tracks
    # element at all.  In a normal large MKV the Segment has unknown size, so
    # ``_ebml_element`` returns None before this branch and falls back.
    return streams if segment_complete and position == segment_end else None


def _complete_matroska_prefix_subtitle_result(
    prefix: bytes,
    required_language: str | Sequence[str],
) -> dict[str, object] | None:
    streams = _matroska_prefix_subtitle_streams(prefix)
    if streams is None:
        return None
    result = classify_embedded_subtitle_streams(streams, required_language)
    if result.get("status") != "missing":
        return None
    result["source"] = "embedded_complete_mkv_prefix"
    return result


def probe_remote_subtitle_streams(
    client: object,
    video_path: str,
    required_language: str | Sequence[str],
    *,
    timeout: int | None = None,
    max_probe_bytes: int | None = None,
) -> dict[str, object]:
    """Probe embedded subtitle tracks through bounded AList evidence.

    When available, ``read_file_prefix`` is the first (non-URL) probe input;
    a positive matching track is immediately authoritative, while a negative
    prefix result falls through to the signed-link probe so a large MP4/MKV
    whose track table is outside the prefix can still be classified.  Every
    read is bounded by ffprobe's probe/analyse limits and a wall-clock timeout;
    failures remain ``unknown`` and never authorise a speculative write.
    """
    ffprobe = shutil.which("ffprobe")
    if ffprobe is None:
        return {"status": "unknown", "reason": "ffprobe_not_installed"}
    probe_bytes = (
        max_probe_bytes
        if isinstance(max_probe_bytes, int) and not isinstance(max_probe_bytes, bool)
        else _subtitle_probe_int(
            "SCRAPEFLOW_SUBTITLE_PROBE_BYTES",
            _SUBTITLE_PROBE_DEFAULT_BYTES,
            minimum=64 * 1024,
            maximum=_SUBTITLE_PROBE_MAX_BYTES,
        )
    )
    probe_bytes = max(64 * 1024, min(_SUBTITLE_PROBE_MAX_BYTES, probe_bytes))
    probe_timeout = (
        timeout
        if isinstance(timeout, int) and not isinstance(timeout, bool)
        else _subtitle_probe_int(
            "SCRAPEFLOW_SUBTITLE_PROBE_TIMEOUT",
            _SUBTITLE_PROBE_DEFAULT_TIMEOUT,
            minimum=3,
            maximum=120,
        )
    )
    probe_timeout = max(3, min(120, probe_timeout))

    def run_probe(
        *,
        raw_url: str | None = None,
        safe_headers: str = "",
        prefix: bytes | None = None,
    ) -> dict[str, object]:
        command = [
            ffprobe, "-v", "error", "-rw_timeout", "15000000",
            "-probesize", str(probe_bytes),
            "-analyzeduration", str(probe_bytes),
        ]
        if safe_headers:
            command.extend(["-headers", safe_headers])
        command.extend([
            "-select_streams", "s", "-show_entries",
            "stream=index,codec_name:stream_tags=language,title",
            "-of", "json", "-i", "pipe:0" if prefix is not None else raw_url or "",
        ])
        try:
            completed = subprocess.run(
                command,
                check=False,
                capture_output=True,
                # Bytes avoid an implicit locale decode for a pipe prefix;
                # json.loads accepts either bytes or text in the fallback.
                text=False,
                input=prefix,
                timeout=probe_timeout,
            )
        except subprocess.TimeoutExpired:
            return {"status": "unknown", "reason": "ffprobe_timeout"}
        except (OSError, TypeError, ValueError):
            return {"status": "unknown", "reason": "subtitle_probe_error"}
        if completed.returncode != 0:
            return {"status": "unknown", "reason": "ffprobe_nonzero_exit"}
        stdout = completed.stdout
        if isinstance(stdout, (bytes, bytearray)) and len(stdout) > 2 * 1024 * 1024:
            return {"status": "unknown", "reason": "ffprobe_output_too_large"}
        if isinstance(stdout, str) and len(stdout) > 2 * 1024 * 1024:
            return {"status": "unknown", "reason": "ffprobe_output_too_large"}
        try:
            payload = json.loads(stdout)
        except (TypeError, ValueError, json.JSONDecodeError):
            return {"status": "unknown", "reason": "invalid_ffprobe_output"}
        streams = payload.get("streams") if isinstance(payload, Mapping) else None
        if not isinstance(streams, list) or not all(isinstance(row, Mapping) for row in streams):
            return {"status": "unknown", "reason": "invalid_ffprobe_output"}
        return classify_embedded_subtitle_streams(streams, required_language)

    # Prefer an AList bounded-prefix read.  This avoids exposing signed URLs
    # to a subprocess for the common positive (embedded matching track) case.
    prefix_reader = getattr(client, "read_file_prefix", None)
    prefix_result: dict[str, object] | None = None
    if callable(prefix_reader):
        try:
            prefix = prefix_reader(video_path, max_bytes=probe_bytes)
        except Exception:
            prefix = None
        if isinstance(prefix, bytes) and prefix:
            prefix_result = run_probe(prefix=prefix)
            if prefix_result.get("status") == "satisfied":
                return prefix_result

            # A complete Matroska Tracks element is a bounded negative proof:
            # all subtitle TrackEntry records have already been parsed in the
            # prefix, so a large file does not need a second signed-link
            # ffprobe merely to rediscover the same explicit non-target set.
            # The helper is deliberately narrower than the general ffprobe
            # result and returns None for any structural ambiguity.
            if prefix_result.get("status") == "missing":
                complete_mkv = _complete_matroska_prefix_subtitle_result(
                    prefix, required_language,
                )
                if complete_mkv is not None:
                    return complete_mkv

            # A small object whose entire byte range was read is a complete
            # negative probe.  Large objects still require the signed-link
            # fallback because container indexes may live past the prefix.
            exact = getattr(client, "exact_file_info", None)
            if prefix_result.get("status") == "missing" and callable(exact):
                try:
                    info = exact(video_path)
                    size = info.get("size") if isinstance(info, Mapping) else None
                    if isinstance(size, int) and size <= len(prefix):
                        return prefix_result
                except Exception:
                    pass

    link = getattr(client, "file_link", None)
    if not callable(link):
        return prefix_result or {"status": "unknown", "reason": "alist_file_link_unavailable"}
    try:
        try:
            raw_url, headers = link(video_path, refresh=True)
        except TypeError:
            raw_url, headers = link(video_path)
        if (
            not isinstance(raw_url, str)
            or not raw_url.startswith(("http://", "https://"))
            or not isinstance(headers, Mapping)
        ):
            return {"status": "unknown", "reason": "invalid_alist_file_link"}
        safe_headers = _safe_ffprobe_headers(headers)
        if safe_headers is None:
            return {"status": "unknown", "reason": "unsafe_provider_headers"}
        return run_probe(raw_url=raw_url, safe_headers=safe_headers)
    except (OSError, TypeError, ValueError, json.JSONDecodeError):
        return {"status": "unknown", "reason": "subtitle_probe_error"}


def _make_ephemeral_alist_subtitle_checker(
    client: object,
    required_language: str | Sequence[str],
) -> Callable[[str], Mapping[str, object]]:
    """Build one-audit cached checker for explicit embedded-language probes.

    The optional ``prefetch`` attribute is consumed by the semantic audit to
    run a bounded worker pool.  A single monotonic wall-clock budget covers
    that prefetch.  At the deadline, queued and still-running probes are
    cached as ``unknown``; they are therefore surfaced as
    ``unknown_subtitle_evidence`` instead of accidentally becoming a
    ``missing_subtitle`` gap.  We intentionally do not wait for a provider's
    signed-link timeout when the audit budget has expired.

    Calling the returned function directly remains synchronous for small tests
    and non-audit callers.  Once a prefetch has completed, a path that was not
    in its bounded set is treated as budget-exhausted rather than bypassing
    the full-audit deadline with an unbounded late probe.
    """
    cache: dict[str, Mapping[str, object]] = {}
    lock = threading.Lock()
    max_files = _subtitle_probe_int(
        "SCRAPEFLOW_SUBTITLE_PROBE_MAX_FILES",
        _SUBTITLE_PROBE_DEFAULT_MAX_FILES,
        minimum=1,
        maximum=20_000,
    )
    workers = _subtitle_probe_int(
        "SCRAPEFLOW_SUBTITLE_PROBE_WORKERS",
        _SUBTITLE_PROBE_DEFAULT_WORKERS,
        minimum=1,
        maximum=4,
    )
    budget_seconds = _subtitle_probe_seconds(
        "SCRAPEFLOW_SUBTITLE_PROBE_BUDGET_SECONDS",
        _SUBTITLE_PROBE_DEFAULT_BUDGET_SECONDS,
        minimum=0.0,
        maximum=_SUBTITLE_PROBE_MAX_BUDGET_SECONDS,
    )
    # The deadline is created lazily at the beginning of the first prefetch,
    # so time spent building the structural inventory does not consume the
    # subtitle evidence budget.  ``prefetch_finished`` closes the escape hatch
    # for a later synchronous call after a bounded audit has returned.
    deadline: float | None = None
    prefetch_started = False
    prefetch_finished = False

    def unknown_budget() -> Mapping[str, object]:
        return {"status": "unknown", "reason": "subtitle_probe_budget_exhausted"}

    def cache_unknown(paths: Sequence[str]) -> None:
        with lock:
            for path in paths:
                if path not in cache:
                    cache[path] = unknown_budget()

    def check(video_path: str) -> Mapping[str, object]:
        nonlocal deadline
        with lock:
            cached = cache.get(video_path)
            budget_exhausted = (
                video_path not in cache
                and (
                    prefetch_finished
                    or (
                        prefetch_started
                        and deadline is not None
                        and time.monotonic() >= deadline
                    )
                    or len(cache) >= max_files
                )
            )
        if cached is not None:
            return cached
        if budget_exhausted:
            result = unknown_budget()
        else:
            # Direct/synchronous callers do not use ``prefetch``.  Start a
            # budget for the first such call only when a prefetch has not
            # already established one; the per-probe ffprobe timeout still
            # bounds this compatibility path.
            with lock:
                if deadline is None and not prefetch_started:
                    deadline = time.monotonic() + budget_seconds
            result = probe_remote_subtitle_streams(client, video_path, required_language)
            if not isinstance(result, Mapping):
                result = {"status": "unknown", "reason": "subtitle_probe_error"}
        with lock:
            # A concurrent prefetch may have filled this key while the direct
            # probe was running.  Preserve the first authoritative/unknown
            # result and never overwrite a fail-closed budget marker.
            existing = cache.get(video_path)
            if existing is None:
                cache[video_path] = result
                return result
            return existing

    def prefetch(video_paths: Sequence[str]) -> None:
        nonlocal deadline, prefetch_started, prefetch_finished
        unique = list(dict.fromkeys(
            path for path in video_paths if isinstance(path, str) and path.startswith("/")
        ))
        # Preserve the synchronous checker contract for callers that invoke
        # ``prefetch([])`` merely as a no-op; no full-audit budget exists when
        # there is no candidate path at all.
        if not unique:
            return
        with lock:
            if deadline is None:
                deadline = time.monotonic() + budget_seconds
            prefetch_started = True
            pending = [path for path in unique if path not in cache]
            remaining = max(0, max_files - len(cache))
        if not pending:
            with lock:
                prefetch_finished = True
            return

        bounded = pending[:remaining]
        overflow = pending[remaining:]
        # ``max_files`` is a separate cardinality guard.  It is still a
        # conclusive *unknown* outcome, never a missing-subtitle assertion.
        cache_unknown(overflow)
        if not bounded:
            with lock:
                prefetch_finished = True
            return

        # Keep at most ``workers`` provider calls in flight.  In particular,
        # do not submit all 1,451 paths to an executor queue: cancelling a
        # giant queue is slow and makes it easy for late results to leak past
        # the audit deadline.
        futures: dict[object, str] = {}
        next_index = 0
        pool: ThreadPoolExecutor | None = None

        def submit_available() -> None:
            nonlocal next_index
            if pool is None or deadline is None:
                return
            while (
                next_index < len(bounded)
                and len(futures) < workers
                and time.monotonic() < deadline
            ):
                path = bounded[next_index]
                next_index += 1
                try:
                    future = pool.submit(
                        probe_remote_subtitle_streams,
                        client,
                        path,
                        required_language,
                    )
                except Exception:
                    cache_unknown([path])
                    continue
                futures[future] = path

        try:
            pool = ThreadPoolExecutor(max_workers=workers)
            submit_available()
            while futures:
                assert deadline is not None  # established above
                remaining_seconds = deadline - time.monotonic()
                if remaining_seconds <= 0:
                    break
                done, _ = wait(
                    tuple(futures),
                    timeout=remaining_seconds,
                    return_when=FIRST_COMPLETED,
                )
                if not done:
                    # The monotonic budget elapsed while one or more provider
                    # calls were still running.  They are handled uniformly
                    # by the fail-closed cleanup below.
                    break
                for future in done:
                    path = futures.pop(future)
                    try:
                        result = future.result()
                    except Exception:
                        result = {"status": "unknown", "reason": "subtitle_probe_error"}
                    if not isinstance(result, Mapping):
                        result = {"status": "unknown", "reason": "subtitle_probe_error"}
                    with lock:
                        # A future can only be written here before the
                        # deadline cleanup.  ``setdefault`` protects a path
                        # that a concurrent direct checker already marked
                        # unknown.
                        cache.setdefault(path, result)
                submit_available()
        finally:
            # Anything still running, not yet submitted, or left over from a
            # failed executor construction is explicitly unknown.  We never
            # call ``shutdown(wait=True)`` here: the whole point of this lane
            # is that a slow provider cannot hold the full-library audit open.
            # A future that completed right at the deadline is still usable
            # evidence (not an in-flight timeout), so consume those results
            # before marking the remainder unknown.
            for future, path in list(futures.items()):
                if not getattr(future, "done", lambda: False)():
                    continue
                try:
                    result = future.result()
                except Exception:
                    result = {"status": "unknown", "reason": "subtitle_probe_error"}
                if not isinstance(result, Mapping):
                    result = {"status": "unknown", "reason": "subtitle_probe_error"}
                with lock:
                    cache.setdefault(path, result)
                futures.pop(future, None)
            unresolved = list(futures.values()) + bounded[next_index:]
            cache_unknown(unresolved)
            if pool is not None:
                for future in tuple(futures):
                    try:
                        future.cancel()
                    except Exception:
                        pass
                try:
                    pool.shutdown(wait=False, cancel_futures=True)
                except TypeError:  # pragma: no cover - Python < 3.9 fallback
                    try:
                        pool.shutdown(wait=False)
                    except Exception:
                        pass
                except Exception:
                    # A broken executor must not turn a bounded evidence
                    # pass into a hard audit failure; unresolved paths are
                    # already cached as unknown above.
                    pass
            with lock:
                prefetch_finished = True

    # Functions are objects in Python; this keeps the public checker type
    # callable while allowing the audit to opt into bounded parallelism.
    setattr(check, "prefetch", prefetch)

    return check


def make_alist_subtitle_checker(
    client: object,
    required_language: str | Sequence[str],
    *,
    state_root: str | Path | None = None,
) -> Callable[[str], Mapping[str, object]]:
    """Build a fair, bounded subtitle checker with durable safe evidence.

    ``prefetch`` accepts full inventory rows when available.  It reuses only a
    matching, versioned satisfied/missing verdict, then moves a persistent
    cursor through the remaining files.  Unknown evidence is checkpointed for
    diagnostics but always remains eligible for a later probe.
    """
    cache: dict[str, Mapping[str, object]] = {}
    identities: dict[str, dict[str, object]] = {}
    prior_unknowns: dict[str, Mapping[str, object]] = {}
    deferred_paths: set[str] = set()
    lock = threading.Lock()
    ledger = SubtitleEvidenceLedger(state_root)
    max_files = _subtitle_probe_int(
        "SCRAPEFLOW_SUBTITLE_PROBE_MAX_FILES",
        _SUBTITLE_PROBE_DEFAULT_MAX_FILES,
        minimum=1,
        maximum=20_000,
    )
    workers = _subtitle_probe_int(
        "SCRAPEFLOW_SUBTITLE_PROBE_WORKERS",
        _SUBTITLE_PROBE_DEFAULT_WORKERS,
        minimum=1,
        maximum=4,
    )
    batch_size = _subtitle_probe_int(
        "SCRAPEFLOW_SUBTITLE_PROBE_BATCH_SIZE",
        _SUBTITLE_PROBE_DEFAULT_BATCH_SIZE,
        minimum=1,
        maximum=_SUBTITLE_PROBE_MAX_BATCH_SIZE,
    )
    budget_seconds = _subtitle_probe_seconds(
        "SCRAPEFLOW_SUBTITLE_PROBE_BUDGET_SECONDS",
        _SUBTITLE_PROBE_DEFAULT_BUDGET_SECONDS,
        minimum=0.0,
        maximum=_SUBTITLE_PROBE_MAX_BUDGET_SECONDS,
    )
    deadline: float | None = None
    prefetch_started = False
    prefetch_finished = False

    def unknown(reason: str) -> Mapping[str, object]:
        return {"status": "unknown", "reason": reason}

    def persist_ledger() -> None:
        try:
            ledger.persist()
        except OSError:
            # Losing a local checkpoint only causes fresh probing on the next
            # audit.  It must not turn a provider result into a false gap.
            return

    def put(
        path: str,
        identity: Mapping[str, object] | None,
        value: object,
    ) -> Mapping[str, object]:
        result = _normalise_subtitle_evidence_result(value)
        with lock:
            existing = cache.get(path)
            if existing is not None:
                return existing
            cache[path] = result
        if identity is not None:
            ledger.record(identity, result)
        return result

    def known_identity(video_path: object) -> tuple[str | None, dict[str, object] | None]:
        path = _canonical_optional_path(video_path)
        if path is None:
            return None, None
        with lock:
            identity = identities.get(path)
        if identity is None:
            identity = _subtitle_evidence_identity(path, required_language)
        return path, identity

    def check(video_path: str) -> Mapping[str, object]:
        nonlocal deadline
        path, identity = known_identity(video_path)
        if path is None:
            return unknown("subtitle_evidence_unavailable")
        with lock:
            cached = cache.get(path)
            closed = (
                prefetch_finished
                or (prefetch_started and deadline is not None and time.monotonic() >= deadline)
            )
            old_unknown = prior_unknowns.get(path)
            deferred = path in deferred_paths
        if cached is not None:
            return cached
        if closed:
            result = old_unknown or unknown(
                "subtitle_probe_batch_deferred"
                if deferred else "subtitle_probe_budget_exhausted"
            )
        else:
            with lock:
                if deadline is None and not prefetch_started:
                    deadline = time.monotonic() + budget_seconds
            result = probe_remote_subtitle_streams(client, path, required_language)
        outcome = put(path, identity, result)
        persist_ledger()
        return outcome

    def prefetch(video_rows: Sequence[object]) -> None:
        nonlocal deadline, prefetch_started, prefetch_finished
        unique: dict[str, dict[str, object]] = {}
        for raw in video_rows:
            identity = _subtitle_evidence_identity(raw, required_language)
            if identity is not None:
                unique.setdefault(str(identity["path"]), identity)
        if not unique:
            return
        with lock:
            if prefetch_finished:
                return
            identities.update(unique)
            if deadline is None:
                deadline = time.monotonic() + budget_seconds
            prefetch_started = True

        candidates = sorted(unique.values(), key=lambda row: str(row["path"]).casefold())
        language = str(candidates[0]["language"])
        ledger.invalidate_mutated(candidates)
        stale: list[dict[str, object]] = []
        for identity in candidates:
            path = str(identity["path"])
            persisted = ledger.lookup(identity)
            if persisted is not None and persisted.get("status") in _SUBTITLE_EVIDENCE_DEFINITIVE_STATUSES:
                put(path, identity, persisted)
                continue
            if persisted is not None:
                prior_unknowns[path] = persisted
            stale.append(identity)

        selected: list[dict[str, object]] = []
        if stale:
            # Start immediately after the last selected path.  The path based
            # cursor remains useful when the library grows or cache hits are
            # removed between audits; it never repeatedly slices at index 0.
            cursor = ledger.cursor(language)
            ordered_keys = [str(row["path"]).casefold() for row in candidates]
            start = 0
            if cursor is not None:
                cursor_key = cursor.casefold()
                start = next(
                    (index for index, value in enumerate(ordered_keys) if value > cursor_key),
                    0,
                )
            stale_paths = {str(row["path"]) for row in stale}
            for offset in range(len(candidates)):
                candidate = candidates[(start + offset) % len(candidates)]
                if str(candidate["path"]) not in stale_paths:
                    continue
                selected.append(candidate)
                if len(selected) >= min(max_files, batch_size):
                    break
            if selected:
                ledger.set_cursor(language, str(selected[-1]["path"]))

        selected_paths = {str(row["path"]) for row in selected}
        for identity in stale:
            path = str(identity["path"])
            if path in selected_paths:
                continue
            with lock:
                deferred_paths.add(path)
            put(
                path,
                identity,
                prior_unknowns.get(path) or unknown("subtitle_probe_batch_deferred"),
            )

        if not selected:
            with lock:
                prefetch_finished = True
            persist_ledger()
            return

        futures: dict[object, dict[str, object]] = {}
        next_index = 0
        pool: ThreadPoolExecutor | None = None

        def submit_available() -> None:
            nonlocal next_index
            if pool is None or deadline is None:
                return
            while (
                next_index < len(selected)
                and len(futures) < workers
                and time.monotonic() < deadline
            ):
                identity = selected[next_index]
                next_index += 1
                path = str(identity["path"])
                try:
                    future = pool.submit(
                        probe_remote_subtitle_streams,
                        client,
                        path,
                        required_language,
                    )
                except Exception:
                    put(path, identity, unknown("subtitle_probe_error"))
                    continue
                futures[future] = identity

        try:
            pool = ThreadPoolExecutor(max_workers=workers)
            submit_available()
            while futures:
                assert deadline is not None
                remaining_seconds = deadline - time.monotonic()
                if remaining_seconds <= 0:
                    break
                done, _ = wait(
                    tuple(futures),
                    timeout=remaining_seconds,
                    return_when=FIRST_COMPLETED,
                )
                if not done:
                    break
                for future in done:
                    identity = futures.pop(future)
                    try:
                        result = future.result()
                    except Exception:
                        result = unknown("subtitle_probe_error")
                    put(str(identity["path"]), identity, result)
                submit_available()
        finally:
            # Consume only completed work, then mark every not-yet-complete
            # selection as unknown before returning.  A late worker cannot
            # overwrite this fail-closed cache because it has no callback.
            for future, identity in list(futures.items()):
                if not getattr(future, "done", lambda: False)():
                    continue
                try:
                    result = future.result()
                except Exception:
                    result = unknown("subtitle_probe_error")
                put(str(identity["path"]), identity, result)
                futures.pop(future, None)
            for identity in list(futures.values()) + selected[next_index:]:
                put(
                    str(identity["path"]),
                    identity,
                    unknown("subtitle_probe_budget_exhausted"),
                )
            if pool is not None:
                for future in tuple(futures):
                    try:
                        future.cancel()
                    except Exception:
                        pass
                try:
                    pool.shutdown(wait=False, cancel_futures=True)
                except TypeError:  # pragma: no cover - Python < 3.9 fallback
                    pool.shutdown(wait=False)
                except Exception:
                    pass
            with lock:
                prefetch_finished = True
            persist_ledger()

    setattr(check, "prefetch", prefetch)
    return check


def _subtitle_satisfies(paths: Sequence[str], video_path: str, required: set[str]) -> bool:
    parent = PurePosixPath(video_path).parent
    stem = PurePosixPath(video_path).stem.casefold()
    sidecars = [
        path for path in paths
        if PurePosixPath(path).parent == parent
        and PurePosixPath(path).suffix.casefold() in SUBTITLE_SUFFIXES
        and PurePosixPath(path).stem.casefold().split(".", 1)[0] == stem
    ]
    if not sidecars:
        return False
    if not required:
        return True
    # A sidecar without a language marker is useful evidence for the
    # configured default language used by many small libraries.
    return any(
        not _language_keys(PurePosixPath(path).stem)
        or _language_keys(PurePosixPath(path).stem) & required
        for path in sidecars
    )


def _subtitle_check_evidence(
    checker: Callable[[str], object] | None,
    video_path: str,
) -> Mapping[str, object]:
    """Return a fail-closed, report-safe subtitle evidence row."""
    if checker is None:
        return {"status": "unknown", "reason": "subtitle_evidence_unavailable"}
    try:
        result = checker(video_path)
    except Exception:
        return {"status": "unknown", "reason": "subtitle_probe_error"}
    if result is True or result is False:
        # Small injected test/checker callables predate the structured probe
        # contract.  Their boolean false remains an explicit missing verdict;
        # the durable AList checker itself never writes this synthetic source.
        return {"status": "satisfied" if result else "missing", "source": "injected"}
    if isinstance(result, Mapping):
        status = str(result.get("status") or "").casefold()
        if status in {"present", "embedded", "external"}:
            result = {**result, "status": "satisfied"}
        elif status == "absent":
            result = {**result, "status": "missing"}
        return _normalise_subtitle_evidence_result(result)
    return {"status": "unknown", "reason": "subtitle_probe_error"}


def _subtitle_check_result(
    checker: Callable[[str], object] | None,
    video_path: str,
) -> bool | None:
    """Compatibility projection for callers that need only the verdict."""
    status = str(_subtitle_check_evidence(checker, video_path).get("status") or "").casefold()
    if status == "satisfied":
        return True
    if status == "missing":
        return False
    return None


def _semantic_gap(
    *, kind: str, label: str, reason: str, work: Mapping[str, object],
    season: int | None = None, episode: int | None = None, path: str | None = None,
    subtitle_language: str | None = None,
    episode_title: str | None = None,
    episode_title_aliases: Sequence[str] = (),
) -> dict[str, object]:
    metadata = _work_metadata(work)
    tmdb_id = _as_positive_int(metadata.get("tmdb_id"))
    title = str(metadata.get("title") or metadata.get("name") or "").strip()
    target_root = _canonical_optional_path(metadata.get("target_root") or metadata.get("series_root"))
    row: dict[str, object] = {
        "id": f"{kind}:{tmdb_id or 'unknown'}:{label}",
        "kind": kind,
        "label": label,
        "reason": reason,
        "source": "automatic_library_audit",
        "media": {
            "tmdb_id": tmdb_id,
            "title": title,
            "original_title": str(metadata.get("original_title") or "").strip(),
            "year": str(metadata.get("year") or "").strip(),
            "target_root": target_root or "",
            "media_type": str(metadata.get("media_type") or metadata.get("type") or "").casefold(),
            **({"media_format": str(metadata.get("media_format"))} if metadata.get("media_format") else {}),
        },
    }
    if season is not None:
        row["season"] = season
    if episode is not None:
        row["episode"] = episode
    if path is not None:
        row["path"] = path
    if subtitle_language is not None:
        row["subtitle_language"] = subtitle_language
    if isinstance(episode_title, str):
        text = episode_title.strip()
        if text and "\x00" not in text and len(text) <= 512:
            row["title"] = text
    aliases: list[str] = []
    seen_aliases = {str(row.get("title") or "").casefold()}
    for value in episode_title_aliases:
        if not isinstance(value, str):
            continue
        text = value.strip()
        if not text or "\x00" in text or len(text) > 512 or text.casefold() in seen_aliases:
            continue
        seen_aliases.add(text.casefold())
        aliases.append(text)
        if len(aliases) >= 7:
            break
    if aliases:
        row["title_aliases"] = aliases
    return row


def build_automatic_library_gaps(
    report: Mapping[str, object],
    works: Sequence[Mapping[str, object]] = (),
    *,
    episode_catalog: Mapping[object, object] | Callable[[Mapping[str, object]], object] | None = None,
    required_subtitle_language: str | Sequence[str] | None = None,
    subtitle_checker: Callable[[str], object] | None = None,
) -> dict[str, object]:
    """Derive machine-readable semantic gaps from a completed inventory.

    Works come from completed Engine job identities or an unambiguous library
    NFO. A work without a reliable target or TMDB identity becomes unknown.

    Expected episode data may live on each work (expected_episodes,
    official_episodes or seasons) or be supplied by episode_catalog. If no
    catalog is available we report the work as unknown instead of claiming that
    its episode set is complete. A missing subtitle becomes a gap only when an
    injected subtitle checker explicitly says it is absent; without a probe,
    absent sidecars are reported as unknown because an embedded subtitle might
    exist.
    """
    if not isinstance(report, Mapping):
        raise TypeError("audit report must be an object")
    complete = report.get("complete") is True and report.get("status") == "completed"
    inventory = report.get("inventory") if isinstance(report.get("inventory"), list) else []
    file_rows = [
        row for row in inventory
        if isinstance(row, Mapping) and row.get("type") == "file" and isinstance(row.get("path"), str)
    ]
    file_paths = [str(row.get("path")) for row in file_rows]
    video_paths = [path for path in file_paths if _kind(path) == "video"]
    # Preserve the exact inventory size/version for the durable checker.  A
    # path-only checker remains supported, but it must not reuse evidence
    # across restarts because a same-named remote object could have changed.
    video_rows_by_path: dict[str, dict[str, object]] = {}
    for row in file_rows:
        path = str(row.get("path"))
        if _kind(path) != "video":
            continue
        evidence_row: dict[str, object] = {"path": path, "size": row.get("size")}
        if "version" in row:
            evidence_row["version"] = row.get("version")
        video_rows_by_path.setdefault(path, evidence_row)
    observations = report.get("observations") if isinstance(report.get("observations"), Mapping) else {}
    # Recompute the narrow observation from inventory + identity evidence
    # instead of trusting a persisted observations payload.  This matters for
    # the public helper too: a caller must not be able to hide an arbitrary
    # video from unknown aggregation merely by placing its path in JSON.
    ancillary_media = _observed_ancillary_media(report, works)
    ancillary_paths = {
        path
        for row in ancillary_media
        if (path := _canonical_optional_path(row.get("path"))) is not None
    }
    raw_media_dirs = observations.get("media_directories") if isinstance(observations, Mapping) else []
    media_dirs = [row for row in raw_media_dirs if isinstance(row, Mapping)] if isinstance(raw_media_dirs, list) else []
    required: set[str] = set()
    if isinstance(required_subtitle_language, str):
        required = _language_keys(required_subtitle_language)
    elif isinstance(required_subtitle_language, (list, tuple, set, frozenset)):
        for item in required_subtitle_language:
            required.update(_language_keys(item))

    # Probe only videos that lack a matching external sidecar.  The checker
    # supplied by the composition root exposes an optional bounded prefetch;
    # ordinary injected test callables remain untouched and synchronous.
    prefetch = getattr(subtitle_checker, "prefetch", None)
    if required and callable(prefetch) and works:
        probe_paths = [
            video_rows_by_path.get(path, {"path": path}) for path in video_paths
            if not _subtitle_satisfies(file_paths, path, required)
        ]
        prefetch(probe_paths)

    gaps: list[dict[str, object]] = []
    unknowns: list[dict[str, object]] = []
    work_results: list[dict[str, object]] = []
    covered_video_paths: set[str] = set()
    for raw_work in works:
        if not isinstance(raw_work, Mapping):
            continue
        metadata = _work_metadata(raw_work)
        target_root = _canonical_optional_path(metadata.get("target_root") or metadata.get("series_root"))
        tmdb_id = _as_positive_int(metadata.get("tmdb_id"))
        title = str(metadata.get("title") or metadata.get("name") or "").strip()
        work_key = f"tmdb:{tmdb_id}" if tmdb_id is not None else title or "unknown-work"
        result: dict[str, object] = {
            "work": work_key, "target_root": target_root, "gaps": [], "unknown": False,
        }
        sources = metadata.get("identity_sources")
        if isinstance(sources, (list, tuple)) and all(isinstance(item, str) for item in sources):
            result["identity_sources"] = list(sources)
        elif isinstance(metadata.get("identity_source"), str):
            result["identity_sources"] = [str(metadata["identity_source"])]
        if isinstance(metadata.get("owner_job_id"), str) and metadata["owner_job_id"].strip():
            result["owner_job_id"] = metadata["owner_job_id"].strip()
        if target_root is None or tmdb_id is None:
            result["unknown"] = True
            unknowns.append({
                "kind": "unknown_work_identity", "work": work_key, "target_root": target_root,
            })
            work_results.append(result)
            continue
        if not complete:
            result["unknown"] = True
            unknowns.append({
                "kind": "unknown_inventory", "work": work_key, "target_root": target_root,
            })
            work_results.append(result)
            continue

        raw_scope = metadata.get("identity_scope")
        scope_kind = (
            str(raw_scope.get("kind") or "").casefold()
            if isinstance(raw_scope, Mapping)
            else ""
        )
        if scope_kind in {
            "ambiguous_empty_movie_directory",
            "ambiguous_duplicate_nfo_identity",
            "ambiguous_legacy_target_tree",
            "ambiguous_tv_container",
        }:
            # The NFO gives useful identity evidence, but no safe media
            # boundary.  Do not create an acquisition request for it and do
            # not let an empty directory disappear from the completion gate.
            result["unknown"] = True
            unknowns.append({
                "kind": (
                    "unknown_legacy_identity_scope"
                    if scope_kind == "ambiguous_legacy_target_tree"
                    else "unknown_identity_scope"
                ),
                "work": work_key,
                "target_root": target_root,
                "reason": (
                    "历史任务没有精确媒体范围，不能覆盖同目录的 NFO 作品"
                    if scope_kind == "ambiguous_legacy_target_tree"
                    else "NFO 身份对应的媒体范围不唯一，不能自动归属或补源"
                ),
            })
            work_results.append(result)
            continue

        work_videos, scoped_media_paths = _identity_scope_members(
            metadata,
            target_root=target_root,
            video_paths=video_paths,
            media_dirs=media_dirs,
        )
        covered_video_paths.update(work_videos)
        media_type = str(metadata.get("media_type") or metadata.get("type") or "tv").casefold()
        expected, catalog_titles = _catalog_episode_evidence_for_work(
            raw_work, episode_catalog,
        )
        if media_type == "movie" and not work_videos:
            gap = _semantic_gap(
                kind="missing_media", label=title or "电影正片",
                reason="已知电影目标目录没有可用视频文件",
                work=raw_work, path=target_root,
            )
            gaps.append(gap)
            result["gaps"].append(gap)
        elif media_type != "movie":
            default_season = _as_positive_int(metadata.get("season"))
            covered = {
                token
                for path in work_videos
                for token in _episode_tokens(path, default_season=default_season)
            }
            if not expected:
                result["unknown"] = True
                unknowns.append({
                    "kind": "unknown_episode_catalog", "work": work_key,
                    "target_root": target_root,
                })
            else:
                for season, numbers in sorted(expected.items()):
                    for episode in sorted(numbers):
                        if (season, episode) in covered:
                            continue
                        label = f"{title + ' ' if title else ''}S{season:02d}E{episode:02d}"
                        gap = _semantic_gap(
                            kind="missing_episode", label=label,
                            reason="作品目录和正式库都没有该已知集数的视频",
                            work=raw_work, season=season, episode=episode,
                            episode_title=(
                                catalog_titles[(season, episode)][0]
                                if catalog_titles.get((season, episode)) else None
                            ),
                            episode_title_aliases=(
                                catalog_titles.get((season, episode), [])[1:]
                            ),
                        )
                        gaps.append(gap)
                        result["gaps"].append(gap)

        if required:
            for path in work_videos:
                if _subtitle_satisfies(file_paths, path, required):
                    continue
                subtitle_evidence = _subtitle_check_evidence(subtitle_checker, path)
                subtitle_status = str(subtitle_evidence.get("status") or "").casefold()
                if subtitle_status != "missing":
                    result["unknown"] = True
                    unknowns.append({
                        "kind": "unknown_subtitle_evidence", "work": work_key,
                        "target_root": target_root, "path": path,
                        "reason": _safe_subtitle_evidence_reason(subtitle_evidence.get("reason")),
                    })
                    continue
                label = f"{title + ' ' if title else ''}{PurePosixPath(path).name}"
                gap = _semantic_gap(
                    kind="missing_subtitle", label=label,
                    reason="字幕探针确认视频缺少配置语言的内封或外挂字幕",
                    work=raw_work, path=path,
                    subtitle_language=str(required_subtitle_language or "zh"),
                )
                gaps.append(gap)
                result["gaps"].append(gap)

        # Translate only media directories owned by this exact identity scope.
        # A library NFO must never turn its containing bundle into a blanket
        # ownership claim; jobs without an explicit scope retain the legacy
        # target-tree fallback in ``_identity_scope_members``.
        for media in media_dirs:
            media_path = _canonical_optional_path(media.get("path"))
            if media_path is None or media_path not in scoped_media_paths:
                continue
            for field, kind, reason in (
                ("has_nfo", "missing_nfo", "作品目录缺少 NFO 元数据"),
                ("has_poster", "missing_poster", "作品目录缺少海报"),
            ):
                if media.get(field) is True:
                    continue
                gap = _semantic_gap(
                    kind=kind, label=PurePosixPath(media_path).name or title,
                    reason=reason, work=raw_work, path=media_path,
                )
                gaps.append(gap)
                result["gaps"].append(gap)
        work_results.append(result)

    videos_by_media_path: dict[str, list[str]] = defaultdict(list)
    for path in video_paths:
        videos_by_media_path[posixpath.dirname(path)].append(path)
    for media in media_dirs:
        media_path = _canonical_optional_path(media.get("path"))
        if media_path is None:
            continue
        uncovered = sorted(
            (
                path for path in videos_by_media_path.get(media_path, [])
                if path not in covered_video_paths and path not in ancillary_paths
            ),
            key=str.casefold,
        )
        if not uncovered:
            continue
        unknowns.append({
            "kind": "unknown_library_work",
            "path": media_path,
            "reason": "正式库作品目录没有对应的自动任务身份",
            "uncovered_video_paths": uncovered,
        })

    # Preserve order while avoiding duplicates when parent/child records
    # describe the same known work.
    unique: dict[str, dict[str, object]] = {}
    for gap in gaps:
        unique.setdefault(str(gap.get("id") or ""), gap)
    gaps = list(unique.values())
    projects_by_key: dict[tuple[int, str, str], dict[str, object]] = {}
    for gap in gaps:
        kind = str(gap.get("kind") or "")
        if kind not in {"missing_media", "missing_episode", "missing_season"}:
            continue
        media = gap.get("media") if isinstance(gap.get("media"), Mapping) else {}
        tmdb_id = _as_positive_int(media.get("tmdb_id"))
        target_root = _canonical_optional_path(media.get("target_root"))
        if tmdb_id is None or target_root is None:
            continue
        media_type = str(media.get("media_type") or "tv").casefold()
        project_mode = "movie" if kind == "missing_media" else "tv"
        if project_mode == "movie" and media_type not in {"movie", ""}:
            continue
        if project_mode == "tv" and media_type not in {"tv", "mixed", ""}:
            continue
        key = (tmdb_id, target_root, project_mode)
        project = projects_by_key.setdefault(key, {
            "project_key": f"tmdb:{project_mode}:{tmdb_id}",
            "tmdb_id": tmdb_id,
            "target_root": target_root,
            "gaps": [],
            "plan": {
                "mode": project_mode,
                "source_root": target_root,
                "target_root": target_root,
                "metadata": {
                    "tmdb_id": tmdb_id,
                    "title": media.get("title"),
                    "original_title": media.get("original_title"),
                    "year": media.get("year"),
                    "media_type": project_mode,
                    "media_format": media.get("media_format"),
                },
                "scan_report": {"resource_gaps": []},
            },
        })
        project["gaps"].append(gap)
        project["plan"]["scan_report"]["resource_gaps"].append(gap)
    projects = list(projects_by_key.values())
    return {
        "status": "completed" if complete else "unknown",
        "complete": complete,
        "gaps": gaps,
        "gap_count": len(gaps),
        "unknowns": unknowns,
        "unknown_count": len(unknowns),
        "works": work_results,
        # These are explicit observed extras only.  They are intentionally
        # absent from coverage, gaps and acquisition projects.
        "ancillary_media": ancillary_media,
        # Each plan is ready for provider-neutral automatic acquisition.
        "acquisition_projects": projects,
    }


def attach_automatic_gaps(
    report: Mapping[str, object],
    works: Sequence[Mapping[str, object]] = (),
    *,
    episode_catalog: Mapping[object, object] | Callable[[Mapping[str, object]], object] | None = None,
    required_subtitle_language: str | Sequence[str] | None = None,
    subtitle_checker: Callable[[str], object] | None = None,
) -> dict[str, object]:
    """Return a copy of a structural report with automatic semantic gaps."""
    enriched = dict(report)
    raw_observations = report.get("observations")
    observations = dict(raw_observations) if isinstance(raw_observations, Mapping) else {}
    observations["ancillary_media"] = _observed_ancillary_media(report, works)
    enriched["observations"] = observations
    semantic = build_automatic_library_gaps(
        enriched, works, episode_catalog=episode_catalog,
        required_subtitle_language=required_subtitle_language,
        subtitle_checker=subtitle_checker,
    )
    enriched["semantic"] = semantic
    enriched["gaps"] = list(semantic["gaps"])
    enriched["unknowns"] = list(semantic["unknowns"])
    if semantic["gaps"] or semantic["unknowns"]:
        enriched["clean"] = False
    # ``complete`` remains the structural scan result. Library completion is a
    # stricter business state: every structural, semantic and evidence issue
    # must have been resolved.
    library_complete = bool(
        enriched.get("complete") is True
        and enriched.get("clean") is True
        and semantic["gap_count"] == 0
        and semantic["unknown_count"] == 0
    )
    semantic["library_complete"] = library_complete
    enriched["library_complete"] = library_complete
    return enriched


def audit_and_persist(
    client: AListDirectoryLister | object | None,
    state_root: str | Path,
    *,
    works: Sequence[Mapping[str, object]] = (),
    formal_roots: Sequence[str] = DEFAULT_FORMAL_LIBRARY_ROOTS,
    max_directories: int = 20_000,
    max_files: int = 250_000,
    clock: Callable[[], str] | None = None,
    episode_catalog: Mapping[object, object] | Callable[[Mapping[str, object]], object] | None = None,
    required_subtitle_language: str | Sequence[str] | None = None,
    subtitle_checker: Callable[[str], object] | None = None,
) -> dict[str, object]:
    """Write the replaceable structural plus optional semantic report."""
    auditor = SimpleLibraryAuditor(
        client, formal_roots=formal_roots, max_directories=max_directories,
        max_files=max_files, clock=clock,
    )
    report = auditor.scan()
    if works or episode_catalog is not None or required_subtitle_language is not None:
        report = attach_automatic_gaps(
            report, works, episode_catalog=episode_catalog,
            required_subtitle_language=required_subtitle_language,
            subtitle_checker=subtitle_checker,
        )
    atomic_write_json(latest_audit_path(state_root), report, allow_nan=False)
    return report


def run_automatic_library_audit(
    client: AListDirectoryLister | object | None,
    state_root: str | Path,
    jobs: Sequence[object],
    *,
    tmdb_client: object | None = None,
    formal_roots: Sequence[str] = DEFAULT_FORMAL_LIBRARY_ROOTS,
    required_subtitle_language: str | Sequence[str] | None = None,
    subtitle_checker: Callable[[str], object] | None = None,
    max_directories: int = 20_000,
    max_files: int = 250_000,
    clock: Callable[[], str] | None = None,
) -> dict[str, object]:
    """Run the complete read-only audit using library and persisted-job facts.

    The caller supplies existing AList/TMDB clients and automatic jobs. NFO
    bootstrap is deliberately report-only: it does not create a root job or
    authorize provider activity for an old library directory.
    """
    auditor = SimpleLibraryAuditor(
        client,
        formal_roots=formal_roots,
        max_directories=max_directories,
        max_files=max_files,
        clock=clock,
    )
    structural_report = auditor.scan()
    job_works = automatic_works_from_engine_jobs(jobs)
    library_works = bootstrap_automatic_works_from_library(
        structural_report,
        client,
        formal_roots=auditor.roots,
    )
    works, unowned_library_works = _merge_library_and_job_works(library_works, job_works)
    # A content witness is deliberately applied only after both the structural
    # library identities and persisted task identities have been merged.  It
    # may add one exact movie file to its already-proven parent movie scope;
    # it never broadens a TV tree or crosses a ``tvshow.nfo`` boundary by
    # pathname inference.
    works, content_identity_override_diagnostics = apply_content_identity_overrides(
        structural_report,
        works,
        client,
        state_root,
    )
    catalog = TmdbEpisodeCatalog(tmdb_client) if tmdb_client is not None else None
    if catalog is not None:
        # Warm one snapshot per unique TV identity before the semantic pass.
        # This is deliberately best-effort; a failed warm-up is cached as None
        # and remains an unknown catalog rather than a completed one.
        try:
            catalog.prefetch(works, max_workers=_tmdb_audit_workers())
        except Exception:
            # The normal call path below still performs a conservative lookup;
            # if that lookup fails, semantic evidence remains unknown.
            pass
    report = attach_automatic_gaps(
        structural_report,
        works,
        episode_catalog=catalog,
        required_subtitle_language=required_subtitle_language,
        subtitle_checker=subtitle_checker,
    )
    semantic = report.get("semantic")
    if isinstance(semantic, dict):
        semantic["content_identity_overrides"] = content_identity_override_diagnostics
        job_gaps = automatic_job_gaps(jobs)
        semantic["job_gaps"] = job_gaps
        semantic["job_gap_count"] = len(job_gaps)
        unowned_targets = {key[1] for key in unowned_library_works}
        raw_gaps = semantic.get("gaps") if isinstance(semantic.get("gaps"), list) else []
        raw_unknowns = semantic.get("unknowns") if isinstance(semantic.get("unknowns"), list) else []
        unowned_gap_count = sum(
            1
            for gap in raw_gaps
            if isinstance(gap, Mapping)
            and isinstance(gap.get("media"), Mapping)
            and (
                _as_positive_int(gap["media"].get("tmdb_id")),
                _canonical_optional_path(gap["media"].get("target_root")),
                str(gap["media"].get("media_type") or "").casefold(),
            ) in unowned_library_works
        )
        def belongs_to_unowned_target(unknown: Mapping[str, object]) -> bool:
            raw_path = unknown.get("target_root") or unknown.get("path")
            path = _canonical_optional_path(raw_path)
            return path is not None and any(_inside(path, target) for target in unowned_targets)

        unowned_unknown_count = sum(
            1
            for unknown in raw_unknowns
            if isinstance(unknown, Mapping) and belongs_to_unowned_target(unknown)
        )
        semantic["bootstrap"] = {
            "nfo_work_count": len(library_works),
            "unowned_work_count": len(unowned_library_works),
            "unowned_gap_count": unowned_gap_count,
            "unowned_unknown_count": unowned_unknown_count,
            # Ownership/root-job creation is intentionally outside this
            # read-only identity bootstrap step.
            "creates_owner_tasks": False,
        }
        if job_gaps:
            report["clean"] = False
        library_complete = bool(
            report.get("complete") is True
            and report.get("clean") is True
            and semantic.get("gap_count") == 0
            and semantic.get("unknown_count") == 0
            and not job_gaps
        )
        semantic["library_complete"] = library_complete
        report["library_complete"] = library_complete
        atomic_write_json(latest_audit_path(state_root), report, allow_nan=False)
    return report


__all__ = [
    "AListDirectoryLister", "DEFAULT_FORMAL_LIBRARY_ROOTS", "POSTER_SUFFIXES",
    "SimpleLibraryAuditError", "SimpleLibraryAuditUnavailable", "SimpleLibraryAuditor",
    "SUBTITLE_SUFFIXES", "TEMPORARY_SUFFIXES", "VIDEO_SUFFIXES",
    "TmdbEpisodeCatalog", "attach_automatic_gaps", "audit_and_persist",
    "automatic_job_gaps", "automatic_works_from_engine_jobs",
    "bootstrap_automatic_works_from_library", "build_automatic_library_gaps",
    "classify_embedded_subtitle_streams", "latest_audit_path",
    "make_alist_subtitle_checker", "probe_remote_subtitle_streams",
    "run_automatic_library_audit",
]
