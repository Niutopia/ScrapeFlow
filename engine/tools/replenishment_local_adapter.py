#!/usr/bin/env python3
"""Local catalog + aria2 + AList replenishment adapter.

Search is read-only and returns only candidates bound to the request TMDB ID.
Acquisition downloads the selected torrent files into an isolated workspace,
verifies exact file indices and sizes, uploads canonical episode names into the
unscraped AList root, verifies the remote rows, and removes successful local
staging data.
"""

from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager
import fcntl
import hashlib
from html.parser import HTMLParser
import json
import os
from pathlib import Path
import posixpath
import re
import shutil
import socket
import ssl
import subprocess
import sys
import tempfile
import time
from typing import Any, Iterable, Mapping
import urllib.error
import urllib.parse
import urllib.request
import uuid
import xml.etree.ElementTree as ET

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from engine.scraper import AListClient, ApiError, ScraperError, join_remote, split_remote
from engine.scrapeflow.alist_exact_file_adapter import AListExactFileAdapter
from engine.scrapeflow.local_upload_transaction import (
    LocalUploadSpec,
    LocalUploadTransactionError,
    deterministic_local_upload_id,
    run_local_upload_transaction,
)
from engine.scrapeflow.quark_fast_save_bridge import (
    QuarkBridgeError, QuarkFastSaveBridge, QuarkMagnetDeliveryError,
    QuarkMagnetInDoubtError,
    QuarkMagnetOfflineBridge, QuarkNativeHelperTransport, UrlLibQuarkTransport,
    QuarkShareExpiredError, QuarkShareInDoubtError, delegated_quark_session,
    normalize_quark_fast_save_selection,
)
from engine.scrapeflow.replenishment_acquisition import (
    AcquisitionRouteError, acquisition_lane,
)
from engine.scrapeflow.serialization import atomic_write_json
from engine.tools.extract_archives import (
    _local_archive_listing, _seven_zip_password_input,
    _validate_local_extraction_budget,
)


class ReplenishmentDeliveryError(RuntimeError):
    """The verified payload is reusable; only cloud delivery failed."""

    failure_scope = "delivery"
    reusable_candidate = True
    exclude_candidate = False

    def __init__(self, message: str, *, stage: str = "delivery") -> None:
        super().__init__(message)
        self.failure_stage = stage


class ReplenishmentCandidateError(RuntimeError):
    """The selected release itself failed identity, manifest, or acquisition checks."""

    failure_scope = "candidate"
    reusable_candidate = False
    exclude_candidate = True

    def __init__(
        self, message: str, *, stage: str = "candidate_acquire",
        candidate: Mapping[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.failure_stage = stage
        self.candidate = {
            key: candidate.get(key)
            for key in ("provider", "release_name", "locator", "infohash")
            if candidate is not None and candidate.get(key) is not None
        }


class ReplenishmentInfrastructureError(RuntimeError):
    """Local capacity, dependencies, or orchestration failed without blaming a release."""

    failure_scope = "infrastructure"
    reusable_candidate = False
    exclude_candidate = False

    def __init__(self, message: str, *, stage: str = "infrastructure") -> None:
        super().__init__(message)
        self.failure_stage = stage


@contextmanager
def _workspace_lease(root: Path, workspace_key: str):
    """Prevent two retries from mutating one deterministic workspace at once."""
    lock_dir = root / ".locks"
    lock_dir.mkdir(parents=True, exist_ok=True)
    lock_path = lock_dir / f"{workspace_key}.lock"
    with lock_path.open("a+", encoding="utf-8") as handle:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise ReplenishmentInfrastructureError(
                "相同补源工作区已有获取进程在运行",
                stage="orchestration_concurrency",
            ) from exc
        try:
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


@contextmanager
def _search_capacity_lease(root: Path):
    """Bound API-heavy search subprocesses across restored scheduler threads."""
    try:
        slots = int(os.getenv("SCRAPEFLOW_REPLENISHMENT_SEARCH_WORKERS", "2"))
    except ValueError:
        slots = 2
    slots = max(1, min(8, slots))
    state_root = os.getenv("SCRAPEFLOW_STATE_DIR", "").strip()
    lock_dir = (
        Path(state_root) / ".replenishment-search-locks"
        if state_root and Path(state_root).is_absolute()
        else root / ".search-locks"
    )
    lock_dir.mkdir(parents=True, exist_ok=True)
    acquired = None
    try:
        while acquired is None:
            for index in range(slots):
                handle = (lock_dir / f"slot-{index}.lock").open("a+", encoding="utf-8")
                try:
                    fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                except BlockingIOError:
                    handle.close()
                    continue
                acquired = handle
                break
            if acquired is None:
                time.sleep(0.25)
        yield
    finally:
        if acquired is not None:
            fcntl.flock(acquired.fileno(), fcntl.LOCK_UN)
            acquired.close()


def _selection_workspace_key(selection_wrapper: Mapping[str, Any]) -> str:
    selection = selection_wrapper.get("selection")
    rows = selection.get("selections") if isinstance(selection, Mapping) else []
    identities = []
    for row in rows if isinstance(rows, list) else []:
        if not isinstance(row, Mapping):
            continue
        acquisition = row.get("acquisition")
        gap_map = acquisition.get("file_index_by_gap") if isinstance(acquisition, Mapping) else {}
        file_id_map = acquisition.get("file_id_by_gap") if isinstance(acquisition, Mapping) else {}
        indices = sorted({
            int(value) for values in (gap_map.values() if isinstance(gap_map, Mapping) else [])
            if isinstance(values, list) for value in values if type(value) is int
        })
        file_ids = sorted({
            str(value) for values in (
                file_id_map.values() if isinstance(file_id_map, Mapping) else []
            )
            for value in (
                [values] if isinstance(values, str)
                else values if isinstance(values, list) else []
            )
            if isinstance(value, str) and value
        })
        identity = {
            "infohash": str(row.get("infohash") or "").casefold(),
            "locator": (
                "" if row.get("infohash") else str(row.get("locator") or "")
            ),
            "indices": indices,
        }
        # Preserve historical torrent/share workspace keys.  Only archive
        # selections need the additional discriminator, and an empty field
        # would otherwise orphan resumable payloads created by older builds.
        if file_ids:
            identity["file_ids"] = file_ids
        identities.append(identity)
    encoded = json.dumps(identities, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()[:16]
from local.scrapeflow_api.replenishment import (
    _coverage_tokens, _expanded_episode_ids, _normalized_text, _season_markers,
)


VIDEO_EXTENSIONS = frozenset({".mkv", ".mp4", ".avi", ".m2ts", ".ts", ".mov", ".webm"})
MAX_TORRENT_BYTES = 8 * 1024 * 1024
ANIME_PLAIN_EPISODE_RE = re.compile(
    r"(?:^|[\s._-])0*(\d{1,3})(?=\s*(?:\[[^\]]+\]\s*)*$)", re.I,
)
OPTIONAL_EPISODE_RE = re.compile(
    r"(?i)(?:OVA|OAD|SP|SPECIAL|PICTURE[\s._-]*DRAMA|PLAY)\s*#?0*(\d{1,3})"
)
OPTIONAL_CONTAINER_RE = re.compile(
    r"(?i)(?:^|/)(?:SP|SPECIALS?|OVA|OAD|EXTRAS?)(?:/|$)"
)
OPTIONAL_PAST_ARC_RE = re.compile(r"(?:过去|過去)篇\s*0*(\d{1,3})", re.I)
OPTIONAL_PAST_ARC_NAME_RE = re.compile(r"(?:过去|過去)篇", re.I)
OPTIONAL_NEWLYWED_RE = re.compile(r"新婚篇(?:\s*0*(\d{1,3}))?", re.I)
OPTIONAL_RETROSPECTIVE_COLLECTION_RE = re.compile(
    r"(?:精选集|精選集|总集篇|總集篇|総集編|総集篇|"
    r"回想篇|回顾篇|回顧篇|回顾集|回顧集|"
    r"\b(?:recap|digest|compilation|retrospective)\b)",
    re.I,
)
OPTIONAL_EXPLICIT_S00_RE = re.compile(r"(?i)(?<![A-Z0-9])S00[ ._-]*E0*\d{1,4}\b")
QUARK_ARCHIVE_PASSWORD_MARKER_RE = re.compile(
    r"(?:(?:解压|压缩包|归档|默认)\s*)?密码\s*"
    r"(?:是|为|[:：=])\s*(?P<password>[^\s,，;；/\\]{1,128})",
    re.I,
)
QUARK_ARCHIVE_PASSWORD_MAX_DIRECTORIES = 64
QUARK_ARCHIVE_PASSWORD_MAX_ENTRIES = 1_024
QUARK_ARCHIVE_PASSWORD_MAX_DEPTH = 12


def _load(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"JSON 根节点需要是对象: {path}")
    return value


def _optional_semantic_keys(value: str) -> set[str]:
    keys = {
        f"past:{int(match.group(1))}"
        for match in OPTIONAL_PAST_ARC_RE.finditer(value)
        if 0 < int(match.group(1)) <= 999
    }
    for match in OPTIONAL_NEWLYWED_RE.finditer(value):
        ordinal = int(match.group(1) or 1)
        if 0 < ordinal <= 999:
            keys.add(f"newlywed:{ordinal}")
    return keys


def _atomic_json(path: Path, value: Mapping[str, Any]) -> None:
    atomic_write_json(path, value, sort_keys=True)


def _infohash_aliases(value: Any) -> set[str]:
    """Return equivalent lowercase hex and base32 torrent infohash forms."""
    raw = str(value or "").strip().casefold()
    if not raw:
        return set()
    aliases = {raw}
    if re.fullmatch(r"[0-9a-f]{40}", raw):
        aliases.add(_base32_infohash(raw))
    elif re.fullmatch(r"[a-z2-7]{32}", raw):
        import base64
        try:
            aliases.add(base64.b32decode(raw.upper()).hex())
        except ValueError:
            pass
    return {item for item in aliases if item}


def _locator_infohash_aliases(values: Iterable[Any]) -> set[str]:
    """Extract normalized BTIH identities carried by persisted locators."""
    aliases: set[str] = set()
    for value in values:
        locator = str(value or "").strip()
        if not locator:
            continue
        if locator.casefold().startswith("quark_magnet:"):
            aliases.update(_infohash_aliases(locator.split(":", 1)[1]))
        match = re.search(
            r"(?i)(?:urn:)?btih:([0-9a-f]{40}|[a-z2-7]{32})\b", locator,
        )
        if match:
            aliases.update(_infohash_aliases(match.group(1)))
    return aliases


_PERMANENT_EXHAUSTION_KINDS = frozenset({
    "search_complete_no_candidates", "resource_failure_floor_reached",
})


def _verified_provider_exhaustion(
    request: Mapping[str, Any], provider: str,
) -> Mapping[str, Any] | None:
    """Return a complete permanent proof; labels alone never unlock a lane."""
    exhausted = request.get("provider_exhausted")
    if not isinstance(exhausted, Mapping):
        return None
    entry = exhausted.get(provider)
    if not isinstance(entry, Mapping) or entry.get("exhausted") is not True:
        return None
    proof = entry.get("proof")
    if not isinstance(proof, Mapping):
        return None
    kind = str(proof.get("kind") or "")
    if kind not in _PERMANENT_EXHAUSTION_KINDS:
        return None
    if kind == "search_complete_no_candidates":
        required = proof.get("required_sources")
        completed = proof.get("completed_sources")
        if (
            not isinstance(required, list)
            or not required
            or not all(isinstance(item, str) and item for item in required)
            or not isinstance(completed, list)
            or not all(isinstance(item, str) and item for item in completed)
            or set(required) - set(completed)
            or type(proof.get("candidate_count")) is not int
            or proof["candidate_count"] != 0
            or type(proof.get("excluded_candidate_count")) is not int
            or proof["excluded_candidate_count"] < 0
        ):
            return None
    else:
        required_floor = proof.get("required_floor")
        distinct_failures = proof.get("distinct_failure_count")
        rules = request.get("rules") if isinstance(
            request.get("rules"), Mapping,
        ) else {}
        try:
            configured_floor = max(
                0, int(rules.get("minimum_attempts_per_cloud_lane") or 0),
            )
        except (TypeError, ValueError):
            return None
        if (
            type(required_floor) is not int
            or required_floor <= 0
            or required_floor < configured_floor
            or type(distinct_failures) is not int
            or distinct_failures < required_floor
        ):
            return None
    return proof


def _local_torrent_unlocked(request: Mapping[str, Any]) -> bool:
    """Tier 3 needs complete required-source exhaustion from tier 2.

    A resource-failure floor is enough to leave one cloud candidate behind,
    but it is not proof that the remaining configured cloud search space has
    been exhausted.  Local Torrent therefore requires the stronger durable
    ``search_complete_no_candidates`` proof produced only after every required
    tier-2 source has completed.  That proof is independently sufficient:
    an actually empty cloud search space must not be forced to manufacture
    thirty resource failures before tier 3 can run.
    """
    proof = _verified_provider_exhaustion(request, "quark_magnet")
    return bool(
        isinstance(proof, Mapping)
        and proof.get("kind") == "search_complete_no_candidates"
    )


def _dynamic_search_timeout_seconds(request: Mapping[str, Any]) -> int:
    """Give required indexes one bounded exhaustion pass after the cloud floor.

    The normal 45-second budget keeps early discovery responsive.  Once a
    durable resource-failure floor proves repeated cloud attempts, continuing
    to inspect only a few new Torrent manifests per round can prevent a
    required index from ever producing its stronger completion proof.  Expand
    only that post-floor pass; this never unlocks local Torrent by itself.
    """
    configured = _bounded_seconds(
        "SCRAPEFLOW_REPLENISHMENT_DYNAMIC_SEARCH_TIMEOUT", 45, 10, 300,
    )
    proof = _verified_provider_exhaustion(request, "quark_magnet")
    if isinstance(proof, Mapping) and proof.get("kind") == "resource_failure_floor_reached":
        return max(configured, 120)
    return configured


def _catalog_path() -> Path | None:
    raw = os.getenv("SCRAPEFLOW_REPLENISHMENT_CATALOG", "").strip()
    if not raw:
        return None
    path = Path(raw)
    if not path.is_file():
        raise ValueError(f"补源候选目录不存在: {path}")
    return path


def _quark_share_code(value: Any) -> str:
    raw = str(value or "").strip()
    if re.fullmatch(r"[A-Za-z0-9_-]{4,128}", raw):
        return raw
    parsed = urllib.parse.urlsplit(raw)
    if parsed.scheme != "https" or parsed.hostname != "pan.quark.cn":
        raise ValueError("夸克分享需要 pan.quark.cn HTTPS 链接或 share_id")
    match = re.fullmatch(r"/s/([A-Za-z0-9_-]{4,128})/?", parsed.path)
    if not match:
        raise ValueError("夸克分享链接格式无效")
    # Share codes are opaque and may be case-sensitive.  Normalize only the
    # URL envelope; preserve the code byte-for-byte for locator and fast-save.
    return match.group(1)


def _quark_index_path() -> Path | None:
    raw = os.getenv("SCRAPEFLOW_REPLENISHMENT_QUARK_INDEX", "").strip()
    if not raw:
        return None
    path = Path(raw)
    if not path.is_file():
        raise ValueError(f"夸克分享索引不存在: {path}")
    return path


def _archive_password_hint(value: Any) -> str:
    """Extract one explicit inline archive password without guessing."""
    if not isinstance(value, str) or len(value) > 512:
        return ""
    match = QUARK_ARCHIVE_PASSWORD_MARKER_RE.search(value)
    if not match:
        return ""
    password = match.group("password").strip()
    if (
        not password or len(password) > 128
        or any(ord(character) < 32 for character in password)
    ):
        return ""
    return password


def _indexed_archive_password(rows: list[dict[str, Any]]) -> str:
    """Resolve a trusted index password or an inline path marker exactly once."""
    matches: set[str] = set()
    for row in rows:
        explicit = row.get("archive_password")
        if explicit is not None:
            if (
                not isinstance(explicit, str) or not explicit
                or len(explicit) > 128
                or any(ord(character) < 32 for character in explicit)
            ):
                raise ValueError("夸克分享索引 archive_password 无效")
            matches.add(explicit)
        for item in row.get("files") or []:
            if not isinstance(item, Mapping):
                continue
            password = _archive_password_hint(item.get("path"))
            if password:
                matches.add(password)
    if len(matches) > 1:
        raise ValueError("夸克分享索引存在冲突的归档密码提示")
    return next(iter(matches), "")


def _discover_quark_share_archive_password(share_id: str, passcode: str = "") -> str:
    """Read bounded share entry names and return one unambiguous password hint.

    The share is never saved or mutated here.  Password files are not opened:
    only explicit inline markers such as ``默认密码是123456`` are trusted.
    """
    client = _alist_client()
    staging_root = os.getenv(
        "SCRAPEFLOW_REPLENISHMENT_ARCHIVE_STAGING_ROOT",
        "/quark/.scrapeflow-replenishment-archives",
    )
    session = delegated_quark_session(client, staging_root)
    bridge = QuarkFastSaveBridge(UrlLibQuarkTransport())
    token = bridge._call(session, "POST", "/share/sharepage/token", body={
        "pwd_id": share_id, "passcode": passcode,
    })
    stoken = (token.get("data") or {}).get("stoken")
    if not isinstance(stoken, str) or not stoken:
        raise ReplenishmentInfrastructureError(
            "夸克分享密码提示扫描未返回 stoken",
            stage="quark_share_password_discovery",
        )

    queue: list[tuple[str, int]] = [("0", 0)]
    visited: set[str] = set()
    entry_count = 0
    matches: set[str] = set()
    while queue:
        parent, depth = queue.pop(0)
        if parent in visited:
            continue
        if len(visited) >= QUARK_ARCHIVE_PASSWORD_MAX_DIRECTORIES:
            raise ReplenishmentInfrastructureError(
                "夸克分享密码提示扫描超过目录上限",
                stage="quark_share_password_discovery",
            )
        visited.add(parent)
        for page in range(1, 42):
            value = bridge._call(
                session, "GET", "/share/sharepage/detail",
                params={
                    "pwd_id": share_id, "stoken": stoken, "pdir_fid": parent,
                    "force": 0, "_page": page, "_size": 100, "_fetch_total": 1,
                },
            )
            rows = (value.get("data") or {}).get("list")
            if not isinstance(rows, list):
                raise ReplenishmentInfrastructureError(
                    "夸克分享密码提示扫描返回格式异常",
                    stage="quark_share_password_discovery",
                )
            entry_count += len(rows)
            if entry_count > QUARK_ARCHIVE_PASSWORD_MAX_ENTRIES:
                raise ReplenishmentInfrastructureError(
                    "夸克分享密码提示扫描超过条目上限",
                    stage="quark_share_password_discovery",
                )
            for row in rows:
                if not isinstance(row, Mapping):
                    continue
                password = _archive_password_hint(row.get("file_name"))
                if password:
                    matches.add(password)
                if row.get("file") is False:
                    child = row.get("fid")
                    if (
                        depth < QUARK_ARCHIVE_PASSWORD_MAX_DEPTH
                        and isinstance(child, str) and child
                    ):
                        queue.append((child, depth + 1))
            if len(rows) < 100:
                break
        else:
            raise ReplenishmentInfrastructureError(
                "夸克分享密码提示扫描超过分页上限",
                stage="quark_share_password_discovery",
            )
    if len(matches) > 1:
        raise ReplenishmentCandidateError(
            "夸克分享存在多个冲突的归档密码提示",
            stage="candidate_archive_password",
        )
    return next(iter(matches), "")


def _quark_sfx_locator(share_id: str, file_id: str, path: str) -> str:
    """Give each physical archive its own durable exclusion identity."""
    physical = file_id.strip()
    if not re.fullmatch(r"[A-Za-z0-9_-]{1,160}", physical):
        physical = hashlib.sha256(
            (file_id + "\0" + path).encode("utf-8")
        ).hexdigest()[:24]
    return f"quark_share:{share_id}:file:{physical}"


def _search_quark_share(
    request: Mapping[str, Any], existing_locators: set[str],
    *, rows_override: list[Mapping[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    """Normalize a trusted read-only share index into fast-save candidates."""
    if rows_override is None:
        path = _quark_index_path()
        if path is None:
            return []
        payload = _load(path)
        rows = payload.get("shares")
    else:
        rows = rows_override
    if not isinstance(rows, list) or not all(isinstance(row, Mapping) for row in rows):
        raise ValueError("夸克分享索引缺少 shares 数组")

    grouped: dict[str, dict[str, Any]] = {}
    for row in rows:
        code = _quark_share_code(row.get("share_id") or row.get("share_url"))
        bucket = grouped.setdefault(code, {"rows": [], "files": {}})
        bucket["rows"].append(dict(row))
        files = row.get("files")
        if not isinstance(files, list):
            raise ValueError(f"夸克分享 {code} 缺少 files 数组")
        for item in files:
            if not isinstance(item, Mapping):
                raise ValueError(f"夸克分享 {code} 文件项格式无效")
            file_id = str(item.get("file_id") or "").strip()
            file_path = str(item.get("path") or "").strip().replace("\\", "/")
            size = item.get("size")
            if not file_id or not file_path or type(size) is not int or size < 0:
                raise ValueError(f"夸克分享 {code} 文件证据不完整")
            normalized = {"file_id": file_id, "path": file_path, "size": size}
            member_path = item.get("member_path")
            member_size = item.get("member_size")
            if member_path is not None or member_size is not None:
                if (
                    not isinstance(member_path, str) or not member_path.strip()
                    or type(member_size) is not int or member_size <= 0
                ):
                    raise ValueError(f"夸克分享 {code} 归档成员证据不完整")
                normalized["member_path"] = member_path.strip().replace("\\", "/")
                normalized["member_size"] = member_size
            previous = bucket["files"].get(file_id)
            if previous is not None and previous != normalized:
                raise ValueError(f"夸克分享 {code} 的 file_id 冲突: {file_id}")
            bucket["files"][file_id] = normalized

    candidates: list[dict[str, Any]] = []
    for code, bucket in sorted(grouped.items()):
        rows_for_share = sorted(
            bucket["rows"], key=lambda row: str(row.get("updated_at") or ""), reverse=True,
        )
        newest = rows_for_share[0]
        payload_kind = str(newest.get("payload_kind") or "video_payload")
        requires_extraction = newest.get("requires_extraction") is True
        archive_format = str(newest.get("archive_format") or "")
        if payload_kind == "archive_payload":
            if not requires_extraction or archive_format != "sfx":
                raise ValueError(
                    f"夸克分享 {code} 的 archive_payload 必须声明 requires_extraction=true/"
                    "archive_format=sfx"
                )
            allowed_payload_extensions = VIDEO_EXTENSIONS | frozenset({".exe"})
        elif payload_kind == "video_payload":
            allowed_payload_extensions = VIDEO_EXTENSIONS
        else:
            raise ValueError(f"夸克分享 {code} payload_kind 无效: {payload_kind}")
        media = request.get("media") if isinstance(request.get("media"), Mapping) else {}
        requested_id = media.get("tmdb_id")
        indexed_ids = {
            row.get("tmdb_id") for row in rows_for_share if type(row.get("tmdb_id")) is int
        }
        if type(requested_id) is int and indexed_ids and requested_id not in indexed_ids:
            continue
        files = list(bucket["files"].values())
        manifest = {"files": {
            index: {"path": item["path"], "size": item["size"]}
            for index, item in enumerate(files, 1)
        }}
        release_name = str(newest.get("release_name") or newest.get("title") or "").strip()
        gap_map, file_coverage = _gap_file_map(
            request, release_name, manifest,
            allowed_payload_extensions=allowed_payload_extensions,
        )
        if not gap_map:
            continue
        by_index = {index: files[index - 1] for index in manifest["files"]}
        indices = sorted({index for values in gap_map.values() for index in values})
        selected = [by_index[index] for index in indices]
        quality_text = " ".join([release_name, *(item["path"] for item in selected)]).casefold()
        resolution = (
            "2160p" if "2160" in quality_text or "4k" in quality_text
            else "1080p" if "1080" in quality_text
            else "720p" if "720" in quality_text else "unknown"
        )
        archive_password = ""
        if payload_kind == "archive_payload":
            archive_password = _indexed_archive_password(rows_for_share)
            if not archive_password:
                try:
                    archive_password = _discover_quark_share_archive_password(
                        code, str(newest.get("passcode") or ""),
                    )
                except ReplenishmentCandidateError:
                    # Conflicting password evidence invalidates this share, but
                    # must not suppress unrelated indexed shares.
                    continue
                except Exception:
                    # Read-only hint discovery is an enhancement.  An
                    # unencrypted SFX remains usable when AList/Quark is
                    # temporarily unavailable, so preserve the candidate.
                    archive_password = ""

        # A video share remains one server-side-copy candidate.  SFX shares
        # deliberately become one candidate per physical archive so a corrupt
        # or wrongly-passworded file cannot poison every other file in the
        # share's durable exclusion ledger.
        candidate_items = selected if payload_kind == "archive_payload" else [None]
        for archive_item in candidate_items:
            scoped = selected
            scoped_gap_map = gap_map
            locator = f"quark_share:{code}"
            scoped_release_name = release_name
            if archive_item is not None:
                physical_id = str(archive_item["file_id"])
                scoped = [archive_item]
                scoped_gap_map = {
                    gap: values for gap, values in gap_map.items()
                    if any(by_index[index]["file_id"] == physical_id for index in values)
                }
                locator = _quark_sfx_locator(code, physical_id, str(archive_item["path"]))
                scoped_release_name = (
                    f"{release_name} / {Path(str(archive_item['path'])).name}"
                )
            if locator in existing_locators:
                continue
            scoped_coverage = sorted(scoped_gap_map)
            expected_archives = [
                {
                    "file_id": item["file_id"],
                    "name": Path(item["path"]).name,
                    "path": item["path"],
                    "size": item["size"],
                    "gap_ids": scoped_coverage,
                }
                for item in scoped
            ] if payload_kind == "archive_payload" else []
            acquisition: dict[str, Any] = {
                "kind": (
                    "quark_sfx_archive"
                    if payload_kind == "archive_payload" else "quark_fast_save"
                ),
                "share_id": code,
                "share_url": f"https://pan.quark.cn/s/{code}",
                "file_id_by_gap": {
                    gap: [by_index[index]["file_id"] for index in indices_for_gap]
                    for gap, indices_for_gap in scoped_gap_map.items()
                },
                "file_path_by_id": {item["file_id"]: item["path"] for item in scoped},
                "file_size_by_id": {item["file_id"]: item["size"] for item in scoped},
                "save_strategy": "server_side_copy",
                "requires_share_revalidation": True,
                "payload_kind": payload_kind,
                "requires_extraction": requires_extraction,
                "archive_format": archive_format or None,
                "expected_archives": expected_archives,
            }
            if payload_kind == "archive_payload":
                acquisition["archive_password"] = archive_password
                acquisition["archive_member_by_gap"] = {
                    gap: by_index[indices_for_gap[0]].get("member_path")
                    for gap, indices_for_gap in scoped_gap_map.items()
                    if by_index[indices_for_gap[0]].get("member_path")
                }
                acquisition["archive_member_size_by_gap"] = {
                    gap: by_index[indices_for_gap[0]].get("member_size")
                    for gap, indices_for_gap in scoped_gap_map.items()
                    if by_index[indices_for_gap[0]].get("member_size")
                }
            candidates.append({
                "provider": "quark_share",
                "tmdb_id": requested_id if type(requested_id) is int else newest.get("tmdb_id"),
                "release_name": scoped_release_name,
                "resolution": resolution,
                "updated_at": newest.get("updated_at"),
                "availability": "metadata_verified",
                "locator": locator,
                "share_identity": {"provider": "quark", "share_id": code},
                "files": [item["path"] for item in scoped],
                "file_coverage": scoped_coverage,
                # Exact reviewed file evidence is also the release-level
                # coverage claim; it is not video-arrival proof for SFX.
                "name_coverage": scoped_coverage,
                "size": sum(item["size"] for item in scoped),
                "payload_kind": payload_kind,
                "requires_extraction": requires_extraction,
                "archive_format": archive_format or None,
                "video_files_verified": payload_kind == "video_payload",
                "acquisition": acquisition,
            })
    return candidates


def _pansou_url() -> str:
    raw = os.getenv("SCRAPEFLOW_REPLENISHMENT_PANSOU_URL", "").strip()
    if not raw:
        return ""
    parsed = urllib.parse.urlsplit(raw)
    loopback = parsed.hostname in {"127.0.0.1", "localhost", "::1", "pansou"}
    if (
        parsed.scheme not in ({"http", "https"} if loopback else {"https"})
        or not parsed.hostname or parsed.username or parsed.password
        or parsed.query or parsed.fragment or parsed.path.rstrip("/") != "/api/search"
    ):
        raise ValueError("PanSou URL 必须是 HTTPS /api/search 或本地容器地址")
    return raw


def _pansou_quark_links(
    request: Mapping[str, Any], *, deadline: float,
    excluded_locators: set[str] | None = None,
) -> tuple[list[dict[str, Any]], dict[str, int | bool]]:
    """Search PanSou for Quark links; returned rows are still untrusted."""
    url = _pansou_url()
    if not url:
        return [], {
            "query_attempts": 0, "query_responses": 0,
            "raw_discovered": 0, "available_discovered": 0,
            "search_complete": False,
        }
    links: dict[str, dict[str, Any]] = {}
    responses = 0
    # Broad titles find complete packs more reliably than S00/TMDB labels.
    # Exact episode matching happens only after the share is recursively listed.
    terms = _dynamic_search_terms(request)[:6]
    query_attempts = 0
    for term in terms:
        remaining = int(deadline - time.monotonic())
        if remaining <= 0:
            break
        query_attempts += 1
        body = json.dumps({
            "kw": term, "cloud_types": ["quark"], "res": "merge", "src": "all",
        }, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        http_request = urllib.request.Request(
            url, data=body, method="POST", headers={
                "Accept": "application/json", "Content-Type": "application/json",
                "User-Agent": "ScrapeFlow/1.0",
            },
        )
        try:
            with urllib.request.urlopen(http_request, timeout=max(1, min(20, remaining))) as reply:
                payload = json.load(reply)
        except (OSError, ValueError, json.JSONDecodeError):
            continue
        if not isinstance(payload, Mapping) or payload.get("code") not in {0, None}:
            continue
        responses += 1
        data = payload.get("data") if isinstance(payload.get("data"), Mapping) else {}
        merged = data.get("merged_by_type") if isinstance(data.get("merged_by_type"), Mapping) else {}
        rows = merged.get("quark") if isinstance(merged.get("quark"), list) else []
        for row in rows:
            if not isinstance(row, Mapping):
                continue
            try:
                code = _quark_share_code(row.get("url"))
            except ValueError:
                continue
            links.setdefault(code, {
                "share_id": code,
                "share_url": f"https://pan.quark.cn/s/{code}",
                "passcode": str(row.get("password") or ""),
                "release_name": str(row.get("note") or term).strip(),
                "updated_at": row.get("datetime"),
                "source": row.get("source"),
            })
    raw_discovered = len(links)
    excluded_codes = {
        locator.split(":", 1)[1]
        for locator in excluded_locators or set()
        if locator.startswith("quark_share:") and ":" in locator
    }
    available = [row for code, row in links.items() if code not in excluded_codes]
    try:
        limit = int(os.getenv("SCRAPEFLOW_REPLENISHMENT_PANSOU_MAX_SHARES", "30"))
    except ValueError:
        limit = 30
    bounded_limit = max(1, min(100, limit))
    return available[:bounded_limit], {
        "query_attempts": query_attempts,
        "query_responses": responses,
        "raw_discovered": raw_discovered,
        "available_discovered": len(available),
        # PanSou has no cursor, but the full merged unique set is returned for
        # each query.  Exhaustion is decided after exclusions and bounded
        # inspection below, not by repeatedly polling the same first page.
        "search_complete": (
            bool(terms) and query_attempts == len(terms)
            and responses == query_attempts
        ),
    }


def _search_dynamic_quark_share(
    request: Mapping[str, Any], existing_locators: set[str], *, deadline: float,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Discover, validate and exactly map live Quark shares without saving."""
    links, search_stats = _pansou_quark_links(
        request, deadline=deadline, excluded_locators=existing_locators,
    )
    telemetry: dict[str, Any] = {
        **search_stats,
        "discovered": len(links),
        "inspected": 0,
        "candidate_failures": 0,
        "infrastructure_failures": 0,
        "resource_failed_locators": [],
        "infrastructure_failure_types": {},
    }
    if not links:
        telemetry["source_exhausted"] = bool(telemetry["search_complete"])
        return [], telemetry
    if time.monotonic() >= deadline:
        return [], telemetry
    client = _alist_client()
    destination = os.getenv(
        "SCRAPEFLOW_REPLENISHMENT_UNSCRAPED_ROOT", "/quark/影视/ScrapeFlow/补源",
    ).rstrip("/")
    session = delegated_quark_session(client, destination)
    media = request.get("media") if isinstance(request.get("media"), Mapping) else {}
    tmdb_id = media.get("tmdb_id")

    def inspect(row: Mapping[str, Any]) -> tuple[str, dict[str, Any] | str]:
        if time.monotonic() >= deadline:
            return "infrastructure", "DeadlineExceeded"
        bridge = QuarkFastSaveBridge(UrlLibQuarkTransport(timeout=max(
            2.0, min(20.0, deadline - time.monotonic()),
        )))
        last_error: Exception | None = None
        for attempt in range(3):
            try:
                files = bridge.inspect_share(
                    session, pwd_id=str(row["share_id"]),
                    passcode=str(row.get("passcode") or ""),
                )
                break
            except QuarkShareExpiredError:
                return "candidate", dict(row)
            except Exception as exc:
                last_error = exc
                if attempt < 2 and time.monotonic() + 0.5 * (2 ** attempt) < deadline:
                    time.sleep(0.5 * (2 ** attempt))
                    continue
                return "infrastructure", type(exc).__name__
        else:  # pragma: no cover - the loop returns on its final failure
            return "infrastructure", type(last_error).__name__
        output = dict(row)
        output["tmdb_id"] = tmdb_id
        output["files"] = files
        return "inspected", output

    inspected_rows: list[dict[str, Any]] = []
    # Quark share listing is API-heavy (token + recursive pages).  Two lanes
    # retain useful concurrency without causing the burst-rate failures seen
    # with four simultaneous directory walks.
    workers = min(2, len(links))
    with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="quark-share-inspect") as pool:
        futures = [pool.submit(inspect, row) for row in links]
        for future in as_completed(futures):
            status, value = future.result()
            if status == "inspected" and isinstance(value, dict):
                inspected_rows.append(value)
                telemetry["inspected"] += 1
            elif status == "candidate" and isinstance(value, dict):
                telemetry["candidate_failures"] += 1
                telemetry["resource_failed_locators"].append(
                    f"quark_share:{value['share_id']}"
                )
            else:
                telemetry["infrastructure_failures"] += 1
                name = str(value)
                failures = telemetry["infrastructure_failure_types"]
                failures[name] = int(failures.get(name) or 0) + 1
    candidates = _search_quark_share(
        request, existing_locators, rows_override=inspected_rows,
    )
    matched_locators = {str(item.get("locator") or "") for item in candidates}
    telemetry["resource_failed_locators"].extend(
        f"quark_share:{row['share_id']}"
        for row in inspected_rows
        if f"quark_share:{row['share_id']}" not in matched_locators
    )
    telemetry["resource_failed_locators"] = sorted(set(
        telemetry["resource_failed_locators"]
    ))
    telemetry["source_exhausted"] = bool(
        telemetry["search_complete"]
        and telemetry["infrastructure_failures"] == 0
        and telemetry["inspected"] + telemetry["candidate_failures"]
        == telemetry["discovered"]
        and telemetry["available_discovered"] <= telemetry["discovered"]
    )
    return candidates, telemetry


def _search(request: Mapping[str, Any]) -> dict[str, Any]:
    media = request.get("media") if isinstance(request.get("media"), Mapping) else {}
    tmdb_id = media.get("tmdb_id")
    if type(tmdb_id) is not int or tmdb_id <= 0:
        return {"version": 1, "candidates": [], "message": "请求缺少 TMDB 身份"}
    warnings: list[str] = []
    lane_status: dict[str, dict[str, Any]] = {
        "quark_share": {"status": "ready"},
        "quark_magnet": {"status": "ready"},
    }
    rules = request.get("rules") if isinstance(request.get("rules"), Mapping) else {}
    provider_attempts = (
        request.get("provider_attempts")
        if isinstance(request.get("provider_attempts"), Mapping) else {}
    )
    local_torrent_unlocked = _local_torrent_unlocked(request)
    try:
        minimum_cloud_attempts = max(
            0, int(rules.get("minimum_attempts_per_cloud_lane") or 0),
        )
    except (TypeError, ValueError):
        minimum_cloud_attempts = 0
    try:
        quark_share_attempts = max(
            0, int(provider_attempts.get("quark_share") or 0),
        )
    except (TypeError, ValueError):
        quark_share_attempts = 0
    defer_quark_magnet = (
        minimum_cloud_attempts > 0
        and quark_share_attempts < minimum_cloud_attempts
        and _verified_provider_exhaustion(request, "quark_share") is None
    )
    if defer_quark_magnet:
        lane_status["quark_magnet"] = {
            "status": "deferred",
            "reason": "quark_share_attempt_floor_not_reached",
        }
    catalog_ready = False
    catalog_configured = bool(
        os.getenv("SCRAPEFLOW_REPLENISHMENT_CATALOG", "").strip()
    )
    catalog: dict[str, Any] = {}
    raw_candidates: list[dict[str, Any]] = []
    candidates: list[dict[str, Any]] = []
    try:
        catalog_path = _catalog_path()
        if catalog_path is None:
            raise OSError("未配置已核验候选目录")
        catalog = _load(catalog_path)
        projects = catalog.get("projects") if isinstance(catalog.get("projects"), Mapping) else {}
        project = projects.get(str(tmdb_id))
        raw_candidates = project.get("candidates") if isinstance(project, Mapping) else []
        if not isinstance(raw_candidates, list) or not all(
            isinstance(item, dict) for item in raw_candidates
        ):
            raise ValueError(f"TMDB {tmdb_id} 的候选目录格式无效")
        catalog_ready = True
        # A verified catalog row historically stored only the local Torrent
        # transport.  Expand it into the same cloud-first pair produced by
        # live discovery before applying lane-scoped exclusions.  Otherwise a
        # failed local download permanently hides a still-usable Quark offline
        # candidate with the identical BTIH.
        candidates = []
        if not defer_quark_magnet:
            for item in raw_candidates:
                candidates.extend(_catalog_torrent_candidate_variants(
                    item, include_local=local_torrent_unlocked,
                ))
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        # The dated, verified catalog is one source rather than a prerequisite.
        # Preserve dynamic discovery when that optional snapshot is absent or
        # damaged, and expose the degradation to the persisted search artifact.
        warnings.append(f"已核验候选目录不可用: {type(exc).__name__}")
    excluded = request.get("excluded_candidates")
    excluded_rows = excluded if isinstance(excluded, list) else []
    def is_cross_lane_metadata_mismatch(item: Mapping[str, Any]) -> bool:
        return item.get("reason") == "resource_inspected_without_requested_gap"

    def is_excluded(item: Mapping[str, Any]) -> bool:
        locator = str(item.get("locator") or "").strip()
        infohashes = _infohash_aliases(item.get("infohash"))
        provider = str(item.get("provider") or "").strip()
        for excluded_item in excluded_rows:
            if not isinstance(excluded_item, Mapping):
                continue
            excluded_provider = str(excluded_item.get("provider") or "").strip()
            # Legacy rows without a provider remain global/fail-closed.  New
            # rows isolate one acquisition lane so the same verified BTIH may
            # move from local Torrent to Quark cloud, or vice versa.
            if (
                excluded_provider and excluded_provider != provider
                and not is_cross_lane_metadata_mismatch(excluded_item)
            ):
                continue
            excluded_locator = str(excluded_item.get("locator") or "").strip()
            excluded_hashes = _infohash_aliases(excluded_item.get("infohash"))
            if (
                (locator and excluded_locator and locator == excluded_locator)
                or (infohashes and excluded_hashes and infohashes & excluded_hashes)
            ):
                return True
        return False

    output = [dict(item) for item in candidates if not is_excluded(item)]
    try:
        if _quark_index_path() is None:
            raise OSError("未配置夸克分享索引")
        existing_locators = {
            str(item.get("locator")) for item in output if item.get("locator")
        } | {
            str(item.get("locator")) for item in excluded_rows
            if isinstance(item, Mapping) and item.get("provider") in {None, "", "quark_share"}
            and item.get("locator")
        }
        output.extend(_search_quark_share(request, existing_locators))
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        warnings.append(f"夸克分享索引不可用: {type(exc).__name__}")
        lane_status["quark_share"] = {
            "status": "infrastructure_failure",
            "reason": "quark_share_index_unavailable",
        }
    share_discovery: dict[str, Any] = {
        "query_attempts": 0, "query_responses": 0, "raw_discovered": 0,
        "available_discovered": 0,
        "discovered": 0, "inspected": 0, "candidate_failures": 0,
        "infrastructure_failures": 0, "resource_failed_locators": [],
        "infrastructure_failure_types": {}, "search_complete": False,
        "source_exhausted": False,
    }
    try:
        pansou_url = _pansou_url()
        if pansou_url:
            share_deadline = time.monotonic() + _bounded_seconds(
                "SCRAPEFLOW_REPLENISHMENT_SHARE_SEARCH_TIMEOUT", 120, 20, 600,
            )
            existing_locators = {
                str(item.get("locator")) for item in output if item.get("locator")
            } | {
                str(item.get("locator")) for item in excluded_rows
                if isinstance(item, Mapping)
                and item.get("provider") in {None, "", "quark_share"}
                and item.get("locator")
            }
            discovered, share_discovery = _search_dynamic_quark_share(
                request, existing_locators, deadline=share_deadline,
            )
            output.extend(item for item in discovered if not is_excluded(item))
            if share_discovery["query_responses"] == 0:
                lane_status["quark_share"] = {
                    "status": "infrastructure_failure",
                    "reason": "quark_share_discovery_unavailable",
                }
                warnings.append("夸克分享动态发现不可用: PanSou 无成功响应")
            elif (
                share_discovery["discovered"] > 0
                and share_discovery["inspected"] == 0
                and share_discovery["candidate_failures"] == 0
                and not share_discovery["resource_failed_locators"]
                and share_discovery["infrastructure_failures"] > 0
            ):
                lane_status["quark_share"] = {
                    "status": "infrastructure_failure",
                    "reason": "quark_share_inspection_unavailable",
                }
                warnings.append("夸克分享目录检查全部被基础设施故障阻断")
            else:
                # Dynamic discovery is a complete independent source.  Its
                # successful response/inspection clears a stale static-index
                # failure instead of letting that optional snapshot poison the
                # whole first lane.
                lane_status["quark_share"] = {"status": "ready"}
                if share_discovery["infrastructure_failures"]:
                    warnings.append("夸克分享目录检查部分失败；本轮仅计已获得资源级证据的 locator")
    except Exception as exc:
        lane_status["quark_share"] = {
            "status": "infrastructure_failure",
            "reason": "quark_share_discovery_unavailable",
        }
        warnings.append(f"夸克分享动态发现失败: {type(exc).__name__}")
    if (
        share_discovery.get("source_exhausted") is True
        and not any(item.get("provider") == "quark_share" for item in output)
        and lane_status["quark_share"].get("status") != "infrastructure_failure"
    ):
        lane_status["quark_share"] = {
            "status": "exhausted",
            "reason": "configured_share_sources_exhausted",
            "proof": {
                "kind": "search_complete_no_candidates",
                "required_sources": ["PanSou"],
                "completed_sources": ["PanSou"],
                "candidate_count": 0,
                "excluded_candidate_count": int(
                    share_discovery.get("raw_discovered") or 0
                ),
            },
        }
        if defer_quark_magnet:
            # Source exhaustion is stronger evidence than manufacturing empty
            # scheduler rounds up to the numeric floor.  Advance to cloud
            # offline in this same search invocation; local Torrent remains
            # unavailable until the second lane independently exhausts.
            defer_quark_magnet = False
            lane_status["quark_magnet"] = {"status": "ready"}
            for item in raw_candidates:
                output.extend(
                    candidate for candidate in _catalog_torrent_candidate_variants(
                        item, include_local=local_torrent_unlocked,
                    )
                    if not is_excluded(candidate)
                )
    dynamic_enabled = os.getenv("SCRAPEFLOW_REPLENISHMENT_ACG_SEARCH", "1").strip().casefold() not in {
        "0", "false", "no", "off",
    }
    animetosho_enabled = os.getenv(
        "SCRAPEFLOW_REPLENISHMENT_ANIMETOSHO_SEARCH", "0",
    ).strip().casefold() in {"1", "true", "yes", "on"}
    tokyotosho_enabled = os.getenv(
        "SCRAPEFLOW_REPLENISHMENT_TOKYOTOSHO_SEARCH", "0",
    ).strip().casefold() in {"1", "true", "yes", "on"}
    subsplease_enabled = os.getenv(
        "SCRAPEFLOW_REPLENISHMENT_SUBSPLEASE_SEARCH", "0",
    ).strip().casefold() in {"1", "true", "yes", "on"}
    mikan_enabled = os.getenv(
        "SCRAPEFLOW_REPLENISHMENT_MIKAN_SEARCH", "0",
    ).strip().casefold() in {"1", "true", "yes", "on"}
    dmhy_enabled = os.getenv(
        "SCRAPEFLOW_REPLENISHMENT_DMHY_SEARCH", "0",
    ).strip().casefold() in {"1", "true", "yes", "on"}
    dynamic_ready = False
    dynamic_sources_ready = 0
    dynamic_source_count = 0
    required_dynamic_sources_ready = 0
    required_dynamic_source_count = 0
    required_dynamic_sources_exhausted = 0
    magnet_discovery: dict[str, Any] = {
        "sources": {}, "resource_failed_locators": [],
    }
    if dynamic_enabled and not defer_quark_magnet:
        deadline = time.monotonic() + _dynamic_search_timeout_seconds(request)
        searchers = [
            *((
                ("TokyoTosho", _search_tokyotosho, True),
            ) if tokyotosho_enabled else ()),
            *((
                ("AnimeTosho", _search_animetosho, True),
            ) if animetosho_enabled else ()),
            # SubsPlease exposes exact single-file magnets for current anime.
            # It is useful additional coverage, but it is not a complete
            # archive for old/special releases and therefore cannot be a
            # permanent exhaustion gate.
            *((
                ("SubsPlease", _search_subsplease, False),
            ) if subsplease_enabled else ()),
            # Mikan exposes a public title-search RSS with direct Torrent
            # enclosures.  It materially expands Chinese/Japanese release
            # coverage, but is not a complete archive for non-anime titles.
            *((
                ("Mikan", _search_mikan, False),
            ) if mikan_enabled else ()),
            # DMHY exposes public title-search RSS entries and a stable
            # per-release Torrent link.  Every hit is still accepted only
            # after its metainfo has been downloaded and matched to gaps.
            *((
                ("DMHY", _search_dmhy, False),
            ) if dmhy_enabled else ()),
            # Nyaa remains useful when reachable, but it is not a permanent
            # gate: some networks fail its TLS endpoint before receiving any
            # response.  Required sources always run before it.
            ("Nyaa", _search_nyaa, False),
            # ACG.RIP is retained as a best-effort candidate source, but its
            # long-running TLS outage must not erase complete evidence from
            # the two independent required indexes above.
            ("ACG", _search_acg, False),
        ]
        for source_index, (label, searcher, required) in enumerate(searchers):
            dynamic_source_count += 1
            if required:
                required_dynamic_source_count += 1
            try:
                now = time.monotonic()
                if now >= deadline:
                    warnings.append(f"{label} 动态搜索跳过: 总预算已用尽")
                    continue
                # A slow first provider must not consume the whole shared
                # deadline and starve every later source.  Unused slices remain
                # available to the providers that follow.
                remaining = searchers[source_index:]
                # Mikan and DMHY can expose title feeds whose exact
                # season/file proof needs several Torrent manifests. Give
                # them a full time slice without turning either into a
                # required exhaustion gate.
                current_weight = (
                    1.0 if required or label in {"Mikan", "DMHY"} else 0.25
                )
                remaining_weight = sum(
                    1.0
                    if future_required or future_label in {"Mikan", "DMHY"}
                    else 0.25
                    for future_label, _future_searcher, future_required in remaining
                )
                source_deadline = min(
                    deadline,
                    now + (deadline - now) * current_weight / remaining_weight,
                )
                existing_locators = {
                    str(item.get("locator")) for item in output if item.get("locator")
                } | {
                    str(item.get("locator")) for item in excluded_rows
                    if isinstance(item, Mapping)
                    and (
                        item.get("provider") in {None, ""}
                        or is_cross_lane_metadata_mismatch(item)
                        or (
                            not local_torrent_unlocked
                            and item.get("provider") == "quark_magnet"
                        )
                    )
                    and item.get("locator")
                }
                # RSS providers expose BTIH separately from the Torrent URL.
                # Before tier 3, carry cloud-lane historical hashes into the
                # pre-cap filter so the same first 32 resources cannot recur
                # forever.  Once local Torrent is unlocked, a provider-scoped
                # Quark rejection must not hide the same BTIH from the local
                # lane; the provider-aware ``is_excluded`` filter below will
                # retain only the still-eligible variant.
                for excluded_item in excluded_rows:
                    if not isinstance(excluded_item, Mapping):
                        continue
                    excluded_provider = excluded_item.get("provider")
                    if excluded_provider not in {None, ""} and not (
                        is_cross_lane_metadata_mismatch(excluded_item)
                        or (
                        not local_torrent_unlocked
                        and excluded_provider == "quark_magnet"
                        )
                    ):
                        continue
                    for alias in _infohash_aliases(excluded_item.get("infohash")):
                        existing_locators.add(f"quark_magnet:{alias}")
                dynamic = searcher(request, existing_locators, deadline=source_deadline)
                query_attempts = int(getattr(dynamic, "query_attempts", 0))
                query_responses = int(getattr(dynamic, "query_responses", 0))
                source_ready = bool(dynamic or query_responses > 0)
                source_exhausted = bool(getattr(dynamic, "source_exhausted", False))
                resource_failed_locators = [
                    str(value) for value in getattr(
                        dynamic, "resource_failed_locators", [],
                    ) if value
                ]
                magnet_discovery["resource_failed_locators"].extend(
                    resource_failed_locators
                )
                magnet_discovery["sources"][label] = {
                    "required": required, "query_attempts": query_attempts,
                    "query_responses": query_responses,
                    "candidate_count": len(dynamic),
                    "resource_failure_count": len(resource_failed_locators),
                    "source_exhausted": source_exhausted,
                    "preexcluded_count": int(getattr(
                        dynamic, "preexcluded_count", 0,
                    )),
                    "infrastructure_failures": int(getattr(
                        dynamic, "infrastructure_failures", 0,
                    )),
                    "infrastructure_failure_types": dict(getattr(
                        dynamic, "infrastructure_failure_types", {},
                    )),
                    "status": "ready" if source_ready else "infrastructure_failure",
                }
                failure_types = magnet_discovery["sources"][label][
                    "infrastructure_failure_types"
                ]
                if failure_types:
                    warnings.append(
                        f"{label} 动态搜索基础设施故障: "
                        + ", ".join(
                            f"{key}={value}"
                            for key, value in sorted(failure_types.items())
                        )
                    )
                if source_ready:
                    dynamic_ready = True
                    dynamic_sources_ready += 1
                    if required:
                        required_dynamic_sources_ready += 1
                        if source_exhausted:
                            required_dynamic_sources_exhausted += 1
                existing_provider_hashes = {
                    (str(item.get("provider") or ""), alias)
                    for item in output for alias in _infohash_aliases(item.get("infohash"))
                }
                for item in dynamic:
                    if item.get("provider") == "magnet" and not local_torrent_unlocked:
                        continue
                    aliases = _infohash_aliases(item.get("infohash"))
                    provider = str(item.get("provider") or "")
                    provider_keys = {(provider, alias) for alias in aliases}
                    if not is_excluded(item) and not (provider_keys & existing_provider_hashes):
                        output.append(item)
                        existing_provider_hashes.update(provider_keys)
            except Exception as exc:
                warnings.append(f"{label} 动态搜索失败: {type(exc).__name__}")
                magnet_discovery["sources"][label] = {
                    "required": required, "query_attempts": 0,
                    "query_responses": 0, "candidate_count": 0,
                    "status": "infrastructure_failure",
                    "error_type": type(exc).__name__,
                }
    qmag_candidates_present = any(
        item.get("provider") == "quark_magnet" for item in output
    )
    magnet_discovery["resource_failed_locators"] = sorted(set(
        magnet_discovery["resource_failed_locators"]
    ))
    if not defer_quark_magnet and not catalog_ready and not dynamic_ready:
        lane_status["quark_magnet"] = {
            "status": "infrastructure_failure",
            "reason": "torrent_candidate_sources_unavailable",
        }
    elif (
        not defer_quark_magnet and not qmag_candidates_present
        and required_dynamic_sources_ready < required_dynamic_source_count
    ):
        lane_status["quark_magnet"] = {
            "status": "infrastructure_failure",
            "reason": "required_cloud_offline_search_source_unavailable",
        }
    elif (
        not defer_quark_magnet
        and (catalog_ready or not catalog_configured)
        and dynamic_ready
        and required_dynamic_source_count > 0
        and required_dynamic_sources_exhausted == required_dynamic_source_count
        and not qmag_candidates_present
    ):
        lane_status["quark_magnet"] = {
            "status": "exhausted",
            "reason": "configured_cloud_offline_sources_exhausted",
            "proof": {
                "kind": "search_complete_no_candidates",
                "required_sources": sorted(
                    label for label, source in magnet_discovery["sources"].items()
                    if source.get("required") is True
                ),
                "completed_sources": sorted(
                    label for label, source in magnet_discovery["sources"].items()
                    if source.get("required") is True
                    and source.get("source_exhausted") is True
                ),
                "candidate_count": 0,
                "excluded_candidate_count": len(excluded_rows),
                "optional_sources": sorted(
                    label for label, source in magnet_discovery["sources"].items()
                    if source.get("required") is False
                ),
                "incomplete_optional_sources": sorted(
                    label for label, source in magnet_discovery["sources"].items()
                    if source.get("required") is False
                    and source.get("source_exhausted") is not True
                ),
            },
        }
    if not local_torrent_unlocked:
        # Legacy candidate artifacts may still contain both cloud and local
        # variants.  Preserve their cloud evidence but never leak tier 3 to
        # the coordinator before the durable gate is satisfied.
        output = [item for item in output if item.get("provider") != "magnet"]
    return {
        "version": 1,
        "catalog_verified_at": catalog.get("verified_at"),
        "candidates": output,
        "excluded_candidate_count": len(excluded_rows),
        "warnings": warnings,
        "lane_status": lane_status,
        "share_discovery": share_discovery,
        "magnet_discovery": magnet_discovery,
        "active_search_lane": (
            "quark_share" if defer_quark_magnet else "quark_magnet"
        ),
    }


class _AnchorParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.anchors: list[dict[str, str]] = []
        self._href: str | None = None
        self._text: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.casefold() != "a" or self._href is not None:
            return
        href = dict(attrs).get("href")
        if isinstance(href, str):
            self._href = href
            self._text = []

    def handle_data(self, data: str) -> None:
        if self._href is not None:
            self._text.append(data)

    def handle_endtag(self, tag: str) -> None:
        if tag.casefold() == "a" and self._href is not None:
            self.anchors.append({"href": self._href, "text": " ".join(self._text).strip()})
            self._href = None
            self._text = []


class _DynamicSearchResult(list[dict[str, Any]]):
    """Candidates plus enough telemetry to distinguish empty from unreachable."""

    def __init__(
        self, values: list[dict[str, Any]], *, query_attempts: int, query_responses: int,
        source_exhausted: bool = False,
        resource_failed_locators: list[str] | None = None,
        infrastructure_failures: int = 0,
        infrastructure_failure_types: Mapping[str, int] | None = None,
        preexcluded_count: int = 0,
    ) -> None:
        super().__init__(values)
        self.query_attempts = query_attempts
        self.query_responses = query_responses
        self.source_exhausted = bool(source_exhausted)
        self.resource_failed_locators = list(resource_failed_locators or [])
        self.infrastructure_failures = max(0, int(infrastructure_failures))
        self.infrastructure_failure_types = {
            str(key): max(0, int(value))
            for key, value in (infrastructure_failure_types or {}).items()
            if value
        }
        self.preexcluded_count = max(0, int(preexcluded_count))


def _network_failure_code(exc: BaseException) -> str:
    """Return a stable, non-sensitive code for source-health telemetry."""
    chain: list[BaseException] = []
    current: BaseException | None = exc
    while current is not None and current not in chain:
        chain.append(current)
        reason = current.reason if isinstance(current, urllib.error.URLError) else None
        current = reason if isinstance(reason, BaseException) else (
            current.__cause__ or current.__context__
        )
    for error in chain:
        if isinstance(error, urllib.error.HTTPError):
            return f"http_{error.code}"
        if isinstance(error, ConnectionRefusedError):
            return "connection_refused"
        if isinstance(error, ConnectionResetError):
            return "connection_reset"
        if isinstance(error, socket.gaierror):
            return "dns_failure"
        if isinstance(error, (TimeoutError, socket.timeout)):
            return "timeout"
        if isinstance(error, ssl.SSLError):
            return "tls_failure"
    return type(chain[-1] if chain else exc).__name__

def _fetch_bytes(
    url: str, *, max_bytes: int, timeout: int = 60, attempts: int = 4,
    opener: Any | None = None,
) -> bytes:
    request = urllib.request.Request(url, headers={"User-Agent": "ScrapeFlow/1.0"})
    last_error: Exception | None = None
    for attempt in range(attempts):
        try:
            open_request = opener.open if opener is not None else urllib.request.urlopen
            with open_request(request, timeout=timeout) as response:
                data = response.read(max_bytes + 1)
            if len(data) > max_bytes:
                raise ValueError("HTTP 响应超过大小上限")
            return data
        except (OSError, urllib.error.URLError, TimeoutError) as exc:
            last_error = exc
            if attempt < attempts - 1:
                time.sleep(min(2 ** attempt, 4))
    raise RuntimeError(f"HTTP 读取失败: {type(last_error).__name__}") from last_error


def _acg_http_opener() -> Any | None:
    """Use an explicit per-source proxy without changing Quark/AList traffic."""
    value = os.getenv("SCRAPEFLOW_REPLENISHMENT_ACG_PROXY", "").strip()
    if not value:
        return None
    parsed = urllib.parse.urlsplit(value)
    if (
        parsed.scheme not in {"http", "https"}
        or not parsed.hostname or parsed.port is None
        or parsed.username is not None or parsed.password is not None
        or parsed.path not in {"", "/"} or parsed.query or parsed.fragment
    ):
        raise ValueError("SCRAPEFLOW_REPLENISHMENT_ACG_PROXY must be a credential-free HTTP proxy URL")
    return urllib.request.build_opener(urllib.request.ProxyHandler({
        "http": value, "https": value,
    }))


def _gap_file_map(
    request: Mapping[str, Any], release_name: str, manifest: Mapping[str, Any],
    *, allowed_payload_extensions: frozenset[str] = VIDEO_EXTENSIONS,
) -> tuple[dict[str, list[int]], set[str]]:
    gaps = [gap for gap in request.get("gaps") or [] if isinstance(gap, Mapping)]
    optional_discovery = (
        isinstance(request.get("rules"), Mapping)
        and request["rules"].get("optional_discovery_only") is True
    )
    request_seasons = {
        int(gap["season"]) for gap in gaps
        if isinstance(gap.get("season"), int)
        and (int(gap["season"]) > 0 or (optional_discovery and int(gap["season"]) == 0))
    }
    # TMDB Season 00 numbers often do not equal the source release's OVA/OAD
    # ordinal.  For example, TMDB S00E07 may be titled ``Darkness OVA#1``.
    # Treat that explicit title ordinal as an alias only for the named gap;
    # never infer it from a generic query-group marker such as ``OVA``.
    optional_gap_aliases: dict[str, str] = {}
    optional_gap_semantics: dict[str, set[str]] = {}
    optional_gap_official_titles: dict[str, set[str]] = {}
    optional_source_aliases: dict[
        str, list[tuple[str, list[tuple[str, set[str]]]]]
    ] = {}
    if optional_discovery:
        for gap in gaps:
            gap_id = str(gap.get("id") or "")
            if not re.fullmatch(r"S00E\d{2,4}", gap_id):
                continue
            descriptors = [
                str(gap.get(key) or "") for key in ("season_name", "label")
                if isinstance(gap.get(key), str)
            ]
            semantic_keys = {
                key for descriptor in descriptors
                for key in _optional_semantic_keys(descriptor)
            }
            if semantic_keys:
                optional_gap_semantics[gap_id] = semantic_keys
            official_titles = {
                normalized
                for descriptor in (
                    *(
                        str(gap.get(key) or "") for key in ("label", "title")
                        if isinstance(gap.get(key), str)
                    ),
                    *(
                        str(value) for value in gap.get("title_aliases") or []
                        if isinstance(value, str)
                    ),
                )
                if (normalized := _normalized_text(
                    re.sub(re.escape(gap_id), " ", descriptor, count=1, flags=re.I)
                ))
                and len(normalized) >= 4
                and normalized not in {"特别篇", "特別篇", "special", "specials"}
            }
            if official_titles:
                optional_gap_official_titles[gap_id] = official_titles
            for alias in gap.get("source_episode_aliases") or []:
                if not isinstance(alias, Mapping):
                    continue
                source_season = alias.get("season")
                source_episode = alias.get("episode")
                series_titles = [
                    (
                        _normalized_text(value),
                        {
                            word for word in re.findall(
                                r"[a-z0-9]+", value.casefold(),
                            )
                            if len(word) >= 3
                            and word not in {"from", "starting", "season"}
                        },
                    )
                    for value in alias.get("series_titles") or []
                    if isinstance(value, str) and _normalized_text(value)
                ]
                if (
                    type(source_season) is int and source_season > 0
                    and type(source_episode) is int and source_episode > 0
                    and series_titles
                ):
                    optional_source_aliases.setdefault(gap_id, []).append((
                        f"S{source_season:02d}E{source_episode:02d}",
                        series_titles,
                    ))
            source_ordinals = {
                int(match.group(1))
                for descriptor in descriptors
                for match in OPTIONAL_EPISODE_RE.finditer(descriptor)
                if 0 < int(match.group(1)) <= 999
            }
            if len(source_ordinals) == 1:
                optional_gap_aliases[gap_id] = (
                    f"S00E{next(iter(source_ordinals)):02d}"
                )
    release_seasons = _season_markers(release_name)
    semantic_rules: list[tuple[str, int]] = []
    normalized_release = _normalized_text(release_name)
    for group in request.get("query_groups") or []:
        if not isinstance(group, Mapping) or not isinstance(group.get("season"), int):
            continue
        names = group.get("season_names") if isinstance(group.get("season_names"), list) else []
        semantic_rules.extend(
            (key, int(group["season"])) for name in names
            if (key := _normalized_text(name)) and len(key) >= 2
        )
    if optional_discovery:
        semantic_rules.extend(
            (key, 0)
            for value in _optional_series_title_search_terms(request, maximum=12)
            if (key := _normalized_text(value)) and len(key) >= 4
        )
    semantic_matches = [
        (len(key), season) for key, season in semantic_rules if key in normalized_release
    ]
    longest_semantic = max((length for length, _season in semantic_matches), default=0)
    semantic_seasons = {
        season for length, season in semantic_matches if length == longest_semantic
    }
    if len(release_seasons) == 1:
        default_seasons = release_seasons & request_seasons
    elif len(semantic_seasons) == 1:
        default_seasons = semantic_seasons & request_seasons
    elif len(request_seasons) == 1 and (
        next(iter(request_seasons)) == 1 or request_seasons <= semantic_seasons
    ):
        default_seasons = request_seasons
    else:
        default_seasons = set()
    episode_indices: dict[str, list[int]] = {}
    # Release-local OVA/OAD/SP ordinals are not TMDB Season 00 identities.
    # Keep them separate from canonical SxxEyy tokens so ``OVA1`` cannot
    # silently satisfy TMDB ``S00E01``.  They become eligible only through an
    # explicit ordinal alias present in that exact official gap title.
    optional_ordinal_indices: dict[str, list[int]] = {}
    optional_semantic_indices: dict[str, list[int]] = {}
    optional_official_title_indices: dict[str, list[int]] = {}
    optional_source_alias_indices: dict[str, list[int]] = {}
    manifest_has_optional_container = bool(
        optional_discovery and any(
            isinstance(row, Mapping)
            and OPTIONAL_CONTAINER_RE.search(
                str(row.get("path") or "").replace("\\", "/")
            )
            for row in (manifest.get("files") or {}).values()
        )
    )
    for index, row in (manifest.get("files") or {}).items():
        if type(index) is not int or not isinstance(row, Mapping):
            continue
        path = str(row.get("path") or "")
        normalized_path = path.replace("\\", "/").casefold()
        basename = Path(path).stem
        if (
            Path(path).suffix.casefold() not in allowed_payload_extensions
            or (
                not optional_discovery
                and any(part in normalized_path for part in ("/menu/", "/sps/", "/specials/", "/extras/"))
            )
            or re.search(
                r"(?i)(?:^|[\[\s._-])(NCOP|NCED|MENU|PV|CM|TRAILER)(?:[\]\s._-]|$)",
                basename,
            )
        ):
            continue
        # Release CRC groups such as ``[E6FB5CBC]`` otherwise look like an
        # ``E6`` episode token.  Remove only exact eight-hex groups before
        # extracting episode syntax; retain the original path for verification.
        clean_path = re.sub(r"\[[0-9A-Fa-f]{8}\](?=\.[^.]+$|$)", "", path)
        clean_basename = Path(clean_path).stem
        normalized_file = _normalized_text(clean_path)
        normalized_source_identity = _normalized_text(f"{release_name} {clean_path}")
        source_tokens = _expanded_episode_ids(clean_path) | _coverage_tokens(
            [clean_path], default_seasons=_season_markers(release_name),
        )
        source_words = set(re.findall(r"[a-z0-9]+", f"{release_name} {clean_path}".casefold()))
        for gap_id, aliases in optional_source_aliases.items():
            for source_token, series_titles in aliases:
                if source_token not in source_tokens:
                    continue
                identity_matches = False
                for series_title, title_words in series_titles:
                    if series_title in normalized_source_identity:
                        identity_matches = True
                        break
                    if len(title_words) >= 2 and title_words <= source_words:
                        identity_matches = True
                        break
                if identity_matches:
                    optional_source_alias_indices.setdefault(gap_id, []).append(index)
                    break
        file_in_optional_container = bool(
            optional_discovery
            and OPTIONAL_CONTAINER_RE.search(clean_path.replace("\\", "/"))
        )
        file_optional_semantics = (
            _optional_semantic_keys(clean_path) if optional_discovery else set()
        )
        if (
            optional_discovery and not file_optional_semantics
            and OPTIONAL_PAST_ARC_NAME_RE.search(clean_path)
        ):
            # A file named only OAD02 below a 过去篇 directory may use either
            # the global OAD number or the arc-local number.  Suppress numeric
            # fallback until the path itself states 过去篇02 (or equivalent).
            file_optional_semantics = {"past:ambiguous"}
        file_optional_tokens = {
            f"S00E{int(match.group(1)):02d}"
            for match in OPTIONAL_EPISODE_RE.finditer(clean_basename)
            if 0 < int(match.group(1)) <= 999
        } if optional_discovery and not file_optional_semantics else set()
        has_requested_optional_alias = bool(
            file_optional_tokens & set(optional_gap_aliases.values())
        )
        file_semantic_matches = [
            (len(key), season) for key, season in semantic_rules if key in normalized_file
        ]
        longest_file_semantic = max(
            (length for length, _season in file_semantic_matches), default=0,
        )
        file_semantic_seasons = {
            season for length, season in file_semantic_matches
            if length == longest_file_semantic
        }
        # Keep intrinsic path evidence separate from the requested seasons.
        # Intersecting first used to erase an explicit ``Season 2`` marker for
        # an S01-only request and then silently fall back to that request's
        # unique season.  A multi-season pack consequently mapped both
        # ``Clannad - 20`` and ``Clannad After Story - 20`` to S01E20.
        intrinsic_path_seasons = {
            season for season in _season_markers(clean_path)
            if season > 0 or (optional_discovery and season == 0)
        }
        if has_requested_optional_alias:
            # An OAD may physically live under the source release's ``S3``
            # directory while TMDB owns it in Season 00.  The explicit OVA
            # ordinal from the official gap title is stronger evidence than
            # that packaging parent, and is scoped to this exact request.
            file_default_seasons = {0}
        elif len(intrinsic_path_seasons) == 1:
            explicit_season = intrinsic_path_seasons
            if not explicit_season <= request_seasons:
                continue
            # A longest semantic match is also explicit evidence.  Conflicting
            # path/name identities are unsafe rather than a reason to prefer
            # the single requested season.
            if (
                len(file_semantic_seasons) == 1
                and file_semantic_seasons != explicit_season
            ):
                continue
            file_default_seasons = explicit_season
        elif intrinsic_path_seasons:
            # Multiple season markers in one file path do not identify which
            # season owns a naked episode ordinal.  A unique semantic title may
            # disambiguate it only when it agrees with both the path and the
            # requested season set; otherwise fail closed.
            if (
                len(file_semantic_seasons) != 1
                or not file_semantic_seasons <= intrinsic_path_seasons
                or not file_semantic_seasons <= request_seasons
            ):
                continue
            file_default_seasons = file_semantic_seasons
        elif len(file_semantic_seasons) == 1:
            if not file_semantic_seasons <= request_seasons:
                continue
            file_default_seasons = file_semantic_seasons
        elif file_semantic_seasons:
            # Semantic season evidence exists but is ambiguous.  Do not erase
            # it by degrading to the release/request default.
            continue
        else:
            file_default_seasons = (
                set()
                if (
                    optional_discovery
                    and manifest_has_optional_container
                    and 0 in default_seasons
                    and not file_in_optional_container
                )
                else default_seasons
            )
        tokens = _expanded_episode_ids(clean_path) | _coverage_tokens(
            [clean_path], default_seasons=file_default_seasons,
        )
        if len(file_default_seasons) == 1:
            plain = ANIME_PLAIN_EPISODE_RE.search(clean_basename)
            if plain:
                episode = int(plain.group(1))
                if 0 < episode <= 999:
                    tokens.add(f"S{next(iter(file_default_seasons)):02d}E{episode:02d}")
        if (
            optional_discovery
            and OPTIONAL_RETROSPECTIVE_COLLECTION_RE.search(clean_path)
            and not OPTIONAL_EXPLICIT_S00_RE.search(clean_path)
        ):
            # A retrospective collection commonly numbers its own entries
            # (for example ``精选集03 ... 回想篇第03话``).  Those local
            # ordinals are neither TMDB Season 00 identities nor source OVA
            # ordinals, and must not satisfy a coincidentally numbered S00
            # gap.  Explicit S00 syntax remains authoritative; explicit
            # OVA/OAD/SP ordinals and named official arc semantics are kept in
            # their dedicated maps below.
            tokens = {token for token in tokens if not token.startswith("S00E")}
        for token in tokens:
            episode_indices.setdefault(token, []).append(index)
        for token in file_optional_tokens:
            optional_ordinal_indices.setdefault(token, []).append(index)
        for semantic_key in file_optional_semantics:
            optional_semantic_indices.setdefault(semantic_key, []).append(index)
        if optional_discovery:
            for gap_id, official_titles in optional_gap_official_titles.items():
                if any(title in normalized_file for title in official_titles):
                    optional_official_title_indices.setdefault(gap_id, []).append(index)
    mapping: dict[str, list[int]] = {}
    file_coverage: set[str] = set()
    for gap in gaps:
        gap_id = str(gap.get("id") or "")
        if re.fullmatch(r"S\d{2,3}E\d{2,4}", gap_id):
            official_title_indices = set(
                optional_official_title_indices.get(gap_id) or []
            )
            if official_title_indices:
                mapping[gap_id] = sorted(official_title_indices)
                file_coverage.add(gap_id)
                continue
            source_alias_indices = set(
                optional_source_alias_indices.get(gap_id) or []
            )
            if source_alias_indices:
                mapping[gap_id] = sorted(source_alias_indices)
                file_coverage.add(gap_id)
                continue
            gap_semantics = optional_gap_semantics.get(gap_id) or set()
            semantic_indices = {
                index for semantic_key in gap_semantics
                for index in optional_semantic_indices.get(semantic_key) or []
            }
            if semantic_indices:
                mapping[gap_id] = sorted(semantic_indices)
                file_coverage.add(gap_id)
                continue
            if gap_semantics:
                # A named arc in TMDB (for example 过去篇01) must not fall
                # back to a coincidental global OAD number from another arc.
                continue
            direct_indices = set(episode_indices.get(gap_id) or [])
            alias_id = optional_gap_aliases.get(gap_id)
            alias_indices = (
                set(optional_ordinal_indices.get(alias_id) or [])
                if alias_id else set()
            )
            # If both the TMDB number and explicit source OVA ordinal exist but
            # identify different objects, the share is ambiguous and this gap
            # must remain unresolved.
            if direct_indices and alias_indices and direct_indices != alias_indices:
                continue
            matched_indices = direct_indices or alias_indices
            if matched_indices:
                mapping[gap_id] = sorted(matched_indices)
                file_coverage.add(gap_id)
                continue
        if gap.get("kind") != "missing_season":
            continue
        season = gap.get("season")
        expected = gap.get("expected_episode_count")
        if type(season) is not int or type(expected) is not int or expected <= 0:
            continue
        episode_ids = [f"S{season:02d}E{episode:02d}" for episode in range(1, expected + 1)]
        if all(episode_indices.get(item) for item in episode_ids):
            mapping[gap_id] = sorted({
                index for item in episode_ids for index in episode_indices[item]
            })
            file_coverage.update(episode_ids)
    return mapping, file_coverage


def _optional_episode_title_search_terms(
    request: Mapping[str, Any],
) -> list[str]:
    """Keep a bounded set of exact S00 names in provider query budgets.

    Dynamic providers accept only a few queries per pass.  Prefer one exact
    TMDB title for each release season marker (for example ``3rd season`` and
    ``4th season``) before filling with other episode titles.  The later
    recursive file-manifest gate remains authoritative for gap coverage.
    """
    rules = request.get("rules")
    if not (
        isinstance(rules, Mapping)
        and rules.get("optional_discovery_only") is True
    ):
        return []
    media = request.get("media") if isinstance(request.get("media"), Mapping) else {}
    aliases = [
        str(value).strip() for value in media.get("aliases") or []
        if isinstance(value, str) and value.strip()
    ]
    title = str(media.get("title") or "").strip()
    base = next(iter(dict.fromkeys([*aliases, title])), "")
    if not base:
        return []
    exact_titles: list[str] = []
    for group in request.get("query_groups") or []:
        if not isinstance(group, Mapping) or group.get("season") != 0:
            continue
        exact_titles.extend(
            str(value).strip() for value in group.get("episode_titles") or []
            if isinstance(value, str) and value.strip()
        )
    marked: list[str] = []
    remaining: list[str] = []
    seen_markers: set[str] = set()
    for value in dict.fromkeys(exact_titles):
        marker = re.search(
            r"(?i)\b(\d{1,2})(?:st|nd|rd|th)?\s+season\b", value,
        )
        marker_key = marker.group(1) if marker else ""
        if marker_key and marker_key not in seen_markers:
            seen_markers.add(marker_key)
            marked.append(value)
        else:
            remaining.append(value)
    return [f"{base} {value}" for value in [*marked, *remaining][:4]]


def _dynamic_search_terms(request: Mapping[str, Any]) -> list[str]:
    media = request.get("media") if isinstance(request.get("media"), Mapping) else {}
    aliases = [
        str(value).strip() for value in media.get("aliases") or []
        if isinstance(value, str) and value.strip()
    ]
    title = str(media.get("title") or "").strip()
    bases = list(dict.fromkeys([*aliases, title]))
    focused: list[str] = []
    for group in request.get("query_groups") or []:
        if not isinstance(group, Mapping) or not isinstance(group.get("season"), int):
            continue
        season = int(group["season"])
        names = [
            str(value).strip() for value in group.get("season_names") or []
            if isinstance(value, str) and value.strip()
        ]
        for base in bases[:4]:
            focused.append(f"{base} {names[0]}" if names else f"{base} S{season:02d}")
            focused.append(f"{base} S{season:02d}")
    generated = [
        str(value).strip() for value in request.get("search_queries") or []
        if isinstance(value, str)
        and re.search(r"(?i)(?:S\d|\d+x\d|Season\s+\d|第.+季|完结篇|Darkness)", value)
    ]
    episode_focused = _optional_episode_title_search_terms(request)
    terms: list[str] = []
    seen: set[str] = set()
    # Exact S00 episode titles must survive the provider query ceiling; broad
    # season syntax follows them and still finds complete packs.  Focused
    # terms must precede bare aliases.  With many exact TMDB
    # alternative titles, letting bases consume the 12-term budget prevented
    # later official English names from receiving an S00 query at all.
    for value in [*episode_focused, *focused, *generated, *bases]:
        key = re.sub(r"\W+", "", value).casefold()
        if len(key) < 2 or key in seen:
            continue
        seen.add(key)
        terms.append(value)
        if len(terms) >= 12:
            break
    return terms


def _compact_dynamic_search_terms(
    request: Mapping[str, Any], *, maximum: int = 4,
) -> list[str]:
    """Prefer season/episode-specific queries over redundant bare aliases."""
    if maximum < 1:
        return []
    terms = _dynamic_search_terms(request)
    episode_focused = _optional_episode_title_search_terms(request)
    marked_episode_focused = [
        value for value in episode_focused
        if re.search(
            r"(?i)\b\d{1,2}(?:st|nd|rd|th)?\s+season\b", value,
        )
    ]
    exact_season = [
        value for value in terms if re.search(r"(?i)\bS\d{2}(?:E\d+)?\b", value)
    ]
    broad_season = [
        value for value in terms
        if re.search(r"(?i)(?:\bS\d{1,2}\b|\bSeason\s+\d+\b|\b\d+x\d|\u7b2c.+\u5b63)", value)
    ]
    output: list[str] = []
    seen: set[str] = set()
    for value in [
        *marked_episode_focused,
        *exact_season,
        *broad_season,
        *episode_focused,
        *terms,
    ]:
        key = re.sub(r"\W+", "", value).casefold()
        if key in seen:
            continue
        seen.add(key)
        output.append(value)
        if len(output) >= maximum:
            break
    return output


def _torrent_candidate(
    request: Mapping[str, Any], release_name: str, torrent_url: str,
    manifest: Mapping[str, Any],
) -> dict[str, Any] | None:
    gap_map, file_coverage = _gap_file_map(request, release_name, manifest)
    if not gap_map:
        return None
    indices = sorted({index for values in gap_map.values() for index in values})
    files = manifest["files"]
    candidate_paths = [str(files[index]["path"]) for index in indices]
    quality_text = " ".join([release_name, *candidate_paths[:20]]).casefold()
    resolution = (
        "2160p" if "2160" in quality_text or "4k" in quality_text
        else "1080p" if "1080" in quality_text
        else "720p" if "720" in quality_text else "unknown"
    )
    return {
        "provider": "magnet",
        "release_name": release_name,
        "resolution": resolution,
        "availability": "metadata_verified",
        "locator": f"torrent:{torrent_url}",
        "files": candidate_paths,
        "file_coverage": sorted(file_coverage),
        "infohash": manifest["infohash"],
        "acquisition": {
            "kind": "torrent", "url": torrent_url,
            "file_index_by_gap": gap_map,
            "file_size_by_index": {str(index): int(files[index]["size"]) for index in indices},
            "file_path_by_index": {str(index): str(files[index]["path"]) for index in indices},
        },
    }


def _torrent_candidate_variants(
    request: Mapping[str, Any], release_name: str, torrent_url: str,
    manifest: Mapping[str, Any], *, include_local: bool = True,
) -> list[dict[str, Any]]:
    """Derive cloud-offline evidence and only emit local when tier 3 allows it."""
    local = _torrent_candidate(request, release_name, torrent_url, manifest)
    if local is None:
        return []
    enabled = os.getenv("SCRAPEFLOW_REPLENISHMENT_QUARK_OFFLINE", "1").strip().lower()
    if enabled not in {"1", "true", "yes", "on"}:
        return [local] if include_local else []
    offline = _quark_offline_variant(local)
    if offline is None:
        return [local] if include_local else []
    return [offline, local] if include_local else [offline]


def _quark_offline_variant(
    local_candidate: Mapping[str, Any],
) -> dict[str, Any] | None:
    """Derive an exact Quark-offline lane from verified Torrent metadata."""
    if str(local_candidate.get("provider") or "") != "magnet":
        return None
    acquisition = local_candidate.get("acquisition")
    if not isinstance(acquisition, Mapping) or acquisition.get("kind") != "torrent":
        return None
    gap_map = acquisition.get("file_index_by_gap")
    path_map = acquisition.get("file_path_by_index")
    size_map = acquisition.get("file_size_by_index")
    if not all(isinstance(value, Mapping) for value in (gap_map, path_map, size_map)):
        return None
    infohash = str(local_candidate.get("infohash") or "").strip().lower()
    if not re.fullmatch(r"[0-9a-f]{40}", infohash):
        return None
    torrent_url = str(acquisition.get("url") or "").strip()
    release_name = str(local_candidate.get("release_name") or "").strip()
    if not torrent_url or not release_name:
        return None
    by_index: dict[int, list[str]] = {}
    for gap, indexes in gap_map.items():
        if not isinstance(indexes, list):
            return None
        for index in indexes:
            if isinstance(index, bool) or not isinstance(index, int):
                return None
            by_index.setdefault(index, []).append(str(gap))
    expected_files: list[dict[str, Any]] = []
    for index in sorted(by_index):
        path = path_map.get(str(index))
        size = size_map.get(str(index))
        if not isinstance(path, str) or not path.strip() or (
            isinstance(size, bool) or not isinstance(size, int) or size <= 0
        ):
            return None
        expected_files.append({
            "torrent_index": index,
            "path": path.replace("\\", "/"),
            "size": size,
            "gap_ids": sorted(set(by_index[index])),
        })
    if not expected_files:
        return None
    magnet_url = (
        f"magnet:?xt=urn:btih:{infohash}&dn="
        + urllib.parse.quote(release_name, safe="")
    )
    offline = dict(local_candidate)
    offline.update({
        "provider": "quark_magnet",
        "locator": f"quark_magnet:{infohash}",
        "acquisition": {
            "kind": "quark_magnet_offline", "magnet_url": magnet_url,
            "torrent_url": torrent_url,
            "local_fallback": dict(acquisition),
            "expected_files": expected_files,
        },
        # The executor/server may suppress only this lane after quota/auth or
        # provider-delivery failure while retaining the local candidate.
        "fallback_locator": local_candidate.get("locator"),
    })
    return offline


def _catalog_torrent_candidate_variants(
    candidate: Mapping[str, Any], *, include_local: bool = True,
) -> list[dict[str, Any]]:
    """Return cloud-first variants for one already-verified catalog row."""
    local = dict(candidate)
    # Non-torrent legacy fixtures/catalog rows are outside the tier-3 gate.
    # Keep them intact; the core selector still validates their provider.
    if local.get("provider") != "magnet" and not str(
        local.get("locator") or ""
    ).startswith("torrent:"):
        return [local]
    enabled = os.getenv("SCRAPEFLOW_REPLENISHMENT_QUARK_OFFLINE", "1").strip().lower()
    if enabled not in {"1", "true", "yes", "on"}:
        return [local] if include_local else []
    offline = _quark_offline_variant(local)
    if offline is None:
        return [local] if include_local else []
    return [offline, local] if include_local else [offline]


def _search_nyaa(
    request: Mapping[str, Any], existing_locators: set[str], *, deadline: float,
) -> list[dict[str, Any]]:
    def request_timeout() -> int:
        remaining = int(deadline - time.monotonic())
        return max(1, min(12, remaining))

    results: dict[str, tuple[str, str]] = {}
    query_attempts = 0
    query_responses = 0
    terms = _compact_dynamic_search_terms(request, maximum=3)
    hit_cap = False
    excluded_hashes = _locator_infohash_aliases(existing_locators)
    preexcluded_hashes: set[str] = set()

    def rss_infohash(item: ET.Element) -> str:
        for child in item:
            if child.tag.rsplit("}", 1)[-1].casefold() == "infohash":
                value = str(child.text or "").strip().casefold()
                if _infohash_aliases(value):
                    return value
        return ""

    for term in terms:
        if time.monotonic() >= deadline:
            break
        url = "https://nyaa.si/?page=rss&c=1_2&f=0&q=" + urllib.parse.quote(term)
        query_attempts += 1
        try:
            page = _fetch_bytes(
                url, max_bytes=4 * 1024 * 1024,
                timeout=request_timeout(), attempts=1,
            )
            root = ET.fromstring(page)
        except (OSError, RuntimeError, ValueError, ET.ParseError):
            continue
        query_responses += 1
        for item in root.findall("./channel/item"):
            release_name = str(item.findtext("title") or "").strip()
            torrent_url = str(item.findtext("link") or "").strip()
            feed_infohash = rss_infohash(item)
            locator = f"torrent:{torrent_url}"
            aliases = _infohash_aliases(feed_infohash)
            if aliases and aliases & excluded_hashes:
                preexcluded_hashes.add(feed_infohash)
                continue
            if (
                release_name
                and torrent_url.startswith("https://nyaa.si/download/")
                and locator not in existing_locators
            ):
                results.setdefault(torrent_url, (release_name, feed_infohash))
            if len(results) >= 32:
                hit_cap = True
                break
        if len(results) >= 32:
            break

    candidates: list[dict[str, Any]] = []
    resource_failed_locators: list[str] = []
    infrastructure_failures = 0
    processed = 0
    ranked_results = sorted(
        results.items(),
        key=lambda item: _source_episode_release_priority(
            request, item[1][0],
        ),
    )
    for torrent_url, (release_name, feed_infohash) in ranked_results:
        if time.monotonic() >= deadline:
            break
        with tempfile.TemporaryDirectory(prefix="scrapeflow-nyaa-") as directory:
            try:
                manifest = _download_torrent(
                    torrent_url, Path(directory) / "candidate.torrent",
                    timeout=request_timeout(), attempts=1,
                )
            except Exception:
                infrastructure_failures += 1
                continue
        processed += 1
        manifest_aliases = _infohash_aliases(manifest["infohash"])
        if (
            manifest_aliases & excluded_hashes
            or _infohash_aliases(feed_infohash) & excluded_hashes
        ):
            continue
        variants = _torrent_candidate_variants(
            request, release_name, torrent_url, manifest,
            include_local=_local_torrent_unlocked(request),
        )
        if variants:
            candidates.extend(variants)
        else:
            resource_failed_locators.append(f"quark_magnet:{manifest['infohash']}")
    return _DynamicSearchResult(
        candidates,
        query_attempts=query_attempts,
        query_responses=query_responses,
        source_exhausted=bool(
            terms and query_attempts == len(terms) and query_responses == query_attempts
            and not hit_cap and processed == len(results) and infrastructure_failures == 0
        ),
        resource_failed_locators=resource_failed_locators,
        infrastructure_failures=infrastructure_failures,
        preexcluded_count=len(preexcluded_hashes),
    )


def _mikan_search_terms(request: Mapping[str, Any]) -> list[str]:
    """Prefer broad title terms for Season 00, then verify exact metadata."""
    terms = _compact_dynamic_search_terms(request, maximum=3)
    gaps = [gap for gap in request.get("gaps") or [] if isinstance(gap, Mapping)]
    if not any(gap.get("season") == 0 for gap in gaps):
        return terms
    broad = [
        re.sub(r"\s+S00(?:E\d+(?:-E?\d+)?)?$", "", term, flags=re.I).strip()
        for term in terms
    ]
    return list(dict.fromkeys([*filter(None, broad), *terms]))[:3]


def _optional_series_title_search_terms(
    request: Mapping[str, Any], *, maximum: int = 4,
) -> list[str]:
    """Extract bounded Season 00 sub-series names from exact episode titles."""
    if maximum < 1:
        return []
    titles = [
        str(value).strip()
        for group in request.get("query_groups") or []
        if isinstance(group, Mapping) and group.get("season") == 0
        for value in group.get("episode_titles") or []
        if isinstance(value, str) and value.strip()
    ]
    output: list[str] = []
    seen: set[str] = set()
    for title in titles:
        series = re.sub(
            r"(?i)\s+\d{1,2}(?:st|nd|rd|th)(?:\s+season)?\b.*$",
            "", title,
        ).strip(" -:：")
        if series == title and " - " in title and title.count(":") >= 2:
            series = title.rsplit(":", 1)[0].strip(" -:：")
        variants = [series]
        leading_tag = re.match(r"^[A-Za-z]{1,12}[:：]\s*(.+)$", series)
        if leading_tag:
            # DMHY treats the colon-bearing form as a very broad query for
            # some titles (hundreds of unrelated rows).  The suffix remains
            # an exact substring of release names and is the safer identity.
            variants = [leading_tag.group(1).strip()]
        for value in variants:
            key = re.sub(r"\W+", "", value).casefold()
            if len(key) < 3 or key in seen:
                continue
            seen.add(key)
            output.append(value)
            if len(output) >= maximum:
                return output
    return output


def _dmhy_search_terms(request: Mapping[str, Any]) -> list[str]:
    """Prefer Season 00 sub-series names that DMHY release titles retain."""
    focused = _optional_series_title_search_terms(request, maximum=4)
    fallback = _mikan_search_terms(request)
    return list(dict.fromkeys([*focused, *fallback]))[:4]


def _source_episode_search_terms(
    request: Mapping[str, Any], *, maximum: int = 2,
) -> list[str]:
    """Build precise release-season terms from verified local aliases."""
    output: list[str] = []
    for gap in request.get("gaps") or []:
        if not isinstance(gap, Mapping):
            continue
        for alias in gap.get("source_episode_aliases") or []:
            if not isinstance(alias, Mapping) or type(alias.get("season")) is not int:
                continue
            season = int(alias["season"])
            if season <= 0:
                continue
            for title in alias.get("series_titles") or []:
                if not isinstance(title, str):
                    continue
                words = [
                    word for word in re.findall(r"[A-Za-z0-9]+", title)
                    if word.casefold() not in {"from", "starting", "season"}
                ]
                compact = " ".join(dict.fromkeys(words)).strip()
                if len(words) < 3 or not compact:
                    continue
                term = f"{compact} S{season}"
                if term not in output:
                    output.append(term)
                    if len(output) >= maximum:
                        return output
    return output


def _source_episode_release_priority(
    request: Mapping[str, Any], release_name: str,
) -> int:
    """Prioritize metadata rows that can contain a verified local alias."""
    release_tokens = _expanded_episode_ids(release_name) | _coverage_tokens(
        [release_name], default_seasons=_season_markers(release_name),
    )
    release_words = set(re.findall(r"[a-z0-9]+", release_name.casefold()))
    normalized_release = _normalized_text(release_name)
    for gap in request.get("gaps") or []:
        if not isinstance(gap, Mapping):
            continue
        for alias in gap.get("source_episode_aliases") or []:
            if not isinstance(alias, Mapping):
                continue
            source_season = alias.get("season")
            source_episode = alias.get("episode")
            if type(source_season) is not int or type(source_episode) is not int:
                continue
            token = f"S{source_season:02d}E{source_episode:02d}"
            if token not in release_tokens:
                continue
            for title in alias.get("series_titles") or []:
                if not isinstance(title, str):
                    continue
                normalized_title = _normalized_text(title)
                title_words = {
                    word for word in re.findall(r"[a-z0-9]+", title.casefold())
                    if len(word) >= 3
                    and word not in {"from", "starting", "season"}
                }
                if (
                    normalized_title and normalized_title in normalized_release
                    or len(title_words) >= 2 and title_words <= release_words
                ):
                    return 0
    return 1


def _search_mikan(
    request: Mapping[str, Any], existing_locators: set[str], *, deadline: float,
) -> list[dict[str, Any]]:
    """Search Mikan's public RSS and validate every selected Torrent manifest."""
    def request_timeout() -> int:
        remaining = int(deadline - time.monotonic())
        return max(1, min(12, remaining))

    results: dict[str, str] = {}
    query_attempts = 0
    query_responses = 0
    terms = _mikan_search_terms(request)
    hit_cap = False
    preexcluded = 0
    excluded_hashes = _locator_infohash_aliases(existing_locators)
    requested_seasons = {
        int(gap["season"]) for gap in request.get("gaps") or []
        if isinstance(gap, Mapping) and isinstance(gap.get("season"), int)
    }
    resource_failed_locators: set[str] = set()
    for term in terms:
        if time.monotonic() >= deadline:
            break
        url = (
            "https://mikanani.me/RSS/Search?searchstr="
            + urllib.parse.quote(term)
        )
        query_attempts += 1
        try:
            page = _fetch_bytes(
                url, max_bytes=4 * 1024 * 1024,
                timeout=request_timeout(), attempts=1,
            )
            root = ET.fromstring(page)
        except (OSError, RuntimeError, ValueError, ET.ParseError):
            continue
        query_responses += 1
        for item in root.findall("./channel/item"):
            release_name = str(item.findtext("title") or "").strip()
            enclosure = item.find("enclosure")
            torrent_url = (
                str(enclosure.attrib.get("url") or "").strip()
                if enclosure is not None else ""
            )
            parsed = urllib.parse.urlsplit(torrent_url)
            locator = f"torrent:{torrent_url}"
            if not (
                release_name
                and parsed.scheme == "https"
                and parsed.hostname == "mikanani.me"
                and parsed.username is None and parsed.password is None
                and parsed.path.startswith("/Download/")
                and parsed.path.casefold().endswith(".torrent")
                and not parsed.query and not parsed.fragment
            ):
                continue
            if locator in existing_locators:
                preexcluded += 1
                continue
            release_seasons = _season_markers(release_name)
            if (
                requested_seasons and release_seasons
                and release_seasons.isdisjoint(requested_seasons)
            ):
                resource_failed_locators.add(locator)
                continue
            results.setdefault(torrent_url, release_name)
            if len(results) >= 32:
                hit_cap = True
                break
        if hit_cap:
            break

    candidates: list[dict[str, Any]] = []
    infrastructure_failures = 0
    processed = 0
    for torrent_url, release_name in results.items():
        if time.monotonic() >= deadline:
            break
        with tempfile.TemporaryDirectory(prefix="scrapeflow-mikan-") as directory:
            try:
                manifest = _download_torrent(
                    torrent_url, Path(directory) / "candidate.torrent",
                    timeout=request_timeout(), attempts=1,
                )
            except Exception:
                infrastructure_failures += 1
                continue
        processed += 1
        if _infohash_aliases(manifest["infohash"]) & excluded_hashes:
            resource_failed_locators.add(f"torrent:{torrent_url}")
            continue
        variants = _torrent_candidate_variants(
            request, release_name, torrent_url, manifest,
            include_local=_local_torrent_unlocked(request),
        )
        if variants:
            candidates.extend(variants)
        else:
            resource_failed_locators.update({
                f"quark_magnet:{manifest['infohash']}",
                f"torrent:{torrent_url}",
            })
    return _DynamicSearchResult(
        candidates,
        query_attempts=query_attempts,
        query_responses=query_responses,
        source_exhausted=bool(
            terms and query_attempts == len(terms)
            and query_responses == query_attempts and not hit_cap
            and processed == len(results) and infrastructure_failures == 0
        ),
        resource_failed_locators=sorted(resource_failed_locators),
        infrastructure_failures=infrastructure_failures,
        preexcluded_count=preexcluded,
    )


def _search_dmhy(
    request: Mapping[str, Any], existing_locators: set[str], *, deadline: float,
) -> list[dict[str, Any]]:
    """Search DMHY's public RSS and verify its per-release Torrent metainfo."""
    def request_timeout() -> int:
        remaining = int(deadline - time.monotonic())
        return max(1, min(12, remaining))

    def official_detail_url(value: str) -> str:
        parsed = urllib.parse.urlsplit(value)
        if (
            parsed.scheme not in {"http", "https"}
            or parsed.hostname != "share.dmhy.org"
            or parsed.username is not None or parsed.password is not None
            or parsed.port is not None
            or not parsed.path.startswith("/topics/view/")
            or not parsed.path.casefold().endswith(".html")
            or parsed.query or parsed.fragment
        ):
            return ""
        return urllib.parse.urlunsplit(
            ("https", "share.dmhy.org", parsed.path, "", "")
        )

    def official_torrent_url(detail_url: str, href: str) -> str:
        absolute = urllib.parse.urljoin(detail_url, href)
        parsed = urllib.parse.urlsplit(absolute)
        if (
            parsed.scheme not in {"http", "https"}
            or parsed.hostname != "dl.dmhy.org"
            or parsed.username is not None or parsed.password is not None
            or parsed.port is not None
            or not parsed.path.casefold().endswith(".torrent")
            or parsed.query or parsed.fragment
        ):
            return ""
        return urllib.parse.urlunsplit(
            ("https", "dl.dmhy.org", parsed.path, "", "")
        )

    terms = _dmhy_search_terms(request)
    excluded_hashes = _locator_infohash_aliases(existing_locators)
    results: dict[str, tuple[str, str]] = {}
    query_attempts = 0
    query_responses = 0
    hit_cap = False
    preexcluded_hashes: set[str] = set()
    infrastructure_failure_types: dict[str, int] = {}

    for term in terms:
        if time.monotonic() >= deadline:
            break
        url = "https://share.dmhy.org/topics/rss/rss.xml?" + urllib.parse.urlencode({
            "keyword": term,
        })
        query_attempts += 1
        try:
            page = _fetch_bytes(
                url, max_bytes=4 * 1024 * 1024,
                timeout=request_timeout(), attempts=1,
            )
            root = ET.fromstring(page)
        except (OSError, RuntimeError, ValueError, ET.ParseError) as exc:
            code = _network_failure_code(exc)
            infrastructure_failure_types[code] = (
                infrastructure_failure_types.get(code, 0) + 1
            )
            continue
        query_responses += 1
        for item in root.findall("./channel/item"):
            release_name = str(item.findtext("title") or "").strip()
            detail_url = official_detail_url(str(item.findtext("link") or "").strip())
            enclosure = item.find("enclosure")
            magnet_url = (
                str(enclosure.attrib.get("url") or "").strip()
                if enclosure is not None else ""
            )
            match = re.search(
                r"(?i)(?:urn:)?btih:([0-9a-f]{40}|[a-z2-7]{32})\b",
                magnet_url,
            )
            feed_infohash = match.group(1).casefold() if match else ""
            aliases = _infohash_aliases(feed_infohash)
            if aliases and aliases & excluded_hashes:
                preexcluded_hashes.add(feed_infohash)
                continue
            if not release_name or not detail_url:
                continue
            if detail_url not in results and len(results) >= 32:
                hit_cap = True
                break
            results.setdefault(detail_url, (release_name, feed_infohash))
        if hit_cap:
            break

    candidates: list[dict[str, Any]] = []
    resource_failed_locators: list[str] = []
    infrastructure_failures = 0
    processed = 0
    for detail_url, (release_name, feed_infohash) in results.items():
        if time.monotonic() >= deadline:
            break
        try:
            detail_page = _fetch_bytes(
                detail_url, max_bytes=2 * 1024 * 1024,
                timeout=request_timeout(), attempts=1,
            )
            parser = _AnchorParser()
            parser.feed(detail_page.decode("utf-8", "replace"))
            torrent_urls = sorted({
                url for anchor in parser.anchors
                if (url := official_torrent_url(
                    detail_url, str(anchor.get("href") or "").strip(),
                ))
            })
            if len(torrent_urls) != 1:
                raise ValueError("DMHY detail page lacks one verified Torrent link")
            torrent_url = torrent_urls[0]
            with tempfile.TemporaryDirectory(prefix="scrapeflow-dmhy-") as directory:
                manifest = _download_torrent(
                    torrent_url, Path(directory) / "candidate.torrent",
                    timeout=request_timeout(), attempts=1,
                )
        except (OSError, RuntimeError, ValueError, ET.ParseError) as exc:
            infrastructure_failures += 1
            code = _network_failure_code(exc)
            infrastructure_failure_types[code] = (
                infrastructure_failure_types.get(code, 0) + 1
            )
            continue
        processed += 1
        manifest_aliases = _infohash_aliases(manifest["infohash"])
        feed_aliases = _infohash_aliases(feed_infohash)
        if (
            manifest_aliases & excluded_hashes
            or (feed_aliases and not manifest_aliases & feed_aliases)
        ):
            resource_failed_locators.append(
                f"quark_magnet:{manifest['infohash']}"
            )
            continue
        variants = _torrent_candidate_variants(
            request, release_name, torrent_url, manifest,
            include_local=_local_torrent_unlocked(request),
        )
        if variants:
            candidates.extend(variants)
        else:
            resource_failed_locators.append(
                f"quark_magnet:{manifest['infohash']}"
            )
    return _DynamicSearchResult(
        candidates,
        query_attempts=query_attempts,
        query_responses=query_responses,
        source_exhausted=bool(
            terms and query_attempts == len(terms)
            and query_responses == query_attempts and not hit_cap
            and processed == len(results) and infrastructure_failures == 0
        ),
        resource_failed_locators=resource_failed_locators,
        infrastructure_failures=infrastructure_failures,
        infrastructure_failure_types=infrastructure_failure_types,
        preexcluded_count=len(preexcluded_hashes),
    )


def _search_tokyotosho(
    request: Mapping[str, Any], existing_locators: set[str], *, deadline: float,
) -> list[dict[str, Any]]:
    """Search Tokyo Toshokan's official HTML results without browser UI.

    TokyoTosho exposes a magnet immediately before the corresponding Torrent
    link.  The BTIH is used to discard durable exclusions before the 32-row
    processing cap, so old releases cannot occupy every future search round.
    """
    def request_timeout() -> int:
        remaining = int(deadline - time.monotonic())
        return max(1, min(12, remaining))

    base_url = os.getenv(
        "SCRAPEFLOW_REPLENISHMENT_TOKYOTOSHO_URL",
        "https://tokyo-tosho.net",
    ).strip().rstrip("/")
    parsed_base = urllib.parse.urlsplit(base_url)
    if (
        parsed_base.scheme != "https"
        or parsed_base.hostname not in {
            "tokyo-tosho.net", "tokyotosho.info", "www.tokyotosho.info",
            "tokyotosho.se", "www.tokyotosho.se",
        }
        or parsed_base.username is not None or parsed_base.password is not None
        or parsed_base.port is not None
        or parsed_base.path not in {"", "/"}
        or parsed_base.query or parsed_base.fragment
    ):
        raise ValueError(
            "SCRAPEFLOW_REPLENISHMENT_TOKYOTOSHO_URL must be an official HTTPS origin"
        )

    terms = _compact_dynamic_search_terms(request, maximum=4)
    excluded_hashes = _locator_infohash_aliases(existing_locators)
    results: dict[str, tuple[str, str]] = {}
    query_attempts = 0
    query_responses = 0
    hit_cap = False
    preexcluded_hashes: set[str] = set()
    infrastructure_failure_types: dict[str, int] = {}

    for term in terms:
        if time.monotonic() >= deadline:
            break
        url = base_url + "/search.php?" + urllib.parse.urlencode({
            "terms": term, "searchName": "true", "searchComment": "true",
        })
        parser = _AnchorParser()
        query_attempts += 1
        try:
            page = _fetch_bytes(
                url, max_bytes=8 * 1024 * 1024,
                timeout=request_timeout(), attempts=1,
            )
        except (OSError, RuntimeError, ValueError) as exc:
            code = _network_failure_code(exc)
            infrastructure_failure_types[code] = (
                infrastructure_failure_types.get(code, 0) + 1
            )
            continue
        query_responses += 1
        parser.feed(page.decode("utf-8", "replace"))
        pending_infohash = ""
        for anchor in parser.anchors:
            href = str(anchor.get("href") or "").strip()
            if href.casefold().startswith("magnet:"):
                match = re.search(
                    r"(?i)(?:urn:)?btih:([0-9a-f]{40}|[a-z2-7]{32})\b", href,
                )
                pending_infohash = match.group(1).casefold() if match else ""
                continue
            absolute_url = urllib.parse.urljoin(base_url + "/", href)
            parsed_url = urllib.parse.urlsplit(absolute_url)
            if (
                parsed_url.scheme != "https"
                or not parsed_url.hostname
                or not parsed_url.path.casefold().endswith(".torrent")
            ):
                continue
            release_name = str(anchor.get("text") or "").strip()
            infohash = pending_infohash
            pending_infohash = ""
            aliases = _infohash_aliases(infohash)
            locator = f"torrent:{absolute_url}"
            if aliases and aliases & excluded_hashes:
                preexcluded_hashes.add(infohash)
                continue
            if locator in existing_locators or not release_name:
                continue
            if absolute_url not in results and len(results) >= 32:
                hit_cap = True
                break
            results.setdefault(absolute_url, (release_name, infohash))
        if hit_cap:
            break

    candidates: list[dict[str, Any]] = []
    resource_failed_locators: list[str] = []
    processed = 0
    ranked_results = sorted(
        results.items(),
        key=lambda item: _source_episode_release_priority(
            request, item[1][0],
        ),
    )
    for torrent_url, (release_name, feed_infohash) in ranked_results:
        if time.monotonic() >= deadline:
            break
        processed += 1
        with tempfile.TemporaryDirectory(prefix="scrapeflow-tokyotosho-") as directory:
            try:
                manifest = _download_torrent(
                    torrent_url, Path(directory) / "candidate.torrent",
                    timeout=request_timeout(), attempts=1,
                )
            except Exception:
                aliases = _infohash_aliases(feed_infohash)
                resource_failed_locators.append(
                    f"quark_magnet:{sorted(aliases)[0]}"
                    if aliases else f"torrent:{torrent_url}"
                )
                continue
        manifest_aliases = _infohash_aliases(manifest["infohash"])
        if manifest_aliases & excluded_hashes:
            continue
        variants = _torrent_candidate_variants(
            request, release_name, torrent_url, manifest,
            include_local=_local_torrent_unlocked(request),
        )
        if variants:
            candidates.extend(variants)
        else:
            resource_failed_locators.append(
                f"quark_magnet:{manifest['infohash']}"
            )
    return _DynamicSearchResult(
        candidates,
        query_attempts=query_attempts,
        query_responses=query_responses,
        source_exhausted=bool(
            terms and query_attempts == len(terms) and query_responses == query_attempts
            and not hit_cap and processed == len(results)
        ),
        resource_failed_locators=resource_failed_locators,
        infrastructure_failures=0,
        infrastructure_failure_types=infrastructure_failure_types,
        preexcluded_count=len(preexcluded_hashes),
    )


def _search_animetosho(
    request: Mapping[str, Any], existing_locators: set[str], *, deadline: float,
) -> list[dict[str, Any]]:
    """Search AnimeTosho's official JSON feed for old anime torrents."""
    def request_timeout() -> int:
        remaining = int(deadline - time.monotonic())
        return max(1, min(12, remaining))

    focused = [
        *_source_episode_search_terms(request, maximum=2),
        *_optional_series_title_search_terms(request, maximum=3),
    ]
    terms = list(dict.fromkeys([
        *focused, *_compact_dynamic_search_terms(request, maximum=4),
    ]))[:4]
    results: dict[str, tuple[str, str]] = {}
    query_attempts = 0
    query_responses = 0
    hit_cap = False
    for term in terms:
        if time.monotonic() >= deadline:
            break
        url = "https://feed.animetosho.org/json?q=" + urllib.parse.quote(term)
        query_attempts += 1
        try:
            payload = json.loads(_fetch_bytes(
                url, max_bytes=8 * 1024 * 1024,
                timeout=request_timeout(), attempts=2,
            ))
        except (OSError, RuntimeError, ValueError, json.JSONDecodeError):
            continue
        if not isinstance(payload, list):
            continue
        query_responses += 1
        for row in payload:
            if not isinstance(row, Mapping):
                continue
            release_name = str(row.get("title") or "").strip()
            torrent_url = str(row.get("torrent_url") or "").strip()
            infohash = str(row.get("info_hash") or "").strip().casefold()
            locator = f"torrent:{torrent_url}"
            if (
                release_name
                and torrent_url.startswith("https://storage.animetosho.org/torrent/")
                and locator not in existing_locators
                and (
                    not re.fullmatch(r"[0-9a-f]{40}", infohash)
                    or f"quark_magnet:{infohash}" not in existing_locators
                )
            ):
                results.setdefault(torrent_url, (release_name, infohash))
            if len(results) >= 32:
                hit_cap = True
                break
        if hit_cap:
            break

    candidates: list[dict[str, Any]] = []
    resource_failed_locators: list[str] = []
    infrastructure_failures = 0
    processed = 0
    ranked_results = sorted(
        results.items(),
        key=lambda item: _source_episode_release_priority(
            request, item[1][0],
        ),
    )
    for torrent_url, (release_name, feed_infohash) in ranked_results:
        if time.monotonic() >= deadline:
            break
        with tempfile.TemporaryDirectory(prefix="scrapeflow-animetosho-") as directory:
            try:
                manifest = _download_torrent(
                    torrent_url, Path(directory) / "candidate.torrent",
                    timeout=request_timeout(), attempts=2,
                )
            except Exception:
                infrastructure_failures += 1
                continue
        processed += 1
        if (
            f"quark_magnet:{manifest['infohash']}" in existing_locators
            or (
                feed_infohash
                and f"quark_magnet:{feed_infohash}" in existing_locators
            )
        ):
            continue
        variants = _torrent_candidate_variants(
            request, release_name, torrent_url, manifest,
            include_local=_local_torrent_unlocked(request),
        )
        if variants:
            candidates.extend(variants)
        else:
            resource_failed_locators.append(f"quark_magnet:{manifest['infohash']}")
    source_exhausted = bool(
        terms and query_attempts == len(terms) and query_responses == query_attempts
        and not hit_cap and processed == len(results) and infrastructure_failures == 0
    )
    return _DynamicSearchResult(
        candidates, query_attempts=query_attempts,
        query_responses=query_responses, source_exhausted=source_exhausted,
        resource_failed_locators=resource_failed_locators,
        infrastructure_failures=infrastructure_failures,
    )


def _search_acg(
    request: Mapping[str, Any], existing_locators: set[str], *, deadline: float | None = None,
) -> list[dict[str, Any]]:
    deadline = deadline or time.monotonic() + _bounded_seconds(
        "SCRAPEFLOW_REPLENISHMENT_DYNAMIC_SEARCH_TIMEOUT", 45, 10, 300,
    )

    def request_timeout() -> int:
        remaining = int(deadline - time.monotonic())
        return max(1, min(12, remaining))

    result_pages: dict[str, str] = {}
    query_attempts = 0
    query_responses = 0
    infrastructure_failure_types: dict[str, int] = {}
    terms = _compact_dynamic_search_terms(request, maximum=4)
    acg_opener = _acg_http_opener()
    hit_cap = False
    for term in terms:
        if time.monotonic() >= deadline:
            break
        url = "https://acg.rip/?term=" + urllib.parse.quote(term)
        parser = _AnchorParser()
        query_attempts += 1
        try:
            page = _fetch_bytes(
                url, max_bytes=4 * 1024 * 1024,
                timeout=request_timeout(), attempts=1,
                opener=acg_opener,
            )
        except (OSError, RuntimeError, ValueError) as exc:
            code = _network_failure_code(exc)
            infrastructure_failure_types[code] = (
                infrastructure_failure_types.get(code, 0) + 1
            )
            continue
        query_responses += 1
        parser.feed(page.decode("utf-8", "replace"))
        for anchor in parser.anchors:
            match = re.fullmatch(r"(?:https://acg\.rip)?/t/(\d+)", anchor["href"])
            if match and anchor["text"]:
                result_pages.setdefault(match.group(1), anchor["text"])
            if len(result_pages) >= 16:
                hit_cap = True
                break
        if len(result_pages) >= 16:
            break

    candidates: list[dict[str, Any]] = []
    resource_failed_locators: list[str] = []
    infrastructure_failures = 0
    processed = 0
    for torrent_id, release_name in list(result_pages.items())[:16]:
        if time.monotonic() >= deadline:
            break
        torrent_url = f"https://acg.rip/t/{torrent_id}.torrent"
        locator = f"torrent:{torrent_url}"
        if locator in existing_locators:
            continue
        with tempfile.TemporaryDirectory(prefix="scrapeflow-acg-") as directory:
            torrent_path = Path(directory) / "candidate.torrent"
            try:
                manifest = _download_torrent(
                    torrent_url, torrent_path,
                    timeout=request_timeout(), attempts=1,
                    opener=acg_opener,
                )
            except Exception as exc:
                infrastructure_failures += 1
                code = _network_failure_code(exc)
                infrastructure_failure_types[code] = (
                    infrastructure_failure_types.get(code, 0) + 1
                )
                continue
        processed += 1
        if f"quark_magnet:{manifest['infohash']}" in existing_locators:
            continue
        variants = _torrent_candidate_variants(
            request, release_name, torrent_url, manifest,
            include_local=_local_torrent_unlocked(request),
        )
        if variants:
            candidates.extend(variants)
        else:
            resource_failed_locators.append(f"quark_magnet:{manifest['infohash']}")
    return _DynamicSearchResult(
        candidates,
        query_attempts=query_attempts,
        query_responses=query_responses,
        source_exhausted=bool(
            terms and query_attempts == len(terms) and query_responses == query_attempts
            and not hit_cap and processed == len(result_pages)
            and infrastructure_failures == 0
        ),
        resource_failed_locators=resource_failed_locators,
        infrastructure_failures=infrastructure_failures,
        infrastructure_failure_types=infrastructure_failure_types,
    )


def _subsplease_magnet_manifest(
    magnet_url: str,
) -> tuple[str, dict[str, Any]] | None:
    """Return a safe single-file manifest carried by a SubsPlease magnet."""
    if not isinstance(magnet_url, str) or not magnet_url.startswith("magnet:?"):
        return None
    if len(magnet_url) > 32_768:
        return None
    query = urllib.parse.parse_qs(
        urllib.parse.urlsplit(magnet_url).query, keep_blank_values=False,
    )
    xt = query.get("xt", [""])[0]
    match = re.fullmatch(
        r"urn:btih:([0-9a-fA-F]{40}|[A-Z2-7a-z2-7]{32})", str(xt),
    )
    names = query.get("dn") or []
    sizes = query.get("xl") or []
    if match is None or len(names) != 1 or len(sizes) != 1:
        return None
    name = str(names[0]).replace("\\", "/")
    try:
        size = int(sizes[0])
    except (TypeError, ValueError):
        return None
    if (
        not name or name.startswith("/") or len(name.encode("utf-8")) > 1_024
        or any(part in {"", ".", ".."} for part in name.split("/"))
        or Path(name).suffix.casefold() not in VIDEO_EXTENSIONS
        or size <= 0
    ):
        return None
    aliases = _infohash_aliases(match.group(1))
    infohash = next((value for value in aliases if len(value) == 40), "")
    if not re.fullmatch(r"[0-9a-f]{40}", infohash):
        return None
    return infohash, {
        "root": name, "infohash": infohash,
        "files": {1: {"path": name, "size": size}},
    }


def _search_subsplease(
    request: Mapping[str, Any], existing_locators: set[str], *,
    deadline: float | None = None,
) -> _DynamicSearchResult:
    """Search SubsPlease's public API without treating it as an archive."""
    deadline = deadline or time.monotonic() + _bounded_seconds(
        "SCRAPEFLOW_REPLENISHMENT_DYNAMIC_SEARCH_TIMEOUT", 45, 10, 300,
    )

    def request_timeout() -> int:
        remaining = int(deadline - time.monotonic())
        return max(1, min(12, remaining))

    terms = _compact_dynamic_search_terms(request, maximum=4)
    query_attempts = 0
    query_responses = 0
    hit_cap = False
    infrastructure_failure_types: dict[str, int] = {}
    magnets: dict[str, tuple[str, str, dict[str, Any]]] = {}
    preexcluded_count = 0
    resource_failed_locators: list[str] = []
    for term in terms:
        if time.monotonic() >= deadline:
            break
        url = "https://subsplease.org/api/?" + urllib.parse.urlencode({
            "f": "search", "tz": "UTC", "s": term,
        })
        query_attempts += 1
        try:
            payload = json.loads(_fetch_bytes(
                url, max_bytes=4 * 1024 * 1024,
                timeout=request_timeout(), attempts=1,
            ).decode("utf-8"))
            if payload == []:
                # The API uses an empty JSON array for a successful zero-hit
                # search, while non-empty results are keyed objects.
                payload = {}
            if not isinstance(payload, Mapping):
                raise ValueError("SubsPlease response is not an object")
        except (OSError, RuntimeError, UnicodeDecodeError, ValueError,
                json.JSONDecodeError) as exc:
            code = _network_failure_code(exc)
            infrastructure_failure_types[code] = (
                infrastructure_failure_types.get(code, 0) + 1
            )
            continue
        query_responses += 1
        # The endpoint currently caps broad searches at 30 releases.  Such a
        # page can still contribute candidates, but cannot prove exhaustion.
        if len(payload) >= 30:
            hit_cap = True
        for release_name, release in payload.items():
            if not isinstance(release_name, str) or not isinstance(release, Mapping):
                continue
            downloads = release.get("downloads")
            if not isinstance(downloads, list):
                continue
            for download in downloads:
                if not isinstance(download, Mapping):
                    continue
                magnet_url = download.get("magnet")
                parsed = _subsplease_magnet_manifest(magnet_url)
                if parsed is None:
                    continue
                infohash, manifest = parsed
                aliases = _infohash_aliases(infohash)
                if any(
                    f"quark_magnet:{alias}" in existing_locators
                    for alias in aliases
                ):
                    preexcluded_count += 1
                    continue
                magnets.setdefault(infohash, (release_name, str(magnet_url), manifest))
                if len(magnets) >= 16:
                    hit_cap = True
                    break
            if len(magnets) >= 16:
                break
        if len(magnets) >= 16:
            break

    candidates: list[dict[str, Any]] = []
    for infohash, (release_name, magnet_url, manifest) in magnets.items():
        local = _torrent_candidate(
            request, release_name, "https://subsplease.org/api/", manifest,
        )
        if local is None:
            resource_failed_locators.append(f"quark_magnet:{infohash}")
            continue
        offline = _quark_offline_variant(local)
        if offline is None:
            resource_failed_locators.append(f"quark_magnet:{infohash}")
            continue
        acquisition = dict(offline["acquisition"])
        acquisition["magnet_url"] = magnet_url
        # SubsPlease provides a verified single-file magnet, not immutable
        # torrent metainfo.  Keep it cloud-only instead of manufacturing a
        # local Torrent fallback that could not be revalidated.
        acquisition.pop("torrent_url", None)
        acquisition.pop("local_fallback", None)
        offline["acquisition"] = acquisition
        offline.pop("fallback_locator", None)
        candidates.append(offline)
    return _DynamicSearchResult(
        candidates,
        query_attempts=query_attempts,
        query_responses=query_responses,
        source_exhausted=bool(
            terms and query_attempts == len(terms)
            and query_responses == query_attempts and not hit_cap
        ),
        resource_failed_locators=resource_failed_locators,
        infrastructure_failure_types=infrastructure_failure_types,
        preexcluded_count=preexcluded_count,
    )


class _BDecoder:
    def __init__(self, data: bytes) -> None:
        self.data = data

    def parse(self, offset: int = 0) -> tuple[Any, int]:
        data = self.data
        marker = data[offset:offset + 1]
        if marker == b"i":
            end = data.index(b"e", offset)
            return int(data[offset + 1:end]), end + 1
        if marker == b"l":
            values: list[Any] = []
            offset += 1
            while data[offset:offset + 1] != b"e":
                value, offset = self.parse(offset)
                values.append(value)
            return values, offset + 1
        if marker == b"d":
            values: dict[bytes, Any] = {}
            offset += 1
            while data[offset:offset + 1] != b"e":
                key, offset = self.parse(offset)
                value, offset = self.parse(offset)
                if not isinstance(key, bytes):
                    raise ValueError("torrent 字典键格式无效")
                values[key] = value
            return values, offset + 1
        separator = data.index(b":", offset)
        size = int(data[offset:separator])
        start = separator + 1
        return data[start:start + size], start + size


def _bencode(value: Any) -> bytes:
    if isinstance(value, int):
        return b"i" + str(value).encode("ascii") + b"e"
    if isinstance(value, bytes):
        return str(len(value)).encode("ascii") + b":" + value
    if isinstance(value, list):
        return b"l" + b"".join(_bencode(item) for item in value) + b"e"
    if isinstance(value, dict):
        return b"d" + b"".join(_bencode(key) + _bencode(item) for key, item in value.items()) + b"e"
    raise TypeError(type(value))


def _torrent_manifest(data: bytes) -> dict[str, Any]:
    meta, offset = _BDecoder(data).parse()
    if offset != len(data) or not isinstance(meta, dict) or not isinstance(meta.get(b"info"), dict):
        raise ValueError("torrent 元数据格式无效")
    info = meta[b"info"]
    root = info.get(b"name")
    if not isinstance(root, bytes):
        raise ValueError("torrent 缺少名称")
    files: dict[int, dict[str, Any]] = {}
    if isinstance(info.get(b"files"), list):
        for index, row in enumerate(info[b"files"], start=1):
            if not isinstance(row, dict) or not isinstance(row.get(b"path"), list):
                raise ValueError("torrent 文件清单格式无效")
            parts = row[b"path"]
            if not all(isinstance(part, bytes) for part in parts):
                raise ValueError("torrent 文件路径格式无效")
            files[index] = {
                "path": "/".join(part.decode("utf-8", "replace") for part in parts),
                "size": row.get(b"length"),
            }
    else:
        files[1] = {"path": root.decode("utf-8", "replace"), "size": info.get(b"length")}
    return {
        "root": root.decode("utf-8", "replace"),
        "infohash": hashlib.sha1(_bencode(info)).hexdigest(),
        "files": files,
    }


def _download_torrent(
    url: str, destination: Path, *, timeout: int = 60, attempts: int = 4,
    opener: Any | None = None,
) -> dict[str, Any]:
    if not url.startswith("https://"):
        raise ValueError("torrent 地址需要使用 HTTPS")
    data = _fetch_bytes(
        url, max_bytes=MAX_TORRENT_BYTES, timeout=timeout,
        attempts=attempts, opener=opener,
    )
    manifest = _torrent_manifest(data)
    destination.write_bytes(data)
    return manifest


def _selected_indices(selection: Mapping[str, Any]) -> tuple[set[int], dict[int, list[str]]]:
    acquisition = selection.get("acquisition")
    if not isinstance(acquisition, Mapping) or acquisition.get("kind") != "torrent":
        raise ValueError("选中候选缺少 torrent 获取说明")
    gap_map = acquisition.get("file_index_by_gap")
    if not isinstance(gap_map, Mapping):
        raise ValueError("选中候选缺少集号到 torrent 文件索引映射")
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


def _base32_infohash(hex_hash: str) -> str:
    if not re.fullmatch(r"[0-9a-f]{40}", hex_hash):
        return ""
    import base64
    return base64.b32encode(bytes.fromhex(hex_hash)).decode("ascii").rstrip("=").casefold()


def _payload_is_complete(
    payload_dir: Path, acquisition: Mapping[str, Any], indices: set[int],
) -> bool:
    """Trust a retained payload only after aria2 finished and every file matches."""
    if not payload_dir.is_dir() or any(payload_dir.rglob("*.aria2")):
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
    if "quark_fast_save" in acquisition_kinds:
        raise ReplenishmentInfrastructureError(
            "夸克快转执行器未配置；候选保留并等待分享重验证",
            stage="quark_fast_save_not_configured",
        )
    if shutil.which("aria2c") is None:
        raise ReplenishmentInfrastructureError(
            "运行环境缺少 aria2c", stage="local_dependency",
        )
    workspace.mkdir(parents=True, exist_ok=True)
    selected_bytes = 0
    selected_files = 0
    reusable_bytes = 0
    verified: list[dict[str, Any]] = []
    for offset, selection in enumerate(selections, start=1):
        if not isinstance(selection, Mapping):
            raise ValueError("selection 项格式无效")
        acquisition = selection.get("acquisition")
        url = acquisition.get("url") if isinstance(acquisition, Mapping) else None
        if not isinstance(url, str):
            raise ValueError("选中候选缺少 torrent URL")
        torrent_path = workspace / f"candidate-{offset:02d}.torrent"
        try:
            manifest = _download_torrent(url, torrent_path)
            indices, _by_index = _verify_manifest(selection, manifest)
        except (OSError, ValueError, RuntimeError, urllib.error.URLError) as exc:
            raise ReplenishmentCandidateError(
                str(exc), stage="candidate_preflight", candidate=selection,
            ) from exc
        files = manifest["files"]
        byte_count = sum(int(files[index]["size"]) for index in indices)
        selected_bytes += byte_count
        selected_files += len(indices)
        if resume_workspace is not None and _payload_is_complete(
            resume_workspace / f"download-{offset:02d}" / "payload",
            acquisition,
            indices,
        ):
            reusable_bytes += byte_count
        verified.append({
            "release_name": selection.get("release_name"),
            "torrent": url,
            "infohash": manifest["infohash"],
            "selected_indices": sorted(indices),
            "selected_files": len(indices),
            "selected_bytes": byte_count,
            "torrent_path": str(torrent_path),
            "manifest": manifest,
        })
    free = shutil.disk_usage(workspace).free
    remaining_bytes = selected_bytes - reusable_bytes
    required = int(remaining_bytes * 1.15) + 1024 ** 3
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


def _quark_fast_save_port(
    selection: Mapping[str, Any], destination: str, *, dry_run: bool = False,
    resume_task_id: str | None = None, on_prepared: Any = None,
    on_submitted: Any = None,
) -> dict[str, Any]:
    """Bind the generic FastSavePort to delegated AList Quark login state."""
    bridge = QuarkFastSaveBridge(UrlLibQuarkTransport())
    if dry_run:
        return bridge.dry_run(selection, destination)
    client = _alist_client()
    session = delegated_quark_session(client, destination)
    return bridge.execute(
        selection, destination, session, resume_task_id=resume_task_id,
        on_prepared=on_prepared, on_submitted=on_submitted,
    )


def _quark_magnet_offline_port(
    selection: Mapping[str, Any], destination: str, *, dry_run: bool = False,
    resume_task_id: str | None = None,
    on_submitted: Any = None,
) -> dict[str, Any]:
    helper_url = os.getenv("SCRAPEFLOW_QUARK_HELPER_URL", "").strip()
    if helper_url:
        transport = QuarkNativeHelperTransport(
            helper_url, os.getenv("SCRAPEFLOW_QUARK_HELPER_TOKEN", ""),
            timeout=float(_bounded_seconds(
                "SCRAPEFLOW_QUARK_HELPER_TIMEOUT", 120, 10, 600,
            )),
            # Unattended replenishment may use an already-ready WSG runtime,
            # but it must never launch/restart/activate the desktop app.
            passive_only=True,
        )
    else:
        transport = UrlLibQuarkTransport()
    bridge = QuarkMagnetOfflineBridge(transport)
    if dry_run:
        return bridge.dry_run(selection, destination)
    client = _alist_client()
    session = delegated_quark_session(client, destination)
    return bridge.execute(
        selection, destination, session,
        resume_task_id=resume_task_id, on_submitted=on_submitted,
    )


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
) -> dict[str, Any]:
    bundle = wrapper.get("selection")
    rows = bundle.get("selections") if isinstance(bundle, Mapping) else None
    if not isinstance(rows, list) or not rows or not all(isinstance(row, dict) for row in rows):
        raise ReplenishmentInfrastructureError("选择文件缺少 selections", stage="artifact_validation")
    torrents = [dict(row) for row in rows if _selection_acquisition_kind(row) == "torrent"]
    shares = [
        dict(row) for row in rows
        if _selection_acquisition_kind(row) in {"quark_fast_save", "quark_sfx_archive"}
    ]
    offline = [
        dict(row) for row in rows
        if _selection_acquisition_kind(row) == "quark_magnet_offline"
    ]
    if len(torrents) + len(shares) + len(offline) != len(rows):
        raise ReplenishmentInfrastructureError("选择包含未知 acquisition kind", stage="artifact_validation")
    result: dict[str, Any] = {
        "status": "verified", "selected_files": 0, "selected_bytes": 0,
        "reusable_bytes": 0, "remaining_bytes": 0, "candidates": [],
    }
    if torrents:
        torrent_result = _preflight(
            _wrapper_for_selections(wrapper, torrents), workspace / "torrent",
            resume_workspace=(resume_workspace / "torrent" if resume_workspace else None),
        )
        for key in ("selected_files", "selected_bytes", "reusable_bytes", "remaining_bytes"):
            result[key] += int(torrent_result.get(key) or 0)
        result["candidates"].extend(torrent_result.get("candidates") or [])
        result["free_bytes"] = torrent_result.get("free_bytes")
        result["required_bytes"] = torrent_result.get("required_bytes")
    for share in shares:
        try:
            normalized = normalize_quark_fast_save_selection(share)
        except QuarkBridgeError as exc:
            raise ReplenishmentCandidateError(
                str(exc), stage="candidate_manifest", candidate=share,
            ) from exc
        expected = normalized["acquisition"]["expected_files"]
        if _selection_acquisition_kind(share) == "quark_sfx_archive":
            _quark_archive_members(share)
        result["selected_files"] += len(expected)
        result["selected_bytes"] += sum(int(row["size"]) for row in expected)
        result["candidates"].append({
            "release_name": share.get("release_name"), "kind": "quark_fast_save",
            "selected_files": len(expected),
            "selected_bytes": sum(int(row["size"]) for row in expected),
            "expected_files": expected,
        })
    for candidate in offline:
        try:
            plan = QuarkMagnetOfflineBridge.dry_run(candidate, "/fixture/destination")
        except QuarkBridgeError as exc:
            raise ReplenishmentCandidateError(
                str(exc), stage="candidate_manifest", candidate=candidate,
            ) from exc
        result["selected_files"] += len(plan["expected_files"])
        result["selected_bytes"] += sum(int(row["size"]) for row in plan["expected_files"])
        result["candidates"].append({
            "release_name": candidate.get("release_name"),
            "kind": "quark_magnet_offline", "infohash": plan["infohash"],
            "selected_files": len(plan["expected_files"]),
            "selected_bytes": sum(int(row["size"]) for row in plan["expected_files"]),
            "expected_files": plan["expected_files"],
        })
    return result


def _quark_task_epoch(value: Any) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    epoch = float(value)
    if epoch > 100_000_000_000:
        epoch /= 1000.0
    return epoch if epoch > 0 else None


def _completed_quark_share_task_can_resubmit(
    checkpoint: Mapping[str, Any], *, missing_names: set[str],
) -> bool:
    """Allow an exact retry only after strong terminal-and-absence evidence."""
    if checkpoint.get("status") != "completed_missing":
        return False
    task_id = checkpoint.get("task_id")
    observations = checkpoint.get("missing_observations")
    recorded_missing = checkpoint.get("missing_files")
    finished_at = _quark_task_epoch(checkpoint.get("task_finished_at"))
    if (
        not isinstance(task_id, str) or not task_id
        or isinstance(observations, bool) or not isinstance(observations, int)
        or observations < 2
        or not isinstance(recorded_missing, list)
        or {str(name) for name in recorded_missing} != missing_names
        or finished_at is None
    ):
        return False
    grace = _bounded_seconds(
        "SCRAPEFLOW_QUARK_FAST_SAVE_STALE_SECONDS", 900, 60, 86_400,
    )
    return time.time() - finished_at >= grace


def _record_completed_quark_share_missing_observations(
    client: AListClient, remote_root: str,
    receipts: list[dict[str, Any]],
) -> None:
    """Persist refreshed absence evidence for terminal fast-save tasks.

    Failure to obtain a fresh directory listing records nothing.  The normal
    delivery error remains authoritative and the original task checkpoint is
    preserved, so an AList outage can never authorize a duplicate submit.
    """
    if not receipts:
        return
    rows = client.try_list(remote_root, refresh=True)
    if rows is None:
        return
    visible = {
        (str(row.get("name") or ""), int(row.get("size") or 0))
        for row in rows if isinstance(row, Mapping) and not row.get("is_dir")
    }
    observed_at = time.time()
    for receipt in receipts:
        checkpoint_path = receipt.get("checkpoint_path")
        expected = receipt.get("expected_files")
        if not isinstance(checkpoint_path, Path) or not isinstance(expected, list):
            continue
        missing = sorted({
            str(row.get("name") or "")
            for row in expected if isinstance(row, Mapping)
            and (str(row.get("name") or ""), int(row.get("size") or 0)) not in visible
        })
        if not missing:
            continue
        try:
            checkpoint = _load(checkpoint_path)
        except (OSError, ValueError, json.JSONDecodeError):
            continue
        task_id = receipt.get("task_id")
        if checkpoint.get("task_id") != task_id:
            continue
        same_observation = bool(
            checkpoint.get("status") == "completed_missing"
            and checkpoint.get("missing_files") == missing
        )
        observations = (
            int(checkpoint.get("missing_observations") or 0) + 1
            if same_observation else 1
        )
        checkpoint.update({
            "status": "completed_missing",
            "task_status": 2,
            "task_created_at": receipt.get("task_created_at"),
            "task_finished_at": receipt.get("task_finished_at"),
            "missing_files": missing,
            "missing_observations": observations,
            "first_missing_observed_at": (
                checkpoint.get("first_missing_observed_at")
                if same_observation else observed_at
            ),
            "last_missing_observed_at": observed_at,
        })
        _atomic_json(checkpoint_path, checkpoint)


def _acquire_quark_bundle(
    wrapper: Mapping[str, Any], workspace: Path | None = None,
    *, remote_parent_override: str | None = None,
) -> dict[str, Any]:
    request = wrapper.get("request") if isinstance(wrapper.get("request"), Mapping) else {}
    media = request.get("media") if isinstance(request.get("media"), Mapping) else {}
    tmdb_id = media.get("tmdb_id")
    title = _safe_name(str(media.get("title") or f"TMDB-{tmdb_id}"), limit=60)
    remote_parent = (
        remote_parent_override
        or os.getenv(
            "SCRAPEFLOW_REPLENISHMENT_UNSCRAPED_ROOT",
            "/quark/影视/ScrapeFlow/补源",
        )
    ).rstrip("/")
    remote_root = join_remote(
        remote_parent,
        _safe_name(f"ScrapeFlow补源-{tmdb_id}-{title}-{_selection_workspace_key(wrapper)}"),
    )
    bundle = wrapper.get("selection")
    selections = bundle.get("selections") if isinstance(bundle, Mapping) else []
    client = _alist_client()
    try:
        client.mkdir(remote_root)
    except Exception as exc:
        raise ReplenishmentInfrastructureError(str(exc), stage="delivery_prepare") from exc
    all_expected: list[dict[str, Any]] = []
    expected_names: set[str] = set()
    saved_files = 0
    saved_bytes = 0
    completed_task_receipts: list[dict[str, Any]] = []
    checkpoint_root = workspace or Path(os.getenv(
        "SCRAPEFLOW_REPLENISHMENT_DOWNLOAD_DIR", "/var/tmp/scrapeflow/replenishment",
    )) / "share-checkpoints" / _selection_workspace_key(wrapper)
    for selection_index, selection in enumerate(selections, start=1):
        try:
            normalized = normalize_quark_fast_save_selection(selection)
        except QuarkBridgeError as exc:
            raise ReplenishmentCandidateError(
                str(exc), stage="candidate_manifest", candidate=selection,
            ) from exc
        expected = [
            {"remote_name": row["name"], "name": row["name"],
             "size": row["size"], "gap_ids": row["gap_ids"]}
            for row in normalized["acquisition"]["expected_files"]
        ]
        collisions = expected_names & {str(row["name"]) for row in expected}
        if collisions:
            raise ReplenishmentCandidateError(
                f"多个 Quark 候选在目标目录产生同名文件: {sorted(collisions)[0]}",
                stage="candidate_manifest", candidate=selection,
            )
        expected_names.update(str(row["name"]) for row in expected)
        all_expected.extend(expected)
        missing_names = {
            row["name"] for row in expected
            if not _remote_upload_matches(client, remote_root, row["name"], int(row["size"]))
        }
        if not missing_names:
            print(f"[replenishment] 复用已到盘快转: {selection.get('release_name')}", flush=True)
            continue
        missing_gaps = [
            gap for gap, name in normalized["acquisition"]["file_name_by_gap"].items()
            if name in missing_names
        ]
        checkpoint_path = checkpoint_root / f"task-{selection_index:02d}.json"
        current_missing_expected = [
            {"name": row["name"], "size": row["size"], "gap_ids": row["gap_ids"]}
            for row in expected if row["name"] in missing_names
        ]
        submit_expected = list(current_missing_expected)
        resume_task_id = None
        resubmit_evidence: dict[str, Any] | None = None
        if checkpoint_path.exists():
            try:
                checkpoint = _load(checkpoint_path)
            except (OSError, ValueError, json.JSONDecodeError) as exc:
                raise QuarkShareInDoubtError(
                    "Quark fast-save checkpoint 无法读取；禁止重复快转或降级接管"
                ) from exc
            acquisition = normalized["acquisition"]
            share_id = acquisition.get("pwd_id") or acquisition.get("share_id")
            checkpoint_expected = checkpoint.get("expected_files")
            if (
                checkpoint.get("destination") != remote_root
                or checkpoint.get("locator") != selection.get("locator")
                or checkpoint.get("share_id") != share_id
                or not isinstance(checkpoint_expected, list)
                or not checkpoint_expected
                or not all(
                    isinstance(row, Mapping) and dict(row) in [
                        {"name": item["name"], "size": item["size"],
                         "gap_ids": item["gap_ids"]}
                        for item in expected
                    ]
                    for row in checkpoint_expected
                )
            ):
                raise QuarkShareInDoubtError(
                    "Quark fast-save checkpoint 与当前精确计划不符；禁止重复快转或降级接管"
                )
            task_id = checkpoint.get("task_id")
            if (
                isinstance(task_id, str) and task_id
                and _completed_quark_share_task_can_resubmit(
                    checkpoint, missing_names=missing_names,
                )
            ):
                # A terminal task is no longer in doubt.  After repeated,
                # refreshed absence observations and a bounded aging window,
                # submit only the exact files still missing.  This recovers
                # from files removed after a successful save without creating
                # duplicates for members that remain visible.
                resubmit_evidence = {
                    "task_id": task_id,
                    "task_finished_at": checkpoint.get("task_finished_at"),
                    "missing_observations": checkpoint.get("missing_observations"),
                    "missing_files": sorted(missing_names),
                }
                submit_expected = list(current_missing_expected)
            elif isinstance(task_id, str) and task_id:
                resume_task_id = task_id
                submit_expected = [dict(row) for row in checkpoint_expected]
            else:
                raise QuarkShareInDoubtError(
                    "Quark fast-save submit 状态未定；到盘未齐且无 task_id，禁止重提或降级接管"
                )
        submit_gaps = sorted({
            str(gap) for row in submit_expected for gap in row.get("gap_ids") or []
        })
        retry_selection = dict(normalized, selected_gap_ids=submit_gaps)
        mutation_prepared = False
        task_persisted = resume_task_id is not None

        def persist_prepared() -> None:
            nonlocal mutation_prepared
            acquisition = normalized["acquisition"]
            payload = {
                "version": 1, "status": "prepared",
                "destination": remote_root, "locator": selection.get("locator"),
                "share_id": acquisition.get("pwd_id") or acquisition.get("share_id"),
                "selected_gap_ids": submit_gaps,
                "expected_files": submit_expected,
                "prepared_at": time.time(),
            }
            if resubmit_evidence is not None:
                payload["replaces_completed_missing"] = resubmit_evidence
            _atomic_json(checkpoint_path, payload)
            mutation_prepared = True

        def persist_task(task_id: str) -> None:
            nonlocal task_persisted
            acquisition = normalized["acquisition"]
            payload = {
                "version": 1, "status": "submitted", "task_id": task_id,
                "destination": remote_root, "locator": selection.get("locator"),
                "share_id": acquisition.get("pwd_id") or acquisition.get("share_id"),
                "selected_gap_ids": submit_gaps,
                "expected_files": submit_expected,
                "prepared_at": time.time(), "submitted_at": time.time(),
            }
            if resubmit_evidence is not None:
                payload["replaces_completed_missing"] = resubmit_evidence
            _atomic_json(checkpoint_path, payload)
            task_persisted = True
        try:
            receipt = _quark_fast_save_port(
                retry_selection, remote_root, resume_task_id=resume_task_id,
                on_prepared=persist_prepared, on_submitted=persist_task,
            )
        except Exception as exc:
            if resume_task_id is not None or mutation_prepared or task_persisted:
                raise QuarkShareInDoubtError(
                    "Quark fast-save 已越过 mutation 边界；保留 checkpoint 并仅允许恢复轮询"
                ) from exc
            if (
                isinstance(exc, QuarkBridgeError)
                and getattr(exc, "failure_scope", None) == "candidate"
            ):
                exc.candidate = {
                    key: selection.get(key)
                    for key in ("provider", "release_name", "locator")
                    if selection.get(key) is not None
                }
            raise
        receipt_task_id = receipt.get("task_id")
        mutation_committed = task_persisted or (
            isinstance(receipt_task_id, str) and bool(receipt_task_id)
        )
        if (
            receipt.get("status") not in {"submitted", "ready"}
            or receipt.get("destination") != remote_root
        ):
            if mutation_committed:
                raise QuarkShareInDoubtError(
                    "Quark fast-save 已提交但 receipt 目标或状态异常；仅允许恢复核验"
                )
            raise ReplenishmentInfrastructureError(
                "Quark fast-save receipt 目标或状态与请求不符",
                stage="artifact_validation",
            )
        rows = receipt.get("expected_files")
        if not isinstance(rows, list):
            if mutation_committed:
                raise QuarkShareInDoubtError(
                    "Quark fast-save 已提交但 receipt 缺少 manifest；仅允许恢复核验"
                )
            raise ReplenishmentInfrastructureError(
                "Quark fast-save receipt 缺少 expected_files", stage="artifact_validation",
            )
        receipt_files = {
            (str(row.get("name") or ""), int(row.get("size") or 0))
            for row in rows if isinstance(row, Mapping)
        }
        planned_files = {
            (str(row["name"]), int(row["size"])) for row in submit_expected
        }
        if receipt_files != planned_files:
            if mutation_committed:
                raise QuarkShareInDoubtError(
                    "Quark fast-save 已提交但 receipt manifest 异常；仅允许恢复核验"
                )
            raise ReplenishmentCandidateError(
                "Quark fast-save receipt 与精确候选 manifest 不符",
                stage="candidate_manifest", candidate=selection,
            )
        saved_files += len(rows)
        saved_bytes += sum(int(row.get("size") or 0) for row in rows if isinstance(row, Mapping))
        if (
            receipt.get("task_status") == 2
            and isinstance(receipt.get("task_id"), str)
            and receipt.get("task_id")
        ):
            completed_task_receipts.append({
                "checkpoint_path": checkpoint_path,
                "task_id": receipt["task_id"],
                "task_created_at": receipt.get("task_created_at"),
                "task_finished_at": receipt.get("task_finished_at"),
                "expected_files": [dict(row) for row in submit_expected],
            })
    try:
        _verify_remote_uploads(client, remote_root, all_expected)
    except Exception as exc:
        _record_completed_quark_share_missing_observations(
            client, remote_root, completed_task_receipts,
        )
        raise ReplenishmentDeliveryError(str(exc), stage="delivery_visibility") from exc
    return {
        "status": "ready", "source_paths": [remote_root],
        "provider": "quark_share", "materialization": "fast_save",
        "saved_files": saved_files, "saved_bytes": saved_bytes,
    }


def _quark_archive_members(
    selection: Mapping[str, Any],
    archive_members: list[dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    """Validate or safely discover the archive-member-to-gap contract.

    A reviewed catalog may carry an exact member path and size.  Older Quark
    indexes only know the immutable share file id/path/size, because the SFX
    cannot be listed until it has been copied and downloaded.  That weaker
    contract is still safe for a *single-gap* archive when the validated 7-Zip
    listing contains exactly one video file: the archive filename establishes
    the episode identity and the unique video member establishes the payload.
    Multi-gap archives always require an explicit per-gap member manifest.
    """
    acquisition = selection.get("acquisition")
    selected = selection.get("selected_gap_ids")
    if not isinstance(acquisition, Mapping) or not isinstance(selected, list) or not selected:
        raise ReplenishmentCandidateError(
            "SFX archive candidate 缺少 selected gaps",
            stage="candidate_archive_manifest", candidate=selection,
        )
    path_map = acquisition.get("archive_member_by_gap")
    size_map = acquisition.get("archive_member_size_by_gap")
    has_explicit_manifest = (
        isinstance(path_map, Mapping) and isinstance(size_map, Mapping)
        and all(gap in path_map and gap in size_map for gap in selected)
    )
    if not has_explicit_manifest:
        if not isinstance(selected[0], str) or not selected[0]:
            raise ReplenishmentCandidateError(
                "SFX archive gap id 无效",
                stage="candidate_archive_manifest", candidate=selection,
            )
        videos = [
            row for row in (archive_members or [])
            if not row.get("is_dir")
            and Path(str(row.get("path") or "")).suffix.casefold() in VIDEO_EXTENSIONS
        ]
        if len(selected) != 1 or len(videos) != 1:
            raise ReplenishmentCandidateError(
                "SFX archive 缺少精确成员映射，且不是单 gap/单视频安全归因",
                stage="candidate_archive_manifest", candidate=selection,
            )
        video = videos[0]
        size = video.get("size")
        if type(size) is not int or size <= 0:
            raise ReplenishmentCandidateError(
                "SFX archive 唯一视频成员大小无效",
                stage="candidate_archive_manifest", candidate=selection,
            )
        return [{
            "gap_id": selected[0],
            "path": str(video["path"]).replace("\\", "/"),
            "size": size,
            "binding": "unique_video_member",
        }]
    rows: list[dict[str, Any]] = []
    seen_paths: set[str] = set()
    for gap in selected:
        path = path_map.get(gap)
        size = size_map.get(gap)
        if not isinstance(gap, str) or not isinstance(path, str) or type(size) is not int or size <= 0:
            raise ReplenishmentCandidateError(
                "SFX archive gap 成员路径/大小无效",
                stage="candidate_archive_manifest", candidate=selection,
            )
        normalized = path.replace("\\", "/")
        parts = normalized.split("/")
        if (
            normalized.startswith("/") or not parts
            or any(not part or part in {".", ".."} or "\x00" in part for part in parts)
        ):
            raise ReplenishmentCandidateError(
                f"SFX archive 成员路径不安全: {path!r}",
                stage="candidate_archive_manifest", candidate=selection,
            )
        if Path(normalized).suffix.casefold() not in VIDEO_EXTENSIONS:
            raise ReplenishmentCandidateError(
                f"SFX archive gap 未映射到视频: {gap}",
                stage="candidate_archive_manifest", candidate=selection,
            )
        key = normalized.casefold()
        if key in seen_paths:
            raise ReplenishmentCandidateError(
                "SFX archive 多个 gap 映射到同一成员",
                stage="candidate_archive_manifest", candidate=selection,
            )
        seen_paths.add(key)
        rows.append({
            "gap_id": gap, "path": normalized, "size": size,
            "binding": "reviewed_member_manifest",
        })
    return rows


def _split_quark_sfx_selection(selection: Mapping[str, Any]) -> list[dict[str, Any]]:
    """Split a share-level SFX selection into one row per physical archive."""
    acquisition = selection.get("acquisition")
    selected = selection.get("selected_gap_ids")
    if not isinstance(acquisition, Mapping) or not isinstance(selected, list) or not selected:
        raise ReplenishmentCandidateError(
            "SFX archive candidate 缺少 selected gaps",
            stage="candidate_archive_manifest", candidate=selection,
        )
    gap_map = acquisition.get("file_id_by_gap")
    path_map = acquisition.get("file_path_by_id")
    size_map = acquisition.get("file_size_by_id")
    if not all(isinstance(value, Mapping) for value in (gap_map, path_map, size_map)):
        raise ReplenishmentCandidateError(
            "SFX archive candidate 缺少精确分享文件 manifest",
            stage="candidate_archive_manifest", candidate=selection,
        )
    gaps_by_file: dict[str, list[str]] = {}
    for gap in selected:
        raw_ids = gap_map.get(gap)
        file_ids = [raw_ids] if isinstance(raw_ids, str) else raw_ids
        if (
            not isinstance(gap, str) or not gap
            or not isinstance(file_ids, list) or len(file_ids) != 1
            or not isinstance(file_ids[0], str) or not file_ids[0]
        ):
            raise ReplenishmentCandidateError(
                "SFX archive 每个 gap 必须精确映射一个分享文件",
                stage="candidate_archive_manifest", candidate=selection,
            )
        gaps_by_file.setdefault(file_ids[0], []).append(gap)
    output: list[dict[str, Any]] = []
    for file_id, gaps in gaps_by_file.items():
        path = path_map.get(file_id)
        size = size_map.get(file_id)
        if not isinstance(path, str) or not path or type(size) is not int or size <= 0:
            raise ReplenishmentCandidateError(
                "SFX archive 分享文件路径/大小不完整",
                stage="candidate_archive_manifest", candidate=selection,
            )
        child_acquisition = dict(acquisition)
        child_acquisition["file_id_by_gap"] = {gap: [file_id] for gap in gaps}
        child_acquisition["file_path_by_id"] = {file_id: path}
        child_acquisition["file_size_by_id"] = {file_id: size}
        for key in ("archive_member_by_gap", "archive_member_size_by_gap"):
            value = acquisition.get(key)
            if isinstance(value, Mapping):
                child_acquisition[key] = {gap: value[gap] for gap in gaps if gap in value}
        child = dict(selection)
        child["selected_gap_ids"] = list(gaps)
        child["acquisition"] = child_acquisition
        share_id = str(
            acquisition.get("share_id") or acquisition.get("pwd_id") or "unknown"
        )
        child["locator"] = _quark_sfx_locator(share_id, file_id, path)
        output.append(child)
    return output


CANONICAL_VALIDATION_ROOT = "/quark/影视/ScrapeFlow/验证"
MAX_VALIDATION_SCAN_DIRECTORIES = 2_000
MAX_VALIDATION_SCAN_FILES = 20_000


def _canonical_validation_roots() -> list[str]:
    """Return only the governed validation root or explicitly selected children.

    A caller may narrow the scan to one or more comma-separated subdirectories,
    but may not redirect archive reuse to an arbitrary media/library path.  A
    missing narrow root is equivalent to an empty validation cache; transport
    errors remain fail-closed in ``_index_canonical_validation_archives``.
    """
    raw = os.getenv(
        "SCRAPEFLOW_REPLENISHMENT_VALIDATION_ROOTS", CANONICAL_VALIDATION_ROOT,
    )
    values = [value.strip().rstrip("/") for value in raw.split(",") if value.strip()]
    roots: list[str] = []
    for value in values:
        if not value.startswith("/") or "\x00" in value:
            raise ReplenishmentInfrastructureError(
                "canonical 验证目录配置无效", stage="validation_archive_scan",
            )
        if not (
            value == CANONICAL_VALIDATION_ROOT
            or value.startswith(CANONICAL_VALIDATION_ROOT + "/")
        ):
            raise ReplenishmentInfrastructureError(
                "归档复用只能扫描 canonical 验证目录",
                stage="validation_archive_scan",
            )
        if value not in roots:
            roots.append(value)
    if not roots:
        raise ReplenishmentInfrastructureError(
            "canonical 验证目录配置为空", stage="validation_archive_scan",
        )
    return roots


def _index_canonical_validation_archives(
    client: AListClient,
) -> dict[tuple[str, int], list[str]]:
    """Build a bounded exact-name+size index without mutating AList.

    The scan intentionally includes only ``.exe`` SFX objects.  An API error,
    malformed entry, changing subtree, or exceeded bound blocks acquisition:
    after an inconclusive validation scan we cannot safely prove that another
    cloud copy is necessary.
    """
    index: dict[tuple[str, int], list[str]] = {}
    configured_roots = set(_canonical_validation_roots())
    queue = list(configured_roots)
    visited: set[str] = set()
    file_count = 0
    while queue:
        current = queue.pop(0)
        if current in visited:
            continue
        if len(visited) >= MAX_VALIDATION_SCAN_DIRECTORIES:
            raise ReplenishmentInfrastructureError(
                "canonical 验证目录数量超过安全上限",
                stage="validation_archive_scan",
            )
        visited.add(current)
        try:
            entries = client.try_list(current, refresh=True)
        except Exception as exc:
            raise ReplenishmentInfrastructureError(
                str(exc), stage="validation_archive_scan",
            ) from exc
        if entries is None:
            # A configured root may not have been created yet.  A nested path
            # disappearing after its parent was listed is a race and must not
            # be treated as proof that the archive is absent.
            if current in configured_roots:
                continue
            raise ReplenishmentInfrastructureError(
                "canonical 验证子目录在扫描期间消失",
                stage="validation_archive_scan",
            )
        if not isinstance(entries, list):
            raise ReplenishmentInfrastructureError(
                "canonical 验证目录返回格式异常",
                stage="validation_archive_scan",
            )
        for entry in entries:
            if not isinstance(entry, Mapping):
                raise ReplenishmentInfrastructureError(
                    "canonical 验证目录包含无效条目",
                    stage="validation_archive_scan",
                )
            name = entry.get("name")
            if (
                not isinstance(name, str) or not name or name in {".", ".."}
                or "/" in name or "\\" in name or "\x00" in name
            ):
                raise ReplenishmentInfrastructureError(
                    "canonical 验证目录包含不安全名称",
                    stage="validation_archive_scan",
                )
            path = join_remote(current, name)
            if entry.get("is_dir"):
                queue.append(path)
                continue
            file_count += 1
            if file_count > MAX_VALIDATION_SCAN_FILES:
                raise ReplenishmentInfrastructureError(
                    "canonical 验证文件数量超过安全上限",
                    stage="validation_archive_scan",
                )
            if Path(name).suffix.casefold() != ".exe":
                continue
            size = entry.get("size")
            if isinstance(size, bool):
                size = None
            try:
                parsed_size = int(size)
            except (TypeError, ValueError):
                raise ReplenishmentInfrastructureError(
                    "canonical 验证归档缺少有效大小",
                    stage="validation_archive_scan",
                )
            if parsed_size <= 0:
                raise ReplenishmentInfrastructureError(
                    "canonical 验证归档大小无效",
                    stage="validation_archive_scan",
                )
            index.setdefault((name, parsed_size), []).append(path)
    return index


def _resolve_validation_archive(
    selection: Mapping[str, Any],
    index: Mapping[tuple[str, int], list[str]],
) -> str | None:
    """Resolve one selected physical archive to one unambiguous cache object."""
    try:
        normalized = normalize_quark_fast_save_selection(selection)
    except QuarkBridgeError as exc:
        raise ReplenishmentCandidateError(
            str(exc), stage="candidate_archive_manifest", candidate=selection,
        ) from exc
    expected = list(normalized["acquisition"]["expected_files"])
    if len(expected) != 1:
        raise ReplenishmentCandidateError(
            "每个 SFX selection 必须精确对应一个归档",
            stage="candidate_archive_manifest", candidate=selection,
        )
    archive = expected[0]
    paths = list(index.get((str(archive["name"]), int(archive["size"])), []))
    if not paths:
        return None
    if len(paths) != 1:
        raise ReplenishmentInfrastructureError(
            "canonical 验证区存在多个同名同尺寸归档，无法唯一复用",
            stage="validation_archive_ambiguous",
        )
    return paths[0]


def _ffprobe_archive_video(path: Path) -> dict[str, Any]:
    ffprobe = shutil.which("ffprobe")
    if ffprobe is None:
        raise ReplenishmentInfrastructureError(
            "运行环境缺少 ffprobe", stage="local_dependency",
        )
    try:
        completed = subprocess.run(
            [ffprobe, "-v", "error", "-show_entries", "stream=codec_type",
             "-show_entries", "format=duration", "-of", "json", str(path)],
            text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            timeout=120, check=False,
        )
    except subprocess.TimeoutExpired as exc:
        raise ReplenishmentCandidateError(
            f"ffprobe 核验超时: {path.name}", stage="candidate_archive_payload",
        ) from exc
    if completed.returncode != 0:
        raise ReplenishmentCandidateError(
            f"ffprobe 无法读取视频: {path.name}", stage="candidate_archive_payload",
        )
    try:
        value = json.loads(completed.stdout)
    except json.JSONDecodeError as exc:
        raise ReplenishmentCandidateError(
            f"ffprobe 输出无效: {path.name}", stage="candidate_archive_payload",
        ) from exc
    streams = value.get("streams") if isinstance(value, Mapping) else None
    if not isinstance(streams, list) or not any(
        isinstance(row, Mapping) and row.get("codec_type") == "video" for row in streams
    ):
        raise ReplenishmentCandidateError(
            f"归档成员不含视频流: {path.name}", stage="candidate_archive_payload",
        )
    return dict(value)


def _extract_quark_sfx(
    client: AListClient, selection: Mapping[str, Any], staging_root: str,
    workspace: Path,
) -> list[dict[str, Any]]:
    """Download and unpack a Quark SFX with 7-Zip; the .exe is never run."""
    seven_zip = shutil.which("7z") or shutil.which("7zz")
    if seven_zip is None:
        raise ReplenishmentInfrastructureError(
            "运行环境缺少 7z/7zz", stage="local_dependency",
        )
    normalized = normalize_quark_fast_save_selection(selection)
    raw_archive_password = normalized["acquisition"].get("archive_password", "")
    if (
        not isinstance(raw_archive_password, str)
        or len(raw_archive_password) > 128
        or any(ord(character) < 32 for character in raw_archive_password)
    ):
        raise ReplenishmentCandidateError(
            "SFX archive 密码元数据无效",
            stage="candidate_archive_password", candidate=selection,
        )
    archive_password = raw_archive_password
    archives = list(normalized["acquisition"]["expected_files"])
    if len(archives) != 1:
        raise ReplenishmentCandidateError(
            "每个 SFX selection 必须精确对应一个归档",
            stage="candidate_archive_manifest", candidate=selection,
        )
    archive = archives[0]
    archive_name = str(archive["name"])
    archive_size = int(archive["size"])
    workspace.mkdir(parents=True, exist_ok=True)
    local_archive = workspace / "archive" / archive_name
    output_root = workspace / "output"
    local_archive.parent.mkdir(parents=True, exist_ok=True)
    output_root.mkdir(parents=True, exist_ok=True)
    if not local_archive.exists() or local_archive.stat().st_size != archive_size:
        try:
            client.download_file_to_path(
                join_remote(staging_root, archive_name), local_archive,
                expected_size=archive_size,
            )
        except Exception as exc:
            raise ReplenishmentDeliveryError(
                str(exc), stage="archive_download",
            ) from exc
    try:
        _listing, members = _local_archive_listing(
            seven_zip, local_archive, archive_password=archive_password,
        )
    except ScraperError as exc:
        raise ReplenishmentCandidateError(
            str(exc), stage="candidate_archive_listing", candidate=selection,
        ) from exc
    budget_archive = {"archive_path": archive_name, "members": members}
    try:
        _validate_local_extraction_budget(
            budget_archive, workspace, compressed_bytes=archive_size,
        )
    except ScraperError as exc:
        if "空间不足" in str(exc):
            raise ReplenishmentInfrastructureError(
                str(exc), stage="local_capacity",
            ) from exc
        raise ReplenishmentCandidateError(
            str(exc), stage="candidate_archive_budget", candidate=selection,
        ) from exc
    by_path = {
        str(row["path"]): row for row in members if not row.get("is_dir")
    }
    selected_members = _quark_archive_members(selection, members)
    for row in selected_members:
        actual = by_path.get(str(row["path"]))
        if actual is None or int(actual.get("size") or -1) != int(row["size"]):
            raise ReplenishmentCandidateError(
                f"归档目录与 gap manifest 不符: {row['gap_id']}",
                stage="candidate_archive_manifest", candidate=selection,
            )
    # Always re-extract into a fresh output tree. This avoids trusting partial
    # files from an interrupted 7-Zip process while retaining the exact archive.
    shutil.rmtree(output_root, ignore_errors=True)
    output_root.mkdir()
    password_input = _seven_zip_password_input(archive_password)
    run_input: dict[str, Any] = (
        {"input": password_input}
        if password_input is not None
        else {"stdin": subprocess.DEVNULL}
    )
    try:
        completed = subprocess.run(
            [seven_zip, "x", "-y", "-bd", "-bso0", "-bsp0",
             f"-o{output_root}", str(local_archive), "--",
             *(str(row["path"]) for row in selected_members)],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT, timeout=7200, check=False,
            **run_input,
        )
    except subprocess.TimeoutExpired as exc:
        raise ReplenishmentCandidateError(
            "SFX archive 解压超时", stage="candidate_archive_extract",
            candidate=selection,
        ) from exc
    if completed.returncode != 0:
        raise ReplenishmentCandidateError(
            "SFX archive 7z 解压失败", stage="candidate_archive_extract",
            candidate=selection,
        )
    actual_files: dict[str, Path] = {}
    for path in output_root.rglob("*"):
        if path.is_symlink():
            raise ReplenishmentCandidateError(
                "SFX archive 解压结果包含符号链接",
                stage="candidate_archive_extract", candidate=selection,
            )
        if path.is_file():
            relative = path.relative_to(output_root).as_posix()
            actual_files[relative] = path
    expected_files = {
        str(row["path"]): int(row["size"])
        for row in selected_members
    }
    if set(actual_files) != set(expected_files) or any(
        actual_files[name].stat().st_size != size for name, size in expected_files.items()
    ):
        raise ReplenishmentCandidateError(
            "SFX archive 解压结果与 7z 安全目录不一致",
            stage="candidate_archive_extract", candidate=selection,
        )
    output: list[dict[str, Any]] = []
    for row in selected_members:
        source = actual_files[str(row["path"])]
        _ffprobe_archive_video(source)
        extension = source.suffix.casefold()
        output.append({
            "gap_ids": [row["gap_id"]], "source": source,
            "remote_name": _safe_name(
                f"{row['gap_id']} - {source.stem}", limit=170,
            ) + extension,
            "size": int(row["size"]),
            "archive_name": archive_name,
            "member_path": str(row["path"]),
            "member_binding": str(row["binding"]),
            "video_verified": True,
        })
    return output


def _acquire_quark_archive_bundle(
    wrapper: Mapping[str, Any], workspace: Path,
) -> dict[str, Any]:
    staging_parent = os.getenv(
        "SCRAPEFLOW_REPLENISHMENT_ARCHIVE_STAGING_ROOT",
        "/quark/.scrapeflow-replenishment-archives",
    )
    request = wrapper.get("request") if isinstance(wrapper.get("request"), Mapping) else {}
    media = request.get("media") if isinstance(request.get("media"), Mapping) else {}
    tmdb_id = media.get("tmdb_id")
    title = _safe_name(str(media.get("title") or f"TMDB-{tmdb_id}"), limit=60)
    final_parent = os.getenv(
        "SCRAPEFLOW_REPLENISHMENT_UNSCRAPED_ROOT",
        "/quark/影视/ScrapeFlow/补源",
    ).rstrip("/")
    final_root = join_remote(
        final_parent,
        _safe_name(f"ScrapeFlow补源-{tmdb_id}-{title}-{_selection_workspace_key(wrapper)}-extracted"),
    )
    client = _alist_client()
    uploaded: list[dict[str, Any]] = []
    selections = [
        child
        for selection in wrapper["selection"]["selections"]
        for child in _split_quark_sfx_selection(selection)
    ]
    validation_index = _index_canonical_validation_archives(client)
    archive_sources: list[dict[str, Any]] = []
    try:
        for index, selection in enumerate(selections, start=1):
            validation_path = _resolve_validation_archive(selection, validation_index)
            if validation_path is not None:
                source_root, archive_name = split_remote(validation_path)
                source_kind = "existing_validation"
                print(
                    f"[replenishment] 复用 canonical 验证归档: {archive_name}",
                    flush=True,
                )
            else:
                child_wrapper = _wrapper_for_selections(wrapper, [selection])
                staged = _acquire_quark_bundle(
                    child_wrapper, workspace / f"fast-save-{index:02d}",
                    remote_parent_override=staging_parent,
                )
                source_root = str(staged["source_paths"][0])
                archive_name = str(
                    normalize_quark_fast_save_selection(selection)["acquisition"]
                    ["expected_files"][0]["name"]
                )
                source_kind = "fast_save_staging"
            archive_sources.append({
                "archive_name": archive_name,
                "source_path": join_remote(source_root, archive_name),
                "source_kind": source_kind,
            })
            uploaded.extend(_extract_quark_sfx(
                client, selection, source_root, workspace / f"archive-{index:02d}",
            ))
    except ReplenishmentCandidateError:
        # A manifest/payload failure invalidates the archive bytes.  Keep
        # delivery/infrastructure workspaces for retry, but never retain a
        # rejected SFX payload in the local cache.
        shutil.rmtree(workspace, ignore_errors=True)
        raise
    except (ReplenishmentDeliveryError, ReplenishmentInfrastructureError):
        raise
    except Exception as exc:
        raise ReplenishmentInfrastructureError(
            str(exc), stage="archive_preprocess",
        ) from exc
    try:
        client.mkdir(final_root)
        transaction_root = _local_upload_transaction_root(workspace)
        for row in uploaded:
            _upload_with_retry(
                client, final_root, row, transaction_root=transaction_root,
            )
        _verify_remote_uploads(client, final_root, uploaded)
    except ReplenishmentDeliveryError:
        raise
    except Exception as exc:
        raise ReplenishmentDeliveryError(
            str(exc), stage="delivery_visibility",
        ) from exc
    verified_files = [{
        "gap_ids": list(row["gap_ids"]),
        "archive_name": row["archive_name"],
        "member_path": row["member_path"],
        "member_binding": row["member_binding"],
        "video_verified": row["video_verified"],
        "remote_path": join_remote(final_root, row["remote_name"]),
        "size": int(row["size"]),
    } for row in uploaded]
    shutil.rmtree(workspace, ignore_errors=True)
    return {
        "status": "ready", "source_paths": [final_root],
        "provider": "quark_share", "materialization": "sfx_extract_upload",
        "saved_files": len(uploaded),
        "saved_bytes": sum(int(row["size"]) for row in uploaded),
        "archive_sources": archive_sources,
        "reused_validation_archives": sum(
            row["source_kind"] == "existing_validation" for row in archive_sources
        ),
        "acquired_archives": sum(
            row["source_kind"] == "fast_save_staging" for row in archive_sources
        ),
        "verified_files": verified_files,
        "local_workspace_cleaned": not workspace.exists(),
    }


def _remote_tree_snapshot(client: AListClient, remote_root: str) -> dict[str, int]:
    # Torrent manifests routinely place the selected payload below containers
    # such as ``EXTRA``/``SP``.  The library-oriented default walk excludes
    # those folders, which would make a fully delivered offline task appear
    # empty forever.  Delivery verification must inspect them; acceptance is
    # still fail-closed below on the exact torrent-relative path and byte size.
    rows = client.walk(
        remote_root,
        refresh=True,
        include_bonus=True,
        include_title_extras=True,
    )
    prefix = remote_root.rstrip("/") + "/"
    snapshot: dict[str, int] = {}
    for row in rows:
        if row.get("is_dir"):
            continue
        full_path = str(row.get("full_path") or row.get("path") or "")
        if full_path.startswith(prefix):
            relative = full_path[len(prefix):]
        else:
            relative = str(row.get("name") or "")
        if relative:
            snapshot[relative.replace("\\", "/")] = int(row.get("size") or 0)
    return snapshot


def _offline_arrived(
    client: AListClient, remote_root: str, expected: list[dict[str, Any]],
) -> bool:
    try:
        snapshot = _remote_tree_snapshot(client, remote_root)
    except ApiError:
        return False
    return all(
        _offline_snapshot_match(
            snapshot, str(row["path"]), int(row["size"]),
        ) is not None
        for row in expected
    )


def _offline_snapshot_match(
    snapshot: Mapping[str, int], expected_path: str, expected_size: int,
) -> str | None:
    """Resolve one exact torrent-relative file under an optional Quark wrapper.

    Quark may preserve the torrent's own root directory and add another
    collision suffix such as ``Release(1)/`` above it.  Requiring the expected
    path at the delivery-root boundary therefore leaves a fully delivered task
    polling forever.  Match the complete torrent-relative suffix plus exact
    size, never a basename; ambiguity remains fail-closed.
    """
    normalized = expected_path.replace("\\", "/").strip("/")
    if not normalized or expected_size <= 0:
        return None
    exact_matches = [
        path for path, size in snapshot.items()
        if size == expected_size
        and (path == normalized or path.endswith("/" + normalized))
    ]
    if len(exact_matches) == 1:
        return exact_matches[0]
    if exact_matches:
        return None

    # Quark replaces the tail of an overlong basename with a literal ``...``
    # while preserving the extension.  The selected torrent index is already
    # bound to an independently verified infoHash/path/size manifest, so accept
    # only Quark's narrow long-name form: a substantial exact UTF-8 prefix,
    # matching parent suffix and extension, exact byte size, and one unique
    # arrival.  Short or ambiguous ellipsized names remain fail-closed.
    expected_parent, expected_name = posixpath.split(normalized)
    expected_stem, expected_extension = posixpath.splitext(expected_name)
    truncated_matches: list[str] = []
    for path, size in snapshot.items():
        if size != expected_size:
            continue
        actual_parent, actual_name = posixpath.split(path)
        actual_stem, actual_extension = posixpath.splitext(actual_name)
        if (
            actual_extension.casefold() != expected_extension.casefold()
            or not actual_stem.endswith("...")
        ):
            continue
        preserved_prefix = actual_stem[:-3]
        if (
            len(preserved_prefix.encode("utf-8")) < 96
            or len(expected_stem) <= len(preserved_prefix)
            or not expected_stem.startswith(preserved_prefix)
            or (
                expected_parent
                and actual_parent != expected_parent
                and not actual_parent.endswith("/" + expected_parent)
            )
        ):
            continue
        truncated_matches.append(path)
    return truncated_matches[0] if len(truncated_matches) == 1 else None


def _offline_canonical_file(row: Mapping[str, Any]) -> dict[str, Any]:
    """Return the exact filename produced by the local Torrent delivery lane."""
    path = str(row.get("path") or "").replace("\\", "/")
    gaps = row.get("gap_ids")
    size = row.get("size")
    if (
        not path or not isinstance(gaps, list) or not gaps
        or not all(isinstance(gap, str) and gap for gap in gaps)
        or type(size) is not int or size <= 0
    ):
        raise ReplenishmentCandidateError(
            "Quark magnet 联合交付 manifest 不完整",
            stage="candidate_manifest",
        )
    source = Path(path)
    extension = source.suffix.casefold()
    if extension not in VIDEO_EXTENSIONS:
        raise ReplenishmentCandidateError(
            "Quark magnet 联合交付文件不是视频",
            stage="candidate_manifest",
        )
    return {
        "name": _safe_name(
            f"{'+'.join(sorted(set(gaps)))} - {source.stem}", limit=170,
        ) + extension,
        "size": size, "gap_ids": sorted(set(gaps)),
    }


def _offline_union_status(
    client: AListClient, legacy_root: str | None, offline_root: str,
    expected: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Split a manifest into exact already-delivered and still-missing rows.

    The legacy root contains canonical names created by local Torrent upload;
    the offline root contains Quark's original torrent-relative paths.  No
    basename-only matching or cross-directory guessing is accepted.
    """
    try:
        offline_snapshot = _remote_tree_snapshot(client, offline_root)
    except ApiError:
        offline_snapshot = {}
    legacy_snapshot: dict[str, int] = {}
    if legacy_root:
        try:
            legacy_snapshot = _remote_tree_snapshot(client, legacy_root)
        except ApiError:
            legacy_snapshot = {}
    satisfied: list[dict[str, Any]] = []
    missing: list[dict[str, Any]] = []
    canonical_names: set[str] = set()
    for row in expected:
        canonical = _offline_canonical_file(row)
        if canonical["name"] in canonical_names:
            raise ReplenishmentCandidateError(
                "Quark magnet 联合交付规范文件名冲突",
                stage="candidate_manifest",
            )
        canonical_names.add(str(canonical["name"]))
        if legacy_root and legacy_snapshot.get(str(canonical["name"])) == int(row["size"]):
            satisfied.append({**row, "delivery_root": legacy_root,
                              "delivery_path": canonical["name"], "reused": True})
        elif (
            offline_path := _offline_snapshot_match(
                offline_snapshot, str(row["path"]), int(row["size"]),
            )
        ) is not None:
            satisfied.append({**row, "delivery_root": offline_root,
                              "delivery_path": offline_path, "reused": True})
        else:
            missing.append(dict(row))
    return satisfied, missing


def _verify_offline_union_arrival(
    client: AListClient, legacy_root: str | None, offline_root: str,
    expected: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    timeout = _bounded_seconds(
        "SCRAPEFLOW_REPLENISHMENT_OFFLINE_TIMEOUT", 21600, 60, 86400,
    )
    deadline = time.monotonic() + timeout
    while True:
        satisfied, missing = _offline_union_status(
            client, legacy_root, offline_root, expected,
        )
        if not missing:
            return satisfied
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise ReplenishmentDeliveryError(
                "Quark magnet 联合交付尚有精确文件未到盘",
                stage="delivery_visibility",
            )
        time.sleep(min(10.0, remaining))


def _offline_legacy_root(
    wrapper: Mapping[str, Any], remote_parent: str, tmdb_id: Any, title: str,
) -> str | None:
    selections = wrapper.get("selection", {}).get("selections") if isinstance(
        wrapper.get("selection"), Mapping,
    ) else None
    if not isinstance(selections, list) or not selections:
        return None
    try:
        fallback = [_offline_local_fallback(row) for row in selections]
    except ReplenishmentInfrastructureError:
        return None
    legacy_key = _selection_workspace_key(_wrapper_for_selections(wrapper, fallback))
    return join_remote(
        remote_parent,
        _safe_name(f"ScrapeFlow补源-{tmdb_id}-{title}-{legacy_key}"),
    )


def _verify_offline_arrival(
    client: AListClient, remote_root: str, expected: list[dict[str, Any]],
) -> None:
    timeout = _bounded_seconds(
        "SCRAPEFLOW_REPLENISHMENT_OFFLINE_TIMEOUT", 21600, 60, 86400,
    )
    deadline = time.monotonic() + timeout
    while True:
        if _offline_arrived(client, remote_root, expected):
            return
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise ReplenishmentDeliveryError(
                "Quark magnet 云端任务已提交但精确文件未到盘",
                stage="delivery_visibility",
            )
        time.sleep(min(10.0, remaining))


def _acquire_quark_magnet_bundle(
    wrapper: Mapping[str, Any], workspace: Path | None = None,
    *, allow_submit: bool = True,
) -> dict[str, Any]:
    request = wrapper.get("request") if isinstance(wrapper.get("request"), Mapping) else {}
    media = request.get("media") if isinstance(request.get("media"), Mapping) else {}
    tmdb_id = media.get("tmdb_id")
    title = _safe_name(str(media.get("title") or f"TMDB-{tmdb_id}"), limit=60)
    remote_parent = os.getenv(
        "SCRAPEFLOW_REPLENISHMENT_UNSCRAPED_ROOT",
        "/quark/影视/ScrapeFlow/补源",
    ).rstrip("/")
    remote_root = join_remote(
        remote_parent,
        _safe_name(f"ScrapeFlow补源-{tmdb_id}-{title}-{_selection_workspace_key(wrapper)}-offline"),
    )
    legacy_root = _offline_legacy_root(
        wrapper, remote_parent, tmdb_id, title,
    )
    client = _alist_client()
    try:
        client.mkdir(remote_root)
    except Exception as exc:
        raise ReplenishmentInfrastructureError(
            str(exc), stage="delivery_prepare",
        ) from exc
    selections = wrapper["selection"]["selections"]
    all_expected: list[dict[str, Any]] = []
    submitted = 0
    submitted_files = 0
    initially_reused_files = 0
    checkpoint_root = workspace or Path(os.getenv(
        "SCRAPEFLOW_REPLENISHMENT_DOWNLOAD_DIR", "/var/tmp/scrapeflow/replenishment",
    )) / "offline-checkpoints" / _selection_workspace_key(wrapper)
    checkpoint_root.mkdir(parents=True, exist_ok=True)
    for selection_index, selection in enumerate(selections, start=1):
        try:
            plan = QuarkMagnetOfflineBridge.dry_run(selection, remote_root)
        except QuarkBridgeError as exc:
            raise ReplenishmentCandidateError(
                str(exc), stage="candidate_manifest", candidate=selection,
            ) from exc
        expected = list(plan["expected_files"])
        all_expected.extend(expected)
        satisfied, missing = _offline_union_status(
            client, legacy_root, remote_root, expected,
        )
        initially_reused_files += len(satisfied)
        if not missing:
            print(
                f"[replenishment] 复用已到盘联合交付: {selection.get('release_name')}",
                flush=True,
            )
            continue
        if not allow_submit:
            raise QuarkMagnetInDoubtError(
                "Quark magnet 历史 submit 状态未定；联合交付未齐，禁止重提与本地接管"
            )
        checkpoint_path = checkpoint_root / f"task-{selection_index:02d}.json"
        resume_task_id = None
        submit_expected = list(missing)
        if checkpoint_path.exists():
            try:
                checkpoint = _load(checkpoint_path)
                if (
                    checkpoint.get("destination") == remote_root
                    and checkpoint.get("locator") == selection.get("locator")
                    and checkpoint.get("infohash") == plan["infohash"]
                    and isinstance(checkpoint.get("task_id"), str)
                ):
                    resume_task_id = str(checkpoint["task_id"])
                    checkpoint_expected = checkpoint.get("expected_files")
                    if (
                        isinstance(checkpoint_expected, list)
                        and checkpoint_expected
                        and all(isinstance(row, Mapping) and dict(row) in expected
                                for row in checkpoint_expected)
                    ):
                        # Resume the exact original subset.  Some members may
                        # have become visible already, but progress polling must
                        # stay bound to the task that was actually submitted.
                        submit_expected = [dict(row) for row in checkpoint_expected]
            except (OSError, ValueError, json.JSONDecodeError):
                resume_task_id = None
        submit_gaps = sorted({
            str(gap) for row in submit_expected
            for gap in row.get("gap_ids") or []
        })
        submit_selection = dict(selection)
        submit_acquisition = dict(selection.get("acquisition") or {})
        submit_acquisition["expected_files"] = submit_expected
        submit_selection["acquisition"] = submit_acquisition
        submit_selection["selected_gap_ids"] = submit_gaps

        def persist_task(task_id: str) -> None:
            _atomic_json(checkpoint_path, {
                "version": 2, "task_id": task_id, "destination": remote_root,
                "locator": selection.get("locator"), "infohash": plan["infohash"],
                "selected_gap_ids": submit_gaps,
                "expected_files": submit_expected,
                "submitted_at": time.time(),
            })
        try:
            receipt = _quark_magnet_offline_port(
                submit_selection, remote_root, resume_task_id=resume_task_id,
                on_submitted=persist_task,
            )
        except QuarkBridgeError as exc:
            if getattr(exc, "failure_scope", None) == "candidate":
                exc.candidate = {
                    key: selection.get(key)
                    # Offline-provider rejection is lane-specific. Preserve the
                    # locator but not the shared BTIH so the next round can
                    # still try the same bytes through the local torrent lane.
                    for key in ("provider", "release_name", "locator")
                    if selection.get(key) is not None
                }
            raise
        if (
            receipt.get("status") not in {"submitted", "ready"}
            or receipt.get("destination") != remote_root
            or receipt.get("expected_files") != submit_expected
        ):
            raise ReplenishmentInfrastructureError(
                "Quark magnet receipt 与提交计划不符", stage="artifact_validation",
            )
        submitted += 1
        if resume_task_id is None:
            submitted_files += len(submit_expected)
    delivered = _verify_offline_union_arrival(
        client, legacy_root, remote_root, all_expected,
    )
    source_paths = list(dict.fromkeys(
        str(row["delivery_root"]) for row in delivered
    ))
    return {
        "status": "ready", "source_paths": source_paths,
        "provider": "quark_magnet", "materialization": "cloud_offline",
        "saved_files": len(all_expected),
        "saved_bytes": sum(int(row["size"]) for row in all_expected),
        "reused_files": initially_reused_files,
        "submitted_files": submitted_files,
        "submitted_tasks": submitted,
        "delivery_model": "verified_union",
    }


def _offline_local_fallback(selection: Mapping[str, Any]) -> dict[str, Any]:
    acquisition = selection.get("acquisition")
    fallback = acquisition.get("local_fallback") if isinstance(acquisition, Mapping) else None
    locator = selection.get("fallback_locator")
    if (
        not isinstance(fallback, Mapping) or fallback.get("kind") != "torrent"
        or not isinstance(locator, str) or not locator
    ):
        raise ReplenishmentInfrastructureError(
            "Quark magnet lane 缺少可验证的本地 torrent fallback",
            stage="artifact_validation",
        )
    output = dict(selection)
    output.update({
        "provider": "magnet", "locator": locator,
        "acquisition": dict(fallback), "fallback_from": "quark_magnet",
    })
    return output


def _acquire_quark_magnet_with_recovery(
    wrapper: Mapping[str, Any], workspace: Path,
) -> dict[str, Any]:
    """Retry the cloud lane in place before returning failure to the coordinator.

    The task checkpoint written immediately after submit makes progress retries
    idempotent.  An indeterminate submit is different: the mutation may already
    exist but its task id was not returned, so another acquisition could create
    duplicate multi-gigabyte work.  Every exhausted cloud failure is returned to
    the coordinator; its exact locator cooldown lets the next selection round
    continue along the provider chain instead of descending locally in-process.
    """
    attempts = _bounded_seconds(
        "SCRAPEFLOW_REPLENISHMENT_OFFLINE_RECOVERY_ATTEMPTS", 3, 1, 12,
    )
    delay = _bounded_seconds(
        "SCRAPEFLOW_REPLENISHMENT_OFFLINE_RECOVERY_DELAY", 5, 1, 300,
    )
    state_path = workspace / "recovery-state.json"
    submit_in_doubt = False
    if state_path.exists():
        try:
            submit_in_doubt = _load(state_path).get("status") == "submit_in_doubt"
        except (OSError, ValueError, json.JSONDecodeError):
            submit_in_doubt = False
    def persist_state(value: Mapping[str, Any]) -> None:
        try:
            _atomic_json(state_path, value)
        except OSError as exc:
            # Acquisition errors remain authoritative if an externally mocked
            # or read-only workspace cannot record observability metadata.
            print(
                f"[replenishment] 恢复状态无法写入: {type(exc).__name__}",
                flush=True,
            )
    last_error: BaseException | None = None
    for attempt in range(1, attempts + 1):
        try:
            result = _acquire_quark_magnet_bundle(
                wrapper, workspace, allow_submit=not submit_in_doubt,
            )
            persist_state({
                "version": 1, "status": "ready", "attempt": attempt,
                "updated_at": time.time(),
            })
            return result
        except QuarkMagnetInDoubtError as exc:
            persist_state({
                "version": 1, "status": "submit_in_doubt", "attempt": attempt,
                "failure_scope": exc.failure_scope,
                "failure_stage": exc.failure_stage, "updated_at": time.time(),
            })
            raise
        except QuarkBridgeError as exc:
            last_error = exc
            if getattr(exc, "failure_scope", None) == "candidate":
                persist_state({
                    "version": 1, "status": "candidate_rejected", "attempt": attempt,
                    "failure_scope": "candidate",
                    "failure_stage": getattr(exc, "failure_stage", "quark_magnet_candidate"),
                    "updated_at": time.time(),
                })
                raise
        except (ReplenishmentInfrastructureError, ReplenishmentDeliveryError) as exc:
            last_error = exc
        assert last_error is not None
        persist_state({
            "version": 1,
            "status": "retry_wait" if attempt < attempts else "cooldown_eligible",
            "attempt": attempt, "max_attempts": attempts,
            "failure_scope": str(getattr(last_error, "failure_scope", "infrastructure")),
            "failure_stage": str(getattr(last_error, "failure_stage", "unclassified")),
            "updated_at": time.time(),
        })
        if attempt < attempts:
            print(
                f"[replenishment] Quark magnet 可恢复失败，云端原地重试 "
                f"{attempt}/{attempts}", flush=True,
            )
            time.sleep(delay)
    assert last_error is not None
    raise last_error


def _acquire_dispatch(wrapper: Mapping[str, Any], workspace: Path) -> dict[str, Any]:
    bundle = wrapper.get("selection")
    rows = bundle.get("selections") if isinstance(bundle, Mapping) else None
    if not isinstance(rows, list) or not rows or not all(isinstance(row, dict) for row in rows):
        raise ReplenishmentInfrastructureError("选择文件缺少 selections", stage="artifact_validation")
    try:
        for row in rows:
            acquisition_lane(row)
    except AcquisitionRouteError as exc:
        raise ReplenishmentInfrastructureError(
            str(exc), stage="artifact_validation",
        ) from exc
    shares = [dict(row) for row in rows if _selection_acquisition_kind(row) == "quark_fast_save"]
    archives = [dict(row) for row in rows if _selection_acquisition_kind(row) == "quark_sfx_archive"]
    offline = [dict(row) for row in rows if _selection_acquisition_kind(row) == "quark_magnet_offline"]
    torrents = [dict(row) for row in rows if _selection_acquisition_kind(row) == "torrent"]
    if len(shares) + len(archives) + len(offline) + len(torrents) != len(rows):
        raise ReplenishmentInfrastructureError("选择包含未知 acquisition kind", stage="artifact_validation")
    # Keep the historical single-lane receipt intact.  Only a genuinely mixed
    # bundle needs the aggregate receipt below; callers may rely on the richer
    # torrent/quark lane-specific counters during retries.
    if torrents and not shares and not archives and not offline:
        return _acquire(_wrapper_for_selections(wrapper, torrents), workspace)
    results: list[dict[str, Any]] = []
    if shares:
        results.append(_acquire_quark_bundle(
            _wrapper_for_selections(wrapper, shares), workspace / "share",
        ))
    if archives:
        results.append(_acquire_quark_archive_bundle(
            _wrapper_for_selections(wrapper, archives), workspace / "sfx",
        ))
    if offline:
        try:
            results.append(_acquire_quark_magnet_with_recovery(
                _wrapper_for_selections(wrapper, offline), workspace / "offline",
            ))
        except Exception as exc:
            # Preserve the original failure contract, but identify the lane so
            # the acquire artifact can cool down only the exact qmag locators.
            # The coordinator will re-run the selector, which can then choose a
            # different quark_magnet candidate before any local Torrent lane.
            exc.failure_lane = "quark_magnet"
            raise
    torrent_lane = list(torrents)
    if torrent_lane:
        torrent_result = _acquire(
            _wrapper_for_selections(wrapper, torrent_lane),
            workspace if not shares and not archives and not offline else workspace / "torrent",
        )
        results.append(torrent_result)
    sources = [source for result in results for source in result.get("source_paths") or []]
    archive_sources = [
        dict(row)
        for result in results
        for row in result.get("archive_sources") or []
        if isinstance(row, Mapping)
    ]
    return {
        "status": "ready", "source_paths": sources,
        "materializations": [result.get("materialization", "torrent_upload") for result in results],
        "materialized_files": sum(int(result.get("saved_files") or result.get("uploaded_files") or 0) for result in results),
        "materialized_bytes": sum(int(result.get("saved_bytes") or result.get("uploaded_bytes") or 0) for result in results),
        "archive_sources": archive_sources,
        "reused_validation_archives": sum(
            int(result.get("reused_validation_archives") or 0) for result in results
        ),
        "acquired_archives": sum(
            int(result.get("acquired_archives") or 0) for result in results
        ),
        "fallback_from": None,
        "lane_suppressions": [],
    }


def _failure_lane_suppressions(
    wrapper: Mapping[str, Any], exc: BaseException,
) -> list[dict[str, Any]]:
    """Release a higher-priority lane only after a safe infrastructure failure.

    Candidate failures already enter the durable exclusion ledger. Delivery
    failures may have crossed a cloud mutation boundary, so they must remain
    pinned to their original lane. A pre-delivery cloud infrastructure failure
    is safe to cool down. The cooldown is scoped to the exact locator: after a
    qmag failure the next unattended round can try another qmag candidate, and
    only the selector may descend to local Torrent once no cloud candidate is
    still eligible.
    """
    if getattr(exc, "failure_scope", "infrastructure") != "infrastructure":
        return []
    bundle = wrapper.get("selection")
    selections = bundle.get("selections") if isinstance(bundle, Mapping) else None
    if not isinstance(selections, list):
        return []
    cloud_providers = {
        str(row.get("provider")) for row in selections
        if isinstance(row, Mapping)
        and row.get("provider") in {"quark_share", "quark_magnet"}
    }
    failure_lane = str(getattr(exc, "failure_lane", "") or "")
    if not failure_lane:
        stage = str(getattr(exc, "failure_stage", "") or "")
        if stage.startswith("quark_magnet"):
            failure_lane = "quark_magnet"
        elif stage.startswith(("quark_share", "quark_fast_save")):
            failure_lane = "quark_share"
        elif len(cloud_providers) == 1:
            failure_lane = next(iter(cloud_providers))
    cooldowns = {
        "quark_share": (
            "SCRAPEFLOW_REPLENISHMENT_SHARE_COOLDOWN", 900,
            "quark_share_infrastructure_failure",
        ),
        "quark_magnet": (
            "SCRAPEFLOW_REPLENISHMENT_OFFLINE_COOLDOWN", 3600,
            "quark_magnet_infrastructure_failure",
        ),
    }
    if failure_lane not in cooldowns:
        return []
    env_name, default, reason = cooldowns[failure_lane]
    until = time.time() + _bounded_seconds(env_name, default, 60, 86400)
    output: list[dict[str, Any]] = []
    for row in selections:
        if not isinstance(row, Mapping) or row.get("provider") != failure_lane:
            continue
        locator = row.get("locator")
        if isinstance(locator, str) and locator:
            output.append({
                "provider": failure_lane, "locator": locator,
                "until_epoch": until,
                "reason": reason,
            })
    return output


def _safe_name(value: str, *, limit: int = 180) -> str:
    value = re.sub(r"[\\/:*?\"<>|\x00-\x1f]+", " ", value)
    value = re.sub(r"\s+", " ", value).strip(" .")
    return (value or "补源文件")[:limit].rstrip(" .")


def _verify_remote_uploads(
    client: AListClient, remote_root: str, uploaded: list[dict[str, Any]],
) -> None:
    """Wait for cloud-backed AList listings to expose committed uploads.

    AList's upload endpoint can return before a provider refresh exposes the
    new row and its final size.  Treating that short visibility window as a
    failed torrent discards a fully downloaded candidate and poisons the
    durable exclusion ledger, so poll the refreshed directory for a bounded
    period before declaring the acquisition failed.
    """
    timeout = _bounded_seconds(
        "SCRAPEFLOW_REPLENISHMENT_ARRIVAL_TIMEOUT", 120, 10, 600,
    )
    deadline = time.monotonic() + timeout
    last_files: dict[str, int] = {}
    while True:
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


def _provider_remux_path(source: Path, remote_name: str) -> Path:
    """Keep a deterministic stream-copy beside, but outside, fresh extraction output."""
    return source.parent.parent / "provider-remux" / remote_name


def _local_upload_transaction_root(workspace: Path) -> Path:
    """Keep small receipts after successful workspace cleanup."""
    return workspace.parent / ".upload-transactions" / workspace.name


def _upload_with_retry(
    client: AListClient,
    remote_root: str,
    row: dict[str, Any],
    *,
    transaction_root: Path,
) -> None:
    """Run one durable upload request and reconcile without ever resending it."""
    remote_name = str(row["remote_name"])
    source = Path(row["source"]).resolve()
    size = int(row["size"])
    persisted_remux = _provider_remux_path(source, remote_name)
    if persisted_remux.is_file() and persisted_remux.stat().st_size > 0:
        try:
            _ffprobe_archive_video(persisted_remux)
        except ReplenishmentCandidateError:
            persisted_remux.unlink(missing_ok=True)
        else:
            source = persisted_remux
            size = persisted_remux.stat().st_size
            row["source"] = source
            row["size"] = size
    target = join_remote(remote_root, remote_name)
    transaction_id = deterministic_local_upload_id(source, target)
    spec = LocalUploadSpec(
        transaction_id=transaction_id,
        source_path=source,
        target_path=target,
        expected_size=size,
        content_type="video/x-matroska",
    )
    try:
        result = run_local_upload_transaction(
            AListExactFileAdapter(client),
            transaction_root=transaction_root,
            spec=spec,
        )
    except LocalUploadTransactionError as exc:
        raise ReplenishmentDeliveryError(
            f"AList 单次上传结果无法安全证明，已保留完整本机源和事务: {exc}",
            stage="delivery_upload",
        ) from exc
    row["source"] = source
    row["size"] = result.size
    row["sha256"] = result.sha256
    row["upload_transaction_id"] = result.transaction_id
    row["upload_receipt_sha256"] = result.receipt_sha256
    if result.upload_calls_recorded == 0:
        print(f"[replenishment] 复用已验证到盘文件: {remote_name}", flush=True)


def _find_download(payload: Path, relative_path: str, size: int) -> Path:
    suffix = relative_path.replace("\\", "/")
    basename = Path(relative_path).name
    candidates = [
        path for path in payload.rglob("*")
        if path.is_file() and path.name == basename
        and path.as_posix().endswith(suffix) and path.stat().st_size == size
    ]
    if len(candidates) != 1:
        raise ValueError(f"下载文件定位结果异常: {relative_path}; matches={len(candidates)}")
    return candidates[0]


def _acquire(selection_wrapper: Mapping[str, Any], workspace: Path) -> dict[str, Any]:
    request = selection_wrapper.get("request") if isinstance(selection_wrapper.get("request"), Mapping) else {}
    media = request.get("media") if isinstance(request.get("media"), Mapping) else {}
    tmdb_id = media.get("tmdb_id")
    title = _safe_name(str(media.get("title") or f"TMDB-{tmdb_id}"), limit=60)
    remote_parent = os.getenv(
        "SCRAPEFLOW_REPLENISHMENT_UNSCRAPED_ROOT",
        "/quark/影视/ScrapeFlow/补源",
    ).rstrip("/")
    workspace_key = _selection_workspace_key(selection_wrapper)
    remote_name = _safe_name(f"ScrapeFlow补源-{tmdb_id}-{title}-{workspace_key}")
    remote_root = join_remote(remote_parent, remote_name)
    uploaded: list[dict[str, Any]] = []
    client: AListClient | None = None
    payload_verified = False
    delivery_stage = "candidate_preflight"
    try:
        preflight = _preflight(
            selection_wrapper, workspace / "preflight", resume_workspace=workspace,
        )
        bundle = selection_wrapper["selection"]
        for offset, selection in enumerate(bundle["selections"], start=1):
            candidate_dir = workspace / f"download-{offset:02d}"
            payload_dir = candidate_dir / "payload"
            payload_dir.mkdir(parents=True, exist_ok=True)
            verified = preflight["candidates"][offset - 1]
            manifest = verified["manifest"]
            torrent_path = Path(verified["torrent_path"])
            try:
                indices, by_index = _verify_manifest(selection, manifest)
            except ValueError as exc:
                raise ReplenishmentCandidateError(
                    str(exc), stage="candidate_manifest", candidate=selection,
                ) from exc
            acquisition = selection["acquisition"]
            if _payload_is_complete(payload_dir, acquisition, indices):
                print(
                    f"[replenishment] 复用已验证下载 {offset}/{len(bundle['selections'])}: "
                    f"{selection.get('release_name')}",
                    flush=True,
                )
            else:
                command = [
                    "aria2c", "--seed-time=0", "--file-allocation=none",
                    "--allow-overwrite=true", "--auto-file-renaming=false",
                    "--summary-interval=60", "--console-log-level=notice",
                    f"--bt-stop-timeout={_bounded_seconds('SCRAPEFLOW_REPLENISHMENT_BT_IDLE_TIMEOUT', 600, 60, 3600)}",
                    f"--dir={payload_dir}", f"--select-file={','.join(str(i) for i in sorted(indices))}",
                    str(torrent_path),
                ]
                print(f"[replenishment] 下载候选 {offset}/{len(bundle['selections'])}: {selection.get('release_name')}", flush=True)
                try:
                    completed = subprocess.run(
                        command, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                        timeout=_bounded_seconds(
                            "SCRAPEFLOW_REPLENISHMENT_TORRENT_TIMEOUT", 21600, 300, 86400,
                        ),
                    )
                except subprocess.TimeoutExpired as exc:
                    raise ReplenishmentCandidateError(
                        "aria2c 下载超过总时限", stage="candidate_download",
                        candidate=selection,
                    ) from exc
                if completed.returncode != 0:
                    tail = " ".join(completed.stdout.splitlines()[-8:])[:1200]
                    raise ReplenishmentCandidateError(
                        f"aria2c 下载失败: {tail}", stage="candidate_download",
                        candidate=selection,
                    )
            size_map = acquisition["file_size_by_index"]
            path_map = acquisition["file_path_by_index"]
            for index in sorted(indices):
                expected_size = int(size_map[str(index)])
                relative_path = str(path_map[str(index)])
                try:
                    source = _find_download(payload_dir, relative_path, expected_size)
                except ValueError as exc:
                    raise ReplenishmentCandidateError(
                        str(exc), stage="candidate_payload_validation", candidate=selection,
                    ) from exc
                gaps = sorted(set(by_index[index]))
                gap_prefix = "+".join(gaps)
                extension = source.suffix.casefold()
                if extension not in VIDEO_EXTENSIONS:
                    raise ReplenishmentCandidateError(
                        f"选中文件不是支持的视频格式: {source.name}",
                        stage="candidate_payload_validation",
                        candidate=selection,
                    )
                remote_file = _safe_name(f"{gap_prefix} - {source.stem}", limit=170) + extension
                uploaded.append({
                    "gap_ids": gaps, "source": source, "remote_name": remote_file,
                    "size": expected_size,
                })
        if not uploaded:
            raise ReplenishmentCandidateError(
                "没有可上传的补源视频",
                stage="candidate_payload_validation",
            )
        payload_verified = True
        delivery_stage = "delivery_connect"
        client = _alist_client()
        delivery_stage = "delivery_prepare"
        client.mkdir(remote_root)
        delivery_stage = "delivery_upload"
        transaction_root = _local_upload_transaction_root(workspace)
        for offset, row in enumerate(uploaded, start=1):
            print(f"[replenishment] 上传 {offset}/{len(uploaded)}: {row['remote_name']}", flush=True)
            _upload_with_retry(
                client, remote_root, row, transaction_root=transaction_root,
            )
        delivery_stage = "delivery_visibility"
        _verify_remote_uploads(client, remote_root, uploaded)
        shutil.rmtree(workspace)
        return {
            "status": "ready",
            "source_paths": [remote_root],
            "uploaded_files": len(uploaded),
            "uploaded_bytes": sum(int(row["size"]) for row in uploaded),
        }
    except BaseException as exc:
        delivery_failure = payload_verified and isinstance(exc, Exception)
        # Never recursively remove the deterministic remote delivery root on
        # an ambiguous failure.  It may already contain a verified object from
        # this or an earlier attempt, and a bare remove has no recoverable
        # transaction or ownership proof.  A later retry inspects the exact
        # objects in place; explicit cleanup must use the title-scoped hybrid
        # rollback transaction after strict acceptance.
        if delivery_failure:
            # Keep both the verified local payload and any exact-size remote
            # objects.  The deterministic workspace/remote names make the next
            # retry skip the torrent and already committed uploads.
            if isinstance(exc, ReplenishmentDeliveryError):
                raise
            raise ReplenishmentDeliveryError(str(exc), stage=delivery_stage) from exc
        # Capacity/dependency/orchestration failures do not invalidate bytes
        # retained by a previous attempt. Candidate failures do.
        if not isinstance(exc, ReplenishmentInfrastructureError):
            shutil.rmtree(workspace, ignore_errors=True)
        raise


def _bounded_seconds(name: str, default: int, minimum: int, maximum: int) -> int:
    raw = os.getenv(name, str(default)).strip()
    if not raw.isascii() or not raw.isdecimal():
        raise ValueError(f"{name} 需要是整数秒")
    value = int(raw)
    if not minimum <= value <= maximum:
        raise ValueError(f"{name} 需要在 {minimum}–{maximum} 秒之间")
    return value


def main() -> int:
    parser = argparse.ArgumentParser(description="ScrapeFlow 本地补源适配器")
    subparsers = parser.add_subparsers(dest="action", required=True)
    search = subparsers.add_parser("search")
    search.add_argument("--request", type=Path, required=True)
    search.add_argument("--output", type=Path, required=True)
    preflight = subparsers.add_parser("preflight")
    preflight.add_argument("--selection", type=Path, required=True)
    preflight.add_argument("--output", type=Path, required=True)
    acquire = subparsers.add_parser("acquire")
    acquire.add_argument("--selection", type=Path, required=True)
    acquire.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()

    if args.action == "search":
        root = Path(os.getenv(
            "SCRAPEFLOW_REPLENISHMENT_DOWNLOAD_DIR",
            "/var/tmp/scrapeflow/replenishment",
        ))
        with _search_capacity_lease(root):
            result = _search(_load(args.request))
    else:
        wrapper = _load(args.selection)
        root = Path(os.getenv("SCRAPEFLOW_REPLENISHMENT_DOWNLOAD_DIR", "/var/tmp/scrapeflow/replenishment"))
        workspace = (
            root / f"preflight-{uuid.uuid4().hex}"
            if args.action == "preflight"
            else root / f"acquire-{_selection_workspace_key(wrapper)}"
        )
        try:
            if args.action == "preflight":
                result = _preflight_dispatch(wrapper, workspace)
            else:
                with _workspace_lease(root, _selection_workspace_key(wrapper)):
                    result = _acquire_dispatch(wrapper, workspace)
        except Exception as exc:
            if args.action == "acquire":
                _atomic_json(args.output, {
                    "status": "failed",
                    "lane_suppressions": _failure_lane_suppressions(wrapper, exc),
                    "failure": {
                        "scope": str(getattr(exc, "failure_scope", "infrastructure")),
                        "stage": str(getattr(exc, "failure_stage", "unclassified")),
                        "reusable_candidate": bool(
                            getattr(exc, "reusable_candidate", False)
                        ),
                        "exclude_candidate": bool(
                            getattr(exc, "exclude_candidate", False)
                        ),
                        "candidate": dict(getattr(exc, "candidate", {}) or {}),
                        "workspace": str(workspace),
                        "retained_workspace": workspace.exists(),
                        "workspace_key": _selection_workspace_key(wrapper),
                        "message": str(exc),
                    },
                })
            raise
    _atomic_json(args.output, result)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except ReplenishmentDeliveryError as exc:
        print("[replenishment] failure_scope=delivery reusable_candidate=true exclude_candidate=false", flush=True)
        print(f"补源适配器失败: {exc}", flush=True)
        raise SystemExit(1)
    except ReplenishmentCandidateError as exc:
        print("[replenishment] failure_scope=candidate reusable_candidate=false exclude_candidate=true", flush=True)
        print(f"补源适配器失败: {exc}", flush=True)
        raise SystemExit(1)
    except QuarkBridgeError as exc:
        scope = str(getattr(exc, "failure_scope", "infrastructure"))
        reusable = str(bool(getattr(exc, "reusable_candidate", False))).lower()
        excluded = str(bool(getattr(exc, "exclude_candidate", False))).lower()
        print(
            f"[replenishment] failure_scope={scope} "
            f"reusable_candidate={reusable} exclude_candidate={excluded}",
            flush=True,
        )
        print(f"补源适配器失败: {exc}", flush=True)
        raise SystemExit(1)
    except (OSError, ValueError, RuntimeError, ApiError, json.JSONDecodeError) as exc:
        print("[replenishment] failure_scope=infrastructure reusable_candidate=false exclude_candidate=false", flush=True)
        print(f"补源适配器失败: {exc}", flush=True)
        raise SystemExit(1)
