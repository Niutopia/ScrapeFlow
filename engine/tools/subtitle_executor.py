#!/usr/bin/env python3
"""Plan and safely execute external Simplified-Chinese subtitle actions.

Planning is read-only.  Execution is separately gated by the exact selection
digest and can only create a new ``.zh-CN.ass``/``.zh-CN.srt`` companion; it
never moves, replaces, remuxes, or deletes a video.
"""

from __future__ import annotations

import argparse
import base64
from collections import Counter, defaultdict
from contextlib import nullcontext
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import shutil
import subprocess
import tempfile
from typing import Any, Callable, ContextManager, Iterable, Mapping, Sequence

from engine.scraper import AListClient
from engine.scrapeflow.alist_exact_file_adapter import AListExactFileAdapter
from engine.scrapeflow.local_upload_transaction import (
    LocalUploadConflict,
    LocalUploadResult,
    LocalUploadSpec,
    deterministic_local_upload_id,
    run_local_upload_transaction,
)
from engine.tools.audit_live_library import companion_stem, episode_numbers
from engine.tools.refine_subtitle_audit import classify_subtitle_content
from engine.scrapeflow.subtitle_content_witness import (
    resolve_ambiguous_by_embedded_witness,
)
from engine.scrapeflow.serialization import atomic_write_json


FORMAL_ROOTS = ("/quark/影视/电影", "/quark/影视/番剧", "/quark/影视/美剧")
SEARCH_ROOTS = FORMAL_ROOTS + (
    "/quark/影视/待刮削",
    "/quark/影视/ScrapeFlow/备份",
    "/quark/影视/ScrapeFlow/验证",
)
TEXT_EXTS = {".ass", ".srt"}
VIDEO_EXTS = {".mkv", ".mp4", ".m4v", ".m2ts", ".ts", ".avi", ".mov", ".webm"}
MAX_SUBTITLE_BYTES = 16 * 1024 * 1024
TEXT_STREAM_CODECS = {"ass", "ssa", "subrip", "srt", "webvtt", "mov_text", "text"}
AMBIGUITY_SAMPLE_SECONDS = 30.0
AMBIGUITY_PROBE_TIMEOUT = 30
AMBIGUITY_SAMPLE_TIMEOUT = 45
MUTATING_SUBTITLE_LANE = "ensure_external_zh_CN"
VERIFICATION_SUBTITLE_LANE = "subtitle_verification"
CREATE_SIDECAR_SCOPE = "create_external_subtitle_only"
PROBE_ONLY_SCOPE = "probe_only_no_mutation"


def canonical_digest(value: Any) -> str:
    encoded = json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _title_key(value: str) -> str:
    return "".join(re.findall(r"[0-9a-z\u3400-\u9fff]+", value.casefold()))


def _inside(path: str, root: str) -> bool:
    return path == root or path.startswith(root.rstrip("/") + "/")


def _safe_target(video_path: str, extension: str) -> str:
    if extension not in TEXT_EXTS:
        raise ValueError("目标字幕必须是 .ass 或 .srt")
    if not any(_inside(video_path, root) for root in FORMAL_ROOTS):
        raise ValueError("字幕目标不在正式媒体库")
    if PurePosixPath(video_path).suffix.casefold() not in VIDEO_EXTS:
        raise ValueError("字幕请求目标不是视频")
    return str(PurePosixPath(video_path).with_suffix("")) + ".zh-CN" + extension


def build_requests(refined: Mapping[str, Any]) -> dict[str, Any]:
    """Deduplicate episode-range rows into one immutable request per video."""
    grouped: dict[str, dict[str, Any]] = {}
    for source_category, lane in (
        ("confirmed_missing_chinese", "ensure_external_zh_CN"),
        ("pending_review_or_probe", "subtitle_verification"),
    ):
        for row in refined.get(source_category, []) or []:
            if not isinstance(row, Mapping):
                continue
            video_path = str(row.get("video_path") or "")
            if not video_path or not any(_inside(video_path, root) for root in FORMAL_ROOTS):
                continue
            current = grouped.setdefault(video_path, {
                "video_path": video_path,
                "media_type": str(row.get("media_type") or "tv"),
                "title": str(row.get("title") or PurePosixPath(str(row.get("target_root") or "")).name),
                "target_root": str(row.get("target_root") or ""),
                "labels": [],
                "aliases": [],
                "title_aliases": [],
                "source_episode_aliases": [],
                "lanes": [],
                "source_reasons": [],
            })
            raw_aliases = row.get("aliases")
            if isinstance(raw_aliases, list):
                current["aliases"].extend(
                    str(value).strip() for value in raw_aliases
                    if isinstance(value, str) and value.strip()
                )
            raw_title_aliases = row.get("title_aliases")
            if isinstance(raw_title_aliases, list):
                current["title_aliases"].extend(
                    str(value).strip() for value in raw_title_aliases
                    if isinstance(value, str) and value.strip()
                )
            raw_source_aliases = row.get("source_episode_aliases")
            if isinstance(raw_source_aliases, list):
                current["source_episode_aliases"].extend(
                    dict(value) for value in raw_source_aliases
                    if isinstance(value, Mapping)
                )
            label = str(row.get("label") or "").strip()
            if label:
                current["labels"].append(label)
            current["lanes"].append(lane)
            current["source_reasons"].append(str(row.get("reason_code") or row.get("pending_reason") or ""))
    requests = []
    for video_path, row in sorted(grouped.items()):
        lanes = sorted(set(row.pop("lanes")))
        lane = (
            MUTATING_SUBTITLE_LANE
            if MUTATING_SUBTITLE_LANE in lanes
            else VERIFICATION_SUBTITLE_LANE
        )
        identity = episode_numbers(video_path)
        request = {
            **row,
            "labels": sorted(set(row["labels"])),
            "lane": lane,
            "source_reasons": sorted(set(filter(None, row["source_reasons"]))),
            "required_language": "zh-CN",
            "allowed_target_extensions": [".ass", ".srt"],
            "mutation_scope": (
                CREATE_SIDECAR_SCOPE
                if lane == MUTATING_SUBTITLE_LANE
                else PROBE_ONLY_SCOPE
            ),
        }
        aliases = list(dict.fromkeys(request.pop("aliases", [])))[:16]
        if aliases:
            request["aliases"] = aliases
        title_aliases = list(dict.fromkeys(request.pop("title_aliases", [])))[:4]
        if title_aliases:
            request["title_aliases"] = title_aliases
        source_aliases = {
            canonical_digest(value): value
            for value in request.pop("source_episode_aliases", [])
        }
        if source_aliases:
            request["source_episode_aliases"] = [
                source_aliases[key] for key in sorted(source_aliases)
            ][:4]
        if identity is not None:
            season, episodes = identity
            request["season"] = season
            request["episodes"] = sorted(episodes)
        request["request_id"] = canonical_digest({
            "video_path": video_path,
            "lane": lane,
            "required_language": "zh-CN",
        })[:24]
        requests.append(request)
    body = {"schema_version": 1, "kind": "subtitle_requests", "requests": requests}
    return {**body, "request_sha256": canonical_digest(body)}


def _candidate_origin(path: str, roots: Sequence[str]) -> str:
    return next((root for root in roots if _inside(path, root)), "artifact")


def inventory_candidates(rows: Iterable[Mapping[str, Any]], roots: Sequence[str]) -> list[dict[str, Any]]:
    output = []
    for row in rows:
        path = str(row.get("full_path") or row.get("path") or "")
        extension = PurePosixPath(path).suffix.casefold()
        if not path or extension not in TEXT_EXTS:
            continue
        output.append({
            "candidate_id": canonical_digest({"path": path, "source_kind": "alist"})[:24],
            "path": path,
            "extension": extension,
            "source_kind": "alist",
            "origin_root": _candidate_origin(path, roots),
            "retrieval_mode": "read_existing_subtitle_only",
        })
    return sorted(output, key=lambda row: row["path"])


def _artifact_paths(value: Any, *, context: Mapping[str, Any] | None = None) -> Iterable[dict[str, Any]]:
    if isinstance(value, Mapping):
        current = dict(context or {})
        for key in ("video_path", "paired_video_path", "target_path", "title", "label"):
            if isinstance(value.get(key), str):
                current[key] = value[key]
        for key in ("full_path", "path", "source_path", "candidate_path", "member_path"):
            path = value.get(key)
            if isinstance(path, str) and PurePosixPath(path).suffix.casefold() in TEXT_EXTS:
                yield {"path": path, **current}
        for nested in value.values():
            yield from _artifact_paths(nested, context=current)
    elif isinstance(value, list):
        for nested in value:
            yield from _artifact_paths(nested, context=context)


def artifact_candidates(payloads: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    output = []
    seen = set()
    for payload in payloads:
        for item in _artifact_paths(payload):
            path = str(item["path"])
            paired_video = str(item.get("paired_video_path") or item.get("video_path") or "")
            key = (path, paired_video)
            if key in seen:
                continue
            seen.add(key)
            is_alist = path.startswith("/") and any(_inside(path, root) for root in SEARCH_ROOTS)
            output.append({
                "candidate_id": canonical_digest({"path": path, "paired_video": paired_video})[:24],
                "path": path,
                "extension": PurePosixPath(path).suffix.casefold(),
                "source_kind": "alist" if is_alist else "external_manifest",
                "origin_root": _candidate_origin(path, SEARCH_ROOTS),
                "retrieval_mode": (
                    "read_existing_subtitle_only" if is_alist else "fetch_subtitle_member_only"
                ),
                "paired_video_path": paired_video,
                "provider_metadata": {
                    key: str(item[key]) for key in ("title", "label") if item.get(key)
                },
            })
    return sorted(output, key=lambda row: (row["path"], row.get("paired_video_path", "")))


def identity_score(request: Mapping[str, Any], candidate: Mapping[str, Any]) -> tuple[int, str] | None:
    video_path = str(request["video_path"])
    candidate_path = str(candidate["path"])
    video_stem = str(PurePosixPath(video_path).with_suffix(""))
    candidate_companion = companion_stem(candidate_path)
    if candidate_companion.casefold() == video_stem.casefold():
        return 120, "exact_full_companion_stem"
    if PurePosixPath(candidate_companion).name.casefold() == PurePosixPath(video_stem).name.casefold():
        return 110, "exact_companion_basename"
    paired_video = str(candidate.get("paired_video_path") or "")
    if paired_video and PurePosixPath(paired_video).stem.casefold() == PurePosixPath(video_path).stem.casefold():
        return 105, "manifest_paired_video_stem"
    request_identity = episode_numbers(video_path)
    candidate_identity = episode_numbers(candidate_path)
    if request_identity is None or candidate_identity is None:
        return None
    request_season, request_episodes = request_identity
    candidate_season, candidate_episodes = candidate_identity
    if request_season != candidate_season or not request_episodes.issubset(candidate_episodes):
        return None
    title_key = _title_key(str(request.get("title") or ""))
    if len(title_key) < 2 or title_key not in _title_key(candidate_path):
        return None
    return 80, "episode_and_title_identity"


def validate_candidate(candidate: Mapping[str, Any], payload: bytes | None) -> dict[str, Any]:
    row = dict(candidate)
    if row.get("source_kind") == "external_manifest" and payload is None:
        row.update({
            "validation_status": "subtitle_only_fetch_required",
            "eligible": False,
            "acquisition_request": {
                "member_path": row["path"],
                "include_video": False,
                "paired_video_metadata_only": bool(row.get("paired_video_path")),
            },
        })
        return row
    if payload is None:
        row.update({"validation_status": "read_failed", "eligible": False})
        return row
    evidence = classify_subtitle_content(payload[:256 * 1024], str(row["extension"]))
    row["content_evidence"] = evidence
    row["payload_sha256"] = hashlib.sha256(payload).hexdigest()
    row["payload_size"] = len(payload)
    row["validation_status"] = (
        "verified_zh_CN" if evidence.get("status") == "chinese" else "not_verified_zh_CN"
    )
    row["eligible"] = evidence.get("status") == "chinese"
    return row


def _valid_selected_ambiguity_resolution(value: Mapping[str, Any]) -> bool:
    if (
        value.get("status") != "selected"
        or value.get("schema_version") != 1
        or value.get("kind") != "subtitle_ambiguity_content_witness"
        or value.get("reason") != "unique_embedded_multisample_and_timeline_witness"
        or not isinstance(value.get("selected_candidate_id"), str)
        or not re.fullmatch(r"[0-9a-f]{64}", str(value.get("proof_sha256") or ""))
    ):
        return False
    keys = (
        "schema_version", "kind", "request_id", "video_path",
        "video_duration_seconds", "embedded_stream_index", "candidate_proofs",
        "embedded_sample_proofs", "policy", "selected_candidate_id", "reason",
    )
    if any(key not in value for key in keys):
        return False
    return canonical_digest({key: value[key] for key in keys}) == value["proof_sha256"]


def _resolution_matches_ambiguity(
    value: Mapping[str, Any], request: Mapping[str, Any], candidates: Sequence[Mapping[str, Any]],
) -> bool:
    if not _valid_selected_ambiguity_resolution(value) or (
        value.get("request_id") != request.get("request_id")
        or value.get("video_path") != request.get("video_path")
    ):
        return False
    proof_rows = value.get("candidate_proofs")
    if not isinstance(proof_rows, list):
        return False
    proof_digests = {
        str(row.get("candidate_id")): str(row.get("payload_sha256"))
        for row in proof_rows if isinstance(row, Mapping)
    }
    actual_digests = {
        str(row.get("candidate_id")): str(row.get("payload_sha256"))
        for row in candidates
    }
    return proof_digests == actual_digests


def build_selection(
    requests_payload: Mapping[str, Any],
    candidates: Sequence[Mapping[str, Any]],
    *,
    ambiguity_resolutions: Mapping[str, Mapping[str, Any]] | None = None,
) -> dict[str, Any]:
    selections = []
    acquisition_requests = []
    failures = []
    resolutions = ambiguity_resolutions or {}
    for request in requests_payload.get("requests", []):
        lane = str(request.get("lane") or "")
        if lane != MUTATING_SUBTITLE_LANE:
            failures.append({
                "request_id": request.get("request_id"),
                "video_path": request.get("video_path"),
                "lane": lane,
                "status": (
                    "subtitle_verification_required"
                    if lane == VERIFICATION_SUBTITLE_LANE
                    else "ineligible_request_lane"
                ),
                "isolated": True,
            })
            continue
        ambiguity_resolution = None
        matches = []
        for candidate in candidates:
            identity = identity_score(request, candidate)
            if identity is None:
                continue
            score, method = identity
            row = {**dict(candidate), "identity_score": score, "identity_method": method}
            if row.get("validation_status") == "subtitle_only_fetch_required":
                acquisition_requests.append({
                    "request_id": request["request_id"],
                    "lane": request["lane"],
                    "candidate_id": row["candidate_id"],
                    "subtitle_member_path": row["path"],
                    "include_video": False,
                    "paired_video_metadata_only": bool(row.get("paired_video_path")),
                })
            if row.get("eligible") is True:
                matches.append(row)
        matches.sort(key=lambda row: (-int(row["identity_score"]), str(row["path"])))
        if not matches:
            failures.append({
                "request_id": request["request_id"],
                "video_path": request["video_path"],
                "status": "no_verified_zh_CN_candidate",
                "isolated": True,
            })
            continue
        best_score = matches[0]["identity_score"]
        best = [row for row in matches if row["identity_score"] == best_score]
        digests = {str(row.get("payload_sha256") or "") for row in best}
        if len(best) > 1 and len(digests) > 1:
            exact_formal = [
                row for row in best
                if row.get("source_kind") == "alist"
                and any(_inside(str(row["path"]), root) for root in FORMAL_ROOTS)
                and companion_stem(str(row["path"])).casefold()
                == str(PurePosixPath(str(request["video_path"])).with_suffix("")).casefold()
            ]
            attempted = resolutions.get(str(request["request_id"]))
            selected_id = (
                str(attempted.get("selected_candidate_id") or "")
                if isinstance(attempted, Mapping)
                and _resolution_matches_ambiguity(attempted, request, best)
                else ""
            )
            winners = [row for row in best if row["candidate_id"] == selected_id]
            if len(winners) == 1:
                best = winners
                ambiguity_resolution = dict(attempted)
            else:
                failure = {
                    "request_id": request["request_id"],
                    "video_path": request["video_path"],
                    "status": "ambiguous_verified_candidates",
                    "candidate_ids": [row["candidate_id"] for row in best],
                    "candidate_evidence": [{
                        "candidate_id": row["candidate_id"],
                        "path": row["path"],
                        "payload_sha256": row.get("payload_sha256"),
                        "identity_score": row.get("identity_score"),
                        "identity_method": row.get("identity_method"),
                        # Path location is recorded as evidence only.  It is not
                        # sufficient to choose between different verified text.
                        "exact_formal_companion": row in exact_formal,
                    } for row in best],
                    "resolution": "awaiting_unique_deterministic_evidence",
                    "isolated": True,
                }
                if isinstance(attempted, Mapping):
                    failure["content_witness"] = dict(attempted)
                failures.append(failure)
                continue
        selected = best[0]
        existing_companion = (
            selected.get("source_kind") == "alist"
            and any(_inside(str(selected["path"]), root) for root in FORMAL_ROOTS)
            and companion_stem(str(selected["path"])).casefold()
            == str(PurePosixPath(str(request["video_path"])).with_suffix("")).casefold()
        )
        target_path = (
            str(selected["path"])
            if existing_companion
            else _safe_target(str(request["video_path"]), str(selected["extension"]))
        )
        selections.append({
            "request_id": request["request_id"],
            "lane": request["lane"],
            "video_path": request["video_path"],
            "candidate_id": selected["candidate_id"],
            "candidate_path": selected["path"],
            "candidate_source_kind": selected["source_kind"],
            "payload_sha256": selected["payload_sha256"],
            "payload_size": selected["payload_size"],
            "content_evidence": selected["content_evidence"],
            "identity_score": selected["identity_score"],
            "identity_method": selected["identity_method"],
            "target_path": target_path,
            "operation": (
                "already_satisfied_existing_companion"
                if existing_companion else "create_external_subtitle"
            ),
            **(
                {"ambiguity_resolution": ambiguity_resolution}
                if ambiguity_resolution is not None else {}
            ),
        })
    core = {
        "schema_version": 1,
        "kind": "subtitle_selection",
        "request_sha256": requests_payload["request_sha256"],
        "selections": selections,
        "acquisition_requests": sorted(
            acquisition_requests, key=lambda row: (row["request_id"], row["candidate_id"]),
        ),
        "failures": failures,
    }
    digest = canonical_digest(core)
    return {
        **core,
        "selection_sha256": digest,
        "summary": {
            "requests": len(requests_payload.get("requests", [])),
            "selected": len(selections),
            "subtitle_only_acquisitions": len(acquisition_requests),
            "isolated_failures": len(failures),
            "video_mutations": 0,
        },
    }


def _atomic_json(path: Path, payload: Mapping[str, Any]) -> None:
    atomic_write_json(path, payload, allow_nan=False)


def _hash_local_file(path: Path) -> tuple[int, str]:
    digest = hashlib.sha256()
    size = 0
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            size += len(chunk)
            digest.update(chunk)
    return size, digest.hexdigest()


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _materialize_subtitle_payload(
    transaction_root: Path,
    *,
    selection_sha256: str,
    request_id: str,
    target_path: str,
    payload: bytes,
    expected_sha256: str,
) -> Path:
    if hashlib.sha256(payload).hexdigest() != expected_sha256:
        raise ValueError("字幕候选内容已改变")
    identity = canonical_digest({
        "selection_sha256": selection_sha256,
        "request_id": request_id,
        "target_path": target_path,
        "payload_sha256": expected_sha256,
    })
    payload_dir = transaction_root / "payloads"
    payload_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
    payload_path = payload_dir / f"{identity}{PurePosixPath(target_path).suffix.casefold()}"
    if payload_path.exists():
        size, sha256 = _hash_local_file(payload_path)
        if size != len(payload) or sha256 != expected_sha256:
            raise LocalUploadConflict(
                f"持久字幕 payload 与已批准内容不一致，拒绝覆盖: {payload_path}"
            )
        return payload_path

    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{identity}.", suffix=".tmp", dir=payload_dir,
    )
    temporary_path = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        try:
            os.link(temporary_path, payload_path)
        except FileExistsError:
            # A concurrent process materialized the same deterministic payload.
            # Never replace it; prove that it is byte-identical below.
            pass
        _fsync_directory(payload_dir)
    finally:
        try:
            temporary_path.unlink()
        except FileNotFoundError:
            pass
    size, sha256 = _hash_local_file(payload_path)
    if size != len(payload) or sha256 != expected_sha256:
        raise LocalUploadConflict(
            f"持久字幕 payload 写入后校验失败，原文件已保留: {payload_path}"
        )
    return payload_path


def _subtitle_upload_receipt_core(
    *,
    selection_sha256: str,
    request_id: str,
    target_path: str,
    payload_path: Path,
    result: LocalUploadResult,
) -> dict[str, Any]:
    if result.receipt_sha256 is None:
        raise LocalUploadConflict("字幕上传事务未返回完整 receipt")
    return {
        "selection_sha256": selection_sha256,
        "request_id": request_id,
        "transaction_id": result.transaction_id,
        "target_path": target_path,
        "payload_path": str(payload_path),
        "size": result.size,
        "sha256": result.sha256,
        "local_receipt_sha256": result.receipt_sha256,
        "transaction_journal_path": str(result.journal_path),
        "upload_calls_recorded": result.upload_calls_recorded,
    }


def _bind_subtitle_upload_receipt(
    journal: dict[str, Any],
    journal_path: Path,
    *,
    selection_sha256: str,
    request_id: str,
    target_path: str,
    payload_path: Path,
    result: LocalUploadResult,
) -> dict[str, Any]:
    core = _subtitle_upload_receipt_core(
        selection_sha256=selection_sha256,
        request_id=request_id,
        target_path=target_path,
        payload_path=payload_path,
        result=result,
    )
    receipt = {**core, "binding_sha256": canonical_digest(core)}
    receipts = journal.setdefault("local_upload_receipts", {})
    if not isinstance(receipts, dict):
        raise ValueError("字幕 journal 的 local_upload_receipts 格式异常")
    existing = receipts.get(request_id)
    if existing is not None and existing != receipt:
        raise LocalUploadConflict(
            f"父 journal 已绑定不同的字幕上传 receipt: {request_id}"
        )
    receipts[request_id] = receipt
    _atomic_json(journal_path, journal)
    return receipt


def _valid_bound_subtitle_receipt(
    journal: Mapping[str, Any], record: Mapping[str, Any],
) -> bool:
    request_id = str(record.get("request_id") or "")
    receipts = journal.get("local_upload_receipts")
    receipt = receipts.get(request_id) if isinstance(receipts, Mapping) else None
    if not isinstance(receipt, Mapping):
        return False
    core = {
        key: receipt.get(key)
        for key in (
            "selection_sha256", "request_id", "transaction_id", "target_path",
            "payload_path", "size", "sha256", "local_receipt_sha256",
            "transaction_journal_path", "upload_calls_recorded",
        )
    }
    return (
        receipt.get("binding_sha256") == canonical_digest(core)
        and receipt.get("selection_sha256") == journal.get("selection_sha256")
        and receipt.get("request_id") == request_id
        and receipt.get("target_path") == record.get("target_path")
        and receipt.get("sha256") == record.get("payload_sha256")
        and receipt.get("transaction_id") == record.get("transaction_id")
        and receipt.get("binding_sha256") == record.get("receipt_binding_sha256")
    )


def _reject_subtitle_target_aliases(client: Any, target_path: str) -> None:
    parent = str(PurePosixPath(target_path).parent)
    name = PurePosixPath(target_path).name
    collisions = [
        row for row in (client.try_list(parent, refresh=True) or [])
        if str(row.get("name") or "").casefold() == name.casefold()
        and str(row.get("name") or "") != name
    ]
    if collisions:
        raise LocalUploadConflict(
            f"目标字幕存在大小写或 Unicode 冲突，拒绝上传: {target_path}"
        )


def execute_selection(
    client: Any,
    selection: Mapping[str, Any],
    *,
    approved_selection_sha256: str,
    journal_path: Path,
    item_guard: Callable[[], ContextManager[Any]] | None = None,
    local_cache_root: Path | None = None,
) -> dict[str, Any]:
    actual = str(selection.get("selection_sha256") or "")
    selection_core = {
        key: selection[key]
        for key in (
            "schema_version", "kind", "request_sha256", "selections",
            "acquisition_requests", "failures",
        )
    }
    if canonical_digest(selection_core) != actual:
        raise ValueError("字幕 selection 内容与自身摘要不一致")
    if not re.fullmatch(r"[0-9a-f]{64}", approved_selection_sha256) or approved_selection_sha256 != actual:
        raise ValueError("字幕选择摘要未批准或已改变")
    mutation_items = [
        *selection.get("selections", []),
        *selection.get("acquisition_requests", []),
    ]
    if any(
        not isinstance(item, Mapping)
        or item.get("lane") != MUTATING_SUBTITLE_LANE
        for item in mutation_items
    ):
        raise ValueError("字幕选择包含非 confirmed lane，拒绝执行")
    journal = (
        json.loads(journal_path.read_text(encoding="utf-8"))
        if journal_path.exists() else {
            "schema_version": 1,
            "kind": "subtitle_execution_journal",
            "selection_sha256": actual,
            "status": "running",
            "records": [],
            "local_upload_receipts": {},
        }
    )
    if journal.get("selection_sha256") != actual or not isinstance(journal.get("records"), list):
        raise ValueError("现有字幕 journal 不属于当前 selection")
    journal.setdefault("local_upload_receipts", {})
    if not isinstance(journal["local_upload_receipts"], dict):
        raise ValueError("现有字幕 journal 的 receipt 格式异常")
    # A previous isolated failure is evidence, not a terminal success.  It is
    # safe to retry because the only permitted mutation is create-only and an
    # existing target is content-verified before it is accepted.  Successful
    # records remain idempotent across any number of restarts.
    completed = {
        str(row.get("request_id")) for row in journal["records"]
        if (
            row.get("status") == "already_satisfied"
            and (
                not row.get("transaction_id")
                or _valid_bound_subtitle_receipt(journal, row)
            )
        ) or (
            row.get("status") == "created"
            and _valid_bound_subtitle_receipt(journal, row)
        )
    }
    transaction_root = (
        journal_path.parent / ".subtitle-upload-transactions"
    ).resolve()
    for item in selection.get("selections", []):
        request_id = str(item["request_id"])
        if request_id in completed:
            continue
        # The server supplies a global-pause transition guard.  Enter it before
        # even recording a running item so a paused service performs neither a
        # remote read nor an upload.  Standalone/offline usage keeps the former
        # no-op guard behavior.
        with (item_guard() if item_guard is not None else nullcontext()):
            record = {
                "request_id": request_id,
                "target_path": item["target_path"],
                "candidate_path": item["candidate_path"],
                "status": "running",
            }
            journal["records"].append(record)
            _atomic_json(journal_path, journal)
            try:
                target = str(item["target_path"])
                if PurePosixPath(target).suffix.casefold() not in TEXT_EXTS or not any(_inside(target, root) for root in FORMAL_ROOTS):
                    raise ValueError("执行目标越出字幕安全边界")
                if item.get("operation") == "already_satisfied_existing_companion":
                    current = client.read_file_bytes(target, max_bytes=MAX_SUBTITLE_BYTES)
                    if hashlib.sha256(current).hexdigest() != item["payload_sha256"]:
                        raise ValueError("已验证字幕内容已改变")
                    if classify_subtitle_content(current[:256 * 1024], PurePosixPath(target).suffix).get("status") != "chinese":
                        raise ValueError("现有外挂字幕复核未通过")
                    record["status"] = "already_satisfied"
                    record["payload_sha256"] = item["payload_sha256"]
                    record["finished_at"] = _now()
                    _atomic_json(journal_path, journal)
                    continue
                source_kind = item.get("candidate_source_kind")
                if source_kind == "alist":
                    payload = client.read_file_bytes(
                        str(item["candidate_path"]), max_bytes=MAX_SUBTITLE_BYTES,
                    )
                elif source_kind == "local_verified_cache":
                    if local_cache_root is None:
                        raise ValueError("本地字幕缓存未授权")
                    cache_root = local_cache_root.resolve()
                    candidate_path = Path(str(item["candidate_path"])).resolve()
                    if candidate_path == cache_root or cache_root not in candidate_path.parents:
                        raise ValueError("本地字幕缓存越界")
                    payload = candidate_path.read_bytes()
                    if len(payload) > MAX_SUBTITLE_BYTES:
                        raise ValueError("本地字幕超过大小上限")
                else:
                    raise ValueError("外部字幕成员尚未完成 subtitle-only 获取")
                if hashlib.sha256(payload).hexdigest() != item["payload_sha256"]:
                    raise ValueError("字幕候选内容已改变")
                if classify_subtitle_content(payload[:256 * 1024], str(PurePosixPath(target).suffix)).get("status") != "chinese":
                    raise ValueError("执行前字幕正文复核未通过")
                payload_path = _materialize_subtitle_payload(
                    transaction_root,
                    selection_sha256=actual,
                    request_id=request_id,
                    target_path=target,
                    payload=payload,
                    expected_sha256=str(item["payload_sha256"]),
                )
                _reject_subtitle_target_aliases(client, target)
                transaction_id = deterministic_local_upload_id(payload_path, target)
                result = run_local_upload_transaction(
                    AListExactFileAdapter(client),
                    transaction_root=transaction_root,
                    spec=LocalUploadSpec(
                        transaction_id=transaction_id,
                        source_path=payload_path,
                        target_path=target,
                        expected_size=len(payload),
                        expected_sha256=str(item["payload_sha256"]),
                        content_type="text/plain; charset=utf-8",
                    ),
                )
                receipt = _bind_subtitle_upload_receipt(
                    journal,
                    journal_path,
                    selection_sha256=actual,
                    request_id=request_id,
                    target_path=target,
                    payload_path=payload_path,
                    result=result,
                )
                record["status"] = (
                    "created" if result.upload_calls_recorded == 1
                    else "already_satisfied"
                )
                record["payload_sha256"] = result.sha256
                record["transaction_id"] = result.transaction_id
                record["receipt_binding_sha256"] = receipt["binding_sha256"]
                record["finished_at"] = _now()
                _atomic_json(journal_path, journal)
                try:
                    payload_path.unlink()
                    _fsync_directory(payload_path.parent)
                except OSError:
                    # A verified receipt and created record are authoritative;
                    # cleanup failure only leaves a harmless recovery payload.
                    pass
            except Exception as exc:  # isolate one subtitle without aborting siblings
                record["status"] = "failed"
                record["error"] = f"{type(exc).__name__}: {exc}"
            record["finished_at"] = _now()
            _atomic_json(journal_path, journal)
    # The terminal transition is itself a durable task mutation.  Re-enter the
    # same pause boundary after the last item (and for an empty selection) so a
    # pause that wins between the final item commit and this summary cannot
    # publish a misleading terminal journal.
    with (item_guard() if item_guard is not None else nullcontext()):
        latest_records: dict[str, Mapping[str, Any]] = {}
        for row in journal["records"]:
            latest_records[str(row.get("request_id"))] = row
        journal["status"] = (
            "completed_with_isolated_failures"
            if any(row.get("status") == "failed" for row in latest_records.values())
            else "success"
        )
        journal["completed_at"] = _now()
        _atomic_json(journal_path, journal)
    return journal


def _load_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"JSON 必须是对象: {path}")
    return payload


def _safe_provider_headers(headers: Mapping[str, Any]) -> str:
    output = []
    for raw_name, raw_value in headers.items():
        name = str(raw_name).strip()
        value = str(raw_value).strip()
        if not re.fullmatch(r"[A-Za-z0-9-]+", name) or "\r" in value or "\n" in value:
            raise ValueError("unsafe provider headers")
        output.append(f"{name}: {value}\r\n")
    return "".join(output)


def _ambiguity_sample_offsets(duration: float) -> tuple[float, float] | None:
    first = min(300.0, duration * 0.25)
    second = min(600.0, duration - AMBIGUITY_SAMPLE_SECONDS - 30.0)
    if first < 0 or second - first < 120.0:
        return None
    return round(first, 3), round(second, 3)


def _probe_ambiguity_video(client: Any, video_path: str) -> dict[str, Any]:
    if shutil.which("ffprobe") is None:
        raise RuntimeError("ffprobe_not_installed")
    raw_url, headers = client.file_link(video_path, refresh=False)
    safe_headers = _safe_provider_headers(headers)
    command = ["ffprobe", "-v", "error", "-rw_timeout", "15000000"]
    if safe_headers:
        command.extend(["-headers", safe_headers])
    command.extend([
        "-show_entries", "format=duration:stream=index,codec_name",
        "-of", "json", raw_url,
    ])
    completed = subprocess.run(
        command, check=False, capture_output=True, text=True,
        timeout=AMBIGUITY_PROBE_TIMEOUT,
    )
    if completed.returncode:
        raise RuntimeError("ffprobe_nonzero_exit")
    payload = json.loads(completed.stdout)
    duration = float((payload.get("format") or {}).get("duration"))
    if not (duration > 0):
        raise ValueError("video_duration_missing")
    streams = [
        {"index": row.get("index"), "codec_name": row.get("codec_name")}
        for row in payload.get("streams", [])
        if isinstance(row, Mapping)
        and type(row.get("index")) is int
        and str(row.get("codec_name") or "").casefold() in TEXT_STREAM_CODECS
    ]
    return {
        "duration_seconds": duration,
        "streams": streams,
        # These two values are process-local capabilities and are never
        # returned in selection evidence.
        "_raw_url": raw_url,
        "_safe_headers": safe_headers,
    }


def _extract_ambiguity_sample(
    probe: Mapping[str, Any], stream_index: int, offset: float,
) -> bytes:
    if shutil.which("ffmpeg") is None:
        raise RuntimeError("ffmpeg_not_installed")
    command = ["ffmpeg", "-v", "error", "-rw_timeout", "15000000"]
    safe_headers = str(probe.get("_safe_headers") or "")
    if safe_headers:
        command.extend(["-headers", safe_headers])
    command.extend([
        "-ss", str(offset), "-i", str(probe["_raw_url"]),
        "-map", f"0:{stream_index}", "-t", str(AMBIGUITY_SAMPLE_SECONDS),
        "-f", "ass", "pipe:1",
    ])
    completed = subprocess.run(
        command, check=False, capture_output=True,
        timeout=AMBIGUITY_SAMPLE_TIMEOUT,
    )
    if completed.returncode or not completed.stdout:
        raise RuntimeError("ffmpeg_embedded_sample_failed")
    return completed.stdout


def _resolve_one_ambiguity(
    client: Any,
    request: Mapping[str, Any],
    candidates: Sequence[Mapping[str, Any]],
    *,
    scan_gate: Callable[[], None] | None = None,
) -> dict[str, Any]:
    if scan_gate is not None:
        scan_gate()
    probe = _probe_ambiguity_video(client, str(request["video_path"]))
    offsets = _ambiguity_sample_offsets(float(probe["duration_seconds"]))
    if offsets is None:
        return {
            "status": "unresolved", "selected_candidate_id": None,
            "reason": "video_too_short_for_independent_witness_windows",
        }
    stream_results = []
    for stream in probe["streams"]:
        stream_index = int(stream["index"])
        samples = []
        try:
            for offset in offsets:
                if scan_gate is not None:
                    scan_gate()
                samples.append({
                    "offset_seconds": offset,
                    "duration_seconds": AMBIGUITY_SAMPLE_SECONDS,
                    "extension": ".ass",
                    "payload": _extract_ambiguity_sample(probe, stream_index, offset),
                })
            result = resolve_ambiguous_by_embedded_witness(
                request, candidates,
                {"duration_seconds": probe["duration_seconds"], "stream_index": stream_index},
                samples,
            )
        except (OSError, RuntimeError, ValueError, subprocess.TimeoutExpired) as exc:
            result = {
                "status": "unresolved", "selected_candidate_id": None,
                "reason": "embedded_witness_io_failed",
                "error_type": type(exc).__name__,
                "embedded_stream_index": stream_index,
            }
        stream_results.append(result)
    selected = [row for row in stream_results if row.get("status") == "selected"]
    if len(selected) == 1:
        return selected[0]
    return {
        "status": "unresolved", "selected_candidate_id": None,
        "reason": (
            "no_embedded_text_stream" if not probe["streams"] else
            "no_unique_embedded_stream_winner" if not selected else
            "multiple_embedded_stream_winners"
        ),
        "video_duration_seconds": probe["duration_seconds"],
        "stream_results": stream_results,
    }


def _resolve_current_ambiguities(
    client: Any,
    requests: Mapping[str, Any],
    selection: Mapping[str, Any],
    validated: Sequence[Mapping[str, Any]],
    payload_cache: Mapping[str, bytes | None],
    *,
    scan_gate: Callable[[], None] | None = None,
) -> dict[str, dict[str, Any]]:
    request_by_id = {
        str(row.get("request_id")): row for row in requests.get("requests", [])
        if isinstance(row, Mapping) and row.get("request_id")
    }
    candidate_by_id = {
        str(row.get("candidate_id")): row for row in validated
        if isinstance(row, Mapping) and row.get("candidate_id")
    }
    output = {}
    for failure in selection.get("failures", []):
        if not isinstance(failure, Mapping) or failure.get("status") != "ambiguous_verified_candidates":
            continue
        request_id = str(failure.get("request_id") or "")
        request = request_by_id.get(request_id)
        candidate_inputs = []
        for candidate_id in failure.get("candidate_ids", []):
            candidate = candidate_by_id.get(str(candidate_id))
            if candidate is None:
                continue
            payload = payload_cache.get(str(candidate.get("path") or ""))
            if not isinstance(payload, bytes):
                continue
            candidate_inputs.append({
                "candidate_id": candidate["candidate_id"],
                "path": candidate.get("path"),
                "extension": candidate.get("extension"),
                "payload": payload,
            })
        if request is None or len(candidate_inputs) != len(failure.get("candidate_ids", [])):
            output[request_id] = {
                "status": "unresolved", "selected_candidate_id": None,
                "reason": "candidate_payload_missing_for_content_witness",
            }
            continue
        try:
            output[request_id] = _resolve_one_ambiguity(
                client, request, candidate_inputs, scan_gate=scan_gate,
            )
        except (OSError, RuntimeError, ValueError, json.JSONDecodeError, subprocess.TimeoutExpired) as exc:
            output[request_id] = {
                "status": "unresolved", "selected_candidate_id": None,
                "reason": "content_witness_probe_failed",
                "error_type": type(exc).__name__,
            }
    return output


def prepare_selection(
    client: Any,
    refined: Mapping[str, Any],
    *,
    roots: Sequence[str] = SEARCH_ROOTS,
    artifact_payloads: Iterable[Mapping[str, Any]] = (),
    scan_gate: Callable[[], None] | None = None,
    ambiguity_resolver: Callable[..., dict[str, dict[str, Any]]] | None = None,
) -> dict[str, Any]:
    """Build a fresh content-verified selection without mutating AList.

    A configured root that fails while listing/walking is a hard execution
    blocker.  Optional search roots that simply do not exist are recorded and
    skipped; formal library roots are mandatory.  This distinction avoids
    manufacturing an empty remote ``字幕备份`` directory while preserving the
    fail-closed behavior for every real storage or network error.
    """
    requests = build_requests(refined)
    rows: list[dict[str, Any]] = []
    scan_failures: list[dict[str, Any]] = []
    missing_roots: list[str] = []
    for root in roots:
        try:
            if scan_gate is not None:
                scan_gate()
            exists = client.try_list(root, refresh=True)
            if exists is None:
                if root in FORMAL_ROOTS:
                    scan_failures.append({"root": root, "error": "required_root_missing"})
                else:
                    missing_roots.append(root)
                continue
            if scan_gate is not None:
                scan_gate()
            rows.extend(client.walk(
                root, refresh=True, ignore_orphan_temp=False,
                include_bonus=True, include_title_extras=True,
            ))
        except Exception as exc:
            scan_failures.append({
                "root": root,
                "error": f"{type(exc).__name__}: {exc}",
            })
    candidates = inventory_candidates(rows, roots)
    candidates.extend(artifact_candidates(artifact_payloads))
    request_rows = requests["requests"]
    candidates = [
        candidate for candidate in candidates
        if any(identity_score(request, candidate) is not None for request in request_rows)
    ]
    validated = []
    payload_cache: dict[str, bytes | None] = {}
    for candidate in candidates:
        if scan_gate is not None:
            scan_gate()
        payload = None
        if candidate["source_kind"] == "alist":
            path = str(candidate["path"])
            if path not in payload_cache:
                try:
                    payload_cache[path] = client.read_file_bytes(
                        path, max_bytes=MAX_SUBTITLE_BYTES,
                    )
                except Exception:
                    payload_cache[path] = None
            payload = payload_cache[path]
        validated.append(validate_candidate(candidate, payload))
    selection = build_selection(requests, validated)
    resolver = ambiguity_resolver or _resolve_current_ambiguities
    ambiguity_resolutions = resolver(
        client, requests, selection, validated, payload_cache, scan_gate=scan_gate,
    )
    if ambiguity_resolutions:
        selection = build_selection(
            requests, validated, ambiguity_resolutions=ambiguity_resolutions,
        )
    return {
        "requests": requests,
        "selection": selection,
        "candidate_summary": dict(Counter(row["validation_status"] for row in validated)),
        "inventory_scan_failures": scan_failures,
        "inventory_missing_optional_roots": sorted(missing_roots),
        "ambiguity_resolutions": ambiguity_resolutions,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--refined", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--candidate-artifact", type=Path, action="append", default=[])
    parser.add_argument("--search-root", action="append")
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--approve-selection-sha256")
    parser.add_argument("--journal", type=Path)
    args = parser.parse_args()
    roots = tuple(args.search_root or SEARCH_ROOTS)
    client = AListClient(
        os.environ.get("ALIST_URL", "http://127.0.0.1:5244"),
        os.environ.get("ALIST_USERNAME", ""), os.environ.get("ALIST_PASSWORD", ""),
        allow_insecure_http=True,
    )
    client.login()
    prepared = prepare_selection(
        client, _load_json(args.refined), roots=roots,
        artifact_payloads=(_load_json(path) for path in args.candidate_artifact),
    )
    requests = prepared["requests"]
    selection = prepared["selection"]
    inventory_scan_failures = prepared["inventory_scan_failures"]
    result = {
        "mode": "execute" if args.execute else "dry_run",
        "generated_at": _now(),
        **prepared,
    }
    if args.execute:
        if inventory_scan_failures:
            raise ValueError("候选字幕根目录扫描不完整，拒绝执行")
        if not args.approve_selection_sha256 or args.journal is None:
            raise ValueError("执行必须提供 selection 摘要与 journal")
        result["journal"] = execute_selection(
            client, selection,
            approved_selection_sha256=args.approve_selection_sha256,
            journal_path=args.journal,
        )
    _atomic_json(args.output, result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
