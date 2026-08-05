"""Pure planning primitives for an explicitly launched one-time library completion.

This module deliberately has no AList mutation or task-runner dependency.  It
turns already collected inventory/audit evidence into deterministic plans.  A
caller may dispatch those plans only after applying the durable global-pause
gate and revalidating the evidence digests.
"""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timezone
import hashlib
import json
import posixpath
import re
import unicodedata
from typing import Any, Iterable, Mapping

from engine.scrapeflow.one_time_movie_member_scope import (
    seal_movie_member_candidate,
    validate_movie_member_candidate,
)


MEDIA_ROOT = "/quark/影视"
FORMAL_ROOTS = (
    f"{MEDIA_ROOT}/电影",
    f"{MEDIA_ROOT}/番剧",
    f"{MEDIA_ROOT}/美剧",
)
INBOX_ROOT = f"{MEDIA_ROOT}/待刮削"
SYSTEM_ROOT = f"{MEDIA_ROOT}/ScrapeFlow"
VIDEO_EXTENSIONS = frozenset({
    ".mkv", ".mp4", ".m4v", ".m2ts", ".ts", ".avi", ".mov", ".webm",
})
SUBTITLE_EXTENSIONS = frozenset({
    ".ass", ".ssa", ".srt", ".vtt", ".idx", ".sub", ".sup", ".mks",
})
ARCHIVE_EXTENSIONS = frozenset({".zip", ".7z", ".rar", ".001"})
PARTIAL_TRANSFER_EXTENSIONS = frozenset({".pptd", ".part", ".aria2", ".download"})
TERMINAL_TASK_PHASES = frozenset({
    "completed", "cancelled", "failed", "recovered",
})


def canonical_digest(value: Any) -> str:
    payload = json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _normalize_path(value: Any) -> str:
    if not isinstance(value, str) or not value.strip() or "\\" in value:
        raise ValueError(f"无效媒体路径: {value!r}")
    normalized = posixpath.normpath("/" + value.strip().lstrip("/"))
    if normalized == "/" or any(ord(char) < 32 for char in normalized):
        raise ValueError(f"无效媒体路径: {value!r}")
    return normalized


def _direct_child(path: str, parent: str) -> bool:
    path = _normalize_path(path)
    parent = _normalize_path(parent)
    return posixpath.dirname(path) == parent


def _inside(path: str, root: str) -> bool:
    path = _normalize_path(path)
    root = _normalize_path(root)
    return path == root or path.startswith(root.rstrip("/") + "/")


def formal_category(target_root: Any) -> str:
    target = _normalize_path(target_root)
    matches = [root for root in FORMAL_ROOTS if target != root and _inside(target, root)]
    if len(matches) != 1 or _inside(target, SYSTEM_ROOT):
        raise ValueError(f"目标不在唯一正式媒体分类中: {target}")
    return posixpath.basename(matches[0])


def title_key(value: Any) -> str:
    """Return a conservative title key; never use it as fuzzy identity proof."""
    if not isinstance(value, str):
        return ""
    text = unicodedata.normalize("NFKC", value).casefold().strip()
    text = re.sub(r"[\[(（【].*?[\])）】]", " ", text)
    text = re.sub(r"^[a-z]\s+", "", text)
    text = re.sub(
        r"(?i)(?:^|\s)(?:4k|8k|1080p|2160p|uhd|bd(?:rip)?|"
        r"web[- .]?dl|全\d+集|\d+[-~]插?季|内封|内嵌|外挂|字幕|"
        r"系列|全系列|日漫|美剧)(?:\s|$)",
        " ", text,
    )
    return "".join(char for char in text if char.isalnum())


def _identity_aliases(row: Mapping[str, Any]) -> set[str]:
    values: list[Any] = [
        row.get("title"), row.get("official_title"), row.get("original_title"),
        posixpath.basename(str(row.get("target_root") or "")),
    ]
    aliases = row.get("aliases")
    if isinstance(aliases, list):
        values.extend(aliases)
    ancestor_aliases = row.get("ancestor_aliases")
    if isinstance(ancestor_aliases, list):
        values.extend(ancestor_aliases)
    return {key for value in values if (key := title_key(value))}


def build_formal_identity_catalog(projects: Iterable[Any]) -> list[dict[str, Any]]:
    """Validate and deduplicate formal-library identities from a live audit."""
    catalog: dict[tuple[str, int | None, str], dict[str, Any]] = {}
    for raw in projects:
        if not isinstance(raw, Mapping):
            continue
        target = raw.get("target_root")
        try:
            category = formal_category(target)
        except ValueError:
            continue
        ids = raw.get("tmdb_ids")
        if ids is None and type(raw.get("tmdb_id")) is int:
            ids = [raw["tmdb_id"]]
        valid_ids = sorted({value for value in (ids or []) if type(value) is int and value > 0})
        if len(valid_ids) > 1:
            # A title root carrying multiple TMDB identities is not safe inbox
            # inheritance evidence.  Descendant projects remain available.
            continue
        title = str(raw.get("official_title") or raw.get("title") or "").strip()
        if not title:
            continue
        normalized_target = _normalize_path(target)
        category_root = next(root for root in FORMAL_ROOTS if _inside(normalized_target, root))
        relative_parts = [
            part for part in posixpath.relpath(normalized_target, category_root).split("/")
            if part not in {"", "."}
        ]
        declared_type = str(raw.get("media_type") or "").casefold()
        media_type = (
            declared_type if declared_type in {"tv", "movie", "collection"}
            else "movie" if category == "电影" else "tv"
        )
        raw_aliases = raw.get("aliases")
        if not isinstance(raw_aliases, list):
            raw_aliases = []
        regular_gaps = [
            dict(value) for value in (raw.get("regular_missing") or [])
            if isinstance(value, Mapping)
        ]
        optional_gaps = [
            dict(value) for value in (raw.get("optional_missing") or [])
            if isinstance(value, Mapping)
        ]
        row = {
            "title": title,
            "official_title": title,
            "original_title": str(raw.get("original_title") or title).strip(),
            "aliases": sorted(str(value).strip() for value in raw_aliases
                              if isinstance(value, str) and value.strip()),
            "ancestor_aliases": relative_parts[:-1],
            "tmdb_id": valid_ids[0] if valid_ids else None,
            "media_type": media_type,
            "category": category,
            "target_root": normalized_target,
            "regular_gaps": regular_gaps,
            "optional_gaps": optional_gaps,
            "video_gaps": [*regular_gaps, *optional_gaps],
            "video_gap_count": len(regular_gaps) + len(optional_gaps),
            "video_gap_evidence_present": all(
                field in raw and isinstance(raw.get(field), list)
                for field in ("regular_missing", "optional_missing")
            ),
        }
        row["video_complete"] = bool(
            row["video_gap_evidence_present"] and row["video_gap_count"] == 0
        )
        row["formal_complete"] = row["video_complete"]
        row["formal_gap_count"] = (
            row["video_gap_count"] if row["video_gap_evidence_present"] else 0
        )
        row["formal_gaps"] = list(row["video_gaps"])
        key = (normalized_target.casefold(), row["tmdb_id"], row["media_type"])
        catalog[key] = row
    return sorted(catalog.values(), key=lambda row: (
        row["target_root"].casefold(), row["tmdb_id"] or 0,
    ))


def _strong_work_identity(row: Mapping[str, Any]) -> tuple[str, int] | None:
    tmdb_id = row.get("tmdb_id")
    media_type = str(row.get("media_type") or "")
    if type(tmdb_id) is not int or tmdb_id <= 0:
        return None
    namespace = "movie" if media_type in {"movie", "collection"} else media_type
    return (namespace, tmdb_id) if namespace in {"movie", "tv"} else None


def _idempotent_identity(row: Mapping[str, Any] | None) -> Any:
    """Keep submission identity stable when stronger current evidence replaces old evidence."""
    if row is None:
        return None
    strong = _strong_work_identity(row)
    if strong is not None:
        return {"namespace": strong[0], "tmdb_id": strong[1]}
    return {
        "tmdb_id": row.get("tmdb_id"), "media_type": row.get("media_type"),
        "category": row.get("category"), "target_root": row.get("target_root"),
    }


def _collapse_formal_identity(
    matches: list[dict[str, Any]],
) -> dict[str, Any] | None:
    """Collapse cross-category locations only when one strong work ID proves sameness."""
    keys = {_strong_work_identity(row) for row in matches}
    if len(keys) != 1 or None in keys:
        return None
    locations = sorted(({
        "category": row["category"],
        "target_root": row["target_root"],
        "video_complete": row.get("video_complete") is True,
        "video_gap_count": int(row.get("video_gap_count") or 0),
        "video_gap_evidence_present": row.get("video_gap_evidence_present") is True,
        "video_gaps": list(row.get("video_gaps") or []),
    } for row in matches), key=lambda row: (
        not row["video_complete"], row["target_root"].casefold(),
    ))
    representative = next(
        row for row in matches if row["target_root"] == locations[0]["target_root"]
    )
    aliases = sorted({
        alias for row in matches for alias in row.get("aliases", [])
        if isinstance(alias, str) and alias
    })
    return {
        **representative,
        "aliases": aliases,
        "formal_locations": locations,
        "formal_complete": any(row["video_complete"] for row in locations),
        "formal_gap_count": 0 if any(row["video_complete"] for row in locations) else min(
            (row["video_gap_count"] for row in locations if row["video_gap_evidence_present"]),
            default=0,
        ),
        "formal_gaps": [] if any(row["video_complete"] for row in locations) else list(
            min(
                (row for row in locations if row["video_gap_evidence_present"]),
                key=lambda row: row["video_gap_count"],
                default={"video_gaps": []},
            )["video_gaps"]
        ),
        "cross_category_deduplicated": len({row["category"] for row in locations}) > 1,
    }


def inventory_fingerprint(row: Mapping[str, Any]) -> str:
    raw_files = row.get("files", [])
    if not isinstance(raw_files, list):
        raise ValueError("inbox files 必须是数组")
    files = [
        {
            "path": _normalize_path(item.get("path")),
            "size": item.get("size"),
            "hash": item.get("hash"),
        }
        for item in raw_files if isinstance(item, Mapping)
    ]
    files.sort(key=lambda item: item["path"].casefold())
    stable = {
        "path": _normalize_path(row.get("path")),
        "file_count": row.get("file_count"),
        "bytes": row.get("bytes"),
        "files": files,
    }
    return canonical_digest(stable)


def _inbox_action(kind: str, source: str, **payload: Any) -> dict[str, Any]:
    """Return one independently idempotent, mutation-free planned action."""
    body = {"action": kind, "source": source, **payload}
    return {**body, "action_key": canonical_digest(body)}


def _task_identity(row: Mapping[str, Any]) -> dict[str, Any] | None:
    try:
        source = _normalize_path(row.get("source"))
        parent = _normalize_path(row.get("parent"))
    except ValueError:
        return None
    if not _direct_child(source, INBOX_ROOT):
        return None
    try:
        category = formal_category(posixpath.join(parent, "_identity_probe"))
    except ValueError:
        return None
    tmdb_id = row.get("tmdb_id")
    if type(tmdb_id) is not int or tmdb_id <= 0:
        tmdb_id = None
    media_type = str(row.get("media_type") or "auto")
    if media_type not in {"auto", "tv", "movie", "collection"}:
        return None
    return {
        "tmdb_id": tmdb_id,
        "media_type": media_type,
        "category": category,
        "target_root": None,
        "evidence": "historical_exact_source",
    }


def _resolve_inbox_identity(
    source: str,
    *,
    catalog: list[dict[str, Any]],
    historical_tasks: Iterable[Any],
) -> tuple[dict[str, Any] | None, list[dict[str, Any]], str]:
    exact_tasks = []
    for raw in historical_tasks:
        if not isinstance(raw, Mapping):
            continue
        try:
            same_source = _normalize_path(raw.get("source")) == source
        except ValueError:
            continue
        identity = _task_identity(raw) if same_source else None
        if identity is not None:
            exact_tasks.append(identity)
    task_keys = {
        canonical_digest({key: value for key, value in row.items() if key != "evidence"})
        for row in exact_tasks
    }
    if len(task_keys) == 1:
        historical = exact_tasks[0]
        historical_key = _strong_work_identity(historical)
        current = [
            row for row in catalog
            if historical_key is not None and _strong_work_identity(row) == historical_key
        ]
        collapsed = _collapse_formal_identity(current) if current else None
        return {
            **historical,
            **(collapsed or {}),
            "evidence": (
                "historical_exact_source_current_formal_identity"
                if collapsed is not None else "historical_exact_source"
            ),
        }, [], (
            "historical_exact_source_current_formal_identity"
            if collapsed is not None else "historical_exact_source"
        )
    if len(task_keys) > 1:
        return None, exact_tasks, "ambiguous_historical_identity"

    key = title_key(posixpath.basename(source))
    exact = [row for row in catalog if key and key in _identity_aliases(row)]
    if len(exact) == 1:
        collapsed = _collapse_formal_identity(exact) or exact[0]
        return {**collapsed, "evidence": "formal_exact_alias"}, [], "formal_exact_alias"
    if len(exact) > 1:
        collapsed = _collapse_formal_identity(exact)
        if collapsed is not None:
            return {
                **collapsed, "evidence": "formal_cross_category_strong_identity",
            }, [], "formal_cross_category_strong_identity"
        return None, exact, "ambiguous_formal_identity"

    # Containment is accepted only for a globally unique, sufficiently long
    # alias.  This resolves decorated share-folder names without edit distance.
    contained = []
    for row in catalog:
        aliases = _identity_aliases(row)
        if any(
            len(alias) >= 3 and len(key) >= 3 and (alias in key or key in alias)
            for alias in aliases
        ):
            contained.append(row)
    if len(contained) == 1:
        collapsed = _collapse_formal_identity(contained) or contained[0]
        return {**collapsed, "evidence": "formal_unique_containment"}, [], "formal_unique_containment"
    if len(contained) > 1:
        collapsed = _collapse_formal_identity(contained)
        if collapsed is not None:
            return {
                **collapsed, "evidence": "formal_cross_category_strong_identity",
            }, [], "formal_cross_category_strong_identity"
        return None, contained, "ambiguous_formal_identity"
    return None, [], "identity_not_proven"


def build_inbox_discovery_plan(
    inventory: Iterable[Any],
    formal_projects: Iterable[Any],
    *,
    historical_tasks: Iterable[Any] = (),
    unmatched_media_category: str | None = None,
) -> dict[str, Any]:
    """Plan idempotent per-source inbox tasks without performing mutations."""
    catalog = build_formal_identity_catalog(formal_projects)
    if unmatched_media_category is not None and unmatched_media_category not in {
        "电影", "番剧", "美剧",
    }:
        raise ValueError("未匹配媒体默认分类无效")
    historical = list(historical_tasks)
    rows: list[dict[str, Any]] = []
    seen_sources: set[str] = set()
    for raw in inventory:
        if not isinstance(raw, Mapping):
            raise ValueError("inbox inventory 项必须是对象")
        source = _normalize_path(raw.get("path"))
        if not _direct_child(source, INBOX_ROOT) or _inside(source, SYSTEM_ROOT):
            raise ValueError(f"inbox inventory 越界: {source}")
        if source.casefold() in seen_sources:
            raise ValueError(f"inbox inventory 重复: {source}")
        seen_sources.add(source.casefold())
        file_count = raw.get("file_count")
        total_bytes = raw.get("bytes")
        if type(file_count) is not int or file_count < 0:
            raise ValueError(f"inbox file_count 无效: {source}")
        if type(total_bytes) is not int or total_bytes < 0:
            raise ValueError(f"inbox bytes 无效: {source}")
        extensions = raw.get("extensions") if isinstance(raw.get("extensions"), Mapping) else {}
        video_count = sum(
            int(count) for ext, count in extensions.items()
            if str(ext).casefold() in VIDEO_EXTENSIONS and type(count) is int and count >= 0
        )
        subtitle_count = sum(
            int(count) for ext, count in extensions.items()
            if str(ext).casefold() in SUBTITLE_EXTENSIONS and type(count) is int and count >= 0
        )
        archive_count = sum(
            int(count) for ext, count in extensions.items()
            if str(ext).casefold() in ARCHIVE_EXTENSIONS and type(count) is int and count >= 0
        )
        partial_transfer_count = sum(
            int(count) for ext, count in extensions.items()
            if str(ext).casefold() in PARTIAL_TRANSFER_EXTENSIONS
            and type(count) is int and count >= 0
        )
        verified_media_archive_count = raw.get("verified_media_archive_count", 0)
        if (
            type(verified_media_archive_count) is not int
            or not 0 <= verified_media_archive_count <= archive_count
        ):
            raise ValueError(f"inbox verified_media_archive_count 无效: {source}")
        actionable_media_count = video_count + verified_media_archive_count
        fingerprint = inventory_fingerprint(raw)
        pending_delete = bool(
            raw.get("pending_delete") is True
            or re.search(r"[（(]\s*待删\d*\s*[）)]\s*$", posixpath.basename(source))
        )
        identity, candidates, resolution = _resolve_inbox_identity(
            source, catalog=catalog, historical_tasks=historical,
        )
        requires_unique_tmdb_plan = False
        if identity is None and actionable_media_count > 0:
            candidate_categories = {
                str(row.get("category")) for row in candidates
                if isinstance(row, Mapping) and row.get("category") in {"电影", "番剧", "美剧"}
            }
            inherited_category = (
                next(iter(candidate_categories)) if len(candidate_categories) == 1
                else unmatched_media_category if not candidate_categories else None
            )
            if inherited_category is not None:
                identity = {
                    "tmdb_id": None,
                    "media_type": "auto",
                    "category": inherited_category,
                    "target_root": None,
                    "evidence": (
                        "formal_ancestor_category_only"
                        if candidate_categories else "operator_default_category_unresolved"
                    ),
                }
                resolution = str(identity["evidence"])
                requires_unique_tmdb_plan = True
        formal_complete = bool(identity and identity.get("formal_complete") is True)
        formal_gap_count = int((identity or {}).get("formal_gap_count") or 0)
        formal_target = (identity or {}).get("target_root")
        formal_gaps = list(
            (identity or {}).get("formal_gaps")
            or (identity or {}).get("video_gaps")
            or []
        )
        actions: list[dict[str, Any]] = []
        if file_count == 0:
            disposition = "cleanup_empty_input_review"
            actions.append(_inbox_action(
                "review_empty_input_cleanup", source,
                execution="evidence_gated",
                required_evidence=["completed_commit_journal", "clean_formal_reaudit"],
            ))
        elif partial_transfer_count > 0 and video_count == 0:
            disposition = "partial_media_transfer_review"
            actions.append(_inbox_action(
                "review_partial_media_transfer", source,
                partial_transfer_count=partial_transfer_count,
                matched_target=formal_target,
                requires_later_review=True,
                execution="wait_for_transfer_or_quarantine_with_evidence",
            ))
        elif video_count == 0 and archive_count > verified_media_archive_count:
            disposition = "archive_media_verification"
            actions.append(_inbox_action(
                "inspect_archive_media_members", source,
                archive_count=archive_count,
                verified_media_archive_count=verified_media_archive_count,
                requires_later_review=True,
                execution="read_only_member_inventory_before_task_creation",
            ))
        elif actionable_media_count == 0:
            disposition = "non_media_input_review"
            actions.append(_inbox_action(
                "classify_non_media_input", source,
                subtitle_count=subtitle_count,
                matched_target=formal_target,
                requires_later_review=True,
            ))
        elif identity is not None and formal_complete:
            disposition = "cleanup_duplicate_input"
            actions.append(_inbox_action(
                "cleanup_duplicate_input_directory", source,
                target_root=formal_target,
                execution="evidence_gated",
                required_evidence=[
                    "formal_library_still_complete", "input_media_proven_duplicate",
                    "exact_inventory_recheck",
                ],
                delete_unproven_files=False,
            ))
            if actionable_media_count > 0:
                actions.append(_inbox_action(
                    "safe_merge_uncovered_media", source,
                    target_root=formal_target,
                    tmdb_id=identity.get("tmdb_id"),
                    execution="only_if_duplicate_check_finds_uncovered_media",
                    policy={
                        "overwrite_existing": False,
                        "require_exact_work_identity": True,
                        "require_episode_or_movie_mapping": True,
                        "journal_and_reaudit": True,
                    },
                ))
        elif identity is not None and formal_gap_count > 0:
            disposition = "replenish_existing_work"
            actions.append(_inbox_action(
                "replenish_existing_work", source,
                target_root=formal_target,
                tmdb_id=identity.get("tmdb_id"),
                gaps=formal_gaps,
                gap_digest=canonical_digest(formal_gaps),
                review_state="actionable",
            ))
            if actionable_media_count > 0:
                disposition = "merge_and_replenish_existing_work"
                actions.insert(0, _inbox_action(
                    "safe_merge_new_media", source,
                    target_root=formal_target,
                    tmdb_id=identity.get("tmdb_id"),
                    policy={
                        "overwrite_existing": False,
                        "merge_only_mapped_media": True,
                        "retain_unmapped_input": True,
                        "journal_and_reaudit": True,
                    },
                ))
        elif identity is not None and identity.get("formal_locations"):
            disposition = "formal_gap_evidence_review"
            actions.append(_inbox_action(
                "refresh_formal_gap_evidence", source,
                target_root=formal_target,
                requires_later_review=True,
            ))
        elif pending_delete:
            disposition = "pending_delete_review"
            actions.append(_inbox_action(
                "review_pending_delete_input", source, execution="evidence_gated",
            ))
        elif identity is None:
            disposition = "identity_evidence_required"
            actions.append(_inbox_action(
                "resolve_input_identity", source,
                candidates=candidates, evidence_reason=resolution,
                requires_later_review=True,
            ))
        else:
            disposition = (
                "create_or_reuse_identity_resolution_task"
                if requires_unique_tmdb_plan
                else "create_or_reuse_scrape_task"
            )
            actions.append(_inbox_action(
                "create_or_reuse_new_work_scrape", source,
                identity=identity, inventory_fingerprint=fingerprint,
            ))
        idempotency_key = canonical_digest({
            "kind": "inbox_scrape", "source": source,
            "inventory_fingerprint": fingerprint,
            "verified_media_archive_count": verified_media_archive_count,
            "identity": _idempotent_identity(identity),
        })
        existing = [
            task for task in historical if isinstance(task, Mapping)
            and task.get("source") == source
            and task.get("phase") not in {"completed", "recovered", "failed", "cancelled"}
        ]
        rows.append({
            "source": source,
            "file_count": file_count,
            "bytes": total_bytes,
            "video_count": video_count,
            "subtitle_count": subtitle_count,
            "archive_count": archive_count,
            "verified_media_archive_count": verified_media_archive_count,
            "partial_transfer_count": partial_transfer_count,
            "pending_delete": pending_delete,
            "inventory_fingerprint": fingerprint,
            "identity": identity,
            "identity_resolution": resolution,
            "identity_candidates": candidates,
            "idempotency_key": idempotency_key,
            "requires_unique_tmdb_plan": requires_unique_tmdb_plan,
            "disposition": "reuse_existing_task" if existing and (
                disposition.startswith("create_or_reuse_")
                or disposition == "merge_and_replenish_existing_work"
            ) else disposition,
            "existing_task_ids": sorted({str(row.get("id")) for row in existing if row.get("id")}),
            "blocker": None,
            "actions": actions,
            "reconciliation_key": canonical_digest({
                "kind": "formal_work_identity",
                "identity": _strong_work_identity(identity) if identity is not None else None,
            }) if identity is not None and _strong_work_identity(identity) is not None else None,
            "reconciliation_outcome": (
                "formal_identity_complete_cleanup_input"
                if identity is not None and identity.get("formal_complete") is True
                else "formal_identity_has_real_video_gap"
                if identity is not None and int(identity.get("formal_gap_count") or 0) > 0
                else "identity_or_gap_status_unresolved"
            ),
            "reconciliation_action": (
                "cleanup_duplicate_input_directory"
                if identity is not None and identity.get("formal_complete") is True
                else "replenish_existing_work"
                if identity is not None and int(identity.get("formal_gap_count") or 0) > 0
                else "none"
            ),
            "reconciliation_mutation": False,
        })
    by_identity: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        if isinstance(row.get("reconciliation_key"), str):
            by_identity[row["reconciliation_key"]].append(row)
    for group in by_identity.values():
        schedulable = sorted((
            row for row in group
            if str(row.get("disposition") or "").startswith("create_or_reuse_")
            or row.get("disposition") == "merge_and_replenish_existing_work"
            or row.get("disposition") == "reuse_existing_task"
        ), key=lambda row: (
            row.get("disposition") != "reuse_existing_task",
            row["source"].casefold(),
        ))
        if len(schedulable) <= 1:
            continue
        primary = schedulable[0]
        for duplicate in schedulable[1:]:
            duplicate["disposition"] = "coalesced_duplicate_submission"
            duplicate["blocker"] = None
            duplicate["duplicate_of_source"] = primary["source"]
            duplicate["reconciliation_outcome"] = "deduplicated_same_work_submission"
            duplicate["reconciliation_action"] = "coalesce_duplicate_submission"
            duplicate["actions"] = [_inbox_action(
                "coalesce_duplicate_submission", duplicate["source"],
                primary_source=primary["source"],
                reconciliation_key=duplicate["reconciliation_key"],
            )]
    sorted_rows = sorted(rows, key=lambda row: row["source"].casefold())
    return {
        "schema_version": 1,
        "kind": "inbox_discovery_dry_run",
        "mutation": False,
        "scope": {"inbox": INBOX_ROOT, "formal_roots": list(FORMAL_ROOTS), "excluded": [SYSTEM_ROOT]},
        "inventory_digest": canonical_digest(sorted_rows),
        "source_count": len(rows),
        "schedulable_count": sum(
            row["disposition"].startswith("create_or_reuse_")
            or row["disposition"] in {"merge_and_replenish_existing_work", "reuse_existing_task"}
            for row in rows
        ),
        "cleanup_review_count": sum(
            row["disposition"] in {"cleanup_empty_input_review", "cleanup_duplicate_input"}
            for row in rows
        ),
        "blocked_count": 0,
        "completed_identity_cleanup_count": sum(
            row["disposition"] == "cleanup_duplicate_input" for row in rows
        ),
        "true_gap_count": sum(
            row["reconciliation_outcome"] == "formal_identity_has_real_video_gap"
            and (str(row["disposition"]).startswith("create_or_reuse_")
                 or row["disposition"] in {"merge_and_replenish_existing_work", "reuse_existing_task"})
            for row in rows
        ),
        "duplicate_identity_count": sum(
            row["disposition"] == "coalesced_duplicate_submission" for row in rows
        ),
        "action_count": sum(len(row["actions"]) for row in rows),
        "later_review_count": sum(
            any(action.get("requires_later_review") is True for action in row["actions"])
            for row in rows
        ),
        "sources": sorted_rows,
    }


def _project_gaps(project: Mapping[str, Any], field: str) -> list[Any]:
    rows = project.get(field, [])
    if rows is None:
        return []
    if not isinstance(rows, list):
        raise ValueError(f"实时审计 {field} 必须是数组")
    return rows


def build_one_time_library_plan(
    live_audit: Mapping[str, Any],
    *,
    confirmed_subtitle_gaps: Iterable[Any] = (),
    subtitle_verification_items: Iterable[Any] = (),
) -> dict[str, Any]:
    """Plan one explicit full-library worklist: video, S00, subtitles and metadata.

    Subtitle rows whose embedded stream could not be inspected are deliberately
    kept separate from confirmed gaps.  They remain verification actions in
    the one-time plan, but never inflate the missing-subtitle count.
    """
    projects = live_audit.get("projects")
    if not isinstance(projects, list):
        raise ValueError("实时审计缺少 projects")
    lanes: list[dict[str, Any]] = []
    project_targets: set[str] = set()
    identity_by_target: dict[str, dict[str, Any]] = {}
    title_catalog: list[dict[str, Any]] = []
    for project in projects:
        if not isinstance(project, Mapping):
            continue
        target = project.get("target_root")
        try:
            category = formal_category(target)
        except ValueError:
            continue
        target = _normalize_path(target)
        project_targets.add(target)
        tmdb_ids = project.get("tmdb_ids")
        exact_tmdb_id = (
            tmdb_ids[0]
            if isinstance(tmdb_ids, list)
            and len(tmdb_ids) == 1
            and type(tmdb_ids[0]) is int
            and tmdb_ids[0] > 0
            else None
        )
        identity = {
            "status": "exact" if exact_tmdb_id is not None else "ambiguous",
            "tmdb_id": exact_tmdb_id,
            "media_type": (
                "movie" if project.get("media_type") == "movie" else "tv"
            ),
            "title": str(
                project.get("official_title") or project.get("title") or ""
            ).strip(),
        }
        identity["identity_sha256"] = canonical_digest(identity)
        identity_by_target[target] = identity
        catalog_row: dict[str, Any] = {
            "target_root": target, "category": category, "identity": identity,
        }
        if identity["media_type"] == "movie":
            movie_scope_blockers: list[dict[str, Any]] = []
            raw_videos = project.get("video_files")
            video_path: str | None = None
            if (
                not isinstance(raw_videos, list)
                or len(raw_videos) != 1
                or not isinstance(raw_videos[0], str)
            ):
                movie_scope_blockers.append({
                    "reason": "movie_member_video_identity_ambiguous",
                    "video_count": len(raw_videos) if isinstance(raw_videos, list) else None,
                })
            else:
                try:
                    video_path = _normalize_path(raw_videos[0])
                except ValueError:
                    movie_scope_blockers.append({
                        "reason": "movie_member_video_path_invalid",
                    })
                else:
                    extension = posixpath.splitext(video_path)[1].casefold()
                    if (
                        extension not in VIDEO_EXTENSIONS
                        or posixpath.splitext(video_path)[0] != target
                        or posixpath.dirname(video_path) != posixpath.dirname(target)
                    ):
                        movie_scope_blockers.append({
                            "reason": "movie_member_video_stem_mismatch",
                            "video_path": video_path,
                        })
            raw_issues = project.get("issues")
            if not isinstance(raw_issues, list):
                movie_scope_blockers.append({
                    "reason": "movie_member_issue_evidence_invalid",
                })
                raw_issues = []
            unsafe_issue_codes = sorted({
                str(issue.get("code"))
                for issue in raw_issues if isinstance(issue, Mapping)
                and issue.get("code") in {
                    "movie_nfo_video_pair_mismatch",
                    "missing_or_ambiguous_movie_tmdb_id",
                    "movie_year_tmdb_mismatch",
                }
            })
            if unsafe_issue_codes:
                movie_scope_blockers.append({
                    "reason": "movie_member_identity_issue",
                    "issue_codes": unsafe_issue_codes,
                })
            if identity["status"] != "exact":
                movie_scope_blockers.append({
                    "reason": "movie_member_exact_identity_required",
                })
            if not movie_scope_blockers and video_path is not None:
                catalog_row["movie_member_candidate"] = seal_movie_member_candidate({
                    "category": category,
                    "target_stem": target,
                    "parent_root": posixpath.dirname(target),
                    "video_path": video_path,
                    "nfo_path": target + ".nfo",
                    "tmdb_id": identity["tmdb_id"],
                    "title": identity["title"],
                })
            catalog_row["movie_scope_blockers"] = movie_scope_blockers
        title_catalog.append(catalog_row)
        for lane, field in (("regular_video", "regular_missing"), ("s00_video", "optional_missing")):
            gaps = _project_gaps(project, field)
            if gaps:
                lanes.append({
                    "lane": lane, "target_root": target, "category": category,
                    "identity": identity, "gap_count": len(gaps), "gaps": gaps,
                })
        # Raw semantic-audit ``issues`` also contains informational edition and
        # multipart notices.  Only the audit-report-normalized metadata field is
        # eligible for the explicit one-time worklist.
        metadata = project.get("metadata_issues", [])
        if isinstance(metadata, list) and metadata:
            lanes.append({
                "lane": "metadata", "target_root": target, "category": category,
                "identity": identity, "gap_count": len(metadata), "gaps": metadata,
            })
    subtitle_by_target: dict[str, list[Any]] = defaultdict(list)
    for gap in confirmed_subtitle_gaps:
        if not isinstance(gap, Mapping):
            continue
        target = gap.get("target_root")
        if not isinstance(target, str):
            video_path = gap.get("video_path")
            target = next(
                (root for root in sorted(project_targets, key=len, reverse=True)
                 if isinstance(video_path, str) and _inside(video_path, root)),
                None,
            )
        if target is None:
            continue
        try:
            formal_category(target)
        except ValueError:
            continue
        evidence_values = {
            str(gap.get(field) or "")
            for field in ("confirmation", "classification", "status")
        }
        if not evidence_values & {
            "confirmed_missing_chinese", "confirmed_missing",
            "confirmed_missing_chinese_subtitle",
        }:
            continue
        subtitle_by_target[_normalize_path(target)].append(dict(gap))
    for target, gaps in subtitle_by_target.items():
        lanes.append({
            "lane": "subtitle", "target_root": target,
            "category": formal_category(target),
            "identity": identity_by_target.get(target, {"status": "missing"}),
            "gap_count": len(gaps), "gaps": gaps,
            "delivery_policy": {
                "allowed": ["external_chinese_sidecar", "verify_existing_embedded_chinese"],
                "forbidden": ["replace_video", "remux_video", "delete_video"],
            },
        })
    verification_by_target: dict[str, list[Any]] = defaultdict(list)
    for item in subtitle_verification_items:
        if not isinstance(item, Mapping):
            continue
        target = item.get("target_root")
        if not isinstance(target, str):
            video_path = item.get("video_path")
            target = next(
                (root for root in sorted(project_targets, key=len, reverse=True)
                 if isinstance(video_path, str) and _inside(video_path, root)),
                None,
            )
        if target is None:
            continue
        try:
            formal_category(target)
        except ValueError:
            continue
        # Only the explicit refinement output may enter this lane.  A generic
        # inventory ``status=gap`` is not evidence of a missing subtitle.
        embedded_probe = item.get("embedded_probe")
        is_pending_verification = bool(item.get("pending_reason")) or (
            isinstance(embedded_probe, Mapping)
            and embedded_probe.get("status") == "probe_failed"
        )
        if not is_pending_verification:
            continue
        verification_by_target[_normalize_path(target)].append(dict(item))
    for target, items in verification_by_target.items():
        lanes.append({
            "lane": "subtitle_verification", "target_root": target,
            "category": formal_category(target),
            "identity": identity_by_target.get(target, {"status": "missing"}),
            "gap_count": 0,
            "item_count": len(items), "items": items,
            "verification_policy": {
                "preferred": "verify_existing_embedded_chinese",
                "fallback": "add_content_verified_external_zh_cn_sidecar",
                "allowed": [
                    "verify_existing_embedded_chinese",
                    "add_content_verified_external_zh_cn_sidecar",
                ],
                "forbidden": ["replace_video", "remux_video", "delete_video"],
            },
        })
    lanes.sort(key=lambda row: (row["target_root"].casefold(), row["lane"]))
    for row in lanes:
        row["idempotency_key"] = canonical_digest({
            "lane": row["lane"], "target_root": row["target_root"],
            "identity": row.get("identity"),
            "work_items": row.get("gaps", row.get("items", [])),
        })
    return {
        "schema_version": 1,
        "kind": "one_time_full_library_completion_plan",
        "mutation": False,
        "formal_roots": list(FORMAL_ROOTS),
        "excluded_roots": [INBOX_ROOT, SYSTEM_ROOT, f"{MEDIA_ROOT}/已刮削"],
        "project_count": len(project_targets),
        "lane_count": len(lanes),
        "gap_count": sum(row["gap_count"] for row in lanes),
        "verification_action_count": sum(
            int(row.get("item_count") or 0)
            for row in lanes if row.get("lane") == "subtitle_verification"
        ),
        "title_catalog": sorted(
            title_catalog, key=lambda row: row["target_root"].casefold(),
        ),
        "lanes": lanes,
    }


def build_one_time_title_batches(
    library_plan: Mapping[str, Any],
) -> dict[str, Any]:
    """Group discovery lanes into inert, exact-title re-audit batches.

    A full-library observation is not permission to acquire or move anything.
    Each batch therefore starts with a fresh read-only audit of exactly one
    formal title root.  Only that fresh evidence may later feed the ordinary
    current-title replenishment path.
    """
    if library_plan.get("kind") != "one_time_full_library_completion_plan":
        raise ValueError("一次性作品批次需要有效的全库观测计划")
    lanes = library_plan.get("lanes")
    if not isinstance(lanes, list):
        raise ValueError("一次性全库观测缺少 lanes")
    catalog = library_plan.get("title_catalog")
    if not isinstance(catalog, list) or not all(isinstance(row, Mapping) for row in catalog):
        raise ValueError("一次性全库观测缺少完整作品身份目录")
    catalog_roots = sorted({
        _normalize_path(row.get("target_root")) for row in catalog
    }, key=str.casefold)
    catalog_by_root: dict[str, dict[str, Any]] = {}
    identity_roots: dict[tuple[str, int], list[str]] = defaultdict(list)
    for row in catalog:
        catalog_root = _normalize_path(row.get("target_root"))
        previous_catalog = catalog_by_root.get(catalog_root)
        if previous_catalog is not None and previous_catalog != dict(row):
            raise ValueError(f"作品身份目录包含冲突记录: {catalog_root}")
        catalog_by_root[catalog_root] = dict(row)
        identity = row.get("identity")
        if not isinstance(identity, Mapping):
            continue
        media_type = identity.get("media_type")
        tmdb_id = identity.get("tmdb_id")
        if media_type in {"tv", "movie"} and type(tmdb_id) is int and tmdb_id > 0:
            identity_roots[(media_type, tmdb_id)].append(
                _normalize_path(row.get("target_root")),
            )

    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    blocked: list[dict[str, Any]] = []
    identity_by_target: dict[str, dict[str, Any]] = {}
    for raw in lanes:
        if not isinstance(raw, Mapping):
            raise ValueError("一次性全库 lane 必须是对象")
        row = dict(raw)
        target = _normalize_path(row.get("target_root"))
        formal_category(target)
        identity = row.get("identity")
        if not isinstance(identity, Mapping):
            blocked.append({
                "target_root": target, "lane": row.get("lane"),
                "reason": "missing_exact_identity",
            })
            continue
        stable_identity = {
            "status": identity.get("status"),
            "tmdb_id": identity.get("tmdb_id"),
            "media_type": identity.get("media_type"),
            "title": identity.get("title"),
        }
        expected_identity_digest = canonical_digest(stable_identity)
        exact = (
            stable_identity["status"] == "exact"
            and type(stable_identity["tmdb_id"]) is int
            and stable_identity["tmdb_id"] > 0
            and stable_identity["media_type"] in {"tv", "movie"}
            and isinstance(stable_identity["title"], str)
            and bool(stable_identity["title"].strip())
            and identity.get("identity_sha256") == expected_identity_digest
        )
        if not exact:
            blocked.append({
                "target_root": target, "lane": row.get("lane"),
                "reason": "missing_exact_identity",
            })
            continue
        previous = identity_by_target.get(target)
        if previous is not None and previous != dict(identity):
            raise ValueError(f"同一作品目录存在冲突身份: {target}")
        identity_by_target[target] = dict(identity)
        grouped[target].append(row)

    lane_priority = {
        "subtitle_verification": 0,
        "subtitle": 1,
        "regular_video": 2,
        "s00_video": 3,
        "metadata": 4,
    }
    batches: list[dict[str, Any]] = []
    for target, title_lanes in grouped.items():
        title_lanes.sort(key=lambda row: (
            lane_priority.get(str(row.get("lane")), 99),
            str(row.get("idempotency_key") or ""),
        ))
        lane_summaries = [{
            "lane": row.get("lane"),
            "observation_count": int(row.get("gap_count") or row.get("item_count") or 0),
            "observation_sha256": canonical_digest(row.get("gaps", row.get("items", []))),
            "lane_idempotency_key": row.get("idempotency_key"),
        } for row in title_lanes]
        identity = identity_by_target[target]
        catalog_row = catalog_by_root.get(target, {})
        batch_core = {
            "target_root": target,
            "category": formal_category(target),
            "identity": identity,
            "discovery_lanes": lane_summaries,
        }
        movie_member_candidate: dict[str, Any] | None = None
        movie_scope_blockers = catalog_row.get("movie_scope_blockers")
        if identity.get("media_type") == "movie":
            if not isinstance(movie_scope_blockers, list):
                movie_scope_blockers = [{
                    "reason": "movie_member_scope_evidence_missing",
                }]
            raw_candidate = catalog_row.get("movie_member_candidate")
            if not movie_scope_blockers and isinstance(raw_candidate, Mapping):
                try:
                    movie_member_candidate = validate_movie_member_candidate(raw_candidate)
                except ValueError as exc:
                    movie_scope_blockers = [{
                        "reason": "movie_member_candidate_invalid",
                        "detail": str(exc),
                    }]
            elif not movie_scope_blockers:
                movie_scope_blockers = [{
                    "reason": "movie_member_scope_evidence_missing",
                }]
            if movie_member_candidate is not None:
                batch_core["movie_member_candidate"] = movie_member_candidate
        nested_title_roots = [
            root for root in catalog_roots
            if root != target and _inside(root, target)
        ]
        identity_key = (str(identity.get("media_type")), int(identity["tmdb_id"]))
        duplicate_identity_roots = sorted(
            set(identity_roots.get(identity_key, [])), key=str.casefold,
        )
        duplicate_identity_roots = (
            duplicate_identity_roots if len(duplicate_identity_roots) > 1 else []
        )
        scope_blockers = []
        if nested_title_roots:
            scope_blockers.append({
                "reason": "nested_title_identity",
                "target_roots": nested_title_roots,
            })
        if duplicate_identity_roots:
            scope_blockers.append({
                "reason": "duplicate_tmdb_identity_roots",
                "target_roots": duplicate_identity_roots,
            })
        if identity.get("media_type") == "movie":
            scope_blockers.extend(
                dict(row) for row in movie_scope_blockers
                if isinstance(row, Mapping)
            )
        batches.append({
            **batch_core,
            "title_work_key": canonical_digest(batch_core),
            "first_action": "fresh_exact_title_read_only_audit",
            "mutation_allowed": False,
            "discovery_is_completion_proof": False,
            "read_only_audit_allowed": not scope_blockers,
            "scope_blockers": scope_blockers,
        })

    batches.sort(key=lambda row: (
        min(
            (lane_priority.get(str(lane.get("lane")), 99)
             for lane in row["discovery_lanes"]),
            default=99,
        ),
        sum(int(lane["observation_count"]) for lane in row["discovery_lanes"]),
        row["target_root"].casefold(),
    ))
    runnable_roots = [
        batch["target_root"] for batch in batches
        if batch["read_only_audit_allowed"] is True
    ]
    for index, left in enumerate(runnable_roots):
        for right in runnable_roots[index + 1:]:
            if _inside(left, right) or _inside(right, left):
                raise ValueError(f"可并行作品复核范围重叠: {left} <-> {right}")
    runnable_movie_members = [
        validate_movie_member_candidate(batch["movie_member_candidate"])
        for batch in batches
        if batch["read_only_audit_allowed"] is True
        and isinstance(batch.get("movie_member_candidate"), Mapping)
    ]
    claimed_paths: dict[str, str] = {}
    for candidate in runnable_movie_members:
        for path in (candidate["video_path"], candidate["nfo_path"]):
            key = unicodedata.normalize("NFKC", path).casefold()
            previous = claimed_paths.get(key)
            if previous is not None and previous != candidate["target_stem"]:
                raise ValueError(
                    f"一次性电影成员范围重叠: {previous} <-> "
                    f"{candidate['target_stem']}"
                )
            claimed_paths[key] = candidate["target_stem"]
    return {
        "schema_version": 1,
        "kind": "one_time_exact_title_reaudit_batches",
        "mutation": False,
        "dispatch_allowed": False,
        "source_library_plan_sha256": canonical_digest(library_plan),
        "title_count": len(batches),
        "blocked_lane_count": len(blocked),
        "blocked_title_scope_count": sum(
            batch["read_only_audit_allowed"] is False for batch in batches
        ),
        "batches": batches,
        "blocked": sorted(
            blocked,
            key=lambda row: (row["target_root"].casefold(), str(row.get("lane"))),
        ),
        "policy": {
            "scope": "one_exact_formal_title_per_batch",
            "first_action": "read_only_reaudit",
            "acquisition_requires_fresh_title_evidence": True,
            "delivery_returns_to_normal_inbox_scrape": True,
            "synthetic_media_journal_forbidden": True,
            "global_coordinator": False,
        },
    }


def dispatch_gate(global_control: Mapping[str, Any]) -> dict[str, Any]:
    paused = global_control.get("paused")
    persistent = global_control.get("persistent") is True
    if paused is True:
        return {
            "allowed": False,
            "reason": "global_pause_active",
            "proof": {"paused": True, "persistent": persistent},
        }
    if paused is not False:
        return {"allowed": False, "reason": "global_control_incomplete", "proof": dict(global_control)}
    return {"allowed": True, "reason": "global_control_open", "proof": dict(global_control)}




def build_cleanup_plan(
    discovery_plan: Mapping[str, Any],
    *,
    task_evidence: Iterable[Any],
    clean_audit_targets: Iterable[str],
) -> dict[str, Any]:
    """Allow only per-source empty-directory cleanup after three proofs.

    No file deletion action is ever emitted.  Problem/extraneous files must be
    routed by the normal journaled pipeline before this planner can approve the
    now-empty source directory.
    """
    evidence_by_source: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for row in task_evidence:
        if not isinstance(row, Mapping):
            continue
        try:
            evidence_by_source[_normalize_path(row.get("source"))].append(row)
        except ValueError:
            continue
    clean_targets = {_normalize_path(value) for value in clean_audit_targets}
    actions: list[dict[str, Any]] = []
    blocked: list[dict[str, Any]] = []
    for source_row in discovery_plan.get("sources", []):
        if not isinstance(source_row, Mapping):
            continue
        source = _normalize_path(source_row.get("source"))
        blockers: list[str] = []
        if source_row.get("file_count") != 0:
            blockers.append("source_not_empty")
        candidates = []
        for evidence in evidence_by_source.get(source, []):
            target = evidence.get("target_root")
            try:
                target = _normalize_path(target)
                formal_category(target)
            except ValueError:
                continue
            journal = evidence.get("journal")
            journal_success = isinstance(journal, Mapping) and journal.get("success") is True
            records = journal.get("records", []) if isinstance(journal, Mapping) else []
            files_committed = any(
                isinstance(record, Mapping)
                and record.get("action") == "files-committed"
                and record.get("status") == "ok"
                for record in records
            )
            if evidence.get("phase") == "completed" and journal_success and files_committed:
                candidates.append((evidence, target))
        if len(candidates) != 1:
            blockers.append("exactly_one_completed_success_journal_required")
        elif candidates[0][1] not in clean_targets:
            blockers.append("post_commit_reaudit_not_clean")
        if blockers:
            blocked.append({"source": source, "blockers": blockers})
            continue
        evidence, target = candidates[0]
        actions.append({
            "action": "remove_empty_directory",
            "source": source,
            "target_root": target,
            "job_id": evidence.get("id"),
            "journal_digest": canonical_digest(evidence["journal"]),
            "audit_target_proof": target,
            "expected_file_count": 0,
            "requires_fresh_revalidation": True,
        })
    return {
        "schema_version": 1,
        "kind": "verified_inbox_cleanup_dry_run",
        "mutation": False,
        "delete_files_allowed": False,
        "action_count": len(actions),
        "blocked_count": len(blocked),
        "actions": sorted(actions, key=lambda row: row["source"].casefold()),
        "blocked": sorted(blocked, key=lambda row: row["source"].casefold()),
    }


def build_one_time_worklist(
    *,
    global_control: Mapping[str, Any],
    inbox_plan: Mapping[str, Any],
    library_plan: Mapping[str, Any],
    cleanup_plan: Mapping[str, Any],
) -> dict[str, Any]:
    gate = dispatch_gate(global_control)
    dispatchable = []
    if gate["allowed"]:
        dispatchable.extend(
            {"kind": "inbox", **row} for row in inbox_plan.get("sources", [])
            if isinstance(row, Mapping)
            and (str(row.get("disposition") or "").startswith("create_or_reuse_")
                 or row.get("disposition") == "merge_and_replenish_existing_work"
                 or row.get("disposition") == "reuse_existing_task")
        )
        dispatchable.extend(
            {"kind": "library", **row} for row in library_plan.get("lanes", [])
            if isinstance(row, Mapping)
        )
        dispatchable.extend(
            {"kind": "cleanup", **row} for row in cleanup_plan.get("actions", [])
            if isinstance(row, Mapping)
        )
    return {
        "schema_version": 1,
        "kind": "one_time_library_completion_worklist",
        "planned_at": datetime.now(timezone.utc).isoformat(),
        "mutation": False,
        "dispatch_gate": gate,
        "dispatchable_count": len(dispatchable),
        "dispatchable": dispatchable,
        "observations": {
            "inbox": inbox_plan,
            "library": library_plan,
            "cleanup": cleanup_plan,
        },
    }


def seal_one_time_worklist(value: Mapping[str, Any]) -> dict[str, Any]:
    """Attach a digest covering the complete persisted one-time worklist."""
    if value.get("kind") != "one_time_library_completion_worklist":
        raise ValueError("无法封存未知的一次性工作清单")
    core = {key: item for key, item in value.items() if key != "worklist_sha256"}
    return {**core, "worklist_sha256": canonical_digest(core)}


def one_time_worklist_is_valid(value: Mapping[str, Any]) -> bool:
    """Verify the persisted worklist envelope before any future dispatch."""
    if value.get("kind") != "one_time_library_completion_worklist":
        return False
    digest = value.get("worklist_sha256")
    if not isinstance(digest, str) or re.fullmatch(r"[0-9a-f]{64}", digest) is None:
        return False
    core = {key: item for key, item in value.items() if key != "worklist_sha256"}
    return digest == canonical_digest(core)
