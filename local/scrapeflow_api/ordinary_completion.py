"""Produce fail-closed completion evidence for one ordinary scrape job.

The signed media plan is the scope authority.  This module performs a fresh,
read-only AList traversal for every exact title root, including directories
which the media planner intentionally ignores (books, comics, extras, fonts),
and emits the artifact consumed by the local scrape-first gate.
"""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timezone
import posixpath
from pathlib import PurePosixPath
import re
import secrets
import unicodedata
from typing import Any, Callable, Mapping, Sequence

from engine.tools.audit_live_library import (
    companion_stem,
    episode_numbers,
    parse_movie_nfo,
    parse_tvshow_nfo,
)
from engine.tools.refine_subtitle_audit import (
    TEXT_SUBTITLE_EXTS,
    classify_subtitle_content,
    probe_remote_subtitle_streams,
)
from engine.scrapeflow.residual_policy import (
    BLOCK_UNKNOWN,
    IMAGE_EXTENSIONS,
    SUBTITLE_EXTENSIONS,
    VIDEO_EXTENSIONS,
    classify_residual,
)
try:
    from engine.scrapeflow.residual_policy import DELETE_AFTER_REMOTE_ROLLBACK
except ImportError:  # Transitional compatibility while Engine rolls forward.
    from engine.scrapeflow.residual_policy import (
        DELETE_AFTER_LOCAL_QUARANTINE as DELETE_AFTER_REMOTE_ROLLBACK,
    )

from .title_closure import (
    extract_signed_title_targets,
    title_closure_evidence_is_valid,
)
from .validation import canonical_digest


SCHEMA_VERSION = 2
ARTWORK_EXTENSIONS = frozenset({".jpg", ".jpeg", ".png", ".webp"})
_SEASON_DIRECTORY_RE = re.compile(r"^Season\s+(\d{1,3})$", re.I)


ContainerProbe = Callable[[Any, str], Mapping[str, Any]]


def _inside(path: str, root: str) -> bool:
    return path == root or path.startswith(root.rstrip("/") + "/")


def _canonical_remote_path(value: Any, *, label: str) -> str:
    if not isinstance(value, str) or not value or "\\" in value:
        raise ValueError(f"{label} 路径无效")
    normalized = posixpath.normpath(value)
    if normalized != value or not normalized.startswith("/"):
        raise ValueError(f"{label} 必须是规范化绝对路径")
    if any(ord(character) < 32 for character in value):
        raise ValueError(f"{label} 包含控制字符")
    return normalized


def _entry_name(value: Any) -> str:
    if (
        not isinstance(value, str) or not value or value in {".", ".."}
        or "/" in value or "\\" in value
        or any(ord(character) < 32 for character in value)
    ):
        raise ValueError("AList 返回了不安全的条目名")
    return value


def _collision_key(value: str) -> str:
    return unicodedata.normalize("NFC", value).casefold()


def _validated_exclusions(root: str, value: Any) -> list[str]:
    if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
        raise ValueError("excluded_roots 必须是路径数组")
    output: list[str] = []
    for raw in value:
        path = _canonical_remote_path(raw, label="excluded_root")
        if path == root or not _inside(path, root):
            raise ValueError("excluded_root 必须是当前作品的严格后代")
        output.append(path)
    if output != sorted(set(output), key=str.casefold):
        raise ValueError("excluded_roots 不是唯一规范顺序")
    return output


def _is_excluded(path: str, exclusions: Sequence[str]) -> bool:
    return any(_inside(path, excluded) for excluded in exclusions)


def _fresh_inventory(
    alist: Any, root: str, exclusions: Sequence[str], *,
    max_directories: int = 10_000, max_files: int = 200_000,
) -> dict[str, Any]:
    """Traverse all descendants with refreshed read-only directory listings."""
    list_directory = getattr(alist, "list", None)
    if not callable(list_directory):
        raise ValueError("AList client 缺少 list 读取能力")
    stack = [root]
    directories: list[str] = []
    files: list[dict[str, Any]] = []
    visited: set[str] = set()
    while stack:
        current = stack.pop()
        if current in visited:
            continue
        if len(visited) >= max_directories:
            raise ValueError("作品目录数超过安全上限")
        visited.add(current)
        directories.append(current)
        entries = list_directory(current, refresh=True)
        if not isinstance(entries, list) or not all(isinstance(row, Mapping) for row in entries):
            raise ValueError("AList list 返回格式无效")
        seen: set[str] = set()
        for raw in entries:
            name = _entry_name(raw.get("name"))
            collision = _collision_key(name)
            if collision in seen:
                raise ValueError(f"AList 目录包含大小写/Unicode 冲突: {current}/{name}")
            seen.add(collision)
            path = f"{current.rstrip('/')}/{name}"
            if _is_excluded(path, exclusions):
                continue
            is_dir = raw.get("is_dir") is True
            if is_dir:
                stack.append(path)
                continue
            size = raw.get("size")
            if isinstance(size, bool) or not isinstance(size, int) or size < 0:
                size = None
            hash_info = raw.get("hash_info")
            hashes = (
                {str(key): str(item) for key, item in sorted(hash_info.items())}
                if isinstance(hash_info, Mapping) else {}
            )
            files.append({"path": path, "size": size, "hash_info": hashes})
            if len(files) > max_files:
                raise ValueError("作品文件数超过安全上限")
    files.sort(key=lambda row: str(row["path"]).casefold())
    directories.sort(key=str.casefold)
    snapshot = {
        "root": root,
        "excluded_roots": list(exclusions),
        "directories": directories,
        "files": files,
    }
    return {**snapshot, "inventory_sha256": canonical_digest(snapshot)}


def _source_departure(alist: Any, source_path: str) -> dict[str, Any]:
    source = _canonical_remote_path(source_path, label="source")
    parent = str(PurePosixPath(source).parent)
    name = PurePosixPath(source).name
    entries = alist.list(parent, refresh=True)
    if not isinstance(entries, list) or not all(isinstance(row, Mapping) for row in entries):
        raise ValueError("AList 源目录列表无效")
    names = sorted(
        (_entry_name(row.get("name")) for row in entries), key=str.casefold,
    )
    witness = {
        "source_parent": parent,
        "source_name": name,
        "entry_names": names,
    }
    return {
        "source_path": source,
        "source_parent": parent,
        "source_name": name,
        "refresh": True,
        "absent_from_unscraped_root": _collision_key(name) not in {
            _collision_key(item) for item in names
        },
        "parent_inventory_sha256": canonical_digest(witness),
    }


def _non_feature_video(path: str) -> bool:
    decision = classify_residual(path)
    return bool(
        decision.action == DELETE_AFTER_REMOTE_ROLLBACK
        and decision.kind in {"theme_or_promo_video", "planner_verified_non_feature"}
    )


def _residual_kind(path: str, *, media_type: str) -> str | None:
    suffix = PurePosixPath(path).suffix.casefold()
    if suffix in VIDEO_EXTENSIONS:
        if _non_feature_video(path):
            return "ncop"
        if media_type == "tv" and episode_numbers(path) is None:
            return "other_non_feature"
        return None
    if suffix in SUBTITLE_EXTENSIONS or suffix == ".nfo":
        return None
    decision = classify_residual(path)
    if suffix in ARTWORK_EXTENSIONS and decision.action == BLOCK_UNKNOWN:
        # Ordinary poster/fanart files are metadata, while the shared policy
        # still catches book/comic scans and advertisements explicitly.
        return None
    if decision.kind == "detached_audio":
        return "detached_audio"
    if decision.kind == "document_or_comic":
        if suffix in {".doc", ".docx", ".odt", ".rtf"}:
            return "docx"
        if suffix in {".cbr", ".cbz", ".djvu"} or re.search(
            r"(?:^|/)(?:comics?|manga|漫画)(?:/|$)", path, re.I,
        ):
            return "manga"
        return "novel"
    if decision.kind == "manga_or_novel_image":
        if re.search(r"(?:^|/)(?:comics?|manga|漫画)(?:/|$)", path, re.I):
            return "manga"
        return "novel"
    if decision.kind == "theme_or_promo_video":
        return "ncop"
    if decision.kind == "external_subtitle_candidate":
        return None
    if decision.action in {DELETE_AFTER_REMOTE_ROLLBACK, BLOCK_UNKNOWN}:
        return "other_non_feature"
    if suffix in IMAGE_EXTENSIONS:
        return None
    if suffix in {".doc", ".docx"}:  # defensive only; shared policy owns this set
        return "docx"
    return "other_non_feature"


def _main_videos(paths: Sequence[str], media_type: str) -> tuple[list[str], dict[str, list[str]]]:
    videos = sorted({
        path for path in paths
        if PurePosixPath(path).suffix.casefold() in VIDEO_EXTENSIONS
        and _residual_kind(path, media_type=media_type) is None
    }, key=str.casefold)
    duplicate_groups: dict[str, list[str]] = {}
    if media_type == "movie" and len(videos) > 1:
        duplicate_groups["movie"] = videos
    elif media_type == "tv":
        by_episode: dict[tuple[int, int], set[str]] = defaultdict(set)
        for path in videos:
            identity = episode_numbers(path)
            if identity is None:
                continue
            season, episodes = identity
            for episode in episodes:
                by_episode[(season, episode)].add(path)
        duplicate_groups = {
            f"S{season:02d}E{episode:02d}": sorted(items, key=str.casefold)
            for (season, episode), items in sorted(by_episode.items())
            if len(items) > 1
        }
    return videos, duplicate_groups


def _direct_season(path: str, root: str) -> int | None:
    relative = PurePosixPath(path).relative_to(PurePosixPath(root))
    if len(relative.parts) != 2:
        return None
    match = _SEASON_DIRECTORY_RE.fullmatch(relative.parts[0])
    return int(match.group(1)) if match else None


def _hierarchy(
    target: Mapping[str, Any], inventory: Mapping[str, Any], videos: Sequence[str],
) -> dict[str, Any]:
    root = str(target["target_root"])
    media_type = str(target["media_type"])
    directories = [str(item) for item in inventory["directories"]]
    file_paths = [str(row["path"]) for row in inventory["files"]]
    nested_tv_nfos = [
        path for path in file_paths
        if PurePosixPath(path).name.casefold() == "tvshow.nfo"
        and str(PurePosixPath(path).parent) != root
    ]
    first_level_dirs = {
        PurePosixPath(path).relative_to(PurePosixPath(root)).parts[0]
        for path in directories if path != root
    }
    if media_type == "tv":
        noncanonical = []
        seasons: set[int] = set()
        for path in videos:
            identity = episode_numbers(path)
            actual = _direct_season(path, root)
            expected = identity[0] if identity is not None else None
            if actual is None or expected != actual:
                noncanonical.append(path)
            elif actual is not None:
                seasons.add(actual)
        unexpected = sorted(
            name for name in first_level_dirs
            if _SEASON_DIRECTORY_RE.fullmatch(name) is None
        )
        output = {
            "hierarchy_kind": "tv_series_season",
            "season_directory_count": len({
                name for name in first_level_dirs
                if _SEASON_DIRECTORY_RE.fullmatch(name) is not None
            }),
            "episode_outside_season_count": len(noncanonical),
        }
    else:
        noncanonical = [
            path for path in videos if str(PurePosixPath(path).parent) != root
        ]
        unexpected = sorted(first_level_dirs)
        output = {
            "hierarchy_kind": "movie_directory",
            "movie_directory_count": 1,
            "nested_title_directory_count": len(first_level_dirs),
        }
    split_count = len(nested_tv_nfos)
    canonical = not unexpected and not noncanonical and split_count == 0
    return {
        "status": "canonical" if canonical else "noncanonical",
        "canonical_root": root,
        "work_tree_count": 1,
        "unexpected_outer_directory_count": len(unexpected),
        "split_same_work_root_count": split_count,
        "noncanonical_path_count": len(noncanonical),
        **output,
    }


def _read_nfo(alist: Any, path: str) -> bytes | None:
    try:
        value = alist.read_file_prefix(path, max_bytes=256 * 1024)
    except Exception:  # Missing/invalid metadata is evidence, not a guessed success.
        return None
    return value if isinstance(value, bytes) else None


def _metadata(
    alist: Any, target: Mapping[str, Any], paths: set[str], videos: Sequence[str],
) -> dict[str, Any]:
    root = str(target["target_root"])
    tmdb_id = target["tmdb_id"]
    media_type = target["media_type"]
    if media_type == "movie":
        missing_nfo: list[str] = []
        present_nfo = 0
        present_artwork = 0
        missing_artwork: list[str] = []
        for video in videos:
            stem = str(PurePosixPath(video).with_suffix(""))
            nfo_path = stem + ".nfo"
            payload = _read_nfo(alist, nfo_path) if nfo_path in paths else None
            try:
                identity_valid = bool(
                    payload is not None
                    and parse_movie_nfo(payload).get("tmdb_ids") == [tmdb_id]
                )
            except (ValueError, TypeError):
                identity_valid = False
            if identity_valid:
                present_nfo += 1
            else:
                missing_nfo.append(nfo_path)
            artwork_candidates = {
                stem + suffix for suffix in ARTWORK_EXTENSIONS
            } | {
                stem + "-poster" + suffix for suffix in ARTWORK_EXTENSIONS
            } | {
                f"{root}/poster{suffix}" for suffix in ARTWORK_EXTENSIONS
            } | {
                f"{root}/folder{suffix}" for suffix in ARTWORK_EXTENSIONS
            }
            if artwork_candidates & paths:
                present_artwork += 1
            else:
                missing_artwork.append(stem + "-poster.jpg")
        return {
            "contract": "movie",
            "movie_nfo_present": bool(videos) and not missing_nfo,
            "required_movie_nfo_count": len(videos),
            "present_movie_nfo_count": present_nfo,
            "movie_poster_present": bool(videos) and not missing_artwork,
            "required_artwork_count": len(videos),
            "present_artwork_count": present_artwork,
            "missing_nfo_paths": missing_nfo,
            "missing_artwork_paths": missing_artwork,
        }

    series_nfo = f"{root}/tvshow.nfo"
    payload = _read_nfo(alist, series_nfo) if series_nfo in paths else None
    try:
        series_valid = bool(
            payload is not None
            and parse_tvshow_nfo(payload).get("tmdb_ids") == [tmdb_id]
        )
    except (ValueError, TypeError):
        series_valid = False
    missing_nfo = []
    present_episode_nfo = 0
    for video in videos:
        nfo_path = str(PurePosixPath(video).with_suffix(".nfo"))
        if nfo_path in paths:
            present_episode_nfo += 1
        else:
            missing_nfo.append(nfo_path)
    if not series_valid:
        missing_nfo.insert(0, series_nfo)
    root_poster = any(
        f"{root}/{name}{suffix}" in paths
        for name in ("poster", "folder") for suffix in ARTWORK_EXTENSIONS
    )
    seasons = sorted({
        season for path in videos
        if (season := _direct_season(path, root)) is not None
    })
    missing_artwork: list[str] = []
    present_season_posters = 0
    for season in seasons:
        candidates = {
            f"{root}/season {season}-poster{suffix}"
            for suffix in ARTWORK_EXTENSIONS
        } | {
            f"{root}/season {season:02d}-poster{suffix}"
            for suffix in ARTWORK_EXTENSIONS
        } | {
            f"{root}/season{season}-poster{suffix}"
            for suffix in ARTWORK_EXTENSIONS
        } | {
            f"{root}/season{season:02d}-poster{suffix}"
            for suffix in ARTWORK_EXTENSIONS
        }
        available = {_collision_key(path) for path in paths}
        if {_collision_key(path) for path in candidates} & available:
            present_season_posters += 1
        else:
            missing_artwork.append(f"{root}/season {season:02d}-poster.jpg")
    if not root_poster:
        missing_artwork.insert(0, f"{root}/poster.jpg")
    return {
        "contract": "tv",
        "series_nfo_present": series_valid,
        "required_episode_nfo_count": len(videos),
        "present_episode_nfo_count": present_episode_nfo,
        "series_poster_present": root_poster,
        "required_season_poster_count": len(seasons),
        "present_season_poster_count": present_season_posters,
        "missing_nfo_paths": missing_nfo,
        "missing_artwork_paths": missing_artwork,
    }


def _closure_video_buckets(closure: Mapping[str, Any]) -> dict[str, str]:
    refinement = closure.get("subtitle_refinement")
    if not isinstance(refinement, Mapping):
        raise ValueError("当前作品字幕精炼证据无效")
    labels = {
        "resolved_with_chinese": "resolved",
        "confirmed_missing_chinese": "missing",
        "pending_review_or_probe": "pending",
    }
    output: dict[str, str] = {}
    for bucket, label in labels.items():
        rows = refinement.get(bucket)
        if not isinstance(rows, list) or not all(isinstance(row, Mapping) for row in rows):
            raise ValueError(f"当前作品 {bucket} 证据无效")
        for row in rows:
            path = _canonical_remote_path(row.get("video_path"), label="video_path")
            previous = output.setdefault(path, label)
            if previous != label:
                raise ValueError("同一视频同时出现在多个字幕结论桶")
    return output


def _internal_status(
    video_path: str, probe: Mapping[str, Any], closure: Mapping[str, Any],
) -> str:
    status = probe.get("status")
    if status == "embedded_chinese":
        return "embedded_chinese"
    if status in {"no_subtitle_stream", "embedded_non_chinese_only"}:
        refinement = closure.get("subtitle_refinement")
        resolved = refinement.get("resolved_with_chinese", []) if isinstance(refinement, Mapping) else []
        resolutions = {
            str(row.get("resolution") or "") for row in resolved
            if isinstance(row, Mapping) and row.get("video_path") == video_path
        }
        if "burned_in_simplified_chinese_ocr_confirmed" in resolutions:
            return "burned_in_chinese"
        return "absent"
    if status == "subtitle_stream_language_unknown":
        refinement = closure.get("subtitle_refinement")
        resolved = refinement.get("resolved_with_chinese", []) if isinstance(refinement, Mapping) else []
        if any(
            isinstance(row, Mapping)
            and row.get("video_path") == video_path
            and row.get("resolution") == "embedded_chinese_content_confirmed"
            for row in resolved
        ):
            return "embedded_chinese"
    return "undetermined"


def _external_evidence(
    alist: Any, path: str, *, container_probe: ContainerProbe,
) -> dict[str, Any]:
    suffix = PurePosixPath(path).suffix.casefold()
    if suffix == ".mks":
        raw = container_probe(alist, path)
        probe = dict(raw) if isinstance(raw, Mapping) else {
            "status": "probe_failed", "error": "invalid_probe_result",
        }
        status = "chinese" if probe.get("status") == "embedded_chinese" else (
            "non_chinese" if probe.get("status") in {
                "no_subtitle_stream", "embedded_non_chinese_only",
            } else "undetermined"
        )
        return {"path": path, "method": "ffprobe_container", "status": status, "probe": probe}
    if suffix in TEXT_SUBTITLE_EXTS:
        try:
            payload = alist.read_file_prefix(path, max_bytes=256 * 1024)
            if not isinstance(payload, bytes):
                raise TypeError("prefix_not_bytes")
            classification = classify_subtitle_content(payload, suffix)
        except Exception as exc:
            classification = {"status": "undetermined", "reason": type(exc).__name__}
        status = classification.get("status")
        return {
            "path": path,
            "method": "text_content",
            "status": "chinese" if status == "chinese" else (
                "non_chinese" if status in {"non_chinese", "japanese"}
                else "undetermined"
            ),
            "classification": classification,
        }
    return {
        "path": path,
        "method": "unsupported_binary",
        "status": "undetermined",
    }


def _subtitles(
    alist: Any, target: Mapping[str, Any], paths: Sequence[str],
    videos: Sequence[str], closure: Mapping[str, Any], *,
    container_probe: ContainerProbe,
) -> dict[str, Any]:
    root = str(target["target_root"])
    subtitle_paths = [
        path for path in paths
        if PurePosixPath(path).suffix.casefold() in SUBTITLE_EXTENSIONS
    ]
    rows: list[dict[str, Any]] = []
    all_external: list[str] = []
    for video in videos:
        stem = str(PurePosixPath(video).with_suffix(""))
        sidecars = sorted({
            path for path in subtitle_paths
            if PurePosixPath(path).parent == PurePosixPath(video).parent
            and companion_stem(path).casefold() == stem.casefold()
        }, key=str.casefold)
        all_external.extend(sidecars)
        raw_probe = container_probe(alist, video)
        probe = dict(raw_probe) if isinstance(raw_probe, Mapping) else {
            "status": "probe_failed", "error": "invalid_probe_result",
        }
        internal = _internal_status(video, probe, closure)
        external = [
            _external_evidence(alist, path, container_probe=container_probe)
            for path in sidecars
        ]
        if internal in {"embedded_chinese", "burned_in_chinese"}:
            chinese_status = "satisfied_internal"
        elif internal == "absent" and len(external) == 1 and external[0]["status"] == "chinese":
            chinese_status = "satisfied_external"
        elif internal == "absent" and all(item["status"] == "non_chinese" for item in external):
            chinese_status = "missing"
        else:
            chinese_status = "pending"
        rows.append({
            "video_path": video,
            "internal_chinese_status": internal,
            "embedded_probe": probe,
            "external_sidecar_count": len(sidecars),
            "external_sidecars": sidecars,
            "external_evidence": external,
            "chinese_status": chinese_status,
        })
    counts = {path: all_external.count(path) for path in set(all_external)}
    duplicate_external = sum(count - 1 for count in counts.values() if count > 1)
    gap_count = sum(row["chinese_status"] in {"missing", "pending"} for row in rows)
    return {
        "video_count": len(rows),
        "videos": rows,
        "chinese_subtitle_gap_count": gap_count,
        "external_sidecar_count": len(all_external),
        "duplicate_external_sidecar_count": duplicate_external,
        "external_sidecars": sorted(all_external, key=str.casefold),
        "scope_root": root,
    }


def _work(
    alist: Any, target: Mapping[str, Any], closure: Mapping[str, Any], *,
    container_probe: ContainerProbe,
) -> dict[str, Any]:
    root = str(target["target_root"])
    exclusions = _validated_exclusions(root, target.get("excluded_roots", []))
    inventory = _fresh_inventory(alist, root, exclusions)
    file_paths = [str(row["path"]) for row in inventory["files"]]
    paths = set(file_paths)
    videos, duplicate_groups = _main_videos(file_paths, str(target["media_type"]))
    residual_items = []
    residual_counts = {
        "novel": 0, "manga": 0, "docx": 0, "ncop": 0,
        "detached_audio": 0, "other_non_feature": 0,
    }
    for path in file_paths:
        kind = _residual_kind(path, media_type=str(target["media_type"]))
        if kind is None:
            continue
        residual_counts[kind] += 1
        decision = classify_residual(path)
        residual_items.append({
            "path": path,
            "kind": kind,
            "policy_action": decision.action,
            "policy_kind": decision.kind,
            "policy_evidence": list(decision.evidence),
        })
    subtitles = _subtitles(
        alist, target, file_paths, videos, closure,
        container_probe=container_probe,
    )
    return {
        "media_type": target["media_type"],
        "target_root": root,
        "tmdb_id": target["tmdb_id"],
        "title": target["title"],
        "excluded_roots": exclusions,
        "inventory": {
            "refresh": True,
            "directory_count": len(inventory["directories"]),
            "file_count": len(inventory["files"]),
            "inventory_sha256": inventory["inventory_sha256"],
        },
        "hierarchy": _hierarchy(target, inventory, videos),
        "media": {
            "main_video_count": len(videos),
            "main_video_paths": videos,
            "duplicate_main_video_count": sum(
                len(items) - 1 for items in duplicate_groups.values()
            ),
            "duplicate_groups": [
                {"identity": identity, "paths": items}
                for identity, items in sorted(duplicate_groups.items())
            ],
        },
        "metadata": _metadata(alist, target, paths, videos),
        "residuals": {**residual_counts, "items": residual_items},
        "subtitles": subtitles,
    }


def build_ordinary_title_completion(
    media_plan: Mapping[str, Any], approved_plan_sha256: str,
    title_closure: Mapping[str, Any], *, source_path: str, alist: Any,
    container_probe: ContainerProbe = probe_remote_subtitle_streams,
    audited_at: str | None = None,
) -> dict[str, Any]:
    """Build one self-digested, signed-plan-bound ordinary completion artifact."""
    targets = extract_signed_title_targets(media_plan, approved_plan_sha256)
    if not title_closure_evidence_is_valid(title_closure):
        raise ValueError("普通刮削验收要求有效的当前作品闭环证据")
    if (
        title_closure.get("source_scope_kind") != "signed_media_plan"
        or title_closure.get("source_plan_sha256") != approved_plan_sha256
        or title_closure.get("title_targets") != targets
        or title_closure.get("title_targets_sha256") != canonical_digest(targets)
    ):
        raise ValueError("当前作品闭环证据与签名计划不一致")
    closure_sha256 = title_closure.get("evidence_sha256")
    if not isinstance(closure_sha256, str) or re.fullmatch(r"[0-9a-f]{64}", closure_sha256) is None:
        raise ValueError("当前作品闭环 digest 无效")
    if not callable(container_probe):
        raise ValueError("container_probe 必须可调用")
    works = [
        _work(alist, target, title_closure, container_probe=container_probe)
        for target in targets
    ]
    summary = {
        "work_count": len(works),
        "main_video_count": sum(work["media"]["main_video_count"] for work in works),
        "duplicate_main_video_count": sum(work["media"]["duplicate_main_video_count"] for work in works),
        "missing_nfo_count": sum(len(work["metadata"]["missing_nfo_paths"]) for work in works),
        "missing_artwork_count": sum(len(work["metadata"]["missing_artwork_paths"]) for work in works),
        "non_feature_residual_count": sum(len(work["residuals"]["items"]) for work in works),
        "chinese_subtitle_gap_count": sum(work["subtitles"]["chinese_subtitle_gap_count"] for work in works),
        "external_sidecar_count": sum(work["subtitles"]["external_sidecar_count"] for work in works),
    }
    core = {
        "schema_version": SCHEMA_VERSION,
        "kind": "ordinary_title_completion",
        "source_plan_sha256": approved_plan_sha256,
        "title_closure_sha256": closure_sha256,
        "title_targets_sha256": canonical_digest(targets),
        "audited_at": audited_at or datetime.now(timezone.utc).isoformat(),
        "policy": {
            "scope": "signed_exact_title_roots",
            "inventory": "fresh_exhaustive_alist_list",
            "embedded_subtitle": "ffprobe_each_main_video",
            "mks_subtitle": "ffprobe_container_not_text_parser",
            "maximum_external_sidecars_per_video_without_internal_chinese": 1,
            "remote_mutations": False,
        },
        "source_departure": _source_departure(alist, source_path),
        "works": works,
        "summary": summary,
    }
    return {**core, "evidence_sha256": canonical_digest(core)}


def ordinary_completion_evidence_is_valid(value: Mapping[str, Any]) -> bool:
    if not isinstance(value, Mapping) or value.get("schema_version") != SCHEMA_VERSION:
        return False
    digest = value.get("evidence_sha256")
    if not isinstance(digest, str) or re.fullmatch(r"[0-9a-f]{64}", digest) is None:
        return False
    core = {key: item for key, item in value.items() if key != "evidence_sha256"}
    return secrets.compare_digest(canonical_digest(core), digest)
