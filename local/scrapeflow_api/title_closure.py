"""One-job, one-title closure evidence without a global convergence loop.

The signed media plan is the only source of target scope.  This module scans
those exact formal-library roots, refines subtitle evidence with bounded
read-only probes, and returns a self-digested JSON object for the job
directory.  It owns no thread, timer, coordinator, or global state.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import posixpath
from pathlib import PurePosixPath
import re
import secrets
from typing import Any, Callable, Mapping, Sequence

from engine.scraper import plan_from_dict, plan_sha256
from engine.scrapeflow.burned_in_subtitle_ocr import (
    apply_ocr_resolution,
    classify_burned_in_ocr_windows,
)
from engine.scrapeflow.one_time_movie_member_scope import (
    scan_exact_movie_member_subtitle_inventory,
    validate_exact_movie_member_scope,
)
from engine.scrapeflow.one_time_tv_exclusion_scope import (
    path_is_excluded_by_tv_scope,
    scan_exact_nested_root_subtitle_inventory,
    scan_exact_tv_exclusion_subtitle_inventory,
    validate_nested_excluded_roots,
    validate_exact_tv_exclusion_scope,
)
from engine.tools.audit_live_library import scan_subtitle_inventory
from engine.tools.refine_subtitle_audit import (
    classify_subtitle_content,
    extract_remote_text_subtitle_stream,
    probe_remote_subtitle_streams,
    refine_rows,
    text_streams_to_extract,
)

from .validation import TARGET_CATEGORY_PARENTS, canonical_digest


SCHEMA_VERSION = 1
_VIDEO_EXTENSIONS = frozenset({
    ".mkv", ".mp4", ".m4v", ".m2ts", ".ts", ".avi", ".mov", ".webm",
})


class TitleClosureBlocked(RuntimeError):
    """The closure audit stopped because its durable pause gate was unsafe."""


EpisodeGapScanner = Callable[[Mapping[str, Any]], Sequence[Mapping[str, Any]]]
InventoryScanner = Callable[[Any, str], Mapping[str, Any]]
MovieMemberInventoryScanner = Callable[[Any, Mapping[str, Any]], Mapping[str, Any]]
TVExclusionInventoryScanner = Callable[[Any, Mapping[str, Any]], Mapping[str, Any]]
NestedRootInventoryScanner = Callable[
    [Any, str, list[str]], Mapping[str, Any]
]
ExternalPrefixReader = Callable[[Any, str], bytes]
EmbeddedProbe = Callable[[Any, str], Mapping[str, Any]]
EmbeddedTextExtractor = Callable[[Any, str, int], Mapping[str, Any]]
BurnedInOCRProbe = Callable[[Any, str], Mapping[str, Any]]
PauseReader = Callable[[], bool]


def _default_external_prefix_reader(alist: Any, path: str) -> bytes:
    return alist.read_file_prefix(path, max_bytes=128 * 1024)


@dataclass(frozen=True)
class TitleClosureAdapters:
    """I/O boundary for a single closure pass.

    ``scan_episode_gaps`` is deliberately required: stale gaps from the media
    plan can never be mistaken for a current post-placement audit.  The OCR
    adapter is optional; when unavailable, unresolved rows remain pending.
    """

    scan_episode_gaps: EpisodeGapScanner
    scan_inventory: InventoryScanner = scan_subtitle_inventory
    scan_movie_member_inventory: MovieMemberInventoryScanner = (
        scan_exact_movie_member_subtitle_inventory
    )
    scan_tv_exclusion_inventory: TVExclusionInventoryScanner = (
        scan_exact_tv_exclusion_subtitle_inventory
    )
    scan_nested_root_inventory: NestedRootInventoryScanner = (
        scan_exact_nested_root_subtitle_inventory
    )
    read_external_prefix: ExternalPrefixReader = _default_external_prefix_reader
    probe_embedded: EmbeddedProbe = probe_remote_subtitle_streams
    extract_embedded_text: EmbeddedTextExtractor = extract_remote_text_subtitle_stream
    probe_burned_in_ocr: BurnedInOCRProbe | None = None


def _inside(path: str, root: str) -> bool:
    return path == root or path.startswith(root.rstrip("/") + "/")


def _validated_formal_root(value: Any) -> tuple[str, str]:
    if not isinstance(value, str) or not value or "\\" in value:
        raise ValueError("作品目标根目录无效")
    normalized = posixpath.normpath(value)
    if normalized != value or not normalized.startswith("/"):
        raise ValueError(f"作品目标必须是规范化绝对路径: {value!r}")
    if PurePosixPath(normalized).suffix.casefold() in _VIDEO_EXTENSIONS:
        raise ValueError(f"作品目标必须是目录，不是视频文件: {normalized}")
    matches = [
        category
        for category, parent in TARGET_CATEGORY_PARENTS.items()
        if normalized != parent and _inside(normalized, parent)
    ]
    if len(matches) != 1:
        raise ValueError(f"作品目标不在唯一正式媒体分类下: {normalized}")
    category = matches[0]
    return normalized, category


def _movie_root_from_member_target(plan: Any, value: str) -> str:
    """Turn a member-movie destination into its exact directory root.

    Mixed plans may key ``member_movies`` by the exact destination video while
    batch plans key it by the movie directory.  A file-looking key is accepted
    only when the signed file list proves the exact path.
    """
    planned_videos = {
        posixpath.join(item.target_dir.rstrip("/"), item.final_name): item.target_dir
        for item in plan.files
        if item.media_kind == "video"
    }
    if value in planned_videos:
        return planned_videos[value]
    if PurePosixPath(value).suffix.casefold() in _VIDEO_EXTENSIONS:
        raise ValueError(f"电影目标像文件，但签名计划没有对应视频: {value}")
    return value


def _finalize_nested_title_targets(
    candidates: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Validate identity leaves and derive exact exclusions for TV parents."""
    if not candidates:
        raise ValueError("签名媒体计划不包含可审计的确切作品根目录")
    candidates.sort(key=lambda row: row["target_root"].casefold())
    nested_by_parent: dict[str, list[str]] = {}
    for index, left in enumerate(candidates):
        for right in candidates[index + 1:]:
            left_root = left["target_root"]
            right_root = right["target_root"]
            if left_root.casefold() == right_root.casefold():
                raise ValueError(f"多个作品身份指向同一目标根目录: {left_root}")
            if _inside(right_root.casefold(), left_root.casefold()):
                parent, child = left, right
            elif _inside(left_root.casefold(), right_root.casefold()):
                parent, child = right, left
            else:
                continue
            if parent["media_type"] != "tv":
                raise ValueError(
                    f"非 TV 作品不得包含另一独立作品: "
                    f"{parent['target_root']} <-> {child['target_root']}"
                )
            if (
                parent["media_type"] == child["media_type"]
                and parent["tmdb_id"] == child["tmdb_id"]
            ):
                raise ValueError("同一 TMDB 身份不得占用两个嵌套作品根")
            nested_by_parent.setdefault(parent["target_root"], []).append(
                child["target_root"]
            )
    for parent in candidates:
        descendants = nested_by_parent.get(parent["target_root"], [])
        if not descendants:
            parent.pop("excluded_roots", None)
            continue
        # Excluding a nearest independent descendant already excludes all of
        # its descendants.  Keep the boundary non-overlapping and stable.
        nearest = [
            root for root in descendants
            if not any(
                other != root and _inside(root.casefold(), other.casefold())
                for other in descendants
            )
        ]
        parent["excluded_roots"] = validate_nested_excluded_roots(
            parent["target_root"], sorted(nearest, key=str.casefold),
        )
    return candidates


def _validated_signed_closure_targets(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        raise ValueError("签名作品目标必须是数组")
    required = {"media_type", "target_root", "category", "tmdb_id", "title"}
    candidates: list[dict[str, Any]] = []
    supplied_exclusions: dict[str, Any] = {}
    for raw in value:
        if (
            not isinstance(raw, Mapping)
            or not required.issubset(raw)
            or set(raw) - (required | {"excluded_roots"})
        ):
            raise ValueError("签名作品目标字段无效")
        root, category = _validated_formal_root(raw.get("target_root"))
        media_type = raw.get("media_type")
        tmdb_id = raw.get("tmdb_id")
        title = raw.get("title")
        if (
            media_type not in {"tv", "movie"}
            or raw.get("category") != category
            or isinstance(tmdb_id, bool)
            or not isinstance(tmdb_id, int)
            or tmdb_id <= 0
            or not isinstance(title, str)
            or not title.strip()
        ):
            raise ValueError("签名作品目标身份无效")
        candidates.append({
            "media_type": media_type,
            "target_root": root,
            "category": category,
            "tmdb_id": tmdb_id,
            "title": title.strip(),
        })
        if "excluded_roots" in raw:
            supplied_exclusions[root] = raw["excluded_roots"]
    finalized = _finalize_nested_title_targets(candidates)
    for row in finalized:
        root = row["target_root"]
        expected = row.get("excluded_roots")
        if expected is None:
            if root in supplied_exclusions:
                raise ValueError("非父作品不得携带 excluded_roots")
        elif supplied_exclusions.get(root) != expected:
            raise ValueError("父作品 excluded_roots 未精确绑定嵌套作品")
    return finalized


def extract_signed_title_targets(
    media_plan: Mapping[str, Any], approved_plan_sha256: str,
) -> list[dict[str, Any]]:
    """Extract non-overlapping exact work roots from one approved media plan."""
    if not isinstance(media_plan, Mapping):
        raise ValueError("媒体计划必须是对象")
    if not isinstance(approved_plan_sha256, str) or re.fullmatch(
        r"[0-9a-f]{64}", approved_plan_sha256,
    ) is None:
        raise ValueError("媒体计划 digest 无效")
    actual_digest = plan_sha256(media_plan)
    if not secrets.compare_digest(actual_digest, approved_plan_sha256):
        raise ValueError("媒体计划 digest 与已批准计划不一致")

    # Full schema validation prevents an unrecognised field from quietly
    # widening the scope even when the caller supplied a digest for it.
    plan = plan_from_dict(dict(media_plan))
    metadata = plan.metadata
    candidates: list[dict[str, Any]] = []

    def add(media_type: str, root: Any, identity: Mapping[str, Any]) -> None:
        normalized, category = _validated_formal_root(root)
        tmdb_id = identity.get("tmdb_id")
        title = identity.get("title")
        if (
            isinstance(tmdb_id, bool) or not isinstance(tmdb_id, int)
            or tmdb_id <= 0 or not isinstance(title, str) or not title.strip()
        ):
            raise ValueError(f"作品目标缺少唯一 TMDB 身份: {normalized}")
        candidates.append({
            "media_type": media_type,
            "target_root": normalized,
            "category": category,
            "tmdb_id": tmdb_id,
            "title": title.strip(),
        })

    mode = plan.mode
    if mode == "tv":
        add("tv", plan.target_root, metadata)
    elif mode == "movie":
        add("movie", plan.target_root, metadata)
    elif mode == "mixed":
        series_root = str(metadata.get("series_root") or "")
        add("tv", series_root, metadata)
        members = metadata.get("member_movies")
        if isinstance(members, Mapping):
            for raw_target, raw_identity in sorted(members.items()):
                movie_root = _movie_root_from_member_target(plan, str(raw_target))
                add("movie", movie_root, raw_identity)
    elif mode in {"batch", "collection"}:
        tv_members = metadata.get("member_tv")
        if isinstance(tv_members, Mapping):
            for root, identity in sorted(tv_members.items()):
                add("tv", root, identity)
        movie_members = metadata.get("member_movies")
        if isinstance(movie_members, Mapping):
            for raw_target, identity in sorted(movie_members.items()):
                add(
                    "movie",
                    _movie_root_from_member_target(plan, str(raw_target)),
                    identity,
                )
        if mode == "collection" and not candidates:
            add("movie", plan.target_root, metadata)
    else:  # pragma: no cover - plan_from_dict already rejects unknown modes
        raise ValueError(f"不支持的媒体计划模式: {mode}")

    return _finalize_nested_title_targets(candidates)


def _assert_unpaused(pause_active: PauseReader, boundary: str) -> None:
    try:
        value = pause_active()
    except Exception as exc:
        raise TitleClosureBlocked(
            f"pause_state_unavailable:{boundary}",
        ) from exc
    if type(value) is not bool:
        raise TitleClosureBlocked(f"pause_state_invalid:{boundary}")
    if value:
        raise TitleClosureBlocked(f"pause_active:{boundary}")


def _expensive(
    pause_active: PauseReader, stage: str, operation: Callable[[], Any],
) -> Any:
    _assert_unpaused(pause_active, f"before_{stage}")
    try:
        return operation()
    finally:
        _assert_unpaused(pause_active, f"after_{stage}")


def _validated_inventory_payload(raw: Mapping[str, Any]) -> dict[str, Any]:
    """Validate the small, title-scoped contract emitted by the scanner."""
    if raw.get("schema_version") != 1:
        raise ValueError("字幕扫描 schema_version 无效")
    policy = raw.get("subtitle_policy")
    if not isinstance(policy, Mapping):
        raise ValueError("字幕扫描缺少字幕策略")
    required = policy.get("required_languages")
    if not isinstance(required, list) or "zh-CN" not in required:
        raise ValueError("字幕扫描未执行 zh-CN 必需策略")
    raw_rows = raw.get("subtitle_inventory")
    raw_missing = raw.get("missing_subtitles")
    if (
        not isinstance(raw_rows, list)
        or not all(isinstance(row, Mapping) for row in raw_rows)
        or not isinstance(raw_missing, list)
        or not all(isinstance(row, Mapping) for row in raw_missing)
    ):
        raise ValueError("字幕扫描行格式无效")
    rows = [dict(row) for row in raw_rows]
    missing = [dict(row) for row in raw_missing]
    identities: set[tuple[Any, Any, Any]] = set()
    gap_rows: list[dict[str, Any]] = []
    for row in rows:
        identity = (row.get("video_path"), row.get("season"), row.get("episode"))
        if identity in identities:
            raise ValueError("字幕扫描包含重复视频分集身份")
        identities.add(identity)
        if row.get("status") == "gap":
            gap_rows.append(row)
        elif row.get("status") != "external_required_language_present":
            raise ValueError("字幕扫描包含未知状态")
    if sorted(map(canonical_digest, missing)) != sorted(
        map(canonical_digest, gap_rows),
    ):
        raise ValueError("missing_subtitles 与 gap inventory 不一致")
    return {
        "schema_version": 1,
        "library_root": raw.get("library_root"),
        "excluded_roots": raw.get("excluded_roots"),
        "included_roots": raw.get("included_roots"),
        "subtitle_policy": dict(policy),
        "missing_subtitles": sorted(missing, key=canonical_digest),
        "subtitle_inventory": sorted(rows, key=canonical_digest),
    }


def _scoped_inventory(
    root: str,
    raw: Mapping[str, Any],
    *,
    excluded_roots: list[str] | None = None,
) -> dict[str, Any]:
    inventory = _validated_inventory_payload(raw)
    if inventory.get("library_root") != root:
        raise ValueError(f"字幕扫描根目录与签名作品范围不一致: {root}")
    exclusions = excluded_roots or []
    if inventory.get("excluded_roots") != exclusions:
        raise ValueError(f"当前作品扫描的嵌套排除范围不一致: {root}")
    included = inventory.get("included_roots")
    if included != [root]:
        raise ValueError(f"字幕扫描扩大了签名作品范围: {root}")
    for row in inventory["subtitle_inventory"]:
        video_path = str(row.get("video_path") or "")
        target_root = str(row.get("target_root") or "")
        if (
            not _inside(video_path, root)
            or not _inside(target_root, root)
            or any(_inside(video_path, excluded) for excluded in exclusions)
            or any(_inside(target_root, excluded) for excluded in exclusions)
        ):
            raise ValueError(f"字幕扫描返回超出作品范围的路径: {video_path}")
        for field in ("companion_subtitles", "candidate_subtitles"):
            paths = row.get(field, [])
            if not isinstance(paths, list) or any(
                not isinstance(path, str)
                or not _inside(path, root)
                or any(_inside(path, excluded) for excluded in exclusions)
                for path in paths
            ):
                raise ValueError(f"字幕扫描 {field} 超出作品范围: {video_path}")
    return inventory


def _scoped_movie_member_inventory(
    scope_value: Mapping[str, Any], raw: Mapping[str, Any],
) -> dict[str, Any]:
    """Validate the virtual inventory emitted for one exact movie member."""
    scope = validate_exact_movie_member_scope(scope_value)
    inventory = _validated_inventory_payload(raw)
    target_stem = scope["target_stem"]
    member_paths = scope["member_paths"]
    if (
        inventory.get("library_root") != target_stem
        or inventory.get("excluded_roots") != []
        or inventory.get("included_roots") != [target_stem]
        or raw.get("member_paths") != member_paths
        or raw.get("member_paths_sha256") != scope["member_paths_sha256"]
    ):
        raise ValueError("电影字幕 inventory 与精确成员范围不一致")
    rows = inventory["subtitle_inventory"]
    if len(rows) != 1:
        raise ValueError("精确电影成员必须且只能返回一个视频 inventory")
    row = rows[0]
    if (
        row.get("media_type") != "movie"
        or row.get("video_path") != scope["video_path"]
        or row.get("target_root") != target_stem
    ):
        raise ValueError("精确电影 inventory 视频或身份越出成员范围")
    allowed = set(member_paths)
    for field in ("companion_subtitles", "candidate_subtitles"):
        paths = row.get(field, [])
        if not isinstance(paths, list) or any(
            not isinstance(path, str) or path not in allowed
            for path in paths
        ):
            raise ValueError(f"精确电影 inventory {field} 包含兄弟电影文件")
    return {
        **inventory,
        "member_paths": list(member_paths),
        "member_paths_sha256": scope["member_paths_sha256"],
    }


def _scoped_tv_exclusion_inventory(
    scope_value: Mapping[str, Any], raw: Mapping[str, Any],
) -> dict[str, Any]:
    scope = validate_exact_tv_exclusion_scope(scope_value)
    inventory = _validated_inventory_payload(raw)
    root = scope["target_root"]
    if (
        inventory.get("library_root") != root
        or inventory.get("included_roots") != [root]
        or inventory.get("excluded_roots") != scope["excluded_roots"]
    ):
        raise ValueError("TV 字幕 inventory 与封存嵌套排除范围不一致")
    for row in inventory["subtitle_inventory"]:
        video_path = row.get("video_path")
        target_root = row.get("target_root")
        if (
            not isinstance(video_path, str)
            or not _inside(video_path, root)
            or path_is_excluded_by_tv_scope(video_path, scope)
            or not isinstance(target_root, str)
            or not _inside(target_root, root)
        ):
            raise ValueError("TV 字幕 inventory 泄漏了嵌套作品视频")
        for field in ("companion_subtitles", "candidate_subtitles"):
            paths = row.get(field, [])
            if not isinstance(paths, list) or any(
                not isinstance(path, str)
                or not _inside(path, root)
                or path_is_excluded_by_tv_scope(path, scope)
                for path in paths
            ):
                raise ValueError("TV 字幕 inventory 泄漏了嵌套作品字幕")
    return inventory


def _current_episode_gaps(
    target: Mapping[str, Any], scanner: EpisodeGapScanner,
) -> list[dict[str, Any]]:
    raw_rows = scanner(target)
    if not isinstance(raw_rows, Sequence) or isinstance(raw_rows, (str, bytes)):
        raise ValueError("当前作品缺集审计必须返回数组")
    output: list[dict[str, Any]] = []
    for raw in raw_rows:
        if not isinstance(raw, Mapping):
            raise ValueError("当前作品缺集证据必须是对象")
        row = dict(raw)
        if row.get("kind") not in {"missing_episode", "missing_season"}:
            raise ValueError("当前作品缺集证据类型无效")
        declared_root = row.get("target_root")
        if declared_root is not None and declared_root != target["target_root"]:
            raise ValueError("缺集证据与当前作品根目录不一致")
        output.append({**row, "target_root": target["target_root"]})
    unique = {canonical_digest(row): row for row in output}
    return [unique[key] for key in sorted(unique)]


def _safe_mapping_result(value: Any, *, status: str, reason: str) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    return {"status": status, "reason": reason}


def _classified_ocr_result(value: Any) -> dict[str, Any]:
    """Accept host probe evidence, classifying raw windows in the pure engine.

    A host adapter that only extracts frames/OCR lines should return
    ``{"raw_ocr_windows": [...]}``.  The existing combined OCR runner may
    instead return its already-classified, current-policy mapping.
    """
    if not isinstance(value, Mapping):
        return {"status": "pending", "reason": "invalid_ocr_result"}
    raw_windows = value.get("raw_ocr_windows")
    if raw_windows is None:
        return dict(value)
    if not isinstance(raw_windows, list) or not all(
        isinstance(window, Mapping) for window in raw_windows
    ):
        return {"status": "pending", "reason": "invalid_raw_ocr_windows"}
    decision = classify_burned_in_ocr_windows([
        dict(window) for window in raw_windows
    ])
    return {
        **decision,
        "raw_ocr_windows_sha256": canonical_digest(raw_windows),
    }


def _validated_exact_targets(raw_targets: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    targets: list[dict[str, Any]] = []
    for raw in raw_targets:
        if not isinstance(raw, Mapping):
            raise ValueError("一次性作品目标必须是对象")
        if set(raw) != {"media_type", "target_root", "category", "tmdb_id", "title"}:
            raise ValueError("一次性作品目标字段不完整或包含未知字段")
        root, category = _validated_formal_root(raw.get("target_root"))
        if raw.get("category") != category:
            raise ValueError("一次性作品分类与目标根不一致")
        media_type = raw.get("media_type")
        tmdb_id = raw.get("tmdb_id")
        title = raw.get("title")
        if media_type not in {"tv", "movie"}:
            raise ValueError("一次性作品 media_type 必须是 tv 或 movie")
        if (
            isinstance(tmdb_id, bool) or not isinstance(tmdb_id, int)
            or tmdb_id <= 0 or not isinstance(title, str) or not title.strip()
        ):
            raise ValueError("一次性作品缺少唯一 TMDB 身份")
        targets.append({
            "media_type": media_type, "target_root": root,
            "category": category, "tmdb_id": tmdb_id, "title": title.strip(),
        })
    if not targets:
        raise ValueError("一次性作品复核至少需要一个目标")
    targets.sort(key=lambda row: row["target_root"].casefold())
    for index, left in enumerate(targets):
        for right in targets[index + 1:]:
            left_root = left["target_root"].casefold()
            right_root = right["target_root"].casefold()
            if (
                left_root == right_root
                or _inside(left_root, right_root)
                or _inside(right_root, left_root)
            ):
                raise ValueError("一次性作品目标重复或重叠")
    return targets


def build_exact_title_closure_evidence(
    targets: Sequence[Mapping[str, Any]],
    scope_sha256: str,
    *,
    alist: Any,
    pause_active: PauseReader,
    adapters: TitleClosureAdapters,
    audited_at: str | None = None,
) -> dict[str, Any]:
    """Run one explicitly scoped, read-only title pass without a media job."""
    if (
        not isinstance(scope_sha256, str)
        or re.fullmatch(r"[0-9a-f]{64}", scope_sha256) is None
    ):
        raise ValueError("一次性作品范围 digest 无效")
    validated = _validated_exact_targets(targets)
    if not secrets.compare_digest(canonical_digest(validated), scope_sha256):
        raise ValueError("一次性作品范围 digest 不一致")
    return _build_title_closure_evidence_for_targets(
        validated, scope_sha256, alist=alist, pause_active=pause_active,
        adapters=adapters, audited_at=audited_at,
        scope_kind="one_time_exact_title_scope",
    )


def build_exact_movie_member_closure_evidence(
    scopes: Sequence[Mapping[str, Any]],
    scope_sha256: str,
    *,
    alist: Any,
    pause_active: PauseReader,
    adapters: TitleClosureAdapters,
    audited_at: str | None = None,
) -> dict[str, Any]:
    """Run the one-time movie phase over authenticated direct-file members."""
    if (
        not isinstance(scope_sha256, str)
        or re.fullmatch(r"[0-9a-f]{64}", scope_sha256) is None
    ):
        raise ValueError("一次性电影成员范围 digest 无效")
    validated = [validate_exact_movie_member_scope(scope) for scope in scopes]
    if not validated:
        raise ValueError("一次性电影复核至少需要一个精确成员")
    validated.sort(key=lambda row: row["target_stem"].casefold())
    if not secrets.compare_digest(canonical_digest(validated), scope_sha256):
        raise ValueError("一次性电影成员范围 digest 不一致")
    claimed_paths: set[str] = set()
    targets: list[dict[str, Any]] = []
    scopes_by_target: dict[str, dict[str, Any]] = {}
    for scope in validated:
        keys = {path.casefold() for path in scope["member_paths"]}
        if keys & claimed_paths:
            raise ValueError("一次性电影成员路径重叠")
        claimed_paths.update(keys)
        target = {
            "media_type": "movie",
            "target_root": scope["target_stem"],
            "category": scope["category"],
            "tmdb_id": scope["tmdb_id"],
            "title": scope["title"],
        }
        targets.append(target)
        scopes_by_target[scope["target_stem"]] = scope
    return _build_title_closure_evidence_for_targets(
        targets, scope_sha256, alist=alist, pause_active=pause_active,
        adapters=adapters, audited_at=audited_at,
        scope_kind="one_time_exact_movie_member_scope",
        movie_member_scopes=scopes_by_target,
    )


def build_exact_tv_exclusion_closure_evidence(
    scopes: Sequence[Mapping[str, Any]],
    scope_sha256: str,
    *,
    alist: Any,
    pause_active: PauseReader,
    adapters: TitleClosureAdapters,
    audited_at: str | None = None,
) -> dict[str, Any]:
    """Run phase-3 only over TV roots with sealed nested exclusions."""
    if (
        not isinstance(scope_sha256, str)
        or re.fullmatch(r"[0-9a-f]{64}", scope_sha256) is None
    ):
        raise ValueError("一次性 TV 排除范围 digest 无效")
    validated = [validate_exact_tv_exclusion_scope(scope) for scope in scopes]
    if not validated:
        raise ValueError("一次性 TV 排除复核至少需要一个范围")
    validated.sort(key=lambda row: row["target_root"].casefold())
    if not secrets.compare_digest(canonical_digest(validated), scope_sha256):
        raise ValueError("一次性 TV 排除范围 digest 不一致")
    targets = [{
        "media_type": "tv",
        "target_root": scope["target_root"],
        "category": scope["category"],
        "tmdb_id": scope["tmdb_id"],
        "title": scope["title"],
    } for scope in validated]
    scopes_by_target = {scope["target_root"]: scope for scope in validated}
    if len(scopes_by_target) != len(validated):
        raise ValueError("一次性 TV 排除范围目标重复")
    return _build_title_closure_evidence_for_targets(
        targets, scope_sha256, alist=alist, pause_active=pause_active,
        adapters=adapters, audited_at=audited_at,
        scope_kind="one_time_exact_tv_root_with_nested_exclusions",
        tv_exclusion_scopes=scopes_by_target,
    )


def build_title_closure_evidence(
    media_plan: Mapping[str, Any],
    approved_plan_sha256: str,
    *,
    alist: Any,
    pause_active: PauseReader,
    adapters: TitleClosureAdapters,
    audited_at: str | None = None,
) -> dict[str, Any]:
    """Run one signed-media-plan exact-title closure pass."""
    targets = extract_signed_title_targets(media_plan, approved_plan_sha256)
    return _build_title_closure_evidence_for_targets(
        targets, approved_plan_sha256, alist=alist, pause_active=pause_active,
        adapters=adapters, audited_at=audited_at,
        scope_kind="signed_media_plan",
    )


def _build_title_closure_evidence_for_targets(
    targets: list[dict[str, Any]],
    source_scope_sha256: str,
    *,
    alist: Any,
    pause_active: PauseReader,
    adapters: TitleClosureAdapters,
    audited_at: str | None = None,
    scope_kind: str,
    movie_member_scopes: Mapping[str, Mapping[str, Any]] | None = None,
    tv_exclusion_scopes: Mapping[str, Mapping[str, Any]] | None = None,
) -> dict[str, Any]:
    """Shared implementation after the exact title scope is authenticated."""
    _assert_unpaused(pause_active, "entry")

    inventories: dict[str, dict[str, Any]] = {}
    episode_gaps: list[dict[str, Any]] = []
    inventory_rows: list[dict[str, Any]] = []
    missing_rows: list[dict[str, Any]] = []
    for target in targets:
        root = target["target_root"]
        member_scope = (
            movie_member_scopes.get(root)
            if isinstance(movie_member_scopes, Mapping) else None
        )
        tv_exclusion_scope = (
            tv_exclusion_scopes.get(root)
            if isinstance(tv_exclusion_scopes, Mapping) else None
        )
        if member_scope is not None and tv_exclusion_scope is not None:
            raise ValueError("作品不能同时使用电影成员与 TV 排除范围")
        signed_exclusions = target.get("excluded_roots", [])
        if (
            not isinstance(signed_exclusions, list)
            or (signed_exclusions and target["media_type"] != "tv")
        ):
            raise ValueError("签名嵌套排除范围无效")
        if signed_exclusions:
            signed_exclusions = validate_nested_excluded_roots(
                root, signed_exclusions,
            )

        def scan_current_inventory(
            *, member_scope: Mapping[str, Any] | None = member_scope,
            root: str = root,
        ) -> Mapping[str, Any]:
            if member_scope is not None:
                return adapters.scan_movie_member_inventory(alist, member_scope)
            if tv_exclusion_scope is not None:
                return adapters.scan_tv_exclusion_inventory(alist, tv_exclusion_scope)
            if signed_exclusions:
                return adapters.scan_nested_root_inventory(
                    alist, root, signed_exclusions,
                )
            return adapters.scan_inventory(alist, root)

        raw_inventory = _expensive(
            pause_active,
            f"subtitle_inventory:{root}",
            scan_current_inventory,
        )
        if not isinstance(raw_inventory, Mapping):
            raise ValueError("字幕扫描未返回对象")
        inventory = (
            _scoped_movie_member_inventory(member_scope, raw_inventory)
            if member_scope is not None
            else _scoped_tv_exclusion_inventory(tv_exclusion_scope, raw_inventory)
            if tv_exclusion_scope is not None
            else _scoped_inventory(
                root, raw_inventory, excluded_roots=signed_exclusions,
            )
        )
        inventories[root] = inventory
        inventory_rows.extend(inventory["subtitle_inventory"])
        # Trailers and other unnumbered title extras are useful inventory, but
        # they are not part of the work's episode/movie subtitle completion
        # contract. Requiring Chinese sidecars for them would keep an
        # otherwise complete title in an endless supplement loop.
        missing_rows.extend(
            row for row in inventory["missing_subtitles"]
            if row.get("media_type") in {"tv", "movie"}
        )
        if member_scope is None:
            episode_gaps.extend(_expensive(
                pause_active,
                f"episode_gap_scan:{root}",
                lambda target=target: _current_episode_gaps(
                    target, adapters.scan_episode_gaps,
                ),
            ))

    external_paths = sorted({
        str(path)
        for row in missing_rows
        for path in row.get("companion_subtitles", [])
        if isinstance(path, str) and path
    })
    external_content: dict[str, dict[str, Any]] = {}
    for path in external_paths:
        try:
            payload = _expensive(
                pause_active,
                f"external_subtitle:{path}",
                lambda path=path: adapters.read_external_prefix(alist, path),
            )
            if not isinstance(payload, bytes):
                raise TypeError("external_prefix_not_bytes")
            external_content[path] = classify_subtitle_content(
                payload[:256 * 1024], PurePosixPath(path).suffix,
            )
        except TitleClosureBlocked:
            raise
        except Exception as exc:
            external_content[path] = {
                "status": "undetermined", "reason": type(exc).__name__,
            }

    preliminary = refine_rows(
        missing_rows,
        content_results=external_content,
        probe_results={},
    )
    videos_to_probe = sorted({
        str(row.get("video_path") or "")
        for bucket in ("confirmed_missing_chinese", "pending_review_or_probe")
        for row in preliminary[bucket]
        if row.get("video_path")
    })
    video_probes: dict[str, dict[str, Any]] = {}
    for path in videos_to_probe:
        try:
            value = _expensive(
                pause_active,
                f"embedded_probe:{path}",
                lambda path=path: adapters.probe_embedded(alist, path),
            )
            video_probes[path] = _safe_mapping_result(
                value, status="probe_failed", reason="invalid_probe_result",
            )
        except TitleClosureBlocked:
            raise
        except Exception as exc:
            video_probes[path] = {
                "status": "probe_failed", "error": type(exc).__name__,
            }

    after_probe = refine_rows(
        missing_rows,
        content_results=external_content,
        probe_results=video_probes,
    )
    pending_after_probe = {
        str(row.get("video_path") or "")
        for row in after_probe["pending_review_or_probe"]
        if row.get("video_path")
    }
    stream_content: dict[str, dict[str, Any]] = {}
    for key, (video_path, stream_index) in sorted(
        text_streams_to_extract(video_probes).items(),
    ):
        if video_path not in pending_after_probe:
            continue
        try:
            value = _expensive(
                pause_active,
                f"embedded_text:{key}",
                lambda video_path=video_path, stream_index=stream_index: (
                    adapters.extract_embedded_text(alist, video_path, stream_index)
                ),
            )
            stream_content[key] = _safe_mapping_result(
                value, status="undetermined", reason="invalid_stream_result",
            )
        except TitleClosureBlocked:
            raise
        except Exception as exc:
            stream_content[key] = {
                "status": "undetermined", "reason": type(exc).__name__,
            }

    refined = refine_rows(
        missing_rows,
        content_results=external_content,
        probe_results=video_probes,
        stream_results=stream_content,
    )
    # OCR is intentionally the last and narrowest lane.  Confirmed gaps and
    # already-resolved rows never enter it; duplicate multi-episode rows for
    # one physical video trigger one probe only.
    pending_ocr_paths = sorted({
        str(row.get("video_path") or "")
        for row in refined["pending_review_or_probe"]
        if row.get("video_path")
    })
    ocr_evidence: dict[str, dict[str, Any]] = {}
    if adapters.probe_burned_in_ocr is not None:
        for path in pending_ocr_paths:
            try:
                value = _expensive(
                    pause_active,
                    f"burned_in_ocr:{path}",
                    lambda path=path: adapters.probe_burned_in_ocr(alist, path),
                )
                ocr_evidence[path] = _classified_ocr_result(value)
            except TitleClosureBlocked:
                raise
            except Exception as exc:
                ocr_evidence[path] = {
                    "status": "pending", "reason": type(exc).__name__,
                }
        refined = apply_ocr_resolution(refined, ocr_evidence)

    compliant = [
        {**row, "resolution": "external_required_language_present"}
        for row in inventory_rows
        if row.get("status") == "external_required_language_present"
    ]
    refined["resolved_with_chinese"] = [
        *compliant, *refined["resolved_with_chinese"],
    ]

    unique_episode_gaps = {canonical_digest(row): row for row in episode_gaps}
    episode_gaps = [unique_episode_gaps[key] for key in sorted(unique_episode_gaps)]
    summary = {
        "episode_gap_count": len(episode_gaps),
        "confirmed_subtitle_gap_count": len(refined["confirmed_missing_chinese"]),
        "pending_subtitle_verification_count": len(refined["pending_review_or_probe"]),
    }
    summary["complete"] = all(value == 0 for value in summary.values())
    _assert_unpaused(pause_active, "before_evidence_return")
    core: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "audited_at": audited_at or datetime.now(timezone.utc).isoformat(),
        "source_plan_sha256": source_scope_sha256,
        "source_scope_kind": scope_kind,
        "title_targets": targets,
        "title_targets_sha256": canonical_digest(targets),
        "policy": {
            "scope": (
                "authenticated_exact_movie_members"
                if scope_kind == "one_time_exact_movie_member_scope"
                else "authenticated_exact_title_roots"
            ),
            "required_subtitle_language": "zh-CN",
            "ocr_invocation": "pending_only",
            "remote_mutations": False,
            "global_state": False,
        },
        "subtitle_inventories": inventories,
        "episode_gaps": episode_gaps,
        "subtitle_refinement": refined,
        "probe_evidence": {
            "external_content": external_content,
            "video_probes": video_probes,
            "stream_content": stream_content,
            "burned_in_ocr": ocr_evidence,
        },
        "summary": summary,
    }
    if scope_kind == "one_time_exact_movie_member_scope":
        core["movie_member_scopes"] = [
            dict(movie_member_scopes[root])
            for root in sorted(movie_member_scopes or {}, key=str.casefold)
        ]
    if scope_kind == "one_time_exact_tv_root_with_nested_exclusions":
        core["tv_exclusion_scopes"] = [
            dict(tv_exclusion_scopes[root])
            for root in sorted(tv_exclusion_scopes or {}, key=str.casefold)
        ]
    return {**core, "evidence_sha256": canonical_digest(core)}


def title_closure_evidence_is_valid(value: Mapping[str, Any]) -> bool:
    """Verify the self digest and the three exact completion-gate counts."""
    if not isinstance(value, Mapping) or value.get("schema_version") != SCHEMA_VERSION:
        return False
    evidence_sha256 = value.get("evidence_sha256")
    if not isinstance(evidence_sha256, str):
        return False
    core = {key: item for key, item in value.items() if key != "evidence_sha256"}
    try:
        if not secrets.compare_digest(canonical_digest(core), evidence_sha256):
            return False
        targets = value["title_targets"]
        if (
            not isinstance(targets, list)
            or value.get("title_targets_sha256") != canonical_digest(targets)
        ):
            return False
        if value.get("source_scope_kind") == "signed_media_plan":
            if _validated_signed_closure_targets(targets) != targets:
                return False
        if value.get("source_scope_kind") == "one_time_exact_movie_member_scope":
            scopes = value.get("movie_member_scopes")
            if not isinstance(scopes, list) or not scopes:
                return False
            validated_scopes = [
                validate_exact_movie_member_scope(scope) for scope in scopes
            ]
            if value.get("source_plan_sha256") != canonical_digest(validated_scopes):
                return False
            expected_targets = [{
                "media_type": "movie",
                "target_root": scope["target_stem"],
                "category": scope["category"],
                "tmdb_id": scope["tmdb_id"],
                "title": scope["title"],
            } for scope in validated_scopes]
            if targets != expected_targets:
                return False
        if value.get("source_scope_kind") == "one_time_exact_tv_root_with_nested_exclusions":
            scopes = value.get("tv_exclusion_scopes")
            if not isinstance(scopes, list) or not scopes:
                return False
            validated_scopes = [
                validate_exact_tv_exclusion_scope(scope) for scope in scopes
            ]
            if value.get("source_plan_sha256") != canonical_digest(validated_scopes):
                return False
            expected_targets = [{
                "media_type": "tv",
                "target_root": scope["target_root"],
                "category": scope["category"],
                "tmdb_id": scope["tmdb_id"],
                "title": scope["title"],
            } for scope in validated_scopes]
            if targets != expected_targets:
                return False
        summary = value["summary"]
        refinement = value["subtitle_refinement"]
        expected = {
            "episode_gap_count": len(value["episode_gaps"]),
            "confirmed_subtitle_gap_count": len(
                refinement["confirmed_missing_chinese"]
            ),
            "pending_subtitle_verification_count": len(
                refinement["pending_review_or_probe"]
            ),
        }
        return (
            isinstance(summary, Mapping)
            and all(summary.get(key) == count for key, count in expected.items())
            and type(summary.get("complete")) is bool
            and summary.get("complete") == all(
                count == 0 for count in expected.values()
            )
        )
    except (KeyError, TypeError, ValueError):
        return False
