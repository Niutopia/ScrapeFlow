"""Sealed nested-title exclusions for the one-time phase-3 TV audit.

This scope exists only to re-audit a TV root whose predecessor batch was
blocked solely because independently identified titles live below that root.
It does not alter the normal current-title closure contract.
"""

from __future__ import annotations

import hashlib
import json
import posixpath
import re
import secrets
from typing import Any, Mapping
import unicodedata

from engine.scrapeflow.one_time_movie_member_scope import (
    validate_exact_movie_member_scope,
)
from engine.tools.audit_live_library import scan_subtitle_inventory


_FORMAL_CATEGORY_ROOTS = {
    "番剧": "/quark/影视/番剧",
    "美剧": "/quark/影视/美剧",
    "电影": "/quark/影视/电影",
}


def canonical_digest(value: Any) -> str:
    payload = json.dumps(
        value, ensure_ascii=False, sort_keys=True,
        separators=(",", ":"), allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _path(value: Any) -> str:
    if not isinstance(value, str) or not value or "\\" in value:
        raise ValueError("TV 排除范围路径无效")
    normalized = posixpath.normpath(value)
    if normalized != value or not normalized.startswith("/"):
        raise ValueError("TV 排除范围必须是规范化绝对路径")
    return normalized


def _key(value: str) -> str:
    return unicodedata.normalize("NFKC", value).casefold()


def _inside(path: str, root: str) -> bool:
    return path != root and path.startswith(root.rstrip("/") + "/")


def validate_nested_excluded_roots(parent_value: Any, value: Any) -> list[str]:
    """Validate exact, non-overlapping work roots below one signed parent.

    The ordinary signed-title closure and the one-time predecessor flow share
    this path boundary.  The latter adds predecessor digests and direct-file
    movie members; the former only needs canonical directory leaves emitted
    by the approved media plan.
    """
    return _validated_exclusions(_path(parent_value), value)


def path_is_excluded_by_nested_roots(
    path_value: Any,
    *,
    target_root: Any,
    excluded_roots: Any,
) -> bool:
    path = _path(path_value)
    roots = validate_nested_excluded_roots(target_root, excluded_roots)
    return any(path == root or path.startswith(root.rstrip("/") + "/") for root in roots)


class ExactNestedRootExclusionAListView:
    """Read-only view for exact nested directory leaves from a signed plan."""

    def __init__(self, alist: Any, target_root: Any, excluded_roots: Any):
        self._alist = alist
        self.target_root = _path(target_root)
        self.excluded_roots = validate_nested_excluded_roots(
            self.target_root, excluded_roots,
        )

    def walk(self, path: str, **kwargs: Any) -> list[dict[str, Any]]:
        rows = self._alist.walk(path, **kwargs)
        if not isinstance(rows, list) or not all(isinstance(row, Mapping) for row in rows):
            raise ValueError("TV 排除范围 AList walk 返回格式无效")
        output: list[dict[str, Any]] = []
        for raw in rows:
            full_path = raw.get("full_path")
            if not isinstance(full_path, str):
                raise ValueError("TV 排除范围 AList walk 缺少 full_path")
            if not path_is_excluded_by_nested_roots(
                full_path,
                target_root=self.target_root,
                excluded_roots=self.excluded_roots,
            ):
                output.append(dict(raw))
        return output

    def __getattr__(self, name: str) -> Any:
        return getattr(self._alist, name)


def scan_exact_nested_root_subtitle_inventory(
    alist: Any,
    target_root: Any,
    excluded_roots: Any,
) -> dict[str, Any]:
    """Run the standard subtitle audit without crossing independent leaves."""
    root = _path(target_root)
    exclusions = validate_nested_excluded_roots(root, excluded_roots)
    view = ExactNestedRootExclusionAListView(alist, root, exclusions)
    inventory = scan_subtitle_inventory(
        view, root, excluded_roots=exclusions,
    )
    rows = inventory.get("subtitle_inventory") if isinstance(inventory, Mapping) else None
    if not isinstance(rows, list) or any(
        not isinstance(row, Mapping)
        or path_is_excluded_by_nested_roots(
            row.get("video_path"), target_root=root, excluded_roots=exclusions,
        )
        or any(
            path_is_excluded_by_nested_roots(
                path, target_root=root, excluded_roots=exclusions,
            )
            for field in ("companion_subtitles", "candidate_subtitles")
            for path in row.get(field, [])
        )
        for row in rows
    ):
        raise ValueError("TV 字幕 inventory 泄漏了嵌套作品成员")
    return dict(inventory)


def _category(root: str) -> str:
    matches = [
        category for category, parent in _FORMAL_CATEGORY_ROOTS.items()
        if root != parent and root.startswith(parent + "/")
    ]
    if len(matches) != 1:
        raise ValueError("TV 父作品不在唯一正式媒体分类下")
    return matches[0]


def _validated_exclusions(parent: str, value: Any) -> list[str]:
    if not isinstance(value, list) or not value:
        raise ValueError("TV 父作品必须至少有一个嵌套作品排除根")
    roots = [_path(root) for root in value]
    keys = [_key(root) for root in roots]
    if len(keys) != len(set(keys)) or roots != sorted(roots, key=_key):
        raise ValueError("嵌套作品排除根必须唯一且稳定排序")
    if any(not _inside(root, parent) for root in roots):
        raise ValueError("嵌套作品排除根必须严格位于 TV 父根内")
    for index, left in enumerate(roots):
        for right in roots[index + 1:]:
            if _inside(left, right) or _inside(right, left):
                raise ValueError("嵌套作品排除根不得互相包含")
    return roots


def _validated_member_paths(
    nested_roots: list[str], value: Any,
) -> list[str]:
    if not isinstance(value, list):
        raise ValueError("嵌套电影精确成员路径必须是数组")
    paths = [_path(path) for path in value]
    keys = [_key(path) for path in paths]
    if len(keys) != len(set(keys)) or paths != sorted(paths, key=_key):
        raise ValueError("嵌套电影精确成员路径必须唯一且稳定排序")
    if any(not any(
        posixpath.dirname(path) == posixpath.dirname(stem)
        and (
            _key(posixpath.basename(path)) == _key(posixpath.basename(stem))
            or any(
                _key(posixpath.basename(path)).startswith(
                    _key(posixpath.basename(stem)) + separator,
                )
                for separator in (".", "-", "_", " ", "[", "(")
            )
        )
        for stem in nested_roots
    ) for path in paths):
        raise ValueError("嵌套电影成员路径没有归属于 predecessor target stem")
    return paths


def path_is_excluded_by_tv_scope(
    path_value: Any, scope_value: Mapping[str, Any],
) -> bool:
    """Match both real subtrees and direct-file movies stored by stem."""
    scope = validate_exact_tv_exclusion_scope(scope_value)
    path = _path(path_value)
    if any(
        path == root or path.startswith(root.rstrip("/") + "/")
        for root in scope["excluded_roots"]
    ):
        return True
    if _key(path) in {_key(value) for value in scope["excluded_member_paths"]}:
        return True
    # Some ambiguous movies have no phase-2 resolved member set.  Fail closed
    # by excluding direct siblings whose basename is attributable to that
    # predecessor target stem.  This covers video/NFO/subtitle/artwork without
    # treating the whole shared parent as excluded.
    for stem in scope["excluded_roots"]:
        if posixpath.dirname(path) != posixpath.dirname(stem):
            continue
        name = _key(posixpath.basename(path))
        stem_name = _key(posixpath.basename(stem))
        if name == stem_name or any(
            name.startswith(stem_name + separator)
            for separator in (".", "-", "_", " ", "[", "(")
        ):
            return True
    return False


class ExactTVExclusionAListView:
    """Read-only AList view that drops nested subtrees and stem members."""

    def __init__(self, alist: Any, scope_value: Mapping[str, Any]):
        self._alist = alist
        self.scope = validate_exact_tv_exclusion_scope(scope_value)

    def walk(self, path: str, **kwargs: Any) -> list[dict[str, Any]]:
        rows = self._alist.walk(path, **kwargs)
        if not isinstance(rows, list) or not all(isinstance(row, Mapping) for row in rows):
            raise ValueError("TV 排除范围 AList walk 返回格式无效")
        output: list[dict[str, Any]] = []
        for raw in rows:
            full_path = raw.get("full_path")
            if not isinstance(full_path, str):
                raise ValueError("TV 排除范围 AList walk 缺少 full_path")
            if not path_is_excluded_by_tv_scope(full_path, self.scope):
                output.append(dict(raw))
        return output

    def __getattr__(self, name: str) -> Any:
        return getattr(self._alist, name)


def scan_exact_tv_exclusion_subtitle_inventory(
    alist: Any, scope_value: Mapping[str, Any],
) -> dict[str, Any]:
    """Run the standard subtitle scanner through the sealed exclusion view."""
    scope = validate_exact_tv_exclusion_scope(scope_value)
    view = ExactTVExclusionAListView(alist, scope)
    inventory = scan_subtitle_inventory(
        view, scope["target_root"], excluded_roots=scope["excluded_roots"],
    )
    rows = inventory.get("subtitle_inventory") if isinstance(inventory, Mapping) else None
    if not isinstance(rows, list) or any(
        not isinstance(row, Mapping)
        or path_is_excluded_by_tv_scope(row.get("video_path"), scope)
        or any(
            path_is_excluded_by_tv_scope(path, scope)
            for field in ("companion_subtitles", "candidate_subtitles")
            for path in row.get(field, [])
        )
        for row in rows
    ):
        raise ValueError("TV 字幕 inventory 泄漏了嵌套作品成员")
    return dict(inventory)


def validate_exact_tv_exclusion_scope(value: Mapping[str, Any]) -> dict[str, Any]:
    expected = {
        "schema_version", "kind", "target_root", "category", "tmdb_id",
        "title", "excluded_roots", "excluded_roots_sha256",
        "excluded_member_paths", "excluded_member_paths_sha256",
        "predecessor_title_work_key", "predecessor_scope_blockers_sha256",
        "scope_sha256",
    }
    if not isinstance(value, Mapping) or set(value) != expected:
        raise ValueError("TV 嵌套排除范围字段无效")
    if (
        value.get("schema_version") != 1
        or value.get("kind") != "one_time_exact_tv_root_with_nested_exclusions"
    ):
        raise ValueError("TV 嵌套排除范围类型无效")
    target = _path(value.get("target_root"))
    category = _category(target)
    tmdb_id = value.get("tmdb_id")
    title = value.get("title")
    work_key = value.get("predecessor_title_work_key")
    blockers_digest = value.get("predecessor_scope_blockers_sha256")
    if value.get("category") != category:
        raise ValueError("TV 嵌套排除范围分类不一致")
    if (
        isinstance(tmdb_id, bool) or not isinstance(tmdb_id, int) or tmdb_id <= 0
        or not isinstance(title, str) or not title.strip()
        or not isinstance(work_key, str) or re.fullmatch(r"[0-9a-f]{64}", work_key) is None
        or not isinstance(blockers_digest, str)
        or re.fullmatch(r"[0-9a-f]{64}", blockers_digest) is None
    ):
        raise ValueError("TV 嵌套排除范围缺少精确 predecessor 身份")
    exclusions = _validated_exclusions(target, value.get("excluded_roots"))
    member_paths = _validated_member_paths(
        exclusions, value.get("excluded_member_paths"),
    )
    exclusions_digest = value.get("excluded_roots_sha256")
    if not isinstance(exclusions_digest, str) or not secrets.compare_digest(
        exclusions_digest, canonical_digest(exclusions),
    ):
        raise ValueError("TV 嵌套排除根 digest 无效")
    member_paths_digest = value.get("excluded_member_paths_sha256")
    if not isinstance(member_paths_digest, str) or not secrets.compare_digest(
        member_paths_digest, canonical_digest(member_paths),
    ):
        raise ValueError("TV 嵌套电影成员路径 digest 无效")
    core = {
        "schema_version": 1,
        "kind": "one_time_exact_tv_root_with_nested_exclusions",
        "target_root": target,
        "category": category,
        "tmdb_id": tmdb_id,
        "title": title.strip(),
        "excluded_roots": exclusions,
        "excluded_roots_sha256": exclusions_digest,
        "excluded_member_paths": member_paths,
        "excluded_member_paths_sha256": member_paths_digest,
        "predecessor_title_work_key": work_key,
        "predecessor_scope_blockers_sha256": blockers_digest,
    }
    digest = value.get("scope_sha256")
    if not isinstance(digest, str) or not secrets.compare_digest(
        digest, canonical_digest(core),
    ):
        raise ValueError("TV 嵌套排除范围 digest 无效")
    return {**core, "scope_sha256": digest}


def seal_exact_tv_exclusion_scope_from_predecessor_batch(
    batch: Mapping[str, Any],
    *,
    movie_member_scopes: list[Mapping[str, Any]] | None = None,
) -> dict[str, Any]:
    """Derive exclusions only from one sealed predecessor batch blocker."""
    if not isinstance(batch, Mapping):
        raise ValueError("predecessor TV 批次必须是对象")
    identity = batch.get("identity")
    blockers = batch.get("scope_blockers")
    if (
        not isinstance(identity, Mapping)
        or identity.get("status") != "exact"
        or identity.get("media_type") != "tv"
        or batch.get("read_only_audit_allowed") is not False
        or not isinstance(blockers, list)
        or not all(isinstance(row, Mapping) for row in blockers)
    ):
        raise ValueError("predecessor 不是仅待嵌套排除的精确 TV 批次")
    nested = [row for row in blockers if row.get("reason") == "nested_title_identity"]
    other = [row for row in blockers if row.get("reason") != "nested_title_identity"]
    if len(nested) != 1 or other:
        raise ValueError("TV 批次存在同 TMDB/身份歧义或其他 blocker，禁止放宽")
    if set(nested[0]) != {"reason", "target_roots"}:
        raise ValueError("nested_title_identity blocker 字段无效")
    target = _path(batch.get("target_root"))
    category = _category(target)
    tmdb_id = identity.get("tmdb_id")
    title = identity.get("title")
    if (
        batch.get("category") != category
        or isinstance(tmdb_id, bool) or not isinstance(tmdb_id, int) or tmdb_id <= 0
        or not isinstance(title, str) or not title.strip()
    ):
        raise ValueError("predecessor TV 批次身份不精确")
    raw_exclusions = nested[0].get("target_roots")
    if not isinstance(raw_exclusions, list):
        raise ValueError("nested_title_identity target_roots 必须是数组")
    # The predecessor used its own stable Unicode ordering.  Phase 3 binds
    # the exact same set and blocker digest, then canonicalizes that set to
    # this scope's NFKC/casefold order before sealing.
    exclusions = _validated_exclusions(
        target, sorted((_path(root) for root in raw_exclusions), key=_key),
    )
    member_paths: list[str] = []
    seen_movie_stems: set[str] = set()
    for raw_scope in movie_member_scopes or []:
        scope = validate_exact_movie_member_scope(raw_scope)
        stem = scope["target_stem"]
        if stem not in exclusions:
            raise ValueError("phase-2 电影成员范围不属于 predecessor nested targets")
        if stem in seen_movie_stems:
            raise ValueError("同一 nested movie stem 提供了重复 phase-2 范围")
        seen_movie_stems.add(stem)
        member_paths.extend(scope["member_paths"])
    member_paths = sorted(member_paths, key=_key)
    blockers_digest = canonical_digest([dict(row) for row in blockers])
    core = {
        "schema_version": 1,
        "kind": "one_time_exact_tv_root_with_nested_exclusions",
        "target_root": target,
        "category": category,
        "tmdb_id": tmdb_id,
        "title": title.strip(),
        "excluded_roots": exclusions,
        "excluded_roots_sha256": canonical_digest(exclusions),
        "excluded_member_paths": member_paths,
        "excluded_member_paths_sha256": canonical_digest(member_paths),
        "predecessor_title_work_key": batch.get("title_work_key"),
        "predecessor_scope_blockers_sha256": blockers_digest,
    }
    return validate_exact_tv_exclusion_scope({
        **core, "scope_sha256": canonical_digest(core),
    })


def tv_exclusion_scope_matches_batch(
    scope_value: Mapping[str, Any], phase_batch: Mapping[str, Any],
) -> bool:
    """Bind a phase-3 batch back to its predecessor-derived scope."""
    try:
        scope = validate_exact_tv_exclusion_scope(scope_value)
    except (TypeError, ValueError):
        return False
    identity = phase_batch.get("identity") if isinstance(phase_batch, Mapping) else None
    return bool(
        isinstance(identity, Mapping)
        and identity.get("status") == "exact"
        and identity.get("media_type") == "tv"
        and phase_batch.get("target_root") == scope["target_root"]
        and phase_batch.get("category") == scope["category"]
        and identity.get("tmdb_id") == scope["tmdb_id"]
        and identity.get("title") == scope["title"]
    )
