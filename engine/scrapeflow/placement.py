"""Central routing and library-placement rules."""

from __future__ import annotations

import posixpath
import os
from dataclasses import dataclass


MEDIA_ROOT = "/quark/影视"
UNSCRAPED_ROOT = f"{MEDIA_ROOT}/待刮削"
REPLENISHMENT_ROOT = f"{MEDIA_ROOT}/ScrapeFlow/补源"
EXPANSION_ROOT = f"{MEDIA_ROOT}/ScrapeFlow/展开"
CATEGORY_ROOTS = {
    "tv": f"{MEDIA_ROOT}/番剧",
    "series": f"{MEDIA_ROOT}/番剧",
    "us_tv": f"{MEDIA_ROOT}/欧美剧",
    "movie": f"{MEDIA_ROOT}/电影",
    "collection": f"{MEDIA_ROOT}/电影",
}

def _normalize(path: str) -> str:
    normalized = posixpath.normpath("/" + path.lstrip("/"))
    return normalized.rstrip("/") or "/"


def _validate_media_root(value: object, *, field: str = "media_root") -> str:
    """Validate a configured root without silently collapsing traversal."""
    if not isinstance(value, str) or not value.startswith("/"):
        raise ValueError(f"{field} 必须是绝对路径")
    if "\x00" in value or "\\" in value:
        raise ValueError(f"{field} 含有不安全字符")
    normalized = posixpath.normpath(value)
    if normalized != value or normalized == "/":
        raise ValueError(f"{field} 必须是规范化的非根路径")
    return normalized.rstrip("/")


def _within(path: str, parent: str) -> bool:
    path = _normalize(path).casefold()
    parent = _normalize(parent).casefold()
    return path == parent or path.startswith(parent.rstrip("/") + "/")


def _configured_media_root() -> str:
    """Return the media root contract used by the API composition root."""
    value = os.getenv("SCRAPEFLOW_MEDIA_ROOT", "").strip()
    if not value:
        return MEDIA_ROOT
    configured = _validate_media_root(value, field="SCRAPEFLOW_MEDIA_ROOT")
    if configured != MEDIA_ROOT:
        raise ValueError("SCRAPEFLOW_MEDIA_ROOT 必须是 /quark/影视")
    return MEDIA_ROOT


def _path_media_root(source: str, target: str, explicit: str | None) -> str:
    if explicit is not None:
        return _validate_media_root(explicit)
    return _configured_media_root()


def _category_roots(media_root: str) -> dict[str, str]:
    return {
        "tv": f"{media_root}/番剧",
        "series": f"{media_root}/番剧",
        "us_tv": f"{media_root}/欧美剧",
        "movie": f"{media_root}/电影",
        "collection": f"{media_root}/电影",
    }


@dataclass(frozen=True)
class RoutingContext:
    source_root: str
    target_root: str
    category_root: str | None
    production_library: bool


@dataclass(frozen=True)
class PlacementDecision:
    target_root: str
    category_root: str | None
    rule: str


def validate_routing(
    source_root: str,
    target_root: str,
    *,
    media_root: str | None = None,
) -> RoutingContext:
    """Validate the one-way intake -> direct category-root contract.

    Non-production roots remain supported for focused tests. As soon as either path enters the
    real `/quark/影视` namespace, both sides must obey the production contract.
    """
    source = _normalize(source_root)
    target = _normalize(target_root)
    root = _path_media_root(source, target, media_root)
    category_roots = _category_roots(root)
    unscraped_root = f"{root}/待刮削"
    replenishment_root = f"{root}/ScrapeFlow/补源"
    expansion_root = f"{root}/ScrapeFlow/展开"
    production = _within(source, root) or _within(target, root)
    category_root = next(
        (candidate for candidate in dict.fromkeys(category_roots.values()) if _within(target, candidate)),
        None,
    )
    if production:
        ordinary_source = source != unscraped_root and _within(source, unscraped_root)
        replenishment_source = (
            source != replenishment_root and _within(source, replenishment_root)
        )
        # Task-owned disc-expansion staging is the third legitimate source
        # lane: the bridge stages proven disc payloads under
        # ``ScrapeFlow/展开/<root-task-id>/`` and F plans them into the
        # library exactly like a replenishment lane.
        expansion_source = source != expansion_root and _within(source, expansion_root)
        if not ordinary_source and not replenishment_source and not expansion_source:
            raise ValueError(
                "生产源目录必须是待刮削作品或 ScrapeFlow 补源/展开子目录: "
                f"{source}"
            )
        if category_root is None or target == category_root:
            choices = "、".join(dict.fromkeys(category_roots.values()))
            raise ValueError(f"生产目标必须直接位于分类根目录的作品子树中（{choices}）: {target}")
        forbidden = f"{root}/已刮削"
        if _within(target, forbidden):
            raise ValueError(f"禁止使用已废弃目标目录 {forbidden}: {target}")
        relative_target = posixpath.relpath(target, category_root)
        reserved_segments = {
            segment.casefold()
            for segment in relative_target.split("/")
            if segment not in {"", "."}
        } & {"movies", "specials"}
        if reserved_segments:
            raise ValueError(
                "禁止使用 Movies/Specials 中间目录；系列电影必须直接位于系列根目录，"
                f"TV 特别篇必须位于所属作品 Season 00: {target}"
            )
    if _within(source, target) or _within(target, source):
        raise ValueError(f"源目录与目标目录发生重叠: {source} -> {target}")
    return RoutingContext(source, target, category_root, production)


def placement_for(
    source_root: str,
    target_root: str,
    *,
    media_root: str | None = None,
) -> PlacementDecision:
    """Return a placement decision under one explicit root contract.

    Callers may pass the configured media root explicitly.  The optional
    keyword preserves the two-argument API used by older planner fixtures.
    """
    source = _normalize(source_root)
    target = _normalize(target_root)
    root = _path_media_root(source, target, media_root)
    context = validate_routing(source, target, media_root=root)
    return PlacementDecision(
        target_root=context.target_root,
        category_root=context.category_root,
        rule=(
            "system_replenishment_to_direct_category"
            if _within(context.source_root, root + "/ScrapeFlow/补源")
            else "system_expansion_to_direct_category"
            if _within(context.source_root, root + "/ScrapeFlow/展开")
            else "unscraped_to_direct_category"
            if context.production_library
            else "test_roots"
        ),
    )
