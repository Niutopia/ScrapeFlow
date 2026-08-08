"""Central routing and library-placement rules."""

from __future__ import annotations

import posixpath
from dataclasses import dataclass


MEDIA_ROOT = "/quark/影视"
UNSCRAPED_ROOT = f"{MEDIA_ROOT}/待刮削"
REPLENISHMENT_ROOT = f"{MEDIA_ROOT}/ScrapeFlow/补源"
CATEGORY_ROOTS = {
    "tv": f"{MEDIA_ROOT}/番剧",
    "series": f"{MEDIA_ROOT}/番剧",
    "us_tv": f"{MEDIA_ROOT}/美剧",
    "movie": f"{MEDIA_ROOT}/电影",
    "collection": f"{MEDIA_ROOT}/电影",
}


def _normalize(path: str) -> str:
    normalized = posixpath.normpath("/" + path.lstrip("/"))
    return normalized.rstrip("/") or "/"


def _within(path: str, parent: str) -> bool:
    path = _normalize(path).casefold()
    parent = _normalize(parent).casefold()
    return path == parent or path.startswith(parent.rstrip("/") + "/")


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


def validate_routing(source_root: str, target_root: str) -> RoutingContext:
    """Validate the one-way intake -> direct category-root contract.

    Synthetic test roots remain supported. As soon as either path enters the
    real `/quark/影视` namespace, both sides must obey the production contract.
    """
    source = _normalize(source_root)
    target = _normalize(target_root)
    production = _within(source, MEDIA_ROOT) or _within(target, MEDIA_ROOT)
    category_root = next(
        (root for root in dict.fromkeys(CATEGORY_ROOTS.values()) if _within(target, root)),
        None,
    )
    if production:
        ordinary_source = source != UNSCRAPED_ROOT and _within(source, UNSCRAPED_ROOT)
        replenishment_source = (
            source != REPLENISHMENT_ROOT and _within(source, REPLENISHMENT_ROOT)
        )
        if not ordinary_source and not replenishment_source:
            raise ValueError(
                "生产源目录必须是待刮削作品或 ScrapeFlow 补源子目录: "
                f"{source}"
            )
        if category_root is None or target == category_root:
            choices = "、".join(dict.fromkeys(CATEGORY_ROOTS.values()))
            raise ValueError(f"生产目标必须直接位于分类根目录的作品子树中（{choices}）: {target}")
        forbidden = f"{MEDIA_ROOT}/已刮削"
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


def placement_for(source_root: str, target_root: str) -> PlacementDecision:
    context = validate_routing(source_root, target_root)
    return PlacementDecision(
        target_root=context.target_root,
        category_root=context.category_root,
        rule=(
            "system_replenishment_to_direct_category"
            if _within(context.source_root, REPLENISHMENT_ROOT)
            else "unscraped_to_direct_category"
            if context.production_library
            else "isolated_roots"
        ),
    )
