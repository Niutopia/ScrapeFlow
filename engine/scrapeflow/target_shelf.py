"""Stable, backend-owned target-shelf policy for ordinary intake jobs.

The user chooses one *semantic* shelf.  Only this module is allowed to turn
that choice into a formal-library root; HTTP clients, Web code and providers
never submit an arbitrary destination path for an ordinary intake task.
"""

from __future__ import annotations

from enum import Enum
import posixpath


class TargetShelf(str, Enum):
    """The three user-selectable first-level media shelves."""

    MOVIE = "movie"
    ANIME = "anime"
    US_TV = "us_tv"


_SHELF_SEGMENTS: dict[TargetShelf, str] = {
    TargetShelf.MOVIE: "电影",
    TargetShelf.ANIME: "番剧",
    TargetShelf.US_TV: "美剧",
}

_SHELF_LABELS: dict[TargetShelf, str] = {
    TargetShelf.MOVIE: "电影",
    TargetShelf.ANIME: "番剧",
    TargetShelf.US_TV: "美剧",
}

ALLOWED_TARGET_SHELVES: tuple[str, ...] = tuple(shelf.value for shelf in TargetShelf)


def _normal_library_root(value: object) -> str:
    """Normalize a configured library root without accepting path traversal."""
    if not isinstance(value, str) or not value.startswith("/") or "\x00" in value:
        raise ValueError("library_root 必须是规范化绝对路径")
    if "\\" in value:
        raise ValueError("library_root 不得包含反斜杠")
    normalized = posixpath.normpath(value)
    if normalized != value.rstrip("/") or normalized == "/":
        raise ValueError("library_root 必须是非根的规范化绝对路径")
    if any(part in {"", ".", ".."} for part in normalized.split("/")[1:]):
        raise ValueError("library_root 含有不安全的路径段")
    return normalized


def parse_target_shelf(value: object) -> TargetShelf:
    """Accept exactly one stable enum value and reject display labels/paths."""
    if isinstance(value, TargetShelf):
        return value
    if not isinstance(value, str):
        raise ValueError("target_shelf 必须是 movie、anime 或 us_tv")
    try:
        return TargetShelf(value)
    except ValueError as exc:
        raise ValueError("target_shelf 必须是 movie、anime 或 us_tv") from exc


def target_shelf_values() -> tuple[str, ...]:
    """Return the stable API values in display order."""
    return ALLOWED_TARGET_SHELVES


def target_shelf_label(value: TargetShelf | str) -> str:
    """Return a display label without making labels valid API inputs."""
    return _SHELF_LABELS[parse_target_shelf(value)]


def target_root_for_shelf(library_root: str, shelf: TargetShelf | str) -> str:
    """Map one approved shelf to its only permitted first-level root."""
    normalized_root = _normal_library_root(library_root)
    selected = parse_target_shelf(shelf)
    return posixpath.join(normalized_root, _SHELF_SEGMENTS[selected])


def target_shelf_for_root(library_root: str, target_root: object) -> TargetShelf | None:
    """Derive a shelf from an exact legacy category root, if unambiguous.

    This is intentionally a read-only compatibility helper.  It does not
    mutate old job records and it does not infer a shelf from a work directory
    nested below one of the category roots.
    """
    if not isinstance(target_root, str):
        return None
    try:
        root = _normal_library_root(library_root)
    except ValueError:
        return None
    if "\x00" in target_root or "\\" in target_root or not target_root.startswith("/"):
        return None
    normalized = posixpath.normpath(target_root)
    if normalized != target_root.rstrip("/"):
        return None
    for shelf in TargetShelf:
        if normalized == posixpath.join(root, _SHELF_SEGMENTS[shelf]):
            return shelf
    return None


def target_shelf_for_shelf_segment(segment: object) -> TargetShelf | None:
    """Map one exact first-level shelf directory name to its shelf.

    Read-only helper for callers that hold a formal-library work path and
    need the owning shelf semantics (for example per-shelf replenishment
    source policy).  It never accepts partial or fuzzy names.
    """
    if not isinstance(segment, str):
        return None
    for shelf, name in _SHELF_SEGMENTS.items():
        if segment == name:
            return shelf
    return None


__all__ = [
    "ALLOWED_TARGET_SHELVES",
    "TargetShelf",
    "parse_target_shelf",
    "target_root_for_shelf",
    "target_shelf_for_root",
    "target_shelf_for_shelf_segment",
    "target_shelf_label",
    "target_shelf_values",
]
