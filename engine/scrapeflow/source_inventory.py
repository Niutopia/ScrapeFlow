"""Read-only directory-tree snapshot for Phase-2 boundary analysis.

A SourceNode is a pure in-memory representation of one directory (or file)
that was listed from the provider.  It is constructed from the raw ``AList``
walk output or from a serialised test fixture — no live network I/O occurs
after construction.

All fields are plain Python primitives or tuples of plain primitives so the
tree can be cheaply passed to pure-function analysers without coupling them
to the AList client protocol.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Sequence

from engine.scrapeflow.media_policy import (
    POSTER_EXTENSIONS,
    SUBTITLE_EXTENSIONS,
    TEMPORARY_EXTENSIONS,
    VIDEO_EXTENSIONS,
)


# ---------------------------------------------------------------------------
# Object-type classification
# ---------------------------------------------------------------------------

_VIDEO_EXTS = frozenset(VIDEO_EXTENSIONS)
_SUBTITLE_EXTS = frozenset(SUBTITLE_EXTENSIONS)
_POSTER_EXTS = frozenset(POSTER_EXTENSIONS)
_TEMP_EXTS = frozenset(TEMPORARY_EXTENSIONS)
_ARCHIVE_EXTS = frozenset({".zip", ".rar", ".7z", ".tar", ".gz", ".bz2"})
_NFO_EXTS = frozenset({".nfo", ".xml"})


def classify_object_type(name: str) -> str:
    """Return a canonical object-type string for a filename."""
    suffix = Path(name).suffix.lower()
    if suffix in _VIDEO_EXTS:
        return "video"
    if suffix in _SUBTITLE_EXTS:
        return "subtitle"
    if suffix in _POSTER_EXTS:
        return "poster"
    if suffix in _NFO_EXTS:
        return "nfo"
    if suffix in _ARCHIVE_EXTS:
        return "archive"
    if suffix in _TEMP_EXTS:
        return "temporary"
    return "other"


# ---------------------------------------------------------------------------
# Node types
# ---------------------------------------------------------------------------

@dataclass(frozen=True, slots=True)
class SourceFile:
    """A single file observed in the source tree."""

    path: str
    name: str
    size: int
    object_type: str   # video / subtitle / poster / nfo / archive / temporary / other
    modified: str      # ISO-8601 string or empty


@dataclass(frozen=True, slots=True)
class SourceNode:
    """A directory node in the source tree (recursive)."""

    path: str
    name: str
    files: tuple[SourceFile, ...]          # direct file children
    children: tuple["SourceNode", ...]     # direct directory children
    depth: int                             # 0 = root, 1 = immediate child, …


# ---------------------------------------------------------------------------
# Pure helper functions
# ---------------------------------------------------------------------------

def count_video_files(node: SourceNode) -> int:
    """Count all video files in a SourceNode tree (recursive)."""
    total = sum(1 for f in node.files if f.object_type == "video")
    for child in node.children:
        total += count_video_files(child)
    return total


def count_subtitle_files(node: SourceNode) -> int:
    """Count all subtitle files in a SourceNode tree (recursive)."""
    total = sum(1 for f in node.files if f.object_type == "subtitle")
    for child in node.children:
        total += count_subtitle_files(child)
    return total


def collect_all_files(node: SourceNode) -> list[SourceFile]:
    """Flatten the tree into a list of all SourceFile objects (recursive)."""
    result = list(node.files)
    for child in node.children:
        result.extend(collect_all_files(child))
    return result


def has_only_subtitles(node: SourceNode) -> bool:
    """True when the entire subtree contains only subtitle/nfo/poster files."""
    all_files = collect_all_files(node)
    if not all_files:
        return False
    return all(f.object_type in ("subtitle", "nfo", "poster", "other") for f in all_files)


def direct_video_file_count(node: SourceNode) -> int:
    """Count only the video files directly in this node (non-recursive)."""
    return sum(1 for f in node.files if f.object_type == "video")


# ---------------------------------------------------------------------------
# Construction from AList walk output
# ---------------------------------------------------------------------------

def build_source_inventory(
    walk_rows: Sequence[object],
    root_path: str,
) -> SourceNode:
    """Build a SourceNode tree from the flat ``AList.walk()`` output.

    ``walk_rows`` is the raw sequence of row dicts returned by
    ``alist.walk(root_path)``.  Each row has at least ``name``, ``is_dir``,
    ``full_path``, ``size``, and optionally ``modified``.

    This function never calls any network API; it only re-structures the rows
    that were already fetched.
    """
    from typing import Mapping as _Mapping

    root = root_path.rstrip("/")

    # Build a set of child dicts keyed by their full_path
    dir_children: dict[str, list[dict]] = {}      # parent_path -> [dir children]
    dir_files: dict[str, list[SourceFile]] = {}   # parent_path -> [file children]
    all_dirs: set[str] = {root}

    for row in walk_rows:
        if not isinstance(row, _Mapping):
            continue
        full_path = str(row.get("full_path") or row.get("path") or "").rstrip("/")
        if not full_path:
            continue
        name = str(row.get("name") or Path(full_path).name)
        parent = full_path.rsplit("/", 1)[0] if "/" in full_path else root
        size = int(row.get("size") or 0)
        modified = str(row.get("modified") or "")

        if row.get("is_dir"):
            all_dirs.add(full_path)
            dir_children.setdefault(parent, []).append({
                "path": full_path,
                "name": name,
            })
        else:
            file = SourceFile(
                path=full_path,
                name=name,
                size=size,
                object_type=classify_object_type(name),
                modified=modified,
            )
            dir_files.setdefault(parent, []).append(file)

    def _build(path: str, depth: int) -> SourceNode:
        name = path.rsplit("/", 1)[-1] if "/" in path else path
        child_nodes = tuple(
            _build(child["path"], depth + 1)
            for child in sorted(dir_children.get(path, []), key=lambda c: c["name"])
        )
        files = tuple(
            sorted(dir_files.get(path, []), key=lambda f: f.name)
        )
        return SourceNode(
            path=path,
            name=name,
            files=files,
            children=child_nodes,
            depth=depth,
        )

    return _build(root, 0)


# ---------------------------------------------------------------------------
# Construction from test fixture JSON
# ---------------------------------------------------------------------------

def build_source_inventory_from_fixture(fixture_dict: dict) -> SourceNode:
    """Build a SourceNode tree from a fixture dict (for testing).

    Fixture format::

        {
            "root": "/quark/影视/待刮削/Fate",
            "children": [
                {
                    "name": "空之境界",
                    "is_dir": true,
                    "children": [
                        {"name": "01.mkv", "is_dir": false, "size": 4294967296}
                    ]
                }
            ]
        }
    """
    root_path = fixture_dict["root"]

    def _parse(d: dict, parent_path: str, depth: int) -> SourceNode | SourceFile:
        name = str(d["name"])
        full_path = parent_path.rstrip("/") + "/" + name
        if d.get("is_dir", False):
            child_nodes: list[SourceNode] = []
            file_nodes: list[SourceFile] = []
            for child_d in d.get("children", []):
                result = _parse(child_d, full_path, depth + 1)
                if isinstance(result, SourceNode):
                    child_nodes.append(result)
                else:
                    file_nodes.append(result)
            return SourceNode(
                path=full_path,
                name=name,
                files=tuple(sorted(file_nodes, key=lambda f: f.name)),
                children=tuple(child_nodes),
                depth=depth,
            )
        else:
            return SourceFile(
                path=full_path,
                name=name,
                size=int(d.get("size", 0)),
                object_type=classify_object_type(name),
                modified=str(d.get("modified", "")),
            )

    # Build the root node's direct children
    root_children: list[SourceNode] = []
    root_files: list[SourceFile] = []
    for child_d in fixture_dict.get("children", []):
        result = _parse(child_d, root_path, 1)
        if isinstance(result, SourceNode):
            root_children.append(result)
        else:
            root_files.append(result)

    root_name = root_path.rstrip("/").rsplit("/", 1)[-1]
    return SourceNode(
        path=root_path,
        name=root_name,
        files=tuple(sorted(root_files, key=lambda f: f.name)),
        children=tuple(root_children),
        depth=0,
    )


def load_fixture(fixture_path: Path) -> SourceNode:
    """Load a source_tree.json fixture file and return a SourceNode."""
    raw = json.loads(fixture_path.read_text(encoding="utf-8"))
    return build_source_inventory_from_fixture(raw)
