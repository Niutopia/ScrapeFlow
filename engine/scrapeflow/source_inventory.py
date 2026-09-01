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
import math
import posixpath
import unicodedata
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping, Sequence

from engine.scrapeflow.media_policy import (
    AUDIO_EXTENSIONS,
    DISC_IMAGE_EXTENSIONS,
    EXECUTABLE_EXTENSIONS,
    POSTER_EXTENSIONS,
    SUBTITLE_EXTENSIONS,
    TEMPORARY_EXTENSIONS,
    VIDEO_EXTENSIONS,
    classify_filename,
)


# ---------------------------------------------------------------------------
# Object-type classification
# ---------------------------------------------------------------------------

_VIDEO_EXTS = frozenset(VIDEO_EXTENSIONS)
_DISC_IMAGE_EXTS = frozenset(DISC_IMAGE_EXTENSIONS)
_EXECUTABLE_EXTS = frozenset(EXECUTABLE_EXTENSIONS)
_SUBTITLE_EXTS = frozenset(SUBTITLE_EXTENSIONS)
_POSTER_EXTS = frozenset(POSTER_EXTENSIONS)
_TEMP_EXTS = frozenset(TEMPORARY_EXTENSIONS)
_ARCHIVE_EXTS = frozenset({".zip", ".rar", ".7z", ".tar", ".gz", ".bz2"})
_NFO_EXTS = frozenset({".nfo", ".xml"})
_AUDIO_EXTS = frozenset(AUDIO_EXTENSIONS)


def classify_object_type(name: str) -> str:
    """Return a canonical object-type string for a filename."""
    suffix = Path(name).suffix.lower()
    if suffix in _DISC_IMAGE_EXTS:
        return "disc_image"
    if suffix in _EXECUTABLE_EXTS:
        # A font-pack self-extractor (``[Fonts].exe``) is a residual resource,
        # not a disguised-media container.  Boundary analysis must not park the
        # whole source over it; the shared media-policy classifier already has
        # this distinction.
        if classify_filename(name) == "font":
            return "font"
        return "executable"
    if suffix in _VIDEO_EXTS:
        return "video"
    if suffix in _SUBTITLE_EXTS:
        return "subtitle"
    if suffix in _POSTER_EXTS:
        return "poster"
    if suffix in _NFO_EXTS:
        return "nfo"
    if suffix in _AUDIO_EXTS:
        # Detached audio (a soundtrack OST, a CUE track index) is a retained
        # resource; it is neither boundary evidence nor consumable media.
        return "audio"
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
    object_type: str   # video / disc_image / subtitle / poster / nfo / archive / temporary / other
    modified: str      # ISO-8601 string or empty


@dataclass(frozen=True, slots=True)
class SourceNode:
    """A directory node in the source tree (recursive)."""

    path: str
    name: str
    files: tuple[SourceFile, ...]          # direct file children
    children: tuple["SourceNode", ...]     # direct directory children
    depth: int                             # 0 = root, 1 = immediate child, …


class DiscAnalysisProjectionError(ValueError):
    """A verified disc inventory cannot be represented safely for B/W."""


_DISC_BLOCK_BYTES = 2048
_DISC_PROJECTION_KIND = "disc_inventory"


def _has_unsafe_projection_text(value: str) -> bool:
    return any(
        unicodedata.category(character) in {"Cc", "Cf", "Cs"}
        for character in value
    )


def _normalize_projection_path(
    value: object,
    *,
    field_name: str,
    allow_root: bool = False,
) -> str:
    """Validate one already-normalized absolute POSIX projection path."""
    if not isinstance(value, str) or not value:
        raise DiscAnalysisProjectionError(f"{field_name}必须是非空字符串")
    if not value.startswith("/") or "\\" in value:
        raise DiscAnalysisProjectionError(f"{field_name}必须是绝对 POSIX 路径")
    if _has_unsafe_projection_text(value):
        raise DiscAnalysisProjectionError(f"{field_name}不能包含控制或不可见字符")
    if value == "/":
        if allow_root:
            return value
        raise DiscAnalysisProjectionError(f"{field_name}不能是根路径")
    if value.endswith("/") or posixpath.normpath(value) != value:
        raise DiscAnalysisProjectionError(f"{field_name}未规范化")
    if any(part in {"", ".", ".."} for part in value.split("/")[1:]):
        raise DiscAnalysisProjectionError(f"{field_name}包含无效路径段")
    return value


def _projection_collision_key(path: str) -> str:
    return "/".join(
        unicodedata.normalize("NFC", part).casefold()
        for part in path.split("/")
    )


def _projection_descendant(path: str, root: str) -> bool:
    return root == "/" or path.startswith(root + "/")


def _projection_size(value: object, *, field_name: str) -> int:
    if isinstance(value, bool):
        raise DiscAnalysisProjectionError(f"{field_name}必须是非负整数")
    if isinstance(value, int):
        size = value
    elif isinstance(value, str) and value.isdigit():
        size = int(value)
    else:
        raise DiscAnalysisProjectionError(f"{field_name}必须是非负整数")
    if size < 0:
        raise DiscAnalysisProjectionError(f"{field_name}必须是非负整数")
    return size


def _projection_version(value: object) -> str | int | float:
    if isinstance(value, bool) or not isinstance(value, (str, int, float)):
        raise DiscAnalysisProjectionError("backing image version 必须是可序列化标量")
    if isinstance(value, str) and not value.strip():
        raise DiscAnalysisProjectionError("backing image version 不能为空")
    if isinstance(value, float) and not math.isfinite(value):
        raise DiscAnalysisProjectionError("backing image version 必须是有限值")
    return value


def _projection_name(value: object, *, expected: str, field_name: str) -> str:
    name = expected if value in (None, "") else value
    if (
        not isinstance(name, str)
        or not name
        or name in {".", ".."}
        or "/" in name
        or "\\" in name
        or _has_unsafe_projection_text(name)
        or name != expected
    ):
        raise DiscAnalysisProjectionError(f"{field_name}与路径不一致或不安全")
    return name


def _disc_projection_provenance(
    *,
    image_path: str,
    image_size: int,
    image_version: str | int | float,
    disc_kind: str,
    disc_structure: str,
    inner_path: str,
) -> dict[str, object]:
    """Return the immutable physical-source proof copied to every virtual row."""
    return {
        "is_virtual": True,
        "content_expansion": _DISC_PROJECTION_KIND,
        "backing_image_path": image_path,
        "backing_image_size": image_size,
        "backing_image_version": image_version,
        "disc_kind": disc_kind,
        "disc_structure": disc_structure,
        "disc_inner_path": inner_path,
    }


def project_disc_inventories_to_analysis_rows(
    walk_rows: Sequence[object],
    root_path: str,
    inventories: Sequence[object],
    *,
    backing_provenance: Mapping[str, Mapping[str, object]],
) -> list[dict[str, object]]:
    """Project verified optical-disc contents into a pure B/W row snapshot.

    ``walk_rows`` remains the physical provider snapshot and is never mutated.
    For each verified inventory, its one physical image-file row is replaced
    *only in the returned analysis view* by a virtual directory at the same
    path plus the inventory's inner directories/files.  Consequently the
    original rows — not this return value — remain the sole valid input to a
    :class:`SourceManifest`; virtual M2TS/VOB/MPLS rows never become source
    ownership objects.

    ``backing_provenance`` is keyed by ``DiscInventory.image_path`` and must
    provide exact ``size`` and non-empty ``version`` values observed by the
    same read-only probe.  Every virtual row carries that path/size/version so
    a later integration layer can map logical B/W evidence back to the single
    physical ISO/UDF object without inventing a second source object.

    This helper is intentionally domain-neutral: it performs no title,
    season, episode, playlist, acquisition, planning, or writer decisions.
    It only validates and projects an already-proven filesystem inventory.
    """
    root = _normalize_projection_path(
        root_path,
        field_name="analysis root",
        allow_root=True,
    )
    if not isinstance(backing_provenance, Mapping):
        raise DiscAnalysisProjectionError("backing provenance 必须是路径映射")

    # Validate/copy the physical listing first.  Projection is fail-closed on
    # duplicate exact paths and provider-ambiguous case/Unicode spellings.
    physical_rows: list[dict[str, object]] = []
    physical_by_path: dict[str, dict[str, object]] = {}
    physical_collision_paths: dict[str, str] = {}
    for raw_row in walk_rows:
        if not isinstance(raw_row, Mapping):
            raise DiscAnalysisProjectionError("来源 analysis row 必须是映射")
        row = dict(raw_row)
        raw_path = row.get("full_path")
        if raw_path in (None, ""):
            raw_path = row.get("path")
        path = _normalize_projection_path(raw_path, field_name="来源路径")
        if not _projection_descendant(path, root):
            raise DiscAnalysisProjectionError("来源路径不属于 analysis root")
        if path in physical_by_path:
            raise DiscAnalysisProjectionError("来源 rows 包含重复路径")
        collision_key = _projection_collision_key(path)
        collision_path = physical_collision_paths.get(collision_key)
        if collision_path is not None:
            raise DiscAnalysisProjectionError(
                "来源 rows 包含大小写或 Unicode 路径碰撞"
            )
        expected_name = posixpath.basename(path)
        row["name"] = _projection_name(
            row.get("name"),
            expected=expected_name,
            field_name="来源名称",
        )
        row["full_path"] = path
        is_directory = row.get("is_dir") is True
        row["is_dir"] = is_directory
        if is_directory:
            row["size"] = _projection_size(
                row.get("size", 0),
                field_name="来源目录大小",
            )
        else:
            if "size" not in row or row.get("size") is None:
                raise DiscAnalysisProjectionError("来源文件缺少大小")
            row["size"] = _projection_size(
                row.get("size"),
                field_name="来源文件大小",
            )
        if row.get("version") not in (None, ""):
            _projection_version(row.get("version"))
        physical_rows.append(row)
        physical_by_path[path] = row
        physical_collision_paths[collision_key] = path

    ordered_physical_paths = sorted(physical_by_path)
    for index, path in enumerate(ordered_physical_paths[:-1]):
        row = physical_by_path[path]
        if row.get("is_dir") is True:
            continue
        if ordered_physical_paths[index + 1].startswith(path + "/"):
            raise DiscAnalysisProjectionError(
                "来源 rows 把文件同时当作了后代路径的目录"
            )

    provenance_by_path: dict[str, tuple[int, str | int | float]] = {}
    provenance_collision_paths: dict[str, str] = {}
    for raw_path, raw_provenance in backing_provenance.items():
        path = _normalize_projection_path(raw_path, field_name="backing image path")
        if not _projection_descendant(path, root):
            raise DiscAnalysisProjectionError(
                "backing image path 不属于 analysis root"
            )
        if not isinstance(raw_provenance, Mapping):
            raise DiscAnalysisProjectionError("backing provenance 条目必须是映射")
        if "size" not in raw_provenance:
            raise DiscAnalysisProjectionError("backing provenance 缺少 size")
        if "version" not in raw_provenance:
            raise DiscAnalysisProjectionError("backing provenance 缺少 version")
        size = _projection_size(
            raw_provenance.get("size"),
            field_name="backing image size",
        )
        if size <= 0:
            raise DiscAnalysisProjectionError("backing image size 必须大于零")
        version = _projection_version(raw_provenance.get("version"))
        collision_key = _projection_collision_key(path)
        collision_path = provenance_collision_paths.get(collision_key)
        if collision_path is not None:
            raise DiscAnalysisProjectionError(
                "backing provenance 包含大小写或 Unicode 路径碰撞"
            )
        provenance_by_path[path] = (size, version)
        provenance_collision_paths[collision_key] = path

    inventory_by_path: dict[str, object] = {}
    inventory_collision_paths: dict[str, str] = {}
    for inventory in inventories:
        image_path = _normalize_projection_path(
            getattr(inventory, "image_path", None),
            field_name="DiscInventory.image_path",
        )
        if not _projection_descendant(image_path, root):
            raise DiscAnalysisProjectionError(
                "DiscInventory.image_path 不属于 analysis root"
            )
        if image_path in inventory_by_path:
            raise DiscAnalysisProjectionError("DiscInventory 路径重复")
        collision_key = _projection_collision_key(image_path)
        collision_path = inventory_collision_paths.get(collision_key)
        if collision_path is not None:
            raise DiscAnalysisProjectionError(
                "DiscInventory 包含大小写或 Unicode 路径碰撞"
            )
        inventory_by_path[image_path] = inventory
        inventory_collision_paths[collision_key] = image_path

    if set(inventory_by_path) != set(provenance_by_path):
        raise DiscAnalysisProjectionError(
            "DiscInventory 与 backing provenance 路径集合不一致"
        )

    validated: list[
        tuple[
            str,
            object,
            int,
            str | int | float,
            str,
            str,
            tuple[object, ...],
        ]
    ] = []
    for image_path in sorted(inventory_by_path, key=_projection_collision_key):
        inventory = inventory_by_path[image_path]
        image_size, image_version = provenance_by_path[image_path]
        physical_row = physical_by_path.get(image_path)
        if physical_row is None:
            raise DiscAnalysisProjectionError(
                "backing image 不存在于物理来源 rows"
            )
        if physical_row.get("is_dir") is True:
            raise DiscAnalysisProjectionError("backing image 不能是目录")
        physical_type = physical_row.get("object_type")
        if not (
            isinstance(physical_type, str)
            and physical_type.strip().casefold() == "disc_image"
        ) and classify_object_type(str(physical_row["name"])) != "disc_image":
            raise DiscAnalysisProjectionError(
                "backing image 不是受支持的光盘镜像来源对象"
            )
        if physical_row.get("size") != image_size:
            raise DiscAnalysisProjectionError(
                "backing image size 与物理来源 row 不一致"
            )
        observed_version: object = physical_row.get("version")
        if observed_version in (None, ""):
            for key in ("modified", "updated_at", "mtime", "last_modified"):
                candidate = physical_row.get(key)
                if candidate not in (None, ""):
                    observed_version = candidate
                    break
        if observed_version not in (None, ""):
            normalized_observed = _projection_version(observed_version)
            if str(normalized_observed) != str(image_version):
                raise DiscAnalysisProjectionError(
                    "backing image version 与物理来源 row 不一致"
                )

        disc_kind = getattr(inventory, "kind", None)
        if disc_kind not in {"udf", "iso9660"}:
            raise DiscAnalysisProjectionError("DiscInventory.kind 不受支持")
        disc_structure = getattr(inventory, "structure", None)
        if disc_structure not in {"bdmv", "video_ts", "flat", "unknown"}:
            raise DiscAnalysisProjectionError("DiscInventory.structure 不受支持")
        try:
            inner_files = tuple(getattr(inventory, "inner_files"))
        except (AttributeError, TypeError) as exc:
            raise DiscAnalysisProjectionError(
                "DiscInventory.inner_files 无效"
            ) from exc
        if not inner_files:
            raise DiscAnalysisProjectionError(
                "DiscInventory 为空，不能隐藏物理镜像来源对象"
            )
        validated.append(
            (
                image_path,
                inventory,
                image_size,
                image_version,
                disc_kind,
                disc_structure,
                inner_files,
            )
        )

    expanded_paths = set(inventory_by_path)
    analysis_rows = [
        row for row in physical_rows
        if str(row["full_path"]) not in expanded_paths
    ]

    # Track both exact and provider-ambiguous paths across the final virtual
    # view.  Reusing an ancestor directory generated by the same image is the
    # sole allowed duplicate; file/file, file/directory, cross-image, and
    # physical/virtual collisions all fail closed.
    path_kinds: dict[str, str] = {}
    collision_paths: dict[str, str] = {}
    for row in analysis_rows:
        path = str(row["full_path"])
        kind = "directory" if row.get("is_dir") is True else "file"
        path_kinds[path] = kind
        collision_paths[_projection_collision_key(path)] = path

    def add_virtual_row(row: dict[str, object], *, kind: str) -> None:
        path = str(row["full_path"])
        collision_key = _projection_collision_key(path)
        collision_path = collision_paths.get(collision_key)
        if collision_path is not None:
            if (
                collision_path == path
                and path_kinds.get(path) == "directory"
                and kind == "directory"
            ):
                return
            raise DiscAnalysisProjectionError(
                "虚拟 disc analysis row 与现有路径碰撞或重复"
            )
        path_kinds[path] = kind
        collision_paths[collision_key] = path
        analysis_rows.append(row)

    for (
        image_path,
        _inventory,
        image_size,
        image_version,
        disc_kind,
        disc_structure,
        inner_files,
    ) in validated:
        root_provenance = _disc_projection_provenance(
            image_path=image_path,
            image_size=image_size,
            image_version=image_version,
            disc_kind=disc_kind,
            disc_structure=disc_structure,
            inner_path="/",
        )
        add_virtual_row(
            {
                "name": posixpath.basename(image_path),
                "is_dir": True,
                "full_path": image_path,
                "size": 0,
                "modified": "",
                "object_type": "directory",
                **root_provenance,
            },
            kind="directory",
        )

        ordered_inner_files: list[tuple[str, object]] = []
        seen_inner_paths: set[str] = set()
        seen_inner_collision_paths: dict[str, str] = {}
        for inner_file in inner_files:
            inner_path = _normalize_projection_path(
                getattr(inner_file, "inner_path", None),
                field_name="DiscInventory inner path",
            )
            if inner_path in seen_inner_paths:
                raise DiscAnalysisProjectionError(
                    "DiscInventory 包含重复 inner path"
                )
            collision_key = _projection_collision_key(inner_path)
            collision_path = seen_inner_collision_paths.get(collision_key)
            if collision_path is not None:
                raise DiscAnalysisProjectionError(
                    "DiscInventory 包含大小写或 Unicode inner path 碰撞"
                )
            seen_inner_paths.add(inner_path)
            seen_inner_collision_paths[collision_key] = inner_path
            ordered_inner_files.append((inner_path, inner_file))

        for inner_path, inner_file in sorted(
            ordered_inner_files,
            key=lambda item: _projection_collision_key(item[0]),
        ):
            parts = inner_path.split("/")[1:]
            for index in range(1, len(parts)):
                directory_inner_path = "/" + "/".join(parts[:index])
                virtual_directory_path = image_path + directory_inner_path
                provenance = _disc_projection_provenance(
                    image_path=image_path,
                    image_size=image_size,
                    image_version=image_version,
                    disc_kind=disc_kind,
                    disc_structure=disc_structure,
                    inner_path=directory_inner_path,
                )
                add_virtual_row(
                    {
                        "name": parts[index - 1],
                        "is_dir": True,
                        "full_path": virtual_directory_path,
                        "size": 0,
                        "modified": "",
                        "object_type": "directory",
                        **provenance,
                    },
                    kind="directory",
                )

            inner_size = _projection_size(
                getattr(inner_file, "size", None),
                field_name="DiscInventory inner file size",
            )
            try:
                raw_extents = tuple(getattr(inner_file, "extents"))
            except (AttributeError, TypeError) as exc:
                raise DiscAnalysisProjectionError(
                    "DiscInventory inner file extents 无效"
                ) from exc
            extents: list[tuple[int, int]] = []
            byte_ranges: list[tuple[int, int]] = []
            for raw_extent in raw_extents:
                if (
                    not isinstance(raw_extent, (tuple, list))
                    or len(raw_extent) != 2
                ):
                    raise DiscAnalysisProjectionError(
                        "DiscInventory extent 必须是 (lba, block_count)"
                    )
                lba, block_count = raw_extent
                if (
                    isinstance(lba, bool)
                    or not isinstance(lba, int)
                    or lba < 0
                    or isinstance(block_count, bool)
                    or not isinstance(block_count, int)
                    or block_count <= 0
                ):
                    raise DiscAnalysisProjectionError(
                        "DiscInventory extent 坐标无效"
                    )
                byte_start = lba * _DISC_BLOCK_BYTES
                byte_end = (lba + block_count) * _DISC_BLOCK_BYTES
                if byte_end > image_size:
                    raise DiscAnalysisProjectionError(
                        "DiscInventory extent 越过 backing image size"
                    )
                if any(
                    byte_start < existing_end and existing_start < byte_end
                    for existing_start, existing_end in byte_ranges
                ):
                    raise DiscAnalysisProjectionError(
                        "DiscInventory inner file extent 重叠或重复"
                    )
                extents.append((lba, block_count))
                byte_ranges.append((byte_start, byte_end))

            extent_capacity = sum(
                block_count * _DISC_BLOCK_BYTES
                for _, block_count in extents
            )
            if inner_size == 0:
                if extents:
                    raise DiscAnalysisProjectionError(
                        "零字节 inner file 不得声明正长度 extent"
                    )
            elif (
                not extents
                or extent_capacity < inner_size
                or extent_capacity - inner_size >= _DISC_BLOCK_BYTES
            ):
                raise DiscAnalysisProjectionError(
                    "DiscInventory extent 未精确覆盖 inner file size"
                )

            virtual_file_path = image_path + inner_path
            provenance = _disc_projection_provenance(
                image_path=image_path,
                image_size=image_size,
                image_version=image_version,
                disc_kind=disc_kind,
                disc_structure=disc_structure,
                inner_path=inner_path,
            )
            name = parts[-1]
            add_virtual_row(
                {
                    "name": name,
                    "is_dir": False,
                    "full_path": virtual_file_path,
                    "size": inner_size,
                    "modified": "",
                    "object_type": classify_object_type(name),
                    "disc_extents": [list(extent) for extent in extents],
                    **provenance,
                },
                kind="file",
            )

    return sorted(
        analysis_rows,
        key=lambda row: (
            _projection_collision_key(str(row["full_path"])),
            0 if row.get("is_dir") is True else 1,
            str(row["full_path"]),
        ),
    )


def project_disc_inventory_to_analysis_rows(
    walk_rows: Sequence[object],
    root_path: str,
    inventory: object,
    *,
    backing_size: int,
    backing_version: str | int | float,
) -> list[dict[str, object]]:
    """Single-image convenience wrapper for the plural projection helper."""
    image_path = getattr(inventory, "image_path", None)
    if not isinstance(image_path, str):
        raise DiscAnalysisProjectionError("DiscInventory.image_path 无效")
    return project_disc_inventories_to_analysis_rows(
        walk_rows,
        root_path,
        (inventory,),
        backing_provenance={
            image_path: {
                "size": backing_size,
                "version": backing_version,
            }
        },
    )


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


def count_disc_image_files(node: SourceNode) -> int:
    """Count opaque optical-disc image containers in a SourceNode tree."""
    total = sum(1 for f in node.files if f.object_type == "disc_image")
    for child in node.children:
        total += count_disc_image_files(child)
    return total


def has_disc_image_files(node: SourceNode) -> bool:
    """Whether exact source ownership includes an uninspected disc image."""
    return count_disc_image_files(node) > 0


def count_executable_files(node: SourceNode) -> int:
    """Count masquerade ``.exe`` files whose real media type is uninspected."""
    total = sum(1 for f in node.files if f.object_type == "executable")
    for child in node.children:
        total += count_executable_files(child)
    return total


def has_executable_files(node: SourceNode) -> bool:
    """Whether the source includes a masquerade ``.exe`` needing expansion."""
    return count_executable_files(node) > 0


def collect_all_files(node: SourceNode) -> list[SourceFile]:
    """Flatten the tree into a list of all SourceFile objects (recursive)."""
    result = list(node.files)
    for child in node.children:
        result.extend(collect_all_files(child))
    return result


def iter_source_nodes(node: SourceNode) -> list[SourceNode]:
    """Return ``node`` and every descendant directory in stable tree order."""
    output = [node]
    for child in node.children:
        output.extend(iter_source_nodes(child))
    return output


def validate_source_scope(
    root_path: str,
    source_paths: Sequence[str],
) -> tuple[str, ...]:
    """Validate a non-overlapping set of exact ownership scopes.

    The helper is intentionally pure and narrow: it proves only that the
    declared paths are normalized descendants of the intake root and cannot
    overlap one another.  A scope may be either a directory subtree or one
    exact file; callers additionally prove its object kind against the B
    snapshot before using it for identity, planning, or consumption.
    """
    root = str(root_path).rstrip("/") or "/"
    if not root.startswith("/") or "\\" in root:
        raise ValueError("来源根路径无效")
    normalized: list[str] = []
    for value in source_paths:
        if not isinstance(value, str) or not value.startswith("/") or "\\" in value:
            raise ValueError("来源范围包含无效路径")
        path = value.rstrip("/") or "/"
        if posixpath.normpath(path) != path or any(part in {"", ".", ".."} for part in path.split("/")[1:]):
            raise ValueError("来源范围路径不规范")
        root_prefix = "/" if root == "/" else root + "/"
        if not (path == root or path.startswith(root_prefix)):
            raise ValueError("来源范围不属于当前入站根")
        if path not in normalized:
            normalized.append(path)
    if not normalized:
        raise ValueError("作品单元缺少来源范围")
    for index, left in enumerate(normalized):
        for right in normalized[index + 1:]:
            if left == right or left.startswith(right + "/") or right.startswith(left + "/"):
                raise ValueError("作品单元来源范围重叠")
    return tuple(normalized)


def build_scoped_source_node(
    root: SourceNode,
    source_paths: Sequence[str],
    *,
    boundary_key: str,
    display_label: str,
) -> SourceNode:
    """Build a virtual node containing only the exact declared subtrees.

    A WorkUnit that owns several sibling season directories must aggregate
    evidence across them without accidentally inheriting another sibling (for
    example an aftershow) or files at the intake root.  A synthetic root keeps
    this ownership proof local to the pure inventory model.
    """
    paths = validate_source_scope(root.path, source_paths)
    by_path = {
        candidate.path.rstrip("/"): candidate
        for candidate in iter_source_nodes(root)
    }
    # Boundary analysis historically dealt only in directory nodes.  A flat
    # intake container can nevertheless contain several independently titled
    # feature files; WorkUnit ownership for that shape is one exact file per
    # unit.  Materialize those files as virtual one-file nodes so every
    # downstream consumer (C evidence, D proofs, and planner manifests) can
    # use the same SourceNode contract without widening back to the parent
    # directory.
    file_by_path = {
        file.path.rstrip("/"): file
        for file in collect_all_files(root)
    }
    selected: list[SourceNode] = []
    for path in paths:
        selected_node = by_path.get(path)
        if selected_node is None:
            source_file = file_by_path.get(path)
            if source_file is None:
                raise ValueError("来源范围在 B 快照中不存在")
            selected_node = SourceNode(
                path=source_file.path,
                name=source_file.name,
                files=(source_file,),
                children=(),
                depth=max(0, root.depth + source_file.path.count("/") - root.path.count("/")),
            )
        selected.append(selected_node)
    if len(selected) == 1:
        return selected[0]
    label = str(display_label).strip() or str(boundary_key).rstrip("/").rsplit("/", 1)[-1]
    return SourceNode(
        path=str(boundary_key),
        name=label,
        files=(),
        children=tuple(selected),
        depth=0,
    )


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
