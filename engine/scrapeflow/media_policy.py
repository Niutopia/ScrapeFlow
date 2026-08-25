"""Canonical filename and media-extension policy for the local Engine.

The project has several consumers of a file suffix: planning, provider
manifest validation, the local audit, and residual/cleanup classification.
Keeping those sets in one dependency-free module prevents a payload such as
``.mts`` or ``.strm`` from being accepted by one stage and silently ignored
by another.  The collections are immutable on purpose; callers should derive
their own set only when an API explicitly requires a mutable collection.

This module classifies *names*, not bytes.  Archive magic/signature checks and
subtitle/video content validation remain separate safety boundaries.
"""

from __future__ import annotations

from pathlib import PurePosixPath
import re
from typing import Any


# Container/stream names accepted as directly plannable video payloads.
# ``.strm`` is a text pointer understood by the target media library and the
# transport-stream suffixes name actual media streams.  Optical-disc images
# are deliberately *not* in this set: their internal title/season/file
# boundaries are opaque until a dedicated read-only inspection has produced
# an exact inventory.
VIDEO_EXTENSIONS = frozenset({
    ".3gp",
    ".asf",
    ".avi",
    ".flv",
    ".m2ts",
    ".m4v",
    ".mkv",
    ".mov",
    ".mp4",
    ".mpeg",
    ".mpg",
    ".mts",
    ".rm",
    ".rmvb",
    ".strm",
    ".ts",
    ".webm",
    ".wmv",
})


# Optical-disc images and their common descriptor/raw-payload companions.
# They are containers, not directly consumable media.  In particular, a
# filename such as ``Season 01.iso`` is insufficient evidence of whether the
# image contains a feature, multiple episodes, extras, or unrelated files.
# Keep this deliberately separate from ``ARCHIVE_EXTENSIONS``: ordinary
# archive extraction must never mount or directly consume a disc image.  The
# bounded archive-inspection lane may still recognize its on-disk filesystem
# magic and use 7-Zip to list/selectively extract members into task staging.
DISC_IMAGE_EXTENSIONS = frozenset({
    ".b5t",
    ".b6t",
    ".bin",
    ".ccd",
    ".cdi",
    ".cue",
    ".dmg",
    ".img",
    ".iso",
    ".isz",
    ".mdf",
    ".mds",
    ".nrg",
    ".pdi",
    ".udf",
})


# This is a policy fact, rather than a title-specific exception.  The current
# intake flow has no privileged "trust this ISO" escape hatch: a later
# read-only expander/confirmation surface must supply concrete content
# evidence and then rebuild B/W before the ordinary identity/planning flow
# can resume.
DISC_IMAGE_INSPECTION_REQUIRED = (
    "发现光盘镜像容器；必须先完成只读安全内容展开并重建来源快照，"
    "才能确认作品边界、季集与可消费媒体。禁止将镜像直接规划、移动或归档"
)


# Sidecar and subtitle-container suffixes.  IDX/SUB are a paired subtitle
# format and MKS is a Matroska subtitle-only container.
SUBTITLE_EXTENSIONS = frozenset({
    ".ass",
    ".idx",
    ".mks",
    ".srt",
    ".ssa",
    ".sub",
    ".sup",
    ".vtt",
})


AUDIO_EXTENSIONS = frozenset({
    ".aac",
    ".ac3",
    ".ape",
    ".dts",
    ".dtshd",
    ".eac3",
    ".flac",
    ".m4a",
    ".mka",
    ".mp3",
    ".oga",
    ".ogg",
    ".opus",
    ".thd",
    ".truehd",
    ".tta",
    ".wav",
    ".wma",
})


# Documents include common comic/book formats because they must be retained
# as unplanned residuals rather than mistaken for media or cleanup candidates.
DOCUMENT_EXTENSIONS = frozenset({
    ".azw",
    ".azw3",
    ".cbr",
    ".cbz",
    ".djvu",
    ".doc",
    ".docx",
    ".epub",
    ".fb2",
    ".mobi",
    ".odt",
    ".pdf",
    ".rtf",
    ".txt",
})
FONT_EXTENSIONS = frozenset({
    ".otf",
    ".ttc",
    ".ttf",
    ".woff",
    ".woff2",
})
IMAGE_EXTENSIONS = frozenset({
    ".avif",
    ".bmp",
    ".gif",
    ".heic",
    ".jpeg",
    ".jpg",
    ".png",
    ".webp",
})
POSTER_EXTENSIONS = frozenset({
    ".avif",
    ".gif",
    ".jpeg",
    ".jpg",
    ".png",
    ".webp",
})


# Sidecar/checksum/manifest files are not archive members and are never
# executable media.  They remain visible to the residual audit.
MANIFEST_EXTENSIONS = frozenset({
    ".json",
    ".md5",
    ".nfo",
    ".nzb",
    ".sfv",
    ".sha1",
    ".sha256",
    ".torrent",
})


# ``.001`` is the first volume of a split archive.  RAR old-style volumes
# use ``.r00``/``.r01`` and newer releases often use ``.part01.rar``; the
# helper below recognizes the latter without widening ordinary suffix sets.
ARCHIVE_EXTENSIONS = frozenset({
    ".001",
    ".7z",
    ".rar",
    ".zip",
})
ARCHIVE_PART_EXTENSIONS = frozenset({
    ".r00",
    ".r01",
    ".r02",
    ".r03",
    ".r04",
    ".r05",
    ".r06",
    ".r07",
    ".r08",
    ".r09",
})


EXECUTABLE_EXTENSIONS = frozenset({
    ".app",
    ".bat",
    ".cmd",
    ".com",
    ".command",
    ".dll",
    ".exe",
    ".msi",
    ".ps1",
    ".sh",
})


TEMPORARY_EXTENSIONS = frozenset({
    ".!qb",
    ".aria2",
    ".crdownload",
    ".download",
    ".part",
    ".partial",
    ".temp",
    ".tmp",
})


MEDIA_EXTENSIONS = frozenset(VIDEO_EXTENSIONS | SUBTITLE_EXTENSIONS)

_PART_RAR_RE = re.compile(r"\.part\d{1,4}\.rar$", re.IGNORECASE)
_PART_ZIP_RE = re.compile(r"\.z\d{2,3}$", re.IGNORECASE)
_PART_7Z_RE = re.compile(r"\.7z\.\d{3,4}$", re.IGNORECASE)
_NUMERIC_VOLUME_RE = re.compile(r"\.0\d{2,3}$")


def extension(value: Any) -> str:
    """Return a normalized final suffix for a path/name-like value."""
    if not isinstance(value, str):
        return ""
    return PurePosixPath(value.replace("\\", "/")).suffix.casefold()


_FONT_LABEL_RE = re.compile(
    r"(?:font|fonts|字体|フォント)", re.IGNORECASE,
)


def is_video_filename(value: Any) -> bool:
    return extension(value) in VIDEO_EXTENSIONS


def is_disc_image_filename(value: Any) -> bool:
    """Return whether a name denotes an opaque optical-disc image container."""
    return extension(value) in DISC_IMAGE_EXTENSIONS


def is_executable_filename(value: Any) -> bool:
    """Return whether a name must never be executed at an intake boundary."""

    return extension(value) in EXECUTABLE_EXTENSIONS


def is_container_candidate_filename(value: Any) -> bool:
    """Return whether a filename needs byte-level container proof.

    This deliberately includes executable-looking names.  A genuine binary is
    rejected without execution; a self-extracting or renamed archive is only
    admitted after the archive detector finds a real supported signature.
    """

    return (
        is_archive_filename(value)
        or is_disc_image_filename(value)
        or is_executable_filename(value)
    )


def is_subtitle_filename(value: Any) -> bool:
    return extension(value) in SUBTITLE_EXTENSIONS


def is_audio_filename(value: Any) -> bool:
    return extension(value) in AUDIO_EXTENSIONS


def is_archive_filename(value: Any) -> bool:
    """Recognize ordinary and split archive member names."""
    if not isinstance(value, str):
        return False
    normalized = value.replace("\\", "/").casefold()
    suffix = extension(normalized)
    return (
        suffix in ARCHIVE_EXTENSIONS
        or suffix in ARCHIVE_PART_EXTENSIONS
        or bool(_PART_RAR_RE.search(normalized))
        or bool(_PART_ZIP_RE.search(normalized))
        or bool(_PART_7Z_RE.search(normalized))
        or bool(_NUMERIC_VOLUME_RE.search(normalized))
    )


def is_temporary_filename(value: Any) -> bool:
    if not isinstance(value, str):
        return False
    lowered = value.replace("\\", "/").casefold()
    name = PurePosixPath(lowered).name
    return name.startswith(".scraper-tmp-") or any(
        name.endswith(suffix) for suffix in TEMPORARY_EXTENSIONS
    )


def classify_filename(value: Any) -> str:
    """Return one stable coarse category for residual/provider consumers."""
    if is_disc_image_filename(value):
        return "disc_image"
    if is_video_filename(value):
        return "video"
    if is_subtitle_filename(value):
        return "subtitle"
    if is_audio_filename(value):
        return "audio"
    if is_archive_filename(value):
        return "archive"
    suffix = extension(value)
    if suffix in IMAGE_EXTENSIONS:
        return "image"
    if suffix in DOCUMENT_EXTENSIONS:
        return "document"
    if suffix in FONT_EXTENSIONS:
        return "font"
    if suffix in MANIFEST_EXTENSIONS:
        return "manifest"
    # An executable whose basename advertises a font package (e.g. ``[Fonts].exe``,
    # a self-extracting subtitle font installer) is a resource residual, not a
    # disguised media container and not a runnable media binary.
    if suffix in EXECUTABLE_EXTENSIONS and _FONT_LABEL_RE.search(
        PurePosixPath(value.replace("\\", "/")).name
    ):
        return "font"
    if suffix in EXECUTABLE_EXTENSIONS:
        return "executable"
    if is_temporary_filename(value):
        return "temporary"
    return "unknown"


__all__ = [
    "ARCHIVE_EXTENSIONS",
    "ARCHIVE_PART_EXTENSIONS",
    "AUDIO_EXTENSIONS",
    "DISC_IMAGE_EXTENSIONS",
    "DISC_IMAGE_INSPECTION_REQUIRED",
    "DOCUMENT_EXTENSIONS",
    "EXECUTABLE_EXTENSIONS",
    "FONT_EXTENSIONS",
    "IMAGE_EXTENSIONS",
    "MANIFEST_EXTENSIONS",
    "MEDIA_EXTENSIONS",
    "POSTER_EXTENSIONS",
    "SUBTITLE_EXTENSIONS",
    "TEMPORARY_EXTENSIONS",
    "VIDEO_EXTENSIONS",
    "classify_filename",
    "extension",
    "is_archive_filename",
    "is_audio_filename",
    "is_container_candidate_filename",
    "is_disc_image_filename",
    "is_executable_filename",
    "is_subtitle_filename",
    "is_temporary_filename",
    "is_video_filename",
]
