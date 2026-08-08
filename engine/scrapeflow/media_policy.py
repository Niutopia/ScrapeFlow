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


# Container/stream names accepted as video payloads.  ``.strm`` is a text
# pointer understood by the target media library, while ISO and transport
# stream suffixes are intentionally retained for archive/provider parity.
VIDEO_EXTENSIONS = frozenset({
    ".3gp",
    ".asf",
    ".avi",
    ".flv",
    ".iso",
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


def is_video_filename(value: Any) -> bool:
    return extension(value) in VIDEO_EXTENSIONS


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
    if suffix in EXECUTABLE_EXTENSIONS:
        return "executable"
    if is_temporary_filename(value):
        return "temporary"
    return "unknown"


__all__ = [
    "ARCHIVE_EXTENSIONS",
    "ARCHIVE_PART_EXTENSIONS",
    "AUDIO_EXTENSIONS",
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
    "is_subtitle_filename",
    "is_temporary_filename",
    "is_video_filename",
]
