"""Pure media quality and presentation projections.

The Engine keeps its runtime entrypoints in ``engine.scraper``.  This module
only classifies caller-owned metadata and path strings; it never opens a
provider, writes a plan, or performs a remote operation. The canonical video
collection is imported from ``media_policy`` so quality admission and
planning cannot drift.
"""

from __future__ import annotations

import os
import re
import unicodedata
from pathlib import Path
from typing import Any, Collection, Mapping

from .media_policy import DISC_IMAGE_EXTENSIONS, VIDEO_EXTENSIONS


# A regular feature-length film or episode is many orders of magnitude larger
# than this.  Keep an immutable floor even when an operator chooses a higher
# local policy: a few KB of test text renamed to ``.mkv`` must never cross the
# formal-library write boundary merely because its AList size can be read
# back exactly.
ABSOLUTE_MINIMUM_VIDEO_BYTES = 64 * 1024
DEFAULT_MINIMUM_VIDEO_BYTES = 1024 * 1024
# Keep the public name for callers that imported the quality module directly;
# the canonical collection now lives in ``media_policy``.
VIDEO_FILE_EXTENSIONS = VIDEO_EXTENSIONS


def minimum_video_bytes() -> int:
    """Return the configured formal-media admission floor.

    The environment may make the policy stricter, but cannot relax the
    hard 64 KiB floor.  Invalid values fall back to the safe default instead
    of accidentally disabling the admission check during service startup.
    """
    raw = os.getenv("SCRAPEFLOW_MIN_VIDEO_BYTES", "").strip()
    if not raw:
        return DEFAULT_MINIMUM_VIDEO_BYTES
    if not raw.isascii() or not raw.isdecimal():
        return DEFAULT_MINIMUM_VIDEO_BYTES
    return max(ABSOLUTE_MINIMUM_VIDEO_BYTES, int(raw))


def video_size_is_admissible(value: object) -> bool:
    """Return whether a remote/local video object clears the byte floor."""
    return (
        isinstance(value, int)
        and not isinstance(value, bool)
        and value >= minimum_video_bytes()
    )


def is_video_filename(value: object) -> bool:
    """Classify a planned filename without trusting its serialized kind."""
    return (
        isinstance(value, str)
        and Path(value).suffix.casefold() in VIDEO_FILE_EXTENSIONS
    )


def media_kind(name: str, *, video_exts: Collection[str]) -> str:
    """Classify a planned filename using the Engine-owned media policy.

    ``disc_image`` is intentionally a third value: formal plan validation
    rejects it before a plan can be persisted or executed.  Returning it here
    rather than silently treating it as a subtitle makes forged/legacy plan
    rows fail for the real reason.
    """
    ext = Path(name).suffix.lower()
    if ext in DISC_IMAGE_EXTENSIONS:
        return "disc_image"
    return "video" if ext in video_exts else "subtitle"


def video_resolution_rank(item: Mapping[str, Any]) -> int:
    """Return the best-effort vertical resolution advertised by a video entry."""
    name = unicodedata.normalize("NFKC", str(item.get("name", ""))).lower()
    full_path = unicodedata.normalize(
        "NFKC", str(item.get("full_path", name))
    ).lower()

    def parse(value: str) -> int:
        if re.search(r"(?:^|[^0-9a-z])8k(?:$|[^0-9a-z])|7680\s*[x×]\s*4320", value):
            return 4320
        if re.search(
            r"(?:^|[^0-9a-z])(?:4k|2160[pi])(?:$|[^0-9a-z])|"
            r"3840\s*[x×]\s*2160",
            value,
        ):
            return 2160
        for resolution in (1440, 1080, 720, 576, 480):
            if re.search(
                rf"(?:^|[^0-9]){resolution}[pi]?(?:$|[^0-9])",
                value,
            ):
                return resolution
        return 0

    # A file-level tag is more reliable than a release directory.  When the
    # filename is silent, inspect only the three nearest parent segments. A
    # remote collection root such as ``R 4K ...`` can contain mixed 4K/1080p
    # releases and must not label every descendant as 4K.
    file_rank = parse(name)
    if file_rank:
        return file_rank
    parent_segments = full_path.replace("\\", "/").rsplit("/", 1)[0].split("/")
    for segment in reversed(parent_segments[-3:]):
        if rank := parse(segment):
            return rank
    return 0


def subtitle_presentation_rank(item: Mapping[str, Any]) -> int:
    """Prefer switchable subtitle tracks over permanently burned-in subtitles.

    The ranking is used only when both source paths explicitly advertise their
    subtitle presentation.  An unlabelled release is never deleted based on
    this heuristic.
    """
    full_path = unicodedata.normalize(
        "NFKC", str(item.get("full_path", item.get("name", "")))
    ).casefold()
    # Inspect the nearest labelled directory first.  A collection root may say
    # ``内封+内嵌`` because it contains both releases; that combined parent must
    # not override the concrete child release folder.
    for segment in reversed(full_path.replace("\\", "/").split("/")):
        soft = bool(re.search(r"内封|外挂|软字幕|soft[ ._-]*sub|softsub", segment))
        hard = bool(re.search(r"内嵌|硬字幕|hard[ ._-]*sub|hardsub", segment))
        if soft and not hard:
            return 2
        if hard and not soft:
            return 1
    return 0


__all__ = [
    "ABSOLUTE_MINIMUM_VIDEO_BYTES",
    "DEFAULT_MINIMUM_VIDEO_BYTES",
    "VIDEO_FILE_EXTENSIONS",
    "is_video_filename",
    "media_kind",
    "minimum_video_bytes",
    "subtitle_presentation_rank",
    "video_resolution_rank",
    "video_size_is_admissible",
]
