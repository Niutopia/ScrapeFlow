"""Fail-closed classification and cleanup rules for residual source files.

Residual classification is deliberately non-destructive.  A file being an
audio track, a comic, a font, a manifest, an image, or a likely OP/ED is not
proof that this task created it or that it is safe to remove.  Only operating
system litter and a narrowly proven task-staging download temporary are
eligible for unattended cleanup; callers must still check ownership at the
write boundary.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import PurePosixPath
import posixpath
import re
import unicodedata

from .media_policy import (
    ARCHIVE_EXTENSIONS,
    AUDIO_EXTENSIONS,
    DISC_IMAGE_EXTENSIONS,
    DOCUMENT_EXTENSIONS,
    EXECUTABLE_EXTENSIONS,
    FONT_EXTENSIONS,
    IMAGE_EXTENSIONS,
    MANIFEST_EXTENSIONS,
    SUBTITLE_EXTENSIONS,
    TEMPORARY_EXTENSIONS,
    VIDEO_EXTENSIONS,
    is_archive_filename,
)


CLEANUP_AFTER_READBACK = "cleanup_after_readback"
DEFER_SUBTITLE = "defer_subtitle"
KEEP_UNPLANNED = "keep_unplanned"

APPLEDOUBLE_CLEANUP_REASON = "macOS AppleDouble 隐藏文件"
DS_STORE_CLEANUP_REASON = "macOS .DS_Store 隐藏文件"
REBUILDABLE_STAGING_TEMP_CLEANUP_REASON = "任务自有可重建下载临时文件"

# Compatibility alias retained for the cleanup implementation and existing
# callers.  The authoritative collection is ``media_policy.TEMPORARY_EXTENSIONS``.
REBUILDABLE_DOWNLOAD_TEMP_SUFFIXES = TEMPORARY_EXTENSIONS

_BOOK_CONTEXT_RE = re.compile(
    r"(?:^|[/\\._\-\s\[\]()])(?:novels?|manga|comics?|books?|"
    r"小说|漫画|轻小说|扫图|画集|电子书)(?:$|[/\\._\-\s\[\]()])",
    re.I,
)
_THEME_VIDEO_RE = re.compile(
    r"(?:^|[\s._\-\[\]()])(?:NCOP|NCED|OP|ED|MENU|PV|CM|TRAILER)"
    r"(?:\d+(?:v\d+)?)?(?:$|[\s._\-\[\]()])",
    re.I,
)
_ADVERTISEMENT_IMAGE_RE = re.compile(
    r"(?:^|[\s._\-\[\]()])(?:广告|advert(?:isement)?|promo|qr|"
    r"weibo|wechat|qqgroup)(?:$|[\s._\-\[\]()])",
    re.I,
)


@dataclass(frozen=True, slots=True)
class ResidualDecision:
    action: str
    kind: str
    reasons: tuple[str, ...]

    @property
    def can_cleanup(self) -> bool:
        return self.action == CLEANUP_AFTER_READBACK


def _normalized_path(path: str) -> str:
    value = unicodedata.normalize("NFKC", path).replace("\\", "/")
    if not value.startswith("/"):
        value = "/" + value
    return posixpath.normpath(value)


def _path_is_within(path: str, root: str) -> bool:
    return path == root or path.startswith(root.rstrip("/") + "/")


def _is_rebuildable_download_temp_name(name: str) -> bool:
    lowered = name.casefold()
    return (
        lowered.startswith(".scraper-tmp-")
        or any(lowered.endswith(suffix) for suffix in REBUILDABLE_DOWNLOAD_TEMP_SUFFIXES)
    )


def is_task_owned_staging_root(path: str) -> bool:
    """Recognize the only current automatic staging layout.

    The automatic replenishment runtime creates
    ``.../ScrapeFlow/补源/<job-id>/<attempt-id>/...`` and ordinary archive
    intake creates ``.../ScrapeFlow/归档/<job-id>/...``.  Merely living below a
    broad media root or intake folder is not ownership evidence: both layouts
    require at least a job component and a task/attempt component after the
    marker.
    """
    normalized = _normalized_path(path)
    parts = [part for part in normalized.split("/") if part]
    folded = [part.casefold() for part in parts]
    for index in range(len(parts) - 1):
        if folded[index] == "scrapeflow" and parts[index + 1] in {"补源", "归档"}:
            return len(parts[index + 2:]) >= 2
    return False


def cleanup_allowlist_reason(
    source_path: str,
    *,
    task_root: str | None = None,
) -> str | None:
    """Return the sole accepted cleanup reason, or ``None`` when unsafe.

    This function proves only static name/path eligibility.  The executor
    additionally verifies that the cleanup row's source directory and name
    match exactly before calling AList ``remove``.
    """
    normalized = _normalized_path(source_path)
    name = PurePosixPath(normalized).name
    if name.startswith("._"):
        return APPLEDOUBLE_CLEANUP_REASON
    if name.casefold() == ".ds_store":
        return DS_STORE_CLEANUP_REASON
    if task_root is None or not _is_rebuildable_download_temp_name(name):
        return None
    root = _normalized_path(task_root)
    if is_task_owned_staging_root(root) and _path_is_within(normalized, root):
        return REBUILDABLE_STAGING_TEMP_CLEANUP_REASON
    return None


def cleanup_reason_for(
    decision: ResidualDecision, *, trusted_reason: str = "",
) -> str | None:
    """Expose only the tiny name-only cleanup whitelist to planners.

    ``trusted_reason`` remains a compatibility parameter but cannot expand
    the whitelist: historic planner labels for fonts, advertisements, and
    theme videos are no longer delete authority.
    """
    del trusted_reason
    if not decision.can_cleanup:
        return None
    if decision.kind == "appledouble":
        return APPLEDOUBLE_CLEANUP_REASON
    if decision.kind == "ds_store":
        return DS_STORE_CLEANUP_REASON
    return None


def classify_residual(source_path: str, *, reason: str = "") -> ResidualDecision:
    """Classify a residual without granting deletion authority from its type."""
    del reason
    normalized = _normalized_path(source_path)
    name = PurePosixPath(normalized).name
    suffix = PurePosixPath(name).suffix.casefold()
    if suffix in SUBTITLE_EXTENSIONS:
        return ResidualDecision(DEFER_SUBTITLE, "subtitle", (f"extension={suffix}",))
    if name.startswith("._"):
        return ResidualDecision(CLEANUP_AFTER_READBACK, "appledouble", ("appledouble",))
    if name.casefold() == ".ds_store":
        return ResidualDecision(CLEANUP_AFTER_READBACK, "ds_store", ("ds_store",))
    if _is_rebuildable_download_temp_name(name):
        return ResidualDecision(
            KEEP_UNPLANNED,
            "rebuildable_download_temp",
            ("requires_task_owned_staging_root",),
        )
    if suffix in AUDIO_EXTENSIONS:
        return ResidualDecision(KEEP_UNPLANNED, "detached_audio", (f"extension={suffix}",))
    if suffix in DOCUMENT_EXTENSIONS:
        return ResidualDecision(KEEP_UNPLANNED, "document_or_comic", (f"extension={suffix}",))
    if suffix in FONT_EXTENSIONS:
        return ResidualDecision(KEEP_UNPLANNED, "font", (f"extension={suffix}",))
    if suffix in MANIFEST_EXTENSIONS:
        return ResidualDecision(KEEP_UNPLANNED, "manifest", (f"extension={suffix}",))
    if suffix in EXECUTABLE_EXTENSIONS:
        return ResidualDecision(KEEP_UNPLANNED, "unknown_executable", (f"extension={suffix}",))
    if suffix in IMAGE_EXTENSIONS:
        if _BOOK_CONTEXT_RE.search(normalized):
            return ResidualDecision(KEEP_UNPLANNED, "book_image", ("book_context",))
        if _ADVERTISEMENT_IMAGE_RE.search(name):
            return ResidualDecision(KEEP_UNPLANNED, "advertisement_image", ("advertisement_name",))
        return ResidualDecision(KEEP_UNPLANNED, "unknown_image", (f"extension={suffix}",))
    if suffix in DISC_IMAGE_EXTENSIONS:
        return ResidualDecision(
            KEEP_UNPLANNED,
            "disc_image_requires_content_expansion",
            ("opaque_disc_image",),
        )
    if suffix in VIDEO_EXTENSIONS and _THEME_VIDEO_RE.search(name):
        return ResidualDecision(KEEP_UNPLANNED, "theme_video", ("theme_name",))
    if is_archive_filename(normalized):
        return ResidualDecision(KEEP_UNPLANNED, "archive", ("needs_extraction",))
    if suffix in VIDEO_EXTENSIONS:
        return ResidualDecision(KEEP_UNPLANNED, "video", ("not_in_plan",))
    return ResidualDecision(
        KEEP_UNPLANNED,
        "unknown",
        ((f"extension={suffix}" if suffix else "no_extension"),),
    )


__all__ = [
    "APPLEDOUBLE_CLEANUP_REASON",
    "ARCHIVE_EXTENSIONS",
    "AUDIO_EXTENSIONS",
    "CLEANUP_AFTER_READBACK",
    "DOCUMENT_EXTENSIONS",
    "DEFER_SUBTITLE",
    "DS_STORE_CLEANUP_REASON",
    "EXECUTABLE_EXTENSIONS",
    "FONT_EXTENSIONS",
    "IMAGE_EXTENSIONS",
    "KEEP_UNPLANNED",
    "MANIFEST_EXTENSIONS",
    "REBUILDABLE_STAGING_TEMP_CLEANUP_REASON",
    "REBUILDABLE_DOWNLOAD_TEMP_SUFFIXES",
    "ResidualDecision",
    "SUBTITLE_EXTENSIONS",
    "VIDEO_EXTENSIONS",
    "classify_residual",
    "cleanup_allowlist_reason",
    "cleanup_reason_for",
    "is_task_owned_staging_root",
]
