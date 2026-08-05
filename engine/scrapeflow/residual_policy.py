"""One fail-closed policy for files left outside the canonical work plan.

The planner, ordinary completion audit, and executor must not invent separate
meanings for a residual file.  This module only classifies evidence; it has no
remote client and performs no mutation.

Subtitles are deliberately deferred.  A later per-video closure may keep one
only when that exact video lacks Chinese subtitle evidence, or create a proven
remote rollback copy before deleting an unneeded candidate. Unknown videos and
archives always block.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import PurePosixPath
import re
import unicodedata


DELETE_AFTER_REMOTE_ROLLBACK = "delete_after_remote_rollback"
DEFER_SUBTITLE_PER_VIDEO = "defer_subtitle_per_video"
BLOCK_UNKNOWN = "block_unknown"

VIDEO_EXTENSIONS = frozenset({
    ".mkv", ".mp4", ".m4v", ".m2ts", ".ts", ".avi", ".mov", ".webm",
})
SUBTITLE_EXTENSIONS = frozenset({
    ".ass", ".ssa", ".srt", ".vtt", ".idx", ".sub", ".sup", ".mks",
})
AUDIO_EXTENSIONS = frozenset({
    ".aac", ".ac3", ".eac3", ".dts", ".dtshd", ".flac", ".m4a", ".mka",
    ".mp3", ".ogg", ".opus", ".thd", ".truehd", ".tta", ".wav", ".ape",
})
DOCUMENT_OR_COMIC_EXTENSIONS = frozenset({
    ".azw", ".azw3", ".cbr", ".cbz", ".djvu", ".doc", ".docx", ".epub",
    ".fb2", ".mobi", ".odt", ".pdf", ".rtf",
})
FONT_EXTENSIONS = frozenset({".otf", ".ttf", ".ttc", ".woff", ".woff2"})
MANIFEST_EXTENSIONS = frozenset({
    ".md5", ".sha1", ".sha256", ".sfv", ".torrent", ".nzb",
})
ARCHIVE_EXTENSIONS = frozenset({".zip", ".rar", ".7z", ".001"})
IMAGE_EXTENSIONS = frozenset({".bmp", ".gif", ".jpeg", ".jpg", ".png", ".webp"})

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

_TRUSTED_CLEANUP_REASONS = frozenset({
    "macOS AppleDouble 隐藏文件",
    "无字幕片头/片尾/光盘菜单视频",
    "发布组广告图片",
    "字体资源包",
    "经特典目录与同集正片交叉确认的片头/片尾视频",
    "特典动画广告/Animated Magia Report Commercial",
})

_DELETE_KIND_REASONS = {
    "appledouble": "macOS AppleDouble 隐藏文件",
    "detached_audio": "外挂音轨/独立音频附件",
    "document_or_comic": "小说、漫画或文档附件",
    "font_resource": "字体资源包",
    "release_manifest": "发布校验/下载清单文件",
    "manga_or_novel_image": "小说/漫画目录图像",
    "advertisement_image": "发布组广告图片",
    "theme_or_promo_video": "非正片片头/片尾/宣传视频",
}


@dataclass(frozen=True, slots=True)
class ResidualDecision:
    action: str
    kind: str
    evidence: tuple[str, ...]

    @property
    def automatically_mutable(self) -> bool:
        return self.action == DELETE_AFTER_REMOTE_ROLLBACK


def recoverable_delete_reason(
    decision: ResidualDecision, *, trusted_reason: str = "",
) -> str | None:
    """Return the single planner-facing reason for a recoverable deletion."""
    if not decision.automatically_mutable:
        return None
    normalized = unicodedata.normalize("NFKC", trusted_reason).strip()
    if decision.kind == "planner_verified_non_feature":
        return normalized if normalized in _TRUSTED_CLEANUP_REASONS else None
    return _DELETE_KIND_REASONS.get(decision.kind)


def _normalized_path(path: str) -> str:
    value = unicodedata.normalize("NFKC", path).replace("\\", "/")
    if not value.startswith("/"):
        value = "/" + value
    return value


def classify_residual(source_path: str, *, reason: str = "") -> ResidualDecision:
    """Classify a residual using explicit extension/context evidence only."""
    normalized = _normalized_path(source_path)
    name = PurePosixPath(normalized).name
    suffix = PurePosixPath(name).suffix.casefold()
    normalized_reason = unicodedata.normalize("NFKC", reason).strip()
    if suffix in SUBTITLE_EXTENSIONS:
        return ResidualDecision(
            DEFER_SUBTITLE_PER_VIDEO,
            "external_subtitle_candidate",
            (f"subtitle_extension={suffix}", "requires_exact_video_subtitle_closure"),
        )
    if normalized_reason in _TRUSTED_CLEANUP_REASONS:
        return ResidualDecision(
            DELETE_AFTER_REMOTE_ROLLBACK,
            "planner_verified_non_feature",
            (f"trusted_cleanup_reason={normalized_reason}",),
        )
    if name.startswith("._"):
        return ResidualDecision(
            DELETE_AFTER_REMOTE_ROLLBACK,
            "appledouble",
            ("appledouble_prefix",),
        )
    if suffix in AUDIO_EXTENSIONS:
        return ResidualDecision(
            DELETE_AFTER_REMOTE_ROLLBACK,
            "detached_audio",
            (f"audio_extension={suffix}",),
        )
    if suffix in DOCUMENT_OR_COMIC_EXTENSIONS:
        return ResidualDecision(
            DELETE_AFTER_REMOTE_ROLLBACK,
            "document_or_comic",
            (f"document_or_comic_extension={suffix}",),
        )
    if suffix in FONT_EXTENSIONS:
        return ResidualDecision(
            DELETE_AFTER_REMOTE_ROLLBACK,
            "font_resource",
            (f"font_extension={suffix}",),
        )
    if suffix in MANIFEST_EXTENSIONS:
        return ResidualDecision(
            DELETE_AFTER_REMOTE_ROLLBACK,
            "release_manifest",
            (f"manifest_extension={suffix}",),
        )
    if suffix in IMAGE_EXTENSIONS and _BOOK_CONTEXT_RE.search(normalized):
        return ResidualDecision(
            DELETE_AFTER_REMOTE_ROLLBACK,
            "manga_or_novel_image",
            ("book_or_comic_path_context", f"image_extension={suffix}"),
        )
    if suffix in IMAGE_EXTENSIONS and _ADVERTISEMENT_IMAGE_RE.search(name):
        return ResidualDecision(
            DELETE_AFTER_REMOTE_ROLLBACK,
            "advertisement_image",
            ("advertisement_filename_marker", f"image_extension={suffix}"),
        )
    if suffix in VIDEO_EXTENSIONS and _THEME_VIDEO_RE.search(name):
        return ResidualDecision(
            DELETE_AFTER_REMOTE_ROLLBACK,
            "theme_or_promo_video",
            ("theme_video_filename_marker", f"video_extension={suffix}"),
        )
    if suffix in ARCHIVE_EXTENSIONS:
        return ResidualDecision(
            BLOCK_UNKNOWN,
            "archive_requires_extraction_receipt",
            (f"archive_extension={suffix}", "no_verified_extraction_receipt"),
        )
    if suffix in VIDEO_EXTENSIONS:
        return ResidualDecision(
            BLOCK_UNKNOWN,
            "unplanned_video",
            (f"video_extension={suffix}", "not_in_signed_media_plan"),
        )
    return ResidualDecision(
        BLOCK_UNKNOWN,
        "unclassified_residual",
        ((f"extension={suffix}" if suffix else "no_extension"),),
    )


__all__ = [
    "AUDIO_EXTENSIONS",
    "BLOCK_UNKNOWN",
    "DEFER_SUBTITLE_PER_VIDEO",
    "DELETE_AFTER_REMOTE_ROLLBACK",
    "DOCUMENT_OR_COMIC_EXTENSIONS",
    "ResidualDecision",
    "SUBTITLE_EXTENSIONS",
    "classify_residual",
    "recoverable_delete_reason",
]
