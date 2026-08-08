"""Classify files that are not part of the canonical media plan."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import PurePosixPath
import re
import unicodedata


CLEANUP_AFTER_READBACK = "cleanup_after_readback"
DEFER_SUBTITLE = "defer_subtitle"
KEEP_UNPLANNED = "keep_unplanned"

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
DOCUMENT_EXTENSIONS = frozenset({
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

_TRUSTED_REASONS = frozenset({
    "macOS AppleDouble 隐藏文件",
    "无字幕片头/片尾/光盘菜单视频",
    "发布组广告图片",
    "字体资源包",
    "经特典目录与同集正片交叉确认的片头/片尾视频",
    "特典动画广告/Animated Magia Report Commercial",
})

_CLEANUP_REASONS = {
    "appledouble": "macOS AppleDouble 隐藏文件",
    "detached_audio": "外挂音轨/独立音频附件",
    "document": "小说、漫画或文档附件",
    "font": "字体资源包",
    "manifest": "发布校验/下载清单文件",
    "book_image": "小说/漫画目录图像",
    "advertisement": "发布组广告图片",
    "theme_video": "非正片片头/片尾/宣传视频",
}


@dataclass(frozen=True, slots=True)
class ResidualDecision:
    action: str
    kind: str
    reasons: tuple[str, ...]

    @property
    def can_cleanup(self) -> bool:
        return self.action == CLEANUP_AFTER_READBACK


def cleanup_reason_for(
    decision: ResidualDecision, *, trusted_reason: str = "",
) -> str | None:
    if not decision.can_cleanup:
        return None
    normalized = unicodedata.normalize("NFKC", trusted_reason).strip()
    if decision.kind == "planner_non_feature":
        return normalized if normalized in _TRUSTED_REASONS else None
    return _CLEANUP_REASONS.get(decision.kind)


def classify_residual(source_path: str, *, reason: str = "") -> ResidualDecision:
    normalized = unicodedata.normalize("NFKC", source_path).replace("\\", "/")
    if not normalized.startswith("/"):
        normalized = "/" + normalized
    name = PurePosixPath(normalized).name
    suffix = PurePosixPath(name).suffix.casefold()
    known_reason = unicodedata.normalize("NFKC", reason).strip()
    if suffix in SUBTITLE_EXTENSIONS:
        return ResidualDecision(DEFER_SUBTITLE, "subtitle", (f"extension={suffix}",))
    if known_reason in _TRUSTED_REASONS:
        return ResidualDecision(CLEANUP_AFTER_READBACK, "planner_non_feature", (known_reason,))
    if name.startswith("._"):
        return ResidualDecision(CLEANUP_AFTER_READBACK, "appledouble", ("appledouble",))
    if suffix in AUDIO_EXTENSIONS:
        return ResidualDecision(CLEANUP_AFTER_READBACK, "detached_audio", (f"extension={suffix}",))
    if suffix in DOCUMENT_EXTENSIONS:
        return ResidualDecision(CLEANUP_AFTER_READBACK, "document", (f"extension={suffix}",))
    if suffix in FONT_EXTENSIONS:
        return ResidualDecision(CLEANUP_AFTER_READBACK, "font", (f"extension={suffix}",))
    if suffix in MANIFEST_EXTENSIONS:
        return ResidualDecision(CLEANUP_AFTER_READBACK, "manifest", (f"extension={suffix}",))
    if suffix in IMAGE_EXTENSIONS and _BOOK_CONTEXT_RE.search(normalized):
        return ResidualDecision(CLEANUP_AFTER_READBACK, "book_image", ("book_context",))
    if suffix in IMAGE_EXTENSIONS and _ADVERTISEMENT_IMAGE_RE.search(name):
        return ResidualDecision(CLEANUP_AFTER_READBACK, "advertisement", ("advertisement_name",))
    if suffix in VIDEO_EXTENSIONS and _THEME_VIDEO_RE.search(name):
        return ResidualDecision(CLEANUP_AFTER_READBACK, "theme_video", ("theme_name",))
    if suffix in ARCHIVE_EXTENSIONS:
        return ResidualDecision(KEEP_UNPLANNED, "archive", ("needs_extraction",))
    if suffix in VIDEO_EXTENSIONS:
        return ResidualDecision(KEEP_UNPLANNED, "video", ("not_in_plan",))
    return ResidualDecision(KEEP_UNPLANNED, "unknown", ((f"extension={suffix}" if suffix else "no_extension"),))


__all__ = [
    "CLEANUP_AFTER_READBACK",
    "DEFER_SUBTITLE",
    "KEEP_UNPLANNED",
    "ResidualDecision",
    "classify_residual",
    "cleanup_reason_for",
]
