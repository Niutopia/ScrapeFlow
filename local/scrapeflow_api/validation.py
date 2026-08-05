"""Strict local API input and redaction helpers."""

from __future__ import annotations

import hashlib
import json
import os
import posixpath
import re
from typing import Any
from urllib.parse import unquote, urlsplit


MEDIA_LIBRARY_ROOT = "/quark/影视"
UNSCRAPED_MEDIA_ROOT = f"{MEDIA_LIBRARY_ROOT}/待刮削"
RESERVED_UNSCRAPED_PREFIXES = ("_ScrapeFlow",)
REPLENISHMENT_SOURCE_PREFIXES = ("_ScrapeFlow补源-", "ScrapeFlow补源-")
SCRAPEFLOW_SYSTEM_ROOT = f"{MEDIA_LIBRARY_ROOT}/ScrapeFlow"
SCRAPEFLOW_SYSTEM_SOURCE_AREAS = frozenset({"补源", "备份", "验证"})
TARGET_CATEGORY_PARENTS = {
    "番剧": f"{MEDIA_LIBRARY_ROOT}/番剧",
    "美剧": f"{MEDIA_LIBRARY_ROOT}/美剧",
    "电影": f"{MEDIA_LIBRARY_ROOT}/电影",
}
SENSITIVE_FIELD_RE = re.compile(
    r'(?i)("?(?:api[_-]?key|password|passwd|pass|archive[_-]?pass|token|access[_-]?token|authorization)"?\s*[:=]\s*)'
    r'("?)([^"\s,;&}]+)("?)'
)
CHINESE_PASSWORD_MARKER_RE = re.compile(
    r"((?:解压|压缩包|归档)?\s*密码\s*[:：=]\s*)([^\s,，;；/\\]+)",
    re.IGNORECASE,
)


def canonical_digest(value: Any) -> str:
    payload = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def strict_json_loads(text: str) -> Any:
    def reject_duplicates(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        value: dict[str, Any] = {}
        for key, item in pairs:
            if key in value:
                raise ValueError(f"JSON 包含重复字段: {key}")
            value[key] = item
        return value

    return json.loads(text, object_pairs_hook=reject_duplicates)


def normalize_remote_input(value: Any) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError("请输入 AList 媒体路径")
    raw = value.strip()
    parsed = urlsplit(raw)
    if parsed.scheme:
        if parsed.scheme not in {"http", "https"} or not parsed.path:
            raise ValueError("AList 链接格式无效")
        raw = parsed.path
    raw = unquote(raw).replace("\\", "/")
    if not raw.startswith("/") or "\x00" in raw:
        raise ValueError("AList 路径必须以 / 开头")
    parts = [part for part in raw.split("/") if part]
    if not parts or any(part in {".", ".."} for part in parts):
        raise ValueError("AList 路径无效")
    normalized = "/" + "/".join(parts)
    if len(normalized) > 2048:
        raise ValueError("AList 路径过长")
    return normalized


def media_library_path(value: Any, *, allow_root: bool) -> str:
    normalized = normalize_remote_input(value)
    if normalized != MEDIA_LIBRARY_ROOT and not normalized.startswith(MEDIA_LIBRARY_ROOT + "/"):
        raise ValueError(f"媒体路径必须位于 {MEDIA_LIBRARY_ROOT} 下")
    if not allow_root and normalized == MEDIA_LIBRARY_ROOT:
        raise ValueError(f"请选择 {MEDIA_LIBRARY_ROOT} 下的具体媒体目录")
    return normalized


def unscraped_media_path(value: Any) -> str:
    normalized = media_library_path(value, allow_root=False)
    system_reason = scrapeflow_system_source_reason(normalized)
    if system_reason:
        raise ValueError(system_reason)
    if (
        normalized == UNSCRAPED_MEDIA_ROOT
        or not normalized.startswith(UNSCRAPED_MEDIA_ROOT + "/")
    ):
        raise ValueError(f"源目录必须位于 {UNSCRAPED_MEDIA_ROOT} 下，并选择具体作品目录")
    reserved_reason = unscraped_reserved_reason(normalized)
    if reserved_reason:
        raise ValueError(reserved_reason)
    return normalized


def scrapeflow_system_source_reason(value: Any) -> str | None:
    """Explain why a user cannot submit ScrapeFlow-managed staging paths."""
    normalized = normalize_remote_input(value)
    system_prefix = SCRAPEFLOW_SYSTEM_ROOT.rstrip("/") + "/"
    if normalized.startswith(system_prefix):
        area = normalized[len(system_prefix):].split("/", 1)[0]
        if area in SCRAPEFLOW_SYSTEM_SOURCE_AREAS:
            return (
                "ScrapeFlow 补源、备份和验证目录由系统内部管理，"
                "不能作为用户新建整理任务的源目录"
            )
    inbox_prefix = UNSCRAPED_MEDIA_ROOT.rstrip("/") + "/"
    if normalized.startswith(inbox_prefix):
        components = normalized[len(inbox_prefix):].split("/")
        if any(
            component.startswith(marker)
            for component in components
            for marker in REPLENISHMENT_SOURCE_PREFIXES
        ):
            return "ScrapeFlow 补源暂存目录只能由系统内部整理，不能作为用户任务源目录"
    return None


def unscraped_pending_delete(value: Any) -> bool:
    """Return whether the top-level source has ScrapeFlow's delete marker."""
    normalized = normalize_remote_input(value)
    prefix = UNSCRAPED_MEDIA_ROOT.rstrip("/") + "/"
    if not normalized.startswith(prefix):
        return False
    first_component = normalized[len(prefix):].split("/", 1)[0]
    return bool(re.search(r"[（(]\s*待删\d*\s*[)）]\s*$", first_component))


def unscraped_reserved_reason(value: Any) -> str | None:
    """Explain why an internal backup tree cannot become a scrape source."""
    normalized = normalize_remote_input(value)
    system_reason = scrapeflow_system_source_reason(normalized)
    if system_reason:
        return system_reason
    prefix = UNSCRAPED_MEDIA_ROOT.rstrip("/") + "/"
    if not normalized.startswith(prefix):
        return None
    first_component = normalized[len(prefix):].split("/", 1)[0]
    if unscraped_pending_delete(normalized):
        return "待删除目录已完成整理提交，不能再次创建刮削任务"
    if any(first_component.startswith(marker) for marker in RESERVED_UNSCRAPED_PREFIXES):
        return "ScrapeFlow 备份/恢复目录只用于取证和回滚，不能作为新整理任务的源目录"
    return None


def target_parent_for_category(category: Any) -> str:
    if not isinstance(category, str) or category not in TARGET_CATEGORY_PARENTS:
        choices = "、".join(TARGET_CATEGORY_PARENTS)
        raise ValueError(f"请选择目标分类：{choices}")
    return TARGET_CATEGORY_PARENTS[category]


def paths_overlap(left: str, right: str) -> bool:
    normalized_left = normalize_remote_input(left).casefold()
    normalized_right = normalize_remote_input(right).casefold()
    return (
        normalized_left == normalized_right
        or normalized_left.startswith(normalized_right.rstrip("/") + "/")
        or normalized_right.startswith(normalized_left.rstrip("/") + "/")
    )


def default_parent(source: str) -> str:
    parent = posixpath.dirname(source.rstrip("/"))
    if not parent or parent == "/":
        raise ValueError("源目录必须位于媒体库父目录下")
    return parent


def redact(text: str) -> str:
    output = text.rstrip("\r\n")
    for name in (
        "ALIST_PASSWORD", "TMDB_API_KEY", "ARCHIVE_PASSWORD",
        "SCRAPEFLOW_REPLENISHMENT_TOKEN",
    ):
        secret = os.getenv(name)
        if secret:
            output = output.replace(secret, "[REDACTED]")
    output = SENSITIVE_FIELD_RE.sub(r'\1"[REDACTED]"', output)
    return CHINESE_PASSWORD_MARKER_RE.sub(r"\1[REDACTED]", output)
