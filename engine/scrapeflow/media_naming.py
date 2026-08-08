"""Pure media filename, edition, and subtitle-language policy.

The :mod:`engine.scraper` facade remains the runtime entrypoint.
This module owns only deterministic filename/classification rules: it never
opens an AList/TMDB client, writes a plan, or performs a remote operation.
``bind_compat_runtime`` routes direct module calls through that facade after
startup so runtime overrides continue to affect extracted callers.
"""

from __future__ import annotations

import re
import unicodedata
from pathlib import Path
from types import ModuleType
from typing import Any, Callable, Mapping

from .errors import PlanError
from .remote_paths import _truncate_utf8, safe_name


# These patterns intentionally retain their historic order.  The result of
# each function feeds plan filenames and therefore deterministic plan output.
IGNORED_EXTRA_TAG_RE = re.compile(
    r"(?:^|[\s._\-\[\]()])(?:NCOP|NCED|PV|MENU|FONTS?|EXTRAS?)(?:\d+)?(?:$|[\s._\-\[\]()])",
    re.IGNORECASE,
)
SAMPLE_RE = re.compile(
    r"(?:^|[\s._\-\[\]()])(?:sample|样片|试看)(?:$|[\s._\-\[\]()])",
    re.IGNORECASE,
)
BONUS_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("behindthescenes", re.compile(r"behind[ ._-]*the[ ._-]*scenes?|幕后", re.IGNORECASE)),
    ("deleted", re.compile(r"(?:^|[ ._\-])deleted(?:$|[ ._\-])|删减片段", re.IGNORECASE)),
    ("featurette", re.compile(r"featurette|制作特辑", re.IGNORECASE)),
    ("interview", re.compile(r"interview|访谈", re.IGNORECASE)),
    ("trailer", re.compile(r"trailer|预告", re.IGNORECASE)),
    ("scene", re.compile(r"(?:^|[ ._\-])scene(?:$|[ ._\-])|片段", re.IGNORECASE)),
    ("short", re.compile(r"(?:^|[ ._\-])short(?:$|[ ._\-])|短片", re.IGNORECASE)),
)
PLANNED_BONUS_SUFFIX_RE = re.compile(
    rf"-(?:{'|'.join(re.escape(label) for label, _ in BONUS_PATTERNS)})(?:\d+)?$",
    re.IGNORECASE,
)
EDITION_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = tuple(
    (label, re.compile(pattern, re.IGNORECASE))
    for label, pattern in (
        (
            "Director's Cut",
            r"director(?:'?s|')?[ ._-]*cut|导演剪辑|"
            r"\[\s*\d{1,4}\s+cut\s*\]",
        ),
        ("New Edit", r"新编集版|新編集版|shin[ ._-]*hensh(?:u|uu)[ ._-]*ban"),
        ("Extended Cut", r"extended[ ._-]*cut|加长版"),
        ("Theatrical Cut", r"theatrical[ ._-]*cut|院线版"),
        ("Final Cut", r"final[ ._-]*cut"),
        ("Unrated Cut", r"unrated[ ._-]*cut|未分级"),
        ("3D", r"(?:^|[ ._\-\[\]()])(?:SBS[ ._-]*)?3D(?:$|[ ._\-\[\]()])"),
        ("IMAX", r"(?:^|[ ._\-\[\]()])imax(?:$|[ ._\-\[\]()])"),
        (
            "Musani Staff Credit",
            r"musani[ ._-]*staff[ ._-]*credit(?:[ ._-]*ver(?:sion)?)?",
        ),
        (
            "Original Staff Credit",
            r"original[ ._-]*staff[ ._-]*credit(?:[ ._-]*ver(?:sion)?)?",
        ),
        ("Special Edition", r"special[ ._-]*edition|特别版"),
    )
)
SIMPLIFIED_MARKERS = (
    "简体",
    "简中",
    "简日",
    "简英",
    "chs",
    "zh-cn",
    "zh_hans",
    "zh-hans",
    "gb2312",
    "gbk",
)
TRADITIONAL_MARKERS = (
    "繁体",
    "繁中",
    "繁日",
    "繁英",
    "cht",
    "zh-tw",
    "zh-hk",
    "zh_hant",
    "zh-hant",
    "big5",
)
ENGLISH_MARKERS = ("english", "eng", "en")
JAPANESE_MARKERS = ("japanese", "jpn", "jp", "ja")
DEFAULT_LOCK_PREFIX = ".scraper-lock-"


def _fallback_collision_key(value: str) -> str:
    """Match the runtime collision key for standalone module callers."""
    normalized = unicodedata.normalize("NFC", value).casefold()
    if "/" in normalized:
        return "/".join(part.rstrip(" .") for part in normalized.split("/"))
    return normalized.rstrip(" .")


def _provider_safe_episode_title_impl(
    name: str,
    *,
    safe_name_fn: Callable[..., str] = safe_name,
) -> str:
    """Normalize episode-title tokens rejected by Quark rename APIs.

    This is string policy only; it does not contact or control Quark.
    """
    normalized = str(name)
    normalized = re.sub(
        r"(?<![A-Za-z0-9])OVA\s*0*(\d+)(?![A-Za-z0-9])",
        lambda match: f"特别篇 {int(match.group(1))}",
        normalized,
        flags=re.I,
    )
    normalized = re.sub(
        r"(?<![A-Za-z0-9])OVA(?![A-Za-z0-9])",
        "特别篇",
        normalized,
        flags=re.I,
    )
    return safe_name_fn(normalized)


def _limit_filename_impl(
    name: str,
    max_bytes: int = 240,
    *,
    truncate_utf8: Callable[[str, int], str] = _truncate_utf8,
    plan_error: type[Exception] = PlanError,
) -> str:
    suffix = Path(name).suffix
    stem = name[: -len(suffix)] if suffix else name
    budget = max_bytes - len(suffix.encode("utf-8"))
    if budget <= 0:
        raise plan_error(f"文件扩展名过长: {name}")
    return truncate_utf8(stem, budget) + suffix


def _compose_filename_impl(
    base: str,
    semantic_suffix: str,
    extension: str,
    max_bytes: int = 240,
    *,
    truncate_utf8: Callable[[str, int], str] = _truncate_utf8,
    plan_error: type[Exception] = PlanError,
) -> str:
    tail = f"{semantic_suffix}{extension}"
    budget = max_bytes - len(tail.encode("utf-8"))
    if budget <= 0:
        raise plan_error(f"文件名后缀过长: {tail}")
    limited_base = truncate_utf8(base, budget)
    return f"{limited_base}{tail}"


def _is_scraper_temp_impl(
    name: str,
    *,
    collision_key: Callable[[str], str] = _fallback_collision_key,
) -> bool:
    return collision_key(name).startswith(".scraper-tmp-")


def _is_scraper_lock_impl(
    name: str,
    *,
    collision_key: Callable[[str], str] = _fallback_collision_key,
    lock_prefix: str = DEFAULT_LOCK_PREFIX,
) -> bool:
    return collision_key(name).startswith(lock_prefix)


def _should_ignore_extra_impl(
    name: str,
    *,
    ignored_extra_tag_re: re.Pattern[str] = IGNORED_EXTRA_TAG_RE,
) -> bool:
    return bool(ignored_extra_tag_re.search(name))


def _is_sample_impl(
    name: str,
    *,
    sample_re: re.Pattern[str] = SAMPLE_RE,
) -> bool:
    return bool(sample_re.search(name))


def _bonus_type_impl(
    name: str,
    *,
    bonus_patterns: tuple[tuple[str, re.Pattern[str]], ...] = BONUS_PATTERNS,
) -> str | None:
    for label, pattern in bonus_patterns:
        if pattern.search(name):
            return label
    return None


def _is_planned_bonus_impl(
    name: str,
    *,
    planned_bonus_suffix_re: re.Pattern[str] = PLANNED_BONUS_SUFFIX_RE,
) -> bool:
    """Recognize only this tool's generated Infuse bonus suffixes."""
    return bool(planned_bonus_suffix_re.search(Path(name).stem))


def _edition_tag_impl(
    name: str,
    *,
    safe_name_fn: Callable[..., str] = safe_name,
    edition_patterns: tuple[tuple[str, re.Pattern[str]], ...] = EDITION_PATTERNS,
) -> str | None:
    explicit = re.search(r"\{edition-([^{}]+)\}", name, re.IGNORECASE)
    if explicit:
        return safe_name_fn(explicit.group(1), max_bytes=60)
    for label, pattern in edition_patterns:
        if pattern.search(name):
            return label
    return None


def _entry_edition_tag_impl(
    item: Mapping[str, Any],
    *,
    edition_tag_fn: Callable[[str], str | None] = _edition_tag_impl,
    safe_name_fn: Callable[..., str] = safe_name,
) -> str | None:
    """Detect a named edition from either the file or its enclosing folder."""
    override = item.get("_edition_override")
    if isinstance(override, str) and override.strip():
        return safe_name_fn(override.strip(), max_bytes=60)
    return edition_tag_fn(f"{item.get('name', '')} {item.get('full_path', '')}")


def _token_present_impl(text: str, marker: str) -> bool:
    if marker.isascii():
        return bool(
            re.search(
                rf"(?:^|[.\-_\[\]()\s]){re.escape(marker)}(?:$|[.\-_\[\]()\s])",
                text,
            )
        )
    return marker in text


def _subtitle_language_impl(
    name: str,
    *,
    token_present_fn: Callable[[str, str], bool] = _token_present_impl,
    simplified_markers: tuple[str, ...] = SIMPLIFIED_MARKERS,
    traditional_markers: tuple[str, ...] = TRADITIONAL_MARKERS,
    english_markers: tuple[str, ...] = ENGLISH_MARKERS,
    japanese_markers: tuple[str, ...] = JAPANESE_MARKERS,
) -> str | None:
    lower = name.lower()
    if any(token_present_fn(lower, marker) for marker in simplified_markers):
        return "zh-CN"
    if re.search(r"(?:^|[.\-_\[\]()\s])(?:sc|简)(?:$|[.\-_\[\]()\s])", lower):
        return "zh-CN"
    if any(token_present_fn(lower, marker) for marker in traditional_markers):
        return "zh-TW"
    if re.search(r"(?:^|[.\-_\[\]()\s])(?:tc|繁)(?:$|[.\-_\[\]()\s])", lower):
        return "zh-TW"
    if any(token_present_fn(lower, marker) for marker in english_markers):
        return "en"
    if any(token_present_fn(lower, marker) for marker in japanese_markers):
        return "ja"
    return None


def _is_traditional_sub_impl(
    name: str,
    *,
    subtitle_language_fn: Callable[[str], str | None] = _subtitle_language_impl,
) -> bool:
    return subtitle_language_fn(name) == "zh-TW"


def _is_simplified_sub_impl(
    name: str,
    *,
    subtitle_language_fn: Callable[[str], str | None] = _subtitle_language_impl,
) -> bool:
    return subtitle_language_fn(name) == "zh-CN"


def provider_safe_episode_title(name: str) -> str:
    return _provider_safe_episode_title_impl(name)


def _limit_filename(name: str, max_bytes: int = 240) -> str:
    return _limit_filename_impl(name, max_bytes)


def _compose_filename(
    base: str, semantic_suffix: str, extension: str, max_bytes: int = 240,
) -> str:
    return _compose_filename_impl(base, semantic_suffix, extension, max_bytes)


def is_scraper_temp(name: str) -> bool:
    return _is_scraper_temp_impl(name)


def is_scraper_lock(name: str) -> bool:
    return _is_scraper_lock_impl(name)


def should_ignore_extra(name: str) -> bool:
    return _should_ignore_extra_impl(name)


def is_sample(name: str) -> bool:
    return _is_sample_impl(name)


def bonus_type(name: str) -> str | None:
    return _bonus_type_impl(name)


def is_planned_bonus(name: str) -> bool:
    return _is_planned_bonus_impl(name)


def edition_tag(name: str) -> str | None:
    return _edition_tag_impl(name)


def entry_edition_tag(item: Mapping[str, Any]) -> str | None:
    return _entry_edition_tag_impl(item)


def _token_present(text: str, marker: str) -> bool:
    return _token_present_impl(text, marker)


def subtitle_language(name: str) -> str | None:
    return _subtitle_language_impl(name)


def is_traditional_sub(name: str) -> bool:
    return _is_traditional_sub_impl(name)


def is_simplified_sub(name: str) -> bool:
    return _is_simplified_sub_impl(name)


__all__ = [
    "BONUS_PATTERNS",
    "EDITION_PATTERNS",
    "ENGLISH_MARKERS",
    "IGNORED_EXTRA_TAG_RE",
    "JAPANESE_MARKERS",
    "PLANNED_BONUS_SUFFIX_RE",
    "SAMPLE_RE",
    "SIMPLIFIED_MARKERS",
    "TRADITIONAL_MARKERS",
    "_compose_filename",
    "_limit_filename",
    "_token_present",
    "bonus_type",
    "edition_tag",
    "entry_edition_tag",
    "is_planned_bonus",
    "is_sample",
    "is_scraper_lock",
    "is_scraper_temp",
    "is_simplified_sub",
    "is_traditional_sub",
    "provider_safe_episode_title",
    "should_ignore_extra",
    "subtitle_language",
]


_COMPAT_RUNTIME: ModuleType | None = None
_COMPAT_IMPLEMENTATIONS = {name: globals()[name] for name in __all__ if callable(globals()[name])}


def _compat_dispatch(name: str):
    original = _COMPAT_IMPLEMENTATIONS[name]

    def dispatch(*args: Any, **kwargs: Any) -> Any:
        runtime = _COMPAT_RUNTIME
        current = getattr(runtime, name, original) if runtime is not None else original
        if current is not original and current is not dispatch:
            return current(*args, **kwargs)
        return original(*args, **kwargs)

    dispatch.__name__ = name
    dispatch.__qualname__ = name
    dispatch.__doc__ = original.__doc__
    return dispatch


def bind_compat_runtime(runtime: ModuleType) -> None:
    """Keep direct extracted-module calls visible to runtime overrides."""
    global _COMPAT_RUNTIME
    _COMPAT_RUNTIME = runtime
    for name in _COMPAT_IMPLEMENTATIONS:
        current = globals().get(name)
        if current is _COMPAT_IMPLEMENTATIONS[name]:
            globals()[name] = _compat_dispatch(name)
