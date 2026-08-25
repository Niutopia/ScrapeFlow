"""Canonical remote-path and provider-safe filename primitives.

These functions are deliberately independent from the Engine runtime.  They
validate path boundaries before any AList operation and are shared by identity
matching, planning and execution.
"""

from __future__ import annotations

import re
import unicodedata
from types import ModuleType
from typing import Any


# AList forwards rename requests to the backing provider.  Several providers
# reject traversal-looking dot runs and Unicode compatibility spellings of
# reserved ASCII characters even though the literal Unicode code point is not
# one of the usual POSIX separators.  Keep this policy here, at the shared
# filename boundary, rather than teaching individual planners about titles.
_PROVIDER_RESERVED_BASENAME_CHARS = frozenset('/\\:*?"<>|')


def _has_unsafe_unicode(text: str) -> bool:
    return any(unicodedata.category(char) in {"Cc", "Cf", "Cs"} for char in text)


def _terminal_text(value: Any) -> str:
    return "".join(
        "?" if unicodedata.category(char) in {"Cc", "Cf", "Cs"} else char
        for char in str(value)
    )


def normalize_remote_path(path: str) -> str:
    if not isinstance(path, str):
        raise ValueError("远端路径必须是字符串")
    parts: list[str] = []
    for part in path.replace("\\", "/").split("/"):
        if not part:
            continue
        if part in {".", ".."}:
            raise ValueError(f"远端路径不能包含 {part!r} 段: {path}")
        if _has_unsafe_unicode(part):
            raise ValueError("远端路径不能包含控制或不可见格式字符")
        parts.append(part)
    return "/" + "/".join(parts) if parts else "/"


def join_remote(parent: str, name: str) -> str:
    return normalize_remote_path(f"{normalize_remote_path(parent).rstrip('/')}/{name}")


def split_remote(path: str) -> tuple[str, str]:
    normalized = normalize_remote_path(path).rstrip("/")
    parent, _, name = normalized.rpartition("/")
    return parent or "/", name


def _truncate_utf8(text: str, max_bytes: int) -> str:
    encoded = text.encode("utf-8")
    if len(encoded) <= max_bytes:
        return text
    output: list[str] = []
    used = 0
    for char in text:
        size = len(char.encode("utf-8"))
        if used + size > max_bytes:
            break
        output.append(char)
        used += size
    return "".join(output).rstrip(" .")


def _compatibility_text(char: str) -> str:
    """Return the one-character compatibility projection used by providers."""
    return unicodedata.normalize("NFKC", char)


def _compatibility_dot_projection(value: str) -> str:
    """Expose compatibility dots so ``..`` checks cannot be bypassed.

    ``…`` expands to three ASCII dots under NFKC while a fullwidth dot maps
    to one.  The predicate deliberately keeps that expansion, because its
    purpose is detecting provider-visible traversal-like runs rather than
    producing display text.
    """
    output: list[str] = []
    for char in value:
        compatible = _compatibility_text(char)
        if compatible and all(piece == "." for piece in compatible):
            output.append(compatible)
        else:
            output.append(char)
    return "".join(output)


def provider_safe_basename(name: str, max_bytes: int = 240) -> str:
    """Return a deterministic basename safe for AList-backed providers.

    The function retains ordinary Unicode titles, but removes both literal
    reserved characters and their NFKC compatibility spellings.  It also
    collapses every provider-visible ``..`` run (including fullwidth-dot and
    ellipsis forms) before truncating by UTF-8 bytes.
    """
    if isinstance(max_bytes, bool) or not isinstance(max_bytes, int) or max_bytes <= 0:
        raise ValueError("文件名 UTF-8 字节上限必须为正整数")
    normalized = unicodedata.normalize("NFC", str(name))
    output: list[str] = []
    for char in normalized:
        if unicodedata.category(char) in {"Cc", "Cf", "Cs"}:
            output.append("-")
            continue
        compatible = _compatibility_text(char)
        if any(piece in _PROVIDER_RESERVED_BASENAME_CHARS for piece in compatible):
            output.append("-")
            continue
        # Canonicalize every compatibility dot to one literal dot.  A later
        # run collapse catches both literal ``...`` and visually similar
        # forms such as ``…`` / ``．．`` without changing unrelated CJK
        # punctuation.
        if compatible and all(piece == "." for piece in compatible):
            output.append(".")
            continue
        output.append(char)
    cleaned = "".join(output)
    cleaned = re.sub(r"\.{2,}", "-", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip(" .")
    cleaned = _truncate_utf8(cleaned, max_bytes)
    return cleaned or "未命名"


def is_provider_safe_basename(name: str) -> bool:
    """Whether ``name`` can be sent unchanged to an AList rename endpoint."""
    if not isinstance(name, str) or not name or name in {".", ".."}:
        return False
    if _has_unsafe_unicode(name):
        return False
    for char in name:
        compatible = _compatibility_text(char)
        if any(piece in _PROVIDER_RESERVED_BASENAME_CHARS for piece in compatible):
            return False
    return ".." not in _compatibility_dot_projection(name)


def validate_provider_safe_basename(name: str) -> str:
    """Return a provider-safe basename or raise before an AList side effect."""
    if not is_provider_safe_basename(name):
        raise ValueError(f"远端文件名不符合 AList 安全命名规则: {name!r}")
    return name


def safe_name(name: str, max_bytes: int = 160) -> str:
    """Compatibility alias for deterministic provider-safe display names."""
    return provider_safe_basename(name, max_bytes=max_bytes)


__all__ = [
    "_has_unsafe_unicode",
    "_terminal_text",
    "_truncate_utf8",
    "is_provider_safe_basename",
    "join_remote",
    "normalize_remote_path",
    "provider_safe_basename",
    "safe_name",
    "split_remote",
    "validate_provider_safe_basename",
]


_COMPAT_RUNTIME: ModuleType | None = None
_COMPAT_IMPLEMENTATIONS = {name: globals()[name] for name in __all__}


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
    """Keep runtime overrides visible to composed path primitives."""
    global _COMPAT_RUNTIME
    _COMPAT_RUNTIME = runtime
    for name in _COMPAT_IMPLEMENTATIONS:
        current = globals().get(name)
        if current is _COMPAT_IMPLEMENTATIONS[name]:
            globals()[name] = _compat_dispatch(name)
