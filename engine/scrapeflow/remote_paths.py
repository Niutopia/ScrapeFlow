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


def safe_name(name: str, max_bytes: int = 160) -> str:
    normalized = unicodedata.normalize("NFC", str(name))
    cleaned = "".join(
        "-" if unicodedata.category(char) in {"Cc", "Cf", "Cs"} else char
        for char in normalized
    )
    cleaned = re.sub(r"[/\\:*?\"<>|]", "-", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip(" .")
    cleaned = _truncate_utf8(cleaned, max_bytes)
    return cleaned or "未命名"


__all__ = [
    "_has_unsafe_unicode",
    "_terminal_text",
    "_truncate_utf8",
    "join_remote",
    "normalize_remote_path",
    "safe_name",
    "split_remote",
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
