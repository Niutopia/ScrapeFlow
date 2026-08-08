"""Credential-safe projections for local API responses and durable state."""

from __future__ import annotations

import os
import re
from collections.abc import Mapping

from engine.scrapeflow.clients.http import redact_sensitive_text


_SENSITIVE_FIELD_NAMES = frozenset({
    "apikey", "authorization", "password", "passwd", "pass", "archivepass",
    "archivepassword", "token", "accesstoken", "xapikey",
})
_SECRET_ENV_SUFFIXES = ("_password", "_token", "_api_key", "_secret")
_SPACED_SECRET_FIELD_RE = re.compile(
    r'(?i)(\b(?:api[\s_-]?key|archive[\s_-]?pass(?:word)?|access[\s_-]?token)\s*[:=]\s*)'
    r'("?)([^"\s,;&}]+)("?)'
)


def runtime_secret_values() -> tuple[str, ...]:
    """Read configured secrets solely to remove them from state/output text."""
    return tuple(
        value
        for name, value in os.environ.items()
        if name.casefold().endswith(_SECRET_ENV_SUFFIXES) and value
    )


def is_sensitive_field(value: object) -> bool:
    """Recognize common secret-bearing JSON keys, including local aliases."""
    if not isinstance(value, str):
        return False
    normalized = re.sub(r"[\s_-]", "", value.casefold())
    return (
        normalized in _SENSITIVE_FIELD_NAMES
        or normalized.endswith(("password", "passwd", "token", "apikey", "secret"))
    )


def redact_text(value: object) -> str:
    """Remove known credentials and key/value-style secret text."""
    cleaned = redact_sensitive_text(str(value), runtime_secret_values())
    return _SPACED_SECRET_FIELD_RE.sub(r'\1"<redacted>"', cleaned)


def redact_error(error: object) -> str:
    """Return a bounded, credential-safe exception/error message."""
    text = str(error) if error is not None else ""
    return redact_text(text or (type(error).__name__ if error is not None else ""))


def redact_value(value: object, *, sensitive: bool = False) -> object:
    """Recursively redact API or durable-state values without mutating input."""
    if sensitive:
        return "<redacted>" if value is not None else None
    if isinstance(value, BaseException):
        # Exception objects are not JSON serializable, and their ``str`` may
        # contain credentials supplied by a remote client/provider.  Convert
        # them at the boundary instead of letting an incidental JSON encoder
        # failure bypass the safe error projection.
        return redact_error(value)
    if isinstance(value, str):
        return redact_text(value)
    if isinstance(value, Mapping):
        return {
            key: redact_value(item, sensitive=is_sensitive_field(key))
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [redact_value(item) for item in value]
    if isinstance(value, tuple):
        return [redact_value(item) for item in value]
    return value


__all__ = [
    "is_sensitive_field",
    "redact_error",
    "redact_text",
    "redact_value",
    "runtime_secret_values",
]
