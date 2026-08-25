"""The one task-owned staging parent for provider acquisition."""

from __future__ import annotations

import posixpath


PRODUCTION_MEDIA_ROOT = "/quark/影视"
REPLENISHMENT_STAGING_SUFFIX = "/ScrapeFlow/补源"
CANONICAL_REPLENISHMENT_STAGING_ROOT = (
    f"{PRODUCTION_MEDIA_ROOT}{REPLENISHMENT_STAGING_SUFFIX}"
)


class ProviderStagingPathError(ValueError):
    """A configured Provider media or staging path is outside the contract."""


def _safe_absolute_remote_path(value: object, *, label: str) -> str:
    if (
        not isinstance(value, str)
        or not value
        or not value.startswith("/")
        or "\x00" in value
        or "\\" in value
    ):
        raise ProviderStagingPathError(f"{label} must be a safe absolute path")
    normalized = posixpath.normpath(value)
    if (
        normalized != value
        or normalized == "/"
        or any(part in {"", ".", ".."} for part in normalized.split("/")[1:])
    ):
        raise ProviderStagingPathError(f"{label} must be a normalized path")
    return normalized


def validate_provider_media_root(value: object) -> str:
    """Accept only ScrapeFlow's configured formal media root."""

    root = _safe_absolute_remote_path(value, label="provider media root")
    if root != PRODUCTION_MEDIA_ROOT:
        raise ProviderStagingPathError(
            "provider media root must be /quark/影视"
        )
    return root


def replenishment_staging_root_for_media_root(value: object) -> str:
    """Derive the only Provider staging parent for a configured media root."""

    validate_provider_media_root(value)
    return CANONICAL_REPLENISHMENT_STAGING_ROOT


def validate_provider_staging_root(value: object) -> str:
    """Accept only the fixed task-owned replenishment parent."""

    root = _safe_absolute_remote_path(value, label="provider staging root")
    if root != CANONICAL_REPLENISHMENT_STAGING_ROOT:
        raise ProviderStagingPathError(
            "provider staging root must be /quark/影视/ScrapeFlow/补源"
        )
    return root


def media_root_for_provider_staging_root(value: object) -> str:
    """Return the formal media root that uniquely owns the staging parent."""

    validate_provider_staging_root(value)
    return PRODUCTION_MEDIA_ROOT


__all__ = [
    "CANONICAL_REPLENISHMENT_STAGING_ROOT",
    "PRODUCTION_MEDIA_ROOT",
    "ProviderStagingPathError",
    "REPLENISHMENT_STAGING_SUFFIX",
    "media_root_for_provider_staging_root",
    "replenishment_staging_root_for_media_root",
    "validate_provider_media_root",
    "validate_provider_staging_root",
]
