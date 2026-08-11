"""Narrow provider-staging paths for production and isolated acceptance.

Provider acquisition may write only below one reviewed staging parent.  The
production parent remains frozen at ``/quark/影视/ScrapeFlow/补源``.  A real
isolated acceptance run may instead use exactly one named root below
``/quark/影视/ScrapeFlow/验收``; its provider parent is derived, never supplied
as an independent arbitrary path.
"""

from __future__ import annotations

import posixpath
import re


PRODUCTION_MEDIA_ROOT = "/quark/影视"
ACCEPTANCE_MEDIA_ROOT_PARENT = f"{PRODUCTION_MEDIA_ROOT}/ScrapeFlow/验收"
REPLENISHMENT_STAGING_SUFFIX = "/ScrapeFlow/补源"
CANONICAL_REPLENISHMENT_STAGING_ROOT = (
    f"{PRODUCTION_MEDIA_ROOT}{REPLENISHMENT_STAGING_SUFFIX}"
)
_ACCEPTANCE_RUN_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")


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


def validate_acceptance_media_root(value: object) -> str:
    """Accept exactly ``.../ScrapeFlow/验收/<safe-run-id>``.

    The single run-id segment prevents an acceptance declaration from using a
    broad parent, a formal shelf, Provider staging itself, or a nested root
    that another invocation could also select.
    """

    root = _safe_absolute_remote_path(value, label="acceptance media root")
    prefix = ACCEPTANCE_MEDIA_ROOT_PARENT + "/"
    if not root.startswith(prefix):
        raise ProviderStagingPathError(
            "acceptance media root must be below the fixed acceptance namespace"
        )
    run_id = root[len(prefix):]
    if "/" in run_id or _ACCEPTANCE_RUN_ID.fullmatch(run_id) is None:
        raise ProviderStagingPathError(
            "acceptance media root must end in one safe run identifier"
        )
    return root


def validate_provider_media_root(value: object) -> str:
    """Accept the production media root or one exact isolated acceptance root."""

    root = _safe_absolute_remote_path(value, label="provider media root")
    if root == PRODUCTION_MEDIA_ROOT:
        return root
    return validate_acceptance_media_root(root)


def replenishment_staging_root_for_media_root(value: object) -> str:
    """Derive the only Provider staging parent for a configured media root."""

    media_root = validate_provider_media_root(value)
    if media_root == PRODUCTION_MEDIA_ROOT:
        return CANONICAL_REPLENISHMENT_STAGING_ROOT
    return f"{media_root}{REPLENISHMENT_STAGING_SUFFIX}"


def validate_provider_staging_root(value: object) -> str:
    """Accept only a staging parent exactly derived from an allowed media root."""

    root = _safe_absolute_remote_path(value, label="provider staging root")
    if root == CANONICAL_REPLENISHMENT_STAGING_ROOT:
        return root
    if not root.endswith(REPLENISHMENT_STAGING_SUFFIX):
        raise ProviderStagingPathError(
            "provider staging root must be the exact derived staging suffix"
        )
    media_root = root[: -len(REPLENISHMENT_STAGING_SUFFIX)]
    try:
        expected = replenishment_staging_root_for_media_root(media_root)
    except ProviderStagingPathError as exc:
        raise ProviderStagingPathError(
            "provider staging root is outside the accepted media-root namespace"
        ) from exc
    if root != expected:
        raise ProviderStagingPathError(
            "provider staging root must equal the media root's derived staging path"
        )
    return root


def media_root_for_provider_staging_root(value: object) -> str:
    """Return the allowed media root that uniquely owns a staging parent."""

    root = validate_provider_staging_root(value)
    if root == CANONICAL_REPLENISHMENT_STAGING_ROOT:
        return PRODUCTION_MEDIA_ROOT
    return root[: -len(REPLENISHMENT_STAGING_SUFFIX)]


__all__ = [
    "ACCEPTANCE_MEDIA_ROOT_PARENT",
    "CANONICAL_REPLENISHMENT_STAGING_ROOT",
    "PRODUCTION_MEDIA_ROOT",
    "ProviderStagingPathError",
    "REPLENISHMENT_STAGING_SUFFIX",
    "media_root_for_provider_staging_root",
    "replenishment_staging_root_for_media_root",
    "validate_acceptance_media_root",
    "validate_provider_media_root",
    "validate_provider_staging_root",
]
