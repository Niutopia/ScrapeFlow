"""Common provider delivery contract for replenishment materializers."""

from __future__ import annotations

import posixpath
from collections.abc import Mapping

from engine.scrapeflow.media_policy import SUBTITLE_EXTENSIONS, VIDEO_EXTENSIONS

from .provider_staging import (
    CANONICAL_REPLENISHMENT_STAGING_ROOT,
    ProviderStagingPathError,
    validate_provider_staging_root,
)
from .replenishment_tiers import (
    TIER_LOCAL_MAGNET,
    TIER_QUARK_MAGNET,
    TIER_QUARK_SHARE,
)


ALLOWED_DELIVERY_LANES = frozenset({
    TIER_QUARK_SHARE,
    TIER_QUARK_MAGNET,
    TIER_LOCAL_MAGNET,
})
ALLOWED_DELIVERY_KINDS = frozenset({"video", "subtitle"})
ALLOWED_DELIVERY_KEYS = frozenset({
    "lane", "attempt_id", "staging_root", "files", "external_task_id",
})
ALLOWED_DELIVERY_FILE_KEYS = frozenset({"path", "size", "kind", "gap_ids"})
FORBIDDEN_DELIVERY_KEYS = frozenset({
    "formal_path",
    "target_root",
    "destination_parent",
    "movie_root",
    "tv_root",
})
# Retain the public constant for callers and persisted-contract tests.  Its
# value is deliberately the unchanged production staging parent.
DEFAULT_DELIVERY_PARENT = CANONICAL_REPLENISHMENT_STAGING_ROOT


class ProviderDeliveryError(ValueError):
    """A materializer returned an unsafe delivery object."""


def _reject_forbidden_keys(value: object, *, path: str = "$") -> None:
    if isinstance(value, Mapping):
        forbidden = sorted(FORBIDDEN_DELIVERY_KEYS & set(value))
        if forbidden:
            raise ProviderDeliveryError(
                f"delivery {path} 包含正式库字段: {', '.join(forbidden)}"
            )
        for key, nested in value.items():
            _reject_forbidden_keys(nested, path=f"{path}.{key}")
    elif isinstance(value, list):
        for index, nested in enumerate(value):
            _reject_forbidden_keys(nested, path=f"{path}[{index}]")


def _safe_remote_path(value: object, *, label: str) -> str:
    if not isinstance(value, str) or not value.startswith("/") or "\x00" in value or "\\" in value:
        raise ProviderDeliveryError(f"{label} 不是安全绝对路径")
    normalized = posixpath.normpath(value)
    if normalized != value or normalized == "/":
        raise ProviderDeliveryError(f"{label} 不是规范化路径")
    if any(part in {"", ".", ".."} for part in normalized.split("/")[1:]):
        raise ProviderDeliveryError(f"{label} 含有不安全路径段")
    return normalized


def _safe_token(value: object, *, label: str) -> str:
    if not isinstance(value, str) or not value or len(value) > 128:
        raise ProviderDeliveryError(f"{label} 无效")
    if any(char in value for char in ("/", "\\", "\x00", "\n", "\r")):
        raise ProviderDeliveryError(f"{label} 不安全")
    return value


def _expected_staging_root(parent: str, root_job_id: str, attempt_id: str) -> str:
    return f"{parent.rstrip('/')}/{root_job_id}/{attempt_id}"


def validate_provider_delivery(
    delivery: Mapping[str, object],
    *,
    root_job_id: str,
    attempt_id: str,
    staging_parent: str = DEFAULT_DELIVERY_PARENT,
) -> dict[str, object]:
    """Validate and normalize a materializer delivery result.

    This is the shared boundary for all three acquisition lanes.  A delivery
    may describe only task-owned staging bytes; it must not carry any formal
    library destination, because Engine remains the sole naming/planning
    authority after admission.
    """
    if not isinstance(delivery, Mapping):
        raise ProviderDeliveryError("delivery 必须是对象")
    _reject_forbidden_keys(delivery)
    unexpected = sorted(str(key) for key in set(delivery) - ALLOWED_DELIVERY_KEYS)
    if unexpected:
        raise ProviderDeliveryError(
            f"delivery 包含合同外字段: {', '.join(unexpected)}"
        )
    lane = delivery.get("lane")
    if lane not in ALLOWED_DELIVERY_LANES:
        raise ProviderDeliveryError("delivery lane 无效")
    expected_attempt = _safe_token(attempt_id, label="attempt_id")
    delivered_attempt = _safe_token(delivery.get("attempt_id"), label="delivery attempt_id")
    if delivered_attempt != expected_attempt:
        raise ProviderDeliveryError("delivery attempt_id 不匹配")
    safe_root_job = _safe_token(root_job_id, label="root_job_id")
    try:
        parent = validate_provider_staging_root(staging_parent)
    except ProviderStagingPathError as exc:
        raise ProviderDeliveryError("staging_parent 不属于受管 Provider staging 根") from exc
    expected_root = _expected_staging_root(parent, safe_root_job, expected_attempt)
    staging_root = _safe_remote_path(delivery.get("staging_root"), label="staging_root")
    if staging_root != expected_root:
        raise ProviderDeliveryError("delivery staging_root 不属于当前 attempt")
    rows = delivery.get("files")
    if not isinstance(rows, list) or not rows:
        raise ProviderDeliveryError("delivery files 不能为空")

    normalized_files: list[dict[str, object]] = []
    seen_paths: set[str] = set()
    for index, raw in enumerate(rows):
        if not isinstance(raw, Mapping):
            raise ProviderDeliveryError("delivery files 项必须是对象")
        unexpected_file = sorted(
            str(key) for key in set(raw) - ALLOWED_DELIVERY_FILE_KEYS
        )
        if unexpected_file:
            raise ProviderDeliveryError(
                f"files[{index}] 包含合同外字段: {', '.join(unexpected_file)}"
            )
        path = _safe_remote_path(raw.get("path"), label=f"files[{index}].path")
        if not path.startswith(staging_root + "/"):
            raise ProviderDeliveryError("delivery 文件超出当前 staging_root")
        if path in seen_paths:
            raise ProviderDeliveryError("delivery 文件路径重复")
        seen_paths.add(path)
        size = raw.get("size")
        if isinstance(size, bool) or not isinstance(size, int) or size <= 0:
            raise ProviderDeliveryError("delivery 文件大小无效")
        kind = raw.get("kind")
        if kind not in ALLOWED_DELIVERY_KINDS:
            raise ProviderDeliveryError("delivery 文件 kind 无效")
        suffix = posixpath.splitext(path)[1].casefold()
        if kind == "video" and suffix not in VIDEO_EXTENSIONS:
            raise ProviderDeliveryError("delivery 视频扩展名无效")
        if kind == "subtitle" and suffix not in SUBTITLE_EXTENSIONS:
            raise ProviderDeliveryError("delivery 字幕扩展名无效")
        gap_ids = raw.get("gap_ids")
        if not isinstance(gap_ids, list) or not gap_ids:
            raise ProviderDeliveryError("delivery 文件缺少 gap_ids")
        normalized_gap_ids = [
            _safe_token(gap_id, label="gap_id")
            for gap_id in gap_ids
        ]
        if len(normalized_gap_ids) != len(set(normalized_gap_ids)):
            raise ProviderDeliveryError("delivery 文件 gap_ids 重复")
        normalized_files.append({
            "path": path,
            "size": size,
            "kind": kind,
            "gap_ids": normalized_gap_ids,
        })

    result: dict[str, object] = {
        "lane": lane,
        "attempt_id": delivered_attempt,
        "staging_root": staging_root,
        "files": normalized_files,
    }
    if "external_task_id" in delivery:
        result["external_task_id"] = _safe_token(
            delivery.get("external_task_id"), label="external_task_id",
        )
    return result


__all__ = [
    "ALLOWED_DELIVERY_LANES",
    "ALLOWED_DELIVERY_KEYS",
    "ALLOWED_DELIVERY_FILE_KEYS",
    "DEFAULT_DELIVERY_PARENT",
    "FORBIDDEN_DELIVERY_KEYS",
    "ProviderDeliveryError",
    "validate_provider_delivery",
]
