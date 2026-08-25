"""TMDB/AList clients and deterministic media planning.

This module reads remote metadata and builds an in-memory plan.  Remote writes,
job persistence, readback and cleanup belong to the local automatic runner.
"""

from __future__ import annotations

import contextlib
import copy
import difflib
import http.client
import ipaddress
import json
import os
import posixpath
import re
import socket
import time
import unicodedata
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from collections import defaultdict
from datetime import date, datetime
from pathlib import Path
import sys
from typing import Any, Iterable, Mapping, Sequence

from . import identity_matching as _identity_matching
from . import media_naming as _media_naming
from . import media_quality as _media_quality
from . import media_policy as _media_policy
from . import plan_artifacts as _plan_artifacts
from . import remote_paths as _remote_paths
from .data.release_lexicon import (
    FRACTIONAL_SPECIAL_ALIASES,
    NORMALIZED_PARENT_ALIASES,
    RELEASE_EDITION_RULES,
    SPECIAL_CONTEXT_RELEASE_TOKENS,
    SPECIAL_LABEL_RULES,
)
from .canonical_work_tree import (
    CanonicalTreeError,
    CanonicalWork,
    WorkIdentity,
    plan_canonical_work_tree,
)
from .clients.http import (
    JsonHttpClient,
    ValidatingRedirectHandler,
    redact_sensitive_text as _redact_sensitive_text,
    redact_url as _redact_url,
)
from .current_plan import (
    _retain_one_subtitle_track_per_exact_video,
    load_json_text as _load_json_text,
)
from .errors import ApiError, FormalTargetConflictError, PlanError, ScraperError
from .subtitle_content import (
    EXPORTED_SRT_MAX_BYTES,
    EXPORTED_SRT_SUFFIX_RE,
    validate_managed_subtitle_content,
    validate_exported_srt_sidecar,
)
from .identity_matching import (
    AUTO_MATCH_MIN_MARGIN,
    _alternative_tmdb_titles,
    _clean_franchise_root_label,
    _cross_script_unique_match,
    _direct_tmdb_match,
    _explicit_release_season_episode,
    _extract_year,
    _franchise_member_queries,
    _media_context_from_source_and_target,
    _media_type_from_source_context,
    _normalize_match_title,
    _query_from_source,
    _search_item_titles,
    _search_query_variants,
    _season_from_series_variant,
    _season_from_source,
    _source_is_animation_library,
    _source_suggests_batch,
    _source_suggests_collection,
    _title_similarity,
    _tmdb_hint_from_source,
    _usable_release_title_query,
    auto_match_tmdb,
)
from .models import AutoMatch, EpisodeKey, Plan, PlannedCleanup, PlannedFile, PlannedProblem
from .placement import placement_for
from .planning import movie as _movie_planner
from .planning.movie import build_movie_plan
from .planning.tv import season_inference as _tv_season_inference
from .planning.tv import smart as _tv_smart_planner
from .planning.tv.season_inference import (
    _attach_unique_numbered_backup_subtitles,
    _child_work_query_variants,
    _detach_numbered_subgroups_from_mixed_movie_groups,
    _extract_runtime_proven_overflow_movies,
    _merge_broadcast_folders_into_long_tmdb_season,
    _merge_release_seasons_into_long_tmdb_season_by_major_gaps,
    _normalize_cumulative_season_episode_numbers,
    _probe_remote_duration_minutes,
    _proven_absolute_season_group_endpoint,
    _proven_missing_root_season_files,
    _proven_root_first_broadcast_block_files,
    _remap_complete_reset_absolute_season_groups,
    _runtime_matched_related_animation_movie,
    _season_parent_identity_queries,
    _tmdb_long_season_block_counts,
    _unique_backup_subtitle_release_owners,
)
from .planning.tv.smart import build_tv_plan_smart
from .remote_paths import (
    _has_unsafe_unicode,
    _terminal_text,
    _truncate_utf8,
    validate_provider_safe_basename,
    join_remote,
    normalize_remote_path,
    safe_name,
    split_remote,
)
from .replenishment_matching import release_dash_regular_episode
from .residual_policy import (
    classify_residual,
    cleanup_allowlist_reason,
    cleanup_reason_for,
)

__version__ = "4.0.0"
DEFAULT_ALIST_URL = "http://127.0.0.1:5244"
DEFAULT_TMDB_BASE = "https://api.themoviedb.org/3"
DEFAULT_IMAGE_BASE = "https://image.tmdb.org/t/p/original"
LOCK_PREFIX = ".scraper-lock-"
PROGRESS_PREFIX = "SCRAPEFLOW_PROGRESS "


def _trace_io(message: str) -> None:
    if os.getenv("SCRAPEFLOW_TRACE_IO") == "1":
        print(f"SCRAPEFLOW_TRACE_IO {message}", file=sys.stderr, flush=True)


VIDEO_EXTS = _media_policy.VIDEO_EXTENSIONS


def emit_progress(
    stage: str,
    *,
    completed: int,
    total: int,
    percent: float,
    message: str,
) -> None:
    payload = {
        "stage": stage,
        "completed": max(0, int(completed)),
        "total": max(0, int(total)),
        "percent": max(0.0, min(100.0, round(float(percent), 1))),
        "message": message,
    }
    print(PROGRESS_PREFIX + json.dumps(payload, ensure_ascii=False, separators=(",", ":")), flush=True)
SUBTITLE_EXTS = _media_policy.SUBTITLE_EXTENSIONS
MEDIA_EXTS = _media_policy.MEDIA_EXTENSIONS

# Pure filename/classification policy lives in ``media_naming``.  Keep these
# aliases in the runtime namespace because planner dispatch tables resolve
# collaborators from ``engine.scraper``.
IGNORED_EXTRA_TAG_RE = _media_naming.IGNORED_EXTRA_TAG_RE
TITLE_EXTRA_TAG_RE = re.compile(
    r"(?:^|[\s._\-\[\]()])EXTRAS?(?:$|[\s._\-\[\]()])",
    re.IGNORECASE,
)
DISPOSABLE_VIDEO_TAG_RE = re.compile(
    r"(?:^|[\s._\-\[\]()])(?:NCOP|NCED)(?:\d+(?:v\d+)?)?(?:$|[\s._\-\[\]()])|"
    r"(?:^|[\s._\-\[\]()])(?:MENU(?:OVA|MAIN)?|GAME[ ._-]*(?:OP|ED))(?:\d+)?(?:$|[\s._\-\[\]()])|"
    r"(?:メニュー映像|メニュー画面|菜单视频|菜單影片)|"
    r"(?:WEB予告|本予告|特報|CM集|ノンクレジット(?:OP|ED))|"
    r"\[\s*CM\s*\]|"
    r"(?:^|[\s._\-\[\]()])CM(?:\d{1,3}|[ ._-]*COLLECTION)(?:$|[\s._\-\[\]()])|"
    r"(?:SPONSOR|SPONSER)[ ._-]*EYECATCH[ ._-]*COLLECTION|"
    r"(?:^|[\s._\-\[()])EYE[ ._-]*CATCH(?:$|[\s._\-\])()])|"
    r"(?:THEME[ ._-]*SONG[ ._-]*MV|主题曲[ ._-]*MV)",
    re.IGNORECASE,
)
EPISODE_THEME_VARIANT_RE = re.compile(
    r"(?:^|[\s._\-\[\]()])(?:OP|ED)[ ._-]*EP[ ._-]*0*(\d{1,4})(?:$|[\s._\-\[\]()])",
    re.IGNORECASE,
)
BONUS_DIRECTORY_RE = re.compile(
    r"(?:^|/)(?:SPs?|Extras?|Bonus|Tokuten|特典|映像特典)(?:/|$)",
    re.IGNORECASE,
)
ADVERTISEMENT_IMAGE_RE = re.compile(
    r"(?:防失联.*(?:链接|联系)|海量.*(?:资源|合集)|资源文档|"
    r"(?:番剧|动漫|動漫|影视|影視)?合集文档|扫码|二维码|加群|公众号)",
    re.IGNORECASE,
)
NON_MEDIA_LIBRARY_CONTEXT_RE = re.compile(
    r"(?:^|/)(?:小说|同人|漫画|书籍|电子书|文库(?:版)?|短篇集|"
    r"txt|texts?|novels?|ebooks?|comics?)(?:/|$)",
    re.IGNORECASE,
)
ADVERTISEMENT_IMAGE_EXTS = {".jpg", ".jpeg", ".png", ".webp"}
FONT_RESOURCE_RE = re.compile(
    r"(?:字体(?:包|库|文件)?|字库|(?:^|[\s._\-\[\]()])fonts?(?:$|[\s._\-\[\]()]))",
    re.IGNORECASE,
)
FONT_RESOURCE_EXTS = {
    ".exe", ".bin", ".dat", ".zip", ".7z", ".rar",
    ".ttf", ".otf", ".ttc", ".woff", ".woff2",
}
SAMPLE_RE = _media_naming.SAMPLE_RE
BONUS_PATTERNS = _media_naming.BONUS_PATTERNS
PLANNED_BONUS_SUFFIX_RE = _media_naming.PLANNED_BONUS_SUFFIX_RE
BONUS_CONTAINER_RE = re.compile(
    r"^(?:extras?|trailers?|behind[ ._-]*the[ ._-]*scenes|deleted[ ._-]*scenes|"
    r"featurettes?|interviews?|scenes?|shorts?|花絮|预告|幕后|访谈)$",
    re.IGNORECASE,
)
EDITION_PATTERNS = _media_naming.EDITION_PATTERNS
MULTI_EPISODE_RE = re.compile(
    # The second endpoint must carry its own E/EP marker.  Making it
    # optional used to turn a title separator plus a title number into a
    # multi-episode range, e.g. ``S00E01 - 86 - Eighty-Six`` -> ``E01-E86``.
    # Explicit forms such as ``S01E01-E02`` remain accepted.
    r"(?:^|[^0-9])(?:E|EP)?\s*0*(\d{1,3})\s*[-~+&]\s*(?:E|EP)\s*0*(\d{1,3})(?:$|[^0-9])",
    re.IGNORECASE,
)
MULTI_EPISODE_CONCAT_RE = re.compile(
    r"(?:^|[^A-Za-z0-9])(?:S\d{1,2})?E\s*0*(\d{1,3})E\s*0*(\d{1,3})(?:$|[^0-9])",
    re.IGNORECASE,
)
SEASON_DASH_EPISODE_RE = re.compile(
    r"(?:^|[^A-Za-z0-9])S\d{1,2}\s*[-._ ]+\s*0*(\d{1,4})(?:$|[^0-9])",
    re.IGNORECASE,
)
DUAL_BRACKET_EPISODE_RE = re.compile(
    r"\[\s*0*(\d{1,3})\s*[_/]\s*0*(\d{1,3})\s*\]",
    re.IGNORECASE,
)
SEASON_LOCAL_ABSOLUTE_RE = re.compile(
    r"(?:S(?:eason)?\s*\d{1,3}|\d{1,3}(?:st|nd|rd|th)\s+Season)"
    r"\s*-\s*0*(\d{1,3})\s*\(\s*0*(\d{1,3})\s*\)",
    re.IGNORECASE,
)
EPISODE_NOISE_RE = re.compile(
    r"(?:4320|2160|1440|1080|720|576|480)[pi]|(?:4|8)k|x[._ ]?26[45]|h\.?26[45]|"
    r"10bit|8bit|av1|(?<!\d)(?:1|2|5|7)\.1(?!\d)|(?<!\d)(?:1|2)\.0(?!\d)",
    re.IGNORECASE,
)
DATE_NOISE_RE = re.compile(r"\b(?:19|20)\d{2}[-._]\d{1,2}[-._]\d{1,2}\b")

# A bare four-digit year (``2021``) is release metadata, never an episode
# number.  ``S01.2021`` therefore reads as ``Season 1, year 2021``, not
# ``Season 1, episode 2021``.
BARE_YEAR_NOISE_RE = re.compile(r"(?<!\d)(?:19|20)\d{2}(?!\d)")

# A release-group banner advertising a download site (``www.<domain>``) is an
# advertisement, not playable media.  It stays at source and never becomes a
# plan problem file.
ADVERTISEMENT_NAME_RE = re.compile(
    r"www\.[A-Za-z0-9.-]+\.(?:com|net|org|cn|tv|xyz|info|me)",
    re.IGNORECASE,
)

SIMPLIFIED_MARKERS = _media_naming.SIMPLIFIED_MARKERS
TRADITIONAL_MARKERS = _media_naming.TRADITIONAL_MARKERS
ENGLISH_MARKERS = _media_naming.ENGLISH_MARKERS
JAPANESE_MARKERS = _media_naming.JAPANESE_MARKERS


def _collision_key(value: str) -> str:
    """保守的跨平台冲突键：NFC、casefold，并忽略段尾空格和句点。"""
    normalized = unicodedata.normalize("NFC", value).casefold()
    if "/" in normalized:
        return "/".join(part.rstrip(" .") for part in normalized.split("/"))
    return normalized.rstrip(" .")


def _path_is_within(path: str, root: str) -> bool:
    path_key = _collision_key(normalize_remote_path(path).rstrip("/") or "/")
    root_key = _collision_key(normalize_remote_path(root).rstrip("/") or "/")
    return path_key == root_key or path_key.startswith(root_key.rstrip("/") + "/")


def _validate_alist_transport(base_url: str, *, allow_insecure_http: bool) -> str:
    try:
        parsed = urllib.parse.urlsplit(base_url)
    except ValueError as exc:
        raise ValueError(f"无效 AList URL: {base_url!r}") from exc
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        raise ValueError("AList URL 必须是完整的 http:// 或 https:// 地址")
    try:
        _ = parsed.port
    except ValueError as exc:
        raise ValueError("AList URL 包含无效端口") from exc
    if parsed.username is not None or parsed.password is not None:
        raise ValueError("AList URL 不得包含用户名或密码")
    if parsed.query or parsed.fragment:
        raise ValueError("AList URL 不得包含查询参数或片段")
    if parsed.scheme == "http" and not allow_insecure_http:
        host = parsed.hostname
        is_loopback = host.lower() == "localhost"
        if not is_loopback:
            try:
                is_loopback = ipaddress.ip_address(host).is_loopback
            except ValueError:
                is_loopback = False
        if not is_loopback:
            raise ScraperError(
                "远程 AList 使用 HTTP 会明文传输密码和 token；请改用 HTTPS，"
                "或明确传入 --allow-insecure-http 接受风险"
            )
    return base_url.rstrip("/")


def _entry_modified_value(entry: Mapping[str, Any]) -> str | None:
    for key in ("modified", "updated_at", "mtime", "last_modified"):
        value = entry.get(key)
        if value not in (None, ""):
            return str(value)
    return None


def _entry_size_value(entry: Mapping[str, Any]) -> int | None:
    value = entry.get("size")
    if isinstance(value, bool) or value is None:
        return None
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed >= 0 else None


def _planned_file_from_entry(
    item: Mapping[str, Any],
    *,
    final_name: str,
    target_dir: str,
    episode_key: str | None = None,
) -> "PlannedFile":
    source_path = normalize_remote_path(str(item["full_path"]))
    source_dir, original_name = split_remote(source_path)
    source_media_kind = (
        "subtitle"
        if isinstance(item.get("_subtitle_normalization"), Mapping)
        else None
    )
    subtitle_validation = item.get("_managed_subtitle_validation")
    if not isinstance(subtitle_validation, Mapping):
        subtitle_validation = None
    return PlannedFile(
        source_path=source_path,
        source_dir=source_dir,
        original_name=original_name,
        final_name=final_name,
        target_dir=target_dir,
        media_kind=source_media_kind or media_kind(original_name),
        episode_key=episode_key,
        source_size=_entry_size_value(item),
        source_modified=_entry_modified_value(item),
        source_media_kind=source_media_kind,
        subtitle_validation=(
            dict(subtitle_validation) if subtitle_validation is not None else None
        ),
    )


def _validate_remote_basename(name: str) -> str:
    if not isinstance(name, str) or not name or name in {".", ".."}:
        raise ValueError(f"无效远端文件名: {name!r}")
    if "/" in name or "\\" in name or _has_unsafe_unicode(name):
        raise ValueError(f"远端文件名包含非法或不可见控制字符: {name!r}")
    return validate_provider_safe_basename(name)


def _validate_remote_source_basename(name: str) -> str:
    """Validate an existing provider name without applying rename policy.

    Source releases commonly contain punctuation such as ``x264....mp4``.
    That is not a path traversal segment: the name is never sent as a target
    rename.  Keep the strict provider-safe validator for destinations, moves,
    and deletes, but let a read-only source listing retain ordinary internal
    dot runs while still rejecting separators, controls, compatibility
    separators, and the exact ``.``/``..`` path components.
    """
    if not isinstance(name, str) or not name or name in {".", ".."}:
        raise ValueError(f"无效远端源文件名: {name!r}")
    if "/" in name or "\\" in name or _has_unsafe_unicode(name):
        raise ValueError(f"远端源文件名包含非法或不可见字符: {name!r}")
    # Only compatibility forms that alter path segmentation remain unsafe.
    # A provider may already expose a real source object whose literal name
    # contains other compatibility punctuation (for example ``：``).  That
    # source spelling must be retained for reads and source-side moves; the
    # later rename into the formal library still uses the strict target-name
    # policy.
    compatible = unicodedata.normalize("NFKC", name)
    if "/" in compatible or "\\" in compatible:
        raise ValueError(f"远端源文件名包含兼容形式路径分隔符: {name!r}")
    if compatible in {".", ".."}:
        raise ValueError(f"远端源文件名不能是路径段: {name!r}")
    return name


class AListClient:
    def __init__(
        self,
        base_url: str,
        username: str,
        password: str,
        *,
        timeout: float = 20.0,
        retries: int = 3,
        allow_insecure_http: bool = False,
    ) -> None:
        self.base_url = _validate_alist_transport(
            base_url, allow_insecure_http=allow_insecure_http
        )
        self.username = username
        self.password = password
        # Keep the credential only for the lifetime of this local worker so an
        # AList restart can invalidate its token without permanently killing a
        # long-running job.  ``password`` remains cleared after the first login
        # to preserve the existing public/debug surface.
        self._auth_password = password
        self.http = JsonHttpClient(timeout=timeout, retries=retries)
        self.token: str | None = None

    def _validate_api_url(self, url: str) -> None:
        """Keep authenticated AList traffic on the configured origin."""
        try:
            candidate = urllib.parse.urlsplit(url)
            base = urllib.parse.urlsplit(self.base_url)
            candidate_port = candidate.port or (443 if candidate.scheme == "https" else 80)
            base_port = base.port or (443 if base.scheme == "https" else 80)
        except ValueError as exc:
            raise ApiError("AList API 返回了无效的重定向地址") from exc
        if (
            candidate.scheme != base.scheme
            or not candidate.hostname
            or not base.hostname
            or candidate.hostname.casefold() != base.hostname.casefold()
            or candidate_port != base_port
            or candidate.username is not None
            or candidate.password is not None
        ):
            raise ApiError("拒绝把 AList 凭据发送到不同来源")

    def login(self) -> str:
        response = self.http.request_json(
            f"{self.base_url}/api/auth/login",
            method="POST",
            json_body={"username": self.username, "password": self._auth_password},
            url_validator=self._validate_api_url,
        )
        self._require_success(response, "AList 登录")
        token = (response.get("data") or {}).get("token")
        if not isinstance(token, str) or not token:
            raise ApiError("AList 登录成功但未返回 token")
        self.token = token
        self.password = ""
        return token

    @staticmethod
    def _requires_reauthentication(response: Mapping[str, Any]) -> bool:
        code = response.get("code")
        message = str(response.get("message") or "").casefold()
        return code in {401, 403} or any(marker in message for marker in (
            "token is invalidated",
            "token invalidated",
            "invalid token",
            "token expired",
            "unauthorized",
            "未登录",
            "登录失效",
            "令牌失效",
        ))

    def _request_json_authenticated(self, url: str, **kwargs: Any) -> dict[str, Any]:
        """Retry exactly once after an authentication-only failure.

        AList returns authentication failures either as HTTP 401/403 or as a
        successful HTTP response containing ``code`` 401/403.  Replaying is
        safe here because the rejected request was not authorized and therefore
        could not have performed the requested file operation.
        """
        extra_headers = dict(kwargs.pop("headers", {}) or {})
        for attempt in range(2):
            try:
                response = self.http.request_json(
                    url,
                    headers={**self._headers(), **extra_headers},
                    url_validator=self._validate_api_url,
                    **kwargs,
                )
            except ApiError as exc:
                if attempt or exc.status_code not in {401, 403}:
                    raise
                self.login()
                continue
            if not self._requires_reauthentication(response) or attempt:
                return response
            self.login()
        raise ApiError("AList 重新登录后仍未通过认证")

    def _request_bytes_authenticated(
        self,
        url: str,
        *,
        headers: Mapping[str, str] | None = None,
        max_bytes: int,
    ) -> bytes:
        for attempt in range(2):
            try:
                return self.http.request_bytes(
                    url,
                    headers={**self._headers(), **dict(headers or {})},
                    max_bytes=max_bytes,
                    url_validator=self._validate_api_url,
                )
            except ApiError as exc:
                if attempt or exc.status_code not in {401, 403}:
                    raise
                self.login()
        raise ApiError("AList 重新登录后仍未通过认证")

    def _headers(self) -> dict[str, str]:
        if not self.token:
            raise ApiError("尚未登录 AList")
        return {"Authorization": self.token}

    def _require_success(self, response: Mapping[str, Any], action: str) -> dict[str, Any]:
        code = response.get("code")
        if code != 200:
            detail = response.get("message") or response
            raise ApiError(
                f"{action}失败: "
                f"{_redact_sensitive_text(str(detail), (self.password, self._auth_password, self.token or ''))}"
            )
        return dict(response)

    def call(
        self, endpoint: str, body: Mapping[str, Any], *, retryable: bool = False
    ) -> dict[str, Any]:
        response = self._request_json_authenticated(
            f"{self.base_url}/api/fs/{endpoint}",
            method="POST",
            json_body=body,
            retryable=retryable,
        )
        return self._require_success(response, f"AList {endpoint}")

    def admin_storages(self) -> list[dict[str, Any]]:
        """Return admin storage rows for in-process credential delegation.

        Callers must treat ``addition`` as secret material and keep it out of
        logs, artifacts, command arguments, and environment variables.
        """
        response = self._request_json_authenticated(
            f"{self.base_url}/api/admin/storage/list?page=1&per_page=1000",
            method="GET",
        )
        data = self._require_success(response, "AList storage list").get("data")
        content = data.get("content") if isinstance(data, Mapping) else None
        if not isinstance(content, list) or not all(isinstance(row, dict) for row in content):
            raise ApiError("AList storage list returned an invalid payload")
        return [dict(row) for row in content]

    def list(self, path: str, refresh: bool = False) -> list[dict[str, Any]]:
        """完整列出目录内容，显式处理 AList 的分页响应。"""
        normalized = normalize_remote_path(path)
        page = 1
        per_page = 500
        output: list[dict[str, Any]] = []
        seen_pages: set[int] = set()
        seen_signatures: set[tuple[str, ...]] = set()
        seen_entries: set[tuple[str, bool]] = set()

        while True:
            _trace_io(
                f"alist.list start path={normalized!r} page={page} "
                f"refresh={bool(refresh if page == 1 else False)}"
            )
            response = self.call(
                "list",
                {
                    "path": normalized,
                    "page": page,
                    "per_page": per_page,
                    # 只在第一页强制刷新，避免每翻一页都重新排列目录内容。
                    "refresh": refresh if page == 1 else False,
                },
                retryable=True,
            )
            _trace_io(f"alist.list done path={normalized!r} page={page}")
            data = response.get("data") or {}
            if not isinstance(data, Mapping):
                raise ApiError(f"AList list 返回格式异常: {normalized}")
            content = data.get("content")
            if content is None:
                content = []
            if not isinstance(content, list):
                raise ApiError(f"AList list 返回格式异常: {normalized}")
            for item in content:
                if not isinstance(item, dict):
                    continue
                name = item.get("name")
                if isinstance(name, str):
                    entry_key = (name, bool(item.get("is_dir")))
                    if entry_key in seen_entries:
                        raise ApiError(
                            f"AList list 分页包含重复条目，目录可能在扫描中发生变化: "
                            f"{normalized}/{name}"
                        )
                    seen_entries.add(entry_key)
                output.append(dict(item))

            signature = tuple(
                str(item.get("name", "")) for item in content if isinstance(item, dict)
            )
            if signature and signature in seen_signatures:
                raise ApiError(f"AList list 返回了重复分页内容，已停止: {normalized}")
            seen_signatures.add(signature)

            raw_page = data.get("page")
            if isinstance(raw_page, bool):
                raise ApiError(
                    f"AList list 返回了无效页码: {normalized}; {raw_page!r}"
                )
            try:
                returned_page = int(raw_page or page)
            except (TypeError, ValueError) as exc:
                raise ApiError(
                    f"AList list 返回了无效页码: {normalized}; {data.get('page')!r}"
                ) from exc
            if returned_page < 1:
                raise ApiError(f"AList list 返回了无效页码: {normalized}; page={returned_page}")
            if returned_page in seen_pages:
                raise ApiError(f"AList list 分页重复，已停止: {normalized}; page={returned_page}")
            seen_pages.add(returned_page)

            has_more = data.get("has_more")
            pages_total = data.get("pages_total")
            total = data.get("filtered_total")
            if isinstance(total, bool) or not isinstance(total, int):
                total = data.get("total")
            if isinstance(total, bool):
                total = None
            metadata_claims_more = False
            if isinstance(has_more, bool):
                more = has_more
                metadata_claims_more = has_more
            elif isinstance(pages_total, int) and not isinstance(pages_total, bool):
                more = returned_page < pages_total
                metadata_claims_more = more
            elif isinstance(total, int):
                more = len(output) < total
                metadata_claims_more = more
            else:
                # 旧版 AList 没有分页元数据时，满页后再请求一页确认。
                more = len(content) >= per_page

            if not more:
                return output
            if not content:
                if metadata_claims_more:
                    raise ApiError(
                        f"AList list 声称仍有下一页，但当前页为空，已停止: {normalized}; "
                        f"page={returned_page}"
                    )
                return output
            page = returned_page + 1
            if page > 100_000:
                raise ApiError(f"AList list 分页数量异常: {normalized}")

    def try_list(self, path: str, refresh: bool = False) -> list[dict[str, Any]] | None:
        try:
            return self.list(path, refresh=refresh)
        except ApiError as exc:
            message = str(exc).lower()
            missing_markers = (
                "not found",
                "no such file",
                "not exist",
                "object does not exist",
                "不存在",
                "未找到",
            )
            if any(marker in message for marker in missing_markers):
                return None
            raise

    def walk(
        self,
        path: str,
        *,
        refresh: bool = True,
        max_directories: int = 10_000,
        max_files: int = 200_000,
        ignore_orphan_temp: bool = False,
        include_bonus: bool = False,
        include_title_extras: bool = False,
        excluded_roots: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        if max_directories <= 0 or max_files <= 0:
            raise ValueError("max_directories 与 max_files 必须大于 0")
        root = normalize_remote_path(path)
        normalized_excludes = tuple(
            normalize_remote_path(value).rstrip("/") or "/"
            for value in (excluded_roots or [])
        )

        def excluded(candidate: str) -> bool:
            return any(
                excluded_root == "/"
                or candidate == excluded_root
                or candidate.startswith(excluded_root + "/")
                for excluded_root in normalized_excludes
            )

        if excluded(root):
            return []
        output: list[dict[str, Any]] = []
        stack = [root]
        visited: set[str] = set()

        while stack:
            current = stack.pop()
            if current in visited:
                continue
            if len(visited) >= max_directories:
                raise PlanError(
                    f"递归目录数超过安全上限 {max_directories}，已停止扫描: {root}"
                )
            visited.add(current)
            for item in self.list(current, refresh=refresh):
                name = item.get("name")
                if not isinstance(name, str) or not name:
                    continue
                try:
                    _validate_remote_source_basename(name)
                except ValueError as exc:
                    raise PlanError(
                        f"AList 返回了无法安全表示的条目名称: {current}/{name!r}"
                    ) from exc
                full_path = join_remote(current, name)
                item["full_path"] = full_path
                if excluded(full_path):
                    continue
                if is_scraper_lock(name):
                    raise PlanError(
                        f"发现仍在执行或上次异常退出遗留的整理锁，已停止: {full_path}。"
                        "自动恢复会在下一次任务尝试时重新核对远端状态。"
                    )
                if is_scraper_temp(name):
                    if not ignore_orphan_temp:
                        raise PlanError(
                            f"发现上次中断遗留的临时条目，已停止: {full_path}。"
                            "自动流程会在重试前重新核对；可在任务状态中重试。"
                        )
                    continue
                if item.get("is_dir"):
                    if should_ignore_extra(name) and not (
                        include_bonus and BONUS_CONTAINER_RE.fullmatch(name.strip(" []()"))
                    ) and not (
                        include_title_extras and TITLE_EXTRA_TAG_RE.search(name)
                    ):
                        continue
                    stack.append(full_path)
                else:
                    if ADVERTISEMENT_NAME_RE.search(name):
                        # A download-site banner is not playable media and
                        # must never become a plan problem file.
                        continue
                    if should_ignore_extra(name) and cleanup_reason(name) is None and not (
                        include_bonus and bonus_type(name) is not None
                    ) and not (
                        include_title_extras and TITLE_EXTRA_TAG_RE.search(name)
                    ):
                        continue
                    output.append(item)
                    if len(output) > max_files:
                        raise PlanError(
                            f"递归文件数超过安全上限 {max_files}，已停止扫描: {root}"
                        )
        return output

    def rename(self, full_path: str, new_name: str) -> None:
        self.call(
            "rename",
            {"path": normalize_remote_path(full_path), "name": _validate_remote_basename(new_name)},
        )

    def move(self, src_dir: str, dst_dir: str, names: Sequence[str]) -> None:
        if not names:
            return
        # ``names`` selects members that already exist in ``src_dir``; it is
        # not a destination rename.  Use the source-member policy so a
        # provider-listed name such as one containing full-width punctuation
        # can be moved unchanged.  The subsequent destination rename remains
        # guarded by ``_validate_remote_basename``.
        clean_names = [_validate_remote_source_basename(name) for name in names]
        self.call(
            "move",
            {
                "src_dir": normalize_remote_path(src_dir),
                "dst_dir": normalize_remote_path(dst_dir),
                "names": clean_names,
            },
        )

    def mkdir(self, path: str) -> None:
        normalized = normalize_remote_path(path)
        try:
            self.call("mkdir", {"path": normalized})
        except ApiError as original_exc:
            # 部分 AList 存储对已存在目录返回错误。只有目录确实可列出时才视为成功。
            if self.try_list(normalized, refresh=True) is not None:
                return
            # Quark may return the misleading logical error ``illegal text``
            # after a burst of otherwise valid directory creations.  Retry
            # only that proven transient response, reconciling visibility on
            # every attempt; genuinely invalid names still fail closed after
            # the bounded cooldown.
            if "illegal text" not in str(original_exc).casefold():
                raise
            for attempt in range(6):
                time.sleep(min(1.5 * (2**attempt), 12.0))
                try:
                    self.call("mkdir", {"path": normalized})
                    return
                except ApiError as retry_exc:
                    if self.try_list(normalized, refresh=True) is not None:
                        return
                    original_exc = retry_exc
                    if "illegal text" not in str(retry_exc).casefold():
                        raise
            raise original_exc

    def remove(self, parent: str, names: Sequence[str]) -> None:
        if not names:
            return
        clean_names = [_validate_remote_basename(name) for name in names]
        self.call("remove", {"dir": normalize_remote_path(parent), "names": clean_names})

    def remove_empty_dir(self, path: str) -> bool:
        normalized = normalize_remote_path(path)
        if normalized == "/":
            raise PlanError("拒绝删除 AList 根目录")
        try:
            self.call("remove_empty_directory", {"src_dir": normalized})
            return True
        except ApiError as exc:
            message = str(exc).lower()
            nonempty_markers = (
                "not empty",
                "directory is not empty",
                "目录非空",
                "文件夹非空",
                "不是空目录",
            )
            if any(marker in message for marker in nonempty_markers):
                return False
            raise

    def server_version(self) -> str:
        """返回 AList 公开版本号，不需要管理员 API。"""
        response = self.http.request_json(
            f"{self.base_url}/api/public/settings",
            url_validator=self._validate_api_url,
        )
        self._require_success(response, "AList 版本查询")
        data = response.get("data") or {}
        version = data.get("version") if isinstance(data, Mapping) else None
        if not isinstance(version, str) or not version.strip():
            raise ApiError("AList 未返回可识别版本号")
        return version.strip()

    def archive_meta(
        self, path: str, *, archive_password: str = "", refresh: bool = True
    ) -> dict[str, Any]:
        """只读解析归档目录；密码只发送给 AList，不写入计划。"""
        response: dict[str, Any] | None = None
        for attempt in range(4):
            try:
                response = self.call(
                    "archive/meta",
                    {
                        "path": normalize_remote_path(path),
                        "refresh": refresh,
                        "archive_pass": archive_password,
                    },
                    retryable=True,
                )
                break
            except ApiError as exc:
                if "ExceedMaxConcurrency" not in str(exc) or attempt == 3:
                    raise
                time.sleep(0.5 * (attempt + 1))
        if response is None:
            raise ApiError("AList archive/meta 未返回结果")
        data = response.get("data") or {}
        if not isinstance(data, Mapping):
            raise ApiError("AList archive/meta 返回格式异常")
        return dict(data)

    def archive_member_bytes(
        self,
        archive_path: str,
        inner_path: str,
        *,
        archive_password: str = "",
        archive_metadata: Mapping[str, Any] | None = None,
        max_bytes: int = 64 * 1024 * 1024,
    ) -> bytes:
        """Read one archive member without asking AList to upload it back.

        AList's decompression uploader derives the provider ``format_type`` from
        Go's extension MIME table. Subtitle extensions such as ``.ass`` may
        therefore reach Quark with an empty type and be rejected. Reading the
        member and uploading it through ``fs/put`` lets ScrapeFlow provide an
        explicit, correct content type. The member is data only and is never
        executed.
        """
        clean_inner = "/".join(
            _validate_remote_basename(part)
            for part in inner_path.replace("\\", "/").split("/")
            if part
        )
        if not clean_inner:
            raise ValueError("归档成员路径不能为空")
        meta = (
            dict(archive_metadata)
            if archive_metadata is not None
            else self.archive_meta(
                archive_path,
                archive_password=archive_password,
                refresh=True,
            )
        )
        raw_url = meta.get("raw_url")
        if not isinstance(raw_url, str) or not raw_url:
            raise ApiError("AList archive/meta 未返回成员读取地址")
        parsed = urllib.parse.urlsplit(raw_url)
        if parsed.path != "/ae" and not parsed.path.startswith(("/ae/", "/ad/", "/ap/")):
            raise ApiError("AList archive/meta 返回了无效的成员读取地址")
        base = urllib.parse.urlsplit(self.base_url)
        query = {
            "inner": clean_inner,
            "pass": archive_password,
        }
        sign = meta.get("sign")
        if isinstance(sign, str) and sign:
            query["sign"] = sign
        url = urllib.parse.urlunsplit(
            (
                base.scheme,
                base.netloc,
                parsed.path,
                urllib.parse.urlencode(query),
                "",
            )
        )
        return self._request_bytes_authenticated(
            url,
            max_bytes=max_bytes,
        )

    def file_link(self, path: str, *, refresh: bool = True) -> tuple[str, dict[str, str]]:
        """Return a fresh read-only download link and provider-required headers."""
        response = self.call(
            "get",
            {
                "path": normalize_remote_path(path),
                "password": "",
                "page": 1,
                "per_page": 0,
                "refresh": refresh,
            },
            retryable=False,
        )
        data = response.get("data") or {}
        if not isinstance(data, Mapping):
            raise ApiError("AList fs/get 返回格式异常")
        raw_url = data.get("raw_url")
        if not isinstance(raw_url, str) or not raw_url:
            raise ApiError("AList fs/get 未返回文件下载地址")
        self._validate_download_url(raw_url)
        raw_headers = data.get("header") or {}
        headers = (
            {str(key): str(value) for key, value in raw_headers.items()}
            if isinstance(raw_headers, Mapping)
            else {}
        )
        return raw_url, headers

    def exact_file_info(self, path: str) -> dict[str, object] | None:
        """Return exact-path file identity without trusting a parent listing."""
        normalized = normalize_remote_path(path)
        try:
            response = self.call(
                "get",
                {
                    "path": normalized,
                    "password": "",
                    "page": 1,
                    "per_page": 0,
                    "refresh": True,
                },
                retryable=False,
            )
        except ApiError as exc:
            missing_markers = (
                "not found", "no such file", "not exist",
                "object does not exist", "不存在", "未找到",
            )
            if any(marker in str(exc).casefold() for marker in missing_markers):
                return None
            raise
        data = response.get("data") or {}
        if not isinstance(data, Mapping):
            raise ApiError(f"AList fs/get 返回格式异常: {normalized}")
        if data.get("is_dir"):
            raise ApiError(f"AList exact stat 期望文件但得到目录: {normalized}")
        size = _entry_size_value(data)
        if size is None:
            raise ApiError(f"AList fs/get 未返回有效文件大小: {normalized}")
        expected_name = split_remote(normalized)[1]
        returned_name = data.get("name")
        if isinstance(returned_name, str) and returned_name != expected_name:
            raise ApiError(
                f"AList fs/get 返回了非精确路径文件: "
                f"expected={expected_name!r}, actual={returned_name!r}"
            )
        version = _entry_modified_value(data)
        return {"size": size, "version": version}

    @contextlib.contextmanager
    def open_file_reader(self, path: str):
        """Open one non-retrying exact remote read for staging or verification."""
        raw_url, headers = self.file_link(path, refresh=True)
        request = urllib.request.Request(raw_url, headers=headers)
        opener = urllib.request.build_opener(
            # The AList origin is loopback; an ambient host proxy would dial
            # its own loopback instead of this machine and hang the read.
            urllib.request.ProxyHandler({}),
            ValidatingRedirectHandler(self._validate_download_url),
        )
        response = opener.open(request, timeout=60)
        try:
            yield response
        finally:
            response.close()

    def _validate_download_url(self, url: str) -> None:
        """Allow public provider URLs and the configured AList origin only."""
        try:
            parsed = urllib.parse.urlsplit(url)
            port = parsed.port
        except ValueError as exc:
            raise ApiError("AList 返回了无效的文件下载地址") from exc
        if parsed.scheme not in {"http", "https"} or not parsed.hostname:
            raise ApiError("AList 返回了无效的文件下载地址")
        if parsed.username is not None or parsed.password is not None:
            raise ApiError("AList 返回的文件地址不得包含账号信息")
        base = urllib.parse.urlsplit(self.base_url)
        effective_port = port or (443 if parsed.scheme == "https" else 80)
        base_port = base.port or (443 if base.scheme == "https" else 80)
        if (
            parsed.scheme == base.scheme
            and parsed.hostname.casefold() == (base.hostname or "").casefold()
            and effective_port == base_port
        ):
            return
        try:
            addresses = {
                row[4][0]
                for row in socket.getaddrinfo(
                    parsed.hostname,
                    effective_port,
                    type=socket.SOCK_STREAM,
                )
            }
        except socket.gaierror as exc:
            raise ApiError("AList 文件下载域名无法解析") from exc
        if not addresses:
            raise ApiError("AList 文件下载域名未返回可用地址")
        for address in addresses:
            ip = ipaddress.ip_address(address)
            if (
                not ip.is_global
                or ip.is_loopback
                or ip.is_link_local
                or ip.is_multicast
                or ip.is_unspecified
            ):
                raise ApiError("AList 文件下载地址指向了不允许的内网或保留网段")
        if effective_port not in {80, 443}:
            raise ApiError("AList 外部文件下载地址使用了不允许的端口")

    def read_file_prefix(self, path: str, *, max_bytes: int = 1024 * 1024) -> bytes:
        """Read only the beginning of a remote file; used for signature checks.

        ``refresh=False`` keeps the provider's cached read link (the AList
        proxy) instead of forcing a fresh Quark download grant per file.  The
        archive pre-scan reads a prefix of every member of a source tree, so a
        per-file ``refresh=True`` (Quark download grant) turns a 30-file
        directory into dozens of slow, timeout-prone download grants.
        """
        raw_url, headers = self.file_link(path, refresh=False)
        headers["Range"] = f"bytes=0-{max_bytes - 1}"
        return self.http.request_bytes(
            raw_url,
            headers=headers,
            max_bytes=max_bytes,
            url_validator=self._validate_download_url,
        )

    def read_file_bytes(self, path: str, *, max_bytes: int, refresh: bool = False) -> bytes:
        """Download one bounded remote file without relying on its MIME type.

        ``refresh=False`` uses the provider's cached read link (the AList
        proxy) instead of forcing a fresh Quark download grant.  NFO/metadata
        reads are static and must not pay the Quark download-latency per file;
        a whole-library NFO sweep otherwise takes minutes and can trip the
        provider's per-request timeout.
        """
        if max_bytes <= 0:
            raise ValueError("max_bytes 必须大于 0")
        raw_url, headers = self.file_link(path, refresh=refresh)
        return self.http.request_bytes(
            raw_url,
            headers=headers,
            max_bytes=max_bytes,
            url_validator=self._validate_download_url,
        )

    def download_file_to_path(
        self,
        path: str,
        destination: Path,
        *,
        expected_size: int,
    ) -> None:
        """Stream one remote file to disk with an exact snapshot-size bound."""
        if expected_size <= 0:
            raise ValueError("expected_size 必须大于 0")
        written = destination.stat().st_size if destination.exists() else 0
        if written > expected_size:
            destination.unlink()
            written = 0
        last_error: Exception | None = None
        for attempt in range(6):
            try:
                raw_url, headers = self.file_link(path, refresh=True)
                if written:
                    headers["Range"] = f"bytes={written}-"
                request = urllib.request.Request(raw_url, headers=headers)
                opener = urllib.request.build_opener(
                    # Same loopback rationale as open_file_reader: never let
                    # an ambient host proxy dial its own localhost for AList.
                    urllib.request.ProxyHandler({}),
                    ValidatingRedirectHandler(self._validate_download_url),
                )
                with opener.open(request, timeout=60) as response:
                    append = written > 0 and getattr(response, "status", 200) == 206
                    if written and not append:
                        written = 0
                    with destination.open("ab" if append else "wb") as output:
                        while True:
                            chunk = response.read(1024 * 1024)
                            if not chunk:
                                break
                            written += len(chunk)
                            if written > expected_size:
                                raise ApiError(f"远端文件超过计划大小: {path}")
                            output.write(chunk)
                if written == expected_size:
                    return
                last_error = ApiError(
                    f"流式下载大小不匹配: {path}; "
                    f"expected={expected_size}, actual={written}"
                )
            except urllib.error.HTTPError as exc:
                last_error = exc
                if exc.code != 429 and not 500 <= exc.code < 600:
                    raise ApiError(f"流式下载失败，HTTP {exc.code}: {path}") from exc
            except (urllib.error.URLError, TimeoutError, OSError, http.client.HTTPException) as exc:
                last_error = exc
            if attempt < 5:
                time.sleep(min(2 ** attempt, 16))
        raise ApiError(f"流式下载失败: {path}; {last_error}") from last_error

    def upload_file(
        self,
        target_path: str,
        source: Path,
        content_type: str = "application/octet-stream",
    ) -> None:
        """Stream a local file into AList without loading media into memory."""
        size = source.stat().st_size
        parsed = urllib.parse.urlsplit(self.base_url)
        connection_type = (
            http.client.HTTPSConnection
            if parsed.scheme == "https"
            else http.client.HTTPConnection
        )
        # Cloud providers can take several minutes to commit a multi-gigabyte
        # upload after the request body has been sent.  A fixed 60-second
        # response timeout makes a successful transfer look failed and tempts
        # callers to upload the same media twice.
        response_timeout = max(
            120,
            # AList's Quark driver first caches the complete request and then
            # performs the provider upload before returning.  A 4–5 GiB file
            # can therefore legitimately need well over ten minutes even on a
            # fast local connection to AList.
            min(3600, 300 + int(size / (4 * 1024 * 1024))),
        )
        connection = connection_type(
            parsed.hostname,
            parsed.port,
            timeout=response_timeout,
        )
        endpoint = (parsed.path.rstrip("/") if parsed.path else "") + "/api/fs/put"
        try:
            connection.putrequest("PUT", endpoint)
            for key, value in {
                **self._headers(),
                "File-Path": urllib.parse.quote(normalize_remote_path(target_path)),
                # AList v3.62.0 supports an explicit create-only upload.  The
                # default is overwrite=true, which can overwrite a concurrent
                # object or let a backing provider manufacture ``(1)`` names.
                # Safe file transactions always require the exact requested
                # path to remain unoccupied until this single PUT commits.
                "Overwrite": "false",
                "Content-Type": content_type,
                "Content-Length": str(size),
            }.items():
                connection.putheader(key, value)
            connection.endheaders()
            with source.open("rb") as stream:
                while True:
                    chunk = stream.read(1024 * 1024)
                    if not chunk:
                        break
                    connection.send(chunk)
            response = connection.getresponse()
            body = response.read(1024 * 1024 + 1)
            if len(body) > 1024 * 1024:
                raise ApiError("AList 上传响应过大")
            try:
                payload = json.loads(body.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise ApiError(f"AList 上传响应无效: HTTP {response.status}") from exc
            if not isinstance(payload, Mapping):
                raise ApiError("AList 上传响应格式异常")
            self._require_success(payload, "AList 流式上传")
        except (OSError, http.client.HTTPException) as exc:
            target_dir, target_name = split_remote(target_path)
            for delay in (0, 1, 2, 4, 8):
                if delay:
                    time.sleep(delay)
                try:
                    matches = [
                        item
                        for item in (self.try_list(target_dir, refresh=True) or [])
                        if not item.get("is_dir")
                        and _collision_key(str(item.get("name") or ""))
                        == _collision_key(target_name)
                    ]
                except ApiError:
                    continue
                if len(matches) == 1 and _entry_size_value(matches[0]) == size:
                    return
                if matches:
                    break
            raise ApiError(f"AList 流式上传失败: {target_path}; {exc}") from exc
        finally:
            connection.close()

    def archive_decompress(
        self,
        *,
        src_dir: str,
        dst_dir: str,
        name: str,
        archive_password: str = "",
        cache_full: bool = True,
        put_into_new_dir: bool = False,
    ) -> list[dict[str, Any]]:
        response = self.call(
            "archive/decompress",
            {
                "src_dir": normalize_remote_path(src_dir),
                "dst_dir": normalize_remote_path(dst_dir),
                "name": [_validate_remote_basename(name)],
                "archive_pass": archive_password,
                "inner_path": "",
                "cache_full": bool(cache_full),
                "put_into_new_dir": bool(put_into_new_dir),
            },
            retryable=False,
        )
        data = response.get("data") or {}
        tasks = data.get("task") if isinstance(data, Mapping) else None
        if tasks is None:
            return []
        if not isinstance(tasks, list) or not all(isinstance(item, dict) for item in tasks):
            raise ApiError("AList archive/decompress 返回任务格式异常")
        return [dict(item) for item in tasks]

    def archive_tasks(self, kind: str, *, done: bool) -> list[dict[str, Any]]:
        if kind not in {"decompress", "decompress_upload"}:
            raise ValueError(f"无效归档任务类型: {kind}")
        state = "done" if done else "undone"
        response = self._request_json_authenticated(
            f"{self.base_url}/api/task/{kind}/{state}",
        )
        self._require_success(response, f"AList {kind} 任务查询")
        data = response.get("data")
        if data is None:
            return []
        if not isinstance(data, list) or not all(isinstance(item, dict) for item in data):
            raise ApiError(f"AList {kind} 任务返回格式异常")
        return [dict(item) for item in data]

    def upload_bytes(
        self,
        target_path: str,
        data: bytes,
        content_type: str,
        *,
        overwrite: bool = False,
    ) -> None:
        response = self._request_json_authenticated(
            f"{self.base_url}/api/fs/put",
            method="PUT",
            headers={
                "File-Path": urllib.parse.quote(normalize_remote_path(target_path)),
                "Content-Type": content_type,
                "Overwrite": "true" if overwrite else "false",
            },
            raw_body=data,
            retryable=False,
        )
        self._require_success(response, "AList 上传")


class TMDBClient:
    def __init__(
        self,
        api_key: str,
        *,
        base_url: str | None = None,
        image_base_url: str | None = None,
        language: str = "zh-CN",
        timeout: float = 20.0,
        retries: int = 5,
        cache_ttl: float = 600.0,
        cache_max_entries: int = 2048,
        proxy_url: str | None = None,
    ) -> None:
        if not api_key:
            raise ScraperError("缺少 TMDB API Key，请设置 TMDB_API_KEY 或使用 --tmdb-key-file")
        self.api_key = api_key
        self.base_url = self._validated_base_url(
            base_url or os.getenv("TMDB_BASE_URL") or DEFAULT_TMDB_BASE,
            "TMDB_BASE_URL",
        )
        self.image_base_url = self._validated_base_url(
            image_base_url or os.getenv("TMDB_IMAGE_BASE_URL") or DEFAULT_IMAGE_BASE,
            "TMDB_IMAGE_BASE_URL",
        )
        self.language = language
        configured_proxy = proxy_url or os.getenv("TMDB_PROXY_URL")
        self.proxy_url = (
            self._validated_proxy_url(configured_proxy)
            if configured_proxy
            else None
        )
        self.http = JsonHttpClient(
            timeout=timeout, retries=retries, proxy_url=self.proxy_url,
        )
        self.cache_ttl = max(0.0, float(cache_ttl))
        self.cache_max_entries = max(0, int(cache_max_entries))
        self._cache: dict[str, tuple[float, dict[str, Any]]] = {}
        self.request_count = 0
        self.cache_hits = 0

    @staticmethod
    def _validated_base_url(value: str, label: str) -> str:
        try:
            parsed = urllib.parse.urlsplit(value)
        except ValueError as exc:
            raise ScraperError(f"{label} 不是有效 URL") from exc
        if parsed.scheme != "https" or not parsed.hostname:
            raise ScraperError(f"{label} 必须使用可信的 HTTPS 地址")
        if parsed.username is not None or parsed.password is not None or parsed.query or parsed.fragment:
            raise ScraperError(f"{label} 不能包含凭据、查询参数或片段")
        return value.rstrip("/")

    @staticmethod
    def _validated_proxy_url(value: str) -> str:
        try:
            parsed = urllib.parse.urlsplit(value)
            _ = parsed.port
        except ValueError as exc:
            raise ScraperError("TMDB_PROXY_URL 不是有效代理 URL") from exc
        if parsed.scheme != "http" or not parsed.hostname:
            raise ScraperError("TMDB_PROXY_URL 必须是可信网络上的 HTTP 代理地址")
        if (
            parsed.username is not None
            or parsed.password is not None
            or parsed.query
            or parsed.fragment
            or parsed.path not in {"", "/"}
        ):
            raise ScraperError("TMDB_PROXY_URL 不得包含凭据、路径、查询参数或片段")
        return value.rstrip("/")

    @staticmethod
    def _actionable_error(exc: ApiError) -> ApiError:
        message = str(exc)
        lowered = message.casefold()
        if exc.status_code in {401, 403}:
            detail = "TMDB API Key 无效或没有访问权限，请检查 TMDB_API_KEY"
        elif exc.status_code == 429:
            detail = "TMDB 请求过于频繁，请稍后重试"
        elif exc.status_code is not None and exc.status_code >= 500:
            detail = "TMDB 服务暂时不可用，请稍后重试"
        elif any(marker in lowered for marker in (
            "name or service not known", "nodename nor servname",
            "temporary failure in name resolution", "getaddrinfo failed",
        )):
            detail = (
                "TMDB 域名解析失败，请检查本机或容器 DNS；"
                "如使用可信的 TMDB 反向代理，可配置 TMDB_BASE_URL"
            )
        elif "timed out" in lowered or "timeout" in lowered:
            detail = "连接 TMDB 超时，请检查网络、代理或 DNS 后重试"
        elif any(marker in lowered for marker in (
            "certificate verify", "cert_verify", "certificate validation",
            "self signed certificate", "hostname mismatch",
        )):
            detail = "TMDB HTTPS 证书校验失败，请检查代理或系统时间"
        elif any(marker in lowered for marker in (
            "unexpected_eof", "unexpected eof", "eof occurred in violation",
            "tls connection", "ssl connection",
        )):
            detail = "TMDB TLS 连接被中途断开，请检查代理、DNS 或网络链路"
        else:
            return exc
        return ApiError(detail, status_code=exc.status_code)

    def get(self, path: str, **params: Any) -> dict[str, Any]:
        if not path.startswith("/"):
            path = "/" + path
        query = {"api_key": self.api_key, "language": self.language}
        query.update({key: value for key, value in params.items() if value is not None})
        url = f"{self.base_url}{path}?{urllib.parse.urlencode(sorted(query.items()), doseq=True)}"
        cache_key = url
        now = time.monotonic()
        cached = self._cache.get(cache_key)
        if cached is not None:
            expires_at, value = cached
            if now < expires_at:
                self.cache_hits += 1
                _trace_io(f"tmdb.get cache path={path!r} language={query.get('language')!r}")
                return copy.deepcopy(value)
            self._cache.pop(cache_key, None)
        try:
            self.request_count += 1
            _trace_io(f"tmdb.get start path={path!r} language={query.get('language')!r}")
            response = self.http.request_json(
                url, headers={"User-Agent": f"alist-tmdb-scraper/{__version__}"}
            )
            _trace_io(f"tmdb.get done path={path!r} language={query.get('language')!r}")
        except ApiError as exc:
            raise self._actionable_error(exc) from exc
        if response.get("success") is False:
            status_code = response.get("status_code")
            raise ApiError(
                "TMDB 请求失败: "
                + _redact_sensitive_text(
                    str(response.get("status_message") or response), (self.api_key,)
                ),
                status_code=int(status_code) if isinstance(status_code, int) else None,
            )
        if self.cache_ttl > 0 and self.cache_max_entries > 0:
            if len(self._cache) >= self.cache_max_entries:
                oldest_key = min(self._cache, key=lambda key: self._cache[key][0])
                self._cache.pop(oldest_key, None)
            self._cache[cache_key] = (now + self.cache_ttl, copy.deepcopy(response))
        return copy.deepcopy(response)

    def cache_report(self) -> dict[str, int | float]:
        return {
            "request_count": self.request_count,
            "cache_hits": self.cache_hits,
            "cache_entries": len(self._cache),
            "ttl_seconds": self.cache_ttl,
        }

    def download_poster(self, poster_path: str) -> bytes:
        try:
            return self.http.request_bytes(f"{self.image_base_url}{poster_path}")
        except ApiError as exc:
            raise self._actionable_error(exc) from exc


def resolve_poster_target(
    alist: AListClient,
    target_dir: str,
    *,
    overwrite: bool,
) -> tuple[str, bool]:
    """返回实际海报路径及是否覆盖已有文件，并阻止大小写/Unicode 歧义。"""
    normalized_dir = normalize_remote_path(target_dir)
    content = alist.try_list(normalized_dir, refresh=True)
    if content is None:
        raise PlanError(f"目标目录不存在或不可读: {normalized_dir}")
    matches = [
        entry
        for entry in content
        if isinstance(entry.get("name"), str)
        and _collision_key(str(entry["name"])) == _collision_key("folder.jpg")
    ]
    if any(entry.get("is_dir") for entry in matches):
        raise PlanError(f"目标目录存在名为 folder.jpg 的目录: {normalized_dir}")
    file_matches = [entry for entry in matches if not entry.get("is_dir")]
    if len(file_matches) > 1:
        raise PlanError(
            f"目标目录存在多个大小写或 Unicode 等价的 folder.jpg，拒绝选择: {normalized_dir}"
        )
    actual_name = str(file_matches[0]["name"]) if file_matches else "folder.jpg"
    return join_remote(normalized_dir, actual_name), bool(file_matches)


def resolve_artwork_target(
    alist: AListClient, target_path: str
) -> tuple[str, bool]:
    normalized = normalize_remote_path(target_path)
    target_dir, requested_name = split_remote(normalized)
    content = alist.try_list(target_dir, refresh=True)
    if content is None:
        raise PlanError(f"图稿目标目录不存在或不可读: {target_dir}")
    matches = [
        entry
        for entry in content
        if isinstance(entry.get("name"), str)
        and _collision_key(str(entry["name"])) == _collision_key(requested_name)
    ]
    if any(entry.get("is_dir") for entry in matches):
        raise PlanError(f"图稿目标被同名目录占用: {normalized}")
    file_matches = [entry for entry in matches if not entry.get("is_dir")]
    if len(file_matches) > 1:
        raise PlanError(f"存在多个大小写或 Unicode 等价图稿，拒绝选择: {normalized}")
    actual_name = str(file_matches[0]["name"]) if file_matches else requested_name
    return join_remote(target_dir, actual_name), bool(file_matches)


def planned_artwork(plan: Plan) -> list[tuple[str, str, str]]:
    """Return deterministic artwork requests from an in-memory plan."""
    return _plan_artifacts._planned_artwork_impl(  # noqa: SLF001
        plan,
        join_remote_fn=join_remote,
        collision_key_fn=_collision_key,
        normalize_remote_path_fn=normalize_remote_path,
        is_planned_bonus_fn=is_planned_bonus,
    )


def planned_movie_nfos(plan: Plan) -> list[tuple[str, bytes]]:
    return _plan_artifacts._planned_movie_nfos_impl(  # noqa: SLF001
        plan,
        join_remote_fn=join_remote,
        normalize_remote_path_fn=normalize_remote_path,
        is_planned_bonus_fn=is_planned_bonus,
    )


def planned_tv_nfos(plan: Plan) -> list[tuple[str, bytes]]:
    return _plan_artifacts._planned_tv_nfos_impl(  # noqa: SLF001
        plan,
        join_remote_fn=join_remote,
    )


def planned_tv_episode_nfos(plan: Plan) -> list[tuple[str, bytes]]:
    return _plan_artifacts._planned_tv_episode_nfos_impl(  # noqa: SLF001
        plan,
        collision_key_fn=_collision_key,
        join_remote_fn=join_remote,
        normalize_remote_path_fn=normalize_remote_path,
        path_is_within_fn=_path_is_within,
        split_remote_fn=split_remote,
        plan_error=lambda message: PlanError(message),
        is_planned_bonus_fn=is_planned_bonus,
    )


def planned_nfos(plan: Plan) -> list[tuple[str, bytes]]:
    return _plan_artifacts._planned_nfos_impl(  # noqa: SLF001
        plan,
        planned_tv_nfos_fn=planned_tv_nfos,
        planned_tv_episode_nfos_fn=planned_tv_episode_nfos,
        planned_movie_nfos_fn=planned_movie_nfos,
    )



# ---------------------------------------------------------------------------
# 路径、命名与集数识别
# ---------------------------------------------------------------------------


def _preserve_equivalent_source_root(src_path: str, desired_root: str) -> str:
    """Keep the existing directory spelling when only case/Unicode form differs."""
    source_root = normalize_remote_path(src_path).rstrip("/") or "/"
    target_root = normalize_remote_path(desired_root).rstrip("/") or "/"
    if source_root != target_root and _collision_key(source_root) == _collision_key(target_root):
        return source_root
    return target_root


def _nfo_tmdb_ids(
    alist: AListClient,
    directory: str,
    *,
    tv: bool,
    refresh: bool = True,
) -> set[int]:
    entries = alist.try_list(directory, refresh=refresh)
    if entries is None:
        return set()
    candidates = [
        str(item["name"])
        for item in entries
        if not item.get("is_dir")
        and isinstance(item.get("name"), str)
        and str(item["name"]).casefold().endswith(".nfo")
        and (not tv or str(item["name"]).casefold() == "tvshow.nfo")
    ][:20]
    identities: set[int] = set()
    for name in candidates:
        try:
            payload = alist.read_file_bytes(join_remote(directory, name), max_bytes=2 * 1024 * 1024)
            lowered = payload.lower()
            if b"<!doctype" in lowered or b"<!entity" in lowered:
                continue
            root = ET.fromstring(payload)
        except (ApiError, ET.ParseError, ValueError):
            continue
        for element in root.iter():
            tag = str(element.tag).rsplit("}", 1)[-1].casefold()
            tmdb_unique = tag == "uniqueid" and str(element.attrib.get("type", "")).casefold() == "tmdb"
            if tag != "tmdbid" and not tmdb_unique:
                continue
            value = (element.text or "").strip()
            if value.isdigit() and int(value) > 0:
                identities.add(int(value))
    return identities


def _library_root_contains_any_file(
    alist: AListClient,
    root: str,
    *,
    max_directories: int = 128,
) -> bool:
    """Conservatively distinguish an empty preflight tree from real media.

    A failed transaction can leave only the directories it created before the
    first file move.  Such an empty same-name tree has no competing identity
    and is safe to reuse on the automatic retry.  Unreadable or unexpectedly
    large trees return ``True`` so real library content still requires NFO
    evidence.
    """
    stack = [normalize_remote_path(root)]
    visited: set[str] = set()
    while stack:
        current = stack.pop()
        if current in visited:
            continue
        if len(visited) >= max_directories:
            return True
        visited.add(current)
        rows = alist.try_list(current, refresh=True)
        if rows is None:
            return True
        for row in rows:
            name = row.get("name")
            if not isinstance(name, str) or not name:
                return True
            if row.get("is_dir"):
                stack.append(join_remote(current, name))
            else:
                return True
    return False


def resolve_existing_library_root(
    alist: AListClient,
    *,
    parent_path: str,
    desired_root: str,
    tmdb_id: int,
    tv: bool,
) -> tuple[str, str]:
    """Reuse a same-id library folder and block identity-unsafe merges."""
    parent = normalize_remote_path(parent_path)
    desired = normalize_remote_path(desired_root)
    # Refreshing a large provider root and then force-refreshing every child
    # can block a small follow-up job for many minutes.  A cached parent
    # listing is enough to discover names; the matching destination itself is
    # still refreshed below before its NFO identity is trusted.
    entries = alist.try_list(parent, refresh=False)
    if entries is None:
        return desired, "new"
    directory_roots = [
        join_remote(parent, str(entry["name"]))
        for entry in entries
        if entry.get("is_dir") and isinstance(entry.get("name"), str)
    ]
    equivalent_roots = [
        candidate
        for candidate in directory_roots
        if _collision_key(candidate) == _collision_key(desired)
    ]
    if len(equivalent_roots) > 1:
        raise PlanError(
            "媒体库中有多个名称等价的目标目录，拒绝自动选择: "
            + ", ".join(equivalent_roots)
        )
    if equivalent_roots:
        equivalent_root = equivalent_roots[0]
        identities = _nfo_tmdb_ids(alist, equivalent_root, tv=tv, refresh=True)
        if identities and identities != {tmdb_id}:
            raise PlanError(
                f"目标目录 NFO 的 TMDB 身份与本次计划不同，拒绝合并: "
                f"{equivalent_root}; existing={sorted(identities)}, planned={tmdb_id}"
            )
        if identities == {tmdb_id}:
            return equivalent_root, "same_tmdb_id"
        if not _library_root_contains_any_file(alist, equivalent_root):
            return equivalent_root, "empty_without_nfo"
        return equivalent_root, "matching_name_without_nfo"

    # No same-name destination exists. Search renamed library roots using
    # their explicit NFO TMDB identity. A stale miss creates a new desired
    # directory and cannot overwrite an unrelated one.
    same_id_roots: list[str] = []
    for candidate in directory_roots:
        identities = _nfo_tmdb_ids(alist, candidate, tv=tv, refresh=False)
        if tmdb_id in identities and identities != {tmdb_id}:
            raise PlanError(
                f"NFO 同时声明多个 TMDB 身份，拒绝自动合并: "
                f"{candidate}; identities={sorted(identities)}"
            )
        if identities == {tmdb_id}:
            same_id_roots.append(candidate)
    unique_same_id = list(dict.fromkeys(same_id_roots))
    if len(unique_same_id) > 1:
        raise PlanError(
            f"媒体库中有多个目录声明同一 TMDB ID {tmdb_id}，拒绝自动选择: "
            + ", ".join(unique_same_id)
        )
    if unique_same_id:
        return unique_same_id[0], "same_tmdb_id"
    return desired, "new"


def provider_safe_episode_title(name: str) -> str:
    """Compatibility wrapper for the provider-safe episode title policy."""
    return _media_naming._provider_safe_episode_title_impl(  # noqa: SLF001
        name, safe_name_fn=safe_name,
    )


def _limit_filename(name: str, max_bytes: int = 240) -> str:
    return _media_naming._limit_filename_impl(  # noqa: SLF001
        name,
        max_bytes,
        truncate_utf8=_truncate_utf8,
        plan_error=PlanError,
    )


def _compose_filename(base: str, semantic_suffix: str, extension: str, max_bytes: int = 240) -> str:
    return _media_naming._compose_filename_impl(  # noqa: SLF001
        base,
        semantic_suffix,
        extension,
        max_bytes,
        truncate_utf8=_truncate_utf8,
        plan_error=PlanError,
    )


def is_scraper_temp(name: str) -> bool:
    return _media_naming._is_scraper_temp_impl(  # noqa: SLF001
        name, collision_key=_collision_key,
    )


def is_scraper_lock(name: str) -> bool:
    return _media_naming._is_scraper_lock_impl(  # noqa: SLF001
        name,
        collision_key=_collision_key,
        lock_prefix=LOCK_PREFIX,
    )


def should_ignore_extra(name: str) -> bool:
    return _media_naming._should_ignore_extra_impl(  # noqa: SLF001
        name, ignored_extra_tag_re=IGNORED_EXTRA_TAG_RE,
    )


def is_sample(name: str) -> bool:
    return _media_naming._is_sample_impl(name, sample_re=SAMPLE_RE)  # noqa: SLF001


def bonus_type(name: str) -> str | None:
    return _media_naming._bonus_type_impl(  # noqa: SLF001
        name, bonus_patterns=BONUS_PATTERNS,
    )


def is_planned_bonus(name: str) -> bool:
    return _media_naming._is_planned_bonus_impl(  # noqa: SLF001
        name, planned_bonus_suffix_re=PLANNED_BONUS_SUFFIX_RE,
    )


def edition_tag(name: str) -> str | None:
    return _media_naming._edition_tag_impl(  # noqa: SLF001
        name,
        safe_name_fn=safe_name,
        edition_patterns=EDITION_PATTERNS,
    )


def entry_edition_tag(item: Mapping[str, Any]) -> str | None:
    return _media_naming._entry_edition_tag_impl(  # noqa: SLF001
        item,
        edition_tag_fn=edition_tag,
        safe_name_fn=safe_name,
    )


def _token_present(text: str, marker: str) -> bool:
    return _media_naming._token_present_impl(text, marker)  # noqa: SLF001


def subtitle_language(name: str) -> str | None:
    return _media_naming._subtitle_language_impl(  # noqa: SLF001
        name,
        token_present_fn=_token_present,
        simplified_markers=SIMPLIFIED_MARKERS,
        traditional_markers=TRADITIONAL_MARKERS,
        english_markers=ENGLISH_MARKERS,
        japanese_markers=JAPANESE_MARKERS,
    )


def is_traditional_sub(name: str) -> bool:
    return _media_naming._is_traditional_sub_impl(  # noqa: SLF001
        name, subtitle_language_fn=subtitle_language,
    )


def is_simplified_sub(name: str) -> bool:
    return _media_naming._is_simplified_sub_impl(  # noqa: SLF001
        name, subtitle_language_fn=subtitle_language,
    )


ROMAN_MAP = {
    "Ⅰ": "1",
    "Ⅱ": "2",
    "Ⅲ": "3",
    "Ⅳ": "4",
    "Ⅴ": "5",
    "Ⅵ": "6",
    "Ⅶ": "7",
    "Ⅷ": "8",
    "Ⅸ": "9",
    "Ⅹ": "10",
}


def extract_episode_key(text: str) -> EpisodeKey | None:
    clean = DATE_NOISE_RE.sub(" ", text)
    clean = EPISODE_NOISE_RE.sub(" ", clean)
    clean = BARE_YEAR_NOISE_RE.sub(" ", clean)
    for roman, arabic in ROMAN_MAP.items():
        clean = clean.replace(roman, f" {arabic} ")

    # A canonical leading/embedded SxxExx token is already an exact routing
    # identity.  Do not let a later descriptive release token such as
    # ``OVA 01`` replace ``S00E07`` and collide with an unrelated special.
    explicit_season_episode = re.search(
        r"(?:^|[^A-Za-z0-9])S\s*0*(\d{1,3})\s*E\s*0*(\d{1,4})"
        r"(?:$|[^0-9])",
        clean,
        re.IGNORECASE,
    )
    if explicit_season_episode:
        season = int(explicit_season_episode.group(1))
        episode = int(explicit_season_episode.group(2))
        return EpisodeKey("special" if season == 0 else "regular", episode)

    for pattern in (SEASON_LOCAL_ABSOLUTE_RE, DUAL_BRACKET_EPISODE_RE):
        dual_match = pattern.search(clean)
        if dual_match and int(dual_match.group(2)) > int(dual_match.group(1)):
            return EpisodeKey("regular", int(dual_match.group(1)))

    # Release groups use decimal episode labels for interludes, recaps and
    # sometimes ordinary web extras. Preserve the complete decimal token.
    # Whether it is a TMDB special is decided later from official metadata;
    # the parser must not turn every decimal into Season 00.
    fractional_match = re.search(
        r"(?:^|[\[\s_\-(])0*(\d{1,3})\.(\d{1,3})(?:v\d+)?"
        r"(?:[\]\s_\-.)]|$)",
        clean,
        re.IGNORECASE,
    )
    if fractional_match:
        digits = fractional_match.group(2).rstrip("0") or "0"
        return EpisodeKey(
            "fractional",
            int(fractional_match.group(1)),
            fractional_digits=digits,
        )

    special_patterns = [
        r"\[\s*(?:SP|SPECIAL|OVA|OAV|OAD)\s*\]\s*\[\s*0*(\d{1,3})\s*\]",
        r"(?:^|[\s._\-\[\]()])(?:OVA|OAV|OAD)[\s._-]*(?:SERIES|系列)[\s._-]*\[?\s*0*(\d{1,3})\s*\]?(?:$|[\s._\-\[\]()])",
        r"(?:^|[\s._\-\[\]()])(?:SP|SPECIAL|OVA|OAV|OAD)[\s._-]*[\[(]\s*0*(\d{1,3})\s*[\])](?:$|[\s._\-\[\]()])",
        r"(?:^|[\s._\-\[\]()])(?:SP|SPECIAL|OVA|OAV|OAD)[\s._-]*0*(\d{1,3})(?:$|[\s._\-\[\]()])",
        r"(?:^|[\s._\-\[\]()])TOKUTEN[ ._-]*ANIME[ ._-]*0*(\d{1,3})(?:$|[\s._\-\[\]()])",
        r"第\s*0*(\d{1,3})\s*(?:话|集)?\s*(?:特别篇|特典)",
    ]
    for pattern in special_patterns:
        match = re.search(pattern, clean, re.IGNORECASE)
        if match:
            return EpisodeKey("special", int(match.group(1)))

    # ``13 OAV``/``12(OVA)`` is episode N's OAV edition, not special ordinal N.
    # The ordinal is absent, so return an unnumbered special (0) rather than a
    # fake special number that would collide with the regular episode.
    if re.search(
        r"(?:^|[\s._\-\[\]()])0*\d{1,3}[\s._-]*(?:OVA|OAV|OAD)(?:$|[\s._\-\[\]()])",
        clean,
        re.IGNORECASE,
    ):
        return EpisodeKey("special", 0)

    if re.search(
        r"(?:^|[\s._\-\[\]()])(?:SP|SPECIAL|OVA|OAV|OAD)(?:$|[\s._\-\[\]()])",
        clean,
        re.IGNORECASE,
    ):
        # 0 表示标签明确但编号缺失；计划阶段必须拒绝猜测。
        return EpisodeKey("special", 0)

    # 字幕组常把修正版写成 [02v2]。v2 是文件修订号，02 才是集号。
    versioned_episode = re.search(
        r"(?:^|[\[\s_\-(])0*(\d{1,3})v\d+(?=[\]\s_\-.()]|$)",
        clean,
        re.IGNORECASE,
    )
    if versioned_episode:
        return EpisodeKey("regular", int(versioned_episode.group(1)))

    hash_episode = re.search(
        r"(?<![A-Za-z0-9])#\s*0*(\d{1,3})(?:v\d+)?(?=[^0-9]|$)",
        clean,
        re.IGNORECASE,
    )
    if hash_episode:
        return EpisodeKey("regular", int(hash_episode.group(1)))

    finale_bracket = re.search(
        r"\[\s*0*(\d{1,3})\s*(?:END|FIN(?:AL)?)\s*\]",
        clean,
        re.IGNORECASE,
    )
    if finale_bracket:
        return EpisodeKey("regular", int(finale_bracket.group(1)))

    # A bare number in the title can be a sequel marker rather than an
    # episode number (``White Album 2 [01]``).  A release's final pure-numeric
    # bracket is the stronger episode token and must win over that title digit.
    bracketed_episodes = re.findall(r"\[\s*0*(\d{1,3})\s*\]", clean)
    if bracketed_episodes:
        return EpisodeKey("regular", int(bracketed_episodes[-1]))
    titled_bracket_episodes = re.findall(
        r"\[\s*0*(\d{1,3})\s*\([^]\r\n]+\)\s*\]",
        clean,
    )
    if titled_bracket_episodes:
        return EpisodeKey("regular", int(titled_bracket_episodes[-1]))
    cut_bracket = re.search(
        r"\[\s*0*(\d{1,3})\s+(?:director(?:'?s)?[ ._-]*)?cut\s*\]",
        clean,
        re.IGNORECASE,
    )
    if cut_bracket:
        return EpisodeKey("regular", int(cut_bracket.group(1)))

    regular_patterns = [
        r"(?:^|[^A-Za-z0-9])S\d{1,2}\s*E\s*0*(\d{1,4})(?:$|[^0-9])",
        SEASON_DASH_EPISODE_RE.pattern,
        r"(?:^|[^A-Za-z0-9])(?:EP?|E)\s*0*(\d{1,4})(?:$|[\s._\-\[\]()])",
        r"第\s*0*(\d{1,4})\s*(?:话|話|集)",
        r"(?:^|[\s_\-.(])0*(\d{1,3})(?:[\s_\-.()]|$)",
    ]
    for pattern in regular_patterns:
        match = re.search(pattern, clean, re.IGNORECASE)
        if match:
            return EpisodeKey("regular", int(match.group(1)))
    return None


def parse_ep_files(
    files: Sequence[Mapping[str, Any]],
    *,
    prefer_simplified: bool = False,
    defer_unnumbered_specials: bool = False,
    allow_release_dash_ordinal: bool = False,
) -> dict[EpisodeKey, list[dict[str, Any]]]:
    groups: dict[EpisodeKey, list[dict[str, Any]]] = defaultdict(list)
    for raw_item in files:
        item = dict(raw_item)
        if item.get("is_dir"):
            continue
        name = item.get("name")
        full_path = item.get("full_path")
        if not isinstance(name, str) or not isinstance(full_path, str):
            continue
        if cleanup_reason(name) is not None:
            continue
        ext = Path(name).suffix.lower()
        if ext not in MEDIA_EXTS:
            continue
        # ``Title - 01`` is deliberately *not* a global episode grammar: a
        # title can contain its own numbers (``The 100 - 01``) and ordinary
        # planning must not reinterpret them.  F enables this narrow branch
        # only after D has freshly proved one homogeneous, catalog-complete
        # release-dash run and supplied its explicit source-key map.
        release_dash = (
            release_dash_regular_episode(name)
            if allow_release_dash_ordinal
            else None
        )
        multi_text = DATE_NOISE_RE.sub(" ", name)
        multi_clean = EPISODE_NOISE_RE.sub(" ", multi_text)
        # Release groups also write a single episode as ``S4 - 01``.  The
        # generic range matcher used to read that as episodes 4 through 1.
        season_dash_episode = SEASON_DASH_EPISODE_RE.search(multi_clean)
        dual_number_episode = (
            SEASON_LOCAL_ABSOLUTE_RE.search(multi_clean)
            or DUAL_BRACKET_EPISODE_RE.search(multi_clean)
        )
        parent_path, _ = split_remote(full_path)
        parent_season = next(
            (
                season_number
                for segment in reversed(parent_path.strip("/").split("/"))
                if (season_number := _season_from_source("/" + segment)) is not None
            ),
            None,
        )
        titled_season_dash_episode = (
            re.search(
                rf"\b0*{parent_season}\s*-\s*0*(\d{{1,3}})(?=[^0-9]|$)",
                multi_clean,
                re.IGNORECASE,
            )
            if parent_season is not None
            else None
        )
        multi_match = None if season_dash_episode or dual_number_episode else (
            None
            if titled_season_dash_episode
            else (
                MULTI_EPISODE_RE.search(multi_clean)
                or MULTI_EPISODE_CONCAT_RE.search(multi_clean)
            )
        )
        override_key = item.get("_episode_key_override")
        override_end = item.get("_episode_end_override")
        override_kind = item.get("_episode_kind_override", "regular")
        key: EpisodeKey | None = (
            EpisodeKey(
                str(override_kind),
                int(override_key),
                int(override_end)
                if isinstance(override_end, int)
                and not isinstance(override_end, bool)
                and override_end >= int(override_key)
                else 0,
            )
            if isinstance(override_key, int)
            and not isinstance(override_key, bool)
            and override_key >= 0
            and override_kind in {"regular", "special"}
            else None
        )
        if key is None and titled_season_dash_episode is not None:
            key = EpisodeKey("regular", int(titled_season_dash_episode.group(1)))
        if key is None and release_dash is not None:
            key = EpisodeKey("regular", release_dash[1])
        if key is None and dual_number_episode is not None:
            key = EpisodeKey("regular", int(dual_number_episode.group(1)))
        if key is None and multi_match and multi_match.group(1) != multi_match.group(2):
            start = int(multi_match.group(1))
            end = int(multi_match.group(2))
            if end < start:
                # Release names such as ``Youjitsu 3 - 01`` use the first
                # number as the season and the second as the episode. A true
                # multi-episode range is ascending, so keep the episode here.
                key = EpisodeKey("regular", end)
            else:
                key = EpisodeKey("regular", start, end)
        if key is None:
            key = extract_episode_key(name)
        if key is None:
            parent_name = split_remote(full_path)[0].rsplit("/", 1)[-1]
            parent_clean = EPISODE_NOISE_RE.sub(" ", DATE_NOISE_RE.sub(" ", parent_name))
            parent_is_release_range = bool(
                MULTI_EPISODE_RE.search(parent_clean)
                or MULTI_EPISODE_CONCAT_RE.search(parent_clean)
            )
            if (
                not parent_is_release_range
                and not re.fullmatch(
                    r"(?:season|s)\s*0*\d{1,3}", parent_name, re.IGNORECASE
                )
                and not re.search(r"(?<!\d)(?:19|20)\d{2}(?!\d)", parent_name)
                and not re.search(r"www\.[A-Za-z0-9.-]+\.(?:com|net|org|cn|tv)", parent_name, re.IGNORECASE)
            ):
                key = extract_episode_key(parent_name)
        if key is not None:
            if key.kind == "special" and key.number == 0:
                if defer_unnumbered_specials:
                    # Smart TV planning gets a chance to match the title
                    # against TMDB; an unmatched subtitle can safely stay put.
                    continue
                raise PlanError(f"发现未编号特别篇，不能安全猜测集号: {full_path}")
            groups[key].append(item)

    if prefer_simplified:
        for key, items in list(groups.items()):
            explicit_simplified = any(
                Path(item["name"]).suffix.lower() in SUBTITLE_EXTS and is_simplified_sub(item["name"])
                for item in items
            )
            # A strict SRT proof is content evidence, whereas the historical
            # SC/TC preference below is only a filename heuristic.  Preserve
            # every proved/unproved SRT candidate for the global selector so
            # an invalid ``.zh-CN`` cannot discard a valid ``.zh-TW`` fallback
            # before it has a chance to rank bilingual > SC > TC.
            has_managed_subtitle_candidate = any(
                isinstance(item.get("_managed_subtitle_validation"), Mapping)
                for item in items
            )
            if explicit_simplified and not has_managed_subtitle_candidate:
                groups[key] = [
                    item
                    for item in items
                    if not (
                        Path(item["name"]).suffix.lower() in SUBTITLE_EXTS
                        and is_traditional_sub(item["name"])
                    )
                ]
    return dict(groups)


def media_kind(name: str) -> str:
    return _media_quality.media_kind(name, video_exts=VIDEO_EXTS)


def video_resolution_rank(item: Mapping[str, Any]) -> int:
    return _media_quality.video_resolution_rank(item)


def subtitle_presentation_rank(item: Mapping[str, Any]) -> int:
    return _media_quality.subtitle_presentation_rank(item)


def _prefer_highest_resolution_videos(
    items: Sequence[Mapping[str, Any]],
    *,
    prefer_simplified: bool = False,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Drop lower-resolution duplicates when a higher-resolution copy exists.

    Named editions are compared independently so a director's cut, theatrical
    cut, OVA or other meaningful edition is never discarded merely because its
    resolution is lower.
    """
    kept = [dict(item) for item in items]
    removed: list[dict[str, Any]] = []
    video_buckets: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for item in kept:
        name = str(item.get("name", ""))
        if Path(name).suffix.lower() not in VIDEO_EXTS:
            continue
        edition = entry_edition_tag(item)
        part = item.get("_episode_part_override")
        part_bucket = (
            f"part:{int(part)}"
            if isinstance(part, int) and not isinstance(part, bool) and part > 0
            else ""
        )
        bucket = "|".join(
            value
            for value in (
                _collision_key(edition) if edition else "",
                part_bucket,
            )
            if value
        )
        video_buckets[bucket].append(item)

    removed_paths: set[str] = set()
    removed_video_companions: list[tuple[str, str, str, str]] = []
    for bucket_items in video_buckets.values():
        ranks = {id(item): video_resolution_rank(item) for item in bucket_items}
        best_rank = max(ranks.values(), default=0)
        best_resolution_items = [
            item for item in bucket_items if ranks[id(item)] == best_rank
        ]
        presentation_ranks = {
            id(item): subtitle_presentation_rank(item)
            for item in best_resolution_items
        }
        best_presentation = max(presentation_ranks.values(), default=0)
        presentation_winners = [
            item
            for item in best_resolution_items
            if presentation_ranks[id(item)] == best_presentation
        ]
        language_preferred = False
        if prefer_simplified:
            simplified_winners = [
                item
                for item in presentation_winners
                if is_simplified_sub(
                    str(item.get("full_path", item.get("name", "")))
                )
            ]
            traditional_winners = [
                item
                for item in presentation_winners
                if is_traditional_sub(
                    str(item.get("full_path", item.get("name", "")))
                )
            ]
            # Language is a safe tie-breaker only when both alternatives say
            # what they contain.  An unlabelled or bilingual release is not
            # discarded merely because another path advertises 简中.
            if simplified_winners and traditional_winners:
                presentation_winners = simplified_winners
                language_preferred = True
        # Once content identity, edition and resolution are equal, prefer the
        # most complete release.  A larger positive source size is useful
        # evidence here (higher bitrate and/or additional audio/subtitle
        # tracks); the path is only a deterministic tie-breaker and never by
        # itself justifies deleting an equal-size/unknown-size copy.
        preferred = min(
            presentation_winners,
            key=lambda item: (
                -(_entry_size_value(item) or 0),
                _collision_key(
                    str(item.get("full_path", item.get("name", "")))
                ),
            ),
        )
        preferred_source = normalize_remote_path(str(preferred["full_path"]))
        preferred_size = _entry_size_value(preferred) or 0
        for item in bucket_items:
            cleanup_kind: str | None = None
            if ranks[id(item)] < best_rank and (
                ranks[id(item)] > 0 or best_rank >= 2160
            ):
                cleanup_kind = "lower_resolution"
            elif (
                item is not preferred
                and ranks[id(item)] == best_rank
                and best_presentation > 0
                and presentation_ranks[id(item)] < best_presentation
            ):
                cleanup_kind = "burned_subtitle_duplicate"
            elif (
                item is not preferred
                and language_preferred
                and ranks[id(item)] == best_rank
                and presentation_ranks.get(id(item), 0) == best_presentation
                and is_traditional_sub(
                    str(item.get("full_path", item.get("name", "")))
                )
            ):
                cleanup_kind = "traditional_language_duplicate"
            elif (
                item is not preferred
                and ranks[id(item)] == best_rank
                and presentation_ranks.get(id(item), 0) == best_presentation
                and preferred_size > 0
                and (_entry_size_value(item) or 0) > 0
                and preferred_size > (_entry_size_value(item) or 0)
            ):
                cleanup_kind = "same_resolution_duplicate"
            if cleanup_kind is None:
                continue
            removed_item = dict(item)
            removed_item["_preferred_resolution_source"] = preferred_source
            removed_item["_duplicate_cleanup_kind"] = cleanup_kind
            removed.append(removed_item)
            removed_paths.add(
                _collision_key(str(item.get("full_path", item.get("name", ""))))
            )
            if cleanup_kind == "lower_resolution":
                removed_video_companions.append((
                    normalize_remote_path(str(item["full_path"])),
                    _collision_key(Path(str(item.get("name", ""))).stem),
                    preferred_source,
                    _collision_key(Path(str(preferred.get("name", ""))).stem),
                ))

    # Exact-basename subtitle pairs are release-local companions.  If both the
    # discarded and retained video have such a subtitle, remove only the one
    # proven to belong to the low-resolution release.  Language-suffixed or
    # otherwise distinct subtitles do not share the exact video stem and are
    # deliberately preserved.
    if removed_video_companions:
        subtitles_by_stem: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for item in kept:
            if Path(str(item.get("name", ""))).suffix.lower() in SUBTITLE_EXTS:
                subtitles_by_stem[
                    _collision_key(Path(str(item.get("name", ""))).stem)
                ].append(item)
        for (
            removed_video,
            removed_stem,
            preferred_video,
            preferred_stem,
        ) in removed_video_companions:
            removed_release_dir, _ = split_remote(removed_video)
            preferred_release_dir, _ = split_remote(preferred_video)
            # A basename such as ``01`` is shared by many release folders.
            # Pair a subtitle only with the concrete release directory that
            # contains its video (or a nested ``字幕备份`` directory).
            # When both videos live in the same directory, release ownership
            # is ambiguous and no subtitle is deleted.
            if _collision_key(removed_release_dir) == _collision_key(
                preferred_release_dir
            ):
                continue
            lower_subtitles = [
                candidate
                for candidate in subtitles_by_stem.get(removed_stem, [])
                if _path_is_within(
                    normalize_remote_path(str(candidate["full_path"])),
                    removed_release_dir,
                )
            ]
            preferred_subtitles = [
                candidate
                for candidate in subtitles_by_stem.get(preferred_stem, [])
                if _path_is_within(
                    normalize_remote_path(str(candidate["full_path"])),
                    preferred_release_dir,
                )
            ]
            for subtitle in lower_subtitles:
                same_format = [
                    candidate
                    for candidate in preferred_subtitles
                    if Path(str(candidate.get("name", ""))).suffix.lower()
                    == Path(str(subtitle.get("name", ""))).suffix.lower()
                ]
                if len(same_format) != 1:
                    continue
                preferred_subtitle = same_format[0]
                if _collision_key(str(preferred_subtitle["full_path"])) == _collision_key(
                    str(subtitle["full_path"])
                ):
                    continue
                removed_item = dict(subtitle)
                removed_item["_preferred_resolution_source"] = normalize_remote_path(
                    str(preferred_subtitle["full_path"])
                )
                removed_item["_duplicate_cleanup_kind"] = "lower_resolution_subtitle"
                removed.append(removed_item)
                removed_paths.add(
                    _collision_key(str(subtitle.get("full_path", subtitle.get("name", ""))))
                )

    if removed_paths:
        kept = [
            item
            for item in kept
            if _collision_key(str(item.get("full_path", item.get("name", ""))))
            not in removed_paths
        ]
    return kept, removed


def _lower_resolution_cleanup_reason(preferred_source_path: str) -> str:
    return (
        "同一 TMDB 集号已有更高清晰度版本 "
        f"{normalize_remote_path(preferred_source_path)}，删除较低清晰度重复视频"
    )


def _lower_resolution_movie_cleanup_reason(
    preferred_source_path: str,
    tmdb_id: int,
) -> str:
    return (
        f"同一 TMDB 电影 movie/{tmdb_id} 已有更高清晰度版本 "
        f"{normalize_remote_path(preferred_source_path)}，删除较低清晰度重复视频"
    )


def _burned_subtitle_cleanup_reason(preferred_source_path: str) -> str:
    return (
        "同一 TMDB 集号已有同清晰度的内封/软字幕版本 "
        f"{normalize_remote_path(preferred_source_path)}，删除同内容重复视频"
    )


def _same_resolution_cleanup_reason(preferred_source_path: str) -> str:
    return (
        "同一 TMDB 集号已有同清晰度但文件更完整的版本 "
        f"{normalize_remote_path(preferred_source_path)}，删除较小的重复视频"
    )


def _traditional_language_cleanup_reason(preferred_source_path: str) -> str:
    return (
        "同一 TMDB 集号已有同清晰度同字幕形态的简体中文字幕版本 "
        f"{normalize_remote_path(preferred_source_path)}，删除繁体中文字幕重复视频"
    )


def _lower_resolution_subtitle_cleanup_reason(preferred_source_path: str) -> str:
    return (
        "同一 TMDB 集号的更高清晰度版本已有对应字幕 "
        f"{normalize_remote_path(preferred_source_path)}，删除低清发布版附带的重复字幕"
    )


def _dedupe_merged_tv_target_variants(plan: Plan) -> None:
    """Resolve cross-subplan quality duplicates for one confirmed TV target.

    Smart planning can place the same official episode in separate seasonal or
    special subplans before the source labels are fully normalized.  Per-plan
    quality selection cannot see the other copy.  At merge time the exact
    target path is the strongest available identity: it already includes the
    confirmed series, season, episode, multipart range and edition suffix.
    Therefore only exact target collisions with one uniquely highest
    resolution are eligible.  Theme videos, named editions and parts retain
    distinct final names and never enter the same bucket.
    """
    buckets: dict[tuple[str, str], list[PlannedFile]] = defaultdict(list)
    for item in plan.files:
        if item.media_kind == "video":
            buckets[_planned_companion_key(item.target_dir, item.final_name)].append(item)

    removed_ids: set[int] = set()
    cleanup: list[PlannedCleanup] = []
    removed_video_count = 0
    removed_subtitle_count = 0
    preferred_by_key: dict[tuple[str, str], tuple[int, PlannedFile]] = {}
    for companion_key, members in buckets.items():
        if len(members) < 2:
            continue
        ranks = {
            id(item): video_resolution_rank(
                {"name": item.original_name, "full_path": item.source_path}
            )
            for item in members
        }
        best_rank = max(ranks.values(), default=0)
        winners = [item for item in members if ranks[id(item)] == best_rank]
        if best_rank <= 0 or len(winners) != 1:
            continue
        winner = winners[0]
        losers = [
            item
            for item in members
            if ranks[id(item)] < best_rank
            and (ranks[id(item)] > 0 or best_rank >= 2160)
        ]
        # Do not partially rewrite a collision.  Every non-winner must have
        # sufficient resolution evidence before the unique high-quality copy
        # is allowed to win.
        if len(losers) != len(members) - 1:
            continue
        preferred_by_key[companion_key] = (best_rank, winner)
        for item in losers:
            # A lower-quality copy may already have won inside its own
            # subplan and therefore be referenced by an even lower cleanup
            # item.  Once that intermediate winner loses at merge time,
            # redirect the exact generated reason to the surviving winner so
            # execution validation retains an unbroken evidence chain.
            intermediate_reason = _lower_resolution_cleanup_reason(
                item.source_path
            )
            final_reason = _lower_resolution_cleanup_reason(winner.source_path)
            for existing_cleanup in plan.cleanup_files:
                if existing_cleanup.reason == intermediate_reason:
                    existing_cleanup.reason = final_reason
            removed_ids.add(id(item))
            reason = final_reason
            removed_video_count += 1
            cleanup.append(
                PlannedCleanup(
                    source_path=item.source_path,
                    source_dir=item.source_dir,
                    original_name=item.original_name,
                    reason=reason,
                    source_size=item.source_size,
                    source_modified=item.source_modified,
                )
            )

    def subtitle_slot(final_name: str) -> str:
        stem = Path(final_name).stem
        language = re.search(
            r"\.(zh-CN|zh-TW|en|ja)(?:\.\d+)?$", stem, re.IGNORECASE
        )
        if language:
            return _collision_key(language.group(1))
        return "subtitle" if re.search(r"\.subtitle\d*$", stem, re.I) else stem

    subtitle_buckets: dict[tuple[tuple[str, str], str], list[PlannedFile]] = defaultdict(list)
    for item in plan.files:
        if item.media_kind != "subtitle":
            continue
        companion_key = _planned_companion_key(item.target_dir, item.final_name)
        if companion_key in preferred_by_key:
            subtitle_buckets[(companion_key, subtitle_slot(item.final_name))].append(item)
    for (companion_key, _slot), members in subtitle_buckets.items():
        best_video_rank, winner_video = preferred_by_key[companion_key]
        ranks = {
            id(item): video_resolution_rank(
                {"name": item.original_name, "full_path": item.source_path}
            )
            for item in members
        }
        # A lower-release subtitle is deleted only when the retained release
        # supplies the same language/presentation slot.  Otherwise it remains
        # useful and is preserved.
        winners = [item for item in members if ranks[id(item)] == best_video_rank]
        if not winners:
            continue
        preferred_subtitle = min(
            winners, key=lambda item: _collision_key(item.source_path)
        )
        for item in members:
            rank = ranks[id(item)]
            if item is preferred_subtitle or rank >= best_video_rank:
                continue
            if not (rank > 0 or best_video_rank >= 2160):
                continue
            removed_ids.add(id(item))
            removed_subtitle_count += 1
            intermediate_reason = _lower_resolution_subtitle_cleanup_reason(
                item.source_path
            )
            final_reason = _lower_resolution_subtitle_cleanup_reason(
                preferred_subtitle.source_path
            )
            for existing_cleanup in plan.cleanup_files:
                if existing_cleanup.reason == intermediate_reason:
                    existing_cleanup.reason = final_reason
            cleanup.append(
                PlannedCleanup(
                    source_path=item.source_path,
                    source_dir=item.source_dir,
                    original_name=item.original_name,
                    reason=final_reason,
                    source_size=item.source_size,
                    source_modified=item.source_modified,
                )
            )

    if not removed_ids:
        return
    plan.files = [item for item in plan.files if id(item) not in removed_ids]
    plan.cleanup_files = _dedupe_cleanup_files([*plan.cleanup_files, *cleanup])
    if removed_video_count:
        plan.warnings.append(
            "合并子计划后发现同一 TMDB 集号的跨目录清晰度副本；"
            f"已保留最高清晰度并计划清理 {removed_video_count} 个低清视频"
        )
    if removed_subtitle_count:
        plan.warnings.append(
            f"已计划清理 {removed_subtitle_count} 个仅属于低清发布版的重复字幕"
        )


def make_unique_media_names(
    base_name: str,
    files: Sequence[Mapping[str, Any]],
    *,
    preserve_editions: bool = False,
) -> list[str]:
    """为同一集或同一电影的多个文件生成稳定且不会冲突的名称。

    返回值顺序与 ``files`` 输入顺序一致。已符合规范的主视频、v2 视频和字幕
    会优先保留其序号，避免重复运行时交换不同文件的名称。
    """
    output_by_index: dict[int, str] = {}
    used: set[str] = set()
    video_index = 0
    unknown_sub_counts: dict[str, int] = defaultdict(int)
    language_counts: dict[tuple[str, str], int] = defaultdict(int)
    pair_suffixes: dict[str, str] = {}
    named_video_editions = {
        edition
        for item in files
        if Path(str(item.get("name", ""))).suffix.lower() in VIDEO_EXTS
        and (edition := entry_edition_tag(item)) is not None
    }

    def canonical_rank(index_and_item: tuple[int, Mapping[str, Any]]) -> tuple[Any, ...]:
        index, item = index_and_item
        original = str(item["name"])
        stem = Path(original).stem
        ext = Path(original).suffix.lower()
        path_key = _collision_key(str(item.get("full_path", original)))
        if ext in VIDEO_EXTS:
            if stem == base_name:
                return (0, 0, path_key, index)
            match = re.fullmatch(re.escape(base_name) + r" - v(\d+)", stem, re.IGNORECASE)
            if match:
                return (0, int(match.group(1)), path_key, index)
            return (0, 10_000, path_key, index)

        # 已规范化字幕按已有语言/序号排序，保证再次运行时稳定。
        if stem.startswith(base_name + "."):
            suffix = stem[len(base_name) + 1 :]
            language_match = re.fullmatch(r"(zh-CN|zh-TW|en|ja)(?:\.(\d+))?", suffix, re.IGNORECASE)
            if language_match:
                language_order = {"zh-cn": 0, "zh-tw": 1, "en": 2, "ja": 3}
                return (
                    1,
                    language_order[language_match.group(1).lower()],
                    int(language_match.group(2) or 1),
                    path_key,
                    index,
                )
            unknown_match = re.fullmatch(r"subtitle(\d+)?", suffix, re.IGNORECASE)
            if unknown_match:
                return (1, 10, int(unknown_match.group(1) or 1), path_key, index)
        return (1, 100, 10_000, path_key, index)

    ranked_files = sorted(enumerate(files), key=canonical_rank)
    for original_index, item in ranked_files:
        original = str(item["name"])
        stem = Path(original).stem.lower()
        ext = Path(original).suffix.lower()
        if ext in VIDEO_EXTS:
            edition = entry_edition_tag(item) if preserve_editions else None
            if edition:
                suffix = f" {{edition-{edition}}}"
            else:
                video_index += 1
                suffix = "" if video_index == 1 else f" - v{video_index}"
            candidate = _compose_filename(base_name, suffix, ext)
        else:
            subtitle_edition = entry_edition_tag(item) if preserve_editions else None
            if (
                subtitle_edition is None
                and preserve_editions
                and len(named_video_editions) == 1
                and re.search(
                    r"(?:^|[\s._\-\[\]()])cut(?:$|[\s._\-\[\]()])",
                    original,
                    re.IGNORECASE,
                )
            ):
                subtitle_edition = next(iter(named_video_editions))
            edition_suffix = (
                f" {{edition-{subtitle_edition}}}" if subtitle_edition else ""
            )
            lang = subtitle_language(original)
            pair_key = re.sub(r"(?:[.\-_\[\]()\s])(?:idx|sub)$", "", stem)
            if pair_key in pair_suffixes:
                suffix = pair_suffixes[pair_key]
            elif lang:
                language_key = (edition_suffix, lang)
                language_counts[language_key] += 1
                count = language_counts[language_key]
                suffix = (
                    f"{edition_suffix}.{lang}"
                    if count == 1
                    else f"{edition_suffix}.{lang}.{count}"
                )
                pair_suffixes[pair_key] = suffix
            else:
                unknown_sub_counts[edition_suffix] += 1
                unknown_sub_index = unknown_sub_counts[edition_suffix]
                suffix = (
                    f"{edition_suffix}.subtitle"
                    if unknown_sub_index == 1
                    else f"{edition_suffix}.subtitle{unknown_sub_index}"
                )
                pair_suffixes[pair_key] = suffix
            candidate = _compose_filename(base_name, suffix, ext)

        serial = 2
        while _collision_key(candidate) in used:
            candidate = _compose_filename(base_name, f"{suffix}.{serial}", ext)
            serial += 1
        used.add(_collision_key(candidate))
        output_by_index[original_index] = candidate

    return [output_by_index[index] for index in range(len(files))]


# ---------------------------------------------------------------------------
# 计划生成
# ---------------------------------------------------------------------------


def _contextual_theme_cleanup_paths(
    files: Iterable[Mapping[str, Any]],
) -> set[str]:
    """Identify OP/ED episode variants only with directory and peer evidence."""
    entries = [dict(item) for item in files]
    primary_episode_numbers: set[int] = set()
    for item in entries:
        name = str(item.get("name") or "")
        if (
            item.get("is_dir")
            or Path(name).suffix.lower() not in VIDEO_EXTS
            or EPISODE_THEME_VARIANT_RE.search(name)
        ):
            continue
        key = extract_episode_key(name)
        if key is not None and key.kind == "regular" and not key.end_number:
            primary_episode_numbers.add(key.number)

    matched: set[str] = set()
    for item in entries:
        name = str(item.get("name") or "")
        full_path = str(item.get("full_path") or "")
        match = EPISODE_THEME_VARIANT_RE.search(name)
        if (
            item.get("is_dir")
            or not match
            or Path(name).suffix.lower() not in VIDEO_EXTS
            or not BONUS_DIRECTORY_RE.search(full_path)
            or int(match.group(1)) not in primary_episode_numbers
        ):
            continue
        matched.add(_collision_key(normalize_remote_path(full_path)))
    return matched


def _filter_media(files: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    entries = [dict(item) for item in files]
    contextual_cleanup = _contextual_theme_cleanup_paths(entries)
    result = []
    for item in entries:
        name = item.get("name")
        if item.get("is_dir") or not isinstance(name, str):
            continue
        if cleanup_reason(name) is not None or _contextual_cleanup_reason(item) is not None:
            continue
        if ADVERTISEMENT_NAME_RE.search(name):
            continue
        full_path = str(item.get("full_path") or "")
        if full_path and _collision_key(normalize_remote_path(full_path)) in contextual_cleanup:
            continue
        if NON_MEDIA_LIBRARY_CONTEXT_RE.search(full_path):
            continue
        if Path(name).suffix.lower() in MEDIA_EXTS:
            result.append(dict(item))
    return result


def normalize_exported_srt_entries(
    alist: Any,
    files: Iterable[Mapping[str, Any]],
    *,
    original_language: object = None,
    max_bytes: int = EXPORTED_SRT_MAX_BYTES,
) -> tuple[list[dict[str, Any]], list[dict[str, str]]]:
    """Promote only content-proven ``.sc/.tc.srt.txt`` entries to SRT.

    AList listings are immutable discovery evidence, so this helper creates
    shallow entry copies and keeps ``full_path``/``size`` untouched.  The
    canonical ``name`` is a planner-only projection; the executor therefore
    still moves the exact provider basename from ``full_path``.  Every SRT
    candidate is additionally given a bounded full-content proof used by the
    global one-track selector.  Invalid or unreadable candidates remain in
    the returned inventory with a structured marker and are never promoted
    into a formal write.
    """
    normalized: list[dict[str, Any]] = []
    issues: list[dict[str, str]] = []
    reader = getattr(alist, "read_file_bytes", None)
    if not callable(reader):
        reader = getattr(alist, "read_file_prefix", None)
    for raw in files:
        item = dict(raw)
        name = item.get("name")
        full_path = item.get("full_path")
        persisted_issue = item.get("_exported_srt_invalid")
        if isinstance(persisted_issue, str) and persisted_issue:
            if full_path and item.get("_exported_srt_issue_reported") is not True:
                issues.append({
                    "source_path": normalize_remote_path(str(full_path)),
                    "reason": persisted_issue,
                })
                item["_exported_srt_issue_reported"] = True
            normalized.append(item)
            continue
        if item.get("is_dir") or not isinstance(name, str):
            normalized.append(item)
            continue
        path = str(full_path or "")
        declared_size = _entry_size_value(item)
        payload: bytes | None = None

        # First project the provider's ``.sc/.tc.srt.txt`` spelling to a
        # parser-facing SRT basename.  The physical source path is untouched.
        if EXPORTED_SRT_SUFFIX_RE.search(name) is not None:
            reason = "导出字幕不是可验证的 UTF-8 SRT"
            if declared_size is not None and declared_size > max_bytes:
                reason = f"导出字幕超过 {max_bytes} 字节内容校验上限"
            elif not path or not callable(reader):
                reason = "无法从来源读取导出字幕的完整内容"
            else:
                read_limit = declared_size or max_bytes
                try:
                    payload = reader(path, max_bytes=read_limit)
                except (ApiError, OSError, ValueError, TypeError):
                    payload = None
                    reason = "读取导出字幕失败"
            proof = (
                validate_exported_srt_sidecar(
                    name,
                    payload,
                    declared_size=declared_size,
                    max_bytes=max_bytes,
                )
                if payload is not None
                else None
            )
            if proof is None:
                item["_exported_srt_invalid"] = reason
                if path:
                    issues.append({"source_path": normalize_remote_path(path), "reason": reason})
                normalized.append(item)
                continue
            # Keep the exact source object and its declared snapshot metadata;
            # only the parser-facing name is canonicalized.  This marker is
            # later persisted in source-scope rows for replay auditing.
            item["name"] = proof.normalized_name
            item["_subtitle_source_name"] = proof.source_name
            item["_subtitle_normalization"] = {
                "format": proof.format,
                "marker": proof.marker,
                "language": proof.language,
                "source_name": proof.source_name,
                "normalized_name": proof.normalized_name,
                "size": proof.size,
                "source_path": normalize_remote_path(path),
            }

        # Content proof is intentionally limited to SRT.  Other subtitle
        # containers remain on the historical deterministic selector lane;
        # they are never allowed to outrank a proven SRT candidate.
        if Path(str(item.get("name") or "")).suffix.lower() == ".srt":
            if payload is None and path and callable(reader):
                read_limit = declared_size or max_bytes
                try:
                    payload = reader(path, max_bytes=read_limit)
                except (ApiError, OSError, ValueError, TypeError):
                    payload = None
            verdict = validate_managed_subtitle_content(
                payload,
                original_language,
                declared_size=declared_size,
                max_bytes=max_bytes,
            )
            verdict = dict(verdict)
            verdict.update({
                "source_path": normalize_remote_path(path) if path else "",
                "source_name": (
                    split_remote(normalize_remote_path(path))[1]
                    if path else str(item.get("name") or "")
                ),
            })
            item["_managed_subtitle_validation"] = verdict
            if str(verdict.get("status") or "").casefold() == "satisfied":
                chinese_language = verdict.get("chinese_language")
                canonical_language = (
                    "zh-TW"
                    if chinese_language == "traditional_chinese"
                    or verdict.get("selection") == "traditional_chinese"
                    else "zh-CN"
                )
                current_name = str(item.get("name") or "")
                stem = current_name[:-4] if current_name.casefold().endswith(".srt") else current_name
                stem = re.sub(
                    r"\.(?:sc|tc|zh-CN|zh-TW|zh-Hans|zh-Hant)$",
                    "",
                    stem,
                    flags=re.IGNORECASE,
                )
                item["name"] = f"{stem}.{canonical_language}.srt"
                normalization = item.get("_subtitle_normalization")
                if isinstance(normalization, Mapping):
                    normalization = dict(normalization)
                    normalization["normalized_name"] = item["name"]
                    normalization["language"] = canonical_language
                    item["_subtitle_normalization"] = normalization
        normalized.append(item)
    return normalized, issues


def cleanup_reason(name: str) -> str | None:
    """Return a cleanup reason only for name-only OS litter.

    Release labels such as ``NCOP``, an advertisement image, or a font are
    useful residual evidence but no longer authorize deletion.  They remain
    in source until a user handles them or a later, task-owned staging flow
    proves they are rebuildable temporary content.
    """
    return cleanup_reason_for(classify_residual(name))


def _contextual_cleanup_reason(item: Mapping[str, Any]) -> str | None:
    """Classify a release extra only when filename and parent context agree."""
    name = unicodedata.normalize("NFKC", str(item.get("name") or ""))
    full_path = normalize_remote_path(str(item.get("full_path") or "/"))
    if (
        Path(name).suffix.lower() in VIDEO_EXTS
        and re.search(
            r"(?:^|/)(?:NCOP(?:\s*[&+／/]\s*(?:NC)?ED)?|NCED|OP\s*[&+／/]\s*ED)(?:/|$)",
            full_path,
            re.I,
        )
        and re.search(
            r"(?:^|[\s._\-\[\]()])(?:(?:NC)?(?:OP|ED)|MV|PV|MENU)"
            r"(?:\d+(?:v\d+)?)?"
            r"(?:$|[\s._\-\[\]()])",
            name,
            re.I,
        )
    ):
        return "无字幕片头/片尾/光盘菜单视频"
    if (
        Path(name).suffix.lower() in VIDEO_EXTS
        and BONUS_DIRECTORY_RE.search(full_path)
        and extract_episode_key(name) is not None
    ):
        for token_re, label in SPECIAL_LABEL_RULES:
            if re.search(token_re, name, re.I):
                return label
    return None


def _planned_cleanup_files(files: Iterable[Mapping[str, Any]]) -> list[PlannedCleanup]:
    entries = [dict(item) for item in files]
    planned: list[PlannedCleanup] = []
    seen: set[str] = set()
    for item in entries:
        name = item.get("name")
        full_path = item.get("full_path")
        if item.get("is_dir") or not isinstance(name, str) or not isinstance(full_path, str):
            continue
        reason = cleanup_reason(name)
        if reason is None:
            decision = classify_residual(full_path)
            reason = cleanup_reason_for(decision)
        if reason is None:
            continue
        source_path = normalize_remote_path(full_path)
        source_key = _collision_key(source_path)
        if source_key in seen:
            continue
        seen.add(source_key)
        source_dir, original_name = split_remote(source_path)
        planned.append(
            PlannedCleanup(
                source_path=source_path,
                source_dir=source_dir,
                original_name=original_name,
                reason=reason,
                source_size=_entry_size_value(item),
                source_modified=_entry_modified_value(item),
            )
        )
    return sorted(planned, key=lambda item: _collision_key(item.source_path))


def _dedupe_cleanup_files(
    items: Iterable[PlannedCleanup],
) -> list[PlannedCleanup]:
    unique: dict[str, PlannedCleanup] = {}
    for item in items:
        key = _collision_key(item.source_path)
        previous = unique.get(key)
        if previous is not None and previous != item:
            raise PlanError(f"同一清理文件出现不一致计划: {item.source_path}")
        unique[key] = item
    return sorted(unique.values(), key=lambda item: _collision_key(item.source_path))


def _append_cleanup_warning(warnings: list[str], cleanup_files: Sequence[PlannedCleanup]) -> None:
    if not cleanup_files:
        return
    preview = "、".join(item.original_name for item in cleanup_files[:4])
    suffix = f" 等 {len(cleanup_files)} 个文件" if len(cleanup_files) > 4 else ""
    warnings.append(f"写入并回读成功后清理任务来源中的无用文件：{preview}{suffix}")


def _restrict_cleanup_to_allowlist(plan: Plan) -> None:
    """Drop legacy destructive cleanup rows before a current plan is used.

    Older planner branches can still derive duplicate/theme cleanup rows while
    the current light-weight workflow deliberately retains those files.  Do
    not turn that legacy decision into a validation failure or an accidental
    delete: retain the source and make the reduced plan say so explicitly.
    Persisted plans are *not* repaired here; the runner rejects any such row
    again immediately before an executor can write.
    """
    source_root = normalize_remote_path(plan.source_root).rstrip("/") or "/"
    allowed: list[PlannedCleanup] = []
    retained: list[PlannedCleanup] = []
    for item in plan.cleanup_files:
        expected = cleanup_allowlist_reason(
            item.source_path,
            task_root=source_root,
        )
        if expected == item.reason:
            allowed.append(item)
        else:
            retained.append(item)
    if not retained:
        return
    plan.cleanup_files = allowed
    plan.warnings.append(
        f"{len(retained)} 个非任务临时残留已保留在来源；不会自动清理"
    )


def _cleanup_is_generated_housekeeping(item: PlannedCleanup) -> bool:
    """Return true only for operating-system litter, never for media assets."""
    name = item.original_name.casefold()
    return name.startswith("._") or name == ".ds_store"


def _unparsed_media_paths(
    files: Sequence[Mapping[str, Any]],
    groups: Mapping[EpisodeKey, Sequence[Mapping[str, Any]]],
) -> list[str]:
    recognized = {
        _collision_key(str(item.get("full_path", "")))
        for items in groups.values()
        for item in items
        if isinstance(item.get("full_path"), str)
    }
    return sorted(
        str(item["full_path"])
        for item in _filter_media(files)
        if isinstance(item.get("full_path"), str)
        and cleanup_reason(str(item.get("name", ""))) is None
        and _collision_key(str(item["full_path"])) not in recognized
    )


def _raise_unparsed_media(paths: Sequence[str], label: str) -> None:
    if not paths:
        return
    preview = ", ".join(paths[:5])
    suffix = f"，另有 {len(paths) - 5} 个" if len(paths) > 5 else ""
    raise PlanError(
        f"发现无法识别{label}编号的媒体文件，拒绝静默遗漏: {preview}{suffix}"
    )


def _add_snapshot_warnings(plan: Plan) -> None:
    weak = []
    for item in plan.files:
        if item.source_size is None:
            weak.append(item.source_path)
    if weak:
        plan.warnings.append(
            f"{len(weak)} 个文件缺少来源大小；写入前必须重新读取来源大小"
        )


def _resource_gap(
    kind: str,
    label: str,
    reason: str,
    *,
    files: Sequence[str] = (),
) -> dict[str, Any]:
    return {
        "kind": kind,
        "label": label,
        "reason": reason,
        "files": [normalize_remote_path(path) for path in files],
    }


def _planned_tv_episode_numbers(
    plan: Plan,
    season: int,
    *,
    series_dir: str | None = None,
) -> set[int]:
    pattern = re.compile(
        rf"(?:^|[ ._-])S0*{season}E0*(\d{{1,4}})(?:-E0*(\d{{1,4}}))?(?:$|[ ._-])",
        re.I,
    )
    numbers: set[int] = set()
    expected_target = (
        _collision_key(join_remote(series_dir, f"Season {season:02d}"))
        if series_dir is not None
        else None
    )
    for item in plan.files:
        if item.media_kind != "video":
            continue
        if expected_target is not None and _collision_key(item.target_dir) != expected_target:
            continue
        match = pattern.search(Path(item.final_name).stem)
        if match:
            start = int(match.group(1))
            end = int(match.group(2) or start)
            numbers.update(range(min(start, end), max(start, end) + 1))
    return numbers


def _existing_tv_episode_numbers(
    alist: AListClient,
    series_dir: str,
    season: int,
) -> set[int]:
    season_dir = join_remote(series_dir, f"Season {season:02d}")
    pattern = re.compile(
        rf"(?:^|[ ._-])S0*{season}E0*(\d{{1,4}})(?:-E0*(\d{{1,4}}))?(?:$|[ ._-])",
        re.I,
    )
    numbers: set[int] = set()
    for item in alist.try_list(season_dir, refresh=True) or []:
        name = str(item.get("name", ""))
        if item.get("is_dir") or Path(name).suffix.lower() not in VIDEO_EXTS:
            continue
        match = pattern.search(Path(name).stem)
        if match:
            start = int(match.group(1))
            end = int(match.group(2) or start)
            numbers.update(range(min(start, end), max(start, end) + 1))
    return numbers


def _tv_episode_resource_gaps(
    alist: AListClient,
    plan: Plan,
    *,
    series_dir: str,
    season: int,
    official_episodes: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    available = _planned_tv_episode_numbers(
        plan, season, series_dir=series_dir
    ) | _existing_tv_episode_numbers(
        alist, series_dir, season
    )
    today = date.today()
    gaps: list[dict[str, Any]] = []
    for episode in official_episodes:
        number = episode.get("episode_number")
        if not isinstance(number, int) or isinstance(number, bool) or number <= 0:
            continue
        air_date = str(episode.get("air_date") or "")
        if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", air_date):
            continue
        if datetime.fromisoformat(air_date).date() > today:
            continue
        if number in available:
            continue
        title = str(episode.get("name") or f"第 {number} 集").strip()
        gaps.append(_resource_gap(
            "missing_episode",
            f"S{season:02d}E{number:02d} {title}",
            "TMDB 已发布该集，但源目录和现有目标库均没有对应视频",
        ))
    return gaps


def _planned_member_movie_special_coverage(
    plan: Plan,
    official_episodes: Sequence[Mapping[str, Any]],
) -> dict[int, dict[str, Any]]:
    """Return TV specials already represented by a planned independent movie.

    TMDB occasionally exposes the same feature both as a movie and as a TV
    Season 00 row.  ScrapeFlow deliberately keeps the independent movie
    identity for Infuse, so the TV gap audit must not request a second copy of
    that exact content.  Exact normalized title plus release year is required;
    a merely similar title is never suppressed.
    """
    raw_member_movies = plan.metadata.get("member_movies")
    if not isinstance(raw_member_movies, Mapping):
        return {}
    planned_video_paths = {
        normalize_remote_path(join_remote(item.target_dir, item.final_name))
        for item in plan.files
        if item.media_kind == "video"
    }
    planned_video_dirs = {
        normalize_remote_path(item.target_dir)
        for item in plan.files
        if item.media_kind == "video"
    }
    represented: dict[tuple[str, str], dict[str, Any]] = {}
    for raw_target, identity in raw_member_movies.items():
        if not isinstance(raw_target, str) or not isinstance(identity, Mapping):
            continue
        target = normalize_remote_path(raw_target)
        if target not in planned_video_paths and target not in planned_video_dirs:
            continue
        title = str(identity.get("title") or "").strip()
        year = str(identity.get("year") or "").strip()
        title_key = _normalize_match_title(title)
        if not title_key or not re.fullmatch(r"(?:19|20)\d{2}", year):
            continue
        represented[(title_key, year)] = {
            "tmdb_id": identity.get("tmdb_id"),
            "title": title,
            "year": year,
            "target": target,
        }
    covered: dict[int, dict[str, Any]] = {}
    for episode in official_episodes:
        number = episode.get("episode_number")
        air_date = str(episode.get("air_date") or "")
        title = str(episode.get("name") or "").strip()
        if (
            not isinstance(number, int)
            or isinstance(number, bool)
            or number <= 0
            or not re.fullmatch(r"\d{4}-\d{2}-\d{2}", air_date)
        ):
            continue
        match = represented.get((_normalize_match_title(title), air_date[:4]))
        if match is not None:
            covered[number] = match
    return covered


def _append_complete_tv_resource_gaps(
    alist: AListClient,
    tmdb_client: TMDBClient,
    plan: Plan,
) -> None:
    """Check every published TMDB episode, including Season 00.

    Per-season builders used to inspect only the season currently being
    planned.  A complete franchise plan could therefore finish while a
    published special was absent.  Run this once on the final combined plan so
    planned files and existing target files are considered together.
    """
    identities: list[tuple[str, int, Mapping[str, Any]]] = []
    raw_member_tv = plan.metadata.get("member_tv")
    if isinstance(raw_member_tv, Mapping):
        for series_root, identity in raw_member_tv.items():
            if not isinstance(series_root, str) or not isinstance(identity, Mapping):
                continue
            tmdb_id = identity.get("tmdb_id")
            if isinstance(tmdb_id, int) and not isinstance(tmdb_id, bool) and tmdb_id > 0:
                identities.append((normalize_remote_path(series_root), tmdb_id, identity))
    raw_tmdb_id = plan.metadata.get("tmdb_id")
    if (
        isinstance(raw_tmdb_id, int)
        and not isinstance(raw_tmdb_id, bool)
        and raw_tmdb_id > 0
        and plan.mode in {"tv", "mixed"}
    ):
        identities.append((
            normalize_remote_path(str(plan.metadata.get("series_root") or plan.target_root)),
            raw_tmdb_id,
            plan.metadata,
        ))

    # Subplans may have recorded a gap before all sibling seasons were merged
    # into the final plan.  Recompute episode/season absence from the complete
    # file set instead of retaining those stale intermediate observations.
    existing_gaps = [
        dict(gap)
        for gap in (plan.scan_report.get("resource_gaps") or [])
        if isinstance(gap, Mapping)
        and str(gap.get("kind") or "") not in {"missing_episode", "missing_season"}
    ]
    seen = {
        (
            str(gap.get("kind") or ""),
            str(gap.get("label") or ""),
        )
        for gap in existing_gaps
    }
    deduplicated_identities: dict[tuple[str, int], Mapping[str, Any]] = {}
    for series_root, tmdb_id, identity in identities:
        deduplicated_identities[(series_root, tmdb_id)] = identity
    for (series_root, tmdb_id), identity in deduplicated_identities.items():
        try:
            show = tmdb_client.get(f"/tv/{tmdb_id}")
        except ApiError:
            continue
        season_numbers = sorted({
            int(item["season_number"])
            for item in (show.get("seasons") or [])
            if isinstance(item, Mapping)
            and isinstance(item.get("season_number"), int)
            and not isinstance(item.get("season_number"), bool)
            and int(item["season_number"]) >= 0
            and isinstance(item.get("episode_count"), int)
            and not isinstance(item.get("episode_count"), bool)
            and int(item["episode_count"]) > 0
        })
        for season_number in season_numbers:
            try:
                season_payload = tmdb_client.get(
                    f"/tv/{tmdb_id}/season/{season_number}"
                )
            except ApiError:
                continue
            official_episode_rows = [
                item
                for item in (season_payload.get("episodes") or [])
                if isinstance(item, Mapping)
            ]
            covered_by_movie = (
                _planned_member_movie_special_coverage(plan, official_episode_rows)
                if season_number == 0 else {}
            )
            if covered_by_movie:
                audit_rows = plan.scan_report.setdefault(
                    "tv_specials_covered_by_member_movies", [],
                )
                if isinstance(audit_rows, list):
                    for episode_number, movie in sorted(covered_by_movie.items()):
                        row = {
                            "tv_tmdb_id": tmdb_id,
                            "episode": episode_number,
                            "movie_tmdb_id": movie.get("tmdb_id"),
                            "title": movie.get("title"),
                            "year": movie.get("year"),
                            "target": movie.get("target"),
                        }
                        if row not in audit_rows:
                            audit_rows.append(row)
            gaps = _tv_episode_resource_gaps(
                alist,
                plan,
                series_dir=series_root,
                season=season_number,
                official_episodes=[
                    item for item in official_episode_rows
                    if item.get("episode_number") not in covered_by_movie
                ],
            )
            season_name = str(season_payload.get("name") or "").strip()
            if season_name:
                for gap in gaps:
                    gap["season_name"] = season_name
            for gap in gaps:
                gap["media"] = {
                    "tmdb_id": tmdb_id,
                    "title": str(identity.get("title") or show.get("name") or "").strip(),
                    "original_title": str(
                        identity.get("original_title") or show.get("original_name") or ""
                    ).strip(),
                    "year": str(identity.get("year") or "").strip(),
                    "target_root": series_root,
                }
                key = (str(gap.get("kind") or ""), str(gap.get("label") or ""))
                if key in seen:
                    continue
                seen.add(key)
                existing_gaps.append(gap)
    if existing_gaps:
        plan.scan_report["resource_gaps"] = existing_gaps


def _tv_season_resource_gaps(
    alist: AListClient,
    plan: Plan,
    *,
    series_dir: str,
    official_seasons: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    today = date.today()
    gaps: list[dict[str, Any]] = []
    for season_meta in official_seasons:
        number = season_meta.get("season_number")
        if not isinstance(number, int) or isinstance(number, bool) or number <= 0:
            continue
        air_date = str(season_meta.get("air_date") or "")
        if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", air_date):
            continue
        if datetime.fromisoformat(air_date).date() > today:
            continue
        available = _planned_tv_episode_numbers(
            plan, number, series_dir=series_dir
        ) | _existing_tv_episode_numbers(alist, series_dir, number)
        if available:
            continue
        expected = season_meta.get("episode_count")
        expected_text = (
            f"，TMDB 记录 {expected} 集"
            if isinstance(expected, int) and not isinstance(expected, bool) and expected > 0
            else ""
        )
        title = str(season_meta.get("name") or f"Season {number}").strip()
        gap = _resource_gap(
            "missing_season",
            f"Season {number:02d} {title}",
            f"TMDB 已发布该季{expected_text}，但源目录和现有目标库均没有任何该季视频",
        )
        if title:
            gap["season_name"] = title
        if isinstance(expected, int) and not isinstance(expected, bool) and expected > 0:
            gap["expected_episode_count"] = expected
        gaps.append(gap)
    return gaps


def _build_tv_episode_map(
    tmdb_client: TMDBClient,
    show: Mapping[str, Any],
    tmdb_id: int,
    season: int,
    absolute: bool,
    episode_group_id: str | None = None,
    special_titles: dict[EpisodeKey, str] | None = None,
    special_season_candidates: dict[int, list[EpisodeKey]] | None = None,
) -> dict[EpisodeKey, tuple[int, int, str]]:
    mapping: dict[EpisodeKey, tuple[int, int, str]] = {}
    season_data: Mapping[str, Any] | None = None
    if absolute and episode_group_id:
        group_data = tmdb_client.get(f"/tv/episode_group/{episode_group_id}")
        absolute_number = 1
        groups = sorted(
            (item for item in (group_data.get("groups") or []) if isinstance(item, Mapping)),
            key=lambda item: int(item.get("order", 0)),
        )
        for group in groups:
            episodes = sorted(
                (item for item in (group.get("episodes") or []) if isinstance(item, Mapping)),
                key=lambda item: int(item.get("order", item.get("episode_number", 0))),
            )
            for episode in episodes:
                target_season = int(episode["season_number"])
                target_episode = int(episode["episode_number"])
                mapping[EpisodeKey("regular", absolute_number)] = (
                    target_season,
                    target_episode,
                    str(episode.get("name") or f"第{absolute_number}集"),
                )
                absolute_number += 1
    elif absolute:
        seasons = sorted(
            (
                int(item["season_number"])
                for item in (show.get("seasons") or [])
                if isinstance(item, dict) and int(item.get("season_number", 0)) > 0
            )
        )
        absolute_number = 1
        for season_number in seasons:
            season_data = tmdb_client.get(f"/tv/{tmdb_id}/season/{season_number}")
            for episode in (season_data.get("episodes") or []):
                mapping[EpisodeKey("regular", absolute_number)] = (
                    season_number,
                    int(episode["episode_number"]),
                    str(episode.get("name") or f"第{absolute_number}集"),
                )
                absolute_number += 1
    else:
        season_data = tmdb_client.get(f"/tv/{tmdb_id}/season/{season}")
        for episode in (season_data.get("episodes") or []):
            number = int(episode["episode_number"])
            mapping[EpisodeKey("regular", number)] = (
                season,
                number,
                str(episode.get("name") or f"第{number}集"),
            )

    # 特别篇统一映射到 Season 00。即使 TMDB 没有条目，也只在号码明确时回退名称。
    try:
        specials_data = tmdb_client.get(f"/tv/{tmdb_id}/season/0")
    except ApiError as exc:
        if exc.status_code == 404:
            specials_data = {"episodes": []}
        else:
            raise
    for episode in (specials_data.get("episodes") or []):
        number = int(episode["episode_number"])
        key = EpisodeKey("special", number)
        title = str(episode.get("name") or f"特别篇 {number}")
        mapping[key] = (
            0,
            number,
            title,
        )
        if special_titles is not None:
            special_titles[key] = title
    if special_season_candidates is not None and not absolute and season_data is not None:
        regular_dates = sorted(
            str(item.get("air_date"))
            for item in (season_data.get("episodes") or [])
            if isinstance(item, Mapping)
            and re.fullmatch(r"\d{4}-\d{2}-\d{2}", str(item.get("air_date") or ""))
        )
        if regular_dates:
            season_start = datetime.fromisoformat(regular_dates[0]).date()
            season_end = datetime.fromisoformat(regular_dates[-1]).date()
            later_season_starts = sorted(
                datetime.fromisoformat(str(item.get("air_date"))).date()
                for item in (show.get("seasons") or [])
                if isinstance(item, Mapping)
                and isinstance(item.get("season_number"), int)
                and int(item["season_number"]) > season
                and re.fullmatch(r"\d{4}-\d{2}-\d{2}", str(item.get("air_date") or ""))
            )
            window_end = (
                later_season_starts[0]
                if later_season_starts
                # The final season can receive an OVA years later through a
                # game or anniversary release. With no later season boundary,
                # keep the official post-season timeline open instead of
                # dropping a verified TMDB special after an arbitrary cutoff.
                else datetime.max.date()
            )
            candidates = sorted(
                (
                    datetime.fromisoformat(str(item.get("air_date"))).date(),
                    EpisodeKey("special", int(item["episode_number"])),
                )
                for item in (specials_data.get("episodes") or [])
                if isinstance(item, Mapping)
                and isinstance(item.get("episode_number"), int)
                and re.fullmatch(r"\d{4}-\d{2}-\d{2}", str(item.get("air_date") or ""))
                and season_start
                <= datetime.fromisoformat(str(item.get("air_date"))).date()
                < window_end
            )
            if candidates:
                special_season_candidates[season] = [key for _, key in candidates]
    return mapping


def _special_title_key(value: str, series_title: str) -> str:
    value = re.sub(r"\{(?:tmdb|imdb)-[^{}]+\}", " ", value, flags=re.IGNORECASE)
    value = re.sub(
        r"(?:\.(?:mkv|mp4|m4v|avi|mov|wmv|flv|ts|m2ts|ass|ssa|srt|vtt|sub))+$",
        " ",
        value,
        flags=re.IGNORECASE,
    )
    key = _normalize_match_title(value)
    series_key = _normalize_match_title(series_title)
    if series_key:
        key = key.replace(series_key, "")
    for noise in ("特别篇", "特典", "续章", "specials", "special", "movie", "ova", "oav", "oad"):
        key = key.replace(_normalize_match_title(noise), "")
    return key


def _special_label_tokens(value: str) -> set[str]:
    """Return release labels such as MMR03/OVA2 that also occur in TMDB titles."""
    normalized = unicodedata.normalize("NFKC", value).upper()
    tokens = {
        f"{label}{int(number)}"
        for label, number in re.findall(r"(?<![A-Z])([A-Z]{2,12})[\s._#-]*0*(\d{1,3})(?!\d)", normalized)
        if label not in {"EP", "HEVC", "AVC", "AAC", "FLAC", "WEB", "BD", "MA"}
    }
    tokens.update(
        f"{label}{int(number)}"
        for label, number in re.findall(
            r"(?<![A-Z])(OVA|OAV|OAD)[\s._-]*(?:SERIES|系列)[\s._-]*\[?\s*0*(\d{1,3})",
            normalized,
        )
    )
    roman_values = {"I": 1, "V": 5, "X": 10}
    for label, roman in re.findall(
        r"(?<![A-Z])([A-Z]{2,12})[\s._-]+([IVX]{1,5})(?![A-Z])",
        normalized,
    ):
        if label in {"EP", "HEVC", "AVC", "AAC", "FLAC", "WEB", "BD", "MA"}:
            continue
        total = 0
        previous = 0
        for char in reversed(roman):
            value = roman_values[char]
            total += -value if value < previous else value
            previous = max(previous, value)
        if 0 < total <= 100:
            tokens.add(f"{label}{total}")
    return tokens


def _special_context_tokens(value: str) -> set[str]:
    """Return title words that can distinguish two equal OVA/OAD labels.

    TMDB can contain more than one physical release called ``OVA #3`` inside
    one Season 00.  Release names usually retain the sub-series label (for
    example ``Darkness``), while the original OVA does not.  Only meaningful
    Latin title words participate here; codec, container and release-group
    noise must never become identity evidence.
    """
    ignored = {
        "subtitle", "group", "season", "series", "special", "episode",
        "ova", "oav", "oad", "bdrip", "bluray", "webrip", "webdl",
        "hevc", "x264", "x265", "flac", "truehd", "aac", "mkv", "mp4",
        "chs", "cht", "jpn", "eng", "bit", "backup",
    }
    return {
        token
        for token in re.findall(
            r"[a-z]{3,}", unicodedata.normalize("NFKC", value).casefold()
        )
        if token not in ignored
    }


def _fractional_signatures(value: str) -> set[tuple[int, str]]:
    """Extract normalized decimal episode labels from a metadata title."""
    normalized = unicodedata.normalize("NFKC", value)
    return {
        (int(whole), fractional.rstrip("0") or "0")
        for whole, fractional in re.findall(
            r"(?<!\d)0*(\d{1,3})\.(\d{1,3})(?!\d)",
            normalized,
        )
    }


def _multilingual_episode_titles(
    tmdb_client: TMDBClient,
    tmdb_id: int,
    episode_map: Mapping[EpisodeKey, tuple[int, int, str]],
) -> dict[tuple[int, int], list[str]]:
    """Load online TMDB regular and special titles in useful languages.

    This is deliberately evidence gathering, not a fallback guess. A caller
    may auto-map only when the same explicit decimal label identifies one
    TMDB episode across the collected titles.
    """
    titles: dict[tuple[int, int], list[str]] = {}
    for target_season, target_episode, title in episode_map.values():
        target = (target_season, target_episode)
        if title and title not in titles.setdefault(target, []):
            titles[target].append(title)
    target_seasons = sorted({season for season, _ in titles})
    primary_language = str(getattr(tmdb_client, "language", "") or "")
    for language in ("zh-CN", "zh-TW", "ja-JP", "en-US"):
        if language == primary_language:
            continue
        for target_season in target_seasons:
            try:
                payload = tmdb_client.get(
                    f"/tv/{tmdb_id}/season/{target_season}",
                    language=language,
                )
            except ApiError:
                # The main metadata request already succeeded. An unavailable
                # translation is optional evidence and must not break planning.
                continue
            for episode in payload.get("episodes") or []:
                if (
                    not isinstance(episode, Mapping)
                    or isinstance(episode.get("episode_number"), bool)
                    or not isinstance(episode.get("episode_number"), int)
                ):
                    continue
                target = (target_season, int(episode["episode_number"]))
                title = str(episode.get("name") or "").strip()
                if title and title not in titles.setdefault(target, []):
                    titles[target].append(title)
    return titles


def _fractional_recap_evidence_candidates(
    tmdb_client: TMDBClient,
    tmdb_id: int,
    season: int,
    source_key: EpisodeKey,
    titles: Mapping[tuple[int, int], Sequence[str]],
    source_items: Sequence[Mapping[str, Any]],
) -> tuple[
    list[tuple[int, int, str, tuple[str, ...], tuple[str, ...]]],
    str,
]:
    """Resolve N.5 using scored independent evidence and conflict vetoes."""
    if source_key.fractional_digits != "5":
        return [], "没有找到可用于非 N.5 小数集的高置信半集识别证据"

    recap_re = re.compile(
        r"(?:总集篇|總集篇|総集編|総集篇|recap|digest|compilation|summary|回顾|回顧)",
        re.IGNORECASE,
    )
    interlude_re = re.compile(
        r"(?:半集|半话|半話|闲话|閑話|幕间|幕間|interlude|extra|"
        r"side[ ._-]*story|番外|特别篇|special)",
        re.IGNORECASE,
    )
    ova_re = re.compile(
        r"(?:^|[^a-z0-9])(?:ova|oav|oad)(?:$|[^a-z0-9])",
        re.IGNORECASE,
    )
    promo_re = re.compile(
        r"(?:^|[^a-z0-9])(?:pv|cm|trailer|preview|menu)(?:$|[^a-z0-9])|"
        r"预告|預告|特报|特報|メニュー",
        re.IGNORECASE,
    )

    def parsed_date(value: Any) -> date | None:
        raw = str(value or "")
        if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", raw):
            return None
        return datetime.fromisoformat(raw).date()

    def referenced_seasons(values: Sequence[str]) -> set[int]:
        references: set[int] = set()
        for value in values:
            for match in re.finditer(
                r"(?:第\s*0*(\d{1,2})\s*季|\bseason\s*0*(\d{1,2})\b|"
                r"\b0*(\d{1,2})(?:st|nd|rd|th)\s+season\b)",
                unicodedata.normalize("NFKC", value),
                re.IGNORECASE,
            ):
                references.add(
                    int(next(group for group in match.groups() if group is not None))
                )
        return references

    source_values = tuple(dict.fromkeys(
        str(item.get(field) or "")
        for item in source_items
        for field in ("name", "full_path")
        if str(item.get(field) or "")
    ))
    source_text = " ".join(source_values)
    source_recap = bool(recap_re.search(source_text))
    source_interlude = bool(interlude_re.search(source_text))
    source_ova = bool(ova_re.search(source_text))
    source_promo = bool(promo_re.search(source_text))
    source_tokens = _special_context_tokens(source_text)
    source_durations: list[float] = []
    for item in source_items:
        for field, divisor in (
            ("duration_ms", 60_000.0),
            ("duration_seconds", 60.0),
            ("runtime_minutes", 1.0),
        ):
            value = item.get(field)
            if (
                isinstance(value, (int, float))
                and not isinstance(value, bool)
                and float(value) > 0
            ):
                source_durations.append(float(value) / divisor)
                break
    source_duration = (
        sorted(source_durations)[len(source_durations) // 2]
        if source_durations
        else None
    )

    try:
        season_payload = tmdb_client.get(f"/tv/{tmdb_id}/season/{season}")
    except ApiError:
        season_payload = {}
    try:
        specials_payload = tmdb_client.get(f"/tv/{tmdb_id}/season/0")
    except ApiError:
        specials_payload = {}
    # A fractional recap often sits between the LAST episode of one season and
    # the FIRST of the next (War of Underworld 24.5/36.5), or inside a season
    # folder whose ordinal differs from the planning season.  Fetch a bounded
    # set of positive seasons so the N/N+1 air-date interval can be found
    # across season boundaries instead of only inside ``season``.
    season_episodes: dict[int, list[Mapping[str, Any]]] = {
        season: [
            item
            for item in season_payload.get("episodes") or []
            if isinstance(item, Mapping)
            and isinstance(item.get("episode_number"), int)
            and not isinstance(item.get("episode_number"), bool)
        ],
    }
    try:
        show_payload = tmdb_client.get(f"/tv/{tmdb_id}")
        candidate_seasons = [
            int(item["season_number"])
            for item in show_payload.get("seasons") or []
            if isinstance(item, Mapping)
            and isinstance(item.get("season_number"), int)
            and not isinstance(item.get("season_number"), bool)
            and int(item["season_number"]) > 0
        ]
    except ApiError:
        candidate_seasons = []
    for other_season in candidate_seasons[:10]:
        if other_season in season_episodes:
            continue
        try:
            payload = tmdb_client.get(f"/tv/{tmdb_id}/season/{other_season}")
        except ApiError:
            continue
        rows = [
            item
            for item in payload.get("episodes") or []
            if isinstance(item, Mapping)
            and isinstance(item.get("episode_number"), int)
            and not isinstance(item.get("episode_number"), bool)
        ]
        if rows:
            season_episodes[other_season] = rows
    all_regular_episodes = [
        item for rows in season_episodes.values() for item in rows
    ]
    regular_by_number = {
        int(item["episode_number"]): item for item in season_episodes.get(season, [])
    }
    before = regular_by_number.get(source_key.number)
    after = regular_by_number.get(source_key.number + 1)
    before_date = parsed_date(before.get("air_date")) if before else None
    after_date = parsed_date(after.get("air_date")) if after else None
    # Cross-season interval evidence: any season whose episode N precedes the
    # candidate and whose episode N+1 (or the next season's E01) follows it.
    interval_pairs: list[tuple[date, date]] = []
    for _season_number, rows in season_episodes.items():
        by_number = {int(item["episode_number"]): item for item in rows}
        before_item = by_number.get(source_key.number)
        after_item = by_number.get(source_key.number + 1)
        before_value = (
            parsed_date(before_item.get("air_date")) if before_item else None
        )
        after_value = parsed_date(after_item.get("air_date")) if after_item else None
        if before_value is None:
            continue
        if after_value is None:
            # N is the season finale: the next season's first episode is the
            # natural upper bound of the interval.
            for next_season in sorted(season_episodes):
                if next_season <= _season_number:
                    continue
                next_rows = season_episodes.get(next_season) or []
                first_item = next(
                    (
                        item for item in next_rows
                        if int(item.get("episode_number")) == 1
                    ),
                    None,
                )
                after_value = (
                    parsed_date(first_item.get("air_date")) if first_item else None
                )
                break
        if after_value is not None and before_value < after_value:
            interval_pairs.append((before_value, after_value))
    regular_dates = sorted(
        item_date
        for item in all_regular_episodes
        if (item_date := parsed_date(item.get("air_date"))) is not None
    )
    regular_runtimes = sorted(
        int(item["runtime"])
        for item in all_regular_episodes
        if isinstance(item.get("runtime"), int)
        and not isinstance(item.get("runtime"), bool)
        and int(item["runtime"]) > 0
    )
    median_runtime = (
        regular_runtimes[len(regular_runtimes) // 2]
        if regular_runtimes
        else None
    )
    special_metadata: dict[tuple[int, int], Mapping[str, Any]] = {}
    for item in specials_payload.get("episodes") or []:
        if (
            isinstance(item, Mapping)
            and isinstance(item.get("episode_number"), int)
            and not isinstance(item.get("episode_number"), bool)
        ):
            special_metadata[(0, int(item["episode_number"]))] = item

    signature = (source_key.number, source_key.fractional_digits)
    candidate_targets = {
        target
        for target, localized_titles in titles.items()
        if target[0] == 0
        or any(signature in _fractional_signatures(title) for title in localized_titles)
    }
    candidate_targets.update(special_metadata)
    # Data-ized answer for releases whose local N.5 number never appears in
    # any official title (e.g. 刀剑神域 [24.5] -> S00E24 第0话 Reflection).
    lexicon_row = FRACTIONAL_SPECIAL_ALIASES.get(tmdb_id, {}).get(
        f"{source_key.number}.{source_key.fractional_digits}"
    )
    lexicon_target: tuple[int, int] | None = None
    if lexicon_row is not None:
        lexicon_target = (0, int(str(lexicon_row[0])[4:]))
        candidate_targets.add(lexicon_target)
    scored: list[dict[str, Any]] = []
    for target_season, target_episode in sorted(candidate_targets):
        metadata = special_metadata.get((target_season, target_episode), {})
        aliases = tuple(dict.fromkeys((
            *titles.get((target_season, target_episode), ()),
            str(metadata.get("name") or ""),
        )))
        aliases = tuple(alias for alias in aliases if alias)
        if not aliases:
            continue
        signatures: set[tuple[int, str]] = set()
        for alias in aliases:
            signatures.update(_fractional_signatures(alias))
        data_ized = lexicon_target is not None and (target_season, target_episode) == lexicon_target
        explicit_fractional = signature in signatures or data_ized
        if target_season != 0 and not explicit_fractional:
            continue

        score = 1.0
        evidence = ["源文件明确使用 N.5/半集编号"]
        conflicts: list[str] = []
        if data_ized:
            score += 6.0
            evidence.append("数据化官方小数集映射")
            official_alias = str(lexicon_row[1]) if lexicon_row else ""
            if official_alias and official_alias not in aliases:
                aliases = (official_alias, *aliases)
        elif explicit_fractional:
            score += 6.0
            evidence.append("官方多语言标题含相同小数集号")
        elif signatures:
            labels = ", ".join(
                f"{whole}.{digits}" for whole, digits in sorted(signatures)
            )
            conflicts.append(f"官方标题明写其他小数集号 {labels}")

        official_text = " ".join(aliases)
        official_recap = bool(recap_re.search(official_text))
        official_interlude = bool(interlude_re.search(official_text))
        official_ova = bool(ova_re.search(official_text))
        official_promo = bool(promo_re.search(official_text))
        if official_recap:
            score += 2.0
            evidence.append("官方 Season 00 标题/别名明确为总集篇或回顾")
        if official_interlude:
            score += 1.5
            evidence.append("官方标题/别名含半集、幕间或番外语义")
        if source_recap and official_recap:
            score += 2.0
            evidence.append("源标题与官方总集篇语义一致")
        if source_interlude and official_interlude:
            score += 1.0
            evidence.append("源标题与官方半集/幕间语义一致")
        if source_recap and official_ova and not official_recap:
            conflicts.append("源标题是总集篇，官方标题却是 OVA/OAD")
        if source_ova and official_recap and not source_recap:
            conflicts.append("源标题是 OVA/OAD，官方标题却是总集篇")
        if source_promo or official_promo:
            conflicts.append("源或官方标题表明这是预告/PV/菜单而非半集正片")

        season_refs = referenced_seasons(aliases)
        if season_refs and season not in season_refs:
            conflicts.append(
                "官方标题归属于其他季度 "
                + "/".join(str(value) for value in sorted(season_refs))
            )
        elif season in season_refs:
            score += 1.5
            evidence.append(f"官方标题明确归属第 {season} 季")

        official_tokens = _special_context_tokens(official_text)
        common_tokens = {
            token for token in source_tokens & official_tokens if len(token) >= 4
        }
        normalized_source = _normalize_match_title(source_text)
        exact_title_overlap = any(
            len(normalized_alias := _normalize_match_title(alias)) >= 6
            and normalized_alias in normalized_source
            for alias in aliases
        )
        if exact_title_overlap:
            score += 2.0
            evidence.append("源标题含官方特别篇标题/别名")
        elif len(common_tokens) >= 2 or any(
            len(token) >= 7 for token in common_tokens
        ):
            score += 1.0
            evidence.append("源标题与官方别名有特异词重合")

        candidate_date = parsed_date(metadata.get("air_date"))
        exact_interval = bool(
            candidate_date
            and any(
                interval_start < candidate_date < interval_end
                for interval_start, interval_end in interval_pairs
            )
        )
        if exact_interval:
            score += 4.0
            evidence.append("官方播出日位于某季 N 与 N+1 之间（含跨季边界）")
        elif candidate_date and before_date and not after_date:
            if 0 < (candidate_date - before_date).days <= 35:
                score += 1.5
                evidence.append("缺少 N+1 日期时，播出日紧随 N 之后")
        elif candidate_date and after_date and not before_date:
            if 0 < (after_date - candidate_date).days <= 35:
                score += 1.5
                evidence.append("缺少 N 日期时，播出日紧邻 N+1 之前")
        if candidate_date and regular_dates and not explicit_fractional:
            if regular_dates[0] <= candidate_date <= regular_dates[-1]:
                score += 1.0
                evidence.append(f"播出日落在第 {season} 季官方时间线内")
            elif min(
                abs((candidate_date - regular_dates[0]).days),
                abs((candidate_date - regular_dates[-1]).days),
            ) > 120:
                conflicts.append(f"播出日与第 {season} 季时间线相距超过 120 天")

        runtime_value = metadata.get("runtime")
        runtime = (
            int(runtime_value)
            if isinstance(runtime_value, int)
            and not isinstance(runtime_value, bool)
            and runtime_value > 0
            else None
        )
        if runtime is not None:
            if runtime < 8 and not explicit_fractional:
                conflicts.append(f"官方运行时长仅 {runtime} 分钟，更像短预告/特典")
            elif median_runtime and 0.65 * median_runtime <= runtime <= 2.25 * median_runtime:
                score += 1.0
                evidence.append("官方运行时长与本季正片/长篇范围一致")
            elif runtime >= 18:
                score += 0.5
                evidence.append("官方运行时长达到完整节目级别")
            if runtime >= 40:
                score += 0.5
                evidence.append("官方运行时长表明这是长篇特别篇")
            if source_duration is not None:
                if abs(source_duration - runtime) <= max(3.0, runtime * 0.2):
                    score += 1.5
                    evidence.append("源视频与官方运行时长相符")
                elif abs(source_duration - runtime) > max(6.0, runtime * 0.35):
                    conflicts.append(
                        f"源视频约 {source_duration:.1f} 分钟，与官方 {runtime} 分钟明显冲突"
                    )

        movie_release_inside = False
        if target_season == 0 and runtime is not None and runtime >= 40:
            release_dates: set[date] = set()
            for alias in aliases:
                query_key = _normalize_match_title(alias)
                if len(query_key) < 4:
                    continue
                try:
                    movie_results = tmdb_client.get(
                        "/search/movie", query=alias
                    ).get("results") or []
                except ApiError:
                    continue
                for movie in movie_results[:5]:
                    if not isinstance(movie, Mapping):
                        continue
                    result_keys = {
                        _normalize_match_title(str(movie.get(field) or ""))
                        for field in ("title", "original_title")
                    }
                    if not any(
                        result
                        and (query_key in result or result in query_key)
                        for result in result_keys
                    ):
                        continue
                    release_date = parsed_date(movie.get("release_date"))
                    if release_date is not None:
                        release_dates.add(release_date)
            if len(release_dates) == 1 and before_date and after_date:
                release_date = next(iter(release_dates))
                if before_date < release_date < after_date:
                    movie_release_inside = True
                    score += 1.0
                    evidence.append("同名官方电影发行日也落在 N/N+1 区间")
                elif exact_interval:
                    conflicts.append("同名官方电影发行日否定 TMDB TV 特别篇日期")

        identity_evidence = bool(
            explicit_fractional
            or official_recap
            or (official_interlude and (source_interlude or source_recap))
            or (
                source_recap
                and (exact_title_overlap or bool(common_tokens))
                and candidate_date
            )
            or (
                runtime is not None
                and runtime >= 40
                and exact_interval
                and (movie_release_inside or exact_title_overlap)
            )
            or (
                # A full-length program airing exactly between one season's
                # N and N+1 is the generic structural identity of a recap;
                # it must not require any title semantics or data row.
                exact_interval
                and runtime is not None
                and runtime >= 18
            )
        )
        scored.append({
            "target": (target_season, target_episode),
            "title": aliases[0],
            "aliases": aliases,
            "score": score,
            "evidence": evidence,
            "conflicts": conflicts,
            "identity": identity_evidence,
        })

    eligible = [
        item
        for item in scored
        if item["identity"] and not item["conflicts"] and item["score"] >= 6.0
    ]
    eligible.sort(key=lambda item: (-item["score"], item["target"]))
    if len(eligible) == 1:
        eligible[0]["score"] += 1.0
        eligible[0]["evidence"].append("高分候选唯一")
    if eligible and eligible[0]["score"] >= 7.0:
        top_score = float(eligible[0]["score"])
        contenders = [
            item for item in eligible if top_score - float(item["score"]) < 2.0
        ]
        if len(contenders) == 1:
            winner = contenders[0]
            if len(eligible) > 1:
                winner["evidence"].append("与次名得分差满足自动映射安全间隔")
            target_season, target_episode = winner["target"]
            return [(
                target_season,
                target_episode,
                winner["title"],
                winner["aliases"],
                tuple(winner["evidence"]),
            )], ""
        return [
            (
                item["target"][0],
                item["target"][1],
                item["title"],
                item["aliases"],
                tuple(item["evidence"]),
            )
            for item in contenders
        ], "多个候选的多证据得分差小于自动映射安全间隔"

    if scored:
        strongest = max(scored, key=lambda item: float(item["score"]))
        target_season, target_episode = strongest["target"]
        target_label = f"S{target_season:02d}E{target_episode:02d}"
        if strongest["conflicts"]:
            return [], (
                f"{target_label} 存在排他冲突："
                + "；".join(str(value) for value in strongest["conflicts"])
            )
        return [], (
            f"{target_label} 的标题、时间线、季度、语义、运行时长与唯一性"
            "证据未达到高置信门槛"
        )
    return [], "TMDB 官方 Season 00/季度中没有可参与半集多证据评分的候选"


def _e00_special_candidates(
    items: Sequence[Mapping[str, Any]],
    titles: Mapping[tuple[int, int], Sequence[str]],
    series_title: str,
) -> list[EpisodeKey]:
    """Resolve E00 only from a unique official title match.

    ``E00`` describes the source numbering, not a TMDB Season 00 index.  It
    therefore must not silently become S00E01.  Search the multilingual TMDB
    titles and accept only one candidate whose explicit prologue/episode-zero
    wording is also present in the release filename or path.
    """
    source_labels: list[str] = []
    for item in items:
        label = f"{item.get('name', '')} {item.get('full_path', '')}"
        label = re.sub(
            r"(?<![A-Za-z0-9])(?:S\d{1,3}\s*)?E\s*0+(?!\d)|"
            r"(?<![A-Za-z0-9])0{1,3}(?!\d)",
            " ",
            unicodedata.normalize("NFKC", label),
            flags=re.I,
        )
        key = _special_title_key(label, series_title)
        if key:
            source_labels.append(key)
    candidates: list[EpisodeKey] = []
    explicit_zero_title = re.compile(
        r"(?:prologue|episode\s*0|第\s*0\s*[话話集]|序章|プロローグ|前日[譚谭])",
        re.I,
    )
    for (target_season, target_episode), localized_titles in titles.items():
        if target_season != 0:
            continue
        matching_title_keys = [
            _special_title_key(title, series_title)
            for title in localized_titles
            if explicit_zero_title.search(unicodedata.normalize("NFKC", title))
        ]
        matching_title_keys = [key for key in matching_title_keys if len(key) >= 3]
        if not matching_title_keys:
            continue
        if any(
            title_key in source_key or source_key in title_key
            for source_key in source_labels
            for title_key in matching_title_keys
        ):
            candidates.append(EpisodeKey("special", target_episode))
    return sorted(set(candidates))


def _unique_official_episode_zero_candidate(
    titles: Mapping[tuple[int, int], Sequence[str]],
) -> list[EpisodeKey]:
    """Return one Season 00 entry explicitly titled as episode zero.

    This is stronger evidence than merely having one unused special.  It is
    used for bare release names such as ``Show [00].mkv`` only when TMDB's
    multilingual official titles themselves uniquely say Episode 0/第 0 话.
    Generic specials, prologues without a zero label, and ambiguous duplicate
    zero titles remain unresolved.
    """
    explicit_zero_title = re.compile(
        r"(?:episode\s*[:#-]?\s*0(?!\d)|第\s*0\s*[话話集](?!\d)|"
        r"第\s*0\s*话|第零[话話集])",
        re.I,
    )
    candidates = {
        EpisodeKey("special", target_episode)
        for (target_season, target_episode), localized_titles in titles.items()
        if target_season == 0
        and any(
            explicit_zero_title.search(unicodedata.normalize("NFKC", title))
            for title in localized_titles
        )
    }
    return sorted(candidates) if len(candidates) == 1 else []


def _unique_season_referenced_special_candidate(
    titles: Mapping[tuple[int, int], Sequence[str]],
    season: int,
) -> list[EpisodeKey]:
    """Return one special whose official title explicitly names the season."""
    season_ref = re.compile(
        rf"(?:第\s*0*{season}\s*季|\bseason\s*0*{season}\b|"
        rf"\b0*{season}(?:st|nd|rd|th)\s+season\b)",
        re.I,
    )
    candidates = {
        EpisodeKey("special", target_episode)
        for (target_season, target_episode), localized_titles in titles.items()
        if target_season == 0
        and any(
            season_ref.search(unicodedata.normalize("NFKC", title))
            for title in localized_titles
        )
    }
    return sorted(candidates) if len(candidates) == 1 else []


def _e00_timeline_candidates(
    tmdb_client: TMDBClient,
    tmdb_id: int,
    season: int,
) -> list[EpisodeKey]:
    """Find a unique full-length Episode 0 immediately preceding a season.

    This is a second, independent online-evidence path for sources named only
    ``00.mkv``.  A candidate must be the sole full-length Season 00 episode in
    the year before the official season premiere.  The wider window covers
    officially released Episode 0 previews that precede the broadcast season
    by roughly half a year. Recaps released during the season, old unrelated
    specials and short promos are excluded.
    """
    try:
        season_payload = tmdb_client.get(f"/tv/{tmdb_id}/season/{season}")
        specials_payload = tmdb_client.get(f"/tv/{tmdb_id}/season/0")
    except ApiError:
        return []
    regular_episodes = [
        item
        for item in (season_payload.get("episodes") or [])
        if isinstance(item, Mapping)
    ]
    regular_dates = sorted(
        datetime.fromisoformat(str(item["air_date"])).date()
        for item in regular_episodes
        if re.fullmatch(r"\d{4}-\d{2}-\d{2}", str(item.get("air_date") or ""))
    )
    regular_runtimes = sorted(
        int(item["runtime"])
        for item in regular_episodes
        if isinstance(item.get("runtime"), int)
        and not isinstance(item.get("runtime"), bool)
        and int(item["runtime"]) > 0
    )
    if not regular_dates or not regular_runtimes:
        return []
    premiere = regular_dates[0]
    median_runtime = regular_runtimes[len(regular_runtimes) // 2]
    candidates: list[EpisodeKey] = []
    recap_title = re.compile(
        r"(?:总集篇|總集篇|総集編|総集篇|recap|digest|compilation|summary)",
        re.IGNORECASE,
    )
    for item in specials_payload.get("episodes") or []:
        if (
            not isinstance(item, Mapping)
            or not isinstance(item.get("episode_number"), int)
            or isinstance(item.get("episode_number"), bool)
            or not re.fullmatch(
                r"\d{4}-\d{2}-\d{2}",
                str(item.get("air_date") or ""),
            )
            or not isinstance(item.get("runtime"), int)
            or isinstance(item.get("runtime"), bool)
        ):
            continue
        # TMDB occasionally carries recap broadcasts with placeholder or
        # incorrect early dates. An explicit recap title is negative evidence
        # and must never compete with an original Episode 0/prologue.
        if recap_title.search(str(item.get("name") or "")):
            continue
        air_date = datetime.fromisoformat(str(item["air_date"])).date()
        runtime = int(item["runtime"])
        days_before = (premiere - air_date).days
        if (
            0 <= days_before <= 365
            and 0.75 * median_runtime <= runtime <= 1.5 * median_runtime
        ):
            candidates.append(EpisodeKey("special", int(item["episode_number"])))
    return sorted(set(candidates))


def _map_unique_remaining_unnumbered_special_by_runtime(
    tmdb_client: TMDBClient,
    tmdb_id: int,
    files: Sequence[Mapping[str, Any]],
    groups: dict[EpisodeKey, list[dict[str, Any]]],
    special_titles: Mapping[EpisodeKey, str],
) -> list[str]:
    """Map one unnumbered special only with independent runtime evidence.

    A sole unused TMDB Season 00 slot is never enough. The source must contain
    exactly one still-unmapped video explicitly labelled as a special, an
    already identified special from the same release must provide a size
    anchor, and the source-size ratio must agree with TMDB's official runtime
    ratio. This lets a generic ``Special.mkv`` be resolved after an evidenced
    ``E00`` while leaving multiple extras or weak metadata for diagnostics.
    """
    recognized = {
        _collision_key(str(item.get("full_path", "")))
        for items in groups.values()
        for item in items
    }
    pending_videos = [
        dict(item)
        for item in _filter_media(files)
        if Path(str(item.get("name", ""))).suffix.lower() in VIDEO_EXTS
        and _collision_key(str(item.get("full_path", ""))) not in recognized
        and _has_special_context(item)
    ]
    if len(pending_videos) != 1:
        return []

    used_specials = {key for key in groups if key.kind == "special"}
    remaining = sorted(set(special_titles) - used_specials)
    if len(remaining) != 1:
        return []
    target_key = remaining[0]

    try:
        payload = tmdb_client.get(f"/tv/{tmdb_id}/season/0")
    except ApiError:
        return []
    runtimes = {
        EpisodeKey("special", int(item["episode_number"])): int(item["runtime"])
        for item in (payload.get("episodes") or [])
        if isinstance(item, Mapping)
        and isinstance(item.get("episode_number"), int)
        and not isinstance(item.get("episode_number"), bool)
        and isinstance(item.get("runtime"), int)
        and not isinstance(item.get("runtime"), bool)
        and int(item["runtime"]) > 0
    }
    target_runtime = runtimes.get(target_key, 0)
    target_size = int(pending_videos[0].get("size") or 0)
    if target_runtime <= 0 or target_size <= 0:
        return []

    matching_anchors: list[EpisodeKey] = []
    for anchor_key in sorted(used_specials):
        anchor_runtime = runtimes.get(anchor_key, 0)
        anchor_sizes = sorted(
            int(item.get("size") or 0)
            for item in groups.get(anchor_key, ())
            if Path(str(item.get("name", ""))).suffix.lower() in VIDEO_EXTS
            and int(item.get("size") or 0) > 0
        )
        if anchor_runtime <= 0 or not anchor_sizes:
            continue
        anchor_size = anchor_sizes[len(anchor_sizes) // 2]
        source_ratio = target_size / anchor_size
        official_ratio = target_runtime / anchor_runtime
        # Container/codec variation makes byte ratios approximate, but a
        # 33% margin still rejects ordinary same-length episodes, trailers and
        # unrelated extras when the official item is a double-length special.
        if 0.67 <= source_ratio / official_ratio <= 1.33:
            matching_anchors.append(anchor_key)
    if not matching_anchors:
        return []

    groups.setdefault(target_key, []).append(pending_videos[0])
    return [
        "已在线检索 TMDB Season 00：未编号 SP 只剩一个官方候选，且其"
        f"文件大小比例与已确认 {matching_anchors[0].display} 的官方时长比例一致；"
        f"唯一确认映射为 {target_key.display}"
    ]


def _map_unnumbered_special_from_subtitle_title(
    alist: AListClient,
    files: Sequence[Mapping[str, Any]],
    groups: dict[EpisodeKey, list[dict[str, Any]]],
    special_titles: Mapping[EpisodeKey, str],
    *,
    series_titles: Sequence[str],
    regular_episode_count: int,
    tmdb_client: TMDBClient | None = None,
    tmdb_id: int | None = None,
    season: int | None = None,
) -> list[str]:
    """Resolve one generic ``SP`` from bounded ASS/SSA script metadata.

    Some releases deliberately name the bonus video and its subtitle only
    ``[SP]`` while the subtitle's ``[Script Info]`` title retains the official
    release ordinal (for example ``Steins;Gate 25``).  A sole unused Season 00
    row is not enough evidence, so this path additionally requires:

    * exactly one unresolved SP-labelled video and subtitle;
    * a Script Info title naming the same work and the next episode ordinal;
    * exactly one still-unused official Season 00 episode.

    Only the small subtitle file is read.  Dialogue text is intentionally not
    used as identity evidence, and any ambiguity leaves both files untouched.
    """
    recognized = {
        _collision_key(str(item.get("full_path", "")))
        for items in groups.values()
        for item in items
    }
    pending = [
        dict(item)
        for item in _filter_media(files)
        if _collision_key(str(item.get("full_path", ""))) not in recognized
        and _has_special_context(item)
    ]
    videos = [
        item for item in pending
        if Path(str(item.get("name", ""))).suffix.lower() in VIDEO_EXTS
    ]
    subtitles = [
        item for item in pending
        if Path(str(item.get("name", ""))).suffix.lower() in SUBTITLE_EXTS
    ]
    if len(videos) != 1 or len(subtitles) != 1 or regular_episode_count <= 0:
        return []
    remaining = sorted(set(special_titles) - {key for key in groups if key.kind == "special"})
    subtitle = subtitles[0]
    size = int(subtitle.get("size") or 0)
    if size <= 0 or size > 2 * 1024 * 1024:
        return []
    reader = getattr(alist, "read_file_bytes", None)
    if not callable(reader):
        return []
    try:
        payload = reader(
            str(subtitle.get("full_path", "")),
            max_bytes=2 * 1024 * 1024,
        )
    except (ApiError, OSError, ValueError):
        return []
    text = ""
    for encoding in ("utf-8-sig", "utf-16", "utf-16-le", "gb18030"):
        try:
            text = payload.decode(encoding)
            break
        except UnicodeError:
            continue
    if not text:
        return []
    script_info = re.split(r"^\s*\[(?!Script Info\])", text, maxsplit=1, flags=re.MULTILINE)[0]
    title_match = re.search(r"^\s*Title\s*:\s*(.+?)\s*$", script_info, re.MULTILINE | re.IGNORECASE)
    if title_match is None:
        return []
    metadata_title = unicodedata.normalize("NFKC", title_match.group(1)).strip()
    ordinals = [int(value) for value in re.findall(r"(?<!\d)(\d{1,4})(?!\d)", metadata_title)]
    if ordinals != [regular_episode_count + 1]:
        return []
    metadata_key = _normalize_match_title(
        re.sub(r"(?<!\d)\d{1,4}(?!\d)|\b(?:gb|chs|cht|sc|tc)\b", " ", metadata_title, flags=re.IGNORECASE)
    )
    title_keys = {
        _normalize_match_title(value)
        for value in series_titles
        if isinstance(value, str) and value.strip()
    }
    title_keys.discard("")
    if not metadata_key or not any(
        metadata_key == title_key
        or (
            min(len(metadata_key), len(title_key)) >= 6
            and (metadata_key in title_key or title_key in metadata_key)
        )
        for title_key in title_keys
    ):
        return []
    evidence = "官方 Season 00 只剩"
    if len(remaining) != 1:
        if (
            tmdb_client is None
            or not isinstance(tmdb_id, int)
            or isinstance(tmdb_id, bool)
            or not isinstance(season, int)
            or isinstance(season, bool)
        ):
            return []
        try:
            regular_payload = tmdb_client.get(f"/tv/{tmdb_id}/season/{season}")
            special_payload = tmdb_client.get(f"/tv/{tmdb_id}/season/0")
        except ApiError:
            return []
        regular_rows = [
            item for item in (regular_payload.get("episodes") or [])
            if isinstance(item, Mapping)
        ]
        dates = [
            datetime.fromisoformat(str(item["air_date"])).date()
            for item in regular_rows
            if re.fullmatch(r"\d{4}-\d{2}-\d{2}", str(item.get("air_date") or ""))
        ]
        runtimes = sorted(
            int(item["runtime"])
            for item in regular_rows
            if isinstance(item.get("runtime"), int)
            and not isinstance(item.get("runtime"), bool)
            and int(item["runtime"]) > 0
        )
        if not dates or not runtimes:
            return []
        finale = max(dates)
        median_runtime = runtimes[len(runtimes) // 2]
        timeline_candidates: list[EpisodeKey] = []
        for item in special_payload.get("episodes") or []:
            if (
                not isinstance(item, Mapping)
                or not isinstance(item.get("episode_number"), int)
                or isinstance(item.get("episode_number"), bool)
                or not isinstance(item.get("runtime"), int)
                or isinstance(item.get("runtime"), bool)
                or not re.fullmatch(
                    r"\d{4}-\d{2}-\d{2}", str(item.get("air_date") or "")
                )
            ):
                continue
            key = EpisodeKey("special", int(item["episode_number"]))
            days_after = (
                datetime.fromisoformat(str(item["air_date"])).date() - finale
            ).days
            runtime = int(item["runtime"])
            if (
                key in special_titles
                and key not in groups
                and 0 <= days_after <= 365
                and 0.75 * median_runtime <= runtime <= 1.5 * median_runtime
            ):
                timeline_candidates.append(key)
        if len(set(timeline_candidates)) != 1:
            return []
        remaining = [timeline_candidates[0]]
        evidence = "官方季终后一年内且与正片等时长的特别篇只有"
    target = remaining[0]
    groups.setdefault(target, []).extend([videos[0], subtitle])
    return [
        "已有界读取未编号 SP 外挂字幕的 Script Info：内部标题"
        f"明确标记为正片 {regular_episode_count} 集后的第 "
        f"{regular_episode_count + 1} 话，且{evidence} "
        f"{target.display}；已唯一确认映射"
    ]


def _e00_movie_queries(item: Mapping[str, Any]) -> list[str]:
    """Build title queries for an E00 that may actually be a standalone work.

    Release groups use E00 for both genuine television prologues and separately
    catalogued pilot movies. The latter often carries a subtitle after the
    episode token (or in its own bracket), so preserve that subtitle while
    removing only numbering and encode noise.
    """
    raw_stem = unicodedata.normalize(
        "NFKC",
        Path(str(item.get("name", ""))).stem,
    )
    bracket_titles = [
        value.strip()
        for value in re.findall(r"\[([^\]]+)\]", raw_stem)
        if re.search(r"[A-Za-z\u3400-\u9fff\u3040-\u30ff]", value)
        and not re.search(
            r"(?:\b(?:AVC|HEVC|AAC|FLAC|WEB|BD|BluRay|CHS|CHT|JPN|"
            r"1080P|2160P|720P|4K|8BIT|10BIT)\b|字幕组|字幕社)",
            value,
            re.IGNORECASE,
        )
    ]
    queries = list(_movie_queries_from_item(item))
    # A release such as ``[Group] Series [00][Movie Subtitle]`` is best
    # searched by combining the clean series query with the specific subtitle.
    for left in list(queries):
        for right in bracket_titles:
            if left != right:
                queries.append(f"{left} {right}")
    cleaned = re.sub(
        r"(?<![A-Za-z0-9])S\s*0*\d+\s*E\s*0+(?!\d)|"
        r"(?<![A-Za-z0-9])(?:E|EP)\s*0+(?!\d)",
        " ",
        raw_stem,
        flags=re.IGNORECASE,
    )
    cleaned = re.sub(r"[\[\](){}]", " ", cleaned)
    cleaned = re.sub(
        r"\b(?:AVC|HEVC|AAC|FLAC|WEB(?:-?DL)?|BluRay|BD|CR|x26[45]|"
        r"1080P|2160P|720P|4K|8BIT|10BIT|CHS|CHT|JPN|MP4|MKV)\b.*$",
        " ",
        cleaned,
        flags=re.IGNORECASE,
    )
    cleaned = re.sub(r"[._]+|\s+", " ", cleaned).strip(" -")
    if cleaned:
        queries.append(cleaned)
    return list(dict.fromkeys(query for query in queries if query))


def _e00_independent_movie_match(
    tmdb_client: TMDBClient,
    items: Sequence[Mapping[str, Any]],
    show: Mapping[str, Any],
) -> AutoMatch | None:
    """Resolve E00 as a movie only when every release version proves one work.

    A sole unfilled special slot is deliberately ignored. Each E00 video must
    instead contain a title longer than the enclosing TV title and independently
    resolve, at high confidence, to the same TMDB movie. This is suitable for
    separately catalogued pilots while leaving generic ``00.mkv`` for diagnostics.
    """
    videos = [
        item
        for item in items
        if Path(str(item.get("name", ""))).suffix.lower() in VIDEO_EXTS
    ]
    if not videos:
        return None
    show_keys = {
        _normalize_match_title(str(show.get(field) or ""))
        for field in ("name", "original_name")
    }
    show_keys.discard("")
    resolved: list[AutoMatch] = []
    for item in videos:
        item_matches: dict[int, AutoMatch] = {}
        for query in _e00_movie_queries(item):
            evidence_query = re.sub(
                r"(?<![A-Za-z0-9])S\s*0*\d+\s*E\s*0+(?!\d)|"
                r"(?<![A-Za-z0-9])(?:E|EP)\s*0+(?!\d)",
                " ",
                unicodedata.normalize("NFKC", query),
                flags=re.IGNORECASE,
            )
            evidence_query = re.sub(
                r"\b(?:19|20)\d{2}\b|"
                r"\b(?:AVC|HEVC|AAC|FLAC|WEB(?:-?DL)?|BluRay|BD|CR|x26[45]|"
                r"1080P|2160P|720P|4K|8BIT|10BIT|CHS|CHT|JPN|MP4|MKV)\b",
                " ",
                evidence_query,
                flags=re.IGNORECASE,
            )
            query_key = _normalize_match_title(evidence_query)
            if not query_key:
                continue
            # The query must add a real work subtitle, not merely repeat the
            # enclosing series name with E00/codec tokens.
            if show_keys and not any(
                show_key in query_key and len(query_key.replace(show_key, "")) >= 4
                for show_key in show_keys
            ):
                continue
            try:
                match, _ = auto_match_tmdb(
                    tmdb_client,
                    query,
                    media_type="movie",
                    min_confidence=0.96,
                    prefer_animation=True,
                )
                if match.status != "confirmed":
                    continue
            except ScraperError:
                continue
            if match.media_type == "movie":
                item_matches[match.tmdb_id] = match
        if len(item_matches) != 1:
            return None
        resolved.append(next(iter(item_matches.values())))
    tmdb_ids = {item.tmdb_id for item in resolved}
    return resolved[0] if len(tmdb_ids) == 1 else None


def _remap_numbered_specials_by_official_label(
    groups: dict[EpisodeKey, list[dict[str, Any]]],
    special_titles: Mapping[EpisodeKey, str],
) -> list[str]:
    """Prefer a distinctive official label over a release's reset SP number.

    Disc releases commonly restart at ``SP01`` for every season while also
    carrying the franchise-wide label (for example MMR03). TMDB numbers the
    latter globally in Season 00. Matching the shared label prevents SP01 from
    three seasons collapsing onto the same S00E01 target.
    """
    warnings: list[str] = []
    official_tokens = {
        key: _special_label_tokens(title)
        for key, title in special_titles.items()
    }
    official_context = {
        key: _special_context_tokens(title)
        for key, title in special_titles.items()
    }

    # ``parse_ep_files`` initially groups by the release-local number.  Two
    # different mini-series can therefore collapse into one SP03 bucket before
    # the official-label remapper sees them.  Split that bucket only when every
    # member independently resolves: a distinctive shared title word wins; an
    # otherwise ambiguous member may remain at its literal source number.  This
    # keeps an original ``OVA03`` separate from ``Darkness OVA03`` without
    # guessing when neither file carries sub-series evidence.
    for source_key in sorted(list(groups)):
        if source_key.kind != "special":
            continue
        source_items = list(groups[source_key])
        if sum(
            Path(str(item.get("name", ""))).suffix.lower() in VIDEO_EXTS
            for item in source_items
        ) < 2:
            continue
        source_contexts = [
            _special_context_tokens(
                f"{item.get('name', '')} {item.get('full_path', '')}"
            )
            for item in source_items
        ]
        common_source_context = (
            set.intersection(*(set(tokens) for tokens in source_contexts))
            if source_contexts
            else set()
        )
        resolved: list[tuple[dict[str, Any], EpisodeKey]] = []
        for item, raw_context in zip(source_items, source_contexts):
            text = f"{item.get('name', '')} {item.get('full_path', '')}"
            labels = _special_label_tokens(text)
            candidates = sorted(
                key for key, tokens in official_tokens.items() if labels & tokens
            )
            if not candidates:
                resolved = []
                break
            # Franchise/release-group words shared by every item in the
            # collapsed bucket are not sub-series evidence.  For example,
            # ``To Love-Ru Trouble`` appears on both releases while only the
            # later one adds ``Darkness``.
            context = raw_context - common_source_context
            scores = {
                key: len(context & official_context[key]) for key in candidates
            }
            best_score = max(scores.values(), default=0)
            best = [key for key, score in scores.items() if score == best_score]
            if best_score > 0 and len(best) == 1:
                target_key = best[0]
            elif source_key in candidates:
                target_key = source_key
            else:
                resolved = []
                break
            resolved.append((item, target_key))
        video_targets = {
            target_key
            for item, target_key in resolved
            if Path(str(item.get("name", ""))).suffix.lower() in VIDEO_EXTS
        }
        if len(resolved) != len(source_items) or len(video_targets) < 2:
            continue
        groups.pop(source_key)
        for item, target_key in resolved:
            item["_episode_kind_override"] = target_key.kind
            item["_episode_key_override"] = target_key.number
            groups.setdefault(target_key, []).append(item)
            if target_key != source_key:
                warnings.append(
                    f"根据文件中的子系列标题将 {source_key.display} 映射为 {target_key.display}"
                )

    original_specials = [
        (key, list(groups[key]))
        for key in sorted(groups)
        if key.kind == "special"
    ]
    video_routes: dict[EpisodeKey, EpisodeKey] = {}
    subtitle_routes: dict[EpisodeKey, list[tuple[dict[str, Any], EpisodeKey]]] = {}
    used_targets: set[EpisodeKey] = set()
    for source_key, source_items in original_specials:
        video_items = [
            item for item in source_items
            if Path(str(item.get("name", ""))).suffix.lower() in VIDEO_EXTS
        ]
        item_matches: list[tuple[dict[str, Any], list[EpisodeKey]]] = []
        for item in source_items:
            tokens = _special_label_tokens(
                f"{item.get('name', '')} {item.get('full_path', '')}"
            )
            item_matches.append(
                (
                    item,
                    [key for key, official in official_tokens.items() if tokens & official],
                )
            )

        # A video and its companion subtitles stay together.  Prefer the video
        # label, but if that label is a duplicate typo, use the only still-free
        # official label mentioned by another member of the same SP group.
        if video_items:
            video_candidates = {
                key
                for item, matches in item_matches
                if item in video_items
                for key in matches
            }
            all_candidates = {
                key for _item, matches in item_matches for key in matches
            }
            available_video = sorted(video_candidates - used_targets)
            available_all = sorted(all_candidates - used_targets)
            target_key = (
                available_video[0]
                if len(available_video) == 1
                else available_all[0]
                if len(available_all) == 1
                else None
            )
            if target_key is None:
                continue
            used_targets.add(target_key)
            video_routes[source_key] = target_key
            continue

        # Subtitle-only extras may share a reset SP number.  They can still be
        # separated safely when every file carries one unique official label.
        resolved = [
            (item, matches[0])
            for item, matches in item_matches
            if len(matches) == 1
        ]
        if resolved and len(resolved) == len(source_items):
            subtitle_routes[source_key] = resolved
            continue

    # Apply the complete route table atomically.  Sequentially deleting and
    # inserting dictionary keys corrupts shifted mappings such as
    # SP01→SP02, SP02→SP03, …: the second move consumes the first move's files.
    # Rebuilding from the immutable snapshot preserves every source group.
    occupied_sources = {key for key, _items in original_specials}
    accepted_video_routes = {
        source_key: target_key
        for source_key, target_key in video_routes.items()
        if (
            target_key == source_key
            or target_key not in occupied_sources
            or (
                target_key in video_routes
                and video_routes[target_key] != target_key
            )
        )
    }
    for source_key, _source_items in original_specials:
        groups.pop(source_key, None)
    for source_key, source_items in original_specials:
        if source_key in accepted_video_routes:
            target_key = accepted_video_routes[source_key]
            for item in source_items:
                item["_episode_kind_override"] = target_key.kind
                item["_episode_key_override"] = target_key.number
            groups.setdefault(target_key, []).extend(source_items)
            if target_key != source_key:
                warnings.append(
                    f"根据文件中的官方特别篇标签将 {source_key.display} 映射为 {target_key.display}"
                )
            continue
        if source_key in subtitle_routes:
            for item, target_key in subtitle_routes[source_key]:
                item["_episode_kind_override"] = target_key.kind
                item["_episode_key_override"] = target_key.number
                groups.setdefault(target_key, []).append(item)
                if target_key != source_key:
                    warnings.append(
                        f"根据文件中的官方特别篇标签将 {source_key.display} 映射为 {target_key.display}"
                    )
            continue
        groups.setdefault(source_key, []).extend(source_items)

    return list(dict.fromkeys(warnings))


def _remap_postseason_oav_suffix(
    groups: dict[EpisodeKey, list[dict[str, Any]]],
    special_titles: Mapping[EpisodeKey, str],
) -> list[str]:
    """Resolve ``[13 OAV]`` only from a complete season boundary.

    Some releases use ``N OAV`` to mean the OAV shipped after ordinary
    episode N, not OAV number N.  Accept that interpretation only when regular
    videos completely cover E01–EN and TMDB exposes exactly one official
    special.  Prefix forms such as ``OAV13`` and incomplete seasons remain
    unresolved.
    """
    regular_numbers = {
        key.number
        for key, items in groups.items()
        if key.kind == "regular"
        and not key.end_number
        and any(
            Path(str(item.get("name", ""))).suffix.lower() in VIDEO_EXTS
            for item in items
        )
    }
    if not regular_numbers or regular_numbers != set(range(1, max(regular_numbers) + 1)):
        return []
    boundary = max(regular_numbers)
    source_key = EpisodeKey("special", boundary)
    source_items = groups.get(source_key)
    if not source_items or len(special_titles) != 1:
        return []
    suffix_marker = re.compile(
        rf"(?:^|[\s._\-\[()])0*{boundary}[ ._-]+OAV(?:$|[\s._\-\])()])",
        re.IGNORECASE,
    )
    if not all(
        suffix_marker.search(str(item.get("name", "")))
        for item in source_items
    ):
        return []
    target_key = next(iter(special_titles))
    if target_key in groups and target_key != source_key:
        return []
    for item in source_items:
        item["_episode_kind_override"] = "special"
        item["_episode_key_override"] = target_key.number
    groups.pop(source_key)
    groups.setdefault(target_key, []).extend(source_items)
    return [
        f"根据完整 E01–E{boundary:02d} 正片边界、后置 OAV 标记和 TMDB 唯一官方"
        f"特别篇，将 {source_key.display} 映射为 {target_key.display}"
    ]


def _remap_suffix_oav_on_air_versions(
    groups: dict[EpisodeKey, list[dict[str, Any]]],
    official_season_counts: Mapping[int, int],
) -> list[str]:
    """Disambiguate release shorthand ``[N OAV]`` as On-Air Version.

    ``OAV`` is overloaded in release names.  It can mean Original Animation
    Video, but some encodes use it for On-Air Version.  Treat it as the latter
    only when TMDB says an ordinary season ends at N, a plain N video exists,
    and the release also contains N+1 as the separately numbered extra.  The
    complete three-way boundary is required; a lone OAV marker is never enough.
    """
    warnings: list[str] = []
    official_boundaries = set(official_season_counts.values())
    for boundary in sorted(official_boundaries):
        source_key = EpisodeKey("special", boundary)
        target_key = EpisodeKey("regular", boundary)
        next_key = EpisodeKey("regular", boundary + 1)
        source_items = groups.get(source_key)
        if not source_items or target_key not in groups or next_key not in groups:
            continue
        suffix_marker = re.compile(
            rf"(?:^|[\s._\-\[()])0*{boundary}[ ._-]+OAV(?:$|[\s._\-\])()])",
            re.IGNORECASE,
        )
        if not all(suffix_marker.search(str(item.get("name", ""))) for item in source_items):
            continue
        if not any(
            Path(str(item.get("name", ""))).suffix.lower() in VIDEO_EXTS
            for item in groups[target_key]
        ) or not any(
            Path(str(item.get("name", ""))).suffix.lower() in VIDEO_EXTS
            for item in groups[next_key]
        ):
            continue
        groups.pop(source_key)
        for item in source_items:
            item["_episode_kind_override"] = "regular"
            item["_episode_key_override"] = boundary
            item["_edition_override"] = "On-Air Version"
        groups[target_key].extend(source_items)
        warnings.append(
            f"根据 TMDB 正片边界 E{boundary:02d}、同集普通版本及独立 E{boundary + 1:02d}，"
            f"将后置 OAV 解释为 On-Air Version，并归入 E{boundary:02d} 版本比较"
        )
    return warnings


def _has_special_context(item: Mapping[str, Any]) -> bool:
    text = unicodedata.normalize(
        "NFKC",
        f"{item.get('name', '')} {item.get('full_path', '')}",
    )
    key = extract_episode_key(str(item.get("name", "")))
    if key is not None and key.kind == "special":
        return True
    # Generic special markers stay inline; concrete release spellings come
    # from the release lexicon (data, not business logic).
    release_tokens = "|".join(SPECIAL_CONTEXT_RELEASE_TOKENS)
    return bool(
        re.search(
            r"(?:特别篇|特典|番外|specials?|ovbsp|ova|oav|oad|"
            r"ex[ ._-]*season|special[ ._-]*season|"
            r"break[ ._-]*time|休息时间|休憩時間|小剧场|小劇場|petit|ぷち|"
            r"(?:^|[/\\])SPs?(?:[/\\]|$)"
            + ("|" + release_tokens if release_tokens else "")
            + r")",
            text,
            re.IGNORECASE,
        )
    )


def _physical_special_markers(items: Sequence[Mapping[str, Any]]) -> set[str]:
    """Return explicit physical-release markers without inferring placement."""
    markers: set[str] = set()
    for item in items:
        text = unicodedata.normalize(
            "NFKC",
            f"{item.get('name', '')} {item.get('full_path', '')}",
        ).upper()
        markers.update(
            re.findall(r"(?<![A-Z])(?:OVA|OAV|OAD)(?![A-Z])", text)
        )
    return markers


def _numbered_physical_special_markers(
    items: Sequence[Mapping[str, Any]],
) -> set[str]:
    """Return OVA/OAV/OAD markers directly attached to a release ordinal.

    ``OAD 02``, ``[13 OAV]`` and ``OAD Series [01]`` number the
    physical-release order, which is release-local and never by itself equals
    a TMDB Season 00 index.  A marker without an attached number
    (``OVA.mkv``) is resolved by the title/runtime/timeline evidence paths
    instead, so it must not trigger the number-identity threshold.  Years are
    excluded (``OAD 2016`` keeps its dated-run evidence path), and a marker
    followed by an ordinal word (``OVA 2nd Season``) is a season label, not a
    release ordinal.
    """
    markers: set[str] = set()
    for item in items:
        text = unicodedata.normalize(
            "NFKC",
            f"{item.get('name', '')} {item.get('full_path', '')}",
        ).upper()
        for marker, number in re.findall(
            r"(?<![A-Z])(OVA|OAV|OAD)[\s._-]*(?:SERIES|系列)?"
            r"[\s._-]*0*(\d{1,3})(?!(?:st|nd|rd|th)|\d)",
            text,
        ):
            if 0 < int(number) <= 999:
                markers.add(f"{marker}{int(number)}")
        for number, marker in re.findall(
            r"(?<![A-Za-z0-9])0*(\d{1,3})[\s._-]*(OVA|OAV|OAD)(?!\d)",
            text,
        ):
            if 0 < int(number) <= 999:
                markers.add(f"{marker}{int(number)}")
    return markers


def _oav_numbered_special_has_official_evidence(
    key: EpisodeKey,
    items: Sequence[Mapping[str, Any]],
    *,
    season: int,
    special_titles: Mapping[EpisodeKey, str],
    special_season_candidates: Mapping[int, Sequence[EpisodeKey]] | None,
) -> bool:
    """Whether an OVA/OAV/OAD-numbered special is backed by official evidence.

    OAD/OVA/OAV numbering is release-local, so the bare number match against
    a TMDB Season 00 index is never identity evidence.  Mapping is allowed
    only when one of these official Season 00 evidence classes holds:

    * an evidence helper already remapped the group by official label,
      complete season boundary or dated release run (every video item then
      carries the official target number override);
    * TMDB's official air-date timeline places this Season 00 episode inside
      the current season's window (work-relationship evidence);
    * TMDB's official Season 00 title itself names this physical-release
      ordinal (for example an official ``OVA 2``).
    """
    videos = [
        item
        for item in items
        if Path(str(item.get("name", ""))).suffix.lower() in VIDEO_EXTS
    ]
    if videos and all(
        item.get("_episode_kind_override") == "special"
        and item.get("_episode_key_override") == key.number
        for item in videos
    ):
        return True
    if special_season_candidates is not None and key in special_season_candidates.get(
        season, ()
    ):
        return True
    official_title = special_titles.get(key)
    if not official_title:
        return False
    return bool(
        re.search(
            rf"(?<![A-Z])(?:OVA|OAV|OAD)\D*(?<!\d)0*{key.number}(?!\d)",
            unicodedata.normalize("NFKC", official_title).upper(),
        )
    )


def _special_context_overrides_parent_season(item: Mapping[str, Any]) -> bool:
    """Whether explicit inner context must override an outer TV-season folder."""
    text = f"{item.get('name', '')} {item.get('full_path', '')}"
    parent = str(item.get("full_path", "")).rsplit("/", 1)[0]
    return bool(
        re.search(
            r"(?:break[ ._-]*time|休息时间|休憩時間|小剧场|小劇場|petit|ぷち|"
            r"课外授业篇|課外授業編|kagai[ ._-]*jugy[oō][ ._-]*hen|"
            r"(?:^|[/\\])SPs?(?:[/\\]|$))",
            text,
            re.IGNORECASE,
        )
        or re.search(
            r"(?:^|[/\\])(?:OVA|OAV|OAD)(?:[ ._-]|[/\\]|$)",
            parent,
            re.IGNORECASE,
        )
    )


def _special_series_ordinal(value: str) -> int | None:
    """Return an explicit mini-series generation such as ``2nd season``.

    A bare number is intentionally ignored: it may be an episode, a story arc
    or part of the title.  Only an ordinal attached to ``season/series`` (or an
    equivalent CJK marker) is acceptable evidence.
    """
    normalized = unicodedata.normalize("NFKC", value)
    patterns = (
        r"(?<!\d)(\d{1,2})(?:st|nd|rd|th)\s*(?:season|series)\b",
        r"\b(?:season|series)\s*(\d{1,2})(?!\d)",
        r"第\s*(\d{1,2})\s*(?:季|期)",
    )
    for pattern in patterns:
        match = re.search(pattern, normalized, re.IGNORECASE)
        if match:
            number = int(match.group(1))
            if 0 < number <= 99:
                return number
    return None


def _ova_volume_ordinal(value: str) -> int | None:
    """Return an explicit OVA/physical-volume ordinal from a directory label."""
    normalized = unicodedata.normalize("NFKC", value)
    patterns = (
        r"(?:^|[\s._\-\[(])(?:OVA|OAV)[\s._-]*0*(\d{1,3})(?=$|[\s._\-\])（(])",
        r"(?:^|[\s._\-\[(])VOL(?:UME)?[\s._-]*0*(\d{1,3})(?=$|[\s._\-\])（(])",
        r"(?:^|[\s._\-\[(])(\d{1,2})(?:ST|ND|RD|TH)[\s._-]*(?:SEASON|SERIES)(?=$|[\s._\-\])（(])",
    )
    for pattern in patterns:
        match = re.search(pattern, normalized, re.IGNORECASE)
        if match and 0 < int(match.group(1)) <= 999:
            return int(match.group(1))
    labels = (
        (1, r"(?:上卷|前篇|前编|前編)"),
        (2, r"(?:下卷|后篇|後篇|后编|後編)"),
    )
    matched = [number for number, pattern in labels if re.search(pattern, normalized)]
    return matched[0] if len(matched) == 1 else None


def _special_release_latin_tokens(value: str) -> set[str]:
    """Extract stable Latin title words while discarding release metadata."""
    normalized = unicodedata.normalize("NFKC", value).casefold()
    ignored = {
        "season", "series", "special", "specials", "episode", "episodes",
        "the", "from", "with", "and", "part", "version", "video", "subtitle",
        "truehd", "flac", "bluray", "webrip", "bdrip", "ma10p", "x264",
        "x265", "hevc", "av1", "ass", "mkv", "mp4", "backup",
    }
    return {
        token
        for token in re.findall(r"[a-z]{3,}", normalized)
        if token not in ignored
    }


def _named_special_context_matches(source_key: str, title: str) -> bool:
    title_key = _normalize_match_title(title)
    if len(source_key) < 6 or len(title_key) < 6:
        return False
    blocks = difflib.SequenceMatcher(None, source_key, title_key).get_matching_blocks()
    longest_block = max(blocks, key=lambda block: block.size, default=None)
    longest = longest_block.size if longest_block is not None else 0
    # Sharing only the franchise name is insufficient.  The named child label
    # must substantially overlap the official special title.
    if longest >= 6 and longest / min(len(source_key), len(title_key)) >= 0.65:
        return True
    # A five-character CJK arc label ending in an explicit narrative-unit
    # suffix is also distinctive when it occurs verbatim in both the source
    # directory and an official special title.  This covers labels such as
    # “课外授业篇” without treating a shared franchise name as evidence.
    shared = (
        source_key[longest_block.a : longest_block.a + longest]
        if longest_block is not None
        else ""
    )
    return longest >= 5 and bool(re.search(r"(?:篇|編|编|章|物语|物語)$", shared))


def _special_release_source_ordinal(item: Mapping[str, Any]) -> int | None:
    """Return a physical episode ordinal without mistaking its year for one.

    Releases such as ``OAD 2016 [01]`` deliberately place the release year
    between the OAD marker and the actual episode bracket.  The general
    episode parser must treat bare OAD as unnumbered, so this narrowly scoped
    parser accepts the final pure-numeric bracket only when the same item also
    carries an explicit OVA/OAV/OAD marker.
    """
    name = unicodedata.normalize("NFKC", str(item.get("name", "")))
    key = extract_episode_key(name)
    if key is not None and key.kind in {"regular", "special"} and key.number > 0:
        return key.number
    if not re.search(r"(?<![A-Z])(?:OVA|OAV|OAD)(?![A-Z])", name, re.IGNORECASE):
        return None
    bracket_numbers = {
        int(match.group(1))
        for match in re.finditer(r"\[\s*0*(\d{1,3})\s*\]", name)
        if 0 < int(match.group(1)) <= 999
    }
    return next(iter(bracket_numbers)) if len(bracket_numbers) == 1 else None


def _special_arc_title_key(value: str) -> str:
    """Normalize one official arc title while retaining its identity words."""
    normalized = unicodedata.normalize("NFKC", value).strip()
    part_suffix = re.compile(
        r"(?:\s*[-:,，：]?\s*(?:"
        r"前篇|後篇|后篇|上篇|下篇|前編|後編|后编|"
        r"第\s*[12一二]\s*(?:话|話|集)|"
        r"part\s*(?:1|2|one|two|i|ii)|episode\s*(?:1|2)"
        r"))\s*$",
        re.IGNORECASE,
    )
    previous = None
    while normalized and normalized != previous:
        previous = normalized
        normalized = part_suffix.sub("", normalized).strip(" -,:，：")
    return _normalize_match_title(normalized)


def _unique_named_special_run(
    parent_label: str,
    *,
    run_length: int,
    source_year: int,
    official_title_variants: Mapping[int, Sequence[str]],
    official_air_dates: Mapping[int, str],
) -> tuple[int, ...] | None:
    """Return one strongly named, dated consecutive Season 00 run.

    Every episode must have a multilingual official title whose arc identity
    matches the source directory, every official air date must agree with the
    explicit source release year, and the best run must beat the runner-up by
    the global eight-point ambiguity margin.
    """
    parent_keys = {
        _normalize_match_title(parent_label),
        *(
            _normalize_match_title(query)
            for query in _franchise_member_queries("/" + parent_label)
        ),
    }
    parent_keys = {key for key in parent_keys if len(key) >= 5}
    if not parent_keys or run_length < 2:
        return None

    official_numbers = sorted(
        number
        for number in official_title_variants
        if isinstance(number, int) and not isinstance(number, bool) and number > 0
    )
    ranked: list[tuple[float, tuple[int, ...]]] = []
    official_set = set(official_numbers)
    for start in official_numbers:
        candidate = tuple(range(start, start + run_length))
        if not set(candidate).issubset(official_set):
            continue
        dates = [str(official_air_dates.get(number) or "") for number in candidate]
        if any(not re.fullmatch(r"\d{4}-\d{2}-\d{2}", value) for value in dates):
            continue
        if any(int(value[:4]) != source_year for value in dates):
            continue
        episode_scores: list[float] = []
        for number in candidate:
            variant_scores = [
                _title_similarity(parent_key, title_key)
                for parent_key in parent_keys
                for variant in official_title_variants.get(number, ())
                if (title_key := _special_arc_title_key(str(variant)))
            ]
            best = max(variant_scores, default=0.0)
            if best < 0.90:
                break
            episode_scores.append(best)
        if len(episode_scores) != run_length:
            continue
        # The weakest episode is the safety boundary; the mean only breaks
        # ties between otherwise fully qualifying runs.
        score = min(episode_scores) * 0.8 + (sum(episode_scores) / run_length) * 0.2
        ranked.append((score, candidate))
    ranked.sort(key=lambda row: (-row[0], row[1]))
    if not ranked:
        return None
    if len(ranked) > 1 and ranked[0][0] - ranked[1][0] < AUTO_MATCH_MIN_MARGIN:
        return None
    return ranked[0][1]


def _map_explicit_special_release_runs(
    items: list[dict[str, Any]],
    official_title_variants: Mapping[int, Sequence[str]],
    official_air_dates: Mapping[int, str] | None = None,
) -> list[str]:
    """Map a reset-numbered special mini-series only with converging evidence.

    Disc folders commonly place a named mini-series under ``SPs`` and restart
    its files at ``01``. A dated OAD/OVA run requires all of:

    * a distinctive directory matching every multilingual official arc title;
    * one explicit release year matching every official episode air date;
    * a complete consecutive source run matching one consecutive official run;
    * no runner-up official run inside the global ambiguity margin.

    The older explicit generation/named-arc routes remain available for
    undated releases, but a dated release cannot bypass these thresholds.
    """
    warnings: list[str] = []
    by_parent: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for item in items:
        parent, _ = split_remote(normalize_remote_path(str(item.get("full_path", "/"))))
        by_parent[parent].append(item)

    for parent, members in sorted(by_parent.items(), key=lambda row: _collision_key(row[0])):
        videos_by_number: dict[int, list[dict[str, Any]]] = defaultdict(list)
        for item in members:
            if Path(str(item.get("name", ""))).suffix.lower() not in VIDEO_EXTS:
                continue
            source_number = _special_release_source_ordinal(item)
            if source_number is not None:
                videos_by_number[source_number].append(item)
        video_source_numbers = sorted(videos_by_number)
        if (
            not video_source_numbers
            or video_source_numbers != list(range(1, len(video_source_numbers) + 1))
        ):
            continue
        candidate_run: tuple[int, ...] | None = None
        evidence: str | None = None
        text = " ".join(
            [split_remote(parent)[1], *(str(item.get("name", "")) for item in members)]
        )
        ordinal_values = {
            value
            for item in members
            if (value := _special_series_ordinal(
                f"{item.get('name', '')} {item.get('full_path', '')}"
            )) is not None
        }
        source_tokens = {
            token
            for item in members
            for token in _special_release_latin_tokens(str(item.get("name", "")))
        }
        if len(ordinal_values) == 1 and len(source_tokens) >= 2:
            ordinal = next(iter(ordinal_values))
            candidates = sorted({
                int(number)
                for number, variants in official_title_variants.items()
                if any(_special_series_ordinal(title) == ordinal for title in variants)
                and max(
                    (
                        len(source_tokens & _special_release_latin_tokens(title))
                        for title in variants
                    ),
                    default=0,
                ) >= 2
            })
            if (
                len(candidates) == len(video_source_numbers)
                and candidates == list(range(candidates[0], candidates[-1] + 1))
            ):
                candidate_run = tuple(candidates)
                evidence = f"特别篇系列标识（第 {ordinal} 代）、多语言官方标题"

        source_years = {
            int(value)
            for value in re.findall(r"(?<!\d)((?:19|20)\d{2})(?!\d)", text)
        }
        markers = _physical_special_markers(members)
        if (
            candidate_run is None
            and markers
            and len(source_years) == 1
            and official_air_dates is not None
        ):
            source_year = next(iter(source_years))
            parent_label = split_remote(parent)[1]
            candidate_run = _unique_named_special_run(
                parent_label,
                run_length=len(video_source_numbers),
                source_year=source_year,
                official_title_variants=official_title_variants,
                official_air_dates=official_air_dates,
            )
            if candidate_run is not None:
                evidence = (
                    "子目录名称与多语言官方特别篇标题的唯一连续匹配、"
                    f"{source_year} 年官方发行时间线"
                )
        # Preserve the older strongly named arc rule for undated releases.
        # Dated OAD/OVA runs deliberately cannot fall through here: they must
        # satisfy the stricter official-year/timeline and runner-up thresholds above.
        if candidate_run is None and not source_years:
            parent_key = _normalize_match_title(split_remote(parent)[1])
            parent_key = re.sub(r"^(?:剧中剧|劇中劇|作中作)", "", parent_key)
            parent_key = NORMALIZED_PARENT_ALIASES.get(parent_key, parent_key)
            candidates = sorted({
                int(number)
                for number, variants in official_title_variants.items()
                if len(parent_key) >= 6
                and any(
                    _named_special_context_matches(parent_key, title)
                    for title in variants
                )
            })
            if (
                len(candidates) == len(video_source_numbers)
                and candidates == list(range(candidates[0], candidates[-1] + 1))
            ):
                candidate_run = tuple(candidates)
                evidence = (
                    "子目录名称与多语言官方特别篇标题的唯一连续匹配、"
                    "多语言官方标题"
                )
        if candidate_run is None or evidence is None:
            continue
        mapped_count = 0
        for item in members:
            source_number = _special_release_source_ordinal(item)
            if (
                source_number is None
                or not 1 <= source_number <= len(candidate_run)
            ):
                continue
            item["_episode_kind_override"] = "special"
            item["_episode_key_override"] = candidate_run[source_number - 1]
            mapped_count += 1
        # Backup subtitle trees can repeat the named release directory under
        # a quality/group wrapper. Attach only subtitles whose own cleaned
        # parent independently resolves to this exact official run; another
        # video directory still needs its own complete-boundary match.
        for item in items:
            if (
                item in members
                or Path(str(item.get("name", ""))).suffix.lower() not in SUBTITLE_EXTS
                or "_episode_key_override" in item
            ):
                continue
            subtitle_parent, _ = split_remote(
                normalize_remote_path(str(item.get("full_path", "/")))
            )
            subtitle_parent_key = _normalize_match_title(split_remote(subtitle_parent)[1])
            subtitle_parent_key = re.sub(
                r"^(?:剧中剧|劇中劇|作中作)", "", subtitle_parent_key
            )
            subtitle_parent_key = NORMALIZED_PARENT_ALIASES.get(
                subtitle_parent_key, subtitle_parent_key
            )
            subtitle_candidates = tuple(sorted({
                int(number)
                for number, variants in official_title_variants.items()
                if len(subtitle_parent_key) >= 6
                and any(
                    _named_special_context_matches(subtitle_parent_key, title)
                    for title in variants
                )
            }))
            source_number = _special_release_source_ordinal(item)
            if (
                subtitle_candidates != candidate_run
                or source_number is None
                or not 1 <= source_number <= len(candidate_run)
            ):
                continue
            item["_episode_kind_override"] = "special"
            item["_episode_key_override"] = candidate_run[source_number - 1]
            mapped_count += 1
        if mapped_count:
            warnings.append(
                f"已根据{evidence}和完整连续源编号，将 "
                f"{len(video_source_numbers)} 集短篇映射为 "
                f"SP{candidate_run[0]:02d}–SP{candidate_run[-1]:02d}"
            )
    return warnings


def _propagate_explicit_video_episode_overrides(
    items: Iterable[dict[str, Any]],
) -> int:
    """Copy a proven video override to exact-basename subtitle companions."""
    entries = list(items)
    routes: dict[str, set[tuple[str, int]]] = defaultdict(set)
    for item in entries:
        if Path(str(item.get("name", ""))).suffix.lower() not in VIDEO_EXTS:
            continue
        kind = item.get("_episode_kind_override")
        number = item.get("_episode_key_override")
        if kind not in {"regular", "special"} or not isinstance(number, int):
            continue
        stem = _batch_subtitle_release_stem(str(item.get("name", "")))
        routes[stem].add((str(kind), int(number)))
    changed = 0
    for item in entries:
        if Path(str(item.get("name", ""))).suffix.lower() not in SUBTITLE_EXTS:
            continue
        stem = _batch_subtitle_release_stem(str(item.get("name", "")))
        matches = routes.get(stem, set())
        if len(matches) != 1:
            continue
        kind, number = next(iter(matches))
        item["_episode_kind_override"] = kind
        item["_episode_key_override"] = number
        changed += 1
    return changed


def _map_explicit_beta_alternate(
    items: Iterable[dict[str, Any]],
    official_title_variants: Mapping[int, Sequence[str]],
) -> int:
    """Map an explicit ``23B/23β`` release to one official beta special."""
    beta_title = re.compile(
        r"(?:β|BETA|MISSING[ ._-]*LINK|ミッシングリンク|缺失之环|缺失之環)",
        re.IGNORECASE,
    )
    candidates = {
        number
        for number, titles in official_title_variants.items()
        if any(beta_title.search(str(title)) for title in titles)
    }
    if len(candidates) != 1:
        return 0
    target = next(iter(candidates))
    marker = re.compile(r"\[\s*23\s*(?:B|β)\s*\]", re.IGNORECASE)
    changed = 0
    for item in items:
        if not marker.search(str(item.get("name", ""))):
            continue
        item["_episode_kind_override"] = "special"
        item["_episode_key_override"] = target
        item["_edition_override"] = "23β"
        changed += 1
    return changed


def _map_release_label_editions(
    items: Iterable[dict[str, Any]],
    official_title_variants: Mapping[int, Sequence[str]],
) -> int:
    """Map evidenced release-label editions from the release lexicon.

    The mapping logic is generic: two data-supplied title patterns each select
    exactly one official candidate, and data-supplied file patterns bind
    release labels to them.  The one-unique-candidate gate keeps these labels
    from affecting an unrelated show's generic ``2D``/``3D`` extras.
    """
    rules = RELEASE_EDITION_RULES
    romeo_re = re.compile(rules["romeo_title"], re.IGNORECASE)
    after_re = re.compile(rules["after_title"], re.IGNORECASE)
    romeo = {
        number for number, titles in official_title_variants.items()
        if any(
            romeo_re.search(str(title)) and not after_re.search(str(title))
            for title in titles
        )
    }
    after = {
        number for number, titles in official_title_variants.items()
        if any(after_re.search(str(title)) for title in titles)
    }
    if len(romeo) != 1 or len(after) != 1 or romeo == after:
        return 0
    marker_re = re.compile(rules["marker"], re.IGNORECASE)
    after_file_re = re.compile(rules["after_file"], re.IGNORECASE)
    romeo_file_re = re.compile(rules["romeo_file"], re.IGNORECASE)
    changed = 0
    for item in items:
        name = unicodedata.normalize("NFKC", str(item.get("name", "")))
        if not marker_re.search(name):
            continue
        target: int | None = None
        if after_file_re.search(name):
            target = next(iter(after))
        elif romeo_file_re.search(name):
            target = next(iter(romeo))
        if target is None:
            continue
        item["_episode_kind_override"] = "special"
        item["_episode_key_override"] = target
        changed += 1
    return changed


def _map_disc_extras_by_official_release_runs(
    items: Iterable[dict[str, Any]],
    *,
    show: Mapping[str, Any],
    positive_seasons: Sequence[Mapping[str, Any]],
    special_runtimes: Mapping[int, int],
    special_air_dates: Mapping[int, str],
    special_title_variants: Mapping[int, Sequence[str]] | None = None,
) -> int:
    """Map numbered disc mini-anime and adjacent full-length OVAs.

    Some multi-season releases restart ``Tokuten_Anime01`` for every TV
    season, while TMDB stores all disc extras in one Season 00 sequence.  We
    derive one short-extra run per official TV season from runtime and release
    gaps.  A full-length special immediately following a run is that release
    season's OVA.  The mapping is enabled only when the run count exactly
    matches the official positive-season count.
    """
    items = list(items)
    dated_short: list[tuple[int, date]] = []
    for number, runtime in special_runtimes.items():
        raw_date = special_air_dates.get(number, "")
        if runtime > 10 or not re.fullmatch(r"\d{4}-\d{2}-\d{2}", raw_date):
            continue
        dated_short.append((number, datetime.fromisoformat(raw_date).date()))
    dated_short.sort()
    runs: list[list[int]] = []
    previous_number: int | None = None
    previous_date: date | None = None
    for number, air_date in dated_short:
        if (
            not runs
            or previous_number is None
            or number != previous_number + 1
            or previous_date is None
            or (air_date - previous_date).days >= 180
        ):
            runs.append([])
        runs[-1].append(number)
        previous_number, previous_date = number, air_date
    season_numbers = sorted(
        int(item["season_number"])
        for item in positive_seasons
        if isinstance(item.get("season_number"), int)
        and not isinstance(item.get("season_number"), bool)
    )
    season_episode_counts = {
        int(item["season_number"]): int(item["episode_count"])
        for item in positive_seasons
        if isinstance(item.get("season_number"), int)
        and not isinstance(item.get("season_number"), bool)
        and isinstance(item.get("episode_count"), int)
        and not isinstance(item.get("episode_count"), bool)
        and int(item["episode_count"]) > 0
    }
    if not season_numbers:
        return 0
    season_air_dates = {
        int(item["season_number"]): datetime.fromisoformat(str(item["air_date"])).date()
        for item in positive_seasons
        if isinstance(item.get("season_number"), int)
        and not isinstance(item.get("season_number"), bool)
        and re.fullmatch(r"\d{4}-\d{2}-\d{2}", str(item.get("air_date") or ""))
    }
    titled_full_specials_by_season: dict[int, list[int]] = defaultdict(list)
    for number, titles in (special_title_variants or {}).items():
        if special_runtimes.get(number, 0) < 18:
            continue
        matched_seasons = {
            season_number
            for title in titles
            if (season_number := _season_from_source("/" + str(title))) is not None
            and season_number in season_episode_counts
        }
        if len(matched_seasons) == 1:
            titled_full_specials_by_season[next(iter(matched_seasons))].append(
                int(number)
            )
    for numbers in titled_full_specials_by_season.values():
        numbers.sort()
    run_by_season: dict[int, list[int]] = {}
    ambiguous_run_seasons: set[int] = set()
    for run in runs:
        first_date = datetime.fromisoformat(special_air_dates[run[0]]).date()
        eligible = [
            (air_date, season_number)
            for season_number, air_date in season_air_dates.items()
            if air_date <= first_date
        ]
        if not eligible:
            continue
        source_season = max(eligible)[1]
        # Multiple indistinguishable short runs for one release season are not
        # safe enough for local SP ordinal mapping.
        if source_season in run_by_season or source_season in ambiguous_run_seasons:
            run_by_season.pop(source_season, None)
            ambiguous_run_seasons.add(source_season)
            continue
        run_by_season[source_season] = run
    if not run_by_season and len(runs) == len(season_numbers):
        run_by_season = dict(zip(season_numbers, runs))

    def item_source_season(item: Mapping[str, Any]) -> int | None:
        name = unicodedata.normalize("NFKC", str(item.get("name", "")))
        source_season = _season_from_series_variant(name, show)
        if source_season is not None:
            return source_season
        for segment in reversed(
            normalize_remote_path(str(item.get("full_path", ""))).split("/")[:-1]
        ):
            source_season = _season_from_source("/" + segment)
            if source_season is None:
                source_season = _season_from_series_variant(segment, show)
            if source_season is not None:
                return source_season
        return None

    # Some series have no short disc extras at all: every Season 00 row is a
    # normal-length episode.  A local ``SP01`` still cannot be copied directly
    # to global S00E01, but a *complete* SP01..SPNN video run can be mapped to
    # the official full-length rows inside that source season's air-date
    # window.  This is the Clannad boundary: first-season overflow E23/E24 owns
    # S00E01/E02, while After Story's complete SP01..SP03 run owns S00E03..E05.
    full_specials_by_season: dict[int, list[int]] = {}
    for source_season, season_start in season_air_dates.items():
        later_dates = [
            air_date
            for number, air_date in season_air_dates.items()
            if number > source_season
        ]
        cutoff = min(later_dates) if later_dates else date.max
        candidates = sorted(
            number
            for number, runtime in special_runtimes.items()
            if runtime >= 18
            and re.fullmatch(
                r"\d{4}-\d{2}-\d{2}", special_air_dates.get(number, "")
            )
            and season_start
            <= datetime.fromisoformat(special_air_dates[number]).date()
            < cutoff
        )
        if (
            candidates
            and candidates == list(range(candidates[0], candidates[-1] + 1))
        ):
            full_specials_by_season[source_season] = candidates

    local_sp_video_numbers: dict[int, set[int]] = defaultdict(set)
    for item in items:
        if Path(str(item.get("name", ""))).suffix.lower() not in VIDEO_EXTS:
            continue
        source_season = item_source_season(item)
        if source_season is None:
            continue
        match = re.search(
            r"(?:^|[\[ _.-])SP\s*0*(\d{1,2})(?:[\] _.-]|$)",
            unicodedata.normalize("NFKC", str(item.get("name", ""))),
            re.I,
        )
        if match is not None:
            local_sp_video_numbers[source_season].add(int(match.group(1)))
    complete_full_sp_runs = {
        source_season: candidates
        for source_season, candidates in full_specials_by_season.items()
        if sorted(local_sp_video_numbers.get(source_season, set()))
        == list(range(1, len(candidates) + 1))
    }
    full_after_run: dict[int, int] = {}
    assigned_runs = sorted(
        (run[0], season_number, run)
        for season_number, run in run_by_season.items()
    )
    for index, (_start, season_number, run) in enumerate(assigned_runs):
        next_start = (
            assigned_runs[index + 1][0]
            if index + 1 < len(assigned_runs)
            else 10**9
        )
        candidates = [
            number
            for number, runtime in special_runtimes.items()
            if run[-1] < number < next_start and runtime >= 18
        ]
        if len(candidates) == 1:
            full_after_run[season_number] = candidates[0]

    changed = 0
    for item in items:
        name = unicodedata.normalize("NFKC", str(item.get("name", "")))
        full_path = str(item.get("full_path", ""))
        # The release filename is the most specific evidence for shared
        # ``字幕备份`` directories.  Looking at generic parents first can
        # incorrectly select the base season before seeing ``2wei/Herz/3rei``.
        source_season = item_source_season(item)
        run = run_by_season.get(source_season or -1)
        tokuten = re.search(r"tokuten[ ._-]*anime\s*0*(\d{1,2})", name, re.I)
        local_sp = re.search(
            r"(?:^|[\[ _.-])SP\s*0*(\d{1,2})(?:[\] _.-]|$)",
            name,
            re.I,
        )
        target: int | None = None
        if tokuten is not None and run is not None:
            ordinal = int(tokuten.group(1))
            if 1 <= ordinal <= len(run):
                target = run[ordinal - 1]
        elif local_sp is not None and run is not None:
            ordinal = int(local_sp.group(1))
            if 1 <= ordinal <= len(run):
                target = run[ordinal - 1]
        elif local_sp is not None and source_season in complete_full_sp_runs:
            ordinal = int(local_sp.group(1))
            full_run = complete_full_sp_runs[int(source_season)]
            if 1 <= ordinal <= len(full_run):
                target = full_run[ordinal - 1]
        elif re.search(r"(?:^|[\[ _-])OVA(?:[\] _.-]|$)", name, re.I):
            target = full_after_run.get(source_season or -1)
        if target is None and source_season in season_air_dates:
            key = extract_episode_key(name)
            season_meta = next(
                (
                    item
                    for item in positive_seasons
                    if item.get("season_number") == source_season
                ),
                None,
            )
            boundary = (
                int(season_meta["episode_count"])
                if isinstance(season_meta, Mapping)
                and isinstance(season_meta.get("episode_count"), int)
                and not isinstance(season_meta.get("episode_count"), bool)
                else 0
            )
            prior_seasons = range(1, int(source_season))
            prior_count = (
                sum(season_episode_counts[number] for number in prior_seasons)
                if all(number in season_episode_counts for number in prior_seasons)
                else 0
            )
            cumulative_regular = (
                prior_count > 0
                and prior_count < key.number <= prior_count + boundary
                if key is not None and key.kind == "regular"
                else False
            )
            if (
                key is not None
                and key.kind in {"regular", "special"}
                and key.number > boundary
                # A sequel may continue the whole-series counter.  Those
                # values are ordinary episodes, not local overflow ordinals
                # for disc OVAs.  Leave them for the strict cumulative-season
                # normalizer, which still requires the complete TMDB boundary.
                and not cumulative_regular
            ):
                next_season_dates = [
                    air_date
                    for number, air_date in season_air_dates.items()
                    if number > source_season
                ]
                cutoff = min(next_season_dates) if next_season_dates else date.max
                dated_full_candidates = sorted(
                    number
                    for number, runtime in special_runtimes.items()
                    if runtime >= 18
                    and re.fullmatch(
                        r"\d{4}-\d{2}-\d{2}", special_air_dates.get(number, "")
                    )
                    and season_air_dates[source_season]
                    <= datetime.fromisoformat(special_air_dates[number]).date()
                    < cutoff
                )
                full_candidates = (
                    titled_full_specials_by_season.get(int(source_season), [])
                    or dated_full_candidates
                )
                ordinal = key.number - boundary
                if 1 <= ordinal <= len(full_candidates):
                    target = full_candidates[ordinal - 1]
        if target is None:
            continue
        item["_episode_kind_override"] = "special"
        item["_episode_key_override"] = target
        changed += 1

    # Some disc releases name the last short extra only ``OVA``/``OVBSP``
    # instead of continuing the local SP ordinal.  Do not guess from that
    # marker alone: map it only when the same release season has exactly one
    # still-unclaimed official short-extra slot and exactly one unmapped
    # basename carrying an actual video.  Exact-basename subtitle companions
    # follow that video.  This covers layouts such as SP01, SP02, OVBSP and
    # SP01, SP02, OVA without weakening orphan-subtitle handling.
    claimed_by_season: dict[int, set[int]] = defaultdict(set)
    unmapped_named_extras: dict[int, dict[str, list[dict[str, Any]]]] = defaultdict(
        lambda: defaultdict(list)
    )
    for item in items:
        name = unicodedata.normalize("NFKC", str(item.get("name", "")))
        full_path = str(item.get("full_path", ""))
        source_season = item_source_season(item)
        run = run_by_season.get(source_season or -1)
        if run is None:
            continue
        override = item.get("_episode_key_override")
        if (
            item.get("_episode_kind_override") == "special"
            and isinstance(override, int)
            and not isinstance(override, bool)
        ):
            if override in run:
                claimed_by_season[int(source_season)].add(override)
            # A full-length OVA can sit outside the short-extra run.  It is
            # already proven and must not count as a second *unmapped* named
            # basename when resolving an adjacent OVBSP.
            continue
        if not re.search(r"(?:^|[\[ _-])(?:OVBSP|OVA|OAV|OAD)(?:[\] _.-]|$)", name, re.I):
            continue
        stem = _collision_key(Path(name).stem)
        unmapped_named_extras[int(source_season)][stem].append(item)

    for source_season, by_stem in unmapped_named_extras.items():
        remaining = [
            number
            for number in run_by_season.get(source_season, ())
            if number not in claimed_by_season[source_season]
        ]
        video_stems = [
            stem
            for stem, members in by_stem.items()
            if any(
                Path(str(member.get("name", ""))).suffix.lower() in VIDEO_EXTS
                for member in members
            )
        ]
        if len(remaining) != 1 or len(video_stems) != 1:
            continue
        target = remaining[0]
        for item in by_stem[video_stems[0]]:
            item["_episode_kind_override"] = "special"
            item["_episode_key_override"] = target
            changed += 1
    return changed


def _map_split_official_special_folder(
    items: Iterable[dict[str, Any]],
    official_title_variants: Mapping[int, Sequence[str]],
) -> int:
    """Split a proven multi-file special when TMDB collapsed it to one row."""
    items = list(items)
    by_parent: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for item in items:
        if Path(str(item.get("name", ""))).suffix.lower() not in VIDEO_EXTS:
            continue
        full_path = normalize_remote_path(str(item.get("full_path", "/")))
        by_parent[split_remote(full_path)[0]].append(item)

    changed = 0
    for parent, videos in by_parent.items():
        numbered = [
            (key.number, item)
            for item in videos
            if (key := extract_episode_key(str(item.get("name", "")))) is not None
            and key.kind == "regular"
            and not key.end_number
        ]
        numbered.sort(key=lambda pair: pair[0])
        numbers = [number for number, _ in numbered]
        if len(numbered) < 2 or len(numbered) > 4 or numbers != list(range(1, len(numbered) + 1)):
            continue
        parent_label = split_remote(parent)[1]
        parent_queries = {
            _normalize_match_title(query)
            for query in _franchise_member_queries("/" + parent_label)
            if len(_normalize_match_title(query)) >= 6
        }
        matches = {
            number
            for number, variants in official_title_variants.items()
            if any(
                _named_special_context_matches(query, title)
                for query in parent_queries
                for title in variants
            )
        }
        if len(matches) != 1:
            continue
        first_target = next(iter(matches))
        if any(
            first_target + offset in official_title_variants
            for offset in range(1, len(numbered))
        ):
            continue
        base_title = next(
            (
                title
                for title in official_title_variants[first_target]
                if isinstance(title, str) and title.strip()
            ),
            parent_label,
        )
        # TMDB deliberately stores the broadcast as one Season 00 episode,
        # while some releases split that episode into two or more files. Keep
        # the official S00 episode identity and express the physical split as
        # part1/part2; inventing S00E07 would collide with future/real TMDB
        # numbering and can also send a bare ``02.mp4`` to an unrelated movie.
        parent_items = [
            item
            for item in items
            if split_remote(normalize_remote_path(str(item.get("full_path", "/"))))[0]
            == parent
        ]
        for index, (source_number, _video) in enumerate(numbered):
            matching_part_items = [
                item
                for item in parent_items
                if (
                    (key := extract_episode_key(str(item.get("name", ""))))
                    is not None
                    and key.kind == "regular"
                    and key.number == source_number
                    and not key.end_number
                )
            ]
            for item in matching_part_items:
                item["_episode_kind_override"] = "special"
                item["_episode_key_override"] = first_target
                item["_episode_part_override"] = index + 1
                item["_episode_title_override"] = base_title
                changed += 1
    return changed


def _matches_named_special_release_context(
    item: Mapping[str, Any],
    official_title_variants: Mapping[int, Sequence[str]],
    *,
    series_titles: Sequence[str] = (),
) -> bool:
    """Recognize a named official special run even without an SP/OVA token."""
    parent_label = normalize_remote_path(
        str(item.get("full_path", "/"))
    ).rsplit("/", 2)[-2]
    # An explicit ordinary season directory is stronger evidence than fuzzy
    # overlap with an official special title that repeats the franchise name
    # (for example ``魔法禁书目录 第三季`` versus a similarly named
    # disc extra).  Only let a season-looking parent enter the named-special
    # matcher when that parent itself also carries explicit special evidence.
    if (
        _season_from_source("/" + parent_label) is not None
        and not re.search(
            r"(?:特别篇|特典|番外|specials?|ovbsp|ova|oav|oad|"
            r"break[ ._-]*time|休息时间|休憩時間|小剧场|小劇場|petit|ぷち|"
            r"课外授业篇|課外授業編|kagai[ ._-]*jugy[oō][ ._-]*hen|"
            r"(?:^|[/\\])SPs?(?:[/\\]|$))",
            parent_label,
            re.IGNORECASE,
        )
    ):
        return False
    parent_keys = {
        _normalize_match_title(parent_label),
        *(
            _normalize_match_title(query)
            for query in _franchise_member_queries("/" + parent_label)
        ),
    }
    parent_keys = {
        re.sub(r"^(?:剧中剧|劇中劇|作中作)", "", key)
        for key in parent_keys
    }
    parent_keys = {
        NORMALIZED_PARENT_ALIASES.get(key, key)
        for key in parent_keys
        if len(key) >= 6
    }
    # Franchise query cleanup can reduce a noisy release folder to the exact
    # series title.  That shared title is not evidence for Season 00 when an
    # official special merely repeats the franchise name.
    series_keys = {
        _normalize_match_title(title)
        for title in series_titles
        if _normalize_match_title(title)
    }
    parent_keys.difference_update(series_keys)
    return any(
        _named_special_context_matches(parent_key, title)
        for parent_key in parent_keys
        for variants in official_title_variants.values()
        for title in variants
    )


def _embedded_movie_tmdb_id(item: Mapping[str, Any]) -> int | None:
    text = f"{item.get('name', '')} {item.get('full_path', '')}"
    if not re.search(r"(?:剧场版|电影|movie|film)", text, re.IGNORECASE):
        return None
    match = re.search(r"\{tmdb-(\d+)\}", text, re.IGNORECASE)
    return int(match.group(1)) if match else None


def _has_movie_context(item: Mapping[str, Any]) -> bool:
    full_path = str(item.get("full_path", ""))
    # The release title itself can be the only explicit movie marker.  This is
    # common in otherwise ordinary series shelves such as
    # ``STEINS;GATE Movie``.  Looking only at the parent directory made that
    # file depend on the later fuzzy child-work pass and a transient metadata
    # lookup failure could silently leave it as an unidentified extra.
    for segment in normalize_remote_path(full_path).split("/"):
        if not re.search(
            r"(?:剧场版|劇場版|电影|電影|真人版|live[ ._-]*action|movies?|films?)",
            segment,
            re.IGNORECASE,
        ):
            continue
        # A release-container label such as “S01-S03 合集，附两部剧场版”
        # advertises nested bonus movies; it is not movie evidence for every
        # episode below that root. The actual movie subdirectory still carries
        # its own unqualified movie marker and remains detectable.
        if re.search(
            r"(?:(?:附|含|包含)|[+＋/&、和与及])\s*"
            r"(?:\d+|[一二两三四五六七八九十]+)\s*部?\s*"
            r"(?:剧场版|劇場版|电影|電影|movies?|films?)",
            segment,
            re.IGNORECASE,
        ):
            continue
        return True
    return False


def _movie_query_from_item(item: Mapping[str, Any]) -> str:
    name = Path(str(item.get("name", ""))).stem
    name = re.sub(r"\[[^\]]*\]|\{(?:tmdb|imdb)-[^{}]+\}", " ", name, flags=re.I)
    name = re.sub(
        r"[（(][^（）()]*?(?:Ma\d+p|x26[45]|HEVC|AVC|flac|AAC|ass)"
        r"[^（）()]*[）)]",
        " ",
        name,
        flags=re.I,
    )
    name = re.sub(r"\b(?:OVA|OAV|OAD)\s*0*\d+\b", " ", name, flags=re.I)
    name = re.sub(r"\(\s*[0-9A-F]{8}\s*\)\s*$", " ", name, flags=re.I)
    # Chinese release names often put the clean title first and append an
    # English title/codec payload after a dot.
    if re.search(r"[\u3400-\u9fff]", name):
        name = re.split(r"\.(?=[A-Za-z])", name, maxsplit=1)[0]
    name = re.sub(
        r"\b(?:BD|WEB|BluRay|1080P|2160P|720P|4K|x26[45]|HEVC|日语中字|中字).*$",
        " ",
        name,
        flags=re.I,
    )
    return re.sub(r"[._]+|\s+", " ", name).strip(" -")


def _movie_queries_from_item(item: Mapping[str, Any]) -> list[str]:
    """Return the localized title plus an embedded Latin release-title fallback."""
    primary = _movie_query_from_item(item)
    raw_stem = Path(str(item.get("name", ""))).stem
    bracket_latin = [
        match.strip()
        for match in re.findall(r"\[([^\]]*[A-Za-z][^\]]{5,})\]", raw_stem)
        if not re.fullmatch(
            r"(?:BD|WEB|BluRay|1080P|2160P|720P|4K|x26[45]|HEVC|AV1|GB|CHS|CHT|MP4)"
            r"(?:[_\s-].*)?",
            match.strip(),
            flags=re.I,
        )
    ]
    stem = raw_stem
    stem = re.sub(r"\[[^\]]*\]|\{(?:tmdb|imdb)-[^{}]+\}", " ", stem, flags=re.I)
    latin_match = re.search(r"[A-Za-z][A-Za-z0-9'’:&+._\- ]{5,}", stem)
    latin = latin_match.group(0) if latin_match else ""
    latin = re.sub(
        r"\b(?:BD(?:1080P|2160P|720P)?|WEB|BluRay|1080P|2160P|720P|4K|x26[45]|HEVC|AV1|日语中字|中字)\b.*$",
        " ",
        latin,
        flags=re.I,
    )
    latin = re.sub(r"[._]+|\s+", " ", latin).strip(" -")
    latin_without_year = re.sub(r"\s+(?:19|20)\d{2}$", "", latin).strip()
    parent_queries: list[str] = []
    full_path = normalize_remote_path(str(item.get("full_path", "")))
    for segment in reversed(full_path.rsplit("/", 1)[0].split("/")):
        if not re.search(
            r"(?:剧场版|劇場版|电影|電影|真人版|live[ ._-]*action|movie|film)",
            segment,
            re.I,
        ):
            continue
        cleaned = re.sub(r"^\s*(?:19|20)\d{2}(?:[.\-]\d{1,2})?\s*", "", segment)
        cleaned = re.sub(r"\[[^\]]*\]", " ", cleaned)
        cleaned = re.sub(
            r"\b(?:4k|8k|2160p|1080p|720p|bluray|blu-?ray|web-?dl|webrip)\b.*$",
            " ",
            cleaned,
            flags=re.I,
        )
        cleaned = re.sub(r"[._]+|\s+", " ", cleaned).strip(" -")
        if cleaned:
            parent_queries.extend(
                [
                    cleaned,
                    re.sub(r"^(?:剧场版|劇場版)[：:\s]*", "", cleaned).strip(),
                    re.split(
                        r"(?:剧场版|劇場版|电影|電影|movie|film)[：:\s]*",
                        cleaned,
                        maxsplit=1,
                        flags=re.I,
                    )[-1].strip(),
                ]
            )
            if parent_queries:
                break
    return list(
        dict.fromkeys(
            value
            for value in (
                primary,
                latin,
                latin_without_year,
                *bracket_latin,
                *parent_queries,
            )
            if value
        )
    )


def _specific_movie_query_agrees_with_match(query: str, match: AutoMatch) -> bool:
    """Reject a generic franchise alias when the source names another special."""
    query_key = _normalize_match_title(query)
    if len(query_key) < 6:
        return False
    titles = [
        match.title,
        *(match.decision_trace.get("official_titles") or []),
        *(match.decision_trace.get("aliases_checked") or []),
    ]
    for title in titles:
        title_key = _normalize_match_title(str(title))
        if not title_key:
            continue
        if query_key == title_key:
            return True
        length_ratio = min(len(query_key), len(title_key)) / max(
            len(query_key), len(title_key)
        )
        if (
            length_ratio >= 0.72
            and (query_key in title_key or title_key in query_key)
        ):
            return True
        if difflib.SequenceMatcher(None, query_key, title_key).ratio() >= 0.82:
            return True
    return False


def _numbered_movie_collection_ordinal(item: Mapping[str, Any]) -> int | None:
    """Return a release-order number from a clearly numbered movie filename.

    Movie packs often attach the number to the title (``死亡笔记1：前篇``),
    which is intentionally not parsed as a TV episode.  A second encode may
    use only a leading number (``1国粤日音轨``).  Keep this parser private to
    the collection-evidence pass so those forms cannot become TV episodes.
    """
    stem = unicodedata.normalize("NFKC", Path(str(item.get("name", ""))).stem)
    stem = re.sub(r"^\[[^\]]+\]\s*", "", stem)
    match = re.match(
        r"^\s*(?:(?P<title>.*?[A-Za-z\u3400-\u9fff\u3040-\u30ff])\s*)?"
        r"(?P<number>[1-9]\d{0,2})(?=\s*(?:[:：._\-]|[A-Za-z\u3400-\u9fff\u3040-\u30ff]))",
        stem,
    )
    if not match:
        return None
    title = (match.group("title") or "").strip()
    if title and len(_normalize_match_title(title)) < 3:
        return None
    return int(match.group("number"))


def _has_numbered_movie_collection_context(item: Mapping[str, Any]) -> bool:
    if _numbered_movie_collection_ordinal(item) is None:
        return False
    parent = str(item.get("full_path", "")).rsplit("/", 1)[0]
    parent_label = split_remote(parent)[1]
    return bool(
        re.search(
            r"(?:真人版|live[ ._-]*action|电影合集|電影合集|"
            r"电影系列|電影系列|movie[ ._-]*collection)",
            parent_label,
            re.I,
        )
    )


def _numbered_movie_collection_queries(parent: str, items: Sequence[Mapping[str, Any]]) -> list[str]:
    """Build title-only collection queries from a live-action movie folder."""
    labels = [split_remote(parent)[1]]
    grandparent = split_remote(parent)[0]
    if grandparent and grandparent != "/":
        labels.append(split_remote(grandparent)[1])
    prefixes: list[str] = []
    for item in items:
        stem = unicodedata.normalize("NFKC", Path(str(item.get("name", ""))).stem)
        ordinal = _numbered_movie_collection_ordinal(item)
        if ordinal is None:
            continue
        match = re.match(r"^\s*(.*?[A-Za-z\u3400-\u9fff\u3040-\u30ff])\s*" + str(ordinal), stem)
        if match and len(_normalize_match_title(match.group(1))) >= 3:
            prefixes.append(match.group(1).strip())

    queries: list[str] = []
    for value in [*prefixes, *labels]:
        cleaned = _query_from_source("/" + value)
        cleaned = re.sub(r"\d+\s*[-–—~～至到]\s*\d+", " ", cleaned)
        cleaned = re.sub(r"[（(][^）)]*(?:真人版|live[ ._-]*action)[^）)]*[）)]", " ", cleaned, flags=re.I)
        cleaned = re.sub(
            r"(?:真人版|live[ ._-]*action|中文字幕|字幕版|"
            r"国粤日音轨|國粵日音軌|电影合集|電影合集|"
            r"电影系列|電影系列|movie[ ._-]*collection)",
            " ",
            cleaned,
            flags=re.I,
        )
        cleaned = re.sub(r"\s+", " ", cleaned).strip(" -_()（）")
        if len(_normalize_match_title(cleaned)) >= 3:
            queries.append(cleaned)
    return list(dict.fromkeys(queries))


def _resolve_numbered_movie_collection_groups(
    tmdb_client: TMDBClient,
    unresolved: Sequence[dict[str, Any]],
) -> tuple[dict[int, list[dict[str, Any]]], list[dict[str, Any]], list[str]]:
    """Resolve numbered live-action movie encodes from official TMDB collections.

    Evidence is deliberately conjunctive: an explicit live-action/movie-pack
    directory, a unique continuous source order, a matching TMDB collection,
    and either the complete official member count or an explicit source range.
    This also lets a ``1-3`` alternate encode join the same first three movies
    without treating its bare numbers as TV episodes.
    """
    by_parent: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for item in unresolved:
        if Path(str(item.get("name", ""))).suffix.lower() not in VIDEO_EXTS:
            continue
        parent, _ = split_remote(normalize_remote_path(str(item.get("full_path", ""))))
        if _has_numbered_movie_collection_context(item):
            by_parent[parent].append(item)

    resolved: dict[int, list[dict[str, Any]]] = defaultdict(list)
    consumed: set[str] = set()
    warnings: list[str] = []
    for parent, videos in sorted(by_parent.items()):
        number_to_videos: dict[int, list[dict[str, Any]]] = defaultdict(list)
        for item in videos:
            ordinal = _numbered_movie_collection_ordinal(item)
            if ordinal is not None:
                number_to_videos[ordinal].append(item)
        numbers = sorted(number_to_videos)
        if len(numbers) < 2 or numbers != list(range(1, numbers[-1] + 1)):
            continue
        range_match = re.search(r"(?<!\d)1\s*[-–—~～至到]\s*(\d{1,3})(?!\d)", split_remote(parent)[1])
        explicit_range = int(range_match.group(1)) if range_match else None
        if explicit_range is not None and explicit_range != numbers[-1]:
            continue

        candidates: dict[int, tuple[float, Mapping[str, Any]]] = {}
        for query in _numbered_movie_collection_queries(parent, videos):
            response = tmdb_client.get("/search/collection", query=query)
            query_key = _normalize_match_title(query)
            for item in response.get("results") or []:
                if not isinstance(item, Mapping) or isinstance(item.get("id"), bool):
                    continue
                title = str(item.get("name") or item.get("original_name") or "")
                score = _title_similarity(query_key, title)
                if score < 0.88:
                    continue
                collection_id = int(item["id"])
                previous = candidates.get(collection_id)
                if previous is None or score > previous[0]:
                    candidates[collection_id] = (score, item)

        eligible: list[tuple[float, int, list[Mapping[str, Any]]]] = []
        for collection_id, (score, _search_item) in candidates.items():
            detail = tmdb_client.get(f"/collection/{collection_id}")
            parts = [
                item for item in (detail.get("parts") or [])
                if isinstance(item, Mapping)
                and not isinstance(item.get("id"), bool)
                and str(item.get("release_date") or "")
            ]
            parts.sort(key=lambda item: (str(item.get("release_date")), int(item["id"])))
            if numbers[-1] > len(parts):
                continue
            if explicit_range is None and len(parts) != numbers[-1]:
                continue
            eligible.append((score, collection_id, parts))
        eligible.sort(reverse=True, key=lambda item: item[0])
        if not eligible or (
            len(eligible) > 1 and eligible[0][0] - eligible[1][0] < 0.08
        ):
            continue

        _score, collection_id, parts = eligible[0]
        for number, members in number_to_videos.items():
            movie_id = int(parts[number - 1]["id"])
            resolved[movie_id].extend(members)
            consumed.update(str(item.get("full_path", "")) for item in members)
        warnings.append(
            f"目录 {parent} 已根据明确真人版语义、连续编号与 "
            f"TMDB 合集 {collection_id} 官方成员上映顺序唯一映射"
        )

    remaining = [
        item for item in unresolved
        if str(item.get("full_path", "")) not in consumed
    ]
    return dict(resolved), remaining, warnings


def _season_overflow_special(
    special_titles: Mapping[EpisodeKey, str],
    *,
    season: int,
    ordinal: int,
    explicit_special_context: bool = False,
    special_season_candidates: Mapping[int, Sequence[EpisodeKey]] | None = None,
) -> tuple[EpisodeKey, str] | None:
    chinese = {
        1: "一", 2: "二", 3: "三", 4: "四", 5: "五",
        6: "六", 7: "七", 8: "八", 9: "九", 10: "十",
    }.get(season)
    season_tokens = [str(season), *( [chinese] if chinese else [])]
    for key, title in special_titles.items():
        normalized = unicodedata.normalize("NFKC", title)
        if not any(re.search(rf"第?\s*{re.escape(token)}\s*季", normalized) for token in season_tokens):
            continue
        ova = re.search(r"(?:OVA|OAV|OAD|特典)\s*(\d+)?", normalized, re.IGNORECASE)
        if not ova:
            continue
        title_ordinal = int(ova.group(1)) if ova.group(1) else 1
        if title_ordinal == ordinal:
            return key, title
    if special_season_candidates is not None:
        candidates = list(special_season_candidates.get(season) or [])
        if 0 < ordinal <= len(candidates):
            key = candidates[ordinal - 1]
            title = special_titles.get(key)
            if title:
                return key, title
    return None


def _map_unnumbered_specials(
    files: Sequence[Mapping[str, Any]],
    groups: dict[EpisodeKey, list[dict[str, Any]]],
    special_titles: Mapping[EpisodeKey, str],
    series_title: str,
    *,
    season: int | None = None,
    special_season_candidates: Mapping[int, Sequence[EpisodeKey]] | None = None,
) -> list[str]:
    """Map unnumbered, explicitly-labelled specials only when one TMDB title wins clearly."""
    recognized = {
        _collision_key(str(item.get("full_path", "")))
        for items in groups.values()
        for item in items
    }
    warnings: list[str] = []
    for raw_item in _filter_media(files):
        full_path = str(raw_item.get("full_path", ""))
        if _collision_key(full_path) in recognized or not _has_special_context(raw_item):
            continue
        source_dir, _ = split_remote(full_path)
        labels = [str(raw_item.get("name", "")), source_dir.rsplit("/", 1)[-1]]
        source_keys = [
            key for key in (_special_title_key(label, series_title) for label in labels)
            if len(key) >= 3
        ]
        if not source_keys:
            continue
        ranked: list[tuple[float, EpisodeKey]] = []
        for episode_key, title in special_titles.items():
            title_key = _special_title_key(title, series_title)
            if len(title_key) < 3:
                continue
            score = max(
                1.0
                if source_key in title_key or title_key in source_key
                else difflib.SequenceMatcher(None, source_key, title_key).ratio()
                for source_key in source_keys
            )
            ranked.append((score, episode_key))
        ranked.sort(key=lambda row: (-row[0], row[1]))
        if not ranked:
            continue
        best_score, best_key = ranked[0]
        second_score = ranked[1][0] if len(ranked) > 1 else 0.0
        if best_score < 0.65 or best_score - second_score < 0.15:
            continue
        item = dict(raw_item)
        groups.setdefault(best_key, []).append(item)
        recognized.add(_collision_key(full_path))
        warnings.append(
            f"根据 TMDB 官方特别篇标题将 {item['name']} 自动映射为 {best_key.display}"
        )

    if season is not None and special_season_candidates is not None:
        recognized = {
            _collision_key(str(item.get("full_path", "")))
            for items in groups.values()
            for item in items
        }
        pending: dict[str, list[dict[str, Any]]] = defaultdict(list)
        priorities = {"OVBSP": 0, "OVA": 1, "OAV": 1, "OAD": 2}
        for raw_item in _filter_media(files):
            full_path = str(raw_item.get("full_path", ""))
            if _collision_key(full_path) in recognized or not _has_special_context(raw_item):
                continue
            normalized = unicodedata.normalize(
                "NFKC", str(raw_item.get("name", ""))
            ).upper()
            marker_match = re.search(r"OVBSP|OVA|OAV|OAD", normalized)
            if marker_match:
                pending[marker_match.group(0)].append(dict(raw_item))
        used = {key for key in groups if key.kind == "special"}
        candidates = [
            key
            for key in special_season_candidates.get(season, ())
            if key not in used
        ]
        release_groups = sorted(
            pending,
            key=lambda marker: (priorities[marker], marker),
        )
        if release_groups and len(release_groups) == len(candidates):
            for marker, target_key in zip(release_groups, candidates):
                groups.setdefault(target_key, []).extend(pending[marker])
                warnings.append(
                    f"根据第 {season} 季官方时间线将 {marker} "
                    f"自动映射为 {target_key.display}"
                )
    return warnings


def _align_subtitles_to_video_sequence(
    groups: dict[EpisodeKey, list[dict[str, Any]]],
) -> bool:
    """Align a complete subtitle track by order when release numbering has safe gaps."""
    video_keys = sorted(
        key
        for key, items in groups.items()
        if key.kind == "regular"
        and key.end_number == 0
        and any(Path(str(item.get("name", ""))).suffix.lower() in VIDEO_EXTS for item in items)
    )
    subtitle_keys = sorted(
        key
        for key, items in groups.items()
        if key.kind == "regular"
        and key.end_number == 0
        and any(Path(str(item.get("name", ""))).suffix.lower() in SUBTITLE_EXTS for item in items)
    )
    if (
        len(video_keys) < 2
        or len(video_keys) != len(subtitle_keys)
        or video_keys == subtitle_keys
        or [key.number for key in video_keys]
        != list(range(video_keys[0].number, video_keys[0].number + len(video_keys)))
    ):
        return False
    subtitle_items = {
        key: [
            item
            for item in groups[key]
            if Path(str(item.get("name", ""))).suffix.lower() in SUBTITLE_EXTS
        ]
        for key in subtitle_keys
    }
    if any(not items for items in subtitle_items.values()):
        return False
    for key in subtitle_keys:
        remaining = [
            item
            for item in groups[key]
            if Path(str(item.get("name", ""))).suffix.lower() not in SUBTITLE_EXTS
        ]
        if remaining:
            groups[key] = remaining
        else:
            del groups[key]
    for source_key, target_key in zip(subtitle_keys, video_keys):
        groups.setdefault(target_key, []).extend(subtitle_items[source_key])
    return True


def build_tv_plan(
    alist: AListClient,
    tmdb_client: TMDBClient,
    *,
    src_path: str,
    parent_path: str,
    tmdb_id: int,
    season: int,
    absolute: bool,
    prefer_simplified: bool,
    allow_unmapped: bool,
    ignore_orphan_temp: bool = False,
    episode_map_path: Path | None = None,
    episode_group_id: str | None = None,
    auto_special_title_match: bool = False,
    auto_align_subtitles: bool = False,
    allow_release_dash_ordinal: bool = False,
    source_files: Sequence[Mapping[str, Any]] | None = None,
    media_root: str | None = None,
) -> Plan:
    show = tmdb_client.get(f"/tv/{tmdb_id}")
    title = safe_name(str(show.get("name") or show.get("original_name") or tmdb_id))
    original_title = safe_name(str(show.get("original_name") or title))
    year = _extract_year(show.get("first_air_date"))
    series_label = title
    desired_series_dir = join_remote(parent_path, series_label)
    series_dir, library_identity_state = resolve_existing_library_root(
        alist,
        parent_path=parent_path,
        desired_root=desired_series_dir,
        tmdb_id=tmdb_id,
        tv=True,
    )
    special_titles: dict[EpisodeKey, str] = {}
    special_season_candidates: dict[int, list[EpisodeKey]] = {}
    episode_map = _build_tv_episode_map(
        tmdb_client,
        show,
        tmdb_id,
        season,
        absolute,
        episode_group_id=episode_group_id,
        special_titles=special_titles,
        special_season_candidates=special_season_candidates,
    )
    episode_overrides = _load_episode_map(episode_map_path) if episode_map_path else {}
    override_official_titles: dict[tuple[int, int], str] = {}
    if episode_overrides:
        target_seasons = sorted({value[0] for value in episode_overrides.values()})
        for target_season in target_seasons:
            try:
                payload = tmdb_client.get(f"/tv/{tmdb_id}/season/{target_season}")
            except ApiError as exc:
                raise PlanError(
                    f"无法核验显式集号映射的 TMDB Season {target_season:02d}"
                ) from exc
            episodes = payload.get("episodes") if isinstance(payload, Mapping) else None
            if not isinstance(episodes, list):
                raise PlanError(
                    f"显式集号映射目标 Season {target_season:02d} 缺少官方集数"
                )
            for item in episodes:
                if (
                    isinstance(item, Mapping)
                    and type(item.get("episode_number")) is int
                    and int(item["episode_number"]) > 0
                ):
                    override_official_titles[(target_season, int(item["episode_number"]))] = str(
                        item.get("name") or f"第{int(item['episode_number'])}集"
                    )
        for source_key, (target_season, target_episode, target_end) in episode_overrides.items():
            source_end = source_key.end_number or source_key.number
            resolved_target_end = target_end or target_episode
            if source_end - source_key.number != resolved_target_end - target_episode:
                raise PlanError(
                    f"显式集号映射范围长度不一致: {source_key.display}"
                )
            missing = [
                number for number in range(target_episode, resolved_target_end + 1)
                if (target_season, number) not in override_official_titles
            ]
            if missing:
                raise PlanError(
                    f"显式集号映射目标不存在: "
                    f"S{target_season:02d}E{missing[0]:02d}"
                )

    if source_files is not None:
        files = [dict(item) for item in source_files]
    else:
        files = alist.walk(src_path, ignore_orphan_temp=ignore_orphan_temp)
        # Some OVA-only works store every playable file below folders named
        # ``OVA 01``/``Special``. The normal walk intentionally skips bonus
        # containers for ordinary seasons, but when that produces no video at
        # all, retry with bonus containers included instead of declaring the
        # work empty.
        if not any(
            Path(str(item.get("name", ""))).suffix.lower() in VIDEO_EXTS
            for item in _filter_media(files)
        ):
            files = alist.walk(
                src_path,
                ignore_orphan_temp=ignore_orphan_temp,
                include_bonus=True,
            )
    # Provider text exports such as ``.sc.srt.txt`` are admitted to the
    # ordinary parser only after a bounded, content-validated normalization.
    # Keep the source object's real ``full_path`` untouched so the normal
    # writer moves it without a provider-only side channel.
    files, exported_srt_issues = normalize_exported_srt_entries(
        alist,
        files,
        original_language=show.get("original_language"),
    )
    cleanup_files = _planned_cleanup_files(files)
    # Keep destructive cleanup candidates out of every downstream episode
    # parser.  Computing ``cleanup_files`` alone is not sufficient: parsing
    # the original scan again can otherwise put one source path into both the
    # media plan and the cleanup plan (for example an ED_EP45 variant whose
    # matching E45 primary video proves it is a bonus ending).
    media_files = _filter_media(files)
    tv_bonus_files = [
        item
        for item in media_files
        if Path(str(item.get("name", ""))).suffix.lower() in VIDEO_EXTS
        and BONUS_DIRECTORY_RE.search(str(item.get("full_path", "")))
        and bonus_type(str(item.get("name", ""))) is not None
    ]
    if tv_bonus_files:
        bonus_paths = {
            _collision_key(str(item.get("full_path", "")))
            for item in tv_bonus_files
        }
        media_files = [
            item
            for item in media_files
            if _collision_key(str(item.get("full_path", ""))) not in bonus_paths
        ]
    if not any(
        Path(str(item.get("name", ""))).suffix.lower() in VIDEO_EXTS
        for item in media_files
    ):
        raise PlanError(
            "未找到剧集视频文件；目录可能只有字幕或未解压的分卷压缩包，"
            "已停止以避免仅移动字幕。"
        )
    warnings: list[str] = []
    _append_cleanup_warning(warnings, cleanup_files)
    if library_identity_state == "same_tmdb_id" and series_dir != desired_series_dir:
        warnings.append(
            f"现有 tvshow.nfo 已确认相同 TMDB ID {tmdb_id}；"
            f"已保留现有目录名 {split_remote(series_dir)[1]!r}"
        )
    elif library_identity_state == "matching_name_without_nfo":
        warnings.append(
            "目标存在同名目录但缺少可验证的 tvshow.nfo；标题/年份相符，"
            "已保留现有目录名并继续自动补齐元数据"
        )
    if ignore_orphan_temp:
        warnings.append("已显式忽略 .scraper-tmp-* 遗留条目，可能存在未恢复文件")
    all_groups = parse_ep_files(
        media_files,
        prefer_simplified=False,
        defer_unnumbered_specials=auto_special_title_match,
        allow_release_dash_ordinal=allow_release_dash_ordinal,
    )
    subtitle_alignment_applied = bool(
        auto_align_subtitles and _align_subtitles_to_video_sequence(all_groups)
    )
    if subtitle_alignment_applied:
        warnings.append(
            "检测到字幕发布序号与连续视频集号错位；数量完全相等时已按顺序对齐并记录差异"
        )
    if auto_special_title_match:
        warnings.extend(
            _remap_postseason_oav_suffix(all_groups, special_titles)
        )
        warnings.extend(
            _remap_numbered_specials_by_official_label(all_groups, special_titles)
        )
        warnings.extend(
            _map_unnumbered_specials(
                media_files,
                all_groups,
                special_titles,
                title,
                season=season,
                special_season_candidates=special_season_candidates,
            )
        )
        warnings.extend(
            _map_unnumbered_special_from_subtitle_title(
                alist,
                media_files,
                all_groups,
                special_titles,
                series_titles=[
                    str(show.get("name") or ""),
                    str(show.get("original_name") or ""),
                ],
                regular_episode_count=sum(
                    1
                    for key, (target_season, _target_episode, _title) in episode_map.items()
                    if key.kind == "regular" and target_season == season
                ),
                tmdb_client=tmdb_client,
                tmdb_id=tmdb_id,
                season=season,
            )
        )
    unparsed_paths = _unparsed_media_paths(media_files, all_groups)
    unparsed_videos = [
        path for path in unparsed_paths if Path(path).suffix.lower() in VIDEO_EXTS
    ]
    if unparsed_videos and auto_special_title_match:
        warnings.append(
            f"{len(unparsed_videos)} 个无法唯一识别的附加视频将保留于"
            "源目录自动规划未闭合；"
            "其余可确认媒体仍会正常整理"
        )
    else:
        _raise_unparsed_media(unparsed_videos, "剧集")
    retained_subtitles = [
        path for path in unparsed_paths if Path(path).suffix.lower() in SUBTITLE_EXTS
    ]
    if retained_subtitles:
        warnings.append(
            f"{len(retained_subtitles)} 个无对应视频或无法唯一编号的字幕将保留于"
            "源目录自动规划未闭合，不影响其余媒体整理"
        )
    problem_files = [
        *(
            PlannedProblem(
                source_path=path,
                reason="无法唯一识别的附加视频；保留原位并标记规划未闭合",
            )
            for path in unparsed_videos
        ),
    ]
    if exported_srt_issues:
        warnings.append(
            f"{len(exported_srt_issues)} 个导出 .sc/.tc.srt.txt 字幕未通过"
            " UTF-8/SRT 内容校验，已保留原位"
        )

    def record_problem(source_path: str, reason: str, target_path: str | None = None) -> None:
        existing = next(
            (item for item in problem_files if item.source_path == source_path), None
        )
        if existing is None:
            problem_files.append(
                PlannedProblem(
                    source_path=source_path,
                    reason=reason,
                    target_path=target_path,
                )
            )
            return
        if reason not in existing.reason:
            existing.reason = f"{existing.reason}；{reason}"
        if target_path:
            existing.target_path = target_path
    groups = (
        parse_ep_files(
            media_files,
            prefer_simplified=True,
            defer_unnumbered_specials=auto_special_title_match,
            allow_release_dash_ordinal=allow_release_dash_ordinal,
        )
        if prefer_simplified
        else all_groups
    )
    preferred_excluded_subtitles: set[str] = set()
    preferred_excluded_subtitle_paths: list[str] = []
    if prefer_simplified and groups is not all_groups:
        preferred_paths = {
            _collision_key(str(item.get("full_path", "")))
            for items in groups.values()
            for item in items
        }
        preferred_excluded_subtitles = {
            _collision_key(str(item.get("full_path", "")))
            for items in all_groups.values()
            for item in items
            if Path(str(item.get("name", ""))).suffix.lower() in SUBTITLE_EXTS
            and is_traditional_sub(str(item.get("name", "")))
            and _collision_key(str(item.get("full_path", ""))) not in preferred_paths
        }
        if preferred_excluded_subtitles:
            preferred_excluded_subtitle_paths = sorted({
                normalize_remote_path(str(item.get("full_path", "")))
                for items in all_groups.values()
                for item in items
                if _collision_key(str(item.get("full_path", "")))
                in preferred_excluded_subtitles
            }, key=_collision_key)
            warnings.append(
                f"{len(preferred_excluded_subtitles)} 个繁体字幕已有同发行简体对应；"
                "备选字幕已保留在源目录"
            )
    if auto_special_title_match and groups is not all_groups:
        _remap_postseason_oav_suffix(groups, special_titles)
        _remap_numbered_specials_by_official_label(groups, special_titles)
        _map_unnumbered_specials(
            media_files,
            groups,
            special_titles,
            title,
            season=season,
            special_season_candidates=special_season_candidates,
        )
        _map_unnumbered_special_from_subtitle_title(
            alist,
            media_files,
            groups,
            special_titles,
            series_titles=[
                str(show.get("name") or ""),
                str(show.get("original_name") or ""),
            ],
            regular_episode_count=sum(
                1
                for key, (target_season, _target_episode, _title) in episode_map.items()
                if key.kind == "regular" and target_season == season
            ),
            tmdb_client=tmdb_client,
            tmdb_id=tmdb_id,
            season=season,
        )
    if auto_align_subtitles and groups is not all_groups:
        _align_subtitles_to_video_sequence(groups)
    if not groups:
        raise PlanError("未找到可识别的剧集媒体文件")

    fractional_titles = (
        _multilingual_episode_titles(tmdb_client, tmdb_id, episode_map)
        if any(key.kind == "fractional" for key in groups)
        else {}
    )

    zero_key = EpisodeKey("regular", 0)
    if not absolute and zero_key in groups:
        source_regular_numbers = sorted({
            key.number
            for key in groups
            if key.kind == "regular" and not key.end_number
        })
        official_regular_numbers = sorted({
            key.number
            for key, (target_season, _target_episode, _title) in episode_map.items()
            if key.kind == "regular" and target_season == season
        })
        # Some disc releases number a complete season as 00..N-1 even though
        # TMDB uses 01..N (High School DxD Hero is a real example).  The exact
        # complete boundary proves a zero-based sequence; an isolated E00 or a
        # release that already contains E01..EN still follows the conservative
        # special/prologue evidence path below.
        if (
            official_regular_numbers
            and len(official_regular_numbers) >= 2
            and official_regular_numbers
            == list(range(1, len(official_regular_numbers) + 1))
            and source_regular_numbers
            == list(range(0, len(official_regular_numbers)))
        ):
            shifted_groups: dict[EpisodeKey, list[dict[str, Any]]] = {}
            for key, items in groups.items():
                shifted_key = (
                    EpisodeKey("regular", key.number + 1)
                    if key.kind == "regular" and not key.end_number
                    else key
                )
                shifted_groups.setdefault(shifted_key, []).extend(items)
            groups = shifted_groups
            warnings.append(
                f"源季完整使用零基编号 00–"
                f"{len(official_regular_numbers) - 1:02d}；已按 TMDB 完整边界转换为 "
                f"S{season:02d}E01–E{len(official_regular_numbers):02d}"
            )
    if not absolute and zero_key in groups:
        multilingual_titles = _multilingual_episode_titles(
            tmdb_client,
            tmdb_id,
            episode_map,
        )
        e00_candidates = [
            key
            for key in _e00_special_candidates(
                groups[zero_key],
                multilingual_titles,
                title,
            )
            if key in episode_map and key not in groups
        ]
        evidence = "TMDB 多语言标题"
        if not e00_candidates:
            e00_candidates = [
                key
                for key in _unique_season_referenced_special_candidate(
                    multilingual_titles,
                    season,
                )
                if key in episode_map and key not in groups
            ]
            evidence = f"TMDB 多语言官方第 {season} 季标题"
        if not e00_candidates:
            e00_candidates = [
                key
                for key in _unique_official_episode_zero_candidate(
                    multilingual_titles
                )
                if key in episode_map and key not in groups
            ]
            evidence = "TMDB 多语言官方「Episode 0/第 0 话」标题"
        regular_source_numbers = {
            key.number
            for key in groups
            if key.kind == "regular" and key.number > 0 and not key.end_number
        }
        official_regular_numbers = {
            key.number
            for key, (target_season, _target_episode, _title) in episode_map.items()
            if key.kind == "regular" and target_season == season
        }
        if (
            not e00_candidates
            and regular_source_numbers
            and regular_source_numbers == official_regular_numbers
        ):
            e00_candidates = [
                key
                for key in _e00_timeline_candidates(
                    tmdb_client,
                    tmdb_id,
                    season,
                )
                if key in episode_map and key not in groups
            ]
            evidence = "TMDB 官方开播时间与完整时长"
        if len(e00_candidates) == 1:
            target_key = e00_candidates[0]
            groups[target_key] = groups.pop(zero_key)
            warnings.append(
                f"已检索{evidence}并唯一确认源文件 E00 对应 "
                f"{target_key.display}（序章/第 0 话）"
            )

    if auto_special_title_match:
        warnings.extend(
            _map_unique_remaining_unnumbered_special_by_runtime(
                tmdb_client,
                tmdb_id,
                media_files,
                groups,
                special_titles,
            )
        )
        # The first pass intentionally runs before E00 evidence gathering.
        # Rebuild the unresolved list now so a newly evidenced SP is not still
        # shown as a stale problem.
        warnings = [
            warning
            for warning in warnings
            if "个无法唯一识别的附加视频已保留原位" not in warning
            and "个无对应视频或无法唯一编号的字幕已保留原位" not in warning
            and "个无法唯一识别的附加视频将在执行后自动隔离到" not in warning
            and "个无对应视频或无法唯一编号的字幕将在执行后自动隔离到" not in warning
        ]
        unparsed_paths = [
            path
            for path in _unparsed_media_paths(media_files, groups)
            if _collision_key(path) not in preferred_excluded_subtitles
        ]
        unparsed_videos = [
            path for path in unparsed_paths if Path(path).suffix.lower() in VIDEO_EXTS
        ]
        retained_subtitles = [
            path
            for path in unparsed_paths
            if Path(path).suffix.lower() in SUBTITLE_EXTS
        ]
        problem_files = [
            *(
                PlannedProblem(
                    source_path=path,
                    reason="无法唯一识别的附加视频；保留原位并标记规划未闭合",
                )
                for path in unparsed_videos
            ),
        ]
        if unparsed_videos:
            warnings.append(
                f"{len(unparsed_videos)} 个无法唯一识别的附加视频将保留于"
                "源目录自动规划未闭合；"
                "其余可确认媒体仍会正常整理"
            )
        if retained_subtitles:
            warnings.append(
                f"{len(retained_subtitles)} 个无对应视频或无法唯一编号的字幕"
                "将保留原位并标记规划未闭合，不影响其余媒体整理"
            )

    planned: list[PlannedFile] = []
    unresolved: list[str] = []
    regular_episode_max = max(
        (
            key.number
            for key, mapped_value in episode_map.items()
            if key.kind == "regular" and mapped_value[0] == season
        ),
        default=0,
    )

    for key in sorted(groups):
        diagnostic_reason: str | None = None
        fractional_match: tuple[
            int, int, str, tuple[str, ...], tuple[str, ...]
        ] | None = None
        if key.kind == "fractional":
            fractional_candidates, fractional_resolution_reason = (
                _fractional_recap_evidence_candidates(
                    tmdb_client,
                    tmdb_id,
                    season,
                    key,
                    fractional_titles,
                    groups[key],
                )
            )
            if len(fractional_candidates) == 1:
                fractional_match = fractional_candidates[0]
            else:
                if fractional_candidates:
                    candidate_text = "、".join(
                        f"S{target_season:02d}E{target_episode:02d}「{title}」"
                        for target_season, target_episode, title, _aliases, _evidence
                        in fractional_candidates
                    )
                    reason = (
                        f"{key.display} 的 TMDB 多证据评分出现多个近分候选："
                        f"{candidate_text}；{fractional_resolution_reason}；"
                        "未自动猜测；保留原位并标记规划未闭合"
                    )
                else:
                    reason = (
                        f"{key.display} 未通过 TMDB 官方标题/别名、时间线、季度归属、"
                        f"源标题语义、运行时长、唯一性和冲突证据门禁："
                        f"{fractional_resolution_reason}；保留于"
                        "源目录自动规划未闭合"
                    )
                for item in groups[key]:
                    record_problem(normalize_remote_path(str(item["full_path"])), reason)
                warnings.append(reason)
                continue
        preferred_group, lower_resolution_videos = _prefer_highest_resolution_videos(
            groups[key], prefer_simplified=prefer_simplified
        )
        if lower_resolution_videos:
            groups[key] = preferred_group
            for item in lower_resolution_videos:
                source_path = normalize_remote_path(str(item["full_path"]))
                source_dir, original_name = split_remote(source_path)
                preferred_source = str(item["_preferred_resolution_source"])
                cleanup_kind = str(
                    item.get("_duplicate_cleanup_kind", "lower_resolution")
                )
                if cleanup_kind == "burned_subtitle_duplicate":
                    reason = _burned_subtitle_cleanup_reason(preferred_source)
                elif cleanup_kind == "traditional_language_duplicate":
                    reason = _traditional_language_cleanup_reason(preferred_source)
                elif cleanup_kind == "same_resolution_duplicate":
                    reason = _same_resolution_cleanup_reason(preferred_source)
                elif cleanup_kind == "lower_resolution_subtitle":
                    reason = _lower_resolution_subtitle_cleanup_reason(preferred_source)
                else:
                    reason = _lower_resolution_cleanup_reason(preferred_source)
                cleanup_files.append(
                    PlannedCleanup(
                        source_path=source_path,
                        source_dir=source_dir,
                        original_name=original_name,
                        reason=reason,
                        source_size=_entry_size_value(item),
                        source_modified=_entry_modified_value(item),
                    )
                )
            lower_count = sum(
                item.get("_duplicate_cleanup_kind") == "lower_resolution"
                for item in lower_resolution_videos
            )
            burned_count = sum(
                item.get("_duplicate_cleanup_kind") == "burned_subtitle_duplicate"
                for item in lower_resolution_videos
            )
            same_resolution_count = sum(
                item.get("_duplicate_cleanup_kind") == "same_resolution_duplicate"
                for item in lower_resolution_videos
            )
            lower_subtitle_count = sum(
                item.get("_duplicate_cleanup_kind") == "lower_resolution_subtitle"
                for item in lower_resolution_videos
            )
            if lower_count:
                warnings.append(
                    f"{key.display} 存在同一 TMDB 集号的多个清晰度版本；"
                    f"已优先保留最高可确认清晰度，并计划清理 "
                    f"{lower_count} 个低清晰度重复版本"
                )
            if burned_count:
                warnings.append(
                    f"{key.display} 存在同清晰度的内封/软字幕与内嵌/硬字幕版本；"
                    f"已保留可切换字幕版本，并计划清理 {burned_count} 个硬字幕重复视频"
                )
            if same_resolution_count:
                warnings.append(
                    f"{key.display} 存在同清晰度的重复发布版；"
                    f"已保留文件更完整的版本，并计划清理 "
                    f"{same_resolution_count} 个较小重复视频"
                )
            if lower_subtitle_count:
                warnings.append(
                    f"{key.display} 已计划清理 {lower_subtitle_count} 个"
                    "仅属于低清发布版的重复字幕"
                )
        explicit_special_context = any(_has_special_context(item) for item in groups[key])
        override = episode_overrides.get(key)
        if override is not None:
            override_season, override_episode, override_end = override
            mapped = (
                override_season,
                override_episode,
                override_official_titles[(override_season, override_episode)],
            )
            mapped_end = None
            if override_end:
                mapped_end = (
                    override_season,
                    override_end,
                    override_official_titles[(override_season, override_end)],
                )
            warnings.append(f"{key.display} 使用显式覆盖映射")
        else:
            if fractional_match is not None:
                (
                    target_season,
                    target_episode,
                    target_title,
                    _evidence_titles,
                    mapping_evidence,
                ) = fractional_match
                mapped = (target_season, target_episode, target_title)
                warnings.append(
                    f"{key.display} 经 TMDB 官方 Season 00/季度多证据评分"
                    "（含多语言季度/特别篇标题）"
                    f"自动映射为 S{target_season:02d}E{target_episode:02d}"
                    f"「{target_title}」；证据：{'、'.join(mapping_evidence)}"
                )
            else:
                lookup_key = EpisodeKey(key.kind, key.number)
                mapped = episode_map.get(lookup_key)
                if (
                    mapped is not None
                    and key.kind == "special"
                    and _numbered_physical_special_markers(groups[key])
                    and not _oav_numbered_special_has_official_evidence(
                        key,
                        groups[key],
                        season=season,
                        special_titles=special_titles,
                        special_season_candidates=special_season_candidates,
                    )
                ):
                    # OAD/OVA/OAV numbering is release-local: a coincident
                    # TMDB Season 00 index proves nothing about identity.
                    # Only official Season 00 identity/title/work-relationship
                    # evidence may map it; otherwise fail closed below.
                    mapped = None
            mapped_end = (
                episode_map.get(EpisodeKey(key.kind, key.end_number))
                if key.end_number
                else None
            )
        title_overrides = {
            str(item.get("_episode_title_override"))
            for item in groups[key]
            if isinstance(item.get("_episode_title_override"), str)
            and str(item.get("_episode_title_override")).strip()
        }
        if mapped is not None and len(title_overrides) == 1:
            mapped = (mapped[0], mapped[1], next(iter(title_overrides)))
        if mapped is None:
            overflow_special = (
                _season_overflow_special(
                    special_titles,
                    season=season,
                    ordinal=key.number - regular_episode_max,
                    explicit_special_context=explicit_special_context,
                    special_season_candidates=special_season_candidates,
                )
                if key.kind == "regular"
                and not absolute
                and regular_episode_max > 0
                and key.number > regular_episode_max
                else None
            )
            if overflow_special is not None:
                special_key, special_title = overflow_special
                mapped = (0, special_key.number, special_title)
                warnings.append(
                    f"{key.display} 超出第 {season} 季正片集数；根据 TMDB 标题映射为 {special_key.display}"
                )
        if mapped is None:
            if key.kind == "special":
                physical_markers = _physical_special_markers(groups[key])
                if physical_markers:
                    marker_label = "/".join(sorted(physical_markers))
                    reason = (
                        f"{marker_label} {key.display} 未在 TMDB 官方特别篇中唯一确认身份；"
                        "发行形态不能单独证明 Season 00 归属，"
                        f"源编号也不能机械改写为 S00E{key.number:02d}；"
                        "保留原位并标记规划未闭合"
                    )
                else:
                    reason = (
                        f"{key.display} 未在 TMDB 官方特别篇中找到对应集；"
                        "源编号不能证明目标 Season 00 集号；保留于"
                        "源目录自动规划未闭合"
                    )
                for item in groups[key]:
                    record_problem(
                        normalize_remote_path(str(item["full_path"])),
                        reason,
                    )
                warnings.append(reason)
                continue
            elif (
                key.kind == "regular"
                and not absolute
                and regular_episode_max > 0
                and regular_episode_max < key.number <= regular_episode_max + 3
            ):
                reason = (
                    f"{key.display} 超出第 {season} 季官方正片集数，且未检索到"
                    "唯一官方特别篇映射；不能把源编号当作 Season 00 集号；"
                    "保留原位并标记规划未闭合"
                )
                for item in groups[key]:
                    record_problem(
                        normalize_remote_path(str(item["full_path"])),
                        reason,
                    )
                warnings.append(reason)
                continue
            elif not allow_unmapped:
                unresolved.append(key.display)
                continue
            elif key.kind == "fractional":
                unresolved.append(key.display)
                continue
            elif absolute:
                unresolved.append(key.display)
                continue
            else:
                mapped = (season, key.number, f"第{key.number}集")
                diagnostic_reason = f"{key.display} 未在 TMDB 中找到，将使用回退名称"
                warnings.append(f"{key.display} 未在 TMDB 中找到，使用回退名称")

        if key.end_number:
            if mapped_end is None:
                if allow_unmapped and not absolute and key.kind == "regular":
                    mapped_end = (season, key.end_number, f"第{key.end_number}集")
                    warnings.append(
                        f"{key.display} 的结束集未在 TMDB 中找到，使用回退名称"
                    )
                else:
                    unresolved.append(key.display)
                    continue
            if mapped[0] != mapped_end[0]:
                raise PlanError(
                    f"多集文件 {key.display} 跨越 TMDB 季度 "
                    f"S{mapped[0]:02d} → S{mapped_end[0]:02d}，拒绝生成含糊名称；"
                    "请拆分文件或提供单独覆盖映射。"
                )

        target_season, target_episode, raw_episode_title = mapped
        episode_title = provider_safe_episode_title(raw_episode_title)
        episode_token = f"S{target_season:02d}E{target_episode:02d}"
        if mapped_end is not None:
            episode_token += f"-E{mapped_end[1]:02d}"
            episode_title = provider_safe_episode_title(
                f"{raw_episode_title} + {mapped_end[2]}"
            )
        base_name = f"{title} - {episode_token} - {episode_title}"
        group_files = sorted(groups[key], key=lambda x: _collision_key(str(x["full_path"])))
        part_numbers = sorted({
            int(item["_episode_part_override"])
            for item in group_files
            if isinstance(item.get("_episode_part_override"), int)
            and not isinstance(item.get("_episode_part_override"), bool)
            and int(item["_episode_part_override"]) > 0
        })
        group_videos = [
            item
            for item in group_files
            if Path(str(item.get("name", ""))).suffix.lower() in VIDEO_EXTS
        ]
        split_episode = (
            len(part_numbers) >= 2
            and group_videos
            and all(
                isinstance(item.get("_episode_part_override"), int)
                and not isinstance(item.get("_episode_part_override"), bool)
                and int(item["_episode_part_override"]) in part_numbers
                for item in group_videos
            )
        )
        if split_episode:
            final_name_by_path: dict[str, str] = {}
            for part_number in part_numbers:
                part_files = [
                    item
                    for item in group_files
                    if item.get("_episode_part_override") == part_number
                ]
                part_names = make_unique_media_names(
                    f"{base_name} - part{part_number}",
                    part_files,
                    preserve_editions=True,
                )
                for item, part_name in zip(part_files, part_names):
                    final_name_by_path[str(item["full_path"])] = part_name
            final_names = [
                final_name_by_path.get(str(item["full_path"]))
                or make_unique_media_names(
                    base_name, [item], preserve_editions=True
                )[0]
                for item in group_files
            ]
        else:
            final_names = make_unique_media_names(
                base_name,
                group_files,
                preserve_editions=True,
            )
        season_dir = join_remote(series_dir, f"Season {target_season:02d}")
        planned_group: list[PlannedFile] = []
        for item, final_name in zip(group_files, final_names):
            planned_item = _planned_file_from_entry(
                item,
                final_name=final_name,
                target_dir=season_dir,
                episode_key=key.display,
            )
            if isinstance(planned_item.subtitle_validation, Mapping):
                proof = dict(planned_item.subtitle_validation)
                proof["source_coordinate"] = key.display
                proof["target_coordinate"] = episode_token
                planned_item.subtitle_validation = proof
            planned.append(planned_item)
            planned_group.append(planned_item)
            if diagnostic_reason:
                record_problem(
                    planned_item.source_path,
                    diagnostic_reason,
                    join_remote(planned_item.target_dir, planned_item.final_name),
                )
        video_versions = [item for item in planned_group if item.media_kind == "video"]
        video_editions = [
            edition_tag(item.final_name) or edition_tag(item.source_path)
            for item in video_versions
        ]
        distinct_named_editions = (
            len(video_versions) > 1
            and len(set(video_editions)) == len(video_versions)
            and any(video_editions)
        )
        if len(video_versions) > 1 and not distinct_named_editions and not split_episode:
            duplicate_reason = (
                f"同一集检测到 {len(video_versions)} 个视频版本，"
                "已自动保留为主文件及 v2/v3 多发行版，无需额外处理"
            )
            for item in video_versions:
                record_problem(
                    item.source_path,
                    duplicate_reason,
                    join_remote(item.target_dir, item.final_name),
                )

    bonus_counts: dict[str, int] = defaultdict(int)
    for item in sorted(
        tv_bonus_files,
        key=lambda value: _collision_key(str(value.get("full_path", ""))),
    ):
        kind = bonus_type(str(item.get("name", ""))) or "other"
        bonus_counts[kind] += 1
        serial = "" if bonus_counts[kind] == 1 else str(bonus_counts[kind])
        final_name = _compose_filename(
            series_label,
            f"-{kind}{serial}",
            Path(str(item.get("name", ""))).suffix.lower(),
        )
        planned.append(
            _planned_file_from_entry(
                item,
                final_name=final_name,
                target_dir=series_dir,
            )
        )
    if tv_bonus_files:
        warnings.append(
            f"{len(tv_bonus_files)} 个明确位于特典目录的幕后/访谈/花絮视频"
            "已按 Infuse Extras 命名保留，不作为正片集号"
        )

    if unresolved:
        unresolved_text = ", ".join(sorted(set(unresolved)))
        raise PlanError(
            f"以下集数未在 TMDB 映射中找到，已停止以避免错误归档: {unresolved_text}。"
            "可核对文件名或在非绝对集数模式下使用 --allow-unmapped。"
        )

    if subtitle_alignment_applied:
        existing_problem_paths = {item.source_path for item in problem_files}
        for item in planned:
            if item.media_kind != "subtitle" or item.source_path in existing_problem_paths:
                continue
            record_problem(
                item.source_path,
                "字幕发布序号已按连续视频集号自动对齐并记录差异",
                join_remote(item.target_dir, item.final_name),
            )

    cleanup_files = sorted(
        {
            _collision_key(item.source_path): item
            for item in cleanup_files
        }.values(),
        key=lambda item: _collision_key(item.source_path),
    )

    plan = Plan(
        mode="tv",
        source_root=normalize_remote_path(src_path),
        target_root=series_dir,
        files=planned,
        warnings=warnings,
        metadata={
            "tmdb_id": tmdb_id,
            "title": title,
            "original_title": original_title,
            "year": year,
            "poster_path": show.get("poster_path"),
            "backdrop_path": show.get("backdrop_path"),
            "season_posters": {
                str(int(item["season_number"])): str(item["poster_path"])
                for item in (show.get("seasons") or [])
                if isinstance(item, Mapping)
                and not isinstance(item.get("season_number"), bool)
                and item.get("season_number") is not None
                and isinstance(item.get("poster_path"), str)
                and item.get("poster_path")
            },
            "season": season,
            "absolute": absolute,
            "episode_group": episode_group_id,
            "exported_srt_normalizations": [
                dict(item["_subtitle_normalization"])
                for item in files
                if isinstance(item.get("_subtitle_normalization"), Mapping)
            ],
        },
        cleanup_files=cleanup_files,
        problem_files=problem_files,
        # ``parse_ep_files(..., prefer_simplified=True)`` is the existing
        # subtitle preference selector. Its intentionally excluded traditional
        # companion is a retained alternative, not an unresolved identity or
        # pairing error. Keep it visible in the scan report without turning it
        # into a problem-file gate that would stop the verified media plan.
        scan_report={
            "deferred_subtitles": [
                *(
                    {
                        "source_path": path,
                        "action": "preserve_at_source",
                        "reason": "preferred_simplified_subtitle",
                    }
                    for path in preferred_excluded_subtitle_paths
                ),
                *(
                    {
                        "source_path": issue["source_path"],
                        "action": "preserve_at_source",
                        "reason": "invalid_exported_srt",
                        "detail": issue["reason"],
                    }
                    for issue in exported_srt_issues
                ),
            ],
        } if preferred_excluded_subtitle_paths or exported_srt_issues else {},
    )
    if not absolute and season > 0:
        try:
            completeness_payload = tmdb_client.get(f"/tv/{tmdb_id}/season/{season}")
        except ApiError:
            completeness_payload = {}
        if isinstance(completeness_payload, Mapping):
            episode_gaps = _tv_episode_resource_gaps(
                alist,
                plan,
                series_dir=series_dir,
                season=season,
                official_episodes=[
                    item
                    for item in (completeness_payload.get("episodes") or [])
                    if isinstance(item, Mapping)
                ],
            )
            if episode_gaps:
                plan.scan_report["resource_gaps"] = episode_gaps
    _add_snapshot_warnings(plan)
    validate_plan(
        alist,
        plan,
        media_root=media_root,
    )
    return plan


def _combine_plans_as_batch(source_root: str, plans: Sequence[Plan], warning: str) -> Plan:
    member_tv: dict[str, dict[str, Any]] = {}
    member_movies: dict[str, dict[str, Any]] = {}
    member_posters: dict[str, str] = {}
    for plan in plans:
        if plan.mode in {"tv", "mixed"}:
            series_root = str(plan.metadata.get("series_root") or plan.target_root)
            member_tv[series_root] = {
                key: plan.metadata.get(key)
                for key in (
                    "tmdb_id", "title", "original_title", "year", "poster_path", "backdrop_path",
                    "season_posters",
                )
            }
        raw_movies = plan.metadata.get("member_movies")
        raw_posters = plan.metadata.get("member_posters")
        if plan.mode == "movie":
            member_movies[plan.target_root] = {
                "tmdb_id": plan.metadata["tmdb_id"],
                "title": plan.metadata["title"],
                "year": plan.metadata["year"],
            }
        if isinstance(raw_movies, Mapping):
            member_movies.update({str(key): dict(value) for key, value in raw_movies.items()})
        if isinstance(raw_posters, Mapping):
            member_posters.update({str(key): str(value) for key, value in raw_posters.items()})
    target_root = normalize_remote_path(posixpath.commonpath([plan.target_root for plan in plans]))
    resource_gaps = [
        dict(gap)
        for plan in plans
        for gap in (plan.scan_report.get("resource_gaps") or [])
        if isinstance(gap, Mapping)
    ]
    return Plan(
        mode="batch",
        source_root=source_root,
        target_root=target_root,
        files=[item for plan in plans for item in plan.files],
        cleanup_files=_dedupe_cleanup_files(
            item for plan in plans for item in plan.cleanup_files
        ),
        problem_files=[item for plan in plans for item in plan.problem_files],
        warnings=list(dict.fromkeys([warning, *(item for plan in plans for item in plan.warnings)])),
        metadata={
            "title": split_remote(source_root)[1],
            "member_tv": member_tv,
            "member_movies": member_movies,
            "member_posters": member_posters,
        },
        scan_report={"resource_gaps": resource_gaps} if resource_gaps else {},
    )


def _canonical_plan_identity(plan: Plan) -> WorkIdentity | None:
    """Return the singular metadata identity carried by a leaf sub-plan."""
    tmdb_id = plan.metadata.get("tmdb_id")
    if not isinstance(tmdb_id, int) or isinstance(tmdb_id, bool) or tmdb_id <= 0:
        return None
    namespace = {
        "tv": "tmdb.tv",
        "movie": "tmdb.movie",
    }.get(plan.mode)
    return WorkIdentity(namespace, tmdb_id) if namespace is not None else None


def _canonical_leaf_name(
    namespace: str,
    title: str,
    identity: Mapping[str, Any],
) -> str:
    """Derive a work leaf only from confirmed canonical metadata."""
    if namespace == "tmdb.movie":
        year = identity.get("year")
        if isinstance(year, (str, int)) and str(year).strip():
            return safe_name(f"{title} ({str(year).strip()})")
    return safe_name(title)


def _canonical_plan_bindings(
    plan: Plan,
) -> list[tuple[WorkIdentity, str, str, str, str | None]]:
    """Return identity/title/canonical leaf/old root/poster per work."""
    singular = _canonical_plan_identity(plan)
    if singular is not None:
        title = plan.metadata.get("title")
        if isinstance(title, str) and title.strip():
            poster = plan.metadata.get("poster_path")
            return [(
                singular,
                title.strip(),
                _canonical_leaf_name(singular.namespace, title.strip(), plan.metadata),
                normalize_remote_path(plan.target_root),
                str(poster) if isinstance(poster, str) and poster else None,
            )]

    bindings: list[tuple[WorkIdentity, str, str, str, str | None]] = []
    if plan.mode == "mixed":
        tmdb_id = plan.metadata.get("tmdb_id")
        title = plan.metadata.get("title")
        series_root = plan.metadata.get("series_root")
        if (
            isinstance(tmdb_id, int)
            and not isinstance(tmdb_id, bool)
            and tmdb_id > 0
            and isinstance(title, str)
            and title.strip()
            and isinstance(series_root, str)
        ):
            raw_poster = plan.metadata.get("poster_path")
            bindings.append((
                WorkIdentity("tmdb.tv", tmdb_id),
                title.strip(),
                _canonical_leaf_name("tmdb.tv", title.strip(), plan.metadata),
                normalize_remote_path(series_root),
                str(raw_poster)
                if isinstance(raw_poster, str) and raw_poster
                else None,
            ))
    member_posters = plan.metadata.get("member_posters")
    poster_by_root = (
        {normalize_remote_path(str(root)): str(path) for root, path in member_posters.items()}
        if isinstance(member_posters, Mapping)
        else {}
    )
    for field, namespace in (("member_tv", "tmdb.tv"), ("member_movies", "tmdb.movie")):
        rows = plan.metadata.get(field)
        if not isinstance(rows, Mapping):
            continue
        for raw_root, raw_identity in rows.items():
            if not isinstance(raw_identity, Mapping):
                continue
            tmdb_id = raw_identity.get("tmdb_id")
            title = raw_identity.get("title")
            if (
                not isinstance(tmdb_id, int)
                or isinstance(tmdb_id, bool)
                or tmdb_id <= 0
                or not isinstance(title, str)
                or not title.strip()
            ):
                continue
            root = normalize_remote_path(str(raw_root))
            raw_poster = raw_identity.get("poster_path")
            poster = (
                str(raw_poster)
                if isinstance(raw_poster, str) and raw_poster
                else poster_by_root.get(root)
            )
            bindings.append((
                WorkIdentity(namespace, tmdb_id),
                title.strip(),
                _canonical_leaf_name(namespace, title.strip(), raw_identity),
                root,
                poster,
            ))
    return bindings


def _rebase_plan_identity_roots(
    plan: Plan,
    root_map: Mapping[str, str],
) -> None:
    """Rebase several possibly nested identity leaves in one atomic mapping."""
    mappings = sorted(
        (
            (normalize_remote_path(old), normalize_remote_path(new))
            for old, new in root_map.items()
            if _collision_key(old) != _collision_key(new)
        ),
        key=lambda pair: (-len(pair[0]), _collision_key(pair[0])),
    )
    if not mappings:
        return

    def rebase(path: str) -> str:
        normalized = normalize_remote_path(path)
        for old, new in mappings:
            if _path_is_within(normalized, old):
                suffix = normalized[len(old):].lstrip("/")
                return join_remote(new, suffix) if suffix else new
        return normalized

    for item in plan.files:
        item.target_dir = rebase(item.target_dir)
    for item in plan.problem_files:
        if item.target_path:
            item.target_path = rebase(item.target_path)
    if any(_collision_key(plan.target_root) == _collision_key(old) for old, _new in mappings):
        plan.target_root = rebase(plan.target_root)
    series_root = plan.metadata.get("series_root")
    if isinstance(series_root, str):
        plan.metadata["series_root"] = rebase(series_root)
    for field in ("member_posters", "member_movies", "member_tv"):
        rows = plan.metadata.get(field)
        if isinstance(rows, Mapping):
            plan.metadata[field] = {rebase(str(path)): value for path, value in rows.items()}


def _plan_canonical_batch_tree(
    plans: Sequence[Plan],
    *,
    outer_root: str,
    root_identity: WorkIdentity | None = None,
    allow_family_boundaries: bool = True,
    identities: set[WorkIdentity] | None = None,
    preserve_container_root: bool = False,
) -> tuple[str, list[str], dict[str, str]]:
    """Route all identity-bearing sub-plans through the sole tree planner."""
    keyed_bindings: dict[str, tuple[Plan, str]] = {}
    works: list[CanonicalWork] = []
    for index, plan in enumerate(plans):
        for binding_index, (identity, title, leaf_name, old_root, poster) in enumerate(
            _canonical_plan_bindings(plan)
        ):
            if identities is not None and identity not in identities:
                continue
            member_key = (
                f"{index}:{binding_index}:{normalize_remote_path(plan.source_root)}:"
                f"{old_root}"
            )
            keyed_bindings[member_key] = (plan, old_root)
            works.append(CanonicalWork(
                member_key=member_key,
                identity=identity,
                title=safe_name(title),
                leaf_name=leaf_name,
                poster_path=poster,
            ))
    if not works:
        return normalize_remote_path(outer_root), [], {}
    try:
        tree = plan_canonical_work_tree(
            works,
            container_root=normalize_remote_path(outer_root),
            root_identity=root_identity,
            allow_family_boundaries=allow_family_boundaries,
        )
    except CanonicalTreeError as exc:
        raise PlanError(f"作品树无法唯一确定: {exc}") from exc

    per_plan_roots: dict[int, tuple[Plan, dict[str, str]]] = {}
    for placement in tree.placements:
        binding_plan, old_root = keyed_bindings[placement.member_key]
        _plan, roots = per_plan_roots.setdefault(
            id(binding_plan), (binding_plan, {})
        )
        roots[old_root] = placement.target_root
    for binding_plan, roots in per_plan_roots.values():
        _rebase_plan_identity_roots(binding_plan, roots)
        if identities is None:
            binding_plan.target_root = (
                tree.container_root
                if preserve_container_root
                else normalize_remote_path(posixpath.commonpath(list(roots.values())))
            )

    family_posters: dict[str, str] = {}
    for family_root in tree.family_roots:
        candidates = sorted(
            (
                (placement, keyed_bindings[placement.member_key][0])
                for placement in tree.placements
                if _path_is_within(placement.target_root, family_root)
            ),
            key=lambda pair: (
                normalize_remote_path(pair[0].target_root).count("/"),
                _collision_key(str(pair[1].metadata.get("title") or "")),
            ),
        )
        for _placement, candidate_plan in candidates:
            poster = candidate_plan.metadata.get("poster_path")
            if isinstance(poster, str) and poster:
                family_posters[family_root] = poster
                break
    warnings = [
        f"已识别子系列 {split_remote(path)[1]!r}；"
        "canonical 作品树已保持每个独立 TMDB 身份的叶子边界"
        for path in tree.family_roots
    ]
    return tree.container_root, warnings, family_posters


def _confirmed_movie_roots(plan: Plan) -> dict[str, int]:
    """Return target roots carrying an explicit TMDB movie identity."""
    roots: dict[str, int] = {}
    if plan.mode == "movie":
        tmdb_id = plan.metadata.get("tmdb_id")
        if isinstance(tmdb_id, int) and not isinstance(tmdb_id, bool) and tmdb_id > 0:
            roots[normalize_remote_path(plan.target_root)] = tmdb_id
    raw_movies = plan.metadata.get("member_movies")
    if isinstance(raw_movies, Mapping):
        for raw_root, identity in raw_movies.items():
            if not isinstance(identity, Mapping):
                continue
            tmdb_id = identity.get("tmdb_id")
            if isinstance(tmdb_id, int) and not isinstance(tmdb_id, bool) and tmdb_id > 0:
                roots[normalize_remote_path(str(raw_root))] = tmdb_id
    return roots


def _batch_movie_source_variant_key(
    original_name: str,
    source_path: str,
    final_name: str = "",
) -> str:
    """Keep movie parts, named editions and title extras in separate buckets."""
    final_stem = Path(final_name).stem
    if final_name and is_planned_bonus(final_name):
        return f"bonus:{_collision_key(source_path)}"
    release_text = f"{original_name} {source_path} {final_name}"
    if re.search(
        r"(?:^|[\s._\-\[\]()])(?:NC)?(?:OP|ED)(?:\d+)?(?:$|[\s._\-\[\]()])",
        release_text,
        re.IGNORECASE,
    ):
        return f"theme:{_collision_key(source_path)}"
    part_match = re.search(r"(?:^|[\s._\-])part\s*0*(\d+)(?:$|[\s._\-])", final_stem, re.I)
    part = f"part:{int(part_match.group(1))}" if part_match else "main"
    edition = entry_edition_tag({"name": original_name, "full_path": source_path})
    return f"{part}|edition:{_collision_key(edition) if edition else ''}"


def _batch_movie_variant_key(item: PlannedFile) -> str:
    return _batch_movie_source_variant_key(
        item.original_name,
        item.source_path,
        item.final_name,
    )


def _retarget_transitive_movie_cleanups(
    plans: Sequence[Plan],
    removed_winner: PlannedFile,
    final_winner: PlannedFile,
    tmdb_id: int,
) -> int:
    """Retarget proven duplicates when their intermediate winner is removed."""
    removed_entry = {
        "name": removed_winner.original_name,
        "full_path": removed_winner.source_path,
    }
    removed_rank = video_resolution_rank(removed_entry)
    removed_edition = edition_tag(removed_winner.original_name) or edition_tag(
        removed_winner.source_path
    )
    removed_presentation = subtitle_presentation_rank(removed_entry)
    final_rank = video_resolution_rank({
        "name": final_winner.original_name,
        "full_path": final_winner.source_path,
    })
    final_variant = _batch_movie_variant_key(final_winner)
    retargeted = 0
    for plan in plans:
        for cleanup in plan.cleanup_files:
            loser_entry = {
                "name": cleanup.original_name,
                "full_path": cleanup.source_path,
            }
            loser_rank = video_resolution_rank(loser_entry)
            loser_edition = edition_tag(cleanup.original_name) or edition_tag(
                cleanup.source_path
            )
            loser_presentation = subtitle_presentation_rank(loser_entry)
            original_link_proven = False
            if cleanup.reason == _burned_subtitle_cleanup_reason(
                removed_winner.source_path
            ):
                original_link_proven = (
                    loser_edition == removed_edition
                    and loser_rank == removed_rank > 0
                    and loser_presentation < removed_presentation
                )
            elif cleanup.reason == _same_resolution_cleanup_reason(
                removed_winner.source_path
            ):
                original_link_proven = (
                    loser_edition == removed_edition
                    and loser_rank == removed_rank > 0
                    and loser_presentation == removed_presentation
                    and cleanup.source_size is not None
                    and removed_winner.source_size is not None
                    and removed_winner.source_size > cleanup.source_size > 0
                )
            elif cleanup.reason == _lower_resolution_cleanup_reason(
                removed_winner.source_path
            ):
                original_link_proven = (
                    loser_edition == removed_edition
                    and removed_rank > loser_rank
                    and (loser_rank > 0 or removed_rank >= 2160)
                )
            if not original_link_proven:
                continue
            if (
                final_rank <= loser_rank
                or _batch_movie_source_variant_key(
                    cleanup.original_name, cleanup.source_path
                ) != final_variant
                or not _same_concrete_movie_release(
                    cleanup.original_name, cleanup.source_path, final_winner
                )
            ):
                continue
            cleanup.reason = _lower_resolution_movie_cleanup_reason(
                final_winner.source_path, tmdb_id
            )
            retargeted += 1
    return retargeted


def _dedupe_confirmed_batch_movies(
    plans: Sequence[Plan],
    *,
    outer_root: str,
) -> list[str]:
    """Prefer the highest resolution across paths for the same TMDB movie.

    This deliberately requires identical confirmed TMDB movie IDs.  Titles
    never participate in identity.  Equal-resolution files, multipart movies,
    named cuts and OP/ED/bonus videos remain independent.
    """
    outer_root = normalize_remote_path(outer_root)
    roots_by_id: dict[int, set[str]] = defaultdict(set)
    for plan in plans:
        for root, tmdb_id in _confirmed_movie_roots(plan).items():
            if not _path_is_within(root, outer_root):
                continue
            roots_by_id[tmdb_id].add(root)

    warnings: list[str] = []
    for tmdb_id, roots in sorted(roots_by_id.items()):
        if len(roots) < 2:
            continue
        candidates: dict[str, list[tuple[Plan, PlannedFile]]] = defaultdict(list)
        for plan in plans:
            for item in plan.files:
                if item.media_kind != "video":
                    continue
                matching_roots = [
                    root
                    for root in roots
                    if _path_is_within(item.target_dir, root)
                ]
                if not matching_roots:
                    continue
                candidates[_batch_movie_variant_key(item)].append((plan, item))

        removed = 0
        for members in candidates.values():
            if len(members) < 2:
                continue
            ranks = {
                id(item): video_resolution_rank({
                    "name": item.original_name,
                    "full_path": item.source_path,
                })
                for _plan, item in members
            }
            best_rank = max(ranks.values(), default=0)
            if best_rank <= 0:
                continue
            preferred = min(
                (item for _plan, item in members if ranks[id(item)] == best_rank),
                key=lambda item: _collision_key(item.source_path),
            )
            for plan, item in members:
                rank = ranks[id(item)]
                if rank >= best_rank or not (rank > 0 or best_rank >= 2160):
                    continue
                _retarget_transitive_movie_cleanups(
                    plans,
                    item,
                    preferred,
                    tmdb_id,
                )
                plan.files.remove(item)
                plan.cleanup_files.append(
                    PlannedCleanup(
                        source_path=item.source_path,
                        source_dir=item.source_dir,
                        original_name=item.original_name,
                        reason=_lower_resolution_movie_cleanup_reason(
                            preferred.source_path,
                            tmdb_id,
                        ),
                        source_size=item.source_size,
                        source_modified=item.source_modified,
                    )
                )
                removed += 1

        _plan_canonical_batch_tree(
            plans,
            outer_root=outer_root,
            allow_family_boundaries=False,
            identities={WorkIdentity("tmdb.movie", tmdb_id)},
        )
        if removed:
            warnings.append(
                f"TMDB movie/{tmdb_id} 在多个源目录中存在同内容清晰度副本；"
                f"已统一到确认的电影目录并计划清理 {removed} 个低清视频"
            )
    return warnings


def _partition_movie_groups_with_video(
    movie_groups: Mapping[int, Sequence[Mapping[str, Any]]],
) -> tuple[dict[int, list[dict[str, Any]]], list[dict[str, Any]]]:
    """Keep subtitle-only movie matches visible without aborting a TV batch."""
    valid: dict[int, list[dict[str, Any]]] = {}
    orphan_files: list[dict[str, Any]] = []
    for tmdb_id, members in movie_groups.items():
        copied = [dict(item) for item in members]
        if any(
            Path(str(item.get("name", ""))).suffix.lower() in VIDEO_EXTS
            for item in copied
        ):
            valid[int(tmdb_id)] = copied
        else:
            orphan_files.extend(copied)
    return valid, orphan_files






def _load_collection_map(path: Path) -> dict[int, int]:
    try:
        raw = _load_json_text(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise PlanError(f"无法读取合集映射文件: {path}; {exc}") from exc
    if not isinstance(raw, dict):
        raise PlanError("合集映射文件必须是 JSON 对象，例如 {\"1\": 12345}")
    mapping: dict[int, int] = {}
    for key, value in raw.items():
        if not isinstance(key, str) or not re.fullmatch(r"[1-9]\d*", key):
            raise PlanError(f"合集映射的源编号必须是十进制正整数字符串: {key!r}")
        if isinstance(value, bool) or not isinstance(value, int):
            raise PlanError(
                f"合集映射的 TMDB ID 必须是 JSON 正整数，不能使用布尔值、"
                f"小数或字符串: {key!r}: {value!r}"
            )
        source_number = int(key)
        movie_id = value
        if source_number <= 0 or movie_id <= 0:
            raise PlanError(f"合集映射的编号和 TMDB ID 必须为正整数: {key!r}: {value!r}")
        if source_number in mapping:
            raise PlanError(f"合集映射存在重复编号: {key!r}")
        mapping[source_number] = movie_id
    return mapping


def _parse_episode_map_key(value: str) -> EpisodeKey:
    fractional = re.fullmatch(
        r"(?:E)?0*(\d{1,3})\.(\d{1,3})",
        value.strip(),
        re.IGNORECASE,
    )
    if fractional:
        number = int(fractional.group(1))
        if number <= 0:
            raise PlanError(f"无效源集数覆盖键: {value!r}")
        digits = fractional.group(2).rstrip("0") or "0"
        return EpisodeKey("fractional", number, fractional_digits=digits)
    match = re.fullmatch(
        r"(?:(SP|E))?0*(\d{1,4})(?:-(?:(?:SP|E))?0*(\d{1,4}))?",
        value.strip(),
        re.IGNORECASE,
    )
    if not match:
        raise PlanError(f"无效源集数覆盖键: {value!r}")
    kind = "special" if (match.group(1) or "").upper() == "SP" else "regular"
    start = int(match.group(2))
    end = int(match.group(3) or 0)
    if start <= 0 or (end and end < start):
        raise PlanError(f"无效源集数覆盖范围: {value!r}")
    return EpisodeKey(kind, start, end)


def _load_episode_map(path: Path) -> dict[EpisodeKey, tuple[int, int, int]]:
    try:
        raw = _load_json_text(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise PlanError(f"无法读取剧集覆盖映射: {path}; {exc}") from exc
    if not isinstance(raw, dict):
        raise PlanError('剧集覆盖映射必须是对象，例如 {"13":"S02E01"}')
    output: dict[EpisodeKey, tuple[int, int, int]] = {}
    for source, target in raw.items():
        if not isinstance(source, str) or not isinstance(target, str):
            raise PlanError("剧集覆盖映射的键和值都必须是字符串")
        key = _parse_episode_map_key(source)
        match = re.fullmatch(
            r"S0*(\d{1,3})E0*(\d{1,4})(?:-E?0*(\d{1,4}))?",
            target.strip(),
            re.IGNORECASE,
        )
        if not match:
            raise PlanError(f"无效目标剧集覆盖值: {target!r}")
        season = int(match.group(1))
        episode = int(match.group(2))
        end_episode = int(match.group(3) or 0)
        if episode <= 0 or (end_episode and end_episode < episode):
            raise PlanError(f"无效目标剧集覆盖范围: {target!r}")
        if bool(key.end_number) != bool(end_episode):
            raise PlanError(
                f"剧集覆盖的源和目标必须同时为单集或同时为范围: "
                f"{source!r}: {target!r}"
            )
        if key in output:
            raise PlanError(f"剧集覆盖映射包含等价重复键: {source!r}")
        output[key] = (season, episode, end_episode)
    return output


def build_collection_plan(
    alist: AListClient,
    tmdb_client: TMDBClient,
    *,
    src_path: str,
    parent_path: str,
    tmdb_id: int,
    mapping_path: Path | None,
    allow_index_mapping: bool,
    ignore_orphan_temp: bool = False,
) -> Plan:
    collection = tmdb_client.get(f"/collection/{tmdb_id}")
    collection_title = safe_name(str(collection.get("name") or tmdb_id))
    desired_collection_dir = join_remote(parent_path, collection_title)
    collection_dir = _preserve_equivalent_source_root(src_path, desired_collection_dir)
    parts = [
        dict(item) for item in (collection.get("parts") or []) if isinstance(item, dict)
    ]
    if not parts:
        raise PlanError("TMDB 合集中没有电影条目")
    parts.sort(key=lambda item: str(item.get("release_date") or "9999-99-99"))

    explicit_map = _load_collection_map(mapping_path) if mapping_path else None
    if explicit_map is None and not allow_index_mapping:
        raise PlanError(
            "合集按 01、02 顺序映射存在错配风险。请提供 --collection-map JSON 文件，"
            "或显式传入 --allow-index-mapping。"
        )

    source_files = alist.walk(src_path, ignore_orphan_temp=ignore_orphan_temp)
    cleanup_files = _planned_cleanup_files(source_files)
    groups = parse_ep_files(source_files, prefer_simplified=False)
    _raise_unparsed_media(_unparsed_media_paths(source_files, groups), "合集")
    special_keys = sorted(key.display for key in groups if key.kind == "special")
    if special_keys:
        raise PlanError(
            "电影合集不支持特别篇编号，拒绝静默遗漏: " + ", ".join(special_keys)
        )
    regular_groups = {key: value for key, value in groups.items() if key.kind == "regular"}
    if not regular_groups:
        raise PlanError("未找到带编号的合集媒体文件")

    parts_by_id = {int(item["id"]): item for item in parts if item.get("id") is not None}
    warnings: list[str] = []
    _append_cleanup_warning(warnings, cleanup_files)
    if collection_dir != desired_collection_dir:
        warnings.append(
            "TMDB 标题与现有作品目录仅大小写或 Unicode 拼写不同；"
            f"已保留现有目录名 {split_remote(collection_dir)[1]!r}"
        )
    if ignore_orphan_temp:
        warnings.append("已显式忽略 .scraper-tmp-* 遗留条目，可能存在未恢复文件")
    if explicit_map is None:
        warnings.append("使用 TMDB 上映日期顺序完成合集编号映射")
    else:
        source_numbers = {key.number for key in regular_groups}
        extra_numbers = sorted(set(explicit_map) - source_numbers)
        if extra_numbers:
            raise PlanError(
                "合集映射包含源目录中不存在的编号: "
                + ", ".join(str(number) for number in extra_numbers)
            )

    planned: list[PlannedFile] = []
    member_posters: dict[str, str] = {}
    member_movies: dict[str, dict[str, Any]] = {}
    for key in sorted(regular_groups):
        if key.end_number:
            raise PlanError(
                f"电影合集不支持一个文件对应多个合集成员: {key.display}"
            )
        if explicit_map is not None:
            movie_id = explicit_map.get(key.number)
            if movie_id is None:
                raise PlanError(f"合集映射文件缺少编号 {key.number}")
            part = parts_by_id.get(movie_id)
            if part is None:
                raise PlanError(
                    f"合集映射中的 TMDB 电影 ID {movie_id} 不属于合集 {tmdb_id}"
                )
        else:
            index = key.number - 1
            if index < 0 or index >= len(parts):
                raise PlanError(f"编号 {key.number} 超出合集条目范围")
            part = parts[index]
            movie_id = int(part["id"])

        title = safe_name(str(part.get("title") or part.get("original_title") or movie_id))
        year = _extract_year(part.get("release_date"))
        base_name = safe_name(f"{title} ({year})")
        member_dir = join_remote(collection_dir, base_name)
        member_movies[member_dir] = {
            "tmdb_id": movie_id,
            "title": title,
            "year": year,
        }
        if isinstance(part.get("poster_path"), str) and part.get("poster_path"):
            member_posters[member_dir] = str(part["poster_path"])
        group_files = sorted(regular_groups[key], key=lambda x: _collision_key(str(x["full_path"])))
        names = make_unique_media_names(base_name, group_files, preserve_editions=True)
        for item, final_name in zip(group_files, names):
            planned.append(
                _planned_file_from_entry(
                    item,
                    final_name=final_name,
                    target_dir=member_dir,
                    episode_key=str(key.number),
                )
            )

    plan = Plan(
        mode="collection",
        source_root=normalize_remote_path(src_path),
        target_root=collection_dir,
        files=planned,
        warnings=warnings,
        metadata={
            "tmdb_id": tmdb_id,
            "title": collection_title,
            "poster_path": collection.get("poster_path"),
            "backdrop_path": collection.get("backdrop_path"),
            "member_posters": member_posters,
            "member_movies": member_movies,
            "mapping": explicit_map,
        },
        cleanup_files=cleanup_files,
    )
    _add_snapshot_warnings(plan)
    _canonical_root, canonical_warnings, canonical_posters = (
        _plan_canonical_batch_tree(
            [plan],
            outer_root=collection_dir,
            allow_family_boundaries=False,
            preserve_container_root=True,
        )
    )
    # The TMDB collection is a proven directory-only relation.  Its member
    # movies are identity leaves; the collection id is not a media leaf.
    plan.warnings.extend(
        warning for warning in canonical_warnings if warning not in plan.warnings
    )
    if canonical_posters:
        plan.metadata.setdefault("member_posters", {}).update(canonical_posters)
    validate_plan(alist, plan)
    return plan


def _discover_tagged_media_roots(
    alist: AListClient,
    src_path: str,
    *,
    max_depth: int = 4,
) -> list[str]:
    """Find topmost descendant directories carrying an explicit TMDB tag."""
    root = normalize_remote_path(src_path)
    found: list[str] = []

    def visit(path: str, depth: int) -> None:
        if depth > max_depth:
            return
        content = alist.try_list(path, refresh=True)
        if content is None:
            raise PlanError(f"无法读取系列子目录: {path}")
        for entry in sorted(content, key=lambda value: _collision_key(str(value.get("name") or ""))):
            name = entry.get("name")
            if not entry.get("is_dir") or not isinstance(name, str) or not name:
                continue
            child = join_remote(path, name)
            if _tmdb_hint_from_source(child) is not None:
                found.append(child)
            else:
                visit(child, depth + 1)

    visit(root, 1)
    return found


def _discover_franchise_member_roots(
    alist: AListClient,
    src_path: str,
    *,
    ignore_orphan_temp: bool = False,
) -> tuple[list[str], list[str]]:
    """Return immediate franchise members that actually contain video.

    A franchise directory is a user-owned grouping boundary.  Its immediate
    children are therefore independent works (or a work-specific bundle), not
    seasons of one arbitrarily selected TMDB show.  Explicit ``{tmdb-*}`` tags
    remain authoritative, but are no longer required.
    """
    root = normalize_remote_path(src_path)
    content = alist.try_list(root, refresh=True)
    if content is None:
        raise PlanError(f"无法读取系列父目录: {root}")
    members: list[str] = []
    skipped: list[str] = []
    for entry in sorted(
        content,
        key=lambda value: _collision_key(str(value.get("name") or "")),
    ):
        name = entry.get("name")
        if not entry.get("is_dir") or not isinstance(name, str) or not name:
            continue
        child = join_remote(root, name)
        child_files = alist.walk(
            child,
            ignore_orphan_temp=ignore_orphan_temp,
            include_bonus=True,
        )
        if any(
            Path(str(item.get("name") or "")).suffix.lower() in VIDEO_EXTS
            for item in child_files
        ):
            members.append(child)
        else:
            skipped.append(child)

    # Some cloud providers expose a newly moved child directory before its
    # contents become visible.  A single stale empty response previously made
    # real movies look like permanent placeholders.  Retry all empty children
    # together (bounded to two refresh rounds) so a large franchise incurs a
    # fixed delay rather than one delay per directory.
    for delay in (0.2, 0.5):
        if not skipped:
            break
        time.sleep(delay)
        still_skipped: list[str] = []
        for child in skipped:
            child_files = alist.walk(
                child,
                ignore_orphan_temp=ignore_orphan_temp,
                include_bonus=True,
            )
            if any(
                Path(str(item.get("name") or "")).suffix.lower() in VIDEO_EXTS
                for item in child_files
            ):
                members.append(child)
            else:
                still_skipped.append(child)
        skipped = still_skipped
    members.sort(key=_collision_key)
    return members, skipped


def _franchise_title_suffix_episode_overrides(
    member_root: str,
    source_files: Sequence[Mapping[str, Any]],
) -> tuple[dict[str, int], int | None]:
    """Recognize an exact ``<child title><two-digit episode>`` run.

    Asian release packs sometimes omit every separator before the episode
    number (``作品01.mp4``).  The generic parser intentionally does not treat an
    arbitrary trailing number as an episode, so recover it only inside a
    franchise child when *all* videos share the substantial immediate-child
    title and form the complete contiguous run 01..N.
    """
    videos = [
        item for item in source_files
        if Path(str(item.get("name") or "")).suffix.lower() in VIDEO_EXTS
    ]
    if len(videos) < 3:
        return {}, None
    title_keys = list(dict.fromkeys(
        key
        for query in _franchise_member_queries(member_root)
        if len(key := _normalize_match_title(query)) >= 6
        and len(re.findall(r"[\u3400-\u9fff]", query)) >= 4
    ))
    for title_key in title_keys:
        video_numbers: list[int] = []
        for item in videos:
            stem_key = _normalize_match_title(
                Path(str(item.get("name") or "")).stem
            )
            match = re.fullmatch(rf"{re.escape(title_key)}(\d{{2,3}})", stem_key)
            if match is None:
                break
            video_numbers.append(int(match.group(1)))
        else:
            unique_numbers = sorted(set(video_numbers))
            if (
                len(unique_numbers) == len(videos)
                and unique_numbers == list(range(1, len(videos) + 1))
            ):
                overrides: dict[str, int] = {}
                for item in source_files:
                    path = str(item.get("full_path") or "")
                    if not path:
                        continue
                    stem_key = _normalize_match_title(
                        Path(str(item.get("name") or "")).stem
                    )
                    match = re.fullmatch(
                        rf"{re.escape(title_key)}(\d{{2,3}})", stem_key
                    )
                    if match is not None and 1 <= int(match.group(1)) <= len(videos):
                        overrides[path] = int(match.group(1))
                return overrides, len(videos)
    return {}, None


def _apply_franchise_title_suffix_episode_overrides(
    member_root: str,
    source_files: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    overrides, _ = _franchise_title_suffix_episode_overrides(
        member_root, source_files
    )
    copied = [dict(item) for item in source_files]
    for item in copied:
        number = overrides.get(str(item.get("full_path") or ""))
        if number is None:
            continue
        item["_episode_kind_override"] = "regular"
        item["_episode_key_override"] = number
    return copied


def _franchise_member_match(
    tmdb_client: TMDBClient,
    member_root: str,
    source_files: Sequence[Mapping[str, Any]],
    *,
    target_parent: str | None = None,
) -> AutoMatch:
    hint = _tmdb_hint_from_source(member_root)
    if hint is not None:
        return _direct_tmdb_match(tmdb_client, member_root, hint)

    parent_root, name = split_remote(member_root)
    generic_additive_season = (
        "+" in unicodedata.normalize("NFKC", split_remote(parent_root)[1])
        and _is_generic_season_member_label(name)
    )
    additive_sibling_keys: set[str] = set()
    if generic_additive_season:
        parent_label = unicodedata.normalize("NFKC", split_remote(parent_root)[1])
        sibling_label = parent_label.split("+", 1)[1]
        additive_sibling_keys = {
            key
            for query in [
                sibling_label,
                *_franchise_member_queries("/" + sibling_label),
            ]
            if (key := _normalize_match_title(query))
            and _usable_release_title_query(query)
        }
    if _source_suggests_collection(member_root):
        requested_type: str | None = "collection"
    elif _has_movie_context({"name": name, "full_path": member_root}):
        requested_type = "movie"
    else:
        requested_type = _media_type_from_source_context(member_root)
        if requested_type is None and target_parent is not None:
            requested_type, _ = _media_context_from_source_and_target(
                member_root,
                target_parent,
            )

    prefer_animation = _source_is_animation_library(member_root)
    if target_parent is not None:
        _, prefer_animation = _media_context_from_source_and_target(
            member_root,
            target_parent,
        )

    episode_count = len({
        key.number
        for item in source_files
        if Path(str(item.get("name") or "")).suffix.lower() in VIDEO_EXTS
        and (key := extract_episode_key(str(item.get("name") or ""))) is not None
        and key.kind == "regular"
        and not key.end_number
    }) or None
    if episode_count is None:
        _, episode_count = _franchise_title_suffix_episode_overrides(
            member_root, source_files
        )
    video_count = sum(
        1
        for item in source_files
        if Path(str(item.get("name") or "")).suffix.lower() in VIDEO_EXTS
    )
    if (
        video_count == 1
        and episode_count is None
        and not re.search(r"(?:TV|电视|電視|剧集|season|第\s*\d+\s*季)", name, re.I)
    ):
        # A single unnumbered child in an explicit multi-work root may be a
        # standalone movie even when the destination category is animation.
        # Search TV and movie and retain the normal ambiguity margin instead
        # of forcing the category hint to turn it into a one-episode TV show.
        requested_type = None
    # A child containing a real run of three or more numbered episodes is a
    # TV-season bundle unless its own label explicitly says movie/film.  This
    # prevents a named OVA inside that bundle from making the entire season
    # match the OVA movie (for example five Herz disc shorts stealing the ten
    # Herz TV episodes).
    explicit_movie_member = bool(re.search(
        r"(?:剧场版|劇場版|电影|電影|movie|film)",
        name,
        re.I,
    ))
    if episode_count is not None and episode_count >= 3 and not explicit_movie_member:
        requested_type = "tv"
    queries = _franchise_member_match_queries(member_root, source_files)
    excluded_additive_sibling_ids: set[int] = set()
    if generic_additive_season:
        # Resolve the explicitly advertised work to the right of ``+`` first.
        # A generic ``第三季`` belongs to the left series, but TMDB search can
        # rank the additive sibling (for example an anime spin-off) within the
        # normal ambiguity margin.  Excluding only the independently confirmed
        # sibling identity lets the left work be evaluated on its own evidence;
        # no title is hard-coded and an ambiguous sibling remains fail-closed.
        for sibling_query in _franchise_member_queries(parent_root):
            if "+" not in unicodedata.normalize("NFKC", sibling_query):
                continue
            try:
                sibling_match, _ = auto_match_tmdb(
                    tmdb_client,
                    sibling_query,
                    media_type="tv",
                    min_confidence=0.88,
                    prefer_animation=prefer_animation,
                )
            except ScraperError:
                continue
            if sibling_match.status == "confirmed":
                excluded_additive_sibling_ids.add(sibling_match.tmdb_id)
                break
    if requested_type == "tv" and episode_count is not None and episode_count >= 3:
        # Season bundles often use only a sequel suffix in the child name;
        # retain the parent franchise title as the final TV-only lookup.
        queries = list(dict.fromkeys([
            *queries,
            *_franchise_member_queries(parent_root),
        ]))
    last_error: ScraperError | None = None
    for query in queries:
        try:
            match, _ = auto_match_tmdb(
                tmdb_client,
                query,
                media_type=requested_type,
                min_confidence=0.88,
                prefer_animation=prefer_animation,
                expected_episode_count=episode_count,
                excluded_tmdb_ids=excluded_additive_sibling_ids,
            )
            if match.status != "confirmed":
                raise PlanError(
                    f"子作品自动匹配未达到置信度: {member_root}; "
                    + ", ".join(match.decision_trace.get("blockers", []))
                )
            if generic_additive_season and additive_sibling_keys:
                candidate_title_keys = {
                    _normalize_match_title(str(title))
                    for title in [
                        match.title,
                        *(match.decision_trace.get("official_titles") or []),
                        *(match.decision_trace.get("aliases_checked") or []),
                    ]
                    if str(title).strip()
                }
                if any(
                    sibling_key in title_key
                    for sibling_key in additive_sibling_keys
                    for title_key in candidate_title_keys
                ):
                    last_error = PlanError(
                        "通用季度子目录不能匹配同级 + 右侧的附加作品: "
                        f"{member_root}; {match.media_type}/{match.tmdb_id} {match.title}"
                    )
                    continue
            if (
                requested_type == "tv"
                and episode_count is not None
                and episode_count >= 3
                and match.media_type != "tv"
            ):
                raise PlanError(
                    f"多集子目录只允许匹配 TV，拒绝整季被单个电影/OVA 候选吞并: "
                    f"{member_root}; {match.media_type}/{match.tmdb_id} {match.title}"
                )
            if (
                match.media_type == "tv"
                and episode_count is not None
                and episode_count >= 3
            ):
                # A packaging parent may advertise an additional work after
                # ``+`` (for example a main 1–9 season pack plus an anime
                # spinoff).  Title similarity alone can then select that
                # neighbouring work for an otherwise generic ``第一季`` child.
                # The child's complete contiguous episode count is hard
                # identity evidence: at least one positive official season of
                # the candidate must expose the same boundary.  A mismatch is
                # rejected before planning so cleanup items can never point at
                # a winner later discarded as an out-of-range episode.
                detail = tmdb_client.get(f"/tv/{match.tmdb_id}")
                official_counts = {
                    int(item["episode_count"])
                    for item in (detail.get("seasons") or [])
                    if isinstance(item, Mapping)
                    and isinstance(item.get("season_number"), int)
                    and int(item["season_number"]) > 0
                    and isinstance(item.get("episode_count"), int)
                    and int(item["episode_count"]) > 0
                }
                if official_counts and episode_count not in official_counts:
                    last_error = PlanError(
                        "多集子目录的完整连续边界与候选 TMDB 任一季度均不一致: "
                        f"{member_root}; source={episode_count}, "
                        f"{match.media_type}/{match.tmdb_id}={sorted(official_counts)}"
                    )
                    continue
            return match
        except ScraperError as exc:
            last_error = exc
    raise last_error or PlanError(f"无法识别系列子作品: {member_root}")


def _franchise_member_match_queries(
    member_root: str,
    source_files: Sequence[Mapping[str, Any]],
) -> list[str]:
    """Build queries for one franchise child without losing its parent title.

    A two-part film is often stored as ``完整片名/01 前篇（2020）``.  The
    immediate child label is not a title and querying it alone can confidently
    match an unrelated movie.  For these generic part labels, combine the
    actual parent work title with the child part/year before consulting release
    filenames.  This is deliberately generic; no franchise title is embedded
    in the rule.
    """
    member_root = normalize_remote_path(member_root)
    parent_root, child_name = split_remote(member_root)
    direct_queries = _franchise_member_queries(member_root)
    generic_part_pattern = re.compile(
        r"\s*(?:\d{1,3}\s*[.)、_-]?\s*)?"
        r"(?:前篇|后篇|後篇|上篇|下篇|前编|后编|前編|後編|"
        r"part\s*[ivx\d]+|movie\s*[ivx\d]+)"
        r"(?:\s*[（(]?(?:19|20)\d{2}[)）]?)?\s*",
        flags=re.I,
    )
    generic_label = next(
        (
            query
            for query in reversed(direct_queries)
            if generic_part_pattern.fullmatch(unicodedata.normalize("NFKC", query))
        ),
        "",
    )
    generic_part = bool(generic_label)
    contextual_queries: list[str] = []
    generic_additive_season = False
    if generic_part:
        parent_queries = _franchise_member_queries(parent_root)
        child_query = generic_label
        for parent_query in parent_queries:
            parent_base = re.sub(
                r"\s*[（(]?(?:19|20)\d{2}\s*[-–—~～至到]\s*"
                r"(?:19|20)\d{2}[)）]?\s*"
                r"(?:前篇|上篇|前编|前編).*(?:后篇|後篇|下篇|后编|後編).*$",
                " ",
                parent_query,
                flags=re.I,
            )
            parent_base = re.sub(
                r"\s*(?:前篇|上篇|前编|前編)\s*[+＋/&、和与及]\s*"
                r"(?:后篇|後篇|下篇|后编|後編).*$",
                " ",
                parent_base,
                flags=re.I,
            )
            parent_base = re.sub(
                r"^\s*\d{1,3}\s*[.)、_-]?\s*",
                "",
                parent_base,
            )
            parent_base = re.sub(r"\s+", " ", parent_base).strip(" -_")
            if parent_base and not re.match(r"^(?:19|20)\d{2}\)?", parent_base):
                contextual_queries.append(f"{parent_base} {child_query}".strip())
    else:
        clean_parent_anchor = _clean_franchise_root_label(parent_root)
        anchor_key = _normalize_match_title(clean_parent_anchor)
        if len(anchor_key) >= 6:
            for child_query in direct_queries[:2]:
                child_key = _normalize_match_title(child_query)
                # Correct one accidental inserted character in the repeated
                # franchise prefix only when deleting it makes the immediate
                # child begin with the independently cleaned parent anchor.
                # The remainder (the member subtitle) is preserved verbatim.
                for index in range(min(len(anchor_key) + 1, len(child_key))):
                    corrected = child_key[:index] + child_key[index + 1:]
                    if corrected.startswith(anchor_key) and len(corrected) > len(anchor_key):
                        contextual_queries.append(corrected)
                        break
        parent_queries = _franchise_member_queries(parent_root)
        # If a pack label explicitly joins an additional work after ``+``, a
        # generic season child belongs to the left-hand series unless its own
        # title says otherwise.  Keep the full parent queries as fallbacks,
        # but add the left identity without the sibling-work suffix first.
        if re.search(r"(?:season|第\s*[一二三四五六七八九十0-9]+\s*季)", child_name, re.I):
            additive_parent_anchors = list(dict.fromkeys(
                anchor
                for query in parent_queries
                for anchor in [re.split(r"[+＋]", query, maxsplit=1)[0].strip()]
                if _usable_release_title_query(anchor)
            ))
            contextual_queries.extend(additive_parent_anchors)
            contextual_queries.extend(
                f"{anchor} {child_query}".strip()
                for anchor in additive_parent_anchors
                for child_query in direct_queries[:2]
            )
            generic_additive_season = (
                "+" in unicodedata.normalize("NFKC", split_remote(parent_root)[1])
                and _is_generic_season_member_label(child_name)
            )
        for parent_query in parent_queries[:2]:
            parent_base = re.sub(
                r"(?:全系列|系列合集|大合集|合集包|合集|franchise)|"
                r"\d+\s*[-–—~～至到]\s*\d+\s*(?:部|季|集)",
                " ",
                parent_query,
                flags=re.I,
            )
            parent_base = re.sub(
                r"(?:[48]k)?超清(?:2160p|1080p|720p)?|"
                r"(?:2160p|1080p|720p)?收藏版|典藏版|完整版",
                " ",
                parent_base,
                flags=re.I,
            )
            parent_base = re.sub(r"\s+", " ", parent_base).strip(" -_")
            for child_query in direct_queries[:2]:
                parent_key = _normalize_match_title(parent_base)
                child_key = _normalize_match_title(child_query)
                if (
                    len(parent_key) >= 4
                    and len(child_key) >= 3
                    and parent_key not in child_key
                ):
                    contextual_queries.append(
                        f"{parent_base} {child_query}".strip()
                    )

    release_queries: list[str] = []
    for item in source_files:
        if Path(str(item.get("name") or "")).suffix.lower() not in VIDEO_EXTS:
            continue
        release_queries.extend(_movie_queries_from_item(item))
        if len(release_queries) >= 8:
            break

    # A generic label must never be searched by itself.  It is only useful
    # after the real parent title has been attached.
    yearless_direct = [
        re.sub(
            r"\s*[（(]?(?:19|20)\d{2}(?:[.\-/]\d{1,2})?[)）]?\s*",
            " ",
            query,
        ).strip()
        for query in direct_queries
    ]
    yearless_contextual = [
        re.sub(
            r"\s*[（(]?(?:19|20)\d{2}(?:[.\-/]\d{1,2})?[)）]?\s*",
            " ",
            query,
        ).strip()
        for query in contextual_queries
    ]
    # Some TV releases name a season as ``Series 2 -Arc Title-`` while TMDB
    # stores all seasons under the base series record. Keep the full release
    # query first, then add a narrowly bounded base-title fallback; an ordinary
    # trailing number or an unpaired subtitle is not stripped.
    season_arc_bases = [
        re.sub(
            r"\s+(?:\d{1,2}|最终季|第\s*[一二三四五六七八九十]{1,3}\s*季)"
            r"\s*[-–—]\s*[^-–—]{2,}[-–—]\s*$",
            "",
            query,
        ).strip()
        for query in yearless_direct
    ]
    season_arc_bases = [
        query
        for query, original in zip(season_arc_bases, yearless_direct)
        if query and query != original
    ]
    ordered = (
        [*contextual_queries, *yearless_contextual, *release_queries]
        if generic_part
        else [
            *contextual_queries,
            *yearless_contextual,
            *direct_queries,
            *yearless_direct,
            *season_arc_bases,
            *release_queries,
        ]
        if generic_additive_season
        else [
            *direct_queries,
            *contextual_queries,
            *yearless_direct,
            *yearless_contextual,
            *season_arc_bases,
            *release_queries,
        ]
    )
    def usable_title_query(query: str) -> bool:
        return _usable_release_title_query(query)

    return list(dict.fromkeys(
        query for query in ordered if query and usable_title_query(query)
    ))


def _is_generic_season_member_label(label: str) -> bool:
    """Return true only when a child label carries no work title of its own."""
    normalized = unicodedata.normalize("NFKC", label)
    if _season_from_source("/" + normalized) is None:
        return False
    residual = re.sub(
        r"(?:season|s)\s*0*\d{1,3}|"
        r"\d{1,3}(?:st|nd|rd|th)\s+season|"
        r"第\s*(?:\d{1,3}|[一二三四五六七八九十]{1,3})\s*季",
        " ",
        normalized,
        flags=re.I,
    )
    residual = re.sub(r"[（(]\s*(?:19|20)\d{2}\s*[）)]", " ", residual)
    residual = re.sub(r"(?:全|共)?\s*\d{1,4}\s*集", " ", residual)
    residual = re.sub(
        r"\b(?:4k|8k|2160p|1080p|720p|bluray|blu-?ray|web-?dl|"
        r"webrip|x26[45]|hevc|av1|10bit)\b",
        " ",
        residual,
        flags=re.I,
    )
    residual = re.sub(
        r"(?:内封|内嵌|外挂|硬字幕|软字幕|"
        r"简英(?:双语)?|繁英(?:双语)?|中英(?:双语)?|"
        r"日英(?:双语)?|简繁(?:双语)?|双语|多语|"
        r"简中|繁中|中字|字幕)",
        " ",
        residual,
        flags=re.I,
    )
    residual = re.sub(r"[\s._\-+()（）\[\]]+", " ", residual).strip()
    return not _usable_release_title_query(residual)


def _franchise_member_official_season(
    tmdb_client: TMDBClient,
    tmdb_id: int,
    member_root: str,
    source_files: Sequence[Mapping[str, Any]],
) -> int:
    """Use a season only when folder year and exact episode count both prove it."""
    try:
        show = tmdb_client.get(f"/tv/{tmdb_id}")
    except ApiError:
        show = {}
    title_matched_season = _season_from_series_variant(
        split_remote(member_root)[1], show
    )
    if title_matched_season is not None:
        matched_meta = next(
            (
                item for item in (show.get("seasons") or [])
                if isinstance(item, Mapping)
                and item.get("season_number") == title_matched_season
            ),
            None,
        )
        video_numbers = {
            int(item.get("_episode_key_override", key.number))
            for item in source_files
            if Path(str(item.get("name", ""))).suffix.lower() in VIDEO_EXTS
            and (key := extract_episode_key(str(item.get("name", "")))) is not None
            and key.kind == "regular"
            and not key.end_number
        }
        if (
            isinstance(matched_meta, Mapping)
            and matched_meta.get("episode_count") == len(video_numbers)
            and video_numbers == set(range(1, len(video_numbers) + 1))
        ):
            return title_matched_season
    years = set(re.findall(r"(?:19|20)\d{2}", split_remote(member_root)[1]))
    episode_numbers = {
        key.number
        for item in source_files
        if Path(str(item.get("name", ""))).suffix.lower() in VIDEO_EXTS
        and (key := extract_episode_key(str(item.get("name", "")))) is not None
        and key.kind == "regular"
        and not key.end_number
    }
    if len(years) != 1 or not episode_numbers:
        return 1
    if not show:
        return 1
    matches = [
        int(item["season_number"])
        for item in (show.get("seasons") or [])
        if isinstance(item, Mapping)
        and isinstance(item.get("season_number"), int)
        and not isinstance(item.get("season_number"), bool)
        and int(item["season_number"]) > 0
        and item.get("episode_count") == len(episode_numbers)
        and str(item.get("air_date") or "")[:4] in years
    ]
    return matches[0] if len(matches) == 1 else 1


def _franchise_member_special_overrides(
    tmdb_client: TMDBClient,
    tmdb_id: int,
    member_root: str,
    source_files: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    """Map an OVA child only from an exact official Season 00 run."""
    copied = [dict(item) for item in source_files]
    video_numbers = sorted({
        key.number
        for item in copied
        if Path(str(item.get("name", ""))).suffix.lower() in VIDEO_EXTS
        and (key := extract_episode_key(str(item.get("name", "")))) is not None
        and key.kind == "regular"
        and not key.end_number
    })
    if not video_numbers or video_numbers != list(range(1, len(video_numbers) + 1)):
        return copied
    try:
        specials = tmdb_client.get(f"/tv/{tmdb_id}/season/0").get("episodes") or []
    except ApiError:
        return copied
    label_queries = [
        _normalize_match_title(query)
        for query in _franchise_member_queries(member_root)
        if len(_normalize_match_title(query)) >= 5
    ]
    candidates = [
        item for item in specials
        if isinstance(item, Mapping)
        and isinstance(item.get("episode_number"), int)
        and not isinstance(item.get("episode_number"), bool)
        and any(
            query in _normalize_match_title(str(item.get("name") or ""))
            for query in label_queries
        )
    ]
    if len(candidates) != len(video_numbers):
        years = set(re.findall(r"(?:19|20)\d{2}", split_remote(member_root)[1]))
        candidates = [
            item for item in specials
            if isinstance(item, Mapping)
            and isinstance(item.get("episode_number"), int)
            and not isinstance(item.get("episode_number"), bool)
            and len(years) == 1
            and str(item.get("air_date") or "")[:4] in years
        ]
    target_numbers = sorted(int(item["episode_number"]) for item in candidates)
    if (
        len(target_numbers) != len(video_numbers)
        or target_numbers != list(range(target_numbers[0], target_numbers[-1] + 1))
    ):
        return copied
    number_map = dict(zip(video_numbers, target_numbers))
    for item in copied:
        key = extract_episode_key(str(item.get("name", "")))
        if key is None or key.number not in number_map or key.end_number:
            continue
        item["_episode_kind_override"] = "special"
        item["_episode_key_override"] = number_map[key.number]
    return copied


def _attach_unique_movie_subtitles(
    movie_groups: dict[int, list[dict[str, Any]]],
    unresolved: Sequence[dict[str, Any]],
    *,
    tmdb_client: TMDBClient | None = None,
    prefer_animation: bool = False,
) -> list[dict[str, Any]]:
    """Attach proven backup companions to exactly one identified movie.

    Besides subtitles, a lower-resolution OVA/movie video may sit under a
    different wrapper tree from its already identified 4K counterpart.  Such
    a video is attached only when it has explicit movie/special context and
    its release queries identify exactly one existing movie group.
    """
    group_keys: dict[int, set[str]] = {}
    group_video_dirs: dict[int, set[str]] = {}
    group_video_stems: dict[int, set[str]] = {}
    for tmdb_id, members in movie_groups.items():
        group_keys[tmdb_id] = {
            key
            for item in members
            if Path(str(item.get("name", ""))).suffix.lower() in VIDEO_EXTS
            # The first query is the concrete release title.  Later fallbacks
            # can be only the franchise name (for example ``Mushishi``), which
            # must not merge a different TV special into the one known movie.
            for query in _movie_queries_from_item(item)[:1]
            if len(key := _normalize_match_title(query)) >= 8
        }
        group_video_dirs[tmdb_id] = {
            _collision_key(split_remote(normalize_remote_path(str(item["full_path"])))[0])
            for item in members
            if Path(str(item.get("name", ""))).suffix.lower() in VIDEO_EXTS
            and item.get("full_path")
        }
        group_video_stems[tmdb_id] = {
            _batch_subtitle_release_stem(str(item.get("name") or ""))
            for item in members
            if Path(str(item.get("name", ""))).suffix.lower() in VIDEO_EXTS
        }
    remaining: list[dict[str, Any]] = []
    for item in unresolved:
        suffix = Path(str(item.get("name", ""))).suffix.lower()
        is_subtitle = suffix in SUBTITLE_EXTS
        is_contextual_video = suffix in VIDEO_EXTS and (
            _has_movie_context(item) or _has_special_context(item)
        ) and item.get("_episode_kind_override") != "special"
        if not (is_subtitle or is_contextual_video):
            remaining.append(item)
            continue
        subtitle_keys = {
            key
            for query in _movie_queries_from_item(item)[:1]
            if len(key := _normalize_match_title(query)) >= 8
        }
        item_dir = _collision_key(
            split_remote(normalize_remote_path(str(item.get("full_path", "/"))))[0]
        )
        same_directory_matches = [
            tmdb_id
            for tmdb_id, directories in group_video_dirs.items()
            if item_dir in directories
        ]
        exact_release_matches = [
            tmdb_id
            for tmdb_id, stems in group_video_stems.items()
            if _batch_subtitle_release_stem(str(item.get("name") or "")) in stems
        ]
        if is_subtitle and len(exact_release_matches) == 1:
            movie_groups[exact_release_matches[0]].append(item)
            continue
        # A numbered backup subtitle usually belongs to a TV episode.  Do not
        # let fuzzy movie-title matching swallow an entire TV subtitle set
        # merely because a related movie is the only movie group in the same
        # franchise.  A subtitle may still accompany a movie when it shares
        # the movie video's exact directory or carries explicit movie context.
        if (
            is_subtitle
            and extract_episode_key(str(item.get("name", ""))) is not None
            and not same_directory_matches
            and not _has_movie_context(item)
        ):
            remaining.append(item)
            continue
        matches = (
            same_directory_matches
            if len(same_directory_matches) == 1
            else [
                tmdb_id
                for tmdb_id, keys in group_keys.items()
                if any(
                    subtitle_key in video_key or video_key in subtitle_key
                    for subtitle_key in subtitle_keys
                    for video_key in keys
                )
            ]
        )
        if not matches and tmdb_client is not None:
            resolved_ids: set[int] = set()
            for query in _movie_queries_from_item(item):
                try:
                    match, _ = auto_match_tmdb(
                        tmdb_client,
                        query,
                        media_type="movie",
                        min_confidence=0.88,
                        prefer_animation=prefer_animation,
                    )
                except ScraperError:
                    continue
                if (
                    match.status == "confirmed"
                    and match.media_type == "movie"
                    and (
                        not is_contextual_video
                        or _specific_movie_query_agrees_with_match(query, match)
                    )
                ):
                    resolved_ids.add(match.tmdb_id)
            matches = sorted(resolved_ids & set(movie_groups))
        if len(matches) == 1:
            movie_groups[matches[0]].append(item)
        else:
            remaining.append(item)
    return remaining


def _attach_fractional_feature_by_ass_title_to_movie_groups(
    alist: AListClient,
    tmdb_client: TMDBClient,
    movie_groups: dict[int, list[dict[str, Any]]],
    special_files: Sequence[dict[str, Any]],
    all_files: Sequence[Mapping[str, Any]],
) -> tuple[list[dict[str, Any]], list[str]]:
    """Route a titleless ``N.5`` feature using its tiny ASS-text sidecar.

    Some disc packs number a separately catalogued film as ``20.5`` and keep
    its real title only in an exported ``.ass.txt`` companion.  Read only the
    bounded sidecar and only the ASS ``title`` style line; ordinary dialogue
    is not identity evidence.  The title must uniquely match a movie identity
    already confirmed elsewhere in the same batch.
    """
    if not movie_groups:
        return list(special_files), []
    official_keys: dict[int, set[str]] = {}
    for tmdb_id in movie_groups:
        try:
            detail = tmdb_client.get(f"/movie/{tmdb_id}")
        except ApiError:
            continue
        official_keys[tmdb_id] = {
            key
            for field in ("title", "original_title")
            if len(key := _normalize_match_title(str(detail.get(field) or ""))) >= 3
        }
    readers = getattr(alist, "read_file_bytes", None)
    if not callable(readers):
        return list(special_files), []
    by_dir: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for item in all_files:
        path = normalize_remote_path(str(item.get("full_path", "")))
        directory, _ = split_remote(path)
        by_dir[_collision_key(directory)].append(item)
    remaining: list[dict[str, Any]] = []
    warnings: list[str] = []
    for video in special_files:
        key = extract_episode_key(str(video.get("name", "")))
        if key is None or key.kind != "fractional":
            remaining.append(video)
            continue
        video_path = normalize_remote_path(str(video.get("full_path", "")))
        video_dir, video_name = split_remote(video_path)
        sidecars = [
            item for item in by_dir.get(_collision_key(video_dir), [])
            if str(item.get("name", "")).casefold().endswith(".ass.txt")
            and extract_episode_key(str(item.get("name", ""))) == key
            and 0 < int(item.get("size") or 0) <= 2 * 1024 * 1024
        ]
        title_keys: set[str] = set()
        for sidecar in sidecars:
            try:
                payload = readers(
                    str(sidecar.get("full_path", "")), max_bytes=2 * 1024 * 1024
                )
            except (ApiError, OSError, ValueError):
                continue
            text = ""
            for encoding in ("utf-8-sig", "utf-16", "utf-16-le", "gb18030"):
                try:
                    text = payload.decode(encoding)
                    break
                except UnicodeError:
                    continue
            for line in text.splitlines():
                if not line.casefold().startswith("dialogue:"):
                    continue
                fields = line.split(",", 9)
                if len(fields) != 10 or fields[3].strip().casefold() != "title":
                    continue
                value = re.sub(r"\{[^}]*\}", "", fields[9]).replace(r"\N", " ")
                normalized = _normalize_match_title(value)
                if len(normalized) >= 3:
                    title_keys.add(normalized)
        matches = {
            tmdb_id
            for tmdb_id, candidates in official_keys.items()
            if any(
                source in official or official in source
                for source in title_keys
                for official in candidates
            )
        }
        if len(matches) != 1:
            remaining.append(video)
            continue
        tmdb_id = next(iter(matches))
        movie_groups[tmdb_id].append(video)
        warnings.append(
            f"{key.display} 的 ASS 文本伴侣 title 样式唯一标记为已确认的 "
            f"TMDB movie/{tmdb_id}；已按同一电影版本参与清晰度去重"
        )
    return remaining, warnings


def build_tagged_collection_plan(
    alist: AListClient,
    tmdb_client: TMDBClient,
    *,
    src_path: str,
    parent_path: str,
    tmdb_id: int,
    ignore_orphan_temp: bool = False,
) -> Plan:
    """Plan an existing collection whose member filenames already carry ids."""
    collection = tmdb_client.get(f"/collection/{tmdb_id}")
    title = safe_name(str(collection.get("name") or tmdb_id))
    desired_root = join_remote(parent_path, title)
    collection_root = _preserve_equivalent_source_root(src_path, desired_root)
    scanned_entries = alist.walk(src_path, ignore_orphan_temp=ignore_orphan_temp)
    scanned = _filter_media(scanned_entries)
    cleanup_files = _planned_cleanup_files(scanned_entries)
    groups: dict[int, list[dict[str, Any]]] = defaultdict(list)
    untagged: list[str] = []
    for item in scanned:
        hint = _tmdb_hint_from_source(str(item.get("full_path") or item.get("name") or ""))
        if hint is None:
            untagged.append(str(item.get("full_path") or item.get("name") or ""))
        else:
            groups[hint].append(dict(item))
    if untagged:
        _raise_unparsed_media(untagged, "合集成员 TMDB 编号")
    if not groups:
        raise PlanError("未找到带 {tmdb-编号} 的合集媒体文件")

    member_plans: list[Plan] = []
    for member_id, member_files in sorted(groups.items()):
        match = _direct_tmdb_match(
            tmdb_client,
            str(member_files[0].get("full_path") or member_files[0].get("name") or ""),
            member_id,
        )
        if match.media_type != "movie":
            raise PlanError(
                f"合集成员 {member_id} 被识别为 {match.media_type}，不能当作电影整理"
            )
        member_plans.append(
            build_movie_plan(
                alist,
                tmdb_client,
                src_path=src_path,
                parent_path=collection_root,
                tmdb_id=member_id,
                ignore_orphan_temp=ignore_orphan_temp,
                source_files=member_files,
                defer_validation=True,
            )
        )

    member_movies = {
        plan.target_root: {
            "tmdb_id": plan.metadata["tmdb_id"],
            "title": plan.metadata["title"],
            "year": plan.metadata["year"],
        }
        for plan in member_plans
    }
    member_posters = {
        plan.target_root: str(plan.metadata["poster_path"])
        for plan in member_plans
        if isinstance(plan.metadata.get("poster_path"), str) and plan.metadata.get("poster_path")
    }
    if isinstance(collection.get("poster_path"), str) and collection.get("poster_path"):
        member_posters[collection_root] = str(collection["poster_path"])
    canonical_root, canonical_warnings, canonical_posters = (
        _plan_canonical_batch_tree(
            member_plans,
            outer_root=collection_root,
            allow_family_boundaries=False,
        )
    )
    collection_root = canonical_root
    member_movies = {
        plan.target_root: {
            "tmdb_id": plan.metadata["tmdb_id"],
            "title": plan.metadata["title"],
            "year": plan.metadata["year"],
        }
        for plan in member_plans
    }
    member_posters = {
        plan.target_root: str(plan.metadata["poster_path"])
        for plan in member_plans
        if isinstance(plan.metadata.get("poster_path"), str)
        and plan.metadata.get("poster_path")
    }
    member_posters.update(canonical_posters)
    if isinstance(collection.get("poster_path"), str) and collection.get("poster_path"):
        member_posters[collection_root] = str(collection["poster_path"])
    result = Plan(
        mode="collection",
        source_root=normalize_remote_path(src_path),
        target_root=collection_root,
        files=[item for plan in member_plans for item in plan.files],
        problem_files=[item for plan in member_plans for item in plan.problem_files],
        warnings=list(
            dict.fromkeys(
                [
                    f"已按文件名中的 TMDB 编号识别 {len(member_plans)} 部合集电影",
                    *(
                        [f"写入并回读成功后清理 {len(cleanup_files)} 个明确无用的片头片尾/广告文件"]
                        if cleanup_files
                        else []
                    ),
                    *(warning for plan in member_plans for warning in plan.warnings),
                    *canonical_warnings,
                ]
            )
        ),
        metadata={
            "tmdb_id": tmdb_id,
            "title": title,
            "poster_path": collection.get("poster_path"),
            "backdrop_path": collection.get("backdrop_path"),
            "member_posters": member_posters,
            "member_movies": member_movies,
            "mapping": None,
        },
        cleanup_files=cleanup_files,
    )
    validate_plan(alist, result)
    return result


def _batch_subtitle_release_stem(name: str) -> str:
    """Return a release basename without a trailing subtitle-language tag."""
    stem = unicodedata.normalize("NFKC", Path(name).stem)
    # The video and separately exported subtitle can credit slightly
    # different collaborating groups (``[A&B]`` versus ``[A]``) while the
    # complete release payload after that credit remains byte-for-byte named.
    # A leading bracket containing letters is release-group metadata; episode
    # brackets such as ``[01]`` are deliberately retained.
    stem = re.sub(r"^(?:\[[^\]]*[A-Za-z][^\]]*\]\s*)+", "", stem)
    stem = re.sub(
        r"(?:[ ._-]+(?:zh[ ._-]?(?:cn|tw)|chs|cht|sc|tc|gb|big5|"
        r"simplified|traditional|简体?|繁体?|簡體?|繁體?))+$",
        "",
        stem,
        flags=re.IGNORECASE,
    )
    return _collision_key(stem)


def _attach_unique_batch_subtitle_companions(
    source_root: str,
    source_files: Sequence[Mapping[str, Any]],
    subplans: Sequence[Plan],
) -> tuple[list[PlannedFile], list[PlannedProblem]]:
    """Route subtitle-only batch folders by an exact, unique video basename.

    Franchise packs often put movie/spin-off subtitles in a shared backup
    directory outside the member directory that contains the video.  Member
    discovery quite correctly refuses to treat that subtitle-only directory as
    a work, but silently skipping it loses a provable companion.  Only an exact
    release basename (after a trailing language tag is removed) and one unique
    planned video destination are accepted.  Missing or ambiguous evidence is
    surfaced as a problem and the subtitle remains at source.
    """
    root = normalize_remote_path(source_root)
    handled_paths = {
        _collision_key(item.source_path)
        for plan in subplans
        for item in (*plan.files, *plan.cleanup_files, *plan.problem_files)
    }
    videos_by_stem: dict[str, list[PlannedFile]] = defaultdict(list)
    occupied_targets: set[str] = set()
    for plan in subplans:
        for planned in plan.files:
            occupied_targets.add(
                _collision_key(join_remote(planned.target_dir, planned.final_name))
            )
            if planned.media_kind != "video":
                continue
            videos_by_stem[_batch_subtitle_release_stem(planned.original_name)].append(
                planned
            )

    attached: list[PlannedFile] = []
    problems: list[PlannedProblem] = []
    for raw in source_files:
        name = str(raw.get("name") or "")
        full_path = normalize_remote_path(str(raw.get("full_path") or ""))
        if (
            raw.get("is_dir")
            or Path(name).suffix.lower() not in SUBTITLE_EXTS
            or not full_path
            or not _path_is_within(full_path, root)
            or _collision_key(full_path) in handled_paths
        ):
            continue
        destinations = {
            (
                _collision_key(video.target_dir),
                _collision_key(Path(video.final_name).stem),
            ): video
            for video in videos_by_stem.get(
                _batch_subtitle_release_stem(name), []
            )
        }
        if len(destinations) != 1:
            reason = (
                "系列批次中没有与该字幕同发行 basename 的已确认视频；"
                "保留原位并标记规划未闭合"
                if not destinations
                else "系列批次中有多个同发行 basename 视频，无法唯一确认字幕归属；"
                "保留原位并标记规划未闭合"
            )
            problems.append(PlannedProblem(source_path=full_path, reason=reason))
            continue
        video = next(iter(destinations.values()))
        extension = Path(name).suffix.lower()
        language = subtitle_language(name)
        language_suffix = f".{language}" if language else ""
        final_name = _limit_filename(
            f"{Path(video.final_name).stem}{language_suffix}{extension}"
        )
        target_path = join_remote(video.target_dir, final_name)
        target_key = _collision_key(target_path)
        if target_key in occupied_targets:
            problems.append(PlannedProblem(
                source_path=full_path,
                reason=(
                    "已确认视频的目标字幕槽已有文件，不能覆盖或猜测替换；"
                    "保留原位并标记规划未闭合"
                ),
                target_path=target_path,
            ))
            continue
        planned = _planned_file_from_entry(
            raw,
            final_name=final_name,
            target_dir=video.target_dir,
            episode_key=video.episode_key,
        )
        attached.append(planned)
        occupied_targets.add(target_key)
        handled_paths.add(_collision_key(full_path))
    return attached, problems


def build_batch_plan(
    alist: AListClient,
    tmdb_client: TMDBClient,
    *,
    src_path: str,
    parent_path: str,
    ignore_orphan_temp: bool = False,
    _target_root: str | None = None,
    media_root: str | None = None,
) -> Plan:
    """Combine independently identified descendants into one diagnostic plan."""
    source_root = normalize_remote_path(src_path)
    target_root = (
        normalize_remote_path(_target_root)
        if _target_root is not None
        else join_remote(parent_path, _clean_franchise_root_label(source_root))
    )
    member_roots, skipped_empty = _discover_franchise_member_roots(
        alist,
        source_root,
        ignore_orphan_temp=ignore_orphan_temp,
    )
    flat_member_files: dict[str, list[dict[str, Any]]] = {}
    if len(member_roots) < 2:
        source_files = alist.walk(
            source_root,
            ignore_orphan_temp=ignore_orphan_temp,
            include_bonus=True,
        )
        flat_videos = [
            dict(item)
            for item in source_files
            if Path(str(item.get("name") or "")).suffix.lower() in VIDEO_EXTS
            and str(item.get("full_path") or "")
            and cleanup_reason(str(item.get("name") or "")) is None
            and not is_sample(str(item.get("name") or ""))
        ]
        # Some two-film releases place both independently titled movies in
        # the parent directory. Treat each complete filename as a synthetic
        # member only inside an explicit multi-work/batch root. Every member
        # still has to independently confirm as a different TMDB movie below.
        if len(flat_videos) >= 2 and not member_roots:
            member_roots = [str(item["full_path"]) for item in flat_videos]
            flat_member_files = {
                str(item["full_path"]): [item] for item in flat_videos
            }
            skipped_empty = []
        else:
            raise PlanError("系列父目录中未找到至少两个包含视频的独立作品目录")

    subplans: list[Plan] = []
    inferred_members = 0
    flat_movie_ids: set[int] = set()
    primary_identity_candidates: set[WorkIdentity] = set()
    for member_root in member_roots:
        synthetic_flat_member = member_root in flat_member_files
        source_files = (
            flat_member_files[member_root]
            if synthetic_flat_member
            else alist.walk(
                member_root,
                ignore_orphan_temp=ignore_orphan_temp,
                include_bonus=True,
            )
        )
        try:
            nested_members = [] if synthetic_flat_member else _discover_franchise_member_roots(
                alist,
                member_root,
                ignore_orphan_temp=ignore_orphan_temp,
            )[0]
            generic_bucket = bool(re.fullmatch(
                r"(?:其它|其他|misc|miscellaneous|附加|额外)",
                split_remote(member_root)[1].strip(),
                flags=re.I,
            ))
            if len(nested_members) >= 2 and (
                _source_suggests_collection(member_root) or generic_bucket
            ):
                # The source child is usually only a packaging bucket such as
                # ``06 剧场版…4K`` or ``空之境界 1-10部 4K``.  Its wording is
                # not a library identity and must not leak into the target.
                # The recursive plan independently identifies every member;
                # The canonical work-tree planner then creates clean,
                # evidence-based family roots from confirmed metadata.
                nested_target_root = target_root
                subplans.append(
                    build_batch_plan(
                        alist,
                        tmdb_client,
                        src_path=member_root,
                        parent_path=parent_path,
                        ignore_orphan_temp=ignore_orphan_temp,
                        _target_root=nested_target_root,
                        media_root=media_root,
                    )
                )
                inferred_members += len(nested_members)
                continue
            match = _franchise_member_match(
                tmdb_client,
                member_root,
                source_files,
                target_parent=parent_path,
            )
            if (
                match.media_type == "tv"
                and _is_generic_season_member_label(split_remote(member_root)[1])
            ):
                primary_identity_candidates.add(
                    WorkIdentity("tmdb.tv", match.tmdb_id)
                )
            if _tmdb_hint_from_source(member_root) is None:
                inferred_members += 1
            if synthetic_flat_member:
                if match.media_type != "movie":
                    raise PlanError(
                        "扁平多作品目录中的每个视频都必须独立确认成电影: "
                        f"{member_root}; 实际为 {match.media_type}/{match.tmdb_id}"
                    )
                if match.tmdb_id in flat_movie_ids:
                    raise PlanError(
                        "扁平多作品目录中的两个视频匹配到同一 TMDB 电影，"
                        f"拒绝按不同作品拆分: movie/{match.tmdb_id}"
                    )
                flat_movie_ids.add(match.tmdb_id)
            if match.media_type == "tv":
                source_files = _apply_franchise_title_suffix_episode_overrides(
                    member_root, source_files
                )
                member_season = _franchise_member_official_season(
                    tmdb_client,
                    match.tmdb_id,
                    member_root,
                    source_files,
                )
                source_files = _franchise_member_special_overrides(
                    tmdb_client,
                    match.tmdb_id,
                    member_root,
                    source_files,
                )
                subplans.append(
                    build_tv_plan_smart(
                        auto_episode_mode=True,
                        alist=alist,
                        tmdb_client=tmdb_client,
                        src_path=member_root,
                        parent_path=target_root,
                        tmdb_id=match.tmdb_id,
                        season=member_season,
                        absolute=False,
                        prefer_simplified=True,
                        allow_unmapped=False,
                        ignore_orphan_temp=ignore_orphan_temp,
                        episode_map_path=None,
                        episode_group_id=None,
                        source_files=source_files,
                        media_root=media_root,
                        _proven_member_season=member_season != 1,
                    )
                )
            elif match.media_type == "movie":
                subplans.append(
                    build_movie_plan(
                        alist,
                        tmdb_client,
                        src_path=source_root if synthetic_flat_member else member_root,
                        parent_path=target_root,
                        tmdb_id=match.tmdb_id,
                        ignore_orphan_temp=ignore_orphan_temp,
                        source_files=source_files,
                    )
                )
            else:
                subplans.append(
                    build_tagged_collection_plan(
                        alist, tmdb_client,
                        src_path=member_root,
                        parent_path=target_root,
                        tmdb_id=match.tmdb_id,
                        ignore_orphan_temp=ignore_orphan_temp,
                    )
                    if _tmdb_hint_from_source(member_root) is not None
                    else build_collection_plan(
                        alist, tmdb_client,
                        src_path=member_root,
                        parent_path=target_root,
                        tmdb_id=match.tmdb_id,
                        mapping_path=None,
                        allow_index_mapping=True,
                        ignore_orphan_temp=ignore_orphan_temp,
                    )
                )
        except ScraperError as exc:
            raise PlanError(f"子作品规划失败 {member_root}: {exc}") from exc

    if len(primary_identity_candidates) > 1:
        raise PlanError(
            "系列容器中的通用季度目录指向多个不同 TMDB 身份，"
            "无法唯一确定 canonical 主作品"
        )
    root_identity = next(iter(primary_identity_candidates), None)
    movie_quality_warnings = _dedupe_confirmed_batch_movies(
        subplans,
        outer_root=target_root,
    )
    target_root, subseries_warnings, subseries_posters = _plan_canonical_batch_tree(
        subplans,
        outer_root=target_root,
        root_identity=root_identity,
    )
    batch_source_files = alist.walk(
        source_root,
        ignore_orphan_temp=ignore_orphan_temp,
        include_bonus=True,
    )
    attached_batch_subtitles, batch_subtitle_problems = (
        _attach_unique_batch_subtitle_companions(
            source_root,
            batch_source_files,
            subplans,
        )
    )

    member_tv: dict[str, dict[str, Any]] = {}
    member_movies: dict[str, dict[str, Any]] = {}
    member_posters: dict[str, str] = {}
    for plan in subplans:
        if plan.mode in {"tv", "mixed"}:
            series_root = str(plan.metadata.get("series_root") or plan.target_root)
            member_tv[series_root] = {
                key: plan.metadata.get(key)
                for key in (
                    "tmdb_id", "title", "original_title", "year", "poster_path", "backdrop_path",
                    "season_posters",
                )
            }
        if plan.mode == "movie":
            member_movies[plan.target_root] = {
                "tmdb_id": plan.metadata["tmdb_id"],
                "title": plan.metadata["title"],
                "year": plan.metadata["year"],
            }
            if (
                isinstance(plan.metadata.get("poster_path"), str)
                and plan.metadata.get("poster_path")
            ):
                member_posters[plan.target_root] = str(plan.metadata["poster_path"])
        if plan.mode in {"collection", "mixed", "batch"}:
            raw_tv = plan.metadata.get("member_tv")
            raw_movies = plan.metadata.get("member_movies")
            raw_posters = plan.metadata.get("member_posters")
            if isinstance(raw_tv, Mapping):
                member_tv.update({
                    str(key): dict(value)
                    for key, value in raw_tv.items()
                    if isinstance(value, Mapping)
                })
            if isinstance(raw_movies, Mapping):
                member_movies.update({str(key): dict(value) for key, value in raw_movies.items()})
            if isinstance(raw_posters, Mapping):
                member_posters.update({str(key): str(value) for key, value in raw_posters.items()})
    member_posters.update(subseries_posters)

    result = Plan(
        mode="batch",
        source_root=source_root,
        target_root=target_root,
        files=[
            *(item for plan in subplans for item in plan.files),
            *attached_batch_subtitles,
        ],
        cleanup_files=_dedupe_cleanup_files(
            item for plan in subplans for item in plan.cleanup_files
        ),
        problem_files=[
            *(item for plan in subplans for item in plan.problem_files),
            *batch_subtitle_problems,
        ],
        warnings=list(
            dict.fromkeys(
                [
                    (
                        f"已识别 {len(subplans)} 个独立电影文件；每个文件均已独立"
                        "确认不同 TMDB 电影身份，将保留系列父目录并合并规划"
                        if flat_member_files
                        else f"已识别 {len(subplans)} 个独立作品目录，将保留系列父目录并合并规划"
                    ),
                    *(
                        [
                            f"其中 {inferred_members} 个未标注 TMDB 编号的"
                            + ("电影文件" if flat_member_files else "子作品")
                            + "已独立检索确认"
                        ]
                        if inferred_members
                        else []
                    ),
                    *subseries_warnings,
                    *movie_quality_warnings,
                    *(
                        [
                            f"{len(attached_batch_subtitles)} 个独立字幕目录文件已通过"
                            "唯一同发行 basename 跟随已确认视频"
                        ]
                        if attached_batch_subtitles
                        else []
                    ),
                    *(
                        [
                            f"{len(batch_subtitle_problems)} 个独立字幕目录文件缺少"
                            "唯一视频信息，将保留原位并标记规划未闭合"
                        ]
                        if batch_subtitle_problems
                        else []
                    ),
                    *(
                        [
                            f"{sum(plan.mode == 'movie' for plan in subplans)} 部系列电影"
                            "已保留独立 TMDB 身份和各自独立电影目录"
                        ]
                        if any(plan.mode == "movie" for plan in subplans)
                        else []
                    ),
                    *(
                        [
                            "已跳过无视频的空占位目录: "
                            + ", ".join(split_remote(path)[1] for path in skipped_empty)
                        ]
                        if skipped_empty
                        else []
                    ),
                    *(warning for plan in subplans for warning in plan.warnings),
                ]
            )
        ),
        metadata={
            "title": split_remote(source_root)[1],
            "member_tv": member_tv,
            "member_movies": member_movies,
            "member_posters": member_posters,
        },
    )
    validate_plan(alist, result, media_root=media_root)
    return result


def _planned_companion_key(target_dir: str, final_name: str) -> tuple[str, str]:
    """Return the destination-level identity shared by a video and subtitles."""
    stem = Path(final_name).stem
    stem = re.sub(
        r"\.(?:zh-CN|zh-TW|en|ja)(?:\.\d+)*$|\.subtitle(?:\.\d+|\d*)$",
        "",
        stem,
        flags=re.IGNORECASE,
    )
    stem = re.sub(r" - v\d+$", "", stem, flags=re.IGNORECASE)
    return _collision_key(normalize_remote_path(target_dir)), _collision_key(stem)


def _demote_unpaired_subtitles(alist: AListClient, plan: Plan) -> None:
    """Keep a subtitle only when its destination has an exact video companion."""
    planned_video_keys = {
        _planned_companion_key(item.target_dir, item.final_name)
        for item in plan.files
        if item.media_kind == "video"
    }
    subtitle_items = [item for item in plan.files if item.media_kind == "subtitle"]
    target_dirs = {
        normalize_remote_path(item.target_dir)
        for item in subtitle_items
        if _planned_companion_key(item.target_dir, item.final_name)
        not in planned_video_keys
    }
    existing_video_keys: set[tuple[str, str]] = set()
    for target_dir in sorted(target_dirs):
        for entry in alist.try_list(target_dir, refresh=True) or []:
            name = str(entry.get("name", ""))
            if entry.get("is_dir") or Path(name).suffix.lower() not in VIDEO_EXTS:
                continue
            existing_video_keys.add(_planned_companion_key(target_dir, name))

    # Subtitle files are media candidates, never generic cleanup rows.
    plan.cleanup_files = [
        item for item in plan.cleanup_files
        if Path(item.original_name).suffix.lower() not in SUBTITLE_EXTS
    ]

    retained: list[PlannedFile] = []
    unpaired: list[PlannedFile] = []
    for item in plan.files:
        if item.media_kind != "subtitle":
            retained.append(item)
            continue
        companion = _planned_companion_key(item.target_dir, item.final_name)
        if companion in planned_video_keys or companion in existing_video_keys:
            retained.append(item)
        else:
            unpaired.append(item)
    plan.files = retained
    if not unpaired:
        return

    # An orphan subtitle has no video to ingest: it stays at source and is
    # recorded as a resource gap below, not as a blocking plan problem.
    gaps = plan.scan_report.setdefault("resource_gaps", [])
    if not isinstance(gaps, list):
        raise PlanError("scan_report.resource_gaps 必须是数组")
    gaps.append({
        "kind": "subtitle_without_video",
        "label": f"{len(unpaired)} 个未配对字幕",
        "reason": "目标位置没有同名视频",
        "files": sorted(item.source_path for item in unpaired),
    })
def _deferred_subtitle_source_keys(plan: Plan) -> set[str]:
    rows = plan.scan_report.get("deferred_subtitles")
    if not isinstance(rows, list):
        return set()
    return {
        _collision_key(normalize_remote_path(str(row["source_path"])))
        for row in rows
        if isinstance(row, Mapping) and isinstance(row.get("source_path"), str)
    }


def _forward_plan_files(plan: Plan) -> list[PlannedFile]:
    deferred = _deferred_subtitle_source_keys(plan)
    return [
        item for item in plan.files
        if _collision_key(item.source_path) not in deferred
    ]


def _planned_movie_tmdb_id(plan: Plan, item: PlannedFile) -> int | None:
    """Return a movie identity only when target metadata proves it."""
    candidates: list[tuple[int, int]] = []
    if plan.mode == "movie":
        tmdb_id = plan.metadata.get("tmdb_id")
        if (
            isinstance(tmdb_id, int)
            and not isinstance(tmdb_id, bool)
            and tmdb_id > 0
            and _path_is_within(item.target_dir, plan.target_root)
        ):
            candidates.append((len(normalize_remote_path(plan.target_root)), tmdb_id))
    raw_movies = plan.metadata.get("member_movies")
    if isinstance(raw_movies, Mapping):
        target_path = normalize_remote_path(
            join_remote(item.target_dir, item.final_name)
        )
        for raw_root, identity in raw_movies.items():
            if not isinstance(identity, Mapping):
                continue
            tmdb_id = identity.get("tmdb_id")
            root = normalize_remote_path(str(raw_root))
            if (
                isinstance(tmdb_id, int)
                and not isinstance(tmdb_id, bool)
                and tmdb_id > 0
                and (
                    _collision_key(target_path) == _collision_key(root)
                    or _path_is_within(item.target_dir, root)
                )
            ):
                candidates.append((len(root), tmdb_id))
    if not candidates:
        return None
    candidates.sort(reverse=True)
    return candidates[0][1]


def _plan_has_movie_tmdb_id(plan: Plan, tmdb_id: int) -> bool:
    """Return whether batch metadata contains the confirmed movie identity."""
    if plan.mode == "movie" and plan.metadata.get("tmdb_id") == tmdb_id:
        return True
    raw_movies = plan.metadata.get("member_movies")
    return isinstance(raw_movies, Mapping) and any(
        isinstance(identity, Mapping) and identity.get("tmdb_id") == tmdb_id
        for identity in raw_movies.values()
    )


def _same_concrete_movie_release(
    loser_name: str,
    loser_path: str,
    winner: PlannedFile,
) -> bool:
    """Prove two cross-root copies name the same concrete movie release.

    Batch canonical-root rebasing can intentionally move the retained file out
    of the metadata root that first proved its identity.  In that case target
    containment cannot be replayed during final validation.  Require both the
    still-present plan-level TMDB identity and substantial overlap between the
    concrete release queries instead of trusting a generic franchise label.
    """
    loser_queries = {
        key
        for query in _movie_queries_from_item(
            {"name": loser_name, "full_path": loser_path}
        )[:2]
        if len(key := _normalize_match_title(query)) >= 8
    }
    winner_queries = {
        key
        for query in _movie_queries_from_item(
            {"name": winner.original_name, "full_path": winner.source_path}
        )[:2]
        if len(key := _normalize_match_title(query)) >= 8
    }
    return any(
        left in right or right in left
        for left in loser_queries
        for right in winner_queries
    )


def _revalidate_managed_subtitle(
    alist: Any,
    item: PlannedFile,
) -> None:
    """Fresh-read a selected managed subtitle before formal validation.

    The proof is intentionally attached to the selected ``PlannedFile``
    rather than trusted from a plan-level summary.  A restart may observe a
    same-size replacement at the source path; recomputing the content verdict
    catches that drift before the writer can move it.
    """
    proof = item.subtitle_validation
    if not isinstance(proof, Mapping):
        return
    if item.media_kind != "subtitle":
        raise PlanError("字幕内容证明绑定到了非字幕文件")
    source_path = normalize_remote_path(item.source_path)
    proof_path = normalize_remote_path(str(proof.get("source_path") or ""))
    if proof_path != source_path:
        raise PlanError(f"字幕内容证明来源不匹配: {source_path}")
    declared_size = item.source_size
    if (
        isinstance(declared_size, bool)
        or not isinstance(declared_size, int)
        or declared_size <= 0
        or declared_size > EXPORTED_SRT_MAX_BYTES
    ):
        raise PlanError(f"字幕内容证明缺少有效来源大小: {source_path}")
    proof_size = proof.get("size")
    if proof_size != declared_size:
        raise PlanError(f"字幕内容证明大小不匹配: {source_path}")
    proof_name = str(proof.get("source_name") or "")
    if proof_name and proof_name != item.original_name:
        raise PlanError(f"字幕内容证明文件名不匹配: {source_path}")

    exact_info = getattr(alist, "exact_file_info", None)
    if callable(exact_info):
        try:
            observed = exact_info(source_path)
        except Exception as exc:  # pragma: no cover - transport-specific
            raise PlanError(f"无法 fresh 核验字幕来源: {source_path}") from exc
        if isinstance(observed, Mapping):
            observed_size = observed.get("size")
            if observed_size != declared_size:
                raise PlanError(f"字幕来源大小已漂移: {source_path}")

    reader = getattr(alist, "read_file_bytes", None)
    if not callable(reader):
        reader = getattr(alist, "read_file_prefix", None)
    if not callable(reader):
        raise PlanError(f"无法读取已选字幕来源进行内容复核: {source_path}")
    try:
        try:
            payload = reader(source_path, max_bytes=declared_size)
        except TypeError:
            payload = reader(source_path, declared_size)
    except Exception as exc:  # pragma: no cover - transport-specific
        raise PlanError(f"读取已选字幕来源失败: {source_path}") from exc
    if not isinstance(payload, (bytes, bytearray, memoryview)):
        raise PlanError(f"已选字幕来源读取结果无效: {source_path}")
    verdict = validate_managed_subtitle_content(
        bytes(payload),
        proof.get("original_language"),
        declared_size=declared_size,
        max_bytes=EXPORTED_SRT_MAX_BYTES,
    )
    if str(verdict.get("status") or "").casefold() != "satisfied":
        raise PlanError(
            f"已选字幕内容证明失效: {source_path} ({verdict.get('reason')})"
        )
    if verdict.get("preference") != proof.get("preference"):
        raise PlanError(f"已选字幕优先级证明已变化: {source_path}")
    if verdict.get("selection") != proof.get("selection"):
        raise PlanError(f"已选字幕语言证明已变化: {source_path}")
    target_coordinate = proof.get("target_coordinate")
    if (
        isinstance(target_coordinate, str)
        and target_coordinate
        and target_coordinate != "movie"
        and target_coordinate not in Path(item.final_name).stem
    ):
        raise PlanError(f"已选字幕目标集坐标不匹配: {source_path}")


def validate_plan(
    alist: AListClient,
    plan: Plan,
    *,
    media_root: str | None = None,
) -> None:
    # Apply the same one-track selector used by persisted-plan finalization
    # before any destination collision checks.  This keeps direct planner
    # callers and restart/recovery validation on the exact writer input.
    _retain_one_subtitle_track_per_exact_video(plan)
    _demote_unpaired_subtitles(alist, plan)
    _restrict_cleanup_to_allowlist(plan)
    if not plan.files:
        if plan.problem_files:
            preview = "；".join(
                f"{item.source_path}（{item.reason}）"
                for item in plan.problem_files[:5]
            )
            raise PlanError(f"没有可安全执行的媒体；问题文件: {preview}")
        if not (planned_artwork(plan) or planned_nfos(plan)):
            raise PlanError("操作计划为空")

    source_root = normalize_remote_path(plan.source_root).rstrip("/") or "/"
    target_root = normalize_remote_path(plan.target_root).rstrip("/") or "/"
    exported_srt_proofs: dict[str, Mapping[str, Any]] = {}
    raw_exported_proofs = (
        plan.metadata.get("exported_srt_normalizations", [])
        if isinstance(plan.metadata, Mapping)
        else []
    )
    if raw_exported_proofs is None:
        raw_exported_proofs = []
    if not isinstance(raw_exported_proofs, list):
        raise PlanError("exported_srt_normalizations 必须是数组")
    for proof in raw_exported_proofs:
        if not isinstance(proof, Mapping):
            raise PlanError("exported_srt_normalizations 包含无效行")
        proof_path = normalize_remote_path(str(proof.get("source_path", "")))
        if proof_path in exported_srt_proofs:
            raise PlanError(f"导出字幕证明重复: {proof_path}")
        exported_srt_proofs[proof_path] = proof
    try:
        placement_for(source_root, target_root, media_root=media_root)
    except ValueError as exc:
        raise PlanError(str(exc)) from exc
    source_root_folded = _collision_key(source_root)
    target_root_folded = _collision_key(target_root)
    if (
        target_root_folded == source_root_folded
        or target_root_folded.startswith(source_root_folded.rstrip("/") + "/")
        or source_root_folded.startswith(target_root_folded.rstrip("/") + "/")
    ):
        raise PlanError(
            "源目录与目标目录发生重叠，可能导致旧文件夹与 Season 目录混在一起。"
            "请把 --parent 设置为与源目录完全分离的父目录。"
        )

    tv_series_roots: set[str] = set()
    if plan.mode == "mixed":
        series_root = plan.metadata.get("series_root")
        if isinstance(series_root, str):
            tv_series_roots.add(_collision_key(normalize_remote_path(series_root)))
    elif plan.mode == "batch":
        member_tv = plan.metadata.get("member_tv")
        if isinstance(member_tv, Mapping):
            tv_series_roots.update(
                _collision_key(normalize_remote_path(str(series_root)))
                for series_root in member_tv
            )
    for item in plan.files:
        if (
            item.media_kind == "video"
            and _planned_movie_tmdb_id(plan, item) is not None
            and _collision_key(normalize_remote_path(item.target_dir)) in tv_series_roots
        ):
            raise PlanError(
                "独立电影不得直接放入电视剧作品根目录："
                f"{join_remote(item.target_dir, item.final_name)}"
            )

    canonical_path_spellings: dict[str, str] = {}

    def register_path_spelling(path: str, label: str) -> None:
        key = _collision_key(path)
        previous = canonical_path_spellings.get(key)
        if previous is not None and previous != path:
            raise PlanError(
                f"计划包含 Unicode/大小写等价但拼写不同的{label}: {previous!r} 与 {path!r}"
            )
        canonical_path_spellings[key] = path

    register_path_spelling(source_root, "路径")
    register_path_spelling(target_root, "路径")

    source_paths: set[str] = set()
    source_items: dict[str, PlannedFile] = {}
    source_items_by_location: dict[tuple[str, str], PlannedFile] = {}
    final_destinations: dict[tuple[str, str], str] = {}
    per_source_final: dict[tuple[str, str], str] = {}

    for item in plan.files:
        source_path = normalize_remote_path(item.source_path)
        source_dir = normalize_remote_path(item.source_dir)
        target_dir = normalize_remote_path(item.target_dir)
        expected_dir, expected_name = split_remote(source_path)
        register_path_spelling(source_dir, "源目录")
        register_path_spelling(target_dir, "目标目录")
        original_name = _validate_remote_source_basename(item.original_name)
        final_name = _validate_remote_basename(item.final_name)
        declared_source_kind = item.source_media_kind
        if declared_source_kind is not None and declared_source_kind not in {"video", "subtitle"}:
            raise PlanError(
                "计划包含不受支持的 source_media_kind: "
                f"{item.source_path}"
            )
        _revalidate_managed_subtitle(alist, item)
        if declared_source_kind == "subtitle" and Path(original_name).suffix.lower() == ".txt":
            proof = exported_srt_proofs.get(source_path)
            if (
                proof is None
                or proof.get("format") != "srt"
                or proof.get("size") != item.source_size
                or proof.get("source_name") != original_name
                or proof.get("language") not in {"zh-CN", "zh-TW"}
                or subtitle_language(item.final_name) != proof.get("language")
            ):
                raise PlanError(
                    "导出字幕缺少与来源/目标绑定的内容证明: "
                    f"{item.source_path}"
                )
        expected_media_kind = declared_source_kind or media_kind(original_name)
        if expected_media_kind == "disc_image":
            raise PlanError(
                "光盘镜像容器必须先完成只读安全内容展开；"
                f"禁止直接进入正式媒体计划: {item.source_path}"
            )
        if item.media_kind != expected_media_kind:
            raise PlanError(
                "计划媒体类型与源文件扩展名不一致，拒绝绕过正式媒体准入: "
                f"{item.source_path}"
            )
        # A plan normally carries the AList listing size.  Reject a known
        # tiny video before the plan is persisted; when a provider omitted
        # the size, the executor performs the same check against its exact
        # readback immediately before it can move anything.
        if (
            expected_media_kind == "video"
            and item.source_size is not None
            and not _media_quality.video_size_is_admissible(item.source_size)
        ):
            raise PlanError(
                "视频文件小于正式库准入下限 "
                f"{_media_quality.minimum_video_bytes()} bytes，拒绝计划: "
                f"{item.source_path}"
            )

        if source_dir != expected_dir or original_name != expected_name:
            raise PlanError(
                f"计划中的源路径、源目录或原文件名不一致: {item.source_path}"
            )
        source_prefix = source_root_folded.rstrip("/") + "/"
        if _collision_key(source_path) != source_root_folded and not _collision_key(source_path).startswith(
            source_prefix
        ):
            raise PlanError(f"计划中的源文件不在 source_root 下: {source_path}")
        target_prefix = target_root_folded.rstrip("/") + "/"
        if _collision_key(target_dir) != target_root_folded and not _collision_key(target_dir).startswith(
            target_prefix
        ):
            raise PlanError(f"计划中的目标目录不在 target_root 下: {target_dir}")
        try:
            placement_for(source_root, target_dir, media_root=media_root)
        except ValueError as exc:
            raise PlanError(str(exc)) from exc

        source_key = _collision_key(source_path)
        if source_key in source_paths:
            raise PlanError(f"源文件重复出现在计划中: {source_path}")
        source_paths.add(source_key)
        source_items[source_key] = item

        location_key = (_collision_key(source_dir), _collision_key(original_name))
        if location_key in source_items_by_location:
            raise PlanError(f"源目录中存在大小写不安全的重复名称: {source_dir}/{original_name}")
        source_items_by_location[location_key] = item

        destination_key = (_collision_key(target_dir), _collision_key(final_name))
        existing_source = final_destinations.get(destination_key)
        if existing_source is not None:
            raise PlanError(
                f"目标文件名冲突: {target_dir}/{final_name}; "
                f"来源为 {existing_source} 与 {source_path}"
            )
        final_destinations[destination_key] = source_path

        rename_key = (_collision_key(source_dir), _collision_key(final_name))
        existing_source = per_source_final.get(rename_key)
        if existing_source is not None:
            raise PlanError(f"同一源目录重命名冲突: {final_name}")
        per_source_final[rename_key] = source_path

    contextual_cleanup_keys = _contextual_theme_cleanup_paths([
        {
            "name": item.original_name,
            "full_path": item.source_path,
            "is_dir": False,
        }
        for item in [*plan.files, *plan.cleanup_files]
    ])
    for item in plan.cleanup_files:
        source_path = normalize_remote_path(item.source_path)
        source_dir = normalize_remote_path(item.source_dir)
        expected_dir, expected_name = split_remote(source_path)
        register_path_spelling(source_dir, "清理目录")
        original_name = _validate_remote_source_basename(item.original_name)
        if source_dir != expected_dir or original_name != expected_name:
            raise PlanError(f"计划清理项的路径、目录或文件名不一致: {item.source_path}")
        source_prefix = source_root_folded.rstrip("/") + "/"
        if _collision_key(source_path) != source_root_folded and not _collision_key(source_path).startswith(
            source_prefix
        ):
            raise PlanError(f"计划清理项不在 source_root 下: {source_path}")
        source_key = _collision_key(source_path)
        if source_key in source_paths:
            raise PlanError(f"清理文件与媒体计划重复: {source_path}")
        safe_generated_cleanup = (
            cleanup_reason(original_name) == item.reason
            or _contextual_cleanup_reason(
                {"name": original_name, "full_path": source_path}
            ) == item.reason
        )
        if (
            not safe_generated_cleanup
            and item.reason == "经特典目录与同集正片交叉确认的片头/片尾视频"
            and source_key in contextual_cleanup_keys
        ):
            safe_generated_cleanup = True
        if not safe_generated_cleanup and item.reason.startswith(
            "同一 TMDB 电影 movie/"
        ):
            identity_match = re.match(
                r"^同一 TMDB 电影 movie/(\d+) 已有更高清晰度版本 ",
                item.reason,
            )
            cleanup_tmdb_id = (
                int(identity_match.group(1)) if identity_match else None
            )
            loser_rank = video_resolution_rank(
                {"name": original_name, "full_path": source_path}
            )
            loser_variant = _batch_movie_source_variant_key(
                original_name,
                source_path,
            )
            safe_generated_cleanup = (
                cleanup_tmdb_id is not None
                and _plan_has_movie_tmdb_id(plan, cleanup_tmdb_id)
                and any(
                    item.reason
                    == _lower_resolution_movie_cleanup_reason(
                        winner.source_path,
                        cleanup_tmdb_id,
                    )
                    and winner.media_kind == "video"
                    and (
                        _planned_movie_tmdb_id(plan, winner) == cleanup_tmdb_id
                        or (
                            _planned_movie_tmdb_id(plan, winner) is None
                            and _same_concrete_movie_release(
                                original_name,
                                source_path,
                                winner,
                            )
                        )
                    )
                    and _batch_movie_variant_key(winner) == loser_variant
                    and (
                        winner_rank := video_resolution_rank(
                            {
                                "name": winner.original_name,
                                "full_path": winner.source_path,
                            }
                        )
                    ) > loser_rank
                    and (loser_rank > 0 or winner_rank >= 2160)
                    for winner in plan.files
                )
            )
        if not safe_generated_cleanup and item.reason.startswith(
            "同一 TMDB 集号已有更高清晰度版本 "
        ):
            loser_edition = edition_tag(original_name) or edition_tag(source_path)
            loser_rank = video_resolution_rank(
                {"name": original_name, "full_path": source_path}
            )
            safe_generated_cleanup = any(
                item.reason == _lower_resolution_cleanup_reason(winner.source_path)
                and winner.media_kind == "video"
                and (
                    edition_tag(winner.original_name)
                    or edition_tag(winner.source_path)
                ) == loser_edition
                and (
                    winner_rank := video_resolution_rank(
                        {
                            "name": winner.original_name,
                            "full_path": winner.source_path,
                        }
                    )
                ) > loser_rank
                and (loser_rank > 0 or winner_rank >= 2160)
                for winner in plan.files
            )
        if not safe_generated_cleanup and item.reason.startswith(
            "同一 TMDB 集号的更高清晰度版本已有对应字幕 "
        ):
            loser_rank = video_resolution_rank(
                {"name": original_name, "full_path": source_path}
            )
            loser_key = extract_episode_key(original_name)
            safe_generated_cleanup = any(
                item.reason
                == _lower_resolution_subtitle_cleanup_reason(winner.source_path)
                and winner.media_kind == "subtitle"
                and Path(winner.original_name).suffix.lower()
                == Path(original_name).suffix.lower()
                and loser_key is not None
                and extract_episode_key(winner.original_name) == loser_key
                and (
                    winner_rank := video_resolution_rank(
                        {
                            "name": winner.original_name,
                            "full_path": winner.source_path,
                        }
                    )
                ) > loser_rank
                and (loser_rank > 0 or winner_rank >= 2160)
                for winner in plan.files
            )
        if not safe_generated_cleanup and item.reason.startswith(
            "同一 TMDB 集号已有同清晰度的内封/软字幕版本 "
        ):
            loser = {"name": original_name, "full_path": source_path}
            loser_edition = edition_tag(original_name) or edition_tag(source_path)
            loser_resolution = video_resolution_rank(loser)
            loser_presentation = subtitle_presentation_rank(loser)
            safe_generated_cleanup = any(
                item.reason == _burned_subtitle_cleanup_reason(winner.source_path)
                and winner.media_kind == "video"
                and (
                    edition_tag(winner.original_name)
                    or edition_tag(winner.source_path)
                ) == loser_edition
                and loser_resolution > 0
                and loser_resolution == video_resolution_rank(
                    {
                        "name": winner.original_name,
                        "full_path": winner.source_path,
                    }
                )
                and loser_presentation < subtitle_presentation_rank(
                    {
                        "name": winner.original_name,
                        "full_path": winner.source_path,
                    }
                )
                for winner in plan.files
            )
        if not safe_generated_cleanup and item.reason.startswith(
            "同一 TMDB 集号已有同清晰度但文件更完整的版本 "
        ):
            loser = {"name": original_name, "full_path": source_path}
            loser_edition = edition_tag(original_name) or edition_tag(source_path)
            loser_resolution = video_resolution_rank(loser)
            loser_presentation = subtitle_presentation_rank(loser)
            safe_generated_cleanup = any(
                item.reason == _same_resolution_cleanup_reason(winner.source_path)
                and winner.media_kind == "video"
                and (
                    edition_tag(winner.original_name)
                    or edition_tag(winner.source_path)
                ) == loser_edition
                and loser_resolution == video_resolution_rank(
                    {
                        "name": winner.original_name,
                        "full_path": winner.source_path,
                    }
                )
                and loser_presentation
                == subtitle_presentation_rank(
                    {
                        "name": winner.original_name,
                        "full_path": winner.source_path,
                    }
                )
                and item.source_size is not None
                and winner.source_size is not None
                and winner.source_size > item.source_size > 0
                for winner in plan.files
            )
        if not safe_generated_cleanup and item.reason.startswith(
            "同一 TMDB 集号已有同清晰度同字幕形态的简体中文字幕版本 "
        ):
            loser = {"name": original_name, "full_path": source_path}
            loser_edition = edition_tag(original_name) or edition_tag(source_path)
            loser_resolution = video_resolution_rank(loser)
            loser_presentation = subtitle_presentation_rank(loser)
            safe_generated_cleanup = any(
                item.reason
                == _traditional_language_cleanup_reason(winner.source_path)
                and winner.media_kind == "video"
                and (
                    edition_tag(winner.original_name)
                    or edition_tag(winner.source_path)
                ) == loser_edition
                and loser_resolution
                == video_resolution_rank(
                    {
                        "name": winner.original_name,
                        "full_path": winner.source_path,
                    }
                )
                and loser_presentation
                == subtitle_presentation_rank(
                    {
                        "name": winner.original_name,
                        "full_path": winner.source_path,
                    }
                )
                and is_traditional_sub(source_path)
                and is_simplified_sub(winner.source_path)
                for winner in plan.files
            )
        if not safe_generated_cleanup:
            raise PlanError(f"清理文件不再符合安全规则，拒绝删除: {source_path}")
        source_paths.add(source_key)

    # 两阶段重命名发生在源目录中。提前阻止未纳入计划的文件或目录占用最终名称。
    source_listings: dict[str, list[dict[str, Any]]] = {}
    for source_dir in sorted({normalize_remote_path(item.source_dir) for item in plan.files}):
        content = alist.try_list(source_dir, refresh=True)
        if content is None:
            raise PlanError(f"无法读取源目录: {source_dir}")
        source_listings[source_dir] = content

    for item in plan.files:
        source_dir = normalize_remote_path(item.source_dir)
        final_folded = _collision_key(item.final_name)
        occupants = [
            entry
            for entry in source_listings[source_dir]
            if isinstance(entry.get("name"), str)
            and _collision_key(str(entry["name"])) == final_folded
        ]
        for occupant in occupants:
            occupant_name = str(occupant["name"])
            occupying_item = source_items_by_location.get(
                (_collision_key(source_dir), _collision_key(occupant_name))
            )
            if occupying_item is item:
                continue
            if occupying_item is not None and occupying_item.requires_rename:
                continue
            kind = "目录" if occupant.get("is_dir") else "文件"
            raise PlanError(
                f"源目录已有未被安全移开的同名{kind}: {source_dir}/{occupant_name}"
            )

    # 目标目录存在时，检查所有同名条目（包括目录）。计划内会先改为临时名的文件可放行。
    # 视频还必须检查“同规范 basename，不同扩展名”。扩展名不是
    # 内容身份：``Show - S01E01.mkv`` 与 ``Show - S01E01.mp4``
    # 依然是同一官方集。过去只检查完整文件名，会将后者静默搬入
    # 已有前者的媒体库。在没有完成跨库 ffprobe/字幕/画质比较前，
    # 安全行为是拒绝执行，而不是制造双版本。
    target_dirs = sorted({normalize_remote_path(item.target_dir) for item in plan.files})

    for target_dir in target_dirs:
        content = alist.try_list(target_dir, refresh=True)
        if content is None:
            continue
        existing_videos_by_companion: dict[tuple[str, str], list[Mapping[str, Any]]] = (
            defaultdict(list)
        )
        for entry in content:
            name = entry.get("name")
            if (
                not entry.get("is_dir")
                and isinstance(name, str)
                and Path(name).suffix.lower() in VIDEO_EXTS
            ):
                existing_videos_by_companion[
                    _planned_companion_key(target_dir, name)
                ].append(entry)
        for item in plan.files:
            if _collision_key(normalize_remote_path(item.target_dir)) != _collision_key(target_dir):
                continue
            for entry in content:
                name = entry.get("name")
                if not isinstance(name, str) or _collision_key(name) != _collision_key(item.final_name):
                    continue
                current_final_path = join_remote(target_dir, name)
                if _collision_key(current_final_path) == _collision_key(normalize_remote_path(item.source_path)):
                    continue
                occupying_item = source_items.get(_collision_key(current_final_path))
                if (
                    occupying_item is not None
                    and not entry.get("is_dir")
                    and occupying_item.requires_rename
                ):
                    continue
                kind = "目录" if entry.get("is_dir") else "文件"
                raise FormalTargetConflictError(
                    f"目标目录已存在同名{kind}: {current_final_path}"
                )
            if item.media_kind != "video":
                continue
            for entry in existing_videos_by_companion.get(
                _planned_companion_key(target_dir, item.final_name), []
            ):
                name = str(entry["name"])
                current_path = join_remote(target_dir, name)
                if _collision_key(current_path) == _collision_key(
                    normalize_remote_path(item.source_path)
                ):
                    continue
                if _collision_key(name) == _collision_key(item.final_name):
                    # The same-name case was already checked above; retaining
                    # this branch prevents an unusual extension from bypassing
                    # the exact-name conflict check.
                    raise FormalTargetConflictError(
                        f"目标目录已存在同名视频: {current_path}"
                    )
                occupying_item = source_items.get(_collision_key(current_path))
                if occupying_item is not None and occupying_item.requires_rename:
                    continue
                raise FormalTargetConflictError(
                    "目标目录已存在同集不同扩展名视频，未完成跨库质量校验，"
                    f"拒绝生成重复版本: {current_path} 与 "
                    f"{join_remote(target_dir, item.final_name)}"
                )


def _expected_single_tv_episode_count(
    files: Sequence[Mapping[str, Any]],
) -> int | None:
    """Return a complete contiguous episode boundary, never a partial count."""
    explicit_seasons: set[int] = set()
    numbers: set[int] = set()
    for item in files:
        if Path(str(item.get("name", ""))).suffix.lower() not in VIDEO_EXTS:
            continue
        full_path = str(item.get("full_path", ""))
        for segment in full_path.rsplit("/", 1)[0].split("/"):
            season_number = _season_from_source("/" + segment)
            if season_number is not None and season_number > 0:
                explicit_seasons.add(season_number)
        key = extract_episode_key(str(item.get("name", "")))
        if key is not None and key.kind == "regular" and not key.end_number and key.number > 0:
            numbers.add(key.number)
    if len(explicit_seasons) > 1 or not numbers:
        return None
    maximum = max(numbers)
    return maximum if numbers == set(range(1, maximum + 1)) else None




_core_module = sys.modules[__name__]
_movie_planner.bind_compat_runtime(_core_module)
_tv_smart_planner.bind_compat_runtime(_core_module)
_tv_season_inference.bind_compat_runtime(_core_module)
_remote_paths.bind_compat_runtime(_core_module)
_media_naming.bind_compat_runtime(_core_module)
_plan_artifacts.bind_compat_runtime(_core_module)
del _core_module


__all__ = [
    "AListClient",
    "ApiError",
    "FormalTargetConflictError",
    "PlanError",
    "ScraperError",
    "TMDBClient",
    "_expected_single_tv_episode_count",
    "_media_context_from_source_and_target",
    "_media_type_from_source_context",
    "_query_from_source",
    "_season_from_source",
    "_source_is_animation_library",
    "auto_match_tmdb",
    "build_collection_plan",
    "build_movie_plan",
    "build_tv_plan_smart",
    "join_remote",
    "normalize_exported_srt_entries",
    "planned_artwork",
    "planned_nfos",
    "split_remote",
    "subtitle_language",
    "validate_plan",
]
