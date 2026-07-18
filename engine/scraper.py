#!/usr/bin/env python3
"""安全的 TMDB 元数据刮削与 AList 媒体文件整理工具。

默认只生成并保存计划（dry-run）。实际修改必须加载已保存计划，提交其
SHA-256，并显式传入 ``--execute``；实时重新扫描的计划禁止直接执行。
"""

from __future__ import annotations

import argparse
import difflib
import getpass
import hashlib
import html
import ipaddress
import json
import os
import re
import sys
import unicodedata
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from collections import defaultdict
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

__version__ = "3.3.2"
PLAN_SCHEMA_VERSION = 3
SUPPORTED_PLAN_SCHEMA_VERSIONS = {2, 3}

DEFAULT_ALIST_URL = "http://127.0.0.1:5244"
DEFAULT_TMDB_BASE = "https://api.themoviedb.org/3"
DEFAULT_IMAGE_BASE = "https://image.tmdb.org/t/p/original"
MAX_JSON_RESPONSE_BYTES = 16 * 1024 * 1024
MAX_POSTER_BYTES = 32 * 1024 * 1024
MAX_ERROR_BODY_BYTES = 4096
LOCK_PREFIX = ".scraper-lock-"

VIDEO_EXTS = {
    ".mkv", ".mp4", ".m4v", ".m2ts", ".ts", ".avi", ".wmv", ".mov",
    ".webm", ".flv", ".mpeg", ".mpg", ".rmvb", ".strm",
}
SUBTITLE_EXTS = {".ass", ".srt", ".ssa", ".sub", ".idx", ".vtt", ".sup"}
MEDIA_EXTS = VIDEO_EXTS | SUBTITLE_EXTS

# 仅匹配独立标签，不会把 Whisper、Display 等普通单词误判为 SP。
IGNORED_EXTRA_TAG_RE = re.compile(
    r"(?:^|[\s._\-\[\]()])(?:NCOP|NCED|PV|MENU|FONTS?|EXTRAS?)(?:\d+)?(?:$|[\s._\-\[\]()])",
    re.IGNORECASE,
)
SAMPLE_RE = re.compile(
    r"(?:^|[\s._\-\[\]()])(?:sample|样片|试看)(?:$|[\s._\-\[\]()])",
    re.IGNORECASE,
)
BONUS_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("behindthescenes", re.compile(r"behind[ ._-]*the[ ._-]*scenes|幕后", re.IGNORECASE)),
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
BONUS_CONTAINER_RE = re.compile(
    r"^(?:extras?|trailers?|behind[ ._-]*the[ ._-]*scenes|deleted[ ._-]*scenes|"
    r"featurettes?|interviews?|scenes?|shorts?|花絮|预告|幕后|访谈)$",
    re.IGNORECASE,
)
EDITION_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = tuple(
    (label, re.compile(pattern, re.IGNORECASE))
    for label, pattern in (
        ("Director's Cut", r"director'?s[ ._-]*cut|导演剪辑"),
        ("Extended Cut", r"extended[ ._-]*cut|加长版"),
        ("Theatrical Cut", r"theatrical[ ._-]*cut|院线版"),
        ("Final Cut", r"final[ ._-]*cut"),
        ("Unrated Cut", r"unrated[ ._-]*cut|未分级"),
        ("IMAX", r"(?:^|[ ._\-\[\]()])imax(?:$|[ ._\-\[\]()])"),
        ("Special Edition", r"special[ ._-]*edition|特别版"),
    )
)
MULTI_EPISODE_RE = re.compile(
    r"(?:^|[^0-9])(?:E|EP)?\s*0*(\d{1,3})\s*[-~+&]\s*(?:E|EP)?\s*0*(\d{1,3})(?:$|[^0-9])",
    re.IGNORECASE,
)
MULTI_EPISODE_CONCAT_RE = re.compile(
    r"(?:^|[^A-Za-z0-9])(?:S\d{1,2})?E\s*0*(\d{1,3})E\s*0*(\d{1,3})(?:$|[^0-9])",
    re.IGNORECASE,
)
EPISODE_NOISE_RE = re.compile(
    r"(?:4320|2160|1440|1080|720|576|480)[pi]|(?:4|8)k|x26[45]|h\.?26[45]|"
    r"10bit|8bit|av1|(?:1|2|5|7)\.1|(?:1|2)\.0",
    re.IGNORECASE,
)
DATE_NOISE_RE = re.compile(r"\b(?:19|20)\d{2}[-._]\d{1,2}[-._]\d{1,2}\b")

SIMPLIFIED_MARKERS = (
    "简体",
    "简中",
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
    "cht",
    "zh-tw",
    "zh-hk",
    "zh_hant",
    "zh-hant",
    "big5",
)
ENGLISH_MARKERS = ("english", "eng", "en")
JAPANESE_MARKERS = ("japanese", "jpn", "jp", "ja")


def _redact_url(url: str) -> str:
    """从错误消息中移除 URL 查询凭据。"""
    try:
        parsed = urllib.parse.urlsplit(url)
        query = urllib.parse.parse_qsl(parsed.query, keep_blank_values=True)
        redacted = [
            (
                key,
                "<redacted>"
                if key.lower()
                in {"api_key", "token", "access_token", "password", "passwd", "authorization"}
                else value,
            )
            for key, value in query
        ]
        hostname = parsed.hostname or ""
        if ":" in hostname and not hostname.startswith("["):
            hostname = f"[{hostname}]"
        port = f":{parsed.port}" if parsed.port is not None else ""
        netloc = f"<redacted>@{hostname}{port}" if parsed.username is not None else f"{hostname}{port}"
        return urllib.parse.urlunsplit(
            (
                parsed.scheme,
                netloc,
                parsed.path,
                urllib.parse.urlencode(redacted),
                "<redacted>" if parsed.fragment else "",
            )
        )
    except ValueError:
        return "<invalid-url>"


SENSITIVE_FIELD_RE = re.compile(
    r'(?i)("?(?:api[_-]?key|password|passwd|token|access[_-]?token|authorization)"?\s*[:=]\s*)'
    r'("?)([^"\s,;&}]+)("?)'
)


def _redact_sensitive_text(text: str, secrets: Iterable[str] = ()) -> str:
    """清除错误正文中的凭据、控制字符和超长内容。"""
    cleaned = "".join(
        "?" if unicodedata.category(char) in {"Cc", "Cf", "Cs"} else char
        for char in str(text)
    )
    for secret in sorted({value for value in secrets if isinstance(value, str) and value}, key=len, reverse=True):
        cleaned = cleaned.replace(secret, "<redacted>")
    cleaned = SENSITIVE_FIELD_RE.sub(r'\1"<redacted>"', cleaned)
    return cleaned.replace("\r", " ").replace("\n", " ")[:500]


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


def _extract_year(value: Any) -> str:
    text = str(value or "")
    match = re.match(r"^(19|20)\d{2}(?:$|[-/])", text)
    return match.group(0)[:4] if match else "未知年份"


def _entry_hash_value(entry: Mapping[str, Any]) -> str | None:
    for key in ("hash_info", "hash", "etag", "sha1", "sha256", "md5"):
        value = entry.get(key)
        if value not in (None, "", {}, []):
            if isinstance(value, (dict, list)):
                return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
            return str(value)
    return None


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
    return PlannedFile(
        source_path=source_path,
        source_dir=source_dir,
        original_name=original_name,
        final_name=final_name,
        target_dir=target_dir,
        media_kind=media_kind(original_name),
        episode_key=episode_key,
        source_size=_entry_size_value(item),
        source_modified=_entry_modified_value(item),
        source_hash=_entry_hash_value(item),
    )


def _has_unsafe_unicode(text: str) -> bool:
    return any(unicodedata.category(char) in {"Cc", "Cf", "Cs"} for char in text)


def _terminal_text(value: Any) -> str:
    return "".join(
        "?" if unicodedata.category(char) in {"Cc", "Cf", "Cs"} else char
        for char in str(value)
    )


def _validate_remote_basename(name: str) -> str:
    if not isinstance(name, str) or not name or name in {".", ".."}:
        raise ValueError(f"无效远端文件名: {name!r}")
    if "/" in name or "\\" in name or _has_unsafe_unicode(name):
        raise ValueError(f"远端文件名包含非法或不可见控制字符: {name!r}")
    return name


class ScraperError(RuntimeError):
    """可向用户显示的预期错误。"""


class ApiError(ScraperError):
    """远端 API 调用失败。"""

    def __init__(self, message: str, *, status_code: int | None = None) -> None:
        super().__init__(message)
        self.status_code = status_code


class PlanError(ScraperError):
    """操作计划不安全或不完整。"""


class PartialMoveError(ApiError):
    """移动请求只完成了一部分；moved_names 需要由回滚流程处理。"""

    def __init__(self, message: str, moved_names: Sequence[str]) -> None:
        super().__init__(message)
        self.moved_names = list(moved_names)


@dataclass(frozen=True, order=True)
class EpisodeKey:
    """源文件中识别出的集号。kind 为 regular、special 或 fractional。"""

    kind: str
    number: int
    end_number: int = 0

    @property
    def display(self) -> str:
        if self.kind == "fractional":
            return f"E{self.number:02d}.5"
        prefix = "SP" if self.kind == "special" else "E"
        start = f"{prefix}{self.number:02d}"
        if self.end_number and self.end_number != self.number:
            return f"{start}-{prefix}{self.end_number:02d}"
        return start


@dataclass
class PlannedFile:
    source_path: str
    source_dir: str
    original_name: str
    final_name: str
    target_dir: str
    media_kind: str
    episode_key: str | None = None
    source_size: int | None = None
    source_modified: str | None = None
    source_hash: str | None = None

    @property
    def requires_rename(self) -> bool:
        return self.original_name != self.final_name


@dataclass
class Plan:
    mode: str
    source_root: str
    target_root: str
    files: list[PlannedFile]
    warnings: list[str]
    metadata: dict[str, Any]


@dataclass
class ExecutionRecord:
    action: str
    source: str
    target: str
    status: str
    message: str = ""


@dataclass
class ExecutionJournal:
    created_at: str
    plan: dict[str, Any]
    records: list[ExecutionRecord]
    success: bool = False

    def save(self, path: Path) -> None:
        payload = {
            "created_at": self.created_at,
            "plan_sha256": plan_sha256(self.plan),
            "plan": self.plan,
            "records": [asdict(item) for item in self.records],
            "success": self.success,
        }
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
        descriptor = os.open(temporary, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        try:
            with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
                json.dump(payload, handle, ensure_ascii=False, indent=2, allow_nan=False)
                handle.write("\n")
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, path)
        finally:
            try:
                temporary.unlink()
            except FileNotFoundError:
                pass


@dataclass
class RecoveryState:
    item: PlannedFile
    current_dir: str
    current_name: str
    entry: Mapping[str, Any]


@dataclass(frozen=True)
class AutoMatch:
    media_type: str
    tmdb_id: int
    title: str
    year: str
    confidence: float


class JsonHttpClient:
    def __init__(self, timeout: float = 20.0, retries: int = 3) -> None:
        if timeout <= 0:
            raise ValueError("timeout 必须大于 0")
        if retries < 0:
            raise ValueError("retries 不能小于 0")
        self.timeout = timeout
        self.retries = retries

    def request_json(
        self,
        url: str,
        *,
        method: str = "GET",
        headers: Mapping[str, str] | None = None,
        json_body: Mapping[str, Any] | None = None,
        raw_body: bytes | None = None,
        retryable: bool = True,
    ) -> dict[str, Any]:
        if json_body is not None and raw_body is not None:
            raise ValueError("json_body 与 raw_body 不能同时提供")

        request_headers = dict(headers or {})
        body = raw_body
        if json_body is not None:
            body = json.dumps(json_body, ensure_ascii=False).encode("utf-8")
            request_headers.setdefault("Content-Type", "application/json")

        secret_values: set[str] = set()
        try:
            parsed_url = urllib.parse.urlsplit(url)
            for key, value in urllib.parse.parse_qsl(parsed_url.query, keep_blank_values=True):
                if key.lower() in {"api_key", "token", "access_token", "password"} and value:
                    secret_values.add(value)
        except ValueError:
            pass
        for key, value in request_headers.items():
            if key.lower() in {"authorization", "x-api-key"} and value:
                secret = str(value)
                secret_values.add(secret)
                if key.lower() == "authorization" and " " in secret:
                    # 同时清除 Bearer/Basic 等方案后的凭据部分，防止服务端只回显 token。
                    scheme, credential = secret.split(None, 1)
                    if scheme and credential:
                        secret_values.add(credential)
        if json_body is not None:
            for key, value in json_body.items():
                if key.lower() in {"api_key", "token", "access_token", "password", "passwd"} and value:
                    secret_values.add(str(value))

        last_error: Exception | None = None
        max_attempts = self.retries + 1 if retryable else 1
        for attempt in range(max_attempts):
            try:
                req = urllib.request.Request(url, data=body, method=method, headers=request_headers)
                with urllib.request.urlopen(req, timeout=self.timeout) as response:
                    raw = response.read(MAX_JSON_RESPONSE_BYTES + 1)
                if len(raw) > MAX_JSON_RESPONSE_BYTES:
                    raise ApiError(
                        f"接口响应超过 {MAX_JSON_RESPONSE_BYTES} 字节上限: {_redact_url(url)}"
                    )
                try:
                    parsed = json.loads(raw.decode("utf-8"))
                except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                    raise ApiError(f"接口返回的不是有效 JSON: {_redact_url(url)}") from exc
                if not isinstance(parsed, dict):
                    raise ApiError(f"接口返回格式异常: {_redact_url(url)}")
                return parsed
            except urllib.error.HTTPError as exc:
                last_error = exc
                status_retryable = exc.code == 429 or 500 <= exc.code < 600
                if not retryable or not status_retryable or attempt >= max_attempts - 1:
                    detail = ""
                    try:
                        detail = _redact_sensitive_text(
                            exc.read(MAX_ERROR_BODY_BYTES + 1).decode("utf-8", errors="replace"),
                            secret_values,
                        )
                    except Exception:
                        pass
                    finally:
                        exc.close()
                    suffix = f"; {detail}" if detail else ""
                    raise ApiError(
                        f"HTTP {exc.code}: {_redact_url(url)}{suffix}",
                        status_code=exc.code,
                    ) from exc
                exc.close()
            except (urllib.error.URLError, TimeoutError, OSError) as exc:
                last_error = exc
                if attempt >= max_attempts - 1:
                    raise ApiError(
                        f"网络请求失败: {_redact_url(url)}; "
                        f"{_redact_sensitive_text(str(exc), secret_values)}"
                    ) from exc

            time.sleep(min(2**attempt, 8))

        raise ApiError(
            f"网络请求失败: {_redact_url(url)}; "
            f"{_redact_sensitive_text(str(last_error), secret_values)}"
        )

    def request_bytes(
        self,
        url: str,
        *,
        headers: Mapping[str, str] | None = None,
        max_bytes: int = MAX_POSTER_BYTES,
    ) -> bytes:
        if max_bytes <= 0:
            raise ValueError("max_bytes 必须大于 0")
        request_headers = dict(headers or {})
        secret_values: set[str] = set()
        try:
            parsed_url = urllib.parse.urlsplit(url)
            for key, value in urllib.parse.parse_qsl(parsed_url.query, keep_blank_values=True):
                if key.lower() in {
                    "api_key", "token", "access_token", "password", "passwd", "authorization"
                } and value:
                    secret_values.add(value)
        except ValueError:
            pass
        for key, value in request_headers.items():
            if key.lower() in {"authorization", "x-api-key"} and value:
                secret = str(value)
                secret_values.add(secret)
                if key.lower() == "authorization" and " " in secret:
                    # 同时清除 Bearer/Basic 等方案后的凭据部分，防止服务端只回显 token。
                    scheme, credential = secret.split(None, 1)
                    if scheme and credential:
                        secret_values.add(credential)

        last_error: Exception | None = None
        for attempt in range(self.retries + 1):
            try:
                req = urllib.request.Request(url, headers=request_headers)
                with urllib.request.urlopen(req, timeout=self.timeout) as response:
                    data = response.read(max_bytes + 1)
                if len(data) > max_bytes:
                    raise ApiError(
                        f"下载内容超过 {max_bytes} 字节上限: {_redact_url(url)}"
                    )
                return data
            except urllib.error.HTTPError as exc:
                last_error = exc
                retryable = exc.code == 429 or 500 <= exc.code < 600
                if not retryable or attempt >= self.retries:
                    exc.close()
                    raise ApiError(
                        f"下载失败，HTTP {exc.code}: {_redact_url(url)}",
                        status_code=exc.code,
                    ) from exc
                exc.close()
            except (urllib.error.URLError, TimeoutError, OSError) as exc:
                last_error = exc
                if attempt >= self.retries:
                    raise ApiError(
                        f"下载失败: {_redact_url(url)}; "
                        f"{_redact_sensitive_text(str(exc), secret_values)}"
                    ) from exc
            time.sleep(min(2**attempt, 8))
        raise ApiError(
            f"下载失败: {_redact_url(url)}; "
            f"{_redact_sensitive_text(str(last_error), secret_values)}"
        )


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
        self.http = JsonHttpClient(timeout=timeout, retries=retries)
        self.token: str | None = None

    def login(self) -> str:
        response = self.http.request_json(
            f"{self.base_url}/api/auth/login",
            method="POST",
            json_body={"username": self.username, "password": self.password},
        )
        self._require_success(response, "AList 登录")
        token = (response.get("data") or {}).get("token")
        if not isinstance(token, str) or not token:
            raise ApiError("AList 登录成功但未返回 token")
        self.token = token
        self.password = ""
        return token

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
                f"{_redact_sensitive_text(str(detail), (self.password, self.token or ''))}"
            )
        return dict(response)

    def call(
        self, endpoint: str, body: Mapping[str, Any], *, retryable: bool = False
    ) -> dict[str, Any]:
        response = self.http.request_json(
            f"{self.base_url}/api/fs/{endpoint}",
            method="POST",
            headers=self._headers(),
            json_body=body,
            retryable=retryable,
        )
        return self._require_success(response, f"AList {endpoint}")

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
    ) -> list[dict[str, Any]]:
        if max_directories <= 0 or max_files <= 0:
            raise ValueError("max_directories 与 max_files 必须大于 0")
        root = normalize_remote_path(path)
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
                    _validate_remote_basename(name)
                except ValueError as exc:
                    raise PlanError(
                        f"AList 返回了无法安全表示的条目名称: {current}/{name!r}"
                    ) from exc
                full_path = join_remote(current, name)
                item["full_path"] = full_path
                if is_scraper_lock(name):
                    raise PlanError(
                        f"发现仍在执行或上次异常退出遗留的整理锁，已停止: {full_path}。"
                        "请确认没有其他整理任务运行，再使用 --inspect-journal 或 "
                        "--recover-journal 处理对应任务。"
                    )
                if is_scraper_temp(name):
                    if not ignore_orphan_temp:
                        raise PlanError(
                            f"发现上次中断遗留的临时条目，已停止: {full_path}。"
                            "请依据对应 journal 恢复或人工核对；只有明确接受遗漏风险时才使用 "
                            "--ignore-orphan-temp。"
                        )
                    continue
                if item.get("is_dir"):
                    if should_ignore_extra(name) and not (
                        include_bonus and BONUS_CONTAINER_RE.fullmatch(name.strip(" []()"))
                    ):
                        continue
                    stack.append(full_path)
                else:
                    if should_ignore_extra(name) and not (
                        include_bonus and bonus_type(name) is not None
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
        clean_names = [_validate_remote_basename(name) for name in names]
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
        except ApiError:
            # 部分 AList 存储对已存在目录返回错误。只有目录确实可列出时才视为成功。
            if self.try_list(normalized) is None:
                raise

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
        response = self.http.request_json(f"{self.base_url}/api/public/settings")
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
        response = self.call(
            "archive/meta",
            {
                "path": normalize_remote_path(path),
                "refresh": refresh,
                "archive_pass": archive_password,
            },
            retryable=False,
        )
        data = response.get("data") or {}
        if not isinstance(data, Mapping):
            raise ApiError("AList archive/meta 返回格式异常")
        return dict(data)

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
        response = self.http.request_json(
            f"{self.base_url}/api/task/{kind}/{state}",
            headers=self._headers(),
        )
        self._require_success(response, f"AList {kind} 任务查询")
        data = response.get("data")
        if data is None:
            return []
        if not isinstance(data, list) or not all(isinstance(item, dict) for item in data):
            raise ApiError(f"AList {kind} 任务返回格式异常")
        return [dict(item) for item in data]

    def upload_bytes(self, target_path: str, data: bytes, content_type: str) -> None:
        response = self.http.request_json(
            f"{self.base_url}/api/fs/put",
            method="PUT",
            headers={
                **self._headers(),
                "File-Path": urllib.parse.quote(normalize_remote_path(target_path)),
                "Content-Type": content_type,
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
        base_url: str = DEFAULT_TMDB_BASE,
        language: str = "zh-CN",
        timeout: float = 20.0,
        retries: int = 3,
    ) -> None:
        if not api_key:
            raise ScraperError("缺少 TMDB API Key，请设置 TMDB_API_KEY 或使用 --tmdb-key-file")
        self.api_key = api_key
        self.base_url = base_url.rstrip("/")
        self.language = language
        self.http = JsonHttpClient(timeout=timeout, retries=retries)

    def get(self, path: str, **params: Any) -> dict[str, Any]:
        if not path.startswith("/"):
            path = "/" + path
        query = {"api_key": self.api_key, "language": self.language}
        query.update({key: value for key, value in params.items() if value is not None})
        url = f"{self.base_url}{path}?{urllib.parse.urlencode(query, doseq=True)}"
        response = self.http.request_json(
            url, headers={"User-Agent": f"alist-tmdb-scraper/{__version__}"}
        )
        if response.get("success") is False:
            status_code = response.get("status_code")
            raise ApiError(
                "TMDB 请求失败: "
                + _redact_sensitive_text(
                    str(response.get("status_message") or response), (self.api_key,)
                ),
                status_code=int(status_code) if isinstance(status_code, int) else None,
            )
        return response

    def download_poster(self, poster_path: str) -> bytes:
        return self.http.request_bytes(f"{DEFAULT_IMAGE_BASE}{poster_path}")


# ---------------------------------------------------------------------------
# 兼容旧辅助脚本的函数式接口。凭据与 URL 必须来自环境变量或参数。
# ---------------------------------------------------------------------------

ALIST = os.getenv("ALIST_URL", DEFAULT_ALIST_URL)
TMDB_BASE = os.getenv("TMDB_BASE_URL", DEFAULT_TMDB_BASE)
TMDB_KEY = os.getenv("TMDB_API_KEY", "")
_COMPAT_CLIENTS: dict[str, AListClient] = {}


def alist_login(
    password: str,
    username: str = "admin",
    alist_url: str | None = None,
    *,
    allow_insecure_http: bool = False,
) -> str:
    client = AListClient(
        alist_url or ALIST,
        username,
        password,
        allow_insecure_http=allow_insecure_http,
    )
    token = client.login()
    _COMPAT_CLIENTS[token] = client
    return token


def _compat_client(token: str) -> AListClient:
    client = _COMPAT_CLIENTS.get(token)
    if client is None:
        client = AListClient(ALIST, "admin", "")
        client.token = token
        _COMPAT_CLIENTS[token] = client
    return client


def alist_list(token: str, path: str, refresh: bool = False) -> list[dict[str, Any]]:
    return _compat_client(token).list(path, refresh=refresh)


def alist_walk(token: str, path: str) -> list[dict[str, Any]]:
    return _compat_client(token).walk(path)


def _require_unsafe_compat_write() -> None:
    if os.getenv("SCRAPER_ENABLE_UNSAFE_COMPAT_WRITES") != "1":
        raise ScraperError(
            "兼容写接口默认禁用，因为它们绕过计划 SHA-256、journal 与回滚。"
            "确需运行已审计的旧代码时，显式设置 "
            "SCRAPER_ENABLE_UNSAFE_COMPAT_WRITES=1。"
        )


def alist_rename(token: str, full_path: str, new_name: str) -> dict[str, Any]:
    _require_unsafe_compat_write()
    _compat_client(token).rename(full_path, new_name)
    return {"code": 200, "message": "success"}


def alist_move(token: str, src_dir: str, dst_dir: str, names: Sequence[str]) -> dict[str, Any]:
    _require_unsafe_compat_write()
    _compat_client(token).move(src_dir, dst_dir, names)
    return {"code": 200, "message": "success"}


def alist_mkdir(token: str, path: str) -> dict[str, Any]:
    _require_unsafe_compat_write()
    _compat_client(token).mkdir(path)
    return {"code": 200, "message": "success"}


def alist_remove(token: str, parent: str, names: Sequence[str]) -> dict[str, Any]:
    _require_unsafe_compat_write()
    _compat_client(token).remove(parent, names)
    return {"code": 200, "message": "success"}


def tmdb(path: str, lang: str = "zh-CN") -> dict[str, Any]:
    api_key = os.getenv("TMDB_API_KEY", TMDB_KEY)
    if not api_key:
        raise ScraperError("缺少 TMDB_API_KEY")
    if "?" in path:
        base_path, query_text = path.split("?", 1)
        params = dict(urllib.parse.parse_qsl(query_text))
    else:
        base_path, params = path, {}
    return TMDBClient(api_key, language=lang).get(base_path, **params)


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
    """返回 (目标路径, TMDB 图片路径, 用途)，并按冲突键去重。"""
    requests: list[tuple[str, str, str]] = []
    poster_path = plan.metadata.get("poster_path")
    backdrop_path = plan.metadata.get("backdrop_path")
    if isinstance(poster_path, str) and poster_path:
        requests.append((join_remote(plan.target_root, "folder.jpg"), poster_path, "folder"))
        if plan.mode == "tv":
            requests.append((join_remote(plan.target_root, "poster.jpg"), poster_path, "series-poster"))
        elif plan.mode == "movie":
            for item in plan.files:
                if item.media_kind != "video" or is_planned_bonus(item.final_name):
                    continue
                requests.append(
                    (
                        join_remote(item.target_dir, f"{Path(item.final_name).stem}.jpg"),
                        poster_path,
                        "movie-poster",
                    )
                )
    if isinstance(backdrop_path, str) and backdrop_path:
        requests.append((join_remote(plan.target_root, "fanart.jpg"), backdrop_path, "fanart"))
    season_posters = plan.metadata.get("season_posters")
    if plan.mode == "tv" and isinstance(season_posters, Mapping):
        for season_number, image_path in season_posters.items():
            if isinstance(image_path, str) and image_path:
                requests.append(
                    (
                        join_remote(plan.target_root, f"season {int(season_number)}-poster.jpg"),
                        image_path,
                        "season-poster",
                    )
                )
    member_posters = plan.metadata.get("member_posters")
    if plan.mode == "collection" and isinstance(member_posters, Mapping):
        for target_dir, image_path in member_posters.items():
            if not isinstance(target_dir, str) or not isinstance(image_path, str) or not image_path:
                continue
            requests.append((join_remote(target_dir, "folder.jpg"), image_path, "member-folder"))
            for item in plan.files:
                if item.target_dir == target_dir and item.media_kind == "video":
                    requests.append(
                        (
                            join_remote(target_dir, f"{Path(item.final_name).stem}.jpg"),
                            image_path,
                            "member-movie-poster",
                        )
                    )
    deduplicated: list[tuple[str, str, str]] = []
    seen: set[str] = set()
    for request in requests:
        key = _collision_key(request[0])
        if key not in seen:
            seen.add(key)
            deduplicated.append(request)
    return deduplicated


def planned_movie_nfos(plan: Plan) -> list[tuple[str, bytes]]:
    if plan.mode not in {"movie", "collection"}:
        return []
    output: list[tuple[str, bytes]] = []
    for item in plan.files:
        if item.media_kind != "video" or is_planned_bonus(item.final_name):
            continue
        stem = Path(item.final_name).stem
        id_match = re.search(r"\{tmdb-(\d+)\}", stem, re.IGNORECASE)
        year_match = re.search(r"\(((?:19|20)\d{2})\)", stem)
        if not id_match:
            continue
        tmdb_id = id_match.group(1)
        title = re.sub(r"\s*\((?:19|20)\d{2}\).*", "", stem).strip()
        payload = (
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
            "<movie>\n"
            f"  <title>{html.escape(title)}</title>\n"
            f"  <year>{year_match.group(1) if year_match else ''}</year>\n"
            f"  <tmdbid>{tmdb_id}</tmdbid>\n"
            f"  <uniqueid type=\"tmdb\" default=\"true\">{tmdb_id}</uniqueid>\n"
            "</movie>\n"
        ).encode("utf-8")
        output.append((join_remote(item.target_dir, f"{stem}.nfo"), payload))
    return output


def download_poster(
    token: str,
    poster_path: str | None,
    target_dir: str,
    *,
    overwrite: bool = False,
) -> bool:
    """兼容接口：默认禁用；启用后仍默认拒绝覆盖已有 folder.jpg。"""
    _require_unsafe_compat_write()
    if not poster_path:
        return False
    alist = _compat_client(token)
    target_path, preexisting = resolve_poster_target(
        alist, target_dir, overwrite=overwrite
    )
    if preexisting and not overwrite:
        raise PlanError("目标目录已有 folder.jpg；显式传入 overwrite=True 才会替换")
    api_key = os.getenv("TMDB_API_KEY", TMDB_KEY)
    client = TMDBClient(api_key)
    data = client.download_poster(poster_path)
    alist.upload_bytes(target_path, data, "image/jpeg")
    return True


# ---------------------------------------------------------------------------
# 路径、命名与集数识别
# ---------------------------------------------------------------------------


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


def _limit_filename(name: str, max_bytes: int = 240) -> str:
    suffix = Path(name).suffix
    stem = name[: -len(suffix)] if suffix else name
    budget = max_bytes - len(suffix.encode("utf-8"))
    if budget <= 0:
        raise PlanError(f"文件扩展名过长: {name}")
    return _truncate_utf8(stem, budget) + suffix


def _compose_filename(base: str, semantic_suffix: str, extension: str, max_bytes: int = 240) -> str:
    tail = f"{semantic_suffix}{extension}"
    budget = max_bytes - len(tail.encode("utf-8"))
    if budget <= 0:
        raise PlanError(f"文件名后缀过长: {tail}")
    limited_base = _truncate_utf8(base, budget)
    return f"{limited_base}{tail}"


def is_scraper_temp(name: str) -> bool:
    return _collision_key(name).startswith(".scraper-tmp-")


def is_scraper_lock(name: str) -> bool:
    return _collision_key(name).startswith(LOCK_PREFIX)


def should_ignore_extra(name: str) -> bool:
    return bool(IGNORED_EXTRA_TAG_RE.search(name))


def is_sample(name: str) -> bool:
    return bool(SAMPLE_RE.search(name))


def bonus_type(name: str) -> str | None:
    for label, pattern in BONUS_PATTERNS:
        if pattern.search(name):
            return label
    return None


def is_planned_bonus(name: str) -> bool:
    """仅识别本工具生成的 Infuse 花絮后缀。

    不能直接对标准化后的片名再调用 bonus_type，否则《The Trailer》
    之类本身含花絮关键词的电影会被错当成预告。
    """
    return bool(PLANNED_BONUS_SUFFIX_RE.search(Path(name).stem))


def edition_tag(name: str) -> str | None:
    explicit = re.search(r"\{edition-([^{}]+)\}", name, re.IGNORECASE)
    if explicit:
        return safe_name(explicit.group(1), max_bytes=60)
    for label, pattern in EDITION_PATTERNS:
        if pattern.search(name):
            return label
    return None


def _token_present(text: str, marker: str) -> bool:
    if marker.isascii():
        return bool(
            re.search(
                rf"(?:^|[.\-_\[\]()\s]){re.escape(marker)}(?:$|[.\-_\[\]()\s])",
                text,
            )
        )
    return marker in text


def subtitle_language(name: str) -> str | None:
    lower = name.lower()
    if any(_token_present(lower, marker) for marker in SIMPLIFIED_MARKERS):
        return "zh-CN"
    if re.search(r"(?:^|[.\-_\[\]()\s])(?:sc|简)(?:$|[.\-_\[\]()\s])", lower):
        return "zh-CN"
    if any(_token_present(lower, marker) for marker in TRADITIONAL_MARKERS):
        return "zh-TW"
    if re.search(r"(?:^|[.\-_\[\]()\s])(?:tc|繁)(?:$|[.\-_\[\]()\s])", lower):
        return "zh-TW"
    if any(_token_present(lower, marker) for marker in ENGLISH_MARKERS):
        return "en"
    if any(_token_present(lower, marker) for marker in JAPANESE_MARKERS):
        return "ja"
    return None


def is_traditional_sub(name: str) -> bool:
    return subtitle_language(name) == "zh-TW"


def is_simplified_sub(name: str) -> bool:
    return subtitle_language(name) == "zh-CN"


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
    for roman, arabic in ROMAN_MAP.items():
        clean = clean.replace(roman, f" {arabic} ")

    special_patterns = [
        r"(?:^|[\s._\-\[\]()])(?:SP|SPECIAL|OVA|OAD)[\s._-]*0*(\d{1,3})(?:$|[\s._\-\[\]()])",
        r"第\s*0*(\d{1,3})\s*(?:话|集)?\s*(?:特别篇|特典)",
    ]
    for pattern in special_patterns:
        match = re.search(pattern, clean, re.IGNORECASE)
        if match:
            return EpisodeKey("special", int(match.group(1)))

    if re.search(
        r"(?:^|[\s._\-\[\]()])(?:SP|SPECIAL|OVA|OAD)(?:$|[\s._\-\[\]()])",
        clean,
        re.IGNORECASE,
    ):
        # 0 表示标签明确但编号缺失；计划阶段必须拒绝猜测。
        return EpisodeKey("special", 0)

    # 11.5 / 18.5 等通常是插播回顾篇。将其保留为独立键，
    # 必须由 --episode-map 明确映射到 TMDB 特别篇，不得并入第 11/18 集。
    fractional_match = re.search(
        r"(?:^|[\[\s_\-(])0*(\d{1,3})\.5(?:v\d+)?(?:[\]\s_\-.)]|$)",
        clean,
        re.IGNORECASE,
    )
    if fractional_match:
        return EpisodeKey("fractional", int(fractional_match.group(1)))

    # 字幕组常把修正版写成 [02v2]。v2 是文件修订号，02 才是集号。
    versioned_episode = re.search(
        r"(?:^|[\[\s_\-(])0*(\d{1,3})v\d+(?=[\]\s_\-.()]|$)",
        clean,
        re.IGNORECASE,
    )
    if versioned_episode:
        return EpisodeKey("regular", int(versioned_episode.group(1)))

    regular_patterns = [
        r"(?:^|[^A-Za-z0-9])S\d{1,2}\s*E\s*0*(\d{1,4})(?:$|[^0-9])",
        r"(?:^|[^A-Za-z0-9])(?:EP?|E)\s*0*(\d{1,4})(?:$|[\s._\-\[\]()])",
        r"第\s*0*(\d{1,4})\s*(?:话|集)",
        r"(?:^|[\[\s_\-.(])0*(\d{1,3})(?:[\]\s_\-.()]|$)",
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
        ext = Path(name).suffix.lower()
        if ext not in MEDIA_EXTS:
            continue
        multi_text = DATE_NOISE_RE.sub(" ", name)
        multi_clean = EPISODE_NOISE_RE.sub(" ", multi_text)
        multi_match = MULTI_EPISODE_RE.search(multi_clean) or MULTI_EPISODE_CONCAT_RE.search(
            multi_clean
        )
        key: EpisodeKey | None = None
        if multi_match and multi_match.group(1) != multi_match.group(2):
            start = int(multi_match.group(1))
            end = int(multi_match.group(2))
            if end < start:
                raise PlanError(f"多集文件的结束集数小于开始集数: {full_path}")
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
            ):
                key = extract_episode_key(parent_name)
        if key is not None:
            if key.kind == "special" and key.number == 0:
                raise PlanError(
                    f"检测到未编号特别篇，拒绝猜测为 SP01: {full_path}。"
                    "请将文件明确编号为 SP01、OVA.02 等格式。"
                )
            groups[key].append(item)

    if prefer_simplified:
        for key, items in list(groups.items()):
            explicit_simplified = any(
                Path(item["name"]).suffix.lower() in SUBTITLE_EXTS and is_simplified_sub(item["name"])
                for item in items
            )
            if explicit_simplified:
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
    ext = Path(name).suffix.lower()
    return "video" if ext in VIDEO_EXTS else "subtitle"


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
    unknown_sub_index = 0
    language_counts: dict[str, int] = defaultdict(int)
    pair_suffixes: dict[str, str] = {}

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
            edition = edition_tag(original) if preserve_editions else None
            if edition:
                suffix = f" {{edition-{edition}}}"
            else:
                video_index += 1
                suffix = "" if video_index == 1 else f" - v{video_index}"
            candidate = _compose_filename(base_name, suffix, ext)
        else:
            lang = subtitle_language(original)
            pair_key = re.sub(r"(?:[.\-_\[\]()\s])(?:idx|sub)$", "", stem)
            if pair_key in pair_suffixes:
                suffix = pair_suffixes[pair_key]
            elif lang:
                language_counts[lang] += 1
                count = language_counts[lang]
                suffix = f".{lang}" if count == 1 else f".{lang}.{count}"
                pair_suffixes[pair_key] = suffix
            else:
                unknown_sub_index += 1
                suffix = ".subtitle" if unknown_sub_index == 1 else f".subtitle{unknown_sub_index}"
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


def _filter_media(files: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    result = []
    for item in files:
        name = item.get("name")
        if item.get("is_dir") or not isinstance(name, str):
            continue
        if Path(name).suffix.lower() in MEDIA_EXTS:
            result.append(dict(item))
    return result


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
        available = sum(
            value is not None
            for value in (item.source_size, item.source_modified, item.source_hash)
        )
        if item.source_hash is None and available < 2:
            weak.append(item.source_path)
    if weak:
        plan.warnings.append(
            f"{len(weak)} 个文件只有单一弱快照字段；同大小或同时间戳替换可能无法识别"
        )


def _build_tv_episode_map(
    tmdb_client: TMDBClient,
    show: Mapping[str, Any],
    tmdb_id: int,
    season: int,
    absolute: bool,
    episode_group_id: str | None = None,
) -> dict[EpisodeKey, tuple[int, int, str]]:
    mapping: dict[EpisodeKey, tuple[int, int, str]] = {}
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
        mapping[EpisodeKey("special", number)] = (
            0,
            number,
            str(episode.get("name") or f"特别篇 {number}"),
        )
    return mapping


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
) -> Plan:
    show = tmdb_client.get(f"/tv/{tmdb_id}")
    title = safe_name(str(show.get("name") or show.get("original_name") or tmdb_id))
    year = _extract_year(show.get("first_air_date"))
    series_label = safe_name(f"{title} ({year}) {{tmdb-{tmdb_id}}}")
    series_dir = join_remote(parent_path, series_label)
    episode_map = _build_tv_episode_map(
        tmdb_client,
        show,
        tmdb_id,
        season,
        absolute,
        episode_group_id=episode_group_id,
    )
    episode_overrides = _load_episode_map(episode_map_path) if episode_map_path else {}

    files = alist.walk(src_path, ignore_orphan_temp=ignore_orphan_temp)
    if not any(
        Path(str(item.get("name", ""))).suffix.lower() in VIDEO_EXTS
        for item in files
    ):
        raise PlanError(
            "未找到剧集视频文件；目录可能只有字幕或未解压的分卷压缩包，"
            "已停止以避免仅移动字幕。"
        )
    all_groups = parse_ep_files(files, prefer_simplified=False)
    _raise_unparsed_media(_unparsed_media_paths(files, all_groups), "剧集")
    groups = (
        parse_ep_files(files, prefer_simplified=True)
        if prefer_simplified
        else all_groups
    )
    if not groups:
        raise PlanError("未找到可识别的剧集媒体文件")

    warnings: list[str] = []
    if ignore_orphan_temp:
        warnings.append("已显式忽略 .scraper-tmp-* 遗留条目，可能存在未恢复文件")
    planned: list[PlannedFile] = []
    unresolved: list[str] = []

    for key in sorted(groups):
        override = episode_overrides.get(key)
        if override is not None:
            override_season, override_episode, override_end = override
            official_kind = "special" if override_season == 0 else "regular"
            official = episode_map.get(EpisodeKey(official_kind, override_episode))
            mapped = (
                override_season,
                override_episode,
                official[2] if official else f"第{override_episode}集",
            )
            mapped_end = None
            if override_end:
                official_end = episode_map.get(EpisodeKey(official_kind, override_end))
                mapped_end = (
                    override_season,
                    override_end,
                    official_end[2] if official_end else f"第{override_end}集",
                )
            warnings.append(f"{key.display} 使用显式覆盖映射")
        else:
            lookup_key = EpisodeKey(key.kind, key.number)
            mapped = episode_map.get(lookup_key)
            mapped_end = (
                episode_map.get(EpisodeKey(key.kind, key.end_number))
                if key.end_number
                else None
            )
        if mapped is None:
            if not allow_unmapped:
                unresolved.append(key.display)
                continue
            if key.kind == "special":
                mapped = (0, key.number, f"特别篇 {key.number}")
            elif key.kind == "fractional":
                unresolved.append(key.display)
                continue
            elif absolute:
                unresolved.append(key.display)
                continue
            else:
                mapped = (season, key.number, f"第{key.number}集")
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
        episode_title = safe_name(raw_episode_title)
        episode_token = f"S{target_season:02d}E{target_episode:02d}"
        if mapped_end is not None:
            episode_token += f"-E{mapped_end[1]:02d}"
            episode_title = safe_name(f"{raw_episode_title} + {mapped_end[2]}")
        base_name = f"{title} - {episode_token} - {episode_title}"
        group_files = sorted(groups[key], key=lambda x: _collision_key(str(x["full_path"])))
        final_names = make_unique_media_names(base_name, group_files)
        season_dir = join_remote(series_dir, f"Season {target_season:02d}")
        for item, final_name in zip(group_files, final_names):
            planned.append(
                _planned_file_from_entry(
                    item,
                    final_name=final_name,
                    target_dir=season_dir,
                    episode_key=key.display,
                )
            )

    if unresolved:
        unresolved_text = ", ".join(sorted(set(unresolved)))
        raise PlanError(
            f"以下集数未在 TMDB 映射中找到，已停止以避免错误归档: {unresolved_text}。"
            "可核对文件名或在非绝对集数模式下使用 --allow-unmapped。"
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
        },
    )
    _add_snapshot_warnings(plan)
    validate_plan(alist, plan)
    return plan


def build_movie_plan(
    alist: AListClient,
    tmdb_client: TMDBClient,
    *,
    src_path: str,
    parent_path: str,
    tmdb_id: int,
    ignore_orphan_temp: bool = False,
) -> Plan:
    movie = tmdb_client.get(f"/movie/{tmdb_id}")
    title = safe_name(str(movie.get("title") or movie.get("original_title") or tmdb_id))
    year = _extract_year(movie.get("release_date"))
    movie_label = safe_name(f"{title} ({year}) {{tmdb-{tmdb_id}}}")
    movie_dir = join_remote(parent_path, movie_label)
    scanned_files = _filter_media(
        alist.walk(
            src_path,
            ignore_orphan_temp=ignore_orphan_temp,
            include_bonus=True,
        )
    )
    samples = [item for item in scanned_files if is_sample(str(item.get("name", "")))]
    bonus_files = [
        item
        for item in scanned_files
        if item not in samples and bonus_type(str(item.get("name", ""))) is not None
    ]
    files = [
        item for item in scanned_files if item not in samples and item not in bonus_files
    ]
    if not any(Path(str(item.get("name", ""))).suffix.lower() in VIDEO_EXTS for item in files):
        raise PlanError("未找到电影媒体文件")

    base_name = safe_name(f"{title} ({year}) {{tmdb-{tmdb_id}}}")
    files = sorted(files, key=lambda x: _collision_key(str(x["full_path"])))
    names = make_unique_media_names(base_name, files, preserve_editions=True)
    planned = []
    for item, final_name in zip(files, names):
        planned.append(
            _planned_file_from_entry(
                item,
                final_name=final_name,
                target_dir=movie_dir,
            )
        )

    bonus_counts: dict[str, int] = defaultdict(int)
    for item in sorted(bonus_files, key=lambda value: _collision_key(str(value["full_path"]))):
        kind = bonus_type(str(item["name"])) or "other"
        bonus_counts[kind] += 1
        serial = "" if bonus_counts[kind] == 1 else str(bonus_counts[kind])
        final_name = _compose_filename(
            base_name,
            f"-{kind}{serial}",
            Path(str(item["name"])).suffix.lower(),
        )
        planned.append(
            _planned_file_from_entry(item, final_name=final_name, target_dir=movie_dir)
        )

    plan = Plan(
        mode="movie",
        source_root=normalize_remote_path(src_path),
        target_root=movie_dir,
        files=planned,
        warnings=(
            (["已显式忽略 .scraper-tmp-* 遗留条目，可能存在未恢复文件"] if ignore_orphan_temp else [])
            + ([f"已排除 {len(samples)} 个 sample/样片文件，文件仍保留在源目录"] if samples else [])
            + ([f"已按 Infuse 规则整理 {len(bonus_files)} 个预告/花絮文件"] if bonus_files else [])
        ),
        metadata={
            "tmdb_id": tmdb_id,
            "title": title,
            "year": year,
            "poster_path": movie.get("poster_path"),
            "backdrop_path": movie.get("backdrop_path"),
        },
    )
    _add_snapshot_warnings(plan)
    validate_plan(alist, plan)
    return plan


def _reject_json_constant(value: str) -> None:
    raise ValueError(f"JSON 不允许非有限数值: {value}")


def _reject_duplicate_object_pairs(pairs: Sequence[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"JSON 对象包含重复字段: {key!r}")
        result[key] = value
    return result


def _load_json_text(text: str) -> Any:
    return json.loads(
        text,
        parse_constant=_reject_json_constant,
        object_pairs_hook=_reject_duplicate_object_pairs,
    )


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
    fractional = re.fullmatch(r"(?:E)?0*(\d{1,3})\.5", value.strip(), re.IGNORECASE)
    if fractional:
        number = int(fractional.group(1))
        if number <= 0:
            raise PlanError(f"无效源集数覆盖键: {value!r}")
        return EpisodeKey("fractional", number)
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
    collection_dir = join_remote(parent_path, f"{collection_title} {{tmdb-{tmdb_id}}}")
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
    if ignore_orphan_temp:
        warnings.append("已显式忽略 .scraper-tmp-* 遗留条目，可能存在未恢复文件")
    if explicit_map is None:
        warnings.append("使用按上映日期排序的序号映射；执行前必须人工核对 dry-run 输出")
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
        base_name = safe_name(f"{title} ({year}) {{tmdb-{movie_id}}}")
        member_dir = join_remote(collection_dir, base_name)
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
            "mapping": explicit_map,
        },
    )
    _add_snapshot_warnings(plan)
    validate_plan(alist, plan)
    return plan


def validate_plan(alist: AListClient, plan: Plan) -> None:
    if not plan.files:
        raise PlanError("操作计划为空")

    source_root = normalize_remote_path(plan.source_root).rstrip("/") or "/"
    target_root = normalize_remote_path(plan.target_root).rstrip("/") or "/"
    source_root_folded = _collision_key(source_root)
    target_root_folded = _collision_key(target_root)
    if target_root_folded != source_root_folded and target_root_folded.startswith(
        source_root_folded.rstrip("/") + "/"
    ):
        raise PlanError(
            "目标目录位于源目录内部，可能导致重复扫描和递归整理。"
            "请把 --parent 设置为源目录之外的父目录。"
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
        original_name = _validate_remote_basename(item.original_name)
        final_name = _validate_remote_basename(item.final_name)

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
    target_dirs = sorted({normalize_remote_path(item.target_dir) for item in plan.files})
    for target_dir in target_dirs:
        content = alist.try_list(target_dir, refresh=True)
        if content is None:
            continue
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
                raise PlanError(f"目标目录已存在同名{kind}: {current_final_path}")


def validate_source_state(
    alist: AListClient,
    plan: Plan,
    *,
    require_snapshot: bool = True,
) -> None:
    """执行前确认源文件名称与计划快照均未变化。"""
    by_dir: dict[str, list[PlannedFile]] = defaultdict(list)
    for item in plan.files:
        by_dir[normalize_remote_path(item.source_dir)].append(item)

    for source_dir, items in by_dir.items():
        content = alist.try_list(source_dir, refresh=True)
        if content is None:
            raise PlanError(f"执行前无法读取源目录: {source_dir}")
        entries_by_key: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
        for entry in content:
            name = entry.get("name")
            if entry.get("is_dir") or not isinstance(name, str):
                continue
            entries_by_key[_collision_key(name)].append(entry)

        for item in items:
            matches = entries_by_key.get(_collision_key(item.original_name), [])
            if not matches:
                raise PlanError(
                    f"源目录内容在计划生成后发生变化，缺少文件: "
                    f"{source_dir}/{item.original_name}"
                )
            if len(matches) != 1:
                raise PlanError(
                    f"源目录存在 Unicode/大小写等价的多个文件，无法安全确认来源: "
                    f"{source_dir}/{item.original_name}"
                )
            actual = matches[0]
            actual_name = str(actual.get("name"))
            if actual_name != item.original_name:
                raise PlanError(
                    f"源文件名称在计划生成后发生变化: "
                    f"{source_dir}/{item.original_name} → {actual_name}"
                )

            snapshot_present = any(
                value is not None
                for value in (item.source_size, item.source_modified, item.source_hash)
            )
            if require_snapshot and not snapshot_present:
                raise PlanError(
                    f"计划缺少源文件快照，拒绝执行: {item.source_path}。"
                    "请使用当前版本重新生成计划。"
                )

            actual_size = _entry_size_value(actual)
            actual_modified = _entry_modified_value(actual)
            actual_hash = _entry_hash_value(actual)
            comparisons = (
                ("size", item.source_size, actual_size),
                ("modified", item.source_modified, actual_modified),
                ("hash", item.source_hash, actual_hash),
            )
            changed = [
                f"{field}: {expected!r} → {observed!r}"
                for field, expected, observed in comparisons
                if expected is not None and expected != observed
            ]
            if changed:
                raise PlanError(
                    f"源文件在计划生成后发生变化，拒绝执行: {item.source_path}; "
                    + "; ".join(changed)
                )


# ---------------------------------------------------------------------------
# 计划执行与回滚
# ---------------------------------------------------------------------------


def _ensure_target_dirs(
    alist: AListClient,
    plan: Plan,
    journal: ExecutionJournal,
    journal_path: Path,
    created: list[str],
) -> None:
    directories = sorted(
        {plan.target_root, *(item.target_dir for item in plan.files)},
        key=lambda path: path.count("/"),
    )
    for directory in directories:
        existed = alist.try_list(directory, refresh=True) is not None
        record = _append_pending(
            journal,
            journal_path,
            "mkdir",
            "",
            directory,
            "existing" if existed else "create",
        )
        try:
            alist.mkdir(directory)
        except (Exception, KeyboardInterrupt):
            # 若响应在服务端成功后丢失，目录实况可帮助后续回滚。
            if not existed:
                try:
                    if alist.try_list(directory, refresh=True) is not None:
                        created.append(directory)
                        _mark_record(
                            journal, journal_path, record, "uncertain", "directory now exists"
                        )
                except Exception:
                    pass
            raise
        if not existed:
            created.append(directory)
        _mark_record(
            journal, journal_path, record, "ok", "existing" if existed else "created"
        )


def _temporary_name(original_name: str) -> str:
    ext = Path(original_name).suffix
    digest = hashlib.sha256(original_name.encode("utf-8", errors="surrogatepass")).hexdigest()[:16]
    return f".scraper-tmp-{digest}-{uuid.uuid4().hex[:12]}{ext}"


def _recovery_name(item: PlannedFile) -> str:
    ext = Path(item.original_name).suffix
    digest = hashlib.sha256(item.source_path.encode("utf-8")).hexdigest()[:16]
    return f".scraper-recover-{digest}-{uuid.uuid4().hex[:12]}{ext}"


def _recovery_prefix(item: PlannedFile) -> str:
    digest = hashlib.sha256(item.source_path.encode("utf-8")).hexdigest()[:16]
    return f".scraper-recover-{digest}-"


def _append_pending(
    journal: ExecutionJournal,
    journal_path: Path,
    action: str,
    source: str,
    target: str,
    message: str = "",
) -> ExecutionRecord:
    record = ExecutionRecord(action, source, target, "pending", message)
    journal.records.append(record)
    journal.save(journal_path)
    return record


def _acquire_remote_lock(
    alist: AListClient,
    plan: Plan,
    journal: ExecutionJournal,
    journal_path: Path,
) -> str:
    source_root = normalize_remote_path(plan.source_root)
    entries = alist.try_list(source_root, refresh=True)
    if entries is None:
        raise PlanError(f"无法读取源根目录以获取整理锁: {source_root}")
    existing = [
        str(entry["name"])
        for entry in entries
        if isinstance(entry.get("name"), str) and is_scraper_lock(str(entry["name"]))
    ]
    if existing:
        raise PlanError(
            f"源目录已有整理锁，可能存在并发任务或未恢复任务: "
            + ", ".join(join_remote(source_root, name) for name in existing)
        )

    lock_name = (
        f"{LOCK_PREFIX}{plan_sha256(plan)[:16]}-{uuid.uuid4().hex[:12]}.json"
    )
    lock_path = join_remote(source_root, lock_name)
    payload = _canonical_json_bytes(
        {
            "version": __version__,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "plan_sha256": plan_sha256(plan),
            "journal": str(journal_path),
        }
    )
    record = _append_pending(
        journal, journal_path, "acquire-lock", "", lock_path
    )
    try:
        alist.upload_bytes(lock_path, payload, "application/vnd.scraper-lock+json")
    except Exception:
        names = _directory_file_names(alist, source_root)
        if not _collision_presence(names, lock_name):
            raise
    names = _directory_file_names(alist, source_root)
    matches = _collision_presence(names, lock_name)
    all_locks = sorted(name for name in names if is_scraper_lock(name))
    if len(matches) != 1 or len(all_locks) != 1:
        if len(matches) == 1 and matches[0] == lock_name:
            try:
                alist.remove(source_root, [lock_name])
            except Exception:
                pass
        raise ApiError(
            f"无法确认整理锁为目录中的唯一锁: {lock_path}; "
            f"own_matches={matches}, all_locks={all_locks}"
        )
    _mark_record(journal, journal_path, record, "ok")
    return lock_path


def _release_remote_lock(
    alist: AListClient,
    lock_path: str,
    journal: ExecutionJournal,
    journal_path: Path,
) -> None:
    parent, name = split_remote(lock_path)
    record = _append_pending(
        journal, journal_path, "release-lock", lock_path, ""
    )
    names = _directory_file_names(alist, parent)
    matches = _collision_presence(names, name)
    if not matches:
        _mark_record(journal, journal_path, record, "ok", "already absent")
        return
    if len(matches) != 1 or matches[0] != name:
        raise ApiError(f"整理锁名称状态不明确，拒绝删除: {lock_path}; matches={matches}")
    alist.remove(parent, [name])
    remaining = _collision_presence(_directory_file_names(alist, parent), name)
    if remaining:
        raise ApiError(f"整理锁删除后仍然存在: {lock_path}")
    _mark_record(journal, journal_path, record, "ok")


def _mark_record(
    journal: ExecutionJournal,
    journal_path: Path,
    record: ExecutionRecord,
    status: str,
    message: str | None = None,
) -> None:
    record.status = status
    if message is not None:
        record.message = message
    journal.save(journal_path)


def _directory_file_names(alist: AListClient, directory: str) -> set[str]:
    content = alist.try_list(directory, refresh=True)
    if content is None:
        return set()
    return {
        str(entry["name"])
        for entry in content
        if not entry.get("is_dir") and isinstance(entry.get("name"), str)
    }


def _directory_file_entries(
    alist: AListClient, directory: str
) -> list[Mapping[str, Any]]:
    content = alist.try_list(directory, refresh=True)
    if content is None:
        return []
    return [
        entry
        for entry in content
        if not entry.get("is_dir") and isinstance(entry.get("name"), str)
    ]


def _collision_presence(names: Iterable[str], expected: str) -> list[str]:
    key = _collision_key(expected)
    return [name for name in names if _collision_key(name) == key]


def _rename_with_reconciliation(
    alist: AListClient,
    source_path: str,
    new_name: str,
) -> None:
    source_dir, old_name = split_remote(source_path)
    try:
        alist.rename(source_path, new_name)
        return
    except Exception as original_exc:
        for attempt in range(4):
            names = _directory_file_names(alist, source_dir)
            old_matches = _collision_presence(names, old_name)
            new_matches = _collision_presence(names, new_name)
            if _collision_key(old_name) == _collision_key(new_name):
                if len(new_matches) == 1 and new_matches[0] == new_name:
                    return
            elif len(new_matches) == 1 and not old_matches:
                return
            elif len(old_matches) == 1 and not new_matches:
                if attempt == 3:
                    raise original_exc
            else:
                if attempt == 3:
                    raise ApiError(
                        f"重命名结果不确定: {source_path} → {new_name}; "
                        f"old_matches={old_matches}, new_matches={new_matches}"
                    ) from original_exc
            time.sleep(0.4 * (attempt + 1))
        raise original_exc


def _move_with_reconciliation(
    alist: AListClient,
    src_dir: str,
    dst_dir: str,
    names: Sequence[str],
) -> list[str]:
    try:
        alist.move(src_dir, dst_dir, names)
        return list(names)
    except Exception as exc:
        moved: list[str] = []
        remaining: list[str] = []
        ambiguous: list[str] = []
        for attempt in range(4):
            src_names = _directory_file_names(alist, src_dir)
            dst_names = _directory_file_names(alist, dst_dir)
            moved = []
            remaining = []
            ambiguous = []
            for name in names:
                src_matches = _collision_presence(src_names, name)
                dst_matches = _collision_presence(dst_names, name)
                if len(dst_matches) == 1 and not src_matches:
                    moved.append(name)
                elif len(src_matches) == 1 and not dst_matches:
                    remaining.append(name)
                else:
                    ambiguous.append(name)
            if len(moved) == len(names):
                return moved
            if not moved and len(remaining) == len(names) and attempt == 3:
                raise exc
            if ambiguous and attempt < 3:
                time.sleep(0.4 * (attempt + 1))
                continue
            break
        raise PartialMoveError(
            f"移动只完成了一部分: {src_dir} → {dst_dir}; "
            f"moved={moved}, remaining={remaining}, ambiguous={ambiguous}; "
            f"original={_redact_sensitive_text(str(exc))}",
            moved,
        ) from exc


def _rollback_renames(
    alist: AListClient,
    states: list[tuple[PlannedFile, str, str]],
    journal: ExecutionJournal,
) -> None:
    # 回滚也使用两阶段重命名，避免 a→b、b→a 之类的交换冲突。
    temporary_states: list[tuple[PlannedFile, str, str]] = []
    for item, current_name, original_name in reversed(states):
        if current_name == original_name:
            continue
        current_path = join_remote(item.source_dir, current_name)
        rollback_temp = _temporary_name(original_name)
        try:
            _rename_with_reconciliation(alist, current_path, rollback_temp)
            temporary_states.append((item, rollback_temp, original_name))
            journal.records.append(
                ExecutionRecord(
                    "rollback-rename-temp",
                    current_path,
                    join_remote(item.source_dir, rollback_temp),
                    "ok",
                )
            )
        except Exception as exc:
            journal.records.append(
                ExecutionRecord(
                    "rollback-rename-temp",
                    current_path,
                    join_remote(item.source_dir, rollback_temp),
                    "failed",
                    str(exc),
                )
            )

    for item, rollback_temp, original_name in reversed(temporary_states):
        current_path = join_remote(item.source_dir, rollback_temp)
        try:
            _rename_with_reconciliation(alist, current_path, original_name)
            journal.records.append(
                ExecutionRecord(
                    "rollback-rename-final",
                    current_path,
                    join_remote(item.source_dir, original_name),
                    "ok",
                )
            )
        except Exception as exc:
            journal.records.append(
                ExecutionRecord(
                    "rollback-rename-final",
                    current_path,
                    join_remote(item.source_dir, original_name),
                    "failed",
                    str(exc),
                )
            )


def _reconcile_rename_states(
    alist: AListClient,
    states: list[tuple[PlannedFile, str, str]],
    journal: ExecutionJournal,
) -> None:
    """在中断或网络结果不确定时，根据目录实况更新当前文件名。"""
    by_dir: dict[str, set[str]] = {}
    for item, _, _ in states:
        if item.source_dir not in by_dir:
            by_dir[item.source_dir] = _directory_file_names(alist, item.source_dir)

    for index, (item, current_name, original_name) in enumerate(list(states)):
        names = by_dir[item.source_dir]
        candidates: list[str] = []
        for name in (item.final_name, current_name, original_name):
            if _collision_key(name) not in {_collision_key(value) for value in candidates}:
                candidates.append(name)
        present: list[str] = []
        for candidate in candidates:
            matches = _collision_presence(names, candidate)
            if len(matches) == 1:
                present.append(matches[0])
            elif len(matches) > 1:
                present.extend(matches)
        if len(set(present)) == 1:
            states[index] = (item, present[0], original_name)
            continue
        journal.records.append(
            ExecutionRecord(
                "reconcile-rename",
                item.source_path,
                item.final_name,
                "failed",
                f"无法唯一判断当前名称: {present}",
            )
        )


def _verify_final_state(alist: AListClient, plan: Plan) -> None:
    last_error: str | None = None
    for attempt in range(5):
        try:
            target_entries: dict[str, list[Mapping[str, Any]]] = {}
            for item in plan.files:
                if item.target_dir not in target_entries:
                    target_entries[item.target_dir] = _directory_file_entries(
                        alist, item.target_dir
                    )
                matches = [
                    entry
                    for entry in target_entries[item.target_dir]
                    if _collision_key(str(entry["name"]))
                    == _collision_key(item.final_name)
                ]
                if len(matches) != 1:
                    raise ApiError(
                        f"执行后校验失败，目标文件数量异常: "
                        f"{item.target_dir}/{item.final_name}; "
                        f"matches={[entry.get('name') for entry in matches]}"
                    )

                actual = matches[0]
                actual_size = _entry_size_value(actual)
                actual_hash = _entry_hash_value(actual)
                identity_changes: list[str] = []
                if item.source_size is not None:
                    if actual_size is None:
                        identity_changes.append("目标端未返回 size")
                    elif actual_size != item.source_size:
                        identity_changes.append(
                            f"size: {item.source_size!r} → {actual_size!r}"
                        )
                if item.source_hash is not None:
                    if actual_hash is None:
                        identity_changes.append("目标端未返回 hash")
                    elif actual_hash != item.source_hash:
                        identity_changes.append(
                            f"hash: {item.source_hash!r} → {actual_hash!r}"
                        )
                if identity_changes:
                    raise ApiError(
                        f"执行后校验失败，目标文件身份与计划快照不一致: "
                        f"{item.target_dir}/{item.final_name}; "
                        + "; ".join(identity_changes)
                    )

            source_names: dict[str, set[str]] = {}
            for item in plan.files:
                if _collision_key(item.source_dir) == _collision_key(item.target_dir):
                    continue
                if item.source_dir not in source_names:
                    source_names[item.source_dir] = _directory_file_names(alist, item.source_dir)
                if _collision_presence(source_names[item.source_dir], item.final_name):
                    raise ApiError(
                        f"执行后校验失败，文件仍留在源目录: "
                        f"{item.source_dir}/{item.final_name}"
                    )
            return
        except ApiError as exc:
            last_error = str(exc)
            if attempt == 4:
                raise
            time.sleep(0.5 * (attempt + 1))
    raise ApiError(last_error or "执行后校验失败")


def execute_plan(
    alist: AListClient,
    tmdb_client: TMDBClient | None,
    plan: Plan,
    *,
    journal_path: Path,
    skip_poster: bool,
    overwrite_poster: bool = False,
    cleanup_empty_source: bool = False,
) -> None:
    validate_plan(alist, plan)
    validate_source_state(alist, plan, require_snapshot=True)
    _reserve_output_path(journal_path)
    journal = ExecutionJournal(
        created_at=datetime.now(timezone.utc).isoformat(),
        plan=plan_to_dict(plan),
        records=[],
    )
    try:
        journal.save(journal_path)
    except Exception:
        try:
            journal_path.unlink()
        except FileNotFoundError:
            pass
        raise

    renamed_states: list[tuple[PlannedFile, str, str]] = []
    moved_batches: list[tuple[str, str, list[str]]] = []
    created_dirs: list[str] = []
    files_committed = False
    poster_preexisted = False
    pending_poster_target: str | None = None
    pending_move: tuple[str, str, list[str]] | None = None
    remote_lock_path: str | None = None

    try:
        remote_lock_path = _acquire_remote_lock(alist, plan, journal, journal_path)
        _ensure_target_dirs(alist, plan, journal, journal_path, created_dirs)

        # 第一阶段：所有需重命名文件先变为唯一临时名，避免同目录交换或覆盖。
        for item in plan.files:
            if not item.requires_rename:
                renamed_states.append((item, item.original_name, item.original_name))
                continue
            temp_name = _temporary_name(item.original_name)
            renamed_states.append((item, temp_name, item.original_name))
            record = _append_pending(
                journal,
                journal_path,
                "rename-temp",
                item.source_path,
                join_remote(item.source_dir, temp_name),
            )
            _rename_with_reconciliation(alist, item.source_path, temp_name)
            _mark_record(journal, journal_path, record, "ok")

        # 第二阶段：临时名改为最终名。
        for index, (item, current_name, original_name) in enumerate(list(renamed_states)):
            if not item.requires_rename:
                continue
            current_path = join_remote(item.source_dir, current_name)
            record = _append_pending(
                journal,
                journal_path,
                "rename-final",
                current_path,
                join_remote(item.source_dir, item.final_name),
            )
            _rename_with_reconciliation(alist, current_path, item.final_name)
            renamed_states[index] = (item, item.final_name, original_name)
            _mark_record(journal, journal_path, record, "ok")

        # 分源目录和目标目录移动，单批最多 50 个。
        move_groups: dict[tuple[str, str], list[str]] = defaultdict(list)
        for item in plan.files:
            if item.source_dir == item.target_dir:
                continue
            move_groups[(item.source_dir, item.target_dir)].append(item.final_name)

        for (src_dir, dst_dir), names in sorted(move_groups.items()):
            for offset in range(0, len(names), 50):
                batch = names[offset : offset + 50]
                pending_move = (src_dir, dst_dir, list(batch))
                record = _append_pending(
                    journal, journal_path, "move", src_dir, dst_dir, ", ".join(batch)
                )
                try:
                    moved = _move_with_reconciliation(alist, src_dir, dst_dir, batch)
                except PartialMoveError as move_exc:
                    if move_exc.moved_names:
                        moved_batches.append((src_dir, dst_dir, list(move_exc.moved_names)))
                    pending_move = None
                    _mark_record(journal, journal_path, record, "failed", str(move_exc))
                    raise
                moved_batches.append((src_dir, dst_dir, list(moved)))
                pending_move = None
                _mark_record(journal, journal_path, record, "ok", ", ".join(moved))

        _verify_final_state(alist, plan)
        # 文件事务到此已经完成并通过远端实况校验。之后的海报属于附加步骤，
        # 即使失败也不再回滚已验证的媒体文件，避免为非关键元数据增加破坏面。
        files_committed = True
        journal.records.append(ExecutionRecord("files-committed", "", plan.target_root, "ok"))
        journal.save(journal_path)

        artwork_requests = planned_artwork(plan) if not skip_poster else []
        artwork_cache: dict[str, bytes] = {}
        for requested_target, image_path, artwork_role in artwork_requests:
            poster_target, poster_preexisted = resolve_artwork_target(
                alist, requested_target
            )
            poster_dir, poster_name = split_remote(poster_target)
            if poster_preexisted and not overwrite_poster:
                journal.records.append(
                    ExecutionRecord(
                        "upload-poster",
                        image_path,
                        poster_target,
                        "skipped",
                        f"existing {artwork_role}; use --overwrite-poster to replace",
                    )
                )
                continue
            if tmdb_client is None:
                raise ScraperError("计划包含图稿，但未提供 TMDB API Key；可使用 --skip-poster")
            if image_path not in artwork_cache:
                artwork_cache[image_path] = tmdb_client.download_poster(image_path)
            poster_data = artwork_cache[image_path]
            pending_poster_target = poster_target
            record = _append_pending(
                journal, journal_path, "upload-poster", image_path, poster_target, artwork_role
            )
            try:
                alist.upload_bytes(poster_target, poster_data, "image/jpeg")
            except Exception:
                current_names = _directory_file_names(alist, poster_dir)
                if not poster_preexisted and any(
                    _collision_key(name) == _collision_key(poster_name)
                    for name in current_names
                ):
                    pass
                else:
                    raise
            pending_poster_target = None
            _mark_record(
                journal,
                journal_path,
                record,
                "ok",
                "overwrote existing artwork" if poster_preexisted else "created",
            )

        for nfo_target, nfo_data in planned_movie_nfos(plan):
            actual_target, preexisting_nfo = resolve_artwork_target(alist, nfo_target)
            if preexisting_nfo:
                journal.records.append(
                    ExecutionRecord(
                        "upload-nfo",
                        "generated",
                        actual_target,
                        "skipped",
                        "existing NFO preserved",
                    )
                )
                continue
            nfo_dir, nfo_name = split_remote(actual_target)
            poster_preexisted = False
            pending_poster_target = actual_target
            record = _append_pending(
                journal, journal_path, "upload-nfo", "generated", actual_target
            )
            try:
                alist.upload_bytes(actual_target, nfo_data, "application/xml")
            except Exception:
                if not _collision_presence(_directory_file_names(alist, nfo_dir), nfo_name):
                    raise
            pending_poster_target = None
            _mark_record(journal, journal_path, record, "ok", "created")

        if remote_lock_path is not None:
            _release_remote_lock(alist, remote_lock_path, journal, journal_path)
            remote_lock_path = None
        if cleanup_empty_source:
            source_dirs = sorted(
                {
                    normalize_remote_path(plan.source_root),
                    *(normalize_remote_path(item.source_dir) for item in plan.files),
                },
                key=lambda value: value.count("/"),
                reverse=True,
            )
            for source_dir in source_dirs:
                if source_dir == "/" or _path_is_within(plan.target_root, source_dir):
                    continue
                record = _append_pending(
                    journal, journal_path, "cleanup-empty-source", source_dir, ""
                )
                removed = alist.remove_empty_dir(source_dir)
                _mark_record(
                    journal,
                    journal_path,
                    record,
                    "ok" if removed else "skipped",
                    "removed" if removed else "directory not empty",
                )
        journal.success = True
        journal.save(journal_path)
    except (Exception, KeyboardInterrupt) as exc:
        error_text = str(exc) or type(exc).__name__
        journal.records.append(ExecutionRecord("abort", "", "", "failed", error_text))

        if files_committed:
            # 文件已经通过最终校验。此时只核对可能处于不确定状态的海报，
            # 不再执行文件回滚。
            if pending_poster_target is not None:
                try:
                    poster_name = split_remote(pending_poster_target)[1]
                    poster_dir, _ = split_remote(pending_poster_target)
                    present = any(
                        _collision_key(name) == _collision_key(poster_name)
                        for name in _directory_file_names(alist, poster_dir)
                    )
                    journal.records.append(
                        ExecutionRecord(
                            "reconcile-poster",
                            "TMDB",
                            pending_poster_target,
                            "uncertain" if poster_preexisted else ("ok" if present else "failed"),
                            (
                                "overwrite result cannot be content-verified"
                                if poster_preexisted
                                else f"poster_present={present}"
                            ),
                        )
                    )
                except Exception as reconcile_exc:
                    journal.records.append(
                        ExecutionRecord(
                            "reconcile-poster",
                            "TMDB",
                            pending_poster_target,
                            "failed",
                            str(reconcile_exc),
                        )
                    )
            if remote_lock_path is not None:
                try:
                    _release_remote_lock(
                        alist, remote_lock_path, journal, journal_path
                    )
                    remote_lock_path = None
                except Exception as lock_exc:
                    journal.records.append(
                        ExecutionRecord(
                            "release-lock",
                            remote_lock_path,
                            "",
                            "failed",
                            str(lock_exc),
                        )
                    )
            journal_note = f"详见 {journal_path}"
            try:
                journal.save(journal_path)
            except OSError as journal_exc:
                journal_note = f"执行日志写入失败: {journal_exc}"
            if isinstance(exc, KeyboardInterrupt):
                raise
            raise ScraperError(
                f"媒体文件已完成整理并通过校验，但图稿、NFO 或提交后记录步骤失败；"
                f"未回滚媒体文件；{journal_note}: {error_text}"
            ) from exc

        if pending_move is not None:
            src_dir, dst_dir, names = pending_move
            try:
                src_names = _directory_file_names(alist, src_dir)
                dst_names = _directory_file_names(alist, dst_dir)
                moved = [
                    name
                    for name in names
                    if len(_collision_presence(dst_names, name)) == 1
                    and not _collision_presence(src_names, name)
                ]
                if moved:
                    moved_batches.append((src_dir, dst_dir, moved))
                journal.records.append(
                    ExecutionRecord(
                        "reconcile-move",
                        src_dir,
                        dst_dir,
                        "ok",
                        f"observed moved={moved}",
                    )
                )
            except Exception as reconcile_exc:
                journal.records.append(
                    ExecutionRecord(
                        "reconcile-move", src_dir, dst_dir, "failed", str(reconcile_exc)
                    )
                )

        # 先回滚已移动批次，再回滚文件名。失败记录保留在 journal 中。
        for src_dir, dst_dir, names in reversed(moved_batches):
            try:
                _move_with_reconciliation(alist, dst_dir, src_dir, names)
                journal.records.append(
                    ExecutionRecord("rollback-move", dst_dir, src_dir, "ok", ", ".join(names))
                )
            except Exception as rollback_exc:
                journal.records.append(
                    ExecutionRecord(
                        "rollback-move", dst_dir, src_dir, "failed", f"{names}: {rollback_exc}"
                    )
                )

        try:
            _reconcile_rename_states(alist, renamed_states, journal)
        except Exception as reconcile_exc:
            journal.records.append(
                ExecutionRecord("reconcile-rename", "", "", "failed", str(reconcile_exc))
            )
        _rollback_renames(alist, renamed_states, journal)

        # 不自动删除本次流程观察为“新建”的目录。预检与 mkdir 之间存在并发窗口，
        # 无法证明一个仍为空的目录一定由本进程独占创建。保留空目录比误删其他
        # 进程创建的合法目录更安全；需要清理时使用独立的 remove_empty_dirs.py。
        for directory in sorted(created_dirs, key=lambda path: path.count("/"), reverse=True):
            journal.records.append(
                ExecutionRecord(
                    "rollback-rmdir",
                    directory,
                    "",
                    "retained",
                    "not removed automatically because directory ownership cannot be proven",
                )
            )

        if remote_lock_path is not None:
            try:
                _release_remote_lock(alist, remote_lock_path, journal, journal_path)
                remote_lock_path = None
            except Exception as lock_exc:
                journal.records.append(
                    ExecutionRecord(
                        "release-lock",
                        remote_lock_path,
                        "",
                        "failed",
                        str(lock_exc),
                    )
                )

        journal_note = f"详见 {journal_path}"
        try:
            journal.save(journal_path)
        except OSError as journal_exc:
            journal_note = f"执行日志写入失败: {journal_exc}"
        if isinstance(exc, KeyboardInterrupt):
            raise
        raise ScraperError(f"执行失败，已尝试回滚；{journal_note}: {error_text}") from exc


def plan_to_dict(plan: Plan) -> dict[str, Any]:
    return {
        "mode": plan.mode,
        "source_root": plan.source_root,
        "target_root": plan.target_root,
        "warnings": list(plan.warnings),
        "metadata": dict(plan.metadata),
        "files": [asdict(item) for item in plan.files],
    }


def _canonical_json_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def plan_sha256(plan: Plan | Mapping[str, Any]) -> str:
    payload = plan_to_dict(plan) if isinstance(plan, Plan) else dict(plan)
    return hashlib.sha256(_canonical_json_bytes(payload)).hexdigest()


def _reserve_output_path(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        descriptor = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    except FileExistsError as exc:
        raise ScraperError(f"本地输出文件已存在，拒绝覆盖: {path}") from exc
    else:
        os.close(descriptor)


def _write_json_reserved(path: Path, payload: Mapping[str, Any]) -> None:
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    descriptor = os.open(temporary, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False, indent=2, allow_nan=False)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass


def _require_string(value: Any, field: str, *, allow_empty: bool = False) -> str:
    if not isinstance(value, str) or (not allow_empty and not value):
        raise PlanError(f"计划字段 {field} 必须是字符串")
    return value


def _optional_string(value: Any, field: str) -> str | None:
    if value is None:
        return None
    return _require_string(value, field, allow_empty=True)


def _require_remote_basename(value: Any, field: str) -> str:
    raw = _require_string(value, field)
    try:
        return _validate_remote_basename(raw)
    except ValueError as exc:
        raise PlanError(f"计划字段 {field} 不是安全文件名: {exc}") from exc


def _require_normalized_remote_path(value: Any, field: str) -> str:
    raw = _require_string(value, field)
    try:
        normalized = normalize_remote_path(raw)
    except ValueError as exc:
        raise PlanError(f"计划字段 {field} 不是安全远端路径: {exc}") from exc
    if normalized != raw:
        raise PlanError(f"计划字段 {field} 必须使用规范化绝对路径: {raw!r}")
    return normalized


def _optional_nonnegative_int(value: Any, field: str) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise PlanError(f"计划字段 {field} 必须是非负整数或 null")
    return value


def _reject_unknown_fields(raw: Mapping[str, Any], allowed: set[str], field: str) -> None:
    unknown = sorted(set(raw) - allowed)
    if unknown:
        raise PlanError(f"计划字段 {field} 包含未知成员: {', '.join(unknown)}")


def _validate_plan_metadata(mode: str, raw: Mapping[str, Any]) -> dict[str, Any]:
    common = {"tmdb_id", "title", "poster_path", "backdrop_path"}
    allowed_by_mode = {
        "tv": common | {"year", "season", "absolute", "season_posters", "episode_group"},
        "movie": common | {"year"},
        "collection": common | {"mapping", "member_posters"},
    }
    _reject_unknown_fields(raw, allowed_by_mode[mode], "metadata")

    tmdb_id = raw.get("tmdb_id")
    if isinstance(tmdb_id, bool) or not isinstance(tmdb_id, int) or tmdb_id <= 0:
        raise PlanError("计划字段 metadata.tmdb_id 必须是正整数")
    title = _require_string(raw.get("title"), "metadata.title")
    if _has_unsafe_unicode(title):
        raise PlanError("计划字段 metadata.title 包含控制或不可见格式字符")

    def validate_image_path(value: Any, field: str) -> str | None:
        if value is None:
            return None
        image_path = _require_string(value, field)
        if (
            _has_unsafe_unicode(image_path)
            or not image_path.startswith("/")
            or ".." in image_path.split("/")
            or not re.fullmatch(r"/[A-Za-z0-9._/-]+", image_path)
        ):
            raise PlanError(f"计划字段 {field} 不是安全的 TMDB 图片路径")
        return image_path

    validate_image_path(raw.get("poster_path"), "metadata.poster_path")
    validate_image_path(raw.get("backdrop_path"), "metadata.backdrop_path")

    result = dict(raw)
    if mode in {"tv", "movie"}:
        year = _require_string(raw.get("year"), "metadata.year")
        if year != "未知年份" and not re.fullmatch(r"(?:19|20)\d{2}", year):
            raise PlanError("计划字段 metadata.year 必须是四位年份或 未知年份")
    if mode == "tv":
        season = raw.get("season")
        if isinstance(season, bool) or not isinstance(season, int) or season < 0:
            raise PlanError("计划字段 metadata.season 必须是非负整数")
        if type(raw.get("absolute")) is not bool:
            raise PlanError("计划字段 metadata.absolute 必须是布尔值")
        episode_group = raw.get("episode_group")
        if episode_group is not None:
            episode_group = _require_string(episode_group, "metadata.episode_group")
            if not re.fullmatch(r"[A-Za-z0-9_-]{1,100}", episode_group):
                raise PlanError("计划字段 metadata.episode_group 格式无效")
        if "season_posters" in raw:
            season_posters = raw.get("season_posters")
            if not isinstance(season_posters, Mapping):
                raise PlanError("计划字段 metadata.season_posters 必须是对象")
            normalized_seasons: dict[str, str] = {}
            for key, image_path in season_posters.items():
                if not isinstance(key, str) or not re.fullmatch(r"\d{1,3}", key):
                    raise PlanError("metadata.season_posters 的季度键必须是非负整数字符串")
                normalized_seasons[str(int(key))] = validate_image_path(
                    image_path, f"metadata.season_posters[{key!r}]"
                ) or ""
            result["season_posters"] = normalized_seasons
    if mode == "collection":
        mapping = raw.get("mapping")
        if mapping is not None:
            if not isinstance(mapping, Mapping):
                raise PlanError("计划字段 metadata.mapping 必须是对象或 null")
            normalized_mapping: dict[str, int] = {}
            for key, value in mapping.items():
                if not isinstance(key, (str, int)) or not str(key).isdigit():
                    raise PlanError("计划字段 metadata.mapping 的编号必须是正整数")
                number = int(str(key))
                if number <= 0 or isinstance(value, bool) or not isinstance(value, int) or value <= 0:
                    raise PlanError("计划字段 metadata.mapping 的编号和 TMDB ID 必须是正整数")
                normalized_mapping[str(number)] = value
            result["mapping"] = normalized_mapping
        if "member_posters" in raw:
            member_posters = raw.get("member_posters")
            if not isinstance(member_posters, Mapping):
                raise PlanError("计划字段 metadata.member_posters 必须是对象")
            normalized_posters: dict[str, str] = {}
            for target_dir, image_path in member_posters.items():
                normalized_dir = _require_normalized_remote_path(
                    target_dir, "metadata.member_posters target"
                )
                normalized_posters[normalized_dir] = validate_image_path(
                    image_path, f"metadata.member_posters[{target_dir!r}]"
                ) or ""
            result["member_posters"] = normalized_posters
    return result


def plan_from_dict(raw: Mapping[str, Any]) -> Plan:
    if not isinstance(raw, Mapping):
        raise PlanError("计划内容必须是 JSON 对象")
    _reject_unknown_fields(
        raw,
        {"mode", "source_root", "target_root", "warnings", "metadata", "files"},
        "plan",
    )
    mode = _require_string(raw.get("mode"), "mode")
    if mode not in {"tv", "movie", "collection"}:
        raise PlanError(f"计划字段 mode 无效: {mode!r}")
    source_root = _require_normalized_remote_path(raw.get("source_root"), "source_root")
    target_root = _require_normalized_remote_path(raw.get("target_root"), "target_root")
    warnings_raw = raw.get("warnings", [])
    metadata_raw = raw.get("metadata", {})
    files_raw = raw.get("files")
    if not isinstance(warnings_raw, list) or not all(isinstance(item, str) for item in warnings_raw):
        raise PlanError("计划字段 warnings 必须是字符串数组")
    if any(_has_unsafe_unicode(item) for item in warnings_raw):
        raise PlanError("计划字段 warnings 包含控制或不可见格式字符")
    if not isinstance(metadata_raw, dict):
        raise PlanError("计划字段 metadata 必须是对象")
    metadata = _validate_plan_metadata(mode, metadata_raw)
    if not isinstance(files_raw, list):
        raise PlanError("计划字段 files 必须是数组")

    files: list[PlannedFile] = []
    allowed_file_fields = {
        "source_path", "source_dir", "original_name", "final_name", "target_dir",
        "media_kind", "episode_key", "source_size", "source_modified", "source_hash",
    }
    for index, item in enumerate(files_raw):
        if not isinstance(item, Mapping):
            raise PlanError(f"计划文件项 {index} 必须是对象")
        _reject_unknown_fields(item, allowed_file_fields, f"files[{index}]")
        media = _require_string(item.get("media_kind"), f"files[{index}].media_kind")
        if media not in {"video", "subtitle"}:
            raise PlanError(f"计划文件项 {index} media_kind 无效: {media!r}")
        files.append(
            PlannedFile(
                source_path=_require_normalized_remote_path(
                    item.get("source_path"), f"files[{index}].source_path"
                ),
                source_dir=_require_normalized_remote_path(
                    item.get("source_dir"), f"files[{index}].source_dir"
                ),
                original_name=_require_remote_basename(
                    item.get("original_name"), f"files[{index}].original_name"
                ),
                final_name=_require_remote_basename(
                    item.get("final_name"), f"files[{index}].final_name"
                ),
                target_dir=_require_normalized_remote_path(
                    item.get("target_dir"), f"files[{index}].target_dir"
                ),
                media_kind=media,
                episode_key=_optional_string(item.get("episode_key"), f"files[{index}].episode_key"),
                source_size=_optional_nonnegative_int(item.get("source_size"), f"files[{index}].source_size"),
                source_modified=_optional_string(item.get("source_modified"), f"files[{index}].source_modified"),
                source_hash=_optional_string(item.get("source_hash"), f"files[{index}].source_hash"),
            )
        )
    return Plan(
        mode=mode,
        source_root=source_root,
        target_root=target_root,
        files=files,
        warnings=list(warnings_raw),
        metadata=metadata,
    )


def write_plan_json(plan: Plan, path: Path) -> str:
    validated_plan = plan_from_dict(plan_to_dict(plan))
    missing_snapshots = [
        item.source_path
        for item in validated_plan.files
        if all(
            value is None
            for value in (item.source_size, item.source_modified, item.source_hash)
        )
    ]
    if missing_snapshots:
        preview = ", ".join(missing_snapshots[:3])
        suffix = " ..." if len(missing_snapshots) > 3 else ""
        raise PlanError(
            "计划缺少源文件快照，不能保存为可执行计划: " + preview + suffix
        )
    digest = plan_sha256(validated_plan)
    payload = {
        "schema_version": PLAN_SCHEMA_VERSION,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "plan_sha256": digest,
        "plan": plan_to_dict(validated_plan),
    }
    _reserve_output_path(path)
    try:
        _write_json_reserved(path, payload)
    except Exception:
        try:
            path.unlink()
        except FileNotFoundError:
            pass
        raise
    return digest


def load_plan_json(path: Path) -> tuple[Plan, str]:
    try:
        payload = _load_json_text(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise PlanError(f"无法读取计划文件: {path}; {exc}") from exc
    if not isinstance(payload, dict):
        raise PlanError("计划文件顶层必须是对象")
    _reject_unknown_fields(
        payload, {"schema_version", "created_at", "plan_sha256", "plan"}, "root"
    )
    _require_string(payload.get("created_at"), "created_at")
    if payload.get("schema_version") not in SUPPORTED_PLAN_SCHEMA_VERSIONS:
        raise PlanError(
            f"计划 schema_version 不受支持: {payload.get('schema_version')!r}; "
            f"当前支持 {sorted(SUPPORTED_PLAN_SCHEMA_VERSIONS)}"
        )
    raw_plan = payload.get("plan")
    expected_digest = payload.get("plan_sha256")
    if not isinstance(raw_plan, dict) or not isinstance(expected_digest, str):
        raise PlanError("计划文件缺少 plan 或 plan_sha256")
    actual_digest = plan_sha256(raw_plan)
    if not re.fullmatch(r"[0-9a-f]{64}", expected_digest) or actual_digest != expected_digest:
        raise PlanError(
            f"计划文件 SHA-256 校验失败: expected={expected_digest!r}, actual={actual_digest}"
        )
    plan = plan_from_dict(raw_plan)
    if plan_sha256(plan) != expected_digest:
        raise PlanError("计划内容在类型解析后发生变化，拒绝执行")
    return plan, expected_digest


def load_execution_journal(
    path: Path,
) -> tuple[ExecutionJournal, str, list[dict[str, Any]]]:
    try:
        raw = _load_json_text(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise PlanError(f"无法读取执行 journal: {path}; {exc}") from exc
    if not isinstance(raw, dict):
        raise PlanError("执行 journal 顶层必须是对象")
    _reject_unknown_fields(
        raw,
        {"created_at", "plan_sha256", "plan", "records", "success"},
        "journal",
    )
    created_at = _require_string(raw.get("created_at"), "journal.created_at")
    plan_raw = raw.get("plan")
    expected_plan_digest = raw.get("plan_sha256")
    records_raw = raw.get("records")
    if not isinstance(plan_raw, dict) or not isinstance(expected_plan_digest, str):
        raise PlanError("执行 journal 缺少 plan 或 plan_sha256")
    if plan_sha256(plan_raw) != expected_plan_digest:
        raise PlanError("执行 journal 中的计划 SHA-256 校验失败")
    plan = plan_from_dict(plan_raw)
    if not isinstance(records_raw, list):
        raise PlanError("执行 journal 字段 records 必须是数组")
    records: list[ExecutionRecord] = []
    raw_records: list[dict[str, Any]] = []
    for index, value in enumerate(records_raw):
        if not isinstance(value, dict):
            raise PlanError(f"执行 journal records[{index}] 必须是对象")
        _reject_unknown_fields(
            value, {"action", "source", "target", "status", "message"},
            f"journal.records[{index}]"
        )
        record = ExecutionRecord(
            action=_require_string(value.get("action"), f"journal.records[{index}].action"),
            source=_require_string(
                value.get("source", ""), f"journal.records[{index}].source", allow_empty=True
            ),
            target=_require_string(
                value.get("target", ""), f"journal.records[{index}].target", allow_empty=True
            ),
            status=_require_string(value.get("status"), f"journal.records[{index}].status"),
            message=_require_string(
                value.get("message", ""), f"journal.records[{index}].message", allow_empty=True
            ),
        )
        records.append(record)
        raw_records.append(dict(value))
    success = raw.get("success")
    if type(success) is not bool:
        raise PlanError("执行 journal 字段 success 必须是布尔值")
    digest = hashlib.sha256(_canonical_json_bytes(raw)).hexdigest()
    return (
        ExecutionJournal(created_at, plan_to_dict(plan), records, success),
        digest,
        raw_records,
    )


def _journal_temp_names(
    item: PlannedFile, records: Sequence[Mapping[str, Any]]
) -> set[str]:
    names: set[str] = set()
    source_key = _collision_key(item.source_path)
    final_path_key = _collision_key(join_remote(item.source_dir, item.final_name))
    for record in records:
        action = record.get("action")
        source = record.get("source")
        target = record.get("target")
        if not isinstance(source, str) or not isinstance(target, str):
            continue
        if action == "rename-temp" and _collision_key(source) == source_key:
            names.add(split_remote(target)[1])
        elif action == "rename-final" and _collision_key(target) == final_path_key:
            names.add(split_remote(source)[1])
    return names


def _journal_expected_locations(
    item: PlannedFile, records: Sequence[Mapping[str, Any]]
) -> list[tuple[str, str]]:
    expected: list[tuple[str, str]] = []
    for record in records:
        action = record.get("action")
        status = record.get("status")
        source = record.get("source")
        target = record.get("target")
        message = str(record.get("message") or "")
        if status not in {"ok", "pending", "uncertain"}:
            continue
        if action == "move" and source == item.source_dir and target == item.target_dir:
            if item.final_name in message:
                expected.append((item.target_dir, item.final_name))
        elif action == "rename-final" and isinstance(target, str):
            if _collision_key(target) == _collision_key(
                join_remote(item.source_dir, item.final_name)
            ):
                expected.append((item.source_dir, item.final_name))
        elif action == "rename-temp" and isinstance(source, str) and isinstance(target, str):
            if _collision_key(source) == _collision_key(item.source_path):
                expected.append((item.source_dir, split_remote(target)[1]))
    expected.reverse()
    expected.append((item.source_dir, item.original_name))
    return expected


def inspect_recovery_state(
    alist: AListClient,
    plan: Plan,
    records: Sequence[Mapping[str, Any]],
) -> list[RecoveryState]:
    directories = {
        normalize_remote_path(value)
        for item in plan.files
        for value in (item.source_dir, item.target_dir)
    }
    listings = {
        directory: _directory_file_entries(alist, directory)
        for directory in directories
    }
    states: list[RecoveryState] = []
    occupied_locations: set[tuple[str, str]] = set()
    for item in plan.files:
        candidates = {item.original_name, item.final_name}
        candidates.update(_journal_temp_names(item, records))
        recovery_prefix = _recovery_prefix(item)
        found: list[tuple[str, Mapping[str, Any]]] = []
        for expected_dir, expected_name in _journal_expected_locations(item, records):
            preferred = [
                (expected_dir, entry)
                for entry in listings.get(expected_dir, [])
                if _collision_key(str(entry["name"])) == _collision_key(expected_name)
            ]
            if len(preferred) == 1:
                found = preferred
                break
        if found:
            candidate_directories: set[str] = set()
        else:
            candidate_directories = {item.source_dir, item.target_dir}
        for directory in candidate_directories:
            for entry in listings.get(directory, []):
                name = str(entry["name"])
                if (
                    any(_collision_key(name) == _collision_key(value) for value in candidates)
                    or _collision_key(name).startswith(_collision_key(recovery_prefix))
                ):
                    found.append((directory, entry))
        if len(found) != 1:
            raise PlanError(
                f"恢复前无法唯一定位计划文件: {item.source_path}; "
                f"found={[join_remote(directory, str(entry['name'])) for directory, entry in found]}"
            )
        current_dir, entry = found[0]
        current_name = str(entry["name"])
        location_key = (_collision_key(current_dir), _collision_key(current_name))
        if location_key in occupied_locations:
            raise PlanError(f"恢复状态中同一文件被多个计划项占用: {current_dir}/{current_name}")
        occupied_locations.add(location_key)
        actual_size = _entry_size_value(entry)
        actual_hash = _entry_hash_value(entry)
        if item.source_size is not None and actual_size != item.source_size:
            raise PlanError(
                f"恢复前文件大小与计划不一致: {current_dir}/{current_name}; "
                f"expected={item.source_size!r}, actual={actual_size!r}"
            )
        if item.source_hash is not None and actual_hash != item.source_hash:
            raise PlanError(
                f"恢复前文件哈希与计划不一致: {current_dir}/{current_name}"
            )
        states.append(RecoveryState(item, current_dir, current_name, entry))

    planned_locations = {
        (_collision_key(state.current_dir), _collision_key(state.current_name))
        for state in states
    }
    for state in states:
        original_key = (
            _collision_key(state.item.source_dir),
            _collision_key(state.item.original_name),
        )
        if original_key in planned_locations:
            continue
        occupants = [
            entry
            for entry in listings.get(state.item.source_dir, [])
            if _collision_key(str(entry["name"])) == _collision_key(state.item.original_name)
        ]
        if occupants:
            raise PlanError(
                f"恢复目标原文件名已被计划外条目占用: "
                f"{state.item.source_dir}/{state.item.original_name}"
            )
    return states


def recover_execution(
    alist: AListClient,
    plan: Plan,
    records: Sequence[Mapping[str, Any]],
    *,
    recovery_journal_path: Path,
) -> None:
    states = inspect_recovery_state(alist, plan, records)
    _reserve_output_path(recovery_journal_path)
    recovery = ExecutionJournal(
        created_at=datetime.now(timezone.utc).isoformat(),
        plan=plan_to_dict(plan),
        records=[],
    )
    try:
        recovery.save(recovery_journal_path)
    except Exception:
        try:
            recovery_journal_path.unlink()
        except FileNotFoundError:
            pass
        raise

    temporary: list[tuple[RecoveryState, str]] = []
    for state in states:
        temp_name = _recovery_name(state.item)
        source_path = join_remote(state.current_dir, state.current_name)
        record = _append_pending(
            recovery,
            recovery_journal_path,
            "recover-rename-temp",
            source_path,
            join_remote(state.current_dir, temp_name),
        )
        _rename_with_reconciliation(alist, source_path, temp_name)
        temporary.append((state, temp_name))
        _mark_record(recovery, recovery_journal_path, record, "ok")

    for state, temp_name in temporary:
        if _collision_key(state.current_dir) == _collision_key(state.item.source_dir):
            continue
        record = _append_pending(
            recovery,
            recovery_journal_path,
            "recover-move",
            state.current_dir,
            state.item.source_dir,
            temp_name,
        )
        _move_with_reconciliation(
            alist, state.current_dir, state.item.source_dir, [temp_name]
        )
        _mark_record(recovery, recovery_journal_path, record, "ok")

    for state, temp_name in temporary:
        current_path = join_remote(state.item.source_dir, temp_name)
        record = _append_pending(
            recovery,
            recovery_journal_path,
            "recover-rename-final",
            current_path,
            state.item.source_path,
        )
        _rename_with_reconciliation(alist, current_path, state.item.original_name)
        _mark_record(recovery, recovery_journal_path, record, "ok")

    validate_source_state(alist, plan, require_snapshot=True)
    lock_paths = {
        str(record.get("target"))
        for record in records
        if record.get("action") == "acquire-lock"
        and isinstance(record.get("target"), str)
        and record.get("target")
    }
    for lock_path in sorted(lock_paths):
        parent, name = split_remote(lock_path)
        matches = _collision_presence(_directory_file_names(alist, parent), name)
        if len(matches) == 1 and matches[0] == name:
            record = _append_pending(
                recovery, recovery_journal_path, "recover-release-lock", lock_path, ""
            )
            alist.remove(parent, [name])
            _mark_record(recovery, recovery_journal_path, record, "ok")
        elif matches:
            raise ApiError(f"恢复后整理锁状态不明确，未删除: {lock_path}; matches={matches}")
    recovery.success = True
    recovery.save(recovery_journal_path)


def print_plan(plan: Plan) -> None:
    icons = {"tv": "📺", "movie": "🎬", "collection": "📦"}
    print(f"\n{icons.get(plan.mode, '📁')} 模式: {plan.mode}")
    print(f"   源目录: {_terminal_text(plan.source_root)}")
    print(f"   目标目录: {_terminal_text(plan.target_root)}")
    print(f"   文件数: {len(plan.files)}")
    for warning in plan.warnings:
        print(f"   ⚠️  {_terminal_text(warning)}")
    print("   计划:")
    for item in plan.files:
        relative = item.source_path[len(plan.source_root) :].lstrip("/")
        source_label = relative or item.original_name
        print(
            f"      {_terminal_text(source_label)} → "
            f"{_terminal_text(item.target_dir)}/{_terminal_text(item.final_name)}"
        )


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _read_secret_file(path: Path, label: str) -> str:
    try:
        value = path.read_text(encoding="utf-8").strip()
    except OSError as exc:
        raise ScraperError(f"无法读取 {label} 文件: {path}; {exc}") from exc
    if not value:
        raise ScraperError(f"{label} 文件为空: {path}")
    return value


def _resolve_password(args: argparse.Namespace) -> str:
    if args.password_file:
        return _read_secret_file(args.password_file, "AList 密码")
    password = os.getenv("ALIST_PASSWORD")
    if password:
        return password
    if not sys.stdin.isatty():
        raise ScraperError(
            "缺少 AList 密码，请设置 ALIST_PASSWORD 或使用 --password-file"
        )
    return getpass.getpass("AList 密码: ")


def _resolve_tmdb_key(args: argparse.Namespace) -> str:
    if args.tmdb_key_file:
        return _read_secret_file(args.tmdb_key_file, "TMDB API Key")
    key = os.getenv("TMDB_API_KEY")
    if not key:
        raise ScraperError(
            "缺少 TMDB API Key，请设置 TMDB_API_KEY 或使用 --tmdb-key-file"
        )
    return key


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="TMDB 元数据刮削 + AList 安全重命名/移动工具",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("src", nargs="?", help="源目录（仅用于生成新计划）")
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    parser.add_argument("--parent", help="目标父目录；生成新计划时必须提供")
    parser.add_argument("--id", type=int, dest="tmdb_id", help="TMDB ID")
    parser.add_argument(
        "--type", choices=["tv", "movie", "collection", "auto"], help="媒体类型"
    )
    parser.add_argument(
        "--auto-match", action="store_true", help="根据源目录名自动搜索并选择 TMDB 条目"
    )
    parser.add_argument("--query", help="自动 TMDB 匹配使用的标题；默认取源目录名")
    parser.add_argument(
        "--min-confidence", type=float, default=0.88, help="自动匹配最低置信度"
    )
    parser.add_argument(
        "--wizard",
        action="store_true",
        help="生成并保存计划后，在同一进程中等待 SHA-256 确认并执行",
    )
    parser.add_argument("--season", type=int, help="电视剧季度；生成计划时默认为 1")
    parser.add_argument("--absolute", action="store_true", help="按绝对集数映射")
    parser.add_argument("--allow-unmapped", action="store_true")
    parser.add_argument("--prefer-simplified", action="store_true")
    parser.add_argument("--collection-map", type=Path)
    parser.add_argument(
        "--episode-map", type=Path, help="源集数到 SxxExx 的显式 JSON 覆盖映射"
    )
    parser.add_argument(
        "--episode-group", help="绝对集数使用的 TMDB episode group ID"
    )
    parser.add_argument("--allow-index-mapping", action="store_true")
    parser.add_argument(
        "--ignore-orphan-temp",
        action="store_true",
        help="忽略 .scraper-tmp-* 遗留条目；默认遇到即停止",
    )
    parser.add_argument("--search", help="搜索 TMDB；不连接 AList")

    parser.add_argument(
        "--plan-json",
        type=Path,
        help="将新生成的计划保存为可校验 JSON；拒绝覆盖已有文件",
    )
    parser.add_argument(
        "--execute-plan",
        type=Path,
        help="加载并执行已保存计划；必须同时提供 --execute 与计划 SHA-256",
    )
    parser.add_argument(
        "--approve-plan-sha256",
        help="人工核对后批准的完整 64 位计划 SHA-256",
    )
    parser.add_argument("--execute", action="store_true", help="仅配合 --execute-plan 使用")
    parser.add_argument("--no-dry-run", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--skip-poster", action="store_true")
    parser.add_argument("--overwrite-poster", action="store_true")
    parser.add_argument(
        "--cleanup-empty-source",
        action="store_true",
        help="成功后通过 AList 空目录接口清理确认仍为空的源目录",
    )
    parser.add_argument("--journal", type=Path, help="执行日志路径；拒绝覆盖已有文件")
    parser.add_argument(
        "--inspect-journal", type=Path, help="只读检查执行 journal 及其恢复摘要"
    )
    parser.add_argument(
        "--recover-journal", type=Path, help="根据失败的执行 journal 生成或执行恢复"
    )
    parser.add_argument(
        "--approve-recovery-sha256",
        help="批准 --recover-journal 显示的完整 64 位 journal SHA-256",
    )

    parser.add_argument("--alist-url", default=os.getenv("ALIST_URL", DEFAULT_ALIST_URL))
    parser.add_argument("--username", default=os.getenv("ALIST_USERNAME", "admin"))
    parser.add_argument("--password-file", type=Path, help="仅包含 AList 密码的本地文件")
    parser.add_argument("--tmdb-key-file", type=Path, help="仅包含 TMDB API Key 的本地文件")
    parser.add_argument(
        "--allow-insecure-http",
        action="store_true",
        help="允许非环回地址使用明文 HTTP 连接 AList",
    )
    parser.add_argument("--language", default=os.getenv("TMDB_LANGUAGE", "zh-CN"))
    parser.add_argument("--timeout", type=float, default=20.0)
    parser.add_argument("--retries", type=int, default=3)
    return parser


def _search_tmdb(client: TMDBClient, query: str) -> None:
    print("【剧集搜索结果】")
    for item in client.get("/search/tv", query=query).get("results", [])[:5]:
        year = _extract_year(item.get("first_air_date"))
        print(f"  TMDB ID: {int(item['id']):>7d} | {year} | {_terminal_text(item.get('name', ''))}")
    print("\n【电影搜索结果】")
    for item in client.get("/search/movie", query=query).get("results", [])[:5]:
        year = _extract_year(item.get("release_date"))
        print(f"  TMDB ID: {int(item['id']):>7d} | {year} | {_terminal_text(item.get('title', ''))}")
    print("\n【合集搜索结果】")
    for item in client.get("/search/collection", query=query).get("results", [])[:5]:
        print(f"  TMDB ID: {int(item['id']):>7d} | {_terminal_text(item.get('name', ''))}")


def _normalize_match_title(value: str) -> str:
    cleaned = re.sub(r"\[[^\]]*\]|\([^)]*(?:1080|2160|720|x26|hevc)[^)]*\)", " ", value)
    cleaned = re.sub(
        r"\b(?:2160p|1080p|720p|480p|bluray|blu-ray|web-?dl|webrip|x26[45]|hevc|av1)\b",
        " ",
        cleaned,
        flags=re.IGNORECASE,
    )
    cleaned = re.sub(r"(?:19|20)\d{2}", " ", cleaned)
    return "".join(char for char in unicodedata.normalize("NFKC", cleaned).casefold() if char.isalnum())


def _query_from_source(src: str) -> str:
    name = normalize_remote_path(src).rstrip("/").rsplit("/", 1)[-1]
    name = re.sub(r"\[[^\]]*\]", " ", name)
    name = re.sub(r"\{(?:tmdb|imdb)-[^{}]+\}", " ", name, flags=re.IGNORECASE)
    name = re.sub(r"(?:season|s)\s*\d+", " ", name, flags=re.IGNORECASE)
    name = re.sub(
        r"\b(?:2160p|1080p|720p|480p|bluray|web-?dl|webrip|x26[45]|hevc|av1)\b",
        " ",
        name,
        flags=re.IGNORECASE,
    )
    name = re.sub(r"[._]+", " ", name)
    return re.sub(r"\s+", " ", name).strip(" -[]()") or name


def auto_match_tmdb(
    client: TMDBClient,
    query: str,
    *,
    media_type: str | None,
    min_confidence: float,
) -> tuple[AutoMatch, list[AutoMatch]]:
    if not query.strip():
        raise PlanError("自动匹配查询为空")
    if not 0 <= min_confidence <= 1:
        raise PlanError("自动匹配最低置信度必须在 0 到 1 之间")
    query_key = _normalize_match_title(query)
    query_year_match = re.search(r"(?:19|20)\d{2}", query)
    query_year = query_year_match.group(0) if query_year_match else None
    types = [media_type] if media_type in {"tv", "movie"} else ["tv", "movie"]
    candidates: list[AutoMatch] = []
    for candidate_type in types:
        response = client.get(f"/search/{candidate_type}", query=query)
        for item in (response.get("results") or [])[:10]:
            if not isinstance(item, Mapping) or isinstance(item.get("id"), bool):
                continue
            try:
                tmdb_id = int(item["id"])
            except (KeyError, TypeError, ValueError):
                continue
            title_fields = (
                (item.get("name"), item.get("original_name"))
                if candidate_type == "tv"
                else (item.get("title"), item.get("original_title"))
            )
            titles = [str(value) for value in title_fields if isinstance(value, str) and value]
            if not titles:
                continue
            similarity = max(
                difflib.SequenceMatcher(None, query_key, _normalize_match_title(title)).ratio()
                for title in titles
            )
            date_value = item.get("first_air_date" if candidate_type == "tv" else "release_date")
            year = _extract_year(date_value)
            confidence = similarity
            if query_year:
                confidence += 0.08 if year == query_year else -0.12
            confidence = max(0.0, min(1.0, confidence))
            candidates.append(
                AutoMatch(candidate_type, tmdb_id, titles[0], year, confidence)
            )
    candidates.sort(key=lambda item: (-item.confidence, item.media_type, item.tmdb_id))
    if not candidates:
        raise PlanError(f"TMDB 未找到自动匹配候选: {query}")
    best = candidates[0]
    if best.confidence < min_confidence:
        preview = "; ".join(
            f"{item.media_type}/{item.tmdb_id} {item.title} ({item.confidence:.1%})"
            for item in candidates[:3]
        )
        raise PlanError(
            f"自动匹配最高置信度 {best.confidence:.1%} 低于阈值 {min_confidence:.1%}: {preview}"
        )
    if len(candidates) > 1 and best.confidence - candidates[1].confidence < 0.03:
        raise PlanError(
            "自动匹配前两名过于接近，拒绝自动选择: "
            + "; ".join(
                f"{item.media_type}/{item.tmdb_id} {item.title} ({item.confidence:.1%})"
                for item in candidates[:2]
            )
        )
    return best, candidates


def _new_alist_client(args: argparse.Namespace) -> AListClient:
    client = AListClient(
        args.alist_url,
        args.username,
        _resolve_password(args),
        timeout=args.timeout,
        retries=args.retries,
        allow_insecure_http=args.allow_insecure_http,
    )
    client.login()
    return client


def main(argv: Sequence[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    execute = bool(args.execute or args.no_dry_run)

    try:
        if args.season is not None and args.season < 0:
            raise ScraperError("--season 不能小于 0")
        if args.tmdb_id is not None and args.tmdb_id <= 0:
            raise ScraperError("--id 必须是正整数")
        if args.timeout <= 0:
            raise ScraperError("--timeout 必须大于 0")
        if args.retries < 0:
            raise ScraperError("--retries 不能小于 0")
        if args.skip_poster and args.overwrite_poster:
            raise ScraperError("--skip-poster 与 --overwrite-poster 不能同时使用")

        if args.inspect_journal:
            if any(
                (
                    args.recover_journal,
                    args.execute_plan,
                    args.src,
                    args.search,
                    execute,
                    args.approve_recovery_sha256,
                    args.auto_match,
                    args.wizard,
                    args.query,
                    args.episode_map,
                    args.episode_group,
                    args.parent,
                    args.type,
                    args.tmdb_id,
                    args.skip_poster,
                    args.overwrite_poster,
                    args.cleanup_empty_source,
                )
            ):
                raise ScraperError("--inspect-journal 不能与计划、执行或恢复参数同时使用")
            journal, digest, _ = load_execution_journal(args.inspect_journal)
            print(f"Journal: {args.inspect_journal}")
            print(f"创建时间: {journal.created_at}")
            print(f"计划 SHA-256: {plan_sha256(journal.plan)}")
            print(f"Journal SHA-256: {digest}")
            print(f"执行成功: {'是' if journal.success else '否'}")
            print(f"记录数: {len(journal.records)}")
            for record in journal.records:
                print(
                    f"  {record.action}: {record.status} "
                    f"{_terminal_text(record.source)} → {_terminal_text(record.target)}"
                )
            return 0

        if args.recover_journal:
            forbidden = (
                args.execute_plan,
                args.src,
                args.parent,
                args.tmdb_id,
                args.type,
                args.search,
                args.plan_json,
                args.collection_map,
                args.episode_map,
                args.episode_group,
                args.query,
                args.approve_plan_sha256,
            )
            if any(value is not None for value in forbidden):
                raise ScraperError("--recover-journal 不能与计划生成或普通执行参数同时使用")
            if args.auto_match or args.wizard:
                raise ScraperError("--recover-journal 不能与自动匹配或 wizard 同时使用")
            if args.skip_poster or args.overwrite_poster or args.cleanup_empty_source:
                raise ScraperError("--recover-journal 不接受图稿或源目录清理参数")
            journal, digest, raw_records = load_execution_journal(args.recover_journal)
            if journal.success:
                raise ScraperError("该 journal 已标记成功，不需要恢复")
            plan = plan_from_dict(journal.plan)
            alist = _new_alist_client(args)
            states = inspect_recovery_state(alist, plan, raw_records)
            print(f"恢复来源: {args.recover_journal}")
            print(f"Journal SHA-256: {digest}")
            for state in states:
                print(
                    f"  {_terminal_text(join_remote(state.current_dir, state.current_name))} "
                    f"→ {_terminal_text(state.item.source_path)}"
                )
            if not execute:
                print(
                    "恢复 DRY RUN：未修改远端文件。确认后添加 "
                    f"--approve-recovery-sha256 {digest} --execute。"
                )
                return 0
            approved = (args.approve_recovery_sha256 or "").lower()
            if approved != digest:
                raise ScraperError("恢复批准 SHA-256 与当前 journal 不匹配")
            recovery_path = args.journal or Path(
                f"scraper-recovery-{datetime.now().strftime('%Y%m%d-%H%M%S-%f')}.json"
            )
            recover_execution(
                alist,
                plan,
                raw_records,
                recovery_journal_path=recovery_path,
            )
            print(f"✅ 恢复完成；恢复日志: {recovery_path}")
            return 0

        if args.approve_recovery_sha256:
            raise ScraperError("--approve-recovery-sha256 只能与 --recover-journal 使用")

        if args.search:
            search_extras = (
                args.src,
                args.parent,
                args.tmdb_id,
                args.type,
                args.season,
                args.collection_map,
                args.episode_map,
                args.episode_group,
                args.query,
                args.plan_json,
                args.execute_plan,
                args.journal,
            )
            search_flags = (
                execute,
                args.absolute,
                args.allow_unmapped,
                args.prefer_simplified,
                args.allow_index_mapping,
                args.ignore_orphan_temp,
                args.skip_poster,
                args.overwrite_poster,
                args.cleanup_empty_source,
                bool(args.approve_plan_sha256),
                args.auto_match,
                args.wizard,
            )
            if any(value is not None for value in search_extras) or any(search_flags):
                raise ScraperError("--search 不能与媒体计划或执行参数同时使用")
            client = TMDBClient(
                _resolve_tmdb_key(args),
                language=args.language,
                timeout=args.timeout,
                retries=args.retries,
            )
            _search_tmdb(client, args.search)
            return 0

        if args.execute_plan:
            if not execute:
                raise ScraperError("--execute-plan 必须同时提供 --execute")
            generation_values = (
                args.src,
                args.parent,
                args.tmdb_id,
                args.type,
                args.plan_json,
                args.season,
                args.collection_map,
                args.episode_map,
                args.episode_group,
                args.query,
            )
            generation_flags = (
                args.absolute,
                args.allow_unmapped,
                args.prefer_simplified,
                args.allow_index_mapping,
                args.ignore_orphan_temp,
                args.auto_match,
                args.wizard,
            )
            if any(value is not None for value in generation_values) or any(generation_flags):
                raise ScraperError(
                    "执行已保存计划时不得同时提供源目录或计划生成参数"
                )
            plan, digest = load_plan_json(args.execute_plan)
            approved = (args.approve_plan_sha256 or "").lower()
            if not re.fullmatch(r"[0-9a-f]{64}", approved):
                raise ScraperError("--approve-plan-sha256 必须是完整的 64 位十六进制值")
            if approved != digest:
                raise ScraperError(
                    f"批准的计划 SHA-256 不匹配: approved={approved}, actual={digest}"
                )
            print_plan(plan)
            print(f"\n计划 SHA-256: {digest}")

            alist = _new_alist_client(args)
            poster_needed = bool(planned_artwork(plan)) and not args.skip_poster
            tmdb_client: TMDBClient | None = None
            if poster_needed:
                tmdb_client = TMDBClient(
                    _resolve_tmdb_key(args),
                    language=args.language,
                    timeout=args.timeout,
                    retries=args.retries,
                )
            journal_path = args.journal or Path(
                f"scraper-journal-{datetime.now().strftime('%Y%m%d-%H%M%S-%f')}.json"
            )
            execute_plan(
                alist,
                tmdb_client,
                plan,
                journal_path=journal_path,
                skip_poster=args.skip_poster,
                overwrite_poster=args.overwrite_poster,
                cleanup_empty_source=args.cleanup_empty_source,
            )
            print(f"\n✅ 完成 {len(plan.files)} 个文件；执行日志: {journal_path}")
            return 0

        if execute and args.wizard:
            raise ScraperError("--wizard 不需要也不能同时使用 --execute")
        if execute:
            raise ScraperError(
                "安全策略禁止对实时重新扫描的计划直接执行。先使用 --plan-json 保存计划，"
                "再用 --execute-plan、--approve-plan-sha256 和 --execute 执行。"
            )
        if args.approve_plan_sha256:
            raise ScraperError("--approve-plan-sha256 只能与 --execute-plan 使用")
        if (
            args.journal
            or args.skip_poster
            or args.overwrite_poster
            or args.cleanup_empty_source
        ) and not args.wizard:
            raise ScraperError(
                "--journal、--skip-poster 和 --overwrite-poster 只能用于执行已保存计划"
            )
        if not args.src:
            parser.error("生成计划必须提供 src")
        if not args.parent:
            parser.error("生成计划必须提供 --parent")
        if not args.type:
            parser.error("生成计划必须提供 --type")

        if args.tmdb_id is not None and (args.auto_match or args.type == "auto"):
            raise ScraperError("显式 --id 不能与 --auto-match 或 --type auto 同时使用")
        if args.auto_match and args.type == "collection":
            raise ScraperError("合集不能自动匹配；请明确提供合集 TMDB ID")
        if args.query and not (args.auto_match or args.type == "auto"):
            raise ScraperError("--query 只能与 --auto-match 或 --type auto 使用")
        if not 0 <= args.min_confidence <= 1:
            raise ScraperError("--min-confidence 必须在 0 到 1 之间")

        tmdb_client = TMDBClient(
            _resolve_tmdb_key(args),
            language=args.language,
            timeout=args.timeout,
            retries=args.retries,
        )
        if args.auto_match or args.type == "auto":
            query = args.query or _query_from_source(args.src)
            requested_type = args.type if args.type in {"tv", "movie"} else None
            match, candidates = auto_match_tmdb(
                tmdb_client,
                query,
                media_type=requested_type,
                min_confidence=args.min_confidence,
            )
            print("自动匹配候选:")
            for candidate in candidates[:3]:
                print(
                    f"  {candidate.media_type}/{candidate.tmdb_id} | "
                    f"{candidate.confidence:.1%} | {_terminal_text(candidate.title)} "
                    f"({candidate.year})"
                )
            args.type = match.media_type
            args.tmdb_id = match.tmdb_id
            print(
                f"已选择: {match.media_type}/{match.tmdb_id} "
                f"{_terminal_text(match.title)} ({match.confidence:.1%})"
            )
        if not args.tmdb_id:
            parser.error("生成计划必须提供 --id，或启用 --auto-match/--type auto")

        if args.type == "tv":
            if args.collection_map is not None or args.allow_index_mapping:
                raise ScraperError("电视剧模式不接受合集映射参数")
            if args.absolute and args.allow_unmapped:
                raise ScraperError("绝对集数模式不允许 --allow-unmapped 猜测映射")
            if args.episode_group and not args.absolute:
                raise ScraperError("--episode-group 必须与 --absolute 同时使用")
            if args.episode_group and not re.fullmatch(r"[A-Za-z0-9_-]{1,100}", args.episode_group):
                raise ScraperError("--episode-group 格式无效")
        elif args.type == "movie":
            if any(
                (
                    args.season is not None,
                    args.absolute,
                    args.allow_unmapped,
                    args.prefer_simplified,
                    args.collection_map is not None,
                    args.episode_map is not None,
                    args.episode_group is not None,
                    args.allow_index_mapping,
                )
            ):
                raise ScraperError("电影模式收到不适用的剧集或合集参数")
        else:
            if any(
                (
                    args.season is not None,
                    args.absolute,
                    args.allow_unmapped,
                    args.prefer_simplified,
                    args.episode_map is not None,
                    args.episode_group is not None,
                )
            ):
                raise ScraperError("合集模式收到不适用的剧集参数")
            if args.collection_map is not None and args.allow_index_mapping:
                raise ScraperError(
                    "--collection-map 与 --allow-index-mapping 只能选择一种"
                )

        alist = _new_alist_client(args)
        if args.type == "tv":
            plan = build_tv_plan(
                alist,
                tmdb_client,
                src_path=args.src,
                parent_path=args.parent,
                tmdb_id=args.tmdb_id,
                season=args.season if args.season is not None else 1,
                absolute=args.absolute,
                prefer_simplified=args.prefer_simplified,
                allow_unmapped=args.allow_unmapped,
                ignore_orphan_temp=args.ignore_orphan_temp,
                episode_map_path=args.episode_map,
                episode_group_id=args.episode_group,
            )
        elif args.type == "movie":
            plan = build_movie_plan(
                alist,
                tmdb_client,
                src_path=args.src,
                parent_path=args.parent,
                tmdb_id=args.tmdb_id,
                ignore_orphan_temp=args.ignore_orphan_temp,
            )
        else:
            plan = build_collection_plan(
                alist,
                tmdb_client,
                src_path=args.src,
                parent_path=args.parent,
                tmdb_id=args.tmdb_id,
                mapping_path=args.collection_map,
                allow_index_mapping=args.allow_index_mapping,
                ignore_orphan_temp=args.ignore_orphan_temp,
            )

        print_plan(plan)
        digest = plan_sha256(plan)
        print(f"\n计划 SHA-256: {digest}")
        if args.wizard:
            if not sys.stdin.isatty():
                raise ScraperError("--wizard 需要交互式终端；批处理请使用两阶段计划流程")
            plan_path = args.plan_json or Path(
                f"scraper-plan-{datetime.now().strftime('%Y%m%d-%H%M%S-%f')}.json"
            )
            written_digest = write_plan_json(plan, plan_path)
            print(f"计划已写入: {plan_path}")
            print("请先打开计划文件核对所有路径。")
            confirmation = input("粘贴完整计划 SHA-256 以执行，直接回车取消: ").strip().lower()
            if confirmation != written_digest:
                print("未执行：确认摘要不匹配或已取消。")
                return 0
            journal_path = args.journal or Path(
                f"scraper-journal-{datetime.now().strftime('%Y%m%d-%H%M%S-%f')}.json"
            )
            execute_plan(
                alist,
                tmdb_client if planned_artwork(plan) and not args.skip_poster else None,
                plan,
                journal_path=journal_path,
                skip_poster=args.skip_poster,
                overwrite_poster=args.overwrite_poster,
                cleanup_empty_source=args.cleanup_empty_source,
            )
            print(f"✅ 完成 {len(plan.files)} 个文件；执行日志: {journal_path}")
            return 0
        if args.plan_json:
            written_digest = write_plan_json(plan, args.plan_json)
            print(f"计划已写入: {args.plan_json}")
            print(
                "审核后执行：python3 scraper.py --execute-plan "
                f"{args.plan_json} --approve-plan-sha256 {written_digest} --execute"
            )
        else:
            print("未指定 --plan-json；该临时计划不能进入执行流程。")
        print("\n🔍 DRY RUN：未修改任何远端文件。")
        return 0
    except (ScraperError, OSError, ValueError) as exc:
        print(f"❌ {exc}", file=sys.stderr)
        return 2
    except KeyboardInterrupt:
        print("\n已取消。", file=sys.stderr)
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
