"""Bounded, retrying HTTP transport with credential-safe errors."""

from __future__ import annotations

import http.client
import json
import re
import time
import unicodedata
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Iterable, Mapping
from typing import Any, Callable

from ..errors import ApiError

MAX_JSON_RESPONSE_BYTES = 16 * 1024 * 1024
MAX_POSTER_BYTES = 32 * 1024 * 1024
MAX_ERROR_BODY_BYTES = 4096
_CONTENT_RANGE_RE = re.compile(r"bytes[ \t]+([0-9]+)-([0-9]+)/([0-9]+|\*)", re.IGNORECASE)
SENSITIVE_KEYS = {
    "api_key", "api-key", "password", "passwd", "pass", "archive_pass",
    "archive-password", "token", "access_token", "access-token", "authorization",
}


REDIRECT_SENSITIVE_HEADERS = {
    "authorization", "cookie", "proxy-authorization", "x-api-key",
}


def _url_origin(url: str) -> tuple[str, str, int]:
    parsed = urllib.parse.urlsplit(url)
    if not parsed.hostname or parsed.scheme not in {"http", "https"}:
        raise ApiError("重定向地址无效")
    try:
        port = parsed.port or (443 if parsed.scheme == "https" else 80)
    except ValueError as exc:
        raise ApiError("重定向地址端口无效") from exc
    return parsed.scheme, parsed.hostname.casefold(), port


class ValidatingRedirectHandler(urllib.request.HTTPRedirectHandler):
    """Validate redirects and never forward credentials across origins."""

    def __init__(self, validator: Callable[[str], None] | None = None) -> None:
        super().__init__()
        self.validator = validator

    def redirect_request(self, req, fp, code, msg, headers, newurl):  # type: ignore[no-untyped-def]
        old_origin = _url_origin(req.full_url)
        new_origin = _url_origin(newurl)
        if old_origin[0] == "https" and new_origin[0] != "https":
            raise ApiError("拒绝从 HTTPS 降级重定向到 HTTP")
        if self.validator is not None:
            self.validator(newurl)
        redirected = super().redirect_request(req, fp, code, msg, headers, newurl)
        if redirected is not None and old_origin != new_origin:
            for name in list(redirected.headers) + list(redirected.unredirected_hdrs):
                if name.casefold() in REDIRECT_SENSITIVE_HEADERS:
                    redirected.remove_header(name)
        return redirected


def redact_url(url: str) -> str:
    """Remove query credentials and user information from an error URL."""
    try:
        parsed = urllib.parse.urlsplit(url)
        query = urllib.parse.parse_qsl(parsed.query, keep_blank_values=True)
        # Provider/CDN signatures use many non-standard key names.  Error URLs
        # only need query names for diagnostics, never their values.
        redacted = [(key, "<redacted>") for key, _value in query]
        hostname = parsed.hostname or ""
        if ":" in hostname and not hostname.startswith("["):
            hostname = f"[{hostname}]"
        port = f":{parsed.port}" if parsed.port is not None else ""
        netloc = f"<redacted>@{hostname}{port}" if parsed.username is not None else f"{hostname}{port}"
        return urllib.parse.urlunsplit((
            parsed.scheme, netloc, parsed.path, urllib.parse.urlencode(redacted),
            "<redacted>" if parsed.fragment else "",
        ))
    except ValueError:
        return "<invalid-url>"


SENSITIVE_FIELD_RE = re.compile(
    r'(?i)("?(?:api[_-]?key|password|passwd|pass|archive[_-]?pass|token|access[_-]?token|authorization)"?\s*[:=]\s*)'
    r'("?)([^"\s,;&}]+)("?)'
)
CHINESE_PASSWORD_MARKER_RE = re.compile(
    r"((?:解压|压缩包|归档)?\s*密码\s*[:：=]\s*)([^\s,，;；/\\]+)",
    re.IGNORECASE,
)


def redact_sensitive_text(text: str, secrets: Iterable[str] = ()) -> str:
    """Remove credentials, control characters and excessive error detail."""
    cleaned = "".join(
        "?" if unicodedata.category(char) in {"Cc", "Cf", "Cs"} else char
        for char in str(text)
    )
    values = {value for value in secrets if isinstance(value, str) and value}
    for secret in sorted(values, key=len, reverse=True):
        cleaned = cleaned.replace(secret, "<redacted>")
    cleaned = SENSITIVE_FIELD_RE.sub(r'\1"<redacted>"', cleaned)
    cleaned = CHINESE_PASSWORD_MARKER_RE.sub(r"\1<redacted>", cleaned)
    return cleaned.replace("\r", " ").replace("\n", " ")[:500]


class JsonHttpClient:
    def __init__(
        self,
        timeout: float = 20.0,
        retries: int = 3,
        *,
        proxy_url: str | None = None,
    ) -> None:
        if timeout <= 0:
            raise ValueError("timeout 必须大于 0")
        if retries < 0:
            raise ValueError("retries 不能小于 0")
        self.timeout = timeout
        self.retries = retries
        self.proxy_url = proxy_url

    def _build_opener(
        self, url_validator: Callable[[str], None] | None = None,
    ) -> urllib.request.OpenerDirector:
        handlers: list[Any] = []
        if self.proxy_url:
            handlers.append(urllib.request.ProxyHandler({
                "http": self.proxy_url,
                "https": self.proxy_url,
            }))
        else:
            # Never fall back to an ambient host proxy (macOS system proxy
            # included): loopback targets such as the AList origin would be
            # dialed from the proxy's own host and hang or misroute.
            handlers.append(urllib.request.ProxyHandler({}))
        if url_validator is not None:
            handlers.append(ValidatingRedirectHandler(url_validator))
        return urllib.request.build_opener(*handlers)

    @staticmethod
    def _secrets(url: str, headers: Mapping[str, str], body: Mapping[str, Any] | None = None) -> set[str]:
        values: set[str] = set()
        try:
            parsed_url = urllib.parse.urlsplit(url)
            for key, value in urllib.parse.parse_qsl(parsed_url.query, keep_blank_values=True):
                if key.lower() in SENSITIVE_KEYS and value:
                    values.add(value)
        except ValueError:
            pass
        for key, value in headers.items():
            if key.lower() in {"authorization", "x-api-key"} and value:
                secret = str(value)
                values.add(secret)
                if key.lower() == "authorization" and " " in secret:
                    scheme, credential = secret.split(None, 1)
                    if scheme and credential:
                        values.add(credential)
        for key, value in (body or {}).items():
            if key.lower() in SENSITIVE_KEYS and value:
                values.add(str(value))
        return values

    def request_json(
        self,
        url: str,
        *,
        method: str = "GET",
        headers: Mapping[str, str] | None = None,
        json_body: Mapping[str, Any] | None = None,
        raw_body: bytes | None = None,
        retryable: bool = True,
        url_validator: Callable[[str], None] | None = None,
    ) -> dict[str, Any]:
        if json_body is not None and raw_body is not None:
            raise ValueError("json_body 与 raw_body 不能同时提供")
        request_headers = dict(headers or {})
        body = raw_body
        if json_body is not None:
            body = json.dumps(json_body, ensure_ascii=False).encode("utf-8")
            request_headers.setdefault("Content-Type", "application/json")
        secret_values = self._secrets(url, request_headers, json_body)
        last_error: Exception | None = None
        max_attempts = self.retries + 1 if retryable else 1
        if url_validator is not None:
            url_validator(url)
        opener = self._build_opener(url_validator)
        for attempt in range(max_attempts):
            try:
                request = urllib.request.Request(url, data=body, method=method, headers=request_headers)
                open_request = opener.open if opener is not None else urllib.request.urlopen
                with open_request(request, timeout=self.timeout) as response:
                    raw = response.read(MAX_JSON_RESPONSE_BYTES + 1)
                if len(raw) > MAX_JSON_RESPONSE_BYTES:
                    raise ApiError(f"接口响应超过 {MAX_JSON_RESPONSE_BYTES} 字节上限: {redact_url(url)}")
                try:
                    parsed = json.loads(raw.decode("utf-8"))
                except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                    raise ApiError(f"接口返回的不是有效 JSON: {redact_url(url)}") from exc
                if not isinstance(parsed, dict):
                    raise ApiError(f"接口返回格式异常: {redact_url(url)}")
                return parsed
            except urllib.error.HTTPError as exc:
                last_error = exc
                status_retryable = exc.code == 429 or 500 <= exc.code < 600
                if not retryable or not status_retryable or attempt >= max_attempts - 1:
                    detail = ""
                    try:
                        detail = redact_sensitive_text(
                            exc.read(MAX_ERROR_BODY_BYTES + 1).decode("utf-8", errors="replace"),
                            secret_values,
                        )
                    except Exception:
                        pass
                    finally:
                        exc.close()
                    suffix = f"; {detail}" if detail else ""
                    raise ApiError(f"HTTP {exc.code}: {redact_url(url)}{suffix}", status_code=exc.code) from exc
                exc.close()
            except (urllib.error.URLError, TimeoutError, OSError, http.client.HTTPException) as exc:
                last_error = exc
                if attempt >= max_attempts - 1:
                    raise ApiError(
                        f"网络请求失败: {redact_url(url)}; {redact_sensitive_text(str(exc), secret_values)}"
                    ) from exc
            time.sleep(min(2**attempt, 8))
        raise ApiError(
            f"网络请求失败: {redact_url(url)}; {redact_sensitive_text(str(last_error), secret_values)}"
        )

    def request_bytes(
        self,
        url: str,
        *,
        headers: Mapping[str, str] | None = None,
        max_bytes: int = MAX_POSTER_BYTES,
        url_validator: Callable[[str], None] | None = None,
    ) -> bytes:
        if max_bytes <= 0:
            raise ValueError("max_bytes 必须大于 0")
        request_headers = dict(headers or {})
        secret_values = self._secrets(url, request_headers)
        last_error: Exception | None = None
        if url_validator is not None:
            url_validator(url)
        opener = self._build_opener(url_validator) or urllib.request.build_opener()
        for attempt in range(self.retries + 1):
            try:
                request = urllib.request.Request(url, headers=request_headers)
                with opener.open(request, timeout=self.timeout) as response:
                    data = response.read(max_bytes + 1)
                if len(data) > max_bytes:
                    raise ApiError(f"下载内容超过 {max_bytes} 字节上限: {redact_url(url)}")
                return data
            except urllib.error.HTTPError as exc:
                last_error = exc
                retryable = exc.code == 429 or 500 <= exc.code < 600
                if not retryable or attempt >= self.retries:
                    exc.close()
                    raise ApiError(f"下载失败，HTTP {exc.code}: {redact_url(url)}", status_code=exc.code) from exc
                exc.close()
            except (urllib.error.URLError, TimeoutError, OSError, http.client.HTTPException) as exc:
                last_error = exc
                if attempt >= self.retries:
                    raise ApiError(
                        f"下载失败: {redact_url(url)}; {redact_sensitive_text(str(exc), secret_values)}"
                    ) from exc
            time.sleep(min(2**attempt, 8))
        raise ApiError(
            f"下载失败: {redact_url(url)}; {redact_sensitive_text(str(last_error), secret_values)}"
        )

    def request_exact_range(
        self,
        url: str,
        *,
        offset: int,
        length: int,
        headers: Mapping[str, str] | None = None,
        expected_total: int | None = None,
        url_validator: Callable[[str], None] | None = None,
    ) -> bytes:
        """Read one exact HTTP byte range and reject any ambiguous response.

        This primitive is intentionally stricter than :meth:`request_bytes`:
        a bounded body alone cannot prove that a server honored ``Range``.  A
        successful response must be HTTP 206, carry one matching
        ``Content-Range`` field, and contain exactly ``length`` bytes.
        """
        if isinstance(offset, bool) or not isinstance(offset, int) or offset < 0:
            raise ValueError("offset 必须是非负整数")
        if isinstance(length, bool) or not isinstance(length, int) or length <= 0:
            raise ValueError("length 必须是正整数")
        if expected_total is not None:
            if (
                isinstance(expected_total, bool)
                or not isinstance(expected_total, int)
                or expected_total <= 0
            ):
                raise ValueError("expected_total 必须是正整数")
            if offset + length > expected_total:
                raise ValueError("请求 Range 超出 expected_total")

        end = offset + length - 1
        request_headers = dict(headers or {})
        for name in list(request_headers):
            if name.casefold() == "range":
                del request_headers[name]
        request_headers["Range"] = f"bytes={offset}-{end}"

        # Provider links and headers may carry opaque signatures under names we
        # cannot predict.  Treat every query/header value as secret when an
        # underlying transport exception is projected into an operator error.
        secret_values = self._secrets(url, request_headers)
        secret_values.update(
            str(value) for value in request_headers.values() if str(value)
        )
        try:
            secret_values.update(
                value
                for _key, value in urllib.parse.parse_qsl(
                    urllib.parse.urlsplit(url).query,
                    keep_blank_values=True,
                )
                if value
            )
        except ValueError:
            pass

        if url_validator is not None:
            url_validator(url)
        opener = self._build_opener(url_validator)
        last_error: Exception | None = None
        for attempt in range(self.retries + 1):
            try:
                request = urllib.request.Request(url, headers=request_headers)
                with opener.open(request, timeout=self.timeout) as response:
                    status = getattr(response, "status", None)
                    if status is None:
                        getcode = getattr(response, "getcode", None)
                        status = getcode() if callable(getcode) else None
                    if status != 206:
                        raise ApiError(
                            "下载服务器未严格执行 HTTP Range: "
                            f"expected_status=206, actual_status={status!r}; "
                            f"{redact_url(url)}"
                        )

                    response_headers = getattr(response, "headers", None)
                    content_range_values: list[str] = []
                    if response_headers is not None:
                        get_all = getattr(response_headers, "get_all", None)
                        if callable(get_all):
                            content_range_values = [
                                str(value) for value in (get_all("Content-Range") or [])
                            ]
                        else:
                            get_header = getattr(response_headers, "get", None)
                            value = get_header("Content-Range") if callable(get_header) else None
                            if value is not None:
                                content_range_values = [str(value)]
                    if not content_range_values:
                        getheader = getattr(response, "getheader", None)
                        value = getheader("Content-Range") if callable(getheader) else None
                        if value is not None:
                            content_range_values = [str(value)]
                    if len(content_range_values) != 1:
                        raise ApiError(
                            "下载服务器未返回唯一 Content-Range: "
                            f"{redact_url(url)}"
                        )

                    match = _CONTENT_RANGE_RE.fullmatch(content_range_values[0])
                    if match is None:
                        raise ApiError(
                            "下载服务器返回了无效 Content-Range: "
                            f"{redact_url(url)}"
                        )
                    actual_start = int(match.group(1))
                    actual_end = int(match.group(2))
                    actual_total_text = match.group(3)
                    if actual_start != offset or actual_end != end:
                        raise ApiError(
                            "下载服务器返回的 Content-Range 范围不匹配: "
                            f"expected={offset}-{end}, actual={actual_start}-{actual_end}; "
                            f"{redact_url(url)}"
                        )
                    if actual_total_text != "*":
                        actual_total = int(actual_total_text)
                        if actual_total <= actual_end:
                            raise ApiError(
                                "下载服务器返回了无效 Content-Range 总大小: "
                                f"{redact_url(url)}"
                            )
                        if expected_total is not None and actual_total != expected_total:
                            raise ApiError(
                                "下载服务器返回的 Content-Range 总大小不匹配: "
                                f"expected={expected_total}, actual={actual_total}; "
                                f"{redact_url(url)}"
                            )
                    elif expected_total is not None:
                        raise ApiError(
                            "下载服务器未返回可验证的 Content-Range 总大小: "
                            f"expected={expected_total}; {redact_url(url)}"
                        )

                    data = response.read(length + 1)
                if len(data) != length:
                    raise ApiError(
                        "Range 响应长度不匹配: "
                        f"expected={length}, actual={len(data)}; {redact_url(url)}"
                    )
                return data
            except urllib.error.HTTPError as exc:
                last_error = exc
                retryable = exc.code == 429 or 500 <= exc.code < 600
                if not retryable or attempt >= self.retries:
                    exc.close()
                    raise ApiError(
                        f"Range 下载失败，HTTP {exc.code}: {redact_url(url)}",
                        status_code=exc.code,
                    ) from exc
                exc.close()
            except (urllib.error.URLError, TimeoutError, OSError, http.client.HTTPException) as exc:
                last_error = exc
                if attempt >= self.retries:
                    raise ApiError(
                        f"Range 下载失败: {redact_url(url)}; "
                        f"{redact_sensitive_text(str(exc), secret_values)}"
                    ) from exc
            time.sleep(min(2**attempt, 8))
        raise ApiError(
            f"Range 下载失败: {redact_url(url)}; "
            f"{redact_sensitive_text(str(last_error), secret_values)}"
        )
