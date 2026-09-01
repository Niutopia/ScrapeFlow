"""Strict HTTP Range regressions for AList-backed remote reads."""

from __future__ import annotations

import urllib.error
import urllib.request
import unittest
from email.message import Message
from unittest import mock

from engine.scrapeflow.clients.http import JsonHttpClient, ValidatingRedirectHandler
from engine.scrapeflow.core import AListClient
from engine.scrapeflow.errors import ApiError


class _Response:
    def __init__(
        self,
        *,
        status: int,
        body: bytes = b"",
        content_ranges: tuple[str, ...] = (),
    ) -> None:
        self.status = status
        self.body = body
        self.headers = Message()
        for value in content_ranges:
            self.headers.add_header("Content-Range", value)
        self.read_limits: list[int] = []
        self.closed = False

    def __enter__(self) -> "_Response":
        return self

    def __exit__(self, *_args: object) -> None:
        self.closed = True

    def read(self, limit: int = -1) -> bytes:
        self.read_limits.append(limit)
        return self.body if limit < 0 else self.body[:limit]


class _Opener:
    def __init__(self, *outcomes: object) -> None:
        self.outcomes = list(outcomes)
        self.calls: list[tuple[urllib.request.Request, float]] = []

    def open(self, request: urllib.request.Request, *, timeout: float):
        self.calls.append((request, timeout))
        if not self.outcomes:
            raise AssertionError("unexpected opener call")
        outcome = self.outcomes.pop(0)
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome


class JsonHttpClientExactRangeTests(unittest.TestCase):
    def _request(
        self,
        response: _Response,
        *,
        offset: int = 10,
        length: int = 4,
        expected_total: int | None = 100,
        headers: dict[str, str] | None = None,
    ) -> tuple[bytes, _Opener, mock.Mock]:
        client = JsonHttpClient(timeout=7, retries=0)
        opener = _Opener(response)
        validator = mock.Mock()
        with mock.patch.object(client, "_build_opener", return_value=opener) as build:
            result = client.request_exact_range(
                "https://cdn.example.test/object?signature=opaque",
                offset=offset,
                length=length,
                expected_total=expected_total,
                headers=headers,
                url_validator=validator,
            )
        build.assert_called_once_with(validator)
        return result, opener, validator

    def test_accepts_only_exact_206_range_and_reads_length_plus_one(self) -> None:
        provider_headers = {
            "X-Provider-Grant": "opaque-grant",
            "range": "bytes=0-999",
        }
        response = _Response(
            status=206,
            body=b"data",
            content_ranges=("bytes 10-13/100",),
        )

        result, opener, validator = self._request(response, headers=provider_headers)

        self.assertEqual(result, b"data")
        self.assertEqual(provider_headers["range"], "bytes=0-999")
        self.assertEqual(response.read_limits, [5])
        self.assertTrue(response.closed)
        validator.assert_called_once_with(
            "https://cdn.example.test/object?signature=opaque"
        )
        request, timeout = opener.calls[0]
        self.assertEqual(timeout, 7)
        self.assertEqual(request.get_header("Range"), "bytes=10-13")
        self.assertEqual(request.get_header("X-provider-grant"), "opaque-grant")

    def test_uses_no_environment_proxy_and_keeps_redirect_validation(self) -> None:
        client = JsonHttpClient(timeout=3, retries=0)
        response = _Response(
            status=206,
            body=b"abcd",
            content_ranges=("bytes 0-3/4",),
        )
        opener = _Opener(response)
        validator = mock.Mock()

        with mock.patch(
            "engine.scrapeflow.clients.http.urllib.request.build_opener",
            return_value=opener,
        ) as build_opener:
            result = client.request_exact_range(
                "https://cdn.example.test/file",
                offset=0,
                length=4,
                expected_total=4,
                url_validator=validator,
            )

        self.assertEqual(result, b"abcd")
        handlers = build_opener.call_args.args
        proxy_handlers = [
            handler for handler in handlers if isinstance(handler, urllib.request.ProxyHandler)
        ]
        redirect_handlers = [
            handler for handler in handlers if isinstance(handler, ValidatingRedirectHandler)
        ]
        self.assertEqual(len(proxy_handlers), 1)
        self.assertEqual(proxy_handlers[0].proxies, {})
        self.assertEqual(len(redirect_handlers), 1)
        self.assertIs(redirect_handlers[0].validator, validator)
        validator.assert_called_once_with("https://cdn.example.test/file")

    def test_rejects_http_200_before_reading_body(self) -> None:
        response = _Response(status=200, body=b"whole-file")
        client = JsonHttpClient(retries=0)
        opener = _Opener(response)

        with mock.patch.object(client, "_build_opener", return_value=opener):
            with self.assertRaisesRegex(ApiError, "expected_status=206"):
                client.request_exact_range(
                    "https://cdn.example.test/file",
                    offset=2,
                    length=3,
                )

        self.assertEqual(response.read_limits, [])
        self.assertTrue(response.closed)

    def test_rejects_missing_duplicate_malformed_or_wrong_content_range(self) -> None:
        cases = (
            ((), "唯一 Content-Range"),
            (("bytes 10-13/100", "bytes 10-13/100"), "唯一 Content-Range"),
            (("items 10-13/100",), "无效 Content-Range"),
            (("bytes 9-12/100",), "范围不匹配"),
            (("bytes 10-14/100",), "范围不匹配"),
            (("bytes 10-13/13",), "无效 Content-Range 总大小"),
        )

        for content_ranges, message in cases:
            with self.subTest(content_ranges=content_ranges):
                response = _Response(
                    status=206,
                    body=b"data",
                    content_ranges=content_ranges,
                )
                client = JsonHttpClient(retries=0)
                opener = _Opener(response)
                with mock.patch.object(client, "_build_opener", return_value=opener):
                    with self.assertRaisesRegex(ApiError, message):
                        client.request_exact_range(
                            "https://cdn.example.test/file",
                            offset=10,
                            length=4,
                        )
                self.assertEqual(response.read_limits, [])

    def test_validates_optional_expected_total(self) -> None:
        mismatch = _Response(
            status=206,
            body=b"data",
            content_ranges=("bytes 10-13/101",),
        )
        client = JsonHttpClient(retries=0)
        with mock.patch.object(client, "_build_opener", return_value=_Opener(mismatch)):
            with self.assertRaisesRegex(ApiError, "总大小不匹配"):
                client.request_exact_range(
                    "https://cdn.example.test/file",
                    offset=10,
                    length=4,
                    expected_total=100,
                )
        self.assertEqual(mismatch.read_limits, [])

        wildcard = _Response(
            status=206,
            body=b"data",
            content_ranges=("bytes 10-13/*",),
        )
        with mock.patch.object(client, "_build_opener", return_value=_Opener(wildcard)):
            self.assertEqual(
                client.request_exact_range(
                    "https://cdn.example.test/file",
                    offset=10,
                    length=4,
                ),
                b"data",
            )

        unverifiable = _Response(
            status=206,
            body=b"data",
            content_ranges=("bytes 10-13/*",),
        )
        with mock.patch.object(
            client, "_build_opener", return_value=_Opener(unverifiable)
        ):
            with self.assertRaisesRegex(ApiError, "未返回可验证"):
                client.request_exact_range(
                    "https://cdn.example.test/file",
                    offset=10,
                    length=4,
                    expected_total=100,
                )
        self.assertEqual(unverifiable.read_limits, [])

    def test_rejects_short_and_overlong_bodies_with_one_bounded_read(self) -> None:
        for body, actual in ((b"abc", 3), (b"abcde-more", 5)):
            with self.subTest(body=body):
                response = _Response(
                    status=206,
                    body=body,
                    content_ranges=("bytes 10-13/100",),
                )
                client = JsonHttpClient(retries=0)
                with mock.patch.object(
                    client, "_build_opener", return_value=_Opener(response)
                ):
                    with self.assertRaisesRegex(
                        ApiError,
                        rf"expected=4, actual={actual}",
                    ):
                        client.request_exact_range(
                            "https://cdn.example.test/file",
                            offset=10,
                            length=4,
                            expected_total=100,
                        )
                self.assertEqual(response.read_limits, [5])

    def test_rejects_invalid_arguments_before_opening_transport(self) -> None:
        client = JsonHttpClient(retries=0)
        invalid = (
            {"offset": -1, "length": 1},
            {"offset": True, "length": 1},
            {"offset": 0, "length": 0},
            {"offset": 0, "length": True},
            {"offset": 0, "length": 1, "expected_total": 0},
            {"offset": 9, "length": 2, "expected_total": 10},
        )
        with mock.patch.object(client, "_build_opener") as build:
            for kwargs in invalid:
                with self.subTest(kwargs=kwargs), self.assertRaises(ValueError):
                    client.request_exact_range(
                        "https://cdn.example.test/file",
                        **kwargs,
                    )
        build.assert_not_called()

    def test_transport_error_redacts_opaque_url_and_header_secrets(self) -> None:
        url_secret = "query-secret-123"
        header_secret = "header-secret-456"
        client = JsonHttpClient(retries=0)
        opener = _Opener(
            urllib.error.URLError(f"failed {url_secret} {header_secret}")
        )
        with mock.patch.object(client, "_build_opener", return_value=opener):
            with self.assertRaises(ApiError) as raised:
                client.request_exact_range(
                    f"https://cdn.example.test/file?opaque={url_secret}",
                    offset=0,
                    length=1,
                    headers={"X-Opaque-Grant": header_secret},
                )

        message = str(raised.exception)
        self.assertNotIn(url_secret, message)
        self.assertNotIn(header_secret, message)
        self.assertIn("<redacted>", message)


class AListClientExactRangeTests(unittest.TestCase):
    def setUp(self) -> None:
        self.client = AListClient(
            "http://localhost:5244",
            "user",
            "password",
            allow_insecure_http=True,
        )

    def test_public_read_file_range_encapsulates_link_and_headers(self) -> None:
        raw_url = "https://cdn.example.test/object?signature=secret"
        provider_headers = {"X-Provider-Grant": "opaque"}
        exact_http = mock.Mock()
        exact_http.request_exact_range.return_value = b"payload"
        self.client.http = exact_http

        with mock.patch.object(
            self.client,
            "file_link",
            return_value=(raw_url, provider_headers),
        ) as file_link:
            result = self.client.read_file_range(
                "/quark/影视/待刮削/disc.iso",
                4096,
                7,
                expected_size=8192,
                refresh=False,
            )

        self.assertEqual(result, b"payload")
        file_link.assert_called_once_with(
            "/quark/影视/待刮削/disc.iso",
            refresh=False,
        )
        exact_http.request_exact_range.assert_called_once_with(
            raw_url,
            offset=4096,
            length=7,
            headers=provider_headers,
            expected_total=8192,
            url_validator=self.client._validate_download_url,
        )
        self.assertEqual(provider_headers, {"X-Provider-Grant": "opaque"})

    def test_invalid_range_is_rejected_before_requesting_a_file_link(self) -> None:
        with mock.patch.object(self.client, "file_link") as file_link:
            with self.assertRaises(ValueError):
                self.client.read_file_range("/disc.iso", 9, 2, expected_size=10)
        file_link.assert_not_called()


if __name__ == "__main__":
    unittest.main()
