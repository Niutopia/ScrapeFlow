"""Zero-local-disk AList upload regressions."""

from __future__ import annotations

import json
import unittest
from unittest import mock

from engine.scrapeflow.core import AListClient
from engine.scrapeflow.errors import ApiError


class _Response:
    status = 200

    def __init__(self, payload: dict[str, object] | None = None) -> None:
        self.payload = payload or {"code": 200, "message": "success", "data": None}

    def read(self, _limit: int = -1) -> bytes:
        return json.dumps(self.payload).encode("utf-8")


class _Connection:
    def __init__(self, response: _Response | None = None) -> None:
        self.response = response or _Response()
        self.request: tuple[str, str] | None = None
        self.headers: dict[str, str] = {}
        self.body = bytearray()
        self.ended = False
        self.response_requested = False
        self.closed = False

    def putrequest(self, method: str, endpoint: str) -> None:
        self.request = (method, endpoint)

    def putheader(self, key: str, value: str) -> None:
        self.headers[key] = value

    def endheaders(self) -> None:
        self.ended = True

    def send(self, chunk: bytes) -> None:
        self.body.extend(chunk)

    def getresponse(self) -> _Response:
        self.response_requested = True
        return self.response

    def close(self) -> None:
        self.closed = True


class AListStreamUploadTests(unittest.TestCase):
    def _client(self) -> AListClient:
        client = AListClient(
            "http://127.0.0.1:5244",
            "user",
            "password",
            retries=0,
            allow_insecure_http=True,
        )
        client.token = "opaque-token"
        return client

    def test_streams_exact_body_with_hash_headers_and_create_only_semantics(self) -> None:
        connection = _Connection()
        client = self._client()
        with mock.patch(
            "engine.scrapeflow.core.http.client.HTTPConnection",
            return_value=connection,
        ) as constructor:
            client.upload_stream(
                "/quark/影视/ScrapeFlow/展开/episode.m2ts",
                (chunk for chunk in (b"ab", bytearray(b"cd"), memoryview(b"ef"))),
                size=6,
                md5="e80b5017098950fc58aad83c8c14978e",
                sha1="1f8ac10f23c5b5bc1167bda84b833e5c057a77d2",
                content_type="video/mp2t",
            )

        constructor.assert_called_once_with("127.0.0.1", 5244, timeout=300)
        self.assertEqual(connection.request, ("PUT", "/api/fs/put"))
        self.assertTrue(connection.ended)
        self.assertTrue(connection.response_requested)
        self.assertTrue(connection.closed)
        self.assertEqual(bytes(connection.body), b"abcdef")
        self.assertEqual(connection.headers["Content-Length"], "6")
        self.assertEqual(connection.headers["Overwrite"], "false")
        self.assertEqual(connection.headers["Content-Type"], "video/mp2t")
        self.assertEqual(
            connection.headers["X-File-Md5"],
            "e80b5017098950fc58aad83c8c14978e",
        )
        self.assertEqual(
            connection.headers["X-File-Sha1"],
            "1f8ac10f23c5b5bc1167bda84b833e5c057a77d2",
        )

    def test_rejects_short_or_overlong_iterables_without_accepting_response(self) -> None:
        cases = (
            ([b"abc"], 4, "大小不足"),
            ([b"abcde"], 4, "超过计划大小"),
        )
        for chunks, size, message in cases:
            with self.subTest(chunks=chunks):
                connection = _Connection()
                client = self._client()
                with mock.patch(
                    "engine.scrapeflow.core.http.client.HTTPConnection",
                    return_value=connection,
                ):
                    with self.assertRaisesRegex(ApiError, message):
                        client.upload_stream(
                            "/quark/staging/file.bin",
                            chunks,
                            size=size,
                            md5="0" * 32,
                            sha1="1" * 40,
                        )
                self.assertFalse(connection.response_requested)
                self.assertTrue(connection.closed)

    def test_validates_size_hashes_and_content_type_before_opening_transport(self) -> None:
        client = self._client()
        invalid = (
            {"size": 0, "md5": "0" * 32, "sha1": "1" * 40},
            {"size": 1, "md5": "bad", "sha1": "1" * 40},
            {"size": 1, "md5": "0" * 32, "sha1": "bad"},
            {
                "size": 1,
                "md5": "0" * 32,
                "sha1": "1" * 40,
                "content_type": "",
            },
        )
        with mock.patch("engine.scrapeflow.core.http.client.HTTPConnection") as ctor:
            for kwargs in invalid:
                with self.subTest(kwargs=kwargs), self.assertRaises(ValueError):
                    client.upload_stream("/quark/staging/file.bin", [b"x"], **kwargs)
        ctor.assert_not_called()


if __name__ == "__main__":
    unittest.main()
