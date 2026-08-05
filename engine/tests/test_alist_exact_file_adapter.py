from __future__ import annotations

import io
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from engine import scraper
from engine.scrapeflow.alist_exact_file_adapter import AListExactFileAdapter


class Backend:
    def __init__(self) -> None:
        self.info = {"size": 3, "sha256": None, "version": "v1"}
        self.upload_calls = 0
        self.removed: list[tuple[str, list[str]]] = []
        self.directories: list[str] = []

    def exact_file_info(self, path: str):
        return dict(self.info) if path == "/source/file.mkv" else None

    def open_file_reader(self, path: str):
        return io.BytesIO(b"abc")

    def upload_file(self, target_path: str, source: Path, content_type: str) -> None:
        self.upload_calls += 1
        raise RuntimeError("ambiguous upload response")

    def remove(self, parent: str, names: list[str]) -> None:
        self.removed.append((parent, names))

    def mkdir(self, path: str) -> None:
        self.directories.append(path)


class AListExactFileAdapterTests(unittest.TestCase):
    def test_alist_exact_stat_uses_fs_get_and_extracts_sha256(self):
        client = scraper.AListClient("https://example.invalid", "admin", "")
        digest = "a" * 64
        client.call = mock.Mock(return_value={
            "code": 200,
            "data": {
                "name": "file.mkv",
                "size": 3,
                "is_dir": False,
                "modified": "2026-08-04T00:00:00Z",
                "hash_info": {"sha256": digest},
            },
        })

        result = client.exact_file_info("/source/file.mkv")

        self.assertEqual(result, {
            "size": 3,
            "sha256": digest,
            "version": "2026-08-04T00:00:00Z",
        })
        self.assertEqual(client.call.call_args.args[0], "get")
        self.assertEqual(client.call.call_args.args[1]["path"], "/source/file.mkv")
        self.assertFalse(client.call.call_args.kwargs["retryable"])

    def test_adapter_upload_is_called_once_and_never_generates_a_retry(self):
        backend = Backend()
        adapter = AListExactFileAdapter(backend)
        with self.assertRaisesRegex(RuntimeError, "ambiguous"):
            adapter.upload_file_once(
                "/library/file.mkv", Path("unused"), "video/x-matroska",
            )
        self.assertEqual(backend.upload_calls, 1)

    def test_adapter_remove_targets_one_exact_basename(self):
        backend = Backend()
        AListExactFileAdapter(backend).remove_file("/source/file.mkv")
        self.assertEqual(backend.removed, [("/source", ["file.mkv"])])

    def test_adapter_ensures_exact_remote_directory(self):
        backend = Backend()
        adapter = AListExactFileAdapter(backend)
        adapter.ensure_directory(
            "/quark/影视/ScrapeFlow/事务回滚/work-42/items/episode-01"
        )
        self.assertEqual(backend.directories, [
            "/quark/影视/ScrapeFlow/事务回滚/work-42/items/episode-01"
        ])

    def test_adapter_rejects_non_normalized_directory_before_backend_call(self):
        backend = Backend()
        adapter = AListExactFileAdapter(backend)
        for path in ("relative/path", "/safe//wrong", "/safe/../wrong"):
            with self.subTest(path=path), self.assertRaises(ValueError):
                adapter.ensure_directory(path)
        self.assertEqual(backend.directories, [])

    def test_alist_stream_upload_is_create_only(self):
        class Response:
            status = 200

            @staticmethod
            def read(_limit: int) -> bytes:
                return b'{"code":200,"message":"success"}'

        class Connection:
            def __init__(self) -> None:
                self.headers: dict[str, str] = {}
                self.sent = bytearray()

            def putrequest(self, method: str, endpoint: str) -> None:
                self.method = method
                self.endpoint = endpoint

            def putheader(self, key: str, value: str) -> None:
                self.headers[key] = value

            def endheaders(self) -> None:
                return None

            def send(self, chunk: bytes) -> None:
                self.sent.extend(chunk)

            @staticmethod
            def getresponse() -> Response:
                return Response()

            def close(self) -> None:
                return None

        connection = Connection()
        client = scraper.AListClient("https://example.invalid", "admin", "")
        client.token = "test-token"
        with tempfile.TemporaryDirectory() as raw_root:
            source = Path(raw_root) / "payload.bin"
            source.write_bytes(b"abc")
            with mock.patch.object(
                scraper.http.client,
                "HTTPSConnection",
                return_value=connection,
            ):
                client.upload_file("/library/file.mkv", source)

        self.assertEqual(connection.method, "PUT")
        self.assertEqual(connection.endpoint, "/api/fs/put")
        self.assertEqual(connection.headers["Overwrite"], "false")
        self.assertEqual(bytes(connection.sent), b"abc")

    def test_alist_byte_upload_is_create_only_unless_explicitly_overwriting(self):
        client = scraper.AListClient("https://example.invalid", "admin", "")
        client.token = "test-token"
        client._request_json_authenticated = mock.Mock(  # type: ignore[method-assign]
            return_value={"code": 200, "message": "success"},
        )

        client.upload_bytes("/library/poster.jpg", b"first", "image/jpeg")
        first = client._request_json_authenticated.call_args
        self.assertEqual(first.kwargs["headers"]["Overwrite"], "false")
        self.assertFalse(first.kwargs["retryable"])

        client.upload_bytes(
            "/library/poster.jpg",
            b"replacement",
            "image/jpeg",
            overwrite=True,
        )
        second = client._request_json_authenticated.call_args
        self.assertEqual(second.kwargs["headers"]["Overwrite"], "true")


if __name__ == "__main__":
    unittest.main()
