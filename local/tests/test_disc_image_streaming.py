"""Remote ISO member streaming regressions."""

from __future__ import annotations

import contextlib
import hashlib
import unittest

from engine.scrapeflow.disc_image import (
    DiscImageError,
    InnerFile,
    digest_inner_file,
    stream_inner_file_via_alist,
)


class _FakeAList:
    def __init__(self, image_path: str, image: bytes) -> None:
        self.image_path = image_path
        self.image = image
        self.source_version = "v1"
        self.targets: dict[str, bytes] = {}
        self.range_reads: list[tuple[int, int]] = []
        self.upload_headers: dict[str, object] = {}

    def exact_file_info(self, path: str):
        if path == self.image_path:
            return {"size": len(self.image), "version": self.source_version}
        if path in self.targets:
            return {"size": len(self.targets[path]), "version": "uploaded-v1"}
        return None

    @contextlib.contextmanager
    def open_file_range_reader(self, path: str, *, expected_size: int, refresh: bool):
        assert path == self.image_path
        assert expected_size == len(self.image)
        assert refresh is True

        def read_range(offset: int, length: int) -> bytes:
            self.range_reads.append((offset, length))
            return self.image[offset : offset + length]

        yield read_range

    def upload_stream(
        self,
        target_path: str,
        chunks,
        *,
        size: int,
        md5: str,
        sha1: str,
        content_type: str,
    ) -> None:
        body = b"".join(chunks)
        assert len(body) == size
        assert hashlib.md5(body, usedforsecurity=False).hexdigest() == md5
        assert hashlib.sha1(body, usedforsecurity=False).hexdigest() == sha1
        self.upload_headers = {
            "size": size,
            "md5": md5,
            "sha1": sha1,
            "content_type": content_type,
        }
        self.targets[target_path] = body


class DiscImageStreamingTests(unittest.TestCase):
    def setUp(self) -> None:
        self.image_path = "/quark/intake/disc.iso"
        self.target_path = "/quark/staging/S01E01.m2ts"
        sectors = [bytes([value]) * 2048 for value in range(6)]
        self.image = b"".join(sectors)
        self.inner = InnerFile(
            inner_path="BDMV/STREAM/00005.m2ts",
            size=3000,
            extents=((1, 1), (4, 1)),
        )
        self.expected = self.image[2048:4096] + self.image[8192 : 8192 + 952]

    def test_two_pass_transfer_never_materialises_a_local_file(self) -> None:
        alist = _FakeAList(self.image_path, self.image)

        result = stream_inner_file_via_alist(
            alist,
            image_path=self.image_path,
            inner_file=self.inner,
            target_path=self.target_path,
            chunk_bytes=1024,
        )

        self.assertEqual(alist.targets[self.target_path], self.expected)
        self.assertEqual(result.size, len(self.expected))
        self.assertEqual(
            result.md5,
            hashlib.md5(self.expected, usedforsecurity=False).hexdigest(),
        )
        self.assertEqual(
            result.sha1,
            hashlib.sha1(self.expected, usedforsecurity=False).hexdigest(),
        )
        self.assertEqual(alist.upload_headers["content_type"], "video/mp2t")
        # Three exact ranges per pass: two chunks for the full first extent and
        # one useful partial chunk for the second extent.
        self.assertEqual(len(alist.range_reads), 6)

    def test_digest_rejects_a_short_exact_range(self) -> None:
        def short_read(_offset: int, length: int) -> bytes:
            return b"x" * (length - 1)

        with self.assertRaisesRegex(DiscImageError, "短读"):
            digest_inner_file(short_read, self.inner, image_size=len(self.image))

    def test_refuses_an_existing_staging_target(self) -> None:
        alist = _FakeAList(self.image_path, self.image)
        alist.targets[self.target_path] = b"occupied"

        with self.assertRaisesRegex(DiscImageError, "目标已存在"):
            stream_inner_file_via_alist(
                alist,
                image_path=self.image_path,
                inner_file=self.inner,
                target_path=self.target_path,
                chunk_bytes=1024,
            )
        self.assertEqual(alist.range_reads, [])


if __name__ == "__main__":
    unittest.main()
