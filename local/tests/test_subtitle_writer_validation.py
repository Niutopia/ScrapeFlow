"""Focused bounded-content checks for the formal subtitle sidecar writer."""

from __future__ import annotations

import posixpath
import unittest
from unittest.mock import patch

from engine.scrapeflow.subtitle_content import DEFAULT_MAX_PREFIX_BYTES
from local.scrapeflow_api.automatic_replenishment import (
    AutomaticReplenishmentError,
    AutomaticReplenishmentRuntime,
)
from local.scrapeflow_api.simple_engine_runner import (
    EngineExecutionError,
    SimplePlanExecutor,
)


VIDEO = "/library/Show/Season 01/Show.S01E01.mkv"
SOURCE = "/quark/影视/ScrapeFlow/补源/root/attempt-1/Show.S01E01.zh.srt"
TARGET = "/library/Show/Season 01/Show.S01E01.zh.srt"


def _srt(body: str) -> bytes:
    return f"1\n00:00:01,000 --> 00:00:02,000\n{body}\n".encode("utf-8")


class ContentAList:
    def __init__(self, content: bytes) -> None:
        self.files = {VIDEO: b"video", SOURCE: bytes(content)}
        self.read_limits: list[int] = []

    def read_file_prefix(self, path: str, *, max_bytes: int) -> bytes:
        self.read_limits.append(max_bytes)
        if path not in self.files:
            raise FileNotFoundError(path)
        return self.files[path][:max_bytes]

    def exact_file_info(self, path: str):
        value = self.files.get(path)
        return None if value is None else {"size": len(value)}

    def list(self, path: str, refresh: bool = False):
        del refresh
        prefix = path.rstrip("/") + "/"
        rows = []
        for full, value in self.files.items():
            if full.startswith(prefix) and "/" not in full[len(prefix):]:
                rows.append({
                    "name": full[len(prefix):], "is_dir": False,
                    "size": len(value),
                })
        return rows

    def mkdir(self, _path: str) -> None:
        return

    def move(self, source_dir: str, target_dir: str, names: list[str]) -> None:
        for name in names:
            source = posixpath.join(source_dir, name)
            target = posixpath.join(target_dir, name)
            self.files[target] = self.files.pop(source)

    def rename(self, full_path: str, new_name: str) -> None:
        parent = posixpath.dirname(full_path)
        self.files[posixpath.join(parent, new_name)] = self.files.pop(full_path)


class SubtitleWriterValidationTests(unittest.TestCase):
    def test_simplified_content_passes_and_uses_bounded_prefix(self) -> None:
        alist = ContentAList(_srt("这是一个测试字幕"))
        result = SimplePlanExecutor(alist).install_subtitle_sidecar(
            SOURCE, TARGET, expected_size=len(alist.files[SOURCE]),
            video_path=VIDEO, subtitle_language="zh",
        )
        self.assertEqual(result["status"], "moved")
        self.assertEqual(alist.read_limits, [DEFAULT_MAX_PREFIX_BYTES])
        self.assertNotIn(SOURCE, alist.files)

    def test_traditional_japanese_and_invalid_content_are_rejected(self) -> None:
        for content in (
            _srt("這是一個測試字幕"),
            _srt("これはテスト字幕です"),
            b"not a subtitle payload",
        ):
            with self.subTest(content=content):
                alist = ContentAList(content)
                with patch(
                    "local.scrapeflow_api.simple_engine_runner.time.sleep"
                ), self.assertRaises(EngineExecutionError):
                    SimplePlanExecutor(alist).install_subtitle_sidecar(
                        SOURCE, TARGET, expected_size=len(content),
                        video_path=VIDEO, subtitle_language="zh",
                    )
                self.assertIn(SOURCE, alist.files)
                self.assertNotIn(TARGET, alist.files)

    def test_runtime_preprobe_rejects_unknown_before_current_writer(self) -> None:
        """A modern custom writer cannot bypass the coordinator's gate."""
        alist = ContentAList(_srt("這是一個測試字幕"))
        calls: list[tuple[object, ...]] = []

        class ModernRunner:
            def install_subtitle_sidecar(self, *args, **kwargs):
                calls.append((args, kwargs))
                return {"status": "moved", "size": len(alist.files[SOURCE])}

        runtime = AutomaticReplenishmentRuntime.__new__(
            AutomaticReplenishmentRuntime
        )
        runtime.engine_runner = ModernRunner()
        runtime.alist = alist
        with self.assertRaises(AutomaticReplenishmentError):
            runtime._validate_subtitle_source_content(
                SOURCE, "zh",
                installer=runtime.engine_runner.install_subtitle_sidecar,
            )
        self.assertEqual(calls, [])


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
