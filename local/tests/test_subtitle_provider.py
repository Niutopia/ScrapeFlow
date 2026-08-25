"""Focused regression tests for the active RootJob subtitle provider."""

from __future__ import annotations

import posixpath
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock

from engine.tools.replenishment_adapter import (
    SubtitleDiscoveryService,
    SubtitleMaterializer,
    SubtitlePauseRequested,
)
from engine.tools.replenishment_adapter.subtitle_provider import score_subtitle_candidate


class _AList:
    def __init__(self) -> None:
        self.files: dict[str, bytes] = {}
        self.tree: dict[str, list[dict[str, object]]] = {
            "/quark/影视/ScrapeFlow/补源": [],
        }

    def list(self, path: str, refresh: bool = False) -> list[dict[str, object]]:
        del refresh
        return [dict(row) for row in self.tree.get(path, [])]

    def mkdir(self, path: str) -> None:
        if path in self.tree:
            return
        parent, name = posixpath.dirname(path) or "/", posixpath.basename(path)
        self.tree.setdefault(parent, [])
        if not any(row.get("name") == name for row in self.tree[parent]):
            self.tree[parent].append({"name": name, "is_dir": True})
        self.tree[path] = []

    def upload_bytes(self, remote_dir: str, name: str, data: bytes) -> None:
        path = f"{remote_dir.rstrip('/')}/{name}"
        self.files[path] = data
        self.tree.setdefault(remote_dir.rstrip("/"), []).append({
            "name": name, "is_dir": False, "size": len(data),
        })

    def exact_file_info(self, path: str) -> dict[str, object] | None:
        if path not in self.files:
            return None
        return {
            "name": posixpath.basename(path),
            "size": len(self.files[path]),
            "is_dir": False,
        }

    def read_file_prefix(self, path: str, *, max_bytes: int = 1024 * 1024) -> bytes:
        return self.files.get(path, b"")[:max_bytes]


def _gap() -> dict[str, object]:
    return {
        "id": "missing_subtitle:show-s01e01",
        "kind": "missing_subtitle",
        "path": "/library/Show/Show.S01E01.mkv",
        "season": 1,
        "episode": 1,
        "subtitle_language": "zh",
        "media": {"title": "Show", "tmdb_id": 123},
    }


class SubtitleProviderTests(unittest.TestCase):
    @staticmethod
    def _srt(*rows: str) -> bytes:
        blocks = []
        for index, row in enumerate(rows, 1):
            start = index
            blocks.append(
                f"{index}\n00:00:{start:02d},000 --> 00:00:{start + 2:02d},000\n{row}\n"
            )
        return ("\n".join(blocks) + "\n").encode("utf-8")

    def test_discovery_disabled_has_no_network_work(self) -> None:
        self.assertEqual(
            SubtitleDiscoveryService(enabled=False).search_gap(_gap(), {}),
            [],
        )

    def test_exact_episode_and_simplified_chinese_score_highest(self) -> None:
        gap = {
            **_gap(),
            "id": "missing_subtitle:123:Show.S01E02.mkv",
            "path": "/library/Show/[VCB-Studio] Show - 02 [1080p].mkv",
            "season": 1,
            "episode": 2,
        }
        request = {"media": {"title": "Show", "tmdb_id": 123}}
        exact = {
            "provider": "assrt",
            "title": "[VCB-Studio] Show - 02 [简中特效 ASS]",
            "format": "ass",
            "downloads": 500,
        }
        wrong_episode = {
            "provider": "assrt",
            "title": "Show S01E05 [简中]",
            "format": "ass",
            "downloads": 5000,
        }
        self.assertGreater(
            score_subtitle_candidate(exact, gap, request),
            score_subtitle_candidate(wrong_episode, gap, request),
        )

    def test_bad_candidate_does_not_block_verified_sidecar(self) -> None:
        chinese = (
            "1\n00:00:01,000 --> 00:00:04,000\n"
            "你好，这是简体中文测试字幕。\n"
        ).encode("utf-8")
        discovery = MagicMock()
        discovery.search_gap.return_value = [
            {
                "provider": "assrt",
                "url": "https://example.test/Show.S01E01.bad.srt",
                "direct_file": True,
                "format": "srt",
                "title": "Show S01E01 中文字幕",
            },
            {
                "provider": "assrt",
                "url": "https://example.test/Show.S01E01.good.srt",
                "direct_file": True,
                "format": "srt",
                "title": "Show S01E01 中文字幕",
            },
        ]
        materializer = SubtitleMaterializer(
            discovery=discovery,
            downloader=lambda url: (
                b"1\n00:00:01,000 --> 00:00:04,000\nEnglish only.\n"
                if "bad" in url else chinese
            ),
        )
        alist = _AList()
        with tempfile.TemporaryDirectory() as directory:
            result = materializer.acquire_subtitles(
                {"tmdb_id": 123},
                [_gap()],
                staging_root="/quark/影视/ScrapeFlow/补源/root/attempt",
                workspace=Path(directory),
                alist=alist,
            )
        self.assertEqual(len(result["files"]), 1)
        self.assertIn(
            "/quark/影视/ScrapeFlow/补源/root/attempt/Show.S01E01.zh-CN.srt",
            alist.files,
        )

    def test_provider_keeps_searching_for_bilingual_before_sc_fallback(self) -> None:
        chinese_first = self._srt(
            "你好，这是第一份简体中文字幕测试。",
            "请继续观看这一集的内容测试。",
        )
        chinese_second = self._srt(
            "你好，这是第二份简体中文字幕测试。",
            "请继续观看这一集的内容测试。",
        )
        original_bad = self._srt("こんにちは、かな字幕です。")
        original_good = self._srt(
            "こんにちは、これはかな字幕です。",
            "つぎのばめんをごらんください。",
        )
        first_chinese = {
            "provider": "assrt",
            "url": "https://example.test/Show.S01E01.first.srt",
            "direct_file": True,
            "format": "srt",
            "title": "Show S01E01 简体中文",
        }
        second_chinese = {
            "provider": "subhd",
            "url": "https://example.test/Show.S01E01.second.srt",
            "direct_file": True,
            "format": "srt",
            "title": "Show S01E01 简体中文",
        }
        original_candidate = {
            "provider": "assrt",
            "url": "https://example.test/Show.S01E01.original.srt",
            "direct_file": True,
            "format": "srt",
            "title": "Show S01E01 日本語",
            "language": "japanese",
        }
        discovery = MagicMock()
        # Initial Chinese search, then one failed original search for the
        # first SC candidate, then a successful original search for the next.
        discovery.search_gap.side_effect = [
            [first_chinese, second_chinese],
            [original_candidate],
            [original_candidate],
        ]
        downloads = {
            first_chinese["url"]: chinese_first,
            second_chinese["url"]: chinese_second,
            original_candidate["url"]: original_good,
        }
        # The first original attempt deliberately has a mismatched cue shape;
        # the second call gets aligned Japanese bytes.
        original_calls = {"count": 0}

        def download(url: str) -> bytes:
            if url == original_candidate["url"]:
                original_calls["count"] += 1
                return original_bad if original_calls["count"] == 1 else original_good
            return downloads[url]

        request = {
            "media": {
                "title": "Show",
                "tmdb_id": 123,
                "original_language": "ja",
                "original_language_verified_by_tmdb": True,
            }
        }
        alist = _AList()
        with tempfile.TemporaryDirectory() as directory:
            result = SubtitleMaterializer(
                discovery=discovery,
                downloader=download,
            ).acquire_subtitles(
                request,
                [_gap()],
                staging_root="/quark/影视/ScrapeFlow/补源/root/attempt",
                workspace=Path(directory),
                alist=alist,
            )
        self.assertEqual(len(result["files"]), 1)
        self.assertEqual(original_calls["count"], 2)
        bilingual_paths = [
            path for path in alist.files
            if "zh-CN-bilingual-ja" in path
        ]
        self.assertEqual(len(bilingual_paths), 1)
        self.assertIn("こんにちは", alist.files[bilingual_paths[0]].decode("utf-8"))

    def test_provider_falls_back_from_sc_to_tc(self) -> None:
        wrong_sc = self._srt(
            "This is an English-only subtitle candidate.",
            "The language lane is intentionally wrong.",
        )
        traditional = self._srt(
            "這是繁體中文字幕測試內容。",
            "請繼續觀看這一集的內容。",
        )
        sc_candidate = {
            "provider": "assrt",
            "url": "https://example.test/Show.S01E01.sc.srt",
            "direct_file": True,
            "format": "srt",
            "title": "Show S01E01 简体中文",
        }
        tc_candidate = {
            "provider": "subhd",
            "url": "https://example.test/Show.S01E01.tc.srt",
            "direct_file": True,
            "format": "srt",
            "title": "Show S01E01 繁體中文",
        }
        discovery = MagicMock()
        discovery.search_gap.return_value = [sc_candidate, tc_candidate]
        alist = _AList()
        with tempfile.TemporaryDirectory() as directory:
            result = SubtitleMaterializer(
                discovery=discovery,
                downloader=lambda url: wrong_sc if url == sc_candidate["url"] else traditional,
            ).acquire_subtitles(
                {"media": {"title": "Show", "tmdb_id": 123}},
                [_gap()],
                staging_root="/quark/影视/ScrapeFlow/补源/root/attempt",
                workspace=Path(directory),
                alist=alist,
            )
        self.assertEqual(len(result["files"]), 1)
        self.assertIn(
            "/quark/影视/ScrapeFlow/补源/root/attempt/Show.S01E01.zh-TW.srt",
            alist.files,
        )

    def test_provider_accepts_one_file_tmdb_verified_bilingual_candidate(self) -> None:
        bilingual = self._srt(
            "这是简体中文字幕。\nこんにちは、かな字幕です。",
            "我们继续测试。\nつぎのばめんです。",
        )
        candidate = {
            "provider": "assrt",
            "url": "https://example.test/Show.S01E01.bilingual.srt",
            "direct_file": True,
            "format": "srt",
            "title": "Show S01E01 简日双语",
        }
        discovery = MagicMock()
        discovery.search_gap.return_value = [candidate]
        alist = _AList()
        with tempfile.TemporaryDirectory() as directory:
            result = SubtitleMaterializer(
                discovery=discovery,
                downloader=lambda _url: bilingual,
            ).acquire_subtitles(
                {
                    "media": {
                        "title": "Show",
                        "tmdb_id": 123,
                        "original_language": "ja",
                        "original_language_verified_by_tmdb": True,
                    }
                },
                [_gap()],
                staging_root="/quark/影视/ScrapeFlow/补源/root/attempt",
                workspace=Path(directory),
                alist=alist,
            )
        self.assertEqual(len(result["files"]), 1)
        self.assertTrue(result["files"][0]["bilingual"])
        self.assertIn(
            "/quark/影视/ScrapeFlow/补源/root/attempt/Show.S01E01.zh-CN-bilingual-ja.srt",
            alist.files,
        )

    def test_season_pack_is_never_downloaded_for_one_missing_sidecar(self) -> None:
        downloads: list[str] = []
        discovery = MagicMock()
        discovery.search_gap.return_value = [{
            "provider": "assrt",
            "url": "https://example.test/show-season.zip",
            "direct_file": True,
            "format": "srt",
            "title": "Show S01 完整字幕包",
        }]
        alist = _AList()
        with tempfile.TemporaryDirectory() as directory:
            result = SubtitleMaterializer(
                discovery=discovery,
                downloader=lambda url: downloads.append(url) or b"unreachable",
            ).acquire_subtitles(
                {"media": {"title": "Show"}},
                [_gap()],
                staging_root="/quark/影视/ScrapeFlow/补源/root/attempt",
                workspace=Path(directory),
                alist=alist,
            )
        self.assertEqual(result["files"], [])
        self.assertEqual(downloads, [])
        self.assertEqual(alist.files, {})

    def test_pause_after_download_prevents_any_staging_write(self) -> None:
        paused = {"value": False}
        discovery = MagicMock()
        discovery.search_gap.return_value = [{
            "provider": "assrt",
            "url": "https://example.test/Show.S01E01.zh.srt",
            "direct_file": True,
            "format": "srt",
            "title": "Show S01E01 中文字幕",
        }]

        def download(_url: str) -> bytes:
            paused["value"] = True
            return (
                "1\n00:00:01,000 --> 00:00:04,000\n"
                "字幕下载后立即撤销试运行范围。\n"
            ).encode("utf-8")

        alist = _AList()
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaises(SubtitlePauseRequested):
                SubtitleMaterializer(discovery=discovery, downloader=download).acquire_subtitles(
                    {"media": {"title": "Show"}},
                    [_gap()],
                    staging_root="/quark/影视/ScrapeFlow/补源/root/attempt",
                    workspace=Path(directory),
                    alist=alist,
                    pause_requested=lambda: paused["value"],
                )
        self.assertEqual(alist.files, {})


if __name__ == "__main__":
    unittest.main()
