"""Unit tests for dedicated Multi-Source Subtitle Provider and weighted ranking."""

from __future__ import annotations

import json
from pathlib import Path
import posixpath
import tempfile
import unittest
from unittest.mock import MagicMock

from engine.tools.replenishment_adapter import (
    PROVIDER_SUBTITLE_A4K,
    PROVIDER_SUBTITLE_ANIMETOSHO,
    PROVIDER_SUBTITLE_ASSRT,
    PROVIDER_SUBTITLE_OPENSUBTITLES,
    PROVIDER_SUBTITLE_SUBDOG,
    PROVIDER_SUBTITLE_SUBHD,
    PROVIDER_SUBTITLE_ZIMUKU,
    SubtitleDiscoveryService,
    SubtitleInfrastructureError,
    SubtitleMaterializer,
    SubtitleProviderError,
)

from engine.tools.replenishment_adapter.subtitle_provider import (
    score_subtitle_candidate,
)
from local.scrapeflow_api.automatic_replenishment import (
    AutomaticReplenishmentRuntime,
)
from local.scrapeflow_api.simple_engine_runner import EngineJob


class MockAList:
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
        parent = posixpath.dirname(path) or "/"
        name = posixpath.basename(path)
        self.tree.setdefault(parent, [])
        if not any(row.get("name") == name for row in self.tree[parent]):
            self.tree[parent].append({"name": name, "is_dir": True})
        self.tree[path] = []

    def remove(self, parent: str, names: list[str]) -> None:
        for name in names:
            self.tree[parent] = [row for row in self.tree.get(parent, []) if row.get("name") != name]
            child = posixpath.join(parent, name)
            self.tree.pop(child, None)

    def remove_empty_dir(self, path: str) -> bool:
        if self.tree.get(path):
            return False
        parent = posixpath.dirname(path) or "/"
        name = posixpath.basename(path)
        self.remove(parent, [name])
        return True

    def upload_bytes(self, remote_dir: str, name: str, data: bytes) -> None:
        path = f"{remote_dir.rstrip('/')}/{name}"
        self.files[path] = data
        parent = remote_dir.rstrip('/')
        self.tree.setdefault(parent, [])
        self.tree[parent].append({"name": name, "is_dir": False, "size": len(data)})

    def upload_file(self, remote_dir: str, local_path: str, name: str | None = None) -> None:
        filename = name or Path(local_path).name
        path = f"{remote_dir.rstrip('/')}/{filename}"
        data = Path(local_path).read_bytes()
        self.files[path] = data
        parent = remote_dir.rstrip('/')
        self.tree.setdefault(parent, [])
        self.tree[parent].append({"name": filename, "is_dir": False, "size": len(data)})

    def exact_file_info(self, path: str) -> dict[str, object] | None:
        if path in self.files:
            return {"name": posixpath.basename(path), "size": len(self.files[path]), "is_dir": False}
        return None

    def read_file_prefix(self, path: str, *, max_bytes: int = 1024 * 1024) -> bytes:
        if path in self.files:
            return self.files[path][:max_bytes]
        return b""


class SubtitleProviderTests(unittest.TestCase):
    def test_discovery_disabled(self) -> None:
        service = SubtitleDiscoveryService(enabled=False)
        gap = {"kind": "missing_subtitle", "media": {"title": "Test Show"}}
        self.assertEqual(service.search_gap(gap, {}), [])

    def test_weighted_scoring_criteria(self) -> None:
        gap = {
            "id": "missing_subtitle:123:Show.S01E02.mkv",
            "kind": "missing_subtitle",
            "path": "/library/番剧/Show/[VCB-Studio] Show - 02 [1080p].mkv",
            "season": 1,
            "episode": 2,
            "subtitle_language": "zh",
        }
        req = {"media": {"title": "Show", "tmdb_id": 123}}

        # Candidate A: perfect match (.ass, S01E02, VCB-Studio, Simplified Chinese)
        cand_a = {
            "provider": PROVIDER_SUBTITLE_ASSRT,
            "title": "[VCB-Studio] Show - 02 [简中特效 ASS]",
            "format": "ass",
            "downloads": 500,
        }
        # Candidate B: generic SRT without fansub match
        cand_b = {
            "provider": PROVIDER_SUBTITLE_ASSRT,
            "title": "Show S01E02.srt",
            "format": "srt",
            "downloads": 10,
        }
        # Candidate C: wrong episode (S01E05)
        cand_c = {
            "provider": PROVIDER_SUBTITLE_ASSRT,
            "title": "Show S01E05 [简中].ass",
            "format": "ass",
            "downloads": 1000,
        }

        score_a = score_subtitle_candidate(cand_a, gap, req)
        score_b = score_subtitle_candidate(cand_b, gap, req)
        score_c = score_subtitle_candidate(cand_c, gap, req)

        self.assertGreater(score_a, score_b, "ASS + Fansub match + CHS must score higher than generic SRT")
        self.assertGreater(score_b, score_c, "Correct episode must score higher than wrong episode")

    def test_discovery_and_ranking_order(self) -> None:
        assrt_resp = json.dumps({
            "data": {
                "subs": [
                    {"id": 1, "url": "http://sub1.com", "native_name": "Show - 01 [繁体]", "format": "srt", "download_count": 10},
                    {"id": 2, "url": "http://sub2.com", "native_name": "[Kamigami] Show - 01 [简中特效]", "format": "ass", "download_count": 500},
                ]
            }
        }).encode("utf-8")

        service = SubtitleDiscoveryService(
            enabled=True,
            fetcher=lambda url, headers: assrt_resp,
        )
        gap = {
            "id": "missing_subtitle:123:Show.S01E01.mkv",
            "kind": "missing_subtitle",
            "path": "/library/番剧/Show/[Kamigami] Show S01E01.mkv",
            "media": {"title": "Show", "tmdb_id": 123},
            "season": 1,
            "episode": 1,
            "subtitle_language": "zh",
        }
        results = service.search_gap(gap, {})
        self.assertEqual(len(results), 2)
        # Winner must be candidate #2 because of Kamigami match + ASS + Simplified Chinese
        self.assertEqual(results[0]["url"], "http://sub2.com")
        self.assertIn("Kamigami", results[0]["title"])

    def test_materializer_failover_to_second_candidate_on_content_failure(self) -> None:
        # Candidate 1 has invalid (English only / non-matching) text
        cand1_bytes = b"1\n00:00:01,000 --> 00:00:04,000\nHello English only subtitle.\n"
        # Candidate 2 has valid Simplified Chinese
        cand2_bytes = "1\n00:00:01,000 --> 00:00:04,000\n你好，这是简体中文测试字幕。\n".encode("utf-8")

        def mock_downloader(url: str) -> bytes:
            if "bad" in url:
                return cand1_bytes
            return cand2_bytes

        mock_discovery = MagicMock()
        mock_discovery.search_gap.return_value = [
            {"provider": "assrt", "url": "http://example.com/bad.srt", "format": "srt", "weight_score": 200.0},
            {"provider": "assrt", "url": "http://example.com/good.ass", "format": "ass", "weight_score": 150.0},
        ]
        materializer = SubtitleMaterializer(
            discovery=mock_discovery,
            downloader=mock_downloader,
        )

        alist = MockAList()
        gap = {
            "id": "missing_subtitle:123:Show.S01E01.mkv",
            "kind": "missing_subtitle",
            "path": "/library/番剧/Show/Show S01E01.mkv",
            "media": {"title": "Show", "tmdb_id": 123},
            "subtitle_language": "zh",
        }

        with tempfile.TemporaryDirectory() as tmpdir:
            workspace = Path(tmpdir)
            acquisition = materializer.acquire_subtitles(
                {"tmdb_id": 123},
                [gap],
                staging_root="/quark/影视/ScrapeFlow/补源/test-root/subtitles",
                workspace=workspace,
                alist=alist,
            )

        self.assertEqual(len(acquisition["files"]), 1)
        # Verify that candidate 2 (.ass) was installed after candidate 1 failed language check
        self.assertIn("/quark/影视/ScrapeFlow/补源/test-root/subtitles/Show S01E01.zh-CN.ass", alist.files)

    def test_materializer_uses_production_alist_upload_signature(self) -> None:
        """The real AList client receives one complete target path and MIME."""
        content = (
            "1\n00:00:01,000 --> 00:00:04,000\n"
            "你好，这是生产签名测试字幕。\n"
        ).encode("utf-8")

        class ProductionShapeAList(MockAList):
            def __init__(self) -> None:
                super().__init__()
                self.calls: list[tuple[str, int, str, bool]] = []

            def upload_bytes(
                self, target_path: str, data: bytes, content_type: str,
                *, overwrite: bool = False,
            ) -> None:
                self.calls.append((target_path, len(data), content_type, overwrite))
                parent = posixpath.dirname(target_path)
                name = posixpath.basename(target_path)
                self.files[target_path] = data
                self.tree.setdefault(parent, [])
                self.tree[parent].append({
                    "name": name, "is_dir": False, "size": len(data),
                })

        discovery = MagicMock()
        discovery.search_gap.return_value = [{
            "provider": "assrt", "url": "https://example.test/sub.srt",
            "format": "srt",
        }]
        materializer = SubtitleMaterializer(
            discovery=discovery,
            downloader=lambda _url: content,
        )
        alist = ProductionShapeAList()
        gap = {
            "id": "missing_subtitle:production",
            "kind": "missing_subtitle",
            "path": "/library/Show/Show.S01E01.mkv",
            "subtitle_language": "zh",
        }
        with tempfile.TemporaryDirectory() as tmpdir:
            result = materializer.acquire_subtitles(
                {"media": {"title": "Show"}}, [gap],
                staging_root="/quark/影视/ScrapeFlow/补源/production/attempt",
                workspace=Path(tmpdir), alist=alist,
            )

        self.assertEqual(len(result["files"]), 1)
        self.assertEqual(
            alist.calls,
            [(
                "/quark/影视/ScrapeFlow/补源/production/attempt/Show.S01E01.zh-CN.srt",
                len(content), "application/x-subrip", False,
            )],
        )

    def test_runtime_handles_no_subtitles_found_as_completed_with_gaps(self) -> None:
        discovery = SubtitleDiscoveryService(
            enabled=True,
            fetcher=lambda url, headers: json.dumps({"data": {"subs": []}}).encode("utf-8"),
        )
        materializer = SubtitleMaterializer(discovery=discovery)

        mock_search = MagicMock()
        mock_mat = MagicMock()
        mock_runner = MagicMock()
        alist = MockAList()

        with tempfile.TemporaryDirectory() as tmpdir:
            runtime = AutomaticReplenishmentRuntime(
                state_root=Path(tmpdir),
                engine_runner=mock_runner,
                alist=alist,
                search=mock_search,
                materializer=mock_mat,
                subtitle_materializer=materializer,
            )

            job = EngineJob.from_dict({
                "id": "audit-root-1",
                "phase": "executed",
                "created_at": "2026-08-14T00:00:00Z",
                "updated_at": "2026-08-14T00:00:00Z",
                "request": {"raw_path": "/library/番剧/Show"},
                "plan": {
                    "mode": "tv",
                    "metadata": {"tmdb_id": 123, "title": "Show"},
                    "scan_report": {
                        "resource_gaps": [
                            {
                                "id": "missing_subtitle:123:Show.S01E01.mkv",
                                "kind": "missing_subtitle",
                                "label": "Show S01E01",
                                "path": "/library/番剧/Show/Season 01/Show S01E01.mkv",
                                "media": {"title": "Show", "tmdb_id": 123},
                                "subtitle_language": "zh",
                            }
                        ]
                    },
                },
                "summary": {"audit_subtitle_only": True},
            })

            outcome = runtime.run_for_job(job)
            self.assertEqual(len(outcome["outcomes"]), 1)
            first = outcome["outcomes"][0]
            self.assertEqual(first["status"], "completed_with_gaps")
            self.assertTrue(first["terminal"])
            mock_search.search.assert_not_called()
            mock_mat.acquire.assert_not_called()

    def test_runtime_acquires_and_installs_subtitles_successfully(self) -> None:
        srt_content = (
            "1\n"
            "00:00:01,000 --> 00:00:04,000\n"
            "你好，这是一个简体中文测试字幕。\n"
        ).encode("utf-8")

        mock_assrt = json.dumps({
            "data": {
                "subs": [
                    {"id": 100, "url": "http://example.com/sub.srt", "format": "srt", "download_count": 10}
                ]
            }
        }).encode("utf-8")

        discovery = SubtitleDiscoveryService(
            enabled=True,
            fetcher=lambda url, headers: mock_assrt,
        )
        materializer = SubtitleMaterializer(
            discovery=discovery,
            downloader=lambda url: srt_content,
        )

        mock_search = MagicMock()
        mock_mat = MagicMock()
        mock_runner = MagicMock()
        mock_runner.install_subtitle_sidecar.return_value = {
            "source": "/quark/影视/ScrapeFlow/补源/test/sub.srt",
            "target": "/library/番剧/Show/Season 01/Show S01E01.zh-CN.srt",
            "size": len(srt_content),
        }
        alist = MockAList()

        with tempfile.TemporaryDirectory() as tmpdir:
            runtime = AutomaticReplenishmentRuntime(
                state_root=Path(tmpdir),
                engine_runner=mock_runner,
                alist=alist,
                search=mock_search,
                materializer=mock_mat,
                subtitle_materializer=materializer,
            )

            job = EngineJob.from_dict({
                "id": "audit-root-2",
                "phase": "executed",
                "created_at": "2026-08-14T00:00:00Z",
                "updated_at": "2026-08-14T00:00:00Z",
                "request": {"raw_path": "/library/番剧/Show"},
                "plan": {
                    "mode": "tv",
                    "metadata": {"tmdb_id": 123, "title": "Show"},
                    "scan_report": {
                        "resource_gaps": [
                            {
                                "id": "missing_subtitle:123:Show.S01E01.mkv",
                                "kind": "missing_subtitle",
                                "label": "Show S01E01",
                                "path": "/library/番剧/Show/Season 01/Show S01E01.mkv",
                                "media": {"title": "Show", "tmdb_id": 123},
                                "subtitle_language": "zh",
                            }
                        ]
                    },
                },
                "summary": {"audit_subtitle_only": True},
            })

            outcome = runtime.run_for_job(job)
            self.assertEqual(len(outcome["outcomes"]), 1)
            first = outcome["outcomes"][0]
            self.assertEqual(first["status"], "completed")
            self.assertTrue(first["terminal"])
            self.assertEqual(first["resolved_gap_ids"], ["missing_subtitle:123:Show.S01E01.mkv"])
            mock_runner.install_subtitle_sidecar.assert_called_once()

    def test_runtime_handles_infrastructure_error_as_retry_wait(self) -> None:
        def failing_fetcher(url: str, headers: dict | None) -> bytes:
            raise SubtitleInfrastructureError("连接超时")

        discovery = SubtitleDiscoveryService(
            enabled=True,
            fetcher=failing_fetcher,
        )
        materializer = SubtitleMaterializer(discovery=discovery)


        mock_search = MagicMock()
        mock_mat = MagicMock()
        mock_runner = MagicMock()
        alist = MockAList()

        with tempfile.TemporaryDirectory() as tmpdir:
            runtime = AutomaticReplenishmentRuntime(
                state_root=Path(tmpdir),
                engine_runner=mock_runner,
                alist=alist,
                search=mock_search,
                materializer=mock_mat,
                subtitle_materializer=materializer,
            )

            job = EngineJob.from_dict({
                "id": "audit-root-3",
                "phase": "executed",
                "created_at": "2026-08-14T00:00:00Z",
                "updated_at": "2026-08-14T00:00:00Z",
                "request": {"raw_path": "/library/番剧/Show"},
                "plan": {
                    "mode": "tv",
                    "metadata": {"tmdb_id": 123, "title": "Show"},
                    "scan_report": {
                        "resource_gaps": [
                            {
                                "id": "missing_subtitle:123:Show.S01E01.mkv",
                                "kind": "missing_subtitle",
                                "label": "Show S01E01",
                                "path": "/library/番剧/Show/Season 01/Show S01E01.mkv",
                                "media": {"title": "Show", "tmdb_id": 123},
                                "subtitle_language": "zh",
                            }
                        ]
                    },
                },
                "summary": {"audit_subtitle_only": True},
            })

            outcome = runtime.run_for_job(job)
            self.assertEqual(len(outcome["outcomes"]), 1)
            first = outcome["outcomes"][0]
            self.assertEqual(first["status"], "retry_wait")
            self.assertFalse(first["terminal"])
            self.assertEqual(first["failure_scope"], "infrastructure")


if __name__ == "__main__":
    unittest.main()
