from __future__ import annotations

import io
import json
import os
from pathlib import Path
import tempfile
import time
from unittest import mock
import unittest

from engine.tools import replenishment_local_adapter as adapter


class ReplenishmentLocalAdapterTests(unittest.TestCase):
    def manifest(self):
        info = {
            b"name": b"Example",
            b"piece length": 16384,
            b"pieces": b"x" * 20,
            b"files": [
                {b"length": 100, b"path": [b"Example S01E01.mkv"]},
                {b"length": 200, b"path": [b"Example S01E02.mkv"]},
            ],
        }
        return adapter._torrent_manifest(adapter._bencode({b"announce": b"https://tracker.invalid", b"info": info}))

    def unlock_local(self, request):
        request["rules"] = {
            **(request.get("rules") or {}),
            "minimum_attempts_per_cloud_lane": 30,
        }
        request["provider_attempts"] = {
            "quark_share": 30, "quark_magnet": 30,
        }
        request["provider_exhausted"] = {"quark_magnet": {
            "exhausted": True,
            "proof": {
                "kind": "search_complete_no_candidates",
                "required_sources": ["Nyaa"],
                "completed_sources": ["Nyaa"],
                "candidate_count": 0,
                "excluded_candidate_count": 0,
            },
        }}
        return request

    def test_dynamic_provider_queries_prefer_compact_season_identity(self):
        request = {
            "media": {"title": "\u745e\u514b\u548c\u83ab\u8482", "aliases": ["Rick and Morty"]},
            "query_groups": [{"season": 6, "season_names": ["\u7b2c 6 \u5b63"]}],
            "search_queries": [
                "\u745e\u514b\u548c\u83ab\u8482 S06E01-E10", "\u745e\u514b\u548c\u83ab\u8482 Season 6",
            ],
        }
        self.assertEqual(adapter._compact_dynamic_search_terms(request, maximum=4), [
            "Rick and Morty S06",
            "\u745e\u514b\u548c\u83ab\u8482 S06",
            "\u745e\u514b\u548c\u83ab\u8482 S06E01-E10",
            "Rick and Morty \u7b2c 6 \u5b63",
        ])

    def test_dynamic_terms_reach_prioritized_official_english_alias(self):
        request = {
            "media": {
                "title": "本地标题",
                "aliases": [
                    "本地标题", "原始タイトル",
                    "Official English Title", "Another Official Alias",
                ],
            },
            "query_groups": [{"season": 0, "season_names": ["特别篇"]}],
            "search_queries": [],
        }
        terms = adapter._compact_dynamic_search_terms(request, maximum=4)
        self.assertIn("Official English Title S00", terms)

    def test_dynamic_terms_keep_distinct_multilingual_s00_release_seasons(self):
        request = {
            "media": {
                "title": "Re：从零开始的异世界生活",
                "aliases": [
                    "Re：从零开始的异世界生活",
                    "Re:ゼロから始める異世界生活",
                ],
            },
            "query_groups": [{
                "season": 0,
                "season_names": ["特别篇"],
                "episode_titles": [
                    "中文四期集名",
                    "Re:ゼロから始める休憩時間 4th season 有言実行備忘録#1",
                    "English fourth title",
                    "中文三期集名",
                    "Re:ゼロから始める休憩時間 3rd season 眠れる鬼の夜話",
                ],
            }],
            "search_queries": [
                "Re：从零开始的异世界生活 S00E51 S00E71",
            ],
            "rules": {"optional_discovery_only": True},
        }
        base = "Re：从零开始的异世界生活 "
        self.assertEqual(
            adapter._compact_dynamic_search_terms(request, maximum=3),
            [
                base + "Re:ゼロから始める休憩時間 4th season 有言実行備忘録#1",
                base + "Re:ゼロから始める休憩時間 3rd season 眠れる鬼の夜話",
                "Re：从零开始的异世界生活 S00",
            ],
        )

    def test_optional_subseries_name_maps_global_s00_ordinals_only_on_identity_match(self):
        request = {
            "media": {"title": "Re:Zero", "aliases": ["Re:Zero"]},
            "gaps": [{
                "id": "S00E73", "season": 0, "episodes": [73],
                "kind": "missing_episode", "label": "S00E73",
            }],
            "query_groups": [{
                "season": 0, "season_names": ["Specials"],
                "episode_titles": [
                    "Re:从零开始的休息时间 4th 有言必行备忘录#3",
                ],
            }],
            "rules": {"optional_discovery_only": True},
        }
        manifest = {
            "root": "Re Zero Break Time", "infohash": "a" * 40,
            "files": {1: {
                "path": "Re 从零开始的休息时间 73.mp4", "size": 100,
            }},
        }

        self.assertEqual(
            adapter._optional_series_title_search_terms(request),
            ["从零开始的休息时间"],
        )
        verified = adapter._torrent_candidate_variants(
            request, "Re 从零开始的休息时间 [73]",
            "https://dl.dmhy.org/verified.torrent", manifest,
            include_local=False,
        )
        unrelated = adapter._torrent_candidate_variants(
            request, "Re Zero S4 - 73",
            "https://dl.dmhy.org/unrelated.torrent", {
                **manifest,
                "files": {1: {"path": "Re Zero S4 - 73.mp4", "size": 100}},
            },
            include_local=False,
        )

        self.assertEqual(verified[0]["provider"], "quark_magnet")
        self.assertEqual(verified[0]["file_coverage"], ["S00E73"])
        self.assertEqual(unrelated, [])

    def test_verified_release_local_alias_maps_break_time_pack(self):
        request = {
            "media": {"title": "Re:Zero", "aliases": ["Re:Zero"]},
            "gaps": [{
                "id": "S00E51", "season": 0, "episodes": [51],
                "kind": "missing_episode", "label": "S00E51",
                "source_episode_aliases": [{
                    "season": 3, "episode": 1,
                    "series_titles": [
                        "Re:Zero - Starting Break Time from Zero",
                    ],
                }],
            }],
            "query_groups": [{
                "season": 0, "season_names": ["Specials"],
                "episode_titles": [
                    "Re:ゼロから始める休憩時間 3rd season 眠れる鬼の夜話",
                    "Re:Zero - Starting Break Time from Zero: Night Tales",
                ],
            }],
            "rules": {"optional_discovery_only": True},
        }
        manifest = {
            "root": "Re Zero Break Time S3", "infohash": "a" * 40,
            "files": {
                1: {"path": "Re Zero Break Time S3 - 01.mkv", "size": 100},
                2: {"path": "Unrelated Show S3 - 01.mkv", "size": 100},
            },
        }

        verified = adapter._torrent_candidate_variants(
            request, "Re Zero Break Time S3 - 01-08",
            "https://storage.animetosho.org/verified.torrent", manifest,
            include_local=False,
        )
        rejected = adapter._torrent_candidate_variants(
            request, "Unrelated Show S3 - 01-08",
            "https://storage.animetosho.org/unrelated.torrent", {
                **manifest,
                "files": {2: manifest["files"][2]},
            },
            include_local=False,
        )

        self.assertEqual(verified[0]["file_coverage"], ["S00E51"])
        self.assertEqual(rejected, [])
        self.assertEqual(adapter._source_episode_search_terms(request), [
            "Re Zero Break Time S3",
        ])
        self.assertEqual(adapter._source_episode_release_priority(
            request, "Re Zero Break Time S3 - 01-08",
        ), 0)
        self.assertEqual(adapter._source_episode_release_priority(
            request, "Re Zero Break Time S3 - 09-16",
        ), 1)
        self.assertEqual(adapter._source_episode_release_priority(
            request, "Unrelated Show S3 - 01-08",
        ), 1)

    def test_local_torrent_gate_requires_complete_permanent_exhaustion_proof(self):
        request = {
            "rules": {"minimum_attempts_per_cloud_lane": 30},
            "provider_attempts": {"quark_magnet": 30},
        }
        self.assertFalse(adapter._local_torrent_unlocked(request))
        request["provider_exhausted"] = {"quark_magnet": {
            "exhausted": True,
            "proof": {"kind": "search_complete_no_candidates"},
        }}
        self.assertFalse(adapter._local_torrent_unlocked(request))
        self.unlock_local(request)
        request["provider_attempts"]["quark_magnet"] = 0
        self.assertTrue(adapter._local_torrent_unlocked(request))

    def test_resource_failure_floor_never_unlocks_local_without_source_exhaustion(self):
        request = {
            "rules": {"minimum_attempts_per_cloud_lane": 30},
            "provider_attempts": {"quark_magnet": 30},
            "provider_exhausted": {"quark_magnet": {
                "exhausted": True,
                "proof": {
                    "kind": "resource_failure_floor_reached",
                    "required_floor": 20,
                    "distinct_failure_count": 30,
                },
            }},
        }
        self.assertFalse(adapter._local_torrent_unlocked(request))
        request["provider_exhausted"]["quark_magnet"]["proof"][
            "required_floor"
        ] = 30
        self.assertFalse(adapter._local_torrent_unlocked(request))

    def test_resource_failure_floor_expands_only_the_exhaustion_search_budget(self):
        request = {
            "rules": {"minimum_attempts_per_cloud_lane": 30},
            "provider_attempts": {"quark_magnet": 30},
            "provider_exhausted": {"quark_magnet": {
                "exhausted": True,
                "proof": {
                    "kind": "resource_failure_floor_reached",
                    "required_floor": 30,
                    "distinct_failure_count": 30,
                },
            }},
        }
        with mock.patch.dict(os.environ, {
            "SCRAPEFLOW_REPLENISHMENT_DYNAMIC_SEARCH_TIMEOUT": "45",
        }):
            self.assertEqual(adapter._dynamic_search_timeout_seconds({}), 45)
            self.assertEqual(
                adapter._dynamic_search_timeout_seconds(request), 120,
            )
        with mock.patch.dict(os.environ, {
            "SCRAPEFLOW_REPLENISHMENT_DYNAMIC_SEARCH_TIMEOUT": "180",
        }):
            self.assertEqual(
                adapter._dynamic_search_timeout_seconds(request), 180,
            )
        self.assertFalse(adapter._local_torrent_unlocked(request))

    def test_torrent_manifest_preserves_one_based_file_indices(self):
        manifest = self.manifest()
        self.assertEqual(manifest["root"], "Example")
        self.assertEqual(manifest["files"][1], {"path": "Example S01E01.mkv", "size": 100})
        self.assertEqual(manifest["files"][2], {"path": "Example S01E02.mkv", "size": 200})

    def test_manifest_verification_requires_exact_path_and_size(self):
        manifest = self.manifest()
        selection = {
            "selected_gap_ids": ["S01E02"],
            "infohash": manifest["infohash"],
            "acquisition": {
                "kind": "torrent", "url": "https://example.invalid/item.torrent",
                "file_index_by_gap": {"S01E02": [2]},
                "file_size_by_index": {"2": 200},
                "file_path_by_index": {"2": "Example S01E02.mkv"},
            },
        }
        indices, by_index = adapter._verify_manifest(selection, manifest)
        self.assertEqual(indices, {2})
        self.assertEqual(by_index, {2: ["S01E02"]})
        selection["acquisition"]["file_size_by_index"]["2"] = 201
        with self.assertRaisesRegex(ValueError, "大小"):
            adapter._verify_manifest(selection, manifest)

    def test_search_only_returns_the_requested_tmdb_catalog(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "catalog.json"
            path.write_text(json.dumps({
                "verified_at": "2026-07-27T00:00:00Z",
                "projects": {
                    "42": {"candidates": [{"tmdb_id": 42, "locator": "fixture:42"}]},
                    "43": {"candidates": [{"tmdb_id": 43, "locator": "fixture:43"}]},
                },
            }), encoding="utf-8")
            with mock.patch.dict(os.environ, {
                "SCRAPEFLOW_REPLENISHMENT_CATALOG": str(path),
                "SCRAPEFLOW_REPLENISHMENT_ACG_SEARCH": "0",
            }):
                result = adapter._search({"media": {"tmdb_id": 42}})
        self.assertEqual([item["locator"] for item in result["candidates"]], ["fixture:42"])
        self.assertEqual(result["lane_status"]["quark_magnet"]["status"], "ready")

    def test_missing_quark_share_index_is_structured_as_lane_infrastructure_failure(self):
        with tempfile.TemporaryDirectory() as directory, mock.patch.dict(
            os.environ,
            {
                "SCRAPEFLOW_REPLENISHMENT_CATALOG": str(Path(directory) / "missing.json"),
                "SCRAPEFLOW_REPLENISHMENT_QUARK_INDEX": "",
                "SCRAPEFLOW_REPLENISHMENT_ACG_SEARCH": "0",
            },
        ):
            result = adapter._search({"media": {"tmdb_id": 42}})
        self.assertEqual(result["lane_status"]["quark_share"], {
            "status": "infrastructure_failure",
            "reason": "quark_share_index_unavailable",
        })
        self.assertIn("夸克分享索引不可用: OSError", result["warnings"])
        self.assertEqual(result["lane_status"]["quark_magnet"], {
            "status": "infrastructure_failure",
            "reason": "torrent_candidate_sources_unavailable",
        })

    def test_search_defers_magnet_sources_until_share_attempt_floor(self):
        request = {
            "media": {"title": "Example", "tmdb_id": 42},
            "rules": {"minimum_attempts_per_cloud_lane": 30},
            "provider_attempts": {"quark_share": 29, "quark_magnet": 0},
        }
        with tempfile.TemporaryDirectory() as directory:
            catalog = Path(directory) / "catalog.json"
            index = Path(directory) / "quark-index.json"
            catalog.write_text(json.dumps({
                "verified_at": "2026-07-29T00:00:00Z",
                "projects": {"42": {"candidates": [{
                    "tmdb_id": 42,
                    "locator": "magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567",
                    "infohash": "0123456789abcdef0123456789abcdef01234567",
                }]}},
            }), encoding="utf-8")
            index.write_text(json.dumps({"shares": []}), encoding="utf-8")
            with mock.patch.dict(os.environ, {
                "SCRAPEFLOW_REPLENISHMENT_CATALOG": str(catalog),
                "SCRAPEFLOW_REPLENISHMENT_QUARK_INDEX": str(index),
                "SCRAPEFLOW_REPLENISHMENT_ACG_SEARCH": "1",
            }), mock.patch.object(adapter, "_search_nyaa") as nyaa, mock.patch.object(
                adapter, "_search_acg",
            ) as acg:
                result = adapter._search(request)
        self.assertEqual(result["candidates"], [])
        self.assertEqual(result["active_search_lane"], "quark_share")
        self.assertEqual(result["lane_status"]["quark_magnet"], {
            "status": "deferred",
            "reason": "quark_share_attempt_floor_not_reached",
        })
        nyaa.assert_not_called()
        acg.assert_not_called()

    def test_search_enables_magnet_sources_after_share_attempt_floor(self):
        request = {
            "media": {"title": "Example", "tmdb_id": 42},
            "rules": {"minimum_attempts_per_cloud_lane": 30},
            "provider_attempts": {"quark_share": 30, "quark_magnet": 0},
        }
        with tempfile.TemporaryDirectory() as directory:
            catalog = Path(directory) / "catalog.json"
            index = Path(directory) / "quark-index.json"
            catalog.write_text(json.dumps({
                "verified_at": "2026-07-29T00:00:00Z",
                "projects": {"42": {"candidates": []}},
            }), encoding="utf-8")
            index.write_text(json.dumps({"shares": []}), encoding="utf-8")
            with mock.patch.dict(os.environ, {
                "SCRAPEFLOW_REPLENISHMENT_CATALOG": str(catalog),
                "SCRAPEFLOW_REPLENISHMENT_QUARK_INDEX": str(index),
                "SCRAPEFLOW_REPLENISHMENT_ACG_SEARCH": "1",
            }), mock.patch.object(
                adapter, "_search_nyaa", return_value=adapter._DynamicSearchResult(
                    [], query_attempts=1, query_responses=1,
                ),
            ) as nyaa, mock.patch.object(
                adapter, "_search_acg", return_value=adapter._DynamicSearchResult(
                    [], query_attempts=1, query_responses=1,
                ),
            ) as acg:
                result = adapter._search(request)
        self.assertEqual(result["active_search_lane"], "quark_magnet")
        self.assertEqual(result["lane_status"]["quark_magnet"], {"status": "ready"})
        nyaa.assert_called_once()
        acg.assert_called_once()

    def test_pansou_exclusions_do_not_consume_the_first_thirty_share_slots(self):
        request = {
            "media": {"title": "Example", "aliases": ["Example"], "tmdb_id": 42},
            "gaps": [{"id": "S01E01", "kind": "missing_episode", "season": 1}],
            "query_groups": [{"season": 1, "season_names": ["Season 1"]}],
            "excluded_candidates": [
                {"provider": "quark_share", "locator": f"quark_share:excluded{index:02d}"}
                for index in range(30)
            ],
        }
        rows = [
            {
                "url": f"https://pan.quark.cn/s/excluded{index:02d}",
                "note": "Example S01E01 1080p",
            }
            for index in range(30)
        ] + [{
            "url": "https://pan.quark.cn/s/fresh0030",
            "note": "Example S01E01 1080p",
        }]
        response_body = json.dumps({
            "code": 0,
            "data": {"merged_by_type": {"quark": rows}},
        }).encode("utf-8")
        inspected: list[str] = []

        def inspect(_bridge, _session, *, pwd_id, passcode=""):
            inspected.append(pwd_id)
            return [{
                "file_id": f"file-{pwd_id}",
                "path": "Example S01E01 1080p.mkv",
                "size": 101,
            }]

        with tempfile.TemporaryDirectory() as directory:
            catalog = Path(directory) / "catalog.json"
            index = Path(directory) / "quark-index.json"
            catalog.write_text(json.dumps({
                "verified_at": "2026-07-29T00:00:00Z",
                "projects": {"42": {"candidates": []}},
            }), encoding="utf-8")
            index.write_text(json.dumps({"shares": []}), encoding="utf-8")
            with mock.patch.dict(os.environ, {
                "SCRAPEFLOW_REPLENISHMENT_CATALOG": str(catalog),
                "SCRAPEFLOW_REPLENISHMENT_QUARK_INDEX": str(index),
                "SCRAPEFLOW_REPLENISHMENT_PANSOU_URL": "https://fixture.invalid/api/search",
                "SCRAPEFLOW_REPLENISHMENT_PANSOU_MAX_SHARES": "30",
                "SCRAPEFLOW_REPLENISHMENT_ACG_SEARCH": "0",
            }), mock.patch.object(
                adapter.urllib.request, "urlopen",
                side_effect=lambda *_args, **_kwargs: io.BytesIO(response_body),
            ), mock.patch.object(
                adapter, "_alist_client", return_value=mock.Mock(),
            ), mock.patch.object(
                adapter, "delegated_quark_session", return_value=mock.Mock(),
            ), mock.patch.object(
                adapter.QuarkFastSaveBridge, "inspect_share", new=inspect,
            ):
                result = adapter._search(request)

        self.assertIn("fresh0030", inspected)
        self.assertEqual(
            [row["locator"] for row in result["candidates"]],
            ["quark_share:fresh0030"],
        )

    def test_all_pansou_share_inspection_failures_are_infrastructure_failure(self):
        request = {
            "media": {"title": "Example", "aliases": ["Example"], "tmdb_id": 42},
            "gaps": [{"id": "S01E01", "kind": "missing_episode", "season": 1}],
            "query_groups": [{"season": 1, "season_names": ["Season 1"]}],
        }
        links = [{
            "share_id": f"share{index:02d}",
            "share_url": f"https://pan.quark.cn/s/share{index:02d}",
            "passcode": "",
            "release_name": "Example S01E01 1080p",
        } for index in range(3)]
        with tempfile.TemporaryDirectory() as directory:
            catalog = Path(directory) / "catalog.json"
            index = Path(directory) / "quark-index.json"
            catalog.write_text(json.dumps({
                "verified_at": "2026-07-29T00:00:00Z",
                "projects": {"42": {"candidates": []}},
            }), encoding="utf-8")
            index.write_text(json.dumps({"shares": []}), encoding="utf-8")
            with mock.patch.dict(os.environ, {
                "SCRAPEFLOW_REPLENISHMENT_CATALOG": str(catalog),
                "SCRAPEFLOW_REPLENISHMENT_QUARK_INDEX": str(index),
                "SCRAPEFLOW_REPLENISHMENT_PANSOU_URL": "https://fixture.invalid/api/search",
                "SCRAPEFLOW_REPLENISHMENT_ACG_SEARCH": "0",
            }), mock.patch.object(
                adapter, "_pansou_quark_links", return_value=(links, {
                    "query_attempts": 1,
                    "query_responses": 1,
                    "raw_discovered": len(links),
                    "search_complete": True,
                }),
            ), mock.patch.object(
                adapter, "_alist_client", return_value=mock.Mock(),
            ), mock.patch.object(
                adapter, "delegated_quark_session", return_value=mock.Mock(),
            ), mock.patch.object(
                adapter.QuarkFastSaveBridge, "inspect_share",
                side_effect=adapter.QuarkBridgeError("Quark API unavailable"),
            ):
                result = adapter._search(request)

        self.assertEqual(result["candidates"], [])
        self.assertEqual(result["lane_status"]["quark_share"], {
            "status": "infrastructure_failure",
            "reason": "quark_share_inspection_unavailable",
        })

    def test_mixed_share_resource_and_infrastructure_failures_keep_resource_evidence(self):
        request = {
            "media": {"title": "Example", "aliases": ["Example"], "tmdb_id": 42},
            "gaps": [{"id": "S01E01", "kind": "missing_episode", "season": 1}],
            "query_groups": [{"season": 1, "season_names": ["Season 1"]}],
        }
        links = [{
            "share_id": f"share{index:02d}",
            "share_url": f"https://pan.quark.cn/s/share{index:02d}",
            "passcode": "", "release_name": "Example S01E01 1080p",
        } for index in range(6)]

        def inspect(_bridge, _session, *, pwd_id, passcode=""):
            if pwd_id == "share05":
                raise adapter.QuarkBridgeError("fixture infrastructure")
            raise adapter.QuarkShareExpiredError("fixture expired")

        with tempfile.TemporaryDirectory() as directory:
            catalog = Path(directory) / "catalog.json"
            index = Path(directory) / "quark-index.json"
            catalog.write_text(json.dumps({
                "verified_at": "2026-07-29T00:00:00Z",
                "projects": {"42": {"candidates": []}},
            }), encoding="utf-8")
            index.write_text(json.dumps({"shares": []}), encoding="utf-8")
            with mock.patch.dict(os.environ, {
                "SCRAPEFLOW_REPLENISHMENT_CATALOG": str(catalog),
                "SCRAPEFLOW_REPLENISHMENT_QUARK_INDEX": str(index),
                "SCRAPEFLOW_REPLENISHMENT_PANSOU_URL": "https://fixture.invalid/api/search",
                "SCRAPEFLOW_REPLENISHMENT_ACG_SEARCH": "0",
            }), mock.patch.object(
                adapter, "_pansou_quark_links", return_value=(links, {
                    "query_attempts": 1, "query_responses": 1,
                    "raw_discovered": 6, "available_discovered": 6,
                    "search_complete": True,
                }),
            ), mock.patch.object(
                adapter, "_alist_client", return_value=mock.Mock(),
            ), mock.patch.object(
                adapter, "delegated_quark_session", return_value=mock.Mock(),
            ), mock.patch.object(
                adapter.QuarkFastSaveBridge, "inspect_share", new=inspect,
            ):
                result = adapter._search(request)

        self.assertEqual(result["lane_status"]["quark_share"], {"status": "ready"})
        self.assertEqual(result["share_discovery"]["candidate_failures"], 5)
        self.assertEqual(result["share_discovery"]["infrastructure_failures"], 1)
        self.assertEqual(len(result["share_discovery"]["resource_failed_locators"]), 5)
        self.assertFalse(result["share_discovery"]["source_exhausted"])

    def test_successful_empty_pansou_result_exhausts_share_source(self):
        request = {
            "media": {"title": "Example", "aliases": ["Example"], "tmdb_id": 42},
            "gaps": [{"id": "S01E01", "kind": "missing_episode", "season": 1}],
            "query_groups": [{"season": 1, "season_names": ["Season 1"]}],
            "rules": {"minimum_attempts_per_cloud_lane": 30},
            "provider_attempts": {"quark_share": 0, "quark_magnet": 0},
        }
        response_body = json.dumps({
            "code": 0, "data": {"merged_by_type": {"quark": []}},
        }).encode("utf-8")
        with tempfile.TemporaryDirectory() as directory:
            catalog = Path(directory) / "catalog.json"
            index = Path(directory) / "quark-index.json"
            catalog.write_text(json.dumps({
                "verified_at": "2026-07-29T00:00:00Z",
                "projects": {"42": {"candidates": []}},
            }), encoding="utf-8")
            index.write_text(json.dumps({"shares": []}), encoding="utf-8")
            with mock.patch.dict(os.environ, {
                "SCRAPEFLOW_REPLENISHMENT_CATALOG": str(catalog),
                "SCRAPEFLOW_REPLENISHMENT_QUARK_INDEX": str(index),
                "SCRAPEFLOW_REPLENISHMENT_PANSOU_URL": "https://fixture.invalid/api/search",
                "SCRAPEFLOW_REPLENISHMENT_ACG_SEARCH": "1",
            }), mock.patch.object(
                adapter.urllib.request, "urlopen",
                side_effect=lambda *_args, **_kwargs: io.BytesIO(response_body),
            ), mock.patch.object(
                adapter, "_search_nyaa", return_value=adapter._DynamicSearchResult(
                    [], query_attempts=1, query_responses=1,
                ),
            ), mock.patch.object(
                adapter, "_search_acg", return_value=adapter._DynamicSearchResult(
                    [], query_attempts=1, query_responses=1,
                ),
            ):
                result = adapter._search(request)
        self.assertTrue(result["share_discovery"]["source_exhausted"])
        self.assertEqual(result["lane_status"]["quark_share"]["status"], "exhausted")
        self.assertEqual(result["active_search_lane"], "quark_magnet")

    def test_animetosho_official_feed_builds_quark_offline_candidate(self):
        request = {
            "media": {"title": "Example", "aliases": ["Example"], "tmdb_id": 42},
            "gaps": [{"id": "S01E01", "kind": "missing_episode", "season": 1}],
            "query_groups": [{"season": 1, "season_names": ["Season 1"]}],
        }
        torrent_url = (
            "https://storage.animetosho.org/torrent/"
            "0123456789abcdef0123456789abcdef01234567/example.torrent"
        )
        feed = json.dumps([{
            "title": "Example S01E01 1080p", "torrent_url": torrent_url,
        }]).encode("utf-8")
        manifest = self.manifest()
        with mock.patch.object(
            adapter, "_fetch_bytes", return_value=feed,
        ), mock.patch.object(
            adapter, "_download_torrent", return_value=manifest,
        ):
            result = adapter._search_animetosho(
                request, set(), deadline=time.monotonic() + 30,
            )
        self.assertTrue(result.source_exhausted)
        self.assertEqual(result.query_attempts, result.query_responses)
        self.assertEqual([row["provider"] for row in result], ["quark_magnet"])
        self.assertEqual(result[0]["infohash"], manifest["infohash"])

    def test_animetosho_downloads_verified_source_alias_coverage_first(self):
        request = {
            "media": {"title": "Re:Zero", "aliases": ["Re:Zero"], "tmdb_id": 42},
            "gaps": [{
                "id": "S00E51", "kind": "missing_episode", "season": 0,
                "source_episode_aliases": [{
                    "season": 3, "episode": 1,
                    "series_titles": ["Re:Zero - Starting Break Time from Zero"],
                }],
            }],
            "query_groups": [{
                "season": 0,
                "episode_titles": [
                    "Re:Zero - Starting Break Time from Zero: Night Tales",
                ],
            }],
            "rules": {"optional_discovery_only": True},
        }
        wrong_url = (
            "https://storage.animetosho.org/torrent/"
            + "1" * 40 + "/wrong.torrent"
        )
        target_url = (
            "https://storage.animetosho.org/torrent/"
            + "2" * 40 + "/target.torrent"
        )
        feed = json.dumps([
            {"title": "Re Zero Break Time S3 - 13", "torrent_url": wrong_url},
            {"title": "Re Zero Break Time S3 - 01-08", "torrent_url": target_url},
        ]).encode()
        target_manifest = {
            "root": "Re Zero Break Time S3", "infohash": "2" * 40,
            "files": {1: {
                "path": "Re Zero Break Time S3 - 01.mkv", "size": 100,
            }},
        }
        wrong_manifest = {
            **target_manifest,
            "infohash": "1" * 40,
            "files": {1: {
                "path": "Re Zero Break Time S3 - 13.mkv", "size": 100,
            }},
        }
        downloads: list[str] = []

        def download(url, *_args, **_kwargs):
            downloads.append(url)
            return target_manifest if url == target_url else wrong_manifest

        with mock.patch.object(adapter, "_fetch_bytes", return_value=feed), \
                mock.patch.object(adapter, "_download_torrent", side_effect=download):
            result = adapter._search_animetosho(
                request, set(), deadline=time.monotonic() + 30,
            )

        self.assertEqual(downloads[0], target_url)
        self.assertEqual(result[0]["file_coverage"], ["S00E51"])

    def test_tokyotosho_html_builds_quark_offline_candidate(self):
        request = {
            "media": {"title": "Example", "aliases": ["Example"], "tmdb_id": 42},
            "gaps": [{"id": "S01E01", "kind": "missing_episode", "season": 1}],
            "query_groups": [{"season": 1, "season_names": ["Season 1"]}],
        }
        manifest = self.manifest()
        infohash = manifest["infohash"]
        page = (
            "<html><body>"
            f"<a href='magnet:?xt=urn:btih:{infohash}'></a>"
            "<a href='https://tracker.example/get/example.torrent'>"
            "Example S01E01 1080p</a>"
            "</body></html>"
        ).encode("utf-8")
        with mock.patch.object(
            adapter, "_dynamic_search_terms", return_value=["Example S01E01"],
        ), mock.patch.object(
            adapter, "_fetch_bytes", return_value=page,
        ), mock.patch.object(
            adapter, "_download_torrent", return_value=manifest,
        ):
            result = adapter._search_tokyotosho(
                request, set(), deadline=time.monotonic() + 30,
            )
        self.assertTrue(result.source_exhausted)
        self.assertEqual(result.query_attempts, 1)
        self.assertEqual(result.query_responses, 1)
        self.assertEqual([row["provider"] for row in result], ["quark_magnet"])
        self.assertEqual(result[0]["infohash"], infohash)

    def test_tokyotosho_preexcludes_infohash_before_result_cap(self):
        request = {
            "media": {"title": "Example", "aliases": ["Example"], "tmdb_id": 42},
            "gaps": [{"id": "S01E01", "kind": "missing_episode", "season": 1}],
            "query_groups": [{"season": 1, "season_names": ["Season 1"]}],
        }
        manifest = self.manifest()
        excluded_hashes = [f"{index + 1:040x}" for index in range(32)]
        anchors = "".join(
            f"<a href='magnet:?xt=urn:btih:{infohash}'></a>"
            f"<a href='https://tracker.example/get/{index}.torrent'>"
            f"Example old {index:02d} S01E01</a>"
            for index, infohash in enumerate(excluded_hashes)
        ) + (
            f"<a href='magnet:?xt=urn:btih:{manifest['infohash']}'></a>"
            "<a href='https://tracker.example/get/fresh.torrent'>"
            "Example fresh S01E01</a>"
        )
        existing = {f"quark_magnet:{value}" for value in excluded_hashes}
        with mock.patch.object(
            adapter, "_dynamic_search_terms", return_value=["Example S01E01"],
        ), mock.patch.object(
            adapter, "_fetch_bytes", return_value=anchors.encode("utf-8"),
        ), mock.patch.object(
            adapter, "_download_torrent", return_value=manifest,
        ) as download:
            result = adapter._search_tokyotosho(
                request, existing, deadline=time.monotonic() + 30,
            )
        download.assert_called_once()
        self.assertEqual(result.preexcluded_count, 32)
        self.assertTrue(result.source_exhausted)
        self.assertEqual([row["provider"] for row in result], ["quark_magnet"])

    def test_required_tokyotosho_can_exhaust_when_nyaa_tls_is_unavailable(self):
        request = {
            "media": {"title": "Example", "tmdb_id": 42},
            "rules": {"minimum_attempts_per_cloud_lane": 30},
            "provider_attempts": {"quark_share": 30, "quark_magnet": 0},
        }
        exhausted = adapter._DynamicSearchResult(
            [], query_attempts=1, query_responses=1, source_exhausted=True,
        )
        unavailable = adapter._DynamicSearchResult(
            [], query_attempts=1, query_responses=0,
            infrastructure_failure_types={"tls_failure": 1},
        )
        with tempfile.TemporaryDirectory() as directory:
            catalog = Path(directory) / "catalog.json"
            index = Path(directory) / "quark-index.json"
            catalog.write_text(json.dumps({
                "verified_at": "2026-07-31T00:00:00Z",
                "projects": {"42": {"candidates": []}},
            }), encoding="utf-8")
            index.write_text(json.dumps({"shares": []}), encoding="utf-8")
            with mock.patch.dict(os.environ, {
                "SCRAPEFLOW_REPLENISHMENT_CATALOG": str(catalog),
                "SCRAPEFLOW_REPLENISHMENT_QUARK_INDEX": str(index),
                "SCRAPEFLOW_REPLENISHMENT_ACG_SEARCH": "1",
                "SCRAPEFLOW_REPLENISHMENT_TOKYOTOSHO_SEARCH": "1",
                "SCRAPEFLOW_REPLENISHMENT_ANIMETOSHO_SEARCH": "1",
                "SCRAPEFLOW_REPLENISHMENT_MIKAN_SEARCH": "1",
            }), mock.patch.object(
                adapter, "_search_nyaa", return_value=unavailable,
            ), mock.patch.object(
                adapter, "_search_tokyotosho", return_value=exhausted,
            ), mock.patch.object(
                adapter, "_search_animetosho", return_value=exhausted,
            ), mock.patch.object(
                adapter, "_search_mikan", return_value=exhausted,
            ), mock.patch.object(
                adapter, "_search_acg", return_value=unavailable,
            ):
                result = adapter._search(request)
        sources = result["magnet_discovery"]["sources"]
        self.assertFalse(sources["Nyaa"]["required"])
        self.assertTrue(sources["TokyoTosho"]["required"])
        self.assertTrue(sources["AnimeTosho"]["required"])
        self.assertFalse(sources["Mikan"]["required"])
        self.assertTrue(sources["Mikan"]["source_exhausted"])
        self.assertEqual(result["lane_status"]["quark_magnet"]["status"], "exhausted")
        self.assertEqual(
            result["lane_status"]["quark_magnet"]["proof"]["required_sources"],
            ["AnimeTosho", "TokyoTosho"],
        )

    def test_unconfigured_optional_catalog_does_not_block_dynamic_exhaustion(self):
        request = {
            "media": {"title": "Example", "tmdb_id": 42},
            "rules": {"minimum_attempts_per_cloud_lane": 30},
            "provider_attempts": {"quark_share": 30, "quark_magnet": 30},
        }
        exhausted = adapter._DynamicSearchResult(
            [], query_attempts=1, query_responses=1, source_exhausted=True,
        )
        unavailable = adapter._DynamicSearchResult(
            [], query_attempts=1, query_responses=0,
            infrastructure_failure_types={"tls_failure": 1},
        )
        with mock.patch.dict(os.environ, {
            "SCRAPEFLOW_REPLENISHMENT_CATALOG": "",
            "SCRAPEFLOW_REPLENISHMENT_QUARK_INDEX": "",
            "SCRAPEFLOW_REPLENISHMENT_PANSOU_URL": "",
            "SCRAPEFLOW_REPLENISHMENT_ACG_SEARCH": "1",
            "SCRAPEFLOW_REPLENISHMENT_TOKYOTOSHO_SEARCH": "1",
            "SCRAPEFLOW_REPLENISHMENT_ANIMETOSHO_SEARCH": "1",
        }), mock.patch.object(
            adapter, "_search_nyaa", return_value=unavailable,
        ), mock.patch.object(
            adapter, "_search_tokyotosho", return_value=exhausted,
        ), mock.patch.object(
            adapter, "_search_animetosho", return_value=exhausted,
        ), mock.patch.object(
            adapter, "_search_acg", return_value=unavailable,
        ):
            result = adapter._search(request)

        self.assertEqual(
            result["lane_status"]["quark_magnet"]["status"], "exhausted",
        )
        self.assertEqual(
            result["lane_status"]["quark_magnet"]["proof"]["required_sources"],
            ["AnimeTosho", "TokyoTosho"],
        )

    def test_broken_configured_catalog_still_blocks_dynamic_exhaustion(self):
        request = {
            "media": {"title": "Example", "tmdb_id": 42},
            "rules": {"minimum_attempts_per_cloud_lane": 30},
            "provider_attempts": {"quark_share": 30, "quark_magnet": 30},
        }
        exhausted = adapter._DynamicSearchResult(
            [], query_attempts=1, query_responses=1, source_exhausted=True,
        )
        unavailable = adapter._DynamicSearchResult(
            [], query_attempts=1, query_responses=0,
        )
        with tempfile.TemporaryDirectory() as directory, mock.patch.dict(
            os.environ,
            {
                "SCRAPEFLOW_REPLENISHMENT_CATALOG": str(
                    Path(directory) / "missing-catalog.json"
                ),
                "SCRAPEFLOW_REPLENISHMENT_QUARK_INDEX": "",
                "SCRAPEFLOW_REPLENISHMENT_PANSOU_URL": "",
                "SCRAPEFLOW_REPLENISHMENT_ACG_SEARCH": "1",
                "SCRAPEFLOW_REPLENISHMENT_TOKYOTOSHO_SEARCH": "1",
                "SCRAPEFLOW_REPLENISHMENT_ANIMETOSHO_SEARCH": "1",
            },
        ), mock.patch.object(
            adapter, "_search_nyaa", return_value=unavailable,
        ), mock.patch.object(
            adapter, "_search_tokyotosho", return_value=exhausted,
        ), mock.patch.object(
            adapter, "_search_animetosho", return_value=exhausted,
        ), mock.patch.object(
            adapter, "_search_acg", return_value=unavailable,
        ):
            result = adapter._search(request)

        self.assertEqual(
            result["lane_status"]["quark_magnet"], {"status": "ready"},
        )
        self.assertIn("已核验候选目录不可用: ValueError", result["warnings"])

    def test_animetosho_excluded_first_thirty_two_hashes_do_not_fill_batch(self):
        request = {
            "media": {"title": "Example", "aliases": ["Example"], "tmdb_id": 42},
            "gaps": [{"id": "S01E01", "kind": "missing_episode", "season": 1}],
            "query_groups": [{"season": 1, "season_names": ["Season 1"]}],
        }
        manifest = self.manifest()
        excluded_hashes = [f"{index:040x}" for index in range(32)]
        rows = [
            {
                "title": f"Example excluded {index:02d} S01E01 1080p",
                "torrent_url": (
                    "https://storage.animetosho.org/torrent/"
                    f"{infohash}/excluded-{index:02d}.torrent"
                ),
                "info_hash": infohash,
            }
            for index, infohash in enumerate(excluded_hashes)
        ] + [{
            "title": "Example fresh S01E01 1080p",
            "torrent_url": (
                "https://storage.animetosho.org/torrent/"
                f"{manifest['infohash']}/fresh.torrent"
            ),
            "info_hash": manifest["infohash"],
        }]
        existing = {f"quark_magnet:{infohash}" for infohash in excluded_hashes}
        with mock.patch.object(
            adapter, "_fetch_bytes", return_value=json.dumps(rows).encode("utf-8"),
        ), mock.patch.object(
            adapter, "_download_torrent", return_value=manifest,
        ) as download:
            result = adapter._search_animetosho(
                request, existing, deadline=time.monotonic() + 30,
            )

        download.assert_called_once()
        self.assertEqual([row["provider"] for row in result], ["quark_magnet"])
        self.assertEqual(result[0]["infohash"], manifest["infohash"])
        self.assertTrue(result.source_exhausted)

    def test_successful_dynamic_share_discovery_overrides_static_index_failure(self):
        candidate = {
            "provider": "quark_share",
            "tmdb_id": 42,
            "release_name": "Example S01E01 1080p",
            "locator": "quark_share:fresh",
        }
        with tempfile.TemporaryDirectory() as directory, mock.patch.dict(
            os.environ,
            {
                "SCRAPEFLOW_REPLENISHMENT_CATALOG": str(Path(directory) / "missing-catalog.json"),
                "SCRAPEFLOW_REPLENISHMENT_QUARK_INDEX": str(Path(directory) / "missing-index.json"),
                "SCRAPEFLOW_REPLENISHMENT_PANSOU_URL": "https://fixture.invalid/api/search",
                "SCRAPEFLOW_REPLENISHMENT_ACG_SEARCH": "0",
            },
        ), mock.patch.object(
            adapter, "_search_dynamic_quark_share",
            return_value=([candidate], {
                "query_attempts": 1, "query_responses": 1,
                "raw_discovered": 1, "search_complete": True,
                "discovered": 1, "inspected": 1,
                "candidate_failures": 0, "infrastructure_failures": 0,
                "resource_failed_locators": [],
                "infrastructure_failure_types": {}, "source_exhausted": False,
            }),
        ):
            result = adapter._search({"media": {"title": "Example", "tmdb_id": 42}})

        self.assertEqual(result["candidates"], [candidate])
        self.assertEqual(
            result["lane_status"]["quark_share"], {"status": "ready"},
        )

    def test_quark_share_index_deduplicates_share_and_returns_fast_save_evidence(self):
        request = {
            "media": {"title": "Example", "aliases": ["Example"], "tmdb_id": 42},
            "gaps": [
                {"id": "S01E01", "kind": "missing_episode", "season": 1},
                {"id": "S01E02", "kind": "missing_episode", "season": 1},
            ],
            "query_groups": [{"season": 1, "season_names": ["Season 1"]}],
        }
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "quark-index.json"
            path.write_text(json.dumps({"shares": [
                {"share_url": "https://pan.quark.cn/s/AbCd1234", "tmdb_id": 42,
                 "release_name": "Example S01 1080p", "updated_at": "2026-07-26T00:00:00Z",
                 "files": [{"file_id": "f1", "path": "Example S01E01 1080p.mkv", "size": 101}]},
                {"share_id": "AbCd1234", "tmdb_id": 42,
                 "release_name": "Example S01 1080p", "updated_at": "2026-07-27T00:00:00Z",
                 "files": [{"file_id": "f2", "path": "Example S01E02 1080p.mkv", "size": 202}]},
            ]}), encoding="utf-8")
            with mock.patch.dict(os.environ, {"SCRAPEFLOW_REPLENISHMENT_QUARK_INDEX": str(path)}):
                result = adapter._search_quark_share(request, set())
        self.assertEqual(len(result), 1)
        candidate = result[0]
        self.assertEqual(candidate["provider"], "quark_share")
        self.assertEqual(candidate["locator"], "quark_share:AbCd1234")
        self.assertEqual(candidate["file_coverage"], ["S01E01", "S01E02"])
        self.assertEqual(candidate["size"], 303)
        self.assertEqual(candidate["acquisition"], {
            "kind": "quark_fast_save", "share_id": "AbCd1234",
            "share_url": "https://pan.quark.cn/s/AbCd1234",
            "file_id_by_gap": {"S01E01": ["f1"], "S01E02": ["f2"]},
            "file_path_by_id": {"f1": "Example S01E01 1080p.mkv", "f2": "Example S01E02 1080p.mkv"},
            "file_size_by_id": {"f1": 101, "f2": 202},
            "save_strategy": "server_side_copy", "requires_share_revalidation": True,
            "payload_kind": "video_payload", "requires_extraction": False,
            "archive_format": None, "expected_archives": [],
        })

    def test_quark_share_index_rejects_conflicting_file_identity(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "quark-index.json"
            path.write_text(json.dumps({"shares": [
                {"share_id": "abcd1234", "files": [{"file_id": "same", "path": "a.mkv", "size": 1}]},
                {"share_id": "abcd1234", "files": [{"file_id": "same", "path": "b.mkv", "size": 1}]},
            ]}), encoding="utf-8")
            with mock.patch.dict(os.environ, {"SCRAPEFLOW_REPLENISHMENT_QUARK_INDEX": str(path)}):
                with self.assertRaisesRegex(ValueError, "file_id 冲突"):
                    adapter._search_quark_share({"media": {"tmdb_id": 42}}, set())

    def test_quark_reviewed_sfx_maps_as_archive_payload_only_when_explicit(self):
        request = {
            "media": {"title": "Example", "aliases": ["Example"], "tmdb_id": 42},
            "gaps": [{"id": "S03E01", "kind": "missing_episode", "season": 3}],
            "query_groups": [{"season": 3, "season_names": ["Season 3"]}],
        }
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "quark-index.json"
            row = {
                "share_id": "archive42", "tmdb_id": 42,
                "release_name": "Example Season 3 1080p",
                "payload_kind": "archive_payload", "requires_extraction": True,
                "archive_format": "sfx", "archive_password": "123456", "files": [{
                    "file_id": "archive-e01", "path": "Season 3/Example - 01 [1080p].exe",
                    "size": 123,
                }],
            }
            path.write_text(json.dumps({"shares": [row]}), encoding="utf-8")
            with mock.patch.dict(os.environ, {"SCRAPEFLOW_REPLENISHMENT_QUARK_INDEX": str(path)}):
                result = adapter._search_quark_share(request, set())
            self.assertEqual(result[0]["file_coverage"], ["S03E01"])
            self.assertEqual(result[0]["payload_kind"], "archive_payload")
            self.assertFalse(result[0]["video_files_verified"])
            self.assertEqual(result[0]["acquisition"]["archive_password"], "123456")
            self.assertEqual(
                result[0]["locator"], "quark_share:archive42:file:archive-e01",
            )
            self.assertEqual(result[0]["acquisition"]["expected_archives"][0], {
                "file_id": "archive-e01", "name": "Example - 01 [1080p].exe",
                "path": "Season 3/Example - 01 [1080p].exe", "size": 123,
                "gap_ids": ["S03E01"],
            })
            row.pop("payload_kind"); row.pop("requires_extraction"); row.pop("archive_format")
            path.write_text(json.dumps({"shares": [row]}), encoding="utf-8")
            with mock.patch.dict(os.environ, {"SCRAPEFLOW_REPLENISHMENT_QUARK_INDEX": str(path)}):
                self.assertEqual(adapter._search_quark_share(request, set()), [])

    def test_quark_sfx_password_marker_and_physical_candidates_are_bounded(self):
        self.assertEqual(
            adapter._archive_password_hint("如需要密码，默认密码是123456"),
            "123456",
        )
        self.assertEqual(adapter._archive_password_hint("解压密码看这里.txt"), "")
        request = {
            "media": {"title": "Example", "aliases": ["Example"], "tmdb_id": 42},
            "gaps": [
                {"id": "S03E01", "kind": "missing_episode", "season": 3},
                {"id": "S03E02", "kind": "missing_episode", "season": 3},
            ],
            "query_groups": [{"season": 3, "season_names": ["Season 3"]}],
        }
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "quark-index.json"
            path.write_text(json.dumps({"shares": [{
                "share_id": "archive42", "tmdb_id": 42,
                "release_name": "Example Season 3 1080p",
                "payload_kind": "archive_payload", "requires_extraction": True,
                "archive_format": "sfx", "files": [
                    {"file_id": "archive-e01", "path": "Example.S03E01.exe", "size": 101},
                    {"file_id": "archive-e02", "path": "Example.S03E02.exe", "size": 202},
                ],
            }]}), encoding="utf-8")
            with mock.patch.dict(os.environ, {
                "SCRAPEFLOW_REPLENISHMENT_QUARK_INDEX": str(path),
            }), mock.patch.object(
                adapter, "_discover_quark_share_archive_password", return_value="123456",
            ) as discover:
                result = adapter._search_quark_share(request, set())
        discover.assert_called_once_with("archive42", "")
        self.assertEqual(
            [row["locator"] for row in result],
            [
                "quark_share:archive42:file:archive-e01",
                "quark_share:archive42:file:archive-e02",
            ],
        )
        self.assertEqual(
            [row["file_coverage"] for row in result], [["S03E01"], ["S03E02"]],
        )
        self.assertTrue(all(
            row["acquisition"]["archive_password"] == "123456" for row in result
        ))

    def test_legacy_share_level_failure_does_not_blacklist_physical_sfx_files(self):
        request = {
            "media": {"title": "Example", "aliases": ["Example"], "tmdb_id": 42},
            "gaps": [
                {"id": "S03E01", "kind": "missing_episode", "season": 3},
                {"id": "S03E02", "kind": "missing_episode", "season": 3},
            ],
            "query_groups": [{"season": 3, "season_names": ["Season 3"]}],
        }
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "quark-index.json"
            path.write_text(json.dumps({"shares": [{
                "share_id": "archive42", "tmdb_id": 42,
                "release_name": "Example Season 3 1080p",
                "payload_kind": "archive_payload", "requires_extraction": True,
                "archive_format": "sfx", "files": [
                    {"file_id": "e01", "path": "Example.S03E01.exe", "size": 101},
                    {"file_id": "e02", "path": "Example.S03E02.exe", "size": 202},
                ],
            }]}), encoding="utf-8")
            with mock.patch.dict(os.environ, {
                "SCRAPEFLOW_REPLENISHMENT_QUARK_INDEX": str(path),
                "SCRAPEFLOW_REPLENISHMENT_CATALOG": str(Path(directory) / "missing.json"),
                "SCRAPEFLOW_REPLENISHMENT_ACG_SEARCH": "0",
            }), mock.patch.object(
                adapter, "_discover_quark_share_archive_password", return_value="",
            ):
                result = adapter._search({
                    **request,
                    "excluded_candidates": [{
                        "provider": "quark_share",
                        "locator": "quark_share:archive42",
                    }],
                })
        self.assertEqual(
            [row["locator"] for row in result["candidates"]],
            [
                "quark_share:archive42:file:e01",
                "quark_share:archive42:file:e02",
            ],
        )

    def test_quark_share_password_discovery_reads_names_without_saving_share(self):
        transport = mock.Mock()

        def request(_method, endpoint, *, params, body, cookie):
            self.assertEqual(cookie, "delegated-cookie")
            if endpoint.endswith("/share/sharepage/token"):
                return {"code": 0, "data": {"stoken": "share-token"}}
            self.assertTrue(endpoint.endswith("/share/sharepage/detail"))
            if params["pdir_fid"] == "0":
                return {"code": 0, "data": {"list": [
                    {
                        "file_name": "如需要密码，默认密码是123456",
                        "file": False, "fid": "hint-dir",
                    },
                    {"file_name": "Show.S03E01.exe", "file": True, "fid": "archive"},
                ]}}
            return {"code": 0, "data": {"list": []}}

        transport.request.side_effect = request
        session = mock.Mock(cookie="delegated-cookie")
        with mock.patch.object(adapter, "_alist_client", return_value=mock.Mock()), mock.patch.object(
            adapter, "delegated_quark_session", return_value=session,
        ), mock.patch.object(
            adapter, "UrlLibQuarkTransport", return_value=transport,
        ):
            password = adapter._discover_quark_share_archive_password("archive42")
        self.assertEqual(password, "123456")
        endpoints = [call.args[1] for call in transport.request.call_args_list]
        self.assertFalse(any(endpoint.endswith("/share/sharepage/save") for endpoint in endpoints))

    def test_quark_share_precedes_magnet_in_selection(self):
        request = {
            "media": {"title": "Example", "aliases": ["Example"], "tmdb_id": 42},
            "gaps": [{"id": "S01E01", "kind": "missing_episode", "season": 1}],
            "query_groups": [{"season": 1, "season_names": ["Season 1"]}],
        }
        from local.scrapeflow_api.replenishment import select_replenishment_candidates
        bundle = select_replenishment_candidates(request, [
            {"provider": "magnet", "release_name": "Example S01E01 2160p",
             "locator": "torrent:https://example.invalid/1", "files": ["Example S01E01.mkv"]},
            {"provider": "quark_share", "release_name": "Example S01E01 1080p",
             "locator": "quark_share:abcd1234", "files": ["Example S01E01.mkv"],
             "acquisition": {
                 "kind": "quark_fast_save",
                 "file_id_by_gap": {"S01E01": ["f1"]},
                 "file_path_by_id": {"f1": "Example S01E01.mkv"},
                 "file_size_by_id": {"f1": 1},
             }},
        ])
        self.assertEqual(bundle["selections"][0]["provider"], "quark_share")

    def test_quark_fast_save_without_executor_is_infrastructure_failure(self):
        wrapper = {"selection": {"selections": [{
            "provider": "quark_share",
            "selected_gap_ids": ["S01E01"],
            "acquisition": {
                "kind": "quark_fast_save",
                "share_id": "AbCd1234",
                "file_id_by_gap": {"S01E01": ["f1"]},
                "requires_share_revalidation": True,
            },
        }]}}
        with tempfile.TemporaryDirectory() as directory, self.assertRaises(
            adapter.ReplenishmentInfrastructureError,
        ) as raised:
            adapter._preflight(wrapper, Path(directory) / "preflight")
        self.assertEqual(raised.exception.failure_stage, "quark_fast_save_not_configured")
        self.assertFalse(getattr(raised.exception, "exclude_candidate", False))

    def test_search_excludes_failed_locator_and_infohash_before_dynamic_fallback(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "catalog.json"
            path.write_text(json.dumps({
                "projects": {"42": {"candidates": [
                    {"locator": "torrent:dead", "infohash": "DEAD", "release_name": "Dead"},
                    {"locator": "torrent:next", "infohash": "NEXT", "release_name": "Next"},
                ]}},
            }), encoding="utf-8")
            with mock.patch.dict(os.environ, {
                "SCRAPEFLOW_REPLENISHMENT_CATALOG": str(path),
                "SCRAPEFLOW_REPLENISHMENT_ACG_SEARCH": "0",
            }):
                result = adapter._search(self.unlock_local({
                    "media": {"tmdb_id": 42},
                    "excluded_candidates": [{"locator": "torrent:dead", "infohash": "dead"}],
                }))
        self.assertEqual([item["locator"] for item in result["candidates"]], ["torrent:next"])
        self.assertEqual(result["excluded_candidate_count"], 1)

    def test_search_treats_hex_and_base32_infohash_as_the_same_candidate(self):
        hex_hash = "0123456789abcdef0123456789abcdef01234567"
        base32_hash = adapter._base32_infohash(hex_hash)
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "catalog.json"
            path.write_text(json.dumps({
                "projects": {"42": {"candidates": [
                    {"locator": "torrent:new-url", "infohash": hex_hash},
                ]}},
            }), encoding="utf-8")
            with mock.patch.dict(os.environ, {
                "SCRAPEFLOW_REPLENISHMENT_CATALOG": str(path),
                "SCRAPEFLOW_REPLENISHMENT_ACG_SEARCH": "0",
            }):
                result = adapter._search({
                    "media": {"tmdb_id": 42},
                    "excluded_candidates": [{"infohash": base32_hash}],
                })
        self.assertEqual(result["candidates"], [])

    def test_search_exclusion_is_scoped_to_provider_lane(self):
        infohash = "0123456789abcdef0123456789abcdef01234567"
        cloud = {
            "provider": "quark_magnet", "locator": "quark_magnet:" + infohash,
            "infohash": infohash,
        }
        local = {
            "provider": "magnet", "locator": "torrent:https://fixture/release.torrent",
            "infohash": infohash,
        }
        with tempfile.TemporaryDirectory() as directory, mock.patch.dict(os.environ, {
            "SCRAPEFLOW_REPLENISHMENT_CATALOG": str(Path(directory) / "missing.json"),
            "SCRAPEFLOW_REPLENISHMENT_ACG_SEARCH": "1",
        }), mock.patch.object(
            adapter, "_search_nyaa", return_value=[cloud, local],
        ), mock.patch.object(adapter, "_search_acg", return_value=[]):
            result = adapter._search({
                "media": {"tmdb_id": 42},
                "excluded_candidates": [{
                    "provider": "magnet", "locator": local["locator"],
                    "infohash": infohash,
                }],
            })
        self.assertEqual(
            [(item["provider"], item["locator"]) for item in result["candidates"]],
            [("quark_magnet", cloud["locator"])],
        )

    def test_unlocked_local_lane_is_not_preexcluded_by_cloud_infohash(self):
        infohash = "0123456789abcdef0123456789abcdef01234567"
        cloud = {
            "provider": "quark_magnet", "locator": "quark_magnet:" + infohash,
            "infohash": infohash,
        }
        local = {
            "provider": "magnet", "locator": "torrent:https://fixture/release.torrent",
            "infohash": infohash,
        }
        observed_locators = []

        def nyaa(_request, locators, *, deadline):
            observed_locators.append(set(locators))
            return [cloud, local]

        with tempfile.TemporaryDirectory() as directory, mock.patch.dict(os.environ, {
            "SCRAPEFLOW_REPLENISHMENT_CATALOG": str(Path(directory) / "missing.json"),
            "SCRAPEFLOW_REPLENISHMENT_ACG_SEARCH": "1",
            "SCRAPEFLOW_REPLENISHMENT_PANSOU_URL": "",
        }), mock.patch.object(
            adapter, "_search_nyaa", side_effect=nyaa,
        ), mock.patch.object(adapter, "_search_acg", return_value=[]):
            result = adapter._search(self.unlock_local({
                "media": {"tmdb_id": 42},
                "excluded_candidates": [{
                    "provider": "quark_magnet", "locator": cloud["locator"],
                    "infohash": infohash,
                }],
            }))
        self.assertNotIn(cloud["locator"], observed_locators[0])
        self.assertEqual(
            [(item["provider"], item["locator"]) for item in result["candidates"]],
            [("magnet", local["locator"])],
        )

    def test_verified_metadata_mismatch_is_preexcluded_across_cloud_and_local(self):
        infohash = "0123456789abcdef0123456789abcdef01234567"
        cloud = {
            "provider": "quark_magnet", "locator": "quark_magnet:" + infohash,
            "infohash": infohash,
        }
        local = {
            "provider": "magnet", "locator": "torrent:https://fixture/release.torrent",
            "infohash": infohash,
        }
        observed_locators = []

        def nyaa(_request, locators, *, deadline):
            observed_locators.append(set(locators))
            return [cloud, local]

        with tempfile.TemporaryDirectory() as directory, mock.patch.dict(os.environ, {
            "SCRAPEFLOW_REPLENISHMENT_CATALOG": str(Path(directory) / "missing.json"),
            "SCRAPEFLOW_REPLENISHMENT_ACG_SEARCH": "1",
            "SCRAPEFLOW_REPLENISHMENT_PANSOU_URL": "",
        }), mock.patch.object(
            adapter, "_search_nyaa", side_effect=nyaa,
        ), mock.patch.object(adapter, "_search_acg", return_value=[]):
            result = adapter._search(self.unlock_local({
                "media": {"tmdb_id": 42},
                "excluded_candidates": [{
                    "provider": "quark_magnet", "locator": cloud["locator"],
                    "infohash": infohash,
                    "reason": "resource_inspected_without_requested_gap",
                }],
            }))

        self.assertIn(cloud["locator"], observed_locators[0])
        self.assertEqual(result["candidates"], [])

    def test_missing_verified_catalog_falls_back_to_dynamic_sources(self):
        dynamic = {
            "locator": "torrent:https://nyaa.si/download/42.torrent",
            "infohash": "0123456789abcdef0123456789abcdef01234567",
        }
        deadlines = []

        def nyaa(_request, _locators, *, deadline):
            deadlines.append(("nyaa", deadline))
            return [dynamic]

        def acg(_request, _locators, *, deadline):
            deadlines.append(("acg", deadline))
            return []

        with tempfile.TemporaryDirectory() as directory, mock.patch.dict(os.environ, {
            "SCRAPEFLOW_REPLENISHMENT_CATALOG": str(Path(directory) / "missing.json"),
            "SCRAPEFLOW_REPLENISHMENT_ACG_SEARCH": "1",
            "SCRAPEFLOW_REPLENISHMENT_DYNAMIC_SEARCH_TIMEOUT": "45",
        }), mock.patch.object(adapter, "_search_nyaa", side_effect=nyaa), mock.patch.object(
            adapter, "_search_acg", side_effect=acg,
        ), mock.patch.object(adapter.time, "monotonic", return_value=100.0):
            result = adapter._search({"media": {"tmdb_id": 42}})

        self.assertEqual(result["candidates"], [dynamic])
        self.assertIn("已核验候选目录不可用: ValueError", result["warnings"])
        self.assertEqual(deadlines, [("nyaa", 122.5), ("acg", 145.0)])

    def test_download_locator_treats_square_brackets_as_literal_filename_text(self):
        with tempfile.TemporaryDirectory() as directory:
            payload = Path(directory)
            media = payload / "[Provider] Example [01].mkv"
            media.write_bytes(b"abc")
            self.assertEqual(
                adapter._find_download(payload, "[Provider] Example [01].mkv", 3),
                media,
            )

    def test_failed_acquisition_removes_disposable_workspace(self):
        with tempfile.TemporaryDirectory() as directory:
            workspace = Path(directory) / "acquire-fixture"
            with mock.patch.object(adapter, "_preflight", side_effect=RuntimeError("no peers")):
                with self.assertRaisesRegex(RuntimeError, "no peers"):
                    adapter._acquire({
                        "request": {"media": {"tmdb_id": 42, "title": "Example"}},
                        "selection": {"selections": []},
                    }, workspace)
            self.assertFalse(workspace.exists())

    def test_cancelled_acquisition_removes_disposable_workspace(self):
        with tempfile.TemporaryDirectory() as directory:
            workspace = Path(directory) / "acquire-cancelled"
            with mock.patch.object(adapter, "_preflight", side_effect=KeyboardInterrupt):
                with self.assertRaises(KeyboardInterrupt):
                    adapter._acquire({
                        "request": {"media": {"tmdb_id": 42, "title": "Example"}},
                        "selection": {"selections": []},
                    }, workspace)
            self.assertFalse(workspace.exists())

    def test_remote_arrival_verification_waits_for_provider_visibility(self):
        class FakeClient:
            def __init__(self):
                self.calls = 0

            def list(self, _path, refresh=False):
                self.calls += 1
                self.assert_refresh = refresh
                if self.calls == 1:
                    return []
                return [{"name": "S01E01 - Example.mkv", "size": 123, "is_dir": False}]

        client = FakeClient()
        with mock.patch.object(adapter.time, "sleep") as sleep:
            adapter._verify_remote_uploads(client, "/remote", [{
                "remote_name": "S01E01 - Example.mkv", "size": 123,
            }])
        self.assertEqual(client.calls, 2)
        self.assertTrue(client.assert_refresh)
        sleep.assert_called_once()

    def test_upload_failure_is_persisted_and_never_retried(self):
        class FakeClient:
            def __init__(self):
                self.uploads = 0
                self.files = {}

            def exact_file_info(self, path):
                payload = self.files.get(path)
                if payload is None:
                    return None
                digest = __import__("hashlib").sha256(payload).hexdigest()
                return {"size": len(payload), "sha256": digest, "version": digest}

            def open_file_reader(self, path):
                return io.BytesIO(self.files[path])

            def upload_file(self, _target, _source, _content_type):
                self.uploads += 1
                raise RuntimeError("EntityTooSmall PartNumber=47")

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "video.mkv"
            source.write_bytes(b"verified-video")
            row = {
                "remote_name": "S03E01.mkv", "source": source,
                "size": source.stat().st_size,
            }
            client = FakeClient()
            with mock.patch(
                "engine.scrapeflow.local_upload_transaction.time.sleep",
            ):
                with self.assertRaises(adapter.ReplenishmentDeliveryError):
                    adapter._upload_with_retry(
                        client, "/remote", row,
                        transaction_root=root / "transactions",
                    )
                with self.assertRaises(adapter.ReplenishmentDeliveryError):
                    adapter._upload_with_retry(
                        client, "/remote", row,
                        transaction_root=root / "transactions",
                    )
            self.assertEqual(client.uploads, 1)
            journals = list((root / "transactions").rglob("journal.json"))
            self.assertEqual(len(journals), 1)
            self.assertEqual(json.loads(journals[0].read_text())["upload_calls"], 1)

    def test_upload_reuses_exact_remote_file_after_interrupted_attempt(self):
        class FakeClient:
            def __init__(self, payload):
                self.payload = payload
                self.uploads = 0

            def exact_file_info(self, _path):
                digest = __import__("hashlib").sha256(self.payload).hexdigest()
                return {"size": len(self.payload), "sha256": digest, "version": digest}

            def open_file_reader(self, _path):
                return io.BytesIO(self.payload)

            def upload_file(self, *_args):
                self.uploads += 1

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "video.mkv"
            source.write_bytes(b"already-remote")
            client = FakeClient(source.read_bytes())
            row = {
                "remote_name": "S03E01.mkv", "source": source,
                "size": source.stat().st_size,
            }
            adapter._upload_with_retry(
                client, "/remote", row,
                transaction_root=root / "transactions",
            )
            self.assertEqual(client.uploads, 0)
            self.assertRegex(row["sha256"], r"^[0-9a-f]{64}$")
            self.assertRegex(row["upload_receipt_sha256"], r"^[0-9a-f]{64}$")

    def test_provider_invalid_file_is_not_assumed_safe_to_retry(self):
        class FakeClient:
            def __init__(self):
                self.sources = []

            def exact_file_info(self, _path):
                return None

            def open_file_reader(self, _path):
                raise AssertionError("invisible target must not be read")

            def upload_file(self, _target, source, _content_type):
                self.sources.append(Path(source))
                raise RuntimeError("invalid file [非法文件不能上传]")

        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / "archive-01" / "output" / "episode.mkv"
            source.parent.mkdir(parents=True)
            source.write_bytes(b"original")
            row = {
                "remote_name": "S00E07.mkv", "source": source,
                "size": source.stat().st_size,
            }

            client = FakeClient()
            with mock.patch(
                "engine.scrapeflow.local_upload_transaction.time.sleep",
            ), self.assertRaises(adapter.ReplenishmentDeliveryError):
                adapter._upload_with_retry(
                    client, "/remote", row,
                    transaction_root=Path(directory) / "transactions",
                )

            self.assertEqual(client.sources[0], source.resolve())
            self.assertEqual(len(client.sources), 1)
            self.assertEqual(row["source"].read_bytes(), b"original")

    def test_provider_remux_is_reused_after_delivery_retry(self):
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / "archive-01" / "output" / "episode.mkv"
            source.parent.mkdir(parents=True)
            source.write_bytes(b"original")
            remux = adapter._provider_remux_path(source, "S00E07.mkv")
            remux.parent.mkdir(parents=True)
            remux.write_bytes(b"persisted-remux")
            row = {
                "remote_name": "S00E07.mkv", "source": source,
                "size": source.stat().st_size,
            }
            client = mock.Mock()
            remote_payload = remux.read_bytes()
            client.exact_file_info.side_effect = lambda _path: None
            client.open_file_reader.side_effect = lambda _path: io.BytesIO(remote_payload)

            def upload(_target, source_path, _content_type):
                nonlocal remote_payload
                remote_payload = Path(source_path).read_bytes()
                digest = __import__("hashlib").sha256(remote_payload).hexdigest()
                client.exact_file_info.side_effect = lambda _path: {
                    "size": len(remote_payload), "sha256": digest, "version": digest,
                }

            client.upload_file.side_effect = upload
            with mock.patch.object(
                adapter, "_ffprobe_archive_video", return_value={
                    "streams": [{"codec_type": "video"}],
                },
            ):
                adapter._upload_with_retry(
                    client, "/remote", row,
                    transaction_root=Path(directory) / "transactions",
                )
            client.upload_file.assert_called_once_with(
                "/remote/S00E07.mkv", remux.resolve(), "video/x-matroska",
            )
            self.assertEqual(row["source"], remux.resolve())
            self.assertEqual(row["size"], len(b"persisted-remux"))

    def test_complete_retained_payload_skips_aria_and_space_for_reused_bytes(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "acquire-fixture"
            payload = root / "download-01" / "payload"
            media = payload / "release" / "Episode.mkv"
            media.parent.mkdir(parents=True)
            media.write_bytes(b"abc")
            manifest = {
                "infohash": "a" * 40,
                "files": {1: {"path": "release/Episode.mkv", "size": 3}},
            }
            wrapper = {"selection": {"selections": [{
                "infohash": "a" * 40,
                "release_name": "fixture",
                "selected_gap_ids": ["S01E01"],
                "acquisition": {
                    "kind": "torrent",
                    "url": "https://example.invalid/item.torrent",
                    "file_index_by_gap": {"S01E01": [1]},
                    "file_size_by_index": {"1": 3},
                    "file_path_by_index": {"1": "release/Episode.mkv"},
                },
            }]}}
            with mock.patch.object(adapter, "_download_torrent", return_value=manifest), mock.patch.object(
                adapter.shutil, "which", return_value="/usr/bin/aria2c",
            ), mock.patch.object(adapter.shutil, "disk_usage", return_value=mock.Mock(free=2 * 1024 ** 3)):
                result = adapter._preflight(
                    wrapper, root / "preflight", resume_workspace=root,
                )
        self.assertEqual(result["reusable_bytes"], 3)
        self.assertEqual(result["remaining_bytes"], 0)
        self.assertEqual(result["required_bytes"], 1024 ** 3)

    def test_capacity_failure_preserves_complete_retained_payload(self):
        with tempfile.TemporaryDirectory() as directory:
            workspace = Path(directory) / "acquire-fixture"
            payload = workspace / "download-01" / "payload"
            media = payload / "release" / "Episode.mkv"
            media.parent.mkdir(parents=True)
            media.write_bytes(b"abc")
            selection = {
                "infohash": "a" * 40,
                "release_name": "fixture",
                "selected_gap_ids": ["S01E01"],
                "acquisition": {
                    "kind": "torrent", "url": "https://example.invalid/item.torrent",
                    "file_index_by_gap": {"S01E01": [1]},
                    "file_size_by_index": {"1": 3},
                    "file_path_by_index": {"1": "release/Episode.mkv"},
                },
            }
            wrapper = {
                "request": {"media": {"tmdb_id": 42, "title": "Example"}},
                "selection": {"selections": [selection]},
            }
            with mock.patch.object(
                adapter, "_preflight",
                side_effect=adapter.ReplenishmentInfrastructureError(
                    "capacity", stage="local_capacity",
                ),
            ):
                with self.assertRaises(adapter.ReplenishmentInfrastructureError):
                    adapter._acquire(wrapper, workspace)
            self.assertEqual(media.read_bytes(), b"abc")

    def test_workspace_lease_rejects_concurrent_same_selection(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with adapter._workspace_lease(root, "same-key"):
                with self.assertRaises(adapter.ReplenishmentInfrastructureError) as raised:
                    with adapter._workspace_lease(root, "same-key"):
                        pass
            self.assertEqual(raised.exception.failure_stage, "orchestration_concurrency")

    def test_visibility_failure_preserves_verified_payload_and_stage(self):
        with tempfile.TemporaryDirectory() as directory:
            workspace = Path(directory) / "acquire-fixture"
            payload = workspace / "download-01" / "payload"
            media = payload / "release" / "Episode.mkv"
            media.parent.mkdir(parents=True)
            media.write_bytes(b"abc")
            selection = {
                "infohash": "a" * 40,
                "release_name": "fixture",
                "selected_gap_ids": ["S01E01"],
                "acquisition": {
                    "kind": "torrent",
                    "url": "https://example.invalid/item.torrent",
                    "file_index_by_gap": {"S01E01": [1]},
                    "file_size_by_index": {"1": 3},
                    "file_path_by_index": {"1": "release/Episode.mkv"},
                },
            }
            manifest = {
                "infohash": "a" * 40,
                "files": {1: {"path": "release/Episode.mkv", "size": 3}},
            }
            wrapper = {
                "request": {"media": {"tmdb_id": 42, "title": "Example"}},
                "selection": {"selections": [selection]},
            }
            preflight = {"candidates": [{
                "torrent_path": str(workspace / "preflight" / "candidate-01.torrent"),
                "manifest": manifest,
            }]}
            client = mock.Mock()
            with mock.patch.object(adapter, "_preflight", return_value=preflight), mock.patch.object(
                adapter, "_alist_client", return_value=client,
            ), mock.patch.object(adapter, "_upload_with_retry"), mock.patch.object(
                adapter, "_verify_remote_uploads", side_effect=ValueError("not visible"),
            ):
                with self.assertRaises(adapter.ReplenishmentDeliveryError) as raised:
                    adapter._acquire(wrapper, workspace)
            self.assertEqual(raised.exception.failure_stage, "delivery_visibility")
            self.assertTrue(workspace.exists())

    def test_main_writes_structured_failure_artifact(self):
        with tempfile.TemporaryDirectory() as directory:
            selection = Path(directory) / "selection.json"
            output = Path(directory) / "acquisition.json"
            selection.write_text(json.dumps({
                "selection": {"selections": []}, "request": {},
            }), encoding="utf-8")
            with mock.patch.object(
                adapter, "_acquire_dispatch",
                side_effect=adapter.ReplenishmentDeliveryError(
                    "provider failed", stage="delivery_upload",
                ),
            ), mock.patch.dict(os.environ, {
                "SCRAPEFLOW_REPLENISHMENT_DOWNLOAD_DIR": directory,
            }), mock.patch.object(adapter.sys, "argv", [
                "adapter", "acquire", "--selection", str(selection),
                "--output", str(output),
            ]):
                with self.assertRaises(adapter.ReplenishmentDeliveryError):
                    adapter.main()
            artifact = json.loads(output.read_text(encoding="utf-8"))
        self.assertEqual(artifact["failure"]["scope"], "delivery")
        self.assertEqual(artifact["failure"]["stage"], "delivery_upload")
        self.assertTrue(artifact["failure"]["reusable_candidate"])

    def test_main_atomically_records_dispatch_validation_failure(self):
        with tempfile.TemporaryDirectory() as directory:
            selection = Path(directory) / "selection.json"
            output = Path(directory) / "acquisition.json"
            selection.write_text(json.dumps({
                "selection": {"selections": []}, "request": {},
            }), encoding="utf-8")
            with mock.patch.dict(os.environ, {
                "SCRAPEFLOW_REPLENISHMENT_DOWNLOAD_DIR": directory,
            }), mock.patch.object(adapter.sys, "argv", [
                "adapter", "acquire", "--selection", str(selection),
                "--output", str(output),
            ]):
                with self.assertRaises(adapter.ReplenishmentInfrastructureError):
                    adapter.main()
            artifact = json.loads(output.read_text(encoding="utf-8"))
            self.assertEqual(artifact["status"], "failed")
            self.assertEqual(artifact["failure"]["scope"], "infrastructure")
            self.assertEqual(artifact["failure"]["stage"], "artifact_validation")
            self.assertFalse(artifact["failure"]["exclude_candidate"])

    def test_selection_workspace_key_survives_round_change(self):
        selection = {
            "selection": {"selections": [{
                "locator": "torrent:fixture", "infohash": "ABC",
                "acquisition": {"file_index_by_gap": {"S03E01": [7]}},
            }]},
            "request": {"round": 1},
        }
        first = adapter._selection_workspace_key(selection)
        selection["request"]["round"] = 2
        self.assertEqual(adapter._selection_workspace_key(selection), first)

    def test_selection_workspace_key_distinguishes_quark_share_files(self):
        def wrapper(file_id):
            return {"selection": {"selections": [{
                "locator": "quark_share:same-share",
                "acquisition": {"file_id_by_gap": {"S00E01": [file_id]}},
            }]}}
        self.assertNotEqual(
            adapter._selection_workspace_key(wrapper("archive-01")),
            adapter._selection_workspace_key(wrapper("archive-02")),
        )

    def quark_selection(self):
        return {
            "provider": "quark_share", "release_name": "Example S01E01",
            "locator": "quark-share:fixture", "selected_gap_ids": ["S01E01"],
            "acquisition": {
                "kind": "quark_fast_save", "pwd_id": "fixture-share",
                "file_id_by_gap": {"S01E01": ["share-fid"]},
                "file_path_by_id": {"share-fid": "Season 01/Example.S01E01.mkv"},
                "file_size_by_id": {"share-fid": 123},
            },
        }

    def quark_sfx_selection(self, gap_count=2):
        gaps = [f"S03E{index:02d}" for index in range(1, gap_count + 1)]
        return {
            "provider": "quark_share", "release_name": "Example Season 3 SFX",
            "locator": "quark-share:sfx-fixture", "selected_gap_ids": gaps,
            "payload_kind": "archive_payload", "archive_format": "sfx",
            "requires_extraction": True,
            "acquisition": {
                "kind": "quark_sfx_archive", "pwd_id": "sfx-share",
                "payload_kind": "archive_payload", "archive_format": "sfx",
                "requires_extraction": True,
                "file_id_by_gap": {gap: ["archive-fid"] for gap in gaps},
                "file_path_by_id": {"archive-fid": "Example.Season3.exe"},
                "file_size_by_id": {"archive-fid": 10},
                "archive_member_by_gap": {
                    gap: f"Season 03/Example.{gap}.mkv" for gap in gaps
                },
                "archive_member_size_by_gap": {
                    gap: 100 + index for index, gap in enumerate(gaps, 1)
                },
            },
        }

    def quark_magnet_selection(self):
        return {
            "provider": "quark_magnet", "release_name": "Example S04E01",
            "locator": "magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567",
            "selected_gap_ids": ["S04E01"],
            "acquisition": {
                "kind": "quark_magnet_offline",
                "magnet_url": "magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567",
                "expected_files": [{
                    "path": "Example/Example.S04E01.mkv", "size": 456,
                    "gap_ids": ["S04E01"],
                }],
            },
        }

    def test_quark_only_bundle_fast_saves_then_uses_alist_arrival_verifier(self):
        client = mock.Mock()
        client.try_list.return_value = []
        wrapper = {
            "request": {"media": {"tmdb_id": 42, "title": "Example"}},
            "selection": {"selections": [self.quark_selection()]},
        }
        def receipt(_selection, destination, **_kwargs):
            return {
                "status": "submitted", "destination": destination,
                "expected_files": [{"name": "Example.S01E01.mkv", "size": 123, "gap_ids": ["S01E01"]}],
            }
        with mock.patch.object(adapter, "_alist_client", return_value=client), mock.patch.object(
            adapter, "_quark_fast_save_port", side_effect=receipt,
        ) as save, mock.patch.object(adapter, "_verify_remote_uploads") as verify:
            result = adapter._acquire_dispatch(wrapper, Path("/fixture/workspace"))
        self.assertEqual(result["status"], "ready")
        self.assertEqual(result["materializations"], ["fast_save"])
        save.assert_called_once()
        verify.assert_called_once()
        self.assertEqual(verify.call_args.args[2][0]["remote_name"], "Example.S01E01.mkv")

    def test_quark_retry_reuses_exact_arrival_without_resaving(self):
        client = mock.Mock()
        client.try_list.return_value = [{
            "name": "Example.S01E01.mkv", "size": 123, "is_dir": False,
        }]
        wrapper = {
            "request": {"media": {"tmdb_id": 42, "title": "Example"}},
            "selection": {"selections": [self.quark_selection()]},
        }
        with mock.patch.object(adapter, "_alist_client", return_value=client), mock.patch.object(
            adapter, "_quark_fast_save_port",
        ) as save, mock.patch.object(adapter, "_verify_remote_uploads"):
            result = adapter._acquire_dispatch(wrapper, Path("/fixture/workspace"))
        save.assert_not_called()
        self.assertEqual(result["materialized_files"], 0)

    def test_quark_share_task_checkpoint_is_atomic_and_resumed(self):
        selection = self.quark_selection()
        wrapper = {
            "request": {"media": {"tmdb_id": 42, "title": "Example"}},
            "selection": {"selections": [selection]},
        }
        client = mock.Mock()
        client.try_list.return_value = []
        resume_calls = []

        with tempfile.TemporaryDirectory() as directory:
            workspace = Path(directory)
            checkpoint_path = workspace / "share" / "task-01.json"

            def port(_selection, destination, **kwargs):
                resume_calls.append(kwargs.get("resume_task_id"))
                if kwargs.get("resume_task_id") is None:
                    kwargs["on_prepared"]()
                    prepared = json.loads(checkpoint_path.read_text())
                    self.assertEqual(prepared["status"], "prepared")
                    self.assertNotIn("task_id", prepared)
                    kwargs["on_submitted"]("share-task")
                return {
                    "status": "submitted", "destination": destination,
                    "task_id": kwargs.get("resume_task_id") or "share-task",
                    "expected_files": [{
                        "name": "Example.S01E01.mkv", "size": 123,
                        "gap_ids": ["S01E01"],
                    }],
                }

            with mock.patch.object(
                adapter, "_alist_client", return_value=client,
            ), mock.patch.object(
                adapter, "_quark_fast_save_port", side_effect=port,
            ), mock.patch.object(adapter, "_verify_remote_uploads"):
                adapter._acquire_dispatch(wrapper, workspace)
                submitted = json.loads(checkpoint_path.read_text())
                self.assertEqual(submitted["status"], "submitted")
                self.assertEqual(submitted["task_id"], "share-task")
                adapter._acquire_dispatch(wrapper, workspace)
        self.assertEqual(resume_calls, [None, "share-task"])

    def test_completed_empty_share_task_is_resubmitted_after_strong_absence_evidence(self):
        selection = self.quark_selection()
        wrapper = {
            "request": {"media": {"tmdb_id": 42, "title": "Example"}},
            "selection": {"selections": [selection]},
        }
        expected = [{
            "name": "Example.S01E01.mkv", "size": 123,
            "gap_ids": ["S01E01"],
        }]
        client = mock.Mock()
        client.try_list.return_value = []
        resume_calls = []

        with tempfile.TemporaryDirectory() as directory:
            workspace = Path(directory)
            checkpoint_path = workspace / "share" / "task-01.json"
            checkpoint_path.parent.mkdir(parents=True)
            remote_root = adapter.join_remote(
                "/quark/影视/ScrapeFlow/补源",
                adapter._safe_name(
                    "ScrapeFlow补源-42-Example-"
                    + adapter._selection_workspace_key(wrapper),
                ),
            )
            checkpoint_path.write_text(json.dumps({
                "version": 1, "status": "submitted", "task_id": "old-task",
                "destination": remote_root, "locator": selection["locator"],
                "share_id": "fixture-share",
                "selected_gap_ids": ["S01E01"],
                "expected_files": expected,
            }))

            def port(_selection, destination, **kwargs):
                resume_task_id = kwargs.get("resume_task_id")
                resume_calls.append(resume_task_id)
                if resume_task_id is None:
                    kwargs["on_prepared"]()
                    kwargs["on_submitted"]("replacement-task")
                return {
                    "status": "submitted", "destination": destination,
                    "task_id": resume_task_id or "replacement-task",
                    "task_status": 2,
                    "task_created_at": 900,
                    "task_finished_at": 1_000,
                    "expected_files": expected,
                }

            with mock.patch.dict(os.environ, {
                "SCRAPEFLOW_QUARK_FAST_SAVE_STALE_SECONDS": "900",
            }), mock.patch.object(
                adapter.time, "time", return_value=2_000,
            ), mock.patch.object(
                adapter, "_alist_client", return_value=client,
            ), mock.patch.object(
                adapter, "_quark_fast_save_port", side_effect=port,
            ), mock.patch.object(
                adapter, "_verify_remote_uploads",
                side_effect=[ValueError("not visible"), ValueError("not visible"), None],
            ):
                with self.assertRaises(adapter.ReplenishmentDeliveryError):
                    adapter._acquire_dispatch(wrapper, workspace)
                first = json.loads(checkpoint_path.read_text())
                self.assertEqual(first["status"], "completed_missing")
                self.assertEqual(first["missing_observations"], 1)

                with self.assertRaises(adapter.ReplenishmentDeliveryError):
                    adapter._acquire_dispatch(wrapper, workspace)
                second = json.loads(checkpoint_path.read_text())
                self.assertEqual(second["missing_observations"], 2)

                result = adapter._acquire_dispatch(wrapper, workspace)

            self.assertEqual(result["status"], "ready")
            replacement = json.loads(checkpoint_path.read_text())
            self.assertEqual(replacement["task_id"], "replacement-task")
            self.assertEqual(
                replacement["replaces_completed_missing"]["task_id"],
                "old-task",
            )
        self.assertEqual(resume_calls, ["old-task", "old-task", None])

    def test_quark_share_resume_keeps_original_task_subset_after_partial_arrival(self):
        selection = self.quark_selection()
        selection["selected_gap_ids"].append("S01E02")
        selection["acquisition"]["file_id_by_gap"]["S01E02"] = ["share-fid-2"]
        selection["acquisition"]["file_path_by_id"]["share-fid-2"] = (
            "Season 01/Example.S01E02.mkv"
        )
        selection["acquisition"]["file_size_by_id"]["share-fid-2"] = 456
        wrapper = {
            "request": {"media": {"tmdb_id": 42, "title": "Example"}},
            "selection": {"selections": [selection]},
        }
        expected = [
            {"name": "Example.S01E01.mkv", "size": 123, "gap_ids": ["S01E01"]},
            {"name": "Example.S01E02.mkv", "size": 456, "gap_ids": ["S01E02"]},
        ]
        client = mock.Mock()
        client.try_list.return_value = []
        resume_calls = []

        def port(_selection, destination, **kwargs):
            resume_calls.append(kwargs.get("resume_task_id"))
            if kwargs.get("resume_task_id") is None:
                kwargs["on_prepared"]()
                kwargs["on_submitted"]("two-file-task")
            return {
                "status": "submitted", "destination": destination,
                "task_id": kwargs.get("resume_task_id") or "two-file-task",
                "expected_files": expected,
            }

        with tempfile.TemporaryDirectory() as directory, mock.patch.object(
            adapter, "_alist_client", return_value=client,
        ), mock.patch.object(
            adapter, "_quark_fast_save_port", side_effect=port,
        ), mock.patch.object(adapter, "_verify_remote_uploads"):
            workspace = Path(directory)
            adapter._acquire_dispatch(wrapper, workspace)
            client.try_list.return_value = [{
                "name": "Example.S01E01.mkv", "size": 123, "is_dir": False,
            }]
            adapter._acquire_dispatch(wrapper, workspace)
        self.assertEqual(resume_calls, [None, "two-file-task"])

    def test_share_submit_in_doubt_blocks_resubmit_and_lower_lanes(self):
        share = self.quark_selection()
        wrapper = {
            "request": {"media": {"tmdb_id": 42, "title": "Example"}},
            "selection": {"selections": [
                share, self.quark_magnet_selection(), {
                    "provider": "magnet", "release_name": "Local fallback",
                    "locator": "magnet:local", "selected_gap_ids": ["S01E02"],
                    "acquisition": {"kind": "torrent"},
                },
            ]},
        }
        client = mock.Mock()
        client.try_list.return_value = []

        def uncertain(_selection, _destination, **kwargs):
            kwargs["on_prepared"]()
            raise adapter.QuarkBridgeError("connection lost during save")

        with tempfile.TemporaryDirectory() as directory, mock.patch.object(
            adapter, "_alist_client", return_value=client,
        ), mock.patch.object(
            adapter, "_quark_fast_save_port", side_effect=uncertain,
        ) as save, mock.patch.object(
            adapter, "_acquire_quark_magnet_with_recovery",
        ) as magnet, mock.patch.object(adapter, "_acquire") as local:
            workspace = Path(directory)
            with self.assertRaises(adapter.QuarkShareInDoubtError) as first:
                adapter._acquire_dispatch(wrapper, workspace)
            self.assertEqual(first.exception.failure_scope, "delivery")
            self.assertEqual(adapter._failure_lane_suppressions(wrapper, first.exception), [])
            with self.assertRaises(adapter.QuarkShareInDoubtError):
                adapter._acquire_dispatch(wrapper, workspace)
            self.assertEqual(save.call_count, 1)
            magnet.assert_not_called()
            local.assert_not_called()

            client.try_list.return_value = [{
                "name": "Example.S01E01.mkv", "size": 123, "is_dir": False,
            }]
            share_wrapper = adapter._wrapper_for_selections(wrapper, [share])
            with mock.patch.object(
                adapter, "_quark_fast_save_port",
            ) as resumed_save, mock.patch.object(adapter, "_verify_remote_uploads"):
                result = adapter._acquire_quark_bundle(
                    share_wrapper, workspace / "share",
                )
            resumed_save.assert_not_called()
            self.assertEqual(result["saved_files"], 0)

    def test_mixed_bundle_runs_quark_and_torrent_lanes_and_combines_sources(self):
        quark = self.quark_selection()
        torrent = {
            "provider": "magnet", "release_name": "Example S01E02",
            "selected_gap_ids": ["S01E02"], "acquisition": {"kind": "torrent"},
        }
        wrapper = {"request": {}, "selection": {"selections": [quark, torrent]}}
        with mock.patch.object(adapter, "_acquire_quark_bundle", return_value={
            "status": "ready", "source_paths": ["/quark/fast"],
            "materialization": "fast_save", "saved_files": 1, "saved_bytes": 123,
        }) as fast, mock.patch.object(adapter, "_acquire", return_value={
            "status": "ready", "source_paths": ["/quark/torrent"],
            "uploaded_files": 1, "uploaded_bytes": 456,
        }) as torrent_acquire:
            result = adapter._acquire_dispatch(wrapper, Path("/fixture/workspace"))
        self.assertEqual(result["source_paths"], ["/quark/fast", "/quark/torrent"])
        self.assertEqual(result["materialized_files"], 2)
        self.assertEqual(result["materialized_bytes"], 579)
        fast.assert_called_once()
        self.assertEqual(torrent_acquire.call_args.args[1], Path("/fixture/workspace/torrent"))

    def test_quark_candidate_failure_carries_precise_candidate_identity(self):
        from engine.scrapeflow.quark_fast_save_bridge import QuarkShareExpiredError
        client = mock.Mock(); client.try_list.return_value = []
        wrapper = {
            "request": {"media": {"tmdb_id": 42, "title": "Example"}},
            "selection": {"selections": [self.quark_selection()]},
        }
        error = QuarkShareExpiredError("expired")
        with mock.patch.object(adapter, "_alist_client", return_value=client), mock.patch.object(
            adapter, "_quark_fast_save_port", side_effect=error,
        ):
            with self.assertRaises(QuarkShareExpiredError) as raised:
                adapter._acquire_dispatch(wrapper, Path("/fixture/workspace"))
        self.assertEqual(raised.exception.candidate["locator"], "quark-share:fixture")
        self.assertTrue(raised.exception.exclude_candidate)

    def test_share_infrastructure_failure_temporarily_releases_magnet_lane(self):
        wrapper = {
            "request": {},
            "selection": {"selections": [self.quark_selection()]},
        }
        error = adapter.ReplenishmentInfrastructureError(
            "helper unavailable", stage="quark_fast_save_submit",
        )
        with mock.patch.dict(os.environ, {
            "SCRAPEFLOW_REPLENISHMENT_SHARE_COOLDOWN": "600",
        }), mock.patch.object(adapter.time, "time", return_value=1000):
            rows = adapter._failure_lane_suppressions(wrapper, error)
        self.assertEqual(rows, [{
            "provider": "quark_share",
            "locator": self.quark_selection()["locator"],
            "until_epoch": 1600,
            "reason": "quark_share_infrastructure_failure",
        }])

    def test_share_delivery_failure_never_releases_lower_lane(self):
        wrapper = {
            "request": {},
            "selection": {"selections": [self.quark_selection()]},
        }
        self.assertEqual(adapter._failure_lane_suppressions(
            wrapper,
            adapter.ReplenishmentDeliveryError(
                "arrival pending", stage="delivery_visibility",
            ),
        ), [])

    def test_quark_arrival_failure_stays_delivery_and_keeps_candidate(self):
        client = mock.Mock(); client.try_list.return_value = []
        wrapper = {
            "request": {"media": {"tmdb_id": 42, "title": "Example"}},
            "selection": {"selections": [self.quark_selection()]},
        }
        def receipt(_selection, destination, **_kwargs):
            return {
                "status": "submitted", "destination": destination,
                "expected_files": [{
                    "name": "Example.S01E01.mkv", "size": 123,
                    "gap_ids": ["S01E01"],
                }],
            }
        with mock.patch.object(adapter, "_alist_client", return_value=client), mock.patch.object(
            adapter, "_quark_fast_save_port", side_effect=receipt,
        ), mock.patch.object(
            adapter, "_verify_remote_uploads", side_effect=ValueError("not visible"),
        ):
            with self.assertRaises(adapter.ReplenishmentDeliveryError) as raised:
                adapter._acquire_dispatch(wrapper, Path("/fixture/workspace"))
        self.assertEqual(raised.exception.failure_stage, "delivery_visibility")
        self.assertTrue(raised.exception.reusable_candidate)
        self.assertFalse(getattr(raised.exception, "exclude_candidate", False))

    def test_sfx_member_manifest_rejects_path_traversal_as_candidate(self):
        selection = self.quark_sfx_selection(1)
        selection["acquisition"]["archive_member_by_gap"]["S03E01"] = "../escape.mkv"
        with self.assertRaises(adapter.ReplenishmentCandidateError) as raised:
            adapter._quark_archive_members(selection)
        self.assertEqual(raised.exception.failure_stage, "candidate_archive_manifest")
        self.assertTrue(raised.exception.exclude_candidate)

    def test_sfx_without_member_manifest_binds_one_gap_to_unique_video_only(self):
        selection = self.quark_sfx_selection(1)
        selection["acquisition"].pop("archive_member_by_gap")
        selection["acquisition"].pop("archive_member_size_by_gap")
        members = [
            {"path": "payload/episode.mkv", "size": 321, "is_dir": False},
            {"path": "payload/installer.exe", "size": 99, "is_dir": False},
        ]
        self.assertEqual(adapter._quark_archive_members(selection, members), [{
            "gap_id": "S03E01", "path": "payload/episode.mkv", "size": 321,
            "binding": "unique_video_member",
        }])

    def test_sfx_without_member_manifest_rejects_ambiguous_video_members(self):
        selection = self.quark_sfx_selection(1)
        selection["acquisition"].pop("archive_member_by_gap")
        selection["acquisition"].pop("archive_member_size_by_gap")
        members = [
            {"path": "episode-a.mkv", "size": 100, "is_dir": False},
            {"path": "episode-b.mkv", "size": 101, "is_dir": False},
        ]
        with self.assertRaises(adapter.ReplenishmentCandidateError) as raised:
            adapter._quark_archive_members(selection, members)
        self.assertEqual(raised.exception.failure_stage, "candidate_archive_manifest")

    def test_sfx_share_bundle_splits_fifteen_physical_archives(self):
        gaps = [f"S03E{index:02d}" for index in range(1, 16)]
        selection = {
            "provider": "quark_share", "locator": "quark_share:fixture",
            "selected_gap_ids": gaps,
            "payload_kind": "archive_payload", "archive_format": "sfx",
            "requires_extraction": True,
            "acquisition": {
                "kind": "quark_sfx_archive",
                "payload_kind": "archive_payload", "archive_format": "sfx",
                "requires_extraction": True,
                "file_id_by_gap": {gap: [f"fid-{gap}"] for gap in gaps},
                "file_path_by_id": {f"fid-{gap}": f"Show.{gap}.exe" for gap in gaps},
                "file_size_by_id": {f"fid-{gap}": 1000 for gap in gaps},
            },
        }
        children = adapter._split_quark_sfx_selection(selection)
        self.assertEqual(len(children), 15)
        self.assertEqual(children[0]["selected_gap_ids"], ["S03E01"])
        self.assertEqual(children[-1]["selected_gap_ids"], ["S03E15"])
        self.assertEqual(
            children[0]["locator"], "quark_share:unknown:file:fid-S03E01",
        )
        self.assertEqual(len({child["locator"] for child in children}), 15)
        self.assertEqual(
            set(children[-1]["acquisition"]["file_path_by_id"]), {"fid-S03E15"},
        )

    def test_sfx_is_listed_and_extracted_by_7z_without_running_exe(self):
        selection = self.quark_sfx_selection(2)
        selection["acquisition"]["archive_password"] = "123456"
        members = [
            {"path": row["path"], "size": row["size"], "is_dir": False}
            for row in adapter._quark_archive_members(selection)
        ]
        client = mock.Mock()

        def download(_remote, destination, *, expected_size):
            destination.write_bytes(b"A" * expected_size)

        client.download_file_to_path.side_effect = download

        def run(command, **_kwargs):
            output_arg = next(value for value in command if str(value).startswith("-o"))
            output = Path(str(output_arg)[2:])
            for member in members:
                target = output / member["path"]
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_bytes(b"V" * member["size"])
            return mock.Mock(returncode=0, stdout=b"")

        with tempfile.TemporaryDirectory() as directory, mock.patch.object(
            adapter.shutil, "which", side_effect=lambda name: "/fixture/7z" if name == "7z" else None,
        ), mock.patch.object(
            adapter, "_local_archive_listing", return_value=("fixture", members),
        ) as listing, mock.patch.object(
            adapter.subprocess, "run", side_effect=run,
        ) as invoke, mock.patch.object(
            adapter, "_ffprobe_archive_video", return_value={"streams": [{"codec_type": "video"}]},
        ) as probe:
            rows = adapter._extract_quark_sfx(
                client, selection, "/quark/staging", Path(directory),
            )
        self.assertEqual(len(rows), 2)
        command = invoke.call_args.args[0]
        self.assertEqual(command[:2], ["/fixture/7z", "x"])
        self.assertNotEqual(command[0], str(Path(directory) / "archive" / "Example.Season3.exe"))
        self.assertNotIn("123456", command)
        self.assertEqual(invoke.call_args.kwargs["input"], b"123456\n")
        self.assertEqual(listing.call_args.kwargs["archive_password"], "123456")
        self.assertEqual(probe.call_count, 2)

    def test_sfx_exact_local_archive_is_reused_on_retry(self):
        selection = self.quark_sfx_selection(1)
        member = adapter._quark_archive_members(selection)[0]
        members = [{"path": member["path"], "size": member["size"], "is_dir": False}]
        client = mock.Mock()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            archive = root / "archive" / "Example.Season3.exe"
            archive.parent.mkdir(); archive.write_bytes(b"A" * 10)

            def run(command, **_kwargs):
                output = Path(next(value for value in command if str(value).startswith("-o"))[2:])
                target = output / member["path"]
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_bytes(b"V" * member["size"])
                return mock.Mock(returncode=0, stdout=b"")

            with mock.patch.object(adapter.shutil, "which", return_value="/fixture/7z"), mock.patch.object(
                adapter, "_local_archive_listing", return_value=("fixture", members),
            ), mock.patch.object(adapter.subprocess, "run", side_effect=run), mock.patch.object(
                adapter, "_ffprobe_archive_video", return_value={},
            ):
                adapter._extract_quark_sfx(client, selection, "/quark/staging", root)
        client.download_file_to_path.assert_not_called()

    def test_rejected_sfx_payload_is_removed_from_local_cache(self):
        selection = self.quark_sfx_selection(1)
        wrapper = {
            "request": {"media": {"tmdb_id": 42, "title": "Example"}},
            "selection": {"selections": [selection]},
        }
        with tempfile.TemporaryDirectory() as directory:
            workspace = Path(directory) / "sfx"
            workspace.mkdir()
            (workspace / "retained.exe").write_bytes(b"bad")
            with mock.patch.object(adapter, "_acquire_quark_bundle", return_value={
                "source_paths": ["/quark/staging"],
            }), mock.patch.object(adapter, "_alist_client", return_value=mock.Mock()), mock.patch.object(
                adapter, "_index_canonical_validation_archives", return_value={},
            ), mock.patch.object(
                adapter, "_extract_quark_sfx",
                side_effect=adapter.ReplenishmentCandidateError("bad archive"),
            ):
                with self.assertRaises(adapter.ReplenishmentCandidateError):
                    adapter._acquire_quark_archive_bundle(wrapper, workspace)
            self.assertFalse(workspace.exists())

    def test_canonical_validation_archive_index_is_bounded_and_exact(self):
        client = mock.Mock()
        root = adapter.CANONICAL_VALIDATION_ROOT
        tree = {
            root: [{"name": "历史验证", "is_dir": True}],
            root + "/历史验证": [
                {"name": "optional", "is_dir": True},
                {"name": "note.json", "is_dir": False, "size": 20},
            ],
            root + "/历史验证/optional": [{
                "name": "Example.Season3.exe", "is_dir": False, "size": 10,
            }],
        }
        client.try_list.side_effect = lambda path, refresh=True: tree.get(path)
        self.assertEqual(adapter._index_canonical_validation_archives(client), {
            ("Example.Season3.exe", 10): [
                root + "/历史验证/optional/Example.Season3.exe",
            ],
        })

    def test_existing_canonical_sfx_skips_fast_save_then_extracts_and_verifies(self):
        selection = self.quark_sfx_selection(1)
        wrapper = {
            "request": {"media": {"tmdb_id": 42, "title": "Example"}},
            "selection": {"selections": [selection]},
        }
        validation_path = (
            adapter.CANONICAL_VALIDATION_ROOT
            + "/历史验证/optional/Example.Season3.exe"
        )
        uploaded = [{
            "gap_ids": ["S03E01"], "source": Path("/fixture/video.mkv"),
            "remote_name": "S03E01 - Example.mkv", "size": 101,
            "archive_name": "Example.Season3.exe",
            "member_path": "Season 03/Example.S03E01.mkv",
            "member_binding": "reviewed_member_manifest", "video_verified": True,
        }]
        client = mock.Mock()
        with tempfile.TemporaryDirectory() as directory, mock.patch.object(
            adapter, "_alist_client", return_value=client,
        ), mock.patch.object(
            adapter, "_index_canonical_validation_archives", return_value={
                ("Example.Season3.exe", 10): [validation_path],
            },
        ), mock.patch.object(
            adapter, "_acquire_quark_bundle",
        ) as acquire, mock.patch.object(
            adapter, "_extract_quark_sfx", return_value=uploaded,
        ) as extract, mock.patch.object(
            adapter, "_upload_with_retry",
        ), mock.patch.object(
            adapter, "_verify_remote_uploads",
        ):
            result = adapter._acquire_quark_archive_bundle(
                wrapper, Path(directory) / "workspace",
            )
        acquire.assert_not_called()
        self.assertEqual(extract.call_args.args[2], validation_path.rsplit("/", 1)[0])
        self.assertEqual(result["reused_validation_archives"], 1)
        self.assertEqual(result["acquired_archives"], 0)
        self.assertEqual(result["archive_sources"], [{
            "archive_name": "Example.Season3.exe",
            "source_path": validation_path,
            "source_kind": "existing_validation",
        }])
        self.assertTrue(result["verified_files"][0]["video_verified"])

    def test_missing_canonical_sfx_fast_saves_only_that_physical_archive(self):
        selection = self.quark_sfx_selection(1)
        wrapper = {
            "request": {"media": {"tmdb_id": 42, "title": "Example"}},
            "selection": {"selections": [selection]},
        }
        uploaded = [{
            "gap_ids": ["S03E01"], "source": Path("/fixture/video.mkv"),
            "remote_name": "S03E01 - Example.mkv", "size": 101,
            "archive_name": "Example.Season3.exe",
            "member_path": "Season 03/Example.S03E01.mkv",
            "member_binding": "reviewed_member_manifest", "video_verified": True,
        }]
        client = mock.Mock()
        with tempfile.TemporaryDirectory() as directory, mock.patch.object(
            adapter, "_alist_client", return_value=client,
        ), mock.patch.object(
            adapter, "_index_canonical_validation_archives", return_value={},
        ), mock.patch.object(
            adapter, "_acquire_quark_bundle", return_value={
                "source_paths": ["/quark/hidden-staging/one"],
            },
        ) as acquire, mock.patch.object(
            adapter, "_extract_quark_sfx", return_value=uploaded,
        ) as extract, mock.patch.object(
            adapter, "_upload_with_retry",
        ), mock.patch.object(
            adapter, "_verify_remote_uploads",
        ):
            result = adapter._acquire_quark_archive_bundle(
                wrapper, Path(directory) / "workspace",
            )
        acquire.assert_called_once()
        self.assertEqual(
            len(acquire.call_args.args[0]["selection"]["selections"]), 1,
        )
        self.assertEqual(extract.call_args.args[2], "/quark/hidden-staging/one")
        self.assertEqual(result["reused_validation_archives"], 0)
        self.assertEqual(result["acquired_archives"], 1)

    def test_mixed_sfx_bundle_reuses_present_archive_and_acquires_only_missing(self):
        gaps = ["S03E01", "S03E02"]
        selection = {
            "provider": "quark_share", "locator": "quark_share:mixed",
            "release_name": "Example mixed SFX", "selected_gap_ids": gaps,
            "payload_kind": "archive_payload", "archive_format": "sfx",
            "requires_extraction": True,
            "acquisition": {
                "kind": "quark_sfx_archive", "pwd_id": "mixed-share",
                "payload_kind": "archive_payload", "archive_format": "sfx",
                "requires_extraction": True,
                "file_id_by_gap": {"S03E01": ["a1"], "S03E02": ["a2"]},
                "file_path_by_id": {"a1": "Example.01.exe", "a2": "Example.02.exe"},
                "file_size_by_id": {"a1": 10, "a2": 20},
                "archive_member_by_gap": {
                    "S03E01": "Example.01.mkv", "S03E02": "Example.02.mkv",
                },
                "archive_member_size_by_gap": {"S03E01": 101, "S03E02": 102},
            },
        }
        wrapper = {
            "request": {"media": {"tmdb_id": 42, "title": "Example"}},
            "selection": {"selections": [selection]},
        }
        existing_path = adapter.CANONICAL_VALIDATION_ROOT + "/Example.01.exe"

        def extracted(_client, child, _root, _workspace):
            gap = child["selected_gap_ids"][0]
            number = int(gap[-2:])
            return [{
                "gap_ids": [gap], "source": Path(f"/fixture/{gap}.mkv"),
                "remote_name": f"{gap} - Example.mkv", "size": 100 + number,
                "archive_name": f"Example.{number:02d}.exe",
                "member_path": f"Example.{number:02d}.mkv",
                "member_binding": "reviewed_member_manifest", "video_verified": True,
            }]

        client = mock.Mock()
        with tempfile.TemporaryDirectory() as directory, mock.patch.object(
            adapter, "_alist_client", return_value=client,
        ), mock.patch.object(
            adapter, "_index_canonical_validation_archives", return_value={
                ("Example.01.exe", 10): [existing_path],
            },
        ), mock.patch.object(
            adapter, "_acquire_quark_bundle", return_value={
                "source_paths": ["/quark/hidden-staging/missing"],
            },
        ) as acquire, mock.patch.object(
            adapter, "_extract_quark_sfx", side_effect=extracted,
        ), mock.patch.object(adapter, "_upload_with_retry"), mock.patch.object(
            adapter, "_verify_remote_uploads",
        ):
            result = adapter._acquire_quark_archive_bundle(
                wrapper, Path(directory) / "workspace",
            )
        acquire.assert_called_once()
        acquired_child = acquire.call_args.args[0]["selection"]["selections"][0]
        self.assertEqual(acquired_child["selected_gap_ids"], ["S03E02"])
        self.assertEqual(result["reused_validation_archives"], 1)
        self.assertEqual(result["acquired_archives"], 1)
        self.assertEqual(
            [row["source_kind"] for row in result["archive_sources"]],
            ["existing_validation", "fast_save_staging"],
        )

    def test_ambiguous_canonical_sfx_blocks_duplicate_acquisition(self):
        selection = self.quark_sfx_selection(1)
        wrapper = {
            "request": {"media": {"tmdb_id": 42, "title": "Example"}},
            "selection": {"selections": [selection]},
        }
        client = mock.Mock()
        with tempfile.TemporaryDirectory() as directory, mock.patch.object(
            adapter, "_alist_client", return_value=client,
        ), mock.patch.object(
            adapter, "_index_canonical_validation_archives", return_value={
                ("Example.Season3.exe", 10): [
                    adapter.CANONICAL_VALIDATION_ROOT + "/a/Example.Season3.exe",
                    adapter.CANONICAL_VALIDATION_ROOT + "/b/Example.Season3.exe",
                ],
            },
        ), mock.patch.object(adapter, "_acquire_quark_bundle") as acquire:
            with self.assertRaises(adapter.ReplenishmentInfrastructureError) as raised:
                adapter._acquire_quark_archive_bundle(
                    wrapper, Path(directory) / "workspace",
                )
        self.assertEqual(
            raised.exception.failure_stage, "validation_archive_ambiguous",
        )
        acquire.assert_not_called()

    def test_validation_scan_failure_blocks_cloud_mutation(self):
        selection = self.quark_sfx_selection(1)
        wrapper = {
            "request": {"media": {"tmdb_id": 42, "title": "Example"}},
            "selection": {"selections": [selection]},
        }
        client = mock.Mock()
        client.try_list.side_effect = RuntimeError("AList unavailable")
        with tempfile.TemporaryDirectory() as directory, mock.patch.object(
            adapter, "_alist_client", return_value=client,
        ), mock.patch.object(adapter, "_acquire_quark_bundle") as acquire:
            with self.assertRaises(adapter.ReplenishmentInfrastructureError) as raised:
                adapter._acquire_quark_archive_bundle(
                    wrapper, Path(directory) / "workspace",
                )
        self.assertEqual(raised.exception.failure_stage, "validation_archive_scan")
        acquire.assert_not_called()

    def test_sfx_extracts_only_inferred_video_member_not_embedded_executable(self):
        selection = self.quark_sfx_selection(1)
        selection["acquisition"].pop("archive_member_by_gap")
        selection["acquisition"].pop("archive_member_size_by_gap")
        members = [
            {"path": "payload/episode.mkv", "size": 123, "is_dir": False},
            {"path": "payload/untrusted.exe", "size": 50, "is_dir": False},
        ]
        client = mock.Mock()
        client.download_file_to_path.side_effect = (
            lambda _remote, destination, *, expected_size:
            destination.write_bytes(b"A" * expected_size)
        )

        def run(command, **_kwargs):
            output = Path(next(value for value in command if str(value).startswith("-o"))[2:])
            target = output / "payload/episode.mkv"
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(b"V" * 123)
            return mock.Mock(returncode=0, stdout=b"")

        with tempfile.TemporaryDirectory() as directory, mock.patch.object(
            adapter.shutil, "which", return_value="/fixture/7z",
        ), mock.patch.object(
            adapter, "_local_archive_listing", return_value=("fixture", members),
        ), mock.patch.object(adapter.subprocess, "run", side_effect=run) as invoke, mock.patch.object(
            adapter, "_ffprobe_archive_video", return_value={"streams": [{"codec_type": "video"}]},
        ):
            rows = adapter._extract_quark_sfx(
                client, selection, "/quark/staging", Path(directory),
            )
        command = invoke.call_args.args[0]
        self.assertIn("payload/episode.mkv", command)
        self.assertNotIn("payload/untrusted.exe", command)
        self.assertEqual(rows[0]["member_binding"], "unique_video_member")

    def test_mixed_fifteen_sfx_and_three_torrent_gaps_combines_receipt(self):
        archive = self.quark_sfx_selection(15)
        torrent = {
            "provider": "magnet", "release_name": "Example remaining",
            "selected_gap_ids": ["S03E16", "S03E17", "S03E18"],
            "acquisition": {"kind": "torrent"},
        }
        wrapper = {"request": {}, "selection": {"selections": [archive, torrent]}}
        with mock.patch.object(adapter, "_acquire_quark_archive_bundle", return_value={
            "status": "ready", "source_paths": ["/quark/sfx"],
            "materialization": "sfx_extract_upload", "saved_files": 15,
            "saved_bytes": 1500,
            "archive_sources": [{
                "archive_name": "Show.S03E01.exe",
                "source_path": adapter.CANONICAL_VALIDATION_ROOT + "/Show.S03E01.exe",
                "source_kind": "existing_validation",
            }],
            "reused_validation_archives": 1, "acquired_archives": 14,
        }), mock.patch.object(adapter, "_acquire", return_value={
            "status": "ready", "source_paths": ["/quark/torrent"],
            "uploaded_files": 3, "uploaded_bytes": 300,
        }):
            result = adapter._acquire_dispatch(wrapper, Path("/fixture/workspace"))
        self.assertEqual(result["materialized_files"], 18)
        self.assertEqual(result["materializations"], ["sfx_extract_upload", "torrent_upload"])
        self.assertEqual(result["reused_validation_archives"], 1)
        self.assertEqual(result["acquired_archives"], 14)
        self.assertEqual(result["archive_sources"][0]["source_kind"], "existing_validation")

    def test_quark_magnet_submits_then_uses_exact_tree_arrival(self):
        selection = self.quark_magnet_selection()
        selection["infohash"] = "0123456789abcdef0123456789abcdef01234567"
        wrapper = {
            "request": {"media": {"tmdb_id": 42, "title": "Example"}},
            "selection": {"selections": [selection]},
        }
        client = mock.Mock(); client.walk.return_value = []

        def receipt(_selection, destination, **_kwargs):
            return {
                "status": "submitted", "destination": destination,
                "expected_files": selection["acquisition"]["expected_files"],
            }

        with tempfile.TemporaryDirectory() as directory, mock.patch.object(adapter, "_alist_client", return_value=client), mock.patch.object(
            adapter, "_quark_magnet_offline_port", side_effect=receipt,
        ) as submit, mock.patch.object(
            adapter, "_verify_offline_union_arrival",
            return_value=[{
                **selection["acquisition"]["expected_files"][0],
                "delivery_root": "/quark/offline",
            }],
        ) as verify:
            result = adapter._acquire_dispatch(wrapper, Path(directory))
        self.assertEqual(result["materializations"], ["cloud_offline"])
        submit.assert_called_once()
        verify.assert_called_once()

    def test_quark_magnet_retry_reuses_exact_tree_without_resubmit(self):
        selection = self.quark_magnet_selection()
        wrapper = {
            "request": {"media": {"tmdb_id": 42, "title": "Example"}},
            "selection": {"selections": [selection]},
        }
        client = mock.Mock()
        client.walk.return_value = [{
            "full_path": "/quark/ignored/Example/Example.S04E01.mkv",
            "name": "Example.S04E01.mkv", "size": 456, "is_dir": False,
        }]

        def snapshot(_client, _root):
            return {"Example/Example.S04E01.mkv": 456}

        with tempfile.TemporaryDirectory() as directory, mock.patch.object(adapter, "_alist_client", return_value=client), mock.patch.object(
            adapter, "_remote_tree_snapshot", side_effect=snapshot,
        ), mock.patch.object(adapter, "_quark_magnet_offline_port") as submit:
            result = adapter._acquire_dispatch(wrapper, Path(directory))
        submit.assert_not_called()
        self.assertEqual(result["materialized_files"], 1)

    def test_quark_magnet_reuses_thirteen_canonical_files_and_submits_only_five_indices(self):
        gaps = [
            *[f"S04E{index:02d}" for index in range(1, 15)],
            "S03E01", "S03E06", "S03E09", "S03E10",
        ]
        torrent_indices = [*range(41, 55), 58, 63, 66, 67]
        # Exact 092b identity: indices 41..54,58,63,66,67 under this BTIH
        # deterministically resolve to the already populated legacy root key.
        infohash = "b7115397a198554c17fd40caa2664fd981ff14fa"
        expected = [{
            "torrent_index": torrent_index,
            "path": f"Release/Example.{gap}.mkv",
            "size": 1000 + index,
            "gap_ids": [gap],
        } for index, (gap, torrent_index) in enumerate(
            zip(gaps, torrent_indices, strict=True), start=1,
        )]
        local = {
            "kind": "torrent", "url": "https://fixture/release.torrent",
            "file_index_by_gap": {
                gap: [torrent_index]
                for gap, torrent_index in zip(gaps, torrent_indices, strict=True)
            },
            "file_size_by_index": {
                str(torrent_index): 1000 + index
                for index, torrent_index in enumerate(torrent_indices, start=1)
            },
            "file_path_by_index": {
                str(torrent_index): f"Release/Example.{gap}.mkv"
                for gap, torrent_index in zip(gaps, torrent_indices, strict=True)
            },
        }
        selection = {
            "provider": "quark_magnet", "release_name": "Example complete",
            "locator": f"quark_magnet:{infohash}", "infohash": infohash,
            "fallback_locator": "torrent:https://fixture/release.torrent",
            "selected_gap_ids": gaps,
            "acquisition": {
                "kind": "quark_magnet_offline",
                "magnet_url": f"magnet:?xt=urn:btih:{infohash}",
                "expected_files": expected, "local_fallback": local,
            },
        }
        wrapper = {
            "request": {"media": {"tmdb_id": 34742, "title": "Example"}},
            "selection": {"selections": [selection]},
        }
        remote_parent = "/quark/影视/ScrapeFlow/补源"
        legacy_root = adapter._offline_legacy_root(
            wrapper, remote_parent, 34742, "Example",
        )
        offline_root = adapter.join_remote(
            remote_parent,
            adapter._safe_name(
                f"ScrapeFlow补源-34742-Example-{adapter._selection_workspace_key(wrapper)}-offline"
            ),
        )
        self.assertIsNotNone(legacy_root)
        self.assertTrue(str(legacy_root).endswith("-278afb6d4c67a748"))
        snapshots = {
            legacy_root: {
                adapter._offline_canonical_file(row)["name"]: row["size"]
                for row in expected[:13]
            },
            offline_root: {},
        }
        submitted = []

        def submit(partial, destination, **kwargs):
            self.assertEqual(destination, offline_root)
            partial_expected = partial["acquisition"]["expected_files"]
            submitted.extend(row["torrent_index"] for row in partial_expected)
            self.assertEqual(partial["selected_gap_ids"], sorted(gaps[13:]))
            self.assertEqual(len(partial_expected), 5)
            kwargs["on_submitted"]("partial-task")
            snapshots[offline_root].update({
                row["path"]: row["size"] for row in partial_expected
            })
            return {
                "status": "submitted", "destination": destination,
                "expected_files": partial_expected,
            }

        client = mock.Mock()
        with tempfile.TemporaryDirectory() as directory, mock.patch.object(
            adapter, "_alist_client", return_value=client,
        ), mock.patch.object(
            adapter, "_remote_tree_snapshot",
            side_effect=lambda _client, root: dict(snapshots.get(root, {})),
        ), mock.patch.object(
            adapter, "_quark_magnet_offline_port", side_effect=submit,
        ):
            result = adapter._acquire_quark_magnet_bundle(wrapper, Path(directory))
            checkpoint = json.loads(
                (Path(directory) / "task-01.json").read_text(encoding="utf-8")
            )
        self.assertEqual(submitted, [54, 58, 63, 66, 67])
        self.assertEqual(
            [row["torrent_index"] for row in checkpoint["expected_files"]],
            [54, 58, 63, 66, 67],
        )
        self.assertEqual(result["source_paths"], [legacy_root, offline_root])
        self.assertEqual(result["saved_files"], 18)
        self.assertEqual(result["reused_files"], 13)
        self.assertEqual(result["submitted_files"], 5)
        self.assertEqual(result["delivery_model"], "verified_union")

    def test_quark_magnet_does_not_reuse_canonical_name_with_wrong_size(self):
        selection = self.quark_magnet_selection()
        selection["infohash"] = "0123456789abcdef0123456789abcdef01234567"
        selection["fallback_locator"] = "torrent:https://fixture/release.torrent"
        selection["acquisition"]["local_fallback"] = {
            "kind": "torrent", "url": "https://fixture/release.torrent",
            "file_index_by_gap": {"S04E01": [7]},
            "file_size_by_index": {"7": 456},
            "file_path_by_index": {"7": "Example/Example.S04E01.mkv"},
        }
        wrapper = {
            "request": {"media": {"tmdb_id": 42, "title": "Example"}},
            "selection": {"selections": [selection]},
        }
        expected = selection["acquisition"]["expected_files"]
        canonical = adapter._offline_canonical_file(expected[0])
        client = mock.Mock()
        with mock.patch.object(
            adapter, "_remote_tree_snapshot",
            side_effect=[{}, {canonical["name"]: 455}],
        ):
            satisfied, missing = adapter._offline_union_status(
                client, "/legacy", "/offline", expected,
            )
        self.assertEqual(satisfied, [])
        self.assertEqual(missing, expected)

    def test_quark_magnet_accepts_unique_exact_path_below_provider_wrapper(self):
        expected = [{
            "path": "Season 03/Example.S03E01.mkv",
            "size": 456,
            "gap_ids": ["S03E01"],
        }]
        client = mock.Mock()
        wrapped = "Example Release(1)/Season 03/Example.S03E01.mkv"
        with mock.patch.object(
            adapter, "_remote_tree_snapshot",
            side_effect=[{wrapped: 456}, {}],
        ):
            satisfied, missing = adapter._offline_union_status(
                client, None, "/offline", expected,
            )
        self.assertEqual(missing, [])
        self.assertEqual(satisfied[0]["delivery_path"], wrapped)

    def test_quark_magnet_accepts_unique_provider_truncated_long_basename(self):
        expected_name = "A" * 104 + " [01234567].mkv"
        delivered = "Release/" + "A" * 104 + "....mkv"
        expected = [{
            "path": expected_name,
            "size": 456,
            "gap_ids": ["S00E09"],
        }]
        client = mock.Mock()
        with mock.patch.object(
            adapter, "_remote_tree_snapshot",
            side_effect=[{delivered: 456}, {}],
        ):
            satisfied, missing = adapter._offline_union_status(
                client, None, "/offline", expected,
            )
        self.assertEqual(missing, [])
        self.assertEqual(satisfied[0]["delivery_path"], delivered)

    def test_quark_magnet_rejects_short_or_ambiguous_ellipsis_delivery(self):
        expected = [{
            "path": "Example release with a descriptive ending.mkv",
            "size": 456,
            "gap_ids": ["S00E09"],
        }]
        client = mock.Mock()
        with mock.patch.object(
            adapter, "_remote_tree_snapshot",
            side_effect=[{"Example....mkv": 456}, {}],
        ):
            satisfied, missing = adapter._offline_union_status(
                client, None, "/offline", expected,
            )
        self.assertEqual(satisfied, [])
        self.assertEqual(missing, expected)

        expected_name = "A" * 104 + " [01234567].mkv"
        expected = [{
            "path": expected_name,
            "size": 456,
            "gap_ids": ["S00E09"],
        }]
        delivered = "A" * 104 + "....mkv"
        with mock.patch.object(
            adapter, "_remote_tree_snapshot",
            side_effect=[{
                "Release/" + delivered: 456,
                "Release(1)/" + delivered: 456,
            }, {}],
        ):
            satisfied, missing = adapter._offline_union_status(
                client, None, "/offline", expected,
            )
        self.assertEqual(satisfied, [])
        self.assertEqual(missing, expected)

    def test_quark_magnet_delivery_snapshot_scans_bonus_containers(self):
        client = mock.Mock()
        client.walk.return_value = [{
            "full_path": "/offline/Release/EXTRA/Example.SP03.mkv",
            "name": "Example.SP03.mkv",
            "size": 456,
            "is_dir": False,
        }]

        snapshot = adapter._remote_tree_snapshot(client, "/offline")

        client.walk.assert_called_once_with(
            "/offline",
            refresh=True,
            include_bonus=True,
            include_title_extras=True,
        )
        self.assertEqual(snapshot, {
            "Release/EXTRA/Example.SP03.mkv": 456,
        })

    def test_quark_magnet_rejects_ambiguous_wrapped_exact_paths(self):
        expected = [{
            "path": "Season 03/Example.S03E01.mkv",
            "size": 456,
            "gap_ids": ["S03E01"],
        }]
        client = mock.Mock()
        with mock.patch.object(
            adapter, "_remote_tree_snapshot",
            side_effect=[{
                "Release/Season 03/Example.S03E01.mkv": 456,
                "Release(1)/Season 03/Example.S03E01.mkv": 456,
            }, {}],
        ):
            satisfied, missing = adapter._offline_union_status(
                client, None, "/offline", expected,
            )
        self.assertEqual(satisfied, [])
        self.assertEqual(missing, expected)

    def test_quark_magnet_delivery_failure_never_uses_local_fallback(self):
        selection = self.quark_magnet_selection()
        selection["infohash"] = "0123456789abcdef0123456789abcdef01234567"
        selection["fallback_locator"] = "torrent:https://fixture/release.torrent"
        selection["acquisition"]["local_fallback"] = {
            "kind": "torrent", "url": "https://fixture/release.torrent",
            "file_index_by_gap": {"S04E01": [1]},
            "file_size_by_index": {"1": 456},
            "file_path_by_index": {"1": "Example/Example.S04E01.mkv"},
        }
        wrapper = {"request": {}, "selection": {"selections": [selection]}}
        error = adapter.ReplenishmentDeliveryError(
            "provider timeout", stage="quark_magnet_progress",
        )
        with mock.patch.dict(os.environ, {
            "SCRAPEFLOW_REPLENISHMENT_OFFLINE_RECOVERY_ATTEMPTS": "1",
        }), mock.patch.object(
            adapter, "_acquire_quark_magnet_bundle",
            side_effect=error,
        ), mock.patch.object(adapter, "_acquire", return_value={
            "status": "ready", "source_paths": ["/quark/local"],
            "uploaded_files": 1, "uploaded_bytes": 456,
        }) as local:
            with self.assertRaises(adapter.ReplenishmentDeliveryError):
                adapter._acquire_dispatch(wrapper, Path("/fixture/workspace"))
        local.assert_not_called()

    def test_quark_magnet_retries_cloud_in_place_before_returning_ready(self):
        selection = self.quark_magnet_selection()
        selection["infohash"] = "0123456789abcdef0123456789abcdef01234567"
        selection["fallback_locator"] = "torrent:https://fixture/release.torrent"
        selection["acquisition"]["local_fallback"] = {
            "kind": "torrent", "url": "https://fixture/release.torrent",
            "file_index_by_gap": {"S04E01": [1]},
            "file_size_by_index": {"1": 456},
            "file_path_by_index": {"1": "Example/Example.S04E01.mkv"},
        }
        wrapper = {"request": {}, "selection": {"selections": [selection]}}
        ready = {
            "status": "ready", "source_paths": ["/quark/offline"],
            "materialization": "cloud_offline", "saved_files": 1,
            "saved_bytes": 456,
        }
        with tempfile.TemporaryDirectory() as directory, mock.patch.dict(os.environ, {
            "SCRAPEFLOW_REPLENISHMENT_OFFLINE_RECOVERY_ATTEMPTS": "3",
            "SCRAPEFLOW_REPLENISHMENT_OFFLINE_RECOVERY_DELAY": "1",
        }), mock.patch.object(
            adapter, "_acquire_quark_magnet_bundle",
            side_effect=[adapter.ReplenishmentDeliveryError("transient"), ready],
        ) as cloud, mock.patch.object(adapter.time, "sleep") as sleep, mock.patch.object(
            adapter, "_acquire",
        ) as local:
            result = adapter._acquire_dispatch(wrapper, Path(directory))
        self.assertEqual(cloud.call_count, 2)
        sleep.assert_called_once_with(1)
        local.assert_not_called()
        self.assertEqual(result["materializations"], ["cloud_offline"])

    def test_quark_magnet_infrastructure_failure_returns_without_local_fallback(self):
        selection = self.quark_magnet_selection()
        selection["locator"] = "quark_magnet:0123456789abcdef0123456789abcdef01234567"
        selection["infohash"] = "0123456789abcdef0123456789abcdef01234567"
        selection["fallback_locator"] = "torrent:https://fixture/release.torrent"
        selection["acquisition"]["local_fallback"] = {
            "kind": "torrent", "url": "https://fixture/release.torrent",
            "file_index_by_gap": {"S04E01": [1]},
            "file_size_by_index": {"1": 456},
            "file_path_by_index": {"1": "Example/Example.S04E01.mkv"},
        }
        wrapper = {"request": {}, "selection": {"selections": [selection]}}
        error = adapter.ReplenishmentInfrastructureError(
            "native helper unavailable", stage="quark_magnet_submit",
        )
        with tempfile.TemporaryDirectory() as directory, mock.patch.dict(os.environ, {
            "SCRAPEFLOW_REPLENISHMENT_OFFLINE_RECOVERY_ATTEMPTS": "1",
        }), mock.patch.object(
            adapter, "_acquire_quark_magnet_bundle", side_effect=error,
        ), mock.patch.object(adapter, "_acquire") as local:
            with self.assertRaises(adapter.ReplenishmentInfrastructureError) as raised:
                adapter._acquire_dispatch(wrapper, Path(directory))
        local.assert_not_called()
        self.assertIs(raised.exception, error)
        self.assertEqual(raised.exception.failure_lane, "quark_magnet")

    def test_first_of_two_qmag_candidates_cools_exact_locator_before_torrent(self):
        first = self.quark_magnet_selection()
        first["locator"] = "quark_magnet:first"
        second = self.quark_magnet_selection()
        second["locator"] = "quark_magnet:second"
        wrapper = {
            "request": {},
            "selection": {
                "selections": [first],
                "provider_chain_by_gap": {
                    "S04E01": [
                        {"provider": "quark_magnet", "locator": first["locator"]},
                        {"provider": "quark_magnet", "locator": second["locator"]},
                        {"provider": "magnet", "locator": "torrent:https://fixture/local"},
                    ],
                },
            },
        }
        error = adapter.ReplenishmentInfrastructureError(
            "native helper unavailable", stage="quark_magnet_submit",
        )
        with mock.patch.dict(os.environ, {
            "SCRAPEFLOW_REPLENISHMENT_OFFLINE_COOLDOWN": "600",
        }), mock.patch.object(adapter.time, "time", return_value=1000):
            suppressions = adapter._failure_lane_suppressions(wrapper, error)
        self.assertEqual(suppressions, [{
            "provider": "quark_magnet",
            "locator": first["locator"],
            "until_epoch": 1600,
            "reason": "quark_magnet_infrastructure_failure",
        }])
        self.assertNotIn(second["locator"], {
            row["locator"] for row in suppressions
        })

    def test_quark_magnet_in_doubt_never_starts_local_fallback(self):
        from engine.scrapeflow.quark_fast_save_bridge import QuarkMagnetInDoubtError
        selection = self.quark_magnet_selection()
        wrapper = {"request": {}, "selection": {"selections": [selection]}}
        with tempfile.TemporaryDirectory() as directory, mock.patch.object(
            adapter, "_acquire_quark_magnet_bundle",
            side_effect=QuarkMagnetInDoubtError("unknown submit"),
        ), mock.patch.object(adapter, "_acquire") as local:
            with self.assertRaises(QuarkMagnetInDoubtError):
                adapter._acquire_dispatch(wrapper, Path(directory))
        local.assert_not_called()

    def test_persisted_partial_submit_in_doubt_blocks_changed_subset_and_local_fallback(self):
        from engine.scrapeflow.quark_fast_save_bridge import QuarkMagnetInDoubtError
        selection = self.quark_magnet_selection()
        selection["infohash"] = "0123456789abcdef0123456789abcdef01234567"
        selection["fallback_locator"] = "torrent:https://fixture/release.torrent"
        selection["acquisition"]["local_fallback"] = {
            "kind": "torrent", "url": "https://fixture/release.torrent",
            "file_index_by_gap": {"S04E01": [7]},
            "file_size_by_index": {"7": 456},
            "file_path_by_index": {"7": "Example/Example.S04E01.mkv"},
        }
        wrapper = {
            "request": {"media": {"tmdb_id": 42, "title": "Example"}},
            "selection": {"selections": [selection]},
        }
        client = mock.Mock()
        with tempfile.TemporaryDirectory() as directory:
            workspace = Path(directory)
            state = workspace / "offline/recovery-state.json"
            state.parent.mkdir()
            state.write_text(json.dumps({
                "version": 1, "status": "submit_in_doubt",
            }), encoding="utf-8")
            with mock.patch.object(
                adapter, "_alist_client", return_value=client,
            ), mock.patch.object(
                adapter, "_remote_tree_snapshot", return_value={},
            ), mock.patch.object(
                adapter, "_quark_magnet_offline_port",
            ) as submit, mock.patch.object(adapter, "_acquire") as local:
                with self.assertRaises(QuarkMagnetInDoubtError):
                    adapter._acquire_dispatch(wrapper, workspace)
        submit.assert_not_called()
        local.assert_not_called()

    def test_magnet_port_uses_configured_native_helper_transport(self):
        selection = self.quark_magnet_selection()
        with mock.patch.dict(os.environ, {
            "SCRAPEFLOW_QUARK_HELPER_URL": "http://host.docker.internal:18765",
            "SCRAPEFLOW_QUARK_HELPER_TOKEN": "x" * 24,
        }), mock.patch.object(adapter, "QuarkNativeHelperTransport") as transport, mock.patch.object(
            adapter, "QuarkMagnetOfflineBridge",
        ) as bridge:
            bridge.return_value.dry_run.return_value = {"status": "dry_run"}
            adapter._quark_magnet_offline_port(selection, "/quark", dry_run=True)
        transport.assert_called_once()
        self.assertTrue(transport.call_args.kwargs["passive_only"])
        bridge.assert_called_once_with(transport.return_value)

    def test_quark_magnet_task_checkpoint_is_atomic_and_resumed(self):
        selection = self.quark_magnet_selection()
        wrapper = {
            "request": {"media": {"tmdb_id": 42, "title": "Example"}},
            "selection": {"selections": [selection]},
        }
        client = mock.Mock()
        calls = []

        def port(_selection, destination, **kwargs):
            calls.append(kwargs.get("resume_task_id"))
            if kwargs.get("resume_task_id") is None:
                kwargs["on_submitted"]("task-checkpoint")
            return {
                "status": "submitted", "destination": destination,
                "expected_files": selection["acquisition"]["expected_files"],
            }

        with tempfile.TemporaryDirectory() as directory, mock.patch.object(
            adapter, "_alist_client", return_value=client,
        ), mock.patch.object(
            adapter, "_offline_union_status",
            side_effect=lambda _client, _legacy, _root, expected: ([], expected),
        ), mock.patch.object(
            adapter, "_verify_offline_union_arrival",
            return_value=[{
                **selection["acquisition"]["expected_files"][0],
                "delivery_root": "/quark/offline",
            }],
        ), mock.patch.object(adapter, "_quark_magnet_offline_port", side_effect=port):
            workspace = Path(directory)
            adapter._acquire_quark_magnet_bundle(wrapper, workspace)
            checkpoint = json.loads((workspace / "task-01.json").read_text())
            self.assertEqual(checkpoint["task_id"], "task-checkpoint")
            adapter._acquire_quark_magnet_bundle(wrapper, workspace)
        self.assertEqual(calls, [None, "task-checkpoint"])

    def test_dynamic_search_uses_single_attempt_with_bounded_request_timeout(self):
        page = b'<a href="/t/42">Example S01E01 1080p</a>'
        with mock.patch.object(adapter, "_fetch_bytes", return_value=page) as fetch:
            with mock.patch.object(adapter, "_download_torrent", side_effect=RuntimeError("fixture")):
                with mock.patch.dict(os.environ, {
                    "SCRAPEFLOW_REPLENISHMENT_DYNAMIC_SEARCH_TIMEOUT": "45",
                }):
                    result = adapter._search_acg({
                        "media": {"title": "Example", "aliases": []},
                        "gaps": [{"id": "S01E01", "kind": "missing_episode", "season": 1}],
                    }, set())
        self.assertEqual(result, [])
        self.assertEqual(fetch.call_args.kwargs["attempts"], 1)
        self.assertLessEqual(fetch.call_args.kwargs["timeout"], 12)

    def test_acg_failure_exposes_stable_source_health_diagnostic(self):
        refused = RuntimeError("HTTP read failed")
        refused.__cause__ = ConnectionRefusedError("fixture")
        with mock.patch.object(adapter, "_fetch_bytes", side_effect=refused):
            result = adapter._search_acg({
                "media": {"title": "Example", "aliases": []},
                "gaps": [{"id": "S01E01", "kind": "missing_episode", "season": 1}],
            }, set(), deadline=time.monotonic() + 30)

        self.assertGreater(result.query_attempts, 0)
        self.assertEqual(result.query_responses, 0)
        self.assertFalse(result.source_exhausted)
        self.assertEqual(
            result.infrastructure_failure_types,
            {"connection_refused": result.query_attempts},
        )

    def test_acg_uses_credential_free_per_source_proxy(self):
        proxy = mock.Mock()
        page = b'<a href="/t/42">Example S01E01 1080p</a>'
        with mock.patch.dict(os.environ, {
            "SCRAPEFLOW_REPLENISHMENT_ACG_PROXY": "http://host.docker.internal:7897",
        }), mock.patch.object(
            adapter, "_acg_http_opener", return_value=proxy,
        ), mock.patch.object(
            adapter, "_fetch_bytes", return_value=page,
        ) as fetch, mock.patch.object(
            adapter, "_download_torrent", side_effect=RuntimeError("fixture"),
        ) as download:
            adapter._search_acg({
                "media": {"title": "Example", "aliases": []},
                "gaps": [{"id": "S01E01", "kind": "missing_episode", "season": 1}],
            }, set(), deadline=time.monotonic() + 30)
        self.assertIs(fetch.call_args.kwargs["opener"], proxy)
        self.assertIs(download.call_args.kwargs["opener"], proxy)

        with mock.patch.dict(os.environ, {
            "SCRAPEFLOW_REPLENISHMENT_ACG_PROXY": "http://user:secret@proxy.invalid:7897",
        }):
            with self.assertRaisesRegex(ValueError, "credential-free"):
                adapter._acg_http_opener()

    def test_acg_outage_is_diagnostic_but_never_a_required_source(self):
        request = {
            "media": {"title": "Example", "tmdb_id": 42},
            "rules": {"minimum_attempts_per_cloud_lane": 30},
            "provider_attempts": {"quark_share": 30, "quark_magnet": 0},
        }
        acg_failure = adapter._DynamicSearchResult(
            [], query_attempts=4, query_responses=0,
            infrastructure_failure_types={"connection_refused": 4},
        )
        with tempfile.TemporaryDirectory() as directory:
            catalog = Path(directory) / "catalog.json"
            index = Path(directory) / "quark-index.json"
            catalog.write_text(json.dumps({
                "verified_at": "2026-07-29T00:00:00Z",
                "projects": {"42": {"candidates": []}},
            }), encoding="utf-8")
            index.write_text(json.dumps({"shares": []}), encoding="utf-8")
            with mock.patch.dict(os.environ, {
                "SCRAPEFLOW_REPLENISHMENT_CATALOG": str(catalog),
                "SCRAPEFLOW_REPLENISHMENT_QUARK_INDEX": str(index),
                "SCRAPEFLOW_REPLENISHMENT_ACG_SEARCH": "1",
            }), mock.patch.object(
                adapter, "_search_nyaa", return_value=adapter._DynamicSearchResult(
                    [], query_attempts=1, query_responses=1,
                ),
            ), mock.patch.object(adapter, "_search_acg", return_value=acg_failure):
                result = adapter._search(request)

        self.assertFalse(result["magnet_discovery"]["sources"]["ACG"]["required"])
        self.assertEqual(
            result["magnet_discovery"]["sources"]["ACG"]["infrastructure_failure_types"],
            {"connection_refused": 4},
        )
        self.assertIn(
            "ACG 动态搜索基础设施故障: connection_refused=4",
            result["warnings"],
        )

    def test_subsplease_builds_verified_cloud_only_candidate(self):
        magnet = (
            "magnet:?xt=urn:btih:" + "a" * 40
            + "&dn=%5BSubsPlease%5D%20Example%20S4%20-%2016%20%281080p%29.mkv"
            + "&xl=123456789&tr=https%3A%2F%2Ftracker.invalid%2Fannounce"
        )
        payload = json.dumps({
            "Example S4 - 16": {
                "episode": "16", "downloads": [{"res": "1080", "magnet": magnet}],
            },
        }).encode()
        request = {
            "media": {"title": "Example", "aliases": ["Example"]},
            "gaps": [{"id": "S04E16", "kind": "missing_episode", "season": 4}],
            "query_groups": [{"season": 4, "season_names": ["Season 4"]}],
        }
        with mock.patch.object(adapter, "_fetch_bytes", return_value=payload):
            result = adapter._search_subsplease(
                request, set(), deadline=time.monotonic() + 30,
            )

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["provider"], "quark_magnet")
        self.assertEqual(result[0]["file_coverage"], ["S04E16"])
        self.assertEqual(result[0]["acquisition"]["magnet_url"], magnet)
        self.assertNotIn("local_fallback", result[0]["acquisition"])
        self.assertNotIn("fallback_locator", result[0])
        self.assertTrue(result.source_exhausted)

    def test_dmhy_builds_candidate_from_verified_detail_torrent(self):
        infohash = "a" * 40
        rss = f"""<?xml version='1.0' encoding='UTF-8'?>
        <rss><channel><item>
          <title>Re 从零开始的休息时间 [73]</title>
          <link>http://share.dmhy.org/topics/view/123_release.html</link>
          <enclosure url="magnet:?xt=urn:btih:{infohash}" />
        </item></channel></rss>""".encode()
        detail = b"""<html><body>
          <a href="http://dl.dmhy.org/2026/08/04/verified.torrent">download</a>
        </body></html>"""
        manifest = {
            "root": "Re Zero Break Time", "infohash": infohash,
            "files": {1: {
                "path": "Re 从零开始的休息时间 73.mp4", "size": 100,
            }},
        }
        request = {
            "media": {"title": "Re:Zero", "aliases": ["Re:Zero"]},
            "gaps": [{
                "id": "S00E73", "season": 0, "episodes": [73],
                "kind": "missing_episode", "label": "S00E73",
            }],
            "query_groups": [{
                "season": 0, "season_names": ["Specials"],
                "episode_titles": [
                    "Re:从零开始的休息时间 4th 有言必行备忘录#3",
                ],
            }],
            "rules": {"optional_discovery_only": True},
        }

        def fetch(url, **_kwargs):
            return detail if "/topics/view/" in url else rss

        with mock.patch.object(adapter, "_fetch_bytes", side_effect=fetch), \
                mock.patch.object(
                    adapter, "_download_torrent", return_value=manifest,
                ) as download:
            result = adapter._search_dmhy(
                request, set(), deadline=time.monotonic() + 30,
            )

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["provider"], "quark_magnet")
        self.assertEqual(result[0]["file_coverage"], ["S00E73"])
        self.assertTrue(result.source_exhausted)
        self.assertEqual(result.query_attempts, result.query_responses)
        self.assertEqual(
            download.call_args.args[0],
            "https://dl.dmhy.org/2026/08/04/verified.torrent",
        )

    def test_subsplease_regular_season_never_satisfies_tmdb_special(self):
        magnet = (
            "magnet:?xt=urn:btih:" + "b" * 40
            + "&dn=%5BSubsPlease%5D%20Example%20S4%20-%2016%20%281080p%29.mkv"
            + "&xl=123456789"
        )
        payload = json.dumps({
            "Example S4 - 16": {
                "episode": "16", "downloads": [{"res": "1080", "magnet": magnet}],
            },
        }).encode()
        request = {
            "media": {"title": "Example", "aliases": ["Example"]},
            "gaps": [{
                "id": "S00E16", "kind": "missing_episode", "season": 0,
                "label": "S00E16 第 16 集",
            }],
            "query_groups": [{"season": 0, "season_names": ["特别篇"]}],
            "rules": {"optional_discovery_only": True},
        }
        with mock.patch.object(adapter, "_fetch_bytes", return_value=payload):
            result = adapter._search_subsplease(
                request, set(), deadline=time.monotonic() + 30,
            )

        self.assertEqual(list(result), [])
        self.assertEqual(
            result.resource_failed_locators, ["quark_magnet:" + "b" * 40],
        )

    def test_subsplease_empty_array_is_a_successful_zero_hit_response(self):
        request = {
            "media": {"title": "Example", "aliases": ["Example"]},
            "gaps": [{"id": "S00E16", "kind": "missing_episode", "season": 0}],
            "query_groups": [{"season": 0, "season_names": ["特别篇"]}],
            "rules": {"optional_discovery_only": True},
        }
        with mock.patch.object(adapter, "_fetch_bytes", return_value=b"[]"):
            result = adapter._search_subsplease(
                request, set(), deadline=time.monotonic() + 30,
            )

        self.assertEqual(result.query_attempts, result.query_responses)
        self.assertTrue(result.source_exhausted)
        self.assertEqual(list(result), [])

    def test_nyaa_rss_search_builds_a_verified_gap_candidate(self):
        rss = b"""<?xml version='1.0' encoding='UTF-8'?>
        <rss><channel><item>
          <title>Example S01E01 1080p</title>
          <link>https://nyaa.si/download/42.torrent</link>
        </item></channel></rss>"""
        manifest = self.manifest()
        with mock.patch.object(adapter, "_fetch_bytes", return_value=rss), mock.patch.object(
            adapter, "_download_torrent", return_value=manifest,
        ):
            result = adapter._search_nyaa({
                "media": {"title": "Example", "aliases": ["Example"]},
                "gaps": [{"id": "S01E01", "kind": "missing_episode", "season": 1}],
                "query_groups": [{"season": 1, "season_names": ["Season 1"]}],
            }, set(), deadline=time.monotonic() + 30)
        self.assertEqual(len(result), 1)
        self.assertEqual([row["provider"] for row in result], ["quark_magnet"])
        self.assertEqual(result[0]["file_coverage"], ["S01E01"])
        self.assertEqual(result[0]["acquisition"]["expected_files"][0]["torrent_index"], 1)

    def test_mikan_rss_search_builds_a_verified_gap_candidate(self):
        rss = b"""<?xml version='1.0' encoding='UTF-8'?>
        <rss><channel><item>
          <title>Example S01E01 1080p</title>
          <enclosure type='application/x-bittorrent'
            url='https://mikanani.me/Download/20260804/example.torrent'/>
        </item></channel></rss>"""
        manifest = self.manifest()
        with mock.patch.object(
            adapter, "_fetch_bytes", return_value=rss,
        ), mock.patch.object(
            adapter, "_download_torrent", return_value=manifest,
        ):
            result = adapter._search_mikan({
                "media": {"title": "Example", "aliases": ["Example"]},
                "gaps": [{
                    "id": "S01E01", "kind": "missing_episode", "season": 1,
                }],
                "query_groups": [{"season": 1, "season_names": ["Season 1"]}],
            }, set(), deadline=time.monotonic() + 30)

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["provider"], "quark_magnet")
        self.assertEqual(result[0]["file_coverage"], ["S01E01"])
        self.assertEqual(result.query_attempts, result.query_responses)
        self.assertTrue(result.source_exhausted)

    def test_mikan_season_zero_searches_broad_title_but_rejects_season_four(self):
        rss = b"""<?xml version='1.0' encoding='UTF-8'?>
        <rss><channel><item>
          <title>Example S4 - 16 1080p</title>
          <enclosure type='application/x-bittorrent'
            url='https://mikanani.me/Download/20260804/example-s4e16.torrent'/>
        </item></channel></rss>"""
        info = {
            b"name": b"Example S4 - 16.mkv", b"piece length": 16384,
            b"pieces": b"x" * 20, b"length": 100,
        }
        manifest = adapter._torrent_manifest(adapter._bencode({
            b"announce": b"https://tracker.invalid", b"info": info,
        }))
        request = {
            "media": {"title": "Example", "aliases": ["Example"]},
            "gaps": [{
                "id": "S00E16", "kind": "missing_episode", "season": 0,
            }],
            "query_groups": [{"season": 0, "season_names": ["特别篇"]}],
            "rules": {"optional_discovery_only": True},
        }
        with mock.patch.object(
            adapter, "_fetch_bytes", return_value=rss,
        ) as fetch, mock.patch.object(
            adapter, "_download_torrent", return_value=manifest,
        ) as download:
            result = adapter._search_mikan(
                request, set(), deadline=time.monotonic() + 30,
            )

        self.assertEqual(list(result), [])
        self.assertGreater(result.query_responses, 0)
        first_url = fetch.call_args_list[0].args[0]
        self.assertIn("searchstr=Example", first_url)
        self.assertNotIn("searchstr=Example%20S00", first_url)
        download.assert_not_called()
        self.assertEqual(result.resource_failed_locators, [
            "torrent:https://mikanani.me/Download/20260804/example-s4e16.torrent",
        ])
        self.assertTrue(result.source_exhausted)

    def test_nyaa_rss_excludes_old_infohashes_before_thirty_two_item_cap(self):
        manifest = self.manifest()
        excluded_hashes = [f"{index + 1:040x}" for index in range(32)]
        items = "".join(
            "<item>"
            f"<title>Example old {index:02d} S01E01 1080p</title>"
            f"<link>https://nyaa.si/download/{index + 1}.torrent</link>"
            f"<nyaa:infoHash>{infohash}</nyaa:infoHash>"
            "</item>"
            for index, infohash in enumerate(excluded_hashes)
        ) + (
            "<item><title>Example fresh S01E01 1080p</title>"
            "<link>https://nyaa.si/download/999.torrent</link>"
            f"<nyaa:infoHash>{manifest['infohash']}</nyaa:infoHash></item>"
        )
        rss = (
            "<?xml version='1.0' encoding='UTF-8'?>"
            "<rss xmlns:nyaa='https://nyaa.si/xmlns/nyaa'><channel>"
            + items + "</channel></rss>"
        ).encode("utf-8")
        existing = {f"quark_magnet:{value}" for value in excluded_hashes}
        with mock.patch.object(
            adapter, "_fetch_bytes", return_value=rss,
        ), mock.patch.object(
            adapter, "_download_torrent", return_value=manifest,
        ) as download:
            result = adapter._search_nyaa({
                "media": {"title": "Example", "aliases": ["Example"]},
                "gaps": [{"id": "S01E01", "kind": "missing_episode", "season": 1}],
                "query_groups": [{"season": 1, "season_names": ["Season 1"]}],
            }, existing, deadline=time.monotonic() + 30)

        download.assert_called_once_with(
            "https://nyaa.si/download/999.torrent", mock.ANY,
            timeout=mock.ANY, attempts=1,
        )
        self.assertEqual([row["provider"] for row in result], ["quark_magnet"])
        self.assertEqual(result.preexcluded_count, 32)
        self.assertTrue(result.source_exhausted)

    def test_optional_discovery_maps_ova_without_enabling_acquisition(self):
        manifest = {
            "infohash": "a" * 40,
            "files": {1: {"path": "Example/Specials/Example OVA 01.mkv", "size": 100}},
        }
        request = {
            "media": {"title": "Example", "aliases": ["Example"]},
            "gaps": [{
                "id": "S00E01", "kind": "missing_episode", "season": 0,
                "label": "S00E01 Example OVA#1",
            }],
            "query_groups": [{"season": 0, "season_names": ["OVA"]}],
            "rules": {
                "optional_discovery_only": True, "discovery_only": True,
                "acquire_enabled": False, "upload_enabled": False,
                "library_mutation_enabled": False, "delete_enabled": False,
            },
        }
        candidate = adapter._torrent_candidate(
            request, "Example OVA 01 1080p", "https://example.invalid/1.torrent", manifest,
        )
        self.assertIsNotNone(candidate)
        self.assertEqual(candidate["file_coverage"], ["S00E01"])
        self.assertEqual(candidate["acquisition"]["file_index_by_gap"], {"S00E01": [1]})

    def test_optional_discovery_rejects_explicit_regular_season_files(self):
        request = {
            "media": {"title": "Rick and Morty", "aliases": ["Rick and Morty"]},
            "gaps": [
                {"id": "S00E01", "kind": "missing_episode", "season": 0},
            ],
            "query_groups": [{"season": 0, "season_names": ["特别篇"]}],
            "rules": {"optional_discovery_only": True},
        }
        manifest = {"files": {
            1: {"path": "RAM/S09E08.2026.1080p.WEB-DL.mkv", "size": 108},
            2: {"path": "RAM/S09E09.2026.1080p.WEB-DL.mkv", "size": 109},
            3: {"path": "RAM/S09E10.2026.1080p.WEB-DL.mkv", "size": 110},
        }}
        mapping, coverage = adapter._gap_file_map(
            request,
            "Rick and Morty Season 9 (2026) 1080p",
            manifest,
        )
        self.assertEqual(mapping, {})
        self.assertEqual(coverage, set())

    def test_specials_directory_prevents_root_episodes_from_becoming_s00(self):
        request = {
            "media": {"title": "Black Clover", "aliases": ["黑色五叶草"]},
            "gaps": [
                {"id": "S00E01", "kind": "missing_episode", "season": 0},
                {"id": "S00E02", "kind": "missing_episode", "season": 0},
            ],
            "query_groups": [{"season": 0, "season_names": ["特别篇"]}],
            "rules": {"optional_discovery_only": True},
        }
        manifest = {"files": {
            141: {
                "path": "SP/[DBD-Raws][Black Clover][SP][01][1080P].mkv",
                "size": 273194222,
            },
            142: {
                "path": "SP/[DBD-Raws][Black Clover][SP][02][1080P].mkv",
                "size": 208243297,
            },
            232: {
                "path": "[DBD-Raws][Black Clover][001][1080P].mkv",
                "size": 606410196,
            },
            235: {
                "path": "[DBD-Raws][Black Clover][002][1080P].mkv",
                "size": 665194501,
            },
        }}

        mapping, coverage = adapter._gap_file_map(
            request,
            "[DBD-Raws][Black Clover][001-170TV全集+特别篇]",
            manifest,
        )

        self.assertEqual(mapping, {"S00E01": [141], "S00E02": [142]})
        self.assertEqual(coverage, {"S00E01", "S00E02"})

    def test_release_local_ova_ordinal_does_not_guess_tmdb_s00_identity(self):
        request = {
            "media": {"title": "School Days", "aliases": ["日在校园"]},
            "gaps": [{
                "id": "S00E01", "kind": "missing_episode", "season": 0,
                "label": "S00E01 School Days ONA",
                "season_name": "特别篇",
            }],
            "query_groups": [{"season": 0, "season_names": ["特别篇"]}],
            "rules": {"optional_discovery_only": True},
        }
        manifest = {"files": {1: {
            "path": "School Days/OVA1/魔法少女桂心.mkv",
            "size": 100,
        }}}
        mapping, coverage = adapter._gap_file_map(
            request,
            "School Days 01-12 + OVA",
            manifest,
        )
        self.assertEqual(mapping, {})
        self.assertEqual(coverage, set())
        self.assertIsNone(adapter._torrent_candidate(
            request,
            "School Days 01-12 + OVA",
            "https://example.invalid/school-days.torrent",
            {"infohash": "d" * 40, **manifest},
        ))

    def test_existing_s00e02_does_not_make_ova1_cover_remaining_s00e01(self):
        # The target already contains the correctly remapped OVA at S00E02;
        # the fresh audit therefore asks only for the unrelated ONA S00E01.
        # Re-searching the same release-local OVA1 must remain fail-closed.
        request = {
            "media": {"title": "School Days", "aliases": ["日在校园"]},
            "existing_target_episode_ids": ["S00E02"],
            "gaps": [{
                "id": "S00E01", "kind": "missing_episode", "season": 0,
                "label": "S00E01 School Days ONA",
            }],
            "query_groups": [{"season": 0, "season_names": ["特别篇"]}],
            "rules": {"optional_discovery_only": True},
        }
        mapping, coverage = adapter._gap_file_map(
            request,
            "School Days + OVA",
            {"files": {1: {"path": "School Days OVA1.mkv", "size": 100}}},
        )
        self.assertEqual(mapping, {})
        self.assertEqual(coverage, set())

    def test_specials_remain_excluded_for_regular_discovery(self):
        manifest = {
            "infohash": "a" * 40,
            "files": {1: {"path": "Example/Specials/Example S01E01.mkv", "size": 100}},
        }
        request = {
            "media": {"title": "Example", "aliases": ["Example"]},
            "gaps": [{"id": "S01E01", "kind": "missing_episode", "season": 1}],
            "query_groups": [{"season": 1, "season_names": ["Season 1"]}],
        }
        self.assertIsNone(adapter._torrent_candidate(
            request, "Example S01E01", "https://example.invalid/1.torrent", manifest,
        ))

    def test_nyaa_isolates_one_malformed_torrent_and_keeps_later_candidates(self):
        rss = b"""<?xml version='1.0' encoding='UTF-8'?>
        <rss><channel>
          <item><title>Example S01E01 malformed</title><link>https://nyaa.si/download/1.torrent</link></item>
          <item><title>Example S01E01 1080p</title><link>https://nyaa.si/download/2.torrent</link></item>
        </channel></rss>"""
        with mock.patch.dict(os.environ, {
            "SCRAPEFLOW_REPLENISHMENT_QUARK_OFFLINE": "0",
        }), mock.patch.object(adapter, "_fetch_bytes", return_value=rss), mock.patch.object(
            adapter, "_download_torrent",
            side_effect=[TypeError("malformed bencode"), self.manifest()],
        ):
            result = adapter._search_nyaa(self.unlock_local({
                "media": {"title": "Example", "aliases": ["Example"]},
                "gaps": [{"id": "S01E01", "kind": "missing_episode", "season": 1}],
                "query_groups": [{"season": 1, "season_names": ["Season 1"]}],
            }), set(), deadline=time.monotonic() + 30)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["locator"], "torrent:https://nyaa.si/download/2.torrent")

    def test_dynamic_torrent_mapping_uses_unique_requested_season(self):
        manifest = self.manifest()
        request = {
            "gaps": [
                {"id": "S01E01", "kind": "missing_episode", "season": 1},
                {"id": "S01E02", "kind": "missing_episode", "season": 1},
            ],
        }
        mapping, coverage = adapter._gap_file_map(request, "Example 1080p", manifest)
        self.assertEqual(mapping, {"S01E01": [1], "S01E02": [2]})
        self.assertEqual(coverage, {"S01E01", "S01E02"})

    def test_quark_offline_derivation_keeps_same_infohash_local_fallback(self):
        request = {
            "media": {"title": "Example"},
            "gaps": [{"id": "S03E01", "kind": "missing_episode", "season": 3}],
            "query_groups": [{"season": 3, "season_names": ["Season 3"]}],
        }
        manifest = {
            "infohash": "0123456789abcdef0123456789abcdef01234567",
            "files": {1: {"path": "Example/Example.S03E01.mkv", "size": 456}},
        }
        with mock.patch.dict(os.environ, {
            "SCRAPEFLOW_REPLENISHMENT_QUARK_OFFLINE": "1",
        }):
            variants = adapter._torrent_candidate_variants(
                request, "Example Season 3 1080p",
                "https://fixture/release.torrent", manifest,
            )
        self.assertEqual([row["provider"] for row in variants], ["quark_magnet", "magnet"])
        self.assertEqual(variants[0]["infohash"], variants[1]["infohash"])
        self.assertEqual(variants[0]["fallback_locator"], variants[1]["locator"])
        self.assertEqual(variants[0]["acquisition"]["expected_files"], [{
            "torrent_index": 1,
            "path": "Example/Example.S03E01.mkv", "size": 456,
            "gap_ids": ["S03E01"],
        }])

    def test_catalog_local_failure_still_derives_quark_offline_lane(self):
        infohash = "0123456789abcdef0123456789abcdef01234567"
        catalog_row = {
            "provider": "magnet",
            "release_name": "Example Season 3 1080p",
            "locator": "torrent:https://fixture/release.torrent",
            "infohash": infohash,
            "acquisition": {
                "kind": "torrent", "url": "https://fixture/release.torrent",
                "file_index_by_gap": {"S03E01": [7]},
                "file_path_by_index": {"7": "Example/Example.S03E01.mkv"},
                "file_size_by_index": {"7": 456},
            },
        }
        with mock.patch.dict(os.environ, {
            "SCRAPEFLOW_REPLENISHMENT_QUARK_OFFLINE": "1",
        }):
            variants = adapter._catalog_torrent_candidate_variants(catalog_row)
        self.assertEqual([row["provider"] for row in variants], ["quark_magnet", "magnet"])
        self.assertEqual(variants[0]["locator"], f"quark_magnet:{infohash}")
        self.assertEqual(variants[0]["fallback_locator"], catalog_row["locator"])
        self.assertEqual(variants[0]["acquisition"]["expected_files"], [{
            "torrent_index": 7,
            "path": "Example/Example.S03E01.mkv", "size": 456,
            "gap_ids": ["S03E01"],
        }])

    def test_later_season_requires_number_or_semantic_season_identity(self):
        manifest = self.manifest()
        manifest["files"][1]["path"] = "Example [01].mkv"
        manifest["files"][2]["path"] = "Example [02].mkv"
        request = {
            "gaps": [{
                "id": "S02E01", "kind": "missing_episode", "season": 2,
                "season_name": "完结篇",
            }],
            "query_groups": [{"season": 2, "season_names": ["完结篇"]}],
        }
        mapping, _coverage = adapter._gap_file_map(request, "Example original series", manifest)
        self.assertEqual(mapping, {})
        mapping, _coverage = adapter._gap_file_map(request, "Example 完结篇", manifest)
        self.assertEqual(mapping, {"S02E01": [1]})

    def test_multi_season_request_uses_one_unique_semantic_season(self):
        manifest = self.manifest()
        manifest["files"][1]["path"] = "Example [01].mkv"
        manifest["files"][2]["path"] = "Example [02].mkv"
        request = {
            "gaps": [
                {"id": "S03E01", "kind": "missing_episode", "season": 3},
                {"id": "S04E01", "kind": "missing_episode", "season": 4},
            ],
            "query_groups": [
                {"season": 3, "season_names": ["To LOVE-Ru Darkness"]},
                {"season": 4, "season_names": ["To LOVE-Ru Darkness 2nd"]},
            ],
        }
        mapping, coverage = adapter._gap_file_map(
            request, "To LOVE-Ru Darkness 2nd 1080p", manifest,
        )
        self.assertEqual(mapping, {"S04E01": [1]})
        self.assertEqual(coverage, {"S04E01"})

    def test_multi_season_bundle_maps_each_semantic_directory_independently(self):
        manifest = self.manifest()
        manifest["files"][1]["path"] = (
            "To LOVE-Ru Darkness/To LOVE-Ru Darkness 01 [BD 1080p][E6FB5CBC].mkv"
        )
        manifest["files"][2]["path"] = (
            "To LOVE-Ru Darkness 2nd/To LOVE-Ru Darkness 2nd 01 [BD 1080p][A1CA6B0C].mkv"
        )
        request = {
            "gaps": [
                {"id": "S03E01", "kind": "missing_episode", "season": 3},
                {"id": "S04E01", "kind": "missing_episode", "season": 4},
            ],
            "query_groups": [
                {"season": 3, "season_names": ["To LOVE-Ru Darkness"]},
                {"season": 4, "season_names": ["To LOVE-Ru Darkness 2nd"]},
            ],
        }
        mapping, coverage = adapter._gap_file_map(
            request, "To LOVE-Ru Darkness + Darkness 2nd", manifest,
        )
        self.assertEqual(mapping, {"S03E01": [1], "S04E01": [2]})
        self.assertEqual(coverage, {"S03E01", "S04E01"})

    def test_optional_gap_uses_explicit_ova_ordinal_from_official_title(self):
        request = {
            "gaps": [{
                "id": "S00E07", "kind": "missing_episode", "season": 0,
                "season_name": "Darkness OVA#1「Prologue」",
                "label": "S00E07 Darkness OVA#1「Prologue」",
            }],
            "query_groups": [{"season": 0, "season_names": ["OVA", "OAD"]}],
            "rules": {"optional_discovery_only": True},
        }
        manifest = {"files": {1: {
            "path": "出包王女 S3/OAD/To Love-Ru Darkness - OVA 01 [720p].exe",
            "size": 123,
        }}}
        mapping, coverage = adapter._gap_file_map(
            request, "To Love-Ru Darkness OAD", manifest,
            allowed_payload_extensions=frozenset({".exe"}),
        )
        self.assertEqual(mapping, {"S00E07": [1]})
        self.assertEqual(coverage, {"S00E07"})

    def test_optional_gap_uses_exact_multilingual_tmdb_title_alias(self):
        request = {
            "gaps": [{
                "id": "S00E51", "kind": "missing_episode", "season": 0,
                "title": "沉睡鬼的枕边夜话",
                "title_aliases": ["眠れる鬼の夜話"],
            }],
            "rules": {"optional_discovery_only": True},
        }
        manifest = {"files": {1: {
            "path": "ReZero Break Time/ReZero - 眠れる鬼の夜話.mkv",
            "size": 123,
        }}}
        mapping, coverage = adapter._gap_file_map(
            request, "ReZero Break Time collection", manifest,
        )
        self.assertEqual(mapping, {"S00E51": [1]})
        self.assertEqual(coverage, {"S00E51"})

    def test_optional_gap_rejects_conflicting_tmdb_and_ova_ordinals(self):
        request = {
            "gaps": [{
                "id": "S00E07", "kind": "missing_episode", "season": 0,
                "season_name": "Darkness OVA#1「Prologue」",
            }],
            "rules": {"optional_discovery_only": True},
        }
        manifest = {"files": {
            1: {"path": "Darkness OVA 01.mkv", "size": 101},
            2: {"path": "Darkness S00E07.mkv", "size": 107},
        }}
        mapping, coverage = adapter._gap_file_map(request, "Darkness OVA", manifest)
        self.assertEqual(mapping, {})
        self.assertEqual(coverage, set())

    def test_named_optional_arc_overrides_global_oad_ordinal(self):
        request = {
            "gaps": [
                {"id": "S00E02", "kind": "missing_episode", "season": 0,
                 "label": "S00E02 OVA2：神明，去泡温泉"},
                {"id": "S00E03", "kind": "missing_episode", "season": 0,
                 "label": "S00E03 过去篇01：神明，回到过去"},
                {"id": "S00E07", "kind": "missing_episode", "season": 0,
                 "label": "S00E07 新婚篇：神明，幸福圆满"},
            ],
            "rules": {"optional_discovery_only": True},
        }
        manifest = {"files": {
            1: {"path": "OAD02(过去篇01).mp4", "size": 102},
            2: {"path": "OAD06(新婚篇01).mp4", "size": 106},
        }}
        mapping, coverage = adapter._gap_file_map(request, "元气少女缘结神 OAD", manifest)
        self.assertEqual(mapping, {"S00E03": [1], "S00E07": [2]})
        self.assertEqual(coverage, {"S00E03", "S00E07"})

    def test_optional_arc_folder_without_ordinal_does_not_guess(self):
        request = {
            "gaps": [
                {"id": "S00E02", "kind": "missing_episode", "season": 0,
                 "label": "S00E02 OVA2：神明，去泡温泉"},
                {"id": "S00E04", "kind": "missing_episode", "season": 0,
                 "label": "S00E04 过去篇02：狐妖，堕入爱河"},
            ],
            "rules": {"optional_discovery_only": True},
        }
        manifest = {"files": {1: {"path": "过去篇/OAD 02.mp4", "size": 102}}}
        mapping, coverage = adapter._gap_file_map(request, "元气少女缘结神", manifest)
        self.assertEqual(mapping, {})
        self.assertEqual(coverage, set())

    def test_optional_gap_without_explicit_source_ordinal_does_not_guess(self):
        request = {
            "gaps": [{
                "id": "S00E17", "kind": "missing_episode", "season": 0,
                "season_name": "Darkness SP: 后宫计划 梦梦的秘密档案",
            }],
            "query_groups": [{"season": 0, "season_names": ["OVA", "Special"]}],
            "rules": {"optional_discovery_only": True},
        }
        manifest = {"files": {1: {"path": "Darkness OVA 10.mkv", "size": 110}}}
        mapping, coverage = adapter._gap_file_map(request, "Darkness extras", manifest)
        self.assertEqual(mapping, {})
        self.assertEqual(coverage, set())

    def test_optional_discovery_rejects_recap_collection_bare_ordinals(self):
        request = {
            "media": {"title": "银魂", "aliases": ["Gintama"]},
            "gaps": [
                {"id": f"S00E{episode:02d}", "kind": "missing_episode", "season": 0,
                 "label": label}
                for episode, label in (
                    (3, "S00E03 动画银魂大反省会"),
                    (4, "S00E04 银魂剧场版 完结篇 永远的万事屋"),
                    (5, "S00E05 银魂 总集篇 on theatre 2d 真选组动乱篇"),
                )
            ],
            "query_groups": [{"season": 0, "season_names": ["特别篇"]}],
            "rules": {"optional_discovery_only": True},
        }
        manifest = {"files": {
            1: {"path": "银魂 精选集/精选集03(285)集 过去回想篇第03话 恋爱需要小强呸呸.mp4", "size": 103},
            2: {"path": "银魂 精选集/精选集04(223)集 过去回想篇第04话 大叔的家庭状况.mp4", "size": 104},
            3: {"path": "银魂 精选集/精选集05(17)集 过去回想篇第05话 所谓亲子.mp4", "size": 105},
        }}

        mapping, coverage = adapter._gap_file_map(
            request, "银魂 367集+剧场版+OAD+精选集", manifest,
        )

        self.assertEqual(mapping, {})
        self.assertEqual(coverage, set())

    def test_optional_recap_collection_keeps_explicit_s00_identity(self):
        request = {
            "gaps": [{
                "id": "S00E03", "kind": "missing_episode", "season": 0,
                "label": "S00E03 Official recap",
            }],
            "rules": {"optional_discovery_only": True},
        }
        manifest = {"files": {1: {
            "path": "Recap Collection/Show.S00E03.Official.Recap.mkv", "size": 103,
        }}}

        mapping, coverage = adapter._gap_file_map(request, "Show recap collection", manifest)

        self.assertEqual(mapping, {"S00E03": [1]})
        self.assertEqual(coverage, {"S00E03"})

    def test_optional_recap_collection_keeps_official_ova_alias(self):
        request = {
            "gaps": [{
                "id": "S00E07", "kind": "missing_episode", "season": 0,
                "label": "S00E07 Darkness OVA#1 Prologue",
            }],
            "rules": {"optional_discovery_only": True},
        }
        manifest = {"files": {1: {
            "path": "Recap Collection/Darkness OVA 01.mkv", "size": 101,
        }}}

        mapping, coverage = adapter._gap_file_map(request, "Darkness recap collection", manifest)

        self.assertEqual(mapping, {"S00E07": [1]})
        self.assertEqual(coverage, {"S00E07"})

    def test_optional_recap_collection_keeps_exact_official_title_semantics(self):
        request = {
            "gaps": [{
                "id": "S00E04", "kind": "missing_episode", "season": 0,
                "label": "S00E04 总集篇3：决战",
                "season_name": "特别篇",
            }],
            "rules": {"optional_discovery_only": True},
        }
        manifest = {"files": {1: {
            "path": "Show recap/总集篇3：决战.mkv", "size": 104,
        }}}

        mapping, coverage = adapter._gap_file_map(request, "Show recap", manifest)

        self.assertEqual(mapping, {"S00E04": [1]})
        self.assertEqual(coverage, {"S00E04"})

    def test_single_season_request_excludes_explicit_other_season_parent(self):
        manifest = {
            "infohash": "a" * 40,
            "files": {
                1: {
                    "path": "Season 1 (2007-2008)/[Trix] Clannad - 20 [76037F69].mkv",
                    "size": 240686730,
                },
                2: {
                    "path": (
                        "Season 2 (2008-2009)/"
                        "[Trix] Clannad After Story - 20 [2A089E18].mkv"
                    ),
                    "size": 256560414,
                },
            },
        }
        request = {
            "gaps": [{"id": "S01E20", "kind": "missing_episode", "season": 1}],
            "query_groups": [{"season": 1, "season_names": []}],
        }

        mapping, coverage = adapter._gap_file_map(
            request, "Clannad Season 1 + Season 2 complete", manifest,
        )

        self.assertEqual(mapping, {"S01E20": [1]})
        self.assertEqual(coverage, {"S01E20"})

    def test_multi_season_request_maps_clannad_semantic_titles_separately(self):
        manifest = {
            "infohash": "a" * 40,
            "files": {
                1: {"path": "[Trix] Clannad - 20 [76037F69].mkv", "size": 240686730},
                2: {
                    "path": "[Trix] Clannad After Story - 20 [2A089E18].mkv",
                    "size": 256560414,
                },
            },
        }
        request = {
            "gaps": [
                {"id": "S01E20", "kind": "missing_episode", "season": 1},
                {"id": "S02E20", "kind": "missing_episode", "season": 2},
            ],
            "query_groups": [
                {"season": 1, "season_names": ["Clannad"]},
                {"season": 2, "season_names": ["Clannad After Story"]},
            ],
        }

        mapping, coverage = adapter._gap_file_map(
            request, "Clannad + Clannad After Story", manifest,
        )

        self.assertEqual(mapping, {"S01E20": [1], "S02E20": [2]})
        self.assertEqual(coverage, {"S01E20", "S02E20"})


if __name__ == "__main__":
    unittest.main()
