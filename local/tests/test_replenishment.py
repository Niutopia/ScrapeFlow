import base64
import unittest

from engine.tools import replenishment_local_adapter as local_adapter
from local.scrapeflow_api.replenishment import (
    _coverage_tokens,
    _expanded_episode_ids,
    _season_markers,
    build_replenishment_request,
    build_replenishment_requests,
    select_replenishment_candidate,
    select_replenishment_candidates,
    suppress_gaps_satisfied_by_planned_videos,
    suppress_request_gaps_present_in_names,
    validate_acquisition_result,
    validate_acquisition_results,
)


class ReplenishmentTests(unittest.TestCase):
    def request(self):
        return build_replenishment_request({
            "target_root": "/library/Example",
            "metadata": {"title": "Example", "year": "2026", "tmdb_id": 42},
            "scan_report": {"resource_gaps": [
                {"kind": "missing_episode", "label": "S04E13 New member", "reason": "missing"},
                {"kind": "missing_episode", "label": "S04E14 Black corps", "reason": "missing"},
                {"kind": "subtitle_without_video", "label": "subtitle", "reason": "optional"},
            ]},
        }, job_id="abc123abc123", round_number=1)

    def test_episode_tokens_do_not_backtrack_into_partial_numbers(self):
        self.assertEqual(_expanded_episode_ids("Show.S09E10.mkv"), {"S09E10"})
        self.assertEqual(_expanded_episode_ids("Show.S09E08.mkv"), {"S09E08"})
        self.assertEqual(_expanded_episode_ids("Show.S01E01.mkv"), {"S01E01"})
        self.assertEqual(_expanded_episode_ids("Show.S00E01.mkv"), {"S00E01"})
        self.assertEqual(_expanded_episode_ids("Show.S09E10000.mkv"), set())
        self.assertEqual(_expanded_episode_ids("Show.1234x00001.mkv"), set())

    def test_episode_tokens_are_not_misread_as_season_markers(self):
        for value in (
            "Show.S09E10.mkv",
            "Show.S09E08.mkv",
            "Show.S01E01.mkv",
            "Show.S00E01.mkv",
        ):
            with self.subTest(value=value):
                self.assertEqual(_season_markers(value), set())
        self.assertEqual(_season_markers("Show.S09.Complete"), {9})
        self.assertEqual(_season_markers("Show.S00.Specials"), {0})
        self.assertEqual(_season_markers("Show S4 - 16"), {4})
        self.assertEqual(_season_markers("Show S4 - S16"), set(range(4, 17)))
        self.assertEqual(_season_markers("Show.S1234.Complete"), set())
        self.assertEqual(_season_markers("Show.S1-S1234.Complete"), {1})

    def test_episode_only_tokens_do_not_backtrack_at_numeric_boundary(self):
        self.assertEqual(
            _coverage_tokens(["Show.E0001.mkv"], default_seasons={9}),
            {"S09E01"},
        )
        self.assertEqual(
            _coverage_tokens(["Show.E10000.mkv"], default_seasons={9}),
            set(),
        )

    def unlock_local(self, request):
        request["rules"]["minimum_attempts_per_cloud_lane"] = 30
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

    def test_request_contains_released_tv_gaps(self):
        request = self.request()
        self.assertEqual([gap["id"] for gap in request["gaps"]], ["S04E13", "S04E14"])
        self.assertTrue(request["rules"]["name_coverage_is_hard_gate"])
        self.assertIn("Example S04E13-E14", request["search_queries"])
        self.assertIn("Example Season 4", request["search_queries"])
        self.assertIn("Example 第四季", request["search_queries"])

    def test_post_commit_plan_videos_remove_only_matching_stale_gaps(self):
        plan = {
            "mode": "tv", "target_root": "/library/Example",
            "metadata": {"tmdb_id": 42, "title": "Example"},
            "files": [
                {
                    "media_kind": "video",
                    "final_name": f"Example - S01E{episode:02d}.mkv",
                    "target_dir": "/library/Example/Season 01",
                }
                for episode in range(1, 23)
            ] + [
                {
                    "media_kind": "video", "final_name": f"Example - S00E{episode:02d}.mkv",
                    "target_dir": "/library/Example/Season 00",
                }
                for episode in range(1, 25)
            ],
            "scan_report": {"resource_gaps": [
                {"kind": "missing_episode", "label": "S01E01-E22 stale"},
                {"kind": "missing_episode", "label": "S00E25 still missing"},
            ]},
        }
        filtered, removed = suppress_gaps_satisfied_by_planned_videos(plan)
        self.assertEqual(removed, [f"S01E{episode:02d}" for episode in range(1, 23)])
        self.assertEqual(
            [row["label"] for row in filtered["scan_report"]["resource_gaps"]],
            ["S00E25 still missing"],
        )
        self.assertEqual(len(plan["scan_report"]["resource_gaps"]), 2)

    def test_live_target_names_remove_gap_before_acquire(self):
        filtered, removed = suppress_request_gaps_present_in_names(
            self.request(), ["Example - S04E13.mkv", "Example - S04E14.mkv"],
        )
        self.assertEqual(removed, ["S04E13", "S04E14"])
        self.assertEqual(filtered["gaps"], [])
        self.assertEqual(filtered["query_groups"], [])

    def test_released_season_zero_gap_enters_automatic_replenishment(self):
        request = build_replenishment_request({
            "target_root": "/library/Example",
            "metadata": {"title": "Example", "year": "2026", "tmdb_id": 42},
            "scan_report": {"resource_gaps": [{
                "kind": "missing_episode", "label": "S00E01 OVA",
                "reason": "TMDB 已发布但缺少视频",
            }]},
        }, job_id="abc123abc123", round_number=1)
        self.assertEqual([gap["id"] for gap in request["gaps"]], ["S00E01"])
        self.assertEqual(request["gaps"][0]["title"], "OVA")
        self.assertEqual(request["query_groups"][0]["season"], 0)
        self.assertEqual(request["query_groups"][0]["episode_titles"], ["OVA"])
        self.assertIn("Example S00E01", request["search_queries"])
        self.assertIn("Example OVA", request["search_queries"])
        self.assertTrue(request["rules"]["optional_discovery_only"])
        self.assertTrue(request["rules"]["season_zero_replenishment_required"])

    def test_multilingual_s00_episode_titles_expand_search_and_exact_file_mapping(self):
        request = build_replenishment_request({
            "target_root": "/library/ReZero",
            "metadata": {
                "title": "Re:Zero", "tmdb_id": 65942,
            },
            "scan_report": {"resource_gaps": [{
                "kind": "missing_episode", "label": "S00E51",
                "title": "沉睡鬼的枕边夜话",
                "title_aliases": [
                    "眠れる鬼の夜話", "A Night Tale of a Sleeping Oni",
                ],
                "source_episode_aliases": [{
                    "season": 3,
                    "episode": 1,
                    "series_titles": [
                        "Re:ゼロから始める休憩時間",
                        "Re:Zero - Starting Break Time from Zero",
                    ],
                }],
            }]},
        }, job_id="abc123abc123", round_number=1)
        self.assertEqual(request["gaps"][0]["title_aliases"], [
            "眠れる鬼の夜話", "A Night Tale of a Sleeping Oni",
        ])
        self.assertEqual(request["gaps"][0]["source_episode_aliases"], [{
            "season": 3,
            "episode": 1,
            "series_titles": [
                "Re:ゼロから始める休憩時間",
                "Re:Zero - Starting Break Time from Zero",
            ],
        }])
        self.assertEqual(request["query_groups"][0]["episode_titles"], [
            "沉睡鬼的枕边夜话", "眠れる鬼の夜話", "A Night Tale of a Sleeping Oni",
        ])
        self.assertIn("Re:Zero 眠れる鬼の夜話", request["search_queries"])

        bundle = select_replenishment_candidates(request, [{
            "provider": "cloud_share",
            "release_name": "Re Zero Break Time collection",
            "files": ["ReZero - 眠れる鬼の夜話.mkv"],
            "locator": "share:exact-japanese-title",
        }])
        self.assertEqual(bundle["status"], "complete")
        self.assertEqual(bundle["covered_gap_ids"], ["S00E51"])

    def test_exact_tmdb_aliases_survive_grouped_request_building(self):
        batch = build_replenishment_requests({
            "mode": "tv", "target_root": "/library/Example",
            "metadata": {
                "title": "示例", "original_title": "サンプル",
                "aliases": ["Example: Starting Life"],
                "tmdb_id": 42, "series_root": "/library/Example",
            },
            "scan_report": {"resource_gaps": [{
                "kind": "missing_episode", "label": "S00E01 OVA",
            }]},
        }, job_id="abc123abc123", round_number=1)
        request = batch["requests"][0]
        self.assertIn("Example: Starting Life", request["media"]["aliases"])
        self.assertIn(
            "Example: Starting Life S00E01", request["search_queries"],
        )

    def test_query_ceiling_does_not_drop_eighth_official_alias(self):
        aliases = [f"Official Alias {index}" for index in range(1, 9)]
        request = build_replenishment_request({
            "mode": "tv", "target_root": "/library/Example",
            "metadata": {
                "title": "本地标题", "original_title": "原始タイトル",
                "aliases": aliases, "tmdb_id": 42,
            },
            "scan_report": {"resource_gaps": [
                {
                    "kind": "missing_episode", "label": f"S00E{episode:02d}",
                    "title": f"Special Episode {episode}",
                }
                for episode in range(1, 28)
            ]},
        }, job_id="abc123abc123", round_number=1)
        self.assertEqual(len(request["search_queries"]), 120)
        self.assertIn("Official Alias 6", request["search_queries"])
        self.assertIn("Official Alias 6 S00E01-E27", request["search_queries"])

    def test_regular_gap_does_not_enable_optional_manifest_mapping(self):
        self.assertNotIn("optional_discovery_only", self.request()["rules"])

    def test_s00_only_request_maps_specials_ova_sp_and_oad_ordinals(self):
        cases = [
            (
                "S00E07", "Darkness OVA#1 Prologue",
                "Example/Specials/Example OVA 01.mkv",
            ),
            (
                "S00E17", "Darkness SP#2 Secret File",
                "Example/SPs/Example SP 02.mkv",
            ),
            (
                "S00E23", "Darkness OAD#3 After Story",
                "Example/Extras/Example OAD 03.mkv",
            ),
        ]
        for gap_id, official_title, path in cases:
            with self.subTest(gap_id=gap_id, path=path):
                request = build_replenishment_request({
                    "target_root": "/library/Example",
                    "metadata": {
                        "title": "Example", "year": "2026", "tmdb_id": 42,
                    },
                    "scan_report": {"resource_gaps": [{
                        "kind": "missing_episode",
                        "label": f"{gap_id} {official_title}",
                        "season_name": official_title,
                    }]},
                }, job_id="abc123abc123", round_number=1)
                manifest = {
                    "infohash": "a" * 40,
                    "files": {1: {"path": path, "size": 100}},
                }

                candidate = local_adapter._torrent_candidate(
                    request,
                    "Example Complete Extras 1080p",
                    "https://example.invalid/optional.torrent",
                    manifest,
                )

                self.assertTrue(request["rules"]["optional_discovery_only"])
                self.assertIsNotNone(candidate)
                self.assertEqual(candidate["file_coverage"], [gap_id])
                self.assertEqual(
                    candidate["acquisition"]["file_index_by_gap"],
                    {gap_id: [1]},
                )

    def test_regular_lane_does_not_map_specials_ova_sp_or_oad_ordinals(self):
        paths = [
            "Example/Specials/Example OVA 01.mkv",
            "Example/SPs/Example SP 01.mkv",
            "Example/Extras/Example OAD 01.mkv",
        ]
        for path in paths:
            with self.subTest(path=path):
                request = build_replenishment_request({
                    "target_root": "/library/Example",
                    "metadata": {
                        "title": "Example", "year": "2026", "tmdb_id": 42,
                    },
                    "scan_report": {"resource_gaps": [{
                        "kind": "missing_episode",
                        "label": "S01E07 Darkness OVA#1 Prologue",
                        "season_name": "Season 1",
                    }]},
                }, job_id="abc123abc123", round_number=1)
                manifest = {
                    "infohash": "b" * 40,
                    "files": {1: {"path": path, "size": 100}},
                }

                self.assertNotIn(
                    "optional_discovery_only", request["rules"],
                )
                self.assertIsNone(local_adapter._torrent_candidate(
                    request,
                    "Example Complete Extras 1080p",
                    "https://example.invalid/regular.torrent",
                    manifest,
                ))

    def test_name_coverage_is_a_hard_gate(self):
        request = self.request()
        selected = select_replenishment_candidate(request, [{
            "provider": "cloud_share", "release_name": "Example S04E13",
            "name_coverage": ["S04E13"], "resolution": "2160p",
            "updated_at": "2026-07-27T00:00:00Z", "locator": "share:partial",
        }])
        self.assertIsNone(selected)

    def test_optional_discovery_accepts_manifest_verified_s00_coverage(self):
        request = {
            "media": {"title": "Example", "aliases": ["Example"], "tmdb_id": 42},
            "gaps": [{"id": "S00E01", "kind": "missing_episode", "season": 0}],
            "query_groups": [{"season": 0, "season_names": ["Picture Drama"]}],
            "rules": {"optional_discovery_only": True, "discovery_only": True,
                      "acquire_enabled": False, "upload_enabled": False,
                      "library_mutation_enabled": False, "delete_enabled": False},
        }
        self.unlock_local(request)
        bundle = select_replenishment_candidates(request, [{
            "provider": "magnet", "release_name": "Example Complete 1080p",
            "files": ["SPs/Example SP01.mkv"], "file_coverage": ["S00E01"],
            "resolution": "1080p", "availability": "metadata_verified",
            "locator": "torrent:https://example.invalid/1.torrent",
        }])
        self.assertEqual(bundle["status"], "complete")
        self.assertEqual(bundle["covered_gap_ids"], ["S00E01"])

    def test_optional_semantic_name_cannot_claim_gap_missing_from_exact_manifest(self):
        request = {
            "media": {"title": "Example", "aliases": ["Example"], "tmdb_id": 42},
            "gaps": [
                {"id": "S00E05", "kind": "missing_episode", "season": 0},
                {"id": "S00E06", "kind": "missing_episode", "season": 0},
            ],
            "query_groups": [{"season": 0, "season_names": ["特别篇"]}],
            "rules": {"optional_discovery_only": True},
        }
        bundle = select_replenishment_candidates(request, [{
            "provider": "quark_share",
            "release_name": "Example 特别篇 1080p",
            "file_coverage": ["S00E05"],
            "files": ["Example 特别篇第05话.mp4"],
            "locator": "quark_share:exact-only",
            "acquisition": {
                "kind": "quark_fast_save", "share_id": "exact-only",
                "file_id_by_gap": {"S00E05": ["fid-5"]},
                "file_path_by_id": {"fid-5": "Example 特别篇第05话.mp4"},
                "file_size_by_id": {"fid-5": 123},
            },
        }])
        self.assertEqual(bundle["status"], "partial")
        self.assertEqual(bundle["covered_gap_ids"], ["S00E05"])
        self.assertEqual(bundle["uncovered_gap_ids"], ["S00E06"])
        self.assertEqual(bundle["selections"][0]["selected_gap_ids"], ["S00E05"])

    def test_optional_tmdb_episode_title_requires_exact_file_member(self):
        request = {
            "media": {"title": "Example", "aliases": ["Example"], "tmdb_id": 42},
            "gaps": [
                {"id": "S00E05", "kind": "missing_episode", "season": 0,
                 "title": "Memory Snow"},
                {"id": "S00E06", "kind": "missing_episode", "season": 0,
                 "title": "Frozen Bond"},
            ],
            "query_groups": [{
                "season": 0, "season_names": ["Specials"],
                "episode_titles": ["Memory Snow", "Frozen Bond"],
            }],
            "rules": {"optional_discovery_only": True},
        }
        bundle = select_replenishment_candidates(request, [{
            "provider": "cloud_share",
            "release_name": "Example Specials Collection",
            "files": ["Example - Memory Snow.mkv"],
            "locator": "quark_share:memory-snow",
        }])

        self.assertEqual(bundle["status"], "partial")
        self.assertEqual(bundle["covered_gap_ids"], ["S00E05"])
        self.assertEqual(bundle["uncovered_gap_ids"], ["S00E06"])

    def test_optional_tmdb_episode_title_in_release_name_cannot_override_file_list(self):
        request = {
            "media": {"title": "Example", "aliases": ["Example"], "tmdb_id": 42},
            "gaps": [{
                "id": "S00E05", "kind": "missing_episode", "season": 0,
                "title": "Memory Snow",
            }],
            "query_groups": [{
                "season": 0, "season_names": ["Specials"],
                "episode_titles": ["Memory Snow"],
            }],
            "rules": {"optional_discovery_only": True},
        }
        bundle = select_replenishment_candidates(request, [{
            "provider": "cloud_share",
            "release_name": "Example Memory Snow",
            "files": ["Example - Unrelated Bonus.mkv"],
            "locator": "share:misleading-release-title",
        }])

        self.assertEqual(bundle["status"], "no_match")
        self.assertEqual(bundle["covered_gap_ids"], [])
        self.assertEqual(bundle["uncovered_gap_ids"], ["S00E05"])

    def test_exact_tmdb_official_alias_can_prove_cross_script_candidate_identity(self):
        request = {
            "media": {
                "title": "Re：从零开始的异世界生活",
                "aliases": [
                    "Re：从零开始的异世界生活",
                    "Re:ゼロから始める異世界生活",
                    "Re:ZERO -Starting Life in Another World-",
                ],
                "tmdb_id": 65942,
            },
            "gaps": [{
                "id": "S00E55", "kind": "missing_episode", "season": 0,
                "title": "Re:Zero Break Time Season 3",
            }],
            "query_groups": [{"season": 0, "episode_titles": [
                "Re:Zero Break Time Season 3",
            ]}],
            "rules": {"optional_discovery_only": True},
        }
        bundle = select_replenishment_candidates(request, [{
            "provider": "cloud_share",
            "release_name": (
                "Re ZERO Starting Life in Another World S00E55 "
                "Re:Zero Break Time Season 3"
            ),
            "files": ["Re ZERO - S00E55 - ReZero Break Time Season 3.mkv"],
            "locator": "share:official-en-alias",
        }])
        self.assertEqual(bundle["status"], "complete")
        self.assertEqual(bundle["covered_gap_ids"], ["S00E55"])

    def test_regular_discovery_still_rejects_file_only_coverage(self):
        request = {
            "media": {"title": "Example", "aliases": ["Example"], "tmdb_id": 42},
            "gaps": [{"id": "S01E01", "kind": "missing_episode", "season": 1}],
            "query_groups": [{"season": 1, "season_names": ["Season 1"]}],
            "rules": {},
        }
        bundle = select_replenishment_candidates(request, [{
            "provider": "magnet", "release_name": "Example Complete 1080p",
            "files": ["Example S01E01.mkv"], "file_coverage": ["S01E01"],
            "resolution": "1080p", "availability": "metadata_verified",
            "locator": "torrent:https://example.invalid/1.torrent",
        }])
        self.assertEqual(bundle["status"], "no_match")

    def test_share_precedes_magnet_and_720p_is_fallback_only(self):
        request = self.request()
        candidates = [
            {"provider": "magnet", "release_name": "Example S04 complete", "name_coverage": ["S04E13", "S04E14"], "resolution": "2160p", "updated_at": "2026-07-27T00:00:00Z", "locator": "magnet:new"},
            {"provider": "cloud_share", "release_name": "Example S04 older 1080", "name_coverage": ["S04E13", "S04E14"], "resolution": "1080p", "updated_at": "2025-01-01T00:00:00Z", "locator": "share:1080"},
            {"provider": "cloud_share", "release_name": "Example S04 new 720", "name_coverage": ["S04E13", "S04E14"], "resolution": "720p", "updated_at": "2026-07-27T00:00:00Z", "locator": "share:720"},
        ]
        selected = select_replenishment_candidate(request, candidates)
        self.assertEqual(selected["locator"], "share:1080")

    def test_quark_fast_save_precedes_generic_share_and_torrent(self):
        selected = select_replenishment_candidate(self.request(), [
            {"provider": "magnet", "release_name": "Example S04E13-E14 2160p", "locator": "torrent:fixture"},
            {"provider": "cloud_share", "release_name": "Example S04E13-E14 1080p", "locator": "share:fixture"},
            {
                "provider": "quark_share", "release_name": "Example S04E13-E14 1080p",
                "locator": "quark-share:opaque", "availability": "verified",
                "acquisition": {
                    "kind": "quark_fast_save",
                    "file_id_by_gap": {"S04E13": ["f13"], "S04E14": ["f14"]},
                    "file_path_by_id": {"f13": "S04E13.mkv", "f14": "S04E14.mkv"},
                    "file_size_by_id": {"f13": 13, "f14": 14},
                },
            },
        ])
        self.assertEqual(selected["provider"], "quark_share")
        self.assertEqual(selected["locator"], "quark-share:opaque")

    def test_quark_magnet_offline_precedes_local_torrent_but_not_fast_save(self):
        base = {
            "release_name": "Example S04E13-E14 1080p",
            "name_coverage": ["S04E13", "S04E14"],
            "resolution": "1080p", "availability": "verified",
        }
        selected = select_replenishment_candidate(self.request(), [
            {**base, "provider": "magnet", "locator": "torrent:local"},
            {**base, "provider": "quark_magnet", "locator": "magnet:cloud",
             "acquisition": {"kind": "quark_magnet_offline"}},
        ])
        self.assertEqual(selected["provider"], "quark_magnet")
        selected = select_replenishment_candidate(self.request(), [
            {**base, "provider": "quark_magnet", "locator": "magnet:cloud",
             "acquisition": {"kind": "quark_magnet_offline"}},
            {**base, "provider": "quark_share", "locator": "quark-share:first",
             "acquisition": {
                 "kind": "quark_fast_save",
                 "file_id_by_gap": {"S04E13": ["f13"], "S04E14": ["f14"]},
                 "file_path_by_id": {"f13": "S04E13.mkv", "f14": "S04E14.mkv"},
                 "file_size_by_id": {"f13": 13, "f14": 14},
             }},
        ])
        self.assertEqual(selected["provider"], "quark_share")

    def test_exact_three_lane_chain_advances_only_after_higher_lane_exclusion(self):
        base = {
            "release_name": "Example S04E13-E14 1080p",
            "name_coverage": ["S04E13", "S04E14"],
            "resolution": "1080p", "availability": "verified",
        }
        share = {
            **base, "provider": "quark_share", "locator": "quark_share:exact",
            "acquisition": {
                "kind": "quark_fast_save", "share_id": "exact",
                "file_id_by_gap": {
                    "S04E13": ["share-13"], "S04E14": ["share-14"],
                },
                "file_path_by_id": {
                    "share-13": "S04E13.mkv", "share-14": "S04E14.mkv",
                },
                "file_size_by_id": {"share-13": 13, "share-14": 14},
            },
        }
        offline = {
            **base, "provider": "quark_magnet", "locator": "quark_magnet:exact",
            "acquisition": {"kind": "quark_magnet_offline"},
        }
        local = {
            **base, "provider": "magnet", "locator": "torrent:exact",
            "acquisition": {"kind": "torrent"},
        }
        request = self.unlock_local(self.request())
        first = select_replenishment_candidates(request, [local, offline, share])
        self.assertEqual(first["selections"][0]["provider"], "quark_share")
        self.assertEqual(
            [row["provider"] for row in first["provider_chain_by_gap"]["S04E13"]],
            ["quark_share", "quark_magnet", "magnet"],
        )

        request["excluded_candidates"] = [{
            "provider": "quark_share", "locator": share["locator"],
        }]
        second = select_replenishment_candidates(request, [local, offline, share])
        self.assertEqual(second["selections"][0]["provider"], "quark_magnet")

        request["excluded_candidates"].append({
            "provider": "quark_magnet", "locator": offline["locator"],
        })
        third = select_replenishment_candidates(request, [local, offline, share])
        self.assertEqual(third["selections"][0]["provider"], "magnet")

    def test_cloud_failure_does_not_unlock_local_without_permanent_exhaustion(self):
        base = {
            "release_name": "Example S04E13-E14 1080p",
            "name_coverage": ["S04E13", "S04E14"],
            "resolution": "1080p", "availability": "verified",
        }
        offline = {
            **base, "provider": "quark_magnet", "locator": "quark_magnet:exact",
            "acquisition": {"kind": "quark_magnet_offline"},
        }
        local = {
            **base, "provider": "magnet", "locator": "torrent:exact",
            "acquisition": {"kind": "torrent"},
        }
        request = self.request()
        request["excluded_candidates"] = [{
            "provider": "quark_magnet", "locator": offline["locator"],
            "until_epoch": 9999999999,
            "reason": "quark_magnet_infrastructure_failure",
        }]

        blocked = select_replenishment_candidates(request, [local, offline])

        self.assertEqual(blocked["selections"], [])
        self.assertEqual(blocked["rejection_reasons"]["local_torrent_locked"], 1)

        request["excluded_candidates"] = [{
            "provider": "quark_magnet", "locator": offline["locator"],
            "failure_scope": "candidate",
            "reason": "quark rejected this resource",
        }]
        fallback = select_replenishment_candidates(request, [local, offline])
        self.assertEqual(fallback["selections"], [])
        self.unlock_local(request)
        unlocked = select_replenishment_candidates(request, [local, offline])
        self.assertEqual(unlocked["selections"][0]["provider"], "magnet")

    def test_complete_cloud_exhaustion_unlocks_local_without_fake_failures(self):
        base = {
            "release_name": "Example S04E13-E14 1080p",
            "name_coverage": ["S04E13", "S04E14"],
            "resolution": "1080p", "availability": "verified",
        }
        local = {
            **base, "provider": "magnet", "locator": "torrent:local",
            "acquisition": {"kind": "torrent"},
        }
        request = self.request()
        request["rules"]["minimum_attempts_per_cloud_lane"] = 30
        request["provider_attempts"] = {"quark_share": 0, "quark_magnet": 0}

        share_stage = select_replenishment_candidates(request, [local])
        self.assertEqual(share_stage["selections"], [])
        self.assertEqual(share_stage["required_attempt_provider"], "quark_share")

        request["provider_attempts"] = {"quark_share": 30, "quark_magnet": 29}
        offline_stage = select_replenishment_candidates(request, [local])
        self.assertEqual(offline_stage["selections"], [])
        self.assertEqual(offline_stage["required_attempt_provider"], "quark_magnet")

        request["provider_attempts"] = {"quark_share": 30, "quark_magnet": 30}
        still_locked = select_replenishment_candidates(request, [local])
        self.assertEqual(still_locked["selections"], [])
        self.assertFalse(still_locked["local_torrent_unlocked"])

        self.unlock_local(request)
        request["provider_attempts"]["quark_magnet"] = 0
        local_stage = select_replenishment_candidates(request, [local])
        self.assertEqual(local_stage["selections"][0]["provider"], "magnet")
        self.assertTrue(local_stage["local_torrent_unlocked"])

    def test_incomplete_exhaustion_label_cannot_unlock_local_torrent(self):
        base = {
            "provider": "magnet",
            "release_name": "Example S04E13-E14 1080p",
            "name_coverage": ["S04E13", "S04E14"],
            "resolution": "1080p",
            "availability": "verified",
            "locator": "torrent:local",
            "acquisition": {"kind": "torrent"},
        }
        request = self.request()
        request["rules"]["minimum_attempts_per_cloud_lane"] = 30
        request["provider_attempts"] = {
            "quark_share": 30, "quark_magnet": 30,
        }
        request["provider_exhausted"] = {"quark_magnet": {
            "exhausted": True,
            "proof": {"kind": "search_complete_no_candidates"},
        }}

        bundle = select_replenishment_candidates(request, [base])

        self.assertEqual(bundle["selections"], [])
        self.assertFalse(bundle["provider_exhausted"]["quark_magnet"])
        self.assertFalse(bundle["local_torrent_unlocked"])
        self.assertEqual(bundle["rejection_reasons"]["local_torrent_locked"], 1)

    def test_resource_failure_floor_cannot_unlock_local_before_required_sources_exhaust(self):
        local = {
            "provider": "magnet",
            "release_name": "Example S04E13-E14 1080p",
            "name_coverage": ["S04E13", "S04E14"],
            "resolution": "1080p",
            "availability": "verified",
            "locator": "torrent:local",
            "acquisition": {"kind": "torrent"},
        }
        request = self.request()
        request["rules"]["minimum_attempts_per_cloud_lane"] = 30
        request["provider_attempts"] = {
            "quark_share": 30, "quark_magnet": 30,
        }
        request["provider_exhausted"] = {"quark_magnet": {
            "exhausted": True,
            "proof": {
                "kind": "resource_failure_floor_reached",
                "required_floor": 30,
                "distinct_failure_count": 30,
            },
        }}

        bundle = select_replenishment_candidates(request, [local])

        self.assertEqual(bundle["selections"], [])
        self.assertFalse(bundle["local_torrent_unlocked"])
        self.assertEqual(bundle["rejection_reasons"]["local_torrent_locked"], 1)

    def test_reaching_attempt_floor_does_not_skip_a_remaining_cloud_candidate(self):
        base = {
            "release_name": "Example S04E13-E14 1080p",
            "name_coverage": ["S04E13", "S04E14"],
            "resolution": "1080p", "availability": "verified",
        }
        request = self.request()
        request["rules"]["minimum_attempts_per_cloud_lane"] = 30
        request["provider_attempts"] = {"quark_share": 30, "quark_magnet": 30}
        bundle = select_replenishment_candidates(request, [
            {
                **base, "provider": "magnet", "locator": "torrent:local",
                "acquisition": {"kind": "torrent"},
            },
            {
                **base, "provider": "quark_magnet", "locator": "magnet:cloud",
                "acquisition": {"kind": "quark_magnet_offline"},
            },
        ])
        self.assertEqual(bundle["selections"][0]["provider"], "quark_magnet")

    def test_deterministic_cloud_identity_rejection_exposes_durable_locator(self):
        candidate = {
            "provider": "quark_magnet",
            "release_name": "Completely Different Show S01E01",
            "name_coverage": ["S04E13"],
            "resolution": "1080p",
            "availability": "verified",
            "locator": "quark_magnet:0123456789abcdef0123456789abcdef01234567",
            "infohash": "0123456789abcdef0123456789abcdef01234567",
            "acquisition": {"kind": "quark_magnet_offline"},
        }

        bundle = select_replenishment_candidates(self.request(), [candidate])

        self.assertEqual(bundle["selections"], [])
        self.assertEqual(bundle["rejection_reasons"], {"title_identity_mismatch": 1})
        self.assertEqual(bundle["durably_rejected_candidates"], [{
            "provider": "quark_magnet",
            "locator": candidate["locator"],
            "infohash": candidate["infohash"],
            "release_name": candidate["release_name"],
            "reason": "title_identity_mismatch",
        }])

    def test_quark_share_without_exact_fast_save_manifest_cannot_win(self):
        base = {
            "release_name": "Example S04E13-E14 1080p",
            "name_coverage": ["S04E13", "S04E14"],
            "resolution": "1080p", "availability": "verified",
        }
        bundle = select_replenishment_candidates(self.request(), [
            {**base, "provider": "quark_share", "locator": "quark_share:legacy"},
            {**base, "provider": "quark_magnet", "locator": "quark_magnet:exact",
             "acquisition": {"kind": "quark_magnet_offline"}},
        ])
        self.assertEqual(bundle["selections"][0]["provider"], "quark_magnet")
        self.assertEqual(bundle["rejection_reasons"]["provider_acquisition_mismatch"], 1)
        self.assertEqual(bundle["provider_diagnostics"]["quark_share"], {
            "candidate_count": 1,
            "eligible_candidate_count": 0,
            "rejection_reasons": {"provider_acquisition_mismatch": 1},
        })

    def test_excluded_quark_offline_lane_falls_back_to_local_same_btih(self):
        request = self.unlock_local(self.request())
        magnet = "magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567"
        request["excluded_candidates"] = [{
            "provider": "quark_magnet", "locator": "quark_magnet:0123456789abcdef",
        }]
        base = {
            "release_name": "Example S04E13-E14 1080p",
            "name_coverage": ["S04E13", "S04E14"], "resolution": "1080p",
        }
        selected = select_replenishment_candidate(request, [
            {**base, "provider": "quark_magnet",
             "locator": "quark_magnet:0123456789abcdef", "infohash": magnet},
            {**base, "provider": "magnet", "locator": "torrent:https://fixture/release.torrent",
             "infohash": "0123456789abcdef0123456789abcdef01234567"},
        ])
        self.assertEqual(selected["provider"], "magnet")

    def test_failed_local_lane_can_retry_same_btih_in_quark_cloud(self):
        request = self.request()
        infohash = "0123456789abcdef0123456789abcdef01234567"
        request["excluded_candidates"] = [{
            "provider": "magnet", "locator": "torrent:https://fixture/old.torrent",
            "infohash": infohash,
        }]
        base = {
            "release_name": "Example S04E13-E14 1080p",
            "name_coverage": ["S04E13", "S04E14"], "resolution": "1080p",
        }
        selected = select_replenishment_candidate(request, [{
            **base, "provider": "quark_magnet",
            "locator": f"quark_magnet:{infohash}", "infohash": infohash,
            "acquisition": {"kind": "quark_magnet_offline"},
        }])
        self.assertEqual(selected["provider"], "quark_magnet")

    def test_malformed_quark_magnet_provider_kind_never_enters_pool(self):
        base = {
            "release_name": "Example S04E13-E14 1080p",
            "name_coverage": ["S04E13", "S04E14"], "resolution": "1080p",
        }
        bundle = select_replenishment_candidates(self.unlock_local(self.request()), [
            {**base, "provider": "quark_magnet", "locator": "quark_magnet:bad",
             "acquisition": {"kind": "torrent"}},
            {**base, "provider": "magnet", "locator": "torrent:fallback",
             "acquisition": {"kind": "torrent"}},
        ])
        self.assertEqual(bundle["selections"][0]["provider"], "magnet")
        self.assertEqual(bundle["rejection_reasons"]["provider_acquisition_mismatch"], 1)

    def test_default_rules_never_jump_from_excluded_quark_share_to_local_torrent(self):
        request = self.request()
        request["excluded_candidates"] = [{"locator": "quark-share:expired"}]
        bundle = select_replenishment_candidates(request, [
            {
                "provider": "quark_share", "release_name": "Example S04E13-E14 1080p",
                "locator": "quark-share:expired",
                "acquisition": {
                    "kind": "quark_fast_save",
                    "file_id_by_gap": {"S04E13": ["f13"], "S04E14": ["f14"]},
                    "file_path_by_id": {"f13": "S04E13.mkv", "f14": "S04E14.mkv"},
                    "file_size_by_id": {"f13": 13, "f14": 14},
                },
            },
            {
                "provider": "magnet", "release_name": "Example S04E13-E14 1080p",
                "locator": "torrent:fallback", "acquisition": {"kind": "torrent"},
            },
        ])
        self.assertEqual(bundle["selections"], [])
        self.assertEqual(bundle["required_attempt_provider"], "quark_magnet")

    def test_old_resource_drops_to_720p_when_higher_tier_is_absent(self):
        request = self.request()
        selected = select_replenishment_candidate(request, [
            {"provider": "cloud_share", "release_name": "Example S04 720 old", "name_coverage": ["S04E13", "S04E14"], "resolution": "720p", "updated_at": "2024-01-01T00:00:00Z", "locator": "share:old"},
            {"provider": "cloud_share", "release_name": "Example S04 720 new", "name_coverage": ["S04E13", "S04E14"], "resolution": "720p", "updated_at": "2026-01-01T00:00:00Z", "locator": "share:new"},
        ])
        self.assertEqual(selected["locator"], "share:new")

    def test_newer_high_quality_candidate_precedes_older_4k(self):
        request = self.request()
        selected = select_replenishment_candidate(request, [
            {"provider": "cloud_share", "release_name": "Example S04 4K", "name_coverage": ["S04E13", "S04E14"], "resolution": "2160p", "updated_at": "2025-01-01T00:00:00Z", "locator": "share:4k-old"},
            {"provider": "cloud_share", "release_name": "Example S04 1080", "name_coverage": ["S04E13", "S04E14"], "resolution": "1080p", "updated_at": "2026-01-01T00:00:00Z", "locator": "share:1080-new"},
        ])
        self.assertEqual(selected["locator"], "share:1080-new")

    def test_acquisition_result_requires_a_ready_source(self):
        self.assertEqual(
            validate_acquisition_result({"status": "ready", "source_path": "/media/inbox/show"}),
            "/media/inbox/show",
        )
        with self.assertRaisesRegex(ValueError, "ready"):
            validate_acquisition_result({"status": "queued"})

    def test_release_name_can_prove_full_season_without_adapter_claim(self):
        bundle = select_replenishment_candidates(self.request(), [{
            "provider": "cloud_share", "release_name": "Example 第四季 1080P 全集",
            "resolution": "FHD", "updated_at": "2026-07-27T00:00:00Z",
            "locator": "share:season-four", "availability": "verified",
        }])
        self.assertEqual(bundle["status"], "complete")
        self.assertEqual(bundle["covered_gap_ids"], ["S04E13", "S04E14"])
        self.assertEqual(bundle["selections"][0]["resolution"], "1080p")

    def test_official_season_name_expands_queries_and_proves_semantic_coverage(self):
        plan = {
            "source_root": "/inbox/Example",
            "target_root": "/library/Example",
            "metadata": {"title": "Example", "tmdb_id": 42},
            "scan_report": {"resource_gaps": [
                {"kind": "missing_episode", "label": "S02E01 One", "season_name": "完结篇"},
                {"kind": "missing_episode", "label": "S02E02 Two", "season_name": "完结篇"},
            ]},
        }
        request = build_replenishment_request(
            plan, job_id="abc123abc123", round_number=1,
        )
        self.assertIn("Example 完结篇", request["search_queries"])
        bundle = select_replenishment_candidates(request, [{
            "provider": "cloud_share", "release_name": "Example 完结篇 1080P 全集",
            "locator": "share:semantic-season", "updated_at": "2026-07-27T00:00:00Z",
        }])
        self.assertEqual(bundle["status"], "complete")
        self.assertEqual(bundle["covered_gap_ids"], ["S02E01", "S02E02"])

    def test_file_listing_restricts_a_full_season_name_claim(self):
        bundle = select_replenishment_candidates(self.unlock_local(self.request()), [
            {
                "provider": "cloud_share", "release_name": "Example S04 complete 2160P",
                "file_coverage": ["S04E13"], "updated_at": "2026-07-27T00:00:00Z",
                "locator": "share:incomplete-listing",
            },
            {
                "provider": "magnet", "release_name": "Example S04E14 1080P",
                "file_coverage": ["S04E14"], "updated_at": "2026-07-26T00:00:00Z",
                "locator": "magnet:e14",
            },
        ])
        self.assertEqual(bundle["status"], "complete")
        self.assertEqual(
            [(item["provider"], item["selected_gap_ids"]) for item in bundle["selections"]],
            [("cloud_share", ["S04E13"]), ("magnet", ["S04E14"])],
        )

    def test_provider_style_season_dash_episode_names_prove_coverage(self):
        bundle = select_replenishment_candidates(self.unlock_local(self.request()), [
            {
                "provider": "magnet",
                "release_name": "[Provider] Example S4 - 13 (1080p) [HASH].mkv",
                "updated_at": "2026-07-26T00:00:00Z",
                "locator": "magnet:e13",
            },
            {
                "provider": "magnet",
                "release_name": "[Provider] Example Season 4 - 14 (1080p) [HASH].mkv",
                "updated_at": "2026-07-27T00:00:00Z",
                "locator": "magnet:e14",
            },
        ])
        self.assertEqual(bundle["status"], "complete")
        self.assertEqual(bundle["covered_gap_ids"], ["S04E13", "S04E14"])
        self.assertEqual(
            [item["selected_gap_ids"] for item in bundle["selections"]],
            [["S04E14"], ["S04E13"]],
        )

    def test_anime_batch_range_infers_the_only_requested_season(self):
        plan = {
            "target_root": "/library/Fate Strange Fake",
            "metadata": {"title": "Fate Strange Fake", "tmdb_id": 229858},
            "scan_report": {"resource_gaps": [
                {"kind": "missing_episode", "label": "S01E13"},
            ]},
        }
        request = build_replenishment_request(plan, job_id="abc123abc123", round_number=1)
        self.unlock_local(request)
        bundle = select_replenishment_candidates(request, [{
            "provider": "magnet",
            "release_name": "[Provider] Fate Strange Fake (01-13) (1080p) [Batch]",
            "updated_at": "2026-07-27T00:00:00Z",
            "locator": "magnet:fate-batch",
        }])
        self.assertEqual(bundle["status"], "complete")
        self.assertEqual(bundle["covered_gap_ids"], ["S01E13"])

    def test_anime_dash_episode_infers_the_only_requested_season(self):
        plan = {
            "target_root": "/library/Fate Strange Fake",
            "metadata": {"title": "Fate Strange Fake", "tmdb_id": 229858},
            "scan_report": {"resource_gaps": [
                {"kind": "missing_episode", "label": "S01E13"},
            ]},
        }
        request = build_replenishment_request(plan, job_id="abc123abc123", round_number=1)
        self.unlock_local(request)
        bundle = select_replenishment_candidates(request, [{
            "provider": "magnet",
            "release_name": "[Provider] Fate Strange Fake - 13 (1080p) [HASH].mkv",
            "updated_at": "2026-07-27T00:00:00Z",
            "locator": "magnet:fate-13",
        }])
        self.assertEqual(bundle["status"], "complete")
        self.assertEqual(bundle["covered_gap_ids"], ["S01E13"])

    def test_bracketed_anime_range_proves_named_missing_season(self):
        plan = {
            "target_root": "/library/Inuyasha",
            "metadata": {"title": "犬夜叉", "original_title": "Inuyasha", "tmdb_id": 35610},
            "scan_report": {"resource_gaps": [
                {
                    "kind": "missing_season",
                    "label": "S02 犬夜叉:完结篇",
                    "season_name": "犬夜叉:完结篇",
                    "expected_episode_count": 26,
                },
            ]},
        }
        request = build_replenishment_request(plan, job_id="abc123abc123", round_number=1)
        self.unlock_local(request)
        bundle = select_replenishment_candidates(request, [{
            "provider": "magnet",
            "release_name": "[Group][犬夜叉完结篇/Inuyasha The Final Act][01-26全集][1080P]",
            "files": [f"[Group][犬夜叉完结篇][{episode:02d}][1080P].mkv" for episode in range(1, 27)],
            "updated_at": "2021-05-28T05:15:08+08:00",
            "locator": "torrent:https://example.invalid/inuyasha.torrent",
        }])
        self.assertEqual(bundle["status"], "complete")
        self.assertEqual(bundle["covered_gap_ids"], ["S02"])

    def test_complete_episode_range_proves_a_missing_whole_season(self):
        request = build_replenishment_request({
            "source_root": "/inbox/Example",
            "metadata": {"title": "Example", "tmdb_id": 42},
            "scan_report": {"resource_gaps": [{
                "kind": "missing_season", "label": "Season 02 Final Act",
                "season_name": "Final Act", "expected_episode_count": 3,
            }]},
        }, job_id="abc123abc123", round_number=1)
        bundle = select_replenishment_candidates(request, [{
            "provider": "cloud_share", "release_name": "Example S02E01-E03 1080P",
            "file_coverage": ["S02E01-E03"], "locator": "share:complete-range",
            "updated_at": "2026-07-27T00:00:00Z",
        }])
        self.assertEqual(bundle["status"], "complete")
        self.assertEqual(bundle["covered_gap_ids"], ["S02"])

    def test_cloud_share_is_used_per_gap_then_magnet_fills_only_remainder(self):
        bundle = select_replenishment_candidates(self.unlock_local(self.request()), [
            {"provider": "cloud_share", "release_name": "Example S04E13 1080P", "locator": "share:e13", "updated_at": "2026-07-01T00:00:00Z"},
            {"provider": "magnet", "release_name": "Example S04E13-E14 2160P", "locator": "magnet:full", "updated_at": "2026-07-27T00:00:00Z"},
        ])
        self.assertEqual(bundle["status"], "complete")
        self.assertEqual(bundle["selections"][0]["locator"], "share:e13")
        self.assertEqual(bundle["selections"][0]["selected_gap_ids"], ["S04E13"])
        self.assertEqual(bundle["selections"][1]["selected_gap_ids"], ["S04E14"])

    def test_720p_is_allowed_only_for_gap_without_high_quality_hit(self):
        bundle = select_replenishment_candidates(self.request(), [
            {"provider": "cloud_share", "release_name": "Example S04E13 1080P", "locator": "share:e13-high", "updated_at": "2026-01-01T00:00:00Z"},
            {"provider": "cloud_share", "release_name": "Example S04E13-E14 720P", "locator": "share:both-low", "updated_at": "2026-07-27T00:00:00Z"},
        ])
        by_locator = {item["locator"]: item["selected_gap_ids"] for item in bundle["selections"]}
        self.assertEqual(by_locator, {
            "share:e13-high": ["S04E13"],
            "share:both-low": ["S04E14"],
        })

    def test_known_bad_and_wrong_title_candidates_are_reported(self):
        bundle = select_replenishment_candidates(self.request(), [
            {"provider": "cloud_share", "release_name": "Example S04 complete", "locator": "share:expired", "status": "share_expired"},
            {"provider": "cloud_share", "release_name": "Different S04 complete", "locator": "share:wrong"},
        ])
        self.assertEqual(bundle["status"], "no_match")
        self.assertEqual(bundle["rejection_reasons"], {
            "known_unavailable": 1,
            "title_identity_mismatch": 1,
        })

    def test_core_selector_enforces_cross_round_infohash_exclusion(self):
        request = self.unlock_local(self.request())
        hex_hash = "0123456789abcdef0123456789abcdef01234567"
        base32_hash = base64.b32encode(bytes.fromhex(hex_hash)).decode("ascii").rstrip("=")
        request["excluded_candidates"] = [{
            "locator": "torrent:https://old.example/release.torrent",
            "infohash": base32_hash,
        }]
        bundle = select_replenishment_candidates(request, [
            {
                "provider": "magnet", "release_name": "Example S04 complete 1080p",
                "name_coverage": ["S04E13", "S04E14"], "resolution": "1080p",
                "locator": "torrent:https://mirror.example/same-release.torrent",
                "infohash": hex_hash,
            },
            {
                "provider": "magnet", "release_name": "Example S04 complete 720p",
                "name_coverage": ["S04E13", "S04E14"], "resolution": "720p",
                "locator": "torrent:https://next.example/release.torrent",
                "infohash": "89abcdef0123456789abcdef0123456789abcdef",
            },
        ])
        self.assertEqual(bundle["status"], "complete")
        self.assertEqual(
            bundle["selections"][0]["locator"],
            "torrent:https://next.example/release.torrent",
        )
        self.assertEqual(bundle["rejection_reasons"], {"excluded_candidate": 1})

    def test_acquisition_accepts_multiple_materialized_directories(self):
        self.assertEqual(validate_acquisition_results({
            "status": "ready",
            "source_paths": ["/media/inbox/e13", "/media/inbox/e14", "/media/inbox/e13"],
        }), ["/media/inbox/e13", "/media/inbox/e14"])

    def test_common_episode_notations_improve_name_hits(self):
        candidates = [
            {"provider": "cloud_share", "release_name": "Example S04.E13 1080P", "locator": "share:dot"},
            {"provider": "cloud_share", "release_name": "Example 4x14 1080P", "locator": "share:x"},
        ]
        bundle = select_replenishment_candidates(self.request(), candidates)
        self.assertEqual(bundle["status"], "complete")
        self.assertEqual(bundle["covered_gap_ids"], ["S04E13", "S04E14"])

    def test_file_names_can_supply_episode_only_evidence_for_a_named_season(self):
        bundle = select_replenishment_candidates(self.request(), [{
            "provider": "cloud_share", "release_name": "Example 第四季 全集 1080P",
            "files": [{"name": "Example EP13.mkv"}, {"path": "Example 第14集.mkv"}],
            "locator": "share:file-list",
        }])
        self.assertEqual(bundle["status"], "complete")
        self.assertEqual(bundle["covered_gap_ids"], ["S04E13", "S04E14"])

    def test_file_path_season_marker_overrides_request_default_for_naked_episode(self):
        request = build_replenishment_request({
            "target_root": "/library/Example",
            "metadata": {"title": "Example", "year": "2026", "tmdb_id": 42},
            "scan_report": {"resource_gaps": [
                {"kind": "missing_episode", "label": "S01E01 Missing"},
            ]},
        }, job_id="abc123abc123", round_number=1)
        bundle = select_replenishment_candidates(request, [{
            "provider": "quark_magnet",
            "release_name": "Example Season 1 + Season 2 complete",
            "files": [
                {"path": "Season 2/Example After Story - 01.mkv"},
            ],
            "locator": "magnet:multi-season",
        }])
        self.assertEqual(bundle["status"], "no_match")
        self.assertEqual(bundle["covered_gap_ids"], [])

    def test_empty_file_listing_restricts_a_season_pack_claim(self):
        bundle = select_replenishment_candidates(self.request(), [{
            "provider": "cloud_share", "release_name": "Example S04 全集 1080P",
            "files": [], "locator": "share:empty",
        }])
        self.assertEqual(bundle["status"], "no_match")
        self.assertEqual(bundle["rejection_reasons"], {"name_or_file_coverage_miss": 1})

    def test_duplicate_locator_merges_eligible_coverage_instead_of_losing_later_hit(self):
        bundle = select_replenishment_candidates(self.request(), [
            {"provider": "cloud_share", "release_name": "Example S04E13 1080P", "locator": "share:same"},
            {"provider": "cloud_share", "release_name": "Example S04E14 1080P", "locator": "share:same"},
        ])
        self.assertEqual(bundle["status"], "complete")
        self.assertEqual(bundle["selections"][0]["selected_gap_ids"], ["S04E13", "S04E14"])
        self.assertEqual(bundle["rejection_reasons"], {"duplicate_locator": 1})

    def test_explicit_identity_mismatch_overrides_similar_release_title(self):
        bundle = select_replenishment_candidates(self.request(), [{
            "provider": "cloud_share", "release_name": "Example S04 全集 1080P",
            "tmdb_id": 99, "locator": "share:wrong-id",
        }])
        self.assertEqual(bundle["status"], "no_match")
        self.assertEqual(bundle["rejection_reasons"], {"title_identity_mismatch": 1})

    def test_live_replay_rejects_railgun_specials_stamped_with_index_tmdb_id(self):
        request = build_replenishment_request({
            "mode": "tv", "target_root": "/quark/影视/番剧/魔法禁书目录",
            "metadata": {
                "title": "魔法禁书目录", "original_title": "とある魔術の禁書目録",
                "tmdb_id": 30980,
            },
            "scan_report": {"resource_gaps": [{
                "kind": "missing_episode", "label": "S00E07 魔法禁书目录（茵蒂克丝）碳7",
                "season_name": "特别篇",
            }]},
        }, job_id="906b4be5d90d", round_number=1)
        candidate = {
            "provider": "cloud_share", "tmdb_id": 30980,
            "release_name": "某科学的超电磁炮动漫1-3季合集夸克网盘资源",
            "files": [{"path": (
                "某科学的超电磁炮S/EXTRA/Toaru Kagaku no Railgun S "
                "[SP07] OVA.mkv"
            )}],
            "name_coverage": ["S00E07"], "locator": "share:railgun-sibling",
        }
        bundle = select_replenishment_candidates(request, [candidate])
        self.assertEqual(bundle["status"], "no_match")
        self.assertEqual(bundle["rejection_reasons"], {"title_identity_mismatch": 1})

    def test_live_replay_rejects_live_action_adaptation_for_animation_request(self):
        request = build_replenishment_request({
            "mode": "tv", "target_root": "/quark/影视/番剧/【我推的孩子】",
            "metadata": {
                "title": "【我推的孩子】", "original_title": "【推しの子】",
                "tmdb_id": 203737,
            },
            "scan_report": {"resource_gaps": [{
                "kind": "missing_episode", "label": "S00E01 第 1 集",
                "season_name": "特别篇",
            }]},
        }, job_id="742f56129ccf", round_number=1)
        self.assertEqual(request["media"]["media_format"], "animation")
        candidate = {
            "provider": "cloud_share", "tmdb_id": 203737,
            "release_name": "《【我推的孩子】真人电视剧版 (2024)》",
            "name_coverage": ["S00E01"], "locator": "share:live-action",
        }
        bundle = select_replenishment_candidates(request, [candidate])
        self.assertEqual(bundle["status"], "no_match")
        self.assertEqual(bundle["rejection_reasons"], {"title_identity_mismatch": 1})

    def test_same_series_animation_special_remains_eligible(self):
        request = build_replenishment_request({
            "mode": "tv", "target_root": "/quark/影视/番剧/【我推的孩子】",
            "metadata": {
                "title": "【我推的孩子】", "original_title": "【推しの子】",
                "tmdb_id": 203737,
            },
            "scan_report": {"resource_gaps": [{
                "kind": "missing_episode", "label": "S00E01 第 1 集",
                "season_name": "特别篇",
            }]},
        }, job_id="742f56129ccf", round_number=1)
        bundle = select_replenishment_candidates(request, [{
            "provider": "cloud_share", "tmdb_id": 203737,
            "release_name": "【我推的孩子】动画 特别篇 S00E01 1080P",
            "name_coverage": ["S00E01"], "locator": "share:anime-special",
        }])
        self.assertEqual(bundle["status"], "complete")
        self.assertEqual(bundle["covered_gap_ids"], ["S00E01"])

    def test_provider_identity_assertion_and_shortened_title_cannot_bypass_guard(self):
        request = build_replenishment_request({
            "target_root": "/library/Long Example Title",
            "metadata": {"title": "Long Example Title", "tmdb_id": 42},
            "scan_report": {"resource_gaps": [{
                "kind": "missing_episode", "label": "S01E01 Missing",
            }]},
        }, job_id="abc123abc123", round_number=1)
        bundle = select_replenishment_candidates(request, [{
            "provider": "cloud_share", "tmdb_id": 42, "identity_match": True,
            "release_name": "Example S01E01 1080P",
            "locator": "share:unverified-short-title",
        }])
        self.assertEqual(bundle["status"], "no_match")
        self.assertEqual(bundle["rejection_reasons"], {"title_identity_mismatch": 1})

    def test_batch_gaps_are_split_by_unique_season_identity(self):
        batch = build_replenishment_requests({
            "mode": "batch", "source_root": "/inbox/Bundle", "target_root": "/library",
            "metadata": {"title": "Bundle", "member_tv": {
                "/library/Main": {
                    "tmdb_id": 42, "title": "Main", "original_title": "Main Original",
                    "year": "2020", "season_posters": {"1": "/a.jpg", "4": "/b.jpg"},
                },
                "/library/Side": {
                    "tmdb_id": 43, "title": "Side", "year": "2021",
                    "season_posters": {"1": "/c.jpg"},
                },
            }},
            "scan_report": {"resource_gaps": [{
                "kind": "missing_episode", "label": "S04E13 Missing",
            }]},
        }, job_id="abc123abc123", round_number=1)
        self.assertEqual(len(batch["requests"]), 1)
        self.assertEqual(batch["requests"][0]["media"]["tmdb_id"], 42)
        self.assertIn("Main Original", batch["requests"][0]["media"]["aliases"])
        self.assertEqual(batch["unresolved_gaps"], [])

    def test_explicit_gap_media_identity_wins_in_multi_title_plan(self):
        batch = build_replenishment_requests({
            "mode": "batch", "source_root": "/inbox/Bundle", "target_root": "/library",
            "metadata": {"title": "Bundle", "member_tv": {
                "/library/A": {"tmdb_id": 42, "title": "A", "year": "2020"},
                "/library/B": {"tmdb_id": 43, "title": "B", "year": "2021"},
            }},
            "scan_report": {"resource_gaps": [{
                "kind": "missing_episode", "label": "S01E01 Missing",
                "media": {"tmdb_id": 43},
            }]},
        }, job_id="abc123abc123", round_number=1)
        self.assertEqual(batch["requests"][0]["media"]["tmdb_id"], 43)


if __name__ == "__main__":
    unittest.main()
