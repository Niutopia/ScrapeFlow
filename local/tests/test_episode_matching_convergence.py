"""Cross-boundary regressions for the single episode-coordinate parser."""

from __future__ import annotations

import unittest

from engine.scrapeflow.replenishment_matching import (
    audit_episode_tokens,
    bare_regular_episode_context_is_safe,
    bare_regular_episode_number,
    bracketed_regular_episode_number,
    coverage_tokens,
    episode_ranges,
    expanded_episode_ids,
    fractional_episode_tokens,
    release_dash_regular_episode,
    season_markers,
)
from engine.scrapeflow.core import extract_episode_key, _filter_media
from engine.scrapeflow.identity_matching import _query_from_source
from local.scrapeflow_api.replenishment import (
    _coverage_tokens,
    _episode_ranges,
    _expanded_episode_ids,
    _season_markers,
    build_replenishment_request,
)


class EpisodeMatchingConvergenceTests(unittest.TestCase):
    def test_bare_year_is_not_an_episode_number(self) -> None:
        """A four-digit year in a release name is metadata, not an episode."""
        # ``S01.2021`` reads as "Season 1, year 2021", never episode 2021.
        key = extract_episode_key("86.Eighty-Six.S01.2021.1080p.BluRay")
        self.assertNotEqual(getattr(key, "number", None), 2021)
        # The canonical SxxExx token still wins for real files.
        self.assertEqual(
            extract_episode_key("86.Eighty-Six.S01E01.2021.1080p.mkv").number, 1
        )
        self.assertEqual(
            extract_episode_key("86.Eighty-Six.S00E02.2021.1080p.mkv").number, 2
        )

    def test_advertisement_banner_is_not_media(self) -> None:
        """A ``www.<domain>`` download banner stays at source, not in media."""
        filtered = _filter_media([
            {
                "name": "【更多高清剧集下载请访问 www.BPHDTV.com】.mkv",
                "full_path": "/x/【更多高清剧集下载请访问 www.BPHDTV.com】.mkv",
                "is_dir": False,
            },
            {
                "name": "[Ygm] Show [01][Ma10p].mkv",
                "full_path": "/x/[Ygm] Show [01][Ma10p].mkv",
                "is_dir": False,
            },
        ])
        self.assertEqual([item["name"] for item in filtered], ["[Ygm] Show [01][Ma10p].mkv"])

    def test_full_width_metadata_brackets_keep_the_title(self) -> None:
        """Chinese packaging metadata brackets drop, the title bracket unwraps."""
        query = _query_from_source(
            "【重生计划 ReLIFE （2016）】【4K】【日语中字】"
            "【类型：恋爱 校园 青春 治愈 励志】【全 13 集】"
        )
        self.assertIn("ReLIFE", query)
        self.assertNotIn("4K", query)
        self.assertNotIn("日语中字", query)
        self.assertNotIn("全 13 集", query)

    def assert_converged_integer_coordinate(
        self,
        value: str,
        *,
        expected_ranges: list[tuple[int, int, int]],
        expected_ids: set[str],
        expected_audit: set[tuple[int, int]],
    ) -> None:
        self.assertEqual(episode_ranges(value), expected_ranges)
        self.assertEqual(_episode_ranges(value), expected_ranges)
        self.assertEqual(expanded_episode_ids(value), expected_ids)
        self.assertEqual(_expanded_episode_ids(value), expected_ids)
        self.assertEqual(coverage_tokens([value]), expected_ids)
        self.assertEqual(_coverage_tokens([value]), expected_ids)
        self.assertEqual(audit_episode_tokens(f"/library/{value}"), expected_audit)

    def test_dual_ordinal_prefers_the_explicit_season_local_episode(self) -> None:
        value = "[BeanSub] Slime S04-89 [S4][17_89].mkv"
        self.assert_converged_integer_coordinate(
            value,
            expected_ranges=[(4, 17, 17)],
            expected_ids={"S04E17"},
            expected_audit={(4, 17)},
        )
        self.assertEqual(season_markers(value), {4})
        self.assertEqual(_season_markers(value), {4})

    def test_x_notation_range_has_identical_provider_and_audit_coverage(self) -> None:
        self.assert_converged_integer_coordinate(
            "Example.Show.4x017-018.mkv",
            expected_ranges=[(4, 17, 18)],
            expected_ids={"S04E17", "S04E18"},
            expected_audit={(4, 17), (4, 18)},
        )

    def test_chinese_season_and_episode_are_shared_coordinates(self) -> None:
        self.assert_converged_integer_coordinate(
            "示例.第四季.第十七集.mkv",
            expected_ranges=[(4, 17, 17)],
            expected_ids={"S04E17"},
            expected_audit={(4, 17)},
        )

    def test_fractional_episode_never_satisfies_an_integer_gap(self) -> None:
        value = "Example.Show.S01E01.5.mkv"
        self.assertEqual(episode_ranges(value), [])
        self.assertEqual(_episode_ranges(value), [])
        self.assertEqual(expanded_episode_ids(value), set())
        self.assertEqual(_expanded_episode_ids(value), set())
        self.assertEqual(coverage_tokens([value], default_seasons={1}), set())
        self.assertEqual(_coverage_tokens([value], default_seasons={1}), set())
        self.assertEqual(audit_episode_tokens(f"/library/{value}", default_season=1), set())
        self.assertEqual(
            fractional_episode_tokens(value, default_seasons={1}),
            {"S01E01.5"},
        )
        self.assertEqual(
            fractional_episode_tokens("Example.Show.S01E01.5v2.mkv", default_seasons={1}),
            {"S01E01.5"},
        )

    def test_bare_regular_evidence_rejects_ranges_seasons_and_specials(self) -> None:
        self.assertEqual(
            bare_regular_episode_number("/incoming/Example/Example.E01.mkv"),
            1,
        )
        for value in (
            "/incoming/Example/Example.E01-E02.mkv",
            "/incoming/Example/Example.E01E02.mkv",
            "/incoming/Example/Example.E01E02E03.mkv",
            "/incoming/Example/Example.S01E01.mkv",
            "/incoming/Season 01/Example.E01.mkv",
            "/incoming/Example/Example.SP.E01.mkv",
            "/incoming/Example/Example.OVA.E01.mkv",
            "/incoming/Example/Example.E01.5.mkv",
        ):
            with self.subTest(value=value):
                self.assertIsNone(bare_regular_episode_number(value))

    def test_bare_e_context_ignores_batch_parent_but_rejects_hierarchy(self) -> None:
        self.assertTrue(
            bare_regular_episode_context_is_safe(
                "/incoming/Example/Example.E01-E06/Example.E01.mkv",
            ),
        )
        for value in (
            "/incoming/Example/Season 01/Example.E01.mkv",
            "/incoming/Example/S01E01/Example.E01.mkv",
            "/incoming/Example/OVA/Example.E01.mkv",
            "/incoming/Example/SP/Example.E01.mkv",
        ):
            with self.subTest(value=value):
                self.assertFalse(bare_regular_episode_context_is_safe(value))

    def test_pure_bracketed_ordinal_rejects_ambiguous_and_special_forms(self) -> None:
        self.assertEqual(
            bracketed_regular_episode_number(
                "[Ygm] Example Show [01][Ma10p_2160p][x265_flac_ass].mkv"
            ),
            1,
        )
        for value in (
            "Example Show [1080].mkv",
            "Example Show [01][1080].mkv",
            "Example Show [01][02].mkv",
            "Example Show [01v2].mkv",
            "Example Show [01-02].mkv",
            "Example Show E01 [01].mkv",
            "Example Show S01E01 [01].mkv",
            "/incoming/OVA/Example Show [01].mkv",
        ):
            with self.subTest(value=value):
                self.assertIsNone(bracketed_regular_episode_number(value))

    def test_release_dash_ordinal_keeps_a_stable_title_prefix(self) -> None:
        """``Title - 01`` is a D/F proof primitive, not generic coverage."""
        self.assertEqual(
            release_dash_regular_episode(
                "[LoliHouse] Akuyaku Reijou Level 99 - 01 "
                "[WebRip 1080p HEVC-10bit AAC SRTx2].mkv"
            ),
            ("[lolihouse] akuyaku reijou level 99", 1),
        )
        for value in (
            "Title - 01 - 02 [WebRip].mkv",
            "Title S01 - 01 [WebRip].mkv",
            "Title - 01.5 [WebRip].mkv",
            "Title - 01 [OVA].mkv",
            "Title - 01 [MV].mkv",
            "Title - 01 [CM].mkv",
            "Title - 01 [OP2].mkv",
            "Title - 01 [ED].mkv",
            "Title - 01 [MENU].mkv",
            "Title - 01 [02].mkv",
            "Title - 01 [01-02].mkv",
            "Title - 01 [E01].mkv",
            "Title - 01 (EP 01).mkv",
            "Title - 01 1080p.mkv",
            "1080 - 01 [WebRip].mkv",
        ):
            with self.subTest(value=value):
                self.assertIsNone(release_dash_regular_episode(value))

    def test_resolution_suffix_does_not_turn_an_integer_episode_into_a_fraction(self) -> None:
        self.assert_converged_integer_coordinate(
            "Example.Show.S01E01.1080p.mkv",
            expected_ranges=[(1, 1, 1)],
            expected_ids={"S01E01"},
            expected_audit={(1, 1)},
        )
        self.assertEqual(
            fractional_episode_tokens("Example.Show.S01E01.1080p.mkv"),
            set(),
        )

    def test_request_normalization_uses_the_same_dual_ordinal_coordinate(self) -> None:
        request = build_replenishment_request(
            {
                "metadata": {"title": "Slime", "tmdb_id": 82684},
                "scan_report": {"resource_gaps": [{
                    "kind": "missing_episode",
                    "label": "Slime S04-89 [S4][17_89]",
                }]},
            },
            job_id="convergence-test",
            round_number=1,
        )
        self.assertEqual(request["gaps"][0]["id"], "S04E17")
        self.assertEqual(request["gaps"][0]["episodes"], [17])
