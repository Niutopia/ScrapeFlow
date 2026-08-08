"""Cross-boundary regressions for the single episode-coordinate parser."""

from __future__ import annotations

import unittest

from engine.scrapeflow.replenishment_matching import (
    audit_episode_tokens,
    coverage_tokens,
    episode_ranges,
    expanded_episode_ids,
    fractional_episode_tokens,
    season_markers,
)
from local.scrapeflow_api.replenishment import (
    _coverage_tokens,
    _episode_ranges,
    _expanded_episode_ids,
    _season_markers,
    build_replenishment_request,
)
from local.scrapeflow_api.simple_library_audit import _episode_tokens


class EpisodeMatchingConvergenceTests(unittest.TestCase):
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
        self.assertEqual(_episode_tokens(f"/library/{value}"), expected_audit)

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
        self.assertEqual(_episode_tokens(f"/library/{value}", default_season=1), set())
        self.assertEqual(
            fractional_episode_tokens(value, default_seasons={1}),
            {"S01E01.5"},
        )
        self.assertEqual(
            fractional_episode_tokens("Example.Show.S01E01.5v2.mkv", default_seasons={1}),
            {"S01E01.5"},
        )

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
