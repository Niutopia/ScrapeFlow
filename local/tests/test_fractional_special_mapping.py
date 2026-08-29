"""Tests for the fractional-special mapping (generic cross-season intervals)."""

from __future__ import annotations

import unittest
from typing import Any

from engine.scrapeflow.core import _fractional_recap_evidence_candidates
from engine.scrapeflow.data.release_lexicon import FRACTIONAL_SPECIAL_ALIASES
from engine.scrapeflow.models import EpisodeKey


class FakeTMDB:
    """Minimal TMDB double for the fractional-special evidence gate."""

    def __init__(
        self,
        seasons: dict[int, list[dict[str, Any]]],
        specials: list[dict[str, Any]],
    ) -> None:
        self.seasons = seasons
        self.specials = specials

    def get(self, path: str, **params: Any) -> dict[str, Any]:
        del params
        if path.endswith("/season/0"):
            return {"episodes": self.specials}
        for season, rows in self.seasons.items():
            if path.endswith(f"/season/{season}"):
                return {"episodes": rows}
        if not path.endswith("/season/0") and path.count("/") == 2:
            return {
                "seasons": [
                    {"season_number": season, "name": f"S{season}"}
                    for season in sorted(self.seasons)
                ],
            }
        return {"episodes": []}


def _episodes(first: int, last: int, year: int) -> list[dict[str, Any]]:
    return [
        {"episode_number": n, "air_date": f"{year}-{1 + (n % 9):02d}-{1 + (n % 27):02d}",
         "runtime": 24}
        for n in range(first, last + 1)
    ]


SAO_S1 = _episodes(1, 25, 2012)


class FractionalSpecialMappingTests(unittest.TestCase):
    def test_lexicon_data_covers_the_three_sao_recaps(self) -> None:
        rows = FRACTIONAL_SPECIAL_ALIASES[45782]
        self.assertEqual(rows["18.5"], ("S00E23", "第18.5话 Recollection"))
        self.assertEqual(rows["24.5"], ("S00E24", "第0话 Reflection"))
        self.assertEqual(rows["36.5"], ("S00E25", "第12.5话 回忆"))

    def test_explicit_fractional_title_ignores_the_far_future_veto(self) -> None:
        tmdb = FakeTMDB(
            {1: SAO_S1},
            [{"episode_number": 23, "name": "第18.5话 Recollection",
              "air_date": "2019-02-17", "runtime": 24}],
        )
        candidates, _reason = _fractional_recap_evidence_candidates(
            tmdb, 45782, 1,
            EpisodeKey(kind="fractional", number=18, fractional_digits="5"),
            {(0, 23): ["第18.5话 Recollection"]},
            [{"name": "[TUDO] Sword Art Online Alicization [18.5].mkv",
              "full_path": "/x/[18.5].mkv"}],
        )
        self.assertEqual([(item[0], item[1]) for item in candidates], [(0, 23)])

    def test_data_ized_mapping_bridges_a_numberless_title(self) -> None:
        tmdb = FakeTMDB(
            {1: SAO_S1},
            [{"episode_number": 24, "name": "第0话 Reflection",
              "air_date": "2019-10-06", "runtime": 24}],
        )
        candidates, _reason = _fractional_recap_evidence_candidates(
            tmdb, 45782, 1,
            EpisodeKey(kind="fractional", number=24, fractional_digits="5"),
            {(0, 24): ["第0话 Reflection"]},
            [{"name": "[TUDO] Sword Art Online Alicization War of Underworld [24.5].mkv",
              "full_path": "/x/[24.5].mkv"}],
        )
        self.assertEqual([(item[0], item[1]) for item in candidates], [(0, 24)])

    def test_cross_season_interval_matches_without_any_data_row(self) -> None:
        # A series OUTSIDE the lexicon: N=24 is the season-2 finale, and the
        # recap airs between S2E24 and S3E01.  The generic interval evidence
        # must match even though the planning season is 1.
        tmdb = FakeTMDB(
            {1: _episodes(1, 24, 2015),
             2: _episodes(1, 24, 2019),
             3: _episodes(1, 23, 2020)},
            [{"episode_number": 2, "name": "第0话 Reflection",
              "air_date": "2019-10-06", "runtime": 24}],
        )
        candidates, _reason = _fractional_recap_evidence_candidates(
            tmdb, 999, 1,
            EpisodeKey(kind="fractional", number=24, fractional_digits="5"),
            {(0, 2): ["第0话 Reflection"]},
            [{"name": "Some.Show.War.of.Underworld.[24.5].mkv",
              "full_path": "/x/[24.5].mkv"}],
        )
        self.assertEqual([(item[0], item[1]) for item in candidates], [(0, 2)])

    def test_season_finale_fractional_uses_next_season_opener_as_bound(self) -> None:
        # A [13.5] recap inside a 13-episode season folder: the upper bound is
        # the next season's E01.
        tmdb = FakeTMDB(
            {1: _episodes(1, 13, 2016),
             2: _episodes(1, 13, 2017)},
            [{"episode_number": 3, "name": "第13.5话 总集篇",
              "air_date": "2016-12-25", "runtime": 24}],
        )
        candidates, _reason = _fractional_recap_evidence_candidates(
            tmdb, 998, 1,
            EpisodeKey(kind="fractional", number=13, fractional_digits="5"),
            {(0, 3): ["第13.5话 总集篇"]},
            [{"name": "Durarara.S1.[13.5].mkv", "full_path": "/x/[13.5].mkv"}],
        )
        self.assertEqual([(item[0], item[1]) for item in candidates], [(0, 3)])

    def test_non_half_decimal_maps_when_official_title_carries_same_label(self) -> None:
        """``24.9`` maps when TMDB's special title literally says 第24.9话.

        The evidence gate was reserved for the conventional N.5 half-episode
        form, so the Tensura ``[24.9]`` sidecar failed closed even though
        TMDB's Season 00 E07 title is ``第24.9话 闲话：日向·坂口`` — the same
        explicit-label evidence class the N.5 path scores highest.  A non-N.5
        decimal now passes only with that verbatim official label; without it
        the fail-closed verdict stands.
        """
        specials = [
            {"episode_number": 7, "name": "第24.9话 闲话：日向·坂口",
             "air_date": "2021-01-05", "runtime": 24},
            {"episode_number": 8, "name": "第36.5话 闲话：维鲁多拉日记2",
             "air_date": "2021-06-29", "runtime": 24},
        ]
        titles = {
            (0, 7): ["第24.9话 闲话：日向·坂口"],
            (0, 8): ["第36.5话 闲话：维鲁多拉日记2"],
        }
        candidates, reason = _fractional_recap_evidence_candidates(
            FakeTMDB({1: SAO_S1}, specials), 82684, 1,
            EpisodeKey(kind="fractional", number=24, fractional_digits="9"),
            titles,
            [{"name": "[Ygm] Show 2nd Season [24.9][Ma10p].mkv",
              "full_path": "/x/[24.9].mkv"}],
        )
        self.assertTrue(candidates, reason)
        best = max(candidates, key=lambda row: row[4] if len(row) > 4 else 0)
        targets = {row[1] for row in candidates if isinstance(row[1], int)}
        self.assertIn(7, targets)

    def test_non_half_decimal_without_official_label_stays_closed(self) -> None:
        """No official title carrying ``24.9`` keeps the non-N.5 gate shut."""
        specials = [
            {"episode_number": 1, "name": "Totally unrelated special",
             "air_date": "2021-01-05", "runtime": 24},
        ]
        candidates, reason = _fractional_recap_evidence_candidates(
            FakeTMDB({1: SAO_S1}, specials), 82684, 1,
            EpisodeKey(kind="fractional", number=24, fractional_digits="9"),
            {(0, 1): ["Totally unrelated special"]},
            [{"name": "[Ygm] Show 2nd Season [24.9][Ma10p].mkv",
              "full_path": "/x/[24.9].mkv"}],
        )
        self.assertEqual(candidates, [])
        self.assertTrue(reason)

    def test_unknown_fractional_still_fails_closed(self) -> None:
        tmdb = FakeTMDB({1: SAO_S1}, [])
        candidates, reason = _fractional_recap_evidence_candidates(
            tmdb, 45782, 1,
            EpisodeKey(kind="fractional", number=99, fractional_digits="5"),
            {},
            [{"name": "[TUDO] Something [99.5].mkv", "full_path": "/x/[99.5].mkv"}],
        )
        self.assertEqual(candidates, [])
        self.assertTrue(reason)  # fail-closed with a bounded reason


if __name__ == "__main__":
    unittest.main()
