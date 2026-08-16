"""Tests for the fractional-special mapping (data-ized N.5 answers)."""

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
        season_episodes: list[dict[str, Any]],
        specials: list[dict[str, Any]],
    ) -> None:
        self.season_episodes = season_episodes
        self.specials = specials

    def get(self, path: str, **params: Any) -> dict[str, Any]:
        del params
        if path.endswith("/season/0"):
            return {"episodes": self.specials}
        if path.endswith("/season/1"):
            return {"episodes": self.season_episodes}
        return {"episodes": []}


SAO_S1 = [
    {"episode_number": n, "air_date": f"2012-{7 + (n % 6):02d}-{1 + (n % 27):02d}",
     "runtime": 24}
    for n in range(1, 26)
]


class FractionalSpecialMappingTests(unittest.TestCase):
    def test_lexicon_data_covers_the_three_sao_recaps(self) -> None:
        rows = FRACTIONAL_SPECIAL_ALIASES[45782]
        self.assertEqual(rows["18.5"], ("S00E23", "第18.5话 Recollection"))
        self.assertEqual(rows["24.5"], ("S00E24", "第0话 Reflection"))
        self.assertEqual(rows["36.5"], ("S00E25", "第12.5话 回忆"))

    def test_explicit_fractional_title_ignores_the_far_future_veto(self) -> None:
        # [18.5] matches the official S00E23 title literally, but the special
        # aired in 2019 while the season context is 2012.  The literal title
        # must win over the season-timeline veto.
        tmdb = FakeTMDB(
            SAO_S1,
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
        # [24.5] has no official title containing "24.5"; only the lexicon
        # row says it is S00E24 第0话 Reflection.
        tmdb = FakeTMDB(
            SAO_S1,
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

    def test_unknown_fractional_still_fails_closed(self) -> None:
        tmdb = FakeTMDB(SAO_S1, [])
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
