"""C-layer regressions for named-season windows and releaseless movie rows."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from engine.scrapeflow.identity_matching import auto_match_tmdb
from engine.scrapeflow.root_boundaries import analyze_root_boundaries
from engine.scrapeflow.unit_identity import resolve_work_unit_identities

from local.tests.test_root_boundaries import DictAList


class _NamedSeasonTMDB:
    """A parent show whose Season 2 is the named continuation arc."""

    def __init__(self) -> None:
        self.movie_detail_reads: list[int] = []

    def get(self, path: str, **params: object) -> dict[str, object]:
        query = str(params.get("query") or "").strip()
        if path == "/search/tv":
            return {
                "results": [{
                    "id": 210,
                    "name": "Northwind Show",
                    "first_air_date": "2000-10-16",
                    "genre_ids": [16],
                }],
            }
        if path == "/search/movie":
            if query == "北风完结篇":
                return {
                    "results": [{
                        "id": 1587181,
                        "title": "北风完结篇",
                        "release_date": "",
                        "genre_ids": [16],
                    }],
                }
            return {"results": []}
        if path == "/tv/210":
            return {
                "name": "Northwind Show",
                "original_name": "Northwind Show",
                "first_air_date": "2000-10-16",
                "number_of_episodes": 38,
                "seasons": [
                    {
                        "season_number": 1,
                        "name": "Northwind Show",
                        "air_date": "2000-10-16",
                        "episode_count": 12,
                    },
                    {
                        "season_number": 2,
                        "name": "北风完结篇",
                        "air_date": "2009-10-04",
                        "episode_count": 26,
                    },
                ],
            }
        if path == "/tv/210/alternative_titles":
            return {"results": [{"title": "北风完结篇"}]}
        if path == "/movie/1587181":
            self.movie_detail_reads.append(1587181)
            return {"title": "北风完结篇", "release_date": "", "runtime": 0}
        if path == "/movie/1587181/alternative_titles":
            return {"titles": []}
        raise AssertionError(f"unexpected TMDB path: {path}")


class NamedSeasonWindowTests(unittest.TestCase):
    def test_query_with_continuation_year_resolves_parent_show(self) -> None:
        """``北风完结篇`` + 2009 anchors the named Season 2, not a junk film.

        The exact-title junk movie row has neither a release date nor a
        runtime; the real parent show aired 2000, so its first-air year
        conflicts with the boundary year until the official season window
        is probed.
        """
        tmdb = _NamedSeasonTMDB()
        match, _ = auto_match_tmdb(
            tmdb,
            "北风完结篇 (2009)",
            media_type=None,
            min_confidence=0.88,
        )
        self.assertEqual(match.status, "confirmed")
        self.assertEqual((match.media_type, match.tmdb_id), ("tv", 210))
        self.assertEqual(
            match.decision_trace.get("season_window_year"),
            "2009",
        )

    def test_releaseless_movie_row_is_never_confirmed(self) -> None:
        """A movie with neither release date nor runtime cannot win."""

        class _JunkOnlyTMDB:
            def get(self, path: str, **params: object) -> dict[str, object]:
                if path == "/search/tv":
                    return {"results": []}
                if path == "/search/movie":
                    return {
                        "results": [{
                            "id": 1587181,
                            "title": "北风完结篇",
                            "release_date": "",
                            "genre_ids": [16],
                        }],
                    }
                if path == "/movie/1587181":
                    return {"title": "北风完结篇", "release_date": "", "runtime": 0}
                if path.endswith("/alternative_titles"):
                    return {"results": [], "titles": []}
                raise AssertionError(f"unexpected TMDB path: {path}")

        with self.assertRaises(Exception) as caught:
            auto_match_tmdb(_JunkOnlyTMDB(), "北风完结篇", media_type=None, min_confidence=0.88)
        self.assertNotIn("confirmed", str(caught.exception).lower() or "x")
        self.assertTrue(
            str(caught.exception).startswith(("自动匹配", "TMDB")),
            msg=str(caught.exception),
        )

    def test_boundary_evidence_resolves_the_parent_show(self) -> None:
        """B→C on the continuation-season folder shape confirms the parent."""
        root = "/incoming/04 北风完结篇（2009）全26集 1080P"
        alist = DictAList({
            root: [
                *[
                    {
                        "name": f"{number:02d}「第{number}话」.mkv",
                        "is_dir": False,
                        "size": 300 * 1024 * 1024,
                    }
                    for number in range(1, 27)
                ],
                {"name": "01「第1话」.ass", "is_dir": False, "size": 10240},
            ],
        })
        tmdb = _NamedSeasonTMDB()
        with tempfile.TemporaryDirectory() as directory:
            state_root = Path(directory)
            analyze_root_boundaries(
                alist,
                root,
                root_task_id="root-named-season-window",
                state_root=state_root,
            )
            resolved = resolve_work_unit_identities(
                tmdb,
                state_root,
                "root-named-season-window",
                prefer_animation=True,
            )

        self.assertEqual(len(resolved), 1)
        self.assertEqual(resolved[0].identity_status, "confirmed")
        identity = resolved[0].identity or {}
        self.assertEqual((identity.get("media_type"), identity.get("tmdb_id")), ("tv", 210))


if __name__ == "__main__":
    unittest.main()
