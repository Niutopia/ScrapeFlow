"""F-layer regressions for numbered child runs inside one TV release root."""

from __future__ import annotations

import unittest
from pathlib import Path

from engine.scraper import build_tv_plan_smart


FAKE_VIDEO_SIZE = 300 * 1024 * 1024


class _StubAList:
    def try_list(self, _path: str, refresh: bool = True) -> list[dict[str, object]]:
        del refresh
        return []

    def walk(self, _path: str, **_kwargs: object) -> list[dict[str, object]]:
        return []


class _NumberedRunTMDB:
    """A season table plus a junk movie that answers bare-ordinal queries."""

    def __init__(self, junk_movie_id: int, junk_queries: set[str]) -> None:
        self.junk_movie_id = junk_movie_id
        self.junk_queries = junk_queries
        self.movie_queries: list[str] = []

    def get(self, path: str, **params: object) -> dict[str, object]:
        query = str(params.get("query") or "").strip()
        if path == "/search/movie":
            self.movie_queries.append(query)
            if query in self.junk_queries:
                return {
                    "results": [{
                        "id": self.junk_movie_id,
                        "title": f"#{query}",
                        "release_date": "2019-01-01",
                        "genre_ids": [99],
                    }],
                }
            return {"results": []}
        if path == "/search/tv":
            return {"results": []}
        if path == "/tv/210":
            return {
                "name": "Northwind Show",
                "original_name": "Northwind Show",
                "first_air_date": "2020-01-01",
                "seasons": [
                    {"season_number": 1, "episode_count": 12},
                    {"season_number": 2, "episode_count": 8},
                ],
            }
        if path == "/tv/210/season/1":
            return {
                "episodes": [
                    {
                        "episode_number": number,
                        "name": f"第{number}话",
                        "air_date": f"2020-01-{number:02d}",
                        "runtime": 24,
                    }
                    for number in range(1, 13)
                ],
            }
        if path == "/tv/210/season/2":
            return {
                "episodes": [
                    {
                        "episode_number": number,
                        "name": f"第二季第{number}话",
                        "air_date": f"2021-01-{number:02d}",
                        "runtime": 24,
                    }
                    for number in range(1, 9)
                ],
            }
        if path == "/tv/210/season/0":
            return {"episodes": []}
        if path == "/tv/210/alternative_titles":
            return {"results": []}
        if path == f"/movie/{self.junk_movie_id}":
            return {
                "title": "#001",
                "release_date": "2019-01-01",
                "runtime": 5,
                "overview": "",
                "poster_path": "",
                "genres": [],
            }
        if path == f"/movie/{self.junk_movie_id}/alternative_titles":
            return {"titles": []}
        raise AssertionError(f"unexpected TMDB path: {path}")


def _numbered_video(source_root: str, segment: str, number: int) -> dict[str, object]:
    name = f"{number:03d}.mkv"
    return {
        "name": name,
        "full_path": (
            source_root
            + "/1080P 日中双语 内封简繁英字幕（NF.WEB-DL.x264.DDP.2.0.2Audio-Huawei）"
            + f"/{segment}/{name}"
        ),
        "size": FAKE_VIDEO_SIZE + number * 1024,
        "is_dir": False,
    }


class SmartChildNumberedRunTests(unittest.TestCase):
    def test_bare_ordinal_segment_names_never_become_movie_identity_queries(self) -> None:
        """``001-006``/``007-012`` folders stay release-local season numbering.

        A junk TMDB movie titled ``#001`` (2019) answers the bare-ordinal
        query exactly.  The child-work pass must not turn that collision
        into an independent-movie sub-plan that swallows the numbered run
        and leaves its siblings behind as residue.
        """
        source_root = "/quark/影视/待刮削/Northwind Show（2020）全12集 1080P"
        source_files = [
            *(_numbered_video(source_root, "001-006", number) for number in range(1, 7)),
            *(_numbered_video(source_root, "007-012", number) for number in range(7, 13)),
        ]
        tmdb = _NumberedRunTMDB(875820, {"001"})
        plan = build_tv_plan_smart(
            auto_episode_mode=True,
            alist=_StubAList(),
            tmdb_client=tmdb,
            src_path=source_root,
            parent_path="/quark/影视/番剧",
            tmdb_id=210,
            season=1,
            absolute=False,
            prefer_simplified=True,
            allow_unmapped=False,
            ignore_orphan_temp=False,
            source_files=source_files,
            media_root="/quark/影视",
        )

        self.assertEqual(plan.mode, "tv")
        self.assertNotIn("member_movies", plan.metadata)
        self.assertEqual(
            sorted(item.episode_key for item in plan.files),
            [f"E{number:02d}" for number in range(1, 13)],
        )
        self.assertFalse(
            any(Path(item.final_name).stem.startswith("#") for item in plan.files)
        )

    def test_titled_child_segment_with_episode_run_is_not_one_movie(self) -> None:
        """A titled folder holding a numbered run is not a single film.

        Even when the folder label itself confirms a same-titled junk movie,
        a segment with several episode-keyed videos is release-local season
        numbering, never one movie's file set.
        """
        source_root = "/quark/影视/待刮削/Northwind Show（2020）全12集 1080P"
        source_files = [
            *(_numbered_video(source_root, "花絮合辑", number) for number in range(1, 7)),
        ]
        tmdb = _NumberedRunTMDB(875821, {"花絮合辑"})
        plan = build_tv_plan_smart(
            auto_episode_mode=True,
            alist=_StubAList(),
            tmdb_client=tmdb,
            src_path=source_root,
            parent_path="/quark/影视/番剧",
            tmdb_id=210,
            season=1,
            absolute=False,
            prefer_simplified=True,
            allow_unmapped=False,
            ignore_orphan_temp=False,
            source_files=source_files,
            media_root="/quark/影视",
        )

        self.assertEqual(plan.mode, "tv")
        self.assertNotIn("member_movies", plan.metadata)
        self.assertEqual(
            sorted(item.episode_key for item in plan.files),
            [f"E{number:02d}" for number in range(1, 7)],
        )
        self.assertFalse(
            any(Path(item.final_name).stem.startswith("#") for item in plan.files)
        )


if __name__ == "__main__":
    unittest.main()


class MisplacedForeignSeasonSubtitleTests(unittest.TestCase):
    def test_misplaced_foreign_season_subtitles_do_not_abort_sibling_seasons(self) -> None:
        """A foreign season's sidecars under another season never kill the plan.

        ``第二季/备份字幕/`` can carry Season 3 external subtitles the
        uploader misplaced.  They group as an undeclared Season 3 bucket with
        zero videos; executing that bucket as a TV sub-plan raises the generic
        "no video" failure and aborts the whole otherwise-valid Season 2 plan.
        The subtitle-only bucket must be preserved in the source with a
        warning instead, exactly like a declared subtitle-only season.
        """

        class SubtitleAList:
            def try_list(self, _path: str, refresh: bool = True) -> list[dict[str, object]]:
                del refresh
                return []

            def walk(self, _path: str, **_kwargs: object) -> list[dict[str, object]]:
                return []

        class ThreeSeasonTMDB:
            def get(self, path: str, **_params: object) -> dict[str, object]:
                if path == "/tv/650":
                    return {
                        "name": "Psychic Show",
                        "original_name": "Psychic Show",
                        "first_air_date": "2016-07-12",
                        "seasons": [
                            {"season_number": 0, "episode_count": 9},
                            {"season_number": 1, "episode_count": 12},
                            {"season_number": 2, "episode_count": 13},
                            {"season_number": 3, "episode_count": 12},
                        ],
                    }
                if path.startswith("/tv/650/season/"):
                    season = int(path.rsplit("/", 1)[1])
                    counts = {0: 9, 1: 12, 2: 13, 3: 12}
                    return {
                        "episodes": [
                            {
                                "episode_number": number,
                                "name": f"Episode {number}",
                                "air_date": f"2016-07-{number:02d}",
                                "runtime": 24,
                            }
                            for number in range(1, counts.get(season, 0) + 1)
                        ],
                    }
                if path == "/tv/650/alternative_titles":
                    return {"results": []}
                raise AssertionError(f"unexpected TMDB path: {path}")

        season_root = "/quark/影视/待刮削/L 4k Psychic Show/第二季"
        source_files = [
            {
                "name": f"[Ygm] Psychic Show II [{number:02d}][Ma10p_2160p].mkv",
                "full_path": (
                    season_root
                    + f"/[Ygm] Psychic Show II [{number:02d}][Ma10p_2160p].mkv"
                ),
                "size": FAKE_VIDEO_SIZE + number * 1024,
                "is_dir": False,
            }
            for number in range(1, 14)
        ]
        source_files.extend(
            {
                "name": f"[Ygm] Psychic Show III [{number:02d}][Ma10p_2160p].ass",
                "full_path": (
                    season_root
                    + "/备份字幕/"
                    + f"[Ygm] Psychic Show III [{number:02d}][Ma10p_2160p].ass"
                ),
                "size": 40_000,
                "is_dir": False,
            }
            for number in range(1, 13)
        )
        plan = build_tv_plan_smart(
            auto_episode_mode=True,
            alist=SubtitleAList(),
            tmdb_client=ThreeSeasonTMDB(),
            src_path=season_root,
            parent_path="/quark/影视/番剧",
            tmdb_id=650,
            season=2,
            absolute=False,
            prefer_simplified=True,
            allow_unmapped=False,
            ignore_orphan_temp=False,
            source_files=source_files,
            source_declared_seasons=(2,),
            media_root="/quark/影视",
        )

        planned = {item.source_path for item in plan.files}
        self.assertEqual(len(planned), 13)
        self.assertFalse(
            any("备份字幕" in path for path in planned),
            "misplaced foreign-season subtitles must not be written",
        )
        self.assertFalse(
            any(
                "备份字幕" in str(item.source_path)
                for item in [*plan.cleanup_files, *plan.problem_files]
            ),
        )
        deferred = plan.scan_report.get("deferred_subtitle_only_seasons") or []
        self.assertTrue(
            any(entry.get("season") == 3 for entry in deferred),
            msg=deferred,
        )
        self.assertTrue(
            any("Season 03" in warning for warning in plan.warnings),
            msg=plan.warnings,
        )


class InWindowPartialAbsoluteRunTests(unittest.TestCase):
    def test_mid_window_absolute_tail_run_normalizes_to_season_relative(self) -> None:
        """``[79]..[83]`` under ``第四季`` is Season 4 E07-E11, not E79-E83.

        A 24+24+24 show whose Season 4 absolute window is 73-96 can arrive
        as a multi-season cohort whose Season 4 folder carries only the
        season's tail (the first six episodes already live in the formal
        library).  The per-season sub-plan's run starts mid-window, so
        neither the season-boundary anchor nor the full-count local offset
        applied, and the whole cohort plan failed with unmapped E79-E83
        before the absolute fallback could rescue a single-season request.
        Every key inside the season's absolute window, all keys absolute,
        and a consecutive run prove the offset mechanically.
        """

        class TailAList:
            def try_list(self, _path: str, refresh: bool = True) -> list[dict[str, object]]:
                del refresh
                return []

            def walk(self, _path: str, **_kwargs: object) -> list[dict[str, object]]:
                return []

        class TailTMDB:
            def get(self, path: str, **_params: object) -> dict[str, object]:
                if path == "/tv/82684":
                    return {
                        "name": "Slime Show",
                        "original_name": "Slime Show",
                        "first_air_date": "2018-10-01",
                        "seasons": [
                            {"season_number": 0, "episode_count": 16},
                            {"season_number": 1, "episode_count": 24},
                            {"season_number": 2, "episode_count": 24},
                            {"season_number": 3, "episode_count": 24},
                            {"season_number": 4, "episode_count": 24},
                        ],
                    }
                if path.startswith("/tv/82684/season/"):
                    season = int(path.rsplit("/", 1)[1])
                    counts = {0: 16, 1: 24, 2: 24, 3: 24, 4: 24}
                    return {
                        "episodes": [
                            {
                                "episode_number": number,
                                "name": f"Episode {number}",
                                "air_date": f"2018-10-{number:02d}",
                                "runtime": 24,
                            }
                            for number in range(1, counts.get(season, 0) + 1)
                        ],
                    }
                if path == "/tv/82684/alternative_titles":
                    return {"results": []}
                if path in ("/search/tv", "/search/movie"):
                    return {"results": []}
                raise AssertionError(f"unexpected TMDB path: {path}")

        source_root = "/quark/影视/待刮削/G 4k Slime Show"
        source_files = [
            # Season 1 head (a couple of episodes are enough for the cohort).
            *(
                {
                    "name": f"[Ygm] Slime Show [{number}][Ma10p_2160p][x265_flac_ass].mkv",
                    "full_path": (
                        source_root
                        + f"/关于我转生变成史莱姆这档事 第一季/[Ygm] Slime Show [{number}][Ma10p_2160p][x265_flac_ass].mkv"
                    ),
                    "size": FAKE_VIDEO_SIZE + number * 1024,
                    "is_dir": False,
                }
                for number in range(1, 3)
            ),
            # Season 4 absolute tail inside the season-4 directory.
            *(
                {
                    "name": f"[Ygm] Slime Show 4rd Season [{number}][Ma10p_2160p][x265_aac_ass].mkv",
                    "full_path": (
                        source_root
                        + f"/关于我转生变成史莱姆这档事 第四季/[Ygm] Slime Show 4rd Season [{number}][Ma10p_2160p][x265_aac_ass].mkv"
                    ),
                    "size": FAKE_VIDEO_SIZE + number * 1024,
                    "is_dir": False,
                }
                for number in range(79, 84)
            ),
        ]
        plan = build_tv_plan_smart(
            auto_episode_mode=True,
            alist=TailAList(),
            tmdb_client=TailTMDB(),
            src_path=source_root,
            parent_path="/quark/影视/番剧",
            tmdb_id=82684,
            season=1,
            absolute=False,
            prefer_simplified=True,
            allow_unmapped=False,
            ignore_orphan_temp=False,
            source_files=source_files,
            source_declared_seasons=(1, 2, 3, 4),
            media_root="/quark/影视",
        )

        season4_targets = sorted(
            item.final_name
            for item in plan.files
            if item.media_kind == "video" and "S04" in item.final_name
        )
        self.assertEqual(len(season4_targets), 5, msg=season4_targets)
        for expected in ("S04E07", "S04E08", "S04E09", "S04E10", "S04E11"):
            self.assertTrue(
                any(expected in name for name in season4_targets),
                msg=season4_targets,
            )
        self.assertFalse(
            any("E79" in name or "E83" in name for name in season4_targets),
            msg=season4_targets,
        )
