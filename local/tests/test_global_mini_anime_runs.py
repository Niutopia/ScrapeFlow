"""Regression tests: a globally numbered mini-anime run under ``SPs/``.

Re:Zero Break Time 3rd is released as ``[Mini Anime 51]``..``[66]`` and those
ordinals ARE the global TMDB Season 00 ordinals (S00E51..E66, every row a
3-minute short).  The preclassifier still withholds every video inside a
bonus directory because a mini-series ordinal is usually release-local, so
the smart planner's bonus retry must re-admit the run only when the complete
consecutive labeled ordinals coincide exactly with official Season 00
short-extra rows at the same numbers.  Frieren's ``[Mini Anime 01]``..``[11]``
shows why the gate must stay strict: its official Season 00 interleaves
full-length specials (E05/E12), so a release-local restart can never satisfy
the run coincidence and must remain a preserved-at-source residual.
"""

from __future__ import annotations

import unittest

from engine.scrapeflow.core import (
    _map_global_mini_anime_runs,
    build_tv_plan_smart,
    extract_episode_key,
)

FAKE_VIDEO_SIZE = 5_000_000

_REZERO_NAME = (
    "[hyakuhuyu&VCB-Studio] Re Zero kara Hajimeru Isekai Seikatsu 3rd "
    "Season [Mini Anime {number}][Ma10p_1080p][x265_flac].mkv"
)
_FRIEREN_NAME = (
    "[Nekomoe kissaten&VCB-Studio] Sousou no Frieren "
    "[Mini Anime {number:02d}][Ma10p_1080p][x265_flac].mkv"
)


def _video(name: str, full_path: str) -> dict[str, object]:
    return {
        "name": name,
        "full_path": full_path,
        "size": FAKE_VIDEO_SIZE,
        "is_dir": False,
    }


class MiniAnimeParserTests(unittest.TestCase):
    def test_label_bracket_shapes(self) -> None:
        cases = {
            _REZERO_NAME.format(number=51): ("special", 51),
            _FRIEREN_NAME.format(number=1): ("special", 1),
            "[Kano&Ygm] Sousou no Frieren Mini Anime [05][BDRip].mkv": (
                "special",
                5,
            ),
            "ミニアニメ 03.ass": ("special", 3),
            "葬送的芙莉莲 迷你动画 07.mkv": ("special", 7),
            # A plain episode bracket beside the label is NOT hijacked.
            "[VCB-Studio] Re Zero 3rd Season [51][Ma10p_1080p].mkv": (
                "regular",
                51,
            ),
            # The SP-batch vocabulary keeps its established meaning.
            "[VCB-Studio] Re Zero [SP01_01][Ma10p_1080p].mkv": ("special", 1),
        }
        for text, (kind, number) in cases.items():
            with self.subTest(text=text):
                key = extract_episode_key(text)
                self.assertIsNotNone(key)
                assert key is not None  # for the type checker
                self.assertEqual((key.kind, key.number), (kind, number))


class GlobalMiniAnimeMapperTests(unittest.TestCase):
    def test_a_global_run_matching_official_short_rows_is_re_admitted(self) -> None:
        items = [
            _video(
                _REZERO_NAME.format(number=number),
                f"/src/06/[VCB-Studio] 3rd [Fin]/SPs/f{number}.mkv",
            )
            for number in range(51, 67)
        ]
        runtimes = {number: 3 for number in range(1, 82)}
        self.assertEqual(_map_global_mini_anime_runs(items, runtimes), 16)
        self.assertEqual(
            [(item["_episode_kind_override"], item["_episode_key_override"]) for item in items],
            [("special", number) for number in range(51, 67)],
        )

    def test_a_release_local_restart_interleaving_full_specials_stays_withheld(self) -> None:
        items = [
            _video(
                _FRIEREN_NAME.format(number=number),
                f"/src/S1/[VCB-Studio] S1 [Fin]/SPs/f{number}.mkv",
            )
            for number in range(1, 12)
        ]
        # Official rows E05/E12 are full-length specials, so the labeled run
        # {1..11} does not coincide with a contiguous official short run.
        runtimes = {**{n: 3 for n in (1, 2, 3, 4, 6, 7, 8, 9, 10, 11, 13)}, 5: 24, 12: 24}
        self.assertEqual(_map_global_mini_anime_runs(items, runtimes), 0)
        self.assertNotIn("_episode_key_override", items[0])

    def test_a_start_at_one_run_stays_reserved_for_stronger_evidence(self) -> None:
        # A run that restarts at 1 is the release-local shape (Frieren), so
        # even when the official Season 00 rows E01..E03 happen to be one
        # contiguous short run the 1:1 ordinal guess is not ours to make;
        # the embedded-official-ordinal reclamation owns that boundary.
        items = [
            _video(
                _FRIEREN_NAME.format(number=number),
                f"/src/S1/[VCB-Studio] S1 [Fin]/SPs/f{number}.mkv",
            )
            for number in (1, 2, 3)
        ]
        self.assertEqual(_map_global_mini_anime_runs(items, {1: 3, 2: 3, 3: 3}), 0)
        self.assertNotIn("_episode_key_override", items[0])

    def test_a_gappy_or_single_file_run_proves_nothing(self) -> None:
        runtimes = {number: 3 for number in range(1, 82)}
        gappy = [
            _video(
                _REZERO_NAME.format(number=number),
                f"/src/06/SPs/f{number}.mkv",
            )
            for number in (51, 52, 55)
        ]
        self.assertEqual(_map_global_mini_anime_runs(gappy, runtimes), 0)
        single = [
            _video(_REZERO_NAME.format(number=51), "/src/06/SPs/f51.mkv")
        ]
        self.assertEqual(_map_global_mini_anime_runs(single, runtimes), 0)

    def test_a_full_length_official_row_blocks_the_run_and_overrides_win(self) -> None:
        items = [
            _video(
                _REZERO_NAME.format(number=number),
                f"/src/06/SPs/f{number}.mkv",
            )
            for number in range(51, 57)
        ]
        # E54 is officially a full-length episode: ordinal identity broken.
        runtimes = {**{n: 3 for n in range(51, 57) if n != 54}, 54: 24}
        self.assertEqual(_map_global_mini_anime_runs(items, runtimes), 0)

        # An earlier mapper's explicit override must never be rewritten.
        protected = [
            _video(_REZERO_NAME.format(number=51), "/src/06/SPs/f51.mkv"),
            _video(_REZERO_NAME.format(number=52), "/src/06/SPs/f52.mkv"),
        ]
        protected[0]["_episode_kind_override"] = "regular"
        protected[0]["_episode_key_override"] = 9
        runtimes_ok = {51: 3, 52: 3}
        self.assertEqual(_map_global_mini_anime_runs(protected, runtimes_ok), 0)
        self.assertEqual(protected[0]["_episode_key_override"], 9)
        self.assertNotIn("_episode_key_override", protected[1])

    def test_subtitles_and_missing_runtimes_are_ignored(self) -> None:
        runtimes = {51: 3, 52: 3}
        items = [
            _video(_REZERO_NAME.format(number=51), "/src/06/SPs/f51.mkv"),
            {
                "name": _REZERO_NAME.format(number=52).replace(".mkv", ".CHS.ass"),
                "full_path": "/src/06/SPs/f52.ass",
                "size": 100,
                "is_dir": False,
            },
        ]
        self.assertEqual(_map_global_mini_anime_runs(items, runtimes), 0)
        # A run whose official rows carry no runtime stays fail-closed.
        no_runtime = [
            _video(_REZERO_NAME.format(number=51), "/src/06/SPs/f51.mkv"),
            _video(_REZERO_NAME.format(number=52), "/src/06/SPs/f52.mkv"),
        ]
        self.assertEqual(_map_global_mini_anime_runs(no_runtime, {}), 0)


class _SmartPlannerAList:
    """Minimal AList double: source listing only, no library writes needed."""

    def __init__(self, source_files: list[dict[str, object]]) -> None:
        self._source_files = source_files

    def walk(self, path: str, **_kwargs: object) -> list[dict[str, object]]:
        return [
            dict(item)
            for item in self._source_files
            if str(item["full_path"]).startswith(path.rstrip("/") + "/")
        ]

    def list(self, path: str, refresh: bool = False) -> list[dict[str, object]]:
        del refresh
        return self.walk(path)

    def try_list(self, path: str, refresh: bool = True) -> list[dict[str, object]]:
        del refresh
        return self.walk(path)

    def exact_file_info(self, path: str) -> dict[str, object] | None:
        for item in self._source_files:
            if item["full_path"] == path:
                return {"size": item["size"]}
        return None

    def mkdir(self, _path: str) -> None:
        return

    def ensure_directory(self, _path: str) -> None:
        return


def _make_tmdb(special_runtimes: dict[int, int]) -> object:
    class PlannerTMDB:
        language = "zh-CN"

        def get(self, path: str, **_kwargs: object) -> dict[str, object]:
            if path == "/tv/210":
                return {
                    "name": "Northwind Show",
                    "original_name": "Northwind Show",
                    "first_air_date": "2020-01-01",
                    "seasons": [{"season_number": 1, "episode_count": 2}],
                }
            if path == "/tv/210/season/1":
                return {
                    "episodes": [
                        {
                            "episode_number": 1,
                            "name": "启程",
                            "air_date": "2020-01-01",
                            "runtime": 24,
                        },
                        {
                            "episode_number": 2,
                            "name": "山道",
                            "air_date": "2020-01-08",
                            "runtime": 24,
                        },
                    ]
                }
            if path == "/tv/210/season/0":
                return {
                    "episodes": [
                        {
                            "episode_number": number,
                            "name": f"休息时间 {number}",
                            "air_date": f"2024-10-{(number % 28) + 1:02d}",
                            "runtime": runtime,
                        }
                        for number, runtime in sorted(
                            special_runtimes.items()
                        )
                    ]
                }
            if path == "/tv/210/alternative_titles":
                return {"results": []}
            raise AssertionError(f"unexpected TMDB path: {path}")

    return PlannerTMDB()


class SmartPlanGlobalMiniAnimeTests(unittest.TestCase):
    def _kwargs(self, source_files: list[dict[str, object]], tmdb: object) -> dict[str, object]:
        return {
            "auto_episode_mode": True,
            "alist": _SmartPlannerAList(source_files),
            "tmdb_client": tmdb,
            "src_path": "/quark/影视/待刮削/Northwind",
            "parent_path": "/quark/影视/番剧",
            "tmdb_id": 210,
            "season": 1,
            "absolute": False,
            "prefer_simplified": True,
            "allow_unmapped": False,
            "ignore_orphan_temp": False,
            "source_files": source_files,
            "source_declared_seasons": (1,),
            "media_root": "/quark/影视",
        }

    def test_global_mini_anime_run_is_planned_into_season_00(self) -> None:
        source_root = "/quark/影视/待刮削/Northwind"
        source_files = [
            {
                "name": "Northwind.Show.S01E01.mkv",
                "full_path": source_root + "/Northwind.Show.S01E01.mkv",
                "size": FAKE_VIDEO_SIZE,
                "is_dir": False,
            },
            {
                "name": "Northwind.Show.S01E02.mkv",
                "full_path": source_root + "/Northwind.Show.S01E02.mkv",
                "size": FAKE_VIDEO_SIZE,
                "is_dir": False,
            },
            *(
                _video(
                    _REZERO_NAME.format(number=number).replace(
                        "Re Zero kara Hajimeru Isekai Seikatsu 3rd Season",
                        "Northwind Show",
                    ),
                    source_root + f"/SPs/Mini Anime {number}.mkv",
                )
                for number in range(51, 67)
            ),
        ]
        runtimes = {number: 3 for number in range(1, 82)}
        plan = build_tv_plan_smart(**self._kwargs(source_files, _make_tmdb(runtimes)))
        self.assertEqual(
            sorted(item.episode_key for item in plan.files),
            ["E01", "E02", *(f"SP{number}" for number in range(51, 67))],
        )
        special_names = [
            item.final_name
            for item in plan.files
            if item.episode_key.startswith("SP")
        ]
        self.assertTrue(all("S00E" in name for name in special_names))
        self.assertTrue(
            any("迷你动画" in warning or "Season 00" in warning for warning in plan.warnings)
        )
        residuals = plan.scan_report.get("preserved_source_residuals", [])
        self.assertFalse(
            any("/SPs/" in str(row.get("source_path", "")) for row in residuals)
        )

    def test_interleaved_official_rows_keep_the_run_fail_closed(self) -> None:
        source_root = "/quark/影视/待刮削/Northwind"
        source_files = [
            {
                "name": "Northwind.Show.S01E01.mkv",
                "full_path": source_root + "/Northwind.Show.S01E01.mkv",
                "size": FAKE_VIDEO_SIZE,
                "is_dir": False,
            },
            {
                "name": "Northwind.Show.S01E02.mkv",
                "full_path": source_root + "/Northwind.Show.S01E02.mkv",
                "size": FAKE_VIDEO_SIZE,
                "is_dir": False,
            },
            *(
                _video(
                    _FRIEREN_NAME.format(number=number).replace(
                        "Sousou no Frieren", "Northwind Show"
                    ),
                    source_root + f"/SPs/Mini Anime {number:02d}.mkv",
                )
                for number in range(1, 12)
            ),
        ]
        # E05/E12 are full-length: the labeled {1..11} run is release-local.
        runtimes = {
            **{n: 3 for n in (1, 2, 3, 4, 6, 7, 8, 9, 10, 11, 13)},
            5: 24,
            12: 24,
        }
        plan = build_tv_plan_smart(**self._kwargs(source_files, _make_tmdb(runtimes)))
        self.assertEqual(
            sorted(item.episode_key for item in plan.files),
            ["E01", "E02"],
        )
        self.assertEqual(
            {
                str(row.get("source_path", ""))
                for row in plan.scan_report.get("preserved_source_residuals", [])
            },
            {
                source_root + f"/SPs/Mini Anime {number:02d}.mkv"
                for number in range(1, 12)
            },
        )


if __name__ == "__main__":
    unittest.main()
