"""Tests for the scoped gap re-audit (phantom-gap closure)."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from engine.scrapeflow.gap_ledger import Gap, load_gap_ledger, save_gap_ledger
from engine.scrapeflow.work_units import WorkUnitRecord, save_work_unit_records

from local.scrapeflow_api.gap_reaudit import reaudit_open_gaps
from local.scrapeflow_api.simple_engine_runner import SimpleEngineRunner

from local.tests.test_library_index import IndexAList
from local.tests.test_simple_engine_runner import FAKE_VIDEO_BYTES


class _BrokenListAList(IndexAList):
    """AList double whose listing always fails (re-audit must fail closed)."""

    def list(self, path: str, refresh: bool = False) -> list:
        raise OSError("backend down")


def _work_unit(root_task_id: str, unit_id: str, work_root: str | None) -> WorkUnitRecord:
    return WorkUnitRecord(
        work_unit_id=unit_id,
        root_task_id=root_task_id,
        boundary_key=unit_id,
        source_paths=(f"/待刮削/{unit_id}",),
        source_revision=1,
        role="single_work",
        media_context="tv",
        identity_status="confirmed",
        identity={"media_type": "tv", "tmdb_id": 45782, "title": "Sword Art Online"},
        matched_work_root=work_root,
    )


def _episode_gap(root_task_id: str, unit_id: str, season: int, episode: int) -> Gap:
    token = f"S{season:02d}E{episode:02d}"
    return Gap(
        gap_id=f"{unit_id}::missing_episode::{token}",
        root_task_id=root_task_id,
        work_unit_id=unit_id,
        kind="missing_episode",
        media_type="tv",
        tmdb_id=45782,
        season=season,
        episodes=(episode,),
        subtitle_path=None,
        subtitle_language=None,
        status="open",
    )


class GapReauditTests(unittest.TestCase):
    def _setup(self, library_files: dict[str, bytes], *, alist_class=IndexAList):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        state_root = Path(temp.name)
        alist = alist_class(library_files)
        runner = SimpleEngineRunner(
            state_root, alist=alist, tmdb=object(),
            validate=False, library_root="/library",
        )
        return state_root, alist, runner

    def test_present_coordinates_close_and_missing_stay_open(self) -> None:
        library = {
            "/library/番剧/刀剑神域/Season 01/S01E01.mkv": FAKE_VIDEO_BYTES,
            "/library/番剧/刀剑神域/Season 01/S01E02.mkv": FAKE_VIDEO_BYTES,
        }
        state_root, _alist, runner = self._setup(library)
        save_work_unit_records(state_root, "root-1", [
            _work_unit("root-1", "unit-main", "/library/番剧/刀剑神域"),
        ])
        save_gap_ledger(state_root, "root-1", [
            _episode_gap("root-1", "unit-main", 1, 1),   # present -> close
            _episode_gap("root-1", "unit-main", 1, 2),   # present -> close
            _episode_gap("root-1", "unit-main", 1, 3),   # missing -> stay
        ])

        result = reaudit_open_gaps(runner, state_root, "root-1")

        self.assertEqual(
            sorted(result["closed"]),
            [
                "unit-main::missing_episode::S01E01",
                "unit-main::missing_episode::S01E02",
            ],
        )
        self.assertEqual(result["kept_open"], ["unit-main::missing_episode::S01E03"])
        statuses = {
            gap.gap_id: gap.status
            for gap in load_gap_ledger(state_root, "root-1")
        }
        self.assertEqual(statuses["unit-main::missing_episode::S01E01"], "closed")
        self.assertEqual(statuses["unit-main::missing_episode::S01E03"], "open")

    def test_nested_season_dirs_are_walked(self) -> None:
        library = {
            "/library/番剧/刀剑神域/Season 03/Sub/S03E01.mkv": FAKE_VIDEO_BYTES,
        }
        state_root, _alist, runner = self._setup(library)
        save_work_unit_records(state_root, "root-1", [
            _work_unit("root-1", "unit-main", "/library/番剧/刀剑神域"),
        ])
        save_gap_ledger(state_root, "root-1", [
            _episode_gap("root-1", "unit-main", 3, 1),
        ])

        result = reaudit_open_gaps(runner, state_root, "root-1")

        self.assertEqual(result["closed"], ["unit-main::missing_episode::S03E01"])

    def test_unknown_unit_root_keeps_gaps_open(self) -> None:
        state_root, _alist, runner = self._setup({})
        save_work_unit_records(state_root, "root-1", [
            _work_unit("root-1", "unit-orphan", None),
        ])
        save_gap_ledger(state_root, "root-1", [
            _episode_gap("root-1", "unit-orphan", 1, 1),
        ])

        result = reaudit_open_gaps(runner, state_root, "root-1")

        self.assertEqual(result["closed"], [])
        self.assertEqual(
            result["kept_open"], ["unit-orphan::missing_episode::S01E01"],
        )

    def test_season_gap_without_coordinates_stays_open(self) -> None:
        library = {
            "/library/番剧/刀剑神域/Season 02/S02E01.mkv": FAKE_VIDEO_BYTES,
        }
        state_root, _alist, runner = self._setup(library)
        save_work_unit_records(state_root, "root-1", [
            _work_unit("root-1", "unit-main", "/library/番剧/刀剑神域"),
        ])
        save_gap_ledger(state_root, "root-1", [
            Gap(
                gap_id="unit-main::missing_season::S02",
                root_task_id="root-1",
                work_unit_id="unit-main",
                kind="missing_season",
                media_type="tv",
                tmdb_id=45782,
                season=2,
                episodes=(),
                subtitle_path=None,
                subtitle_language=None,
                status="open",
            ),
        ])

        result = reaudit_open_gaps(runner, state_root, "root-1")

        # An episode-less season gap cannot be proven; fail closed.
        self.assertEqual(result["closed"], [])
        self.assertEqual(result["kept_open"], ["unit-main::missing_season::S02"])

    def test_listing_failure_keeps_everything_open(self) -> None:
        state_root, _alist, runner = self._setup(
            {}, alist_class=_BrokenListAList,
        )
        save_work_unit_records(state_root, "root-1", [
            _work_unit("root-1", "unit-main", "/library/番剧/刀剑神域"),
        ])
        save_gap_ledger(state_root, "root-1", [
            _episode_gap("root-1", "unit-main", 1, 1),
        ])

        result = reaudit_open_gaps(runner, state_root, "root-1")

        self.assertEqual(result["closed"], [])
        self.assertEqual(result["kept_open"], ["unit-main::missing_episode::S01E01"])
        self.assertTrue(result["errors"])

    def test_rerun_is_idempotent(self) -> None:
        library = {
            "/library/番剧/刀剑神域/Season 01/S01E01.mkv": FAKE_VIDEO_BYTES,
        }
        state_root, _alist, runner = self._setup(library)
        save_work_unit_records(state_root, "root-1", [
            _work_unit("root-1", "unit-main", "/library/番剧/刀剑神域"),
        ])
        save_gap_ledger(state_root, "root-1", [
            _episode_gap("root-1", "unit-main", 1, 1),
        ])

        first = reaudit_open_gaps(runner, state_root, "root-1")
        second = reaudit_open_gaps(runner, state_root, "root-1")

        self.assertEqual(first["closed"], ["unit-main::missing_episode::S01E01"])
        self.assertEqual(second["closed"], [])


if __name__ == "__main__":
    unittest.main()
