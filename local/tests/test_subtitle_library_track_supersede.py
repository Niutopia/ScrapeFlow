"""Regression coverage: library subtitle tracks supersede re-offered candidates.

When every primary video of a work already sits in the formal library, a
later source scan may still re-offer the losing external tracks (e.g. the
traditional-Chinese sidecar beside the already-migrated simplified winner).
The unpaired-subtitle demotion must keep only a candidate whose language is
strictly preferred over the track the library already holds beside that
exact video; an equal-or-worse candidate stays at source as deferred
evidence and never duplicates or rewrites the standing winner.
"""

from __future__ import annotations

import unittest

from engine.scrapeflow.core import _demote_unpaired_subtitles
from engine.scrapeflow.models import Plan, PlannedFile


class _FakeAList:
    """AList double: one listing per target directory."""

    def __init__(self, listings: dict[str, list[dict[str, object]]]) -> None:
        self._listings = listings
        self.listed: list[str] = []

    def try_list(self, path: str, refresh: bool = True) -> list[dict[str, object]]:
        del refresh
        self.listed.append(path)
        return self._listings.get(path, [])


def _plan(*files: PlannedFile) -> Plan:
    return Plan(
        mode="tv",
        source_root="/incoming/show",
        target_root="/library/show",
        files=list(files),
        cleanup_files=[],
        problem_files=[],
        warnings=[],
        notices=[],
        metadata={"media_type": "tv", "tmdb_id": 1, "season": 1},
        decision_trace={},
        scan_report={},
    )


def _subtitle(source_name: str, final_name: str) -> PlannedFile:
    return PlannedFile(
        source_path=f"/incoming/show/{source_name}",
        source_dir="/incoming/show",
        original_name=source_name,
        final_name=final_name,
        target_dir="/library/show/Season 01",
        media_kind="subtitle",
        source_size=512,
    )


SEASON = "/library/show/Season 01"


class LibraryTrackSupersedeTests(unittest.TestCase):
    def test_equal_or_worse_language_track_is_deferred(self) -> None:
        """库内已有简中赢家时，再出现的繁中候选留在源目录。"""
        alist = _FakeAList({SEASON: [
            {"name": "Show - S01E51 - 标题.mkv", "is_dir": False, "size": 9},
            {"name": "Show - S01E51 - 标题.zh-CN.ass", "is_dir": False, "size": 8},
        ]})
        plan = _plan(
            _subtitle("[Ygm] Show [51].CHT.ass", "Show - S01E51 - 标题.zh-TW.ass"),
        )
        _demote_unpaired_subtitles(alist, plan)
        self.assertEqual(plan.files, [])
        deferred = plan.scan_report["deferred_subtitles"]
        self.assertEqual(len(deferred), 1)
        self.assertEqual(
            deferred[0]["reason"], "library_track_supersedes_candidate"
        )
        self.assertIn("库内已有同轨或更优轨道", plan.warnings[0])

    def test_same_language_track_is_deferred(self) -> None:
        """库内已有同语言轨道（同名即已存在）时，候选同样留在源目录。"""
        alist = _FakeAList({SEASON: [
            {"name": "Show - S01E51 - 标题.mkv", "is_dir": False, "size": 9},
            {"name": "Show - S01E51 - 标题.zh-CN.ass", "is_dir": False, "size": 8},
        ]})
        plan = _plan(
            _subtitle("[Ygm] Show [51].CHS.ass", "Show - S01E51 - 标题.zh-CN.ass"),
        )
        _demote_unpaired_subtitles(alist, plan)
        self.assertEqual(plan.files, [])
        self.assertEqual(
            plan.scan_report["deferred_subtitles"][0]["reason"],
            "library_track_supersedes_candidate",
        )

    def test_strictly_preferred_candidate_is_kept(self) -> None:
        """库内只有繁中时，简中候选仍要写入（轨道升级）。"""
        alist = _FakeAList({SEASON: [
            {"name": "Show - S01E51 - 标题.mkv", "is_dir": False, "size": 9},
            {"name": "Show - S01E51 - 标题.zh-TW.ass", "is_dir": False, "size": 8},
        ]})
        plan = _plan(
            _subtitle("[Ygm] Show [51].CHS.ass", "Show - S01E51 - 标题.zh-CN.ass"),
        )
        _demote_unpaired_subtitles(alist, plan)
        self.assertEqual(len(plan.files), 1)
        self.assertNotIn("deferred_subtitles", plan.scan_report)

    def test_first_track_beside_an_existing_video_is_kept(self) -> None:
        """伴随视频在库且旁边没有轨道时，候选正常获得写入车道。"""
        alist = _FakeAList({SEASON: [
            {"name": "Show - S01E51 - 标题.mkv", "is_dir": False, "size": 9},
        ]})
        plan = _plan(
            _subtitle("[Ygm] Show [51].CHS.ass", "Show - S01E51 - 标题.zh-CN.ass"),
        )
        _demote_unpaired_subtitles(alist, plan)
        self.assertEqual(len(plan.files), 1)

    def test_edition_companions_are_judged_separately(self) -> None:
        """edition 后缀是伴随键的一部分：New Edit 的字幕不与正片轨道比较。"""
        alist = _FakeAList({SEASON: [
            {"name": "Show - S01E01 - 标题.mkv", "is_dir": False, "size": 9},
            {"name": "Show - S01E01 - 标题.zh-CN.ass", "is_dir": False, "size": 8},
            {
                "name": "Show - S01E01 - 标题 {edition-New Edit}.mkv",
                "is_dir": False,
                "size": 9,
            },
        ]})
        plan = _plan(
            _subtitle(
                "[Ygm] Show [NE 01].CHS.ass",
                "Show - S01E01 - 标题 {edition-New Edit}.zh-CN.ass",
            ),
        )
        _demote_unpaired_subtitles(alist, plan)
        self.assertEqual(len(plan.files), 1)

    def test_unpaired_gap_still_works_without_library_video(self) -> None:
        alist = _FakeAList({SEASON: []})
        plan = _plan(
            _subtitle("[Ygm] Show [51].CHS.ass", "Show - S01E51 - 标题.zh-CN.ass"),
        )
        _demote_unpaired_subtitles(alist, plan)
        self.assertEqual(plan.files, [])
        gaps = plan.scan_report["resource_gaps"]
        self.assertEqual(gaps[0]["kind"], "subtitle_without_video")


if __name__ == "__main__":
    unittest.main()
