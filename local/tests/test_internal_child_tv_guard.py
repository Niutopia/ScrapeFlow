"""Regression coverage for the internal-TV-child primary-video guard."""

from __future__ import annotations

import unittest

from engine.scrapeflow.models import Plan, PlannedFile

from local.scrapeflow_api.simple_engine_runner import (
    _internal_child_tv_primary_video_errors,
)


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


def _video(source_name: str, final_name: str) -> PlannedFile:
    return PlannedFile(
        source_path=f"/incoming/show/{source_name}",
        source_dir="/incoming/show",
        original_name=source_name,
        final_name=final_name,
        target_dir="/library/show/Season 01",
        media_kind="video",
        source_size=1024,
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


class InternalChildTvGuardTests(unittest.TestCase):
    def test_bracketed_source_with_single_final_coordinate_passes(self) -> None:
        """``[01]`` source carries no SxxEyy; the final S01E01 is authoritative."""
        plan = _plan(
            _video("[Ygm] Hyouka [01][Ma10p_2160p].mkv", "冰菓 - S01E01 - 标题.mkv"),
            _video("[Ygm] Hyouka [02][Ma10p_2160p].mkv", "冰菓 - S01E02 - 标题.mkv"),
        )
        self.assertEqual(_internal_child_tv_primary_video_errors(plan), [])

    def test_explicit_source_token_must_match_final(self) -> None:
        plan = _plan(
            _video("[Ygm] Show S01E01.mkv", "Show - S01E02 - Title.mkv"),
        )
        errors = _internal_child_tv_primary_video_errors(plan)
        self.assertTrue(any("集号" in error for error in errors))

    def test_duplicate_final_coordinate_is_rejected(self) -> None:
        plan = _plan(
            _video("[Ygm] Show [01].mkv", "Show - S01E01 - A.mkv"),
            _video("[Ygm] Show [02].mkv", "Show - S01E01 - B.mkv"),
        )
        errors = _internal_child_tv_primary_video_errors(plan)
        self.assertTrue(any("多个视频" in error for error in errors))

    def test_edition_distinct_videos_for_one_episode_pass(self) -> None:
        """同一集的两个命名 edition 是两行独立正片，不算重复。"""
        plan = _plan(
            _video(
                "[Ygm] Show [13 OAV][Ma10p_1440p].mkv",
                "Show - S01E13 - 空之女王 {edition-On-Air Version}.mkv",
            ),
            _video(
                "[Ygm] Show [13][Ma10p_1440p].mkv",
                "Show - S01E13 - 空之女王.mkv",
            ),
        )
        self.assertEqual(_internal_child_tv_primary_video_errors(plan), [])

    def test_same_edition_duplicate_coordinate_is_rejected(self) -> None:
        plan = _plan(
            _video(
                "[Ygm] Show [01 OAV].mkv",
                "Show - S01E01 - A {edition-On-Air Version}.mkv",
            ),
            _video(
                "[Ygm] Show [01].mkv",
                "Show - S01E01 - B {edition-On-Air Version}.mkv",
            ),
        )
        errors = _internal_child_tv_primary_video_errors(plan)
        self.assertTrue(any("多个视频" in error for error in errors))

    def test_subtitle_only_child_with_coordinates_passes(self) -> None:
        """正片已全部在库的 TV child 合法收敛为纯字幕补写。"""
        plan = _plan(
            _subtitle("[Ygm] Show [51].CHS.ass", "Show - S01E51 - 标题.zh-CN.ass"),
            _subtitle("[Ygm] Show [52].CHS.ass", "Show - S01E52 - 标题.zh-CN.ass"),
        )
        self.assertEqual(_internal_child_tv_primary_video_errors(plan), [])

    def test_subtitle_only_child_with_a_multi_episode_row_passes(self) -> None:
        """双集合并字幕（S01E02-E03）也是库坐标的复述，不是所有权主张。"""
        plan = _plan(
            _subtitle(
                "[Ygm] Show [NE 02-03].CHS.ass",
                "Show - S01E02-E03 - 标题 {edition-New Edit}.zh-CN.ass",
            ),
        )
        self.assertEqual(_internal_child_tv_primary_video_errors(plan), [])

    def test_subtitle_without_coordinates_is_still_rejected(self) -> None:
        plan = _plan(
            _subtitle("[Ygm] Show extras.ass", "Show 特典字幕.ass"),
        )
        errors = _internal_child_tv_primary_video_errors(plan)
        self.assertEqual(errors, ["内部 TV child 没有视频"])

    def test_mixed_non_subtitle_video_less_child_is_still_rejected(self) -> None:
        plan = _plan(
            _subtitle("[Ygm] Show [51].CHS.ass", "Show - S01E51 - 标题.zh-CN.ass"),
            PlannedFile(
                source_path="/incoming/show/menu.jpg",
                source_dir="/incoming/show",
                original_name="menu.jpg",
                final_name="menu.jpg",
                target_dir="/library/show/Season 01",
                media_kind="other",
                source_size=10,
            ),
        )
        errors = _internal_child_tv_primary_video_errors(plan)
        self.assertEqual(errors, ["内部 TV child 没有视频"])


if __name__ == "__main__":
    unittest.main()
