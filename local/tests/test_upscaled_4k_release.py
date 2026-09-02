"""Regression tests: a self-labelled ``4K Ver.`` upscale must not beat a native BD.

The answer book ruled every ``[ 4K Ver. ]`` release in the corpus an upscale
(``losing_upscaled_4k_video_or_plain_srt``): the official package has no UHD
disc, so the inflated 4K label is derived from a native-resolution master.
The engine's resolution competition used to take the advertised 2160 at face
value and write the upscale while stranding the native BDRip at source
(observed on Re:Zero S3 E51-E66).  These tests pin the generic rule: the
upscale competes as resolution-unlabelled whenever a native competitor with a
positive advertised resolution shares the bucket, and it stays at source as an
informational problem row instead of a write or a cleanup.
"""

from __future__ import annotations

import unittest

from engine.scrapeflow.core import (
    _dedupe_merged_tv_target_variants,
    _prefer_highest_resolution_videos,
    upscaled_4k_release,
)
from engine.scrapeflow.models import Plan, PlannedFile

_VCB_NAME = (
    "[VCB-Studio] Re Zero kara Hajimeru Isekai Seikatsu 3rd Season "
    "[51][Ma10p_1080p][x265_flac_aac].mkv"
)
_VCB_PATH = (
    "/quark/影视/待刮削/Re：從零開始的異世界生活/06. 第三季/"
    "[VCB-Studio] Re：從零開始的異世界生活 第三季 10-bit 1080p HEVC BDRip [Fin]/"
    + _VCB_NAME
)
_MOOZZI2_NAME = (
    "[Moozzi2] Re Zero Kara Hajimeru Isekai Seikatsu 3 - 1 [ 51 ] "
    "(BD 3840x2160 x265-10Bit FLACx2).mkv"
)
_MOOZZI2_PATH = (
    "/quark/影视/待刮削/Re：從零開始的異世界生活/06. 第三季/"
    "[Moozzi2] Re：從零開始的異世界生活 第三季 [ 4K Ver. ] - TV/"
    + _MOOZZI2_NAME
)


class Upscaled4kDetectionTests(unittest.TestCase):
    def test_a_4k_ver_directory_marks_its_descendants(self) -> None:
        self.assertTrue(upscaled_4k_release({"full_path": _MOOZZI2_PATH}))
        self.assertTrue(
            upscaled_4k_release(
                {"full_path": "/src/Show [4K Version]/Show 01.mkv"}
            )
        )

    def test_native_4k_and_collection_labels_are_not_upscale_markers(self) -> None:
        # A native UHD release advertises resolution without a version tag.
        self.assertFalse(upscaled_4k_release({"full_path": _VCB_PATH}))
        self.assertFalse(
            upscaled_4k_release(
                {"full_path": "/src/Show 2160p/Show - 01 [Ma10p_2160p].mkv"}
            )
        )
        # A collection root that merely mentions 4K is not a "Ver." variant.
        self.assertFalse(
            upscaled_4k_release(
                {"full_path": "/src/Show（2009）4K超清2160P收藏版/Show 01.mkv"}
            )
        )
        self.assertFalse(
            upscaled_4k_release({"full_path": "/src/[4K_NW] Show/Show 01.mkv"})
        )


class PreferHighestResolutionUpscaleTests(unittest.TestCase):
    def test_a_4k_ver_upscale_loses_to_the_native_bd(self) -> None:
        kept, removed = _prefer_highest_resolution_videos(
            [
                {"name": _VCB_NAME, "full_path": _VCB_PATH, "size": 2720387739},
                {"name": _MOOZZI2_NAME, "full_path": _MOOZZI2_PATH, "size": 4808737190},
            ]
        )
        self.assertEqual(
            [item["full_path"] for item in kept], [_VCB_PATH]
        )
        self.assertEqual(len(removed), 1)
        self.assertEqual(removed[0]["full_path"], _MOOZZI2_PATH)
        self.assertEqual(
            removed[0]["_duplicate_cleanup_kind"], "upscaled_4k_duplicate"
        )
        self.assertEqual(removed[0]["_preferred_resolution_source"], _VCB_PATH)

    def test_an_upscale_without_a_native_competitor_is_not_demoted(self) -> None:
        # Only the upscale exists: it is the sole copy and still wins normally.
        kept, removed = _prefer_highest_resolution_videos(
            [{"name": _MOOZZI2_NAME, "full_path": _MOOZZI2_PATH, "size": 4808737190}]
        )
        self.assertEqual(kept[0]["full_path"], _MOOZZI2_PATH)
        self.assertEqual(removed, [])

    def test_a_genuine_2160p_native_still_beats_the_1080p_bd(self) -> None:
        native_4k = {
            "name": "Show - 01 [Ma10p_2160p][x265_flac].mkv",
            "full_path": "/src/Show UHD/Show - 01 [Ma10p_2160p][x265_flac].mkv",
            "size": 8000000000,
        }
        kept, removed = _prefer_highest_resolution_videos(
            [native_4k, {"name": _VCB_NAME, "full_path": _VCB_PATH, "size": 2720387739}]
        )
        self.assertEqual([item["full_path"] for item in kept], [native_4k["full_path"]])
        self.assertEqual(
            removed[0]["_duplicate_cleanup_kind"], "lower_resolution"
        )


def _planned_video(source_path: str, original_name: str) -> PlannedFile:
    return PlannedFile(
        source_path=source_path,
        source_dir=source_path.rsplit("/", 1)[0],
        original_name=original_name,
        final_name="Re-从零开始的异世界生活 - S01E51 - 戏剧性的恶意.mkv",
        target_dir="/quark/影视/番剧/Re-從零開始的異世界生活/Re-从零开始的异世界生活/Season 01",
        media_kind="video",
        episode_key="S01E51",
        source_size=1,
    )


class MergedTargetUpscaleTests(unittest.TestCase):
    def test_a_cross_subplan_upscale_moves_to_a_stays_at_source_problem_row(
        self,
    ) -> None:
        plan = Plan(
            mode="auto",
            source_root="/quark/影视/待刮削/Re：從零開始的異世界生活",
            target_root="/quark/影视/番剧/Re-從零開始的異世界生活",
            files=[
                _planned_video(_VCB_PATH, _VCB_NAME),
                _planned_video(_MOOZZI2_PATH, _MOOZZI2_NAME),
            ],
            warnings=[],
            metadata={},
        )
        _dedupe_merged_tv_target_variants(plan)
        self.assertEqual(
            [item.source_path for item in plan.files], [_VCB_PATH]
        )
        self.assertEqual(len(plan.problem_files), 1)
        problem = plan.problem_files[0]
        self.assertEqual(problem.source_path, _MOOZZI2_PATH)
        self.assertTrue(problem.stays_at_source)
        self.assertIn("升频版本", problem.reason)
        self.assertEqual(plan.cleanup_files, [])


if __name__ == "__main__":
    unittest.main()
