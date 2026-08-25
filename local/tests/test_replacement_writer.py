"""Replacement archival must stay inside the ordinary single writer."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from engine.scrapeflow.replacement import (
    archive_path_for,
    build_replacement_manifest,
)
from local.scrapeflow_api.simple_engine_runner import (
    EngineJobConflictError,
    EnginePauseRequested,
    SimpleEngineRunner,
)
from local.tests.test_simple_engine_runner import FAKE_VIDEO_BYTES, FakeAList


SOURCE_ROOT = "/incoming/replacement"
TARGET_ROOT = "/library/番剧/Example Show"


class ReplacementWriterTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.state_root = Path(self.temp.name)
        self.alist = FakeAList()
        self.old_video = b"old-video" * (len(FAKE_VIDEO_BYTES) // len(b"old-video") + 1)
        self.old_video = self.old_video[:len(FAKE_VIDEO_BYTES) + 17]
        self.new_subtitle = b"new simplified subtitle"
        self.old_subtitle = b"old simplified subtitle"
        self.alist.files.update({
            f"{SOURCE_ROOT}/01.mkv": FAKE_VIDEO_BYTES,
            f"{SOURCE_ROOT}/01.zh-CN.srt": self.new_subtitle,
            f"{TARGET_ROOT}/Example.Show.S01E13.mkv": self.old_video,
            f"{TARGET_ROOT}/Example.Show.S01E13.zh-CN.srt": self.old_subtitle,
        })
        self.runner = SimpleEngineRunner(
            self.state_root,
            alist=self.alist,
            tmdb=object(),
            validate=False,
            library_root="/library",
        )
        self.runner.create_pending_job(
            SOURCE_ROOT,
            job_id="root-replacement-1",
        )

    def _manifest(self):
        return build_replacement_manifest(
            manifest_id="replacement-writer-1",
            root_job_id="root-replacement-1",
            work_unit_id="unit-replacement-1",
            tmdb_id=123,
            media_type="tv",
            source_root=SOURCE_ROOT,
            target_work_root=TARGET_ROOT,
            library_root="/library",
            source_snapshot_id="snapshot-replacement-1",
            coordinate_map={"S01E01": "S01E13"},
            source_objects=[
                {
                    "path": f"{SOURCE_ROOT}/01.mkv",
                    "size": len(FAKE_VIDEO_BYTES),
                    "kind": "video",
                    "coordinate": "S01E01",
                },
                {
                    "path": f"{SOURCE_ROOT}/01.zh-CN.srt",
                    "size": len(self.new_subtitle),
                    "kind": "subtitle",
                    "coordinate": "S01E01",
                    "language": "zh-CN",
                },
            ],
            target_objects=[
                {
                    "path": f"{TARGET_ROOT}/Example.Show.S01E13.mkv",
                    "size": len(self.old_video),
                    "kind": "video",
                    "coordinate": "S01E13",
                },
                {
                    "path": f"{TARGET_ROOT}/Example.Show.S01E13.zh-CN.srt",
                    "size": len(self.old_subtitle),
                    "kind": "subtitle",
                    "coordinate": "S01E13",
                    "language": "zh-CN",
                },
            ],
            selected_subtitles={
                "S01E01": {
                    "path": f"{SOURCE_ROOT}/01.zh-CN.srt",
                    "size": len(self.new_subtitle),
                    "kind": "subtitle",
                    "coordinate": "S01E01",
                    "language": "zh-CN",
                },
            },
        )

    def test_archives_only_exact_old_video_and_subtitle_with_fresh_readback(self) -> None:
        manifest = self._manifest()
        item = manifest.items[0]
        video_archive = archive_path_for(manifest, item)
        subtitle_archive = archive_path_for(manifest, item, subtitle=True)

        result = self.runner.archive_replacement_targets(manifest)

        self.assertEqual(result["status"], "archived")
        self.assertEqual(
            [(row["kind"], row["status"]) for row in result["objects"]],
            [("video", "moved"), ("subtitle", "moved")],
        )
        self.assertNotIn(item.target_path, self.alist.files)
        self.assertNotIn(item.subtitle_target_path, self.alist.files)
        self.assertEqual(self.alist.files[video_archive], self.old_video)
        self.assertEqual(self.alist.files[subtitle_archive], self.old_subtitle)
        # The incoming sources remain untouched for the normal Planner/Writer
        # that follows the archival boundary.
        self.assertEqual(self.alist.files[item.source_path], FAKE_VIDEO_BYTES)
        self.assertEqual(self.alist.files[item.subtitle_source_path], self.new_subtitle)

    def test_recovery_replays_only_missing_archives(self) -> None:
        manifest = self._manifest()
        first = self.runner.archive_replacement_targets(manifest)
        move_count = len(self.alist.moves)

        second = self.runner.archive_replacement_targets(manifest)

        self.assertEqual(first["status"], "archived")
        self.assertEqual(second["status"], "archived")
        self.assertEqual(len(self.alist.moves), move_count)
        self.assertTrue(all(row["status"] == "already_archived" for row in second["objects"]))

    def test_preflight_collision_stops_before_any_old_target_moves(self) -> None:
        manifest = self._manifest()
        item = manifest.items[0]
        subtitle_archive = archive_path_for(manifest, item, subtitle=True)
        self.alist.files[subtitle_archive] = self.old_subtitle

        with self.assertRaises(EngineJobConflictError):
            self.runner.archive_replacement_targets(manifest)

        self.assertIn(item.target_path, self.alist.files)
        self.assertNotIn(archive_path_for(manifest, item), self.alist.files)
        self.assertIn(item.subtitle_target_path, self.alist.files)

    def test_source_drift_or_pause_stops_before_archiving(self) -> None:
        manifest = self._manifest()
        item = manifest.items[0]
        self.alist.files[f"{SOURCE_ROOT}/unowned.mkv"] = FAKE_VIDEO_BYTES

        with self.assertRaises(EngineJobConflictError):
            self.runner.archive_replacement_targets(manifest)
        self.assertIn(item.target_path, self.alist.files)

        self.alist.files.pop(f"{SOURCE_ROOT}/unowned.mkv")
        with self.assertRaises(EnginePauseRequested):
            self.runner.archive_replacement_targets(
                manifest,
                pause_requested=lambda: True,
            )
        self.assertIn(item.target_path, self.alist.files)
