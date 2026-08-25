"""Fail-closed replacement mapping and restart evidence tests."""

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from engine.scrapeflow.replacement import (
    ReplacementManifest,
    ReplacementObject,
    ReplacementValidationError,
    archive_path_for,
    build_replacement_manifest,
    compare_replacement_fresh,
    derive_replacement_archive_root,
    load_replacement_manifest,
    save_replacement_manifest,
    transition_replacement_manifest,
)


SOURCE_ROOT = "/quark/影视/待刮削/DBD"
TARGET_ROOT = "/quark/影视/番剧/物理魔法使-马修-"
LIBRARY_ROOT = "/quark/影视"


def _objects() -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    source = [
        {"path": f"{SOURCE_ROOT}/01.mkv", "size": 101, "kind": "video", "coordinate": "S01E01"},
        {"path": f"{SOURCE_ROOT}/01.sc.srt", "size": 11, "kind": "subtitle", "coordinate": "S01E01", "language": "zh-CN"},
        {"path": f"{SOURCE_ROOT}/menu/tc.srt", "size": 12, "kind": "subtitle", "coordinate": "S01E01", "language": "zh-TW"},
        {"path": f"{SOURCE_ROOT}/02.mkv", "size": 102, "kind": "video", "coordinate": "S01E02"},
        {"path": f"{SOURCE_ROOT}/02.sc.srt", "size": 12, "kind": "subtitle", "coordinate": "S01E02", "language": "zh-CN"},
    ]
    target = [
        {"path": f"{TARGET_ROOT}/S01E13.mkv", "size": 201, "kind": "video", "coordinate": "S01E13"},
        {"path": f"{TARGET_ROOT}/S01E13.zh-CN.srt", "size": 21, "kind": "subtitle", "coordinate": "S01E13", "language": "zh-CN"},
        {"path": f"{TARGET_ROOT}/S01E14.mkv", "size": 202, "kind": "video", "coordinate": "S01E14"},
    ]
    return source, target


class TestReplacementManifest(unittest.TestCase):
    def _manifest(self):
        source, target = _objects()
        return build_replacement_manifest(
            manifest_id="replacement-1",
            root_job_id="root-1",
            work_unit_id="unit-1",
            tmdb_id=123,
            source_root=SOURCE_ROOT,
            target_work_root=TARGET_ROOT,
            library_root=LIBRARY_ROOT,
            source_snapshot_id="snap-1",
            coordinate_map={"S01E01": "S01E13", "S01E02": "S01E14"},
            source_objects=source,
            target_objects=target,
            selected_subtitles={
                "S01E01": source[1],
                "S01E02": source[4],
            },
        )

    def test_builds_explicit_mapping_and_residuals(self) -> None:
        manifest = self._manifest()
        self.assertEqual(manifest.target_coordinates, ("S01E13", "S01E14"))
        self.assertEqual(manifest.items[0].source_coordinate, "S01E01")
        self.assertIn(f"{SOURCE_ROOT}/menu/tc.srt", manifest.residual_paths)
        self.assertTrue(archive_path_for(manifest, manifest.items[0]).startswith(
            f"{LIBRARY_ROOT}/.scrapeflow-archive/root-1/"
        ))
        self.assertIsNotNone(archive_path_for(manifest, manifest.items[0], subtitle=True))

    def test_missing_explicit_map_or_source_is_rejected(self) -> None:
        source, target = _objects()
        kwargs = dict(
            manifest_id="replacement-1", root_job_id="root-1", work_unit_id="unit-1",
            tmdb_id=123, source_root=SOURCE_ROOT, target_work_root=TARGET_ROOT,
            library_root=LIBRARY_ROOT, source_snapshot_id="snap-1",
            source_objects=source, target_objects=target,
            selected_subtitles={"S01E01": source[1], "S01E02": source[4]},
        )
        with self.assertRaises(ReplacementValidationError):
            build_replacement_manifest(coordinate_map={}, **kwargs)
        with self.assertRaises(ReplacementValidationError):
            build_replacement_manifest(
                coordinate_map={"S01E01": "S01E13"}, **kwargs,
            )

    def test_tc_and_same_size_are_not_silently_accepted(self) -> None:
        source, target = _objects()
        kwargs = dict(
            manifest_id="replacement-1", root_job_id="root-1", work_unit_id="unit-1",
            tmdb_id=123, source_root=SOURCE_ROOT, target_work_root=TARGET_ROOT,
            library_root=LIBRARY_ROOT, source_snapshot_id="snap-1",
            coordinate_map={"S01E01": "S01E13", "S01E02": "S01E14"},
            source_objects=source, target_objects=target,
        )
        with self.assertRaises(ReplacementValidationError):
            build_replacement_manifest(
                selected_subtitles={"S01E01": source[2], "S01E02": source[4]}, **kwargs,
            )
        equal_target = [dict(row) for row in target]
        equal_target[0]["size"] = 101
        with self.assertRaises(ReplacementValidationError):
            build_replacement_manifest(
                selected_subtitles={"S01E01": source[1], "S01E02": source[4]},
                target_objects=equal_target, **{k: v for k, v in kwargs.items() if k != "target_objects"},
            )

    def test_roots_and_external_archive_injection_fail_closed(self) -> None:
        with self.assertRaises(ReplacementValidationError):
            derive_replacement_archive_root("/library", "/outside/work", "root-1")
        manifest = self._manifest()
        with self.assertRaises(ReplacementValidationError):
            ReplacementManifest.from_dict({**manifest.as_dict(), "archive_root": "/tmp/user-chosen"})
        with self.assertRaises(ReplacementValidationError):
            ReplacementManifest.from_dict({
                **manifest.as_dict(),
                "archive_root": f"{LIBRARY_ROOT}/.scrapeflow-archive/root-1/not-the-work-root",
            })

    def test_residuals_need_no_fake_coordinate_but_links_are_rejected(self) -> None:
        source, target = _objects()
        source.append({
            "path": f"{SOURCE_ROOT}/menu/readme.txt",
            "size": 1,
            "kind": "other",
        })
        manifest = build_replacement_manifest(
            manifest_id="replacement-1",
            root_job_id="root-1",
            work_unit_id="unit-1",
            tmdb_id=123,
            source_root=SOURCE_ROOT,
            target_work_root=TARGET_ROOT,
            library_root=LIBRARY_ROOT,
            source_snapshot_id="snap-1",
            coordinate_map={"S01E01": "S01E13", "S01E02": "S01E14"},
            source_objects=source,
            target_objects=target,
            selected_subtitles={"S01E01": source[1], "S01E02": source[4]},
        )
        self.assertIn(f"{SOURCE_ROOT}/menu/readme.txt", manifest.residual_paths)
        self.assertIsNone(ReplacementObject.from_dict(source[-1]).coordinate)
        with self.assertRaises(ReplacementValidationError):
            ReplacementObject.from_dict({
                "path": f"{SOURCE_ROOT}/linked.mkv",
                "size": 1,
                "kind": "video",
                "coordinate": "S01E03",
                "is_symlink": True,
            })

    def test_recovery_rejects_unowned_source_or_target_video(self) -> None:
        manifest = self._manifest()
        source, target = _objects()
        source.append({
            "path": f"{SOURCE_ROOT}/unexpected.mkv",
            "size": 99,
            "kind": "video",
            "coordinate": "S01E99",
        })
        decision = compare_replacement_fresh(
            manifest,
            source_objects=source,
            target_objects=target,
        )
        self.assertEqual(decision.status, "attention")
        self.assertIn(f"{SOURCE_ROOT}/unexpected.mkv", decision.unknown_paths)

        source, target = _objects()
        target.append({
            "path": f"{TARGET_ROOT}/S01E99.mkv",
            "size": 999,
            "kind": "video",
            "coordinate": "S01E99",
        })
        decision = compare_replacement_fresh(
            manifest,
            source_objects=source,
            target_objects=target,
        )
        self.assertEqual(decision.status, "attention")
        self.assertIn(f"{TARGET_ROOT}/S01E99.mkv", decision.unknown_paths)

    def test_caller_cannot_hide_or_invent_residual_ownership(self) -> None:
        source, target = _objects()
        kwargs = dict(
            manifest_id="replacement-1",
            root_job_id="root-1",
            work_unit_id="unit-1",
            tmdb_id=123,
            source_root=SOURCE_ROOT,
            target_work_root=TARGET_ROOT,
            library_root=LIBRARY_ROOT,
            source_snapshot_id="snap-1",
            coordinate_map={"S01E01": "S01E13", "S01E02": "S01E14"},
            source_objects=source,
            target_objects=target,
            selected_subtitles={"S01E01": source[1], "S01E02": source[4]},
        )
        with self.assertRaises(ReplacementValidationError):
            build_replacement_manifest(residual_paths=(), **kwargs)
        with self.assertRaises(ReplacementValidationError):
            build_replacement_manifest(
                residual_paths=(f"{SOURCE_ROOT}/invented.txt",),
                **kwargs,
            )

    def test_round_trip_and_state_transitions(self) -> None:
        manifest = self._manifest()
        manifest = transition_replacement_manifest(manifest, "archiving")
        manifest = transition_replacement_manifest(manifest, "archived")
        manifest = transition_replacement_manifest(manifest, "writing")
        with tempfile.TemporaryDirectory() as tmp:
            save_replacement_manifest(Path(tmp), manifest)
            self.assertEqual(load_replacement_manifest(Path(tmp), manifest.manifest_id), manifest)
            raw = json.loads((Path(tmp) / "replacement-manifests" / "replacement-1.json").read_text())
            self.assertNotIn("archive_path_injected_by_client", raw)
            self.assertEqual(raw["library_root"], LIBRARY_ROOT)

    def test_recovery_distinguishes_not_started_archived_and_conflict(self) -> None:
        manifest = self._manifest()
        source, target = _objects()
        decision = compare_replacement_fresh(manifest, source_objects=source, target_objects=target)
        self.assertEqual(decision.status, "not_started")
        first = manifest.items[0]
        archive = [{
            "path": archive_path_for(manifest, first), "size": first.target_size,
            "kind": "video", "coordinate": first.target_coordinate,
        }, {
            "path": archive_path_for(manifest, first, subtitle=True),
            "size": first.subtitle_target_size,
            "kind": "subtitle", "coordinate": first.target_coordinate,
            "language": "zh-CN",
        }]
        target_without_first = [
            row for row in target
            if row["path"] not in {first.target_path, first.subtitle_target_path}
        ]
        decision = compare_replacement_fresh(
            manifest, source_objects=source, target_objects=target_without_first,
            archive_objects=archive,
        )
        self.assertEqual(decision.status, "safe_to_resume")
        conflict = compare_replacement_fresh(
            manifest, source_objects=source, target_objects=target,
            archive_objects=archive,
        )
        self.assertEqual(conflict.status, "attention")


if __name__ == "__main__":
    unittest.main()
