from __future__ import annotations

import json
from pathlib import Path
import tempfile
import unittest

from engine.scrapeflow.archive import ArchiveLimits, ArchiveListing, ArchiveMember
from engine.scrapeflow.container_inspection import (
    ContainerInspection,
    ContainerInspectionError,
    ContainerInspectionPersistenceError,
    ContainerLimitResult,
    ContainerMediaCandidate,
    ContainerMember,
    load_container_inspection,
    list_container_inspections,
    save_container_inspection,
)
from engine.scrapeflow.source_objects import SourceObjectRef


def _source(path: str = "/incoming/feature.iso") -> SourceObjectRef:
    return SourceObjectRef(
        path=path,
        object_type="disc_image",
        size=4096,
        snapshot_id="snapshot-1",
        version="etag-1",
    )


class ContainerInspectionTests(unittest.TestCase):
    def _inspection(self, *, source: SourceObjectRef | None = None) -> ContainerInspection:
        members = (
            ContainerMember("BDMV/STREAM/00001.m2ts", 1024, "video"),
            ContainerMember("BDMV/PLAYLIST/00000.mpls", 128, "other"),
            ContainerMember("BDMV/CLIPINF/00001.clpi", 64, "other"),
            ContainerMember("字幕/00001.srt", 42, "subtitle"),
        )
        candidates = (
            ContainerMediaCandidate("BDMV/STREAM/00001.m2ts", "video", 1024),
            ContainerMediaCandidate("字幕/00001.srt", "subtitle", 42),
        )
        limits = ContainerLimitResult.from_archive_members(
            members,
            limits=ArchiveLimits(max_depth=16, min_free_bytes=0),
        )
        return ContainerInspection(
            inspection_id="inspect-1",
            source_object=source or _source(),
            detected_format="iso9660",
            members=members,
            media_candidates=candidates,
            limits=limits,
            staging_path="/tmp/scrapeflow-staging/inspect-1",
            status="ready",
            created_at="2026-08-21T00:00:00Z",
        )

    def test_archive_listing_projection_records_media_and_limits(self) -> None:
        listing = ArchiveListing(
            archive_path=Path("/tmp/feature.iso"),
            archive_format="iso",
            volumes=(Path("/tmp/feature.iso"),),
            members=(
                ArchiveMember("Movie/feature.m2ts", size=11),
                ArchiveMember("Movie/feature.srt", size=5),
                ArchiveMember("Movie/readme.txt", size=1),
            ),
            archive_size=20,
        )
        inspection = ContainerInspection.from_archive_listing(
            inspection_id="iso-1",
            source_object=_source(),
            listing=listing,
            limits=ArchiveLimits(min_free_bytes=0),
            staging_path="/tmp/staging/iso-1",
            created_at="2026-08-21T00:00:00Z",
        )
        self.assertEqual(inspection.detected_format, "iso")
        self.assertEqual([item.kind for item in inspection.media_candidates], ["video", "subtitle"])
        self.assertEqual(inspection.limits.member_count, 3)
        self.assertEqual(inspection.limits.observed_max_depth, 2)
        self.assertTrue(inspection.is_successful)

    def test_persistence_is_atomic_and_round_trips(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            original = self._inspection()
            path = save_container_inspection(root, original)
            self.assertTrue(path.is_file())
            loaded = load_container_inspection(root, original.inspection_id, strict=True)
            self.assertIsNotNone(loaded)
            assert loaded is not None
            self.assertEqual(loaded.as_dict(), original.as_dict())
            self.assertEqual(list_container_inspections(root), (loaded,))

    def test_durable_projection_redacts_password_markers(self) -> None:
        inspection = self._inspection(source=_source("/incoming/密码:super-secret/feature.iso?token=query-secret"))
        # Marker is part of a provider path, while the member path is a
        # separately validated archive-relative name.
        leaked = json.dumps(inspection.as_dict(), ensure_ascii=False)
        self.assertNotIn("super-secret", leaked)
        self.assertIn("<redacted>", leaked)
        self.assertNotIn("super-secret", repr(inspection))
        self.assertNotIn("query-secret", leaked)

    def test_member_path_and_candidate_ownership_fail_closed(self) -> None:
        with self.assertRaises(ContainerInspectionError):
            ContainerMember("../escape.m2ts", 1, "video")
        members = (ContainerMember("Movie/file.m2ts", 1, "video"),)
        with self.assertRaises(ContainerInspectionError):
            ContainerInspection(
                inspection_id="bad",
                source_object=_source(),
                detected_format="zip",
                members=members,
                media_candidates=(ContainerMediaCandidate("Movie/file.m2ts", "subtitle", 1),),
                limits=ContainerLimitResult(True, 1, 1),
                created_at="now",
            )
        with self.assertRaises(ContainerInspectionError):
            ContainerInspection(
                inspection_id="bad-collision",
                source_object=_source(),
                detected_format="zip",
                members=(
                    ContainerMember("Movie", 1, "other"),
                    ContainerMember("movie/file.m2ts", 1, "video"),
                ),
                limits=ContainerLimitResult(True, 2, 1),
                created_at="now",
            )

    def test_expanded_state_requires_task_staging_and_new_snapshot(self) -> None:
        with self.assertRaises(ContainerInspectionError):
            ContainerInspection(
                inspection_id="expanded-no-proof",
                source_object=_source(),
                detected_format="udf",
                limits=ContainerLimitResult(True, 0, 0),
                status="expanded",
                created_at="now",
            )
        expanded = ContainerInspection(
            inspection_id="expanded-ok",
            source_object=_source(),
            detected_format="udf",
            limits=ContainerLimitResult(True, 0, 0),
            staging_path="/tmp/staging/expanded-ok",
            status="expanded",
            expanded_snapshot_id="snapshot-2",
            created_at="now",
        )
        self.assertTrue(expanded.is_successful)

    def test_staging_cannot_be_inside_the_source_directory(self) -> None:
        with self.assertRaises(ContainerInspectionError):
            ContainerInspection(
                inspection_id="source-staging",
                source_object=_source("/incoming/show/feature.iso"),
                detected_format="iso",
                limits=ContainerLimitResult(True, 0, 0),
                staging_path="/incoming/show/.staging",
                status="inspected",
                created_at="now",
            )

    def test_malformed_persisted_record_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path = root / "container-inspection-bad.json"
            path.write_text(json.dumps({"version": 1, "status": "ready"}), encoding="utf-8")
            self.assertIsNone(load_container_inspection(root, "bad"))
            with self.assertRaises(ContainerInspectionPersistenceError):
                load_container_inspection(root, "bad", strict=True)

    def test_malformed_member_boolean_is_not_silently_coerced(self) -> None:
        payload = self._inspection().as_dict()
        payload["members"][0]["is_link"] = "false"
        with self.assertRaises(ContainerInspectionError):
            ContainerInspection.from_dict(payload)


if __name__ == "__main__":
    unittest.main()
