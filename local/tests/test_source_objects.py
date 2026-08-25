"""Pure-domain tests for exact SourceObjectRef manifests and ownership."""

from __future__ import annotations

import unittest

from engine.scrapeflow.source_objects import (
    SourceManifest,
    SourceManifestDriftError,
    SourceObjectClaim,
    SourceObjectOwnershipError,
    SourceObjectRef,
    SourceObjectValidationError,
    compare_fresh_manifest,
    normalize_source_object_path,
    validate_source_object_ownership,
    validate_unique_source_object_ownership,
)


ROOT = "/quark/影视/待刮削/Example"


def _object(
    name: str,
    *,
    snapshot_id: str = "snapshot-a",
    object_type: str = "video",
    size: int = 1024,
    version: str | None = "v1",
    modified: str | None = "2026-08-21T00:00:00Z",
) -> SourceObjectRef:
    return SourceObjectRef(
        path=f"{ROOT}/{name}",
        object_type=object_type,
        size=size,
        snapshot_id=snapshot_id,
        version=version,
        modified=modified,
    )


class TestSourceObjectRef(unittest.TestCase):
    def test_round_trip_retains_exact_observation_fields(self) -> None:
        original = _object("Season 01/E01.mkv")
        restored = SourceObjectRef.from_dict(original.as_dict())

        self.assertEqual(restored, original)
        self.assertEqual(restored.fingerprint, (
            f"{ROOT}/Season 01/E01.mkv", "video", 1024, "v1", "2026-08-21T00:00:00Z",
        ))

    def test_listing_row_uses_shared_classifier_and_provider_metadata(self) -> None:
        ref = SourceObjectRef.from_listing_row({
            "full_path": f"{ROOT}/Season 01/E01.mkv",
            "size": "2048",
            "last_modified": "2026-08-21T01:00:00Z",
            "version": "etag-1",
        }, snapshot_id="snapshot-a")

        self.assertEqual(ref.object_type, "video")
        self.assertEqual(ref.size, 2048)
        self.assertEqual(ref.modified, "2026-08-21T01:00:00Z")
        self.assertEqual(ref.version, "etag-1")

    def test_directory_row_is_explicitly_typed(self) -> None:
        ref = SourceObjectRef.from_listing_row({
            "full_path": f"{ROOT}/Season 01",
            "is_dir": True,
        }, snapshot_id="snapshot-a")

        self.assertTrue(ref.is_directory)
        self.assertEqual(ref.object_type, "directory")

    def test_file_row_without_size_is_rejected(self) -> None:
        with self.assertRaises(SourceObjectValidationError):
            SourceObjectRef.from_listing_row({
                "full_path": f"{ROOT}/E01.mkv",
            }, snapshot_id="snapshot-a")

    def test_rejects_paths_that_would_need_normalization(self) -> None:
        invalid = (
            "relative/file.mkv",
            f"{ROOT}/./E01.mkv",
            f"{ROOT}//E01.mkv",
            f"{ROOT}/E01.mkv/",
            f"{ROOT}\\E01.mkv",
            f"{ROOT}/bad\x00name.mkv",
        )
        for path in invalid:
            with self.subTest(path=path):
                with self.assertRaises(SourceObjectValidationError):
                    normalize_source_object_path(path)

    def test_rejects_invalid_object_fields(self) -> None:
        with self.assertRaises(SourceObjectValidationError):
            _object("E01.mkv", size=-1)
        with self.assertRaises(SourceObjectValidationError):
            _object("E01.mkv", size=True)  # type: ignore[arg-type]
        with self.assertRaises(SourceObjectValidationError):
            _object("E01.mkv", snapshot_id=" ")


class TestSourceManifest(unittest.TestCase):
    def test_empty_source_is_a_valid_exact_manifest(self) -> None:
        manifest = SourceManifest("snapshot-empty", ROOT, ())

        self.assertEqual(manifest.objects, ())
        self.assertEqual(manifest.object_paths, ())

    def test_root_source_manifest_is_supported(self) -> None:
        manifest = SourceManifest("snapshot-root", "/", (
            SourceObjectRef("/E01.mkv", "video", 1, "snapshot-root"),
        ))

        self.assertEqual(manifest.root_path, "/")

    def test_manifest_requires_one_snapshot_and_one_safe_root(self) -> None:
        with self.assertRaises(SourceObjectValidationError):
            SourceManifest("snapshot-a", ROOT, (_object("E01.mkv", snapshot_id="snapshot-b"),))
        with self.assertRaises(SourceObjectValidationError):
            SourceManifest("snapshot-a", ROOT, (
                SourceObjectRef("/outside/E01.mkv", "video", 1, "snapshot-a"),
            ))
        with self.assertRaises(SourceObjectValidationError):
            SourceManifest("snapshot-a", ROOT, (_object("E01.mkv"), _object("E01.mkv")))

    def test_manifest_rejects_case_or_unicode_ambiguous_paths(self) -> None:
        with self.assertRaises(SourceObjectValidationError):
            SourceManifest("snapshot-a", ROOT, (
                _object("E01.mkv"),
                _object("e01.MKV"),
            ))

    def test_from_listing_rows_serializes_deterministically(self) -> None:
        manifest = SourceManifest.from_listing_rows([
            {"full_path": f"{ROOT}/Z.mkv", "size": 5},
            {"full_path": f"{ROOT}/A.srt", "size": 2},
        ], root_path=ROOT, snapshot_id="snapshot-a")

        self.assertEqual(manifest.object_paths, (f"{ROOT}/A.srt", f"{ROOT}/Z.mkv"))
        self.assertEqual(SourceManifest.from_dict(manifest.as_dict()), manifest)


class TestFreshManifestComparison(unittest.TestCase):
    def test_new_snapshot_id_does_not_itself_count_as_source_drift(self) -> None:
        expected = SourceManifest("snapshot-a", ROOT, (_object("E01.mkv"),))
        fresh = SourceManifest("snapshot-b", ROOT, (_object("E01.mkv", snapshot_id="snapshot-b"),))

        comparison = compare_fresh_manifest(expected, fresh)

        self.assertTrue(comparison.matches)
        self.assertEqual(comparison.unchanged_paths, (f"{ROOT}/E01.mkv",))

    def test_fresh_comparison_reports_missing_new_and_changed_objects(self) -> None:
        expected = SourceManifest("snapshot-a", ROOT, (
            _object("E01.mkv"),
            _object("E02.mkv"),
            _object("E03.mkv"),
        ))
        fresh = SourceManifest("snapshot-b", ROOT, (
            _object("E01.mkv", snapshot_id="snapshot-b", size=2048, modified="2026-08-21T02:00:00Z"),
            _object("E03.mkv", snapshot_id="snapshot-b"),
            _object("PV.mkv", snapshot_id="snapshot-b"),
        ))

        comparison = expected.compare_fresh(fresh)

        self.assertFalse(comparison.matches)
        self.assertEqual(comparison.missing_paths, (f"{ROOT}/E02.mkv",))
        self.assertEqual(comparison.unexpected_paths, (f"{ROOT}/PV.mkv",))
        self.assertEqual(comparison.changed[0].path, f"{ROOT}/E01.mkv")
        self.assertEqual(comparison.changed[0].changed_fields, ("size", "modified"))
        self.assertEqual(comparison.unchanged_paths, (f"{ROOT}/E03.mkv",))
        with self.assertRaises(SourceManifestDriftError):
            expected.require_fresh_match(fresh)

    def test_root_change_is_drift_even_when_object_rows_match(self) -> None:
        expected = SourceManifest("snapshot-a", ROOT, (_object("E01.mkv"),))
        fresh = SourceManifest("snapshot-b", "/quark/影视/待刮削", (
            _object("Example/E01.mkv", snapshot_id="snapshot-b"),
        ))

        self.assertFalse(compare_fresh_manifest(expected, fresh).matches)


class TestSourceObjectOwnership(unittest.TestCase):
    def test_each_object_can_have_one_workunit_residual_or_attention_owner(self) -> None:
        owned = validate_source_object_ownership(
            work_units={"unit-1": (_object("E01.mkv"),)},
            residuals={"residual-menu": (_object("menu/index.html", object_type="other", size=8),)},
            attentions={"attention-pv": (_object("PV.mkv"),)},
        )

        self.assertEqual(owned[f"{ROOT}/E01.mkv"].owner_kind, "work_unit")
        self.assertEqual(owned[f"{ROOT}/menu/index.html"].owner_id, "residual-menu")
        self.assertEqual(owned[f"{ROOT}/PV.mkv"].owner_kind, "attention")

    def test_duplicate_workunit_and_residual_claim_fails_closed(self) -> None:
        with self.assertRaises(SourceObjectOwnershipError):
            validate_source_object_ownership(
                work_units={"unit-1": (_object("E01.mkv"),)},
                residuals={"residual-1": (_object("E01.mkv"),)},
            )

    def test_manifest_ownership_requires_full_coverage_and_current_object_state(self) -> None:
        manifest = SourceManifest("snapshot-a", ROOT, (
            _object("E01.mkv"),
            _object("PV.mkv"),
        ))
        with self.assertRaises(SourceObjectOwnershipError):
            validate_source_object_ownership(
                manifest=manifest,
                work_units={"unit-1": (_object("E01.mkv"),)},
            )
        with self.assertRaises(SourceObjectOwnershipError):
            validate_source_object_ownership(
                manifest=manifest,
                work_units={"unit-1": (_object("E01.mkv", size=2048),)},
                attentions={"attention-pv": (_object("PV.mkv"),)},
            )

        owned = validate_source_object_ownership(
            manifest=manifest,
            work_units={"unit-1": (_object("E01.mkv"),)},
            attentions={"attention-pv": (_object("PV.mkv"),)},
        )
        self.assertEqual(set(owned), {f"{ROOT}/E01.mkv", f"{ROOT}/PV.mkv"})

    def test_same_owner_redeclaring_an_object_also_fails(self) -> None:
        claim = SourceObjectClaim("work_unit", "unit-1", (_object("E01.mkv"),))
        with self.assertRaises(SourceObjectOwnershipError):
            validate_unique_source_object_ownership((claim, claim))

    def test_mixed_snapshot_claims_fail_closed(self) -> None:
        with self.assertRaises(SourceObjectOwnershipError):
            validate_source_object_ownership(
                work_units={"unit-1": (_object("E01.mkv", snapshot_id="snapshot-a"),)},
                residuals={"residual-1": (_object("PV.mkv", snapshot_id="snapshot-b"),)},
            )

    def test_non_mapping_owner_collection_is_rejected_even_when_empty(self) -> None:
        with self.assertRaises(SourceObjectValidationError):
            validate_source_object_ownership(work_units=[])  # type: ignore[arg-type]

    def test_case_ambiguous_claims_fail_closed(self) -> None:
        with self.assertRaises(SourceObjectOwnershipError):
            validate_source_object_ownership(
                work_units={"unit-1": (_object("E01.mkv"),)},
                residuals={"residual-1": (_object("e01.MKV"),)},
            )


if __name__ == "__main__":
    unittest.main()
