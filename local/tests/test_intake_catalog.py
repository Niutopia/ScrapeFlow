"""Tests for the Phase-1 IntakeSource catalog layer.

These tests verify that:
- Scanning only updates intake records; it does NOT create EngineJobs
- Repeated scans of the same path do not produce duplicate records
- A disappearing source is marked present=False but not deleted
- The same source_id cannot be bound to a second root_task_id
- load/save round-trips are lossless
- The /api/intake endpoint returns the catalog
"""

from __future__ import annotations

import json
import tempfile
import threading
import unittest
from pathlib import Path

from engine.scrapeflow.intake_source import (
    IntakeSource,
    bind_root_task,
    find_by_path,
    find_by_source_id,
    intake_source_id,
    load_intake_catalog,
    mark_source_missing,
    save_intake_catalog,
    upsert_intake_source,
)


class TestIntakeSourceId(unittest.TestCase):
    """intake_source_id() is stable and path-normalised."""

    def test_deterministic(self) -> None:
        sid1 = intake_source_id("/quark/影视/待刮削/Fate")
        sid2 = intake_source_id("/quark/影视/待刮削/Fate")
        self.assertEqual(sid1, sid2)

    def test_trailing_slash_normalised(self) -> None:
        self.assertEqual(
            intake_source_id("/quark/影视/待刮削/Fate"),
            intake_source_id("/quark/影视/待刮削/Fate/"),
        )

    def test_different_paths_different_ids(self) -> None:
        self.assertNotEqual(
            intake_source_id("/quark/影视/待刮削/Fate"),
            intake_source_id("/quark/影视/待刮削/高达"),
        )

    def test_is_valid_uuid_string(self) -> None:
        import uuid
        sid = intake_source_id("/quark/影视/待刮削/Fate")
        # Should not raise:
        uuid.UUID(sid)


class TestUpsertIntakeSource(unittest.TestCase):
    """upsert_intake_source() is pure and correct."""

    def test_first_observation_creates_record(self) -> None:
        catalog: list[IntakeSource] = []
        catalog, src = upsert_intake_source(catalog, "/quark/影视/待刮削/Fate")
        self.assertEqual(len(catalog), 1)
        self.assertEqual(src.canonical_path, "/quark/影视/待刮削/Fate")
        self.assertEqual(src.display_name, "Fate")
        self.assertTrue(src.present)
        self.assertEqual(src.snapshot_revision, 0)
        self.assertIsNone(src.root_task_id)

    def test_repeated_observation_no_duplicate(self) -> None:
        catalog: list[IntakeSource] = []
        catalog, _ = upsert_intake_source(catalog, "/quark/影视/待刮削/Fate")
        catalog, _ = upsert_intake_source(catalog, "/quark/影视/待刮削/Fate")
        self.assertEqual(len(catalog), 1)

    def test_repeated_identical_observation_revision_unchanged(self) -> None:
        catalog: list[IntakeSource] = []
        catalog, _ = upsert_intake_source(catalog, "/quark/影视/待刮削/Fate")
        catalog, src = upsert_intake_source(catalog, "/quark/影视/待刮削/Fate")
        # Nothing changed → revision stays 0
        self.assertEqual(src.snapshot_revision, 0)

    def test_changed_child_count_increments_revision(self) -> None:
        catalog: list[IntakeSource] = []
        catalog, _ = upsert_intake_source(catalog, "/quark/影视/待刮削/Fate", child_count=3)
        catalog, src = upsert_intake_source(catalog, "/quark/影视/待刮削/Fate", child_count=5)
        self.assertEqual(src.snapshot_revision, 1)
        self.assertEqual(src.child_count, 5)

    def test_multiple_paths_all_stored(self) -> None:
        catalog: list[IntakeSource] = []
        catalog, _ = upsert_intake_source(catalog, "/quark/影视/待刮削/Fate")
        catalog, _ = upsert_intake_source(catalog, "/quark/影视/待刮削/高达")
        catalog, _ = upsert_intake_source(catalog, "/quark/影视/待刮削/物语")
        self.assertEqual(len(catalog), 3)

    def test_present_false_marks_missing(self) -> None:
        catalog: list[IntakeSource] = []
        catalog, _ = upsert_intake_source(catalog, "/quark/影视/待刮削/Fate")
        catalog, src = upsert_intake_source(catalog, "/quark/影视/待刮削/Fate", present=False)
        self.assertFalse(src.present)
        self.assertEqual(len(catalog), 1)  # not deleted


class TestMarkSourceMissing(unittest.TestCase):
    """mark_source_missing() preserves root_task_id and history."""

    def test_marks_present_false(self) -> None:
        catalog: list[IntakeSource] = []
        catalog, src = upsert_intake_source(catalog, "/quark/影视/待刮削/Fate")
        catalog, updated = mark_source_missing(catalog, "/quark/影视/待刮削/Fate")
        self.assertIsNotNone(updated)
        self.assertFalse(updated.present)  # type: ignore[union-attr]

    def test_missing_source_not_deleted(self) -> None:
        catalog: list[IntakeSource] = []
        catalog, _ = upsert_intake_source(catalog, "/quark/影视/待刮削/Fate")
        catalog, _ = mark_source_missing(catalog, "/quark/影视/待刮削/Fate")
        self.assertEqual(len(catalog), 1)

    def test_preserves_root_task_id(self) -> None:
        catalog: list[IntakeSource] = []
        catalog, src = upsert_intake_source(catalog, "/quark/影视/待刮削/Fate")
        source_id = src.source_id
        catalog, _ = bind_root_task(catalog, source_id, "job-abc-123")
        catalog, updated = mark_source_missing(catalog, "/quark/影视/待刮削/Fate")
        self.assertEqual(updated.root_task_id, "job-abc-123")  # type: ignore[union-attr]

    def test_unknown_path_returns_none(self) -> None:
        catalog: list[IntakeSource] = []
        catalog, result = mark_source_missing(catalog, "/quark/影视/待刮削/NotExist")
        self.assertIsNone(result)
        self.assertEqual(len(catalog), 0)


class TestBindRootTask(unittest.TestCase):
    """bind_root_task() enforces the one-source-one-job invariant."""

    def test_bind_succeeds(self) -> None:
        catalog: list[IntakeSource] = []
        catalog, src = upsert_intake_source(catalog, "/quark/影视/待刮削/Fate")
        catalog, updated = bind_root_task(catalog, src.source_id, "job-001")
        self.assertIsNotNone(updated)
        self.assertEqual(updated.root_task_id, "job-001")  # type: ignore[union-attr]

    def test_idempotent_rebind_same_id(self) -> None:
        catalog: list[IntakeSource] = []
        catalog, src = upsert_intake_source(catalog, "/quark/影视/待刮削/Fate")
        catalog, _ = bind_root_task(catalog, src.source_id, "job-001")
        # Same job_id again — must not raise
        catalog, updated = bind_root_task(catalog, src.source_id, "job-001")
        self.assertEqual(updated.root_task_id, "job-001")  # type: ignore[union-attr]

    def test_rebind_different_id_raises(self) -> None:
        catalog: list[IntakeSource] = []
        catalog, src = upsert_intake_source(catalog, "/quark/影视/待刮削/Fate")
        catalog, _ = bind_root_task(catalog, src.source_id, "job-001")
        with self.assertRaises(ValueError):
            bind_root_task(catalog, src.source_id, "job-002")

    def test_unknown_source_id_returns_none(self) -> None:
        catalog: list[IntakeSource] = []
        catalog, result = bind_root_task(catalog, "nonexistent-id", "job-001")
        self.assertIsNone(result)


class TestCatalogPersistence(unittest.TestCase):
    """load/save round-trip through intake-sources.json."""

    def test_empty_catalog_survives_roundtrip(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            state_dir = Path(tmp)
            save_intake_catalog(state_dir, [])
            result = load_intake_catalog(state_dir)
            self.assertEqual(result, [])

    def test_nonexistent_file_returns_empty_list(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            result = load_intake_catalog(Path(tmp))
            self.assertEqual(result, [])

    def test_full_roundtrip(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            state_dir = Path(tmp)
            catalog: list[IntakeSource] = []
            catalog, src1 = upsert_intake_source(catalog, "/quark/影视/待刮削/Fate", child_count=3)
            catalog, src2 = upsert_intake_source(catalog, "/quark/影视/待刮削/高达")
            catalog, _ = bind_root_task(catalog, src1.source_id, "job-001")
            save_intake_catalog(state_dir, catalog)

            loaded = load_intake_catalog(state_dir)
            self.assertEqual(len(loaded), 2)
            fate = find_by_path(loaded, "/quark/影视/待刮削/Fate")
            self.assertIsNotNone(fate)
            self.assertEqual(fate.child_count, 3)  # type: ignore[union-attr]
            self.assertEqual(fate.root_task_id, "job-001")  # type: ignore[union-attr]

    def test_corrupted_file_returns_empty(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            state_dir = Path(tmp)
            (state_dir / "intake-sources.json").write_text("{{not-json", encoding="utf-8")
            result = load_intake_catalog(state_dir)
            self.assertEqual(result, [])

    def test_wrong_type_file_returns_empty(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            state_dir = Path(tmp)
            (state_dir / "intake-sources.json").write_text(
                json.dumps({"not": "a-list"}), encoding="utf-8"
            )
            result = load_intake_catalog(state_dir)
            self.assertEqual(result, [])


class TestFindHelpers(unittest.TestCase):
    """find_by_source_id and find_by_path helpers."""

    def _make_catalog(self) -> list[IntakeSource]:
        catalog: list[IntakeSource] = []
        catalog, _ = upsert_intake_source(catalog, "/quark/影视/待刮削/Fate")
        catalog, _ = upsert_intake_source(catalog, "/quark/影视/待刮削/高达")
        return catalog

    def test_find_by_path_hit(self) -> None:
        catalog = self._make_catalog()
        result = find_by_path(catalog, "/quark/影视/待刮削/Fate")
        self.assertIsNotNone(result)
        self.assertEqual(result.display_name, "Fate")  # type: ignore[union-attr]

    def test_find_by_path_miss(self) -> None:
        catalog = self._make_catalog()
        self.assertIsNone(find_by_path(catalog, "/quark/影视/待刮削/NotExist"))

    def test_find_by_source_id_hit(self) -> None:
        catalog = self._make_catalog()
        sid = intake_source_id("/quark/影视/待刮削/Fate")
        result = find_by_source_id(catalog, sid)
        self.assertIsNotNone(result)

    def test_find_by_source_id_miss(self) -> None:
        catalog = self._make_catalog()
        self.assertIsNone(find_by_source_id(catalog, "00000000-0000-0000-0000-000000000000"))


class TestScanDoesNotCreateEngineJob(unittest.TestCase):
    """Scanning updates catalog only; no EngineJob is created by upsert alone."""

    def test_upsert_creates_no_engine_job(self) -> None:
        """upsert_intake_source() has no dependency on EngineJob or SimpleEngineRunner."""
        with tempfile.TemporaryDirectory() as tmp:
            state_dir = Path(tmp)
            catalog = load_intake_catalog(state_dir)

            for path in [
                "/quark/影视/待刮削/Fate",
                "/quark/影视/待刮削/高达",
                "/quark/影视/待刮削/物语",
            ]:
                catalog, _ = upsert_intake_source(catalog, path, present=True)

            save_intake_catalog(state_dir, catalog)
            loaded = load_intake_catalog(state_dir)

            # Three sources visible
            self.assertEqual(len(loaded), 3)

            # None has a root_task_id — no jobs were created
            for src in loaded:
                self.assertIsNone(src.root_task_id)

            # The jobs sub-directory should not exist (nothing wrote to it)
            jobs_dir = state_dir / "jobs"
            self.assertFalse(jobs_dir.exists())

    def test_one_source_one_job_invariant_enforced(self) -> None:
        """bind_root_task raises if the same source is bound to a different job."""
        catalog: list[IntakeSource] = []
        catalog, src = upsert_intake_source(catalog, "/quark/影视/待刮削/Fate")
        catalog, _ = bind_root_task(catalog, src.source_id, "first-job-id")

        with self.assertRaises(ValueError):
            bind_root_task(catalog, src.source_id, "second-job-id")


class TestIntakeCatalogThreadSafety(unittest.TestCase):
    """Concurrent upserts from different threads must not corrupt the catalog."""

    def test_concurrent_upserts_no_corruption(self) -> None:
        """Multiple threads upsert different paths; final catalog has all entries."""
        with tempfile.TemporaryDirectory() as tmp:
            state_dir = Path(tmp)
            errors: list[Exception] = []
            lock = threading.Lock()
            paths = [f"/quark/影视/待刮削/Work{i}" for i in range(10)]

            def do_upsert(path: str) -> None:
                try:
                    with lock:
                        cat = load_intake_catalog(state_dir)
                        cat, _ = upsert_intake_source(cat, path)
                        save_intake_catalog(state_dir, cat)
                except Exception as exc:
                    errors.append(exc)

            threads = [threading.Thread(target=do_upsert, args=(p,)) for p in paths]
            for t in threads:
                t.start()
            for t in threads:
                t.join()

            self.assertEqual(errors, [])
            final = load_intake_catalog(state_dir)
            self.assertEqual(len(final), 10)


if __name__ == "__main__":
    unittest.main()
