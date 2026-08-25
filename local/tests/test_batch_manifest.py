"""Focused safety tests for the passive batch-manifest domain module."""

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from engine.scrapeflow.intake_source import intake_source_id
from local.scrapeflow_api.batch_manifest import (
    BATCH_ITEM_STATES,
    BatchManifest,
    BatchManifestItem,
    BatchManifestTransitionError,
    BatchManifestValidationError,
    FreshResult,
    load_batch_manifest,
    save_batch_manifest,
    transition_batch_item,
)


def _item(
    path: str = "/quark/影视/待刮削/Fate",
    *,
    shelf: str | None = "anime",
    sort: int = 10,
    state: str = "scheduled",
    fresh: FreshResult | None = None,
) -> BatchManifestItem:
    return BatchManifestItem(
        source_id=intake_source_id(path),
        source_path_snapshot=path,
        shelf=shelf,
        sort=sort,
        state=state,
        recent_fresh_result=fresh,
    )


def _fresh(*, present: bool = True, media_count: int = 3) -> FreshResult:
    return FreshResult(
        checked_at="2026-08-21T01:02:03Z",
        source_present=present,
        media_count=media_count,
        snapshot_id="fresh-snapshot-1",
    )


class TestBatchManifestSchema(unittest.TestCase):
    def test_exact_states_are_exposed(self) -> None:
        self.assertEqual(
            BATCH_ITEM_STATES,
            {
                "scheduled",
                "inspecting",
                "ready",
                "active",
                "completed",
                "needs_attention",
                "skipped_currently_nonmedia",
                "technical_failure",
            },
        )

    def test_item_has_only_queue_metadata_not_a_control_record(self) -> None:
        row = _item().as_dict()
        self.assertEqual(
            set(row),
            {
                "source_id",
                "source_path_snapshot",
                "shelf",
                "sort",
                "state",
                "recent_fresh_result",
            },
        )
        self.assertNotIn("root_job_id", row)
        self.assertNotIn("paused", row)

    def test_source_id_must_be_the_intake_identity_for_its_path(self) -> None:
        with self.assertRaises(BatchManifestValidationError):
            BatchManifestItem(
                source_id=intake_source_id("/quark/影视/待刮削/Other"),
                source_path_snapshot="/quark/影视/待刮削/Fate",
                shelf="anime",
                sort=0,
                state="scheduled",
            )

    def test_path_must_be_canonical_absolute_posix(self) -> None:
        with self.assertRaises(BatchManifestValidationError):
            _item("/quark/影视/待刮削/../Fate")
        with self.assertRaises(BatchManifestValidationError):
            _item("/quark/影视/待刮削/Fate/")

    def test_untyped_state_is_a_validation_error(self) -> None:
        with self.assertRaises(BatchManifestValidationError):
            BatchManifestItem(
                source_id=intake_source_id("/quark/影视/待刮削/Fate"),
                source_path_snapshot="/quark/影视/待刮削/Fate",
                shelf="anime",
                sort=0,
                state=[],  # type: ignore[arg-type]
            )

    def test_ready_and_active_need_present_media_fresh_result_and_shelf(self) -> None:
        with self.assertRaises(BatchManifestValidationError):
            _item(state="ready")
        with self.assertRaises(BatchManifestValidationError):
            _item(state="active", shelf=None, fresh=_fresh())
        ready = _item(state="ready", fresh=_fresh())
        self.assertEqual(ready.fresh_result.media_count, 3)  # type: ignore[union-attr]

    def test_skipped_currently_nonmedia_requires_a_real_zero_media_inspection(self) -> None:
        with self.assertRaises(BatchManifestValidationError):
            _item(state="skipped_currently_nonmedia", fresh=_fresh())
        skipped = _item(
            shelf=None,
            state="skipped_currently_nonmedia",
            fresh=_fresh(media_count=0),
        )
        self.assertEqual(skipped.state, "skipped_currently_nonmedia")

    def test_manifest_enforces_unique_owner_order_and_single_active(self) -> None:
        first = _item(state="active", fresh=_fresh())
        second = _item(
            "/quark/影视/待刮削/高达",
            sort=20,
            state="active",
            fresh=_fresh(),
        )
        with self.assertRaises(BatchManifestValidationError):
            BatchManifest(items=(first, second))
        with self.assertRaises(BatchManifestValidationError):
            BatchManifest(items=(first, _item(sort=10)))


class TestBatchManifestTransitions(unittest.TestCase):
    def test_happy_path_clears_old_evidence_before_each_inspection(self) -> None:
        item = _item()
        item = transition_batch_item(item, "inspecting")
        item = transition_batch_item(item, "ready", recent_fresh_result=_fresh())
        item = transition_batch_item(item, "active")
        item = transition_batch_item(item, "completed")
        self.assertEqual(item.state, "completed")
        self.assertEqual(item.source_path, "/quark/影视/待刮削/Fate")

    def test_completed_source_cannot_be_reactivated(self) -> None:
        item = _item()
        item = transition_batch_item(item, "inspecting")
        item = transition_batch_item(item, "ready", recent_fresh_result=_fresh())
        item = transition_batch_item(item, "active")
        item = transition_batch_item(item, "completed")
        with self.assertRaises(BatchManifestTransitionError):
            transition_batch_item(item, "inspecting")

    def test_attention_and_currently_nonmedia_reenter_via_fresh_inspection(self) -> None:
        attention = transition_batch_item(_item(), "needs_attention", shelf=None)
        self.assertEqual(transition_batch_item(attention, "inspecting").state, "inspecting")

        skipped = _item(
            shelf=None,
            state="skipped_currently_nonmedia",
            fresh=_fresh(media_count=0),
        )
        self.assertEqual(transition_batch_item(skipped, "inspecting").fresh_result, None)

    def test_manifest_transition_refuses_second_active_item(self) -> None:
        first = _item(
            "/quark/影视/待刮削/Fate",
            sort=0,
            state="ready",
            fresh=_fresh(),
        )
        second = _item(
            "/quark/影视/待刮削/高达",
            sort=1,
            state="ready",
            fresh=_fresh(),
        )
        manifest = BatchManifest(items=(first, second)).transition(first.source_id, "active")
        with self.assertRaises(BatchManifestValidationError):
            manifest.transition(second.source_id, "active")

    def test_next_ready_is_ordered_and_does_not_promote_scheduled_items(self) -> None:
        scheduled = _item("/quark/影视/待刮削/Fate", sort=0)
        ready = _item(
            "/quark/影视/待刮削/高达", sort=1, state="ready", fresh=_fresh(),
        )
        manifest = BatchManifest(items=(ready, scheduled))
        self.assertEqual(manifest.next_ready(), ready)
        self.assertIsNone(BatchManifest(items=(scheduled,)).next_ready())


class TestBatchManifestPersistence(unittest.TestCase):
    def test_missing_file_means_empty_not_an_implicit_job(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            self.assertEqual(load_batch_manifest(Path(tmp)), BatchManifest.empty())

    def test_round_trip_is_atomic_schema_and_preserves_sorted_queue(self) -> None:
        early = _item(
            "/quark/影视/待刮削/早",
            sort=1,
            state="ready",
            fresh=_fresh(),
        )
        later = _item("/quark/影视/待刮削/晚", sort=99)
        manifest = BatchManifest(items=(later, early))
        with tempfile.TemporaryDirectory() as tmp:
            state_dir = Path(tmp)
            save_batch_manifest(state_dir, manifest)
            raw = json.loads((state_dir / "batch-manifest.json").read_text(encoding="utf-8"))
            self.assertEqual([row["sort"] for row in raw["items"]], [1, 99])
            self.assertEqual(load_batch_manifest(state_dir).ordered_items(), (early, later))

    def test_existing_corrupt_or_extra_control_field_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "batch-manifest.json"
            path.write_text("{not-json", encoding="utf-8")
            with self.assertRaises(BatchManifestValidationError):
                load_batch_manifest(Path(tmp))

            path.write_text(
                json.dumps({"version": 1, "items": [], "paused": False}),
                encoding="utf-8",
            )
            with self.assertRaises(BatchManifestValidationError):
                load_batch_manifest(Path(tmp))


if __name__ == "__main__":
    unittest.main()
