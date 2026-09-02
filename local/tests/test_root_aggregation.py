"""Tests for the R-node root aggregation and public unit projection."""

from __future__ import annotations

import tempfile
import unittest
from dataclasses import replace
from pathlib import Path

from engine.scrapeflow.gap_ledger import close_gap, discover_episode_gaps
from engine.scrapeflow.work_units import WorkUnitRecord, save_work_unit_records

from local.scrapeflow_api.root_aggregation import (
    aggregate_root_job,
    public_work_unit_row,
)
from local.scrapeflow_api.unit_execution import (
    WorkAcceptanceResult,
    save_work_acceptance,
)


def _unit(
    unit_id: str,
    *,
    identity_status: str = "pending",
    outcome: str | None = None,
    lane_status: str | None = None,
) -> WorkUnitRecord:
    return WorkUnitRecord(
        work_unit_id=unit_id,
        root_task_id="root-agg",
        boundary_key=f"/incoming/series/{unit_id}",
        source_paths=(f"/incoming/series/{unit_id}",),
        source_revision=1,
        role="series_container",
        media_context="tv",
        identity_status=identity_status,
        identity=(
            {"media_type": "tv", "tmdb_id": 1, "title": "Show", "year": "2020", "confidence": 1.0}
            if identity_status == "confirmed" else None
        ),
        candidate_identities=(
            ({"media_type": "tv", "tmdb_id": 2, "title": "Other", "year": "2021", "confidence": 0.6},)
            if identity_status == "uncertain" else ()
        ),
        reconciliation_outcome=outcome,
        matched_work_root=(
            "/library/番剧/Show" if outcome == "duplicate_complete" else None
        ),
        lane_status=lane_status,
    )


class RootAggregationTests(unittest.TestCase):
    def test_aggregate_counts_units_gaps_and_acceptance(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            state_root = Path(directory)
            units = [
                _unit("u1", identity_status="confirmed", outcome="new_work"),
                _unit("u2", identity_status="uncertain"),
                _unit(
                    "u3", identity_status="confirmed",
                    outcome="duplicate_complete", lane_status="duplicate_consumed",
                ),
            ]
            save_work_unit_records(state_root, "root-agg", units)
            save_work_acceptance(state_root, "root-agg", [
                WorkAcceptanceResult(
                    work_unit_id="u1", outcome="accepted", writer_job_id="unit-u1",
                    phase="executed", target_root="/library/番剧/Show",
                    planned_files=1, error=None, recorded_at="2026-08-16T00:00:00Z",
                ),
            ])
            discover_episode_gaps(
                state_root, "root-agg", "u1",
                media_type="tv", tmdb_id=1,
                expected_by_season={1: [2, 3]},
                actual_tokens=["S01E01"],
            )
            gaps = discover_episode_gaps(
                state_root, "root-agg", "u1",
                media_type="tv", tmdb_id=1,
                expected_by_season={1: [2, 3]},
                actual_tokens=["S01E01"],
            )
            close_gap(state_root, "root-agg", gaps[0].gap_id)

            aggregate = aggregate_root_job(state_root, "root-agg")
            self.assertEqual(aggregate.unit_count, 3)
            self.assertEqual(aggregate.completed, 2)   # accepted + duplicate
            self.assertEqual(aggregate.attention, 1)   # uncertain identity
            self.assertEqual(aggregate.in_progress, 0)
            self.assertEqual(aggregate.failed, 0)
            self.assertEqual(aggregate.open_gaps, 1)
            self.assertEqual(aggregate.closed_gaps, 1)
            # Identity uncertainty remains the stronger root-level stop even
            # when another accepted unit also has a replenishment Gap.
            self.assertEqual(aggregate.status, "needs_attention")

    def test_open_gap_is_not_a_completed_root_aggregate(self) -> None:
        """H acceptance remains visible, but J/N keeps the root pending."""
        with tempfile.TemporaryDirectory() as directory:
            state_root = Path(directory)
            unit = _unit("u1", identity_status="confirmed", outcome="new_work")
            save_work_unit_records(state_root, "root-agg", [unit])
            save_work_acceptance(state_root, "root-agg", [
                WorkAcceptanceResult(
                    work_unit_id="u1", outcome="accepted", writer_job_id="unit-u1",
                    phase="executed", target_root="/library/番剧/Show",
                    planned_files=1, error=None, recorded_at="2026-08-16T00:00:00Z",
                ),
            ])
            discover_episode_gaps(
                state_root, "root-agg", "u1",
                media_type="tv", tmdb_id=1,
                expected_by_season={1: [1, 2]},
                actual_tokens=["S01E01"],
            )

            aggregate = aggregate_root_job(state_root, "root-agg")

            self.assertEqual(aggregate.completed, 1)
            self.assertEqual(aggregate.open_gaps, 1)
            self.assertEqual(aggregate.status, "gaps_pending")
            self.assertEqual(aggregate.as_dict()["status"], "gaps_pending")

    def test_public_projection_hides_internal_json(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            state_root = Path(directory)
            unit = _unit("u2", identity_status="uncertain")
            save_work_unit_records(state_root, "root-agg", [unit])
            row = public_work_unit_row(unit, state_root, "root-agg")
            self.assertEqual(row["work_unit_id"], "u2")
            self.assertEqual(row["identity_status"], "uncertain")
            self.assertEqual(row["candidate_identities"][0]["tmdb_id"], 2)
            self.assertNotIn("plan", row)
            self.assertNotIn("summary", row)
            self.assertNotIn("decision_trace", row)

    def test_public_projection_shows_bounded_expansion_provenance(self) -> None:
        """An expansion-born unit exposes its basis, never its internals."""
        with tempfile.TemporaryDirectory() as directory:
            state_root = Path(directory)
            unit = replace(
                _unit("u3", identity_status="confirmed"),
                disc_expansion={
                    "basis": "operator-ruling",
                    "tmdb_id": 34307,
                    "season": 3,
                    "source_scope": "/quark/影视/待刮削/无耻之徒/第三季",
                    "staging_scope": "/quark/影视/ScrapeFlow/展开/r/scope",
                    "members": [{"episode": index} for index in range(1, 13)],
                },
            )
            row = public_work_unit_row(unit, state_root, "root-agg")
            provenance = row["disc_expansion"]
            self.assertEqual(provenance["basis"], "operator-ruling")
            self.assertEqual(provenance["tmdb_id"], 34307)
            self.assertEqual(provenance["members"], 12)
            self.assertNotIn("staging_scope", provenance)
            self.assertNotIn("members_list", provenance)

            plain = public_work_unit_row(
                _unit("u4", identity_status="confirmed"), state_root, "root-agg",
            )
            self.assertIsNone(plain["disc_expansion"])


if __name__ == "__main__":
    unittest.main()
