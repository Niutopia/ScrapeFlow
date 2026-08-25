"""Focused C-stage TV-season WorkUnit coalescing tests."""

from __future__ import annotations

import unittest
from dataclasses import replace

from engine.scrapeflow.work_unit_coalescing import (
    coalesce_confirmed_tv_season_work_units,
)
from engine.scrapeflow.work_units import WorkUnitRecord


class ConfirmedTvSeasonCoalescingTests(unittest.TestCase):
    root = "/incoming/Season Bundle"

    def _record(
        self,
        unit_id: str,
        source_path: str,
        *,
        tmdb_id: int = 7001,
        season: int | None = None,
        claimed_seasons: tuple[int, ...] = (),
    ) -> WorkUnitRecord:
        return WorkUnitRecord(
            work_unit_id=unit_id,
            root_task_id="root-coalesce",
            boundary_key=source_path,
            source_paths=(source_path,),
            source_revision=1,
            role="season",
            display_label=source_path.rsplit("/", 1)[-1],
            claimed_seasons=claimed_seasons,
            media_context="tv",
            identity_status="confirmed",
            identity={
                "media_type": "tv",
                "tmdb_id": tmdb_id,
                "season": season,
                "source": "operator_override",
            },
        )

    def _snapshot(
        self,
        *members: tuple[str, tuple[str, ...]],
    ) -> dict[str, object]:
        rows: list[dict[str, object]] = []
        for directory, filenames in members:
            path = f"{self.root}/{directory}"
            rows.append({"name": directory, "is_dir": True, "full_path": path})
            rows.extend({
                "name": filename,
                "is_dir": False,
                "size": 1_073_741_824,
                "full_path": f"{path}/{filename}",
            } for filename in filenames)
        return {"root": self.root, "rows": rows}

    def test_merges_untouched_same_tmdb_sibling_seasons_into_canonical_record(self) -> None:
        season_one = f"{self.root}/Example Release S01"
        season_two = f"{self.root}/Example Release S02"
        records = [
            self._record("unit-s01", season_one, season=1, claimed_seasons=(1,)),
            self._record("unit-s02", season_two, season=2),
        ]
        snapshot = self._snapshot(
            ("Example Release S01", ("Example.Show.S01E01.mkv",)),
            ("Example Release S02", ("Example.Show.S02E01.mkv",)),
        )

        merged = coalesce_confirmed_tv_season_work_units(records, snapshot)

        self.assertEqual(len(merged), 1)
        record = merged[0]
        self.assertEqual(record.work_unit_id, "unit-s01")
        self.assertEqual(record.source_paths, (season_one, season_two))
        self.assertEqual(record.claimed_seasons, (1, 2))
        self.assertEqual(record.role, "single_work")
        self.assertEqual(record.media_context, "tv")
        self.assertNotIn("season", record.identity or {})
        self.assertIsNone(record.reconciliation_outcome)
        self.assertIsNone(record.writer_job_id)

    def test_overlapping_external_scope_blocks_the_entire_group(self) -> None:
        season_one = f"{self.root}/Example Release S01"
        season_two = f"{self.root}/Example Release S02"
        records = [
            self._record("unit-s01", season_one, season=1),
            self._record("unit-s02", season_two, season=2),
            self._record("overlapping-root", self.root, tmdb_id=9001),
        ]
        snapshot = self._snapshot(
            ("Example Release S01", ("Example.Show.S01E01.mkv",)),
            ("Example Release S02", ("Example.Show.S02E01.mkv",)),
        )

        unchanged = coalesce_confirmed_tv_season_work_units(records, snapshot)

        self.assertEqual(unchanged, records)

    def test_conflicting_sxx_file_marker_blocks_the_entire_group(self) -> None:
        season_one = f"{self.root}/Example Release S01"
        season_two = f"{self.root}/Example Release S02"
        records = [
            self._record("unit-s01", season_one, season=1),
            self._record("unit-s02", season_two, season=2),
        ]
        snapshot = self._snapshot(
            # The S02 video inside the explicit S01 directory is contradictory.
            ("Example Release S01", ("Example.Show.S02E01.mkv",)),
            ("Example Release S02", ("Example.Show.S02E01.mkv",)),
        )

        unchanged = coalesce_confirmed_tv_season_work_units(records, snapshot)

        self.assertEqual(unchanged, records)

    def test_acceptance_or_existing_writer_blocks_coalescing(self) -> None:
        season_one = f"{self.root}/Example Release S01"
        season_two = f"{self.root}/Example Release S02"
        records = [
            self._record("unit-s01", season_one, season=1),
            self._record("unit-s02", season_two, season=2),
        ]
        snapshot = self._snapshot(
            ("Example Release S01", ("Example.Show.S01E01.mkv",)),
            ("Example Release S02", ("Example.Show.S02E01.mkv",)),
        )

        self.assertEqual(
            coalesce_confirmed_tv_season_work_units(
                records, snapshot, acceptance_work_unit_ids=("unit-s02",),
            ),
            records,
        )
        with_writer = [records[0], replace(records[1], writer_job_id="unit-s02")]
        self.assertEqual(
            coalesce_confirmed_tv_season_work_units(with_writer, snapshot),
            with_writer,
        )


if __name__ == "__main__":
    unittest.main()
