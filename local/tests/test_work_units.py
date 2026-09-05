"""Corruption semantics and save readback for the work-unit ledger."""

import tempfile
import unittest
from pathlib import Path

from engine.scrapeflow.work_units import (
    load_work_unit_records,
    save_work_unit_records,
)




class WorkUnitLedgerCorruptionTests(unittest.TestCase):
    """Absence is empty; corruption fails closed instead of reading as no-units."""

    def test_missing_ledger_is_empty(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            state = Path(directory)
            self.assertEqual(load_work_unit_records(state, "root-x"), [])

    def test_corrupt_ledger_raises(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            state = Path(directory)
            (state / "work_units_root-x.json").write_text("{{broken", encoding="utf-8")
            with self.assertRaises(ValueError):
                load_work_unit_records(state, "root-x")

    def test_non_list_ledger_raises(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            state = Path(directory)
            (state / "work_units_root-x.json").write_text('{"units": []}', encoding="utf-8")
            with self.assertRaises(ValueError):
                load_work_unit_records(state, "root-x")

    def test_invalid_row_raises(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            state = Path(directory)
            (state / "work_units_root-x.json").write_text("[42]", encoding="utf-8")
            with self.assertRaises(ValueError):
                load_work_unit_records(state, "root-x")

    def test_save_reads_back_exact_count(self) -> None:
        from engine.scrapeflow.work_units import WorkUnitRecord

        with tempfile.TemporaryDirectory() as directory:
            state = Path(directory)
            records = [
                WorkUnitRecord(
                    work_unit_id=f"unit-{i}",
                    root_task_id="root-x",
                    boundary_key="/in",
                    source_paths=("/in",),
                    source_revision=1,
                    role="single_work",
                    display_label="W",
                )
                for i in range(3)
            ]
            save_work_unit_records(state, "root-x", records)
            self.assertEqual(len(load_work_unit_records(state, "root-x")), 3)
