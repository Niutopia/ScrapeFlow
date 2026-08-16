"""Tests for the runtime B/W composition (root_boundaries)."""

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from engine.scrapeflow.root_boundaries import (
    analyze_root_boundaries,
    walk_source_rows,
)
from engine.scrapeflow.work_units import load_work_unit_records

_FIXTURE_DIR = Path(__file__).parent / "fixtures" / "media_cases"


class DictAList:
    """Bounded AList double serving one in-memory tree per path."""

    def __init__(self, entries: dict[str, list[dict[str, object]]]) -> None:
        self.entries = {str(path).rstrip("/"): rows for path, rows in entries.items()}
        self.list_calls: list[str] = []

    def list(self, path: str, refresh: bool = False) -> list[dict[str, object]]:
        del refresh
        self.list_calls.append(str(path).rstrip("/"))
        return [dict(row) for row in self.entries.get(str(path).rstrip("/"), [])]


def _entries_from_fixture(case_id: str) -> dict[str, list[dict[str, object]]]:
    raw = json.loads(
        (_FIXTURE_DIR / case_id / "source_tree.json").read_text(encoding="utf-8")
    )
    root = raw["root"]
    entries: dict[str, list[dict[str, object]]] = {}

    def walk(node: dict, path: str) -> None:
        rows: list[dict[str, object]] = []
        for child in node.get("children", []):
            row = {
                "name": child["name"],
                "is_dir": bool(child.get("is_dir", False)),
                "size": int(child.get("size", 0)),
            }
            rows.append(row)
            if row["is_dir"]:
                walk(child, path.rstrip("/") + "/" + child["name"])
        entries[path.rstrip("/")] = rows

    walk(raw, root)
    return entries


class RootBoundaryCompositionTests(unittest.TestCase):
    def _analyze(self, case_id: str, root_task_id: str = "root-test"):
        alist = DictAList(_entries_from_fixture(case_id))
        fixture = json.loads(
            (_FIXTURE_DIR / case_id / "source_tree.json").read_text(encoding="utf-8")
        )
        with tempfile.TemporaryDirectory() as directory:
            state_root = Path(directory)
            records = analyze_root_boundaries(
                alist,
                fixture["root"],
                root_task_id=root_task_id,
                state_root=state_root,
            )
            loaded = load_work_unit_records(state_root, root_task_id)
            return records, loaded, alist

    def test_fate_container_splits_into_independent_work_units(self) -> None:
        records, loaded, alist = self._analyze("fate_container")
        self.assertEqual(len(records), 3)
        self.assertEqual(len(loaded), 3)
        self.assertEqual(
            {record.boundary_key for record in records},
            {
                "/quark/影视/待刮削/Fate系列/空之境界",
                "/quark/影视/待刮削/Fate系列/Fate Stay Night UBW",
                "/quark/影视/待刮削/Fate系列/Fate Zero",
            },
        )
        for record in records:
            self.assertEqual(record.root_task_id, "root-test")
            self.assertEqual(record.identity_status, "pending")
            self.assertIsNone(record.identity)
            self.assertEqual(record.role, "series_container")
        # The snapshot must have been fetched from the real tree.
        self.assertTrue(any(call.endswith("Fate系列") for call in alist.list_calls))

    def test_multiseason_tv_is_one_single_work(self) -> None:
        records, loaded, _alist = self._analyze("single_tv_multiseason")
        self.assertEqual(len(records), 1)
        self.assertEqual(len(loaded), 1)
        record = records[0]
        self.assertEqual(record.role, "single_work")
        self.assertEqual(record.boundary_key, "/quark/影视/待刮削/绝命毒师")
        self.assertEqual(record.identity_status, "pending")

    def test_ordinary_movie_is_one_movie_work(self) -> None:
        records, _loaded, _alist = self._analyze("ordinary_movie")
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0].role, "single_work")

    def test_subtitle_only_source_is_a_subtitle_group(self) -> None:
        records, _loaded, _alist = self._analyze("subtitle_only")
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0].role, "subtitle_group")


class WalkSourceRowsTests(unittest.TestCase):
    def test_walk_returns_directories_and_files_with_full_paths(self) -> None:
        alist = DictAList(
            {
                "/incoming/show": [
                    {"name": "Season 01", "is_dir": True},
                    {"name": "poster.jpg", "is_dir": False, "size": 3},
                ],
                "/incoming/show/Season 01": [
                    {"name": "S01E01.mkv", "is_dir": False, "size": 5},
                ],
            }
        )
        rows = walk_source_rows(alist, "/incoming/show")
        paths = {row["full_path"] for row in rows}
        self.assertEqual(paths, {
            "/incoming/show/Season 01",
            "/incoming/show/poster.jpg",
            "/incoming/show/Season 01/S01E01.mkv",
        })

    def test_walk_skips_unsafe_names(self) -> None:
        alist = DictAList(
            {
                "/incoming": [
                    {"name": "..", "is_dir": True},
                    {"name": "a/b", "is_dir": False, "size": 1},
                    {"name": "ok.mkv", "is_dir": False, "size": 2},
                ],
            }
        )
        rows = walk_source_rows(alist, "/incoming")
        self.assertEqual([row["full_path"] for row in rows], ["/incoming/ok.mkv"])

    def test_walk_requires_a_listing_port(self) -> None:
        with self.assertRaises(ValueError):
            walk_source_rows(object(), "/incoming")


if __name__ == "__main__":
    unittest.main()
