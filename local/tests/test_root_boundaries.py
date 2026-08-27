"""Tests for the runtime B/W composition (root_boundaries)."""

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from engine.scrapeflow.root_boundaries import (
    analyze_root_boundaries,
    build_root_boundary_analysis,
    load_source_manifest,
    walk_source_rows,
)
from engine.scrapeflow.work_units import load_work_unit_records
from engine.scrapeflow.source_inventory import build_scoped_source_node, build_source_inventory

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

    def test_disc_image_source_persists_visible_content_expansion_attention(self) -> None:
        root = "/incoming/Disc source"
        alist = DictAList({
            root: [
                {"name": "Season 01", "is_dir": True},
                {"name": "Season 02", "is_dir": True},
            ],
            f"{root}/Season 01": [
                {"name": "Disc 1.iso", "is_dir": False, "size": 45 * 1024**3},
            ],
            f"{root}/Season 02": [
                {"name": "Disc 2.iso", "is_dir": False, "size": 45 * 1024**3},
            ],
        })
        with tempfile.TemporaryDirectory() as directory:
            state_root = Path(directory)
            records = analyze_root_boundaries(
                alist, root, root_task_id="root-disc", state_root=state_root,
            )
            loaded = load_work_unit_records(state_root, "root-disc")

        self.assertEqual(records, loaded)
        self.assertEqual(len(records), 1)
        record = records[0]
        self.assertTrue(record.requires_content_expansion)
        self.assertEqual(record.identity_status, "uncertain")
        self.assertEqual(record.media_context, "unknown")
        self.assertIsNone(record.identity)
        self.assertIn("只读安全内容展开", record.attention or "")

    def test_executable_masquerade_source_requires_expansion_not_silent_skip(self) -> None:
        root = "/incoming/Masquerade"
        alist = DictAList({
            root: [
                {"name": "Show S1", "is_dir": True},
            ],
            f"{root}/Show S1": [
                {"name": "[FSH] Show - 01 [BD].exe", "is_dir": False, "size": 400 * 1024**3},
                {"name": "[FSH] Show - 02 [BD].exe", "is_dir": False, "size": 400 * 1024**3},
            ],
        })
        with tempfile.TemporaryDirectory() as directory:
            state_root = Path(directory)
            records = analyze_root_boundaries(
                alist, root, root_task_id="root-exe", state_root=state_root,
            )
        self.assertEqual(len(records), 1)
        record = records[0]
        self.assertTrue(record.requires_content_expansion)
        self.assertEqual(record.identity_status, "uncertain")
        self.assertIn("伪装视频 .exe", record.attention or "")

    def test_boundary_snapshot_persists_exact_source_object_manifest(self) -> None:
        root = "/incoming/Exact"
        alist = DictAList({
            root: [
                {"name": "Season 01", "is_dir": True, "size": 0},
                {"name": "poster.jpg", "is_dir": False, "size": 7, "version": "v1"},
            ],
            f"{root}/Season 01": [
                {"name": "E01.mkv", "is_dir": False, "size": 1024, "mtime": "m1"},
            ],
        })
        with tempfile.TemporaryDirectory() as directory:
            state_root = Path(directory)
            records = analyze_root_boundaries(
                alist, root, root_task_id="root-exact", state_root=state_root,
            )
            manifest = load_source_manifest(state_root, "root-exact")

        self.assertTrue(records)
        self.assertIsNotNone(manifest)
        assert manifest is not None
        self.assertEqual(manifest.root_path, root)
        self.assertEqual(
            manifest.object_paths,
            (f"{root}/Season 01", f"{root}/Season 01/E01.mkv", f"{root}/poster.jpg"),
        )
        self.assertEqual(manifest.object_at(f"{root}/Season 01/E01.mkv").size, 1024)  # type: ignore[union-attr]

    def test_boundary_snapshot_records_missing_exact_object_metadata_without_faking_it(self) -> None:
        root = "/incoming/No-size"
        alist = DictAList({
            root: [{"name": "E01.mkv", "is_dir": False}],
        })
        snapshot, _records = build_root_boundary_analysis(
            alist, root, root_task_id="root-no-size",
        )

        self.assertNotIn("source_manifest", snapshot)
        self.assertIn("source_manifest_error", snapshot)

    def test_flat_movie_units_claim_exact_file_scopes(self) -> None:
        root = "/incoming/paired-films"
        first = f"{root}/First Feature Film 2160p.mkv"
        second = f"{root}/Second Feature Film 2160p.mkv"
        alist = DictAList({
            root: [
                {"name": "First Feature Film 2160p.mkv", "is_dir": False, "size": 300 * 1024 * 1024},
                {"name": "Second Feature Film 2160p.mkv", "is_dir": False, "size": 301 * 1024 * 1024},
            ],
        })
        with tempfile.TemporaryDirectory() as directory:
            state_root = Path(directory)
            records = analyze_root_boundaries(
                alist, root, root_task_id="flat-pair", state_root=state_root,
            )
            self.assertEqual(len(records), 2)
            self.assertEqual(
                {record.source_paths for record in records},
                {(first,), (second,)},
            )
            snapshot = json.loads(
                (state_root / "work_snapshot_flat-pair.json").read_text(encoding="utf-8")
            )
            node = build_source_inventory(snapshot["rows"], root)
            for record in records:
                scoped = build_scoped_source_node(
                    node,
                    record.source_paths,
                    boundary_key=record.boundary_key,
                    display_label=record.display_label,
                )
                self.assertEqual(len(scoped.files), 1)
                self.assertEqual(scoped.files[0].path, record.source_paths[0])

    def test_decorated_sibling_seasons_persist_one_exact_multi_source_unit(self) -> None:
        root = "/incoming/Northwind Bundle"
        entries: dict[str, list[dict[str, object]]] = {
            root: [
                *[
                    {"name": f"Northwind.Show.S{season:02d}.1080p", "is_dir": True}
                    for season in (1, 2, 3, 4)
                ],
                {"name": "Northwind.Aftershow", "is_dir": True},
            ],
            f"{root}/Northwind.Show.S04.1080p": [],
            f"{root}/Northwind.Aftershow": [
                {"name": "Northwind.Aftershow.E01.mkv", "is_dir": False, "size": 10},
            ],
        }
        for season in (1, 2, 3):
            entries[f"{root}/Northwind.Show.S{season:02d}.1080p"] = [
                {
                    "name": f"Northwind.Show.S{season:02d}E01.mkv",
                    "is_dir": False,
                    "size": 10,
                },
            ]
        alist = DictAList(entries)
        with tempfile.TemporaryDirectory() as directory:
            state_root = Path(directory)
            records = analyze_root_boundaries(
                alist, root, root_task_id="root-northwind", state_root=state_root,
            )
            loaded = load_work_unit_records(state_root, "root-northwind")

        self.assertEqual(records, loaded)
        self.assertEqual(len(records), 2)
        cohort = next(record for record in records if len(record.source_paths) == 4)
        self.assertEqual(cohort.claimed_seasons, (1, 2, 3, 4))
        self.assertEqual(cohort.source_revision, 1)
        self.assertTrue(all(record.identity_status == "pending" for record in records))

    def test_mixed_season_and_generic_film_group_persist_disjoint_units(self) -> None:
        """B/W must persist a TV scope separate from nested film title scopes."""
        root = "/incoming/Example Show"
        alist = DictAList({
            root: [
                {"name": "S01", "is_dir": True},
                {"name": "S02", "is_dir": True},
                {"name": "SP", "is_dir": True},
                {"name": "剧场版", "is_dir": True},
            ],
            f"{root}/S01": [
                {"name": "Example.Show.S01E01.mkv", "is_dir": False, "size": 1_073_741_824},
            ],
            f"{root}/S02": [
                {"name": "Example.Show.S02E01.mkv", "is_dir": False, "size": 1_073_741_824},
            ],
            f"{root}/SP": [
                {"name": "Example.Show.S00E01.mkv", "is_dir": False, "size": 536_870_912},
            ],
            f"{root}/剧场版": [
                {"name": "Example Feature (2019)", "is_dir": True},
                {"name": "Example Reminiscence (2021)", "is_dir": True},
            ],
            f"{root}/剧场版/Example Feature (2019)": [
                {"name": "Example.Feature.2019.mkv", "is_dir": False, "size": 2_147_483_648},
            ],
            f"{root}/剧场版/Example Reminiscence (2021)": [
                {"name": "Example.Reminiscence.2021.mkv", "is_dir": False, "size": 2_147_483_648},
                {"name": "Example.Reminiscence.Promo.mkv", "is_dir": False, "size": 67_108_864},
            ],
        })
        with tempfile.TemporaryDirectory() as directory:
            state_root = Path(directory)
            records = analyze_root_boundaries(
                alist, root, root_task_id="root-mixed", state_root=state_root,
            )
            loaded = load_work_unit_records(state_root, "root-mixed")

        self.assertEqual(records, loaded)
        self.assertEqual(len(records), 3)
        tv = next(record for record in records if record.display_label == "Example Show")
        self.assertEqual(tv.role, "single_work")
        self.assertEqual(tv.source_paths, (f"{root}/S01", f"{root}/S02", f"{root}/SP"))
        self.assertEqual(tv.claimed_seasons, (1, 2))
        self.assertEqual(
            {record.source_paths for record in records if record is not tv},
            {
                (f"{root}/剧场版/Example Feature (2019)",),
                (f"{root}/剧场版/Example Reminiscence (2021)",),
            },
        )


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
