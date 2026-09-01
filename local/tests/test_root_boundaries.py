"""Tests for the runtime B/W composition (root_boundaries)."""

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from dataclasses import replace

from engine.scrapeflow.root_boundaries import (
    analyze_root_boundaries,
    build_root_boundary_analysis,
    load_source_manifest,
    rebuild_root_boundary_if_unwritten,
    walk_source_rows,
)
from engine.scrapeflow.work_units import (
    WorkUnitRecord,
    load_work_unit_records,
    save_work_unit_records,
)
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

    def test_mixed_feature_and_bracket_run_folder_splits_two_units(self) -> None:
        """A feature film beside a bracket-numbered mini-series is two units.

        One release folder can bundle a theatrical feature with a
        same-titled ``[01]``/``[02]`` short series.  The flat splitter must
        emit one exact-file-scope movie candidate for the feature and one
        tv-shaped candidate owning the complete bracketed run, instead of
        parking the whole folder as an unidentifiable mix.
        """
        root = "/incoming/Y 4k 银魂"
        folder = f"{root}/银魂 剧场版 The Final"
        feature = folder + "/[Ygm] Gintama ~The Final~ [Ma10p_2160p][x265_flac_DTS5.1_ass].mkv"
        run_one = folder + "/[Ygm] Gintama ~The Semi-Final~ [01][Ma10p_2160p][x265_flac_ass].mkv"
        run_two = folder + "/[Ygm] Gintama ~The Semi-Final~ [02][Ma10p_2160p][x265_flac_ass].mkv"
        big = 2 * 1024 ** 3
        alist = DictAList({
            root: [
                {"name": "银魂 第一季", "is_dir": True},
                {"name": "银魂 剧场版 The Final", "is_dir": True},
            ],
            f"{root}/银魂 第一季": [
                {"name": f"[Ygm] Gintama S01E{number:02d}.mkv", "is_dir": False, "size": big}
                for number in (1, 2)
            ],
            folder: [
                {"name": Path(feature).name, "is_dir": False, "size": 13 * 1024 ** 3},
                {"name": Path(run_one).name, "is_dir": False, "size": big},
                {"name": Path(run_two).name, "is_dir": False, "size": big},
            ],
        })
        with tempfile.TemporaryDirectory() as directory:
            records = analyze_root_boundaries(
                alist, root, root_task_id="final-mix", state_root=Path(directory),
            )
        by_paths = {record.source_paths: record for record in records}
        self.assertIn((feature,), by_paths)
        self.assertIn((run_one, run_two), by_paths)
        movie = by_paths[(feature,)]
        self.assertEqual(movie.media_context, "movie")
        series = by_paths[(run_one, run_two)]
        self.assertEqual(series.media_context, "tv")
        self.assertEqual(series.display_label, "Gintama ~The Semi-Final~")

    def test_marker_run_and_numbered_sequence_folders_stay_whole(self) -> None:
        """Fail-closed shapes for the child-level flat split.

        A marker-bearing run (``Gintama OAD 2016 [01]``) is physical-special
        evidence anchored by its directory label, and a numbered
        same-franchise sequence (``01. 俯瞰风景``) is one collection.  Both
        must keep their whole-directory boundary.
        """
        root = "/incoming/Guarded Bundle"
        oad_folder = f"{root}/银魂 爱染香篇"
        numbered_folder = f"{root}/剧场版合集"
        big = 2 * 1024 ** 3
        alist = DictAList({
            root: [
                {"name": "银魂 爱染香篇", "is_dir": True},
                {"name": "剧场版合集", "is_dir": True},
            ],
            oad_folder: [
                {"name": f"[Ygm] Gintama OAD 2016 [{number:02d}].mkv", "is_dir": False, "size": big}
                for number in (1, 2)
            ],
            numbered_folder: [
                {"name": f"0{number}. 俯瞰风景{number}.mkv", "is_dir": False, "size": big}
                for number in (1, 2, 3)
            ],
        })
        with tempfile.TemporaryDirectory() as directory:
            records = analyze_root_boundaries(
                alist, root, root_task_id="guarded", state_root=Path(directory),
            )
        self.assertEqual(
            {record.source_paths for record in records},
            {(oad_folder,), (numbered_folder,)},
        )

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


class BoundaryRebuildBeforeWriteTests(unittest.TestCase):
    """A parked, unwritten root may re-derive its boundary; anything else may not."""

    SOURCE = "/quark/影视/待刮削/Rebuild Show"
    BODY = f"{SOURCE}/Rebuild Show 4K 全02集"

    def _alist(self) -> DictAList:
        return DictAList({
            self.SOURCE: [
                {"name": "Rebuild Show 4K 全02集", "is_dir": True, "size": 0},
            ],
            self.BODY: [
                {"name": "Rebuild Show [01].mkv", "is_dir": False, "size": 1_400_000_000},
                {"name": "Rebuild Show [02].mkv", "is_dir": False, "size": 1_400_000_000},
                {"name": "剧场版", "is_dir": True, "size": 0},
            ],
            f"{self.BODY}/剧场版": [
                {
                    "name": "Rebuild Show the Movie Distant Shore [2160p].mkv",
                    "is_dir": False,
                    "size": 9_000_000_000,
                },
            ],
        })

    def _seed_stale_ledger(self, state_root: Path, root_task_id: str, **overrides: object):
        record = WorkUnitRecord(
            work_unit_id="stale-unit",
            root_task_id=root_task_id,
            boundary_key=self.SOURCE,
            source_paths=(self.SOURCE,),
            source_revision=1,
            role="single_work",
            display_label="Rebuild Show",
            media_context="tv",
            identity_status="confirmed",
            identity={"media_type": "tv", "tmdb_id": 555, "source": "operator_override"},
            reconciliation_outcome="uncertain",
            attention="纯方括号集号未能证明完整唯一正季",
        )
        record = replace(record, **overrides) if overrides else record
        save_work_unit_records(state_root, root_task_id, [record])
        return record

    def test_parked_unwritten_root_rebuilds_and_retires_the_old_ledger(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            state_root = Path(tmp)
            rid = "engine-rebuild"
            self._seed_stale_ledger(state_root, rid)
            records = rebuild_root_boundary_if_unwritten(
                self._alist(), self.SOURCE, root_task_id=rid, state_root=state_root,
            )
            self.assertIsNotNone(records)
            assert records is not None
            self.assertEqual(len(records), 2, msg=[r.display_label for r in records])
            self.assertEqual(
                {r.media_context for r in records}, {"tv", "movie"},
            )
            # 旧账本必须留证退役，而不是被悄悄覆盖
            retired = list(state_root.glob(f"work_units_{rid}.retired-*.json"))
            self.assertEqual(len(retired), 1)
            payload = json.loads(retired[0].read_text(encoding="utf-8"))
            self.assertEqual(payload["reason"], "boundary_rebuild_before_any_write")
            self.assertEqual(payload["records"][0]["work_unit_id"], "stale-unit")
            # 作用域已变，人工确认不得被新单元继承
            self.assertTrue(all(r.identity is None for r in records))
            self.assertTrue(all(r.identity_status == "pending" for r in records))

    def test_write_side_fact_refuses_the_rebuild(self) -> None:
        for field, value in (
            ("writer_job_id", "writer-1"),
            ("lane_status", "merge_done"),
            ("lane_detail", "已归档"),
            ("matched_work_root", "/quark/影视/番剧/Rebuild Show"),
            ("gap_status", "registered"),
            ("gap_detail", "S01E03 缺"),
            ("uncovered_tokens", ("S01E03",)),
            ("reconciliation_outcome", "new_work"),
        ):
            with self.subTest(field=field), tempfile.TemporaryDirectory() as tmp:
                state_root = Path(tmp)
                rid = "engine-written"
                self._seed_stale_ledger(state_root, rid, **{field: value})
                self.assertIsNone(rebuild_root_boundary_if_unwritten(
                    self._alist(), self.SOURCE,
                    root_task_id=rid, state_root=state_root,
                ))
                self.assertEqual(
                    [r.work_unit_id for r in load_work_unit_records(state_root, rid)],
                    ["stale-unit"],
                )

    def test_healthy_in_flight_ledger_is_left_untouched(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            state_root = Path(tmp)
            rid = "engine-healthy"
            self._seed_stale_ledger(
                state_root, rid, reconciliation_outcome=None, attention=None,
            )
            self.assertIsNone(rebuild_root_boundary_if_unwritten(
                self._alist(), self.SOURCE, root_task_id=rid, state_root=state_root,
            ))
            self.assertEqual(
                [r.work_unit_id for r in load_work_unit_records(state_root, rid)],
                ["stale-unit"],
            )

    def test_unchanged_boundary_keeps_the_operator_confirmation(self) -> None:
        """An identical scope reproduces the unit id, so the override survives."""
        with tempfile.TemporaryDirectory() as tmp:
            state_root = Path(tmp)
            rid = "engine-same"
            alist = DictAList({
                self.SOURCE: [
                    {"name": "Rebuild Show [01].mkv", "is_dir": False, "size": 1_400_000_000},
                ],
            })
            _snapshot, seeded = build_root_boundary_analysis(
                alist, self.SOURCE, root_task_id=rid,
            )
            self.assertEqual(len(seeded), 1)
            save_work_unit_records(state_root, rid, [replace(
                seeded[0],
                identity_status="confirmed",
                identity={"media_type": "tv", "tmdb_id": 777, "source": "operator_override"},
                reconciliation_outcome="uncertain",
            )])
            records = rebuild_root_boundary_if_unwritten(
                alist, self.SOURCE, root_task_id=rid, state_root=state_root,
            )
            self.assertIsNotNone(records)
            assert records is not None
            self.assertEqual(len(records), 1)
            self.assertEqual(records[0].identity_status, "confirmed")
            self.assertEqual((records[0].identity or {}).get("tmdb_id"), 777)

    def test_automatic_confirmation_is_re_derived_not_carried_over(self) -> None:
        """A rebuild exists to re-derive C; an auto confirm must not survive it.

        Carrying an automatic verdict across the rebuild would freeze the old
        conclusion behind the new split — the exact failure that kept a nested
        feature confirmed as its parent TV series after the boundary was fixed.
        """
        with tempfile.TemporaryDirectory() as tmp:
            state_root = Path(tmp)
            rid = "engine-auto"
            alist = DictAList({
                self.SOURCE: [
                    {"name": "Rebuild Show [01].mkv", "is_dir": False, "size": 1_400_000_000},
                ],
            })
            _snapshot, seeded = build_root_boundary_analysis(
                alist, self.SOURCE, root_task_id=rid,
            )
            save_work_unit_records(state_root, rid, [replace(
                seeded[0],
                identity_status="confirmed",
                identity={"media_type": "tv", "tmdb_id": 31911, "confidence": 1.0},
                reconciliation_outcome="uncertain",
            )])
            records = rebuild_root_boundary_if_unwritten(
                alist, self.SOURCE, root_task_id=rid, state_root=state_root,
            )
            self.assertIsNotNone(records)
            assert records is not None
            self.assertEqual(records[0].identity_status, "pending")
            self.assertIsNone(records[0].identity)


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
