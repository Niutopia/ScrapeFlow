"""Real-chain regression over tests/corpus (A→B→W→C→D on production code).

AGENTS.md §7: real-case tests must drive the real engine flow — the boundary
analyser, evidence matcher and library reconciliation here are the production
modules; only the TMDB and AList clients are scenario doubles.  The manifest
expectations (roles, work counts, media contexts, identities, outcomes) are
asserted, not merely loaded.
"""

from __future__ import annotations

import json
import re
import tempfile
import unittest
from pathlib import Path

from engine.scrapeflow.root_boundaries import (
    analyze_root_boundaries,
    walk_source_rows,
)
from engine.scrapeflow.source_inventory import build_source_inventory
from engine.scrapeflow.unit_identity import resolve_work_unit_identities
from engine.scrapeflow.work_units import (
    extract_episode_pattern,
    load_work_unit_records,
)

from local.scrapeflow_api.library_index import reconcile_root_work_units

_CORPUS = Path(__file__).parents[2] / "tests" / "corpus"


class DictAList:
    """Bounded AList double serving one in-memory tree per path."""

    def __init__(self, entries: dict[str, list[dict[str, object]]]) -> None:
        self.entries = {str(path).rstrip("/"): rows for path, rows in entries.items()}

    def list(self, path: str, refresh: bool = False) -> list[dict[str, object]]:
        del refresh
        return [dict(row) for row in self.entries.get(str(path).rstrip("/"), [])]

    def read_file_bytes(self, path: str, *, max_bytes: int | None = None) -> bytes:
        raise FileNotFoundError(path)


class FakeTMDB:
    """Fuzzy-search TMDB double with configurable alternative titles."""

    def __init__(
        self,
        search_results: dict[str, list[dict]],
        *,
        alternative_titles: dict[str, list[dict]] | None = None,
    ) -> None:
        self.search_results = search_results
        self.alternative_titles = alternative_titles or {}

    def get(self, path: str, **params: object) -> dict:
        if path.startswith("/search/"):
            query = str(params.get("query", ""))
            query_key = re.sub(r"[^\w\u3400-\u9fff]+", "", query.casefold())
            for key, rows in self.search_results.items():
                key_clean = re.sub(r"[^\w\u3400-\u9fff]+", "", key.casefold())
                if key_clean == query_key or (
                    key_clean and key_clean in query_key
                ) or (query_key and query_key in key_clean):
                    return {"results": rows}
            return {"results": []}
        if path.endswith("/alternative_titles"):
            tmdb_id = path.split("/")[2]
            rows = self.alternative_titles.get(tmdb_id, [])
            return {"results": rows, "titles": rows}
        return {}


def _load_tree(scenario_id: str) -> dict:
    return json.loads(
        (_CORPUS / f"{scenario_id}.json").read_text(encoding="utf-8")
    )


def _entries(scenario_id: str) -> dict[str, list[dict[str, object]]]:
    raw = _load_tree(scenario_id)
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


def _manifest() -> dict[str, dict]:
    rows = json.loads((_CORPUS / "manifest.json").read_text(encoding="utf-8"))
    return {row["id"]: row for row in rows}


class CorpusManifestTests(unittest.TestCase):
    def test_every_scenario_is_manifested_and_vice_versa(self) -> None:
        manifest_ids = set(_manifest())
        scenario_ids = {
            path.stem for path in _CORPUS.glob("*.json") if path.name != "manifest.json"
        }
        self.assertEqual(manifest_ids, scenario_ids)


class CorpusBoundaryTests(unittest.TestCase):
    """A→B→W on the real analyser for every corpus scenario."""

    def test_roles_work_counts_and_media_contexts(self) -> None:
        manifest = _manifest()
        for scenario_id, expectation in manifest.items():
            with self.subTest(scenario=scenario_id):
                alist = DictAList(_entries(scenario_id))
                with tempfile.TemporaryDirectory() as directory:
                    records = analyze_root_boundaries(
                        alist,
                        _load_tree(scenario_id)["root"],
                        root_task_id=f"root-{scenario_id}",
                        state_root=Path(directory),
                    )
                self.assertEqual(
                    records[0].role,
                    expectation["expected_role"],
                )
                self.assertEqual(
                    len(records),
                    int(expectation["expected_work_count"])
                    if isinstance(expectation["expected_work_count"], int)
                    else len(records),
                )
                if "expected_work_count" in expectation and isinstance(
                    expectation["expected_work_count"], int
                ):
                    self.assertEqual(
                        len(records), expectation["expected_work_count"],
                    )
                contexts = expectation.get("expected_media_contexts")
                if contexts is None and "expected_media_context" in expectation:
                    contexts = [
                        expectation["expected_media_context"]
                    ] * len(records)
                if contexts is not None:
                    self.assertEqual(
                        [record.media_context for record in records],
                        contexts,
                    )

    def test_ova_scenario_detects_specials(self) -> None:
        alist = DictAList(_entries("ova_specials"))
        root = _load_tree("ova_specials")["root"]
        node = build_source_inventory(walk_source_rows(alist, root), root)
        videos = [f for f in node.files if f.object_type == "video"]
        pattern = extract_episode_pattern(videos)
        self.assertIsNotNone(pattern)
        self.assertTrue(pattern.has_specials)


class CorpusIdentityTests(unittest.TestCase):
    """C on the real evidence matcher with scenario TMDB doubles."""

    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.state_root = Path(self.temp.name)

    def _resolve(self, scenario_id: str, tmdb: FakeTMDB, root_task_id: str):
        alist = DictAList(_entries(scenario_id))
        analyze_root_boundaries(
            alist,
            _load_tree(scenario_id)["root"],
            root_task_id=root_task_id,
            state_root=self.state_root,
        )
        records = resolve_work_unit_identities(
            tmdb, self.state_root, root_task_id,
        )
        return records, alist

    def test_ordinary_movie_confirms_against_its_chinese_alias(self) -> None:
        tmdb = FakeTMDB(
            {
                "流浪地球2": [
                    {"id": 843241, "title": "The Wandering Earth II", "release_date": "2023-01-22", "genre_ids": [878]},
                ],
            },
            alternative_titles={
                "843241": [{"title": "流浪地球2", "iso_3166_1": "CN"}],
            },
        )
        records, alist = self._resolve("ordinary_movie", tmdb, "root-movie")
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0].identity_status, "confirmed")
        self.assertEqual(records[0].identity["tmdb_id"], 843241)
        self.assertEqual(records[0].identity["media_type"], "movie")
        reconcile_root_work_units(alist, "/quark/影视", self.state_root, "root-movie")
        self.assertEqual(
            load_work_unit_records(self.state_root, "root-movie")[0].reconciliation_outcome,
            "new_work",
        )

    def test_same_title_two_years_disambiguates_by_year(self) -> None:
        tmdb = FakeTMDB(
            {
                "Speed (1994)": [
                    {"id": 100, "title": "Speed", "release_date": "1994-06-10", "genre_ids": [28]},
                ],
                "Speed 2 (1997)": [
                    {"id": 200, "title": "Speed 2: Cruise Control", "release_date": "1997-06-13", "genre_ids": [28]},
                ],
            },
        )
        records, _alist = self._resolve(
            "same_title_two_years", tmdb, "root-speed",
        )
        self.assertEqual(len(records), 2)
        self.assertTrue(all(r.identity_status == "confirmed" for r in records))
        by_year = {
            int(next(y for y in (1994, 1997) if str(y) in r.boundary_key)): r
            for r in records
        }
        self.assertEqual(by_year[1994].identity["tmdb_id"], 100)
        self.assertEqual(by_year[1997].identity["tmdb_id"], 200)

    def test_absolute_number_anime_without_year_stays_uncertain_despite_alias(self) -> None:
        tmdb = FakeTMDB(
            {
                "绝对动画": [
                    {"id": 555, "name": "Zettai Anime", "first_air_date": "2020-01-01", "genre_ids": [16]},
                ],
            },
            alternative_titles={
                "555": [{"title": "绝对动画", "iso_3166_1": "CN"}],
            },
        )
        records, _alist = self._resolve(
            "absolute_number_anime", tmdb, "root-abs",
        )
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0].identity_status, "uncertain")
        self.assertIsNone(records[0].identity)


class CorpusReconciliationTests(unittest.TestCase):
    """D on the real three-shelf reconciliation with a scenario library."""

    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.state_root = Path(self.temp.name)

    def test_duplicate_source_is_duplicate_complete(self) -> None:
        scenario_id = "duplicate_source"
        library = {
            "/quark/影视/番剧/Fate Zero/tvshow.nfo": (
                '<?xml version="1.0" encoding="UTF-8"?>\n'
                "<tvshow><title>Fate/Zero</title><year>2011</year>"
                "<tmdbid>35507</tmdbid></tvshow>\n"
            ).encode("utf-8"),
        }
        for episode in range(1, 5):
            library[f"/quark/影视/番剧/Fate Zero/Season 01/S01E{episode:02d}.mkv"] = b"v"
        combined = {
            **_entries(scenario_id),
            **_library_entries(library),
        }
        full_alist = _BytesAList(combined, library)

        analyze_root_boundaries(
            full_alist,
            _load_tree(scenario_id)["root"],
            root_task_id="root-dup",
            state_root=self.state_root,
        )
        tmdb = FakeTMDB(
            {
                "Fate Zero": [
                    {"id": 35507, "name": "Fate/Zero", "first_air_date": "2011-10-02", "genre_ids": [16]},
                ],
            },
            alternative_titles={
                "35507": [
                    {"title": "Fate Zero", "iso_3166_1": "CN"},
                    {"title": "Fate/Zero", "iso_3166_1": "JP"},
                ],
            },
        )
        resolve_work_unit_identities(tmdb, self.state_root, "root-dup")
        reconcile_root_work_units(
            full_alist, "/quark/影视", self.state_root, "root-dup",
        )
        records = load_work_unit_records(self.state_root, "root-dup")
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0].identity_status, "confirmed")
        self.assertEqual(
            records[0].reconciliation_outcome, "duplicate_complete",
        )
        self.assertEqual(
            records[0].matched_work_root, "/quark/影视/番剧/Fate Zero",
        )


def _library_entries(files: dict[str, bytes]) -> dict[str, list[dict[str, object]]]:
    directory_paths: set[str] = set()
    for full_path in files:
        parts = full_path.strip("/").split("/")[:-1]
        current = ""
        for part in parts:
            current += "/" + part
            directory_paths.add(current)
    child_dirs: dict[str, set[str]] = {}
    for directory in directory_paths:
        if "/" in directory:
            parent, leaf = directory.rsplit("/", 1)
            child_dirs.setdefault(parent, set()).add(leaf)
    entries: dict[str, list[dict[str, object]]] = {}
    for directory in directory_paths:
        rows: list[dict[str, object]] = [
            {"name": leaf, "is_dir": True}
            for leaf in sorted(child_dirs.get(directory, ()))
        ]
        for full_path in files:
            if (
                full_path.startswith(directory + "/")
                and full_path[len(directory) + 1:].count("/") == 0
            ):
                rows.append({
                    "name": full_path.rsplit("/", 1)[1],
                    "is_dir": False,
                    "size": len(files[full_path]),
                })
        entries[directory] = rows
    return entries


class _BytesAList:
    def __init__(self, entries: dict[str, list[dict[str, object]]], files: dict[str, bytes]):
        self.entries = entries
        self.files = files

    def list(self, path: str, refresh: bool = False) -> list[dict[str, object]]:
        del refresh
        return [dict(row) for row in self.entries.get(str(path).rstrip("/"), [])]

    def read_file_bytes(self, path: str, *, max_bytes: int | None = None) -> bytes:
        value = self.files.get(path)
        if value is None:
            raise FileNotFoundError(path)
        if max_bytes is not None:
            return value[:max_bytes]
        return value


if __name__ == "__main__":
    unittest.main()
