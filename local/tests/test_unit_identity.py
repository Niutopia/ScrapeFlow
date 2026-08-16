"""Tests for per-work-unit identity resolution (C/U) and durable overrides."""

from __future__ import annotations

import re
import tempfile
import unittest
from pathlib import Path

from engine.scrapeflow.root_boundaries import analyze_root_boundaries
from engine.scrapeflow.unit_identity import (
    apply_work_unit_override,
    resolve_work_unit_identities,
)
from engine.scrapeflow.work_units import load_work_unit_records

from local.tests.test_root_boundaries import DictAList, _entries_from_fixture


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
        self.calls: list[tuple[str, dict]] = []

    def get(self, path: str, **params: object) -> dict:
        self.calls.append((path, dict(params)))
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


def _build_snapshot(case_id: str, root_task_id: str, state_root: Path):
    alist = DictAList(_entries_from_fixture(case_id))
    import json
    root = json.loads(
        (Path(__file__).parent / "fixtures" / "media_cases" / case_id / "source_tree.json")
        .read_text(encoding="utf-8")
    )["root"]
    analyze_root_boundaries(alist, root, root_task_id=root_task_id, state_root=state_root)
    return root


class WorkUnitIdentityTests(unittest.TestCase):
    def test_sibling_units_resolve_independently(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            state_root = Path(directory)
            root_task_id = "root-fate"
            _build_snapshot("fate_container", root_task_id, state_root)
            tmdb = FakeTMDB(
                {
                    "空之境界": [
                        {"id": 100, "name": "Kara no Kyoukai", "first_air_date": "2007-12-01", "genre_ids": [16]},
                    ],
                    "Fate Stay Night UBW": [
                        {"id": 201, "name": "Fate/stay night: Unlimited Blade Works", "first_air_date": "2014-10-04", "genre_ids": [16]},
                        {"id": 202, "name": "Fate/stay night: Unlimited Blade Works", "first_air_date": "2015-01-01", "genre_ids": [16]},
                    ],
                    "Fate Zero": [
                        {"id": 35507, "name": "Fate/Zero", "first_air_date": "2011-10-02", "genre_ids": [16]},
                    ],
                },
                alternative_titles={
                    "100": [{"title": "空之境界", "iso_3166_1": "CN"}],
                },
            )
            records = resolve_work_unit_identities(tmdb, state_root, root_task_id)
            by_key = {record.boundary_key: record for record in records}
            self.assertEqual(by_key["/quark/影视/待刮削/Fate系列/空之境界"].identity_status, "confirmed")
            self.assertEqual(
                by_key["/quark/影视/待刮削/Fate系列/空之境界"].identity["tmdb_id"], 100,
            )
            self.assertEqual(by_key["/quark/影视/待刮削/Fate系列/Fate Zero"].identity_status, "confirmed")
            self.assertEqual(
                by_key["/quark/影视/待刮削/Fate系列/Fate Zero"].identity["tmdb_id"], 35507,
            )
            # The ambiguous sibling is parked alone; the others proceed.
            ubw = by_key["/quark/影视/待刮削/Fate系列/Fate Stay Night UBW"]
            self.assertEqual(ubw.identity_status, "uncertain")
            self.assertIsNone(ubw.identity)
            self.assertEqual(len(ubw.candidate_identities), 2)
            # The ledger is durable and reloadable.
            self.assertEqual(len(load_work_unit_records(state_root, root_task_id)), 3)

    def test_resolve_is_idempotent_and_keeps_operator_overrides(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            state_root = Path(directory)
            root_task_id = "root-fate"
            _build_snapshot("fate_container", root_task_id, state_root)
            tmdb = FakeTMDB(
                {
                    "Fate Zero": [
                        {"id": 35507, "name": "Fate/Zero", "first_air_date": "2011-10-02", "genre_ids": [16]},
                    ],
                    "Fate Stay Night UBW": [
                        {"id": 201, "name": "Fate/stay night: Unlimited Blade Works", "first_air_date": "2014-10-04", "genre_ids": [16]},
                        {"id": 202, "name": "Fate/stay night: Unlimited Blade Works", "first_air_date": "2015-01-01", "genre_ids": [16]},
                    ],
                },
            )
            first = resolve_work_unit_identities(tmdb, state_root, root_task_id)
            ubw = next(r for r in first if "UBW" in r.boundary_key)
            calls_before = len(tmdb.calls)
            overridden = apply_work_unit_override(
                state_root, root_task_id, ubw.work_unit_id,
                media_type="tv", tmdb_id=201, season=1,
            )
            self.assertEqual(overridden.identity_status, "confirmed")
            self.assertEqual(overridden.identity["source"], "operator_override")
            self.assertEqual(overridden.identity["season"], 1)
            # A later resolve pass must not re-match or downgrade the override.
            second = resolve_work_unit_identities(tmdb, state_root, root_task_id)
            reloaded = next(r for r in second if r.work_unit_id == ubw.work_unit_id)
            self.assertEqual(reloaded.identity_status, "confirmed")
            self.assertEqual(reloaded.identity["source"], "operator_override")
            self.assertEqual(reloaded.identity["tmdb_id"], 201)
            self.assertEqual(len(tmdb.calls), calls_before)

    def test_single_work_uses_empty_parent_labels_and_confirms(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            state_root = Path(directory)
            root_task_id = "root-movie"
            _build_snapshot("ordinary_movie", root_task_id, state_root)
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
            records = resolve_work_unit_identities(tmdb, state_root, root_task_id)
            self.assertEqual(len(records), 1)
            record = records[0]
            self.assertEqual(record.identity_status, "confirmed")
            self.assertEqual(record.identity["tmdb_id"], 843241)
            self.assertEqual(record.identity["media_type"], "movie")

    def test_override_validates_the_confirmation_surface(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            state_root = Path(directory)
            root_task_id = "root-fate"
            _build_snapshot("fate_container", root_task_id, state_root)
            records = load_work_unit_records(state_root, root_task_id)
            unit_id = records[0].work_unit_id
            with self.assertRaises(ValueError):
                apply_work_unit_override(
                    state_root, root_task_id, unit_id, media_type="ova", tmdb_id=1,
                )
            for bad_id in (0, -1, True, "5"):
                with self.assertRaises(ValueError):
                    apply_work_unit_override(
                        state_root, root_task_id, unit_id, media_type="tv", tmdb_id=bad_id,
                    )
            with self.assertRaises(ValueError):
                apply_work_unit_override(
                    state_root, root_task_id, unit_id, media_type="tv", tmdb_id=1, season=0,
                )
            with self.assertRaises(KeyError):
                apply_work_unit_override(
                    state_root, root_task_id, "missing-unit", media_type="tv", tmdb_id=1,
                )


if __name__ == "__main__":
    unittest.main()
