"""Tests for per-work-unit identity resolution (C/U) and durable overrides."""

from __future__ import annotations

import re
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path

from engine.scrapeflow.root_boundaries import analyze_root_boundaries
from engine.scrapeflow.unit_identity import (
    apply_work_unit_override,
    requeue_uncertain_work_units,
    resolve_work_unit_identities,
)
from engine.scrapeflow.work_units import load_work_unit_records, save_work_unit_records

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
            # A four-file pure ordinal run without the narrow CJK+year proof
            # remains parked; it does not block independently confirmed work.
            fate_zero = by_key["/quark/影视/待刮削/Fate系列/Fate Zero"]
            self.assertEqual(fate_zero.identity_status, "uncertain")
            self.assertIsNone(fate_zero.identity)
            # The other ambiguous sibling is likewise parked alone.
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

    def test_cjk_season_children_use_parent_and_representative_title_evidence(self) -> None:
        """A titled container must rescue bare ``第一季``/``第二季`` leaves.

        This is a real source-shape regression: the direct child names carry
        only structural season labels, while the parent and release filenames
        contain the work title.  A response for ``第二季`` alone deliberately
        exists, so the assertion proves C/U did not let that generic query
        select an unrelated TV result.
        """
        with tempfile.TemporaryDirectory() as directory:
            state_root = Path(directory)
            root_task_id = "root-seraph-cjk-seasons"
            _build_snapshot(
                "seraph_of_end_cjk_seasons", root_task_id, state_root,
            )
            tmdb = FakeTMDB(
                {
                    "终结的炽天使": [{
                        "id": 61945,
                        "name": "Seraph of the End",
                        "original_name": "Owari no Seraph",
                        "first_air_date": "2015-04-04",
                        "genre_ids": [16],
                    }],
                    "第二季": [{
                        "id": 280326,
                        "name": "汉语",
                        "original_name": "中国 第二季",
                        "first_air_date": "",
                        "genre_ids": [16],
                    }],
                },
                alternative_titles={
                    "61945": [{"title": "终结的炽天使", "iso_3166_1": "CN"}],
                },
            )
            records = resolve_work_unit_identities(tmdb, state_root, root_task_id)

            self.assertEqual(len(records), 2)
            self.assertTrue(all(record.identity_status == "confirmed" for record in records))
            self.assertEqual(
                {record.identity["tmdb_id"] for record in records if record.identity},
                {61945},
            )
            queries = [
                str(params.get("query"))
                for path, params in tmdb.calls
                if path == "/search/tv"
            ]
            self.assertIn("终结的炽天使", queries)
            self.assertIn("Seraph of the End：Vampire Reign", queries)
            self.assertIn("Seraph of the End：Battle in Nagoya", queries)

    def test_retry_rechecks_unwritten_auto_match_from_bare_season_query(self) -> None:
        """Retry must not preserve an old no-writer identity from ``第二季``."""
        with tempfile.TemporaryDirectory() as directory:
            state_root = Path(directory)
            root_task_id = "root-seraph-retry"
            _build_snapshot(
                "seraph_of_end_cjk_seasons", root_task_id, state_root,
            )
            initial = load_work_unit_records(state_root, root_task_id)
            second = next(record for record in initial if record.display_label == "第二季")
            stale = replace(
                second,
                identity_status="confirmed",
                identity={
                    "media_type": "tv",
                    "tmdb_id": 280326,
                    "title": "汉语",
                    "year": "未知年份",
                    "confidence": 0.75,
                    "decision_trace": {
                        "query": "第二季",
                        "matched_query_variant": "第二季",
                    },
                },
                candidate_identities=((
                    {"media_type": "tv", "tmdb_id": 280326, "status": "confirmed"}
                ),),
                reconciliation_outcome="new_work",
                reconciliation_evidence={"kind": "stale"},
                attention="旧 C 结果",
            )
            save_work_unit_records(
                state_root,
                root_task_id,
                [stale if record.work_unit_id == second.work_unit_id else record for record in initial],
            )

            requeued = requeue_uncertain_work_units(state_root, root_task_id)
            reopened = next(record for record in requeued if record.work_unit_id == second.work_unit_id)
            self.assertEqual(reopened.identity_status, "pending")
            self.assertIsNone(reopened.identity)
            self.assertIsNone(reopened.reconciliation_outcome)
            self.assertIsNone(reopened.reconciliation_evidence)
            self.assertIsNone(reopened.writer_job_id)

            tmdb = FakeTMDB(
                {
                    "终结的炽天使": [{
                        "id": 61945,
                        "name": "Seraph of the End",
                        "first_air_date": "2015-04-04",
                        "genre_ids": [16],
                    }],
                    "第二季": [{
                        "id": 280326,
                        "name": "汉语",
                        "first_air_date": "",
                        "genre_ids": [16],
                    }],
                },
                alternative_titles={
                    "61945": [{"title": "终结的炽天使", "iso_3166_1": "CN"}],
                },
            )
            resolved = resolve_work_unit_identities(tmdb, state_root, root_task_id)
            retried = next(record for record in resolved if record.work_unit_id == second.work_unit_id)
            self.assertEqual(retried.identity_status, "confirmed")
            self.assertEqual(retried.identity["tmdb_id"], 61945)
            self.assertIsNone(retried.writer_job_id)

            # A corrected match can retain the generic boundary label, but it
            # must not be reopened endlessly once a parent/representative
            # query, rather than the season label itself, earned the match.
            second_retry = requeue_uncertain_work_units(state_root, root_task_id)
            stable = next(record for record in second_retry if record.work_unit_id == second.work_unit_id)
            self.assertEqual(stable.identity_status, "confirmed")
            self.assertEqual(stable.identity["tmdb_id"], 61945)

    def test_retry_rechecks_unwritten_auto_match_from_parent_escalation(self) -> None:
        """Retry must re-open an ancestor-escalated confirmation.

        A total-miss parent escalation can auto-confirm from a
        franchise-bundle ancestor label alone (``Fate系列`` -> ``Fate`` ->
        Fate/Apocrypha for a Prisma☆Illya movie leaf).  The decision trace
        marks that provenance with ``matched_query_via_parent_escalation``;
        an explicit retry must re-run C under the current matcher instead of
        trusting recovery-grade ancestor evidence.  A confirmation whose
        earning query was the unit's own boundary label is not reopened.
        """
        with tempfile.TemporaryDirectory() as directory:
            state_root = Path(directory)
            root_task_id = "root-fate-escalation"
            _build_snapshot("fate_container", root_task_id, state_root)
            initial = load_work_unit_records(state_root, root_task_id)
            escalated_unit = next(
                record for record in initial if record.display_label == "Fate Zero"
            )
            boundary_unit = next(
                record for record in initial if record.display_label == "空之境界"
            )
            escalated = replace(
                escalated_unit,
                identity_status="confirmed",
                identity={
                    "media_type": "tv",
                    "tmdb_id": 72304,
                    "title": "命运／外典",
                    "year": "2017",
                    "confidence": 0.96,
                    "decision_trace": {
                        "query": escalated_unit.display_label,
                        "matched_query_variant": "Fate",
                        "matched_query_via_parent_escalation": True,
                    },
                },
                candidate_identities=((
                    {"media_type": "tv", "tmdb_id": 72304, "status": "confirmed"}
                ),),
                reconciliation_outcome=None,
                attention=None,
            )
            boundary_confirmed = replace(
                boundary_unit,
                identity_status="confirmed",
                identity={
                    "media_type": "movie",
                    "tmdb_id": 40981,
                    "title": "空之境界",
                    "year": "2007",
                    "confidence": 0.9,
                    "decision_trace": {
                        "query": boundary_unit.display_label,
                        "matched_query_variant": "空之境界",
                    },
                },
                candidate_identities=((
                    {"media_type": "movie", "tmdb_id": 40981, "status": "confirmed"}
                ),),
                reconciliation_outcome=None,
                attention=None,
            )
            save_work_unit_records(state_root, root_task_id, [
                escalated if record.work_unit_id == escalated_unit.work_unit_id
                else boundary_confirmed if record.work_unit_id == boundary_unit.work_unit_id
                else record
                for record in initial
            ])

            requeued = requeue_uncertain_work_units(state_root, root_task_id)
            reopened = next(
                record for record in requeued
                if record.work_unit_id == escalated_unit.work_unit_id
            )
            self.assertEqual(reopened.identity_status, "pending")
            self.assertIsNone(reopened.identity)
            stable = next(
                record for record in requeued
                if record.work_unit_id == boundary_unit.work_unit_id
            )
            self.assertEqual(stable.identity_status, "confirmed")
            self.assertEqual(stable.identity["tmdb_id"], 40981)

    def test_bare_season_without_parent_or_title_evidence_stays_uncertain(self) -> None:
        """A season coordinate alone cannot become a TMDB identity query."""
        root = "/incoming/Season 02"
        entries = {
            root: [
                {"name": "S02E01.mkv", "is_dir": False, "size": 10},
                {"name": "S02E02.mkv", "is_dir": False, "size": 10},
            ],
        }
        with tempfile.TemporaryDirectory() as directory:
            state_root = Path(directory)
            analyze_root_boundaries(
                DictAList(entries), root, root_task_id="root-bare-season", state_root=state_root,
            )
            tmdb = FakeTMDB({
                "Season 02": [{
                    "id": 280326,
                    "name": "Unrelated Season Two",
                    "first_air_date": "2020-01-01",
                    "genre_ids": [16],
                }],
            })
            records = resolve_work_unit_identities(
                tmdb, state_root, "root-bare-season",
            )

        self.assertEqual(records[0].identity_status, "uncertain")
        self.assertIsNone(records[0].identity)
        self.assertEqual(
            [path for path, _params in tmdb.calls if path.startswith("/search/")],
            [],
        )

    def test_noisy_root_uses_nested_titled_episode_evidence_after_root_files(self) -> None:
        """Nested titled episodes must not be hidden by root-level ``SxxExx`` files.

        A single TV work can carry a newly released season directly at its
        root while older seasons remain in folders.  The root files establish
        season structure but do not themselves contain a title, so identity
        resolution must also retain a bounded representative from the nested
        exact source tree.  The TMDB double deliberately recognizes only that
        nested title; no package or work name is an identity override.
        """
        root = "/incoming/发布包-未校验"
        entries: dict[str, list[dict[str, object]]] = {
            root: [
                *[
                    {"name": f"S09E{episode:02d}.mkv", "is_dir": False, "size": 10}
                    for episode in range(1, 11)
                ],
                {"name": "Season 01", "is_dir": True},
                {"name": "Season 02", "is_dir": True},
            ],
            f"{root}/Season 01": [
                {"name": "Northwind.Show.S01E01.1080p.mkv", "is_dir": False, "size": 10},
                {"name": "Northwind.Show.S01E02.1080p.mkv", "is_dir": False, "size": 10},
            ],
            f"{root}/Season 02": [
                {"name": "Northwind.Show.S02E01.1080p.mkv", "is_dir": False, "size": 10},
            ],
        }
        with tempfile.TemporaryDirectory() as directory:
            state_root = Path(directory)
            analyze_root_boundaries(
                DictAList(entries), root, root_task_id="root-nested-title", state_root=state_root,
            )
            tmdb = FakeTMDB({
                "Northwind Show": [{
                    "id": 90210,
                    "name": "Northwind Show",
                    "first_air_date": "2015-01-01",
                    "genre_ids": [18],
                }],
            })
            records = resolve_work_unit_identities(tmdb, state_root, "root-nested-title")

        self.assertEqual(len(records), 1)
        self.assertEqual(records[0].identity_status, "confirmed")
        self.assertEqual(records[0].identity["tmdb_id"], 90210)
        search_queries = [
            str(params.get("query"))
            for path, params in tmdb.calls
            if path == "/search/tv"
        ]
        self.assertIn("Northwind.Show", search_queries)

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

    def test_disc_image_cannot_be_resolved_overridden_or_requeued_as_media(self) -> None:
        root = "/incoming/opaque-disc"
        alist = DictAList({
            root: [{"name": "Season 01", "is_dir": True}],
            f"{root}/Season 01": [
                {"name": "Episode collection.iso", "is_dir": False, "size": 45 * 1024**3},
            ],
        })
        with tempfile.TemporaryDirectory() as directory:
            state_root = Path(directory)
            records = analyze_root_boundaries(
                alist, root, root_task_id="root-disc-id", state_root=state_root,
            )
            self.assertEqual(len(records), 1)
            record = records[0]
            self.assertTrue(record.requires_content_expansion)

            # Simulate a pre-policy persisted confirmation.  C/U must inspect
            # the exact B scope rather than trust the old status.
            stale_confirmed = replace(
                record,
                requires_content_expansion=False,
                identity_status="confirmed",
                identity={"media_type": "tv", "tmdb_id": 123},
                reconciliation_outcome="new_work",
                attention=None,
            )
            save_work_unit_records(state_root, "root-disc-id", [stale_confirmed])
            tmdb = FakeTMDB({})
            resolved = resolve_work_unit_identities(tmdb, state_root, "root-disc-id")
            parked = resolved[0]
            self.assertTrue(parked.requires_content_expansion)
            self.assertEqual(parked.identity_status, "uncertain")
            self.assertIsNone(parked.identity)
            self.assertIsNone(parked.reconciliation_outcome)
            self.assertEqual(tmdb.calls, [])

            with self.assertRaisesRegex(ValueError, "光盘镜像"):
                apply_work_unit_override(
                    state_root,
                    "root-disc-id",
                    parked.work_unit_id,
                    media_type="tv",
                    tmdb_id=123,
                )
            retried = requeue_uncertain_work_units(state_root, "root-disc-id")
            self.assertEqual(retried[0].identity_status, "uncertain")
            self.assertTrue(retried[0].requires_content_expansion)

    def test_explicit_requeue_reopens_only_uncertain_identity_and_reconciliation(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            state_root = Path(directory)
            root_task_id = "root-fate"
            _build_snapshot("fate_container", root_task_id, state_root)
            records = load_work_unit_records(state_root, root_task_id)
            self.assertGreaterEqual(len(records), 3)
            records[0] = replace(
                records[0],
                identity_status="uncertain",
                identity=None,
                candidate_identities=({"tmdb_id": 1},),
                reconciliation_outcome=None,
                attention="identity evidence needs retry",
            )
            records[1] = replace(
                records[1],
                identity_status="confirmed",
                identity={"media_type": "tv", "tmdb_id": 35507},
                reconciliation_outcome="uncertain",
                matched_work_root=None,
                attention="library index needs retry",
            )
            durable = replace(
                records[2],
                identity_status="confirmed",
                identity={
                    "media_type": "tv",
                    "tmdb_id": 201,
                    "source": "operator_override",
                },
                reconciliation_outcome="new_work",
                writer_job_id="writer-complete",
                attention=None,
            )
            records[2] = durable
            save_work_unit_records(state_root, root_task_id, records)

            reopened = requeue_uncertain_work_units(state_root, root_task_id)

            self.assertEqual(reopened[0].identity_status, "pending")
            self.assertIsNone(reopened[0].identity)
            self.assertEqual(reopened[0].candidate_identities, ())
            self.assertIsNone(reopened[0].attention)
            self.assertEqual(reopened[1].identity_status, "confirmed")
            self.assertEqual(reopened[1].identity["tmdb_id"], 35507)
            self.assertIsNone(reopened[1].reconciliation_outcome)
            self.assertIsNone(reopened[1].attention)
            self.assertEqual(reopened[2], durable)

    def test_explicit_requeue_retries_parked_j_without_discarding_writer(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            state_root = Path(directory)
            root_task_id = "root-j-retry"
            _build_snapshot("fate_container", root_task_id, state_root)
            records = load_work_unit_records(state_root, root_task_id)
            parked = replace(
                records[0],
                identity_status="confirmed",
                identity={"media_type": "tv", "tmdb_id": 35507},
                reconciliation_outcome="new_work",
                writer_job_id="writer-already-accepted",
                gap_status="attention",
                gap_detail="TMDB 季集目录不可用",
                attention="写后缺口无法核对，需要确认",
            )
            records[0] = parked
            save_work_unit_records(state_root, root_task_id, records)

            reopened = requeue_uncertain_work_units(state_root, root_task_id)

            self.assertEqual(reopened[0].identity, parked.identity)
            self.assertEqual(reopened[0].reconciliation_outcome, "new_work")
            self.assertEqual(reopened[0].writer_job_id, "writer-already-accepted")
            self.assertIsNone(reopened[0].gap_status)
            self.assertIsNone(reopened[0].gap_detail)
            self.assertIsNone(reopened[0].attention)

    def test_retry_reopens_automatic_undated_movie_confirmation(self) -> None:
        """A pre-guard automatic junk-movie confirmation re-runs C on retry.

        The matcher now refuses movie rows TMDB never dated or timed.  An
        older automatic confirmation whose accepted identity is an undated
        movie was accepted from exactly that junk shape, so an explicit
        retry must re-resolve it.  The writer carrier is deliberately kept:
        once C re-resolves, G retires the superseded carrier through the
        identity-mismatch rule.
        """
        with tempfile.TemporaryDirectory() as directory:
            state_root = Path(directory)
            root_task_id = "root-junk-movie-retry"
            _build_snapshot("fate_container", root_task_id, state_root)
            records = load_work_unit_records(state_root, root_task_id)
            self.assertGreaterEqual(len(records), 3)
            records[0] = replace(
                records[0],
                identity_status="confirmed",
                identity={
                    "media_type": "movie",
                    "tmdb_id": 1587181,
                    "title": "某剧:完结篇",
                    "year": "未知年份",
                    "confidence": 0.98,
                    "decision_trace": {"query": "完结篇"},
                },
                reconciliation_outcome="new_work",
                writer_job_id="writer-junk-movie",
            )
            dated = replace(
                records[1],
                identity_status="confirmed",
                identity={
                    "media_type": "movie",
                    "tmdb_id": 447,
                    "title": "A Real Film",
                    "year": "2009",
                    "confidence": 0.95,
                },
                reconciliation_outcome="new_work",
                writer_job_id="writer-dated",
            )
            records[1] = dated
            override = replace(
                records[2],
                identity_status="confirmed",
                identity={
                    "media_type": "movie",
                    "tmdb_id": 997,
                    "title": None,
                    "year": "未知年份",
                    "source": "operator_override",
                },
                reconciliation_outcome="new_work",
                writer_job_id="writer-override",
            )
            records[2] = override
            save_work_unit_records(state_root, root_task_id, records)

            reopened = requeue_uncertain_work_units(state_root, root_task_id)

            self.assertEqual(reopened[0].identity_status, "pending")
            self.assertIsNone(reopened[0].identity)
            self.assertIsNone(reopened[0].reconciliation_outcome)
            self.assertEqual(reopened[0].writer_job_id, "writer-junk-movie")
            # A dated movie and an operator's explicit override stay durable.
            self.assertEqual(reopened[1], dated)
            self.assertEqual(reopened[2], override)


if __name__ == "__main__":
    unittest.main()
