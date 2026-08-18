"""Tests for F/G/H composition: unit-driven planning, single-writer execution,
and typed acceptance."""

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from engine.scrapeflow.errors import PlanError
from engine.scrapeflow.models import Plan, PlannedFile
from engine.scrapeflow.root_boundaries import analyze_root_boundaries
from engine.scrapeflow.unit_identity import apply_work_unit_override
from engine.scrapeflow.work_units import load_work_unit_records

from local.scrapeflow_api.library_index import reconcile_root_work_units
from local.scrapeflow_api.simple_engine_runner import (
    EnginePauseRequested,
    SimpleEngineRunner,
)
from local.scrapeflow_api.unit_execution import (
    _register_unit_episode_gaps,
    _request_for_unit,
    execute_new_work_units,
    load_work_acceptance,
)

from local.tests.test_library_index import IndexAList
from local.tests.test_simple_engine_runner import FAKE_VIDEO_BYTES, FAKE_VIDEO_SIZE


def _recording_planner(events: list[dict], *, fail_for: str | None = None):
    def planner(request, _alist, _tmdb) -> Plan:
        events.append({
            "source_path": request.source_path,
            "parent_path": request.parent_path,
            "media_type": request.media_type,
            "tmdb_id": request.tmdb_id,
        })
        if fail_for is not None and request.source_path == fail_for:
            raise PlanError("injected planner failure")
        target = f"{request.parent_path.rstrip('/')}/Work ({request.tmdb_id})"
        return Plan(
            mode="tv" if request.media_type == "tv" else "movie",
            source_root=request.source_path,
            target_root=target,
            files=[PlannedFile(
                source_path=f"{request.source_path}/S01E01.mkv",
                source_dir=request.source_path,
                original_name="S01E01.mkv",
                final_name="S01E01.mkv",
                target_dir=target,
                media_kind="video",
                source_size=FAKE_VIDEO_SIZE,
            )],
            warnings=[],
            metadata={
                "tmdb_id": request.tmdb_id,
                "title": "Work",
                "year": "2020",
                "poster_path": None,
                "backdrop_path": None,
            },
        )

    return planner


class CatalogTMDB:
    """Minimal TMDB double for the episode catalog used by gap discovery."""

    def __init__(self, tmdb_id: int, episodes: int) -> None:
        self.tmdb_id = tmdb_id
        self.episodes = episodes

    def get(self, path: str, **params: object) -> dict:
        del params
        if path == f"/tv/{self.tmdb_id}":
            return {"seasons": [{"season_number": 1, "name": "Season 1"}]}
        if path == f"/tv/{self.tmdb_id}/season/1":
            return {
                "episodes": [
                    {"episode_number": number, "air_date": "2020-01-01"}
                    for number in range(1, self.episodes + 1)
                ],
            }
        return {}


class MultiSeasonTMDB:
    """TMDB double with several positive seasons (S1 25/S2 24/S3 24/S4 23)."""

    def __init__(self, tmdb_id: int, seasons: dict[int, int]) -> None:
        self.tmdb_id = tmdb_id
        self.seasons = seasons

    def get(self, path: str, **params: object) -> dict:
        del params
        if path == f"/tv/{self.tmdb_id}":
            return {
                "seasons": [
                    {"season_number": season, "name": f"Season {season}"}
                    for season in self.seasons
                ],
            }
        for season, count in self.seasons.items():
            if path == f"/tv/{self.tmdb_id}/season/{season}":
                return {
                    "episodes": [
                        {"episode_number": number, "air_date": "2020-01-01"}
                        for number in range(1, count + 1)
                    ],
                }
        return {}


class UnitExecutionTests(unittest.TestCase):
    def _setup(self, files: dict[str, bytes], *, library_files: dict[str, bytes] | None = None, tmdb: object | None = None):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        state_root = Path(temp.name)
        all_files = dict(files)
        library = library_files or {}
        alist = IndexAList({**all_files, **library})
        planner_events: list[dict] = []
        executor_events: list[str] = []
        runner = SimpleEngineRunner(
            state_root,
            alist=alist,
            tmdb=tmdb if tmdb is not None else object(),
            planner=_recording_planner(planner_events),
            validate=False,
            library_root="/library",
            executor=lambda plan: (
                executor_events.append(str(plan.target_root)) or {"ok": True}
            ),
        )
        return state_root, alist, runner, planner_events, executor_events

    def _prepare_two_new_work_units(
        self,
        files: dict[str, bytes],
        runner: SimpleEngineRunner,
        alist: IndexAList,
        state_root: Path,
        root_task_id: str,
    ) -> None:
        pending = runner.create_pending_job("/incoming/two", job_id=root_task_id)
        runner.start_automatic_job(pending.id, target_shelf="anime")
        analyze_root_boundaries(
            alist, "/incoming/two", root_task_id=root_task_id, state_root=state_root,
        )
        records = load_work_unit_records(state_root, root_task_id)
        self.assertEqual(len(records), 2)
        for index, record in enumerate(records):
            apply_work_unit_override(
                state_root, root_task_id, record.work_unit_id,
                media_type="tv", tmdb_id=101 + index,
            )

    def test_new_work_units_plan_and_execute_through_the_carrier(self) -> None:
        files = {
            "/incoming/two/Fate Zero/S01E01.mkv": FAKE_VIDEO_BYTES,
            "/incoming/two/Another Show/S01E01.mkv": FAKE_VIDEO_BYTES,
        }
        state_root, alist, runner, planner_events, executor_events = self._setup(files)
        root_task_id = "root-1"
        self._prepare_two_new_work_units(files, runner, alist, state_root, root_task_id)
        reconcile_root_work_units(alist, "/library", state_root, root_task_id)
        for record in load_work_unit_records(state_root, root_task_id):
            self.assertEqual(record.reconciliation_outcome, "new_work")

        results = execute_new_work_units(runner, state_root, root_task_id)
        self.assertEqual([result.outcome for result in results], ["accepted", "accepted"])
        self.assertEqual(len(planner_events), 2)
        for event in planner_events:
            # Two distinct TV identities in one intake root follow the
            # Fate-style container rule: everything nests under one folder
            # named after the cleaned intake directory.
            self.assertEqual(event["parent_path"], "/library/番剧/two")
            self.assertEqual(event["media_type"], "tv")
        self.assertIn(101, {event["tmdb_id"] for event in planner_events})
        self.assertIn(102, {event["tmdb_id"] for event in planner_events})
        self.assertEqual(len(executor_events), 2)
        for event in executor_events:
            self.assertIn("/library/番剧/", event)
        records = load_work_unit_records(state_root, root_task_id)
        self.assertTrue(all(record.writer_job_id for record in records))
        persisted = load_work_acceptance(state_root, root_task_id)
        self.assertEqual(len(persisted), 2)
        self.assertTrue(all(result.outcome == "accepted" for result in persisted))
        self.assertTrue(all(result.planned_files == 1 for result in persisted))

    def test_retry_skips_executed_units_and_retries_failures(self) -> None:
        files = {
            "/incoming/two/Fate Zero/S01E01.mkv": FAKE_VIDEO_BYTES,
            "/incoming/two/Broken Show/S01E01.mkv": FAKE_VIDEO_BYTES,
        }
        state_root, alist, runner, _planner_events, executor_events = self._setup(files)
        root_task_id = "root-2"
        self._prepare_two_new_work_units(files, runner, alist, state_root, root_task_id)
        reconcile_root_work_units(alist, "/library", state_root, root_task_id)
        # Fail the planner for the second unit on the first pass.
        retry_events: list[dict] = []
        runner.planner = _recording_planner(
            retry_events, fail_for="/incoming/two/Broken Show",
        )
        first = execute_new_work_units(runner, state_root, root_task_id)
        by_outcome = {result.outcome for result in first}
        self.assertEqual(by_outcome, {"accepted", "failed"})
        failed = next(result for result in first if result.outcome == "failed")
        self.assertIsNone(failed.writer_job_id)
        self.assertIsNotNone(failed.error)
        records = load_work_unit_records(state_root, root_task_id)
        written = {record.boundary_key for record in records if record.writer_job_id}
        self.assertEqual(written, {"/incoming/two/Fate Zero"})
        # A second pass retries only the failed unit.
        retry_events.clear()
        runner.planner = _recording_planner(retry_events)
        second = execute_new_work_units(runner, state_root, root_task_id)
        self.assertTrue(all(result.outcome == "accepted" for result in second))
        self.assertEqual(len(retry_events), 1)
        self.assertEqual(retry_events[0]["source_path"], "/incoming/two/Broken Show")
        self.assertEqual(len(executor_events), 2)

    def test_non_new_work_units_are_skipped_without_writes(self) -> None:
        files = {
            "/incoming/two/Fate Zero/S01E01.mkv": FAKE_VIDEO_BYTES,
            "/incoming/two/Another Show/S01E01.mkv": FAKE_VIDEO_BYTES,
        }
        library = {
            "/library/番剧/Fate Zero/tvshow.nfo": (
                b'<?xml version="1.0" encoding="UTF-8"?>\n'
                b"<tvshow><title>Fate/Zero</title><year>2011</year>"
                b"<tmdbid>101</tmdbid></tvshow>\n"
            ),
            "/library/番剧/Fate Zero/Season 01/S01E01.mkv": b"v",
        }
        state_root, alist, runner, planner_events, executor_events = self._setup(
            files, library_files=library,
        )
        root_task_id = "root-3"
        self._prepare_two_new_work_units(files, runner, alist, state_root, root_task_id)
        # Fate Zero (tmdb 101) now exists in 番剧; its unit must flip to
        # duplicate/merge and stay out of the new-work writer.
        reconcile_root_work_units(alist, "/library", state_root, root_task_id)
        records = load_work_unit_records(state_root, root_task_id)
        by_tmdb = {record.identity["tmdb_id"]: record for record in records}
        self.assertNotEqual(by_tmdb[101].reconciliation_outcome, "new_work")
        self.assertEqual(by_tmdb[102].reconciliation_outcome, "new_work")

        results = execute_new_work_units(runner, state_root, root_task_id)
        self.assertEqual(
            {result.outcome for result in results},
            {"accepted", "skipped"},
        )
        self.assertEqual(len(planner_events), 1)
        self.assertEqual(len(executor_events), 1)

    def test_root_scope_pause_after_plan_blocks_unit_formal_writer(self) -> None:
        """F/G/H must pass the root predicate into the child executor."""
        files = {"/incoming/one/My Show/S01E01.mkv": FAKE_VIDEO_BYTES}
        state_root, alist, _runner, planner_events, executor_events = self._setup(files)
        root_task_id = "root-pause-after-plan"
        paused = {"value": False}

        class PauseAfterPlanRunner(SimpleEngineRunner):
            def plan_job(self, *args, **kwargs):
                planned = super().plan_job(*args, **kwargs)
                paused["value"] = True
                return planned

        runner = PauseAfterPlanRunner(
            state_root,
            alist=alist,
            tmdb=object(),
            planner=_recording_planner(planner_events),
            validate=False,
            library_root="/library",
            executor=lambda plan: (
                executor_events.append(str(plan.target_root)) or {"ok": True}
            ),
        )
        pending = runner.create_pending_job("/incoming/one", job_id=root_task_id)
        runner.start_automatic_job(pending.id, target_shelf="anime")
        analyze_root_boundaries(
            alist, "/incoming/one", root_task_id=root_task_id, state_root=state_root,
        )
        record = load_work_unit_records(state_root, root_task_id)[0]
        apply_work_unit_override(
            state_root,
            root_task_id,
            record.work_unit_id,
            media_type="tv",
            tmdb_id=101,
        )
        reconcile_root_work_units(alist, "/library", state_root, root_task_id)

        with self.assertRaises(EnginePauseRequested):
            execute_new_work_units(
                runner,
                state_root,
                root_task_id,
                pause_requested=lambda: paused["value"],
            )

        self.assertEqual(executor_events, [])
        record = load_work_unit_records(state_root, root_task_id)[0]
        self.assertIsNotNone(record.writer_job_id)
        self.assertEqual(runner.get_job(record.writer_job_id).phase, "planned")
        self.assertFalse(
            any(row.outcome == "failed" for row in load_work_acceptance(state_root, root_task_id)),
        )


    def test_accepted_tv_unit_registers_precise_episode_gaps(self) -> None:
        files = {"/incoming/one/Fate Zero/S01E01.mkv": FAKE_VIDEO_BYTES}
        state_root, alist, runner, _planner_events, _executor_events = self._setup(
            files, tmdb=CatalogTMDB(101, 10),
        )
        root_task_id = "root-gaps"
        pending = runner.create_pending_job("/incoming/one", job_id=root_task_id)
        runner.start_automatic_job(pending.id, target_shelf="anime")
        analyze_root_boundaries(
            alist, "/incoming/one", root_task_id=root_task_id, state_root=state_root,
        )
        records = load_work_unit_records(state_root, root_task_id)
        self.assertEqual(len(records), 1)
        apply_work_unit_override(
            state_root, root_task_id, records[0].work_unit_id,
            media_type="tv", tmdb_id=101,
        )
        reconcile_root_work_units(alist, "/library", state_root, root_task_id)
        results = execute_new_work_units(runner, state_root, root_task_id)
        self.assertEqual(results[0].outcome, "accepted")
        from engine.scrapeflow.gap_ledger import load_gap_ledger

        gaps = load_gap_ledger(state_root, root_task_id)
        # Official catalog E01..E10, plan wrote only E01 -> 9 open gaps.
        self.assertEqual(len(gaps), 9)
        self.assertTrue(
            all(gap.work_unit_id == records[0].work_unit_id for gap in gaps),
        )
        self.assertEqual(
            {gap.gap_id.rsplit("::", 1)[1] for gap in gaps},
            {f"S01E{episode:02d}" for episode in range(2, 11)},
        )

    def _record_for(self, state_root: Path, root_task_id: str, tmdb_id: int):
        pending = self._record_runner.create_pending_job(
            "/incoming/one", job_id=root_task_id,
        )
        self._record_runner.start_automatic_job(pending.id, target_shelf="anime")
        analyze_root_boundaries(
            self._record_alist, "/incoming/one",
            root_task_id=root_task_id, state_root=state_root,
        )
        records = load_work_unit_records(state_root, root_task_id)
        apply_work_unit_override(
            state_root, root_task_id, records[0].work_unit_id,
            media_type="tv", tmdb_id=tmdb_id,
        )
        return load_work_unit_records(state_root, root_task_id)[0]

    def test_gap_registration_restricts_to_unit_owned_seasons(self) -> None:
        files = {"/incoming/one/Fate Zero/S02E01.mkv": FAKE_VIDEO_BYTES}
        state_root, alist, runner, _p, _e = self._setup(
            files, tmdb=MultiSeasonTMDB(101, {1: 25, 2: 24}),
        )
        self._record_runner = runner
        self._record_alist = alist
        record = self._record_for(state_root, "root-owned", 101)
        executed_plan = {
            "files": [{
                "final_name": "S02E01.mkv",
                "media_kind": "video",
                "target_dir": "/library/番剧/Work (101)",
            }],
            "target_root": "/library/番剧/Work (101)",
        }
        _register_unit_episode_gaps(
            runner, state_root, "root-owned", record, executed_plan,
        )
        from engine.scrapeflow.gap_ledger import load_gap_ledger

        gaps = load_gap_ledger(state_root, "root-owned")
        tokens = {gap.gap_id.rsplit("::", 1)[1] for gap in gaps}
        # Only the unit's own season (S2) is registered; the sibling S1
        # catalog rows must never become phantom gaps.
        self.assertEqual(tokens, {f"S02E{e:02d}" for e in range(2, 25)})

    def test_gap_registration_dedupes_across_units_of_one_series(self) -> None:
        files = {"/incoming/one/Fate Zero/S02E01.mkv": FAKE_VIDEO_BYTES}
        state_root, alist, runner, _p, _e = self._setup(
            files, tmdb=MultiSeasonTMDB(101, {1: 25, 2: 24}),
        )
        self._record_runner = runner
        self._record_alist = alist
        first = self._record_for(state_root, "root-dedupe", 101)
        plan = {
            "files": [{
                "final_name": "S02E01.mkv",
                "media_kind": "video",
                "target_dir": "/library/番剧/Work (101)",
            }],
            "target_root": "/library/番剧/Work (101)",
        }
        _register_unit_episode_gaps(runner, state_root, "root-dedupe", first, plan)
        # A second unit of the same series registering the same season must
        # not create duplicate open rows for the same coordinates.
        second = replace_work_unit_record(first)
        _register_unit_episode_gaps(runner, state_root, "root-dedupe", second, plan)
        from engine.scrapeflow.gap_ledger import load_gap_ledger

        gaps = load_gap_ledger(state_root, "root-dedupe")
        coordinates = [
            (gap.season, list(gap.episodes)[0])
            for gap in gaps
            if gap.status == "open"
        ]
        self.assertEqual(len(coordinates), len(set(coordinates)))
        self.assertEqual(len(gaps), 23)

    def test_dir_move_plan_derives_coverage_from_the_snapshot(self) -> None:
        files = {
            "/incoming/one/Fate Zero/Season 01/S01E01.mkv": FAKE_VIDEO_BYTES,
            "/incoming/one/Fate Zero/Season 02/S02E01.mkv": FAKE_VIDEO_BYTES,
        }
        state_root, alist, runner, _p, _e = self._setup(
            files, tmdb=MultiSeasonTMDB(101, {1: 25, 2: 24}),
        )
        self._record_runner = runner
        self._record_alist = alist
        record = self._record_for(state_root, "root-dirmove", 101)
        executed_plan = {
            # Whole-directory move plans carry bare season rows, not videos.
            "files": [
                {"final_name": "S01", "media_kind": None, "target_dir": "/library/番剧/Work (101)"},
                {"final_name": "S02", "media_kind": None, "target_dir": "/library/番剧/Work (101)"},
            ],
            "target_root": "/library/番剧/Work (101)",
        }
        _register_unit_episode_gaps(
            runner, state_root, "root-dirmove", record, executed_plan,
        )
        from engine.scrapeflow.gap_ledger import load_gap_ledger

        tokens = {
            gap.gap_id.rsplit("::", 1)[1]
            for gap in load_gap_ledger(state_root, "root-dirmove")
        }
        self.assertEqual(
            tokens,
            {f"S01E{e:02d}" for e in range(2, 26)}
            | {f"S02E{e:02d}" for e in range(2, 25)},
        )

    def test_unverifiable_season_registers_nothing(self) -> None:
        files = {"/incoming/one/Fate Zero/[01] 第一集.mkv": FAKE_VIDEO_BYTES}
        state_root, alist, runner, _p, _e = self._setup(
            files, tmdb=MultiSeasonTMDB(101, {3: 24}),
        )
        self._record_runner = runner
        self._record_alist = alist
        record = self._record_for(state_root, "root-absolute", 101)
        executed_plan = {
            "files": [
                {"final_name": "S03", "media_kind": None, "target_dir": "/library/番剧/Work (101)"},
            ],
            "target_root": "/library/番剧/Work (101)",
        }
        _register_unit_episode_gaps(
            runner, state_root, "root-absolute", record, executed_plan,
        )
        from engine.scrapeflow.gap_ledger import load_gap_ledger

        # Absolute-number names cannot be verified against season coordinates:
        # the J step must fail closed and register nothing.
        self.assertEqual(load_gap_ledger(state_root, "root-absolute"), [])


def replace_work_unit_record(record):
    from dataclasses import replace as _replace
    from engine.scrapeflow.work_units import WorkUnitRecord

    return _replace(record, work_unit_id=f"{record.work_unit_id}-b")


if __name__ == "__main__":
    unittest.main()


class MultiSeasonAbsoluteMapTests(unittest.TestCase):
    def _setup(self, files: dict[str, bytes], tmdb: object):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        state_root = Path(temp.name)
        alist = IndexAList(dict(files))
        runner = SimpleEngineRunner(
            state_root,
            alist=alist,
            tmdb=tmdb,
            planner=_recording_planner([]),
            validate=False,
            library_root="/library",
            executor=lambda plan: {"ok": True},
        )
        return state_root, alist, runner

    def test_multi_season_absolute_block_carries_explicit_episode_map(self) -> None:
        files = {
            f"/incoming/sao/[TUDO] Sword Art Online Alicization [{i:02d}][Ma10p_2160p][x265].mkv": FAKE_VIDEO_BYTES
            for i in range(1, 48)
        }
        # Specials without a bracketed regular number stay on the planner's
        # ordinary special path and must not abort the map bridge.
        files["/incoming/sao/[TUDO] Sword Art Online Alicization [NCED01][Ma10p].mkv"] = FAKE_VIDEO_BYTES
        files["/incoming/sao/[TUDO] Sword Art Online Alicization [18.5][Ma10p].mkv"] = FAKE_VIDEO_BYTES
        state_root, alist, runner = self._setup(
            files, MultiSeasonTMDB(45782, {1: 25, 2: 24, 3: 24, 4: 23}),
        )
        pending = runner.create_pending_job("/incoming/sao", job_id="root-map")
        runner.start_automatic_job(pending.id, target_shelf="anime")
        analyze_root_boundaries(
            alist, "/incoming/sao", root_task_id="root-map", state_root=state_root,
        )
        records = load_work_unit_records(state_root, "root-map")
        self.assertEqual(len(records), 1)
        apply_work_unit_override(
            state_root, "root-map", records[0].work_unit_id,
            media_type="tv", tmdb_id=45782,
        )
        record = load_work_unit_records(state_root, "root-map")[0]

        request = _request_for_unit(runner, record, "root-map", state_root)

        self.assertIsNotNone(request.episode_map_path)
        mapping = json.loads(Path(request.episode_map_path).read_text(encoding="utf-8"))
        self.assertEqual(mapping["1"], "S03E01")
        self.assertEqual(mapping["24"], "S03E24")
        self.assertEqual(mapping["25"], "S04E01")
        self.assertEqual(mapping["47"], "S04E23")

    def test_single_season_block_keeps_the_ordinary_path(self) -> None:
        files = {
            f"/incoming/sao/[TUDO] Sword Art Online II [{i:02d}][Ma10p].mkv": FAKE_VIDEO_BYTES
            for i in range(1, 25)
        }
        state_root, alist, runner = self._setup(
            files, MultiSeasonTMDB(45782, {1: 25, 2: 24, 3: 24, 4: 23}),
        )
        pending = runner.create_pending_job("/incoming/sao", job_id="root-single")
        runner.start_automatic_job(pending.id, target_shelf="anime")
        analyze_root_boundaries(
            alist, "/incoming/sao", root_task_id="root-single", state_root=state_root,
        )
        records = load_work_unit_records(state_root, "root-single")
        apply_work_unit_override(
            state_root, "root-single", records[0].work_unit_id,
            media_type="tv", tmdb_id=45782,
        )
        record = load_work_unit_records(state_root, "root-single")[0]

        request = _request_for_unit(runner, record, "root-single", state_root)

        self.assertIsNone(request.episode_map_path)

    def test_se_token_files_skip_the_map_bridge(self) -> None:
        files = {
            f"/incoming/sao/Show.S03E{i:02d}.mkv": FAKE_VIDEO_BYTES
            for i in range(1, 48)
        }
        state_root, alist, runner = self._setup(
            files, MultiSeasonTMDB(45782, {1: 25, 2: 24, 3: 24, 4: 23}),
        )
        pending = runner.create_pending_job("/incoming/sao", job_id="root-se")
        runner.start_automatic_job(pending.id, target_shelf="anime")
        analyze_root_boundaries(
            alist, "/incoming/sao", root_task_id="root-se", state_root=state_root,
        )
        records = load_work_unit_records(state_root, "root-se")
        apply_work_unit_override(
            state_root, "root-se", records[0].work_unit_id,
            media_type="tv", tmdb_id=45782,
        )
        record = load_work_unit_records(state_root, "root-se")[0]

        request = _request_for_unit(runner, record, "root-se", state_root)

        self.assertIsNone(request.episode_map_path)


class FailedUnitRetryTests(unittest.TestCase):
    def test_failed_unit_replans_on_retry_and_retires_stale_carrier(self) -> None:
        files = {"/incoming/one/My Show/S01E01.mkv": FAKE_VIDEO_BYTES}
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        state_root = Path(temp.name)
        alist = IndexAList(dict(files))
        plan_calls: list[str] = []
        executor_failures = {"remaining": 1}

        def failing_executor(plan):
            if executor_failures["remaining"] > 0:
                executor_failures["remaining"] -= 1
                raise RuntimeError("injected executor failure")
            return {"ok": True}

        runner = SimpleEngineRunner(
            state_root, alist=alist, tmdb=object(),
            planner=_recording_planner(plan_calls), validate=False,
            library_root="/library", executor=failing_executor,
        )
        pending = runner.create_pending_job("/incoming/one", job_id="root-fail")
        runner.start_automatic_job(pending.id, target_shelf="anime")
        analyze_root_boundaries(
            alist, "/incoming/one", root_task_id="root-fail", state_root=state_root,
        )
        records = load_work_unit_records(state_root, "root-fail")
        apply_work_unit_override(
            state_root, "root-fail", records[0].work_unit_id,
            media_type="tv", tmdb_id=101,
        )
        reconcile_root_work_units(alist, "/library", state_root, "root-fail")

        first = execute_new_work_units(runner, state_root, "root-fail")
        self.assertEqual(first[0].outcome, "failed")
        records = load_work_unit_records(state_root, "root-fail")
        self.assertIsNone(records[0].writer_job_id)
        # The stale carrier must not survive a failed attempt.
        carrier_path = state_root / "jobs" / f"unit-{records[0].work_unit_id}.json"
        self.assertFalse(carrier_path.exists())

        second = execute_new_work_units(runner, state_root, "root-fail")
        self.assertEqual(second[0].outcome, "accepted")
        self.assertEqual(len(plan_calls), 2)
        records = load_work_unit_records(state_root, "root-fail")
        self.assertIsNotNone(records[0].writer_job_id)
