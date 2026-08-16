"""Tests for the P11 authoritative root pipeline (B/W/C/D -> F/G/H/J -> R)."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from typing import Any

from engine.scrapeflow.intake_source import intake_source_id
from engine.scrapeflow.models import Plan, PlannedFile
from engine.scrapeflow.unit_identity import apply_work_unit_override
from engine.scrapeflow.work_units import load_work_unit_records

from local.scrapeflow_api.root_pipeline import (
    is_intake_bound_root,
    run_root_pipeline,
)
from local.scrapeflow_api.simple_engine_runner import SimpleEngineRunner
from local.scrapeflow_api.unit_execution import load_work_acceptance

from local.tests.test_library_index import IndexAList, _sample_library
from local.tests.test_simple_engine_runner import FAKE_VIDEO_BYTES, FAKE_VIDEO_SIZE
from local.tests.test_work_unit_identity import FakeTMDBClient


def _recording_planner(events: list[dict[str, Any]]):
    def planner(request, _alist, _tmdb) -> Plan:
        events.append({
            "source_path": request.source_path,
            "parent_path": request.parent_path,
            "media_type": request.media_type,
            "tmdb_id": request.tmdb_id,
        })
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
            metadata={"tmdb_id": request.tmdb_id, "title": "Work", "year": "2020"},
        )

    return planner


def _merge_planner(events: list[dict[str, Any]]):
    """Planner double that resolves the series dir under the given parent."""

    def planner(request, _alist, _tmdb) -> Plan:
        events.append({
            "source_path": request.source_path,
            "parent_path": request.parent_path,
            "media_type": request.media_type,
            "tmdb_id": request.tmdb_id,
        })
        target = f"{request.parent_path.rstrip('/')}/Fate Zero"
        return Plan(
            mode="tv" if request.media_type == "tv" else "movie",
            source_root=request.source_path,
            target_root=target,
            files=[PlannedFile(
                source_path=f"{request.source_path}/S01E11.mkv",
                source_dir=request.source_path,
                original_name="S01E11.mkv",
                final_name="S01E11.mkv",
                target_dir=target,
                media_kind="video",
                source_size=FAKE_VIDEO_SIZE,
            )],
            warnings=[],
            metadata={
                "tmdb_id": request.tmdb_id,
                "title": "Fate/Zero",
                "year": "2011",
            },
        )

    return planner


def _confirming_tmdb() -> FakeTMDBClient:
    return FakeTMDBClient(
        search_results={
            "My Show": [
                {
                    "id": 101,
                    "name": "My Show",
                    "first_air_date": "2020-01-01",
                    "genre_ids": [16],
                },
            ],
        },
        details={"/tv/101": {"number_of_episodes": 2}},
    )


class RootPipelineTests(unittest.TestCase):
    def _setup(
        self,
        files: dict[str, bytes],
        *,
        library_files: dict[str, bytes] | None = None,
        tmdb: object | None = None,
        shelf: str = "anime",
        planner=None,
    ):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        state_root = Path(temp.name)
        alist = IndexAList({**files, **(library_files or {})})
        planner_events: list[dict[str, Any]] = []
        executor_events: list[str] = []
        runner = SimpleEngineRunner(
            state_root,
            alist=alist,
            tmdb=tmdb if tmdb is not None else _confirming_tmdb(),
            planner=planner if planner is not None else _recording_planner(planner_events),
            validate=False,
            library_root="/library",
            executor=lambda plan: (
                executor_events.append(str(plan.target_root)) or {"ok": True}
            ),
        )
        return state_root, alist, runner, planner_events, executor_events

    def _new_path_root(
        self,
        runner: SimpleEngineRunner,
        source: str,
        shelf: str,
    ):
        job = runner.create_root_job(
            intake_source_id(source),
            source_path=source,
            target_shelf=shelf,
        )
        return runner.start_automatic_job(job.id, target_shelf=shelf)

    def test_pipeline_completes_new_work_root_end_to_end(self) -> None:
        files = {
            "/incoming/My Show/S01E01.mkv": FAKE_VIDEO_BYTES,
            "/incoming/My Show/S01E02.mkv": FAKE_VIDEO_BYTES,
        }
        state_root, _alist, runner, planner_events, executor_events = self._setup(files)
        job = self._new_path_root(runner, "/incoming/My Show", "anime")
        root_task_id = job.id
        summary_keys_before = set(job.summary)

        final = run_root_pipeline(runner, state_root, root_task_id)

        self.assertEqual(final.phase, "completed")
        self.assertEqual(len(planner_events), 1)
        self.assertEqual(len(executor_events), 1)
        records = load_work_unit_records(state_root, root_task_id)
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0].reconciliation_outcome, "new_work")
        self.assertIsNotNone(records[0].writer_job_id)
        acceptance = load_work_acceptance(state_root, root_task_id)
        self.assertEqual([row.outcome for row in acceptance], ["accepted"])
        # The pipeline adds no new EngineJob.summary business fields.
        self.assertLessEqual(set(final.summary), summary_keys_before)

    def test_pipeline_parks_uncertain_identity_without_writing(self) -> None:
        files = {
            "/incoming/Mystery Show/S01E01.mkv": FAKE_VIDEO_BYTES,
        }
        state_root, _alist, runner, planner_events, executor_events = self._setup(
            files, tmdb=FakeTMDBClient(),
        )
        job = self._new_path_root(runner, "/incoming/Mystery Show", "anime")
        root_task_id = job.id

        final = run_root_pipeline(runner, state_root, root_task_id)

        self.assertEqual(final.phase, "reconciliation_uncertain")
        self.assertEqual(executor_events, [])
        self.assertEqual(planner_events, [])
        records = load_work_unit_records(state_root, root_task_id)
        self.assertEqual(records[0].identity_status, "uncertain")

    def test_pipeline_consumes_duplicate_units_into_archive(self) -> None:
        files = {
            "/incoming/Fate Zero/S01E03.mkv": FAKE_VIDEO_BYTES,
        }
        state_root, alist, runner, planner_events, executor_events = self._setup(
            files, library_files=_sample_library(),
        )
        job = self._new_path_root(runner, "/incoming/Fate Zero", "anime")
        root_task_id = job.id
        # Seed B/W + a durable identity the same way the pipeline would on
        # its first pass, then let the pipeline run D and the E1 lane.
        from engine.scrapeflow.root_boundaries import analyze_root_boundaries
        analyze_root_boundaries(
            alist, "/incoming/Fate Zero", root_task_id=root_task_id, state_root=state_root,
        )
        records = load_work_unit_records(state_root, root_task_id)
        apply_work_unit_override(
            state_root, root_task_id, records[0].work_unit_id,
            media_type="tv", tmdb_id=35507,
        )

        final = run_root_pipeline(runner, state_root, root_task_id)

        self.assertEqual(final.phase, "completed")
        self.assertEqual(executor_events, [])
        self.assertEqual(planner_events, [])
        records = load_work_unit_records(state_root, root_task_id)
        self.assertEqual(records[0].reconciliation_outcome, "duplicate_complete")
        self.assertEqual(records[0].lane_status, "duplicate_consumed")
        # Exactly one task-archive move; never a formal-library write.
        self.assertEqual(len(alist.move_calls), 1)
        parent, target, names = alist.move_calls[0]
        self.assertEqual(parent, "/incoming")
        self.assertIn("/ScrapeFlow/归档/", target)
        self.assertEqual(names, ["Fate Zero"])
        self.assertNotIn("/incoming/Fate Zero/S01E03.mkv", alist.files)

    def test_pipeline_reruns_duplicate_consumption_idempotently(self) -> None:
        files = {
            "/incoming/Fate Zero/S01E03.mkv": FAKE_VIDEO_BYTES,
        }
        state_root, alist, runner, _p, _e = self._setup(
            files, library_files=_sample_library(),
        )
        job = self._new_path_root(runner, "/incoming/Fate Zero", "anime")
        root_task_id = job.id
        from engine.scrapeflow.root_boundaries import analyze_root_boundaries
        analyze_root_boundaries(
            alist, "/incoming/Fate Zero", root_task_id=root_task_id, state_root=state_root,
        )
        records = load_work_unit_records(state_root, root_task_id)
        apply_work_unit_override(
            state_root, root_task_id, records[0].work_unit_id,
            media_type="tv", tmdb_id=35507,
        )

        first = run_root_pipeline(runner, state_root, root_task_id)
        second = run_root_pipeline(runner, state_root, root_task_id)

        self.assertEqual(first.phase, "completed")
        self.assertEqual(second.phase, "completed")
        self.assertEqual(len(alist.move_calls), 1)

    def test_pipeline_registers_existing_gap_and_keeps_source(self) -> None:
        files = {
            "/incoming/Fate Zero/S01E03.mkv": FAKE_VIDEO_BYTES,
        }
        state_root, alist, runner, planner_events, executor_events = self._setup(
            files, library_files=_sample_library(),
        )
        job = self._new_path_root(runner, "/incoming/Fate Zero", "anime")
        root_task_id = job.id
        # A known open gap for (tv, 35507) lives in another root's ledger.
        from engine.scrapeflow.gap_ledger import Gap, save_gap_ledger
        save_gap_ledger(state_root, "root-other", [Gap(
            gap_id="other::missing_episode::S02E01",
            root_task_id="root-other",
            work_unit_id="other-unit",
            kind="missing_episode",
            media_type="tv",
            tmdb_id=35507,
            season=2,
            episodes=(1,),
            subtitle_path=None,
            subtitle_language=None,
            status="open",
        )])
        from engine.scrapeflow.root_boundaries import analyze_root_boundaries
        analyze_root_boundaries(
            alist, "/incoming/Fate Zero", root_task_id=root_task_id, state_root=state_root,
        )
        records = load_work_unit_records(state_root, root_task_id)
        apply_work_unit_override(
            state_root, root_task_id, records[0].work_unit_id,
            media_type="tv", tmdb_id=35507,
        )

        final = run_root_pipeline(runner, state_root, root_task_id)

        self.assertEqual(final.phase, "completed")
        self.assertEqual(executor_events, [])
        records = load_work_unit_records(state_root, root_task_id)
        self.assertEqual(records[0].reconciliation_outcome, "existing_gap")
        self.assertEqual(records[0].lane_status, "existing_gap_registered")
        # The uncovered S02E01 coordinate is registered on the unit ledger.
        from engine.scrapeflow.gap_ledger import load_gap_ledger
        gaps = load_gap_ledger(state_root, root_task_id)
        self.assertEqual(len(gaps), 1)
        self.assertEqual(gaps[0].gap_id, f"{records[0].work_unit_id}::missing_episode::S02E01")
        self.assertEqual(gaps[0].status, "open")
        # Non-empty source stays in intake: no move happened.
        self.assertEqual(alist.move_calls, [])
        self.assertIn("/incoming/Fate Zero/S01E03.mkv", alist.files)

    def test_pipeline_merges_new_episodes_into_existing_work(self) -> None:
        files = {
            "/incoming/Fate Zero/S01E11.mkv": FAKE_VIDEO_BYTES,
        }
        merge_events: list[dict[str, Any]] = []
        state_root, alist, runner, _generic_planner, executor_events = self._setup(
            files, library_files=_sample_library(),
            planner=_merge_planner(merge_events),
        )
        job = self._new_path_root(runner, "/incoming/Fate Zero", "anime")
        root_task_id = job.id
        from engine.scrapeflow.root_boundaries import analyze_root_boundaries
        analyze_root_boundaries(
            alist, "/incoming/Fate Zero", root_task_id=root_task_id, state_root=state_root,
        )
        records = load_work_unit_records(state_root, root_task_id)
        apply_work_unit_override(
            state_root, root_task_id, records[0].work_unit_id,
            media_type="tv", tmdb_id=35507, season=1,
        )

        final = run_root_pipeline(runner, state_root, root_task_id)

        self.assertEqual(final.phase, "completed")
        self.assertEqual(len(merge_events), 1)
        self.assertEqual(merge_events[0]["parent_path"], "/library/番剧")
        self.assertEqual(len(executor_events), 1)
        records = load_work_unit_records(state_root, root_task_id)
        self.assertEqual(records[0].reconciliation_outcome, "merge_existing")
        self.assertEqual(records[0].lane_status, "merge_done")
        self.assertEqual(records[0].matched_work_root, "/library/番剧/Fate Zero")
        self.assertIsNotNone(records[0].writer_job_id)
        # The carrier is an internal child, never a second public task.
        carrier = runner.get_job(records[0].writer_job_id)
        self.assertIs(carrier.summary.get("internal_child"), True)
        self.assertEqual(carrier.summary.get("root_job_id"), root_task_id)

    def test_pipeline_pause_gate_keeps_writer_idle(self) -> None:
        files = {
            "/incoming/My Show/S01E01.mkv": FAKE_VIDEO_BYTES,
        }
        state_root, _alist, runner, _planner, executor_events = self._setup(files)
        job = self._new_path_root(runner, "/incoming/My Show", "anime")
        root_task_id = job.id

        final = run_root_pipeline(
            runner, state_root, root_task_id, pause_requested=lambda: True,
        )

        self.assertEqual(final.phase, "queued")
        self.assertEqual(executor_events, [])
        records = load_work_unit_records(state_root, root_task_id)
        self.assertEqual(records[0].reconciliation_outcome, "new_work")
        self.assertIsNone(records[0].writer_job_id)

    def test_pipeline_rerun_is_idempotent(self) -> None:
        files = {
            "/incoming/My Show/S01E01.mkv": FAKE_VIDEO_BYTES,
        }
        state_root, _alist, runner, planner_events, executor_events = self._setup(files)
        job = self._new_path_root(runner, "/incoming/My Show", "anime")
        root_task_id = job.id

        first = run_root_pipeline(runner, state_root, root_task_id)
        second = run_root_pipeline(runner, state_root, root_task_id)

        self.assertEqual(first.phase, "completed")
        self.assertEqual(second.phase, "completed")
        self.assertEqual(len(executor_events), 1)
        self.assertEqual(len(planner_events), 1)

    def test_intake_binding_detects_only_catalog_roots(self) -> None:
        files = {"/incoming/My Show/S01E01.mkv": FAKE_VIDEO_BYTES}
        state_root, _alist, runner, _p, _e = self._setup(files)
        bound = self._new_path_root(runner, "/incoming/My Show", "anime")
        plain = runner.create_pending_job("/incoming/two", job_id="root-plain")

        self.assertTrue(is_intake_bound_root(state_root, bound.id))
        self.assertFalse(is_intake_bound_root(state_root, plain.id))

    def test_override_clears_stale_reconciliation_decision(self) -> None:
        files = {
            "/incoming/Fate Zero/S01E03.mkv": FAKE_VIDEO_BYTES,
        }
        state_root, alist, runner, _p, _e = self._setup(
            files, library_files=_sample_library(),
        )
        from engine.scrapeflow.root_boundaries import analyze_root_boundaries
        from local.scrapeflow_api.library_index import reconcile_root_work_units
        analyze_root_boundaries(
            alist, "/incoming/Fate Zero", root_task_id="root-ov", state_root=state_root,
        )
        records = load_work_unit_records(state_root, "root-ov")
        apply_work_unit_override(
            state_root, "root-ov", records[0].work_unit_id,
            media_type="tv", tmdb_id=35507,
        )
        reconcile_root_work_units(alist, "/library", state_root, "root-ov")
        decided = load_work_unit_records(state_root, "root-ov")
        self.assertEqual(decided[0].reconciliation_outcome, "duplicate_complete")

        apply_work_unit_override(
            state_root, "root-ov", records[0].work_unit_id,
            media_type="tv", tmdb_id=9999,
        )
        reopened = load_work_unit_records(state_root, "root-ov")
        self.assertIsNone(reopened[0].reconciliation_outcome)
        self.assertIsNone(reopened[0].matched_work_root)


class CleaningIndexAList(IndexAList):
    """IndexAList plus explicit empty-directory removal for shell cleanup."""

    def __init__(self, files: dict[str, bytes] | None = None) -> None:
        super().__init__(files)
        self.remove_empty_calls: list[str] = []

    def remove_empty_dir(self, path: str) -> bool:
        normalized = path.rstrip("/") or "/"
        self.remove_empty_calls.append(normalized)
        prefix = normalized.rstrip("/") + "/"
        if any(name.startswith(prefix) for name in self.files):
            return False
        if any(name.startswith(prefix) and name != normalized for name in self.dirs):
            return False
        if normalized in self.dirs:
            self.dirs.discard(normalized)
            return True
        return False


class SourceShellCleanupTests(unittest.TestCase):
    """Empty source-dir shells are dropped after a root completes."""

    def _setup(
        self,
        files: dict[str, bytes],
        *,
        executor=None,
        tmdb: object | None = None,
    ):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        state_root = Path(temp.name)
        alist = CleaningIndexAList(files)
        executor_events: list[str] = []
        if executor is None:
            def executor(plan):
                executor_events.append(str(plan.target_root))
                for item in plan.files:
                    data = alist.files.pop(item.source_path, None)
                    if data is not None:
                        alist.files[f"{item.target_dir.rstrip('/')}/{item.final_name}"] = data
                return {"ok": True}

        runner = SimpleEngineRunner(
            state_root,
            alist=alist,
            tmdb=tmdb if tmdb is not None else _confirming_tmdb(),
            planner=_recording_planner([]),
            validate=False,
            library_root="/library",
            executor=executor,
        )
        return state_root, alist, runner, executor_events

    def _root(self, runner: SimpleEngineRunner, source: str) -> str:
        job = runner.create_root_job(
            intake_source_id(source), source_path=source, target_shelf="anime",
        )
        started = runner.start_automatic_job(job.id, target_shelf="anime")
        return started.id

    def test_completion_removes_empty_source_shells(self) -> None:
        files = {"/incoming/My Show/S01E01.mkv": FAKE_VIDEO_BYTES}
        state_root, alist, runner, executor_events = self._setup(files)
        root_task_id = self._root(runner, "/incoming/My Show")

        # A real write leaves emptied directory shells behind; model that by
        # seeding them when the executor has already moved the media out.
        original = runner.executor

        def executor(plan):
            result = original(plan)
            alist.dirs.add("/incoming/My Show")
            alist.dirs.add("/incoming/My Show/Extras")
            return result

        runner.executor = executor

        final = run_root_pipeline(runner, state_root, root_task_id)

        self.assertEqual(final.phase, "completed")
        self.assertEqual(len(executor_events), 1)
        self.assertIn("/incoming/My Show/Extras", alist.remove_empty_calls)
        self.assertIn("/incoming/My Show", alist.remove_empty_calls)
        self.assertNotIn("/incoming/My Show", alist.dirs)
        self.assertNotIn("/incoming/My Show/Extras", alist.dirs)

    def test_completion_keeps_nonempty_source_and_unclaimed_junk(self) -> None:
        files = {"/incoming/My Show/S01E01.mkv": FAKE_VIDEO_BYTES}
        state_root, alist, runner, executor_events = self._setup(files)
        root_task_id = self._root(runner, "/incoming/My Show")
        original = runner.executor

        def executor(plan):
            result = original(plan)
            # A leftover junk file the plan never claimed keeps its shell.
            alist.files["/incoming/My Show/notes.txt"] = b"junk"
            alist.dirs.add("/incoming/My Show")
            alist.dirs.add("/incoming/My Show/Extras")
            return result

        runner.executor = executor

        final = run_root_pipeline(runner, state_root, root_task_id)

        self.assertEqual(final.phase, "completed")
        self.assertIn("/incoming/My Show/Extras", alist.remove_empty_calls)
        self.assertNotIn("/incoming/My Show", alist.remove_empty_calls)
        self.assertIn("/incoming/My Show", alist.dirs)
        self.assertIn("/incoming/My Show/notes.txt", alist.files)

    def test_parked_root_never_triggers_shell_cleanup(self) -> None:
        files = {"/incoming/Mystery Show/S01E01.mkv": FAKE_VIDEO_BYTES}
        state_root, alist, runner, executor_events = self._setup(
            files, tmdb=FakeTMDBClient(),
        )
        root_task_id = self._root(runner, "/incoming/Mystery Show")

        final = run_root_pipeline(runner, state_root, root_task_id)

        self.assertEqual(final.phase, "reconciliation_uncertain")
        self.assertEqual(executor_events, [])
        self.assertEqual(alist.remove_empty_calls, [])
        self.assertIn("/incoming/Mystery Show/S01E01.mkv", alist.files)

    def test_unbound_source_is_never_cleaned(self) -> None:
        files = {"/incoming/My Show/S01E01.mkv": FAKE_VIDEO_BYTES}
        state_root, alist, runner, executor_events = self._setup(files)
        root_task_id = self._root(runner, "/incoming/My Show")
        # Simulate a catalog that no longer binds the source to this root
        # (e.g. an external catalog reset): the cleanup ownership gate must
        # refuse the tree even when it is fully empty.
        from engine.scrapeflow.intake_source import save_intake_catalog
        save_intake_catalog(state_root, [])
        alist.dirs.add("/incoming/My Show")

        final = run_root_pipeline(runner, state_root, root_task_id)

        self.assertEqual(final.phase, "completed")
        self.assertEqual(alist.remove_empty_calls, [])
        self.assertIn("/incoming/My Show", alist.dirs)


if __name__ == "__main__":
    unittest.main()
