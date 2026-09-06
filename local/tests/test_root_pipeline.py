"""Tests for the P11 authoritative root pipeline (B/W/C/D -> F/G/H/J -> R)."""

from __future__ import annotations

import json
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path
from typing import Any
from unittest.mock import patch

from engine.scrapeflow.gap_ledger import Gap, load_gap_ledger, save_gap_ledger
from engine.scrapeflow.intake_source import (
    bind_root_task,
    intake_source_id,
    load_intake_catalog,
    save_intake_catalog,
    upsert_intake_source,
)
from engine.scrapeflow.models import Plan, PlannedFile
from engine.scrapeflow.serialization import atomic_write_json
from engine.scrapeflow.unit_identity import apply_work_unit_override
from engine.scrapeflow.work_units import (
    WorkUnitRecord,
    load_work_unit_records,
    save_work_unit_records,
)

from local.scrapeflow_api.root_pipeline import (
    _coalesce_confirmed_tv_season_records,
    finalize_root_gap_closure,
    is_intake_bound_root,
    refresh_root_after_j_rereview,
    run_root_pipeline,
)
from local.scrapeflow_api.root_aggregation import aggregate_root_job
from local.scrapeflow_api.simple_engine_runner import (
    EngineJobConflictError,
    EnginePauseRequested,
    EngineRequestError,
    SimpleEngineRunner,
)
from local.scrapeflow_api.unit_execution import load_work_acceptance

from local.tests.test_library_index import IndexAList, _nfo_movie, _sample_library
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
    season_101 = [
        {"episode_number": number, "air_date": "2020-01-01"}
        for number in range(1, 2)
    ]
    season_35507 = [
        {"episode_number": number, "air_date": "2011-01-01"}
        for number in range(1, 12)
    ]
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
        details={
            "/tv/101": {
                "number_of_episodes": 1,
                "seasons": [{"season_number": 1, "name": "Season 1"}],
            },
            "/tv/101/season/1": {"episodes": season_101},
            "/tv/35507": {
                "seasons": [{"season_number": 1, "name": "Season 1"}],
            },
            "/tv/35507/season/1": {"episodes": season_35507},
        },
    )


class LoginRequiredIndexAList(IndexAList):
    """A persisted-root double that rejects B/W before login."""

    def __init__(self, files: dict[str, bytes]) -> None:
        super().__init__(files)
        self.token: str | None = None
        self.login_calls = 0

    def login(self) -> str:
        self.login_calls += 1
        self.token = "test-token"
        return self.token

    def list(self, path: str, refresh: bool = False) -> list[dict[str, object]]:
        if not self.token:
            raise RuntimeError("尚未登录 AList")
        return super().list(path, refresh=refresh)


class RootPipelineTests(unittest.TestCase):
    def _setup(
        self,
        files: dict[str, bytes],
        *,
        library_files: dict[str, bytes] | None = None,
        tmdb: object | None = None,
        shelf: str = "anime",
        planner=None,
        archive_preprocessor: object | None = None,
        alist_cls=None,
    ):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        state_root = Path(temp.name)
        alist = (alist_cls or IndexAList)({**files, **(library_files or {})})
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
            archive_preprocessor=archive_preprocessor,
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

    def test_stale_intake_binding_fails_closed_without_creating_replacement(self) -> None:
        """A deleted RootJob must be reconciled, never silently recreated."""
        state_root, _alist, runner, _planner_events, _executor_events = self._setup(
            {"/incoming/Orphaned/S01E01.mkv": FAKE_VIDEO_BYTES},
        )
        source = "/incoming/Orphaned"
        catalog, _ = upsert_intake_source([], source, present=True)
        catalog, _ = bind_root_task(catalog, intake_source_id(source), "deleted-root")
        save_intake_catalog(state_root, catalog)

        with self.assertRaises(EngineJobConflictError):
            runner.create_root_job(
                intake_source_id(source),
                source_path=source,
                target_shelf="anime",
            )
        self.assertEqual(runner.list_jobs(), [])
        self.assertEqual(
            load_intake_catalog(state_root)[0].root_task_id,
            "deleted-root",
        )

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

    def test_open_j_gap_parks_root_until_replenishment_closes_it(self) -> None:
        """A successful H receipt cannot skip the J/N closure boundary."""
        files = {
            "/incoming/My Show/S01E01.mkv": FAKE_VIDEO_BYTES,
            "/incoming/My Show/S01E02.mkv": FAKE_VIDEO_BYTES,
        }
        state_root, _alist, runner, _planner_events, _executor_events = self._setup(files)
        job = self._new_path_root(runner, "/incoming/My Show", "anime")
        root_task_id = job.id

        # Seed the ordinary B/W/C/D/F/H records through the real pipeline,
        # but arrange a precise J gap after H so the R node sees the same
        # durable state that a partial catalog reconciliation would produce.
        from local.scrapeflow_api import root_pipeline
        original_execute = root_pipeline.execute_new_work_units

        def execute_then_register(*args, **kwargs):
            result = original_execute(*args, **kwargs)
            record = load_work_unit_records(state_root, root_task_id)[0]
            identity = record.identity or {}
            save_gap_ledger(state_root, root_task_id, [Gap(
                gap_id=f"{record.work_unit_id}::missing_episode::S01E03",
                root_task_id=root_task_id,
                work_unit_id=record.work_unit_id,
                kind="missing_episode",
                media_type="tv",
                tmdb_id=int(identity["tmdb_id"]),
                season=1,
                episodes=(3,),
                subtitle_path=None,
                subtitle_language=None,
                status="open",
            )])
            return result

        with patch(
            "local.scrapeflow_api.root_pipeline.execute_new_work_units",
            side_effect=execute_then_register,
        ):
            final = run_root_pipeline(runner, state_root, root_task_id)

        self.assertEqual(final.phase, "gaps_pending")
        self.assertEqual(aggregate_root_job(state_root, root_task_id).status, "gaps_pending")
        # A narrow J-only rereview must project the same existing R state
        # without re-entering B/W/C/D/F/G/H.
        stale_completed = replace(runner.get_job(root_task_id), phase="completed")
        atomic_write_json(
            runner._job_path(root_task_id),  # noqa: SLF001 - stale-root fixture
            stale_completed.as_dict(),
            allow_nan=False,
        )
        rereviewed = refresh_root_after_j_rereview(runner, state_root, root_task_id)
        self.assertEqual(rereviewed.phase, "gaps_pending")
        gap = load_gap_ledger(state_root, root_task_id)[0]
        save_gap_ledger(state_root, root_task_id, [replace(gap, status="closed")])
        closed = finalize_root_gap_closure(runner, state_root, root_task_id)
        self.assertEqual(closed.phase, "completed")

    def test_gap_closure_cleans_the_consumed_intake_tree(self) -> None:
        """A root completed through gap closure deletes its intake tree too.

        The direct-completion path already deletes the source (operator
        ruling 2026-08-27: intake is staging, not storage).  A root that
        reaches ``completed`` via ``finalize_root_gap_closure`` after its
        gaps closed must obey the same rule instead of leaving the tree
        for manual cleanup forever.
        """
        files = {
            "/incoming/My Show/S01E01.mkv": FAKE_VIDEO_BYTES,
            "/incoming/My Show/S01E02.mkv": FAKE_VIDEO_BYTES,
        }
        state_root, _alist, runner, _planner_events, _executor_events = self._setup(files)
        job = self._new_path_root(runner, "/incoming/My Show", "anime")
        root_task_id = job.id
        # Seed the ordinary B/W/C/D/F/H records first, then plant an open gap
        # so R parks the root in gaps_pending.
        from local.scrapeflow_api import root_pipeline
        original_execute = root_pipeline.execute_new_work_units

        def execute_then_register(*args, **kwargs):
            result = original_execute(*args, **kwargs)
            record = load_work_unit_records(state_root, root_task_id)[0]
            identity = record.identity or {}
            save_gap_ledger(state_root, root_task_id, [Gap(
                gap_id=f"{record.work_unit_id}::missing_episode::S01E03",
                root_task_id=root_task_id,
                work_unit_id=record.work_unit_id,
                kind="missing_episode",
                media_type="tv",
                tmdb_id=int(identity["tmdb_id"]),
                season=1,
                episodes=(3,),
                subtitle_path=None,
                subtitle_language=None,
                status="open",
            )])
            return result

        with patch(
            "local.scrapeflow_api.root_pipeline.execute_new_work_units",
            side_effect=execute_then_register,
        ):
            parked = run_root_pipeline(runner, state_root, root_task_id)
        self.assertEqual(parked.phase, "gaps_pending")

        # Swap in a deleting AList double, close the gap, and let the
        # finalize transition run the same intake cleanup as a direct
        # completion.
        runner.alist = CleaningIndexAList({
            "/incoming/My Show/S01E01.mkv": FAKE_VIDEO_BYTES,
            "/incoming/My Show/S01E02.mkv": FAKE_VIDEO_BYTES,
        })
        gap = load_gap_ledger(state_root, root_task_id)[0]
        save_gap_ledger(state_root, root_task_id, [replace(gap, status="closed")])
        closed = finalize_root_gap_closure(runner, state_root, root_task_id)
        self.assertEqual(closed.phase, "completed")
        listing = runner.alist.list("/incoming", refresh=True)
        self.assertEqual(
            [row["name"] for row in listing],
            [],
            "gap-closure completion must delete the consumed intake tree",
        )

    def test_pipeline_authenticates_a_resumed_root_before_boundary_read(self) -> None:
        files = {
            "/incoming/My Show/S01E01.mkv": FAKE_VIDEO_BYTES,
            "/incoming/My Show/S01E02.mkv": FAKE_VIDEO_BYTES,
        }
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        state_root = Path(temp.name)
        alist = LoginRequiredIndexAList(files)
        planner_events: list[dict[str, Any]] = []
        runner = SimpleEngineRunner(
            state_root,
            alist=alist,
            tmdb=_confirming_tmdb(),
            planner=_recording_planner(planner_events),
            validate=False,
            library_root="/library",
            executor=lambda _plan: {"ok": True},
        )
        job = self._new_path_root(runner, "/incoming/My Show", "anime")

        final = run_root_pipeline(runner, state_root, job.id)

        self.assertEqual(final.phase, "completed")
        self.assertEqual(alist.login_calls, 1)
        self.assertEqual(len(planner_events), 1)

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

    def test_pipeline_parks_disc_image_without_planning_moving_or_archiving(self) -> None:
        source = "/incoming/Disc source"
        image_path = f"{source}/Season 01.iso"
        state_root, alist, runner, planner_events, executor_events = self._setup({
            image_path: b"i" * (1024 * 1024),
        }, tmdb=FakeTMDBClient())
        job = self._new_path_root(runner, source, "anime")

        final = run_root_pipeline(runner, state_root, job.id)

        self.assertEqual(final.phase, "reconciliation_uncertain")
        self.assertEqual(planner_events, [])
        self.assertEqual(executor_events, [])
        self.assertEqual(alist.move_calls, [])
        self.assertIn(image_path, alist.files)
        record = load_work_unit_records(state_root, job.id)[0]
        self.assertTrue(record.requires_content_expansion)
        self.assertEqual(record.identity_status, "uncertain")
        self.assertIsNone(record.identity)
        self.assertIsNone(record.reconciliation_outcome)
        self.assertIn("只读安全内容展开", record.attention or "")

    def test_pipeline_does_not_stage_opaque_container_before_snapshot_bridge(self) -> None:
        """A configured archive adapter cannot replace RootJob ingress pre-B/W.

        The adapter itself is safe, but its task-owned staging root is not an
        IntakeSource.  Calling it from the RootJob pipeline before there is a
        durable expanded-source ownership bridge would make the B snapshot and
        later F scope checks disagree.  The container must stay visible as
        explicit attention instead.
        """
        class WouldStageArchive:
            def __init__(self) -> None:
                self.calls = 0

            def prepare_ordinary_request(self, request, **_kwargs):
                self.calls += 1
                return {
                    **request,
                    "source_path": "/library/ScrapeFlow/归档/forbidden-staging",
                    "archive_preprocessed": {"changed": True},
                }

        source = "/incoming/No pre-BW stage"
        image_path = f"{source}/Disc.iso"
        preprocessor = WouldStageArchive()
        state_root, _alist, runner, planner_events, executor_events = self._setup(
            {image_path: b"i" * (1024 * 1024)},
            tmdb=FakeTMDBClient(),
            archive_preprocessor=preprocessor,
        )
        job = self._new_path_root(runner, source, "anime")

        final = run_root_pipeline(runner, state_root, job.id)

        self.assertEqual(final.phase, "reconciliation_uncertain")
        self.assertEqual(preprocessor.calls, 0)
        self.assertEqual(planner_events, [])
        self.assertEqual(executor_events, [])
        record = load_work_unit_records(state_root, job.id)[0]
        self.assertTrue(record.requires_content_expansion)
        self.assertTrue(record.source_paths[0].startswith(source))

    def test_pipeline_coalesces_confirmed_same_tv_season_siblings_before_d(self) -> None:
        """C-confirmed sibling season scopes become one D input, pre-write only."""
        source = "/incoming/Explicit Seasons"
        season_one = f"{source}/Release S01"
        season_two = f"{source}/Release S02"
        files = {
            f"{season_one}/My.Show.S01E01.mkv": FAKE_VIDEO_BYTES,
            f"{season_two}/My.Show.S02E01.mkv": FAKE_VIDEO_BYTES,
        }
        state_root, _alist, runner, _planner_events, _executor_events = self._setup(files)
        job = self._new_path_root(runner, source, "anime")
        records = [
            WorkUnitRecord(
                work_unit_id="unit-season-one",
                root_task_id=job.id,
                boundary_key=season_one,
                source_paths=(season_one,),
                source_revision=1,
                role="season",
                display_label="Release S01",
                claimed_seasons=(1,),
                media_context="tv",
                identity_status="confirmed",
                identity={
                    "media_type": "tv",
                    "tmdb_id": 101,
                    "season": 1,
                    "source": "operator_override",
                },
            ),
            WorkUnitRecord(
                work_unit_id="unit-season-two",
                root_task_id=job.id,
                boundary_key=season_two,
                source_paths=(season_two,),
                source_revision=1,
                role="season",
                display_label="Release S02",
                claimed_seasons=(2,),
                media_context="tv",
                identity_status="confirmed",
                identity={
                    "media_type": "tv",
                    "tmdb_id": 101,
                    "season": 2,
                    "source": "operator_override",
                },
            ),
        ]
        save_work_unit_records(state_root, job.id, records)
        atomic_write_json(
            state_root / f"work_snapshot_{job.id}.json",
            {
                "root": source,
                "rows": [
                    {"name": "Release S01", "is_dir": True, "full_path": season_one},
                    {
                        "name": "My.Show.S01E01.mkv",
                        "is_dir": False,
                        "size": FAKE_VIDEO_SIZE,
                        "full_path": f"{season_one}/My.Show.S01E01.mkv",
                    },
                    {"name": "Release S02", "is_dir": True, "full_path": season_two},
                    {
                        "name": "My.Show.S02E01.mkv",
                        "is_dir": False,
                        "size": FAKE_VIDEO_SIZE,
                        "full_path": f"{season_two}/My.Show.S02E01.mkv",
                    },
                ],
            },
            allow_nan=False,
        )
        observed_d_inputs: list[list[WorkUnitRecord]] = []

        def observe_d(*_args, **_kwargs):
            observed_d_inputs.append(load_work_unit_records(state_root, job.id))
            return observed_d_inputs[-1]

        with patch(
            "local.scrapeflow_api.root_pipeline.reconcile_root_work_units",
            side_effect=observe_d,
        ), patch(
            "local.scrapeflow_api.root_pipeline.execute_unit_e_lanes",
            return_value=[],
        ):
            final = run_root_pipeline(runner, state_root, job.id)

        self.assertEqual(final.phase, "reconciliation_uncertain")
        self.assertEqual(len(observed_d_inputs), 1)
        self.assertEqual(len(observed_d_inputs[0]), 1)
        merged = observed_d_inputs[0][0]
        self.assertEqual(merged.work_unit_id, "unit-season-one")
        self.assertEqual(merged.source_paths, (season_one, season_two))
        self.assertEqual(merged.claimed_seasons, (1, 2))
        self.assertNotIn("season", merged.identity or {})

    def test_retry_coalescing_preserves_partially_accepted_same_tv_sibling(self) -> None:
        """An H receipt blocks C-stage re-keying during a later retry."""
        source = "/incoming/Partial Seasons"
        season_one = f"{source}/Release S01"
        season_two = f"{source}/Release S02"
        state_root, _alist, runner, _planner_events, _executor_events = self._setup({
            f"{season_one}/My.Show.S01E01.mkv": FAKE_VIDEO_BYTES,
            f"{season_two}/My.Show.S02E01.mkv": FAKE_VIDEO_BYTES,
        })
        job = self._new_path_root(runner, source, "anime")
        records = [
            WorkUnitRecord(
                work_unit_id="unit-accepted-s01",
                root_task_id=job.id,
                boundary_key=season_one,
                source_paths=(season_one,),
                source_revision=1,
                role="season",
                display_label="Release S01",
                claimed_seasons=(1,),
                media_context="tv",
                identity_status="confirmed",
                identity={"media_type": "tv", "tmdb_id": 101},
                reconciliation_outcome="new_work",
                writer_job_id="unit-accepted-s01",
                gap_status="registered",
            ),
            WorkUnitRecord(
                work_unit_id="unit-pending-s02",
                root_task_id=job.id,
                boundary_key=season_two,
                source_paths=(season_two,),
                source_revision=1,
                role="season",
                display_label="Release S02",
                claimed_seasons=(2,),
                media_context="tv",
                identity_status="confirmed",
                identity={"media_type": "tv", "tmdb_id": 101},
            ),
        ]
        save_work_unit_records(state_root, job.id, records)
        atomic_write_json(
            state_root / f"work_snapshot_{job.id}.json",
            {
                "root": source,
                "rows": [
                    {"name": "Release S01", "is_dir": True, "full_path": season_one},
                    {
                        "name": "My.Show.S01E01.mkv", "is_dir": False,
                        "size": FAKE_VIDEO_SIZE,
                        "full_path": f"{season_one}/My.Show.S01E01.mkv",
                    },
                    {"name": "Release S02", "is_dir": True, "full_path": season_two},
                    {
                        "name": "My.Show.S02E01.mkv", "is_dir": False,
                        "size": FAKE_VIDEO_SIZE,
                        "full_path": f"{season_two}/My.Show.S02E01.mkv",
                    },
                ],
            },
            allow_nan=False,
        )
        atomic_write_json(
            state_root / f"work_acceptance_{job.id}.json",
            [
                {
                    "work_unit_id": "unit-accepted-s01",
                    "outcome": "accepted",
                    "writer_job_id": "unit-accepted-s01",
                    "phase": "executed",
                    "target_root": "/library/番剧/My Show",
                    "planned_files": 1,
                    "error": None,
                    "recorded_at": "2026-08-19T00:00:00Z",
                },
            ],
            allow_nan=False,
        )

        unchanged = _coalesce_confirmed_tv_season_records(
            state_root, job.id, load_work_unit_records(state_root, job.id),
        )

        self.assertEqual(unchanged, records)
        self.assertEqual(
            load_work_unit_records(state_root, job.id), records,
        )

    def test_uncertain_sibling_does_not_block_confirmed_new_work_unit(self) -> None:
        files = {
            "/incoming/Container/Confirmed/S01E01.mkv": FAKE_VIDEO_BYTES,
            "/incoming/Container/Mystery/S01E01.mkv": FAKE_VIDEO_BYTES,
        }
        state_root, alist, runner, planner_events, executor_events = self._setup(
            files, tmdb=_confirming_tmdb(),
        )
        job = self._new_path_root(runner, "/incoming/Container", "anime")
        from engine.scrapeflow.root_boundaries import analyze_root_boundaries

        analyze_root_boundaries(
            alist,
            "/incoming/Container",
            root_task_id=job.id,
            state_root=state_root,
        )
        records = load_work_unit_records(state_root, job.id)
        confirmed = next(record for record in records if record.display_label == "Confirmed")
        apply_work_unit_override(
            state_root,
            job.id,
            confirmed.work_unit_id,
            media_type="tv",
            tmdb_id=101,
        )

        final = run_root_pipeline(runner, state_root, job.id)

        self.assertEqual(final.phase, "reconciliation_uncertain")
        self.assertEqual(len(planner_events), 1)
        self.assertEqual(len(executor_events), 1)
        updated = {record.display_label: record for record in load_work_unit_records(state_root, job.id)}
        self.assertEqual(updated["Confirmed"].reconciliation_outcome, "new_work")
        self.assertIsNotNone(updated["Confirmed"].writer_job_id)
        self.assertEqual(updated["Mystery"].identity_status, "uncertain")

    def test_d_uncertain_sibling_does_not_block_confirmed_new_work_unit(self) -> None:
        """A cross-shelf D conflict parks only its own WorkUnit.

        The unrelated confirmed movie still follows F/G/H, while R keeps the
        root in attention for the conflict rather than silently skipping it.
        """
        files = {
            "/incoming/Container/Conflicted/Feature.mkv": FAKE_VIDEO_BYTES,
            "/incoming/Container/Fresh/Feature.mkv": FAKE_VIDEO_BYTES,
        }
        library_files = _sample_library()
        library_files[
            "/library/番剧/Inception copy/movie.nfo"
        ] = _nfo_movie(27205, "Inception", "2010")
        library_files[
            "/library/番剧/Inception copy/Inception.2010.2160p.mkv"
        ] = b"v"
        state_root, alist, runner, planner_events, executor_events = self._setup(
            files,
            library_files=library_files,
        )
        job = self._new_path_root(runner, "/incoming/Container", "anime")
        from engine.scrapeflow.root_boundaries import analyze_root_boundaries

        analyze_root_boundaries(
            alist,
            "/incoming/Container",
            root_task_id=job.id,
            state_root=state_root,
        )
        for record in load_work_unit_records(state_root, job.id):
            if record.display_label == "Conflicted":
                apply_work_unit_override(
                    state_root,
                    job.id,
                    record.work_unit_id,
                    media_type="movie",
                    tmdb_id=27205,
                )
            elif record.display_label == "Fresh":
                apply_work_unit_override(
                    state_root,
                    job.id,
                    record.work_unit_id,
                    media_type="movie",
                    tmdb_id=99092,
                )
            else:  # pragma: no cover - fixture boundary assertion
                self.fail(f"unexpected WorkUnit: {record.display_label}")

        final = run_root_pipeline(runner, state_root, job.id)

        self.assertEqual(final.phase, "reconciliation_uncertain")
        self.assertEqual(len(planner_events), 1)
        self.assertEqual(len(executor_events), 1)
        updated = {
            record.display_label: record
            for record in load_work_unit_records(state_root, job.id)
        }
        self.assertEqual(updated["Conflicted"].reconciliation_outcome, "uncertain")
        self.assertIsNone(updated["Conflicted"].writer_job_id)
        self.assertEqual(updated["Fresh"].reconciliation_outcome, "new_work")
        self.assertIsNotNone(updated["Fresh"].writer_job_id)
        aggregate = aggregate_root_job(state_root, job.id)
        self.assertEqual((aggregate.completed, aggregate.attention, aggregate.failed), (1, 1, 0))

    def test_decorated_multi_season_cohort_writes_only_its_scoped_manifest(self) -> None:
        """A confirmed cohort must not widen into an ambiguous aftershow.

        This is intentionally a generic fixture: two decorated, corroborated
        season folders plus an adjacent empty declared season form one TV
        WorkUnit, while a separately named aftershow remains a distinct C/U
        unit.  The injected writer is only an observation seam; normal
        runner scope validation still runs before it is called.
        """
        root = "/incoming/Northwind Bundle"
        season_one = f"{root}/Northwind.Series.S01.Blu-ray"
        season_two = f"{root}/Northwind.Series.S02.WEB-DL"
        season_three = f"{root}/Northwind.Series.S03.WEB-DL"
        aftershow = f"{root}/Aftershow"
        expected_scopes = (season_one, season_two, season_three)
        expected_manifest = {
            f"{season_one}/Northwind.Series.S01E01.mkv",
            f"{season_one}/Northwind.Series.S01E02.mkv",
            f"{season_two}/Northwind.Series.S02E01.mkv",
            f"{season_two}/Northwind.Series.S02E02.mkv",
        }
        aftershow_video = f"{aftershow}/Aftershow.E01.mkv"
        files = {
            path: FAKE_VIDEO_BYTES
            for path in expected_manifest | {aftershow_video}
        }
        catalog_id = 99091
        season_rows = {
            season: [
                {
                    "season_number": season,
                    "episode_number": episode,
                    "air_date": "2020-01-01",
                }
                for episode in (1, 2)
            ]
            for season in (1, 2, 3)
        }
        tmdb = FakeTMDBClient(details={
            f"/tv/{catalog_id}": {
                "seasons": [
                    {"season_number": season, "name": f"Season {season}"}
                    for season in (1, 2, 3)
                ],
            },
            **{
                f"/tv/{catalog_id}/season/{season}": {"episodes": rows}
                for season, rows in season_rows.items()
            },
        })
        planner_observations: list[dict[str, object]] = []
        writer_sources: list[tuple[str, ...]] = []

        def scoped_planner(request, _alist, _tmdb) -> Plan:
            manifest = tuple(request.source_files or ())
            planner_observations.append({
                "source_path": request.source_path,
                "scope_paths": tuple(request.source_scope_paths),
                "manifest_paths": tuple(sorted(
                    str(row.get("full_path") or "") for row in manifest
                )),
            })
            target = f"{request.parent_path.rstrip('/')}/Scoped Work ({request.tmdb_id})"
            planned = [
                PlannedFile(
                    source_path=str(row["full_path"]),
                    source_dir=str(row["full_path"]).rsplit("/", 1)[0],
                    original_name=str(row["name"]),
                    final_name=str(row["name"]),
                    target_dir=target,
                    media_kind="video",
                    source_size=int(row["size"]),
                )
                for row in manifest
                if str(row.get("name") or "").endswith(".mkv")
            ]
            return Plan(
                mode="tv",
                source_root=request.source_path,
                target_root=target,
                files=planned,
                warnings=[],
                metadata={"tmdb_id": request.tmdb_id, "title": "Scoped Work"},
            )

        def observing_writer(plan: Plan) -> dict[str, object]:
            writer_sources.append(tuple(sorted(item.source_path for item in plan.files)))
            return {"ok": True}

        state_root, alist, runner, _planner_events, _executor_events = self._setup(
            files,
            tmdb=tmdb,
            planner=scoped_planner,
        )
        # A real empty provider directory is required for B/W to prove the
        # adjacent S03 boundary; it intentionally has no manifest file rows.
        alist.dirs.add(season_three)
        runner.executor = observing_writer
        job = self._new_path_root(runner, root, "anime")

        from engine.scrapeflow.root_boundaries import analyze_root_boundaries
        analyze_root_boundaries(
            alist,
            root,
            root_task_id=job.id,
            state_root=state_root,
        )
        discovered = load_work_unit_records(state_root, job.id)
        self.assertEqual(len(discovered), 2)
        cohort = next(record for record in discovered if record.claimed_seasons)
        sibling = next(record for record in discovered if not record.claimed_seasons)
        self.assertEqual(cohort.source_paths, expected_scopes)
        self.assertEqual(cohort.claimed_seasons, (1, 2, 3))
        self.assertEqual(cohort.role, "single_work")
        self.assertEqual(sibling.source_paths, (aftershow,))

        # This is the only manual C/U input.  The aftershow deliberately has
        # no identity evidence and must stay isolated as operator attention.
        apply_work_unit_override(
            state_root,
            job.id,
            cohort.work_unit_id,
            media_type="tv",
            tmdb_id=catalog_id,
        )

        final = run_root_pipeline(runner, state_root, job.id)

        self.assertEqual(final.phase, "reconciliation_uncertain")
        self.assertEqual(len(planner_observations), 1)
        self.assertEqual(planner_observations[0]["source_path"], root)
        self.assertEqual(planner_observations[0]["scope_paths"], expected_scopes)
        self.assertEqual(
            set(planner_observations[0]["manifest_paths"]), expected_manifest,
        )
        self.assertEqual(writer_sources, [tuple(sorted(expected_manifest))])
        self.assertNotIn(aftershow_video, writer_sources[0])
        # The ambiguous sibling was neither consumed nor widened into F/G.
        self.assertEqual(alist.files[aftershow_video], FAKE_VIDEO_BYTES)
        self.assertEqual(alist.move_calls, [])

        updated = {record.work_unit_id: record for record in load_work_unit_records(state_root, job.id)}
        self.assertEqual(updated[cohort.work_unit_id].reconciliation_outcome, "new_work")
        self.assertEqual(updated[cohort.work_unit_id].gap_status, "registered")
        self.assertEqual(updated[sibling.work_unit_id].identity_status, "uncertain")
        self.assertIsNone(updated[sibling.work_unit_id].writer_job_id)
        carrier = runner.get_job(updated[cohort.work_unit_id].writer_job_id)
        self.assertEqual(carrier.phase, "executed")
        self.assertEqual(tuple(carrier.request["source_scope_paths"]), expected_scopes)
        self.assertEqual(
            {
                str(row.get("full_path") or "")
                for row in carrier.request["source_files"]
            },
            expected_manifest,
        )
        gaps = load_gap_ledger(state_root, job.id)
        self.assertEqual(
            {(gap.work_unit_id, gap.season, gap.episodes, gap.status) for gap in gaps},
            {
                (cohort.work_unit_id, 3, (1,), "open"),
                (cohort.work_unit_id, 3, (2,), "open"),
            },
        )

    def test_j_persistence_failure_wins_over_separate_attention(self) -> None:
        files = {
            "/incoming/Container/Confirmed/S01E01.mkv": FAKE_VIDEO_BYTES,
            "/incoming/Container/Mystery/S01E01.mkv": FAKE_VIDEO_BYTES,
        }
        state_root, alist, runner, _planner_events, _executor_events = self._setup(
            files, tmdb=_confirming_tmdb(),
        )
        job = self._new_path_root(runner, "/incoming/Container", "anime")
        from engine.scrapeflow.root_boundaries import analyze_root_boundaries

        analyze_root_boundaries(
            alist,
            "/incoming/Container",
            root_task_id=job.id,
            state_root=state_root,
        )
        confirmed = next(
            record for record in load_work_unit_records(state_root, job.id)
            if record.display_label == "Confirmed"
        )
        apply_work_unit_override(
            state_root,
            job.id,
            confirmed.work_unit_id,
            media_type="tv",
            tmdb_id=101,
        )

        with patch(
            "local.scrapeflow_api.unit_execution.discover_episode_gaps",
            side_effect=OSError("injected J ledger failure"),
        ):
            final = run_root_pipeline(runner, state_root, job.id)

        # A real J persistence/readback fault must not be obscured by the
        # sibling's normal identity uncertainty.
        self.assertEqual(final.phase, "failed")
        updated = {record.display_label: record for record in load_work_unit_records(state_root, job.id)}
        self.assertEqual(updated["Confirmed"].gap_status, "failed")
        self.assertEqual(updated["Mystery"].identity_status, "uncertain")

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

    def test_pipeline_registers_existing_gap_and_consumes_source(self) -> None:
        files = {
            "/incoming/Fate Zero/S01E03.mkv": FAKE_VIDEO_BYTES,
        }
        state_root, alist, runner, planner_events, executor_events = self._setup(
            files, library_files=_sample_library(), alist_cls=CleaningIndexAList,
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

        self.assertEqual(final.phase, "gaps_pending")
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
        # Non-empty source stays in intake until the terminal hand-off; the
        # gaps_pending consumption then deletes the tree.  The source S01E03
        # carries a coordinate the library already holds (E01-E10) — a KNOWN
        # loser under the standing 只留高版本 ruling — so the 2026-09-05
        # gate deletes it as junk instead of quarantining it for the
        # operator (adjudication is only for content the library does NOT
        # hold).  No quarantine move, no residual note.
        self.assertEqual(alist.move_calls, [])
        self.assertNotIn("/incoming/Fate Zero/S01E03.mkv", alist.files)
        self.assertNotIn("/incoming/Fate Zero", alist.dirs)
        self.assertIsNone(final.error)

    def test_existing_gap_lane_never_duplicates_an_open_coordinate_from_another_unit(self) -> None:
        files = {
            "/incoming/Fate Zero/S01E03.mkv": FAKE_VIDEO_BYTES,
        }
        state_root, alist, runner, planner_events, executor_events = self._setup(
            files, library_files=_sample_library(), alist_cls=CleaningIndexAList,
        )
        job = self._new_path_root(runner, "/incoming/Fate Zero", "anime")
        root_task_id = job.id
        # A sibling unit of THIS root already holds the open S02E01 row: the
        # coordinate is a root-level fact and the existing-gap lane must not
        # append a second row for it.
        from engine.scrapeflow.gap_ledger import Gap, save_gap_ledger
        save_gap_ledger(state_root, root_task_id, [Gap(
            gap_id="sibling-unit::missing_episode::S02E01",
            root_task_id=root_task_id,
            work_unit_id="sibling-unit",
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

        self.assertEqual(final.phase, "gaps_pending")
        self.assertEqual(executor_events, [])
        records = load_work_unit_records(state_root, root_task_id)
        self.assertEqual(records[0].reconciliation_outcome, "existing_gap")
        self.assertEqual(records[0].lane_status, "existing_gap_registered")
        from engine.scrapeflow.gap_ledger import load_gap_ledger
        gaps = load_gap_ledger(state_root, root_task_id)
        self.assertEqual(len(gaps), 1)
        self.assertEqual(gaps[0].gap_id, "sibling-unit::missing_episode::S02E01")
        self.assertEqual(gaps[0].status, "open")

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
        # J reuses the locked post-write work root: the incoming E11 must not
        # make already-present E01–E10 appear as fabricated missing gaps.
        self.assertEqual(records[0].gap_status, "registered")
        from engine.scrapeflow.gap_ledger import load_gap_ledger
        self.assertEqual(load_gap_ledger(state_root, root_task_id), [])
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

    def test_cancel_after_read_only_boundary_wins_over_stale_root_transition(self) -> None:
        """A cancel must not be overwritten by the pipeline's old root copy."""
        files = {"/incoming/My Show/S01E01.mkv": FAKE_VIDEO_BYTES}
        state_root, _alist, runner, _planner, executor_events = self._setup(files)
        job = self._new_path_root(runner, "/incoming/My Show", "anime")

        from local.scrapeflow_api import root_pipeline

        original = root_pipeline.analyze_root_boundaries

        def analyze_then_cancel(*args, **kwargs):
            result = original(*args, **kwargs)
            runner.cancel_job(job.id, reason="stop after analysis")
            return result

        with patch.object(root_pipeline, "analyze_root_boundaries", side_effect=analyze_then_cancel):
            final = run_root_pipeline(runner, state_root, job.id)

        self.assertEqual(final.phase, "cancelled")
        self.assertEqual(runner.get_job(job.id).phase, "cancelled")
        self.assertEqual(executor_events, [])

    def test_merge_lane_pause_after_plan_keeps_formal_writer_idle(self) -> None:
        """E3 passes the root callback through planning and execution."""
        files = {"/incoming/Fate Zero/S01E11.mkv": FAKE_VIDEO_BYTES}
        paused = {"value": False}
        merge_events: list[dict[str, Any]] = []
        base_planner = _merge_planner(merge_events)

        def pausing_merge_planner(request, alist, tmdb):
            plan = base_planner(request, alist, tmdb)
            paused["value"] = True
            return plan

        state_root, alist, runner, _planner, executor_events = self._setup(
            files,
            library_files=_sample_library(),
            planner=pausing_merge_planner,
        )
        job = self._new_path_root(runner, "/incoming/Fate Zero", "anime")
        from engine.scrapeflow.root_boundaries import analyze_root_boundaries

        analyze_root_boundaries(
            alist,
            "/incoming/Fate Zero",
            root_task_id=job.id,
            state_root=state_root,
        )
        record = load_work_unit_records(state_root, job.id)[0]
        apply_work_unit_override(
            state_root,
            job.id,
            record.work_unit_id,
            media_type="tv",
            tmdb_id=35507,
            season=1,
        )

        final = run_root_pipeline(
            runner,
            state_root,
            job.id,
            pause_requested=lambda: paused["value"],
        )

        self.assertEqual(final.phase, "queued")
        self.assertEqual(len(merge_events), 1)
        self.assertEqual(executor_events, [])
        record = load_work_unit_records(state_root, job.id)[0]
        self.assertEqual(record.reconciliation_outcome, "merge_existing")
        self.assertNotEqual(record.lane_status, "merge_done")

    def test_merge_lane_pause_inside_writer_remains_resumable_not_failed(self) -> None:
        """A pause returned by G must not turn E3 into a failed root."""
        files = {"/incoming/Fate Zero/S01E11.mkv": FAKE_VIDEO_BYTES}
        paused = {"value": False}
        executor_events: list[str] = []

        def pausing_executor(plan):
            executor_events.append(str(plan.target_root))
            paused["value"] = True
            raise EnginePauseRequested("fixture pause during formal write")

        state_root, alist, runner, _planner, _default_events = self._setup(
            files,
            library_files=_sample_library(),
            planner=_merge_planner([]),
        )
        runner.executor = pausing_executor
        job = self._new_path_root(runner, "/incoming/Fate Zero", "anime")
        from engine.scrapeflow.root_boundaries import analyze_root_boundaries

        analyze_root_boundaries(
            alist,
            "/incoming/Fate Zero",
            root_task_id=job.id,
            state_root=state_root,
        )
        record = load_work_unit_records(state_root, job.id)[0]
        apply_work_unit_override(
            state_root,
            job.id,
            record.work_unit_id,
            media_type="tv",
            tmdb_id=35507,
            season=1,
        )

        final = run_root_pipeline(
            runner,
            state_root,
            job.id,
            pause_requested=lambda: paused["value"],
        )

        self.assertEqual(final.phase, "queued")
        self.assertEqual(len(executor_events), 1)
        record = load_work_unit_records(state_root, job.id)[0]
        self.assertNotEqual(record.lane_status, "merge_done")
        carrier = runner.get_job(f"unit-{record.work_unit_id}")
        self.assertEqual(carrier.phase, "executing")

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
    """IndexAList plus real delete semantics for source-root cleanup."""

    def __init__(self, files: dict[str, bytes] | None = None) -> None:
        super().__init__(files)
        self.remove_empty_calls: list[str] = []
        self.remove_calls: list[tuple[str, list[str]]] = []

    def remove(self, parent: str, names: list[str]) -> bool:
        """Delete named entries (files and empty dirs) from the parent."""
        self.remove_calls.append((parent.rstrip("/"), list(names)))
        for name in names:
            target = f"{parent.rstrip('/')}/{name}"
            self.files.pop(target, None)
            # Only delete the directory when nothing remains below it.
            if not any(
                path.startswith(target + "/") for path in self.files
            ) and not any(
                directory.startswith(target + "/")
                for directory in self.dirs
            ):
                self.dirs.discard(target)
        return True

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


class NoopRemoveEmptyAList(CleaningIndexAList):
    """AList double whose remove_empty_directory succeeds but never deletes
    (the observed Quark-via-AList behavior); explicit remove() does delete."""

    def __init__(self, files: dict[str, bytes] | None = None) -> None:
        super().__init__(files)
        self.remove_calls: list[tuple[str, list[str]]] = []

    def remove_empty_dir(self, path: str) -> bool:
        self.remove_empty_calls.append(path.rstrip("/") or "/")
        return True

    def remove(self, parent: str, names: list[str]) -> bool:
        self.remove_calls.append((parent.rstrip("/") or "/", list(names)))
        prefix = parent.rstrip("/") + "/"
        for name in names:
            target = prefix + name
            self.dirs.discard(target)
            for full in list(self.files):
                if full == target or full.startswith(target + "/"):
                    self.files.pop(full, None)
        return True


class PolicyRejectingAList(NoopRemoveEmptyAList):
    """AList double whose remove() enforces the rename-safety policy.

    Full-width punctuation (``：``/``／``) that the provider itself accepted
    at upload time is refused — the observed AListClient behavior — while
    the raw ``call('remove', ...)`` endpoint still deletes by exact name.
    The provider's own guard additionally refuses ``..`` runs on every
    remove path (the observed Quark behavior); renaming the entry to its
    provider-safe spelling first is the only way through.
    """

    def __init__(self, files: dict[str, bytes] | None = None) -> None:
        super().__init__(files)
        self.raw_remove_calls: list[tuple[str, list[str]]] = []
        self.rename_calls: list[tuple[str, str]] = []

    def remove(self, parent: str, names: list[str]) -> bool:
        from engine.scrapeflow.remote_paths import is_provider_safe_basename

        unsafe = [name for name in names if not is_provider_safe_basename(name)]
        if unsafe:
            raise ValueError(f"远端文件名不符合 AList 安全命名规则: {unsafe!r}")
        return super().remove(parent, names)

    def rename(self, full_path: str, new_name: str) -> None:
        parent, _, old_name = full_path.rpartition("/")
        target = f"{parent.rstrip('/')}/{old_name}"
        renamed = f"{parent.rstrip('/')}/{new_name}"
        if target in self.files:
            self.files[renamed] = self.files.pop(target)
        elif target in self.dirs:
            self.dirs.discard(target)
            self.dirs.add(renamed)
            for full in list(self.files):
                if full.startswith(target + "/"):
                    self.files[full.replace(target, renamed, 1)] = self.files.pop(full)
        else:
            raise FileNotFoundError(target)
        self.rename_calls.append((target, new_name))

    def call(self, endpoint: str, body: dict[str, object], *, retryable: bool = False) -> dict[str, object]:
        del retryable
        if endpoint == "rename":
            self.rename(str(body.get("path") or ""), str(body.get("name") or ""))
            return {"code": 200, "message": "success", "data": None}
        if endpoint != "remove":
            raise AssertionError(f"unexpected raw endpoint: {endpoint}")
        parent = str(body.get("dir") or "")
        names = [str(name) for name in (body.get("names") or [])]
        dot_runs = [name for name in names if ".." in name]
        if dot_runs:
            # The provider's own name guard: Quark refuses dot runs.
            raise RuntimeError("AList remove失败: invalid file name")
        self.raw_remove_calls.append((parent.rstrip("/"), list(names)))
        super().remove(parent, names)
        return {"code": 200, "message": "ok"}


class HostileTreeAList(NoopRemoveEmptyAList):
    """AList double that acknowledges every delete but deletes nothing.

    The provider keeps reporting the tree no matter what the engine sends,
    so the walk must stop fail-closed and the residual must surface as the
    job's informational note instead of a silent partial cleanup.
    """

    def remove(self, parent: str, names: list[str]) -> bool:
        self.remove_calls.append((parent.rstrip("/") or "/", list(names)))
        return True


class SourceShellCleanupTests(unittest.TestCase):
    """A completed root's entire intake tree is deleted, residuals included."""

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

    def test_completion_deletes_residual_files_and_whole_source_root(self) -> None:
        """Residual junk (themes/MVs/backup subs) is deleted with the tree.

        Operator ruling 2026-08-27: the intake area is staging — after the
        media is verified in the library the entire source root goes,
        including files the plan never claimed.
        """
        files = {"/incoming/My Show/S01E01.mkv": FAKE_VIDEO_BYTES}
        state_root, alist, runner, executor_events = self._setup(files)
        root_task_id = self._root(runner, "/incoming/My Show")
        original = runner.executor

        def executor(plan):
            result = original(plan)
            # A leftover residual file the plan never claimed.
            alist.files["/incoming/My Show/notes.txt"] = b"junk"
            alist.dirs.add("/incoming/My Show")
            alist.dirs.add("/incoming/My Show/Extras")
            return result

        runner.executor = executor

        final = run_root_pipeline(runner, state_root, root_task_id)

        self.assertEqual(final.phase, "completed")
        self.assertIn("/incoming/My Show/Extras", alist.remove_empty_calls)
        self.assertIn("/incoming/My Show", alist.remove_empty_calls)
        self.assertNotIn("/incoming/My Show", alist.dirs)
        self.assertNotIn("/incoming/My Show/notes.txt", alist.files)

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

    def test_noop_remove_empty_driver_falls_back_to_explicit_remove(self) -> None:
        files = {"/incoming/My Show/S01E01.mkv": FAKE_VIDEO_BYTES}
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        state_root = Path(temp.name)
        alist = NoopRemoveEmptyAList(files)
        executor_events: list[str] = []
        runner = SimpleEngineRunner(
            state_root,
            alist=alist,
            tmdb=_confirming_tmdb(),
            planner=_recording_planner([]),
            validate=False,
            library_root="/library",
            executor=lambda plan: executor_events.append(str(plan.target_root)) or {"ok": True},
        )
        root_task_id = self._root(runner, "/incoming/My Show")

        def original(plan):
            executor_events.append(str(plan.target_root))
            for item in plan.files:
                data = alist.files.pop(item.source_path, None)
                if data is not None:
                    alist.files[f"{item.target_dir.rstrip('/')}/{item.final_name}"] = data
            return {"ok": True}

        def executor(plan):
            result = original(plan)
            alist.dirs.add("/incoming/My Show")
            alist.dirs.add("/incoming/My Show/Extras")
            return result

        runner.executor = executor

        final = run_root_pipeline(runner, state_root, root_task_id)

        self.assertEqual(final.phase, "completed")
        # remove_empty_directory succeeded but deleted nothing: the cleanup
        # must fall back to an explicit verified-empty remove().
        self.assertIn("/incoming/My Show/Extras", alist.remove_empty_calls)
        self.assertIn("/incoming/My Show", alist.remove_empty_calls)
        self.assertEqual(len(alist.remove_calls), 2)
        self.assertNotIn("/incoming/My Show", alist.dirs)
        self.assertNotIn("/incoming/My Show/Extras", alist.dirs)

    def test_provider_unsafe_names_still_delete_via_exact_call(self) -> None:
        """Rename policy must not veto deleting an existing intake name.

        Source releases carry full-width ``：``/``／`` the provider itself
        accepted; the cleanup falls back to the raw exact-name call so one
        such file can no longer abort the whole walk (the Re:Zero failure).
        """
        files = {"/incoming/My Show/S01E01.mkv": FAKE_VIDEO_BYTES}
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        state_root = Path(temp.name)
        alist = PolicyRejectingAList(files)
        executor_events: list[str] = []
        runner = SimpleEngineRunner(
            state_root,
            alist=alist,
            tmdb=_confirming_tmdb(),
            planner=_recording_planner([]),
            validate=False,
            library_root="/library",
            executor=lambda plan: executor_events.append(str(plan.target_root)) or {"ok": True},
        )
        root_task_id = self._root(runner, "/incoming/My Show")

        def executor(plan):
            executor_events.append(str(plan.target_root))
            for item in plan.files:
                data = alist.files.pop(item.source_path, None)
                if data is not None:
                    alist.files[f"{item.target_dir.rstrip('/')}/{item.final_name}"] = data
            # Residuals the plan never claimed: an unsafe-name file, an
            # unsafe-name directory, a dot-run name the provider itself
            # refuses to delete, and a safe sibling behind them all.  The
            # unsafe file name deliberately carries no episode grammar: a
            # 2026-09-05 gate would quarantine an episode-named video
            # instead of deleting it, and this test exercises deletion.
            alist.files["/incoming/My Show/特別映像：序章.mp4"] = b"junk"
            alist.files["/incoming/My Show/外伝／特別篇/safe.txt"] = b"junk"
            alist.files["/incoming/My Show/O. S. T..cue"] = b"junk"
            alist.dirs.add("/incoming/My Show")
            alist.dirs.add("/incoming/My Show/外伝／特別篇")
            return {"ok": True}

        runner.executor = executor

        final = run_root_pipeline(runner, state_root, root_task_id)

        self.assertEqual(final.phase, "completed")
        self.assertIsNone(final.error)
        self.assertNotIn("/incoming/My Show/特別映像：序章.mp4", alist.files)
        self.assertNotIn("/incoming/My Show/外伝／特別篇/safe.txt", alist.files)
        self.assertNotIn("/incoming/My Show/O. S. T..cue", alist.files)
        self.assertNotIn("/incoming/My Show/O. S. T.cue", alist.files)
        self.assertNotIn("/incoming/My Show/外伝／特別篇", alist.dirs)
        self.assertNotIn("/incoming/My Show", alist.dirs)
        # The unsafe names went through the raw exact-name call, the dot-run
        # name went through rename-to-safe + remove, and the safe sibling
        # behind them was still reached (no silent walk abort).
        raw_names = {name for _parent, names in alist.raw_remove_calls for name in names}
        strict_names = {name for _parent, names in alist.remove_calls for name in names}
        self.assertIn("特別映像：序章.mp4", raw_names)
        self.assertIn("外伝／特別篇", raw_names)
        # provider_safe_basename collapses the ``..`` run into ``-``.
        self.assertIn("O. S. T-cue", strict_names)
        self.assertIn(
            ("/incoming/My Show/O. S. T..cue", "O. S. T-cue"),
            alist.rename_calls,
        )

    def test_residual_tree_becomes_job_note_instead_of_silence(self) -> None:
        """A provider that keeps reporting the tree leaves a visible note.

        The phase still completes, but the surviving residual is recorded on
        the job so the operator can re-run the consumption instead of the
        old behavior: a silent partial cleanup nobody notices.
        """
        files = {"/incoming/My Show/S01E01.mkv": FAKE_VIDEO_BYTES}
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        state_root = Path(temp.name)
        alist = HostileTreeAList(files)
        executor_events: list[str] = []
        runner = SimpleEngineRunner(
            state_root,
            alist=alist,
            tmdb=_confirming_tmdb(),
            planner=_recording_planner([]),
            validate=False,
            library_root="/library",
            executor=lambda plan: executor_events.append(str(plan.target_root)) or {"ok": True},
        )
        root_task_id = self._root(runner, "/incoming/My Show")

        def executor(plan):
            executor_events.append(str(plan.target_root))
            for item in plan.files:
                data = alist.files.pop(item.source_path, None)
                if data is not None:
                    alist.files[f"{item.target_dir.rstrip('/')}/{item.final_name}"] = data
            alist.files["/incoming/My Show/notes.txt"] = b"junk"
            alist.dirs.add("/incoming/My Show")
            return {"ok": True}

        runner.executor = executor

        final = run_root_pipeline(runner, state_root, root_task_id)

        self.assertEqual(final.phase, "completed")
        self.assertIsNotNone(final.error)
        self.assertIn("收官清源未完成", final.error)
        self.assertIn("/incoming/My Show/notes.txt", alist.files)

    def test_consume_terminal_source_root_operator_action(self) -> None:
        """The explicit consumption is idempotent and phase-guarded."""
        from local.scrapeflow_api.root_pipeline import consume_terminal_source_root

        files = {"/incoming/My Show/S01E01.mkv": FAKE_VIDEO_BYTES}
        state_root, alist, runner, _executor_events = self._setup(files)
        root_task_id = self._root(runner, "/incoming/My Show")
        final = run_root_pipeline(runner, state_root, root_task_id)
        self.assertEqual(final.phase, "completed")
        self.assertNotIn("/incoming/My Show", alist.dirs)

        # Re-running on an already-consumed root is a clean no-op.
        receipt = consume_terminal_source_root(
            runner, state_root, runner.get_job(root_task_id)
        )
        self.assertFalse(receipt["source_remaining"])
        self.assertIsNone(receipt["note"])
        self.assertEqual(receipt["phase"], "completed")
        self.assertIsNone(runner.get_job(root_task_id).error)

        # A parked root keeps its source until reconciliation resolves.
        parked_files = {"/incoming/Mystery Show/S01E01.mkv": FAKE_VIDEO_BYTES}
        park_state, park_alist, park_runner, _ = self._setup(
            parked_files, tmdb=FakeTMDBClient(),
        )
        parked_root = self._root(park_runner, "/incoming/Mystery Show")
        parked_final = run_root_pipeline(park_runner, park_state, parked_root)
        self.assertEqual(parked_final.phase, "reconciliation_uncertain")
        with self.assertRaises(EngineRequestError):
            consume_terminal_source_root(
                park_runner, park_state, park_runner.get_job(parked_root)
            )
        self.assertIn("/incoming/Mystery Show/S01E01.mkv", park_alist.files)

    def test_pause_blocks_every_remote_delete_boundary(self) -> None:
        """Each remote delete gets its own root-scoped pause checkpoint."""
        from local.scrapeflow_api.root_pipeline import _cleanup_consumed_source_root

        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        state_root = Path(temp.name)
        alist = NoopRemoveEmptyAList({})
        runner = SimpleEngineRunner(
            state_root,
            alist=alist,
            tmdb=_confirming_tmdb(),
            planner=_recording_planner([]),
            validate=False,
            library_root="/library",
            executor=lambda _plan: {"ok": True},
        )
        alist.dirs.update({"/incoming/My Show", "/incoming/My Show/Extras"})
        root_task_id = self._root(runner, "/incoming/My Show")
        checks = {"count": 0}

        def pause_after_first_listing() -> bool:
            checks["count"] += 1
            # The first paused listing leaves the whole tree untouched.
            return checks["count"] >= 1

        _cleanup_consumed_source_root(
            runner,
            state_root,
            root_task_id,
            "/incoming/My Show",
            pause_requested=pause_after_first_listing,
        )

        # Paused at the very first boundary: nothing was deleted at all.
        self.assertEqual(alist.remove_calls, [])
        self.assertEqual(alist.remove_empty_calls, [])
        self.assertIn("/incoming/My Show/Extras", alist.dirs)

if __name__ == "__main__":
    unittest.main()


class UnmappedVideoQuarantineGateTests(unittest.TestCase):
    """The terminal unmapped-video gate (operator ruling 2026-09-05).

    Junk (theme-named, bonus-directory, short without episode grammar) dies
    with the source tree.  Everything the engine cannot prove worthless —
    episode-named videos, content-grade runtimes, unprobeable files — moves
    to /ScrapeFlow/待裁决/<root>/ with a manifest, and a quarantined
    video's sidecar subtitle moves with it.
    """

    def _setup(self, files: dict[str, bytes], durations: dict[str, float] | None = None):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        state_root = Path(temp.name)
        alist = CleaningIndexAList(files)

        def probe_duration(path: str) -> float:
            if path not in alist.files:
                raise FileNotFoundError(path)
            return durations.get(path, 90.0)

        alist.video_duration_probe = probe_duration

        def executor(plan):
            for item in plan.files:
                data = alist.files.pop(item.source_path, None)
                if data is not None:
                    alist.files[f"{item.target_dir.rstrip('/')}/{item.final_name}"] = data
            return {"ok": True}

        runner = SimpleEngineRunner(
            state_root,
            alist=alist,
            tmdb=_confirming_tmdb(),
            planner=_recording_planner([]),
            validate=False,
            library_root="/library",
            executor=executor,
        )
        job = runner.create_root_job(
            intake_source_id("/incoming/My Show"),
            source_path="/incoming/My Show",
            target_shelf="anime",
        )
        started = runner.start_automatic_job(job.id, target_shelf="anime")
        return state_root, alist, runner, started.id

    def test_gate_splits_junk_from_suspects_with_manifest(self) -> None:
        files = {
            # a planned episode anchors the unit; the executor moves it out
            # before terminal consumption, so it never reaches the gate.
            "/incoming/My Show/S01E01.mkv": FAKE_VIDEO_BYTES,
            # junk: theme-named credits
            "/incoming/My Show/NCOP01.mkv": FAKE_VIDEO_BYTES,
            # junk: short and no episode grammar
            "/incoming/My Show/making_of.mkv": FAKE_VIDEO_BYTES,
            # suspect: unaired episode, episode grammar + content runtime
            "/incoming/My Show/第13话 未放送.mkv": FAKE_VIDEO_BYTES,
            # suspect: episode-named even though short (web mini-episode)
            "/incoming/My Show/小剧场SP01.mkv": FAKE_VIDEO_BYTES,
            # sidecar subtitle of the quarantined mini-episode moves too
            "/incoming/My Show/小剧场SP01.zh-Hans.ass": b"subtitle-bytes",
            # junk: unpaired subtitle dies with the tree
            "/incoming/My Show/unrelated.ass": b"other-bytes",
            # non-video residual
            "/incoming/My Show/poster.jpg": b"jpg",
        }
        durations = {
            "/incoming/My Show/NCOP01.mkv": 90.0,
            "/incoming/My Show/making_of.mkv": 180.0,
            "/incoming/My Show/第13话 未放送.mkv": 1420.0,
            "/incoming/My Show/小剧场SP01.mkv": 240.0,
        }
        state_root, alist, runner, root_task_id = self._setup(files, durations)

        final = run_root_pipeline(runner, state_root, root_task_id)

        self.assertEqual(final.phase, "completed")
        # Quarantine is a success outcome: no residual note on the job.
        self.assertIsNone(final.error)
        quarantine = "/library/ScrapeFlow/待裁决/My Show"
        # Suspects landed in quarantine, junk did not.
        self.assertIn(f"{quarantine}/第13话 未放送.mkv", alist.files)
        self.assertIn(f"{quarantine}/小剧场SP01.mkv", alist.files)
        self.assertIn(f"{quarantine}/小剧场SP01.zh-Hans.ass", alist.files)
        for name in ("NCOP01.mkv", "making_of.mkv", "unrelated.ass", "poster.jpg"):
            self.assertNotIn(f"/incoming/My Show/{name}", alist.files)
            self.assertNotIn(f"{quarantine}/{name}", alist.files)
        # The whole source tree is gone.
        self.assertNotIn("/incoming/My Show", alist.dirs)
        # The manifest records every quarantined object with its reason.
        manifest = json.loads(
            alist.files[f"{quarantine}/manifest.json"].decode("utf-8")
        )
        by_path = {
            entry["source_path"]: entry for entry in manifest["entries"]
        }
        self.assertEqual(len(by_path), 3)
        # The manifest carries the real landed size, not a stale zero (the
        # size must come from the post-move target readback).
        self.assertEqual(
            by_path["/incoming/My Show/第13话 未放送.mkv"]["size"],
            len(FAKE_VIDEO_BYTES),
        )
        self.assertIn("正片级时长", by_path["/incoming/My Show/第13话 未放送.mkv"]["reason"])
        self.assertIn(
            "正片命名", by_path["/incoming/My Show/小剧场SP01.mkv"]["reason"]
        )
        self.assertEqual(
            by_path["/incoming/My Show/小剧场SP01.zh-Hans.ass"]["reason"],
            "疑似内容视频的配对字幕",
        )

    def test_unprobeable_video_fails_closed_into_quarantine(self) -> None:
        files = {
            "/incoming/My Show/S01E01.mkv": FAKE_VIDEO_BYTES,
            "/incoming/My Show/mystery.mkv": FAKE_VIDEO_BYTES,
        }
        state_root, alist, runner, root_task_id = self._setup(files)

        def broken_probe(path: str) -> float:
            raise RuntimeError("provider link down")

        alist.video_duration_probe = broken_probe
        final = run_root_pipeline(runner, state_root, root_task_id)

        self.assertEqual(final.phase, "completed")
        quarantine = "/library/ScrapeFlow/待裁决/My Show"
        self.assertIn(f"{quarantine}/mystery.mkv", alist.files)
        manifest = json.loads(
            alist.files[f"{quarantine}/manifest.json"].decode("utf-8")
        )
        (entry,) = manifest["entries"]
        self.assertIn("无法证明", entry["reason"])

    def test_bonus_directory_short_clip_dies_but_episode_runtime_lives(self) -> None:
        files = {
            "/incoming/My Show/S01E01.mkv": FAKE_VIDEO_BYTES,
            "/incoming/My Show/特典/menu_clip.mkv": FAKE_VIDEO_BYTES,
            "/incoming/My Show/特典/未収録エピソード.mkv": FAKE_VIDEO_BYTES,
        }
        durations = {
            "/incoming/My Show/特典/menu_clip.mkv": 60.0,
            # An unaired episode hiding inside a bonus directory: the
            # content-grade runtime trumps the directory context.
            "/incoming/My Show/特典/未収録エピソード.mkv": 1450.0,
        }
        state_root, alist, runner, root_task_id = self._setup(files, durations)

        final = run_root_pipeline(runner, state_root, root_task_id)

        self.assertEqual(final.phase, "completed")
        quarantine = "/library/ScrapeFlow/待裁决/My Show"
        self.assertNotIn(f"{quarantine}/menu_clip.mkv", alist.files)
        self.assertNotIn(f"{quarantine}/特典/menu_clip.mkv", alist.files)
        # Flat layout (2026-09-05): no source-structure mirroring subdirs.
        self.assertIn(f"{quarantine}/未収録エピソード.mkv", alist.files)

    def test_same_basename_collision_never_overwrites(self) -> None:
        """Two suspects with the same basename: first takes the flat slot,
        the second lands in a parent-named disambiguation subdir — an AList
        move never overwrites an earlier quarantine."""
        files = {
            "/incoming/My Show/S01E01.mkv": FAKE_VIDEO_BYTES,
            "/incoming/My Show/SP/sp01.mkv": FAKE_VIDEO_BYTES,
            "/incoming/My Show/特典/sp01.mkv": FAKE_VIDEO_BYTES,
        }
        durations = {
            "/incoming/My Show/SP/sp01.mkv": 1420.0,
            "/incoming/My Show/特典/sp01.mkv": 1430.0,
        }
        state_root, alist, runner, root_task_id = self._setup(files, durations)

        final = run_root_pipeline(runner, state_root, root_task_id)

        self.assertEqual(final.phase, "completed")
        self.assertIsNone(final.error)
        quarantine = "/library/ScrapeFlow/待裁决/My Show"
        # One sp01.mkv at the flat root, the other under its full source
        # parent chain; both survive.
        # One sp01.mkv in the flat slot, the other under a digest-suffixed
        # tagged directory: count files by NAME across the whole quarantine
        # tree instead of pinning exact paths.
        quarantined_files = [
            key for key in alist.files
            if key.startswith(f"{quarantine}/") and key.endswith("sp01.mkv")
        ]
        self.assertEqual(len(quarantined_files), 2, quarantined_files)
        self.assertIn(f"{quarantine}/sp01.mkv", quarantined_files)
        self.assertEqual(sum(1 for k in quarantined_files if k == f"{quarantine}/sp01.mkv"), 1)
        # The manifest records the original relative path of each (both files).
        manifest = json.loads(
            alist.files[f"{quarantine}/manifest.json"].decode("utf-8")
        )
        relatives = {entry["relative_path"] for entry in manifest["entries"]}
        self.assertEqual(
            relatives, {"SP/sp01.mkv", "特典/sp01.mkv"},
        )
        # The flat slot holds one file; the other's tagged dir now carries a
        # digest suffix, so assert by identity of the file name only.
        paths = {entry["quarantine_path"] for entry in manifest["entries"]}
        self.assertIn(f"{quarantine}/sp01.mkv", paths)
        self.assertEqual(len(paths), 2)
        other = next(p for p in paths if p != f"{quarantine}/sp01.mkv")
        self.assertTrue(other.startswith(f"{quarantine}/"))
        self.assertTrue(other.endswith("sp01.mkv"))

    def test_recreated_source_name_disambiguates_from_old_root(self) -> None:
        """A manifest left by a different root under the same folder name
        routes this root's suspects to a short-id-suffixed directory."""
        files = {
            "/incoming/My Show/S01E01.mkv": FAKE_VIDEO_BYTES,
            "/incoming/My Show/第13话 未放送.mkv": FAKE_VIDEO_BYTES,
        }
        durations = {"/incoming/My Show/第13话 未放送.mkv": 1420.0}
        state_root, alist, runner, root_task_id = self._setup(files, durations)
        # A prior root (same folder name, different task) already left a
        # manifest at the plain quarantine directory.
        old_manifest = {
            "schema_version": 1,
            "root_task_id": "engine-000000000000000000000000oldroot",
            "entries": [],
        }
        alist.files["/library/ScrapeFlow/待裁决/My Show/manifest.json"] = json.dumps(
            old_manifest, ensure_ascii=False,
        ).encode("utf-8")

        final = run_root_pipeline(runner, state_root, root_task_id)

        self.assertEqual(final.phase, "completed")
        suffixed = f"/library/ScrapeFlow/待裁决/My Show-{root_task_id[-8:]}"
        self.assertIn(f"{suffixed}/第13话 未放送.mkv", alist.files)
        # The old root's manifest is untouched.
        self.assertIn("/library/ScrapeFlow/待裁决/My Show/manifest.json", alist.files)


class ProvablyAbsentTests(unittest.TestCase):
    """F4 regression: an unprovable listing is never "already consumed"."""

    def _alist(self, files, *, fail_parents=()):
        from local.tests.test_library_index import IndexAList

        class FlakyAList(IndexAList):
            def list(self, path, refresh=False):
                if str(path).rstrip("/") in fail_parents:
                    raise RuntimeError("provider outage")
                return super().list(path, refresh=refresh)

        return FlakyAList(files)

    def test_absent_when_parent_lists_and_name_missing(self) -> None:
        from local.scrapeflow_api.root_pipeline import _provably_absent
        from local.scrapeflow_api.simple_engine_runner import SimpleEngineRunner

        alist = self._alist({"/other/thing.mkv": b"x"})
        runner = SimpleEngineRunner(
            Path(tempfile.mkdtemp()), alist=alist, tmdb=None,
            validate=False, library_root="/library",
        )
        self.assertTrue(_provably_absent(runner, "/incoming/Gone Show"))

    def test_present_when_name_listed(self) -> None:
        from local.scrapeflow_api.root_pipeline import _provably_absent
        from local.scrapeflow_api.simple_engine_runner import SimpleEngineRunner

        alist = self._alist({"/incoming/My Show/S01E01.mkv": b"x"})
        runner = SimpleEngineRunner(
            Path(tempfile.mkdtemp()), alist=alist, tmdb=None,
            validate=False, library_root="/library",
        )
        self.assertFalse(_provably_absent(runner, "/incoming/My Show"))

    def test_provider_outage_is_not_absence(self) -> None:
        from local.scrapeflow_api.root_pipeline import _provably_absent
        from local.scrapeflow_api.simple_engine_runner import SimpleEngineRunner

        alist = self._alist(
            {"/incoming/My Show/S01E01.mkv": b"x"},
            fail_parents={"/incoming"},
        )
        runner = SimpleEngineRunner(
            Path(tempfile.mkdtemp()), alist=alist, tmdb=None,
            validate=False, library_root="/library",
        )
        # The parent cannot be listed: absence is unprovable, so NOT absent.
        self.assertFalse(_provably_absent(runner, "/incoming/My Show"))


class LibraryCoverageScopingTests(unittest.TestCase):
    """The coverage gate's coordinate scoping (audit re-review regressions)."""

    def test_bounded_ancestor_scan_ignores_queue_prefixes(self):
        from local.scrapeflow_api.root_pipeline import _cleanup_consumed_source_root
        from local.scrapeflow_api.simple_engine_runner import SimpleEngineRunner
        from engine.scrapeflow.intake_source import (
            upsert_intake_source, save_intake_catalog, bind_root_task,
        )
        from engine.scrapeflow.work_units import (
            WorkUnitRecord, save_work_unit_records,
        )
        class LibAList(CleaningIndexAList):
            def move(self, parent, target, names):
                for name in names:
                    self.files[f"{target.rstrip('/')}/{name}"] = self.files.pop(
                        f"{parent.rstrip('/')}/{name}"
                    )

        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        state_root = Path(temp.name)
        # The operator's queue prefix `2. 新番合集` must NOT read as season 2:
        # the bare ordinal [05] under a scope with no season marker stays a
        # SUSPECT (quarantine), never junk-deleted against S02E05.
        alist = LibAList({
            "/incoming/2. 新番合集/Some Show/sp/[05].mkv": FAKE_VIDEO_BYTES,
            "/library/番剧/Some Show/Season 02/Show - S02E05 - x.mkv": FAKE_VIDEO_BYTES,
        })
        alist.video_duration_probe = lambda p: 1420.0
        runner = SimpleEngineRunner(
            state_root, alist=alist, tmdb=None, validate=False,
            library_root="/library",
        )
        catalog, entry = upsert_intake_source([], "/incoming/2. 新番合集/Some Show", present=True)
        catalog, _ = bind_root_task(catalog, entry.source_id, "root-q")
        save_intake_catalog(state_root, catalog)
        save_work_unit_records(state_root, "root-q", [
            WorkUnitRecord(
                work_unit_id="unit-q",
                root_task_id="root-q",
                boundary_key="/incoming/2. 新番合集/Some Show",
                source_paths=("/incoming/2. 新番合集/Some Show",),
                source_revision=1,
                role="single_work",
                display_label="Some Show",
            ),
        ])
        import local.scrapeflow_api.unit_execution as ue
        ue_rows = [ue.WorkAcceptanceResult(
            work_unit_id="unit-q", outcome="accepted",
            writer_job_id="job-q", phase="executed",
            target_root="/library/番剧/Some Show", planned_files=1,
            error=None, recorded_at="t",
        )]
        ue.save_work_acceptance(state_root, "root-q", ue_rows)
        receipt = _cleanup_consumed_source_root(
            runner, state_root, "root-q",
            "/incoming/2. 新番合集/Some Show",
            pause_requested=lambda: False,
        )
        # The queue prefix is ABOVE the owning scope: no season derives, the
        # file is an unmapped suspect → quarantined, never junk-deleted.
        self.assertEqual(receipt.get("quarantined_count"), 1, receipt)
        self.assertEqual(receipt.get("source_remaining"), False)


if __name__ == "__main__":
    unittest.main()
