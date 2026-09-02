"""Tests for F/G/H composition: unit-driven planning, single-writer execution,
and typed acceptance."""

from __future__ import annotations

from dataclasses import replace
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from engine.scrapeflow.errors import PlanError
from engine.scrapeflow.core import _tv_season_resource_gaps
from engine.scrapeflow.current_plan import plan_to_dict
from engine.scrapeflow.gap_ledger import load_gap_ledger, save_gap_ledger
from engine.scrapeflow.models import Plan, PlannedFile
from engine.scrapeflow.replenishment_matching import audit_episode_tokens
from engine.scrapeflow.root_boundaries import analyze_root_boundaries
from engine.scrapeflow.serialization import atomic_write_json
from engine.scrapeflow.unit_identity import (
    apply_work_unit_override,
    resolve_work_unit_identities,
)
from engine.scrapeflow.work_units import load_work_unit_records, save_work_unit_records

from local.scrapeflow_api.library_index import reconcile_root_work_units
from local.scrapeflow_api.tmdb_episode_catalog import TmdbEpisodeCatalog
from local.scrapeflow_api.simple_engine_runner import (
    EnginePauseRequested,
    SimplePlanExecutor,
    SimpleEngineRunner,
    recover_persisted_engine_jobs,
)
from local.scrapeflow_api.unit_execution import (
    GapDiscoveryAttention,
    _register_unit_episode_gaps,
    _request_for_unit,
    _unit_owns_tv_root_scope,
    _complete_unit_episode_gap_registration,
    execute_new_work_units,
    load_work_acceptance,
    rereview_executed_unit_gaps,
)

from local.tests.test_library_index import (
    IndexAList,
    StrictBareEpisodeTMDB,
    _nfo_tv,
)
from local.tests.test_simple_engine_runner import (
    FAKE_VIDEO_BYTES,
    FAKE_VIDEO_SIZE,
    FakeAList,
)


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


class MultiIdentityTMDB:
    """TMDB double for a main TV identity plus a separate TV child."""

    def __init__(self, catalogs: dict[int, dict[int, int]]) -> None:
        self.catalogs = catalogs

    def get(self, path: str, **params: object) -> dict:
        del params
        for tmdb_id, seasons in self.catalogs.items():
            if path == f"/tv/{tmdb_id}":
                return {
                    "seasons": [
                        {"season_number": season, "name": f"Season {season}"}
                        for season in seasons
                    ],
                }
            for season, count in seasons.items():
                if path == f"/tv/{tmdb_id}/season/{season}":
                    return {
                        "episodes": [
                            {
                                "episode_number": number,
                                "air_date": "2020-01-01",
                            }
                            for number in range(1, count + 1)
                        ],
                    }
        return {}


class BareEpisodePlanningTMDB:
    """One complete published regular season used to prove the D→F→J bridge."""

    def __init__(
        self,
        tmdb_id: int,
        episode_count: int,
        *,
        specials: int = 0,
    ) -> None:
        self.tmdb_id = tmdb_id
        self.episode_count = episode_count
        self.specials = specials

    def get(self, path: str, **_params: object) -> dict[str, object]:
        if path == f"/tv/{self.tmdb_id}":
            return {
                "name": "One Season Show",
                "original_name": "One Season Show",
                "first_air_date": "2020-01-01",
                "number_of_seasons": 1,
                "number_of_episodes": self.episode_count,
                "seasons": (
                    ([{
                        "season_number": 0,
                        "episode_count": self.specials,
                        "name": "Specials",
                    }] if self.specials else [])
                    + [{
                        "season_number": 1,
                        "episode_count": self.episode_count,
                        "name": "Season 1",
                    }]
                ),
            }
        if path == f"/tv/{self.tmdb_id}/season/1":
            return {
                "episodes": [
                    {
                        "episode_number": episode,
                        "air_date": "2020-01-01",
                        "name": f"Episode {episode}",
                    }
                    for episode in range(1, self.episode_count + 1)
                ],
            }
        if path == f"/tv/{self.tmdb_id}/season/0":
            return {
                "episodes": [
                    {
                        "episode_number": episode,
                        "air_date": "2020-01-01",
                        "name": f"Special {episode}",
                    }
                    for episode in range(1, self.specials + 1)
                ],
            }
        return {}


class BareEpisodePlanningAList(IndexAList):
    """Index double with the read-only walk surface used by the real planner."""

    def try_list(self, path: str, refresh: bool = False) -> list[dict[str, object]]:
        return self.list(path, refresh=refresh)

    def walk(self, path: str, **_kwargs: object) -> list[dict[str, object]]:
        prefix = path.rstrip("/") + "/"
        return [
            {
                "name": full_path.rsplit("/", 1)[-1],
                "full_path": full_path,
                "size": len(payload),
                "is_dir": False,
            }
            for full_path, payload in sorted(self.files.items())
            if full_path.startswith(prefix)
        ]


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

    def test_disc_image_stale_new_work_record_cannot_reach_planner_or_writer(self) -> None:
        """F repeats the B/C barrier even if persisted state was stale/forged."""
        source = "/incoming/disc"
        state_root, alist, runner, planner_events, executor_events = self._setup({
            f"{source}/Season 01.iso": b"i" * (1024 * 1024),
        })
        root_task_id = "root-disc-f"
        pending = runner.create_pending_job(source, job_id=root_task_id)
        runner.start_automatic_job(pending.id, target_shelf="anime")
        analyze_root_boundaries(
            alist, source, root_task_id=root_task_id, state_root=state_root,
        )
        record = load_work_unit_records(state_root, root_task_id)[0]
        self.assertTrue(record.requires_content_expansion)
        # Simulate a legacy state written before the B/C ISO gate existed.
        stale = replace(
            record,
            requires_content_expansion=False,
            identity_status="confirmed",
            identity={"media_type": "tv", "tmdb_id": 101},
            reconciliation_outcome="new_work",
            attention=None,
        )
        save_work_unit_records(state_root, root_task_id, [stale])

        results = execute_new_work_units(runner, state_root, root_task_id)

        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].outcome, "skipped")
        self.assertIn("光盘镜像", results[0].error or "")
        self.assertEqual(planner_events, [])
        self.assertEqual(executor_events, [])
        parked = load_work_unit_records(state_root, root_task_id)[0]
        self.assertTrue(parked.requires_content_expansion)
        self.assertEqual(parked.identity_status, "uncertain")
        self.assertIsNone(parked.reconciliation_outcome)

    def test_explicit_chinese_scope_season_reaches_planner_request(self) -> None:
        """A season-directory source keeps its B/W season when F builds a request.

        The smart planner receives paths relative to ``source_path``.  When
        that path is itself ``第二季``, the parent marker is no longer present
        in the relative filename, so F must carry the independently proved
        scope season instead of falling back to EngineRequest's Season 01
        default.
        """
        source = "/incoming/某剧第二季"
        files = {
            f"{source}/[{episode:02d}].mkv": FAKE_VIDEO_BYTES
            for episode in range(1, 3)
        }
        state_root, alist, runner, _planner_events, _executor_events = self._setup(
            files,
            tmdb=MultiSeasonTMDB(91001, {1: 2, 2: 2}),
        )
        root_task_id = "root-explicit-cjk-season"
        pending = runner.create_pending_job(source, job_id=root_task_id)
        runner.start_automatic_job(pending.id, target_shelf="anime")
        analyze_root_boundaries(
            alist, source, root_task_id=root_task_id, state_root=state_root,
        )
        record = load_work_unit_records(state_root, root_task_id)[0]
        apply_work_unit_override(
            state_root,
            root_task_id,
            record.work_unit_id,
            media_type="tv",
            tmdb_id=91001,
        )
        record = load_work_unit_records(state_root, root_task_id)[0]

        request = _request_for_unit(runner, record, root_task_id, state_root)

        self.assertEqual(request.season, 2)
        self.assertEqual(request.source_path, source)

    def test_explicit_scope_season_rejects_conflicting_file_marker(self) -> None:
        """A contradictory SxxExx marker cannot silently choose a season."""
        source = "/incoming/某剧第二季"
        files = {f"{source}/某剧.S03E01.mkv": FAKE_VIDEO_BYTES}
        state_root, alist, runner, _planner_events, _executor_events = self._setup(
            files,
            tmdb=MultiSeasonTMDB(91002, {1: 1, 2: 1}),
        )
        root_task_id = "root-conflicting-scope-season"
        pending = runner.create_pending_job(source, job_id=root_task_id)
        runner.start_automatic_job(pending.id, target_shelf="anime")
        analyze_root_boundaries(
            alist, source, root_task_id=root_task_id, state_root=state_root,
        )
        record = load_work_unit_records(state_root, root_task_id)[0]
        apply_work_unit_override(
            state_root,
            root_task_id,
            record.work_unit_id,
            media_type="tv",
            tmdb_id=91002,
        )
        record = load_work_unit_records(state_root, root_task_id)[0]

        with self.assertRaisesRegex(ValueError, "来源文件显式季号与目录季号冲突"):
            _request_for_unit(runner, record, root_task_id, state_root)

        self.assertEqual(alist.move_calls, [])

    def test_explicit_scope_season_ignores_s00_specials_bucket(self) -> None:
        """S00 specials beside S01 are one season, not a boundary conflict."""
        source = "/incoming/某剧第一季"
        files = {
            f"{source}/某剧.S01E01.mkv": FAKE_VIDEO_BYTES,
            f"{source}/某剧.S00E02.mkv": FAKE_VIDEO_BYTES,
        }
        state_root, alist, runner, _planner_events, _executor_events = self._setup(
            files,
            tmdb=MultiSeasonTMDB(91003, {1: 1}),
        )
        root_task_id = "root-s00-specials-not-conflict"
        pending = runner.create_pending_job(source, job_id=root_task_id)
        runner.start_automatic_job(pending.id, target_shelf="anime")
        analyze_root_boundaries(
            alist, source, root_task_id=root_task_id, state_root=state_root,
        )
        record = load_work_unit_records(state_root, root_task_id)[0]
        apply_work_unit_override(
            state_root,
            root_task_id,
            record.work_unit_id,
            media_type="tv",
            tmdb_id=91003,
        )
        record = load_work_unit_records(state_root, root_task_id)[0]

        request = _request_for_unit(runner, record, root_task_id, state_root)

        self.assertEqual(request.season, 1)

    def test_named_season_window_reaches_planner_request(self) -> None:
        """C's officially proved named-season window becomes the F season.

        A continuation arc ships as its own release package years after the
        parent premiered (``某剧完结篇`` = the parent's Season 2 airing in
        the boundary year).  C records the officially named season window in
        the identity's decision trace; without carrying it into the request,
        F would fall back to EngineRequest's Season 01 default and map the
        release-local ordinals onto the wrong season.
        """
        source = "/incoming/某剧完结篇（2009）全26集"
        files = {
            f"{source}/01「第一话」.mkv": FAKE_VIDEO_BYTES,
            f"{source}/02「第二话」.mkv": FAKE_VIDEO_BYTES,
        }
        state_root, alist, runner, _planner_events, _executor_events = self._setup(
            files,
            tmdb=MultiSeasonTMDB(91004, {1: 12, 2: 2}),
        )
        root_task_id = "root-named-season-window"
        pending = runner.create_pending_job(source, job_id=root_task_id)
        runner.start_automatic_job(pending.id, target_shelf="anime")
        analyze_root_boundaries(
            alist, source, root_task_id=root_task_id, state_root=state_root,
        )
        records = load_work_unit_records(state_root, root_task_id)
        records[0] = replace(
            records[0],
            identity_status="confirmed",
            identity={
                "media_type": "tv",
                "tmdb_id": 91004,
                "title": "某剧",
                "year": "2000",
                "confidence": 1.0,
                "decision_trace": {
                    "season_window_season": 2,
                    "season_window_year": "2009",
                    "season_window_name": "某剧:完结篇",
                },
            },
            reconciliation_outcome="new_work",
        )
        save_work_unit_records(state_root, root_task_id, records)

        request = _request_for_unit(runner, records[0], root_task_id, state_root)

        self.assertEqual(request.season, 2)
        self.assertEqual(request.source_path, source)

    def test_named_season_window_conflicts_with_scope_season_visibly(self) -> None:
        """A directory season marker disagreeing with the window is an error."""
        source = "/incoming/某剧第二季"
        files = {
            f"{source}/01「第一话」.mkv": FAKE_VIDEO_BYTES,
            f"{source}/02「第二话」.mkv": FAKE_VIDEO_BYTES,
        }
        state_root, alist, runner, _planner_events, _executor_events = self._setup(
            files,
            tmdb=MultiSeasonTMDB(91005, {1: 12, 2: 2, 3: 2}),
        )
        root_task_id = "root-window-scope-conflict"
        pending = runner.create_pending_job(source, job_id=root_task_id)
        runner.start_automatic_job(pending.id, target_shelf="anime")
        analyze_root_boundaries(
            alist, source, root_task_id=root_task_id, state_root=state_root,
        )
        records = load_work_unit_records(state_root, root_task_id)
        records[0] = replace(
            records[0],
            identity_status="confirmed",
            identity={
                "media_type": "tv",
                "tmdb_id": 91005,
                "title": "某剧",
                "year": "2000",
                "confidence": 1.0,
                "decision_trace": {
                    "season_window_season": 3,
                    "season_window_year": "2009",
                    "season_window_name": "某剧:完结篇",
                },
            },
            reconciliation_outcome="new_work",
        )
        save_work_unit_records(state_root, root_task_id, records)

        with self.assertRaisesRegex(ValueError, "来源目录显式季号与身份命名季窗口冲突"):
            _request_for_unit(runner, records[0], root_task_id, state_root)

        self.assertEqual(alist.move_calls, [])

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

    def test_planner_missing_season_emits_a_structured_season_coordinate(self) -> None:
        plan = Plan(
            mode="tv",
            source_root="/incoming/Example",
            target_root="/library/欧美剧/Example",
            files=[],
            warnings=[],
            metadata={},
        )

        gaps = _tv_season_resource_gaps(
            BareEpisodePlanningAList({}),
            plan,
            series_dir="/library/欧美剧/Example",
            official_seasons=[{
                "season_number": 4,
                "name": "Fourth Season",
                "air_date": "2000-01-01",
                "episode_count": 10,
            }],
        )

        self.assertEqual(len(gaps), 1)
        self.assertEqual(gaps[0]["season"], 4)
        self.assertEqual(gaps[0]["label"], "Season 04 Fourth Season")
        self.assertEqual(gaps[0]["expected_episode_count"], 10)

    def test_subtitle_only_merge_derives_season_ownership_from_the_merged_root(self) -> None:
        """纯字幕 merge 车道的季所有权来自归并目标根的既有集号。"""
        source = "/incoming/rezero"
        state_root, alist, runner, _p, _e = self._setup(
            {
                f"{source}/VCB [51].mkv": FAKE_VIDEO_BYTES,
                f"{source}/VCB [51].CHS.ass": b"[Script Info]",
            },
            library_files={
                "/library/番剧/Example/Season 01/Show - S01E01 - Title.mkv":
                    FAKE_VIDEO_BYTES,
            },
            tmdb=MultiSeasonTMDB(101, {1: 2}),
        )
        root_task_id = "root-subtitle-only-merge"
        pending = runner.create_pending_job(source, job_id=root_task_id)
        runner.start_automatic_job(pending.id, target_shelf="anime")
        analyze_root_boundaries(
            alist, source, root_task_id=root_task_id, state_root=state_root,
        )
        record = replace(
            load_work_unit_records(state_root, root_task_id)[0],
            media_context="tv",
            identity={"media_type": "tv", "tmdb_id": 101},
            reconciliation_outcome="merge_existing",
        )
        executed_plan = {
            "files": [{
                "final_name": "Show - S01E01 - Title.zh-CN.ass",
                "media_kind": "subtitle",
            }],
            "target_root": "/library/番剧/Example",
            "scan_report": {},
        }

        # Before the fix this raised GapDiscoveryAttention: the plan carries
        # no video row, the bracketed source name proves no season, and the
        # merge tokens only reached the coverage set, never the ownership.
        _register_unit_episode_gaps(
            runner, state_root, root_task_id, record, executed_plan,
        )
        self.assertEqual(
            {
                gap.gap_id.rsplit("::", 1)[1]
                for gap in load_gap_ledger(state_root, root_task_id)
            },
            {"S01E02"},
        )

    def test_planner_missing_season_registers_exact_structured_and_legacy_coordinates(self) -> None:
        """J accepts the current field and only the old canonical fallback."""
        for legacy in (False, True):
            with self.subTest(legacy=legacy):
                source = "/incoming/one"
                state_root, alist, runner, _p, _e = self._setup(
                    {f"{source}/Example/S01E01.mkv": FAKE_VIDEO_BYTES},
                    tmdb=MultiSeasonTMDB(101, {1: 1, 4: 2}),
                )
                root_task_id = f"root-missing-season-{'legacy' if legacy else 'structured'}"
                pending = runner.create_pending_job(source, job_id=root_task_id)
                runner.start_automatic_job(pending.id, target_shelf="anime")
                analyze_root_boundaries(
                    alist, source, root_task_id=root_task_id, state_root=state_root,
                )
                record = replace(
                    load_work_unit_records(state_root, root_task_id)[0],
                    media_context="tv",
                    identity={"media_type": "tv", "tmdb_id": 101},
                )
                resource_gap = {
                    "kind": "missing_season",
                    "label": "Season 04 Fourth Season",
                    "reason": "planner proved no source or formal-library video",
                    "files": [],
                    "season_name": "Fourth Season",
                    "expected_episode_count": 2,
                }
                if not legacy:
                    resource_gap["season"] = 4
                executed_plan = {
                    "files": [{"final_name": "S01E01.mkv", "media_kind": "video"}],
                    "target_root": "/library/番剧/Example",
                    "scan_report": {"resource_gaps": [resource_gap]},
                }

                _register_unit_episode_gaps(
                    runner, state_root, root_task_id, record, executed_plan,
                )

                self.assertEqual(
                    {
                        gap.gap_id.rsplit("::", 1)[1]
                        for gap in load_gap_ledger(state_root, root_task_id)
                    },
                    {"S04E01", "S04E02"},
                )

    def test_planner_missing_season_rejects_a_noncanonical_legacy_label(self) -> None:
        source = "/incoming/one"
        state_root, alist, runner, _p, _e = self._setup(
            {f"{source}/Example/S01E01.mkv": FAKE_VIDEO_BYTES},
            tmdb=MultiSeasonTMDB(101, {1: 1, 4: 2}),
        )
        root_task_id = "root-missing-season-malformed"
        pending = runner.create_pending_job(source, job_id=root_task_id)
        runner.start_automatic_job(pending.id, target_shelf="anime")
        analyze_root_boundaries(
            alist, source, root_task_id=root_task_id, state_root=state_root,
        )
        record = replace(
            load_work_unit_records(state_root, root_task_id)[0],
            media_context="tv",
            identity={"media_type": "tv", "tmdb_id": 101},
        )
        executed_plan = {
            "files": [{"final_name": "S01E01.mkv", "media_kind": "video"}],
            "target_root": "/library/番剧/Example",
            "scan_report": {"resource_gaps": [{
                "kind": "missing_season",
                "label": "Season 4 Fourth Season",
                "reason": "planner proved no source or formal-library video",
                "files": [],
                "season_name": "Fourth Season",
                "expected_episode_count": 2,
            }]},
        }

        with self.assertRaises(GapDiscoveryAttention):
            _register_unit_episode_gaps(
                runner, state_root, root_task_id, record, executed_plan,
            )
        self.assertEqual(load_gap_ledger(state_root, root_task_id), [])

    def test_planner_missing_in_progress_season_registers_only_published_prefix(self) -> None:
        class PartiallyPublishedTMDB:
            def get(self, path: str, **_params: object) -> dict[str, object]:
                if path == "/tv/101":
                    return {"seasons": [
                        {"season_number": 1, "name": "Season 1"},
                        {"season_number": 4, "name": "Season 4"},
                    ]}
                if path == "/tv/101/season/1":
                    return {"episodes": [{"episode_number": 1, "air_date": "2000-01-01"}]}
                if path == "/tv/101/season/4":
                    return {"episodes": [
                        {"episode_number": 1, "air_date": "2000-01-01"},
                        *[
                            {"episode_number": episode, "air_date": "2099-01-01"}
                            for episode in range(2, 11)
                        ],
                    ]}
                return {}

        source = "/incoming/one"
        state_root, alist, runner, _p, _e = self._setup(
            {f"{source}/Example/S01E01.mkv": FAKE_VIDEO_BYTES},
            tmdb=PartiallyPublishedTMDB(),
        )
        root_task_id = "root-missing-season-in-progress"
        pending = runner.create_pending_job(source, job_id=root_task_id)
        runner.start_automatic_job(pending.id, target_shelf="anime")
        analyze_root_boundaries(
            alist, source, root_task_id=root_task_id, state_root=state_root,
        )
        record = replace(
            load_work_unit_records(state_root, root_task_id)[0],
            media_context="tv",
            identity={"media_type": "tv", "tmdb_id": 101},
        )
        executed_plan = {
            "files": [{"final_name": "S01E01.mkv", "media_kind": "video"}],
            "target_root": "/library/番剧/Example",
            "scan_report": {"resource_gaps": [{
                "kind": "missing_season",
                "season": 4,
                "label": "Season 04 Season 4",
                "reason": "planner proved no source or formal-library video",
                "files": [],
                "season_name": "Season 4",
                "expected_episode_count": 10,
            }]},
        }

        _register_unit_episode_gaps(
            runner, state_root, root_task_id, record, executed_plan,
        )

        self.assertEqual(
            {
                gap.gap_id.rsplit("::", 1)[1]
                for gap in load_gap_ledger(state_root, root_task_id)
            },
            {"S04E01"},
        )

    def test_ordinary_retry_keeps_registered_j_without_a_second_catalog_read(self) -> None:
        source = "/incoming/one"
        state_root, alist, runner, _planner_events, executor_events = self._setup(
            {f"{source}/Example/S01E01.mkv": FAKE_VIDEO_BYTES},
            tmdb=CatalogTMDB(101, 1),
        )
        root_task_id = "root-ordinary-j-retry"
        pending = runner.create_pending_job(source, job_id=root_task_id)
        runner.start_automatic_job(pending.id, target_shelf="anime")
        analyze_root_boundaries(
            alist, source, root_task_id=root_task_id, state_root=state_root,
        )
        record = load_work_unit_records(state_root, root_task_id)[0]
        apply_work_unit_override(
            state_root, root_task_id, record.work_unit_id,
            media_type="tv", tmdb_id=101,
        )
        reconcile_root_work_units(alist, "/library", state_root, root_task_id)
        execute_new_work_units(runner, state_root, root_task_id)
        self.assertEqual(
            load_work_unit_records(state_root, root_task_id)[0].gap_status,
            "registered",
        )

        with patch(
            "local.scrapeflow_api.unit_execution._register_unit_episode_gaps",
            side_effect=AssertionError("ordinary retry must not repeat J"),
        ) as registered:
            execute_new_work_units(runner, state_root, root_task_id)

        registered.assert_not_called()
        self.assertEqual(len(executor_events), 1)

    def test_explicit_rereview_materializes_planner_gap_without_replanning_or_writing(self) -> None:
        source = "/incoming/one"
        state_root, alist, runner, planner_events, executor_events = self._setup(
            {f"{source}/Example/S01E01.mkv": FAKE_VIDEO_BYTES},
            tmdb=MultiSeasonTMDB(101, {1: 1, 4: 2}),
        )
        base_planner = _recording_planner(planner_events)

        def planner(request, alist_port, tmdb_port):
            plan = base_planner(request, alist_port, tmdb_port)
            plan.scan_report["resource_gaps"] = [{
                "kind": "missing_season",
                "label": "Season 04 Fourth Season",
                "reason": "planner proved no source or formal-library video",
                "files": [],
                "season_name": "Fourth Season",
                "expected_episode_count": 2,
            }]
            return plan

        runner.planner = planner
        root_task_id = "root-explicit-j-rereview"
        pending = runner.create_pending_job(source, job_id=root_task_id)
        runner.start_automatic_job(pending.id, target_shelf="anime")
        analyze_root_boundaries(
            alist, source, root_task_id=root_task_id, state_root=state_root,
        )
        record = load_work_unit_records(state_root, root_task_id)[0]
        apply_work_unit_override(
            state_root, root_task_id, record.work_unit_id,
            media_type="tv", tmdb_id=101,
        )
        reconcile_root_work_units(alist, "/library", state_root, root_task_id)
        execute_new_work_units(runner, state_root, root_task_id)
        # Simulate the historical false "registered" outcome before this
        # bridge existed.  The executed carrier remains the sole evidence.
        save_gap_ledger(state_root, root_task_id, [])
        before_plans = len(planner_events)
        before_writes = len(executor_events)

        rereviewed = rereview_executed_unit_gaps(runner, state_root, root_task_id)

        self.assertEqual(rereviewed[0].gap_status, "registered")
        self.assertEqual(len(planner_events), before_plans)
        self.assertEqual(len(executor_events), before_writes)
        self.assertEqual(
            {
                gap.gap_id.rsplit("::", 1)[1]
                for gap in load_gap_ledger(state_root, root_task_id)
            },
            {"S04E01", "S04E02"},
        )

    def test_complete_tv_root_registers_s00_and_regular_gaps(self) -> None:
        source = "/incoming/Oshi Root"
        files = {
            f"{source}/Show [{episode:02d}].mkv": FAKE_VIDEO_BYTES
            for episode in range(1, 25)
        }
        state_root, alist, runner, _planner_events, _executor_events = self._setup(
            files, tmdb=MultiSeasonTMDB(203737, {0: 2, 1: 35}),
        )
        root_task_id = "root-s00-full"
        pending = runner.create_pending_job(source, job_id=root_task_id)
        runner.start_automatic_job(pending.id, target_shelf="anime")
        analyze_root_boundaries(
            alist, source, root_task_id=root_task_id, state_root=state_root,
        )
        record = load_work_unit_records(state_root, root_task_id)[0]
        record = replace(
            record,
            media_context="tv",
            identity_status="confirmed",
            identity={"media_type": "tv", "tmdb_id": 203737},
        )
        save_work_unit_records(state_root, root_task_id, [record])
        executed_plan = {
            "files": [
                {"final_name": f"S01E{episode:02d}.mkv", "media_kind": "video"}
                for episode in range(1, 25)
            ],
            "target_root": "/library/番剧/Oshi Root",
        }
        _register_unit_episode_gaps(runner, state_root, root_task_id, record, executed_plan)
        from engine.scrapeflow.gap_ledger import load_gap_ledger
        tokens = {gap.gap_id.rsplit("::", 1)[1] for gap in load_gap_ledger(state_root, root_task_id)}
        self.assertEqual(
            tokens,
            {"S00E01", "S00E02"} | {f"S01E{episode:02d}" for episode in range(25, 36)},
        )
        _register_unit_episode_gaps(runner, state_root, root_task_id, record, executed_plan)
        self.assertEqual(len(load_gap_ledger(state_root, root_task_id)), 13)

    def test_j_parks_when_whole_directory_plan_lacks_b_snapshot(self) -> None:
        source = "/incoming/Missing B Snapshot"
        state_root, alist, runner, _planner_events, _executor_events = self._setup(
            {f"{source}/S01E01.mkv": FAKE_VIDEO_BYTES},
            tmdb=MultiSeasonTMDB(203736, {1: 3}),
        )
        root_task_id = "root-j-missing-b-snapshot"
        pending = runner.create_pending_job(source, job_id=root_task_id)
        runner.start_automatic_job(pending.id, target_shelf="anime")
        analyze_root_boundaries(
            alist, source, root_task_id=root_task_id, state_root=state_root,
        )
        record = load_work_unit_records(state_root, root_task_id)[0]
        record = replace(
            record,
            media_context="tv",
            identity_status="confirmed",
            identity={"media_type": "tv", "tmdb_id": 203736},
            claimed_seasons=(1,),
        )
        save_work_unit_records(state_root, root_task_id, [record])
        (state_root / f"work_snapshot_{root_task_id}.json").unlink()

        revised = _complete_unit_episode_gap_registration(
            runner,
            state_root,
            root_task_id,
            record,
            {"files": [], "target_root": "/library/番剧/Missing B Snapshot"},
        )
        self.assertEqual(revised.gap_status, "attention")
        self.assertIn("来源快照", revised.gap_detail or "")

    def test_exact_tv_root_does_not_own_s00_when_work_unit_ledger_fails(self) -> None:
        source = "/incoming/Unreadable Root"
        state_root, alist, runner, _planner_events, _executor_events = self._setup(
            {f"{source}/S01E01.mkv": FAKE_VIDEO_BYTES},
            tmdb=MultiSeasonTMDB(203739, {0: 2, 1: 1}),
        )
        root_task_id = "root-s00-unreadable-ledger"
        pending = runner.create_pending_job(source, job_id=root_task_id)
        runner.start_automatic_job(pending.id, target_shelf="anime")
        analyze_root_boundaries(
            alist, source, root_task_id=root_task_id, state_root=state_root,
        )
        record = replace(
            load_work_unit_records(state_root, root_task_id)[0],
            media_context="tv",
            identity_status="confirmed",
            identity={"media_type": "tv", "tmdb_id": 203739},
        )

        with patch(
            "local.scrapeflow_api.unit_execution.load_work_unit_records",
            side_effect=OSError("corrupt ledger"),
        ):
            self.assertFalse(
                _unit_owns_tv_root_scope(
                    runner, state_root, root_task_id, record,
                )
            )

    def test_exact_tv_root_does_not_own_s00_when_sibling_overlaps(self) -> None:
        source = "/incoming/Overlapping Root"
        state_root, alist, runner, _planner_events, _executor_events = self._setup(
            {f"{source}/S01E01.mkv": FAKE_VIDEO_BYTES},
            tmdb=MultiSeasonTMDB(203740, {0: 2, 1: 1}),
        )
        root_task_id = "root-s00-overlapping-ledger"
        pending = runner.create_pending_job(source, job_id=root_task_id)
        runner.start_automatic_job(pending.id, target_shelf="anime")
        analyze_root_boundaries(
            alist, source, root_task_id=root_task_id, state_root=state_root,
        )
        root_record = replace(
            load_work_unit_records(state_root, root_task_id)[0],
            media_context="tv",
            identity_status="confirmed",
            identity={"media_type": "tv", "tmdb_id": 203740},
        )
        sibling = replace(
            root_record,
            work_unit_id="nested-sibling",
            boundary_key=f"{source}/Sibling",
            source_paths=(f"{source}/Sibling",),
            identity={"media_type": "tv", "tmdb_id": 203741, "season": 1},
            claimed_seasons=(1,),
        )
        save_work_unit_records(
            state_root, root_task_id, [root_record, sibling],
        )

        self.assertFalse(
            _unit_owns_tv_root_scope(
                runner, state_root, root_task_id, root_record,
            )
        )

    def test_claimed_multiseason_without_distinct_season_scopes_does_not_own_s00(self) -> None:
        source = "/incoming/Stale Multiseason Claim"
        state_root, alist, runner, _planner_events, _executor_events = self._setup(
            {f"{source}/Season 01/S01E01.mkv": FAKE_VIDEO_BYTES},
            tmdb=MultiIdentityTMDB({
                203742: {0: 2, 1: 1, 2: 1},
                203743: {0: 1, 1: 1},
            }),
        )
        root_task_id = "root-s00-stale-multiseason-claim"
        pending = runner.create_pending_job(source, job_id=root_task_id)
        runner.start_automatic_job(pending.id, target_shelf="us_tv")
        analyze_root_boundaries(
            alist, source, root_task_id=root_task_id, state_root=state_root,
        )
        template = load_work_unit_records(state_root, root_task_id)[0]
        main = replace(
            template,
            work_unit_id="stale-main",
            boundary_key=f"{source}/@generic-season-root",
            source_paths=(f"{source}/Season 01",),
            media_context="tv",
            identity_status="confirmed",
            identity={"media_type": "tv", "tmdb_id": 203742},
            claimed_seasons=(1, 2),
        )
        child = replace(
            template,
            work_unit_id="single-season-child",
            boundary_key=f"{source}/Child",
            source_paths=(f"{source}/Child",),
            media_context="tv",
            identity_status="confirmed",
            identity={"media_type": "tv", "tmdb_id": 203743, "season": 1},
            claimed_seasons=(1,),
        )
        save_work_unit_records(state_root, root_task_id, [main, child])

        self.assertFalse(
            _unit_owns_tv_root_scope(
                runner, state_root, root_task_id, main,
            )
        )

    def test_multi_season_main_tv_owns_s00_but_single_season_child_does_not(self) -> None:
        source = "/incoming/瑞克和MD 1-9季+日漫版 内封+内嵌字幕 4K+1080P"
        season_names = ("一", "二", "三", "四", "五", "六", "七", "八", "九")
        files = {
            f"{source}/第{name}季（20{season:02d}）全1集 内封字幕/"
            f"S{season:02d}E01.mkv": FAKE_VIDEO_BYTES
            for season, name in enumerate(season_names, 1)
        }
        anime_scope = f"{source}/瑞克和莫蒂：日漫版（2024）全10集"
        files.update({
            f"{anime_scope}/S01E{episode:02d}.mkv": FAKE_VIDEO_BYTES
            for episode in range(1, 11)
        })
        state_root, alist, runner, _planner_events, _executor_events = self._setup(
            files,
            tmdb=MultiIdentityTMDB({
                60625: {0: 37, **{season: 1 for season in range(1, 10)}},
                202282: {0: 2, 1: 10},
            }),
        )
        root_task_id = "root-s00-multi-season-main"
        pending = runner.create_pending_job(source, job_id=root_task_id)
        runner.start_automatic_job(pending.id, target_shelf="us_tv")
        analyze_root_boundaries(
            alist, source, root_task_id=root_task_id, state_root=state_root,
        )
        records = load_work_unit_records(state_root, root_task_id)
        self.assertEqual(len(records), 2)
        main = next(record for record in records if len(record.claimed_seasons) > 1)
        child = next(record for record in records if record.work_unit_id != main.work_unit_id)
        self.assertEqual(child.claimed_seasons, (1,))
        main = replace(
            main,
            media_context="tv",
            identity_status="confirmed",
            identity={"media_type": "tv", "tmdb_id": 60625},
        )
        child = replace(
            child,
            media_context="tv",
            identity_status="confirmed",
            identity={"media_type": "tv", "tmdb_id": 202282, "season": 1},
        )
        save_work_unit_records(state_root, root_task_id, [main, child])

        _register_unit_episode_gaps(
            runner,
            state_root,
            root_task_id,
            main,
            {
                "files": [
                    {
                        "final_name": f"S{season:02d}E01.mkv",
                        "media_kind": "video",
                    }
                    for season in range(1, 10)
                ],
                "target_root": "/library/欧美剧/Rick and Morty",
            },
        )
        _register_unit_episode_gaps(
            runner,
            state_root,
            root_task_id,
            child,
            {
                "files": [
                    {
                        "final_name": f"S01E{episode:02d}.mkv",
                        "media_kind": "video",
                    }
                    for episode in range(1, 11)
                ],
                "target_root": "/library/欧美剧/Rick and Morty/Anime Child",
            },
        )

        gaps = load_gap_ledger(state_root, root_task_id)
        self.assertEqual(
            {gap.gap_id.rsplit("::", 1)[1] for gap in gaps},
            {f"S00E{episode:02d}" for episode in range(1, 38)},
        )
        self.assertTrue(all(gap.work_unit_id == main.work_unit_id for gap in gaps))

    def test_explicit_season_unit_never_claims_s00(self) -> None:
        source = "/incoming/Oshi Root/Season 02"
        files = {f"{source}/Show S02E01.mkv": FAKE_VIDEO_BYTES}
        state_root, alist, runner, _planner_events, _executor_events = self._setup(
            files, tmdb=MultiSeasonTMDB(203738, {0: 2, 1: 35, 2: 24}),
        )
        root_task_id = "root-s00-season-unit"
        pending = runner.create_pending_job(source, job_id=root_task_id)
        runner.start_automatic_job(pending.id, target_shelf="anime")
        analyze_root_boundaries(
            alist, source, root_task_id=root_task_id, state_root=state_root,
        )
        record = load_work_unit_records(state_root, root_task_id)[0]
        record = replace(record, media_context="tv", identity={"media_type": "tv", "tmdb_id": 203738})
        executed_plan = {
            "files": [{"final_name": "S02E01.mkv", "media_kind": "video"}],
            "target_root": "/library/番剧/Oshi Root",
        }
        _register_unit_episode_gaps(runner, state_root, root_task_id, record, executed_plan)
        from engine.scrapeflow.gap_ledger import load_gap_ledger
        tokens = {gap.gap_id.rsplit("::", 1)[1] for gap in load_gap_ledger(state_root, root_task_id)}
        self.assertNotIn("S00E01", tokens)
        self.assertNotIn("S00E02", tokens)

    def test_complete_bare_e_proof_is_revalidated_for_f_and_read_by_j(self) -> None:
        """D's automatic season proof is not an EngineRequest default.

        The real TV planner receives an explicit, freshly revalidated S01;
        its final names then give J exact S01E01..E06 coordinates with no
        fabricated gaps.  This is read/plan-only: no writer is invoked.
        """
        tmdb_id = 99101
        tmdb = BareEpisodePlanningTMDB(tmdb_id, 6)
        source = "/quark/影视/待刮削/One Season Show"
        files = {
            f"{source}/One.Season.Show.E{episode:02d}.mkv": FAKE_VIDEO_BYTES
            for episode in range(1, 7)
        }
        with tempfile.TemporaryDirectory() as directory:
            state_root = Path(directory)
            alist = BareEpisodePlanningAList(files)
            runner = SimpleEngineRunner(
                state_root,
                alist=alist,
                tmdb=tmdb,
                validate=False,
                library_root="/quark/影视",
            )
            root_task_id = "root-bare-f-j"
            pending = runner.create_pending_job(source, job_id=root_task_id)
            runner.start_automatic_job(pending.id, target_shelf="us_tv")
            analyze_root_boundaries(
                alist, source, root_task_id=root_task_id, state_root=state_root,
            )
            record = load_work_unit_records(state_root, root_task_id)[0]
            apply_work_unit_override(
                state_root,
                root_task_id,
                record.work_unit_id,
                media_type="tv",
                tmdb_id=tmdb_id,
            )
            reconciled = reconcile_root_work_units(
                alist,
                "/quark/影视",
                state_root,
                root_task_id,
                episode_catalog=TmdbEpisodeCatalog(tmdb),
                tmdb_client=tmdb,
            )
            record = reconciled[0]
            self.assertEqual(record.reconciliation_outcome, "new_work")
            self.assertIsNotNone(record.reconciliation_evidence)

            request = _request_for_unit(runner, record, root_task_id, state_root)
            self.assertEqual(request.season, 1)
            plan = runner._build_plan(request)  # noqa: SLF001 - F planner seam
            primary_tokens = {
                f"S{season:02d}E{episode:02d}"
                for item in plan.files
                if item.media_kind == "video"
                for season, episode in audit_episode_tokens(item.final_name)
            }
            self.assertEqual(
                primary_tokens,
                {f"S01E{episode:02d}" for episode in range(1, 7)},
            )
            self.assertEqual(
                _register_unit_episode_gaps(
                    runner,
                    state_root,
                    root_task_id,
                    record,
                    plan_to_dict(plan),
                ),
                [],
            )

            # Changing a source object after D invalidates the persisted
            # receipt; F stops before it could plan or write anything.
            del alist.files[f"{source}/One.Season.Show.E06.mkv"]
            alist.files[f"{source}/One.Season.Show.E07.mkv"] = FAKE_VIDEO_BYTES
            with self.assertRaisesRegex(ValueError, "D 裸 E 季集证据"):
                _request_for_unit(runner, record, root_task_id, state_root)
            self.assertEqual(alist.move_calls, [])

    def test_release_dash_proof_is_revalidated_for_f(self) -> None:
        """F repeats the exact D release-dash proof before planning.

        A dash ordinal alone is not a planner default.  Only the persisted
        D receipt from the shared proof turns this source into explicit S01;
        source drift then stops F before it can build or write a plan.
        """
        prefixes = (
            "[LoliHouse] Akuyaku Reijou Level 99",
            "[Group] The 100",
            "[Group] Show 2",
        )
        with tempfile.TemporaryDirectory() as directory:
            state_root = Path(directory)
            for index, prefix in enumerate(prefixes):
                with self.subTest(prefix=prefix):
                    tmdb_id = 99103 + index
                    tmdb = BareEpisodePlanningTMDB(tmdb_id, 3)
                    source = f"/quark/影视/待刮削/Release Dash {index}"
                    names = [
                        (
                            f"{prefix} - {episode:02d} "
                            "[WebRip 1080p HEVC-10bit AAC SRTx2].mkv"
                        )
                        for episode in range(1, 4)
                    ]
                    files = {
                        f"{source}/{name}": FAKE_VIDEO_BYTES for name in names
                    }
                    alist = BareEpisodePlanningAList(files)
                    runner = SimpleEngineRunner(
                        state_root,
                        alist=alist,
                        tmdb=tmdb,
                        validate=False,
                        library_root="/quark/影视",
                    )
                    root_task_id = f"root-release-dash-f-{index}"
                    pending = runner.create_pending_job(source, job_id=root_task_id)
                    runner.start_automatic_job(pending.id, target_shelf="anime")
                    analyze_root_boundaries(
                        alist,
                        source,
                        root_task_id=root_task_id,
                        state_root=state_root,
                    )
                    record = load_work_unit_records(state_root, root_task_id)[0]
                    apply_work_unit_override(
                        state_root,
                        root_task_id,
                        record.work_unit_id,
                        media_type="tv",
                        tmdb_id=tmdb_id,
                    )
                    record = reconcile_root_work_units(
                        alist,
                        "/quark/影视",
                        state_root,
                        root_task_id,
                        episode_catalog=TmdbEpisodeCatalog(tmdb),
                        tmdb_client=tmdb,
                    )[0]
                    self.assertEqual(record.reconciliation_outcome, "new_work")
                    self.assertEqual(
                        (record.reconciliation_evidence or {}).get("kind"),
                        "tmdb_single_positive_season_release_dash_episodes",
                    )

                    request = _request_for_unit(
                        runner, record, root_task_id, state_root,
                    )
                    self.assertEqual(request.season, 1)
                    self.assertTrue(request.allow_release_dash_ordinal)
                    self.assertEqual(request.source_scope_paths, (source,))
                    self.assertEqual(
                        {
                            str(item.get("full_path") or "")
                            for item in request.source_files or ()
                        },
                        {f"{source}/{name}" for name in names},
                    )
                    self.assertIsNotNone(request.episode_map_path)
                    mapping = json.loads(
                        Path(str(request.episode_map_path)).read_text(encoding="utf-8")
                    )
                    self.assertEqual(
                        mapping,
                        {str(episode): f"S01E{episode:02d}" for episode in range(1, 4)},
                    )
                    plan = runner._build_plan(request)  # noqa: SLF001 - F planner seam
                    self.assertEqual(
                        [item.episode_key for item in plan.files if item.media_kind == "video"],
                        ["E01", "E02", "E03"],
                    )
                    self.assertEqual(
                        {
                            token
                            for item in plan.files
                            if item.media_kind == "video"
                            for season, episode in audit_episode_tokens(item.final_name)
                            for token in (f"S{season:02d}E{episode:02d}",)
                        },
                        {f"S01E{episode:02d}" for episode in range(1, 4)},
                    )

                    old_path = f"{source}/{names[-1]}"
                    new_path = (
                        f"{source}/[LoliHouse] Other Show - 03 "
                        "[WebRip 1080p HEVC-10bit AAC SRTx2].mkv"
                    )
                    alist.files[new_path] = alist.files.pop(old_path)
                    with self.assertRaisesRegex(
                        ValueError, "D 发行组短横线集号 季集证据",
                    ):
                        _request_for_unit(runner, record, root_task_id, state_root)
                    self.assertEqual(alist.move_calls, [])

    def test_title_ordinal_proof_is_revalidated_for_f(self) -> None:
        """F consumes only the D-proven ``Title 01`` map and gate."""
        with tempfile.TemporaryDirectory() as directory:
            state_root = Path(directory)
            tmdb_id = 99130
            tmdb = BareEpisodePlanningTMDB(tmdb_id, 3)
            source = "/quark/影视/待刮削/Title Ordinal 0"
            names = [
                f"[4K_EA] One Season Show {episode:02d} [WebRip].mkv"
                for episode in range(1, 4)
            ]
            alist = BareEpisodePlanningAList(
                {f"{source}/{name}": FAKE_VIDEO_BYTES for name in names}
            )
            runner = SimpleEngineRunner(
                state_root,
                alist=alist,
                tmdb=tmdb,
                validate=False,
                library_root="/quark/影视",
            )
            root_task_id = "root-title-ordinal-f"
            pending = runner.create_pending_job(source, job_id=root_task_id)
            runner.start_automatic_job(pending.id, target_shelf="anime")
            analyze_root_boundaries(
                alist, source, root_task_id=root_task_id, state_root=state_root,
            )
            record = load_work_unit_records(state_root, root_task_id)[0]
            apply_work_unit_override(
                state_root, root_task_id, record.work_unit_id,
                media_type="tv", tmdb_id=tmdb_id,
            )
            record = reconcile_root_work_units(
                alist, "/quark/影视", state_root, root_task_id,
                episode_catalog=TmdbEpisodeCatalog(tmdb), tmdb_client=tmdb,
            )[0]
            self.assertEqual(record.reconciliation_outcome, "new_work")
            self.assertEqual(
                (record.reconciliation_evidence or {}).get("kind"),
                "tmdb_single_positive_season_title_ordinal_episodes",
            )
            request = _request_for_unit(runner, record, root_task_id, state_root)
            self.assertEqual(request.season, 1)
            self.assertTrue(request.allow_release_title_ordinal)
            mapping = json.loads(Path(str(request.episode_map_path)).read_text())
            self.assertEqual(
                mapping,
                {str(episode): f"S01E{episode:02d}" for episode in range(1, 4)},
            )
            plan = runner._build_plan(request)  # noqa: SLF001
            self.assertEqual(
                [item.episode_key for item in plan.files if item.media_kind == "video"],
                ["E01", "E02", "E03"],
            )

    def test_bracketed_proof_is_revalidated_for_f(self) -> None:
        """F consumes the D-proven ``[01]`` map, not the movie keyword.

        A bracketed episode run is often released beside theatrical films
        inside one ``剧场版``-labelled folder.  The smart planner's loose
        movie-context heuristic used to hijack exactly that shape and fail
        closed on movie auto-match; the revalidated D bracketed proof must
        route the unit through the explicit episode-map path instead.
        """
        with tempfile.TemporaryDirectory() as directory:
            state_root = Path(directory)
            tmdb_id = 99150
            tmdb = BareEpisodePlanningTMDB(tmdb_id, 2)
            source = "/quark/影视/待刮削/Bracket Run 0"
            run_dir = f"{source}/剧场版 Show The Final"
            names = [
                f"[Ygm] Show ~Semi-Final~ [{episode:02d}]"
                "[Ma10p_2160p][x265_flac_ass].mkv"
                for episode in range(1, 3)
            ]
            alist = BareEpisodePlanningAList(
                {f"{run_dir}/{name}": FAKE_VIDEO_BYTES for name in names}
            )
            runner = SimpleEngineRunner(
                state_root,
                alist=alist,
                tmdb=tmdb,
                validate=False,
                library_root="/quark/影视",
            )
            root_task_id = "root-bracketed-f"
            pending = runner.create_pending_job(source, job_id=root_task_id)
            runner.start_automatic_job(pending.id, target_shelf="anime")
            analyze_root_boundaries(
                alist, source, root_task_id=root_task_id, state_root=state_root,
            )
            records = load_work_unit_records(state_root, root_task_id)
            self.assertEqual(len(records), 1)
            apply_work_unit_override(
                state_root, root_task_id, records[0].work_unit_id,
                media_type="tv", tmdb_id=tmdb_id,
            )
            record = reconcile_root_work_units(
                alist, "/quark/影视", state_root, root_task_id,
                episode_catalog=TmdbEpisodeCatalog(tmdb), tmdb_client=tmdb,
            )[0]
            self.assertEqual(record.reconciliation_outcome, "new_work")
            self.assertEqual(
                (record.reconciliation_evidence or {}).get("kind"),
                "tmdb_single_positive_season_bracketed_episodes",
            )
            request = _request_for_unit(runner, record, root_task_id, state_root)
            self.assertEqual(request.season, 1)
            mapping = json.loads(Path(str(request.episode_map_path)).read_text())
            self.assertEqual(mapping, {"1": "S01E01", "2": "S01E02"})
            plan = runner._build_plan(request)  # noqa: SLF001 - F planner seam
            self.assertEqual(
                [item.episode_key for item in plan.files if item.media_kind == "video"],
                ["E01", "E02"],
            )
            self.assertEqual(
                {
                    token
                    for item in plan.files
                    if item.media_kind == "video"
                    for season, episode in audit_episode_tokens(item.final_name)
                    for token in (f"S{season:02d}E{episode:02d}",)
                },
                {"S01E01", "S01E02"},
            )

            old_path = f"{run_dir}/{names[-1]}"
            new_path = f"{run_dir}/[Ygm] Show ~Semi-Final~ [03][Ma10p_2160p].mkv"
            alist.files[new_path] = alist.files.pop(old_path)
            with self.assertRaisesRegex(ValueError, "D 纯方括号集号 季集证据"):
                _request_for_unit(runner, record, root_task_id, state_root)
            self.assertEqual(alist.move_calls, [])
        """The F handoff may not widen after its fresh D proof.

        The injected fourth file appears after the release-dash proof and the
        exact fresh scope fingerprint, but while the planner-shaped manifest
        is being assembled.  It must stop before an EngineRequest can carry
        that new object into the narrowly enabled parser.
        """
        class InjectingManifestAList(BareEpisodePlanningAList):
            def __init__(self, files: dict[str, bytes], injected_path: str) -> None:
                super().__init__(files)
                self.injected_path = injected_path
                self.inject_on_next_walk = False

            def walk(self, path: str, **kwargs: object) -> list[dict[str, object]]:
                if self.inject_on_next_walk:
                    self.inject_on_next_walk = False
                    self.files[self.injected_path] = FAKE_VIDEO_BYTES
                return super().walk(path, **kwargs)

        tmdb_id = 99120
        tmdb = BareEpisodePlanningTMDB(tmdb_id, 3)
        source = "/quark/影视/待刮削/Release Dash Manifest Race"
        names = [
            f"[Group] The 100 - {episode:02d} [WebRip].mkv"
            for episode in range(1, 4)
        ]
        injected_path = f"{source}/[Group] The 100 - 04 [WebRip].mkv"
        with tempfile.TemporaryDirectory() as directory:
            state_root = Path(directory)
            alist = InjectingManifestAList(
                {f"{source}/{name}": FAKE_VIDEO_BYTES for name in names},
                injected_path,
            )
            runner = SimpleEngineRunner(
                state_root,
                alist=alist,
                tmdb=tmdb,
                validate=False,
                library_root="/quark/影视",
            )
            root_task_id = "root-release-dash-manifest-race"
            pending = runner.create_pending_job(source, job_id=root_task_id)
            runner.start_automatic_job(pending.id, target_shelf="anime")
            analyze_root_boundaries(
                alist,
                source,
                root_task_id=root_task_id,
                state_root=state_root,
            )
            record = load_work_unit_records(state_root, root_task_id)[0]
            apply_work_unit_override(
                state_root,
                root_task_id,
                record.work_unit_id,
                media_type="tv",
                tmdb_id=tmdb_id,
            )
            record = reconcile_root_work_units(
                alist,
                "/quark/影视",
                state_root,
                root_task_id,
                episode_catalog=TmdbEpisodeCatalog(tmdb),
                tmdb_client=tmdb,
            )[0]
            self.assertEqual(record.reconciliation_outcome, "new_work")

            alist.inject_on_next_walk = True
            with self.assertRaisesRegex(
                ValueError, "fresh 来源清单在快照核验后变化",
            ):
                _request_for_unit(runner, record, root_task_id, state_root)
            self.assertIn(injected_path, alist.files)
            self.assertEqual(alist.move_calls, [])

    def test_bracketed_proof_carries_edition_cut_files_through_f(self) -> None:
        """F's episode map keeps the edition cut file of a repeated ordinal.

        ``[02]`` beside ``[02(Director' Cut)]`` proves one two-episode run;
        the map carries both source files onto the same coordinate and the
        planner keeps the edition file as a distinct cut of that episode.
        """
        with tempfile.TemporaryDirectory() as directory:
            state_root = Path(directory)
            tmdb_id = 99151
            tmdb = BareEpisodePlanningTMDB(tmdb_id, 2)
            source = "/quark/影视/待刮削/Bracket Run Edition"
            run_dir = f"{source}/剧场版 Show The Final"
            names = [
                f"[Ygm] Show ~Semi-Final~ [{episode:02d}]"
                "[Ma10p_2160p][x265_flac_ass].mkv"
                for episode in range(1, 3)
            ] + [
                "[Ygm] Show ~Semi-Final~ [02(Director' Cut)]"
                "[Ma10p_2160p][x265_flac_ass].mkv",
            ]
            alist = BareEpisodePlanningAList(
                {f"{run_dir}/{name}": FAKE_VIDEO_BYTES for name in names}
            )
            runner = SimpleEngineRunner(
                state_root,
                alist=alist,
                tmdb=tmdb,
                validate=False,
                library_root="/quark/影视",
            )
            root_task_id = "root-bracketed-edition-f"
            pending = runner.create_pending_job(source, job_id=root_task_id)
            runner.start_automatic_job(pending.id, target_shelf="anime")
            analyze_root_boundaries(
                alist, source, root_task_id=root_task_id, state_root=state_root,
            )
            records = load_work_unit_records(state_root, root_task_id)
            self.assertEqual(len(records), 1)
            apply_work_unit_override(
                state_root, root_task_id, records[0].work_unit_id,
                media_type="tv", tmdb_id=tmdb_id,
            )
            record = reconcile_root_work_units(
                alist, "/quark/影视", state_root, root_task_id,
                episode_catalog=TmdbEpisodeCatalog(tmdb), tmdb_client=tmdb,
            )[0]
            self.assertEqual(record.reconciliation_outcome, "new_work")
            self.assertEqual(
                (record.reconciliation_evidence or {}).get("episode_count"), 2
            )
            request = _request_for_unit(runner, record, root_task_id, state_root)
            self.assertEqual(request.season, 1)
            mapping = json.loads(Path(str(request.episode_map_path)).read_text())
            self.assertEqual(mapping, {"1": "S01E01", "2": "S01E02"})
            plan = runner._build_plan(request)  # noqa: SLF001 - F planner seam
            videos = [item for item in plan.files if item.media_kind == "video"]
            self.assertEqual(
                [item.episode_key for item in videos], ["E01", "E02", "E02"]
            )
            self.assertEqual(
                {
                    token
                    for item in videos
                    for season, episode in audit_episode_tokens(item.final_name)
                    for token in (f"S{season:02d}E{episode:02d}",)
                },
                {"S01E01", "S01E02"},
            )
            self.assertEqual(len({item.final_name for item in videos}), 3)

    def test_complete_bracketed_proof_is_revalidated_for_f_and_read_by_j(self) -> None:
        """The strict ``[01]..[12]`` D proof is fresh again at F.

        This mirrors the real one-season release shape: primary bracketed
        ordinals plus NCOP/NCED and a shorter published TMDB Season 00.
        F must pass the proved regular season explicitly to the existing
        planner; a changed source snapshot must stop planning before a writer
        could be reached.
        """
        tmdb_id = 99102
        tmdb = BareEpisodePlanningTMDB(tmdb_id, 12, specials=2)
        source = "/quark/影视/待刮削/Bracketed One Season Show"
        files = {
            (
                f"{source}/[Ygm] Example Show [{episode:02d}]"
                "[Ma10p_2160p][x265_flac_ass].mkv"
            ): FAKE_VIDEO_BYTES
            for episode in range(1, 13)
        }
        files.update({
            f"{source}/[Ygm] Example Show [NCOP][Ma10p_2160p].mkv": FAKE_VIDEO_BYTES,
            f"{source}/[Ygm] Example Show [NCED][Ma10p_2160p].mkv": FAKE_VIDEO_BYTES,
        })
        with tempfile.TemporaryDirectory() as directory:
            state_root = Path(directory)
            alist = BareEpisodePlanningAList(files)
            runner = SimpleEngineRunner(
                state_root,
                alist=alist,
                tmdb=tmdb,
                validate=False,
                library_root="/quark/影视",
            )
            root_task_id = "root-bracketed-f-j"
            pending = runner.create_pending_job(source, job_id=root_task_id)
            runner.start_automatic_job(pending.id, target_shelf="anime")
            analyze_root_boundaries(
                alist, source, root_task_id=root_task_id, state_root=state_root,
            )
            record = load_work_unit_records(state_root, root_task_id)[0]
            apply_work_unit_override(
                state_root,
                root_task_id,
                record.work_unit_id,
                media_type="tv",
                tmdb_id=tmdb_id,
            )
            reconciled = reconcile_root_work_units(
                alist,
                "/quark/影视",
                state_root,
                root_task_id,
                episode_catalog=TmdbEpisodeCatalog(tmdb),
                tmdb_client=tmdb,
            )
            record = reconciled[0]
            self.assertEqual(record.reconciliation_outcome, "new_work")
            self.assertEqual(
                (record.reconciliation_evidence or {}).get("kind"),
                "tmdb_single_positive_season_bracketed_episodes",
            )

            request = _request_for_unit(runner, record, root_task_id, state_root)
            self.assertEqual(request.season, 1)
            plan = runner._build_plan(request)  # noqa: SLF001 - F planner seam
            primary_tokens = {
                f"S{season:02d}E{episode:02d}"
                for item in plan.files
                if item.media_kind == "video"
                for season, episode in audit_episode_tokens(item.final_name)
            }
            self.assertEqual(
                primary_tokens,
                {f"S01E{episode:02d}" for episode in range(1, 13)},
            )
            bracketed_gaps = _register_unit_episode_gaps(
                runner,
                state_root,
                root_task_id,
                record,
                plan_to_dict(plan),
            )
            self.assertEqual(
                {gap.gap_id.rsplit("::", 1)[1] for gap in bracketed_gaps},
                {"S00E01", "S00E02"},
            )

            old_name = (
                f"{source}/[Ygm] Example Show [12]"
                "[Ma10p_2160p][x265_flac_ass].mkv"
            )
            new_name = (
                f"{source}/[Ygm] Example Show [13]"
                "[Ma10p_2160p][x265_flac_ass].mkv"
            )
            del alist.files[old_name]
            alist.files[new_name] = FAKE_VIDEO_BYTES
            with self.assertRaisesRegex(ValueError, "D 纯方括号集号 季集证据"):
                _request_for_unit(runner, record, root_task_id, state_root)
            self.assertEqual(alist.move_calls, [])

    def test_bracketed_proof_excludes_complete_physical_special_run(self) -> None:
        """A complete OAD family beside ``[01]..[N]`` no longer fails the proof.

        The regular bracketed run still proves the single positive season; a
        separately complete OAD/OVA/OAV family is excluded instead of
        invalidating the whole proof.
        """
        tmdb_id = 99122
        tmdb = BareEpisodePlanningTMDB(tmdb_id, 24, specials=3)
        source = "/quark/影视/待刮削/Bracketed Show With OADs"
        files = {
            (
                f"{source}/[Ygm] Example Show [{episode:02d}]"
                "[Ma10p_2160p][x265_flac_ass].mkv"
            ): FAKE_VIDEO_BYTES
            for episode in range(1, 25)
        }
        files.update({
            f"{source}/[Ygm] Example Show [OAD{number:02d}][Ma10p_1440p].mkv": FAKE_VIDEO_BYTES
            for number in range(1, 4)
        })
        with tempfile.TemporaryDirectory() as directory:
            state_root = Path(directory)
            alist = BareEpisodePlanningAList(files)
            runner = SimpleEngineRunner(
                state_root, alist=alist, tmdb=tmdb, validate=False,
                library_root="/quark/影视",
            )
            root_task_id = "root-bracketed-oad"
            pending = runner.create_pending_job(source, job_id=root_task_id)
            runner.start_automatic_job(pending.id, target_shelf="anime")
            analyze_root_boundaries(
                alist, source, root_task_id=root_task_id, state_root=state_root,
            )
            record = load_work_unit_records(state_root, root_task_id)[0]
            apply_work_unit_override(
                state_root, root_task_id, record.work_unit_id,
                media_type="tv", tmdb_id=tmdb_id,
            )
            reconciled = reconcile_root_work_units(
                alist, "/quark/影视", state_root, root_task_id,
                episode_catalog=TmdbEpisodeCatalog(tmdb), tmdb_client=tmdb,
            )[0]
            self.assertEqual(reconciled.reconciliation_outcome, "new_work")
            self.assertEqual(
                (reconciled.reconciliation_evidence or {}).get("kind"),
                "tmdb_single_positive_season_bracketed_episodes",
            )

    def test_bracketed_proof_fails_closed_on_incomplete_physical_special(self) -> None:
        """A gapped OAD family keeps the bracketed proof fail-closed."""
        tmdb_id = 99123
        tmdb = BareEpisodePlanningTMDB(tmdb_id, 24, specials=3)
        source = "/quark/影视/待刮削/Bracketed Show With Partial OADs"
        files = {
            (
                f"{source}/[Ygm] Example Show [{episode:02d}]"
                "[Ma10p_2160p][x265_flac_ass].mkv"
            ): FAKE_VIDEO_BYTES
            for episode in range(1, 25)
        }
        files.update({
            f"{source}/[Ygm] Example Show [OAD01][Ma10p_1440p].mkv": FAKE_VIDEO_BYTES,
            f"{source}/[Ygm] Example Show [OAD03][Ma10p_1440p].mkv": FAKE_VIDEO_BYTES,
        })
        with tempfile.TemporaryDirectory() as directory:
            state_root = Path(directory)
            alist = BareEpisodePlanningAList(files)
            runner = SimpleEngineRunner(
                state_root, alist=alist, tmdb=tmdb, validate=False,
                library_root="/quark/影视",
            )
            root_task_id = "root-bracketed-oad-partial"
            pending = runner.create_pending_job(source, job_id=root_task_id)
            runner.start_automatic_job(pending.id, target_shelf="anime")
            analyze_root_boundaries(
                alist, source, root_task_id=root_task_id, state_root=state_root,
            )
            record = load_work_unit_records(state_root, root_task_id)[0]
            apply_work_unit_override(
                state_root, root_task_id, record.work_unit_id,
                media_type="tv", tmdb_id=tmdb_id,
            )
            reconciled = reconcile_root_work_units(
                alist, "/quark/影视", state_root, root_task_id,
                episode_catalog=TmdbEpisodeCatalog(tmdb), tmdb_client=tmdb,
            )[0]
            self.assertEqual(reconciled.reconciliation_outcome, "uncertain")

    def test_bracketed_proof_excludes_bonus_directory_video(self) -> None:
        """An ``[MV]`` inside ``NCOP&ED`` no longer fails the bracketed proof.

        The OP/ED bonus-directory context is enough to omit a non-NCOP/NCED
        video (``[MV]``) from the integer regular run.
        """
        tmdb_id = 99124
        tmdb = BareEpisodePlanningTMDB(tmdb_id, 12, specials=0)
        source = "/quark/影视/待刮削/Bracketed Show 99124"
        files = {
            (
                f"{source}/[Ygm] Example Show [{episode:02d}]"
                "[Ma10p_2160p][x265_flac_ass].mkv"
            ): FAKE_VIDEO_BYTES
            for episode in range(1, 13)
        }
        files.update({
            f"{source}/NCOP&ED/[Ygm] Example Show [MV][Ma10p_2160p].mkv": FAKE_VIDEO_BYTES,
            f"{source}/NCOP&ED/[Ygm] Example Show [NCOP][Ma10p_2160p].mkv": FAKE_VIDEO_BYTES,
        })
        with tempfile.TemporaryDirectory() as directory:
            state_root = Path(directory)
            alist = BareEpisodePlanningAList(files)
            runner = SimpleEngineRunner(
                state_root, alist=alist, tmdb=tmdb, validate=False,
                library_root="/quark/影视",
            )
            root_task_id = "root-bracketed-bonus-dir"
            pending = runner.create_pending_job(source, job_id=root_task_id)
            runner.start_automatic_job(pending.id, target_shelf="anime")
            analyze_root_boundaries(
                alist, source, root_task_id=root_task_id, state_root=state_root,
            )
            record = load_work_unit_records(state_root, root_task_id)[0]
            apply_work_unit_override(
                state_root, root_task_id, record.work_unit_id,
                media_type="tv", tmdb_id=tmdb_id,
            )
            reconciled = reconcile_root_work_units(
                alist, "/quark/影视", state_root, root_task_id,
                episode_catalog=TmdbEpisodeCatalog(tmdb), tmdb_client=tmdb,
            )[0]
            self.assertEqual(reconciled.reconciliation_outcome, "new_work")
            self.assertEqual(
                (reconciled.reconciliation_evidence or {}).get("kind"),
                "tmdb_single_positive_season_bracketed_episodes",
            )

    def test_theme_directory_classifies_mv_as_cleanup_only_inside_ncop_ed(self) -> None:
        """An ``[MV]`` is theme only inside a recognized OP/ED directory.

        This is the F-step complement of the D-step bonus-directory proof: a
        music video beside ``NCOP&ED`` must not leak into the smart planner's
        movie detection (which would otherwise mint an empty related-movie
        child).  The same basename outside an OP/ED directory stays media.
        """
        from engine.scrapeflow.core import _contextual_cleanup_reason

        def item(name: str, path: str) -> dict[str, object]:
            return {"name": name, "full_path": path}

        self.assertEqual(
            _contextual_cleanup_reason(item(
                "[Ygm] Show [MV][Ma10p_2160p].mkv",
                "/x/Show/NCOP&ED/[Ygm] Show [MV][Ma10p_2160p].mkv",
            )),
            "无字幕片头/片尾/光盘菜单视频",
        )
        self.assertIsNone(_contextual_cleanup_reason(item(
            "[Ygm] Show [MV][Ma10p_2160p].mkv",
            "/x/Show/[Ygm] Show [MV][Ma10p_2160p].mkv",
        )))
        self.assertIsNone(_contextual_cleanup_reason(item(
            "[Ygm] Show [01][Ma10p_2160p].mkv",
            "/x/Show/[Ygm] Show [01][Ma10p_2160p].mkv",
        )))

    def test_bracketed_proof_excludes_movie_shaped_sibling_subdir(self) -> None:
        """A titled sibling with exactly one large video is an independent film.

        The rooted TV ``[01]..[12]`` run must still prove the season; the
        movie-shaped sibling subtree is omitted instead of invalidating it.
        """
        tmdb_id = 99126
        tmdb = BareEpisodePlanningTMDB(tmdb_id, 12, specials=0)
        source = "/quark/影视/待刮削/Bracketed Show 99126"
        files = {
            (
                f"{source}/[Ygm] Example Show [{episode:02d}]"
                "[Ma10p_2160p][x265_flac_ass].mkv"
            ): FAKE_VIDEO_BYTES
            for episode in range(1, 13)
        }
        files[f"{source}/电影/[Ygm] Example Show Movie [Ma10p_2160p].mkv"] = (
            b"m" * (200 * 1024 * 1024 + 1)
        )
        with tempfile.TemporaryDirectory() as directory:
            state_root = Path(directory)
            alist = BareEpisodePlanningAList(files)
            runner = SimpleEngineRunner(
                state_root, alist=alist, tmdb=tmdb, validate=False,
                library_root="/quark/影视",
            )
            root_task_id = "root-bracketed-movie-sibling"
            pending = runner.create_pending_job(source, job_id=root_task_id)
            runner.start_automatic_job(pending.id, target_shelf="anime")
            analyze_root_boundaries(
                alist, source, root_task_id=root_task_id, state_root=state_root,
            )
            record = load_work_unit_records(state_root, root_task_id)[0]
            apply_work_unit_override(
                state_root, root_task_id, record.work_unit_id,
                media_type="tv", tmdb_id=tmdb_id,
            )
            reconciled = reconcile_root_work_units(
                alist, "/quark/影视", state_root, root_task_id,
                episode_catalog=TmdbEpisodeCatalog(tmdb), tmdb_client=tmdb,
            )[0]
            self.assertEqual(reconciled.reconciliation_outcome, "new_work")
            self.assertEqual(
                (reconciled.reconciliation_evidence or {}).get("kind"),
                "tmdb_single_positive_season_bracketed_episodes",
            )

    def test_bracketed_proof_excludes_unnumbered_special_marker(self) -> None:
        """A bare ``[OAD]`` (no ordinal) is a named special, not an episode."""
        tmdb_id = 99127
        tmdb = BareEpisodePlanningTMDB(tmdb_id, 12, specials=0)
        source = "/quark/影视/待刮削/Bracketed Show 99127"
        files = {
            (
                f"{source}/[Ygm] Example Show [{episode:02d}]"
                "[Ma10p_2160p][x265_flac_ass].mkv"
            ): FAKE_VIDEO_BYTES
            for episode in range(1, 13)
        }
        files[f"{source}/[Ygm] Example Show [OAD][Ma10p_2160p].mkv"] = FAKE_VIDEO_BYTES
        with tempfile.TemporaryDirectory() as directory:
            state_root = Path(directory)
            alist = BareEpisodePlanningAList(files)
            runner = SimpleEngineRunner(
                state_root, alist=alist, tmdb=tmdb, validate=False,
                library_root="/quark/影视",
            )
            root_task_id = "root-bracketed-unnumbered-oad"
            pending = runner.create_pending_job(source, job_id=root_task_id)
            runner.start_automatic_job(pending.id, target_shelf="anime")
            analyze_root_boundaries(
                alist, source, root_task_id=root_task_id, state_root=state_root,
            )
            record = load_work_unit_records(state_root, root_task_id)[0]
            apply_work_unit_override(
                state_root, root_task_id, record.work_unit_id,
                media_type="tv", tmdb_id=tmdb_id,
            )
            reconciled = reconcile_root_work_units(
                alist, "/quark/影视", state_root, root_task_id,
                episode_catalog=TmdbEpisodeCatalog(tmdb), tmdb_client=tmdb,
            )[0]
            self.assertEqual(reconciled.reconciliation_outcome, "new_work")

    def test_bracketed_proof_excludes_disc_menu_video(self) -> None:
        """A ``[Menu01]`` disc menu is bonus content, not an episode."""
        tmdb_id = 99128
        tmdb = BareEpisodePlanningTMDB(tmdb_id, 12, specials=0)
        source = "/quark/影视/待刮削/Bracketed Show 99128"
        files = {
            (
                f"{source}/[Ygm] Example Show [{episode:02d}]"
                "[Ma10p_2160p][x265_flac_ass].mkv"
            ): FAKE_VIDEO_BYTES
            for episode in range(1, 13)
        }
        files[f"{source}/Menu/[Ygm] Example Show [Menu01][Ma10p_2160p].mkv"] = FAKE_VIDEO_BYTES
        with tempfile.TemporaryDirectory() as directory:
            state_root = Path(directory)
            alist = BareEpisodePlanningAList(files)
            runner = SimpleEngineRunner(
                state_root, alist=alist, tmdb=tmdb, validate=False,
                library_root="/quark/影视",
            )
            root_task_id = "root-bracketed-menu"
            pending = runner.create_pending_job(source, job_id=root_task_id)
            runner.start_automatic_job(pending.id, target_shelf="anime")
            analyze_root_boundaries(
                alist, source, root_task_id=root_task_id, state_root=state_root,
            )
            record = load_work_unit_records(state_root, root_task_id)[0]
            apply_work_unit_override(
                state_root, root_task_id, record.work_unit_id,
                media_type="tv", tmdb_id=tmdb_id,
            )
            reconciled = reconcile_root_work_units(
                alist, "/quark/影视", state_root, root_task_id,
                episode_catalog=TmdbEpisodeCatalog(tmdb), tmdb_client=tmdb,
            )[0]
            self.assertEqual(reconciled.reconciliation_outcome, "new_work")

    def test_bracketed_proof_excludes_numbered_op_ed_theme(self) -> None:
        """A numbered ``[OPn]``/``[EDn]`` theme is non-story, not an episode."""
        tmdb_id = 99129
        tmdb = BareEpisodePlanningTMDB(tmdb_id, 12, specials=0)
        source = "/quark/影视/待刮削/Bracketed Show 99129"
        files = {
            (
                f"{source}/[Ygm] Example Show [{episode:02d}]"
                "[Ma10p_2160p][x265_flac_ass].mkv"
            ): FAKE_VIDEO_BYTES
            for episode in range(1, 13)
        }
        files[f"{source}/[Ygm] Example Show [OP1][Ma10p_2160p].mkv"] = FAKE_VIDEO_BYTES
        files[f"{source}/[Ygm] Example Show [ED1][Ma10p_2160p].mkv"] = FAKE_VIDEO_BYTES
        with tempfile.TemporaryDirectory() as directory:
            state_root = Path(directory)
            alist = BareEpisodePlanningAList(files)
            runner = SimpleEngineRunner(
                state_root, alist=alist, tmdb=tmdb, validate=False,
                library_root="/quark/影视",
            )
            root_task_id = "root-bracketed-op-ed"
            pending = runner.create_pending_job(source, job_id=root_task_id)
            runner.start_automatic_job(pending.id, target_shelf="anime")
            analyze_root_boundaries(
                alist, source, root_task_id=root_task_id, state_root=state_root,
            )
            record = load_work_unit_records(state_root, root_task_id)[0]
            apply_work_unit_override(
                state_root, root_task_id, record.work_unit_id,
                media_type="tv", tmdb_id=tmdb_id,
            )
            reconciled = reconcile_root_work_units(
                alist, "/quark/影视", state_root, root_task_id,
                episode_catalog=TmdbEpisodeCatalog(tmdb), tmdb_client=tmdb,
            )[0]
            self.assertEqual(reconciled.reconciliation_outcome, "new_work")

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

    def test_declared_empty_season_registers_only_its_precise_gap(self) -> None:
        """A cohort's declared empty middle season remains visible to J.

        The executed plan proves S01E01 and S03E01.  B/W additionally
        declared S02 as part of the same exact source cohort, so J must use
        the official catalog to register only S02E01—not invent gaps for the
        two written seasons or silently erase the empty declared season.
        """
        files = {
            "/incoming/one/Fate Zero/S01E01.mkv": FAKE_VIDEO_BYTES,
            "/incoming/one/Fate Zero/S03E01.mkv": FAKE_VIDEO_BYTES,
        }
        state_root, alist, runner, _p, _e = self._setup(
            files, tmdb=MultiSeasonTMDB(101, {1: 1, 2: 1, 3: 1}),
        )
        self._record_runner = runner
        self._record_alist = alist
        record = replace(
            self._record_for(state_root, "root-declared-empty", 101),
            claimed_seasons=(1, 2, 3),
        )
        executed_plan = {
            "files": [
                {
                    "final_name": "S01E01.mkv",
                    "media_kind": "video",
                    "target_dir": "/library/番剧/Work (101)",
                },
                {
                    "final_name": "S03E01.mkv",
                    "media_kind": "video",
                    "target_dir": "/library/番剧/Work (101)",
                },
            ],
            "target_root": "/library/番剧/Work (101)",
        }

        _register_unit_episode_gaps(
            runner, state_root, "root-declared-empty", record, executed_plan,
        )

        from engine.scrapeflow.gap_ledger import load_gap_ledger

        gaps = load_gap_ledger(state_root, "root-declared-empty")
        self.assertEqual(
            {gap.gap_id.rsplit("::", 1)[1] for gap in gaps},
            {"S02E01"},
        )
        self.assertTrue(all(gap.work_unit_id == record.work_unit_id for gap in gaps))

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
        with self.assertRaises(GapDiscoveryAttention):
            _register_unit_episode_gaps(
                runner, state_root, "root-absolute", record, executed_plan,
            )
        from engine.scrapeflow.gap_ledger import load_gap_ledger

        # Absolute-number names cannot be verified against season coordinates:
        # the J step fails closed as explicit operator attention rather than
        # silently accepting a zero-gap result.
        self.assertEqual(load_gap_ledger(state_root, "root-absolute"), [])

    def test_catalog_unavailable_becomes_durable_attention_without_rewriting(self) -> None:
        files = {"/incoming/one/Fate Zero/S01E01.mkv": FAKE_VIDEO_BYTES}
        state_root, alist, runner, _p, executor_events = self._setup(files)
        root_task_id = "root-gap-attention"
        pending = runner.create_pending_job("/incoming/one", job_id=root_task_id)
        runner.start_automatic_job(pending.id, target_shelf="anime")
        analyze_root_boundaries(
            alist, "/incoming/one", root_task_id=root_task_id, state_root=state_root,
        )
        record = load_work_unit_records(state_root, root_task_id)[0]
        apply_work_unit_override(
            state_root, root_task_id, record.work_unit_id,
            media_type="tv", tmdb_id=101,
        )
        reconcile_root_work_units(alist, "/library", state_root, root_task_id)

        first = execute_new_work_units(runner, state_root, root_task_id)

        self.assertEqual(first[0].outcome, "accepted")
        record = load_work_unit_records(state_root, root_task_id)[0]
        self.assertEqual(record.gap_status, "attention")
        self.assertIn("缺口", record.attention or "")
        from local.scrapeflow_api.root_aggregation import aggregate_root_job

        self.assertEqual(aggregate_root_job(state_root, root_task_id).attention, 1)
        # A later pass revisits J only; it must never schedule a second G/H
        # writer for the already accepted carrier.
        execute_new_work_units(runner, state_root, root_task_id)
        self.assertEqual(len(executor_events), 1)

    def test_gap_ledger_persistence_failure_is_durable_failed_state(self) -> None:
        files = {"/incoming/one/Fate Zero/S01E01.mkv": FAKE_VIDEO_BYTES}
        state_root, alist, runner, _p, executor_events = self._setup(
            files, tmdb=CatalogTMDB(101, 1),
        )
        root_task_id = "root-gap-ledger-failure"
        pending = runner.create_pending_job("/incoming/one", job_id=root_task_id)
        runner.start_automatic_job(pending.id, target_shelf="anime")
        analyze_root_boundaries(
            alist, "/incoming/one", root_task_id=root_task_id, state_root=state_root,
        )
        record = load_work_unit_records(state_root, root_task_id)[0]
        apply_work_unit_override(
            state_root, root_task_id, record.work_unit_id,
            media_type="tv", tmdb_id=101,
        )
        reconcile_root_work_units(alist, "/library", state_root, root_task_id)

        with patch(
            "local.scrapeflow_api.unit_execution.discover_episode_gaps",
            side_effect=OSError("injected ledger fsync failure"),
        ):
            results = execute_new_work_units(runner, state_root, root_task_id)

        self.assertEqual(results[0].outcome, "accepted")
        record = load_work_unit_records(state_root, root_task_id)[0]
        self.assertEqual(record.gap_status, "failed")
        self.assertIn("缺口账本", record.gap_detail or "")
        from local.scrapeflow_api.root_aggregation import aggregate_root_job

        self.assertEqual(aggregate_root_job(state_root, root_task_id).failed, 1)
        self.assertEqual(len(executor_events), 1)

    def test_deleted_declared_empty_scope_cannot_pass_fresh_boundary_proof(self) -> None:
        root = "/incoming/Northwind"
        files = {
            f"{root}/Northwind.Show.S01.1080p/Northwind.Show.S01E01.mkv": FAKE_VIDEO_BYTES,
            f"{root}/Northwind.Show.S02.1080p/Northwind.Show.S02E01.mkv": FAKE_VIDEO_BYTES,
        }
        with tempfile.TemporaryDirectory() as directory:
            state_root = Path(directory)
            alist = IndexAList(files)
            # B/W sees this adjacent declared empty season.  It disappears
            # before F, which must not look equivalent to an empty listing.
            empty_scope = f"{root}/Northwind.Show.S03.1080p"
            alist.dirs.add(empty_scope)
            runner = SimpleEngineRunner(
                state_root,
                alist=alist,
                tmdb=object(),
                planner=_recording_planner([]),
                validate=False,
                library_root="/library",
                executor=lambda _plan: {"ok": True},
            )
            pending = runner.create_pending_job(root, job_id="root-fresh-scope")
            runner.start_automatic_job(pending.id, target_shelf="anime")
            analyze_root_boundaries(
                alist, root, root_task_id=pending.id, state_root=state_root,
            )
            cohort = next(
                record for record in load_work_unit_records(state_root, pending.id)
                if len(record.source_paths) > 1
            )
            self.assertEqual(cohort.claimed_seasons, (1, 2, 3))
            apply_work_unit_override(
                state_root, pending.id, cohort.work_unit_id,
                media_type="tv", tmdb_id=101,
            )
            cohort = next(
                record for record in load_work_unit_records(state_root, pending.id)
                if record.work_unit_id == cohort.work_unit_id
            )
            alist.dirs.discard(empty_scope)

            with self.assertRaisesRegex(ValueError, "可证明目录"):
                _request_for_unit(runner, cohort, pending.id, state_root)

    def test_new_sibling_nests_under_duplicate_main_work_root(self) -> None:
        """A D-locked main TV root remains the parent for a new sibling.

        This is intentionally a mixed-shelf case: the root was authorised as
        anime, while the pre-existing main work is in 欧美剧.  The new movie
        must inherit that real existing root instead of being planned at the
        newly selected anime shelf.
        """
        files = {
            "/incoming/container/Main Show/S01E01.mkv": FAKE_VIDEO_BYTES,
            "/incoming/container/Side Film/Feature.mkv": FAKE_VIDEO_BYTES,
            "/library/欧美剧/Main Show/tvshow.nfo": _nfo_tv(101, "Main Show", "2020"),
            "/library/欧美剧/Main Show/Season 01/S01E01.mkv": b"v",
        }
        state_root, alist, runner, planner_events, executor_events = self._setup(files)
        root_task_id = "root-existing-main"
        pending = runner.create_pending_job("/incoming/container", job_id=root_task_id)
        runner.start_automatic_job(pending.id, target_shelf="anime")
        analyze_root_boundaries(
            alist, "/incoming/container", root_task_id=root_task_id,
            state_root=state_root,
        )
        records = load_work_unit_records(state_root, root_task_id)
        self.assertEqual(len(records), 2)
        for record in records:
            tmdb_id = 101 if record.source_paths == ("/incoming/container/Main Show",) else 202
            media_type = "tv" if tmdb_id == 101 else "movie"
            apply_work_unit_override(
                state_root, root_task_id, record.work_unit_id,
                media_type=media_type, tmdb_id=tmdb_id,
            )

        reconciled = reconcile_root_work_units(
            alist, "/library", state_root, root_task_id,
        )
        main = next(record for record in reconciled if record.identity["tmdb_id"] == 101)
        side = next(record for record in reconciled if record.identity["tmdb_id"] == 202)
        self.assertEqual(main.reconciliation_outcome, "duplicate_complete")
        self.assertEqual(main.matched_work_root, "/library/欧美剧/Main Show")
        self.assertEqual(side.reconciliation_outcome, "new_work")

        results = execute_new_work_units(runner, state_root, root_task_id)

        self.assertEqual({result.outcome for result in results}, {"accepted", "skipped"})
        self.assertEqual(len(executor_events), 1)
        side_event = next(event for event in planner_events if event["tmdb_id"] == 202)
        self.assertEqual(side_event["parent_path"], "/library/欧美剧/Main Show")

    def test_failed_new_main_does_not_write_a_sibling_below_unaccepted_root(self) -> None:
        files = {
            "/incoming/container/Main Show/S01E01.mkv": FAKE_VIDEO_BYTES,
            "/incoming/container/Side Film/Feature.mkv": FAKE_VIDEO_BYTES,
        }
        state_root, alist, runner, planner_events, executor_events = self._setup(files)
        root_task_id = "root-main-failure"
        pending = runner.create_pending_job("/incoming/container", job_id=root_task_id)
        runner.start_automatic_job(pending.id, target_shelf="anime")
        analyze_root_boundaries(
            alist, "/incoming/container", root_task_id=root_task_id,
            state_root=state_root,
        )
        for record in load_work_unit_records(state_root, root_task_id):
            tmdb_id = 101 if record.source_paths == ("/incoming/container/Main Show",) else 202
            apply_work_unit_override(
                state_root, root_task_id, record.work_unit_id,
                media_type="tv" if tmdb_id == 101 else "movie",
                tmdb_id=tmdb_id,
            )
        reconcile_root_work_units(alist, "/library", state_root, root_task_id)
        runner.planner = _recording_planner(
            planner_events, fail_for="/incoming/container/Main Show",
        )

        results = execute_new_work_units(runner, state_root, root_task_id)

        self.assertEqual([result.outcome for result in results], ["failed"])
        self.assertEqual([event["tmdb_id"] for event in planner_events], [101])
        self.assertEqual(executor_events, [])


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

    def test_physical_oad_proof_carries_only_sp_to_regular_episode_map(self) -> None:
        """F may map OAD source keys only from the exact persisted D proof."""
        class OadTMDB(BareEpisodePlanningTMDB):
            def get(self, path: str, **_params: object) -> dict[str, object]:
                if path == f"/tv/{self.tmdb_id}":
                    return {
                        "name": "Example OAD",
                        "original_name": "Example OAD",
                        "first_air_date": "2020-01-01",
                        "number_of_seasons": 1,
                        "number_of_episodes": 5,
                        "seasons": [{
                            "season_number": 1,
                            "episode_count": 5,
                            "name": "OAD",
                        }],
                    }
                if path == f"/tv/{self.tmdb_id}/season/1":
                    return {"episodes": [{
                        "episode_number": number,
                        "air_date": "2020-01-01",
                        "name": f"OAD #{number}",
                    } for number in range(1, 6)]}
                return {}

        source = "/incoming/Example OAD"
        files = {
            f"{source}/Example [OAD{number:02d}].mkv": FAKE_VIDEO_BYTES
            for number in range(1, 6)
        }
        tmdb = OadTMDB(99100, 5)
        state_root, alist, runner = self._setup(files, tmdb)
        root_task_id = "root-oad-map"
        pending = runner.create_pending_job(source, job_id=root_task_id)
        runner.start_automatic_job(pending.id, target_shelf="anime")
        analyze_root_boundaries(
            alist, source, root_task_id=root_task_id, state_root=state_root,
        )
        record = load_work_unit_records(state_root, root_task_id)[0]
        apply_work_unit_override(
            state_root, root_task_id, record.work_unit_id,
            media_type="tv", tmdb_id=99100,
        )
        record = reconcile_root_work_units(
            alist, "/library", state_root, root_task_id,
            episode_catalog=TmdbEpisodeCatalog(tmdb), tmdb_client=tmdb,
        )[0]
        self.assertEqual(
            record.reconciliation_evidence and record.reconciliation_evidence["kind"],
            "tmdb_single_positive_season_physical_special",
        )
        request = _request_for_unit(runner, record, root_task_id, state_root)
        self.assertEqual(request.season, 1)
        self.assertIsNotNone(request.episode_map_path)
        mapping = json.loads(Path(request.episode_map_path).read_text(encoding="utf-8"))
        self.assertEqual(
            mapping,
            {f"SP{number:02d}": f"S01E{number:02d}" for number in range(1, 6)},
        )

    def test_named_arc_oad_proof_maps_release_ordinals_onto_official_window(self) -> None:
        """A named-arc Season 00 proof maps ``SP01``/``SP02`` to ``S00E08``/``E09``.

        The D proof carries the official window the release ordinals were
        proved onto.  F must use those proved tokens instead of guessing a
        1-based Season 00 position.
        """
        class ParentArcTMDB:
            def __init__(self, tmdb_id: int) -> None:
                self.tmdb_id = tmdb_id

            def get(self, path: str, **_params: object) -> dict[str, object]:
                if path == f"/tv/{self.tmdb_id}":
                    return {
                        "name": "示例剧",
                        "original_name": "示例剧",
                        "first_air_date": "2006-04-04",
                        "number_of_seasons": 2,
                        "number_of_episodes": 212,
                        "seasons": [
                            {"season_number": 0, "episode_count": 11, "name": "特别篇"},
                            {"season_number": 1, "episode_count": 201, "name": "第 1 季"},
                        ],
                    }
                if path == f"/tv/{self.tmdb_id}/season/0":
                    names = {
                        1: "短篇 1", 2: "短篇 2", 3: "短篇 3", 4: "短篇 4",
                        5: "短篇 5", 6: "短篇 6", 7: "短篇 7",
                        8: "示例剧 爱染香篇 前篇",
                        9: "示例剧 爱染香篇 后篇",
                        10: "周年感谢祭",
                        11: "番外兔子",
                    }
                    air = {8: "2016-05-13", 9: "2016-06-10"}
                    return {
                        "episodes": [
                            {
                                "episode_number": number,
                                "air_date": air.get(number, "2007-01-01"),
                                "name": names[number],
                            }
                            for number in range(1, 12)
                        ]
                    }
                if path == f"/tv/{self.tmdb_id}/season/1":
                    return {
                        "episodes": [
                            {
                                "episode_number": number,
                                "air_date": "2006-04-04",
                                "name": f"第{number}集",
                            }
                            for number in range(1, 202)
                        ]
                    }
                return {}

        source = "/incoming/示例剧 爱染香篇"
        files = {
            f"{source}/示例剧 OAD 2016 [{number:02d}][Ma10p_2160p].mkv": FAKE_VIDEO_BYTES
            for number in (1, 2)
        }
        tmdb = ParentArcTMDB(99101)
        state_root, alist, runner = self._setup(files, tmdb)
        root_task_id = "root-named-arc-map"
        pending = runner.create_pending_job(source, job_id=root_task_id)
        runner.start_automatic_job(pending.id, target_shelf="anime")
        analyze_root_boundaries(
            alist, source, root_task_id=root_task_id, state_root=state_root,
        )
        record = load_work_unit_records(state_root, root_task_id)[0]
        apply_work_unit_override(
            state_root, root_task_id, record.work_unit_id,
            media_type="tv", tmdb_id=99101,
        )
        record = reconcile_root_work_units(
            alist, "/library", state_root, root_task_id,
            episode_catalog=TmdbEpisodeCatalog(tmdb), tmdb_client=tmdb,
        )[0]
        self.assertEqual(
            record.reconciliation_evidence and record.reconciliation_evidence["episode_tokens"],
            ["S00E08", "S00E09"],
        )
        request = _request_for_unit(runner, record, root_task_id, state_root)
        self.assertEqual(request.season, 0)
        self.assertIsNotNone(request.episode_map_path)
        mapping = json.loads(Path(request.episode_map_path).read_text(encoding="utf-8"))
        self.assertEqual(
            mapping,
            {"SP01": "S00E08", "SP02": "S00E09"},
        )

    def test_titled_single_ova_maps_onto_official_season00_episode(self) -> None:
        """An unnumbered single OVA positions by its arc title alone.

        ``X 4k 示例剧/示例剧 和猫老师的初次跑腿/[Ygm] … OVA ….mkv`` carries no
        release ordinal at all, so no run grammar can position it.  C confirms
        the parent show (its cleaned parent query leads), and D must prove the
        concrete arc title — minus the parent show's own title — against the
        published Season 00 titles whose ``OVA1：``-style ordinal prefixes are
        stripped, tolerating the one-character release-label drift.  F then
        inherits season 0 without an explicit episode-map bridge.
        """
        class TitledSingleOvaTMDB:
            def __init__(self, tmdb_id: int) -> None:
                self.tmdb_id = tmdb_id

            def get(self, path: str, **params: object) -> dict[str, object]:
                if path == "/search/tv":
                    if str(params.get("query", "")) == "示例剧":
                        return {
                            "results": [
                                {
                                    "id": self.tmdb_id,
                                    "name": "示例剧",
                                    "first_air_date": "2008-07-01",
                                    "genre_ids": [16],
                                },
                            ],
                        }
                    return {"results": []}
                if path == "/search/movie":
                    return {"results": []}
                if path == f"/tv/{self.tmdb_id}":
                    return {
                        "name": "示例剧",
                        "original_name": "示例剧",
                        "first_air_date": "2008-07-01",
                        "number_of_seasons": 2,
                        "number_of_episodes": 27,
                        "seasons": [
                            {"season_number": 0, "episode_count": 14, "name": "特别篇"},
                            {"season_number": 1, "episode_count": 13, "name": "第 1 季"},
                        ],
                    }
                if path == f"/tv/{self.tmdb_id}/season/0":
                    names = {
                        1: "3D猫咪剧场 1", 2: "3D猫咪剧场 2", 3: "3D猫咪剧场 3",
                        4: "3D猫咪剧场 4", 5: "3D猫咪剧场 5",
                        6: "OVA1：和猫咪老师的初次跑腿",
                        7: "OVA2：曾几何时下雪之日",
                        8: "第五季OVA1：一夜酒杯", 9: "第五季OVA2：游戏之宴",
                        10: "第六季OVA1：铃响的残株", 11: "第六季OVA2：梦幻的碎片",
                        12: "一番赏 示例剧 猫咪老师和花卉图鉴",
                        13: "示例剧×熊本县《人吉・球磨的温柔时光》",
                        14: "第七季OVA：伸手可及的地方",
                    }
                    air = {6: "2013-12-15", 7: "2014-02-05"}
                    return {
                        "episodes": [
                            {
                                "episode_number": number,
                                "air_date": air.get(number, "2009-04-22"),
                                "name": names[number],
                            }
                            for number in range(1, 15)
                        ]
                    }
                if path == f"/tv/{self.tmdb_id}/season/1":
                    return {
                        "episodes": [
                            {
                                "episode_number": number,
                                "air_date": "2008-07-01",
                                "name": f"第{number}集",
                            }
                            for number in range(1, 14)
                        ]
                    }
                return {}

        source = "/incoming/X 4k 示例剧"
        files = {
            f"{source}/示例剧 和猫老师的初次跑腿/[Ygm] Example OVA [Ma10p_2160p].mkv": FAKE_VIDEO_BYTES,
            f"{source}/示例剧 曾几何时下雪日/[Ygm] Example OVA [Ma10p_2160p].mkv": FAKE_VIDEO_BYTES,
        }
        tmdb = TitledSingleOvaTMDB(99102)
        state_root, alist, runner = self._setup(files, tmdb)
        root_task_id = "root-titled-single-ova"
        pending = runner.create_pending_job(source, job_id=root_task_id)
        runner.start_automatic_job(pending.id, target_shelf="anime")
        analyze_root_boundaries(
            alist, source, root_task_id=root_task_id, state_root=state_root,
        )
        records = resolve_work_unit_identities(
            tmdb, state_root, root_task_id, prefer_animation=True,
        )
        by_label = {record.display_label: record for record in records}
        self.assertEqual(len(records), 2)
        for label in ("示例剧 和猫老师的初次跑腿", "示例剧 曾几何时下雪日"):
            self.assertEqual(by_label[label].identity_status, "confirmed")
            self.assertEqual(by_label[label].identity["tmdb_id"], 99102)
            self.assertEqual(by_label[label].identity["title"], "示例剧")
        reconciled = reconcile_root_work_units(
            alist, "/library", state_root, root_task_id,
            episode_catalog=TmdbEpisodeCatalog(tmdb), tmdb_client=tmdb,
        )
        by_label = {record.display_label: record for record in reconciled}
        self.assertEqual(
            by_label["示例剧 和猫老师的初次跑腿"].reconciliation_evidence["episode_tokens"],
            ["S00E06"],
        )
        self.assertEqual(
            by_label["示例剧 曾几何时下雪日"].reconciliation_evidence["episode_tokens"],
            ["S00E07"],
        )
        request = _request_for_unit(
            runner, by_label["示例剧 和猫老师的初次跑腿"], root_task_id, state_root,
        )
        self.assertEqual(request.season, 0)
        self.assertIsNone(request.episode_map_path)

    def test_dual_encoded_unnumbered_ova_maps_onto_single_proved_special(self) -> None:
        """A dual-encoded unnumbered OVA scope still proves one S00 episode.

        ``03 OVA：黑色的铁碎牙（2008）内封+外挂字幕 1080P`` holds two encodes of
        one special: a hardsub mp4 named only by release packaging and a
        softsub mkv named by the arc title.  The boundary's sibling-position
        prefix ``03 OVA`` feeds the scope-level ordinal parse through the
        ancestor path, and the boundary label itself is too polluted for arc
        matching — the proof must ignore ancestor-fed ordinals, anchor the
        arc title on the video stems, and let every anchor agree on exactly
        one published Season 00 episode.
        """
        class DualEncodedOvaTMDB:
            def __init__(self, tmdb_id: int) -> None:
                self.tmdb_id = tmdb_id

            def get(self, path: str, **params: object) -> dict[str, object]:
                if path == "/search/tv":
                    return {"results": []}
                if path == "/search/movie":
                    return {"results": []}
                if path == f"/tv/{self.tmdb_id}":
                    return {
                        "name": "示例剧",
                        "original_name": "示例剧",
                        "first_air_date": "2000-10-16",
                        "number_of_seasons": 2,
                        "number_of_episodes": 14,
                        "seasons": [
                            {"season_number": 0, "episode_count": 1, "name": "特别篇"},
                            {"season_number": 1, "episode_count": 13, "name": "第 1 季"},
                        ],
                    }
                if path == f"/tv/{self.tmdb_id}/season/0":
                    return {
                        "episodes": [
                            {
                                "episode_number": 1,
                                "air_date": "2008-07-30",
                                "name": "黑色铁碎牙",
                            },
                        ]
                    }
                if path == f"/tv/{self.tmdb_id}/season/1":
                    return {
                        "episodes": [
                            {
                                "episode_number": number,
                                "air_date": "2000-10-16",
                                "name": f"第{number}集",
                            }
                            for number in range(1, 14)
                        ]
                    }
                return {}

        source = "/incoming/X 4k 示例剧"
        unit = f"{source}/03 OVA：黑色的铁碎牙（2008）内封+外挂字幕 1080P"
        files = {
            f"{unit}/1080P 内嵌简中字幕.mp4": FAKE_VIDEO_BYTES,
            f"{unit}/1080P 外挂简中字幕/OVA：黑色的铁碎牙 1080P.mkv": FAKE_VIDEO_BYTES,
            f"{unit}/1080P 外挂简中字幕/简中.ass": b"[Script Info]\n",
        }
        tmdb = DualEncodedOvaTMDB(99104)
        state_root, alist, runner = self._setup(files, tmdb)
        root_task_id = "root-dual-encoded-ova"
        pending = runner.create_pending_job(source, job_id=root_task_id)
        runner.start_automatic_job(pending.id, target_shelf="anime")
        analyze_root_boundaries(
            alist, source, root_task_id=root_task_id, state_root=state_root,
        )
        records = load_work_unit_records(state_root, root_task_id)
        self.assertEqual(len(records), 1)
        apply_work_unit_override(
            state_root, root_task_id, records[0].work_unit_id,
            media_type="tv", tmdb_id=99104,
        )
        record = reconcile_root_work_units(
            alist, "/library", state_root, root_task_id,
            episode_catalog=TmdbEpisodeCatalog(tmdb), tmdb_client=tmdb,
        )[0]
        self.assertEqual(
            record.reconciliation_evidence and record.reconciliation_evidence["episode_tokens"],
            ["S00E01"],
        )
        request = _request_for_unit(runner, record, root_task_id, state_root)
        self.assertEqual(request.season, 0)
        self.assertIsNone(request.episode_map_path)

    def test_two_distinct_unnumbered_ova_stems_stay_unproven(self) -> None:
        """Two arc-titled stems that win different episodes stay unproven.

        The same ordinal-free admission must not collapse two distinct
        specials onto one episode: when the video stems match two different
        published Season 00 episodes, the proof fails closed instead of
        silently rewriting one arc onto the other's coordinate.
        """
        class TwoArcOvaTMDB:
            def __init__(self, tmdb_id: int) -> None:
                self.tmdb_id = tmdb_id

            def get(self, path: str, **params: object) -> dict[str, object]:
                if path == f"/tv/{self.tmdb_id}":
                    return {
                        "name": "示例剧",
                        "original_name": "示例剧",
                        "first_air_date": "2000-10-16",
                        "number_of_seasons": 2,
                        "number_of_episodes": 15,
                        "seasons": [
                            {"season_number": 0, "episode_count": 2, "name": "特别篇"},
                            {"season_number": 1, "episode_count": 13, "name": "第 1 季"},
                        ],
                    }
                if path == f"/tv/{self.tmdb_id}/season/0":
                    return {
                        "episodes": [
                            {
                                "episode_number": 1,
                                "air_date": "2008-07-30",
                                "name": "黑色铁碎牙",
                            },
                            {
                                "episode_number": 2,
                                "air_date": "2008-07-31",
                                "name": "银色铁碎牙",
                            },
                        ]
                    }
                if path == f"/tv/{self.tmdb_id}/season/1":
                    return {
                        "episodes": [
                            {
                                "episode_number": number,
                                "air_date": "2000-10-16",
                                "name": f"第{number}集",
                            }
                            for number in range(1, 14)
                        ]
                    }
                return {}

        source = "/incoming/X 4k 示例剧"
        unit = f"{source}/03 OVA（2008）内封+外挂字幕 1080P"
        files = {
            f"{unit}/1080P 外挂简中字幕/OVA：黑色的铁碎牙 1080P.mkv": FAKE_VIDEO_BYTES,
            f"{unit}/1080P 内嵌简中字幕/OVA：银色铁碎牙 1080P.mp4": FAKE_VIDEO_BYTES,
        }
        tmdb = TwoArcOvaTMDB(99105)
        state_root, alist, runner = self._setup(files, tmdb)
        root_task_id = "root-two-arc-ova"
        pending = runner.create_pending_job(source, job_id=root_task_id)
        runner.start_automatic_job(pending.id, target_shelf="anime")
        analyze_root_boundaries(
            alist, source, root_task_id=root_task_id, state_root=state_root,
        )
        records = load_work_unit_records(state_root, root_task_id)
        self.assertEqual(len(records), 1)
        apply_work_unit_override(
            state_root, root_task_id, records[0].work_unit_id,
            media_type="tv", tmdb_id=99105,
        )
        record = reconcile_root_work_units(
            alist, "/library", state_root, root_task_id,
            episode_catalog=TmdbEpisodeCatalog(tmdb), tmdb_client=tmdb,
        )[0]
        self.assertIsNone(record.reconciliation_evidence)


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

class InterruptedUnitCarrierRecoveryTests(unittest.TestCase):
    def test_restart_retry_wait_recovers_owned_partial_carrier_without_replay(self) -> None:
        """An interrupted internal carrier resumes from its durable plan.

        The first media file has already reached the formal target when the
        process restarts.  Startup converts ``executing`` to ``retry_wait``;
        the next root pass must exact-read that carrier, preserve the first
        target, and move only the still-present second source.
        """
        root_task_id = "root-partial-recovery"
        source_root = "/incoming/recovery"
        target_root = "/library/番剧/Restart Work"
        first_source = f"{source_root}/Restart.Work.S01E01.mkv"
        second_source = f"{source_root}/Restart.Work.S01E02.mkv"
        first_target = f"{target_root}/Season 01/Restart Work - S01E01.mkv"
        second_target = f"{target_root}/Season 01/Restart Work - S01E02.mkv"
        planner_calls: list[object] = []

        def planner(request, _alist, _tmdb) -> Plan:
            planner_calls.append(request)
            return Plan(
                mode="tv",
                source_root=request.source_path,
                target_root=target_root,
                files=[
                    PlannedFile(
                        source_path=first_source,
                        source_dir=source_root,
                        original_name="Restart.Work.S01E01.mkv",
                        final_name="Restart Work - S01E01.mkv",
                        target_dir=f"{target_root}/Season 01",
                        media_kind="video",
                        episode_key="E01",
                        source_size=FAKE_VIDEO_SIZE,
                    ),
                    PlannedFile(
                        source_path=second_source,
                        source_dir=source_root,
                        original_name="Restart.Work.S01E02.mkv",
                        final_name="Restart Work - S01E02.mkv",
                        target_dir=f"{target_root}/Season 01",
                        media_kind="video",
                        episode_key="E02",
                        source_size=FAKE_VIDEO_SIZE,
                    ),
                ],
                warnings=[],
                metadata={
                    "tmdb_id": 101,
                    "title": "Restart Work",
                    "year": "2020",
                    "poster_path": None,
                    "backdrop_path": None,
                },
            )

        class PauseAfterFirstMove(SimplePlanExecutor):
            def execute(self, plan):  # type: ignore[no-untyped-def]
                item = plan.files[0]
                self._move_file(
                    item.source_dir,
                    item.target_dir,
                    item.original_name,
                    item.final_name,
                )
                self._check_size(
                    f"{item.target_dir}/{item.final_name}", item.source_size,
                )
                self._verify_source_absent(item.source_path)
                raise EnginePauseRequested("injected interrupted formal write")

        with tempfile.TemporaryDirectory() as directory:
            state_root = Path(directory)
            alist = FakeAList()
            alist.files = {
                first_source: FAKE_VIDEO_BYTES,
                second_source: FAKE_VIDEO_BYTES,
            }
            tmdb = CatalogTMDB(101, 2)
            runner = SimpleEngineRunner(
                state_root,
                alist=alist,
                tmdb=tmdb,
                planner=planner,
                executor=PauseAfterFirstMove(alist, tmdb),
                validate=False,
                library_root="/library",
            )
            pending = runner.create_pending_job(source_root, job_id=root_task_id)
            runner.start_automatic_job(pending.id, target_shelf="anime")
            analyze_root_boundaries(
                alist,
                source_root,
                root_task_id=root_task_id,
                state_root=state_root,
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
                execute_new_work_units(runner, state_root, root_task_id)

            record = load_work_unit_records(state_root, root_task_id)[0]
            self.assertIsNotNone(record.writer_job_id)
            carrier_id = str(record.writer_job_id)
            self.assertEqual(runner.get_job(carrier_id).phase, "executing")
            self.assertEqual(alist.files[first_target], FAKE_VIDEO_BYTES)
            self.assertNotIn(first_source, alist.files)
            self.assertEqual(alist.files[second_source], FAKE_VIDEO_BYTES)

            # This is the process-start transition which used to strand the
            # unit in retry_wait and then fail before it could read back the
            # partially moved first episode.
            recover_persisted_engine_jobs(state_root)
            self.assertEqual(runner.get_job(carrier_id).phase, "retry_wait")

            runner.executor = SimplePlanExecutor(alist, tmdb)
            # The missing second target follows the visibility retry schedule;
            # collapse sleeps in this focused state-machine regression.
            with patch("local.scrapeflow_api.simple_engine_runner.time.sleep"):
                results = execute_new_work_units(runner, state_root, root_task_id)

            self.assertEqual([result.outcome for result in results], ["accepted"])
            self.assertEqual(len(planner_calls), 1)
            self.assertEqual(runner.get_job(carrier_id).phase, "executed")
            self.assertEqual(alist.files[first_target], FAKE_VIDEO_BYTES)
            self.assertEqual(alist.files[second_target], FAKE_VIDEO_BYTES)
            self.assertNotIn(first_source, alist.files)
            self.assertNotIn(second_source, alist.files)
            # First move occurred before the simulated restart; continuation
            # moved only E02, proving the existing E01 target was not replayed.
            self.assertEqual(len(alist.moves), 2)
            self.assertEqual(alist.moves[0][2], ["Restart.Work.S01E01.mkv"])
            self.assertEqual(alist.moves[1][2], ["Restart.Work.S01E02.mkv"])
            execution = runner.get_job(carrier_id).execution or {}
            self.assertEqual(
                [row["status"] for row in execution.get("files", [])],
                ["already_present", "moved"],
            )

    def test_failed_owned_legacy_name_carrier_keeps_plan_and_resumes_rename(self) -> None:
        """A failed pre-policy carrier must not be retired against a mutated source."""
        root_task_id = "root-legacy-name-recovery"
        source_root = "/incoming/legacy-name"
        target_root = "/library/番剧/Legacy Name Work"
        first_source = f"{source_root}/Legacy.Name.S01E01.mkv"
        second_source = f"{source_root}/Legacy.Name.S01E02.mkv"
        first_intermediate = f"{target_root}/Season 01/Legacy.Name.S01E01.mkv"
        first_final = f"{target_root}/Season 01/Legacy Name Work - S01E01 - First-Title.mkv"
        second_final = f"{target_root}/Season 01/Legacy Name Work - S01E02 - Second.mkv"
        planner_calls: list[object] = []

        def planner(request, _alist, _tmdb) -> Plan:
            planner_calls.append(request)
            return Plan(
                mode="tv",
                source_root=request.source_path,
                target_root=target_root,
                files=[
                    PlannedFile(
                        source_path=first_source,
                        source_dir=source_root,
                        original_name="Legacy.Name.S01E01.mkv",
                        final_name="Legacy Name Work - S01E01 - First...Title.mkv",
                        target_dir=f"{target_root}/Season 01",
                        media_kind="video",
                        episode_key="E01",
                        source_size=FAKE_VIDEO_SIZE,
                    ),
                    PlannedFile(
                        source_path=second_source,
                        source_dir=source_root,
                        original_name="Legacy.Name.S01E02.mkv",
                        final_name="Legacy Name Work - S01E02 - Second.mkv",
                        target_dir=f"{target_root}/Season 01",
                        media_kind="video",
                        episode_key="E02",
                        source_size=FAKE_VIDEO_SIZE,
                    ),
                ],
                warnings=[],
                metadata={
                    "tmdb_id": 101,
                    "title": "Legacy Name Work",
                    "year": "2020",
                    "poster_path": None,
                    "backdrop_path": None,
                },
            )

        with tempfile.TemporaryDirectory() as directory:
            state_root = Path(directory)
            alist = FakeAList()
            alist.files = {
                first_source: FAKE_VIDEO_BYTES,
                second_source: FAKE_VIDEO_BYTES,
            }
            runner = SimpleEngineRunner(
                state_root,
                alist=alist,
                tmdb=CatalogTMDB(101, 2),
                planner=planner,
                validate=False,
                library_root="/library",
            )
            pending = runner.create_pending_job(source_root, job_id=root_task_id)
            runner.start_automatic_job(pending.id, target_shelf="anime")
            analyze_root_boundaries(
                alist,
                source_root,
                root_task_id=root_task_id,
                state_root=state_root,
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
            record = load_work_unit_records(state_root, root_task_id)[0]
            request = _request_for_unit(runner, record, root_task_id, state_root)
            carrier = runner.plan_job(
                request,
                job_id=f"unit-{record.work_unit_id}",
                internal_child_of=root_task_id,
            )
            # Model the exact prior provider effect: the cross-directory move
            # reached formal storage, but the old `...` final rename failed.
            alist.files[first_intermediate] = alist.files.pop(first_source)
            failed = replace(carrier, phase="failed", error="legacy rename rejected")
            atomic_write_json(
                runner._job_path(carrier.id),  # noqa: SLF001 - durable carrier fixture
                failed.as_dict(),
                allow_nan=False,
            )
            save_work_unit_records(
                state_root,
                root_task_id,
                [replace(record, writer_job_id=carrier.id)],
            )

            results = execute_new_work_units(runner, state_root, root_task_id)

            self.assertEqual([result.outcome for result in results], ["accepted"])
            self.assertEqual(len(planner_calls), 1)
            recovered = runner.get_job(carrier.id)
            self.assertEqual(recovered.phase, "executed")
            self.assertEqual(recovered.plan["files"][0]["final_name"], first_final.rsplit("/", 1)[1])
            self.assertIn(first_final, alist.files)
            self.assertIn(second_final, alist.files)
            self.assertNotIn(first_intermediate, alist.files)
            self.assertNotIn(first_source, alist.files)
            self.assertNotIn(second_source, alist.files)
            self.assertEqual(len(alist.moves), 1)
            self.assertEqual(alist.moves[0][2], ["Legacy.Name.S01E02.mkv"])
            self.assertEqual(
                [row["status"] for row in (recovered.execution or {})["files"]],
                ["renamed_after_interrupted_move", "moved"],
            )


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


class ConsumedSourceContinuationTests(unittest.TestCase):
    """A partially written source is continued, not deadlocked.

    Shape under test: the writer moved every planned media object and the
    run then failed during artifacts (before the carrier persisted).  The
    retry must re-plan from the consumed B-snapshot objects; the executor
    re-reads each moved target byte-exactly (already_present) and only
    regenerates the missing artifacts.
    """

    def _setup_root(self, files):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        state_root = Path(temp.name)
        alist = IndexAList(dict(files))
        plan_calls: list[dict] = []
        fail_once = {"remaining": 1}

        def executor(plan):
            for item in plan.files:
                source = f"{item.source_dir.rstrip('/')}/{item.original_name}"
                target = f"{item.target_dir.rstrip('/')}/{item.final_name}"
                if source in alist.files:
                    # First (failing) pass: perform the real move, then fail
                    # during artifacts.
                    alist.move(item.source_dir, item.target_dir, [item.original_name])
                else:
                    # Continuation pass: the target must already hold the
                    # exact bytes (already_present readback).
                    payload = alist.files.get(target)
                    if payload is None:
                        raise RuntimeError(f"continuation target missing: {target}")
                    if item.source_size is not None and len(payload) != item.source_size:
                        raise RuntimeError(f"continuation target size drift: {target}")
            if fail_once["remaining"] > 0:
                fail_once["remaining"] -= 1
                raise RuntimeError("injected artifact failure after moves")
            return {"ok": True, "files": [], "file_count": 0, "artifacts": [], "artifact_count": 0, "cleanup": [], "cleanup_count": 0}

        runner = SimpleEngineRunner(
            state_root, alist=alist, tmdb=object(),
            planner=_recording_planner(plan_calls), validate=False,
            library_root="/library", executor=executor,
        )
        pending = runner.create_pending_job("/incoming/one", job_id="root-cont")
        runner.start_automatic_job(pending.id, target_shelf="anime")
        analyze_root_boundaries(
            alist, "/incoming/one", root_task_id="root-cont", state_root=state_root,
        )
        records = load_work_unit_records(state_root, "root-cont")
        apply_work_unit_override(
            state_root, "root-cont", records[0].work_unit_id,
            media_type="tv", tmdb_id=101,
        )
        return runner, state_root, alist, plan_calls, records

    def test_consumed_source_continues_after_artifact_failure(self) -> None:
        files = {"/incoming/one/S01E01.mkv": FAKE_VIDEO_BYTES}
        runner, state_root, alist, plan_calls, records = self._setup_root(files)
        reconcile_root_work_units(alist, "/library", state_root, "root-cont")
        first = execute_new_work_units(runner, state_root, "root-cont")
        self.assertEqual(first[0].outcome, "failed")
        # The executor already performed the moves: the source is consumed
        # and the library holds the media.
        self.assertNotIn("/incoming/one/My Show/S01E01.mkv", alist.files)
        self.assertIn(
            "/library/番剧/Work (101)/S01E01.mkv", alist.files,
        )
        # The retry continues from the consumed snapshot instead of failing
        # on the drifted source.
        second = execute_new_work_units(runner, state_root, "root-cont")
        self.assertEqual(second[0].outcome, "accepted")
        self.assertEqual(len(plan_calls), 2)

    def test_consumed_source_with_wrong_target_bytes_stays_failed(self) -> None:
        files = {"/incoming/one/S01E01.mkv": FAKE_VIDEO_BYTES}
        runner, state_root, alist, plan_calls, records = self._setup_root(files)
        reconcile_root_work_units(alist, "/library", state_root, "root-cont")
        first = execute_new_work_units(runner, state_root, "root-cont")
        self.assertEqual(first[0].outcome, "failed")
        # Corrupt the moved target: the continuation readback must not pass.
        alist.files["/library/番剧/Work (101)/S01E01.mkv"] = b"corrupted"
        second = execute_new_work_units(runner, state_root, "root-cont")
        self.assertEqual(second[0].outcome, "failed")

    def test_receipt_backs_continuation_when_source_was_mutated(self) -> None:
        """A persisted plan receipt beats source-shape inference.

        The failed attempt's receipt names exactly what the interrupted
        writer was executing.  Even when the provider source now holds
        different objects (so absent-fresh inference finds nothing), the
        continuation still plans the receipt's objects: present ones move
        normally, already-moved ones read back.
        """
        files = {"/incoming/one/S01E01.mkv": FAKE_VIDEO_BYTES}
        runner, state_root, alist, plan_calls, records = self._setup_root(files)
        reconcile_root_work_units(alist, "/library", state_root, "root-cont")
        first = execute_new_work_units(runner, state_root, "root-cont")
        self.assertEqual(first[0].outcome, "failed")
        self.assertIsNotNone(first[0].planned_receipt)
        # A brand-new sibling appears in the source after the failure.  The
        # receipt still drives the continuation for its own object.
        alist.files["/incoming/one/NEW.mkv"] = FAKE_VIDEO_BYTES
        second = execute_new_work_units(runner, state_root, "root-cont")
        self.assertEqual(second[0].outcome, "accepted")
        # The receipt's object reached the library; the unrelated sibling
        # stayed in the source.
        self.assertIn("/library/番剧/Work (101)/S01E01.mkv", alist.files)
        self.assertIn("/incoming/one/NEW.mkv", alist.files)

    def test_preplan_failure_keeps_previous_receipt_for_continuation(self) -> None:
        """A receipt-less attempt must not erase the interrupted write's receipt.

        An operator rollback of a wrong partial write restores the source
        objects under fresh provider mtimes, so the next retry can only
        resume through the previous failed attempt's planned receipt.  When
        that retry itself fails before planning anything (here an injected
        planner conflict, like the source occupant check), persisting the
        new failure without the old receipt would destroy the continuation
        evidence and leave the root with no sanctioned resume path.
        """
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        state_root = Path(temp.name)
        alist = IndexAList({"/incoming/one/S01E01.mkv": FAKE_VIDEO_BYTES})
        alist.modified["/incoming/one/S01E01.mkv"] = "T1"
        plan_calls: list[dict] = []
        planner_calls = {"count": 0}
        base_planner = _recording_planner(plan_calls)
        fail_once = {"remaining": 1}

        def flaky_planner(request, _alist, _tmdb):
            planner_calls["count"] += 1
            if planner_calls["count"] == 2:
                raise PlanError("injected occupant conflict")
            return base_planner(request, _alist, _tmdb)

        def executor(plan):
            for item in plan.files:
                source = f"{item.source_dir.rstrip('/')}/{item.original_name}"
                target = f"{item.target_dir.rstrip('/')}/{item.final_name}"
                if source in alist.files:
                    alist.move(item.source_dir, item.target_dir, [item.original_name])
                else:
                    payload = alist.files.get(target)
                    if payload is None or (
                        item.source_size is not None
                        and len(payload) != item.source_size
                    ):
                        raise RuntimeError(f"continuation target missing: {target}")
            if fail_once["remaining"] > 0:
                fail_once["remaining"] -= 1
                raise RuntimeError("injected artifact failure after moves")
            return {"ok": True, "files": [], "file_count": 0, "artifacts": [], "artifact_count": 0, "cleanup": [], "cleanup_count": 0}

        runner = SimpleEngineRunner(
            state_root, alist=alist, tmdb=object(),
            planner=flaky_planner, validate=False,
            library_root="/library", executor=executor,
        )
        pending = runner.create_pending_job("/incoming/one", job_id="root-preplan")
        runner.start_automatic_job(pending.id, target_shelf="anime")
        analyze_root_boundaries(
            alist, "/incoming/one", root_task_id="root-preplan",
            state_root=state_root,
        )
        records = load_work_unit_records(state_root, "root-preplan")
        apply_work_unit_override(
            state_root, "root-preplan", records[0].work_unit_id,
            media_type="tv", tmdb_id=101,
        )
        reconcile_root_work_units(alist, "/library", state_root, "root-preplan")

        # First attempt: the writer moves the media and fails during
        # artifacts, leaving a receipt-bearing failed acceptance record.
        first = execute_new_work_units(runner, state_root, "root-preplan")
        self.assertEqual(first[0].outcome, "failed")
        self.assertIsNotNone(first[0].planned_receipt)

        # Operator rollback: the wrongly placed media returns to the source
        # under a fresh provider mtime (the rename/move touched it).
        payload = alist.files.pop("/library/番剧/Work (101)/S01E01.mkv")
        alist.files["/incoming/one/S01E01.mkv"] = payload
        alist.modified["/incoming/one/S01E01.mkv"] = "T2"

        # Second attempt: the manifest drift is bridged by the receipt, but
        # planning itself fails.  The persisted failure must keep the receipt.
        second = execute_new_work_units(runner, state_root, "root-preplan")
        self.assertEqual(second[0].outcome, "failed")
        self.assertIn("injected occupant conflict", str(second[0].error))
        self.assertIsNotNone(second[0].planned_receipt)
        self.assertEqual(len(second[0].planned_receipt or ()), 1)

        # Third attempt: the carried receipt still bridges the drift; the
        # restored source object moves normally and the unit is accepted.
        third = execute_new_work_units(runner, state_root, "root-preplan")
        self.assertEqual(third[0].outcome, "accepted")
        self.assertIn("/library/番剧/Work (101)/S01E01.mkv", alist.files)
        self.assertNotIn("/incoming/one/S01E01.mkv", alist.files)

    def test_continuation_keeps_the_stored_proof_season(self) -> None:
        """A consumed-source continuation replans the D-proven season.

        The interrupted write moved a season-2 bracketed run.  The fresh D
        revalidator cannot recompute that verdict from the consumed source,
        so the continuation used to fall back to EngineRequest's historical
        implicit Season 01 default and replan a different season than the
        one the interrupted write had already partly written — every
        already-moved target then read back as a missing conflict.
        """
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        state_root = Path(temp.name)
        tmdb = StrictBareEpisodeTMDB(101, {2: 2})
        episodes = [
            (
                f"[Ygm] Show [{episode:02d}][Ma10p_2160p].mkv",
                episode,
            )
            for episode in range(1, 3)
        ]
        alist = IndexAList(
            {
                f"/incoming/one/{name}": FAKE_VIDEO_BYTES
                for name, _ in episodes
            }
        )
        plan_calls: list[dict] = []
        fail_once = {"remaining": 1}

        def planner(request, _alist, _tmdb) -> Plan:
            plan_calls.append({
                "source_path": request.source_path,
                "season": request.season,
            })
            target = f"{request.parent_path.rstrip('/')}/Work ({request.tmdb_id})"
            return Plan(
                mode="tv",
                source_root=request.source_path,
                target_root=target,
                files=[
                    PlannedFile(
                        source_path=f"{request.source_path}/{name}",
                        source_dir=request.source_path,
                        original_name=name,
                        final_name=f"S{request.season:02d}E{episode:02d}.mkv",
                        target_dir=target,
                        media_kind="video",
                        source_size=FAKE_VIDEO_SIZE,
                    )
                    for name, episode in episodes
                ],
                warnings=[],
                metadata={
                    "tmdb_id": request.tmdb_id,
                    "title": "Work",
                    "year": "2020",
                    "poster_path": None,
                    "backdrop_path": None,
                },
            )

        def executor(plan):
            for item in plan.files:
                source = f"{item.source_dir.rstrip('/')}/{item.original_name}"
                target = f"{item.target_dir.rstrip('/')}/{item.final_name}"
                if source in alist.files:
                    # The write shape under test moves AND renames, like the
                    # real executor's move-then-rename ladder.
                    alist.move(item.source_dir, item.target_dir, [item.original_name])
                    landed = f"{item.target_dir.rstrip('/')}/{item.original_name}"
                    if landed != target:
                        payload = alist.files.pop(landed)
                        alist.files[target] = payload
                else:
                    payload = alist.files.get(target)
                    if payload is None:
                        raise RuntimeError(f"continuation target missing: {target}")
                    if item.source_size is not None and len(payload) != item.source_size:
                        raise RuntimeError(f"continuation target size drift: {target}")
            if fail_once["remaining"] > 0:
                fail_once["remaining"] -= 1
                raise RuntimeError("injected artifact failure after moves")
            return {"ok": True, "files": [], "file_count": 0, "artifacts": [], "artifact_count": 0, "cleanup": [], "cleanup_count": 0}

        runner = SimpleEngineRunner(
            state_root, alist=alist, tmdb=tmdb, planner=planner,
            validate=False, library_root="/library", executor=executor,
        )
        pending = runner.create_pending_job("/incoming/one", job_id="root-season-cont")
        runner.start_automatic_job(pending.id, target_shelf="anime")
        analyze_root_boundaries(
            alist, "/incoming/one", root_task_id="root-season-cont",
            state_root=state_root,
        )
        records = load_work_unit_records(state_root, "root-season-cont")
        apply_work_unit_override(
            state_root, "root-season-cont", records[0].work_unit_id,
            media_type="tv", tmdb_id=101,
        )
        record = reconcile_root_work_units(
            alist, "/library", state_root, "root-season-cont",
            episode_catalog=TmdbEpisodeCatalog(tmdb), tmdb_client=tmdb,
        )[0]
        self.assertEqual(record.reconciliation_outcome, "new_work")
        self.assertEqual(
            (record.reconciliation_evidence or {}).get("kind"),
            "tmdb_single_positive_season_bracketed_episodes",
        )
        self.assertEqual((record.reconciliation_evidence or {}).get("season"), 2)

        # First attempt: the writer moves the season-2 media and fails
        # during artifacts, leaving a receipt-bearing failed record.
        first = execute_new_work_units(runner, state_root, "root-season-cont")
        self.assertEqual(first[0].outcome, "failed")
        self.assertEqual(plan_calls[0]["season"], 2)
        self.assertIn("/library/番剧/Work (101)/S02E01.mkv", alist.files)
        self.assertIn("/library/番剧/Work (101)/S02E02.mkv", alist.files)

        # The continuation must replan the same season: the stored D proof,
        # not the implicit Season 01 default, carries the request's season.
        second = execute_new_work_units(runner, state_root, "root-season-cont")
        self.assertEqual(second[0].outcome, "accepted")
        self.assertEqual(plan_calls[1]["season"], 2)
        self.assertIn("/library/番剧/Work (101)/S02E01.mkv", alist.files)
        self.assertIn("/library/番剧/Work (101)/S02E02.mkv", alist.files)


class SupersededExecutedCarrierTests(unittest.TestCase):
    """F must not reuse an executed carrier built under a superseded identity."""

    def test_superseded_executed_carrier_replans_under_corrected_identity(self) -> None:
        """A receipt rollback invalidates the old executed write.

        The carrier executed under the wrong TMDB identity; a receipt
        rollback restored the source file while the record kept pointing at
        that executed carrier.  After the record is re-confirmed to the
        correct identity, F must retire the superseded carrier and re-plan
        from the restored source instead of accepting the stale write fact
        (which would let R's intake cleanup delete the restored file).
        """
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        state_root = Path(temp.name)
        alist = FakeAList()
        alist.files["/incoming/one/S01E01.mkv"] = FAKE_VIDEO_BYTES
        plan_calls: list[dict] = []
        right_target = "/library/番剧/Work (101)"

        runner = SimpleEngineRunner(
            state_root, alist=alist, tmdb=object(),
            planner=_recording_planner(plan_calls), validate=False,
            library_root="/library",
        )
        pending = runner.create_pending_job("/incoming/one", job_id="root-superseded")
        runner.start_automatic_job(pending.id, target_shelf="anime")
        analyze_root_boundaries(
            alist, "/incoming/one", root_task_id="root-superseded",
            state_root=state_root,
        )
        record = load_work_unit_records(state_root, "root-superseded")[0]
        apply_work_unit_override(
            state_root, "root-superseded", record.work_unit_id,
            media_type="tv", tmdb_id=101,
        )
        reconcile_root_work_units(alist, "/library", state_root, "root-superseded")
        record = load_work_unit_records(state_root, "root-superseded")[0]

        # The historical wrong write: an executed internal carrier whose
        # durable plan carries the superseded identity (tmdb 201).
        request = _request_for_unit(runner, record, "root-superseded", state_root)
        carrier = runner.plan_job(
            request,
            job_id=f"unit-{record.work_unit_id}",
            internal_child_of="root-superseded",
        )
        wrong_plan = dict(carrier.plan)
        wrong_plan["metadata"] = {
            "tmdb_id": 201, "title": "Wrong Work", "year": "2020",
            "poster_path": None, "backdrop_path": None,
        }
        executed = replace(
            carrier, phase="executed",
            plan=wrong_plan, execution={"file_count": 1},
        )
        atomic_write_json(
            runner._job_path(carrier.id),  # noqa: SLF001 - durable carrier fixture
            executed.as_dict(),
            allow_nan=False,
        )

        # Receipt rollback state: the wrongly placed media is back at the
        # source, while the record still references the executed wrong
        # carrier.  The B snapshot matches the restored source exactly.
        save_work_unit_records(
            state_root, "root-superseded",
            [replace(record, writer_job_id=carrier.id)],
        )
        plans_before = len(plan_calls)

        results = execute_new_work_units(runner, state_root, "root-superseded")

        self.assertEqual([result.outcome for result in results], ["accepted"])
        self.assertEqual(len(plan_calls), plans_before + 1)
        self.assertEqual(plan_calls[-1]["tmdb_id"], 101)
        self.assertIn(f"{right_target}/S01E01.mkv", alist.files)
        self.assertNotIn("/incoming/one/S01E01.mkv", alist.files)
        records = load_work_unit_records(state_root, "root-superseded")
        self.assertEqual(
            records[0].writer_job_id, f"unit-{records[0].work_unit_id}",
        )
        replanned = runner.get_job(records[0].writer_job_id)
        self.assertEqual(replanned.phase, "executed")
        self.assertEqual(
            (replanned.plan.get("metadata") or {}).get("tmdb_id"), 101,
        )

    def test_executed_carrier_of_same_identity_is_still_reused(self) -> None:
        """An executed carrier matching the confirmed identity stays proof."""
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        state_root = Path(temp.name)
        alist = FakeAList()
        alist.files["/incoming/one/S01E01.mkv"] = FAKE_VIDEO_BYTES
        plan_calls: list[dict] = []
        runner = SimpleEngineRunner(
            state_root, alist=alist, tmdb=object(),
            planner=_recording_planner(plan_calls), validate=False,
            library_root="/library",
        )
        pending = runner.create_pending_job("/incoming/one", job_id="root-reuse")
        runner.start_automatic_job(pending.id, target_shelf="anime")
        analyze_root_boundaries(
            alist, "/incoming/one", root_task_id="root-reuse", state_root=state_root,
        )
        record = load_work_unit_records(state_root, "root-reuse")[0]
        apply_work_unit_override(
            state_root, "root-reuse", record.work_unit_id,
            media_type="tv", tmdb_id=101,
        )
        reconcile_root_work_units(alist, "/library", state_root, "root-reuse")
        record = load_work_unit_records(state_root, "root-reuse")[0]
        request = _request_for_unit(runner, record, "root-reuse", state_root)
        carrier = runner.plan_job(
            request,
            job_id=f"unit-{record.work_unit_id}",
            internal_child_of="root-reuse",
        )
        executed = replace(carrier, phase="executed", execution={"file_count": 1})
        atomic_write_json(
            runner._job_path(carrier.id),  # noqa: SLF001 - durable carrier fixture
            executed.as_dict(),
            allow_nan=False,
        )
        save_work_unit_records(
            state_root, "root-reuse", [replace(record, writer_job_id=carrier.id)],
        )
        plans_before = len(plan_calls)

        results = execute_new_work_units(runner, state_root, "root-reuse")

        self.assertEqual([result.outcome for result in results], ["accepted"])
        self.assertEqual(len(plan_calls), plans_before)
        records = load_work_unit_records(state_root, "root-reuse")
        self.assertEqual(records[0].writer_job_id, carrier.id)


class ArcEpisodeMapTests(unittest.TestCase):
    """A cumulative arc proof must produce a multi-season F episode map."""

    def test_bracket_map_accepts_the_proved_season_boundaries(self) -> None:
        from engine.scrapeflow.serialization import atomic_write_json
        from engine.scrapeflow.work_units import WorkUnitRecord
        from local.scrapeflow_api.library_index import (
            SingleSeasonEpisodeProof,
            _BRACKETED_EPISODE_EVIDENCE_KIND,
        )
        from local.scrapeflow_api.unit_execution import _bracketed_episode_map_path

        with tempfile.TemporaryDirectory() as directory:
            state_root = Path(directory)
            root_task_id = "root-arc"
            source = "/incoming/爱丽丝篇"
            rows = [{"name": "爱丽丝篇", "is_dir": True, "full_path": source}]
            for episode in range(1, 25):
                name = f"[TUDO] Alicization [{episode:02d}][Ma10p_2160p].mkv"
                rows.append({"name": name, "is_dir": False, "size": 2_000_000_000,
                             "full_path": f"{source}/{name}"})
            for episode in range(25, 48):
                name = f"[TUDO] Alicization War of Underworld [{episode:02d}][Ma10p_2160p].mkv"
                rows.append({"name": name, "is_dir": False, "size": 2_000_000_000,
                             "full_path": f"{source}/{name}"})
            atomic_write_json(
                state_root / f"work_snapshot_{root_task_id}.json",
                {"root": source, "rows": rows},
                allow_nan=False,
            )
            record = WorkUnitRecord(
                work_unit_id="unit-arc", root_task_id=root_task_id,
                boundary_key=source, source_paths=(source,), source_revision=1,
                role="single_work", display_label="爱丽丝篇", media_context="tv",
                identity_status="confirmed",
                identity={"media_type": "tv", "tmdb_id": 45782},
            )
            tokens = tuple(f"S03E{n:02d}" for n in range(1, 25)) + tuple(
                f"S04E{n:02d}" for n in range(1, 24)
            )
            proof = SingleSeasonEpisodeProof(
                tmdb_id=45782, season=3, episode_count=47,
                episode_tokens=tokens,
                evidence_kind=_BRACKETED_EPISODE_EVIDENCE_KIND,
                season_boundaries=((3, 24), (4, 23)),
            )
            path = _bracketed_episode_map_path(
                state_root, root_task_id, record, proof,
            )
            self.assertIsNotNone(path)
            assert path is not None
            mapping = json.loads(Path(path).read_text(encoding="utf-8"))
            self.assertEqual(len(mapping), 47)
            self.assertEqual(mapping["1"], "S03E01")
            self.assertEqual(mapping["24"], "S03E24")
            self.assertEqual(mapping["25"], "S04E01")
            self.assertEqual(mapping["47"], "S04E23")

        # 未被证明的季号仍然 fail-closed
        with tempfile.TemporaryDirectory() as directory:
            state_root = Path(directory)
            atomic_write_json(
                state_root / "work_snapshot_root-bad.json",
                {"root": "/incoming/x", "rows": [
                    {"name": "x", "is_dir": True, "full_path": "/incoming/x"},
                    {"name": "[T] Show [01].mkv", "is_dir": False, "size": 2_000_000_000,
                     "full_path": "/incoming/x/[T] Show [01].mkv"},
                    {"name": "[T] Show [02].mkv", "is_dir": False, "size": 2_000_000_000,
                     "full_path": "/incoming/x/[T] Show [02].mkv"},
                ]},
                allow_nan=False,
            )
            record = WorkUnitRecord(
                work_unit_id="unit-bad", root_task_id="root-bad",
                boundary_key="/incoming/x", source_paths=("/incoming/x",),
                source_revision=1, role="single_work", display_label="x",
                media_context="tv", identity_status="confirmed",
                identity={"media_type": "tv", "tmdb_id": 1},
            )
            proof = SingleSeasonEpisodeProof(
                tmdb_id=1, season=1, episode_count=2,
                episode_tokens=("S01E01", "S07E01"),
                evidence_kind=_BRACKETED_EPISODE_EVIDENCE_KIND,
            )
            self.assertIsNone(
                _bracketed_episode_map_path(state_root, "root-bad", record, proof)
            )


class BroadcastSeasonCoalescingTests(unittest.TestCase):
    """An alternate cut must not block a broadcast-season group from merging."""

    SOURCE = "/incoming/Re Zero"

    def _snapshot(self, labels):
        rows = [{"name": "Re Zero", "is_dir": True, "full_path": self.SOURCE}]
        for label in labels:
            path = f"{self.SOURCE}/{label}"
            rows.append({"name": label, "is_dir": True, "full_path": path})
            for episode in (1, 2):
                name = f"[X] Re Zero [{episode:02d}].mkv"
                rows.append({"name": name, "is_dir": False, "size": 2_000_000_000,
                             "full_path": f"{path}/{name}"})
        return {"root": self.SOURCE, "rows": rows}

    def _records(self, labels):
        from engine.scrapeflow.work_units import WorkUnitRecord
        out = []
        for index, label in enumerate(labels):
            path = f"{self.SOURCE}/{label}"
            out.append(WorkUnitRecord(
                work_unit_id=f"unit-{index}", root_task_id="root", boundary_key=path,
                source_paths=(path,), source_revision=1, role="season",
                display_label=label, media_context="tv", identity_status="confirmed",
                identity={"media_type": "tv", "tmdb_id": 65942},
            ))
        return out

    def test_alternate_cut_joins_its_base_season_group(self) -> None:
        from engine.scrapeflow.work_unit_coalescing import (
            coalesce_confirmed_tv_season_work_units,
        )
        labels = ["第一季", "第一季 新编集版", "第二季", "第三季", "第四季"]
        merged = coalesce_confirmed_tv_season_work_units(
            self._records(labels), self._snapshot(labels),
        )
        self.assertEqual(len(merged), 1, msg=[r.display_label for r in merged])
        unit = merged[0]
        self.assertEqual(len(unit.source_paths), 5)
        # 备用剪辑与它的基准季共享季号，合并后的季声明必须去重
        self.assertEqual(unit.claimed_seasons, (1, 2, 3, 4))
        self.assertNotIn("season", unit.identity or {})

    def test_alternate_cut_alone_keeps_per_unit_boundaries(self) -> None:
        from engine.scrapeflow.work_unit_coalescing import (
            coalesce_confirmed_tv_season_work_units,
        )
        labels = ["第一季", "第一季 新编集版"]
        records = self._records(labels)
        merged = coalesce_confirmed_tv_season_work_units(records, self._snapshot(labels))
        self.assertEqual(len(merged), 2)

    def test_alternate_cut_of_an_unowned_season_fails_closed(self) -> None:
        from engine.scrapeflow.work_unit_coalescing import (
            coalesce_confirmed_tv_season_work_units,
        )
        labels = ["第一季", "第二季", "第五季 新编集版"]
        records = self._records(labels)
        merged = coalesce_confirmed_tv_season_work_units(records, self._snapshot(labels))
        self.assertEqual(len(merged), 3, msg=[r.display_label for r in merged])

