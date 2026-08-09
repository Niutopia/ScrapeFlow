"""Focused safety coverage for the user-selected target-shelf start gate."""

from __future__ import annotations

import tempfile
import unittest
from dataclasses import replace
from pathlib import Path

from engine.scrapeflow.current_plan import plan_to_dict
from engine.scrapeflow.identity_matching import _query_from_source
from engine.scrapeflow.models import Plan, PlannedFile
from engine.scrapeflow.target_shelf import (
    parse_target_shelf,
    target_root_for_shelf,
    target_shelf_allows_media_type,
)
from local.scrapeflow_api.simple_engine_runner import (
    AutomaticIdentity,
    EngineJobConflictError,
    EngineRequest,
    EngineRequestError,
    SimpleEngineRunner,
    TargetShelfPolicyConflictError,
    atomic_write_json,
)


class DirectoryAList:
    """Only the narrow parent-listing surface used by the start gate."""

    def __init__(self) -> None:
        self.entries: dict[str, list[dict[str, object]]] = {
            "/library/待刮削": [{"name": "Source", "is_dir": True}],
        }
        self.login_calls = 0

    def login(self) -> None:
        self.login_calls += 1

    def list(self, path: str, refresh: bool = False) -> list[dict[str, object]]:
        del refresh
        return [dict(row) for row in self.entries.get(path, [])]


class RecordingArchivePreprocessor:
    def __init__(self, events: list[str]) -> None:
        self.events = events

    def prepare_ordinary_request(self, request, **_kwargs):
        self.events.append("archive")
        return request


class ReusableArchivePreprocessor:
    """Return a task-owned projection so reselection can reuse it."""

    def __init__(self) -> None:
        self.calls = 0
        self.sources: list[str] = []

    def prepare_ordinary_request(self, request, **kwargs):
        self.calls += 1
        local_staging = str(kwargs["task_staging"])
        remote_staging = str(kwargs["remote_staging_root"])
        source = f"{remote_staging}/archive/extracted"
        self.sources.append(source)
        return {
            **request,
            "source_path": source,
            "archive_preprocessed": {
                "changed": True,
                "ingress": "archive",
                "source_path": source,
                "task_staging": local_staging,
            },
        }


class TargetShelfStartGateTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.events: list[str] = []
        self.alist = DirectoryAList()
        self.runner = SimpleEngineRunner(
            Path(self.temporary.name),
            alist=self.alist,
            tmdb=object(),
            planner=lambda *_args: self.events.append("planner"),
            validate=False,
            library_root="/library",
            archive_preprocessor=RecordingArchivePreprocessor(self.events),
        )

    @staticmethod
    def _escaped_plan(source_path: str) -> Plan:
        """A malicious/injected plan that must never cross an anime shelf."""
        target_root = "/library/电影/Escaped"
        return Plan(
            mode="tv",
            source_root=source_path,
            target_root=target_root,
            files=[PlannedFile(
                source_path=f"{source_path}/episode.mkv",
                source_dir=source_path,
                original_name="episode.mkv",
                final_name="episode.mkv",
                target_dir=target_root,
                media_kind="video",
                source_size=1,
            )],
            warnings=[],
            metadata={"tmdb_id": 1, "title": "Escaped", "year": "2020"},
        )

    def test_shelf_mapping_and_type_matrix_are_closed(self) -> None:
        self.assertEqual(target_root_for_shelf("/library", "movie"), "/library/电影")
        self.assertEqual(target_root_for_shelf("/library", "anime"), "/library/番剧")
        self.assertEqual(target_root_for_shelf("/library", "us_tv"), "/library/美剧")
        self.assertTrue(target_shelf_allows_media_type("movie", "movie"))
        self.assertFalse(target_shelf_allows_media_type("anime", "movie"))
        self.assertTrue(target_shelf_allows_media_type("anime", "tv"))
        self.assertTrue(target_shelf_allows_media_type("us_tv", "tv"))
        self.assertFalse(target_shelf_allows_media_type("movie", "unknown"))
        with self.assertRaises(ValueError):
            parse_target_shelf("/library/电影")

    def test_archive_suffix_is_not_part_of_the_identity_query(self) -> None:
        for suffix in ("zip", "7z", "rar"):
            with self.subTest(suffix=suffix):
                query = _query_from_source(
                    f"/library/ScrapeFlow/归档/job/archive/Movie.2020.{suffix}"
                )
                self.assertNotIn(suffix, query.casefold())
                self.assertIn("Movie", query)

    def test_pending_registration_has_zero_formal_calls_then_start_is_idempotent(self) -> None:
        pending = self.runner.create_pending_job("/library/待刮削/Source")

        self.assertEqual(pending.phase, "awaiting_target_shelf")
        self.assertEqual(pending.plan, {})
        self.assertEqual(pending.summary["ingress_source_path"], "/library/待刮削/Source")
        self.assertIsNone(pending.target_shelf)
        self.assertEqual(self.events, [])
        self.assertEqual(self.alist.login_calls, 0)

        started = self.runner.start_automatic_job(pending.id, target_shelf="anime")
        repeated = self.runner.start_automatic_job(pending.id, target_shelf="anime")

        self.assertEqual(started.phase, "queued")
        self.assertEqual(started.target_shelf, "anime")
        self.assertEqual(started.target_root, "/library/番剧")
        self.assertEqual(repeated.selected_at, started.selected_at)
        self.assertEqual(self.events, [])
        with self.assertRaises(EngineJobConflictError):
            self.runner.start_automatic_job(pending.id, target_shelf="movie")

    def test_deduplication_normalizes_a_legacy_trailing_source_slash(self) -> None:
        pending = self.runner.create_pending_job("/library/待刮削/Source")
        legacy = replace(
            pending,
            summary={
                **pending.summary,
                "ingress_source_path": "/library/待刮削/Source/",
            },
        )
        atomic_write_json(
            self.runner.jobs_root / f"{pending.id}.json",
            legacy.as_dict(),
            allow_nan=False,
        )

        self.assertEqual(
            self.runner.find_by_source("/library/待刮削/Source").id,
            pending.id,
        )

    def test_missing_source_or_invalid_shelf_leaves_waiting_record_unchanged(self) -> None:
        pending = self.runner.create_pending_job("/library/待刮削/Source")
        with self.assertRaises(EngineRequestError):
            self.runner.start_automatic_job(pending.id, target_shelf="/library/电影")
        self.alist.entries["/library/待刮削"] = []
        with self.assertRaises(EngineJobConflictError):
            self.runner.start_automatic_job(pending.id, target_shelf="movie")
        persisted = self.runner.get_job(pending.id)
        self.assertEqual(persisted.phase, "awaiting_target_shelf")
        self.assertIsNone(persisted.target_shelf)
        self.assertEqual(self.events, [])

    def test_recovery_does_not_advance_waiting_gate(self) -> None:
        pending = self.runner.create_pending_job("/library/待刮削/Source")

        recovered = self.runner.recover_job(pending.id)

        self.assertEqual(recovered.phase, "awaiting_target_shelf")
        self.assertEqual(self.runner.get_job(pending.id).phase, "awaiting_target_shelf")
        self.assertEqual(self.events, [])

    def test_legacy_automatic_root_cannot_execute_without_a_selection(self) -> None:
        pending = self.runner.create_pending_job("/library/待刮削/Source")
        legacy_planned = replace(
            pending,
            phase="planned",
            plan=plan_to_dict(self._escaped_plan("/library/待刮削/Source")),
        )
        atomic_write_json(
            self.runner.jobs_root / f"{pending.id}.json",
            legacy_planned.as_dict(),
            allow_nan=False,
        )

        with self.assertRaisesRegex(EngineRequestError, "尚未选择目标货架"):
            self.runner.execute_job(pending.id)
        self.assertEqual(self.events, [])

    def test_type_conflict_stops_before_planner_and_can_be_explicitly_reselected(self) -> None:
        pending = self.runner.create_pending_job("/library/待刮削/Source")
        started = self.runner.start_automatic_job(pending.id, target_shelf="anime")
        identity = AutomaticIdentity(
            media_type="movie",
            tmdb_id=1,
            title="Movie",
            year="2020",
            confidence=0.99,
            target_parent="/library/番剧",
            season=None,
            trace={},
            target_shelf="anime",
            target_shelf_root="/library/番剧",
        )

        def conflict(*_args, **_kwargs):
            raise TargetShelfPolicyConflictError(
                target_shelf="anime", media_type="movie", identity=identity,
            )

        original = self.runner.resolve_automatic_request
        self.runner.resolve_automatic_request = conflict  # type: ignore[method-assign]
        try:
            conflicted = self.runner.plan_automatic_job(started.id)
        finally:
            self.runner.resolve_automatic_request = original  # type: ignore[method-assign]

        self.assertEqual(conflicted.phase, "target_policy_conflict")
        self.assertEqual(conflicted.plan, {})
        self.assertEqual(conflicted.target_shelf, "anime")
        self.assertEqual(conflicted.summary["identity"]["media_type"], "movie")
        self.assertEqual(self.events, ["archive"])
        reselected = self.runner.start_automatic_job(conflicted.id, target_shelf="movie")
        self.assertEqual(reselected.phase, "queued")
        self.assertEqual(reselected.target_root, "/library/电影")

    def test_conflict_reselection_reuses_verified_archive_projection(self) -> None:
        preprocessor = ReusableArchivePreprocessor()
        self.runner.archive_preprocessor = preprocessor
        pending = self.runner.create_pending_job("/library/待刮削/Source")
        started = self.runner.start_automatic_job(pending.id, target_shelf="anime")
        conflict_identity = AutomaticIdentity(
            media_type="movie",
            tmdb_id=1,
            title="Movie",
            year="2020",
            confidence=0.99,
            target_parent="/library/番剧",
            season=None,
            trace={},
            target_shelf="anime",
            target_shelf_root="/library/番剧",
        )

        def resolve(source_path, *, target_shelf):
            shelf = target_shelf.value if hasattr(target_shelf, "value") else str(target_shelf)
            if shelf == "anime":
                raise TargetShelfPolicyConflictError(
                    target_shelf="anime", media_type="movie", identity=conflict_identity,
                )
            request = EngineRequest.from_mapping({
                "source_path": source_path,
                "parent_path": "/library/电影",
                "media_type": "movie",
                "target_shelf": "movie",
                "tmdb_id": 1,
            })
            identity = AutomaticIdentity(
                media_type="movie",
                tmdb_id=1,
                title="Movie",
                year="2020",
                confidence=0.99,
                target_parent="/library/电影",
                season=None,
                trace={},
                target_shelf="movie",
                target_shelf_root="/library/电影",
            )
            return request, identity

        def valid_plan(request, *_args):
            return Plan(
                mode="movie",
                source_root=request.source_path,
                target_root="/library/电影/Movie (2020)",
                files=[PlannedFile(
                    source_path=f"{request.source_path}/movie.mkv",
                    source_dir=request.source_path,
                    original_name="movie.mkv",
                    final_name="Movie (2020).mkv",
                    target_dir="/library/电影/Movie (2020)",
                    media_kind="video",
                    source_size=1,
                )],
                warnings=[],
                metadata={"tmdb_id": 1, "title": "Movie", "year": "2020"},
            )

        original_resolver = self.runner.resolve_automatic_request
        original_planner = self.runner.planner
        self.runner.resolve_automatic_request = resolve  # type: ignore[method-assign]
        self.runner.planner = valid_plan
        try:
            conflicted = self.runner.plan_automatic_job(started.id)
            self.assertEqual(conflicted.phase, "target_policy_conflict")
            self.assertEqual(preprocessor.calls, 1)
            projection = conflicted.summary["archive_preprocessed"]
            self.assertTrue(projection["source_path"].startswith("/library/ScrapeFlow/归档/"))

            reselected = self.runner.start_automatic_job(conflicted.id, target_shelf="movie")
            planned = self.runner.plan_automatic_job(reselected.id)
        finally:
            self.runner.resolve_automatic_request = original_resolver  # type: ignore[method-assign]
            self.runner.planner = original_planner

        self.assertEqual(planned.phase, "planned")
        self.assertEqual(preprocessor.calls, 1)
        self.assertEqual(planned.request["source_path"], projection["source_path"])

    def test_foreign_archive_projection_fails_closed_without_reprocessing(self) -> None:
        preprocessor = ReusableArchivePreprocessor()
        self.runner.archive_preprocessor = preprocessor
        pending = self.runner.create_pending_job("/library/待刮削/Source")
        started = self.runner.start_automatic_job(pending.id, target_shelf="anime")
        forged = replace(
            started,
            summary={
                **started.summary,
                "archive_preprocessed": {
                    "changed": True,
                    "ingress": "archive",
                    "source_path": "/library/电影/foreign/archive/extracted",
                    "task_staging": str(self.runner._archive_task_roots(started.id)[0]),  # noqa: SLF001
                },
            },
        )
        atomic_write_json(
            self.runner.jobs_root / f"{started.id}.json",
            forged.as_dict(),
            allow_nan=False,
        )
        resolver_calls: list[str] = []
        original_resolver = self.runner.resolve_automatic_request

        def resolver(*args, **kwargs):
            resolver_calls.append(str(args[0] if args else kwargs))
            raise AssertionError("foreign staging must fail before identity")

        self.runner.resolve_automatic_request = resolver  # type: ignore[method-assign]
        try:
            with self.assertRaisesRegex(EngineRequestError, "staging"):
                self.runner.plan_automatic_job(started.id)
        finally:
            self.runner.resolve_automatic_request = original_resolver  # type: ignore[method-assign]

        self.assertEqual(preprocessor.calls, 0)
        self.assertEqual(resolver_calls, [])
        failed = self.runner.get_job(started.id)
        self.assertEqual(failed.phase, "failed_archive")
        self.assertTrue(failed.summary["automatic_terminal"])

    def test_identity_interruption_reuses_persisted_archive_projection(self) -> None:
        preprocessor = ReusableArchivePreprocessor()
        self.runner.archive_preprocessor = preprocessor
        pending = self.runner.create_pending_job("/library/待刮削/Source")
        started = self.runner.start_automatic_job(pending.id, target_shelf="movie")
        identity_calls = 0

        def resolve(source_path, *, target_shelf):
            nonlocal identity_calls
            identity_calls += 1
            if identity_calls == 1:
                raise RuntimeError("simulated identity interruption")
            request = EngineRequest.from_mapping({
                "source_path": source_path,
                "parent_path": "/library/电影",
                "media_type": "movie",
                "target_shelf": "movie",
                "tmdb_id": 1,
            })
            return request, AutomaticIdentity(
                media_type="movie",
                tmdb_id=1,
                title="Movie",
                year="2020",
                confidence=0.99,
                target_parent="/library/电影",
                season=None,
                trace={},
                target_shelf="movie",
                target_shelf_root="/library/电影",
            )

        def valid_plan(request, *_args):
            return Plan(
                mode="movie",
                source_root=request.source_path,
                target_root="/library/电影/Movie (2020)",
                files=[PlannedFile(
                    source_path=f"{request.source_path}/movie.mkv",
                    source_dir=request.source_path,
                    original_name="movie.mkv",
                    final_name="Movie (2020).mkv",
                    target_dir="/library/电影/Movie (2020)",
                    media_kind="video",
                    source_size=1,
                )],
                warnings=[],
                metadata={"tmdb_id": 1, "title": "Movie", "year": "2020"},
            )

        original_resolver = self.runner.resolve_automatic_request
        original_planner = self.runner.planner
        self.runner.resolve_automatic_request = resolve  # type: ignore[method-assign]
        self.runner.planner = valid_plan
        try:
            with self.assertRaisesRegex(RuntimeError, "identity interruption"):
                self.runner.plan_automatic_job(started.id)
            interrupted = self.runner.get_job(started.id)
            self.assertEqual(interrupted.phase, "identity_matching")
            self.assertIn("archive_preprocessed", interrupted.summary)
            planned = self.runner.plan_automatic_job(started.id)
        finally:
            self.runner.resolve_automatic_request = original_resolver  # type: ignore[method-assign]
            self.runner.planner = original_planner

        self.assertEqual(planned.phase, "planned")
        self.assertEqual(preprocessor.calls, 1)
        self.assertEqual(identity_calls, 2)

    def test_terminal_job_cannot_be_reopened_by_start(self) -> None:
        pending = self.runner.create_pending_job("/library/待刮削/Source")
        started = self.runner.start_automatic_job(pending.id, target_shelf="anime")
        terminal = replace(started, phase="cancelled", error="operator stop")
        atomic_write_json(
            self.runner.jobs_root / f"{started.id}.json",
            terminal.as_dict(),
            allow_nan=False,
        )

        with self.assertRaises(EngineJobConflictError):
            self.runner.start_automatic_job(started.id, target_shelf="anime")
        self.assertEqual(self.runner.get_job(started.id).phase, "cancelled")

    def test_planning_and_execution_reject_targets_outside_confirmed_shelf(self) -> None:
        pending = self.runner.create_pending_job("/library/待刮削/Source")
        started = self.runner.start_automatic_job(pending.id, target_shelf="anime")
        escaped = self._escaped_plan("/library/待刮削/Source")
        identity = AutomaticIdentity(
            media_type="tv",
            tmdb_id=1,
            title="Escaped",
            year="2020",
            confidence=0.99,
            target_parent="/library/番剧",
            season=1,
            trace={},
            target_shelf="anime",
            target_shelf_root="/library/番剧",
        )

        def resolved(*_args, **_kwargs):
            request = EngineRequest.from_mapping({
                "source_path": "/library/待刮削/Source",
                "parent_path": "/library/番剧",
                "media_type": "tv",
                "target_shelf": "anime",
                "tmdb_id": 1,
            })
            return request, identity

        original_planner = self.runner.planner
        original_resolver = self.runner.resolve_automatic_request
        self.runner.planner = lambda *_args: escaped
        self.runner.resolve_automatic_request = resolved  # type: ignore[method-assign]
        try:
            with self.assertRaisesRegex(EngineRequestError, "目标货架外"):
                self.runner.plan_automatic_job(started.id)
        finally:
            self.runner.planner = original_planner
            self.runner.resolve_automatic_request = original_resolver  # type: ignore[method-assign]

        rejected_plan = self.runner.get_job(started.id)
        self.assertEqual(rejected_plan.plan, {})
        self.assertEqual(self.events, ["archive"])

        planned = replace(
            started,
            phase="planned",
            plan=plan_to_dict(escaped),
            summary={"automatic": True},
        )
        atomic_write_json(
            self.runner.jobs_root / f"{started.id}.json",
            planned.as_dict(),
            allow_nan=False,
        )
        self.runner.executor = lambda _plan: self.events.append("execute")
        with self.assertRaisesRegex(EngineRequestError, "目标货架外"):
            self.runner.execute_job(started.id)
        self.assertEqual(self.runner.get_job(started.id).phase, "planned")
        self.assertEqual(self.events, ["archive"])

    def test_metadata_derived_artifact_targets_are_inside_confirmed_shelf(self) -> None:
        escaped_artifact_plan = Plan(
            mode="batch",
            source_root="/library/待刮削/Source",
            target_root="/library/番剧/Allowed",
            files=[],
            warnings=[],
            metadata={
                "member_tv": {
                    "/library/电影/Escaped": {
                        "tmdb_id": 1,
                        "title": "Escaped",
                        "year": "2020",
                        "poster_path": "/tmdb/poster",
                    },
                },
            },
        )

        with self.assertRaisesRegex(EngineRequestError, "目标货架外"):
            self.runner._require_plan_target_shelf_containment(  # noqa: SLF001 - gate coverage
                escaped_artifact_plan,
                target_root="/library/番剧",
                stage="测试计划",
            )

        unsafe_intermediate_plan = Plan(
            mode="tv",
            source_root="/library/待刮削/Source",
            target_root="/library/番剧/Allowed",
            files=[PlannedFile(
                source_path="/library/电影/Escaped.mkv",
                source_dir="/library/电影",
                original_name="/library/电影/Escaped.mkv",
                final_name="episode.mkv",
                target_dir="/library/番剧/Allowed",
                media_kind="video",
                source_size=1,
            )],
            warnings=[],
            metadata={},
        )
        with self.assertRaisesRegex(EngineRequestError, "安全文件名"):
            self.runner._require_plan_target_shelf_containment(  # noqa: SLF001 - gate coverage
                unsafe_intermediate_plan,
                target_root="/library/番剧",
                stage="测试计划",
            )

    def test_metadata_artifact_escape_is_blocked_at_every_replay_boundary(self) -> None:
        pending = self.runner.create_pending_job("/library/待刮削/Source")
        started = self.runner.start_automatic_job(pending.id, target_shelf="anime")
        artifact_escape = Plan(
            mode="batch",
            source_root="/library/待刮削/Source",
            target_root="/library/番剧/Allowed",
            files=[],
            warnings=[],
            metadata={
                "member_tv": {
                    "/library/电影/Escaped": {
                        "tmdb_id": 1,
                        "title": "Escaped",
                        "year": "2020",
                        "poster_path": "/tmdb/poster",
                    },
                },
            },
        )
        persisted = replace(
            started,
            plan=plan_to_dict(artifact_escape),
            summary={**started.summary, "automatic": True},
        )

        for phase, invoke in (
            ("planned", self.runner.execute_job),
            ("executed", self.runner.repair_automatic_artifacts),
        ):
            with self.subTest(boundary=phase):
                atomic_write_json(
                    self.runner.jobs_root / f"{started.id}.json",
                    replace(persisted, phase=phase).as_dict(),
                    allow_nan=False,
                )
                with self.assertRaisesRegex(EngineRequestError, "目标货架外"):
                    invoke(started.id)

        atomic_write_json(
            self.runner.jobs_root / f"{started.id}.json",
            replace(persisted, phase="failed").as_dict(),
            allow_nan=False,
        )
        recovered = self.runner.recover_job(started.id)
        self.assertEqual(recovered.phase, "failed_verification")
        self.assertTrue(recovered.summary["automatic_terminal"])
        self.assertEqual(
            recovered.summary["recovery"]["reason"],
            "target_shelf_policy_violation",
        )

    def test_terminal_job_cannot_reenter_automatic_planning(self) -> None:
        pending = self.runner.create_pending_job("/library/待刮削/Source")
        started = self.runner.start_automatic_job(pending.id, target_shelf="anime")
        terminal = replace(started, phase="cancelled", error="operator stop")
        atomic_write_json(
            self.runner.jobs_root / f"{started.id}.json",
            terminal.as_dict(),
            allow_nan=False,
        )

        with self.assertRaises(EngineJobConflictError):
            self.runner.plan_automatic_job(started.id)
        self.assertEqual(self.events, [])
