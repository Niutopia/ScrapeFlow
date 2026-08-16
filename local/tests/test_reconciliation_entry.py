"""Focused coverage for the read-only ordinary-intake reconciliation entry."""

from __future__ import annotations

import posixpath
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from engine.scrapeflow.identity_matching import AutoMatchAmbiguityError
from engine.scrapeflow.models import Plan, PlannedFile
from local.simple_server import SimpleApplication
from local.scrapeflow_api.simple_engine_runner import (
    EngineJobConflictError,
    EnginePauseRequested,
    EngineRequestError,
    EngineWorkerBusyError,
    SimpleEngineRunner,
)


class ReadOnlyAList:
    """An in-memory AList surface that fails loudly on every mutation."""

    def __init__(self, tree: dict[str, list[dict[str, object]]], files: dict[str, bytes] | None = None) -> None:
        self.tree = {path: [dict(row) for row in rows] for path, rows in tree.items()}
        self.files = dict(files or {})
        self.mutations: list[str] = []

    def list(self, path: str, refresh: bool = False) -> list[dict[str, object]]:
        del refresh
        return [dict(row) for row in self.tree.get(path, [])]

    def read_file_bytes(self, path: str, *, max_bytes: int) -> bytes:
        return self.files[path][:max_bytes]

    def walk(self, path: str, **_kwargs: object) -> list[dict[str, object]]:
        prefix = path.rstrip("/") + "/"
        return [
            {
                "path": full_path,
                "full_path": full_path,
                "size": len(self.files[full_path]),
            }
            for full_path in sorted(self.files)
            if full_path.startswith(prefix)
        ]

    def _mutation(self, name: str, *_args: object, **_kwargs: object) -> None:
        self.mutations.append(name)
        raise AssertionError(f"reconciliation must not call {name}")

    mkdir = ensure_directory = move = rename = upload_bytes = remove = _mutation


class MutableDirectoryAList(ReadOnlyAList):
    """Small directory-only AList double for the duplicate consume boundary."""

    def __init__(
        self,
        tree: dict[str, list[dict[str, object]]],
        files: dict[str, bytes] | None = None,
    ) -> None:
        super().__init__(tree, files)
        self.move_calls: list[tuple[str, str, list[str]]] = []

    def mkdir(self, path: str) -> None:
        normalized = path.rstrip("/") or "/"
        if normalized == "/":
            self.tree.setdefault("/", [])
            return
        current = ""
        for component in normalized.strip("/").split("/"):
            current = f"{current}/{component}"
            parent = posixpath.dirname(current) or "/"
            self.tree.setdefault(parent, [])
            if not any(
                row.get("name") == component and row.get("is_dir") is True
                for row in self.tree[parent]
            ):
                self.tree[parent].append({"name": component, "is_dir": True})
            self.tree.setdefault(current, [])

    ensure_directory = mkdir

    def move(self, source_parent: str, target_parent: str, names: list[str]) -> None:
        self.move_calls.append((source_parent, target_parent, list(names)))
        self.mkdir(target_parent)
        for name in names:
            source = posixpath.join(source_parent, name)
            target = posixpath.join(target_parent, name)
            prefix = source.rstrip("/") + "/"
            moved_tree = {
                path: rows
                for path, rows in self.tree.items()
                if path == source or path.startswith(prefix)
            }
            if source not in moved_tree:
                raise AssertionError(f"missing source directory: {source}")
            self.tree[source_parent] = [
                row for row in self.tree.get(source_parent, [])
                if row.get("name") != name
            ]
            self.tree[target_parent].append({"name": name, "is_dir": True})
            for path in moved_tree:
                suffix = path[len(source):]
                self.tree[posixpath.normpath(target + suffix)] = moved_tree[path]
                self.tree.pop(path, None)
            moved_files = {
                path: value
                for path, value in self.files.items()
                if path == source or path.startswith(prefix)
            }
            for path in moved_files:
                suffix = path[len(source):]
                self.files[posixpath.normpath(target + suffix)] = moved_files[path]
                self.files.pop(path, None)


def _movie_nfo(tmdb_id: int, title: str = "Movie") -> bytes:
    return (
        f"<movie><tmdbid>{tmdb_id}</tmdbid><title>{title}</title><year>2020</year></movie>"
    ).encode("utf-8")


def _tv_nfo(tmdb_id: int, title: str = "Show") -> bytes:
    return (
        f"<tvshow><tmdbid>{tmdb_id}</tmdbid><title>{title}</title><year>2020</year></tvshow>"
    ).encode("utf-8")


# Keep reconciliation fixtures on the same default admission floor as the
# formal writer. Tiny byte strings are deliberately reserved for the negative
# reconciliation cases below.
_ADMISSIBLE_VIDEO = b"v" * (1024 * 1024)


class ReconciliationEntryTests(unittest.TestCase):
    library_root = "/library"
    intake_root = "/library/待刮削"
    movie_root = "/library/电影"
    anime_root = "/library/番剧"
    us_tv_root = "/library/美剧"

    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)

    def _runner(self, client: ReadOnlyAList) -> SimpleEngineRunner:
        return SimpleEngineRunner(
            Path(self.temporary.name),
            alist=client,
            tmdb=object(),
            validate=False,
            library_root=self.library_root,
        )

    def _tree(self, *, intake_name: str = "Incoming") -> dict[str, list[dict[str, object]]]:
        return {
            self.movie_root: [],
            self.anime_root: [],
            self.us_tv_root: [],
            self.intake_root: [{"name": intake_name, "is_dir": True}],
        }

    @staticmethod
    def _match(media_type: str, tmdb_id: int, title: str) -> SimpleNamespace:
        return SimpleNamespace(
            media_type=media_type,
            tmdb_id=tmdb_id,
            title=title,
            year="2020",
            confidence=0.99,
            decision_trace={"matcher": "existing_engine"},
        )

    def _reconcile(self, runner: SimpleEngineRunner, *, source: str, match: SimpleNamespace):
        pending = runner.create_pending_job(source)
        shelf = "movie" if match.media_type == "movie" else "anime"
        started = runner.start_automatic_job(pending.id, target_shelf=shelf)
        runner.mark_reconciling(started.id)
        with patch("engine.scraper.auto_match_tmdb", return_value=(match, [])) as matcher:
            result = runner.reconcile_automatic_job(pending.id)
        self.assertEqual(matcher.call_count, 1)
        return result

    def test_empty_formal_library_becomes_new_work_and_only_then_allows_start(self) -> None:
        client = ReadOnlyAList(self._tree())
        runner = self._runner(client)

        result = self._reconcile(
            runner,
            source=f"{self.intake_root}/Incoming",
            match=self._match("movie", 10, "New Movie"),
        )

        self.assertEqual(result.phase, "queued")
        self.assertEqual(result.summary["reconciliation"]["outcome"], "new_work")
        started = runner.start_automatic_job(result.id, target_shelf="movie")
        self.assertEqual(started.phase, "queued")
        self.assertEqual(client.mutations, [])

    def test_new_work_planning_reuses_reconciled_identity_after_shelf_start(self) -> None:
        source = f"{self.intake_root}/Incoming"
        target = f"{self.movie_root}/New Movie (2020)"
        client = ReadOnlyAList(
            self._tree(),
            {f"{source}/New.Movie.2020.mkv": b"incoming movie"},
        )
        requests: list[object] = []

        def planner(request, _alist, _tmdb):
            requests.append(request)
            return Plan(
                mode="movie",
                source_root=source,
                target_root=target,
                files=[],
                warnings=[],
                metadata={"tmdb_id": 24, "title": "New Movie", "year": "2020"},
            )

        runner = SimpleEngineRunner(
            Path(self.temporary.name),
            alist=client,
            tmdb=object(),
            planner=planner,
            validate=False,
            library_root=self.library_root,
        )
        reconciled = self._reconcile(
            runner,
            source=source,
            match=self._match("movie", 24, "New Movie"),
        )
        started = runner.start_automatic_job(reconciled.id, target_shelf="movie")
        with patch("engine.scraper.auto_match_tmdb", side_effect=AssertionError("matcher rerun")):
            planned = runner.plan_automatic_job(started.id)
        self.assertEqual(planned.phase, "planned")
        self.assertEqual(planned.summary["reconciliation"]["outcome"], "new_work")
        self.assertEqual(requests[0].tmdb_id, 24)
        self.assertEqual(requests[0].parent_path, self.movie_root)
        self.assertEqual(client.mutations, [])

    def test_complete_formal_nfo_match_is_duplicate_without_writer_or_provider(self) -> None:
        target = f"{self.movie_root}/Movie (2020)"
        tree = self._tree()
        tree[self.movie_root] = [{"name": "Movie (2020)", "is_dir": True}]
        tree[target] = [
            {"name": "Movie (2020).mkv", "is_dir": False, "size": len(_ADMISSIBLE_VIDEO)},
            {"name": "Movie (2020).nfo", "is_dir": False, "size": 10},
        ]
        client = ReadOnlyAList(
            tree,
            {
                f"{target}/Movie (2020).mkv": _ADMISSIBLE_VIDEO,
                f"{target}/Movie (2020).nfo": _movie_nfo(11),
            },
        )
        runner = self._runner(client)
        runner.executor = lambda _plan: (_ for _ in ()).throw(AssertionError("writer called"))

        result = self._reconcile(
            runner,
            source=f"{self.intake_root}/Incoming",
            match=self._match("movie", 11, "Movie"),
        )

        self.assertEqual(result.phase, "reconciled")
        reconciliation = result.summary["reconciliation"]
        self.assertEqual(reconciliation["outcome"], "duplicate_complete")
        self.assertEqual(reconciliation["matched_shelf"], "movie")
        self.assertEqual(reconciliation["matched_formal_work"]["target_root"], target)
        with self.assertRaises(EngineJobConflictError):
            runner.start_automatic_job(result.id, target_shelf="movie")
        self.assertEqual(client.mutations, [])

    def _duplicate_source_fixture(self) -> tuple[SimpleEngineRunner, MutableDirectoryAList, object]:
        """Return a complete duplicate reconciliation with an owned source dir."""
        source = f"{self.intake_root}/Incoming"
        target = f"{self.movie_root}/Movie (2020)"
        tree = self._tree()
        tree[self.movie_root] = [{"name": "Movie (2020)", "is_dir": True}]
        tree[target] = [
            {"name": "Movie (2020).mkv", "is_dir": False, "size": len(_ADMISSIBLE_VIDEO)},
            {"name": "Movie (2020).nfo", "is_dir": False, "size": 10},
        ]
        tree[source] = []
        client = MutableDirectoryAList(
            tree,
            {
                f"{target}/Movie (2020).mkv": _ADMISSIBLE_VIDEO,
                f"{target}/Movie (2020).nfo": _movie_nfo(11),
            },
        )
        runner = self._runner(client)
        reconciled = self._reconcile(
            runner,
            source=source,
            match=self._match("movie", 11, "Movie"),
        )
        self.assertEqual(reconciled.summary["reconciliation"]["outcome"], "duplicate_complete")
        return runner, client, reconciled

    def _existing_gap_source_fixture(
        self,
        *,
        source_rows: list[dict[str, object]] | None = None,
    ) -> tuple[SimpleEngineRunner, MutableDirectoryAList, object]:
        """Return an existing-gap reconciliation with a task-owned intake dir."""
        source = f"{self.intake_root}/Incoming"
        target = f"{self.movie_root}/Gap Movie (2020)"
        tree = self._tree()
        tree[self.movie_root] = [{"name": "Gap Movie (2020)", "is_dir": True}]
        tree[target] = [{"name": "Gap Movie (2020).nfo", "is_dir": False, "size": 10}]
        tree[source] = list(source_rows or [])
        client = MutableDirectoryAList(
            tree,
            {f"{target}/Gap Movie (2020).nfo": _movie_nfo(121, "Gap Movie")},
        )
        runner = self._runner(client)
        reconciled = self._reconcile(
            runner,
            source=source,
            match=self._match("movie", 121, "Gap Movie"),
        )
        self.assertEqual(reconciled.phase, "reconciled")
        self.assertEqual(reconciled.summary["reconciliation"]["outcome"], "existing_gap")
        return runner, client, reconciled

    def test_existing_gap_empty_source_registers_task_owned_hold_without_writer_and_is_idempotent(self) -> None:
        runner, client, reconciled = self._existing_gap_source_fixture()
        runner.executor = lambda _plan: (_ for _ in ()).throw(AssertionError("writer called"))

        registered = runner.hold_existing_gap_source(reconciled.id)
        marker = registered.summary["existing_gap_registration"]
        self.assertEqual(registered.phase, "completed")
        self.assertEqual(marker["status"], "moved_to_hold")
        self.assertEqual(
            marker["target"],
            f"{self.library_root}/ScrapeFlow/归档/{reconciled.id}/existing-gap-hold/Incoming",
        )
        self.assertFalse(runner._remote_directory_exists(marker["source"]))  # noqa: SLF001
        self.assertTrue(runner._remote_directory_exists(marker["target"]))  # noqa: SLF001
        self.assertTrue(runner.existing_gap_source_hold_verified(reconciled.id))
        self.assertEqual(len(client.move_calls), 1)

        repeated = runner.hold_existing_gap_source(reconciled.id)
        self.assertEqual(repeated.summary["existing_gap_registration"], marker)
        self.assertEqual(len(client.move_calls), 1)

    def test_existing_gap_nonempty_source_is_needs_attention_and_stays_in_intake(self) -> None:
        runner, client, reconciled = self._existing_gap_source_fixture(
            source_rows=[{"name": "sidecar.srt", "is_dir": False, "size": 12}],
        )

        blocked = runner.hold_existing_gap_source(reconciled.id)
        marker = blocked.summary["existing_gap_registration"]
        self.assertEqual(blocked.phase, "reconciliation_uncertain")
        self.assertEqual(marker["status"], "blocked_nonempty_source")
        self.assertEqual(blocked.summary["source_fate"], "retained_needs_attention")
        self.assertEqual(blocked.summary["reconciliation"]["outcome"], "existing_gap")
        self.assertEqual(blocked.summary["reconciliation"]["status"], "needs_attention")
        self.assertTrue(runner._remote_directory_exists(reconciled.request["source_path"]))  # noqa: SLF001
        self.assertEqual(client.move_calls, [])
        self.assertFalse(runner.existing_gap_source_hold_verified(reconciled.id))

    def test_existing_gap_blocked_source_can_retry_empty_hold_without_identity_or_writer(self) -> None:
        runner, client, reconciled = self._existing_gap_source_fixture(
            source_rows=[{"name": "irrelevant.srt", "is_dir": False, "size": 12}],
        )
        blocked = runner.hold_existing_gap_source(reconciled.id)
        source = reconciled.request["source_path"]
        client.tree[source] = []
        with patch.object(SimpleApplication, "_start_startup_thread"):
            application = SimpleApplication(
                state_root=Path(self.temporary.name) / "app",
                remote_root=self.library_root,
                remote=client,
                engine_runner=runner,
                enforce_engine_roots=False,
            )
        self.addCleanup(application.close)
        with patch.object(application, "_scan_inbound_once", return_value=[]), patch.object(
            application, "_start_startup_thread"
        ):
            application.set_paused(False, "test")
        with patch.object(runner, "executor", lambda _plan: (_ for _ in ()).throw(AssertionError("writer called"))):
            retried = application.retry_public_job(blocked.id, {})
        self.assertEqual(retried["phase"], "completed")
        self.assertEqual(retried["readback"]["status"], "source_held")
        self.assertEqual(len(client.move_calls), 1)

    def test_existing_gap_missing_source_with_unrecorded_hold_stays_needs_attention(self) -> None:
        """A bare target directory is not crash-recovery proof of an empty E source."""
        runner, client, reconciled = self._existing_gap_source_fixture()
        hold_root = f"{self.library_root}/ScrapeFlow/归档/{reconciled.id}/existing-gap-hold"
        # Simulate a manually/stale moved object: the source is absent and the
        # derived target happens to exist, but this task has no completion
        # receipt proving what was moved.
        client.move(self.intake_root, hold_root, ["Incoming"])
        client.move_calls.clear()

        blocked = runner.hold_existing_gap_source(reconciled.id)

        self.assertEqual(blocked.phase, "reconciliation_uncertain")
        self.assertEqual(blocked.summary["existing_gap_registration"]["status"], "failed")
        self.assertFalse(runner.existing_gap_source_hold_verified(reconciled.id))
        self.assertEqual(client.move_calls, [])

    def test_existing_gap_prepared_empty_hold_recovers_after_move_before_completion_write(self) -> None:
        """The exact empty pre-move receipt is enough to finish a crash recovery."""
        runner, client, reconciled = self._existing_gap_source_fixture()
        source = reconciled.request["source_path"]
        hold_root = f"{self.library_root}/ScrapeFlow/归档/{reconciled.id}/existing-gap-hold"
        target = f"{hold_root}/Incoming"
        from engine.scrapeflow.serialization import atomic_write_json

        summary = dict(reconciled.summary)
        summary["existing_gap_registration"] = {
            "status": "hold_prepared",
            "source": source,
            "target": target,
            "prepared_at": "2026-08-13T00:00:00+00:00",
            "evidence": "reconciliation.existing_gap.empty_ingress",
        }
        summary["source_fate"] = "hold_prepared"
        prepared = replace(reconciled, summary=summary)
        atomic_write_json(runner._job_path(prepared.id), prepared.as_dict(), allow_nan=False)  # noqa: SLF001
        # Simulate AList accepting the move immediately before the process
        # crashes; the target remains empty, as the narrow receipt promised.
        client.move(self.intake_root, hold_root, ["Incoming"])
        client.move_calls.clear()

        recovered = runner.hold_existing_gap_source(reconciled.id)

        self.assertEqual(recovered.phase, "completed")
        self.assertEqual(
            recovered.summary["existing_gap_registration"]["status"],
            "moved_to_hold",
        )
        self.assertTrue(runner.existing_gap_source_hold_verified(recovered.id))
        self.assertEqual(client.move_calls, [])

    def test_existing_gap_prepared_hold_reenters_through_public_empty_retry(self) -> None:
        """An interrupted E hand-off never falls through to identity/writer retry."""
        runner, client, reconciled = self._existing_gap_source_fixture()
        source = reconciled.request["source_path"]
        hold_root = f"{self.library_root}/ScrapeFlow/归档/{reconciled.id}/existing-gap-hold"
        target = f"{hold_root}/Incoming"
        from engine.scrapeflow.serialization import atomic_write_json

        summary = dict(reconciled.summary)
        summary.update({
            "existing_gap_registration": {
                "status": "hold_prepared",
                "source": source,
                "target": target,
                "prepared_at": "2026-08-13T00:00:00+00:00",
                "evidence": "reconciliation.existing_gap.empty_ingress",
            },
            "source_fate": "hold_prepared",
            "automatic_stage": "existing_gap_registration_failed",
        })
        reconciliation = dict(summary["reconciliation"])
        reconciliation["status"] = "needs_attention"
        summary["reconciliation"] = reconciliation
        interrupted = replace(
            reconciled,
            phase="reconciliation_uncertain",
            summary=summary,
            error="AList readback interrupted",
        )
        atomic_write_json(runner._job_path(interrupted.id), interrupted.as_dict(), allow_nan=False)  # noqa: SLF001
        client.move(self.intake_root, hold_root, ["Incoming"])
        client.move_calls.clear()

        with patch.object(SimpleApplication, "_start_startup_thread"):
            application = SimpleApplication(
                state_root=Path(self.temporary.name) / "app-prepared-retry",
                remote_root=self.library_root,
                remote=client,
                engine_runner=runner,
                enforce_engine_roots=False,
            )
        self.addCleanup(application.close)
        with patch.object(application, "_scan_inbound_once", return_value=[]), patch.object(
            application, "_start_startup_thread"
        ):
            application.set_paused(False, "test")
        retried = application.retry_public_job(interrupted.id, {})

        self.assertEqual(retried["engine_phase"], "completed")
        self.assertEqual(retried["readback"]["status"], "source_held")
        self.assertEqual(client.move_calls, [])

    def test_existing_gap_holder_failure_never_enters_formal_cleanup_retry(self) -> None:
        """A transient E hand-off error remains on the narrow holder lane."""
        runner, client, reconciled = self._existing_gap_source_fixture()
        with patch.object(SimpleApplication, "_start_startup_thread"):
            application = SimpleApplication(
                state_root=Path(self.temporary.name) / "app-holder-failure",
                remote_root=self.library_root,
                remote=client,
                engine_runner=runner,
                enforce_engine_roots=False,
            )
        self.addCleanup(application.close)
        with patch.object(application, "_scan_inbound_once", return_value=[]), patch.object(
            application, "_start_startup_thread"
        ):
            application.set_paused(False, "test")
        with patch.object(runner, "finalize_automatic_lifecycle") as finalizer:
            application._record_existing_gap_hold_failure(  # noqa: SLF001
                reconciled.id,
                RuntimeError("post-move readback temporarily unavailable"),
            )
        persisted = runner.get_job(reconciled.id)
        self.assertEqual(persisted.phase, "reconciliation_uncertain")
        self.assertEqual(
            persisted.summary["existing_gap_registration"]["status"],
            "failed",
        )
        self.assertNotIn("cleanup_only_retry", persisted.summary)
        finalizer.assert_not_called()

    def test_existing_gap_rejects_forged_ingress_path_without_mutation(self) -> None:
        runner, client, reconciled = self._existing_gap_source_fixture()
        from engine.scrapeflow.serialization import atomic_write_json

        summary = dict(reconciled.summary)
        summary["ingress_source_path"] = f"{self.library_root}/ScrapeFlow/归档/forged"
        forged = replace(reconciled, summary=summary)
        atomic_write_json(runner._job_path(forged.id), forged.as_dict(), allow_nan=False)  # noqa: SLF001

        with self.assertRaises(EngineJobConflictError):
            runner.hold_existing_gap_source(forged.id)
        self.assertEqual(client.move_calls, [])
        self.assertTrue(runner._remote_directory_exists(reconciled.request["source_path"]))  # noqa: SLF001

    def test_existing_gap_pause_and_cancellation_stop_before_hold_move(self) -> None:
        runner, client, reconciled = self._existing_gap_source_fixture()
        with self.assertRaises(EnginePauseRequested):
            runner.hold_existing_gap_source(reconciled.id, pause_requested=lambda: True)
        self.assertEqual(client.move_calls, [])
        self.assertEqual(runner.get_job(reconciled.id).phase, "reconciled")

        cancelled = runner.cancel_job(reconciled.id, reason="test cancellation")
        self.assertEqual(cancelled.phase, "cancelled")
        with self.assertRaises(EngineJobConflictError):
            runner.hold_existing_gap_source(reconciled.id)
        self.assertEqual(client.move_calls, [])
        self.assertTrue(runner._remote_directory_exists(reconciled.request["source_path"]))  # noqa: SLF001

    def test_duplicate_complete_consumes_owned_source_without_formal_writer(self) -> None:
        runner, client, reconciled = self._duplicate_source_fixture()
        runner.executor = lambda _plan: (_ for _ in ()).throw(AssertionError("writer called"))

        consumed = runner.consume_duplicate_complete_source(reconciled.id)
        marker = consumed.summary["duplicate_complete_consumption"]
        self.assertEqual(consumed.phase, "completed")
        self.assertEqual(marker["status"], "moved_to_processed")
        self.assertFalse(runner._remote_directory_exists(marker["source"]))  # noqa: SLF001
        self.assertTrue(runner._remote_directory_exists(marker["target"]))  # noqa: SLF001
        self.assertEqual(len(client.move_calls), 1)

        # A scheduler retry is a readback-only idempotent operation.
        repeated = runner.consume_duplicate_complete_source(reconciled.id)
        self.assertEqual(repeated.summary["duplicate_complete_consumption"], marker)
        self.assertEqual(len(client.move_calls), 1)

    def test_duplicate_complete_fails_closed_when_provider_state_is_active(self) -> None:
        runner, client, reconciled = self._duplicate_source_fixture()
        gap_dir = runner.state_root / "gaps" / reconciled.id
        gap_dir.mkdir(parents=True)
        (gap_dir / "gap.json").write_text(
            '{"phase":"waiting_reconcile","active_attempt":{"attempt_id":"a"}}',
            encoding="utf-8",
        )

        with self.assertRaises(EngineWorkerBusyError):
            runner.consume_duplicate_complete_source(reconciled.id)
        self.assertEqual(client.move_calls, [])
        self.assertTrue(runner._remote_directory_exists(reconciled.request["source_path"]))  # noqa: SLF001

    def test_duplicate_complete_blocks_stale_attempt_even_after_terminal_phase(self) -> None:
        runner, client, reconciled = self._duplicate_source_fixture()
        gap_dir = runner.state_root / "gaps" / reconciled.id
        gap_dir.mkdir(parents=True)
        (gap_dir / "gap.json").write_text(
            '{"phase":"completed","tier_status":"completed",'
            '"active_attempt":{"attempt_id":"stale"}}',
            encoding="utf-8",
        )

        with self.assertRaises(EngineWorkerBusyError):
            runner.consume_duplicate_complete_source(reconciled.id)
        self.assertEqual(client.move_calls, [])
        self.assertTrue(runner._remote_directory_exists(reconciled.request["source_path"]))  # noqa: SLF001

    def test_duplicate_complete_pause_checkpoint_precedes_remote_move(self) -> None:
        runner, client, reconciled = self._duplicate_source_fixture()
        checks = iter((False, True))

        with self.assertRaises(EnginePauseRequested):
            runner.consume_duplicate_complete_source(
                reconciled.id,
                pause_requested=lambda: next(checks),
            )
        self.assertEqual(len(client.move_calls), 0)
        self.assertTrue(runner._remote_directory_exists(reconciled.request["source_path"]))  # noqa: SLF001

    def test_duplicate_complete_rejects_incomplete_evidence_before_mutation(self) -> None:
        runner, client, reconciled = self._duplicate_source_fixture()
        summary = dict(reconciled.summary)
        reconciliation = dict(summary["reconciliation"])
        reconciliation.pop("reason", None)
        summary["reconciliation"] = reconciliation
        atomic = replace(reconciled, summary=summary)
        from engine.scrapeflow.serialization import atomic_write_json

        atomic_write_json(runner._job_path(atomic.id), atomic.as_dict(), allow_nan=False)  # noqa: SLF001

        with self.assertRaises(EngineJobConflictError):
            runner.consume_duplicate_complete_source(atomic.id)
        self.assertEqual(client.move_calls, [])

    def test_tiny_formal_movie_video_does_not_prove_duplicate_complete(self) -> None:
        target = f"{self.movie_root}/Tiny Movie (2020)"
        tree = self._tree()
        tree[self.movie_root] = [{"name": "Tiny Movie (2020)", "is_dir": True}]
        tree[target] = [
            {"name": "Tiny Movie (2020).mkv", "is_dir": False, "size": 1},
            {"name": "Tiny Movie (2020).nfo", "is_dir": False, "size": 10},
        ]
        client = ReadOnlyAList(
            tree,
            {
                f"{target}/Tiny Movie (2020).mkv": b"x",
                f"{target}/Tiny Movie (2020).nfo": _movie_nfo(111, "Tiny Movie"),
            },
        )
        runner = self._runner(client)

        result = self._reconcile(
            runner,
            source=f"{self.intake_root}/Incoming",
            match=self._match("movie", 111, "Tiny Movie"),
        )

        self.assertEqual(result.phase, "reconciled")
        self.assertEqual(result.summary["reconciliation"]["outcome"], "existing_gap")
        self.assertEqual(client.mutations, [])

    def test_confirmed_empty_movie_work_is_existing_gap_without_shelf(self) -> None:
        target = f"{self.movie_root}/Gap Movie (2020)"
        tree = self._tree()
        tree[self.movie_root] = [{"name": "Gap Movie (2020)", "is_dir": True}]
        tree[target] = [{"name": "Gap Movie (2020).nfo", "is_dir": False, "size": 10}]
        client = ReadOnlyAList(tree, {f"{target}/Gap Movie (2020).nfo": _movie_nfo(12, "Gap Movie")})
        runner = self._runner(client)

        result = self._reconcile(
            runner,
            source=f"{self.intake_root}/Incoming",
            match=self._match("movie", 12, "Gap Movie"),
        )

        self.assertEqual(result.phase, "reconciled")
        self.assertEqual(result.summary["reconciliation"]["outcome"], "existing_gap")
        self.assertIsNotNone(result.target_shelf)
        self.assertEqual(client.mutations, [])

    def test_incoming_movie_video_overlaps_existing_media_gap_as_merge(self) -> None:
        target = f"{self.movie_root}/Gap Movie (2020)"
        source = f"{self.intake_root}/Incoming"
        tree = self._tree()
        tree[self.movie_root] = [{"name": "Gap Movie (2020)", "is_dir": True}]
        tree[target] = [{"name": "Gap Movie (2020).nfo", "is_dir": False, "size": 10}]
        client = ReadOnlyAList(
            tree,
            {
                f"{target}/Gap Movie (2020).nfo": _movie_nfo(17, "Gap Movie"),
                f"{source}/Gap.Movie.2020.mkv": _ADMISSIBLE_VIDEO,
            },
        )
        runner = self._runner(client)

        result = self._reconcile(
            runner,
            source=source,
            match=self._match("movie", 17, "Gap Movie"),
        )

        self.assertEqual(result.phase, "reconciled")
        self.assertEqual(result.summary["reconciliation"]["outcome"], "merge_existing")
        self.assertEqual(client.mutations, [])

    def test_tiny_incoming_movie_does_not_become_merge_existing(self) -> None:
        target = f"{self.movie_root}/Gap Movie (2020)"
        source = f"{self.intake_root}/Incoming"
        tree = self._tree()
        tree[self.movie_root] = [{"name": "Gap Movie (2020)", "is_dir": True}]
        tree[target] = [{"name": "Gap Movie (2020).nfo", "is_dir": False, "size": 10}]
        client = ReadOnlyAList(
            tree,
            {
                f"{target}/Gap Movie (2020).nfo": _movie_nfo(117, "Gap Movie"),
                f"{source}/Gap.Movie.2020.mkv": b"tiny",
            },
        )
        runner = self._runner(client)

        result = self._reconcile(
            runner,
            source=source,
            match=self._match("movie", 117, "Gap Movie"),
        )

        self.assertEqual(result.phase, "reconciled")
        self.assertEqual(result.summary["reconciliation"]["outcome"], "existing_gap")
        self.assertEqual(client.mutations, [])

    def test_new_tv_episode_is_merge_existing_without_shelf(self) -> None:
        target = f"{self.anime_root}/Show (2020)"
        source = f"{self.intake_root}/Incoming"
        tree = self._tree()
        tree[self.anime_root] = [{"name": "Show (2020)", "is_dir": True}]
        tree[target] = [
            {"name": "tvshow.nfo", "is_dir": False, "size": 10},
            {"name": "S01E01.mkv", "is_dir": False, "size": len(_ADMISSIBLE_VIDEO)},
        ]
        client = ReadOnlyAList(
            tree,
            {
                f"{target}/tvshow.nfo": _tv_nfo(13, "Show"),
                f"{target}/S01E01.mkv": _ADMISSIBLE_VIDEO,
                f"{source}/S01E02.mkv": _ADMISSIBLE_VIDEO,
            },
        )
        runner = self._runner(client)

        result = self._reconcile(
            runner,
            source=source,
            match=self._match("tv", 13, "Show"),
        )

        self.assertEqual(result.phase, "reconciled")
        self.assertEqual(result.summary["reconciliation"]["outcome"], "merge_existing")
        self.assertEqual(result.summary["reconciliation"]["matched_shelf"], "anime")
        self.assertEqual(result.target_shelf, "anime")
        self.assertEqual(client.mutations, [])

    def test_tiny_incoming_tv_episode_does_not_become_merge_existing(self) -> None:
        target = f"{self.anime_root}/Show (2020)"
        source = f"{self.intake_root}/Incoming"
        tree = self._tree()
        tree[self.anime_root] = [{"name": "Show (2020)", "is_dir": True}]
        tree[target] = [
            {"name": "tvshow.nfo", "is_dir": False, "size": 10},
            {"name": "S01E01.mkv", "is_dir": False, "size": len(_ADMISSIBLE_VIDEO)},
        ]
        client = ReadOnlyAList(
            tree,
            {
                f"{target}/tvshow.nfo": _tv_nfo(113, "Show"),
                f"{target}/S01E01.mkv": _ADMISSIBLE_VIDEO,
                f"{source}/S01E02.mkv": b"tiny",
            },
        )
        runner = self._runner(client)

        result = self._reconcile(
            runner,
            source=source,
            match=self._match("tv", 113, "Show"),
        )

        self.assertEqual(result.phase, "reconciliation_uncertain")
        self.assertNotEqual(result.summary["reconciliation"]["outcome"], "merge_existing")
        self.assertEqual(client.mutations, [])

    def test_incoming_episode_overlaps_existing_episode_gap_as_merge(self) -> None:
        target = f"{self.anime_root}/Show (2020)"
        source = f"{self.intake_root}/Incoming"
        tree = self._tree()
        tree[self.anime_root] = [{"name": "Show (2020)", "is_dir": True}]
        tree[target] = [
            {"name": "tvshow.nfo", "is_dir": False, "size": 10},
            {"name": "S01E01.mkv", "is_dir": False, "size": len(_ADMISSIBLE_VIDEO)},
        ]
        client = ReadOnlyAList(
            tree,
            {
                f"{target}/tvshow.nfo": _tv_nfo(18, "Show"),
                f"{target}/S01E01.mkv": _ADMISSIBLE_VIDEO,
                f"{source}/S01E02.mkv": _ADMISSIBLE_VIDEO,
            },
        )
        runner = self._runner(client)

        class Catalog:
            def prefetch(self, _works: object) -> None:
                return None

            def __call__(self, _work: object) -> dict[int, set[int]]:
                return {1: {1, 2}}

        with patch("local.scrapeflow_api.simple_library_audit.TmdbEpisodeCatalog", return_value=Catalog()):
            result = self._reconcile(
                runner,
                source=source,
                match=self._match("tv", 18, "Show"),
            )

        self.assertEqual(result.phase, "reconciled")
        self.assertEqual(result.summary["reconciliation"]["outcome"], "merge_existing")
        self.assertEqual(client.mutations, [])

    def test_tv_sidecars_do_not_count_as_incoming_or_formal_episode_media(self) -> None:
        target = f"{self.anime_root}/Show (2020)"
        source = f"{self.intake_root}/Incoming"
        tree = self._tree()
        tree[self.anime_root] = [{"name": "Show (2020)", "is_dir": True}]
        tree[target] = [
            {"name": "tvshow.nfo", "is_dir": False, "size": 10},
            {"name": "S01E01.srt", "is_dir": False, "size": 10},
        ]
        client = ReadOnlyAList(
            tree,
            {
                f"{target}/tvshow.nfo": _tv_nfo(15, "Show"),
                f"{target}/S01E01.srt": b"formal sidecar",
                f"{source}/S01E02.srt": b"incoming sidecar",
            },
        )
        runner = self._runner(client)

        result = self._reconcile(
            runner,
            source=source,
            match=self._match("tv", 15, "Show"),
        )

        self.assertEqual(result.phase, "reconciliation_uncertain")
        self.assertEqual(result.summary["reconciliation"]["outcome"], "uncertain")
        self.assertNotEqual(result.summary["reconciliation"]["outcome"], "merge_existing")
        self.assertEqual(client.mutations, [])

    def test_multiple_formal_identity_matches_are_uncertain(self) -> None:
        first = f"{self.movie_root}/First (2020)"
        second = f"{self.movie_root}/Second (2020)"
        tree = self._tree()
        tree[self.movie_root] = [
            {"name": "First (2020)", "is_dir": True},
            {"name": "Second (2020)", "is_dir": True},
        ]
        tree[first] = [
            {"name": "First (2020).mkv", "is_dir": False, "size": 10},
            {"name": "First (2020).nfo", "is_dir": False, "size": 10},
        ]
        tree[second] = [
            {"name": "Second (2020).mkv", "is_dir": False, "size": 10},
            {"name": "Second (2020).nfo", "is_dir": False, "size": 10},
        ]
        client = ReadOnlyAList(
            tree,
            {
                f"{first}/First (2020).mkv": b"first",
                f"{first}/First (2020).nfo": _movie_nfo(14, "Same"),
                f"{second}/Second (2020).mkv": b"second",
                f"{second}/Second (2020).nfo": _movie_nfo(14, "Same"),
            },
        )
        runner = self._runner(client)

        result = self._reconcile(
            runner,
            source=f"{self.intake_root}/Incoming",
            match=self._match("movie", 14, "Same"),
        )

        self.assertEqual(result.phase, "reconciliation_uncertain")
        self.assertEqual(result.summary["reconciliation"]["outcome"], "uncertain")
        self.assertIn("多个正式作品", result.summary["reconciliation"]["reason"])
        self.assertEqual(client.mutations, [])

    def test_ambiguous_matcher_rejection_exposes_bounded_identity_candidates(self) -> None:
        """A safe matcher rejection surfaces its scored candidates to the U-node."""
        client = ReadOnlyAList(self._tree())
        runner = self._runner(client)
        scored = [
            SimpleNamespace(media_type="tv", tmdb_id=501, title="Show A",
                            year="2020", confidence=0.62, status="near_confident"),
            SimpleNamespace(media_type="movie", tmdb_id=502, title="Movie B",
                            year="2019", confidence=0.61, status="candidate"),
            # A collection row and an invalid TMDB id are read-only noise the
            # confirmation tuple cannot consume; they must be filtered out.
            SimpleNamespace(media_type="collection", tmdb_id=503, title="Set C",
                            year="2018", confidence=0.53, status="candidate"),
            SimpleNamespace(media_type="tv", tmdb_id=0, title="Broken",
                            year="2017", confidence=0.5, status="candidate"),
            SimpleNamespace(media_type="tv", tmdb_id=505, title="Show C",
                            year="2021", confidence=0.44, status="candidate"),
            # Bounded at five rows inside the matcher error itself.
            SimpleNamespace(media_type="tv", tmdb_id=506, title="Cut",
                            year="2016", confidence=0.4, status="candidate"),
        ]
        error = AutoMatchAmbiguityError(
            "自动匹配前两名证据无法区分，拒绝自动选择", candidates=scored,
        )
        pending = runner.create_pending_job(f"{self.intake_root}/Incoming")
        runner.start_automatic_job(pending.id, target_shelf="anime")
        runner.mark_reconciling(pending.id)
        with patch("engine.scraper.auto_match_tmdb", side_effect=error):
            result = runner.reconcile_automatic_job(pending.id)

        self.assertEqual(result.phase, "reconciliation_uncertain")
        reconciliation = result.summary["reconciliation"]
        self.assertEqual(reconciliation["outcome"], "uncertain")
        self.assertIn("自动匹配", reconciliation["reason"])
        self.assertEqual(reconciliation["identity_candidates"], [
            {"media_type": "tv", "tmdb_id": 501, "title": "Show A",
             "year": "2020", "confidence": 0.62, "status": "near_confident"},
            {"media_type": "movie", "tmdb_id": 502, "title": "Movie B",
             "year": "2019", "confidence": 0.61, "status": "candidate"},
            {"media_type": "tv", "tmdb_id": 505, "title": "Show C",
             "year": "2021", "confidence": 0.44, "status": "candidate"},
        ])
        # The public task payload carries the same bounded list so the
        # operator can confirm an identity without researching TMDB by hand.
        payload = SimpleApplication.public_engine_job(result)
        self.assertEqual(
            payload["reconciliation"]["identity_candidates"],
            reconciliation["identity_candidates"],
        )
        self.assertEqual(client.mutations, [])

    def test_classification_uncertain_exposes_trace_candidates(self) -> None:
        """Library-evidence uncertainty reuses the identity's own trace rows."""
        first = f"{self.movie_root}/First (2020)"
        second = f"{self.movie_root}/Second (2020)"
        tree = self._tree()
        tree[self.movie_root] = [
            {"name": "First (2020)", "is_dir": True},
            {"name": "Second (2020)", "is_dir": True},
        ]
        tree[first] = [
            {"name": "First (2020).mkv", "is_dir": False, "size": 10},
            {"name": "First (2020).nfo", "is_dir": False, "size": 10},
        ]
        tree[second] = [
            {"name": "Second (2020).mkv", "is_dir": False, "size": 10},
            {"name": "Second (2020).nfo", "is_dir": False, "size": 10},
        ]
        client = ReadOnlyAList(
            tree,
            {
                f"{first}/First (2020).mkv": b"first",
                f"{first}/First (2020).nfo": _movie_nfo(14, "Same"),
                f"{second}/Second (2020).mkv": b"second",
                f"{second}/Second (2020).nfo": _movie_nfo(14, "Same"),
            },
        )
        runner = self._runner(client)
        match = self._match("movie", 14, "Same")
        runner_up = SimpleNamespace(
            media_type="movie", tmdb_id=15, title="Same Remake",
            year="2023", confidence=0.41, status="candidate",
        )
        best_row = SimpleNamespace(
            media_type="movie", tmdb_id=14, title="Same",
            year="2020", confidence=0.99, status="confirmed",
        )
        pending = runner.create_pending_job(f"{self.intake_root}/Incoming")
        runner.start_automatic_job(pending.id, target_shelf="movie")
        runner.mark_reconciling(pending.id)
        with patch(
            "engine.scraper.auto_match_tmdb",
            return_value=(match, [best_row, runner_up]),
        ):
            result = runner.reconcile_automatic_job(pending.id)

        self.assertEqual(result.phase, "reconciliation_uncertain")
        reconciliation = result.summary["reconciliation"]
        self.assertIn("多个正式作品", reconciliation["reason"])
        self.assertEqual(reconciliation["identity_candidates"], [
            {"media_type": "movie", "tmdb_id": 14, "title": "Same",
             "year": "2020", "confidence": 0.99, "status": "confirmed"},
            {"media_type": "movie", "tmdb_id": 15, "title": "Same Remake",
             "year": "2023", "confidence": 0.41, "status": "candidate"},
        ])
        self.assertEqual(client.mutations, [])

    def test_uncertain_identity_confirmation_reopens_only_read_only_reconciliation(self) -> None:
        """A bounded confirmation may rerun B/C but cannot name a write path."""
        source = f"{self.intake_root}/Incoming"
        client = ReadOnlyAList(self._tree())
        runner = self._runner(client)
        pending = runner.create_pending_job(source)
        uncertain = replace(
            pending,
            phase="reconciliation_uncertain",
            summary={
                **pending.summary,
                "automatic_terminal": True,
                "reconciliation": {
                    "status": "needs_attention",
                    "outcome": "uncertain",
                    "reason": "multiple plausible matches",
                },
            },
            error="multiple plausible matches",
        )
        from engine.scrapeflow.serialization import atomic_write_json

        atomic_write_json(runner._job_path(pending.id), uncertain.as_dict(), allow_nan=False)  # noqa: SLF001
        reopened = runner.reopen_reconciliation_uncertain(
            pending.id,
            {"tmdb_id": 77, "media_type": "movie", "season": 1},
        )
        self.assertEqual(reopened.phase, "reconciling")
        self.assertEqual(reopened.request, {"source_path": source})
        self.assertEqual(reopened.plan, {})
        self.assertEqual(
            reopened.summary["reconciliation_identity_confirmation"]["tmdb_id"], 77,
        )

        # The existing Engine matcher still provides source title/year; the
        # confirmation only fixes the bounded TMDB/type tuple used by the
        # normal read-only formal-library comparison.
        with patch(
            "engine.scraper.auto_match_tmdb",
            return_value=(self._match("movie", 10, "Incoming"), []),
        ):
            reconciled = runner.reconcile_automatic_job(pending.id)
        self.assertEqual(reconciled.phase, "queued")
        self.assertEqual(reconciled.summary["reconciliation"]["outcome"], "new_work")
        self.assertEqual(reconciled.summary["reconciliation"]["identity"]["tmdb_id"], 77)
        self.assertEqual(client.mutations, [])

    def test_uncertain_identity_confirmation_rejects_unowned_fields_or_active_state(self) -> None:
        client = ReadOnlyAList(self._tree())
        runner = self._runner(client)
        pending = runner.create_pending_job(f"{self.intake_root}/Incoming")
        uncertain = replace(
            pending,
            phase="reconciliation_uncertain",
            summary={
                **pending.summary,
                "reconciliation": {
                    "status": "needs_attention",
                    "outcome": "uncertain",
                    "reason": "ambiguous",
                },
            },
        )
        from engine.scrapeflow.serialization import atomic_write_json

        atomic_write_json(runner._job_path(pending.id), uncertain.as_dict(), allow_nan=False)  # noqa: SLF001
        with self.assertRaises(EngineRequestError):
            runner.reopen_reconciliation_uncertain(
                pending.id,
                {"tmdb_id": 77, "media_type": "movie", "target_root": self.movie_root},
            )
        self.assertEqual(runner.get_job(pending.id).phase, "reconciliation_uncertain")

    def test_scheduler_reconciliation_only_hands_off_merge_existing(self) -> None:
        client = ReadOnlyAList(self._tree())
        runner = self._runner(client)
        pending = runner.create_pending_job(f"{self.intake_root}/Incoming")
        runner.start_automatic_job(pending.id, target_shelf="movie")
        pending = runner.mark_reconciling(pending.id)
        # Do not let startup recovery enqueue this fixture before the explicit
        # scheduler-boundary assertion below.
        with patch.object(SimpleApplication, "_start_startup_thread"):
            application = SimpleApplication(
                state_root=Path(self.temporary.name) / "app",
                remote_root=self.library_root,
                remote=client,
                engine_runner=runner,
            )
            application.set_paused(False, "test")
        self.addCleanup(application.close)
        reconciled = [
            replace(
                pending,
                phase="reconciliation_uncertain" if outcome == "uncertain" else "reconciled",
                summary={
                    **pending.summary,
                    "reconciliation": {"outcome": outcome},
                },
            )
            for outcome in ("duplicate_complete", "existing_gap", "uncertain", "new_work")
        ]
        with patch.object(runner, "reconcile_automatic_job", side_effect=reconciled) as reconcile, patch.object(
            runner, "consume_duplicate_complete_source", return_value=pending
        ) as consume_duplicate, patch.object(
            runner, "hold_existing_gap_source", return_value=pending
        ) as hold_existing, patch.object(
            runner, "prepare_reconciled_merge_job"
        ) as prepare, patch.object(
            runner, "plan_automatic_job"
        ) as plan, patch.object(runner, "execute_automatic") as execute, patch.object(
            application, "_queue_provider_job"
        ) as provider, patch.object(
            application, "_refresh_intake_settlement", return_value=False,
        ):
            for outcome in ("duplicate_complete", "existing_gap", "uncertain", "new_work"):
                with self.subTest(outcome=outcome):
                    application._run_automatic_job(pending.id)  # noqa: SLF001 - scheduler boundary
        self.assertEqual(reconcile.call_count, 4)
        consume_duplicate.assert_called_once()
        hold_existing.assert_called_once()
        prepare.assert_not_called()
        plan.assert_not_called()
        execute.assert_not_called()
        provider.assert_not_called()
        self.assertEqual(client.mutations, [])

    def test_paused_queue_accepts_only_read_only_reconciliation(self) -> None:
        client = ReadOnlyAList(self._tree())
        runner = self._runner(client)
        pending = runner.create_pending_job(f"{self.intake_root}/Incoming")
        selected_root = runner.start_automatic_job(pending.id, target_shelf="movie")
        pending = runner.mark_reconciling(pending.id)
        selected = replace(
            selected_root,
            phase="planned",
            target_shelf="movie",
            target_root=self.movie_root,
            selected_at="2026-08-12T00:00:00Z",
        )
        with patch.object(SimpleApplication, "_start_startup_thread"):
            application = SimpleApplication(
                state_root=Path(self.temporary.name) / "app",
                remote_root=self.library_root,
                remote=client,
                engine_runner=runner,
            )
        self.addCleanup(application.close)
        self.assertTrue(application.control()["paused"])

        with patch.object(runner, "get_job", return_value=pending), patch.object(
            application, "_schedule_timer"
        ) as schedule:
            application._queue_automatic_job(pending.id)  # noqa: SLF001 - pause dispatch boundary
        schedule.assert_called_once()

        with patch.object(runner, "get_job", return_value=selected), patch.object(
            application, "_schedule_timer"
        ) as schedule:
            application._queue_automatic_job(selected.id)  # noqa: SLF001 - pause dispatch boundary
        schedule.assert_not_called()

    def test_paused_reconciliation_persists_result_without_any_handoff(self) -> None:
        target = f"{self.movie_root}/Gap Movie (2020)"
        tree = self._tree()
        tree[self.movie_root] = [{"name": "Gap Movie (2020)", "is_dir": True}]
        tree[target] = [{"name": "Gap Movie (2020).nfo", "is_dir": False, "size": 10}]
        client = ReadOnlyAList(
            tree,
            {f"{target}/Gap Movie (2020).nfo": _movie_nfo(31, "Gap Movie")},
        )
        runner = self._runner(client)
        pending = runner.create_pending_job(f"{self.intake_root}/Incoming")
        runner.start_automatic_job(pending.id, target_shelf="movie")
        pending = runner.mark_reconciling(pending.id)
        with patch.object(SimpleApplication, "_start_startup_thread"):
            application = SimpleApplication(
                state_root=Path(self.temporary.name) / "app",
                remote_root=self.library_root,
                remote=client,
                engine_runner=runner,
            )
        self.addCleanup(application.close)
        self.assertTrue(application.control()["paused"])

        with patch("engine.scraper.auto_match_tmdb", return_value=(
            self._match("movie", 31, "Gap Movie"), [],
        )) as matcher, patch.object(
            runner, "prepare_reconciled_merge_job"
        ) as prepare, patch.object(runner, "plan_automatic_job") as plan, patch.object(
            runner, "execute_automatic"
        ) as execute, patch.object(application, "_queue_scoped_library_audit") as scoped_audit, patch.object(
            application, "_queue_provider_job"
        ) as provider:
            application._run_automatic_job(pending.id)  # noqa: SLF001 - paused read-only boundary

        reconciled = runner.get_job(pending.id)
        self.assertEqual(matcher.call_count, 1)
        self.assertEqual(reconciled.phase, "reconciled")
        self.assertEqual(reconciled.summary["reconciliation"]["outcome"], "existing_gap")
        prepare.assert_not_called()
        plan.assert_not_called()
        execute.assert_not_called()
        scoped_audit.assert_not_called()
        provider.assert_not_called()
        self.assertEqual(client.mutations, [])

    def test_scheduler_merge_existing_uses_existing_plan_and_execute_lane(self) -> None:
        client = ReadOnlyAList(self._tree())
        runner = self._runner(client)
        pending = runner.create_pending_job(f"{self.intake_root}/Incoming")
        runner.start_automatic_job(pending.id, target_shelf="movie")
        pending = runner.mark_reconciling(pending.id)
        reconciliation = {
            "outcome": "merge_existing",
            "identity": {"media_type": "movie", "tmdb_id": 23},
            "matched_shelf": "movie",
            "matched_formal_work": {"target_root": f"{self.movie_root}/Gap Movie (2020)"},
        }
        reconciled = replace(
            pending,
            phase="reconciled",
            summary={**pending.summary, "reconciliation": reconciliation},
        )
        queued = replace(
            reconciled,
            phase="queued",
            summary={
                **reconciled.summary,
                "merge_existing_ready": True,
            },
        )
        planned = replace(queued, phase="planned")
        executed = replace(planned, phase="executed")
        # Do not let startup recovery enqueue this fixture before the explicit
        # scheduler-boundary assertion below.
        with patch.object(SimpleApplication, "_start_startup_thread"):
            application = SimpleApplication(
                state_root=Path(self.temporary.name) / "app",
                remote_root=self.library_root,
                remote=client,
                engine_runner=runner,
            )
            application.set_paused(False, "test")
        self.addCleanup(application.close)
        self.assertTrue(application._ordinary_job_has_confirmed_selection(queued))  # noqa: SLF001
        # The ordinary queue guard must treat the durable reconciliation
        # hand-off as an authoritative selection instead of routing this
        # existing work back through the ``new_work`` shelf gate.
        with patch.object(runner, "get_job", return_value=queued), patch.object(
            application, "_schedule_timer"
        ) as schedule:
            application._queue_automatic_job(pending.id)  # noqa: SLF001 - queue predicate boundary
        schedule.assert_called_once()

        with patch.object(runner, "reconcile_automatic_job", return_value=reconciled) as reconcile, patch.object(
            runner, "prepare_reconciled_merge_job", return_value=queued
        ) as prepare, patch.object(runner, "plan_automatic_job", return_value=planned) as plan, patch.object(
            runner, "execute_automatic", return_value=executed
        ) as execute, patch.object(
            application, "_sync_replenishment_child", return_value=executed
        ) as sync, patch.object(
            application, "_settle_disabled_automatic_lifecycle", return_value=True
        ) as settle, patch.object(application, "_queue_provider_job") as provider:
            application._run_automatic_job(pending.id)  # noqa: SLF001 - scheduler boundary

        reconcile.assert_called_once_with(pending.id)
        prepare.assert_called_once_with(pending.id)
        plan.assert_called_once_with(pending.id)
        execute.assert_called_once_with(pending.id)
        sync.assert_called_once_with(executed)
        settle.assert_called_once_with(executed)
        provider.assert_not_called()
        self.assertEqual(client.mutations, [])

    def test_merge_existing_handoff_reuses_persisted_identity_and_work_root(self) -> None:
        target = f"{self.movie_root}/Gap Movie (2020)"
        source = f"{self.intake_root}/Incoming"
        tree = self._tree()
        tree[self.movie_root] = [{"name": "Gap Movie (2020)", "is_dir": True}]
        tree[target] = [{"name": "Gap Movie (2020).nfo", "is_dir": False, "size": 10}]
        client = ReadOnlyAList(
            tree,
            {
                f"{target}/Gap Movie (2020).nfo": _movie_nfo(21, "Gap Movie"),
                f"{source}/Gap.Movie.2020.mkv": _ADMISSIBLE_VIDEO,
            },
        )
        planner_requests: list[object] = []

        def planner(request, _alist, _tmdb):
            planner_requests.append(request)
            return Plan(
                mode="movie",
                source_root=source,
                target_root=target,
                files=[PlannedFile(
                    source_path=f"{source}/Gap.Movie.2020.mkv",
                    source_dir=source,
                    original_name="Gap.Movie.2020.mkv",
                    final_name="Gap Movie (2020).mkv",
                    target_dir=target,
                    media_kind="video",
                    source_size=len(_ADMISSIBLE_VIDEO),
                )],
                warnings=[],
                metadata={"tmdb_id": 21, "title": "Gap Movie", "year": "2020"},
            )

        runner = SimpleEngineRunner(
            Path(self.temporary.name),
            alist=client,
            tmdb=object(),
            planner=planner,
            validate=False,
            library_root=self.library_root,
        )
        reconciled = self._reconcile(
            runner, source=source, match=self._match("movie", 21, "Gap Movie"),
        )
        self.assertEqual(reconciled.summary["reconciliation"]["outcome"], "merge_existing")
        handed_off = runner.prepare_reconciled_merge_job(reconciled.id)
        self.assertEqual(handed_off.phase, "queued")
        self.assertIsNone(handed_off.target_shelf)
        self.assertIsNone(handed_off.target_root)
        self.assertEqual(handed_off.summary["target_work_path"], target)

        # The persisted reconciliation identity is authoritative after the
        # hand-off; planning must not invoke a second matcher/TMDB identity
        # implementation.
        with patch("engine.scraper.auto_match_tmdb", side_effect=AssertionError("matcher rerun")):
            planned = runner.plan_automatic_job(handed_off.id)
        self.assertEqual(planned.phase, "planned")
        self.assertEqual(planned.plan["target_root"], target)
        self.assertEqual(planned.summary["reconciliation"]["outcome"], "merge_existing")
        self.assertEqual(planner_requests[0].tmdb_id, 21)
        self.assertEqual(planner_requests[0].parent_path, self.movie_root)
        self.assertEqual(client.mutations, [])

    def test_merge_existing_plan_must_reuse_exact_matched_work_root(self) -> None:
        target = f"{self.movie_root}/Gap Movie (2020)"
        source = f"{self.intake_root}/Incoming"
        tree = self._tree()
        tree[self.movie_root] = [{"name": "Gap Movie (2020)", "is_dir": True}]
        tree[target] = [{"name": "Gap Movie (2020).nfo", "is_dir": False, "size": 10}]
        client = ReadOnlyAList(
            tree,
            {
                f"{target}/Gap Movie (2020).nfo": _movie_nfo(22, "Gap Movie"),
                f"{source}/Gap.Movie.2020.mkv": _ADMISSIBLE_VIDEO,
            },
        )

        def mismatching_planner(request, _alist, _tmdb):
            return Plan(
                mode="movie",
                source_root=source,
                target_root=f"{self.movie_root}/Another Movie (2020)",
                files=[],
                warnings=[],
                metadata={"tmdb_id": 22, "title": "Gap Movie", "year": "2020"},
            )

        runner = SimpleEngineRunner(
            Path(self.temporary.name),
            alist=client,
            tmdb=object(),
            planner=mismatching_planner,
            validate=False,
            library_root=self.library_root,
        )
        reconciled = self._reconcile(
            runner, source=source, match=self._match("movie", 22, "Gap Movie"),
        )
        handed_off = runner.prepare_reconciled_merge_job(reconciled.id)
        failed = runner.plan_automatic_job(handed_off.id)
        self.assertEqual(failed.phase, "failed_planning")
        self.assertTrue(failed.summary["automatic_terminal"])
        self.assertEqual(failed.plan, {})
        self.assertIn("既有作品", failed.error or "")
        self.assertEqual(client.mutations, [])
