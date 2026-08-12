"""Focused coverage for the read-only ordinary-intake reconciliation entry."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from local.simple_server import SimpleApplication
from local.scrapeflow_api.simple_engine_runner import (
    EngineJobConflictError,
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
            {"path": full_path, "full_path": full_path}
            for full_path in sorted(self.files)
            if full_path.startswith(prefix)
        ]

    def _mutation(self, name: str, *_args: object, **_kwargs: object) -> None:
        self.mutations.append(name)
        raise AssertionError(f"reconciliation must not call {name}")

    mkdir = ensure_directory = move = rename = upload_bytes = remove = _mutation


def _movie_nfo(tmdb_id: int, title: str = "Movie") -> bytes:
    return (
        f"<movie><tmdbid>{tmdb_id}</tmdbid><title>{title}</title><year>2020</year></movie>"
    ).encode("utf-8")


def _tv_nfo(tmdb_id: int, title: str = "Show") -> bytes:
    return (
        f"<tvshow><tmdbid>{tmdb_id}</tmdbid><title>{title}</title><year>2020</year></tvshow>"
    ).encode("utf-8")


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

        self.assertEqual(result.phase, "awaiting_target_shelf")
        self.assertEqual(result.summary["reconciliation"]["outcome"], "new_work")
        started = runner.start_automatic_job(result.id, target_shelf="movie")
        self.assertEqual(started.phase, "queued")
        self.assertEqual(client.mutations, [])

    def test_complete_formal_nfo_match_is_duplicate_without_writer_or_provider(self) -> None:
        target = f"{self.movie_root}/Movie (2020)"
        tree = self._tree()
        tree[self.movie_root] = [{"name": "Movie (2020)", "is_dir": True}]
        tree[target] = [
            {"name": "Movie (2020).mkv", "is_dir": False, "size": 10},
            {"name": "Movie (2020).nfo", "is_dir": False, "size": 10},
        ]
        client = ReadOnlyAList(
            tree,
            {
                f"{target}/Movie (2020).mkv": b"video",
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
        self.assertIsNone(result.target_shelf)
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
                f"{source}/Gap.Movie.2020.mkv": b"incoming movie",
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

    def test_new_tv_episode_is_merge_existing_without_shelf(self) -> None:
        target = f"{self.anime_root}/Show (2020)"
        source = f"{self.intake_root}/Incoming"
        tree = self._tree()
        tree[self.anime_root] = [{"name": "Show (2020)", "is_dir": True}]
        tree[target] = [
            {"name": "tvshow.nfo", "is_dir": False, "size": 10},
            {"name": "S01E01.mkv", "is_dir": False, "size": 10},
        ]
        client = ReadOnlyAList(
            tree,
            {
                f"{target}/tvshow.nfo": _tv_nfo(13, "Show"),
                f"{target}/S01E01.mkv": b"formal episode",
                f"{source}/S01E02.mkv": b"incoming episode",
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
        self.assertIsNone(result.target_shelf)
        self.assertEqual(client.mutations, [])

    def test_incoming_episode_overlaps_existing_episode_gap_as_merge(self) -> None:
        target = f"{self.anime_root}/Show (2020)"
        source = f"{self.intake_root}/Incoming"
        tree = self._tree()
        tree[self.anime_root] = [{"name": "Show (2020)", "is_dir": True}]
        tree[target] = [
            {"name": "tvshow.nfo", "is_dir": False, "size": 10},
            {"name": "S01E01.mkv", "is_dir": False, "size": 10},
        ]
        client = ReadOnlyAList(
            tree,
            {
                f"{target}/tvshow.nfo": _tv_nfo(18, "Show"),
                f"{target}/S01E01.mkv": b"formal episode",
                f"{source}/S01E02.mkv": b"incoming episode",
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

    def test_scheduler_reconciliation_branch_never_calls_plan_writer_or_provider(self) -> None:
        client = ReadOnlyAList(self._tree())
        runner = self._runner(client)
        pending = runner.create_pending_job(f"{self.intake_root}/Incoming")
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
        reconciled = object()
        with patch.object(runner, "reconcile_automatic_job", return_value=reconciled) as reconcile, patch.object(
            runner, "plan_automatic_job"
        ) as plan, patch.object(runner, "execute_automatic") as execute, patch.object(
            application, "_queue_provider_job"
        ) as provider:
            application._run_automatic_job(pending.id)  # noqa: SLF001 - scheduler boundary
        reconcile.assert_called_once_with(pending.id)
        plan.assert_not_called()
        execute.assert_not_called()
        provider.assert_not_called()
        self.assertEqual(client.mutations, [])
