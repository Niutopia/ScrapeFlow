from __future__ import annotations

import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from engine.scraper import AListClient
from local.scrapeflow_api.simple_library_audit import (
    DEFAULT_FORMAL_LIBRARY_ROOTS,
    SimpleLibraryAuditor,
    audit_and_persist,
    latest_audit_path,
)


class TreeAList:
    """Small list-only AList fake; it deliberately has no write methods."""

    def __init__(self, tree: dict[str, list[dict[str, object]]]) -> None:
        self.tree = {path: [dict(row) for row in rows] for path, rows in tree.items()}
        self.calls: list[tuple[str, bool]] = []

    def list(self, path: str, refresh: bool = False) -> list[dict[str, object]]:
        self.calls.append((path, refresh))
        return [dict(row) for row in self.tree.get(path, [])]


class OneArgumentTreeAList:
    def __init__(self) -> None:
        self.calls: list[str] = []

    def list(self, path: str) -> list[dict[str, object]]:
        self.calls.append(path)
        return []


class LoginRequiredAList(TreeAList):
    def __init__(self) -> None:
        super().__init__({root: [] for root in DEFAULT_FORMAL_LIBRARY_ROOTS})
        self.token: str | None = None
        self.login_calls = 0

    def login(self) -> str:
        self.login_calls += 1
        self.token = "logged-in"
        return self.token

    def list(self, path: str, refresh: bool = False) -> list[dict[str, object]]:
        if self.token is None:
            raise RuntimeError("not logged in")
        return super().list(path, refresh=refresh)


class BrokenAList:
    def list(self, _path: str, refresh: bool = False) -> list[dict[str, object]]:
        del refresh
        raise RuntimeError("connection password=do-not-persist")


class SimpleLibraryAuditTests(unittest.TestCase):
    def test_recursive_inventory_persists_plain_actionable_report(self) -> None:
        movie_root, anime_root, us_root = DEFAULT_FORMAL_LIBRARY_ROOTS
        client = TreeAList({
            movie_root: [
                {"name": "Movie", "is_dir": True},
                {"name": "Empty", "is_dir": True},
                {"name": "orphan.part", "is_dir": False, "size": 4},
            ],
            f"{movie_root}/Movie": [
                {"name": "same.mkv", "is_dir": False, "size": 10},
                {"name": "empty-video.mkv", "is_dir": False, "size": 0},
                {"name": "same.srt", "is_dir": False, "size": 2},
                {"name": "movie.nfo", "is_dir": False, "size": 3},
                {"name": "poster.jpg", "is_dir": False, "size": 5},
            ],
            f"{movie_root}/Empty": [],
            anime_root: [{"name": "Anime", "is_dir": True}],
            f"{anime_root}/Anime": [{"name": "Season 1", "is_dir": True}],
            f"{anime_root}/Anime/Season 1": [
                {"name": "same.mkv", "is_dir": False, "size": 10},
            ],
            us_root: [],
        })
        ticks = iter(("2026-08-07T00:00:00Z", "2026-08-07T00:00:01Z"))
        with tempfile.TemporaryDirectory() as directory:
            report = audit_and_persist(
                client,
                Path(directory),
                clock=lambda: next(ticks),
            )
            saved = json.loads(latest_audit_path(directory).read_text(encoding="utf-8"))

        self.assertEqual(report, saved)
        self.assertEqual(report["status"], "completed")
        self.assertTrue(report["available"])
        self.assertTrue(report["complete"])
        self.assertFalse(report["clean"])
        self.assertEqual(report["started_at"], "2026-08-07T00:00:00Z")
        self.assertEqual(report["finished_at"], "2026-08-07T00:00:01Z")
        self.assertEqual(report["counts"], {
            "files": 7,
            "directories": 7,
            "videos": 3,
            "subtitles": 1,
            "nfo": 1,
            "posters": 1,
        })
        self.assertEqual(
            [row["path"] for row in report["zero_byte_files"]],
            [f"{movie_root}/Movie/empty-video.mkv"],
        )
        self.assertEqual(
            [row["path"] for row in report["temporary_entries"]],
            [f"{movie_root}/orphan.part"],
        )
        self.assertEqual(report["duplicates"], [{
            "basename": "same.mkv",
            "size": 10,
            "paths": [
                f"{movie_root}/Movie/same.mkv",
                f"{anime_root}/Anime/Season 1/same.mkv",
            ],
        }])
        self.assertEqual(report["empty_directories"], [f"{movie_root}/Empty", us_root])
        # Duplicate/empty findings are evidence for manual inspection only;
        # they must not be exposed as remote-write/delete tasks.
        task_kinds = {row["kind"] for row in report["automatic_tasks"]}
        self.assertNotIn("possible_duplicate", task_kinds)
        self.assertNotIn("empty_directory", task_kinds)
        media = {row["path"]: row for row in report["observations"]["media_directories"]}
        self.assertEqual(media[f"{movie_root}/Movie"]["subtitle_count"], 1)
        self.assertTrue(media[f"{movie_root}/Movie"]["has_nfo"])
        self.assertTrue(media[f"{movie_root}/Movie"]["has_poster"])
        self.assertFalse(media[f"{anime_root}/Anime/Season 1"]["has_nfo"])
        self.assertFalse(media[f"{anime_root}/Anime/Season 1"]["has_poster"])
        self.assertTrue({
            "zero_byte_file",
            "temporary_entry",
            "missing_nfo",
            "missing_poster",
        }.issubset(task_kinds))
        self.assertTrue(all(refresh for _path, refresh in client.calls))
        self.assertNotIn("sha", json.dumps(report, ensure_ascii=False).casefold())

    def test_observational_duplicates_and_empty_directories_do_not_block_clean(self) -> None:
        """Same-size metadata collisions and placeholders remain read-only evidence."""
        movie_root, anime_root, us_root = DEFAULT_FORMAL_LIBRARY_ROOTS
        first = f"{movie_root}/First"
        second = f"{movie_root}/Second"
        placeholder = f"{movie_root}/Season 00 Placeholder"
        client = TreeAList({
            movie_root: [
                {"name": "First", "is_dir": True},
                {"name": "Second", "is_dir": True},
                {"name": "Season 00 Placeholder", "is_dir": True},
            ],
            first: [{"name": "tvshow.nfo", "is_dir": False, "size": 12}],
            second: [{"name": "tvshow.nfo", "is_dir": False, "size": 12}],
            placeholder: [],
            anime_root: [],
            us_root: [],
        })

        report = SimpleLibraryAuditor(client).scan()

        self.assertTrue(report["complete"])
        self.assertTrue(report["clean"])
        self.assertEqual(report["automatic_tasks"], [])
        self.assertEqual(report["duplicates"], [{
            "basename": "tvshow.nfo",
            "size": 12,
            "paths": [f"{movie_root}/First/tvshow.nfo", f"{movie_root}/Second/tvshow.nfo"],
        }])
        self.assertEqual(
            report["empty_directories"],
            sorted([anime_root, placeholder, us_root], key=str.casefold),
        )

    def test_residuals_archives_and_orphan_subtitles_are_visible_but_not_delete_tasks(self) -> None:
        movie_root, anime_root, us_root = DEFAULT_FORMAL_LIBRARY_ROOTS
        movie = f"{movie_root}/Residuals"
        client = TreeAList({
            movie_root: [{"name": "Residuals", "is_dir": True}],
            movie: [
                {"name": "Example.mkv", "is_dir": False, "size": 10},
                {"name": "Example.zh-CN.srt", "is_dir": False, "size": 2},
                {"name": "Detached.en.srt", "is_dir": False, "size": 2},
                {"name": "notes.pdf", "is_dir": False, "size": 3},
                {"name": "source.7z", "is_dir": False, "size": 4},
                {"name": "mystery.payload", "is_dir": False, "size": 5},
                {"name": "movie.nfo", "is_dir": False, "size": 6},
                {"name": "poster.jpg", "is_dir": False, "size": 7},
            ],
            anime_root: [],
            us_root: [],
        })

        report = SimpleLibraryAuditor(client).scan()

        self.assertFalse(report["clean"])
        self.assertEqual(report["automatic_tasks"], [])
        self.assertEqual(
            [row["path"] for row in report["orphan_subtitles"]],
            [f"{movie}/Detached.en.srt"],
        )
        self.assertEqual(
            [row["path"] for row in report["archives"]], [f"{movie}/source.7z"],
        )
        attachments = {
            row["path"]: row["residual_kind"]
            for row in report["attachments"]
        }
        self.assertEqual(attachments[f"{movie}/notes.pdf"], "document_or_comic")
        self.assertEqual(
            [row["path"] for row in report["unknown_files"]],
            [f"{movie}/mystery.payload"],
        )
        self.assertEqual(report["observations"]["archives"], report["archives"])
        self.assertTrue(all(
            row["kind"] not in {"archive", "orphan_subtitle", "unknown_file"}
            for row in report["automatic_tasks"]
        ))

    def test_tv_season_inherits_nearest_tvshow_nfo_and_poster(self) -> None:
        movie_root, anime_root, us_root = DEFAULT_FORMAL_LIBRARY_ROOTS
        bundle = f"{anime_root}/Bundle"
        show = f"{bundle}/Show"
        season = f"{show}/Season 1"
        client = TreeAList({
            movie_root: [],
            anime_root: [{"name": "Bundle", "is_dir": True}],
            bundle: [
                {"name": "tvshow.nfo", "is_dir": False, "size": 1},
                {"name": "poster.jpg", "is_dir": False, "size": 1},
                {"name": "Show", "is_dir": True},
            ],
            show: [
                {"name": "tvshow.nfo", "is_dir": False, "size": 2},
                {"name": "poster.jpg", "is_dir": False, "size": 2},
                {"name": "Season 1", "is_dir": True},
            ],
            season: [{"name": "Episode 01.mkv", "is_dir": False, "size": 10}],
            us_root: [],
        })

        report = SimpleLibraryAuditor(client).scan()

        evidence = {
            row["path"]: row for row in report["observations"]["media_directories"]
        }[season]
        self.assertTrue(evidence["has_nfo"])
        self.assertTrue(evidence["has_poster"])
        self.assertTrue(evidence["nfo_inherited"])
        self.assertTrue(evidence["poster_inherited"])
        self.assertEqual(evidence["nfo_path"], f"{show}/tvshow.nfo")
        self.assertEqual(evidence["poster_path"], f"{show}/poster.jpg")
        self.assertEqual(evidence["metadata_source"], show)
        missing_for_season = [
            row for row in report["automatic_tasks"]
            if row.get("path") == season and row["kind"] in {
                "missing_nfo", "missing_poster",
            }
        ]
        self.assertEqual(missing_for_season, [])
        self.assertIn(movie_root, report["empty_directories"])
        self.assertIn(us_root, report["empty_directories"])
        self.assertFalse(any(
            row["kind"] == "empty_directory" and row["path"] in {movie_root, us_root}
            for row in report["automatic_tasks"]
        ))

    def test_episode_nfo_does_not_shadow_parent_tvshow_metadata(self) -> None:
        movie_root, anime_root, us_root = DEFAULT_FORMAL_LIBRARY_ROOTS
        show = f"{anime_root}/Episode Sidecars"
        season = f"{show}/Season 03"
        client = TreeAList({
            movie_root: [],
            anime_root: [{"name": "Episode Sidecars", "is_dir": True}],
            show: [
                {"name": "tvshow.nfo", "is_dir": False, "size": 2},
                {"name": "Season 03", "is_dir": True},
            ],
            season: [
                {"name": "Episode Sidecars S03E05.mkv", "is_dir": False, "size": 10},
                # This is per-episode metadata, not a competing work-level
                # NFO boundary for the season directory.
                {"name": "Episode Sidecars S03E05.nfo", "is_dir": False, "size": 2},
            ],
            us_root: [],
        })

        report = SimpleLibraryAuditor(client).scan()

        evidence = {
            row["path"]: row for row in report["observations"]["media_directories"]
        }[season]
        self.assertTrue(evidence["has_nfo"])
        self.assertTrue(evidence["nfo_inherited"])
        self.assertEqual(evidence["nfo_path"], f"{show}/tvshow.nfo")
        self.assertEqual(evidence["metadata_source"], show)
        self.assertEqual(evidence["nfo_count"], 1)

    def test_missing_client_is_persisted_as_unavailable_not_clean(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            report = audit_and_persist(None, directory, clock=lambda: "2026-08-07T00:00:00Z")
            saved = json.loads(latest_audit_path(directory).read_text(encoding="utf-8"))

        self.assertEqual(report, saved)
        self.assertEqual(report["status"], "unavailable")
        self.assertFalse(report["available"])
        self.assertFalse(report["complete"])
        self.assertIsNone(report["clean"])
        self.assertEqual(report["errors"], [{
            "scope": "client", "code": "directory_listing_unavailable",
        }])
        self.assertTrue(all(row["status"] == "unavailable" for row in report["roots"]))

    def test_listing_error_is_visible_and_does_not_persist_exception_text(self) -> None:
        report = SimpleLibraryAuditor(
            BrokenAList(),
            clock=lambda: "2026-08-07T00:00:00Z",
        ).scan()

        self.assertEqual(report["status"], "error")
        self.assertFalse(report["available"])
        self.assertFalse(report["complete"])
        self.assertIsNone(report["clean"])
        self.assertEqual(len(report["errors"]), 3)
        self.assertEqual(
            {row["error_type"] for row in report["errors"]}, {"RuntimeError"},
        )
        self.assertNotIn("password=do-not-persist", json.dumps(report, ensure_ascii=False))

    def test_small_one_argument_fake_and_real_client_login_shape_are_supported(self) -> None:
        one_argument = OneArgumentTreeAList()
        report = SimpleLibraryAuditor(one_argument).scan()
        self.assertEqual(report["status"], "completed")
        self.assertEqual(one_argument.calls, list(DEFAULT_FORMAL_LIBRARY_ROOTS))

        login_required = LoginRequiredAList()
        logged_in_report = SimpleLibraryAuditor(login_required).scan()
        self.assertEqual(logged_in_report["status"], "completed")
        self.assertEqual(login_required.login_calls, 1)

    def test_real_alist_client_can_be_injected_without_a_network_call(self) -> None:
        client = AListClient(
            "http://127.0.0.1:5244",
            "admin",
            "test-password",
            allow_insecure_http=True,
        )

        def login() -> str:
            client.token = "test-token"
            return client.token

        with patch.object(client, "login", side_effect=login) as mocked_login, patch.object(
            client, "list", return_value=[]
        ) as mocked_list:
            report = SimpleLibraryAuditor(client).scan()

        self.assertEqual(report["status"], "completed")
        mocked_login.assert_called_once_with()
        self.assertEqual(mocked_list.call_count, len(DEFAULT_FORMAL_LIBRARY_ROOTS))

    def test_invalid_file_size_is_an_error_not_a_successful_empty_library(self) -> None:
        root = DEFAULT_FORMAL_LIBRARY_ROOTS[0]
        client = TreeAList({
            root: [{"name": "bad.mkv", "is_dir": False, "size": "unknown"}],
            DEFAULT_FORMAL_LIBRARY_ROOTS[1]: [],
            DEFAULT_FORMAL_LIBRARY_ROOTS[2]: [],
        })
        report = SimpleLibraryAuditor(client).scan()

        self.assertEqual(report["status"], "error")
        self.assertFalse(report["complete"])
        self.assertIsNone(report["clean"])
        first = report["roots"][0]
        self.assertEqual(first["status"], "error")
        self.assertEqual(first["error"], "SimpleLibraryAuditError")


if __name__ == "__main__":
    unittest.main()
