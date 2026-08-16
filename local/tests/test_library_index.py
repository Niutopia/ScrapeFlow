"""Tests for the D-node three-shelf LibraryIndex and five-way reconciliation."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from engine.scrapeflow.root_boundaries import analyze_root_boundaries
from engine.scrapeflow.unit_identity import apply_work_unit_override
from engine.scrapeflow.work_units import load_work_unit_records

from local.scrapeflow_api.library_index import (
    build_library_index,
    decide_reconciliation,
    reconcile_root_work_units,
)


def _nfo_tv(tmdb_id: int, title: str, year: str) -> bytes:
    return (
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        f"<tvshow><title>{title}</title><year>{year}</year>"
        f"<tmdbid>{tmdb_id}</tmdbid></tvshow>\n"
    ).encode("utf-8")


def _nfo_movie(tmdb_id: int, title: str, year: str) -> bytes:
    return (
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        f"<movie><title>{title}</title><year>{year}</year>"
        f"<tmdbid>{tmdb_id}</tmdbid></movie>\n"
    ).encode("utf-8")


class IndexAList:
    """Read-only AList double over a files dict (plus explicit dirs/moves)."""

    def __init__(self, files: dict[str, bytes] | None = None) -> None:
        self.files: dict[str, bytes] = dict(files or {})
        self.dirs: set[str] = set()
        self.move_calls: list[tuple[str, str, list[str]]] = []
        for full_path in self.files:
            parts = full_path.strip("/").split("/")[:-1]
            current = ""
            for part in parts:
                current += "/" + part
                self.dirs.add(current)

    def list(self, path: str, refresh: bool = False) -> list[dict[str, object]]:
        del refresh
        normalized = path.rstrip("/") or "/"
        prefix = normalized.rstrip("/") + "/"
        directory_paths: set[str] = set()
        for full_path in self.files:
            parts = full_path.strip("/").split("/")[:-1]
            current = ""
            for part in parts:
                current += "/" + part
                directory_paths.add(current)
        directory_paths |= self.dirs
        rows: dict[str, dict[str, object]] = {}
        for directory in directory_paths:
            if not directory.startswith(prefix):
                continue
            remainder = directory[len(prefix):]
            if remainder and "/" not in remainder:
                rows[remainder] = {"name": remainder, "is_dir": True}
        for full_path in self.files:
            if not full_path.startswith(prefix):
                continue
            remainder = full_path[len(prefix):]
            if remainder and "/" not in remainder:
                rows[remainder] = {
                    "name": remainder,
                    "is_dir": False,
                    "size": len(self.files[full_path]),
                }
        return [rows[name] for name in sorted(rows)]

    def read_file_bytes(self, path: str, *, max_bytes: int | None = None) -> bytes:
        value = self.files.get(path)
        if value is None:
            raise FileNotFoundError(path)
        if max_bytes is not None:
            return value[:max_bytes]
        return value

    def ensure_directory(self, path: str) -> bool:
        self.dirs.add(path.rstrip("/") or "/")
        return True

    def move(self, parent: str, target: str, names: list[str]) -> bool:
        parent = parent.rstrip("/")
        target = target.rstrip("/")
        self.move_calls.append((parent, target, list(names)))
        for name in names:
            src = f"{parent}/{name}"
            dst = f"{target}/{name}"
            self.dirs.discard(src)
            self.dirs.add(dst)
            moved: dict[str, bytes] = {}
            for full_path, payload in list(self.files.items()):
                if full_path == src or full_path.startswith(src + "/"):
                    moved[full_path[len(src):]] = payload
                    del self.files[full_path]
            for relative, payload in moved.items():
                self.files[dst + relative] = payload
        return True


def _sample_library() -> dict[str, bytes]:
    files: dict[str, bytes] = {}
    files["/library/番剧/Fate Zero/tvshow.nfo"] = _nfo_tv(35507, "Fate/Zero", "2011")
    for episode in range(1, 11):
        files[f"/library/番剧/Fate Zero/Season 01/S01E{episode:02d}.mkv"] = b"v"
    files["/library/电影/Inception (2010)/movie.nfo"] = _nfo_movie(27205, "Inception", "2010")
    files["/library/电影/Inception (2010)/Inception.2010.2160p.mkv"] = b"v"
    files["/library/美剧/Breaking Bad/tvshow.nfo"] = _nfo_tv(1396, "Breaking Bad", "2008")
    for episode in range(1, 9):
        files[f"/library/美剧/Breaking Bad/Season 01/S01E{episode:02d}.mkv"] = b"v"
    return files


class LibraryIndexTests(unittest.TestCase):
    def setUp(self) -> None:
        self.alist = IndexAList(_sample_library())
        self.index = build_library_index(self.alist, "/library")

    def test_index_reads_nfo_identities_and_episode_coverage(self) -> None:
        fate = self.index.entries_for("tv", 35507)
        self.assertEqual(len(fate), 1)
        self.assertEqual(fate[0].shelf, "anime")
        self.assertEqual(fate[0].work_root, "/library/番剧/Fate Zero")
        self.assertEqual(fate[0].title, "Fate/Zero")
        self.assertEqual(fate[0].year, "2011")
        self.assertEqual(
            fate[0].episode_tokens,
            {f"S01E{e:02d}" for e in range(1, 11)},
        )
        movie = self.index.entries_for("movie", 27205)
        self.assertEqual(len(movie), 1)
        self.assertEqual(movie[0].shelf, "movie")
        breaking = self.index.entries_for("tv", 1396)
        self.assertEqual(len(breaking), 1)
        self.assertEqual(breaking[0].shelf, "us_tv")

    def test_decide_new_work_when_absent_from_all_shelves(self) -> None:
        decision = decide_reconciliation(
            self.index, media_type="tv", tmdb_id=999, unit_tokens=frozenset({"S01E01"}),
        )
        self.assertEqual(decision.outcome, "new_work")
        self.assertIsNone(decision.shelf)
        self.assertIsNone(decision.work_root)

    def test_decide_merge_existing_locks_the_existing_shelf(self) -> None:
        # The root task may be created with a different shelf; the existing
        # 番剧 work must still win (cross-shelf dedup).
        decision = decide_reconciliation(
            self.index, media_type="tv", tmdb_id=35507, unit_tokens=frozenset({"S01E11"}),
        )
        self.assertEqual(decision.outcome, "merge_existing")
        self.assertEqual(decision.shelf, "anime")
        self.assertEqual(decision.work_root, "/library/番剧/Fate Zero")

    def test_decide_duplicate_complete_when_all_media_present(self) -> None:
        decision = decide_reconciliation(
            self.index, media_type="tv", tmdb_id=35507, unit_tokens=frozenset({"S01E03"}),
        )
        self.assertEqual(decision.outcome, "duplicate_complete")
        self.assertEqual(decision.work_root, "/library/番剧/Fate Zero")

    def test_decide_existing_gap_when_known_gaps_remain_uncovered(self) -> None:
        decision = decide_reconciliation(
            self.index,
            media_type="tv",
            tmdb_id=1396,
            unit_tokens=frozenset({"S01E03"}),
            known_gap_tokens=frozenset({"S02E01"}),
        )
        self.assertEqual(decision.outcome, "existing_gap")
        self.assertEqual(decision.shelf, "us_tv")

    def test_decide_uncertain_when_identity_spans_two_shelves(self) -> None:
        files = _sample_library()
        files["/library/番剧/Inception copy/movie.nfo"] = _nfo_movie(27205, "Inception", "2010")
        files["/library/番剧/Inception copy/Inception.2010.2160p.mkv"] = b"v"
        conflicted = build_library_index(IndexAList(files), "/library")
        decision = decide_reconciliation(
            conflicted, media_type="movie", tmdb_id=27205,
        )
        self.assertEqual(decision.outcome, "uncertain")

    def test_reconcile_root_work_units_persists_the_decision(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            state_root = Path(directory)
            root_task_id = "root-fz"
            files = _sample_library()
            files["/incoming/Fate Zero/S01E11.mkv"] = b"v"
            files["/incoming/Fate Zero/S01E12.mkv"] = b"v"
            alist = IndexAList(files)
            analyze_root_boundaries(
                alist, "/incoming/Fate Zero",
                root_task_id=root_task_id, state_root=state_root,
            )
            records = load_work_unit_records(state_root, root_task_id)
            self.assertEqual(len(records), 1)
            apply_work_unit_override(
                state_root, root_task_id, records[0].work_unit_id,
                media_type="tv", tmdb_id=35507,
            )
            reconciled = reconcile_root_work_units(
                alist, "/library", state_root, root_task_id,
            )
            self.assertEqual(reconciled[0].reconciliation_outcome, "merge_existing")
            self.assertEqual(reconciled[0].matched_work_root, "/library/番剧/Fate Zero")
            # Idempotent: a second pass leaves the decision untouched.
            second = reconcile_root_work_units(
                alist, "/library", state_root, root_task_id,
            )
            self.assertEqual(second[0].reconciliation_outcome, "merge_existing")
            self.assertEqual(second[0].matched_work_root, "/library/番剧/Fate Zero")


if __name__ == "__main__":
    unittest.main()


class TitledMovieNfoTests(unittest.TestCase):
    def test_index_recognises_titled_movie_nfo(self) -> None:
        files = _sample_library()
        files["/library/电影/Big Buck Bunny (2008)/大雄兔 (2008).nfo"] = _nfo_movie(
            10378, "Big Buck Bunny", "2008",
        )
        files["/library/电影/Big Buck Bunny (2008)/大雄兔 (2008).mkv"] = b"v"
        index = build_library_index(IndexAList(files), "/library")
        entries = index.entries_for("movie", 10378)
        self.assertEqual(len(entries), 1)
        self.assertEqual(entries[0].work_root, "/library/电影/Big Buck Bunny (2008)")
        # The duplicate is then provable, not a fresh new_work.
        decision = decide_reconciliation(
            index, media_type="movie", tmdb_id=10378, unit_tokens=frozenset(),
        )
        self.assertEqual(decision.outcome, "duplicate_complete")

    def test_episode_nfo_without_tmdbid_is_ignored(self) -> None:
        files = _sample_library()
        files["/library/番剧/Fate Zero/Season 01/番剧 - S01E01 - 试播.nfo"] = (
            "<episodedetails><title>x</title></episodedetails>"
        ).encode("utf-8")
        index = build_library_index(IndexAList(files), "/library")
        self.assertEqual(len(index.entries_for("tv", 35507)), 1)


class FailedAcceptanceRedecisionTests(unittest.TestCase):
    def test_failed_acceptance_reopens_the_decision(self) -> None:
        files = _sample_library()
        files["/library/电影/Big Buck Bunny (2008)/大雄兔 (2008).nfo"] = _nfo_movie(
            10378, "Big Buck Bunny", "2008",
        )
        files["/library/电影/Big Buck Bunny (2008)/大雄兔 (2008).mkv"] = b"v"
        alist = IndexAList(files)
        import tempfile
        from pathlib import Path
        from local.scrapeflow_api.unit_execution import (
            WorkAcceptanceResult, save_work_acceptance,
        )
        with tempfile.TemporaryDirectory() as directory:
            state_root = Path(directory)
            root_task_id = "root-redo"
            analyze_root_boundaries(
                alist, "/incoming/Big Buck Bunny (2008)",
                root_task_id=root_task_id, state_root=state_root,
            )
            records = load_work_unit_records(state_root, root_task_id)
            apply_work_unit_override(
                state_root, root_task_id, records[0].work_unit_id,
                media_type="movie", tmdb_id=10378,
            )
            # First pass with a library that does not yet contain the movie:
            # the decision is new_work and the write fails.
            empty = IndexAList({})
            first = reconcile_root_work_units(empty, "/library", state_root, root_task_id)
            self.assertEqual(first[0].reconciliation_outcome, "new_work")
            save_work_acceptance(state_root, root_task_id, [
                WorkAcceptanceResult(
                    work_unit_id=records[0].work_unit_id, outcome="failed",
                    writer_job_id=None, phase="failed", target_root="",
                    planned_files=0, error="目标已存在", recorded_at="2026-08-16T00:00:00Z",
                ),
            ])
            # Second pass: the failed acceptance reopens the decision and the
            # fixed index now proves the duplicate.
            second = reconcile_root_work_units(alist, "/library", state_root, root_task_id)
            self.assertEqual(second[0].reconciliation_outcome, "duplicate_complete")


class NestedWorkNfoTests(unittest.TestCase):
    def test_nested_sub_work_nfo_does_not_override_container_identity(self) -> None:
        files = _sample_library()
        # A Fate-style container: the series root plus a nested movie.
        files["/library/番剧/刀剑神域/tvshow.nfo"] = _nfo_tv(45782, "刀剑神域", "2012")
        files["/library/番剧/刀剑神域/Season 01/S01E01.mkv"] = b"v"
        files["/library/番剧/刀剑神域/序列之争 (2017)/序列之争 (2017).nfo"] = _nfo_movie(
            413594, "序列之争", "2017",
        )
        files["/library/番剧/刀剑神域/序列之争 (2017)/序列之争 (2017).mkv"] = b"v"
        index = build_library_index(IndexAList(files), "/library")
        entries = index.entries_for("tv", 45782)
        self.assertEqual(len(entries), 1)
        self.assertEqual(entries[0].work_root, "/library/番剧/刀剑神域")
        self.assertIn("S01E01", entries[0].episode_tokens)
        # The nested movie must not hijack the container identity.
        self.assertEqual(index.entries_for("movie", 413594), ())
