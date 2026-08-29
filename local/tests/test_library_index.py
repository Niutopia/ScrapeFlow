"""Tests for the D-node three-shelf LibraryIndex and five-way reconciliation."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

from engine.scrapeflow.root_boundaries import analyze_root_boundaries
from engine.scrapeflow.unit_identity import apply_work_unit_override
from engine.scrapeflow.work_units import WorkUnitRecord, load_work_unit_records

from local.scrapeflow_api.library_index import (
    SingleSeasonEpisodeProof,
    _fresh_scopes_match_snapshot,
    _merged_multi_season_evidence,
    _single_positive_tmdb_season,
    _title_ordinal_prefix_matches_record,
    build_library_index,
    decide_reconciliation,
    reconcile_root_work_units,
)
from local.scrapeflow_api.tmdb_episode_catalog import TmdbEpisodeCatalog


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
        # Optional per-path provider mtimes.  Absent entries leave rows
        # without ``modified`` (the default read shape); tests that need
        # exact-manifest drift set an entry, then change it.
        self.modified: dict[str, str] = {}
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
                row: dict[str, object] = {
                    "name": remainder,
                    "is_dir": False,
                    "size": len(self.files[full_path]),
                }
                if full_path in self.modified:
                    row["modified"] = self.modified[full_path]
                rows[remainder] = row
        return [rows[name] for name in sorted(rows)]

    def read_file_bytes(self, path: str, *, max_bytes: int | None = None) -> bytes:
        value = self.files.get(path)
        if value is None:
            raise FileNotFoundError(path)
        if max_bytes is not None:
            return value[:max_bytes]
        return value

    def try_list(self, path: str, refresh: bool = False) -> list[dict[str, object]]:
        del refresh
        return self.list(path)

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


class StrictBareEpisodeTMDB:
    """TMDB double exposing both show-detail and published season evidence."""

    def __init__(
        self,
        tmdb_id: int,
        seasons: dict[int, int],
        *,
        payload_counts: dict[int, int] | None = None,
        episode_runtimes: dict[int, list[int | None]] | None = None,
    ) -> None:
        self.tmdb_id = tmdb_id
        self.seasons = dict(seasons)
        self.payload_counts = dict(payload_counts or seasons)
        self.episode_runtimes = dict(episode_runtimes or {})

    def get(self, path: str, **_params: object) -> dict[str, object]:
        if path == f"/tv/{self.tmdb_id}":
            positive = {
                season: count
                for season, count in self.seasons.items()
                if season > 0
            }
            return {
                "name": "One Season Show",
                "original_name": "One Season Show",
                "number_of_seasons": len(positive),
                "number_of_episodes": sum(positive.values()),
                "seasons": [
                    {
                        "season_number": season,
                        "episode_count": count,
                    }
                    for season, count in sorted(self.seasons.items())
                ],
            }
        for season, count in self.payload_counts.items():
            if path == f"/tv/{self.tmdb_id}/season/{season}":
                runtimes = self.episode_runtimes.get(season)
                return {
                    "episodes": [
                        {
                            "episode_number": episode,
                            "air_date": "2020-01-01",
                            "name": f"Episode {episode}",
                            "runtime": (
                                runtimes[episode - 1]
                                if runtimes is not None and episode - 1 < len(runtimes)
                                else 24
                            ),
                        }
                        for episode in range(1, count + 1)
                    ],
                }
        return {}


class SameDayBlockTMDB:
    """TMDB double whose Season 00 holds a same-day multi-episode block."""

    def __init__(self, tmdb_id: int, third_air: str) -> None:
        self.tmdb_id = tmdb_id
        self.third_air = third_air

    def get(self, path: str, **_params: object) -> dict[str, object]:
        if path == f"/tv/{self.tmdb_id}":
            return {
                "name": "示例剧",
                "original_name": "示例剧",
                "number_of_seasons": 2,
                "number_of_episodes": 60,
                "seasons": [
                    {"season_number": 0, "episode_count": 6, "name": "特别篇"},
                    {"season_number": 1, "episode_count": 60, "name": "第 1 季"},
                ],
            }
        if path == f"/tv/{self.tmdb_id}/season/0":
            names = {
                1: "短篇 1",
                2: "VS 不及格",
                3: "特集",
                4: "示例剧：陆 VS 空",
                5: "球之“道”",
                6: "第一季OAD",
            }
            air = {
                1: "2015-03-04",
                2: "2016-05-02",
                3: "2017-08-04",
                4: "2020-01-22",
                5: "2020-01-22",
                6: self.third_air,
            }
            return {
                "episodes": [
                    {
                        "episode_number": number,
                        "air_date": air[number],
                        "name": names[number],
                    }
                    for number in range(1, 7)
                ]
            }
        if path == f"/tv/{self.tmdb_id}/season/1":
            return {
                "episodes": [
                    {
                        "episode_number": number,
                        "air_date": "2014-04-06",
                        "name": f"第{number}集",
                    }
                    for number in range(1, 61)
                ]
            }
        return {}


def _sample_library() -> dict[str, bytes]:
    files: dict[str, bytes] = {}
    files["/library/番剧/Fate Zero/tvshow.nfo"] = _nfo_tv(35507, "Fate/Zero", "2011")
    for episode in range(1, 11):
        files[f"/library/番剧/Fate Zero/Season 01/S01E{episode:02d}.mkv"] = b"v"
    files["/library/电影/Inception (2010)/movie.nfo"] = _nfo_movie(27205, "Inception", "2010")
    files["/library/电影/Inception (2010)/Inception.2010.2160p.mkv"] = b"v"
    files["/library/欧美剧/Breaking Bad/tvshow.nfo"] = _nfo_tv(1396, "Breaking Bad", "2008")
    for episode in range(1, 9):
        files[f"/library/欧美剧/Breaking Bad/Season 01/S01E{episode:02d}.mkv"] = b"v"
    return files


class LibraryIndexTests(unittest.TestCase):
    def setUp(self) -> None:
        self.alist = IndexAList(_sample_library())
        self.index = build_library_index(self.alist, "/library")

    def test_merged_multi_season_evidence_and_proof_roundtrip(self) -> None:
        """A whole-series counter spanning two seasons round-trips its boundary."""
        class TwoSeasonTMDB:
            def get(self, path: str, **_kwargs: object) -> dict[str, object]:
                if path == "/tv/9000":
                    return {
                        "seasons": [
                            {"season_number": 1, "episode_count": 24},
                            {"season_number": 2, "episode_count": 24},
                        ],
                    }
                return {}

        evidence = _merged_multi_season_evidence(
            TwoSeasonTMDB(), tmdb_id=9000, episode_count=48,
        )
        self.assertIsNotNone(evidence)
        assert evidence is not None
        self.assertEqual(evidence.boundaries, ((1, 24), (2, 24)))

        proof = SingleSeasonEpisodeProof(
            tmdb_id=9000,
            season=1,
            episode_count=48,
            episode_tokens=tuple(
                f"S{season:02d}E{episode:02d}"
                for season, count in evidence.boundaries
                for episode in range(1, count + 1)
            ),
            evidence_kind="tmdb_single_positive_season_bracketed_episodes",
            season_boundaries=evidence.boundaries,
        )
        restored = SingleSeasonEpisodeProof.from_dict(proof.as_dict())
        self.assertEqual(restored.season_boundaries, ((1, 24), (2, 24)))
        self.assertEqual(restored.episode_count, 48)

    def test_single_season_unit_of_multi_season_show_matches_exactly(self) -> None:
        """A 13-episode run proves Season 1 of a 13+12 show, not a merge."""
        class TwoSeasonTMDB:
            def get(self, path: str, **_kwargs: object) -> dict[str, object]:
                if path == "/tv/7000":
                    return {
                        "seasons": [
                            {"season_number": 1, "episode_count": 13},
                            {"season_number": 2, "episode_count": 12},
                        ],
                    }
                return {}

        evidence = _single_positive_tmdb_season(
            TwoSeasonTMDB(), tmdb_id=7000, episode_count=13,
        )
        self.assertIsNotNone(evidence)
        assert evidence is not None
        self.assertEqual(evidence.season, 1)
        # A run equal to neither season alone must not match.
        self.assertIsNone(
            _single_positive_tmdb_season(
                TwoSeasonTMDB(), tmdb_id=7000, episode_count=25,
            )
        )

    def test_overflow_tail_lands_in_published_specials(self) -> None:
        """1..14 with 12 regular + 2 specials proves the regular season + S00."""
        class OverflowTMDB:
            def get(self, path: str, **_kwargs: object) -> dict[str, object]:
                if path == "/tv/9000":
                    return {
                        "number_of_episodes": 14,
                        "seasons": [
                            {"season_number": 1, "episode_count": 12},
                            {"season_number": 0, "episode_count": 2},
                        ],
                    }
                return {}

        evidence = _single_positive_tmdb_season(
            OverflowTMDB(), tmdb_id=9000, episode_count=14,
        )
        self.assertIsNotNone(evidence)
        assert evidence is not None
        self.assertEqual(evidence.season, 1)
        self.assertEqual(evidence.regular_episode_count, 12)
        self.assertEqual(evidence.overflow_episode_count, 2)
        # A mismatch between the overflow tail and the specials count fails closed.
        class MismatchTMDB:
            def get(self, path: str, **_kwargs: object) -> dict[str, object]:
                if path == "/tv/9001":
                    return {
                        "number_of_episodes": 14,
                        "seasons": [
                            {"season_number": 1, "episode_count": 11},
                            {"season_number": 0, "episode_count": 2},
                        ],
                    }
                return {}

        self.assertIsNone(
            _single_positive_tmdb_season(
                MismatchTMDB(), tmdb_id=9001, episode_count=14,
            )
        )

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

    def test_index_uses_the_real_season_directory_for_bare_episode_names(self) -> None:
        files = _sample_library()
        files["/library/欧美剧/Path Context/tvshow.nfo"] = _nfo_tv(
            99001, "Path Context", "2020",
        )
        files["/library/欧美剧/Path Context/Season 02/Path.Context.E01.mkv"] = b"v"

        index = build_library_index(IndexAList(files), "/library")

        entry = index.entries_for("tv", 99001)
        self.assertEqual(len(entry), 1)
        self.assertEqual(entry[0].episode_tokens, {"S02E01"})

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

    def test_unqualified_tv_episode_is_uncertain_until_a_season_is_proven(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            state_root = Path(directory)
            root_task_id = "root-unqualified"
            files = _sample_library()
            files["/incoming/Hall/Hall.E01.mkv"] = b"v"
            alist = IndexAList(files)
            analyze_root_boundaries(
                alist, "/incoming/Hall",
                root_task_id=root_task_id, state_root=state_root,
            )
            record = load_work_unit_records(state_root, root_task_id)[0]
            apply_work_unit_override(
                state_root, root_task_id, record.work_unit_id,
                media_type="tv", tmdb_id=35507,
            )

            uncertain = reconcile_root_work_units(
                alist, "/library", state_root, root_task_id,
            )
            self.assertEqual(uncertain[0].reconciliation_outcome, "uncertain")
            self.assertIn("裸 E", uncertain[0].attention or "")

            # An explicit operator season is evidence-backed and can safely
            # turn the same bare E01 into an ordinary duplicate check.
            apply_work_unit_override(
                state_root, root_task_id, record.work_unit_id,
                media_type="tv", tmdb_id=35507, season=1,
            )
            confirmed = reconcile_root_work_units(
                alist, "/library", state_root, root_task_id,
            )
            self.assertEqual(confirmed[0].reconciliation_outcome, "duplicate_complete")

    def _reconcile_bare_episode_source(
        self,
        names: list[str],
        tmdb: StrictBareEpisodeTMDB,
        *,
        state_root: Path,
        root_task_id: str,
    ):
        files = {
            f"/incoming/One Season Show/{name}": b"v"
            for name in names
        }
        alist = IndexAList(files)
        analyze_root_boundaries(
            alist,
            "/incoming/One Season Show",
            root_task_id=root_task_id,
            state_root=state_root,
        )
        records = load_work_unit_records(state_root, root_task_id)
        self.assertEqual(len(records), 1)
        apply_work_unit_override(
            state_root,
            root_task_id,
            records[0].work_unit_id,
            media_type="tv",
            tmdb_id=tmdb.tmdb_id,
        )
        reconciled = reconcile_root_work_units(
            alist,
            "/library",
            state_root,
            root_task_id,
            episode_catalog=TmdbEpisodeCatalog(tmdb),
            tmdb_client=tmdb,
        )
        return alist, state_root, reconciled[0]

    def test_complete_bare_e_source_gets_a_revalidatable_single_season_proof(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            state_root = Path(directory)
            tmdb = StrictBareEpisodeTMDB(99003, {1: 6})
            _alist, state_root, record = self._reconcile_bare_episode_source(
                [f"One.Season.Show.E{episode:02d}.mkv" for episode in range(1, 7)],
                tmdb,
                state_root=state_root,
                root_task_id="root-bare-complete",
            )
            self.assertEqual(record.reconciliation_outcome, "new_work")
            self.assertEqual(
                record.reconciliation_evidence,
                {
                    "kind": "tmdb_single_positive_season_bare_episodes",
                    "tmdb_id": 99003,
                    "season": 1,
                    "episode_count": 6,
                    "episode_tokens": [f"S01E{episode:02d}" for episode in range(1, 7)],
                },
            )
            # The D evidence is durable but deliberately separate from the
            # operator's identity confirmation.
            persisted = load_work_unit_records(state_root, "root-bare-complete")[0]
            self.assertEqual(persisted.reconciliation_evidence, record.reconciliation_evidence)
            self.assertNotEqual(persisted.identity.get("source"), "derived_bare_episode")

    def test_partial_season_prefix_source_gets_a_revalidatable_proof(self) -> None:
        """A contiguous ``1..N`` run shorter than the sole positive season is a
        partial-season prefix (the still-airing tail stays a J gap), not an
        uncertain run."""
        with tempfile.TemporaryDirectory() as directory:
            state_root = Path(directory)
            tmdb = StrictBareEpisodeTMDB(99006, {1: 8})
            _alist, _state_root, record = self._reconcile_bare_episode_source(
                [f"One.Season.Show.E{episode:02d}.mkv" for episode in range(1, 7)],
                tmdb,
                state_root=state_root,
                root_task_id="root-bare-partial",
            )
            self.assertEqual(record.reconciliation_outcome, "new_work")
            self.assertEqual(
                record.reconciliation_evidence,
                {
                    "kind": "tmdb_single_positive_season_bare_episodes",
                    "tmdb_id": 99006,
                    "season": 1,
                    "episode_count": 6,
                    "episode_tokens": [f"S01E{episode:02d}" for episode in range(1, 7)],
                },
            )

    def test_partial_prefix_of_first_season_survives_a_shorter_later_season(self) -> None:
        """A ``1..N`` run is the first season's prefix when it is the only
        season long enough to host it (犬夜叉 001-100 of 167 with a 26-episode
        sequel season)."""
        with tempfile.TemporaryDirectory() as directory:
            state_root = Path(directory)
            tmdb = StrictBareEpisodeTMDB(99007, {1: 8, 2: 4})
            _alist, _state_root, record = self._reconcile_bare_episode_source(
                [f"One.Season.Show.E{episode:02d}.mkv" for episode in range(1, 7)],
                tmdb,
                state_root=state_root,
                root_task_id="root-bare-partial-multiseason",
            )
            self.assertEqual(record.reconciliation_outcome, "new_work")
            self.assertEqual(
                record.reconciliation_evidence,
                {
                    "kind": "tmdb_single_positive_season_bare_episodes",
                    "tmdb_id": 99007,
                    "season": 1,
                    "episode_count": 6,
                    "episode_tokens": [f"S01E{episode:02d}" for episode in range(1, 7)],
                },
            )

    def test_partial_prefix_stays_uncertain_when_two_seasons_could_host_it(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            state_root = Path(directory)
            tmdb = StrictBareEpisodeTMDB(99008, {1: 8, 2: 9})
            _alist, _state_root, record = self._reconcile_bare_episode_source(
                [f"One.Season.Show.E{episode:02d}.mkv" for episode in range(1, 7)],
                tmdb,
                state_root=state_root,
                root_task_id="root-bare-partial-ambiguous",
            )
            self.assertEqual(record.reconciliation_outcome, "uncertain")
            self.assertIn("裸 E", record.attention or "")

    def test_partial_prefix_never_lands_on_a_later_season(self) -> None:
        """A run that crossed the first season in absolute order could be an
        absolute batch (first season complete plus more), so a later-season
        prefix reading stays unproven."""
        with tempfile.TemporaryDirectory() as directory:
            state_root = Path(directory)
            tmdb = StrictBareEpisodeTMDB(99009, {1: 4, 2: 9})
            _alist, _state_root, record = self._reconcile_bare_episode_source(
                [f"One.Season.Show.E{episode:02d}.mkv" for episode in range(1, 7)],
                tmdb,
                state_root=state_root,
                root_task_id="root-bare-partial-later-season",
            )
            self.assertEqual(record.reconciliation_outcome, "uncertain")
            self.assertIn("裸 E", record.attention or "")

    def test_quoted_ordinal_disc_rip_run_gets_single_season_proof(self) -> None:
        """``01「…」``…``26「…」`` disc rips prove their TMDB season.

        Japanese disc rips lead with the episode ordinal and quote the episode
        title (犬夜叉完结篇 26 files against a 167-episode first season).  The
        run length identifies the second season uniquely; D must persist the
        same revalidatable receipt the other unqualified grammars produce.
        """
        with tempfile.TemporaryDirectory() as directory:
            state_root = Path(directory)
            tmdb = StrictBareEpisodeTMDB(99060, {1: 167, 2: 26})
            names = [
                f"{episode:02d}「第{episode}話」.mkv" for episode in range(1, 27)
            ]
            _alist, _state_root, record = self._reconcile_bare_episode_source(
                names,
                tmdb,
                state_root=state_root,
                root_task_id="root-quoted-ordinal-complete",
            )
            self.assertEqual(record.reconciliation_outcome, "new_work")
            self.assertEqual(
                record.reconciliation_evidence,
                {
                    "kind": "tmdb_single_positive_season_quoted_ordinal",
                    "tmdb_id": 99060,
                    "season": 2,
                    "episode_count": 26,
                    "episode_tokens": [
                        f"S02E{episode:02d}" for episode in range(1, 27)
                    ],
                },
            )

    def test_quoted_ordinal_run_mixed_with_bracket_ordinal_stays_uncertain(self) -> None:
        """One bracket ordinal mixed into a quoted run is two grammars."""
        with tempfile.TemporaryDirectory() as directory:
            state_root = Path(directory)
            tmdb = StrictBareEpisodeTMDB(99061, {1: 26})
            names = [
                f"{episode:02d}「第{episode}話」.mkv"
                for episode in range(1, 26)
            ] + ["Example Show [26].mkv"]
            _alist, _state_root, record = self._reconcile_bare_episode_source(
                names,
                tmdb,
                state_root=state_root,
                root_task_id="root-quoted-ordinal-mixed",
            )
            self.assertEqual(record.reconciliation_outcome, "uncertain")
            self.assertIn("混合了不同的无季号集号格式", record.attention or "")

    def test_complete_naked_numeric_source_gets_a_revalidatable_single_season_proof(self) -> None:
        """A clean ``01.mp4`` … ``N.mp4`` run is proven only by D/TMDB.

        The numeric stems are deliberately title-free: the source title is
        still supplied to C by the boundary, while D proves the season and
        episode coordinates against a single positive TMDB season.  This is
        the generic path used by bare-number anime releases; it is not a
        work-specific exception.
        """
        with tempfile.TemporaryDirectory() as directory:
            state_root = Path(directory)
            tmdb = StrictBareEpisodeTMDB(99004, {1: 6})
            _alist, _state_root, record = self._reconcile_bare_episode_source(
                [f"{episode:02d}.mp4" for episode in range(1, 7)],
                tmdb,
                state_root=state_root,
                root_task_id="root-naked-numeric-complete",
            )
            self.assertEqual(record.reconciliation_outcome, "new_work")
            self.assertEqual(
                record.reconciliation_evidence,
                {
                    "kind": "tmdb_single_positive_season_naked_numeric",
                    "tmdb_id": 99004,
                    "season": 1,
                    "episode_count": 6,
                    "episode_tokens": [
                        f"S01E{episode:02d}" for episode in range(1, 7)
                    ],
                },
            )

    def test_complete_release_dash_source_gets_a_revalidatable_single_season_proof(self) -> None:
        """A homogeneous release-style ``Title - 01`` run is D-proven once.

        This mirrors the source shape that has no explicit season or ``E``
        marker.  The shared proof has to establish the one title prefix,
        contiguous ordinal set, sole positive TMDB season, and exact catalog
        before D may create ordinary S01 coordinates.
        """
        with tempfile.TemporaryDirectory() as directory:
            state_root = Path(directory)
            tmdb = StrictBareEpisodeTMDB(99060, {1: 6})
            names = [
                (
                    "[LoliHouse] Akuyaku Reijou Level 99 "
                    f"- {episode:02d} [WebRip 1080p HEVC-10bit AAC SRTx2].mkv"
                )
                for episode in range(1, 7)
            ]
            _alist, _state_root, record = self._reconcile_bare_episode_source(
                names,
                tmdb,
                state_root=state_root,
                root_task_id="root-release-dash-complete",
            )
            self.assertEqual(record.reconciliation_outcome, "new_work")
            self.assertEqual(
                record.reconciliation_evidence,
                {
                    "kind": "tmdb_single_positive_season_release_dash_episodes",
                    "tmdb_id": 99060,
                    "season": 1,
                    "episode_count": 6,
                    "episode_tokens": [
                        f"S01E{episode:02d}" for episode in range(1, 7)
                    ],
                },
            )

    def test_complete_title_ordinal_source_gets_a_revalidatable_single_season_proof(self) -> None:
        """A homogeneous ``Title 01`` run is a bounded D/F grammar."""
        with tempfile.TemporaryDirectory() as directory:
            state_root = Path(directory)
            tmdb = StrictBareEpisodeTMDB(99090, {1: 6})
            names = [
                (
                    "[4K_EA] One Season Show "
                    f"{episode:02d} [简体内嵌][WebRip].mkv"
                )
                for episode in range(1, 7)
            ]
            _alist, _state_root, record = self._reconcile_bare_episode_source(
                names,
                tmdb,
                state_root=state_root,
                root_task_id="root-title-ordinal-complete",
            )
            self.assertEqual(record.reconciliation_outcome, "new_work")
            self.assertEqual(
                (record.reconciliation_evidence or {}).get("kind"),
                "tmdb_single_positive_season_title_ordinal_episodes",
            )
            self.assertEqual(
                (record.reconciliation_evidence or {}).get("episode_tokens"),
                [f"S01E{episode:02d}" for episode in range(1, 7)],
            )

    def test_title_ordinal_proof_fails_closed_for_competing_shape(self) -> None:
        cases = (
            (
                "different-prefix",
                ["[4K] One Season Show 01 [WebRip].mkv", "[4K] Other Show 02 [WebRip].mkv"],
            ),
            (
                "duplicate-ordinal",
                ["[4K] One Season Show 01 [WebRip].mkv", "[4K] One Season Show 01 [v2].mkv"],
            ),
            (
                "non-contiguous",
                ["[4K] One Season Show 01 [WebRip].mkv", "[4K] One Season Show 03 [WebRip].mkv"],
            ),
            (
                "extra-title-number",
                ["[4K] One Season Show 2 01 [WebRip].mkv", "[4K] One Season Show 2 02 [WebRip].mkv"],
            ),
            (
                "leading-ordinal-tag",
                ["[01] One Season Show 01 [WebRip].mkv", "[01] One Season Show 02 [WebRip].mkv"],
            ),
            (
                "leading-year-tag-with-whitespace",
                [
                    " [2024] One Season Show 01 [WebRip].mkv",
                    " [2024] One Season Show 02 [WebRip].mkv",
                ],
            ),
            (
                "trailing-year-tag",
                [
                    "[4K] One Season Show 01 [2024].mkv",
                    "[4K] One Season Show 02 [2024].mkv",
                ],
            ),
            (
                "special-tail",
                ["[4K] One Season Show 01 [WebRip].mkv", "[4K] One Season Show 02 [OVA].mkv"],
            ),
            (
                "extra-video",
                ["[4K] One Season Show 01 [WebRip].mkv", "[4K] One Season Show 02 [WebRip].mkv", "trailer.mkv"],
            ),
        )
        with tempfile.TemporaryDirectory() as directory:
            state_root = Path(directory)
            for index, (label, names) in enumerate(cases):
                with self.subTest(label=label):
                    _alist, _state_root, record = self._reconcile_bare_episode_source(
                        names,
                        StrictBareEpisodeTMDB(99100 + index, {1: 2}),
                        state_root=state_root,
                        root_task_id=f"root-title-ordinal-reject-{index}",
                    )
                    self.assertEqual(record.reconciliation_outcome, "uncertain")
                    self.assertIsNone(record.reconciliation_evidence)

    def test_title_ordinal_identity_prefix_requires_an_exact_automatic_alias(self) -> None:
        automatic = SimpleNamespace(
            display_label="One Season Show",
            identity={
                "source": "tmdb",
                "title": "One Season Show",
                "decision_trace": {
                    "official_titles": ["One Season Show"],
                    "aliases_checked": ["One Season Show (2024)"],
                },
            },
        )
        self.assertTrue(
            _title_ordinal_prefix_matches_record("one season show", automatic),
        )
        self.assertFalse(
            _title_ordinal_prefix_matches_record(
                "another one season show", automatic,
            ),
        )
        # A malformed scalar alias list must not be iterated character by
        # character and accidentally validate a one-letter title prefix.
        malformed = SimpleNamespace(
            display_label="",
            identity={
                "source": "tmdb",
                "decision_trace": {"official_titles": "A"},
            },
        )
        self.assertFalse(_title_ordinal_prefix_matches_record("a", malformed))

    def test_release_dash_proof_fails_closed_for_prefix_special_video_or_catalog_drift(self) -> None:
        cases = (
            (
                "different-title-prefix",
                [
                    "[Group] Example Show - 01 [WebRip].mkv",
                    "[Group] Other Show - 02 [WebRip].mkv",
                ],
                StrictBareEpisodeTMDB(99061, {1: 2}),
            ),
            (
                "duplicate-ordinal",
                [
                    "[Group] Example Show - 01 [WebRip].mkv",
                    "[Group] Example Show - 01 [WebRip][v2].mkv",
                ],
                StrictBareEpisodeTMDB(99062, {1: 2}),
            ),
            (
                "special-video",
                [
                    "[Group] Example Show - 01 [WebRip].mkv",
                    "[Group] Example Show - 02 [OVA].mkv",
                ],
                StrictBareEpisodeTMDB(99063, {1: 2}),
            ),
            (
                "mv-video",
                [
                    "[Group] Example Show - 01 [WebRip].mkv",
                    "[Group] Example Show - 02 [MV].mkv",
                ],
                StrictBareEpisodeTMDB(99067, {1: 2}),
            ),
            (
                "tail-range",
                [
                    "[Group] Example Show - 01 [WebRip].mkv",
                    "[Group] Example Show - 02 [01-02].mkv",
                ],
                StrictBareEpisodeTMDB(99068, {1: 2}),
            ),
            (
                "tail-year-tag",
                [
                    "[Group] Example Show - 01 [WebRip].mkv",
                    "[Group] Example Show - 02 [ 2024 ].mkv",
                ],
                StrictBareEpisodeTMDB(99069, {1: 2}),
            ),
            (
                "tail-long-range",
                [
                    "[Group] Example Show - 01 [WebRip].mkv",
                    "[Group] Example Show - 02 [2024-2025].mkv",
                ],
                StrictBareEpisodeTMDB(99070, {1: 2}),
            ),
            (
                "other-video",
                [
                    "[Group] Example Show - 01 [WebRip].mkv",
                    "[Group] Example Show - 02 [WebRip].mkv",
                    "trailer.mkv",
                ],
                StrictBareEpisodeTMDB(99064, {1: 2}),
            ),
            (
                "multiple-positive-seasons",
                [
                    "[Group] Example Show - 01 [WebRip].mkv",
                    "[Group] Example Show - 02 [WebRip].mkv",
                ],
                StrictBareEpisodeTMDB(99065, {1: 2, 2: 2}),
            ),
            (
                "catalog-drift",
                [
                    "[Group] Example Show - 01 [WebRip].mkv",
                    "[Group] Example Show - 02 [WebRip].mkv",
                ],
                StrictBareEpisodeTMDB(
                    99066, {1: 2}, payload_counts={1: 1},
                ),
            ),
        )
        with tempfile.TemporaryDirectory() as directory:
            state_root = Path(directory)
            for index, (label, names, tmdb) in enumerate(cases):
                with self.subTest(label=label):
                    _alist, _state_root, record = self._reconcile_bare_episode_source(
                        names,
                        tmdb,
                        state_root=state_root,
                        root_task_id=f"root-release-dash-reject-{index}",
                    )
                    self.assertEqual(record.reconciliation_outcome, "uncertain")
                    self.assertIsNone(record.reconciliation_evidence)
                    self.assertIn("发行组短横线集号", record.attention or "")

    def test_release_dash_non_story_tail_or_explicit_e_coordinate_is_uncertain(self) -> None:
        """D must not turn presentation tags or a second coordinate into S01."""
        tails = ("[CM]", "[OP2]", "[ED]", "[MENU]", "[E02]", "[EP 02]")
        with tempfile.TemporaryDirectory() as directory:
            state_root = Path(directory)
            for index, tail in enumerate(tails):
                with self.subTest(tail=tail):
                    _alist, _state_root, record = self._reconcile_bare_episode_source(
                        [
                            "[Group] Example Show - 01 [WebRip].mkv",
                            f"[Group] Example Show - 02 {tail}.mkv",
                        ],
                        StrictBareEpisodeTMDB(99080 + index, {1: 2}),
                        state_root=state_root,
                        root_task_id=f"root-release-dash-tail-{index}",
                    )
                    self.assertEqual(record.reconciliation_outcome, "uncertain")
                    self.assertIsNone(record.reconciliation_evidence)
                    self.assertTrue(record.attention)

    def test_complete_physical_oad_run_gets_a_revalidatable_regular_season_proof(self) -> None:
        """D maps only a separately proven OAD work, never parent Season 00."""
        class OadTMDB(StrictBareEpisodeTMDB):
            def get(self, path: str, **_params: object) -> dict[str, object]:
                if path == f"/tv/{self.tmdb_id}":
                    return {
                        "name": "Example OAD",
                        "original_name": "Example OAD",
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

        with tempfile.TemporaryDirectory() as directory:
            state_root = Path(directory)
            root_task_id = "root-physical-oad"
            tmdb = OadTMDB(99050, {1: 5})
            files = {
                f"/incoming/Example OAD/Example [OAD{episode:02d}].mkv": b"v"
                for episode in range(1, 6)
            }
            alist = IndexAList(files)
            analyze_root_boundaries(
                alist, "/incoming/Example OAD",
                root_task_id=root_task_id, state_root=state_root,
            )
            record = load_work_unit_records(state_root, root_task_id)[0]
            apply_work_unit_override(
                state_root, root_task_id, record.work_unit_id,
                media_type="tv", tmdb_id=tmdb.tmdb_id,
            )
            record = reconcile_root_work_units(
                alist, "/library", state_root, root_task_id,
                episode_catalog=TmdbEpisodeCatalog(tmdb), tmdb_client=tmdb,
            )[0]
            self.assertEqual(record.reconciliation_outcome, "new_work")
            self.assertEqual(
                record.reconciliation_evidence,
                {
                    "kind": "tmdb_single_positive_season_physical_special",
                    "tmdb_id": 99050,
                    "season": 1,
                    "episode_count": 5,
                    "episode_tokens": [f"S01E{episode:02d}" for episode in range(1, 6)],
                },
            )

    def test_physical_oad_run_rejects_same_count_without_official_marker(self) -> None:
        """The D/F proof cannot be granted from count equality alone."""
        with tempfile.TemporaryDirectory() as directory:
            state_root = Path(directory)
            root_task_id = "root-physical-oad-no-marker"
            tmdb = StrictBareEpisodeTMDB(99051, {1: 5})
            files = {
                f"/incoming/Example OAD/Example [OAD{episode:02d}].mkv": b"v"
                for episode in range(1, 6)
            }
            alist = IndexAList(files)
            analyze_root_boundaries(
                alist, "/incoming/Example OAD",
                root_task_id=root_task_id, state_root=state_root,
            )
            record = load_work_unit_records(state_root, root_task_id)[0]
            apply_work_unit_override(
                state_root, root_task_id, record.work_unit_id,
                media_type="tv", tmdb_id=tmdb.tmdb_id,
            )
            record = reconcile_root_work_units(
                alist, "/library", state_root, root_task_id,
                episode_catalog=TmdbEpisodeCatalog(tmdb), tmdb_client=tmdb,
            )[0]
            self.assertEqual(record.reconciliation_outcome, "uncertain")
            self.assertIsNone(record.reconciliation_evidence)

    def test_named_arc_oad_run_maps_onto_parent_season00_window(self) -> None:
        """A named-arc OAD run maps onto the parent's official S00 window.

        ``示例剧 爱染香篇 OAD 2016 [01][02]`` is officially catalogued as
        part-titled Season 00 episodes E08/E09 of the multi-season parent
        show.  D must prove that named window (not 1-based guessing) so the
        release-local ordinals land on ``S00E08``/``S00E09``.
        """
        class ParentShowTMDB:
            def __init__(self, tmdb_id: int) -> None:
                self.tmdb_id = tmdb_id

            def get(self, path: str, **_params: object) -> dict[str, object]:
                if path == f"/tv/{self.tmdb_id}":
                    return {
                        "name": "示例剧",
                        "original_name": "示例剧",
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
                    air = {
                        8: "2016-05-13",
                        9: "2016-06-10",
                    }
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

        with tempfile.TemporaryDirectory() as directory:
            state_root = Path(directory)
            root_task_id = "root-named-arc-oad"
            tmdb = ParentShowTMDB(99052)
            files = {
                "/incoming/示例剧 爱染香篇/示例剧 OAD 2016 [01].mkv": b"v",
                "/incoming/示例剧 爱染香篇/示例剧 OAD 2016 [02].mkv": b"v",
            }
            alist = IndexAList(files)
            analyze_root_boundaries(
                alist, "/incoming/示例剧 爱染香篇",
                root_task_id=root_task_id, state_root=state_root,
            )
            record = load_work_unit_records(state_root, root_task_id)[0]
            apply_work_unit_override(
                state_root, root_task_id, record.work_unit_id,
                media_type="tv", tmdb_id=tmdb.tmdb_id,
            )
            record = reconcile_root_work_units(
                alist, "/library", state_root, root_task_id,
                episode_catalog=TmdbEpisodeCatalog(tmdb), tmdb_client=tmdb,
            )[0]
            self.assertEqual(record.reconciliation_outcome, "new_work")
            self.assertEqual(
                record.reconciliation_evidence,
                {
                    "kind": "tmdb_single_positive_season_physical_special",
                    "tmdb_id": 99052,
                    "season": 0,
                    "episode_count": 2,
                    "episode_tokens": ["S00E08", "S00E09"],
                },
            )

    def test_named_arc_oad_run_rejects_mismatched_release_year(self) -> None:
        """A named window aired outside the source year window fails closed."""

        class DatedParentTMDB:
            def __init__(self, tmdb_id: int, year: str) -> None:
                self.tmdb_id = tmdb_id
                self.year = year

            def get(self, path: str, **_params: object) -> dict[str, object]:
                if path == f"/tv/{self.tmdb_id}":
                    return {
                        "name": "示例剧",
                        "original_name": "示例剧",
                        "seasons": [
                            {"season_number": 0, "episode_count": 3, "name": "特别篇"},
                            {"season_number": 1, "episode_count": 12, "name": "第 1 季"},
                        ],
                    }
                if path == f"/tv/{self.tmdb_id}/season/0":
                    return {
                        "episodes": [
                            {"episode_number": 1, "air_date": "2010-01-01", "name": "短篇"},
                            {
                                "episode_number": 2,
                                "air_date": self.year,
                                "name": "示例剧 爱染香篇 前篇",
                            },
                            {
                                "episode_number": 3,
                                "air_date": self.year,
                                "name": "示例剧 爱染香篇 后篇",
                            },
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
                            for number in range(1, 13)
                        ]
                    }
                return {}

        with tempfile.TemporaryDirectory() as directory:
            state_root = Path(directory)
            root_task_id = "root-named-arc-oad-year"
            tmdb = DatedParentTMDB(99053, "2019-01-01")
            files = {
                "/incoming/示例剧 爱染香篇/示例剧 OAD 2016 [01].mkv": b"v",
                "/incoming/示例剧 爱染香篇/示例剧 OAD 2016 [02].mkv": b"v",
            }
            alist = IndexAList(files)
            analyze_root_boundaries(
                alist, "/incoming/示例剧 爱染香篇",
                root_task_id=root_task_id, state_root=state_root,
            )
            record = load_work_unit_records(state_root, root_task_id)[0]
            apply_work_unit_override(
                state_root, root_task_id, record.work_unit_id,
                media_type="tv", tmdb_id=tmdb.tmdb_id,
            )
            record = reconcile_root_work_units(
                alist, "/library", state_root, root_task_id,
                episode_catalog=TmdbEpisodeCatalog(tmdb), tmdb_client=tmdb,
            )[0]
            self.assertEqual(record.reconciliation_outcome, "uncertain")
            self.assertIsNone(record.reconciliation_evidence)

    def test_titled_release_run_maps_onto_same_day_season00_block(self) -> None:
        """A titled multi-part special run maps onto its same-day S00 block.

        ``OVA 示例剧 陆 VS 空`` ships as ``OVA 01``/``OVA 02`` while the parent
        show's Season 00 holds the arc as consecutive episodes that all aired
        on the same day (``E04 陆 VS 空``/``E05 球之"道"``), with only the
        block's head carrying the arc title.  D must anchor the label onto the
        head episode and let the exact same-day block size position the run.
        """
        with tempfile.TemporaryDirectory() as directory:
            state_root = Path(directory)
            root_task_id = "root-same-day-block"
            tmdb = SameDayBlockTMDB(99054, "2014-09-25")
            files = {
                "/incoming/OVA 示例剧 陆 VS 空/示例剧 OVA 01.mkv": b"v",
                "/incoming/OVA 示例剧 陆 VS 空/示例剧 OVA 02.mkv": b"v",
            }
            alist = IndexAList(files)
            analyze_root_boundaries(
                alist, "/incoming/OVA 示例剧 陆 VS 空",
                root_task_id=root_task_id, state_root=state_root,
            )
            record = load_work_unit_records(state_root, root_task_id)[0]
            apply_work_unit_override(
                state_root, root_task_id, record.work_unit_id,
                media_type="tv", tmdb_id=tmdb.tmdb_id,
            )
            record = reconcile_root_work_units(
                alist, "/library", state_root, root_task_id,
                episode_catalog=TmdbEpisodeCatalog(tmdb), tmdb_client=tmdb,
            )[0]
            self.assertEqual(record.reconciliation_outcome, "new_work")
            self.assertEqual(
                record.reconciliation_evidence,
                {
                    "kind": "tmdb_single_positive_season_physical_special",
                    "tmdb_id": 99054,
                    "season": 0,
                    "episode_count": 2,
                    "episode_tokens": ["S00E04", "S00E05"],
                },
            )

    def test_titled_release_run_rejects_oversized_same_day_block(self) -> None:
        """A same-day block larger than the source run stays unproven.

        ``OVA 01``/``OVA 02`` against a same-day official block of three
        episodes is a subset guess, not a proof: which two of the three the
        release covers cannot be decided from release ordinals alone.
        """
        with tempfile.TemporaryDirectory() as directory:
            state_root = Path(directory)
            root_task_id = "root-same-day-block-oversized"
            tmdb = SameDayBlockTMDB(99055, "2020-01-22")
            files = {
                "/incoming/OVA 示例剧 陆 VS 空/示例剧 OVA 01.mkv": b"v",
                "/incoming/OVA 示例剧 陆 VS 空/示例剧 OVA 02.mkv": b"v",
            }
            alist = IndexAList(files)
            analyze_root_boundaries(
                alist, "/incoming/OVA 示例剧 陆 VS 空",
                root_task_id=root_task_id, state_root=state_root,
            )
            record = load_work_unit_records(state_root, root_task_id)[0]
            apply_work_unit_override(
                state_root, root_task_id, record.work_unit_id,
                media_type="tv", tmdb_id=tmdb.tmdb_id,
            )
            record = reconcile_root_work_units(
                alist, "/library", state_root, root_task_id,
                episode_catalog=TmdbEpisodeCatalog(tmdb), tmdb_client=tmdb,
            )[0]
            self.assertEqual(record.reconciliation_outcome, "uncertain")
            self.assertIsNone(record.reconciliation_evidence)
        """A season-confirmed same-marker OVA run proves its S00 coordinates.

        ``W 4k 某剧 第三季OVA`` holds ``[12(OVA)]``/``[13(OVA)]`` with no
        episode grammar, but the operator-confirmed season 3 plus the official
        timeline (two S00 slots inside season 3's window) fixes the token set.
        D must reuse the F pairing rule instead of staying uncertain, and must
        not persist a Season 00 proof receipt against the season-3 identity.
        """

        class SeasonScopedTMDB:
            def __init__(self, tmdb_id: int) -> None:
                self.tmdb_id = tmdb_id

            def get(self, path: str, **_params: object) -> dict[str, object]:
                if path == f"/tv/{self.tmdb_id}":
                    return {
                        "name": "示例剧",
                        "original_name": "示例剧",
                        "seasons": [
                            {"season_number": 0, "episode_count": 4, "name": "特别篇"},
                            {"season_number": 1, "episode_count": 10, "name": "第 1 季"},
                            {"season_number": 2, "episode_count": 10, "name": "第 2 季"},
                            {"season_number": 3, "episode_count": 11, "name": "第 3 季"},
                        ],
                    }
                if path == f"/tv/{self.tmdb_id}/season/0":
                    return {
                        "episodes": [
                            {"episode_number": 1, "air_date": "2016-06-24", "name": "OAD1"},
                            {"episode_number": 2, "air_date": "2017-07-24", "name": "OAD2"},
                            {"episode_number": 3, "air_date": "2025-04-25", "name": "OVA1"},
                            {"episode_number": 4, "air_date": "2025-04-25", "name": "OVA2"},
                        ]
                    }
                if path == f"/tv/{self.tmdb_id}/season/3":
                    return {
                        "episodes": [
                            {
                                "episode_number": number,
                                "air_date": "2024-04-01" if number == 1 else "2024-04-08",
                                "name": f"第{number}集",
                            }
                            for number in range(1, 12)
                        ]
                    }
                if path.startswith(f"/tv/{self.tmdb_id}/season/"):
                    return {"episodes": []}
                return {}

        def library_files() -> dict[str, bytes]:
            files: dict[str, bytes] = {
                "/library/番剧/示例剧/tvshow.nfo": _nfo_tv(99054, "示例剧", "2016"),
            }
            for season in (1, 2, 3):
                for episode in range(1, 11 if season == 3 else 10 + 1):
                    files[
                        f"/library/番剧/示例剧/Season {season:02d}/S{season:02d}E{episode:02d}.mkv"
                    ] = b"v"
            return files

        with tempfile.TemporaryDirectory() as directory:
            state_root = Path(directory)
            root_task_id = "root-season-scoped-ova"
            tmdb = SeasonScopedTMDB(99054)
            alist = IndexAList(library_files() | {
                "/incoming/W 4k 某剧 第三季OVA/[Grp] 某剧 [12(OVA)][2160p].mkv": b"v",
                "/incoming/W 4k 某剧 第三季OVA/[Grp] 某剧 [13(OVA)][2160p].mkv": b"v",
            })
            analyze_root_boundaries(
                alist, "/incoming/W 4k 某剧 第三季OVA",
                root_task_id=root_task_id, state_root=state_root,
            )
            record = load_work_unit_records(state_root, root_task_id)[0]
            apply_work_unit_override(
                state_root, root_task_id, record.work_unit_id,
                media_type="tv", tmdb_id=tmdb.tmdb_id, season=3,
            )
            record = reconcile_root_work_units(
                alist, "/library", state_root, root_task_id,
                episode_catalog=TmdbEpisodeCatalog(tmdb), tmdb_client=tmdb,
            )[0]
            self.assertEqual(record.reconciliation_outcome, "merge_existing")
            self.assertIsNone(record.reconciliation_evidence)
            self.assertEqual(record.attention, None)

    def test_season_scoped_ova_run_fails_closed_on_window_mismatch(self) -> None:
        """A run longer than the official window keeps its manual surface."""

        class MismatchedWindowTMDB:
            def __init__(self, tmdb_id: int) -> None:
                self.tmdb_id = tmdb_id

            def get(self, path: str, **_params: object) -> dict[str, object]:
                if path == f"/tv/{self.tmdb_id}":
                    return {
                        "name": "示例剧",
                        "seasons": [
                            {"season_number": 0, "episode_count": 3, "name": "特别篇"},
                            {"season_number": 1, "episode_count": 10, "name": "第 1 季"},
                        ],
                    }
                if path == f"/tv/{self.tmdb_id}/season/0":
                    return {
                        "episodes": [
                            {"episode_number": 1, "air_date": "2016-06-24", "name": "OAD1"},
                            {"episode_number": 2, "air_date": "2017-07-24", "name": "OAD2"},
                        ]
                    }
                if path == f"/tv/{self.tmdb_id}/season/1":
                    return {
                        "episodes": [
                            {
                                "episode_number": number,
                                "air_date": "2020-01-01",
                                "name": f"第{number}集",
                            }
                            for number in range(1, 11)
                        ]
                    }
                return {}

        with tempfile.TemporaryDirectory() as directory:
            state_root = Path(directory)
            root_task_id = "root-season-scoped-ova-mismatch"
            tmdb = MismatchedWindowTMDB(99055)
            alist = IndexAList({
                "/incoming/W 4k 某剧 第一季OVA/[Grp] 某剧 [11(OVA)].mkv": b"v",
                "/incoming/W 4k 某剧 第一季OVA/[Grp] 某剧 [12(OVA)].mkv": b"v",
                "/incoming/W 4k 某剧 第一季OVA/[Grp] 某剧 [13(OVA)].mkv": b"v",
            })
            analyze_root_boundaries(
                alist, "/incoming/W 4k 某剧 第一季OVA",
                root_task_id=root_task_id, state_root=state_root,
            )
            record = load_work_unit_records(state_root, root_task_id)[0]
            apply_work_unit_override(
                state_root, root_task_id, record.work_unit_id,
                media_type="tv", tmdb_id=tmdb.tmdb_id, season=1,
            )
            record = reconcile_root_work_units(
                alist, "/library", state_root, root_task_id,
                episode_catalog=TmdbEpisodeCatalog(tmdb), tmdb_client=tmdb,
            )[0]
            self.assertEqual(record.reconciliation_outcome, "uncertain")
            self.assertIsNone(record.reconciliation_evidence)
            self.assertIn("缺少可证明的季集坐标", record.attention or "")

    def test_specials_only_source_reconciles_via_planner_evidence(self) -> None:
        """D derives a specials-only unit's S00 coordinates from F's own plan.

        A source holding only a letter-variant cut and an unnumbered ``[SP]``
        pair has no episode grammar for any inline D proof.  The planner that
        will write the files resolves both against official Season 00 rows
        (the β-titled special, the ASS Script Info ordinal), so D dry-runs
        that same smart plan and reconciles exactly the coordinates it
        proves — here S00E01/S00E06 beside an existing S01 run.
        """

        class SteinsGateTMDB:
            language = "zh-CN"
            tmdb_id = 42509

            def get(self, path: str, **_params: object) -> dict[str, object]:
                if path == "/tv/42509":
                    return {
                        "name": "命运石之门",
                        "original_name": "Steins;Gate",
                        "first_air_date": "2011-04-06",
                        "seasons": [
                            {"season_number": 0, "episode_count": 6},
                            {"season_number": 1, "episode_count": 24},
                        ],
                    }
                if path == "/tv/42509/season/1":
                    return {"episodes": [
                        {
                            "episode_number": number,
                            "name": f"第{number}话",
                            "air_date": "2011-04-06",
                            "runtime": 24,
                        }
                        for number in range(1, 25)
                    ]}
                if path == "/tv/42509/season/0":
                    return {"episodes": [
                        {
                            "episode_number": 1,
                            "name": "横行跋扈的浪荡之徒",
                            "air_date": "2012-02-22",
                            "runtime": 24,
                        },
                        *[
                            {
                                "episode_number": number,
                                "name": f"聪明睿智的认知计算 第{number - 1}话",
                                "air_date": f"2014-0{number}-01",
                                "runtime": 4,
                            }
                            for number in range(2, 6)
                        ],
                        {
                            "episode_number": 6,
                            "name": "境界面上的缺失之环（β线）",
                            "air_date": "2015-12-03",
                            "runtime": 24,
                        },
                    ]}
                raise AssertionError(f"unexpected TMDB path: {path}")

        source_root = "/library/待刮削/命运石之门SP补扫"
        library: dict[str, bytes] = {
            "/library/番剧/命运石之门/tvshow.nfo": _nfo_tv(42509, "命运石之门", "2011"),
        }
        for episode in range(1, 25):
            library[
                f"/library/番剧/命运石之门/Season 01/S01E{episode:02d}.mkv"
            ] = b"v"
        for episode in range(2, 6):
            library[
                f"/library/番剧/命运石之门/Season 00/S00E{episode:02d}.mkv"
            ] = b"v"
        subtitle_name = "[Ygm]Steins;Gate[SP][Ma10p_2160p][x265_flac_ass].ass"
        files = dict(library)
        files.update({
            (
                f"{source_root}/"
                "[TUDO&Ygm] Steins;Gate [SP][Ma10p_2160p][x265_flac_ass].mkv"
            ): b"v" * 2_000_000,
            (
                f"{source_root}/"
                "[TUDO&Ygm] Steins;Gate [23B][Ma10p_2160p][x265_flac_ass].mkv"
            ): b"v" * 2_000_000,
            f"{source_root}/备份字幕/{subtitle_name}": (
                "[Script Info]\n"
                "Title: Steins;Gate 25 gb\n"
                "ScriptType: v4.00+\n"
                "\n"
                "[V4+ Styles]\n"
            ).encode("utf-8"),
        })
        with tempfile.TemporaryDirectory() as directory:
            state_root = Path(directory)
            root_task_id = "root-specials-only"
            tmdb = SteinsGateTMDB()
            alist = IndexAList(files)
            analyze_root_boundaries(
                alist, source_root,
                root_task_id=root_task_id, state_root=state_root,
            )
            records = load_work_unit_records(state_root, root_task_id)
            self.assertEqual(len(records), 1)
            apply_work_unit_override(
                state_root, root_task_id, records[0].work_unit_id,
                media_type="tv", tmdb_id=tmdb.tmdb_id,
            )
            record = reconcile_root_work_units(
                alist, "/library", state_root, root_task_id,
                episode_catalog=TmdbEpisodeCatalog(tmdb), tmdb_client=tmdb,
            )[0]
            self.assertEqual(record.reconciliation_outcome, "merge_existing")
            self.assertEqual(
                record.matched_work_root, "/library/番剧/命运石之门"
            )
            self.assertIsNone(record.reconciliation_evidence)
            self.assertEqual(record.attention, None)

    def test_specials_only_planner_evidence_fails_closed_on_two_betas(self) -> None:
        """Two β-titled officials keep the specials-only unit parked.

        The dry-run planner is the evidence, and the beta-alternate mapper
        fails closed when the multilingual Season 00 titles name more than
        one β special: ``[23B]`` stays unmapped, the plan reports a problem
        row, and D must not reconcile coordinates the planner cannot prove.
        """

        class TwoBetaTMDB:
            language = "zh-CN"
            tmdb_id = 42509

            def get(self, path: str, **_params: object) -> dict[str, object]:
                if path == "/tv/42509":
                    return {
                        "name": "命运石之门",
                        "original_name": "Steins;Gate",
                        "first_air_date": "2011-04-06",
                        "seasons": [
                            {"season_number": 0, "episode_count": 7},
                            {"season_number": 1, "episode_count": 24},
                        ],
                    }
                if path == "/tv/42509/season/1":
                    return {"episodes": [
                        {
                            "episode_number": number,
                            "name": f"第{number}话",
                            "air_date": "2011-04-06",
                            "runtime": 24,
                        }
                        for number in range(1, 25)
                    ]}
                if path == "/tv/42509/season/0":
                    return {"episodes": [
                        {
                            "episode_number": 1,
                            "name": "横行跋扈的浪荡之徒",
                            "air_date": "2012-02-22",
                            "runtime": 24,
                        },
                        {
                            "episode_number": 6,
                            "name": "境界面上的缺失之环（β线）",
                            "air_date": "2015-12-03",
                            "runtime": 24,
                        },
                        {
                            "episode_number": 7,
                            "name": "另一条β线的特别篇",
                            "air_date": "2016-03-03",
                            "runtime": 24,
                        },
                    ]}
                raise AssertionError(f"unexpected TMDB path: {path}")

        source_root = "/library/待刮削/命运石之门SP补扫"
        subtitle_name = "[Ygm]Steins;Gate[SP][Ma10p_2160p][x265_flac_ass].ass"
        files: dict[str, bytes] = {
            "/library/番剧/命运石之门/tvshow.nfo": _nfo_tv(42509, "命运石之门", "2011"),
            (
                f"{source_root}/"
                "[TUDO&Ygm] Steins;Gate [SP][Ma10p_2160p][x265_flac_ass].mkv"
            ): b"v" * 2_000_000,
            (
                f"{source_root}/"
                "[TUDO&Ygm] Steins;Gate [23B][Ma10p_2160p][x265_flac_ass].mkv"
            ): b"v" * 2_000_000,
            f"{source_root}/备份字幕/{subtitle_name}": (
                "[Script Info]\n"
                "Title: Steins;Gate 25 gb\n"
                "ScriptType: v4.00+\n"
                "\n"
                "[V4+ Styles]\n"
            ).encode("utf-8"),
        }
        with tempfile.TemporaryDirectory() as directory:
            state_root = Path(directory)
            root_task_id = "root-specials-only-two-betas"
            tmdb = TwoBetaTMDB()
            alist = IndexAList(files)
            analyze_root_boundaries(
                alist, source_root,
                root_task_id=root_task_id, state_root=state_root,
            )
            records = load_work_unit_records(state_root, root_task_id)
            self.assertEqual(len(records), 1)
            apply_work_unit_override(
                state_root, root_task_id, records[0].work_unit_id,
                media_type="tv", tmdb_id=tmdb.tmdb_id,
            )
            record = reconcile_root_work_units(
                alist, "/library", state_root, root_task_id,
                episode_catalog=TmdbEpisodeCatalog(tmdb), tmdb_client=tmdb,
            )[0]
            self.assertEqual(record.reconciliation_outcome, "uncertain")
            self.assertIsNone(record.reconciliation_evidence)
            self.assertIn("缺少可证明的季集坐标", record.attention or "")

    def test_separated_season_token_videos_prove_the_declared_season(self) -> None:
        """``S2 [01]`` release names still corroborate their declared season.

        A release run may split the season token from the bracketed episode
        ordinal.  D's unit tokens already read those coordinates through the
        shared coverage parser; the root-scope season proof must not narrow
        to the contiguous ``SxxEyy`` grammar and lose the directory-to-season
        linkage, or a fully covered season would stay uncertain instead of
        reporting its gap.
        """

        class SeparatedSeasonTMDB:
            def __init__(self, tmdb_id: int) -> None:
                self.tmdb_id = tmdb_id

            def get(self, path: str, **_params: object) -> dict[str, object]:
                if path == f"/tv/{self.tmdb_id}":
                    return {
                        "name": "示例剧",
                        "original_name": "示例剧",
                        "seasons": [
                            {"season_number": 1, "episode_count": 10, "name": "第 1 季"},
                            {"season_number": 2, "episode_count": 12, "name": "第 2 季"},
                        ],
                    }
                if path == f"/tv/{self.tmdb_id}/season/1":
                    return {
                        "episodes": [
                            {
                                "episode_number": number,
                                "air_date": "2019-01-11",
                                "name": f"第{number}集",
                            }
                            for number in range(1, 11)
                        ]
                    }
                if path == f"/tv/{self.tmdb_id}/season/2":
                    return {
                        "episodes": [
                            {
                                "episode_number": number,
                                "air_date": "2021-01-08",
                                "name": f"第{number}集",
                            }
                            for number in range(1, 13)
                        ]
                    }
                return {}

        def library_files() -> dict[str, bytes]:
            files: dict[str, bytes] = {
                "/library/番剧/示例剧/tvshow.nfo": _nfo_tv(99057, "示例剧", "2019"),
            }
            for episode in range(1, 11):
                files[
                    f"/library/番剧/示例剧/Season 01/S01E{episode:02d}.mkv"
                ] = b"v"
            return files

        with tempfile.TemporaryDirectory() as directory:
            state_root = Path(directory)
            root_task_id = "root-separated-season-token"
            tmdb = SeparatedSeasonTMDB(99057)
            source = {
                (
                    "/incoming/W 4k 某剧/第一季/"
                    f"[Grp] 某剧 S1 [{episode:02d}][Ma10p_2160p].mkv"
                ): b"v"
                for episode in range(1, 11)
            }
            source.update({
                (
                    "/incoming/W 4k 某剧/第二季/"
                    f"[Grp] 某剧 S2 [{episode:02d}][Ma10p_2160p].mkv"
                ): b"v"
                for episode in range(1, 13)
            })
            alist = IndexAList(library_files() | source)
            analyze_root_boundaries(
                alist, "/incoming/W 4k 某剧",
                root_task_id=root_task_id, state_root=state_root,
            )
            record = next(
                unit
                for unit in load_work_unit_records(state_root, root_task_id)
                if unit.claimed_seasons == (2,)
            )
            apply_work_unit_override(
                state_root, root_task_id, record.work_unit_id,
                media_type="tv", tmdb_id=tmdb.tmdb_id, season=2,
            )
            records = {
                unit.work_unit_id: unit
                for unit in reconcile_root_work_units(
                    alist, "/library", state_root, root_task_id,
                    episode_catalog=TmdbEpisodeCatalog(tmdb), tmdb_client=tmdb,
                )
            }
            record = records[record.work_unit_id]
            self.assertEqual(record.reconciliation_outcome, "merge_existing")
            self.assertEqual(record.matched_work_root, "/library/番剧/示例剧")
            self.assertIsNone(record.attention)
        """TMDB's zero-episode announced season has no coordinate to infer."""
        with tempfile.TemporaryDirectory() as directory:
            state_root = Path(directory)
            tmdb = StrictBareEpisodeTMDB(99040, {1: 6, 2: 0})
            _alist, _state_root, record = self._reconcile_bare_episode_source(
                [f"{episode:02d}.mp4" for episode in range(1, 7)],
                tmdb,
                state_root=state_root,
                root_task_id="root-naked-numeric-future-placeholder",
            )
            self.assertEqual(record.reconciliation_outcome, "new_work")
            self.assertEqual(
                record.reconciliation_evidence and record.reconciliation_evidence["season"],
                1,
            )

    def test_naked_numeric_proof_allows_short_positive_specials(self) -> None:
        """A differently-sized Season 00 is auxiliary, not the source run."""
        with tempfile.TemporaryDirectory() as directory:
            state_root = Path(directory)
            tmdb = StrictBareEpisodeTMDB(99041, {0: 1, 1: 6})
            _alist, _state_root, record = self._reconcile_bare_episode_source(
                [f"{episode:02d}.mp4" for episode in range(1, 7)],
                tmdb,
                state_root=state_root,
                root_task_id="root-naked-numeric-positive-specials",
            )
            self.assertEqual(record.reconciliation_outcome, "new_work")
            self.assertEqual(
                record.reconciliation_evidence and record.reconciliation_evidence["season"],
                1,
            )

    def test_naked_numeric_proof_rejects_same_sized_positive_specials(self) -> None:
        """Equal-sized regular and Season 00 runs remain indistinguishable."""
        with tempfile.TemporaryDirectory() as directory:
            state_root = Path(directory)
            tmdb = StrictBareEpisodeTMDB(99042, {0: 6, 1: 6})
            _alist, _state_root, record = self._reconcile_bare_episode_source(
                [f"{episode:02d}.mp4" for episode in range(1, 7)],
                tmdb,
                state_root=state_root,
                root_task_id="root-naked-numeric-same-sized-specials",
            )
            self.assertEqual(record.reconciliation_outcome, "uncertain")
            self.assertIsNone(record.reconciliation_evidence)

    def test_naked_numeric_source_rejects_a_gap_or_release_suffix(self) -> None:
        """A missing number or ``01.1080p`` must remain uncertain."""
        cases = (
            ["01.mp4", "03.mp4"],
            ["01.1080p.mp4", "02.mp4"],
        )
        with tempfile.TemporaryDirectory() as directory:
            state_root = Path(directory)
            for index, names in enumerate(cases):
                with self.subTest(names=names):
                    tmdb = StrictBareEpisodeTMDB(99005 + index, {1: 2})
                    _alist, _state_root, record = self._reconcile_bare_episode_source(
                        names,
                        tmdb,
                        state_root=state_root,
                        root_task_id=f"root-naked-numeric-reject-{index}",
                    )
                    self.assertEqual(record.reconciliation_outcome, "uncertain")
                    self.assertIsNone(record.reconciliation_evidence)

    def test_bare_e_proof_uses_media_basename_not_a_release_range_parent(self) -> None:
        """``E01-E06`` may label a batch directory, not each member."""
        with tempfile.TemporaryDirectory() as directory:
            state_root = Path(directory)
            tmdb = StrictBareEpisodeTMDB(99013, {1: 6})
            _alist, _state_root, record = self._reconcile_bare_episode_source(
                [
                    f"Hall.of.Shame.E01-E06/Hall.of.Shame.E{episode:02d}.mkv"
                    for episode in range(1, 7)
                ],
                tmdb,
                state_root=state_root,
                root_task_id="root-bare-release-range-parent",
            )
            self.assertEqual(record.reconciliation_outcome, "new_work")
            self.assertEqual(
                record.reconciliation_evidence,
                {
                    "kind": "tmdb_single_positive_season_bare_episodes",
                    "tmdb_id": 99013,
                    "season": 1,
                    "episode_count": 6,
                    "episode_tokens": [
                        f"S01E{episode:02d}" for episode in range(1, 7)
                    ],
                },
            )

    def test_bare_e_proof_rejects_special_parent_context(self) -> None:
        """An OVA/SP hierarchy remains stronger than member-name evidence."""
        with tempfile.TemporaryDirectory() as directory:
            state_root = Path(directory)
            for index, parent in enumerate(("OVA", "SP")):
                with self.subTest(parent=parent):
                    tmdb = StrictBareEpisodeTMDB(99014 + index, {1: 6})
                    _alist, _state_root, record = self._reconcile_bare_episode_source(
                        [
                            f"{parent}/Show.E{episode:02d}.mkv"
                            for episode in range(1, 7)
                        ],
                        tmdb,
                        state_root=state_root,
                        root_task_id=f"root-bare-special-parent-{parent.casefold()}",
                    )
                    self.assertEqual(record.reconciliation_outcome, "uncertain")
                    self.assertIsNone(record.reconciliation_evidence)

    def test_bare_e_proof_fails_closed_for_nonunique_or_nonexact_evidence(self) -> None:
        cases = (
            (
                "multiple-positive-seasons",
                [f"Show.E{episode:02d}.mkv" for episode in range(1, 7)],
                StrictBareEpisodeTMDB(99004, {1: 6, 2: 6}),
            ),
            (
                "same-sized-specials",
                [f"Show.E{episode:02d}.mkv" for episode in range(1, 7)],
                StrictBareEpisodeTMDB(99005, {0: 6, 1: 6}),
            ),
            (
                "catalog-count-mismatch",
                [f"Show.E{episode:02d}.mkv" for episode in range(1, 7)],
                StrictBareEpisodeTMDB(99006, {1: 6}, payload_counts={1: 5}),
            ),
            (
                "noncontiguous",
                ["Show.E01.mkv", "Show.E03.mkv"],
                StrictBareEpisodeTMDB(99007, {1: 2}),
            ),
            (
                "duplicate-version",
                ["Show.E01.720p.mkv", "Show.E01.1080p.mkv"],
                StrictBareEpisodeTMDB(99008, {1: 2}),
            ),
            (
                "adjacent-episode-markers",
                ["Show.E01E02.mkv", "Show.E02.mkv"],
                StrictBareEpisodeTMDB(99012, {1: 2}),
            ),
            (
                "mixed-qualified",
                ["Show.E01.mkv", "Show.S01E02.mkv"],
                StrictBareEpisodeTMDB(99009, {1: 2}),
            ),
            (
                "special-video",
                ["Show.E01.mkv", "Show.SP.E02.mkv"],
                StrictBareEpisodeTMDB(99010, {1: 2}),
            ),
            (
                "unparsed-video",
                ["Show.E01.mkv", "trailer.mkv"],
                StrictBareEpisodeTMDB(99011, {1: 2}),
            ),
        )
        with tempfile.TemporaryDirectory() as directory:
            state_root = Path(directory)
            for index, (label, names, tmdb) in enumerate(cases):
                with self.subTest(label=label):
                    _alist, _state_root, record = self._reconcile_bare_episode_source(
                        names,
                        tmdb,
                        state_root=state_root,
                        root_task_id=f"root-bare-reject-{index}",
                    )
                    self.assertEqual(record.reconciliation_outcome, "uncertain")
                    self.assertIsNone(record.reconciliation_evidence)
                    self.assertIn("裸 E", record.attention or "")

    def test_bare_e_proof_requires_the_fresh_scope_to_match_b_snapshot(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            tmdb = StrictBareEpisodeTMDB(99012, {1: 6})
            names = [f"Show.E{episode:02d}.mkv" for episode in range(1, 7)]
            state_root = Path(directory)
            alist = IndexAList({
                f"/incoming/One Season Show/{name}": b"v" for name in names
            })
            analyze_root_boundaries(
                alist,
                "/incoming/One Season Show",
                root_task_id="root-bare-stale",
                state_root=state_root,
            )
            record = load_work_unit_records(state_root, "root-bare-stale")[0]
            apply_work_unit_override(
                state_root,
                "root-bare-stale",
                record.work_unit_id,
                media_type="tv",
                tmdb_id=tmdb.tmdb_id,
            )
            del alist.files["/incoming/One Season Show/Show.E06.mkv"]
            alist.files["/incoming/One Season Show/Show.E07.mkv"] = b"v"
            reconciled = reconcile_root_work_units(
                alist,
                "/library",
                state_root,
                "root-bare-stale",
                episode_catalog=TmdbEpisodeCatalog(tmdb),
                tmdb_client=tmdb,
            )
            self.assertEqual(reconciled[0].reconciliation_outcome, "uncertain")
            self.assertIsNone(reconciled[0].reconciliation_evidence)

    def test_complete_bracketed_run_excludes_only_ncop_nced_and_persists_proof(self) -> None:
        """A pure ``[01]..[12]`` TV run gets the same D receipt as bare E.

        The NCOP/NCED files are intentionally present in the source tree:
        they are the only known non-story extras that may be excluded from
        the strict all-primary-video contiguous-run proof.
        """
        with tempfile.TemporaryDirectory() as directory:
            state_root = Path(directory)
            tmdb = StrictBareEpisodeTMDB(99030, {1: 12})
            names = [
                (
                    f"[Ygm] Example Show [{episode:02d}]"
                    "[Ma10p_2160p][x265_flac_ass].mkv"
                )
                for episode in range(1, 13)
            ] + [
                "[Ygm] Example Show [NCOP][Ma10p_2160p][x265_flac_ass].mkv",
                "[Ygm] Example Show [NCED][Ma10p_2160p][x265_flac_ass].mkv",
            ]
            _alist, state_root, record = self._reconcile_bare_episode_source(
                names,
                tmdb,
                state_root=state_root,
                root_task_id="root-bracketed-complete",
            )
            self.assertEqual(record.reconciliation_outcome, "new_work")
            self.assertEqual(
                record.reconciliation_evidence,
                {
                    "kind": "tmdb_single_positive_season_bracketed_episodes",
                    "tmdb_id": 99030,
                    "season": 1,
                    "episode_count": 12,
                    "episode_tokens": [
                        f"S01E{episode:02d}" for episode in range(1, 13)
                    ],
                },
            )
            persisted = load_work_unit_records(
                state_root, "root-bracketed-complete"
            )[0]
            self.assertEqual(
                persisted.reconciliation_evidence,
                record.reconciliation_evidence,
            )

    def test_bracketed_run_of_flat_split_file_scopes_gets_season_proof(self) -> None:
        """A flat-split bracket run owns exact FILE scopes; D must prove it.

        The mixed feature-plus-bracket-run B/W split emits one movie unit and
        one tv unit whose source_paths are the exact media files, not a
        directory.  The single-season proof used to accept directory scopes
        only, so such a run parked as "纯方括号集号未能证明为完整唯一的
        TMDB 正季" even when the run and the catalog matched exactly.
        """

        class RowsAList:
            """Sized listing double: one flat folder of large video files."""

            def __init__(self, entries: dict[str, list[dict[str, object]]]) -> None:
                self.entries = {
                    str(path).rstrip("/"): rows for path, rows in entries.items()
                }

            def list(self, path: str, refresh: bool = False) -> list[dict[str, object]]:
                del refresh
                return [dict(row) for row in self.entries.get(path.rstrip("/"), [])]

        with tempfile.TemporaryDirectory() as directory:
            state_root = Path(directory)
            big = 2 * 1024 ** 3
            folder = "/incoming/Mixed Bundle"
            run_one = f"{folder}/Northwind ~The Semi-Final~ [01] 2160p.mkv"
            run_two = f"{folder}/Northwind ~The Semi-Final~ [02] 2160p.mkv"
            alist = RowsAList({
                folder: [
                    {
                        "name": "Northwind ~The Final~ 2160p.mkv",
                        "is_dir": False,
                        "size": 13 * 1024 ** 3,
                    },
                    {"name": run_one.rsplit("/", 1)[-1], "is_dir": False, "size": big},
                    {"name": run_two.rsplit("/", 1)[-1], "is_dir": False, "size": big},
                ],
            })
            analyze_root_boundaries(
                alist,
                folder,
                root_task_id="root-flat-bracket",
                state_root=state_root,
            )
            records = load_work_unit_records(state_root, "root-flat-bracket")
            self.assertEqual(len(records), 2)
            series = next(
                record for record in records if record.media_context == "tv"
            )
            self.assertEqual(series.source_paths, (run_one, run_two))
            apply_work_unit_override(
                state_root,
                "root-flat-bracket",
                series.work_unit_id,
                media_type="tv",
                tmdb_id=99031,
            )
            tmdb = StrictBareEpisodeTMDB(99031, {1: 2})
            reconciled = reconcile_root_work_units(
                alist,
                "/library",
                state_root,
                "root-flat-bracket",
                episode_catalog=TmdbEpisodeCatalog(tmdb),
                tmdb_client=tmdb,
            )
            updated = next(
                record
                for record in reconciled
                if record.work_unit_id == series.work_unit_id
            )
            self.assertEqual(updated.reconciliation_outcome, "new_work")
            self.assertEqual(
                updated.reconciliation_evidence,
                {
                    "kind": "tmdb_single_positive_season_bracketed_episodes",
                    "tmdb_id": 99031,
                    "season": 1,
                    "episode_count": 2,
                    "episode_tokens": ["S01E01", "S01E02"],
                },
            )

    def test_bracketed_proof_accepts_auxiliary_specials_and_ncop_nced(self) -> None:
        """Published S00 plus explicit NCOP/NCED do not change S01 proof."""
        with tempfile.TemporaryDirectory() as directory:
            state_root = Path(directory)
            tmdb = StrictBareEpisodeTMDB(99038, {0: 2, 1: 12})
            names = [
                f"[Ygm] Example Show [{episode:02d}][Ma10p_2160p].mkv"
                for episode in range(1, 13)
            ] + [
                "NCOP&ED/[Ygm] Example Show [NCOP][Ma10p_2160p].mkv",
                "NCOP&ED/[Ygm] Example Show [NCED][Ma10p_2160p].mkv",
            ]
            _alist, _state_root, record = self._reconcile_bare_episode_source(
                names,
                tmdb,
                state_root=state_root,
                root_task_id="root-bracketed-auxiliary-specials",
            )
            self.assertEqual(record.reconciliation_outcome, "new_work")
            self.assertEqual(
                record.reconciliation_evidence and record.reconciliation_evidence["season"],
                1,
            )

    def test_bracketed_proof_excludes_fractional_special_video(self) -> None:
        """A ``[11.5]`` fractional special is a separate coordinate.

        It must not invalidate the integer ``[01]..[12]`` single-season proof,
        just like NCOP/NCED are omitted.
        """
        with tempfile.TemporaryDirectory() as directory:
            state_root = Path(directory)
            tmdb = StrictBareEpisodeTMDB(99039, {0: 1, 1: 12})
            names = [
                f"[Ygm] Example Show [{episode:02d}][Ma10p_2160p].mkv"
                for episode in range(1, 13)
            ] + [
                "[Ygm] Example Show [11.5][Ma10p_2160p].mkv",
            ]
            _alist, _state_root, record = self._reconcile_bare_episode_source(
                names,
                tmdb,
                state_root=state_root,
                root_task_id="root-bracketed-fractional-special",
            )
            self.assertEqual(record.reconciliation_outcome, "new_work")
            self.assertEqual(record.reconciliation_evidence["season"], 1)
            self.assertEqual(record.reconciliation_evidence["episode_count"], 12)

    def test_bracketed_proof_excludes_dedicated_bonus_directory_ordinals(self) -> None:
        """A dedicated ``PV``/``特典映像``/``menu`` directory is bonus context.

        Releases put bare ordinals inside bonus directories
        (``PV/[01].mkv``, ``特典映像/[01].mkv``); those collide with the real
        episode run if treated as primary videos.  The directory itself is
        strong non-story context (same tier as ``NCOP&ED``), so the proof
        must omit them instead of failing closed.
        """
        with tempfile.TemporaryDirectory() as directory:
            state_root = Path(directory)
            tmdb = StrictBareEpisodeTMDB(99041, {1: 4})
            names = [
                f"[Raws] Example Show [{episode:02d}][1080P].mkv"
                for episode in range(1, 5)
            ] + [
                "PV/[Raws] Example Show [01][1080P].mkv",
                "PV/[Raws] Example Show [02][1080P].mkv",
                "特典映像/[Raws] Example Show [01][1080P].mkv",
                "特典映像/[Raws] Example Show [02][1080P].mkv",
                "menu/[Raws] Example Show [Menu01].mkv",
            ]
            _alist, _state_root, record = self._reconcile_bare_episode_source(
                names,
                tmdb,
                state_root=state_root,
                root_task_id="root-bracketed-bonus-dirs",
            )
            self.assertEqual(record.reconciliation_outcome, "new_work")
            self.assertEqual(
                record.reconciliation_evidence and record.reconciliation_evidence["episode_count"],
                4,
            )

    def test_sp_marker_in_bonus_directory_is_release_naming_not_a_special_family(self) -> None:
        """``[SP01] NCOP [02 [ Type-A ]]`` in ``NCOP&ED/`` is not a special run.

        Bonus-directory NCOP files often carry an ``SP`` release marker with
        duplicated variant ordinals (Type-A/B/C).  Their context already
        proves them non-story, so they must not enter the physical-special
        family completeness check and break the TV proof (黑色五叶草 shape).
        """
        with tempfile.TemporaryDirectory() as directory:
            state_root = Path(directory)
            tmdb = StrictBareEpisodeTMDB(99042, {1: 4})
            names = [
                f"[Raws] Example Show [{episode:03d}][Ma10p].mkv"
                for episode in range(1, 5)
            ] + [
                "NCOP&ED/[Raws] Example Show [SP01] NCOP [01][Ma10p].mkv",
                "NCOP&ED/[Raws] Example Show [SP01] NCOP [02 [ Type-A ]][Ma10p].mkv",
                "NCOP&ED/[Raws] Example Show [SP01] NCOP [02 [ Type-B ]][Ma10p].mkv",
            ]
            _alist, _state_root, record = self._reconcile_bare_episode_source(
                names,
                tmdb,
                state_root=state_root,
                root_task_id="root-sp-in-bonus-dir",
            )
            self.assertEqual(record.reconciliation_outcome, "new_work")
            self.assertEqual(
                record.reconciliation_evidence and record.reconciliation_evidence["episode_count"],
                4,
            )

    def test_incomplete_special_family_outside_bonus_context_still_fails_closed(self) -> None:
        """An SP-marked story run with duplicate ordinals keeps failing closed.

        The bonus-context exclusion must not swallow a genuinely incomplete
        physical-special family among story-candidate files.
        """
        with tempfile.TemporaryDirectory() as directory:
            state_root = Path(directory)
            tmdb = StrictBareEpisodeTMDB(99043, {1: 4})
            names = [
                f"[Raws] Example Show [{episode:03d}][Ma10p].mkv"
                for episode in range(1, 5)
            ] + [
                "OAD/[Raws] Example Show [SP01].mkv",
                "OAD/[Raws] Example Show [SP02].mkv",
                "OAD/[Raws] Example Show [SP02].mkv",
            ]
            _alist, _state_root, record = self._reconcile_bare_episode_source(
                names,
                tmdb,
                state_root=state_root,
                root_task_id="root-incomplete-special-family",
            )
            self.assertNotEqual(record.reconciliation_outcome, "new_work")

    def test_bracketed_proof_fails_closed_for_ambiguous_special_or_extra_video(self) -> None:
        cases = (
            (
                "resolution-only",
                ["Example Show [1080].mkv"],
                StrictBareEpisodeTMDB(99031, {1: 1}),
            ),
            (
                "multiple-pure-numbers",
                ["Example Show [01][02].mkv", "Example Show [02].mkv"],
                StrictBareEpisodeTMDB(99032, {1: 2}),
            ),
            (
                "ova-parent",
                [
                    "OVA/Example Show [01].mkv",
                    "OVA/Example Show [02].mkv",
                ],
                StrictBareEpisodeTMDB(99033, {1: 2}),
            ),
            (
                "special-parent",
                [
                    "SP/Example Show [01].mkv",
                    "SP/Example Show [02].mkv",
                ],
                StrictBareEpisodeTMDB(99037, {1: 2}),
            ),
            (
                "unparsed-video",
                ["Example Show [01].mkv", "trailer.mkv"],
                StrictBareEpisodeTMDB(99034, {1: 2}),
            ),
            (
                "mv-is-not-an-automatic-extra",
                [
                    "Example Show [01].mkv",
                    "Example Show [02].mkv",
                    "Example Show [MV].mkv",
                ],
                StrictBareEpisodeTMDB(99039, {1: 2}),
            ),
            (
                "multiple-positive-seasons",
                ["Example Show [01].mkv", "Example Show [02].mkv"],
                StrictBareEpisodeTMDB(99035, {1: 2, 2: 2}),
            ),
        )
        with tempfile.TemporaryDirectory() as directory:
            state_root = Path(directory)
            for index, (label, names, tmdb) in enumerate(cases):
                with self.subTest(label=label):
                    _alist, _state_root, record = self._reconcile_bare_episode_source(
                        names,
                        tmdb,
                        state_root=state_root,
                        root_task_id=f"root-bracketed-reject-{index}",
                    )
                    self.assertEqual(record.reconciliation_outcome, "uncertain")
                    self.assertIsNone(record.reconciliation_evidence)

    def test_bracketed_proof_requires_the_fresh_scope_to_match_b_snapshot(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            state_root = Path(directory)
            root_task_id = "root-bracketed-stale"
            tmdb = StrictBareEpisodeTMDB(99036, {1: 2})
            alist = IndexAList({
                "/incoming/One Season Show/Example Show [01].mkv": b"v",
                "/incoming/One Season Show/Example Show [02].mkv": b"v",
            })
            analyze_root_boundaries(
                alist,
                "/incoming/One Season Show",
                root_task_id=root_task_id,
                state_root=state_root,
            )
            record = load_work_unit_records(state_root, root_task_id)[0]
            apply_work_unit_override(
                state_root,
                root_task_id,
                record.work_unit_id,
                media_type="tv",
                tmdb_id=tmdb.tmdb_id,
            )
            del alist.files["/incoming/One Season Show/Example Show [02].mkv"]
            alist.files["/incoming/One Season Show/Example Show [03].mkv"] = b"v"
            reconciled = reconcile_root_work_units(
                alist,
                "/library",
                state_root,
                root_task_id,
                episode_catalog=TmdbEpisodeCatalog(tmdb),
                tmdb_client=tmdb,
            )
            self.assertEqual(reconciled[0].reconciliation_outcome, "uncertain")
            self.assertIsNone(reconciled[0].reconciliation_evidence)

    def test_source_season_directory_proves_a_bare_episode_coordinate(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            state_root = Path(directory)
            root_task_id = "root-source-season-context"
            files = _sample_library()
            files["/incoming/Season Context/Season 01/Hall.E01.mkv"] = b"v"
            alist = IndexAList(files)
            analyze_root_boundaries(
                alist, "/incoming/Season Context",
                root_task_id=root_task_id, state_root=state_root,
            )
            record = load_work_unit_records(state_root, root_task_id)[0]
            apply_work_unit_override(
                state_root, root_task_id, record.work_unit_id,
                media_type="tv", tmdb_id=35507,
            )

            reconciled = reconcile_root_work_units(
                alist, "/library", state_root, root_task_id,
            )

            self.assertEqual(reconciled[0].reconciliation_outcome, "duplicate_complete")

    def test_mixed_season_scope_revalidates_claims_without_counting_sp_as_season(self) -> None:
        """A generic SP scope does not invalidate exact season-directory claims."""
        with tempfile.TemporaryDirectory() as directory:
            state_root = Path(directory)
            root_task_id = "root-mixed-season-sp"
            source = "/incoming/Example Show"
            files = {
                f"{source}/S01/Example.Show.S01E01.mkv": b"v",
                f"{source}/S02/Example.Show.S02E01.mkv": b"v",
                f"{source}/SP/Example.Show.S00E01.mkv": b"v",
                f"{source}/剧场版/Example Feature (2019)/Example.Feature.2019.mkv": b"v",
            }
            alist = IndexAList(files)
            analyze_root_boundaries(
                alist, source, root_task_id=root_task_id, state_root=state_root,
            )
            records = load_work_unit_records(state_root, root_task_id)
            tv = next(record for record in records if record.display_label == "Example Show")
            self.assertEqual(tv.source_paths, (f"{source}/S01", f"{source}/S02", f"{source}/SP"))
            self.assertEqual(tv.claimed_seasons, (1, 2))
            apply_work_unit_override(
                state_root, root_task_id, tv.work_unit_id,
                media_type="tv", tmdb_id=99030,
            )

            reconciled = reconcile_root_work_units(
                alist, "/library", state_root, root_task_id,
            )

            updated = next(record for record in reconciled if record.work_unit_id == tv.work_unit_id)
            self.assertEqual(updated.reconciliation_outcome, "new_work")

    def test_claimed_empty_season_needs_catalog_evidence_before_duplicate_check(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            state_root = Path(directory)
            root_task_id = "root-declared-empty-season"
            files = {
                "/library/欧美剧/Example Show/tvshow.nfo": _nfo_tv(
                    99002, "Example Show", "2020",
                ),
                "/library/欧美剧/Example Show/Season 01/S01E01.mkv": b"v",
                "/library/欧美剧/Example Show/Season 02/S02E01.mkv": b"v",
                "/incoming/Example Bundle/Example.Show.S01.WEB-DL/Example.Show.S01E01.mkv": b"v",
                "/incoming/Example Bundle/Example.Show.S02.WEB-DL/Example.Show.S02E01.mkv": b"v",
            }
            alist = IndexAList(files)
            alist.dirs.add("/incoming/Example Bundle/Example.Show.S03.WEB-DL")
            analyze_root_boundaries(
                alist, "/incoming/Example Bundle",
                root_task_id=root_task_id, state_root=state_root,
            )
            record = load_work_unit_records(state_root, root_task_id)[0]
            self.assertEqual(record.claimed_seasons, (1, 2, 3))
            apply_work_unit_override(
                state_root, root_task_id, record.work_unit_id,
                media_type="tv", tmdb_id=99002,
            )

            without_catalog = reconcile_root_work_units(
                alist, "/library", state_root, root_task_id,
            )
            self.assertEqual(without_catalog[0].reconciliation_outcome, "uncertain")
            self.assertIn("官方季集证据", without_catalog[0].attention or "")

            # Reset the D verdict through the normal durable identity
            # confirmation, then provide only the declared empty S03 rows.
            apply_work_unit_override(
                state_root, root_task_id, record.work_unit_id,
                media_type="tv", tmdb_id=99002,
            )
            with_catalog = reconcile_root_work_units(
                alist,
                "/library",
                state_root,
                root_task_id,
                episode_catalog=lambda _identity: {
                    3: [
                        {"season_number": 3, "episode_number": 1},
                        {"season_number": 3, "episode_number": 2},
                    ],
                },
            )
            self.assertEqual(with_catalog[0].reconciliation_outcome, "existing_gap")
            self.assertEqual(
                with_catalog[0].uncovered_tokens,
                ("S03E01", "S03E02"),
            )

    def test_residual_only_source_reconciles_by_library_identity(self) -> None:
        """A theme-video-only TV source is classified by its library identity.

        The planner never writes theme/menu/commercial/bonus-directory media,
        so an all-residual source has no story coordinates to contribute and
        must not park uncertain forever once the formal library already holds
        the identity: catalog episodes missing from the library's owned
        seasons register as gaps, and a fully covered work consumes the
        source as a duplicate.  Without the library identity the same source
        stays fail-closed — an extras-only source can never found a work
        root.
        """
        with tempfile.TemporaryDirectory() as directory:
            state_root = Path(directory)
            root_task_id = "root-residual-only"
            source = "/incoming/Residual Show"
            files = {
                "/library/番剧/Residual Show/tvshow.nfo": _nfo_tv(
                    99045, "Residual Show", "2020",
                ),
                "/library/番剧/Residual Show/Season 00/S00E01.mkv": b"v",
                "/library/番剧/Residual Show/Season 00/S00E02.mkv": b"v",
                "/library/番剧/Residual Show/Season 01/S01E01.mkv": b"v",
                "/library/番剧/Residual Show/Season 01/S01E02.mkv": b"v",
                f"{source}/[Group] Residual Show [NCOP][Ma10p].mkv": b"v",
                f"{source}/[Group] Residual Show [NCED][Ma10p].mkv": b"v",
                f"{source}/menu/[Group] Residual Show [Menu01].mkv": b"v",
            }
            alist = IndexAList(files)
            alist.dirs.add(f"{source}/SPs")
            analyze_root_boundaries(
                alist, source, root_task_id=root_task_id, state_root=state_root,
            )
            record = load_work_unit_records(state_root, root_task_id)[0]
            apply_work_unit_override(
                state_root, root_task_id, record.work_unit_id,
                media_type="tv", tmdb_id=99045,
            )

            catalog = {
                0: [
                    {"season_number": 0, "episode_number": 1},
                    {"season_number": 0, "episode_number": 2},
                    {"season_number": 0, "episode_number": 3},
                ],
                1: [
                    {"season_number": 1, "episode_number": 1},
                    {"season_number": 1, "episode_number": 2},
                ],
            }
            gapped = reconcile_root_work_units(
                alist, "/library", state_root, root_task_id,
                episode_catalog=lambda _identity: catalog,
            )
            self.assertEqual(gapped[0].reconciliation_outcome, "existing_gap")
            self.assertEqual(gapped[0].uncovered_tokens, ("S00E03",))

            with tempfile.TemporaryDirectory() as second:
                covered_root = Path(second)
                covered_task = "root-residual-only-covered"
                covered_alist = IndexAList(files)
                covered_alist.dirs.add(f"{source}/SPs")
                analyze_root_boundaries(
                    covered_alist, source,
                    root_task_id=covered_task, state_root=covered_root,
                )
                covered_record = load_work_unit_records(
                    covered_root, covered_task,
                )[0]
                apply_work_unit_override(
                    covered_root, covered_task, covered_record.work_unit_id,
                    media_type="tv", tmdb_id=99045,
                )
                # Add the missing library episode so the owned seasons are
                # fully covered: the same residual source is a pure duplicate.
                covered_alist.files[
                    "/library/番剧/Residual Show/Season 00/S00E03.mkv"
                ] = b"v"
                duplicate = reconcile_root_work_units(
                    covered_alist, "/library", covered_root, covered_task,
                    episode_catalog=lambda _identity: catalog,
                )
                self.assertEqual(
                    duplicate[0].reconciliation_outcome, "duplicate_complete",
                )

            with tempfile.TemporaryDirectory() as third:
                unknown_root = Path(third)
                unknown_task = "root-residual-only-unknown"
                unknown_alist = IndexAList({
                    key: value for key, value in files.items()
                    if key.startswith("/incoming/")
                })
                unknown_alist.dirs.add(f"{source}/SPs")
                analyze_root_boundaries(
                    unknown_alist, source,
                    root_task_id=unknown_task, state_root=unknown_root,
                )
                unknown_record = load_work_unit_records(
                    unknown_root, unknown_task,
                )[0]
                apply_work_unit_override(
                    unknown_root, unknown_task, unknown_record.work_unit_id,
                    media_type="tv", tmdb_id=99046,
                )
                unknown = reconcile_root_work_units(
                    unknown_alist, "/library", unknown_root, unknown_task,
                    episode_catalog=lambda _identity: catalog,
                )
                self.assertEqual(unknown[0].reconciliation_outcome, "uncertain")
                self.assertIn(
                    "季集坐标", unknown[0].attention or "",
                )

    def test_rooted_declared_subtitle_only_season_is_reconciled_as_exact_gap(self) -> None:
        """A rooted multi-season TV need not expose one WorkUnit scope per season.

        The source root owns decorated S01/S02/S03 directories and direct
        S04 media.  S02 has only a complete, explicitly numbered subtitle
        sequence.  B/W may declare that season; D must revalidate the direct
        child evidence instead of incorrectly zipping the one root scope to
        all four claimed seasons.
        """
        with tempfile.TemporaryDirectory() as directory:
            state_root = Path(directory)
            root_task_id = "rooted-declared-subtitle-season"
            source = "/incoming/Rooted Bundle"
            files = {
                "/library/番剧/Rooted Show/tvshow.nfo": _nfo_tv(
                    99020, "Rooted Show", "2020",
                ),
                "/library/番剧/Rooted Show/Season 01/S01E01.mkv": b"v",
                "/library/番剧/Rooted Show/Season 03/S03E01.mkv": b"v",
                "/library/番剧/Rooted Show/Season 04/S04E01.mkv": b"v",
                f"{source}/第 1 季 - 1080p/Rooted.Show.S01E01.mkv": b"v",
                f"{source}/第 2 季 - 1080p/S02字幕/Rooted.Show.S02E01.sup": b"s",
                f"{source}/第 2 季 - 1080p/S02字幕/Rooted.Show.S02E02.sup": b"s",
                f"{source}/第 3 季 - 1080p/Rooted.Show.S03E01.mkv": b"v",
                f"{source}/Rooted.Show.S04E01.mkv": b"v",
            }
            alist = IndexAList(files)
            analyze_root_boundaries(
                alist, source, root_task_id=root_task_id, state_root=state_root,
            )
            record = load_work_unit_records(state_root, root_task_id)[0]
            self.assertEqual(record.source_paths, (source,))
            self.assertEqual(record.claimed_seasons, (1, 2, 3, 4))
            apply_work_unit_override(
                state_root, root_task_id, record.work_unit_id,
                media_type="tv", tmdb_id=99020,
            )

            reconciled = reconcile_root_work_units(
                alist,
                "/library",
                state_root,
                root_task_id,
                episode_catalog=lambda _identity: {
                    2: [
                        {"season_number": 2, "episode_number": 1},
                        {"season_number": 2, "episode_number": 2},
                    ],
                },
            )

            self.assertEqual(reconciled[0].reconciliation_outcome, "existing_gap")
            self.assertEqual(
                reconciled[0].uncovered_tokens,
                ("S02E01", "S02E02"),
            )

    def test_rooted_claim_rejects_duplicate_child_and_direct_video_layout(self) -> None:
        """One claimed season may not be represented by two physical layouts."""
        with tempfile.TemporaryDirectory() as directory:
            state_root = Path(directory)
            root_task_id = "rooted-duplicate-season-layout"
            source = "/incoming/Rooted Duplicate"
            alist = IndexAList({
                f"{source}/Season 01/Rooted.Show.S01E01.mkv": b"v",
                f"{source}/Season 02/Rooted.Show.S02E01.mkv": b"v",
                f"{source}/Rooted.Show.S01E01.mkv": b"v",
            })
            analyze_root_boundaries(
                alist, source, root_task_id=root_task_id, state_root=state_root,
            )
            record = load_work_unit_records(state_root, root_task_id)[0]
            self.assertEqual(record.claimed_seasons, (1, 2))
            apply_work_unit_override(
                state_root, root_task_id, record.work_unit_id,
                media_type="tv", tmdb_id=99021,
            )

            reconciled = reconcile_root_work_units(
                alist, "/library", state_root, root_task_id,
            )

            self.assertEqual(reconciled[0].reconciliation_outcome, "uncertain")
            self.assertIn("声明季与来源目录", reconciled[0].attention or "")


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
        files["/incoming/Big Buck Bunny (2008)/大雄兔 (2008).mkv"] = b"v"
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

    def test_failed_acceptance_keeps_decision_when_source_is_consumed(self) -> None:
        """A partial write must not discard its durable D verdict.

        When the writer moved the media and only an artifact upload failed,
        the fresh source no longer equals the B snapshot.  Re-evaluating D
        from the consumed source can prove nothing (deadlock); the verdict
        stays and F's already-present readback completes the artifacts.
        """
        files = _sample_library()
        files["/incoming/Big Buck Bunny (2008)/大雄兔 (2008).mkv"] = b"v"
        alist = IndexAList(files)
        import tempfile
        from pathlib import Path
        from local.scrapeflow_api.unit_execution import (
            WorkAcceptanceResult, save_work_acceptance,
        )
        with tempfile.TemporaryDirectory() as directory:
            state_root = Path(directory)
            root_task_id = "root-partial-write"
            analyze_root_boundaries(
                alist, "/incoming/Big Buck Bunny (2008)",
                root_task_id=root_task_id, state_root=state_root,
            )
            records = load_work_unit_records(state_root, root_task_id)
            apply_work_unit_override(
                state_root, root_task_id, records[0].work_unit_id,
                media_type="movie", tmdb_id=10378,
            )
            empty = IndexAList({})
            first = reconcile_root_work_units(empty, "/library", state_root, root_task_id)
            self.assertEqual(first[0].reconciliation_outcome, "new_work")
            save_work_acceptance(state_root, root_task_id, [
                WorkAcceptanceResult(
                    work_unit_id=records[0].work_unit_id, outcome="failed",
                    writer_job_id=None, phase="failed", target_root="",
                    planned_files=0, error="AList 上传失败: timeout",
                    recorded_at="2026-08-27T00:00:00Z",
                ),
            ])
            # The source directory no longer holds the video: the write
            # consumed it (only the infrastructure artifact upload failed).
            consumed = IndexAList({})
            second = reconcile_root_work_units(consumed, "/library", state_root, root_task_id)
            self.assertEqual(second[0].reconciliation_outcome, "new_work")
            self.assertEqual(second[0].matched_work_root, first[0].matched_work_root)


class NestedWorkNfoTests(unittest.TestCase):
    def test_nested_sub_work_nfos_are_indexed_without_overwriting_container_identity(self) -> None:
        files = _sample_library()
        # A Fate-style container: the series root plus a nested movie.
        files["/library/番剧/刀剑神域/tvshow.nfo"] = _nfo_tv(45782, "刀剑神域", "2012")
        files["/library/番剧/刀剑神域/Season 01/S01E01.mkv"] = b"v"
        files["/library/番剧/刀剑神域/序列之争 (2017)/序列之争 (2017).nfo"] = _nfo_movie(
            413594, "序列之争", "2017",
        )
        files["/library/番剧/刀剑神域/序列之争 (2017)/序列之争 (2017).mkv"] = b"v"
        files["/library/番剧/刀剑神域/Alternative/tvshow.nfo"] = _nfo_tv(
            77661, "Alternative", "2018",
        )
        files["/library/番剧/刀剑神域/Alternative/Season 02/S02E03.mkv"] = b"v"
        index = build_library_index(IndexAList(files), "/library")
        entries = index.entries_for("tv", 45782)
        self.assertEqual(len(entries), 1)
        self.assertEqual(entries[0].work_root, "/library/番剧/刀剑神域")
        self.assertIn("S01E01", entries[0].episode_tokens)
        self.assertNotIn("S02E03", entries[0].episode_tokens)
        # Nested identities get their own work roots rather than hijacking
        # the container identity or disappearing from the three-shelf index.
        movie = index.entries_for("movie", 413594)
        self.assertEqual(len(movie), 1)
        self.assertEqual(
            movie[0].work_root,
            "/library/番剧/刀剑神域/序列之争 (2017)",
        )
        nested_decision = decide_reconciliation(
            index, media_type="movie", tmdb_id=413594,
        )
        self.assertEqual(nested_decision.outcome, "duplicate_complete")
        self.assertEqual(nested_decision.work_root, movie[0].work_root)
        tv = index.entries_for("tv", 77661)
        self.assertEqual(len(tv), 1)
        self.assertEqual(tv[0].episode_tokens, {"S02E03"})

class ReleaseDashExtraDirectoryTests(LibraryIndexTests):
    def test_release_dash_proof_excludes_dedicated_extra_directory(self) -> None:
        """``EXTRA/[SP00] Menu - 01`` does not break the dash run.

        The dash grammar excludes no video by its own file shape, but a
        dedicated bonus directory is strong non-story context: SP-marked
        Menu/NCOP/Picture-Drama assets inside ``EXTRA/`` must be omitted
        from the run instead of colliding with the real episodes
        (轮回七次 Moozzi2 shape).
        """
        with tempfile.TemporaryDirectory() as directory:
            state_root = Path(directory)
            tmdb = StrictBareEpisodeTMDB(99044, {1: 4})
            names = [
                f"Example Show - {episode:02d} (BD 1080p).mkv"
                for episode in range(1, 5)
            ] + [
                "EXTRA/Example Show [SP00] Menu - 01 (BD 1080p).mkv",
                "EXTRA/Example Show [SP01] NCOP (BD 1080p).mkv",
                "EXTRA/Example Show [SP05] Picture Drama - 01 (BD 1080p).mkv",
                "EXTRA/Example Show [SP05] Picture Drama - 02 (BD 1080p).mkv",
            ]
            _alist, _state_root, record = self._reconcile_bare_episode_source(
                names,
                tmdb,
                state_root=state_root,
                root_task_id="root-dash-extra-dir",
            )
            self.assertEqual(record.reconciliation_outcome, "new_work")
            self.assertEqual(
                record.reconciliation_evidence and record.reconciliation_evidence["episode_count"],
                4,
            )


class SameSizedSpecialsRuntimeTests(LibraryIndexTests):
    def test_same_sized_specials_bucket_resolved_by_runtime_profiles(self) -> None:
        """A same-sized S00 bucket is unambiguous when runtimes separate it.

        轮回七次 shape: TMDB declares 12 one-minute specials beside twelve
        24-minute regular episodes.  The published runtime profiles resolve
        the run to the regular season; missing or overlapping profiles keep
        the fail-closed verdict.
        """
        with tempfile.TemporaryDirectory() as directory:
            state_root = Path(directory)
            resolved = StrictBareEpisodeTMDB(
                99045, {0: 4, 1: 4},
                episode_runtimes={0: [1, 1, 1, 1], 1: [24, 24, 24, 24]},
            )
            names = [
                f"Example Show - {episode:02d} (BD 1080p).mkv"
                for episode in range(1, 5)
            ]
            _alist, _state_root, record = self._reconcile_bare_episode_source(
                names,
                resolved,
                state_root=state_root,
                root_task_id="root-runtime-resolved",
            )
            self.assertEqual(record.reconciliation_outcome, "new_work")
            self.assertEqual(
                record.reconciliation_evidence and record.reconciliation_evidence["season"],
                1,
            )

    def test_same_sized_specials_bucket_without_runtime_proof_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            state_root = Path(directory)
            # Specials carry no runtime data: the ambiguity stays closed.
            ambiguous = StrictBareEpisodeTMDB(
                99046, {0: 4, 1: 4},
                episode_runtimes={0: [None, None, None, None], 1: [24, 24, 24, 24]},
            )
            names = [
                f"Example Show - {episode:02d} (BD 1080p).mkv"
                for episode in range(1, 5)
            ]
            _alist, _state_root, record = self._reconcile_bare_episode_source(
                names,
                ambiguous,
                state_root=state_root,
                root_task_id="root-runtime-missing",
            )
            self.assertNotEqual(record.reconciliation_outcome, "new_work")

    def test_same_sized_specials_bucket_with_overlapping_runtimes_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            state_root = Path(directory)
            # Both buckets are full-length: counts and runtimes agree, so the
            # run stays ambiguous.
            same_shape = StrictBareEpisodeTMDB(
                99047, {0: 4, 1: 4},
                episode_runtimes={0: [24, 24, 24, 24], 1: [24, 24, 24, 24]},
            )
            names = [
                f"Example Show - {episode:02d} (BD 1080p).mkv"
                for episode in range(1, 5)
            ]
            _alist, _state_root, record = self._reconcile_bare_episode_source(
                names,
                same_shape,
                state_root=state_root,
                root_task_id="root-runtime-overlap",
            )
            self.assertNotEqual(record.reconciliation_outcome, "new_work")


class SharedBonusDirectoryVocabularyTests(unittest.TestCase):
    def test_f_preclassification_uses_the_shared_bonus_directory_vocabulary(self) -> None:
        """F removes bonus-directory videos by directory context alone.

        EXTRA/[SP05] Picture Drama - 01 carries no theme label of its own,
        yet the EXTRA directory context proves it non-story: the shared
        vocabulary means B/W, D, and F agree it never enters episode
        parsing (轮回七次 F-stage shape).
        """
        from engine.scrapeflow.residual_policy import is_bonus_directory_path
        base = "/incoming/Example/EXTRA"
        self.assertTrue(is_bonus_directory_path(f"{base}/Example [SP05] Picture Drama - 01 (BD).mkv"))
        self.assertTrue(is_bonus_directory_path("/incoming/Example/PV/Example [01].mkv"))
        self.assertTrue(is_bonus_directory_path("/incoming/Example/特典映像/Example [01].mkv"))
        self.assertTrue(is_bonus_directory_path("/incoming/Example/NCOP&ED/NCOP.mkv"))
        # An identically named file outside a bonus directory keeps its
        # ordinary classification.
        self.assertFalse(is_bonus_directory_path("/incoming/Example/Example [SP05] Picture Drama - 01 (BD).mkv"))
        self.assertFalse(is_bonus_directory_path("/incoming/Example/Example - 01 (BD).mkv"))


class SeasonQualifiedContainerTests(LibraryIndexTests):
    def test_season_dir_members_do_not_enter_unqualified_proofs(self) -> None:
        """A root release plus season-organized release folders (间谍过家家).

        The root holds one unqualified ``[01]..[N]`` release whose count is
        the merged sum of every published season; the wrapper carries the
        same show as per-season release folders.  The season-directory
        members are qualified by their hierarchy and must not poison the
        unqualified run proof with cross-release duplicate ordinals.
        """
        with tempfile.TemporaryDirectory() as directory:
            state_root = Path(directory)
            tmdb = StrictBareEpisodeTMDB(
                99048, {0: 1, 1: 4, 2: 2},
            )
            names = [
                f"Example Show [{episode:02d}][1080p].mkv"
                for episode in range(1, 7)
            ] + [
                # Season-organized multi-release wrapper: two editions of S1
                # and one of S2, all with duplicate ordinals.
                "Wrapper/01.第一季/ReleaseA/Example Show [01][1080p].mkv",
                "Wrapper/01.第一季/ReleaseB/Example Show [01][1080p].mkv",
                "Wrapper/02.第二季/ReleaseA/Example Show [01][1080p].mkv",
            ]
            _alist, _state_root, record = self._reconcile_bare_episode_source(
                names,
                tmdb,
                state_root=state_root,
                root_task_id="root-season-qualified-container",
            )
            self.assertEqual(record.reconciliation_outcome, "new_work")
            evidence = record.reconciliation_evidence
            self.assertIsNotNone(evidence)
            assert evidence is not None
            self.assertEqual(evidence["episode_count"], 6)
            self.assertEqual(
                evidence.get("season_boundaries") or [],
                [[1, 4], [2, 2]],
            )

    def test_filename_qualified_mixed_shape_still_fails_closed(self) -> None:
        """An ``SxxExx`` filename beside unqualified files stays ambiguous.

        Qualification must come from the directory hierarchy; a qualified
        NAME mixed into the same directory as unqualified files keeps the
        historical mixed-shape failure.
        """
        with tempfile.TemporaryDirectory() as directory:
            state_root = Path(directory)
            tmdb = StrictBareEpisodeTMDB(99049, {1: 2})
            names = [
                "Example Show E01.mkv",
                "Example Show S01E02.mkv",
            ]
            _alist, _state_root, record = self._reconcile_bare_episode_source(
                names,
                tmdb,
                state_root=state_root,
                root_task_id="root-mixed-qualified-name",
            )
            self.assertNotEqual(record.reconciliation_outcome, "new_work")

    def test_bracket_zero_is_a_special_not_a_run_member(self) -> None:
        """``[00]`` is a prologue coordinate, not part of ``1..N``."""
        with tempfile.TemporaryDirectory() as directory:
            state_root = Path(directory)
            tmdb = StrictBareEpisodeTMDB(99050, {1: 4})
            names = [
                "Example Show [00][1080p].mkv",
            ] + [
                f"Example Show [{episode:02d}][1080p].mkv"
                for episode in range(1, 5)
            ]
            _alist, _state_root, record = self._reconcile_bare_episode_source(
                names,
                tmdb,
                state_root=state_root,
                root_task_id="root-bracket-zero",
            )
            self.assertEqual(record.reconciliation_outcome, "new_work")
            self.assertEqual(
                record.reconciliation_evidence
                and record.reconciliation_evidence["episode_count"],
                4,
            )


class ScopeFreshnessTests(unittest.TestCase):
    """The freshness gate compares per-scope snapshot shapes to the walk."""

    @staticmethod
    def _record(paths: list[str]) -> "WorkUnitRecord":
        return WorkUnitRecord(
            work_unit_id="wu-fresh",
            root_task_id="root-fresh",
            boundary_key="boundary",
            source_paths=tuple(paths),
            source_revision=1,
            role="work",
        )

    def test_directory_scope_with_own_snapshot_row_still_matches(self) -> None:
        """A scope directory's own snapshot row must not break freshness.

        The B snapshot is a walk of the parent root, so a directory scope's
        own row is always among the snapshot rows; ``walk_source_rows``
        never returns the scope itself.  The expected rows must therefore
        follow each scope's snapshot shape instead of blindly including
        every row whose path touches a scope — otherwise every directory
        scope below the root would fail freshness forever.
        """
        scope = "/incoming/root/示例剧"
        alist = IndexAList({
            f"{scope}/示例剧 OVA [01].mkv": b"video",
            f"{scope}/示例剧 OVA [02].mkv": b"video",
        })
        snapshot = {
            "rows": [
                {
                    "full_path": scope,
                    "name": "示例剧",
                    "is_dir": True,
                    "size": 0,
                    "modified": "",
                },
                {
                    "full_path": f"{scope}/示例剧 OVA [01].mkv",
                    "name": "示例剧 OVA [01].mkv",
                    "is_dir": False,
                    "size": 5,
                    "modified": "",
                },
                {
                    "full_path": f"{scope}/示例剧 OVA [02].mkv",
                    "name": "示例剧 OVA [02].mkv",
                    "is_dir": False,
                    "size": 5,
                    "modified": "",
                },
            ]
        }
        self.assertTrue(
            _fresh_scopes_match_snapshot(alist, snapshot, self._record([scope]))
        )

    def test_flat_split_file_scope_still_matches(self) -> None:
        """A flat-split file scope compares its exact snapshot file row."""
        scope = "/incoming/root/Example Show [01].mkv"
        alist = IndexAList({
            scope: b"video",
            "/incoming/root/Example Show [02].mkv": b"video",
        })
        snapshot = {
            "rows": [
                {
                    "full_path": scope,
                    "name": "Example Show [01].mkv",
                    "is_dir": False,
                    "size": 5,
                    "modified": "",
                },
                {
                    "full_path": "/incoming/root/Example Show [02].mkv",
                    "name": "Example Show [02].mkv",
                    "is_dir": False,
                    "size": 5,
                    "modified": "",
                },
            ]
        }
        self.assertTrue(
            _fresh_scopes_match_snapshot(alist, snapshot, self._record([scope]))
        )

    def test_directory_scope_drift_fails_closed(self) -> None:
        """A file that appeared inside the scope after B must fail freshness."""
        scope = "/incoming/root/示例剧"
        alist = IndexAList({
            f"{scope}/示例剧 OVA [01].mkv": b"video",
            f"{scope}/示例剧 OVA [02].mkv": b"video",
        })
        snapshot = {
            "rows": [
                {
                    "full_path": scope,
                    "name": "示例剧",
                    "is_dir": True,
                    "size": 0,
                    "modified": "",
                },
                {
                    "full_path": f"{scope}/示例剧 OVA [01].mkv",
                    "name": "示例剧 OVA [01].mkv",
                    "is_dir": False,
                    "size": 0,
                    "modified": "",
                },
            ]
        }
        self.assertFalse(
            _fresh_scopes_match_snapshot(alist, snapshot, self._record([scope]))
        )
