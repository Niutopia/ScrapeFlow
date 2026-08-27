"""Tests for the D-node three-shelf LibraryIndex and five-way reconciliation."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

from engine.scrapeflow.root_boundaries import analyze_root_boundaries
from engine.scrapeflow.unit_identity import apply_work_unit_override
from engine.scrapeflow.work_units import load_work_unit_records

from local.scrapeflow_api.library_index import (
    SingleSeasonEpisodeProof,
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


class StrictBareEpisodeTMDB:
    """TMDB double exposing both show-detail and published season evidence."""

    def __init__(
        self,
        tmdb_id: int,
        seasons: dict[int, int],
        *,
        payload_counts: dict[int, int] | None = None,
    ) -> None:
        self.tmdb_id = tmdb_id
        self.seasons = dict(seasons)
        self.payload_counts = dict(payload_counts or seasons)

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
                return {
                    "episodes": [
                        {
                            "episode_number": episode,
                            "air_date": "2020-01-01",
                            "name": f"Episode {episode}",
                        }
                        for episode in range(1, count + 1)
                    ],
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

    def test_naked_numeric_proof_ignores_an_empty_future_tmdb_season(self) -> None:
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
