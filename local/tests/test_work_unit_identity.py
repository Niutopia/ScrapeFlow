"""Comprehensive test suite for Phase 3: WorkUnit Identity & Evidence Matching.

Tests domain models (EpisodePattern, IdentityEvidence, WorkUnitRecord),
evidence extraction, serialization, atomic persistence, and confidence-scored
TMDB matching with disambiguation, parent-clue weighting, and ambiguity handling.
"""

from __future__ import annotations

import re
import tempfile
import unittest
from pathlib import Path
from typing import Any, Mapping

from engine.scrapeflow.boundary_analysis import (
    BoundaryEvidence,
    DirectoryRole,
    WorkCandidate,
)
from engine.scrapeflow.errors import PlanError
from engine.scrapeflow.identity_matching import (
    AutoMatchAmbiguityError,
    auto_match_from_evidence,
)
from engine.scrapeflow.source_inventory import SourceFile, SourceNode
from engine.scrapeflow.work_units import (
    EpisodePattern,
    IdentityEvidence,
    WorkUnitRecord,
    create_work_units_from_candidates,
    extract_episode_pattern,
    extract_identity_evidence,
    load_work_unit_records,
    save_work_unit_records,
)


class FakeTMDBClient:
    """Mock TMDB client with configurable search results and details."""

    def __init__(
        self,
        *,
        search_results: dict[str, list[dict[str, Any]]] | None = None,
        details: dict[str, dict[str, Any]] | None = None,
        alternative_titles: dict[str, list[dict[str, Any]]] | None = None,
    ) -> None:
        self.search_results = search_results or {}
        self.details = details or {}
        self.alternative_titles = alternative_titles or {}
        self.call_log: list[tuple[str, dict[str, Any]]] = []

    def get(self, path: str, **params: Any) -> dict[str, Any]:
        self.call_log.append((path, params))
        if path.startswith("/search/"):
            q = str(params.get("query", ""))
            if q in self.search_results:
                return {"results": self.search_results[q]}
            q_clean = re.sub(r"[^\w\u3400-\u9fff]+", "", q.lower())
            for k, v in self.search_results.items():
                k_clean = re.sub(r"[^\w\u3400-\u9fff]+", "", k.lower())
                if k_clean == q_clean or (k_clean and k_clean in q_clean) or (q_clean and q_clean in k_clean):
                    return {"results": v}
            return {"results": []}
        if path in self.details:
            return self.details[path]
        if path.endswith("/alternative_titles"):
            key = path.split("/")[2] if len(path.split("/")) > 2 else ""
            if key in self.alternative_titles:
                return {"results": self.alternative_titles[key], "titles": self.alternative_titles[key]}
            return {"results": [], "titles": []}
        return {}


# ---------------------------------------------------------------------------
# Test Cases
# ---------------------------------------------------------------------------

class TestEpisodePattern(unittest.TestCase):
    """Tests for EpisodePattern extraction and serialization."""

    def test_extract_standard_season_episode(self) -> None:
        files = [
            SourceFile("/path/Show.S01E01.mkv", "Show.S01E01.mkv", 1000, "video", ""),
            SourceFile("/path/Show.S01E02.mkv", "Show.S01E02.mkv", 1000, "video", ""),
            SourceFile("/path/Show.S02E01.mkv", "Show.S02E01.mkv", 1000, "video", ""),
        ]
        pattern = extract_episode_pattern(files)
        self.assertIsNotNone(pattern)
        self.assertEqual(pattern.season_numbers, (1, 2))
        self.assertEqual(pattern.episode_numbers, (1, 2))
        self.assertEqual(pattern.total_episodes, 2)
        self.assertFalse(pattern.has_specials)

    def test_extract_specials_and_ova(self) -> None:
        files = [
            "Show.S00E01.mkv",
            "Show.SP01.mkv",
            "Show OVA 02.mp4",
            "Show 特别篇.mkv",
        ]
        pattern = extract_episode_pattern(files)
        self.assertIsNotNone(pattern)
        self.assertTrue(pattern.has_specials)

    def test_extract_standalone_episode_tokens(self) -> None:
        files = [
            "MyShow EP01 [1080p].mkv",
            "MyShow EP02 [1080p].mkv",
            "MyShow 第03集.mp4",
            "MyShow [04].mkv",
        ]
        pattern = extract_episode_pattern(files)
        self.assertIsNotNone(pattern)
        self.assertEqual(pattern.episode_numbers, (1, 2, 3, 4))
        self.assertEqual(pattern.total_episodes, 4)

    def test_avoids_resolution_and_codec_false_positives(self) -> None:
        files = [
            "Movie.Title.2023.1080p.x264.AAC.mkv",
            "Sample.2160p.HEVC.mkv",
        ]
        pattern = extract_episode_pattern(files)
        # Resolutions 1080/2160 and codecs should not trigger episode recognition
        self.assertIsNone(pattern)

    def test_empty_or_no_video_files(self) -> None:
        self.assertIsNone(extract_episode_pattern([]))
        files = [
            SourceFile("/path/subs.ass", "subs.ass", 100, "subtitle", ""),
        ]
        self.assertIsNone(extract_episode_pattern(files))

    def test_episode_pattern_serialization_roundtrip(self) -> None:
        p1 = EpisodePattern(
            season_numbers=(1, 2),
            episode_numbers=(1, 2, 3),
            total_episodes=3,
            has_specials=True,
        )
        d = p1.as_dict()
        p2 = EpisodePattern.from_dict(d)
        self.assertEqual(p1, p2)


class TestIdentityEvidence(unittest.TestCase):
    """Tests for IdentityEvidence extraction and serialization."""

    def test_extract_evidence_for_movie_candidate(self) -> None:
        evidence_boundary = BoundaryEvidence(
            role=DirectoryRole.SINGLE_WORK,
            confidence=0.85,
            reasons=("Single video file",),
            competing_roles=(),
        )
        candidate = WorkCandidate(
            work_unit_id="unit-movie-1",
            boundary_key="/待刮削/Inception (2010)",
            source_paths=("/待刮削/Inception (2010)",),
            display_label="Inception (2010)",
            proposed_media_context="movie",
            boundary_evidence=evidence_boundary,
        )
        node = SourceNode(
            path="/待刮削/Inception (2010)",
            name="Inception (2010)",
            files=(
                SourceFile("/待刮削/Inception (2010)/Inception.2010.1080p.mkv", "Inception.2010.1080p.mkv", 2000000000, "video", ""),
            ),
            children=(),
            depth=0,
        )
        evidence = extract_identity_evidence(candidate, node)
        self.assertEqual(evidence.work_unit_id, "unit-movie-1")
        self.assertEqual(evidence.boundary_label, "Inception (2010)")
        self.assertIn(2010, evidence.years)
        self.assertEqual(evidence.media_shape, "movie")
        self.assertIn("Inception", evidence.normalized_titles)

    def test_extract_evidence_for_series_container_child(self) -> None:
        evidence_boundary = BoundaryEvidence(
            role=DirectoryRole.SERIES_CONTAINER,
            confidence=0.90,
            reasons=("Multiple titled subdirs",),
            competing_roles=(),
        )
        candidate = WorkCandidate(
            work_unit_id="unit-fate-zero",
            boundary_key="/待刮削/Fate Series/Fate Zero (2011)",
            source_paths=("/待刮削/Fate Series/Fate Zero (2011)",),
            display_label="Fate Zero (2011)",
            proposed_media_context="tv",
            boundary_evidence=evidence_boundary,
        )
        node = SourceNode(
            path="/待刮削/Fate Series/Fate Zero (2011)",
            name="Fate Zero (2011)",
            files=(
                SourceFile("/p/Fate.Zero.S01E01.mkv", "Fate.Zero.S01E01.mkv", 500000000, "video", ""),
                SourceFile("/p/Fate.Zero.S01E02.mkv", "Fate.Zero.S01E02.mkv", 500000000, "video", ""),
            ),
            children=(),
            depth=1,
        )
        evidence = extract_identity_evidence(candidate, node, parent_labels=["Fate Series"])
        self.assertEqual(evidence.parent_labels, ("Fate Series",))
        self.assertIn(2011, evidence.years)
        self.assertIsNotNone(evidence.episode_pattern)
        self.assertEqual(evidence.episode_pattern.total_episodes, 2)
        self.assertEqual(evidence.media_shape, "tv")

    def test_evidence_serialization_roundtrip(self) -> None:
        ep = EpisodePattern(season_numbers=(1,), episode_numbers=(1, 2), total_episodes=2, has_specials=False)
        ev1 = IdentityEvidence(
            work_unit_id="unit-123",
            boundary_label="My Show",
            parent_labels=("Parent Box",),
            representative_names=("My Show", "My Show S01"),
            normalized_titles=("My Show",),
            years=(2022,),
            episode_pattern=ep,
            media_shape="tv",
            aliases=("Show Alias",),
        )
        d = ev1.as_dict()
        ev2 = IdentityEvidence.from_dict(d)
        self.assertEqual(ev1, ev2)


class TestWorkUnitRecordPersistence(unittest.TestCase):
    """Tests for WorkUnitRecord creation, state lifecycle, and atomic persistence."""

    def test_create_work_units_from_candidates(self) -> None:
        boundary_ev = BoundaryEvidence(
            role=DirectoryRole.SINGLE_WORK,
            confidence=0.8,
            reasons=(),
            competing_roles=(),
        )
        candidates = [
            WorkCandidate(
                work_unit_id="wu-1",
                boundary_key="/path/1",
                source_paths=("/path/1",),
                display_label="Show 1",
                proposed_media_context="tv",
                boundary_evidence=boundary_ev,
            ),
            WorkCandidate(
                work_unit_id="wu-2",
                boundary_key="/path/2",
                source_paths=("/path/2",),
                display_label="Show 2",
                proposed_media_context="tv",
                boundary_evidence=boundary_ev,
            ),
        ]
        records = create_work_units_from_candidates(candidates, root_task_id="root-100", source_revision=2)
        self.assertEqual(len(records), 2)
        self.assertEqual(records[0].work_unit_id, "wu-1")
        self.assertEqual(records[0].root_task_id, "root-100")
        self.assertEqual(records[0].source_revision, 2)
        self.assertEqual(records[0].identity_status, "pending")
        self.assertIsNone(records[0].identity)

    def test_save_and_load_work_unit_records_roundtrip(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            state_dir = Path(tmp_dir)
            rec = WorkUnitRecord(
                work_unit_id="wu-test-1",
                root_task_id="root-abc",
                boundary_key="/source/test",
                source_paths=("/source/test",),
                source_revision=1,
                role="single_work",
                identity_status="confirmed",
                identity={"media_type": "movie", "tmdb_id": 999, "title": "Test Movie"},
                candidate_identities=({"media_type": "movie", "tmdb_id": 999},),
                reconciliation_outcome="new_work",
                matched_work_root=None,
                attention=None,
            )
            save_work_unit_records(state_dir, "root-abc", [rec])
            loaded = load_work_unit_records(state_dir, "root-abc")
            self.assertEqual(len(loaded), 1)
            self.assertEqual(loaded[0].work_unit_id, "wu-test-1")
            self.assertEqual(loaded[0].identity_status, "confirmed")
            self.assertEqual(loaded[0].identity["title"], "Test Movie")

    def test_load_nonexistent_returns_empty(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            loaded = load_work_unit_records(Path(tmp_dir), "nonexistent-root")
            self.assertEqual(loaded, [])


class TestAutoMatchFromEvidence(unittest.TestCase):
    """Tests for TMDB auto-matching using IdentityEvidence."""

    def test_disambiguates_same_title_by_year(self) -> None:
        # Two TMDB results with exact same title but different years
        client = FakeTMDBClient(
            search_results={
                "Fate/stay night": [
                    {
                        "id": 100,
                        "name": "Fate/stay night",
                        "first_air_date": "2006-01-06",
                        "genre_ids": [16],
                    },
                    {
                        "id": 200,
                        "name": "Fate/stay night: Unlimited Blade Works",
                        "first_air_date": "2014-10-04",
                        "genre_ids": [16],
                    },
                ],
            },
            details={
                "/tv/100": {"number_of_episodes": 24},
                "/tv/200": {"number_of_episodes": 26},
            },
        )
        evidence = IdentityEvidence(
            work_unit_id="wu-fate-2014",
            boundary_label="Fate stay night (2014)",
            parent_labels=(),
            representative_names=("Fate stay night",),
            normalized_titles=("Fate stay night",),
            years=(2014,),
            episode_pattern=EpisodePattern(season_numbers=(1,), episode_numbers=tuple(range(1, 27)), total_episodes=26, has_specials=False),
            media_shape="tv",
            aliases=(),
        )
        best, candidates = auto_match_from_evidence(client, evidence)
        self.assertEqual(best.tmdb_id, 200)
        self.assertEqual(best.year, "2014")
        self.assertEqual(best.status, "confirmed")

    def test_chinese_to_english_alias_matching(self) -> None:
        client = FakeTMDBClient(
            search_results={
                "进击的巨人": [
                    {
                        "id": 1429,
                        "name": "Attack on Titan",
                        "first_air_date": "2013-04-07",
                        "genre_ids": [16],
                    }
                ],
                "Attack on Titan": [
                    {
                        "id": 1429,
                        "name": "Attack on Titan",
                        "first_air_date": "2013-04-07",
                        "genre_ids": [16],
                    }
                ],
            },
            alternative_titles={
                "1429": [
                    {"title": "进击的巨人", "iso_3166_1": "CN"},
                    {"title": "Shingeki no Kyojin", "iso_3166_1": "JP"},
                ]
            },
        )
        evidence = IdentityEvidence(
            work_unit_id="wu-aot",
            boundary_label="进击的巨人 (2013)",
            parent_labels=(),
            representative_names=("进击的巨人",),
            normalized_titles=("进击的巨人",),
            years=(2013,),
            episode_pattern=None,
            media_shape="tv",
            aliases=(),
        )
        best, candidates = auto_match_from_evidence(client, evidence)
        self.assertEqual(best.tmdb_id, 1429)
        self.assertEqual(best.status, "confirmed")

    def test_parent_container_reinforcement(self) -> None:
        # Child boundary label is just "Zero", parent is "Fate"
        client = FakeTMDBClient(
            search_results={
                "Zero": [
                    {
                        "id": 501,
                        "name": "Zero",
                        "first_air_date": "2020-01-01",
                        "genre_ids": [18],
                    },
                ],
                "Fate Zero": [
                    {
                        "id": 35507,
                        "name": "Fate/Zero",
                        "first_air_date": "2011-10-02",
                        "genre_ids": [16],
                    },
                ],
            },
        )
        evidence = IdentityEvidence(
            work_unit_id="wu-fate-zero",
            boundary_label="Zero",
            parent_labels=("Fate",),
            representative_names=("Zero",),
            normalized_titles=("Zero",),
            years=(2011,),
            episode_pattern=None,
            media_shape="tv",
            aliases=(),
        )
        best, candidates = auto_match_from_evidence(client, evidence)
        self.assertEqual(best.tmdb_id, 35507)
        self.assertEqual(best.title, "Fate/Zero")

    def test_ambiguity_raises_automatch_ambiguity_error(self) -> None:
        # Two candidates with very close scores and neither exact
        client = FakeTMDBClient(
            search_results={
                "Mystery Show": [
                    {
                        "id": 1,
                        "name": "Mystery Show Alpha",
                        "first_air_date": "2020-01-01",
                        "genre_ids": [18],
                    },
                    {
                        "id": 2,
                        "name": "Mystery Show Beta",
                        "first_air_date": "2020-01-01",
                        "genre_ids": [18],
                    },
                ],
            },
        )
        evidence = IdentityEvidence(
            work_unit_id="wu-ambiguous",
            boundary_label="Mystery Show",
            parent_labels=(),
            representative_names=("Mystery Show",),
            normalized_titles=("Mystery Show",),
            years=(2020,),
            episode_pattern=None,
            media_shape="tv",
            aliases=(),
        )
        with self.assertRaises(AutoMatchAmbiguityError) as ctx:
            auto_match_from_evidence(client, evidence)
        self.assertIn("无法区分", str(ctx.exception))
        self.assertEqual(len(ctx.exception.candidates), 2)

    def test_multi_work_unit_independence(self) -> None:
        # In a series container with 2 WorkUnits: WorkUnit 1 succeeds, WorkUnit 2 is ambiguous
        client = FakeTMDBClient(
            search_results={
                "Fate Stay Night": [
                    {
                        "id": 100,
                        "name": "Fate/stay night",
                        "first_air_date": "2006-01-06",
                        "genre_ids": [16],
                    },
                ],
                "Unknown Extra": [
                    {
                        "id": 881,
                        "name": "Extra One",
                        "first_air_date": "2020-01-01",
                        "genre_ids": [18],
                    },
                    {
                        "id": 882,
                        "name": "Extra Two",
                        "first_air_date": "2020-01-01",
                        "genre_ids": [18],
                    },
                ],
            },
        )
        evidence1 = IdentityEvidence(
            work_unit_id="wu-1",
            boundary_label="Fate Stay Night",
            parent_labels=("Fate Series",),
            representative_names=("Fate Stay Night",),
            normalized_titles=("Fate Stay Night",),
            years=(2006,),
            episode_pattern=None,
            media_shape="tv",
            aliases=(),
        )
        evidence2 = IdentityEvidence(
            work_unit_id="wu-2",
            boundary_label="Unknown Extra",
            parent_labels=("Fate Series",),
            representative_names=("Unknown Extra",),
            normalized_titles=("Unknown Extra",),
            years=(2020,),
            episode_pattern=None,
            media_shape="tv",
            aliases=(),
        )

        # WorkUnit 1 matches cleanly
        best1, cands1 = auto_match_from_evidence(client, evidence1)
        self.assertEqual(best1.tmdb_id, 100)
        self.assertEqual(best1.status, "confirmed")

        # WorkUnit 2 raises ambiguity error independently
        with self.assertRaises(AutoMatchAmbiguityError):
            auto_match_from_evidence(client, evidence2)

        # Ensure WorkUnit 1 is unaffected and can be persisted independently
        rec1 = WorkUnitRecord(
            work_unit_id=evidence1.work_unit_id,
            root_task_id="root-multi",
            boundary_key="/path/fate",
            source_paths=("/path/fate",),
            source_revision=1,
            role="single_work",
            identity_status="confirmed",
            identity={"tmdb_id": best1.tmdb_id, "title": best1.title},
            candidate_identities=tuple({"tmdb_id": c.tmdb_id} for c in cands1),
        )
        rec2 = WorkUnitRecord(
            work_unit_id=evidence2.work_unit_id,
            root_task_id="root-multi",
            boundary_key="/path/extra",
            source_paths=("/path/extra",),
            source_revision=1,
            role="single_work",
            identity_status="uncertain",
            identity=None,
            candidate_identities=(),
        )
        with tempfile.TemporaryDirectory() as tmp_dir:
            save_work_unit_records(Path(tmp_dir), "root-multi", [rec1, rec2])
            loaded = load_work_unit_records(Path(tmp_dir), "root-multi")
            self.assertEqual(len(loaded), 2)
            self.assertEqual(loaded[0].identity_status, "confirmed")
            self.assertEqual(loaded[1].identity_status, "uncertain")

    def test_excluded_tmdb_ids_are_skipped(self) -> None:
        client = FakeTMDBClient(
            search_results={
                "My Show": [
                    {"id": 101, "name": "My Show", "first_air_date": "2022-01-01", "genre_ids": [16]},
                    {"id": 102, "name": "My Show Special Edition", "first_air_date": "2022-01-01", "genre_ids": [16]},
                ]
            }
        )
        evidence = IdentityEvidence(
            work_unit_id="wu-excl",
            boundary_label="My Show",
            parent_labels=(),
            representative_names=("My Show",),
            normalized_titles=("My Show",),
            years=(2022,),
            episode_pattern=None,
            media_shape="tv",
            aliases=(),
        )
        best, _ = auto_match_from_evidence(client, evidence, excluded_tmdb_ids={101})
        self.assertEqual(best.tmdb_id, 102)

    def test_prefer_animation_penalizes_live_action(self) -> None:
        client = FakeTMDBClient(
            search_results={
                "Avatar": [
                    {"id": 201, "name": "Avatar: The Last Airbender", "first_air_date": "2005-02-21", "genre_ids": [16]},
                    {"id": 202, "name": "Avatar", "first_air_date": "2024-02-22", "genre_ids": [18, 10759]},
                ]
            }
        )
        evidence = IdentityEvidence(
            work_unit_id="wu-avatar",
            boundary_label="Avatar",
            parent_labels=(),
            representative_names=("Avatar",),
            normalized_titles=("Avatar",),
            years=(),
            episode_pattern=None,
            media_shape="tv",
            aliases=(),
        )
        best, _ = auto_match_from_evidence(client, evidence, prefer_animation=True)
        self.assertEqual(best.tmdb_id, 201)

    def test_expected_episode_count_matching(self) -> None:
        client = FakeTMDBClient(
            search_results={
                "Steins Gate": [
                    {"id": 301, "name": "Steins;Gate", "first_air_date": "2011-04-06", "genre_ids": [16]},
                    {"id": 302, "name": "Steins;Gate 0", "first_air_date": "2018-04-12", "genre_ids": [16]},
                ]
            },
            details={
                "/tv/301": {"number_of_episodes": 24},
                "/tv/302": {"number_of_episodes": 23},
            }
        )
        evidence = IdentityEvidence(
            work_unit_id="wu-steins",
            boundary_label="Steins Gate",
            parent_labels=(),
            representative_names=("Steins Gate",),
            normalized_titles=("Steins Gate",),
            years=(),
            episode_pattern=EpisodePattern(season_numbers=(1,), episode_numbers=tuple(range(1, 25)), total_episodes=24, has_specials=False),
            media_shape="tv",
            aliases=(),
        )
        best, _ = auto_match_from_evidence(client, evidence)
        self.assertEqual(best.tmdb_id, 301)

    def test_invalid_input_validation(self) -> None:
        client = FakeTMDBClient()
        evidence_empty = IdentityEvidence(
            work_unit_id="wu-bad",
            boundary_label="   ",
            parent_labels=(),
            representative_names=(),
            normalized_titles=(),
            years=(),
            episode_pattern=None,
            media_shape="tv",
            aliases=(),
        )
        with self.assertRaises(PlanError) as ctx:
            auto_match_from_evidence(client, evidence_empty)
        self.assertIn("为空", str(ctx.exception))

        evidence_valid = IdentityEvidence(
            work_unit_id="wu-ok",
            boundary_label="Valid Title",
            parent_labels=(),
            representative_names=(),
            normalized_titles=(),
            years=(),
            episode_pattern=None,
            media_shape="tv",
            aliases=(),
        )
        with self.assertRaises(PlanError) as ctx2:
            auto_match_from_evidence(client, evidence_valid, min_confidence=1.5)
        self.assertIn("最低置信度必须在 0 到 1 之间", str(ctx2.exception))


if __name__ == "__main__":
    unittest.main()
