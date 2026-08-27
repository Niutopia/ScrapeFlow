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
from unittest.mock import patch

from engine.scrapeflow.boundary_analysis import (
    BoundaryEvidence,
    DirectoryRole,
    WorkCandidate,
)
from engine.scrapeflow.errors import PlanError
from engine.scrapeflow.identity_matching import (
    AutoMatchAmbiguityError,
    _clean_boundary_identity_query,
    _search_query_variants,
    _script_evidence_text,
    _title_from_representative_episode_filename,
    _usable_representative_identity_query,
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

    def test_extract_physical_oad_run_keeps_release_context_separate(self) -> None:
        """OAD ordinals are evidence, never implicit Season 00/01 tokens."""
        evidence_boundary = BoundaryEvidence(
            role=DirectoryRole.SPECIAL_GROUP,
            confidence=0.9,
            reasons=("explicit physical OAD files",),
            competing_roles=(),
        )
        candidate = WorkCandidate(
            work_unit_id="wu-oad-run",
            boundary_key="/incoming/Show OAD",
            source_paths=("/incoming/Show OAD",),
            display_label="Show OAD",
            proposed_media_context="tv",
            boundary_evidence=evidence_boundary,
        )
        node = SourceNode(
            path="/incoming/Show OAD",
            name="Show OAD",
            files=tuple(
                SourceFile(
                    f"/incoming/Show OAD/Show [OAD{number:02d}].mkv",
                    f"Show [OAD{number:02d}].mkv",
                    1_000,
                    "video",
                    "",
                )
                for number in range(1, 6)
            ),
            children=(),
            depth=0,
        )
        evidence = extract_identity_evidence(candidate, node)
        self.assertEqual(evidence.special_markers, ("OAD",))
        self.assertEqual(evidence.special_episode_numbers, (1, 2, 3, 4, 5))
        self.assertEqual(evidence.special_episode_count, 5)
        self.assertTrue(evidence.special_numbered_run_complete)
        self.assertTrue(evidence.episode_pattern.has_specials)

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

    def test_meaningful_cjk_tv_boundary_marks_strict_naked_numeric_run(self) -> None:
        """``01.mp4`` … ``12.mp4`` are shape only, never coordinates/titles."""
        label = "B 有意义中文剧名（2024）全12集 1080P"
        candidate = WorkCandidate(
            work_unit_id="unit-cjk-naked-run",
            boundary_key=f"/待刮削/{label}",
            source_paths=(f"/待刮削/{label}",),
            display_label=label,
            proposed_media_context="tv",
            boundary_evidence=BoundaryEvidence(
                role=DirectoryRole.SINGLE_WORK,
                confidence=0.75,
                reasons=("root episode pack",),
                competing_roles=(),
            ),
        )
        node = SourceNode(
            path=f"/待刮削/{label}",
            name=label,
            files=tuple(
                SourceFile(
                    f"/待刮削/{label}/{episode:02d}.mp4",
                    f"{episode:02d}.mp4",
                    1000,
                    "video",
                    "",
                )
                for episode in range(1, 13)
            ) + (
                SourceFile(
                    f"/待刮削/{label}/资源文档.docx",
                    "资源文档.docx",
                    100,
                    "other",
                    "",
                ),
            ),
            children=(),
            depth=0,
        )

        evidence = extract_identity_evidence(candidate, node)

        self.assertEqual(evidence.representative_names, (label,))
        self.assertIsNone(evidence.episode_pattern)
        self.assertTrue(evidence.strict_naked_numeric_video_run)
        self.assertTrue(evidence.naked_numeric_cjk_release_eligible)

        # Future broadening of the generic parser must not synthesize season
        # one from this filename shape either.
        with patch(
            "engine.scrapeflow.work_units.extract_episode_pattern",
            return_value=EpisodePattern(
                season_numbers=(1,),
                episode_numbers=tuple(range(1, 13)),
                total_episodes=12,
                has_specials=False,
            ),
        ):
            guarded = extract_identity_evidence(candidate, node)
        self.assertIsNone(guarded.episode_pattern)
        self.assertTrue(guarded.strict_naked_numeric_video_run)
        self.assertTrue(guarded.naked_numeric_cjk_release_eligible)

    def test_naked_numeric_structure_rejects_generic_noisy_or_nonexact_boundaries(self) -> None:
        def candidate_and_node(
            label: str,
            names: list[str],
            *,
            media_shape: str = "tv",
        ) -> tuple[WorkCandidate, SourceNode]:
            candidate = WorkCandidate(
                work_unit_id=f"unit-{label}",
                boundary_key=f"/待刮削/{label}",
                source_paths=(f"/待刮削/{label}",),
                display_label=label,
                proposed_media_context=media_shape,
                boundary_evidence=BoundaryEvidence(
                    role=DirectoryRole.SINGLE_WORK,
                    confidence=0.75,
                    reasons=(),
                    competing_roles=(),
                ),
            )
            node = SourceNode(
                path=f"/待刮削/{label}",
                name=label,
                files=tuple(
                    SourceFile(
                        f"/待刮削/{label}/{name}", name, 1000, "video", ""
                    )
                    for name in names
                ),
                children=(),
                depth=0,
            )
            return candidate, node

        cases = (
            (
                "全12集 1080P",
                [f"{episode:02d}.mp4" for episode in range(1, 13)],
                "tv",
                True,
            ),
            (
                "有意义中文剧名（2024）全12集",
                ["01.mp4", "02.mp4", "04.mp4", "05.mp4"],
                "tv",
                False,
            ),
            (
                "有意义中文剧名（2024）全12集",
                ["01.mp4", "02.mp4", "03.mp4", "trailer.mp4"],
                "tv",
                False,
            ),
            (
                "有意义中文剧名（2024）全12集",
                [f"{episode:02d}.mp4" for episode in range(1, 13)],
                "unknown",
                True,
            ),
            (
                "有意义中文剧名 全12集",
                [f"{episode:02d}.mp4" for episode in range(1, 13)],
                "tv",
                True,
            ),
        )
        for label, names, media_shape, expected_strict_run in cases:
            with self.subTest(label=label, media_shape=media_shape, names=names):
                candidate, node = candidate_and_node(
                    label, names, media_shape=media_shape,
                )
                evidence = extract_identity_evidence(candidate, node)
                self.assertIsNone(evidence.episode_pattern)
                self.assertEqual(
                    evidence.strict_naked_numeric_video_run,
                    expected_strict_run,
                )
                self.assertFalse(evidence.naked_numeric_cjk_release_eligible)
                self.assertNotIn("01", evidence.representative_names)

    def test_boundary_clean_query_keeps_raw_label_and_only_strips_release_tails(self) -> None:
        raw = "B 有意义中文剧名（2024）全12集 1080P"
        self.assertEqual(
            _clean_boundary_identity_query(raw),
            "有意义中文剧名",
        )
        self.assertEqual(
            _script_evidence_text(raw),
            "有意义中文剧名",
        )
        # Arbitrary bracketed Latin text is not a quality tail and must remain
        # available to the normal cross-script evidence guard.
        self.assertEqual(
            _clean_boundary_identity_query("B 有意义中文剧名 [Meaningful Show]"),
            "有意义中文剧名 [Meaningful Show]",
        )

    def test_boundary_clean_query_strips_year_and_mid_quality_metadata(self) -> None:
        """A title-bearing folder with a disambiguating year must still match."""
        self.assertEqual(
            _clean_boundary_identity_query(
                "钢之炼金术师（2003）全51集 1080P",
            ),
            "钢之炼金术师",
        )
        self.assertEqual(
            _clean_boundary_identity_query(
                "钢之炼金术师 FA（2009）4K超清2160P收藏版 内封中文字幕",
            ),
            "钢之炼金术师 FA",
        )
        self.assertEqual(
            _clean_boundary_identity_query("01 寒蝉鸣泣之时（2006）全26集+OVA"),
            "寒蝉鸣泣之时",
        )

    def test_boundary_clean_query_strips_streaming_technical_parenthetical(self) -> None:
        """Technical release fingerprints must not crowd out the title query."""
        self.assertEqual(
            _clean_boundary_identity_query(
                "瑞克和莫蒂：日漫版（2024）全10集 日英双语 内封简中字幕 "
                "1080P（AMZN.WEB-DL.AVC.DDP.2.0）",
            ),
            "瑞克和莫蒂:日漫版",
        )

    def test_boundary_clean_query_strips_genre_bucket_dotted_title_and_season_span(self) -> None:
        """Container labels such as ``【美剧】金.斯.敦.市.长 1-3季``
        must yield the ordinary title query without an identity override."""
        self.assertEqual(
            _clean_boundary_identity_query("【美剧】金.斯.敦.市.长 1-3季"),
            "金斯敦市长",
        )
        self.assertEqual(
            _clean_boundary_identity_query("金.斯.敦.市.长 S01-S03"),
            "金斯敦市长",
        )

    def test_boundary_clean_query_strips_full_subtitle_language_label(self) -> None:
        """Full ``简体内嵌`` packaging labels must not become title evidence."""
        self.assertEqual(
            _clean_boundary_identity_query("末日三问.简体内嵌4K"),
            "末日三问",
        )

    def test_boundary_clean_query_unwraps_bracket_only_release_titles(self) -> None:
        """A bracket-only release carries the work title inside ``[]``.

        The clean query must unwrap the CJK title group and drop the
        group/codec/packaging brackets instead of sending the raw release
        fingerprint to TMDB.
        """
        self.assertEqual(
            _clean_boundary_identity_query(
                "[DBD-Raws][大剑][01-26TV全集+特典映像][1080P][BDRip]"
                "[HEVC-10bit][简繁外挂][FLAC][MKV]",
            ),
            "大剑",
        )
        self.assertEqual(
            _clean_boundary_identity_query(
                "[VCB-Studio] 轮回七次的恶役千金 10-bit 1080p HEVC BDRip [Fin]",
            ),
            "轮回七次的恶役千金",
        )
        self.assertEqual(
            _clean_boundary_identity_query(
                "[DBD-Raws][物理魔法使马修 神觉者候补选拔试验篇]"
                "[01-12TV全集][美版][1080P][BDRip]",
            ),
            "物理魔法使马修神觉者候补选拔试验篇",
        )
        # A multi-word bracketed Latin alias is still title evidence and
        # stays verbatim; a bare ordinal bracket never becomes a query.
        self.assertEqual(
            _clean_boundary_identity_query("B 有意义中文剧名 [Meaningful Show]"),
            "有意义中文剧名 [Meaningful Show]",
        )

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

    def test_naked_numeric_representatives_never_become_movie_queries(self) -> None:
        """A meaningful CJK boundary remains the identity query for a TV pack."""
        label = "B 有意义中文剧名（2024）全12集 1080P"
        client = FakeTMDBClient(
            search_results={
                "有意义中文剧名": [{
                    "id": 1201,
                    "name": "有意义中文剧名",
                    "first_air_date": "2024-07-01",
                    "genre_ids": [16],
                }],
                "01": [{
                    "id": 1202,
                    "title": "01",
                    "release_date": "2024-01-01",
                    "genre_ids": [18],
                }],
            },
            details={"/tv/1201": {"number_of_episodes": 12}},
        )
        evidence = IdentityEvidence(
            work_unit_id="wu-naked-numeric-cjk",
            boundary_label=label,
            parent_labels=(),
            representative_names=(label, *(f"{episode:02d}" for episode in range(1, 13))),
            normalized_titles=(label,),
            years=(2024,),
            episode_pattern=None,
            media_shape="tv",
            aliases=(),
            strict_naked_numeric_video_run=True,
            naked_numeric_cjk_release_eligible=True,
        )

        best, _ = auto_match_from_evidence(client, evidence, prefer_animation=True)

        self.assertEqual((best.media_type, best.tmdb_id), ("tv", 1201))
        self.assertIsNone(best.decision_trace["expected_episode_count"])
        self.assertTrue(
            best.decision_trace["naked_numeric_same_script_exact_title_or_alias"]
        )
        self.assertTrue(best.decision_trace["naked_numeric_exact_year"])
        search_queries = [
            str(params.get("query"))
            for path, params in client.call_log
            if path.startswith("/search/")
        ]
        self.assertNotIn("01", search_queries)
        self.assertFalse(any(query.isdigit() for query in search_queries))

    def test_naked_numeric_cjk_evidence_does_not_weaken_cross_script_guard(self) -> None:
        """A CJK boundary still needs an official same-script title or alias."""
        label = "B 有意义中文剧名（2024）全12集 1080P"
        client = FakeTMDBClient(
            search_results={
                "有意义中文剧名": [{
                    "id": 1203,
                    "name": "Meaningful Show",
                    "first_air_date": "2024-07-01",
                    "genre_ids": [16],
                }],
            },
            details={"/tv/1203": {"number_of_episodes": 12}},
        )
        evidence = IdentityEvidence(
            work_unit_id="wu-naked-numeric-cross-script",
            boundary_label=label,
            parent_labels=(),
            representative_names=(label, "01", "02", "03", "04"),
            normalized_titles=(label,),
            years=(2024,),
            episode_pattern=None,
            media_shape="tv",
            aliases=(),
            strict_naked_numeric_video_run=True,
            naked_numeric_cjk_release_eligible=True,
        )

        with self.assertRaises(AutoMatchAmbiguityError):
            auto_match_from_evidence(client, evidence, prefer_animation=True)

        search_queries = [
            str(params.get("query"))
            for path, params in client.call_log
            if path.startswith("/search/")
        ]
        self.assertFalse(any(query.isdigit() for query in search_queries))

    def test_naked_numeric_guard_requires_exact_same_script_title_and_year(self) -> None:
        """A close title or one-year mismatch remains C/U-uncertain."""
        label = "B 有意义中文剧名（2024）全12集 1080P"

        def evidence() -> IdentityEvidence:
            return IdentityEvidence(
                work_unit_id="wu-naked-numeric-strict",
                boundary_label=label,
                parent_labels=(),
                representative_names=(label, "01", "02", "03", "04"),
                normalized_titles=(label,),
                years=(2024,),
                episode_pattern=None,
                media_shape="tv",
                aliases=(),
                strict_naked_numeric_video_run=True,
                naked_numeric_cjk_release_eligible=True,
            )

        cases = (
            {
                "id": 1204,
                "name": "有意义中文剧名 外传",
                "first_air_date": "2024-07-01",
                "genre_ids": [16],
            },
            {
                "id": 1205,
                "name": "有意义中文剧名",
                "first_air_date": "2023-07-01",
                "genre_ids": [16],
            },
        )
        for row in cases:
            with self.subTest(candidate=row["id"]):
                client = FakeTMDBClient(
                    search_results={"有意义中文剧名": [row]},
                )
                with self.assertRaises(AutoMatchAmbiguityError):
                    auto_match_from_evidence(client, evidence(), prefer_animation=True)

    def test_naked_numeric_exact_year_uses_boundary_not_parent_or_file_years(self) -> None:
        """A parent 2023 cannot validate a boundary explicitly labeled 2024."""
        label = "B 有意义中文剧名（2024）全12集 1080P"
        client = FakeTMDBClient(
            search_results={
                "有意义中文剧名": [{
                    "id": 12051,
                    "name": "有意义中文剧名",
                    "first_air_date": "2023-07-01",
                    "genre_ids": [16],
                }],
            },
        )
        evidence = IdentityEvidence(
            work_unit_id="wu-naked-numeric-boundary-year",
            boundary_label=label,
            parent_labels=("父容器（2023）",),
            representative_names=(label, "01", "02", "03", "04"),
            normalized_titles=(label,),
            # This is intentionally the aggregate collection produced by
            # evidence extraction: the strict gate must not use the parent
            # year merely because ordinary scoring can see it.
            years=(2023, 2024),
            episode_pattern=None,
            media_shape="tv",
            aliases=(),
            strict_naked_numeric_video_run=True,
            naked_numeric_cjk_release_eligible=True,
        )

        with self.assertRaises(AutoMatchAmbiguityError):
            auto_match_from_evidence(client, evidence, prefer_animation=True)

    def test_strict_naked_numeric_prioritizes_clean_boundary_before_three_parents(self) -> None:
        """The only eligible title proof must be dispatched inside the budget."""
        label = "B 有意义中文剧名（2024）全12集 1080P"
        clean_query = "有意义中文剧名"
        client = FakeTMDBClient(
            search_results={
                "有意义中文剧名": [{
                    "id": 12052,
                    "name": "有意义中文剧名",
                    "first_air_date": "2024-07-01",
                    "genre_ids": [16],
                }],
            },
        )
        evidence = IdentityEvidence(
            work_unit_id="wu-naked-numeric-parent-budget",
            boundary_label=label,
            parent_labels=("父容器甲", "父容器乙", "父容器丙"),
            representative_names=(label, "01", "02", "03", "04"),
            normalized_titles=(label,),
            years=(2024,),
            episode_pattern=None,
            media_shape="tv",
            aliases=(),
            strict_naked_numeric_video_run=True,
            naked_numeric_cjk_release_eligible=True,
        )

        best, _ = auto_match_from_evidence(client, evidence, prefer_animation=True)

        self.assertEqual(best.tmdb_id, 12052)
        self.assertTrue(best.decision_trace["naked_numeric_clean_boundary_query_sent"])
        search_queries = [
            str(params.get("query") or "")
            for path, params in client.call_log
            if path == "/search/tv"
        ]
        self.assertIn(clean_query, search_queries)

    def test_strict_naked_numeric_stays_uncertain_if_clean_boundary_is_not_sent(self) -> None:
        """Unsent clean evidence cannot be borrowed from an unqueried variant."""
        label = "B 有意义中文剧名（2024）全12集 1080P"
        clean_query = "有意义中文剧名"
        client = FakeTMDBClient(
            search_results={
                "有意义中文剧名": [{
                    "id": 12053,
                    "name": "有意义中文剧名",
                    "first_air_date": "2024-07-01",
                    "genre_ids": [16],
                }],
            },
        )
        evidence = IdentityEvidence(
            work_unit_id="wu-naked-numeric-unsent-clean-boundary",
            boundary_label=label,
            parent_labels=("父容器甲", "父容器乙", "父容器丙"),
            representative_names=(label, "01", "02", "03", "04"),
            normalized_titles=(label,),
            years=(2024,),
            episode_pattern=None,
            media_shape="tv",
            aliases=(),
            strict_naked_numeric_video_run=True,
            naked_numeric_cjk_release_eligible=True,
        )

        def skip_clean_boundary(query: str) -> list[str]:
            if query == clean_query:
                return []
            return _search_query_variants(query)

        with patch(
            "engine.scrapeflow.identity_matching._search_query_variants",
            side_effect=skip_clean_boundary,
        ):
            with self.assertRaises(AutoMatchAmbiguityError):
                auto_match_from_evidence(client, evidence, prefer_animation=True)

        search_queries = [
            str(params.get("query") or "")
            for path, params in client.call_log
            if path == "/search/tv"
        ]
        self.assertNotIn(clean_query, search_queries)

    def test_generic_naked_numeric_label_rejects_same_title_same_year_tv(self) -> None:
        """Even an exact fake row cannot authenticate a generic 01…N pack."""
        label = "全12集 1080P"
        client = FakeTMDBClient(
            search_results={
                label: [{
                    "id": 12055,
                    "name": label,
                    "first_air_date": "2024-07-01",
                    "genre_ids": [16],
                }],
            },
        )
        evidence = IdentityEvidence(
            work_unit_id="wu-naked-numeric-generic-label",
            boundary_label=label,
            parent_labels=(),
            representative_names=(label, "01", "02", "03", "04"),
            normalized_titles=(label,),
            years=(2024,),
            episode_pattern=None,
            media_shape="tv",
            aliases=(),
            strict_naked_numeric_video_run=True,
            naked_numeric_cjk_release_eligible=False,
        )

        with self.assertRaises(AutoMatchAmbiguityError):
            auto_match_from_evidence(client, evidence, prefer_animation=True)

    def test_naked_numeric_guard_allows_same_script_official_alias(self) -> None:
        """An exact official CJK alias remains valid identity evidence."""
        label = "B 有意义中文剧名（2024）全12集 1080P"
        client = FakeTMDBClient(
            search_results={
                "有意义中文剧名": [{
                    "id": 1206,
                    "name": "Meaningful Show",
                    "first_air_date": "2024-07-01",
                    "genre_ids": [16],
                }],
            },
            alternative_titles={"1206": [{"title": "有意义中文剧名"}]},
        )
        evidence = IdentityEvidence(
            work_unit_id="wu-naked-numeric-alias",
            boundary_label=label,
            parent_labels=(),
            representative_names=(label, "01", "02", "03", "04"),
            normalized_titles=(label,),
            years=(2024,),
            episode_pattern=None,
            media_shape="tv",
            aliases=(),
            strict_naked_numeric_video_run=True,
            naked_numeric_cjk_release_eligible=True,
        )

        best, _ = auto_match_from_evidence(client, evidence, prefer_animation=True)

        self.assertEqual(best.tmdb_id, 1206)
        self.assertTrue(
            best.decision_trace["naked_numeric_same_script_exact_title_or_alias"]
        )

    def test_naked_numeric_guard_keeps_normal_ambiguity_margin(self) -> None:
        """A unique exact title cannot waive a nearby candidate for this shape."""
        label = "B 有意义中文剧名（2024）全12集 1080P"
        client = FakeTMDBClient(
            search_results={
                "有意义中文剧名": [
                    {
                        "id": 1207,
                        "name": "有意义中文剧名",
                        "first_air_date": "2024-07-01",
                        "genre_ids": [16],
                    },
                    {
                        "id": 1208,
                        "name": "有意义中文剧名 特别篇",
                        "first_air_date": "2024-07-01",
                        "genre_ids": [16],
                    },
                ],
            },
        )
        evidence = IdentityEvidence(
            work_unit_id="wu-naked-numeric-margin",
            boundary_label=label,
            parent_labels=(),
            representative_names=(label, "01", "02", "03", "04"),
            normalized_titles=(label,),
            years=(2024,),
            episode_pattern=None,
            media_shape="tv",
            aliases=(),
            strict_naked_numeric_video_run=True,
            naked_numeric_cjk_release_eligible=True,
        )

        with self.assertRaises(AutoMatchAmbiguityError):
            auto_match_from_evidence(client, evidence, prefer_animation=True)

    def test_representative_episode_title_recovers_noisy_boundary(self) -> None:
        """A clean SxxExx filename can prove identity for a package label.

        The boundary is intentionally cross-script and release-like, while
        the TMDB row exposes only its original-language title.  This guards
        against treating an exact representative-file title as weak
        cross-script evidence, and against using a multi-season set of
        repeated episode ordinals as an aggregate episode-count penalty.
        """
        class StrictEpisodeTitleClient:
            """Only the cleaned filename title may retrieve this candidate."""

            def __init__(self) -> None:
                self.call_log: list[tuple[str, dict[str, Any]]] = []

            def get(self, path: str, **params: Any) -> dict[str, Any]:
                self.call_log.append((path, params))
                if path == "/search/tv" and params.get("query") == "Known.Show":
                    return {
                        "results": [{
                            "id": 1399,
                            "name": "Known Show",
                            "first_air_date": "2011-04-17",
                            "genre_ids": [18],
                        }]
                    }
                if path.endswith("/alternative_titles"):
                    return {"results": [], "titles": []}
                return {"results": []}

        client = StrictEpisodeTitleClient()
        evidence = IdentityEvidence(
            work_unit_id="wu-noisy-package",
            boundary_label="4K示例剧集【全8季】无删减",
            parent_labels=(),
            representative_names=(
                "4K示例剧集【全8季】无删减",
                "Known.Show.S01E01.2160p",
                "Known.Show.S02E01.2160p",
            ),
            normalized_titles=("4K示例剧集【全8季】无删减",),
            years=(),
            episode_pattern=EpisodePattern(
                season_numbers=(1, 2),
                episode_numbers=(1, 2, 3),
                total_episodes=3,
                has_specials=False,
            ),
            media_shape="tv",
            aliases=(),
        )

        best, _ = auto_match_from_evidence(client, evidence)

        self.assertEqual(best.tmdb_id, 1399)
        self.assertEqual(best.status, "confirmed")
        self.assertEqual(best.score_components["episode_structure_score"], 0.0)
        self.assertEqual(best.decision_trace["matched_query_variant"], "Known.Show")
        search_queries = [
            str(params.get("query"))
            for path, params in client.call_log
            if path == "/search/tv"
        ]
        self.assertIn("Known.Show", search_queries)
        self.assertNotIn("/tv/1399", [path for path, _ in client.call_log])

    def test_bare_episode_marker_title_derivation_requires_a_title_prefix(self) -> None:
        for source_name in (
            "Example.Side.Story.E01.2021.1080p.WEB-DL.mkv",
            "Example.Side.Story.E01-E06.2021.1080p.WEB-DL.mkv",
        ):
            with self.subTest(source_name=source_name):
                self.assertEqual(
                    _title_from_representative_episode_filename(source_name),
                    "Example.Side.Story",
                )
        self.assertIsNone(
            _title_from_representative_episode_filename(
                "E01-E06.2021.1080p.WEB-DL.mkv"
            )
        )
        self.assertIsNone(
            _title_from_representative_episode_filename(
                "Example.Side.Story.E01-06.2021.1080p.WEB-DL.mkv"
            )
        )

    def test_representative_query_filter_rejects_naked_numeric_media_names(self) -> None:
        for name in ("01", "001", "01.mp4", "001.mkv", "1080.mp4"):
            with self.subTest(name=name):
                self.assertFalse(_usable_representative_identity_query(name))
        self.assertTrue(
            _usable_representative_identity_query(
                "Known.Show.S01E01.1080p.WEB-DL.mkv"
            )
        )

    def test_representative_bare_episode_title_recovers_noisy_boundary(self) -> None:
        """Individual ``E01`` companion files still supply a title query.

        Standalone specials frequently omit the season prefix.  The matcher
        may use their prefix as a query only because the explicit episode
        marker follows a usable title; an opaque package label remains merely
        weak evidence.
        """
        class StrictBareEpisodeClient:
            def __init__(self) -> None:
                self.call_log: list[tuple[str, dict[str, Any]]] = []

            def get(self, path: str, **params: Any) -> dict[str, Any]:
                self.call_log.append((path, params))
                if path == "/search/tv" and params.get("query") == "Example.Side.Story":
                    return {
                        "results": [{
                            "id": 208,
                            "name": "Example Side Story",
                            "first_air_date": "2020-12-22",
                            "genre_ids": [18],
                        }]
                    }
                if path == "/tv/208":
                    return {"number_of_episodes": 6}
                if path.endswith("/alternative_titles"):
                    return {"results": [], "titles": []}
                return {"results": []}

        client = StrictBareEpisodeClient()
        evidence = IdentityEvidence(
            work_unit_id="wu-bare-episode-range",
            boundary_label="无标题发布包.E01-E06.2021.1080p.WEB-DL",
            parent_labels=(),
            representative_names=(
                "无标题发布包.E01-E06.2021.1080p.WEB-DL",
                *(
                    f"Example.Side.Story.E{episode:02d}.2021.1080p.WEB-DL.x265.AC3-Group"
                    for episode in range(1, 7)
                ),
            ),
            normalized_titles=("无标题发布包.E01-E06",),
            years=(2021,),
            episode_pattern=EpisodePattern(
                season_numbers=(),
                episode_numbers=(1, 2, 3, 4, 5, 6),
                total_episodes=6,
                has_specials=False,
            ),
            media_shape="tv",
            aliases=(),
        )

        best, _ = auto_match_from_evidence(client, evidence)

        self.assertEqual(best.tmdb_id, 208)
        self.assertEqual(best.status, "confirmed")
        self.assertEqual(best.decision_trace["matched_query_variant"], "Example.Side.Story")
        search_queries = [
            str(params.get("query"))
            for path, params in client.call_log
            if path == "/search/tv"
        ]
        self.assertIn("Example.Side.Story", search_queries)

    def test_cross_script_prefix_only_representative_stays_uncertain(self) -> None:
        """A merely prefix-similar release label cannot waive alias evidence."""
        client = FakeTMDBClient(
            search_results={
                "Known Show Preview": [
                    {
                        "id": 1400,
                        "name": "Known Show",
                        "first_air_date": "2011-04-17",
                        "genre_ids": [18],
                    }
                ],
            },
        )
        evidence = IdentityEvidence(
            work_unit_id="wu-cross-script-prefix",
            boundary_label="示例剧集",
            parent_labels=(),
            representative_names=("Known Show Preview",),
            normalized_titles=("示例剧集",),
            years=(),
            episode_pattern=None,
            media_shape="tv",
            aliases=(),
        )

        with self.assertRaises(AutoMatchAmbiguityError):
            auto_match_from_evidence(client, evidence)

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

    def test_complete_oad_run_requires_official_marker_and_count(self) -> None:
        """An OAD child chooses only the formally matching short TV work."""
        client = FakeTMDBClient(
            search_results={
                "Magi Sinbad OAD": [
                    {"id": 1, "name": "Magi Sinbad", "first_air_date": "2016-01-01", "genre_ids": [16]},
                    {"id": 2, "name": "Magi Sinbad OAD", "first_air_date": "2014-01-01", "genre_ids": [16]},
                ],
            },
            details={
                "/tv/1": {
                    "name": "Magi Sinbad",
                    "seasons": [{"season_number": 1, "episode_count": 13, "name": "Season 1"}],
                },
                "/tv/2": {
                    "name": "Magi Sinbad OAD",
                    "seasons": [{"season_number": 1, "episode_count": 5, "name": "OAD"}],
                },
            },
        )
        evidence = IdentityEvidence(
            work_unit_id="wu-oad-candidate",
            boundary_label="Magi Sinbad OAD",
            parent_labels=(),
            representative_names=("Magi Sinbad OAD",),
            normalized_titles=("Magi Sinbad OAD",),
            years=(),
            episode_pattern=EpisodePattern((), (), 0, True),
            media_shape="tv",
            aliases=(),
            special_markers=("OAD",),
            special_episode_numbers=(1, 2, 3, 4, 5),
            special_episode_count=5,
            special_numbered_run_complete=True,
        )
        best, candidates = auto_match_from_evidence(client, evidence)
        self.assertEqual(best.tmdb_id, 2)
        self.assertEqual(best.decision_trace["official_special_marker_hits"], ["OAD"])
        self.assertTrue(best.decision_trace["official_special_count_match"])
        rejected = next(item for item in candidates if item.tmdb_id == 1)
        self.assertEqual(rejected.status, "rejected")
        self.assertIn(
            "physical_special_marker_not_officially_proven",
            rejected.decision_trace["blockers"],
        )

    def test_complete_oad_run_rejects_same_count_without_official_marker(self) -> None:
        """Equal count alone cannot turn a physical release into a TV work."""
        client = FakeTMDBClient(
            search_results={"Example OAD": [{"id": 9, "name": "Example", "first_air_date": "2020-01-01", "genre_ids": [16]}]},
            details={"/tv/9": {"name": "Example", "seasons": [{"season_number": 1, "episode_count": 5, "name": "Season 1"}]}},
        )
        evidence = IdentityEvidence(
            work_unit_id="wu-oad-no-marker",
            boundary_label="Example OAD",
            parent_labels=(), representative_names=("Example OAD",),
            normalized_titles=("Example OAD",), years=(),
            episode_pattern=EpisodePattern((), (), 0, True), media_shape="tv", aliases=(),
            special_markers=("OAD",), special_episode_numbers=(1, 2, 3, 4, 5),
            special_episode_count=5, special_numbered_run_complete=True,
        )
        with self.assertRaises(AutoMatchAmbiguityError) as ctx:
            auto_match_from_evidence(client, evidence)
        self.assertEqual(ctx.exception.candidates[0]["status"], "rejected")

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

    def test_chinese_season_ordinal_filename_yields_title_query(self) -> None:
        """``东京喰种 第1季 03.mkv`` has explicit CJK season coordinates."""
        self.assertEqual(
            _title_from_representative_episode_filename(
                "[4K_NW] 东京喰种 第1季 03.mkv"
            ),
            "[4K_NW] 东京喰种",
        )
        self.assertIsNone(
            _title_from_representative_episode_filename(
                "第1季 03.mkv"
            )
        )
