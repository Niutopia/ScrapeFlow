"""Regression coverage for the three Steins;Gate generic engine fixes.

* a letter-suffixed bracket ordinal (``[23B]``) is its own release
  coordinate, omitted from the strict ``1..N`` run like a fractional episode;
* a qualifier word before OP/ED (``[Game OP]``) is still a non-story theme
  asset;
* a marker-less franchise sub-work (``命运石之门 聪明睿智的认知计算``)
  escalates C to the parent's standalone query and proves D onto the
  parent's published Season 00 arc window.
"""

from __future__ import annotations

import re
import tempfile
import unittest
from pathlib import Path

from engine.scrapeflow.root_boundaries import analyze_root_boundaries
from engine.scrapeflow.unit_identity import (
    apply_work_unit_override,
    resolve_work_unit_identities,
)
from engine.scrapeflow.work_units import load_work_unit_records

from engine.scrapeflow.core import _has_special_context, extract_episode_key
from engine.scrapeflow.source_inventory import SourceFile
from local.scrapeflow_api.library_index import (
    SingleSeasonEpisodeProof,
    _franchise_arc_anchor_labels,
    _is_known_non_story_theme_video,
    _is_letter_variant_episode_video,
    _named_arc_franchise_subwork_evidence,
    _named_arc_season00_run,
    _single_positive_tmdb_season,
    reconcile_root_work_units,
)
from local.scrapeflow_api.tmdb_episode_catalog import TmdbEpisodeCatalog
from local.tests.test_library_index import IndexAList


def _video(path: str, size: int = 100) -> SourceFile:
    return SourceFile(
        path=path,
        name=path.rsplit("/", 1)[-1],
        size=size,
        object_type="video",
        modified="",
    )


def _special_context(name: str) -> bool:
    return _has_special_context({"name": name, "full_path": "/quark/x/" + name})


class LetterVariantBracketOrdinalTests(unittest.TestCase):
    """``[23B]`` is an alternate cut, not an integer-run member."""

    def test_letter_variant_is_detected_and_excluded_from_run(self) -> None:
        plain = _video("/incoming/Show/[G] Show [23].mkv")
        beta = _video("/incoming/Show/[G] Show [23B].mkv")
        self.assertFalse(_is_letter_variant_episode_video(plain))
        self.assertTrue(_is_letter_variant_episode_video(beta))
        self.assertTrue(
            _is_letter_variant_episode_video(
                _video("/incoming/Show/[G] Show [23β].mkv")
            )
        )
        # A version revision re-releases the same ordinal: not a variant.
        self.assertFalse(
            _is_letter_variant_episode_video(
                _video("/incoming/Show/[G] Show [02v2].mkv")
            )
        )
        # A second season marker is not a letter variant either.
        self.assertFalse(
            _is_letter_variant_episode_video(
                _video("/incoming/Show/[G] Show S02E04 [1080p].mkv")
            )
        )
        # Scan tags are release quality labels, not cut letters.
        for tag in ("720p", "1080i", "360p", "576p", "Ma10p_2160p"):
            self.assertFalse(
                _is_letter_variant_episode_video(
                    _video(f"/incoming/Show/[G] Show [{tag}].mkv")
                ),
                tag,
            )

    def test_letter_variant_and_theme_assets_are_special_context(self) -> None:
        """Non-story assets stay at source instead of blocking the plan."""
        for name in (
            "[TUDO&Ygm] Steins;Gate [23B][Ma10p_2160p][x265_flac_ass].mkv",
            "[TUDO&Ygm]Steins;Gate[NCOP][Ma10p_2160p][x265_flac].mkv",
            "[TUDO&Ygm]Steins;Gate[NCED][Ma10p_2160p][x265_flac].mkv",
            "[TUDO&Ygm] Steins;Gate 0 [NCOP01][Ma10p_2160p][x265_flac].mkv",
            "[TUDO&Ygm] Steins;Gate 0 [NCED02][Ma10p_2160p][x265_flac].mkv",
            "[TUDO&Ygm] Steins;Gate 0 [Game OP][Ma10p_2160p][x265_flac].mkv",
        ):
            self.assertTrue(_special_context(name), name)
        # A story episode, a revision, and a quality tag stay regular.
        for name in (
            "[TUDO&Ygm] Steins;Gate [23][Ma10p_2160p][x265_flac_ass].mkv",
            "[G] Show [02v2].mkv",
            "Show [720p].mkv",
            "Show [1080i].mkv",
            "[G] Show [Ma10p_2160p].mkv",
        ):
            self.assertFalse(_special_context(name), name)

    def test_non_story_assets_leave_the_bare_number_fallback(self) -> None:
        """A sequel title digit on a non-story asset is not an episode.

        ``Steins;Gate 0 [NCOP01]`` has no numeric episode bracket, so the
        bare-number fallback used to read the sequel ``0`` as ``E00`` — a
        coordinate no catalog carries, which blocked the plan instead of
        staying at source.  Stronger tokens still win over the asset marker.
        """
        for name in (
            "[TUDO&Ygm] Steins;Gate 0 [NCOP01][Ma10p_2160p][x265_flac].mkv",
            "[TUDO&Ygm] Steins;Gate 0 [NCED02][Ma10p_2160p][x265_flac].mkv",
            "[TUDO&Ygm] Steins;Gate 0 [Game OP][Ma10p_2160p][x265_flac].mkv",
            "[TUDO&Ygm] Steins;Gate [23B][Ma10p_2160p][x265_flac_ass].mkv",
        ):
            self.assertIsNone(extract_episode_key(name), name)
        # A pure numeric bracket, an explicit token, and a revision keep
        # their ordinal even beside a theme bracket or a sequel digit.
        for name, expected in (
            ("[TUDO&Ygm] Steins;Gate 0 [01][Ma10p_2160p][x265_flac_ass].mkv", "E01"),
            ("[TUDO&Ygm] Steins;Gate 0 [24][Ma10p_2160p][x265_flac_ass].mkv", "E24"),
            ("Show S01E05 [NCOP].mkv", "E05"),
            ("Show [23][NCOP].mkv", "E23"),
        ):
            key = extract_episode_key(name)
            self.assertIsNotNone(key, name)
            self.assertEqual(key.display, expected, name)


class ProofReceiptShapeTests(unittest.TestCase):
    """``SingleSeasonEpisodeProof`` receipts round-trip the proved shapes."""

    def test_season00_arc_window_receipt_round_trips(self) -> None:
        proof = SingleSeasonEpisodeProof(
            tmdb_id=42509,
            season=0,
            episode_count=4,
            episode_tokens=("S00E02", "S00E03", "S00E04", "S00E05"),
            evidence_kind="tmdb_single_positive_season_bracketed_episodes",
        )
        restored = SingleSeasonEpisodeProof.from_dict(proof.as_dict())
        self.assertEqual(restored, proof)

    def test_overflow_receipt_round_trips(self) -> None:
        proof = SingleSeasonEpisodeProof(
            tmdb_id=78102,
            season=1,
            episode_count=24,
            episode_tokens=(
                *(f"S01E{episode:02d}" for episode in range(1, 24)),
                "S00E01",
            ),
            evidence_kind="tmdb_single_positive_season_bracketed_episodes",
        )
        restored = SingleSeasonEpisodeProof.from_dict(proof.as_dict())
        self.assertEqual(restored, proof)

    def test_malformed_receipts_fail_closed(self) -> None:
        base = {
            "kind": "tmdb_single_positive_season_bracketed_episodes",
            "tmdb_id": 42509,
            "season": 1,
            "episode_count": 3,
        }
        # Interleaved seasons are not one of the proved shapes.
        self.assertIsNone(
            SingleSeasonEpisodeProof.from_dict({
                **base,
                "episode_tokens": ["S01E01", "S00E01", "S01E02"],
            })
        )
        # A regular block must start at E01.
        self.assertIsNone(
            SingleSeasonEpisodeProof.from_dict({
                **base,
                "episode_tokens": ["S01E02", "S01E03", "S01E04"],
            })
        )
        # A non-consecutive Season 00 window is not a run.
        self.assertIsNone(
            SingleSeasonEpisodeProof.from_dict({
                **base,
                "season": 0,
                "episode_tokens": ["S00E02", "S00E04", "S00E05"],
            })
        )


class QualifierThemeMarkerTests(unittest.TestCase):
    """``[Game OP]`` names the game-version opening, not a story episode."""

    def test_qualifier_prefix_still_classifies_as_theme_video(self) -> None:
        self.assertTrue(
            _is_known_non_story_theme_video(
                _video("/incoming/Show/[G] Show [Game OP].mkv")
            )
        )
        self.assertTrue(
            _is_known_non_story_theme_video(
                _video("/incoming/Show/[G] Show [Creditless OP 2].mkv")
            )
        )
        # The historical shapes keep their classification.
        self.assertTrue(
            _is_known_non_story_theme_video(
                _video("/incoming/Show/[G] Show [NCOP01].mkv")
            )
        )
        self.assertTrue(
            _is_known_non_story_theme_video(
                _video("/incoming/Show/[G] Show [EDv2].mkv")
            )
        )
        # An episode bracket or an unrelated tag stays a regular video.
        self.assertFalse(
            _is_known_non_story_theme_video(
                _video("/incoming/Show/[G] Show [01].mkv")
            )
        )
        self.assertFalse(
            _is_known_non_story_theme_video(
                _video("/incoming/Show/[G] Show [Ma10p_2160p].mkv")
            )
        )
        # One bounded qualifier word only: a long phrase is unknown shape.
        self.assertFalse(
            _is_known_non_story_theme_video(
                _video("/incoming/Show/[G] Show [Opening Theme Full Song].mkv")
            )
        )


class RegularTotalExcludesSpecialsTests(unittest.TestCase):
    """An overflow run may exceed ``number_of_episodes`` by the specials."""

    def test_regular_only_total_still_proves_the_overflow_run(self) -> None:
        # tv/78102 shape: 23 regular + 1 special, run 1..24.  The detail
        # total counts regular episodes only, so the old strict bound
        # (23 < 24) rejected exactly the run the overflow rule exists for.
        class RegularOnlyTMDB:
            def get(self, path: str, **_kwargs: object) -> dict[str, object]:
                if path == "/tv/78102":
                    return {
                        "number_of_seasons": 1,
                        "number_of_episodes": 23,
                        "seasons": [
                            {"season_number": 0, "episode_count": 1},
                            {"season_number": 1, "episode_count": 23},
                        ],
                    }
                return {}

        evidence = _single_positive_tmdb_season(
            RegularOnlyTMDB(), tmdb_id=78102, episode_count=24,
        )
        self.assertIsNotNone(evidence)
        assert evidence is not None
        self.assertEqual(evidence.season, 1)
        self.assertEqual(evidence.regular_episode_count, 23)
        self.assertEqual(evidence.overflow_episode_count, 1)
        # A run longer than everything the show published still fails.
        class TooLongTMDB(RegularOnlyTMDB):
            def get(self, path: str, **kwargs: object) -> dict[str, object]:
                payload = super().get(path, **kwargs)
                if path == "/tv/78102":
                    payload = dict(payload)
                    payload["number_of_episodes"] = 20
                return payload

        self.assertIsNone(
            _single_positive_tmdb_season(
                TooLongTMDB(), tmdb_id=78102, episode_count=24,
            )
        )


class FranchiseArcAnchorTests(unittest.TestCase):
    """The enclosing label must be franchise title + concrete arc."""

    def test_anchor_label_is_the_parent_prefixed_directory(self) -> None:
        anchors = _franchise_arc_anchor_labels(
            [
                "/incoming/M 4k 命运石之门/命运石之门 聪明睿智的认知计算/[G] ONA [01].mkv",
            ],
            "/incoming/M 4k 命运石之门",
            "命运石之门",
        )
        self.assertEqual(anchors, ("命运石之门 聪明睿智的认知计算",))

    def test_bare_franchise_or_short_residual_is_not_an_anchor(self) -> None:
        # The label equals the franchise title: a plain shelf, no arc.
        self.assertEqual(
            _franchise_arc_anchor_labels(
                ["/incoming/M 4k 命运石之门/命运石之门/[G] Show [01].mkv"],
                "/incoming/M 4k 命运石之门",
                "命运石之门",
            ),
            (),
        )
        # The residual is shorter than four identity characters.
        self.assertEqual(
            _franchise_arc_anchor_labels(
                ["/incoming/示例/示例剧 X/[G] Show [01].mkv"],
                "/incoming/示例",
                "示例剧",
            ),
            (),
        )


class NamedArcFranchiseSubworkTests(unittest.TestCase):
    """The parent-stripped arc key proves the published Season 00 window."""

    def test_parent_stripped_arc_finds_the_official_window(self) -> None:
        published = {
            1: "横行跋扈的浪荡之徒",
            2: "聪明睿智的认知计算：料理篇",
            3: "聪明睿智的认知计算：导航篇",
            4: "聪明睿智的认知计算：时尚篇",
            5: "聪明睿智的认知计算：会议篇",
            6: "境界面上的缺失之环（β线）",
        }
        years = {number: 2014 for number in published}
        # The full label misses (franchise prefix not in official titles),
        # but the label minus the parent title is the arc itself.
        window = _named_arc_season00_run(
            published,
            published_years=years,
            run_length=4,
            boundary_label="命运石之门 聪明睿智的认知计算",
            source_years=(),
            parent_title="命运石之门",
        )
        self.assertEqual(window, (2, 3, 4, 5))

    def test_without_parent_title_the_window_stays_unproven(self) -> None:
        published = {
            2: "聪明睿智的认知计算：料理篇",
            3: "聪明睿智的认知计算：导航篇",
        }
        window = _named_arc_season00_run(
            published,
            published_years={2: 2014, 3: 2014},
            run_length=2,
            boundary_label="命运石之门 聪明睿智的认知计算",
            source_years=(),
        )
        self.assertIsNone(window)

    def test_marked_run_keeps_its_historical_year_gate(self) -> None:
        # The original 爱染香篇 shape: a marker-bearing label with years.
        published = {8: "示例剧 爱染香篇 前篇", 9: "示例剧 爱染香篇 后篇"}
        window = _named_arc_season00_run(
            published,
            published_years={8: 2016, 9: 2016},
            run_length=2,
            boundary_label="示例剧 爱染香篇",
            source_years=(2016,),
        )
        self.assertEqual(window, (8, 9))
        # Without any year evidence the marked shape also still proves: the
        # title containment plus the unique window remain the proof.
        window_no_year = _named_arc_season00_run(
            published,
            published_years={8: 2016, 9: 2016},
            run_length=2,
            boundary_label="示例剧 爱染香篇",
            source_years=(),
        )
        self.assertEqual(window_no_year, (8, 9))


class _SubworkTMDB:
    """Detail/season double for the parent show 42509 and rival 78102."""

    def get(self, path: str, **_params: object) -> dict[str, object]:
        if path == "/tv/42509":
            return {
                "name": "命运石之门",
                "number_of_seasons": 1,
                "number_of_episodes": 24,
                "seasons": [
                    {"season_number": 0, "episode_count": 6, "name": "特别篇"},
                    {"season_number": 1, "episode_count": 24, "name": "第 1 季"},
                ],
            }
        if path == "/tv/78102":
            # The rival sibling carries real detail shape too: an empty
            # response would skip the episode-structure check and let the
            # rival keep a penalty-free score it does not earn.
            return {
                "name": "命运石之门 0",
                "number_of_seasons": 1,
                "number_of_episodes": 23,
                "seasons": [
                    {"season_number": 0, "episode_count": 1, "name": "特别篇"},
                    {"season_number": 1, "episode_count": 23, "name": "第 1 季"},
                ],
            }
        if path == "/tv/42509/season/0":
            names = {
                1: "横行跋扈的浪荡之徒",
                2: "聪明睿智的认知计算：料理篇",
                3: "聪明睿智的认知计算：导航篇",
                4: "聪明睿智的认知计算：时尚篇",
                5: "聪明睿智的认知计算：会议篇",
                6: "境界面上的缺失之环（β线）",
            }
            air = {1: "2012-02-22", 6: "2015-12-03"}
            return {
                "episodes": [
                    {
                        "episode_number": number,
                        "air_date": air.get(number, "2014-10-14"),
                        "name": names[number],
                    }
                    for number in range(1, 7)
                ]
            }
        if path == "/tv/42509/season/1":
            return {
                "episodes": [
                    {
                        "episode_number": number,
                        "air_date": "2011-04-06",
                        "name": f"第{number}集",
                    }
                    for number in range(1, 25)
                ]
            }
        return {}


class _StrictSearchTMDB(_SubworkTMDB):
    """Exact-match search double for the escalation scenario.

    Real TMDB search does not substring-match: a long franchise+arc label
    returns nothing, only the cleaned parent title finds the franchise.
    The fuzzy containment double would let a round-one combined query hit
    and the escalation would never fire, so this scenario needs equality.
    """

    def __init__(self, search_results: dict[str, list[dict]]) -> None:
        self.search_results = search_results
        self.calls: list[tuple[str, dict]] = []

    def get(self, path: str, **params: object) -> dict[str, object]:
        self.calls.append((path, dict(params)))
        if path.startswith("/search/"):
            query_key = re.sub(
                r"[^\w㐀-鿿]+", "", str(params.get("query", "")).casefold()
            )
            rows = self.search_results.get(query_key, [])
            return {"results": rows}
        if path.endswith("/alternative_titles"):
            return {"results": [], "titles": []}
        return super().get(path, **params)


class SubworkEvidenceProofTests(unittest.TestCase):
    """D proves the sub-work through the enclosing arc label."""

    def test_subwork_run_proves_the_season00_window(self) -> None:
        proof = _named_arc_franchise_subwork_evidence(
            TmdbEpisodeCatalog(_SubworkTMDB()),
            tmdb_id=42509,
            run_length=4,
            source_paths=[
                "/incoming/M 4k 命运石之门/命运石之门 聪明睿智的认知计算/[G] ONA [01].mkv",
                "/incoming/M 4k 命运石之门/命运石之门 聪明睿智的认知计算/[G] ONA [04].mkv",
            ],
            root_path="/incoming/M 4k 命运石之门",
            display_label="G ONA",
            source_years=(),
            identity_title="命运石之门",
            evidence_kind="tmdb_single_positive_season_bracketed_episodes",
        )
        self.assertIsNotNone(proof)
        assert proof is not None
        self.assertEqual(proof.season, 0)
        self.assertEqual(
            tuple(proof.episode_tokens),
            ("S00E02", "S00E03", "S00E04", "S00E05"),
        )

    def test_mismatched_run_length_stays_unproven(self) -> None:
        proof = _named_arc_franchise_subwork_evidence(
            TmdbEpisodeCatalog(_SubworkTMDB()),
            tmdb_id=42509,
            run_length=3,
            source_paths=[
                "/incoming/M 4k 命运石之门/命运石之门 聪明睿智的认知计算/[G] ONA [01].mkv",
            ],
            root_path="/incoming/M 4k 命运石之门",
            display_label="G ONA",
            source_years=(),
            identity_title="命运石之门",
            evidence_kind="tmdb_single_positive_season_bracketed_episodes",
        )
        self.assertIsNone(proof)


class FranchiseSubworkPipelineTests(unittest.TestCase):
    """End-to-end: C escalation + D window over a real B/W snapshot."""

    def test_zero_candidate_boundary_escalates_to_parent_query(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            state_root = Path(directory)
            root_task_id = "root-sg-subwork"
            files = {
                # The sibling main-series directory makes B/W split the arc
                # directory into its own unit, like the real source tree.
                "/incoming/M 4k 命运石之门/命运石之门/[G] Steins;Gate [01].mkv": b"v",
                "/incoming/M 4k 命运石之门/命运石之门/[G] Steins;Gate [02].mkv": b"v",
                "/incoming/M 4k 命运石之门/命运石之门 聪明睿智的认知计算/"
                "[G] Steins;Gate Soumei Eichi no Cognitive Computing [01].mkv": b"v",
                "/incoming/M 4k 命运石之门/命运石之门 聪明睿智的认知计算/"
                "[G] Steins;Gate Soumei Eichi no Cognitive Computing [02].mkv": b"v",
                "/incoming/M 4k 命运石之门/命运石之门 聪明睿智的认知计算/"
                "[G] Steins;Gate Soumei Eichi no Cognitive Computing [03].mkv": b"v",
                "/incoming/M 4k 命运石之门/命运石之门 聪明睿智的认知计算/"
                "[G] Steins;Gate Soumei Eichi no Cognitive Computing [04].mkv": b"v",
            }
            alist = IndexAList(files)
            analyze_root_boundaries(
                alist,
                "/incoming/M 4k 命运石之门",
                root_task_id=root_task_id,
                state_root=state_root,
            )
            tmdb = _StrictSearchTMDB(
                {
                    "命运石之门": [
                        {
                            "id": 42509,
                            "name": "命运石之门",
                            "original_name": "Steins;Gate",
                            "first_air_date": "2011-04-06",
                            "genre_ids": [16],
                        },
                        {
                            "id": 78102,
                            "name": "命运石之门 0",
                            "original_name": "シュタインズ・ゲート ゼロ",
                            "first_air_date": "2018-04-12",
                            "genre_ids": [16],
                        },
                    ],
                },
            )
            resolve_work_unit_identities(tmdb, state_root, root_task_id)
            queries = [
                str(params.get("query"))
                for path, params in tmdb.calls
                if path == "/search/tv"
            ]
            # The parent's standalone cleaned query is what found the
            # franchise: no boundary query equals it.
            self.assertIn("命运石之门", queries)
            records = load_work_unit_records(state_root, root_task_id)
            subwork = next(
                record for record in records
                if "认知计算" in record.display_label
            )
            self.assertEqual(subwork.identity_status, "confirmed")
            self.assertEqual(subwork.identity["tmdb_id"], 42509)

    def test_subwork_d_proves_season00_arc_window(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            state_root = Path(directory)
            root_task_id = "root-sg-subwork-d"
            files = {
                # The sibling main-series directory is what makes B/W split
                # the arc directory into its own unit; a lone child would
                # stay folded into the root.
                "/incoming/M 4k 命运石之门/命运石之门/[G] Steins;Gate [01].mkv": b"v",
                "/incoming/M 4k 命运石之门/命运石之门/[G] Steins;Gate [02].mkv": b"v",
                "/incoming/M 4k 命运石之门/命运石之门 聪明睿智的认知计算/"
                "[G] Steins;Gate Soumei Eichi no Cognitive Computing [01].mkv": b"v",
                "/incoming/M 4k 命运石之门/命运石之门 聪明睿智的认知计算/"
                "[G] Steins;Gate Soumei Eichi no Cognitive Computing [02].mkv": b"v",
                "/incoming/M 4k 命运石之门/命运石之门 聪明睿智的认知计算/"
                "[G] Steins;Gate Soumei Eichi no Cognitive Computing [03].mkv": b"v",
                "/incoming/M 4k 命运石之门/命运石之门 聪明睿智的认知计算/"
                "[G] Steins;Gate Soumei Eichi no Cognitive Computing [04].mkv": b"v",
            }
            alist = IndexAList(files)
            analyze_root_boundaries(
                alist,
                "/incoming/M 4k 命运石之门",
                root_task_id=root_task_id,
                state_root=state_root,
            )
            records = load_work_unit_records(state_root, root_task_id)
            subwork = next(
                record for record in records
                if "认知计算" in record.display_label
            )
            # An operator override confirms media_type + tmdb_id only: the
            # identity carries no title, exactly like the C-escalation unit.
            apply_work_unit_override(
                state_root, root_task_id, subwork.work_unit_id,
                media_type="tv", tmdb_id=42509,
            )
            tmdb = _SubworkTMDB()
            results = reconcile_root_work_units(
                alist, "/library", state_root, root_task_id,
                episode_catalog=TmdbEpisodeCatalog(tmdb), tmdb_client=tmdb,
            )
            result = next(
                record for record in results
                if "认知计算" in record.display_label
            )
            self.assertEqual(result.reconciliation_outcome, "new_work")
            self.assertEqual(
                result.reconciliation_evidence,
                {
                    "kind": "tmdb_single_positive_season_bracketed_episodes",
                    "tmdb_id": 42509,
                    "season": 0,
                    "episode_count": 4,
                    "episode_tokens": [
                        "S00E02", "S00E03", "S00E04", "S00E05",
                    ],
                },
            )


if __name__ == "__main__":
    unittest.main()
