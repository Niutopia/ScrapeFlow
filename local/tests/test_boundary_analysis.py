"""Tests for Phase-2 SourceInventory and BoundaryAnalysis.

All tests are fully offline — no network, no AList, no TMDB.
Fixtures are loaded from local/tests/fixtures/media_cases/*.json.
"""

from __future__ import annotations

import json
import unittest
from pathlib import Path

from engine.scrapeflow.boundary_analysis import (
    DirectoryRole,
    WorkCandidate,
    _directory_matches_declared_season,
    _generic_season_child,
    analyze_boundaries,
)
from engine.scrapeflow.source_inventory import (
    SourceFile,
    SourceNode,
    build_source_inventory_from_fixture,
    classify_object_type,
    collect_all_files,
    count_video_files,
    count_subtitle_files,
    direct_video_file_count,
    has_only_subtitles,
    load_fixture,
)

_FIXTURE_DIR = Path(__file__).parent / "fixtures" / "media_cases"


def _load(case_id: str) -> SourceNode:
    return load_fixture(_FIXTURE_DIR / case_id / "source_tree.json")


# ===========================================================================
# SourceInventory unit tests
# ===========================================================================

class TestClassifyObjectType(unittest.TestCase):
    def test_video_extensions(self) -> None:
        for name in ("movie.mkv", "ep01.mp4", "ep02.avi", "clip.ts"):
            with self.subTest(name=name):
                self.assertEqual(classify_object_type(name), "video")

    def test_disc_image_is_not_classified_as_video(self) -> None:
        for name in ("disc.iso", "disc.img", "track.bin"):
            with self.subTest(name=name):
                self.assertEqual(classify_object_type(name), "disc_image")

    def test_cue_sheet_is_audio_sidecar_not_disc_image(self) -> None:
        # A CD-audio CUE beside FLAC tracks must not trigger the opaque
        # disc-image boundary; an OST folder parks no release.
        self.assertEqual(classify_object_type("ost.cue"), "audio")
        self.assertEqual(classify_object_type("track.flac"), "audio")

    def test_subtitle_extensions(self) -> None:
        for name in ("sub.srt", "sub.ass", "sub.ssa"):
            with self.subTest(name=name):
                self.assertEqual(classify_object_type(name), "subtitle")

    def test_poster(self) -> None:
        self.assertEqual(classify_object_type("poster.jpg"), "poster")
        self.assertEqual(classify_object_type("fanart.png"), "poster")

    def test_nfo(self) -> None:
        self.assertEqual(classify_object_type("tvshow.nfo"), "nfo")

    def test_other(self) -> None:
        self.assertEqual(classify_object_type("readme.txt"), "other")

    def test_font_pack_exe_is_not_executable(self) -> None:
        """A ``[Fonts].exe`` installer is a residual resource, not a disguised
        media container: boundary analysis must not park the whole source."""
        for name in (
            "[Sakurato] Oshi no Ko [Fonts].exe",
            "冰菓 [Fonts].exe",
            "字体包.exe",
            "font_installer.exe",
        ):
            with self.subTest(name=name):
                self.assertEqual(classify_object_type(name), "font")
        # A non-font executable is still flagged for expansion.
        self.assertEqual(classify_object_type("wrapper.exe"), "executable")


class TestBuildFromFixture(unittest.TestCase):
    def test_ordinary_movie_structure(self) -> None:
        node = _load("ordinary_movie")
        self.assertEqual(node.name, "流浪地球2 (2023)")
        self.assertEqual(node.depth, 0)
        # No sub-directories — all items are direct files
        self.assertEqual(len(node.children), 0)
        file_names = {f.name for f in node.files}
        self.assertIn("流浪地球2.2023.2160p.mkv", file_names)
        self.assertIn("poster.jpg", file_names)

    def test_fate_container_structure(self) -> None:
        node = _load("fate_container")
        self.assertEqual(node.name, "Fate系列")
        child_names = {c.name for c in node.children}
        self.assertIn("空之境界", child_names)
        self.assertIn("Fate Stay Night UBW", child_names)
        self.assertIn("Fate Zero", child_names)

    def test_multiseason_structure(self) -> None:
        node = _load("single_tv_multiseason")
        child_names = {c.name for c in node.children}
        self.assertIn("Season 01", child_names)
        self.assertIn("Season 02", child_names)
        self.assertIn("Season 03", child_names)

    def test_depth_assignment(self) -> None:
        node = _load("fate_container")
        # Root = depth 0
        self.assertEqual(node.depth, 0)
        # Immediate children = depth 1
        for child in node.children:
            self.assertEqual(child.depth, 1)
        # Season dirs inside FSN UBW = depth 2
        fsn = next(c for c in node.children if c.name == "Fate Stay Night UBW")
        for season in fsn.children:
            self.assertEqual(season.depth, 2)


class TestCountHelpers(unittest.TestCase):
    def test_count_video_files_ordinary_movie(self) -> None:
        node = _load("ordinary_movie")
        self.assertEqual(count_video_files(node), 1)

    def test_count_video_files_fate_container(self) -> None:
        node = _load("fate_container")
        # 空之境界: 3, FSN UBW S1: 2 + S2: 1, Fate Zero: 4 = 10
        self.assertEqual(count_video_files(node), 10)

    def test_count_subtitle_files(self) -> None:
        node = _load("ordinary_movie")
        self.assertEqual(count_subtitle_files(node), 1)

    def test_subtitle_only_has_no_video(self) -> None:
        node = _load("subtitle_only")
        self.assertEqual(count_video_files(node), 0)
        self.assertEqual(count_subtitle_files(node), 4)

    def test_direct_video_count_excludes_subdirs(self) -> None:
        node = _load("fate_container")
        # Fate series root has NO videos directly — all are in sub-directories
        self.assertEqual(direct_video_file_count(node), 0)

    def test_collect_all_files_flat(self) -> None:
        node = _load("ordinary_movie")
        all_files = collect_all_files(node)
        self.assertEqual(len(all_files), 3)  # mkv + srt + jpg

    def test_has_only_subtitles_true(self) -> None:
        node = _load("subtitle_only")
        self.assertTrue(has_only_subtitles(node))

    def test_has_only_subtitles_false_when_video_present(self) -> None:
        node = _load("ordinary_movie")
        self.assertFalse(has_only_subtitles(node))


# ===========================================================================
# BoundaryAnalysis — per-fixture contract tests
# ===========================================================================

class TestBoundaryOrdinaryMovie(unittest.TestCase):
    """单部电影 → single_work, media_context=movie."""

    def setUp(self) -> None:
        node = _load("ordinary_movie")
        self.candidates = analyze_boundaries(node, root_task_id="test-root-movie")

    def test_exactly_one_candidate(self) -> None:
        self.assertEqual(len(self.candidates), 1)

    def test_role_is_single_work(self) -> None:
        c = self.candidates[0]
        self.assertEqual(c.boundary_evidence.role, DirectoryRole.SINGLE_WORK)

    def test_media_context_is_movie(self) -> None:
        c = self.candidates[0]
        self.assertEqual(c.proposed_media_context, "movie")

    def test_confidence_above_threshold(self) -> None:
        self.assertGreater(self.candidates[0].boundary_evidence.confidence, 0.5)

    def test_work_unit_id_is_stable(self) -> None:
        node = _load("ordinary_movie")
        candidates2 = analyze_boundaries(node, root_task_id="test-root-movie")
        self.assertEqual(
            self.candidates[0].work_unit_id,
            candidates2[0].work_unit_id,
        )


class TestBoundaryMultiSeason(unittest.TestCase):
    """单剧多季 → single_work（多个季目录属于同一作品）, media_context=tv."""

    def setUp(self) -> None:
        node = _load("single_tv_multiseason")
        self.candidates = analyze_boundaries(node, root_task_id="test-root-bb")

    def test_exactly_one_candidate(self) -> None:
        """Multi-season single TV show must NOT produce multiple WorkCandidates."""
        self.assertEqual(len(self.candidates), 1)

    def test_role_is_single_work(self) -> None:
        c = self.candidates[0]
        self.assertEqual(c.boundary_evidence.role, DirectoryRole.SINGLE_WORK)

    def test_media_context_is_tv(self) -> None:
        self.assertEqual(self.candidates[0].proposed_media_context, "tv")


class TestBoundaryFateContainer(unittest.TestCase):
    """Fate 系列容器 → series_container, 至少 2 个独立 WorkCandidate."""

    def setUp(self) -> None:
        node = _load("fate_container")
        self.candidates = analyze_boundaries(node, root_task_id="test-root-fate")

    def test_at_least_two_candidates(self) -> None:
        self.assertGreaterEqual(len(self.candidates), 2)

    def test_all_roles_are_series_container(self) -> None:
        for c in self.candidates:
            self.assertEqual(
                c.boundary_evidence.role,
                DirectoryRole.SERIES_CONTAINER,
                msg=f"Candidate '{c.display_label}' has unexpected role",
            )

    def test_known_works_are_candidates(self) -> None:
        labels = {c.display_label for c in self.candidates}
        self.assertIn("空之境界", labels)
        self.assertIn("Fate Zero", labels)

    def test_work_unit_ids_are_distinct(self) -> None:
        ids = [c.work_unit_id for c in self.candidates]
        self.assertEqual(len(ids), len(set(ids)))

    def test_all_candidates_have_source_paths(self) -> None:
        for c in self.candidates:
            self.assertTrue(len(c.source_paths) > 0)


class TestBoundarySubtitleOnly(unittest.TestCase):
    """只有字幕文件的目录 → subtitle_group."""

    def setUp(self) -> None:
        node = _load("subtitle_only")
        self.candidates = analyze_boundaries(node, root_task_id="test-root-sub")

    def test_exactly_one_candidate(self) -> None:
        self.assertEqual(len(self.candidates), 1)

    def test_role_is_subtitle_group(self) -> None:
        c = self.candidates[0]
        self.assertEqual(c.boundary_evidence.role, DirectoryRole.SUBTITLE_GROUP)


# ===========================================================================
# BoundaryAnalysis — semantic unit tests
# ===========================================================================

class TestSeasonDirNotIndependentWork(unittest.TestCase):
    """Season sub-directories must NOT become independent WorkCandidates
    when they are inside a multi-season root."""

    def test_season_dirs_treated_as_single_work(self) -> None:
        node = _load("single_tv_multiseason")
        candidates = analyze_boundaries(node, root_task_id="test-root")
        # Season 01/02/03 are NOT separate works
        self.assertEqual(len(candidates), 1)
        season_labels = {"Season 01", "Season 02", "Season 03"}
        for c in candidates:
            self.assertNotIn(c.display_label, season_labels)

    def test_season_subdir_analyzed_directly_returns_season_role(self) -> None:
        """If we analyse a season dir in isolation, it should report SEASON."""
        fixture = {
            "root": "/quark/影视/待刮削/绝命毒师/Season 01",
            "children": [
                {"name": "S01E01.mkv", "is_dir": False, "size": 1073741824},
            ],
        }
        node = build_source_inventory_from_fixture(fixture)
        candidates = analyze_boundaries(node, root_task_id="test-root")
        self.assertEqual(len(candidates), 1)
        self.assertEqual(candidates[0].boundary_evidence.role, DirectoryRole.SEASON)


class TestManifestConsistency(unittest.TestCase):
    """Manifest entries must each have a matching fixture directory."""

    def test_all_manifest_cases_have_fixture(self) -> None:
        manifest_path = _FIXTURE_DIR / "manifest.json"
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        for entry in manifest:
            case_id = entry["id"]
            fixture_path = _FIXTURE_DIR / case_id / "source_tree.json"
            self.assertTrue(
                fixture_path.exists(),
                msg=f"Fixture missing: {fixture_path}",
            )

    def test_manifest_cases_produce_expected_role(self) -> None:
        manifest_path = _FIXTURE_DIR / "manifest.json"
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        for entry in manifest:
            case_id = entry["id"]
            expected_role = entry.get("expected_role")
            if not expected_role:
                continue
            node = _load(case_id)
            candidates = analyze_boundaries(node, root_task_id=f"test-{case_id}")
            actual_roles = {c.boundary_evidence.role.value for c in candidates}
            self.assertIn(
                expected_role,
                actual_roles,
                msg=f"Case '{case_id}': expected role '{expected_role}' not in {actual_roles}",
            )

    def test_manifest_cases_produce_expected_count(self) -> None:
        manifest_path = _FIXTURE_DIR / "manifest.json"
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        for entry in manifest:
            case_id = entry["id"]
            expected = entry.get("expected_work_count")
            if expected is None:
                continue
            node = _load(case_id)
            candidates = analyze_boundaries(node, root_task_id=f"test-{case_id}")
            count = len(candidates)
            if isinstance(expected, int):
                self.assertEqual(
                    count, expected,
                    msg=f"Case '{case_id}': expected {expected} candidates, got {count}",
                )
            elif isinstance(expected, str) and expected.startswith(">="):
                threshold = int(expected[2:])
                self.assertGreaterEqual(
                    count, threshold,
                    msg=f"Case '{case_id}': expected >={threshold} candidates, got {count}",
                )


class TestSyntheticCases(unittest.TestCase):
    """Synthetic inline fixtures for edge cases not covered by file fixtures."""

    def _node(self, fixture_dict: dict) -> SourceNode:
        return build_source_inventory_from_fixture(fixture_dict)

    def test_flat_independent_feature_files_split_into_exact_movie_units(self) -> None:
        """Two titled feature files at the root are peer movies, not one job."""
        root = "/quark/影视/待刮削/paired-films"
        node = self._node({
            "root": root,
            "children": [
                {
                    "name": "[Group] First Feature Film [2160p][x265].mkv",
                    "is_dir": False,
                    "size": 300 * 1024 * 1024,
                },
                {
                    "name": "[Group] Second Feature Film [2160p][x265].mkv",
                    "is_dir": False,
                    "size": 301 * 1024 * 1024,
                },
            ],
        })
        candidates = analyze_boundaries(node, root_task_id="flat-pair")
        self.assertEqual(len(candidates), 2)
        self.assertTrue(all(c.proposed_media_context == "movie" for c in candidates))
        self.assertEqual(
            {path for c in candidates for path in c.source_paths},
            {
                f"{root}/[Group] First Feature Film [2160p][x265].mkv",
                f"{root}/[Group] Second Feature Film [2160p][x265].mkv",
            },
        )

    def test_flat_episode_like_files_remain_one_work(self) -> None:
        """Explicit episode coordinates must never trigger movie splitting."""
        root = "/quark/影视/待刮削/show"
        node = self._node({
            "root": root,
            "children": [
                {
                    "name": "Show S01E01 [2160p].mkv",
                    "is_dir": False,
                    "size": 300 * 1024 * 1024,
                },
                {
                    "name": "Show S01E02 [2160p].mkv",
                    "is_dir": False,
                    "size": 301 * 1024 * 1024,
                },
            ],
        })
        candidates = analyze_boundaries(node, root_task_id="flat-episodes")
        self.assertEqual(len(candidates), 1)
        self.assertEqual(candidates[0].boundary_key, root)

    def test_single_subdirectory_with_videos_is_single_work(self) -> None:
        fixture = {
            "root": "/quark/影视/待刮削/SomeMovie",
            "children": [
                {
                    "name": "Video",
                    "is_dir": True,
                    "children": [
                        {"name": "movie.mkv", "is_dir": False, "size": 5368709120},
                    ],
                }
            ],
        }
        node = self._node(fixture)
        candidates = analyze_boundaries(node, root_task_id="t")
        self.assertEqual(len(candidates), 1)
        self.assertEqual(candidates[0].boundary_evidence.role, DirectoryRole.SINGLE_WORK)

    def test_two_titled_children_gives_two_candidates(self) -> None:
        fixture = {
            "root": "/quark/影视/待刮削/Container",
            "children": [
                {
                    "name": "Work A",
                    "is_dir": True,
                    "children": [
                        {"name": "ep01.mkv", "is_dir": False, "size": 1073741824},
                    ],
                },
                {
                    "name": "Work B",
                    "is_dir": True,
                    "children": [
                        {"name": "ep01.mkv", "is_dir": False, "size": 1073741824},
                    ],
                },
            ],
        }
        node = self._node(fixture)
        candidates = analyze_boundaries(node, root_task_id="t")
        self.assertEqual(len(candidates), 2)

    def test_extras_subdir_excluded_from_series_container(self) -> None:
        """An 'Extras' directory must not make the root a SERIES_CONTAINER."""
        fixture = {
            "root": "/quark/影视/待刮削/SomeSeries",
            "children": [
                {
                    "name": "Extras",
                    "is_dir": True,
                    "children": [
                        {"name": "bonus.mkv", "is_dir": False, "size": 500000000},
                    ],
                },
                {
                    "name": "Season 01",
                    "is_dir": True,
                    "children": [
                        {"name": "S01E01.mkv", "is_dir": False, "size": 1073741824},
                    ],
                },
            ],
        }
        node = self._node(fixture)
        candidates = analyze_boundaries(node, root_task_id="t")
        # Should be single_work (multi-season), not series_container
        self.assertEqual(len(candidates), 1)
        self.assertEqual(candidates[0].boundary_evidence.role, DirectoryRole.SINGLE_WORK)

    def test_titled_movie_bundle_splits_into_dated_feature_units(self) -> None:
        """A titled bundle of dated one-video feature folders is a collection.

        ``02 剧场版4部（2001-2004）日英双语 内封+外挂字幕 1080P`` is a release
        package around four independently titled, independently dated feature
        films.  One WorkUnit cannot hold four different works, and the bundle
        label itself matches no TMDB title, so each dated feature folder must
        become its own movie-shaped unit.
        """
        root = "/quark/影视/待刮削/Example Franchise"
        def feature_dir(name: str, movie: str) -> dict:
            return {
                "name": name,
                "is_dir": True,
                "children": [
                    {"name": "简中.ass", "is_dir": False, "size": 110_000},
                    {"name": movie, "is_dir": False, "size": 20_000_000_000},
                ],
            }

        fixture = {
            "root": root,
            "children": [
                {
                    "name": "01 正片（2000）全24集 日中双语 1080P",
                    "is_dir": True,
                    "children": [
                        {
                            "name": "1080P 日中双语",
                            "is_dir": True,
                            "children": [
                                {"name": "01.mp4", "is_dir": False, "size": 1_073_741_824},
                                {"name": "02.mp4", "is_dir": False, "size": 1_073_741_824},
                            ],
                        },
                    ],
                },
                {
                    "name": "02 剧场版4部（2001-2004）日英双语 内封+外挂字幕 1080P",
                    "is_dir": True,
                    "children": [
                        feature_dir(
                            "01 穿越时空的思念（2001）日英双语 内封+外挂字幕 1080P",
                            "Example：穿越时空的思念.mkv",
                        ),
                        feature_dir(
                            "02 镜中的梦幻城（2002）日英双语 内封+外挂字幕 1080P",
                            "Example：镜中的梦幻城.mkv",
                        ),
                        feature_dir(
                            "03 天下霸道之剑（2003）日英双语 内封+外挂字幕 1080P",
                            "Example：天下霸道之剑.mkv",
                        ),
                        feature_dir(
                            "04 红莲之蓬莱岛（2004）日英双语 内封+外挂字幕 1080P",
                            "Example：红莲之蓬莱岛.mkv",
                        ),
                    ],
                },
            ],
        }
        candidates = analyze_boundaries(self._node(fixture), root_task_id="t")
        self.assertEqual(len(candidates), 5, msg=[c.display_label for c in candidates])
        movie_units = [c for c in candidates if c.proposed_media_context == "movie"]
        self.assertEqual(len(movie_units), 4)
        self.assertEqual(
            {c.display_label for c in movie_units},
            {
                "01 穿越时空的思念（2001）日英双语 内封+外挂字幕 1080P",
                "02 镜中的梦幻城（2002）日英双语 内封+外挂字幕 1080P",
                "03 天下霸道之剑（2003）日英双语 内封+外挂字幕 1080P",
                "04 红莲之蓬莱岛（2004）日英双语 内封+外挂字幕 1080P",
            },
        )
        # Every feature unit owns exactly its own directory scope, carrying the
        # external subtitle alongside the film.
        for unit in movie_units:
            self.assertEqual(len(unit.source_paths), 1)
            self.assertTrue(unit.source_paths[0].startswith(f"{root}/02 剧场版4部"))
        tv_units = [c for c in candidates if c.proposed_media_context != "movie"]
        self.assertEqual(len(tv_units), 1)
        self.assertEqual(tv_units[0].display_label, "01 正片（2000）全24集 日中双语 1080P")

    def test_movie_bundle_without_year_evidence_stays_one_unit(self) -> None:
        """Undated one-video folders could be episodes; fail closed."""
        root = "/quark/影视/待刮削/Example Franchise"
        fixture = {
            "root": root,
            "children": [
                {
                    "name": "01 正片（2000）全24集 日中双语 1080P",
                    "is_dir": True,
                    "children": [
                        {"name": "01.mp4", "is_dir": False, "size": 1_073_741_824},
                    ],
                },
                {
                    "name": "02 剧场版 日英双语 内封+外挂字幕 1080P",
                    "is_dir": True,
                    "children": [
                        {
                            "name": "穿越时空的思念",
                            "is_dir": True,
                            "children": [
                                {"name": "Example：穿越时空的思念.mkv", "is_dir": False, "size": 2_147_483_648},
                            ],
                        },
                        {
                            "name": "镜中的梦幻城",
                            "is_dir": True,
                            "children": [
                                {"name": "Example：镜中的梦幻城.mkv", "is_dir": False, "size": 2_147_483_648},
                            ],
                        },
                    ],
                },
            ],
        }
        candidates = analyze_boundaries(self._node(fixture), root_task_id="t")
        self.assertEqual(len(candidates), 2, msg=[c.display_label for c in candidates])

    def test_episode_coordinate_subdirs_never_split_as_features(self) -> None:
        """A per-episode folder layout is a TV shape even when episodes are big.

        Episode coordinates (``第01话``/``E01``/part tokens) and same-year
        folders are series evidence: the bundle must stay one unit and let
        C/D resolve it on the container's own title.
        """
        root = "/quark/影视/待刮削/Example Series"
        fixture = {
            "root": root,
            "children": [
                {
                    "name": "01 正片（2000）全26集",
                    "is_dir": True,
                    "children": [
                        {"name": "E01.mkv", "is_dir": False, "size": 1_073_741_824},
                        {"name": "E02.mkv", "is_dir": False, "size": 1_073_741_824},
                    ],
                },
                {
                    "name": "02 完结篇（2009）",
                    "is_dir": True,
                    "children": [
                        {
                            "name": "第01话「起点」（2009）",
                            "is_dir": True,
                            "children": [
                                {"name": "01.mkv", "is_dir": False, "size": 2_147_483_648},
                            ],
                        },
                        {
                            "name": "第02话「终章」（2009）",
                            "is_dir": True,
                            "children": [
                                {"name": "02.mkv", "is_dir": False, "size": 2_147_483_648},
                            ],
                        },
                    ],
                },
            ],
        }
        candidates = analyze_boundaries(self._node(fixture), root_task_id="t")
        self.assertEqual(len(candidates), 2, msg=[c.display_label for c in candidates])
        labels = {c.display_label for c in candidates}
        self.assertIn("02 完结篇（2009）", labels)

    def test_multiseason_root_splits_generic_nested_film_collection(self) -> None:
        """Season folders and a generic film group own disjoint WorkUnits.

        This covers a normal series root which carries its own ``SP`` material
        alongside a ``剧场版`` folder.  The nested titles must not be handed to
        the TV planner as if they were episodes of the main work.
        """
        root = "/quark/影视/待刮削/Example Show"
        fixture = {
            "root": root,
            "children": [
                {
                    "name": "S01",
                    "is_dir": True,
                    "children": [
                        {
                            "name": "Example.Show.S01E01.mkv",
                            "is_dir": False,
                            "size": 1_073_741_824,
                        },
                    ],
                },
                {
                    "name": "S02",
                    "is_dir": True,
                    "children": [
                        {
                            "name": "Example.Show.S02E01.mkv",
                            "is_dir": False,
                            "size": 1_073_741_824,
                        },
                    ],
                },
                {
                    "name": "SP",
                    "is_dir": True,
                    "children": [
                        {
                            "name": "Example.Show.S00E01.mkv",
                            "is_dir": False,
                            "size": 536_870_912,
                        },
                    ],
                },
                {
                    "name": "剧场版",
                    "is_dir": True,
                    "children": [
                        {
                            "name": "Example Feature (2019)",
                            "is_dir": True,
                            "children": [
                                {
                                    "name": "Example.Feature.2019.mkv",
                                    "is_dir": False,
                                    "size": 2_147_483_648,
                                },
                            ],
                        },
                        {
                            "name": "Example Reminiscence (2021)",
                            "is_dir": True,
                            "children": [
                                {
                                    "name": "Example.Reminiscence.2021.mkv",
                                    "is_dir": False,
                                    "size": 2_147_483_648,
                                },
                                {
                                    "name": "Example.Reminiscence.Promo.mkv",
                                    "is_dir": False,
                                    "size": 67_108_864,
                                },
                            ],
                        },
                    ],
                },
            ],
        }

        candidates = analyze_boundaries(self._node(fixture), root_task_id="t")

        self.assertEqual(len(candidates), 3)
        tv = next(candidate for candidate in candidates if candidate.display_label == "Example Show")
        self.assertEqual(tv.boundary_evidence.role, DirectoryRole.SINGLE_WORK)
        self.assertEqual(tv.proposed_media_context, "tv")
        self.assertEqual(tv.source_paths, (f"{root}/S01", f"{root}/S02", f"{root}/SP"))
        # SP is auxiliary; only explicit S01/S02 folders contribute claims.
        self.assertEqual(tv.claimed_seasons, (1, 2))

        films = [candidate for candidate in candidates if candidate is not tv]
        self.assertEqual(
            {candidate.source_paths for candidate in films},
            {
                (f"{root}/剧场版/Example Feature (2019)",),
                (f"{root}/剧场版/Example Reminiscence (2021)",),
            },
        )
        self.assertTrue(all(
            candidate.boundary_evidence.role == DirectoryRole.MOVIE_COLLECTION
            for candidate in films
        ))
        all_scopes = [scope for candidate in candidates for scope in candidate.source_paths]
        for left in all_scopes:
            for right in all_scopes:
                if left != right:
                    self.assertFalse(left.startswith(right + "/"))

    def test_multiseason_root_with_unknown_video_branch_stays_whole(self) -> None:
        """A non-generic nested branch cannot be silently treated as films."""
        root = "/quark/影视/待刮削/Example Show"
        fixture = {
            "root": root,
            "children": [
                *[
                    {
                        "name": f"S{season:02d}",
                        "is_dir": True,
                        "children": [
                            {
                                "name": f"Example.Show.S{season:02d}E01.mkv",
                                "is_dir": False,
                                "size": 1_073_741_824,
                            },
                        ],
                    }
                    for season in (1, 2)
                ],
                {
                    "name": "Aftershow",
                    "is_dir": True,
                    "children": [
                        {
                            "name": f"Aftershow.E{episode:02d}.mkv",
                            "is_dir": False,
                            "size": 536_870_912,
                        }
                        for episode in (1, 2)
                    ],
                },
            ],
        }

        candidates = analyze_boundaries(self._node(fixture), root_task_id="t")

        self.assertEqual(len(candidates), 1)
        self.assertEqual(candidates[0].source_paths, (root,))
        self.assertEqual(candidates[0].boundary_evidence.role, DirectoryRole.SINGLE_WORK)

    def test_decorated_generic_seasons_group_and_keep_year_marked_sibling(self) -> None:
        """Release noise around season folders must not become TMDB titles."""
        root = "/quark/影视/待刮削/Rick bundle"
        fixture = {
            "root": root,
            "children": [
                {
                    "name": "第一季（2013）全2集 内封字幕 1080P",
                    "is_dir": True,
                    "children": [
                        {"name": "Rick.And.Morty.S01E01.mkv", "is_dir": False, "size": 2_000_000},
                        {"name": "Rick.And.Morty.S01E02.mkv", "is_dir": False, "size": 2_000_000},
                    ],
                },
                {
                    "name": "第二季（2015）全2集 内封字幕 1080P",
                    "is_dir": True,
                    "children": [
                        {"name": "Rick.And.Morty.S02E01.mkv", "is_dir": False, "size": 2_000_000},
                        {"name": "Rick.And.Morty.S02E02.mkv", "is_dir": False, "size": 2_000_000},
                    ],
                },
                {
                    "name": "日漫版（2024）全2集",
                    "is_dir": True,
                    "children": [
                        {"name": "Rick Anime S01E01.mkv", "is_dir": False, "size": 2_000_000},
                        {"name": "Rick Anime S01E02.mkv", "is_dir": False, "size": 2_000_000},
                    ],
                },
            ],
        }

        candidates = analyze_boundaries(self._node(fixture), root_task_id="t")

        self.assertEqual(len(candidates), 2)
        main = next(candidate for candidate in candidates if candidate.display_label == "Rick bundle")
        self.assertEqual(main.proposed_media_context, "tv")
        self.assertEqual(main.claimed_seasons, (1, 2))
        self.assertEqual(len(main.source_paths), 2)
        self.assertEqual(
            next(candidate for candidate in candidates if candidate is not main).display_label,
            "日漫版（2024）全2集",
        )
        sibling = next(candidate for candidate in candidates if candidate is not main)
        self.assertEqual(sibling.claimed_seasons, (1,))

    def test_decorated_generic_seasons_do_not_split_unproven_aftershow(self) -> None:
        """A bare titled branch without release evidence stays fail-closed."""
        root = "/quark/影视/待刮削/Example Show"
        fixture = {
            "root": root,
            "children": [
                {
                    "name": "第一季（2013）全2集",
                    "is_dir": True,
                    "children": [
                        {"name": "Example.Show.S01E01.mkv", "is_dir": False, "size": 2_000_000},
                        {"name": "Example.Show.S01E02.mkv", "is_dir": False, "size": 2_000_000},
                    ],
                },
                {
                    "name": "第二季（2015）全2集",
                    "is_dir": True,
                    "children": [
                        {"name": "Example.Show.S02E01.mkv", "is_dir": False, "size": 2_000_000},
                        {"name": "Example.Show.S02E02.mkv", "is_dir": False, "size": 2_000_000},
                    ],
                },
                {
                    "name": "Aftershow",
                    "is_dir": True,
                    "children": [
                        {"name": "Aftershow.E01.mkv", "is_dir": False, "size": 2_000_000},
                    ],
                },
            ],
        }

        candidates = analyze_boundaries(self._node(fixture), root_task_id="t")

        self.assertEqual(len(candidates), 1)
        self.assertEqual(candidates[0].source_paths, (root,))

    def test_generic_season_keeps_unknown_bracketed_title_evidence(self) -> None:
        """Title-shaped bracket contents must not become structural seasons."""
        for name in (
            "[Attack on Titan] Season 1",
            "Season 1 (The Expanse)",
        ):
            with self.subTest(name=name):
                self.assertIsNone(_generic_season_child(name))

        self.assertEqual(
            _generic_season_child("第六季（2022）全10集 内封字幕 1080P"),
            6,
        )

    def test_declared_season_bare_numeric_run_rejects_duplicate_episode_versions(self) -> None:
        """Two physical versions of one ordinal are not unique season proof."""
        node = self._node({
            "root": "/quark/影视/待刮削/Example Show/Season 01",
            "children": [
                {"name": name, "is_dir": False, "size": 2_000_000}
                for name in ("01.mkv", "01.v2.mkv", "02.mkv", "03.mkv", "04.mkv")
            ],
        })

        self.assertFalse(_directory_matches_declared_season(node, 1))

    def test_empty_directory_returns_uncertain(self) -> None:
        fixture = {
            "root": "/quark/影视/待刮削/EmptyDir",
            "children": [],
        }
        node = self._node(fixture)
        candidates = analyze_boundaries(node, root_task_id="t")
        self.assertEqual(len(candidates), 1)
        self.assertEqual(candidates[0].boundary_evidence.role, DirectoryRole.UNCERTAIN)

    def test_optical_disc_images_park_the_whole_source_until_read_only_expansion(self) -> None:
        """Image filenames must not invent season/title boundaries or video rows."""
        fixture = {
            "root": "/quark/影视/待刮削/Disc bundle",
            "children": [
                {
                    "name": "Season 01",
                    "is_dir": True,
                    "children": [
                        {"name": "Disc 1.iso", "is_dir": False, "size": 45 * 1024**3},
                        {"name": "Disc 2.iso", "is_dir": False, "size": 45 * 1024**3},
                    ],
                },
                {
                    "name": "Season 02",
                    "is_dir": True,
                    "children": [
                        {"name": "Disc 1.iso", "is_dir": False, "size": 45 * 1024**3},
                    ],
                },
            ],
        }
        node = self._node(fixture)
        self.assertEqual(count_video_files(node), 0)
        candidates = analyze_boundaries(node, root_task_id="disc-root")
        self.assertEqual(len(candidates), 1)
        candidate = candidates[0]
        self.assertEqual(candidate.boundary_evidence.role, DirectoryRole.UNCERTAIN)
        self.assertEqual(candidate.proposed_media_context, "unknown")
        self.assertTrue(candidate.requires_content_expansion)
        self.assertEqual(candidate.source_paths, (node.path,))
        self.assertIn("光盘镜像", candidate.boundary_evidence.reasons[0])

    def test_font_pack_exe_does_not_park_source(self) -> None:
        """A ``[Fonts].exe`` installer must not block a normal video source."""
        fixture = {
            "root": "/quark/影视/待刮削/W 4k 我推的孩子",
            "children": [
                {"name": "[Sakurato] Oshi no Ko [Fonts].exe", "is_dir": False, "size": 54615467},
                {"name": "[Ygm] Oshi no Ko [01][Ma10p_2160p].mkv", "is_dir": False, "size": 7 * 1024**3},
                {"name": "[Ygm] Oshi no Ko [02][Ma10p_2160p].mkv", "is_dir": False, "size": 2 * 1024**3},
            ],
        }
        node = self._node(fixture)
        candidates = analyze_boundaries(node, root_task_id="font-root")
        self.assertEqual(len(candidates), 1)
        candidate = candidates[0]
        self.assertNotEqual(candidate.boundary_evidence.role, DirectoryRole.UNCERTAIN)
        self.assertFalse(candidate.requires_content_expansion)

    def test_chinese_season_dir_is_season(self) -> None:
        """'第1季' and similar Chinese patterns must match the season rule."""
        fixture = {
            "root": "/quark/影视/待刮削/某剧/第1季",
            "children": [
                {"name": "EP01.mkv", "is_dir": False, "size": 1073741824},
            ],
        }
        node = self._node(fixture)
        candidates = analyze_boundaries(node, root_task_id="t")
        self.assertEqual(candidates[0].boundary_evidence.role, DirectoryRole.SEASON)

    def test_chinese_numeral_season_dir_is_season(self) -> None:
        """Bounded Chinese cardinal season labels are structural evidence."""
        for label, expected in (("第一季", 1), ("第十一季", 11), ("第二十一季", 21)):
            with self.subTest(label=label):
                fixture = {
                    "root": f"/quark/影视/待刮削/某剧/{label}",
                    "children": [
                        {
                            "name": f"Example.Show.S{expected:02d}E01.mkv",
                            "is_dir": False,
                            "size": 1_073_741_824,
                        },
                    ],
                }
                candidate = analyze_boundaries(self._node(fixture), root_task_id="t")[0]
                self.assertEqual(candidate.boundary_evidence.role, DirectoryRole.SEASON)

    def test_malformed_chinese_season_label_is_not_a_season(self) -> None:
        for label in ("第十百季", "第十零季", "第零十季"):
            with self.subTest(label=label):
                fixture = {
                    "root": f"/quark/影视/待刮削/某剧/{label}",
                    "children": [
                        {"name": "Example.Show.S01E01.mkv", "is_dir": False, "size": 1_073_741_824},
                    ],
                }
                candidate = analyze_boundaries(self._node(fixture), root_task_id="t")[0]
                self.assertEqual(candidate.boundary_evidence.role, DirectoryRole.SINGLE_WORK)

    def test_s01_dir_is_season(self) -> None:
        fixture = {
            "root": "/quark/影视/待刮削/SomeShow/S01",
            "children": [
                {"name": "ep01.mkv", "is_dir": False, "size": 1073741824},
            ],
        }
        node = self._node(fixture)
        candidates = analyze_boundaries(node, root_task_id="t")
        self.assertEqual(candidates[0].boundary_evidence.role, DirectoryRole.SEASON)

    def test_work_unit_id_stable_across_calls(self) -> None:
        fixture = {
            "root": "/quark/影视/待刮削/Fate系列",
            "children": [
                {
                    "name": "空之境界",
                    "is_dir": True,
                    "children": [
                        {"name": "01.mkv", "is_dir": False, "size": 4000000000},
                    ],
                },
                {
                    "name": "Fate Zero",
                    "is_dir": True,
                    "children": [
                        {"name": "01.mkv", "is_dir": False, "size": 1000000000},
                    ],
                },
            ],
        }
        node = build_source_inventory_from_fixture(fixture)
        c1 = analyze_boundaries(node, root_task_id="root-xyz")
        c2 = analyze_boundaries(node, root_task_id="root-xyz")
        ids1 = {c.work_unit_id for c in c1}
        ids2 = {c.work_unit_id for c in c2}
        self.assertEqual(ids1, ids2)

    def test_decorated_multi_season_cohort_keeps_aftershow_separate(self) -> None:
        fixture = {
            "root": "/quark/影视/待刮削/Northwind Bundle",
            "children": [
                *[
                    {
                        "name": (
                            f"Northwind.Show.S{season:02d}."
                            f"{'Blu-ray' if season < 3 else 'WEB-DL'}.x265"
                        ),
                        "is_dir": True,
                        "children": [
                            {
                                "name": f"Northwind.Show.S{season:02d}E01.1080p.mkv",
                                "is_dir": False,
                                "size": 1073741824,
                            },
                        ],
                    }
                    for season in (1, 2, 3)
                ],
                {
                    "name": "Northwind.Show.S04.WEB-DL.x265",
                    "is_dir": True,
                    "children": [],
                },
                {
                    "name": "Northwind.Aftershow",
                    "is_dir": True,
                    "children": [
                        {"name": f"Northwind.Aftershow.E{episode:02d}.mkv", "is_dir": False, "size": 10}
                        for episode in range(1, 7)
                    ],
                },
            ],
        }

        candidates = analyze_boundaries(self._node(fixture), root_task_id="t")

        self.assertEqual(len(candidates), 2)
        cohort = next(candidate for candidate in candidates if len(candidate.source_paths) == 4)
        aftershow = next(candidate for candidate in candidates if candidate is not cohort)
        self.assertEqual(cohort.boundary_evidence.role, DirectoryRole.SINGLE_WORK)
        self.assertEqual(cohort.claimed_seasons, (1, 2, 3, 4))
        self.assertEqual(
            cohort.source_paths,
            tuple(
                (
                    f"/quark/影视/待刮削/Northwind Bundle/Northwind.Show.S{season:02d}."
                    f"{'Blu-ray' if season < 3 else 'WEB-DL'}.x265"
                )
                for season in range(1, 5)
            ),
        )
        self.assertEqual(aftershow.source_paths, ("/quark/影视/待刮削/Northwind Bundle/Northwind.Aftershow",))
        self.assertEqual(set(cohort.source_paths).intersection(aftershow.source_paths), set())

    def test_decorated_duplicate_season_editions_fail_closed_without_cohort(self) -> None:
        fixture = {
            "root": "/quark/影视/待刮削/Northwind Bundle",
            "children": [
                *[
                    {
                        "name": f"Northwind.Show.S01.{release}",
                        "is_dir": True,
                        "children": [
                            {"name": "Northwind.Show.S01E01.mkv", "is_dir": False, "size": 10},
                        ],
                    }
                    for release in ("1080p", "2160p")
                ],
                {
                    "name": "Northwind.Show.S02.1080p",
                    "is_dir": True,
                    "children": [
                        {"name": "Northwind.Show.S02E01.mkv", "is_dir": False, "size": 10},
                    ],
                },
            ],
        }

        candidates = analyze_boundaries(self._node(fixture), root_task_id="t")

        self.assertEqual(len(candidates), 3)
        self.assertTrue(all(len(candidate.source_paths) == 1 for candidate in candidates))
        # Duplicate editions stay separate candidates (fail closed); each
        # still records the explicit season marker from its own directory
        # name as a B/W claimed-season fact.
        self.assertEqual(
            [candidate.claimed_seasons for candidate in candidates],
            [(1,), (1,), (2,)],
        )

    def test_decorated_stale_empty_season_is_not_claimed_as_a_gap(self) -> None:
        fixture = {
            "root": "/quark/影视/待刮削/Northwind Bundle",
            "children": [
                {"name": "Northwind.Show.S01.WEB-DL", "is_dir": True, "children": []},
                *[
                    {
                        "name": f"Northwind.Show.S{season:02d}.WEB-DL",
                        "is_dir": True,
                        "children": [
                            {"name": f"Northwind.Show.S{season:02d}E01.mkv", "is_dir": False, "size": 10},
                        ],
                    }
                    for season in (2, 3)
                ],
            ],
        }

        candidates = analyze_boundaries(self._node(fixture), root_task_id="t")

        self.assertEqual(len(candidates), 1)
        self.assertEqual(candidates[0].claimed_seasons, (2, 3))
        self.assertNotIn(
            "/quark/影视/待刮削/Northwind Bundle/Northwind.Show.S01.WEB-DL",
            candidates[0].source_paths,
        )

    def test_rooted_decorated_subtitle_only_season_is_a_proven_claim(self) -> None:
        """A declared in-between season may be retained only with exact sidecars.

        The root has ordinary qualified video evidence for several seasons and
        a Chinese decorated Season 06 directory containing a complete S06E
        subtitle sequence but no video.  B/W must keep one root-owned TV unit
        and preserve the declared empty season for J; it must not turn the
        subtitle directory into a second work or infer an arbitrary season.
        """
        fixture = {
            "root": "/quark/影视/待刮削/Northwind Root",
            "children": [
                *[
                    {
                        "name": f"第 {season} 季 - 1080p BluRay REMUX",
                        "is_dir": True,
                        "children": (
                            [
                                {
                                    "name": f"Northwind.Show.S{season:02d}E01.mkv",
                                    "is_dir": False,
                                    "size": 1_073_741_824,
                                },
                            ]
                            if season != 6
                            else [
                                {
                                    "name": f"Northwind.Show.S06E{episode:02d}.sup",
                                    "is_dir": False,
                                    "size": 1_024,
                                }
                                for episode in range(1, 11)
                            ]
                        ),
                    }
                    for season in range(1, 9)
                ],
                *[
                    {
                        "name": f"S09E{episode:02d}.mkv",
                        "is_dir": False,
                        "size": 1_073_741_824,
                    }
                    for episode in range(1, 11)
                ],
                {
                    "name": "第 99 季 - 1080p BluRay REMUX",
                    "is_dir": True,
                    "children": [{
                        "name": "Northwind.Show.S99E01.sup",
                        "is_dir": False,
                        "size": 1_024,
                    }],
                },
            ],
        }

        candidates = analyze_boundaries(self._node(fixture), root_task_id="t")

        self.assertEqual(len(candidates), 1)
        candidate = candidates[0]
        self.assertEqual(candidate.boundary_evidence.role, DirectoryRole.SINGLE_WORK)
        self.assertEqual(candidate.source_paths, (fixture["root"],))
        self.assertEqual(candidate.claimed_seasons, tuple(range(1, 10)))

    def test_decorated_subtitle_only_season_needs_two_video_anchors(self) -> None:
        fixture = {
            "root": "/quark/影视/待刮削/Northwind Root",
            "children": [
                {
                    "name": "第 1 季 - 1080p BluRay REMUX",
                    "is_dir": True,
                    "children": [{
                        "name": "Northwind.Show.S01E01.mkv",
                        "is_dir": False,
                        "size": 1_073_741_824,
                    }],
                },
                {
                    "name": "第 2 季 - 1080p BluRay REMUX",
                    "is_dir": True,
                    "children": [{
                        "name": "Northwind.Show.S02E01.sup",
                        "is_dir": False,
                        "size": 1_024,
                    }],
                },
            ],
        }

        candidates = analyze_boundaries(self._node(fixture), root_task_id="t")

        self.assertEqual(len(candidates), 1)
        self.assertEqual(candidates[0].claimed_seasons, ())

    def test_chinese_decorated_episode_directory_is_not_a_season_boundary(self) -> None:
        fixture = {
            "root": "/quark/影视/待刮削/Northwind/第 1 季 S01E01",
            "children": [{
                "name": "Northwind.Show.S01E01.mkv",
                "is_dir": False,
                "size": 1_073_741_824,
            }],
        }

        candidates = analyze_boundaries(self._node(fixture), root_task_id="t")

        self.assertEqual(candidates[0].boundary_evidence.role, DirectoryRole.SINGLE_WORK)

    def test_theatrical_marked_one_video_folders_split_without_years(self) -> None:
        """A ``剧场版 X`` bundle splits per film even without year evidence.

        ``排球少年 剧场版合集`` wraps five separately released theatrical
        films, one folder per film, each folder named with the bounded
        theatrical-form marker (``剧场版``) plus a concrete film title and
        each holding exactly one feature-sized video.  The undated shape
        cannot use the year-corroborated nested-collection rule, but the
        explicit theatrical form marker plus a substantial standalone title
        is its own bounded evidence: one WorkUnit cannot own five different
        films, and the bundle label itself matches no TMDB work.
        """
        root = "/quark/影视/待刮削/P 4k 排球少年"
        def film_dir(label: str, movie: str) -> dict:
            return {
                "name": label,
                "is_dir": True,
                "children": [
                    {"name": "简中.ass", "is_dir": False, "size": 110_000},
                    {"name": movie, "is_dir": False, "size": 20_000_000_000},
                ],
            }

        fixture = {
            "root": root,
            "children": [
                {
                    "name": "排球少年 剧场版合集",
                    "is_dir": True,
                    "children": [
                        film_dir(
                            "剧场版 排球少年 垃圾场决战",
                            "[Ygm] Haikyuu!! Gomisuteba no Kessen [Ma10p_2160p][x265_TrueHD_ass].mkv",
                        ),
                        film_dir(
                            "剧场版 排球少年 才能与感觉",
                            "[Ygm] Haikyuu!! Sainou to Sense [Ma10p_2160p][x265_flac_ass].mkv",
                        ),
                        film_dir(
                            "剧场版 排球少年 结束与开始",
                            "[Ygm] Haikyuu!! Owari to Hajimari [Ma10p_2160p][x265_flac_ass].mkv",
                        ),
                        film_dir(
                            "剧场版 排球少年 胜者与败者",
                            "[Ygm] Haikyuu!! Shousha to Haisha [Ma10p_2160p][x265_flac_ass].mkv",
                        ),
                        film_dir(
                            "剧场版 排球少年 观念之战",
                            "[Ygm] Haikyuu!! Concept no Tatakai [Ma10p_2160p][x265_flac_ass].mkv",
                        ),
                    ],
                },
                {
                    "name": "排球少年第二季",
                    "is_dir": True,
                    "children": [
                        {
                            "name": "[Ygm] Haikyuu!! 2nd Season [01][Ma10p_2160p][x265_flac_ass].mkv",
                            "is_dir": False,
                            "size": 3_000_000_000,
                        },
                    ],
                },
            ],
        }
        candidates = analyze_boundaries(self._node(fixture), root_task_id="t")
        self.assertEqual(
            len(candidates), 6, msg=[c.display_label for c in candidates]
        )
        self.assertTrue(all(c.proposed_media_context == "movie" for c in candidates[:5]))
        self.assertEqual(
            {c.display_label for c in candidates},
            {
                "剧场版 排球少年 垃圾场决战",
                "剧场版 排球少年 才能与感觉",
                "剧场版 排球少年 结束与开始",
                "剧场版 排球少年 胜者与败者",
                "剧场版 排球少年 观念之战",
                "排球少年第二季",
            },
        )
        for unit in candidates[:5]:
            self.assertEqual(len(unit.source_paths), 1)
            self.assertTrue(
                unit.source_paths[0].startswith(f"{root}/排球少年 剧场版合集/")
            )

    def test_theatrical_marker_alone_does_not_split_titled_children(self) -> None:
        """``剧场版`` prefix without a concrete film title fails closed.

        A bundle whose children share one theatrical marker but carry no
        distinct substantial titles (bare ordinals, generic labels) is not
        proven to be one-film-per-folder; the whole-child boundary stays.
        """
        root = "/quark/影视/待刮削/Example Bundle"
        fixture = {
            "root": root,
            "children": [
                {
                    "name": "剧场版合集",
                    "is_dir": True,
                    "children": [
                        {
                            "name": "剧场版 01",
                            "is_dir": True,
                            "children": [
                                {"name": "Film 01.mkv", "is_dir": False, "size": 20_000_000_000},
                            ],
                        },
                        {
                            "name": "剧场版 02",
                            "is_dir": True,
                            "children": [
                                {"name": "Film 02.mkv", "is_dir": False, "size": 20_000_000_000},
                            ],
                        },
                    ],
                },
                {
                    "name": "正片第一季",
                    "is_dir": True,
                    "children": [
                        {"name": "Show [01].mkv", "is_dir": False, "size": 3_000_000_000},
                    ],
                },
            ],
        }
        candidates = analyze_boundaries(self._node(fixture), root_task_id="t")
        bundle_units = [
            c for c in candidates if "剧场版" in c.display_label
        ]
        self.assertEqual(len(bundle_units), 1, msg=[c.display_label for c in candidates])

    def test_terminal_collection_label_bundle_splits_plain_film_dirs(self) -> None:
        """``来自深渊 剧场版合集`` splits even when film dirs carry no marker.

        A bundle whose normalized name ends with a generic film-collection
        role label (``剧场版合集``) declares the one-folder-per-film form
        itself.  The per-film titles are plain franchise subtitles with no
        year and no theatrical marker of their own (``来自深渊 启程之拂晓``),
        so the terminal bundle label substitutes for the per-child evidence
        while every other movie-shape proof still applies.  Residual
        NCOP/NCED-only season folders beside it stay whole-child units.
        """
        root = "/quark/影视/待刮削/L 4k 来自深渊"
        def film_dir(label: str, movie: str) -> dict:
            return {
                "name": label,
                "is_dir": True,
                "children": [
                    {"name": movie, "is_dir": False, "size": 8_000_000_000},
                ],
            }

        fixture = {
            "root": root,
            "children": [
                {
                    "name": "来自深渊",
                    "is_dir": True,
                    "children": [
                        {"name": "[TUDO&Ygm] Made in Abyss [NCED01][Ma10p_2160p][x265_flac].mkv", "is_dir": False, "size": 162_337_851},
                        {"name": "[TUDO&Ygm] Made in Abyss [NCOP01][Ma10p_2160p][x265_flac].mkv", "is_dir": False, "size": 228_342_112},
                        {"name": "备份字幕", "is_dir": True, "children": []},
                    ],
                },
                {
                    "name": "来自深渊 剧场版合集",
                    "is_dir": True,
                    "children": [
                        film_dir(
                            "来自深渊 启程之拂晓",
                            "[TUDO&Ygm] Made in Abyss Movie 1 Tabidachi no Yoake [Ma10p_2160p][x265_flac7.1_ass].mkv",
                        ),
                        film_dir(
                            "来自深渊 漂泊之黄昏",
                            "[TUDO&Ygm] Made in Abyss Movie 2 Hourou Suru Tasogare [Ma10p_2160p][x265_flac7.1_ass].mkv",
                        ),
                        film_dir(
                            "来自深渊 深魂之黎明",
                            "[TUDO&Ygm] Made in Abyss Movie 3 Fukaki Tamashii no Reimei [Ma10p_2160p][x265_flac7.1_ass].mkv",
                        ),
                    ],
                },
                {
                    "name": "来自深渊 烈日的黄金乡",
                    "is_dir": True,
                    "children": [
                        {"name": "[TUDO&Ygm] Made in Abyss Retsujitsu no Ougonkyou [NCED][Ma10p_2160p][x265_flac].mkv", "is_dir": False, "size": 235_288_798},
                        {"name": "备份字幕", "is_dir": True, "children": []},
                    ],
                },
            ],
        }
        candidates = analyze_boundaries(self._node(fixture), root_task_id="t")
        self.assertEqual(len(candidates), 5, msg=[c.display_label for c in candidates])
        bundle_prefix = f"{root}/来自深渊 剧场版合集/"
        movie_units = [
            c for c in candidates
            if all(path.startswith(bundle_prefix) for path in c.source_paths)
        ]
        self.assertEqual(
            {c.display_label for c in movie_units},
            {"来自深渊 启程之拂晓", "来自深渊 漂泊之黄昏", "来自深渊 深魂之黎明"},
        )
        self.assertTrue(all(c.proposed_media_context == "movie" for c in movie_units))
        for unit in movie_units:
            self.assertEqual(len(unit.source_paths), 1)
        # The NCOP/NCED-only season folders remain whole-child boundaries and
        # never absorb the film scopes.
        self.assertEqual(
            {
                c.display_label for c in candidates
                if not all(path.startswith(bundle_prefix) for path in c.source_paths)
            },
            {"来自深渊", "来自深渊 烈日的黄金乡"},
        )

    def test_nested_multi_work_container_recurses_per_work(self) -> None:
        """A franchise grab-bag child splits into its independent works.

        ``其它`` beside the numbered seasons holds 卫宫家 (a 13-video pack),
        a dated one-video feature, and a ten-film 空之境界 bundle — three
        independent works, none of them one unmatchable whole-child unit.
        The recursion is gated: every video-bearing child must be
        independently work-shaped (multi-video, or dated/theatrical with no
        episode coordinates), so undated one-video bundles and per-episode
        directories keep their fail-closed verdicts.
        """
        root = "/quark/影视/待刮削/Fate"
        def video_dir(label, count=1, dated=True):
            return {
                "name": label,
                "is_dir": True,
                "children": [
                    {"name": f"{number:02d}.mkv", "is_dir": False, "size": 800_000_000}
                    for number in range(1, count + 1)
                ],
            }
        fixture = {
            "root": root,
            "children": [
                video_dir("01 命运之夜（2006）全24集", count=24),
                {
                    "name": "其它",
                    "is_dir": True,
                    "children": [
                        video_dir("卫宫家今天的饭（2017）全13集", count=13),
                        video_dir("命运 奇异赝品 黎明低语（2023）", count=1),
                        {
                            "name": "空之境界 1-10部 4K",
                            "is_dir": True,
                            "children": [
                                video_dir(f"0{index} 第{index}章 俯瞰风景（2007）", count=1)
                                for index in range(1, 10)
                            ],
                        },
                    ],
                },
            ],
        }
        candidates = analyze_boundaries(self._node(fixture), root_task_id="t")
        labels = [c.display_label for c in candidates]
        self.assertIn("卫宫家今天的饭（2017）全13集", labels, msg=labels)
        self.assertIn("命运 奇异赝品 黎明低语（2023）", labels, msg=labels)
        # The 空之境界 bundle is itself a movie collection of dated films.
        self.assertTrue(
            any("俯瞰风景" in label for label in labels), msg=labels,
        )
        self.assertNotIn("其它", labels, msg=labels)

    def test_undated_nested_bundle_stays_whole_child(self) -> None:
        """The recursion respects the undated one-video fail-closed rule."""
        root = "/quark/影视/待刮削/Example Franchise"
        fixture = {
            "root": root,
            "children": [
                {
                    "name": "01 正片（2000）全24集 日中双语 1080P",
                    "is_dir": True,
                    "children": [
                        {"name": "01.mp4", "is_dir": False, "size": 1_073_741_824},
                    ],
                },
                {
                    "name": "02 剧场版 日英双语 内封+外挂字幕 1080P",
                    "is_dir": True,
                    "children": [
                        {
                            "name": "穿越时空的思念",
                            "is_dir": True,
                            "children": [
                                {"name": "Example：穿越时空的思念.mkv", "is_dir": False, "size": 2_147_483_648},
                            ],
                        },
                        {
                            "name": "镜中的梦幻城",
                            "is_dir": True,
                            "children": [
                                {"name": "Example：镜中的梦幻城.mkv", "is_dir": False, "size": 2_147_483_648},
                            ],
                        },
                    ],
                },
            ],
        }
        candidates = analyze_boundaries(self._node(fixture), root_task_id="t")
        labels = [c.display_label for c in candidates]
        self.assertEqual(len(candidates), 2, msg=labels)
        self.assertIn("02 剧场版 日英双语 内封+外挂字幕 1080P", labels)

    def test_season_marked_filenames_never_split_as_flat_movies(self) -> None:
        """``…第四季 - 01`` files are one season's episodes, not N films.

        A WEB-DL season batch names each file with the show title, its own
        season marker, and a release ordinal (``[ANi] Re：從零開始的異世界
        生活 第四季 - 01 [1080P][Baha][WEB-DL]…``).  The flat movie splitter
        used to shred it into one movie unit per file — twelve unmatchable
        shards.  A filename carrying a season marker is TV evidence; the
        splitter fails closed and the ordinary TV boundary owns the batch.
        """
        root = "/quark/影视/待刮削/Re：從零開始的異世界生活"
        fixture = {
            "root": root,
            "children": [
                {
                    "name": "07. 第四季",
                    "is_dir": True,
                    "children": [
                        {
                            "name": f"[ANi] Re：從零開始的異世界生活 第四季 - {number:02d} [1080P][Baha][WEB-DL][AAC AVC][CHT].mp4",
                            "is_dir": False,
                            "size": 400_000_000 + number * 1024,
                        }
                        for number in range(1, 13)
                    ],
                },
            ],
        }
        candidates = analyze_boundaries(self._node(fixture), root_task_id="t")
        self.assertEqual(len(candidates), 1, msg=[c.display_label for c in candidates])
        self.assertNotEqual(
            candidates[0].proposed_media_context, "movie",
            "season-marked batch must not be split into movie units",
        )

    def test_non_terminal_collection_word_bundle_stays_whole_child(self) -> None:
        """A collection word that is not a terminal role label is decoration.

        ``来自深渊 蓝光合集`` ends with ``合集``, which is not one of the
        generic film-collection role labels, and ``蓝光`` mid-name is release
        noise rather than a collection declaration.  Plain undated film dirs
        inside it stay one whole-child boundary, exactly like the mid-name
        ``02 剧场版 日英双语 …`` bundle rule.
        """
        root = "/quark/影视/待刮削/L 4k 来自深渊"
        def film_dir(label: str, movie: str) -> dict:
            return {
                "name": label,
                "is_dir": True,
                "children": [
                    {"name": movie, "is_dir": False, "size": 8_000_000_000},
                ],
            }

        fixture = {
            "root": root,
            "children": [
                {
                    "name": "来自深渊 蓝光合集",
                    "is_dir": True,
                    "children": [
                        film_dir(
                            "来自深渊 启程之拂晓",
                            "[TUDO&Ygm] Made in Abyss Movie 1 Tabidachi no Yoake [Ma10p_2160p][x265_flac7.1_ass].mkv",
                        ),
                        film_dir(
                            "来自深渊 漂泊之黄昏",
                            "[TUDO&Ygm] Made in Abyss Movie 2 Hourou Suru Tasogare [Ma10p_2160p][x265_flac7.1_ass].mkv",
                        ),
                    ],
                },
                {
                    "name": "来自深渊 烈日的黄金乡",
                    "is_dir": True,
                    "children": [
                        {"name": "[TUDO&Ygm] Made in Abyss Retsujitsu no Ougonkyou [NCED][Ma10p_2160p][x265_flac].mkv", "is_dir": False, "size": 235_288_798},
                    ],
                },
            ],
        }
        candidates = analyze_boundaries(self._node(fixture), root_task_id="t")
        self.assertEqual(len(candidates), 2, msg=[c.display_label for c in candidates])
        self.assertIn("来自深渊 蓝光合集", {c.display_label for c in candidates})

    def test_sole_video_child_bundle_splits_before_whole_root_fallback(self) -> None:
        """A consumed intake whose only video child is a bundle still splits.

        After the TV units of ``L 4k 来自深渊`` were consumed, the intake
        held one video-bearing child: the terminal-collection film bundle.
        The whole-root single-work fallback must not swallow it — the
        conservative per-film splitter decides, so each film gets its own
        unit instead of one unmatchable mixed root.
        """
        root = "/quark/影视/待刮削/L 4k 来自深渊"
        def film_dir(label: str, movie: str) -> dict:
            return {
                "name": label,
                "is_dir": True,
                "children": [
                    {"name": movie, "is_dir": False, "size": 8_000_000_000},
                ],
            }

        fixture = {
            "root": root,
            "children": [
                {"name": "海量4K高清资源文档合集.jpg", "is_dir": False, "size": 86_916},
                {"name": "防失联永久链接.jpg", "is_dir": False, "size": 85_918},
                {
                    "name": "来自深渊 剧场版合集",
                    "is_dir": True,
                    "children": [
                        film_dir(
                            "来自深渊 启程之拂晓",
                            "[TUDO&Ygm] Made in Abyss Movie 1 Tabidachi no Yoake [Ma10p_2160p][x265_flac7.1_ass].mkv",
                        ),
                        film_dir(
                            "来自深渊 漂泊之黄昏",
                            "[TUDO&Ygm] Made in Abyss Movie 2 Hourou Suru Tasogare [Ma10p_2160p][x265_flac7.1_ass].mkv",
                        ),
                        film_dir(
                            "来自深渊 深魂之黎明",
                            "[TUDO&Ygm] Made in Abyss Movie 3 Fukaki Tamashii no Reimei [Ma10p_2160p][x265_flac7.1_ass].mkv",
                        ),
                    ],
                },
                {"name": "玛露露库的日常", "is_dir": True, "children": []},
            ],
        }
        candidates = analyze_boundaries(self._node(fixture), root_task_id="t")
        self.assertEqual(
            {c.display_label for c in candidates},
            {"来自深渊 启程之拂晓", "来自深渊 漂泊之黄昏", "来自深渊 深魂之黎明"},
            msg=[c.display_label for c in candidates],
        )
        self.assertTrue(all(c.proposed_media_context == "movie" for c in candidates))

    def test_sole_ordinary_video_child_stays_whole_root(self) -> None:
        """One ordinary titled child is a single work, bundle split never fires.

        ``Some Movie Pack/Some Movie (2020)/`` has exactly one video-bearing
        child, but the conservative splitter finds no multi-film proof, so
        the whole-root single-work boundary must survive unchanged.
        """
        root = "/quark/影视/待刮削/Some Movie Pack"
        fixture = {
            "root": root,
            "children": [
                {
                    "name": "Some Movie (2020)",
                    "is_dir": True,
                    "children": [
                        {"name": "Some Movie (2020).mkv", "is_dir": False, "size": 20_000_000_000},
                    ],
                },
            ],
        }
        candidates = analyze_boundaries(self._node(fixture), root_task_id="t")
        self.assertEqual(len(candidates), 1, msg=[c.display_label for c in candidates])
        self.assertEqual(candidates[0].display_label, root.split("/")[-1])
        self.assertEqual(candidates[0].boundary_evidence.role, DirectoryRole.SINGLE_WORK)

    def test_opaque_container_parks_only_its_own_branch(self) -> None:
        """A light-novel ``.exe`` must not park a seven-work container.

        ``刀剑神域/系列小说/刀剑神域 小说.exe`` is a publication bundle: it will
        never be an episode, yet the whole-root opaque park blocked all 214
        objects of the container.  Contract rule 3 — one uncertain child never
        blocks its siblings.
        """
        root = "/quark/影视/待刮削/刀剑神域"
        fixture = {
            "root": root,
            "children": [
                {"name": "1.刀剑神域 第一季", "is_dir": True, "children": [
                    {"name": "SAO S01E01.mkv", "is_dir": False, "size": 2_000_000_000},
                    {"name": "SAO S01E02.mkv", "is_dir": False, "size": 2_000_000_000},
                ]},
                {"name": "3.刀剑神域：序列之争", "is_dir": True, "children": [
                    {"name": "Ordinal Scale (2017).mkv", "is_dir": False, "size": 9_000_000_000},
                ]},
                {"name": "系列小说", "is_dir": True, "children": [
                    {"name": "刀剑神域 小说.exe", "is_dir": False, "size": 484_655_989},
                ]},
            ],
        }
        candidates = analyze_boundaries(self._node(fixture), root_task_id="t")
        by_label = {c.display_label: c for c in candidates}
        self.assertEqual(len(candidates), 3, msg=list(by_label))
        novels = by_label["系列小说"]
        self.assertEqual(novels.boundary_evidence.role, DirectoryRole.UNCERTAIN)
        self.assertTrue(novels.requires_content_expansion)
        self.assertEqual(novels.source_paths, (f"{root}/系列小说",))
        # 兄弟作品照常拆分，且绝不带上内容展开标记
        for label in ("1.刀剑神域 第一季", "3.刀剑神域：序列之争"):
            self.assertFalse(by_label[label].requires_content_expansion)
            self.assertNotEqual(
                by_label[label].boundary_evidence.role, DirectoryRole.UNCERTAIN,
            )

    def test_opaque_container_beside_the_root_files_still_parks_everything(self) -> None:
        """An opaque container directly under the root keeps the whole-root park."""
        root = "/quark/影视/待刮削/Mixed"
        fixture = {
            "root": root,
            "children": [
                {"name": "Season 01", "is_dir": True, "children": [
                    {"name": "S01E01.mkv", "is_dir": False, "size": 2_000_000_000},
                ]},
            ],
            "files": [],
        }
        fixture["children"].append(
            {"name": "disc.iso", "is_dir": False, "size": 45_000_000_000}
        )
        candidates = analyze_boundaries(self._node(fixture), root_task_id="t")
        self.assertEqual(len(candidates), 1)
        self.assertEqual(candidates[0].boundary_evidence.role, DirectoryRole.UNCERTAIN)
        self.assertTrue(candidates[0].requires_content_expansion)
        self.assertEqual(candidates[0].source_paths, (root,))

    def test_episode_body_splits_from_bare_theatrical_group(self) -> None:
        """A TV body plus a generic ``剧场版`` group is two works, not one.

        ``钢之炼金术师 FA …/`` keeps the whole episode run as direct files and
        hangs the feature off a bare ``剧场版`` folder.  The folder label names
        no film, so only the file's own substantial standalone title proves the
        second work — the episode body must not swallow a 22 GB feature.
        """
        root = "/quark/影视/待刮削/钢之炼金术师"
        episodes = [
            {
                "name": f"[MAI] Fullmetal Alchemist: Brotherhood [{index:02d}][Ma10p_2160p][x265_flac_ass].mkv",
                "is_dir": False,
                "size": 1_400_000_000,
            }
            for index in range(1, 65)
        ]
        fixture = {
            "root": root,
            "children": [
                {
                    "name": "钢之炼金术师 FA（2009）4K超清2160P收藏版 全64集 110.2G",
                    "is_dir": True,
                    "children": episodes + [
                        {
                            "name": "[MAI&POPGO] Fullmetal Alchemist: Brotherhood [Fonts].exe",
                            "is_dir": False,
                            "size": 50_797_113,
                        },
                        {
                            "name": "剧场版",
                            "is_dir": True,
                            "children": [
                                {
                                    "name": "[AI-Raws] Fullmetal Alchemist the Movie The Sacred Star of Milos [Ma10p_2160p][x265_DTS-HD5.1_ass].mkv",
                                    "is_dir": False,
                                    "size": 22_087_828_467,
                                },
                                {
                                    "name": "[Kamigami] Fullmetal Alchemist the Movie The Sacred Star of Milos [Fonts].exe",
                                    "is_dir": False,
                                    "size": 7_393_862,
                                },
                            ],
                        },
                    ],
                },
            ],
        }
        candidates = analyze_boundaries(self._node(fixture), root_task_id="t")
        self.assertEqual(len(candidates), 2, msg=[c.display_label for c in candidates])
        contexts = {c.proposed_media_context for c in candidates}
        self.assertEqual(contexts, {"tv", "movie"})
        tv = next(c for c in candidates if c.proposed_media_context == "tv")
        movie = next(c for c in candidates if c.proposed_media_context == "movie")
        self.assertEqual(movie.boundary_evidence.role, DirectoryRole.MOVIE_COLLECTION)
        self.assertTrue(movie.source_paths[0].endswith("Milos [Ma10p_2160p][x265_DTS-HD5.1_ass].mkv"))
        # 剧集单元必须精确拿住自己的直接文件，且绝不覆盖电影所在子树
        self.assertEqual(len(tv.source_paths), 65)
        self.assertTrue(all("/剧场版/" not in path for path in tv.source_paths))
        self.assertNotIn(movie.source_paths[0], tv.source_paths)

    def test_episode_body_keeps_whole_boundary_when_group_holds_a_run(self) -> None:
        """A bracket-numbered run inside ``剧场版`` is not a feature set.

        The group label alone never proves independent films; the members must
        each name their own title.  A same-title ordinal run fails closed so
        the historical whole-directory boundary survives.
        """
        root = "/quark/影视/待刮削/Some Show"
        fixture = {
            "root": root,
            "children": [
                {"name": "Some Show [01].mkv", "is_dir": False, "size": 1_400_000_000},
                {"name": "Some Show [02].mkv", "is_dir": False, "size": 1_400_000_000},
                {
                    "name": "剧场版",
                    "is_dir": True,
                    "children": [
                        {"name": "Some Show Recap [01].mkv", "is_dir": False, "size": 900_000_000},
                        {"name": "Some Show Recap [02].mkv", "is_dir": False, "size": 900_000_000},
                    ],
                },
            ],
        }
        candidates = analyze_boundaries(self._node(fixture), root_task_id="t")
        self.assertEqual(len(candidates), 1, msg=[c.display_label for c in candidates])
        self.assertEqual(candidates[0].source_paths, (root,))

    def test_episode_body_split_fails_closed_on_unclassified_sibling(self) -> None:
        """An unclassified video-bearing sibling keeps the whole boundary.

        The splitter is all-or-nothing: losing ownership of a branch nobody
        classified is worse than one over-wide fail-closed unit.
        """
        root = "/quark/影视/待刮削/Mixed Show"
        fixture = {
            "root": root,
            "children": [
                {"name": "Mixed Show [01].mkv", "is_dir": False, "size": 1_400_000_000},
                {"name": "Mixed Show [02].mkv", "is_dir": False, "size": 1_400_000_000},
                {
                    "name": "剧场版",
                    "is_dir": True,
                    "children": [
                        {
                            "name": "Mixed Show the Movie Distant Shore [2160p].mkv",
                            "is_dir": False,
                            "size": 9_000_000_000,
                        },
                    ],
                },
                {
                    "name": "未分类花絮合辑",
                    "is_dir": True,
                    "children": [
                        {"name": "unknown.mkv", "is_dir": False, "size": 3_000_000_000},
                    ],
                },
            ],
        }
        candidates = analyze_boundaries(self._node(fixture), root_task_id="t")
        self.assertEqual(len(candidates), 1, msg=[c.display_label for c in candidates])
        self.assertEqual(candidates[0].source_paths, (root,))

    def test_flat_marker_ordinal_feature_files_stay_one_special_run(self) -> None:
        """``OVA 01 - Title`` feature files are one special run, not films.

        A physical OVA directory (``OVA 排球少年 陆 VS 空``) ships its
        extras as marker-ordinal feature files.  Each file is large and
        independently titled, but the ``OVA NN -`` release-ordinal prefix is
        episodic evidence of one run owned by the parent show: the flat
        movie splitter must fail closed and leave the whole directory as
        one unit for the physical-special grammar.
        """
        root = "/quark/影视/待刮削/P 4k 排球少年/OVA 排球少年 陆 VS 空"
        fixture = {
            "root": root,
            "children": [
                {
                    "name": "[Ygm] Haikyuu!! OVA 01 - Riku vs. Kuu [Ma10p_2160p][x265_flac_ass].mkv",
                    "is_dir": False,
                    "size": 8_000_000_000,
                },
                {
                    "name": "[Ygm] Haikyuu!! OVA 02 - Bouru no 'Michi' [Ma10p_2160p][x265_flac_ass].mkv",
                    "is_dir": False,
                    "size": 8_000_000_000,
                },
            ],
        }
        candidates = analyze_boundaries(self._node(fixture), root_task_id="t")
        self.assertEqual(len(candidates), 1, msg=[c.display_label for c in candidates])
        self.assertEqual(candidates[0].source_paths, (root,))


if __name__ == "__main__":
    unittest.main()


class CollectionSeriesGroupingTests(unittest.TestCase):
    """Layout + artwork for the operator's 2026-08-30 series-grouping ruling."""

    def test_collection_label_strips_series_suffix_and_keeps_safe_chars(self) -> None:
        from engine.scrapeflow.media_naming import collection_directory_label

        self.assertEqual(
            collection_directory_label("命运之夜——天之杯（系列）"),
            "命运之夜——天之杯",
        )
        self.assertEqual(collection_directory_label("空之境界（系列）"), "空之境界")
        self.assertEqual(
            collection_directory_label("命运／万华描绘者 魔法少女☆伊莉雅剧场版（系列）"),
            "命运-万华描绘者 魔法少女☆伊莉雅剧场版",
        )
        self.assertIsNone(collection_directory_label("（系列）"))
        self.assertIsNone(collection_directory_label(""))

    def test_movie_plan_carries_collection_artwork_when_parent_is_collection_dir(self) -> None:
        """A movie inside its own collection directory paints that directory.

        ``build_movie_plan`` receives the layout-decided parent path; when
        that directory is named after the movie's TMDB collection, the plan
        carries the collection root and official artwork paths so the single
        writer uploads poster/folder/fanart for the sub-series directory —
        idempotently across the collection's members.
        """
        import engine.scraper  # binds the planner runtime
        from engine.scrapeflow.planning.movie import build_movie_plan

        class CollectionAList:
            def try_list(self, _path: str, refresh: bool = True) -> list[dict[str, object]]:
                del refresh
                return []

            def walk(self, _path: str, **_kwargs: object) -> list[dict[str, object]]:
                return []

        class CollectionTMDB:
            def get(self, path: str, **_params: object) -> dict[str, object]:
                if path == "/movie/283984":
                    return {
                        "title": "命运之夜——天之杯Ⅰ-恶兆之花",
                        "release_date": "2017-10-14",
                        "poster_path": "/movie-poster.jpg",
                        "backdrop_path": "/movie-backdrop.jpg",
                        "belongs_to_collection": {
                            "id": 390636,
                            "name": "命运之夜——天之杯（系列）",
                            "poster_path": "/collection-poster.jpg",
                            "backdrop_path": "/collection-backdrop.jpg",
                        },
                    }
                raise AssertionError(path)

        plan = build_movie_plan(
            CollectionAList(),
            CollectionTMDB(),
            src_path="/incoming/天之杯Ⅰ",
            parent_path="/library/Fate/命运之夜——天之杯",
            tmdb_id=283984,
            source_files=[
                {
                    "name": "Sakura no Uta.mkv",
                    "full_path": "/incoming/天之杯Ⅰ/Sakura no Uta.mkv",
                    "size": 20_000_000_000,
                    "is_dir": False,
                }
            ],
            defer_validation=True,
        )
        self.assertEqual(
            plan.metadata.get("collection_root"),
            "/library/Fate/命运之夜——天之杯",
        )
        self.assertEqual(
            plan.metadata.get("collection_poster_path"),
            "/collection-poster.jpg",
        )
        from engine.scraper import planned_artwork

        artwork = planned_artwork(plan)
        targets = {target for target, _image, _role in artwork}
        self.assertIn("/library/Fate/命运之夜——天之杯/poster.jpg", targets)
        self.assertIn("/library/Fate/命运之夜——天之杯/folder.jpg", targets)
        self.assertIn("/library/Fate/命运之夜——天之杯/fanart.jpg", targets)

    def test_movie_plan_outside_collection_dir_has_no_collection_metadata(self) -> None:
        """A flat movie keeps its ordinary layout: no phantom parent artwork."""
        import engine.scraper  # binds the planner runtime
        from engine.scrapeflow.planning.movie import build_movie_plan

        class FlatAList:
            def try_list(self, _path: str, refresh: bool = True) -> list[dict[str, object]]:
                del refresh
                return []

            def walk(self, _path: str, **_kwargs: object) -> list[dict[str, object]]:
                return []

        class FlatTMDB:
            def get(self, path: str, **_params: object) -> dict[str, object]:
                if path == "/movie/900497":
                    return {
                        "title": "命运-冠位指定 -月光-失落之室-",
                        "release_date": "2017-12-31",
                        "poster_path": "/movie-poster.jpg",
                        "belongs_to_collection": {
                            "id": 390636,
                            "name": "命运之夜——天之杯（系列）",
                            "poster_path": "/collection-poster.jpg",
                        },
                    }
                raise AssertionError(path)

        plan = build_movie_plan(
            FlatAList(),
            FlatTMDB(),
            src_path="/incoming/月光",
            parent_path="/library/Fate",
            tmdb_id=900497,
            source_files=[
                {
                    "name": "Moonlight.mp4",
                    "full_path": "/incoming/月光/Moonlight.mp4",
                    "size": 20_000_000_000,
                    "is_dir": False,
                }
            ],
            defer_validation=True,
        )
        self.assertIsNone(plan.metadata.get("collection_root"))
