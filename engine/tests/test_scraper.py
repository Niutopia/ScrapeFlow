from __future__ import annotations

import importlib.util
import io
import json
import os
import stat
import urllib.error
import tempfile
import unittest
from unittest import mock
from pathlib import Path
from typing import Any

import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import scraper


class FakeAList:
    def __init__(self, files=None, listings=None):
        self.files = []
        self.listings = listings or {}
        self.operations = []
        self.fail_on_move = False
        self.names_by_dir: dict[str, set[str]] = {}
        self.entries_by_dir: dict[str, dict[str, dict[str, Any]]] = {}
        for raw in files or []:
            item = dict(raw)
            item.setdefault("size", 1)
            item.setdefault("modified", "2026-01-01T00:00:00Z")
            self.files.append(item)
            directory, name = scraper.split_remote(item["full_path"])
            self.names_by_dir.setdefault(directory, set()).add(name)
            entry = {key: value for key, value in item.items() if key != "full_path"}
            entry.setdefault("name", name)
            entry.setdefault("is_dir", False)
            self.entries_by_dir.setdefault(directory, {})[name] = entry

    def walk(self, path, refresh=True, **kwargs):
        return list(self.files)

    def try_list(self, path, refresh=False):
        if path in self.listings:
            return [dict(item) for item in self.listings[path]]
        if path in self.entries_by_dir:
            return [dict(item) for item in self.entries_by_dir[path].values()]
        if path in self.names_by_dir:
            return [{"name": name, "is_dir": False, "size": 1, "modified": "2026-01-01T00:00:00Z"} for name in self.names_by_dir[path]]
        return None

    def mkdir(self, path):
        self.operations.append(("mkdir", path))
        self.names_by_dir.setdefault(path, set())
        self.entries_by_dir.setdefault(path, {})

    def rename(self, full_path, new_name):
        directory, old_name = scraper.split_remote(full_path)
        names = self.names_by_dir.setdefault(directory, set())
        if old_name not in names:
            raise scraper.ApiError(f"missing {full_path}")
        if new_name in names:
            raise scraper.ApiError(f"exists {new_name}")
        names.remove(old_name)
        names.add(new_name)
        entries = self.entries_by_dir.setdefault(directory, {})
        entry = entries.pop(old_name, {"name": old_name, "is_dir": False, "size": 1, "modified": "2026-01-01T00:00:00Z"})
        entry["name"] = new_name
        entries[new_name] = entry
        self.operations.append(("rename", full_path, new_name))

    def move(self, src_dir, dst_dir, names):
        if self.fail_on_move:
            self.fail_on_move = False
            raise scraper.ApiError("injected move failure")
        src = self.names_by_dir.setdefault(src_dir, set())
        dst = self.names_by_dir.setdefault(dst_dir, set())
        src_entries = self.entries_by_dir.setdefault(src_dir, {})
        dst_entries = self.entries_by_dir.setdefault(dst_dir, {})
        for name in names:
            if name not in src:
                raise scraper.ApiError(f"missing move source {name}")
            if name in dst:
                raise scraper.ApiError(f"move collision {name}")
        for name in names:
            src.remove(name)
            dst.add(name)
            dst_entries[name] = src_entries.pop(name)
        self.operations.append(("move", src_dir, dst_dir, tuple(names)))

    def upload_bytes(self, target_path, data, content_type):
        directory, name = scraper.split_remote(target_path)
        self.names_by_dir.setdefault(directory, set()).add(name)
        self.entries_by_dir.setdefault(directory, {})[name] = {
            "name": name,
            "is_dir": False,
            "size": len(data),
            "modified": "2026-01-01T00:00:01Z",
        }
        self.operations.append(("upload", target_path, content_type, len(data)))

    def remove_empty_dir(self, path):
        names = self.names_by_dir.get(path)
        if names is None or names:
            return False
        del self.names_by_dir[path]
        self.entries_by_dir.pop(path, None)
        self.operations.append(("rmdir", path))
        return True

    def remove(self, parent, names):
        directory = self.names_by_dir.setdefault(parent, set())
        entries = self.entries_by_dir.setdefault(parent, {})
        for name in names:
            directory.discard(name)
            entries.pop(name, None)
        self.operations.append(("remove", parent, tuple(names)))


class FakeTMDB:
    def __init__(self, responses: dict[str, dict[str, Any]]):
        self.responses = responses

    def get(self, path, **params):
        if path not in self.responses:
            raise scraper.ApiError(f"missing fake response {path}")
        return self.responses[path]

    def download_poster(self, poster_path):
        return b"poster"


class ParsingTests(unittest.TestCase):
    def test_auto_match_selects_exact_title_and_rejects_ambiguous_results(self):
        client = FakeTMDB(
            {
                "/search/tv": {
                    "results": [
                        {
                            "id": 10,
                            "name": "Frieren",
                            "first_air_date": "2023-09-29",
                        },
                        {
                            "id": 11,
                            "name": "Frieren: Beyond Journey's End",
                            "first_air_date": "2023-09-29",
                        },
                    ]
                }
            }
        )
        best, candidates = scraper.auto_match_tmdb(
            client, "Frieren", media_type="tv", min_confidence=0.88
        )
        self.assertEqual(best.tmdb_id, 10)
        self.assertGreaterEqual(best.confidence, 0.99)
        self.assertEqual(len(candidates), 2)

    def test_sp_filter_does_not_match_normal_words(self):
        self.assertFalse(scraper.should_ignore_extra("Whisper.E01.mkv"))
        self.assertFalse(scraper.should_ignore_extra("Display.E01.mkv"))
        self.assertFalse(scraper.should_ignore_extra("Show.SP01.mkv"))
        self.assertTrue(scraper.should_ignore_extra("Show.NCOP.mkv"))
        self.assertTrue(scraper.should_ignore_extra("[Extras] trailer.mkv"))

    def test_episode_detection(self):
        self.assertEqual(scraper.extract_episode_key("Show.S02E03.1080p.mkv"), scraper.EpisodeKey("regular", 3))
        self.assertEqual(scraper.extract_episode_key("Show EP 12.ass"), scraper.EpisodeKey("regular", 12))
        self.assertEqual(scraper.extract_episode_key("Show.SP02.mkv"), scraper.EpisodeKey("special", 2))
        self.assertEqual(scraper.extract_episode_key("Show OVA 1.mkv"), scraper.EpisodeKey("special", 1))
        self.assertIsNone(scraper.extract_episode_key("Show.1080p.x265.mkv"))

    def test_fractional_recap_is_not_merged_into_regular_episode(self):
        key = scraper.extract_episode_key(
            "[Sakurato] EIGHTY SIX [18.5v2][1080p].mkv"
        )
        self.assertEqual(key, scraper.EpisodeKey("fractional", 18))
        self.assertEqual(key.display, "E18.5")

    def test_subgroup_revision_suffix_preserves_episode_number(self):
        for number in (1, 2, 3, 12, 23):
            with self.subTest(number=number):
                name = (
                    f"[Sakurato] 86—Eitishikkusu— [{number:02d}v2]"
                    "[HEVC-10bit 1080p AAC][CHS&CHT].mkv"
                )
                self.assertEqual(
                    scraper.extract_episode_key(name),
                    scraper.EpisodeKey("regular", number),
                )

    def test_release_range_parent_is_not_used_as_episode_one(self):
        path = (
            "/src/[Sakurato] EIGHTY SIX [01-23 Fin v2]/"
            "unparseable-video-name.mkv"
        )
        self.assertEqual(
            scraper.parse_ep_files([{"name": path.rsplit("/", 1)[-1], "full_path": path}]),
            {},
        )

    def test_subtitle_language_is_explicit(self):
        self.assertEqual(scraper.subtitle_language("Show.E01.CHS.ass"), "zh-CN")
        self.assertEqual(scraper.subtitle_language("Show.E01.CHT.ass"), "zh-TW")
        self.assertEqual(scraper.subtitle_language("Show.E01.English.ass"), "en")
        self.assertIsNone(scraper.subtitle_language("Show.E01.ass"))
        self.assertIsNone(scraper.subtitle_language("Hans.E01.ass"))
        self.assertIsNone(scraper.subtitle_language("Nacht.E01.ass"))

    def test_prefer_simplified_does_not_treat_english_as_simplified(self):
        files = [
            {"name": "Show.E01.English.ass", "full_path": "/src/Show.E01.English.ass"},
            {"name": "Show.E01.CHT.ass", "full_path": "/src/Show.E01.CHT.ass"},
            {"name": "Show.E01.mkv", "full_path": "/src/Show.E01.mkv"},
        ]
        groups = scraper.parse_ep_files(files, prefer_simplified=True)
        names = {item["name"] for item in groups[scraper.EpisodeKey("regular", 1)]}
        self.assertIn("Show.E01.English.ass", names)
        self.assertIn("Show.E01.CHT.ass", names)

        files.append({"name": "Show.E01.CHS.ass", "full_path": "/src/Show.E01.CHS.ass"})
        groups = scraper.parse_ep_files(files, prefer_simplified=True)
        names = {item["name"] for item in groups[scraper.EpisodeKey("regular", 1)]}
        self.assertIn("Show.E01.English.ass", names)
        self.assertIn("Show.E01.CHS.ass", names)
        self.assertNotIn("Show.E01.CHT.ass", names)




    def test_ten_bit_tag_is_not_multi_episode_range(self):
        files = [
            {"name": "Show.01-10bit.mkv", "full_path": "/src/Show.01-10bit.mkv"}
        ]
        groups = scraper.parse_ep_files(files)
        self.assertIn(scraper.EpisodeKey("regular", 1), groups)

    def test_iso_date_is_not_treated_as_multi_episode(self):
        files = [
            {
                "name": "Show.2020-01-02.E03.mkv",
                "full_path": "/src/Show.2020-01-02.E03.mkv",
            }
        ]
        groups = scraper.parse_ep_files(files)
        self.assertIn(scraper.EpisodeKey("regular", 3), groups)

    def test_audio_channel_tag_is_not_episode(self):
        files = [{"name": "Show.5.1.mkv", "full_path": "/src/Show.5.1.mkv"}]
        self.assertEqual(scraper.parse_ep_files(files), {})

    def test_multi_episode_file_is_grouped_as_range(self):
        files = [
            {"name": "Show.S01E01-E02.mkv", "full_path": "/src/Show.S01E01-E02.mkv"}
        ]
        groups = scraper.parse_ep_files(files)
        self.assertIn(scraper.EpisodeKey("regular", 1, 2), groups)

    def test_season_folder_is_not_used_as_episode_number(self):
        files = [
            {"name": "video.mkv", "full_path": "/src/Season 01/video.mkv"},
        ]
        groups = scraper.parse_ep_files(files)
        self.assertEqual(groups, {})


    def test_canonical_multiple_video_names_are_idempotent(self):
        base = "Title (2020) {tmdb-1}"
        files = [
            {"name": f"{base} - v2.mkv", "full_path": f"/src/{base} - v2.mkv"},
            {"name": f"{base}.mkv", "full_path": f"/src/{base}.mkv"},
        ]
        names = scraper.make_unique_media_names(base, files)
        self.assertEqual(names, [f"{base} - v2.mkv", f"{base}.mkv"])

    def test_movie_editions_are_preserved(self):
        base = "Movie (2026) {tmdb-1}"
        files = [
            {
                "name": "Movie.2026.IMAX.mkv",
                "full_path": "/src/Movie.2026.IMAX.mkv",
            },
            {"name": "Movie.2026.mkv", "full_path": "/src/Movie.2026.mkv"},
        ]
        names = scraper.make_unique_media_names(
            base, files, preserve_editions=True
        )
        self.assertIn(f"{base} {{edition-IMAX}}.mkv", names)
        self.assertIn(f"{base}.mkv", names)

    def test_sample_and_bonus_classification(self):
        self.assertTrue(scraper.is_sample("Movie.Sample.1080p.mkv"))
        self.assertEqual(scraper.bonus_type("Movie Official Trailer.mkv"), "trailer")
        self.assertIsNone(scraper.bonus_type("Movie Feature.mkv"))
        self.assertFalse(scraper.is_planned_bonus("The Trailer (2026) {tmdb-1}.mkv"))
        self.assertTrue(
            scraper.is_planned_bonus("The Trailer (2026) {tmdb-1}-trailer2.mkv")
        )

    def test_movie_nfo_contains_tmdb_identity(self):
        plan = scraper.Plan(
            mode="movie",
            source_root="/src",
            target_root="/movies/Movie",
            files=[
                scraper.PlannedFile(
                    source_path="/src/a.mkv",
                    source_dir="/src",
                    original_name="a.mkv",
                    final_name="Movie (2026) {tmdb-123}.mkv",
                    target_dir="/movies/Movie",
                    media_kind="video",
                )
            ],
            warnings=[],
            metadata={},
        )
        nfos = scraper.planned_movie_nfos(plan)
        self.assertEqual(nfos[0][0], "/movies/Movie/Movie (2026) {tmdb-123}.nfo")
        self.assertIn(b'<uniqueid type="tmdb" default="true">123</uniqueid>', nfos[0][1])

    def test_infuse_artwork_targets_include_series_season_and_fanart(self):
        plan = scraper.Plan(
            mode="tv",
            source_root="/src",
            target_root="/tv/Show",
            files=[],
            warnings=[],
            metadata={
                "poster_path": "/poster.jpg",
                "backdrop_path": "/backdrop.jpg",
                "season_posters": {"1": "/season1.jpg"},
            },
        )
        targets = {target for target, _, _ in scraper.planned_artwork(plan)}
        self.assertEqual(
            targets,
            {
                "/tv/Show/folder.jpg",
                "/tv/Show/poster.jpg",
                "/tv/Show/fanart.jpg",
                "/tv/Show/season 1-poster.jpg",
            },
        )

    def test_single_chinese_character_marker_requires_token_boundary(self):
        self.assertIsNone(scraper.subtitle_language("简单任务.E01.ass"))
        self.assertEqual(scraper.subtitle_language("Show.E01.[简].ass"), "zh-CN")

    def test_idx_sub_pair_uses_same_basename(self):
        files = [
            {"name": "Show.E01.mkv", "full_path": "/src/Show.E01.mkv"},
            {"name": "Show.E01.zh-TW.idx", "full_path": "/src/Show.E01.zh-TW.idx"},
            {"name": "Show.E01.zh-TW.sub", "full_path": "/src/Show.E01.zh-TW.sub"},
        ]
        names = scraper.make_unique_media_names("Title - S01E01 - Episode", files)
        stems = {Path(name).stem for name in names if Path(name).suffix in {".idx", ".sub"}}
        self.assertEqual(stems, {"Title - S01E01 - Episode.zh-TW"})

    def test_long_names_preserve_semantic_suffix_and_limit(self):
        base = "长" * 200
        files = [
            {"name": "video.mkv", "full_path": "/src/video.mkv"},
            {"name": "video2.mkv", "full_path": "/src/video2.mkv"},
            {"name": "subtitle.zh-CN.ass", "full_path": "/src/subtitle.zh-CN.ass"},
        ]
        names = scraper.make_unique_media_names(base, files)
        self.assertTrue(names[1].endswith(" - v2.mkv"))
        self.assertTrue(names[2].endswith(".zh-CN.ass"))
        self.assertTrue(all(len(name.encode("utf-8")) <= 240 for name in names))

    def test_remote_path_rejects_dot_segments(self):
        with self.assertRaises(ValueError):
            scraper.normalize_remote_path("/library/../secret")
        with self.assertRaises(ValueError):
            scraper.normalize_remote_path("/library/./show")

    def test_url_redaction_hides_tmdb_key(self):
        redacted = scraper._redact_url(
            "https://api.themoviedb.org/3/movie/1?api_key=super-secret&language=zh-CN"
        )
        self.assertNotIn("super-secret", redacted)
        self.assertIn("api_key=%3Credacted%3E", redacted)


    def test_ascii_language_markers_require_boundaries(self):
        self.assertIsNone(scraper.subtitle_language("Englishman.E01.ass"))
        self.assertIsNone(scraper.subtitle_language("Japanesestyle.E01.ass"))

    def test_url_redaction_hides_userinfo(self):
        redacted = scraper._redact_url("http://admin:secret@example.test/api?token=abc")
        self.assertNotIn("secret", redacted)
        self.assertNotIn("admin", redacted)
        self.assertNotIn("abc", redacted)

    def test_unicode_format_controls_are_removed_or_rejected(self):
        self.assertNotIn("\u202e", scraper.safe_name("Title\u202e.mkv"))
        with self.assertRaises(ValueError):
            scraper.normalize_remote_path("/library/Title\u202e.mkv")

    def test_url_redaction_hides_password_and_fragment(self):
        redacted = scraper._redact_url(
            "https://example.test/api?password=secret#fragment-secret"
        )
        self.assertNotIn("secret", redacted)
        self.assertNotIn("fragment-secret", redacted)

    def test_orphan_temporary_files_are_detected_separately(self):
        self.assertTrue(scraper.is_scraper_temp(".scraper-tmp-deadbeef-file.mkv"))
        self.assertFalse(scraper.should_ignore_extra(".scraper-tmp-deadbeef-file.mkv"))

    def test_concatenated_multi_episode_file_is_grouped_as_range(self):
        files = [
            {"name": "Show.S01E01E02.mkv", "full_path": "/src/Show.S01E01E02.mkv"}
        ]
        groups = scraper.parse_ep_files(files)
        self.assertIn(scraper.EpisodeKey("regular", 1, 2), groups)

    def test_sup_subtitle_is_supported(self):
        files = [
            {"name": "Show.E01.sup", "full_path": "/src/Show.E01.sup"},
            {"name": "Show.E01.mkv", "full_path": "/src/Show.E01.mkv"},
        ]
        groups = scraper.parse_ep_files(files)
        names = {item["name"] for item in groups[scraper.EpisodeKey("regular", 1)]}
        self.assertIn("Show.E01.sup", names)


class PlanTests(unittest.TestCase):
    def test_collection_members_use_individual_movie_directories(self):
        alist = FakeAList(
            [{"name": "01.mkv", "full_path": "/src/01.mkv", "size": 1}]
        )
        tmdb = FakeTMDB(
            {
                "/collection/99": {
                    "name": "合集",
                    "parts": [
                        {
                            "id": 101,
                            "title": "电影一",
                            "release_date": "2020-01-01",
                            "poster_path": "/movie1.jpg",
                        }
                    ],
                }
            }
        )
        with tempfile.TemporaryDirectory() as tmp:
            mapping = Path(tmp) / "map.json"
            mapping.write_text('{"1":101}', encoding="utf-8")
            plan = scraper.build_collection_plan(
                alist,
                tmdb,
                src_path="/src",
                parent_path="/movies",
                tmdb_id=99,
                mapping_path=mapping,
                allow_index_mapping=False,
            )
        self.assertIn("电影一 (2020) {tmdb-101}", plan.files[0].target_dir)
        self.assertEqual(
            plan.metadata["member_posters"][plan.files[0].target_dir], "/movie1.jpg"
        )
    def setUp(self):
        self.show = {
            "name": "测试剧",
            "first_air_date": "2020-01-01",
            "poster_path": "/poster.jpg",
            "seasons": [{"season_number": 1}],
        }
        self.responses = {
            "/tv/10": self.show,
            "/tv/10/season/1": {
                "episodes": [{"episode_number": 1, "name": "第一集"}]
            },
            "/tv/10/season/0": {
                "episodes": [{"episode_number": 1, "name": "特别篇"}]
            },
        }

    def test_special_goes_to_season_zero(self):
        files = [
            {"name": "Show.SP01.mkv", "full_path": "/src/Show.SP01.mkv"},
            {"name": "Show.SP01.CHT.ass", "full_path": "/src/Show.SP01.CHT.ass"},
        ]
        plan = scraper.build_tv_plan(
            FakeAList(files),
            FakeTMDB(self.responses),
            src_path="/src",
            parent_path="/library",
            tmdb_id=10,
            season=1,
            absolute=False,
            prefer_simplified=False,
            allow_unmapped=False,
        )
        self.assertTrue(all(item.target_dir.endswith("Season 00") for item in plan.files))
        self.assertTrue(all("S00E01" in item.final_name for item in plan.files))

    def test_unmapped_absolute_episode_stops(self):
        files = [{"name": "Show.E02.mkv", "full_path": "/src/Show.E02.mkv"}]
        with self.assertRaises(scraper.PlanError):
            scraper.build_tv_plan(
                FakeAList(files),
                FakeTMDB(self.responses),
                src_path="/src",
                parent_path="/library",
                tmdb_id=10,
                season=1,
                absolute=True,
                prefer_simplified=False,
                allow_unmapped=True,
            )


    def test_equivalent_directory_spellings_are_rejected(self):
        files = [
            {"name": "a.mkv", "full_path": "/src/a.mkv"},
            {"name": "b.mkv", "full_path": "/src/b.mkv"},
        ]
        plan = scraper.Plan(
            mode="movie",
            source_root="/src",
            target_root="/library",
            files=[
                scraper.PlannedFile(
                    "/src/a.mkv", "/src", "a.mkv", "a.mkv",
                    "/library/Café", "video", source_size=1,
                ),
                scraper.PlannedFile(
                    "/src/b.mkv", "/src", "b.mkv", "b.mkv",
                    "/library/Café", "video", source_size=1,
                ),
            ],
            warnings=[],
            metadata={},
        )
        with self.assertRaisesRegex(scraper.PlanError, "拼写不同"):
            scraper.validate_plan(FakeAList(files), plan)

    def test_target_nested_inside_source_is_rejected(self):
        files = [{"name": "old.mkv", "full_path": "/src/old.mkv"}]
        plan = scraper.Plan(
            mode="movie",
            source_root="/src",
            target_root="/src/new-target",
            files=[
                scraper.PlannedFile(
                    source_path="/src/old.mkv",
                    source_dir="/src",
                    original_name="old.mkv",
                    final_name="new.mkv",
                    target_dir="/src/new-target",
                    media_kind="video",
                    source_size=1,
                    source_modified="2026-01-01T00:00:00Z",
                )
            ],
            warnings=[],
            metadata={},
        )
        with self.assertRaises(scraper.PlanError):
            scraper.validate_plan(FakeAList(files), plan)

    def test_existing_destination_collision_stops(self):
        files = [{"name": "old.mkv", "full_path": "/src/old.mkv"}]
        target = "/library/电影 (2020) {tmdb-20}"
        alist = FakeAList(files, listings={target: [{"name": "电影 (2020) {tmdb-20}.mkv", "is_dir": False}]})
        tmdb = FakeTMDB(
            {
                "/movie/20": {
                    "title": "电影",
                    "release_date": "2020-01-01",
                    "poster_path": None,
                }
            }
        )
        with self.assertRaises(scraper.PlanError):
            scraper.build_movie_plan(
                alist,
                tmdb,
                src_path="/src",
                parent_path="/library",
                tmdb_id=20,
            )

    def test_unsorted_files_keep_video_and_subtitle_extensions(self):
        files = [
            {
                "name": "Show.E01.en.srt",
                "full_path": "/src/z/Show.E01.en.srt",
            },
            {
                "name": "Show.E01.mkv",
                "full_path": "/src/a/Show.E01.mkv",
            },
        ]
        plan = scraper.build_tv_plan(
            FakeAList(files),
            FakeTMDB(self.responses),
            src_path="/src",
            parent_path="/library",
            tmdb_id=10,
            season=1,
            absolute=False,
            prefer_simplified=False,
            allow_unmapped=False,
        )
        by_source = {item.source_path: item.final_name for item in plan.files}
        self.assertTrue(by_source["/src/a/Show.E01.mkv"].endswith(".mkv"))
        self.assertTrue(by_source["/src/z/Show.E01.en.srt"].endswith(".en.srt"))


    def test_unplanned_source_name_collision_stops_before_execution(self):
        files = [
            {"name": "old.mkv", "full_path": "/src/old.mkv"},
            {"name": "new.mkv", "full_path": "/src/new.mkv"},
        ]
        plan = scraper.Plan(
            mode="movie",
            source_root="/src",
            target_root="/dst",
            files=[
                scraper.PlannedFile(
                    source_path="/src/old.mkv",
                    source_dir="/src",
                    original_name="old.mkv",
                    final_name="new.mkv",
                    target_dir="/dst",
                    media_kind="video",
                    source_size=1,
                    source_modified="2026-01-01T00:00:00Z",
                )
            ],
            warnings=[],
            metadata={},
        )
        with self.assertRaises(scraper.PlanError):
            scraper.validate_plan(FakeAList(files), plan)

    def test_destination_directory_name_collision_stops(self):
        files = [{"name": "old.mkv", "full_path": "/src/old.mkv"}]
        plan = scraper.Plan(
            mode="movie",
            source_root="/src",
            target_root="/dst",
            files=[
                scraper.PlannedFile(
                    source_path="/src/old.mkv",
                    source_dir="/src",
                    original_name="old.mkv",
                    final_name="new.mkv",
                    target_dir="/dst",
                    media_kind="video",
                    source_size=1,
                    source_modified="2026-01-01T00:00:00Z",
                )
            ],
            warnings=[],
            metadata={},
        )
        alist = FakeAList(files, listings={"/dst": [{"name": "new.mkv", "is_dir": True}]})
        with self.assertRaises(scraper.PlanError):
            scraper.validate_plan(alist, plan)

    def test_tv_plan_rejects_unparsed_media(self):
        files = [
            {"name": "Show.E01.mkv", "full_path": "/src/Show.E01.mkv"},
            {"name": "mystery.mkv", "full_path": "/src/mystery.mkv"},
        ]
        with self.assertRaisesRegex(scraper.PlanError, "拒绝静默遗漏"):
            scraper.build_tv_plan(
                FakeAList(files),
                FakeTMDB(self.responses),
                src_path="/src",
                parent_path="/library",
                tmdb_id=10,
                season=1,
                absolute=False,
                prefer_simplified=False,
                allow_unmapped=False,
            )

    def test_tv_plan_rejects_subtitle_only_archive_source(self):
        files = [
            {"name": "Show.E01.sc.ass", "full_path": "/src/Show.E01.sc.ass"},
            {
                "name": "Show.7z.001",
                "full_path": "/src/Show.7z.001",
            },
        ]
        with self.assertRaisesRegex(scraper.PlanError, "未找到剧集视频文件"):
            scraper.build_tv_plan(
                FakeAList(files),
                FakeTMDB(self.responses),
                src_path="/src",
                parent_path="/library",
                tmdb_id=10,
                season=1,
                absolute=False,
                prefer_simplified=False,
                allow_unmapped=False,
            )

    def test_collection_rejects_special_entries(self):
        files = [{"name": "SP01.mkv", "full_path": "/src/SP01.mkv"}]
        tmdb = FakeTMDB(
            {
                "/collection/99": {
                    "name": "合集",
                    "poster_path": None,
                    "parts": [
                        {"id": 101, "title": "第一部", "release_date": "2020-01-01"}
                    ],
                }
            }
        )
        with tempfile.TemporaryDirectory() as tmp:
            mapping = Path(tmp) / "map.json"
            mapping.write_text(json.dumps({"1": 101}), encoding="utf-8")
            with self.assertRaisesRegex(scraper.PlanError, "不支持特别篇"):
                scraper.build_collection_plan(
                    FakeAList(files), tmdb, src_path="/src", parent_path="/library",
                    tmdb_id=99, mapping_path=mapping, allow_index_mapping=False,
                )

    def test_collection_map_rejects_unused_source_number(self):
        files = [{"name": "01.mkv", "full_path": "/src/01.mkv"}]
        tmdb = FakeTMDB(
            {
                "/collection/99": {
                    "name": "合集",
                    "poster_path": None,
                    "parts": [
                        {"id": 101, "title": "第一部", "release_date": "2020-01-01"},
                        {"id": 102, "title": "第二部", "release_date": "2021-01-01"},
                    ],
                }
            }
        )
        with tempfile.TemporaryDirectory() as tmp:
            mapping = Path(tmp) / "map.json"
            mapping.write_text(json.dumps({"1": 101, "2": 102}), encoding="utf-8")
            with self.assertRaisesRegex(scraper.PlanError, "不存在的编号"):
                scraper.build_collection_plan(
                    FakeAList(files), tmdb, src_path="/src", parent_path="/library",
                    tmdb_id=99, mapping_path=mapping, allow_index_mapping=False,
                )

    def test_collection_map_rejects_movie_outside_collection(self):
        files = [{"name": "01.mkv", "full_path": "/src/01.mkv"}]
        tmdb = FakeTMDB(
            {
                "/collection/99": {
                    "name": "合集",
                    "poster_path": None,
                    "parts": [
                        {"id": 101, "title": "第一部", "release_date": "2020-01-01"}
                    ],
                }
            }
        )
        with tempfile.TemporaryDirectory() as tmp:
            mapping = Path(tmp) / "map.json"
            mapping.write_text(json.dumps({"1": 202}), encoding="utf-8")
            with self.assertRaises(scraper.PlanError):
                scraper.build_collection_plan(
                    FakeAList(files),
                    tmdb,
                    src_path="/src",
                    parent_path="/library",
                    tmdb_id=99,
                    mapping_path=mapping,
                    allow_index_mapping=False,
                )

    def test_collection_map_rejects_coercible_non_integer_ids(self):
        for value in (True, 12.9, "123"):
            with self.subTest(value=value), tempfile.TemporaryDirectory() as tmp:
                mapping = Path(tmp) / "map.json"
                mapping.write_text(json.dumps({"1": value}), encoding="utf-8")
                with self.assertRaisesRegex(scraper.PlanError, "JSON 正整数"):
                    scraper._load_collection_map(mapping)

    def test_episode_override_map_supports_single_and_range(self):
        with tempfile.TemporaryDirectory() as tmp:
            mapping = Path(tmp) / "episodes.json"
            mapping.write_text(
                json.dumps({"13": "S02E01", "E14-E15": "S02E02-E03"}),
                encoding="utf-8",
            )
            loaded = scraper._load_episode_map(mapping)
        self.assertEqual(loaded[scraper.EpisodeKey("regular", 13)], (2, 1, 0))
        self.assertEqual(
            loaded[scraper.EpisodeKey("regular", 14, 15)], (2, 2, 3)
        )

    def test_episode_override_map_supports_fractional_recap(self):
        with tempfile.TemporaryDirectory() as tmp:
            mapping = Path(tmp) / "episodes.json"
            mapping.write_text(
                json.dumps({"11.5": "S00E02", "E18.5": "S00E04"}),
                encoding="utf-8",
            )
            loaded = scraper._load_episode_map(mapping)
        self.assertEqual(loaded[scraper.EpisodeKey("fractional", 11)], (0, 2, 0))
        self.assertEqual(loaded[scraper.EpisodeKey("fractional", 18)], (0, 4, 0))

    def test_absolute_episode_group_controls_tmdb_order(self):
        tmdb = FakeTMDB(
            {
                "/tv/episode_group/group1": {
                    "groups": [
                        {
                            "order": 1,
                            "episodes": [
                                {
                                    "order": 1,
                                    "season_number": 2,
                                    "episode_number": 3,
                                    "name": "目标集",
                                }
                            ],
                        }
                    ]
                },
                "/tv/1/season/0": {"episodes": []},
            }
        )
        mapping = scraper._build_tv_episode_map(
            tmdb, {}, 1, 1, True, episode_group_id="group1"
        )
        self.assertEqual(mapping[scraper.EpisodeKey("regular", 1)], (2, 3, "目标集"))


class ClientTests(unittest.TestCase):
    def test_list_fetches_all_pages(self):
        class PagedClient(scraper.AListClient):
            def __init__(self):
                super().__init__("https://example.invalid", "admin", "")
                self.bodies = []

            def call(self, endpoint, body, retryable=False):
                self.bodies.append(dict(body))
                page = body["page"]
                if page == 1:
                    content = [{"name": f"f{i}", "is_dir": False} for i in range(500)]
                else:
                    content = [{"name": f"f{i}", "is_dir": False} for i in range(500, 620)]
                return {
                    "code": 200,
                    "data": {
                        "content": content,
                        "page": page,
                        "per_page": 500,
                        "filtered_total": 620,
                        "has_more": page == 1,
                    },
                }

        client = PagedClient()
        result = client.list("/many", refresh=True)
        self.assertEqual(len(result), 620)
        self.assertEqual([body["page"] for body in client.bodies], [1, 2])
        self.assertTrue(client.bodies[0]["refresh"])
        self.assertFalse(client.bodies[1]["refresh"])

    def test_remove_empty_dir_rejects_root(self):
        client = scraper.AListClient("https://example.invalid", "admin", "")
        with self.assertRaises(scraper.PlanError):
            client.remove_empty_dir("/")


    def test_list_old_server_exact_full_page_ends_on_empty_probe(self):
        class OldPagedClient(scraper.AListClient):
            def __init__(self):
                super().__init__("https://example.invalid", "admin", "")

            def call(self, endpoint, body, retryable=False):
                page = body["page"]
                content = (
                    [{"name": f"f{i}", "is_dir": False} for i in range(500)]
                    if page == 1
                    else []
                )
                return {"code": 200, "data": {"content": content, "page": page}}

        self.assertEqual(len(OldPagedClient().list("/src")), 500)

    def test_list_rejects_empty_page_that_claims_more(self):
        class BrokenClient(scraper.AListClient):
            def __init__(self):
                super().__init__("https://example.invalid", "admin", "")

            def call(self, endpoint, body, retryable=False):
                return {
                    "code": 200,
                    "data": {"content": [], "page": 1, "has_more": True},
                }

        with self.assertRaises(scraper.ApiError):
            BrokenClient().list("/broken")

    def test_list_rejects_overlapping_pages(self):
        class OverlapClient(scraper.AListClient):
            def __init__(self):
                super().__init__("https://example.invalid", "admin", "")

            def call(self, endpoint, body, retryable=False):
                page = body["page"]
                names = ["a", "b"] if page == 1 else ["b", "c"]
                return {
                    "code": 200,
                    "data": {
                        "content": [{"name": name, "is_dir": False} for name in names],
                        "page": page,
                        "has_more": page == 1,
                    },
                }

        with self.assertRaises(scraper.ApiError):
            OverlapClient().list("/overlap")

    def test_walk_has_directory_safety_limit(self):
        class TreeClient(scraper.AListClient):
            def __init__(self):
                super().__init__("https://example.invalid", "admin", "")

            def list(self, path, refresh=False):
                depth = path.count("/")
                return [{"name": f"d{depth}", "is_dir": True}]

        with self.assertRaises(scraper.PlanError):
            TreeClient().walk("/root", max_directories=2)


class ExecutionTests(unittest.TestCase):
    def test_explicit_cleanup_removes_confirmed_empty_source(self):
        source = "/src/old.mkv"
        alist = FakeAList([{"name": "old.mkv", "full_path": source}])
        plan = scraper.Plan(
            mode="movie",
            source_root="/src",
            target_root="/dst",
            files=[
                scraper.PlannedFile(
                    source_path=source,
                    source_dir="/src",
                    original_name="old.mkv",
                    final_name="new.mkv",
                    target_dir="/dst",
                    media_kind="video",
                    source_size=1,
                    source_modified="2026-01-01T00:00:00Z",
                )
            ],
            warnings=[],
            metadata={"poster_path": None},
        )
        with tempfile.TemporaryDirectory() as tmp:
            scraper.execute_plan(
                alist,
                None,
                plan,
                journal_path=Path(tmp) / "journal.json",
                skip_poster=True,
                cleanup_empty_source=True,
            )
        self.assertNotIn("/src", alist.names_by_dir)
        self.assertTrue(any(operation[0] == "rmdir" for operation in alist.operations))

    def test_failed_journal_can_restore_moved_file_and_release_lock(self):
        lock_path = "/src/.scraper-lock-deadbeef-run.json"
        alist = FakeAList(
            [
                {
                    "name": "new.mkv",
                    "full_path": "/dst/new.mkv",
                    "size": 1,
                    "modified": "2026-01-01T00:00:00Z",
                },
                {
                    "name": scraper.split_remote(lock_path)[1],
                    "full_path": lock_path,
                    "size": 10,
                },
            ]
        )
        plan = scraper.Plan(
            mode="movie",
            source_root="/src",
            target_root="/dst",
            files=[
                scraper.PlannedFile(
                    source_path="/src/old.mkv",
                    source_dir="/src",
                    original_name="old.mkv",
                    final_name="new.mkv",
                    target_dir="/dst",
                    media_kind="video",
                    source_size=1,
                    source_modified="2026-01-01T00:00:00Z",
                )
            ],
            warnings=[],
            metadata={
                "tmdb_id": 1,
                "title": "Movie",
                "year": "2026",
                "poster_path": None,
            },
        )
        records = [
            {
                "action": "acquire-lock",
                "source": "",
                "target": lock_path,
                "status": "ok",
                "message": "",
            },
            {
                "action": "rename-final",
                "source": "/src/.scraper-tmp-x.mkv",
                "target": "/src/new.mkv",
                "status": "ok",
                "message": "",
            },
        ]
        states = scraper.inspect_recovery_state(alist, plan, records)
        self.assertEqual(states[0].current_dir, "/dst")
        with tempfile.TemporaryDirectory() as tmp:
            recovery_path = Path(tmp) / "recovery.json"
            scraper.recover_execution(
                alist, plan, records, recovery_journal_path=recovery_path
            )
            recovered, _, _ = scraper.load_execution_journal(recovery_path)
            self.assertTrue(recovered.success)
        self.assertIn("old.mkv", alist.names_by_dir["/src"])
        self.assertNotIn(scraper.split_remote(lock_path)[1], alist.names_by_dir["/src"])

    def test_final_verification_rejects_changed_target_identity(self):
        class CorruptingAList(FakeAList):
            def move(self, src_dir, dst_dir, names):
                super().move(src_dir, dst_dir, names)
                for name in names:
                    self.entries_by_dir[dst_dir][name]["size"] = 999

        source = "/src/old.mkv"
        alist = CorruptingAList(
            [{"name": "old.mkv", "full_path": source, "size": 1}]
        )
        plan = scraper.Plan(
            mode="movie",
            source_root="/src",
            target_root="/dst",
            files=[
                scraper.PlannedFile(
                    source_path=source,
                    source_dir="/src",
                    original_name="old.mkv",
                    final_name="new.mkv",
                    target_dir="/dst",
                    media_kind="video",
                    source_size=1,
                    source_modified="2026-01-01T00:00:00Z",
                )
            ],
            warnings=[],
            metadata={"poster_path": None},
        )
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaisesRegex(scraper.ScraperError, "身份与计划快照不一致"):
                scraper.execute_plan(
                    alist,
                    FakeTMDB({}),
                    plan,
                    journal_path=Path(tmp) / "journal.json",
                    skip_poster=True,
                )

    def test_move_failure_rolls_back_final_name(self):
        source = "/src/old.mkv"
        alist = FakeAList([{"name": "old.mkv", "full_path": source}])
        alist.fail_on_move = True
        plan = scraper.Plan(
            mode="movie",
            source_root="/src",
            target_root="/dst",
            files=[
                scraper.PlannedFile(
                    source_path=source,
                    source_dir="/src",
                    original_name="old.mkv",
                    final_name="new.mkv",
                    target_dir="/dst",
                    media_kind="video",
                    source_size=1,
                    source_modified="2026-01-01T00:00:00Z",
                )
            ],
            warnings=[],
            metadata={"poster_path": None},
        )
        with tempfile.TemporaryDirectory() as tmp:
            journal = Path(tmp) / "journal.json"
            with self.assertRaises(scraper.ScraperError):
                scraper.execute_plan(alist, FakeTMDB({}), plan, journal_path=journal, skip_poster=True)
            self.assertTrue(journal.exists())
        self.assertIn("old.mkv", alist.names_by_dir["/src"])
        self.assertNotIn("new.mkv", alist.names_by_dir["/src"])
        self.assertIn("/dst", alist.names_by_dir)
        self.assertEqual(alist.names_by_dir["/dst"], set())
        self.assertFalse(any(op[0] == "rmdir" for op in alist.operations))

    def test_swap_names_rolls_back_without_collision(self):
        files = [
            {"name": "a.mkv", "full_path": "/src/a.mkv"},
            {"name": "b.mkv", "full_path": "/src/b.mkv"},
        ]
        alist = FakeAList(files)
        alist.fail_on_move = True
        plan = scraper.Plan(
            mode="movie",
            source_root="/src",
            target_root="/dst",
            files=[
                scraper.PlannedFile(
                    source_path="/src/a.mkv",
                    source_dir="/src",
                    original_name="a.mkv",
                    final_name="b.mkv",
                    target_dir="/dst",
                    media_kind="video",
                    source_size=1,
                    source_modified="2026-01-01T00:00:00Z",
                ),
                scraper.PlannedFile(
                    source_path="/src/b.mkv",
                    source_dir="/src",
                    original_name="b.mkv",
                    final_name="a.mkv",
                    target_dir="/dst",
                    media_kind="video",
                    source_size=1,
                    source_modified="2026-01-01T00:00:00Z",
                ),
            ],
            warnings=[],
            metadata={"poster_path": None},
        )
        with tempfile.TemporaryDirectory() as tmp:
            journal = Path(tmp) / "journal.json"
            with self.assertRaises(scraper.ScraperError):
                scraper.execute_plan(
                    alist, FakeTMDB({}), plan, journal_path=journal, skip_poster=True
                )
        self.assertEqual(alist.names_by_dir["/src"], {"a.mkv", "b.mkv"})

    def test_keyboard_interrupt_runs_rollback(self):
        class InterruptingAList(FakeAList):
            def __init__(self, files):
                super().__init__(files)
                self.interrupted = False

            def rename(self, full_path, new_name):
                super().rename(full_path, new_name)
                if not self.interrupted:
                    self.interrupted = True
                    raise KeyboardInterrupt()

        source = "/src/old.mkv"
        alist = InterruptingAList([{"name": "old.mkv", "full_path": source}])
        plan = scraper.Plan(
            mode="movie",
            source_root="/src",
            target_root="/dst",
            files=[
                scraper.PlannedFile(
                    source_path=source,
                    source_dir="/src",
                    original_name="old.mkv",
                    final_name="new.mkv",
                    target_dir="/dst",
                    media_kind="video",
                    source_size=1,
                    source_modified="2026-01-01T00:00:00Z",
                )
            ],
            warnings=[],
            metadata={"poster_path": None},
        )
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaises(KeyboardInterrupt):
                scraper.execute_plan(
                    alist,
                    FakeTMDB({}),
                    plan,
                    journal_path=Path(tmp) / "journal.json",
                    skip_poster=True,
                )
        self.assertEqual(alist.names_by_dir["/src"], {"old.mkv"})

    def test_existing_poster_is_not_overwritten_by_default(self):
        source = "/dst/old.mkv"
        alist = FakeAList(
            [
                {"name": "old.mkv", "full_path": source},
                {"name": "folder.jpg", "full_path": "/dst/folder.jpg"},
            ]
        )
        plan = scraper.Plan(
            mode="movie",
            source_root="/dst",
            target_root="/dst",
            files=[
                scraper.PlannedFile(
                    source_path=source,
                    source_dir="/dst",
                    original_name="old.mkv",
                    final_name="new.mkv",
                    target_dir="/dst",
                    media_kind="video",
                    source_size=1,
                    source_modified="2026-01-01T00:00:00Z",
                )
            ],
            warnings=[],
            metadata={"poster_path": "/poster.jpg"},
        )
        with tempfile.TemporaryDirectory() as tmp:
            scraper.execute_plan(
                alist,
                FakeTMDB({}),
                plan,
                journal_path=Path(tmp) / "journal.json",
                skip_poster=False,
            )
        self.assertFalse(
            any(
                operation[0] == "upload"
                and operation[1].lower().endswith("/folder.jpg")
                and operation[2] == "image/jpeg"
                for operation in alist.operations
            )
        )


    def test_poster_failure_after_file_commit_does_not_roll_back_files(self):
        class PosterFailureTMDB(FakeTMDB):
            def download_poster(self, poster_path):
                raise scraper.ApiError("poster download failed")

        source = "/src/old.mkv"
        alist = FakeAList([{"name": "old.mkv", "full_path": source}])
        plan = scraper.Plan(
            mode="movie",
            source_root="/src",
            target_root="/dst",
            files=[
                scraper.PlannedFile(
                    source_path=source,
                    source_dir="/src",
                    original_name="old.mkv",
                    final_name="new.mkv",
                    target_dir="/dst",
                    media_kind="video",
                    source_size=1,
                    source_modified="2026-01-01T00:00:00Z",
                )
            ],
            warnings=[],
            metadata={"poster_path": "/poster.jpg"},
        )
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaisesRegex(scraper.ScraperError, "未回滚媒体文件"):
                scraper.execute_plan(
                    alist,
                    PosterFailureTMDB({}),
                    plan,
                    journal_path=Path(tmp) / "journal.json",
                    skip_poster=False,
                )
        self.assertIn("new.mkv", alist.names_by_dir["/dst"])
        self.assertNotIn("old.mkv", alist.names_by_dir["/src"])
        self.assertFalse(any(scraper.is_scraper_lock(name) for name in alist.names_by_dir["/src"]))

    def test_existing_poster_case_variant_is_preserved(self):
        source = "/dst/old.mkv"
        alist = FakeAList(
            [
                {"name": "old.mkv", "full_path": source},
                {"name": "Folder.JPG", "full_path": "/dst/Folder.JPG"},
            ]
        )
        plan = scraper.Plan(
            mode="movie",
            source_root="/dst",
            target_root="/dst",
            files=[
                scraper.PlannedFile(
                    source_path=source,
                    source_dir="/dst",
                    original_name="old.mkv",
                    final_name="new.mkv",
                    target_dir="/dst",
                    media_kind="video",
                    source_size=1,
                    source_modified="2026-01-01T00:00:00Z",
                )
            ],
            warnings=[],
            metadata={"poster_path": "/poster.jpg"},
        )
        with tempfile.TemporaryDirectory() as tmp:
            scraper.execute_plan(
                alist,
                FakeTMDB({}),
                plan,
                journal_path=Path(tmp) / "journal.json",
                skip_poster=False,
            )
        self.assertFalse(
            any(
                operation[0] == "upload"
                and operation[1].lower().endswith("/folder.jpg")
                and operation[2] == "image/jpeg"
                for operation in alist.operations
            )
        )


class SecurityRegressionTests(unittest.TestCase):
    def test_active_remote_lock_blocks_new_walk(self):
        class LockedClient(scraper.AListClient):
            def __init__(self):
                super().__init__("https://example.invalid", "admin", "")

            def list(self, path, refresh=False):
                return [
                    {
                        "name": ".scraper-lock-deadbeef-run.json",
                        "is_dir": False,
                        "size": 10,
                    }
                ]

        with self.assertRaisesRegex(scraper.PlanError, "整理锁"):
            LockedClient().walk("/src")

    def test_http_error_body_redacts_all_request_secrets(self):
        url = "https://example.test/api?api_key=query-secret"
        error = urllib.error.HTTPError(
            url,
            401,
            "Unauthorized",
            {},
            io.BytesIO(
                b'{"api_key":"query-secret","password":"body-secret",'
                b'"token":"auth-secret"}'
            ),
        )
        client = scraper.JsonHttpClient(retries=0)
        with mock.patch("urllib.request.urlopen", side_effect=error):
            with self.assertRaises(scraper.ApiError) as caught:
                client.request_json(
                    url,
                    method="POST",
                    headers={"Authorization": "auth-secret"},
                    json_body={"password": "body-secret"},
                )
        message = str(caught.exception)
        for secret in ("query-secret", "body-secret", "auth-secret"):
            self.assertNotIn(secret, message)
        self.assertEqual(caught.exception.status_code, 401)

    def test_special_separators_preserve_number(self):
        cases = {
            "Show.SP.02.mkv": 2,
            "Show.SP-03.mkv": 3,
            "Show.SP_04.mkv": 4,
            "Show.OVA.05.mkv": 5,
            "Show.Special-06.mkv": 6,
        }
        for name, number in cases.items():
            with self.subTest(name=name):
                self.assertEqual(
                    scraper.extract_episode_key(name),
                    scraper.EpisodeKey("special", number),
                )

    def test_unnumbered_special_is_rejected(self):
        files = [{"name": "Show.OVA.mkv", "full_path": "/src/Show.OVA.mkv"}]
        with self.assertRaisesRegex(scraper.PlanError, "未编号特别篇"):
            scraper.parse_ep_files(files)

    def test_orphan_temp_blocks_walk_unless_explicitly_ignored(self):
        class TempClient(scraper.AListClient):
            def __init__(self):
                super().__init__("https://example.invalid", "admin", "")

            def list(self, path, refresh=False):
                return [{"name": ".scraper-tmp-deadbeef.mkv", "is_dir": False}]

        with self.assertRaisesRegex(scraper.PlanError, "遗留的临时"):
            TempClient().walk("/src")
        self.assertEqual(TempClient().walk("/src", ignore_orphan_temp=True), [])

    def test_unknown_year_never_generates_question_marks(self):
        alist = FakeAList([{"name": "movie.mkv", "full_path": "/src/movie.mkv"}])
        plan = scraper.build_movie_plan(
            alist,
            FakeTMDB(
                {"/movie/1": {"title": "Untitled", "release_date": "", "poster_path": None}}
            ),
            src_path="/src",
            parent_path="/library",
            tmdb_id=1,
        )
        self.assertNotIn("?", plan.target_root)
        self.assertTrue(all("?" not in item.final_name for item in plan.files))
        self.assertIn("未知年份", plan.target_root)

    def test_source_snapshot_change_blocks_execution(self):
        alist = FakeAList(
            [{"name": "old.mkv", "full_path": "/src/old.mkv", "size": 2}]
        )
        plan = scraper.Plan(
            mode="movie",
            source_root="/src",
            target_root="/dst",
            files=[
                scraper.PlannedFile(
                    source_path="/src/old.mkv",
                    source_dir="/src",
                    original_name="old.mkv",
                    final_name="new.mkv",
                    target_dir="/dst",
                    media_kind="video",
                    source_size=1,
                    source_modified="2026-01-01T00:00:00Z",
                )
            ],
            warnings=[],
            metadata={},
        )
        with self.assertRaisesRegex(scraper.PlanError, "发生变化"):
            scraper.validate_source_state(alist, plan)

    def test_season_zero_only_ignores_404(self):
        class ErrorTMDB(FakeTMDB):
            def __init__(self, status):
                self.status = status

            def get(self, path, **params):
                if path == "/tv/10/season/0":
                    raise scraper.ApiError("failed", status_code=self.status)
                if path == "/tv/10/season/1":
                    return {"episodes": [{"episode_number": 1, "name": "One"}]}
                raise AssertionError(path)

        show = {"seasons": [{"season_number": 1}]}
        result = scraper._build_tv_episode_map(ErrorTMDB(404), show, 10, 1, False)
        self.assertIn(scraper.EpisodeKey("regular", 1), result)
        with self.assertRaises(scraper.ApiError):
            scraper._build_tv_episode_map(ErrorTMDB(401), show, 10, 1, False)

    def test_plan_loader_rejects_terminal_control_paths(self):
        raw_plan = {
            "mode": "movie",
            "source_root": "/src",
            "target_root": "/dst",
            "warnings": [],
            "metadata": {},
            "files": [
                {
                    "source_path": "/src/a\u202e.mkv",
                    "source_dir": "/src",
                    "original_name": "a\u202e.mkv",
                    "final_name": "a.mkv",
                    "target_dir": "/dst",
                    "media_kind": "video",
                    "episode_key": None,
                    "source_size": 1,
                    "source_modified": None,
                    "source_hash": None,
                }
            ],
        }
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "plan.json"
            payload = {
                "schema_version": scraper.PLAN_SCHEMA_VERSION,
                "created_at": "2026-01-01T00:00:00+00:00",
                "plan_sha256": scraper.plan_sha256(raw_plan),
                "plan": raw_plan,
            }
            path.write_text(json.dumps(payload), encoding="utf-8")
            with self.assertRaises(scraper.PlanError):
                scraper.load_plan_json(path)

    def test_plan_file_hash_round_trip_and_tamper_detection(self):
        plan = scraper.Plan(
            mode="movie",
            source_root="/src",
            target_root="/dst",
            files=[
                scraper.PlannedFile(
                    source_path="/src/old.mkv",
                    source_dir="/src",
                    original_name="old.mkv",
                    final_name="new.mkv",
                    target_dir="/dst",
                    media_kind="video",
                    source_size=1,
                    source_modified="2026-01-01T00:00:00Z",
                )
            ],
            warnings=[],
            metadata={"tmdb_id": 1, "title": "Test", "year": "2026", "poster_path": None},
        )
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "plan.json"
            digest = scraper.write_plan_json(plan, path)
            loaded, loaded_digest = scraper.load_plan_json(path)
            self.assertEqual(digest, loaded_digest)
            self.assertEqual(scraper.plan_to_dict(plan), scraper.plan_to_dict(loaded))
            raw = json.loads(path.read_text(encoding="utf-8"))
            raw["plan"]["files"][0]["final_name"] = "tampered.mkv"
            path.write_text(json.dumps(raw), encoding="utf-8")
            with self.assertRaisesRegex(scraper.PlanError, "SHA-256"):
                scraper.load_plan_json(path)

    def test_plan_and_journal_outputs_refuse_overwrite(self):
        plan = scraper.Plan(
            "movie",
            "/src",
            "/dst",
            [
                scraper.PlannedFile(
                    "/src/a.mkv", "/src", "a.mkv", "a.mkv", "/dst", "video",
                    source_size=1,
                )
            ],
            [],
            {"tmdb_id": 1, "title": "Test", "year": "2026", "poster_path": None},
        )
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "existing.json"
            path.write_text("keep", encoding="utf-8")
            with self.assertRaisesRegex(scraper.ScraperError, "拒绝覆盖"):
                scraper.write_plan_json(plan, path)

    def test_unicode_equivalent_destination_names_conflict(self):
        nfc = "Caf\u00e9.mkv"
        nfd = "Cafe\u0301.mkv"
        files = [
            {"name": "a.mkv", "full_path": "/src/a.mkv"},
            {"name": "b.mkv", "full_path": "/src/b.mkv"},
        ]
        plan = scraper.Plan(
            mode="movie",
            source_root="/src",
            target_root="/dst",
            files=[
                scraper.PlannedFile(
                    "/src/a.mkv", "/src", "a.mkv", nfc, "/dst", "video", source_size=1
                ),
                scraper.PlannedFile(
                    "/src/b.mkv", "/src", "b.mkv", nfd, "/dst", "video", source_size=1
                ),
            ],
            warnings=[],
            metadata={},
        )
        with self.assertRaisesRegex(scraper.PlanError, "目标文件名冲突"):
            scraper.validate_plan(FakeAList(files), plan)

    def test_compatibility_write_interfaces_are_disabled_by_default(self):
        with mock.patch.dict(os.environ, {}, clear=False):
            os.environ.pop("SCRAPER_ENABLE_UNSAFE_COMPAT_WRITES", None)
            with self.assertRaisesRegex(scraper.ScraperError, "兼容写接口默认禁用"):
                scraper.alist_rename("token", "/src/a.mkv", "b.mkv")

    def test_remote_plain_http_requires_explicit_risk_flag(self):
        with self.assertRaisesRegex(scraper.ScraperError, "明文传输"):
            scraper.AListClient("http://example.invalid", "admin", "secret")
        client = scraper.AListClient(
            "http://example.invalid", "admin", "secret", allow_insecure_http=True
        )
        self.assertEqual(client.base_url, "http://example.invalid")
        loopback = scraper.AListClient("http://127.0.0.1:5244", "admin", "secret")
        self.assertEqual(loopback.base_url, "http://127.0.0.1:5244")

    def test_poster_target_preserves_existing_case(self):
        alist = FakeAList(
            [{"name": "Folder.JPG", "full_path": "/dst/Folder.JPG"}]
        )
        target, existing = scraper.resolve_poster_target(alist, "/dst", overwrite=True)
        self.assertTrue(existing)
        self.assertEqual(target, "/dst/Folder.JPG")

    def test_batch_rejects_numeric_boolean(self):
        tool_path = Path(__file__).resolve().parents[1] / "tools" / "run_batch.py"
        spec = importlib.util.spec_from_file_location("run_batch_bool_module", tool_path)
        module = importlib.util.module_from_spec(spec)
        assert spec.loader is not None
        spec.loader.exec_module(module)
        with tempfile.TemporaryDirectory() as tmp:
            manifest = Path(tmp) / "manifest.json"
            manifest.write_text(
                json.dumps(
                    [
                        {
                            "src": "/src",
                            "parent": "/dst",
                            "type": "movie",
                            "id": 1,
                            "plan_json": "plan.json",
                            "absolute": 1,
                        }
                    ]
                ),
                encoding="utf-8",
            )
            with mock.patch.object(module.sys, "argv", ["run_batch.py", str(manifest)]):
                with self.assertRaisesRegex(SystemExit, "JSON 布尔值"):
                    module.main()


    def test_executable_plan_requires_source_snapshot(self):
        plan = scraper.Plan(
            mode="movie",
            source_root="/src",
            target_root="/dst",
            files=[
                scraper.PlannedFile(
                    source_path="/src/old.mkv",
                    source_dir="/src",
                    original_name="old.mkv",
                    final_name="new.mkv",
                    target_dir="/dst",
                    media_kind="video",
                )
            ],
            warnings=[],
            metadata={"tmdb_id": 1, "title": "Test", "year": "2026", "poster_path": None},
        )
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaisesRegex(scraper.PlanError, "缺少源文件快照"):
                scraper.write_plan_json(plan, Path(tmp) / "plan.json")

    @unittest.skipUnless(os.name == "posix", "POSIX permission check")
    def test_plan_and_journal_files_are_owner_only(self):
        plan = scraper.Plan(
            mode="movie",
            source_root="/src",
            target_root="/dst",
            files=[
                scraper.PlannedFile(
                    source_path="/src/old.mkv",
                    source_dir="/src",
                    original_name="old.mkv",
                    final_name="new.mkv",
                    target_dir="/dst",
                    media_kind="video",
                    source_size=1,
                )
            ],
            warnings=[],
            metadata={"tmdb_id": 1, "title": "Test", "year": "2026", "poster_path": None},
        )
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            plan_path = root / "plan.json"
            journal_path = root / "journal.json"
            scraper.write_plan_json(plan, plan_path)
            journal = scraper.ExecutionJournal(
                created_at="2026-01-01T00:00:00+00:00",
                plan=scraper.plan_to_dict(plan),
                records=[],
            )
            scraper._reserve_output_path(journal_path)
            journal.save(journal_path)
            self.assertEqual(stat.S_IMODE(plan_path.stat().st_mode), 0o600)
            self.assertEqual(stat.S_IMODE(journal_path.stat().st_mode), 0o600)
            payload = json.loads(journal_path.read_text(encoding="utf-8"))
            self.assertEqual(payload["plan_sha256"], scraper.plan_sha256(plan))

    def test_execute_mode_rejects_plan_generation_flags(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "plan.json"
            plan = scraper.Plan(
                "movie",
                "/src",
                "/dst",
                [
                    scraper.PlannedFile(
                        "/src/a.mkv", "/src", "a.mkv", "a.mkv", "/dst", "video",
                        source_size=1,
                    )
                ],
                [],
                {"tmdb_id": 1, "title": "Test", "year": "2026", "poster_path": None},
            )
            digest = scraper.write_plan_json(plan, path)
            stderr = io.StringIO()
            with mock.patch.object(
                scraper,
                "_new_alist_client",
                side_effect=AssertionError("must reject before connecting"),
            ), mock.patch("sys.stderr", stderr):
                result = scraper.main(
                    [
                        "--execute-plan", str(path),
                        "--approve-plan-sha256", digest,
                        "--execute",
                        "--season", "2",
                    ]
                )
            self.assertEqual(result, 2)
            self.assertIn("计划生成参数", stderr.getvalue())

    def test_dry_run_rejects_execution_only_flags(self):
        stderr = io.StringIO()
        with mock.patch("sys.stderr", stderr):
            result = scraper.main(
                [
                    "/src", "--parent", "/dst", "--type", "movie", "--id", "1",
                    "--skip-poster",
                ]
            )
        self.assertEqual(result, 2)
        self.assertIn("只能用于执行已保存计划", stderr.getvalue())


    def test_batch_rejects_fields_from_wrong_mode(self):
        tool_path = Path(__file__).resolve().parents[1] / "tools" / "run_batch.py"
        spec = importlib.util.spec_from_file_location("run_batch_fields_module", tool_path)
        module = importlib.util.module_from_spec(spec)
        assert spec.loader is not None
        spec.loader.exec_module(module)
        with tempfile.TemporaryDirectory() as tmp:
            manifest = Path(tmp) / "manifest.json"
            manifest.write_text(
                json.dumps(
                    [
                        {
                            "execute_plan": "plan.json",
                            "approve_plan_sha256": "a" * 64,
                            "src": "/must-not-be-ignored",
                        }
                    ]
                ),
                encoding="utf-8",
            )
            with mock.patch.object(
                module.sys, "argv", ["run_batch.py", str(manifest), "--execute"]
            ):
                with self.assertRaisesRegex(SystemExit, "不支持的字段"):
                    module.main()


    def test_local_json_rejects_duplicate_fields(self):
        with self.assertRaisesRegex(ValueError, "重复字段"):
            scraper._load_json_text('{"mode":"movie","mode":"tv"}')

    def test_loaded_plan_rejects_unknown_fields(self):
        raw = {
            "mode": "movie",
            "source_root": "/src",
            "target_root": "/dst",
            "warnings": [],
            "metadata": {
                "tmdb_id": 1,
                "title": "Test",
                "year": "2026",
                "poster_path": None,
            },
            "files": [
                {
                    "source_path": "/src/a.mkv",
                    "source_dir": "/src",
                    "original_name": "a.mkv",
                    "final_name": "a.mkv",
                    "target_dir": "/dst",
                    "media_kind": "video",
                    "episode_key": None,
                    "source_size": 1,
                    "source_modified": None,
                    "source_hash": None,
                    "ignored_field": "confusing",
                }
            ],
        }
        with self.assertRaisesRegex(scraper.PlanError, "未知成员"):
            scraper.plan_from_dict(raw)

    def test_http_error_redacts_bearer_credential_without_scheme(self):
        error = urllib.error.HTTPError(
            "https://example.test/api",
            400,
            "bad request",
            hdrs=None,
            fp=io.BytesIO(b'{"message":"bearer-secret"}'),
        )
        with mock.patch("urllib.request.urlopen", side_effect=error):
            with self.assertRaises(scraper.ApiError) as caught:
                scraper.JsonHttpClient(retries=0).request_json(
                    "https://example.test/api",
                    headers={"Authorization": "Bearer bearer-secret"},
                )
        self.assertNotIn("bearer-secret", str(caught.exception))

    def test_failed_run_retains_new_empty_directories(self):
        source = "/src/old.mkv"
        alist = FakeAList([{"name": "old.mkv", "full_path": source}])
        alist.fail_on_move = True
        plan = scraper.Plan(
            "movie",
            "/src",
            "/dst",
            [
                scraper.PlannedFile(
                    source, "/src", "old.mkv", "new.mkv", "/dst", "video",
                    source_size=1, source_modified="2026-01-01T00:00:00Z",
                )
            ],
            [],
            {"poster_path": None},
        )
        with tempfile.TemporaryDirectory() as tmp:
            journal_path = Path(tmp) / "journal.json"
            with self.assertRaises(scraper.ScraperError):
                scraper.execute_plan(
                    alist, FakeTMDB({}), plan, journal_path=journal_path, skip_poster=True
                )
            payload = json.loads(journal_path.read_text(encoding="utf-8"))
        retained = [
            record for record in payload["records"]
            if record["action"] == "rollback-rmdir" and record["status"] == "retained"
        ]
        self.assertTrue(retained)
        self.assertIn("/dst", alist.names_by_dir)


class BatchToolTests(unittest.TestCase):
    def test_batch_manifest_rejects_duplicate_fields(self):
        tool_path = Path(__file__).resolve().parents[1] / "tools" / "run_batch.py"
        spec = importlib.util.spec_from_file_location("run_batch_duplicate_module", tool_path)
        module = importlib.util.module_from_spec(spec)
        assert spec.loader is not None
        spec.loader.exec_module(module)
        with tempfile.TemporaryDirectory() as tmp:
            manifest = Path(tmp) / "manifest.json"
            manifest.write_text(
                '[{"src":"/a","src":"/b","parent":"/dst","type":"movie",'
                '"id":1,"plan_json":"plan.json"}]',
                encoding="utf-8",
            )
            with mock.patch.object(module.sys, "argv", ["run_batch.py", str(manifest)]):
                with self.assertRaisesRegex(SystemExit, "重复字段"):
                    module.main()

    def test_batch_uses_option_terminator_and_resolves_relative_map(self):
        tool_path = Path(__file__).resolve().parents[1] / "tools" / "run_batch.py"
        spec = importlib.util.spec_from_file_location("run_batch_test_module", tool_path)
        module = importlib.util.module_from_spec(spec)
        assert spec.loader is not None
        spec.loader.exec_module(module)

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            manifest = root / "manifest.json"
            manifest.write_text(
                json.dumps(
                    [
                        {
                            "src": "-dangerous-looking-path",
                            "parent": "/library",
                            "type": "collection",
                            "id": 99,
                            "collection_map": "map.json",
                            "plan_json": "plan.json",
                        }
                    ]
                ),
                encoding="utf-8",
            )
            with mock.patch.object(module.sys, "argv", ["run_batch.py", str(manifest)]), mock.patch.object(
                module.subprocess, "run"
            ) as run_mock:
                self.assertEqual(module.main(), 0)
            command = run_mock.call_args.args[0]
            separator = command.index("--")
            self.assertEqual(command[separator + 1], "-dangerous-looking-path")
            map_index = command.index("--collection-map")
            self.assertEqual(
                Path(command[map_index + 1]).resolve(), (root / "map.json").resolve()
            )

    def test_batch_builds_auto_match_command(self):
        tool_path = Path(__file__).resolve().parents[1] / "tools" / "run_batch.py"
        spec = importlib.util.spec_from_file_location("run_batch_auto_module", tool_path)
        module = importlib.util.module_from_spec(spec)
        assert spec.loader is not None
        spec.loader.exec_module(module)

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            manifest = root / "manifest.json"
            manifest.write_text(
                json.dumps(
                    [
                        {
                            "src": "/incoming/The Movie 2026",
                            "parent": "/library",
                            "type": "auto",
                            "query": "The Movie 2026",
                            "min_confidence": 0.93,
                            "plan_json": "auto-plan.json",
                        }
                    ]
                ),
                encoding="utf-8",
            )
            with mock.patch.object(
                module.sys, "argv", ["run_batch.py", str(manifest)]
            ), mock.patch.object(module.subprocess, "run") as run_mock:
                self.assertEqual(module.main(), 0)
            command = run_mock.call_args.args[0]
            self.assertIn("--query", command)
            self.assertEqual(command[command.index("--query") + 1], "The Movie 2026")
            self.assertEqual(command[command.index("--type") + 1], "auto")
            self.assertEqual(command[command.index("--min-confidence") + 1], "0.93")


class ArchiveToolTests(unittest.TestCase):
    @staticmethod
    def load_tool(module_name: str = "extract_archives_test_module"):
        tool_path = Path(__file__).resolve().parents[1] / "tools" / "extract_archives.py"
        spec = importlib.util.spec_from_file_location(module_name, tool_path)
        module = importlib.util.module_from_spec(spec)
        assert spec.loader is not None
        spec.loader.exec_module(module)
        return module

    def test_old_alist_version_is_rejected_for_archive_extraction(self):
        module = self.load_tool("extract_archives_version_module")

        class OldAList:
            def server_version(self):
                return "v3.32.0"

        with self.assertRaisesRegex(scraper.ScraperError, "v3.57.0"):
            module.require_safe_archive_server(OldAList())

    def test_password_marker_is_discovered_without_logging_value(self):
        module = self.load_tool("extract_archives_password_module")

        class MarkerAList:
            def list(self, path, refresh=False):
                return [
                    {"name": "密码： ruach@66", "is_dir": True},
                    {"name": "archive.7z.001", "is_dir": False},
                ]

        value, source = module.discover_archive_password(MarkerAList(), "/archive")
        self.assertEqual(value, "ruach@66")
        self.assertEqual(source, "sibling-marker")

    def test_archive_plan_supports_contiguous_multipart_and_omits_password(self):
        module = self.load_tool("extract_archives_plan_module")
        archive_dir = "/src/release"
        first_path = f"{archive_dir}/Show.7z.001"
        parts = [
            {
                "name": "Show.7z.001",
                "full_path": first_path,
                "is_dir": False,
                "size": 100,
                "modified": "2026-01-01T00:00:00Z",
            },
            {
                "name": "Show.7z.002",
                "full_path": f"{archive_dir}/Show.7z.002",
                "is_dir": False,
                "size": 50,
                "modified": "2026-01-01T00:00:00Z",
            },
        ]

        class ArchiveAList:
            def server_version(self):
                return "v3.62.0"

            def walk(self, path):
                return [dict(item) for item in parts]

            def list(self, path, refresh=False):
                if path == archive_dir:
                    return [
                        *[dict(item) for item in parts],
                        {"name": "解压密码: secret-value", "is_dir": True},
                    ]
                return []

            def archive_meta(self, path, archive_password="", refresh=True):
                self.received_password = archive_password
                return {
                    "content": [
                        {
                            "name": "Show",
                            "is_dir": True,
                            "children": [
                                {
                                    "name": "Show.E01.mkv",
                                    "is_dir": False,
                                    "size": 1234,
                                }
                            ],
                        }
                    ]
                }

        alist = ArchiveAList()
        plan, passwords = module.build_archive_plan(
            alist, "/src", explicit_archive_password=None
        )
        self.assertEqual(plan["archives"][0]["video_count"], 1)
        self.assertEqual(len(plan["archives"][0]["parts"]), 2)
        self.assertEqual(passwords[first_path], "secret-value")
        self.assertEqual(alist.received_password, "secret-value")
        self.assertNotIn("secret-value", json.dumps(plan, ensure_ascii=False))

    def test_archive_member_path_traversal_is_rejected(self):
        module = self.load_tool("extract_archives_traversal_module")
        with self.assertRaisesRegex(scraper.ScraperError, "不安全路径"):
            module._flatten_members(
                [{"name": "..", "is_dir": True, "children": []}]
            )


class MaintenanceToolTests(unittest.TestCase):
    @staticmethod
    def load_tool(filename: str, module_name: str):
        tool_path = Path(__file__).resolve().parents[1] / "tools" / filename
        spec = importlib.util.spec_from_file_location(module_name, tool_path)
        module = importlib.util.module_from_spec(spec)
        assert spec.loader is not None
        spec.loader.exec_module(module)
        return module

    def test_scan_dirs_lists_only_media_and_honors_limit(self):
        module = self.load_tool("scan_dirs.py", "scan_dirs_test_module")

        class FakeScanAList:
            def __init__(self, *args, **kwargs):
                pass

            def login(self):
                return "token"

            def walk(self, path, **kwargs):
                return [
                    {"name": "one.mkv", "full_path": f"{path}/one.mkv"},
                    {"name": "two.mp4", "full_path": f"{path}/two.mp4"},
                    {"name": "note.txt", "full_path": f"{path}/note.txt"},
                ]

        stdout = io.StringIO()
        with mock.patch.object(module, "AListClient", FakeScanAList), mock.patch.dict(
            os.environ, {"ALIST_PASSWORD": "secret"}, clear=False
        ), mock.patch.object(
            module.sys, "argv", ["scan_dirs.py", "/incoming", "--limit", "1"]
        ), mock.patch("sys.stdout", stdout):
            self.assertEqual(module.main(), 0)
        output = stdout.getvalue()
        self.assertIn("媒体文件: 2", output)
        self.assertIn("/incoming/one.mkv", output)
        self.assertNotIn("/incoming/two.mp4", output)
        self.assertIn("其余 1 个", output)

    def test_remove_empty_dirs_deletes_only_refreshed_empty_directory(self):
        module = self.load_tool("remove_empty_dirs.py", "remove_empty_dirs_test_module")
        removed: list[str] = []

        class FakeRemoveAList:
            def __init__(self, *args, **kwargs):
                pass

            def login(self):
                return "token"

            def list(self, path, refresh=False):
                self.last_refresh = refresh
                return [] if path == "/empty" else [{"name": "kept.mkv"}]

            def remove_empty_dir(self, path):
                removed.append(path)
                return True

        stdout = io.StringIO()
        with mock.patch.object(module, "AListClient", FakeRemoveAList), mock.patch.dict(
            os.environ, {"ALIST_PASSWORD": "secret"}, clear=False
        ), mock.patch.object(
            module.sys,
            "argv",
            ["remove_empty_dirs.py", "/empty", "/nonempty", "--execute"],
        ), mock.patch("sys.stdout", stdout):
            self.assertEqual(module.main(), 0)
        self.assertEqual(removed, ["/empty"])
        self.assertIn("跳过非空目录: /nonempty", stdout.getvalue())

    def test_remove_empty_dirs_rejects_root(self):
        module = self.load_tool("remove_empty_dirs.py", "remove_empty_root_module")

        class FakeRemoveAList:
            def __init__(self, *args, **kwargs):
                pass

            def login(self):
                return "token"

        stderr = io.StringIO()
        with mock.patch.object(module, "AListClient", FakeRemoveAList), mock.patch.dict(
            os.environ, {"ALIST_PASSWORD": "secret"}, clear=False
        ), mock.patch.object(
            module.sys, "argv", ["remove_empty_dirs.py", "/", "--execute"]
        ), mock.patch("sys.stderr", stderr):
            self.assertEqual(module.main(), 2)
        self.assertIn("拒绝检查或删除 AList 根目录", stderr.getvalue())


class PosterToolTests(unittest.TestCase):
    def test_poster_execute_requires_matching_dry_run_digest(self):
        tool_path = Path(__file__).resolve().parents[1] / "tools" / "set_poster.py"
        spec = importlib.util.spec_from_file_location("set_poster_test_module", tool_path)
        module = importlib.util.module_from_spec(spec)
        assert spec.loader is not None
        spec.loader.exec_module(module)

        uploads: list[tuple[str, bytes, str]] = []

        class FakePosterAList:
            def __init__(self, *args, **kwargs):
                pass

            def login(self):
                return "token"

            def try_list(self, path, refresh=False):
                return []

            def upload_bytes(self, target, data, content_type):
                uploads.append((target, data, content_type))

        class FakePosterTMDB:
            def __init__(self, *args, **kwargs):
                pass

            def get(self, path):
                return {"poster_path": "/poster.jpg"}

            def download_poster(self, path):
                return b"poster"

        env = {"ALIST_PASSWORD": "secret", "TMDB_API_KEY": "key"}
        with mock.patch.object(module, "AListClient", FakePosterAList), mock.patch.object(
            module, "TMDBClient", FakePosterTMDB
        ), mock.patch.dict(os.environ, env, clear=False):
            stdout = io.StringIO()
            with mock.patch.object(
                module.sys,
                "argv",
                ["set_poster.py", "/dst", "--type", "movie", "--id", "1"],
            ), mock.patch("sys.stdout", stdout):
                self.assertEqual(module.main(), 0)
            match = scraper.re.search(r"计划 SHA-256: ([0-9a-f]{64})", stdout.getvalue())
            self.assertIsNotNone(match)
            digest = match.group(1)
            self.assertEqual(uploads, [])

            with mock.patch.object(
                module.sys,
                "argv",
                [
                    "set_poster.py", "/dst", "--type", "movie", "--id", "1",
                    "--execute",
                ],
            ):
                self.assertEqual(module.main(), 2)
            self.assertEqual(uploads, [])

            with mock.patch.object(
                module.sys,
                "argv",
                [
                    "set_poster.py", "/dst", "--type", "movie", "--id", "1",
                    "--approve-sha256", digest, "--execute",
                ],
            ):
                self.assertEqual(module.main(), 0)
            self.assertEqual(uploads, [("/dst/folder.jpg", b"poster", "image/jpeg")])


if __name__ == "__main__":
    unittest.main()
