from __future__ import annotations

import contextlib
import hashlib
import importlib.util
import http.client
import io
import json
import os
import re
import stat
import urllib.error
import urllib.request
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
            raw_size = entry.get("size")
            content_size = (
                int(raw_size)
                if not isinstance(raw_size, bool) and raw_size is not None
                else 1
            )
            entry.setdefault("_content", b"x" * content_size)
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

    def exact_file_info(self, path):
        directory, name = scraper.split_remote(path)
        entry = self.entries_by_dir.get(directory, {}).get(name)
        if entry is None or entry.get("is_dir"):
            return None
        content = bytes(entry.get("_content", b"x" * int(entry.get("size", 0))))
        return {
            "size": len(content),
            "sha256": None,
            "version": str(entry.get("modified") or "test-version"),
        }

    def open_file_reader(self, path):
        directory, name = scraper.split_remote(path)
        entry = self.entries_by_dir.get(directory, {}).get(name)
        if entry is None or entry.get("is_dir"):
            raise scraper.ApiError(f"missing {path}")
        return io.BytesIO(bytes(entry.get("_content", b"x" * int(entry.get("size", 0)))))

    def upload_file(self, target_path, source, content_type="application/octet-stream"):
        if self.fail_on_move:
            self.fail_on_move = False
            raise scraper.ApiError("injected safe upload failure")
        directory, name = scraper.split_remote(target_path)
        if name in self.names_by_dir.setdefault(directory, set()):
            raise scraper.ApiError(f"upload collision {target_path}")
        content = source.read_bytes()
        self.names_by_dir[directory].add(name)
        self.entries_by_dir.setdefault(directory, {})[name] = {
            "name": name,
            "is_dir": False,
            "size": len(content),
            "modified": "2026-01-01T00:00:01Z",
            "_content": content,
        }
        self.operations.append(("safe-upload", target_path, content_type, len(content)))

    def upload_bytes(self, target_path, data, content_type, *, overwrite=False):
        directory, name = scraper.split_remote(target_path)
        if not overwrite and name in self.names_by_dir.setdefault(directory, set()):
            raise scraper.ApiError(f"upload collision {target_path}")
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
    def test_strong_evidence_warning_classes_finalize_as_automatic(self):
        warnings = {
            "numbered_movies": (
                "编号 01–02 的完整视频序列与 TMDB 2 部电影的官方标题/"
                "别名逐一一致；已仅按官方上映日期顺序建立电影归属"
            ),
            "batch_subtitle_attached": (
                "2 个独立字幕目录文件已通过唯一同发行 basename "
                "跟随已确认视频"
            ),
            "batch_subtitle_isolated": (
                "2 个独立字幕目录文件缺少唯一视频证据，"
                "将保留原位待人工确认"
            ),
            "tv_extras": (
                "2 个明确位于特典目录的幕后/访谈/花絮视频"
                "已按 Infuse Extras 命名保留，不作为正片集号"
            ),
            "ass_title_companion": (
                "E20.5 的 ASS 文本伴侣 title 样式唯一标记为已确认的 "
                "TMDB movie/123；已按同一电影版本参与清晰度去重"
            ),
            "e00_timeline_runtime": (
                "已检索TMDB 官方开播时间与完整时长并唯一确认源文件 E00 对应 "
                "SP01（序章/第 0 话）"
            ),
        }

        for label, warning in warnings.items():
            with self.subTest(label=label):
                plan = scraper.Plan(
                    mode="batch", source_root="/src", target_root="/library/Series",
                    files=[], warnings=[warning], metadata={},
                )
                scraper.finalize_plan_evidence(plan)
                notice = next(item for item in plan.notices if item.message == warning)
                self.assertEqual(
                    notice.requires_review,
                    label == "batch_subtitle_isolated",
                )
                self.assertEqual(
                    notice.severity,
                    "warning" if label == "batch_subtitle_isolated" else "info",
                )

    def test_complete_reset_absolute_blocks_are_proven_safe(self):
        warning = (
            "源发行将长篇剧集分为重置编号的跨季 absolute 块；"
            "已仅在所有视频块完整覆盖 01–N，且与 TMDB 全部季集数"
            "边界唯一分割时自动映射：源第 1 组 01–201 → S01–S08"
        )
        plan = scraper.Plan(
            mode="tv", source_root="/src", target_root="/library/Gintama",
            files=[], warnings=[warning], metadata={},
        )
        scraper.finalize_plan_evidence(plan)
        notice = next(item for item in plan.notices if item.message == warning)
        self.assertFalse(notice.requires_review)
        self.assertEqual(notice.severity, "info")

    def test_independently_confirmed_batch_and_generated_duplicates_are_automatic(self):
        warnings = [
            "已识别 2 个独立电影文件；每个文件均已独立确认不同 TMDB 电影身份，将保留系列父目录并合并审核",
            "子目录《银魂 剧场版 The Final》已通过 TMDB 标题/别名和完整 2 集边界唯一确认是独立剧集《银魂 THE SEMI-FINAL》（2021），未归入母作品 Season 00",
            "其中 2 个未标注 TMDB 编号的电影文件已独立检索确认",
            "2 部系列电影已保留独立 TMDB 身份，并以影片、同名 NFO/海报扁平归入各自系列根目录",
            "已跳过无视频的空占位目录: 小说",
            "E01 存在同清晰度的内封/软字幕与内嵌/硬字幕版本；已保留可切换字幕版本，并计划清理 1 个硬字幕重复视频",
            "识别到 1 部剧场版，已保留独立 TMDB 电影身份并以文件、同名 NFO/海报扁平归入本系列根目录",
            "4 个特典小动画/OVA 已依官方短片时长、发行断档和源季序映射到全局 Season 00 编号",
            "已根据子目录名称与多语言官方特别篇标题的唯一连续匹配、多语言官方标题和完整连续源编号，将 2 集短篇映射为 SP03–SP04",
            "4 个与已确认特别篇视频同名的外挂字幕已跟随视频的官方季集映射",
            "电影原目标目录 /library/Movie (2020) 已扁平化；影片、同名 NFO 与海报将直接旁挂在系列根目录",
            "同一视频的多份外挂字幕仅保留 1 条首选轨道；2 个备选字幕已保留在源目录",
        ]
        plan = scraper.Plan(
            mode="batch", source_root="/src", target_root="/library/Series",
            files=[], warnings=warnings, metadata={},
        )
        scraper.finalize_plan_evidence(plan)
        self.assertEqual(
            {notice.message for notice in plan.notices if notice.requires_review},
            set(),
        )
        self.assertTrue(all(notice.severity == "info" for notice in plan.notices))

    def test_unmapped_video_warning_count_is_rebuilt_from_aggregated_problems(self):
        child_warning = (
            "2 个无法唯一识别的附加视频将保留于"
            "源目录待人工确认；其余可确认媒体仍会正常整理"
        )
        reason = (
            "无法唯一识别的附加视频；保留于"
            "源目录待人工确认"
        )
        plan = scraper.Plan(
            mode="batch",
            source_root="/src",
            target_root="/library/Show",
            files=[],
            warnings=[child_warning, child_warning],
            problem_files=[
                scraper.PlannedProblem(source_path=f"/src/extra-{number}.mkv", reason=reason)
                for number in range(1, 5)
            ],
            metadata={},
        )

        scraper.finalize_plan_evidence(plan)

        summaries = [
            warning
            for warning in plan.warnings
            if scraper.UNMAPPED_VIDEO_SUMMARY_RE.fullmatch(warning)
        ]
        self.assertEqual(
            summaries,
            [
                "4 个无法唯一识别的附加视频将保留原位待人工确认；"
                "其余媒体在问题闭合前不得执行"
            ],
        )
        notice = next(item for item in plan.notices if item.message == summaries[0])
        self.assertTrue(notice.requires_review)
        self.assertEqual(
            notice.evidence["source_paths"],
            [f"/src/extra-{number}.mkv" for number in range(1, 5)],
        )

    def test_destructive_cleanup_notice_requires_review(self):
        plan = scraper.Plan(
            mode="tv",
            source_root="/src",
            target_root="/target",
            files=[],
            warnings=["确认执行后将删除明确无用的系统隐藏/片头片尾/广告文件：Show [NCOP1].mkv"],
            metadata={},
            cleanup_files=[scraper.PlannedCleanup(
                source_path="/src/Show [NCOP1].mkv",
                source_dir="/src",
                original_name="Show [NCOP1].mkv",
                reason="无字幕片头/片尾视频",
            )],
        )
        scraper.finalize_plan_evidence(plan)
        notice = next(item for item in plan.notices if item.message.startswith("确认执行后将删除"))
        self.assertEqual(notice.code, "destructive_cleanup_requires_review")
        self.assertTrue(notice.requires_review)

    def test_appledouble_cleanup_notice_remains_automatic(self):
        plan = scraper.Plan(
            mode="tv",
            source_root="/src",
            target_root="/target",
            files=[],
            warnings=["确认执行后将删除明确无用的系统隐藏/片头片尾/广告文件：._Show.mkv"],
            metadata={},
            cleanup_files=[scraper.PlannedCleanup(
                source_path="/src/._Show.mkv",
                source_dir="/src",
                original_name="._Show.mkv",
                reason="macOS AppleDouble 隐藏文件",
            )],
        )
        scraper.finalize_plan_evidence(plan)
        notice = next(item for item in plan.notices if item.message.startswith("确认执行后将删除"))
        self.assertEqual(notice.code, "housekeeping_cleanup")
        self.assertFalse(notice.requires_review)

    def test_split_sp_then_number_brackets_are_an_explicit_special(self):
        self.assertEqual(
            scraper.extract_episode_key("[DBD-Raws][Fate stay night][SP][02].mkv"),
            scraper.EpisodeKey("special", 2),
        )

    def test_three_digit_absolute_episode_is_not_truncated(self):
        self.assertEqual(
            scraper.extract_episode_key("[Group] Gintama - 100 [1080p].mkv"),
            scraper.EpisodeKey("regular", 100),
        )

    def test_japanese_episode_kanji_is_a_regular_episode(self):
        self.assertEqual(
            scraper.extract_episode_key(
                "[DMG] Reゼロから始める異世界生活 第25話「ただそれだけの物語」.mp4"
            ),
            scraper.EpisodeKey("regular", 25),
        )

    def test_numbered_cut_subtitle_follows_directors_cut_episode(self):
        name = "Toaru Kagaku no Railgun T [25 cut].ass"
        self.assertEqual(
            scraper.extract_episode_key(name),
            scraper.EpisodeKey("regular", 25),
        )
        self.assertEqual(scraper.edition_tag(name), "Director's Cut")

    def test_inner_ova_directory_overrides_outer_future_season(self):
        item = {
            "name": "Show Memory Snow [1080p].mp4",
            "full_path": (
                "/src/Show 第四季/Show/OVA.2018.Memory Snow/"
                "Show Memory Snow [1080p].mp4"
            ),
        }
        self.assertTrue(scraper._special_context_overrides_parent_season(item))

    def test_tokuten_anime_number_is_an_explicit_special(self):
        self.assertEqual(
            scraper.extract_episode_key(
                "Fate／Kaleid Liner Prisma Illya [Tokuten_Anime05]"
                "[Ma10p_2160p][x265_flac_ass].mkv"
            ),
            scraper.EpisodeKey("special", 5),
        )

    def test_disc_commercials_and_sponsor_eyecatches_are_disposable(self):
        self.assertIsNotNone(
            scraper.cleanup_reason("Prisma Phantasm [CM][Ma10p_2160p].mkv")
        )
        self.assertIsNotNone(
            scraper.cleanup_reason("Show [CM01][Ma10p_2160p][x265_flac].mkv")
        )
        self.assertIsNotNone(
            scraper.cleanup_reason(
                "Movie [CM Collection][Ma10p_2160p][x265_flac].mkv"
            )
        )
        self.assertIsNotNone(
            scraper.cleanup_reason(
                "Show [Sponser_Eyecatch_Collection][Ma10p_2160p].mkv"
            )
        )
        self.assertIsNotNone(
            scraper.cleanup_reason("Show [MenuOVA][Ma10p_2160p].mkv")
        )

    def test_source_name_infers_only_explicit_season_markers(self):
        cases = {
            "/tv/Frieren S02": 2,
            "/tv/Frieren.Season 03.2160p": 3,
            "/tv/Kimi ni Todoke 2nd Season [01].ass": 2,
            "/tv/从零开始的异世界生活S03.Part2.2024": 3,
            "/tv/葬送的芙莉莲 第 2 季": 2,
            "/tv/03.無職轉生～第三季": 3,
            "/tv/约会大作战 V": 5,
            "/tv/86-不存在的战区- (2021) {tmdb-100565}": None,
            "/tv/Show E12": None,
        }
        for source, expected in cases.items():
            with self.subTest(source=source):
                self.assertEqual(scraper._season_from_source(source), expected)

    def test_release_season_dash_episode_requires_official_boundary(self):
        name = (
            "[ANi] GRAND BLUE 碧藍之海 3 - 01 "
            "[1080P][Baha][WEB-DL][AAC AVC][CHT].mp4"
        )
        self.assertEqual(
            scraper._explicit_release_season_episode(name, {1: 12, 2: 12, 3: 12}),
            (3, 1),
        )
        self.assertIsNone(
            scraper._explicit_release_season_episode(name, {1: 12, 2: 12})
        )

    def test_live_action_folder_is_explicit_movie_context(self):
        item = {
            "name": "碧蓝之海.mkv",
            "full_path": "/src/真人版（2020）内嵌简中字幕 1080P/碧蓝之海.mkv",
        }
        self.assertTrue(scraper._has_movie_context(item))
        self.assertEqual(scraper._movie_queries_from_item(item)[0], "碧蓝之海")

    def test_explicit_season_parent_is_not_fuzzy_named_special(self):
        item = {
            "name": "Toaru Majutsu no Index III [01].mkv",
            "full_path": (
                "/src/魔法禁书目录 第三季/"
                "Toaru Majutsu no Index III [01].mkv"
            ),
        }
        self.assertFalse(
            scraper._matches_named_special_release_context(
                item,
                {1: ["魔法禁书目录 第三季 特典动画"]},
            )
        )

    def test_query_removes_chinese_season_marker(self):
        self.assertEqual(scraper._query_from_source("/tv/葬送的芙莉莲 第2季"), "葬送的芙莉莲")

    def test_query_removes_season_range_and_release_package_suffix(self):
        self.assertEqual(
            scraper._query_from_source(
                "/quark/影视/待刮削/"
                "魔法禁书目录 S01-S03合集 4K超清2160P收藏版 "
                "内封中文字幕 附两部剧场版"
            ),
            "魔法禁书目录",
        )
        self.assertEqual(
            scraper._query_from_source("/tv/魔法禁书目录 第1-3季 合集"),
            "魔法禁书目录",
        )
        self.assertEqual(scraper._query_from_source("/tv/MS01-S03 Project"), "MS01-S03 Project")
        self.assertEqual(scraper._query_from_source("/tv/标题 收藏版的秘密"), "标题 收藏版的秘密")
        self.assertEqual(scraper._query_from_source("/tv/标题 内封之印"), "标题 内封之印")

    def test_query_expands_bounded_86_release_abbreviation(self):
        self.assertEqual(
            scraper._query_from_source("/tv/86不存.ZDZQ 全2季 1080P"),
            "86 -不存在的战区-",
        )

    def test_cross_script_season_variant_uses_longest_unique_suffix(self):
        show = {
            "seasons": [
                {"season_number": 2, "name": "魔法少女☆伊莉雅 2wei!"},
                {"season_number": 3, "name": "魔法少女☆伊莉雅 2wei Herz!"},
                {"season_number": 4, "name": "魔法少女☆伊莉雅 3rei!!"},
            ]
        }
        self.assertEqual(
            scraper._season_from_series_variant(
                "Fate Kaleid Liner Prisma Illya 2wei Herz! [01].ass",
                show,
            ),
            3,
        )
        self.assertEqual(
            scraper._season_from_series_variant(
                "Fate Kaleid Liner Prisma Illya [01].ass",
                {
                    "seasons": [
                        {"season_number": 1, "name": "魔法少女☆伊莉雅"},
                        *show["seasons"],
                    ]
                },
            ),
            1,
        )

    def test_release_noisy_official_season_variant_maps_by_exact_cleaned_title(self):
        show = {
            "seasons": [
                {"season_number": 1, "name": "魔法少女☆伊莉雅"},
                {"season_number": 4, "name": "魔法少女☆伊莉雅 3rei!!"},
            ]
        }
        self.assertEqual(
            scraper._season_from_series_variant(
                "04 魔法少女☆伊莉雅 3rei!!（2016）全12集 内封简繁字幕 "
                "4K（Ma10p x265 flac）",
                show,
            ),
            4,
        )
        self.assertIsNone(
            scraper._season_from_series_variant("04 其它作品 3rei!!", show)
        )

    def test_parent_title_prefix_does_not_swallow_named_spinoff(self):
        show = {
            "seasons": [
                {"season_number": 1, "name": "约会大作战"},
                {"season_number": 2, "name": "约会大作战Ⅱ"},
            ]
        }
        self.assertIsNone(
            scraper._season_from_series_variant("约会大作战 赤黑新章", show)
        )

    def test_query_removes_library_letter_and_4k_prefix(self):
        self.assertEqual(scraper._query_from_source("/quark/影视/番剧/H 4k 好想告诉你"), "好想告诉你")
        self.assertEqual(scraper._query_from_source("/quark/影视/番剧/L 4K 来自深渊"), "来自深渊")
        self.assertEqual(scraper._query_from_source("/quark/影视/番剧/B 4k 白色相薄"), "白色相簿")
        self.assertEqual(scraper._query_from_source("/quark/影视/番剧/E 4k 恶魔高校(1)"), "恶魔高校")
        self.assertEqual(scraper._query_from_source("/quark/影视/番剧/R 日在校园"), "日在校园")
        self.assertEqual(
            scraper._query_from_source("/quark/影视/番剧/最弱无败神龙"),
            "最弱无败神装机龙",
        )
        self.assertEqual(
            scraper._query_from_source("/quark/影视/待刮削/末日三问.简体内嵌4K"),
            "末日三问",
        )

    def test_bounded_noisy_title_variants_keep_tmdb_as_authority(self):
        self.assertIn(
            "自称恶役大小姐的婚约者观察记录",
            scraper._search_query_variants("Z自称恶役大小姐的婚约者观察记录"),
        )
        self.assertNotIn("战警", scraper._search_query_variants("X战警"))
        self.assertIn(
            "末日时在做什么？有没有空？可以来拯救吗？",
            scraper._search_query_variants("末日三问"),
        )
        self.assertIn(
            "杖与剑的魔剑谭",
            scraper._search_query_variants("杖与剑的魔法谭"),
        )
        self.assertTrue(any(
            variant.startswith("瑞克和莫蒂")
            for variant in scraper._search_query_variants("瑞克和MD 1-9季")
        ))

    def test_franchise_member_queries_remove_release_folder_noise(self):
        queries = scraper._franchise_member_queries(
            "/番剧/Fate全系列/01 命运之夜（2006）全24集 外挂简中字幕 "
            "1080P（DBD-Raws BDRip HEVC-10bit FLAC）"
        )
        self.assertIn("命运之夜(2006)", queries)
        self.assertFalse(any("外挂" in query or "BDRip" in query for query in queries))
        self.assertIn("命运之夜", scraper._search_query_variants("命运之夜(2006)"))

    def test_search_variants_remove_parenthetical_year_month_as_one_date(self):
        variants = scraper._search_query_variants("叛逆的物语 (2013 10)")
        self.assertIn("叛逆的物语", variants)
        self.assertIn("叛逆的故事", variants)
        self.assertNotIn("叛逆的物语 10", variants)

    def test_codec_payload_is_not_used_as_a_title_query(self):
        for query in ("MAI Ma10p", "FLAC＋AAC", "x265 flac ass", "1080p HEVC 10bit"):
            with self.subTest(query=query):
                self.assertFalse(scraper._usable_release_title_query(query))
        self.assertTrue(
            scraper._usable_release_title_query("Mushishi Zoku Shou Suzu no Shizuku")
        )

    def test_language_and_release_group_only_folder_is_not_a_title_query(self):
        for query in (
            "简日双语 喵萌奶茶屋",
            "内封简繁字幕 Nekomoe kissaten",
            "北宇治字幕组 简中 1080P",
        ):
            with self.subTest(query=query):
                self.assertFalse(scraper._usable_release_title_query(query))
        self.assertTrue(
            scraper._usable_release_title_query(
                "青春猪头少年不会梦到兔女郎学姐 简日双语 喵萌奶茶屋"
            )
        )

    def test_numbered_sibling_work_is_detached_before_movie_quality_cleanup(self):
        final = {
            "name": "[Ygm] Gintama ~The Final~ [2160p].mkv",
            "full_path": "/src/The Final/[Ygm] Gintama ~The Final~ [2160p].mkv",
        }
        semi_one = {
            "name": "[Ygm] Gintama ~The Semi-Final~ [01][2160p].mkv",
            "full_path": "/src/The Final/[Ygm] Gintama ~The Semi-Final~ [01][2160p].mkv",
        }
        semi_two = {
            "name": "[Ygm] Gintama ~The Semi-Final~ [02][2160p].mkv",
            "full_path": "/src/The Final/[Ygm] Gintama ~The Semi-Final~ [02][2160p].mkv",
        }
        semi_subtitle = {
            "name": "[Ygm] Gintama ~The Semi-Final~ [01][2160p].ass",
            "full_path": "/src/The Final/[Ygm] Gintama ~The Semi-Final~ [01][2160p].ass",
        }
        groups = {1: [final, semi_one, semi_two, semi_subtitle]}
        detached = scraper._detach_numbered_subgroups_from_mixed_movie_groups(groups)
        self.assertEqual({item["full_path"] for item in detached}, {
            semi_one["full_path"], semi_two["full_path"], semi_subtitle["full_path"],
        })
        self.assertEqual(groups, {1: [final]})

    def test_child_work_query_rewrites_only_proven_parent_alias_prefix(self):
        variants = scraper._child_work_query_variants(
            ["Gintama ~The Semi-Final~", "Gintama", "Ma10p_2160p"],
            parent_titles=["银魂", "銀魂"],
            parent_aliases=["Gintama", "银魂", "銀魂"],
        )
        self.assertIn("Gintama ~The Semi-Final~", variants)
        self.assertIn("银魂 The Semi-Final", variants)
        self.assertIn("銀魂 The Semi-Final", variants)
        self.assertNotIn("银魂", variants)
        self.assertFalse(any("Ma10p" in value for value in variants))

    def test_staff_credit_versions_are_named_editions_not_v2(self):
        self.assertEqual(
            scraper.edition_tag("Exodus! [01(Musani Staff Credit Ver.)].mkv"),
            "Musani Staff Credit",
        )
        self.assertEqual(
            scraper.edition_tag("Exodus! [01(Original Staff Credit Ver.)].mkv"),
            "Original Staff Credit",
        )

    def test_named_ova_movie_requires_specific_subtitle_agreement(self):
        match = scraper.AutoMatch(
            media_type="movie",
            tmdb_id=312966,
            title="Mushishi: The Next Chapter - Drops of Bells",
            year="2015",
            confidence=1.0,
            decision_trace={
                "official_titles": ["虫师 续章 铃之滴"],
                "aliases_checked": ["Mushishi", "Mushishi Zoku Shou: Suzu no Shizuku"],
            },
        )
        self.assertFalse(
            scraper._specific_movie_query_agrees_with_match(
                "Mushishi Hihamukage", match
            )
        )
        self.assertTrue(
            scraper._specific_movie_query_agrees_with_match(
                "Mushishi Zoku Shou Suzu no Shizuku", match
            )
        )

    def test_release_decorated_named_special_subtitle_context_is_recognized(self):
        item = {
            "name": "Daisan Hikou Shoujotai [01(Musani Staff Credit Ver.)].ass",
            "full_path": (
                "/src/剧中剧 Daisan Hikou Shoujotai/Daisan Hikou Shoujotai "
                "[01(Musani Staff Credit Ver.)].ass"
            ),
        }
        self.assertTrue(
            scraper._matches_named_special_release_context(
                item,
                {2: ["第三飞行少女队", "The Third Aerial Girls"]},
            )
        )

    def test_franchise_root_label_removes_release_advertising(self):
        cases = {
            "/src/Fate全系列 硬字幕+软字幕 4K+1080P": "Fate",
            # Source-only cleanup cannot invent an official work identity.
            # The canonical tree planner later replaces this noisy wrapper
            # with the independently confirmed primary TMDB title.
            "/src/瑞克和MD 1-9季+日漫版 内封字幕": "瑞克和MD 1-9季+日漫版",
            "/src/H 寒蝉鸣泣之时全系列 外挂+内嵌字幕 1080P": "寒蝉鸣泣之时",
            "/src/魔法少女小圆 系列合集 4K超清2160P收藏版 内封简日双语字幕": "魔法少女小圆",
            (
                "/src/【日漫】青春猪头少年系列.全系列+三部剧场版."
                "简日双语.喵萌奶茶屋.1080P"
            ): "青春猪头少年",
        }
        for source, expected in cases.items():
            with self.subTest(source=source):
                self.assertEqual(scraper._clean_franchise_root_label(source), expected)

    def test_magirepo_bonus_commercials_are_cleanup_not_regular_episodes(self):
        item = {
            "name": "MagiRepo - 01.mkv",
            "full_path": "/src/魔法纪录/Bonus/MagiRepo - 01.mkv",
            "size": 1234,
        }

        self.assertEqual(
            scraper._contextual_cleanup_reason(item),
            "特典动画广告/Animated Magia Report Commercial",
        )
        self.assertEqual(scraper._filter_media([item]), [])
        cleanup = scraper._planned_cleanup_files([item])
        self.assertEqual(len(cleanup), 1)
        self.assertEqual(cleanup[0].source_path, item["full_path"])

    def test_explicit_ncop_ed_directory_and_versioned_ncop_are_cleanup(self):
        items = [
            {
                "name": "[ReinForce] Mantama - OP (BDRip 1920x1080 x264 FLAC).mkv",
                "full_path": "/src/第三季/NCOP&ED/[ReinForce] Mantama - OP (BDRip 1920x1080 x264 FLAC).mkv",
                "size": 1234,
            },
            {
                "name": "[Ygm] Gintama° [NCOP4v3][Ma10p_2160p][x265_flac_ass].mkv",
                "full_path": "/src/第三季/NCOP&ED/[Ygm] Gintama° [NCOP4v3][Ma10p_2160p][x265_flac_ass].mkv",
                "size": 2345,
            },
        ]
        self.assertEqual(
            scraper._contextual_cleanup_reason(items[0]),
            "无字幕片头/片尾/光盘菜单视频",
        )
        self.assertEqual(
            scraper.cleanup_reason(items[1]["name"]),
            "无字幕片头/片尾/光盘菜单视频",
        )
        self.assertEqual(len(scraper._planned_cleanup_files(items)), 2)

    def test_shared_residual_policy_plans_nonfeature_attachments_but_defers_subtitles(self):
        items = [
            {"name": "小说.docx", "full_path": "/src/小说.docx", "size": 10},
            {"name": "commentary.flac", "full_path": "/src/commentary.flac", "size": 11},
            {"name": "page001.jpg", "full_path": "/src/漫画/page001.jpg", "size": 12},
            {"name": "E01.zh-CN.mks", "full_path": "/src/E01.zh-CN.mks", "size": 13},
            {"name": "unknown.mkv", "full_path": "/src/unknown.mkv", "size": 14},
        ]

        cleanup = scraper._planned_cleanup_files(items)

        self.assertEqual(
            {item.source_path for item in cleanup},
            {"/src/小说.docx", "/src/commentary.flac", "/src/漫画/page001.jpg"},
        )

    def test_franchise_member_match_never_uses_numeric_episode_names_as_titles(self):
        root = "/番剧/寒蝉全系列/02 寒蝉鸣泣之时·解（2007）全24集"
        files = [
            {"name": f"{number:02d}.mkv", "full_path": f"{root}/{number:02d}.mkv"}
            for number in range(1, 4)
        ]
        queries = scraper._franchise_member_match_queries(root, files)
        self.assertIn("寒蝉鸣泣之时·解(2007)", queries)
        self.assertFalse(any(query.isdigit() for query in queries))

    def test_multi_episode_franchise_member_forces_tv_before_ova_movie_match(self):
        root = "/src/Fate/03 Prisma Illya 2wei Herz 2015"
        files = [
            {
                "name": f"Prisma Illya 2wei Herz [{number:02d}].mkv",
                "full_path": f"{root}/Prisma Illya 2wei Herz [{number:02d}].mkv",
            }
            for number in range(1, 11)
        ] + [
            {
                "name": "Prisma Illya 2wei Herz [Tokuten_Anime01].mkv",
                "full_path": f"{root}/SPs/Prisma Illya 2wei Herz [Tokuten_Anime01].mkv",
            }
        ]
        confirmed = mock.Mock(
            status="confirmed", media_type="tv", tmdb_id=1,
            title="Fate Prisma Illya 2wei Herz", year="2015",
        )
        stolen_movie = mock.Mock(
            status="confirmed",
            media_type="movie",
            tmdb_id=999,
            title="Herz OVA Movie",
        )

        def match_query(_client, query, **_kwargs):
            selected = confirmed if query == "Fate" else stolen_movie
            return selected, [selected]

        with mock.patch.object(
            scraper,
            "auto_match_tmdb",
            side_effect=match_query,
        ) as matcher:
            result = scraper._franchise_member_match(
                FakeTMDB({
                    "/tv/1": {
                        "seasons": [{"season_number": 1, "episode_count": 10}],
                    },
                }),
                root,
                files,
                target_parent="/library/番剧/Fate",
            )

        self.assertIs(result, confirmed)
        self.assertEqual(matcher.call_args.kwargs["media_type"], "tv")
        self.assertEqual(matcher.call_args.kwargs["expected_episode_count"], 10)
        self.assertEqual(matcher.call_args.args[1], "Fate")

    def test_single_video_franchise_member_may_match_movie_under_tv_library(self):
        root = (
            "/src/Fate/"
            "05 命运 冠位指定：序章（2016）内封+外挂字幕 4K"
        )
        files = [
            {
                "name": "4K 内封简中字幕（MAI Ma10p x265 flac ass）.mkv",
                "full_path": (
                    f"{root}/4K 内封简中字幕（MAI Ma10p x265 flac ass）.mkv"
                ),
            },
            {"name": "简中.ass", "full_path": f"{root}/简中.ass"},
        ]
        movie = mock.Mock(
            status="confirmed",
            media_type="movie",
            tmdb_id=428142,
            title="命运／冠位指定：序章",
        )

        with mock.patch.object(
            scraper,
            "auto_match_tmdb",
            return_value=(movie, [movie]),
        ) as matcher:
            result = scraper._franchise_member_match(
                FakeTMDB({}),
                root,
                files,
                target_parent="/library/番剧/Fate",
            )

        self.assertIs(result, movie)
        self.assertIsNone(matcher.call_args.kwargs["media_type"])
        self.assertIsNone(matcher.call_args.kwargs["expected_episode_count"])

    def test_release_group_name_is_not_a_franchise_member_title_query(self):
        root = (
            "/src/寒蝉全系列/"
            "05 寒蝉鸣泣之时·扩（2013）全1集 内嵌简中字幕 1080P"
        )
        files = [{
            "name": (
                "[DBD-Raw] 寒蝉鸣泣之时 扩 [OVA][1080P][BDrip]"
                "[HEVC-10bit][GB][FLAC＋AAC][MKV].mkv"
            ),
            "full_path": (
                f"{root}/[DBD-Raw] 寒蝉鸣泣之时 扩 [OVA][1080P]"
                "[BDrip][HEVC-10bit][GB][FLAC＋AAC][MKV].mkv"
            ),
        }]

        queries = scraper._franchise_member_match_queries(root, files)

        self.assertIn("寒蝉鸣泣之时·扩", queries)
        self.assertIn("寒蝉鸣泣之时 扩", queries)
        self.assertNotIn("DBD-Raw", queries)

    def test_nested_movie_part_query_keeps_parent_title_and_child_year(self):
        root = (
            "/番剧/Fate全系列/"
            "11 命运 冠位指定 神圣圆桌领域卡美洛 剧场版（2020-2021）前篇+后篇/"
            "01 前篇（2020）内封&外挂简中字幕 4K"
        )
        files = [{
            "name": (
                "[MAI] Fate Grand Order Shinsei Entaku Ryouiki Camelot "
                "Paladin Agateram 01 [Ma10p_2160p].mkv"
            ),
            "full_path": (
                f"{root}/[MAI] Fate Grand Order Shinsei Entaku Ryouiki Camelot "
                "Paladin Agateram 01 [Ma10p_2160p].mkv"
            ),
        }]

        queries = scraper._franchise_member_match_queries(root, files)

        self.assertIn(
            "命运 冠位指定 神圣圆桌领域卡美洛 剧场版 前篇(2020)",
            queries,
        )
        self.assertFalse(any(query in {"01 前篇(2020)", "前篇(2020)"} for query in queries))
        self.assertLess(
            queries.index("命运 冠位指定 神圣圆桌领域卡美洛 剧场版 前篇(2020)"),
            queries.index(
                "Fate Grand Order Shinsei Entaku Ryouiki Camelot "
                "Paladin Agateram 01"
            ),
        )

    def test_franchise_context_query_also_has_yearless_variant(self):
        root = (
            "/番剧/魔法少女小圆 系列合集/"
            "② 剧场版 [前篇] 起始的物语 (2012.10)"
        )
        queries = scraper._franchise_member_match_queries(root, [])
        self.assertIn("魔法少女小圆 起始的物语", queries)

    def test_franchise_tv_season_arc_has_bounded_base_series_fallback(self):
        root = (
            "/番剧/魔法少女小圆 系列合集/"
            "魔法纪录 魔法少女小圆外传 2 -觉醒前夜- (2021.7)"
        )
        queries = scraper._franchise_member_match_queries(root, [])
        self.assertIn("魔法纪录 魔法少女小圆外传", queries)
        final_root = (
            "/番剧/魔法少女小圆 系列合集/"
            "魔法纪录 魔法少女小圆外传 最终季 -浅梦之晓- (2022.4)"
        )
        self.assertIn(
            "魔法纪录 魔法少女小圆外传",
            scraper._franchise_member_match_queries(final_root, []),
        )

    def test_bare_episode_number_is_not_work_identity_evidence(self):
        self.assertFalse(scraper._usable_release_title_query("01"))
        self.assertFalse(scraper._usable_release_title_query("S02"))
        self.assertFalse(scraper._usable_release_title_query("DBD-Raw"))
        self.assertTrue(scraper._usable_release_title_query("魔法少女☆伊莉雅 2wei"))

    def test_subtitle_only_movie_group_is_retained_without_aborting_tv_batch(self):
        valid, orphan = scraper._partition_movie_groups_with_video({
            10: [{"name": "Movie.mkv", "full_path": "/src/Movie.mkv"}],
            20: [{"name": "Movie.ass", "full_path": "/src/Movie.ass"}],
        })
        self.assertEqual(list(valid), [10])
        self.assertEqual([item["full_path"] for item in orphan], ["/src/Movie.ass"])

    def test_batch_subtitle_only_folder_follows_one_exact_spinoff_video(self):
        video = scraper.PlannedFile(
            source_path=(
                "/src/约会大作战外传/"
                "Date A Bullet - Dead or Bullet [1080p].mkv"
            ),
            source_dir="/src/约会大作战外传",
            original_name="Date A Bullet - Dead or Bullet [1080p].mkv",
            final_name="约会大作战：赤黑新章-虚或实 (2020).mkv",
            target_dir="/library/约会大作战/约会大作战：赤黑新章-虚或实 (2020)",
            media_kind="video",
        )
        plan = scraper.Plan(
            "movie", "/src/约会大作战外传", video.target_dir,
            [video], [], {"tmdb_id": 1},
        )
        subtitle = {
            "name": "Date A Bullet - Dead or Bullet [1080p].sc.ass",
            "full_path": (
                "/src/备份字幕/"
                "Date A Bullet - Dead or Bullet [1080p].sc.ass"
            ),
            "size": 321,
        }

        attached, problems = scraper._attach_unique_batch_subtitle_companions(
            "/src", [subtitle], [plan]
        )

        self.assertEqual(problems, [])
        self.assertEqual(len(attached), 1)
        self.assertEqual(attached[0].target_dir, video.target_dir)
        self.assertEqual(
            attached[0].final_name,
            "约会大作战：赤黑新章-虚或实 (2020).zh-CN.ass",
        )

    def test_batch_subtitle_ambiguity_remains_a_problem(self):
        videos = [
            scraper.PlannedFile(
                source_path=f"/src/movie-{number}/Shared Release.mkv",
                source_dir=f"/src/movie-{number}",
                original_name="Shared Release.mkv",
                final_name=f"Movie {number} (2020).mkv",
                target_dir=f"/library/Movie {number} (2020)",
                media_kind="video",
            )
            for number in (1, 2)
        ]
        plans = [
            scraper.Plan("movie", video.source_dir, video.target_dir, [video], [], {})
            for video in videos
        ]
        subtitle = {
            "name": "Shared Release.ass",
            "full_path": "/src/备份字幕/Shared Release.ass",
        }

        attached, problems = scraper._attach_unique_batch_subtitle_companions(
            "/src", [subtitle], plans
        )

        self.assertEqual(attached, [])
        self.assertEqual(len(problems), 1)
        self.assertIn("多个", problems[0].reason)

    def test_numbered_backup_subtitle_exactly_follows_confirmed_movie_release(self):
        video = {
            "name": "[TUDO&Ygm] DATE A BULLET [01][Ma10p_2160p].mkv",
            "full_path": "/src/赤黑新章/[TUDO&Ygm] DATE A BULLET [01][Ma10p_2160p].mkv",
        }
        subtitle = {
            "name": "[TUDO] DATE A BULLET [01][Ma10p_2160p].ass",
            "full_path": "/src/备份字幕/外传/[TUDO] DATE A BULLET [01][Ma10p_2160p].ass",
        }
        groups = {685099: [video]}
        remaining = scraper._attach_unique_movie_subtitles(groups, [subtitle])
        self.assertEqual(remaining, [])
        self.assertEqual(groups[685099], [video, subtitle])

    def test_release_group_variant_subtitle_inherits_proven_video_override(self):
        items = [
            {
                "name": "[TUDO&Ygm] DATE A BULLET [01][Ma10_2160p].mkv",
                "full_path": "/src/赤黑/[TUDO&Ygm] DATE A BULLET [01][Ma10_2160p].mkv",
                "_episode_kind_override": "special",
                "_episode_key_override": 3,
            },
            {
                "name": "[TUDO] DATE A BULLET [01][Ma10_2160p].ass",
                "full_path": "/src/备份字幕/[TUDO] DATE A BULLET [01][Ma10_2160p].ass",
            },
        ]
        self.assertEqual(
            scraper._propagate_explicit_video_episode_overrides(items), 1
        )
        self.assertEqual(items[1]["_episode_kind_override"], "special")
        self.assertEqual(items[1]["_episode_key_override"], 3)

    def test_complete_root_run_fills_only_missing_official_season(self):
        unknown = [
            {"name": f"Show [{number:02d}].mkv", "full_path": f"/src/Show [{number:02d}].mkv"}
            for number in range(1, 4)
        ] + [
            {"name": f"Show [{number:02d}].ass", "full_path": f"/src/备份字幕/Show [{number:02d}].ass"}
            for number in range(1, 4)
        ]
        season, attached, remaining = scraper._proven_missing_root_season_files(
            "/src", unknown, {2: [{}], 3: [{}]}, {1: 3, 2: 4, 3: 5}
        )
        self.assertEqual(season, 1)
        self.assertEqual(len(attached), 6)
        self.assertEqual(remaining, [])

    def test_resource_gaps_report_only_released_missing_episodes(self):
        plan = scraper.Plan(
            mode="tv", source_root="/src", target_root="/library/Show",
            files=[scraper.PlannedFile(
                "/src/Show 01.mkv", "/src", "Show 01.mkv",
                "Show - S01E01 - One.mkv", "/library/Show/Season 01", "video",
            )],
            warnings=[], metadata={},
        )
        gaps = scraper._tv_episode_resource_gaps(
            FakeAList(), plan, series_dir="/library/Show", season=1,
            official_episodes=[
                {"episode_number": 1, "name": "One", "air_date": "2020-01-01"},
                {"episode_number": 2, "name": "Two", "air_date": "2020-01-08"},
                {"episode_number": 3, "name": "TBA"},
                {"episode_number": 4, "name": "Future", "air_date": "2099-01-01"},
            ],
        )
        self.assertEqual([gap["label"] for gap in gaps], ["S01E02 Two"])
        self.assertEqual(gaps[0]["kind"], "missing_episode")

    def test_resource_gaps_treat_multi_episode_ranges_as_complete(self):
        plan = scraper.Plan(
            mode="tv", source_root="/src", target_root="/library/Show",
            files=[scraper.PlannedFile(
                "/src/Show 01-02.mkv", "/src", "Show 01-02.mkv",
                "Show - S01E01-E02 - One + Two.mkv", "/library/Show/Season 01", "video",
            )],
            warnings=[], metadata={},
        )
        gaps = scraper._tv_episode_resource_gaps(
            FakeAList(), plan, series_dir="/library/Show", season=1,
            official_episodes=[
                {"episode_number": 1, "name": "One", "air_date": "2020-01-01"},
                {"episode_number": 2, "name": "Two", "air_date": "2020-01-08"},
            ],
        )
        self.assertEqual(gaps, [])

    def test_resource_gaps_do_not_use_a_sibling_series_in_a_collection(self):
        plan = scraper.Plan(
            mode="collection", source_root="/src", target_root="/library/Collection",
            files=[scraper.PlannedFile(
                "/src/Sibling 01.mkv", "/src", "Sibling 01.mkv",
                "Sibling - S01E01 - One.mkv", "/library/Collection/Sibling/Season 01", "video",
            )],
            warnings=[], metadata={},
        )
        gaps = scraper._tv_episode_resource_gaps(
            FakeAList(), plan,
            series_dir="/library/Collection/Expected", season=1,
            official_episodes=[
                {"episode_number": 1, "name": "One", "air_date": "2020-01-01"},
            ],
        )
        self.assertEqual([gap["label"] for gap in gaps], ["S01E01 One"])

    def test_plan_roundtrip_accepts_lower_resolution_subtitle_cleanup_reason(self):
        preferred = "/src/4K/Show [01][2160p].ass"
        plan = scraper.Plan(
            mode="tv", source_root="/src", target_root="/library/Show",
            files=[], warnings=[], metadata={
                "title": "Show", "tmdb_id": 1, "year": "2020",
                "season": 1, "absolute": False, "episode_group": None,
            },
            cleanup_files=[scraper.PlannedCleanup(
                source_path="/src/1080p/Show [01][1080p].ass",
                source_dir="/src/1080p",
                original_name="Show [01][1080p].ass",
                reason=scraper._lower_resolution_subtitle_cleanup_reason(preferred),
            )],
        )
        restored = scraper.plan_from_dict(scraper.plan_to_dict(plan))
        self.assertEqual(restored.cleanup_files[0].reason, plan.cleanup_files[0].reason)

    def test_plan_roundtrip_accepts_traditional_language_cleanup_reason(self):
        preferred = "/src/简中/Show [01][1080p].mp4"
        plan = scraper.Plan(
            mode="tv", source_root="/src", target_root="/library/Show",
            files=[], warnings=[], metadata={
                "title": "Show", "tmdb_id": 1, "year": "2020",
                "season": 1, "absolute": False, "episode_group": None,
            },
            cleanup_files=[scraper.PlannedCleanup(
                source_path="/src/繁中/Show [01][1080p].mp4",
                source_dir="/src/繁中",
                original_name="Show [01][1080p].mp4",
                reason=scraper._traditional_language_cleanup_reason(preferred),
            )],
        )
        restored = scraper.plan_from_dict(scraper.plan_to_dict(plan))
        self.assertEqual(restored.cleanup_files[0].reason, plan.cleanup_files[0].reason)

    def test_resource_gaps_report_only_released_missing_seasons(self):
        plan = scraper.Plan(
            mode="tv", source_root="/src", target_root="/library/Show",
            files=[scraper.PlannedFile(
                "/src/Show 01.mkv", "/src", "Show 01.mkv",
                "Show - S01E01 - One.mkv", "/library/Show/Season 01", "video",
            )],
            warnings=[], metadata={},
        )
        gaps = scraper._tv_season_resource_gaps(
            FakeAList(), plan, series_dir="/library/Show",
            official_seasons=[
                {"season_number": 0, "episode_count": 2, "air_date": "2019-01-01"},
                {"season_number": 1, "episode_count": 1, "air_date": "2020-01-01"},
                {"season_number": 2, "episode_count": 12, "air_date": "2021-01-01"},
                {"season_number": 3, "episode_count": 12},
                {"season_number": 4, "episode_count": 12, "air_date": "2099-01-01"},
            ],
        )
        self.assertEqual([gap["label"] for gap in gaps], ["Season 02 Season 2"])
        self.assertEqual(gaps[0]["kind"], "missing_season")
        self.assertEqual(gaps[0]["season_name"], "Season 2")
        self.assertEqual(gaps[0]["expected_episode_count"], 12)

    def test_divergent_sequel_check_uses_season_parent_not_episode_name(self):
        show = {"seasons": [{"season_number": 4, "name": "第四季"}]}
        queries = scraper._season_parent_identity_queries(
            [{
                "name": "Re Zero 4nd Season [01].mkv",
                "full_path": "/src/4K动漫/Re：从零开始的异世界生活 第四季/Re Zero 4nd Season [01].mkv",
            }],
            source_root="/src",
            season_number=4,
            show=show,
        )
        self.assertEqual(queries, ["Re：从零开始的异世界生活 第四季"])
        self.assertNotIn("Re Zero 4nd Season 01", queries)

    def test_complete_root_run_can_be_first_long_season_block_alternate(self):
        unknown = [
            {"name": f"Show [{number:02d}].mkv", "full_path": f"/src/Show [{number:02d}].mkv"}
            for number in range(1, 4)
        ] + [
            {"name": f"Show [{number:02d}].ass", "full_path": f"/src/备份字幕/Show [{number:02d}].ass"}
            for number in range(1, 4)
        ]
        attached, remaining = scraper._proven_root_first_broadcast_block_files(
            "/src", unknown, 3
        )
        self.assertEqual(len(attached), 6)
        self.assertEqual(remaining, [])

    def test_json_http_retries_incomplete_chunked_response(self):
        client = scraper.JsonHttpClient(retries=1)
        with mock.patch(
            "urllib.request.urlopen",
            side_effect=[
                http.client.IncompleteRead(b""),
                io.BytesIO(b'{"ok": true}'),
            ],
        ), mock.patch("time.sleep"):
            self.assertEqual(
                client.request_json("https://example.test/data"),
                {"ok": True},
            )

    def test_movie_query_keeps_localized_and_embedded_english_titles(self):
        item = {
            "name": "来自深渊：出发的黎明.Made.in.Abyss.Journey's.Dawn.2019.BD1080P.日语中字.mp4"
        }
        self.assertEqual(
            scraper._movie_queries_from_item(item),
            [
                "来自深渊：出发的黎明",
                "Made in Abyss Journey's Dawn 2019",
                "Made in Abyss Journey's Dawn",
            ],
        )

    def test_movie_query_uses_descriptive_parent_when_release_name_is_generic(self):
        item = {
            "name": (
                "[Comicat&KissSub]"
                "[Tensei shitara Slime Datta Ken Movie - Guren no Kizuna-hen]"
                "[1080P][GB].mp4"
            ),
            "full_path": (
                "/src/2022.11 轉生史萊姆 劇場版 紅蓮之絆篇/[字幕组][劇場版]/"
                "[Comicat&KissSub]"
                "[Tensei shitara Slime Datta Ken Movie - Guren no Kizuna-hen]"
                "[1080P][GB].mp4"
            ),
        }
        queries = scraper._movie_queries_from_item(item)
        self.assertIn("紅蓮之絆篇", queries)
        self.assertIn(
            "Tensei shitara Slime Datta Ken Movie - Guren no Kizuna-hen",
            queries,
        )

    def test_movie_query_removes_ova_ordinal_and_release_checksum(self):
        item = {
            "name": (
                "[Group] Re：ゼロから始める異世界生活 "
                "OVA02「氷結の絆」 [1080P](412FC32B).mp4"
            )
        }
        self.assertIn(
            "Re：ゼロから始める異世界生活 「氷結の絆」",
            scraper._movie_queries_from_item(item),
        )

    def test_movie_query_removes_parenthesized_release_codec_payload(self):
        item = {
            "name": "痛觉残留（MAI Ma10p x265 flac5.1 ass）.mkv",
            "full_path": "/src/03 第三章 痛觉残留（2008）/痛觉残留（MAI Ma10p x265 flac5.1 ass）.mkv",
        }
        self.assertEqual(
            scraper._movie_query_from_item(item),
            "痛觉残留",
        )

    def test_movie_marker_in_filename_is_explicit_movie_context(self):
        item = {
            "name": "[TUDO&Ygm] STEINS;GATE Movie [Ma10p_2160p].mkv",
            "full_path": (
                "/src/命运石之门 负荷领域的既视感/"
                "[TUDO&Ygm] STEINS;GATE Movie [Ma10p_2160p].mkv"
            ),
        }
        self.assertTrue(scraper._has_movie_context(item))

    def test_numbered_live_action_encodes_use_unique_official_collection_order(self):
        root = "/src/S 死亡笔记（动漫+漫画+真人版）"
        named = f"{root}/死亡笔记 真人版"
        backup = f"{root}/死亡笔记1-3（真人版）中文字幕"
        files = [
            {"name": "死亡笔记1：前篇 日版.mkv", "full_path": f"{named}/死亡笔记1：前篇 日版.mkv"},
            {"name": "死亡笔记2：最后的名字 日版.mkv", "full_path": f"{named}/死亡笔记2：最后的名字 日版.mkv"},
            {"name": "死亡笔记3：L改变世界 日版.mkv", "full_path": f"{named}/死亡笔记3：L改变世界 日版.mkv"},
            {"name": "死亡笔记4：点亮新世界 台版.mkv", "full_path": f"{named}/死亡笔记4：点亮新世界 台版.mkv"},
            {"name": "1国粤日音轨.中文字幕.mkv", "full_path": f"{backup}/1国粤日音轨.中文字幕.mkv"},
            {"name": "2国粤日音轨.中文字幕.mkv", "full_path": f"{backup}/2国粤日音轨.中文字幕.mkv"},
            {"name": "3粤日音轨.中文字幕.mkv", "full_path": f"{backup}/3粤日音轨.中文字幕.mkv"},
        ]
        tmdb = FakeTMDB({
            "/search/collection": {
                "results": [
                    {"id": 102019, "name": "死亡笔记（系列）"},
                    {"id": 444485, "name": "死亡笔记特别篇（系列）"},
                ]
            },
            "/collection/102019": {
                "parts": [
                    {"id": 20329, "release_date": "2008-02-07"},
                    {"id": 16007, "release_date": "2006-06-17"},
                    {"id": 382272, "release_date": "2016-10-29"},
                    {"id": 16140, "release_date": "2006-10-28"},
                ]
            },
            "/collection/444485": {
                "parts": [
                    {"id": 51482, "release_date": "2007-08-31"},
                    {"id": 68555, "release_date": "2009-10-07"},
                ]
            },
        })

        groups, remaining, warnings = scraper._resolve_numbered_movie_collection_groups(
            tmdb,
            files,
        )

        self.assertEqual(remaining, [])
        self.assertEqual(
            {movie_id: len(items) for movie_id, items in groups.items()},
            {16007: 2, 16140: 2, 20329: 2, 382272: 1},
        )
        self.assertEqual(len(warnings), 2)
        self.assertTrue(all("TMDB 合集 102019" in warning for warning in warnings))

    def test_numbered_live_action_group_rejects_incomplete_unlabelled_range(self):
        parent = "/src/Show 真人版"
        files = [
            {"name": f"Show{number}：Part.mkv", "full_path": f"{parent}/Show{number}：Part.mkv"}
            for number in range(1, 4)
        ]
        tmdb = FakeTMDB({
            "/search/collection": {"results": [{"id": 9, "name": "Show Collection"}]},
            "/collection/9": {
                "parts": [
                    {"id": number, "release_date": f"202{number}-01-01"}
                    for number in range(1, 5)
                ]
            },
        })
        groups, remaining, warnings = scraper._resolve_numbered_movie_collection_groups(
            tmdb,
            files,
        )
        self.assertEqual(groups, {})
        self.assertEqual(remaining, files)
        self.assertEqual(warnings, [])

    def test_smart_tv_pipeline_routes_numbered_live_action_collection_before_season_inference(self):
        source = "/src/Show"
        files = [
            {"name": "Show [01].mkv", "full_path": f"{source}/Show [01].mkv", "size": 1_000},
            {"name": "Show1：First.mkv", "full_path": f"{source}/Show 真人版/Show1：First.mkv", "size": 2_000},
            {"name": "Show2：Second.mkv", "full_path": f"{source}/Show 真人版/Show2：Second.mkv", "size": 2_100},
            {"name": "1国粤日音轨.mkv", "full_path": f"{source}/Show1-2（真人版）中文字幕/1国粤日音轨.mkv", "size": 1_000},
            {"name": "2国粤日音轨.mkv", "full_path": f"{source}/Show1-2（真人版）中文字幕/2国粤日音轨.mkv", "size": 1_100},
        ]
        tmdb = FakeTMDB({
            "/tv/1": {
                "name": "Show",
                "original_name": "Show",
                "first_air_date": "2020-01-01",
                "poster_path": None,
                "seasons": [{"season_number": 1, "episode_count": 1}],
            },
            "/tv/1/season/1": {
                "episodes": [{"episode_number": 1, "name": "Episode 1"}]
            },
            "/tv/1/season/0": {"episodes": []},
            "/search/collection": {
                "results": [{"id": 9, "name": "Show Collection"}]
            },
            "/collection/9": {
                "parts": [
                    {"id": 101, "title": "First", "release_date": "2021-01-01"},
                    {"id": 102, "title": "Second", "release_date": "2022-01-01"},
                ]
            },
            "/movie/101": {
                "title": "First", "release_date": "2021-01-01", "poster_path": None,
            },
            "/movie/102": {
                "title": "Second", "release_date": "2022-01-01", "poster_path": None,
            },
        })

        plan = scraper.build_tv_plan_smart(
            auto_episode_mode=True,
            alist=FakeAList(files),
            tmdb_client=tmdb,
            src_path=source,
            parent_path="/library",
            tmdb_id=1,
            season=1,
            absolute=False,
            prefer_simplified=True,
            allow_unmapped=False,
            episode_map_path=None,
            episode_group_id=None,
        )

        self.assertEqual(plan.problem_files, [])
        handled = {item.source_path for item in plan.files} | {
            item.source_path for item in plan.cleanup_files
        }
        self.assertTrue(all(item["full_path"] in handled for item in files))
        self.assertTrue(any(
            item.final_name == "First (2021).mkv"
            and item.target_dir == "/library/First (2021)"
            for item in plan.files
        ))
        self.assertTrue(any(
            item.final_name == "Second (2022).mkv"
            and item.target_dir == "/library/Second (2022)"
            for item in plan.files
        ))
        self.assertFalse(any("v2" in item.final_name for item in plan.files))

    def test_cleanup_rules_only_match_explicit_junk(self):
        self.assertEqual(
            scraper.cleanup_reason("._Show.S01E01.mkv"),
            "macOS AppleDouble 隐藏文件",
        )
        self.assertEqual(
            scraper.cleanup_reason("Show [NCOP1].mkv"),
            "无字幕片头/片尾/光盘菜单视频",
        )

    def test_title_season_dash_episode_uses_parent_season_not_episode_range(self):
        name = (
            "[DMG&LoliHouse] Youjitsu 3 - 10 "
            "[WebRip 1080p HEVC-10bit AAC ASSx2].mkv"
        )
        groups = scraper.parse_ep_files([
            {"name": name, "full_path": f"/src/第三季/{name}"}
        ])
        self.assertEqual(list(groups), [scraper.EpisodeKey("regular", 10)])
        self.assertEqual(
            scraper.cleanup_reason("Show [NCED].mp4"),
            "无字幕片头/片尾/光盘菜单视频",
        )
        self.assertIsNone(scraper.cleanup_reason(
            "[Ygm] Fullmetal Alchemist Brotherhood [ED_EP45(O.A. Ver.)][Ma10p_2160p].mkv"
        ))
        self.assertIsNone(
            scraper.cleanup_reason("Fullmetal Alchemist Brotherhood [EP45][Ma10p_2160p].mkv")
        )
        self.assertEqual(
            scraper.cleanup_reason("Show [Menu03].mkv"),
            "无字幕片头/片尾/光盘菜单视频",
        )
        self.assertEqual(
            scraper.cleanup_reason(
                "[AI-Raws&swaR-KNA] 【推しの子】Vol.1 メニュー映像 "
                "(BD HEVC 1920x1080 FLAC).mkv"
            ),
            "无字幕片头/片尾/光盘菜单视频",
        )
        for name in (
            "【推しの子】WEB予告#02.mkv",
            "【推しの子】本予告１.mkv",
            "【推しの子】特報.mkv",
            "【推しの子】CM集.mkv",
            "【推しの子】ノンクレジットOP.mkv",
            "Tensura Nikki [Eye Catch].mkv",
        ):
            with self.subTest(name=name):
                self.assertEqual(
                    scraper.cleanup_reason(name),
                    "无字幕片头/片尾/光盘菜单视频",
                )

    def test_hash_prefixed_episode_number_is_preserved(self):
        self.assertEqual(
            scraper.extract_episode_key("【推しの子】#01 (BD HEVC).mkv"),
            scraper.EpisodeKey("regular", 1),
        )
        self.assertIsNone(
            scraper.extract_episode_key("【推しの子】 Behind the Scene.mkv")
        )

    def test_bracketed_finale_marker_preserves_episode_number(self):
        self.assertEqual(
            scraper.extract_episode_key("Show S3 [13 END].ass"),
            scraper.EpisodeKey("regular", 13),
        )

    def test_local_and_absolute_dual_numbering_uses_local_episode(self):
        cases = (
            "[BeanSub][Tensei Shitara Slime Datta Ken S4][01_73].mp4",
            "Tensei Shitara Slime Datta Ken 4th Season - 01(73).ass",
        )
        for name in cases:
            with self.subTest(name=name):
                self.assertEqual(
                    scraper.extract_episode_key(name),
                    scraper.EpisodeKey("regular", 1),
                )
                groups = scraper.parse_ep_files([
                    {"name": name, "full_path": f"/src/第四季/{name}"}
                ])
                self.assertEqual(list(groups), [scraper.EpisodeKey("regular", 1)])

    def test_episode_op_ed_cleanup_requires_bonus_directory_and_matching_main_episode(self):
        main = {
            "name": "[Ygm] Fullmetal Alchemist Brotherhood [45][Ma10p_2160p].mkv",
            "full_path": "/src/[Ygm] Fullmetal Alchemist Brotherhood [45][Ma10p_2160p].mkv",
        }
        ending = {
            "name": "[Ygm] Fullmetal Alchemist Brotherhood [ED_EP45(O.A. Ver.)][Ma10p_2160p].mkv",
            "full_path": "/src/SPs/[Ygm] Fullmetal Alchemist Brotherhood [ED_EP45(O.A. Ver.)][Ma10p_2160p].mkv",
        }
        cleanup = scraper._planned_cleanup_files([main, ending])
        self.assertEqual(len(cleanup), 1)
        self.assertEqual(cleanup[0].source_path, ending["full_path"])
        self.assertIn("同集正片", cleanup[0].reason)

        outside_bonus = dict(ending, full_path="/src/ED_EP45.mkv")
        outside_cleanup = scraper._planned_cleanup_files([main, outside_bonus])
        self.assertEqual([item.source_path for item in outside_cleanup], ["/src/ED_EP45.mkv"])
        no_matching_main = dict(main, name="Show [44].mkv", full_path="/src/Show [44].mkv")
        self.assertEqual(
            [item.source_path for item in scraper._planned_cleanup_files([no_matching_main, ending])],
            [ending["full_path"]],
        )

    def test_tv_plan_never_reparses_contextual_theme_cleanup_as_episode_media(self):
        source = "/tv/Fullmetal Alchemist Brotherhood"
        primary = {
            "name": "[Ygm] Fullmetal Alchemist Brotherhood [45][2160p].mkv",
            "full_path": f"{source}/[Ygm] Fullmetal Alchemist Brotherhood [45][2160p].mkv",
        }
        ending = {
            "name": (
                "[Ygm] Fullmetal Alchemist Brotherhood "
                "[ED_EP45(O.A. Ver.)][2160p].mkv"
            ),
            "full_path": (
                f"{source}/SPs/[Ygm] Fullmetal Alchemist Brotherhood "
                "[ED_EP45(O.A. Ver.)][2160p].mkv"
            ),
        }
        tmdb = FakeTMDB(
            {
                "/tv/10": {
                    "name": "Fullmetal Alchemist Brotherhood",
                    "first_air_date": "2009-04-05",
                    "poster_path": None,
                    "seasons": [{"season_number": 1}],
                },
                "/tv/10/season/1": {
                    "episodes": [
                        {"episode_number": number, "name": f"Episode {number}"}
                        for number in range(1, 65)
                    ]
                },
                "/tv/10/season/0": {"episodes": []},
            }
        )

        plan = scraper.build_tv_plan(
            FakeAList([primary, ending]),
            tmdb,
            src_path=source,
            parent_path="/library",
            tmdb_id=10,
            season=1,
            absolute=False,
            prefer_simplified=True,
            allow_unmapped=False,
        )

        self.assertEqual([item.source_path for item in plan.files], [primary["full_path"]])
        self.assertEqual(
            [item.source_path for item in plan.cleanup_files],
            [ending["full_path"]],
        )
        self.assertEqual(
            scraper.cleanup_reason("Tokuten Theme Song MV [01].mkv"),
            "无字幕片头/片尾/光盘菜单视频",
        )
        self.assertEqual(
            scraper.cleanup_reason("Show [Game OP].mkv"),
            "无字幕片头/片尾/光盘菜单视频",
        )
        self.assertEqual(
            scraper.cleanup_reason("防失联永久链接.jpg"),
            "发布组广告图片",
        )
        self.assertEqual(
            scraper.cleanup_reason("海量4K高清资源文档合集.jpg"),
            "发布组广告图片",
        )
        self.assertEqual(
            scraper.cleanup_reason("番剧合集文档.png"),
            "发布组广告图片",
        )
        self.assertEqual(scraper.cleanup_reason("字体包.exe"), "字体资源包")
        self.assertEqual(scraper.cleanup_reason("Fonts.zip"), "字体资源包")
        self.assertEqual(scraper.cleanup_reason("字幕组字库.ttf"), "字体资源包")
        self.assertIsNone(scraper.cleanup_reason("Show.zh-CN.ass"))
        self.assertIsNone(scraper.cleanup_reason("Subtitles.zip"))
        self.assertIsNone(scraper.cleanup_reason("poster.jpg"))
        self.assertIsNone(scraper.cleanup_reason(".hidden.mkv"))
        self.assertIsNone(scraper.cleanup_reason("Show S01E01.mkv"))
        self.assertIsNone(scraper.cleanup_reason("NCOP notes.txt"))

    def test_search_query_variants_preserve_bounded_sacred_title_synonym(self):
        variants = scraper._search_query_variants("钢之炼金术师 叹息之丘的神圣之星")
        self.assertIn("钢之炼金术师 叹息之丘的神圣之星", variants)
        self.assertIn("钢之炼金术师 叹息之丘的圣星", variants)

    def test_internal_episode_override_rebases_reset_cour_numbering(self):
        groups = scraper.parse_ep_files([
            {
                "name": "Show S3 [01].mkv",
                "full_path": "/media/Show S3/Show S3 [01].mkv",
                "_episode_key_override": 25,
            }
        ])
        self.assertIn(scraper.EpisodeKey("regular", 25), groups)
        self.assertNotIn(scraper.EpisodeKey("regular", 1), groups)

    def test_internal_special_override_keeps_multi_part_ova_out_of_main_season(self):
        groups = scraper.parse_ep_files([
            {
                "name": "01.mp4",
                "full_path": "/media/通往大人的阶梯/01.mp4",
                "_episode_kind_override": "special",
                "_episode_key_override": 6,
            }
        ])
        self.assertIn(scraper.EpisodeKey("special", 6), groups)
        self.assertNotIn(scraper.EpisodeKey("regular", 1), groups)

    def test_appledouble_sidecar_is_not_treated_as_media(self):
        files = [
            {"name": "._Show.S01E01.mkv", "full_path": "/src/._Show.S01E01.mkv"},
            {"name": "Show.S01E01.mkv", "full_path": "/src/Show.S01E01.mkv"},
        ]
        self.assertEqual(
            [item["name"] for item in scraper._filter_media(files)],
            ["Show.S01E01.mkv"],
        )
        groups = scraper.parse_ep_files(files)
        self.assertEqual(
            [item["name"] for item in groups[scraper.EpisodeKey("regular", 1)]],
            ["Show.S01E01.mkv"],
        )

    def test_disguised_archive_inside_manga_shelf_is_not_media(self):
        files = [
            {"name": "官方精选集【修改后缀为zip】.mp4", "full_path": "/src/漫画/官方精选集【修改后缀为zip】.mp4"},
            {"name": "漫画家与助手 S01E01.mkv", "full_path": "/src/漫画家与助手 S01E01.mkv"},
        ]
        self.assertEqual(
            [item["name"] for item in scraper._filter_media(files)],
            ["漫画家与助手 S01E01.mkv"],
        )

    def test_query_keeps_balanced_year_and_extracts_tmdb_hint(self):
        source = "/quark/影视/番剧/86-不存在的战区- (2021) {tmdb-100565}"
        self.assertEqual(scraper._query_from_source(source), "86-不存在的战区- (2021)")
        self.assertEqual(scraper._tmdb_hint_from_source(source), 100565)

    def test_embedded_tmdb_id_resolves_directly_without_search(self):
        client = FakeTMDB(
            {
                "/tv/100565": {
                    "name": "86-不存在的战区-",
                    "first_air_date": "2021-04-11",
                }
            }
        )
        match = scraper._direct_tmdb_match(
            client,
            "/quark/影视/番剧/86-不存在的战区- (2021) {tmdb-100565}",
            100565,
        )
        self.assertEqual((match.media_type, match.tmdb_id), ("tv", 100565))

    def test_collection_hint_and_auto_match(self):
        self.assertTrue(scraper._source_suggests_collection("/movies/指环王三部曲"))
        self.assertFalse(scraper._source_suggests_collection("/movies/指环王 (2001)"))
        client = FakeTMDB({"/search/collection": {"results": [{"id": 119, "name": "指环王三部曲"}]}})
        best, candidates = scraper.auto_match_tmdb(
            client, "指环王三部曲", media_type="collection", min_confidence=0.88
        )
        self.assertEqual(best.media_type, "collection")
        self.assertEqual(best.tmdb_id, 119)
        self.assertEqual(len(candidates), 1)

    def test_empty_tmdb_search_retries_with_explicit_first_page(self):
        class EventuallyConsistentTMDB:
            def __init__(self):
                self.calls = []

            def get(self, path, **params):
                self.calls.append((path, dict(params)))
                if path == "/search/tv" and params.get("page") == 1:
                    return {"results": [{
                        "id": 30980,
                        "name": "魔法禁书目录",
                        "original_name": "とある魔術の禁書目録",
                        "first_air_date": "2008-10-05",
                        "genre_ids": [16],
                    }]}
                return {"results": []}

        client = EventuallyConsistentTMDB()
        best, _ = scraper.auto_match_tmdb(
            client, "魔法禁书目录", media_type="tv", min_confidence=0.88,
            prefer_animation=True,
        )
        self.assertEqual(best.tmdb_id, 30980)
        self.assertTrue(any(params.get("page") == 1 for _, params in client.calls))

    def test_auto_match_can_exclude_independently_confirmed_sibling_identity(self):
        client = FakeTMDB({
            "/search/tv": {"results": [
                {
                    "id": 1, "name": "Example", "first_air_date": "2020-01-01",
                    "genre_ids": [16],
                },
                {
                    "id": 2, "name": "Example", "first_air_date": "2020-01-01",
                    "genre_ids": [16],
                },
            ]},
            "/tv/1/alternative_titles": {"results": []},
            "/tv/2/alternative_titles": {"results": []},
        })
        with self.assertRaisesRegex(scraper.PlanError, "前两名证据无法区分"):
            scraper.auto_match_tmdb(
                client, "Example", media_type="tv", min_confidence=0.88,
            )

        best, candidates = scraper.auto_match_tmdb(
            client, "Example", media_type="tv", min_confidence=0.88,
            excluded_tmdb_ids={2},
        )

        self.assertEqual(best.tmdb_id, 1)
        self.assertEqual([item.tmdb_id for item in candidates], [1])

    def test_series_collection_phrase_selects_multi_work_batch_only(self):
        self.assertTrue(scraper._source_suggests_batch(
            "/quark/影视/待刮削/魔法少女小圆 系列合集 4K超清2160P收藏版"
        ))
        self.assertFalse(scraper._source_suggests_batch(
            "/quark/影视/待刮削/魔法禁书目录 S01-S03合集"
        ))
        self.assertFalse(scraper._source_suggests_collection(
            "/quark/影视/待刮削/魔法禁书目录 S01-S03合集"
        ))
        self.assertTrue(scraper._source_suggests_batch(
            "/quark/影视/待刮削/致你深爱的那个我&致我深爱的每个你"
        ))
        self.assertTrue(scraper._source_suggests_batch(
            "/quark/影视/待刮削/瑞克和MD 1-9季+日漫版 内封字幕"
        ))
        mixed = "/quark/影视/待刮削/青春猪头少年 TV+剧场版电影合集"
        self.assertTrue(scraper._source_suggests_batch(mixed))
        self.assertFalse(scraper._source_suggests_collection(mixed))

    def test_attached_movie_advertisement_does_not_taint_tv_episodes(self):
        root = "/media/魔法禁书目录 S01-S03合集 附两部剧场版"
        episode = {
            "name": "Show [01].mkv",
            "full_path": f"{root}/魔法禁书目录 I/Show [01].mkv",
        }
        movie = {
            "name": "Movie.mkv",
            "full_path": f"{root}/魔法禁书目录剧场版/Movie.mkv",
        }
        self.assertFalse(scraper._has_movie_context(episode))
        self.assertTrue(scraper._has_movie_context(movie))

    def test_plus_movie_count_advertisement_does_not_taint_tv_child(self):
        root = "/src/青春猪头少年系列.全系列+三部剧场版.简日双语.1080P"
        episode = {
            "name": "青春猪头少年不会梦到兔女郎学委01.mp4",
            "full_path": (
                f"{root}/01 青春猪头少年不会梦到兔女郎学姐/"
                "青春猪头少年不会梦到兔女郎学委01.mp4"
            ),
        }
        self.assertFalse(scraper._has_movie_context(episode))

    def test_contiguous_attached_title_suffix_proves_tv_episode_run(self):
        root = "/src/系列/01 青春猪头少年不会梦到兔女郎学姐"
        files = [
            {
                "name": f"青春猪头少年不会梦到兔女郎学姐{number:02d}.mp4",
                "full_path": (
                    f"{root}/青春猪头少年不会梦到兔女郎学姐{number:02d}.mp4"
                ),
            }
            for number in range(1, 14)
        ]
        overrides, count = scraper._franchise_title_suffix_episode_overrides(
            root, files
        )
        self.assertEqual(count, 13)
        self.assertEqual(sorted(overrides.values()), list(range(1, 14)))
        copied = scraper._apply_franchise_title_suffix_episode_overrides(root, files)
        self.assertEqual(
            [item["_episode_key_override"] for item in copied],
            list(range(1, 14)),
        )

    def test_official_season_title_and_complete_run_select_requested_season(self):
        client = FakeTMDB({
            "/tv/82739": {
                "seasons": [
                    {
                        "season_number": 1,
                        "name": "青春猪头少年不会梦到兔女郎学姐",
                        "episode_count": 13,
                    },
                    {
                        "season_number": 2,
                        "name": "青春猪头少年不会梦到圣诞服女郎",
                        "episode_count": 13,
                    },
                ]
            }
        })
        root = "/src/05 青春猪头少年不会梦到圣诞服女郎"
        files = [
            {
                "name": f"{number:02d}.mkv",
                "full_path": f"{root}/内封简繁.CR/{number:02d}.mkv",
            }
            for number in range(1, 14)
        ] + [
            {
                "name": f"{number:02d}.mp4",
                "full_path": f"{root}/日语繁中.Ani/{number:02d}.mp4",
            }
            for number in range(1, 14)
        ]
        self.assertEqual(
            scraper._franchise_member_official_season(client, 82739, root, files),
            2,
        )

    def test_proven_member_season_merges_complete_bare_quality_versions(self):
        root = "/src/05 青春猪头少年不会梦到圣诞服女郎"
        files = [
            {
                "name": f"{number:02d}.mkv",
                "full_path": f"{root}/内封简繁.CR/{number:02d}.mkv",
                "size": 100,
            }
            for number in range(1, 14)
        ] + [
            {
                "name": f"{number:02d}.mp4",
                "full_path": f"{root}/日语繁中.Ani/{number:02d}.mp4",
                "size": 90,
            }
            for number in range(1, 14)
        ]
        client = FakeTMDB({
            "/tv/82739": {
                "name": "青春猪头少年不会梦到兔女郎学姐",
                "seasons": [
                    {"season_number": 1, "name": "第 1 季", "episode_count": 13},
                    {
                        "season_number": 2,
                        "name": "青春猪头少年不会梦到圣诞服女郎",
                        "episode_count": 13,
                    },
                ],
            }
        })
        planned = scraper.Plan(
            mode="tv",
            source_root=root,
            target_root="/target/show",
            files=[],
            warnings=[],
            metadata={"title": "show", "season": 2},
        )
        with (
            mock.patch.object(scraper, "build_tv_plan", return_value=planned) as build,
            mock.patch.object(scraper, "validate_plan"),
        ):
            scraper.build_tv_plan_smart(
                auto_episode_mode=True,
                alist=FakeAList(files),
                tmdb_client=client,
                src_path=root,
                parent_path="/target",
                tmdb_id=82739,
                season=2,
                absolute=False,
                prefer_simplified=True,
                allow_unmapped=False,
                ignore_orphan_temp=False,
                episode_map_path=None,
                episode_group_id=None,
                source_files=files,
                _proven_member_season=True,
            )
        call = build.call_args.kwargs
        self.assertEqual(call["season"], 2)
        self.assertEqual(len(call["source_files"]), 26)
        self.assertTrue(
            all(
                1 <= scraper.extract_episode_key(item["name"]).number <= 13
                for item in call["source_files"]
            )
        )

    def test_repeated_franchise_prefix_single_insert_typo_gets_parent_fallback(self):
        root = (
            "/src/【日漫】青春猪头少年系列.全系列+三部剧场版/"
            "03 剧场版 青春期猪头少年不会梦到娇怜外出妹"
        )
        files = [{"name": "movie.mkv", "full_path": f"{root}/movie.mkv"}]
        queries = scraper._franchise_member_match_queries(root, files)
        normalized = {scraper._normalize_match_title(query) for query in queries}
        self.assertIn("青春猪头少年不会梦到娇怜外出妹", normalized)

    def test_shelf_prefix_x_is_not_season_ten(self):
        self.assertIsNone(scraper._season_from_source("/src/X 4k 夏目友人帐"))
        self.assertEqual(scraper._season_from_source("/src/Season X"), 10)

    def test_real_case_expanded_official_titles_keep_high_confidence(self):
        cases = [
            ("无职转生", "无职转生～到了异世界就拿出真本事～", 94664),
            ("恶魔高校", "恶魔高校D×D", 45950),
        ]
        for query, official, tmdb_id in cases:
            with self.subTest(query=query):
                client = FakeTMDB({
                    "/search/tv": {
                        "results": [{
                            "id": tmdb_id,
                            "name": official,
                            "first_air_date": "2021-01-01",
                            "genre_ids": [16],
                        }]
                    }
                })
                best, _ = scraper.auto_match_tmdb(
                    client, query, media_type="tv", min_confidence=0.88,
                    prefer_animation=True,
                )
                self.assertEqual(best.tmdb_id, tmdb_id)
                self.assertGreaterEqual(best.confidence, 0.96)

    def test_unique_cross_script_result_without_alias_evidence_is_rejected(self):
        client = FakeTMDB({
            "/search/movie": {
                "results": [{
                    "id": 331061,
                    "title": "约会大作战：万由里裁决",
                    "original_title": "劇場版デート・ア・ライブ 万由里ジャッジメント",
                    "release_date": "2015-08-22",
                }]
            }
        })
        with self.assertRaisesRegex(scraper.PlanError, "缺少可验证的标题/别名证据"):
            scraper.auto_match_tmdb(
                client, "Date A Live Mayuri Judgement", media_type="movie",
                min_confidence=0.88,
            )

    def test_exact_title_with_wrong_year_requires_review(self):
        client = FakeTMDB({
            "/search/tv": {"results": [{
                "id": 10,
                "name": "同名作品",
                "first_air_date": "2018-01-01",
            }]},
            "/search/movie": {"results": []},
        })
        with self.assertRaisesRegex(scraper.PlanError, "年份与源目录冲突"):
            scraper.auto_match_tmdb(
                client, "同名作品 (2024)", media_type="tv", min_confidence=0.88,
            )

    def test_animation_shelf_falls_back_to_movie_only_when_tv_has_no_candidate(self):
        client = FakeTMDB({
            "/search/tv": {"results": []},
            "/search/movie": {
                "results": [{
                    "id": 360814,
                    "title": "我想吃掉你的胰脏",
                    "release_date": "2018-09-01",
                }]
            },
        })
        best, candidates = scraper.auto_match_tmdb(
            client,
            "我想吃掉你的胰脏",
            media_type="tv",
            min_confidence=0.88,
            prefer_animation=True,
        )
        self.assertEqual((best.media_type, best.tmdb_id), ("movie", 360814))
        self.assertEqual(len(candidates), 1)

    def test_real_case_movie_search_retries_without_chinese_punctuation(self):
        class QueryAwareTMDB:
            def __init__(self):
                self.queries = []

            def get(self, path, **params):
                self.queries.append(params.get("query"))
                if params.get("query") == "来自深渊 出发的黎明":
                    return {"results": [{
                        "id": 299536,
                        "title": "来自深渊：出发的黎明",
                        "release_date": "2019-01-04",
                    }]}
                return {"results": []}

        client = QueryAwareTMDB()
        best, _ = scraper.auto_match_tmdb(
            client, "来自深渊：出发的黎明", media_type="movie",
            min_confidence=0.88,
        )
        self.assertEqual(best.tmdb_id, 299536)
        self.assertEqual(client.queries[:2], ["来自深渊：出发的黎明", "来自深渊 出发的黎明"])

    def test_search_retries_relaxed_title_even_when_first_query_has_wrong_results(self):
        class QueryAwareTMDB:
            def __init__(self):
                self.queries = []

            def get(self, path, **params):
                query = params.get("query")
                self.queries.append(query)
                if query == "来自深渊 出发的黎明":
                    return {"results": [{
                        "id": 299536,
                        "title": "来自深渊：出发的黎明",
                        "release_date": "2019-01-04",
                    }]}
                return {"results": [{
                    "id": 999,
                    "title": "来自深渊",
                    "release_date": "2017-07-07",
                }]}

        client = QueryAwareTMDB()
        best, _ = scraper.auto_match_tmdb(
            client, "来自深渊：出发的黎明", media_type="movie",
            min_confidence=0.88,
        )
        self.assertEqual(best.tmdb_id, 299536)
        self.assertEqual(client.queries[:2], ["来自深渊：出发的黎明", "来自深渊 出发的黎明"])

    def test_bounded_synonym_variant_is_used_as_candidate_scoring_evidence(self):
        class QueryAwareTMDB:
            def get(self, path, **params):
                if params.get("query") == "魔法少女小圆 叛逆的故事":
                    return {"results": [{
                        "id": 212167,
                        "title": "魔法少女小圆 剧场版 叛逆的故事",
                        "release_date": "2013-10-26",
                        "genre_ids": [16],
                    }]}
                return {"results": []}

        best, _ = scraper.auto_match_tmdb(
            QueryAwareTMDB(),
            "魔法少女小圆 叛逆的物语 (2013.10)",
            media_type="movie",
            min_confidence=0.88,
            prefer_animation=True,
        )
        self.assertEqual(best.tmdb_id, 212167)
        self.assertEqual(
            best.decision_trace["matched_query_variant"],
            "魔法少女小圆 叛逆的故事",
        )
        self.assertGreaterEqual(best.confidence, 0.88)

    def test_ambiguous_search_uses_tmdb_alternative_titles(self):
        client = FakeTMDB({
            "/search/tv": {"results": [{
                "id": 127532,
                "name": "Solo Leveling",
                "original_name": "俺だけレベルアップな件",
                "first_air_date": "2024-01-07",
            }]},
            "/tv/127532/alternative_titles": {"results": [
                {"iso_3166_1": "CN", "title": "我独自升级", "type": ""},
            ]},
        })
        best, _ = scraper.auto_match_tmdb(
            client, "我独自升级", media_type="tv", min_confidence=0.88,
            prefer_animation=True,
        )
        self.assertEqual(best.tmdb_id, 127532)
        self.assertEqual(best.confidence, 1.0)

    def test_tmdb_client_uses_trusted_https_endpoint_configuration(self):
        with mock.patch.dict(os.environ, {
            "TMDB_BASE_URL": "https://tmdb-proxy.example/api/3",
            "TMDB_IMAGE_BASE_URL": "https://tmdb-images.example/original",
        }):
            client = scraper.TMDBClient("test-key")
        self.assertEqual(client.base_url, "https://tmdb-proxy.example/api/3")
        self.assertEqual(client.image_base_url, "https://tmdb-images.example/original")

        with mock.patch.dict(os.environ, {"TMDB_BASE_URL": "http://unsafe.example/3"}):
            with self.assertRaisesRegex(scraper.ScraperError, "可信的 HTTPS"):
                scraper.TMDBClient("test-key")

    def test_tmdb_client_scopes_a_credential_free_http_proxy(self):
        client = scraper.TMDBClient(
            "test-key", proxy_url="http://proxy.example:7897",
        )
        self.assertEqual(client.proxy_url, "http://proxy.example:7897")
        self.assertEqual(client.http.proxy_url, "http://proxy.example:7897")

        for invalid in (
            "https://proxy.example:7897",
            "http://user:secret@proxy.example:7897",
            "http://proxy.example:7897/tunnel",
        ):
            with self.subTest(proxy=invalid):
                with self.assertRaisesRegex(scraper.ScraperError, "TMDB_PROXY_URL"):
                    scraper.TMDBClient("test-key", proxy_url=invalid)

    def test_tmdb_client_reports_network_and_auth_failures_separately(self):
        client = scraper.TMDBClient("test-key")
        client.http = mock.Mock()
        client.http.request_json.side_effect = scraper.ApiError(
            "网络请求失败: getaddrinfo failed"
        )
        with self.assertRaisesRegex(scraper.ApiError, "域名解析失败"):
            client.get("/search/tv", query="测试")

        client.http.request_json.side_effect = scraper.ApiError(
            "HTTP 401", status_code=401
        )
        with self.assertRaisesRegex(scraper.ApiError, "API Key 无效"):
            client.get("/search/tv", query="测试")

        client.http.request_json.side_effect = scraper.ApiError(
            "SSL: UNEXPECTED_EOF_WHILE_READING"
        )
        with self.assertRaisesRegex(scraper.ApiError, "TLS 连接被中途断开"):
            client.get("/search/tv", query="测试")

        client.http.request_json.side_effect = scraper.ApiError(
            "SSL certificate verify failed"
        )
        with self.assertRaisesRegex(scraper.ApiError, "证书校验失败"):
            client.get("/search/tv", query="测试")

    def test_media_type_is_inferred_from_nearest_strong_parent_hint(self):
        cases = {
            "/quark/影视/番剧/虫师": "tv",
            "/media/TV Shows/Frieren": "tv",
            "/quark/影视/电影/虫师": "movie",
            "/library/movies/anime/Title": "tv",
            "/quark/影视/待整理/虫师": None,
            "/quark/影视/动漫电影/Title": None,
        }
        for source, expected in cases.items():
            with self.subTest(source=source):
                self.assertEqual(scraper._media_type_from_source_context(source), expected)

    def test_selected_target_category_supplies_missing_source_context(self):
        requested_type, prefer_animation = scraper._media_context_from_source_and_target(
            "/quark/影视/待刮削/H 4k 好想告诉你",
            "/quark/影视/番剧",
        )
        self.assertEqual(requested_type, "tv")
        self.assertTrue(prefer_animation)

        movie_type, movie_animation = scraper._media_context_from_source_and_target(
            "/quark/影视/待刮削/虫师",
            "/quark/影视/电影",
        )
        self.assertEqual(movie_type, "movie")
        self.assertFalse(movie_animation)

    def test_source_context_remains_stronger_than_selected_target(self):
        requested_type, prefer_animation = scraper._media_context_from_source_and_target(
            "/library/movies/Title",
            "/quark/影视/番剧",
        )
        self.assertEqual(requested_type, "movie")
        self.assertTrue(prefer_animation)

    def test_animation_library_prefers_animated_same_title_candidate(self):
        client = FakeTMDB(
            {
                "/search/tv": {
                    "results": [
                        {
                            "id": 209648,
                            "name": "好想告诉你",
                            "first_air_date": "2023-03-30",
                            "genre_ids": [18],
                        },
                        {
                            "id": 68854,
                            "name": "好想告诉你",
                            "first_air_date": "2009-10-07",
                            "genre_ids": [16, 18],
                        },
                    ]
                }
            }
        )
        best, _ = scraper.auto_match_tmdb(
            client,
            "好想告诉你",
            media_type="tv",
            min_confidence=0.88,
            prefer_animation=True,
        )
        self.assertTrue(scraper._source_is_animation_library("/quark/影视/番剧/H 4k 好想告诉你"))
        self.assertEqual(best.tmdb_id, 68854)

    def test_animation_preference_survives_alternative_title_enrichment(self):
        client = FakeTMDB(
            {
                "/search/tv": {
                    "results": [
                        {
                            "id": 203737,
                            "name": "【我推的孩子】",
                            "original_name": "【推しの子】",
                            "first_air_date": "2023-04-12",
                            "genre_ids": [16, 18],
                        },
                        {
                            "id": 244377,
                            "name": "【我推的孩子】",
                            "original_name": "【推しの子】",
                            "first_air_date": "2024-11-28",
                            "genre_ids": [18],
                        },
                    ]
                },
                "/tv/203737/alternative_titles": {"results": []},
                "/tv/244377/alternative_titles": {
                    "results": [{"title": "【我推的孩子】"}]
                },
            }
        )
        best, candidates = scraper.auto_match_tmdb(
            client,
            "我推的孩子",
            media_type="tv",
            min_confidence=0.88,
            prefer_animation=True,
        )
        self.assertEqual(best.tmdb_id, 203737)
        self.assertGreaterEqual(best.confidence - candidates[1].confidence, 0.03)

    def test_animation_movie_beats_same_title_live_action_movie(self):
        client = FakeTMDB(
            {
                "/search/movie": {
                    "results": [
                        {
                            "id": 449132,
                            "title": "我想吃掉你的胰脏",
                            "release_date": "2017-07-28",
                            "genre_ids": [18, 10749],
                        },
                        {
                            "id": 504253,
                            "title": "我想吃掉你的胰脏",
                            "release_date": "2018-09-01",
                            "genre_ids": [16, 18, 10749],
                        },
                    ]
                },
                "/movie/449132/alternative_titles": {"titles": []},
                "/movie/504253/alternative_titles": {"titles": []},
            }
        )
        best, candidates = scraper.auto_match_tmdb(
            client,
            "我想吃掉你的胰脏",
            media_type="movie",
            min_confidence=0.88,
            prefer_animation=True,
        )
        self.assertEqual(best.tmdb_id, 504253)
        self.assertGreaterEqual(best.confidence - candidates[1].confidence, 0.03)

    def test_same_title_tv_uses_exact_source_episode_count_to_break_tie(self):
        client = FakeTMDB({
            "/search/tv": {"results": [
                {
                    "id": 70072,
                    "name": "白色相簿2",
                    "original_name": "WHITE ALBUM 2",
                    "genre_ids": [16],
                },
                {"id": 312322, "name": "White Album 2", "genre_ids": [16]},
            ]},
            "/tv/70072/alternative_titles": {"results": [{"title": "White Album 2"}]},
            "/tv/312322/alternative_titles": {"results": [{"title": "White Album 2"}]},
            "/tv/70072": {"number_of_episodes": 13},
            "/tv/312322": {"number_of_episodes": 8},
        })
        best, _ = scraper.auto_match_tmdb(
            client,
            "White Album 2",
            media_type="tv",
            min_confidence=0.88,
            prefer_animation=True,
            expected_episode_count=13,
        )
        self.assertEqual(best.tmdb_id, 70072)

    def test_exact_series_title_beats_animated_franchise_expansion(self):
        client = FakeTMDB({
            "/search/tv": {
                "results": [
                    {"id": 60654, "name": "魔笛MAGI", "genre_ids": [16]},
                    {"id": 66870, "name": "魔笛MAGI 辛巴达的冒险", "genre_ids": [16]},
                ]
            }
        })
        best, candidates = scraper.auto_match_tmdb(
            client, "魔笛MAGI", media_type="tv", min_confidence=0.88,
            prefer_animation=True,
        )
        self.assertEqual(best.tmdb_id, 60654)
        self.assertGreater(best.confidence - candidates[1].confidence, 0.03)

    def test_smart_episode_mode_retries_only_unmapped_plan_as_absolute(self):
        plan = scraper.Plan("tv", "/src", "/dst", [], [], {})
        error = scraper.PlanError("以下集数未在 TMDB 映射中找到，已停止以避免错误归档: E25。")
        with mock.patch.object(scraper, "build_tv_plan", side_effect=[error, plan]) as builder:
            result = scraper.build_tv_plan_smart(
                auto_episode_mode=True,
                alist=FakeAList([]),
                tmdb_client=object(),
                src_path="/src",
                parent_path="/dst",
                tmdb_id=1,
                season=1,
                absolute=False,
                prefer_simplified=True,
                allow_unmapped=False,
                episode_map_path=None,
                episode_group_id=None,
            )
        self.assertIs(result, plan)
        self.assertFalse(builder.call_args_list[0].kwargs["absolute"])
        self.assertTrue(builder.call_args_list[1].kwargs["absolute"])
        self.assertIn("自动改用", plan.warnings[0])

    def test_complete_reset_absolute_blocks_partition_all_official_seasons(self):
        groups = {}
        for source_season, count in enumerate((201, 64, 51, 51), 1):
            groups[source_season] = [
                {
                    "name": f"Show {number:03d}.mkv",
                    "full_path": f"/src/第{source_season}季/Show {number:03d}.mkv",
                }
                for number in range(1, count + 1)
            ]
        official_counts = (49, 50, 51, 51, 51, 13, 51, 12, 13, 12, 14)
        official = [
            {"season_number": number, "episode_count": count}
            for number, count in enumerate(official_counts, 1)
        ]
        remapped, warning = scraper._remap_complete_reset_absolute_season_groups(
            groups, official
        )
        self.assertEqual(sorted(remapped), list(range(1, 12)))
        self.assertEqual(
            [
                len({item["_episode_key_override"] for item in remapped[number]})
                for number in sorted(remapped)
            ],
            list(official_counts),
        )
        self.assertIn("跨季 absolute", warning)

    def test_reset_absolute_partition_rejects_one_missing_source_episode(self):
        groups = {}
        for source_season, count in enumerate((201, 64, 51, 51), 1):
            groups[source_season] = [
                {
                    "name": f"Show {number:03d}.mkv",
                    "full_path": f"/src/第{source_season}季/Show {number:03d}.mkv",
                }
                for number in range(1, count + 1)
                if not (source_season == 1 and number == 100)
            ]
        official = [
            {"season_number": number, "episode_count": count}
            for number, count in enumerate(
                (49, 50, 51, 51, 51, 13, 51, 12, 13, 12, 14),
                1,
            )
        ]

        remapped, warning = scraper._remap_complete_reset_absolute_season_groups(
            groups, official
        )

        self.assertIsNone(warning)
        self.assertEqual(set(remapped), {1, 2, 3, 4})
        self.assertFalse(any(
            "_episode_key_override" in item
            for members in remapped.values()
            for item in members
        ))

    def test_absolute_episode_map_supports_episode_one_hundred_and_beyond(self):
        show = {
            "seasons": [
                {"season_number": 1},
                {"season_number": 2},
                {"season_number": 3},
            ]
        }
        tmdb = FakeTMDB({
            "/tv/1/season/1": {"episodes": [
                {"episode_number": number, "name": f"One {number}"}
                for number in range(1, 50)
            ]},
            "/tv/1/season/2": {"episodes": [
                {"episode_number": number, "name": f"Two {number}"}
                for number in range(1, 51)
            ]},
            "/tv/1/season/3": {"episodes": [
                {"episode_number": number, "name": f"Three {number}"}
                for number in range(1, 4)
            ]},
            "/tv/1/season/0": {"episodes": []},
        })

        mapping = scraper._build_tv_episode_map(
            tmdb, show, 1, 1, True
        )

        self.assertEqual(
            mapping[scraper.EpisodeKey("regular", 100)],
            (3, 1, "Three 1"),
        )
        self.assertEqual(
            mapping[scraper.EpisodeKey("regular", 102)],
            (3, 3, "Three 3"),
        )

    def test_cumulative_season_numbers_use_official_boundaries_but_leave_decimals(self):
        files = [
            {
                "name": "Show [03].mkv",
                "full_path": "/src/第二季/Show [03].mkv",
            },
            {
                "name": "Show [04].mkv",
                "full_path": "/src/第二季/Show [04].mkv",
            },
            {
                "name": "Show [03].ass",
                "full_path": "/src/第二季/字幕/Show [03].ass",
            },
            {
                "name": "Show [2.5].mkv",
                "full_path": "/src/第二季/Show [2.5].mkv",
            },
        ]
        normalized, warning = scraper._normalize_cumulative_season_episode_numbers(
            2,
            files,
            [
                {"season_number": 1, "episode_count": 2},
                {"season_number": 2, "episode_count": 3},
            ],
        )
        by_name = {item["name"]: item for item in normalized}
        self.assertEqual(by_name["Show [03].mkv"]["_episode_key_override"], 1)
        self.assertEqual(by_name["Show [04].mkv"]["_episode_key_override"], 2)
        self.assertEqual(by_name["Show [03].ass"]["_episode_key_override"], 1)
        self.assertNotIn("_episode_key_override", by_name["Show [2.5].mkv"])
        self.assertIn("小数集号未参与换算", warning)

    def test_cumulative_season_numbers_do_not_guess_from_an_incomplete_middle_range(self):
        files = [
            {
                "name": "Show [04].mkv",
                "full_path": "/src/第二季/Show [04].mkv",
            }
        ]
        normalized, warning = scraper._normalize_cumulative_season_episode_numbers(
            2,
            files,
            [
                {"season_number": 1, "episode_count": 2},
                {"season_number": 2, "episode_count": 3},
            ],
        )
        self.assertNotIn("_episode_key_override", normalized[0])
        self.assertIsNone(warning)

    def test_complete_sequel_local_run_uses_current_season_boundary(self):
        files = [
            {
                "name": f"Tokyo Ghoul re II - {number:02d}.mkv",
                "full_path": f"/src/S4/Tokyo Ghoul re II - {number:02d}.mkv",
            }
            for number in range(13, 25)
        ]
        normalized, warning = scraper._normalize_cumulative_season_episode_numbers(
            4,
            files,
            [
                {"season_number": 1, "episode_count": 12},
                {"season_number": 2, "episode_count": 12},
                {"season_number": 3, "episode_count": 12},
                {"season_number": 4, "episode_count": 12},
            ],
        )
        self.assertEqual(normalized[0]["_episode_key_override"], 1)
        self.assertEqual(normalized[-1]["_episode_key_override"], 12)
        self.assertIn("续作内累计编号", warning)

    def test_cumulative_and_relative_backup_numbers_share_episode_keys(self):
        files = [
            *[
                {
                    "name": f"Show [{number:02d}].2160p.mkv",
                    "full_path": f"/src/第二季/2160p/Show [{number:02d}].2160p.mkv",
                }
                for number in range(25, 49)
            ],
            *[
                {
                    "name": f"Show [{number:02d}].1080p.mkv",
                    "full_path": f"/src/第二季/1080p-cumulative/Show [{number:02d}].1080p.mkv",
                }
                for number in range(25, 49)
            ],
            *[
                {
                    "name": f"Show [{number:02d}].1080p.mkv",
                    "full_path": f"/src/第二季/1080p-relative/Show [{number:02d}].1080p.mkv",
                }
                for number in range(1, 25)
            ],
            {
                "name": "Show [24.9].2160p.mkv",
                "full_path": "/src/第二季/Show [24.9].2160p.mkv",
            },
        ]
        normalized, warning = scraper._normalize_cumulative_season_episode_numbers(
            2,
            files,
            [
                {"season_number": 1, "episode_count": 24},
                {"season_number": 2, "episode_count": 24},
            ],
        )
        by_path = {item["full_path"]: item for item in normalized}
        self.assertEqual(
            by_path["/src/第二季/2160p/Show [25].2160p.mkv"]["_episode_key_override"],
            1,
        )
        self.assertEqual(
            by_path[
                "/src/第二季/1080p-cumulative/Show [48].1080p.mkv"
            ]["_episode_key_override"],
            24,
        )
        self.assertEqual(
            by_path[
                "/src/第二季/1080p-relative/Show [01].1080p.mkv"
            ]["_episode_key_override"],
            1,
        )
        self.assertNotIn(
            "_episode_key_override",
            by_path["/src/第二季/Show [24.9].2160p.mkv"],
        )
        self.assertIn("低清晰度备份", warning)

    def test_smart_mode_maps_whole_series_counters_per_official_season(self):
        files = [
            {"name": "Show [01].mkv", "full_path": "/src/第一季/Show [01].mkv"},
            {"name": "Show [02].mkv", "full_path": "/src/第一季/Show [02].mkv"},
            {"name": "Show [03].mkv", "full_path": "/src/第二季/Show [03].mkv"},
            {"name": "Show [04].mkv", "full_path": "/src/第二季/Show [04].mkv"},
        ]
        tmdb = FakeTMDB({
            "/tv/1": {
                "name": "Show",
                "first_air_date": "2020-01-01",
                "poster_path": None,
                "backdrop_path": None,
                "seasons": [
                    {"season_number": 1, "episode_count": 2},
                    {"season_number": 2, "episode_count": 2},
                ],
            },
            "/tv/1/season/1": {"episodes": [
                {"episode_number": 1, "name": "One"},
                {"episode_number": 2, "name": "Two"},
            ]},
            "/tv/1/season/2": {"episodes": [
                {"episode_number": 1, "name": "Three"},
                {"episode_number": 2, "name": "Four"},
            ]},
            "/tv/1/season/0": {"episodes": []},
        })
        plan = scraper.build_tv_plan_smart(
            auto_episode_mode=True,
            alist=FakeAList(files),
            tmdb_client=tmdb,
            src_path="/src",
            parent_path="/library",
            tmdb_id=1,
            season=1,
            absolute=False,
            prefer_simplified=True,
            allow_unmapped=False,
            episode_map_path=None,
            episode_group_id=None,
        )
        source_to_name = {
            item.source_path: item.final_name
            for item in plan.files
        }
        self.assertIn("S02E01", source_to_name["/src/第二季/Show [03].mkv"])
        self.assertIn("S02E02", source_to_name["/src/第二季/Show [04].mkv"])
        self.assertTrue(any("全剧累计编号" in item for item in plan.warnings))

    def test_smart_mode_attaches_complete_bare_backup_subtitle_track_uniquely(self):
        files = [
            *[
                {
                    "name": f"Show [{number:02d}].mkv",
                    "full_path": f"/src/第一季/Show [{number:02d}].mkv",
                }
                for number in range(1, 4)
            ],
            *[
                {
                    "name": f"Show 2nd Season [{number:02d}].mkv",
                    "full_path": f"/src/第二季/Show 2nd Season [{number:02d}].mkv",
                }
                for number in range(1, 3)
            ],
            *[
                {
                    "name": f"Show [{number:02d}].ass",
                    "full_path": f"/src/备份字幕/Show [{number:02d}].ass",
                }
                for number in range(1, 4)
            ],
        ]
        tmdb = FakeTMDB({
            "/tv/1": {
                "name": "Show",
                "first_air_date": "2020-01-01",
                "poster_path": None,
                "backdrop_path": None,
                "seasons": [
                    {"season_number": 1, "episode_count": 3},
                    {"season_number": 2, "episode_count": 2},
                ],
            },
            "/tv/1/season/1": {"episodes": [
                {"episode_number": number, "name": f"One {number}"}
                for number in range(1, 4)
            ]},
            "/tv/1/season/2": {"episodes": [
                {"episode_number": number, "name": f"Two {number}"}
                for number in range(1, 3)
            ]},
            "/tv/1/season/0": {"episodes": []},
        })
        plan = scraper.build_tv_plan_smart(
            auto_episode_mode=True,
            alist=FakeAList(files),
            tmdb_client=tmdb,
            src_path="/src",
            parent_path="/library",
            tmdb_id=1,
            season=1,
            absolute=False,
            prefer_simplified=True,
            allow_unmapped=False,
            episode_map_path=None,
            episode_group_id=None,
        )
        subtitle_targets = {
            item.source_path: item.final_name
            for item in plan.files
            if item.source_path.endswith(".ass")
        }
        self.assertEqual(len(subtitle_targets), 3)
        self.assertTrue(all("S01E" in name for name in subtitle_targets.values()))
        self.assertTrue(any("完整备份字幕轨" in warning for warning in plan.warnings))

    def test_smart_mode_splits_seasons_and_title_matches_official_specials(self):
        files = [
            {"name": "虫师.S01E01.mkv", "full_path": "/src/Season 1/虫师.S01E01.mkv"},
            {"name": "虫师.S02E01.mkv", "full_path": "/src/Season 2/虫师.S02E01.mkv"},
            {
                "name": "Mushishi Hihamukage [Ma10p_2160p][x265_flac_ass].mkv",
                "full_path": "/src/特别篇 蚀日之翳/Mushishi Hihamukage [Ma10p_2160p][x265_flac_ass].mkv",
                "size": 3_100_000_000,
            },
            {
                "name": "Mushishi Zoku Shou Suzu no Shizuku [Ma10p_2160p][x265_flac_ass].mkv",
                "full_path": "/src/特别篇 铃之雫/Mushishi Zoku Shou Suzu no Shizuku [Ma10p_2160p][x265_flac_ass].mkv",
                "size": 3_600_000_000,
            },
        ]
        tmdb = FakeTMDB(
            {
                "/tv/26867": {
                    "name": "虫师",
                    "first_air_date": "2005-10-23",
                    "poster_path": None,
                    "backdrop_path": None,
                    "seasons": [{"season_number": 1}, {"season_number": 2}],
                },
                "/tv/26867/season/1": {
                    "episodes": [{"episode_number": 1, "name": "绿色座"}]
                },
                "/tv/26867/season/2": {
                    "episodes": [{"episode_number": 1, "name": "野末之宴"}]
                },
                "/tv/26867/season/0": {
                    "episodes": [
                        {"episode_number": 2, "name": "虫师 蚀日之翳"},
                    ]
                },
                "/search/movie": {"results": [{
                    "id": 312966,
                    "title": "虫师 续章 铃之雫",
                    "original_title": "蟲師 続章 鈴の雫",
                    "release_date": "2015-05-16",
                    "genre_ids": [16],
                }]},
                "/movie/312966": {
                    "id": 312966,
                    "title": "虫师 续章 铃之雫",
                    "original_title": "蟲師 続章 鈴の雫",
                    "release_date": "2015-05-16",
                    "poster_path": None,
                    "backdrop_path": None,
                },
                "/movie/312966/alternative_titles": {"titles": [
                    {"title": "Mushishi Zoku Shou: Suzu no Shizuku"},
                    {"title": "Mushishi: The Next Chapter - Drops of Bells"},
                ]},
            }
        )
        plan = scraper.build_tv_plan_smart(
            auto_episode_mode=True,
            alist=FakeAList(files),
            tmdb_client=tmdb,
            src_path="/src",
            parent_path="/library",
            tmdb_id=26867,
            season=1,
            absolute=False,
            prefer_simplified=True,
            allow_unmapped=False,
            episode_map_path=None,
            episode_group_id=None,
        )
        final_names = {item.final_name for item in plan.files}
        targets_by_source = {
            item.source_path: item.final_name for item in plan.files
        }
        self.assertTrue(any("S01E01" in name for name in final_names))
        self.assertTrue(any("S02E01" in name for name in final_names))
        self.assertTrue(any("S00E02" in name for name in final_names))
        self.assertIn("S00E02", targets_by_source[files[2]["full_path"]])
        self.assertEqual(
            targets_by_source[files[3]["full_path"]],
            "虫师 续章 铃之雫 (2015).mkv",
        )
        suzu_plan_file = next(
            item for item in plan.files if item.source_path == files[3]["full_path"]
        )
        self.assertEqual(suzu_plan_file.target_dir, "/library/虫师 续章 铃之雫 (2015)")
        self.assertFalse(plan.cleanup_files)
        self.assertTrue(any("2 个季度" in warning for warning in plan.warnings))

    def test_shirobako_credit_editions_survive_and_orphan_ova_subtitle_stays_put(self):
        files = [
            {
                "name": "Shirobako [01].mkv",
                "full_path": "/src/Season 1/Shirobako [01].mkv",
                "size": 1_000_000,
            },
        ]
        for edition, size in (
            ("Musani Staff Credit", 500_000),
            ("Original Staff Credit", 480_000),
        ):
            files.extend([
                {
                    "name": f"Exodus! [01({edition} Ver.)][Ma10p_1080p].mkv",
                    "full_path": f"/src/OVAs/Exodus!/Exodus! [01({edition} Ver.)][Ma10p_1080p].mkv",
                    "size": size,
                },
                {
                    "name": f"Exodus! [01({edition} Ver.)].ass",
                    "full_path": f"/src/OVAs/Exodus!/subs/Exodus! [01({edition} Ver.)].ass",
                    "size": 10_000,
                },
            ])
        orphan_subtitle = {
            "name": "Daisan Hikou Shoujotai [01(Musani Staff Credit Ver.)].ass",
            "full_path": (
                "/src/备份字幕/[VCB-Studio] Daisan Hikou Shoujotai "
                "[Ma10p_1080p]/Daisan Hikou Shoujotai "
                "[01(Musani Staff Credit Ver.)].ass"
            ),
            "size": 10_000,
        }
        files.append(orphan_subtitle)
        tmdb = FakeTMDB({
            "/tv/1": {
                "name": "SHIROBAKO",
                "first_air_date": "2014-10-09",
                "poster_path": None,
                "backdrop_path": None,
                "seasons": [{"season_number": 1, "episode_count": 1}],
            },
            "/tv/1/season/1": {
                "episodes": [{"episode_number": 1, "name": "Exodus Christmas"}]
            },
            "/tv/1/season/0": {"episodes": [
                {"episode_number": 1, "name": "Exodus!"},
                {"episode_number": 2, "name": "The Third Aerial Girls"},
            ]},
        })
        plan = scraper.build_tv_plan_smart(
            auto_episode_mode=True,
            alist=FakeAList(files),
            tmdb_client=tmdb,
            src_path="/src",
            parent_path="/library",
            tmdb_id=1,
            season=1,
            absolute=False,
            prefer_simplified=True,
            allow_unmapped=False,
            episode_map_path=None,
            episode_group_id=None,
        )
        exodus_targets = [
            item.final_name for item in plan.files if "/OVAs/Exodus!/" in item.source_path
        ]
        self.assertEqual(len(exodus_targets), 4)
        self.assertTrue(all("S00E01" in name for name in exodus_targets))
        self.assertTrue(any("{edition-Musani Staff Credit}" in name for name in exodus_targets))
        self.assertTrue(any("{edition-Original Staff Credit}" in name for name in exodus_targets))
        self.assertFalse(any(
            cleanup.source_path in {item["full_path"] for item in files[1:5]}
            for cleanup in plan.cleanup_files
        ))
        self.assertNotIn(
            orphan_subtitle["full_path"],
            {item.source_path for item in plan.files},
        )
        self.assertIn(
            orphan_subtitle["full_path"],
            {row["source_path"] for row in plan.scan_report["deferred_subtitles"]},
        )

    def test_shirobako_in_universe_prefix_maps_third_aerial_girls_special(self):
        files = [
            {"name": "SHIROBAKO [01].mkv", "full_path": "/src/SHIROBAKO [01].mkv"},
            {
                "name": "Daisan Hikou Shoujotai [01(Musani Staff Credit Ver.)].mkv",
                "full_path": (
                    "/src/剧中剧 Daisan Hikou Shoujotai/"
                    "Daisan Hikou Shoujotai [01(Musani Staff Credit Ver.)].mkv"
                ),
            },
        ]
        tmdb = FakeTMDB({
            "/tv/1": {
                "name": "SHIROBAKO", "first_air_date": "2014-10-09",
                "poster_path": None, "backdrop_path": None,
                "seasons": [{"season_number": 1, "episode_count": 1}],
            },
            "/tv/1/season/1": {
                "episodes": [{"episode_number": 1, "name": "Exodus Christmas"}]
            },
            "/tv/1/season/0": {"episodes": [
                {"episode_number": 1, "name": "Exodus!"},
                {"episode_number": 2, "name": "第三飞行少女队"},
            ]},
        })
        plan = scraper.build_tv_plan_smart(
            auto_episode_mode=True, alist=FakeAList(files), tmdb_client=tmdb,
            src_path="/src", parent_path="/library", tmdb_id=1,
            season=1, absolute=False, prefer_simplified=True,
            allow_unmapped=False, episode_map_path=None, episode_group_id=None,
        )
        special = next(item for item in plan.files if "Daisan" in item.source_path)
        self.assertIn("S00E02", special.final_name)
        self.assertIn("Musani Staff Credit", special.final_name)

    def test_shirobako_third_aerial_editions_attach_subtitles_and_keep_making_problems(self):
        files = [
            {"name": "SHIROBAKO [01].mkv", "full_path": "/src/SHIROBAKO [01].mkv"},
        ]
        for edition in ("Musani Staff Credit", "Original Staff Credit"):
            files.extend([
                {
                    "name": f"Daisan Hikou Shoujotai [01({edition} Ver.)][Ma10p_2160p].mkv",
                    "full_path": (
                        "/src/剧中剧 Daisan Hikou Shoujotai/"
                        f"Daisan Hikou Shoujotai [01({edition} Ver.)][Ma10p_2160p].mkv"
                    ),
                    "size": 500_000,
                },
                {
                    "name": f"Daisan Hikou Shoujotai [01({edition} Ver.)].ass",
                    "full_path": (
                        "/src/备份字幕/[VCB-Studio] Daisan Hikou Shoujotai "
                        f"[Ma10p_1080p]/Daisan Hikou Shoujotai [01({edition} Ver.)].ass"
                    ),
                    "size": 10_000,
                },
            ])
        making_paths = []
        for number in range(1, 6):
            path = f"/src/备份字幕/SPs/SHIROBAKO [Making{number:02d}].sc.ass"
            making_paths.append(path)
            files.append({
                "name": f"SHIROBAKO [Making{number:02d}].sc.ass",
                "full_path": path,
                "size": 5_000,
            })
        tmdb = FakeTMDB({
            "/tv/1": {
                "name": "SHIROBAKO", "first_air_date": "2014-10-09",
                "poster_path": None, "backdrop_path": None,
                "seasons": [{"season_number": 1, "episode_count": 1}],
            },
            "/tv/1/season/1": {
                "episodes": [{"episode_number": 1, "name": "Exodus Christmas"}]
            },
            "/tv/1/season/0": {"episodes": [
                {"episode_number": 1, "name": "Exodus!"},
                {"episode_number": 2, "name": "第三飞行少女队"},
            ]},
        })
        plan = scraper.build_tv_plan_smart(
            auto_episode_mode=True, alist=FakeAList(files), tmdb_client=tmdb,
            src_path="/src", parent_path="/library", tmdb_id=1,
            season=1, absolute=False, prefer_simplified=True,
            allow_unmapped=False, episode_map_path=None, episode_group_id=None,
        )
        aerial = [item for item in plan.files if "Daisan" in item.source_path]
        self.assertEqual(len(aerial), 4)
        self.assertTrue(all("S00E02" in item.final_name for item in aerial))
        self.assertTrue(any("{edition-Musani Staff Credit}" in item.final_name for item in aerial))
        self.assertTrue(any("{edition-Original Staff Credit}" in item.final_name for item in aerial))
        self.assertFalse(any("Daisan" in item.source_path for item in plan.cleanup_files))
        self.assertTrue(set(making_paths).issubset({
            row["source_path"] for row in plan.scan_report["deferred_subtitles"]
        }))
        self.assertFalse(set(making_paths) & {item.source_path for item in plan.files})

    def test_named_three_part_special_arc_maps_to_official_special_run(self):
        files = [
            {
                "name": "Slime.S01E01.mkv",
                "full_path": "/src/Season 1/Slime.S01E01.mkv",
            },
            *[
                {
                    "name": f"Coleus no Yume [{number:02d}].2160p.mkv",
                    "full_path": "/src/关于我转生变成史莱姆这档事 柯里乌斯之梦/"
                    f"Coleus no Yume [{number:02d}].2160p.mkv",
                }
                for number in range(1, 4)
            ],
            *[
                {
                    "name": f"Coleus no Yume [{number:02d}].1080p.mkv",
                    "full_path": (
                        "/src/1080P备份版/2023.11 轉生史萊姆 柯里乌斯之梦/"
                        "[LoliHouse] 轉生史萊姆 彩叶草之梦 柯里乌斯之梦 "
                        "[01-03 合集][WebRip 1080p HEVC-10bit AAC][Fin]/"
                        f"Coleus no Yume [{number:02d}].1080p.mkv"
                    ),
                }
                for number in range(1, 4)
            ],
        ]
        tmdb = FakeTMDB(
            {
                "/tv/82684": {
                    "name": "关于我转生变成史莱姆这档事",
                    "first_air_date": "2018-10-02",
                    "poster_path": None,
                    "backdrop_path": None,
                    "seasons": [
                        {"season_number": 0, "episode_count": 13},
                        {"season_number": 1, "episode_count": 1},
                    ],
                },
                "/tv/82684/season/1": {
                    "episodes": [
                        {"episode_number": 1, "name": "暴风龙维鲁德拉"}
                    ]
                },
                "/tv/82684/season/0": {
                    "episodes": [
                        {
                            "episode_number": 11,
                            "name": "柯里乌斯之梦 前往柯里乌斯国",
                        },
                        {
                            "episode_number": 12,
                            "name": "柯里乌斯之梦 大怪盗阿悟",
                        },
                        {
                            "episode_number": 13,
                            "name": "柯里乌斯之梦 紫与蔷薇",
                        },
                    ]
                },
            }
        )
        plan = scraper.build_tv_plan_smart(
            auto_episode_mode=True,
            alist=FakeAList(files),
            tmdb_client=tmdb,
            src_path="/src",
            parent_path="/library",
            tmdb_id=82684,
            season=1,
            absolute=False,
            prefer_simplified=True,
            allow_unmapped=False,
            episode_map_path=None,
            episode_group_id=None,
        )
        final_names = {item.final_name for item in plan.files}
        self.assertTrue(any("S00E11" in name for name in final_names))
        self.assertTrue(any("S00E12" in name for name in final_names))
        self.assertTrue(any("S00E13" in name for name in final_names))
        self.assertFalse(any("1080p" in item.original_name for item in plan.files))
        self.assertEqual(len(plan.cleanup_files), 3)
        self.assertEqual(plan.problem_files, [])

    def test_smart_mode_matches_named_series_variants_to_tmdb_seasons(self):
        files = [
            {
                "name": "[Ygm] Toaru Kagaku no Railgun [01].mkv",
                "full_path": "/src/某科学的超电磁炮/[Ygm] Toaru Kagaku no Railgun [01].mkv",
            },
            {
                "name": "[Ygm] Toaru Kagaku no Railgun S [01].mkv",
                "full_path": "/src/某科学的超电磁炮 S/[Ygm] Toaru Kagaku no Railgun S [01].mkv",
            },
            {
                "name": "[Ygm] Toaru Kagaku no Railgun T [01].mkv",
                "full_path": "/src/某科学的超电磁炮 T/[Ygm] Toaru Kagaku no Railgun T [01].mkv",
            },
            {
                "name": "[Ygm] Toaru Kagaku no Railgun [SP01_MMR1].mkv",
                "full_path": "/src/某科学的超电磁炮/[Ygm] Toaru Kagaku no Railgun [SP01_MMR1].mkv",
            },
            {
                "name": "[Ygm] Toaru Kagaku no Railgun [SP02_MMR2].mkv",
                "full_path": "/src/某科学的超电磁炮/[Ygm] Toaru Kagaku no Railgun [SP02_MMR2].mkv",
            },
            {
                "name": "[Ygm] Toaru Kagaku no Railgun [OVBSP].mkv",
                "full_path": "/src/某科学的超电磁炮/[Ygm] Toaru Kagaku no Railgun [OVBSP].mkv",
            },
            {
                "name": "[Ygm] Toaru Kagaku no Railgun [OVA].mkv",
                "full_path": "/src/某科学的超电磁炮/[Ygm] Toaru Kagaku no Railgun [OVA].mkv",
            },
            {
                "name": "[Ygm] Toaru Kagaku no Railgun S [SP01_MMR03].mkv",
                "full_path": "/src/某科学的超电磁炮 S/[Ygm] Toaru Kagaku no Railgun S [SP01_MMR03].mkv",
            },
            {
                "name": "[Ygm] Toaru Kagaku no Railgun S [SP02_MMR03].mkv",
                "full_path": "/src/某科学的超电磁炮 S/[Ygm] Toaru Kagaku no Railgun S [SP02_MMR03].mkv",
            },
            {
                "name": "[Ygm] Toaru Kagaku no Railgun S [SP02_MMR04].ass",
                "full_path": "/src/某科学的超电磁炮 S/备份字幕/[Ygm] Toaru Kagaku no Railgun S [SP02_MMR04].ass",
            },
            {
                "name": "[Ygm] Toaru Kagaku no Railgun S [OVA].mkv",
                "full_path": "/src/某科学的超电磁炮 S/[Ygm] Toaru Kagaku no Railgun S [OVA].mkv",
            },
            {
                "name": "[Ygm] Toaru Kagaku no Railgun T [SP01_MMR05].mkv",
                "full_path": "/src/某科学的超电磁炮 T/[Ygm] Toaru Kagaku no Railgun T [SP01_MMR05].mkv",
            },
            {
                "name": "[Moozzi2] Railgun T [SP04] MMR V.sc.ass",
                "full_path": "/src/某科学的超电磁炮 T/备份字幕/[Moozzi2] Railgun T [SP04] MMR V.sc.ass",
            },
            {
                "name": "[Moozzi2] Railgun T [SP04] MMR VI.sc.ass",
                "full_path": "/src/某科学的超电磁炮 T/备份字幕/[Moozzi2] Railgun T [SP04] MMR VI.sc.ass",
            },
        ]
        tmdb = FakeTMDB(
            {
                "/tv/30977": {
                    "name": "某科学的超电磁炮",
                    "first_air_date": "2009-10-03",
                    "poster_path": None,
                    "backdrop_path": None,
                    "seasons": [
                        {"season_number": 1, "name": "某科学的超电磁炮", "air_date": "2009-10-02"},
                        {"season_number": 2, "name": "某科学的超电磁炮S", "air_date": "2013-04-12"},
                        {"season_number": 3, "name": "某科学的超电磁炮T", "air_date": "2020-01-10"},
                    ],
                },
                "/tv/30977/season/1": {
                    "episodes": [{"episode_number": 1, "name": "Electromaster", "air_date": "2009-10-02"}]
                },
                "/tv/30977/season/2": {
                    "episodes": [{"episode_number": 1, "name": "Railgun S", "air_date": "2013-04-12"}]
                },
                "/tv/30977/season/3": {
                    "episodes": [{"episode_number": 1, "name": "Railgun T", "air_date": "2020-01-10"}]
                },
                "/tv/30977/season/0": {
                    "episodes": [
                        {"episode_number": 1, "name": "更多更多的超电磁炮 MMR 01", "air_date": "2010-01-29"},
                        {"episode_number": 2, "name": "更多更多的超电磁炮 MMR 02", "air_date": "2010-05-28"},
                        {"episode_number": 3, "name": "炎炎酷日下做摄影模特也不轻松", "air_date": "2010-07-24"},
                        {"episode_number": 4, "name": "御坂学姐现在是焦点人物", "air_date": "2010-10-29"},
                        {"episode_number": 5, "name": "更多更多的超电磁炮 MMR 03", "air_date": "2013-07-24"},
                        {"episode_number": 6, "name": "更多更多的超电磁炮 MMR 04", "air_date": "2013-11-27"},
                        {"episode_number": 7, "name": "重要的事都能在澡堂学到", "air_date": "2014-03-25"},
                        {"episode_number": 8, "name": "更多更多的超电磁炮 MMR 05", "air_date": "2020-04-30"},
                        {"episode_number": 9, "name": "更多更多的超电磁炮 MMR 06", "air_date": "2020-10-09"},
                    ]
                },
            }
        )
        plan = scraper.build_tv_plan_smart(
            auto_episode_mode=True,
            alist=FakeAList(files),
            tmdb_client=tmdb,
            src_path="/src",
            parent_path="/library",
            tmdb_id=30977,
            season=1,
            absolute=False,
            prefer_simplified=True,
            allow_unmapped=False,
            episode_map_path=None,
            episode_group_id=None,
        )
        targets = {item.source_path: item.target_dir for item in plan.files}
        self.assertIn("/Season 01", targets[files[0]["full_path"]])
        self.assertIn("/Season 02", targets[files[1]["full_path"]])
        self.assertIn("/Season 03", targets[files[2]["full_path"]])
        self.assertFalse(any(" - v2" in item.final_name for item in plan.files))
        self.assertFalse(any(" - v3" in item.final_name for item in plan.files))
        special_names = {item.final_name for item in plan.files if "S00" in item.final_name}
        self.assertTrue(any("S00E01" in name for name in special_names))
        self.assertTrue(any("S00E03" in name for name in special_names))
        self.assertTrue(any("S00E04" in name for name in special_names))
        self.assertTrue(any("S00E05" in name for name in special_names))
        self.assertTrue(any("S00E06" in name for name in special_names))
        self.assertTrue(any("S00E07" in name for name in special_names))
        self.assertTrue(any("S00E08" in name for name in special_names))
        self.assertFalse(any("S00E09" in name for name in special_names))
        self.assertIn(
            files[-1]["full_path"],
            {row["source_path"] for row in plan.scan_report["deferred_subtitles"]},
        )

    def test_disc_extra_run_maps_one_remaining_named_ova_with_video(self):
        files = [
            {
                "name": "Railgun S [SP01_MMR03].mkv",
                "full_path": "/src/某科学的超电磁炮 S/Railgun S [SP01_MMR03].mkv",
            },
            {
                "name": "Railgun S [SP02_MMR04].mkv",
                "full_path": "/src/某科学的超电磁炮 S/Railgun S [SP02_MMR04].mkv",
            },
            {
                "name": "Railgun S [OVA].mkv",
                "full_path": "/src/某科学的超电磁炮 S/Railgun S [OVA].mkv",
            },
            {
                "name": "Railgun S [OVA].ass",
                "full_path": "/src/某科学的超电磁炮 S/备份字幕/Railgun S [OVA].ass",
            },
        ]
        show = {
            "name": "某科学的超电磁炮",
            "original_name": "とある科学の超電磁砲",
            "seasons": [
                {"season_number": 1, "name": "某科学的超电磁炮", "episode_count": 24, "air_date": "2009-10-02"},
                {"season_number": 2, "name": "某科学的超电磁炮 S", "episode_count": 24, "air_date": "2013-04-12"},
                {"season_number": 3, "name": "某科学的超电磁炮 T", "episode_count": 25, "air_date": "2020-01-10"},
            ],
        }
        changed = scraper._map_disc_extras_by_official_release_runs(
            files,
            show=show,
            positive_seasons=show["seasons"],
            special_runtimes={1: 9, 2: 9, 3: 5, 4: 35, 5: 9, 6: 9, 7: 6, 8: 10},
            special_air_dates={
                1: "2010-01-29", 2: "2010-05-28", 3: "2010-07-24", 4: "2010-10-29",
                5: "2013-07-24", 6: "2013-11-27", 7: "2014-03-25", 8: "2020-04-30",
            },
        )

        self.assertEqual(changed, 4)
        self.assertEqual(files[0]["_episode_key_override"], 5)
        self.assertEqual(files[1]["_episode_key_override"], 6)
        self.assertEqual(files[2]["_episode_key_override"], 7)
        self.assertEqual(files[3]["_episode_key_override"], 7)

    def test_disc_extra_run_does_not_map_orphan_named_ova_subtitle(self):
        files = [{
            "name": "Railgun S [OVA].ass",
            "full_path": "/src/某科学的超电磁炮 S/备份字幕/Railgun S [OVA].ass",
        }]
        show = {
            "name": "某科学的超电磁炮",
            "seasons": [{"season_number": 2, "name": "某科学的超电磁炮 S", "episode_count": 24, "air_date": "2013-04-12"}],
        }
        changed = scraper._map_disc_extras_by_official_release_runs(
            files,
            show=show,
            positive_seasons=show["seasons"],
            special_runtimes={5: 9, 6: 9, 7: 6},
            special_air_dates={5: "2013-07-24", 6: "2013-11-27", 7: "2014-03-25"},
        )

        self.assertEqual(changed, 0)
        self.assertNotIn("_episode_key_override", files[0])

    def test_disc_extra_run_maps_ovbsp_beside_already_mapped_full_ova(self):
        files = [
            {"name": "Railgun [SP01].mkv", "full_path": "/src/某科学的超电磁炮/Railgun [SP01].mkv"},
            {"name": "Railgun [SP02].mkv", "full_path": "/src/某科学的超电磁炮/Railgun [SP02].mkv"},
            {"name": "Railgun [OVA].mkv", "full_path": "/src/某科学的超电磁炮/Railgun [OVA].mkv"},
            {"name": "Railgun [OVBSP].mkv", "full_path": "/src/某科学的超电磁炮/Railgun [OVBSP].mkv"},
        ]
        show = {
            "name": "某科学的超电磁炮",
            "seasons": [
                {"season_number": 1, "name": "某科学的超电磁炮", "episode_count": 24, "air_date": "2009-10-02"},
                {"season_number": 2, "name": "某科学的超电磁炮 S", "episode_count": 24, "air_date": "2013-04-12"},
            ],
        }
        changed = scraper._map_disc_extras_by_official_release_runs(
            files,
            show=show,
            positive_seasons=show["seasons"],
            special_runtimes={1: 9, 2: 9, 3: 5, 4: 35, 5: 9},
            special_air_dates={
                1: "2010-01-29", 2: "2010-05-28", 3: "2010-07-24",
                4: "2010-10-29", 5: "2013-07-24",
            },
        )

        self.assertEqual(changed, 4)
        self.assertEqual([item["_episode_key_override"] for item in files], [1, 2, 4, 3])

    def test_smart_mode_splits_embedded_tmdb_movies_from_multi_season_show(self):
        files = [
            {"name": "Show.S01E01.mkv", "full_path": "/src/Season 1/Show.S01E01.mkv"},
            {"name": "Show.S02E01.mkv", "full_path": "/src/Season 2/Show.S02E01.mkv"},
            {
                "name": "剧场版 Show (2024) {tmdb-99}.mkv",
                "full_path": "/src/剧场版 Show/剧场版 Show (2024) {tmdb-99}.mkv",
            },
            {
                "name": "Show Movie [NCOP].mkv",
                "full_path": "/src/剧场版 Show/SPs/Show Movie [NCOP].mkv",
            },
        ]
        tmdb = FakeTMDB(
            {
                "/tv/1": {
                    "name": "Show",
                    "first_air_date": "2020-01-01",
                    "poster_path": "/show.jpg",
                    "backdrop_path": None,
                    "seasons": [{"season_number": 1}, {"season_number": 2}],
                },
                "/tv/1/season/1": {"episodes": [{"episode_number": 1, "name": "One"}]},
                "/tv/1/season/2": {"episodes": [{"episode_number": 1, "name": "Two"}]},
                "/tv/1/season/0": {"episodes": []},
                "/movie/99": {
                    "title": "Show Movie",
                    "release_date": "2024-01-01",
                    "poster_path": "/movie.jpg",
                    "backdrop_path": None,
                },
            }
        )
        plan = scraper.build_tv_plan_smart(
            auto_episode_mode=True,
            alist=FakeAList(files),
            tmdb_client=tmdb,
            src_path="/src",
            parent_path="/library",
            tmdb_id=1,
            season=1,
            absolute=False,
            prefer_simplified=True,
            allow_unmapped=False,
            episode_map_path=None,
            episode_group_id=None,
        )
        self.assertEqual(plan.mode, "mixed")
        self.assertEqual(len(plan.files), 3)
        self.assertEqual(
            [item.source_path for item in plan.cleanup_files],
            ["/src/剧场版 Show/SPs/Show Movie [NCOP].mkv"],
        )
        self.assertTrue(any(item.final_name == "Show Movie (2024).mkv" for item in plan.files))
        self.assertTrue(any("并列的独立电影目录" in warning for warning in plan.warnings))
        self.assertEqual(
            plan.metadata["member_posters"]["/library/Show Movie (2024)"],
            "/movie.jpg",
        )
        self.assertEqual(
            plan.metadata["member_movies"]["/library/Show Movie (2024)"]["tmdb_id"],
            99,
        )
        self.assertEqual(plan.target_root, "/library")
        self.assertEqual(
            next(item.target_dir for item in plan.files if item.final_name == "Show Movie (2024).mkv"),
            "/library/Show Movie (2024)",
        )
        artwork_targets = {target for target, _, _ in scraper.planned_artwork(plan)}
        self.assertIn("/library/Show/poster.jpg", artwork_targets)
        self.assertIn("/library/Show Movie (2024)/Show Movie (2024).jpg", artwork_targets)
        self.assertIn("/library/Show Movie (2024)/folder.jpg", artwork_targets)
        self.assertEqual(
            scraper.planned_movie_nfos(plan)[0][0],
            "/library/Show Movie (2024)/Show Movie (2024).nfo",
        )
        self.assertEqual(
            scraper.planned_tv_nfos(plan)[0][0],
            "/library/Show/tvshow.nfo",
        )
        self.assertEqual(scraper.plan_from_dict(scraper.plan_to_dict(plan)).mode, "mixed")

    def test_smart_mode_preserves_new_edit_as_proven_episode_ranges(self):
        files = [
            {
                "name": f"Re Zero [{number:02d}].mkv",
                "full_path": f"/src/第一季/Re Zero [{number:02d}].mkv",
            }
            for number in range(1, 4)
        ] + [
            {
                "name": f"Re Zero 新编集版 [{number:02d}].mkv",
                "full_path": f"/src/新编集版/Re Zero 新编集版 [{number:02d}].mkv",
            }
            for number in range(1, 3)
        ]
        tmdb = FakeTMDB(
            {
                "/tv/1": {
                    "name": "Re Zero",
                    "first_air_date": "2016-04-04",
                    "poster_path": None,
                    "backdrop_path": None,
                    "seasons": [
                        {
                            "season_number": 1,
                            "name": "Season 1",
                            "episode_count": 3,
                        }
                    ],
                },
                "/tv/1/season/1": {
                    "episodes": [
                        {"episode_number": 1, "name": "One"},
                        {"episode_number": 2, "name": "Two"},
                        {"episode_number": 3, "name": "Three"},
                    ]
                },
                "/tv/1/season/0": {"episodes": []},
            }
        )
        plan = scraper.build_tv_plan_smart(
            auto_episode_mode=True,
            alist=FakeAList(files),
            tmdb_client=tmdb,
            src_path="/src",
            parent_path="/library",
            tmdb_id=1,
            season=1,
            absolute=False,
            prefer_simplified=True,
            allow_unmapped=False,
            episode_map_path=None,
            episode_group_id=None,
        )
        self.assertEqual(len(plan.files), 5)
        final_names = {item.final_name for item in plan.files}
        self.assertTrue(any(
            "S01E01" in name and "{edition-New Edit}" in name
            for name in final_names
        ))
        self.assertTrue(any(
            "S01E02-E03" in name and "{edition-New Edit}" in name
            for name in final_names
        ))
        self.assertFalse(any("Director's Cut" in name for name in final_names))
        self.assertTrue(any(
            "New Edit" in warning and "双集重编范围" in warning
            for warning in plan.warnings
        ))

    def test_smart_mode_packs_partial_latest_broadcast_block_from_air_dates(self):
        files = [
            {
                "name": f"Show [{episode:02d}].mkv",
                "full_path": f"/src/第{season}季/Show [{episode:02d}].mkv",
            }
            for season, count in (("一", 2), ("二", 2), ("三", 1))
            for episode in range(1, count + 1)
        ]
        tmdb = FakeTMDB(
            {
                "/tv/1": {
                    "name": "Show",
                    "first_air_date": "2020-01-01",
                    "poster_path": None,
                    "backdrop_path": None,
                    "seasons": [
                        {
                            "season_number": 1,
                            "name": "Season 1",
                            "episode_count": 6,
                        }
                    ],
                },
                "/tv/1/season/1": {
                    "episodes": [
                        {"episode_number": 1, "name": "One", "air_date": "2020-01-01"},
                        {"episode_number": 2, "name": "Two", "air_date": "2020-01-08"},
                        {"episode_number": 3, "name": "Three", "air_date": "2021-01-01"},
                        {"episode_number": 4, "name": "Four", "air_date": "2021-01-08"},
                        {"episode_number": 5, "name": "Five", "air_date": "2022-01-01"},
                        {"episode_number": 6, "name": "Six", "air_date": "2022-01-08"},
                    ]
                },
                "/tv/1/season/0": {"episodes": []},
            }
        )
        plan = scraper.build_tv_plan_smart(
            auto_episode_mode=True,
            alist=FakeAList(files),
            tmdb_client=tmdb,
            src_path="/src",
            parent_path="/library",
            tmdb_id=1,
            season=1,
            absolute=False,
            prefer_simplified=True,
            allow_unmapped=False,
            episode_map_path=None,
            episode_group_id=None,
        )
        self.assertEqual(len(plan.files), 5)
        self.assertTrue(any("S01E05" in item.final_name for item in plan.files))
        self.assertFalse(any("S01E06" in item.final_name for item in plan.files))
        self.assertTrue(any(
            "官方播出日期" in warning and "1/2 集" in warning
            for warning in plan.warnings
        ))

    def test_smart_mode_folds_cumulative_backup_inside_complete_local_block(self):
        files = [
            *[
                {"name": f"Show [{episode:02d}] 2160p.mkv", "full_path": f"/src/第一季/Show [{episode:02d}] 2160p.mkv"}
                for episode in range(1, 3)
            ],
            *[
                {"name": f"Show [{episode:02d}] 2160p.mkv", "full_path": f"/src/第二季/Show [{episode:02d}] 2160p.mkv"}
                for episode in range(1, 3)
            ],
            *[
                {"name": f"Show [{episode:02d}] 1080p.mkv", "full_path": f"/src/第二季/累计版/Show [{episode:02d}] 1080p.mkv"}
                for episode in range(3, 5)
            ],
            {"name": "Show [01] 2160p.mkv", "full_path": "/src/第三季/Show [01] 2160p.mkv"},
        ]
        tmdb = FakeTMDB({
            "/tv/1": {"name": "Show", "first_air_date": "2020-01-01", "poster_path": None, "backdrop_path": None, "seasons": [{"season_number": 1, "name": "Season 1", "episode_count": 6}]},
            "/tv/1/season/1": {"episodes": [
                {"episode_number": number, "name": str(number), "air_date": air_date}
                for number, air_date in enumerate((
                    "2020-01-01", "2020-01-08", "2021-01-01",
                    "2021-01-08", "2022-01-01", "2022-01-08",
                ), start=1)
            ]},
            "/tv/1/season/0": {"episodes": []},
        })
        plan = scraper.build_tv_plan_smart(
            auto_episode_mode=True, alist=FakeAList(files), tmdb_client=tmdb,
            src_path="/src", parent_path="/library", tmdb_id=1, season=1,
            absolute=False, prefer_simplified=True, allow_unmapped=False,
            episode_map_path=None, episode_group_id=None,
        )
        self.assertEqual(len(plan.files), 5)
        self.assertTrue(any("S01E03" in item.final_name for item in plan.files))
        self.assertTrue(any("S01E04" in item.final_name for item in plan.files))
        self.assertTrue(any("全剧累计编号" in warning for warning in plan.warnings))

    def test_smart_mode_maps_complete_cumulative_only_broadcast_block(self):
        files = [
            *[
                {"name": f"Show [{episode:02d}].mkv", "full_path": f"/src/第一季/Show [{episode:02d}].mkv"}
                for episode in range(1, 3)
            ],
            *[
                {"name": f"Show [{episode:02d}].mkv", "full_path": f"/src/第二季/Show [{episode:02d}].mkv"}
                for episode in range(3, 5)
            ],
            {"name": "Show [01].mkv", "full_path": "/src/第三季/Show [01].mkv"},
        ]
        tmdb = FakeTMDB({
            "/tv/1": {"name": "Show", "first_air_date": "2020-01-01", "poster_path": None, "backdrop_path": None, "seasons": [{"season_number": 1, "name": "Season 1", "episode_count": 6}]},
            "/tv/1/season/1": {"episodes": [
                {"episode_number": number, "name": str(number), "air_date": date}
                for number, date in enumerate(("2020-01-01", "2020-01-08", "2021-01-01", "2021-01-08", "2022-01-01", "2022-01-08"), start=1)
            ]},
            "/tv/1/season/0": {"episodes": []},
        })
        plan = scraper.build_tv_plan_smart(
            auto_episode_mode=True, alist=FakeAList(files), tmdb_client=tmdb,
            src_path="/src", parent_path="/library", tmdb_id=1, season=1,
            absolute=False, prefer_simplified=True, allow_unmapped=False,
            episode_map_path=None, episode_group_id=None,
        )
        self.assertEqual(len(plan.files), 5)
        self.assertTrue(any("S01E03" in item.final_name for item in plan.files))
        self.assertTrue(any("S01E04" in item.final_name for item in plan.files))

    def test_smart_mode_maps_named_special_run_before_parent_tv_season(self):
        files = [
            {
                "name": "Show [01].mkv",
                "full_path": "/src/第一季/Show [01].mkv",
            },
            *[
                {
                    "name": f"Show Break Time 2nd Season [{episode:02d}].mkv",
                    "full_path": (
                        "/src/第二季/SPs/"
                        f"Show Break Time 2nd Season [{episode:02d}].mkv"
                    ),
                }
                for episode in range(1, 3)
            ],
            {
                "name": "Show Break Time 2nd Season [01].ass",
                "full_path": (
                    "/src/第二季/SPs/备份字幕/"
                    "Show Break Time 2nd Season [01].ass"
                ),
            },
        ]

        class MultilingualSpecialTMDB(FakeTMDB):
            def get(self, path, **params):
                if path == "/tv/1/season/0":
                    language = params.get("language", "zh-CN")
                    names = {
                        "zh-CN": ["短篇甲", "短篇乙"],
                        "ja-JP": [
                            "Show Break Time 2nd season 第一話",
                            "Show Break Time 2nd season 第二話",
                        ],
                        "en-US": [
                            "Show Break Time 2: First",
                            "Show Break Time 2: Second",
                        ],
                    }.get(language, ["短篇甲", "短篇乙"])
                    return {
                        "episodes": [
                            {"episode_number": number + 4, "name": title}
                            for number, title in enumerate(names, start=1)
                        ]
                    }
                return super().get(path, **params)

        tmdb = MultilingualSpecialTMDB(
            {
                "/tv/1": {
                    "name": "Show",
                    "first_air_date": "2020-01-01",
                    "poster_path": None,
                    "backdrop_path": None,
                    "seasons": [
                        {"season_number": 0, "name": "Specials", "episode_count": 6},
                        {"season_number": 1, "name": "Season 1", "episode_count": 1},
                    ],
                },
                "/tv/1/season/1": {
                    "episodes": [{"episode_number": 1, "name": "One"}]
                },
            }
        )
        plan = scraper.build_tv_plan_smart(
            auto_episode_mode=True,
            alist=FakeAList(files),
            tmdb_client=tmdb,
            src_path="/src",
            parent_path="/library",
            tmdb_id=1,
            season=1,
            absolute=False,
            prefer_simplified=True,
            allow_unmapped=False,
            episode_map_path=None,
            episode_group_id=None,
        )
        self.assertEqual(len(plan.files), 4)
        self.assertTrue(any("S00E05" in item.final_name for item in plan.files))
        self.assertTrue(any("S00E06" in item.final_name for item in plan.files))
        self.assertFalse(any("S02E" in item.final_name for item in plan.files))
        self.assertTrue(any(
            "多语言官方标题" in warning and "SP05–SP06" in warning
            for warning in plan.warnings
        ))

    def test_real_case_nested_spinoff_series_becomes_batch_member(self):
        files = [
            {"name": "Main.S01E01.mkv", "full_path": "/src/Season 1/Main.S01E01.mkv"},
            {"name": "Main.S02E01.mkv", "full_path": "/src/Season 2/Main.S02E01.mkv"},
            {"name": "Magi Sinbad no Bouken [01].2160p.mkv", "full_path": "/src/魔笛MAGI 辛巴达的冒险/Magi Sinbad no Bouken [01].2160p.mkv"},
            {"name": "Magi Sinbad no Bouken [01].1080p.mkv", "full_path": "/src/魔笛MAGI 辛巴达的冒险 1080p/Magi Sinbad no Bouken [01].1080p.mkv"},
            {"name": "Magi Sinbad no Bouken [01].ass", "full_path": "/src/备份字幕/Magi Sinbad no Bouken [01].ass"},
        ]

        class SpinoffTMDB(FakeTMDB):
            def get(self, path, **params):
                if path == "/search/tv":
                    return {"results": [{"id": 66870, "name": "魔笛MAGI 辛巴达的冒险", "genre_ids": [16]}]}
                if path == "/tv/66870/alternative_titles":
                    return {"results": [{"title": "Magi Sinbad no Bouken"}]}
                return super().get(path, **params)

        tmdb = SpinoffTMDB({
            "/tv/1": {"name": "Main", "first_air_date": "2020-01-01", "seasons": [{"season_number": 1}, {"season_number": 2}]},
            "/tv/1/season/1": {"episodes": [{"episode_number": 1, "name": "One"}]},
            "/tv/1/season/2": {"episodes": [{"episode_number": 1, "name": "Two"}]},
            "/tv/1/season/0": {"episodes": []},
            "/tv/66870": {"name": "魔笛MAGI 辛巴达的冒险", "first_air_date": "2016-01-01", "seasons": [{"season_number": 1}]},
            "/tv/66870/season/1": {"episodes": [{"episode_number": 1, "name": "命运之子"}]},
            "/tv/66870/season/0": {"episodes": []},
        })
        plan = scraper.build_tv_plan_smart(
            auto_episode_mode=True, alist=FakeAList(files), tmdb_client=tmdb,
            src_path="/src", parent_path="/library", tmdb_id=1, season=1,
            absolute=False, prefer_simplified=True, allow_unmapped=False,
            episode_map_path=None, episode_group_id=None,
        )
        self.assertEqual(plan.mode, "batch")
        self.assertEqual(len(plan.files), 4, [item.source_path for item in plan.files])
        self.assertTrue(any(identity["tmdb_id"] == 66870 for identity in plan.metadata["member_tv"].values()))
        spinoff_root = "/library/Main/魔笛MAGI 辛巴达的冒险"
        self.assertIn(spinoff_root, plan.metadata["member_tv"])
        self.assertTrue(any(
            item.source_path.endswith("Magi Sinbad no Bouken [01].2160p.mkv")
            and item.target_dir == f"{spinoff_root}/Season 01"
            for item in plan.files
        ))
        self.assertEqual(
            [item.source_path for item in plan.cleanup_files],
            [
                "/src/魔笛MAGI 辛巴达的冒险 1080p/"
                "Magi Sinbad no Bouken [01].1080p.mkv"
            ],
        )
        self.assertTrue(any("保留在主系列目录内" in warning for warning in plan.warnings))

    def test_franchise_batch_independently_matches_untagged_members(self):
        files = [
            {
                "name": "Main [01].mkv",
                "full_path": "/src/01 Main (2020)/Main [01].mkv",
            },
            {
                "name": "Spin [01].mkv",
                "full_path": "/src/02 Spin (2022)/Spin [01].mkv",
            },
        ]

        class PrefixAList(FakeAList):
            def walk(self, path, refresh=True, **kwargs):
                prefix = path.rstrip("/") + "/"
                return [
                    dict(item)
                    for item in self.files
                    if str(item["full_path"]).startswith(prefix)
                ]

        alist = PrefixAList(
            files,
            listings={
                "/src": [
                    {"name": "01 Main (2020)", "is_dir": True},
                    {"name": "02 Spin (2022)", "is_dir": True},
                ]
            },
        )
        tmdb = FakeTMDB({
            "/search/movie": {"results": []},
            "/search/tv": {
                "results": [
                    {
                        "id": 1,
                        "name": "01 Main",
                        "first_air_date": "2020-01-01",
                        "genre_ids": [16],
                    },
                    {
                        "id": 2,
                        "name": "02 Spin",
                        "first_air_date": "2022-01-01",
                        "genre_ids": [16],
                    },
                ]
            },
            "/tv/1": {
                "name": "01 Main",
                "first_air_date": "2020-01-01",
                "seasons": [{"season_number": 1, "episode_count": 1}],
            },
            "/tv/1/season/1": {
                "episodes": [{"episode_number": 1, "name": "One"}]
            },
            "/tv/1/season/0": {"episodes": []},
            "/tv/2": {
                "name": "02 Spin",
                "first_air_date": "2022-01-01",
                "seasons": [{"season_number": 1, "episode_count": 1}],
            },
            "/tv/2/season/1": {
                "episodes": [{"episode_number": 1, "name": "One"}]
            },
            "/tv/2/season/0": {"episodes": []},
        })
        plan = scraper.build_batch_plan(
            alist,
            tmdb,
            src_path="/src",
            parent_path="/library",
        )
        self.assertEqual(plan.mode, "batch")
        self.assertEqual(
            {identity["tmdb_id"] for identity in plan.metadata["member_tv"].values()},
            {1, 2},
        )
        self.assertTrue(any("2 个未标注" in warning for warning in plan.warnings))

    def test_generic_season_child_rejects_additive_sibling_tv_by_episode_boundary(self):
        root = "/src/瑞克和MD 1-9季+日漫版"
        member = f"{root}/第一季（2013）全11集"
        files = [
            {"name": f"{number:02d}.mkv", "full_path": f"{member}/{number:02d}.mkv"}
            for number in range(1, 12)
        ]
        queries = scraper._franchise_member_match_queries(member, files)
        self.assertTrue(any(
            query.startswith("瑞克和MD 第一季") and "+日漫版" not in query
            for query in queries
        ), queries)

        anime = scraper.AutoMatch("tv", 2, "瑞克和莫蒂：日漫版", "2024", 0.99)
        main = scraper.AutoMatch("tv", 1, "瑞克和莫蒂", "2013", 0.99)

        def fake_match(_client, query, **_kwargs):
            match = anime if "+日漫版" in query else main
            return match, [match]

        tmdb = FakeTMDB({
            "/tv/1": {
                "name": "瑞克和莫蒂",
                "seasons": [{"season_number": 1, "episode_count": 11}],
            },
            "/tv/2": {
                "name": "瑞克和莫蒂：日漫版",
                "seasons": [{"season_number": 1, "episode_count": 10}],
            },
        })
        with mock.patch.object(scraper, "auto_match_tmdb", side_effect=fake_match):
            match = scraper._franchise_member_match(
                tmdb, member, files, target_parent="/library"
            )
        self.assertEqual((match.media_type, match.tmdb_id), ("tv", 1))

    def test_equal_boundary_additive_child_keeps_main_season_and_anime_separate(self):
        root = "/src/瑞克和MD 1-9季+日漫版"
        main_root = f"{root}/第七季（2023）全10集"
        anime_root = f"{root}/瑞克和莫蒂：日漫版（2024）全10集"
        files = [
            {
                "name": f"{number:02d}.mkv",
                "full_path": f"{member}/{number:02d}.mkv",
            }
            for member in (main_root, anime_root)
            for number in range(1, 11)
        ]
        class PrefixAList(FakeAList):
            def walk(self, path, refresh=True, **kwargs):
                prefix = path.rstrip("/") + "/"
                return [
                    dict(item)
                    for item in self.files
                    if str(item["full_path"]).startswith(prefix)
                ]

        alist = PrefixAList(files, listings={
            root: [
                {"name": main_root.rsplit("/", 1)[-1], "is_dir": True},
                {"name": anime_root.rsplit("/", 1)[-1], "is_dir": True},
            ],
        })
        tmdb = FakeTMDB({
            "/tv/1": {
                "name": "瑞克和莫蒂",
                "first_air_date": "2013-12-02",
                "seasons": [{
                    "season_number": 7,
                    "name": "第 7 季",
                    "episode_count": 10,
                    "air_date": "2023-10-15",
                }],
            },
            "/tv/1/season/7": {"episodes": [
                {"episode_number": number, "name": f"Main {number}"}
                for number in range(1, 11)
            ]},
            "/tv/1/season/0": {"episodes": []},
            "/tv/2": {
                "name": "瑞克和莫蒂：日漫版",
                "first_air_date": "2024-08-16",
                "seasons": [{
                    "season_number": 1,
                    "name": "瑞克和莫蒂：日漫版",
                    "episode_count": 10,
                    "air_date": "2024-08-16",
                }],
            },
            "/tv/2/season/1": {"episodes": [
                {"episode_number": number, "name": f"Anime {number}"}
                for number in range(1, 11)
            ]},
            "/tv/2/season/0": {"episodes": []},
            "/search/movie": {"results": []},
        })
        main = scraper.AutoMatch("tv", 1, "瑞克和莫蒂", "2013", 0.99)
        anime = scraper.AutoMatch(
            "tv", 2, "瑞克和莫蒂：日漫版", "2024", 0.99
        )
        queries: list[str] = []
        returned_wrong_additive_sibling = False

        def fake_match(_client, query, **_kwargs):
            nonlocal returned_wrong_additive_sibling
            queries.append(query)
            if "日漫版" in query:
                match = anime
            elif not returned_wrong_additive_sibling:
                # Reproduce the live failure: the provider returns the sibling
                # anime for the first otherwise-correct main-season query.
                returned_wrong_additive_sibling = True
                match = anime
            else:
                match = main
            return match, [match]

        self.assertTrue(scraper._is_generic_season_member_label(
            "第七季（2023）全10集"
        ))
        self.assertTrue(scraper._is_generic_season_member_label(
            "第七季（2023）全10集 内封简英字幕 4K+1080P"
        ))
        self.assertTrue(scraper._is_generic_season_member_label(
            "第七季（2023）全10集 内封简英字幕 4K+1080P"
        ))
        self.assertFalse(scraper._is_generic_season_member_label(
            "瑞克和莫蒂：日漫版（2024）全10集"
        ))
        with mock.patch.object(scraper, "auto_match_tmdb", side_effect=fake_match):
            plan = scraper.build_batch_plan(
                alist, tmdb, src_path=root, parent_path="/library"
            )

        by_source = {item.source_path: item for item in plan.files}
        main_first = by_source[f"{main_root}/01.mkv"]
        anime_first = by_source[f"{anime_root}/01.mkv"]
        self.assertIn("S07E01", main_first.final_name)
        self.assertTrue(main_first.target_dir.endswith("/Season 07"))
        self.assertIn("S01E01", anime_first.final_name)
        self.assertTrue(anime_first.target_dir.endswith("/Season 01"))
        self.assertIn("日漫版", anime_first.target_dir)
        self.assertNotEqual(main_first.target_dir, anime_first.target_dir)
        canonical_root = "/library/瑞克和莫蒂"
        self.assertEqual(plan.target_root, canonical_root)
        self.assertEqual(main_first.target_dir, f"{canonical_root}/Season 07")
        self.assertEqual(
            anime_first.target_dir,
            f"{canonical_root}/瑞克和莫蒂：日漫版/Season 01",
        )
        self.assertEqual(
            sorted(plan.metadata["member_tv"]),
            [canonical_root, f"{canonical_root}/瑞克和莫蒂：日漫版"],
        )
        self.assertTrue(queries)
        main_queries = [query for query in queries if "日漫版" not in query]
        self.assertTrue(main_queries)
        self.assertIn("瑞克和MD", main_queries[0])

    def test_flat_two_movie_batch_independently_matches_each_video(self):
        files = [
            {
                "name": "君を愛したひとりの僕へ.mkv",
                "full_path": "/src/君を愛したひとりの僕へ.mkv",
                "size": 9_345_083_403,
            },
            {
                "name": "僕が愛したすべての君へ.mkv",
                "full_path": "/src/僕が愛したすべての君へ.mkv",
                "size": 10_378_264_249,
            },
        ]
        alist = FakeAList(files)
        tmdb = FakeTMDB({
            "/movie/1": {
                "title": "致我深爱的每个你",
                "release_date": "2022-10-07",
                "poster_path": None,
                "backdrop_path": None,
            },
            "/movie/2": {
                "title": "致深爱你的那个我",
                "release_date": "2022-10-07",
                "poster_path": None,
                "backdrop_path": None,
            },
        })

        def match_each(_client, member_root, _source_files, **_kwargs):
            if "君を愛したひとりの僕へ" in member_root:
                return scraper.AutoMatch("movie", 1, "致我深爱的每个你", "2022", 1.0)
            return scraper.AutoMatch("movie", 2, "致深爱你的那个我", "2022", 1.0)

        with mock.patch.object(scraper, "_franchise_member_match", side_effect=match_each):
            plan = scraper.build_batch_plan(
                alist,
                tmdb,
                src_path="/src",
                parent_path="/library",
            )

        self.assertEqual(plan.mode, "batch")
        self.assertEqual({item.source_path for item in plan.files}, {
            "/src/君を愛したひとりの僕へ.mkv",
            "/src/僕が愛したすべての君へ.mkv",
        })
        self.assertEqual(
            {identity["tmdb_id"] for identity in plan.metadata["member_movies"].values()},
            {1, 2},
        )
        self.assertTrue(any("2 个独立电影文件" in warning for warning in plan.warnings))

    def test_flat_two_movie_batch_rejects_duplicate_tmdb_identity(self):
        files = [
            {"name": "Movie A.mkv", "full_path": "/src/Movie A.mkv"},
            {"name": "Movie A alternate.mkv", "full_path": "/src/Movie A alternate.mkv"},
        ]
        duplicate = scraper.AutoMatch("movie", 1, "Movie A", "2022", 1.0)
        with mock.patch.object(
            scraper, "_franchise_member_match", return_value=duplicate
        ):
            with self.assertRaisesRegex(scraper.PlanError, "同一 TMDB 电影"):
                scraper.build_batch_plan(
                    FakeAList(files),
                    FakeTMDB({
                        "/movie/1": {
                            "title": "Movie A",
                            "release_date": "2022-01-01",
                            "poster_path": None,
                            "backdrop_path": None,
                        }
                    }),
                    src_path="/src",
                    parent_path="/library",
                )

    def test_flat_multi_work_batch_rejects_tv_identity(self):
        files = [
            {"name": "Movie A.mkv", "full_path": "/src/Movie A.mkv"},
            {"name": "Show B.mkv", "full_path": "/src/Show B.mkv"},
        ]
        television = scraper.AutoMatch("tv", 2, "Show B", "2022", 1.0)
        with mock.patch.object(
            scraper, "_franchise_member_match", return_value=television
        ):
            with self.assertRaisesRegex(scraper.PlanError, "必须独立确认成电影"):
                scraper.build_batch_plan(
                    FakeAList(files),
                    FakeTMDB({}),
                    src_path="/src",
                    parent_path="/library",
                )

    def test_single_tmdb_season_packs_multiple_reset_source_cours_in_order(self):
        files = [
            {"name": "Show [01].mkv", "full_path": "/src/Show S1/Show [01].mkv"},
            {"name": "Show [02].mkv", "full_path": "/src/Show S1/Show [02].mkv"},
            {"name": "Show S2 [01].mkv", "full_path": "/src/Show S2/Show S2 [01].mkv"},
            {"name": "Show S2 [02].mkv", "full_path": "/src/Show S2/Show S2 [02].mkv"},
        ]
        tmdb = FakeTMDB({
            "/tv/1": {
                "name": "Show",
                "first_air_date": "2020-01-01",
                "seasons": [{"season_number": 1, "episode_count": 4, "name": "Show"}],
            },
            "/tv/1/season/1": {
                "episodes": [
                    {"episode_number": number, "name": f"Episode {number}"}
                    for number in range(1, 5)
                ]
            },
            "/tv/1/season/0": {"episodes": []},
        })
        plan = scraper.build_tv_plan_smart(
            auto_episode_mode=True,
            alist=FakeAList(files),
            tmdb_client=tmdb,
            src_path="/src",
            parent_path="/library",
            tmdb_id=1,
            season=1,
            absolute=False,
            prefer_simplified=True,
            allow_unmapped=False,
            episode_map_path=None,
            episode_group_id=None,
        )
        self.assertEqual(
            [item.final_name for item in plan.files],
            [
                "Show - S01E01 - Episode 1.mkv",
                "Show - S01E02 - Episode 2.mkv",
                "Show - S01E03 - Episode 3.mkv",
                "Show - S01E04 - Episode 4.mkv",
            ],
        )

    def test_nonexistent_second_season_can_be_independent_sequel_work(self):
        files = [
            {
                "name": "White Album S1 [01].mkv",
                "full_path": "/src/第一季/White Album S1 [01].mkv",
            },
            {
                "name": "White Album 2 [01].mkv",
                "full_path": "/src/第二季/White Album 2 [01].mkv",
            },
        ]

        class SequelTMDB(FakeTMDB):
            def get(self, path, **params):
                if path == "/search/tv":
                    query = str(params.get("query") or "")
                    if "White Album 2" in query:
                        return {
                            "results": [{
                                "id": 2,
                                "name": "White Album 2",
                                "genre_ids": [16],
                            }]
                        }
                    return {"results": []}
                if path == "/search/movie":
                    return {"results": []}
                return super().get(path, **params)

        tmdb = SequelTMDB({
            "/tv/1": {
                "name": "White Album",
                "first_air_date": "2009-01-01",
                "seasons": [
                    {
                        "season_number": 1,
                        "episode_count": 1,
                        "name": "White Album",
                    },
                    {
                        "season_number": 2,
                        "episode_count": 1,
                        "name": "White Album Second Season",
                    },
                ],
            },
            "/tv/1/season/1": {
                "episodes": [{"episode_number": 1, "name": "Episode 1"}]
            },
            "/tv/1/season/0": {"episodes": []},
            "/tv/2": {
                "name": "White Album 2",
                "first_air_date": "2013-10-05",
                "seasons": [{
                    "season_number": 1,
                    "episode_count": 1,
                    "name": "White Album 2",
                }],
            },
            "/tv/2/season/1": {
                "episodes": [{"episode_number": 1, "name": "Episode 1"}]
            },
            "/tv/2/season/0": {"episodes": []},
        })
        plan = scraper.build_tv_plan_smart(
            auto_episode_mode=True,
            alist=FakeAList(files),
            tmdb_client=tmdb,
            src_path="/src",
            parent_path="/library",
            tmdb_id=1,
            season=1,
            absolute=False,
            prefer_simplified=True,
            allow_unmapped=False,
            episode_map_path=None,
            episode_group_id=None,
        )
        self.assertEqual(plan.mode, "batch")
        self.assertEqual(
            {item.source_path: item.final_name for item in plan.files},
            {
                files[0]["full_path"]: "White Album - S01E01 - Episode 1.mkv",
                files[1]["full_path"]: "White Album 2 - S01E01 - Episode 1.mkv",
            },
        )
        self.assertTrue(any(
            identity["tmdb_id"] == 2
            for identity in plan.metadata["member_tv"].values()
        ))
        by_source = {item.source_path: item for item in plan.files}
        self.assertEqual(
            by_source[files[0]["full_path"]].target_dir,
            "/library/White Album/Season 01",
        )
        self.assertEqual(
            by_source[files[1]["full_path"]].target_dir,
            "/library/White Album/White Album 2/Season 01",
        )
        self.assertEqual(
            {
                (identity["tmdb_id"], root)
                for root, identity in plan.metadata["member_tv"].items()
            },
            {
                (1, "/library/White Album"),
                (2, "/library/White Album/White Album 2"),
            },
        )

    def test_numbered_split_movie_uses_parts_instead_of_fake_versions(self):
        files = []
        for number in range(1, 5):
            files.extend([
                {
                    "name": f"Movie [{number:02d}][Ma10p_2160p].mkv",
                    "full_path": f"/src/Movie [{number:02d}][Ma10p_2160p].mkv",
                    "size": 5_000_000_000 - number * 100_000_000,
                },
                {
                    "name": f"Movie [{number:02d}][Ma10p_2160p].ass",
                    "full_path": f"/src/subs/Movie [{number:02d}][Ma10p_2160p].ass",
                    "size": 100_000 + number,
                },
            ])
        tmdb = FakeTMDB({
            "/movie/99": {
                "title": "Movie",
                "release_date": "2022-01-01",
                "poster_path": None,
                "backdrop_path": None,
            }
        })
        plan = scraper.build_movie_plan(
            FakeAList(files),
            tmdb,
            src_path="/src",
            parent_path="/library",
            tmdb_id=99,
            source_files=files,
            defer_validation=True,
        )
        self.assertEqual(
            {item.final_name for item in plan.files},
            {
                "Movie (2022) - part1.mkv",
                "Movie (2022) - part1.subtitle.ass",
                "Movie (2022) - part2.mkv",
                "Movie (2022) - part2.subtitle.ass",
                "Movie (2022) - part3.mkv",
                "Movie (2022) - part3.subtitle.ass",
                "Movie (2022) - part4.mkv",
                "Movie (2022) - part4.subtitle.ass",
            },
        )
        self.assertFalse(plan.cleanup_files)
        self.assertTrue(any("part1-part4" in warning for warning in plan.warnings))

    def test_numbered_split_movie_quality_preference_is_scoped_per_part(self):
        files = [
            {
                "name": "Movie [01][2160p].mkv",
                "full_path": "/src/4K/Movie [01][2160p].mkv",
                "size": 5_000_000_000,
            },
            {
                "name": "Movie [01][1080p].mkv",
                "full_path": "/src/1080P/Movie [01][1080p].mkv",
                "size": 2_000_000_000,
            },
            {
                "name": "Movie [02][1080p].mkv",
                "full_path": "/src/1080P/Movie [02][1080p].mkv",
                "size": 1_800_000_000,
            },
        ]
        plan = scraper.build_movie_plan(
            FakeAList(files),
            FakeTMDB({
                "/movie/99": {
                    "title": "Movie",
                    "release_date": "2022-01-01",
                    "poster_path": None,
                    "backdrop_path": None,
                }
            }),
            src_path="/src",
            parent_path="/library",
            tmdb_id=99,
            source_files=files,
            defer_validation=True,
        )
        self.assertEqual(
            {item.original_name for item in plan.files},
            {"Movie [01][2160p].mkv", "Movie [02][1080p].mkv"},
        )
        self.assertEqual(
            [item.original_name for item in plan.cleanup_files],
            ["Movie [01][1080p].mkv"],
        )

    def test_numbered_split_movie_reports_missing_segment_without_deleting_parts(self):
        files = [
            {"name": "Movie [01][2160p].mkv", "full_path": "/src/Movie [01][2160p].mkv", "size": 5_000},
            {"name": "Movie [03][2160p].mkv", "full_path": "/src/Movie [03][2160p].mkv", "size": 4_000},
        ]
        plan = scraper.build_movie_plan(
            FakeAList(files),
            FakeTMDB({"/movie/99": {
                "title": "Movie", "release_date": "2022-01-01",
                "poster_path": None, "backdrop_path": None,
            }}),
            src_path="/src", parent_path="/library", tmdb_id=99,
            source_files=files, defer_validation=True,
        )
        self.assertEqual(
            {item.final_name for item in plan.files},
            {"Movie (2022) - part1.mkv", "Movie (2022) - part3.mkv"},
        )
        self.assertFalse(plan.cleanup_files)
        self.assertEqual(
            plan.scan_report["resource_gaps"],
            [{
                "kind": "missing_multipart_segment",
                "label": "Movie (2022) - part2",
                "reason": "同一电影的源分段编号不连续，该分段缺失",
                "files": [
                    "/src/Movie [01][2160p].mkv",
                    "/src/Movie [03][2160p].mkv",
                ],
            }],
        )

    def test_same_movie_keeps_4k_and_cleans_lower_resolution_duplicate(self):
        files = [
            {
                "name": "Movie.2160p.mkv",
                "full_path": "/src/4K/Movie.2160p.mkv",
            },
            {
                "name": "Movie.1080p.mkv",
                "full_path": "/src/1080P/Movie.1080p.mkv",
            },
        ]
        plan = scraper.build_movie_plan(
            FakeAList(files),
            FakeTMDB(
                {
                    "/movie/99": {
                        "title": "Movie",
                        "release_date": "2022-01-01",
                        "poster_path": None,
                        "backdrop_path": None,
                    }
                }
            ),
            src_path="/src",
            parent_path="/library",
            tmdb_id=99,
            source_files=files,
            defer_validation=True,
        )
        self.assertEqual(
            [item.original_name for item in plan.files],
            ["Movie.2160p.mkv"],
        )
        self.assertEqual(
            [item.original_name for item in plan.cleanup_files],
            ["Movie.1080p.mkv"],
        )
        self.assertIn("更高清晰度", plan.cleanup_files[0].reason)
        self.assertFalse(plan.problem_files)

    def test_release_episode_bracket_beats_numeric_sequel_title(self):
        self.assertEqual(
            scraper.extract_episode_key("White Album 2 [01][Ma10p_2160p].mkv"),
            scraper.EpisodeKey("regular", 1),
        )
        self.assertEqual(
            scraper.extract_episode_key(
                "Gekijouban Soushuuhen OVERLORD [02(Fushisha no Ou)].mkv"
            ),
            scraper.EpisodeKey("regular", 2),
        )
        self.assertIsNone(
            scraper.extract_episode_key(
                "Fullmetal Alchemist The Sacred Star (BD 1920x1080 x.264 5Audio).ass"
            )
        )

    def test_smart_mode_auto_matches_untagged_movie_folder(self):
        files = [
            {"name": "Show.S01E01.mkv", "full_path": "/src/第一季/Show.S01E01.mkv"},
            {"name": "Show.S02E01.mkv", "full_path": "/src/第二季/Show.S02E01.mkv"},
            {
                "name": "来自深渊：出发的黎明.Made.in.Abyss.Journeys.Dawn.2019.BD1080P.mp4",
                "full_path": "/src/剧场版3部/来自深渊：出发的黎明.Made.in.Abyss.Journeys.Dawn.2019.BD1080P.mp4",
            },
            {
                "name": "来自深渊：出发的黎明.Made.in.Abyss.Journeys.Dawn.2019.ass",
                "full_path": "/src/备份字幕/剧场版/来自深渊：出发的黎明.Made.in.Abyss.Journeys.Dawn.2019.ass",
            },
        ]
        tmdb = FakeTMDB(
            {
                "/tv/1": {"name": "Show", "first_air_date": "2020-01-01", "seasons": [{"season_number": 1}, {"season_number": 2}]},
                "/tv/1/season/1": {"episodes": [{"episode_number": 1, "name": "One"}]},
                "/tv/1/season/2": {"episodes": [{"episode_number": 1, "name": "Two"}]},
                "/tv/1/season/0": {"episodes": []},
                "/search/movie": {"results": [{"id": 99, "title": "来自深渊：出发的黎明", "release_date": "2019-01-04"}]},
                "/movie/99": {"title": "来自深渊：出发的黎明", "release_date": "2019-01-04", "poster_path": None, "backdrop_path": None},
            }
        )
        plan = scraper.build_tv_plan_smart(
            auto_episode_mode=True,
            alist=FakeAList(files),
            tmdb_client=tmdb,
            src_path="/src",
            parent_path="/library",
            tmdb_id=1,
            season=1,
            absolute=False,
            prefer_simplified=True,
            allow_unmapped=False,
            episode_map_path=None,
            episode_group_id=None,
        )
        self.assertEqual(plan.mode, "mixed")
        self.assertTrue(any("出发的黎明 (2019).mp4" in item.final_name for item in plan.files))
        self.assertTrue(any(
            item.source_path.endswith(".ass")
            and "出发的黎明 (2019)" in item.final_name
            and item.target_dir == "/library/来自深渊：出发的黎明 (2019)"
            for item in plan.files
        ))
        self.assertTrue(any(
            "独立电影" in warning and "并列的独立电影目录" in warning
            for warning in plan.warnings
        ))

    def test_real_case_movies_split_even_without_explicit_season_directory(self):
        files = [
            {"name": "Show [01].mkv", "full_path": "/src/Show [01].mkv"},
            {"name": "Show Movie.mkv", "full_path": "/src/剧场版/Show Movie.mkv"},
        ]
        tmdb = FakeTMDB({
            "/tv/1": {"name": "Show", "first_air_date": "2020-01-01", "seasons": [{"season_number": 1}]},
            "/tv/1/season/1": {"episodes": [{"episode_number": 1, "name": "One"}]},
            "/tv/1/season/0": {"episodes": []},
            "/search/movie": {"results": [{"id": 99, "title": "Show Movie", "release_date": "2021-01-01"}]},
            "/movie/99": {"title": "Show Movie", "release_date": "2021-01-01", "poster_path": None},
        })
        plan = scraper.build_tv_plan_smart(
            auto_episode_mode=True, alist=FakeAList(files), tmdb_client=tmdb,
            src_path="/src", parent_path="/library", tmdb_id=1, season=1,
            absolute=False, prefer_simplified=True, allow_unmapped=False,
            episode_map_path=None, episode_group_id=None,
        )
        self.assertEqual(plan.mode, "mixed")
        self.assertEqual(len(plan.files), 2)
        self.assertTrue(any(
            item.target_dir == "/library/Show Movie (2021)"
            and item.final_name == "Show Movie (2021).mkv"
            for item in plan.files
        ))

    def test_numbered_cross_script_side_story_without_aliases_stays_in_review(self):
        files = [
            {"name": "Main.S01E01.mkv", "full_path": "/src/Season 1/Main.S01E01.mkv"},
            {"name": "Main.S02E01.mkv", "full_path": "/src/Season 2/Main.S02E01.mkv"},
            {"name": "DATE A BULLET [01].mkv", "full_path": "/src/赤黑新章/DATE A BULLET [01].mkv"},
            {"name": "DATE A BULLET [02].mkv", "full_path": "/src/赤黑新章/DATE A BULLET [02].mkv"},
            {"name": "DATE A BULLET [01].ass", "full_path": "/src/备份字幕/外传/DATE A BULLET [01].ass"},
            {"name": "DATE A BULLET [02].ass", "full_path": "/src/备份字幕/外传/DATE A BULLET [02].ass"},
        ]
        tmdb = FakeTMDB({
            "/tv/1": {"name": "Main", "first_air_date": "2020-01-01", "seasons": [{"season_number": 1}, {"season_number": 2}]},
            "/tv/1/season/1": {"episodes": [{"episode_number": 1, "name": "One"}]},
            "/tv/1/season/2": {"episodes": [{"episode_number": 1, "name": "Two"}]},
            "/tv/1/season/0": {"episodes": []},
            "/search/tv": {"results": []},
            "/search/movie": {"results": [
                {"id": 723343, "title": "红或白篇", "release_date": "2020-11-13"},
                {"id": 685099, "title": "虚或实篇", "release_date": "2020-08-14"},
            ]},
            "/movie/685099": {"title": "虚或实篇", "release_date": "2020-08-14", "poster_path": None},
            "/movie/723343": {"title": "红或白篇", "release_date": "2020-11-13", "poster_path": None},
        })
        with self.assertRaisesRegex(scraper.PlanError, "无法识别多季度目录中的季度编号"):
            scraper.build_tv_plan_smart(
                auto_episode_mode=True, alist=FakeAList(files), tmdb_client=tmdb,
                src_path="/src", parent_path="/library", tmdb_id=1, season=1,
                absolute=False, prefer_simplified=True, allow_unmapped=False,
                episode_map_path=None, episode_group_id=None,
            )

    def test_numbered_movie_duology_routes_exact_backup_subtitles(self):
        files = [
            {
                "name": f"[TUDO&Ygm] DATE A BULLET [{number:02d}][Ma10p_2160p].mkv",
                "full_path": (
                    "/src/约会大作战 赤黑新章/"
                    f"[TUDO&Ygm] DATE A BULLET [{number:02d}][Ma10p_2160p].mkv"
                ),
            }
            for number in (1, 2)
        ] + [
            {
                "name": f"[TUDO] DATE A BULLET [{number:02d}][Ma10p_2160p].ass",
                "full_path": (
                    "/src/备份字幕/外传/"
                    f"[TUDO] DATE A BULLET [{number:02d}][Ma10p_2160p].ass"
                ),
            }
            for number in (1, 2)
        ] + [
            {
                "name": f"Date A Live S0{season}E01.mkv",
                "full_path": (
                    f"/src/Season {season}/Date A Live S0{season}E01.mkv"
                ),
            }
            for season in (1, 2)
        ]
        movie_results = [
            {
                "id": 685099,
                "title": "约会大作战 赤黑新章：虚或实",
                "release_date": "2020-08-14",
                "genre_ids": [16],
            },
            {
                "id": 723343,
                "title": "约会大作战 赤黑新章：红或白",
                "release_date": "2020-11-13",
                "genre_ids": [16],
            },
        ]
        tmdb = FakeTMDB({
            "/tv/1": {
                "name": "约会大作战",
                "first_air_date": "2013-04-06",
                "seasons": [
                    {"season_number": 1, "episode_count": 1, "air_date": "2013-04-06"},
                    {"season_number": 2, "episode_count": 1, "air_date": "2014-04-12"},
                ],
            },
            "/tv/1/season/1": {"episodes": [{"episode_number": 1, "name": "One"}]},
            "/tv/1/season/2": {"episodes": [{"episode_number": 1, "name": "Two"}]},
            "/tv/1/season/0": {"episodes": []},
            "/search/tv": {"results": []},
            "/search/movie": {"results": movie_results},
            "/movie/685099/alternative_titles": {
                "titles": [{"title": "DATE A BULLET"}],
            },
            "/movie/723343/alternative_titles": {
                "titles": [{"title": "DATE A BULLET"}],
            },
            "/movie/685099": {
                "title": "约会大作战 赤黑新章：虚或实",
                "release_date": "2020-08-14",
                "poster_path": None,
            },
            "/movie/723343": {
                "title": "约会大作战 赤黑新章：红或白",
                "release_date": "2020-11-13",
                "poster_path": None,
            },
        })

        plan = scraper.build_tv_plan_smart(
            auto_episode_mode=True,
            alist=FakeAList(files), tmdb_client=tmdb,
            src_path="/src", parent_path="/library", tmdb_id=1, season=1,
            absolute=False, prefer_simplified=True, allow_unmapped=False,
            episode_map_path=None, episode_group_id=None,
        )

        self.assertEqual(
            plan.mode,
            "mixed",
            msg=(plan.warnings, plan.metadata, [item.source_path for item in plan.files]),
        )
        by_source = {item.source_path: item for item in plan.files}
        for number, movie_id in ((1, 685099), (2, 723343)):
            video_path = files[number - 1]["full_path"]
            subtitle_path = files[number + 1]["full_path"]
            self.assertIn(video_path, by_source)
            self.assertIn(subtitle_path, by_source)
            self.assertEqual(
                by_source[subtitle_path].target_dir,
                by_source[video_path].target_dir,
            )
            self.assertEqual(plan.metadata["member_movies"][
                by_source[video_path].target_dir
            ]["tmdb_id"], movie_id)
        warning = next(
            warning for warning in plan.warnings
            if "官方标题/别名逐一一致" in warning
            and "上映日期顺序" in warning
        )
        scraper.finalize_plan_evidence(plan)
        notice = next(item for item in plan.notices if item.message == warning)
        self.assertFalse(notice.requires_review)

    def test_explicit_unpublished_season_is_retained_for_review(self):
        files = [
            {
                "name": "Show S01E01.mkv",
                "full_path": "/src/Show 第一季/Show S01E01.mkv",
            },
            {
                "name": "Show 4nd Season [01].mkv",
                "full_path": "/src/Show 第四季/Show 4nd Season [01].mkv",
            },
        ]
        tmdb = FakeTMDB({
            "/tv/1": {
                "name": "Show",
                "first_air_date": "2020-01-01",
                "seasons": [{"season_number": 1, "episode_count": 1}],
            },
            "/tv/1/season/1": {
                "episodes": [{"episode_number": 1, "name": "One"}],
            },
            "/tv/1/season/0": {"episodes": []},
            "/search/tv": {"results": []},
            "/search/movie": {"results": []},
        })

        plan = scraper.build_tv_plan_smart(
            auto_episode_mode=True,
            alist=FakeAList(files),
            tmdb_client=tmdb,
            src_path="/src",
            parent_path="/library",
            tmdb_id=1,
            season=1,
            absolute=False,
            prefer_simplified=True,
            allow_unmapped=False,
            episode_map_path=None,
            episode_group_id=None,
        )

        self.assertEqual([item.source_path for item in plan.files], [files[0]["full_path"]])
        self.assertEqual(
            [item.source_path for item in plan.problem_files],
            [files[1]["full_path"]],
        )
        self.assertIn("TMDB 当前尚未发布", plan.problem_files[0].reason)

    def test_smart_special_title_match_retains_unrelated_unnumbered_video(self):
        files = [
            {"name": "Show.S01E01.mkv", "full_path": "/src/Show.S01E01.mkv"},
            {"name": "unrelated.mkv", "full_path": "/src/Specials/unrelated.mkv"},
        ]
        tmdb = FakeTMDB(
            {
                "/tv/1": {
                    "name": "Show",
                    "first_air_date": "2020-01-01",
                    "seasons": [{"season_number": 1}],
                },
                "/tv/1/season/1": {
                    "episodes": [{"episode_number": 1, "name": "Pilot"}]
                },
                "/tv/1/season/0": {
                    "episodes": [{"episode_number": 1, "name": "Official Special"}]
                },
            }
        )
        plan = scraper.build_tv_plan_smart(
            auto_episode_mode=True,
            alist=FakeAList(files),
            tmdb_client=tmdb,
            src_path="/src",
            parent_path="/library",
            tmdb_id=1,
            season=1,
            absolute=False,
            prefer_simplified=True,
            allow_unmapped=False,
            episode_map_path=None,
            episode_group_id=None,
        )
        self.assertEqual(len(plan.files), 1)
        self.assertEqual(
            [item.source_path for item in plan.problem_files],
            ["/src/Specials/unrelated.mkv"],
        )

    def test_unnumbered_orphan_special_subtitle_is_retained_without_blocking(self):
        files = [
            {"name": "Show.S01E01.mkv", "full_path": "/src/Show.S01E01.mkv"},
            {"name": "[SP][Unknown_Drama].ass", "full_path": "/src/备份字幕/[SP][Unknown_Drama].ass"},
        ]
        tmdb = FakeTMDB(
            {
                "/tv/1": {
                    "name": "Show",
                    "first_air_date": "2020-01-01",
                    "seasons": [{"season_number": 1}],
                },
                "/tv/1/season/1": {"episodes": [{"episode_number": 1, "name": "Pilot"}]},
                "/tv/1/season/0": {"episodes": [{"episode_number": 1, "name": "Official Special"}]},
            }
        )
        plan = scraper.build_tv_plan_smart(
            auto_episode_mode=True,
            alist=FakeAList(files),
            tmdb_client=tmdb,
            src_path="/src",
            parent_path="/library",
            tmdb_id=1,
            season=1,
            absolute=False,
            prefer_simplified=True,
            allow_unmapped=False,
            episode_map_path=None,
            episode_group_id=None,
        )
        self.assertEqual(len(plan.files), 1)
        self.assertTrue(any(
            "保留原位待人工确认" in warning
            for warning in plan.warnings
        ))

    def test_smart_mode_maps_season_ova_overflow_to_official_specials(self):
        files = [
            {"name": "Show.S05E11.mkv", "full_path": "/src/Season 5/Show.S05E11.mkv"},
            {"name": "Show.S05E12.mkv", "full_path": "/src/Season 5/Show.S05E12.mkv"},
            {"name": "Show.S05E13.mkv", "full_path": "/src/Season 5/Show.S05E13.mkv"},
            {"name": "Show.S06E01.mkv", "full_path": "/src/Season 6/Show.S06E01.mkv"},
        ]
        tmdb = FakeTMDB(
            {
                "/tv/1": {
                    "name": "Show",
                    "first_air_date": "2020-01-01",
                    "poster_path": None,
                    "backdrop_path": None,
                    "seasons": [{"season_number": 5}, {"season_number": 6}],
                },
                "/tv/1/season/5": {
                    "episodes": [{"episode_number": number, "name": str(number)} for number in range(1, 12)]
                },
                "/tv/1/season/6": {"episodes": [{"episode_number": 1, "name": "One"}]},
                "/tv/1/season/0": {
                    "episodes": [
                        {"episode_number": 8, "name": "第五季OVA1：One"},
                        {"episode_number": 9, "name": "第五季OVA2：Two"},
                    ]
                },
            }
        )
        plan = scraper.build_tv_plan_smart(
            auto_episode_mode=True,
            alist=FakeAList(files),
            tmdb_client=tmdb,
            src_path="/src",
            parent_path="/library",
            tmdb_id=1,
            season=1,
            absolute=False,
            prefer_simplified=True,
            allow_unmapped=False,
            episode_map_path=None,
            episode_group_id=None,
        )
        targets = {item.final_name for item in plan.files}
        self.assertTrue(any("S00E08" in name for name in targets))
        self.assertTrue(any("S00E09" in name for name in targets))
        self.assertTrue(any("超出第 5 季" in warning for warning in plan.warnings))
        self.assertEqual(plan.problem_files, [])

    def test_unlabelled_single_episode_overflow_uses_official_air_date_special(self):
        files = [
            {
                "name": "Show II [11].mkv",
                "full_path": "/src/第二季/Show II [11].mkv",
            }
        ]
        tmdb = FakeTMDB(
            {
                "/tv/1": {
                    "name": "Show",
                    "first_air_date": "2013-01-01",
                    "seasons": [
                        {
                            "season_number": 2,
                            "episode_count": 10,
                            "air_date": "2014-04-01",
                            "name": "Show II",
                        }
                    ],
                },
                "/tv/1/season/2": {
                    "episodes": [
                        {
                            "episode_number": number,
                            "name": str(number),
                            "air_date": f"2014-04-{number:02d}",
                        }
                        for number in range(1, 11)
                    ]
                },
                "/tv/1/season/0": {
                    "episodes": [
                        {
                            "episode_number": 2,
                            "name": "Show II OVA 第11话",
                            "air_date": "2014-12-09",
                        }
                    ]
                },
            }
        )
        plan = scraper.build_tv_plan_smart(
            auto_episode_mode=True,
            alist=FakeAList(files),
            tmdb_client=tmdb,
            src_path="/src",
            parent_path="/library",
            tmdb_id=1,
            season=1,
            absolute=False,
            prefer_simplified=True,
            allow_unmapped=False,
            episode_map_path=None,
            episode_group_id=None,
        )
        self.assertIn("S00E02", plan.files[0].final_name)
        self.assertEqual(plan.problem_files, [])

    def test_explicit_ova_folder_maps_overflow_by_tmdb_air_date_window(self):
        files = [
            {
                "name": "[Group] Show [13].mkv",
                "full_path": "/src/S01-S03+OVA/Season 1/[Group] Show [13].mkv",
            }
        ]
        tmdb = FakeTMDB(
            {
                "/tv/1": {
                    "name": "Show",
                    "first_air_date": "2013-01-01",
                    "poster_path": None,
                    "backdrop_path": None,
                    "seasons": [
                        {"season_number": 1, "name": "Show", "air_date": "2013-01-01"},
                        {"season_number": 2, "name": "Show II", "air_date": "2014-04-01"},
                    ],
                },
                "/tv/1/season/1": {
                    "episodes": [
                        {
                            "episode_number": number,
                            "name": str(number),
                            "air_date": f"2013-03-{number:02d}",
                        }
                        for number in range(1, 13)
                    ]
                },
                "/tv/1/season/0": {
                    "episodes": [
                        {
                            "episode_number": 5,
                            "name": "Encore OVA",
                            "air_date": "2013-12-09",
                        }
                    ]
                },
            }
        )
        plan = scraper.build_tv_plan_smart(
            auto_episode_mode=True,
            alist=FakeAList(files),
            tmdb_client=tmdb,
            src_path="/src",
            parent_path="/library",
            tmdb_id=1,
            season=1,
            absolute=False,
            prefer_simplified=True,
            allow_unmapped=False,
            episode_map_path=None,
            episode_group_id=None,
        )
        self.assertIn("S00E05", plan.files[0].final_name)
        self.assertNotIn("S00E13", plan.files[0].final_name)

    def test_smart_mode_aligns_complete_gapped_subtitle_release_sequence(self):
        files = [
            {"name": "Show.S02E01.mkv", "full_path": "/src/Show.S02E01.mkv"},
            {"name": "Show.S02E02.mkv", "full_path": "/src/Show.S02E02.mkv"},
            {"name": "[Group] Show [03].ass", "full_path": "/src/[Group] Show [03].ass"},
            {"name": "[Group] Show [04].ass", "full_path": "/src/[Group] Show [04].ass"},
        ]
        tmdb = FakeTMDB(
            {
                "/tv/1": {
                    "name": "Show",
                    "first_air_date": "2020-01-01",
                    "seasons": [{"season_number": 2}],
                },
                "/tv/1/season/2": {
                    "episodes": [
                        {"episode_number": 1, "name": "One"},
                        {"episode_number": 2, "name": "Two"},
                    ]
                },
                "/tv/1/season/0": {"episodes": []},
            }
        )
        plan = scraper.build_tv_plan_smart(
            auto_episode_mode=True,
            alist=FakeAList(files),
            tmdb_client=tmdb,
            src_path="/src",
            parent_path="/library",
            tmdb_id=1,
            season=2,
            absolute=False,
            prefer_simplified=True,
            allow_unmapped=False,
            episode_map_path=None,
            episode_group_id=None,
        )
        subtitle_names = [
            item.final_name for item in plan.files if item.media_kind == "subtitle"
        ]
        self.assertEqual(len(subtitle_names), 2)
        self.assertTrue(any("S02E01" in name for name in subtitle_names))
        self.assertTrue(any("S02E02" in name for name in subtitle_names))
        self.assertTrue(any("字幕发布序号" in warning for warning in plan.warnings))

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

    def test_auto_match_rejects_candidates_inside_global_eight_percent_margin(self):
        client = FakeTMDB(
            {
                "/search/tv": {
                    "results": [
                        {"id": 20, "name": "Example Alpha", "genre_ids": [16]},
                        {"id": 21, "name": "Example Alpha", "genre_ids": [16]},
                    ]
                }
            }
        )
        with self.assertRaisesRegex(scraper.PlanError, "前两名证据无法区分"):
            scraper.auto_match_tmdb(
                client,
                "Example Alpha",
                media_type="tv",
                min_confidence=0.88,
            )

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

    def test_canonical_s00_episode_wins_over_descriptive_ova_ordinal(self):
        name = (
            "S00E07 - [FSH subtitle group]To Love-Ru Trouble Darkness - "
            "OVA 01 [BD 1280x720 HEVC x265 10bit FLAC][CHS].mkv"
        )
        expected = scraper.EpisodeKey("special", 7)
        self.assertEqual(scraper.extract_episode_key(name), expected)
        groups = scraper.parse_ep_files([{
            "name": name,
            "full_path": f"/quark/\u5f71\u89c6/ScrapeFlow/\u8865\u6e90/{name}",
        }])
        self.assertIn(expected, groups)
        self.assertNotIn(scraper.EpisodeKey("special", 1), groups)

    def test_season_dash_episode_is_not_treated_as_reverse_range(self):
        name = (
            "[LoliHouse] Youkoso Jitsuryoku Shijou Shugi no Kyoushitsu e "
            "S4 - 01 [WebRip 1080p HEVC-10bit AAC].mkv"
        )
        path = f"/src/{name}"
        self.assertEqual(
            scraper.extract_episode_key(name),
            scraper.EpisodeKey("regular", 1),
        )
        groups = scraper.parse_ep_files([{"name": name, "full_path": path}])
        self.assertIn(scraper.EpisodeKey("regular", 1), groups)

    def test_bare_season_number_dash_episode_uses_episode_number(self):
        name = (
            "[DMG&LoliHouse] Youjitsu 3 - 01 "
            "[WebRip 1080p HEVC-10bit AAC ASSx2].CHS.ass"
        )
        groups = scraper.parse_ep_files(
            [{"name": name, "full_path": f"/src/{name}"}]
        )
        self.assertIn(scraper.EpisodeKey("regular", 1), groups)
        self.assertNotIn(scraper.EpisodeKey("regular", 3), groups)

    def test_fractional_recap_is_not_merged_into_regular_episode(self):
        key = scraper.extract_episode_key(
            "[Sakurato] EIGHTY SIX [18.5v2][1080p].mkv"
        )
        self.assertEqual(
            key,
            scraper.EpisodeKey("fractional", 18, fractional_digits="5"),
        )
        self.assertEqual(key.display, "E18.5")

    def test_fractional_recaps_map_by_unique_official_broadcast_interval(self):
        source = "/tv/Eighty Six"
        files = [
            {
                "name": "[Sakurato] EIGHTY SIX [11.5][1080p].mkv",
                "full_path": f"{source}/[Sakurato] EIGHTY SIX [11.5][1080p].mkv",
            },
            {
                "name": "[Sakurato] EIGHTY SIX [18.5v2][1080p].mkv",
                "full_path": f"{source}/[Sakurato] EIGHTY SIX [18.5v2][1080p].mkv",
            },
        ]
        dates = {
            11: "2021-06-20",
            12: "2021-10-03",
            18: "2021-11-21",
            19: "2021-12-05",
        }
        season_episodes = [
            {
                "episode_number": number,
                "name": f"Episode {number}",
                "air_date": dates.get(number, f"2021-01-{number:02d}"),
                "runtime": 24,
            }
            for number in range(1, 24)
        ]
        specials = [
            {
                "episode_number": 2,
                "name": "总集篇「在战场上绽放的红色虞美人」",
                "air_date": "2021-06-27",
                "runtime": 24,
            },
            {
                "episode_number": 4,
                "name": "总集篇「若是死得其所」",
                "air_date": "2021-11-28",
                "runtime": 24,
            },
        ]
        plan = scraper.build_tv_plan(
            FakeAList(files),
            FakeTMDB(
                {
                    "/tv/100565": {
                        "name": "86-不存在的战区-",
                        "first_air_date": "2021-04-11",
                        "poster_path": None,
                        "seasons": [{"season_number": 0}, {"season_number": 1}],
                    },
                    "/tv/100565/season/1": {"episodes": season_episodes},
                    "/tv/100565/season/0": {"episodes": specials},
                }
            ),
            src_path=source,
            parent_path="/library",
            tmdb_id=100565,
            season=1,
            absolute=False,
            prefer_simplified=True,
            allow_unmapped=False,
        )
        targets = {item.original_name: item.final_name for item in plan.files}
        self.assertIn("S00E02", targets[files[0]["name"]])
        self.assertIn("S00E04", targets[files[1]["name"]])
        self.assertFalse(plan.problem_files)

    def test_fractional_recap_maps_without_requiring_next_episode_date(self):
        files = [
            {"name": "Show.E01.mkv", "full_path": "/src/Show.E01.mkv"},
            {
                "name": "Show Recap [11.5].mkv",
                "full_path": "/src/Show Recap [11.5].mkv",
                "duration_seconds": 24 * 60,
            },
        ]
        responses = {
            "/tv/10": {
                "name": "测试剧",
                "first_air_date": "2020-01-01",
                "poster_path": None,
                "seasons": [{"season_number": 0}, {"season_number": 1}],
            },
        }
        responses["/tv/10/season/1"] = {"episodes": [
            {
                "episode_number": 1,
                "name": "第一集",
                "air_date": "2021-01-01",
                "runtime": 24,
            },
            {
                "episode_number": 11,
                "name": "第十一集",
                "air_date": "2021-03-12",
                "runtime": 24,
            },
            # N+1 exists in the official numbering but its date is absent.
            {"episode_number": 12, "name": "第十二集", "runtime": 24},
        ]}
        responses["/tv/10/season/0"] = {"episodes": [{
            "episode_number": 2,
            "name": "第 1 季总集篇",
            "air_date": "2021-03-19",
            "runtime": 24,
        }]}

        plan = scraper.build_tv_plan(
            FakeAList(files),
            FakeTMDB(responses),
            src_path="/src",
            parent_path="/library",
            tmdb_id=10,
            season=1,
            absolute=False,
            prefer_simplified=False,
            allow_unmapped=False,
        )

        targets = {item.original_name: item.final_name for item in plan.files}
        self.assertIn("S00E02", targets["Show Recap [11.5].mkv"])
        self.assertIn("S01E01", targets["Show.E01.mkv"])
        self.assertFalse(plan.problem_files)
        scraper.finalize_plan_evidence(plan)
        mapped_notice = next(
            notice for notice in plan.notices
            if "E11.5 经 TMDB 官方 Season 00/季度多证据评分" in notice.message
        )
        self.assertFalse(mapped_notice.requires_review)

    def test_unique_timeline_slot_without_semantic_identity_stays_put(self):
        files = [
            {"name": "Show.E01.mkv", "full_path": "/src/Show.E01.mkv"},
            {"name": "Show [11.5].mkv", "full_path": "/src/Show [11.5].mkv"},
        ]
        responses = {
            "/tv/10": {
                "name": "测试剧",
                "first_air_date": "2020-01-01",
                "poster_path": None,
                "seasons": [{"season_number": 0}, {"season_number": 1}],
            },
        }
        responses["/tv/10/season/1"] = {"episodes": [
            {
                "episode_number": 1,
                "name": "第一集",
                "air_date": "2021-01-01",
                "runtime": 24,
            },
            {
                "episode_number": 11,
                "name": "第十一集",
                "air_date": "2021-03-12",
                "runtime": 24,
            },
            {
                "episode_number": 12,
                "name": "第十二集",
                "air_date": "2021-03-26",
                "runtime": 24,
            },
        ]}
        responses["/tv/10/season/0"] = {"episodes": [{
            "episode_number": 2,
            "name": "Ordinary Special",
            "air_date": "2021-03-19",
            "runtime": 24,
        }]}

        plan = scraper.build_tv_plan(
            FakeAList(files),
            FakeTMDB(responses),
            src_path="/src",
            parent_path="/library",
            tmdb_id=10,
            season=1,
            absolute=False,
            prefer_simplified=False,
            allow_unmapped=False,
        )

        self.assertTrue(any("S01E01" in item.final_name for item in plan.files))
        self.assertFalse(any(item.original_name == "Show [11.5].mkv" for item in plan.files))
        self.assertEqual(
            [item.source_path for item in plan.problem_files],
            ["/src/Show [11.5].mkv"],
        )
        self.assertIn("高置信门槛", plan.problem_files[0].reason)

    def test_fractional_semantic_or_runtime_conflict_vetoes_auto_mapping(self):
        cases = [
            (
                "Show Recap [11.5].mkv",
                None,
                "OVA 1",
                24,
                "总集篇，官方标题却是 OVA/OAD",
            ),
            (
                "Show Recap [11.5].mkv",
                60 * 60,
                "第 1 季总集篇",
                24,
                "运行时长",
            ),
            (
                "Show [11.5] Preview.mkv",
                None,
                "第 1 季总集篇",
                24,
                "预告/PV/菜单",
            ),
            (
                "Show Recap [11.5].mkv",
                None,
                "第 2 季总集篇",
                24,
                "其他季度 2",
            ),
            (
                "Show Recap [11.5].mkv",
                None,
                "第 12.5 话总集篇",
                24,
                "其他小数集号 12.5",
            ),
        ]
        for name, duration_seconds, official_title, runtime, expected in cases:
            with self.subTest(name=name, official_title=official_title):
                item = {"name": name, "full_path": f"/src/{name}"}
                if duration_seconds is not None:
                    item["duration_seconds"] = duration_seconds
                responses = {
                    "/tv/10": {
                        "name": "测试剧",
                        "first_air_date": "2020-01-01",
                        "poster_path": None,
                        "seasons": [{"season_number": 0}, {"season_number": 1}],
                    },
                }
                responses["/tv/10/season/1"] = {"episodes": [
                    {
                        "episode_number": 1,
                        "name": "第一集",
                        "air_date": "2021-01-01",
                        "runtime": 24,
                    },
                    {
                        "episode_number": 11,
                        "name": "第十一集",
                        "air_date": "2021-03-12",
                        "runtime": 24,
                    },
                    {
                        "episode_number": 12,
                        "name": "第十二集",
                        "air_date": "2021-03-26",
                        "runtime": 24,
                    },
                ]}
                responses["/tv/10/season/0"] = {"episodes": [{
                    "episode_number": 2,
                    "name": official_title,
                    "air_date": "2021-03-19",
                    "runtime": runtime,
                }]}

                plan = scraper.build_tv_plan(
                    FakeAList([
                        {"name": "Show.E01.mkv", "full_path": "/src/Show.E01.mkv"},
                        item,
                    ]),
                    FakeTMDB(responses),
                    src_path="/src",
                    parent_path="/library",
                    tmdb_id=10,
                    season=1,
                    absolute=False,
                    prefer_simplified=False,
                    allow_unmapped=False,
                )

                self.assertTrue(any("S01E01" in value.final_name for value in plan.files))
                self.assertFalse(any(value.original_name == name for value in plan.files))
                self.assertEqual(len(plan.problem_files), 1)
                self.assertIn("排他冲突", plan.problem_files[0].reason)
                self.assertIn(expected, plan.problem_files[0].reason)

    def test_arbitrary_fractional_episode_is_preserved_without_assuming_special(self):
        cases = {
            "[Group] Show [24.9][1080p].mkv": ("9", "E24.9"),
            "[Group] Show SP 24.9.mkv": ("9", "E24.9"),
            "[Group] Show [24.50].mkv": ("5", "E24.5"),
            "[Group] Show [11.25].mkv": ("25", "E11.25"),
        }
        for name, (digits, display) in cases.items():
            with self.subTest(name=name):
                key = scraper.extract_episode_key(name)
                self.assertEqual(
                    key,
                    scraper.EpisodeKey(
                        "fractional",
                        11 if "11.25" in name else 24,
                        fractional_digits=digits,
                    ),
                )
                self.assertEqual(key.display, display)
        self.assertEqual(
            scraper.extract_episode_key("[Group] Show SP 24.5.mkv"),
            scraper.EpisodeKey("fractional", 24, fractional_digits="5"),
        )

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

    def test_explicit_episode_range_override_is_grouped_as_range(self):
        files = [
            {
                "name": "Show New Edit [02].mkv",
                "full_path": "/src/新编集版/Show New Edit [02].mkv",
                "_episode_kind_override": "regular",
                "_episode_key_override": 2,
                "_episode_end_override": 3,
            }
        ]
        groups = scraper.parse_ep_files(files)
        self.assertIn(scraper.EpisodeKey("regular", 2, 3), groups)

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

    def test_official_movie_title_containing_extra_is_not_filtered_as_bonus(self):
        source = "/movies/10 Future Gospel extra chorus (2013)"
        files = [
            {
                "name": "Future Gospel extra chorus.mkv",
                "full_path": f"{source}/Future Gospel extra chorus.mkv",
            },
            {
                "name": "English.ass",
                "full_path": f"{source}/English.ass",
            },
        ]
        tmdb = FakeTMDB(
            {
                "/movie/10": {
                    "id": 10,
                    "title": "Future Gospel extra chorus",
                    "original_title": "Future Gospel extra chorus",
                    "release_date": "2013-09-28",
                    "poster_path": None,
                }
            }
        )

        plan = scraper.build_movie_plan(
            FakeAList(files),
            tmdb,
            src_path=source,
            parent_path="/movies",
            tmdb_id=10,
        )

        self.assertEqual(
            [item.media_kind for item in plan.files],
            ["subtitle", "video"],
        )

    def test_fullwidth_slash_fate_prototype_has_special_context(self):
        self.assertTrue(
            scraper._has_special_context(
                {
                    "name": "Fate／Prototype.mkv",
                    "full_path": "/media/Carnival Phantasm/Fate／Prototype.mkv",
                }
            )
        )

    def test_director_cut_and_regular_episode_keep_clear_names(self):
        base = "Show - S03E25 - Episode"
        files = [
            {"name": "Show [25(Director' Cut)].mkv", "full_path": "/src/director.mkv"},
            {"name": "Show [25].mkv", "full_path": "/src/regular.mkv"},
            {"name": "Show [25 cut].ass", "full_path": "/src/director.ass"},
            {"name": "Show [25].ass", "full_path": "/src/regular.ass"},
        ]
        names = scraper.make_unique_media_names(base, files, preserve_editions=True)
        self.assertIn(f"{base} {{edition-Director's Cut}}.mkv", names)
        self.assertIn(f"{base}.mkv", names)
        self.assertIn(f"{base} {{edition-Director's Cut}}.subtitle.ass", names)
        self.assertIn(f"{base}.subtitle.ass", names)

    def test_new_edit_folder_is_preserved_as_its_official_edition(self):
        base = "Re：从零开始的异世界生活 - S01E02-E03 - Episode 2 + Episode 3"
        files = [
            {
                "name": "Re Zero Shin Henshuu-ban [02].mkv",
                "full_path": "/src/Re：从零开始的异世界生活 新编集版/"
                "Re Zero Shin Henshuu-ban [02].mkv",
            }
        ]
        self.assertEqual(
            scraper.make_unique_media_names(base, files, preserve_editions=True),
            [f"{base} {{edition-New Edit}}.mkv"],
        )

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

    def test_provider_safe_episode_title_translates_quark_rejected_ova_token(self):
        self.assertEqual(
            scraper.provider_safe_episode_title("OVA1 JACK"),
            "特别篇 1 JACK",
        )
        self.assertEqual(
            scraper.provider_safe_episode_title("OVA02 PINTO"),
            "特别篇 2 PINTO",
        )
        self.assertEqual(
            scraper.provider_safe_episode_title("NOVA 1"),
            "NOVA 1",
        )

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

    def test_mks_subtitle_is_supported(self):
        files = [
            {"name": "Show.E01.zh-CN.mks", "full_path": "/src/Show.E01.zh-CN.mks"},
            {"name": "Show.E01.mkv", "full_path": "/src/Show.E01.mkv"},
        ]
        groups = scraper.parse_ep_files(files)
        names = {item["name"] for item in groups[scraper.EpisodeKey("regular", 1)]}
        self.assertIn("Show.E01.zh-CN.mks", names)

    def test_1080p_after_episode_does_not_corrupt_episode_number(self):
        groups = scraper.parse_ep_files(
            [
                {
                    "name": "Show.S01E01.1080p.mkv",
                    "full_path": "/src/Show.S01E01.1080p.mkv",
                }
            ]
        )
        self.assertIn(scraper.EpisodeKey("regular", 1), groups)
        self.assertNotIn(scraper.EpisodeKey("regular", 0), groups)


class PlanTests(unittest.TestCase):
    def test_mixed_plan_rejects_movie_directly_inside_tv_series_root(self):
        source = "/quark/影视/待刮削/Example"
        tv_root = "/quark/影视/番剧/Family/Example"
        movie_name = "Movie (2026).mkv"
        plan = scraper.Plan(
            mode="mixed", source_root=source, target_root="/quark/影视/番剧/Family",
            files=[scraper.PlannedFile(
                source_path=f"{source}/movie.mkv", source_dir=source,
                original_name="movie.mkv", final_name=movie_name,
                target_dir=tv_root, media_kind="video",
            )],
            warnings=[],
            metadata={
                "tmdb_id": 1, "title": "Example", "year": "2026",
                "series_root": tv_root,
                "member_movies": {
                    f"{tv_root}/{movie_name}": {
                        "tmdb_id": 2, "title": "Movie", "year": "2026",
                    },
                },
            },
        )
        with self.assertRaisesRegex(scraper.PlanError, "独立电影不得直接放入"):
            scraper.validate_plan(FakeAList([]), plan)

    def test_smart_tv_explicit_episode_map_initializes_common_gap_audit(self):
        plan = mock.Mock(target_root="/library/Show", scan_report={})
        alist = object()
        with mock.patch.object(scraper, "build_tv_plan", return_value=plan), mock.patch.object(
            scraper, "_tv_season_resource_gaps", return_value=[]
        ) as gap_audit:
            result = scraper.build_tv_plan_smart(
                auto_episode_mode=True,
                alist=alist,
                episode_map_path=Path("episode-map.json"),
                absolute=False,
                episode_group_id=None,
            )

        self.assertIs(result, plan)
        gap_audit.assert_called_once_with(
            alist, plan, series_dir="/library/Show", official_seasons=[],
        )

    def test_major_gap_long_season_maps_only_complete_aired_release_seasons(self):
        groups = {
            1: [
                {
                    "name": f"Show [{number:02d}].mkv",
                    "full_path": f"/src/S1/{number:02d}.mkv",
                }
                for number in range(1, 5)
            ],
            2: [
                {"name": f"Show [{number:02d}].mkv", "full_path": f"/src/S2/{number:02d}.mkv"}
                for number in range(1, 3)
            ],
            3: [
                {"name": f"Show [{number:02d}].mkv", "full_path": f"/src/S3/{number:02d}.mkv"}
                for number in range(1, 3)
            ],
        }
        episodes = [
            {"episode_number": 1, "air_date": "2020-01-01"},
            {"episode_number": 2, "air_date": "2020-01-08"},
            {"episode_number": 3, "air_date": "2021-01-01"},
            {"episode_number": 4, "air_date": "2021-01-08"},
            {"episode_number": 5, "air_date": "2022-01-01"},
            {"episode_number": 6, "air_date": "2022-01-08"},
            {"episode_number": 7, "air_date": "2022-01-15"},
            {"episode_number": 8, "air_date": "2023-01-01"},
        ]
        warnings = scraper._merge_release_seasons_into_long_tmdb_season_by_major_gaps(
            groups,
            official_season=1,
            official_episodes=episodes,
            today=scraper.date(2022, 1, 10),
        )
        self.assertEqual(set(groups), {1})
        routed = {
            item["full_path"]: item.get("_episode_key_override")
            for item in groups[1]
        }
        self.assertEqual(routed["/src/S2/01.mkv"], 3)
        self.assertEqual(routed["/src/S2/02.mkv"], 4)
        self.assertEqual(routed["/src/S3/01.mkv"], 5)
        self.assertEqual(routed["/src/S3/02.mkv"], 6)
        self.assertTrue(all("未包含未播集" in warning for warning in warnings))

    def test_major_gap_long_season_reuses_proven_cumulative_backup_overrides(self):
        groups = {
            1: [
                {"name": "Show [01].mkv", "full_path": "/src/S1/01.mkv"},
                {"name": "Show [02].mkv", "full_path": "/src/S1/02.mkv"},
            ],
            2: [
                {"name": "Show [01].mkv", "full_path": "/src/S2/local01.mkv"},
                {"name": "Show [02].mkv", "full_path": "/src/S2/local02.mkv"},
                {
                    "name": "Show [03].mkv",
                    "full_path": "/src/S2/cumulative03.mkv",
                    "_episode_key_override": 1,
                },
                {
                    "name": "Show [04].mkv",
                    "full_path": "/src/S2/cumulative04.mkv",
                    "_episode_key_override": 2,
                },
            ],
        }
        warnings = scraper._merge_release_seasons_into_long_tmdb_season_by_major_gaps(
            groups,
            official_season=1,
            official_episodes=[
                {"episode_number": 1, "air_date": "2020-01-01"},
                {"episode_number": 2, "air_date": "2020-01-08"},
                {"episode_number": 3, "air_date": "2021-01-01"},
                {"episode_number": 4, "air_date": "2021-01-08"},
            ],
            today=scraper.date(2021, 2, 1),
        )
        self.assertEqual(set(groups), {1})
        routed = {
            item["full_path"]: item.get("_episode_key_override")
            for item in groups[1]
        }
        self.assertEqual(routed["/src/S2/local01.mkv"], 3)
        self.assertEqual(routed["/src/S2/cumulative03.mkv"], 3)
        self.assertEqual(routed["/src/S2/local02.mkv"], 4)
        self.assertEqual(routed["/src/S2/cumulative04.mkv"], 4)
        self.assertEqual(len(warnings), 1)

    def test_minitodo_2d_3d_and_epilogue_use_unique_official_titles(self):
        items = [
            {"name": "Kimi ni Todoke [Minitodo Gekijou (2D Ver.)].mkv"},
            {"name": "Kimi ni Todoke [Minitodo Gekijou (3D Ver.)].mkv"},
            {"name": "Kimi ni Todoke [Minitodo Gekijou Epilogue].mkv"},
            {"name": "Mini_Todo_Gekijou_Romeo_and_Juliet_3D.ass"},
            {"name": "Mini_Todo_Gekijou_Romeo_and_Juliet_Sorekara.ass"},
        ]
        changed = scraper._map_minitodo_release_editions(
            items,
            {
                4: ["Mini Todoke Theatre: Romeo & Juliet 3D", "罗密欧与朱丽叶 前篇"],
                5: ["Mini Todoke Theatre: Romeo & Juliet, After Story", "罗密欧与朱丽叶 后篇"],
            },
        )
        self.assertEqual(changed, 5)
        self.assertEqual(
            [item["_episode_key_override"] for item in items],
            [4, 4, 5, 4, 5],
        )
        self.assertEqual(scraper.edition_tag(items[1]["name"]), "3D")

    def test_overflow_video_runtime_can_prove_independent_animation_movie(self):
        class RuntimeTMDB:
            def get(self, path, **params):
                if path == "/search/movie":
                    self.assert_query = params.get("query")
                    return {"results": [{"id": 439191}]}
                if path == "/movie/439191":
                    return {
                        "id": 439191,
                        "title": "Assassination Classroom: Jump Festa 2013 Special",
                        "original_title": "暗殺教室 修学旅行編",
                        "runtime": 31,
                        "genres": [{"id": 16, "name": "Animation"}],
                    }
                if path == "/movie/439191/alternative_titles":
                    return {"titles": [{"title": "Ansatsu Kyoushitsu OVA"}]}
                raise AssertionError(path)

        groups = {
            1: [
                {
                    "name": "Ansatsu Kyoushitsu [22].mkv",
                    "full_path": "/src/Season 1/22.mkv",
                },
                {
                    "name": "Ansatsu Kyoushitsu [23].mkv",
                    "full_path": "/src/Season 1/23.mkv",
                },
                {
                    "name": "Ansatsu Kyoushitsu [22].ass",
                    "full_path": "/src/Season 1/22.ass",
                },
            ]
        }
        unknown = [{
            "name": "Ansatsu Kyoushitsu [23].ass",
            "full_path": "/src/备份字幕/23.ass",
        }]
        with mock.patch.object(
            scraper,
            "_probe_remote_duration_minutes",
            return_value=31.75,
        ):
            movies, warnings = scraper._extract_runtime_proven_overflow_movies(
                object(),
                RuntimeTMDB(),
                {"name": "Assassination Classroom", "original_name": "暗殺教室"},
                groups,
                {1: 22},
                {1: 10, 2: 5},
                unknown,
            )
        self.assertEqual(
            [item["full_path"] for item in movies[439191]],
            ["/src/Season 1/23.mkv", "/src/备份字幕/23.ass"],
        )
        self.assertEqual(unknown, [])
        self.assertEqual(
            [item["full_path"] for item in groups[1]],
            ["/src/Season 1/22.mkv", "/src/Season 1/22.ass"],
        )
        self.assertIn("TMDB/439191", warnings[0])

    def test_overflow_runtime_does_not_override_same_length_official_special(self):
        class RuntimeTMDB:
            def get(self, path, **params):
                if path == "/search/movie":
                    return {"results": [{"id": 439191}]}
                if path == "/movie/439191":
                    return {
                        "id": 439191,
                        "title": "Assassination Classroom Special",
                        "original_title": "暗殺教室 特別編",
                        "runtime": 31,
                        "genres": [{"id": 16}],
                    }
                if path.endswith("/alternative_titles"):
                    return {"titles": []}
                raise AssertionError(path)

        groups = {1: [{"name": "Show [23].mkv", "full_path": "/src/23.mkv"}]}
        with mock.patch.object(
            scraper,
            "_probe_remote_duration_minutes",
            return_value=31.0,
        ):
            movies, warnings = scraper._extract_runtime_proven_overflow_movies(
                object(), RuntimeTMDB(),
                {"name": "Assassination Classroom", "original_name": "暗殺教室"},
                groups, {1: 22}, {1: 31},
            )
        self.assertEqual(movies, {})
        self.assertEqual(warnings, [])
        self.assertEqual(len(groups[1]), 1)

    def test_partial_backup_subtitles_attach_only_to_unique_release_episode(self):
        groups = {
            1: [{
                "name": "Ansatsu Kyoushitsu [01].mkv",
                "full_path": "/src/Season 1/01.mkv",
            }],
            2: [{
                "name": "Ansatsu Kyoushitsu 2nd Season [01].mkv",
                "full_path": "/src/Season 2/01.mkv",
            }],
        }
        unknown = [{
            "name": "Ansatsu Kyoushitsu [01].ass",
            "full_path": "/src/备份字幕/01.ass",
        }]
        remaining, attached = scraper._attach_unique_numbered_backup_subtitles(
            unknown, groups, {1: 22, 2: 25},
        )
        self.assertEqual(remaining, [])
        self.assertEqual(attached, 1)
        self.assertEqual(groups[1][-1]["full_path"], "/src/备份字幕/01.ass")
        self.assertEqual(len(groups[2]), 1)
        self.assertEqual(len(groups[1]), 2)

    def test_clannad_backup_subtitles_route_by_longest_unique_release_identity(self):
        groups = {
            "Clannad 2007": [{
                "name": "[Ygm] Clannad 2007 [01][Ma10p_2160p].mkv",
                "full_path": "/src/Clannad 2007/[Ygm] Clannad 2007 [01][Ma10p_2160p].mkv",
            }],
            "Clannad After Story 2008": [{
                "name": "[Ygm] Clannad ~After Story~ 2008 [01][Ma10p_2160p].mkv",
                "full_path": (
                    "/src/Clannad After Story 2008/"
                    "[Ygm] Clannad ~After Story~ 2008 [01][Ma10p_2160p].mkv"
                ),
            }],
        }
        first = {
            "name": "[Ygm] Clannad 2007 [23][Ma10p_2160p][x265_flac_ass].ass",
            "full_path": (
                "/src/备份字幕/"
                "[Ygm] Clannad 2007 [23][Ma10p_2160p][x265_flac_ass].ass"
            ),
        }
        sequel = {
            "name": (
                "[Ygm] Clannad ~After Story~ 2008 [SP01]"
                "[Ma10p_2160p][x265_flac_ass].ass"
            ),
            "full_path": (
                "/src/备份字幕/[Ygm] Clannad ~After Story~ 2008 [SP01]"
                "[Ma10p_2160p][x265_flac_ass].ass"
            ),
        }

        owners = scraper._unique_backup_subtitle_release_owners(
            groups, [first, sequel]
        )

        self.assertEqual(owners[first["full_path"]], "Clannad 2007")
        self.assertEqual(
            owners[sequel["full_path"]], "Clannad After Story 2008"
        )
        self.assertNotEqual(owners[first["full_path"]], owners[sequel["full_path"]])

    def test_clannad_full_length_special_runs_route_by_release_timeline(self):
        items = [
            {
                "name": "Clannad [23].mkv",
                "full_path": "/src/第一季/Clannad [23].mkv",
            },
            {
                "name": "Clannad 2007 [23].ass",
                "full_path": "/src/备份字幕/Clannad 2007 [23].ass",
            },
            {
                "name": "Clannad [24].mkv",
                "full_path": "/src/第一季/Clannad [24].mkv",
            },
            *[
                {
                    "name": f"Clannad After Story 2008 [SP{number:02d}].mkv",
                    "full_path": (
                        f"/src/第二季/Clannad After Story 2008 "
                        f"[SP{number:02d}].mkv"
                    ),
                }
                for number in range(1, 4)
            ],
            {
                "name": "Clannad After Story 2008 [SP01].ass",
                "full_path": (
                    "/src/备份字幕/Clannad After Story 2008 [SP01].ass"
                ),
            },
        ]
        show = {
            "seasons": [
                {
                    "season_number": 1,
                    "name": "CLANNAD",
                    "episode_count": 22,
                    "air_date": "2007-10-05",
                },
                {
                    "season_number": 2,
                    "name": "CLANNAD ~AFTER STORY~",
                    "episode_count": 22,
                    "air_date": "2008-10-03",
                },
            ]
        }
        positive_seasons = show["seasons"]

        changed = scraper._map_disc_extras_by_official_release_runs(
            items,
            show=show,
            positive_seasons=positive_seasons,
            special_runtimes={number: 24 for number in range(1, 6)},
            special_air_dates={
                1: "2008-03-28",
                2: "2008-07-16",
                3: "2009-03-19",
                4: "2009-03-26",
                5: "2009-07-01",
            },
        )

        self.assertEqual(changed, len(items))
        self.assertEqual(
            [item["_episode_key_override"] for item in items],
            [1, 1, 2, 3, 4, 5, 3],
        )
        self.assertTrue(
            all(item["_episode_kind_override"] == "special" for item in items)
        )

    def test_numbered_alternate_subtitle_keeps_the_video_companion_identity(self):
        target = "/library/Show/Season 00"
        self.assertEqual(
            scraper._planned_companion_key(
                target, "Show - S00E04 - Special.subtitle.2.ass"
            ),
            scraper._planned_companion_key(
                target, "Show - S00E04 - Special.mkv"
            ),
        )

    def test_backup_subtitle_shared_alias_without_release_identity_stays_unresolved(self):
        groups = {
            "Clannad 2007": [{
                "name": "Clannad 2007 [01].mkv",
                "full_path": "/src/Clannad 2007/Clannad 2007 [01].mkv",
            }],
            "Clannad After Story 2008": [{
                "name": "Clannad After Story 2008 [01].mkv",
                "full_path": (
                    "/src/Clannad After Story 2008/"
                    "Clannad After Story 2008 [01].mkv"
                ),
            }],
        }
        ambiguous = {
            "name": "Clannad [SP01].ass",
            "full_path": "/src/备份字幕/Clannad [SP01].ass",
        }

        owners = scraper._unique_backup_subtitle_release_owners(
            groups, [ambiguous]
        )

        self.assertNotIn(ambiguous["full_path"], owners)

    def test_runtime_probe_is_skipped_for_multi_overflow_release_runs(self):
        groups = {1: [
            {"name": "Show [13].mkv", "full_path": "/src/13.mkv"},
            {"name": "Show [14].mkv", "full_path": "/src/14.mkv"},
        ]}
        with mock.patch.object(
            scraper,
            "_probe_remote_duration_minutes",
        ) as probe:
            movies, warnings = scraper._extract_runtime_proven_overflow_movies(
                object(), object(), {"name": "Show"}, groups, {1: 12}, {},
            )
        probe.assert_not_called()
        self.assertEqual(movies, {})
        self.assertEqual(warnings, [])

    def test_runtime_probe_is_skipped_when_multiple_seasons_have_one_extra(self):
        groups = {
            1: [{"name": "Show S1 [13].mkv", "full_path": "/src/S1/13.mkv"}],
            2: [{"name": "Show S2 [13].mkv", "full_path": "/src/S2/13.mkv"}],
        }
        with mock.patch.object(
            scraper,
            "_probe_remote_duration_minutes",
        ) as probe:
            movies, warnings = scraper._extract_runtime_proven_overflow_movies(
                object(), object(), {"name": "Show"}, groups, {1: 12, 2: 12}, {},
            )
        probe.assert_not_called()
        self.assertEqual(movies, {})
        self.assertEqual(warnings, [])

    def test_disc_extras_restart_ordinals_per_official_release_season(self):
        items = [
            {
                "name": "Prisma Illya [Tokuten_Anime01].mkv",
                "full_path": "/src/Prisma Illya/Season 01/SPs/Tokuten_Anime01.mkv",
            },
            {
                "name": "Prisma Illya 2wei [Tokuten_Anime01].mkv",
                "full_path": "/src/Prisma Illya/Season 02/SPs/Tokuten_Anime01.mkv",
            },
            {
                "name": "Prisma Illya [OVA].ass",
                "full_path": "/src/字幕备份/Prisma Illya [OVA].ass",
            },
            {
                "name": "Prisma Illya 2wei [OVA].ass",
                "full_path": "/src/字幕备份/Prisma Illya 2wei [OVA].ass",
            },
        ]
        changed = scraper._map_disc_extras_by_official_release_runs(
            items,
            show={
                "seasons": [
                    {"season_number": 1, "name": "魔法少女☆伊莉雅"},
                    {"season_number": 2, "name": "魔法少女☆伊莉雅 2wei"},
                ]
            },
            positive_seasons=[
                {"season_number": 1},
                {"season_number": 2},
            ],
            special_runtimes={1: 5, 2: 5, 3: 24, 4: 5, 5: 5, 6: 26},
            special_air_dates={
                1: "2013-09-01",
                2: "2013-10-01",
                3: "2014-01-01",
                4: "2014-09-01",
                5: "2014-10-01",
                6: "2015-01-01",
            },
        )
        self.assertEqual(changed, 4)
        self.assertEqual(
            [item["_episode_key_override"] for item in items],
            [1, 4, 3, 6],
        )

    def test_disc_extra_mapper_does_not_steal_sequel_cumulative_episodes(self):
        items = [
            {
                "name": f"Show 2nd Season [{number:02d}].mkv",
                "full_path": f"/src/第二季/Show 2nd Season [{number:02d}].mkv",
            }
            for number in range(25, 30)
        ]
        changed = scraper._map_disc_extras_by_official_release_runs(
            items,
            show={
                "seasons": [
                    {"season_number": 1, "name": "Show"},
                    {"season_number": 2, "name": "Show 2nd Season"},
                ]
            },
            positive_seasons=[
                {
                    "season_number": 1,
                    "episode_count": 24,
                    "air_date": "2019-01-01",
                },
                {
                    "season_number": 2,
                    "episode_count": 24,
                    "air_date": "2021-01-01",
                },
            ],
            special_runtimes={
                1: 5,
                2: 5,
                3: 5,
                4: 5,
                5: 5,
                6: 24,
                7: 24,
                8: 24,
                9: 24,
                10: 24,
            },
            special_air_dates={
                1: "2019-02-01",
                2: "2019-03-01",
                3: "2019-04-01",
                4: "2019-05-01",
                5: "2019-06-01",
                6: "2021-02-01",
                7: "2021-03-01",
                8: "2021-04-01",
                9: "2021-05-01",
                10: "2021-06-01",
            },
        )
        self.assertEqual(changed, 0)
        self.assertTrue(
            all("_episode_key_override" not in item for item in items)
        )

    def test_local_sp_and_overflow_ordinals_map_within_their_release_season(self):
        items = [
            {
                "name": "High School DxD [14].mkv",
                "full_path": "/src/第一季/High School DxD [14].mkv",
            },
            {
                "name": "High School DxD [SP05].mkv",
                "full_path": "/src/第一季/High School DxD [SP05].mkv",
            },
            {
                "name": "High School DxD BorN [SP02].mkv",
                "full_path": "/src/第三季/High School DxD BorN [SP02].mkv",
            },
            {
                "name": "High School DxD BorN [SP04].mkv",
                "full_path": "/src/第三季/High School DxD BorN [SP04].mkv",
            },
        ]
        special_runtimes = {
            **{number: 4 for number in range(2, 7)},
            8: 24,
            9: 24,
            **{number: 4 for number in range(12, 16)},
        }
        special_air_dates = {
            2: "2012-04-01", 3: "2012-05-01", 4: "2012-06-01",
            5: "2012-07-01", 6: "2012-08-01",
            8: "2012-09-01", 9: "2012-10-01",
            12: "2015-05-01", 13: "2015-06-01",
            14: "2015-07-01", 15: "2015-08-01",
        }
        changed = scraper._map_disc_extras_by_official_release_runs(
            items,
            show={
                "seasons": [
                    {"season_number": 1, "name": "High School DxD"},
                    {"season_number": 3, "name": "High School DxD BorN"},
                ]
            },
            positive_seasons=[
                {"season_number": 1, "episode_count": 12, "air_date": "2012-01-06"},
                {"season_number": 2, "episode_count": 12, "air_date": "2013-07-07"},
                {"season_number": 3, "episode_count": 12, "air_date": "2015-04-04"},
                {"season_number": 4, "episode_count": 13, "air_date": "2018-04-10"},
            ],
            special_runtimes=special_runtimes,
            special_air_dates=special_air_dates,
        )

        self.assertEqual(changed, 4)
        self.assertEqual(
            [item["_episode_key_override"] for item in items],
            [9, 6, 13, 15],
        )

    def test_numbered_ova_suffixes_use_release_season_full_length_specials(self):
        items = [
            {
                "name": f"High School DxD {number} OVA.mkv",
                "full_path": f"/src/High School DxD S1/High School DxD {number} OVA.mkv",
            }
            for number in (13, 14)
        ]
        changed = scraper._map_disc_extras_by_official_release_runs(
            items,
            show={"name": "High School DxD"},
            positive_seasons=[
                {"season_number": 1, "episode_count": 12, "air_date": "2012-01-06"},
                {"season_number": 2, "episode_count": 12, "air_date": "2013-07-07"},
                {"season_number": 3, "episode_count": 12, "air_date": "2015-04-04"},
                {"season_number": 4, "episode_count": 13, "air_date": "2018-04-10"},
            ],
            special_runtimes={
                **{number: 4 for number in range(2, 8)},
                8: 24,
                9: 24,
                11: 24,
                **{number: 4 for number in range(12, 16)},
                16: 24,
                17: 4,
                18: 4,
            },
            special_air_dates={
                2: "2012-03-21", 3: "2012-04-25", 4: "2012-05-23",
                5: "2012-06-27", 6: "2012-07-25", 7: "2012-08-29",
                8: "2012-09-06", 9: "2013-05-31", 11: "2015-03-10",
                12: "2015-07-24", 13: "2015-08-26", 14: "2015-10-28",
                15: "2015-11-25", 16: "2015-12-09", 17: "2015-12-25",
                18: "2016-01-27",
            },
        )

        self.assertEqual(changed, 2)
        self.assertEqual([item["_episode_key_override"] for item in items], [8, 9])

    def test_two_part_official_special_folder_gets_distinct_episode_keys(self):
        items = [
            {
                "name": "01.mp4",
                "full_path": "/src/辉夜大小姐想让我告白 通往大人的阶梯/01.mp4",
            },
            {
                "name": "02.mp4",
                "full_path": "/src/辉夜大小姐想让我告白 通往大人的阶梯/02.mp4",
            },
        ]

        changed = scraper._map_split_official_special_folder(
            items,
            {6: ["通往大人的阶梯", "The Stairway to Adulthood"]},
        )

        self.assertEqual(changed, 2)
        self.assertEqual([item["_episode_key_override"] for item in items], [6, 6])
        self.assertEqual(
            [item["_episode_part_override"] for item in items],
            [1, 2],
        )
        self.assertEqual(
            [item["_episode_title_override"] for item in items],
            ["通往大人的阶梯", "通往大人的阶梯"],
        )

    def test_kaguya_stairway_bare_parts_stay_in_official_tv_special(self):
        files = [
            {"name": "Kaguya [01].mkv", "full_path": "/src/Season 1/Kaguya [01].mkv", "size": 1_000_000},
            {"name": "01.mp4", "full_path": "/src/辉夜大小姐想让我告白 通往大人的阶梯/01.mp4", "size": 600_000},
            {"name": "02.mp4", "full_path": "/src/辉夜大小姐想让我告白 通往大人的阶梯/02.mp4", "size": 500_000},
        ]
        tmdb = FakeTMDB({
            "/tv/69357": {
                "name": "辉夜大小姐想让我告白", "first_air_date": "2019-01-12",
                "poster_path": None, "backdrop_path": None,
                "seasons": [{"season_number": 0, "episode_count": 6}, {"season_number": 1, "episode_count": 1}],
            },
            "/tv/69357/season/1": {"episodes": [{"episode_number": 1, "name": "我想让辉夜告白"}]},
            "/tv/69357/season/0": {"episodes": [{
                "episode_number": 6,
                "name": "辉夜大小姐想让我告白 通往大人的阶梯",
            }]},
        })
        unrelated_movie = mock.Mock(
            status="confirmed", media_type="movie", tmdb_id=918713,
            title="02", year="2006", confidence=1.0,
        )
        with mock.patch.object(
            scraper,
            "auto_match_tmdb",
            return_value=(unrelated_movie, [unrelated_movie]),
        ) as matcher:
            plan = scraper.build_tv_plan_smart(
                auto_episode_mode=True, alist=FakeAList(files), tmdb_client=tmdb,
                src_path="/src", parent_path="/library", tmdb_id=69357,
                season=1, absolute=False, prefer_simplified=True,
                allow_unmapped=False, episode_map_path=None, episode_group_id=None,
            )
        matcher.assert_not_called()
        special_files = [item for item in plan.files if "通往大人的阶梯" in item.source_path]
        self.assertEqual(len(special_files), 2)
        self.assertTrue(all("S00E06" in item.final_name for item in special_files))
        self.assertEqual(
            {suffix for item in special_files for suffix in ("part1" if "part1" in item.final_name else "part2",)},
            {"part1", "part2"},
        )
        self.assertTrue(all(item.target_dir.endswith("/Season 00") for item in special_files))
        self.assertFalse(plan.cleanup_files)
        self.assertFalse(plan.problem_files)

    def test_quintuplets_named_special_runs_require_independent_movie_identity(self):
        files = [
            {
                "name": "Go-Toubun no Hanayome SP [01].mkv",
                "full_path": "/src/特别篇/Go-Toubun no Hanayome SP [01].mkv",
                "size": 600_000,
            },
            {
                "name": "Go-Toubun no Hanayome SP [02].mkv",
                "full_path": "/src/特别篇/Go-Toubun no Hanayome SP [02].mkv",
                "size": 500_000,
            },
            {
                "name": "Go-Toubun no Hanayome Honeymoon SP [01].mkv",
                "full_path": "/src/新婚旅行篇/Go-Toubun no Hanayome Honeymoon SP [01].mkv",
                "size": 400_000,
            },
            {
                "name": "Go-Toubun no Hanayome Honeymoon SP [02].mkv",
                "full_path": "/src/新婚旅行篇/Go-Toubun no Hanayome Honeymoon SP [02].mkv",
                "size": 300_000,
            },
            {
                "name": "Go-Toubun no Hanayome [01].mkv",
                "full_path": "/src/第一季/Go-Toubun no Hanayome [01].mkv",
                "size": 1_000_000,
            },
        ]
        tmdb = FakeTMDB({
            "/tv/62568": {
                "name": "五等分的新娘",
                "original_name": "五等分の花嫁",
                "first_air_date": "2019-01-11",
                "poster_path": None,
                "backdrop_path": None,
                "seasons": [{"season_number": 1, "episode_count": 1}],
            },
            "/tv/62568/season/1": {
                "episodes": [{"episode_number": 1, "name": "五等分的新娘"}]
            },
            "/tv/62568/season/0": {"episodes": []},
        })
        movie_inputs = {}

        def fake_match(_client, query, **_kwargs):
            if "honeymoon" in query.casefold() or "新婚旅行篇" in query:
                match = scraper.AutoMatch(
                    "movie", 1287324, "五等分的新娘＊", "2024", 0.99
                )
                return match, [match]
            if "sp" in query.casefold() or "特别篇" in query:
                match = scraper.AutoMatch(
                    "movie", 1153706, "五等分的新娘∽", "2023", 0.99
                )
                return match, [match]
            raise scraper.PlanError("no independent movie identity")

        def fake_movie_plan(
            _alist, _tmdb, *, src_path, parent_path, tmdb_id,
            source_files, **_kwargs,
        ):
            movie_inputs[tmdb_id] = {
                str(item["full_path"]) for item in source_files
            }
            title, year = {
                1153706: ("五等分的新娘∽", "2023"),
                1287324: ("五等分的新娘＊", "2024"),
            }[tmdb_id]
            return scraper.Plan(
                mode="movie",
                source_root=src_path,
                target_root=parent_path,
                files=[],
                warnings=[],
                metadata={
                    "tmdb_id": tmdb_id,
                    "title": title,
                    "year": year,
                    "poster_path": None,
                },
            )

        with mock.patch.object(
            scraper, "auto_match_tmdb", side_effect=fake_match
        ), mock.patch.object(
            scraper, "build_movie_plan", side_effect=fake_movie_plan
        ):
            plan = scraper.build_tv_plan_smart(
                auto_episode_mode=True,
                alist=FakeAList(files),
                tmdb_client=tmdb,
                src_path="/src",
                parent_path="/library",
                tmdb_id=62568,
                season=1,
                absolute=False,
                prefer_simplified=True,
                allow_unmapped=False,
                ignore_orphan_temp=False,
                episode_map_path=None,
                episode_group_id=None,
                source_files=files,
            )

        self.assertEqual(set(movie_inputs), {1153706, 1287324})
        self.assertEqual(len(movie_inputs[1153706]), 2)
        self.assertEqual(len(movie_inputs[1287324]), 2)
        self.assertTrue(any("独立电影《五等分的新娘∽》" in item for item in plan.warnings))
        self.assertTrue(any("独立电影《五等分的新娘＊》" in item for item in plan.warnings))
        self.assertFalse(any("Season 00" in item.target_dir for item in plan.files))

    def test_explicit_sp01_without_official_identity_is_not_guessed_as_s00e01(self):
        files = [
            {"name": "Show [01].mkv", "full_path": "/src/Show [01].mkv"},
            {"name": "Show SP [01].mkv", "full_path": "/src/SP/Show SP [01].mkv"},
            {"name": "Show SP [01].ass", "full_path": "/src/SP/Show SP [01].ass"},
        ]
        responses = dict(self.responses)
        responses["/tv/10/season/0"] = {"episodes": []}

        plan = scraper.build_tv_plan(
            FakeAList(files),
            FakeTMDB(responses),
            src_path="/src",
            parent_path="/library",
            tmdb_id=10,
            season=1,
            absolute=False,
            prefer_simplified=False,
            allow_unmapped=False,
        )

        self.assertEqual([item.original_name for item in plan.files], ["Show [01].mkv"])
        self.assertEqual(
            {item.source_path for item in plan.problem_files},
            {"/src/SP/Show SP [01].mkv"},
        )
        self.assertIn(
            "/src/SP/Show SP [01].ass",
            {row["source_path"] for row in plan.scan_report["deferred_subtitles"]},
        )
        self.assertTrue(
            all("Season 00" not in (item.target_path or "") for item in plan.problem_files)
        )

    def test_explicit_23b_maps_only_to_unique_official_beta_special(self):
        item = {"name": "Steins;Gate [23B].mkv", "full_path": "/src/Steins;Gate [23B].mkv"}
        changed = scraper._map_explicit_beta_alternate(
            [item],
            {
                1: ["Egoistic Poriomania"],
                6: ["Open the Missing Link", "境界面上的缺失之环（β线）"],
            },
        )
        self.assertEqual(changed, 1)
        self.assertEqual(item["_episode_kind_override"], "special")
        self.assertEqual(item["_episode_key_override"], 6)
        self.assertEqual(scraper.entry_edition_tag(item), "23β")

    def test_tv_behind_the_scenes_in_bonus_folder_is_extra_not_episode(self):
        files = [
            {"name": "Show [01].mkv", "full_path": "/src/Show [01].mkv"},
            {
                "name": "Show Behind the Scene #01.mkv",
                "full_path": "/src/SPs/Show Behind the Scene #01.mkv",
            },
            {
                "name": "Show Behind the Scene #02.mkv",
                "full_path": "/src/SPs/Show Behind the Scene #02.mkv",
            },
        ]
        plan = scraper.build_tv_plan(
            FakeAList(files), FakeTMDB(self.responses),
            src_path="/src", parent_path="/library", tmdb_id=10, season=1,
            absolute=False, prefer_simplified=True, allow_unmapped=False,
            auto_special_title_match=True, source_files=files,
        )
        extras = [item for item in plan.files if "behindthescenes" in item.final_name]
        self.assertEqual(len(extras), 2)
        self.assertTrue(all(item.target_dir == plan.target_root for item in extras))
        self.assertFalse(any("Behind the Scene" in item.source_path for item in plan.problem_files))
        warning = next(item for item in plan.warnings if "Infuse Extras" in item)
        scraper.finalize_plan_evidence(plan)
        notice = next(item for item in plan.notices if item.message == warning)
        self.assertFalse(notice.requires_review)

    def test_simplified_preference_defers_matching_traditional_subtitle(self):
        files = [
            {"name": "Show [01].mkv", "full_path": "/src/Show [01].mkv"},
            {"name": "Show [01].chs.ass", "full_path": "/src/Show [01].chs.ass"},
            {"name": "Show [01].cht.ass", "full_path": "/src/Show [01].cht.ass"},
        ]
        plan = scraper.build_tv_plan(
            FakeAList(files),
            FakeTMDB(self.responses),
            src_path="/src", parent_path="/library", tmdb_id=10, season=1,
            absolute=False, prefer_simplified=True, allow_unmapped=False,
            auto_special_title_match=True, source_files=files,
        )
        self.assertEqual(
            {item.original_name for item in plan.files},
            {"Show [01].mkv", "Show [01].chs.ass"},
        )
        self.assertIn(
            "/src/Show [01].cht.ass",
            {row["source_path"] for row in plan.scan_report["deferred_subtitles"]},
        )
        self.assertTrue(any(
            "繁体字幕" in warning and "已保留在源目录" in warning
            for warning in plan.warnings
        ))

    def test_long_tmdb_season_detects_quarterly_broadcast_resets(self):
        from datetime import datetime, timedelta

        starts = ["2023-04-01", "2024-07-01", "2026-01-14"]
        expected = [11, 13, 11]
        episodes = []
        number = 1
        for start, count in zip(starts, expected):
            base = datetime.strptime(start, "%Y-%m-%d")
            for offset in range(count):
                episodes.append({
                    "episode_number": number,
                    "air_date": (base + timedelta(days=7 * offset)).strftime("%Y-%m-%d"),
                })
                number += 1
        self.assertEqual(
            scraper._tmdb_long_season_block_counts(episodes),
            expected,
        )

    def test_long_tmdb_season_merges_absolute_and_reset_release_folders(self):
        groups = {
            1: [
                {"name": f"Show [{number:02d}].mkv", "full_path": f"/src/{number:02d}.mkv"}
                for number in range(1, 25)
            ],
            2: [
                {"name": f"Show S2 [{number:02d}].mkv", "full_path": f"/src/S2/{number:02d}.mkv"}
                for number in range(12, 25)
            ],
            3: [
                {"name": f"Show S3 [{number:02d}].mkv", "full_path": f"/src/S3/{number:02d}.mkv"}
                for number in range(1, 12)
            ],
        }
        warnings = scraper._merge_broadcast_folders_into_long_tmdb_season(
            groups,
            official_season=1,
            block_counts=[11, 13, 11],
        )
        self.assertEqual(set(groups), {1})
        s2 = [item for item in groups[1] if "/S2/" in item["full_path"]]
        s3 = [item for item in groups[1] if "/S3/" in item["full_path"]]
        self.assertEqual(
            {item["_episode_key_override"] for item in s2},
            set(range(12, 25)),
        )
        self.assertEqual(
            {item["_episode_key_override"] for item in s3},
            set(range(25, 36)),
        )
        self.assertEqual(len(warnings), 2)

    @staticmethod
    def _subseries_plan(mode, title, target_root, tmdb_id):
        target_year = re.search(r"\(((?:19|20)\d{2})\)$", target_root)
        metadata = {
            "tmdb_id": tmdb_id,
            "title": title,
            "year": target_year.group(1) if target_year else "2020",
            "poster_path": f"/poster-{tmdb_id}.jpg",
        }
        if mode == "tv":
            metadata.update({"season": 1, "absolute": False})
        return scraper.Plan(
            mode=mode,
            source_root=f"/src/{tmdb_id}",
            target_root=target_root,
            files=[
                scraper.PlannedFile(
                    source_path=f"/src/{tmdb_id}/video.mkv",
                    source_dir=f"/src/{tmdb_id}",
                    original_name="video.mkv",
                    final_name=(
                        f"{title} - S01E01.mkv" if mode == "tv" else f"{title}.mkv"
                    ),
                    target_dir=(
                        f"{target_root}/Season 01" if mode == "tv" else target_root
                    ),
                    media_kind="video",
                )
            ],
            warnings=[],
            metadata=metadata,
        )

    def test_batch_subseries_create_recursive_family_roots(self):
        outer = "/library/Fate"
        illya = self._subseries_plan("tv", "魔法少女☆伊莉雅", f"{outer}/魔法少女☆伊莉雅", 1)
        snow = self._subseries_plan(
            "movie", "魔法少女☆伊莉雅：雪下的誓言", f"{outer}/魔法少女☆伊莉雅：雪下的誓言 (2017)", 2,
        )
        nameless = self._subseries_plan(
            "movie", "魔法少女☆伊莉雅：无名的少女", f"{outer}/魔法少女☆伊莉雅：无名的少女 (2021)", 3,
        )
        unrelated = self._subseries_plan("tv", "命运石之门", f"{outer}/命运石之门", 4)

        warnings, posters = scraper._nest_batch_subseries(
            [illya, snow, nameless, unrelated],
            outer_root=outer,
        )

        family = f"{outer}/魔法少女☆伊莉雅"
        self.assertEqual(illya.target_root, family)
        self.assertEqual(snow.target_root, f"{family}/魔法少女☆伊莉雅：雪下的誓言 (2017)")
        self.assertEqual(nameless.target_root, f"{family}/魔法少女☆伊莉雅：无名的少女 (2021)")
        self.assertEqual(unrelated.target_root, f"{outer}/命运石之门")
        self.assertTrue(any("子系列 '魔法少女☆伊莉雅'" in warning for warning in warnings))
        self.assertEqual(posters[family], "/poster-1.jpg")

        flattened = scraper._flatten_series_owned_batch_movies(
            [illya, snow, nameless, unrelated]
        )
        self.assertEqual(flattened, 0)
        self.assertEqual(snow.target_root, f"{family}/魔法少女☆伊莉雅：雪下的誓言 (2017)")
        self.assertEqual(snow.files[0].target_dir, snow.target_root)
        self.assertEqual(nameless.target_root, f"{family}/魔法少女☆伊莉雅：无名的少女 (2021)")
        self.assertEqual(nameless.files[0].target_dir, nameless.target_root)
        self.assertEqual(illya.target_root, family)
        self.assertEqual(unrelated.target_root, f"{outer}/命运石之门")

        batch = scraper.Plan(
            mode="batch",
            source_root="/src",
            target_root=outer,
            files=[item for plan in (illya, snow, nameless) for item in plan.files],
            warnings=[],
            metadata={
                "title": "Fate",
                "member_tv": {},
                "member_movies": {},
                "member_posters": posters,
            },
        )
        self.assertIn(
            (f"{family}/folder.jpg", "/poster-1.jpg", "member-folder"),
            scraper.planned_artwork(batch),
        )

    def test_backup_subtitle_follows_only_one_matching_movie_group(self):
        movie_groups = {
            11: [{
                "name": "[Ygm] Fullmetal Alchemist The Sacred Star of Milos [2160p].mkv",
                "full_path": "/src/Milos/[Ygm] Fullmetal Alchemist The Sacred Star of Milos [2160p].mkv",
            }],
            12: [{
                "name": "[Ygm] Fullmetal Alchemist Conqueror of Shamballa [2160p].mkv",
                "full_path": "/src/Shamballa/[Ygm] Fullmetal Alchemist Conqueror of Shamballa [2160p].mkv",
            }],
        }
        milos_subtitle = {
            "name": "[Moozzi2] Fullmetal Alchemist The Sacred Star of Milos (BD 1080p).ass",
            "full_path": "/src/备份字幕/[Moozzi2] Fullmetal Alchemist The Sacred Star of Milos (BD 1080p).ass",
        }
        unrelated = {
            "name": "unrelated.ass",
            "full_path": "/src/备份字幕/unrelated.ass",
        }
        remaining = scraper._attach_unique_movie_subtitles(
            movie_groups,
            [milos_subtitle, unrelated],
        )
        self.assertIn(milos_subtitle, movie_groups[11])
        self.assertNotIn(milos_subtitle, movie_groups[12])
        self.assertEqual(remaining, [unrelated])

    def test_contextual_ova_video_follows_one_existing_movie_edition(self):
        movie_groups = {
            532321: [{
                "name": "Re Zero Memory Snow [2160p].mkv",
                "full_path": "/src/4K/Re Zero Memory Snow [2160p].mkv",
            }],
            566451: [{
                "name": "Re Zero Hyouketsu no Kizuna [2160p].mkv",
                "full_path": "/src/4K/Re Zero Hyouketsu no Kizuna [2160p].mkv",
            }],
        }
        backup = {
            "name": "Re Zero Memory Snow [1080p].mp4",
            "full_path": "/src/OVA.2018.Memory Snow/Re Zero Memory Snow [1080p].mp4",
        }
        remaining = scraper._attach_unique_movie_subtitles(
            movie_groups,
            [backup],
        )
        self.assertEqual(remaining, [])
        self.assertIn(backup, movie_groups[532321])
        self.assertNotIn(backup, movie_groups[566451])

    def test_generic_subtitle_in_confirmed_movie_directory_follows_video(self):
        video = {
            "name": "Prisma Phantasm [2160p].mkv",
            "full_path": "/src/06 OVA Prisma Phantasm/Prisma Phantasm [2160p].mkv",
        }
        subtitle = {
            "name": "简中.ass",
            "full_path": "/src/06 OVA Prisma Phantasm/简中.ass",
        }
        movie_groups = {658436: [video]}

        remaining = scraper._attach_unique_movie_subtitles(
            movie_groups,
            [subtitle],
        )

        self.assertEqual(remaining, [])
        self.assertEqual(movie_groups[658436], [video, subtitle])

    def test_numbered_tv_backup_subtitle_is_not_stolen_by_related_movie(self):
        movie_video = {
            "name": "Koutetsujou no Kabaneri Unato Kessen [2160p].mkv",
            "full_path": "/src/Unato/Koutetsujou no Kabaneri Unato Kessen [2160p].mkv",
        }
        tv_subtitle = {
            "name": "[Ygm] Koutetsujou no Kabaneri [01].ass",
            "full_path": "/src/备份字幕/[Ygm] Koutetsujou no Kabaneri [01].ass",
        }
        movie_groups = {710538: [movie_video]}

        remaining = scraper._attach_unique_movie_subtitles(
            movie_groups,
            [tv_subtitle],
        )

        self.assertEqual(remaining, [tv_subtitle])
        self.assertEqual(movie_groups[710538], [movie_video])

    def test_official_special_override_cannot_be_stolen_by_movie_companion_pass(self):
        movie_video = {
            "name": "Magic Girl Hot Spring Trip.mkv",
            "full_path": "/src/OVA/Magic Girl Hot Spring Trip.mkv",
        }
        official_special = {
            "name": "Prisma Illya Herz Tokuten_Anime01.mkv",
            "full_path": "/src/Season 03/SPs/Prisma Illya Herz Tokuten_Anime01.mkv",
            "_episode_kind_override": "special",
            "_episode_key_override": 13,
        }
        movie_groups = {1280875: [movie_video]}

        remaining = scraper._attach_unique_movie_subtitles(
            movie_groups,
            [official_special],
        )

        self.assertEqual(remaining, [official_special])
        self.assertEqual(movie_groups[1280875], [movie_video])

    def test_movie_only_chapter_series_gets_a_family_root(self):
        outer = "/library/Fate"
        first = self._subseries_plan(
            "movie", "空之境界 第一章 俯瞰风景", f"{outer}/空之境界 第一章 俯瞰风景 (2007)", 11,
        )
        second = self._subseries_plan(
            "movie", "空之境界 第二章 杀人考察（前）", f"{outer}/空之境界 第二章 杀人考察（前） (2007)", 12,
        )

        _warnings, posters = scraper._nest_batch_subseries(
            [first, second], outer_root=outer,
        )

        self.assertEqual(
            first.target_root,
            f"{outer}/空之境界/空之境界 第一章 俯瞰风景 (2007)",
        )
        self.assertEqual(
            second.target_root,
            f"{outer}/空之境界/空之境界 第二章 杀人考察（前） (2007)",
        )
        self.assertEqual(posters[f"{outer}/空之境界"], "/poster-11.jpg")

    def test_roman_numbered_movie_trilogy_gets_shared_family_root(self):
        outer = "/library/Fate"
        plans = [
            self._subseries_plan(
                "movie",
                f"命运之夜——天之杯{roman}：{subtitle}",
                f"{outer}/命运之夜——天之杯{roman}：{subtitle} ({year})",
                30 + index,
            )
            for index, (roman, subtitle, year) in enumerate((
                ("Ⅰ", "恶兆之花", 2017),
                ("Ⅱ", "迷失之蝶", 2019),
                ("Ⅲ", "春之歌", 2020),
            ))
        ]

        _warnings, posters = scraper._nest_batch_subseries(plans, outer_root=outer)

        family = f"{outer}/命运之夜——天之杯"
        self.assertTrue(all(plan.target_root.startswith(family + "/") for plan in plans))
        self.assertEqual(posters[family], "/poster-30.jpg")

    def test_fullwidth_slash_subseries_preserves_display_title_and_poster(self):
        outer = "/library/Fate"
        moonlight = self._subseries_plan(
            "movie",
            "命运／冠位指定 -月光／失落之室-",
            f"{outer}/命运／冠位指定 -月光／失落之室- (2017)",
            41,
        )
        camelot = self._subseries_plan(
            "movie",
            "命运／冠位指定 -神圣圆桌领域卡美洛- 前篇 漂泊的银之臂",
            f"{outer}/命运／冠位指定 -神圣圆桌领域卡美洛- 前篇 漂泊的银之臂 (2020)",
            42,
        )

        _warnings, posters = scraper._nest_batch_subseries(
            [moonlight, camelot], outer_root=outer,
        )

        family = f"{outer}/命运／冠位指定"
        self.assertTrue(moonlight.target_root.startswith(family + "/"))
        self.assertTrue(camelot.target_root.startswith(family + "/"))
        self.assertNotIn("命运-冠位指定", moonlight.target_root)
        self.assertEqual(posters[family], "/poster-41.jpg")

    @staticmethod
    def _confirmed_movie_copy(root, source_path, name, tmdb_id, *, final_name=None):
        source_dir, original_name = scraper.split_remote(source_path)
        return scraper.Plan(
            mode="movie",
            source_root=source_dir,
            target_root=root,
            files=[
                scraper.PlannedFile(
                    source_path=source_path,
                    source_dir=source_dir,
                    original_name=original_name,
                    final_name=final_name or name,
                    target_dir=root,
                    media_kind="video",
                    source_size=100,
                )
            ],
            warnings=[],
            metadata={
                "tmdb_id": tmdb_id,
                "title": "Confirmed Movie",
                "year": "2023",
            },
        )

    def test_batch_same_tmdb_movie_across_paths_keeps_only_higher_resolution(self):
        outer = "/library/Fate"
        canonical = f"{outer}/Confirmed Movie (2023)"
        generic = f"{outer}/其它/Confirmed Movie (2023)"
        low = self._confirmed_movie_copy(
            canonical,
            "/src/1080p/Confirmed.Movie.1080p.mkv",
            "Confirmed Movie (2023).mkv",
            1145612,
        )
        high = self._confirmed_movie_copy(
            generic,
            "/src/4K/Confirmed.Movie.2160p.mkv",
            "Confirmed Movie (2023).mkv",
            1145612,
        )

        warnings = scraper._dedupe_confirmed_batch_movies(
            [low, high], outer_root=outer,
        )

        self.assertEqual(low.files, [])
        self.assertEqual(len(low.cleanup_files), 1)
        self.assertIn("/src/4K/Confirmed.Movie.2160p.mkv", low.cleanup_files[0].reason)
        self.assertEqual(high.target_root, canonical)
        self.assertEqual(high.files[0].target_dir, canonical)
        self.assertTrue(any("movie/1145612" in warning for warning in warnings))

    def test_batch_movie_dedupe_retargets_cleanup_from_removed_1080_winner(self):
        outer = "/library/Fate"
        low_root = f"{outer}/TV/Whispers of Dawn (2023)"
        high_root = f"{outer}/其它/Whispers of Dawn (2023)"
        soft = self._confirmed_movie_copy(
            low_root,
            "/src/1080P 内封多国字幕/Fate.strange.Fake.S01E00.Whispers.of.Dawn.2023.1080p.mkv",
            "Whispers of Dawn (2023).mkv",
            1145612,
        )
        burned_path = (
            "/src/1080P 内嵌简日双语字幕/"
            "[Sakurato] Fate Strange Fake [00][Whispers of Dawn][1080p][CHS].mp4"
        )
        soft.cleanup_files.append(scraper.PlannedCleanup(
            source_path=burned_path,
            source_dir=burned_path.rsplit("/", 1)[0],
            original_name=burned_path.rsplit("/", 1)[-1],
            reason=scraper._burned_subtitle_cleanup_reason(
                soft.files[0].source_path
            ),
            source_size=90,
        ))
        high = self._confirmed_movie_copy(
            high_root,
            "/src/4K/[MAI] Fate strange Fake - Whispers of Dawn [SP][2160p].mkv",
            "Whispers of Dawn (2023).mkv",
            1145612,
        )

        scraper._dedupe_confirmed_batch_movies(
            [soft, high], outer_root=outer,
        )

        burned_cleanup = next(
            item for item in soft.cleanup_files if item.source_path == burned_path
        )
        self.assertEqual(
            burned_cleanup.reason,
            scraper._lower_resolution_movie_cleanup_reason(
                high.files[0].source_path, 1145612,
            ),
        )

    def test_movie_version_dedupe_then_family_planner_preserves_family_container(self):
        outer = "/library/Fate"
        family = f"{outer}/魔法少女☆伊莉雅"
        illya = self._subseries_plan("tv", "魔法少女☆伊莉雅", family, 1)
        canonical_title = "魔法少女☆伊莉雅：雪下的誓言"
        low = self._confirmed_movie_copy(
            f"{outer}/其他/{canonical_title} (2017)",
            "/src/1080p/Snow.1080p.mkv",
            f"{canonical_title} (2017).mkv",
            2,
        )
        high = self._confirmed_movie_copy(
            f"{outer}/剧场版/{canonical_title} (2017)",
            "/src/4K/Snow.2160p.mkv",
            f"{canonical_title} (2017).mkv",
            2,
        )
        for plan in (low, high):
            plan.metadata["title"] = canonical_title
            plan.metadata["year"] = "2017"

        scraper._dedupe_confirmed_batch_movies([low, high], outer_root=outer)
        scraper._nest_batch_subseries([illya, low, high], outer_root=outer)

        expected = f"{family}/{canonical_title} (2017)"
        self.assertEqual(high.target_root, expected)
        self.assertEqual(high.files[0].target_dir, expected)
        self.assertEqual(low.target_root, expected)
        self.assertEqual(low.files, [])

    def test_batch_movie_quality_dedupe_keeps_parts_editions_themes_and_equal_quality(self):
        outer = "/library/Fate"
        roots = (f"{outer}/Movie (2023)", f"{outer}/其他/Movie (2023)")
        pairs = (
            ("Movie - part1.mkv", "Movie - part2.mkv"),
            ("Movie Director's Cut 1080p.mkv", "Movie Theatrical Cut 2160p.mkv"),
            ("Movie OP 1080p.mkv", "Movie 2160p.mkv"),
            ("Movie A 1080p.mkv", "Movie B 1080p.mkv"),
        )
        for index, (low_name, high_name) in enumerate(pairs, start=1):
            with self.subTest(pair=(low_name, high_name)):
                low = self._confirmed_movie_copy(
                    roots[0], f"/src/{index}/1080p/{low_name}", low_name,
                    9000 + index, final_name=low_name,
                )
                high = self._confirmed_movie_copy(
                    roots[1], f"/src/{index}/2160p/{high_name}", high_name,
                    9000 + index, final_name=high_name,
                )

                scraper._dedupe_confirmed_batch_movies(
                    [low, high], outer_root=outer,
                )

                self.assertEqual(len(low.files), 1)
                self.assertEqual(len(high.files), 1)
                self.assertEqual(low.cleanup_files, [])

    def test_batch_movie_quality_dedupe_requires_same_confirmed_tmdb_id(self):
        outer = "/library/Fate"
        low = self._confirmed_movie_copy(
            f"{outer}/Movie A", "/src/A/Movie.1080p.mkv", "Movie A.mkv", 1,
        )
        high = self._confirmed_movie_copy(
            f"{outer}/Movie B", "/src/B/Movie.2160p.mkv", "Movie B.mkv", 2,
        )

        warnings = scraper._dedupe_confirmed_batch_movies(
            [low, high], outer_root=outer,
        )

        self.assertEqual(warnings, [])
        self.assertEqual(len(low.files), 1)
        self.assertEqual(len(high.files), 1)

    def test_batch_same_tmdb_equal_quality_versions_share_canonical_leaf(self):
        outer = "/library/Fate"
        first_root = f"{outer}/Movie (2023)"
        second_root = f"{outer}/其他/Movie (2023)"
        first = self._confirmed_movie_copy(
            first_root, "/src/A/Movie.1080p.mkv", "Movie (2023).mkv", 77,
        )
        second = self._confirmed_movie_copy(
            second_root, "/src/B/Movie.1080p.mkv", "Movie (2023).mkv", 77,
        )

        warnings = scraper._dedupe_confirmed_batch_movies(
            [first, second], outer_root=outer,
        )

        self.assertEqual(warnings, [])
        canonical = f"{outer}/Confirmed Movie (2023)"
        self.assertEqual(first.target_root, canonical)
        self.assertEqual(second.target_root, canonical)
        self.assertEqual(first.files[0].target_dir, canonical)
        self.assertEqual(second.files[0].target_dir, canonical)
        self.assertEqual(first.cleanup_files, [])
        self.assertEqual(second.cleanup_files, [])

    def test_batch_movie_cleanup_validator_requires_confirmed_identity_and_variant(self):
        source_root = "/quark/影视/待刮削/Fate"
        target_root = "/quark/影视/番剧/Fate"
        movie_root = f"{target_root}/命运／奇异赝品 黎明低语 (2023)"
        winner = scraper.PlannedFile(
            source_path=f"{source_root}/4K/Fate Strange Fake 2160p.mkv",
            source_dir=f"{source_root}/4K",
            original_name="Fate Strange Fake 2160p.mkv",
            final_name="命运／奇异赝品 黎明低语 (2023).mkv",
            target_dir=movie_root,
            media_kind="video",
            source_size=200,
        )
        loser_path = (
            f"{source_root}/1080P/"
            "Fate Strange Fake [00][Whispers of Dawn] 1080p.mkv"
        )
        plan = scraper.Plan(
            mode="batch",
            source_root=source_root,
            target_root=target_root,
            files=[winner],
            cleanup_files=[
                scraper.PlannedCleanup(
                    source_path=loser_path,
                    source_dir=f"{source_root}/1080P",
                    original_name=loser_path.rsplit("/", 1)[-1],
                    reason=scraper._lower_resolution_movie_cleanup_reason(
                        winner.source_path,
                        1145612,
                    ),
                    source_size=100,
                )
            ],
            warnings=[],
            metadata={
                "title": "Fate",
                "member_tv": {},
                "member_posters": {},
                "member_movies": {
                    movie_root: {
                        "tmdb_id": 1145612,
                        "title": "命运／奇异赝品 黎明低语",
                        "year": "2023",
                    }
                },
            },
        )
        alist = FakeAList([
            {
                "name": winner.original_name,
                "full_path": winner.source_path,
                "size": 200,
            },
            {
                "name": loser_path.rsplit("/", 1)[-1],
                "full_path": loser_path,
                "size": 100,
            },
        ])

        scraper.validate_plan(alist, plan)

        # A batch may flatten/rebase the retained movie after identity was
        # confirmed.  Final validation then uses the still-confirmed plan ID
        # plus the concrete release title rather than stale root containment.
        winner.target_dir = f"{target_root}/flattened"
        scraper.validate_plan(alist, plan)

        plan.metadata["member_movies"][movie_root]["tmdb_id"] = 999
        with self.assertRaisesRegex(scraper.PlanError, "清理文件不再符合安全规则"):
            scraper.validate_plan(alist, plan)

        plan.metadata["member_movies"][movie_root]["tmdb_id"] = 1145612
        plan.cleanup_files[0].original_name = (
            "Fate Strange Fake Director's Cut 1080p.mkv"
        )
        plan.cleanup_files[0].source_path = (
            f"{source_root}/1080P/"
            "Fate Strange Fake Director's Cut 1080p.mkv"
        )
        with self.assertRaisesRegex(scraper.PlanError, "清理文件不再符合安全规则"):
            scraper.validate_plan(alist, plan)

    def test_production_routing_only_allows_unscraped_to_direct_category(self):
        for target in (
            "/quark/影视/番剧/作品",
            "/quark/影视/美剧/作品",
            "/quark/影视/电影/作品 (2024)",
        ):
            with self.subTest(target=target):
                decision = scraper.placement_for(
                    "/quark/影视/待刮削/作品",
                    target,
                )
                self.assertEqual(decision.rule, "unscraped_to_direct_category")
        replenishment = scraper.placement_for(
            "/quark/影视/ScrapeFlow/补源/ScrapeFlow补源-123-作品",
            "/quark/影视/番剧/作品",
        )
        self.assertEqual(
            replenishment.rule,
            "system_replenishment_to_direct_category",
        )
        for source, target in (
            ("/quark/影视/番剧/作品", "/quark/影视/电影/作品"),
            ("/quark/影视/待刮削/作品", "/quark/影视/已刮削/番剧/作品"),
            ("/quark/影视/待刮削/作品", "/quark/影视/番剧"),
            ("/quark/影视/待刮削/Fate", "/quark/影视/番剧/Fate/Movies/电影"),
            ("/quark/影视/待刮削/作品", "/quark/影视/番剧/作品/Specials"),
            ("/quark/影视/ScrapeFlow/补源", "/quark/影视/番剧/作品"),
            ("/quark/影视/ScrapeFlow/备份/作品", "/quark/影视/番剧/作品"),
        ):
            with self.subTest(source=source, target=target):
                with self.assertRaises(ValueError):
                    scraper.placement_for(source, target)

    def test_two_tv_works_keep_their_own_season_zero_episode_one(self):
        def make_plan(tmdb_id, title):
            return scraper.build_tv_plan(
                FakeAList([
                    {"name": f"{title}.SP01.mkv", "full_path": f"/src/{title}/{title}.SP01.mkv"},
                ]),
                FakeTMDB({
                    f"/tv/{tmdb_id}": {
                        "name": title,
                        "first_air_date": "2024-01-01",
                        "poster_path": None,
                        "seasons": [{"season_number": 0}, {"season_number": 1}],
                    },
                    f"/tv/{tmdb_id}/season/1": {"episodes": []},
                    f"/tv/{tmdb_id}/season/0": {
                        "episodes": [{"episode_number": 1, "name": "特别篇"}],
                    },
                }),
                src_path=f"/src/{title}",
                parent_path="/library",
                tmdb_id=tmdb_id,
                season=1,
                absolute=False,
                prefer_simplified=False,
                allow_unmapped=False,
            )

        first = make_plan(101, "Alpha")
        second = make_plan(202, "Beta")

        self.assertNotEqual(first.target_root, second.target_root)
        self.assertTrue(first.files[0].target_dir.endswith("/Alpha/Season 00"))
        self.assertTrue(second.files[0].target_dir.endswith("/Beta/Season 00"))
        self.assertIn("S00E01", first.files[0].final_name)
        self.assertIn("S00E01", second.files[0].final_name)

    def test_existing_library_root_reuses_same_tmdb_id_and_blocks_different_id(self):
        class NfoAList(FakeAList):
            def __init__(self, nfo_by_path):
                super().__init__(listings={
                    "/library": [{"name": "Legacy Name", "is_dir": True}],
                    "/library/Legacy Name": [{"name": "movie.nfo", "is_dir": False}],
                })
                self.nfo_by_path = nfo_by_path

            def read_file_bytes(self, path, *, max_bytes):
                return self.nfo_by_path[path]

        same = NfoAList({
            "/library/Legacy Name/movie.nfo": b"<movie><tmdbid>20</tmdbid></movie>"
        })
        root, state = scraper.resolve_existing_library_root(
            same, parent_path="/library", desired_root="/library/New Name (2020)",
            tmdb_id=20, tv=False,
        )
        self.assertEqual((root, state), ("/library/Legacy Name", "same_tmdb_id"))

        different = NfoAList({
            "/library/Legacy Name/movie.nfo": b"<movie><tmdbid>99</tmdbid></movie>"
        })
        different.listings["/library"] = [{"name": "Legacy Name", "is_dir": True}]
        with self.assertRaisesRegex(scraper.PlanError, "身份与本次计划不同"):
            scraper.resolve_existing_library_root(
                different, parent_path="/library", desired_root="/library/Legacy Name",
                tmdb_id=20, tv=False,
            )

    def test_existing_empty_matching_name_without_nfo_is_safe_preflight_residue(self):
        alist = FakeAList(listings={
            "/library": [{"name": "Movie (2020)", "is_dir": True}],
            "/library/Movie (2020)": [{"name": "Season 01", "is_dir": True}],
            "/library/Movie (2020)/Season 01": [],
        })
        root, state = scraper.resolve_existing_library_root(
            alist, parent_path="/library", desired_root="/library/Movie (2020)",
            tmdb_id=20, tv=False,
        )
        self.assertEqual(root, "/library/Movie (2020)")
        self.assertEqual(state, "empty_without_nfo")

    def test_existing_nonempty_matching_name_without_nfo_is_review_state(self):
        alist = FakeAList(listings={
            "/library": [{"name": "Movie (2020)", "is_dir": True}],
            "/library/Movie (2020)": [{"name": "unidentified.mkv", "is_dir": False}],
        })
        root, state = scraper.resolve_existing_library_root(
            alist, parent_path="/library", desired_root="/library/Movie (2020)",
            tmdb_id=20, tv=False,
        )
        self.assertEqual(root, "/library/Movie (2020)")
        self.assertEqual(state, "matching_name_without_nfo")

    def test_final_season_accepts_verified_ova_released_years_later(self):
        show = {
            "name": "Show",
            "seasons": [
                {"season_number": 0, "air_date": "2013-09-19"},
                {"season_number": 1, "air_date": "2013-04-05"},
                {"season_number": 2, "air_date": "2015-04-03"},
                {"season_number": 3, "air_date": "2020-07-10"},
            ],
        }
        tmdb = FakeTMDB(
            {
                "/tv/10/season/3": {
                    "episodes": [
                        {
                            "episode_number": 1,
                            "name": "Episode 1",
                            "air_date": "2020-07-10",
                        },
                        {
                            "episode_number": 12,
                            "name": "Episode 12",
                            "air_date": "2020-09-25",
                        },
                    ]
                },
                "/tv/10/season/0": {
                    "episodes": [
                        {
                            "episode_number": 1,
                            "name": "First OVA",
                            "air_date": "2013-09-19",
                        },
                        {
                            "episode_number": 2,
                            "name": "Second OVA",
                            "air_date": "2016-10-27",
                        },
                        {
                            "episode_number": 3,
                            "name": "Final-season OVA",
                            "air_date": "2023-04-27",
                        },
                    ]
                },
            }
        )
        candidates: dict[int, list[scraper.EpisodeKey]] = {}

        scraper._build_tv_episode_map(
            tmdb,
            show,
            10,
            3,
            False,
            special_season_candidates=candidates,
        )

        self.assertEqual(candidates[3], [scraper.EpisodeKey("special", 3)])

    def test_reset_sp_ordinals_map_by_fifth_and_sixth_season_windows(self):
        items = [
            {
                "name": f"Natsume Yuujinchou Go [SP{number:02d}].mkv",
                "full_path": f"/src/S05/SP/Natsume Go [SP{number:02d}].mkv",
            }
            for number in (1, 2)
        ] + [
            {
                "name": f"Natsume Yuujinchou Roku [SP{number:02d}].mkv",
                "full_path": f"/src/S06/SP/Natsume Roku [SP{number:02d}].mkv",
            }
            for number in (1, 2)
        ]
        seasons = [
            {
                "season_number": 5,
                "name": "Natsume Yuujinchou Go",
                "episode_count": 11,
                "air_date": "2016-10-05",
            },
            {
                "season_number": 6,
                "name": "Natsume Yuujinchou Roku",
                "episode_count": 11,
                "air_date": "2017-04-12",
            },
        ]

        changed = scraper._map_disc_extras_by_official_release_runs(
            items,
            show={"seasons": seasons},
            positive_seasons=seasons,
            special_runtimes={1: 24, 2: 24, 3: 24, 4: 24},
            special_air_dates={
                1: "2017-02-22", 2: "2017-03-29",
                3: "2017-09-27", 4: "2017-10-25",
            },
        )

        self.assertEqual(changed, 4)
        self.assertEqual(
            [item["_episode_key_override"] for item in items],
            [1, 2, 3, 4],
        )

    def test_postseason_overflow_prefers_explicit_official_season_titles(self):
        items = [
            {
                "name": f"Natsume Yuujinchou S5 [{number:02d}].mkv",
                "full_path": f"/src/第五季/Natsume S5 [{number:02d}].mkv",
            }
            for number in range(1, 14)
        ] + [
            {
                "name": f"Natsume Yuujinchou S6 [{number:02d}].mkv",
                "full_path": f"/src/第六季/Natsume S6 [{number:02d}].mkv",
            }
            for number in range(1, 14)
        ]
        seasons = [
            {"season_number": 5, "episode_count": 11, "air_date": "2016-10-04"},
            {"season_number": 6, "episode_count": 11, "air_date": "2017-04-12"},
        ]
        changed = scraper._map_disc_extras_by_official_release_runs(
            items,
            show={"seasons": seasons},
            positive_seasons=seasons,
            special_runtimes={8: 23, 9: 23, 10: 23, 11: 23},
            special_air_dates={
                8: "2017-03-30", 9: "2017-04-26",
                10: "2017-09-27", 11: "2017-10-25",
            },
            special_title_variants={
                8: ["第五季OVA1"], 9: ["第五季OVA2"],
                10: ["第六季OVA1"], 11: ["第六季OVA2"],
            },
        )
        self.assertEqual(changed, 4)
        mapped = {
            item["full_path"]: item["_episode_key_override"]
            for item in items if "_episode_key_override" in item
        }
        self.assertEqual(mapped["/src/第五季/Natsume S5 [12].mkv"], 8)
        self.assertEqual(mapped["/src/第五季/Natsume S5 [13].mkv"], 9)
        self.assertEqual(mapped["/src/第六季/Natsume S6 [12].mkv"], 10)
        self.assertEqual(mapped["/src/第六季/Natsume S6 [13].mkv"], 11)

    def test_natsume_fifth_and_sixth_season_overflow_stays_globally_unique(self):
        files = [
            {
                "name": f"Natsume Yuujinchou S{season} [{number:02d}].mkv",
                "full_path": (
                    f"/src/第{season}季/"
                    f"Natsume Yuujinchou S{season} [{number:02d}].mkv"
                ),
            }
            for season in (5, 6)
            for number in range(1, 14)
        ]
        seasons = [
            {
                "season_number": 5,
                "name": "夏目友人帐 伍",
                "episode_count": 11,
                "air_date": "2016-10-04",
            },
            {
                "season_number": 6,
                "name": "夏目友人帐 陆",
                "episode_count": 11,
                "air_date": "2017-04-12",
            },
        ]
        tmdb = FakeTMDB({
            "/tv/1": {
                "name": "夏目友人帐",
                "first_air_date": "2008-07-08",
                "seasons": seasons,
            },
            "/tv/1/season/5": {"episodes": [
                {"episode_number": number, "name": f"S5 Episode {number}"}
                for number in range(1, 12)
            ]},
            "/tv/1/season/6": {"episodes": [
                {"episode_number": number, "name": f"S6 Episode {number}"}
                for number in range(1, 12)
            ]},
            "/tv/1/season/0": {"episodes": [
                {
                    "episode_number": 8,
                    "name": "第五季特别篇 1：一夜杯",
                    "runtime": 23,
                    "air_date": "2017-03-30",
                },
                {
                    "episode_number": 9,
                    "name": "第五季特别篇 2：游戏之宴",
                    "runtime": 23,
                    "air_date": "2017-04-26",
                },
                {
                    "episode_number": 10,
                    "name": "第六季特别篇 1：铃响的残株",
                    "runtime": 23,
                    "air_date": "2017-09-27",
                },
                {
                    "episode_number": 11,
                    "name": "第六季特别篇 2：梦幻的碎片",
                    "runtime": 23,
                    "air_date": "2017-10-25",
                },
            ]},
        })

        plan = scraper.build_tv_plan_smart(
            auto_episode_mode=True,
            alist=FakeAList(files), tmdb_client=tmdb,
            src_path="/src", parent_path="/library", tmdb_id=1, season=1,
            absolute=False, prefer_simplified=True, allow_unmapped=False,
            episode_map_path=None, episode_group_id=None,
        )

        targets = {
            item.source_path: scraper.join_remote(item.target_dir, item.final_name)
            for item in plan.files
        }
        expected = {
            "/src/第5季/Natsume Yuujinchou S5 [12].mkv": "S00E08",
            "/src/第5季/Natsume Yuujinchou S5 [13].mkv": "S00E09",
            "/src/第6季/Natsume Yuujinchou S6 [12].mkv": "S00E10",
            "/src/第6季/Natsume Yuujinchou S6 [13].mkv": "S00E11",
        }
        for source_path, episode in expected.items():
            self.assertIn(episode, targets[source_path])
        self.assertEqual(len(set(targets.values())), len(targets))

    def test_reset_sp_ordinals_stay_unmapped_without_release_dates(self):
        items = [
            {
                "name": "Show Fifth [SP01].mkv",
                "full_path": "/src/S05/SP/Show Fifth [SP01].mkv",
            },
            {
                "name": "Show Sixth [SP01].mkv",
                "full_path": "/src/S06/SP/Show Sixth [SP01].mkv",
            },
        ]
        seasons = [
            {"season_number": 5, "name": "Show Fifth", "episode_count": 11},
            {"season_number": 6, "name": "Show Sixth", "episode_count": 11},
        ]

        changed = scraper._map_disc_extras_by_official_release_runs(
            items,
            show={"seasons": seasons},
            positive_seasons=seasons,
            special_runtimes={1: 24, 2: 24},
            special_air_dates={},
        )

        self.assertEqual(changed, 0)
        self.assertTrue(
            all("_episode_key_override" not in item for item in items)
        )

    def test_same_episode_prefers_4k_and_cleans_lower_resolution_copy(self):
        source = "/tv/Show"
        files = [
            {
                "name": "Show.S01E01.[2160p].mkv",
                "full_path": f"{source}/Show.S01E01.[2160p].mkv",
                "size": 4000,
            },
            {
                "name": "Show.S01E01.[1080p].mkv",
                "full_path": f"{source}/Show.S01E01.[1080p].mkv",
                "size": 1000,
            },
        ]
        tmdb = FakeTMDB(
            {
                "/tv/10": {
                    "name": "Show",
                    "first_air_date": "2020-01-01",
                    "poster_path": None,
                    "seasons": [{"season_number": 1}],
                },
                "/tv/10/season/1": {
                    "episodes": [{"episode_number": 1, "name": "Pilot"}]
                },
                "/tv/10/season/0": {"episodes": []},
            }
        )

        plan = scraper.build_tv_plan(
            FakeAList(files),
            tmdb,
            src_path=source,
            parent_path="/library",
            tmdb_id=10,
            season=1,
            absolute=False,
            prefer_simplified=True,
            allow_unmapped=False,
        )

        self.assertEqual([item.original_name for item in plan.files], ["Show.S01E01.[2160p].mkv"])
        self.assertEqual(
            [item.original_name for item in plan.cleanup_files],
            ["Show.S01E01.[1080p].mkv"],
        )
        self.assertIn("更高清晰度", plan.cleanup_files[0].reason)

    def test_same_episode_without_4k_prefers_1080p_over_720p(self):
        items = [
            {
                "name": "Show.S01E01.1080p.mkv",
                "full_path": "/tv/Show.S01E01.1080p.mkv",
            },
            {
                "name": "Show.S01E01.720p.mkv",
                "full_path": "/tv/Show.S01E01.720p.mkv",
            },
        ]

        kept, removed = scraper._prefer_highest_resolution_videos(items)

        self.assertEqual([item["name"] for item in kept], ["Show.S01E01.1080p.mkv"])
        self.assertEqual([item["name"] for item in removed], ["Show.S01E01.720p.mkv"])

    def test_same_resolution_prefers_larger_equivalent_release(self):
        items = [
            {
                "name": "Show.S01E01.SourceA.1080p.mkv",
                "full_path": "/tv/A/Show.S01E01.SourceA.1080p.mkv",
                "size": 2_000,
            },
            {
                "name": "Show.S01E01.SourceB.1080p.mkv",
                "full_path": "/tv/B/Show.S01E01.SourceB.1080p.mkv",
                "size": 3_000,
            },
        ]

        kept, removed = scraper._prefer_highest_resolution_videos(items)

        self.assertEqual([item["name"] for item in kept], ["Show.S01E01.SourceB.1080p.mkv"])
        self.assertEqual([item["name"] for item in removed], ["Show.S01E01.SourceA.1080p.mkv"])
        self.assertEqual(removed[0]["_duplicate_cleanup_kind"], "same_resolution_duplicate")

    def test_same_resolution_prefers_explicit_simplified_over_larger_traditional(self):
        items = [
            {
                "name": "Show.S01E01.1080p.mkv",
                "full_path": "/tv/豌豆字幕组 简日/Show.S01E01.1080p.mkv",
                "size": 2_000,
            },
            {
                "name": "Show.S01E01.1080p.mp4",
                "full_path": "/tv/ANi 繁中/Show.S01E01.1080p.mp4",
                "size": 3_000,
            },
        ]

        kept, removed = scraper._prefer_highest_resolution_videos(
            items, prefer_simplified=True
        )

        self.assertEqual([item["full_path"] for item in kept], [items[0]["full_path"]])
        self.assertEqual([item["full_path"] for item in removed], [items[1]["full_path"]])
        self.assertEqual(
            removed[0]["_duplicate_cleanup_kind"],
            "traditional_language_duplicate",
        )

    def test_same_resolution_default_still_prefers_larger_traditional_release(self):
        items = [
            {
                "name": "Show.S01E01.1080p.mkv",
                "full_path": "/tv/豌豆字幕组 简日/Show.S01E01.1080p.mkv",
                "size": 2_000,
            },
            {
                "name": "Show.S01E01.1080p.mp4",
                "full_path": "/tv/ANi 繁中/Show.S01E01.1080p.mp4",
                "size": 3_000,
            },
        ]

        kept, removed = scraper._prefer_highest_resolution_videos(items)

        self.assertEqual([item["full_path"] for item in kept], [items[1]["full_path"]])
        self.assertEqual([item["full_path"] for item in removed], [items[0]["full_path"]])

    def test_same_resolution_explicit_softsub_beats_unlabelled_release(self):
        items = [
            {
                "name": "Show.S01E01.1080p.mkv",
                "full_path": "/tv/简繁内封字幕/Show.S01E01.1080p.mkv",
                "size": 2_000,
            },
            {
                "name": "Show.S01E01.1080p.mp4",
                "full_path": "/tv/ANi/Show.S01E01.1080p.mp4",
                "size": 3_000,
            },
        ]

        kept, removed = scraper._prefer_highest_resolution_videos(items)

        self.assertEqual([item["name"] for item in kept], ["Show.S01E01.1080p.mkv"])
        self.assertEqual([item["name"] for item in removed], ["Show.S01E01.1080p.mp4"])
        self.assertEqual(removed[0]["_duplicate_cleanup_kind"], "burned_subtitle_duplicate")

    def test_same_resolution_unknown_sizes_are_not_deleted_by_path_tiebreak(self):
        items = [
            {
                "name": "Show.S01E01.SourceA.1080p.mkv",
                "full_path": "/tv/A/Show.S01E01.SourceA.1080p.mkv",
            },
            {
                "name": "Show.S01E01.SourceB.1080p.mkv",
                "full_path": "/tv/B/Show.S01E01.SourceB.1080p.mkv",
            },
        ]

        kept, removed = scraper._prefer_highest_resolution_videos(items)

        self.assertEqual(len(kept), 2)
        self.assertEqual(removed, [])

    def test_unranked_same_episode_versions_are_automatic_multi_releases(self):
        source = "/tv/Show"
        files = [
            {
                "name": "Show.S01E01.SourceA.mkv",
                "full_path": f"{source}/Show.S01E01.SourceA.mkv",
                "size": None,
            },
            {
                "name": "Show.S01E01.SourceB.mkv",
                "full_path": f"{source}/Show.S01E01.SourceB.mkv",
                "size": None,
            },
        ]
        tmdb = FakeTMDB({
            "/tv/10": {
                "name": "Show",
                "first_air_date": "2020-01-01",
                "poster_path": None,
                "seasons": [{"season_number": 1}],
            },
            "/tv/10/season/1": {
                "episodes": [{"episode_number": 1, "name": "Pilot"}],
            },
            "/tv/10/season/0": {"episodes": []},
        })

        plan = scraper.build_tv_plan(
            FakeAList(files), tmdb,
            src_path=source,
            parent_path="/library",
            tmdb_id=10,
            season=1,
            absolute=False,
            prefer_simplified=True,
            allow_unmapped=False,
        )

        self.assertEqual(len(plan.problem_files), 2)
        self.assertTrue(all(
            "已自动保留为主文件及 v2/v3 多发行版，不要求人工确认"
            in problem.reason
            for problem in plan.problem_files
        ))
        self.assertFalse(any("请确认" in problem.reason for problem in plan.problem_files))

    def test_unpaired_destination_subtitle_is_deferred_at_source(self):
        subtitle = scraper.PlannedFile(
            source_path="/src/MMR V.ass",
            source_dir="/src",
            original_name="MMR V.ass",
            final_name="Show - S00E08 - MMR V.zh-CN.ass",
            target_dir="/dst/Show/Season 00",
            media_kind="subtitle",
            episode_key="SP08",
        )
        plan = scraper.Plan("tv", "/src", "/dst/Show", [subtitle], [], {})

        scraper._demote_unpaired_subtitles(FakeAList(), plan)

        self.assertEqual(plan.files, [])
        self.assertEqual(plan.problem_files, [])
        self.assertEqual(
            [row["source_path"] for row in plan.scan_report["deferred_subtitles"]],
            ["/src/MMR V.ass"],
        )

    def test_subtitle_may_accompany_video_already_in_target_library(self):
        target_dir = "/dst/Show/Season 00"
        subtitle = scraper.PlannedFile(
            source_path="/src/MMR V.ass",
            source_dir="/src",
            original_name="MMR V.ass",
            final_name="Show - S00E08 - MMR V.zh-CN.ass",
            target_dir=target_dir,
            media_kind="subtitle",
            episode_key="SP08",
        )
        plan = scraper.Plan("tv", "/src", "/dst/Show", [subtitle], [], {})
        alist = FakeAList(
            listings={
                target_dir: [
                    {
                        "name": "Show - S00E08 - MMR V.mkv",
                        "is_dir": False,
                    }
                ]
            }
        )

        scraper._demote_unpaired_subtitles(alist, plan)

        self.assertEqual(plan.files, [subtitle])
        self.assertEqual(plan.problem_files, [])

    def test_numbered_alternate_subtitle_may_accompany_planned_video(self):
        target_dir = "/dst/Show/Season 01"
        video = scraper.PlannedFile(
            source_path="/src/Show.mkv", source_dir="/src",
            original_name="Show.mkv", final_name="Show - S01E01 - One.mkv",
            target_dir=target_dir, media_kind="video", episode_key="E01",
        )
        subtitle = scraper.PlannedFile(
            source_path="/src/Show.alt.ass", source_dir="/src",
            original_name="Show.alt.ass",
            final_name="Show - S01E01 - One.zh-CN.2.3.ass",
            target_dir=target_dir, media_kind="subtitle", episode_key="E01",
        )
        plan = scraper.Plan("tv", "/src", "/dst/Show", [video, subtitle], [], {})

        scraper._demote_unpaired_subtitles(FakeAList(), plan)

        self.assertEqual(plan.files, [video, subtitle])
        self.assertEqual(plan.problem_files, [])

    def test_4k_priority_does_not_remove_distinct_directors_cut(self):
        items = [
            {
                "name": "Show.S01E01.2160p.mkv",
                "full_path": "/tv/Show.S01E01.2160p.mkv",
            },
            {
                "name": "Show.S01E01.Directors.Cut.1080p.mkv",
                "full_path": "/tv/Show.S01E01.Directors.Cut.1080p.mkv",
            },
        ]

        kept, removed = scraper._prefer_highest_resolution_videos(items)

        self.assertEqual(len(kept), 2)
        self.assertEqual(removed, [])

    def test_distant_4k_collection_root_does_not_label_1080p_descendant(self):
        item = {
            "name": "01.Ani繁中.mp4",
            "full_path": (
                "/media/R 4k Complete/Show/S03.Part1.2024/"
                "Ani.繁中/01.Ani繁中.mp4"
            ),
        }
        self.assertEqual(scraper.video_resolution_rank(item), 0)
        self.assertEqual(
            scraper.video_resolution_rank(
                {
                    "name": "01.mkv",
                    "full_path": "/media/Show/4K 内封软字幕/01.mkv",
                }
            ),
            2160,
        )

    def test_same_resolution_prefers_soft_subtitle_release(self):
        source = "/tv/Show"
        files = [
            {
                "name": "01.mkv",
                "full_path": f"{source}/4K 内封软字幕/01.mkv",
                "size": 4000,
            },
            {
                "name": "01.mp4",
                "full_path": f"{source}/4K 内嵌硬字幕/01.mp4",
                "size": 4100,
            },
            {
                "name": "01.ass",
                "full_path": f"{source}/4K 内封软字幕/字幕备份/01.ass",
                "size": 100,
            },
        ]
        tmdb = FakeTMDB(
            {
                "/tv/10": {
                    "name": "Show",
                    "first_air_date": "2020-01-01",
                    "poster_path": None,
                    "seasons": [{"season_number": 1}],
                },
                "/tv/10/season/1": {
                    "episodes": [{"episode_number": 1, "name": "Pilot"}]
                },
                "/tv/10/season/0": {"episodes": []},
            }
        )

        plan = scraper.build_tv_plan(
            FakeAList(files),
            tmdb,
            src_path=source,
            parent_path="/library",
            tmdb_id=10,
            season=1,
            absolute=False,
            prefer_simplified=True,
            allow_unmapped=False,
        )

        self.assertEqual(
            [item.original_name for item in plan.files],
            ["01.mkv", "01.ass"],
        )
        self.assertEqual(
            [item.original_name for item in plan.cleanup_files],
            ["01.mp4"],
        )
        self.assertIn("内封/软字幕", plan.cleanup_files[0].reason)
        self.assertFalse(plan.problem_files)

    def test_lower_resolution_video_does_not_claim_high_release_subtitle(self):
        source = "/tv/Show"
        high_subtitle = {
            "name": "01.ass",
            "full_path": f"{source}/4K 内封软字幕/字幕备份/01.ass",
            "size": 100,
        }
        items = [
            {
                "name": "01.mkv",
                "full_path": f"{source}/4K 内封软字幕/01.mkv",
                "size": 4000,
            },
            {
                "name": "01.mp4",
                "full_path": f"{source}/1080P 内嵌字幕/01.mp4",
                "size": 2000,
            },
            high_subtitle,
        ]

        kept, removed = scraper._prefer_highest_resolution_videos(items)

        self.assertIn(high_subtitle, kept)
        self.assertEqual(
            [item["full_path"] for item in removed],
            [f"{source}/1080P 内嵌字幕/01.mp4"],
        )

    def test_lower_release_subtitle_is_removed_only_with_distinct_high_companion(self):
        source = "/tv/Show"
        items = [
            {
                "name": "01.mkv",
                "full_path": f"{source}/4K/01.mkv",
                "size": 4000,
            },
            {
                "name": "01.ass",
                "full_path": f"{source}/4K/字幕备份/01.ass",
                "size": 100,
            },
            {
                "name": "01.mp4",
                "full_path": f"{source}/1080P/01.mp4",
                "size": 2000,
            },
            {
                "name": "01.ass",
                "full_path": f"{source}/1080P/字幕备份/01.ass",
                "size": 90,
            },
        ]

        kept, removed = scraper._prefer_highest_resolution_videos(items)

        self.assertEqual(
            sorted(item["full_path"] for item in kept),
            sorted([
                f"{source}/4K/01.mkv",
                f"{source}/4K/字幕备份/01.ass",
            ]),
        )
        self.assertEqual(
            sorted(item["full_path"] for item in removed),
            sorted([
                f"{source}/1080P/01.mp4",
                f"{source}/1080P/字幕备份/01.ass",
            ]),
        )

    def test_flattened_multi_season_sequence_uses_official_boundaries(self):
        source = "/tv/Show 1-2季"
        files = [
            {"name": "00.mkv", "full_path": f"{source}/00.mkv", "size": 5100},
            {"name": "01.mkv", "full_path": f"{source}/01.mkv"},
            {"name": "02.mkv", "full_path": f"{source}/02.mkv"},
            {"name": "03.mkv", "full_path": f"{source}/03.mkv"},
            {"name": "SP.mkv", "full_path": f"{source}/SP.mkv", "size": 1000},
        ]
        tmdb = FakeTMDB(
            {
                "/tv/10": {
                    "name": "Show",
                    "first_air_date": "2020-01-01",
                    "poster_path": None,
                    "seasons": [
                        {"season_number": 0, "episode_count": 2},
                        {"season_number": 1, "episode_count": 2},
                        {"season_number": 2, "episode_count": 1},
                    ],
                },
                "/tv/10/season/0": {
                    "episodes": [
                        {"episode_number": 1, "name": "Prologue", "runtime": 51},
                        {"episode_number": 2, "name": "Sunny Day", "runtime": 10},
                    ]
                },
                "/tv/10/season/1": {
                    "episodes": [
                        {"episode_number": 1, "name": "One"},
                        {"episode_number": 2, "name": "Two"},
                    ]
                },
                "/tv/10/season/2": {
                    "episodes": [{"episode_number": 1, "name": "Three"}]
                },
            }
        )

        plan = scraper.build_tv_plan_smart(
            auto_episode_mode=True,
            alist=FakeAList(files),
            tmdb_client=tmdb,
            src_path=source,
            parent_path="/library",
            tmdb_id=10,
            season=1,
            absolute=False,
            prefer_simplified=True,
            allow_unmapped=False,
            ignore_orphan_temp=False,
            episode_map_path=None,
            episode_group_id=None,
            source_files=files,
        )

        targets = {item.original_name: item.final_name for item in plan.files}
        self.assertIn("S00E01", targets["00.mkv"])
        self.assertIn("S00E02", targets["SP.mkv"])
        self.assertIn("S01E01", targets["01.mkv"])
        self.assertIn("S01E02", targets["02.mkv"])
        self.assertIn("S02E01", targets["03.mkv"])
        self.assertFalse(plan.problem_files)

    def test_proven_flattened_sequence_is_not_reclassified_as_an_independent_sequel(self):
        source = "/tv/Show 1-2季"
        files = [
            {"name": "01.mkv", "full_path": f"{source}/01.mkv"},
            {"name": "02.mkv", "full_path": f"{source}/02.mkv"},
            {"name": "03.mkv", "full_path": f"{source}/03.mkv"},
        ]
        tmdb = FakeTMDB(
            {
                "/tv/10": {
                    "name": "Show",
                    "first_air_date": "2020-01-01",
                    "poster_path": None,
                    "seasons": [
                        {"season_number": 1, "episode_count": 2},
                        {"season_number": 2, "episode_count": 1},
                    ],
                },
                "/tv/10/season/0": {"episodes": []},
                "/tv/10/season/1": {
                    "episodes": [
                        {"episode_number": 1, "name": "One"},
                        {"episode_number": 2, "name": "Two"},
                    ]
                },
                "/tv/10/season/2": {
                    "episodes": [{"episode_number": 1, "name": "Three"}]
                },
            }
        )

        with mock.patch.object(
            scraper,
            "auto_match_tmdb",
            side_effect=AssertionError(
                "a proven flattened official season must not be searched as a sequel"
            ),
        ):
            plan = scraper.build_tv_plan_smart(
                auto_episode_mode=True,
                alist=FakeAList(files),
                tmdb_client=tmdb,
                src_path=source,
                parent_path="/tv",
                tmdb_id=10,
                season=1,
                absolute=False,
                prefer_simplified=True,
                allow_unmapped=False,
                ignore_orphan_temp=False,
                episode_map_path=None,
                episode_group_id=None,
                source_files=files,
            )

        targets = {item.original_name: item.final_name for item in plan.files}
        self.assertIn("S01E01", targets["01.mkv"])
        self.assertIn("S01E02", targets["02.mkv"])
        self.assertIn("S02E01", targets["03.mkv"])

    def test_unnumbered_special_is_not_inferred_from_only_remaining_candidate(self):
        source = "/tv/Show 1-2季"
        files = [
            {"name": "00.mkv", "full_path": f"{source}/00.mkv"},
            {"name": "01.mkv", "full_path": f"{source}/01.mkv"},
            {"name": "02.mkv", "full_path": f"{source}/02.mkv"},
            {"name": "03.mkv", "full_path": f"{source}/03.mkv"},
            {"name": "SP.mkv", "full_path": f"{source}/SP.mkv"},
        ]
        tmdb = FakeTMDB(
            {
                "/tv/10": {
                    "name": "Show",
                    "first_air_date": "2020-01-01",
                    "poster_path": None,
                    "seasons": [
                        {"season_number": 0, "episode_count": 2},
                        {"season_number": 1, "episode_count": 2},
                        {"season_number": 2, "episode_count": 1},
                    ],
                },
                "/tv/10/season/0": {
                    "episodes": [
                        {"episode_number": 1, "name": "Prologue", "runtime": 51},
                        {"episode_number": 2, "name": "Unrelated", "runtime": 10},
                    ]
                },
                "/tv/10/season/1": {
                    "episodes": [
                        {"episode_number": 1, "name": "One"},
                        {"episode_number": 2, "name": "Two"},
                    ]
                },
                "/tv/10/season/2": {
                    "episodes": [{"episode_number": 1, "name": "Three"}]
                },
            }
        )

        plan = scraper.build_tv_plan_smart(
            auto_episode_mode=True,
            alist=FakeAList(files),
            tmdb_client=tmdb,
            src_path=source,
            parent_path="/library",
            tmdb_id=10,
            season=1,
            absolute=False,
            prefer_simplified=True,
            allow_unmapped=False,
            ignore_orphan_temp=False,
            episode_map_path=None,
            episode_group_id=None,
            source_files=files,
        )

        self.assertTrue(
            any(item.source_path.endswith("/SP.mkv") for item in plan.problem_files)
        )

    def test_tv_e00_maps_to_first_tmdb_special(self):
        source = "/tv/Show {tmdb-10}"
        files = [{"name": "Show.S01E00.Prologue.mkv", "full_path": f"{source}/Show.S01E00.Prologue.mkv"}]
        plan = scraper.build_tv_plan(
            FakeAList(files),
            FakeTMDB(
                {
                    "/tv/10": {
                        "name": "Show",
                        "first_air_date": "2020-01-01",
                        "poster_path": None,
                        "seasons": [{"season_number": 0}, {"season_number": 1}],
                    },
                    "/tv/10/season/1": {"episodes": [{"episode_number": 1, "name": "Pilot"}]},
                    "/tv/10/season/0": {"episodes": [{"episode_number": 1, "name": "Prologue"}]},
                }
            ),
            src_path=source,
            parent_path="/tv",
            tmdb_id=10,
            season=1,
            absolute=False,
            prefer_simplified=True,
            allow_unmapped=False,
        )
        self.assertIn("S00E01", plan.files[0].final_name)
        self.assertTrue(any("E00" in warning for warning in plan.warnings))

    def test_tv_e00_is_not_inferred_from_only_one_official_special(self):
        source = "/tv/Show {tmdb-10}"
        files = [{
            "name": "Show.S01E00.mkv",
            "full_path": f"{source}/Show.S01E00.mkv",
        }]
        plan = scraper.build_tv_plan(
            FakeAList(files),
            FakeTMDB(
                {
                    "/tv/10": {
                        "name": "Show",
                        "first_air_date": "2020-01-01",
                        "poster_path": None,
                        "seasons": [{"season_number": 0}, {"season_number": 1}],
                    },
                    "/tv/10/season/1": {
                        "episodes": [{"episode_number": 1, "name": "Pilot"}]
                    },
                    "/tv/10/season/0": {
                        "episodes": [{"episode_number": 1, "name": "Unrelated Special"}]
                    },
                }
            ),
            src_path=source,
            parent_path="/tv",
            tmdb_id=10,
            season=1,
            absolute=False,
            prefer_simplified=True,
            allow_unmapped=True,
        )

        self.assertIn("S01E00", plan.files[0].final_name)
        self.assertTrue(
            any(item.source_path.endswith("/Show.S01E00.mkv") for item in plan.problem_files)
        )

    def test_complete_zero_based_disc_season_shifts_to_one_based_tmdb_episodes(self):
        source = "/tv/Show Season 4"
        files = [
            {"name": f"Show Hero {episode:02d}.mkv", "full_path": f"{source}/Show Hero {episode:02d}.mkv"}
            for episode in range(3)
        ]
        plan = scraper.build_tv_plan(
            FakeAList(files),
            FakeTMDB(
                {
                    "/tv/10": {
                        "name": "Show",
                        "first_air_date": "2020-01-01",
                        "poster_path": None,
                        "seasons": [{"season_number": 4, "episode_count": 3}],
                    },
                    "/tv/10/season/4": {
                        "episodes": [
                            {"episode_number": 1, "name": "One"},
                            {"episode_number": 2, "name": "Two"},
                            {"episode_number": 3, "name": "Three"},
                        ]
                    },
                    "/tv/10/season/0": {"episodes": []},
                }
            ),
            src_path=source,
            parent_path="/tv",
            tmdb_id=10,
            season=4,
            absolute=False,
            prefer_simplified=True,
            allow_unmapped=False,
        )

        targets = {item.original_name: item.final_name for item in plan.files}
        self.assertIn("S04E01", targets["Show Hero 00.mkv"])
        self.assertIn("S04E02", targets["Show Hero 01.mkv"])
        self.assertIn("S04E03", targets["Show Hero 02.mkv"])
        self.assertTrue(any("零基编号" in warning for warning in plan.warnings))

    def test_bare_e00_maps_when_official_special_uniquely_says_episode_zero(self):
        source = "/tv/Show {tmdb-10}"
        files = [
            {"name": "Show [00].mkv", "full_path": f"{source}/Show [00].mkv"},
            {"name": "Show [00].ass", "full_path": f"{source}/backup/Show [00].ass"},
        ]
        plan = scraper.build_tv_plan(
            FakeAList(files),
            FakeTMDB(
                {
                    "/tv/10": {
                        "name": "Show",
                        "first_air_date": "2020-01-01",
                        "poster_path": None,
                        "seasons": [{"season_number": 0}, {"season_number": 1}],
                    },
                    "/tv/10/season/1": {
                        "episodes": [{"episode_number": 1, "name": "Pilot"}]
                    },
                    "/tv/10/season/0": {
                        "episodes": [
                            {"episode_number": 1, "name": "Episode:0 Meeting Time"},
                            {"episode_number": 2, "name": "Unrelated Special"},
                        ]
                    },
                }
            ),
            src_path=source,
            parent_path="/tv",
            tmdb_id=10,
            season=1,
            absolute=False,
            prefer_simplified=True,
            allow_unmapped=False,
        )
        self.assertEqual(len(plan.files), 2)
        self.assertTrue(all("S00E01" in item.final_name for item in plan.files))
        self.assertTrue(any("Episode 0" in warning for warning in plan.warnings))

    def test_bare_zero_prefers_unique_special_referencing_same_season(self):
        titles = {
            (0, 1): ["第 2 季 第 0 话：单恋"],
            (0, 6): ["第 3 季前情提要：10分钟回顾"],
        }
        self.assertEqual(
            scraper._unique_season_referenced_special_candidate(titles, 3),
            [scraper.EpisodeKey("special", 6)],
        )

    def test_e00_independent_movie_requires_same_unique_search_result(self):
        files = [
            {
                "name": (
                    "[Group] Fate Strange Fake [00][Whispers of Dawn]"
                    "[1080p].mp4"
                ),
                "full_path": (
                    "/tv/Fate Strange Fake/[Group] Fate Strange Fake "
                    "[00][Whispers of Dawn][1080p].mp4"
                ),
            },
            {
                "name": (
                    "Fate.strange.Fake.S01E00.Whispers.of.Dawn.2023."
                    "1080p.WEB-DL.mkv"
                ),
                "full_path": (
                    "/tv/Fate Strange Fake/Fate.strange.Fake.S01E00."
                    "Whispers.of.Dawn.2023.1080p.WEB-DL.mkv"
                ),
            },
        ]

        def fake_match(_client, query, **_kwargs):
            normalized = scraper._normalize_match_title(query)
            if (
                "fatestrangefake" in normalized
                and "whispersofdawn" in normalized
            ):
                match = scraper.AutoMatch(
                    "movie",
                    1145612,
                    "Fate/strange Fake -Whispers of Dawn-",
                    "2023",
                    1.0,
                )
                return match, [match]
            raise scraper.PlanError("no match")

        with mock.patch.object(scraper, "auto_match_tmdb", side_effect=fake_match):
            result = scraper._e00_independent_movie_match(
                object(),
                files,
                {"name": "命运／奇异赝品", "original_name": "Fate/strange Fake"},
            )

        self.assertIsNotNone(result)
        self.assertEqual(result.tmdb_id, 1145612)

    def test_generic_e00_cannot_become_independent_movie(self):
        file = {
            "name": "Show.S01E00.1080p.mkv",
            "full_path": "/tv/Show/Show.S01E00.1080p.mkv",
        }
        with mock.patch.object(scraper, "auto_match_tmdb") as matcher:
            result = scraper._e00_independent_movie_match(
                object(),
                [file],
                {"name": "Show", "original_name": "Show"},
            )
        self.assertIsNone(result)
        matcher.assert_not_called()

    def test_tv_e00_uses_unique_matching_prologue_instead_of_sp1(self):
        source = "/tv/Show {tmdb-10}"
        files = [{
            "name": "Show.S01E00.Prologue.mkv",
            "full_path": f"{source}/Show.S01E00.Prologue.mkv",
        }]
        plan = scraper.build_tv_plan(
            FakeAList(files),
            FakeTMDB(
                {
                    "/tv/10": {
                        "name": "Show",
                        "first_air_date": "2020-01-01",
                        "poster_path": None,
                        "seasons": [{"season_number": 0}, {"season_number": 1}],
                    },
                    "/tv/10/season/1": {
                        "episodes": [{"episode_number": 1, "name": "Pilot"}]
                    },
                    "/tv/10/season/0": {
                        "episodes": [
                            {"episode_number": 1, "name": "Interview"},
                            {"episode_number": 2, "name": "Prologue"},
                        ]
                    },
                }
            ),
            src_path=source,
            parent_path="/tv",
            tmdb_id=10,
            season=1,
            absolute=False,
            prefer_simplified=True,
            allow_unmapped=False,
        )

        self.assertIn("S00E02", plan.files[0].final_name)

    def test_tv_e00_uses_unique_full_length_preseason_timeline_evidence(self):
        source = "/tv/Show {tmdb-10}"
        files = [
            {
                "name": f"Show.{number:02d}.mkv",
                "full_path": f"{source}/Show.{number:02d}.mkv",
            }
            for number in range(3)
        ]
        plan = scraper.build_tv_plan(
            FakeAList(files),
            FakeTMDB(
                {
                    "/tv/10": {
                        "name": "Show",
                        "first_air_date": "2020-01-01",
                        "poster_path": None,
                        "seasons": [{"season_number": 0}, {"season_number": 1}],
                    },
                    "/tv/10/season/1": {
                        "episodes": [
                            {
                                "episode_number": 1,
                                "name": "Pilot",
                                "air_date": "2020-10-01",
                                "runtime": 24,
                            },
                            {
                                "episode_number": 2,
                                "name": "Second",
                                "air_date": "2020-10-08",
                                "runtime": 24,
                            },
                        ]
                    },
                    "/tv/10/season/0": {
                        "episodes": [
                            {
                                "episode_number": 1,
                                "name": "Initium",
                                "air_date": "2020-08-01",
                                "runtime": 26,
                            },
                            {
                                "episode_number": 2,
                                "name": "Old Special",
                                "air_date": "2019-01-01",
                                "runtime": 24,
                            },
                            {
                                "episode_number": 3,
                                "name": "Short Promo",
                                "air_date": "2020-09-01",
                                "runtime": 3,
                            },
                        ]
                    },
                }
            ),
            src_path=source,
            parent_path="/tv",
            tmdb_id=10,
            season=1,
            absolute=False,
            prefer_simplified=True,
            allow_unmapped=False,
        )

        targets = {item.original_name: item.final_name for item in plan.files}
        self.assertIn("S00E01", targets["Show.00.mkv"])
        warning = next(item for item in plan.warnings if "开播时间" in item)
        scraper.finalize_plan_evidence(plan)
        notice = next(item for item in plan.notices if item.message == warning)
        self.assertFalse(notice.requires_review)

    def test_tv_e00_accepts_official_episode_zero_six_months_before_season(self):
        source = "/tv/Show {tmdb-10}"
        files = [
            {
                "name": f"{number:02d}.mkv",
                "full_path": f"{source}/{number:02d}.mkv",
            }
            for number in range(3)
        ]
        plan = scraper.build_tv_plan(
            FakeAList(files),
            FakeTMDB(
                {
                    "/tv/10": {
                        "name": "Show",
                        "first_air_date": "2019-07-07",
                        "poster_path": None,
                        "seasons": [{"season_number": 0}, {"season_number": 1}],
                    },
                    "/tv/10/season/1": {
                        "episodes": [
                            {
                                "episode_number": 1,
                                "name": "One",
                                "air_date": "2019-07-07",
                                "runtime": 24,
                            },
                            {
                                "episode_number": 2,
                                "name": "Two",
                                "air_date": "2019-07-14",
                                "runtime": 24,
                            },
                        ]
                    },
                    "/tv/10/season/0": {
                        "episodes": [
                            {
                                "episode_number": 1,
                                "name": "Episode 0",
                                "air_date": "2018-12-31",
                                "runtime": 25,
                            },
                            {
                                "episode_number": 2,
                                "name": "Later Special",
                                "air_date": "2021-12-31",
                                "runtime": 53,
                            },
                        ]
                    },
                }
            ),
            src_path=source,
            parent_path="/tv",
            tmdb_id=10,
            season=1,
            absolute=False,
            prefer_simplified=True,
            allow_unmapped=False,
        )

        targets = {item.original_name: item.final_name for item in plan.files}
        self.assertIn("S00E01", targets["00.mkv"])

    def test_tv_e00_excludes_recap_with_misleading_preseason_date(self):
        source = "/tv/Show {tmdb-10}"
        files = [
            {
                "name": f"{number:02d}.mkv",
                "full_path": f"{source}/{number:02d}.mkv",
            }
            for number in range(3)
        ]
        plan = scraper.build_tv_plan(
            FakeAList(files),
            FakeTMDB(
                {
                    "/tv/10": {
                        "name": "Show",
                        "first_air_date": "2019-10-05",
                        "poster_path": None,
                        "seasons": [{"season_number": 0}, {"season_number": 1}],
                    },
                    "/tv/10/season/1": {
                        "episodes": [
                            {
                                "episode_number": 1,
                                "name": "One",
                                "air_date": "2019-10-05",
                                "runtime": 24,
                            },
                            {
                                "episode_number": 2,
                                "name": "Two",
                                "air_date": "2019-10-12",
                                "runtime": 24,
                            },
                        ]
                    },
                    "/tv/10/season/0": {
                        "episodes": [
                            {
                                "episode_number": 1,
                                "name": "The Beginning of the Journey",
                                "air_date": "2019-08-04",
                                "runtime": 26,
                            },
                            {
                                "episode_number": 4,
                                "name": "总集篇3：决战",
                                "air_date": "2018-12-31",
                                "runtime": 24,
                            },
                        ]
                    },
                }
            ),
            src_path=source,
            parent_path="/tv",
            tmdb_id=10,
            season=1,
            absolute=False,
            prefer_simplified=True,
            allow_unmapped=False,
        )

        targets = {item.original_name: item.final_name for item in plan.files}
        self.assertIn("S00E01", targets["00.mkv"])

    def test_tv_e00_does_not_use_only_remaining_special_without_evidence(self):
        source = "/tv/Show {tmdb-10}"
        files = [
            {
                "name": f"{number:02d}.mkv",
                "full_path": f"{source}/{number:02d}.mkv",
            }
            for number in range(3)
        ]
        plan = scraper.build_tv_plan(
            FakeAList(files),
            FakeTMDB(
                {
                    "/tv/10": {
                        "name": "Show",
                        "first_air_date": "2020-01-01",
                        "poster_path": None,
                        "seasons": [{"season_number": 0}, {"season_number": 1}],
                    },
                    "/tv/10/season/1": {
                        "episodes": [
                            {
                                "episode_number": 1,
                                "name": "One",
                                "air_date": "2020-01-01",
                                "runtime": 24,
                            },
                            {
                                "episode_number": 2,
                                "name": "Two",
                                "air_date": "2020-01-08",
                                "runtime": 24,
                            },
                        ]
                    },
                    "/tv/10/season/0": {
                        "episodes": [
                            {
                                "episode_number": 1,
                                "name": "Unrelated Special",
                                "air_date": "2017-01-01",
                                "runtime": 24,
                            }
                        ]
                    },
                }
            ),
            src_path=source,
            parent_path="/tv",
            tmdb_id=10,
            season=1,
            absolute=False,
            prefer_simplified=True,
            allow_unmapped=True,
        )

        targets = {item.original_name: item.final_name for item in plan.files}
        self.assertIn("S01E00", targets["00.mkv"])
        self.assertTrue(
            any(item.source_path.endswith("/00.mkv") for item in plan.problem_files)
        )

    def test_unnumbered_special_requires_unique_online_candidate_and_runtime_ratio(self):
        source = "/tv/Show {tmdb-10}"
        files = [
            {
                "name": "00.mkv",
                "full_path": f"{source}/00.mkv",
                "size": 1_000,
            },
            {
                "name": "01.mkv",
                "full_path": f"{source}/01.mkv",
                "size": 1_000,
            },
            {
                "name": "Special.mkv",
                "full_path": f"{source}/Special.mkv",
                "size": 2_100,
            },
        ]
        tmdb = FakeTMDB(
            {
                "/tv/10": {
                    "name": "Show",
                    "first_air_date": "2019-07-07",
                    "poster_path": None,
                    "seasons": [{"season_number": 0}, {"season_number": 1}],
                },
                "/tv/10/season/1": {
                    "episodes": [
                        {
                            "episode_number": 1,
                            "name": "One",
                            "air_date": "2019-07-07",
                            "runtime": 24,
                        }
                    ]
                },
                "/tv/10/season/0": {
                    "episodes": [
                        {
                            "episode_number": 1,
                            "name": "Episode 0",
                            "air_date": "2018-12-31",
                            "runtime": 25,
                        },
                        {
                            "episode_number": 2,
                            "name": "Reunion",
                            "air_date": "2021-12-31",
                            "runtime": 53,
                        },
                    ]
                },
            }
        )

        plan = scraper.build_tv_plan(
            FakeAList(files),
            tmdb,
            src_path=source,
            parent_path="/tv",
            tmdb_id=10,
            season=1,
            absolute=False,
            prefer_simplified=True,
            allow_unmapped=False,
            auto_special_title_match=True,
        )

        targets = {item.original_name: item.final_name for item in plan.files}
        self.assertIn("S00E01", targets["00.mkv"])
        self.assertIn("S00E02", targets["Special.mkv"])
        self.assertFalse(
            any(item.source_path.endswith("/Special.mkv") for item in plan.problem_files)
        )
        self.assertTrue(any("文件大小比例" in warning for warning in plan.warnings))

    def test_unnumbered_special_stays_unmapped_when_runtime_ratio_disagrees(self):
        groups = {
            scraper.EpisodeKey("special", 1): [
                {
                    "name": "00.mkv",
                    "full_path": "/tv/Show/00.mkv",
                    "size": 1_000,
                }
            ]
        }
        files = [
            *groups[scraper.EpisodeKey("special", 1)],
            {
                "name": "Special.mkv",
                "full_path": "/tv/Show/Special.mkv",
                "size": 900,
            },
        ]
        tmdb = FakeTMDB(
            {
                "/tv/10/season/0": {
                    "episodes": [
                        {"episode_number": 1, "name": "Episode 0", "runtime": 25},
                        {"episode_number": 2, "name": "Long Special", "runtime": 53},
                    ]
                }
            }
        )

        warnings = scraper._map_unique_remaining_unnumbered_special_by_runtime(
            tmdb,
            10,
            files,
            groups,
            {
                scraper.EpisodeKey("special", 1): "Episode 0",
                scraper.EpisodeKey("special", 2): "Long Special",
            },
        )

        self.assertEqual(warnings, [])
        self.assertNotIn(scraper.EpisodeKey("special", 2), groups)

    def test_unnumbered_sp_uses_matching_ass_script_info_episode_ordinal(self):
        class SubtitleAList:
            def read_file_bytes(self, path, *, max_bytes):
                self.path = path
                self.max_bytes = max_bytes
                return (
                    "[Script Info]\nTitle: Steins;Gate 25 gb\n"
                    "[Events]\nDialogue: 0,0:00:00.00,0:00:01.00,Default,,"
                    "0,0,0,,untrusted dialogue"
                ).encode("utf-8")

        groups = {
            scraper.EpisodeKey("special", 6): [
                {
                    "name": "Steins;Gate [23B].mkv",
                    "full_path": "/show/Steins;Gate [23B].mkv",
                    "size": 1_000,
                }
            ]
        }
        files = [
            *groups[scraper.EpisodeKey("special", 6)],
            {
                "name": "Steins;Gate [SP].mkv",
                "full_path": "/show/Steins;Gate [SP].mkv",
                "size": 1_010,
            },
            {
                "name": "Steins;Gate[SP].ass",
                "full_path": "/show/subs/Steins;Gate[SP].ass",
                "size": 200,
            },
        ]

        warnings = scraper._map_unnumbered_special_from_subtitle_title(
            SubtitleAList(),
            files,
            groups,
            {
                scraper.EpisodeKey("special", 1): "Egoistic Poriomania",
                scraper.EpisodeKey("special", 6): "Open the Missing Link",
            },
            series_titles=["命运石之门", "Steins;Gate"],
            regular_episode_count=24,
        )

        self.assertTrue(any("Script Info" in warning for warning in warnings))
        mapped = groups[scraper.EpisodeKey("special", 1)]
        self.assertEqual(
            {item["name"] for item in mapped},
            {"Steins;Gate [SP].mkv", "Steins;Gate[SP].ass"},
        )

    def test_unnumbered_sp_rejects_unrelated_ass_title_ordinal(self):
        class SubtitleAList:
            def read_file_bytes(self, path, *, max_bytes):
                return b"[Script Info]\nTitle: Other Show 25\n[Events]\n"

        groups = {}
        files = [
            {"name": "Show [SP].mkv", "full_path": "/show/Show [SP].mkv", "size": 1000},
            {"name": "Show [SP].ass", "full_path": "/show/Show [SP].ass", "size": 100},
        ]
        warnings = scraper._map_unnumbered_special_from_subtitle_title(
            SubtitleAList(),
            files,
            groups,
            {scraper.EpisodeKey("special", 1): "Special"},
            series_titles=["Show"],
            regular_episode_count=24,
        )

        self.assertEqual(warnings, [])
        self.assertEqual(groups, {})

    def test_unnumbered_sp_ordinal_uses_unique_postseason_full_length_timeline(self):
        class SubtitleAList:
            def read_file_bytes(self, path, *, max_bytes):
                return b"[Script Info]\nTitle: Steins;Gate 25 gb\n[Events]\n"

        files = [
            {"name": "Steins;Gate [SP].mkv", "full_path": "/show/SP.mkv", "size": 1000},
            {"name": "Steins;Gate [SP].ass", "full_path": "/show/SP.ass", "size": 100},
        ]
        groups = {}
        tmdb = FakeTMDB(
            {
                "/tv/10/season/1": {
                    "episodes": [
                        {
                            "episode_number": number,
                            "air_date": f"2011-09-{number:02d}",
                            "runtime": 24,
                        }
                        for number in range(1, 25)
                    ]
                },
                "/tv/10/season/0": {
                    "episodes": [
                        {"episode_number": 1, "air_date": "2012-02-22", "runtime": 24},
                        {"episode_number": 6, "air_date": "2015-12-03", "runtime": 24},
                    ]
                },
            }
        )
        warnings = scraper._map_unnumbered_special_from_subtitle_title(
            SubtitleAList(),
            files,
            groups,
            {
                scraper.EpisodeKey("special", 1): "Egoistic Poriomania",
                scraper.EpisodeKey("special", 6): "Open the Missing Link",
            },
            series_titles=["Steins;Gate"],
            regular_episode_count=24,
            tmdb_client=tmdb,
            tmdb_id=10,
            season=1,
        )
        self.assertTrue(any("季终后一年内" in warning for warning in warnings))
        self.assertIn(scraper.EpisodeKey("special", 1), groups)

    def test_ova_volume_folders_map_one_packed_video_to_each_official_season(self):
        source = "/tv/Show OVA"
        files = [
            {
                "name": "Show OVA [01].mkv",
                "full_path": f"{source}/OVA 01（2021.6）/Show OVA [01].mkv",
                "size": 1_000,
            },
            {
                "name": "Show OVA 2nd Season.mkv",
                "full_path": (
                    f"{source}/OVA 02（2021.10）/Show OVA 2nd Season.mkv"
                ),
                "size": 1_100,
            },
        ]
        tmdb = FakeTMDB(
            {
                "/tv/10": {
                    "name": "Show OVA",
                    "original_name": "Show OVA",
                    "first_air_date": "2021-06-01",
                    "poster_path": None,
                    "seasons": [
                        {"season_number": 1, "episode_count": 2},
                        {"season_number": 2, "episode_count": 2},
                    ],
                },
                "/tv/10/season/0": {"episodes": []},
                "/tv/10/season/1": {
                    "episodes": [
                        {"episode_number": 1, "name": "One"},
                        {"episode_number": 2, "name": "Two"},
                    ]
                },
                "/tv/10/season/2": {
                    "episodes": [
                        {"episode_number": 1, "name": "Three"},
                        {"episode_number": 2, "name": "Four"},
                    ]
                },
            }
        )

        plan = scraper.build_tv_plan_smart(
            auto_episode_mode=True,
            alist=FakeAList(files),
            tmdb_client=tmdb,
            src_path=source,
            parent_path="/library",
            tmdb_id=10,
            season=1,
            absolute=False,
            prefer_simplified=True,
            allow_unmapped=False,
            ignore_orphan_temp=False,
            episode_map_path=None,
            episode_group_id=None,
            source_files=files,
        )

        targets = {item.original_name: item for item in plan.files}
        self.assertIn("S01E01-E02", targets["Show OVA [01].mkv"].final_name)
        self.assertIn(
            "S02E01-E02",
            targets["Show OVA 2nd Season.mkv"].final_name,
        )
        self.assertTrue(any("合并集" in warning for warning in plan.warnings))

    def test_ova_volume_ordinal_supports_release_naming_variants(self):
        cases = {
            "OVA 01（2021.6）": 1,
            "1st Season": 1,
            "2nd Season": 2,
            "Vol.1": 1,
            "Volume 02": 2,
            "上卷": 1,
            "下卷": 2,
            "前篇": 1,
            "後篇": 2,
            "Season": None,
        }
        for label, expected in cases.items():
            with self.subTest(label=label):
                self.assertEqual(scraper._ova_volume_ordinal(label), expected)

    def test_structured_special_notices_use_explicit_evidence_sources(self):
        plan = scraper.Plan(
            mode="tv",
            source_root="/src",
            target_root="/dst",
            files=[
                scraper.PlannedFile(
                    source_path="/src/Show.SP01.mkv",
                    source_dir="/src",
                    original_name="Show.SP01.mkv",
                    final_name="Show - S00E01.mkv",
                    target_dir="/dst/Season 00",
                    media_kind="video",
                )
            ],
            warnings=[
                "根据 TMDB 官方特别篇标题将 OVA 自动映射为 SP01",
                "根据第 1 季官方时间线将 OAD 自动映射为 SP02",
                "SP03 未在 TMDB 特别篇中找到，已按明确编号保留到 Season 00",
                "E13 超出第 1 季官方正片集数，已保守移入 Season 00；请在审核时核对",
                "已唯一确认 OVA 是独立电影 Movie",
                "已唯一确认 OVA 是独立 TV 剧集",
            ],
            metadata={
                "tmdb_id": 1,
                "title": "Show",
                "year": "2024",
                "season": 1,
                "absolute": False,
            },
        )

        scraper.finalize_plan_evidence(plan)

        by_source = {
            notice.evidence.get("mapping_source"): notice
            for notice in plan.notices
        }
        self.assertIn("official_title_match", by_source)
        self.assertIn("timeline_runtime_match", by_source)
        self.assertIn("explicit_sp_number_fallback", by_source)
        self.assertIn("overflow_episode_fallback", by_source)
        self.assertIn("independent_movie", by_source)
        self.assertIn("independent_tv", by_source)
        self.assertTrue(by_source["explicit_sp_number_fallback"].requires_review)
        self.assertTrue(by_source["overflow_episode_fallback"].requires_review)

    def test_complete_unique_bare_season_boundary_is_proven_safe(self):
        warning = (
            "源根目录中的裸集号完整覆盖 TMDB 唯一官方季度；"
            "已按完整边界归入 Season 01"
        )
        plan = scraper.Plan(
            mode="tv",
            source_root="/src",
            target_root="/dst",
            files=[],
            warnings=[warning],
            metadata={"tmdb_id": 1, "title": "Show", "year": "2024"},
        )
        scraper.finalize_plan_evidence(plan)
        notice = next(item for item in plan.notices if item.message == warning)
        self.assertEqual(notice.code, "proven_safe_planning_action")
        self.assertFalse(notice.requires_review)

    def test_confirmed_official_season_boundary_has_structured_safe_notice(self):
        warning = (
            "源根目录中的裸集号完整覆盖已确认的 TMDB Season 02 边界；"
            "已按完整边界归入"
        )
        plan = scraper.Plan(
            mode="tv",
            source_root="/src",
            target_root="/dst",
            files=[
                scraper.PlannedFile(
                    source_path=f"/src/{number:02d}.mkv",
                    source_dir="/src",
                    original_name=f"{number:02d}.mkv",
                    final_name=f"Show - S02E{number:02d}.mkv",
                    target_dir="/dst/Season 02",
                    media_kind="video",
                    episode_key=f"E{number:02d}",
                )
                for number in range(1, 4)
            ],
            warnings=[warning],
            metadata={"tmdb_id": 42, "title": "Show", "year": "2024"},
        )
        scraper.finalize_plan_evidence(plan)
        notice = next(item for item in plan.notices if item.message == warning)
        self.assertEqual(notice.code, "complete_official_season_boundary")
        self.assertFalse(notice.requires_review)
        self.assertEqual(notice.evidence["season"], 2)
        self.assertEqual(notice.evidence["official_episode_count"], 3)
        self.assertEqual(notice.evidence["source_episode_numbers"], [1, 2, 3])

    def test_retained_unmapped_problem_has_structured_blocking_notice(self):
        warning = (
            "E12.5 已检索 TMDB 多语言标题，没有找到唯一候选；"
            "文件保留原位"
        )
        plan = scraper.Plan(
            mode="tv",
            source_root="/src",
            target_root="/dst",
            files=[],
            warnings=[warning],
            metadata={"tmdb_id": 42, "title": "Show", "year": "2024"},
            problem_files=[scraper.PlannedProblem(
                source_path="/src/Show.E12.5.mkv",
                target_path=None,
                reason=warning,
            )],
        )
        scraper.finalize_plan_evidence(plan)
        notice = next(item for item in plan.notices if item.message == warning)
        self.assertEqual(notice.code, "blocked_unclosed_problem_files")
        self.assertTrue(notice.requires_review)
        self.assertEqual(
            notice.evidence["source_paths"], ["/src/Show.E12.5.mkv"]
        )

    def test_named_special_files_next_to_bare_complete_season_map_to_season_zero(self):
        source = "/tv/Carnival"
        files = [
            {"name": "01.mkv", "full_path": f"{source}/01.mkv"},
            {"name": "02.mkv", "full_path": f"{source}/02.mkv"},
            {
                "name": "EX Season 01.mkv",
                "full_path": f"{source}/EX Season 01.mkv",
            },
            {
                "name": "EX Season 02.mkv",
                "full_path": f"{source}/EX Season 02.mkv",
            },
            {
                "name": "Fate Prototype.mkv",
                "full_path": f"{source}/Fate Prototype.mkv",
            },
            {
                "name": "Special Season.mkv",
                "full_path": f"{source}/Special Season.mkv",
            },
        ]
        tmdb = FakeTMDB(
            {
                "/tv/10": {
                    "name": "Carnival",
                    "original_name": "Carnival",
                    "first_air_date": "2011-01-01",
                    "poster_path": None,
                    "seasons": [
                        {"season_number": 0, "episode_count": 5},
                        {"season_number": 1, "episode_count": 2},
                    ],
                },
                "/tv/10/season/0": {
                    "episodes": [
                        {"episode_number": 1, "name": "Commentary"},
                        {"episode_number": 2, "name": "EX Season"},
                        {
                            "episode_number": 3,
                            "name": "EX Season Akiha's Trouble",
                        },
                        {"episode_number": 4, "name": "Fate Prototype"},
                        {"episode_number": 5, "name": "Special Season"},
                    ]
                },
                "/tv/10/season/1": {
                    "episodes": [
                        {"episode_number": 1, "name": "One"},
                        {"episode_number": 2, "name": "Two"},
                    ]
                },
            }
        )

        plan = scraper.build_tv_plan_smart(
            auto_episode_mode=True,
            alist=FakeAList(files),
            tmdb_client=tmdb,
            src_path=source,
            parent_path="/library",
            tmdb_id=10,
            season=1,
            absolute=False,
            prefer_simplified=True,
            allow_unmapped=False,
            ignore_orphan_temp=False,
            episode_map_path=None,
            episode_group_id=None,
            source_files=files,
        )

        targets = {item.original_name: item.final_name for item in plan.files}
        self.assertIn("S01E01", targets["01.mkv"])
        self.assertIn("S01E02", targets["02.mkv"])
        self.assertIn("S00E02", targets["EX Season 01.mkv"])
        self.assertIn("S00E03", targets["EX Season 02.mkv"])
        self.assertIn("S00E04", targets["Fate Prototype.mkv"])
        self.assertIn("S00E05", targets["Special Season.mkv"])

    def test_batch_plan_metadata_round_trips_and_generates_member_nfos(self):
        plan = scraper.Plan(
            mode="batch",
            source_root="/series",
            target_root="/series",
            files=[
                scraper.PlannedFile(
                    source_path="/series/show/01.mkv",
                    source_dir="/series/show",
                    original_name="01.mkv",
                    final_name="Show - S01E01 - Pilot.mkv",
                    target_dir="/series/Show (2020)/Season 01",
                    media_kind="video",
                    source_size=1,
                ),
                scraper.PlannedFile(
                    source_path="/series/movie/movie.mkv",
                    source_dir="/series/movie",
                    original_name="movie.mkv",
                    final_name="Movie (2021).mkv",
                    target_dir="/series/Movie (2021)",
                    media_kind="video",
                    source_size=1,
                ),
            ],
            warnings=[],
            metadata={
                "title": "Fate全系列",
                "member_tv": {
                    "/series/Show (2020)": {
                        "tmdb_id": 10,
                        "title": "Show",
                        "year": "2020",
                        "poster_path": "/show.jpg",
                        "backdrop_path": None,
                        "season_posters": {"1": "/season.jpg"},
                    }
                },
                "member_movies": {
                    "/series/Movie (2021)": {
                        "tmdb_id": 20,
                        "title": "Movie",
                        "year": "2021",
                    }
                },
                "member_posters": {"/series/Movie (2021)": "/movie.jpg"},
            },
        )
        restored = scraper.plan_from_dict(scraper.plan_to_dict(plan))
        nfo_paths = {path for path, _ in scraper.planned_nfos(restored)}
        self.assertIn("/series/Show (2020)/tvshow.nfo", nfo_paths)
        self.assertIn(
            "/series/Show (2020)/Season 01/Show - S01E01 - Pilot.nfo",
            nfo_paths,
        )
        self.assertIn("/series/Movie (2021)/Movie (2021).nfo", nfo_paths)
        episode_target, episode_payload = scraper.planned_tv_episode_nfos(restored)[0]
        self.assertEqual(
            episode_target,
            "/series/Show (2020)/Season 01/Show - S01E01 - Pilot.nfo",
        )
        episode_xml = episode_payload.decode("utf-8")
        self.assertIn("<episodedetails>", episode_xml)
        self.assertIn("<showtitle>Show</showtitle>", episode_xml)
        self.assertIn("<season>1</season>", episode_xml)
        self.assertIn("<episode>1</episode>", episode_xml)
        self.assertNotIn("<uniqueid", episode_xml)
        artwork = scraper.planned_artwork(restored)
        self.assertIn(("/series/folder.jpg", "/show.jpg", "batch-folder"), artwork)
        self.assertIn(("/series/poster.jpg", "/show.jpg", "batch-poster"), artwork)

    def test_complete_tv_gap_check_includes_missing_season_zero_episode(self):
        root = "/quark/影视/番剧/Example"
        plan = scraper.Plan(
            mode="tv",
            source_root="/quark/影视/待刮削/Example",
            target_root=root,
            files=[
                scraper.PlannedFile(
                    source_path="/quark/影视/待刮削/Example/SP1.mkv",
                    source_dir="/quark/影视/待刮削/Example",
                    original_name="SP1.mkv",
                    final_name="Example - S00E01 - OVA.mkv",
                    target_dir=f"{root}/Season 00",
                    media_kind="video",
                ),
                scraper.PlannedFile(
                    source_path="/quark/影视/待刮削/Example/01.mkv",
                    source_dir="/quark/影视/待刮削/Example",
                    original_name="01.mkv",
                    final_name="Example - S01E01 - Pilot.mkv",
                    target_dir=f"{root}/Season 01",
                    media_kind="video",
                ),
            ],
            warnings=[],
            metadata={"tmdb_id": 1, "title": "Example", "year": "2020"},
        )
        tmdb = FakeTMDB({
            "/tv/1": {
                "seasons": [
                    {"season_number": 0, "episode_count": 2},
                    {"season_number": 1, "episode_count": 1},
                ]
            },
            "/tv/1/season/0": {
                "episodes": [
                    {"episode_number": 1, "name": "OVA", "air_date": "2020-01-01"},
                    {"episode_number": 2, "name": "Second OVA", "air_date": "2020-02-01"},
                ]
            },
            "/tv/1/season/1": {
                "episodes": [
                    {"episode_number": 1, "name": "Pilot", "air_date": "2020-03-01"},
                ]
            },
        })
        plan.scan_report["resource_gaps"] = [
            scraper._resource_gap(
                "missing_episode",
                "S01E01 Pilot",
                "stale subplan observation",
            )
        ]
        scraper._append_complete_tv_resource_gaps(FakeAList([]), tmdb, plan)
        self.assertEqual(
            [gap["label"] for gap in plan.scan_report["resource_gaps"]],
            ["S00E02 Second OVA"],
        )

    def test_complete_tv_gap_does_not_duplicate_exact_independent_movie_special(self):
        root = "/quark/影视/番剧/约会大作战"
        movie_name = "约会大作战：万由里裁决 (2015).mkv"
        movie_target = f"{root}/{movie_name}"
        plan = scraper.Plan(
            mode="mixed",
            source_root="/quark/影视/待刮削/约会大作战",
            target_root=root,
            files=[scraper.PlannedFile(
                source_path="/quark/影视/待刮削/约会大作战/movie.mkv",
                source_dir="/quark/影视/待刮削/约会大作战",
                original_name="movie.mkv",
                final_name=movie_name,
                target_dir=root,
                media_kind="video",
            )],
            warnings=[],
            metadata={
                "tmdb_id": 46004,
                "title": "约会大作战",
                "year": "2013",
                "series_root": root,
                "member_movies": {
                    movie_target: {
                        "tmdb_id": 331061,
                        "title": "约会大作战：万由里裁决",
                        "year": "2015",
                    }
                },
            },
        )
        tmdb = FakeTMDB({
            "/tv/46004": {"seasons": [{"season_number": 0, "episode_count": 1}]},
            "/tv/46004/season/0": {
                "name": "特别篇",
                "episodes": [{
                    "episode_number": 5,
                    "name": "约会大作战：万由里裁决",
                    "air_date": "2015-08-22",
                }],
            },
        })

        scraper._append_complete_tv_resource_gaps(FakeAList([]), tmdb, plan)

        self.assertEqual(plan.scan_report.get("resource_gaps"), None)
        coverage = plan.scan_report["tv_specials_covered_by_member_movies"]
        self.assertEqual(coverage[0]["episode"], 5)
        self.assertEqual(coverage[0]["movie_tmdb_id"], 331061)
        self.assertEqual(coverage[0]["target"], movie_target)

    def test_complete_tv_gap_keeps_same_title_movie_when_release_year_differs(self):
        root = "/library/Example"
        movie_target = f"{root}/Example Movie (2014).mkv"
        plan = scraper.Plan(
            mode="mixed", source_root="/source", target_root=root,
            files=[scraper.PlannedFile(
                source_path="/source/movie.mkv", source_dir="/source",
                original_name="movie.mkv", final_name="Example Movie (2014).mkv",
                target_dir=root, media_kind="video",
            )],
            warnings=[],
            metadata={
                "tmdb_id": 1, "title": "Example", "year": "2013",
                "series_root": root,
                "member_movies": {movie_target: {
                    "tmdb_id": 2, "title": "Example Movie", "year": "2014",
                }},
            },
        )
        tmdb = FakeTMDB({
            "/tv/1": {"seasons": [{"season_number": 0, "episode_count": 1}]},
            "/tv/1/season/0": {"episodes": [{
                "episode_number": 1, "name": "Example Movie", "air_date": "2015-01-01",
            }]},
        })

        scraper._append_complete_tv_resource_gaps(FakeAList([]), tmdb, plan)

        self.assertEqual(
            [gap["label"] for gap in plan.scan_report["resource_gaps"]],
            ["S00E01 Example Movie"],
        )

    def test_finalize_keeps_one_preferred_subtitle_and_retains_alternatives_at_source(self):
        source = "/quark/影视/待刮削/Show"
        target = "/quark/影视/番剧/Show/Season 01"
        base = "Show - S01E01 - Pilot"
        plan = scraper.Plan(
            mode="tv",
            source_root=source,
            target_root="/quark/影视/番剧/Show",
            files=[
                scraper.PlannedFile(
                    source_path=f"{source}/01.mkv", source_dir=source,
                    original_name="01.mkv", final_name=f"{base}.mkv",
                    target_dir=target, media_kind="video",
                ),
                scraper.PlannedFile(
                    source_path=f"{source}/Kitauji/01.ass", source_dir=f"{source}/Kitauji",
                    original_name="01.ass", final_name=f"{base}.zh-CN.ass",
                    target_dir=target, media_kind="subtitle",
                ),
                scraper.PlannedFile(
                    source_path=f"{source}/Kitauji/子集化字幕/01.ass",
                    source_dir=f"{source}/Kitauji/子集化字幕",
                    original_name="01.ass", final_name=f"{base}.zh-CN.2.ass",
                    target_dir=target, media_kind="subtitle",
                ),
                scraper.PlannedFile(
                    source_path=f"{source}/Other/01.ass", source_dir=f"{source}/Other",
                    original_name="01.ass", final_name=f"{base}.zh-CN.3.ass",
                    target_dir=target, media_kind="subtitle",
                ),
            ],
            warnings=[],
            metadata={"tmdb_id": 1, "title": "Show", "year": "2020"},
        )
        scraper.finalize_plan_evidence(plan)
        self.assertEqual(
            [item.final_name for item in plan.files if item.media_kind == "subtitle"],
            [f"{base}.zh-CN.ass"],
        )
        self.assertEqual(plan.problem_files, [])
        deferred = {row["source_path"] for row in plan.scan_report["deferred_subtitles"]}
        self.assertEqual(len(deferred), 2)

    def test_franchise_discovery_retries_transiently_empty_child(self):
        root = "/source/Fate"
        movie = f"{root}/10 未来福音 extra chorus"

        class EventuallyVisibleAList:
            def __init__(self):
                self.walk_calls = 0

            def try_list(self, path, refresh=False):
                self.assert_refresh = refresh
                if path == root:
                    return [{"name": "10 未来福音 extra chorus", "is_dir": True}]
                return []

            def walk(self, path, **kwargs):
                self.walk_calls += 1
                if self.walk_calls == 1:
                    return []
                return [{"name": "extra chorus.mkv", "full_path": f"{movie}/extra chorus.mkv"}]

        alist = EventuallyVisibleAList()
        with mock.patch.object(scraper.time, "sleep") as sleep_mock:
            members, skipped = scraper._discover_franchise_member_roots(alist, root)
        self.assertEqual(members, [movie])
        self.assertEqual(skipped, [])
        sleep_mock.assert_called_once_with(0.2)

    def test_tv_plan_uses_separate_target_when_title_differs_only_by_case(self):
        source = "/quark/影视/待刮削/Overlord"
        files = [
            {
                "name": "Overlord.S01E01.mkv",
                "full_path": f"{source}/Overlord.S01E01.mkv",
            }
        ]
        tmdb = FakeTMDB(
            {
                "/tv/64196": {
                    "name": "OVERLORD",
                    "first_air_date": "2015-01-01",
                    "poster_path": None,
                    "seasons": [{"season_number": 1}],
                },
                "/tv/64196/season/1": {
                    "episodes": [{"episode_number": 1, "name": "终结与起始"}]
                },
                "/tv/64196/season/0": {"episodes": []},
            }
        )

        plan = scraper.build_tv_plan(
            FakeAList(files),
            tmdb,
            src_path=source,
            parent_path="/quark/影视/番剧",
            tmdb_id=64196,
            season=1,
            absolute=False,
            prefer_simplified=True,
            allow_unmapped=False,
        )

        target = "/quark/影视/番剧/OVERLORD"
        self.assertEqual(plan.target_root, target)
        self.assertEqual(plan.files[0].target_dir, f"{target}/Season 01")

    def test_movie_plan_uses_separate_target_when_title_differs_only_by_case(self):
        source = "/quark/影视/待刮削/Overlord (2015)"
        files = [{"name": "movie.mkv", "full_path": f"{source}/movie.mkv"}]
        plan = scraper.build_movie_plan(
            FakeAList(files),
            FakeTMDB(
                {
                    "/movie/1": {
                        "title": "OVERLORD",
                        "release_date": "2015-01-01",
                        "poster_path": None,
                    }
                }
            ),
            src_path=source,
            parent_path="/quark/影视/电影",
            tmdb_id=1,
        )

        target = "/quark/影视/电影/OVERLORD (2015)"
        self.assertEqual(plan.target_root, target)
        self.assertEqual(plan.files[0].target_dir, target)

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
        self.assertIn("电影一 (2020)", plan.files[0].target_dir)
        self.assertNotIn("{tmdb-", plan.files[0].target_dir)
        self.assertEqual(
            plan.metadata["member_posters"][plan.files[0].target_dir], "/movie1.jpg"
        )
        self.assertEqual(
            plan.metadata["member_movies"][plan.files[0].target_dir]["tmdb_id"], 101
        )

    def test_series_title_shared_by_specials_does_not_turn_complete_bare_run_into_specials(self):
        source = "/src/卫宫家今天的饭（2017）全13集 外挂简中字幕 1080P"
        files = [
            {"name": f"{number:02d}.mp4", "full_path": f"{source}/{number:02d}.mp4"}
            for number in range(1, 14)
        ]
        tmdb = FakeTMDB({
            "/tv/10": {
                "name": "卫宫家今天的饭",
                "original_name": "衛宮さんちの今日のごはん",
                "first_air_date": "2018-01-25",
                "poster_path": None,
                "backdrop_path": None,
                "seasons": [
                    {"season_number": 0, "episode_count": 13},
                    {"season_number": 1, "episode_count": 13},
                ],
            },
            "/tv/10/season/0": {"episodes": [
                {"episode_number": number, "name": f"3分钟就知道了！卫宫家今天的饭{number}"}
                for number in range(1, 14)
            ]},
            "/tv/10/season/1": {"episodes": [
                {"episode_number": number, "name": f"正片 {number}", "air_date": "2018-01-25"}
                for number in range(1, 14)
            ]},
        })

        plan = scraper.build_tv_plan_smart(
            auto_episode_mode=True,
            alist=FakeAList(files),
            tmdb_client=tmdb,
            src_path=source,
            parent_path="/library",
            tmdb_id=10,
            season=1,
            absolute=False,
            prefer_simplified=True,
            allow_unmapped=False,
            episode_map_path=None,
            episode_group_id=None,
            source_files=files,
        )

        self.assertEqual(len(plan.files), 13)
        self.assertTrue(all(item.target_dir.endswith("Season 01") for item in plan.files))
        self.assertTrue(any("S01E13" in item.final_name for item in plan.files))

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

    def test_named_kagai_jugyo_run_uses_official_specials_not_parent_season(self):
        source = "/src/暗杀教室"
        files = [
            {
                "name": f"Ansatsu Kyoushitsu S2 Kagai Jugyo Hen [{number:02d}].mkv",
                "full_path": (
                    f"{source}/暗杀教室第二季 课外授业篇/"
                    f"Ansatsu Kyoushitsu S2 Kagai Jugyo Hen [{number:02d}].mkv"
                ),
            }
            for number in range(1, 9)
        ] + [
            {
                "name": f"Ansatsu Kyoushitsu S2 Kagai Jugyo Hen [{number:02d}].ass",
                "full_path": (
                    f"{source}/备份字幕/"
                    f"Ansatsu Kyoushitsu S2 Kagai Jugyo Hen [{number:02d}].ass"
                ),
            }
            for number in range(1, 9)
        ] + [
            {
                "name": "Ansatsu Kyoushitsu S2 [01].mkv",
                "full_path": f"{source}/暗杀教室第二季/Ansatsu Kyoushitsu S2 [01].mkv",
            }
        ]
        responses = {
            "/tv/10": {
                "name": "暗杀教室",
                "original_name": "暗殺教室",
                "first_air_date": "2015-01-10",
                "poster_path": None,
                "seasons": [
                    {"season_number": 0, "episode_count": 9},
                    {"season_number": 1, "episode_count": 22},
                    {"season_number": 2, "episode_count": 25},
                ],
            },
            "/tv/10/season/0": {
                "episodes": [
                    {"episode_number": 1, "name": "Episode 0"},
                    *[
                        {
                            "episode_number": number + 1,
                            "name": f"OVA：课外授业篇 第 {number} 话",
                        }
                        for number in range(1, 9)
                    ],
                ]
            },
            "/tv/10/season/1": {
                "episodes": [
                    {"episode_number": number, "name": f"S1 {number}"}
                    for number in range(1, 23)
                ]
            },
            "/tv/10/season/2": {
                "episodes": [
                    {"episode_number": number, "name": f"S2 {number}"}
                    for number in range(1, 26)
                ]
            },
        }

        plan = scraper.build_tv_plan_smart(
            auto_episode_mode=True,
            alist=FakeAList(files),
            tmdb_client=FakeTMDB(responses),
            src_path=source,
            parent_path="/library",
            tmdb_id=10,
            season=1,
            absolute=False,
            prefer_simplified=True,
            allow_unmapped=False,
            ignore_orphan_temp=False,
            episode_map_path=None,
            episode_group_id=None,
            source_files=files,
        )

        by_name = {item.original_name: item.final_name for item in plan.files}
        for number in range(1, 9):
            name = f"Ansatsu Kyoushitsu S2 Kagai Jugyo Hen [{number:02d}].mkv"
            self.assertIn(f"S00E{number + 1:02d}", by_name[name])
            subtitle = f"Ansatsu Kyoushitsu S2 Kagai Jugyo Hen [{number:02d}].ass"
            self.assertIn(f"S00E{number + 1:02d}", by_name[subtitle])
        self.assertIn("S02E01", by_name["Ansatsu Kyoushitsu S2 [01].mkv"])

    def test_oav_without_official_special_mapping_stays_in_place_with_subtitle(self):
        files = [
            {"name": "Show [01].mkv", "full_path": "/src/Show [01].mkv"},
            {"name": "Show [13 OAV].mkv", "full_path": "/src/Show [13 OAV].mkv"},
            {"name": "Show [13 OAV].ass", "full_path": "/src/backup/Show [13 OAV].ass"},
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
        self.assertEqual([item.original_name for item in plan.files], ["Show [01].mkv"])
        self.assertEqual(
            {item.source_path for item in plan.problem_files},
            {"/src/Show [13 OAV].mkv"},
        )
        self.assertIn(
            "/src/backup/Show [13 OAV].ass",
            {row["source_path"] for row in plan.scan_report["deferred_subtitles"]},
        )
        self.assertTrue(all("Season 00" not in (item.target_path or "") for item in plan.problem_files))
        self.assertTrue(any("发行形态不能单独证明 Season 00" in item.reason for item in plan.problem_files))

    def test_split_sp_label_maps_explicit_number_without_aborting_tv_season(self):
        files = [
            {"name": "Show [01].mkv", "full_path": "/src/Show [01].mkv"},
            {"name": "[Group][Show][SP][01].mkv", "full_path": "/src/其它/SP/[Group][Show][SP][01].mkv"},
        ]
        plan = scraper.build_tv_plan_smart(
            auto_episode_mode=True,
            alist=FakeAList(files),
            tmdb_client=FakeTMDB(self.responses),
            src_path="/src",
            parent_path="/library",
            tmdb_id=10,
            season=1,
            absolute=False,
            prefer_simplified=True,
            allow_unmapped=False,
            ignore_orphan_temp=False,
            episode_map_path=None,
            episode_group_id=None,
            source_files=files,
        )
        self.assertTrue(any(item.original_name == "Show [01].mkv" for item in plan.files))
        self.assertTrue(
            any(
                "[SP][01]" in item.source_path and "S00E01" in item.final_name
                for item in plan.files
            )
        )
        self.assertFalse(any("[SP][01]" in item.source_path for item in plan.problem_files))

    def test_fractional_episode_maps_only_to_unique_official_special_title(self):
        files = [
            {
                "name": "Show [24.5].2160p.mkv",
                "full_path": "/src/4K/Show [24.5].2160p.mkv",
            },
            {
                "name": "Show [24.5].1080p.mkv",
                "full_path": "/src/1080P/Show [24.5].1080p.mkv",
            },
            {
                "name": "Show SP 24.5.1080p.mkv",
                "full_path": "/src/backup/Show SP 24.5.1080p.mkv",
            },
        ]
        responses = dict(self.responses)
        responses["/tv/10/season/0"] = {
            "episodes": [
                {"episode_number": 1, "name": "第24.5话 闲话"},
                {"episode_number": 2, "name": "普通特别篇"},
            ]
        }
        plan = scraper.build_tv_plan(
            FakeAList(files),
            FakeTMDB(responses),
            src_path="/src",
            parent_path="/library",
            tmdb_id=10,
            season=1,
            absolute=False,
            prefer_simplified=False,
            allow_unmapped=False,
        )
        self.assertEqual(len(plan.files), 1)
        self.assertIn("S00E01", plan.files[0].final_name)
        self.assertEqual(len(plan.cleanup_files), 2)
        self.assertTrue(any("多语言季度/特别篇标题" in item for item in plan.warnings))

    def test_fractional_episode_can_map_to_regular_season_when_evidence_says_so(self):
        files = [{"name": "Show [24.5].mkv", "full_path": "/src/Show [24.5].mkv"}]
        responses = dict(self.responses)
        responses["/tv/10/season/1"] = {
            "episodes": [
                {"episode_number": 7, "name": "第24.5话 正篇插话"}
            ]
        }
        responses["/tv/10/season/0"] = {
            "episodes": [{"episode_number": 1, "name": "无编号特别篇"}]
        }
        plan = scraper.build_tv_plan(
            FakeAList(files),
            FakeTMDB(responses),
            src_path="/src",
            parent_path="/library",
            tmdb_id=10,
            season=1,
            absolute=False,
            prefer_simplified=False,
            allow_unmapped=False,
        )
        self.assertEqual(len(plan.files), 1)
        self.assertIn("S01E07", plan.files[0].final_name)
        self.assertFalse(any("S00" in item.final_name for item in plan.files))

    def test_fractional_episode_uses_multilingual_tmdb_evidence(self):
        class MultilingualTMDB(FakeTMDB):
            language = "zh-CN"

            def get(self, path, **params):
                if path == "/tv/10/season/0" and params.get("language") == "ja-JP":
                    return {
                        "episodes": [
                            {"episode_number": 1, "name": "第24.5話 ヴェルドラ日記"}
                        ]
                    }
                return super().get(path, **params)

        files = [{"name": "Show [24.5].mkv", "full_path": "/src/Show [24.5].mkv"}]
        responses = dict(self.responses)
        responses["/tv/10/season/0"] = {
            "episodes": [{"episode_number": 1, "name": "Veldora's Journal"}]
        }
        plan = scraper.build_tv_plan(
            FakeAList(files),
            MultilingualTMDB(responses),
            src_path="/src",
            parent_path="/library",
            tmdb_id=10,
            season=1,
            absolute=False,
            prefer_simplified=False,
            allow_unmapped=False,
        )
        self.assertEqual(len(plan.files), 1)
        self.assertIn("S00E01", plan.files[0].final_name)

    def test_fractional_feature_length_special_uses_unique_broadcast_interval(self):
        class ChronologyTMDB(FakeTMDB):
            def get(self, path, **params):
                if path == "/search/movie":
                    if params.get("query") == "Path of Thorns":
                        return {"results": [{
                            "title": "Mushishi: Path of Thorns",
                            "original_title": "Path of Thorns",
                            "release_date": "2014-08-20",
                        }]}
                    if params.get("query") == "Bell Droplets":
                        return {"results": [{
                            "title": "Mushishi: Bell Droplets",
                            "original_title": "Bell Droplets",
                            "release_date": "2015-05-16",
                        }]}
                    return {"results": []}
                return super().get(path, **params)

        files = [{
            "name": "Mushishi Zoku Shou [10.5].mkv",
            "full_path": "/src/Mushishi Zoku Shou [10.5].mkv",
        }]
        responses = dict(self.responses)
        responses["/tv/10/season/1"] = {
            "episodes": [
                {
                    "episode_number": 10,
                    "name": "Winter's End",
                    "air_date": "2014-06-21",
                    "runtime": 24,
                },
                {
                    "episode_number": 11,
                    "name": "Cushion of Grass",
                    "air_date": "2014-10-19",
                    "runtime": 24,
                },
            ]
        }
        responses["/tv/10/season/0"] = {
            "episodes": [
                {
                    "episode_number": 5,
                    "name": "Path of Thorns",
                    "air_date": "2014-08-20",
                    "runtime": 47,
                },
                {
                    # TMDB's TV-special date is misleadingly inside the
                    # hiatus; the movie record has the real 2015 release.
                    "episode_number": 6,
                    "name": "Bell Droplets",
                    "air_date": "2014-10-02",
                    "runtime": 47,
                },
            ]
        }

        plan = scraper.build_tv_plan(
            FakeAList(files),
            ChronologyTMDB(responses),
            src_path="/src",
            parent_path="/library",
            tmdb_id=10,
            season=1,
            absolute=False,
            prefer_simplified=False,
            allow_unmapped=False,
        )

        self.assertEqual(len(plan.files), 1)
        self.assertIn("S00E05", plan.files[0].final_name)
        self.assertEqual(plan.problem_files, [])

    def test_fractional_in_special_pack_keeps_inferred_second_season_timeline(self):
        class ChronologyTMDB(FakeTMDB):
            language = "zh-CN"

            def get(self, path, **params):
                if path == "/search/movie":
                    query = params.get("query")
                    releases = {
                        "Path of Thorns": ("Path of Thorns", "2014-08-20"),
                        "Bell Droplets": ("Bell Droplets", "2015-05-16"),
                    }
                    if query in releases:
                        title, release_date = releases[query]
                        return {"results": [{
                            "title": title,
                            "original_title": title,
                            "release_date": release_date,
                        }]}
                    return {"results": []}
                return super().get(path, **params)

        responses = {
            "/tv/10": {
                "name": "Mushishi",
                "first_air_date": "2005-01-01",
                "seasons": [
                    {"season_number": 1, "episode_count": 1, "name": "虫师"},
                    {"season_number": 2, "episode_count": 20, "name": "虫师 续章"},
                ],
            },
            "/tv/10/season/1": {
                "episodes": [{
                    "episode_number": 1, "name": "The Green Seat",
                    "air_date": "2005-01-01", "runtime": 24,
                }]
            },
            "/tv/10/season/2": {"episodes": [
                {"episode_number": 10, "name": "Winter's End", "air_date": "2014-06-21", "runtime": 24},
                {"episode_number": 11, "name": "Cushion of Grass", "air_date": "2014-10-19", "runtime": 24},
            ]},
            "/tv/10/season/0": {"episodes": [
                {"episode_number": 5, "name": "Path of Thorns", "air_date": "2014-08-20", "runtime": 47},
                {"episode_number": 6, "name": "Bell Droplets", "air_date": "2014-10-02", "runtime": 47},
            ]},
        }
        source = (
            "/src/[DBD-Raws][虫师 续章][01-20TV全集+SP+特典映像]/"
            "Mushishi Zoku Shou [10.5].mkv"
        )
        plan = scraper.build_tv_plan_smart(
            auto_episode_mode=True,
            alist=FakeAList([{"name": source.rsplit("/", 1)[-1], "full_path": source}]),
            tmdb_client=ChronologyTMDB(responses),
            src_path="/src",
            parent_path="/library",
            tmdb_id=10,
            season=1,
            absolute=False,
            prefer_simplified=False,
            allow_unmapped=False,
        )

        self.assertEqual(len(plan.files), 1)
        self.assertIn("S00E05", plan.files[0].final_name)
        self.assertEqual(plan.problem_files, [])

    def test_fractional_feature_ass_title_joins_confirmed_movie_group(self):
        class ReadableAList(FakeAList):
            def read_file_bytes(self, path, *, max_bytes):
                self.last_read = (path, max_bytes)
                return (
                    "[Events]\n"
                    "Format: Layer, Start, End, Style, Name, MarginL, MarginR, MarginV, Effect, Text\n"
                    "Dialogue: 0,0:01:34.44,0:01:41.33,title,NTP,0,0,0,,{\\fad(2000,0)}铃之滴\n"
                ).encode()

        directory = "/src/[DBD-Raws][虫师 续章][01-20+SP]"
        video = {
            "name": "Mushishi Zoku Shou [20.5][1080P].mkv",
            "full_path": f"{directory}/Mushishi Zoku Shou [20.5][1080P].mkv",
            "size": 1_700_000_000,
        }
        sidecar = {
            "name": "Mushishi Zoku Shou [20.5][1080P].sc.ass.txt",
            "full_path": f"{directory}/Mushishi Zoku Shou [20.5][1080P].sc.ass.txt",
            "size": 32_000,
        }
        existing_movie = {
            "name": "Mushishi Zoku Shou Suzu no Shizuku [2160P].mkv",
            "full_path": "/src/movie/Suzu no Shizuku [2160P].mkv",
        }
        movie_groups = {312966: [existing_movie]}

        remaining, warnings = (
            scraper._attach_fractional_feature_by_ass_title_to_movie_groups(
                ReadableAList([video, sidecar, existing_movie]),
                FakeTMDB({
                    "/movie/312966": {
                        "title": "虫师 续章 铃之滴",
                        "original_title": "蟲師 続章 鈴の雫",
                    }
                }),
                movie_groups,
                [video],
                [video, sidecar, existing_movie],
            )
        )

        self.assertEqual(remaining, [])
        self.assertIn(video, movie_groups[312966])
        warning = next(item for item in warnings if "movie/312966" in item)
        plan = scraper.Plan(
            mode="batch", source_root="/src", target_root="/library/Series",
            files=[], warnings=[warning], metadata={},
        )
        scraper.finalize_plan_evidence(plan)
        notice = next(item for item in plan.notices if item.message == warning)
        self.assertFalse(notice.requires_review)

    def test_unverified_fractional_episode_stays_in_review_without_cleanup(self):
        files = [
            {"name": "Show.E01.mkv", "full_path": "/src/Show.E01.mkv"},
            {
                "name": "Show [24.7].2160p.mkv",
                "full_path": "/src/4K/Show [24.7].2160p.mkv",
            },
            {
                "name": "Show [24.7].1080p.mkv",
                "full_path": "/src/1080P/Show [24.7].1080p.mkv",
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
        self.assertEqual(len(plan.files), 1)
        self.assertIn("S01E01", plan.files[0].final_name)
        self.assertEqual(plan.cleanup_files, [])
        self.assertEqual(len(plan.problem_files), 2)
        self.assertTrue(
            all("没有找到" in item.reason for item in plan.problem_files)
        )

    def test_ambiguous_fractional_special_candidates_stay_in_review(self):
        files = [
            {"name": "Show.E01.mkv", "full_path": "/src/Show.E01.mkv"},
            {"name": "Show [24.5].mkv", "full_path": "/src/Show [24.5].mkv"},
        ]
        responses = dict(self.responses)
        responses["/tv/10/season/0"] = {
            "episodes": [
                {"episode_number": 1, "name": "第24.5话 版本 A"},
                {"episode_number": 2, "name": "第24.5话 版本 B"},
            ]
        }
        plan = scraper.build_tv_plan(
            FakeAList(files),
            FakeTMDB(responses),
            src_path="/src",
            parent_path="/library",
            tmdb_id=10,
            season=1,
            absolute=False,
            prefer_simplified=False,
            allow_unmapped=False,
        )
        self.assertEqual(len(plan.problem_files), 1)
        self.assertIn("多个候选", plan.problem_files[0].reason)
        self.assertIn("S00E01", plan.problem_files[0].reason)
        self.assertIn("S00E02", plan.problem_files[0].reason)

    def test_numbered_specials_without_tmdb_rows_are_not_given_fake_targets(self):
        files = [
            {"name": f"魔笛MAGI.SP{number:02d}.mkv", "full_path": f"/src/SPs/魔笛MAGI.SP{number:02d}.mkv"}
            for number in range(1, 6)
        ]
        responses = dict(self.responses)
        responses["/tv/10/season/0"] = {"episodes": []}
        with self.assertRaisesRegex(scraper.PlanError, "源编号不能证明"):
            scraper.build_tv_plan(
                FakeAList(files), FakeTMDB(responses), src_path="/src",
                parent_path="/library", tmdb_id=10, season=1, absolute=False,
                prefer_simplified=False, allow_unmapped=False,
            )

    def test_oad_numbering_alone_does_not_map_to_season_zero(self):
        # 魔笛MAGI OAD 案例：TMDB Season 00 恰好存在同编号行也不能仅凭
        # OADnn 编号映射；发行编号不是母作品 Season 00 身份证据。
        files = [
            {
                "name": f"魔笛MAGI OAD {number:02d}.mkv",
                "full_path": f"/src/OAD/魔笛MAGI OAD {number:02d}.mkv",
            }
            for number in range(1, 6)
        ]
        responses = dict(self.responses)
        responses["/tv/10/season/0"] = {
            "episodes": [
                {"episode_number": number, "name": f"特别篇 {number}"}
                for number in range(1, 6)
            ]
        }
        with self.assertRaisesRegex(scraper.PlanError, "不能机械改写为 S00E01"):
            scraper.build_tv_plan(
                FakeAList(files), FakeTMDB(responses), src_path="/src",
                parent_path="/library", tmdb_id=10, season=1, absolute=False,
                prefer_simplified=False, allow_unmapped=False,
                auto_special_title_match=True,
            )

    def test_oad_numbering_maps_when_official_season_zero_title_names_ordinal(self):
        # 官方 Season 00 标题本身声明物理发行编号（OAD/OVA 第 n 号）时，
        # 标题身份证据成立，OADnn 允许映射。
        files = [
            {
                "name": f"Show OAD {number:02d}.mkv",
                "full_path": f"/src/OAD/Show OAD {number:02d}.mkv",
            }
            for number in range(1, 4)
        ]
        responses = dict(self.responses)
        responses["/tv/10/season/0"] = {
            "episodes": [
                {"episode_number": 1, "name": "OAD 1 外传"},
                {"episode_number": 2, "name": "OAD#2 特典"},
                {"episode_number": 3, "name": "OVA 第 3 话"},
            ]
        }
        plan = scraper.build_tv_plan(
            FakeAList(files), FakeTMDB(responses), src_path="/src",
            parent_path="/library", tmdb_id=10, season=1, absolute=False,
            prefer_simplified=False, allow_unmapped=False,
            auto_special_title_match=True,
        )
        self.assertEqual(len(plan.files), 3)
        self.assertTrue(all("S00E0" in item.final_name for item in plan.files))
        self.assertFalse(plan.problem_files)

    def test_oad_numbering_maps_with_official_season_window_timeline(self):
        # 官方时间线证据：Season 00 该集官方播出日期落在本季开播窗口内
        #（作品关系证据），OADnn 允许映射。
        files = [
            {
                "name": "Show OAD 01.mkv",
                "full_path": "/src/OAD/Show OAD 01.mkv",
            }
        ]
        responses = dict(self.responses)
        responses["/tv/10"] = {
            "name": "测试剧",
            "first_air_date": "2020-01-01",
            "poster_path": "/poster.jpg",
            "seasons": [
                {"season_number": 1, "episode_count": 3},
                {"season_number": 2, "episode_count": 3},
            ],
        }
        responses["/tv/10/season/1"] = {
            "episodes": [
                {
                    "episode_number": number,
                    "name": f"第一集 {number}",
                    "air_date": f"2020-01-{number:02d}",
                }
                for number in range(1, 4)
            ]
        }
        responses["/tv/10/season/0"] = {
            "episodes": [
                {
                    "episode_number": 1,
                    "name": "特别篇",
                    "air_date": "2020-01-15",
                }
            ]
        }
        plan = scraper.build_tv_plan(
            FakeAList(files), FakeTMDB(responses), src_path="/src",
            parent_path="/library", tmdb_id=10, season=1, absolute=False,
            prefer_simplified=False, allow_unmapped=False,
        )
        self.assertEqual(len(plan.files), 1)
        self.assertIn("S00E01", plan.files[0].final_name)
        self.assertFalse(plan.problem_files)

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

    def test_small_regular_overflow_without_official_match_stays_unplanned(self):
        files = [{"name": "High School DxD [14].mkv", "full_path": "/src/High School DxD [14].mkv"}]
        responses = dict(self.responses)
        responses["/tv/10/season/1"] = {
            "episodes": [{"episode_number": number, "name": f"Episode {number}"} for number in range(1, 13)]
        }
        responses["/tv/10/season/0"] = {"episodes": []}
        with self.assertRaisesRegex(scraper.PlanError, "不能把源编号当作 Season 00"):
            scraper.build_tv_plan(
                FakeAList(files), FakeTMDB(responses), src_path="/src",
                parent_path="/library", tmdb_id=10, season=1, absolute=False,
                prefer_simplified=False, allow_unmapped=False,
            )

    def test_retained_unparsed_media_is_listed_as_problem_file(self):
        files = [
            {"name": "Show.E01.mkv", "full_path": "/src/Show.E01.mkv"},
            {"name": "unmatched bonus.mkv", "full_path": "/src/unmatched bonus.mkv"},
            {"name": "unmatched notes.ass", "full_path": "/src/unmatched notes.ass"},
        ]
        plan = scraper.build_tv_plan(
            FakeAList(files), FakeTMDB(self.responses), src_path="/src",
            parent_path="/library", tmdb_id=10, season=1, absolute=False,
            prefer_simplified=False, allow_unmapped=False,
            auto_special_title_match=True,
        )
        by_source = {item.source_path: item.reason for item in plan.problem_files}
        self.assertIn("/src/unmatched bonus.mkv", by_source)
        self.assertIn(
            "/src/unmatched notes.ass",
            {row["source_path"] for row in plan.scan_report["deferred_subtitles"]},
        )
        self.assertIn(
            "保留原位待人工确认",
            by_source["/src/unmatched bonus.mkv"],
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

    def test_source_nested_inside_target_is_rejected(self):
        files = [{"name": "old.mkv", "full_path": "/library/source/old.mkv"}]
        plan = scraper.Plan(
            mode="movie",
            source_root="/library/source",
            target_root="/library",
            files=[
                scraper.PlannedFile(
                    source_path="/library/source/old.mkv",
                    source_dir="/library/source",
                    original_name="old.mkv",
                    final_name="new.mkv",
                    target_dir="/library",
                    media_kind="video",
                    source_size=1,
                    source_modified="2026-01-01T00:00:00Z",
                )
            ],
            warnings=[],
            metadata={},
        )
        with self.assertRaisesRegex(scraper.PlanError, "发生重叠"):
            scraper.validate_plan(FakeAList(files), plan)

    def test_existing_destination_collision_stops(self):
        files = [{"name": "old.mkv", "full_path": "/src/old.mkv"}]
        target = "/library/电影 (2020)"
        alist = FakeAList(files, listings={target: [{"name": "电影 (2020).mkv", "is_dir": False}]})
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

    def test_destination_same_episode_different_video_extension_stops(self):
        files = [{"name": "old.mp4", "full_path": "/src/old.mp4"}]
        plan = scraper.Plan(
            mode="tv",
            source_root="/src",
            target_root="/dst/Show",
            files=[
                scraper.PlannedFile(
                    source_path="/src/old.mp4",
                    source_dir="/src",
                    original_name="old.mp4",
                    final_name="Show - S01E01 - One.mp4",
                    target_dir="/dst/Show/Season 01",
                    media_kind="video",
                    source_size=100,
                    source_modified="2026-01-01T00:00:00Z",
                )
            ],
            warnings=[],
            metadata={"tmdb_id": 10, "title": "Show"},
        )
        alist = FakeAList(
            files,
            listings={
                "/dst/Show/Season 01": [
                    {
                        "name": "Show - S01E01 - One.mkv",
                        "is_dir": False,
                        "size": 1000,
                    }
                ]
            },
        )

        with self.assertRaisesRegex(
            scraper.PlanError,
            "同集不同扩展名视频",
        ):
            scraper.validate_plan(alist, plan)

    def test_destination_different_episode_video_extension_is_allowed(self):
        files = [{"name": "old.mp4", "full_path": "/src/old.mp4"}]
        plan = scraper.Plan(
            mode="tv",
            source_root="/src",
            target_root="/dst/Show",
            files=[
                scraper.PlannedFile(
                    source_path="/src/old.mp4",
                    source_dir="/src",
                    original_name="old.mp4",
                    final_name="Show - S01E02 - Two.mp4",
                    target_dir="/dst/Show/Season 01",
                    media_kind="video",
                    source_size=100,
                    source_modified="2026-01-01T00:00:00Z",
                )
            ],
            warnings=[],
            metadata={"tmdb_id": 10, "title": "Show"},
        )
        alist = FakeAList(
            files,
            listings={
                "/dst/Show/Season 01": [
                    {
                        "name": "Show - S01E01 - One.mkv",
                        "is_dir": False,
                        "size": 1000,
                    }
                ]
            },
        )

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
        self.assertEqual(
            loaded[scraper.EpisodeKey("fractional", 11, fractional_digits="5")],
            (0, 2, 0),
        )
        self.assertEqual(
            loaded[scraper.EpisodeKey("fractional", 18, fractional_digits="5")],
            (0, 4, 0),
        )

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
    def test_mkdir_retries_transient_quark_illegal_text_with_reconciliation(self):
        class ThrottledClient(scraper.AListClient):
            def __init__(self):
                super().__init__("https://example.invalid", "admin", "")
                self.mkdir_calls = 0

            def call(self, endpoint, body, retryable=False):
                del body, retryable
                self.assert_endpoint = endpoint
                self.mkdir_calls += 1
                if self.mkdir_calls < 3:
                    raise scraper.ApiError("failed to make parent dir: illegal text")
                return {"code": 200}

            def try_list(self, path, refresh=False):
                del path, refresh
                return None

        client = ThrottledClient()
        with mock.patch.object(scraper.time, "sleep") as sleeper:
            client.mkdir("/library/valid-title")
        self.assertEqual(client.mkdir_calls, 3)
        self.assertEqual(sleeper.call_count, 2)

    def test_tmdb_get_uses_ttl_cache_and_returns_isolated_values(self):
        client = scraper.TMDBClient("test-key", cache_ttl=60)
        client.http = mock.Mock()
        client.http.request_json.return_value = {"results": [{"id": 1}]}
        first = client.get("/search/tv", query="Show", page=1)
        first["results"][0]["id"] = 999
        second = client.get("/search/tv", page=1, query="Show")
        self.assertEqual(second["results"][0]["id"], 1)
        self.assertEqual(client.http.request_json.call_count, 1)
        self.assertEqual(client.cache_report()["cache_hits"], 1)

    def test_authenticated_call_relogs_once_after_alist_restart(self):
        class RestartingHttp:
            def __init__(self):
                self.fs_calls = 0
                self.login_calls = 0

            def request_json(self, url, **kwargs):
                if url.endswith("/api/auth/login"):
                    self.login_calls += 1
                    self.assert_password(kwargs)
                    return {"code": 200, "data": {"token": "fresh-token"}}
                self.fs_calls += 1
                if self.fs_calls == 1:
                    return {"code": 401, "message": "token is invalidated"}
                self.last_headers = kwargs.get("headers")
                return {"code": 200, "data": {"content": []}}

            @staticmethod
            def assert_password(kwargs):
                if kwargs.get("json_body", {}).get("password") != "secret":
                    raise AssertionError("AList password was not retained for re-login")

        client = scraper.AListClient("https://example.invalid", "admin", "secret")
        http = RestartingHttp()
        client.http = http
        client.token = "stale-token"

        result = client.call("list", {"path": "/src"}, retryable=True)

        self.assertEqual(result["code"], 200)
        self.assertEqual(http.login_calls, 1)
        self.assertEqual(http.fs_calls, 2)
        self.assertEqual(http.last_headers, {"Authorization": "fresh-token"})
        self.assertEqual(client.password, "")

    def test_walk_keeps_explicit_junk_for_the_reviewed_cleanup_plan(self):
        class CleanupClient(scraper.AListClient):
            def __init__(self):
                super().__init__("https://example.invalid", "admin", "")

            def list(self, path, refresh=False):
                return [
                    {"name": "Show [NCOP1].mkv", "is_dir": False},
                    {"name": "Show [Menu02].mkv", "is_dir": False},
                    {"name": "fonts.zip", "is_dir": False},
                    {"name": "Show.S01E01.mkv", "is_dir": False},
                ]

        names = {item["name"] for item in CleanupClient().walk("/src")}
        self.assertEqual(
            names,
            {
                "Show [NCOP1].mkv",
                "Show [Menu02].mkv",
                "fonts.zip",
                "Show.S01E01.mkv",
            },
        )

    def test_walk_can_include_a_legitimate_title_directory_containing_extra(self):
        class ExtraTitleClient(scraper.AListClient):
            def __init__(self):
                super().__init__("https://example.invalid", "admin", "")

            def list(self, path, refresh=False):
                if path == "/library":
                    return [{"name": "Future Gospel extra chorus (2013)", "is_dir": True}]
                return [{"name": "Future Gospel extra chorus (2013).mkv", "is_dir": False}]

        client = ExtraTitleClient()
        self.assertEqual(client.walk("/library"), [])
        included = client.walk("/library", include_title_extras=True)
        self.assertEqual(
            [row["full_path"] for row in included],
            [
                "/library/Future Gospel extra chorus (2013)/"
                "Future Gospel extra chorus (2013).mkv"
            ],
        )

    def test_walk_prunes_excluded_root_before_visiting_its_children(self):
        class PruningClient(scraper.AListClient):
            def __init__(self):
                super().__init__("https://example.invalid", "admin", "")
                self.listed = []

            def list(self, path, refresh=False):
                self.listed.append(path)
                return {
                    "/quark/影视": [
                        {"name": "番剧", "is_dir": True},
                        {"name": "ScrapeFlow", "is_dir": True},
                    ],
                    "/quark/影视/番剧": [
                        {"name": "Visible.mkv", "is_dir": False},
                    ],
                    "/quark/影视/ScrapeFlow": [
                        {"name": "未来系统目录", "is_dir": True},
                    ],
                }.get(path, [])

        client = PruningClient()
        rows = client.walk(
            "/quark/影视",
            excluded_roots=["/quark/影视/ScrapeFlow"],
        )

        self.assertEqual(
            [row["full_path"] for row in rows],
            ["/quark/影视/番剧/Visible.mkv"],
        )
        self.assertNotIn("/quark/影视/ScrapeFlow", client.listed)

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
    def test_target_directory_creation_waits_until_parent_is_visible(self):
        class DelayedDirectoryAList(FakeAList):
            def __init__(self):
                super().__init__()
                self.hidden_reads = 0

            def mkdir(self, path):
                super().mkdir(path)
                if path == "/dst/Show":
                    self.hidden_reads = 2

            def try_list(self, path, refresh=False):
                if path == "/dst/Show" and self.hidden_reads:
                    self.hidden_reads -= 1
                    return None
                return super().try_list(path, refresh=refresh)

        plan = scraper.Plan(
            mode="tv", source_root="/src", target_root="/dst/Show",
            files=[scraper.PlannedFile(
                source_path="/src/old.mkv", source_dir="/src", original_name="old.mkv",
                final_name="Show - S01E01.mkv", target_dir="/dst/Show/Season 01",
                media_kind="video", source_size=1,
            )],
            warnings=[], metadata={
                "tmdb_id": 1, "title": "Movie", "year": "2026",
                "poster_path": None,
            },
        )
        journal = scraper.ExecutionJournal(
            created_at="2026-01-01T00:00:00+00:00",
            plan=scraper.plan_to_dict(plan),
            records=[],
        )
        with tempfile.TemporaryDirectory() as tmp, mock.patch.object(scraper.time, "sleep"):
            journal_path = Path(tmp) / "journal.json"
            scraper._reserve_output_path(journal_path)
            journal.save(journal_path)
            created = []
            scraper._ensure_target_dirs(
                DelayedDirectoryAList(), plan, journal, journal_path, created,
            )
        self.assertEqual(created, ["/dst/Show", "/dst/Show/Season 01"])
        self.assertTrue(all(record.status == "ok" for record in journal.records))

    def test_existing_target_lock_blocks_concurrent_series_write(self):
        source = "/src/old.mkv"
        lock_name = f"{scraper.LOCK_PREFIX}other-task.json"
        alist = FakeAList(
            [{"name": "old.mkv", "full_path": source}],
            listings={"/dst": [{"name": lock_name, "is_dir": False}]},
        )
        plan = scraper.Plan(
            mode="movie", source_root="/src", target_root="/dst",
            files=[scraper.PlannedFile(
                source_path=source, source_dir="/src", original_name="old.mkv",
                final_name="new.mkv", target_dir="/dst", media_kind="video",
                source_size=1, source_modified="2026-01-01T00:00:00Z",
            )],
            warnings=[], metadata={"poster_path": None},
        )
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaisesRegex(scraper.ScraperError, "target目录已有整理锁"):
                scraper.execute_plan(
                    alist, None, plan, journal_path=Path(tmp) / "journal.json",
                    skip_poster=True,
                )
        self.assertIn("old.mkv", alist.names_by_dir["/src"])
        self.assertNotIn("new.mkv", alist.names_by_dir["/src"])

    def test_distinct_target_scope_locks_can_share_an_existing_parent(self):
        source = "/src/old.mkv"
        other_scope = scraper._remote_lock_scope_key("/dst", "/dst/Other Show")
        other_lock = f"{scraper.LOCK_PREFIX}v2-{other_scope}-other-task.json"
        alist = FakeAList([{"name": "old.mkv", "full_path": source}])
        alist.names_by_dir["/dst"] = {other_lock}
        alist.entries_by_dir["/dst"] = {
            other_lock: {"name": other_lock, "is_dir": False, "size": 1},
        }
        plan = scraper.Plan(
            mode="movie", source_root="/src", target_root="/dst/This Show",
            files=[scraper.PlannedFile(
                source_path=source, source_dir="/src", original_name="old.mkv",
                final_name="new.mkv", target_dir="/dst/This Show", media_kind="video",
                source_size=1, source_modified="2026-01-01T00:00:00Z",
            )],
            warnings=[], metadata={"poster_path": None},
        )
        with tempfile.TemporaryDirectory() as tmp:
            scraper.execute_plan(
                alist, None, plan, journal_path=Path(tmp) / "journal.json",
                skip_poster=True,
            )
        self.assertIn(other_lock, alist.names_by_dir["/dst"])
        self.assertIn("new.mkv", alist.names_by_dir["/dst/This Show"])

    def test_lock_acquisition_waits_for_eventually_consistent_listing(self):
        class DelayedLockAList(FakeAList):
            def __init__(self):
                super().__init__([{"name": "old.mkv", "full_path": "/src/old.mkv"}])
                self.names_by_dir["/dst"] = set()
                self.entries_by_dir["/dst"] = {}
                self.hidden_target_reads = 0

            def upload_bytes(self, target_path, data, content_type):
                super().upload_bytes(target_path, data, content_type)
                if scraper.split_remote(target_path)[0] == "/dst":
                    self.hidden_target_reads = 2

            def try_list(self, path, refresh=False):
                rows = super().try_list(path, refresh=refresh)
                if path == "/dst" and self.hidden_target_reads:
                    self.hidden_target_reads -= 1
                    return [
                        row for row in (rows or [])
                        if not scraper.is_scraper_lock(str(row.get("name") or ""))
                    ]
                return rows

        alist = DelayedLockAList()
        plan = scraper.Plan(
            mode="movie", source_root="/src", target_root="/dst",
            files=[scraper.PlannedFile(
                source_path="/src/old.mkv", source_dir="/src", original_name="old.mkv",
                final_name="new.mkv", target_dir="/dst", media_kind="video",
                source_size=1, source_modified="2026-01-01T00:00:00Z",
            )],
            warnings=[], metadata={"poster_path": None},
        )
        with tempfile.TemporaryDirectory() as tmp, mock.patch.object(scraper.time, "sleep"):
            scraper.execute_plan(
                alist, None, plan, journal_path=Path(tmp) / "journal.json",
                skip_poster=True,
            )
        self.assertIn("new.mkv", alist.names_by_dir["/dst"])
        self.assertFalse(any(scraper.is_scraper_lock(name) for name in alist.names_by_dir["/dst"]))

    def test_target_commit_never_renames_source_pending_delete(self):
        source = "/src/old.mkv"
        alist = FakeAList([{"name": "old.mkv", "full_path": source}])
        plan = scraper.Plan(
            mode="movie", source_root="/src", target_root="/dst",
            files=[scraper.PlannedFile(
                source_path=source, source_dir="/src", original_name="old.mkv",
                final_name="new.mkv", target_dir="/dst", media_kind="video",
                source_size=1, source_modified="2026-01-01T00:00:00Z",
            )],
            warnings=[], metadata={"poster_path": None},
        )
        with tempfile.TemporaryDirectory() as tmp:
            journal_path = Path(tmp) / "journal.json"
            scraper.execute_plan(
                alist, None, plan, journal_path=journal_path,
                skip_poster=True, cleanup_empty_source=True,
            )
            records = json.loads(journal_path.read_text(encoding="utf-8"))["records"]
        actions = [record["action"] for record in records]
        self.assertIn("files-committed", actions)
        self.assertNotIn("mark-source-pending-delete", actions)

    def test_safe_transfer_stages_each_file_without_alist_move(self):
        names = ["one.mkv", "two.mkv", "three.mkv"]
        alist = FakeAList(
            [{"name": name, "full_path": f"/src/{name}"} for name in names]
        )

        with tempfile.TemporaryDirectory() as tmp:
            moved = scraper._move_with_reconciliation(
                alist,
                "/src",
                "/dst",
                names,
                stage_root=Path(tmp),
                transaction_scope="test-forward",
                expected_sizes={name: 1 for name in names},
            )

        self.assertCountEqual(moved, names)
        self.assertEqual(alist.names_by_dir["/src"], set())
        self.assertEqual(alist.names_by_dir["/dst"], set(names))
        self.assertFalse(any(operation[0] == "move" for operation in alist.operations))
        self.assertEqual(
            len([operation for operation in alist.operations if operation[0] == "safe-upload"]),
            3,
        )

    def test_safe_transfer_reconciles_lost_upload_response_without_retry(self):
        class LostResponseAList(FakeAList):
            def __init__(self):
                super().__init__([{"name": "episode.mkv", "full_path": "/src/episode.mkv"}])
                self.attempts = 0

            def upload_file(self, target_path, source, content_type="application/octet-stream"):
                self.attempts += 1
                super().upload_file(target_path, source, content_type)
                raise scraper.ApiError("HTTP 500 after provider commit")

        alist = LostResponseAList()
        with tempfile.TemporaryDirectory() as tmp:
            moved = scraper._move_with_reconciliation(
                alist,
                "/src",
                "/dst",
                ["episode.mkv"],
                stage_root=Path(tmp),
                transaction_scope="test-response-lost",
                expected_sizes={"episode.mkv": 1},
            )

        self.assertEqual(moved, ["episode.mkv"])
        self.assertEqual(alist.attempts, 1)
        self.assertEqual(alist.names_by_dir["/src"], set())
        self.assertEqual(alist.names_by_dir["/dst"], {"episode.mkv"})

    def test_execute_stages_original_bytes_before_first_user_file_mutation(self):
        class StageCheckingAList(FakeAList):
            stage_root: Path

            def upload_file(self, target_path, source, content_type="application/octet-stream"):
                self.assert_stage_exists(target_path)
                super().upload_file(target_path, source, content_type)

            def assert_stage_exists(self, target_path):
                payloads = list(self.stage_root.rglob("payload.bin"))
                if target_path.startswith("/dst/") and (
                    not payloads or not any(path.read_bytes() == b"x" for path in payloads)
                ):
                    raise AssertionError("remote upload began before durable local staging")

        alist = StageCheckingAList([{
            "name": "old.mkv", "full_path": "/src/old.mkv", "size": 1,
            "modified": "2026-01-01T00:00:00Z",
        }])
        plan = scraper.Plan(
            mode="movie", source_root="/src", target_root="/dst",
            files=[scraper.PlannedFile(
                source_path="/src/old.mkv", source_dir="/src",
                original_name="old.mkv", final_name="new.mkv", target_dir="/dst",
                media_kind="video", source_size=1,
                source_modified="2026-01-01T00:00:00Z",
            )],
            warnings=[], metadata={"poster_path": None},
        )
        with tempfile.TemporaryDirectory() as tmp:
            alist.stage_root = Path(tmp) / ".hybrid-remote-transactions"
            scraper.execute_plan(
                alist, None, plan, journal_path=Path(tmp) / "journal.json",
                skip_poster=True,
            )
        self.assertIn("new.mkv", alist.names_by_dir["/dst"])
        self.assertFalse(any(operation[0] in {"move", "rename"} for operation in alist.operations))

    def test_confirmed_cleanup_removes_junk_and_releases_lock(self):
        files = [
            {
                "name": "old.mkv",
                "full_path": "/src/old.mkv",
                "size": 100,
                "modified": "2026-01-01T00:00:00Z",
            },
            {
                "name": "Show [NCOP1].mkv",
                "full_path": "/src/Show [NCOP1].mkv",
                "size": 20,
                "modified": "2026-01-01T00:00:00Z",
            },
            {
                "name": "防失联永久链接.jpg",
                "full_path": "/src/防失联永久链接.jpg",
                "size": 5,
                "modified": "2026-01-01T00:00:00Z",
            },
            {
                "name": "._old.mkv",
                "full_path": "/src/._old.mkv",
                "size": 4,
                "modified": "2026-01-01T00:00:00Z",
            },
        ]
        alist = FakeAList(files)
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
                    source_size=100,
                    source_modified="2026-01-01T00:00:00Z",
                )
            ],
            warnings=[],
            metadata={"poster_path": None},
            cleanup_files=scraper._planned_cleanup_files(files),
        )

        with tempfile.TemporaryDirectory() as tmp:
            scraper.execute_plan(
                alist,
                None,
                plan,
                journal_path=Path(tmp) / "journal.json",
                skip_poster=True,
            )

        self.assertNotIn("Show [NCOP1].mkv", alist.names_by_dir["/src"])
        self.assertNotIn("防失联永久链接.jpg", alist.names_by_dir["/src"])
        self.assertNotIn("._old.mkv", alist.names_by_dir["/src"])
        self.assertFalse(
            any(
                name.startswith(scraper.LOCK_PREFIX)
                for names in alist.names_by_dir.values()
                for name in names
            )
        )
        lock_uploads = [
            operation[1]
            for operation in alist.operations
            if operation[0] == "upload"
            and scraper.split_remote(operation[1])[1].startswith(scraper.LOCK_PREFIX)
        ]
        self.assertEqual(len(lock_uploads), 2)
        self.assertEqual(
            [scraper.split_remote(path)[0] for path in lock_uploads],
            ["/", "/src"],
        )
        removed_names = {
            name
            for operation in alist.operations
            if operation[0] == "remove"
            for name in operation[2]
        }
        self.assertIn("Show [NCOP1].mkv", removed_names)
        self.assertIn("防失联永久链接.jpg", removed_names)
        self.assertIn("._old.mkv", removed_names)

    def test_planned_cleanup_retains_remote_payload_until_title_acceptance(self):
        source = "/src/Show [NCOP1].mkv"
        alist = FakeAList([{
            "name": "Show [NCOP1].mkv",
            "full_path": source,
            "size": 20,
            "modified": "2026-01-01T00:00:00Z",
        }])
        plan = scraper.Plan(
            mode="movie", source_root="/src", target_root="/dst", files=[],
            cleanup_files=scraper._planned_cleanup_files(alist.files),
            warnings=[], metadata={"poster_path": None},
        )
        journal = scraper.ExecutionJournal(
            "2026-01-01T00:00:00Z", scraper.plan_to_dict(plan), [],
        )
        with tempfile.TemporaryDirectory() as tmp:
            journal_path = Path(tmp) / "journal.json"
            journal.save(journal_path)
            state_root = Path(tmp) / ".hybrid-remote-transactions"
            specs, by_source = scraper._hybrid_batch_specs(
                alist, plan, transaction_scope="cleanup-test",
            )
            self.assertEqual(len(specs), 1)
            self.assertEqual(specs[0].operation, "delete")
            scraper.prepare_hybrid_batch(
                scraper._HybridAListAdapter(alist),
                state_root=state_root,
                specs=specs,
            )
            scraper._cleanup_planned_files(
                alist,
                plan,
                journal,
                journal_path,
                hybrid_state_root=state_root,
                hybrid_by_source=by_source,
            )

            rollback = specs[0].rollback_path
            rollback_dir, rollback_name = scraper.split_remote(rollback)
            payload = alist.entries_by_dir[rollback_dir][rollback_name]["_content"]
            self.assertEqual(payload, b"x" * 20)
            transaction = json.loads(
                (state_root / specs[0].batch_id / specs[0].item_id / "journal.json")
                .read_text(encoding="utf-8")
            )
            self.assertEqual(transaction["state"], "complete")
        self.assertNotIn("Show [NCOP1].mkv", alist.names_by_dir["/src"])

    def test_same_directory_hybrid_spec_binds_original_directly_to_final_name(self):
        alist = FakeAList([{
            "name": "old.mkv", "full_path": "/src/old.mkv", "size": 3,
        }])
        plan = scraper.Plan(
            mode="movie", source_root="/intake", target_root="/library",
            files=[scraper.PlannedFile(
                source_path="/src/old.mkv", source_dir="/src",
                original_name="old.mkv", final_name="new.mkv",
                target_dir="/src", media_kind="video", source_size=3,
            )], warnings=[], metadata={},
        )
        specs, _ = scraper._hybrid_batch_specs(
            alist, plan, transaction_scope="same-dir-direct",
        )
        self.assertEqual(len(specs), 1)
        self.assertEqual(specs[0].source_path, "/src/old.mkv")
        self.assertEqual(specs[0].target_path, "/src/new.mkv")
        self.assertNotIn(".scraper-tmp-", specs[0].target_path)

    def test_same_directory_planned_name_cycle_is_rejected_before_prepare(self):
        alist = FakeAList([
            {"name": "a.mkv", "full_path": "/src/a.mkv", "size": 1},
            {"name": "b.mkv", "full_path": "/src/b.mkv", "size": 1},
        ])
        plan = scraper.Plan(
            mode="movie", source_root="/intake", target_root="/library",
            files=[
                scraper.PlannedFile(
                    "/src/a.mkv", "/src", "a.mkv", "b.mkv", "/src", "video",
                    source_size=1,
                ),
                scraper.PlannedFile(
                    "/src/b.mkv", "/src", "b.mkv", "a.mkv", "/src", "video",
                    source_size=1,
                ),
            ], warnings=[], metadata={},
        )
        with self.assertRaisesRegex(scraper.PlanError, "角色重叠路径"):
            scraper._hybrid_batch_specs(
                alist, plan, transaction_scope="same-dir-cycle",
            )

    def test_release_lock_waits_for_eventually_consistent_listing(self):
        lock_name = f"{scraper.LOCK_PREFIX}v2-all-0123456789abcdef-fixture.json"

        class EventuallyConsistentAList(FakeAList):
            def __init__(self):
                super().__init__([{
                    "name": lock_name, "full_path": f"/dst/{lock_name}",
                }])
                self.stale_reads = 0

            def remove(self, parent, names):
                first_removal = any(
                    name in self.names_by_dir.get(parent, set()) for name in names
                )
                super().remove(parent, names)
                if first_removal:
                    self.stale_reads = 2

            def try_list(self, path, refresh=False):
                rows = super().try_list(path, refresh=refresh) or []
                if path == "/dst" and self.stale_reads:
                    self.stale_reads -= 1
                    return [*rows, {
                        "name": lock_name, "is_dir": False, "size": 1,
                        "modified": "2026-01-01T00:00:00Z",
                    }]
                return rows

        alist = EventuallyConsistentAList()
        journal = scraper.ExecutionJournal("2026-01-01T00:00:00Z", {}, [])
        with tempfile.TemporaryDirectory() as tmp, mock.patch.object(
            scraper.time, "sleep",
        ) as sleep:
            path = Path(tmp) / "journal.json"
            scraper._release_remote_lock(
                alist, f"/dst/{lock_name}", journal, path,
            )
        removals = [op for op in alist.operations if op[0] == "remove"]
        self.assertGreaterEqual(len(removals), 2)
        sleep.assert_called()
        self.assertEqual(journal.records[-1].status, "ok")

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

    def test_source_cleanup_includes_intermediate_directories_deepest_first(self):
        plan = scraper.Plan(
            mode="tv",
            source_root="/src/show",
            target_root="/library/show",
            files=[
                scraper.PlannedFile(
                    source_path="/src/show/disc/Season 01/old.mkv",
                    source_dir="/src/show/disc/Season 01",
                    original_name="old.mkv",
                    final_name="Show - S01E01.mkv",
                    target_dir="/library/show/Season 01",
                    media_kind="video",
                )
            ],
            warnings=[],
            metadata={"poster_path": None},
        )

        self.assertEqual(
            scraper._source_cleanup_directories(plan),
            ["/src/show/disc/Season 01", "/src/show/disc", "/src/show"],
        )

    def test_source_cleanup_failure_does_not_fail_committed_task(self):
        class CleanupFailingAList(FakeAList):
            def remove_empty_dir(self, path):
                raise scraper.ApiError("storage does not support directory cleanup")

        source = "/src/old.mkv"
        alist = CleanupFailingAList([{"name": "old.mkv", "full_path": source}])
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
            journal_path = Path(tmp) / "journal.json"
            scraper.execute_plan(
                alist,
                None,
                plan,
                journal_path=journal_path,
                skip_poster=True,
                cleanup_empty_source=True,
            )
            journal = json.loads(journal_path.read_text(encoding="utf-8"))

        self.assertTrue(journal["success"])
        cleanup_records = [
            record
            for record in journal["records"]
            if record["action"] == "cleanup-empty-source"
        ]
        self.assertEqual(cleanup_records[0]["status"], "failed")
        self.assertIn("does not support", cleanup_records[0]["message"])

    def test_undeletable_fileless_source_is_not_renamed(self):
        class UndeletableAList(FakeAList):
            def __init__(self, files):
                super().__init__(files)
                self.names_by_dir.setdefault("/", set()).add("src")
                self.entries_by_dir.setdefault("/", {})["src"] = {
                    "name": "src",
                    "is_dir": True,
                }

            def remove_empty_dir(self, path):
                return False

        source = "/src/old.mkv"
        alist = UndeletableAList([{"name": "old.mkv", "full_path": source}])
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
            journal_path = Path(tmp) / "journal.json"
            scraper.execute_plan(
                alist,
                None,
                plan,
                journal_path=journal_path,
                skip_poster=True,
                cleanup_empty_source=True,
            )
            journal = json.loads(journal_path.read_text(encoding="utf-8"))

        self.assertIn("src", alist.names_by_dir["/"])
        self.assertNotIn("src（待删）", alist.names_by_dir["/"])
        self.assertFalse(any(operation[0] == "rename" for operation in alist.operations))
        self.assertFalse(any(
            record["action"] == "mark-source-pending-delete"
            for record in journal["records"]
        ))

    def test_fileless_pending_delete_rename_helper_is_removed(self):
        alist = FakeAList([])
        alist.names_by_dir["/src"] = {"keep.txt"}
        alist.entries_by_dir["/src"] = {
            "keep.txt": {"name": "keep.txt", "is_dir": False}
        }
        self.assertFalse(hasattr(scraper, "_mark_fileless_source_pending_delete"))

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
        self.assertFalse(any(operation[0] in {"move", "rename"} for operation in alist.operations))

    def test_recovery_recreates_nested_original_directories_before_move(self):
        alist = FakeAList([{
            "name": "new.mkv", "full_path": "/library/Show/new.mkv", "size": 1,
        }])
        alist.mkdir("/unscraped")
        plan = scraper.Plan(
            mode="movie", source_root="/unscraped/Old",
            target_root="/library/Show",
            files=[scraper.PlannedFile(
                source_path="/unscraped/Old/Season 01/old.mkv",
                source_dir="/unscraped/Old/Season 01",
                original_name="old.mkv", final_name="new.mkv",
                target_dir="/library/Show", media_kind="video",
                source_size=1, source_modified="2026-01-01T00:00:00Z",
            )],
            warnings=[], metadata={
                "tmdb_id": 1, "title": "Show", "year": "2026",
                "poster_path": None,
            },
        )
        records = [{
            "action": "move", "source": "/unscraped/Old/Season 01",
            "target": "/library/Show", "status": "ok", "message": "new.mkv",
        }]
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "recovery.json"
            scraper.recover_execution(
                alist, plan, records, recovery_journal_path=path,
            )
            recovered, _digest, _rows = scraper.load_execution_journal(path)
        self.assertTrue(recovered.success)
        self.assertIn("old.mkv", alist.names_by_dir["/unscraped/Old/Season 01"])
        self.assertIn(("mkdir", "/unscraped/Old"), alist.operations)
        self.assertIn(("mkdir", "/unscraped/Old/Season 01"), alist.operations)

    def test_recovery_accepts_provider_mtime_change_caused_by_rename(self):
        class MtimeChangingAList(FakeAList):
            def rename(self, path, new_name):
                super().rename(path, new_name)
                parent, _ = scraper.split_remote(path)
                self.entries_by_dir[parent][new_name]["modified"] = (
                    "2026-07-23T02:02:42Z"
                )

            def move(self, src_dir, dst_dir, names):
                super().move(src_dir, dst_dir, names)
                for name in names:
                    self.entries_by_dir[dst_dir][name]["modified"] = (
                        "2026-07-23T02:02:42Z"
                    )

        alist = MtimeChangingAList(
            [
                {
                    "name": "new.mkv",
                    "full_path": "/dst/new.mkv",
                    "size": 1,
                    "modified": "2026-01-01T00:00:00Z",
                }
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
            metadata={"tmdb_id": 1, "title": "Movie", "year": "2026"},
        )
        records = [
            {
                "action": "move",
                "source": "/src",
                "target": "/dst",
                "status": "ok",
                "message": "new.mkv",
            }
        ]
        with tempfile.TemporaryDirectory() as tmp:
            scraper.recover_execution(
                alist,
                plan,
                records,
                recovery_journal_path=Path(tmp) / "recovery.json",
            )
        self.assertIn("old.mkv", alist.names_by_dir["/src"])

    def test_recovery_does_not_require_intentionally_deleted_cleanup_file(self):
        alist = FakeAList([{
            "name": "new.mkv", "full_path": "/dst/new.mkv", "size": 1,
            "modified": "2026-01-01T00:00:00Z",
        }])
        plan = scraper.Plan(
            mode="movie", source_root="/src", target_root="/dst",
            files=[scraper.PlannedFile(
                source_path="/src/old.mkv", source_dir="/src",
                original_name="old.mkv", final_name="new.mkv",
                target_dir="/dst", media_kind="video", source_size=1,
                source_modified="2026-01-01T00:00:00Z",
            )],
            cleanup_files=[scraper.PlannedCleanup(
                source_path="/src/NCED01.mkv", source_dir="/src",
                original_name="NCED01.mkv", reason="无字幕片头/片尾视频",
            )],
            warnings=[], metadata={
                "tmdb_id": 1, "title": "Movie", "year": "2026",
                "poster_path": None,
            },
        )
        records = [{
            "action": "move", "source": "/src", "target": "/dst",
            "status": "ok", "message": "new.mkv",
        }, {
            "action": "cleanup", "source": "/src/NCED01.mkv", "target": "",
            "status": "ok", "message": "无字幕片头/片尾视频",
        }]
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "recovery.json"
            scraper.recover_execution(
                alist, plan, records, recovery_journal_path=path,
            )
            recovered, _digest, _rows = scraper.load_execution_journal(path)
        self.assertTrue(recovered.success)
        self.assertIn("old.mkv", alist.names_by_dir["/src"])

    def test_final_verification_rejects_changed_target_identity(self):
        class CorruptingAList(FakeAList):
            def upload_file(self, target_path, source, content_type="application/octet-stream"):
                super().upload_file(target_path, source, content_type)
                if not target_path.startswith("/dst/"):
                    return
                target_dir, name = scraper.split_remote(target_path)
                self.entries_by_dir[target_dir][name]["_content"] = b"corrupt"
                self.entries_by_dir[target_dir][name]["size"] = 7

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
            with self.assertRaisesRegex(scraper.ScraperError, "wrong size|different size"):
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
        self.assertNotIn("/dst", alist.names_by_dir)
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

    def test_same_directory_name_swap_uses_only_staged_safe_transfers(self):
        alist = FakeAList([
            {"name": "a.mkv", "full_path": "/src/a.mkv", "size": 1, "_content": b"a"},
            {"name": "b.mkv", "full_path": "/src/b.mkv", "size": 1, "_content": b"b"},
        ])
        with tempfile.TemporaryDirectory() as tmp:
            stage_root = Path(tmp)
            rows = [
                ("/src/a.mkv", "/src/.safe-a.mkv", "/src/b.mkv"),
                ("/src/b.mkv", "/src/.safe-b.mkv", "/src/a.mkv"),
            ]
            for source, temporary, _target in rows:
                scraper._prepare_remote_transfer(
                    alist, source, temporary, stage_root=stage_root,
                    transaction_scope="swap-temp", expected_size=1,
                )
            for source, temporary, _target in rows:
                scraper._execute_remote_transfer(
                    alist, source, temporary, stage_root=stage_root,
                    transaction_scope="swap-temp", expected_size=1,
                )
            for _source, temporary, target in rows:
                scraper._execute_remote_transfer(
                    alist, temporary, target, stage_root=stage_root,
                    transaction_scope="swap-final", expected_size=1,
                )
        self.assertEqual(alist.entries_by_dir["/src"]["a.mkv"]["_content"], b"b")
        self.assertEqual(alist.entries_by_dir["/src"]["b.mkv"]["_content"], b"a")
        self.assertFalse(any(operation[0] in {"move", "rename"} for operation in alist.operations))

    def test_keyboard_interrupt_runs_rollback(self):
        class InterruptingAList(FakeAList):
            def __init__(self, files):
                super().__init__(files)
                self.interrupted = False

            def upload_file(self, target_path, source, content_type="application/octet-stream"):
                super().upload_file(target_path, source, content_type)
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

    def test_existing_poster_must_match_generated_content(self):
        source = "/src/old.mkv"
        alist = FakeAList(
            [
                {"name": "old.mkv", "full_path": source},
                {"name": "folder.jpg", "full_path": "/dst/folder.jpg"},
            ]
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
            metadata={"poster_path": "/poster.jpg"},
        )
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaisesRegex(scraper.ScraperError, "different size"):
                scraper.execute_plan(
                    alist,
                    FakeTMDB({}),
                    plan,
                    journal_path=Path(tmp) / "journal.json",
                    skip_poster=False,
                )
        self.assertIn("old.mkv", alist.names_by_dir["/src"])
        self.assertFalse(
            any(
                operation[0] == "upload"
                and operation[1].lower().endswith("/folder.jpg")
                and operation[2] == "image/jpeg"
                for operation in alist.operations
            )
        )


    def test_poster_failure_before_work_commit_rolls_back_files(self):
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
            with self.assertRaisesRegex(scraper.ScraperError, "已尝试回滚"):
                scraper.execute_plan(
                    alist,
                    PosterFailureTMDB({}),
                    plan,
                    journal_path=Path(tmp) / "journal.json",
                    skip_poster=False,
                )
        self.assertNotIn("new.mkv", alist.names_by_dir["/dst"])
        self.assertIn("old.mkv", alist.names_by_dir["/src"])
        self.assertFalse(any(scraper.is_scraper_lock(name) for name in alist.names_by_dir["/src"]))

    def test_existing_poster_case_variant_is_content_verified(self):
        source = "/src/old.mkv"
        alist = FakeAList(
            [
                {"name": "old.mkv", "full_path": source},
                {"name": "Folder.JPG", "full_path": "/dst/Folder.JPG"},
            ]
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
            metadata={"poster_path": "/poster.jpg"},
        )
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaisesRegex(scraper.ScraperError, "different size"):
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
    def test_alist_download_url_rejects_private_targets_and_redirects(self):
        client = scraper.AListClient("https://alist.example", "admin", "")
        with self.assertRaisesRegex(scraper.ApiError, "内网或保留网段"):
            client._validate_download_url("http://127.0.0.1:8765/api/session")
        handler = scraper.ValidatingRedirectHandler(client._validate_download_url)
        with self.assertRaisesRegex(scraper.ApiError, "内网或保留网段"):
            handler.redirect_request(
                urllib.request.Request("https://public.example/file"),
                None,
                302,
                "Found",
                {},
                "https://169.254.169.254/latest/meta-data",
            )

    def test_redirect_strips_credentials_and_rejects_https_downgrade(self):
        handler = scraper.ValidatingRedirectHandler()
        request = urllib.request.Request(
            "https://files.example/start",
            headers={"Authorization": "secret-token", "Cookie": "session=secret"},
        )
        redirected = handler.redirect_request(
            request, None, 302, "Found", {}, "https://cdn.example/file"
        )
        self.assertIsNotNone(redirected)
        self.assertNotIn("Authorization", redirected.headers)
        self.assertNotIn("Cookie", redirected.headers)
        with self.assertRaisesRegex(scraper.ApiError, "降级"):
            handler.redirect_request(
                request, None, 302, "Found", {}, "http://files.example/file"
            )

    def test_alist_api_redirect_must_remain_on_exact_origin(self):
        client = scraper.AListClient("https://alist.example", "admin", "secret")
        client._validate_api_url("https://alist.example/api/public/settings")
        with self.assertRaisesRegex(scraper.ApiError, "不同来源"):
            client._validate_api_url("https://evil.example/api/public/settings")
        with self.assertRaisesRegex(scraper.ApiError, "不同来源"):
            client._validate_api_url("https://alist.example:8443/api/public/settings")

    def test_alist_download_url_allows_only_exact_configured_private_origin(self):
        client = scraper.AListClient("http://127.0.0.1:5244", "admin", "")
        client._validate_download_url("http://127.0.0.1:5244/d/file")
        with self.assertRaisesRegex(scraper.ApiError, "内网或保留网段"):
            client._validate_download_url("http://127.0.0.1:8765/api/jobs")
        with mock.patch.object(
            scraper.socket,
            "getaddrinfo",
            return_value=[(None, None, None, None, ("93.184.216.34", 8443))],
        ), self.assertRaisesRegex(scraper.ApiError, "端口"):
            client._validate_download_url("https://downloads.example:8443/file")

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

    def test_archive_password_aliases_and_markers_are_redacted(self):
        url = "https://example.test/ae?pass=archive-secret&archive_pass=second-secret"
        redacted_url = scraper._redact_url(url)
        self.assertNotIn("archive-secret", redacted_url)
        self.assertNotIn("second-secret", redacted_url)
        message = scraper._redact_sensitive_text(
            "archive_pass=third-secret /归档/解压密码：folder-secret/file.7z"
        )
        self.assertNotIn("third-secret", message)
        self.assertNotIn("folder-secret", message)

    def test_url_redaction_hides_unknown_provider_signature_values(self):
        redacted = scraper._redact_url(
            "https://cdn.example/file?auth_key=provider-secret&x-custom-ticket=other-secret"
        )
        self.assertNotIn("provider-secret", redacted)
        self.assertNotIn("other-secret", redacted)
        self.assertIn("auth_key=%3Credacted%3E", redacted)

    def test_special_separators_preserve_number(self):
        cases = {
            "Show.SP.02.mkv": 2,
            "Show.SP-03.mkv": 3,
            "Show.SP_04.mkv": 4,
            "Show.OVA.05.mkv": 5,
            "Show [13 OAV].mkv": 13,
            "Show.Special-06.mkv": 6,
            "Tensei Shitara Slime Datta Ken OAD Series [01].mkv": 1,
            "The Quintessential Quintuplets SP [01].mkv": 1,
        }
        for name, number in cases.items():
            with self.subTest(name=name):
                self.assertEqual(
                    scraper.extract_episode_key(name),
                    scraper.EpisodeKey("special", number),
                )

    def test_postseason_oav_suffix_requires_complete_boundary_and_unique_special(self):
        regular = {
            scraper.EpisodeKey("regular", number): [
                {"name": f"Show [{number:02d}].mkv", "full_path": f"/src/{number:02d}.mkv"}
            ]
            for number in range(1, 14)
        }
        oav = {
            "name": "Show [13 OAV].mkv",
            "full_path": "/src/Show [13 OAV].mkv",
        }
        groups = {**regular, scraper.EpisodeKey("special", 13): [oav]}
        official = {scraper.EpisodeKey("special", 1): "Project Pink"}

        warnings = scraper._remap_postseason_oav_suffix(groups, official)

        self.assertNotIn(scraper.EpisodeKey("special", 13), groups)
        self.assertEqual(groups[scraper.EpisodeKey("special", 1)], [oav])
        self.assertTrue(any("完整 E01–E13" in warning for warning in warnings))

        incomplete = {
            key: value
            for key, value in {**regular, scraper.EpisodeKey("special", 13): [oav]}.items()
            if key != scraper.EpisodeKey("regular", 12)
        }
        self.assertEqual(
            scraper._remap_postseason_oav_suffix(incomplete, official),
            [],
        )

    def test_suffix_oav_with_plain_finale_and_next_extra_is_on_air_version(self):
        groups = {
            scraper.EpisodeKey("regular", number): [
                {"name": f"Show [{number:02d}].mkv", "full_path": f"/src/{number:02d}.mkv"}
            ]
            for number in range(1, 15)
        }
        on_air = {
            "name": "Show [13 OAV].mkv",
            "full_path": "/src/Show [13 OAV].mkv",
        }
        groups[scraper.EpisodeKey("special", 13)] = [on_air]

        warnings = scraper._remap_suffix_oav_on_air_versions(groups, {1: 13})

        self.assertNotIn(scraper.EpisodeKey("special", 13), groups)
        self.assertIn(on_air, groups[scraper.EpisodeKey("regular", 13)])
        self.assertEqual(on_air["_episode_kind_override"], "regular")
        self.assertEqual(on_air["_episode_key_override"], 13)
        self.assertEqual(scraper.entry_edition_tag(on_air), "On-Air Version")
        self.assertTrue(any("On-Air Version" in warning for warning in warnings))

    def test_smart_plan_resolves_postseason_oav_before_splitting_groups(self):
        files = [
            {
                "name": f"Sora no Otoshimono [{number:02d}].mkv",
                "full_path": f"/src/Sora no Otoshimono [{number:02d}].mkv",
            }
            for number in range(1, 14)
        ] + [
            {
                "name": "Sora no Otoshimono [13 OAV].mkv",
                "full_path": "/src/Sora no Otoshimono [13 OAV].mkv",
            },
            {
                "name": "Sora no Otoshimono [13 OAV].ass",
                "full_path": "/src/备份字幕/Sora no Otoshimono [13 OAV].ass",
            },
        ]
        responses = {
            "/tv/10": {
                "name": "天降之物",
                "original_name": "そらのおとしもの",
                "first_air_date": "2009-10-05",
                "poster_path": None,
                "seasons": [
                    {"season_number": 0, "episode_count": 1},
                    {"season_number": 1, "episode_count": 13},
                ],
            },
            "/tv/10/season/0": {
                "episodes": [{"episode_number": 1, "name": "Project Pink"}]
            },
            "/tv/10/season/1": {
                "episodes": [
                    {"episode_number": number, "name": f"Episode {number}"}
                    for number in range(1, 14)
                ]
            },
        }
        plan = scraper.build_tv_plan_smart(
            auto_episode_mode=True,
            alist=FakeAList(files),
            tmdb_client=FakeTMDB(responses),
            src_path="/src",
            parent_path="/library",
            tmdb_id=10,
            season=1,
            absolute=False,
            prefer_simplified=True,
            allow_unmapped=False,
            ignore_orphan_temp=False,
            episode_map_path=None,
            episode_group_id=None,
            source_files=files,
        )
        oav = [item for item in plan.files if "13 OAV" in item.original_name]
        self.assertEqual(len(oav), 2)
        self.assertTrue(all("S00E01" in item.final_name for item in oav))
        self.assertFalse(any("13 OAV" in item.source_path for item in plan.problem_files))

    def test_smart_plan_keeps_suffix_oav_as_on_air_finale_when_extra_is_numbered_14(self):
        files = [
            {
                "name": f"Show [{number:02d}].mkv",
                "full_path": f"/src/Season 01/Show [{number:02d}].mkv",
            }
            for number in range(1, 15)
        ] + [
            {
                "name": "Show [13 OAV].mkv",
                "full_path": "/src/Season 01/Show [13 OAV].mkv",
            },
            {
                "name": "Show [13 OAV].ass",
                "full_path": "/src/Season 01/backup/Show [13 OAV].ass",
            },
        ]
        responses = {
            "/tv/10": {
                "name": "Show",
                "first_air_date": "2009-01-01",
                "poster_path": None,
                "seasons": [
                    {"season_number": 0, "episode_count": 1},
                    {"season_number": 1, "episode_count": 13},
                ],
            },
            "/tv/10/season/0": {
                "episodes": [{"episode_number": 1, "name": "OVA"}]
            },
            "/tv/10/season/1": {
                "episodes": [
                    {"episode_number": number, "name": f"Episode {number}"}
                    for number in range(1, 14)
                ]
            },
        }
        plan = scraper.build_tv_plan_smart(
            auto_episode_mode=True,
            alist=FakeAList(files), tmdb_client=FakeTMDB(responses),
            src_path="/src", parent_path="/library", tmdb_id=10, season=1,
            absolute=False, prefer_simplified=True, allow_unmapped=False,
            ignore_orphan_temp=False, episode_map_path=None,
            episode_group_id=None, source_files=files,
        )
        on_air = [item for item in plan.files if "13 OAV" in item.original_name]
        self.assertEqual(len(on_air), 2)
        self.assertTrue(all("S01E13" in item.final_name for item in on_air))
        self.assertTrue(any("On-Air Version" in item.final_name for item in on_air))
        self.assertFalse(any("13 OAV" in item.source_path for item in plan.problem_files))

    def test_oad_series_number_remaps_only_from_official_oad_label(self):
        source = scraper.EpisodeKey("special", 1)
        official = scraper.EpisodeKey("special", 2)
        item = {
            "name": "Tensei Shitara Slime Datta Ken OAD Series [01].mkv",
            "full_path": "/src/OAD/Tensei Shitara Slime Datta Ken OAD Series [01].mkv",
        }
        groups = {source: [item]}
        warnings = scraper._remap_numbered_specials_by_official_label(
            groups,
            {
                scraper.EpisodeKey("special", 1): "第24.5话 闲话：维鲁多拉日记",
                official: "OAD#1 外传：HEY！屁股！",
            },
        )
        self.assertNotIn(source, groups)
        self.assertEqual(groups[official], [item])
        self.assertTrue(any("SP01" in warning and "SP02" in warning for warning in warnings))

    def test_dated_two_episode_oad_folder_maps_to_unique_official_special_run(self):
        items = [
            {
                "name": (
                    f"[Ygm] Gintama OAD 2016 [{number:02d}]"
                    "[Ma10p_2160p][x265_flac_srt].mkv"
                ),
                "full_path": (
                    "/src/银魂 爱染香篇/"
                    f"[Ygm] Gintama OAD 2016 [{number:02d}]"
                    "[Ma10p_2160p][x265_flac_srt].mkv"
                ),
            }
            for number in (1, 2)
        ]

        warnings = scraper._map_explicit_special_release_runs(
            items,
            {
                8: ["银魂 爱染香篇 前篇", "Gintama: Love Incense Arc Part 1"],
                9: ["银魂 爱染香篇 后篇", "Gintama: Love Incense Arc Part 2"],
            },
            {8: "2016-08-04", 9: "2016-11-04"},
        )

        self.assertEqual(
            [item["_episode_key_override"] for item in items],
            [8, 9],
        )
        self.assertTrue(all(item["_episode_kind_override"] == "special" for item in items))
        self.assertTrue(any("2016 年官方发行时间线" in warning for warning in warnings))
        self.assertTrue(any("SP08–SP09" in warning for warning in warnings))

    def test_smart_plan_maps_real_dated_oad_pair_into_season_zero(self):
        files = [
            {
                "name": "Gintama [01].mkv",
                "full_path": "/src/银魂 第一季/Gintama [01].mkv",
                "size": 1_000,
            },
            *[
                {
                    "name": (
                        f"[Ygm] Gintama OAD 2016 [{number:02d}]"
                        "[Ma10p_2160p][x265_flac_srt].mkv"
                    ),
                    "full_path": (
                        "/src/银魂 爱染香篇/"
                        f"[Ygm] Gintama OAD 2016 [{number:02d}]"
                        "[Ma10p_2160p][x265_flac_srt].mkv"
                    ),
                    "size": 1_000 + number,
                }
                for number in (1, 2)
            ],
        ]
        tmdb = FakeTMDB(
            {
                "/tv/10": {
                    "name": "银魂",
                    "original_name": "銀魂",
                    "first_air_date": "2006-04-04",
                    "poster_path": None,
                    "seasons": [
                        {"season_number": 0, "episode_count": 2},
                        {"season_number": 1, "episode_count": 1},
                    ],
                },
                "/tv/10/season/0": {
                    "episodes": [
                        {
                            "episode_number": 8,
                            "name": "银魂 爱染香篇 前篇",
                            "air_date": "2016-08-04",
                            "runtime": 24,
                        },
                        {
                            "episode_number": 9,
                            "name": "银魂 爱染香篇 后篇",
                            "air_date": "2016-11-04",
                            "runtime": 24,
                        },
                    ]
                },
                "/tv/10/season/1": {
                    "episodes": [
                        {
                            "episode_number": 1,
                            "name": "First",
                            "air_date": "2006-04-04",
                            "runtime": 24,
                        }
                    ]
                },
            }
        )

        plan = scraper.build_tv_plan_smart(
            auto_episode_mode=True,
            alist=FakeAList(files),
            tmdb_client=tmdb,
            src_path="/src",
            parent_path="/library",
            tmdb_id=10,
            season=1,
            absolute=False,
            prefer_simplified=True,
            allow_unmapped=False,
            ignore_orphan_temp=False,
            episode_map_path=None,
            episode_group_id=None,
            source_files=files,
        )

        oad = [item for item in plan.files if "OAD 2016" in item.original_name]
        self.assertEqual(len(oad), 2)
        self.assertEqual(
            [item.final_name for item in oad],
            [
                "银魂 - S00E08 - 银魂 爱染香篇 前篇.mkv",
                "银魂 - S00E09 - 银魂 爱染香篇 后篇.mkv",
            ],
        )
        self.assertTrue(all(item.target_dir == "/library/银魂/Season 00" for item in oad))
        self.assertFalse(any("OAD 2016" in item.source_path for item in plan.problem_files))

    def test_dated_oad_run_rejects_weak_year_and_near_candidate_evidence(self):
        def release(parent: str, year: int = 2016):
            return [
                {
                    "name": f"Example OAD {year} [{number:02d}].mkv",
                    "full_path": f"/src/{parent}/Example OAD {year} [{number:02d}].mkv",
                }
                for number in (1, 2)
            ]

        cases = (
            (
                "generic parent has no official-title identity",
                release("OAD"),
                {8: ["Example Love Arc Part 1"], 9: ["Example Love Arc Part 2"]},
                {8: "2016-01-01", 9: "2016-02-01"},
            ),
            (
                "official timeline disagrees with explicit source year",
                release("Example Love Arc"),
                {8: ["Example Love Arc Part 1"], 9: ["Example Love Arc Part 2"]},
                {8: "2017-01-01", 9: "2017-02-01"},
            ),
            (
                "two equal official runs are near candidates",
                release("Example Love Arc"),
                {
                    8: ["Example Love Arc Part 1"],
                    9: ["Example Love Arc Part 2"],
                    12: ["Example Love Arc Part 1"],
                    13: ["Example Love Arc Part 2"],
                },
                {
                    8: "2016-01-01",
                    9: "2016-02-01",
                    12: "2016-03-01",
                    13: "2016-04-01",
                },
            ),
        )
        for label, items, titles, dates in cases:
            with self.subTest(label=label):
                self.assertEqual(
                    scraper._map_explicit_special_release_runs(items, titles, dates),
                    [],
                )
                self.assertTrue(
                    all("_episode_key_override" not in item for item in items)
                )

    def test_oad_video_and_subtitle_persist_the_same_explicit_override(self):
        video = {
            "name": "Tensei Shitara Slime Datta Ken OAD Series [01].mkv",
            "full_path": "/src/OAD/OAD Series [01].mkv",
        }
        subtitle = {
            "name": "Tensei Shitara Slime Datta Ken OAD Series [01].ass",
            "full_path": "/src/OAD/外挂字幕/OAD Series [01].ass",
        }
        groups = {scraper.EpisodeKey("special", 1): [video, subtitle]}
        scraper._remap_numbered_specials_by_official_label(
            groups,
            {
                scraper.EpisodeKey("special", 1): "第24.5话 闲话：维鲁多拉日记",
                scraper.EpisodeKey("special", 2): "OAD#1 外传：HEY！屁股！",
            },
        )
        self.assertEqual(groups[scraper.EpisodeKey("special", 2)], [video, subtitle])
        self.assertTrue(all(item["_episode_kind_override"] == "special" for item in (video, subtitle)))
        self.assertTrue(all(item["_episode_key_override"] == 2 for item in (video, subtitle)))

    def test_equal_ova_numbers_split_by_distinct_subseries_title(self):
        original_video = {
            "name": "To Love-Ru Trouble - OVA03.mkv",
            "full_path": "/src/Season 01/OVA/To Love-Ru Trouble - OVA03.mkv",
        }
        darkness_video = {
            "name": "To Love-Ru Trouble Darkness - OVA 03.mkv",
            "full_path": "/src/Season 03/OAD/To Love-Ru Trouble Darkness - OVA 03.mkv",
        }
        darkness_subtitle = {
            "name": "To Love-Ru Trouble Darkness - OVA 03.ass",
            "full_path": "/src/Season 03/OAD/To Love-Ru Trouble Darkness - OVA 03.ass",
        }
        source = scraper.EpisodeKey("special", 3)
        darkness_target = scraper.EpisodeKey("special", 9)
        groups = {source: [original_video, darkness_video, darkness_subtitle]}

        warnings = scraper._remap_numbered_specials_by_official_label(
            groups,
            {
                source: "OVA#3「欢迎来到南岛」",
                darkness_target: "Darkness OVA#3「Exchange」",
            },
        )

        self.assertEqual(groups[source], [original_video])
        self.assertEqual(groups[darkness_target], [darkness_video, darkness_subtitle])
        self.assertTrue(any("子系列标题" in warning for warning in warnings))
        self.assertEqual(darkness_video["_episode_key_override"], 9)
        self.assertEqual(darkness_subtitle["_episode_key_override"], 9)

    def test_equal_ova_numbers_do_not_split_without_subseries_evidence(self):
        first = {"name": "Show OVA03 1080p.mkv", "full_path": "/src/OVA03 1080p.mkv"}
        second = {"name": "Show OVA03 720p.mkv", "full_path": "/src/OVA03 720p.mkv"}
        source = scraper.EpisodeKey("special", 3)
        groups = {source: [first, second]}

        warnings = scraper._remap_numbered_specials_by_official_label(
            groups,
            {
                source: "OVA#3 Original",
                scraper.EpisodeKey("special", 9): "Darkness OVA#3",
            },
        )

        self.assertEqual(groups, {source: [first, second]})
        self.assertEqual(warnings, [])

    def test_oad_series_shift_is_applied_atomically_without_overwriting_groups(self):
        groups = {
            scraper.EpisodeKey("special", number): [
                {
                    "name": f"Tensei Shitara Slime Datta Ken OAD Series [{number:02d}].mkv",
                    "full_path": (
                        "/src/OAD/Tensei Shitara Slime Datta Ken "
                        f"OAD Series [{number:02d}].mkv"
                    ),
                }
            ]
            for number in range(1, 6)
        }
        titles = {
            scraper.EpisodeKey("special", number + 1): f"OAD#{number} Official title"
            for number in range(1, 6)
        }

        warnings = scraper._remap_numbered_specials_by_official_label(groups, titles)

        self.assertEqual(sorted(key.number for key in groups), [2, 3, 4, 5, 6])
        self.assertEqual(sum(len(items) for items in groups.values()), 5)
        for source_number in range(1, 6):
            target = scraper.EpisodeKey("special", source_number + 1)
            self.assertIn(f"[{source_number:02d}]", groups[target][0]["name"])
        self.assertEqual(len(warnings), 5)

    def test_merged_tv_subplans_keep_4k_episode_and_matching_subtitle(self):
        def planned(name, final_name, kind):
            source_dir = "/src/4K" if "2160p" in name else "/src/1080P"
            return scraper.PlannedFile(
                source_path=f"{source_dir}/{name}",
                source_dir=source_dir,
                original_name=name,
                final_name=final_name,
                target_dir="/library/Show/Season 00",
                media_kind=kind,
            )

        plan = scraper.Plan(
            mode="tv",
            source_root="/src",
            target_root="/library/Show",
            files=[
                planned("Show.OAD01.2160p.mkv", "Show - S00E02 - OAD 1.mkv", "video"),
                planned("Show.OAD01.1080p.mkv", "Show - S00E02 - OAD 1.mkv", "video"),
                planned(
                    "Show.OAD01.2160p.ass",
                    "Show - S00E02 - OAD 1.subtitle2.ass",
                    "subtitle",
                ),
                planned(
                    "Show.OAD01.1080p.ass",
                    "Show - S00E02 - OAD 1.subtitle.ass",
                    "subtitle",
                ),
            ],
            warnings=[],
            metadata={"tmdb_id": 10, "title": "Show"},
        )

        scraper._dedupe_merged_tv_target_variants(plan)

        self.assertEqual(
            {item.original_name for item in plan.files},
            {"Show.OAD01.2160p.mkv", "Show.OAD01.2160p.ass"},
        )
        self.assertEqual(
            {item.original_name for item in plan.cleanup_files},
            {"Show.OAD01.1080p.mkv", "Show.OAD01.1080p.ass"},
        )

    def test_merged_tv_dedupe_rebases_transitive_cleanup_to_final_4k_winner(self):
        video_4k = scraper.PlannedFile(
            source_path="/src/4K/Show.E25.2160p.mkv", source_dir="/src/4K",
            original_name="Show.E25.2160p.mkv", final_name="Show - S00E02.mkv",
            target_dir="/library/Show/Season 00", media_kind="video",
        )
        video_1080 = scraper.PlannedFile(
            source_path="/src/1080P/Show.E25.1080p.mkv", source_dir="/src/1080P",
            original_name="Show.E25.1080p.mkv", final_name="Show - S00E02.mkv",
            target_dir="/library/Show/Season 00", media_kind="video",
        )
        cleanup_720 = scraper.PlannedCleanup(
            source_path="/src/720P/Show.E25.720p.mp4", source_dir="/src/720P",
            original_name="Show.E25.720p.mp4",
            reason=scraper._lower_resolution_cleanup_reason(video_1080.source_path),
        )
        plan = scraper.Plan(
            mode="tv", source_root="/src", target_root="/library/Show",
            files=[video_4k, video_1080], cleanup_files=[cleanup_720], warnings=[],
            metadata={"tmdb_id": 10, "title": "Show"},
        )

        scraper._dedupe_merged_tv_target_variants(plan)

        self.assertEqual([item.source_path for item in plan.files], [video_4k.source_path])
        cleanup_by_source = {item.source_path: item for item in plan.cleanup_files}
        self.assertEqual(
            cleanup_by_source[cleanup_720.source_path].reason,
            scraper._lower_resolution_cleanup_reason(video_4k.source_path),
        )
        self.assertIn(video_1080.source_path, cleanup_by_source)

    def test_merged_tv_quality_dedupe_keeps_distinct_editions_parts_and_themes(self):
        files = [
            scraper.PlannedFile(
                source_path=f"/src/{name}", source_dir="/src", original_name=name,
                final_name=final_name, target_dir="/library/Show/Season 01",
                media_kind="video",
            )
            for name, final_name in (
                ("Show.E01.2160p.mkv", "Show - S01E01.mkv"),
                ("Show.E01.Directors.Cut.1080p.mkv", "Show - S01E01 - Director's Cut.mkv"),
                ("Show.E01.Part2.1080p.mkv", "Show - S01E01 - part 2.mkv"),
                ("Show.OP.1080p.mkv", "Show - S01E01 - OP.mkv"),
                ("Show.ED.1080p.mkv", "Show - S01E01 - ED.mkv"),
            )
        ]
        plan = scraper.Plan(
            mode="tv", source_root="/src", target_root="/library/Show",
            files=files, warnings=[], metadata={"tmdb_id": 10, "title": "Show"},
        )

        scraper._dedupe_merged_tv_target_variants(plan)

        self.assertEqual(len(plan.files), 5)
        self.assertEqual(plan.cleanup_files, [])

    def test_smart_split_dedupes_slime_oad_4k_and_1080p_across_subplans(self):
        files = [
            {
                "name": f"[TUDO] Slime [{number:02d}][Ma10p_2160p].mkv",
                "full_path": (
                    "/src/关于我转生变成史莱姆这档事 第一季/"
                    f"[TUDO] Slime [{number:02d}][Ma10p_2160p].mkv"
                ),
                "size": 1000 + number,
            }
            for number in range(1, 25)
        ] + [
            {
                "name": "[TUDO] Slime [24.5][Ma10p_2160p].mkv",
                "full_path": (
                    "/src/关于我转生变成史莱姆这档事 第一季/"
                    "[TUDO] Slime [24.5][Ma10p_2160p].mkv"
                ),
                "size": 2000,
            },
            {
                "name": "[TUDO] Slime OAD Series [01][Ma10p_2160p].mkv",
                "full_path": (
                    "/src/关于我转生变成史莱姆这档事 第一季/"
                    "[TUDO] Slime OAD Series [01][Ma10p_2160p].mkv"
                ),
                "size": 3000,
            },
            {
                "name": "[TUDO] Slime OAD Series [01][Ma10p_2160p].ass",
                "full_path": (
                    "/src/关于我转生变成史莱姆这档事 第一季/"
                    "[TUDO] Slime OAD Series [01][Ma10p_2160p].ass"
                ),
                "size": 100,
            },
            {
                "name": "[VCB] Slime OAD Series [01][Ma10p_1080p].mkv",
                "full_path": (
                    "/src/1080P备份版/2018.10 轉生史萊姆 第一季/OAD/"
                    "[VCB] Slime OAD Series [01][Ma10p_1080p].mkv"
                ),
                "size": 1500,
            },
            {
                "name": "[VCB] Slime OAD Series [01][Ma10p_1080p].ass",
                "full_path": (
                    "/src/1080P备份版/2018.10 轉生史萊姆 第一季/OAD/外挂字幕/"
                    "[VCB] Slime OAD Series [01][Ma10p_1080p].ass"
                ),
                "size": 80,
            },
        ]
        responses = {
            "/tv/10": {
                "name": "关于我转生变成史莱姆这档事",
                "original_name": "転生したらスライムだった件",
                "first_air_date": "2018-01-01",
                "poster_path": None,
                "seasons": [
                    {"season_number": 0, "episode_count": 2},
                    {"season_number": 1, "episode_count": 24},
                ],
            },
            "/tv/10/season/0": {
                "episodes": [
                    {"episode_number": 1, "name": "第24.5话 闲话"},
                    {"episode_number": 2, "name": "OAD#1 外传"},
                ]
            },
            "/tv/10/season/1": {
                "episodes": [
                    {"episode_number": number, "name": f"Episode {number}"}
                    for number in range(1, 25)
                ]
            },
        }

        plan = scraper.build_tv_plan_smart(
            auto_episode_mode=True,
            alist=FakeAList(files), tmdb_client=FakeTMDB(responses),
            src_path="/src", parent_path="/library", tmdb_id=10, season=1,
            absolute=False, prefer_simplified=True, allow_unmapped=False,
            ignore_orphan_temp=False, episode_map_path=None,
            episode_group_id=None, source_files=files,
        )

        kept_oad = [item for item in plan.files if "OAD Series [01]" in item.original_name]
        cleaned_oad = [
            item for item in plan.cleanup_files if "OAD Series [01]" in item.original_name
        ]
        self.assertEqual(
            {item.original_name for item in kept_oad},
            {
                "[TUDO] Slime OAD Series [01][Ma10p_2160p].mkv",
                "[TUDO] Slime OAD Series [01][Ma10p_2160p].ass",
            },
        )
        self.assertEqual(
            {item.original_name for item in cleaned_oad},
            {
                "[VCB] Slime OAD Series [01][Ma10p_1080p].mkv",
            },
        )
        self.assertIn(
            "/src/1080P备份版/2018.10 轉生史萊姆 第一季/OAD/外挂字幕/"
            "[VCB] Slime OAD Series [01][Ma10p_1080p].ass",
            {
                row["source_path"]
                for row in plan.scan_report.get("deferred_subtitles", [])
            },
        )
        self.assertTrue(all("S00E02" in item.final_name for item in kept_oad))

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

    def test_font_cleanup_reason_survives_plan_file_validation(self):
        plan = scraper.Plan(
            mode="tv",
            source_root="/src",
            target_root="/dst",
            files=[],
            warnings=[],
            metadata={
                "tmdb_id": 1,
                "title": "Show",
                "year": "2026",
                "season": 1,
                "absolute": False,
                "poster_path": None,
                "backdrop_path": None,
            },
            cleanup_files=[
                scraper.PlannedCleanup(
                    source_path="/src/字体包.exe",
                    source_dir="/src",
                    original_name="字体包.exe",
                    reason="字体资源包",
                    source_size=1,
                )
            ],
        )
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "font-cleanup-plan.json"
            scraper.write_plan_json(plan, path)
            loaded, _digest = scraper.load_plan_json(path)
        self.assertEqual(loaded.cleanup_files[0].reason, "字体资源包")

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

    def test_compatibility_write_interfaces_are_permanently_removed(self):
        with self.assertRaisesRegex(scraper.ScraperError, "永久移除"):
            scraper.alist_rename("token", "/src/a.mkv", "b.mkv")
        with self.assertRaisesRegex(scraper.ScraperError, "永久移除"):
            scraper.alist_remove("token", "/src", ["a.mkv"])
        with self.assertRaisesRegex(scraper.ScraperError, "永久移除"):
            scraper.download_poster("token", "/poster.jpg", "/dst")

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
        # Hybrid preparation fails before any forward mkdir, so there is no
        # newly created user directory to retain or roll back.
        self.assertEqual(retained, [])
        self.assertNotIn("/dst", alist.names_by_dir)


class ArchiveToolTests(unittest.TestCase):
    @staticmethod
    def load_tool(module_name: str = "extract_archives_test_module"):
        tool_path = Path(__file__).resolve().parents[1] / "tools" / "extract_archives.py"
        spec = importlib.util.spec_from_file_location(module_name, tool_path)
        module = importlib.util.module_from_spec(spec)
        assert spec.loader is not None
        spec.loader.exec_module(module)
        return module

    @staticmethod
    def upload_context(module, root: Path):
        journal = {}
        return module.ArchiveLocalUploadContext(
            root / "transactions",
            "a" * 64,
            journal,
            root / "archive-journal.json",
        )

    @staticmethod
    def install_exact_upload_api(alist, *, store_mime: bool = False):
        def payload(path):
            value = alist.files.get(path)
            if value is None:
                return None
            return value[0] if isinstance(value, tuple) else value

        def exact_file_info(path):
            data = payload(path)
            if data is None:
                return None
            digest = hashlib.sha256(data).hexdigest()
            return {"size": len(data), "sha256": digest, "version": digest}

        def open_file_reader(path):
            data = payload(path)
            if data is None:
                raise OSError(path)
            return contextlib.closing(io.BytesIO(data))

        alist.exact_file_info = exact_file_info
        alist.open_file_reader = open_file_reader
        if not hasattr(alist, "upload_file"):
            def upload_file(target, source, content_type):
                if target in alist.files:
                    raise scraper.ApiError(f"target exists: {target}")
                data = source.read_bytes()
                alist.files[target] = (
                    (data, content_type) if store_mime else data
                )
            alist.upload_file = upload_file

    def test_old_alist_version_is_rejected_for_archive_extraction(self):
        module = self.load_tool("extract_archives_version_module")

        class OldAList:
            def server_version(self):
                return "v3.32.0"

        with self.assertRaisesRegex(scraper.ScraperError, "v3.57.0"):
            module.require_safe_archive_server(OldAList())

    def test_no_archives_is_a_non_error_control_flow_signal(self):
        module = self.load_tool("extract_archives_empty_module")

        class EmptyAList:
            def server_version(self):
                return "v3.62.0"

            def walk(self, path):
                return []

        with self.assertRaises(module.NoArchivesFound):
            module.build_archive_plan(
                EmptyAList(), "/media/show", explicit_archive_password=None
            )
        self.assertEqual(module.NO_ARCHIVES_EXIT_CODE, 3)

    def test_font_executable_is_cleanup_not_disguised_archive(self):
        module = self.load_tool("extract_archives_font_cleanup_module")
        entry = {
            "name": "字体包.exe",
            "full_path": "/media/show/字体包.exe",
            "is_dir": False,
            "size": 99,
        }

        class FontAList:
            def server_version(self):
                return "v3.62.0"

            def walk(self, path):
                return [dict(entry)]

            def read_file_prefix(self, path):
                raise AssertionError("字体清理项不应进入魔数或解压检测")

        with self.assertRaises(module.NoArchivesFound):
            module.build_archive_plan(
                FontAList(), "/media/show", explicit_archive_password=None
            )

    def test_document_txt_archives_are_skipped_before_metadata_reads(self):
        module = self.load_tool("extract_archives_txt_context_module")
        archive_path = "/media/show/文库版/txt/41.rar"

        class DocumentAList:
            def server_version(self):
                return "v3.62.0"

            def walk(self, path):
                return [{
                    "name": "41.rar", "full_path": archive_path,
                    "is_dir": False, "size": 1024,
                }]

            def archive_meta(self, *args, **kwargs):
                raise AssertionError("文档归档不应读取压缩包元数据")

        with self.assertRaises(module.NoArchivesFound):
            module.build_archive_plan(
                DocumentAList(), "/media/show", explicit_archive_password=None
            )

    def test_subtitle_archive_may_discard_bundled_fonts(self):
        module = self.load_tool("extract_archives_subtitle_font_module")
        archive = {
            "members": [
                {"path": "subs", "is_dir": True, "size": 0},
                {"path": "subs/Show.S01E01.ass", "is_dir": False, "size": 100},
                {"path": "fonts", "is_dir": True, "size": 0},
                {"path": "fonts/Example.ttf", "is_dir": False, "size": 200},
            ]
        }
        self.assertTrue(module._is_direct_subtitle_archive(archive))
        module._retain_only_subtitle_members(archive)
        self.assertEqual(
            [member["path"] for member in archive["members"]],
            ["subs", "subs/Show.S01E01.ass"],
        )

    def test_archive_with_video_is_never_classified_as_subtitle_only(self):
        module = self.load_tool("extract_archives_mixed_video_module")
        archive = {
            "members": [
                {"path": "Show.S01E01.ass", "is_dir": False, "size": 100},
                {"path": "Show.S01E01.mkv", "is_dir": False, "size": 2_000_000},
            ]
        }
        self.assertFalse(module._is_direct_subtitle_archive(archive))

    def test_tiny_instruction_zip_does_not_block_media_planning(self):
        module = self.load_tool("extract_archives_instruction_module")
        archive_path = "/media/show/下载后右键解压即可.zip"

        class InstructionAList:
            def server_version(self):
                return "v3.62.0"

            def walk(self, path):
                return [{"name": archive_path.rsplit("/", 1)[-1], "full_path": archive_path, "is_dir": False, "size": 482}]

            def list(self, path, refresh=False):
                return []

            def archive_meta(self, path, archive_password="", refresh=True):
                return {"content": [{"name": "下载说明.txt", "is_dir": False, "size": 82}]}

        with self.assertRaises(module.NoArchivesFound):
            module.build_archive_plan(
                InstructionAList(), "/media/show", explicit_archive_password=None
            )

    def test_large_nested_executable_archive_is_actionable(self):
        module = self.load_tool("extract_archives_nested_module")
        archive_path = "/media/show/内层压缩.zip"

        class NestedAList:
            def server_version(self):
                return "v3.62.0"

            def walk(self, path):
                return [{"name": "内层压缩.zip", "full_path": archive_path, "is_dir": False, "size": 4_000_000}]

            def list(self, path, refresh=False):
                return []

            def archive_meta(self, path, archive_password="", refresh=True):
                return {"content": [{"name": "视频.exe", "is_dir": False, "size": 3_000_000}]}

        with self.assertRaisesRegex(scraper.ScraperError, "请先在网盘中解开内层"):
            module.build_archive_plan(
                NestedAList(), "/media/show", explicit_archive_password=None
            )

    def test_exe_renamed_zip_is_planned_by_signature_without_running_it(self):
        module = self.load_tool("extract_archives_disguised_zip_module")
        source_root = "/media/show"
        entry = {
            "name": "Show.E02.exe",
            "full_path": f"{source_root}/Show.E02.exe",
            "is_dir": False,
            "size": 900_000_000,
            "modified": "2026-01-01T00:00:00Z",
        }

        class DisguisedArchiveAList:
            def server_version(self):
                return "v3.62.0"

            def walk(self, path):
                return [dict(entry)]

            def read_file_prefix(self, path):
                return b"PK\x03\x04" + b"\0" * 32

            def list(self, path, refresh=False):
                return [dict(entry)] if path == source_root else []

            def archive_meta(self, *_args, **_kwargs):
                raise AssertionError("规划阶段不应改名或试图按 .exe 解压")

        plan, _ = module.build_archive_plan(
            DisguisedArchiveAList(), source_root, explicit_archive_password=None
        )
        archive = plan["archives"][0]
        self.assertTrue(archive["deferred_inspection"])
        self.assertEqual(archive["detected_format"], "zip")
        self.assertEqual(archive["members"], [])

    def test_exe_renamed_matroska_is_planned_as_safe_extension_fix(self):
        module = self.load_tool("extract_archives_disguised_mkv_module")
        source_root = "/media/show"
        entry = {
            "name": "Show.E02.exe",
            "full_path": f"{source_root}/Show.E02.exe",
            "is_dir": False,
            "size": 900_000_000,
            "modified": "2026-01-01T00:00:00Z",
        }

        class DisguisedMediaAList:
            def server_version(self):
                return "v3.62.0"

            def walk(self, path):
                return [dict(entry)]

            def read_file_prefix(self, path):
                return b"\x1aE\xdf\xa3" + b"\0" * 32

            def list(self, path, refresh=False):
                return [dict(entry)] if path == source_root else []

        plan, _ = module.build_archive_plan(
            DisguisedMediaAList(), source_root, explicit_archive_password=None
        )
        self.assertEqual(plan["archives"], [])
        self.assertEqual(plan["media_renames"][0]["new_name"], "Show.E02.mkv")
        self.assertEqual(plan["media_renames"][0]["detected_format"], "mkv")

    def test_bin_and_dat_disguised_files_are_detected_by_magic(self):
        module = self.load_tool("extract_archives_disguised_bin_dat_module")
        source_root = "/media/show"
        entries = [
            {"name": "pack.bin", "full_path": f"{source_root}/pack.bin", "is_dir": False, "size": 99},
            {"name": "episode.dat", "full_path": f"{source_root}/episode.dat", "is_dir": False, "size": 99},
        ]

        class MagicAList:
            def server_version(self):
                return "v3.62.0"

            def walk(self, path):
                return [dict(item) for item in entries]

            def read_file_prefix(self, path):
                return b"PK\x03\x04" if path.endswith(".bin") else b"\x1aE\xdf\xa3"

            def list(self, path, refresh=False):
                return [dict(item) for item in entries] if path == source_root else []

            def archive_meta(self, *_args, **_kwargs):
                raise AssertionError("伪装归档规划阶段不应直接读取成员")

        plan, _ = module.build_archive_plan(
            MagicAList(), source_root, explicit_archive_password=None
        )
        self.assertEqual(plan["archives"][0]["detected_format"], "zip")
        self.assertEqual(plan["media_renames"][0]["new_name"], "episode.mkv")

    def test_unknown_disguised_magic_requires_manual_review(self):
        module = self.load_tool("extract_archives_unknown_magic_module")
        source_root = "/media/show"
        entry = {"name": "payload.dat", "full_path": f"{source_root}/payload.dat", "is_dir": False, "size": 99}

        class UnknownMagicAList:
            def server_version(self):
                return "v3.62.0"

            def walk(self, path):
                return [dict(entry)]

            def read_file_prefix(self, path):
                return b"unknown-format"

        with self.assertRaisesRegex(scraper.ScraperError, "魔数无法确认"):
            module.build_archive_plan(
                UnknownMagicAList(), source_root, explicit_archive_password=None
            )

    def test_missing_first_multipart_volume_is_rejected(self):
        module = self.load_tool("extract_archives_missing_first_module")

        class MissingFirstAList:
            def server_version(self):
                return "v3.62.0"

            def walk(self, path):
                return [
                    {
                        "name": "3.zip.002",
                        "full_path": f"{path}/3/3.zip.002",
                        "is_dir": False,
                    }
                ]

        with self.assertRaisesRegex(scraper.ScraperError, "缺少第 001 卷"):
            module.build_archive_plan(
                MissingFirstAList(), "/media/show", explicit_archive_password=None
            )

    def test_password_marker_is_discovered_without_logging_value(self):
        module = self.load_tool("extract_archives_password_module")

        class MarkerAList:
            def list(self, path, refresh=False):
                return [
                    {"name": "全部下载后解压，解压密码：TUDO", "is_dir": True},
                    {"name": "archive.7z.001", "is_dir": False},
                ]

        value, source = module.discover_archive_password(MarkerAList(), "/archive")
        self.assertEqual(value, "TUDO")
        self.assertEqual(source, "sibling-marker")

    def test_archive_plan_includes_single_subtitle_zip_and_tree_password(self):
        module = self.load_tool("extract_archives_subtitle_module")
        source_root = "/src/show"
        archive_path = f"{source_root}/恶魔高校字幕.zip"
        archive_entry = {
            "name": "恶魔高校字幕.zip",
            "full_path": archive_path,
            "is_dir": False,
            "size": 730_000,
            "modified": "2026-01-01T00:00:00Z",
        }

        class SubtitleArchiveAList:
            def server_version(self):
                return "v3.62.0"

            def walk(self, path):
                return [dict(archive_entry)]

            def list(self, path, refresh=False):
                if path == source_root:
                    return [
                        dict(archive_entry),
                        {"name": "1", "is_dir": True},
                    ]
                if path == f"{source_root}/1":
                    return [
                        {
                            "name": "全部下载后解压，解压密码：TUDO",
                            "is_dir": True,
                        }
                    ]
                return []

            def archive_meta(self, path, archive_password="", refresh=True):
                self.received_password = archive_password
                return {
                    "content": [
                        {
                            "name": "字幕",
                            "is_dir": True,
                            "children": [
                                {"name": "s1", "is_dir": True, "children": []}
                            ],
                        }
                    ]
                }

        alist = SubtitleArchiveAList()
        plan, passwords = module.build_archive_plan(
            alist, source_root, explicit_archive_password=None
        )
        archive = plan["archives"][0]
        self.assertEqual(archive["archive_path"], archive_path)
        self.assertEqual(archive["video_count"], 0)
        self.assertEqual(archive["password_source"], "source-tree-marker")
        self.assertEqual(passwords[archive_path], "TUDO")
        self.assertEqual(alist.received_password, "TUDO")
        self.assertNotIn("TUDO", json.dumps(plan, ensure_ascii=False))

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

    def test_local_extraction_rejects_archive_bomb_and_insufficient_disk(self):
        module = self.load_tool("extract_archives_budget_module")
        archive = {
            "archive_path": "/src/bomb.7z",
            "members": [{"path": "video.mkv", "is_dir": False, "size": 201}],
        }
        with tempfile.TemporaryDirectory() as temporary, mock.patch.object(
            module, "MAX_LOCAL_EXPANSION_RATIO", 2
        ):
            with self.assertRaisesRegex(scraper.ScraperError, "展开比"):
                module._validate_local_extraction_budget(
                    archive, Path(temporary), compressed_bytes=100
                )

        safe_ratio = {
            "archive_path": "/src/large.7z",
            "members": [{"path": "video.mkv", "is_dir": False, "size": 100}],
        }
        disk = type("Disk", (), {"free": 200})()
        with tempfile.TemporaryDirectory() as temporary, mock.patch.object(
            module, "LOCAL_DISK_RESERVE_BYTES", 100
        ), mock.patch.object(module.shutil, "disk_usage", return_value=disk):
            with self.assertRaisesRegex(scraper.ScraperError, "空间不足"):
                module._validate_local_extraction_budget(
                    safe_ratio, Path(temporary), compressed_bytes=100
                )

    def test_local_extraction_rejects_links_before_invoking_extract(self):
        module = self.load_tool("extract_archives_link_metadata_module")
        listing = (
            b"Path = archive.7z\nType = 7z\n\n----------\n"
            b"Path = link\nSize = 0\nAttributes = A lrwxrwxrwx\n"
        )
        with mock.patch.object(
            module.subprocess,
            "run",
            return_value=mock.Mock(returncode=0, stdout=listing),
        ) as run_mock, self.assertRaisesRegex(scraper.ScraperError, "符号链接"):
            module._reject_local_archive_links("/usr/bin/7z", Path("archive.7z"))
        self.assertEqual(run_mock.call_args.args[0][1], "l")

    def test_archive_member_snapshot_is_stable_across_provider_order(self):
        module = self.load_tool("extract_archives_stable_member_order_module")
        first = [
            {"name": "b.ass", "is_dir": False, "size": 20},
            {"name": "A.ass", "is_dir": False, "size": 10},
        ]
        second = list(reversed(first))

        self.assertEqual(
            module._flatten_members(first),
            module._flatten_members(second),
        )

    def test_subtitle_archive_digest_ignores_only_discarded_font_mojibake(self):
        module = self.load_tool("extract_archives_font_mojibake_digest_module")
        first = [
            {"path": "Show.E01.ass", "is_dir": False, "size": 100},
            {"path": "字体/乱码.ttf", "is_dir": False, "size": 200},
            {"path": "字体", "is_dir": True, "size": 0},
        ]
        second = [
            {"path": "Show.E01.ass", "is_dir": False, "size": 100},
            {"path": "Fonts/另一种乱码.ttf", "is_dir": False, "size": 200},
            {"path": "Fonts", "is_dir": True, "size": 0},
            {"path": "Fonts/乱码子目录", "is_dir": True, "size": 0},
        ]
        changed_subtitle = [dict(item) for item in second]
        changed_subtitle[0]["path"] = "Show.E02.ass"

        self.assertEqual(module._members_digest(first), module._members_digest(second))
        self.assertNotEqual(
            module._members_digest(first),
            module._members_digest(changed_subtitle),
        )

    def test_subtitle_archive_is_uploaded_with_explicit_mime_and_retained(self):
        module = self.load_tool("extract_archives_typed_subtitle_module")
        archive = {
            "archive_path": "/src/show/subtitles.7z",
            "src_dir": "/src/show",
            "dst_dir": "/src/show",
            "parts": [{"name": "subtitles.7z"}],
            "members": [
                {"path": "subs", "is_dir": True, "size": 0},
                {"path": "subs/Show.S01E01.ass", "is_dir": False, "size": 3},
            ],
        }

        class TypedSubtitleAList:
            def __init__(self):
                self.directories = {"/", "/src", "/src/show"}
                self.files = {"/src/show/subtitles.7z": (b"archive", "application/octet-stream")}
                self.archive_meta_calls = 0

            def archive_meta(self, path, **kwargs):
                self.archive_meta_calls += 1
                return {"raw_url": "http://alist:5244/ae", "sign": "test-sign"}

            def list(self, path, refresh=False):
                entries = []
                prefix = path.rstrip("/") + "/"
                for directory in self.directories:
                    if directory != path and directory.startswith(prefix) and "/" not in directory[len(prefix):]:
                        entries.append({"name": directory.rsplit("/", 1)[-1], "is_dir": True})
                for file_path, (data, _mime) in self.files.items():
                    if file_path.startswith(prefix) and "/" not in file_path[len(prefix):]:
                        entries.append({"name": file_path.rsplit("/", 1)[-1], "is_dir": False, "size": len(data)})
                return entries

            def mkdir(self, path):
                self.directories.add(path)

            def archive_member_bytes(self, archive_path, inner_path, **kwargs):
                self.assertions = (archive_path, inner_path, kwargs)
                return b"sub"

            def upload_bytes(self, target_path, data, content_type):
                self.files[target_path] = (data, content_type)

            def remove(self, parent, names):
                for name in names:
                    self.files.pop(f"{parent}/{name}")

        alist = TypedSubtitleAList()
        self.install_exact_upload_api(alist, store_mime=True)
        self.assertTrue(module._is_direct_subtitle_archive(archive))
        with tempfile.TemporaryDirectory() as temporary:
            module._extract_subtitles_with_explicit_types(
                alist,
                archive,
                archive_password="secret",
                upload_context=self.upload_context(module, Path(temporary)),
            )
        self.assertEqual(
            alist.files["/src/show/subs/Show.S01E01.ass"],
            (b"sub", "text/x-ssa"),
        )
        self.assertEqual(alist.archive_meta_calls, 1)
        self.assertEqual(
            alist.assertions[2]["archive_metadata"]["sign"],
            "test-sign",
        )
        retained = module._retained_archive_paths(archive)
        self.assertEqual(retained, ["/src/show/subtitles.7z"])
        self.assertIn("/src/show/subtitles.7z", alist.files)

    def test_stable_ass_member_is_accepted_when_alist_reports_stale_size(self):
        module = self.load_tool("extract_archives_stale_subtitle_size_module")
        payload = b"[Script Info]\nTitle: Example\n[Events]\nDialogue: 0,0:00:00.00,0:00:01.00,Default,,0,0,0,,ok\n"
        archive = {
            "archive_path": "/src/show/subtitles.7z",
            "src_dir": "/src/show",
            "dst_dir": "/src/show",
            "parts": [{"name": "subtitles.7z"}],
            "members": [
                {"path": "Show.S01E01.ass", "is_dir": False, "size": len(payload) + 9},
            ],
        }

        class StaleSizeAList:
            def __init__(self):
                self.files = {}
                self.reads = 0
                self.directories = {"/", "/src", "/src/show"}

            def archive_meta(self, path, **kwargs):
                return {"raw_url": "http://alist:5244/ae", "sign": "test-sign"}

            def list(self, path, refresh=False):
                entries = [
                    {"name": target.rsplit("/", 1)[-1], "is_dir": False, "size": len(data)}
                    for target, data in self.files.items()
                    if target.rsplit("/", 1)[0] == path
                ]
                prefix = path.rstrip("/") + "/"
                entries.extend(
                    {"name": directory.rsplit("/", 1)[-1], "is_dir": True}
                    for directory in self.directories
                    if directory != path
                    and directory.startswith(prefix)
                    and "/" not in directory[len(prefix):]
                )
                return entries

            def mkdir(self, path):
                self.directories.add(path)

            def archive_member_bytes(self, archive_path, inner_path, **kwargs):
                self.reads += 1
                return payload

            def upload_bytes(self, target_path, data, content_type):
                self.files[target_path] = data

        alist = StaleSizeAList()
        self.install_exact_upload_api(alist)
        with tempfile.TemporaryDirectory() as temporary:
            module._extract_subtitles_with_explicit_types(
                alist,
                archive,
                archive_password="",
                upload_context=self.upload_context(module, Path(temporary)),
            )
        self.assertEqual(alist.reads, 2)
        self.assertEqual(archive["members"][0]["size"], len(payload))
        self.assertEqual(alist.files["/src/show/Show.S01E01.ass"], payload)

    def test_unencrypted_subtitle_archive_is_extracted_locally_once(self):
        module = self.load_tool("extract_archives_local_subtitle_module")
        archive_bytes = b"7z-test-archive"
        subtitle = b"[Script Info]\nTitle: Example\n[Events]\nDialogue: 0,0:00:00.00,0:00:01.00,Default,,0,0,0,,ok\n"
        archive = {
            "archive_path": "/src/show/subtitles.7z",
            "src_dir": "/src/show",
            "dst_dir": "/src/show",
            "name": "subtitles.7z",
            "parts": [
                {
                    "name": "subtitles.7z",
                    "path": "/src/show/subtitles.7z",
                    "size": len(archive_bytes),
                }
            ],
            "members": [
                {"path": "subs", "is_dir": True, "size": 0},
                {"path": "subs/Show.S01E01.ass", "is_dir": False, "size": 1},
                {"path": "乱码字体/旧解码.ttf", "is_dir": False, "size": 10},
            ],
            "deferred_inspection": False,
        }

        class LocalSubtitleAList:
            def __init__(self):
                self.directories = {"/", "/src", "/src/show"}
                self.files = {}
                self.downloads = 0

            def read_file_bytes(self, path, *, max_bytes):
                self.downloads += 1
                self.assertions = (path, max_bytes)
                return archive_bytes

            def list(self, path, refresh=False):
                prefix = path.rstrip("/") + "/"
                rows = [
                    {"name": target.rsplit("/", 1)[-1], "is_dir": False, "size": len(data)}
                    for target, data in self.files.items()
                    if target.rsplit("/", 1)[0] == path
                ]
                rows.extend(
                    {"name": directory.rsplit("/", 1)[-1], "is_dir": True}
                    for directory in self.directories
                    if directory != path
                    and directory.startswith(prefix)
                    and "/" not in directory[len(prefix):]
                )
                return rows

            def mkdir(self, path):
                self.directories.add(path)

            def upload_bytes(self, target_path, data, content_type):
                self.files[target_path] = data

        def run_7z(command, **kwargs):
            if command[1] == "l":
                return mock.Mock(
                    returncode=0,
                    stdout=(
                        b"Path = subtitles.7z\nType = 7z\n\n----------\n"
                        b"Path = subs/Show.S01E01.ass\nSize = 1\n"
                        b"Folder = -\nAttributes = A -rw-r--r--\n"
                    ),
                )
            output = Path(next(value[2:] for value in command if value.startswith("-o")))
            target = output / "subs" / "Show.S01E01.ass"
            target.parent.mkdir(parents=True)
            target.write_bytes(subtitle)
            font = output / "Fonts" / "new-decoding.ttf"
            font.parent.mkdir(parents=True)
            font.write_bytes(b"font-bytes")
            return mock.Mock(returncode=0)

        alist = LocalSubtitleAList()
        self.install_exact_upload_api(alist)
        with tempfile.TemporaryDirectory() as temporary, mock.patch.object(
            module.shutil, "which", return_value="/usr/bin/7z"
        ), mock.patch.object(module.subprocess, "run", side_effect=run_7z) as run_mock:
            self.assertTrue(
                module._extract_subtitles_locally(
                    alist,
                    archive,
                    archive_password="",
                    upload_context=self.upload_context(module, Path(temporary)),
                )
            )
        self.assertEqual(alist.downloads, 1)
        self.assertEqual(run_mock.call_count, 2)
        self.assertEqual(archive["members"][1]["size"], len(subtitle))
        self.assertEqual(alist.files["/src/show/subs/Show.S01E01.ass"], subtitle)
        self.assertNotIn("/src/show/乱码字体", alist.directories)
        self.assertFalse(any(path.endswith(".ttf") for path in alist.files))

    def test_encrypted_subtitle_archive_keeps_alist_fallback(self):
        module = self.load_tool("extract_archives_encrypted_subtitle_module")
        with mock.patch.object(module.shutil, "which", return_value="/usr/bin/7z"):
            self.assertFalse(
                module._extract_subtitles_locally(
                    {"unused": True},
                    {"archive_path": "/src/show/subtitles.7z"},
                    archive_password="secret",
                )
            )

    def test_disguised_media_archive_uses_streaming_local_7z_fallback(self):
        module = self.load_tool("extract_archives_local_media_module")
        archive_bytes = b"PK-test"
        video = b"video-payload"
        archive = {
            "archive_path": "/src/.scraper-tmp-test.zip",
            "src_dir": "/src",
            "dst_dir": "/src",
            "name": ".scraper-tmp-test.zip",
            "parts": [{
                "name": "release.exe",
                "path": "/src/release.exe",
                "size": len(archive_bytes),
            }],
            "members": [],
            "deferred_inspection": True,
            "_local_fallback_required": True,
        }

        class StreamingAList:
            def __init__(self):
                self.files = {}
                self.directories = {"/", "/src"}
                self.downloaded = []

            def download_file_to_path(self, path, destination, *, expected_size):
                self.downloaded.append((path, expected_size))
                destination.write_bytes(archive_bytes)

            def list(self, path, refresh=False):
                return [
                    {"name": target.rsplit("/", 1)[-1], "is_dir": False, "size": len(data)}
                    for target, data in self.files.items()
                    if target.rsplit("/", 1)[0] == path
                ]

            def mkdir(self, path):
                self.directories.add(path)

            def upload_file(self, target, source, content_type):
                self.files[target] = source.read_bytes()

        def run_7z(command, **kwargs):
            if command[1] == "l":
                return mock.Mock(
                    returncode=0,
                    stdout=(
                        b"Path = archive.7z\nType = 7z\n\n----------\n"
                        b"Path = Show.S01E01.mkv\n"
                        + f"Size = {len(video)}\n".encode()
                        + b"Folder = -\nAttributes = A -rw-r--r--\n"
                    ),
                )
            output = Path(next(value[2:] for value in command if value.startswith("-o")))
            (output / "Show.S01E01.mkv").write_bytes(video)
            return mock.Mock(returncode=0)

        alist = StreamingAList()
        self.install_exact_upload_api(alist)
        with tempfile.TemporaryDirectory() as temporary, mock.patch.object(
            module.shutil, "which", return_value="/usr/bin/7z"
        ), mock.patch.object(module.subprocess, "run", side_effect=run_7z) as run_mock:
            self.assertTrue(
                module._extract_deferred_media_locally(
                    alist,
                    archive,
                    archive_password="correct-secret",
                    upload_context=self.upload_context(module, Path(temporary)),
                )
            )
        self.assertEqual(
            alist.downloaded,
            [("/src/.scraper-tmp-test.zip", len(archive_bytes))],
        )
        self.assertEqual(alist.files["/src/Show.S01E01.mkv"], video)
        self.assertEqual(archive["members"][0]["path"], "Show.S01E01.mkv")
        listing_command = next(
            call.args[0] for call in run_mock.mock_calls
            if call.args[0][1] == "l"
        )
        self.assertFalse(any(value.startswith("-p") for value in listing_command))
        listing_call = next(
            call for call in run_mock.mock_calls if call.args[0][1] == "l"
        )
        self.assertEqual(listing_call.kwargs["input"], b"correct-secret\n")

    def test_member_checkpoint_local_resume_uploads_only_missing_member(self):
        module = self.load_tool("extract_archives_local_member_resume_module")
        archive_bytes = b"archive"
        first_video = b"one"
        second_video = b"two!"
        members = [
            {"path": "Show.S01E01.mkv", "is_dir": False, "size": len(first_video)},
            {"path": "Show.S01E02.mkv", "is_dir": False, "size": len(second_video)},
        ]
        archive = {
            "archive_path": "/src/show.7z",
            "src_dir": "/src",
            "dst_dir": "/src",
            "name": "show.7z",
            "parts": [{
                "name": "show.7z",
                "path": "/src/show.7z",
                "size": len(archive_bytes),
            }],
            "members": members,
            "members_sha256": module._members_digest(members),
        }

        class ResumeAList:
            def __init__(self):
                self.files = {"/src/Show.S01E01.mkv": first_video}
                self.uploaded = []

            def download_file_to_path(self, path, destination, *, expected_size):
                self.assertions = (path, expected_size)
                destination.write_bytes(archive_bytes)

            def list(self, path, refresh=False):
                return [
                    {
                        "name": target.rsplit("/", 1)[-1],
                        "is_dir": False,
                        "size": len(data),
                    }
                    for target, data in self.files.items()
                    if target.rsplit("/", 1)[0] == path
                ]

            def mkdir(self, path):
                return None

            def upload_file(self, target, source, content_type):
                self.uploaded.append(target)
                self.files[target] = source.read_bytes()

        def run_7z(command, **kwargs):
            if command[1] == "l":
                return mock.Mock(
                    returncode=0,
                    stdout=(
                        b"Path = show.7z\nType = 7z\n\n----------\n"
                        b"Path = Show.S01E01.mkv\nSize = 3\n"
                        b"Folder = -\nAttributes = A -rw-r--r--\n\n"
                        b"Path = Show.S01E02.mkv\nSize = 4\n"
                        b"Folder = -\nAttributes = A -rw-r--r--\n"
                    ),
                )
            output = Path(next(value[2:] for value in command if value.startswith("-o")))
            (output / "Show.S01E01.mkv").write_bytes(first_video)
            (output / "Show.S01E02.mkv").write_bytes(second_video)
            return mock.Mock(returncode=0)

        alist = ResumeAList()
        self.install_exact_upload_api(alist)
        with tempfile.TemporaryDirectory() as temporary, mock.patch.object(
            module.shutil, "which", return_value="/usr/bin/7z"
        ), mock.patch.object(module.subprocess, "run", side_effect=run_7z):
            self.assertTrue(module._extract_deferred_media_locally(
                alist,
                archive,
                archive_password="",
                upload_context=self.upload_context(module, Path(temporary)),
                resume_checkpoint=True,
            ))
        self.assertEqual(alist.assertions, ("/src/show.7z", len(archive_bytes)))
        self.assertEqual(alist.uploaded, ["/src/Show.S01E02.mkv"])
        self.assertEqual(alist.files["/src/Show.S01E01.mkv"], first_video)
        self.assertEqual(alist.files["/src/Show.S01E02.mkv"], second_video)

    def test_archive_execution_resumes_completed_checkpoints(self):
        module = self.load_tool("extract_archives_resume_module")
        archives = [
            {
                "archive_path": f"/src/{name}",
                "src_dir": "/src",
                "dst_dir": "/src",
                "name": name,
                "parts": [{"name": name}],
            }
            for name in ("one.7z", "two.7z")
        ]
        plan = {"source_root": "/src", "media_renames": [], "archives": archives}
        digest = module.hashlib.sha256(module._canonical_json_bytes(plan)).hexdigest()

        class ResumeAList:
            def __init__(self):
                self.lock_name = None
                self.lock_payload = None

            def server_version(self):
                return "v3.57.0"

            def upload_bytes(self, path, data, content_type):
                self.lock_name = path.rsplit("/", 1)[-1]
                self.lock_payload = data

            def read_file_bytes(self, path, *, max_bytes):
                return self.lock_payload

            def list(self, path, refresh=False):
                if self.lock_name:
                    return [{"name": self.lock_name, "is_dir": False}]
                return []

            def remove(self, parent, names):
                self.lock_name = None

        with tempfile.TemporaryDirectory() as temporary:
            journal_path = Path(temporary) / "archive-journal.json"
            journal_path.write_text(json.dumps({
                "created_at": "2026-07-27T00:00:00+00:00",
                "plan_sha256": digest,
                "status": "failed",
                "locks": [],
                "tasks": [{"state": 4}],
                "renames": [],
                "archive_removals": [],
                "retained_archives": ["/src/one.7z", "/src/two.7z"],
                "error": "interrupted",
            }), encoding="utf-8")
            module.execute_archive_plan(
                ResumeAList(),
                plan,
                {"/src/one.7z": "", "/src/two.7z": ""},
                timeout=1,
                journal_path=journal_path,
            )
            journal = json.loads(journal_path.read_text(encoding="utf-8"))
        self.assertEqual(journal["status"], "success")
        self.assertEqual(len(journal["retained_archives"]), 2)
        self.assertEqual(journal["task_history"], [[{"state": 4}]])

    def test_archive_execution_resumes_only_missing_members_after_alist_restart(self):
        module = self.load_tool("extract_archives_member_resume_module")
        members = [
            {"path": "Show.S01E01.mkv", "is_dir": False, "size": 3},
            {"path": "Show.S01E02.mkv", "is_dir": False, "size": 4},
        ]
        archive = {
            "archive_path": "/src/show.7z",
            "src_dir": "/src",
            "dst_dir": "/src",
            "name": "show.7z",
            "parts": [{
                "name": "show.7z", "path": "/src/show.7z", "size": 7,
            }],
            "members": members,
            "members_sha256": module._members_digest(members),
        }
        plan = {"source_root": "/src", "media_renames": [], "archives": [archive]}
        digest = module.hashlib.sha256(module._canonical_json_bytes(plan)).hexdigest()

        class RestartedAList:
            def __init__(self):
                self.files = {
                    "show.7z": 7,
                    "Show.S01E01.mkv": 3,
                }
                self.lock_name = None
                self.lock_payload = None

            def server_version(self):
                return "v3.57.0"

            def archive_tasks(self, kind, done=False):
                return []

            def archive_meta(self, path, archive_password="", refresh=True):
                return {"content": [
                    {
                        "name": row["path"],
                        "is_dir": row["is_dir"],
                        "size": row["size"],
                    }
                    for row in members
                ]}

            def list(self, path, refresh=False):
                if path != "/src":
                    return []
                rows = [
                    {"name": name, "is_dir": False, "size": size}
                    for name, size in self.files.items()
                ]
                if self.lock_name:
                    rows.append({"name": self.lock_name, "is_dir": False, "size": 1})
                return rows

            def try_list(self, path, refresh=False):
                return self.list(path, refresh=refresh)

            def upload_bytes(self, path, data, content_type):
                self.lock_name = path.rsplit("/", 1)[-1]
                self.lock_payload = data

            def read_file_bytes(self, path, *, max_bytes):
                return self.lock_payload

            def remove(self, parent, names):
                if self.lock_name in names:
                    self.lock_name = None

        alist = RestartedAList()
        checkpoint = {
            "archive_path": "/src/show.7z",
            "dst_dir": "/src",
            "members_sha256": archive["members_sha256"],
            "expected": [
                {"path": "Show.S01E01.mkv", "size": 3},
                {"path": "Show.S01E02.mkv", "size": 4},
            ],
            "verified": [{"path": "Show.S01E01.mkv", "size": 3}],
            "remaining": ["Show.S01E02.mkv"],
        }
        with tempfile.TemporaryDirectory() as temporary:
            journal_path = Path(temporary) / "archive-journal.json"
            journal_path.write_text(json.dumps({
                "created_at": "2026-07-27T00:00:00+00:00",
                "plan_sha256": digest,
                "status": "running",
                "locks": [],
                "tasks": [{"id": "lost-after-restart", "state": 1}],
                "renames": [],
                "archive_removals": [],
                "retained_archives": [],
                "archive_member_checkpoints": {"/src/show.7z": checkpoint},
            }), encoding="utf-8")

            def resume_missing(
                _alist,
                _archive,
                *,
                archive_password,
                upload_context,
                resume_checkpoint,
            ):
                self.assertIs(_alist, alist)
                self.assertEqual(archive_password, "")
                self.assertIsNotNone(upload_context)
                self.assertTrue(resume_checkpoint)
                self.assertEqual(_alist.files["Show.S01E01.mkv"], 3)
                _alist.files["Show.S01E02.mkv"] = 4
                return True

            with mock.patch.object(
                module,
                "_extract_deferred_media_locally",
                side_effect=resume_missing,
            ) as resume_mock:
                module.execute_archive_plan(
                    alist,
                    plan,
                    {"/src/show.7z": ""},
                    timeout=1,
                    journal_path=journal_path,
                )
            journal = json.loads(journal_path.read_text(encoding="utf-8"))

        resume_mock.assert_called_once()
        self.assertEqual(journal["status"], "success")
        self.assertEqual(journal["retained_archives"], ["/src/show.7z"])
        final_checkpoint = journal["archive_member_checkpoints"]["/src/show.7z"]
        self.assertEqual(final_checkpoint["remaining"], [])
        self.assertEqual(
            [row["path"] for row in final_checkpoint["verified"]],
            ["Show.S01E01.mkv", "Show.S01E02.mkv"],
        )

    def test_archive_member_resume_rejects_unrecorded_existing_member(self):
        module = self.load_tool("extract_archives_member_collision_module")
        members = [
            {"path": "Show.S01E01.mkv", "is_dir": False, "size": 3},
            {"path": "Show.S01E02.mkv", "is_dir": False, "size": 4},
        ]
        archive = {
            "archive_path": "/src/show.7z",
            "dst_dir": "/src",
            "members": members,
            "members_sha256": module._members_digest(members),
        }

        class CollisionAList:
            def try_list(self, path, refresh=False):
                return [
                    {"name": "Show.S01E01.mkv", "is_dir": False, "size": 3},
                    {"name": "Show.S01E02.mkv", "is_dir": False, "size": 4},
                ]

        with tempfile.TemporaryDirectory() as temporary:
            journal_path = Path(temporary) / "archive-journal.json"
            journal = {"archive_member_checkpoints": {"/src/show.7z": {
                "archive_path": "/src/show.7z",
                "dst_dir": "/src",
                "members_sha256": archive["members_sha256"],
                "verified": [{"path": "Show.S01E01.mkv", "size": 3}],
            }}}
            with self.assertRaisesRegex(scraper.ScraperError, "未登记"):
                module._resume_archive_member_checkpoint(
                    CollisionAList(),
                    archive,
                    journal=journal,
                    journal_path=journal_path,
                )

    def test_archive_member_resume_rejects_corrupt_recorded_member_identity(self):
        module = self.load_tool("extract_archives_member_corrupt_checkpoint_module")
        members = [
            {"path": "Show.S01E01.mkv", "is_dir": False, "size": 3},
        ]
        archive = {
            "archive_path": "/src/show.7z",
            "dst_dir": "/src",
            "members": members,
            "members_sha256": module._members_digest(members),
        }

        class MatchingAList:
            def try_list(self, path, refresh=False):
                return [{
                    "name": "Show.S01E01.mkv", "is_dir": False, "size": 3,
                }]

        with tempfile.TemporaryDirectory() as temporary:
            journal_path = Path(temporary) / "archive-journal.json"
            journal = {"archive_member_checkpoints": {"/src/show.7z": {
                "archive_path": "/src/show.7z",
                "dst_dir": "/src",
                "members_sha256": archive["members_sha256"],
                "verified": [{"path": "Show.S01E01.mkv", "size": 99}],
            }}}
            with self.assertRaisesRegex(scraper.ScraperError, "身份不一致"):
                module._resume_archive_member_checkpoint(
                    MatchingAList(),
                    archive,
                    journal=journal,
                    journal_path=journal_path,
                )

    def test_archive_resume_rejects_still_trackable_alist_task(self):
        module = self.load_tool("extract_archives_live_task_module")
        plan = {"source_root": "/src", "media_renames": [], "archives": []}
        digest = module.hashlib.sha256(module._canonical_json_bytes(plan)).hexdigest()

        class LiveTaskAList:
            def server_version(self):
                return "v3.57.0"

            def archive_tasks(self, kind, done=False):
                if kind == "decompress" and not done:
                    return [{"id": "still-running", "state": 1}]
                return []

        with tempfile.TemporaryDirectory() as temporary:
            journal_path = Path(temporary) / "archive-journal.json"
            journal_path.write_text(json.dumps({
                "plan_sha256": digest,
                "status": "running",
                "tasks": [{"id": "still-running", "state": 1}],
            }), encoding="utf-8")
            with self.assertRaisesRegex(scraper.ScraperError, "拒绝并发续跑"):
                module.execute_archive_plan(
                    LiveTaskAList(),
                    plan,
                    {},
                    timeout=1,
                    journal_path=journal_path,
                )

    def test_failed_archive_task_persists_partial_member_checkpoint(self):
        module = self.load_tool("extract_archives_partial_failure_module")
        members = [
            {"path": "Show.S01E01.mkv", "is_dir": False, "size": 3},
            {"path": "Show.S01E02.mkv", "is_dir": False, "size": 4},
        ]
        archive = {
            "archive_path": "/src/show.7z",
            "dst_dir": "/src",
            "members": members,
            "members_sha256": module._members_digest(members),
        }

        class FailedTaskAList:
            def archive_tasks(self, kind, done=False):
                if kind == "decompress" and not done:
                    return [{"id": "new", "state": 4, "error": "restart"}]
                return []

            def try_list(self, path, refresh=False):
                return [{"name": "Show.S01E01.mkv", "is_dir": False, "size": 3}]

        with tempfile.TemporaryDirectory() as temporary:
            journal_path = Path(temporary) / "archive-journal.json"
            journal = {"archive_member_checkpoints": {}}
            with self.assertRaisesRegex(scraper.ScraperError, "AList 解压任务失败"):
                module.wait_for_archive_tasks(
                    FailedTaskAList(),
                    {"decompress": set(), "decompress_upload": set()},
                    {"new"},
                    timeout=1,
                    journal=journal,
                    journal_path=journal_path,
                    archive=archive,
                )
            persisted = json.loads(journal_path.read_text(encoding="utf-8"))
        checkpoint = persisted["archive_member_checkpoints"]["/src/show.7z"]
        self.assertEqual(
            [row["path"] for row in checkpoint["verified"]],
            ["Show.S01E01.mkv"],
        )
        self.assertEqual(checkpoint["remaining"], ["Show.S01E02.mkv"])

    def test_provider_rejected_local_media_is_classified_without_deleting_archive(self):
        module = self.load_tool("extract_archives_provider_rejection_module")
        archive_bytes = b"archive"
        video = b"video"
        archive = {
            "archive_path": "/src/.scraper-tmp.7z",
            "src_dir": "/src",
            "dst_dir": "/src",
            "name": ".scraper-tmp.7z",
            "parts": [{"name": "episode.exe", "path": "/src/episode.exe", "size": len(archive_bytes)}],
            "members": [],
            "deferred_inspection": True,
            "_local_fallback_required": True,
        }

        class RejectingAList:
            def download_file_to_path(self, path, destination, *, expected_size):
                destination.write_bytes(archive_bytes)

            def list(self, path, refresh=False):
                return []

            def mkdir(self, path):
                return None

            def upload_file(self, target, source, content_type):
                raise scraper.ApiError("invalid file [非法文件不能上传]")

        def run_7z(command, **kwargs):
            if command[1] == "l":
                return mock.Mock(
                    returncode=0,
                    stdout=(
                        b"Path = archive.7z\nType = 7z\n\n----------\n"
                        b"Path = Show.S01E01.mkv\nSize = 5\n"
                        b"Folder = -\nAttributes = A -rw-r--r--\n"
                    ),
                )
            output = Path(next(value[2:] for value in command if value.startswith("-o")))
            (output / "Show.S01E01.mkv").write_bytes(video)
            return mock.Mock(returncode=0)

        alist = RejectingAList()
        alist.files = {}
        self.install_exact_upload_api(alist)
        with tempfile.TemporaryDirectory() as temporary, mock.patch.object(
            module.shutil, "which", return_value="/usr/bin/7z"
        ), mock.patch.object(
            module.subprocess, "run", side_effect=run_7z
        ), mock.patch.object(
            module.run_local_upload_transaction.__globals__["time"], "sleep"
        ), self.assertRaises(module.ArchiveOutputRejected):
            module._extract_deferred_media_locally(
                alist,
                archive,
                archive_password="secret",
                upload_context=self.upload_context(module, Path(temporary)),
            )

    def test_restored_temporary_archive_allows_only_modified_time_drift(self):
        module = self.load_tool("extract_archives_restored_snapshot_module")
        archive = {
            "src_dir": "/src",
            "parts": [{
                "name": "episode.exe",
                "path": "/src/episode.exe",
                "size": 123,
                "modified": "before",
                "hash": None,
            }],
        }

        class SnapshotAList:
            size = 123

            def list(self, path, refresh=False):
                return [{
                    "name": "episode.exe",
                    "is_dir": False,
                    "size": self.size,
                    "modified": "after",
                }]

        alist = SnapshotAList()
        with self.assertRaisesRegex(scraper.ScraperError, "modified"):
            module._verify_part_snapshots(alist, archive)
        module._verify_part_snapshots(
            alist, archive, allow_restored_modified_drift=True
        )
        alist.size = 124
        with self.assertRaisesRegex(scraper.ScraperError, "size"):
            module._verify_part_snapshots(
                alist, archive, allow_restored_modified_drift=True
            )

    def test_extracted_member_verification_waits_for_cloud_listing(self):
        module = self.load_tool("extract_archives_eventual_listing_module")
        archive = {
            "dst_dir": "/media/show",
            "members": [
                {"path": "Show.S01E01.ass", "is_dir": False, "size": 42},
            ],
        }

        class EventuallyConsistentAList:
            def __init__(self):
                self.calls = 0

            def list(self, path, refresh=False):
                self.calls += 1
                if self.calls == 1:
                    return []
                return [{"name": "Show.S01E01.ass", "is_dir": False, "size": 42}]

        alist = EventuallyConsistentAList()
        with mock.patch.object(module.time, "sleep") as sleep_mock:
            module._verify_extracted_members(alist, archive)
        self.assertEqual(alist.calls, 2)
        sleep_mock.assert_called_once_with(module.EXTRACT_VERIFY_DELAY_SECONDS)

    def test_unstable_member_is_rejected_when_alist_reports_stale_size(self):
        module = self.load_tool("extract_archives_unstable_subtitle_size_module")
        payloads = [
            f"[Script Info]\n[Events]\nDialogue: {index}\n".encode()
            for index in range(4)
        ]
        archive = {
            "archive_path": "/src/show/subtitles.7z",
            "src_dir": "/src/show",
            "dst_dir": "/src/show",
            "parts": [{"name": "subtitles.7z"}],
            "members": [{"path": "Show.ass", "is_dir": False, "size": 999}],
        }

        class UnstableAList:
            def __init__(self):
                self.directories = {"/", "/src", "/src/show"}

            def archive_meta(self, path, **kwargs):
                return {"raw_url": "http://alist:5244/ae", "sign": "test-sign"}

            def list(self, path, refresh=False):
                prefix = path.rstrip("/") + "/"
                return [
                    {"name": directory.rsplit("/", 1)[-1], "is_dir": True}
                    for directory in self.directories
                    if directory != path
                    and directory.startswith(prefix)
                    and "/" not in directory[len(prefix):]
                ]

            def mkdir(self, path):
                self.directories.add(path)

            def archive_member_bytes(self, archive_path, inner_path, **kwargs):
                return payloads.pop(0)

        with self.assertRaisesRegex(scraper.ScraperError, "归档字幕读取大小不匹配"):
            module._extract_subtitles_with_explicit_types(
                UnstableAList(), archive, archive_password=""
            )


if __name__ == "__main__":
    unittest.main()
