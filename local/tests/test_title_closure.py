from __future__ import annotations

import copy
import unittest

from engine.scraper import plan_sha256
from engine.scrapeflow.one_time_movie_member_scope import (
    seal_movie_member_candidate,
    validate_exact_movie_member_scope,
)
from engine.scrapeflow.one_time_tv_exclusion_scope import (
    seal_exact_tv_exclusion_scope_from_predecessor_batch,
)
from local.scrapeflow_api.title_closure import (
    TitleClosureAdapters,
    TitleClosureBlocked,
    build_exact_movie_member_closure_evidence,
    build_exact_title_closure_evidence,
    build_exact_tv_exclusion_closure_evidence,
    build_title_closure_evidence,
    extract_signed_title_targets,
    title_closure_evidence_is_valid,
)
from local.scrapeflow_api.validation import canonical_digest


TV_ROOT = "/quark/影视/番剧/Example (2026)"
SOURCE_ROOT = "/quark/影视/待刮削/Example"


def tv_plan(*, target_root: str = TV_ROOT) -> dict:
    return {
        "mode": "tv",
        "source_root": SOURCE_ROOT,
        "target_root": target_root,
        "warnings": [],
        "metadata": {
            "tmdb_id": 42,
            "title": "Example",
            "year": "2026",
            "season": 1,
            "absolute": False,
        },
        "files": [],
        "notices": [],
        "decision_trace": {},
        "scan_report": {},
    }


def batch_plan(*, child_root: str | None = None) -> dict:
    first = f"/quark/影视/番剧/Family/First (2025)"
    second = child_root or f"/quark/影视/美剧/Second (2024)"
    movie = "/quark/影视/电影/Movie (2023)"
    return {
        "mode": "batch",
        "source_root": SOURCE_ROOT,
        "target_root": "/quark/影视/番剧/Family",
        "warnings": [],
        "metadata": {
            "title": "Family",
            "member_tv": {
                first: {
                    "tmdb_id": 1,
                    "title": "First",
                    "original_title": "First",
                    "year": "2025",
                    "poster_path": None,
                    "backdrop_path": None,
                },
                second: {
                    "tmdb_id": 2,
                    "title": "Second",
                    "original_title": "Second",
                    "year": "2024",
                    "poster_path": None,
                    "backdrop_path": None,
                },
            },
            "member_movies": {
                movie: {"tmdb_id": 3, "title": "Movie", "year": "2023"},
            },
            "member_posters": {},
        },
        "files": [],
        "notices": [],
        "decision_trace": {},
        "scan_report": {},
    }


def inventory(root: str, rows: list[dict]) -> dict:
    missing = [dict(row) for row in rows if row.get("status") == "gap"]
    return {
        "schema_version": 1,
        "audited_at": "2026-08-03T00:00:00+00:00",
        "library_root": root,
        "excluded_roots": [],
        "included_roots": [root],
        "subtitle_policy": {
            "scope": "external_sidecar",
            "required_languages": ["zh-CN"],
            "embedded_subtitle_status": "not_inspectable_from_alist_inventory",
        },
        "missing_subtitles": missing,
        "subtitle_inventory": rows,
    }


def gap_row(
    video_name: str,
    *,
    companions: list[str] | None = None,
    reason: str = "missing_external_subtitle",
) -> dict:
    video = f"{TV_ROOT}/Season 01/{video_name}"
    return {
        "media_type": "tv",
        "title": "Example (2026)",
        "target_root": TV_ROOT,
        "season": 1,
        "episode": int(video_name.split("E", 1)[1].split(".", 1)[0]),
        "label": video_name.split(".", 1)[0],
        "video_path": video,
        "status": "gap",
        "scope": "external_sidecar",
        "reason_code": reason,
        "required_languages": ["zh-CN"],
        "companion_subtitles": companions or [],
        "candidate_subtitles": companions or [],
        "candidate_languages": ["und"] if companions else [],
        "remediation_action": "acquire_or_verify_embedded_subtitle",
        "embedded_subtitle_status": "not_inspectable_from_alist_inventory",
    }


def chinese_ass() -> bytes:
    return (
        "Dialogue: 0,0:00:01.00,0:00:02.00,Default,,0,0,0,,这是一个测试字幕我们现在开始\n"
        "Dialogue: 0,0:00:03.00,0:00:04.00,Default,,0,0,0,,他们说这件事情已经处理完毕\n"
        "Dialogue: 0,0:00:05.00,0:00:06.00,Default,,0,0,0,,你可以继续看下去了没有问题\n"
    ).encode("utf-8")


def positive_raw_ocr_windows() -> list[dict]:
    def line(text: str) -> dict:
        return {
            "text": text,
            "confidence": 92,
            "left": 400,
            "top": 800,
            "width": 800,
            "height": 80,
        }

    return [
        {
            "offset_seconds": 300,
            "frames": [{
                "status": "ocr_success",
                "width": 1920,
                "height": 1080,
                "ocr_lines": [line(text)],
            } for text in ("我们现在开始", "我们现在开始", "下一句话来了")],
        },
        {
            "offset_seconds": 900,
            "frames": [{
                "status": "ocr_success",
                "width": 1920,
                "height": 1080,
                "ocr_lines": [line(text)],
            } for text in ("这是今天的字幕", "这是今天的字幕", "继续前进吧")],
        },
    ]


class SignedTargetTests(unittest.TestCase):
    def test_extracts_exact_tv_and_batch_member_roots(self) -> None:
        plan = tv_plan()
        self.assertEqual(
            extract_signed_title_targets(plan, plan_sha256(plan)),
            [{
                "media_type": "tv",
                "target_root": TV_ROOT,
                "category": "番剧",
                "tmdb_id": 42,
                "title": "Example",
            }],
        )

        batch = batch_plan()
        targets = extract_signed_title_targets(batch, plan_sha256(batch))
        self.assertEqual(len(targets), 3)
        self.assertEqual(
            {row["target_root"] for row in targets},
            {
                "/quark/影视/番剧/Family/First (2025)",
                "/quark/影视/美剧/Second (2024)",
                "/quark/影视/电影/Movie (2023)",
            },
        )

    def test_movie_mixed_and_collection_modes_use_work_roots_not_parent_root(self) -> None:
        movie_root = "/quark/影视/电影/Only Movie (2026)"
        movie = {
            **tv_plan(target_root=movie_root),
            "mode": "movie",
            "metadata": {
                "tmdb_id": 90,
                "title": "Only Movie",
                "year": "2026",
            },
        }
        self.assertEqual(
            extract_signed_title_targets(movie, plan_sha256(movie))[0]["target_root"],
            movie_root,
        )

        series_root = "/quark/影视/番剧/Family/Series (2020)"
        mixed = {
            **tv_plan(target_root="/quark/影视/番剧/Family"),
            "mode": "mixed",
            "metadata": {
                "tmdb_id": 91,
                "title": "Series",
                "year": "2020",
                "season": 1,
                "absolute": False,
                "series_root": series_root,
                "member_movies": {},
            },
        }
        self.assertEqual(
            extract_signed_title_targets(mixed, plan_sha256(mixed))[0]["target_root"],
            series_root,
        )

        first = "/quark/影视/电影/Collection/First (2020)"
        second = "/quark/影视/电影/Collection/Second (2022)"
        collection = {
            "mode": "collection",
            "source_root": SOURCE_ROOT,
            "target_root": "/quark/影视/电影/Collection",
            "warnings": [],
            "metadata": {
                "tmdb_id": 92,
                "title": "Collection",
                "member_movies": {
                    first: {"tmdb_id": 93, "title": "First", "year": "2020"},
                    second: {"tmdb_id": 94, "title": "Second", "year": "2022"},
                },
            },
            "files": [],
            "notices": [],
            "decision_trace": {},
            "scan_report": {},
        }
        self.assertEqual(
            {
                row["target_root"]
                for row in extract_signed_title_targets(
                    collection, plan_sha256(collection),
                )
            },
            {first, second},
        )

    def test_rejects_digest_nonformal_category_root_and_invalid_overlap(self) -> None:
        plan = tv_plan()
        with self.assertRaisesRegex(ValueError, "digest"):
            extract_signed_title_targets(plan, "0" * 64)

        for invalid_root in ("/tmp/Example", "/quark/影视/番剧"):
            invalid = tv_plan(target_root=invalid_root)
            with self.assertRaises(ValueError):
                extract_signed_title_targets(invalid, plan_sha256(invalid))

        overlapping = batch_plan()
        movie_parent = "/quark/影视/电影/Movie (2023)"
        overlapping["metadata"]["member_movies"][f"{movie_parent}/Child"] = {
            "tmdb_id": 4, "title": "Child", "year": "2024",
        }
        with self.assertRaisesRegex(ValueError, "非 TV"):
            extract_signed_title_targets(overlapping, plan_sha256(overlapping))

    def test_canonical_nested_work_goldens_have_exact_parent_exclusions(self) -> None:
        def signed_batch(
            target_root: str,
            tv_rows: dict[str, tuple[int, str]],
            movie_rows: dict[str, tuple[int, str]] | None = None,
        ) -> list[dict]:
            plan = {
                "mode": "batch",
                "source_root": SOURCE_ROOT,
                "target_root": target_root,
                "warnings": [],
                "metadata": {
                    "title": target_root.rsplit("/", 1)[-1],
                    "member_tv": {
                        root: {
                            "tmdb_id": identity,
                            "title": title,
                            "original_title": title,
                            "year": "2026",
                            "poster_path": None,
                            "backdrop_path": None,
                        }
                        for root, (identity, title) in tv_rows.items()
                    },
                    "member_movies": {
                        root: {"tmdb_id": identity, "title": title, "year": "2026"}
                        for root, (identity, title) in (movie_rows or {}).items()
                    },
                    "member_posters": {},
                },
                "files": [],
                "notices": [],
                "decision_trace": {},
                "scan_report": {},
            }
            return extract_signed_title_targets(plan, plan_sha256(plan))

        cases = [
            (
                "/quark/影视/番剧/瑞克和莫蒂",
                {
                    "/quark/影视/番剧/瑞克和莫蒂": (60625, "瑞克和莫蒂"),
                    "/quark/影视/番剧/瑞克和莫蒂/瑞克和莫蒂：日漫版": (250923, "瑞克和莫蒂：日漫版"),
                },
                {},
            ),
            (
                "/quark/影视/番剧/White Album",
                {
                    "/quark/影视/番剧/White Album": (1, "White Album"),
                    "/quark/影视/番剧/White Album/White Album 2": (2, "White Album 2"),
                },
                {},
            ),
        ]
        for root, tv_rows, movie_rows in cases:
            with self.subTest(root=root):
                targets = signed_batch(root, tv_rows, movie_rows)
                by_root = {row["target_root"]: row for row in targets}
                child = next(path for path in tv_rows if path != root)
                self.assertEqual(by_root[root]["excluded_roots"], [child])
                self.assertNotIn("excluded_roots", by_root[child])
                self.assertNotEqual(by_root[root]["tmdb_id"], by_root[child]["tmdb_id"])

        fate = "/quark/影视/番剧/Fate"
        illya = f"{fate}/魔法少女☆伊莉雅"
        snow = f"{illya}/魔法少女☆伊莉雅：雪下的誓言 (2017)"
        fate_targets = signed_batch(
            fate,
            {
                f"{fate}/命运之夜": (463, "命运之夜"),
                illya: (64375, "魔法少女☆伊莉雅"),
            },
            {snow: (374475, "魔法少女☆伊莉雅：雪下的誓言")},
        )
        fate_by_root = {row["target_root"]: row for row in fate_targets}
        self.assertNotIn(fate, fate_by_root)  # pure franchise container
        self.assertEqual(fate_by_root[illya]["excluded_roots"], [snow])
        self.assertEqual(fate_by_root[snow]["tmdb_id"], 374475)

    def test_signed_parent_closure_routes_nested_leaf_through_exclusion_scanner(self) -> None:
        root = "/quark/影视/番剧/瑞克和莫蒂"
        child = f"{root}/瑞克和莫蒂：日漫版"
        plan = batch_plan(child_root=child)
        first = next(iter(plan["metadata"]["member_tv"]))
        plan["metadata"]["member_tv"] = {
            root: plan["metadata"]["member_tv"].pop(first),
            child: plan["metadata"]["member_tv"].pop(child),
        }
        inventory_calls: list[tuple[str, list[str]]] = []
        gap_targets: list[dict] = []

        def empty_inventory(root_value: str, exclusions: list[str]) -> dict:
            payload = inventory(root_value, [])
            payload["excluded_roots"] = exclusions
            return payload

        adapters = TitleClosureAdapters(
            scan_episode_gaps=lambda target: gap_targets.append(dict(target)) or [],
            scan_inventory=lambda _alist, root_value: empty_inventory(root_value, []),
            scan_nested_root_inventory=lambda _alist, root_value, exclusions: (
                inventory_calls.append((root_value, list(exclusions)))
                or empty_inventory(root_value, list(exclusions))
            ),
        )
        result = build_title_closure_evidence(
            plan, plan_sha256(plan), alist=object(), pause_active=lambda: False,
            adapters=adapters, audited_at="2026-08-04T00:00:00+00:00",
        )
        self.assertEqual(inventory_calls, [(root, [child])])
        self.assertEqual(
            next(row for row in gap_targets if row["target_root"] == root)["excluded_roots"],
            [child],
        )
        self.assertEqual({row["tmdb_id"] for row in gap_targets}, {1, 2, 3})
        self.assertTrue(result["summary"]["complete"])
        self.assertTrue(title_closure_evidence_is_valid(result))


class ClosureEvidenceTests(unittest.TestCase):
    def tv_exclusion_scope(self) -> dict:
        identity = {
            "status": "exact", "tmdb_id": 42,
            "media_type": "tv", "title": "Example",
        }
        return seal_exact_tv_exclusion_scope_from_predecessor_batch({
            "target_root": TV_ROOT, "category": "番剧",
            "identity": {
                **identity, "identity_sha256": canonical_digest(identity),
            },
            "title_work_key": "b" * 64,
            "read_only_audit_allowed": False,
            "scope_blockers": [{
                "reason": "nested_title_identity",
                "target_roots": [TV_ROOT + "/Movie (2025)"],
            }],
        })

    def run_closure(
        self,
        rows: list[dict],
        *,
        gaps: list[dict] | None = None,
        read_external=lambda _alist, _path: b"",
        probe=lambda _alist, _path: {"status": "no_subtitle_stream", "streams": []},
        extract=lambda _alist, _path, _index: {"status": "undetermined"},
        ocr=None,
        pause=lambda: False,
    ) -> dict:
        plan = tv_plan()
        adapters = TitleClosureAdapters(
            scan_episode_gaps=lambda _target: list(gaps or []),
            scan_inventory=lambda _alist, root: inventory(root, rows),
            read_external_prefix=read_external,
            probe_embedded=probe,
            extract_embedded_text=extract,
            probe_burned_in_ocr=ocr,
        )
        return build_title_closure_evidence(
            plan,
            plan_sha256(plan),
            alist=object(),
            pause_active=pause,
            adapters=adapters,
            audited_at="2026-08-03T00:00:00+00:00",
        )

    def test_external_content_can_complete_and_evidence_is_self_digested(self) -> None:
        subtitle = f"{TV_ROOT}/Season 01/Example - S01E01.ass"
        row = gap_row(
            "S01E01.mkv",
            companions=[subtitle],
            reason="subtitle_language_unverified",
        )
        probe_calls: list[str] = []
        result = self.run_closure(
            [row],
            read_external=lambda _alist, path: chinese_ass(),
            probe=lambda _alist, path: probe_calls.append(path),
        )
        self.assertEqual(probe_calls, [])
        self.assertEqual(result["summary"], {
            "episode_gap_count": 0,
            "confirmed_subtitle_gap_count": 0,
            "pending_subtitle_verification_count": 0,
            "complete": True,
        })
        self.assertTrue(title_closure_evidence_is_valid(result))
        tampered = copy.deepcopy(result)
        tampered["summary"]["complete"] = False
        self.assertFalse(title_closure_evidence_is_valid(tampered))

    def test_title_extra_does_not_keep_subtitle_closure_open(self) -> None:
        row = gap_row("S01E01.mkv")
        row.update({
            "media_type": "extra",
            "video_path": f"{TV_ROOT}/trailer.mkv",
            "season": None,
            "episode": None,
            "label": None,
        })
        probes: list[str] = []
        result = self.run_closure(
            [row],
            probe=lambda _alist, path: probes.append(path),
        )

        self.assertEqual(probes, [])
        self.assertTrue(result["summary"]["complete"])
        self.assertEqual(
            result["subtitle_inventories"][TV_ROOT]["missing_subtitles"],
            [row],
        )

    def test_one_time_exact_scope_needs_no_media_plan_or_journal(self) -> None:
        target = {
            "media_type": "tv", "target_root": TV_ROOT, "category": "番剧",
            "tmdb_id": 42, "title": "Example",
        }
        adapters = TitleClosureAdapters(
            scan_episode_gaps=lambda value: [] if value == target else [
                {"kind": "invalid"},
            ],
            scan_inventory=lambda _alist, root: inventory(root, []),
        )
        result = build_exact_title_closure_evidence(
            [target], canonical_digest([target]), alist=object(),
            pause_active=lambda: False, adapters=adapters,
            audited_at="2026-08-03T00:00:00+00:00",
        )
        self.assertEqual(result["source_scope_kind"], "one_time_exact_title_scope")
        self.assertTrue(result["summary"]["complete"])
        self.assertTrue(title_closure_evidence_is_valid(result))

        with self.assertRaisesRegex(ValueError, "digest"):
            build_exact_title_closure_evidence(
                [target], "0" * 64, alist=object(), pause_active=lambda: False,
                adapters=adapters,
            )

    def test_one_time_movie_member_scope_reuses_closure_without_directory_scan(self) -> None:
        stem = "/quark/影视/电影/Shared/Movie A (2026)"
        candidate = seal_movie_member_candidate({
            "category": "电影", "target_stem": stem,
            "parent_root": "/quark/影视/电影/Shared",
            "video_path": stem + ".mkv", "nfo_path": stem + ".nfo",
            "tmdb_id": 99, "title": "Movie A",
        })
        member_paths = [stem + ".mkv", stem + ".nfo", stem + ".zh-CN.ass"]
        scope = validate_exact_movie_member_scope({
            "schema_version": 1,
            "kind": "one_time_exact_movie_member_scope",
            **{key: candidate[key] for key in (
                "category", "target_stem", "parent_root", "video_path",
                "nfo_path", "tmdb_id", "title", "candidate_sha256",
            )},
            "member_paths": member_paths,
            "member_paths_sha256": canonical_digest(member_paths),
        })
        target = {
            "media_type": "movie", "target_root": stem, "category": "电影",
            "tmdb_id": 99, "title": "Movie A",
        }
        member_scans: list[dict] = []

        def scan_member(_alist, value: dict) -> dict:
            member_scans.append(dict(value))
            row = {
                "media_type": "movie", "title": "Movie A",
                "target_root": stem, "video_path": stem + ".mkv",
                "status": "external_required_language_present",
                "required_languages": ["zh-CN"],
                "companion_subtitles": [stem + ".zh-CN.ass"],
                "candidate_subtitles": [stem + ".zh-CN.ass"],
                "candidate_languages": ["zh-CN"],
            }
            return {
                **inventory(stem, [row]),
                "member_paths": member_paths,
                "member_paths_sha256": scope["member_paths_sha256"],
            }

        adapters = TitleClosureAdapters(
            scan_episode_gaps=lambda _target: self.fail("movie must not run TV gap scanner"),
            scan_inventory=lambda _alist, _root: self.fail("shared parent must not be scanned as title"),
            scan_movie_member_inventory=scan_member,
        )
        result = build_exact_movie_member_closure_evidence(
            [scope], canonical_digest([scope]), alist=object(),
            pause_active=lambda: False, adapters=adapters,
            audited_at="2026-08-03T00:00:00+00:00",
        )
        self.assertEqual(member_scans, [scope])
        self.assertEqual(result["source_scope_kind"], "one_time_exact_movie_member_scope")
        self.assertEqual(result["title_targets"], [target])
        self.assertEqual(result["movie_member_scopes"], [scope])
        self.assertTrue(result["summary"]["complete"])
        self.assertTrue(title_closure_evidence_is_valid(result))

        tampered = copy.deepcopy(result)
        tampered["movie_member_scopes"][0]["member_paths"].append(
            "/quark/影视/电影/Shared/Movie B (2025).zh-CN.ass",
        )
        core = {key: value for key, value in tampered.items() if key != "evidence_sha256"}
        tampered["evidence_sha256"] = canonical_digest(core)
        self.assertFalse(title_closure_evidence_is_valid(tampered))

    def test_movie_member_builder_rejects_overlapping_members(self) -> None:
        stem = "/quark/影视/电影/Movie (2026)"
        candidate = seal_movie_member_candidate({
            "category": "电影", "target_stem": stem,
            "parent_root": "/quark/影视/电影",
            "video_path": stem + ".mkv", "nfo_path": stem + ".nfo",
            "tmdb_id": 99, "title": "Movie",
        })
        paths = [stem + ".mkv", stem + ".nfo"]
        scope = validate_exact_movie_member_scope({
            "schema_version": 1, "kind": "one_time_exact_movie_member_scope",
            **{key: candidate[key] for key in (
                "category", "target_stem", "parent_root", "video_path",
                "nfo_path", "tmdb_id", "title", "candidate_sha256",
            )},
            "member_paths": paths, "member_paths_sha256": canonical_digest(paths),
        })
        adapters = TitleClosureAdapters(scan_episode_gaps=lambda _target: [])
        with self.assertRaisesRegex(ValueError, "重叠"):
            build_exact_movie_member_closure_evidence(
                [scope, scope], canonical_digest([scope, scope]),
                alist=object(), pause_active=lambda: False, adapters=adapters,
            )

    def test_tv_exclusion_scope_is_bound_into_closure_evidence(self) -> None:
        scope = self.tv_exclusion_scope()
        target = {
            "media_type": "tv", "target_root": TV_ROOT,
            "category": "番剧", "tmdb_id": 42, "title": "Example",
        }
        row = {
            **gap_row("S01E01.mkv"),
            "status": "external_required_language_present",
            "companion_subtitles": [], "candidate_subtitles": [],
            "candidate_languages": [],
        }

        def scoped_inventory(_alist, value: dict) -> dict:
            self.assertEqual(value, scope)
            result = inventory(TV_ROOT, [row])
            result["excluded_roots"] = scope["excluded_roots"]
            return result

        adapters = TitleClosureAdapters(
            scan_episode_gaps=lambda value: [] if value == target else self.fail("wrong target"),
            scan_inventory=lambda _alist, _root: self.fail("ordinary scan must stay unused"),
            scan_tv_exclusion_inventory=scoped_inventory,
        )
        evidence = build_exact_tv_exclusion_closure_evidence(
            [scope], canonical_digest([scope]), alist=object(),
            pause_active=lambda: False, adapters=adapters,
            audited_at="2026-08-03T00:00:00+00:00",
        )
        self.assertEqual(
            evidence["source_scope_kind"],
            "one_time_exact_tv_root_with_nested_exclusions",
        )
        self.assertEqual(evidence["tv_exclusion_scopes"], [scope])
        self.assertTrue(title_closure_evidence_is_valid(evidence))
        tampered = copy.deepcopy(evidence)
        tampered["tv_exclusion_scopes"][0]["excluded_roots"].append(
            TV_ROOT + "/Other",
        )
        core = {key: value for key, value in tampered.items() if key != "evidence_sha256"}
        tampered["evidence_sha256"] = canonical_digest(core)
        self.assertFalse(title_closure_evidence_is_valid(tampered))

    def test_tv_exclusion_inventory_cannot_leak_direct_movie_member(self) -> None:
        scope = self.tv_exclusion_scope()
        nested_video = TV_ROOT + "/Movie (2025).mkv"
        leaked = {
            "media_type": "movie", "title": "Movie",
            "target_root": TV_ROOT, "video_path": nested_video,
            "status": "external_required_language_present",
            "required_languages": ["zh-CN"],
            "companion_subtitles": [], "candidate_subtitles": [],
            "candidate_languages": [],
        }
        raw = inventory(TV_ROOT, [leaked])
        raw["excluded_roots"] = scope["excluded_roots"]
        adapters = TitleClosureAdapters(
            scan_episode_gaps=lambda _target: self.fail("leak must fail before episode scan"),
            scan_tv_exclusion_inventory=lambda _alist, _scope: raw,
        )
        with self.assertRaisesRegex(ValueError, "泄漏"):
            build_exact_tv_exclusion_closure_evidence(
                [scope], canonical_digest([scope]), alist=object(),
                pause_active=lambda: False, adapters=adapters,
            )

    def test_unknown_text_stream_is_extracted_before_ocr(self) -> None:
        row = gap_row("S01E01.mkv")
        extracted: list[tuple[str, int]] = []
        ocr_calls: list[str] = []

        def extract(_alist, path: str, index: int) -> dict:
            extracted.append((path, index))
            return {
                "status": "chinese",
                "language_variant": "simplified_chinese",
            }

        result = self.run_closure(
            [row],
            probe=lambda _alist, _path: {
                "status": "subtitle_stream_language_unknown",
                "streams": [{
                    "index": 2,
                    "codec_name": "ass",
                    "classification": "unknown",
                }],
            },
            extract=extract,
            ocr=lambda _alist, path: ocr_calls.append(path),
        )
        self.assertEqual(extracted, [(row["video_path"], 2)])
        self.assertEqual(ocr_calls, [])
        self.assertTrue(result["summary"]["complete"])

    def test_ocr_runs_only_for_still_pending_video(self) -> None:
        confirmed = gap_row("S01E01.mkv")
        pending = gap_row("S01E02.mkv")
        ocr_calls: list[str] = []

        def probe(_alist, path: str) -> dict:
            if path == confirmed["video_path"]:
                return {"status": "no_subtitle_stream", "streams": []}
            return {"status": "probe_failed", "error": "timeout"}

        def ocr(_alist, path: str) -> dict:
            ocr_calls.append(path)
            return {"raw_ocr_windows": positive_raw_ocr_windows()}

        result = self.run_closure([confirmed, pending], probe=probe, ocr=ocr)
        self.assertEqual(ocr_calls, [pending["video_path"]])
        self.assertEqual(result["summary"], {
            "episode_gap_count": 0,
            "confirmed_subtitle_gap_count": 1,
            "pending_subtitle_verification_count": 0,
            "complete": False,
        })

    def test_episode_gap_is_part_of_the_same_completion_gate(self) -> None:
        result = self.run_closure([], gaps=[{
            "kind": "missing_episode",
            "label": "S01E02",
        }])
        self.assertEqual(result["summary"]["episode_gap_count"], 1)
        self.assertFalse(result["summary"]["complete"])
        self.assertTrue(title_closure_evidence_is_valid(result))

    def test_inventory_cannot_smuggle_an_out_of_scope_subtitle_read(self) -> None:
        outside = "/quark/影视/番剧/Other/Season 01/Other.ass"
        row = gap_row(
            "S01E01.mkv",
            companions=[outside],
            reason="subtitle_language_unverified",
        )
        reads: list[str] = []
        with self.assertRaisesRegex(ValueError, "超出作品范围"):
            self.run_closure(
                [row],
                read_external=lambda _alist, path: reads.append(path) or b"",
            )
        self.assertEqual(reads, [])

    def test_pause_activated_during_scan_fails_closed_before_later_stages(self) -> None:
        state = {"paused": False}
        episode_calls: list[dict] = []

        def scan(_alist, root: str) -> dict:
            state["paused"] = True
            return inventory(root, [])

        plan = tv_plan()
        adapters = TitleClosureAdapters(
            scan_episode_gaps=lambda target: episode_calls.append(dict(target)),
            scan_inventory=scan,
        )
        with self.assertRaisesRegex(TitleClosureBlocked, "after_subtitle_inventory"):
            build_title_closure_evidence(
                plan,
                plan_sha256(plan),
                alist=object(),
                pause_active=lambda: state["paused"],
                adapters=adapters,
            )
        self.assertEqual(episode_calls, [])

    def test_malformed_pause_reader_fails_closed_without_scan(self) -> None:
        scans: list[str] = []
        plan = tv_plan()
        adapters = TitleClosureAdapters(
            scan_episode_gaps=lambda _target: [],
            scan_inventory=lambda _alist, root: scans.append(root),
        )
        with self.assertRaisesRegex(TitleClosureBlocked, "pause_state_invalid"):
            build_title_closure_evidence(
                plan,
                plan_sha256(plan),
                alist=object(),
                pause_active=lambda: 0,  # type: ignore[return-value]
                adapters=adapters,
            )
        self.assertEqual(scans, [])


if __name__ == "__main__":
    unittest.main()
