from __future__ import annotations

from datetime import date
from pathlib import Path
import unittest

from engine.scraper import plan_sha256
from engine.scrapeflow.one_time_tv_exclusion_scope import (
    seal_exact_tv_exclusion_scope_from_predecessor_batch,
)
from local.scrapeflow_api.title_closure import (
    TitleClosureAdapters,
    build_title_closure_evidence,
)
from local.scrapeflow_api.title_closure_runtime import (
    make_burned_in_ocr_adapter,
    make_current_title_episode_gap_scanner,
    make_current_tv_exclusion_episode_gap_scanner,
)


TV_ROOT = "/quark/影视/番剧/Example (2026)"
MOVIE_ROOT = "/quark/影视/电影/Movie (2025)"


def target(media_type: str = "tv") -> dict:
    return {
        "media_type": media_type,
        "target_root": TV_ROOT if media_type == "tv" else MOVIE_ROOT,
        "category": "番剧" if media_type == "tv" else "电影",
        "tmdb_id": 42 if media_type == "tv" else 84,
        "title": "Example" if media_type == "tv" else "Movie",
    }


def envelope(root: str, *, projects: list[dict], movies: list[dict]) -> dict:
    return {
        "schema_version": 1,
        "library_root": root,
        "included_roots": [root],
        "excluded_roots": [],
        "nfo_parse_errors": [],
        "duplicate_tmdb_ids": {},
        "uncovered_media": [],
        "projects": projects,
        "movies": movies,
    }


def tv_project() -> dict:
    return {
        "title": "Example",
        "official_title": "Example Official",
        "original_title": "Example Original",
        "target_root": TV_ROOT,
        "tmdb_ids": [42],
        "video_files": 2,
        "regular_missing": [{
            "season": 1,
            "episode": 2,
            "label": "S01E02",
            "title": "Second",
            "season_name": "Season 1",
            "expected_episode_count": 2,
        }],
        "optional_missing": [{
            "season": 0,
            "episode": 5,
            "label": "S00E05",
            "title": "Special",
            "season_name": "Specials",
            "expected_episode_count": 1,
        }],
        "issues": [],
    }


def movie_row() -> dict:
    stem = f"{MOVIE_ROOT}/Movie (2025)"
    return {
        "title": "Movie",
        "official_title": "Movie Official",
        "target_stem": stem,
        "tmdb_ids": [84],
        "video_files": [f"{stem}.mkv"],
        "missing_subtitles": [],
        "issues": [],
    }


class CurrentTitleEpisodeScannerTests(unittest.TestCase):
    def test_signed_target_exclusions_filter_nested_work_for_normal_scanner(self) -> None:
        nested = TV_ROOT + "/Independent Anime"

        class AList:
            def walk(self, _root, **_kwargs):
                return [
                    {"full_path": TV_ROOT + "/Season 01/Example - S01E01.mkv"},
                    {"full_path": nested + "/Season 01/Anime - S01E01.mkv"},
                ]

        def fake_scan(scoped_alist, _tmdb, root, **kwargs):
            self.assertEqual(
                [row["full_path"] for row in scoped_alist.walk(root)],
                [TV_ROOT + "/Season 01/Example - S01E01.mkv"],
            )
            self.assertEqual(kwargs["excluded_roots"], [nested])
            result = envelope(root, projects=[tv_project()], movies=[])
            result["excluded_roots"] = [nested]
            return result

        scanner = make_current_title_episode_gap_scanner(
            AList(), object(), library_scanner=fake_scan,
        )
        signed_target = {**target(), "excluded_roots": [nested]}
        gaps = scanner(signed_target)
        self.assertEqual([row["label"] for row in gaps], ["S00E05", "S01E02"])

    def test_tv_exclusion_episode_scan_filters_direct_movie_before_library_audit(self) -> None:
        nested_stem = TV_ROOT + "/Nested Movie (2025)"
        identity = {
            "status": "exact", "tmdb_id": 42,
            "media_type": "tv", "title": "Example",
        }
        scope = seal_exact_tv_exclusion_scope_from_predecessor_batch({
            "target_root": TV_ROOT, "category": "番剧",
            "identity": {**identity, "identity_sha256": "ignored"},
            "title_work_key": "c" * 64,
            "read_only_audit_allowed": False,
            "scope_blockers": [{
                "reason": "nested_title_identity",
                "target_roots": [nested_stem],
            }],
        })

        class AList:
            def walk(self, _root, **_kwargs):
                return [
                    {"full_path": TV_ROOT + "/Season 01/Example - S01E01.mkv"},
                    {"full_path": nested_stem + ".mkv"},
                    {"full_path": nested_stem + ".nfo"},
                    {"full_path": nested_stem + ".zh-CN.ass"},
                ]

        def fake_scan(scoped_alist, _tmdb, root, **kwargs):
            visible = scoped_alist.walk(root, refresh=True)
            self.assertEqual(
                [row["full_path"] for row in visible],
                [TV_ROOT + "/Season 01/Example - S01E01.mkv"],
            )
            self.assertEqual(kwargs["excluded_roots"], [nested_stem])
            result = envelope(root, projects=[tv_project()], movies=[])
            result["excluded_roots"] = [nested_stem]
            return result

        scanner = make_current_tv_exclusion_episode_gap_scanner(
            AList(), object(), scope, library_scanner=fake_scan,
        )
        gaps = scanner(target())
        self.assertEqual([row["label"] for row in gaps], ["S00E05", "S01E02"])

    def test_tv_scan_is_exact_root_and_returns_regular_and_s00_with_identity(self) -> None:
        calls: list[tuple] = []

        def fake_scan(alist, tmdb, root, **kwargs):
            calls.append((alist, tmdb, root, kwargs))
            return envelope(root, projects=[tv_project()], movies=[])

        alist = object()
        tmdb = object()
        scanner = make_current_title_episode_gap_scanner(
            alist,
            tmdb,
            today=date(2026, 8, 3),
            library_scanner=fake_scan,
        )
        gaps = scanner(target())

        self.assertEqual(len(calls), 1)
        self.assertIs(calls[0][0], alist)
        self.assertIs(calls[0][1], tmdb)
        self.assertEqual(calls[0][2], TV_ROOT)
        self.assertEqual(calls[0][3], {
            "today": date(2026, 8, 3),
            "excluded_roots": [],
            "required_subtitle_languages": ("zh-CN",),
        })
        self.assertEqual(
            [(gap["lane"], gap["label"]) for gap in gaps],
            [("s00", "S00E05"), ("regular", "S01E02")],
        )
        self.assertTrue(all(gap["kind"] == "missing_episode" for gap in gaps))
        self.assertTrue(all(gap["target_root"] == TV_ROOT for gap in gaps))
        self.assertTrue(all(gap["media"] == {
            "media_type": "tv",
            "tmdb_id": 42,
            "title": "Example Official",
            "original_title": "Example Original",
            "target_root": TV_ROOT,
            "category": "番剧",
        } for gap in gaps))

    def test_tv_scan_attaches_only_official_aliases_from_exact_tmdb_id(self) -> None:
        class TMDB:
            def get(self, path):
                self_path = path
                if self_path != "/tv/42/alternative_titles":
                    raise AssertionError(self_path)
                return {"results": [
                    {"iso_3166_1": "BR", "title": "Example: Starting Life"},
                    {"iso_3166_1": "GB", "title": "Example Starting Life"},
                    {"iso_3166_1": "US", "title": "  Example Official EN  "},
                    {"name": "ignored"},
                ]}

        scanner = make_current_title_episode_gap_scanner(
            object(), TMDB(),
            library_scanner=lambda _alist, _tmdb, root, **_kwargs: envelope(
                root, projects=[tv_project()], movies=[],
            ),
        )
        gaps = scanner(target())
        self.assertTrue(all(gap["media"]["aliases"] == [
            "Example Starting Life", "Example Official EN",
        ] for gap in gaps))

    def test_tv_scan_attaches_exact_multilingual_s00_episode_titles(self) -> None:
        class TMDB:
            def get(self, path, **params):
                if path == "/tv/42/alternative_titles":
                    return {"results": []}
                if path != "/tv/42/season/0":
                    raise AssertionError(path)
                names = {
                    "ja-JP": "Re:ゼロから始める休憩時間 特別編",
                    "en-US": "Re:Zero Break Time Special",
                }
                return {"episodes": [{
                    "episode_number": 5,
                    "name": names[params["language"]],
                }]}

        scanner = make_current_title_episode_gap_scanner(
            object(), TMDB(),
            library_scanner=lambda _alist, _tmdb, root, **_kwargs: envelope(
                root, projects=[tv_project()], movies=[],
            ),
        )
        gaps = scanner(target())
        by_label = {gap["label"]: gap for gap in gaps}
        self.assertEqual(by_label["S00E05"]["title_aliases"], [
            "Re:ゼロから始める休憩時間 特別編",
            "Re:Zero Break Time Special",
        ])
        self.assertNotIn("title_aliases", by_label["S01E02"])

    def test_tv_scan_attaches_verified_release_local_episode_aliases(self) -> None:
        class TMDB:
            def get(self, path, **params):
                if path == "/tv/42/alternative_titles":
                    return {"results": []}
                if path != "/tv/42/season/0":
                    raise AssertionError(path)
                language = params["language"]
                return {"episodes": [
                    {
                        "episode_number": number,
                        "name": (
                            f"Re:ゼロから始める休憩時間 3rd season 第{number}話"
                            if language == "ja-JP"
                            else f"Re:Zero - Starting Break Time from Zero: Episode {number}"
                        ),
                    }
                    for number in range(51, 55)
                ]}

        project = tv_project()
        project["optional_missing"] = [{
            "season": 0,
            "episode": 51,
            "label": "S00E51",
            "title": "沉睡鬼的枕边夜话",
            "season_name": "Specials",
            "expected_episode_count": 70,
        }]
        scanner = make_current_title_episode_gap_scanner(
            object(), TMDB(),
            library_scanner=lambda _alist, _tmdb, root, **_kwargs: envelope(
                root, projects=[project], movies=[],
            ),
        )
        gaps = scanner(target())
        by_label = {gap["label"]: gap for gap in gaps}
        self.assertEqual(by_label["S00E51"]["source_episode_aliases"], [{
            "season": 3,
            "episode": 1,
            "series_titles": [
                "Re:ゼロから始める休憩時間",
                "Re:Zero - Starting Break Time from Zero",
            ],
        }])

    def test_tv_rejects_wrong_root_tmdb_extra_project_and_unsafe_structure(self) -> None:
        current = target()
        cases = []
        wrong_root = tv_project()
        wrong_root["target_root"] = f"{TV_ROOT}/Other"
        cases.append(envelope(TV_ROOT, projects=[wrong_root], movies=[]))
        wrong_tmdb = tv_project()
        wrong_tmdb["tmdb_ids"] = [999]
        cases.append(envelope(TV_ROOT, projects=[wrong_tmdb], movies=[]))
        cases.append(envelope(
            TV_ROOT,
            projects=[tv_project(), {**tv_project(), "target_root": f"{TV_ROOT}/Child"}],
            movies=[],
        ))
        unsafe = tv_project()
        unsafe["issues"] = [{"code": "unnumbered_video_in_season_directory"}]
        cases.append(envelope(TV_ROOT, projects=[unsafe], movies=[]))

        for audit in cases:
            with self.subTest(audit=audit):
                scanner = make_current_title_episode_gap_scanner(
                    object(),
                    object(),
                    library_scanner=(
                        lambda *_args, audit=audit, **_kwargs: audit
                    ),
                )
                with self.assertRaises(ValueError):
                    scanner(current)

    def test_target_category_root_is_rejected_without_calling_scan_library(self) -> None:
        calls: list[str] = []
        scanner = make_current_title_episode_gap_scanner(
            object(),
            object(),
            library_scanner=lambda *_args, **_kwargs: calls.append("scan"),
        )
        invalid = {
            **target(),
            "target_root": "/quark/影视/番剧",
        }
        with self.assertRaises(ValueError):
            scanner(invalid)
        self.assertEqual(calls, [])

    def test_movie_validates_single_nfo_video_tmdb_identity_and_has_no_episode_gap(self) -> None:
        scanner = make_current_title_episode_gap_scanner(
            object(),
            object(),
            library_scanner=lambda _alist, _tmdb, root, **_kwargs: envelope(
                root, projects=[], movies=[movie_row()],
            ),
        )
        self.assertEqual(scanner(target("movie")), [])

        for mutate in ("tmdb", "video", "year"):
            row = movie_row()
            if mutate == "tmdb":
                row["tmdb_ids"] = [999]
            elif mutate == "video":
                row["video_files"] = []
            else:
                row["issues"] = [{"code": "movie_year_tmdb_mismatch"}]
            broken = make_current_title_episode_gap_scanner(
                object(),
                object(),
                library_scanner=lambda _a, _t, root, row=row, **_k: envelope(
                    root, projects=[], movies=[row],
                ),
            )
            with self.subTest(mutate=mutate), self.assertRaises(ValueError):
                broken(target("movie"))


class LazyOcrAdapterTests(unittest.TestCase):
    def test_factory_loads_engine_only_on_first_video_and_reuses_it(self) -> None:
        engines: list[object] = []
        calls: list[tuple] = []

        def engine_factory():
            engine = object()
            engines.append(engine)
            return engine

        def probe(alist, engine, path, **kwargs):
            calls.append((alist, engine, path, kwargs))
            return {"status": "pending", "reason": "test"}

        evidence_root = Path("/tmp/title-closure-test-evidence")
        adapter = make_burned_in_ocr_adapter(
            timeout=45,
            evidence_root=evidence_root,
            engine_factory=engine_factory,
            probe_video=probe,
        )
        self.assertEqual(engines, [])
        alist = object()
        adapter(alist, f"{TV_ROOT}/Season 01/S01E01.mkv")
        adapter(alist, f"{TV_ROOT}/Season 01/S01E02.mkv")
        self.assertEqual(len(engines), 1)
        self.assertEqual([call[2] for call in calls], [
            f"{TV_ROOT}/Season 01/S01E01.mkv",
            f"{TV_ROOT}/Season 01/S01E02.mkv",
        ])
        self.assertTrue(all(call[1] is engines[0] for call in calls))
        self.assertTrue(all(call[3] == {
            "timeout": 45,
            "evidence_root": evidence_root,
        } for call in calls))

    def test_probe_exception_is_converted_to_pending_by_title_closure(self) -> None:
        video = f"{TV_ROOT}/Season 01/S01E01.mkv"
        plan = {
            "mode": "tv",
            "source_root": "/quark/影视/待刮削/Example",
            "target_root": TV_ROOT,
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
        row = {
            "media_type": "tv",
            "title": "Example",
            "target_root": TV_ROOT,
            "season": 1,
            "episode": 1,
            "label": "S01E01",
            "video_path": video,
            "status": "gap",
            "scope": "external_sidecar",
            "reason_code": "missing_external_subtitle",
            "required_languages": ["zh-CN"],
            "companion_subtitles": [],
            "candidate_subtitles": [],
            "candidate_languages": [],
            "remediation_action": "acquire_or_verify_embedded_subtitle",
            "embedded_subtitle_status": "not_inspectable_from_alist_inventory",
        }
        inventory = {
            "schema_version": 1,
            "library_root": TV_ROOT,
            "excluded_roots": [],
            "included_roots": [TV_ROOT],
            "subtitle_policy": {"required_languages": ["zh-CN"]},
            "missing_subtitles": [row],
            "subtitle_inventory": [row],
        }

        def raise_probe(*_args, **_kwargs):
            raise RuntimeError("host_ocr_failed")

        ocr = make_burned_in_ocr_adapter(
            engine_factory=lambda: object(),
            probe_video=raise_probe,
        )
        evidence = build_title_closure_evidence(
            plan,
            plan_sha256(plan),
            alist=object(),
            pause_active=lambda: False,
            adapters=TitleClosureAdapters(
                scan_episode_gaps=lambda _target: [],
                scan_inventory=lambda _alist, _root: inventory,
                probe_embedded=lambda _alist, _path: {
                    "status": "probe_failed", "error": "timeout",
                },
                probe_burned_in_ocr=ocr,
            ),
            audited_at="2026-08-03T00:00:00+00:00",
        )
        self.assertEqual(
            evidence["summary"]["pending_subtitle_verification_count"], 1,
        )
        self.assertEqual(
            evidence["probe_evidence"]["burned_in_ocr"][video],
            {"status": "pending", "reason": "RuntimeError"},
        )


if __name__ == "__main__":
    unittest.main()
