from datetime import date
import os
import socket
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from engine.tools import audit_live_library
from engine.tools.audit_live_library import (
    SUBTITLE_EXTS,
    _parallel_map_with_serial_retry, analyze_series, companion_stem,
    default_excluded_roots, empty_library_roots, episode_numbers,
    external_subtitle_gap,
    formal_media_roots,
    parse_movie_nfo, parse_tvshow_nfo,
    scan_library, scan_subtitle_inventory,
    temporary_tmdb_dns_override,
)


class AuditLiveLibraryTests(unittest.TestCase):
    def test_matroska_subtitle_container_is_part_of_library_inventory(self):
        self.assertIn(".mks", SUBTITLE_EXTS)

    def test_tmdb_dns_override_scopes_official_host_and_restores_resolver(self):
        original = socket.getaddrinfo
        calls = []

        def fake_resolver(host, port, family=0, type=0, proto=0, flags=0):
            calls.append((host, port))
            return [(family, type, proto, "", (host, port))]

        with mock.patch.dict(os.environ, {
            "TMDB_BASE_URL": "https://api.themoviedb.org/3",
        }), mock.patch("socket.getaddrinfo", side_effect=fake_resolver) as resolver:
            patched_original = socket.getaddrinfo
            with temporary_tmdb_dns_override("13.224.245.63"):
                socket.getaddrinfo("api.themoviedb.org", 443)
                socket.getaddrinfo("example.com", 443)
            self.assertIs(socket.getaddrinfo, patched_original)
            self.assertEqual(calls, [("13.224.245.63", 443), ("example.com", 443)])
            self.assertEqual(resolver.call_count, 2)
        self.assertIs(socket.getaddrinfo, original)

    def test_tmdb_dns_override_rejects_nonofficial_endpoint_and_invalid_ip(self):
        with mock.patch.dict(os.environ, {
            "TMDB_BASE_URL": "https://tmdb-proxy.example/3",
        }):
            with self.assertRaisesRegex(ValueError, "官方 TMDB"):
                with temporary_tmdb_dns_override("13.224.245.63"):
                    pass
        with self.assertRaises(ValueError):
            with temporary_tmdb_dns_override("not-an-ip"):
                pass

    def test_tmdb_dns_override_restores_resolver_after_body_error(self):
        original = socket.getaddrinfo
        with mock.patch.dict(os.environ, {
            "TMDB_BASE_URL": "https://api.themoviedb.org/3",
        }):
            with self.assertRaisesRegex(RuntimeError, "stop"):
                with temporary_tmdb_dns_override("13.224.245.63"):
                    raise RuntimeError("stop")
        self.assertIs(socket.getaddrinfo, original)

    def test_whole_library_defaults_exclude_workflow_and_system_roots(self):
        self.assertEqual(default_excluded_roots("/quark/影视/"), [
            "/quark/影视/待刮削",
            "/quark/影视/已刮削",
            "/quark/影视/ScrapeFlow",
        ])
        self.assertEqual(default_excluded_roots("/quark/影视/番剧"), [])
        self.assertEqual(formal_media_roots("/quark/影视"), (
            "/quark/影视/番剧", "/quark/影视/美剧", "/quark/影视/电影",
        ))

    def test_system_root_exclusion_covers_existing_and_future_subdirectories(self):
        class FakeAList:
            def walk(self, *_args, **_kwargs):
                return [
                    {"full_path": "/quark/影视/番剧/Visible/E01.mkv"},
                    {"full_path": "/quark/影视/待刮削/Pending/E01.mkv"},
                    {"full_path": "/quark/影视/已刮削/Done/E01.mkv"},
                    {"full_path": "/quark/影视/ScrapeFlow/备份/Old/E01.mkv"},
                    {"full_path": "/quark/影视/ScrapeFlow/验证/Check/E01.mkv"},
                    {"full_path": "/quark/影视/ScrapeFlow/验证/Check/tvshow.nfo"},
                    {"full_path": "/quark/影视/ScrapeFlow/补源/Job/E01.mkv"},
                    {"full_path": "/quark/影视/ScrapeFlow/未来系统目录/E01.mkv"},
                ]

            def list(self, *_args, **_kwargs):
                return []

            def read_file_prefix(self, path, **_kwargs):
                raise AssertionError(f"排除目录内的 NFO 不应被解析: {path}")

        class FakeTMDB:
            def get(self, _path):
                raise AssertionError("没有 NFO 时不应请求 TMDB")

            def cache_report(self):
                return {}

        payload = scan_library(
            FakeAList(),
            FakeTMDB(),
            "/quark/影视",
            excluded_roots=default_excluded_roots("/quark/影视"),
        )

        self.assertEqual(
            payload["uncovered_media"],
            ["/quark/影视/番剧/Visible/E01.mkv"],
        )
        self.assertEqual(payload["summary"]["uncovered_media"], 1)

    def test_cli_passes_default_system_exclusions_to_scan_library(self):
        output = {"schema_version": 1}
        client = mock.Mock()
        with tempfile.TemporaryDirectory() as directory, mock.patch.object(
            sys,
            "argv",
            [
                "audit_live_library.py",
                "--root", "/quark/影视",
                "--output", str(Path(directory) / "audit.json"),
            ],
        ), mock.patch.object(
            audit_live_library, "AListClient", return_value=client,
        ), mock.patch.object(
            audit_live_library, "TMDBClient", return_value=mock.sentinel.tmdb,
        ), mock.patch.object(
            audit_live_library, "scan_library", return_value=output,
        ) as scan:
            self.assertEqual(audit_live_library.main(), 0)

        client.login.assert_called_once_with()
        self.assertEqual(
            scan.call_args.kwargs["excluded_roots"],
            [
                "/quark/影视/待刮削",
                "/quark/影视/已刮削",
                "/quark/影视/ScrapeFlow",
            ],
        )

    def test_parallel_audit_retries_transient_items_serially_in_original_order(self):
        attempts = {}

        def inspect(value):
            attempts[value] = attempts.get(value, 0) + 1
            if value == "transient" and attempts[value] == 1:
                raise RuntimeError("temporary TLS failure")
            return value.upper()

        result = _parallel_map_with_serial_retry(
            inspect, ["first", "transient", "last"], max_workers=3
        )

        self.assertEqual(result, ["FIRST", "TRANSIENT", "LAST"])
        self.assertEqual(attempts["transient"], 2)

    def test_parallel_audit_does_not_hide_permanent_failures(self):
        attempts = 0

        def inspect(_value):
            nonlocal attempts
            attempts += 1
            raise RuntimeError("still unavailable")

        with self.assertRaisesRegex(RuntimeError, "still unavailable"):
            _parallel_map_with_serial_retry(
                inspect, ["broken"], max_workers=1, serial_attempts=2
            )
        self.assertEqual(attempts, 3)

    def test_empty_work_root_is_not_lost_by_file_only_inventory(self):
        class FakeAList:
            def list(self, path, refresh=False):
                self.refresh = refresh
                return {
                    "/quark/影视": [{"name": "番剧", "is_dir": True}],
                    "/quark/影视/番剧": [
                        {"name": "东京食尸鬼", "is_dir": True},
                        {"name": "Fate", "is_dir": True},
                    ],
                }.get(path, [])

        roots = empty_library_roots(
            FakeAList(),
            "/quark/影视",
            {"/quark/影视/番剧/Fate/poster.jpg"},
        )
        self.assertEqual(roots, ["/quark/影视/番剧/东京食尸鬼"])

    def test_parses_nfo_and_multi_episode_range(self):
        nfo = parse_tvshow_nfo(b"<tvshow><title>Example</title><uniqueid type='tmdb'>10</uniqueid></tvshow>")
        self.assertEqual(nfo, {"title": "Example", "tmdb_ids": [10]})
        self.assertEqual(episode_numbers("Example - S01E03-E04.mkv"), (1, {3, 4}))
        movie = parse_movie_nfo(b"<movie><title>Film</title><year>2020</year><uniqueid type='tmdb'>20</uniqueid></movie>")
        self.assertEqual(movie, {"title": "Film", "year": "2020", "tmdb_ids": [20]})
        self.assertEqual(companion_stem("/movies/Film (2020).zh-CN.ass"), "/movies/Film (2020)")
        self.assertEqual(companion_stem("/movies/Film (2020).subtitle2.ass"), "/movies/Film (2020)")
        self.assertEqual(companion_stem("/movies/Film (2020).sc.ass"), "/movies/Film (2020)")

    def test_reports_published_gap_and_orphan_subtitle(self):
        payloads = {
            "/tv/10": {"name": "Example", "seasons": [{"season_number": 1}]},
            "/tv/10/season/1": {"episodes": [
                {"episode_number": 1, "air_date": "2020-01-01", "name": "One"},
                {"episode_number": 2, "air_date": "2020-01-08", "name": "Two"},
            ]},
        }
        files = [
            {"full_path": "/library/Example/Season 01/Example - S01E01.mkv"},
            {"full_path": "/library/Example/Season 01/Example - S01E02.zh-CN.ass"},
            {"full_path": "/library/Example/poster.jpg"},
            {"full_path": "/library/Example/season 1-poster.jpg"},
        ]
        result = analyze_series(
            root="/library/Example",
            nfo={"title": "Example", "tmdb_ids": [10]},
            files=files,
            tmdb_get=payloads.__getitem__,
            today=date(2026, 1, 1),
        )
        self.assertEqual([row["label"] for row in result["regular_missing"]], ["S01E02"])
        self.assertIn("subtitle_without_current_video", {row["code"] for row in result["issues"]})
        self.assertEqual(
            result["missing_subtitles"][0]["reason_code"],
            "missing_external_subtitle",
        )

    def test_subtitle_gap_distinguishes_missing_language_and_version_mismatch(self):
        video = "/library/Show/Season 01/Show - S01E01.mkv"
        self.assertEqual(
            external_subtitle_gap(video, [
                "/library/Show/Season 01/Show - S01E01.zh-TW.ass",
            ])["reason_code"],
            "required_subtitle_language_missing",
        )
        self.assertEqual(
            external_subtitle_gap(video, [
                "/library/Show/Season 01/Show - S01E01.ass",
            ])["reason_code"],
            "subtitle_language_unverified",
        )
        mismatch = external_subtitle_gap(video, [
            "/library/Show/Season 01/Show - S01E01 {edition-Director's Cut}.zh-CN.ass",
        ])
        self.assertEqual(mismatch["reason_code"], "subtitle_video_stem_mismatch")
        self.assertEqual(mismatch["remediation_action"], "review_subtitle_pairing")
        self.assertIsNone(external_subtitle_gap(video, [
            "/library/Show/Season 01/Show - S01E01.zh-CN.sup",
        ]))

    def test_mks_requires_stream_probe_instead_of_filename_language_claim(self):
        video = "/library/Show/Season 01/Show - S01E01.mkv"
        gap = external_subtitle_gap(video, [
            "/library/Show/Season 01/Show - S01E01.zh-CN.mks",
        ])
        self.assertEqual(gap["reason_code"], "subtitle_language_unverified")
        self.assertEqual(gap["remediation_action"], "probe_mks_stream_content")
        self.assertEqual(
            gap["candidate_languages"], ["und-mks-stream-unverified"]
        )

    def test_whole_library_inventory_classifies_every_residual_type(self):
        class FakeAList:
            def walk(self, *_args, **_kwargs):
                return [
                    {"full_path": "/library/Show/notes.docx"},
                    {"full_path": "/library/Show/audio.flac"},
                    {"full_path": "/library/Show/release.7z"},
                    {"full_path": "/library/Show/unknown.bin"},
                ]

            def list(self, *_args, **_kwargs):
                return []

        class FakeTMDB:
            def get(self, path):
                raise AssertionError(path)

            def cache_report(self):
                return {}

        payload = scan_library(FakeAList(), FakeTMDB(), "/library")
        by_path = {row["path"]: row for row in payload["residual_inventory"]}
        self.assertEqual(by_path["/library/Show/notes.docx"]["kind"], "document_or_comic")
        self.assertEqual(by_path["/library/Show/audio.flac"]["kind"], "detached_audio")
        self.assertEqual(
            by_path["/library/Show/release.7z"]["kind"],
            "archive_requires_extraction_receipt",
        )
        self.assertEqual(by_path["/library/Show/unknown.bin"]["kind"], "unclassified_residual")
        self.assertEqual(payload["summary"]["residual_files"], 4)

    def test_subtitle_only_scan_uses_series_boundaries_and_needs_no_tmdb(self):
        class FakeAList:
            def walk(self, *_args, **_kwargs):
                return [
                    {"full_path": "/library/A/tvshow.nfo"},
                    {"full_path": "/library/A/A - S01E01.mkv"},
                    {"full_path": "/library/A/A - S01E01.zh-CN.ass"},
                    {"full_path": "/library/A/A - S01E02.mkv"},
                    {"full_path": "/library/B/tvshow.nfo"},
                    {"full_path": "/library/B/B - S01E02.zh-CN.ass"},
                ]

        payload = scan_subtitle_inventory(FakeAList(), "/library")
        self.assertEqual(payload["summary"]["videos"], 2)
        self.assertEqual(len(payload["subtitle_inventory"]), 2)
        self.assertEqual(
            payload["subtitle_inventory"][0]["status"],
            "external_required_language_present",
        )
        self.assertEqual(payload["summary"]["missing_subtitles"], 1)
        self.assertEqual(
            payload["missing_subtitles"][0]["video_path"],
            "/library/A/A - S01E02.mkv",
        )

    def test_subtitle_only_whole_library_ignores_non_formal_roots(self):
        class FakeAList:
            def walk(self, *_args, **_kwargs):
                return [
                    {"full_path": "/quark/影视/番剧/A/A - S01E01.mkv"},
                    {"full_path": "/quark/影视/待刮削/B/B - S01E01.mkv"},
                    {"full_path": "/quark/影视/ScrapeFlow/补源/C/C - S01E01.mkv"},
                ]

        payload = scan_subtitle_inventory(FakeAList(), "/quark/影视")
        self.assertEqual(payload["summary"]["videos"], 1)
        self.assertEqual(payload["included_roots"], [
            "/quark/影视/番剧", "/quark/影视/美剧", "/quark/影视/电影",
        ])

    def test_subtitle_inventory_identifies_movie_by_companion_nfo(self):
        class FakeAList:
            def walk(self, *_args, **_kwargs):
                return [
                    {"full_path": "/library/Film (2022).mkv"},
                    {"full_path": "/library/Film (2022).nfo"},
                ]

        payload = scan_subtitle_inventory(FakeAList(), "/library")
        self.assertEqual(payload["subtitle_inventory"][0]["media_type"], "movie")

    def test_movie_sidecar_image_is_accepted_as_its_poster(self):
        class FakeAList:
            def walk(self, *_args, **_kwargs):
                return [
                    {"full_path": "/library/Film (2022).mkv"},
                    {"full_path": "/library/Film (2022).nfo"},
                    {"full_path": "/library/Film (2022).jpg"},
                ]

            def list(self, *_args, **_kwargs):
                return []

            def read_file_prefix(self, path, **_kwargs):
                self.assert_path = path
                return (
                    b"<movie><title>Film</title><year>2022</year>"
                    b"<uniqueid type='tmdb'>20</uniqueid></movie>"
                )

        class FakeTMDB:
            def get(self, path):
                if path == "/movie/20":
                    return {"title": "Film", "release_date": "2022-01-01"}
                raise AssertionError(path)

            def cache_report(self):
                return {}

        payload = scan_library(FakeAList(), FakeTMDB(), "/library")

        self.assertNotIn(
            "missing_movie_poster", payload["summary"]["movie_issue_codes"]
        )
        self.assertEqual(
            payload["movies"][0]["missing_subtitles"][0]["reason_code"],
            "missing_external_subtitle",
        )


if __name__ == "__main__":
    unittest.main()
