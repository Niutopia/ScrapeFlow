from __future__ import annotations

from datetime import date
import json
import os
from pathlib import Path
import tempfile
import threading
import time
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from local.scrapeflow_api.simple_library_audit import (
    DEFAULT_FORMAL_LIBRARY_ROOTS,
    SimpleLibraryAuditor,
    TmdbEpisodeCatalog,
    attach_automatic_gaps,
    audit_and_persist,
    automatic_works_from_engine_jobs,
    bootstrap_automatic_works_from_library,
    automatic_job_gaps,
    build_automatic_library_gaps,
    classify_embedded_subtitle_streams,
    make_alist_subtitle_checker,
    probe_remote_subtitle_streams,
    subtitle_evidence_ledger_path,
    _normalise_expected_episodes,
    _episode_tokens,
    run_automatic_library_audit,
)
from local.scrapeflow_api.replenishment import build_replenishment_request


class TreeAList:
    def __init__(self, tree: dict[str, list[dict[str, object]]]) -> None:
        self.tree = tree

    def list(self, path: str, refresh: bool = False) -> list[dict[str, object]]:
        del refresh
        return [dict(row) for row in self.tree.get(path, [])]


class NfoTreeAList(TreeAList):
    """Read-only fake that exposes bounded NFO reads but no mutation API."""

    def __init__(
        self,
        tree: dict[str, list[dict[str, object]]],
        nfos: dict[str, bytes],
    ) -> None:
        super().__init__(tree)
        self.nfos = dict(nfos)
        self.read_calls: list[tuple[str, int]] = []

    def read_file_bytes(self, path: str, *, max_bytes: int) -> bytes:
        self.read_calls.append((path, max_bytes))
        return self.nfos[path]


def _report() -> dict[str, object]:
    movie_root, anime_root, us_root = DEFAULT_FORMAL_LIBRARY_ROOTS
    show = f"{anime_root}/Show"
    season = f"{show}/Season 01"
    client = TreeAList({
        movie_root: [],
        anime_root: [{"name": "Show", "is_dir": True}],
        show: [{"name": "Season 01", "is_dir": True}],
        season: [
            {"name": "Show S01E01.mkv", "is_dir": False, "size": 10},
            {"name": "Show S01E01.zh.srt", "is_dir": False, "size": 2},
            {"name": "Show S01E03.mkv", "is_dir": False, "size": 10},
        ],
        us_root: [],
    })
    from local.scrapeflow_api.simple_library_audit import SimpleLibraryAuditor
    return SimpleLibraryAuditor(client).scan()


class AutomaticLibraryGapTests(unittest.TestCase):
    def test_scoped_audit_keeps_authoritative_shelf_types(self) -> None:
        """A concrete TV work scope must not be reclassified as a movie root."""
        movie_root, anime_root, us_root = DEFAULT_FORMAL_LIBRARY_ROOTS
        show = f"{anime_root}/Scoped Only"
        nfo_path = f"{show}/tvshow.nfo"
        client = NfoTreeAList(
            {
                movie_root: [],
                anime_root: [{"name": "Scoped Only", "is_dir": True}],
                show: [{"name": "tvshow.nfo", "is_dir": False, "size": 80}],
                us_root: [],
            },
            {nfo_path: b"<tvshow><title>Scoped Only</title><tmdbid>4242</tmdbid></tvshow>"},
        )

        class OneEpisodeTMDB:
            def get(self, path: str) -> dict[str, object]:
                if path == "/tv/4242":
                    return {"seasons": [{"season_number": 1}]}
                return {"episodes": [{"episode_number": 1, "air_date": "2020-01-01"}]}

        with tempfile.TemporaryDirectory() as tmp:
            report = run_automatic_library_audit(
                client,
                tmp,
                [],
                formal_roots=DEFAULT_FORMAL_LIBRARY_ROOTS,
                scope_roots=(show,),
                tmdb_client=OneEpisodeTMDB(),
            )

        self.assertEqual([row["path"] for row in report["roots"]], [show])
        self.assertEqual(report["semantic"]["gap_count"], 1)
        self.assertEqual(report["semantic"]["unknown_count"], 0)
        self.assertEqual(report["semantic"]["gaps"][0]["kind"], "missing_episode")
        self.assertEqual(report["semantic"]["gaps"][0]["media"]["media_type"], "tv")

    def test_scoped_audit_rejects_work_root_outside_formal_shelves(self) -> None:
        with self.assertRaises(ValueError):
            SimpleLibraryAuditor(
                TreeAList({root: [] for root in DEFAULT_FORMAL_LIBRARY_ROOTS}),
                formal_roots=DEFAULT_FORMAL_LIBRARY_ROOTS,
                scope_roots=("/library/待刮削/Incoming",),
            )

    def test_missing_episode_and_metadata_are_machine_gaps(self) -> None:
        report = _report()
        show = f"{DEFAULT_FORMAL_LIBRARY_ROOTS[1]}/Show"
        result = build_automatic_library_gaps(
            report,
            [{
                "tmdb_id": 100,
                "title": "Show",
                "media_type": "tv",
                "target_root": show,
                "season": 1,
                "expected_episodes": {1: [1, 2, 3]},
            }],
            required_subtitle_language="zh",
            subtitle_checker=lambda path: False if path.endswith("E03.mkv") else True,
        )
        self.assertEqual(result["status"], "completed")
        gaps = result["gaps"]
        self.assertEqual(
            {(row["kind"], row.get("season"), row.get("episode")) for row in gaps if row["kind"] == "missing_episode"},
            {("missing_episode", 1, 2)},
        )
        self.assertTrue(any(row["kind"] == "missing_subtitle" and row["path"].endswith("E03.mkv") for row in gaps))
        self.assertTrue(any(row["kind"] == "missing_nfo" for row in gaps))
        self.assertTrue(any(row["kind"] == "missing_poster" for row in gaps))
        projects = result["acquisition_projects"]
        self.assertEqual(len(projects), 1)
        self.assertEqual(projects[0]["plan"]["scan_report"]["resource_gaps"][0]["kind"], "missing_episode")
        self.assertNotIn("sha256", json.dumps(result, ensure_ascii=False).casefold())

    def test_explicit_episode_range_avoids_false_acquisition_gaps(self) -> None:
        movie_root, anime_root, us_root = DEFAULT_FORMAL_LIBRARY_ROOTS
        show = f"{anime_root}/Range Show"
        season = f"{show}/Season 01"
        client = TreeAList({
            movie_root: [],
            anime_root: [{"name": "Range Show", "is_dir": True}],
            show: [{"name": "Season 01", "is_dir": True}],
            season: [
                {"name": "Range Show - S01E01-E02.mkv", "is_dir": False, "size": 10},
                {"name": "Range Show - S01E133-E134.mkv", "is_dir": False, "size": 10},
            ],
            us_root: [],
        })
        report = SimpleLibraryAuditor(client).scan()

        result = build_automatic_library_gaps(
            report,
            [{
                "tmdb_id": 101,
                "title": "Range Show",
                "media_type": "tv",
                "target_root": show,
                "expected_episodes": {1: [1, 2, 133, 134, 135]},
            }],
        )

        self.assertEqual(
            [
                (row["season"], row["episode"])
                for row in result["gaps"]
                if row["kind"] == "missing_episode"
            ],
            [(1, 135)],
        )

    def test_episode_range_requires_explicit_bounded_endpoint(self) -> None:
        self.assertEqual(
            _episode_tokens("/library/Show - S01E01-E02.mkv"),
            {(1, 1), (1, 2)},
        )
        self.assertEqual(
            _episode_tokens("/library/Show - S01E133-E134.mkv"),
            {(1, 133), (1, 134)},
        )
        self.assertEqual(
            _episode_tokens("/library/Show - S01E01-2024.mkv"),
            {(1, 1)},
        )
        self.assertEqual(
            _episode_tokens("/library/Show - S01E01-E9999.mkv"),
            {(1, 1)},
        )

    def test_subtitle_without_probe_is_unknown_not_a_false_gap(self) -> None:
        report = _report()
        show = f"{DEFAULT_FORMAL_LIBRARY_ROOTS[1]}/Show"
        result = build_automatic_library_gaps(
            report,
            [{"tmdb_id": 100, "title": "Show", "media_type": "tv",
              "target_root": show, "season": 1, "expected_episodes": {1: [1, 3]}}],
            required_subtitle_language="zh",
        )
        self.assertFalse(any(row["kind"] == "missing_subtitle" for row in result["gaps"]))
        self.assertTrue(any(row["kind"] == "unknown_subtitle_evidence" for row in result["unknowns"]))

    def test_embedded_subtitle_track_classifier_recognises_chinese_aliases(self) -> None:
        for language in ("zh", "chi", "zho", "chs", "cht", "zh-Hans", "zh-Hant"):
            with self.subTest(language=language):
                result = classify_embedded_subtitle_streams(
                    [{"index": 2, "codec_name": "ass", "tags": {"language": language}}],
                    "zh",
                )
                self.assertEqual(result["status"], "satisfied")

    def test_zh_hans_lane_rejects_traditional_and_keeps_unqualified_unknown(self) -> None:
        simplified = classify_embedded_subtitle_streams(
            [{"index": 1, "codec_name": "ass", "tags": {"language": "zh-Hans"}}],
            "zh-Hans",
        )
        traditional = classify_embedded_subtitle_streams(
            [{"index": 1, "codec_name": "ass", "tags": {"language": "zh-Hant"}}],
            "zh-Hans",
        )
        unqualified = classify_embedded_subtitle_streams(
            [{"index": 1, "codec_name": "ass", "tags": {"language": "zh"}}],
            "zh-Hans",
        )
        self.assertEqual(simplified["status"], "satisfied")
        self.assertEqual(simplified["tracks"][0]["lane"], "zh-Hans")
        self.assertEqual(traditional["status"], "missing")
        self.assertEqual(traditional["tracks"][0]["lane"], "non-zh-Hans")
        self.assertEqual(unqualified["status"], "unknown")
        self.assertEqual(unqualified["tracks"][0]["lane"], "unknown")

    def test_embedded_subtitle_track_classifier_keeps_ambiguous_track_unknown(self) -> None:
        result = classify_embedded_subtitle_streams(
            [{"index": 2, "codec_name": "ass", "tags": {}}], "zh",
        )
        self.assertEqual(result["status"], "unknown")

    def test_embedded_non_target_iso639_tracks_are_conclusive_missing(self) -> None:
        result = classify_embedded_subtitle_streams(
            [
                {"index": 2, "codec_name": "ass", "tags": {"language": "eng"}},
                {"index": 3, "codec_name": "ass", "tags": {"language": "ita"}},
            ],
            "zh",
        )
        self.assertEqual(result["status"], "missing")

    def test_bounded_remote_subtitle_probe_creates_only_conclusive_gap_evidence(self) -> None:
        class LinkAList:
            def __init__(self) -> None:
                self.links: list[str] = []

            def file_link(self, path: str, refresh: bool = False):
                self.links.append(path)
                self.assertTrue(refresh)
                return "https://provider.example/video.mkv?signature=redacted", {"Referer": "https://alist.example"}

            def assertTrue(self, value: object) -> None:
                if not value:
                    raise AssertionError("expected true")

        client = LinkAList()
        response = SimpleNamespace(
            returncode=0,
            stdout=json.dumps({"streams": [{
                "index": 2, "codec_name": "ass", "tags": {"language": "eng"},
            }]}),
            stderr="",
        )
        with patch("local.scrapeflow_api.simple_library_audit.shutil.which", return_value="/usr/bin/ffprobe"), patch(
            "local.scrapeflow_api.simple_library_audit.subprocess.run", return_value=response,
        ) as run:
            result = probe_remote_subtitle_streams(client, "/library/Show S01E01.mkv", "zh")

        self.assertEqual(result["status"], "missing")
        self.assertEqual(client.links, ["/library/Show S01E01.mkv"])
        command = run.call_args.args[0]
        self.assertIn("-probesize", command)
        self.assertIn("-analyzeduration", command)
        self.assertIn("-select_streams", command)
        self.assertNotIn("signature=redacted", json.dumps(result))

    def test_bounded_prefix_positive_probe_does_not_spawn_signed_url_fallback(self) -> None:
        class PrefixAList:
            def read_file_prefix(self, _path: str, *, max_bytes: int) -> bytes:
                self.max_bytes = max_bytes
                return b"bounded-container-prefix"

            def file_link(self, _path: str, refresh: bool = False):
                raise AssertionError("positive prefix must not request a signed URL")

        response = SimpleNamespace(
            returncode=0,
            stdout=json.dumps({"streams": [{
                "index": 1, "codec_name": "ass", "tags": {"language": "chs"},
            }]}).encode(),
            stderr=b"",
        )
        client = PrefixAList()
        with patch("local.scrapeflow_api.simple_library_audit.shutil.which", return_value="/usr/bin/ffprobe"), patch(
            "local.scrapeflow_api.simple_library_audit.subprocess.run", return_value=response,
        ):
            result = probe_remote_subtitle_streams(client, "/library/Show.mkv", "zh")
        self.assertEqual(result["status"], "satisfied")
        self.assertGreater(client.max_bytes, 0)

    def test_complete_matroska_tracks_prefix_skips_signed_link_for_negative_result(self) -> None:
        """A full EBML Tracks header makes explicit non-target tracks conclusive."""
        def element(identifier: str, payload: bytes) -> bytes:
            self.assertLess(len(payload), 0x7F)
            return bytes.fromhex(identifier) + bytes([0x80 | len(payload)]) + payload

        track = element("AE", element("D7", b"\x11") + element("22B59C", b"eng"))
        prefix = (
            element("1A45DFA3", b"")
            + bytes.fromhex("18538067") + b"\xff"  # unknown-size Segment
            + element("1654AE6B", track)
        )

        class PrefixAList:
            def read_file_prefix(self, _path: str, *, max_bytes: int) -> bytes:
                self.max_bytes = max_bytes
                return prefix

            def file_link(self, _path: str, refresh: bool = False):
                raise AssertionError("complete Matroska Tracks must not request a signed URL")

        response = SimpleNamespace(
            returncode=0,
            stdout=json.dumps({"streams": [{
                "index": 2, "codec_name": "ass", "tags": {"language": "eng"},
            }]}).encode(),
            stderr=b"",
        )
        client = PrefixAList()
        with patch("local.scrapeflow_api.simple_library_audit.shutil.which", return_value="/usr/bin/ffprobe"), patch(
            "local.scrapeflow_api.simple_library_audit.subprocess.run", return_value=response,
        ) as run:
            result = probe_remote_subtitle_streams(client, "/library/Show.mkv", "zh")

        self.assertEqual(result["status"], "missing")
        self.assertEqual(result["source"], "embedded_complete_mkv_prefix")
        self.assertEqual(run.call_count, 1)
        self.assertGreater(client.max_bytes, 0)

    def test_incomplete_matroska_tracks_prefix_still_uses_signed_link_fallback(self) -> None:
        """A truncated Tracks element is not absence evidence."""
        def element(identifier: str, payload: bytes) -> bytes:
            self.assertLess(len(payload), 0x7F)
            return bytes.fromhex(identifier) + bytes([0x80 | len(payload)]) + payload

        track = element("AE", element("D7", b"\x11") + element("22B59C", b"eng"))
        complete = (
            element("1A45DFA3", b"")
            + bytes.fromhex("18538067") + b"\xff"
            + element("1654AE6B", track)
        )

        class PrefixAList:
            def __init__(self) -> None:
                self.links = 0

            def read_file_prefix(self, _path: str, *, max_bytes: int) -> bytes:
                del max_bytes
                return complete[:-1]

            def file_link(self, _path: str, refresh: bool = False):
                self.links += 1
                self.assertTrue(refresh)
                return "https://provider.example/video.mkv", {}

            def assertTrue(self, value: object) -> None:
                if not value:
                    raise AssertionError("expected true")

        response = SimpleNamespace(
            returncode=0,
            stdout=json.dumps({"streams": [{
                "index": 2, "codec_name": "ass", "tags": {"language": "eng"},
            }]}).encode(),
            stderr=b"",
        )
        client = PrefixAList()
        with patch("local.scrapeflow_api.simple_library_audit.shutil.which", return_value="/usr/bin/ffprobe"), patch(
            "local.scrapeflow_api.simple_library_audit.subprocess.run", return_value=response,
        ) as run:
            result = probe_remote_subtitle_streams(client, "/library/Show.mkv", "zh")

        self.assertEqual(result["status"], "missing")
        self.assertEqual(client.links, 1)
        self.assertEqual(run.call_count, 2)

    def test_subtitle_probe_cache_and_embedded_result_prevent_duplicate_gap(self) -> None:
        class LinkAList:
            def __init__(self) -> None:
                self.calls = 0

            def file_link(self, _path: str, refresh: bool = False):
                self.calls += 1
                self.assertTrue(refresh)
                return "https://provider.example/video.mkv", {}

            def assertTrue(self, value: object) -> None:
                if not value:
                    raise AssertionError("expected true")

        client = LinkAList()
        response = SimpleNamespace(
            returncode=0,
            stdout=json.dumps({"streams": [{
                "index": 3, "codec_name": "ass", "tags": {"language": "zho"},
            }]}),
            stderr="",
        )
        with patch("local.scrapeflow_api.simple_library_audit.shutil.which", return_value="/usr/bin/ffprobe"), patch(
            "local.scrapeflow_api.simple_library_audit.subprocess.run", return_value=response,
        ):
            checker = make_alist_subtitle_checker(client, "zh")
            self.assertEqual(checker("/library/Show S01E01.mkv")["status"], "satisfied")
            self.assertEqual(checker("/library/Show S01E01.mkv")["status"], "satisfied")
        self.assertEqual(client.calls, 1)

        report = _report()
        show = f"{DEFAULT_FORMAL_LIBRARY_ROOTS[1]}/Show"
        result = build_automatic_library_gaps(
            report,
            [{"tmdb_id": 100, "title": "Show", "media_type": "tv",
              "target_root": show, "season": 1, "expected_episodes": {1: [1, 3]}}],
            required_subtitle_language="zh",
            subtitle_checker=lambda _path: {"status": "satisfied", "source": "embedded"},
        )
        self.assertFalse(any(row["kind"] == "missing_subtitle" for row in result["gaps"]))

    def test_subtitle_checker_prefetch_is_bounded_and_cached(self) -> None:
        with patch(
            "local.scrapeflow_api.simple_library_audit.probe_remote_subtitle_streams",
            side_effect=lambda _client, _path, _language: {
                "status": "unknown", "reason": "test"
            },
        ) as probe:
            checker = make_alist_subtitle_checker(object(), "zh")
            prefetch = getattr(checker, "prefetch")
            prefetch(["/library/A.mkv", "/library/B.mkv", "/library/A.mkv"])
            checker("/library/A.mkv")
        self.assertEqual(probe.call_count, 2)

    def test_subtitle_evidence_cursor_fairly_advances_unknown_batch_after_restart(self) -> None:
        """A persistent cursor must not keep retrying the first paths forever."""
        rows = [
            {"path": f"/library/Show S01E{number:02d}.mkv", "size": 100, "version": "v1"}
            for number in range(1, 6)
        ]
        calls: list[str] = []

        def unknown_probe(_client: object, path: str, _language: str) -> dict[str, object]:
            calls.append(path)
            return {"status": "unknown", "reason": "ffprobe_timeout"}

        with tempfile.TemporaryDirectory() as directory, patch.dict(
            os.environ,
            {
                "SCRAPEFLOW_SUBTITLE_PROBE_BATCH_SIZE": "2",
                "SCRAPEFLOW_SUBTITLE_PROBE_WORKERS": "1",
            },
            clear=False,
        ), patch(
            "local.scrapeflow_api.simple_library_audit.probe_remote_subtitle_streams",
            side_effect=unknown_probe,
        ):
            first = make_alist_subtitle_checker(object(), "zh", state_root=directory)
            getattr(first, "prefetch")(rows)
            second = make_alist_subtitle_checker(object(), "zh", state_root=directory)
            getattr(second, "prefetch")(rows)

            ledger = json.loads(subtitle_evidence_ledger_path(directory).read_text())

        self.assertEqual(calls, [
            "/library/Show S01E01.mkv",
            "/library/Show S01E02.mkv",
            "/library/Show S01E03.mkv",
            "/library/Show S01E04.mkv",
        ])
        self.assertEqual(ledger["cursors"], {"zh": "/library/Show S01E04.mkv"})

    def test_subtitle_evidence_reuses_only_matching_size_and_version(self) -> None:
        """A renamed/replaced provider object cannot inherit an old verdict."""
        row_v1 = {"path": "/library/Show S01E01.mkv", "size": 100, "version": "v1"}
        row_v2 = {**row_v1, "version": "v2"}
        calls: list[str] = []

        def satisfied_probe(_client: object, path: str, _language: str) -> dict[str, object]:
            calls.append(path)
            return {
                "status": "satisfied",
                "source": "embedded",
                # A provider return may contain sensitive/verbose fields; the
                # durable ledger must keep none of them.
                "url": "https://signed.example/secret",
                "headers": {"Authorization": "secret"},
                "tracks": [{"title": "private title"}],
            }

        with tempfile.TemporaryDirectory() as directory, patch(
            "local.scrapeflow_api.simple_library_audit.probe_remote_subtitle_streams",
            side_effect=satisfied_probe,
        ):
            first = make_alist_subtitle_checker(object(), "zh", state_root=directory)
            getattr(first, "prefetch")([row_v1])

            same = make_alist_subtitle_checker(object(), "zh", state_root=directory)
            getattr(same, "prefetch")([row_v1])
            self.assertEqual(same(row_v1["path"])["status"], "satisfied")

            mutated = make_alist_subtitle_checker(object(), "zh", state_root=directory)
            getattr(mutated, "prefetch")([row_v2])
            ledger_text = subtitle_evidence_ledger_path(directory).read_text()
            ledger = json.loads(ledger_text)

        self.assertEqual(calls, [row_v1["path"], row_v2["path"]])
        self.assertNotIn("signed.example", ledger_text)
        self.assertNotIn("Authorization", ledger_text)
        self.assertNotIn("private title", ledger_text)
        self.assertEqual(len(ledger["entries"]), 1)
        self.assertTrue(all(
            set(entry).issubset({"status", "reason", "source"})
            for entry in ledger["entries"].values()
        ))

    def test_subtitle_evidence_unknown_preserves_reason_and_never_creates_gap(self) -> None:
        """A timed-out probe remains visible unknown, never a speculative write."""
        report = _report()
        show = f"{DEFAULT_FORMAL_LIBRARY_ROOTS[1]}/Show"
        with patch(
            "local.scrapeflow_api.simple_library_audit.probe_remote_subtitle_streams",
            return_value={"status": "unknown", "reason": "ffprobe_timeout"},
        ):
            checker = make_alist_subtitle_checker(object(), "zh")
            result = build_automatic_library_gaps(
                report,
                [{
                    "tmdb_id": 100,
                    "title": "Show",
                    "media_type": "tv",
                    "target_root": show,
                    "season": 1,
                    "expected_episodes": {1: [1, 3]},
                }],
                required_subtitle_language="zh",
                subtitle_checker=checker,
            )

        self.assertFalse(any(row["kind"] == "missing_subtitle" for row in result["gaps"]))
        unknowns = [
            row for row in result["unknowns"]
            if row["kind"] == "unknown_subtitle_evidence"
            and str(row.get("path", "")).endswith("E03.mkv")
        ]
        self.assertEqual(len(unknowns), 1)
        self.assertEqual(unknowns[0]["reason"], "ffprobe_timeout")

    def test_subtitle_prefetch_budget_fails_closed_without_waiting_for_slow_probe(self) -> None:
        """Queued/running probes become unknown at the monotonic deadline."""
        release = threading.Event()
        calls: list[str] = []

        def slow_probe(_client: object, path: str, _language: str) -> dict[str, object]:
            calls.append(path)
            # The checker must return before this provider call is released.
            release.wait(5)
            return {"status": "missing", "source": "slow-test"}

        show = f"{DEFAULT_FORMAL_LIBRARY_ROOTS[1]}/Show"
        try:
            with patch.dict(
                os.environ,
                {
                    "SCRAPEFLOW_SUBTITLE_PROBE_BUDGET_SECONDS": "0.05",
                    "SCRAPEFLOW_SUBTITLE_PROBE_WORKERS": "2",
                },
                clear=False,
            ), patch(
                "local.scrapeflow_api.simple_library_audit.probe_remote_subtitle_streams",
                side_effect=slow_probe,
            ):
                checker = make_alist_subtitle_checker(object(), "zh")
                prefetch = getattr(checker, "prefetch")
                started_at = time.monotonic()
                prefetch([
                    "/library/Show S01E01.mkv",
                    "/library/Show S01E02.mkv",
                    "/library/Show S01E03.mkv",
                    "/library/Show S01E04.mkv",
                ])
                elapsed = time.monotonic() - started_at
                self.assertLess(elapsed, 0.8)
                # A late completion must not overwrite the fail-closed cache
                # entry with ``missing``.
                self.assertEqual(
                    checker("/library/Show S01E01.mkv")["status"], "unknown",
                )
                self.assertEqual(
                    checker("/library/Show S01E04.mkv")["status"], "unknown",
                )
                result = build_automatic_library_gaps(
                    _report(),
                    [{
                        "tmdb_id": 100, "title": "Show", "media_type": "tv",
                        "target_root": show, "season": 1,
                        "expected_episodes": {1: [1, 3]},
                    }],
                    required_subtitle_language="zh",
                    subtitle_checker=checker,
                )
                self.assertFalse(any(
                    row["kind"] == "missing_subtitle" for row in result["gaps"]
                ))
                self.assertTrue(any(
                    row["kind"] == "unknown_subtitle_evidence"
                    and str(row.get("path", "")).endswith("E03.mkv")
                    for row in result["unknowns"]
                ))
        finally:
            release.set()
        self.assertLessEqual(len(calls), 2)

    def test_conclusive_embedded_probe_absence_creates_subtitle_gap(self) -> None:
        report = _report()
        show = f"{DEFAULT_FORMAL_LIBRARY_ROOTS[1]}/Show"
        result = build_automatic_library_gaps(
            report,
            [{"tmdb_id": 100, "title": "Show", "media_type": "tv",
              "target_root": show, "season": 1, "expected_episodes": {1: [1, 3]}}],
            required_subtitle_language="zh",
            subtitle_checker=lambda _path: {"status": "missing", "source": "embedded"},
        )
        subtitles = [row for row in result["gaps"] if row["kind"] == "missing_subtitle"]
        self.assertEqual(len(subtitles), 1)
        self.assertTrue(str(subtitles[0]["path"]).endswith("E03.mkv"))

    def test_incomplete_inventory_never_fabricates_gap(self) -> None:
        report = _report()
        report["complete"] = False
        result = build_automatic_library_gaps(
            report,
            [{"tmdb_id": 100, "title": "Show", "media_type": "tv",
              "target_root": f"{DEFAULT_FORMAL_LIBRARY_ROOTS[1]}/Show",
              "expected_episodes": {1: [1, 2]}}],
        )
        self.assertEqual(result["status"], "unknown")
        self.assertEqual(result["gaps"], [])
        self.assertEqual(result["unknowns"][0]["kind"], "unknown_inventory")

    def test_unregistered_media_directory_is_explicitly_unknown(self) -> None:
        result = build_automatic_library_gaps(_report(), [])
        self.assertTrue(any(row["kind"] == "unknown_library_work" for row in result["unknowns"]))

    def test_untrusted_ancillary_observation_cannot_hide_unknown_media(self) -> None:
        report = _report()
        season = f"{DEFAULT_FORMAL_LIBRARY_ROOTS[1]}/Show/Season 01"
        report["observations"]["ancillary_media"] = [{
            "path": f"{season}/Show S01E01.mkv",
            "source": "explicit_ancillary_filename",
        }]

        result = build_automatic_library_gaps(report, [])

        self.assertEqual(result["ancillary_media"], [])
        self.assertTrue(any(
            row["kind"] == "unknown_library_work"
            and f"{season}/Show S01E01.mkv" in row["uncovered_video_paths"]
            for row in result["unknowns"]
        ))

    def test_audit_and_persist_accepts_automatic_works(self) -> None:
        roots = DEFAULT_FORMAL_LIBRARY_ROOTS
        show = f"{roots[1]}/Show"
        client = TreeAList({root: [] for root in roots})
        with tempfile.TemporaryDirectory() as tmp:
            report = audit_and_persist(
                client, Path(tmp), formal_roots=roots,
                works=[{"tmdb_id": 100, "title": "Show", "media_type": "tv",
                        "target_root": show, "expected_episodes": {1: [1]}}],
            )
            saved = json.loads((Path(tmp) / "library-audit" / "latest.json").read_text())
        self.assertEqual(report, saved)
        self.assertIn("semantic", saved)
        self.assertEqual(saved["semantic"]["gap_count"], 1)

    def test_engine_jobs_are_reduced_to_completed_automatic_works(self) -> None:
        jobs = [
            {"phase": "planned", "plan": {"target_root": "/lib/Show"},
             "summary": {"identity": {"tmdb_id": 1, "media_type": "tv"}}},
            {"phase": "completed", "plan": {
                "mode": "tv", "target_root": "/lib/Show",
                "metadata": {"tmdb_id": 1, "title": "Show"}},
             "summary": {"identity": {"tmdb_id": 1, "media_type": "tv", "title": "Show"}}},
            {"phase": "completed", "plan": {
                "mode": "movie", "target_root": "/lib/Movie",
                "metadata": {"tmdb_id": 2, "title": "Movie"}},
             "summary": {"identity": {"tmdb_id": 2, "media_type": "movie", "title": "Movie"}}},
        ]
        works = automatic_works_from_engine_jobs(jobs)
        self.assertEqual([(row["tmdb_id"], row["media_type"]) for row in works], [(1, "tv"), (2, "movie")])

    def test_unfinished_job_is_reported_separately_from_provider_gaps(self) -> None:
        rows = automatic_job_gaps([
            {"id": "job-1", "phase": "retry_wait",
             "request": {"source_path": "/quark/影视/待刮削/Show"}},
            {"id": "job-2", "phase": "completed", "request": {}},
        ])
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["kind"], "incomplete_job")
        self.assertTrue(rows[0]["retryable"])

    def test_internal_provider_child_is_not_an_audit_work_or_job_gap(self) -> None:
        child = {
            "id": "engine-child-1",
            "phase": "failed",
            "plan": {
                "mode": "tv",
                "target_root": "/quark/影视/番剧/Show",
                "metadata": {"tmdb_id": 7, "title": "Show"},
            },
            "request": {"source_path": "/quark/影视/ScrapeFlow/补源/child"},
            "summary": {
                "internal_child": True,
                "root_job_id": "engine-root-1",
                "identity": {"tmdb_id": 7, "media_type": "tv"},
            },
        }
        self.assertEqual(automatic_works_from_engine_jobs([child]), [])
        self.assertEqual(automatic_job_gaps([child]), [])

    def test_composition_root_entry_point_needs_only_jobs_and_clients(self) -> None:
        roots = DEFAULT_FORMAL_LIBRARY_ROOTS
        client = TreeAList({root: [] for root in roots})
        jobs = [{
            "phase": "completed",
            "plan": {"mode": "tv", "target_root": f"{roots[1]}/Show",
                     "metadata": {"tmdb_id": 1, "title": "Show",
                                  "expected_episodes": {1: [1]}}},
            "summary": {"identity": {"tmdb_id": 1, "media_type": "tv", "title": "Show"}},
        }]
        with tempfile.TemporaryDirectory() as tmp:
            report = run_automatic_library_audit(client, tmp, jobs, formal_roots=roots)
            saved = json.loads((Path(tmp) / "library-audit" / "latest.json").read_text())
        self.assertEqual(report["semantic"]["status"], "completed")
        self.assertEqual(report["semantic"]["gap_count"], 1)
        self.assertEqual(saved["semantic"]["job_gap_count"], 0)

    def test_existing_movie_nfo_bootstraps_a_read_only_semantic_work(self) -> None:
        movie_root, anime_root, us_root = DEFAULT_FORMAL_LIBRARY_ROOTS
        movie = f"{movie_root}/Movie (2020)"
        nfo_path = f"{movie}/movie.nfo"
        client = NfoTreeAList({
            movie_root: [{"name": "Movie (2020)", "is_dir": True}],
            movie: [
                {"name": "Movie.mkv", "is_dir": False, "size": 10},
                {"name": "movie.nfo", "is_dir": False, "size": 80},
                {"name": "poster.jpg", "is_dir": False, "size": 2},
            ],
            anime_root: [],
            us_root: [],
        }, {
            nfo_path: b"<?xml version='1.0'?><movie><tmdbid>8</tmdbid></movie>",
        })
        structural = SimpleLibraryAuditor(client, formal_roots=DEFAULT_FORMAL_LIBRARY_ROOTS).scan()
        works = bootstrap_automatic_works_from_library(
            structural,
            client,
            formal_roots=DEFAULT_FORMAL_LIBRARY_ROOTS,
        )
        self.assertEqual(works, [{
            "tmdb_id": 8,
            "title": "Movie",
            "original_title": None,
            "year": "2020",
            "target_root": movie,
            "media_type": "movie",
            "identity_source": "library_nfo",
            "nfo_path": nfo_path,
            "identity_scope": {
                "kind": "video_file",
                "video_path": f"{movie}/Movie.mkv",
            },
        }])

        with tempfile.TemporaryDirectory() as tmp:
            report = run_automatic_library_audit(
                client,
                tmp,
                [],
                formal_roots=DEFAULT_FORMAL_LIBRARY_ROOTS,
            )

        self.assertTrue(report["complete"], "inventory completion is still reported separately")
        self.assertTrue(report["clean"])
        self.assertTrue(report["library_complete"])
        self.assertEqual(report["semantic"]["gap_count"], 0)
        self.assertEqual(report["semantic"]["unknown_count"], 0)
        self.assertEqual(report["semantic"]["bootstrap"], {
            "nfo_work_count": 1,
            "unowned_work_count": 1,
            "unowned_gap_count": 0,
            "unowned_unknown_count": 0,
            "creates_owner_tasks": False,
        })
        self.assertTrue(client.read_calls)

    def test_empty_tvshow_nfo_is_a_missing_tv_work_not_a_green_library(self) -> None:
        movie_root, anime_root, us_root = DEFAULT_FORMAL_LIBRARY_ROOTS
        show = f"{anime_root}/Empty Show"
        nfo_path = f"{show}/tvshow.nfo"
        client = NfoTreeAList({
            movie_root: [],
            anime_root: [{"name": "Empty Show", "is_dir": True}],
            show: [{"name": "tvshow.nfo", "is_dir": False, "size": 80}],
            us_root: [],
        }, {
            nfo_path: b"<tvshow><title>Empty Show</title><tmdbid>42</tmdbid></tvshow>",
        })

        class OneEpisodeTMDB:
            def get(self, path: str) -> dict[str, object]:
                if path == "/tv/42":
                    return {"seasons": [{"season_number": 1}]}
                return {"episodes": [{"episode_number": 1, "air_date": "2020-01-01"}]}

        with tempfile.TemporaryDirectory() as tmp:
            report = run_automatic_library_audit(
                client,
                tmp,
                [],
                tmdb_client=OneEpisodeTMDB(),
                formal_roots=DEFAULT_FORMAL_LIBRARY_ROOTS,
            )

        self.assertFalse(report["library_complete"])
        self.assertEqual(report["semantic"]["bootstrap"]["nfo_work_count"], 1)
        self.assertEqual(
            [(row["kind"], row.get("season"), row.get("episode")) for row in report["semantic"]["gaps"]],
            [("missing_episode", 1, 1)],
        )

    def test_episode_nfo_keeps_season_under_parent_tvshow_scope(self) -> None:
        movie_root, anime_root, us_root = DEFAULT_FORMAL_LIBRARY_ROOTS
        show = f"{anime_root}/Scoped Show"
        season = f"{show}/Season 03"
        parent_nfo = f"{show}/tvshow.nfo"
        episode_nfo = f"{season}/Scoped Show S03E05.nfo"
        client = NfoTreeAList({
            movie_root: [],
            anime_root: [{"name": "Scoped Show", "is_dir": True}],
            show: [
                {"name": "tvshow.nfo", "is_dir": False, "size": 64},
                {"name": "Season 03", "is_dir": True},
            ],
            season: [
                {"name": "Scoped Show S03E05.mkv", "is_dir": False, "size": 10},
                {"name": "Scoped Show S03E05.nfo", "is_dir": False, "size": 32},
            ],
            us_root: [],
        }, {
            parent_nfo: b"<tvshow><title>Scoped Show</title><tmdbid>903</tmdbid></tvshow>",
            # An episode-details NFO is intentionally not a competing work
            # identity; the auditor still inventories it as a file.
            episode_nfo: b"<episodedetails><season>3</season><episode>5</episode></episodedetails>",
        })

        structural = SimpleLibraryAuditor(client).scan()
        works = bootstrap_automatic_works_from_library(structural, client)

        self.assertEqual(len(works), 1)
        self.assertEqual(works[0]["tmdb_id"], 903)
        self.assertEqual(works[0]["identity_scope"], {
            "kind": "tv_metadata_source",
            "metadata_source": show,
            "media_paths": [season],
        })

    def test_ambiguous_empty_movie_nfos_remain_unknown_not_green(self) -> None:
        movie_root, anime_root, us_root = DEFAULT_FORMAL_LIBRARY_ROOTS
        bundle = f"{movie_root}/Empty Bundle"
        first_nfo = f"{bundle}/First.nfo"
        second_nfo = f"{bundle}/Second.nfo"
        client = NfoTreeAList({
            movie_root: [{"name": "Empty Bundle", "is_dir": True}],
            bundle: [
                {"name": "First.nfo", "is_dir": False, "size": 80},
                {"name": "Second.nfo", "is_dir": False, "size": 80},
            ],
            anime_root: [],
            us_root: [],
        }, {
            first_nfo: b"<movie><title>First</title><tmdbid>101</tmdbid></movie>",
            second_nfo: b"<movie><title>Second</title><tmdbid>102</tmdbid></movie>",
        })

        with tempfile.TemporaryDirectory() as tmp:
            report = run_automatic_library_audit(
                client,
                tmp,
                [],
                formal_roots=DEFAULT_FORMAL_LIBRARY_ROOTS,
            )

        self.assertFalse(report["library_complete"])
        self.assertEqual(report["semantic"]["gap_count"], 0)
        self.assertEqual(report["semantic"]["acquisition_projects"], [])
        self.assertEqual(report["semantic"]["bootstrap"]["nfo_work_count"], 2)
        self.assertEqual(
            [row["kind"] for row in report["semantic"]["unknowns"]],
            ["unknown_identity_scope", "unknown_identity_scope"],
        )

    def test_movie_nfos_on_tv_shelf_use_file_scopes_and_leave_unpaired_video_unknown(self) -> None:
        movie_root, anime_root, us_root = DEFAULT_FORMAL_LIBRARY_ROOTS
        bundle = f"{anime_root}/Mixed Movies"
        client = NfoTreeAList({
            movie_root: [],
            anime_root: [{"name": "Mixed Movies", "is_dir": True}],
            bundle: [
                {"name": "Alpha.mkv", "is_dir": False, "size": 10},
                {"name": "Alpha.nfo", "is_dir": False, "size": 80},
                {"name": "Beta.mkv", "is_dir": False, "size": 10},
                {"name": "Beta.nfo", "is_dir": False, "size": 80},
                {"name": "Unpaired.mkv", "is_dir": False, "size": 10},
            ],
            us_root: [],
        }, {
            f"{bundle}/Alpha.nfo": b"<movie><title>Alpha</title><tmdbid>101</tmdbid></movie>",
            f"{bundle}/Beta.nfo": b"<movie><title>Beta</title><tmdbid>102</tmdbid></movie>",
        })
        report = SimpleLibraryAuditor(client).scan()
        works = bootstrap_automatic_works_from_library(report, client)

        scopes = {
            row["tmdb_id"]: row["identity_scope"]
            for row in works
        }
        self.assertEqual(scopes, {
            101: {"kind": "video_file", "video_path": f"{bundle}/Alpha.mkv"},
            102: {"kind": "video_file", "video_path": f"{bundle}/Beta.mkv"},
        })
        semantic = build_automatic_library_gaps(report, works)
        unknown = [
            row for row in semantic["unknowns"]
            if row.get("kind") == "unknown_library_work"
        ]
        self.assertEqual(len(unknown), 1)
        self.assertEqual(unknown[0]["path"], bundle)
        self.assertEqual(unknown[0]["uncovered_video_paths"], [f"{bundle}/Unpaired.mkv"])

    def test_conflicting_legacy_job_cannot_claim_a_mixed_nfo_bundle(self) -> None:
        movie_root, anime_root, us_root = DEFAULT_FORMAL_LIBRARY_ROOTS
        bundle = f"{anime_root}/Mixed Legacy"
        nfo_path = f"{bundle}/Alpha.nfo"
        client = NfoTreeAList({
            movie_root: [],
            anime_root: [{"name": "Mixed Legacy", "is_dir": True}],
            bundle: [
                {"name": "Alpha.mkv", "is_dir": False, "size": 10},
                {"name": "Alpha.nfo", "is_dir": False, "size": 80},
                {"name": "Unpaired.mkv", "is_dir": False, "size": 10},
                {"name": "poster.jpg", "is_dir": False, "size": 2},
            ],
            us_root: [],
        }, {
            nfo_path: b"<movie><title>Alpha</title><tmdbid>101</tmdbid></movie>",
        })
        legacy_job = {
            "id": "legacy-bundle-job",
            "phase": "completed",
            "plan": {
                "mode": "movie",
                "target_root": bundle,
                "metadata": {"tmdb_id": 999, "title": "Legacy Bundle"},
            },
            "summary": {
                "identity": {
                    "tmdb_id": 999,
                    "media_type": "movie",
                    "title": "Legacy Bundle",
                },
            },
        }

        with tempfile.TemporaryDirectory() as tmp:
            report = run_automatic_library_audit(
                client,
                tmp,
                [legacy_job],
                formal_roots=DEFAULT_FORMAL_LIBRARY_ROOTS,
            )

        unknown_kinds = [row["kind"] for row in report["semantic"]["unknowns"]]
        self.assertIn("unknown_legacy_identity_scope", unknown_kinds)
        self.assertIn("unknown_library_work", unknown_kinds)
        self.assertEqual(report["semantic"]["acquisition_projects"], [])
        self.assertFalse(report["library_complete"])

    def test_matching_audit_owned_job_still_merges_with_nfo_scope(self) -> None:
        movie_root, anime_root, us_root = DEFAULT_FORMAL_LIBRARY_ROOTS
        movie = f"{movie_root}/Movie (2020)"
        nfo_path = f"{movie}/movie.nfo"
        client = NfoTreeAList({
            movie_root: [{"name": "Movie (2020)", "is_dir": True}],
            movie: [
                {"name": "Movie.mkv", "is_dir": False, "size": 10},
                {"name": "movie.nfo", "is_dir": False, "size": 80},
                {"name": "poster.jpg", "is_dir": False, "size": 2},
            ],
            anime_root: [],
            us_root: [],
        }, {
            nfo_path: b"<movie><title>Movie</title><tmdbid>8</tmdbid></movie>",
        })
        audit_owned_job = {
            "id": "audit-owned-movie",
            "phase": "executed",
            "plan": {
                "mode": "movie",
                "target_root": movie,
                "metadata": {"tmdb_id": 8, "title": "Movie"},
            },
            "summary": {
                "audit_owned": True,
                "identity": {"tmdb_id": 8, "media_type": "movie", "title": "Movie"},
            },
        }

        with tempfile.TemporaryDirectory() as tmp:
            report = run_automatic_library_audit(
                client,
                tmp,
                [audit_owned_job],
                formal_roots=DEFAULT_FORMAL_LIBRARY_ROOTS,
            )

        self.assertTrue(report["library_complete"])
        self.assertFalse(any(
            row["kind"] == "unknown_legacy_identity_scope"
            for row in report["semantic"]["unknowns"]
        ))

    def test_tv_scope_stops_at_nested_show_and_excludes_direct_movie_files(self) -> None:
        movie_root, anime_root, us_root = DEFAULT_FORMAL_LIBRARY_ROOTS
        show = f"{anime_root}/Mixed Show"
        parent_season = f"{show}/第1季"
        child = f"{show}/Nested Show"
        child_season = f"{child}/Season 01"
        client = NfoTreeAList({
            movie_root: [],
            anime_root: [{"name": "Mixed Show", "is_dir": True}],
            show: [
                {"name": "tvshow.nfo", "is_dir": False, "size": 80},
                {"name": "Film.mkv", "is_dir": False, "size": 10},
                {"name": "Film.nfo", "is_dir": False, "size": 80},
                {"name": "Unpaired.mkv", "is_dir": False, "size": 10},
                {"name": "第1季", "is_dir": True},
                {"name": "Nested Show", "is_dir": True},
            ],
            parent_season: [{"name": "Mixed Show S01E01.mkv", "is_dir": False, "size": 10}],
            child: [
                {"name": "tvshow.nfo", "is_dir": False, "size": 80},
                {"name": "Season 01", "is_dir": True},
            ],
            child_season: [{"name": "Nested Show S01E01.mkv", "is_dir": False, "size": 10}],
            us_root: [],
        }, {
            f"{show}/tvshow.nfo": b"<tvshow><title>Mixed Show</title><tmdbid>201</tmdbid></tvshow>",
            f"{show}/Film.nfo": b"<movie><title>Film</title><tmdbid>202</tmdbid></movie>",
            f"{child}/tvshow.nfo": b"<tvshow><title>Nested Show</title><tmdbid>203</tmdbid></tvshow>",
        })
        report = SimpleLibraryAuditor(client).scan()
        works = bootstrap_automatic_works_from_library(report, client)
        by_id = {row["tmdb_id"]: row for row in works}
        self.assertEqual(set(by_id), {201, 202, 203})
        self.assertEqual(
            by_id[201]["identity_scope"]["media_paths"],
            [parent_season],
        )
        self.assertEqual(
            by_id[202]["identity_scope"],
            {"kind": "video_file", "video_path": f"{show}/Film.mkv"},
        )
        self.assertEqual(
            by_id[203]["identity_scope"]["media_paths"],
            [child_season],
        )

        semantic = build_automatic_library_gaps(report, [
            {**by_id[201], "expected_episodes": {1: [1]}},
            by_id[202],
            {**by_id[203], "expected_episodes": {1: [1]}},
        ])
        unknown = [
            row for row in semantic["unknowns"]
            if row.get("kind") == "unknown_library_work"
        ]
        self.assertEqual(
            [(row["path"], row["uncovered_video_paths"]) for row in unknown],
            [(show, [f"{show}/Unpaired.mkv"])],
        )

    def test_parent_tv_scope_keeps_duplicate_movie_file_scopes_in_nested_bundle(self) -> None:
        """A TV parent must not swallow its exact-stem movie copies."""
        movie_root, anime_root, us_root = DEFAULT_FORMAL_LIBRARY_ROOTS
        show = f"{anime_root}/Black Butler"
        season = f"{show}/Season 01"
        movie = f"{show}/Black Butler Movie (2017)"
        parent_video = f"{show}/Black Butler Movie (2017).mkv"
        parent_nfo = f"{show}/Black Butler Movie (2017).nfo"
        child_video = f"{movie}/Black Butler Movie (2017).mp4"
        child_nfo = f"{movie}/Black Butler Movie (2017).nfo"
        client = NfoTreeAList({
            movie_root: [],
            anime_root: [{"name": "Black Butler", "is_dir": True}],
            show: [
                {"name": "tvshow.nfo", "is_dir": False, "size": 80},
                {"name": "Black Butler Movie (2017).mkv", "is_dir": False, "size": 10},
                {"name": "Black Butler Movie (2017).nfo", "is_dir": False, "size": 80},
                {"name": "poster.jpg", "is_dir": False, "size": 2},
                {"name": "Season 01", "is_dir": True},
                {"name": "Black Butler Movie (2017)", "is_dir": True},
            ],
            season: [{"name": "Black Butler S01E01.mkv", "is_dir": False, "size": 10}],
            movie: [
                {"name": "Black Butler Movie (2017).mp4", "is_dir": False, "size": 10},
                {"name": "Black Butler Movie (2017).nfo", "is_dir": False, "size": 80},
                {"name": "poster.jpg", "is_dir": False, "size": 2},
            ],
            us_root: [],
        }, {
            f"{show}/tvshow.nfo": b"<tvshow><title>Black Butler</title><tmdbid>50712</tmdbid></tvshow>",
            parent_nfo: b"<movie><title>Black Butler Movie</title><tmdbid>432131</tmdbid></movie>",
            child_nfo: b"<movie><title>Black Butler Movie</title><tmdbid>432131</tmdbid></movie>",
        })

        report = SimpleLibraryAuditor(client).scan()
        works = bootstrap_automatic_works_from_library(report, client)
        movie_works = [row for row in works if row["tmdb_id"] == 432131]
        self.assertEqual(
            {row["identity_scope"]["video_path"] for row in movie_works},
            {parent_video, child_video},
        )

        semantic = build_automatic_library_gaps(
            report,
            [
                (
                    {**row, "expected_episodes": {1: [1]}}
                    if row["media_type"] == "tv" else row
                )
                for row in works
            ],
        )
        self.assertEqual(semantic["gaps"], [])
        self.assertEqual(semantic["unknowns"], [])

    def test_direct_tv_episode_scope_excludes_a_same_directory_movie_file_scope(self) -> None:
        movie_root, anime_root, us_root = DEFAULT_FORMAL_LIBRARY_ROOTS
        show = f"{anime_root}/Mixed Direct"
        episode = f"{show}/Mixed Direct S01E01.mkv"
        movie_video = f"{show}/Film (2020).mkv"
        movie_nfo = f"{show}/Film (2020).nfo"
        client = NfoTreeAList({
            movie_root: [],
            anime_root: [{"name": "Mixed Direct", "is_dir": True}],
            show: [
                {"name": "tvshow.nfo", "is_dir": False, "size": 80},
                {"name": "Mixed Direct S01E01.mkv", "is_dir": False, "size": 10},
                {"name": "Film (2020).mkv", "is_dir": False, "size": 10},
                {"name": "Film (2020).nfo", "is_dir": False, "size": 80},
                {"name": "poster.jpg", "is_dir": False, "size": 2},
            ],
            us_root: [],
        }, {
            f"{show}/tvshow.nfo": b"<tvshow><title>Mixed Direct</title><tmdbid>701</tmdbid></tvshow>",
            movie_nfo: b"<movie><title>Film</title><tmdbid>702</tmdbid></movie>",
        })

        report = SimpleLibraryAuditor(client).scan()
        works = bootstrap_automatic_works_from_library(report, client)
        by_id = {row["tmdb_id"]: row for row in works}
        self.assertEqual(by_id[701]["identity_scope"]["video_paths"], [episode])
        self.assertEqual(
            by_id[702]["identity_scope"],
            {"kind": "video_file", "video_path": movie_video},
        )
        semantic = build_automatic_library_gaps(
            report,
            [
                {**row, "expected_episodes": {1: [1]}}
                if row["tmdb_id"] == 701 else row
                for row in works
            ],
        )
        self.assertEqual(semantic["gaps"], [])
        self.assertEqual(semantic["unknowns"], [])

    def test_parent_movie_nfo_does_not_cross_a_child_tvshow_boundary(self) -> None:
        """An exact stem is not enough to cross a nested TV identity boundary."""
        movie_root, anime_root, us_root = DEFAULT_FORMAL_LIBRARY_ROOTS
        bundle = f"{anime_root}/Fate"
        show = f"{bundle}/Strange Fake"
        season = f"{show}/Season 01"
        stem = "Strange Fake Dawn (2023)"
        parent_video = f"{bundle}/{stem}.mkv"
        parent_nfo = f"{bundle}/{stem}.nfo"
        child_video = f"{show}/{stem}.mkv"
        client = NfoTreeAList({
            movie_root: [],
            anime_root: [{"name": "Fate", "is_dir": True}],
            bundle: [
                {"name": f"{stem}.mkv", "is_dir": False, "size": 10},
                {"name": f"{stem}.nfo", "is_dir": False, "size": 80},
                {"name": "poster.jpg", "is_dir": False, "size": 2},
                {"name": "Strange Fake", "is_dir": True},
            ],
            show: [
                {"name": "tvshow.nfo", "is_dir": False, "size": 80},
                {"name": f"{stem}.mkv", "is_dir": False, "size": 10},
                {"name": "poster.jpg", "is_dir": False, "size": 2},
                {"name": "Season 01", "is_dir": True},
            ],
            season: [
                {"name": "Strange Fake S01E01.mkv", "is_dir": False, "size": 10},
                {"name": "poster.jpg", "is_dir": False, "size": 2},
            ],
            us_root: [],
        }, {
            parent_nfo: b"<movie><title>Strange Fake Dawn</title><tmdbid>1145612</tmdbid></movie>",
            f"{show}/tvshow.nfo": b"<tvshow><title>Strange Fake</title><tmdbid>229858</tmdbid></tvshow>",
        })

        report = SimpleLibraryAuditor(client).scan()
        works = bootstrap_automatic_works_from_library(report, client)
        by_id = {row["tmdb_id"]: row for row in works}
        self.assertEqual(
            by_id[1145612]["identity_scope"],
            {"kind": "video_file", "video_path": parent_video},
        )
        semantic = build_automatic_library_gaps(
            report,
            [
                {**row, "expected_episodes": {1: [1]}}
                if row["tmdb_id"] == 229858 else row
                for row in works
            ],
        )
        self.assertEqual(semantic["gaps"], [])
        child_unknowns = [
            row for row in semantic["unknowns"]
            if row.get("kind") == "unknown_library_work"
        ]
        self.assertEqual(
            child_unknowns,
            [{
                "kind": "unknown_library_work",
                "path": show,
                "reason": "正式库作品目录没有对应的自动任务身份",
                "uncovered_video_paths": [child_video],
            }],
        )
        self.assertEqual(semantic["acquisition_projects"], [])

    def test_oshi_direct_behind_the_scenes_is_observed_not_an_unknown_work(self) -> None:
        """A direct, explicitly labelled Oshi extra is observation-only."""
        movie_root, anime_root, us_root = DEFAULT_FORMAL_LIBRARY_ROOTS
        show = f"{anime_root}/【我推的孩子】"
        season = f"{show}/Season 01"
        extras = [
            f"{show}/【我推的孩子】-behindthescenes.mkv",
            f"{show}/【我推的孩子】-behindthescenes2.mkv",
        ]
        client = NfoTreeAList({
            movie_root: [],
            anime_root: [{"name": "【我推的孩子】", "is_dir": True}],
            show: [
                {"name": "tvshow.nfo", "is_dir": False, "size": 80},
                {"name": "poster.jpg", "is_dir": False, "size": 2},
                {"name": "Season 01", "is_dir": True},
                {"name": "【我推的孩子】-behindthescenes.mkv", "is_dir": False, "size": 10},
                {"name": "【我推的孩子】-behindthescenes2.mkv", "is_dir": False, "size": 10},
            ],
            season: [{"name": "Oshi no Ko S01E01.mkv", "is_dir": False, "size": 10}],
            us_root: [],
        }, {
            f"{show}/tvshow.nfo": (
                b"<tvshow><title>Oshi no Ko</title><tmdbid>203737</tmdbid></tvshow>"
            ),
        })

        structural = SimpleLibraryAuditor(client).scan()
        works = bootstrap_automatic_works_from_library(structural, client)
        report = attach_automatic_gaps(
            structural,
            [{**works[0], "expected_episodes": {1: [1]}}],
        )

        self.assertEqual(
            report["observations"]["ancillary_media"],
            [
                {
                    "path": extras[0],
                    "target_root": show,
                    "tmdb_id": 203737,
                    "kind": "behind_the_scenes",
                    "source": "explicit_ancillary_filename",
                },
                {
                    "path": extras[1],
                    "target_root": show,
                    "tmdb_id": 203737,
                    "kind": "behind_the_scenes",
                    "source": "explicit_ancillary_filename",
                },
            ],
        )
        self.assertEqual(report["semantic"]["ancillary_media"], report["observations"]["ancillary_media"])
        self.assertEqual(report["semantic"]["gaps"], [])
        self.assertEqual(report["semantic"]["unknowns"], [])
        self.assertEqual(report["semantic"]["acquisition_projects"], [])

    def test_slime_direct_trailers_are_observed_without_claiming_movie_scopes(self) -> None:
        """Direct trailer markers stay non-actionable beside Slime movie NFOs."""
        movie_root, anime_root, us_root = DEFAULT_FORMAL_LIBRARY_ROOTS
        show = f"{anime_root}/关于我转生变成史莱姆这档事"
        season = f"{show}/Season 01"
        child = f"{show}/Nested Child"
        child_season = f"{child}/Season 01"
        scarlet_stem = "关于我转生变成史莱姆这档事：红莲之绊篇 (2022)"
        azure_stem = "关于我转生变成史莱姆这档事：苍海之泪篇 (2024)"
        trailers = [
            f"{show}/关于我转生变成史莱姆这档事-trailer.mkv",
            f"{show}/关于我转生变成史莱姆这档事-trailer2.mkv",
        ]
        child_trailer = f"{child}/Nested Child-trailer.mkv"
        client = NfoTreeAList({
            movie_root: [],
            anime_root: [{"name": "关于我转生变成史莱姆这档事", "is_dir": True}],
            show: [
                {"name": "tvshow.nfo", "is_dir": False, "size": 80},
                {"name": "poster.jpg", "is_dir": False, "size": 2},
                {"name": "Season 01", "is_dir": True},
                {"name": "Nested Child", "is_dir": True},
                {"name": f"{scarlet_stem}.mkv", "is_dir": False, "size": 10},
                {"name": f"{scarlet_stem}.nfo", "is_dir": False, "size": 80},
                {"name": f"{azure_stem}.mp4", "is_dir": False, "size": 10},
                {"name": f"{azure_stem}.nfo", "is_dir": False, "size": 80},
                {"name": "关于我转生变成史莱姆这档事-trailer.mkv", "is_dir": False, "size": 10},
                {"name": "关于我转生变成史莱姆这档事-trailer2.mkv", "is_dir": False, "size": 10},
            ],
            season: [{"name": "Slime S01E01.mkv", "is_dir": False, "size": 10}],
            child: [
                {"name": "tvshow.nfo", "is_dir": False, "size": 80},
                {"name": "poster.jpg", "is_dir": False, "size": 2},
                {"name": "Season 01", "is_dir": True},
                {"name": "Nested Child-trailer.mkv", "is_dir": False, "size": 10},
            ],
            child_season: [{"name": "Nested Child S01E01.mkv", "is_dir": False, "size": 10}],
            us_root: [],
        }, {
            f"{show}/tvshow.nfo": (
                b"<tvshow><title>Slime</title><tmdbid>82684</tmdbid></tvshow>"
            ),
            f"{show}/{scarlet_stem}.nfo": (
                b"<movie><title>Scarlet Bond</title><tmdbid>876792</tmdbid></movie>"
            ),
            f"{show}/{azure_stem}.nfo": (
                b"<movie><title>Azure Tear</title><tmdbid>1363974</tmdbid></movie>"
            ),
            f"{child}/tvshow.nfo": (
                b"<tvshow><title>Nested Child</title><tmdbid>999001</tmdbid></tvshow>"
            ),
        })

        structural = SimpleLibraryAuditor(client).scan()
        works = bootstrap_automatic_works_from_library(structural, client)
        by_id = {row["tmdb_id"]: row for row in works}
        report = attach_automatic_gaps(
            structural,
            [
                {**by_id[82684], "expected_episodes": {1: [1]}},
                {**by_id[999001], "expected_episodes": {1: [1]}},
                by_id[876792],
                by_id[1363974],
            ],
        )

        ancillary = report["observations"]["ancillary_media"]
        self.assertEqual(
            {
                (row["path"], row["target_root"], row["kind"])
                for row in ancillary
            },
            {
                (trailers[0], show, "trailer"),
                (trailers[1], show, "trailer"),
                (child_trailer, child, "trailer"),
            },
        )
        # The nested file is observed only under its own direct TV identity,
        # never attributed to the parent show.
        self.assertNotIn((child_trailer, show), {
            (row["path"], row["target_root"]) for row in ancillary
        })
        self.assertEqual(report["semantic"]["gaps"], [])
        self.assertEqual(report["semantic"]["unknowns"], [])
        self.assertEqual(report["semantic"]["acquisition_projects"], [])

    def test_invalid_existing_nfo_stays_unknown_and_never_creates_an_owner(self) -> None:
        movie_root, anime_root, us_root = DEFAULT_FORMAL_LIBRARY_ROOTS
        movie = f"{movie_root}/Ambiguous"
        nfo_path = f"{movie}/movie.nfo"
        client = NfoTreeAList({
            movie_root: [{"name": "Ambiguous", "is_dir": True}],
            movie: [
                {"name": "Movie.mkv", "is_dir": False, "size": 10},
                {"name": "movie.nfo", "is_dir": False, "size": 80},
                {"name": "poster.jpg", "is_dir": False, "size": 2},
            ],
            anime_root: [],
            us_root: [],
        }, {
            nfo_path: (
                b"<movie><tmdbid>8</tmdbid>"
                b"<uniqueid type='tmdb'>9</uniqueid></movie>"
            ),
        })

        with tempfile.TemporaryDirectory() as tmp:
            report = run_automatic_library_audit(
                client,
                tmp,
                [],
                formal_roots=DEFAULT_FORMAL_LIBRARY_ROOTS,
            )

        self.assertTrue(report["complete"])
        self.assertFalse(report["library_complete"])
        self.assertTrue(any(
            row["kind"] == "unknown_library_work" and row["path"] == movie
            for row in report["semantic"]["unknowns"]
        ))
        self.assertEqual(report["semantic"]["bootstrap"]["nfo_work_count"], 0)
        self.assertFalse(report["semantic"]["bootstrap"]["creates_owner_tasks"])

    def test_parent_tvshow_nfo_cannot_hide_a_nested_independent_show(self) -> None:
        movie_root, anime_root, us_root = DEFAULT_FORMAL_LIBRARY_ROOTS
        bundle = f"{anime_root}/Bundle"
        show = f"{bundle}/Show"
        season = f"{show}/Season 01"
        client = NfoTreeAList({
            movie_root: [],
            anime_root: [{"name": "Bundle", "is_dir": True}],
            bundle: [
                {"name": "tvshow.nfo", "is_dir": False, "size": 80},
                {"name": "Show", "is_dir": True},
            ],
            show: [
                {"name": "tvshow.nfo", "is_dir": False, "size": 80},
                {"name": "Season 01", "is_dir": True},
            ],
            season: [{"name": "Show S01E01.mkv", "is_dir": False, "size": 10}],
            us_root: [],
        }, {
            f"{bundle}/tvshow.nfo": b"<tvshow><tmdbid>1</tmdbid></tvshow>",
            f"{show}/tvshow.nfo": b"<tvshow><tmdbid>2</tmdbid></tvshow>",
        })

        report = SimpleLibraryAuditor(client).scan()
        works = bootstrap_automatic_works_from_library(report, client)

        by_id = {row["tmdb_id"]: row for row in works}
        self.assertEqual(set(by_id), {1, 2})
        self.assertEqual(
            by_id[1]["identity_scope"],
            {"kind": "ambiguous_tv_container"},
        )
        self.assertEqual(by_id[2]["target_root"], show)

    def test_persisted_job_identity_covers_a_library_directory_without_an_nfo(self) -> None:
        movie_root, anime_root, us_root = DEFAULT_FORMAL_LIBRARY_ROOTS
        movie = f"{movie_root}/Movie (2020)"
        client = TreeAList({
            movie_root: [{"name": "Movie (2020)", "is_dir": True}],
            movie: [
                {"name": "Movie.mkv", "is_dir": False, "size": 10},
                {"name": "poster.jpg", "is_dir": False, "size": 2},
            ],
            anime_root: [],
            us_root: [],
        })
        jobs = [{
            "id": "engine-movie",
            "phase": "completed",
            "plan": {
                "mode": "movie",
                "target_root": movie,
                "metadata": {"tmdb_id": 8, "title": "Movie", "year": "2020"},
            },
            "summary": {"identity": {"tmdb_id": 8, "media_type": "movie", "title": "Movie"}},
        }]

        with tempfile.TemporaryDirectory() as tmp:
            report = run_automatic_library_audit(
                client,
                tmp,
                jobs,
                formal_roots=DEFAULT_FORMAL_LIBRARY_ROOTS,
            )

        self.assertFalse(any(
            row["kind"] == "unknown_library_work" for row in report["semantic"]["unknowns"]
        ))
        self.assertTrue(any(row["kind"] == "missing_nfo" for row in report["semantic"]["gaps"]))
        self.assertEqual(report["semantic"]["bootstrap"]["nfo_work_count"], 0)
        self.assertEqual(report["semantic"]["bootstrap"]["unowned_gap_count"], 0)

    def test_inventory_completion_is_not_business_completion_while_jobs_are_open(self) -> None:
        roots = DEFAULT_FORMAL_LIBRARY_ROOTS
        client = TreeAList({root: [] for root in roots})
        jobs = [{
            "id": "engine-open",
            "phase": "retry_wait",
            "request": {"source_path": "/quark/影视/待刮削/Open"},
        }]

        with tempfile.TemporaryDirectory() as tmp:
            report = run_automatic_library_audit(client, tmp, jobs, formal_roots=roots)

        self.assertTrue(report["complete"])
        self.assertTrue(report["semantic"]["complete"])
        self.assertEqual(report["semantic"]["gap_count"], 0)
        self.assertEqual(report["semantic"]["unknown_count"], 0)
        self.assertEqual(report["semantic"]["job_gap_count"], 1)
        self.assertFalse(report["clean"])
        self.assertFalse(report["library_complete"])

    def test_tmdb_catalog_excludes_unreleased_episodes(self) -> None:
        class FakeTMDB:
            def get(self, path: str) -> dict[str, object]:
                if path == "/tv/7":
                    return {"seasons": [{"season_number": 1}]}
                return {"episodes": [
                    {"episode_number": 1, "air_date": "2020-01-01", "name": "Past"},
                    {"episode_number": 2, "air_date": "2099-01-01", "name": "Future"},
                ]}

        catalog = TmdbEpisodeCatalog(FakeTMDB(), today=lambda: date(2026, 8, 7))
        rows = catalog({"tmdb_id": 7, "media_type": "tv"})
        self.assertEqual(rows, {1: [{"season_number": 1, "episode_number": 1,
                                    "name": "Past", "air_date": "2020-01-01"}]})

    def test_tmdb_s00_localized_titles_flow_into_semantic_gap_aliases(self) -> None:
        class LocalizedTMDB:
            def __init__(self) -> None:
                self.calls: list[tuple[str, str | None]] = []

            def get(self, path: str, **params: object) -> dict[str, object]:
                language = params.get("language")
                self.calls.append((path, language if isinstance(language, str) else None))
                if path == "/tv/100":
                    return {"seasons": [{"season_number": 0}]}
                if path != "/tv/100/season/0":
                    raise AssertionError(f"unexpected TMDB path: {path}")
                if language is None:
                    return {"episodes": [{
                        "season_number": 0,
                        "episode_number": 5,
                        "air_date": "2020-01-01",
                        "name": "黎明特别篇",
                    }]}
                if language == "en-US":
                    return {"episodes": [
                        {
                            "season_number": 0,
                            "episode_number": 5,
                            # Localized metadata must not change the primary
                            # row's authoritative air date.
                            "air_date": "2099-01-01",
                            "name": "Dawn OVA",
                        },
                        {
                            "season_number": 1,
                            "episode_number": 5,
                            "name": "Wrong Season",
                        },
                    ]}
                if language == "ja-JP":
                    return {"episodes": [{
                        "season_number": 0,
                        "episode_number": 5,
                        "name": "暁のOVA",
                    }]}
                raise AssertionError(f"unexpected TMDB language: {language}")

        tmdb = LocalizedTMDB()
        catalog = TmdbEpisodeCatalog(tmdb, today=lambda: date(2026, 8, 7))
        work = {
            "tmdb_id": 100,
            "title": "Show",
            "media_type": "tv",
            "target_root": f"{DEFAULT_FORMAL_LIBRARY_ROOTS[1]}/Show",
        }
        self.assertEqual(catalog(work), {0: [{
            "season_number": 0,
            "episode_number": 5,
            "name": "黎明特别篇",
            "air_date": "2020-01-01",
            "title_aliases": ["Dawn OVA", "暁のOVA"],
        }]})
        self.assertEqual(tmdb.calls, [
            ("/tv/100", None),
            ("/tv/100/season/0", None),
            ("/tv/100/season/0", "en-US"),
            ("/tv/100/season/0", "ja-JP"),
        ])

        semantic = build_automatic_library_gaps(
            _report(), [work], episode_catalog=catalog,
        )
        gap = next(row for row in semantic["gaps"] if row["kind"] == "missing_episode")
        self.assertEqual(gap["title"], "黎明特别篇")
        self.assertEqual(gap["title_aliases"], ["Dawn OVA", "暁のOVA"])
        # The cached catalog is reused by the semantic pass.
        self.assertEqual(len(tmdb.calls), 4)

    def test_tmdb_s00_optional_language_failures_keep_primary_catalog_usable(self) -> None:
        class FailingLocalizedTMDB:
            def __init__(self) -> None:
                self.calls: list[tuple[str, str | None]] = []

            def get(self, path: str, **params: object) -> dict[str, object]:
                language = params.get("language")
                language_name = language if isinstance(language, str) else None
                self.calls.append((path, language_name))
                if path == "/tv/101":
                    return {"seasons": [{"season_number": 0}, {"season_number": 1}]}
                if language_name is not None:
                    raise RuntimeError("localized TMDB endpoint unavailable")
                if path == "/tv/101/season/0":
                    return {"episodes": [{
                        "episode_number": 1,
                        "air_date": "2020-01-01",
                        "name": "中文特别篇",
                    }]}
                if path == "/tv/101/season/1":
                    return {"episodes": [{
                        "episode_number": 1,
                        "air_date": "2020-01-01",
                        "name": "普通集",
                    }]}
                raise AssertionError(f"unexpected TMDB path: {path}")

        tmdb = FailingLocalizedTMDB()
        catalog = TmdbEpisodeCatalog(tmdb, today=lambda: date(2026, 8, 7))
        work = {
            "tmdb_id": 101,
            "title": "Show",
            "media_type": "tv",
            "target_root": f"{DEFAULT_FORMAL_LIBRARY_ROOTS[1]}/Show",
        }
        rows = catalog(work)
        self.assertEqual(rows, {
            0: [{
                "season_number": 0,
                "episode_number": 1,
                "name": "中文特别篇",
                "air_date": "2020-01-01",
            }],
            1: [{
                "season_number": 1,
                "episode_number": 1,
                "name": "普通集",
                "air_date": "2020-01-01",
            }],
        })
        self.assertEqual(
            [path for path, language in tmdb.calls if language is not None],
            ["/tv/101/season/0", "/tv/101/season/0"],
        )
        semantic = build_automatic_library_gaps(
            _report(), [work], episode_catalog=catalog,
        )
        self.assertFalse(any(
            row["kind"] == "unknown_episode_catalog" and row["work"] == "tmdb:101"
            for row in semantic["unknowns"]
        ))

    def test_tmdb_s00_legacy_client_without_language_kwarg_keeps_primary_rows(self) -> None:
        class LegacyTMDB:
            def __init__(self) -> None:
                self.calls: list[str] = []

            def get(self, path: str) -> dict[str, object]:
                self.calls.append(path)
                if path == "/tv/102":
                    return {"seasons": [{"season_number": 0}]}
                return {"episodes": [{
                    "episode_number": 3,
                    "air_date": "2020-01-01",
                    "name": "仅主标题",
                }]}

        tmdb = LegacyTMDB()
        catalog = TmdbEpisodeCatalog(tmdb, today=lambda: date(2026, 8, 7))
        self.assertEqual(catalog({"tmdb_id": 102, "media_type": "tv"}), {0: [{
            "season_number": 0,
            "episode_number": 3,
            "name": "仅主标题",
            "air_date": "2020-01-01",
        }]})
        # The attempted localized keyword calls raise before this legacy
        # method executes; its primary catalog remains intact.
        self.assertEqual(tmdb.calls, ["/tv/102", "/tv/102/season/0"])

    def test_catalog_episode_titles_are_search_evidence_without_changing_ids(self) -> None:
        """Season 00 titles survive to the provider request, never to coverage."""
        report = _report()
        show = f"{DEFAULT_FORMAL_LIBRARY_ROOTS[1]}/Show"
        catalog_row = {
            "season_number": 0,
            "episode_number": 5,
            "name": "Dawn OVA",
            "aliases": ["黎明特别篇", "Dawn OVA"],
            "air_date": "2020-01-01",
        }
        title_evidence: dict[tuple[int, int], list[str]] = {}
        self.assertEqual(
            _normalise_expected_episodes({0: [catalog_row]}),
            _normalise_expected_episodes(
                {0: [catalog_row]}, title_evidence=title_evidence,
            ),
        )
        self.assertEqual(title_evidence, {(0, 5): ["Dawn OVA", "黎明特别篇"]})

        work = {
            "tmdb_id": 100,
            "title": "Show",
            "media_type": "tv",
            "target_root": show,
        }
        unnamed = build_automatic_library_gaps(
            report,
            [work],
            episode_catalog={100: {0: [{
                "season_number": 0,
                "episode_number": 5,
                "air_date": "2020-01-01",
            }]}},
        )
        named = build_automatic_library_gaps(
            report,
            [work],
            episode_catalog={100: {0: [catalog_row]}},
        )
        unnamed_gap = next(row for row in unnamed["gaps"] if row["kind"] == "missing_episode")
        gap = next(row for row in named["gaps"] if row["kind"] == "missing_episode")
        self.assertEqual(
            (gap["id"], gap["label"], gap["season"], gap["episode"]),
            (unnamed_gap["id"], unnamed_gap["label"], 0, 5),
        )
        self.assertEqual(gap["title"], "Dawn OVA")
        self.assertEqual(gap["title_aliases"], ["黎明特别篇"])

        project = named["acquisition_projects"][0]
        request = build_replenishment_request(
            project["plan"], job_id="audit-title-evidence", round_number=1,
        )
        self.assertEqual(request["gaps"][0]["id"], "S00E05")
        self.assertEqual(request["gaps"][0]["title"], "Dawn OVA")
        self.assertEqual(request["gaps"][0]["title_aliases"], ["黎明特别篇"])
        self.assertEqual(
            request["query_groups"][0]["episode_titles"],
            ["Dawn OVA", "黎明特别篇"],
        )
        self.assertIn("Show Dawn OVA", request["search_queries"])

    def test_tmdb_catalog_caches_success_and_failure_for_one_audit(self) -> None:
        class CountingTMDB:
            def __init__(self) -> None:
                self.calls: list[str] = []

            def get(self, path: str) -> dict[str, object]:
                self.calls.append(path)
                if path == "/tv/7":
                    return {"seasons": [{"season_number": 1}]}
                return {"episodes": [{"episode_number": 1, "air_date": "2020-01-01"}]}

        client = CountingTMDB()
        catalog = TmdbEpisodeCatalog(client, today=lambda: date(2026, 8, 7))
        first = catalog({"tmdb_id": 7, "media_type": "tv"})
        second = catalog({"tmdb_id": 7, "media_type": "tv"})
        self.assertEqual(first, second)
        self.assertEqual(client.calls, ["/tv/7", "/tv/7/season/1"])

        class FailingTMDB:
            def __init__(self) -> None:
                self.calls = 0

            def get(self, _path: str) -> dict[str, object]:
                self.calls += 1
                raise RuntimeError("TMDB unavailable")

        failing = FailingTMDB()
        failed_catalog = TmdbEpisodeCatalog(failing, today=lambda: date(2026, 8, 7))
        self.assertIsNone(failed_catalog({"tmdb_id": 8, "media_type": "tv"}))
        self.assertIsNone(failed_catalog({"tmdb_id": 8, "media_type": "tv"}))
        self.assertEqual(failing.calls, 1)

    def test_tmdb_catalog_prefetches_only_unique_tv_ids(self) -> None:
        class CountingTMDB:
            def __init__(self) -> None:
                self.calls: list[str] = []

            def get(self, path: str) -> dict[str, object]:
                self.calls.append(path)
                return {"seasons": []} if path.startswith("/tv/") else {"episodes": []}

        client = CountingTMDB()
        catalog = TmdbEpisodeCatalog(client, today=lambda: date(2026, 8, 7))
        catalog.prefetch([
            {"tmdb_id": 1, "media_type": "tv"},
            {"tmdb_id": 1, "media_type": "tv"},
            {"tmdb_id": 2, "media_type": "tv"},
            {"tmdb_id": 3, "media_type": "movie"},
        ], max_workers=99)
        self.assertEqual(sorted(client.calls), ["/tv/1", "/tv/2"])
        # The warm cache is consumed by the normal semantic callable without
        # another provider request.
        catalog({"tmdb_id": 1, "media_type": "tv"})
        self.assertEqual(sorted(client.calls), ["/tv/1", "/tv/2"])

    def test_tmdb_catalog_prefetch_budget_is_bounded_and_fail_closed(self) -> None:
        """A stalled provider cannot hold the audit open or leak late rows."""
        calls: list[str] = []
        lock = threading.Lock()
        release = threading.Event()
        entered = threading.Event()
        finished = threading.Event()
        active = 0
        max_active = 0

        class SlowTMDB:
            def get(self, path: str) -> dict[str, object]:
                nonlocal active, max_active
                with lock:
                    calls.append(path)
                    active += 1
                    max_active = max(max_active, active)
                    entered.set()
                try:
                    release.wait(2)
                    return {"seasons": []}
                finally:
                    with lock:
                        active -= 1
                        if active == 0:
                            finished.set()

        catalog = TmdbEpisodeCatalog(SlowTMDB(), today=lambda: date(2026, 8, 7))
        works = [
            {"tmdb_id": value, "media_type": "tv"}
            for value in range(1, 9)
        ]
        try:
            with patch.dict(
                os.environ,
                {"SCRAPEFLOW_TMDB_AUDIT_BUDGET_SECONDS": "0.2"},
                clear=False,
            ):
                started = time.monotonic()
                catalog.prefetch(works, max_workers=2)
                elapsed = time.monotonic() - started

            self.assertLess(elapsed, 0.8)
            self.assertTrue(entered.wait(0.5))
            with lock:
                # There is never an executor queue larger than the worker
                # bound, and the two calls remain blocked until cleanup.
                self.assertLessEqual(max_active, 2)
                self.assertLessEqual(len(calls), 2)

            # Every submitted, queued, and unsent identity is cached as an
            # unresolved catalog.  A normal semantic lookup cannot start a
            # second unbounded provider request after the deadline.
            before = len(calls)
            for work in works:
                self.assertIsNone(catalog(work))
            self.assertEqual(len(calls), before)
        finally:
            release.set()
            finished.wait(1)

    def test_tmdb_catalog_zero_budget_caches_all_ids_without_requests(self) -> None:
        class CountingTMDB:
            def __init__(self) -> None:
                self.calls: list[str] = []

            def get(self, path: str) -> dict[str, object]:
                self.calls.append(path)
                return {"seasons": []}

        client = CountingTMDB()
        catalog = TmdbEpisodeCatalog(client, today=lambda: date(2026, 8, 7))
        with patch.dict(
            os.environ,
            {"SCRAPEFLOW_TMDB_AUDIT_BUDGET_SECONDS": "-1"},
            clear=False,
        ):
            catalog.prefetch(
                [
                    {"tmdb_id": 20, "media_type": "tv"},
                    {"tmdb_id": 21, "media_type": "tv"},
                ],
            )
        self.assertEqual(client.calls, [])
        self.assertIsNone(catalog({"tmdb_id": 20, "media_type": "tv"}))
        self.assertIsNone(catalog({"tmdb_id": 21, "media_type": "tv"}))

    def test_failed_tmdb_prefetch_keeps_nfo_tv_work_unknown(self) -> None:
        movie_root, anime_root, us_root = DEFAULT_FORMAL_LIBRARY_ROOTS
        show = f"{anime_root}/Unavailable Show"
        season = f"{show}/Season 01"
        nfo_path = f"{show}/tvshow.nfo"
        alist = NfoTreeAList({
            movie_root: [],
            anime_root: [{"name": "Unavailable Show", "is_dir": True}],
            show: [
                {"name": "tvshow.nfo", "is_dir": False, "size": 64},
                {"name": "Season 01", "is_dir": True},
            ],
            season: [{"name": "Unavailable Show S01E01.mkv", "is_dir": False, "size": 10}],
            us_root: [],
        }, {nfo_path: b"<tvshow><tmdbid>42</tmdbid></tvshow>"})

        class FailingTMDB:
            def __init__(self) -> None:
                self.calls: list[str] = []

            def get(self, path: str) -> dict[str, object]:
                self.calls.append(path)
                raise RuntimeError("TMDB unavailable")

        tmdb = FailingTMDB()
        with tempfile.TemporaryDirectory() as tmp:
            report = run_automatic_library_audit(
                alist,
                tmp,
                [],
                tmdb_client=tmdb,
                formal_roots=DEFAULT_FORMAL_LIBRARY_ROOTS,
            )

        self.assertEqual(tmdb.calls, ["/tv/42"])
        self.assertTrue(any(
            row["kind"] == "unknown_episode_catalog"
            and row["target_root"] == show
            for row in report["semantic"]["unknowns"]
        ))
        self.assertFalse(report["library_complete"])


if __name__ == "__main__":
    unittest.main()
