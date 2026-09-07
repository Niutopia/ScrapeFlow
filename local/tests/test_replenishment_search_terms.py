"""Regression tests for exact episode query generation at the gap bridge."""

from __future__ import annotations

import json
import unittest
import tempfile
from pathlib import Path
from unittest.mock import patch
import urllib.error
import xml.etree.ElementTree as ET

from engine.scrapeflow.gap_ledger import Gap, save_gap_ledger
from engine.scrapeflow.work_units import WorkUnitRecord, save_work_unit_records
from engine.tools import _replenishment_local_adapter_impl as adapter
from local.scrapeflow_api.replenishment_bridge import gap_ledger_requests


class ExplicitEpisodeSearchTermsTests(unittest.TestCase):
    @staticmethod
    def _bridge_style_regular_season_request() -> dict[str, object]:
        """A real ledger projection: aliases/gaps, but no legacy groups."""
        return {
            "media": {
                "title": "物理魔法使-马修-",
                # These represent durable C/TMDB title evidence, not source
                # directory names or a result obtained from a web index.
                "aliases": [
                    "物理魔法使-马修-",
                    "Mashle: Magic and Muscles",
                    "Mashle",
                    "マッシュル-MASHLE-",
                ],
            },
            "gaps": [
                {"kind": "missing_episode", "season": 1, "episodes": [13]},
                {"kind": "missing_episode", "season": 1, "episodes": [14]},
            ],
            # A source label must never become a provider-search alias.
            "source_path": "/待刮削/not-authoritative-directory-name",
        }

    def test_bridge_episode_list_generates_all_twelve_exact_coordinates(self) -> None:
        """P14 bridge rows use ``episodes`` rather than scalar ``episode``."""
        with tempfile.TemporaryDirectory() as directory:
            state_root = Path(directory)
            root_id = "root-bridge"
            unit_id = "unit-example"
            save_work_unit_records(state_root, root_id, [WorkUnitRecord(
                work_unit_id=unit_id,
                root_task_id=root_id,
                boundary_key=unit_id,
                source_paths=("/待刮削/Example Series",),
                source_revision=1,
                role="single_work",
                media_context="tv",
                identity_status="confirmed",
                identity={
                    "media_type": "tv",
                    "tmdb_id": 12345,
                    "title": "Example Series",
                    "aliases": ["Example Show"],
                },
            )])
            save_gap_ledger(state_root, root_id, [Gap(
                gap_id=f"{unit_id}::missing_episode::S01E{episode:02d}",
                root_task_id=root_id,
                work_unit_id=unit_id,
                kind="missing_episode",
                media_type="tv",
                tmdb_id=12345,
                season=1,
                episodes=(episode,),
                subtitle_path=None,
                subtitle_language=None,
                status="open",
            ) for episode in range(13, 25)])
            request = gap_ledger_requests(state_root, root_id)[0]

            self.assertEqual(len(request["gaps"]), 12)
            terms = adapter._explicit_episode_search_terms(request, maximum=24)

            expected = {
                f"{base} S01E{episode:02d}"
                for base in ("Example Series", "Example Show")
                for episode in range(13, 25)
            }
            self.assertEqual(set(terms), expected)
            self.assertEqual(len(terms), 24)
            self.assertTrue(all("S01E" in term for term in terms))

    def test_query_groups_remain_authoritative_over_gap_fallback(self) -> None:
        request = {
            "media": {"title": "Example", "aliases": ["Example"]},
            "query_groups": [{"season": 1, "episodes": [2]}],
            "gaps": [{"season": 1, "episodes": [3]}],
        }

        terms = adapter._explicit_episode_search_terms(request, maximum=8)

        self.assertEqual(terms, ["Example S01E02"])

    def test_dmhy_derives_positive_season_from_bridge_gap_rows(self) -> None:
        request = self._bridge_style_regular_season_request()

        terms = adapter._dmhy_search_terms(request)

        self.assertIn("Mashle S1", terms)
        self.assertIn("Mashle: Magic and Muscles S1", terms)
        self.assertEqual(terms[0], "Mashle S1")
        self.assertTrue(all("not-authoritative-directory-name" not in term for term in terms))

    def test_dmhy_skips_overlong_aliases_and_backfills_exact_gap_terms(self) -> None:
        request = {
            "media": {
                "title": "本地显示名",
                "aliases": [
                    "Alpha",
                    "Alpha: Official Title",
                    "A" * 100,
                    "B" * 100,
                ],
            },
            "gaps": [
                {"kind": "missing_episode", "season": 1, "episodes": [13]},
                {"kind": "missing_episode", "season": 1, "episodes": [14]},
            ],
            "source_path": "/待刮削/not-a-query-term",
        }

        terms = adapter._dmhy_search_terms(request)

        self.assertEqual(terms, [
            "Alpha S1",
            "Alpha: Official Title S1",
            "Alpha S01E13",
            "Alpha S01E14",
        ])
        self.assertEqual(len(terms), adapter._DMHY_MAX_QUERY_TERMS)
        self.assertTrue(all(
            len(term) <= adapter._DMHY_MAX_QUERY_TERM_LENGTH
            for term in terms
        ))
        self.assertFalse(any("A" * 80 in term for term in terms))
        self.assertFalse(any("B" * 80 in term for term in terms))
        self.assertFalse(any("not-a-query-term" in term for term in terms))

    def test_declared_query_groups_do_not_widen_dmhy_to_incidental_gaps(self) -> None:
        request = self._bridge_style_regular_season_request()
        request["query_groups"] = [{"season": 2, "episodes": [1]}]

        self.assertEqual(adapter._positive_requested_seasons(request), [2])

    def test_animetosho_uses_latin_tmdb_aliases_with_exact_gap_coordinates(self) -> None:
        request = self._bridge_style_regular_season_request()

        terms = adapter._animetosho_search_terms(request, maximum=4)

        self.assertEqual(terms, [
            "Mashle S01E13",
            "Mashle: Magic and Muscles S01E13",
            "Mashle S01E14",
            "Mashle",
        ])
        self.assertTrue(all("物理魔法使" not in term for term in terms))
        self.assertTrue(all("not-authoritative-directory-name" not in term for term in terms))

    def test_animetosho_broad_alias_is_after_exact_terms(self) -> None:
        request = self._bridge_style_regular_season_request()

        terms = adapter._animetosho_search_terms(request, maximum=4)

        self.assertEqual(terms[-1], "Mashle")
        self.assertTrue(all("S01E" in term for term in terms[:-1]))

    def test_nyaa_uses_exact_identity_terms_then_release_style_fallback(self) -> None:
        request = self._bridge_style_regular_season_request()
        terms = adapter._nyaa_search_terms(request)

        self.assertEqual(terms, [
            "Mashle S01E13",
            "Mashle: Magic and Muscles S01E13",
            "Mashle 13",
            "Mashle 14",
        ])
        self.assertTrue(all("not-authoritative-directory-name" not in term for term in terms))
        self.assertTrue(all(
            "S01E" in term
            for term in terms[:2]
        ))
        self.assertTrue(all(
            "S01E" not in term
            for term in terms[2:]
        ))

    def test_nyaa_rejects_overlong_aliases(self) -> None:
        request = {
            "media": {
                "title": "Local",
                "aliases": ["A" * 200, "Alpha"],
            },
            "gaps": [{"season": 1, "episodes": [13]}],
            "source_path": "/待刮削/not-a-query-term",
        }
        terms = adapter._nyaa_search_terms(request)

        self.assertTrue(terms)
        self.assertTrue(all(len(term) <= adapter._NYAA_MAX_QUERY_TERM_LENGTH for term in terms))
        self.assertFalse(any("A" * 80 in term for term in terms))
        self.assertFalse(any("not-a-query-term" in term for term in terms))


class NyaaSearchRegressionTests(unittest.TestCase):
    @staticmethod
    def _request() -> dict[str, object]:
        return {
            "media": {
                "media_type": "tv",
                "tmdb_id": 204832,
                "title": "物理魔法使-马修-",
                "original_title": "マッシュル-MASHLE-",
                "aliases": ["Mashle: Magic and Muscles", "Mashle"],
            },
            "gaps": [{
                "id": "S01E13", "kind": "missing_episode", "season": 1,
                "episodes": [13],
            }],
        }

    @staticmethod
    def _rss(rows: list[tuple[str, str]]) -> bytes:
        root = ET.Element("rss")
        channel = ET.SubElement(root, "channel")
        for title, url in rows:
            item = ET.SubElement(channel, "item")
            ET.SubElement(item, "title").text = title
            ET.SubElement(item, "link").text = url
        return ET.tostring(root, encoding="utf-8")

    @staticmethod
    def _rss_with_infohash(rows: list[tuple[str, str, str]]) -> bytes:
        root = ET.Element("rss")
        channel = ET.SubElement(root, "channel")
        for title, url, infohash in rows:
            item = ET.SubElement(channel, "item")
            ET.SubElement(item, "title").text = title
            ET.SubElement(item, "link").text = url
            ET.SubElement(item, "infohash").text = infohash
        return ET.tostring(root, encoding="utf-8")

    def test_late_relevant_row_survives_broad_75_row_window_without_false_exhaustion(self) -> None:
        rows = [
            (f"[Fixture] unrelated release {index}",
             f"https://nyaa.si/download/{index}.torrent")
            for index in range(1, 75)
        ]
        relevant_url = "https://nyaa.si/download/999.torrent"
        rows.append(("[Fixture] Mashle S01E13 1080p", relevant_url))
        downloaded: list[str] = []

        def download(url, destination, **_kwargs):  # noqa: ANN001
            del destination
            downloaded.append(url)
            return {"infohash": f"{len(downloaded):040x}"}

        def variants(_request, release_name, torrent_url, _manifest, **_kwargs):  # noqa: ANN001
            if "Mashle S01E13" in release_name:
                return [{
                    "provider": "magnet",
                    "locator": f"torrent:{torrent_url}",
                    "file_coverage": ["S01E13"],
                }]
            return []

        with patch.object(adapter, "_nyaa_search_terms", return_value=["Mashle"]), \
             patch.object(adapter, "_fetch_bytes", return_value=self._rss(rows)), \
             patch.object(adapter, "_download_torrent", side_effect=download), \
             patch.object(adapter, "_torrent_candidate_variants", side_effect=variants):
            result = adapter._search_nyaa(
                self._request(), set(), deadline=adapter.time.monotonic() + 100,
            )

        self.assertEqual(len(result), 1)
        self.assertEqual(downloaded[0], relevant_url)
        self.assertEqual(len(downloaded), 1)
        self.assertEqual(result.query_attempts, 1)
        # Nyaa's fixed, non-pageable 75-row RSS response may hide later
        # rows.  A useful candidate may be selected from it, but the source
        # cannot be used as a no-resource proof.
        self.assertFalse(result.source_exhausted)
        self.assertEqual(result.query_cursor["term_index"], 0)
        self.assertFalse(result.query_cursor["exhausted"])

    def test_nyaa_release_style_fallback_follows_exact_token_query(self) -> None:
        request = self._request()
        queried_terms: list[str] = []
        relevant_url = "https://nyaa.si/download/13.torrent"

        def fetch(url: str, **_kwargs):  # noqa: ANN001
            term = urllib.parse.parse_qs(urllib.parse.urlsplit(url).query)["q"][0]
            queried_terms.append(term)
            if term == "Mashle 13":
                return self._rss([("[Fixture] Mashle - 13 1080p", relevant_url)])
            return self._rss([])

        def download(_url, _destination, **_kwargs):  # noqa: ANN001
            return {"infohash": "1" * 40}

        def variants(_request, release_name, torrent_url, _manifest, **_kwargs):  # noqa: ANN001
            if torrent_url == relevant_url and "- 13" in release_name:
                return [{
                    "provider": "magnet",
                    "locator": f"torrent:{torrent_url}",
                    "file_coverage": ["S01E13"],
                }]
            return []

        with patch.object(adapter, "_fetch_bytes", side_effect=fetch), \
             patch.object(adapter, "_download_torrent", side_effect=download), \
             patch.object(adapter, "_torrent_candidate_variants", side_effect=variants):
            result = adapter._search_nyaa(
                request, set(), deadline=adapter.time.monotonic() + 100,
            )

        self.assertEqual(queried_terms[:3], [
            "Mashle S01E13",
            "Mashle: Magic and Muscles S01E13",
            "Mashle 13",
        ])
        self.assertNotIn("/待刮削", " ".join(queried_terms))
        self.assertEqual(result[0]["file_coverage"], ["S01E13"])
        self.assertFalse(result.source_exhausted)
        self.assertEqual(result.query_cursor["term_index"], 4)
        self.assertFalse(result.query_cursor["exhausted"])

    def test_nyaa_naked_ordinal_manifest_needs_the_requested_single_season(self) -> None:
        request = {
            "media": {
                "media_type": "tv", "tmdb_id": 1,
                "title": "Alpha", "aliases": ["Alpha"],
            },
            "gaps": [{
                "id": "S01E13", "kind": "missing_episode", "season": 1,
                "episodes": [13],
            }],
        }
        manifest = {
            "infohash": "a" * 40,
            "files": {1: {"path": "[Fixture] Alpha - 13.mkv", "size": 1_234_567}},
        }
        accepted = adapter._torrent_candidate_variants(
            request, "[Fixture] Alpha - 13", "https://nyaa.si/download/1.torrent",
            manifest,
        )
        wrong_season = adapter._torrent_candidate_variants(
            request, "[Fixture] Alpha S02E13", "https://nyaa.si/download/2.torrent",
            {
                **manifest,
                "files": {1: {
                    "path": "[Fixture] Alpha S02E13.mkv", "size": 1_234_567,
                }},
            },
        )

        self.assertEqual(accepted[0]["file_coverage"], ["S01E13"])
        self.assertEqual(wrong_season, [])

    def test_nyaa_manifest_window_cap_stays_incomplete(self) -> None:
        rows = [
            (f"[Fixture] Alpha - {index}",
             f"https://nyaa.si/download/{index}.torrent")
            for index in range(1, adapter._NYAA_MAX_MANIFEST_INSPECTIONS + 2)
        ]
        downloaded: list[str] = []

        def download(url, _destination, **_kwargs):  # noqa: ANN001
            downloaded.append(url)
            return {"infohash": f"{len(downloaded):040x}"}

        with patch.object(adapter, "_nyaa_search_terms", return_value=["Alpha"]), \
             patch.object(adapter, "_fetch_bytes", return_value=self._rss(rows)), \
             patch.object(adapter, "_download_torrent", side_effect=download), \
             patch.object(adapter, "_torrent_candidate_variants", return_value=[]):
            result = adapter._search_nyaa(
                self._request(), set(), deadline=adapter.time.monotonic() + 100,
            )

        self.assertEqual(len(downloaded), adapter._NYAA_MAX_MANIFEST_INSPECTIONS)
        self.assertFalse(result.source_exhausted)
        self.assertEqual(result.query_cursor["term_index"], 0)

    def test_nyaa_cursor_rotates_full_logical_schedule_before_exhaustion(self) -> None:
        request = {
            "media": {
                "media_type": "tv", "tmdb_id": 1,
                "title": "Alpha", "aliases": ["Alpha Official"],
            },
            "gaps": [{
                "id": f"S01E{episode:02d}", "kind": "missing_episode",
                "season": 1, "episodes": [episode],
            } for episode in (13, 14, 15, 16, 19, 20, 21)],
        }
        queried_windows: list[list[str]] = []

        def fetch(url: str, **_kwargs):  # noqa: ANN001
            queried_windows[-1].append(
                urllib.parse.parse_qs(urllib.parse.urlsplit(url).query)["q"][0],
            )
            return self._rss([])

        current = dict(request)
        with patch.object(adapter, "_fetch_bytes", side_effect=fetch):
            for _round in range(16):
                queried_windows.append([])
                result = adapter._search_nyaa(
                    current, set(), deadline=adapter.time.monotonic() + 100,
                )
                if result.source_exhausted:
                    break
                current = {
                    **request,
                    "search_cursors": {"Nyaa": result.query_cursor},
                }
            else:  # pragma: no cover - a regression should show a useful diff
                self.fail("Nyaa logical term cursor did not reach exhaustion")

        self.assertFalse(result.query_cursor is None)
        self.assertTrue(result.source_exhausted)
        self.assertFalse(queried_windows[0] == queried_windows[1])
        self.assertEqual(queried_windows[0], [
            "Alpha S01E13", "Alpha Official S01E13", "Alpha 13", "Alpha 14",
        ])
        self.assertIn("Alpha 15", queried_windows[1])
        self.assertTrue(result.query_cursor["exhausted"])

    def test_nyaa_cursor_fingerprint_reset_restarts_changed_gap_coordinates(self) -> None:
        request = {
            "media": {
                "media_type": "tv", "tmdb_id": 1,
                "title": "Alpha", "aliases": ["Alpha Official"],
            },
            "gaps": [{
                "id": "S01E13", "kind": "missing_episode", "season": 1,
                "episodes": [13],
            }, {
                "id": "S01E15", "kind": "missing_episode", "season": 1,
                "episodes": [15],
            }],
        }
        observed: list[str] = []

        def fetch(url: str, **_kwargs):  # noqa: ANN001
            observed.append(
                urllib.parse.parse_qs(urllib.parse.urlsplit(url).query)["q"][0],
            )
            return self._rss([])

        with patch.object(adapter, "_fetch_bytes", side_effect=fetch):
            first = adapter._search_nyaa(
                request, set(), deadline=adapter.time.monotonic() + 100,
            )
            changed = {
                **request,
                "gaps": [request["gaps"][1]],
                "search_cursors": {"Nyaa": first.query_cursor},
            }
            adapter._search_nyaa(
                changed, set(), deadline=adapter.time.monotonic() + 100,
            )

        self.assertNotEqual(first.query_cursor["fingerprint"], adapter._nyaa_request_fingerprint(
            changed, adapter._nyaa_search_terms(
                changed, maximum=adapter._NYAA_MAX_LOGICAL_QUERY_TERMS,
            ),
        ))
        self.assertIn("Alpha S01E15", observed[4:])

    def test_nyaa_full_logical_buffer_never_certifies_absence(self) -> None:
        terms = [f"Alpha {index}" for index in range(
            adapter._NYAA_MAX_LOGICAL_QUERY_TERMS,
        )]
        current = self._request()
        with patch.object(adapter, "_nyaa_search_terms", return_value=terms), \
             patch.object(adapter, "_fetch_bytes", return_value=self._rss([])):
            for _round in range(16):
                result = adapter._search_nyaa(
                    current, set(), deadline=adapter.time.monotonic() + 100,
                )
                current = {
                    **self._request(),
                    "search_cursors": {"Nyaa": result.query_cursor},
                }

        self.assertFalse(result.query_cursor["exhausted"])
        self.assertEqual(result.query_cursor["term_index"], 0)
        self.assertFalse(result.source_exhausted)

    def test_nyaa_reuses_only_verified_noncovering_manifest_receipt(self) -> None:
        infohash = "a" * 40
        torrent_url = "https://nyaa.si/download/101.torrent"
        request = self._request()
        downloaded: list[str] = []

        def download(url, _destination, **_kwargs):  # noqa: ANN001
            downloaded.append(url)
            return {"infohash": infohash}

        with patch.object(adapter, "_nyaa_search_terms", return_value=["Alpha"]), \
             patch.object(adapter, "_fetch_bytes", return_value=self._rss_with_infohash([
                 ("[Fixture] Alpha - 01", torrent_url, infohash),
             ])), \
             patch.object(adapter, "_download_torrent", side_effect=download), \
             patch.object(adapter, "_torrent_candidate_variants", return_value=[]):
            first = adapter._search_nyaa(
                request, set(), deadline=adapter.time.monotonic() + 100,
            )
            request["reviewed_torrent_miss_locators"] = (
                first.reviewed_torrent_miss_locators
            )
            second = adapter._search_nyaa(
                request, set(), deadline=adapter.time.monotonic() + 100,
            )

        self.assertEqual(first.reviewed_torrent_miss_locators, [f"torrent:{infohash}"])
        self.assertEqual(downloaded, [torrent_url])
        self.assertTrue(second.source_exhausted)

    def test_nyaa_non_rss_document_is_infrastructure_incomplete(self) -> None:
        with patch.object(adapter, "_nyaa_search_terms", return_value=["Alpha"]), \
             patch.object(adapter, "_fetch_bytes", return_value=b"<html />"):
            result = adapter._search_nyaa(
                self._request(), set(), deadline=adapter.time.monotonic() + 100,
            )

        self.assertEqual(result.query_attempts, 1)
        self.assertEqual(result.query_responses, 0)
        self.assertEqual(result.infrastructure_failure_types, {"value_error": 1})
        self.assertFalse(result.source_exhausted)

    def test_nyaa_http_failure_is_counted_without_sensitive_text(self) -> None:
        calls: list[str] = []

        def fetch(url: str, **_kwargs):
            calls.append(url)
            if len(calls) == 1:
                raise urllib.error.HTTPError(
                    url, 502, "token=secret", hdrs=None, fp=None,
                )
            return b"<rss><channel /></rss>"

        with patch.object(
            adapter, "_nyaa_search_terms", return_value=["Alpha S01E13", "Alpha S01"],
        ), patch.object(adapter, "_fetch_bytes", side_effect=fetch):
            result = adapter._search_nyaa(
                self._request(), set(), deadline=adapter.time.monotonic() + 100,
            )

        self.assertEqual(result.query_attempts, 2)
        self.assertEqual(result.query_responses, 1)
        self.assertEqual(result.infrastructure_failures, 1)
        self.assertEqual(result.infrastructure_failure_types, {"http_502": 1})
        self.assertFalse(result.source_exhausted)
        self.assertNotIn("token=secret", repr(result.infrastructure_failure_types))


class MagnetMetadataResolutionTests(unittest.TestCase):
    """Magnet locators resolve their .torrent via a bounded aria2 DHT pass."""

    def _torrent_bytes(self) -> bytes:
        # A minimal single-file torrent: announce + info{length, name, piece length, pieces}
        name = b"Example.Show.S01E01.1080p.mkv"
        announce = b"udp://tracker.example:1337/announce"
        # Bencoded dicts must be sorted by key: length < name < piece length < pieces
        info = (
            b"d6:lengthi1048576e4:name" + str(len(name)).encode() + b":" + name
            + b"12:piece lengthi16384e6:pieces20:00000000000000000000e"
        )
        return (
            b"d8:announce" + str(len(announce)).encode() + b":" + announce
            + b"4:info" + info + b"e"
        )

    def test_magnet_resolves_metadata_and_writes_torrent_file(self) -> None:
        magnet = "magnet:?xt=urn:btih:" + "0" * 40
        commands: list[list[str]] = []

        class _Completed:
            returncode = 0

        def fake_run(command, **_kwargs):
            commands.append(command)
            directory = next(
                Path(item.removeprefix("--dir=")).resolve()
                for item in command if item.startswith("--dir=")
            )
            directory.mkdir(parents=True, exist_ok=True)
            infohash = magnet.split("urn:btih:")[1][:40]
            (directory / f"{infohash}.torrent").write_bytes(self._torrent_bytes())
            return _Completed()

        with tempfile.TemporaryDirectory() as directory:
            destination = Path(directory) / "candidate-01.torrent"
            with patch.object(adapter.subprocess, "run", side_effect=fake_run):
                manifest = adapter._download_torrent(magnet, destination)
            self.assertTrue(destination.exists())
            self.assertEqual(destination.read_bytes(), self._torrent_bytes())
            # The scratch directory must not leak into the workspace.
            self.assertFalse(destination.with_name(".candidate-01.torrent.magnet").exists())

        self.assertEqual(len(commands), 1)
        self.assertIn("--bt-metadata-only=true", commands[0])
        self.assertIn(magnet, commands[0][-1])
        self.assertEqual(manifest["files"][1]["path"], "Example.Show.S01E01.1080p.mkv")

    def test_magnet_without_btih_is_rejected_before_any_run(self) -> None:
        with patch.object(adapter.subprocess, "run") as run:
            with self.assertRaises(ValueError):
                adapter._download_torrent(
                    "magnet:?xt=urn:sha1:AAAA", Path("/tmp/never.torrent"),
                )
        run.assert_not_called()

    def test_magnet_metadata_failure_is_a_candidate_error(self) -> None:
        magnet = "magnet:?xt=urn:btih:" + "1" * 40

        class _Completed:
            returncode = 7
            stdout = "bt metadata timeout"

        def fake_run(command, **_kwargs):
            return _Completed()

        with tempfile.TemporaryDirectory() as directory:
            destination = Path(directory) / "candidate-01.torrent"
            with patch.object(adapter.subprocess, "run", side_effect=fake_run):
                with self.assertRaises(RuntimeError):
                    adapter._download_torrent(magnet, destination)

    def test_magnet_metadata_timeout_is_infra_not_candidate(self) -> None:
        # A hung aria2 killed past its own bt-stop window is the same DHT
        # cold-window fault as the rc!=0 variant (741abde); the timeout must
        # not slip into _preflight's OSError branch and permanently exclude
        # the locator.
        magnet = "magnet:?xt=urn:btih:" + "1" * 40
        with tempfile.TemporaryDirectory() as directory:
            destination = Path(directory) / "candidate-01.torrent"
            with patch.object(
                adapter.subprocess, "run",
                side_effect=adapter.subprocess.TimeoutExpired("aria2c", 300),
            ):
                with self.assertRaises(adapter.MagnetMetadataUnavailable):
                    adapter._magnet_metadata(magnet, destination)
            self.assertFalse(
                destination.with_name(".candidate-01.torrent.magnet").exists(),
                "scratch must be reclaimed on the timeout path",
            )

    def test_magnet_metadata_scratch_is_reclaimed_on_parse_failure(self) -> None:
        # A malformed .torrent that survives the size cap must not leak its
        # scratch directory when the manifest parser rejects it.
        magnet = "magnet:?xt=urn:btih:" + "2" * 40

        class _Completed:
            returncode = 0
            stdout = "ok"

        scratch_name = ".candidate-01.torrent.magnet"

        def fake_run(command, **_kwargs):
            scratch = Path(command[command.index(f"--dir={scratch_name}") + 1]) \
                if False else None
            # Locate the scratch dir from the --dir flag and drop a bogus
            # .torrent into it before "aria2" reports success.
            for flag in command:
                if isinstance(flag, str) and flag.startswith("--dir="):
                    scratch = Path(flag[len("--dir="):])
            scratch.mkdir(parents=True, exist_ok=True)
            (scratch / "bogus.torrent").write_bytes(b"not-a-bencoded-dict")
            return _Completed()

        with tempfile.TemporaryDirectory() as directory:
            destination = Path(directory) / "candidate-01.torrent"
            with patch.object(adapter.subprocess, "run", side_effect=fake_run):
                with self.assertRaises(ValueError):
                    adapter._magnet_metadata(magnet, destination)
            leaked = destination.parent / scratch_name
            self.assertFalse(leaked.exists(), "parse failure must reclaim the scratch")


class BitSearchSourceTests(unittest.TestCase):
    """The general-purpose magnet index resolves candidates via DHT only."""

    @staticmethod
    def _request() -> dict[str, object]:
        return {
            "media": {
                "title": "无耻之徒",
                "aliases": ["Shameless", "Shameless (US)"],
                "year": "2011",
                "tmdb_id": 34307,
            },
            "gaps": [{
                "id": "S11E01", "kind": "missing_episode",
                "season": 11, "episodes": [1],
            }],
        }

    @staticmethod
    def _page() -> str:
        return (
            '<a href="magnet:?xt&#x3D;urn:btih:79CD2AA9A0B923E2C13131653143BA55A9D2CFF7'
            '&amp;dn&#x3D;%5BBitsearch.to%5D%20Shameless.US.S11">Magnet</a>'
            '<a href="/download/torrent/79CD2AA9A0B923E2C13131653143BA55A9D2CFF7'
            '?title=Shameless.US.S11.1080p.AMZN.WEB-DL.DDP5.1.H.264-NTb">Torrent</a>'
            '<a href="magnet:?xt&#x3D;urn:btih:93D92D9A5BEB83AD465F967E740DB912008E3EF3'
            '&amp;dn&#x3D;%5BBitsearch.to%5D%20Shameless%20UK%202004">Magnet</a>'
            '<a href="/download/torrent/93D92D9A5BEB83AD465F967E740DB912008E3EF3'
            '?title=Shameless UK 2004 S01-S11 Complete 1080p ALL4 WEB-DL">Torrent</a>'
        )

    def test_page_rows_use_full_magnet_hashes_with_clean_titles(self) -> None:
        rows = adapter._bitsearch_page_rows(self._page())

        self.assertEqual(
            rows["79cd2aa9a0b923e2c13131653143ba55a9d2cff7"],
            "Shameless.US.S11.1080p.AMZN.WEB-DL.DDP5.1.H.264-NTb",
        )
        self.assertEqual(len(rows), 2)

    def test_year_conflicting_same_title_series_is_rejected(self) -> None:
        request = self._request()

        self.assertTrue(
            adapter._general_index_row_relevant(
                "Shameless.US.S11.1080p.AMZN.WEB-DL.DDP5.1.H.264-NTb", request,
            ),
        )
        # Same title, different series, premiere year predates the work.
        self.assertFalse(
            adapter._general_index_row_relevant(
                "Shameless UK 2004 S01-S11 Complete 1080p ALL4 WEB-DL", request,
            ),
        )
        # A pack named by its season year stays acceptable.
        self.assertTrue(
            adapter._general_index_row_relevant(
                "Shameless US Season 10 (2019) 1080p WEB-DL", request,
            ),
        )

    def test_search_builds_dht_anchored_candidates_and_accounts_misses(self) -> None:
        request = self._request()
        manifest = {
            "root": "Shameless.US.S11",
            "infohash": "79cd2aa9a0b923e2c13131653143ba55a9d2cff7",
            "files": {
                1: {
                    "path": "Shameless.US.S11E01.1080p.AMZN.WEB-DL.DDP5.1.H.264-NTb.mkv",
                    "size": 3_932_748_332,
                },
            },
        }
        fetched: list[str] = []

        def fake_fetch(url, **_kwargs):
            fetched.append(url)
            return self._page().encode()

        def fake_batch(magnet_uris, scratch, **_kwargs):
            self.assertEqual(len(magnet_uris), 1)
            self.assertTrue(
                magnet_uris[0].startswith("magnet:?xt=urn:btih:79cd2aa9a0b9"),
            )
            return {"79cd2aa9a0b923e2c13131653143ba55a9d2cff7": manifest}, 0

        with patch.object(adapter, "_fetch_bytes", side_effect=fake_fetch), \
                patch.object(adapter, "_magnet_metadatas_batch", side_effect=fake_batch):
            result = adapter._search_bitsearch(
                request, set(), deadline=adapter.time.monotonic() + 30,
            )

        self.assertEqual(len(result), 1)
        candidate = result[0]
        self.assertTrue(candidate["locator"].startswith("torrent:magnet:?xt=urn:btih:"))
        self.assertEqual(candidate["acquisition"]["url"], candidate["locator"][len("torrent:"):])
        self.assertTrue(result.source_exhausted)
        self.assertEqual(result.infrastructure_failures, 0)
        self.assertEqual(len(fetched), len(adapter._general_index_search_terms(request)))

    def test_unresolved_swarm_is_a_resource_miss_not_an_outage(self) -> None:
        request = self._request()

        # The whole batch unresolved (a cold DHT window): the source must
        # NOT claim exhaustion — the rows the index returned were dropped by
        # our own window, and the telemetry carries dht_window_cold.
        with patch.object(
            adapter, "_fetch_bytes",
            return_value=self._page().encode(),
        ), patch.object(
            adapter, "_magnet_metadatas_batch", return_value=({}, 2),
        ):
            result = adapter._search_bitsearch(
                request, set(), deadline=adapter.time.monotonic() + 30,
            )

        self.assertEqual(len(result), 0)
        self.assertFalse(result.source_exhausted)
        self.assertGreaterEqual(
            result.infrastructure_failure_types.get("dht_window_cold", 0), 1,
            "the cold DHT window must be visible in telemetry",
        )
        self.assertGreaterEqual(result.infrastructure_failures, 1)


class MagnetMemberPipelineTests(unittest.TestCase):
    """Torrent acquisition downloads, uploads, and frees one member at a time."""

    def _selection(self) -> dict[str, object]:
        import engine.tools._replenishment_local_adapter_impl as adapter_
        infohash = adapter_._torrent_manifest(self._torrent_bytes())["infohash"]
        return {
            "provider": "magnet",
            "locator": f"torrent:https://example.test/pack.torrent",
            "release_name": "Example Show S01 1080p",
            "infohash": infohash,
            "selected_gap_ids": ["S01E01", "S01E02"],
            "acquisition": {
                "kind": "torrent",
                "url": "https://example.test/pack.torrent",
                "file_index_by_gap": {"S01E01": [1], "S01E02": [2]},
                "file_size_by_index": {"1": 2 * 1024 * 1024, "2": 3 * 1024 * 1024},
                "file_path_by_index": {
                    "1": "Example.Show.S01E01.1080p.mkv",
                    "2": "Example.Show.S01E02.1080p.mkv",
                },
                "download_bytes": 5 * 1024 * 1024,
                "selected_download_bytes": 5 * 1024 * 1024,
            },
        }

    @staticmethod
    def _torrent_bytes() -> bytes:
        def bstr(value: bytes) -> bytes:
            return str(len(value)).encode() + b":" + value

        def bint(value: int) -> bytes:
            return b"i" + str(value).encode() + b"e"

        name = b"Example Show S01 1080p"
        announce = b"udp://tracker.example:1337/announce"
        f1_path = b"Example.Show.S01E01.1080p.mkv"
        f2_path = b"Example.Show.S01E02.1080p.mkv"
        f1 = b"d" + bstr(b"length") + bint(2 * 1024 * 1024) + bstr(b"path") + b"l" + bstr(f1_path) + b"e" + b"e"
        f2 = b"d" + bstr(b"length") + bint(3 * 1024 * 1024) + bstr(b"path") + b"l" + bstr(f2_path) + b"e" + b"e"
        # Bencoded dicts must be sorted by key: files < name < piece length < pieces
        info = (
            b"d" + bstr(b"files") + b"l" + f1 + f2 + b"e"
            + bstr(b"name") + bstr(name)
            + bstr(b"piece length") + bint(16384)
            + bstr(b"pieces") + bstr(b"0" * 40)
            + b"e"
        )
        return b"d" + bstr(b"announce") + bstr(announce) + bstr(b"info") + info + b"e"

    @staticmethod
    def _wrapper() -> dict[str, object]:
        return {
            "request": {
                "gaps": [
                    {"id": "S01E01", "kind": "missing_episode", "season": 1, "episodes": [1]},
                    {"id": "S01E02", "kind": "missing_episode", "season": 1, "episodes": [2]},
                ],
            },
            "selection": {"selections": [MagnetMemberPipelineTests._selection_static()]},
            "automatic_staging_root": "/quark/影视/ScrapeFlow/补源/root-x/attempt-1",
            "automatic_staging_parent": "/quark/影视/ScrapeFlow/补源",
        }

    @staticmethod
    def _selection_static() -> dict[str, object]:
        return MagnetMemberPipelineTests()._selection()

    class _FakeClient:
        def __init__(self) -> None:
            self.token = "fake"
            self.remote: dict[str, int] = {}
            self.uploads: list[str] = []

        def login(self) -> None:  # pragma: no cover - token preset
            pass

        def mkdir(self, path: str) -> None:
            pass

        def exact_file_info(self, path: str) -> dict[str, object] | None:
            if path in self.remote:
                return {"name": path.rsplit("/", 1)[-1], "size": self.remote[path]}
            return None

        def upload_file(self, target: str, source: Path, content_type: str = "") -> None:
            self.remote[target] = source.stat().st_size
            self.uploads.append(target)

        def list(self, path: str, refresh: bool = False) -> list[dict[str, object]]:
            prefix = path.rstrip("/") + "/"
            return [
                {"name": name.rsplit("/", 1)[-1], "size": size, "is_dir": False}
                for name, size in self.remote.items()
                if name.startswith(prefix)
            ]

    def _run_acquire(self, fake_client, aria2_calls, *, preexisting=None):
        import engine.tools._replenishment_local_adapter_impl as adapter_

        class _Completed:
            returncode = 0
            stdout = "ok"

        def fake_run(command, **kwargs):
            aria2_calls.append(command)
            # Emulate aria2: materialize exactly the selected member under
            # the --dir given, honouring the manifest's real paths.
            directory = next(
                Path(item[len("--dir="):]) for item in command
                if item.startswith("--dir=")
            )
            selected = next(
                item[len("--select-file="):] for item in command
                if item.startswith("--select-file=")
            )
            manifest = adapter_._torrent_manifest(Path(command[-1]).read_bytes())
            for value in selected.split(","):
                row = manifest["files"][int(value)]
                target = directory / row["path"]
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_bytes(b"x" * row["size"])
            return _Completed()

        wrapper = self._wrapper()
        with tempfile.TemporaryDirectory() as directory:
            workspace = Path(directory)
            torrent_path = workspace / "preflight" / "candidate-01.torrent"
            torrent_path.parent.mkdir(parents=True)
            torrent_path.write_bytes(self._torrent_bytes())
            if preexisting:
                preexisting(workspace)
            with patch.object(adapter_.subprocess, "run", side_effect=fake_run), \
                    patch.object(adapter_, "_verify_video_payload"), \
                    patch.object(adapter_.shutil, "which", return_value="/usr/bin/aria2c"), \
                    patch.object(
                        adapter_, "_download_torrent",
                        return_value=adapter_._torrent_manifest(self._torrent_bytes()),
                    ), \
                    patch.object(adapter_, "_alist_client", return_value=fake_client):
                return adapter_._acquire(wrapper, workspace, client=fake_client)

    def test_batch_group_downloaded_uploaded_and_freed(self) -> None:
        fake_client = self._FakeClient()
        aria2_calls: list[list[str]] = []
        deleted: list[Path] = []

        def watch_rmtree(path, **kwargs):
            deleted.append(Path(path))

        import engine.tools._replenishment_local_adapter_impl as adapter_
        with patch.object(adapter_.shutil, "rmtree", side_effect=watch_rmtree), \
                patch.dict("os.environ", {"SCRAPEFLOW_REPLENISHMENT_MEMBER_BATCH": "2"}):
            delivery = self._run_acquire(fake_client, aria2_calls)

        # One aria2 invocation covering both members as a single group.
        self.assertEqual(len(aria2_calls), 1)
        select_arg = next(
            i for i in aria2_calls[0] if i.startswith("--select-file=")
        )
        self.assertEqual(select_arg, "--select-file=1,2")
        # Every member landed remotely with its exact size.
        self.assertEqual(len(fake_client.uploads), 2)
        self.assertEqual(len(delivery["files"]), 2)
        self.assertEqual(delivery["files"][0]["gap_ids"], ["S01E01"])
        self.assertEqual(delivery["files"][1]["gap_ids"], ["S01E02"])
        # The group directory was freed after its verified uploads.
        group_dirs = [p for p in deleted if p.name.startswith("group-")]
        self.assertEqual(len(group_dirs), 1)

    def test_batch_of_one_keeps_single_member_footprint(self) -> None:
        fake_client = self._FakeClient()
        aria2_calls: list[list[str]] = []

        import engine.tools._replenishment_local_adapter_impl as adapter_
        with patch.dict("os.environ", {"SCRAPEFLOW_REPLENISHMENT_MEMBER_BATCH": "1"}):
            delivery = self._run_acquire(fake_client, aria2_calls)

        # Batch size 1 keeps the strict per-member pipeline: one aria2 run
        # and one --select-file per member.
        self.assertEqual(len(aria2_calls), 2)
        for call, expected in zip(aria2_calls, ("--select-file=1", "--select-file=2")):
            self.assertEqual(
                next(i for i in call if i.startswith("--select-file=")), expected,
            )
        self.assertEqual(len(fake_client.uploads), 2)
        self.assertEqual(len(delivery["files"]), 2)

    def test_unclassified_failure_preserves_workspace_tree(self) -> None:
        # Same scenario, but asserting on the filesystem itself: the whole
        # run happens inside the harness's temp workspace, so capture it.
        import engine.tools._replenishment_local_adapter_impl as adapter_
        class _FlakyClient(self._FakeClient):
            def exact_file_info(self, path: str):
                raise RuntimeError("ApiError: AList 瞬时不可用")

        fake_client = _FlakyClient()
        seen: dict[str, Path] = {}

        original_run = self._run_acquire

        def capture_run(client, aria2_calls, *, preexisting=None):
            with tempfile.TemporaryDirectory() as directory:
                workspace = Path(directory)
                seen["workspace"] = workspace
                retained = (
                    workspace / "download-01" / "group-001" / "payload"
                    / "Example.Show.S01E01.1080p.mkv"
                )
                retained.parent.mkdir(parents=True, exist_ok=True)
                retained.write_bytes(b"resume-bytes" * 1024)
                torrent_path = workspace / "preflight" / "candidate-01.torrent"
                torrent_path.parent.mkdir(parents=True)
                torrent_path.write_bytes(self._torrent_bytes())

                class _Completed:
                    returncode = 0
                    stdout = "ok"

                def fake_run(command, **kwargs):
                    aria2_calls.append(command)
                    return _Completed()

                wrapper = self._wrapper()
                with patch.object(adapter_.subprocess, "run", side_effect=fake_run), \
                        patch.object(adapter_, "_verify_video_payload"), \
                        patch.object(adapter_.shutil, "which", return_value="/usr/bin/aria2c"), \
                        patch.object(
                            adapter_, "_download_torrent",
                            return_value=adapter_._torrent_manifest(self._torrent_bytes()),
                        ), \
                        patch.object(adapter_, "_alist_client", return_value=client):
                    try:
                        adapter_._acquire(wrapper, workspace, client=client)
                    except RuntimeError as exc:
                        seen["error"] = exc
                        # Assert INSIDE the tempdir context: the context
                        # manager itself reclaims the directory on exit.
                        self.assertNotIsInstance(
                            exc, adapter_.ReplenishmentCandidateError,
                        )
                        self.assertTrue(
                            torrent_path.exists(),
                            "unclassified failure must keep the attempt workspace",
                        )
                        self.assertTrue(
                            retained.exists(),
                            "retained resume bytes must survive an unclassified failure",
                        )
                        return
                    raise AssertionError("expected the flaky client to fail the run")

        capture_run(fake_client, [])
        self.assertIsNotNone(seen.get("error"))

    def test_zero_byte_local_failure_is_infrastructure(self) -> None:
        # Zero payload + host-side errno evidence (a full disk) must not
        # permanently exclude the locator: the download never proved the
        # resource bad.
        import engine.tools._replenishment_local_adapter_impl as adapter_

        fake_client = self._FakeClient()
        aria2_calls: list[list[str]] = []

        class _Completed:
            returncode = 1
            stdout = (
                "Download aborted"
                " [FileSyncController#write] errno=28: No space left on device"
            )

        def fake_run(command, **kwargs):
            aria2_calls.append(command)
            return _Completed()

        wrapper = self._wrapper()
        with tempfile.TemporaryDirectory() as directory:
            workspace = Path(directory)
            torrent_path = workspace / "preflight" / "candidate-01.torrent"
            torrent_path.parent.mkdir(parents=True)
            torrent_path.write_bytes(self._torrent_bytes())
            with patch.object(adapter_.subprocess, "run", side_effect=fake_run), \
                    patch.object(adapter_, "_verify_video_payload"), \
                    patch.object(adapter_.shutil, "which", return_value="/usr/bin/aria2c"), \
                    patch.object(
                        adapter_, "_download_torrent",
                        return_value=adapter_._torrent_manifest(self._torrent_bytes()),
                    ), \
                    patch.object(adapter_, "_alist_client", return_value=fake_client):
                with self.assertRaises(adapter_.ReplenishmentInfrastructureError):
                    adapter_._acquire(wrapper, workspace, client=fake_client)

    def test_zero_byte_dead_swarm_still_excludes_candidate(self) -> None:
        # The default stays: zero payload without local-failure evidence is
        # a dead resource verdict and permanently excludes the locator.
        import engine.tools._replenishment_local_adapter_impl as adapter_

        fake_client = self._FakeClient()
        aria2_calls: list[list[str]] = []

        class _Completed:
            returncode = 1
            stdout = "bt-stop-timeout reached; no peers"

        def fake_run(command, **kwargs):
            aria2_calls.append(command)
            return _Completed()

        wrapper = self._wrapper()
        with tempfile.TemporaryDirectory() as directory:
            workspace = Path(directory)
            torrent_path = workspace / "preflight" / "candidate-01.torrent"
            torrent_path.parent.mkdir(parents=True)
            torrent_path.write_bytes(self._torrent_bytes())
            with patch.object(adapter_.subprocess, "run", side_effect=fake_run), \
                    patch.object(adapter_, "_verify_video_payload"), \
                    patch.object(adapter_.shutil, "which", return_value="/usr/bin/aria2c"), \
                    patch.object(
                        adapter_, "_download_torrent",
                        return_value=adapter_._torrent_manifest(self._torrent_bytes()),
                    ), \
                    patch.object(adapter_, "_alist_client", return_value=fake_client):
                with self.assertRaises(adapter_.ReplenishmentCandidateError):
                    adapter_._acquire(wrapper, workspace, client=fake_client)

    def test_remote_committed_member_skips_download_and_upload(self) -> None:
        fake_client = self._FakeClient()
        # Member 1 is already committed remotely from an earlier interrupted
        # attempt; only member 2 should download and upload now.
        staging = "/quark/影视/ScrapeFlow/补源/root-x/attempt-1"
        fake_client.remote[f"{staging}/S01E01 - Example.Show.S01E01.1080p.mkv"] = 2 * 1024 * 1024
        aria2_calls: list[list[str]] = []

        delivery = self._run_acquire(fake_client, aria2_calls)

        self.assertEqual(len(aria2_calls), 1)
        self.assertEqual(
            aria2_calls[0][
                aria2_calls[0].index(next(i for i in aria2_calls[0] if i.startswith("--select-file=")))
            ],
            "--select-file=2",
        )
        self.assertEqual(len(fake_client.uploads), 1)
        self.assertEqual(len(delivery["files"]), 2)

    def test_legacy_bulk_payload_directory_is_reclaimed(self) -> None:
        fake_client = self._FakeClient()
        aria2_calls: list[list[str]] = []
        removed: list[Path] = []

        def preexisting(workspace: Path) -> None:
            legacy = workspace / "download-01" / "payload" / "stale"
            legacy.mkdir(parents=True)
            (legacy / "old.mkv").write_bytes(b"0" * 1024)

        import engine.tools._replenishment_local_adapter_impl as adapter_
        real_rmtree = adapter_.shutil.rmtree

        def watch_rmtree(path, **kwargs):
            removed.append(Path(path))
            real_rmtree(path, **kwargs)

        with patch.object(adapter_.shutil, "rmtree", side_effect=watch_rmtree):
            self._run_acquire(fake_client, aria2_calls, preexisting=preexisting)

        self.assertIn(workspace_marker := "payload", [p.name for p in removed])

    def test_preflight_capacity_floor_is_one_member_not_the_pack(self) -> None:
        import engine.tools._replenishment_local_adapter_impl as adapter_
        wrapper = {
            "request": {"gaps": [{"id": "S01E01", "kind": "missing_episode"}]},
            "selection": {"selections": [self._selection()]},
            "automatic_staging_root": "/quark/影视/ScrapeFlow/补源/root-x/attempt-1",
            "automatic_staging_parent": "/quark/影视/ScrapeFlow/补源",
        }
        with tempfile.TemporaryDirectory() as directory:
            workspace = Path(directory)
            (workspace / "preflight").mkdir()
            (workspace / "preflight" / "candidate-01.torrent").write_bytes(
                self._torrent_bytes(),
            )
            class _TinyDisk:
                @staticmethod
                def disk_usage(_path):
                    class _Usage:
                        free = int(5 * 1024 * 1024 * 1.15) + 1024 ** 3
                    return _Usage()
            with patch.object(adapter_.shutil, "disk_usage", _TinyDisk.disk_usage), \
                    patch.object(adapter_.shutil, "which", return_value="/usr/bin/aria2c"), \
                    patch.object(adapter_, "_download_torrent", return_value=adapter_._torrent_manifest(self._torrent_bytes())):
                # A disk holding only ~one member + headroom must pass for a
                # two-member pack (the old whole-pack floor would reject it).
                result = adapter_._preflight(wrapper, workspace / "preflight")
        self.assertEqual(result["status"], "verified")
        self.assertEqual(result["selected_files"], 2)


class KnabenSourceTests(unittest.TestCase):
    """The knaben.org meta-index parses title+magnet anchors directly."""

    @staticmethod
    def _request() -> dict[str, object]:
        return {
            "media": {
                "title": "无耻之徒",
                "aliases": ["Shameless", "Shameless (US)"],
                "year": "2011",
                "tmdb_id": 34307,
            },
            "gaps": [{
                "id": "S10E01", "kind": "missing_episode",
                "season": 10, "episodes": [1],
            }],
        }

    def test_page_rows_pair_titles_with_full_hashes(self) -> None:
        page = (
            '<tr data-id="0471981bea7aa70f38dc77b21475e9ef92a9c13c"><td>'
            '<a title="Shameless.US.S10.1080p.AMZN.WEBRip.DDP5.1.x264-NTb" '
            'href="magnet:?xt=urn:btih:0471981BEA7AA70F38DC77B21475E9EF92A9C13C'
            '&amp;dn=Shameless.US.S10&amp;tr=http%3A%2F%2Fp4p.arenabg.com%3A1337%2Fannounce">NTb</a>'
            '<a title="Shameless UK 2004 S01-S11 Complete" '
            'href="magnet:?xt=urn:btih:93D92D9A5BEB83AD465F967E740DB912008E3EF3">UK</a>'
            '</td></tr>'
        )
        rows = adapter._knaben_page_rows(page)

        self.assertEqual(
            rows["0471981bea7aa70f38dc77b21475e9ef92a9c13c"],
            "Shameless.US.S10.1080p.AMZN.WEBRip.DDP5.1.x264-NTb",
        )
        self.assertEqual(len(rows), 2)

    def test_magnet_with_trackers_only_appends_when_absent(self) -> None:
        magnet = "magnet:?xt=urn:btih:" + "1" * 40

        bare = adapter._magnet_with_trackers(magnet)
        self.assertIn("&tr=", bare)
        tracked = adapter._magnet_with_trackers(bare)
        self.assertEqual(tracked, bare)


class PartialProgressClassificationTests(unittest.TestCase):
    """An intermittent swarm is a window problem, not a bad resource."""

    def _run_with_aria2(self, *, returncode, stdout, partial):
        import engine.tools._replenishment_local_adapter_impl as adapter_

        class _Completed:
            pass

        def fake_run(command, **_kwargs):
            directory = next(
                Path(item[len("--dir="):]) for item in command
                if item.startswith("--dir=")
            )
            if partial:
                (directory / "partial.mkv").write_bytes(b"x" * 1024)
            completed = _Completed()
            completed.returncode = returncode
            completed.stdout = stdout
            return completed

        fake_client = MagnetMemberPipelineTests._FakeClient()
        aria2_calls: list[list[str]] = []
        with patch.object(
            adapter_.subprocess, "run", side_effect=fake_run,
        ), patch.object(
            adapter_, "_verify_video_payload",
        ), patch.object(
            adapter_.shutil, "which", return_value="/usr/bin/aria2c",
        ), patch.object(
            adapter_, "_download_torrent",
            return_value=adapter_._torrent_manifest(
                MagnetMemberPipelineTests._torrent_bytes(),
            ),
        ), patch.object(
            adapter_, "_alist_client", return_value=fake_client,
        ):
            adapter_._acquire(
                MagnetMemberPipelineTests._wrapper(), Path(tempfile.mkdtemp()),
                client=fake_client,
            )

    def test_partial_progress_is_infrastructure_and_keeps_workspace(self) -> None:
        import engine.tools._replenishment_local_adapter_impl as adapter_
        with patch.object(
            adapter_.shutil, "rmtree",
            side_effect=lambda p, **k: (_ for _ in ()).throw(
                AssertionError("workspace must not be removed"),
            ) if "group-" in str(p) else None,
        ):
            with self.assertRaises(adapter_.ReplenishmentInfrastructureError):
                self._run_with_aria2(
                    returncode=7, stdout="INPR 347KiB/s", partial=True,
                )

    def test_zero_progress_stays_a_candidate_failure(self) -> None:
        import engine.tools._replenishment_local_adapter_impl as adapter_
        with self.assertRaises(adapter_.ReplenishmentCandidateError):
            self._run_with_aria2(
                returncode=7, stdout="no peers", partial=False,
            )


class OperatorSuppliedPreferenceTests(unittest.TestCase):
    """A hand-fed catalog resource outranks equal index discoveries."""

    def test_operator_supplied_wins_equal_ranking(self) -> None:
        from local.scrapeflow_api.replenishment import select_replenishment_candidates
        request = {
            "media": {"title": "Example Show", "aliases": ["Example Show"], "year": "2020"},
            "gaps": [{"id": "S01E01", "kind": "missing_episode", "season": 1, "episodes": [1]}],
        }
        base = {
            "provider": "magnet",
            "resolution": "1080p",
            "availability": "metadata_verified",
            "acquisition": {"kind": "torrent"},
        }

        def row(name, **extra):
            return {
                **base, "release_name": name,
                "locator": f"torrent:magnet:?xt=urn:btih:{abs(hash(name)) % 10**40:040d}",
                "file_coverage": ["S01E01"],
                "files": [f"{name}.S01E01.mkv"],
                "acquisition": {
                    "kind": "torrent", "url": "magnet:?xt=urn:btih:"
                    + f"{abs(hash(name)) % 10**40:040d}",
                    "file_index_by_gap": {"S01E01": [1]},
                    "file_size_by_index": {"1": 1024 * 1024},
                    "file_path_by_index": {"1": f"{name}.S01E01.mkv"},
                    "download_bytes": 1024 * 1024,
                    "selected_download_bytes": 1024 * 1024,
                },
                **extra,
            }

        discovered = row("Example.Show.S01.1080p.WEBRip.x265-Index")
        supplied = row("Example.Show.S01.1080p.BluRay.Remux-Operator", operator_supplied=True)

        selection = select_replenishment_candidates(
            request, [discovered, supplied], current_tier="magnet",
        )
        self.assertEqual(selection["status"], "complete")
        self.assertEqual(len(selection["selections"]), 1)
        self.assertEqual(
            selection["selections"][0]["release_name"],
            "Example.Show.S01.1080p.BluRay.Remux-Operator",
        )

        # Without the marker the same pair falls back to the ordinary
        # ranking (alphabetical here), proving the marker is what decides.
        plain = select_replenishment_candidates(
            request,
            [discovered, {**supplied, "operator_supplied": None}],
            current_tier="magnet",
        )
        self.assertNotIn(
            "operator_supplied",
            {k: v for k, v in plain["selections"][0].items()},
        )


if __name__ == "__main__":
    unittest.main()


class CatalogWriteSurfaceTests(unittest.TestCase):
    """The operator-supply catalog finally has a validated write surface."""

    MAGNET = "torrent:magnet:?xt=urn:btih:" + "ab" * 20 + "&dn=Test.Release"

    def _candidate(self, **overrides):
        base = {
            "provider": "magnet",
            "release_name": "Test.Release.1080p-GROUP",
            "locator": self.MAGNET,
            "resolution": "1080p",
        }
        base.update(overrides)
        return base

    def test_add_derives_infohash_and_replaces_idempotently(self) -> None:
        from local.scrapeflow_api.replenishment import (
            remove_catalog_candidate,
            upsert_catalog_candidate,
        )
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "catalog.json"
            first = upsert_catalog_candidate(path, 34307, self._candidate())
            self.assertEqual(first["stored"]["infohash"], "ab" * 20)
            second = upsert_catalog_candidate(
                path, 34307,
                self._candidate(release_name="Test.Release.1080p.v2"),
            )
            self.assertEqual(second["project_candidates"], 1)
            self.assertEqual(second["stored"]["release_name"], "Test.Release.1080p.v2")
            removed = remove_catalog_candidate(path, 34307, infohash="AB" * 20)
            self.assertTrue(removed["removed"])
            self.assertEqual(
                json.loads(path.read_text(encoding="utf-8"))["projects"], {},
                "emptied project disappears",
            )

    def test_write_rejects_wrong_lane_and_typos_and_bad_acquisition(self) -> None:
        from local.scrapeflow_api.replenishment import (
            CatalogWriteError,
            upsert_catalog_candidate,
        )
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "catalog.json"
            for bad in (
                self._candidate(provider="quark_share"),
                self._candidate(relese_name="typo"),
                self._candidate(locator="magnet:?xt=urn:btih:zz"),
                self._candidate(infohash="cd" * 20),
                self._candidate(acquisition={"kind": "http", "url": "x"}),
                self._candidate(acquisition={
                    "kind": "torrent",
                    "url": "magnet:?xt=urn:btih:" + "cd" * 20,
                    "file_index_by_gap": {"S01E01": [0]},
                }),
            ):
                with self.assertRaises(CatalogWriteError):
                    upsert_catalog_candidate(path, 34307, bad)
            self.assertFalse(path.exists(), "rejected writes persist nothing")

    def test_validated_acquisition_maps_pass_through(self) -> None:
        from local.scrapeflow_api.replenishment import upsert_catalog_candidate
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "catalog.json"
            result = upsert_catalog_candidate(path, 999, self._candidate(
                acquisition={
                    "kind": "torrent",
                    "url": "magnet:?xt=urn:btih:" + "ab" * 20,
                    "file_index_by_gap": {"S10E01": [3]},
                    "file_size_by_index": {"3": 11502449200},
                    "file_path_by_index": {"3": "Show/S10E01.mkv"},
                },
            ))
            stored = result["stored"]["acquisition"]
            self.assertEqual(stored["file_index_by_gap"], {"S10E01": [3]})
            self.assertEqual(stored["file_size_by_index"], {"3": 11502449200})


class CrossGroupAdoptionTests(unittest.TestCase):
    """Drift-fix regressions: retained members are keyed by path, not group."""

    def test_find_download_prefers_exact_relative_path(self) -> None:
        # Two same-named, same-sized members in different subdirectories:
        # the exact manifest path is the primary key; a name+size scan alone
        # could never tell them apart (the old burn-locator path).
        import engine.tools._replenishment_local_adapter_impl as adapter_

        with tempfile.TemporaryDirectory() as directory:
            payload = Path(directory)
            for folder in ("disc1", "disc2"):
                target = payload / folder / "Show.S01E01.mkv"
                target.parent.mkdir(parents=True)
                target.write_bytes(b"x" * 1024)
            found = adapter_._find_download(
                payload, "disc2/Show.S01E01.mkv", 1024,
            )
            self.assertEqual(found.parent.name, "disc2")

    def test_find_download_falls_back_to_basename_when_flattened(self) -> None:
        import engine.tools._replenishment_local_adapter_impl as adapter_

        with tempfile.TemporaryDirectory() as directory:
            payload = Path(directory)
            target = payload / "Show.S01E02.mkv"
            target.write_bytes(b"y" * 2048)
            found = adapter_._find_download(
                payload, "nested/missing/Show.S01E02.mkv", 2048,
            )
            self.assertEqual(found, target)

    def test_find_retained_member_adopts_across_groups(self) -> None:
        # The drift scenario: the member completed in group-001 but the
        # regrouped pending list placed it in group-002.  The retained bytes
        # are found by exact path+size regardless of the group ordinal.
        import engine.tools._replenishment_local_adapter_impl as adapter_

        with tempfile.TemporaryDirectory() as directory:
            candidate_dir = Path(directory)
            retained = (
                candidate_dir / "group-001" / "payload"
                / "Example.Show.S01E01.1080p.mkv"
            )
            retained.parent.mkdir(parents=True)
            retained.write_bytes(b"z" * 4096)
            empty = candidate_dir / "group-002" / "payload"
            empty.mkdir(parents=True)

            found = adapter_._find_retained_member(
                candidate_dir, "Example.Show.S01E01.1080p.mkv", 4096,
            )
            self.assertEqual(found, retained)
            self.assertIsNone(
                adapter_._find_retained_member(
                    candidate_dir, "Example.Show.S01E01.1080p.mkv", 4097,
                ),
            )
            self.assertIsNone(
                adapter_._find_retained_member(
                    candidate_dir / "no-such", "any.mkv", 1,
                ),
            )

    def test_magnet_metadatas_batch_reports_unresolved_count(self) -> None:
        # The window fact crosses the adapter boundary: magnets the DHT
        # window could not resolve are counted, not silently dropped.  The
        # resolved manifest's true infohash (sha1 of the bencoded info) is
        # what keys the returned mapping.
        import engine.tools._replenishment_local_adapter_impl as adapter_

        torrent_bytes = self._single_file_torrent()
        resolved_hash = adapter_._torrent_manifest(torrent_bytes)["infohash"]
        unresolved_hash = "b" * 40
        magnet = "magnet:?xt=urn:btih:{hash}&dn=Name"
        # The scratch file name is irrelevant — aria2 names it after the
        # infohash, and the helper indexes by the manifest's own hash.
        seed_name = "resolved.torrent"

        def fake_run(command, **_kwargs):
            directory = next(
                Path(item[len("--dir="):]) for item in command
                if item.startswith("--dir=")
            )
            directory.mkdir(parents=True, exist_ok=True)
            (directory / seed_name).write_bytes(torrent_bytes)
            return type("_C", (), {"returncode": 0, "stdout": "ok"})()

        with tempfile.TemporaryDirectory() as directory:
            with patch.object(
                adapter.subprocess, "run", side_effect=fake_run,
            ):
                manifests, unresolved = adapter._magnet_metadatas_batch(
                    [
                        magnet.format(hash=resolved_hash),
                        magnet.format(hash=unresolved_hash),
                    ],
                    Path(directory),
                    timeout=60,
                )
        self.assertIn(resolved_hash, manifests)
        self.assertNotIn(unresolved_hash, manifests)
        self.assertEqual(unresolved, 1)

    @staticmethod
    def _single_file_torrent() -> bytes:
        return MagnetMemberPipelineTests._torrent_bytes()
