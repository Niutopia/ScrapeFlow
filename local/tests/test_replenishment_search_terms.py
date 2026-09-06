"""Regression tests for exact episode query generation at the gap bridge."""

from __future__ import annotations

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
            adapter._bitsearch_row_relevant(
                "Shameless.US.S11.1080p.AMZN.WEB-DL.DDP5.1.H.264-NTb", request,
            ),
        )
        # Same title, different series, premiere year predates the work.
        self.assertFalse(
            adapter._bitsearch_row_relevant(
                "Shameless UK 2004 S01-S11 Complete 1080p ALL4 WEB-DL", request,
            ),
        )
        # A pack named by its season year stays acceptable.
        self.assertTrue(
            adapter._bitsearch_row_relevant(
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
            return {"79cd2aa9a0b923e2c13131653143ba55a9d2cff7": manifest}

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
        self.assertEqual(len(fetched), len(adapter._bitsearch_search_terms(request)))

    def test_unresolved_swarm_is_a_resource_miss_not_an_outage(self) -> None:
        request = self._request()

        with patch.object(
            adapter, "_fetch_bytes",
            return_value=self._page().encode(),
        ), patch.object(adapter, "_magnet_metadatas_batch", return_value={}):
            result = adapter._search_bitsearch(
                request, set(), deadline=adapter.time.monotonic() + 30,
            )

        self.assertEqual(len(result), 0)
        self.assertTrue(result.source_exhausted)
        self.assertEqual(
            result.resource_failed_locators,
            ["torrent:79cd2aa9a0b923e2c13131653143ba55a9d2cff7"],
        )
        self.assertEqual(result.infrastructure_failures, 0)


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
                int(item[len("--select-file="):]) for item in command
                if item.startswith("--select-file=")
            )
            manifest = adapter_._torrent_manifest(Path(command[-1]).read_bytes())
            row = manifest["files"][selected]
            target = directory / row["path"]
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(b"x" * row["size"])
            return _Completed()

        wrapper = {
            "request": {
                "gaps": [
                    {"id": "S01E01", "kind": "missing_episode", "season": 1, "episodes": [1]},
                    {"id": "S01E02", "kind": "missing_episode", "season": 1, "episodes": [2]},
                ],
            },
            "selection": {"selections": [self._selection()]},
            "automatic_staging_root": "/quark/影视/ScrapeFlow/补源/root-x/attempt-1",
            "automatic_staging_parent": "/quark/影视/ScrapeFlow/补源",
        }
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

    def test_one_member_downloaded_uploaded_and_freed_at_a_time(self) -> None:
        fake_client = self._FakeClient()
        aria2_calls: list[list[str]] = []
        deleted: list[Path] = []

        def watch_rmtree(path, **kwargs):
            deleted.append(Path(path))

        import engine.tools._replenishment_local_adapter_impl as adapter_
        with patch.object(adapter_.shutil, "rmtree", side_effect=watch_rmtree):
            delivery = self._run_acquire(fake_client, aria2_calls)

        # Exactly one --select-file per aria2 invocation, once per member.
        self.assertEqual(len(aria2_calls), 2)
        self.assertEqual(
            [c[c.index(next(i for i in c if i.startswith("--select-file=")))] for c in aria2_calls],
            ["--select-file=1", "--select-file=2"],
        )
        # Every member landed remotely with its exact size.
        self.assertEqual(len(fake_client.uploads), 2)
        self.assertEqual(len(delivery["files"]), 2)
        self.assertEqual(delivery["files"][0]["gap_ids"], ["S01E01"])
        self.assertEqual(delivery["files"][1]["gap_ids"], ["S01E02"])
        # Member directories were freed after their verified uploads.
        member_dirs = [p for p in deleted if p.name.startswith("member-")]
        self.assertEqual(len(member_dirs), 2)

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
                        free = int(4 * 1024 * 1024 * 1.15) + 1024 ** 3
                    return _Usage()
            with patch.object(adapter_.shutil, "disk_usage", _TinyDisk.disk_usage), \
                    patch.object(adapter_.shutil, "which", return_value="/usr/bin/aria2c"), \
                    patch.object(adapter_, "_download_torrent", return_value=adapter_._torrent_manifest(self._torrent_bytes())):
                # A disk holding only ~one member + headroom must pass for a
                # two-member pack (the old whole-pack floor would reject it).
                result = adapter_._preflight(wrapper, workspace / "preflight")
        self.assertEqual(result["status"], "verified")
        self.assertEqual(result["selected_files"], 2)


if __name__ == "__main__":
    unittest.main()
