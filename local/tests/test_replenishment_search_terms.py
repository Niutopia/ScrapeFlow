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


if __name__ == "__main__":
    unittest.main()
