"""Focused regression tests for bounded AnimeTosho page continuation."""

from __future__ import annotations

import json
import unittest
from unittest.mock import patch

from engine.tools import _replenishment_local_adapter_impl as adapter
from engine.tools import replenishment_search_sources as search_sources


def _request() -> dict[str, object]:
    return {
        "tier": "magnet",
        "media": {
            "media_type": "tv",
            "tmdb_id": 204832,
            "title": "物理魔法使-马修-",
            "original_title": "Mashle: Magic and Muscles",
            "aliases": ["Mashle: Magic and Muscles", "Mashle"],
        },
        "gaps": [
            {"id": "S01E13", "kind": "missing_episode", "season": 1, "episodes": [13]},
            {"id": "S01E14", "kind": "missing_episode", "season": 1, "episodes": [14]},
        ],
    }


def _feed_row(index: int, title: str | None = None) -> dict[str, object]:
    digest = f"{index:040x}"
    return {
        "title": title or f"[Fixture] Mashle unrelated {index}",
        "torrent_url": f"https://storage.animetosho.org/torrent/{digest}/fixture-{index}.torrent",
        "info_hash": digest,
        "seeders": 1,
        "leechers": 0,
    }


class AnimeToshoPaginationTests(unittest.TestCase):
    def test_verified_late_feed_candidate_is_promoted_and_keeps_cursor(self) -> None:
        """A broad page must not spend its deadline on unrelated manifests."""
        request = _request()
        # Put the only covering row at the end of the feed.  Its title carries
        # a naked episode number, which is an ordering hint only; the mocked
        # manifest validator below remains the authority for candidate proof.
        first_page = [
            _feed_row(index, "[Fixture] unrelated release")
            for index in range(1, 75)
        ]
        first_page.append(_feed_row(100, "[Fixture] Mashle - 14 1080p"))
        downloaded: list[str] = []

        def fetch(url: str, **_kwargs: object) -> bytes:
            del url
            return json.dumps(first_page).encode()

        def download(url: str, destination, **_kwargs: object) -> dict[str, object]:  # noqa: ANN001
            del destination
            downloaded.append(url)
            suffix = url.rsplit("/", 2)[-2]
            return {"infohash": suffix}

        def variants(_request, release_name, torrent_url, _manifest, **_kwargs):  # noqa: ANN001
            if "- 14 " in release_name:
                return [{
                    "provider": "magnet",
                    "locator": f"torrent:{torrent_url}",
                    "file_coverage": ["S01E13", "S01E14"],
                }]
            return []

        with patch.object(search_sources, "_ANIMETOSHO_MAX_PAGES_PER_RUN", 1), patch.object(
            search_sources, "_animetosho_search_terms", return_value=["Mashle"],
        ), patch.object(search_sources, "_identity_query_bases", return_value=[]), patch.object(
            search_sources, "_fetch_bytes", side_effect=fetch,
        ), patch.object(search_sources, "_download_torrent", side_effect=download), patch.object(
            search_sources, "_torrent_candidate_variants", side_effect=variants,
        ):
            result = adapter._search_animetosho(
                request, set(), deadline=adapter.time.monotonic() + 100,
            )

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["file_coverage"], ["S01E13", "S01E14"])
        self.assertEqual(len(downloaded), 1)
        self.assertEqual(result.query_cursor["term_index"], 0)
        self.assertEqual(result.query_cursor["page"], 1)
        self.assertFalse(result.query_cursor["exhausted"])
        self.assertFalse(result.source_exhausted)
        # No unreviewed rows after the verified candidate may be cached as
        # misses; the page will resume only after the gap state changes.
        self.assertEqual(result.reviewed_torrent_miss_locators, [])

    def test_partial_candidate_survives_deadline_without_false_miss(self) -> None:
        """A deadline may return partial evidence but never cache unseen rows."""
        request = _request()
        rows = [
            _feed_row(1, "[Fixture] Mashle - 13 1080p"),
            _feed_row(2, "[Fixture] unrelated release"),
        ]
        clock = [0.0]
        downloaded: list[str] = []

        def monotonic() -> float:
            return clock[0]

        def fetch(_url: str, **_kwargs: object) -> bytes:
            return json.dumps(rows).encode()

        def download(url: str, destination, **_kwargs: object) -> dict[str, object]:  # noqa: ANN001
            del destination
            downloaded.append(url)
            # Force the next row's deadline checkpoint to fire after the
            # first candidate has been fully validated.
            clock[0] = 11.0
            suffix = url.rsplit("/", 2)[-2]
            return {"infohash": suffix}

        def variants(_request, release_name, torrent_url, _manifest, **_kwargs):  # noqa: ANN001
            if "- 13 " in release_name:
                return [{
                    "provider": "magnet",
                    "locator": f"torrent:{torrent_url}",
                    "file_coverage": ["S01E13"],
                }]
            return []

        with patch.object(search_sources.time, "monotonic", side_effect=monotonic), patch.object(
            search_sources, "_ANIMETOSHO_MAX_PAGES_PER_RUN", 1,
        ), patch.object(search_sources, "_animetosho_search_terms", return_value=["Mashle"]), patch.object(
            search_sources, "_identity_query_bases", return_value=[]
        ), patch.object(search_sources, "_fetch_bytes", side_effect=fetch), patch.object(
            search_sources, "_download_torrent", side_effect=download,
        ), patch.object(search_sources, "_torrent_candidate_variants", side_effect=variants):
            result = adapter._search_animetosho(
                request, set(), deadline=10.0,
            )

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["file_coverage"], ["S01E13"])
        self.assertEqual(len(downloaded), 1)
        self.assertEqual(result.query_cursor["page"], 1)
        self.assertFalse(result.query_cursor["exhausted"])
        self.assertFalse(result.source_exhausted)
        self.assertEqual(result.reviewed_torrent_miss_locators, [])

    def test_page_cursor_continues_after_old_32_row_boundary(self) -> None:
        request = _request()
        first_page = [_feed_row(index) for index in range(1, 76)]
        second_page = [_feed_row(1), _feed_row(100, "[Fixture] Mashle S01E13 1080p")]
        payloads = {1: first_page, 2: second_page}
        fetched_urls: list[str] = []
        downloaded: list[str] = []

        def fetch(url: str, **_kwargs: object) -> bytes:
            fetched_urls.append(url)
            page = int(url.rsplit("page=", 1)[1])
            return json.dumps(payloads[page]).encode()

        def download(url: str, destination, **_kwargs: object) -> dict[str, object]:  # noqa: ANN001
            del destination
            downloaded.append(url)
            suffix = url.rsplit("/", 2)[-2]
            return {"infohash": suffix}

        def variants(_request, release_name, torrent_url, _manifest, **_kwargs):  # noqa: ANN001
            if "S01E13" in release_name:
                return [{"provider": "magnet", "locator": f"torrent:{torrent_url}"}]
            return []

        with patch.object(search_sources, "_ANIMETOSHO_MAX_PAGES_PER_RUN", 1), patch.object(
            search_sources, "_animetosho_search_terms", return_value=["Mashle S01E13"],
        ), patch.object(search_sources, "_identity_query_bases", return_value=[]), patch.object(
            search_sources, "_fetch_bytes", side_effect=fetch,
        ), patch.object(search_sources, "_download_torrent", side_effect=download), patch.object(
            search_sources, "_torrent_candidate_variants", side_effect=variants,
        ):
            first = adapter._search_animetosho(
                request, set(), deadline=adapter.time.monotonic() + 100,
            )

        self.assertFalse(first.source_exhausted)
        self.assertEqual(first.query_cursor["page"], 2)
        # Every one of the 75 page-one manifests was validated as non-covering;
        # these are the only negative facts eligible for the next run.
        self.assertEqual(len(first.reviewed_torrent_miss_locators), 75)
        request["search_cursors"] = {"animetosho": first.query_cursor}
        request["reviewed_torrent_miss_locators"] = first.reviewed_torrent_miss_locators

        with patch.object(search_sources, "_ANIMETOSHO_MAX_PAGES_PER_RUN", 1), patch.object(
            search_sources, "_animetosho_search_terms", return_value=["Mashle S01E13"],
        ), patch.object(search_sources, "_identity_query_bases", return_value=[]), patch.object(
            search_sources, "_fetch_bytes", side_effect=fetch,
        ), patch.object(search_sources, "_download_torrent", side_effect=download), patch.object(
            search_sources, "_torrent_candidate_variants", side_effect=variants,
        ):
            second = adapter._search_animetosho(
                request, set(), deadline=adapter.time.monotonic() + 100,
            )

        self.assertTrue(second.source_exhausted is False)
        # The validated candidate short-circuits the otherwise expensive
        # remainder of page two.  The page is incomplete, so its cursor stays
        # at page two; once the selected gap is reconciled the request
        # fingerprint changes and the stale row is not replayed.
        self.assertEqual(second.query_cursor["page"], 2)
        self.assertEqual(len(second), 1)
        # The duplicate page-one row was skipped from its validated infohash;
        # only the new page-two manifest was fetched this run.
        self.assertEqual(len(downloaded), 76)
        self.assertIn("page=1", fetched_urls[0])
        self.assertIn("page=2", fetched_urls[-1])

    def test_http_or_json_failure_does_not_advance_page(self) -> None:
        request = _request()
        cursor = {
            "fingerprint": adapter._animetosho_request_fingerprint(
                request, ["Mashle"],
            ),
            "term_index": 0,
            "page": 2,
            "exhausted": False,
        }
        request["search_cursors"] = {"animetosho": cursor}

        with patch.object(search_sources, "_animetosho_search_terms", return_value=["Mashle"]), patch.object(
            search_sources, "_identity_query_bases", return_value=[]
        ), patch.object(search_sources, "_fetch_bytes", return_value=b"not-json"):
            result = adapter._search_animetosho(
                request, set(), deadline=adapter.time.monotonic() + 100,
            )

        self.assertEqual(result.query_cursor["page"], 2)
        self.assertFalse(result.source_exhausted)
        self.assertEqual(result.infrastructure_failures, 1)
        self.assertEqual(result.reviewed_torrent_miss_locators, [])

    def test_finished_cursor_is_revalidated_with_a_real_query(self) -> None:
        request = _request()
        terms = ["Mashle"]
        fingerprint = adapter._animetosho_request_fingerprint(request, terms)
        request["search_cursors"] = {"animetosho": {
            "fingerprint": fingerprint,
            "term_index": 1,
            "page": 7,
            "exhausted": True,
        }}
        calls: list[str] = []

        def fetch(url: str, **_kwargs: object) -> bytes:
            calls.append(url)
            return b"[]"

        with patch.object(search_sources, "_animetosho_search_terms", return_value=terms), patch.object(
            search_sources, "_identity_query_bases", return_value=[]
        ), patch.object(search_sources, "_fetch_bytes", side_effect=fetch):
            result = adapter._search_animetosho(
                request, set(), deadline=adapter.time.monotonic() + 100,
            )

        self.assertTrue(result.source_exhausted)
        self.assertEqual(result.query_attempts, 1)
        self.assertEqual(result.query_responses, 1)
        self.assertEqual(result.query_cursor["page"], 7)
        self.assertEqual(len(calls), 1)


if __name__ == "__main__":
    unittest.main()
