"""Regression coverage for the one real automatic provider lane."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from engine.scrapeflow.replenishment_acquisition import (
    AcquisitionRouteError,
    acquire_selection,
    acquisition_lane,
)
from engine.tools.replenishment_adapter.search import ReplenishmentSearchService
from local.scrapeflow_api.automatic_replenishment import (
    AutomaticReplenishmentError,
    LocalTorrentAutomaticMaterializer,
)
from local.scrapeflow_api.replenishment import select_replenishment_candidates


def _torrent_candidate() -> dict[str, object]:
    return {
        "provider": "magnet",
        "locator": "torrent:https://example.test/example.torrent",
        "release_name": "Example Show S01E01 1080p",
        "title": "Example Show",
        "files": ["Example.Show.S01E01.mkv"],
        "resolution": "1080p",
        "acquisition": {
            "kind": "torrent",
            "url": "https://example.test/example.torrent",
            "file_index_by_gap": {"S01E01": [1]},
            "file_size_by_index": {"1": 1024 * 1024},
            "file_path_by_index": {"1": "Example.Show.S01E01.mkv"},
        },
    }


def _cloud_share_candidate() -> dict[str, object]:
    return {
        "provider": "cloud_share",
        "locator": "https://example.test/share/opaque",
        "release_name": "Example Show S01E01 2160p",
        "title": "Example Show",
        "files": ["Example.Show.S01E01.mkv"],
        "resolution": "2160p",
        "acquisition": {
            "kind": "http",
            "url": "https://example.test/share/opaque",
        },
    }


class ProviderCapabilityTests(unittest.TestCase):
    def test_cloud_share_http_has_no_acquisition_lane_or_injected_fallback(self) -> None:
        cloud = _cloud_share_candidate()
        with self.assertRaises(AcquisitionRouteError):
            acquisition_lane(cloud)

        http = Mock(return_value={"status": "ready"})
        torrent = Mock(return_value={"status": "ready"})
        with self.assertRaises(AcquisitionRouteError):
            acquire_selection(
                cloud,
                "/task/staging",
                acquire_http=http,
                acquire_torrent=torrent,
            )
        http.assert_not_called()
        torrent.assert_not_called()

    def test_magnet_torrent_routes_to_the_only_executor(self) -> None:
        torrent = Mock(return_value={"status": "ready", "expected_files": []})
        result = acquire_selection(
            _torrent_candidate(),
            "/task/staging",
            acquire_torrent=torrent,
        )
        self.assertEqual(result["status"], "ready")
        torrent.assert_called_once()

    def test_search_filters_non_executable_candidates_and_reports_truthful_lanes(self) -> None:
        service = ReplenishmentSearchService(
            lambda _request: {
                "candidates": [_cloud_share_candidate(), _torrent_candidate()],
                "lane_status": {"cloud_share": {"status": "ready"}},
            },
        )
        result = service.run({})
        self.assertEqual([row["provider"] for row in result["candidates"]], ["magnet"])
        self.assertEqual(result["lane_status"]["magnet"]["status"], "ready")
        self.assertEqual(result["lane_status"]["cloud_share"]["status"], "unavailable")
        self.assertEqual(result["provider_rejections"], {"unsupported_provider": 1})

    def test_selector_rejects_cloud_share_and_selects_executable_magnet(self) -> None:
        request = {
            "media": {
                "tmdb_id": 1,
                "title": "Example Show",
                "aliases": ["Example Show"],
            },
            "gaps": [{
                "id": "S01E01",
                "kind": "missing_episode",
                "season": 1,
                "episodes": [1],
                "label": "Example Show S01E01",
            }],
        }
        # Provider selection is independent of episode-name parsing.  Keep
        # this regression focused on the capability gate rather than making
        # it another copy of the matching authority test matrix.
        gap_lookup = {"S01E01": request["gaps"][0]}
        with patch(
            "local.scrapeflow_api.replenishment._request_gap_ids",
            return_value=({"S01E01"}, gap_lookup),
        ), patch(
            "local.scrapeflow_api.replenishment._name_coverage",
            return_value={"S01E01"},
        ):
            result = select_replenishment_candidates(
                request, [_cloud_share_candidate(), _torrent_candidate()],
            )
        self.assertEqual(result["status"], "complete")
        self.assertEqual([row["provider"] for row in result["selections"]], ["magnet"])
        self.assertEqual(result["rejection_reasons"], {"unsupported_provider": 1})
        self.assertEqual(
            result["provider_chain_by_gap"]["S01E01"][0]["acquisition_kind"],
            "torrent",
        )

    def test_selector_rejects_magnet_without_a_torrent_acquisition(self) -> None:
        request = {
            "media": {"tmdb_id": 1, "title": "Example Show", "aliases": ["Example Show"]},
            "gaps": [{
                "id": "S01E01", "kind": "missing_episode", "season": 1,
                "episodes": [1], "label": "Example Show S01E01",
            }],
        }
        incomplete = _torrent_candidate()
        incomplete.pop("acquisition")
        gap_lookup = {"S01E01": request["gaps"][0]}
        with patch(
            "local.scrapeflow_api.replenishment._request_gap_ids",
            return_value=({"S01E01"}, gap_lookup),
        ):
            result = select_replenishment_candidates(request, [incomplete])
        self.assertEqual(result["status"], "no_match")
        self.assertEqual(
            result["rejection_reasons"],
            {"provider_acquisition_mismatch": 1},
        )

    def test_local_automatic_materializer_does_not_delegate_cloud_share(self) -> None:
        delegate = Mock()
        delegate.acquire.return_value = {"status": "ready"}
        materializer = LocalTorrentAutomaticMaterializer(delegate=delegate)
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaises(AutomaticReplenishmentError):
                materializer.acquire(
                    {},
                    [_cloud_share_candidate()],
                    staging_root="/library/ScrapeFlow/补源/job/attempt",
                    workspace=Path(directory),
                    alist=object(),
                )
            delegate.acquire.assert_not_called()

            result = materializer.acquire(
                {},
                [_torrent_candidate()],
                staging_root="/library/ScrapeFlow/补源/job/attempt",
                workspace=Path(directory),
                alist=object(),
            )
        self.assertEqual(result, {"status": "ready"})
        delegate.acquire.assert_called_once()


if __name__ == "__main__":
    unittest.main()
