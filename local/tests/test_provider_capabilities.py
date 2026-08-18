"""Regression coverage for the exact automatic provider lanes."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from engine.scrapeflow.provider_capabilities import (
    ACQUISITION_QUARK_FAST_SAVE,
    ACQUISITION_TORRENT,
    QUARK_HELPER_NAME,
    QUARK_HELPER_REQUIRED_ACTIONS,
    provider_capability_snapshot,
)
from engine.scrapeflow.replenishment_acquisition import (
    AcquisitionRouteError,
    acquire_selection,
    acquisition_lane,
)
from engine.tools.replenishment_adapter.search import (
    ReplenishmentSearchService,
    candidate_variants,
)
from local.scrapeflow_api.automatic_replenishment import (
    AutomaticReplenishmentError,
    LocalTorrentAutomaticMaterializer,
)
from local.scrapeflow_api.replenishment import select_replenishment_candidates
from local.scrapeflow_api.replenishment_tiers import TIER_LOCAL_MAGNET


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


def _quark_share_candidate() -> dict[str, object]:
    return {
        "provider": "quark_share",
        "locator": "quark_share:fixture-share",
        "release_name": "Example Show S01E01 1080p",
        "title": "Example Show",
        "files": ["Example.Show.S01E01.mkv"],
        "file_coverage": ["S01E01"],
        "resolution": "1080p",
        "acquisition": {
            "kind": "quark_fast_save",
            "share_id": "fixture-share",
            "share_url": "https://pan.quark.cn/s/fixture-share",
            "file_id_by_gap": {"S01E01": ["share-fid"]},
            "file_path_by_id": {"share-fid": "Example.Show.S01E01.mkv"},
            "file_size_by_id": {"share-fid": 1024 * 1024},
            "save_strategy": "server_side_copy",
            "requires_share_revalidation": True,
        },
    }


def _retired_alist_candidate() -> dict[str, object]:
    candidate = _torrent_candidate()
    candidate.update({
        "provider": "alist_offline",
        "locator": "alist_offline:0123456789012345678901234567890123456789",
        "acquisition": {"kind": "alist_offline"},
    })
    return candidate


def _legacy_http_candidate() -> dict[str, object]:
    return {
        "provider": "legacy_http",
        "locator": "https://example.test/share/opaque",
        "release_name": "Example Show S01E01 2160p",
        "title": "Example Show",
        "files": ["Example.Show.S01E01.mkv"],
        "resolution": "2160p",
        "acquisition": {"kind": "http", "url": "https://example.test/share/opaque"},
    }


def _request() -> dict[str, object]:
    return {
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


class ProviderCapabilityTests(unittest.TestCase):
    def test_snapshot_exposes_only_quark_and_exact_local_torrent(self) -> None:
        snapshot = provider_capability_snapshot()
        self.assertEqual(set(snapshot), {"quark_share", "magnet"})
        self.assertEqual(
            snapshot["quark_share"]["acquisition_kinds"],
            [ACQUISITION_QUARK_FAST_SAVE],
        )
        self.assertEqual(
            snapshot["magnet"]["acquisition_kinds"],
            [ACQUISITION_TORRENT],
        )
        self.assertEqual(
            snapshot["quark_share"]["runtime_dependency"],
            {
                "helper": QUARK_HELPER_NAME,
                "required_actions": list(QUARK_HELPER_REQUIRED_ACTIONS),
            },
        )
        self.assertEqual(snapshot["magnet"]["sfx"]["status"], "deferred")

    def test_unsupported_or_retired_provider_has_no_acquisition_lane(self) -> None:
        torrent = Mock(return_value={"status": "ready"})
        for candidate in (_legacy_http_candidate(), _retired_alist_candidate()):
            with self.subTest(provider=candidate["provider"]):
                with self.assertRaises(AcquisitionRouteError):
                    acquisition_lane(candidate)
                with self.assertRaises(AcquisitionRouteError):
                    acquire_selection(candidate, "/task/staging", acquire_torrent=torrent)
        torrent.assert_not_called()

    def test_magnet_and_quark_route_to_their_only_executors(self) -> None:
        torrent = Mock(return_value={"status": "ready", "expected_files": []})
        share = Mock(return_value={"status": "ready"})
        torrent_result = acquire_selection(
            _torrent_candidate(), "/task/staging", acquire_torrent=torrent,
        )
        share_result = acquire_selection(
            _quark_share_candidate(),
            "/task/staging",
            acquire_torrent=torrent,
            acquire_quark_share=share,
        )
        self.assertEqual(torrent_result["status"], "ready")
        self.assertEqual(share_result["status"], "ready")
        torrent.assert_called_once()
        share.assert_called_once()

    def test_search_filters_retired_alist_and_reports_two_lanes(self) -> None:
        service = ReplenishmentSearchService(
            lambda _request: {
                "candidates": [
                    _legacy_http_candidate(),
                    _quark_share_candidate(),
                    _retired_alist_candidate(),
                    _torrent_candidate(),
                ],
            },
        )
        result = service.run({})
        self.assertEqual(
            [row["provider"] for row in result["candidates"]],
            ["quark_share", "magnet"],
        )
        self.assertEqual(set(result["lane_status"]), {"quark_share", "magnet"})
        self.assertEqual(result["provider_rejections"], {"unsupported_provider": 2})

    def test_torrent_variants_never_project_an_alist_candidate(self) -> None:
        manifest = {
            "root": "Example Show",
            "infohash": "0123456789012345678901234567890123456789",
            "files": {
                1: {"path": "Example.Show.S01E01.mkv", "size": 1024 * 1024},
                2: {"path": "Extras/source.iso", "size": 7 * 1024 * 1024},
            },
        }
        rows = candidate_variants(
            _request(),
            "Example Show S01E01 1080p",
            "https://example.test/example.torrent",
            manifest,
            include_local=True,
        )
        self.assertEqual([row["provider"] for row in rows], ["magnet"])
        acquisition = rows[0]["acquisition"]
        self.assertEqual(acquisition["kind"], ACQUISITION_TORRENT)
        self.assertEqual(acquisition["file_index_by_gap"], {"S01E01": [1]})
        self.assertEqual(acquisition["selected_download_bytes"], 1024 * 1024)
        self.assertEqual(acquisition["manifest_member_count"], 2)

    def test_torrent_variant_requires_explicit_local_execution(self) -> None:
        rows = candidate_variants(
            _request(),
            "Example Show S01E01 1080p",
            "https://example.test/example.torrent",
            {
                "root": "Example Show",
                "infohash": "0123456789012345678901234567890123456789",
                "files": {1: {"path": "Example.Show.S01E01.mkv", "size": 1}},
            },
            include_local=False,
        )
        self.assertEqual(rows, [])

    def test_selector_rejects_retired_alist_and_prefers_quark(self) -> None:
        request = _request()
        gap_lookup = {"S01E01": request["gaps"][0]}
        with patch(
            "local.scrapeflow_api.replenishment._request_gap_ids",
            return_value=({"S01E01"}, gap_lookup),
        ), patch(
            "local.scrapeflow_api.replenishment._name_coverage",
            return_value={"S01E01"},
        ):
            result = select_replenishment_candidates(
                request,
                [_legacy_http_candidate(), _torrent_candidate(), _retired_alist_candidate(), _quark_share_candidate()],
            )
        self.assertEqual(result["status"], "complete")
        self.assertEqual([row["provider"] for row in result["selections"]], ["quark_share"])
        self.assertEqual(result["rejection_reasons"], {"unsupported_provider": 2})
        self.assertEqual(
            [row["provider"] for row in result["provider_chain_by_gap"]["S01E01"]],
            ["quark_share", "magnet"],
        )

    def test_selector_rejects_removed_alist_as_a_current_tier(self) -> None:
        request = {**_request(), "tier": "alist_offline"}
        with self.assertRaises(ValueError):
            select_replenishment_candidates(request, [_torrent_candidate()])

    def test_local_materializer_never_delegates_retired_provider(self) -> None:
        delegate = Mock()
        materializer = LocalTorrentAutomaticMaterializer(delegate=delegate)
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaises(AutomaticReplenishmentError):
                materializer.acquire(
                    {},
                    [_retired_alist_candidate()],
                    staging_root="/library/ScrapeFlow/补源/job/attempt",
                    workspace=Path(directory),
                    alist=object(),
                )
        delegate.acquire.assert_not_called()

    def test_local_materializer_calls_shared_archive_preprocessor(self) -> None:
        delivery = {
            "lane": TIER_LOCAL_MAGNET,
            "attempt_id": "attempt",
            "staging_root": "/library/ScrapeFlow/补源/job/attempt",
            "files": [{
                "path": "/library/ScrapeFlow/补源/job/attempt/Example.Show.S01E01.mkv",
                "size": 123,
                "kind": "video",
                "gap_ids": ["S01E01"],
            }],
        }
        delegate = Mock()
        delegate.acquire.return_value = delivery
        archive_adapter = Mock()
        archive_adapter.prepare_provider_delivery.return_value = {
            **delivery,
            "archive_preprocessed": True,
        }
        materializer = LocalTorrentAutomaticMaterializer(
            delegate=delegate,
            archive_preprocessor=archive_adapter,
        )
        with tempfile.TemporaryDirectory() as directory:
            result = materializer.acquire(
                {"media": {"tmdb_id": 1}},
                [_torrent_candidate()],
                staging_root="/library/ScrapeFlow/补源/job/attempt",
                workspace=Path(directory),
                alist=object(),
            )
        self.assertEqual(result, delivery)
        archive_adapter.prepare_provider_delivery.assert_called_once()
        self.assertNotIn(
            "formal_target",
            archive_adapter.prepare_provider_delivery.call_args.kwargs,
        )


if __name__ == "__main__":
    unittest.main()
