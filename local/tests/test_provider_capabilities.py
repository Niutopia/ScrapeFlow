"""Regression coverage for fixed automatic provider lanes."""

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
from engine.tools.replenishment_adapter.search import (
    ReplenishmentSearchService,
    candidate_variants,
)
from local.scrapeflow_api.automatic_replenishment import (
    AutomaticReplenishmentError,
    LocalTorrentAutomaticMaterializer,
)
from local.scrapeflow_api.replenishment import select_replenishment_candidates
from local.scrapeflow_api.replenishment_tiers import (
    TIER_LOCAL_MAGNET,
    TIER_QUARK_MAGNET,
)
from engine.scrapeflow.provider_capabilities import (
    ACQUISITION_QUARK_FAST_SAVE,
    ACQUISITION_QUARK_MAGNET_OFFLINE,
    ACQUISITION_TORRENT,
    QUARK_HELPER_NAME,
    QUARK_HELPER_REQUIRED_ACTIONS,
    provider_capability_snapshot,
)
from engine.tools._replenishment_local_adapter_impl import _locator_infohash_aliases


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


def _quark_magnet_candidate() -> dict[str, object]:
    torrent = _torrent_candidate()
    return {
        **torrent,
        "provider": "quark_magnet",
        "locator": "quark_magnet:0123456789012345678901234567890123456789",
        "infohash": "0123456789012345678901234567890123456789",
        "acquisition": {
            "kind": "quark_magnet_offline",
            "magnet_url": (
                "magnet:?xt=urn:btih:0123456789012345678901234567890123456789"
            ),
            "expected_files": [{
                "torrent_index": 1,
                "path": "Example.Show.S01E01.mkv",
                "size": 1024 * 1024,
                "gap_ids": ["S01E01"],
            }],
        },
    }


def _legacy_http_candidate() -> dict[str, object]:
    return {
        "provider": "legacy_http",
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
    def test_provider_snapshot_exposes_only_fixed_lanes(self) -> None:
        snapshot = provider_capability_snapshot()
        self.assertEqual(set(snapshot), {"quark_share", "quark_magnet", "magnet"})
        self.assertEqual(snapshot["quark_share"]["status"], "ready")
        self.assertEqual(
            snapshot["quark_share"]["acquisition_kinds"],
            [ACQUISITION_QUARK_FAST_SAVE],
        )
        self.assertEqual(
            snapshot["quark_magnet"]["acquisition_kinds"],
            [ACQUISITION_QUARK_MAGNET_OFFLINE],
        )
        self.assertEqual(
            snapshot["quark_magnet"]["status_scope"],
            "declared_materializer",
        )
        expected_helper_dependency = {
            "helper": QUARK_HELPER_NAME,
            "required_actions": list(QUARK_HELPER_REQUIRED_ACTIONS),
        }
        self.assertEqual(
            snapshot["quark_share"]["runtime_dependency"],
            expected_helper_dependency,
        )
        self.assertEqual(
            snapshot["quark_magnet"]["runtime_dependency"],
            expected_helper_dependency,
        )
        self.assertIsNot(
            snapshot["quark_share"]["runtime_dependency"],
            snapshot["quark_magnet"]["runtime_dependency"],
        )
        self.assertEqual(snapshot["magnet"]["sfx"]["status"], "deferred")

    def test_unsupported_provider_has_no_acquisition_lane_or_injected_fallback(self) -> None:
        legacy = _legacy_http_candidate()
        with self.assertRaises(AcquisitionRouteError):
            acquisition_lane(legacy)

        torrent = Mock(return_value={"status": "ready"})
        with self.assertRaises(AcquisitionRouteError):
            acquire_selection(
                legacy,
                "/task/staging",
                acquire_torrent=torrent,
            )
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

    def test_quark_share_routes_to_injected_fast_save_executor(self) -> None:
        share = Mock(return_value={"status": "ready"})
        torrent = Mock(return_value={"status": "ready"})
        result = acquire_selection(
            _quark_share_candidate(),
            "/task/staging",
            acquire_torrent=torrent,
            acquire_quark_share=share,
        )
        self.assertEqual(result["status"], "ready")
        share.assert_called_once()
        torrent.assert_not_called()

    def test_quark_magnet_routes_to_injected_offline_executor(self) -> None:
        offline = Mock(return_value={"status": "ready"})
        torrent = Mock(return_value={"status": "ready"})
        result = acquire_selection(
            _quark_magnet_candidate(),
            "/task/staging",
            acquire_torrent=torrent,
            acquire_quark_magnet=offline,
        )
        self.assertEqual(result["status"], "ready")
        offline.assert_called_once()
        torrent.assert_not_called()

    def test_search_filters_non_executable_candidates_and_reports_truthful_lanes(self) -> None:
        service = ReplenishmentSearchService(
            lambda _request: {
                "candidates": [
                    _legacy_http_candidate(),
                    _quark_share_candidate(),
                    _quark_magnet_candidate(),
                    _torrent_candidate(),
                ],
                "lane_status": {"legacy_http": {"status": "ready"}},
            },
        )
        result = service.run({})
        self.assertEqual(
            [row["provider"] for row in result["candidates"]],
            ["quark_share", "quark_magnet", "magnet"],
        )
        self.assertEqual(result["lane_status"]["quark_share"]["status"], "ready")
        self.assertEqual(result["lane_status"]["quark_magnet"]["status"], "ready")
        self.assertEqual(result["lane_status"]["magnet"]["status"], "ready")
        self.assertNotIn("legacy_http", result["lane_status"])
        self.assertEqual(result["provider_rejections"], {"unsupported_provider": 1})

    def test_verified_torrent_manifest_projects_quark_magnet_before_local(self) -> None:
        request = {
            "media": {"tmdb_id": 1, "title": "Example Show", "aliases": ["Example Show"]},
            "gaps": [{
                "id": "S01E01", "kind": "missing_episode", "season": 1,
                "episodes": [1], "label": "Example Show S01E01",
            }],
        }
        manifest = {
            "root": "Example Show",
            "infohash": "0123456789012345678901234567890123456789",
            "files": {1: {"path": "Example.Show.S01E01.mkv", "size": 1024 * 1024}},
        }

        rows = candidate_variants(
            request,
            "Example Show S01E01 1080p",
            "https://example.test/example.torrent",
            manifest,
            include_local=True,
        )

        self.assertEqual([row["provider"] for row in rows], ["quark_magnet", "magnet"])
        self.assertEqual(
            rows[0]["acquisition"]["kind"],
            ACQUISITION_QUARK_MAGNET_OFFLINE,
        )
        self.assertEqual(rows[1]["acquisition"]["kind"], ACQUISITION_TORRENT)

    def test_quark_magnet_locator_does_not_preexclude_local_torrent_hash(self) -> None:
        infohash = "0123456789012345678901234567890123456789"

        self.assertEqual(_locator_infohash_aliases({f"quark_magnet:{infohash}"}), set())
        self.assertIn(
            infohash,
            _locator_infohash_aliases({
                f"torrent:magnet:?xt=urn:btih:{infohash}",
            }),
        )

    def test_selector_rejects_unsupported_provider_and_prefers_quark_share(self) -> None:
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
                request, [
                    _legacy_http_candidate(),
                    _torrent_candidate(),
                    _quark_magnet_candidate(),
                    _quark_share_candidate(),
                ],
            )
        self.assertEqual(result["status"], "complete")
        self.assertEqual([row["provider"] for row in result["selections"]], ["quark_share"])
        self.assertEqual(result["rejection_reasons"], {"unsupported_provider": 1})
        self.assertEqual(
            result["provider_chain_by_gap"]["S01E01"][0]["acquisition_kind"],
            "quark_fast_save",
        )
        self.assertEqual(
            result["provider_chain_by_gap"]["S01E01"][1]["acquisition_kind"],
            "quark_magnet_offline",
        )
        self.assertEqual(
            result["provider_chain_by_gap"]["S01E01"][2]["acquisition_kind"],
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

    def test_selector_current_tier_never_falls_through_to_another_provider(self) -> None:
        request = {
            "tier": TIER_QUARK_MAGNET,
            "media": {
                "tmdb_id": 1,
                "title": "Example Show",
                "aliases": ["Example Show"],
            },
            "gaps": [{
                "id": "S01E01", "kind": "missing_episode", "season": 1,
                "episodes": [1], "label": "Example Show S01E01",
            }],
        }
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
                [
                    _quark_share_candidate(),
                    _quark_magnet_candidate(),
                    _torrent_candidate(),
                ],
            )

        self.assertEqual(result["tier"], TIER_QUARK_MAGNET)
        self.assertEqual(
            [row["provider"] for row in result["selections"]],
            [TIER_QUARK_MAGNET],
        )
        self.assertEqual(
            [row["provider"] for row in result["provider_chain_by_gap"]["S01E01"]],
            [TIER_QUARK_MAGNET],
        )
        self.assertEqual(result["unchecked_current_tier_candidate_count"], 0)

    def test_local_automatic_materializer_does_not_delegate_unsupported_provider(self) -> None:
        delegate = Mock()
        delegate.acquire.return_value = {
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
        materializer = LocalTorrentAutomaticMaterializer(delegate=delegate)
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaises(AutomaticReplenishmentError):
                materializer.acquire(
                    {},
                    [_legacy_http_candidate()],
                    staging_root="/library/ScrapeFlow/补源/job/attempt",
                    workspace=Path(directory),
                    alist=object(),
                )
            with self.assertRaises(AutomaticReplenishmentError):
                materializer.acquire(
                    {},
                    [_quark_share_candidate()],
                    staging_root="/library/ScrapeFlow/补源/job/attempt",
                    workspace=Path(directory),
                    alist=object(),
                )
            with self.assertRaises(AutomaticReplenishmentError):
                materializer.acquire(
                    {},
                    [_quark_magnet_candidate()],
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
        self.assertEqual(result, {
            "lane": TIER_LOCAL_MAGNET,
            "attempt_id": "attempt",
            "staging_root": "/library/ScrapeFlow/补源/job/attempt",
            "files": [{
                "path": "/library/ScrapeFlow/补源/job/attempt/Example.Show.S01E01.mkv",
                "size": 123,
                "kind": "video",
                "gap_ids": ["S01E01"],
            }],
        })
        delegate.acquire.assert_called_once()

    def test_local_automatic_materializer_calls_shared_archive_preprocessor(self) -> None:
        delegate = Mock()
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
        delegate.acquire.return_value = delivery
        archive_adapter = Mock()
        archive_adapter.prepare_provider_delivery.return_value = {
            **delivery, "archive_preprocessed": True,
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
        kwargs = archive_adapter.prepare_provider_delivery.call_args.kwargs
        self.assertEqual(kwargs["staging_root"], "/library/ScrapeFlow/补源/job/attempt")
        self.assertNotIn("formal_target", kwargs)


if __name__ == "__main__":
    unittest.main()
