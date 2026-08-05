import unittest
from unittest import mock

from engine.scrapeflow.replenishment_acquisition import (
    AcquisitionRouteError,
    QuarkFastSaveDeliveryError,
    QuarkFastSaveInfrastructureError,
    QuarkShareCandidateError,
    acquire_selection,
    acquisition_lane,
)


class ReplenishmentAcquisitionTests(unittest.TestCase):
    def share(self):
        return {
            "provider": "quark_share", "release_name": "Example S01E01",
            "locator": "quark-share:opaque-ref", "selected_gap_ids": ["S01E01"],
            "acquisition": {"kind": "quark_fast_save", "share_ref": "opaque-ref"},
        }

    def receipt(self):
        return {
            "status": "submitted", "destination": "/quark/inbox/fixture",
            "expected_files": [{"name": "Example.S01E01.mkv", "size": 123, "gap_ids": ["S01E01"]}],
        }

    def test_provider_kind_alignment_routes_share_and_torrent(self):
        self.assertEqual(acquisition_lane(self.share()), "quark_fast_save")
        self.assertEqual(acquisition_lane({
            "provider": "magnet", "acquisition": {"kind": "torrent"},
        }), "torrent")
        self.assertEqual(acquisition_lane({
            "provider": "quark_share", "acquisition": {
                "kind": "quark_sfx_archive", "payload_kind": "archive_payload",
                "archive_format": "sfx", "requires_extraction": True,
            },
        }), "quark_sfx_archive")
        self.assertEqual(acquisition_lane({
            "provider": "quark_magnet", "acquisition": {"kind": "quark_magnet_offline"},
        }), "quark_magnet_offline")
        with self.assertRaises(AcquisitionRouteError):
            acquisition_lane({"provider": "quark_share", "acquisition": {"kind": "torrent"}})

    def test_archive_payload_cannot_silently_use_direct_fast_save_lane(self):
        with self.assertRaisesRegex(AcquisitionRouteError, "archive payload"):
            acquisition_lane({
                "provider": "quark_share", "payload_kind": "archive_payload",
                "requires_extraction": True,
                "acquisition": {"kind": "quark_fast_save"},
            })

    def test_fast_save_is_injected_and_exact_arrival_is_verified(self):
        save = mock.Mock(return_value=self.receipt())
        verify = mock.Mock()
        torrent = mock.Mock()
        result = acquire_selection(
            self.share(), "/quark/inbox/fixture", fast_save=save,
            verify_arrival=verify, acquire_torrent=torrent,
        )
        self.assertEqual(result["status"], "ready")
        self.assertEqual(result["materialization"], "fast_save")
        self.assertEqual(result["saved_bytes"], 123)
        verify.assert_called_once_with("/quark/inbox/fixture", self.receipt()["expected_files"])
        torrent.assert_not_called()

    def test_torrent_lane_does_not_call_cloud_ports(self):
        torrent = mock.Mock(return_value={"status": "ready", "source_paths": ["/fixture"]})
        result = acquire_selection(
            {"provider": "magnet", "acquisition": {"kind": "torrent"}},
            "/fixture", fast_save=mock.Mock(), verify_arrival=mock.Mock(),
            acquire_torrent=torrent,
        )
        self.assertEqual(result["source_paths"], ["/fixture"])
        torrent.assert_called_once()

    def test_sfx_and_offline_require_their_own_injected_executors(self):
        common = {
            "destination": "/fixture", "fast_save": mock.Mock(),
            "verify_arrival": mock.Mock(), "acquire_torrent": mock.Mock(),
        }
        sfx = {"provider": "quark_share", "acquisition": {
            "kind": "quark_sfx_archive", "payload_kind": "archive_payload",
            "archive_format": "sfx", "requires_extraction": True,
        }}
        offline = {"provider": "quark_magnet", "acquisition": {"kind": "quark_magnet_offline"}}
        with self.assertRaises(AcquisitionRouteError):
            acquire_selection(sfx, **common)
        result = acquire_selection(
            sfx, **common,
            acquire_sfx=mock.Mock(return_value={"status": "ready", "materialization": "sfx_extract_upload"}),
        )
        self.assertEqual(result["materialization"], "sfx_extract_upload")
        result = acquire_selection(
            offline, **common,
            acquire_offline=mock.Mock(return_value={"status": "ready", "materialization": "cloud_offline"}),
        )
        self.assertEqual(result["materialization"], "cloud_offline")

    def test_visibility_failure_is_reusable_delivery_not_candidate_failure(self):
        with self.assertRaises(QuarkFastSaveDeliveryError) as raised:
            acquire_selection(
                self.share(), "/quark/inbox/fixture",
                fast_save=lambda *_args: self.receipt(),
                verify_arrival=mock.Mock(side_effect=RuntimeError("not visible")),
                acquire_torrent=mock.Mock(),
            )
        self.assertEqual(raised.exception.failure_scope, "delivery")
        self.assertTrue(raised.exception.reusable_candidate)
        self.assertFalse(raised.exception.exclude_candidate)

    def test_expired_share_is_candidate_failure_for_next_round_fallback(self):
        error = QuarkShareCandidateError("expired", self.share())
        with self.assertRaises(QuarkShareCandidateError) as raised:
            acquire_selection(
                self.share(), "/quark/inbox/fixture",
                fast_save=mock.Mock(side_effect=error), verify_arrival=mock.Mock(),
                acquire_torrent=mock.Mock(),
            )
        self.assertTrue(raised.exception.exclude_candidate)
        self.assertEqual(raised.exception.candidate["locator"], "quark-share:opaque-ref")

    def test_unexpected_bridge_error_is_infrastructure_without_exclusion(self):
        with self.assertRaises(QuarkFastSaveInfrastructureError) as raised:
            acquire_selection(
                self.share(), "/quark/inbox/fixture",
                fast_save=mock.Mock(side_effect=RuntimeError("bridge down")),
                verify_arrival=mock.Mock(), acquire_torrent=mock.Mock(),
            )
        self.assertEqual(raised.exception.failure_scope, "infrastructure")
        self.assertFalse(raised.exception.exclude_candidate)

    def test_receipt_missing_selected_gap_is_candidate_failure(self):
        receipt = self.receipt()
        receipt["expected_files"][0]["gap_ids"] = ["S01E02"]
        with self.assertRaises(QuarkShareCandidateError):
            acquire_selection(
                self.share(), "/quark/inbox/fixture",
                fast_save=lambda *_args: receipt, verify_arrival=mock.Mock(),
                acquire_torrent=mock.Mock(),
            )


if __name__ == "__main__":
    unittest.main()
