from __future__ import annotations

import unittest

from local.scrapeflow_api.provider_delivery import (
    ProviderDeliveryError,
    validate_provider_delivery,
)
from local.scrapeflow_api.replenishment_tiers import (
    TIER_LOCAL_MAGNET,
    TIER_QUARK_SHARE,
)


ROOT = "root-job"
ATTEMPT = "attempt-1"
STAGING = f"/quark/影视/ScrapeFlow/补源/{ROOT}/{ATTEMPT}"

def _delivery(lane: str) -> dict[str, object]:
    return {
        "lane": lane,
        "attempt_id": ATTEMPT,
        "staging_root": STAGING,
        "files": [{
            "path": f"{STAGING}/Season 01/Example.S01E01.mkv",
            "size": 1024 * 1024,
            "kind": "video",
            "gap_ids": ["S01E01"],
        }],
    }


class ProviderDeliveryContractTests(unittest.TestCase):
    def test_two_current_lanes_share_one_delivery_shape(self) -> None:
        for lane in (TIER_QUARK_SHARE, TIER_LOCAL_MAGNET):
            with self.subTest(lane=lane):
                result = validate_provider_delivery(
                    _delivery(lane),
                    root_job_id=ROOT,
                    attempt_id=ATTEMPT,
                )
                self.assertEqual(result["lane"], lane)
                self.assertEqual(result["staging_root"], STAGING)
                self.assertEqual(result["files"][0]["gap_ids"], ["S01E01"])

    def test_only_canonical_provider_parent_is_accepted(self) -> None:
        delivery = _delivery(TIER_QUARK_SHARE)
        with self.assertRaises(ProviderDeliveryError):
            validate_provider_delivery(
                delivery,
                root_job_id=ROOT,
                attempt_id=ATTEMPT,
                staging_parent="/another-root/ScrapeFlow/补源",
            )

    def test_formal_library_fields_are_rejected_anywhere(self) -> None:
        for key in ("formal_path", "target_root", "destination_parent", "movie_root", "tv_root"):
            delivery = _delivery(TIER_LOCAL_MAGNET)
            delivery["files"][0][key] = "/quark/影视/电影/Example"
            with self.subTest(key=key), self.assertRaises(ProviderDeliveryError):
                validate_provider_delivery(
                    delivery,
                    root_job_id=ROOT,
                    attempt_id=ATTEMPT,
                )

    def test_staging_root_must_belong_to_current_root_and_attempt(self) -> None:
        delivery = _delivery(TIER_QUARK_SHARE)
        delivery["staging_root"] = "/quark/影视/ScrapeFlow/补源/other/attempt-1"
        with self.assertRaises(ProviderDeliveryError):
            validate_provider_delivery(delivery, root_job_id=ROOT, attempt_id=ATTEMPT)

    def test_files_must_stay_inside_staging_and_bind_gap_ids(self) -> None:
        outside = _delivery(TIER_LOCAL_MAGNET)
        outside["files"][0]["path"] = "/quark/影视/番剧/Example/Example.S01E01.mkv"
        with self.assertRaises(ProviderDeliveryError):
            validate_provider_delivery(outside, root_job_id=ROOT, attempt_id=ATTEMPT)

        unbound = _delivery(TIER_LOCAL_MAGNET)
        unbound["files"][0]["gap_ids"] = []
        with self.assertRaises(ProviderDeliveryError):
            validate_provider_delivery(unbound, root_job_id=ROOT, attempt_id=ATTEMPT)

    def test_kind_must_match_media_extension(self) -> None:
        bad_video = _delivery(TIER_LOCAL_MAGNET)
        bad_video["files"][0]["path"] = f"{STAGING}/Example.txt"
        with self.assertRaises(ProviderDeliveryError):
            validate_provider_delivery(bad_video, root_job_id=ROOT, attempt_id=ATTEMPT)

        subtitle = _delivery(TIER_LOCAL_MAGNET)
        subtitle["files"][0] = {
            "path": f"{STAGING}/Example.S01E01.zh.srt",
            "size": 128,
            "kind": "subtitle",
            "gap_ids": ["missing_subtitle:1"],
        }
        result = validate_provider_delivery(subtitle, root_job_id=ROOT, attempt_id=ATTEMPT)
        self.assertEqual(result["files"][0]["kind"], "subtitle")

    def test_contract_rejects_status_roots_manifest_and_row_provenance(self) -> None:
        for key, value in (
            ("status", "ready"),
            ("media_staging_root", f"{STAGING}/media"),
            ("subtitle_staging_root", f"{STAGING}/subtitles"),
            ("manifest", {"files": []}),
        ):
            delivery = _delivery(TIER_LOCAL_MAGNET)
            delivery[key] = value
            with self.subTest(key=key), self.assertRaises(ProviderDeliveryError):
                validate_provider_delivery(
                    delivery, root_job_id=ROOT, attempt_id=ATTEMPT,
                )

        for key, value in (
            ("manifest_index", 1),
            ("provider_path", "Example.S01E01.mkv"),
            ("source_name", "Example.S01E01.mkv"),
        ):
            delivery = _delivery(TIER_LOCAL_MAGNET)
            delivery["files"][0][key] = value
            with self.subTest(row_key=key), self.assertRaises(ProviderDeliveryError):
                validate_provider_delivery(
                    delivery, root_job_id=ROOT, attempt_id=ATTEMPT,
                )

    def test_duplicate_paths_and_gap_bindings_are_rejected(self) -> None:
        duplicate_path = _delivery(TIER_QUARK_SHARE)
        duplicate_path["files"].append(dict(duplicate_path["files"][0]))
        with self.assertRaises(ProviderDeliveryError):
            validate_provider_delivery(
                duplicate_path, root_job_id=ROOT, attempt_id=ATTEMPT,
            )

        duplicate_gap = _delivery(TIER_LOCAL_MAGNET)
        duplicate_gap["files"][0]["gap_ids"] = ["S01E01", "S01E01"]
        with self.assertRaises(ProviderDeliveryError):
            validate_provider_delivery(
                duplicate_gap, root_job_id=ROOT, attempt_id=ATTEMPT,
            )

    def test_retired_alist_lane_is_not_an_accepted_delivery(self) -> None:
        with self.assertRaises(ProviderDeliveryError):
            validate_provider_delivery(
                _delivery("alist_offline"),
                root_job_id=ROOT,
                attempt_id=ATTEMPT,
            )


if __name__ == "__main__":
    unittest.main()
