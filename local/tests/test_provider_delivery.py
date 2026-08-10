from __future__ import annotations

import unittest

from local.scrapeflow_api.provider_delivery import (
    ProviderDeliveryError,
    validate_provider_delivery,
)
from local.scrapeflow_api.replenishment_tiers import (
    TIER_LOCAL_MAGNET,
    TIER_QUARK_MAGNET,
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
    def test_three_lanes_share_one_delivery_shape(self) -> None:
        for lane in (TIER_QUARK_SHARE, TIER_QUARK_MAGNET, TIER_LOCAL_MAGNET):
            with self.subTest(lane=lane):
                result = validate_provider_delivery(
                    _delivery(lane),
                    root_job_id=ROOT,
                    attempt_id=ATTEMPT,
                )
                self.assertEqual(result["lane"], lane)
                self.assertEqual(result["staging_root"], STAGING)
                self.assertEqual(result["files"][0]["gap_ids"], ["S01E01"])

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
        outside = _delivery(TIER_QUARK_MAGNET)
        outside["files"][0]["path"] = "/quark/影视/番剧/Example/Example.S01E01.mkv"
        with self.assertRaises(ProviderDeliveryError):
            validate_provider_delivery(outside, root_job_id=ROOT, attempt_id=ATTEMPT)

        unbound = _delivery(TIER_QUARK_MAGNET)
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


if __name__ == "__main__":
    unittest.main()
