"""Narrow media-root to Provider-staging mapping tests."""

from __future__ import annotations

import unittest

from local.scrapeflow_api.provider_staging import (
    CANONICAL_REPLENISHMENT_STAGING_ROOT,
    PRODUCTION_MEDIA_ROOT,
    ProviderStagingPathError,
    media_root_for_provider_staging_root,
    replenishment_staging_root_for_media_root,
    validate_acceptance_media_root,
    validate_provider_staging_root,
)


ACCEPTANCE_MEDIA_ROOT = "/quark/影视/ScrapeFlow/验收/run-20260811-e30a0b8"
ACCEPTANCE_STAGING_ROOT = f"{ACCEPTANCE_MEDIA_ROOT}/ScrapeFlow/补源"


class ProviderStagingTests(unittest.TestCase):
    def test_production_mapping_stays_byte_for_byte_canonical(self) -> None:
        self.assertEqual(
            replenishment_staging_root_for_media_root(PRODUCTION_MEDIA_ROOT),
            CANONICAL_REPLENISHMENT_STAGING_ROOT,
        )
        self.assertEqual(
            media_root_for_provider_staging_root(
                CANONICAL_REPLENISHMENT_STAGING_ROOT
            ),
            PRODUCTION_MEDIA_ROOT,
        )

    def test_one_acceptance_run_has_one_derived_staging_parent(self) -> None:
        self.assertEqual(
            validate_acceptance_media_root(ACCEPTANCE_MEDIA_ROOT),
            ACCEPTANCE_MEDIA_ROOT,
        )
        self.assertEqual(
            replenishment_staging_root_for_media_root(ACCEPTANCE_MEDIA_ROOT),
            ACCEPTANCE_STAGING_ROOT,
        )
        self.assertEqual(
            validate_provider_staging_root(ACCEPTANCE_STAGING_ROOT),
            ACCEPTANCE_STAGING_ROOT,
        )
        self.assertEqual(
            media_root_for_provider_staging_root(ACCEPTANCE_STAGING_ROOT),
            ACCEPTANCE_MEDIA_ROOT,
        )

    def test_formal_broad_and_nearby_paths_are_not_provider_roots(self) -> None:
        for media_root in (
            "/quark/影视/电影",
            "/quark/影视/ScrapeFlow/验收",
            "/quark/影视/ScrapeFlow/验收/run-1/child",
            "/quark/影视/ScrapeFlow/补源",
            "/library",
        ):
            with self.subTest(media_root=media_root), self.assertRaises(
                ProviderStagingPathError
            ):
                replenishment_staging_root_for_media_root(media_root)

        for staging_root in (
            "/quark/影视/电影/ScrapeFlow/补源",
            "/quark/影视/ScrapeFlow/验收/run-1/补源",
            "/quark/影视/ScrapeFlow/验收/run-1/ScrapeFlow/补源/extra",
            "/library/ScrapeFlow/补源",
        ):
            with self.subTest(staging_root=staging_root), self.assertRaises(
                ProviderStagingPathError
            ):
                validate_provider_staging_root(staging_root)


if __name__ == "__main__":
    unittest.main()
