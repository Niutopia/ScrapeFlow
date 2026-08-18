"""Static deployment contracts for truthful runtime observability."""

from __future__ import annotations

import unittest
from pathlib import Path


class RuntimeObservabilityContractTests(unittest.TestCase):
    @property
    def root(self) -> Path:
        return Path(__file__).resolve().parents[2]

    def test_image_bakes_identity_into_oci_labels_and_process_environment(self) -> None:
        dockerfile = (self.root / "Dockerfile.api").read_text(encoding="utf-8")

        for marker in (
            "ARG SCRAPEFLOW_BUILD_VERSION=p15",
            "ARG SCRAPEFLOW_BUILD_COMMIT=unrecorded",
            "ARG SCRAPEFLOW_BUILD_TIME=unrecorded",
            "org.opencontainers.image.revision",
            "org.opencontainers.image.created",
            "SCRAPEFLOW_BUILD_COMMIT=${SCRAPEFLOW_BUILD_COMMIT}",
            "SCRAPEFLOW_BUILD_TIME=${SCRAPEFLOW_BUILD_TIME}",
        ):
            with self.subTest(marker=marker):
                self.assertIn(marker, dockerfile)

    def test_compose_supplies_build_args_without_erasing_baked_identity_at_runtime(self) -> None:
        compose = (self.root / "docker-compose.yml").read_text(encoding="utf-8")

        self.assertIn("SCRAPEFLOW_BUILD_COMMIT: ${SCRAPEFLOW_BUILD_COMMIT:-unrecorded}", compose)
        self.assertIn("SCRAPEFLOW_BUILD_TIME: ${SCRAPEFLOW_BUILD_TIME:-unrecorded}", compose)
        self.assertNotIn("SCRAPEFLOW_BUILD_COMMIT: ${SCRAPEFLOW_BUILD_COMMIT:-}", compose)
        self.assertNotIn("SCRAPEFLOW_BUILD_TIME: ${SCRAPEFLOW_BUILD_TIME:-}", compose)

    def test_compose_bounds_aria2_logs_and_pins_pansou_content(self) -> None:
        compose = (self.root / "docker-compose.yml").read_text(encoding="utf-8")
        offline_block = compose.split("  offline-aria2:\n", 1)[1].split("  quark-helper:\n", 1)[0]
        pansou_block = compose.split("  pansou:\n", 1)[1].split("  offline-aria2:\n", 1)[0]

        self.assertIn('max-size: "10m"', offline_block)
        self.assertIn('max-file: "3"', offline_block)
        self.assertIn("image: ghcr.io/fish2018/pansou@sha256:", pansou_block)
        self.assertNotIn("pansou:latest", pansou_block)


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
