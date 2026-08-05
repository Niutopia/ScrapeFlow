import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from local.scrapeflow_api import config


class LocalEnvLoadingTests(unittest.TestCase):
    def test_docker_specific_tmdb_settings_override_compose_fallbacks(self):
        with mock.patch.dict(os.environ, {
            "SCRAPEFLOW_DOCKER": "1",
            "TMDB_BASE_URL": "https://api.themoviedb.org/3",
            "TMDB_BASE_URL_DOCKER": "https://api.tmdb.org/3",
            "TMDB_PROXY_URL": "http://127.0.0.1:7897",
            "TMDB_PROXY_URL_DOCKER": "http://host.docker.internal:7897",
        }, clear=True):
            config.apply_docker_env_overrides()
            self.assertEqual(
                os.environ["TMDB_BASE_URL"], "https://api.tmdb.org/3",
            )
            self.assertEqual(
                os.environ["TMDB_PROXY_URL"],
                "http://host.docker.internal:7897",
            )

    def test_docker_specific_tmdb_settings_do_not_affect_local_runtime(self):
        with mock.patch.dict(os.environ, {
            "SCRAPEFLOW_DOCKER": "0",
            "TMDB_BASE_URL": "https://api.themoviedb.org/3",
            "TMDB_BASE_URL_DOCKER": "https://api.tmdb.org/3",
        }, clear=True):
            config.apply_docker_env_overrides()
            self.assertEqual(
                os.environ["TMDB_BASE_URL"],
                "https://api.themoviedb.org/3",
            )

    def test_non_docker_local_env_loads_replenishment_automation_settings(self):
        settings = {
            "SCRAPEFLOW_DOCKER": "0",
            "SCRAPEFLOW_REPLENISHMENT_RETRY_DELAY": "30",
            "SCRAPEFLOW_REPLENISHMENT_MIN_CLOUD_ATTEMPTS": "30",
            "SCRAPEFLOW_REPLENISHMENT_QUARK_INDEX": "/tmp/quark-share-index.json",
            "SCRAPEFLOW_REPLENISHMENT_QUARK_OFFLINE": "1",
            "SCRAPEFLOW_QUARK_HELPER_URL": "http://127.0.0.1:18765",
            "SCRAPEFLOW_QUARK_HELPER_TOKEN": "local-helper-token",
            "SCRAPEFLOW_QUARK_HELPER_TIMEOUT": "90",
            "SCRAPEFLOW_QUARK_OFFLINE_PROGRESS_POLLS": "8",
            "SCRAPEFLOW_REPLENISHMENT_OFFLINE_RECOVERY_ATTEMPTS": "4",
            "SCRAPEFLOW_REPLENISHMENT_OFFLINE_RECOVERY_DELAY": "7",
            "SCRAPEFLOW_REPLENISHMENT_OFFLINE_COOLDOWN": "300",
            "SCRAPEFLOW_REPLENISHMENT_OFFLINE_TIMEOUT": "1800",
            "SCRAPEFLOW_REPLENISHMENT_DYNAMIC_SEARCH_TIMEOUT": "45",
            "SCRAPEFLOW_REPLENISHMENT_SUBSPLEASE_SEARCH": "1",
            "SCRAPEFLOW_REPLENISHMENT_MIKAN_SEARCH": "1",
            "SCRAPEFLOW_REPLENISHMENT_ACG_PROXY": "http://127.0.0.1:7897",
            "TMDB_PROXY_URL": "http://127.0.0.1:7897",
            "SCRAPEFLOW_REPLENISHMENT_ARRIVAL_TIMEOUT": "120",
            "SCRAPEFLOW_REPLENISHMENT_ARCHIVE_STAGING_ROOT": "/tmp/archive-staging",
        }
        env_text = "\n".join(
            [*(f'{key}="{value}"' for key, value in settings.items()),
             "SCRAPEFLOW_NOT_ALLOWED=must-not-load"]
        )

        with tempfile.TemporaryDirectory() as directory:
            env_path = Path(directory) / ".env.local"
            env_path.write_text(env_text, encoding="utf-8")
            with mock.patch.dict(os.environ, {}, clear=True):
                config.load_local_env(env_path)
                self.assertEqual(
                    {key: os.environ.get(key) for key in settings}, settings,
                )
                self.assertNotIn("SCRAPEFLOW_NOT_ALLOWED", os.environ)

    def test_cloud_attempt_floor_cannot_be_configured_below_thirty(self):
        with mock.patch.dict(os.environ, {
            "SCRAPEFLOW_REPLENISHMENT_MIN_CLOUD_ATTEMPTS": "29",
        }, clear=False):
            with self.assertRaisesRegex(ValueError, "30–1000"):
                config.replenishment_min_cloud_attempts()

        with mock.patch.dict(os.environ, {
            "SCRAPEFLOW_REPLENISHMENT_MIN_CLOUD_ATTEMPTS": "30",
        }, clear=False):
            self.assertEqual(config.replenishment_min_cloud_attempts(), 30)

    def test_process_environment_wins_over_local_env(self):
        with tempfile.TemporaryDirectory() as directory:
            env_path = Path(directory) / ".env.local"
            env_path.write_text(
                "SCRAPEFLOW_QUARK_HELPER_TOKEN=file-token\n",
                encoding="utf-8",
            )
            with mock.patch.dict(
                os.environ,
                {"SCRAPEFLOW_QUARK_HELPER_TOKEN": "process-token"},
                clear=True,
            ):
                config.load_local_env(env_path)
                self.assertEqual(
                    os.environ["SCRAPEFLOW_QUARK_HELPER_TOKEN"],
                    "process-token",
                )


if __name__ == "__main__":
    unittest.main()
