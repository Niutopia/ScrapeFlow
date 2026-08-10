"""Tests for isolated acceptance declaration preflight checks."""

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from local.scrapeflow_api.isolated_preflight import (
    isolated_preflight_issues,
    isolated_preflight_template,
    load_declaration,
)


def valid_declaration(root: Path) -> dict[str, object]:
    payload = isolated_preflight_template()
    payload.update({
        "scrapeflow_state_dir": str(root / "state" / "scrapeflow-data"),
        "alist_data_dir": str(root / "state" / "alist-data"),
        "media_root": "/quark/影视/ScrapeFlow/验收/run-20260810",
        "storage_label": "isolated-quark-storage-20260810",
        "offline_backup_manifest": str(root / "backup" / "scrapeflow-offline-backup.json"),
        "media_recovery_point": "snapshot:isolated-media-before-acceptance",
    })
    return payload


class IsolatedPreflightTests(unittest.TestCase):
    def test_valid_declaration_passes(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            declaration = valid_declaration(root)

            issues = isolated_preflight_issues(declaration, root=root / "repo")

        self.assertEqual(issues, [])

    def test_unmodified_template_is_not_ready(self) -> None:
        declaration = isolated_preflight_template()

        issues = isolated_preflight_issues(declaration, root=Path("/tmp/repo"))

        self.assertTrue(any("scrapeflow_state_dir" in issue for issue in issues))
        self.assertTrue(any("alist_data_dir" in issue for issue in issues))
        self.assertTrue(any("media_root" in issue for issue in issues))
        self.assertTrue(any("storage_label" in issue for issue in issues))
        self.assertTrue(any("offline_backup_manifest" in issue for issue in issues))
        self.assertTrue(any("media_recovery_point" in issue for issue in issues))

    def test_rejects_non_loopback_urls_and_runtime_inside_repo(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            repo = root / "repo"
            repo.mkdir()
            declaration = valid_declaration(root)
            declaration.update({
                "api_url": "http://192.0.2.10:8765",
                "alist_url": "https://example.invalid:5244",
                "scrapeflow_state_dir": str(repo / "state"),
            })

            issues = isolated_preflight_issues(declaration, root=repo)

        self.assertTrue(any("api_url" in issue for issue in issues))
        self.assertTrue(any("alist_url" in issue for issue in issues))
        self.assertTrue(any("scrapeflow_state_dir" in issue for issue in issues))

    def test_rejects_nested_state_paths_and_formal_media_root(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            declaration = valid_declaration(root)
            declaration.update({
                "scrapeflow_state_dir": str(root / "isolated"),
                "alist_data_dir": str(root / "isolated" / "alist-data"),
                "media_root": "/quark/影视/电影",
            })

            issues = isolated_preflight_issues(declaration, root=root / "repo")

        self.assertTrue(any("must not be nested" in issue for issue in issues))
        self.assertTrue(any("formal library shelves" in issue for issue in issues))

    def test_rejects_gate_worker_and_recovery_drift(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            declaration = valid_declaration(root)
            declaration.update({
                "provider_workers": 2,
                "start_paused": False,
                "provider_auto_repair": True,
                "one_task_at_a_time": False,
                "offline_backup_manifest": "",
                "media_recovery_point": "",
                "bulk_cleanup": True,
            })

            issues = isolated_preflight_issues(declaration, root=root / "repo")

        self.assertTrue(any("provider_workers" in issue for issue in issues))
        self.assertTrue(any("start_paused" in issue for issue in issues))
        self.assertTrue(any("provider auto repair" in issue for issue in issues))
        self.assertTrue(any("one_task_at_a_time" in issue for issue in issues))
        self.assertTrue(any("offline_backup_manifest" in issue for issue in issues))
        self.assertTrue(any("media_recovery_point" in issue for issue in issues))
        self.assertTrue(any("bulk cleanup" in issue for issue in issues))

    def test_load_declaration_requires_json_object(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "declaration.json"
            path.write_text(json.dumps(["not-object"]), encoding="utf-8")

            with self.assertRaises(ValueError):
                load_declaration(path)


if __name__ == "__main__":
    unittest.main()
