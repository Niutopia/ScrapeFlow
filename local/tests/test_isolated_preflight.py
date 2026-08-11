"""Tests for isolated acceptance declaration preflight checks."""

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from engine.scrapeflow.serialization import atomic_write_json
from local.scrapeflow_api.isolated_preflight import (
    isolated_preflight_issues,
    isolated_preflight_template,
    load_declaration,
)
from local.scrapeflow_api.offline_backup import MANIFEST_NAME, create_offline_backup


def _verified_backup_manifest(root: Path) -> Path:
    source = root / "backup-source"
    alist_source = source / "alist-data"
    scrapeflow_source = source / "scrapeflow-data"
    alist_source.mkdir(parents=True)
    scrapeflow_source.mkdir(parents=True)
    atomic_write_json(alist_source / "config.json", {"version": 1}, allow_nan=False)
    atomic_write_json(
        scrapeflow_source / "global-control.json",
        {
            "version": 1,
            "paused": True,
            "scheduler_paused": True,
            "persistent": True,
            "updated_at": "2026-08-10T00:00:00Z",
            "reason": "test pause",
        },
        allow_nan=False,
    )
    output = root / "backup"
    output.mkdir()
    create_offline_backup(
        alist_data=alist_source,
        scrapeflow_data=scrapeflow_source,
        output_dir=output,
        media_snapshot_note="test media recovery point",
        label="backup-one",
    )
    return output / "backup-one" / MANIFEST_NAME


def valid_declaration(root: Path) -> dict[str, object]:
    state_dir = root / "isolated-runtime" / "scrapeflow-data"
    alist_dir = root / "isolated-runtime" / "alist-data"
    state_dir.mkdir(parents=True)
    alist_dir.mkdir(parents=True)
    payload = isolated_preflight_template()
    payload.update({
        "scrapeflow_state_dir": str(state_dir),
        "alist_data_dir": str(alist_dir),
        "media_root": "/quark/影视/ScrapeFlow/验收/run-20260810",
        "storage_label": "isolated-quark-storage-20260810",
        "offline_backup_manifest": str(_verified_backup_manifest(root)),
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
        self.assertTrue(any("formal library shelf" in issue for issue in issues))

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

    def test_requires_existing_empty_writable_runtime_directories(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            declaration = valid_declaration(root)
            state_dir = Path(str(declaration["scrapeflow_state_dir"]))
            state_dir.joinpath("old-state.json").write_text("{}", encoding="utf-8")
            declaration["alist_data_dir"] = str(root / "missing-alist-data")

            issues = isolated_preflight_issues(declaration, root=root / "repo")

        self.assertTrue(any("scrapeflow_state_dir must be empty" in issue for issue in issues))
        self.assertTrue(any("alist_data_dir must exist" in issue for issue in issues))

    def test_reports_non_writable_isolated_runtime_directory(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            declaration = valid_declaration(root)
            with patch(
                "local.scrapeflow_api.isolated_preflight._directory_write_issue",
                return_value="alist_data_dir must be writable: test denial",
            ):
                issues = isolated_preflight_issues(declaration, root=root / "repo")

        self.assertTrue(any("must be writable" in issue for issue in issues))

    def test_rejects_formal_media_descendant_and_tampered_backup_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            declaration = valid_declaration(root)
            declaration["media_root"] = "/quark/影视/电影/验收/run-20260810"
            manifest_path = Path(str(declaration["offline_backup_manifest"]))
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            manifest["checks"]["copied_stats"]["alist_data"]["files"] += 1
            manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

            issues = isolated_preflight_issues(declaration, root=root / "repo")

        self.assertTrue(any("formal library shelf" in issue for issue in issues))
        self.assertTrue(any("valid verified backup" in issue for issue in issues))

    def test_load_declaration_requires_json_object(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "declaration.json"
            path.write_text(json.dumps(["not-object"]), encoding="utf-8")

            with self.assertRaises(ValueError):
                load_declaration(path)


if __name__ == "__main__":
    unittest.main()
