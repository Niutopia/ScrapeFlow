"""Regression tests for the manual offline backup helper."""

from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from engine.scrapeflow.serialization import atomic_write_json
from local.scrapeflow_api.offline_backup import (
    MANIFEST_NAME,
    OfflineBackupError,
    create_offline_backup,
    restore_offline_backup,
    verify_offline_backup,
)


def _write_sqlite(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(path) as connection:
        connection.execute("CREATE TABLE sample (id INTEGER PRIMARY KEY, name TEXT)")
        connection.execute("INSERT INTO sample (name) VALUES ('ok')")


def _write_paused_control(root: Path, *, paused: bool = True) -> None:
    atomic_write_json(
        root / "global-control.json",
        {
            "version": 1,
            "paused": paused,
            "scheduler_paused": paused,
            "persistent": True,
            "updated_at": "2026-08-10T00:00:00Z",
            "reason": "test pause" if paused else None,
        },
        allow_nan=False,
    )


def _sample_state(root: Path) -> tuple[Path, Path, Path]:
    alist = root / "alist-data"
    scrapeflow = root / "scrapeflow-data"
    output = root / "backup-output"
    alist.mkdir()
    scrapeflow.mkdir()
    output.mkdir()
    _write_sqlite(alist / "data.db")
    atomic_write_json(
        alist / "config.json",
        {"version": 1, "driver": "fake"},
        allow_nan=False,
    )
    _write_paused_control(scrapeflow)
    atomic_write_json(
        scrapeflow / "jobs" / "job-1.json",
        {"id": "job-1", "phase": "completed"},
        allow_nan=False,
    )
    atomic_write_json(
        scrapeflow / "gaps" / "job-1" / "S01E01.json",
        {"id": "S01E01", "phase": "resolved"},
        allow_nan=False,
    )
    _write_sqlite(scrapeflow / "metadata.sqlite")
    (scrapeflow / "staging" / "root" / "attempt").mkdir(parents=True)
    (scrapeflow / "staging" / "root" / "attempt" / "payload.part").write_bytes(b"abc")
    return alist, scrapeflow, output


class OfflineBackupTests(unittest.TestCase):
    def test_create_verify_and_restore_preserve_paused_local_state(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            alist, scrapeflow, output = _sample_state(root)

            manifest = create_offline_backup(
                alist_data=alist,
                scrapeflow_data=scrapeflow,
                output_dir=output,
                media_snapshot_note="fake media snapshot at /snapshots/media",
                label="backup-one",
            )
            backup_dir = output / "backup-one"
            verified = verify_offline_backup(backup_dir)
            restored = restore_offline_backup(
                backup_dir=backup_dir,
                restore_dir=root / "isolated-restore",
            )
            manifest_exists = (backup_dir / MANIFEST_NAME).exists()
            restored_job_exists = Path(
                restored["scrapeflow_data"], "jobs", "job-1.json",
            ).exists()

        self.assertEqual(manifest["version"], 1)
        self.assertTrue(manifest_exists)
        self.assertTrue(manifest["control"]["paused"])
        self.assertEqual(
            manifest["checks"]["source_stats"],
            manifest["checks"]["copied_stats"],
        )
        self.assertGreaterEqual(
            manifest["checks"]["json"]["scrapeflow_data"]["checked"],
            3,
        )
        self.assertEqual(
            manifest["checks"]["sqlite_quick_check"]["alist_data"][0]["result"],
            "ok",
        )
        self.assertTrue(verified["control"]["paused"])
        self.assertTrue(restored["control"]["paused"])
        self.assertTrue(restored_job_exists)

    def test_create_refuses_unpaused_scrapeflow_state(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            alist, scrapeflow, output = _sample_state(root)
            _write_paused_control(scrapeflow, paused=False)

            with self.assertRaisesRegex(OfflineBackupError, "pause"):
                create_offline_backup(
                    alist_data=alist,
                    scrapeflow_data=scrapeflow,
                    output_dir=output,
                    media_snapshot_note="fake media snapshot",
                    label="backup-one",
                )

    def test_create_requires_media_snapshot_note(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            alist, scrapeflow, output = _sample_state(root)

            with self.assertRaisesRegex(OfflineBackupError, "正式媒体库"):
                create_offline_backup(
                    alist_data=alist,
                    scrapeflow_data=scrapeflow,
                    output_dir=output,
                    media_snapshot_note="",
                    label="backup-one",
                )

    def test_create_rejects_corrupt_json(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            alist, scrapeflow, output = _sample_state(root)
            (scrapeflow / "jobs" / "bad.json").write_text("{bad", encoding="utf-8")

            with self.assertRaisesRegex(OfflineBackupError, "JSON"):
                create_offline_backup(
                    alist_data=alist,
                    scrapeflow_data=scrapeflow,
                    output_dir=output,
                    media_snapshot_note="fake media snapshot",
                    label="backup-one",
                )

    def test_create_rejects_broken_sqlite(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            alist, scrapeflow, output = _sample_state(root)
            (alist / "broken.db").write_text("not sqlite", encoding="utf-8")

            with self.assertRaisesRegex(OfflineBackupError, "SQLite"):
                create_offline_backup(
                    alist_data=alist,
                    scrapeflow_data=scrapeflow,
                    output_dir=output,
                    media_snapshot_note="fake media snapshot",
                    label="backup-one",
                )

    def test_create_refuses_output_inside_state_source(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            alist, scrapeflow, _output = _sample_state(root)
            nested_output = scrapeflow / "nested-backups"

            with self.assertRaisesRegex(OfflineBackupError, "输出"):
                create_offline_backup(
                    alist_data=alist,
                    scrapeflow_data=scrapeflow,
                    output_dir=nested_output,
                    media_snapshot_note="fake media snapshot",
                    label="backup-one",
                )

    def test_manifest_contains_no_media_file_fingerprints(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            alist, scrapeflow, output = _sample_state(root)

            create_offline_backup(
                alist_data=alist,
                scrapeflow_data=scrapeflow,
                output_dir=output,
                media_snapshot_note="fake media snapshot",
                label="backup-one",
            )
            manifest_text = (output / "backup-one" / MANIFEST_NAME).read_text(
                encoding="utf-8",
            )
            manifest = json.loads(manifest_text)

        self.assertNotIn("media_file_fingerprints", manifest_text.casefold())
        self.assertNotIn("content_witness", manifest_text.casefold())
        self.assertEqual(manifest["media_library"]["status"], "external_snapshot_required")


if __name__ == "__main__":
    unittest.main()
