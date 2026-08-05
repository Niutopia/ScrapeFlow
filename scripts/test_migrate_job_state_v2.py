#!/usr/bin/env python3
"""Tests for the offline, fail-closed job-state v2 migration."""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock


SCRIPT = Path(__file__).with_name("migrate_job_state_v2.py")
SPEC = importlib.util.spec_from_file_location("migrate_job_state_v2", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
migration = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(migration)


def json_bytes(value: object) -> bytes:
    return (
        json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    ).encode("utf-8")


def write_json(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(json_bytes(value))


def tree_bytes(root: Path) -> dict[str, bytes]:
    return {
        path.relative_to(root).as_posix(): path.read_bytes()
        for path in sorted(root.rglob("*"))
        if path.is_file()
    }


class JobStateV2MigrationTests(unittest.TestCase):
    def setUp(self) -> None:
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name) / "state"
        self.root.mkdir()
        self.control = self.root / "global-control.json"
        self.control_bytes = json_bytes({
            "version": 1,
            "paused": True,
            "updated_at": "2026-08-03T00:00:00+00:00",
            "reason": "test-migration",
        })
        self.control.write_bytes(self.control_bytes)
        (self.root / "jobs").mkdir()
        self.processed = self.root / "processed.json"
        self.processed.write_bytes(b'{"version":1,"paths":{}}\n')

    def make_job(
        self, job_id: str, phase: str, *, plan: dict[str, object] | None = None,
    ) -> Path:
        directory = self.root / "jobs" / job_id
        directory.mkdir()
        write_json(directory / "job.json", {
            "id": job_id,
            "phase": phase,
            "source": f"/inbox/{job_id}",
            "plan": plan,
            "progress": {"stage": "fixture"},
        })
        return directory

    def apply(self) -> dict[str, object]:
        return migration.run_migration(
            self.root, apply=True, confirm_api_stopped=True,
        )

    def test_dry_run_is_zero_write(self) -> None:
        self.make_job("a" * 12, "completed", plan={
            "target_root": "/library/show",
            "recovery_create_only": {"status": "activated"},
        })
        before = tree_bytes(self.root)

        result = migration.run_migration(self.root)

        self.assertEqual(result["mode"], "dry-run")
        self.assertEqual(result["actions"]["project"], 1)
        self.assertEqual(tree_bytes(self.root), before)
        self.assertFalse((self.root / migration.MANIFEST_RELATIVE_PATH).exists())
        self.assertFalse((self.root / "state-schema.json").exists())

    def test_unpaused_or_invalid_control_is_rejected(self) -> None:
        write_json(self.control, {
            "version": 1,
            "paused": False,
            "updated_at": "2026-08-03T00:00:00+00:00",
            "reason": None,
        })
        self.make_job("a" * 12, "completed", plan={})
        with self.assertRaisesRegex(migration.MigrationError, "pause is not active"):
            migration.run_migration(self.root)

        self.control.write_bytes(b'{"version":1,"paused":true')
        with self.assertRaisesRegex(migration.MigrationError, "invalid JSON"):
            migration.run_migration(self.root)

    def test_apply_requires_explicit_api_stopped_confirmation(self) -> None:
        self.make_job("a" * 12, "completed", plan={})
        before = tree_bytes(self.root)
        with self.assertRaisesRegex(migration.MigrationError, "confirm-api-stopped"):
            migration.run_migration(self.root, apply=True)
        self.assertEqual(tree_bytes(self.root), before)

    def test_superseded_directory_is_archived_byte_for_byte(self) -> None:
        directory = self.make_job("a" * 12, "superseded", plan={
            "superseded_by_job_id": "b" * 12,
            "supersession_provenance": {"delete": False},
        })
        (directory / "media-journal.json").write_bytes(b'{"step":2}\n')
        (directory / "job.log").write_bytes(b"original terminal log\n")
        nested = directory / "evidence" / "selection.json"
        nested.parent.mkdir()
        nested.write_bytes(b'{"selected":true}\n')
        before = tree_bytes(directory)

        result = self.apply()

        self.assertFalse(directory.exists())
        manifest_path = self.root / migration.MANIFEST_RELATIVE_PATH
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        entry = manifest["entries"][0]
        archive = self.root / entry["target"]
        self.assertEqual(tree_bytes(archive), before)
        self.assertEqual(entry["action"], "archive")
        self.assertEqual(
            entry["original_sha256"],
            migration._sha256_bytes(before["job.json"]),
        )
        self.assertEqual(result["mode"], "applied")
        self.assertEqual(
            json.loads((self.root / "state-schema.json").read_text())["version"], 2,
        )

    def test_live_phases_and_business_data_survive_field_projection(self) -> None:
        journal_bytes = b'{"success":false,"completed_steps":[1]}\n'
        jobs = (
            ("b" * 12, "completed", "activated"),
            ("c" * 12, "failed", "activated"),
            ("d" * 12, "replenishing", None),
        )
        before_business: dict[str, object] = {}
        for job_id, phase, marker_status in jobs:
            plan: dict[str, object] = {
                "target_root": f"/library/{job_id}",
                "replenishment": {"status": "awaiting_sources"},
                "superseded_at": "old-history",
                "superseded_by_job_id": "e" * 12,
                "superseded_from_phase": "queued",
                "supersession_kind": "legacy",
                "supersession_provenance": {"reason": "legacy"},
                "supersedes_job_id": "f" * 12,
            }
            if marker_status is not None:
                plan["recovery_create_only"] = {
                    "status": marker_status,
                    "recovery_baseline_digest": "0" * 64,
                }
            directory = self.make_job(job_id, phase, plan=plan)
            (directory / "media-journal.json").write_bytes(journal_bytes)
            before_business[job_id] = plan["replenishment"]
        processed_before = self.processed.read_bytes()

        self.apply()

        for job_id, phase, _marker_status in jobs:
            directory = self.root / "jobs" / job_id
            payload = json.loads((directory / "job.json").read_text(encoding="utf-8"))
            self.assertEqual(payload["phase"], phase)
            self.assertEqual(payload["plan"]["replenishment"], before_business[job_id])
            self.assertEqual(payload["plan"]["target_root"], f"/library/{job_id}")
            for field in migration.LEGACY_PLAN_FIELDS:
                self.assertNotIn(field, payload["plan"])
            self.assertEqual((directory / "media-journal.json").read_bytes(), journal_bytes)
        self.assertEqual(self.processed.read_bytes(), processed_before)
        self.assertEqual(self.control.read_bytes(), self.control_bytes)

    def test_interrupted_apply_can_be_rerun_from_manifest(self) -> None:
        archived = self.make_job("a" * 12, "superseded", plan={
            "superseded_at": "legacy",
        })
        archived_artifact = b"must survive interruption\n"
        (archived / "job.log").write_bytes(archived_artifact)
        self.make_job("b" * 12, "completed", plan={
            "target_root": "/library/b",
            "recovery_create_only": {"status": "activated"},
        })

        real_apply = migration._apply_entry
        calls = 0

        def interrupt_after_first(root: Path, entry: dict[str, object]) -> str:
            nonlocal calls
            result = real_apply(root, entry)
            calls += 1
            if calls == 1:
                raise RuntimeError("simulated process crash")
            return result

        with mock.patch.object(migration, "_apply_entry", side_effect=interrupt_after_first):
            with self.assertRaisesRegex(RuntimeError, "simulated process crash"):
                self.apply()

        manifest_path = self.root / migration.MANIFEST_RELATIVE_PATH
        self.assertTrue(manifest_path.exists())
        self.assertFalse(self.root.joinpath("jobs", "a" * 12).exists())
        self.assertFalse((self.root / "state-schema.json").exists())

        result = self.apply()

        self.assertEqual(result["mode"], "applied")
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        self.assertEqual(manifest["status"], "completed")
        archive = self.root / manifest["entries"][0]["target"]
        self.assertEqual((archive / "job.log").read_bytes(), archived_artifact)
        projected = json.loads(
            self.root.joinpath("jobs", "b" * 12, "job.json").read_text()
        )
        self.assertNotIn("recovery_create_only", projected["plan"])

        again = self.apply()
        self.assertEqual(again["mode"], "already-completed")

    def test_sha_conflict_after_manifest_is_rejected(self) -> None:
        directory = self.make_job("a" * 12, "completed", plan={
            "recovery_create_only": {"status": "activated"},
        })

        with mock.patch.object(
            migration, "_apply_entry", side_effect=RuntimeError("simulated crash"),
        ):
            with self.assertRaisesRegex(RuntimeError, "simulated crash"):
                self.apply()
        job_path = directory / "job.json"
        payload = json.loads(job_path.read_text())
        payload["source"] = "/changed-after-manifest"
        write_json(job_path, payload)
        schema_before = (self.root / "state-schema.json").exists()

        with self.assertRaisesRegex(migration.MigrationError, "SHA conflict"):
            self.apply()

        self.assertFalse(schema_before)
        self.assertFalse((self.root / "state-schema.json").exists())
        manifest = json.loads(
            (self.root / migration.MANIFEST_RELATIVE_PATH).read_text()
        )
        self.assertEqual(manifest["status"], "applying")


if __name__ == "__main__":
    unittest.main()
