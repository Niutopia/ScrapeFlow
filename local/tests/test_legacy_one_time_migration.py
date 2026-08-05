"""Fail-closed retirement of the removed Local one-time owner runtime."""

from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import tarfile
import tempfile
import unittest
from unittest import mock

from local.scrapeflow_api import legacy_one_time_migration as migration


FIXED_NOW = datetime(2026, 8, 5, 4, 5, 6, tzinfo=timezone.utc)


def json_bytes(value: dict) -> bytes:
    return (json.dumps(value, ensure_ascii=False, indent=2) + "\n").encode("utf-8")


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


class LegacyOneTimeMigrationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory(prefix="legacy-owner-retirement-")
        self.root = Path(self.temporary.name)
        self.host = self.root / "host-state"
        self.state = self.host / "scrapeflow-data"
        self.jobs = self.state / "jobs"
        self.jobs.mkdir(parents=True)
        self.control = self.state / "global-control.json"
        self._write_control(paused=True)
        self._create_ordinary_job()

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def _write_control(self, *, paused: bool) -> None:
        self.control.write_bytes(json_bytes({
            "version": 1,
            "paused": paused,
            "updated_at": "2026-08-05T03:00:00+00:00",
            "reason": "legacy-owner-retirement" if paused else None,
        }))

    def _create_ordinary_job(self) -> None:
        directory = self.jobs / ("f" * 12)
        directory.mkdir()
        (directory / "job.json").write_bytes(json_bytes({
            "id": "f" * 12,
            "source": "/媒体库/待刮削/ordinary",
            "parent": "/媒体库/电影",
            "phase": "completed",
            "visibility": "user",
            "approval_source": None,
        }))

    def _create_owner(self, index: int, *, phase: str = "replenishing") -> str:
        job_id = f"{index:012x}"
        directory = self.jobs / job_id
        directory.mkdir()
        origin = {
            "schema_version": 1,
            "kind": "one_time_title_import_origin",
            "owner_job_id": job_id,
            "batch_sha256": hashlib.sha256(b"historical-batch").hexdigest(),
            "title_work_key": f"tmdb-tv-{index}",
        }
        job = {
            "id": job_id,
            "source": f"/媒体库/电视剧/Legacy {index} (2020) {{tmdb-{index}}}",
            "parent": "/媒体库/电视剧",
            "media_type": "tv",
            "absolute": False,
            "prefer_simplified": True,
            "tmdb_id": index,
            "visibility": "internal",
            "root_job_id": None,
            "created_at": "2026-08-01T00:00:00+00:00",
            "updated_at": "2026-08-01T00:00:00+00:00",
            "phase": phase,
            "error": None,
            "digest": None,
            "approval_source": migration.LEGACY_ONE_TIME_APPROVAL_SOURCE,
            "plan": {"one_time_import_origin": origin},
            "progress": {"stage": "one_time_import_retry_wait"},
            "replenishment_round": 2,
        }
        (directory / "job.json").write_bytes(json_bytes(job))
        (directory / "one-time-import-origin.json").write_bytes(json_bytes(origin))
        (directory / "one-time-import-journal.json").write_bytes(json_bytes({
            "schema_version": 1,
            "kind": "one_time_title_import_journal",
            "owner_job_id": job_id,
            "status": "retryable",
        }))
        (directory / "one-time-import-preparation.json").write_bytes(json_bytes({
            "schema_version": 1,
            "kind": "one_time_title_import_preparation",
            "title_work_key": f"tmdb-tv-{index}",
        }))
        (directory / "job.log").write_text("historical owner\n", encoding="utf-8")
        return job_id

    def _create_owners(
        self, active_count: int, terminal_count: int = 0,
    ) -> list[str]:
        active = [
            self._create_owner(index) for index in range(1, active_count + 1)
        ]
        terminal = [
            self._create_owner(active_count + index, phase="failed")
            for index in range(1, terminal_count + 1)
        ]
        return [*active, *terminal]

    def _create_descendant(
        self, index: int, parent_id: str, *, phase: str = "completed",
    ) -> str:
        job_id = f"{0x800000000000 + index:012x}"
        directory = self.jobs / job_id
        directory.mkdir()
        (directory / "job.json").write_bytes(json_bytes({
            "id": job_id,
            "source": f"/媒体库/ScrapeFlow/补源/legacy-child-{index}",
            "parent": "/媒体库/电视剧",
            "media_type": "tv",
            "absolute": False,
            "prefer_simplified": True,
            "visibility": "internal",
            "root_job_id": parent_id,
            "created_at": "2026-08-01T00:00:00+00:00",
            "updated_at": "2026-08-01T00:00:00+00:00",
            "phase": phase,
            "error": None,
            "digest": None,
            "approval_source": None,
            "plan": {"kind": "historical_replenishment_child"},
            "progress": None,
        }))
        empty = directory / "nested" / "empty"
        empty.mkdir(parents=True)
        empty.chmod(0o700)
        return job_id

    def _backup(self, *, snapshot_name: str = "2026-08-05T120000+0800") -> Path:
        snapshot = self.root / "backups" / snapshot_name
        snapshot.mkdir(parents=True)
        archive = snapshot / "scrapeflow-data.tar.gz"
        with tarfile.open(archive, "w:gz") as output:
            output.add(self.state, arcname="scrapeflow-data", recursive=True)
        manifest = {
            "schema_version": 1,
            "started_at": "2026-08-05T03:59:00+00:00",
            "completed_at": "2026-08-05T04:00:00+00:00",
            "state_root": str(self.host),
            "global_control": {
                "version": 1,
                "paused": True,
                "updated_at": "2026-08-05T03:00:00+00:00",
                "reason": "legacy-owner-retirement",
            },
            "scrapeflow_file_count": sum(1 for path in self.state.rglob("*") if path.is_file()),
            "paused_during_backup": True,
            "coordination": {
                "mode": "already_paused",
                "quiescence": {"waited_seconds": 0.0, "reasons": []},
            },
            "archives": {
                "scrapeflow-data.tar.gz": {
                    "bytes": archive.stat().st_size,
                    "sha256": sha256(archive),
                },
            },
        }
        manifest_path = snapshot / "manifest.json"
        manifest_path.write_bytes(json_bytes(manifest))
        return manifest_path

    def _rewrite_backup_member_mode(
        self, manifest_path: Path, member_name: str, mode: int,
    ) -> None:
        archive = manifest_path.parent / "scrapeflow-data.tar.gz"
        rewritten = archive.with_name("scrapeflow-data.mode-tampered.tar.gz")
        found = False
        with tarfile.open(archive, "r:gz") as source, tarfile.open(
            rewritten, "w:gz",
        ) as destination:
            for member in source.getmembers():
                stream = source.extractfile(member) if member.isfile() else None
                if member.name == member_name:
                    member.mode = mode
                    found = True
                destination.addfile(member, stream)
        self.assertTrue(found, f"backup member absent: {member_name}")
        rewritten.replace(archive)
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        archive_row = manifest["archives"]["scrapeflow-data.tar.gz"]
        archive_row["bytes"] = archive.stat().st_size
        archive_row["sha256"] = sha256(archive)
        manifest_path.write_bytes(json_bytes(manifest))

    def _state_bytes(self) -> dict[str, bytes]:
        return {
            path.relative_to(self.state).as_posix(): path.read_bytes()
            for path in sorted(self.state.rglob("*")) if path.is_file()
        }

    def _plan(
        self, active_count: int = 1, terminal_count: int = 0,
    ) -> tuple[dict, list[str], Path]:
        ids = self._create_owners(active_count, terminal_count)
        manifest = self._backup()
        plan = migration.build_retirement_plan(
            self.state, manifest,
            expected_active_count=active_count,
            expected_terminal_count=terminal_count,
            now=FIXED_NOW,
        )
        return plan, ids, manifest

    def test_plan_is_zero_write_for_all_sixteen_active_and_two_terminal_owners(self) -> None:
        ids = self._create_owners(16, 2)
        manifest = self._backup()
        before = self._state_bytes()

        plan = migration.build_retirement_plan(
            self.state, manifest,
            expected_active_count=16, expected_terminal_count=2,
            now=FIXED_NOW,
        )

        self.assertEqual(self._state_bytes(), before)
        self.assertEqual(plan["active_owner_count"], 16)
        self.assertEqual(plan["terminal_owner_count"], 2)
        self.assertEqual(plan["total_owner_count"], 18)
        self.assertEqual([row["job_id"] for row in plan["entries"]], ids)
        self.assertEqual(plan["default_mode"], "read_only")
        self.assertTrue(plan["backup"]["latest_snapshot_verified"])
        self.assertTrue(plan["apply_guards"]["approved_plan_sha256_required"])
        self.assertEqual(
            migration.validate_retirement_plan(plan)["plan_sha256"],
            plan["plan_sha256"],
        )
        for entry in plan["entries"]:
            self.assertEqual(entry["action"], "cancel_and_archive")
            self.assertTrue(entry["archive_target"].endswith("/" + entry["job_id"]))
            self.assertNotEqual(
                entry["original_job_sha256"], entry["cancelled_job_sha256"],
            )

    def test_operator_count_is_required_but_not_hardcoded(self) -> None:
        self._create_owners(3)
        manifest = self._backup()
        plan = migration.build_retirement_plan(
            self.state, manifest,
            expected_active_count=3, expected_terminal_count=0,
            now=FIXED_NOW,
        )
        self.assertEqual(plan["total_owner_count"], 3)
        with self.assertRaisesRegex(migration.LegacyOneTimeMigrationError, "count mismatch"):
            migration.build_retirement_plan(
                self.state, manifest,
                expected_active_count=16, expected_terminal_count=0,
                now=FIXED_NOW,
            )

    def test_unpaused_state_refuses_planning(self) -> None:
        self._create_owners(1)
        manifest = self._backup()
        self._write_control(paused=False)
        with self.assertRaisesRegex(migration.LegacyOneTimeMigrationError, "pause is required"):
            migration.build_retirement_plan(
                self.state, manifest,
                expected_active_count=1, expected_terminal_count=0,
                now=FIXED_NOW,
            )

    def test_backup_must_match_every_owner_file(self) -> None:
        ids = self._create_owners(1)
        manifest = self._backup()
        (self.jobs / ids[0] / "job.log").write_text("changed after backup\n", encoding="utf-8")
        with self.assertRaisesRegex(
            migration.LegacyOneTimeMigrationError, "backup does not match",
        ):
            migration.build_retirement_plan(
                self.state, manifest,
                expected_active_count=1, expected_terminal_count=0,
                now=FIXED_NOW,
            )

    def test_backup_rejects_rehashed_archive_with_tampered_file_mode(self) -> None:
        owner_id = self._create_owner(1)
        manifest = self._backup()
        self._rewrite_backup_member_mode(
            manifest,
            f"scrapeflow-data/jobs/{owner_id}/job.log",
            0o777,
        )

        with self.assertRaisesRegex(
            migration.LegacyOneTimeMigrationError, "file modes differ",
        ):
            migration.build_retirement_plan(
                self.state, manifest,
                expected_active_count=1, expected_terminal_count=0,
                now=FIXED_NOW,
            )

    def test_backup_rejects_rehashed_archive_with_tampered_empty_dir_mode(self) -> None:
        owner_id = self._create_owner(1)
        empty = self.jobs / owner_id / "nested" / "empty"
        empty.mkdir(parents=True)
        empty.chmod(0o710)
        manifest = self._backup()
        self._rewrite_backup_member_mode(
            manifest,
            f"scrapeflow-data/jobs/{owner_id}/nested/empty",
            0o777,
        )

        with self.assertRaisesRegex(
            migration.LegacyOneTimeMigrationError, "directory modes differ",
        ):
            migration.build_retirement_plan(
                self.state, manifest,
                expected_active_count=1, expected_terminal_count=0,
                now=FIXED_NOW,
            )

    def test_backup_manifest_must_be_latest(self) -> None:
        self._create_owners(1)
        manifest = self._backup()
        newer = self.root / "backups" / "2026-08-05T130000+0800"
        newer.mkdir()
        payload = json.loads(manifest.read_text(encoding="utf-8"))
        payload["completed_at"] = "2026-08-05T05:00:00+00:00"
        (newer / "manifest.json").write_bytes(json_bytes(payload))
        with self.assertRaisesRegex(migration.LegacyOneTimeMigrationError, "not the latest"):
            migration.build_retirement_plan(
                self.state, manifest,
                expected_active_count=1, expected_terminal_count=0,
                now=FIXED_NOW,
            )

    def test_ambiguous_artifact_without_marker_refuses_inventory(self) -> None:
        ordinary = self.jobs / ("f" * 12)
        (ordinary / "one-time-import-origin.json").write_bytes(json_bytes({"kind": "stale"}))
        self._create_owners(1)
        manifest = self._backup()
        with self.assertRaisesRegex(migration.LegacyOneTimeMigrationError, "ambiguous"):
            migration.build_retirement_plan(
                self.state, manifest,
                expected_active_count=1, expected_terminal_count=0,
                now=FIXED_NOW,
            )

    def test_sealed_plan_writes_only_outside_live_state(self) -> None:
        plan, _ids, _manifest = self._plan(1)
        before = self._state_bytes()
        destination = self.root / "sealed-plan.json"
        migration.write_retirement_plan(destination, plan)
        self.assertEqual(self._state_bytes(), before)
        self.assertEqual(migration.read_retirement_plan(destination), plan)
        with self.assertRaisesRegex(migration.LegacyOneTimeMigrationError, "outside"):
            migration.write_retirement_plan(self.state / "unsafe-plan.json", plan)

    def test_apply_requires_exact_approval_and_stopped_confirmation(self) -> None:
        plan, _ids, _manifest = self._plan(1)
        with self.assertRaisesRegex(migration.LegacyOneTimeMigrationError, "approved SHA"):
            migration.apply_retirement_plan(
                plan, approved_sha256="0" * 64, confirm_api_stopped=True,
            )
        with self.assertRaisesRegex(migration.LegacyOneTimeMigrationError, "stopped"):
            migration.apply_retirement_plan(
                plan,
                approved_sha256=plan["plan_sha256"],
                confirm_api_stopped=False,
            )

    def test_apply_and_restore_are_auditable_and_idempotent(self) -> None:
        plan, ids, _manifest = self._plan(3)
        originals = {
            job_id: (self.jobs / job_id / "job.json").read_bytes()
            for job_id in ids
        }
        applied = migration.apply_retirement_plan(
            plan,
            approved_sha256=plan["plan_sha256"],
            confirm_api_stopped=True,
        )
        self.assertEqual(applied["archived"], 3)
        for entry in plan["entries"]:
            self.assertFalse((self.jobs / entry["job_id"]).exists())
            archived = self.state / entry["archive_target"]
            payload = json.loads((archived / "job.json").read_text(encoding="utf-8"))
            self.assertEqual(payload["phase"], "cancelled")
            self.assertEqual(
                payload["plan"]["legacy_one_time_retirement"]["disposition"],
                "cancelled_and_archived",
            )
        self.assertEqual(
            migration.apply_retirement_plan(
                plan,
                approved_sha256=plan["plan_sha256"],
                confirm_api_stopped=True,
            )["mode"],
            "applied",
        )

        restored = migration.restore_retirement_plan(
            plan,
            approved_sha256=plan["plan_sha256"],
            confirm_api_stopped=True,
        )
        self.assertEqual(restored["restored"], 3)
        for job_id, raw in originals.items():
            self.assertEqual((self.jobs / job_id / "job.json").read_bytes(), raw)
        self.assertEqual(
            migration.restore_retirement_plan(
                plan,
                approved_sha256=plan["plan_sha256"],
                confirm_api_stopped=True,
            )["mode"],
            "restored",
        )
        journal = json.loads(Path(restored["journal"]).read_text(encoding="utf-8"))
        self.assertEqual(journal["status"], "restored")
        self.assertTrue(all(value == "original" for value in journal["positions"].values()))

    def test_crash_after_cancel_projection_resumes_without_scope_expansion(self) -> None:
        plan, ids, _manifest = self._plan(2)
        first = plan["entries"][0]
        source = self.state / first["source"]
        real_replace = migration.os.replace
        crashed = False

        def crash_before_first_archive(old, new):
            nonlocal crashed
            if not crashed and Path(old).is_dir():
                crashed = True
                raise OSError("simulated directory-rename crash")
            return real_replace(old, new)

        with mock.patch.object(migration.os, "replace", side_effect=crash_before_first_archive):
            with self.assertRaisesRegex(OSError, "simulated"):
                migration.apply_retirement_plan(
                    plan,
                    approved_sha256=plan["plan_sha256"],
                    confirm_api_stopped=True,
                )
        self.assertTrue(source.is_dir())
        self.assertEqual(
            json.loads((source / "job.json").read_text(encoding="utf-8"))["phase"],
            "cancelled",
        )
        result = migration.apply_retirement_plan(
            plan,
            approved_sha256=plan["plan_sha256"],
            confirm_api_stopped=True,
        )
        self.assertEqual(result["archived"], 2)
        self.assertEqual(
            sorted(path.name for path in (self.state / plan["archive_root"]).iterdir()),
            ids,
        )

    def test_cli_default_is_read_only_and_plan_output_is_explicit(self) -> None:
        self._create_owners(1)
        manifest = self._backup()
        script = Path(__file__).parents[1] / "tools" / "retire_legacy_one_time_owners.py"
        before = self._state_bytes()
        completed = subprocess.run(
            [
                sys.executable, str(script),
                "--state-root", str(self.state),
                "--backup-manifest", str(manifest),
                "--expected-active-count", "1",
                "--expected-terminal-count", "0",
            ],
            check=False, capture_output=True, text=True,
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)
        self.assertEqual(self._state_bytes(), before)
        summary = json.loads(completed.stdout)
        self.assertEqual(summary["mode"], "dry-run")
        self.assertIsNone(summary["plan"])
        self.assertNotIn("entries", summary)
        self.assertNotIn("job_json_base64", completed.stdout)

        output = self.root / "operator" / "plan.json"
        output.parent.mkdir()
        completed = subprocess.run(
            [
                sys.executable, str(script),
                "--state-root", str(self.state),
                "--backup-manifest", str(manifest),
                "--expected-active-count", "1",
                "--expected-terminal-count", "0",
                "--plan-output", str(output),
            ],
            check=False, capture_output=True, text=True,
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)
        self.assertTrue(output.is_file())
        self.assertEqual(output.stat().st_mode & 0o777, 0o600)
        self.assertEqual(self._state_bytes(), before)
        self.assertEqual(json.loads(completed.stdout)["mutation_performed"], False)

    def test_child_and_grandchild_are_sealed_archived_and_indexed_with_owner(self) -> None:
        owner_id = self._create_owner(1)
        child_id = self._create_descendant(1, owner_id)
        grandchild_id = self._create_descendant(2, child_id, phase="failed")
        manifest = self._backup()
        plan = migration.build_retirement_plan(
            self.state, manifest,
            expected_active_count=1, expected_terminal_count=0,
            now=FIXED_NOW,
        )
        self.assertEqual(plan["descendant_count"], 2)
        self.assertEqual(plan["total_entry_count"], 3)
        by_id = {entry["job_id"]: entry for entry in plan["entries"]}
        self.assertEqual(by_id[owner_id]["lineage_depth"], 0)
        self.assertEqual(by_id[child_id]["lineage_depth"], 1)
        self.assertEqual(by_id[grandchild_id]["lineage_depth"], 2)
        self.assertIn(
            {"path": "nested/empty", "mode": 0o700},
            by_id[child_id]["directories"],
        )

        result = migration.apply_retirement_plan(
            plan,
            approved_sha256=plan["plan_sha256"],
            confirm_api_stopped=True,
        )
        self.assertEqual(result["archived"], 3)
        for job_id in (owner_id, child_id, grandchild_id):
            self.assertFalse((self.jobs / job_id).exists())
            self.assertTrue((self.state / by_id[job_id]["archive_target"]).is_dir())
        index = migration.load_retired_root_index(self.state)
        self.assertEqual(
            index["roots"][owner_id]["member_job_ids"],
            sorted([owner_id, child_id, grandchild_id]),
        )
        self.assertEqual(index["roots"][owner_id]["status"], "retired")

        migration.restore_retirement_plan(
            plan,
            approved_sha256=plan["plan_sha256"],
            confirm_api_stopped=True,
        )
        self.assertTrue(all((self.jobs / job_id).is_dir() for job_id in (
            owner_id, child_id, grandchild_id,
        )))
        restored_index = migration.load_retired_root_index(self.state)
        self.assertEqual(restored_index["roots"][owner_id]["status"], "restored")

    def test_apply_refuses_descendant_added_after_sealed_plan(self) -> None:
        owner_id = self._create_owner(1)
        self._create_descendant(1, owner_id)
        manifest = self._backup()
        plan = migration.build_retirement_plan(
            self.state, manifest,
            expected_active_count=1, expected_terminal_count=0,
            now=FIXED_NOW,
        )
        unexpected_id = self._create_descendant(2, owner_id)
        with self.assertRaisesRegex(
            migration.LegacyOneTimeMigrationError, "descendant set changed",
        ):
            migration.apply_retirement_plan(
                plan,
                approved_sha256=plan["plan_sha256"],
                confirm_api_stopped=True,
            )
        self.assertTrue((self.jobs / owner_id).is_dir())
        self.assertTrue((self.jobs / unexpected_id).is_dir())
        self.assertFalse((self.state / "retired-one-time-roots.json").exists())

    def test_local_server_has_no_resident_one_time_control_plane(self) -> None:
        local_root = Path(__file__).parents[1]
        server = (local_root / "server.py").read_text(encoding="utf-8")
        for retired_symbol in (
            "dispatch_phase3_one_time_imports",
            "create_or_adopt_one_time_import_owner",
            "finalize_one_time_title_import",
            "start_one_time_import_owner",
            "request_one_time_import_review_now",
            "ONE_TIME_IMPORT_RETRY_PENDING",
            "PHASE3_BATCH_SHA256",
        ):
            self.assertNotIn(retired_symbol, server)
        self.assertNotRegex(server, r"approve\|recover\|retry\|cancel\|resolve\|review")
        self.assertIn("is_legacy_one_time_lineage(job)", server)
        self.assertIn("require_nonlegacy_job_mutation(job)", server)


if __name__ == "__main__":
    unittest.main()
