from __future__ import annotations

import hashlib
import io
import json
import os
import tempfile
import unittest
from pathlib import Path

from engine.scrapeflow.formal_library_remediation import (
    build_formal_remediation_plan,
    canonical_digest,
    plan_hybrid_specs,
)
from engine.scrapeflow.remote_file_transaction import RemoteFileInfo
from local.scrapeflow_api.formal_library_maintenance import (
    FormalMaintenanceError,
    accept_and_commit_formal_remediation,
    build_formal_maintenance_acceptance,
    build_formal_post_audit_evidence,
    execute_formal_remediation,
    formal_maintenance_acceptance_is_valid,
)


class MemoryRemote:
    def __init__(self, files):
        self.files = dict(files)
        self.directories = set()

    def stat_exact(self, path):
        payload = self.files.get(path)
        if payload is None:
            return None
        digest = hashlib.sha256(payload).hexdigest()
        return RemoteFileInfo(size=len(payload), sha256=digest, version=digest)

    def open_reader(self, path):
        if path not in self.files:
            raise FileNotFoundError(path)
        return io.BytesIO(self.files[path])

    def upload_file_once(self, path, source, content_type):
        if path in self.files:
            raise FileExistsError(path)
        self.files[path] = source.read_bytes()

    def remove_file(self, path):
        self.files.pop(path, None)

    def ensure_directory(self, path):
        self.directories.add(path)


def fixture(payload=b"rick"):
    source = "/quark/影视/番剧/瑞克和MD 1-9季+日漫版/日漫/Season 01/old.mkv"
    digest = hashlib.sha256(payload).hexdigest()
    audit = {
        "schema_version": 1, "library_root": "/quark/影视/番剧",
        "projects": [], "movies": [], "residual_inventory": [],
        "uncovered_media": [], "multipart_media": [],
        "inventory_paths": [source],
    }
    work_order = {
        "schema_version": 1, "plan_id": "rick-maintenance-20260805",
        "container_root": "/quark/影视/番剧/瑞克和MD 1-9季+日漫版",
        "root_identity": {"namespace": "tmdb.tv", "metadata_id": 60625},
        "works": [
            {"member_key": "main", "identity": {"namespace": "tmdb.tv", "metadata_id": 60625},
             "title": "瑞克和莫蒂", "leaf_name": "瑞克和莫蒂", "poster_path": None},
            {"member_key": "anime", "identity": {"namespace": "tmdb.tv", "metadata_id": 202102},
             "title": "瑞克和莫蒂：日漫版", "leaf_name": "瑞克和莫蒂：日漫版", "poster_path": None},
        ],
        "operations": [{
            "item_id": "anime-s01e01", "operation": "transfer", "kind": "media",
            "member_key": "anime", "source_path": source,
            "relative_path": "Season 01/瑞克和莫蒂：日漫版 S01E01.mkv",
            "expected_size": len(payload), "expected_sha256": digest,
            "content_type": "video/x-matroska", "episode_key": "tmdb.tv:202102:S01E01",
            "edition_key": "default", "duplicate_group": None, "retained_path": None,
            "retained_size": None, "retained_sha256": None,
        }],
        "duplicate_edition_evidence": {},
        "source_inventory": {source: {"expected_size": len(payload), "expected_sha256": digest}},
        "staged_artifacts": {},
        "rollback_root": "/quark/影视/ScrapeFlow/事务回滚",
    }
    return build_formal_remediation_plan(audit, work_order), source, payload


def write_pause(path: Path, paused=True):
    os.environ["SCRAPEFLOW_STATE_DIR"] = str(path.parent)
    core = {
        "version": 1, "paused": paused,
        "reason": "formal-library-maintenance", "updated_at": "2026-08-05T00:00:00Z",
    }
    path.write_text(json.dumps(core), "utf-8")


def post_audit(plan, spec, payload, source, *, blocking=None):
    library_audit = {
        "schema_version": 1, "library_root": plan["library_root"],
        "projects": [], "movies": [], "summary": {},
        "canonical_tree_sha256": canonical_digest(plan["canonical_tree"]),
    }
    return build_formal_post_audit_evidence(
        library_audit,
        target_witnesses={spec.target_path: {
            "size": len(payload), "sha256": hashlib.sha256(payload).hexdigest(),
        }},
        absent_paths=[source], blocking_issue_codes=blocking or [],
    )


class FormalLibraryMaintenanceTests(unittest.TestCase):
    def test_rick_batch_executes_sealed_then_local_post_audit_commits(self):
        plan, source, payload = fixture()
        spec = plan_hybrid_specs(plan)[0]
        remote = MemoryRemote({source: payload})
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "transactions"
            pause = Path(tmp) / "global-control.json"
            write_pause(pause)
            receipt = execute_formal_remediation(
                remote, state_root=root, pause_receipt_path=pause,
                plan=plan, approved_plan_sha256=plan["plan_sha256"],
            )
            self.assertFalse(receipt["committed"])
            self.assertNotIn(source, remote.files)
            self.assertEqual(remote.files[spec.target_path], payload)
            self.assertIn(spec.rollback_path, remote.files)
            acceptance = build_formal_maintenance_acceptance(
                plan, post_audit_evidence=post_audit(plan, spec, payload, source),
            )
            committed = accept_and_commit_formal_remediation(
                remote, state_root=root, pause_receipt_path=pause,
                plan=plan, approved_plan_sha256=plan["plan_sha256"],
                acceptance=acceptance,
                post_audit_reader=lambda: acceptance["post_audit_evidence"]["library_audit"],
            )
            self.assertTrue(committed["committed"])
            self.assertNotIn(spec.rollback_path, remote.files)

    def test_pause_and_explicit_plan_sha_are_required_before_remote_writes(self):
        plan, source, payload = fixture()
        remote = MemoryRemote({source: payload})
        with tempfile.TemporaryDirectory() as tmp:
            pause = Path(tmp) / "global-control.json"
            write_pause(pause, paused=False)
            with self.assertRaises(FormalMaintenanceError):
                execute_formal_remediation(
                    remote, state_root=Path(tmp) / "tx", pause_receipt_path=pause,
                    plan=plan, approved_plan_sha256="0" * 64,
                )
            self.assertEqual(remote.files, {source: payload})

    def test_acceptance_fails_closed_for_missing_target_or_blocking_issue(self):
        plan, source, payload = fixture()
        spec = plan_hybrid_specs(plan)[0]
        with self.assertRaises(FormalMaintenanceError):
            build_formal_maintenance_acceptance(
                plan, post_audit_evidence=build_formal_post_audit_evidence(
                    {"schema_version": 1, "library_root": plan["library_root"],
                     "canonical_tree_sha256": canonical_digest(plan["canonical_tree"])},
                    target_witnesses={}, absent_paths=[source], blocking_issue_codes=[],
                ),
            )
        with self.assertRaises(FormalMaintenanceError):
            build_formal_maintenance_acceptance(
                plan, post_audit_evidence=post_audit(
                    plan, spec, payload, source, blocking=["canonical_tree_mismatch"]
                ),
            )

    def test_tampered_acceptance_never_commits(self):
        plan, source, payload = fixture()
        spec = plan_hybrid_specs(plan)[0]
        acceptance = build_formal_maintenance_acceptance(
            plan, post_audit_evidence=post_audit(plan, spec, payload, source),
        )
        acceptance["post_audit_sha256"] = "d" * 64
        self.assertFalse(formal_maintenance_acceptance_is_valid(plan, acceptance))

    def test_forward_conflict_aborts_without_losing_source(self):
        plan, source, payload = fixture()
        spec = plan_hybrid_specs(plan)[0]
        remote = MemoryRemote({source: payload, spec.target_path: b"foreign"})
        with tempfile.TemporaryDirectory() as tmp:
            pause = Path(tmp) / "global-control.json"
            write_pause(pause)
            with self.assertRaises(Exception):
                execute_formal_remediation(
                    remote, state_root=Path(tmp) / "tx", pause_receipt_path=pause,
                    plan=plan, approved_plan_sha256=plan["plan_sha256"],
                )
            self.assertEqual(remote.files[source], payload)
            self.assertEqual(remote.files[spec.target_path], b"foreign")


if __name__ == "__main__":
    unittest.main()
