from __future__ import annotations

import hashlib
import io
import tempfile
import unittest
from pathlib import Path

from engine.scrapeflow.remote_delete_transaction import (
    RemoteDeleteConflict,
    RemoteDeleteInfo,
    RemoteDeleteSpec,
    RemoteDeleteUncertain,
    commit_remote_delete_transaction,
    prepare_remote_delete_transaction,
    restore_remote_delete_transaction,
    run_remote_delete_transaction,
)


class MemoryRemote:
    def __init__(self, files: dict[str, bytes]) -> None:
        self.files = dict(files)
        self.remove_calls = 0
        self.upload_calls = 0
        self.remove_mode = "normal"

    def stat_exact(self, path: str):
        payload = self.files.get(path)
        if payload is None:
            return None
        digest = hashlib.sha256(payload).hexdigest()
        return RemoteDeleteInfo(len(payload), sha256=digest, version=digest)

    def open_reader(self, path: str):
        if path not in self.files:
            raise FileNotFoundError(path)
        return io.BytesIO(self.files[path])

    def remove_file(self, path: str) -> None:
        self.remove_calls += 1
        if self.remove_mode == "fail_before_delete":
            raise RuntimeError("provider unavailable")
        self.files.pop(path, None)
        if self.remove_mode == "response_lost":
            raise RuntimeError("response lost")

    def upload_file_once(self, target_path: str, source: Path, content_type: str) -> None:
        self.upload_calls += 1
        payload = source.read_bytes()
        if target_path in self.files:
            self.files[target_path + " (1)"] = payload
        else:
            self.files[target_path] = payload


def make_spec(payload: bytes) -> RemoteDeleteSpec:
    return RemoteDeleteSpec(
        transaction_id="delete-residual",
        source_path="/inbox/Show.NCOP.mkv",
        expected_size=len(payload),
        expected_sha256=hashlib.sha256(payload).hexdigest(),
        content_type="video/x-matroska",
    )


class RemoteDeleteTransactionTests(unittest.TestCase):
    def test_prepare_stages_complete_hash_without_deleting_source(self):
        payload = b"non-feature"
        remote = MemoryRemote({"/inbox/Show.NCOP.mkv": payload})
        with tempfile.TemporaryDirectory() as directory:
            result = prepare_remote_delete_transaction(
                remote, stage_root=Path(directory), spec=make_spec(payload),
            )
            self.assertEqual(result.state, "staged")
            self.assertEqual(result.stage_path.read_bytes(), payload)
        self.assertEqual(remote.remove_calls, 0)
        self.assertEqual(remote.files["/inbox/Show.NCOP.mkv"], payload)

    def test_delete_quarantines_locally_until_title_acceptance(self):
        payload = b"non-feature"
        remote = MemoryRemote({"/inbox/Show.NCOP.mkv": payload})
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            result = run_remote_delete_transaction(
                remote, stage_root=root, spec=make_spec(payload),
            )
            self.assertEqual(result.state, "quarantined")
            self.assertTrue(result.stage_path.is_file())
            self.assertNotIn("/inbox/Show.NCOP.mkv", remote.files)
            committed = commit_remote_delete_transaction(
                remote, stage_root=root, spec=make_spec(payload),
            )
            self.assertEqual(committed.state, "committed")
            self.assertFalse(committed.stage_path.exists())

    def test_lost_delete_response_reconciles_absence_and_retains_stage(self):
        payload = b"non-feature"
        remote = MemoryRemote({"/inbox/Show.NCOP.mkv": payload})
        remote.remove_mode = "response_lost"
        with tempfile.TemporaryDirectory() as directory:
            result = run_remote_delete_transaction(
                remote, stage_root=Path(directory), spec=make_spec(payload),
            )
            self.assertEqual(result.state, "quarantined")
            self.assertEqual(result.stage_path.read_bytes(), payload)
        self.assertEqual(remote.remove_calls, 1)

    def test_failed_delete_keeps_source_and_local_stage(self):
        payload = b"non-feature"
        remote = MemoryRemote({"/inbox/Show.NCOP.mkv": payload})
        remote.remove_mode = "fail_before_delete"
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with self.assertRaises(RemoteDeleteUncertain):
                run_remote_delete_transaction(
                    remote, stage_root=root, spec=make_spec(payload),
                )
            self.assertEqual(
                (root / "delete-residual" / "payload.bin").read_bytes(), payload,
            )
        self.assertEqual(remote.files["/inbox/Show.NCOP.mkv"], payload)

    def test_quarantined_payload_restores_exact_original_without_duplicate(self):
        payload = b"non-feature"
        remote = MemoryRemote({"/inbox/Show.NCOP.mkv": payload})
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            run_remote_delete_transaction(
                remote, stage_root=root, spec=make_spec(payload),
            )
            restored = restore_remote_delete_transaction(
                remote, stage_root=root, spec=make_spec(payload),
            )
            self.assertEqual(restored.state, "restored")
            self.assertEqual(remote.files["/inbox/Show.NCOP.mkv"], payload)
            self.assertNotIn("/inbox/Show.NCOP.mkv (1)", remote.files)
        self.assertEqual(remote.upload_calls, 1)

    def test_changed_source_is_never_deleted_or_used_as_restored_content(self):
        payload = b"non-feature"
        remote = MemoryRemote({"/inbox/Show.NCOP.mkv": payload})
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            prepare_remote_delete_transaction(
                remote, stage_root=root, spec=make_spec(payload),
            )
            remote.files["/inbox/Show.NCOP.mkv"] = b"replacement"
            with self.assertRaises(RemoteDeleteConflict):
                run_remote_delete_transaction(
                    remote, stage_root=root, spec=make_spec(payload),
                )
        self.assertEqual(remote.remove_calls, 0)

    def test_commit_refuses_when_original_path_reappears(self):
        payload = b"non-feature"
        remote = MemoryRemote({"/inbox/Show.NCOP.mkv": payload})
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            run_remote_delete_transaction(
                remote, stage_root=root, spec=make_spec(payload),
            )
            remote.files["/inbox/Show.NCOP.mkv"] = payload
            with self.assertRaises(RemoteDeleteConflict):
                commit_remote_delete_transaction(
                    remote, stage_root=root, spec=make_spec(payload),
                )
            self.assertTrue((root / "delete-residual" / "payload.bin").is_file())


if __name__ == "__main__":
    unittest.main()
