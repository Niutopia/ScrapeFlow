from __future__ import annotations

import hashlib
import io
import json
from pathlib import Path
import tempfile
import unittest

from engine.scrapeflow.local_upload_transaction import (
    LocalUploadConflict,
    LocalUploadRemoteInfo,
    LocalUploadSpec,
    LocalUploadUncertain,
    deterministic_local_upload_id,
    run_local_upload_transaction,
)
from engine.scrapeflow.serialization import canonical_json_bytes


class SimulatedCrash(BaseException):
    pass


class MemoryUploadRemote:
    def __init__(self, files: dict[str, bytes] | None = None) -> None:
        self.files = dict(files or {})
        self.upload_calls = 0
        self.mode = "success"
        self.hidden_after_upload = 0

    def stat_exact(self, path: str):
        if self.upload_calls and self.hidden_after_upload > 0:
            self.hidden_after_upload -= 1
            return None
        payload = self.files.get(path)
        if payload is None:
            return None
        digest = hashlib.sha256(payload).hexdigest()
        return LocalUploadRemoteInfo(
            size=len(payload), sha256=digest, version=digest,
        )

    def open_reader(self, path: str):
        return io.BytesIO(self.files[path])

    def upload_file_once(self, target_path: str, source: Path, _content_type: str):
        self.upload_calls += 1
        payload = source.read_bytes()
        if target_path in self.files:
            self.files[target_path + " (1)"] = payload
        else:
            self.files[target_path] = payload
        if self.mode in {"response_lost", "delayed_response_lost"}:
            raise RuntimeError("HTTP 500 after provider commit")
        if self.mode == "500_missing":
            self.files.pop(target_path, None)
            raise RuntimeError("HTTP 500 without visible target")


class LocalUploadTransactionTests(unittest.TestCase):
    def make_spec(self, source: Path) -> LocalUploadSpec:
        target = "/remote/S01E01.mkv"
        return LocalUploadSpec(
            transaction_id=deterministic_local_upload_id(source, target),
            source_path=source,
            target_path=target,
            expected_size=source.stat().st_size,
            content_type="video/x-matroska",
        )

    def test_lost_response_is_reconciled_by_full_hash_without_second_put(self):
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            source = root / "episode.mkv"
            source.write_bytes(b"verified-media")
            remote = MemoryUploadRemote()
            remote.mode = "response_lost"

            result = run_local_upload_transaction(
                remote,
                transaction_root=root / "transactions",
                spec=self.make_spec(source),
                reconciliation_delays=(0.0,),
            )

            self.assertEqual(result.state, "complete")
            self.assertEqual(result.upload_calls_recorded, 1)
            self.assertEqual(remote.upload_calls, 1)
            self.assertEqual(remote.files["/remote/S01E01.mkv"], b"verified-media")
            self.assertNotIn("/remote/S01E01.mkv (1)", remote.files)

    def test_500_with_invisible_target_retains_source_and_never_reuploads(self):
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            source = root / "episode.mkv"
            source.write_bytes(b"only-local-copy")
            spec = self.make_spec(source)
            remote = MemoryUploadRemote()
            remote.mode = "500_missing"

            with self.assertRaises(LocalUploadUncertain):
                run_local_upload_transaction(
                    remote, transaction_root=root / "transactions", spec=spec,
                    reconciliation_delays=(0.0,),
                )
            with self.assertRaises(LocalUploadUncertain):
                run_local_upload_transaction(
                    remote, transaction_root=root / "transactions", spec=spec,
                    reconciliation_delays=(0.0,),
                )

            self.assertEqual(remote.upload_calls, 1)
            self.assertEqual(source.read_bytes(), b"only-local-copy")
            journal = json.loads(
                (root / "transactions" / spec.transaction_id / "journal.json")
                .read_text(encoding="utf-8")
            )
            self.assertEqual(journal["state"], "upload_uncertain")
            self.assertEqual(journal["upload_calls"], 1)
            self.assertNotIn("/remote/S01E01.mkv (1)", remote.files)

    def test_response_lost_target_can_become_visible_during_reconciliation(self):
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            source = root / "episode.mkv"
            source.write_bytes(b"eventually-visible")
            remote = MemoryUploadRemote()
            remote.mode = "delayed_response_lost"
            remote.hidden_after_upload = 2

            result = run_local_upload_transaction(
                remote, transaction_root=root / "transactions",
                spec=self.make_spec(source),
                reconciliation_delays=(0.0, 0.0, 0.0),
            )

            self.assertEqual(result.state, "complete")
            self.assertEqual(remote.upload_calls, 1)
            self.assertNotIn("/remote/S01E01.mkv (1)", remote.files)

    def test_process_restart_reconciles_existing_target_without_another_put(self):
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            source = root / "episode.mkv"
            source.write_bytes(b"restart-safe")
            spec = self.make_spec(source)
            files: dict[str, bytes] = {}
            first = MemoryUploadRemote(files)
            first.files = files
            first.mode = "response_lost"
            first.hidden_after_upload = 10

            with self.assertRaises(LocalUploadUncertain):
                run_local_upload_transaction(
                    first, transaction_root=root / "transactions", spec=spec,
                    reconciliation_delays=(0.0,),
                )
            second = MemoryUploadRemote(files)
            result = run_local_upload_transaction(
                second, transaction_root=root / "transactions", spec=spec,
                reconciliation_delays=(0.0,),
            )

            self.assertEqual(first.upload_calls, 1)
            self.assertEqual(second.upload_calls, 0)
            self.assertEqual(result.upload_calls_recorded, 1)
            self.assertNotIn("/remote/S01E01.mkv (1)", files)

    def test_same_size_wrong_target_fails_closed_before_upload(self):
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            source = root / "episode.mkv"
            source.write_bytes(b"correct")
            remote = MemoryUploadRemote({"/remote/S01E01.mkv": b"WRONG!!"})

            with self.assertRaises(LocalUploadConflict):
                run_local_upload_transaction(
                    remote, transaction_root=root / "transactions",
                    spec=self.make_spec(source), reconciliation_delays=(0.0,),
                )

            self.assertEqual(remote.upload_calls, 0)
            self.assertEqual(remote.files["/remote/S01E01.mkv"], b"WRONG!!")

    def test_crash_after_upload_started_never_allows_a_later_put(self):
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            source = root / "episode.mkv"
            source.write_bytes(b"checkpointed")
            spec = self.make_spec(source)
            remote = MemoryUploadRemote()

            def crash(event: str, _journal) -> None:
                if event == "upload_started":
                    raise SimulatedCrash(event)

            with self.assertRaises(SimulatedCrash):
                run_local_upload_transaction(
                    remote, transaction_root=root / "transactions", spec=spec,
                    reconciliation_delays=(0.0,), checkpoint_hook=crash,
                )
            with self.assertRaises(LocalUploadUncertain):
                run_local_upload_transaction(
                    remote, transaction_root=root / "transactions", spec=spec,
                    reconciliation_delays=(0.0,),
                )

            self.assertEqual(remote.upload_calls, 0)

    def test_success_receipt_is_bound_to_verified_content_hash(self):
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            source = root / "episode.mkv"
            source.write_bytes(b"receipt-content")
            spec = self.make_spec(source)
            result = run_local_upload_transaction(
                MemoryUploadRemote(), transaction_root=root / "transactions",
                spec=spec, reconciliation_delays=(0.0,),
            )
            journal = json.loads(result.journal_path.read_text(encoding="utf-8"))
            receipt = {
                "transaction_id": journal["transaction_id"],
                "target_path": journal["target_path"],
                "size": journal["size"],
                "sha256": journal["sha256"],
                "verified_at": journal["verified_at"],
            }
            self.assertEqual(
                result.receipt_sha256,
                hashlib.sha256(canonical_json_bytes(receipt)).hexdigest(),
            )

    def test_complete_receipt_reentry_rechecks_target_without_reupload(self):
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            source = root / "episode.mkv"
            source.write_bytes(b"completed-but-live-checked")
            spec = self.make_spec(source)
            remote = MemoryUploadRemote()
            run_local_upload_transaction(
                remote, transaction_root=root / "transactions", spec=spec,
                reconciliation_delays=(0.0,),
            )
            remote.files.pop(spec.target_path)

            with self.assertRaises(LocalUploadUncertain):
                run_local_upload_transaction(
                    remote, transaction_root=root / "transactions", spec=spec,
                    reconciliation_delays=(0.0,),
                )

            self.assertEqual(remote.upload_calls, 1)


if __name__ == "__main__":
    unittest.main()
