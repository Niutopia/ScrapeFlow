from __future__ import annotations

import hashlib
import io
import tempfile
import unittest
from pathlib import Path

from engine.scrapeflow.remote_file_transaction import (
    RemoteFileInfo,
    RemoteFileTransferSpec,
    TransactionConflict,
    TransactionUncertain,
    discard_completed_remote_file_transaction,
    prepare_remote_file_transaction,
    run_remote_file_transaction,
)


class SimulatedCrash(BaseException):
    pass


class MemoryRemote:
    def __init__(self, files: dict[str, bytes]) -> None:
        self.files = dict(files)
        self.upload_calls = 0
        self.remove_calls = 0
        self.upload_mode = "normal"

    def stat_exact(self, path: str) -> RemoteFileInfo | None:
        payload = self.files.get(path)
        if payload is None:
            return None
        return RemoteFileInfo(size=len(payload), version=hashlib.sha256(payload).hexdigest())

    def open_reader(self, path: str):
        if path not in self.files:
            raise FileNotFoundError(path)
        return io.BytesIO(self.files[path])

    def upload_file_once(self, target_path: str, source: Path, content_type: str) -> None:
        self.upload_calls += 1
        payload = source.read_bytes()
        if self.upload_mode == "source_lost_500":
            self.files.pop("/source/episode.mkv", None)
            raise RuntimeError("HTTP 500 and provider lost the source")
        if target_path in self.files:
            # Model the provider behaviour that caused historical ``(1)``
            # duplicates.  Correct transaction code must never reach this on a
            # retry once an upload call may have started.
            self.files[target_path + " (1)"] = payload
        else:
            self.files[target_path] = payload
        if self.upload_mode == "response_lost":
            raise RuntimeError("response lost after commit")

    def remove_file(self, path: str) -> None:
        self.remove_calls += 1
        self.files.pop(path, None)


def make_spec(payload: bytes) -> RemoteFileTransferSpec:
    return RemoteFileTransferSpec(
        transaction_id="episode-transaction",
        source_path="/source/episode.mkv",
        target_path="/library/episode.mkv",
        expected_size=len(payload),
        expected_sha256=hashlib.sha256(payload).hexdigest(),
        content_type="video/x-matroska",
    )


class RemoteFileTransactionTests(unittest.TestCase):
    def test_prepare_persists_hash_without_any_remote_mutation(self):
        payload = b"prepare-before-mutation"
        remote = MemoryRemote({"/source/episode.mkv": payload})
        with tempfile.TemporaryDirectory() as tmp:
            stage_root = Path(tmp)
            result = prepare_remote_file_transaction(
                remote, stage_root=stage_root, spec=make_spec(payload),
            )
            self.assertEqual(result.state, "staged")
            self.assertEqual(result.stage_path.read_bytes(), payload)
        self.assertEqual(remote.upload_calls, 0)
        self.assertEqual(remote.remove_calls, 0)
        self.assertEqual(remote.files["/source/episode.mkv"], payload)

    def test_lost_response_reconciles_exact_target_without_second_upload(self):
        payload = b"media-payload"
        remote = MemoryRemote({"/source/episode.mkv": payload})
        remote.upload_mode = "response_lost"
        with tempfile.TemporaryDirectory() as tmp:
            result = run_remote_file_transaction(
                remote, stage_root=Path(tmp), spec=make_spec(payload),
            )
        self.assertEqual(result.state, "complete")
        self.assertEqual(remote.upload_calls, 1)
        self.assertEqual(remote.files["/library/episode.mkv"], payload)
        self.assertNotIn("/source/episode.mkv", remote.files)
        self.assertNotIn("/library/episode.mkv (1)", remote.files)

    def test_500_with_both_remote_paths_missing_retains_verified_local_payload(self):
        payload = b"irreplaceable-media"
        remote = MemoryRemote({"/source/episode.mkv": payload})
        remote.upload_mode = "source_lost_500"
        with tempfile.TemporaryDirectory() as tmp:
            stage_root = Path(tmp)
            with self.assertRaises(TransactionUncertain):
                run_remote_file_transaction(
                    remote, stage_root=stage_root, spec=make_spec(payload),
                )
            staged = stage_root / "episode-transaction" / "payload.bin"
            self.assertEqual(staged.read_bytes(), payload)
            self.assertTrue((staged.parent / "journal.json").is_file())
        self.assertEqual(remote.upload_calls, 1)
        self.assertNotIn("/source/episode.mkv", remote.files)
        self.assertNotIn("/library/episode.mkv", remote.files)

    def test_matching_source_and_target_deletes_only_source_without_upload(self):
        payload = b"same-media"
        remote = MemoryRemote({
            "/source/episode.mkv": payload,
            "/library/episode.mkv": payload,
        })
        with tempfile.TemporaryDirectory() as tmp:
            result = run_remote_file_transaction(
                remote, stage_root=Path(tmp), spec=make_spec(payload),
            )
        self.assertEqual(result.state, "complete")
        self.assertEqual(remote.upload_calls, 0)
        self.assertNotIn("/source/episode.mkv", remote.files)
        self.assertEqual(remote.files["/library/episode.mkv"], payload)

    def test_conflicting_target_preserves_both_remote_objects(self):
        payload = b"source-data"
        remote = MemoryRemote({
            "/source/episode.mkv": payload,
            "/library/episode.mkv": b"other-bytes",
        })
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaises(TransactionConflict):
                run_remote_file_transaction(
                    remote, stage_root=Path(tmp), spec=make_spec(payload),
                )
        self.assertEqual(remote.upload_calls, 0)
        self.assertEqual(remote.files["/source/episode.mkv"], payload)
        self.assertEqual(remote.files["/library/episode.mkv"], b"other-bytes")

    def test_checkpoint_crashes_resume_without_duplicate_upload(self):
        payload = b"checkpoint-media"
        resumable_events = (
            "staged",
            "upload_intent",
            "target_verified",
            "source_delete_intent",
            "complete",
        )
        for event in resumable_events:
            with self.subTest(event=event), tempfile.TemporaryDirectory() as tmp:
                remote = MemoryRemote({"/source/episode.mkv": payload})
                crashed = False

                def crash_once(current: str, _journal) -> None:
                    nonlocal crashed
                    if current == event and not crashed:
                        crashed = True
                        raise SimulatedCrash(current)

                with self.assertRaises(SimulatedCrash):
                    run_remote_file_transaction(
                        remote,
                        stage_root=Path(tmp),
                        spec=make_spec(payload),
                        checkpoint_hook=crash_once,
                    )
                result = run_remote_file_transaction(
                    remote, stage_root=Path(tmp), spec=make_spec(payload),
                )
                self.assertEqual(result.state, "complete")
                self.assertLessEqual(remote.upload_calls, 1)
                self.assertNotIn("/library/episode.mkv (1)", remote.files)

    def test_crash_after_upload_started_checkpoint_fails_closed(self):
        payload = b"checkpoint-before-request"
        remote = MemoryRemote({"/source/episode.mkv": payload})
        with tempfile.TemporaryDirectory() as tmp:
            stage_root = Path(tmp)

            def crash(current: str, _journal) -> None:
                if current == "upload_started":
                    raise SimulatedCrash(current)

            with self.assertRaises(SimulatedCrash):
                run_remote_file_transaction(
                    remote,
                    stage_root=stage_root,
                    spec=make_spec(payload),
                    checkpoint_hook=crash,
                )
            with self.assertRaises(TransactionUncertain):
                run_remote_file_transaction(
                    remote, stage_root=stage_root, spec=make_spec(payload),
                )
            self.assertEqual(remote.upload_calls, 0)
            self.assertEqual(
                (stage_root / "episode-transaction" / "payload.bin").read_bytes(),
                payload,
            )
            self.assertIn("/source/episode.mkv", remote.files)

    def test_completed_payload_can_be_discarded_but_receipt_stays_resumable(self):
        payload = b"bounded-local-stage"
        remote = MemoryRemote({"/source/episode.mkv": payload})
        spec = make_spec(payload)
        with tempfile.TemporaryDirectory() as tmp:
            stage_root = Path(tmp)
            result = run_remote_file_transaction(
                remote, stage_root=stage_root, spec=spec,
            )
            discard_completed_remote_file_transaction(
                stage_root=stage_root, spec=spec,
            )
            self.assertFalse(result.stage_path.exists())
            resumed = run_remote_file_transaction(
                remote, stage_root=stage_root, spec=spec,
            )
            self.assertEqual(resumed.state, "complete")
        self.assertEqual(remote.upload_calls, 1)


if __name__ == "__main__":
    unittest.main()
