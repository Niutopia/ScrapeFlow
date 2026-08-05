from __future__ import annotations

import hashlib
import io
import json
import tempfile
import unittest
from pathlib import Path

from engine.scrapeflow.hybrid_remote_transaction import (
    DEFAULT_ROLLBACK_ROOT,
    HybridTransferSpec,
    abort_hybrid_batch,
    commit_hybrid_batch,
    load_sealed_batch_specs,
    prepare_hybrid_batch,
    prepare_hybrid_transfer,
    restore_hybrid_transfer,
    run_hybrid_transfer,
)
from engine.scrapeflow.remote_file_transaction import (
    RemoteFileInfo,
    TransactionConflict,
    TransactionUncertain,
)


class SimulatedCrash(BaseException):
    pass


class MemoryRemote:
    def __init__(self, files: dict[str, bytes]) -> None:
        self.files = dict(files)
        self.upload_calls: list[str] = []
        self.remove_calls: list[str] = []
        self.response_lost_for: set[str] = set()
        self.directories: set[str] = set()
        self.remove_response_lost_for: set[str] = set()

    def stat_exact(self, path: str):
        payload = self.files.get(path)
        if payload is None:
            return None
        digest = hashlib.sha256(payload).hexdigest()
        return RemoteFileInfo(size=len(payload), sha256=digest, version=digest)

    def open_reader(self, path: str):
        if path not in self.files:
            raise FileNotFoundError(path)
        return io.BytesIO(self.files[path])

    def upload_file_once(self, path: str, source: Path, content_type: str) -> None:
        self.upload_calls.append(path)
        payload = source.read_bytes()
        if path in self.files:
            self.files[path + " (1)"] = payload
        else:
            self.files[path] = payload
        if path in self.response_lost_for:
            raise RuntimeError("HTTP 500 after provider commit")

    def remove_file(self, path: str) -> None:
        self.remove_calls.append(path)
        self.files.pop(path, None)
        if path in self.remove_response_lost_for:
            raise RuntimeError("HTTP 500 after provider delete")

    def ensure_directory(self, path: str) -> None:
        self.directories.add(path)


def spec(payload: bytes, *, batch: str = "work-42", item: str = "episode-01"):
    return HybridTransferSpec(
        batch_id=batch,
        item_id=item,
        source_path=f"/待刮削/{item}.mkv",
        target_path=f"/番剧/Example/Season 01/{item}.mkv",
        expected_size=len(payload),
        expected_sha256=hashlib.sha256(payload).hexdigest(),
    )


def delete_spec(payload: bytes, *, batch: str = "delete-42", item: str = "ncop-01"):
    return HybridTransferSpec(
        batch_id=batch,
        item_id=item,
        source_path=f"/待刮削/{item}.mkv",
        target_path=None,
        expected_size=len(payload),
        expected_sha256=hashlib.sha256(payload).hexdigest(),
        operation="delete",
    )


class HybridRemoteTransactionTests(unittest.TestCase):
    def test_spec_preserves_full_width_media_punctuation_but_rejects_nfkc_aliases(self):
        item = HybridTransferSpec(
            batch_id="unicode-title", item_id="rick-anime",
            source_path="/待刮削/瑞克和莫蒂：日漫版.mkv",
            target_path="/番剧/瑞克和莫蒂/瑞克和莫蒂：日漫版.mkv",
            expected_size=1,
        )
        self.assertIn("：", item.target_path)
        with self.assertRaisesRegex(ValueError, "overlap"):
            HybridTransferSpec(
                batch_id="unicode-alias", item_id="same",
                source_path="/待刮削/节目：第一集.mkv",
                target_path="/待刮削/节目:第一集.mkv",
                expected_size=1,
            )

    def test_prepare_persists_desired_specs_before_any_item_upload(self):
        payload = b"desired first"
        item = spec(payload, batch="desired-first")
        remote = MemoryRemote({item.source_path: payload})

        def crash(event, _journal):
            if event == "batch_desired_specs_persisted":
                raise SimulatedCrash(event)

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            with self.assertRaises(SimulatedCrash):
                prepare_hybrid_batch(
                    remote, state_root=root, specs=[item], checkpoint_hook=crash,
                )
            self.assertEqual(remote.upload_calls, [])
            batch_path = root / item.batch_id / "batch.json"
            batch = json.loads(batch_path.read_text("utf-8"))
            self.assertEqual(batch["state"], "preparing")
            self.assertEqual(batch["specs"], [item.to_dict()])
            self.assertRegex(batch["specs_sha256"], r"^[0-9a-f]{64}$")

            changed = HybridTransferSpec(
                batch_id=item.batch_id,
                item_id=item.item_id,
                source_path=item.source_path,
                target_path=item.target_path,
                expected_size=item.expected_size,
                expected_sha256=item.expected_sha256,
                content_type="application/x-changed",
            )
            with self.assertRaises(TransactionConflict):
                prepare_hybrid_batch(remote, state_root=root, specs=[changed])
            self.assertEqual(remote.upload_calls, [])

            prepare_hybrid_batch(remote, state_root=root, specs=[item])
            self.assertIn(item.rollback_path, remote.files)

    def test_spec_rejects_any_media_path_inside_entire_rollback_root(self):
        payload = b"x"
        for source_path, target_path in (
            (
                f"{DEFAULT_ROLLBACK_ROOT}/unrelated/source.mkv",
                "/library/source.mkv",
            ),
            (
                "/intake/source.mkv",
                f"{DEFAULT_ROLLBACK_ROOT}/another-batch/target.mkv",
            ),
        ):
            with self.subTest(source=source_path, target=target_path):
                with self.assertRaisesRegex(ValueError, "outside rollback_root"):
                    HybridTransferSpec(
                        batch_id="root-isolation",
                        item_id="one",
                        source_path=source_path,
                        target_path=target_path,
                        expected_size=len(payload),
                    )

    def test_spec_round_trip_is_strict_for_transfer_and_delete(self):
        for item in (spec(b"transfer"), delete_spec(b"delete")):
            with self.subTest(operation=item.operation):
                self.assertEqual(HybridTransferSpec.from_dict(item.to_dict()), item)
                extra = {**item.to_dict(), "unknown": True}
                with self.assertRaises(ValueError):
                    HybridTransferSpec.from_dict(extra)
                missing = item.to_dict()
                missing.pop("content_type")
                with self.assertRaises(ValueError):
                    HybridTransferSpec.from_dict(missing)

    def test_sealed_specs_can_be_rebuilt_exactly_for_restart_restore(self):
        first, second = spec(b"one", item="episode-01"), delete_spec(
            b"two", batch="work-42", item="ncop-01",
        )
        remote = MemoryRemote({first.source_path: b"one", second.source_path: b"two"})
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            prepare_hybrid_batch(remote, state_root=root, specs=[first, second])
            rebuilt = load_sealed_batch_specs(state_root=root, batch_id="work-42")
        self.assertEqual(rebuilt, sorted([first, second], key=lambda item: item.item_id))

    def test_tampered_local_specs_digest_is_recomputed_at_all_core_entries(self):
        payload = b"digest integrity"
        for entry in ("load", "run", "commit", "abort"):
            with self.subTest(entry=entry), tempfile.TemporaryDirectory() as tmp:
                item = spec(payload, batch=f"digest-{entry}")
                remote = MemoryRemote({item.source_path: payload})
                root = Path(tmp)
                prepare_hybrid_batch(remote, state_root=root, specs=[item])
                if entry == "commit":
                    run_hybrid_transfer(remote, state_root=root, spec=item)
                batch_path = root / item.batch_id / "batch.json"
                batch = json.loads(batch_path.read_text("utf-8"))
                batch["specs_sha256"] = "0" * 64
                batch_path.write_text(json.dumps(batch), "utf-8")
                before = list(remote.remove_calls)
                with self.assertRaises(TransactionConflict):
                    if entry == "load":
                        load_sealed_batch_specs(
                            state_root=root, batch_id=item.batch_id,
                        )
                    elif entry == "run":
                        run_hybrid_transfer(remote, state_root=root, spec=item)
                    elif entry == "commit":
                        commit_hybrid_batch(
                            remote, state_root=root, specs=[item],
                        )
                    else:
                        abort_hybrid_batch(
                            remote, state_root=root, specs=[item],
                        )
                self.assertEqual(remote.remove_calls, before)

    def test_tampered_local_specs_content_cannot_reuse_original_digest(self):
        payload = b"spec integrity"
        item = spec(payload, batch="spec-content-tamper")
        remote = MemoryRemote({item.source_path: payload})
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            prepare_hybrid_batch(remote, state_root=root, specs=[item])
            run_hybrid_transfer(remote, state_root=root, spec=item)
            batch_path = root / item.batch_id / "batch.json"
            batch = json.loads(batch_path.read_text("utf-8"))
            batch["specs"][0]["content_type"] = "application/x-tampered"
            batch_path.write_text(json.dumps(batch), "utf-8")
            before = list(remote.remove_calls)
            with self.assertRaises(TransactionConflict):
                load_sealed_batch_specs(
                    state_root=root, batch_id=item.batch_id,
                )
            with self.assertRaises(TransactionConflict):
                commit_hybrid_batch(remote, state_root=root, specs=[item])
            self.assertEqual(remote.remove_calls, before)

    def test_spec_rejects_aliases_and_overlapping_protected_paths(self):
        payload = b"x"
        invalid_sources = (
            "/待刮削/./x.mkv", "/待刮削/x.mkv/", "/待刮削\\x.mkv",
            "/待刮削/%78.mkv", "/待刮削//x.mkv", "/待刮削/e\u0301.mkv",
            "/待刮削/ｅ.mkv", "/待刮削/x\u200b.mkv",
        )
        for source in invalid_sources:
            with self.subTest(source=source), self.assertRaises(ValueError):
                HybridTransferSpec(
                    batch_id="aliases", item_id="one", source_path=source,
                    target_path="/番剧/x.mkv", expected_size=1,
                    expected_sha256=hashlib.sha256(payload).hexdigest(),
                )
        with self.assertRaises(ValueError):
            HybridTransferSpec(
                batch_id="overlap", item_id="one",
                source_path=DEFAULT_ROLLBACK_ROOT,
                target_path="/番剧/x.mkv", expected_size=1,
                expected_sha256=hashlib.sha256(payload).hexdigest(),
            )
        with self.assertRaises(ValueError):
            HybridTransferSpec(
                batch_id="case-alias", item_id="one",
                source_path="/Library/Episode.mkv",
                target_path="/library/episode.mkv", expected_size=1,
                expected_sha256=hashlib.sha256(payload).hexdigest(),
            )

    def test_cross_item_path_overlap_is_rejected_before_any_upload(self):
        first_payload, second_payload = b"one", b"two"
        first = spec(first_payload, item="episode-01")
        second = HybridTransferSpec(
            batch_id=first.batch_id, item_id="episode-02",
            source_path=first.target_path,
            target_path="/番剧/Example/Season 01/episode-02.mkv",
            expected_size=len(second_payload),
            expected_sha256=hashlib.sha256(second_payload).hexdigest(),
        )
        remote = MemoryRemote({
            first.source_path: first_payload, second.source_path: second_payload,
        })
        with tempfile.TemporaryDirectory() as tmp, self.assertRaises(ValueError):
            prepare_hybrid_batch(
                remote, state_root=Path(tmp), specs=[first, second],
            )
        self.assertEqual(remote.upload_calls, [])

    def test_prepare_only_creates_verified_remote_copy_and_releases_local_payload(self):
        payload = b"irreplaceable episode"
        item = spec(payload)
        remote = MemoryRemote({item.source_path: payload})
        with tempfile.TemporaryDirectory() as tmp:
            result = prepare_hybrid_transfer(remote, state_root=Path(tmp), spec=item)
            self.assertEqual(result.state, "rollback_verified")
            self.assertFalse(result.local_payload_retained)
            self.assertFalse((Path(tmp) / item.batch_id / item.item_id / "payload.bin").exists())
        self.assertEqual(remote.files[item.rollback_path], payload)
        self.assertIn(item.remote_manifest_path, remote.files)
        self.assertIn(item.source_path, remote.files)
        self.assertNotIn(item.target_path, remote.files)

    def test_forward_is_rejected_until_every_item_is_prepared_and_batch_sealed(self):
        first_payload, second_payload = b"one", b"two"
        first = spec(first_payload, item="episode-01")
        second = spec(second_payload, item="episode-02")
        remote = MemoryRemote({
            first.source_path: first_payload, second.source_path: second_payload,
        })
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            prepare_hybrid_transfer(remote, state_root=root, spec=first)
            with self.assertRaises(TransactionUncertain):
                run_hybrid_transfer(remote, state_root=root, spec=first)
            self.assertIn(first.source_path, remote.files)
            self.assertNotIn(first.target_path, remote.files)
            prepare_hybrid_batch(remote, state_root=root, specs=[first, second])
            result = run_hybrid_transfer(remote, state_root=root, spec=first)
            self.assertEqual(result.state, "complete")

    def test_response_lost_is_reconciled_without_duplicate_upload(self):
        payload = b"provider committed but replied 500"
        item = spec(payload)
        remote = MemoryRemote({item.source_path: payload})
        remote.response_lost_for.update({item.rollback_path, item.target_path})
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            prepare_hybrid_batch(remote, state_root=root, specs=[item])
            result = run_hybrid_transfer(remote, state_root=root, spec=item)
            self.assertEqual(result.state, "complete")
            resumed = run_hybrid_transfer(remote, state_root=root, spec=item)
            self.assertEqual(resumed.state, "complete")
        self.assertEqual(remote.upload_calls.count(item.rollback_path), 1)
        self.assertEqual(remote.upload_calls.count(item.target_path), 1)
        self.assertFalse(any(path.endswith(" (1)") for path in remote.files))

    def test_wrong_existing_target_fails_closed_and_preserves_source_and_rollback(self):
        payload = b"correct"
        item = spec(payload)
        remote = MemoryRemote({
            item.source_path: payload,
            item.target_path: b"wrong!!",
        })
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            prepare_hybrid_batch(remote, state_root=root, specs=[item])
            with self.assertRaises(TransactionConflict):
                run_hybrid_transfer(remote, state_root=root, spec=item)
        self.assertEqual(remote.files[item.source_path], payload)
        self.assertEqual(remote.files[item.target_path], b"wrong!!")
        self.assertEqual(remote.files[item.rollback_path], payload)

    def test_matching_preexisting_target_is_not_owned_and_never_removed_by_restore(self):
        payload = b"same but preexisting"
        item = spec(payload)
        remote = MemoryRemote({item.source_path: payload, item.target_path: payload})
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            prepare_hybrid_batch(remote, state_root=root, specs=[item])
            with self.assertRaises(TransactionConflict):
                run_hybrid_transfer(remote, state_root=root, spec=item)
            with self.assertRaises(TransactionUncertain):
                restore_hybrid_transfer(remote, state_root=root, spec=item)
        self.assertEqual(remote.files[item.source_path], payload)
        self.assertEqual(remote.files[item.target_path], payload)

    def test_commit_revalidates_target_and_manifests_before_removing_rollback(self):
        payload = b"must remain recoverable"
        item = spec(payload)
        remote = MemoryRemote({item.source_path: payload})
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            prepare_hybrid_batch(remote, state_root=root, specs=[item])
            run_hybrid_transfer(remote, state_root=root, spec=item)
            remote.files.pop(item.target_path)
            with self.assertRaises(TransactionUncertain):
                commit_hybrid_batch(remote, state_root=root, specs=[item])
            self.assertEqual(remote.files[item.rollback_path], payload)
            self.assertIn(item.remote_manifest_path, remote.files)

    def test_commit_refuses_tampered_item_manifest_without_deleting_payload(self):
        payload = b"manifest witness"
        item = spec(payload)
        remote = MemoryRemote({item.source_path: payload})
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            prepare_hybrid_batch(remote, state_root=root, specs=[item])
            run_hybrid_transfer(remote, state_root=root, spec=item)
            remote.files[item.remote_manifest_path] = b"{}\n"
            with self.assertRaises(TransactionConflict):
                commit_hybrid_batch(remote, state_root=root, specs=[item])
        self.assertEqual(remote.files[item.rollback_path], payload)

    def test_sealed_remote_manifest_membership_must_equal_current_spec(self):
        payload = b"sealed membership"
        item = spec(payload)
        remote = MemoryRemote({item.source_path: payload})
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            prepare_hybrid_batch(remote, state_root=root, specs=[item])
            batch_path = root / item.batch_id / "batch.json"
            batch = json.loads(batch_path.read_text("utf-8"))
            remote_path = f"{item.batch_root}/batch-manifest.json"
            manifest = json.loads(remote.files[remote_path].decode("utf-8"))
            manifest["items"][0]["source_path"] = "/待刮削/not-the-sealed-source.mkv"
            changed = (json.dumps(
                manifest, ensure_ascii=False, sort_keys=True, separators=(",", ":")
            ) + "\n").encode()
            remote.files[remote_path] = changed
            batch["manifest_size"] = len(changed)
            batch["manifest_sha256"] = hashlib.sha256(changed).hexdigest()
            batch_path.write_text(json.dumps(batch), encoding="utf-8")
            uploads_before = list(remote.upload_calls)
            with self.assertRaises(TransactionConflict):
                run_hybrid_transfer(remote, state_root=root, spec=item)
            self.assertEqual(remote.upload_calls, uploads_before)
            self.assertIn(item.source_path, remote.files)

    def test_changed_spec_after_seal_is_rejected_before_new_upload(self):
        payload = b"immutable seal"
        item = spec(payload)
        remote = MemoryRemote({item.source_path: payload})
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            prepare_hybrid_batch(remote, state_root=root, specs=[item])
            changed = HybridTransferSpec(
                batch_id=item.batch_id, item_id=item.item_id,
                source_path=item.source_path, target_path="/番剧/Other/episode-01.mkv",
                expected_size=item.expected_size,
                expected_sha256=item.expected_sha256,
            )
            uploads_before = list(remote.upload_calls)
            with self.assertRaises(TransactionConflict):
                prepare_hybrid_batch(remote, state_root=root, specs=[changed])
            self.assertEqual(remote.upload_calls, uploads_before)

            changed_content_type = HybridTransferSpec(
                batch_id=item.batch_id, item_id=item.item_id,
                source_path=item.source_path, target_path=item.target_path,
                expected_size=item.expected_size,
                expected_sha256=item.expected_sha256,
                content_type="application/x-changed",
            )
            with self.assertRaises(TransactionConflict):
                prepare_hybrid_batch(
                    remote, state_root=root, specs=[changed_content_type],
                )
            self.assertEqual(remote.upload_calls, uploads_before)

    def test_batch_commit_refuses_until_all_forward_items_are_terminal(self):
        first_payload, second_payload = b"one", b"two"
        first = spec(first_payload, item="episode-01")
        second = spec(second_payload, item="episode-02")
        remote = MemoryRemote({
            first.source_path: first_payload, second.source_path: second_payload,
        })
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            prepare_hybrid_batch(remote, state_root=root, specs=[first, second])
            run_hybrid_transfer(remote, state_root=root, spec=first)
            with self.assertRaises(TransactionUncertain):
                commit_hybrid_batch(
                    remote, state_root=root, specs=[first, second],
                )
        self.assertEqual(remote.files[first.rollback_path], first_payload)
        self.assertEqual(remote.files[second.rollback_path], second_payload)

    def test_crash_before_first_upload_call_never_retries_ambiguous_intent(self):
        payload = b"checkpoint"
        item = spec(payload)
        remote = MemoryRemote({item.source_path: payload})

        def crash(event, _journal):
            if event == "rollback_upload_started_checkpoint":
                raise SimulatedCrash(event)

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            with self.assertRaises(SimulatedCrash):
                prepare_hybrid_transfer(
                    remote, state_root=root, spec=item, checkpoint_hook=crash,
                )
            with self.assertRaises(TransactionUncertain):
                prepare_hybrid_transfer(remote, state_root=root, spec=item)
        self.assertEqual(remote.upload_calls, [])
        self.assertIn(item.source_path, remote.files)

    def test_restore_is_exact_no_overwrite_and_keeps_rollback_until_batch_commit(self):
        payload = b"restore me"
        item = spec(payload)
        remote = MemoryRemote({item.source_path: payload})
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            prepare_hybrid_batch(remote, state_root=root, specs=[item])
            run_hybrid_transfer(remote, state_root=root, spec=item)
            result = restore_hybrid_transfer(remote, state_root=root, spec=item)
            self.assertEqual(result.state, "restored")
            self.assertEqual(remote.files[item.source_path], payload)
            self.assertNotIn(item.target_path, remote.files)
            self.assertEqual(remote.files[item.rollback_path], payload)
            commit_hybrid_batch(remote, state_root=root, specs=[item])
            self.assertNotIn(item.rollback_path, remote.files)
            self.assertNotIn(item.remote_manifest_path, remote.files)
            self.assertNotIn(f"{item.batch_root}/batch-manifest.json", remote.files)

    def test_restore_refuses_to_overwrite_different_source(self):
        payload = b"original"
        item = spec(payload)
        remote = MemoryRemote({item.source_path: payload})
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            prepare_hybrid_batch(remote, state_root=root, specs=[item])
            run_hybrid_transfer(remote, state_root=root, spec=item)
            remote.files[item.source_path] = b"different"
            with self.assertRaises(TransactionConflict):
                restore_hybrid_transfer(remote, state_root=root, spec=item)
        self.assertEqual(remote.files[item.source_path], b"different")
        self.assertEqual(remote.files[item.target_path], payload)
        self.assertEqual(remote.files[item.rollback_path], payload)

    def test_batch_manifest_uses_expected_quark_root(self):
        payload = b"root"
        item = spec(payload)
        self.assertEqual(item.rollback_root, DEFAULT_ROLLBACK_ROOT)
        self.assertTrue(item.rollback_path.startswith(
            "/quark/影视/ScrapeFlow/事务回滚/work-42/"
        ))

    def test_delete_mode_reconciles_500_restores_and_commits(self):
        payload = b"non-feature residual"
        item = delete_spec(payload)
        remote = MemoryRemote({item.source_path: payload})
        remote.remove_response_lost_for.add(item.source_path)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            prepare_hybrid_batch(remote, state_root=root, specs=[item])
            result = run_hybrid_transfer(remote, state_root=root, spec=item)
            self.assertEqual(result.state, "complete")
            self.assertNotIn(item.source_path, remote.files)
            self.assertEqual(remote.files[item.rollback_path], payload)
            restored = restore_hybrid_transfer(remote, state_root=root, spec=item)
            self.assertEqual(restored.state, "restored")
            self.assertEqual(remote.files[item.source_path], payload)
            commit_hybrid_batch(remote, state_root=root, specs=[item])
            self.assertNotIn(item.rollback_path, remote.files)

    def test_delete_mode_complete_can_commit_without_a_formal_target(self):
        payload = b"delete after acceptance"
        item = delete_spec(payload, batch="delete-commit")
        remote = MemoryRemote({item.source_path: payload})
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            prepare_hybrid_batch(remote, state_root=root, specs=[item])
            run_hybrid_transfer(remote, state_root=root, spec=item)
            committed = commit_hybrid_batch(remote, state_root=root, specs=[item])
            self.assertEqual(committed[0].state, "committed")
        self.assertNotIn(item.source_path, remote.files)
        self.assertNotIn(item.rollback_path, remote.files)

    def test_abort_restores_forward_items_and_closes_unmodified_items(self):
        first_payload, second_payload = b"forward", b"not-forward"
        first = spec(first_payload, batch="abort-42", item="episode-01")
        second = spec(second_payload, batch="abort-42", item="episode-02")
        remote = MemoryRemote({
            first.source_path: first_payload, second.source_path: second_payload,
        })
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            prepare_hybrid_batch(remote, state_root=root, specs=[first, second])
            run_hybrid_transfer(remote, state_root=root, spec=first)
            results = abort_hybrid_batch(
                remote, state_root=root, specs=[first, second],
            )
            self.assertEqual([result.state for result in results], ["aborted", "aborted"])
            resumed = abort_hybrid_batch(
                remote, state_root=root, specs=[first, second],
            )
            self.assertEqual([result.state for result in resumed], ["aborted", "aborted"])
            batch = json.loads((root / "abort-42" / "batch.json").read_text("utf-8"))
            self.assertEqual(batch["state"], "aborted")
            self.assertNotIn("cleanup_lease_token", batch)
            self.assertNotIn("cleanup_lease_purpose", batch)
            self.assertIn("cleanup_lease_receipt", batch)
            self.assertFalse((root / "abort-42" / "cleanup-lease.payload").exists())
        self.assertEqual(remote.files[first.source_path], first_payload)
        self.assertEqual(remote.files[second.source_path], second_payload)
        self.assertNotIn(first.target_path, remote.files)
        self.assertNotIn(second.target_path, remote.files)
        self.assertNotIn(first.rollback_path, remote.files)
        self.assertNotIn(second.rollback_path, remote.files)

    def test_abort_resumes_after_crash_once_validation_is_durable(self):
        payload = b"abort crash recovery"
        item = spec(payload, batch="abort-crash")
        remote = MemoryRemote({item.source_path: payload})
        crashed = False

        def crash(event, _journal):
            nonlocal crashed
            if event == "batch_abort_validated" and not crashed:
                crashed = True
                raise SimulatedCrash(event)

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            prepare_hybrid_batch(remote, state_root=root, specs=[item])
            with self.assertRaises(SimulatedCrash):
                abort_hybrid_batch(
                    remote, state_root=root, specs=[item], checkpoint_hook=crash,
                )
            self.assertIn(item.rollback_path, remote.files)
            results = abort_hybrid_batch(remote, state_root=root, specs=[item])
            self.assertEqual(results[0].state, "aborted")
            self.assertEqual(
                json.loads((root / "abort-crash" / "batch.json").read_text("utf-8"))["state"],
                "aborted",
            )

    def test_commit_reentry_revalidates_terminal_and_refuses_missing_target(self):
        payload = b"commit reentry"
        item = spec(payload, batch="commit-revalidate")
        remote = MemoryRemote({item.source_path: payload})

        def crash(event, _journal):
            if event == "committing":
                raise SimulatedCrash(event)

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            prepare_hybrid_batch(remote, state_root=root, specs=[item])
            run_hybrid_transfer(remote, state_root=root, spec=item)
            with self.assertRaises(SimulatedCrash):
                commit_hybrid_batch(
                    remote, state_root=root, specs=[item], checkpoint_hook=crash,
                )
            remote.files.pop(item.target_path)
            before = list(remote.remove_calls)
            with self.assertRaises(TransactionUncertain):
                commit_hybrid_batch(remote, state_root=root, specs=[item])
            self.assertEqual(remote.remove_calls, before)
            self.assertIn(item.rollback_path, remote.files)
            self.assertIn(item.remote_manifest_path, remote.files)

    def test_abort_reentry_revalidates_unmodified_source_before_remove(self):
        payload = b"abort reentry"
        item = spec(payload, batch="abort-revalidate")
        remote = MemoryRemote({item.source_path: payload})

        def crash(event, _journal):
            if event == "aborting":
                raise SimulatedCrash(event)

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            prepare_hybrid_batch(remote, state_root=root, specs=[item])
            with self.assertRaises(SimulatedCrash):
                abort_hybrid_batch(
                    remote, state_root=root, specs=[item], checkpoint_hook=crash,
                )
            remote.files.pop(item.source_path)
            before = list(remote.remove_calls)
            with self.assertRaises(TransactionUncertain):
                abort_hybrid_batch(remote, state_root=root, specs=[item])
            self.assertEqual(remote.remove_calls, before)
            self.assertIn(item.rollback_path, remote.files)
            self.assertIn(item.remote_manifest_path, remote.files)

    def test_each_commit_remove_revalidates_terminal_and_cleanup_lease(self):
        payload = b"live terminal"
        item = spec(payload, batch="commit-each-remove")

        class MutatingRemote(MemoryRemote):
            def remove_file(self, path: str) -> None:
                super().remove_file(path)
                if path == item.rollback_path:
                    self.files.pop(item.target_path, None)

        remote = MutatingRemote({item.source_path: payload})
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            prepare_hybrid_batch(remote, state_root=root, specs=[item])
            run_hybrid_transfer(remote, state_root=root, spec=item)
            with self.assertRaises(TransactionUncertain):
                commit_hybrid_batch(remote, state_root=root, specs=[item])
            self.assertNotIn(item.rollback_path, remote.files)
            self.assertIn(item.remote_manifest_path, remote.files)
            lease_path = f"{item.batch_root}/cleanup-lease.json"
            lease = json.loads(remote.files[lease_path])
            self.assertEqual(
                lease["scope"], "scrapeflow_internal_cleanup_exclusion_only"
            )
            self.assertFalse(lease["external_writer_cas_guarantee"])

    def test_tampered_cleanup_lease_blocks_commit_without_removal(self):
        payload = b"lease"
        item = spec(payload, batch="lease-tamper")
        remote = MemoryRemote({item.source_path: payload})

        def crash(event, _journal):
            if event == "committing":
                raise SimulatedCrash(event)

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            prepare_hybrid_batch(remote, state_root=root, specs=[item])
            run_hybrid_transfer(remote, state_root=root, spec=item)
            with self.assertRaises(SimulatedCrash):
                commit_hybrid_batch(
                    remote, state_root=root, specs=[item], checkpoint_hook=crash,
                )
            lease_path = f"{item.batch_root}/cleanup-lease.json"
            remote.files[lease_path] = b"{}\n"
            before = list(remote.remove_calls)
            with self.assertRaises(TransactionConflict):
                commit_hybrid_batch(remote, state_root=root, specs=[item])
            self.assertEqual(remote.remove_calls, before)
            self.assertIn(item.rollback_path, remote.files)

    def test_remote_cleanup_lease_is_adopted_after_pre_receipt_crash(self):
        payload = b"adopt remote lease"
        item = spec(payload, batch="lease-adopt")
        remote = MemoryRemote({item.source_path: payload})

        def crash(event, _journal):
            if event == "cleanup_lease_remote_verified":
                raise SimulatedCrash(event)

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            prepare_hybrid_batch(remote, state_root=root, specs=[item])
            run_hybrid_transfer(remote, state_root=root, spec=item)
            with self.assertRaises(SimulatedCrash):
                commit_hybrid_batch(
                    remote, state_root=root, specs=[item], checkpoint_hook=crash,
                )
            lease_path = f"{item.batch_root}/cleanup-lease.json"
            self.assertIn(lease_path, remote.files)
            batch_path = root / item.batch_id / "batch.json"
            crashed_batch = json.loads(batch_path.read_text("utf-8"))
            self.assertNotIn("cleanup_lease_token", crashed_batch)
            (root / item.batch_id / "cleanup-lease.payload").unlink()
            uploads_before = list(remote.upload_calls)

            committed = commit_hybrid_batch(
                remote, state_root=root, specs=[item],
            )
            self.assertEqual(committed[0].state, "committed")
            self.assertEqual(remote.upload_calls, uploads_before)

    def test_remote_cleanup_lease_adoption_rejects_wrong_spec_digest(self):
        payload = b"reject foreign lease"
        item = spec(payload, batch="lease-adopt-conflict")
        remote = MemoryRemote({item.source_path: payload})

        def crash(event, _journal):
            if event == "cleanup_lease_remote_verified":
                raise SimulatedCrash(event)

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            prepare_hybrid_batch(remote, state_root=root, specs=[item])
            run_hybrid_transfer(remote, state_root=root, spec=item)
            with self.assertRaises(SimulatedCrash):
                commit_hybrid_batch(
                    remote, state_root=root, specs=[item], checkpoint_hook=crash,
                )
            lease_path = f"{item.batch_root}/cleanup-lease.json"
            lease = json.loads(remote.files[lease_path])
            lease["specs_sha256"] = "0" * 64
            remote.files[lease_path] = (
                json.dumps(lease, sort_keys=True, separators=(",", ":")) + "\n"
            ).encode()
            (root / item.batch_id / "cleanup-lease.payload").unlink()
            before = list(remote.remove_calls)
            with self.assertRaises(TransactionConflict):
                commit_hybrid_batch(remote, state_root=root, specs=[item])
            self.assertEqual(remote.remove_calls, before)

    def test_commit_reentry_requires_live_rollback_item_and_batch_witnesses(self):
        payload = b"three live witnesses"

        def crash(event, _journal):
            if event == "committing":
                raise SimulatedCrash(event)

        for suffix, missing_path in (
            ("rollback", lambda item: item.rollback_path),
            ("item", lambda item: item.remote_manifest_path),
            ("batch", lambda item: f"{item.batch_root}/batch-manifest.json"),
        ):
            with self.subTest(witness=suffix), tempfile.TemporaryDirectory() as tmp:
                item = spec(payload, batch=f"missing-{suffix}")
                remote = MemoryRemote({item.source_path: payload})
                root = Path(tmp)
                prepare_hybrid_batch(remote, state_root=root, specs=[item])
                run_hybrid_transfer(remote, state_root=root, spec=item)
                with self.assertRaises(SimulatedCrash):
                    commit_hybrid_batch(
                        remote, state_root=root, specs=[item], checkpoint_hook=crash,
                    )
                remote.files.pop(missing_path(item))
                before = list(remote.remove_calls)
                with self.assertRaises(TransactionUncertain):
                    commit_hybrid_batch(remote, state_root=root, specs=[item])
                self.assertEqual(remote.remove_calls, before)

    def test_committed_reentry_only_revalidates_media_and_never_cleans_again(self):
        payload = b"committed idempotence"
        item = spec(payload, batch="committed-reentry")
        remote = MemoryRemote({item.source_path: payload})
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            prepare_hybrid_batch(remote, state_root=root, specs=[item])
            run_hybrid_transfer(remote, state_root=root, spec=item)
            commit_hybrid_batch(remote, state_root=root, specs=[item])
            batch_path = root / item.batch_id / "batch.json"
            terminal_batch = json.loads(batch_path.read_text("utf-8"))
            self.assertNotIn("cleanup_lease_token", terminal_batch)
            self.assertNotIn("cleanup_lease_purpose", terminal_batch)
            self.assertIn("cleanup_lease_receipt", terminal_batch)
            self.assertFalse(
                (root / item.batch_id / "cleanup-lease.payload").exists()
            )
            compact_bytes = batch_path.read_bytes()
            resumed = commit_hybrid_batch(remote, state_root=root, specs=[item])
            self.assertEqual(resumed[0].state, "committed")
            self.assertEqual(batch_path.read_bytes(), compact_bytes)
            before = list(remote.remove_calls)
            remote.files.pop(item.target_path)
            with self.assertRaises(TransactionUncertain):
                commit_hybrid_batch(remote, state_root=root, specs=[item])
            self.assertEqual(remote.remove_calls, before)

    def test_abort_fails_closed_on_unmodified_source_tamper_and_keeps_all_rollbacks(self):
        first_payload, second_payload = b"one", b"two"
        first = spec(first_payload, batch="abort-conflict", item="episode-01")
        second = spec(second_payload, batch="abort-conflict", item="episode-02")
        remote = MemoryRemote({
            first.source_path: first_payload, second.source_path: second_payload,
        })
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            prepare_hybrid_batch(remote, state_root=root, specs=[first, second])
            remote.files[second.source_path] = b"bad"
            with self.assertRaises(TransactionConflict):
                abort_hybrid_batch(remote, state_root=root, specs=[first, second])
        self.assertEqual(remote.files[first.rollback_path], first_payload)
        self.assertEqual(remote.files[second.rollback_path], second_payload)
        self.assertIn(f"{first.batch_root}/batch-manifest.json", remote.files)


if __name__ == "__main__":
    unittest.main()
