import contextlib
import hashlib
import importlib.util
import io
import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock


class InMemoryArchiveAList:
    def __init__(self, files, *, upload_behavior="success"):
        self.files = dict(files)
        self.upload_behavior = upload_behavior
        self.upload_calls = 0
        self.removed = []

    def exact_file_info(self, path):
        payload = self.files.get(path)
        if payload is None:
            return None
        return {
            "size": len(payload),
            "sha256": hashlib.sha256(payload).hexdigest(),
            "version": hashlib.sha256(path.encode() + payload).hexdigest(),
        }

    @contextlib.contextmanager
    def open_file_reader(self, path):
        payload = self.files.get(path)
        if payload is None:
            raise OSError(f"missing: {path}")
        yield io.BytesIO(payload)

    def upload_file(self, target_path, source, content_type="application/octet-stream"):
        self.upload_calls += 1
        payload = source.read_bytes()
        if self.upload_behavior == "lost-after-commit":
            self.files[target_path] = payload
            raise TimeoutError("response lost")
        if self.upload_behavior == "http-500":
            raise OSError("HTTP 500")
        if self.upload_behavior == "both-missing":
            self.files.pop("/src/source.bin", None)
            raise OSError("HTTP 500 and provider lost source")
        if target_path in self.files:
            raise OSError("target exists")
        self.files[target_path] = payload

    def remove(self, parent, names):
        for name in names:
            path = (parent.rstrip("/") or "") + "/" + name
            self.files.pop(path, None)
            self.removed.append(path)

    def list(self, parent, refresh=False):
        return [
            {
                "name": path.rsplit("/", 1)[-1],
                "is_dir": False,
                "size": len(payload),
            }
            for path, payload in self.files.items()
            if path.rsplit("/", 1)[0] == parent
        ]


class ExtractArchiveFileTransactionTests(unittest.TestCase):
    @staticmethod
    def load_tool(module_name):
        tool_path = Path(__file__).resolve().parents[1] / "tools" / "extract_archives.py"
        spec = importlib.util.spec_from_file_location(module_name, tool_path)
        module = importlib.util.module_from_spec(spec)
        assert spec.loader is not None
        spec.loader.exec_module(module)
        return module

    def run_transfer(self, alist, *, module_name="extract_archive_txn"):
        module = self.load_tool(module_name)
        plan_sha = "a" * 64
        journal = {"plan_sha256": plan_sha}
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        journal_path = Path(temporary.name) / "archive-journal.json"
        module._execute_archive_file_transaction(
            alist,
            plan_sha256=plan_sha,
            source_path="/src/source.bin",
            target_path="/src/target.bin",
            expected_size=len(b"payload"),
            journal=journal,
            journal_path=journal_path,
        )
        return module, journal, journal_path

    def test_lost_upload_response_is_reconciled_without_second_put(self):
        alist = InMemoryArchiveAList(
            {"/src/source.bin": b"payload"},
            upload_behavior="lost-after-commit",
        )
        _module, journal, _journal_path = self.run_transfer(
            alist, module_name="extract_archive_lost_response_txn"
        )

        self.assertEqual(alist.upload_calls, 1)
        self.assertNotIn("/src/source.bin", alist.files)
        self.assertEqual(alist.files["/src/target.bin"], b"payload")
        transaction = next(iter(journal["remote_file_transactions"].values()))
        self.assertEqual(transaction["state"], "complete")
        self.assertFalse(transaction["payload_retained"])
        self.assertFalse(
            (Path(transaction["stage_directory"]) / "payload.bin").exists()
        )

    def test_http_500_without_target_keeps_source_and_durable_payload(self):
        module = self.load_tool("extract_archive_http_500_txn")
        alist = InMemoryArchiveAList(
            {"/src/source.bin": b"payload"}, upload_behavior="http-500"
        )
        journal = {"plan_sha256": "b" * 64}
        with tempfile.TemporaryDirectory() as temporary:
            journal_path = Path(temporary) / "archive-journal.json"
            with self.assertRaisesRegex(module.ScraperError, "payload"):
                module._execute_archive_file_transaction(
                    alist,
                    plan_sha256="b" * 64,
                    source_path="/src/source.bin",
                    target_path="/src/target.bin",
                    expected_size=7,
                    journal=journal,
                    journal_path=journal_path,
                )
            transaction = next(iter(journal["remote_file_transactions"].values()))
            self.assertTrue(
                (Path(transaction["stage_directory"]) / "payload.bin").is_file()
            )
            transaction_journal = json.loads(
                (Path(transaction["stage_directory"]) / "journal.json").read_text(
                    encoding="utf-8"
                )
            )

        self.assertEqual(alist.upload_calls, 1)
        self.assertEqual(alist.files, {"/src/source.bin": b"payload"})
        self.assertEqual(transaction_journal["state"], "upload_uncertain")

    def test_both_remote_paths_missing_after_put_keeps_only_proven_local_copy(self):
        module = self.load_tool("extract_archive_both_missing_txn")
        alist = InMemoryArchiveAList(
            {"/src/source.bin": b"payload"}, upload_behavior="both-missing"
        )
        journal = {"plan_sha256": "c" * 64}
        with tempfile.TemporaryDirectory() as temporary:
            journal_path = Path(temporary) / "archive-journal.json"
            with self.assertRaises(module.ScraperError):
                module._execute_archive_file_transaction(
                    alist,
                    plan_sha256="c" * 64,
                    source_path="/src/source.bin",
                    target_path="/src/target.bin",
                    expected_size=7,
                    journal=journal,
                    journal_path=journal_path,
                )
            transaction = next(iter(journal["remote_file_transactions"].values()))
            payload_path = Path(transaction["stage_directory"]) / "payload.bin"
            self.assertEqual(payload_path.read_bytes(), b"payload")

        self.assertEqual(alist.upload_calls, 1)
        self.assertEqual(alist.files, {})

    def test_matching_existing_target_is_verified_then_source_is_deleted(self):
        alist = InMemoryArchiveAList(
            {
                "/src/source.bin": b"payload",
                "/src/target.bin": b"payload",
            }
        )
        self.run_transfer(alist, module_name="extract_archive_matching_target_txn")

        self.assertEqual(alist.upload_calls, 0)
        self.assertNotIn("/src/source.bin", alist.files)
        self.assertEqual(alist.files["/src/target.bin"], b"payload")

    def test_conflicting_existing_target_preserves_every_copy_and_stage(self):
        module = self.load_tool("extract_archive_conflicting_target_txn")
        alist = InMemoryArchiveAList(
            {
                "/src/source.bin": b"payload",
                "/src/target.bin": b"differs",
            }
        )
        journal = {"plan_sha256": "d" * 64}
        with tempfile.TemporaryDirectory() as temporary:
            journal_path = Path(temporary) / "archive-journal.json"
            with self.assertRaisesRegex(module.ScraperError, "different content"):
                module._execute_archive_file_transaction(
                    alist,
                    plan_sha256="d" * 64,
                    source_path="/src/source.bin",
                    target_path="/src/target.bin",
                    expected_size=7,
                    journal=journal,
                    journal_path=journal_path,
                )
            transaction = next(iter(journal["remote_file_transactions"].values()))
            self.assertTrue(
                (Path(transaction["stage_directory"]) / "payload.bin").is_file()
            )

        self.assertEqual(alist.upload_calls, 0)
        self.assertEqual(alist.files["/src/source.bin"], b"payload")
        self.assertEqual(alist.files["/src/target.bin"], b"differs")

    def test_reverse_restore_uses_a_second_file_transaction(self):
        module = self.load_tool("extract_archive_restore_txn")
        alist = InMemoryArchiveAList({"/src/source.bin": b"payload"})
        plan_sha = "e" * 64
        journal = {"plan_sha256": plan_sha}
        with tempfile.TemporaryDirectory() as temporary:
            journal_path = Path(temporary) / "archive-journal.json"
            module._execute_archive_file_transaction(
                alist,
                plan_sha256=plan_sha,
                source_path="/src/source.bin",
                target_path="/src/target.bin",
                expected_size=7,
                journal=journal,
                journal_path=journal_path,
            )
            module._execute_archive_file_transaction(
                alist,
                plan_sha256=plan_sha,
                source_path="/src/target.bin",
                target_path="/src/source.bin",
                expected_size=7,
                journal=journal,
                journal_path=journal_path,
            )

        self.assertEqual(alist.upload_calls, 2)
        self.assertEqual(alist.files, {"/src/source.bin": b"payload"})
        self.assertEqual(len(journal["remote_file_transactions"]), 2)

    def test_transaction_identity_and_temporary_name_are_deterministic(self):
        module = self.load_tool("extract_archive_deterministic_txn")
        first = module._archive_transaction_id(
            "f" * 64, "/src/a.exe", "/src/a.mkv"
        )
        second = module._archive_transaction_id(
            "f" * 64, "/src/a.exe", "/src/a.mkv"
        )
        self.assertEqual(first, second)
        self.assertEqual(
            module._archive_temporary_name("f" * 64, "/src/a.exe", "zip"),
            module._archive_temporary_name("f" * 64, "/src/a.exe", "zip"),
        )

    def test_local_upload_lost_response_binds_parent_receipt_once(self):
        module = self.load_tool("extract_archive_local_lost_response")
        alist = InMemoryArchiveAList(
            {}, upload_behavior="lost-after-commit"
        )
        journal = {}
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            context = module.ArchiveLocalUploadContext(
                root / "transactions",
                "1" * 64,
                journal,
                root / "archive-journal.json",
            )
            receipt = module._execute_archive_local_upload(
                alist,
                b"subtitle-payload",
                "/src/Show.S01E01.ass",
                "text/x-ssa",
                context=context,
            )
            payloads = list((root / "transactions" / "payloads").glob("*"))

        self.assertEqual(alist.upload_calls, 1)
        self.assertEqual(alist.files["/src/Show.S01E01.ass"], b"subtitle-payload")
        self.assertEqual(receipt["state"], "complete")
        self.assertRegex(receipt["receipt_sha256"], r"^[0-9a-f]{64}$")
        self.assertEqual(payloads, [])
        self.assertEqual(len(journal["local_upload_receipts"]), 1)

    def test_local_upload_500_retains_payload_and_never_reputs(self):
        module = self.load_tool("extract_archive_local_500")
        alist = InMemoryArchiveAList({}, upload_behavior="http-500")
        journal = {}
        with tempfile.TemporaryDirectory() as temporary, mock.patch.object(
            module.run_local_upload_transaction.__globals__["time"], "sleep"
        ):
            root = Path(temporary)
            context = module.ArchiveLocalUploadContext(
                root / "transactions",
                "2" * 64,
                journal,
                root / "archive-journal.json",
            )
            for _ in range(2):
                with self.assertRaisesRegex(module.ScraperError, "payload"):
                    module._execute_archive_local_upload(
                        alist,
                        b"only-durable-copy",
                        "/src/Show.S01E02.ass",
                        "text/x-ssa",
                        context=context,
                    )
            payloads = list((root / "transactions" / "payloads").glob("*"))
            transaction_journals = list(
                (root / "transactions").glob("local-upload-*/journal.json")
            )
            child = json.loads(transaction_journals[0].read_text(encoding="utf-8"))

        self.assertEqual(alist.upload_calls, 1)
        self.assertEqual(len(payloads), 1)
        self.assertEqual(child["state"], "upload_uncertain")
        parent = next(iter(journal["local_upload_receipts"].values()))
        self.assertEqual(parent["upload_calls"], 1)

    def test_native_archive_receipt_hashes_every_exact_output_and_detects_tamper(self):
        module = self.load_tool("extract_archive_native_receipt")
        payload = b"native-output"
        alist = InMemoryArchiveAList({"/dst/Show.S01E01.mkv": payload})
        members = [
            {"path": "Show.S01E01.mkv", "is_dir": False, "size": len(payload)}
        ]
        archive = {
            "archive_path": "/src/show.7z",
            "dst_dir": "/dst",
            "members": members,
            "members_sha256": module._members_digest(members),
        }
        journal = {}
        with tempfile.TemporaryDirectory() as temporary:
            journal_path = Path(temporary) / "archive-journal.json"
            receipt = module._capture_native_archive_receipt(
                alist,
                archive,
                archive_identity_path="/src/show.7z",
                plan_sha256="3" * 64,
                task_ids={"task-1"},
                journal=journal,
                journal_path=journal_path,
            )
            self.assertTrue(
                module._verify_native_archive_receipt(
                    alist,
                    archive,
                    archive_identity_path="/src/show.7z",
                    plan_sha256="3" * 64,
                    journal=journal,
                )
            )
            alist.files["/dst/Show.S01E01.mkv"] = b"tamper-output"
            with self.assertRaisesRegex(module.ScraperError, "内容已变化"):
                module._verify_native_archive_receipt(
                    alist,
                    archive,
                    archive_identity_path="/src/show.7z",
                    plan_sha256="3" * 64,
                    journal=journal,
                )

        self.assertEqual(receipt["task_ids"], ["task-1"])
        self.assertRegex(receipt["outputs"][0]["sha256"], r"^[0-9a-f]{64}$")

    def test_native_archive_reentry_rejects_existing_output_without_receipt(self):
        module = self.load_tool("extract_archive_native_reentry_gate")
        payload = b"existing"
        alist = InMemoryArchiveAList({"/dst/Show.S01E01.mkv": payload})
        archive = {
            "archive_path": "/src/show.7z",
            "dst_dir": "/dst",
            "members": [
                {
                    "path": "Show.S01E01.mkv",
                    "is_dir": False,
                    "size": len(payload),
                }
            ],
        }
        with self.assertRaisesRegex(module.ScraperError, "拒绝仅凭存在重入"):
            module._reject_native_archive_reentry_conflicts(alist, archive)

    def test_archive_lock_nonce_mismatch_fails_and_releases_lock(self):
        module = self.load_tool("extract_archive_lock_nonce")
        plan = {"source_root": "/src", "media_renames": [], "archives": []}

        class WrongNonceAList:
            def __init__(self):
                self.lock_name = None
                self.removed = False

            def server_version(self):
                return "v3.57.0"

            def upload_bytes(self, path, data, content_type):
                self.lock_name = path.rsplit("/", 1)[-1]

            def list(self, path, refresh=False):
                return (
                    [{"name": self.lock_name, "is_dir": False}]
                    if self.lock_name
                    else []
                )

            def read_file_bytes(self, path, *, max_bytes):
                return b'{"nonce":"wrong"}'

            def remove(self, parent, names):
                self.removed = True
                self.lock_name = None

        alist = WrongNonceAList()
        with tempfile.TemporaryDirectory() as temporary:
            with self.assertRaisesRegex(module.ScraperError, "nonce"):
                module.execute_archive_plan(
                    alist,
                    plan,
                    {},
                    timeout=1,
                    journal_path=Path(temporary) / "archive-journal.json",
                )
        self.assertTrue(alist.removed)

    def test_native_archive_execution_binds_hashed_output_receipt(self):
        module = self.load_tool("extract_archive_native_execution_receipt")
        archive_bytes = b"archive"
        output = b"native-video"
        members = [
            {"path": "Show.S01E01.mkv", "is_dir": False, "size": len(output)}
        ]
        archive = {
            "archive_path": "/src/show.7z",
            "src_dir": "/src",
            "dst_dir": "/src",
            "name": "show.7z",
            "parts": [
                {
                    "name": "show.7z",
                    "path": "/src/show.7z",
                    "size": len(archive_bytes),
                    "modified": None,
                    "hash": None,
                }
            ],
            "members": members,
            "members_sha256": module._members_digest(members),
            "deferred_inspection": False,
        }
        plan = {
            "source_root": "/src",
            "media_renames": [],
            "archives": [archive],
        }

        class NativeAList(InMemoryArchiveAList):
            def __init__(self):
                super().__init__({"/src/show.7z": archive_bytes})
                self.submitted = False
                self.lock_path = None
                self.lock_payload = None

            def server_version(self):
                return "v3.57.0"

            def archive_meta(self, path, archive_password="", refresh=True):
                return {
                    "content": [
                        {
                            "name": member["path"],
                            "is_dir": member["is_dir"],
                            "size": member["size"],
                        }
                        for member in members
                    ]
                }

            def upload_bytes(self, path, data, content_type):
                self.lock_path = path
                self.lock_payload = data
                self.files[path] = data

            def read_file_bytes(self, path, *, max_bytes):
                return self.files[path]

            def archive_tasks(self, kind, done=False):
                if not self.submitted or not done:
                    return []
                if kind == "decompress":
                    return [{"id": "download-task", "state": 2}]
                return [{"id": "upload-task", "state": 2}]

            def archive_decompress(self, **kwargs):
                self.submitted = True
                self.files["/src/Show.S01E01.mkv"] = output
                return [{"id": "download-task", "state": 1}]

            def try_list(self, path, refresh=False):
                return self.list(path, refresh=refresh)

        alist = NativeAList()
        with tempfile.TemporaryDirectory() as temporary, mock.patch.object(
            module.time, "sleep"
        ):
            journal_path = Path(temporary) / "archive-journal.json"
            module.execute_archive_plan(
                alist,
                plan,
                {"/src/show.7z": ""},
                timeout=1,
                journal_path=journal_path,
            )
            journal = json.loads(journal_path.read_text(encoding="utf-8"))

        receipt = journal["native_archive_receipts"]["/src/show.7z"]
        self.assertEqual(journal["status"], "success")
        self.assertEqual(receipt["task_ids"], ["download-task", "upload-task"])
        self.assertEqual(
            receipt["outputs"],
            [
                {
                    "path": "/src/Show.S01E01.mkv",
                    "relative_path": "Show.S01E01.mkv",
                    "size": len(output),
                    "sha256": hashlib.sha256(output).hexdigest(),
                }
            ],
        )
        self.assertIn("/src/show.7z", alist.files)


if __name__ == "__main__":
    unittest.main()
