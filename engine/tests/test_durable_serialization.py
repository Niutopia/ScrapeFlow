from __future__ import annotations

import errno
import json
import os
from pathlib import Path
import stat
import sys
import tempfile
import unittest
from unittest import mock
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[2]
ENGINE_ROOT = PROJECT_ROOT / "engine"
for root in (PROJECT_ROOT, ENGINE_ROOT):
    if str(root) not in sys.path:
        sys.path.insert(0, str(root))

import scraper
from engine.scrapeflow import serialization
from engine.scrapeflow.models import ExecutionJournal
from engine.scrapeflow.quark_native_helper import SubmitJournal


class DurableSerializationTests(unittest.TestCase):
    def _record_publish_events(self) -> tuple[list[str], Any, Any]:
        events: list[str] = []
        real_fsync = serialization.os.fsync
        real_replace = serialization.os.replace

        def tracked_fsync(descriptor: int) -> None:
            mode = os.fstat(descriptor).st_mode
            events.append(
                "directory-fsync" if stat.S_ISDIR(mode) else "file-fsync"
            )
            real_fsync(descriptor)

        def tracked_replace(source: os.PathLike[str], target: os.PathLike[str]) -> None:
            events.append("replace")
            real_replace(source, target)

        return (
            events,
            mock.patch.object(serialization.os, "fsync", side_effect=tracked_fsync),
            mock.patch.object(serialization.os, "replace", side_effect=tracked_replace),
        )

    def test_atomic_json_syncs_file_then_replaces_then_syncs_directory(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "checkpoint.json"
            events, fsync_patch, replace_patch = self._record_publish_events()
            with fsync_patch, replace_patch:
                serialization.atomic_write_json(path, {"value": "durable"})

            self.assertEqual(
                events,
                ["file-fsync", "replace", "directory-fsync"],
            )
            self.assertEqual(json.loads(path.read_text(encoding="utf-8")), {
                "value": "durable",
            })
            self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o600)

    def test_exclusive_reservation_syncs_inode_then_directory(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "reserved.json"
            events, fsync_patch, replace_patch = self._record_publish_events()
            with fsync_patch, replace_patch:
                serialization.reserve_output_path(path)

            self.assertEqual(events, ["file-fsync", "directory-fsync"])
            self.assertEqual(path.read_bytes(), b"")
            self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o600)

    def test_replace_failure_preserves_reservation_and_cleans_temporary(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path = root / "reserved.json"
            serialization.reserve_output_path(path)

            with mock.patch.object(
                serialization.os, "replace", side_effect=OSError(errno.EIO, "injected"),
            ), self.assertRaisesRegex(OSError, "injected"):
                serialization.write_json_reserved(path, {"value": 1})

            self.assertEqual(path.read_bytes(), b"")
            self.assertEqual(list(root.glob(f".{path.name}.*.tmp")), [])

    def test_failed_reservation_sync_removes_exclusive_marker(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "reserved.json"
            with mock.patch.object(
                serialization,
                "_fsync_directory",
                side_effect=OSError(errno.EIO, "injected"),
            ), self.assertRaisesRegex(OSError, "injected"):
                serialization.reserve_output_path(path)
            self.assertFalse(path.exists())

    def test_linux_and_macos_directory_sync_failures_are_never_downgraded(self):
        with tempfile.TemporaryDirectory() as directory:
            for platform in ("linux", "darwin"):
                with self.subTest(platform=platform):
                    descriptor = os.open(directory, os.O_RDONLY)
                    with mock.patch.object(
                        serialization.sys, "platform", platform,
                    ), mock.patch.object(
                        serialization.os, "open", return_value=descriptor,
                    ), mock.patch.object(
                        serialization.os,
                        "fsync",
                        side_effect=OSError(errno.EINVAL, "injected"),
                    ), self.assertRaisesRegex(OSError, "injected"):
                        serialization._fsync_directory(Path(directory))

    def test_windows_directory_sync_policy_is_an_explicit_noop(self):
        parent = Path("C:/checkpoint-parent")
        with mock.patch.object(serialization.os, "name", "nt"), mock.patch.object(
            serialization.os, "open",
        ) as open_mock:
            serialization._fsync_directory(parent)
        open_mock.assert_not_called()

    def test_known_unsupported_directory_sync_is_skipped_only_off_linux_and_macos(self):
        with mock.patch.object(serialization.sys, "platform", "plan9"), mock.patch.object(
            serialization.os, "open", return_value=41,
        ) as open_mock, mock.patch.object(
            serialization.os, "fsync", side_effect=OSError(errno.EINVAL, "unsupported"),
        ), mock.patch.object(serialization.os, "close") as close_mock:
            serialization._fsync_directory(Path("/checkpoint-parent"))

        open_mock.assert_called_once()
        close_mock.assert_called_once_with(41)

    def test_pending_execution_journal_is_durable_before_first_remote_mutation(self):
        events: list[str] = []

        class LockClient:
            lock_name: str | None = None

            def try_list(self, path: str, refresh: bool = False):
                if self.lock_name is None:
                    return []
                return [{"name": self.lock_name, "is_dir": False}]

            def upload_bytes(self, path: str, payload: bytes, content_type: str) -> None:
                events.append("remote-mutation")
                self.lock_name = path.rsplit("/", 1)[-1]

            def remove(self, parent: str, names: list[str]) -> None:
                self.lock_name = None

        plan = scraper.Plan(
            mode="movie",
            source_root="/src",
            target_root="/dst",
            files=[],
            warnings=[],
            metadata={},
        )
        journal = ExecutionJournal("2026-08-03T00:00:00+00:00", {}, [])
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "journal.json"
            serialization.reserve_output_path(path)
            journal.save(path)
            real_fsync = serialization.os.fsync
            real_replace = serialization.os.replace

            def tracked_fsync(descriptor: int) -> None:
                mode = os.fstat(descriptor).st_mode
                events.append(
                    "directory-fsync" if stat.S_ISDIR(mode) else "file-fsync"
                )
                real_fsync(descriptor)

            def tracked_replace(source: os.PathLike[str], target: os.PathLike[str]) -> None:
                events.append("replace")
                real_replace(source, target)

            with mock.patch.object(
                serialization.os, "fsync", side_effect=tracked_fsync,
            ), mock.patch.object(
                serialization.os, "replace", side_effect=tracked_replace,
            ):
                scraper._acquire_remote_lock(
                    LockClient(), plan, journal, path, "/src", "/src", "source",
                )

            mutation_index = events.index("remote-mutation")
            self.assertEqual(
                events[: mutation_index + 1],
                ["file-fsync", "replace", "directory-fsync", "remote-mutation"],
            )

    def test_directory_sync_failure_blocks_first_remote_mutation(self):
        plan = scraper.Plan(
            mode="movie", source_root="/src", target_root="/dst",
            files=[], warnings=[], metadata={},
        )
        journal = ExecutionJournal("2026-08-03T00:00:00+00:00", {}, [])
        client = mock.Mock()
        client.try_list.return_value = []
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path = root / "journal.json"
            serialization.reserve_output_path(path)
            journal.save(path)

            with mock.patch.object(
                serialization,
                "_fsync_directory",
                side_effect=OSError(errno.EIO, "directory sync failed"),
            ), self.assertRaisesRegex(OSError, "directory sync failed"):
                scraper._acquire_remote_lock(
                    client, plan, journal, path, "/src", "/src", "source",
                )

            client.upload_bytes.assert_not_called()
            self.assertEqual(list(root.glob(f".{path.name}.*.tmp")), [])

    def test_submit_journal_sync_failure_blocks_quark_operation(self):
        operation = mock.Mock(return_value={"code": 0})
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "submit.json"
            with mock.patch.object(
                serialization,
                "_fsync_directory",
                side_effect=OSError(errno.EIO, "directory sync failed"),
            ), self.assertRaisesRegex(OSError, "directory sync failed"):
                SubmitJournal(path).run("a" * 64, operation)

            operation.assert_not_called()
            self.assertEqual(list(path.parent.glob(f".{path.name}.*.tmp")), [])


if __name__ == "__main__":
    unittest.main()
