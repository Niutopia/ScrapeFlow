"""Fail-closed host-state backup: coordinated pause, quiescence, restore.

Every test drives ``backup_host_state.run`` against a throwaway state tree
and an in-process fake of the ScrapeFlow API.  Nothing here touches the real
API, the real state directories, Docker, Quark, or any host persistent state.
"""

from __future__ import annotations

import base64
from datetime import datetime, timezone
import fcntl
import hashlib
import json
import os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
import shutil
import socket
import sqlite3
import tarfile
import tempfile
import threading
from unittest import mock
import unittest
from urllib.parse import urlsplit

from scripts.backup_host_state import (
    BACKUP_PAUSE_REASON_PREFIX,
    ApiClient,
    ApiError,
    BusyStateError,
    PAUSE_JOURNAL_NAME,
    SNAPSHOT_NAME_RE,
    held_slot_locks,
    job_phase_is_busy,
    require_paused,
    restore_pause,
    run,
    write_atomic_bytes,
)


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


class FakeApiState:
    def __init__(self) -> None:
        self.paused: object = False
        self.reason: str | None = None
        self.jobs: list[dict] = []
        self.requests: list[tuple[str, str, dict | None]] = []


class FakeApiHandler(BaseHTTPRequestHandler):
    """Mimics the real API: control writes are mirrored to the durable file."""

    def log_message(self, *args: object) -> None:  # silence test noise
        return

    def _send(self, code: int, payload: dict) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _read_body(self) -> dict:
        length = int(self.headers.get("Content-Length", "0"))
        if length <= 0:
            return {}
        return json.loads(self.rfile.read(length).decode("utf-8"))

    def _control_doc(self, state: FakeApiState) -> dict:
        return {
            "paused": state.paused,
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "reason": state.reason,
            "persistent": True,
        }

    def _persist_control(self) -> None:
        # Mirrors PersistentGlobalControl.set_paused: the API refreshes
        # ``updated_at`` every time it rewrites the durable document.
        state: FakeApiState = self.server.state  # type: ignore[attr-defined]
        doc = {
            "version": 1,
            "paused": state.paused,
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "reason": state.reason,
        }
        control_path: Path = self.server.control_path  # type: ignore[attr-defined]
        control_path.write_text(
            json.dumps(doc, ensure_ascii=False, indent=2) + "\n", encoding="utf-8",
        )

    def do_GET(self) -> None:  # noqa: N802
        state: FakeApiState = self.server.state  # type: ignore[attr-defined]
        path = urlsplit(self.path).path
        state.requests.append(("GET", path, None))
        if path == "/api/control":
            self._send(200, self._control_doc(state))
        elif path == "/api/jobs":
            self._send(200, {"jobs": state.jobs})
        else:
            self._send(404, {"error": "not found"})

    def do_POST(self) -> None:  # noqa: N802
        state: FakeApiState = self.server.state  # type: ignore[attr-defined]
        path = urlsplit(self.path).path
        body = self._read_body()
        state.requests.append(("POST", path, body))
        if path in {"/api/control/pause", "/api/control/resume"}:
            if body.get("confirm") is not True:
                self._send(400, {"error": "confirm required"})
                return
            paused = path.endswith("/pause")
            state.paused = paused
            state.reason = body.get("reason") if paused else None
            self._persist_control()
            self._send(200, self._control_doc(state))
        else:
            self._send(404, {"error": "not found"})


class FakeApiServer:
    def __init__(self, control_path: Path) -> None:
        self.state = FakeApiState()
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), FakeApiHandler)
        self.server.state = self.state  # type: ignore[attr-defined]
        self.server.control_path = control_path  # type: ignore[attr-defined]
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    @property
    def url(self) -> str:
        host, port = self.server.server_address[:2]
        return f"http://{host}:{port}"

    def close(self) -> None:
        self.server.shutdown()
        self.server.server_close()


def closed_port_url() -> str:
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
    return f"http://127.0.0.1:{port}"


CONTROL_DOC = {
    "version": 1,
    "paused": False,
    "updated_at": "2026-08-02T00:00:00+00:00",
    "reason": None,
}


def control_bytes(paused: bool = False, reason: str | None = None) -> bytes:
    doc = dict(CONTROL_DOC)
    doc["paused"] = paused
    doc["reason"] = reason
    return (json.dumps(doc, ensure_ascii=False, indent=2) + "\n").encode("utf-8")


def make_state(root: Path) -> tuple[Path, Path]:
    """Build a minimal state tree; returns (alist_dir, scrapeflow_dir)."""
    alist = root / "alist-data"
    alist.mkdir(parents=True)
    scrapeflow = root / "scrapeflow-data"
    (scrapeflow / "jobs").mkdir(parents=True)
    connection = sqlite3.connect(alist / "data.db")
    connection.execute("CREATE TABLE t (k TEXT PRIMARY KEY, v TEXT)")
    connection.execute("INSERT INTO t VALUES ('a', '1')")
    connection.commit()
    connection.close()
    (alist / "config.json").write_text(json.dumps({"alist": True}), encoding="utf-8")
    (scrapeflow / "global-control.json").write_bytes(control_bytes())
    (scrapeflow / "sample.txt").write_text("hello", encoding="utf-8")
    return alist, scrapeflow


def run_backup(root: Path, api_url: str, extra: list[str] | None = None) -> dict:
    backup = root / "Backups"
    return run([
        "--state-root", str(root),
        "--backup-root", str(backup),
        "--api-url", api_url,
        "--quiesce-timeout", "0",
        *(extra or []),
    ])


class BackupHostStateTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.mkdtemp(prefix="backup-host-state-test-")
        self.root = Path(self.tmp) / "State"
        self.alist, self.scrapeflow = make_state(self.root)
        self.control_path = self.scrapeflow / "global-control.json"
        self.backup = self.root / "Backups"
        self.api = FakeApiServer(self.control_path)
        self.addCleanup(self.api.close)
        self.addCleanup(shutil.rmtree, self.tmp, ignore_errors=True)

    def snapshot_dirs(self) -> list[Path]:
        if not self.backup.exists():
            return []
        return sorted(
            path for path in self.backup.iterdir()
            if path.is_dir() and SNAPSHOT_NAME_RE.fullmatch(path.name)
        )

    # ------------------------------------------------------------------ units

    def test_require_paused_refuses_when_not_paused(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "global pause is not active"):
            require_paused(self.scrapeflow)
        self.control_path.write_bytes(control_bytes(paused=True, reason="maintenance"))
        self.assertTrue(require_paused(self.scrapeflow)["paused"])

    def test_require_paused_rejects_non_boolean_paused(self) -> None:
        for paused in (None, 0, 1, "true", "false", [], {}):
            with self.subTest(paused=paused):
                document = dict(CONTROL_DOC, paused=paused)
                self.control_path.write_text(json.dumps(document), encoding="utf-8")
                with self.assertRaisesRegex(RuntimeError, "strict boolean"):
                    require_paused(self.scrapeflow)

    def test_job_phase_is_busy_uses_execution_phases_only(self) -> None:
        job_dir = self.scrapeflow / "jobs" / "abc123"
        self.assertTrue(job_phase_is_busy(job_dir, "executing_media"))
        self.assertFalse(job_phase_is_busy(job_dir, "queued"))
        self.assertFalse(job_phase_is_busy(job_dir, "replenishing"))

    def test_held_slot_lock_blocks_and_probe_is_readonly(self) -> None:
        lock_dir = self.scrapeflow / ".replenishment-search-locks"
        lock_dir.mkdir()
        lock_path = lock_dir / "slot-0.lock"
        lock_path.write_bytes(b"")
        before = lock_path.stat()
        handle = os.open(lock_path, os.O_RDONLY)
        try:
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
            with self.assertRaises(BusyStateError):
                run_backup(self.root, self.api.url)
        finally:
            fcntl.flock(handle, fcntl.LOCK_UN)
            os.close(handle)
        self.assertEqual(self.snapshot_dirs(), [])
        # The probe must never modify the lock file.
        after = lock_path.stat()
        self.assertEqual((before.st_size, before.st_mtime_ns), (after.st_size, after.st_mtime_ns))

    def test_unreadable_slot_lock_fails_closed(self) -> None:
        lock_dir = self.scrapeflow / ".replenishment-search-locks"
        lock_dir.mkdir()
        lock_path = lock_dir / "slot-0.lock"
        lock_path.write_bytes(b"")
        real_open = os.open

        def fail_known_lock(path: object, flags: int, *args: object, **kwargs: object) -> int:
            if Path(path) == lock_path:
                raise PermissionError("unreadable lock")
            return real_open(path, flags, *args, **kwargs)

        with mock.patch("scripts.backup_host_state.os.open", side_effect=fail_known_lock):
            self.assertEqual(held_slot_locks(lock_dir), ["slot-0.lock"])

    def test_write_atomic_bytes_replaces_durably(self) -> None:
        path = self.backup / ".journal.json"
        write_atomic_bytes(path, b'{"a": 1}')
        self.assertEqual(path.read_bytes(), b'{"a": 1}')
        self.assertEqual(os.stat(path).st_mode & 0o777, 0o600)

    # ------------------------------------------------------------- check mode

    def test_check_mode_is_readonly(self) -> None:
        control = self.control_path.read_bytes()
        evidence = run(["--check", "--state-root", str(self.root),
                        "--backup-root", str(self.backup), "--api-url", self.api.url])
        self.assertTrue(evidence["check"])
        self.assertFalse(evidence["busy"])
        self.assertTrue(evidence["would_pause"])
        self.assertTrue(evidence["control_consistent"])
        self.assertNotIn("scheduler_paused", evidence["control"])
        self.assertEqual(self.control_path.read_bytes(), control)
        self.assertFalse(self.backup.exists())
        self.assertFalse(any(
            method == "POST" for method, _path, _body in self.api.state.requests
        ))
        self.assertEqual(self.api.state.requests[0][:2], ("GET", "/api/control"))

    def test_check_mode_reports_executing_job(self) -> None:
        self.api.state.jobs = [{"id": "busy-check", "phase": "executing_media"}]
        evidence = run(["--check", "--state-root", str(self.root),
                        "--backup-root", str(self.backup), "--api-url", self.api.url])
        self.assertTrue(evidence["busy"])
        self.assertIn("job_executing:busy-check:executing_media", evidence["reasons"])

    def test_check_mode_reports_non_boolean_pause_as_inconsistent(self) -> None:
        self.api.state.paused = 1
        evidence = run(["--check", "--state-root", str(self.root),
                        "--backup-root", str(self.backup), "--api-url", self.api.url])
        self.assertFalse(evidence["control_consistent"])
        self.assertIn("strict boolean", evidence["control_error"])

    # ------------------------------------------------------------- full runs

    def test_full_backup_pauses_snapshots_and_restores(self) -> None:
        original = self.control_path.read_bytes()
        evidence = run_backup(self.root, self.api.url)

        self.assertEqual(evidence["mode"], "paused_by_backup")
        self.assertEqual(evidence["recovery"], {"recovered": False})
        self.assertEqual(evidence["restore"]["method"], "api_resume")
        self.assertTrue(evidence["restore"]["restored"])
        snapshots = self.snapshot_dirs()
        self.assertEqual(len(snapshots), 1)
        snapshot = snapshots[0]
        self.assertTrue((snapshot / "manifest.json").is_file())
        self.assertTrue((snapshot / "alist-data.tar.gz").is_file())
        self.assertTrue((snapshot / "scrapeflow-data.tar.gz").is_file())
        manifest = json.loads((snapshot / "manifest.json").read_text(encoding="utf-8"))
        self.assertTrue(manifest["paused_during_backup"])
        self.assertEqual(manifest["coordination"]["mode"], "paused_by_backup")
        self.assertTrue(manifest["coordination"]["restored"])
        self.assertEqual(manifest["coordination"]["restore_method"], "api_resume")
        self.assertIn("sha256", manifest["archives"]["alist-data.tar.gz"])

        posts = [body for method, path, body in self.api.state.requests
                 if method == "POST" and path == "/api/control/pause"]
        self.assertEqual(len(posts), 1)
        self.assertTrue(posts[0]["confirm"])
        self.assertTrue(posts[0]["reason"].startswith(BACKUP_PAUSE_REASON_PREFIX))
        self.assertIn(
            ("POST", "/api/control/resume", {"confirm": True}),
            self.api.state.requests,
        )
        # The original pause state is restored: unpaused.  The API refreshes
        # ``updated_at`` on resume, so the bytes legitimately differ.
        restored = json.loads(self.control_path.read_text(encoding="utf-8"))
        self.assertIs(restored["paused"], False)
        self.assertIsNone(restored["reason"])
        self.assertNotEqual(self.control_path.read_bytes(), original)
        self.assertFalse((self.backup / PAUSE_JOURNAL_NAME).exists())
        self.assertIn("waited_seconds", evidence["quiescence"])

    def test_backup_archive_preserves_file_and_empty_directory_modes(self) -> None:
        sample = self.scrapeflow / "sample.txt"
        sample.chmod(0o640)
        empty = self.scrapeflow / "jobs" / "empty-mode-evidence"
        empty.mkdir()
        empty.chmod(0o710)

        evidence = run_backup(self.root, self.api.url)

        archive_path = Path(evidence["snapshot"]) / "scrapeflow-data.tar.gz"
        with tarfile.open(archive_path, "r:gz") as archive:
            members = {member.name: member for member in archive.getmembers()}
        self.assertEqual(
            members["scrapeflow-data/sample.txt"].mode & 0o777, 0o640,
        )
        self.assertEqual(
            members[
                "scrapeflow-data/jobs/empty-mode-evidence"
            ].mode & 0o777,
            0o710,
        )

    def test_already_paused_never_touches_control(self) -> None:
        control = control_bytes(paused=True, reason="operator-maintenance")
        self.control_path.write_bytes(control)
        self.api.state.paused = True
        self.api.state.reason = "operator-maintenance"

        evidence = run_backup(self.root, self.api.url)

        self.assertEqual(evidence["mode"], "already_paused")
        self.assertNotIn("restore", evidence)
        self.assertFalse(any(
            method == "POST" for method, _path, _body in self.api.state.requests
        ))
        self.assertEqual(self.control_path.read_bytes(), control)
        self.assertEqual(len(self.snapshot_dirs()), 1)

    def test_inconsistent_api_gate_refused_when_file_paused(self) -> None:
        self.control_path.write_bytes(control_bytes(paused=True, reason="maintenance"))
        self.api.state.paused = False  # API process does not agree with the file
        with self.assertRaisesRegex(RuntimeError, "global control disagree"):
            run_backup(self.root, self.api.url)
        self.assertEqual(self.snapshot_dirs(), [])

    def test_run_rejects_non_boolean_file_pause_before_post(self) -> None:
        document = dict(CONTROL_DOC, paused=1)
        raw = (json.dumps(document) + "\n").encode("utf-8")
        self.control_path.write_bytes(raw)

        with self.assertRaisesRegex(RuntimeError, "strict boolean"):
            run_backup(self.root, self.api.url)

        self.assertEqual(self.control_path.read_bytes(), raw)
        self.assertEqual(self.snapshot_dirs(), [])
        self.assertFalse(any(method == "POST" for method, _path, _body in self.api.state.requests))

    def test_run_rejects_non_boolean_api_pause_before_post(self) -> None:
        original = self.control_path.read_bytes()
        self.api.state.paused = "false"

        with self.assertRaisesRegex(RuntimeError, "strict boolean"):
            run_backup(self.root, self.api.url)

        self.assertEqual(self.control_path.read_bytes(), original)
        self.assertEqual(self.snapshot_dirs(), [])
        self.assertFalse(any(method == "POST" for method, _path, _body in self.api.state.requests))

    def test_pause_response_must_be_strict_true(self) -> None:
        real_pause = ApiClient.pause

        def malformed_pause(client: ApiClient, reason: str) -> dict:
            response = real_pause(client, reason)
            response["paused"] = 1
            return response

        with mock.patch.object(ApiClient, "pause", malformed_pause):
            with self.assertRaisesRegex(RuntimeError, "strict boolean"):
                run_backup(self.root, self.api.url)

        self.assertIs(json.loads(self.control_path.read_text())["paused"], False)
        self.assertEqual(self.snapshot_dirs(), [])
        self.assertFalse((self.backup / PAUSE_JOURNAL_NAME).exists())

    def test_operator_pause_race_is_not_overwritten(self) -> None:
        original = self.control_path.read_bytes()
        with mock.patch(
            "scripts.backup_host_state.ApiClient.control",
            return_value={
                "paused": True,
                "reason": "operator-hold",
                "persistent": True,
            },
        ):
            with self.assertRaisesRegex(RuntimeError, "global control disagree"):
                run_backup(self.root, self.api.url)
        self.assertEqual(self.control_path.read_bytes(), original)
        self.assertFalse((self.backup / PAUSE_JOURNAL_NAME).exists())
        self.assertFalse(any(
            method == "POST" and path == "/api/control/pause"
            for method, path, _body in self.api.state.requests
        ))

    # -------------------------------------------------------- busy fail-closed

    def test_unreadable_disk_job_state_fails_closed_and_restores(self) -> None:
        job_dir = self.scrapeflow / "jobs" / "damaged"
        job_dir.mkdir()
        (job_dir / "job.json").write_text("{", encoding="utf-8")
        control = self.control_path.read_bytes()
        with self.assertRaises(BusyStateError):
            run_backup(self.root, self.api.url)
        self.assertIs(json.loads(self.control_path.read_text(encoding="utf-8"))["paused"], False)
        self.assertNotEqual(self.control_path.read_bytes(), control)  # updated_at refreshed
        self.assertEqual(self.snapshot_dirs(), [])
        self.assertFalse((self.backup / PAUSE_JOURNAL_NAME).exists())
        self.assertTrue(any(
            method == "POST" and path == "/api/control/pause"
            for method, path, _body in self.api.state.requests
        ))
        self.assertTrue(any(
            method == "POST" and path == "/api/control/resume"
            for method, path, _body in self.api.state.requests
        ))

    def test_backup_never_calls_retired_global_endpoints(self) -> None:
        run_backup(self.root, self.api.url)
        requested_paths = {path for _method, path, _body in self.api.state.requests}
        self.assertNotIn("/api/convergence", requested_paths)
        self.assertNotIn("/api/replenishment/sweep", requested_paths)

    def test_disk_execution_job_blocks_backup(self) -> None:
        job_dir = self.scrapeflow / "jobs" / "abc123"
        job_dir.mkdir()
        (job_dir / "job.json").write_text(json.dumps({"phase": "executing_media"}),
                                          encoding="utf-8")
        control = self.control_path.read_bytes()
        with self.assertRaisesRegex(BusyStateError, "executing_media"):
            run_backup(self.root, self.api.url)
        self.assertIs(json.loads(self.control_path.read_text(encoding="utf-8"))["paused"], False)
        self.assertNotEqual(self.control_path.read_bytes(), control)
        self.assertEqual(self.snapshot_dirs(), [])

    def test_live_api_job_phase_blocks_backup(self) -> None:
        self.api.state.jobs = [{"id": "feed12", "phase": "extracting_archives"}]
        with self.assertRaisesRegex(BusyStateError, "extracting_archives"):
            run_backup(self.root, self.api.url)

    def test_paused_replenishing_checkpoint_does_not_starve_backup(self) -> None:
        job_dir = self.scrapeflow / "jobs" / "idle-retry"
        job_dir.mkdir()
        (job_dir / "job.json").write_text(
            json.dumps({
                "phase": "replenishing",
                "plan": {
                    "maintenance_restart": {
                        "status": "parked",
                        "reason": "persistent_global_pause_api_shutdown",
                    },
                },
            }),
            encoding="utf-8",
        )
        (job_dir / "replenishment-acquisition.json").write_text(
            json.dumps({"status": "downloading", "selection_sha256": "durable"}),
            encoding="utf-8",
        )
        self.api.state.jobs = [{"id": "idle-retry", "phase": "replenishing"}]
        evidence = run_backup(self.root, self.api.url)
        self.assertIn("snapshot", evidence)

    def test_free_slot_lock_files_do_not_block(self) -> None:
        lock_dir = self.scrapeflow / ".replenishment-search-locks"
        lock_dir.mkdir()
        (lock_dir / "slot-0.lock").write_bytes(b"")
        (lock_dir / "slot-1.lock").write_bytes(b"")
        evidence = run_backup(self.root, self.api.url)
        self.assertIn("snapshot", evidence)

    def test_retired_convergence_state_is_not_a_backup_gate(self) -> None:
        (self.scrapeflow / "convergence-runtime.json").write_text(
            json.dumps({"running": True}), encoding="utf-8",
        )
        evidence = run_backup(self.root, self.api.url)
        self.assertIn("snapshot", evidence)

    def test_quiescence_aborts_when_pause_lifted_while_waiting(self) -> None:
        calls = {"n": 0}
        case = self

        def flaky_control(_client: object) -> dict:
            calls["n"] += 1
            if calls["n"] <= 3:
                return {"paused": False, "reason": None, "persistent": True}
            if calls["n"] == 4:
                durable = json.loads(case.control_path.read_text(encoding="utf-8"))
                return {
                    "paused": durable["paused"], "reason": durable["reason"],
                    "persistent": True,
                }
            if calls["n"] == 5:
                case.api.state.paused = False
                case.api.state.reason = None
                case.control_path.write_bytes(control_bytes())
            return {"paused": False, "reason": None, "persistent": True}

        with mock.patch("scripts.backup_host_state.evaluate_gate") as gate, \
                mock.patch("scripts.backup_host_state.ApiClient.control", flaky_control):
            gate.return_value = {"busy": True, "reasons": ["fake_busy"], "checked_at": "x"}
            with self.assertRaisesRegex(RuntimeError, "pause was lifted"):
                run(["--state-root", str(self.root),
                     "--backup-root", str(self.backup),
                     "--api-url", self.api.url,
                     "--quiesce-timeout", "5", "--quiesce-poll", "1"])
        self.assertEqual(self.snapshot_dirs(), [])
        self.assertFalse((self.backup / PAUSE_JOURNAL_NAME).exists())

    def test_quiescence_checks_pause_when_gate_is_already_clear(self) -> None:
        calls = {"n": 0}
        case = self

        def lift_before_clear_return(_client: object) -> dict:
            calls["n"] += 1
            if calls["n"] <= 3:
                return {"paused": False, "reason": None, "persistent": True}
            if calls["n"] == 4:
                durable = json.loads(case.control_path.read_text(encoding="utf-8"))
                return {
                    "paused": durable["paused"], "reason": durable["reason"],
                    "persistent": True,
                }
            if calls["n"] == 5:
                case.api.state.paused = False
                case.api.state.reason = None
                case.control_path.write_bytes(control_bytes())
            return {"paused": False, "reason": None, "persistent": True}

        with mock.patch("scripts.backup_host_state.evaluate_gate") as gate, \
                mock.patch("scripts.backup_host_state.ApiClient.control", lift_before_clear_return):
            gate.return_value = {"busy": False, "reasons": [], "checked_at": "x"}
            with self.assertRaisesRegex(RuntimeError, "pause was lifted"):
                run_backup(self.root, self.api.url)
        self.assertEqual(self.snapshot_dirs(), [])
        self.assertFalse((self.backup / PAUSE_JOURNAL_NAME).exists())

    def test_api_down_fails_closed_without_touching_state(self) -> None:
        control = self.control_path.read_bytes()
        with self.assertRaises(ApiError):
            run_backup(self.root, closed_port_url())
        self.assertEqual(self.control_path.read_bytes(), control)
        self.assertFalse(self.backup.exists())

    # ------------------------------------------------------ crash self-healing

    def _write_journal(self, *, reason: str, original: bytes,
                       paused_document_sha: str | None = None) -> dict:
        journal = {
            "schema_version": 1,
            "snapshot_name": "2026-08-01T033000+0800",
            "stage": "paused",
            "reason": reason,
            "original_control_sha256": sha256_bytes(original),
            "original_control_b64": base64.b64encode(original).decode("ascii"),
            "original_control": json.loads(original.decode("utf-8")),
            "api_url": self.api.url,
            "created_at": "2026-08-01T03:30:00+00:00",
        }
        if paused_document_sha is not None:
            journal["paused_document_sha256"] = paused_document_sha
        self.backup.mkdir(parents=True)
        (self.backup / PAUSE_JOURNAL_NAME).write_text(
            json.dumps(journal, ensure_ascii=False, indent=2) + "\n", encoding="utf-8",
        )
        return journal

    def test_stale_journal_restored_then_backup_proceeds(self) -> None:
        reason = "host-state-backup:2026-08-01T033000+0800"
        stale = control_bytes(paused=True, reason=reason)
        self.control_path.write_bytes(stale)
        self.api.state.paused = True
        self.api.state.reason = reason
        self._write_journal(reason=reason, original=control_bytes(),
                            paused_document_sha=sha256_bytes(stale))

        evidence = run_backup(self.root, self.api.url)

        self.assertEqual(evidence["recovery"]["action"], "restored")
        self.assertEqual(evidence["recovery"]["method"], "api_resume")
        self.assertIn("snapshot", evidence)
        self.assertFalse((self.backup / PAUSE_JOURNAL_NAME).exists())
        self.assertIs(json.loads(self.control_path.read_text(encoding="utf-8"))["paused"], False)

    def test_restore_refuses_api_file_disagreement_before_resume(self) -> None:
        original = control_bytes()
        reason = "host-state-backup:2026-08-01T033000+0800"
        stale = control_bytes(paused=True, reason=reason)
        self.control_path.write_bytes(stale)
        self.api.state.paused = False
        self.api.state.reason = reason
        journal = self._write_journal(
            reason=reason, original=original,
            paused_document_sha=sha256_bytes(stale),
        )
        journal_path = self.backup / PAUSE_JOURNAL_NAME

        with self.assertRaisesRegex(RuntimeError, "disagree before pause restoration"):
            restore_pause(ApiClient(self.api.url), self.control_path, journal_path, journal)

        self.assertTrue(journal_path.exists())
        self.assertFalse(any(
            method == "POST" and path == "/api/control/resume"
            for method, path, _body in self.api.state.requests
        ))

    def test_resume_rejects_non_boolean_pause_response_and_keeps_journal(self) -> None:
        original = control_bytes()
        reason = "host-state-backup:2026-08-01T033000+0800"
        stale = control_bytes(paused=True, reason=reason)
        self.control_path.write_bytes(stale)
        self.api.state.paused = True
        self.api.state.reason = reason
        journal = self._write_journal(
            reason=reason, original=original,
            paused_document_sha=sha256_bytes(stale),
        )
        journal_path = self.backup / PAUSE_JOURNAL_NAME
        real_resume = ApiClient.resume

        def malformed_resume(client: ApiClient) -> dict:
            response = real_resume(client)
            response["paused"] = 0
            return response

        with mock.patch.object(ApiClient, "resume", malformed_resume):
            with self.assertRaisesRegex(RuntimeError, "strict boolean"):
                restore_pause(ApiClient(self.api.url), self.control_path, journal_path, journal)

        self.assertTrue(journal_path.exists())

    def test_stale_journal_operator_pause_not_clobbered(self) -> None:
        original = control_bytes()
        operator = control_bytes(paused=True, reason="operator-hold")
        self.control_path.write_bytes(operator)
        self.api.state.paused = True
        self.api.state.reason = "operator-hold"
        self._write_journal(reason="host-state-backup:2026-08-01T033000+0800",
                            original=original)

        evidence = run_backup(self.root, self.api.url)

        self.assertEqual(evidence["recovery"]["method"], "operator_state_preserved")
        self.assertFalse(any(
            method == "POST" and path == "/api/control/resume"
            for method, path, _body in self.api.state.requests
        ))
        self.assertEqual(self.control_path.read_bytes(), operator)
        self.assertFalse((self.backup / PAUSE_JOURNAL_NAME).exists())
        self.assertIn("snapshot", evidence)

    def test_stale_journal_api_down_direct_restore(self) -> None:
        original = control_bytes()
        reason = "host-state-backup:2026-08-01T033000+0800"
        stale = control_bytes(paused=True, reason=reason)
        self.control_path.write_bytes(stale)
        journal = self._write_journal(reason=reason, original=original,
                                      paused_document_sha=sha256_bytes(stale))
        journal_path = self.backup / PAUSE_JOURNAL_NAME

        outcome = restore_pause(
            ApiClient(closed_port_url()), self.control_path, journal_path, journal,
        )
        self.assertEqual(outcome["method"], "direct_file")
        self.assertTrue(outcome["restored"])
        self.assertEqual(self.control_path.read_bytes(), original)
        self.assertFalse(journal_path.exists())

    def test_stale_journal_api_down_operator_reason_not_restored(self) -> None:
        original = control_bytes()
        journal_reason = "host-state-backup:2026-08-01T033000+0800"
        stale = control_bytes(paused=True, reason=journal_reason)
        self.control_path.write_bytes(stale)
        # The durable document was rewritten by an operator after our crash,
        # so the journal's paused-document fingerprint no longer matches.
        operator = control_bytes(paused=True, reason="operator-hold")
        self.control_path.write_bytes(operator)
        journal = self._write_journal(reason=journal_reason, original=original,
                                      paused_document_sha=sha256_bytes(stale))
        journal_path = self.backup / PAUSE_JOURNAL_NAME

        outcome = restore_pause(
            ApiClient(closed_port_url()), self.control_path, journal_path, journal,
        )
        self.assertEqual(outcome["method"], "operator_state_preserved")
        self.assertTrue(outcome["restored"])
        self.assertEqual(self.control_path.read_bytes(), operator)
        self.assertFalse(journal_path.exists())

    def test_api_down_unreadable_control_is_not_overwritten(self) -> None:
        original = control_bytes()
        reason = "host-state-backup:2026-08-01T033000+0800"
        stale = control_bytes(paused=True, reason=reason)
        self.control_path.write_bytes(stale)
        journal = self._write_journal(
            reason=reason,
            original=original,
            paused_document_sha=sha256_bytes(stale),
        )
        journal_path = self.backup / PAUSE_JOURNAL_NAME
        with mock.patch("scripts.backup_host_state.read_json", side_effect=OSError("unreadable")):
            with self.assertRaisesRegex(RuntimeError, "unreadable"):
                restore_pause(
                    ApiClient(closed_port_url()), self.control_path, journal_path, journal,
                )
        self.assertEqual(self.control_path.read_bytes(), stale)
        self.assertTrue(journal_path.exists())

    # ---------------------------------------------------- snapshot consistency

    def test_tree_change_during_copy_fails_closed(self) -> None:
        control = self.control_path.read_bytes()
        from scripts.backup_host_state import copy_stable_tree

        def sneaky_copy(source: Path, destination: Path) -> int:
            (source / "sneaky-new-file.txt").write_text("appeared mid-copy",
                                                        encoding="utf-8")
            return copy_stable_tree(source, destination)

        with mock.patch("scripts.backup_host_state.copy_stable_tree",
                        side_effect=sneaky_copy):
            with self.assertRaisesRegex(RuntimeError, "changed during backup"):
                run_backup(self.root, self.api.url)
        self.assertIs(json.loads(self.control_path.read_text(encoding="utf-8"))["paused"], False)
        self.assertNotEqual(self.control_path.read_bytes(), control)
        self.assertEqual(self.snapshot_dirs(), [])

    def test_pause_lifted_during_copy_aborts_without_snapshot(self) -> None:
        from scripts.backup_host_state import copy_stable_tree

        def copy_then_resume(source: Path, destination: Path) -> int:
            copied = copy_stable_tree(source, destination)
            self.api.state.paused = False
            self.api.state.reason = None
            self.control_path.write_bytes(control_bytes())
            return copied

        with mock.patch(
            "scripts.backup_host_state.copy_stable_tree", side_effect=copy_then_resume,
        ):
            with self.assertRaisesRegex(RuntimeError, "global pause is not active"):
                run_backup(self.root, self.api.url)

        self.assertEqual(self.snapshot_dirs(), [])
        self.assertFalse((self.backup / PAUSE_JOURNAL_NAME).exists())

    def test_retention_prunes_older_snapshots(self) -> None:
        self.backup.mkdir(parents=True)
        old = [
            "2026-07-28T033000+0800", "2026-07-29T033000+0800", "2026-07-30T033000+0800",
        ]
        for name in old:
            (self.backup / name).mkdir()
        evidence = run_backup(self.root, self.api.url, extra=["--retention", "3"])
        self.assertEqual(evidence["removed"], ["2026-07-28T033000+0800"])
        remaining = sorted(path.name for path in self.backup.iterdir()
                           if path.is_dir() and SNAPSHOT_NAME_RE.fullmatch(path.name))
        self.assertEqual(remaining,
                         ["2026-07-29T033000+0800", "2026-07-30T033000+0800",
                          evidence["snapshot"].split("/")[-1]])


if __name__ == "__main__":
    unittest.main()
