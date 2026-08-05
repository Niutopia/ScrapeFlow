"""Regression coverage for paused API maintenance during replenishment."""

from __future__ import annotations

import importlib.util
import json
import signal
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest import mock


SERVER_PATH = Path(__file__).resolve().parents[1] / "server.py"
spec = importlib.util.spec_from_file_location(
    "scrapeflow_maintenance_restart_server", SERVER_PATH,
)
assert spec and spec.loader
server = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = server
spec.loader.exec_module(server)


class FakeProcess:
    def __init__(self) -> None:
        self.running = True
        self.signals: list[int] = []
        self.terminated = False
        self.killed = False

    def poll(self) -> int | None:
        return None if self.running else 130

    def send_signal(self, value: int) -> None:
        self.signals.append(value)
        self.running = False

    def terminate(self) -> None:
        self.terminated = True
        self.running = False

    def kill(self) -> None:
        self.killed = True
        self.running = False

    def wait(self, timeout: float | None = None) -> int:
        if self.running:
            raise subprocess.TimeoutExpired("fixture", timeout)
        return 130


class MaintenanceRestartSafetyTests(unittest.TestCase):
    def setUp(self) -> None:
        self.previous_root = server.JOBS_ROOT
        self.previous_provider = server.Job.root_provider
        self.previous_jobs = server.JOBS
        self.previous_control = server.GLOBAL_CONTROL
        self.previous_scheduler = server.SCHEDULER
        self.previous_shutdown = server.SHUTDOWN_EVENT.is_set()
        server.SHUTDOWN_EVENT.clear()
        self.schedulers: list[object] = []
        self.temporary = tempfile.TemporaryDirectory()
        root = Path(self.temporary.name)
        server.JOBS_ROOT = root / "jobs"
        server.JOBS_ROOT.mkdir()
        server.Job.root_provider = staticmethod(lambda: server.JOBS_ROOT)
        server.JOBS = {}
        server.GLOBAL_CONTROL = server.PersistentGlobalControl(
            root / "global-control.json",
        )
        server.GLOBAL_CONTROL.set_paused(True, reason="maintenance_acceptance")
        scheduler = server.FifoScheduler(
            analysis_workers=1, execution_workers=1,
            pause_reader=lambda: server.GLOBAL_CONTROL.paused,
        )
        server.SCHEDULER = scheduler
        self.schedulers.append(scheduler)

    def tearDown(self) -> None:
        for scheduler in self.schedulers:
            scheduler.stop(timeout=0.2)
        server.JOBS = self.previous_jobs
        server.JOBS_ROOT = self.previous_root
        server.Job.root_provider = staticmethod(self.previous_provider)
        server.GLOBAL_CONTROL = self.previous_control
        server.SCHEDULER = self.previous_scheduler
        if self.previous_shutdown:
            server.SHUTDOWN_EVENT.set()
        else:
            server.SHUTDOWN_EVENT.clear()
        self.temporary.cleanup()

    def _job(self, *, phase: str = "replenishing"):
        process = FakeProcess()
        job = server.Job(
            "a" * 12,
            "/quark/影视/番剧/Maintenance Example",
            "/quark/影视/番剧",
            "tv",
            False,
            True,
            phase=phase,
            visibility="internal",
            plan_summary={
                "replenishment": {"status": "searching", "round": 4},
            },
            progress={
                "stage": "replenishment_acquire", "percent": 96.0,
            },
            replenishment_round=3,
            process=process,
        )
        job.directory.mkdir()
        server.JOBS[job.id] = job
        server.persist_job(job)
        return job, process

    def test_paused_restart_queues_replenishment_and_preserves_checkpoint_until_resume(self):
        job, process = self._job()
        checkpoint = job.directory / "replenishment-acquisition.json"
        checkpoint.write_bytes(
            b'{"selection_sha256":"durable","status":"downloading"}\n'
        )
        checkpoint_before = checkpoint.read_bytes()

        server.shutdown_running_jobs(timeout=0.05)

        self.assertEqual(process.signals, [signal.SIGINT])
        self.assertFalse(process.terminated)
        self.assertFalse(job.cancel_requested)
        self.assertTrue(job.maintenance_stop_requested)
        self.assertEqual(job.phase, "replenishing")
        self.assertIsNone(job.error)
        self.assertEqual(
            job.plan_summary["maintenance_restart"]["status"], "parked",
        )
        self.assertEqual(job.progress["stage"], "replenishment_maintenance_parked")
        self.assertNotEqual(
            job.plan_summary["replenishment"].get("status"),
            "cancelled_after_media_commit",
        )
        self.assertEqual(checkpoint.read_bytes(), checkpoint_before)
        durable = json.loads(job.state_path.read_text(encoding="utf-8"))
        self.assertEqual(durable["phase"], "replenishing")
        self.assertEqual(
            durable["plan"]["maintenance_restart"]["status"], "parked",
        )

        # Simulate the next API process: process-local stop state disappears,
        # while the durable coordinator and adapter checkpoint are restored.
        server.JOBS = {}
        server.restore_jobs()
        restored = server.JOBS[job.id]
        self.assertFalse(restored.maintenance_stop_requested)
        self.assertEqual(restored.phase, "replenishing")
        self.assertEqual(checkpoint.read_bytes(), checkpoint_before)

        calls: list[str] = []
        dispatched = threading.Event()

        def observe(current) -> None:
            calls.append(current.id)
            self.assertEqual(checkpoint.read_bytes(), checkpoint_before)
            dispatched.set()

        with mock.patch.object(
            server, "finalize_media_replenishment", side_effect=observe,
        ):
            server.resume_jobs()
            self.assertEqual(server.SCHEDULER.pending("analysis"), [job.id])
            server.SCHEDULER.start(server.run_scheduled)
            self.assertTrue(server.GLOBAL_CONTROL.paused)
            self.assertEqual(server.SCHEDULER.pending("analysis"), [job.id])
            self.assertFalse(dispatched.wait(0.1))

            server.set_global_pause(False)
            self.assertTrue(dispatched.wait(1.0))
            self.assertEqual(calls, [job.id])
            self.assertEqual(checkpoint.read_bytes(), checkpoint_before)
            server.set_global_pause(False)
            time.sleep(0.05)
            self.assertEqual(calls, [job.id])

    def test_non_replenishment_shutdown_keeps_existing_cancel_semantics(self):
        job, process = self._job(phase="executing_media")

        server.shutdown_running_jobs(timeout=0.05)

        self.assertEqual(process.signals, [signal.SIGINT])
        self.assertTrue(job.cancel_requested)
        self.assertFalse(job.maintenance_stop_requested)
        self.assertEqual(job.phase, "cancelling")
        self.assertEqual(job.error, "服务正在停止，等待引擎安全退出。")

    def test_paused_shutdown_parks_active_worker_between_subprocesses(self):
        job, process = self._job()
        job.process = None
        process.running = False
        server.persist_job(job)
        with mock.patch.object(
            server.SCHEDULER, "active_jobs", return_value=[job],
        ):
            server.shutdown_running_jobs(timeout=0.05)

        self.assertTrue(job.maintenance_stop_requested)
        self.assertFalse(job.cancel_requested)
        self.assertEqual(job.phase, "replenishing")
        self.assertEqual(job.progress["stage"], "replenishment_maintenance_parked")
        self.assertEqual(
            job.plan_summary["maintenance_restart"]["status"], "parked",
        )
        durable = json.loads(job.state_path.read_text(encoding="utf-8"))
        self.assertEqual(durable["phase"], "replenishing")
        self.assertEqual(
            durable["plan"]["maintenance_restart"]["status"], "parked",
        )

    def test_paused_shutdown_leaves_pending_replenishment_resumable(self):
        job, process = self._job()
        job.process = None
        process.running = False
        server.persist_job(job)
        server.start_thread(server.finalize_media_replenishment, job)
        self.assertEqual(server.SCHEDULER.pending("analysis"), [job.id])
        self.assertEqual(server.SCHEDULER.active_jobs(), [])

        server.shutdown_running_jobs(timeout=0.05)

        self.assertFalse(job.maintenance_stop_requested)
        self.assertFalse(job.cancel_requested)
        self.assertEqual(job.phase, "replenishing")
        durable = json.loads(job.state_path.read_text(encoding="utf-8"))
        self.assertEqual(durable["phase"], "replenishing")
        self.assertNotIn("maintenance_restart", durable["plan"])

        server.JOBS = {}
        server.restore_jobs()
        restored = server.JOBS[job.id]
        server.resume_jobs()

        self.assertEqual(restored.phase, "replenishing")
        self.assertEqual(server.SCHEDULER.pending("analysis"), [job.id])

    def test_adapter_maintenance_exception_cannot_complete_coordinator(self):
        job, _process = self._job()
        job.process = None
        server.persist_job(job)
        # This case exercises an explicit maintenance-stop exception inside an
        # already-open worker.  The separate stage-boundary tests cover a
        # worker waiting while global dispatch remains paused.
        server.GLOBAL_CONTROL.set_paused(False)
        server.SCHEDULER.wake()
        checkpoint = job.directory / "replenishment-candidates.json"
        checkpoint.write_bytes(b'{"candidates":[{"locator":"kept"}]}\n')

        def interrupted(_job, **_kwargs):
            _job.maintenance_stop_requested = True
            raise server.ReplenishmentMaintenanceStop

        with mock.patch.object(
            server, "scrape_first_gate_evidence",
            return_value={
                "ready": True, "status": "ready", "blocker_count": 0,
                "blockers": [], "snapshot_sha256": "a" * 64,
            },
        ), mock.patch.object(
            server, "audit_current_job_titles",
            return_value={
                "episode_gaps": [{"kind": "missing_episode", "label": "S01E03"}],
                "summary": {
                    "episode_gap_count": 1,
                    "confirmed_subtitle_gap_count": 0,
                    "pending_subtitle_verification_count": 0,
                    "complete": False,
                },
            },
        ), mock.patch.object(
            server, "prepare_post_scrape_replenishment", side_effect=interrupted,
        ), mock.patch.object(
            server, "_schedule_replenishment_retry",
        ) as retry:
            server.finalize_media_replenishment(job)

        retry.assert_not_called()
        self.assertEqual(job.phase, "replenishing")
        self.assertFalse(job.cancel_requested)
        self.assertEqual(
            job.plan_summary["maintenance_restart"]["status"], "parked",
        )
        self.assertEqual(job.progress["stage"], "replenishment_maintenance_parked")
        self.assertNotEqual(
            job.plan_summary["replenishment"].get("status"),
            "cancelled_after_media_commit",
        )
        self.assertEqual(
            checkpoint.read_bytes(),
            b'{"candidates":[{"locator":"kept"}]}\n',
        )


if __name__ == "__main__":
    unittest.main()
