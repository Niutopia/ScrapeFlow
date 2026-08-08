"""HTTP coverage for the current automatic ScrapeFlow public API."""

from __future__ import annotations

import json
import tempfile
import threading
import unittest
import urllib.error
import urllib.request
from dataclasses import replace
from pathlib import Path

from engine.scrapeflow.serialization import atomic_write_json
from local.simple_server import SimpleApplication, make_server
from local.scrapeflow_api.simple_engine_runner import EngineJob, SimpleEngineRunner


class FakeAList:
    """Small read-only AList double for public API tests."""

    def __init__(self) -> None:
        self.entries: dict[str, list[dict[str, object]]] = {
            "/library": [
                {"name": "待刮削", "is_dir": True},
                {"name": "电影", "is_dir": True},
                {"name": "notes.txt", "is_dir": False, "size": 3},
            ],
            "/library/电影": [],
            "/library/番剧": [],
            "/library/美剧": [],
        }

    def list(self, path: str, refresh: bool = False) -> list[dict[str, object]]:
        del refresh
        return [dict(row) for row in self.entries.get(path, [])]


class SimpleServerAutomaticApiTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.state_root = Path(self.temporary.name) / "state"
        self.remote = FakeAList()
        self.runner = SimpleEngineRunner(
            self.state_root,
            alist=self.remote,
            tmdb=object(),
            validate=False,
            library_root="/library",
        )
        self.application = SimpleApplication(
            state_root=self.state_root,
            remote_root="/library",
            remote=self.remote,
            engine_runner=self.runner,
        )
        # Keep these HTTP tests at the public queue boundary; planning and
        # execution have dedicated Engine tests.
        self.application.set_paused(True, "test")
        self.addCleanup(self.application.close)
        self.server = make_server(self.application, "127.0.0.1", 0)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.addCleanup(self._stop_server)
        self.base = f"http://127.0.0.1:{self.server.server_port}"

    def _stop_server(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)

    def request(
        self,
        method: str,
        path: str,
        payload: dict[str, object] | None = None,
    ) -> tuple[int, dict[str, object]]:
        body = None if payload is None else json.dumps(payload).encode("utf-8")
        request = urllib.request.Request(
            self.base + path,
            data=body,
            method=method,
            headers={"Content-Type": "application/json"} if body is not None else {},
        )
        try:
            with urllib.request.urlopen(request, timeout=3) as response:
                return response.status, json.loads(response.read())
        except urllib.error.HTTPError as exc:
            return exc.code, json.loads(exc.read())

    def create_job(self, source: str = "/library/待刮削/Example") -> dict[str, object]:
        status, payload = self.request("POST", "/api/jobs", {"path": source})
        self.assertEqual(status, 201)
        return payload["job"]

    def test_health_control_and_path_submission_expose_one_automatic_root_job(self) -> None:
        status, health = self.request("GET", "/api/health")
        self.assertEqual(status, 200)
        self.assertEqual(health["mode"], "automatic")
        self.assertTrue(health["connected"])
        self.assertTrue(health["engine_configured"])

        status, control = self.request("GET", "/api/control")
        self.assertEqual(status, 200)
        self.assertTrue(control["paused"])

        created = self.create_job()
        job_id = created["id"]
        self.assertEqual(created["phase"], "queued")
        self.assertEqual(created["source"], "/library/待刮削/Example")
        self.assertNotIn("identity_override", created["plan"])
        persisted = self.runner.get_job(job_id)
        self.assertEqual(persisted.request, {"source_path": "/library/待刮削/Example"})

        status, listing = self.request("GET", "/api/jobs")
        self.assertEqual(status, 200)
        self.assertEqual([row["id"] for row in listing["jobs"]], [job_id])
        status, detail = self.request("GET", f"/api/jobs/{job_id}")
        self.assertEqual(status, 200)
        self.assertEqual(detail["job"]["id"], job_id)

    def test_submission_accepts_only_a_source_path_and_unknown_job_is_not_found(self) -> None:
        status, payload = self.request(
            "POST",
            "/api/jobs",
            {"path": "/library/待刮削/Example", "tmdb_id": 1},
        )
        self.assertEqual(status, 400)
        self.assertIn("path", payload["error"])

        status, _ = self.request("GET", "/api/jobs/does-not-exist")
        self.assertEqual(status, 404)

    def test_production_e2e_sources_are_rejected_before_intake_or_job_creation(self) -> None:
        status, payload = self.request(
            "POST", "/api/jobs", {"path": "/library/待刮削/ScrapeFlow-E2E-Fight-Club-1999"},
        )
        self.assertEqual(status, 400)
        self.assertIn("E2E", payload["error"])

        self.remote.entries["/library/待刮削"] = [
            {"name": "ScrapeFlow-E2E-Keep-Out", "is_dir": True},
            {"name": "Real Release", "is_dir": True},
        ]
        queued: list[str] = []
        original_queue = self.application._queue_automatic_job  # noqa: SLF001 - intake boundary
        self.application._queue_automatic_job = queued.append  # type: ignore[method-assign]
        try:
            self.application.set_paused(False, "test intake filter")
            scheduled = self.application._scan_inbound_once()  # noqa: SLF001 - intake boundary
        finally:
            self.application._queue_automatic_job = original_queue  # type: ignore[method-assign]
            self.application.set_paused(True, "test")
        self.assertEqual(len(scheduled), 1)
        self.assertEqual(queued, scheduled)
        created = self.runner.get_job(scheduled[0])
        self.assertEqual(created.request["source_path"], "/library/待刮削/Real Release")

    def test_retry_reopens_a_terminal_automatic_failure(self) -> None:
        job = self.runner.create_automatic_job("/library/待刮削/Retry")
        failed = replace(
            job,
            phase="failed_identity",
            summary={**job.summary, "automatic_terminal": True, "automatic_attempts": 3},
            error="identity exhausted",
        )
        atomic_write_json(
            self.runner.jobs_root / f"{job.id}.json",
            failed.as_dict(),
            allow_nan=False,
        )

        status, payload = self.request("POST", f"/api/jobs/{job.id}/retry", {})
        self.assertEqual(status, 200)
        self.assertEqual(payload["job"]["phase"], "queued")
        reopened = self.runner.get_job(job.id)
        self.assertEqual(reopened.phase, "queued")
        self.assertFalse(reopened.summary["automatic_terminal"])
        self.assertEqual(reopened.summary["automatic_attempts"], 0)

    def test_cancel_stops_a_planned_root_job(self) -> None:
        job = self.runner.create_automatic_job("/library/待刮削/Cancel")
        planned = replace(
            job,
            phase="planned",
            plan={"mode": "movie", "scan_report": {"resource_gaps": []}},
        )
        atomic_write_json(
            self.runner.jobs_root / f"{job.id}.json",
            planned.as_dict(),
            allow_nan=False,
        )

        status, payload = self.request(
            "POST", f"/api/jobs/{job.id}/cancel", {"reason": "operator stop"},
        )
        self.assertEqual(status, 200)
        self.assertEqual(payload["job"]["phase"], "cancelled")
        self.assertEqual(self.runner.get_job(job.id).phase, "cancelled")

    def test_cancel_closes_terminal_identity_failure_without_remote_delete(self) -> None:
        source = "/library/待刮削/identity-never-matched"
        job = self.runner.create_automatic_job(source)
        failed = replace(
            job,
            phase="failed_identity",
            summary={
                **job.summary,
                "automatic_terminal": True,
                "automatic_attempts": 3,
            },
            error="identity exhausted",
        )
        atomic_write_json(
            self.runner.jobs_root / f"{job.id}.json",
            failed.as_dict(),
            allow_nan=False,
        )

        status, payload = self.request(
            "POST", f"/api/jobs/{job.id}/cancel", {"reason": "operator closed residue"},
        )

        self.assertEqual(status, 200)
        self.assertEqual(payload["job"]["phase"], "cancelled")
        cancelled = self.runner.get_job(job.id)
        self.assertEqual(cancelled.phase, "cancelled")
        self.assertEqual(cancelled.request["source_path"], source)
        self.assertEqual(cancelled.error, "operator closed residue")

    def test_internal_children_are_not_public_jobs(self) -> None:
        root = self.runner.create_automatic_job("/library/待刮削/Root")
        child = EngineJob(
            id="engine-child-test",
            phase="planned",
            created_at=root.created_at,
            updated_at=root.updated_at,
            request={"source_path": "/library/ScrapeFlow/补源/child"},
            plan={},
            summary={"internal_child": True, "root_job_id": root.id},
        )
        atomic_write_json(
            self.runner.jobs_root / f"{child.id}.json",
            child.as_dict(),
            allow_nan=False,
        )

        status, payload = self.request("GET", "/api/jobs")
        self.assertEqual(status, 200)
        self.assertEqual([row["id"] for row in payload["jobs"]], [root.id])
        status, _ = self.request("GET", f"/api/jobs/{child.id}")
        self.assertEqual(status, 404)

    def test_control_browse_and_library_audit_are_current_public_routes(self) -> None:
        status, resumed = self.request("POST", "/api/control/resume", {})
        self.assertEqual(status, 200)
        self.assertFalse(resumed["paused"])
        status, paused = self.request("POST", "/api/control/pause", {"reason": "test"})
        self.assertEqual(status, 200)
        self.assertTrue(paused["paused"])

        status, browse = self.request("GET", "/api/browse?path=/library")
        self.assertEqual(status, 200)
        self.assertEqual([row["name"] for row in browse["directories"]], ["待刮削", "电影"])

        status, report = self.request("POST", "/api/library-audit/run", {})
        self.assertEqual(status, 200)
        self.assertEqual(report["audit"]["status"], "completed")
        status, latest = self.request("GET", "/api/library-audit/latest")
        self.assertEqual(status, 200)
        self.assertEqual(latest["audit"], report["audit"])

    def test_unknown_actions_are_not_public_operations(self) -> None:
        job = self.create_job("/library/待刮削/AutomaticOnly")
        for path, body in (
            (f"/api/jobs/{job['id']}/unsupported-action", {}),
            ("/api/unsupported-action", {}),
        ):
            with self.subTest(path=path):
                status, _ = self.request("POST", path, body)
                self.assertEqual(status, 404)


if __name__ == "__main__":
    unittest.main()
