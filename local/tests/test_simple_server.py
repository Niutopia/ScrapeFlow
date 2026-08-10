"""HTTP coverage for the current automatic ScrapeFlow public API."""

from __future__ import annotations

import json
import os
import tempfile
import threading
import unittest
import urllib.error
import urllib.request
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

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
            "/library/待刮削": [
                {"name": "Example", "is_dir": True},
                {"name": "AutomaticOnly", "is_dir": True},
                {"name": "PlainFile", "is_dir": False, "size": 3},
            ],
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
        headers: dict[str, str] | None = None,
    ) -> tuple[int, dict[str, object]]:
        body = None if payload is None else json.dumps(payload).encode("utf-8")
        request_headers = {"Content-Type": "application/json"} if body is not None else {}
        request_headers.update(headers or {})
        request = urllib.request.Request(
            self.base + path,
            data=body,
            method=method,
            headers=request_headers,
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
        self.assertEqual(health["build_version"], "target-shelf-rc1")
        self.assertIn("build_commit", health)
        self.assertIn("build_time", health)
        self.assertEqual(health["provider_capabilities"]["quark_share"]["status"], "ready")
        self.assertEqual(health["provider_capabilities"]["magnet"]["status"], "ready")
        self.assertEqual(set(health["provider_capabilities"]), {"quark_share", "magnet"})

        status, control = self.request("GET", "/api/control")
        self.assertEqual(status, 200)
        self.assertTrue(control["paused"])

        created = self.create_job()
        job_id = created["id"]
        self.assertEqual(created["phase"], "awaiting_target_shelf")
        self.assertEqual(created["source"], "/library/待刮削/Example")
        self.assertIsNone(created["target_shelf"])
        self.assertIsNone(created["target_root"])
        self.assertEqual(created["allowed_target_shelves"], ["movie", "anime", "us_tv"])
        self.assertNotIn("identity_override", created["plan"])
        persisted = self.runner.get_job(job_id)
        self.assertEqual(persisted.request, {"source_path": "/library/待刮削/Example"})
        status, listing = self.request("GET", "/api/jobs")
        self.assertEqual(status, 200)
        self.assertEqual([row["id"] for row in listing["jobs"]], [job_id])
        status, detail = self.request("GET", f"/api/jobs/{job_id}")
        self.assertEqual(status, 200)
        self.assertEqual(detail["job"]["id"], job_id)

    def test_real_root_lane_gates_fail_closed_until_explicitly_enabled(self) -> None:
        with tempfile.TemporaryDirectory() as directory, patch.dict(
            os.environ,
            {
                "SCRAPEFLOW_START_PAUSED": "1",
                "SCRAPEFLOW_INTAKE_MONITOR": "0",
                "SCRAPEFLOW_AUTOMATIC_AUDIT": "1",
            },
            clear=False,
        ):
            state_root = Path(directory)
            runner = SimpleEngineRunner(state_root, alist=self.remote, tmdb=object(), validate=False, library_root="/quark/影视")
            with patch.object(SimpleApplication, "_start_startup_thread"):
                application = SimpleApplication(
                    state_root=state_root,
                    remote_root="/quark/影视",
                    remote=self.remote,
                    engine_runner=runner,
                    enforce_engine_roots=True,
                )
            try:
                gates = application.health()["lane_gates"]
                self.assertFalse(gates["provider_auto_repair_enabled"])
                self.assertFalse(gates["audit_auto_repair_enabled"])
                self.assertEqual(application._scheduled_timers, {})  # noqa: SLF001
            finally:
                application.close()

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
            scheduled = self.application._scan_inbound_once()  # noqa: SLF001 - intake boundary
        finally:
            self.application._queue_automatic_job = original_queue  # type: ignore[method-assign]
        self.assertEqual(len(scheduled), 1)
        self.assertEqual(queued, [])
        created = self.runner.get_job(scheduled[0])
        self.assertEqual(created.request["source_path"], "/library/待刮削/Real Release")
        self.assertEqual(created.phase, "awaiting_target_shelf")

    def test_start_persists_one_allowed_shelf_without_running_while_paused(self) -> None:
        self.remote.entries["/library/待刮削"] = [
            {"name": "Example", "is_dir": True},
        ]
        created = self.create_job()
        queued: list[str] = []
        original_queue = self.application._queue_automatic_job  # noqa: SLF001 - start gate assertion
        self.application._queue_automatic_job = queued.append  # type: ignore[method-assign]
        try:
            status, payload = self.request(
                "POST", f"/api/jobs/{created['id']}/start", {"target_shelf": "anime"},
            )
        finally:
            self.application._queue_automatic_job = original_queue  # type: ignore[method-assign]
        self.assertEqual(status, 200)
        started = payload["job"]
        self.assertEqual(started["phase"], "queued")
        self.assertEqual(started["target_shelf"], "anime")
        self.assertEqual(started["target_root"], "/library/番剧")
        self.assertEqual(queued, [])
        selected_at = started["selected_at"]
        status, repeated = self.request(
            "POST", f"/api/jobs/{created['id']}/start", {"target_shelf": "anime"},
        )
        self.assertEqual(status, 200)
        self.assertEqual(repeated["job"]["selected_at"], selected_at)
        status, conflict = self.request(
            "POST", f"/api/jobs/{created['id']}/start", {"target_shelf": "movie"},
        )
        self.assertEqual(status, 409)
        self.assertIn("不能更改", conflict["error"])

    def test_start_rejects_invalid_request_or_missing_source_without_losing_waiting_job(self) -> None:
        status, rejected = self.request("POST", "/api/jobs", {"path": "/library/待刮削/Missing"})
        self.assertEqual(status, 400)
        self.assertIn("来源目录不存在", rejected["error"])
        self.assertEqual(self.runner.list_jobs(), [])

        created = self.create_job()
        status, invalid = self.request(
            "POST", f"/api/jobs/{created['id']}/start", {"target_shelf": "/library/电影"},
        )
        self.assertEqual(status, 400)
        self.assertIn("target_shelf", invalid["error"])
        # The intake evidence is deliberately rechecked at start time: a
        # directory can disappear after registration but before a shelf is
        # selected.  That must leave the waiting record untouched.
        self.remote.entries["/library/待刮削"] = []
        status, missing = self.request(
            "POST", f"/api/jobs/{created['id']}/start", {"target_shelf": "movie"},
        )
        self.assertEqual(status, 409)
        self.assertIn("不存在", missing["error"])
        self.assertEqual(self.runner.get_job(created["id"]).phase, "awaiting_target_shelf")

    def test_submission_requires_a_direct_existing_source_directory(self) -> None:
        for source in (
            "/library/待刮削/Missing",
            "/library/待刮削/PlainFile",
            "/library/待刮削/Example/nested",
        ):
            with self.subTest(source=source):
                status, payload = self.request("POST", "/api/jobs", {"path": source})
                self.assertEqual(status, 400)
                self.assertIn("error", payload)
                self.assertEqual(self.runner.list_jobs(), [])

    def test_retry_reopens_a_terminal_automatic_failure(self) -> None:
        pending = self.runner.create_pending_job("/library/待刮削/Example")
        job = self.runner.start_automatic_job(pending.id, target_shelf="movie")
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
        self.assertEqual(reopened.target_shelf, "movie")
        self.assertEqual(reopened.target_root, "/library/电影")
        self.assertIsNotNone(reopened.selected_at)

    def test_legacy_automatic_retry_is_rejected_without_reopening_it(self) -> None:
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
        self.assertEqual(status, 400)
        self.assertIn("缺少已确认的目标货架", payload["error"])
        unchanged = self.runner.get_job(job.id)
        self.assertEqual(unchanged.phase, "failed_identity")
        self.assertIsNone(unchanged.target_shelf)

        legacy_without_flag = replace(
            unchanged,
            summary={"mode": "auto", "automatic_terminal": True},
        )
        atomic_write_json(
            self.runner.jobs_root / f"{job.id}.json",
            legacy_without_flag.as_dict(),
            allow_nan=False,
        )
        status, payload = self.request("POST", f"/api/jobs/{job.id}/retry", {})
        self.assertEqual(status, 400)
        self.assertIn("缺少已确认的目标货架", payload["error"])
        self.assertEqual(self.runner.get_job(job.id).phase, "failed_identity")

    def test_successful_terminal_cleanup_releases_cleanup_fence(self) -> None:
        job = self.runner.create_automatic_job("/library/待刮削/Fence")
        terminal = replace(
            job,
            phase="failed",
            summary={**job.summary, "automatic_terminal": True},
            error="terminal fixture",
        )
        atomic_write_json(self.runner.jobs_root / f"{job.id}.json", terminal.as_dict(), allow_nan=False)
        result = self.application.cleanup_public_job(job.id)
        self.assertTrue(result["removed"])
        self.assertNotIn(job.id, self.application._cleanup_fences)  # noqa: SLF001

    def test_failed_cleanup_retry_dispatches_only_the_finalizer(self) -> None:
        pending = self.runner.create_pending_job("/library/待刮削/Example")
        selected = self.runner.start_automatic_job(pending.id, target_shelf="movie")
        failed = replace(
            selected,
            phase="failed_cleanup",
            plan={"mode": "movie"},
            summary={
                **selected.summary,
                "automatic": True,
                "automatic_terminal": True,
                "cleanup_only_retry": True,
            },
            error="cleanup failed",
        )
        atomic_write_json(
            self.runner.jobs_root / f"{selected.id}.json",
            failed.as_dict(),
            allow_nan=False,
        )
        completed = replace(failed, phase="executed", error=None)
        with patch.object(
            self.runner,
            "finalize_automatic_lifecycle",
            return_value=completed,
        ) as finalizer, patch.object(
            self.application,
            "_queue_automatic_job",
        ) as ordinary_queue, patch.object(
            self.application,
            "_queue_provider_job",
        ) as provider_queue:
            status, payload = self.request("POST", f"/api/jobs/{selected.id}/retry", {})
        self.assertEqual(status, 200)
        self.assertEqual(payload["job"]["engine_phase"], "executed")
        finalizer.assert_called_once_with(selected.id)
        ordinary_queue.assert_not_called()
        provider_queue.assert_not_called()

    def test_cleanup_only_retry_cannot_fall_through_to_formal_writer(self) -> None:
        pending = self.runner.create_pending_job("/library/待刮削/Example")
        selected = self.runner.start_automatic_job(pending.id, target_shelf="movie")
        retry_wait = replace(
            selected,
            phase="retry_wait",
            plan={"mode": "movie"},
            summary={
                **selected.summary,
                "automatic": True,
                "automatic_terminal": False,
                "cleanup_only_retry": True,
            },
            error="cleanup still pending",
        )
        atomic_write_json(
            self.runner.jobs_root / f"{selected.id}.json",
            retry_wait.as_dict(),
            allow_nan=False,
        )
        with patch.object(self.application, "control", return_value={"paused": False}), patch.object(
            self.runner,
            "recover_job",
        ) as recover, patch.object(
            self.runner,
            "execute_automatic",
        ) as execute:
            self.application._run_automatic_job(selected.id)  # noqa: SLF001
        recover.assert_not_called()
        execute.assert_not_called()
        self.assertEqual(self.runner.get_job(selected.id).phase, "retry_wait")

    def test_retry_identity_correction_rejects_client_controlled_metadata_and_parent(self) -> None:
        """Web retry exposes only the bounded identity/password contract."""
        pending = self.runner.create_pending_job("/library/待刮削/Example")
        job = self.runner.start_automatic_job(pending.id, target_shelf="movie")
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

        for forbidden in ("title", "year", "target_parent"):
            status, payload = self.request(
                "POST",
                f"/api/jobs/{job.id}/retry",
                {"tmdb_id": 123, "media_type": "movie", forbidden: "client override"},
            )
            self.assertEqual(status, 400)
            self.assertIn("不支持", payload["error"])
        unchanged = self.runner.get_job(job.id)
        self.assertEqual(unchanged.phase, "failed_identity")
        self.assertNotIn("manual_identity", unchanged.summary)

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

    def test_cancel_stops_a_queued_selected_job_without_touching_its_source(self) -> None:
        pending = self.runner.create_pending_job("/library/待刮削/Example")
        queued = self.runner.start_automatic_job(pending.id, target_shelf="anime")
        self.assertEqual(queued.phase, "queued")

        status, payload = self.request(
            "POST", f"/api/jobs/{queued.id}/cancel", {"reason": "operator stop"},
        )
        self.assertEqual(status, 200)
        self.assertEqual(payload["job"]["phase"], "cancelled")
        cancelled = self.runner.get_job(queued.id)
        self.assertEqual(cancelled.phase, "cancelled")
        self.assertEqual(cancelled.target_shelf, "anime")
        self.assertEqual(cancelled.target_root, "/library/番剧")
        self.assertIn(
            {"name": "Example", "is_dir": True},
            self.remote.entries["/library/待刮削"],
        )

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

    def test_post_requires_json_and_rejects_cross_site_or_non_loopback_hosts(self) -> None:
        attempts = (
            (
                "text/plain",
                {"Content-Type": "text/plain"},
                400,
            ),
            (
                "cross-site origin",
                {"Origin": "https://evil.example", "Sec-Fetch-Site": "cross-site"},
                403,
            ),
            (
                "cross-site fetch metadata",
                {"Sec-Fetch-Site": "Cross-Site"},
                403,
            ),
            (
                "non-loopback host",
                {"Host": "evil.example"},
                403,
            ),
        )
        for label, headers, expected_status in attempts:
            with self.subTest(label=label):
                status, _ = self.request("POST", "/api/control/resume", {}, headers)
                self.assertEqual(status, expected_status)
                self.assertTrue(self.application.control()["paused"])

    def test_post_allows_same_origin_host_forwarded_by_the_local_proxy(self) -> None:
        # The local Node proxy passes the browser's Host through to the API,
        # just as nginx does with ``$http_host`` in Docker.  Its listen port is
        # intentionally different from the private API port.
        proxy_host = "127.0.0.1:3010"
        status, resumed = self.request(
            "POST",
            "/api/control/resume",
            {},
            {
                "Host": proxy_host,
                "Origin": f"http://{proxy_host}",
                "Sec-Fetch-Site": "same-origin",
                "Content-Type": "application/json; charset=utf-8",
            },
        )

        self.assertEqual(status, 200)
        self.assertFalse(resumed["paused"])
        self.application.set_paused(True, "test")

    def test_api_errors_and_public_jobs_redact_runtime_secrets(self) -> None:
        alist_password = "alist-password-not-public"
        tmdb_key = "tmdb-key-not-public"
        provider_token = "provider-token-not-public"
        archive_password = "archive-password-not-public"
        unconfigured_api_key = "unconfigured-api-key-not-public"
        with patch.dict(
            "os.environ",
            {
                "ALIST_PASSWORD": alist_password,
                "TMDB_API_KEY": tmdb_key,
                "SCRAPEFLOW_REPLENISHMENT_TOKEN": provider_token,
            },
            clear=False,
        ):
            job = self.runner.create_automatic_job("/library/待刮削/Redaction")
            failed = replace(
                job,
                phase="failed",
                plan={
                    "nested": {
                        "token": provider_token,
                        "archive_password": archive_password,
                        "message": f"password={alist_password}; api_key={tmdb_key}",
                    },
                },
                error=f"AList password={alist_password}; token={provider_token}; api_key={tmdb_key}",
            )
            atomic_write_json(
                self.runner.jobs_root / f"{job.id}.json",
                failed.as_dict(),
                allow_nan=False,
            )

            # The scheduler's durable retry boundary must redact the complete
            # root document as well, even when an exception string contains
            # credentials that arrived from a provider/client.
            self.application._record_automatic_retry(  # noqa: SLF001 - persistence boundary
                job.id,
                RuntimeError(
                    f"password={alist_password}; token={provider_token}; api_key={tmdb_key}"
                ),
                stage="provider",
            )
            persisted_root = (
                self.runner.jobs_root / f"{job.id}.json"
            ).read_text(encoding="utf-8")
            for secret in (alist_password, tmdb_key, provider_token, archive_password):
                self.assertNotIn(secret, persisted_root)
            self.assertIn("<redacted>", persisted_root)

            serialized_direct_job = json.dumps(
                self.application.public_engine_job(failed), ensure_ascii=False,
            )
            for secret in (alist_password, tmdb_key, provider_token, archive_password):
                self.assertNotIn(secret, serialized_direct_job)
            status, public = self.request("GET", f"/api/jobs/{job.id}")
            self.assertEqual(status, 200)
            serialized_job = json.dumps(public, ensure_ascii=False)
            for secret in (alist_password, tmdb_key, provider_token, archive_password):
                self.assertNotIn(secret, serialized_job)
            self.assertIn("<redacted>", serialized_job)

            with patch.object(
                self.application,
                "create_task",
                side_effect=RuntimeError(
                    "password=" + alist_password
                    + "; token=" + provider_token
                    + "; api key=" + unconfigured_api_key,
                ),
            ):
                status, error_payload = self.request(
                    "POST", "/api/jobs", {"path": "/library/待刮削/NoLeak"},
                )
            self.assertEqual(status, 500)
            serialized_error = json.dumps(error_payload, ensure_ascii=False)
            for secret in (alist_password, tmdb_key, provider_token, unconfigured_api_key):
                self.assertNotIn(secret, serialized_error)
            self.assertIn("<redacted>", serialized_error)

    def test_subtitle_installing_is_a_visible_active_provider_phase(self) -> None:
        job = self.runner.create_automatic_job("/library/待刮削/SubtitleInstall")
        active = replace(
            job,
            phase="executed",
            plan={"scan_report": {"resource_gaps": [{"kind": "missing_subtitle"}]}},
            summary={
                **job.summary,
                "replenishment": {
                    "status": "subtitle_installing",
                    "message": "正在绑定字幕",
                },
            },
        )
        atomic_write_json(
            self.runner.jobs_root / f"{job.id}.json",
            active.as_dict(),
            allow_nan=False,
        )

        status, payload = self.request("GET", f"/api/jobs/{job.id}")

        self.assertEqual(status, 200)
        self.assertEqual(payload["job"]["phase"], "subtitle_installing")
        self.assertEqual(payload["job"]["progress"]["stage"], "subtitle_installing")
        self.assertEqual(payload["job"]["progress"]["percent"], 94)
        self.assertEqual(payload["job"]["progress"]["message"], "系统正在安装精确绑定字幕")
        status, health = self.request("GET", "/api/health")
        self.assertEqual(status, 200)
        self.assertEqual(health["operations"]["provider_active"], 1)

    def test_disabled_lanes_settle_verified_executed_root_without_queueing(self) -> None:
        self.application.set_paused(False, "test")
        pending = self.runner.create_pending_job("/library/待刮削/Example")
        selected = self.runner.start_automatic_job(pending.id, target_shelf="movie")
        lifecycle = {
            "formal_write": {"status": "verified", "updated_at": "fixture"},
            "cleanup": {"status": "pending", "updated_at": "fixture"},
        }
        executed = replace(
            selected,
            phase="executed",
            plan={"scan_report": {"resource_gaps": []}},
            summary={**selected.summary, "lifecycle": lifecycle},
        )
        atomic_write_json(self.runner.jobs_root / f"{selected.id}.json", executed.as_dict(), allow_nan=False)
        with patch.object(self.application, "_audit_auto_repair_enabled", return_value=False), patch.object(
            self.application, "_provider_auto_repair_enabled", return_value=False,
        ), patch.object(
            self.runner, "finalize_automatic_lifecycle", return_value=executed,
        ) as finalizer:
            handled = self.application._settle_disabled_automatic_lifecycle(executed)  # noqa: SLF001
        self.assertTrue(handled)
        finalizer.assert_called_once_with(selected.id)
        persisted = self.runner.get_job(selected.id)
        self.assertEqual(persisted.summary["lifecycle"]["audit"]["status"], "deferred")
        self.assertEqual(persisted.summary["lifecycle"]["provider"]["status"], "deferred")
        self.assertTrue(persisted.summary["lifecycle"]["cleanup_ready"])

    def test_completed_with_gaps_is_terminal_attention_projection(self) -> None:
        pending = self.runner.create_pending_job("/library/待刮削/Example")
        selected = self.runner.start_automatic_job(pending.id, target_shelf="movie")
        gap = {"id": "missing-episode", "kind": "missing_media", "label": "S01E02"}
        lifecycle = {
            "formal_write": {"status": "verified"},
            "audit": {"status": "trusted"},
            "provider": {"status": "deferred", "reason": "disabled"},
            "cleanup": {"status": "completed"},
            "cleanup_ready": True,
        }
        executed = replace(
            selected,
            phase="executed",
            plan={"scan_report": {"resource_gaps": [gap]}},
            summary={**selected.summary, "lifecycle": lifecycle},
        )
        public = self.application.public_engine_job(executed)
        self.assertEqual(public["phase"], "completed_with_gaps")
        self.assertEqual(public["engine_phase"], "executed")
        self.assertEqual(public["progress"]["percent"], 100)
        self.assertEqual(public["progress"]["completed"], 1)
        self.assertEqual(public["plan"]["resource_gaps"], [gap])
        self.assertEqual(public["plan"]["resource_gap_count"], 1)
        self.assertIn("1 项资源缺口", public["progress"]["message"])

    def test_public_gap_projection_keeps_active_and_failed_provider_states(self) -> None:
        pending = self.runner.create_pending_job("/library/待刮削/Example")
        selected = self.runner.start_automatic_job(pending.id, target_shelf="movie")
        gap = {"id": "missing-episode", "kind": "missing_media"}
        for status, terminal, expected in (
            ("provider_searching", False, "provider_searching"),
            ("failed", True, "failed_provider"),
        ):
            with self.subTest(status=status):
                job = replace(
                    selected,
                    phase="executed",
                    plan={"scan_report": {"resource_gaps": [gap]}},
                    summary={
                        **selected.summary,
                        "replenishment": {"status": status, "terminal": terminal},
                        "lifecycle": {
                            "formal_write": {"status": "verified"},
                            "audit": {"status": "trusted"},
                            "provider": {"status": "pending" if not terminal else "terminal"},
                            "cleanup": {"status": "completed"},
                        },
                    },
                )
                self.assertEqual(self.application.public_engine_job(job)["phase"], expected)

    def test_unknown_audit_never_projects_as_completed_with_gaps(self) -> None:
        pending = self.runner.create_pending_job("/library/待刮削/Example")
        selected = self.runner.start_automatic_job(pending.id, target_shelf="movie")
        job = replace(
            selected,
            phase="executed",
            plan={"scan_report": {"resource_gaps": [{"kind": "missing_media"}]}},
            summary={
                **selected.summary,
                "audit": {"status": "unknown", "message": "证据不足"},
                "lifecycle": {
                    "formal_write": {"status": "verified"},
                    "audit": {"status": "unknown"},
                    "provider": {"status": "deferred"},
                    "cleanup": {"status": "completed"},
                },
            },
        )
        self.assertEqual(self.application.public_engine_job(job)["phase"], "failed_verification")


if __name__ == "__main__":
    unittest.main()
