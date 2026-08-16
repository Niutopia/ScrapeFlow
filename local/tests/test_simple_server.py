"""HTTP coverage for the current automatic ScrapeFlow public API."""

from __future__ import annotations

from concurrent.futures import Future
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
from local.simple_server import ApplicationError, SimpleApplication, make_server
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

    def test_root_serves_same_origin_dashboard_with_shelf_controls(self) -> None:
        with urllib.request.urlopen(self.base + "/", timeout=3) as response:
            body = response.read().decode("utf-8")
            self.assertEqual(response.status, 200)
            self.assertEqual(response.headers.get_content_type(), "text/html")
            self.assertIn("ScrapeFlow", body)
            self.assertIn("这个任务应整理到哪里？", body)
            self.assertIn("/api/jobs/${encodeURIComponent(b.dataset.job)}/start", body)
            self.assertIn("target_shelf", body)
            self.assertIn("电影", body)
            self.assertIn("番剧", body)
            self.assertIn("美剧", body)
            self.assertIn("frame-ancestors 'none'", response.headers["Content-Security-Policy"])
            # P2: the S-step intake creation panel (source + shelf in one action).
            self.assertIn("创建任务", body)
            self.assertIn("这个来源整理到哪里？", body)
            self.assertIn("/api/root-jobs", body)
            self.assertIn("data-source", body)
            self.assertIn("/api/intake", body)
            # P8: the minimal uncertain-unit confirmation panel (U node).
            self.assertIn("需要确认", body)
            self.assertIn("识别不确定，请确认正确身份：", body)
            self.assertIn("/confirm", body)
            self.assertIn("data-confirm", body)
            self.assertIn("/work-units", body)

    def create_job(self, source: str = "/library/待刮削/Example") -> dict[str, object]:
        status, payload = self.request("POST", "/api/jobs", {"path": source})
        self.assertEqual(status, 201)
        return payload["job"]

    def new_work_waiting(self, source: str = "/library/待刮削/Example"):
        """Build a legacy downstream fixture for post-selection API tests."""
        pending = self.runner.create_pending_job(source)
        summary = dict(pending.summary)
        summary.pop("reconciliation", None)
        summary.pop("reconciliation_outcome", None)
        waiting = replace(
            pending,
            phase="awaiting_target_shelf",
            summary={
                **summary,
                "automatic_stage": "awaiting_target_shelf",
            },
        )
        atomic_write_json(
            self.runner.jobs_root / f"{pending.id}.json",
            waiting.as_dict(),
            allow_nan=False,
        )
        return waiting

    def test_health_control_and_path_submission_expose_one_automatic_root_job(self) -> None:
        with patch.dict(
            os.environ,
            {
                "SCRAPEFLOW_QUARK_HELPER_URL": "",
                "SCRAPEFLOW_QUARK_HELPER_TOKEN": "",
            },
            clear=False,
        ):
            status, health = self.request("GET", "/api/health")
        self.assertEqual(status, 200)
        self.assertEqual(health["mode"], "automatic")
        self.assertTrue(health["connected"])
        self.assertTrue(health["engine_configured"])
        self.assertEqual(health["build_version"], "p0-p10-migration")
        self.assertIn("build_commit", health)
        self.assertIn("build_time", health)
        self.assertEqual(health["provider_capabilities"]["quark_share"]["status"], "ready")
        self.assertEqual(health["provider_capabilities"]["quark_magnet"]["status"], "ready")
        self.assertEqual(health["provider_capabilities"]["magnet"]["status"], "ready")
        self.assertEqual(health["helper_readiness"]["quark"]["status"], "not_configured")
        self.assertFalse(health["helper_readiness"]["quark"]["configured"])
        self.assertEqual(
            set(health["provider_capabilities"]),
            {"quark_share", "quark_magnet", "magnet"},
        )

        status, control = self.request("GET", "/api/control")
        self.assertEqual(status, 200)
        self.assertTrue(control["paused"])

        created = self.create_job()
        job_id = created["id"]
        self.assertEqual(created["phase"], "awaiting_target_shelf")
        self.assertEqual(created["source"], "/library/待刮削/Example")
        self.assertIsNone(created["target_shelf"])
        self.assertIsNone(created["target_root"])
        # Retirement batch #2: creation no longer writes the legacy
        # blocked_by_target_shelf reconciliation marker; the awaiting gate
        # reads the phase, and reconciliation appears only after D runs.
        self.assertIsNone(created["reconciliation"])
        # The awaiting task is offered the closed shelf enum (S-step): shelf
        # selection no longer waits for a legacy reconciliation verdict.
        self.assertEqual(
            created["allowed_target_shelves"], ["movie", "anime", "us_tv"],
        )
        self.assertNotIn("identity_override", created["plan"])
        persisted = self.runner.get_job(job_id)
        self.assertEqual(persisted.request, {"source_path": "/library/待刮削/Example"})
        status, listing = self.request("GET", "/api/jobs")
        self.assertEqual(status, 200)
        self.assertEqual([row["id"] for row in listing["jobs"]], [job_id])
        status, detail = self.request("GET", f"/api/jobs/{job_id}")
        self.assertEqual(status, 200)
        self.assertEqual(detail["job"]["id"], job_id)

    def test_provider_worker_configuration_is_strictly_single_worker(self) -> None:
        for value in ("2", "0", "not-a-number"):
            with self.subTest(configured=value), patch.dict(
                os.environ,
                {"SCRAPEFLOW_PROVIDER_WORKERS": value},
                clear=False,
            ):
                health = self.application.health()
                self.assertFalse(health["ok"])
                configuration = health["lane_gates"]["provider_workers"]
                self.assertEqual(configuration["expected"], 1)
                self.assertFalse(configuration["valid"])
                with self.assertRaisesRegex(ApplicationError, "必须严格为 1"):
                    self.application._provider_pool()  # noqa: SLF001 - runtime gate

        with patch.dict(os.environ, {"SCRAPEFLOW_PROVIDER_WORKERS": "1"}, clear=False):
            pool = self.application._provider_pool()  # noqa: SLF001 - runtime gate
        self.assertEqual(pool._max_workers, 1)  # noqa: SLF001 - executor contract

    def test_provider_does_not_adopt_an_arbitrary_application_media_root(self) -> None:
        with self.assertRaisesRegex(ApplicationError, "自动补源只允许"):
            self.application._provider_staging_root()  # noqa: SLF001 - safety boundary

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

    def test_discovery_registers_the_intake_catalog_without_creating_jobs(self) -> None:
        status, payload = self.request(
            "POST", "/api/jobs", {"path": "/library/待刮削/ScrapeFlow-E2E-Fight-Club-1999"},
        )
        self.assertEqual(status, 400)
        self.assertIn("E2E", payload["error"])

        self.remote.entries["/library/待刮削"] = [
            {"name": "ScrapeFlow-E2E-Keep-Out", "is_dir": True},
            {"name": "Real Release", "is_dir": True},
        ]
        self.remote.entries["/library/待刮削/Real Release"] = [
            {"name": "Season 01", "is_dir": True},
            {"name": "poster.jpg", "is_dir": False, "size": 3},
            {"name": "notes.txt", "is_dir": False, "size": 3},
        ]
        queued: list[str] = []
        original_queue = self.application._queue_automatic_job  # noqa: SLF001 - intake boundary
        self.application._queue_automatic_job = queued.append  # type: ignore[method-assign]
        try:
            scheduled = self.application._scan_inbound_once()  # noqa: SLF001 - intake boundary
        finally:
            self.application._queue_automatic_job = original_queue  # type: ignore[method-assign]
        # Discovery is a passive observation (A step): it returns the newly
        # registered source paths, fills the catalog with real child/file
        # counts, and must not create an EngineJob or schedule anything
        # before the user creates a RootJob.
        self.assertEqual(scheduled, ["/library/待刮削/Real Release"])
        self.assertEqual(queued, [])
        self.assertEqual(self.runner.list_jobs(), [])
        status, intake = self.request("GET", "/api/intake")
        self.assertEqual(status, 200)
        sources = {row["canonical_path"]: row for row in intake["sources"]}
        self.assertNotIn("/library/待刮削/ScrapeFlow-E2E-Keep-Out", sources)
        self.assertIn("/library/待刮削/Real Release", sources)
        release = sources["/library/待刮削/Real Release"]
        self.assertEqual(release["child_count"], 1)
        self.assertEqual(release["file_count"], 2)

    def test_root_job_creation_selects_shelf_in_one_action(self) -> None:
        self.remote.entries["/library/待刮削"] = [{"name": "Example", "is_dir": True}]
        status, payload = self.request(
            "POST", "/api/root-jobs",
            {"path": "/library/待刮削/Example", "target_shelf": "anime"},
        )
        self.assertEqual(status, 201)
        job = payload["job"]
        self.assertEqual(job["target_shelf"], "anime")
        self.assertEqual(job["target_root"], "/library/番剧")
        self.assertEqual(job["phase"], "queued")
        # Exactly one durable RootJob, bound to the intake catalog (S step).
        self.assertEqual(len(self.runner.list_jobs()), 1)
        status, intake = self.request("GET", "/api/intake")
        self.assertEqual(status, 200)
        sources = {row["canonical_path"]: row for row in intake["sources"]}
        example = sources["/library/待刮削/Example"]
        self.assertEqual(example["root_task_id"], job["id"])
        self.assertEqual(example["root_job_target_shelf"], "anime")

    def test_root_job_creation_is_idempotent_and_rejects_shelf_change(self) -> None:
        self.remote.entries["/library/待刮削"] = [{"name": "Example", "is_dir": True}]
        status, first = self.request(
            "POST", "/api/root-jobs",
            {"path": "/library/待刮削/Example", "target_shelf": "anime"},
        )
        self.assertEqual(status, 201)
        status, repeated = self.request(
            "POST", "/api/root-jobs",
            {"path": "/library/待刮削/Example", "target_shelf": "anime"},
        )
        self.assertEqual(status, 201)
        self.assertEqual(repeated["job"]["id"], first["job"]["id"])
        self.assertEqual(len(self.runner.list_jobs()), 1)
        status, conflict = self.request(
            "POST", "/api/root-jobs",
            {"path": "/library/待刮削/Example", "target_shelf": "movie"},
        )
        self.assertEqual(status, 409)
        self.assertIn("不能更改", conflict["error"])

    def test_root_job_creation_rejects_bad_shelf_and_bad_path(self) -> None:
        self.remote.entries["/library/待刮削"] = [{"name": "Example", "is_dir": True}]
        status, bad_shelf = self.request(
            "POST", "/api/root-jobs",
            {"path": "/library/待刮削/Example", "target_shelf": "/library/电影"},
        )
        self.assertEqual(status, 400)
        self.assertIn("target_shelf", bad_shelf["error"])
        status, missing = self.request(
            "POST", "/api/root-jobs",
            {"path": "/library/待刮削/Missing", "target_shelf": "movie"},
        )
        self.assertEqual(status, 400)
        self.assertIn("来源目录不存在", missing["error"])
        self.assertEqual(self.runner.list_jobs(), [])

    def test_work_units_view_and_durable_confirm_flow(self) -> None:
        from engine.scrapeflow.root_boundaries import analyze_root_boundaries

        self.remote.entries["/library/待刮削"] = [{"name": "Example", "is_dir": True}]
        self.remote.entries["/library/待刮削/Example"] = [
            {"name": "S01E01.mkv", "is_dir": False, "size": 3},
        ]
        status, created = self.request(
            "POST", "/api/root-jobs",
            {"path": "/library/待刮削/Example", "target_shelf": "anime"},
        )
        self.assertEqual(status, 201)
        job_id = created["job"]["id"]
        analyze_root_boundaries(
            self.remote, "/library/待刮削/Example",
            root_task_id=job_id, state_root=self.state_root,
        )
        status, view = self.request("GET", f"/api/jobs/{job_id}/work-units")
        self.assertEqual(status, 200)
        self.assertEqual(view["aggregate"]["unit_count"], 1)
        self.assertEqual(view["aggregate"]["in_progress"], 1)
        unit = view["units"][0]
        self.assertEqual(unit["identity_status"], "pending")
        # The only confirmation surface is media_type + tmdb_id (+season).
        confirm_payload = {"media_type": "tv", "tmdb_id": 123}
        status, confirmed = self.request(
            "POST",
            f"/api/jobs/{job_id}/work-units/{unit['work_unit_id']}/confirm",
            confirm_payload,
        )
        self.assertEqual(status, 200)
        self.assertEqual(confirmed["unit"]["identity"]["tmdb_id"], 123)
        self.assertEqual(confirmed["unit"]["identity"]["source"], "operator_override")
        # Idempotent and durable: a second confirm keeps the override.
        status, repeated = self.request(
            "POST",
            f"/api/jobs/{job_id}/work-units/{unit['work_unit_id']}/confirm",
            confirm_payload,
        )
        self.assertEqual(status, 200)
        self.assertEqual(repeated["unit"]["identity"]["tmdb_id"], 123)
        # Bad payloads are rejected without touching the override.
        status, bad = self.request(
            "POST",
            f"/api/jobs/{job_id}/work-units/{unit['work_unit_id']}/confirm",
            {"media_type": "ova", "tmdb_id": 1},
        )
        self.assertEqual(status, 400)
        status, missing = self.request(
            "POST",
            f"/api/jobs/{job_id}/work-units/missing/confirm",
            confirm_payload,
        )
        self.assertEqual(status, 404)
        status, view2 = self.request("GET", f"/api/jobs/{job_id}/work-units")
        self.assertEqual(status, 200)
        self.assertEqual(view2["units"][0]["identity"]["source"], "operator_override")
        self.assertEqual(view2["units"][0]["identity"]["tmdb_id"], 123)

    def test_start_persists_one_allowed_shelf_without_running_while_paused(self) -> None:
        self.remote.entries["/library/待刮削"] = [
            {"name": "Example", "is_dir": True},
        ]
        waiting = self.new_work_waiting()
        queued: list[str] = []
        original_queue = self.application._queue_automatic_job  # noqa: SLF001 - start gate assertion
        self.application._queue_automatic_job = queued.append  # type: ignore[method-assign]
        try:
            status, payload = self.request(
                "POST", f"/api/jobs/{waiting.id}/start", {"target_shelf": "anime"},
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
            "POST", f"/api/jobs/{waiting.id}/start", {"target_shelf": "anime"},
        )
        self.assertEqual(status, 200)
        self.assertEqual(repeated["job"]["selected_at"], selected_at)
        status, conflict = self.request(
            "POST", f"/api/jobs/{waiting.id}/start", {"target_shelf": "movie"},
        )
        self.assertEqual(status, 409)
        self.assertIn("不能更改", conflict["error"])

    def test_start_rejects_invalid_request_or_missing_source_without_losing_waiting_job(self) -> None:
        status, rejected = self.request("POST", "/api/jobs", {"path": "/library/待刮削/Missing"})
        self.assertEqual(status, 400)
        self.assertIn("来源目录不存在", rejected["error"])
        self.assertEqual(self.runner.list_jobs(), [])

        waiting = self.new_work_waiting()
        status, invalid = self.request(
            "POST", f"/api/jobs/{waiting.id}/start", {"target_shelf": "/library/电影"},
        )
        self.assertEqual(status, 400)
        self.assertIn("target_shelf", invalid["error"])
        # The intake evidence is deliberately rechecked at start time: a
        # directory can disappear after registration but before a shelf is
        # selected.  That must leave the waiting record untouched.
        self.remote.entries["/library/待刮削"] = []
        status, missing = self.request(
            "POST", f"/api/jobs/{waiting.id}/start", {"target_shelf": "movie"},
        )
        self.assertEqual(status, 409)
        self.assertIn("不存在", missing["error"])
        self.assertEqual(self.runner.get_job(waiting.id).phase, "awaiting_target_shelf")

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
        pending = self.new_work_waiting()
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

    def test_artifact_repair_is_an_explicit_empty_request_only(self) -> None:
        pending = self.new_work_waiting()
        selected = self.runner.start_automatic_job(pending.id, target_shelf="movie")
        executed = replace(
            selected,
            phase="executed",
            plan={"mode": "movie", "scan_report": {"resource_gaps": []}},
            summary={**selected.summary, "automatic": True},
        )
        atomic_write_json(
            self.runner.jobs_root / f"{selected.id}.json",
            executed.as_dict(),
            allow_nan=False,
        )

        with patch.object(
            self.runner,
            "repair_automatic_artifacts",
            return_value=executed,
        ) as repair:
            status, payload = self.request(
                "POST", f"/api/jobs/{selected.id}/repair-artifacts", {},
            )
        self.assertEqual(status, 200)
        self.assertEqual(payload["job"]["id"], selected.id)
        repair.assert_called_once_with(selected.id)

        status, payload = self.request(
            "POST",
            f"/api/jobs/{selected.id}/repair-artifacts",
            {"automatic": True},
        )
        self.assertEqual(status, 400)
        self.assertIn("空 JSON", payload["error"])

    def test_failed_cleanup_retry_dispatches_only_the_finalizer(self) -> None:
        pending = self.new_work_waiting()
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
        pending = self.new_work_waiting()
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
        pending = self.new_work_waiting()
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

    def test_uncertain_retry_accepts_only_identity_confirmation_and_queues_read_only_reconcile(self) -> None:
        pending = self.runner.create_pending_job("/library/待刮削/Example")
        uncertain = replace(
            pending,
            phase="reconciliation_uncertain",
            summary={
                **pending.summary,
                "reconciliation": {
                    "status": "needs_attention",
                    "outcome": "uncertain",
                    "reason": "ambiguous formal identity",
                },
            },
        )
        atomic_write_json(
            self.runner.jobs_root / f"{pending.id}.json",
            uncertain.as_dict(),
            allow_nan=False,
        )
        with patch.object(self.application, "_queue_automatic_job") as queue:
            status, payload = self.request(
                "POST",
                f"/api/jobs/{pending.id}/retry",
                {"tmdb_id": 77, "media_type": "movie", "season": 1},
            )
        self.assertEqual(status, 200)
        self.assertEqual(payload["job"]["engine_phase"], "reconciling")
        self.assertEqual(
            self.runner.get_job(pending.id).summary[
                "reconciliation_identity_confirmation"
            ]["tmdb_id"],
            77,
        )
        queue.assert_called_once_with(pending.id)

        blocked = replace(
            uncertain,
            updated_at="2099-01-01T00:00:00+00:00",
        )
        atomic_write_json(
            self.runner.jobs_root / f"{pending.id}.json",
            blocked.as_dict(),
            allow_nan=False,
        )
        status, payload = self.request(
            "POST",
            f"/api/jobs/{pending.id}/retry",
            {"tmdb_id": 77, "media_type": "movie", "target_root": "/library/电影"},
        )
        self.assertEqual(status, 400)
        self.assertIn("不支持", payload["error"])

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
        pending = self.new_work_waiting()
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

        with patch.object(self.application, "_apply_audit_gaps") as apply_gaps:
            status, report = self.request("POST", "/api/library-audit/run", {})
        self.assertEqual(status, 200)
        self.assertEqual(report["audit"]["status"], "completed")
        apply_gaps.assert_not_called()
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
        # This fixture exercises lifecycle settlement, not the explicit
        # resume scan. Keep that read-only scan out of this test so its
        # scheduler cannot contend for the fixture runner lock.
        with patch.object(self.application, "_scan_inbound_once", return_value=[]), patch.object(
            self.application, "_start_startup_thread"
        ):
            self.application.set_paused(False, "test")
        pending = self.new_work_waiting()
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

    def test_intake_settled_audit_early_return_clears_queued_barrier(self) -> None:
        self.application._intake_status["last_scan_empty"] = False  # noqa: SLF001
        with patch.object(self.application, "control", return_value={"paused": False}), patch.object(
            self.application, "_audit_auto_repair_enabled", return_value=True,
        ), patch.object(
            self.application, "_intake_is_settled", return_value=False,
        ), patch.object(
            self.application,
            "_schedule_timer",
            side_effect=lambda _lane, _owner, _delay, callback: callback(),
        ):
            self.application._queue_intake_settled_audit()  # noqa: SLF001

        self.assertFalse(self.application._intake_audit_timer_armed)  # noqa: SLF001
        self.assertEqual(
            self.application._intake_status["full_audit_barrier"],  # noqa: SLF001
            "waiting_for_intake",
        )

    def test_provider_submission_requires_successful_full_audit_admission(self) -> None:
        """A scoped gap projection cannot bypass the L→M global gate."""
        with patch.object(self.application, "_provider_auto_repair_enabled", return_value=True), \
             patch.object(
                 self.application,
                 "_provider_worker_configuration",
                 return_value={"valid": True},
             ), patch.object(self.application, "_scan_inbound_once", return_value=[]), \
             patch.object(self.application, "_schedule_timer") as timer, \
             patch.object(self.application, "_start_startup_thread"):
            self.application.set_paused(False, "test")
            self.application._queue_provider_job("not-yet-admitted")  # noqa: SLF001
        timer.assert_not_called()
        self.assertFalse(self.application._provider_submission_admitted())  # noqa: SLF001

    def test_full_audit_success_opens_provider_admission_but_incomplete_report_does_not(self) -> None:
        """Only a complete L report may project gaps into the provider lane."""
        for complete, expected_admitted, expected_barrier in (
            (True, True, "completed"),
            (False, False, "failed"),
        ):
            with self.subTest(complete=complete):
                future: Future[object] = Future()
                future.set_result({
                    "audit": {
                        "status": "completed" if complete else "unknown",
                        "complete": complete,
                        "semantic": {
                            "gaps": [], "unknowns": [], "acquisition_projects": [],
                        },
                    },
                })

                class Pool:
                    def submit(self, *_args: object, **_kwargs: object) -> Future[object]:
                        return future

                self.application._intake_status.update({  # noqa: SLF001
                    "last_scan_empty": True,
                    "full_audit_barrier": "ready",
                })
                with patch.object(self.application, "control", return_value={"paused": False}), \
                     patch.object(self.application, "_audit_auto_repair_enabled", return_value=True), \
                     patch.object(self.application, "_intake_is_settled", return_value=True), \
                     patch.object(self.application, "_audit_pool", return_value=Pool()), \
                     patch.object(self.application, "_apply_audit_gaps") as apply_gaps, \
                     patch.object(
                         self.application,
                         "_schedule_timer",
                         side_effect=lambda _lane, _owner, _delay, callback: callback(),
                     ):
                    self.application._queue_intake_settled_audit()  # noqa: SLF001

                self.assertEqual(
                    self.application._provider_submission_admitted(),  # noqa: SLF001
                    expected_admitted,
                )
                self.assertEqual(
                    self.application._intake_status["full_audit_barrier"],  # noqa: SLF001
                    expected_barrier,
                )
                if complete:
                    apply_gaps.assert_called_once()
                else:
                    apply_gaps.assert_not_called()

    def test_new_intake_closes_previous_provider_admission_epoch(self) -> None:
        self.remote.entries["/library/待刮削"] = [{"name": "New", "is_dir": True}]
        with self.application._automatic_lock:  # noqa: SLF001
            self.application._provider_full_audit_admitted = True  # noqa: SLF001
            self.application._intake_status["full_audit_barrier"] = "completed"  # noqa: SLF001
        with patch.object(self.application, "_queue_automatic_job"), patch.object(
            self.application, "_refresh_intake_settlement",
        ):
            self.application._scan_inbound_once()  # noqa: SLF001
        self.assertFalse(self.application._provider_submission_admitted())  # noqa: SLF001
        self.assertNotEqual(
            self.application._intake_status["full_audit_barrier"],  # noqa: SLF001
            "completed",
        )

    def test_public_duplicate_readback_distinguishes_pending_and_failed_consumption(self) -> None:
        pending = self.runner.create_automatic_job("/library/待刮削/DuplicateProjection")
        reconciliation = {
            "outcome": "duplicate_complete",
            "reason": "formal match",
        }
        for phase, marker, expected_status in (
            ("completed", None, "pending"),
            ("failed_cleanup", {"status": "failed"}, "failed"),
        ):
            with self.subTest(phase=phase):
                summary = {
                    **pending.summary,
                    "reconciliation": reconciliation,
                }
                if marker is not None:
                    summary["duplicate_complete_consumption"] = marker
                job = replace(pending, phase=phase, summary=summary)
                public = self.application.public_engine_job(job)
                self.assertEqual(public["readback"]["status"], expected_status)
                self.assertNotIn("本任务仅消费重复输入", public["readback"]["message"])

    def test_public_existing_gap_registration_projection_distinguishes_held_and_blocked(self) -> None:
        pending = self.runner.create_automatic_job("/library/待刮削/ExistingGapProjection")
        reconciliation = {
            "outcome": "existing_gap",
            "reason": "formal work is missing media",
        }
        held = replace(
            pending,
            phase="completed",
            summary={
                **pending.summary,
                "reconciliation": {**reconciliation, "status": "completed"},
                "existing_gap_registration": {
                    "status": "moved_to_hold",
                    "source": "/library/待刮削/ExistingGapProjection",
                    "target": "/library/ScrapeFlow/归档/job/existing-gap-hold/ExistingGapProjection",
                },
                "source_fate": "moved_to_hold",
            },
        )
        public_held = self.application.public_engine_job(held)
        self.assertEqual(public_held["completion_kind"], "existing_gap_registered")
        self.assertEqual(public_held["readback"]["status"], "source_held")
        self.assertFalse(public_held["readback"]["formal_write"])
        self.assertEqual(public_held["progress"]["completed"], 1)
        self.assertEqual(public_held["progress"]["percent"], 100)

        blocked = replace(
            pending,
            phase="reconciliation_uncertain",
            summary={
                **pending.summary,
                "reconciliation": {**reconciliation, "status": "needs_attention"},
                "existing_gap_registration": {
                    "status": "blocked_nonempty_source",
                    "source": "/library/待刮削/ExistingGapProjection",
                    "target": "/library/ScrapeFlow/归档/job/existing-gap-hold/ExistingGapProjection",
                },
                "source_fate": "retained_needs_attention",
            },
        )
        public_blocked = self.application.public_engine_job(blocked)
        self.assertEqual(public_blocked["completion_kind"], "existing_gap_registered")
        self.assertEqual(public_blocked["phase"], "needs_attention")
        self.assertEqual(public_blocked["readback"]["status"], "registration_blocked")
        self.assertFalse(public_blocked["progress"]["completed"])

    def test_public_audit_owned_root_never_claims_intake_source_consumption(self) -> None:
        root = self.runner.create_audit_owned_root({
            "project_key": "tmdb:tv:42:Show",
            "tmdb_id": 42,
            "title": "Show",
            "target_root": "/library/番剧/Show",
            "gaps": [{
                "id": "S01E02",
                "kind": "missing_episode",
                "label": "Show S01E02",
                "media": {
                    "tmdb_id": 42,
                    "title": "Show",
                    "target_root": "/library/番剧/Show",
                    "media_type": "tv",
                },
                "season": 1,
                "episode": 2,
            }],
            "plan": {
                "mode": "tv",
                "target_root": "/library/番剧/Show",
                "metadata": {
                    "tmdb_id": 42,
                    "title": "Show",
                    "target_root": "/library/番剧/Show",
                    "media_type": "tv",
                },
            },
        })

        completed = replace(
            root,
            phase="completed",
            plan={
                **root.plan,
                "scan_report": {"resource_gaps": []},
            },
            summary={
                **root.summary,
                "resource_gaps": [],
                "replenishment": {"status": "completed", "terminal": True},
            },
        )
        public = self.application.public_engine_job(completed)

        self.assertEqual(public["readback"]["status"], "audit_completed")
        self.assertFalse(public["readback"]["formal_write"])
        self.assertIn("没有待刮削来源", public["readback"]["message"])

    def test_disabled_lanes_never_settle_a_verified_root_with_known_gaps(self) -> None:
        """A disabled provider is not evidence that a missing work is safe to delete."""
        with patch.object(self.application, "_scan_inbound_once", return_value=[]), patch.object(
            self.application, "_start_startup_thread"
        ):
            self.application.set_paused(False, "test")
        pending = self.new_work_waiting()
        selected = self.runner.start_automatic_job(pending.id, target_shelf="movie")
        gap = {
            "id": "missing-media",
            "kind": "missing_media",
            "label": "Movie (2024)",
            "reason": "not present",
        }
        lifecycle = {
            "formal_write": {"status": "verified", "updated_at": "fixture"},
            "cleanup": {"status": "pending", "updated_at": "fixture"},
        }
        executed = replace(
            selected,
            phase="executed",
            plan={"scan_report": {"resource_gaps": [gap]}},
            summary={**selected.summary, "lifecycle": lifecycle},
        )
        atomic_write_json(self.runner.jobs_root / f"{selected.id}.json", executed.as_dict(), allow_nan=False)
        # Simulate the automatic worker holding the pre-audit snapshot while
        # the audit coordinator has already persisted the missing-media row.
        stale_without_gap = replace(
            executed,
            plan={"scan_report": {"resource_gaps": []}},
        )

        with patch.object(self.application, "_audit_auto_repair_enabled", return_value=False), patch.object(
            self.application, "_provider_auto_repair_enabled", return_value=False,
        ), patch.object(self.runner, "finalize_automatic_lifecycle") as finalizer:
            handled = self.application._settle_disabled_automatic_lifecycle(stale_without_gap)  # noqa: SLF001

        self.assertTrue(handled)
        finalizer.assert_not_called()
        persisted = self.runner.get_job(selected.id)
        self.assertNotIn("audit", persisted.summary["lifecycle"])
        self.assertNotIn("provider", persisted.summary["lifecycle"])
        self.assertIsNot(persisted.summary["lifecycle"].get("cleanup_ready"), True)

    def test_completed_with_gaps_is_terminal_attention_projection(self) -> None:
        pending = self.new_work_waiting()
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
        pending = self.new_work_waiting()
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
        pending = self.new_work_waiting()
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
