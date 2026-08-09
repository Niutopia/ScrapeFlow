"""Focused coverage for durable provider child -> public root projection."""

from __future__ import annotations

import os
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

from engine.scrapeflow.serialization import atomic_write_json
from local.simple_server import SimpleApplication
from local.scrapeflow_api.simple_engine_runner import EngineJob, SimpleEngineRunner


class _Remote:
    def list(self, path: str, refresh: bool = False) -> list[dict[str, object]]:
        del path, refresh
        return []


class RootChildProjectionTests(unittest.TestCase):
    def test_root_projection_uses_durable_child_phase_and_terminal_result(self) -> None:
        with tempfile.TemporaryDirectory() as directory, patch.dict(
            os.environ,
            {
                "SCRAPEFLOW_START_PAUSED": "1",
                "SCRAPEFLOW_INTAKE_MONITOR": "0",
                "SCRAPEFLOW_AUTOMATIC_AUDIT": "0",
            },
            clear=False,
        ):
            state_root = Path(directory)
            remote = _Remote()
            runner = SimpleEngineRunner(
                state_root,
                alist=remote,
                tmdb=object(),
                validate=False,
                library_root="/library",
            )
            root = EngineJob(
                id="engine-root-projection",
                phase="executed",
                created_at="2026-08-08T00:00:00Z",
                updated_at="2026-08-08T00:00:00Z",
                request={"source_path": "/library/待刮削/Show"},
                plan={
                    "scan_report": {
                        "resource_gaps": [{"id": "S01E01", "kind": "missing_episode"}],
                    },
                },
                summary={"mode": "tv", "target_root": "/library/番剧/Show"},
            )
            child = EngineJob(
                id="engine-child-projection",
                phase="planned",
                created_at="2026-08-08T00:00:01Z",
                updated_at="2026-08-08T00:00:02Z",
                request={"source_path": "/library/ScrapeFlow/补源/child"},
                plan={},
                summary={"internal_child": True, "root_job_id": root.id},
                error=None,
            )
            atomic_write_json(runner._job_path(root.id), root.as_dict(), allow_nan=False)
            atomic_write_json(runner._job_path(child.id), child.as_dict(), allow_nan=False)

            with patch.object(SimpleApplication, "_start_startup_thread"):
                application = SimpleApplication(
                    state_root=state_root,
                    remote_root="/library",
                    remote=remote,
                    engine_runner=runner,
                    enforce_engine_roots=False,
                )
            try:
                projected = application._sync_replenishment_child(root)
                replenishment = projected.summary["replenishment"]
                self.assertEqual(replenishment["status"], "child_planning")
                self.assertFalse(replenishment["terminal"])
                self.assertEqual(replenishment["child_jobs"][0]["phase"], "planned")
                self.assertFalse(replenishment["child_jobs"][0]["success"])
                self.assertEqual(
                    application.public_engine_job(projected)["phase"], "child_planning",
                )
                unchanged = application._sync_replenishment_child(projected)
                self.assertEqual(unchanged.updated_at, projected.updated_at)

                completed = replace(
                    child,
                    phase="executed",
                    updated_at="2026-08-08T00:00:03Z",
                    error=None,
                    execution={"files": [{"target": "/library/番剧/Show/S01E01.mkv", "size": 1}]},
                )
                atomic_write_json(runner._job_path(child.id), completed.as_dict(), allow_nan=False)
                projected = application._sync_replenishment_child(projected)
                replenishment = projected.summary["replenishment"]
                self.assertEqual(replenishment["status"], "completed")
                self.assertTrue(replenishment["terminal"])
                self.assertTrue(replenishment["child_jobs"][0]["success"])
                self.assertEqual(application.public_engine_job(projected)["phase"], "retry_wait")
            finally:
                application.close()

    def test_failed_child_is_retryable_but_exhausted_root_failure_stays_terminal(self) -> None:
        with tempfile.TemporaryDirectory() as directory, patch.dict(
            os.environ,
            {
                "SCRAPEFLOW_START_PAUSED": "1",
                "SCRAPEFLOW_INTAKE_MONITOR": "0",
                "SCRAPEFLOW_AUTOMATIC_AUDIT": "0",
            },
            clear=False,
        ):
            state_root = Path(directory)
            runner = SimpleEngineRunner(
                state_root,
                alist=_Remote(),
                tmdb=object(),
                validate=False,
                library_root="/library",
            )
            root = EngineJob(
                id="engine-root-failed-child",
                phase="executed",
                created_at="2026-08-08T00:00:00Z",
                updated_at="2026-08-08T00:00:00Z",
                request={"source_path": "/library/待刮削/Show"},
                plan={},
                summary={"mode": "tv"},
            )
            child = EngineJob(
                id="engine-child-failed",
                phase="failed",
                created_at="2026-08-08T00:00:01Z",
                updated_at="2026-08-08T00:00:02Z",
                request={"source_path": "/library/ScrapeFlow/补源/child"},
                plan={},
                summary={"internal_child": True, "root_job_id": root.id},
                error="candidate rejected",
            )
            atomic_write_json(runner._job_path(root.id), root.as_dict(), allow_nan=False)
            atomic_write_json(runner._job_path(child.id), child.as_dict(), allow_nan=False)
            with patch.object(SimpleApplication, "_start_startup_thread"):
                application = SimpleApplication(
                    state_root=state_root,
                    remote_root="/library",
                    remote=_Remote(),
                    engine_runner=runner,
                    enforce_engine_roots=False,
                )
            try:
                projected = application._sync_replenishment_child(root)
                self.assertEqual(projected.summary["replenishment"]["status"], "child_failed")
                self.assertFalse(projected.summary["replenishment"]["terminal"])

                terminal_root = replace(
                    projected,
                    summary={
                        **projected.summary,
                        "replenishment": {
                            **projected.summary["replenishment"],
                            "status": "failed",
                            "terminal": True,
                        },
                    },
                )
                atomic_write_json(runner._job_path(root.id), terminal_root.as_dict(), allow_nan=False)
                projected = application._sync_replenishment_child(terminal_root)
                self.assertEqual(projected.summary["replenishment"]["status"], "failed")
                self.assertTrue(projected.summary["replenishment"]["terminal"])
            finally:
                application.close()

    def test_cancelled_child_is_terminal_and_does_not_block_root_cleanup_projection(self) -> None:
        with tempfile.TemporaryDirectory() as directory, patch.dict(
            os.environ,
            {"SCRAPEFLOW_START_PAUSED": "1", "SCRAPEFLOW_INTAKE_MONITOR": "0", "SCRAPEFLOW_AUTOMATIC_AUDIT": "0"},
            clear=False,
        ):
            state_root = Path(directory)
            runner = SimpleEngineRunner(state_root, alist=_Remote(), tmdb=object(), validate=False, library_root="/library")
            root = EngineJob(
                id="engine-root-cancelled-child", phase="executed", created_at="2026-08-08T00:00:00Z",
                updated_at="2026-08-08T00:00:00Z", request={"source_path": "/library/待刮削/Show"}, plan={}, summary={},
            )
            child = EngineJob(
                id="engine-child-cancelled", phase="cancelled", created_at=root.created_at,
                updated_at="2026-08-08T00:00:02Z", request={"source_path": "/library/ScrapeFlow/补源/child"},
                plan={}, summary={"internal_child": True, "root_job_id": root.id}, error="operator stop",
            )
            atomic_write_json(runner._job_path(root.id), root.as_dict(), allow_nan=False)
            atomic_write_json(runner._job_path(child.id), child.as_dict(), allow_nan=False)
            with patch.object(SimpleApplication, "_start_startup_thread"):
                application = SimpleApplication(state_root=state_root, remote_root="/library", remote=_Remote(), engine_runner=runner, enforce_engine_roots=False)
            try:
                projected = application._sync_replenishment_child(root)
                replenishment = projected.summary["replenishment"]
                self.assertEqual(replenishment["status"], "cancelled")
                self.assertTrue(replenishment["terminal"])
                row = replenishment["child_jobs"][0]
                self.assertEqual(row["public_phase"], "cancelled")
                self.assertTrue(row["terminal"])
            finally:
                application.close()


if __name__ == "__main__":
    unittest.main()
