"""Regression tests for the automatic root final-cleanup gap fence."""

from __future__ import annotations

from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from engine.scrapeflow.current_plan import plan_to_dict
from engine.scrapeflow.models import Plan
from engine.scrapeflow.serialization import atomic_write_json
from local.scrapeflow_api.simple_engine_runner import (
    EngineJob,
    EngineWorkerBusyError,
    SimpleEngineRunner,
    SimplePlanExecutor,
)


class FinalCleanupGapGuardTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.runner = SimpleEngineRunner(
            self.root,
            alist=object(),
            tmdb=object(),
            validate=False,
            library_root="/library",
        )

    @staticmethod
    def _plan(gaps: list[dict[str, object]]) -> dict[str, object]:
        return plan_to_dict(Plan(
            mode="movie",
            source_root="/library/待刮削/Fixture",
            target_root="/library/电影/Fixture (2024)",
            files=[],
            warnings=[],
            metadata={"tmdb_id": 1, "title": "Fixture"},
            scan_report={"resource_gaps": gaps},
        ))

    def _persist_root(
        self,
        *,
        plan_gaps: list[dict[str, object]],
        summary_gaps: list[dict[str, object]],
        job_id: str,
        cleanup_status: str = "pending",
    ) -> EngineJob:
        job = EngineJob(
            id=job_id,
            phase="executed",
            created_at="2026-08-10T00:00:00Z",
            updated_at="2026-08-10T00:00:00Z",
            request={"source_path": "/library/待刮削/Fixture"},
            plan=self._plan(plan_gaps),
            summary={
                "automatic": True,
                "resource_gaps": summary_gaps,
                "lifecycle": {
                    "formal_write": {"status": "verified"},
                    "audit": {"status": "trusted"},
                    "provider": {"status": "no_gap"},
                    # Simulate a stale imported/coordinator decision.  The
                    # finalizer itself must refuse to trust it.
                    "cleanup_ready": True,
                    "cleanup": {"status": cleanup_status},
                },
            },
        )
        atomic_write_json(self.runner.jobs_root / f"{job.id}.json", job.as_dict(), allow_nan=False)
        return job

    def test_actionable_gaps_from_any_durable_root_projection_fence_cleanup(self) -> None:
        for origin in ("plan", "summary"):
            for kind in (
                "missing_media",
                "missing_episode",
                "missing_season",
                "missing_subtitle",
            ):
                with self.subTest(origin=origin, kind=kind):
                    gap = {"id": f"{origin}-{kind}", "kind": kind, "label": "Fixture"}
                    job = self._persist_root(
                        job_id=f"gap-{origin}-{kind}",
                        plan_gaps=[gap] if origin == "plan" else [],
                        summary_gaps=[gap] if origin == "summary" else [],
                    )

                    with patch.object(SimplePlanExecutor, "finalize_cleanup") as finalizer:
                        with self.assertRaisesRegex(EngineWorkerBusyError, "资源缺口尚未消失"):
                            self.runner.finalize_automatic_lifecycle(job.id)

                    finalizer.assert_not_called()
                    persisted = self.runner.get_job(job.id)
                    lifecycle = persisted.summary["lifecycle"]
                    self.assertFalse(lifecycle["cleanup_ready"])
                    self.assertEqual(lifecycle["cleanup"]["status"], "pending")
                    self.assertEqual(persisted.phase, "executed")

    def test_gap_reopens_a_historical_completed_cleanup_marker(self) -> None:
        job = self._persist_root(
            job_id="completed-marker-with-gap",
            plan_gaps=[{"id": "still-missing", "kind": "missing_media", "label": "Fixture"}],
            summary_gaps=[],
            cleanup_status="completed",
        )

        with patch.object(SimplePlanExecutor, "finalize_cleanup") as finalizer:
            with self.assertRaisesRegex(EngineWorkerBusyError, "资源缺口尚未消失"):
                self.runner.finalize_automatic_lifecycle(job.id)

        finalizer.assert_not_called()
        persisted = self.runner.get_job(job.id)
        lifecycle = persisted.summary["lifecycle"]
        self.assertFalse(lifecycle["cleanup_ready"])
        self.assertEqual(lifecycle["cleanup"]["status"], "pending")
        self.assertIn("invalidated_by_resource_gaps_at", lifecycle["cleanup"])

    def test_no_actionable_gap_allows_the_normal_final_cleanup(self) -> None:
        job = self._persist_root(
            job_id="gap-free-root",
            plan_gaps=[],
            summary_gaps=[],
        )

        with patch.object(
            SimplePlanExecutor,
            "finalize_cleanup",
            return_value={"cleanup": [], "cleanup_count": 0},
        ) as finalizer:
            completed = self.runner.finalize_automatic_lifecycle(job.id)

        finalizer.assert_called_once()
        lifecycle = completed.summary["lifecycle"]
        self.assertEqual(lifecycle["cleanup"]["status"], "completed")
        self.assertTrue(lifecycle["cleanup_ready"])
        self.assertEqual(completed.phase, "executed")

    def test_non_provider_gap_does_not_create_a_false_cleanup_deadlock(self) -> None:
        job = self._persist_root(
            job_id="informational-gap-root",
            plan_gaps=[{"kind": "subtitle_without_video", "label": "Fixture"}],
            summary_gaps=[],
        )

        with patch.object(
            SimplePlanExecutor,
            "finalize_cleanup",
            return_value={"cleanup": [], "cleanup_count": 0},
        ) as finalizer:
            completed = self.runner.finalize_automatic_lifecycle(job.id)

        finalizer.assert_called_once()
        self.assertEqual(completed.summary["lifecycle"]["cleanup"]["status"], "completed")


if __name__ == "__main__":
    unittest.main()
