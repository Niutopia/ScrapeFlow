"""Focused restart readback matrix tests.

These cases intentionally stub only exact remote observations.  Recovery is a
read-only reconciliation path; no provider write, overwrite, or cleanup is
allowed while deciding a matrix outcome.
"""

from __future__ import annotations

from dataclasses import replace
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch

from engine.scrapeflow.serialization import atomic_write_json
from engine.scrapeflow.models import Plan, PlannedFile
from engine.scraper import planned_nfos
from local.scrapeflow_api.simple_engine_runner import (
    EngineJob,
    EngineRequest,
    SimpleEngineRunner,
)
from local.simple_server import SimpleApplication


FAKE_VIDEO_SIZE = 1024 * 1024


def fake_plan(_request: EngineRequest, _alist: object, _tmdb: object) -> Plan:
    return Plan(
        mode="movie",
        source_root="/incoming/movie",
        target_root="/library/Movie (2020)",
        files=[
            PlannedFile(
                source_path="/incoming/movie/source.mkv",
                source_dir="/incoming/movie",
                original_name="source.mkv",
                final_name="Movie (2020).mkv",
                target_dir="/library/Movie (2020)",
                media_kind="video",
                source_size=FAKE_VIDEO_SIZE,
            )
        ],
        warnings=[],
        metadata={
            "tmdb_id": 1,
            "title": "Movie",
            "original_title": "Movie",
            "year": "2020",
            "poster_path": None,
            "backdrop_path": None,
        },
    )


class RecoveryMatrixTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.alist = object()
        self.request = EngineRequest.from_mapping(
            {
                "source_path": "/incoming/movie",
                "parent_path": "/library",
                "media_type": "movie",
                "tmdb_id": 1,
            }
        )
        self.target = "/library/Movie (2020)/Movie (2020).mkv"
        self.source = "/incoming/movie/source.mkv"

    def _recover(
        self,
        *,
        target: int | None,
        source: int | None,
        nfo_size_delta: int = 0,
    ):
        runner = SimpleEngineRunner(
            self.root,
            alist=self.alist,
            tmdb=object(),
            planner=fake_plan,
            validate=False,
        )
        job = runner.plan_job(self.request, job_id="matrix-job")
        interrupted = replace(job, phase="failed", execution=None, error="interrupted")
        atomic_write_json(runner._job_path(job.id), interrupted.as_dict(), allow_nan=False)
        persisted_plan = runner._plan_from_job(job)
        nfo_sizes = {
            target_path: len(content)
            for target_path, content in planned_nfos(persisted_plan)
        }

        def exact(path: str, *, wait_for_visibility: bool = True, expected_size: int | None = None):
            del wait_for_visibility
            if path == self.target and target is not None:
                return {"size": target}
            if path == self.source and source is not None:
                return {"size": source}
            # ``fake_plan`` carries one deterministic NFO; the matrix under
            # test is the media/source pair, so make that already-written
            # artifact visible and avoid coupling these cases to metadata
            # generation details.
            if path.endswith(".nfo"):
                return {"size": nfo_sizes.get(path, expected_size or 1) + nfo_size_delta}
            return None

        with patch.object(runner, "_exact_info", side_effect=exact):
            return runner.recover_job(job.id), runner

    def test_target_correct_source_absent_completes_without_retry(self) -> None:
        recovered, _runner = self._recover(target=FAKE_VIDEO_SIZE, source=None)

        self.assertEqual(recovered.phase, "executed")
        self.assertIsNone(recovered.error)

    def test_target_missing_source_present_is_retryable(self) -> None:
        recovered, _runner = self._recover(target=None, source=FAKE_VIDEO_SIZE)

        self.assertEqual(recovered.phase, "retry_wait")
        self.assertEqual(recovered.summary["recovery"], {
            "status": "retryable",
            "reason": "target_missing_source_present",
            "source": self.source,
            "target": self.target,
        })

    def test_both_present_stops_on_conflict(self) -> None:
        recovered, _runner = self._recover(
            target=FAKE_VIDEO_SIZE,
            source=FAKE_VIDEO_SIZE,
        )

        self.assertEqual(recovered.phase, "failed_verification")
        self.assertTrue(recovered.summary["automatic_terminal"])
        self.assertEqual(recovered.summary["recovery"]["status"], "terminal")
        self.assertEqual(recovered.summary["recovery"]["reason"], "target_source_conflict")

    def test_both_absent_stops_as_source_lost(self) -> None:
        recovered, _runner = self._recover(target=None, source=None)

        self.assertEqual(recovered.phase, "failed_verification")
        self.assertEqual(recovered.summary["recovery"]["reason"], "source_lost")

    def test_target_size_mismatch_stops_without_overwrite(self) -> None:
        recovered, _runner = self._recover(target=FAKE_VIDEO_SIZE - 1, source=None)

        self.assertEqual(recovered.phase, "failed_verification")
        self.assertEqual(recovered.summary["recovery"]["reason"], "target_size_mismatch")
        self.assertEqual(recovered.summary["recovery"]["actual_size"], FAKE_VIDEO_SIZE - 1)

    def test_source_size_mismatch_stops_instead_of_retrying(self) -> None:
        recovered, _runner = self._recover(target=None, source=FAKE_VIDEO_SIZE - 1)

        self.assertEqual(recovered.phase, "failed_verification")
        self.assertEqual(recovered.summary["recovery"]["reason"], "source_size_mismatch")

    def test_artifact_size_mismatch_is_terminal_too(self) -> None:
        recovered, _runner = self._recover(
            target=FAKE_VIDEO_SIZE,
            source=None,
            nfo_size_delta=1,
        )

        self.assertEqual(recovered.phase, "failed_verification")
        self.assertEqual(recovered.summary["recovery"]["reason"], "artifact_size_mismatch")

    def test_scheduler_does_not_execute_after_matrix_marks_terminal(self) -> None:
        initial = EngineJob(
            id="matrix-scheduler-root",
            phase="failed",
            created_at="2026-01-01T00:00:00Z",
            updated_at="2026-01-01T00:00:00Z",
            request={"source_path": self.source},
            plan={"mode": "movie"},
            summary={"automatic": True, "automatic_terminal": False},
        )
        terminal = replace(
            initial,
            phase="failed_verification",
            summary={
                **initial.summary,
                "automatic_terminal": True,
                "recovery": {"status": "terminal", "reason": "target_source_conflict"},
            },
        )
        runner = Mock()
        runner.get_job.return_value = initial
        runner.recover_job.return_value = terminal
        application = object.__new__(SimpleApplication)
        application.control = lambda: {"paused": False}  # type: ignore[method-assign]
        application._get_engine_runner = lambda: runner  # type: ignore[method-assign]
        application._cancel_job_timers = Mock()  # type: ignore[method-assign]

        application._run_automatic_job(initial.id)  # noqa: SLF001 - scheduler boundary

        runner.recover_job.assert_called_once_with(initial.id)
        runner.execute_automatic.assert_not_called()
        application._cancel_job_timers.assert_called_once_with(initial.id)


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
