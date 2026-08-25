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
from unittest.mock import patch

from engine.scrapeflow.serialization import atomic_write_json
from engine.scrapeflow.models import Plan, PlannedFile
from engine.scraper import planned_nfos
from local.scrapeflow_api.simple_engine_runner import EngineRequest, SimpleEngineRunner


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
        self.intermediate = "/library/Movie (2020)/source.mkv"

    def _recover(
        self,
        *,
        target: int | None,
        source: int | None,
        intermediate: int | None = None,
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
            if path == self.intermediate and intermediate is not None:
                return {"size": intermediate}
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
        self.assertNotIn("automatic_terminal", recovered.summary)
        self.assertEqual(recovered.summary["recovery"]["status"], "terminal")
        self.assertEqual(recovered.summary["recovery"]["reason"], "target_source_conflict")

    def test_both_absent_stops_as_source_lost(self) -> None:
        recovered, _runner = self._recover(target=None, source=None)

        self.assertEqual(recovered.phase, "failed_verification")
        self.assertEqual(recovered.summary["recovery"]["reason"], "source_lost")

    def test_intermediate_exact_file_is_a_retryable_rename_continuation(self) -> None:
        recovered, _runner = self._recover(
            target=None,
            source=None,
            intermediate=FAKE_VIDEO_SIZE,
        )

        self.assertEqual(recovered.phase, "retry_wait")
        self.assertEqual(
            recovered.summary["recovery"]["reason"],
            "rename_pending_intermediate",
        )
        self.assertEqual(recovered.summary["recovery"]["source"], self.source)
        self.assertEqual(recovered.summary["recovery"]["target"], self.target)

    def test_intermediate_size_mismatch_is_terminal(self) -> None:
        recovered, _runner = self._recover(
            target=None,
            source=None,
            intermediate=FAKE_VIDEO_SIZE - 1,
        )

        self.assertEqual(recovered.phase, "failed_verification")
        self.assertEqual(
            recovered.summary["recovery"]["reason"],
            "intermediate_size_mismatch",
        )

    def test_target_size_mismatch_stops_without_overwrite(self) -> None:
        recovered, _runner = self._recover(target=FAKE_VIDEO_SIZE - 1, source=None)

        self.assertEqual(recovered.phase, "failed_verification")
        self.assertEqual(recovered.summary["recovery"]["reason"], "target_size_mismatch")
        self.assertEqual(recovered.summary["recovery"]["actual_size"], FAKE_VIDEO_SIZE - 1)

    def test_source_size_mismatch_stops_instead_of_retrying(self) -> None:
        recovered, _runner = self._recover(target=None, source=FAKE_VIDEO_SIZE - 1)

        self.assertEqual(recovered.phase, "failed_verification")
        self.assertEqual(recovered.summary["recovery"]["reason"], "source_size_mismatch")

    def test_existing_artifact_size_difference_is_preserved(self) -> None:
        delta = 1
        recovered, _runner = self._recover(
            target=FAKE_VIDEO_SIZE,
            source=None,
            nfo_size_delta=delta,
        )

        # Existing metadata is authoritative and is never overwritten merely
        # because a later TMDB projection has different bytes/size.
        self.assertEqual(recovered.phase, "executed")
        self.assertIsNone(recovered.error)
        self.assertTrue(
            any(
                row.get("kind") == "nfo"
                and row.get("size") == delta + len(
                    next(iter(planned_nfos(fake_plan(self.request, self.alist, object()))))[1]
                )
                for row in (recovered.execution or {}).get("artifacts", [])
            )
        )


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
