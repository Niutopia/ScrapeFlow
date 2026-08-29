"""Focused safety coverage for the terminal-only local cleanup boundary."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from engine.scrapeflow.serialization import atomic_write_json
from engine.scrapeflow.intake_source import save_intake_catalog, upsert_intake_source, bind_root_task
from local.scrapeflow_api.simple_engine_runner import (
    EngineExecutionError,
    EngineJob,
    EngineWorkerBusyError,
    SimpleEngineRunner,
)


def _job(
    job_id: str,
    *,
    phase: str = "executed",
    summary: dict[str, object] | None = None,
) -> EngineJob:
    return EngineJob(
        id=job_id,
        phase=phase,
        created_at="2026-08-09T00:00:00Z",
        updated_at="2026-08-09T00:00:00Z",
        request={"source_path": "/incoming/example"},
        plan={},
        summary=summary or {},
    )


class TerminalCleanupTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.runner = SimpleEngineRunner(
            self.root,
            alist=object(),
            tmdb=object(),
            planner=lambda *_args: None,
            validate=False,
        )

    def _write(self, job: EngineJob) -> None:
        atomic_write_json(self.runner._job_path(job.id), job.as_dict(), allow_nan=False)  # noqa: SLF001

    def test_terminal_root_removes_only_its_owned_local_state(self) -> None:
        root = _job("engine-root")
        child = _job(
            "engine-child",
            summary={"internal_child": True, "root_job_id": root.id},
        )
        unrelated = _job("engine-unrelated")
        for job in (root, child, unrelated):
            self._write(job)
        for name in (root.id, unrelated.id):
            (self.root / "gaps" / name).mkdir(parents=True)
            (self.root / "gaps" / name / "gap.json").write_text("{}", encoding="utf-8")
            (self.root / "staging" / name).mkdir(parents=True)
            (self.root / "staging" / name / "payload.aria2").write_text("partial", encoding="utf-8")
        (self.root / "archive-staging" / root.id / "archive" ).mkdir(parents=True)
        (self.root / "archive-staging" / root.id / "archive" / "input.bin").write_bytes(b"owned")
        library_sentinel = self.root / "formal-library" / "keep.mkv"
        library_sentinel.parent.mkdir(parents=True)
        library_sentinel.write_bytes(b"formal-media")

        result = self.runner.cleanup_terminal_job(root.id)

        self.assertTrue(result["removed"])
        self.assertEqual(result["removed_child_job_ids"], [child.id])
        self.assertFalse(self.runner._job_path(root.id).exists())  # noqa: SLF001
        self.assertFalse(self.runner._job_path(child.id).exists())  # noqa: SLF001
        self.assertTrue(self.runner._job_path(unrelated.id).exists())  # noqa: SLF001
        self.assertFalse((self.root / "gaps" / root.id).exists())
        self.assertFalse((self.root / "staging" / root.id).exists())
        self.assertFalse((self.root / "archive-staging" / root.id).exists())
        self.assertTrue((self.root / "gaps" / unrelated.id).exists())
        self.assertTrue((self.root / "staging" / unrelated.id).exists())
        self.assertEqual(library_sentinel.read_bytes(), b"formal-media")
        self.assertFalse(result["formal_library_touched"])

    def test_active_or_child_owned_state_is_rejected_without_deleting_anything(self) -> None:
        root = _job("engine-root")
        active_child = _job(
            "engine-active-child",
            phase="executing",
            summary={"internal_child": True, "root_job_id": root.id},
        )
        for job in (root, active_child):
            self._write(job)
        (self.root / "gaps" / root.id).mkdir(parents=True)
        (self.root / "staging" / root.id).mkdir(parents=True)

        with self.assertRaises(EngineWorkerBusyError):
            self.runner.cleanup_terminal_job(root.id)

        self.assertTrue(self.runner._job_path(root.id).exists())  # noqa: SLF001
        self.assertTrue(self.runner._job_path(active_child.id).exists())  # noqa: SLF001
        self.assertTrue((self.root / "gaps" / root.id).exists())
        self.assertTrue((self.root / "staging" / root.id).exists())

    def test_retry_wait_and_internal_child_cannot_be_cleaned(self) -> None:
        retrying = _job("engine-retrying", phase="retry_wait")
        child = _job(
            "engine-child",
            summary={"internal_child": True, "root_job_id": "engine-root"},
        )
        self._write(retrying)
        self._write(child)

        with self.assertRaises(EngineWorkerBusyError):
            self.runner.cleanup_terminal_job(retrying.id)
        with self.assertRaises(EngineExecutionError):
            self.runner.cleanup_terminal_job(child.id)

    def test_intake_bound_cleanup_keeps_identity_tombstone(self) -> None:
        root = _job(
            "engine-root",
            summary={"automatic": True, "ingress_source_path": "/incoming/example"},
        )
        self._write(root)
        catalog, _ = upsert_intake_source([], "/incoming/example")
        catalog, _ = bind_root_task(catalog, catalog[0].source_id, root.id)
        save_intake_catalog(self.root, catalog)
        result = self.runner.cleanup_terminal_job(root.id)
        marker = self.root / "root-job-tombstones" / f"{root.id}.json"
        self.assertTrue(marker.exists())
        self.assertEqual(Path(result["identity_tombstone"]).resolve(), marker.resolve())
        self.assertEqual(marker.read_text(encoding="utf-8").count(root.id), 1)
        # The tombstone now carries the identity evidence, so the catalog's
        # lifecycle pointer must be released: a lingering binding whose job
        # record is gone fail-closes every later create/retry path for the
        # source ("IntakeSource 仍绑定已不存在的 RootJob") and strands it.
        from engine.scrapeflow.intake_source import load_intake_catalog

        remaining = load_intake_catalog(self.root)
        self.assertEqual(len(remaining), 1)
        self.assertIsNone(remaining[0].root_task_id)


if __name__ == "__main__":
    unittest.main()
