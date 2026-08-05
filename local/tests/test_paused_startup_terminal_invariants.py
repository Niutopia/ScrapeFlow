from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import sys
import tempfile
import unittest


SERVER_PATH = Path(__file__).resolve().parents[1] / "server.py"
spec = importlib.util.spec_from_file_location(
    "scrapeflow_local_server_paused_startup_invariants", SERVER_PATH,
)
assert spec and spec.loader
server = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = server
spec.loader.exec_module(server)


class PausedStartupTerminalInvariantTests(unittest.TestCase):
    def _isolate(self, directory: str) -> tuple[Path, object, object, object, object]:
        previous_root = server.JOBS_ROOT
        previous_provider = server.Job.root_provider
        previous_jobs = server.JOBS
        previous_control = server.GLOBAL_CONTROL
        state_root = Path(directory)
        jobs_root = state_root / "jobs"
        jobs_root.mkdir()
        server.JOBS_ROOT = jobs_root
        server.Job.root_provider = staticmethod(lambda: server.JOBS_ROOT)
        server.JOBS = {}
        server.GLOBAL_CONTROL = server.PersistentGlobalControl(
            state_root / "global-control.json", default_paused=True,
        )
        self.assertTrue(server.GLOBAL_CONTROL.paused)
        return (
            state_root, previous_root, previous_provider, previous_jobs,
            previous_control,
        )

    def _restore(self, previous_root: object, previous_provider: object,
                 previous_jobs: object, previous_control: object) -> None:
        server.JOBS = previous_jobs
        server.JOBS_ROOT = previous_root
        server.Job.root_provider = staticmethod(previous_provider)
        server.GLOBAL_CONTROL = previous_control

    def test_restore_preserves_newest_processed_marker_against_old_completed_jobs(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            (state_root, previous_root, previous_provider, previous_jobs,
             previous_control) = self._isolate(directory)
            try:
                path = "/quark/影视/番剧/某科学的超电磁炮"
                newest = "2026-07-30T14:25:52.274568+00:00"
                processed = state_root / "processed.json"
                processed.write_text(json.dumps({
                    "version": 1,
                    "paths": {path: {"phase": "completed", "updated_at": newest}},
                }, ensure_ascii=False, sort_keys=True) + "\n", encoding="utf-8")
                before = processed.read_bytes()

                for job_id, updated_at in (
                    ("d" * 12, "2026-07-26T12:39:42.142361+00:00"),
                    ("e" * 12, "2026-07-29T08:00:00.000000+00:00"),
                ):
                    job = server.Job(
                        job_id, path, "/quark/影视/番剧", "tv", False, True,
                        phase="completed", updated_at=updated_at,
                        plan_summary={"target_root": path},
                        progress={
                            "stage": "execution_complete", "completed": 1,
                            "total": 1, "percent": 100.0, "message": "completed",
                        },
                    )
                    job.directory.mkdir()
                    server.persist_job(job)

                server.restore_jobs()

                marker = server.load_processed_paths()[path]
                self.assertEqual(marker, {"phase": "completed", "updated_at": newest})
                self.assertEqual(processed.read_bytes(), before)
            finally:
                self._restore(
                    previous_root, previous_provider, previous_jobs,
                    previous_control,
                )


if __name__ == "__main__":
    unittest.main()
