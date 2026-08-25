"""Focused coverage for the single local pause/RootJob record."""

from __future__ import annotations

import tempfile
import unittest
import json
from pathlib import Path

from local.scrapeflow_api.control_state import PersistentControlState


class PersistentControlStateTests(unittest.TestCase):
    def test_missing_or_bad_record_starts_paused_without_a_selection(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "global-control.json"
            state = PersistentControlState(path)
            self.assertEqual(state.read(), {"paused": True, "root_job_id": None})

            path.write_text('{"paused": false, "revision": 3}', encoding="utf-8")
            self.assertEqual(state.read(), {"paused": True, "root_job_id": None})

    def test_select_and_pause_keep_only_two_fields(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "global-control.json"
            state = PersistentControlState(path)

            self.assertEqual(
                state.set(paused=False, root_job_id="root-1"),
                {"paused": False, "root_job_id": "root-1"},
            )
            self.assertEqual(
                state.set(paused=True),
                {"paused": True, "root_job_id": "root-1"},
            )
            self.assertEqual(
                json.loads(path.read_text(encoding="utf-8")),
                {"paused": True, "root_job_id": "root-1"},
            )


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
