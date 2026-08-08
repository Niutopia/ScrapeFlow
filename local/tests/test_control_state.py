"""Regression coverage for the fail-closed local scheduler control state."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from local.simple_server import SimpleApplication
from local.scrapeflow_api.control_state import PersistentControlState


class PersistentControlStateTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.path = Path(self.temporary.name) / "state" / "global-control.json"

    def test_missing_document_is_paused_and_read_does_not_bootstrap_a_file(self) -> None:
        control = PersistentControlState(self.path)

        state = control.read()

        self.assertTrue(state["paused"])
        self.assertTrue(state["scheduler_paused"])
        self.assertEqual(state["reason"], "control_state_missing")
        self.assertFalse(self.path.exists())

    def test_corrupt_missing_and_wrong_typed_fields_all_fail_closed_without_rewrite(self) -> None:
        cases = (
            "not json",
            '{"version": 1, "paused": false}',
            (
                '{"version": 1, "paused": false, "scheduler_paused": false, '
                '"persistent": true, "updated_at": "2026-08-08T00:00:00Z", "reason": 1}'
            ),
            (
                '{"version": 1, "paused": "false", "scheduler_paused": false, '
                '"persistent": true, "updated_at": "2026-08-08T00:00:00Z", "reason": null}'
            ),
            (
                '{"version": 1, "paused": false, "scheduler_paused": true, '
                '"persistent": true, "updated_at": "2026-08-08T00:00:00Z", "reason": null}'
            ),
        )
        control = PersistentControlState(self.path)

        for raw in cases:
            with self.subTest(raw=raw):
                self.path.parent.mkdir(parents=True, exist_ok=True)
                self.path.write_text(raw, encoding="utf-8")

                state = control.read()

                self.assertTrue(state["paused"])
                self.assertEqual(state["reason"], "control_state_invalid")
                self.assertEqual(self.path.read_text(encoding="utf-8"), raw)

    def test_explicit_resume_is_the_only_repair_path_and_persists_a_valid_document(self) -> None:
        control = PersistentControlState(self.path)

        state = control.set_paused(False)

        self.assertFalse(state["paused"])
        self.assertTrue(self.path.exists())
        self.assertFalse(PersistentControlState(self.path).read()["paused"])

    def test_application_close_preserves_an_explicit_operator_resume(self) -> None:
        state_root = self.path.parent
        application = SimpleApplication(
            state_root=state_root,
            remote_root="/library",
            remote=object(),
        )
        self.addCleanup(application.close)
        self.assertTrue(application.control()["paused"])
        resumed = application.set_paused(False)
        before_close = self.path.read_text(encoding="utf-8")

        application.close()

        self.assertFalse(resumed["paused"])
        self.assertEqual(self.path.read_text(encoding="utf-8"), before_close)
        self.assertFalse(PersistentControlState(self.path).read()["paused"])


if __name__ == "__main__":
    unittest.main()
