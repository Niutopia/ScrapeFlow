"""Regression coverage for the fail-closed local scheduler control state."""

from __future__ import annotations

import multiprocessing
import os
import tempfile
import unittest
from pathlib import Path

from local.simple_server import SimpleApplication
from local.scrapeflow_api.control_state import PersistentControlState
from local.scrapeflow_api.root_job_pilot import (
    disabled_scope,
    root_job_allowed,
    single_root_scope,
    unrestricted_scope,
)


class _BlockingPersistentControlState(PersistentControlState):
    """Test double that pauses after CAS has read its current document."""

    def __init__(self, path: Path, entered: object, release: object) -> None:
        super().__init__(path)
        self._entered = entered
        self._release = release

    def _write_locked(self, current, *, paused, normalized_reason, automatic_scope):
        self._entered.set()
        if not self._release.wait(8):
            raise RuntimeError("test did not release the first CAS writer")
        return super()._write_locked(
            current,
            paused=paused,
            normalized_reason=normalized_reason,
            automatic_scope=automatic_scope,
        )


def _blocked_cas_worker(
    path: str,
    expected_revision: int,
    scope: dict[str, object],
    entered: object,
    release: object,
    results: object,
) -> None:
    control = _BlockingPersistentControlState(Path(path), entered, release)
    outcome = control.compare_and_set_paused(
        expected_revision=expected_revision,
        expected_paused=True,
        expected_scope=scope,
        paused=False,
        automatic_scope=scope,
    )
    results.put(("first", outcome is not None))


def _competing_cas_worker(
    path: str,
    expected_revision: int,
    scope: dict[str, object],
    started: object,
    results: object,
) -> None:
    started.set()
    control = PersistentControlState(Path(path))
    outcome = control.compare_and_set_paused(
        expected_revision=expected_revision,
        expected_paused=True,
        expected_scope=scope,
        paused=True,
        automatic_scope=scope,
    )
    results.put(("second", outcome is not None))


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

    def test_unscoped_resume_never_implicitly_grants_all_root_automation(self) -> None:
        control = PersistentControlState(self.path)

        unscoped = control.set_paused(False)

        self.assertEqual(unscoped["automatic_scope"], disabled_scope())
        self.assertFalse(root_job_allowed(unscoped["automatic_scope"], "root-a"))

        selected = control.set_paused(
            False,
            automatic_scope=single_root_scope("root-a"),
        )
        self.assertTrue(root_job_allowed(selected["automatic_scope"], "root-a"))
        self.assertFalse(root_job_allowed(selected["automatic_scope"], "root-b"))
        all_scope = control.set_paused(
            False,
            automatic_scope=unrestricted_scope(),
        )
        self.assertTrue(root_job_allowed(all_scope["automatic_scope"], "root-b"))

    def test_legacy_v1_control_document_is_migrated_to_a_disabled_scope(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(
            '{"version":1,"paused":false,"scheduler_paused":false,'
            '"persistent":true,"updated_at":"2026-08-17T00:00:00Z","reason":null}',
            encoding="utf-8",
        )

        state = PersistentControlState(self.path).read()

        self.assertFalse(state["paused"])
        self.assertEqual(state["automatic_scope"], disabled_scope())
        self.assertFalse(root_job_allowed(state["automatic_scope"], "root-a"))

    def test_v2_scope_document_gets_a_zero_revision_then_cas_rejects_a_stale_resume(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(
            '{"version":2,"paused":true,"scheduler_paused":true,'
            '"persistent":true,"updated_at":"2026-08-17T00:00:00Z",'
            '"reason":"operator pause","automatic_scope":'
            '{"mode":"single_root","root_job_id":"root-a"}}',
            encoding="utf-8",
        )
        control = PersistentControlState(self.path)

        snapshot = control.read()
        self.assertEqual(snapshot["revision"], 0)
        changed = control.set_paused(
            True,
            "another operator pause",
            automatic_scope=single_root_scope("root-a"),
        )
        self.assertEqual(changed["revision"], 1)
        stale = control.compare_and_set_paused(
            expected_revision=snapshot["revision"],
            expected_paused=True,
            expected_scope=snapshot["automatic_scope"],
            paused=False,
            automatic_scope=single_root_scope("root-a"),
        )

        self.assertIsNone(stale)
        current = control.read()
        self.assertTrue(current["paused"])
        self.assertEqual(current["revision"], 1)

    @unittest.skipUnless(os.name == "posix", "control-state file lock requires POSIX")
    def test_cross_process_compare_and_set_cannot_overwrite_a_concurrent_pause(self) -> None:
        """The lock covers the actual read/compare/write, not only each write."""
        scope = single_root_scope("root-a")
        initial = PersistentControlState(self.path).set_paused(
            True,
            automatic_scope=scope,
        )
        context = multiprocessing.get_context("spawn")
        entered = context.Event()
        release = context.Event()
        second_started = context.Event()
        results = context.Queue()
        first = context.Process(
            target=_blocked_cas_worker,
            args=(
                str(self.path),
                int(initial["revision"]),
                scope,
                entered,
                release,
                results,
            ),
        )
        second = context.Process(
            target=_competing_cas_worker,
            args=(
                str(self.path),
                int(initial["revision"]),
                scope,
                second_started,
                results,
            ),
        )
        try:
            first.start()
            self.assertTrue(entered.wait(8))
            second.start()
            self.assertTrue(second_started.wait(8))
            # The first process has already completed its compare and is held
            # just before publication.  A second process must be blocked on
            # the same sidecar flock, rather than independently accepting the
            # stale revision and later overwriting an operator pause/resume.
            second.join(0.5)
            self.assertTrue(second.is_alive())
            release.set()
            first.join(8)
            second.join(8)
            self.assertFalse(first.is_alive())
            self.assertFalse(second.is_alive())
            self.assertEqual(first.exitcode, 0)
            self.assertEqual(second.exitcode, 0)
            outcomes = dict(results.get(timeout=3) for _ in range(2))
            self.assertEqual(outcomes, {"first": True, "second": False})
            current = PersistentControlState(self.path).read()
            self.assertFalse(current["paused"])
            self.assertEqual(current["revision"], int(initial["revision"]) + 1)
        finally:
            release.set()
            for process in (first, second):
                if process.is_alive():
                    process.terminate()
                process.join(timeout=2)
            results.close()
            results.join_thread()

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

    def test_new_application_starts_effectively_paused_after_prior_resume(self) -> None:
        state_root = self.path.parent
        first = SimpleApplication(
            state_root=state_root,
            remote_root="/library",
            remote=object(),
        )
        first.set_paused(False, "operator opened lane")
        first.close()

        second = SimpleApplication(
            state_root=state_root,
            remote_root="/library",
            remote=object(),
        )
        self.addCleanup(second.close)

        # The persisted operator decision remains available for inspection,
        # but it cannot authorize a new process to begin side effects.
        self.assertFalse(PersistentControlState(self.path).read()["paused"])
        self.assertTrue(second.control()["paused"])
        self.assertEqual(second.control()["reason"], "startup_pause")

        resumed = second.set_paused(False, "operator reopened lane")
        self.assertFalse(resumed["paused"])
        self.assertFalse(second.control()["paused"])


if __name__ == "__main__":
    unittest.main()
