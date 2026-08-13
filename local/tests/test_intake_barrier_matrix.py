"""Exhaustive matrix for the A→K settlement barrier predicates.

``_intake_is_settled`` (the strict public L barrier) and
``_full_audit_ready_for_provider`` (the relaxed post-restart pre-audit
check) walk the same persisted job state.  This matrix pins, from the
invariants instead of the implementation:

* every phase in the Engine vocabulary against both predicates;
* the summary projections (lifecycle / replenishment / post-audit) that
  keep a terminal root inside the A→K blocker set;
* the intended asymmetry: only an audit-owned Provider root's persisted
  retry state may be relaxed after a restart, never an ordinary root's;
* containment: whenever the strict barrier opens, the relaxed predicate
  must also open (it starts with the strict fast path).
"""

from __future__ import annotations

import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from engine.scrapeflow.serialization import atomic_write_json
from local.simple_server import (
    SimpleApplication,
    _BARRIER_BLOCKED_ROOT_PHASES,
    _ENGINE_TERMINAL_FAILURE_PHASES,
    _ENGINE_TERMINAL_PHASES,
)
from local.scrapeflow_api.simple_engine_runner import (
    _ENGINE_PHASES,
    EngineJob,
    SimpleEngineRunner,
)
from local.tests.test_audit_owned_root import EmptyAList, _gap, _project


_NOW = "2026-08-13T00:00:00+00:00"


def _settled_lifecycle_summary() -> dict[str, object]:
    return {
        "lifecycle": {
            "formal_write": {"status": "verified"},
            "cleanup": {"status": "completed"},
            "audit": {"status": "trusted"},
            "provider": {"status": "terminal"},
        },
    }


def _write_job(
    runner: SimpleEngineRunner,
    job_id: str,
    phase: str,
    *,
    summary: dict[str, object] | None = None,
) -> EngineJob:
    job = EngineJob(
        id=job_id, phase=phase, created_at=_NOW, updated_at=_NOW,
        request={}, plan={}, summary=dict(summary or {}),
    )
    atomic_write_json(
        runner.jobs_root / f"{job_id}.json", job.as_dict(), allow_nan=False,
    )
    return job


def _rewrite_job(
    runner: SimpleEngineRunner,
    job_id: str,
    *,
    phase: str | None = None,
    extra_summary: dict[str, object] | None = None,
) -> None:
    path = runner.jobs_root / f"{job_id}.json"
    record = json.loads(path.read_text(encoding="utf-8"))
    if phase is not None:
        record["phase"] = phase
    if extra_summary:
        summary = dict(record.get("summary") or {})
        summary.update(extra_summary)
        record["summary"] = summary
    atomic_write_json(path, record, allow_nan=False)


class IntakeBarrierMatrixTests(unittest.TestCase):
    def _evaluate(self, build, *, scan_empty: bool = True) -> tuple[bool, bool]:
        """Build one persisted scenario and query both barrier predicates."""
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            remote = EmptyAList()
            runner = SimpleEngineRunner(
                root, alist=remote, tmdb=object(), validate=False,
                library_root="/library",
            )
            with patch.object(SimpleApplication, "_start_startup_thread"):
                app = SimpleApplication(
                    state_root=root, remote_root="/library", remote=remote,
                    engine_runner=runner, enforce_engine_roots=False,
                )
            try:
                build(app, runner)
                app._intake_status["last_scan_empty"] = scan_empty  # noqa: SLF001
                strict = app._intake_is_settled()  # noqa: SLF001
                relaxed = app._full_audit_ready_for_provider()  # noqa: SLF001
            finally:
                app.close()
        # Containment invariant: the relaxed predicate contains the strict
        # one by construction; a strict-open/relaxed-closed pair would mean
        # the restart relaxation accidentally tightened the normal path.
        self.assertGreaterEqual(relaxed, strict)
        return strict, relaxed

    def test_barrier_phase_sets_cover_the_engine_vocabulary(self) -> None:
        """Every Engine phase must be explicitly classified by the barrier."""
        classified = (
            _BARRIER_BLOCKED_ROOT_PHASES
            | _ENGINE_TERMINAL_FAILURE_PHASES
            | {"retry_wait", "cancelled", "reconciled", "executed", "completed"}
        )
        self.assertEqual(classified, _ENGINE_PHASES)
        self.assertTrue(_ENGINE_TERMINAL_PHASES <= _ENGINE_PHASES)

    def test_every_bare_root_phase_keeps_both_barriers_closed(self) -> None:
        """Without settlement proof no phase may open either predicate."""
        for phase in sorted(_ENGINE_PHASES):
            with self.subTest(phase=phase):
                strict, relaxed = self._evaluate(
                    lambda app, runner, phase=phase: _write_job(
                        runner, "root-1", phase,
                    ),
                )
                self.assertFalse(strict)
                self.assertFalse(relaxed)

    def test_settled_ordinary_root_matrix(self) -> None:
        cases: list[tuple[str, str, dict[str, object], bool, bool]] = [
            ("settled_completed", "completed", _settled_lifecycle_summary(), True, True),
            ("settled_executed", "executed", _settled_lifecycle_summary(), True, True),
            # The strict barrier also demands lifecycle audit/provider
            # projections; the relaxed body intentionally re-checks only the
            # write/cleanup receipts (observable via the fast-path union).
            (
                "lifecycle_audit_pending", "completed",
                {
                    "lifecycle": {
                        "formal_write": {"status": "verified"},
                        "cleanup": {"status": "completed"},
                        "audit": {"status": "pending"},
                        "provider": {"status": "terminal"},
                    },
                },
                False, True,
            ),
            (
                "formal_write_unverified", "completed",
                {
                    "lifecycle": {
                        "formal_write": {"status": "planned"},
                        "cleanup": {"status": "completed"},
                        "audit": {"status": "trusted"},
                        "provider": {"status": "terminal"},
                    },
                },
                False, False,
            ),
            (
                "non_terminal_provider_projection", "completed",
                {
                    **_settled_lifecycle_summary(),
                    "replenishment": {"status": "retry_wait", "terminal": False},
                },
                False, False,
            ),
            (
                "orphaned_provider_progress", "completed",
                {
                    **_settled_lifecycle_summary(),
                    "replenishment": {"status": "acquiring", "terminal": True},
                },
                False, False,
            ),
            (
                "terminal_provider_projection", "completed",
                {
                    **_settled_lifecycle_summary(),
                    "replenishment": {"status": "completed", "terminal": True},
                },
                True, True,
            ),
            (
                "post_audit_pending", "completed",
                {
                    **_settled_lifecycle_summary(),
                    "post_acquisition_reaudit": {"status": "pending"},
                },
                False, False,
            ),
            (
                "post_audit_cleaned", "completed",
                {
                    **_settled_lifecycle_summary(),
                    "post_acquisition_reaudit": {"status": "cleaned"},
                },
                True, True,
            ),
        ]
        for name, phase, summary, want_strict, want_relaxed in cases:
            with self.subTest(case=name):
                strict, relaxed = self._evaluate(
                    lambda app, runner, phase=phase, summary=summary: _write_job(
                        runner, "root-1", phase, summary=summary,
                    ),
                )
                self.assertEqual(strict, want_strict)
                self.assertEqual(relaxed, want_relaxed)

    def test_audit_owned_root_matrix_encodes_the_restart_relaxation(self) -> None:
        cases: list[tuple[str, dict[str, object], bool, bool]] = [
            ("fresh_executed_ledger", {}, True, True),
            # The single intended relaxation: persisted audited retry state.
            ("phase_retry_wait", {"phase": "retry_wait"}, False, True),
            (
                "provider_retry_projection",
                {"extra_summary": {
                    "replenishment": {"status": "retry_wait", "terminal": False},
                }},
                False, True,
            ),
            # The strict barrier also reads the summary post-audit marker;
            # the relaxed audit branch trusts the runner's durable gap state
            # (covered by its own pending-reaudit check).
            (
                "summary_post_audit_pending",
                {"extra_summary": {
                    "post_acquisition_reaudit": {"status": "pending"},
                }},
                False, True,
            ),
            ("phase_executing", {"phase": "executing"}, False, False),
            ("phase_failed", {"phase": "failed"}, False, False),
            ("phase_cancelled", {"phase": "cancelled"}, False, False),
            (
                "provider_waiting_reconcile",
                {"extra_summary": {
                    "replenishment": {"status": "waiting_reconcile", "terminal": False},
                }},
                False, False,
            ),
            (
                "provider_needs_attention",
                {"extra_summary": {
                    "replenishment": {"status": "needs_attention", "terminal": False},
                }},
                False, False,
            ),
            (
                "provider_in_doubt",
                {"extra_summary": {
                    "replenishment": {"status": "in_doubt", "terminal": False},
                }},
                False, False,
            ),
            (
                "provider_orphaned_progress",
                {"extra_summary": {
                    "replenishment": {"status": "provider_searching", "terminal": False},
                }},
                False, False,
            ),
            (
                "provider_terminal_failure_projection",
                {"extra_summary": {
                    "replenishment": {"status": "failed", "terminal": True},
                }},
                True, True,
            ),
        ]
        for name, mutation, want_strict, want_relaxed in cases:
            def build(app, runner, mutation=mutation):
                job = runner.create_audit_owned_root(_project(_gap()))
                if mutation:
                    _rewrite_job(runner, job.id, **mutation)
            with self.subTest(case=name):
                strict, relaxed = self._evaluate(build)
                self.assertEqual(strict, want_strict)
                self.assertEqual(relaxed, want_relaxed)

    def test_restart_relaxation_never_extends_to_ordinary_roots(self) -> None:
        """An audited retry may reopen L, but not past ordinary Provider state."""
        def build_relaxable(app, runner):
            job = runner.create_audit_owned_root(_project(_gap()))
            _rewrite_job(runner, job.id, phase="retry_wait")

        def build_blocked(app, runner):
            build_relaxable(app, runner)
            # The ordinary root carries full settlement receipts, yet its
            # persisted Provider marker must keep the relaxed check closed:
            # only audit-owned retry state may be relaxed.
            _write_job(
                runner, "ordinary-1", "completed",
                summary={
                    **_settled_lifecycle_summary(),
                    "replenishment": {"status": "retry_wait", "terminal": True},
                },
            )

        strict, relaxed = self._evaluate(build_relaxable)
        self.assertFalse(strict)
        self.assertTrue(relaxed)

        strict, relaxed = self._evaluate(build_blocked)
        self.assertFalse(strict)
        self.assertFalse(relaxed)

    def test_reconciliation_outcome_matrix(self) -> None:
        def build_outcome_root(
            runner: SimpleEngineRunner,
            *,
            outcome: str,
            phase: str,
            verified: bool,
        ) -> None:
            _write_job(
                runner, "root-1", phase,
                summary={"reconciliation": {"outcome": outcome}},
            )
            if outcome == "duplicate_complete":
                runner.duplicate_complete_consumption_verified = (  # type: ignore[method-assign]
                    lambda job_id, verified=verified: verified
                )
            else:
                runner.existing_gap_source_hold_verified = (  # type: ignore[method-assign]
                    lambda job_id, verified=verified: verified
                )

        cases: list[tuple[str, dict[str, object], bool, bool]] = [
            (
                "duplicate_verified",
                {"outcome": "duplicate_complete", "phase": "completed", "verified": True},
                True, True,
            ),
            (
                "duplicate_unverified",
                {"outcome": "duplicate_complete", "phase": "completed", "verified": False},
                False, False,
            ),
            (
                "duplicate_wrong_phase",
                {"outcome": "duplicate_complete", "phase": "executed", "verified": True},
                False, False,
            ),
            (
                "existing_gap_hold_verified",
                {"outcome": "existing_gap", "phase": "completed", "verified": True},
                True, True,
            ),
            (
                "existing_gap_hold_unverified",
                {"outcome": "existing_gap", "phase": "completed", "verified": False},
                False, False,
            ),
            (
                "existing_gap_wrong_phase",
                {"outcome": "existing_gap", "phase": "reconciled", "verified": True},
                False, False,
            ),
        ]
        for name, kwargs, want_strict, want_relaxed in cases:
            with self.subTest(case=name):
                strict, relaxed = self._evaluate(
                    lambda app, runner, kwargs=kwargs: build_outcome_root(
                        runner, **kwargs,
                    ),
                )
                self.assertEqual(strict, want_strict)
                self.assertEqual(relaxed, want_relaxed)

    def test_duplicate_post_audit_asymmetry_is_preserved(self) -> None:
        """Strict reads the duplicate root's post-audit marker; relaxed continues."""
        def build(app, runner):
            _write_job(
                runner, "root-1", "completed",
                summary={
                    "reconciliation": {"outcome": "duplicate_complete"},
                    "post_acquisition_reaudit": {"status": "pending"},
                },
            )
            runner.duplicate_complete_consumption_verified = (  # type: ignore[method-assign]
                lambda job_id: True
            )

        strict, relaxed = self._evaluate(build)
        self.assertFalse(strict)
        self.assertTrue(relaxed)

    def test_internal_child_matrix(self) -> None:
        def build_child(
            runner: SimpleEngineRunner,
            *,
            child_phase: str,
            linked: bool = True,
            child_summary: dict[str, object] | None = None,
        ) -> None:
            _write_job(
                runner, "root-1", "completed",
                summary=_settled_lifecycle_summary(),
            )
            summary: dict[str, object] = {"internal_child": True}
            if linked:
                summary["root_job_id"] = "root-1"
            summary.update(child_summary or {})
            _write_job(runner, "child-1", child_phase, summary=summary)

        cases: list[tuple[str, dict[str, object], bool, bool]] = [
            ("linked_executed", {"child_phase": "executed"}, True, True),
            ("linked_executing", {"child_phase": "executing"}, False, False),
            # The relaxed body allows only executed/completed/failed/
            # cancelled children, but the strict fast path already settled
            # this tree, so both stay observably open.
            ("linked_failed_write", {"child_phase": "failed_write"}, True, True),
            ("orphaned_terminal", {"child_phase": "executed", "linked": False}, False, False),
            (
                "linked_with_live_provider",
                {
                    "child_phase": "executed",
                    "child_summary": {
                        "replenishment": {"status": "acquiring", "terminal": False},
                    },
                },
                False, False,
            ),
        ]
        for name, kwargs, want_strict, want_relaxed in cases:
            with self.subTest(case=name):
                strict, relaxed = self._evaluate(
                    lambda app, runner, kwargs=kwargs: build_child(
                        runner, **kwargs,
                    ),
                )
                self.assertEqual(strict, want_strict)
                self.assertEqual(relaxed, want_relaxed)

    def test_unproven_intake_scan_closes_both_barriers(self) -> None:
        strict, relaxed = self._evaluate(
            lambda app, runner: _write_job(
                runner, "root-1", "completed",
                summary=_settled_lifecycle_summary(),
            ),
            scan_empty=False,
        )
        self.assertFalse(strict)
        self.assertFalse(relaxed)


class OneDirectoryAList:
    """A read-only intake root with one direct child directory."""

    def list(self, path: str, refresh: bool = False) -> list[dict[str, object]]:
        del refresh
        if path.rstrip("/").endswith("待刮削"):
            return [{"name": "Incoming", "is_dir": True}]
        return []


class IntakeVisibilityProjectionTests(unittest.TestCase):
    """Report-only Q-gate visibility; projections must never gate anything."""

    def _application(self, root: Path, *, remote: object | None = None):
        client = remote if remote is not None else EmptyAList()
        runner = SimpleEngineRunner(
            root, alist=client, tmdb=object(), validate=False,
            library_root="/library",
        )
        with patch.object(SimpleApplication, "_start_startup_thread"):
            app = SimpleApplication(
                state_root=root, remote_root="/library", remote=client,
                engine_runner=runner, enforce_engine_roots=False,
            )
        return app, runner

    def test_waiting_barrier_projects_blockers_and_deferred_gaps(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            app, runner = self._application(Path(directory))
            try:
                _write_job(runner, "u1", "reconciliation_uncertain")
                _write_job(runner, "a1", "executing")
                _write_job(runner, "f1", "failed")
                _write_job(runner, "r1", "retry_wait")
                _write_job(
                    runner, "c1", "executing",
                    summary={"internal_child": True},
                )
                _write_job(
                    runner, "e1", "completed",
                    summary={"reconciliation": {"outcome": "existing_gap"}},
                )
                app._intake_status["last_scan_empty"] = False  # noqa: SLF001
                app._refresh_intake_settlement()  # noqa: SLF001
                status = dict(app._intake_status)  # noqa: SLF001
                first_waiting_since = status["barrier_waiting_since"]
                # The waiting timestamp is sticky across refreshes while the
                # barrier stays closed; it records since-when, not last-seen.
                app._refresh_intake_settlement()  # noqa: SLF001
                second_waiting_since = app._intake_status[  # noqa: SLF001
                    "barrier_waiting_since"
                ]
            finally:
                app.close()
        self.assertEqual(status["barrier_blockers"], {
            "active_roots": 1,
            "uncertain_roots": 1,
            "failed_roots": 1,
            "retry_wait_roots": 1,
            "active_children": 1,
        })
        self.assertEqual(status["deferred_existing_gap_roots"], 1)
        self.assertIsNotNone(first_waiting_since)
        self.assertEqual(second_waiting_since, first_waiting_since)

    def test_settled_barrier_clears_waiting_projection(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            app, runner = self._application(Path(directory))
            try:
                _write_job(runner, "u1", "reconciliation_uncertain")
                app._intake_status["last_scan_empty"] = False  # noqa: SLF001
                app._refresh_intake_settlement()  # noqa: SLF001
                self.assertIsNotNone(
                    app._intake_status["barrier_waiting_since"],  # noqa: SLF001
                )
                (runner.jobs_root / "u1.json").unlink()
                app._intake_status["last_scan_empty"] = True  # noqa: SLF001
                app._refresh_intake_settlement()  # noqa: SLF001
                status = dict(app._intake_status)  # noqa: SLF001
            finally:
                app.close()
        self.assertTrue(status["settled"])
        self.assertIsNone(status["barrier_waiting_since"])
        self.assertIsNone(status["barrier_blockers"])
        self.assertEqual(status["deferred_existing_gap_roots"], 0)

    def test_backlog_warning_flags_persistently_nonempty_intake(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            app, runner = self._application(
                Path(directory), remote=OneDirectoryAList(),
            )
            try:
                with patch.dict(
                    "os.environ",
                    {"SCRAPEFLOW_INTAKE_BACKLOG_WARN_SCANS": "2"},
                ), patch.object(app, "_queue_automatic_job"):
                    app._scan_inbound_once()  # noqa: SLF001
                    self.assertEqual(
                        app._intake_status["nonempty_scan_streak"], 1,  # noqa: SLF001
                    )
                    self.assertFalse(
                        app._intake_status["intake_backlog_warning"],  # noqa: SLF001
                    )
                    app._scan_inbound_once()  # noqa: SLF001
                    self.assertEqual(
                        app._intake_status["nonempty_scan_streak"], 2,  # noqa: SLF001
                    )
                    self.assertTrue(
                        app._intake_status["intake_backlog_warning"],  # noqa: SLF001
                    )
                    # One empty observation resets the report-only streak.
                    runner.alist = EmptyAList()
                    app._scan_inbound_once()  # noqa: SLF001
                    self.assertEqual(
                        app._intake_status["nonempty_scan_streak"], 0,  # noqa: SLF001
                    )
                    self.assertFalse(
                        app._intake_status["intake_backlog_warning"],  # noqa: SLF001
                    )
            finally:
                app.close()


if __name__ == "__main__":
    unittest.main()
