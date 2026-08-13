from __future__ import annotations

from concurrent.futures import Future
import json
import os
from pathlib import Path
import tempfile
import threading
import unittest
from unittest.mock import patch

from engine.scrapeflow.serialization import atomic_write_json
from local.simple_server import ApplicationError, SimpleApplication
from local.scrapeflow_api.simple_engine_runner import (
    EngineJob,
    EngineRequestError,
    SimpleEngineRunner,
)


class EmptyAList:
    def list(self, _path: str, refresh: bool = False) -> list[dict[str, object]]:
        del refresh
        return []


class NfoAList:
    def __init__(self, tree: dict[str, list[dict[str, object]]], files: dict[str, bytes]) -> None:
        self.tree = tree
        self.files = files

    def list(self, path: str, refresh: bool = False) -> list[dict[str, object]]:
        del refresh
        return [dict(row) for row in self.tree.get(path, [])]

    def read_file_bytes(self, path: str, *, max_bytes: int) -> bytes:
        value = self.files[path]
        return value[:max_bytes]


def _gap(kind: str = "missing_episode") -> dict[str, object]:
    row: dict[str, object] = {
        "id": "audit-row-1",
        "kind": kind,
        "label": "Show S01E02" if kind == "missing_episode" else "Show",
        "reason": "not present",
        "source": "automatic_library_audit",
        "media": {
            "tmdb_id": 42,
            "title": "Show",
            "target_root": "/library/番剧/Show",
            "media_type": "tv",
        },
    }
    if kind == "missing_episode":
        row.update(season=1, episode=2)
    return row


def _subtitle_gap(
    *, tmdb_id: int = 42, target: str = "/library/番剧/Show",
    path: str | None = None, language: str = "zh",
) -> dict[str, object]:
    video = path or f"{target}/Season 01/Show S01E02.mkv"
    return {
        "id": f"missing_subtitle:{tmdb_id}:{video.rsplit('/', 1)[-1]}",
        "kind": "missing_subtitle",
        "label": video.rsplit("/", 1)[-1],
        "reason": "字幕探针确认视频缺少配置语言的内封或外挂字幕",
        "source": "automatic_library_audit",
        "media": {
            "tmdb_id": tmdb_id,
            "title": "Show",
            "target_root": target,
            "media_type": "tv",
        },
        "path": video,
        "subtitle_language": language,
    }


def _project(*gaps: dict[str, object]) -> dict[str, object]:
    return {
        "project_key": "tmdb:tv:42",
        "tmdb_id": 42,
        "target_root": "/library/番剧/Show",
        "gaps": list(gaps),
        "plan": {
            "mode": "tv",
            "target_root": "/library/番剧/Show",
            "metadata": {
                "tmdb_id": 42,
                "title": "Show",
                "media_type": "tv",
                "target_root": "/library/番剧/Show",
            },
            "scan_report": {"resource_gaps": list(gaps)},
        },
    }


def _project_for_tmdb(tmdb_id: int) -> dict[str, object]:
    """Build a second valid audit root for provider-pilot dispatch tests."""
    title = f"Show {tmdb_id}"
    target_root = f"/library/番剧/{title}"
    gap = _gap()
    gap.update({
        "id": f"audit-row-{tmdb_id}",
        "label": f"{title} S01E02",
    })
    media = dict(gap["media"])
    media.update({"tmdb_id": tmdb_id, "title": title, "target_root": target_root})
    gap["media"] = media
    return {
        "project_key": f"tmdb:tv:{tmdb_id}:{target_root}",
        "tmdb_id": tmdb_id,
        "target_root": target_root,
        "gaps": [gap],
        "plan": {
            "mode": "tv",
            "target_root": target_root,
            "metadata": {
                "tmdb_id": tmdb_id,
                "title": title,
                "media_type": "tv",
                "target_root": target_root,
            },
            "scan_report": {"resource_gaps": [gap]},
        },
    }


class AuditOwnedRootTests(unittest.TestCase):
    @staticmethod
    def _admit_provider(app: SimpleApplication) -> None:
        """Seed the explicit L-stage admission for provider-unit fixtures."""
        with app._automatic_lock:  # noqa: SLF001
            app._provider_full_audit_admitted = True  # noqa: SLF001

    def test_audit_owned_root_accepts_only_exact_subtitle_video_gap(self) -> None:
        target = "/library/番剧/Show"
        job = EngineJob(
            id="audit-subtitle-root", phase="executed",
            created_at="2026-08-08T00:00:00Z", updated_at="2026-08-08T00:00:00Z",
            request={},
            plan={"target_root": target, "metadata": {"tmdb_id": 7, "series_root": target}},
            summary={
                "audit_owned": True,
                "identity": {"tmdb_id": 7, "target_root": target},
            },
        )
        row = {
            "id": "missing_subtitle:7:S01E01:zh",
            "kind": "missing_subtitle",
            "label": "Show S01E01 中文字幕",
            "path": f"{target}/Season 01/Show S01E01.mkv",
            "subtitle_language": "zh",
            "media": {"tmdb_id": 7, "target_root": target, "media_type": "tv"},
        }
        self.assertTrue(SimpleApplication._audit_root_gap_is_safe(row, job))
        invalid = dict(row, path=f"{target}/Season 01/other.txt")
        self.assertFalse(SimpleApplication._audit_root_gap_is_safe(invalid, job))

    def test_audit_owned_movie_root_accepts_movie_subtitle_gap(self) -> None:
        target = "/library/电影/Movie (2020)"
        job = EngineJob(
            id="audit-movie-subtitle-root", phase="executed",
            created_at="2026-08-08T00:00:00Z", updated_at="2026-08-08T00:00:00Z",
            request={},
            plan={"target_root": target, "metadata": {"tmdb_id": 8, "media_type": "movie"}},
            summary={
                "audit_owned": True,
                "identity": {"tmdb_id": 8, "target_root": target, "media_type": "movie"},
            },
        )
        row = {
            "id": "missing_subtitle:8:movie:zh",
            "kind": "missing_subtitle",
            "label": "Movie (2020) 中文字幕",
            "path": f"{target}/Movie (2020).mkv",
            "subtitle_language": "zh",
            "media": {"tmdb_id": 8, "target_root": target, "media_type": "movie"},
        }
        self.assertTrue(SimpleApplication._audit_root_gap_is_safe(row, job))

    def test_root_is_local_only_idempotent_and_filters_non_media_gaps(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            planner_calls: list[object] = []
            executor_calls: list[object] = []
            runner = SimpleEngineRunner(
                Path(directory),
                alist=EmptyAList(),
                tmdb=object(),
                planner=lambda *args: planner_calls.append(args) or None,
                executor=lambda plan: executor_calls.append(plan) or {"moved": True},
                validate=False,
                library_root="/library",
            )
            project = _project(_gap(), _gap("missing_subtitle"))

            first = runner.create_audit_owned_root(project)
            second = runner.create_audit_owned_root(project)

            self.assertEqual(first.id, second.id)
            self.assertEqual(first.phase, "executed")
            self.assertTrue(first.summary["audit_owned"])
            self.assertIsNone(first.execution)
            self.assertEqual(
                [row["kind"] for row in first.plan["scan_report"]["resource_gaps"]],
                ["missing_episode"],
            )
            self.assertEqual(planner_calls, [])
            # An executed audit root is a state projection; calling the normal
            # execution entry point must not invoke the formal executor.
            self.assertEqual(runner.execute_job(first.id).id, first.id)
            self.assertEqual(executor_calls, [])
            self.assertEqual(len(runner.list_jobs()), 1)

    def test_root_rejects_identity_or_target_mismatch(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            runner = SimpleEngineRunner(
                Path(directory), alist=EmptyAList(), tmdb=object(),
                validate=False, library_root="/library",
            )
            project = _project(_gap())
            project["tmdb_id"] = 99
            with self.assertRaises(EngineRequestError):
                runner.create_audit_owned_root(project)
            project = _project(_gap())
            project["target_root"] = "/library/ScrapeFlow/补源/unsafe"
            with self.assertRaises(EngineRequestError):
                runner.create_audit_owned_root(project)
            project = _project(_gap())
            project["plan"] = {
                **project["plan"],
                "mode": "movie",
                "metadata": {**project["plan"]["metadata"], "media_type": "movie"},
            }
            with self.assertRaises(EngineRequestError):
                runner.create_audit_owned_root(project)
            self.assertEqual(runner.list_jobs(), [])

    def test_subtitle_only_root_is_idempotent_and_never_accepts_media_gap(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            planner_calls: list[object] = []
            executor_calls: list[object] = []
            runner = SimpleEngineRunner(
                Path(directory),
                alist=EmptyAList(),
                tmdb=object(),
                planner=lambda *args: planner_calls.append(args) or None,
                executor=lambda plan: executor_calls.append(plan) or {"moved": True},
                validate=False,
                library_root="/library",
            )
            subtitle = _subtitle_gap()
            project = {
                "project_key": "tmdb:tv:42:subtitle:zh",
                "subtitle_only": True,
                "tmdb_id": 42,
                "target_root": "/library/番剧/Show",
                "gaps": [subtitle],
                "plan": {
                    "mode": "tv",
                    "target_root": "/library/番剧/Show",
                    "metadata": {
                        "tmdb_id": 42,
                        "title": "Show",
                        "media_type": "tv",
                        "target_root": "/library/番剧/Show",
                    },
                    "scan_report": {"resource_gaps": [subtitle]},
                },
            }
            first = runner.create_audit_owned_subtitle_root(project)
            second = runner.create_audit_owned_subtitle_root(project)
            self.assertEqual(first.id, second.id)
            self.assertTrue(first.summary["audit_subtitle_only"])
            self.assertEqual(
                [row["kind"] for row in first.plan["scan_report"]["resource_gaps"]],
                ["missing_subtitle"],
            )
            self.assertIsNone(first.execution)
            self.assertEqual(planner_calls, [])
            self.assertEqual(executor_calls, [])
            bad = dict(project)
            bad["gaps"] = [dict(subtitle, kind="missing_episode")]
            bad["plan"] = {
                **project["plan"],
                "scan_report": {"resource_gaps": bad["gaps"]},
            }
            with self.assertRaises(EngineRequestError):
                runner.create_audit_owned_subtitle_root(bad)
            self.assertEqual(len(runner.list_jobs()), 1)

    def test_nfo_subtitle_gap_bootstraps_paused_owner_without_dispatch(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            remote = EmptyAList()
            runner = SimpleEngineRunner(
                root, alist=remote, tmdb=object(), validate=False, library_root="/library",
            )
            app = SimpleApplication(
                state_root=root, remote_root="/library", remote=remote,
                engine_runner=runner, enforce_engine_roots=False,
            )
            app.set_paused(True, "read-only")
            subtitle = _subtitle_gap()
            semantic = {
                "gaps": [subtitle],
                "unknowns": [],
                "works": [{
                    "work": "tmdb:42",
                    "target_root": "/library/番剧/Show",
                    "identity_sources": ["library_nfo"],
                    "gaps": [subtitle],
                }],
                "acquisition_projects": [],
            }
            try:
                with patch.object(app, "_queue_provider_job") as provider_queue:
                    app._apply_audit_gaps({"semantic": semantic}, runner)
                    provider_queue.assert_not_called()
                jobs = runner.list_jobs()
                self.assertEqual(len(jobs), 1)
                job = jobs[0]
                self.assertTrue(job.summary["audit_subtitle_only"])
                self.assertEqual(
                    job.plan["scan_report"]["resource_gaps"], [subtitle],
                )
                self.assertEqual(app._provider_futures, {})
                # Reapplying the same report must reuse the same owner.
                app._apply_audit_gaps({"semantic": semantic}, runner)
                self.assertEqual([item.id for item in runner.list_jobs()], [job.id])
            finally:
                app.close()

    def test_untrusted_or_out_of_scope_subtitle_gap_never_bootstraps_owner(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            remote = EmptyAList()
            runner = SimpleEngineRunner(
                root, alist=remote, tmdb=object(), validate=False, library_root="/library",
            )
            app = SimpleApplication(
                state_root=root, remote_root="/library", remote=remote,
                engine_runner=runner, enforce_engine_roots=False,
            )
            app.set_paused(True, "read-only")
            subtitle = _subtitle_gap(path="/library/番剧/Other/Season 01/Show S01E02.mkv")
            semantic = {
                "gaps": [subtitle], "unknowns": [],
                "works": [{
                    "work": "tmdb:42", "target_root": "/library/番剧/Show",
                    "identity_sources": ["engine_job"], "gaps": [subtitle],
                }],
                "acquisition_projects": [],
            }
            try:
                app._apply_audit_gaps({"semantic": semantic}, runner)
                self.assertEqual(runner.list_jobs(), [])
            finally:
                app.close()

    def test_application_attaches_audit_root_and_clears_it_after_fresh_scan(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            remote = EmptyAList()
            runner = SimpleEngineRunner(
                root, alist=remote, tmdb=object(), validate=False, library_root="/library",
            )
            app = SimpleApplication(
                state_root=root,
                remote_root="/library",
                remote=remote,
                engine_runner=runner,
                enforce_engine_roots=False,
            )
            app.set_paused(True, "test")
            try:
                gap = _gap()
                app._apply_audit_gaps(
                    {"semantic": {
                        "gaps": [gap], "unknowns": [],
                        "acquisition_projects": [_project(gap)],
                    }},
                    runner,
                )
                jobs = runner.list_jobs()
                self.assertEqual(len(jobs), 1)
                self.assertTrue(jobs[0].summary["audit_owned"])
                # The global pause blocks the provider queue; the gap remains
                # durably attached and therefore the public projection stays
                # non-green until resume.
                self.assertNotIn("replenishment", jobs[0].summary)
                self.assertEqual(SimpleApplication.public_engine_job(jobs[0])["phase"], "gap_discovering")

                app._apply_audit_gaps(
                    {"semantic": {"gaps": [], "unknowns": [], "acquisition_projects": []}},
                    runner,
                )
                cleared = runner.list_jobs()[0]
                self.assertEqual(cleared.plan["scan_report"]["resource_gaps"], [])
                self.assertIsNone(cleared.summary.get("audit"))
            finally:
                app.close()

    def test_subtitle_probe_unknown_stays_non_green_without_periodic_audit_storm(self) -> None:
        """A bounded subtitle probe must wait for fresh media/manual audit."""
        with tempfile.TemporaryDirectory() as directory, patch.dict(
            os.environ,
            {
                "SCRAPEFLOW_START_PAUSED": "1",
                "SCRAPEFLOW_AUTOMATIC_AUDIT": "0",
                "SCRAPEFLOW_INTAKE_MONITOR": "0",
            },
            clear=False,
        ):
            root = Path(directory)
            remote = EmptyAList()
            runner = SimpleEngineRunner(
                root, alist=remote, tmdb=object(), validate=False, library_root="/library",
            )
            target = "/library/番剧/Show"
            video = f"{target}/Season 01/Show S01E01.mkv"
            job = EngineJob(
                id="engine-subtitle-unknown",
                phase="executed",
                created_at="2026-08-08T00:00:00Z",
                updated_at="2026-08-08T00:00:00Z",
                request={"source_path": "/library/待刮削/Show"},
                plan={
                    "mode": "tv", "target_root": target,
                    "metadata": {"tmdb_id": 42, "title": "Show", "media_type": "tv"},
                    "scan_report": {"resource_gaps": []},
                },
                summary={"identity": {
                    "tmdb_id": 42, "media_type": "tv", "target_root": target,
                }},
            )
            atomic_write_json(runner.jobs_root / f"{job.id}.json", job.as_dict(), allow_nan=False)
            app = SimpleApplication(
                state_root=root, remote_root="/library", remote=remote,
                engine_runner=runner, enforce_engine_roots=False,
            )
            unknown = {
                "kind": "unknown_subtitle_evidence",
                "work": "tmdb:42",
                "target_root": target,
                "path": video,
                "media": {"tmdb_id": 42, "target_root": target, "media_type": "tv"},
            }
            try:
                subtitle_unknowns = [unknown for _ in range(1420)]
                with patch.object(app, "_queue_library_audit") as audit_queue:
                    app._apply_audit_gaps(
                        {"semantic": {
                            "gaps": [], "unknowns": subtitle_unknowns,
                            "acquisition_projects": [],
                        }},
                        runner,
                    )
                persisted = runner.get_job(job.id)
                audit = persisted.summary["audit"]
                self.assertEqual(audit["status"], "unknown")
                self.assertTrue(audit["retryable"])
                self.assertFalse(audit["automatic_retry"])
                self.assertIn("新媒体提交或手动审计", audit["message"])
                self.assertFalse(app._audit_needs_retry(persisted))
                self.assertEqual(SimpleApplication.public_engine_job(persisted)["phase"], "failed_verification")
                audit_queue.assert_not_called()
            finally:
                app.close()

        # Existing persisted reports predate ``automatic_retry``.  They must
        # also avoid recreating the scan loop on the first resume.
        legacy = EngineJob(
            id="engine-legacy-subtitle-unknown",
            phase="executed",
            created_at="2026-08-08T00:00:00Z",
            updated_at="2026-08-08T00:00:00Z",
            request={}, plan={},
            summary={"audit": {"status": "unknown", "unknowns": [unknown]}},
        )
        self.assertFalse(SimpleApplication._audit_needs_retry(legacy))

    def test_tmdb_unknowns_stay_non_green_without_periodic_audit_retry(self) -> None:
        job = EngineJob(
            id="engine-unknown-catalog",
            phase="executed",
            created_at="2026-08-08T00:00:00Z",
            updated_at="2026-08-08T00:00:00Z",
            request={}, plan={},
            summary={"audit": {
                "status": "unknown",
                "unknowns": [
                    {"kind": "unknown_episode_catalog"},
                    {"kind": "unknown_library_work"},
                ],
            }},
        )
        self.assertFalse(SimpleApplication._audit_needs_retry(job))

    def test_tmdb_unknown_projection_does_not_queue_audit_heartbeat(self) -> None:
        """Catalog/identity uncertainty remains unknown without a scan storm."""
        with tempfile.TemporaryDirectory() as directory, patch.dict(
            os.environ,
            {
                "SCRAPEFLOW_START_PAUSED": "1",
                "SCRAPEFLOW_AUTOMATIC_AUDIT": "0",
                "SCRAPEFLOW_INTAKE_MONITOR": "0",
            },
            clear=False,
        ):
            root = Path(directory)
            remote = EmptyAList()
            runner = SimpleEngineRunner(
                root, alist=remote, tmdb=object(), validate=False, library_root="/library",
            )
            target = "/library/番剧/Unknown Show"
            job = EngineJob(
                id="engine-unknown-tmdb",
                phase="executed",
                created_at="2026-08-08T00:00:00Z",
                updated_at="2026-08-08T00:00:00Z",
                request={},
                plan={
                    "mode": "tv",
                    "target_root": target,
                    "metadata": {"tmdb_id": 42, "title": "Unknown Show", "media_type": "tv"},
                    "scan_report": {"resource_gaps": []},
                },
                summary={"identity": {
                    "tmdb_id": 42, "media_type": "tv", "target_root": target,
                }},
            )
            atomic_write_json(runner.jobs_root / f"{job.id}.json", job.as_dict(), allow_nan=False)
            app = SimpleApplication(
                state_root=root, remote_root="/library", remote=remote,
                engine_runner=runner, enforce_engine_roots=False,
            )
            unknowns = [
                {
                    "kind": "unknown_episode_catalog",
                    "work": "tmdb:42",
                    "target_root": target,
                    "media": {"tmdb_id": 42, "target_root": target, "media_type": "tv"},
                },
                {
                    "kind": "unknown_library_work",
                    "work": "tmdb:42",
                    "target_root": target,
                    "media": {"tmdb_id": 42, "target_root": target, "media_type": "tv"},
                },
            ]
            try:
                with patch.object(app, "_queue_library_audit") as audit_queue:
                    app._apply_audit_gaps(
                        {"semantic": {
                            "gaps": [], "unknowns": unknowns,
                            "acquisition_projects": [],
                        }},
                        runner,
                    )
                persisted = runner.get_job(job.id)
                audit = persisted.summary["audit"]
                self.assertEqual(audit["status"], "unknown")
                self.assertFalse(audit["automatic_retry"])
                self.assertIn("新媒体提交或手动审计", audit["message"])
                self.assertFalse(app._audit_needs_retry(persisted))
                self.assertEqual(SimpleApplication.public_engine_job(persisted)["phase"], "failed_verification")
                audit_queue.assert_not_called()
            finally:
                app.close()

    def test_other_unknown_evidence_keeps_bounded_automatic_retry(self) -> None:
        job = EngineJob(
            id="engine-unknown-other",
            phase="executed",
            created_at="2026-08-08T00:00:00Z",
            updated_at="2026-08-08T00:00:00Z",
            request={}, plan={},
            summary={"audit": {
                "status": "unknown",
                "unknowns": [{"kind": "unknown_identity_evidence"}],
            }},
        )
        self.assertTrue(SimpleApplication._audit_needs_retry(job))

    def test_only_exact_subtitle_batch_deferred_unknown_advances_ledger_audit(self) -> None:
        def result(*unknowns: object) -> dict[str, object]:
            return {"audit": {"semantic": {"unknowns": list(unknowns)}}}

        deferred = {
            "kind": "unknown_subtitle_evidence",
            "reason": "subtitle_probe_batch_deferred",
        }
        self.assertTrue(
            SimpleApplication._audit_result_needs_subtitle_batch_continuation(
                result({"kind": "unknown_episode_catalog"}, deferred),
            )
        )
        for unknown in (
            {"kind": "unknown_subtitle_evidence", "reason": "subtitle_probe_error"},
            {"kind": "unknown_subtitle_evidence", "reason": "subtitle_probe_budget_exhausted"},
            {"kind": "ambiguous_tv_container", "reason": "subtitle_probe_batch_deferred"},
            {"kind": "unknown_episode_catalog", "reason": "subtitle_probe_batch_deferred"},
            {"kind": "unknown_subtitle_evidence"},
        ):
            with self.subTest(unknown=unknown):
                self.assertFalse(
                    SimpleApplication._audit_result_needs_subtitle_batch_continuation(
                        result(unknown),
                    )
                )

    def test_manual_full_audit_reports_nfo_identity_without_creating_provider_work(self) -> None:
        movie_root = "/library/电影"
        movie = f"{movie_root}/Movie (2020)"
        nfo = f"{movie}/movie.nfo"
        remote = NfoAList(
            {
                movie_root: [{"name": "Movie (2020)", "is_dir": True}],
                movie: [{"name": "movie.nfo", "is_dir": False, "size": 42}],
                "/library/番剧": [],
                "/library/美剧": [],
            },
            {nfo: b"<movie><title>Movie</title><tmdbid>42</tmdbid></movie>"},
        )
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            runner = SimpleEngineRunner(
                root, alist=remote, tmdb=object(), validate=False, library_root="/library",
            )
            app = SimpleApplication(
                state_root=root, remote_root="/library", remote=remote,
                engine_runner=runner, enforce_engine_roots=False,
            )
            app.set_paused(True, "read-only scan")
            try:
                with patch.object(app, "_apply_audit_gaps") as apply_gaps:
                    result = app.run_library_audit()
                self.assertEqual(result["audit"]["semantic"]["gap_count"], 1)
                # The public manual route can save its local report, but it
                # must not project that report into an audit-owned provider
                # root or lifecycle/cleanup decision.
                apply_gaps.assert_not_called()
                self.assertEqual(runner.list_jobs(), [])
            finally:
                app.close()

    def test_audit_keeps_cleanup_pending_for_terminal_or_disabled_provider_gap(self) -> None:
        """A retained gap must fence cleanup regardless of Provider policy."""
        for label, terminal, provider_enabled, expected_status, expected_reason in (
            (
                "terminal",
                True,
                True,
                "terminal",
                "provider_terminal_with_remaining_gap",
            ),
            (
                "disabled",
                False,
                False,
                "deferred",
                "provider_auto_repair_disabled_with_remaining_gap",
            ),
        ):
            with self.subTest(case=label), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                remote = EmptyAList()
                runner = SimpleEngineRunner(
                    root, alist=remote, tmdb=object(), validate=False, library_root="/library",
                )
                gap = _gap()
                replenishment: dict[str, object] = {}
                if terminal:
                    replenishment = {
                        "status": "failed",
                        "terminal": True,
                        "attempts": 5,
                        "gap_signature": SimpleApplication._provider_gap_signature([gap]),
                    }
                job = EngineJob(
                    id=f"engine-cleanup-{label}",
                    phase="executed",
                    created_at="2026-08-08T00:00:00Z",
                    updated_at="2026-08-08T00:00:00Z",
                    request={"source_path": "/library/待刮削/Show"},
                    plan={
                        "mode": "tv",
                        "target_root": "/library/番剧/Show",
                        "metadata": {
                            "tmdb_id": 42,
                            "title": "Show",
                            "media_type": "tv",
                            "target_root": "/library/番剧/Show",
                        },
                        "scan_report": {"resource_gaps": []},
                    },
                    summary={
                        "automatic": True,
                        "identity": {
                            "tmdb_id": 42,
                            "media_type": "tv",
                            "target_root": "/library/番剧/Show",
                        },
                        "replenishment": replenishment,
                        "lifecycle": {
                            "formal_write": {"status": "verified"},
                            "cleanup": {"status": "pending"},
                        },
                    },
                )
                atomic_write_json(runner.jobs_root / f"{job.id}.json", job.as_dict(), allow_nan=False)
                app = SimpleApplication(
                    state_root=root, remote_root="/library", remote=remote,
                    engine_runner=runner, enforce_engine_roots=False,
                )
                app.set_paused(True, "keep provider queue out of this lifecycle test")
                try:
                    with patch.object(
                        app,
                        "_provider_auto_repair_enabled",
                        return_value=provider_enabled,
                    ), patch.object(runner, "finalize_automatic_lifecycle") as finalizer:
                        app._apply_audit_gaps(
                            {"semantic": {
                                "gaps": [gap], "unknowns": [], "acquisition_projects": [],
                            }},
                            runner,
                        )

                    persisted = runner.get_job(job.id)
                    lifecycle = persisted.summary["lifecycle"]
                    self.assertFalse(lifecycle["cleanup_ready"])
                    self.assertEqual(lifecycle["provider"]["status"], expected_status)
                    self.assertEqual(lifecycle["provider"]["reason"], expected_reason)
                    finalizer.assert_not_called()
                finally:
                    app.close()

    def test_terminal_identity_retry_does_not_start_global_audit(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            runner = SimpleEngineRunner(
                root, alist=EmptyAList(), tmdb=object(), validate=False,
                library_root="/library",
            )
            app = SimpleApplication(
                state_root=root, remote_root="/library", remote=EmptyAList(),
                engine_runner=runner, enforce_engine_roots=False,
            )
            app.set_paused(True, "test")
            try:
                queued = runner.create_automatic_job("/library/待刮削/Show")
                with patch.dict(os.environ, {"SCRAPEFLOW_AUTOMATIC_RETRY_LIMIT": "0"}), \
                     patch.object(app, "_queue_library_audit") as audit_queue:
                    app._record_automatic_retry(
                        queued.id, RuntimeError("TMDB temporarily unavailable"), stage="identity",
                    )
                failed = runner.get_job(queued.id)
                self.assertEqual(failed.phase, "failed_identity")
                # No trusted work root exists after identity failure. Automatic
                # audit is therefore fail-closed; an operator may request a
                # deliberate scoped/global audit explicitly.
                audit_queue.assert_not_called()
            finally:
                app.close()

    def test_audit_never_runs_metadata_repair_directly(self) -> None:
        for paused in (True, False):
            with self.subTest(paused=paused), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                remote = EmptyAList()
                runner = SimpleEngineRunner(
                    root, alist=remote, tmdb=object(), validate=False, library_root="/library",
                )
                job = EngineJob(
                    id="engine-existing-show",
                    phase="executed",
                    created_at="2026-08-07T00:00:00Z",
                    updated_at="2026-08-07T00:00:00Z",
                    request={"source_path": "/library/待刮削/Show"},
                    plan={
                        "mode": "tv", "target_root": "/library/番剧/Show",
                        "metadata": {"tmdb_id": 42, "title": "Show"},
                        "scan_report": {"resource_gaps": []},
                    },
                    summary={"identity": {
                        "tmdb_id": 42, "media_type": "tv", "target_root": "/library/番剧/Show",
                    }},
                )
                atomic_write_json(runner.jobs_root / f"{job.id}.json", job.as_dict(), allow_nan=False)
                app = SimpleApplication(
                    state_root=root, remote_root="/library", remote=remote,
                    engine_runner=runner, enforce_engine_roots=False,
                )
                if paused:
                    app.set_paused(True, "read-only scan")
                else:
                    with patch.object(app, "_start_startup_thread"):
                        app.set_paused(False)
                calls: list[str] = []
                runner.repair_automatic_artifacts = lambda job_id: calls.append(job_id)  # type: ignore[method-assign]
                repair_gap = {
                    "id": "missing_nfo:42:Show", "kind": "missing_nfo", "label": "Show",
                    "reason": "missing", "source": "automatic_library_audit",
                    "media": {
                        "tmdb_id": 42, "target_root": "/library/番剧/Show", "media_type": "tv",
                    },
                }
                try:
                    with patch.object(app, "_queue_library_audit") as audit_queue:
                        app._apply_audit_gaps(
                            {"semantic": {"gaps": [repair_gap], "unknowns": [], "acquisition_projects": []}},
                            runner,
                        )
                    persisted = runner.get_job(job.id)
                    audit = persisted.summary["audit"]
                    self.assertEqual(calls, [])
                    self.assertEqual(audit["status"], "blocked")
                    self.assertFalse(audit["automatic_retry"])
                    self.assertFalse(SimpleApplication._audit_needs_retry(persisted))
                    audit_queue.assert_not_called()
                finally:
                    app.close()

    def test_provider_pilot_dispatches_only_the_matching_audit_root(self) -> None:
        with tempfile.TemporaryDirectory() as directory, patch.dict(
            os.environ,
            {
                "SCRAPEFLOW_START_PAUSED": "1",
                "SCRAPEFLOW_PROVIDER_PILOT_TMDB": "42",
            },
            clear=False,
        ):
            root = Path(directory)
            remote = EmptyAList()
            runner = SimpleEngineRunner(
                root, alist=remote, tmdb=object(), validate=False, library_root="/library",
            )
            app = SimpleApplication(
                state_root=root, remote_root="/library", remote=remote,
                engine_runner=runner, enforce_engine_roots=False,
            )
            matching = runner.create_audit_owned_root(_project(_gap()))
            nonmatching = runner.create_audit_owned_root(_project_for_tmdb(99))
            called: list[str] = []
            ran = threading.Event()
            try:
                with patch.object(
                    app,
                    "_run_automatic_replenishment",
                    side_effect=lambda job_id: (called.append(job_id), ran.set()),
                ), patch.object(app, "_start_startup_thread"):
                    app.set_paused(False)
                    self._admit_provider(app)
                    app._resume_automatic_jobs()
                    self.assertTrue(ran.wait(2.0))

                self.assertEqual(called, [matching.id])
                self.assertNotIn(nonmatching.id, app._provider_futures)
                deferred = runner.get_job(nonmatching.id)
                self.assertNotIn("replenishment", deferred.summary)
                self.assertEqual(
                    SimpleApplication.public_engine_job(deferred)["phase"],
                    "gap_discovering",
                )
            finally:
                app.close()

    def test_provider_pilot_gap_projects_one_coordinate_only(self) -> None:
        first = _gap()
        first.update({"id": "missing_episode:42:S00E04", "season": 0, "episode": 4})
        second = _gap()
        second.update({"id": "missing_episode:42:S00E06", "season": 0, "episode": 6})
        with tempfile.TemporaryDirectory() as directory, patch.dict(
            os.environ,
            {
                "SCRAPEFLOW_START_PAUSED": "1",
                "SCRAPEFLOW_PROVIDER_PILOT_TMDB": "42",
                "SCRAPEFLOW_PROVIDER_PILOT_GAP": "S00E04",
            },
            clear=False,
        ):
            root = Path(directory)
            remote = EmptyAList()
            runner = SimpleEngineRunner(
                root, alist=remote, tmdb=object(), validate=False, library_root="/library",
            )
            app = SimpleApplication(
                state_root=root, remote_root="/library", remote=remote,
                engine_runner=runner, enforce_engine_roots=False,
            )
            try:
                job = runner.create_audit_owned_root(_project(first, second))
                self.assertTrue(SimpleApplication._provider_job_allowed(job))
                projected = SimpleApplication._provider_pilot_job(job)
                rows = projected.plan["scan_report"]["resource_gaps"]
                self.assertEqual([row["id"] for row in rows], [first["id"]])
            finally:
                app.close()

    def test_queued_provider_worker_rechecks_pause_and_pilot_before_runtime(self) -> None:
        with tempfile.TemporaryDirectory() as directory, patch.dict(
            os.environ,
            {
                "SCRAPEFLOW_START_PAUSED": "1",
                "SCRAPEFLOW_PROVIDER_PILOT_TMDB": "42",
            },
            clear=False,
        ):
            root = Path(directory)
            remote = EmptyAList()
            runner = SimpleEngineRunner(
                root, alist=remote, tmdb=object(), validate=False, library_root="/library",
            )
            app = SimpleApplication(
                state_root=root, remote_root="/library", remote=remote,
                engine_runner=runner, enforce_engine_roots=False,
            )
            matching = runner.create_audit_owned_root(_project(_gap()))
            try:
                with patch.object(app, "_get_automatic_replenishment") as runtime:
                    # A future that begins after a global pause must exit before
                    # runtime construction or any provider request.
                    app._run_automatic_replenishment(matching.id)
                    runtime.assert_not_called()

                with patch.object(app, "_start_startup_thread"):
                    app.set_paused(False)
                self._admit_provider(app)
                with patch.dict(
                    os.environ,
                    {"SCRAPEFLOW_PROVIDER_PILOT_TMDB": "99"},
                    clear=False,
                ), patch.object(app, "_get_automatic_replenishment") as runtime:
                    # A stale queued future must also honor a changed pilot
                    # selector instead of invoking the former root.
                    app._run_automatic_replenishment(matching.id)
                    runtime.assert_not_called()
            finally:
                app.close()

    def test_provider_future_grant_epoch_blocks_reopened_token(self) -> None:
        """A Future admitted by an old L audit cannot use a reopened token."""
        with tempfile.TemporaryDirectory() as directory, patch.dict(
            os.environ,
            {"SCRAPEFLOW_START_PAUSED": "1", "SCRAPEFLOW_PROVIDER_PILOT_TMDB": "42"},
            clear=False,
        ):
            root = Path(directory)
            remote = EmptyAList()
            runner = SimpleEngineRunner(
                root, alist=remote, tmdb=object(), validate=False, library_root="/library",
            )
            app = SimpleApplication(
                state_root=root, remote_root="/library", remote=remote,
                engine_runner=runner, enforce_engine_roots=False,
            )
            job = runner.create_audit_owned_root(_project(_gap()))
            pending: Future[object] = Future()
            captured: list[tuple[object, tuple[object, ...]]] = []

            class Pool:
                def submit(self, callback: object, *args: object, **_kwargs: object) -> Future[object]:
                    captured.append((callback, args))
                    return pending

            try:
                with patch.object(app, "_start_startup_thread"), patch.object(
                    app, "_scan_inbound_once", return_value=[],
                ):
                    app.set_paused(False)
                self._admit_provider(app)
                with patch.object(app, "_provider_pool", return_value=Pool()), patch.object(
                    app, "_schedule_timer",
                    side_effect=lambda _lane, _owner, _delay, callback: callback(),
                ):
                    app._queue_provider_job(job.id)
                self.assertEqual(len(captured), 1)
                callback, args = captured[0]
                self.assertEqual(args, (job.id,))
                with app._automatic_lock:  # noqa: SLF001
                    old_epoch = app._provider_submission_grants[job.id]  # noqa: SLF001
                    app._provider_admission_epoch = old_epoch + 1  # noqa: SLF001
                    # Simulate a fresh L pass reopening the bool token.
                    app._provider_full_audit_admitted = True  # noqa: SLF001
                with patch.object(app, "_get_automatic_replenishment") as runtime:
                    callback(*args)  # type: ignore[operator]
                runtime.assert_not_called()
                self.assertEqual(
                    runner.get_job(job.id).summary["replenishment"]["status"],
                    "retry_wait",
                )
            finally:
                app.close()

    def test_provider_worker_rechecks_admission_before_runtime_call(self) -> None:
        """Revocation after worker entry must still block the Provider call."""
        with tempfile.TemporaryDirectory() as directory, patch.dict(
            os.environ,
            {"SCRAPEFLOW_START_PAUSED": "1", "SCRAPEFLOW_PROVIDER_PILOT_TMDB": "42"},
            clear=False,
        ):
            root = Path(directory)
            remote = EmptyAList()
            runner = SimpleEngineRunner(
                root, alist=remote, tmdb=object(), validate=False, library_root="/library",
            )
            app = SimpleApplication(
                state_root=root, remote_root="/library", remote=remote,
                engine_runner=runner, enforce_engine_roots=False,
            )
            job = runner.create_audit_owned_root(_project(_gap()))
            calls: list[str] = []

            class Runtime:
                def run_for_job(self, provider_job: EngineJob) -> dict[str, object]:
                    calls.append(provider_job.id)
                    return {"outcomes": [], "unresolved_gaps": []}

            def runtime_after_revoke() -> Runtime:
                with app._automatic_lock:  # noqa: SLF001
                    app._provider_full_audit_admitted = False  # noqa: SLF001
                    app._provider_admission_epoch += 1  # noqa: SLF001
                return Runtime()

            try:
                with patch.object(app, "_start_startup_thread"):
                    app.set_paused(False)
                self._admit_provider(app)
                with app._automatic_lock:  # noqa: SLF001
                    app._provider_submission_grants[job.id] = app._provider_admission_epoch  # noqa: SLF001
                with patch.object(
                    app, "_get_automatic_replenishment", side_effect=runtime_after_revoke,
                ):
                    app._run_automatic_replenishment(job.id)  # noqa: SLF001
                self.assertEqual(calls, [])
                self.assertEqual(
                    runner.get_job(job.id).summary["replenishment"]["status"],
                    "retry_wait",
                )
            finally:
                app.close()

    def test_full_audit_keeps_provider_token_closed_during_gap_projection(self) -> None:
        """L must finish its local gap projection before publishing M admission."""
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            remote = EmptyAList()
            runner = SimpleEngineRunner(
                root, alist=remote, tmdb=object(), validate=False, library_root="/library",
            )
            app = SimpleApplication(
                state_root=root, remote_root="/library", remote=remote,
                engine_runner=runner, enforce_engine_roots=False,
            )
            observed: list[bool] = []
            future: Future[object] = Future()
            future.set_result({
                "audit": {
                    "status": "completed",
                    "complete": True,
                    "semantic": {"gaps": [], "unknowns": [], "acquisition_projects": []},
                },
            })

            class Pool:
                def submit(self, *_args: object, **_kwargs: object) -> Future[object]:
                    return future

            try:
                with patch.object(app, "control", return_value={"paused": False}), patch.object(
                    app, "_audit_auto_repair_enabled", return_value=True,
                ), patch.object(
                    app, "_full_audit_ready_for_provider", return_value=True,
                ), patch.object(app, "_audit_pool", return_value=Pool()), patch.object(
                    app, "_apply_audit_gaps",
                    side_effect=lambda *_args, **_kwargs: (
                        observed.append(app._provider_submission_admitted()),  # noqa: SLF001
                        (),
                    )[1],
                ), patch.object(
                    app, "_schedule_timer",
                    side_effect=lambda _lane, _owner, _delay, callback: callback(),
                ):
                    app._intake_status.update({  # noqa: SLF001
                        "last_scan_empty": True,
                        "full_audit_barrier": "ready",
                    })
                    app._queue_intake_settled_audit()  # noqa: SLF001

                self.assertEqual(observed, [False])
                self.assertTrue(app._provider_submission_admitted())  # noqa: SLF001
                self.assertEqual(app._intake_status["full_audit_barrier"], "completed")  # noqa: SLF001
            finally:
                app.close()

    def test_full_audit_barrier_blocks_durable_pending_reaudit_state(self) -> None:
        """A stale local gap marker keeps L closed even when the root says executed."""
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            remote = EmptyAList()
            runner = SimpleEngineRunner(
                root, alist=remote, tmdb=object(), validate=False, library_root="/library",
            )
            with patch.object(SimpleApplication, "_start_startup_thread"):
                app = SimpleApplication(
                    state_root=root, remote_root="/library", remote=remote,
                    engine_runner=runner, enforce_engine_roots=False,
                )
            job = runner.create_audit_owned_root(_project(_gap()))
            gap_dir = root / "gaps" / job.id
            gap_dir.mkdir(parents=True, exist_ok=True)
            atomic_write_json(
                gap_dir / "audit-row-1.json",
                {
                    "id": "audit-row-1",
                    "phase": "resolved",
                    "post_acquisition_reaudit": {"status": "pending"},
                },
                allow_nan=False,
            )
            try:
                app._intake_status["last_scan_empty"] = True  # noqa: SLF001
                self.assertTrue(runner.has_pending_replenishment_reaudit(job.id))
                self.assertFalse(app._intake_is_settled())  # noqa: SLF001
                self.assertFalse(app._full_audit_ready_for_provider())  # noqa: SLF001
            finally:
                app.close()

    def test_full_audit_barrier_blocks_active_internal_child(self) -> None:
        """An executing provider child is still an L-stage external side effect."""
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            remote = EmptyAList()
            runner = SimpleEngineRunner(
                root, alist=remote, tmdb=object(), validate=False, library_root="/library",
            )
            with patch.object(SimpleApplication, "_start_startup_thread"):
                app = SimpleApplication(
                    state_root=root, remote_root="/library", remote=remote,
                    engine_runner=runner, enforce_engine_roots=False,
                )
            job = runner.create_audit_owned_root(_project(_gap()))
            child = EngineJob(
                id="child-active-barrier",
                phase="executing",
                created_at=job.created_at,
                updated_at=job.updated_at,
                request={},
                plan={},
                summary={"internal_child": True, "root_job_id": job.id},
            )
            atomic_write_json(
                runner.jobs_root / f"{child.id}.json",
                child.as_dict(),
                allow_nan=False,
            )
            try:
                app._intake_status["last_scan_empty"] = True  # noqa: SLF001
                self.assertFalse(app._intake_is_settled())  # noqa: SLF001
                self.assertFalse(app._full_audit_ready_for_provider())  # noqa: SLF001
            finally:
                app.close()

    def test_terminal_provider_gap_blocks_stale_worker_and_timer(self) -> None:
        """Retry exhaustion is a durable stop boundary for the same gap."""
        with tempfile.TemporaryDirectory() as directory, patch.dict(
            os.environ, {"SCRAPEFLOW_START_PAUSED": "1", "SCRAPEFLOW_PROVIDER_PILOT_TMDB": "42"}, clear=False,
        ):
            root = Path(directory)
            remote = EmptyAList()
            runner = SimpleEngineRunner(
                root, alist=remote, tmdb=object(), validate=False, library_root="/library",
            )
            app = SimpleApplication(
                state_root=root, remote_root="/library", remote=remote,
                engine_runner=runner, enforce_engine_roots=False,
            )
            job = runner.create_audit_owned_root(_project(_gap()))
            try:
                signature = SimpleApplication._provider_gap_signature([_gap()])
                app._record_replenishment_summary(job, {
                    "status": "failed", "terminal": True, "attempts": 5,
                    "gap_signature": signature, "error": "retry exhausted",
                })
                terminal = runner.get_job(job.id)
                self.assertFalse(SimpleApplication._provider_job_allowed(terminal))
                with patch.object(app, "_get_automatic_replenishment") as runtime, \
                     patch("local.simple_server.threading.Timer") as timer:
                    app._run_automatic_replenishment(job.id)
                    app._queue_provider_job(job.id, delay=30.0)
                    runtime.assert_not_called()
                    timer.assert_not_called()
                persisted = runner.get_job(job.id)
                self.assertTrue(persisted.summary["replenishment"]["terminal"])
                self.assertEqual(persisted.summary["replenishment"]["gap_signature"], signature)
                self.assertEqual(persisted.summary["replenishment_attempts"], 5)
            finally:
                app.close()

    def test_provider_gap_signature_is_order_independent_and_changes_on_gap(self) -> None:
        first = _gap()
        second = dict(first, id="audit-row-2", season=1, episode=3)
        a = SimpleApplication._provider_gap_signature([first, second])
        b = SimpleApplication._provider_gap_signature([second, first])
        self.assertEqual(a, b)
        self.assertNotEqual(a, SimpleApplication._provider_gap_signature([first]))
        self.assertIsInstance(a, list)
        self.assertEqual(a[0]["media_type"], "tv")

    def test_fresh_audit_does_not_clobber_live_provider_progress(self) -> None:
        """A rediscovered gap must not replace an in-flight root projection.

        A full audit can finish while the provider worker is downloading or
        verifying.  The audit calls ``_queue_provider_job`` again for the same
        durable root, so that queue boundary must leave the worker's live
        ``acquiring`` state intact instead of publishing ``gap_discovering``.
        """
        with tempfile.TemporaryDirectory() as directory, patch.dict(
            os.environ,
            {
                "SCRAPEFLOW_START_PAUSED": "1",
                "SCRAPEFLOW_PROVIDER_PILOT_TMDB": "42",
            },
            clear=False,
        ):
            root = Path(directory)
            remote = EmptyAList()
            runner = SimpleEngineRunner(
                root, alist=remote, tmdb=object(), validate=False, library_root="/library",
            )
            app = SimpleApplication(
                state_root=root, remote_root="/library", remote=remote,
                engine_runner=runner, enforce_engine_roots=False,
            )
            job = runner.create_audit_owned_root(_project(_gap()))
            try:
                app._record_replenishment_progress(
                    job,
                    "acquiring",
                    {"round": 1, "candidate_title": "live provider candidate"},
                )
                in_flight: Future[object] = Future()
                with app._automatic_lock:
                    app._provider_futures[job.id] = in_flight

                # Avoid scheduling a second resume scan in this unit test;
                # this isolates the fresh-audit queue operation below.
                with patch.object(app, "_start_startup_thread"):
                    app.set_paused(False)
                self._admit_provider(app)
                app._apply_audit_gaps(
                    {"semantic": {
                        "gaps": [_gap()],
                        "unknowns": [],
                        "acquisition_projects": [_project(_gap())],
                    }},
                    runner,
                )

                persisted = runner.get_job(job.id)
                replenishment = persisted.summary["replenishment"]
                self.assertEqual(replenishment["status"], "acquiring")
                self.assertEqual(replenishment["candidate_title"], "live provider candidate")
                self.assertIs(app._provider_futures[job.id], in_flight)
                self.assertEqual(SimpleApplication.public_engine_job(persisted)["phase"], "acquiring")
                # A worker's own bounded retry is still allowed to arm its
                # delayed timer; only an immediate duplicate queue is dropped.
                with patch("local.simple_server.threading.Timer") as timer:
                    app._queue_provider_job(job.id, delay=30.0)
                    timer.assert_called_once()
            finally:
                app.close()

    def test_provider_commit_requests_one_fresh_audit_after_a_busy_scan(self) -> None:
        """A new child video must not miss the subtitle-gap audit window.

        The first scan represents an inventory captured before the provider
        child commits. A second request arriving while that scan is running
        is coalesced and runs after it settles, rather than being discarded.
        The normal audit projection then creates the exact ``missing_subtitle``
        row and dispatches the existing pure sidecar lane.
        """
        with tempfile.TemporaryDirectory() as directory, patch.dict(
            os.environ,
            {
                "SCRAPEFLOW_START_PAUSED": "1",
                "SCRAPEFLOW_AUTOMATIC_AUDIT": "0",
                "SCRAPEFLOW_INTAKE_MONITOR": "0",
            },
            clear=False,
        ):
            root = Path(directory)
            remote = EmptyAList()
            runner = SimpleEngineRunner(
                root, alist=remote, tmdb=object(), validate=False, library_root="/library",
            )
            app = SimpleApplication(
                state_root=root, remote_root="/library", remote=remote,
                engine_runner=runner, enforce_engine_roots=False,
            )
            first_started = threading.Event()
            release_first = threading.Event()
            second_finished = threading.Event()
            calls: list[int] = []

            def fake_audit() -> None:
                calls.append(len(calls) + 1)
                if len(calls) == 1:
                    first_started.set()
                    release_first.wait(timeout=3)
                else:
                    second_finished.set()

            try:
                # The test is about the audit coordinator, not startup job
                # recovery; avoid a concurrent resume thread touching state.
                with patch.object(app, "_start_startup_thread"), \
                     patch.object(app, "_run_library_audit_background", side_effect=fake_audit):
                    app.set_paused(False)
                    app._queue_library_audit()
                    self.assertTrue(first_started.wait(timeout=2))
                    app._queue_library_audit(delay=0.0, rerun_if_busy=True)
                    # Multiple child completions during one scan still need
                    # only one follow-up pass.
                    app._queue_library_audit(delay=0.0, rerun_if_busy=True)
                    release_first.set()
                    self.assertTrue(second_finished.wait(timeout=4))
                    self.assertEqual(calls, [1, 2])
            finally:
                app.close()

    def test_subtitle_batch_deferred_queues_one_serial_follow_up_audit(self) -> None:
        """A deferred ledger slice advances once, without retrying probe errors."""
        with tempfile.TemporaryDirectory() as directory, patch.dict(
            os.environ,
            {
                "SCRAPEFLOW_START_PAUSED": "1",
                "SCRAPEFLOW_AUTOMATIC_AUDIT": "0",
                "SCRAPEFLOW_INTAKE_MONITOR": "0",
            },
            clear=False,
        ):
            root = Path(directory)
            remote = EmptyAList()
            runner = SimpleEngineRunner(
                root, alist=remote, tmdb=object(), validate=False, library_root="/library",
            )
            app = SimpleApplication(
                state_root=root, remote_root="/library", remote=remote,
                engine_runner=runner, enforce_engine_roots=False,
            )
            second_finished = threading.Event()
            third_started = threading.Event()
            calls: list[int] = []
            call_lock = threading.Lock()
            active = 0
            peak_active = 0

            def fake_once() -> dict[str, object]:
                nonlocal active, peak_active
                with call_lock:
                    ordinal = len(calls) + 1
                    calls.append(ordinal)
                    active += 1
                    peak_active = max(peak_active, active)
                try:
                    if ordinal == 1:
                        return {"audit": {"semantic": {"unknowns": [{
                            "kind": "unknown_subtitle_evidence",
                            "reason": "subtitle_probe_batch_deferred",
                        }]}}}
                    if ordinal == 2:
                        second_finished.set()
                        return {"audit": {"semantic": {"unknowns": [
                            {
                                "kind": "unknown_subtitle_evidence",
                                "reason": "subtitle_probe_error",
                            },
                            {"kind": "ambiguous_tv_container"},
                            {"kind": "unknown_episode_catalog"},
                        ]}}}
                    third_started.set()
                    return {"audit": {"semantic": {"unknowns": []}}}
                finally:
                    with call_lock:
                        active -= 1

            try:
                with patch.object(app, "_start_startup_thread"), \
                     patch.object(app, "_run_library_audit_once", side_effect=fake_once):
                    app.set_paused(False)
                    app._queue_library_audit()
                    self.assertTrue(second_finished.wait(timeout=4))
                    self.assertFalse(third_started.wait(timeout=1))
                    self.assertEqual(calls, [1, 2])
                    self.assertEqual(peak_active, 1)
            finally:
                app.close()

    def test_paused_subtitle_batch_deferred_does_not_queue_follow_up_audit(self) -> None:
        with tempfile.TemporaryDirectory() as directory, patch.dict(
            os.environ,
            {
                "SCRAPEFLOW_START_PAUSED": "1",
                "SCRAPEFLOW_AUTOMATIC_AUDIT": "0",
                "SCRAPEFLOW_INTAKE_MONITOR": "0",
            },
            clear=False,
        ):
            root = Path(directory)
            remote = EmptyAList()
            runner = SimpleEngineRunner(
                root, alist=remote, tmdb=object(), validate=False, library_root="/library",
            )
            app = SimpleApplication(
                state_root=root, remote_root="/library", remote=remote,
                engine_runner=runner, enforce_engine_roots=False,
            )
            first_started = threading.Event()
            release_first = threading.Event()
            first_finished = threading.Event()
            second_started = threading.Event()
            calls: list[int] = []

            def fake_once() -> dict[str, object]:
                ordinal = len(calls) + 1
                calls.append(ordinal)
                if ordinal == 1:
                    first_started.set()
                    release_first.wait(timeout=3)
                    first_finished.set()
                    return {"audit": {"semantic": {"unknowns": [{
                        "kind": "unknown_subtitle_evidence",
                        "reason": "subtitle_probe_batch_deferred",
                    }]}}}
                second_started.set()
                return {"audit": {"semantic": {"unknowns": []}}}

            try:
                with patch.object(app, "_start_startup_thread"), \
                     patch.object(app, "_run_library_audit_once", side_effect=fake_once):
                    app.set_paused(False)
                    app._queue_library_audit()
                    self.assertTrue(first_started.wait(timeout=2))
                    app.set_paused(True, "test pause")
                    release_first.set()
                    self.assertTrue(first_finished.wait(timeout=2))
                    self.assertFalse(second_started.wait(timeout=1))
                    self.assertEqual(calls, [1])
            finally:
                release_first.set()
                app.close()

    def test_direct_audit_is_report_only_and_serializes_automatic_follow_up(self) -> None:
        """The synchronous route never joins the mutating automatic future.

        Both audit kinds still use one executor, so a provider-triggered
        automatic follow-up waits until the manual report has settled.  The
        first call must be explicitly report-only; the second remains the
        automatic projection path.
        """
        with tempfile.TemporaryDirectory() as directory, patch.dict(
            os.environ,
            {
                "SCRAPEFLOW_START_PAUSED": "1",
                "SCRAPEFLOW_AUTOMATIC_AUDIT": "0",
                "SCRAPEFLOW_INTAKE_MONITOR": "0",
            },
            clear=False,
        ):
            root = Path(directory)
            remote = EmptyAList()
            runner = SimpleEngineRunner(
                root, alist=remote, tmdb=object(), validate=False, library_root="/library",
            )
            app = SimpleApplication(
                state_root=root, remote_root="/library", remote=remote,
                engine_runner=runner, enforce_engine_roots=False,
            )
            first_started = threading.Event()
            release_first = threading.Event()
            second_finished = threading.Event()
            manual_finished = threading.Event()
            call_lock = threading.Lock()
            active = 0
            peak_active = 0
            manual_result: list[dict[str, object]] = []
            calls: list[dict[str, object]] = []

            def fake_once(**kwargs: object) -> dict[str, object]:
                nonlocal active, peak_active
                with call_lock:
                    ordinal = len(calls) + 1
                    calls.append(dict(kwargs))
                    active += 1
                    peak_active = max(peak_active, active)
                try:
                    if ordinal == 1:
                        first_started.set()
                        release_first.wait(timeout=3)
                    else:
                        second_finished.set()
                    return {"audit": {"status": "completed", "ordinal": ordinal}}
                finally:
                    with call_lock:
                        active -= 1

            def run_direct() -> None:
                try:
                    manual_result.append(app.run_library_audit())
                finally:
                    manual_finished.set()

            worker = threading.Thread(target=run_direct, daemon=True)
            try:
                with patch.object(app, "_run_library_audit_once", side_effect=fake_once):
                    app.set_paused(False)
                    worker.start()
                    self.assertTrue(first_started.wait(timeout=2))
                    self.assertTrue(app.health()["operations"]["audit_running"])
                    self.assertTrue(worker.is_alive())
                    self.assertEqual(calls, [{"scope_roots": None, "project_gaps": False}])

                    # This mirrors a provider child committing while the
                    # direct HTTP audit is traversing its earlier inventory.
                    app._queue_library_audit(rerun_if_busy=True)
                    release_first.set()

                    self.assertTrue(manual_finished.wait(timeout=3))
                    worker.join(timeout=1)
                    self.assertEqual(
                        manual_result,
                        [{"audit": {"status": "completed", "ordinal": 1}}],
                    )
                    self.assertTrue(second_finished.wait(timeout=4))
                    self.assertEqual(
                        calls,
                        [
                            {"scope_roots": None, "project_gaps": False},
                            {},
                        ],
                    )
                    self.assertEqual(peak_active, 1)
            finally:
                release_first.set()
                worker.join(timeout=1)
                app.close()

    def test_fresh_audit_clears_stale_nonterminal_provider_projection_only_after_future_finishes(self) -> None:
        """A moved media member can close retry_wait, but not an active worker."""
        with tempfile.TemporaryDirectory() as directory, patch.dict(
            os.environ,
            {"SCRAPEFLOW_START_PAUSED": "1"},
            clear=False,
        ):
            root = Path(directory)
            remote = EmptyAList()
            runner = SimpleEngineRunner(
                root, alist=remote, tmdb=object(), validate=False, library_root="/library",
            )
            app = SimpleApplication(
                state_root=root, remote_root="/library", remote=remote,
                engine_runner=runner, enforce_engine_roots=False,
            )
            job = runner.create_audit_owned_root(_project(_gap()))
            try:
                with patch.object(app, "_start_startup_thread"):
                    app.set_paused(False)
                    self._admit_provider(app)
                app._record_replenishment_progress(
                    job, "retry_wait", {"error": "old child failed", "terminal": False},
                )
                live: Future[object] = Future()
                with app._automatic_lock:
                    app._provider_futures[job.id] = live
                app._apply_audit_gaps(
                    {"semantic": {"gaps": [], "unknowns": [], "acquisition_projects": []}},
                    runner,
                )
                self.assertEqual(
                    runner.get_job(job.id).summary["replenishment"]["status"],
                    "retry_wait",
                )

                live.set_result(None)
                app._apply_audit_gaps(
                    {"semantic": {"gaps": [], "unknowns": [], "acquisition_projects": []}},
                    runner,
                )
                cleared = runner.get_job(job.id)
                self.assertNotIn("status", cleared.summary.get("replenishment", {}))
                self.assertNotIn("replenishment_attempts", cleared.summary)
            finally:
                app.close()

    def test_successful_provider_summary_clears_prior_cancellation_projection(self) -> None:
        """A later completed child must not remain marked as cancelled."""
        with tempfile.TemporaryDirectory() as directory, patch.dict(
            os.environ, {"SCRAPEFLOW_START_PAUSED": "1"}, clear=False,
        ):
            root = Path(directory)
            remote = EmptyAList()
            runner = SimpleEngineRunner(
                root, alist=remote, tmdb=object(), validate=False, library_root="/library",
            )
            app = SimpleApplication(
                state_root=root, remote_root="/library", remote=remote,
                engine_runner=runner, enforce_engine_roots=False,
            )
            job = runner.create_audit_owned_root(_project(_gap()))
            try:
                app._record_replenishment_summary(job, {
                    "status": "retry_wait", "terminal": False, "attempts": 0,
                    "error": "自动补源已暂停或不在当前试点范围",
                    "cancelled": True, "cancellation_boundary": "materialization",
                })
                app._record_replenishment_summary(job, {
                    "status": "completed", "terminal": True, "attempts": 1,
                    "resolved_gap_ids": ["S01E02"],
                })
                persisted = runner.get_job(job.id).summary["replenishment"]
                self.assertEqual(persisted["status"], "completed")
                self.assertTrue(persisted["terminal"])
                self.assertNotIn("error", persisted)
                self.assertNotIn("cancelled", persisted)
                self.assertNotIn("cancellation_boundary", persisted)
            finally:
                app.close()

    def test_paused_restart_reconciles_orphaned_provider_progress(self) -> None:
        """A recreated paused API must not display a dead worker as acquiring."""
        with tempfile.TemporaryDirectory() as directory, patch.dict(
            os.environ,
            {
                "SCRAPEFLOW_START_PAUSED": "1",
                "SCRAPEFLOW_PROVIDER_PILOT_TMDB": "42",
            },
            clear=False,
        ):
            root = Path(directory)
            remote = EmptyAList()
            runner = SimpleEngineRunner(
                root, alist=remote, tmdb=object(), validate=False, library_root="/library",
            )
            first = SimpleApplication(
                state_root=root, remote_root="/library", remote=remote,
                engine_runner=runner, enforce_engine_roots=False,
            )
            job = runner.create_audit_owned_root(_project(_gap()))
            try:
                first._record_replenishment_progress(job, "acquiring", {"round": 1})
                gap_path = root / "gaps" / job.id / "audit-row-1.json"
                atomic_write_json(gap_path, {
                    "id": "audit-row-1",
                    "job_id": job.id,
                    "phase": "acquiring",
                    "attempts": 1,
                    "created_at": "2026-08-08T00:00:00Z",
                    "updated_at": "2026-08-08T00:00:00Z",
                    "error": None,
                }, allow_nan=False)
            finally:
                first.close()

            # Simulate a forced API recreation: no in-memory Future survived,
            # but the persisted root/gap projection still says acquiring.
            with patch.object(SimpleApplication, "_start_startup_thread"):
                resumed = SimpleApplication(
                    state_root=root, remote_root="/library", remote=remote,
                    engine_runner=runner, enforce_engine_roots=False,
                )
            try:
                resumed._resume_automatic_jobs()
                persisted = runner.get_job(job.id)
                replenishment = persisted.summary["replenishment"]
                gap_state = json.loads(gap_path.read_text(encoding="utf-8"))

                self.assertEqual(replenishment["status"], "retry_wait")
                self.assertFalse(replenishment["terminal"])
                self.assertIsNone(replenishment["next_retry_seconds"])
                self.assertEqual(persisted.summary["automatic_stage"], "retry_wait")
                self.assertEqual(gap_state["phase"], "retry_wait")
                self.assertEqual(resumed._provider_futures, {})
                self.assertEqual(SimpleApplication.public_engine_job(persisted)["phase"], "retry_wait")
            finally:
                resumed.close()

    def test_cancelled_provider_outcome_stays_retryable_without_budget(self) -> None:
        """A control stop is not a provider failure or a delayed requeue."""
        with tempfile.TemporaryDirectory() as directory, patch.dict(
            os.environ,
            {
                "SCRAPEFLOW_START_PAUSED": "1",
                "SCRAPEFLOW_PROVIDER_PILOT_TMDB": "42",
                "SCRAPEFLOW_PROVIDER_RETRY_LIMIT": "1",
            },
            clear=False,
        ):
            root = Path(directory)
            remote = EmptyAList()
            runner = SimpleEngineRunner(
                root, alist=remote, tmdb=object(), validate=False, library_root="/library",
            )
            app = SimpleApplication(
                state_root=root, remote_root="/library", remote=remote,
                engine_runner=runner, enforce_engine_roots=False,
            )
            job = runner.create_audit_owned_root(_project(_gap()))
            try:
                with patch.object(app, "_start_startup_thread"):
                    app.set_paused(False)
                self._admit_provider(app)

                class CancelledRuntime:
                    def run_for_job(self, _job: EngineJob) -> dict[str, object]:
                        return {
                            "job_id": job.id,
                            "outcomes": [{
                                "error": "自动补源已暂停或不在当前试点范围",
                                "cancelled": True,
                            }],
                            "unresolved_gaps": [],
                            "cancelled": True,
                        }

                with patch.object(app, "_get_automatic_replenishment", return_value=CancelledRuntime()), \
                     patch.object(app, "_queue_library_audit") as audit_queue, \
                     patch.object(app, "_queue_provider_job") as provider_queue:
                    app._run_automatic_replenishment(job.id)

                persisted = runner.get_job(job.id)
                replenishment = persisted.summary["replenishment"]
                self.assertEqual(replenishment["status"], "retry_wait")
                self.assertFalse(replenishment["terminal"])
                self.assertEqual(persisted.summary.get("replenishment_attempts"), 0)
                audit_queue.assert_not_called()
                provider_queue.assert_not_called()
            finally:
                app.close()

    def test_candidate_failure_uses_tier_budget_not_infrastructure_budget(self) -> None:
        with tempfile.TemporaryDirectory() as directory, patch.dict(
            os.environ,
            {
                "SCRAPEFLOW_START_PAUSED": "1",
                "SCRAPEFLOW_PROVIDER_PILOT_TMDB": "42",
                "SCRAPEFLOW_PROVIDER_RETRY_LIMIT": "1",
            },
            clear=False,
        ):
            root = Path(directory)
            remote = EmptyAList()
            runner = SimpleEngineRunner(
                root, alist=remote, tmdb=object(), validate=False, library_root="/library",
            )
            app = SimpleApplication(
                state_root=root, remote_root="/library", remote=remote,
                engine_runner=runner, enforce_engine_roots=False,
            )
            job = runner.create_audit_owned_root(_project(_gap()))

            class CandidateRuntime:
                def run_for_job(self, _job: EngineJob) -> dict[str, object]:
                    return {
                        "job_id": job.id,
                        "outcomes": [{
                            "error": "candidate invalid",
                            "failure_scope": "candidate",
                        }],
                        "unresolved_gaps": [],
                    }

            try:
                with patch.object(app, "_start_startup_thread"):
                    app.set_paused(False)
                    self._admit_provider(app)
                with patch.object(
                    app, "_get_automatic_replenishment", return_value=CandidateRuntime(),
                ), patch.object(
                    app, "_queue_scoped_library_audit",
                ), patch.object(app, "_queue_provider_job") as provider_queue:
                    app._run_automatic_replenishment(job.id)

                persisted = runner.get_job(job.id)
                replenishment = persisted.summary["replenishment"]
                self.assertEqual(replenishment["status"], "retry_wait")
                self.assertFalse(replenishment["terminal"])
                self.assertEqual(persisted.summary.get("replenishment_attempts"), 0)
                provider_queue.assert_called_once_with(job.id, delay=30.0)
            finally:
                app.close()

    def test_in_doubt_provider_outcome_waits_without_resubmit_or_budget(self) -> None:
        with tempfile.TemporaryDirectory() as directory, patch.dict(
            os.environ,
            {
                "SCRAPEFLOW_START_PAUSED": "1",
                "SCRAPEFLOW_PROVIDER_PILOT_TMDB": "42",
                "SCRAPEFLOW_PROVIDER_RETRY_LIMIT": "1",
            },
            clear=False,
        ):
            root = Path(directory)
            remote = EmptyAList()
            runner = SimpleEngineRunner(
                root, alist=remote, tmdb=object(), validate=False, library_root="/library",
            )
            app = SimpleApplication(
                state_root=root, remote_root="/library", remote=remote,
                engine_runner=runner, enforce_engine_roots=False,
            )
            job = runner.create_audit_owned_root(_project(_gap()))

            class InDoubtRuntime:
                def run_for_job(self, _job: EngineJob) -> dict[str, object]:
                    return {
                        "job_id": job.id,
                        "outcomes": [{
                            "error": "external submit outcome unknown",
                            "failure_scope": "in_doubt",
                            "tier_status": "waiting_reconcile",
                        }],
                        "unresolved_gaps": [],
                    }

            try:
                with patch.object(app, "_start_startup_thread"):
                    app.set_paused(False)
                    self._admit_provider(app)
                with patch.object(
                    app, "_get_automatic_replenishment", return_value=InDoubtRuntime(),
                ), patch.object(
                    app, "_queue_scoped_library_audit",
                ), patch.object(app, "_queue_provider_job") as provider_queue:
                    app._run_automatic_replenishment(job.id)

                persisted = runner.get_job(job.id)
                replenishment = persisted.summary["replenishment"]
                self.assertEqual(replenishment["status"], "waiting_reconcile")
                self.assertFalse(replenishment["terminal"])
                self.assertIsNone(replenishment["next_retry_seconds"])
                self.assertEqual(persisted.summary.get("replenishment_attempts"), 0)
                provider_queue.assert_not_called()
            finally:
                app.close()

    def test_final_tier_candidate_exhaustion_stops_without_infrastructure_budget(self) -> None:
        with tempfile.TemporaryDirectory() as directory, patch.dict(
            os.environ,
            {
                "SCRAPEFLOW_START_PAUSED": "1",
                "SCRAPEFLOW_PROVIDER_PILOT_TMDB": "42",
            },
            clear=False,
        ):
            root = Path(directory)
            remote = EmptyAList()
            runner = SimpleEngineRunner(
                root, alist=remote, tmdb=object(), validate=False, library_root="/library",
            )
            app = SimpleApplication(
                state_root=root, remote_root="/library", remote=remote,
                engine_runner=runner, enforce_engine_roots=False,
            )
            job = runner.create_audit_owned_root(_project(_gap()))
            state_path = root / "gaps" / job.id / "audit-row-1.json"
            state_path.parent.mkdir(parents=True, exist_ok=True)
            atomic_write_json(state_path, {
                "id": "audit-row-1",
                "job_id": job.id,
                "phase": "retry_wait",
                "tier": "magnet",
                "tier_status": "exhausted",
            }, allow_nan=False)

            class ExhaustedRuntime:
                def run_for_job(self, _job: EngineJob) -> dict[str, object]:
                    return {
                        "job_id": job.id,
                        "outcomes": [{
                            "error": "thirtieth local candidate invalid",
                            "failure_scope": "candidate",
                        }],
                        "unresolved_gaps": [],
                    }

            try:
                with patch.object(app, "_start_startup_thread"):
                    app.set_paused(False)
                    self._admit_provider(app)
                with patch.object(
                    app, "_get_automatic_replenishment", return_value=ExhaustedRuntime(),
                ), patch.object(
                    app, "_queue_scoped_library_audit",
                ), patch.object(app, "_queue_provider_job") as provider_queue:
                    app._run_automatic_replenishment(job.id)

                persisted = runner.get_job(job.id)
                replenishment = persisted.summary["replenishment"]
                self.assertEqual(replenishment["status"], "failed")
                self.assertTrue(replenishment["terminal"])
                self.assertEqual(persisted.summary.get("replenishment_attempts"), 0)
                provider_queue.assert_not_called()
            finally:
                app.close()

    def test_only_infrastructure_failure_consumes_five_attempt_budget(self) -> None:
        with tempfile.TemporaryDirectory() as directory, patch.dict(
            os.environ,
            {
                "SCRAPEFLOW_START_PAUSED": "1",
                "SCRAPEFLOW_PROVIDER_PILOT_TMDB": "42",
                "SCRAPEFLOW_PROVIDER_RETRY_LIMIT": "1",
            },
            clear=False,
        ):
            root = Path(directory)
            remote = EmptyAList()
            runner = SimpleEngineRunner(
                root, alist=remote, tmdb=object(), validate=False, library_root="/library",
            )
            app = SimpleApplication(
                state_root=root, remote_root="/library", remote=remote,
                engine_runner=runner, enforce_engine_roots=False,
            )
            job = runner.create_audit_owned_root(_project(_gap()))

            class InfrastructureRuntime:
                def run_for_job(self, _job: EngineJob) -> dict[str, object]:
                    return {
                        "job_id": job.id,
                        "outcomes": [{
                            "error": "helper unavailable",
                            "failure_scope": "infrastructure",
                        }],
                        "unresolved_gaps": [],
                    }

            try:
                with patch.object(app, "_start_startup_thread"):
                    app.set_paused(False)
                    self._admit_provider(app)
                with patch.object(
                    app, "_get_automatic_replenishment", return_value=InfrastructureRuntime(),
                ), patch.object(
                    app, "_queue_scoped_library_audit",
                ), patch.object(app, "_queue_provider_job") as provider_queue:
                    app._run_automatic_replenishment(job.id)

                persisted = runner.get_job(job.id)
                replenishment = persisted.summary["replenishment"]
                self.assertEqual(replenishment["status"], "failed")
                self.assertTrue(replenishment["terminal"])
                self.assertEqual(persisted.summary["replenishment_attempts"], 1)
                provider_queue.assert_not_called()
            finally:
                app.close()

    def test_provider_pilot_selector_requires_a_positive_integer(self) -> None:
        for value in ("0", "-1", "not-an-id", "４２"):
            with self.subTest(value=value), patch.dict(
                os.environ,
                {"SCRAPEFLOW_PROVIDER_PILOT_TMDB": value},
                clear=False,
            ):
                with self.assertRaises(ApplicationError):
                    SimpleApplication._provider_pilot_tmdb()

    def test_provider_pilot_gap_selector_requires_sxxeyy(self) -> None:
        for value in ("", "S1E2", "S00E000", "episode-1"):
            with self.subTest(value=value), patch.dict(
                os.environ,
                {"SCRAPEFLOW_PROVIDER_PILOT_GAP": value},
                clear=False,
            ):
                if not value:
                    self.assertIsNone(SimpleApplication._provider_pilot_gap())
                else:
                    with self.assertRaises(ApplicationError):
                        SimpleApplication._provider_pilot_gap()


if __name__ == "__main__":
    unittest.main()
