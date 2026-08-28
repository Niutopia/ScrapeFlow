"""Focused public API coverage for the single-user local server."""

from __future__ import annotations

import json
import os
import tempfile
import threading
import time
import unittest
import urllib.error
import urllib.request
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

from engine.scrapeflow.serialization import atomic_write_json
from engine.scrapeflow.gap_ledger import Gap, save_gap_ledger
from engine.scrapeflow.intake_source import (
    bind_root_task,
    intake_source_id,
    save_intake_catalog,
    upsert_intake_source,
)
from engine.scrapeflow.replacement import build_replacement_manifest
from engine.scrapeflow.work_units import (
    WorkUnitRecord,
    load_work_unit_records,
    save_work_unit_records,
)
from local.simple_server import ApplicationError, SimpleApplication, make_server
from local.scrapeflow_api.simple_engine_runner import (
    EngineJob,
    EngineJobConflictError,
    EngineRequestError,
    SimpleEngineRunner,
)
from local.scrapeflow_api.batch_manifest import (
    BatchManifest,
    BatchManifestItem,
    FreshResult,
    load_batch_manifest,
    save_batch_manifest,
)
from local.scrapeflow_api.unit_execution import (
    WorkAcceptanceResult,
    save_work_acceptance,
)


class FakeAList:
    def __init__(self) -> None:
        self.entries: dict[str, list[dict[str, object]]] = {
            "/library/电影": [],
            "/library/番剧": [],
            "/library/欧美剧": [],
            "/library/待刮削": [
                {"name": "Example", "is_dir": True},
                {"name": "plain.txt", "is_dir": False, "size": 3},
            ],
            "/library/待刮削/Example": [
                {"name": "Season 01", "is_dir": True},
                {"name": "poster.jpg", "is_dir": False, "size": 3},
            ],
        }

    def list(self, path: str, refresh: bool = False) -> list[dict[str, object]]:
        del refresh
        return [dict(row) for row in self.entries.get(path, [])]


class SimpleServerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.state_root = Path(self.temporary.name) / "state"
        self.remote = FakeAList()
        self.runner = SimpleEngineRunner(
            self.state_root,
            alist=self.remote,
            tmdb=object(),
            validate=False,
            library_root="/library",
        )
        self.application = SimpleApplication(
            state_root=self.state_root,
            remote_root="/library",
            remote=self.remote,
            engine_runner=self.runner,
        )
        self.addCleanup(self.application.close)
        self.server = make_server(self.application, "127.0.0.1", 0)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.addCleanup(self._stop_server)
        self.base = f"http://127.0.0.1:{self.server.server_port}"

    def _stop_server(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)

    def request(
        self,
        method: str,
        path: str,
        payload: dict[str, object] | None = None,
        headers: dict[str, str] | None = None,
    ) -> tuple[int, dict[str, object]]:
        body = None if payload is None else json.dumps(payload).encode("utf-8")
        request_headers = {"Content-Type": "application/json"} if body is not None else {}
        request_headers.update(headers or {})
        request = urllib.request.Request(
            self.base + path,
            data=body,
            method=method,
            headers=request_headers,
        )
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        try:
            with opener.open(request, timeout=3) as response:
                return response.status, json.loads(response.read())
        except urllib.error.HTTPError as exc:
            return exc.code, json.loads(exc.read())

    def create_root(self) -> str:
        status, payload = self.request(
            "POST",
            "/api/root-jobs",
            {"path": "/library/待刮削/Example", "target_shelf": "anime"},
        )
        self.assertEqual(status, 201)
        return str(payload["job"]["id"])

    def _completed_root_with_unmaterialized_planner_season(self) -> str:
        """Build an old completed carrier with the strict legacy J fact."""
        root_id = self.create_root()
        writer_id = "unit-planner-season-gap"
        record = WorkUnitRecord(
            work_unit_id="unit-planner-season-gap",
            root_task_id=root_id,
            boundary_key="/library/待刮削/Example",
            source_paths=("/library/待刮削/Example",),
            source_revision=1,
            role="single_work",
            media_context="tv",
            identity_status="confirmed",
            identity={"media_type": "tv", "tmdb_id": 101, "title": "Example"},
            reconciliation_outcome="new_work",
            writer_job_id=writer_id,
            gap_status="registered",
        )
        save_work_unit_records(self.state_root, root_id, [record])
        carrier = EngineJob(
            id=writer_id,
            phase="executed",
            created_at="2026-08-26T00:00:00Z",
            updated_at="2026-08-26T00:00:00Z",
            request={},
            plan={
                "files": [{"final_name": "S01E01.mkv", "media_kind": "video"}],
                "target_root": "/library/番剧/Example",
                "scan_report": {"resource_gaps": [{
                    "kind": "missing_season",
                    "label": "Season 04 Fourth Season",
                    "reason": "planner proved no source or formal-library video",
                    "files": [],
                    "season_name": "Fourth Season",
                    "expected_episode_count": 2,
                }]},
            },
            summary={"internal_child": True, "root_job_id": root_id},
        )
        atomic_write_json(
            self.runner._job_path(writer_id),  # noqa: SLF001 - durable carrier fixture
            carrier.as_dict(),
            allow_nan=False,
        )
        completed = replace(self.runner.get_job(root_id), phase="completed")
        atomic_write_json(
            self.runner._job_path(root_id),  # noqa: SLF001 - durable root fixture
            completed.as_dict(),
            allow_nan=False,
        )
        return root_id

    def test_health_is_small_local_status(self) -> None:
        status, health = self.request("GET", "/api/health")

        self.assertEqual(status, 200)
        self.assertTrue(health["ok"])
        self.assertEqual(health["status"], "ok")
        self.assertEqual(health["control"], {"paused": True, "root_job_id": None})
        self.assertNotIn("build_commit", health)
        self.assertNotIn("liveness", health)
        self.assertNotIn("dependencies", health)
        self.assertNotIn("provider_capabilities", health)
        self.assertEqual(health["operations"]["worker_busy"], 0)
        self.assertNotIn("formal_write_workers", health["operations"])
        self.assertNotIn("provider_workers", health["operations"])

    def test_clear_orphan_selection_requires_missing_job(self) -> None:
        self.application._control_state.set(  # noqa: SLF001
            paused=True, root_job_id="engine-missing"
        )
        status, payload = self.request(
            "POST", "/api/control/clear-orphan-selection", {},
        )
        self.assertEqual(status, 200)
        self.assertEqual(payload, {"paused": True, "root_job_id": None})

    def test_reopen_orphan_requires_exact_backup_and_rebuilds_fresh_bw(self) -> None:
        orphan_id = "engine-orphan"
        source = "/library/待刮削/Example"
        catalog, _ = upsert_intake_source([], source)
        catalog, _ = bind_root_task(catalog, intake_source_id(source), orphan_id)
        save_intake_catalog(self.state_root, catalog)
        from engine.scrapeflow.root_boundaries import build_root_boundary_analysis, persist_root_boundary_analysis
        old_snapshot, old_records = build_root_boundary_analysis(
            self.remote, source, root_task_id=orphan_id, source_revision=3,
        )
        persist_root_boundary_analysis(self.state_root, orphan_id, old_snapshot, old_records)
        backup = EngineJob(
            id=orphan_id,
            phase="cancelled",
            created_at="2026-08-20T00:00:00Z",
            updated_at="2026-08-20T00:00:00Z",
            request={"source_path": source},
            plan={},
            summary={"automatic": True},
            target_shelf="anime",
            target_root="/library/番剧",
            selected_at="2026-08-20T00:00:00Z",
        )
        reopened = self.application.reopen_orphan_root_task(
            orphan_id, {"backup_job": backup.as_dict()},
        )
        self.assertEqual(reopened.id, orphan_id)
        self.assertEqual(reopened.phase, "queued")
        self.assertEqual(self.application.control(), {"paused": True, "root_job_id": orphan_id})
        self.assertTrue((self.state_root / f"source_manifest_{orphan_id}.json").exists())
        self.assertEqual(len(load_work_unit_records(self.state_root, orphan_id)), 1)
        self.assertFalse((self.state_root / f"work_acceptance_{orphan_id}.json").exists())
        self.assertFalse((self.state_root / f"gap_ledger_{orphan_id}.json").exists())

    def test_reopen_orphan_rejects_mismatched_backup_without_state(self) -> None:
        orphan_id = "engine-orphan"
        source = "/library/待刮削/Example"
        catalog, _ = upsert_intake_source([], source)
        catalog, _ = bind_root_task(catalog, intake_source_id(source), orphan_id)
        save_intake_catalog(self.state_root, catalog)
        backup = EngineJob(
            id="other-id", phase="cancelled", created_at="x", updated_at="x",
            request={"source_path": source}, plan={}, summary={},
            target_shelf="anime", target_root="/library/番剧", selected_at="x",
        )
        with self.assertRaises(EngineJobConflictError):
            self.application.reopen_orphan_root_task(orphan_id, {"backup_job": backup.as_dict()})
        self.assertFalse((self.state_root / "jobs" / f"{orphan_id}.json").exists())

    def _orphan_fixture(self, phase: str = "cancelled") -> tuple[str, str, EngineJob, WorkUnitRecord]:
        orphan_id = "engine-orphan-evidence"
        source = "/library/待刮削/Example"
        catalog, _ = upsert_intake_source([], source)
        catalog, _ = bind_root_task(catalog, intake_source_id(source), orphan_id)
        save_intake_catalog(self.state_root, catalog)
        from engine.scrapeflow.root_boundaries import build_root_boundary_analysis, persist_root_boundary_analysis
        snapshot, records = build_root_boundary_analysis(self.remote, source, root_task_id=orphan_id, source_revision=7)
        persist_root_boundary_analysis(self.state_root, orphan_id, snapshot, records)
        backup = EngineJob(
            id=orphan_id, phase=phase, created_at="x", updated_at="x",
            request={"source_path": source}, plan={}, summary=(
                {"prewrite_failure_proven": True} if phase == "failed" else {}
            ), target_shelf="anime", target_root="/library/番剧", selected_at="x",
        )
        return orphan_id, source, backup, records[0]

    def test_reopen_orphan_rejects_fplus_evidence_without_overwriting_bw(self) -> None:
        evidence = ["writer", "episode_map", "acceptance", "gap", "carrier", "staging"]
        for kind in evidence:
            with self.subTest(kind=kind):
                orphan_id, _, backup, record = self._orphan_fixture()
                units_path = self.state_root / f"work_units_{orphan_id}.json"
                before = units_path.read_bytes()
                if kind == "writer":
                    raw = json.loads(before); raw[0]["writer_job_id"] = "writer-1"; atomic_write_json(units_path, raw)
                elif kind == "episode_map":
                    atomic_write_json(self.state_root / f"episode_map_{record.work_unit_id}.json", {"S01E01": 1})
                elif kind == "acceptance":
                    atomic_write_json(self.state_root / f"work_acceptance_{orphan_id}.json", {"items": []})
                elif kind == "gap":
                    atomic_write_json(self.state_root / f"gap_ledger_{orphan_id}.json", {"gaps": []})
                elif kind == "carrier":
                    atomic_write_json(self.state_root / "jobs" / "carrier.json", {
                        "id": "carrier", "phase": "planned", "created_at": "x", "updated_at": "x",
                        "request": {}, "plan": {}, "summary": {"root_job_id": orphan_id},
                    })
                else:
                    (self.state_root / "staging" / orphan_id).mkdir(parents=True)
                before_reopen = units_path.read_bytes()
                with self.assertRaises(EngineJobConflictError):
                    self.application.reopen_orphan_root_task(orphan_id, {"backup_job": backup.as_dict()})
                self.assertFalse((self.state_root / "jobs" / f"{orphan_id}.json").exists())
                self.assertEqual(units_path.read_bytes(), before_reopen)

    def test_reopen_orphan_rejects_missing_or_malformed_ledger_and_uncertain_terminal(self) -> None:
        for mode in ("missing", "malformed"):
            with self.subTest(mode=mode):
                orphan_id, _, backup, _ = self._orphan_fixture()
                path = self.state_root / f"work_units_{orphan_id}.json"
                if mode == "missing":
                    path.unlink()
                else:
                    path.write_text("{bad", encoding="utf-8")
                with self.assertRaises(EngineJobConflictError):
                    self.application.reopen_orphan_root_task(orphan_id, {"backup_job": backup.as_dict()})
        for phase in ("completed", "failed"):
            with self.subTest(phase=phase):
                orphan_id, _, backup, _ = self._orphan_fixture(phase)
                if phase == "failed":
                    backup = replace(backup, summary={})
                with self.assertRaises(EngineJobConflictError):
                    self.application.reopen_orphan_root_task(orphan_id, {"backup_job": backup.as_dict()})

    def test_reopen_orphan_increments_source_revision(self) -> None:
        orphan_id, _, backup, _ = self._orphan_fixture()
        reopened = self.application.reopen_orphan_root_task(orphan_id, {"backup_job": backup.as_dict()})
        self.assertEqual(reopened.id, orphan_id)
        records = load_work_unit_records(self.state_root, orphan_id)
        self.assertTrue(records)
        self.assertTrue(all(record.source_revision == 8 for record in records))

    def test_completed_legacy_root_with_open_gap_projects_as_gaps_pending(self) -> None:
        """Public state must not let H hide an unclosed J ledger row."""
        root_id = self.create_root()
        record = WorkUnitRecord(
            work_unit_id="unit-gap",
            root_task_id=root_id,
            boundary_key="/library/待刮削/Example",
            source_paths=("/library/待刮削/Example",),
            source_revision=1,
            role="single_work",
            media_context="tv",
            identity_status="confirmed",
            identity={"media_type": "tv", "tmdb_id": 1, "title": "Example"},
            reconciliation_outcome="new_work",
        )
        save_work_unit_records(self.state_root, root_id, [record])
        save_work_acceptance(self.state_root, root_id, [
            WorkAcceptanceResult(
                work_unit_id=record.work_unit_id,
                outcome="accepted",
                writer_job_id="unit-gap",
                phase="executed",
                target_root="/library/番剧/Example",
                planned_files=1,
                error=None,
                recorded_at="2026-08-19T00:00:00Z",
            ),
        ])
        save_gap_ledger(self.state_root, root_id, [Gap(
            gap_id="unit-gap::missing_episode::S01E02",
            root_task_id=root_id,
            work_unit_id=record.work_unit_id,
            kind="missing_episode",
            media_type="tv",
            tmdb_id=1,
            season=1,
            episodes=(2,),
            subtitle_path=None,
            subtitle_language=None,
            status="open",
        )])
        completed = replace(self.runner.get_job(root_id), phase="completed")
        atomic_write_json(
            self.runner._job_path(root_id),  # noqa: SLF001 - legacy fixture
            completed.as_dict(),
            allow_nan=False,
        )

        public = self.application.public_engine_job(self.runner.get_job(root_id))

        self.assertEqual(public["engine_phase"], "completed")
        self.assertEqual(public["phase"], "gaps_pending")
        self.assertEqual(public["readback"]["status"], "verified")
        self.assertEqual(public["aggregate"]["open_gaps"], 1)
        self.assertEqual(public["aggregate"]["status"], "gaps_pending")
        self.assertEqual(
            self.application.work_units_view(root_id)["phase"],
            "gaps_pending",
        )

    def test_deployed_media_root_cannot_switch_to_an_alternate_tree(self) -> None:
        with patch.dict(os.environ, {"SCRAPEFLOW_MEDIA_ROOT": "/library"}):
            with self.assertRaises(ApplicationError):
                SimpleApplication(
                    state_root=self.state_root / "alternate-root",
                    remote=object(),
                )

    def test_archive_disc_limits_are_loaded_once_from_environment(self) -> None:
        with patch.dict(os.environ, {
            "SCRAPEFLOW_DISC_IMAGE_MAX_SOURCE_BYTES": "987654321",
            "SCRAPEFLOW_DISC_IMAGE_COMMAND_TIMEOUT_SECONDS": "1234",
        }):
            application = SimpleApplication(
                state_root=self.state_root / "disc-limits",
                remote_root="/library",
                remote=self.remote,
                engine_runner=self.runner,
            )
        self.addCleanup(application.close)
        limits = application._archive_preprocessor.limits
        self.assertEqual(limits.max_disc_image_bytes, 987654321)
        self.assertEqual(limits.disc_command_timeout_seconds, 1234)

    def test_refresh_only_discovers_sources(self) -> None:
        self.remote.entries["/library/待刮削"] = [
            {"name": "普通来源", "is_dir": True},
            {"name": "Real Release", "is_dir": True},
        ]
        self.remote.entries["/library/待刮削/普通来源"] = []
        self.remote.entries["/library/待刮削/Real Release"] = [
            {"name": "Season 01", "is_dir": True},
            {"name": "poster.jpg", "is_dir": False, "size": 3},
        ]

        status, payload = self.request("POST", "/api/intake/refresh", {})

        self.assertEqual(status, 200)
        self.assertEqual(self.runner.list_jobs(), [])
        sources = {row["canonical_path"]: row for row in payload["sources"]}
        self.assertIn("/library/待刮削/普通来源", sources)
        self.assertEqual(sources["/library/待刮削/Real Release"]["child_count"], 1)
        self.assertEqual(sources["/library/待刮削/Real Release"]["file_count"], 1)

    def test_root_creation_selects_one_paused_job(self) -> None:
        root_id = self.create_root()

        status, control = self.request("GET", "/api/control")
        self.assertEqual(status, 200)
        self.assertEqual(control, {"paused": True, "root_job_id": root_id})
        job = self.runner.get_job(root_id)
        self.assertEqual(job.target_shelf, "anime")
        self.assertEqual(job.target_root, "/library/番剧")
        self.assertEqual(job.phase, "queued")

    def test_resume_runs_only_the_selected_root(self) -> None:
        root_id = self.create_root()
        queued: list[str] = []
        with patch.object(self.application, "_resume_after_control_open", side_effect=lambda: queued.append(root_id)):
            status, control = self.request("POST", "/api/control/resume", {})

        self.assertEqual(status, 200)
        self.assertEqual(control, {"paused": False, "root_job_id": root_id})
        self.assertEqual(queued, [root_id])

    def test_resume_does_not_backfill_a_completed_planner_gap(self) -> None:
        """A historical J repair is explicit retry only, never resume work."""
        root_id = self._completed_root_with_unmaterialized_planner_season()
        self.application._control_state.set(paused=False, root_job_id=root_id)  # noqa: SLF001

        with patch.object(
            self.application, "_queue_completed_root_j_rereview",
        ) as rereview, patch.object(
            self.application, "_queue_root_replenishment",
        ) as replenish:
            self.application._resume_automatic_jobs()  # noqa: SLF001 - scheduler boundary

        rereview.assert_not_called()
        replenish.assert_not_called()

    def test_explicit_retry_of_completed_planner_gap_queues_j_only(self) -> None:
        root_id = self._completed_root_with_unmaterialized_planner_season()
        self.application._control_state.set(paused=False, root_job_id=root_id)  # noqa: SLF001

        with patch.object(
            self.application,
            "_queue_completed_root_j_rereview",
            return_value="queued",
        ) as rereview, patch.object(
            self.application,
            "_queue_root_replenishment",
        ) as replenishment:
            status, _payload = self.request("POST", f"/api/jobs/{root_id}/retry", {})

        self.assertEqual(status, 200)
        rereview.assert_called_once_with(root_id)
        replenishment.assert_not_called()

    def test_select_endpoint_keeps_the_scheduler_paused(self) -> None:
        root_id = self.create_root()
        self.application._control_state.set(paused=False, root_job_id=root_id)  # noqa: SLF001

        status, control = self.request(
            "POST", "/api/control/select", {"root_job_id": root_id},
        )

        self.assertEqual(status, 200)
        self.assertEqual(control, {"paused": True, "root_job_id": root_id})

    def test_explicit_retry_requeues_parked_identity_unit(self) -> None:
        root_id = self.create_root()
        save_work_unit_records(
            self.state_root,
            root_id,
            [WorkUnitRecord(
                work_unit_id="unit-needs-retry",
                root_task_id=root_id,
                boundary_key="/library/待刮削/Example",
                source_paths=("/library/待刮削/Example",),
                source_revision=1,
                role="single_work",
                media_context="tv",
                identity_status="uncertain",
                candidate_identities=({"tmdb_id": 42, "media_type": "tv"},),
                attention="TMDB 暂无候选",
            )],
        )
        parked = replace(
            self.runner.get_job(root_id),
            phase="reconciliation_uncertain",
            error="部分作品单元身份待确认",
        )
        atomic_write_json(
            self.runner._job_path(root_id),  # noqa: SLF001 - durable fixture
            parked.as_dict(),
            allow_nan=False,
        )

        status, payload = self.request("POST", f"/api/jobs/{root_id}/retry", {})

        self.assertEqual(status, 200)
        self.assertEqual(payload["job"]["phase"], "queued")
        records = load_work_unit_records(self.state_root, root_id)
        self.assertEqual(records[0].identity_status, "pending")
        self.assertEqual(records[0].candidate_identities, ())
        self.assertIsNone(records[0].attention)
        self.assertEqual(self.application.control(), {"paused": True, "root_job_id": root_id})

    def test_replacement_manifest_persistence_revalidates_identity_ingress_and_archive(self) -> None:
        root_id = self.create_root()
        source_root = "/library/待刮削/Example"
        target_root = "/library/番剧/Example"
        record = WorkUnitRecord(
            work_unit_id="unit-replacement",
            root_task_id=root_id,
            boundary_key=source_root,
            source_paths=(source_root,),
            source_revision=1,
            role="single_work",
            media_context="tv",
            identity_status="confirmed",
            identity={"media_type": "tv", "tmdb_id": 123},
            reconciliation_outcome="merge_existing",
            matched_work_root=target_root,
        )
        save_work_unit_records(self.state_root, root_id, [record])
        source = [
            {"path": f"{source_root}/01.mkv", "size": 101, "kind": "video", "coordinate": "S01E01"},
            {"path": f"{source_root}/01.sc.srt", "size": 11, "kind": "subtitle", "coordinate": "S01E01", "language": "zh-CN"},
        ]
        target = [
            {"path": f"{target_root}/S01E13.mkv", "size": 201, "kind": "video", "coordinate": "S01E13"},
        ]

        manifest = build_replacement_manifest(
            manifest_id="replacement-server-1",
            root_job_id=root_id,
            work_unit_id=record.work_unit_id,
            tmdb_id=123,
            media_type="tv",
            source_root=source_root,
            target_work_root=target_root,
            library_root="/library",
            source_snapshot_id="fresh-1",
            coordinate_map={"S01E01": "S01E13"},
            source_objects=source,
            target_objects=target,
            selected_subtitles={"S01E01": source[1]},
        )
        persisted = self.application.persist_replacement_manifest(manifest)
        self.assertEqual(persisted["replacement"]["manifest_id"], manifest.manifest_id)  # type: ignore[index]

        wrong_identity = build_replacement_manifest(
            manifest_id="replacement-server-2",
            root_job_id=root_id,
            work_unit_id=record.work_unit_id,
            tmdb_id=999,
            media_type="tv",
            source_root=source_root,
            target_work_root=target_root,
            library_root="/library",
            source_snapshot_id="fresh-2",
            coordinate_map={"S01E01": "S01E13"},
            source_objects=source,
            target_objects=target,
            selected_subtitles={"S01E01": source[1]},
        )
        with self.assertRaises(EngineJobConflictError):
            self.application.persist_replacement_manifest(wrong_identity)

    def _prepare_rebuildable_boundary_root(self) -> str:
        """Create a paused root with only automatically-derived C/U facts."""
        from engine.scrapeflow.root_boundaries import analyze_root_boundaries

        self.remote.entries["/library/待刮削/Example"] = [
            {"name": "Northwind.Show.S01.1080p", "is_dir": True},
            {"name": "Northwind.Show.S02.1080p", "is_dir": True},
            {"name": "Northwind.Show.S03.1080p", "is_dir": True},
            {"name": "Northwind.Show.S04.1080p", "is_dir": True},
            {"name": "Northwind.Aftershow", "is_dir": True},
            {"name": "poster.png", "is_dir": False, "size": 3},
        ]
        for season in (1, 2, 3):
            self.remote.entries[
                f"/library/待刮削/Example/Northwind.Show.S{season:02d}.1080p"
            ] = [
                {
                    "name": f"Northwind.Show.S{season:02d}E01.1080p.mkv",
                    "is_dir": False,
                    "size": 10,
                },
            ]
        self.remote.entries["/library/待刮削/Example/Northwind.Show.S04.1080p"] = []
        self.remote.entries["/library/待刮削/Example/Northwind.Aftershow"] = [
            {"name": f"Northwind.Aftershow.E{episode:02d}.mkv", "is_dir": False, "size": 10}
            for episode in range(1, 7)
        ]
        root_id = self.create_root()
        records = analyze_root_boundaries(
            self.remote,
            "/library/待刮削/Example",
            root_task_id=root_id,
            state_root=self.state_root,
        )
        self.assertEqual(len(records), 2)
        automatic = replace(
            records[0],
            identity_status="confirmed",
            identity={"source": "automatic", "tmdb_id": 17, "media_type": "tv"},
        )
        save_work_unit_records(
            self.state_root,
            root_id,
            [automatic, *records[1:]],
        )
        parked = replace(
            self.runner.get_job(root_id),
            phase="reconciliation_uncertain",
            error="自动身份待重新核对",
        )
        atomic_write_json(
            self.runner._job_path(root_id),  # noqa: SLF001 - durable fixture
            parked.as_dict(),
            allow_nan=False,
        )
        return root_id

    def test_paused_boundary_rebuild_replaces_only_pre_d_work_units(self) -> None:
        root_id = self._prepare_rebuildable_boundary_root()
        before = self.runner.get_job(root_id)

        status, payload = self.request(
            "POST", f"/api/jobs/{root_id}/rebuild-boundaries", {},
        )

        self.assertEqual(status, 200)
        self.assertEqual(payload["job"]["phase"], "queued")
        self.assertEqual(self.application.control(), {"paused": True, "root_job_id": root_id})
        after = self.runner.get_job(root_id)
        self.assertEqual(after.target_shelf, before.target_shelf)
        self.assertEqual(after.target_root, before.target_root)
        self.assertEqual(after.selected_at, before.selected_at)
        self.assertEqual(after.plan, {})
        self.assertIsNone(after.execution)
        self.assertIsNone(after.error)
        records = load_work_unit_records(self.state_root, root_id)
        self.assertEqual(len(records), 2)
        cohort = next(record for record in records if len(record.source_paths) == 4)
        self.assertEqual(cohort.claimed_seasons, (1, 2, 3, 4))
        self.assertEqual(
            cohort.source_paths,
            tuple(
                f"/library/待刮削/Example/Northwind.Show.S{season:02d}.1080p"
                for season in range(1, 5)
            ),
        )
        self.assertTrue(all(record.source_revision == 2 for record in records))
        self.assertTrue(all(record.identity_status == "pending" for record in records))
        self.assertTrue(all(record.identity is None for record in records))
        self.assertTrue(all(record.reconciliation_outcome is None for record in records))

    def test_boundary_rebuild_requires_paused_selected_root(self) -> None:
        root_id = self._prepare_rebuildable_boundary_root()
        self.application._control_state.set(paused=False, root_job_id=root_id)  # noqa: SLF001

        status, _payload = self.request(
            "POST", f"/api/jobs/{root_id}/rebuild-boundaries", {},
        )

        self.assertEqual(status, 400)
        self.assertEqual(self.runner.get_job(root_id).phase, "reconciliation_uncertain")

    def test_boundary_rebuild_refuses_operator_override_but_allows_proven_empty_receipt(self) -> None:
        root_id = self._prepare_rebuildable_boundary_root()
        records = load_work_unit_records(self.state_root, root_id)
        override = replace(
            records[0],
            identity={"source": "operator_override", "tmdb_id": 17, "media_type": "tv"},
        )
        save_work_unit_records(self.state_root, root_id, [override, *records[1:]])

        status, _payload = self.request(
            "POST", f"/api/jobs/{root_id}/rebuild-boundaries", {},
        )

        self.assertEqual(status, 409)
        self.assertEqual(load_work_unit_records(self.state_root, root_id)[0].identity["source"], "operator_override")

        save_work_unit_records(self.state_root, root_id, records)
        (self.state_root / f"work_acceptance_{root_id}.json").write_text("[]", encoding="utf-8")
        status, payload = self.request(
            "POST", f"/api/jobs/{root_id}/rebuild-boundaries", {},
        )
        self.assertEqual(status, 200)
        self.assertEqual(payload["job"]["phase"], "queued")

    def _prepare_prewrite_failed_boundary_root(self) -> tuple[str, str]:
        """Create the sole failed shape that may safely rebuild B/W.

        The unit reached automatic C/D=new_work, then failed before a carrier
        or plan existed.  This is deliberately unlike an accepted/partial
        writer result: its receipt has no target, writer id, or planned files.
        """
        from engine.scrapeflow.root_boundaries import analyze_root_boundaries

        self.remote.entries["/library/待刮削/Example"] = [
            {"name": "Example.Show.S01E01.mkv", "is_dir": False, "size": 10},
        ]
        root_id = self.create_root()
        records = analyze_root_boundaries(
            self.remote,
            "/library/待刮削/Example",
            root_task_id=root_id,
            state_root=self.state_root,
        )
        self.assertEqual(len(records), 1)
        record = replace(
            records[0],
            identity_status="confirmed",
            identity={
                "source": "automatic",
                "tmdb_id": 17,
                "media_type": "tv",
            },
            reconciliation_outcome="new_work",
        )
        save_work_unit_records(self.state_root, root_id, [record])
        failed = replace(
            self.runner.get_job(root_id),
            phase="failed",
            error="planner failed before carrier persistence",
        )
        atomic_write_json(
            self.runner._job_path(root_id),  # noqa: SLF001 - durable fixture
            failed.as_dict(),
            allow_nan=False,
        )
        save_work_acceptance(
            self.state_root,
            root_id,
            [WorkAcceptanceResult(
                work_unit_id=record.work_unit_id,
                outcome="failed",
                writer_job_id=None,
                phase="failed",
                target_root="",
                planned_files=0,
                error="planner failed before carrier persistence",
                recorded_at="2026-08-19T00:00:00Z",
            )],
        )
        return root_id, record.work_unit_id

    def test_prewrite_failed_root_rebuilds_and_discards_only_old_failure_receipt(self) -> None:
        root_id, unit_id = self._prepare_prewrite_failed_boundary_root()
        acceptance = self.state_root / f"work_acceptance_{root_id}.json"

        status, payload = self.request(
            "POST", f"/api/jobs/{root_id}/rebuild-boundaries", {},
        )

        self.assertEqual(status, 200)
        self.assertEqual(payload["job"]["phase"], "queued")
        self.assertEqual(self.application.control(), {"paused": True, "root_job_id": root_id})
        rebuilt = self.runner.get_job(root_id)
        self.assertEqual(rebuilt.target_shelf, "anime")
        self.assertEqual(rebuilt.target_root, "/library/番剧")
        self.assertEqual(rebuilt.plan, {})
        self.assertIsNone(rebuilt.execution)
        self.assertIsNone(rebuilt.error)
        self.assertFalse(acceptance.exists())
        records = load_work_unit_records(self.state_root, root_id)
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0].source_revision, 2)
        self.assertEqual(records[0].identity_status, "pending")
        self.assertIsNone(records[0].identity)
        self.assertIsNone(records[0].reconciliation_outcome)
        self.assertFalse(self.runner._job_path(f"unit-{unit_id}").exists())  # noqa: SLF001

    def test_prewrite_failed_rebuild_authenticates_before_remote_staging_probe(self) -> None:
        """A cold AList session must not make absent task staging look unknown."""
        original_list = self.remote.list
        self.remote.token = "warm"  # type: ignore[attr-defined]
        self.remote.login_calls = 0  # type: ignore[attr-defined]

        def login() -> None:
            self.remote.login_calls += 1  # type: ignore[attr-defined]
            self.remote.token = "fresh"  # type: ignore[attr-defined]

        def guarded_list(path: str, refresh: bool = False) -> list[dict[str, object]]:
            if not self.remote.token:  # type: ignore[attr-defined]
                raise RuntimeError("AList session is cold")
            return original_list(path, refresh=refresh)

        self.remote.login = login  # type: ignore[attr-defined]
        self.remote.list = guarded_list  # type: ignore[method-assign]
        root_id, _unit_id = self._prepare_prewrite_failed_boundary_root()
        self.remote.token = None  # type: ignore[attr-defined]

        status, payload = self.request(
            "POST", f"/api/jobs/{root_id}/rebuild-boundaries", {},
        )

        self.assertEqual(status, 200)
        self.assertEqual(payload["job"]["phase"], "queued")
        self.assertEqual(self.remote.login_calls, 1)  # type: ignore[attr-defined]

    def test_prewrite_failed_rebuild_refuses_any_carrier_or_nonzero_receipt_fact(self) -> None:
        root_id, unit_id = self._prepare_prewrite_failed_boundary_root()
        acceptance = self.state_root / f"work_acceptance_{root_id}.json"
        original = acceptance.read_bytes()
        save_work_acceptance(
            self.state_root,
            root_id,
            [WorkAcceptanceResult(
                work_unit_id=unit_id,
                outcome="failed",
                writer_job_id=None,
                phase="failed",
                target_root="/library/番剧/Example",
                planned_files=1,
                error="not a pre-write failure",
                recorded_at="2026-08-19T00:00:00Z",
            )],
        )

        status, _payload = self.request(
            "POST", f"/api/jobs/{root_id}/rebuild-boundaries", {},
        )

        self.assertEqual(status, 409)
        self.assertEqual(self.runner.get_job(root_id).phase, "failed")
        self.assertNotEqual(acceptance.read_bytes(), original)

        # Restore the exact zero-file receipt, then prove that even an
        # unmarked deterministic carrier (the plan→mark crash window) blocks
        # the rebuild before any B/W state is replaced.
        acceptance.write_bytes(original)
        self.runner._job_path(f"unit-{unit_id}").write_text("{}", encoding="utf-8")  # noqa: SLF001
        status, _payload = self.request(
            "POST", f"/api/jobs/{root_id}/rebuild-boundaries", {},
        )
        self.assertEqual(status, 409)
        self.assertEqual(self.runner.get_job(root_id).phase, "failed")

        self.runner._job_path(f"unit-{unit_id}").unlink()  # noqa: SLF001
        local_archive, remote_archive = self.runner._archive_task_roots(  # noqa: SLF001
            f"unit-{unit_id}",
        )
        local_archive.mkdir(parents=True)
        status, _payload = self.request(
            "POST", f"/api/jobs/{root_id}/rebuild-boundaries", {},
        )
        self.assertEqual(status, 409)
        local_archive.rmdir()

        remote_parent, remote_name = remote_archive.rsplit("/", 1)
        self.remote.entries[remote_parent] = [{"name": remote_name, "is_dir": True}]
        status, _payload = self.request(
            "POST", f"/api/jobs/{root_id}/rebuild-boundaries", {},
        )
        self.assertEqual(status, 409)
        self.assertEqual(self.runner.get_job(root_id).phase, "failed")

    def test_cancel_selected_root_clears_control_and_keeps_all_local_staging(self) -> None:
        root_id = self.create_root()
        owned = self.state_root / "staging" / root_id / "partial.bin"
        foreign = self.state_root / "staging" / "other-root" / "keep.bin"
        owned.parent.mkdir(parents=True)
        foreign.parent.mkdir(parents=True)
        owned.write_bytes(b"task bytes")
        foreign.write_bytes(b"other task bytes")

        status, payload = self.request(
            "POST",
            f"/api/jobs/{root_id}/cancel",
            {"reason": "不再处理"},
        )

        self.assertEqual(status, 200)
        self.assertEqual(payload["job"]["phase"], "cancelled")
        self.assertEqual(self.runner.get_job(root_id).phase, "cancelled")
        self.assertEqual(
            self.application.control(),
            {"paused": True, "root_job_id": None},
        )
        # Cancellation is a state transition only. It never calls cleanup and
        # therefore cannot remove either this task's partial staging or a
        # different task's staging tree.
        self.assertEqual(owned.read_bytes(), b"task bytes")
        self.assertEqual(foreign.read_bytes(), b"other task bytes")

        resume_status, _resume = self.request("POST", "/api/control/resume", {})
        self.assertEqual(resume_status, 400)
        select_status, _select = self.request(
            "POST", "/api/control/select", {"root_job_id": root_id},
        )
        self.assertEqual(select_status, 400)

    def test_cancel_completed_root_stops_active_replenishment_at_pause_boundary(self) -> None:
        root_id = self.create_root()
        completed = replace(self.runner.get_job(root_id), phase="completed")
        atomic_write_json(
            self.runner._job_path(root_id),  # noqa: SLF001 - durable fixture
            completed.as_dict(),
            allow_nan=False,
        )
        self.application._control_state.set(paused=False, root_job_id=root_id)  # noqa: SLF001
        entered = threading.Event()
        saw_pause = threading.Event()
        release = threading.Event()
        provider_effects: list[str] = []

        def blocked_replenishment(
            _runner: object,
            _state_root: Path,
            _root_task_id: str,
            *,
            pause_requested=None,
            **_kwargs: object,
        ) -> dict[str, object]:
            # Model a child formal writer still holding the only writer lock.
            # Cancellation must leave a root marker, then this worker sees the
            # root pause before it could make its next provider-side effect.
            with self.runner.worker_lock():
                entered.set()
                deadline = time.monotonic() + 3
                while not callable(pause_requested) or not pause_requested():
                    if time.monotonic() >= deadline:
                        self.fail("取消没有到达补源 pause 边界")
                    time.sleep(0.01)
                saw_pause.set()
                self.assertTrue(release.wait(timeout=3))
            if callable(pause_requested) and pause_requested():
                return {
                    "tier": "quark_share",
                    "tier_before": "quark_share",
                    "waiting": "paused",
                }
            provider_effects.append("unexpected provider effect")
            return {
                "tier": "quark_share",
                "tier_before": "quark_share",
                "waiting": None,
            }

        with patch(
            "local.scrapeflow_api.root_replenishment.run_root_replenishment",
            side_effect=blocked_replenishment,
        ):
            queued = self.application._queue_selected_work(  # noqa: SLF001
                root_id,
                self.application._run_root_replenishment,  # noqa: SLF001
            )
            self.assertEqual(queued, "queued")
            worker = self.application._worker_future  # noqa: SLF001
            self.assertIsNotNone(worker)
            self.assertTrue(entered.wait(timeout=3))

            status, payload = self.request(
                "POST", f"/api/jobs/{root_id}/cancel", {"reason": "停止补源"},
            )

            self.assertEqual(status, 200)
            # The writer lock is intentionally still held, so the HTTP
            # response may precede the worker's durable final transition.
            self.assertEqual(payload["job"]["phase"], "completed")
            self.assertTrue(self.runner._cancel_request_path(root_id).exists())  # noqa: SLF001
            self.assertEqual(
                self.application.control(),
                {"paused": True, "root_job_id": None},
            )
            self.assertTrue(saw_pause.wait(timeout=3))
            release.set()
            assert worker is not None
            worker.result(timeout=3)

        self.assertEqual(provider_effects, [])
        self.assertEqual(self.runner.get_job(root_id).phase, "cancelled")
        self.assertFalse(self.runner._cancel_request_path(root_id).exists())  # noqa: SLF001

    def test_root_replenishment_wires_read_only_pansou_share_inspector(self) -> None:
        root_id = self.create_root()
        self.application._control_state.set(paused=False, root_job_id=root_id)  # noqa: SLF001
        discovery = object()
        inspector = object()
        captured: list[object] = []

        def fake_replenishment(
            _runner: object,
            _state_root: Path,
            _root_task_id: str,
            *,
            search_runner=None,
            **_kwargs: object,
        ) -> dict[str, object]:
            captured.append(search_runner)
            return {
                "tier": "quark_share",
                "tier_before": "quark_share",
                "waiting": "retry_wait",
            }

        with patch(
            "engine.tools.replenishment_adapter.pansou.quark_share_inspector",
            return_value=inspector,
        ) as make_inspector, patch(
            "engine.tools.replenishment_adapter.pansou.PanSouDiscovery.from_env",
            return_value=discovery,
        ) as from_env, patch(
            "local.scrapeflow_api.root_replenishment.run_root_replenishment",
            side_effect=fake_replenishment,
        ):
            self.application._run_root_replenishment(root_id)  # noqa: SLF001

        self.assertEqual(len(captured), 1)
        search_runner = captured[0]
        self.assertTrue(callable(search_runner))
        service = getattr(search_runner, "__self__", None)
        self.assertIsNotNone(service)
        self.assertIs(getattr(service, "_pansou", None), discovery)
        make_inspector.assert_called_once_with(self.remote, "/library")
        from_env.assert_called_once_with(inspector=inspector)

    def test_dashboard_uses_simple_health_and_controls(self) -> None:
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        with opener.open(self.base + "/", timeout=3) as response:
            body = response.read().decode("utf-8")

        self.assertIn('id="controlButton"', body)
        self.assertIn("state.health.ok !== true", body)
        self.assertIn('"/api/control/resume"', body)
        self.assertNotIn("正在连接", body)

    def test_dashboard_offers_confirmed_task_cancellation(self) -> None:
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        with opener.open(self.base + "/", timeout=3) as response:
            body = response.read().decode("utf-8")

        self.assertIn("function isCancellableJob(job)", body)
        self.assertIn('data-action="cancel"', body)
        self.assertIn("function cancelJob(jobId)", body)
        self.assertIn('"/cancel"', body)
        self.assertIn("确认取消", body)
        self.assertIn('successMessage:"已请求取消任务"', body)

    def test_mutations_require_a_local_same_origin_request(self) -> None:
        status, payload = self.request(
            "POST",
            "/api/intake/refresh",
            {},
            headers={"Host": "example.test"},
        )
        self.assertEqual(status, 403)
        self.assertIn("本机", payload["error"])

class SelectOwnershipIsolationTests(SimpleServerTests):
    """AGENTS.md §3: select is the only authorization entry, so it must prove
    the intake row it relies on describes the very source this root will read.

    ``is_intake_bound_root`` only answers "some catalog row names this id".
    A row whose ``canonical_path`` no longer agrees with the root's own ingress
    is a forged or drifted binding: accepting it would let B/W treat a formal
    shelf as a source tree.  These are the counter-examples, not the happy path.
    """

    def _repoint_root_source(self, root_id: str, source_path: str) -> None:
        """Rewrite only the root's own recorded ingress (no catalog change)."""
        runner = self.application._get_engine_runner()
        payload = runner.get_job(root_id).as_dict()
        payload["request"]["source_path"] = source_path
        payload["summary"]["ingress_source_path"] = source_path
        payload["summary"]["source_root"] = source_path
        atomic_write_json(runner._job_path(root_id), payload, allow_nan=False)

    def test_select_accepts_a_root_whose_binding_still_agrees(self) -> None:
        root_id = self.create_root()
        self.assertEqual(
            self.application.select_root_job(root_id),
            {"paused": True, "root_job_id": root_id},
        )

    def test_forged_binding_cannot_point_select_at_a_formal_shelf(self) -> None:
        root_id = self.create_root()
        before = self.application.control()
        self._repoint_root_source(root_id, "/library/番剧")

        with self.assertRaises(EngineRequestError):
            self.application.select_root_job(root_id)
        # A refused authorization changes nothing: no new selection, still paused.
        self.assertEqual(self.application.control(), before)
        self.assertIs(self.application.control()["paused"], True)

    def test_forged_binding_cannot_point_select_outside_intake(self) -> None:
        root_id = self.create_root()
        before = self.application.control()
        for foreign in (
            "/library/欧美剧/行尸走肉",
            "/library/待刮削",
            "/library/待刮削/Example/Season 01",
            "/library/ScrapeFlow/补源/engine-x/1",
        ):
            with self.subTest(source=foreign):
                self._repoint_root_source(root_id, foreign)
                with self.assertRaises(EngineRequestError):
                    self.application.select_root_job(root_id)
                self.assertEqual(self.application.control(), before)

    def test_stale_catalog_row_for_a_deleted_source_cannot_be_selected(self) -> None:
        """A source that vanished keeps its row, but must not stay selectable.

        This is the "旧权游/旧无耻之徒/旧 Rick" shape: the intake directory is
        gone and the row survives only as history.  The row still names the id,
        so ``is_intake_bound_root`` alone would still say yes.
        """
        from engine.scrapeflow.intake_source import (
            load_intake_catalog,
            save_intake_catalog,
        )

        root_id = self.create_root()
        before = self.application.control()
        catalog = load_intake_catalog(self.state_root)
        save_intake_catalog(
            self.state_root,
            [
                replace(row, canonical_path="/library/待刮削/Deleted", present=False)
                if row.root_task_id == root_id else row
                for row in catalog
            ],
        )

        with self.assertRaises(EngineRequestError):
            self.application.select_root_job(root_id)
        self.assertEqual(self.application.control(), before)

    def test_two_rows_claiming_one_root_is_refused(self) -> None:
        """Overlapping ownership claims are refused instead of picking one."""
        from engine.scrapeflow.intake_source import (
            IntakeSource,
            intake_source_id,
            load_intake_catalog,
            save_intake_catalog,
        )

        root_id = self.create_root()
        before = self.application.control()
        catalog = list(load_intake_catalog(self.state_root))
        owner = next(row for row in catalog if row.root_task_id == root_id)
        duplicate = IntakeSource(
            source_id=intake_source_id("/library/待刮削/Example"),
            canonical_path=owner.canonical_path,
            display_name="Example",
            first_seen_at=owner.first_seen_at,
            last_seen_at=owner.last_seen_at,
            present=True,
            snapshot_revision=owner.snapshot_revision,
            child_count=owner.child_count,
            file_count=owner.file_count,
            root_task_id=root_id,
        )
        save_intake_catalog(self.state_root, [*catalog, duplicate])

        with self.assertRaises(EngineRequestError):
            self.application.select_root_job(root_id)
        self.assertEqual(self.application.control(), before)


if __name__ == "__main__":
    unittest.main()

    def test_boundary_rebuild_discards_automatic_uncertain_parks(self) -> None:
        """An operator source change may discard automatic D=uncertain parks.

        ``uncertain`` is a parked state, not a write-side decision: after the
        operator deletes release folders (轮回七次 shape), the stale units
        and their parks must give way to a fresh B/W rebuild.
        """
        root_id = self._prepare_rebuildable_boundary_root()
        records = load_work_unit_records(self.state_root, root_id)
        parked_units = [
            replace(
                record,
                identity_status="confirmed",
                identity={"source": "automatic", "tmdb_id": 17, "media_type": "tv"},
                reconciliation_outcome="uncertain",
                attention="TV 证据不足挂起",
            )
            for record in records
        ]
        save_work_unit_records(self.state_root, root_id, parked_units)

        status, payload = self.request(
            "POST", f"/api/jobs/{root_id}/rebuild-boundaries", {},
        )

        self.assertEqual(status, 200)
        self.assertEqual(payload["job"]["phase"], "queued")
        fresh = load_work_unit_records(self.state_root, root_id)
        self.assertTrue(all(record.reconciliation_outcome is None for record in fresh))

    def test_boundary_rebuild_keeps_blocking_write_side_facts(self) -> None:
        """A lane/gap fact still blocks rebuild even under an uncertain park."""
        root_id = self._prepare_rebuildable_boundary_root()
        records = load_work_unit_records(self.state_root, root_id)
        tainted = replace(
            records[0],
            identity_status="confirmed",
            identity={"source": "automatic", "tmdb_id": 17, "media_type": "tv"},
            reconciliation_outcome="uncertain",
            lane_status="existing_gap_registered",
        )
        save_work_unit_records(self.state_root, root_id, [tainted, *records[1:]])

        status, payload = self.request(
            "POST", f"/api/jobs/{root_id}/rebuild-boundaries", {},
        )

        self.assertEqual(status, 409)
        self.assertIn("对账或写入", str(payload.get("error", "")))

    def _prepare_partially_written_root(self) -> str:
        """One parked uncertain unit beside a sibling with write-side facts.

        The source shape mirrors a bundled release: a season folder already
        written by G and one mixed feature/mini-series folder whose C match
        parked uncertain before a generic B/W fix was deployed.  The ledger
        is hand-built to the PRE-fix boundary (the whole mixed folder is one
        unit), while the snapshot and exact-object manifest reflect the tree.
        """
        from engine.scrapeflow.root_boundaries import (
            build_root_boundary_analysis,
            persist_root_boundary_analysis,
        )

        big = 2 * 1024 ** 3
        self.remote.entries["/library/待刮削/Example"] = [
            {"name": "Northwind.Show.S01.1080p", "is_dir": True},
            {"name": "Northwind Feature The Final", "is_dir": True},
        ]
        self.remote.entries["/library/待刮削/Example/Northwind.Show.S01.1080p"] = [
            {"name": f"Northwind.Show.S01E{episode:02d}.1080p.mkv", "is_dir": False, "size": big}
            for episode in (1, 2)
        ]
        final_folder = "/library/待刮削/Example/Northwind Feature The Final"
        self.remote.entries[final_folder] = [
            {"name": "Northwind ~The Final~ 2160p.mkv", "is_dir": False, "size": 13 * 1024 ** 3},
            {"name": "Northwind ~The Semi-Final~ [01] 2160p.mkv", "is_dir": False, "size": big},
            {"name": "Northwind ~The Semi-Final~ [02] 2160p.mkv", "is_dir": False, "size": big},
        ]
        root_id = self.create_root()
        snapshot, _records = build_root_boundary_analysis(
            self.remote, "/library/待刮削/Example", root_task_id=root_id,
        )
        persist_root_boundary_analysis(
            self.state_root, root_id, snapshot, _records,
        )
        written = next(
            record for record in _records
            if record.source_paths == (
                "/library/待刮削/Example/Northwind.Show.S01.1080p",
            )
        )
        written = replace(
            written,
            identity_status="confirmed",
            identity={"source": "automatic", "tmdb_id": 17, "media_type": "tv"},
            reconciliation_outcome="new_work",
            writer_job_id="unit-written",
            gap_status="registered",
        )
        parked = WorkUnitRecord(
            work_unit_id="unit-parked-bundle",
            root_task_id=root_id,
            boundary_key=final_folder,
            source_paths=(final_folder,),
            source_revision=1,
            role="series_container",
            display_label="Northwind Feature The Final",
            claimed_seasons=(),
            media_context="unknown",
            identity_status="uncertain",
            attention="自动匹配缺少可验证的标题/别名证据",
        )
        save_work_unit_records(
            self.state_root, root_id, [written, parked],
        )
        parked_job = replace(
            self.runner.get_job(root_id),
            phase="reconciliation_uncertain",
        )
        atomic_write_json(
            self.runner._job_path(root_id),  # noqa: SLF001 - durable fixture
            parked_job.as_dict(),
            allow_nan=False,
        )
        return root_id

    def test_uncertain_unit_rebuild_splits_mixed_folder_after_partial_write(self) -> None:
        """A deployed B/W fix must reach a never-written mixed folder.

        The whole-root rebuild fails closed once a sibling carries writer/gap
        facts; the narrower uncertain-unit surface re-derives only the parked
        unit's scope, keeps the written sibling byte-identical, and queues the
        root for a fresh C/D/E pass over the split candidates.
        """
        root_id = self._prepare_partially_written_root()
        final_folder = "/library/待刮削/Example/Northwind Feature The Final"

        # Whole-root surface must refuse: the sibling has write-side facts.
        status, payload = self.request(
            "POST", f"/api/jobs/{root_id}/rebuild-boundaries", {},
        )
        self.assertEqual(status, 409)
        self.assertIn("对账或写入", str(payload.get("error", "")))

        status, payload = self.request(
            "POST", f"/api/jobs/{root_id}/rebuild-uncertain-units", {},
        )

        self.assertEqual(status, 200)
        self.assertEqual(payload["job"]["phase"], "queued")
        self.assertEqual(
            self.application.control(), {"paused": True, "root_job_id": root_id}
        )
        records = load_work_unit_records(self.state_root, root_id)
        by_paths = {record.source_paths: record for record in records}
        feature = f"{final_folder}/Northwind ~The Final~ 2160p.mkv"
        run_one = f"{final_folder}/Northwind ~The Semi-Final~ [01] 2160p.mkv"
        run_two = f"{final_folder}/Northwind ~The Semi-Final~ [02] 2160p.mkv"
        self.assertIn((feature,), by_paths)
        self.assertIn((run_one, run_two), by_paths)
        self.assertEqual(by_paths[(feature,)].media_context, "movie")
        self.assertEqual(by_paths[(run_one, run_two)].media_context, "tv")
        # The written sibling keeps its identity/D/J facts byte-identical.
        written = next(
            record for record in records
            if record.source_paths == (
                "/library/待刮削/Example/Northwind.Show.S01.1080p",
            )
        )
        self.assertEqual(written.identity_status, "confirmed")
        self.assertEqual(written.writer_job_id, "unit-written")
        self.assertEqual(written.gap_status, "registered")
        # Fresh units are pending at the new revision and own no facts.
        for record in records:
            if record is written:
                continue
            self.assertEqual(record.source_revision, 2)
            self.assertEqual(record.identity_status, "pending")
            self.assertIsNone(record.identity)
            self.assertIsNone(record.reconciliation_outcome)

    def test_uncertain_unit_rebuild_requires_paused_selected_root(self) -> None:
        root_id = self._prepare_partially_written_root()
        self.application._control_state.set(  # noqa: SLF001
            paused=False, root_job_id=root_id,
        )

        status, payload = self.request(
            "POST", f"/api/jobs/{root_id}/rebuild-uncertain-units", {},
        )

        self.assertEqual(status, 400)
        self.assertIn("暂停态", str(payload.get("error", "")))

    def test_uncertain_unit_rebuild_refuses_written_uncertain_unit(self) -> None:
        """A D-written record masquerading as uncertain must stop the surface."""
        root_id = self._prepare_partially_written_root()
        records = load_work_unit_records(self.state_root, root_id)
        parked = next(
            record for record in records if record.identity_status == "uncertain"
        )
        tainted = replace(parked, reconciliation_outcome="new_work")
        save_work_unit_records(
            self.state_root, root_id,
            [record for record in records if record is not parked] + [tainted],
        )

        status, payload = self.request(
            "POST", f"/api/jobs/{root_id}/rebuild-uncertain-units", {},
        )

        self.assertEqual(status, 400)
        self.assertIn("对账或写入", str(payload.get("error", "")))
